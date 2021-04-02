'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

var fs = require('fs');
var throttler = require('throttler');
var progress = require('progress-stream');
var hashStream = require('hash-stream');
var mime = require('mime');
var path = require('path');
var stream = require('stream');
var util = require('util');

function _interopDefaultLegacy (e) { return e && typeof e === 'object' && 'default' in e ? e : { 'default': e }; }

function _interopNamespace(e) {
  if (e && e.__esModule) return e;
  var n = Object.create(null);
  if (e) {
    Object.keys(e).forEach(function (k) {
      if (k !== 'default') {
        var d = Object.getOwnPropertyDescriptor(e, k);
        Object.defineProperty(n, k, d.get ? d : {
          enumerable: true,
          get: function () {
            return e[k];
          }
        });
      }
    });
  }
  n['default'] = e;
  return Object.freeze(n);
}

var fs__default = /*#__PURE__*/_interopDefaultLegacy(fs);
var throttler__default = /*#__PURE__*/_interopDefaultLegacy(throttler);
var progress__default = /*#__PURE__*/_interopDefaultLegacy(progress);
var hashStream__default = /*#__PURE__*/_interopDefaultLegacy(hashStream);
var mime__default = /*#__PURE__*/_interopDefaultLegacy(mime);
var path__default = /*#__PURE__*/_interopDefaultLegacy(path);
var stream__default = /*#__PURE__*/_interopDefaultLegacy(stream);
var util__default = /*#__PURE__*/_interopDefaultLegacy(util);

const pipeline = util__default['default'].promisify(stream__default['default'].pipeline);
const finished = util__default['default'].promisify(stream__default['default'].finished);
function once (fn) {
  let called = false;
  let value;
  return (...args) => {
    if (called) return value
    value = fn(...args);
    called = true;
    return value
  }
}
function unpackMetadata (md, key = 's3cmd-attrs') {
  if (!md || typeof md !== 'object' || !md[key]) return {}
  return md[key].split('/').reduce((o, item) => {
    const [k, v] = item.split(':');
    o[k] = maybeNumber(v);
    return o
  }, {})
}
function packMetadata (obj, key = 's3cmd-attrs') {
  return {
    [key]: Object.keys(obj)
      .sort()
      .filter(k => obj[k] != null)
      .map(k => `${k}:${obj[k]}`)
      .join('/')
  }
}
function maybeNumber (v) {
  const n = parseInt(v, 10);
  if (!isNaN(n) && n.toString() === v) return n
  return v
}

async function getFileMetadata (file) {
  const { mtimeMs, ctimeMs, atimeMs, size, mode } = await fs__default['default'].promises.stat(file);
  const md5 = await getLocalHash(file);
  const contentType = mime__default['default'].getType(path__default['default'].extname(file));
  const uid = 1000;
  const gid = 1000;
  const uname = 'alan';
  const gname = 'alan';
  return {
    uid,
    uname,
    gid,
    gname,
    atime: Math.floor(atimeMs),
    mtime: Math.floor(mtimeMs),
    ctime: Math.floor(ctimeMs),
    size,
    mode,
    md5,
    contentType
  }
}
async function getLocalHash (file) {
  const hs = hashStream__default['default']();
  fs__default['default'].createReadStream(file).pipe(hs);
  hs.resume();
  await finished(hs);
  return hs.hash
}

const {
  createReadStream,
  createWriteStream,
  promises: { chmod, utimes }
} = fs__default['default'];
const getS3 = once(async () => {
  const REGION = 'eu-west-1';
  const AWS = await Promise.resolve().then(function () { return /*#__PURE__*/_interopNamespace(require('aws-sdk')); });
  return new AWS.S3({ region: REGION })
});
function parseAddress (url) {
  const match = /^s3:\/\/([a-zA-Z0-9_-]+)\/?(.*)$/.exec(url);
  if (!match) throw new Error(`Bad S3 URL: ${url}`)
  const [, Bucket, Key] = match;
  return { Bucket, Key }
}
async function * scan (url, opts = {}) {
  const { Delimiter, MaxKeys } = opts;
  const { Bucket, Key: Prefix } = parseAddress(url);
  const s3 = await getS3();
  const request = { Bucket, Prefix, Delimiter, MaxKeys };
  let pResult = s3.listObjectsV2(request).promise();
  while (pResult) {
    const result = await pResult;
    if (result.IsTruncated) {
      request.ContinuationToken = result.NextContinuationToken;
      pResult = s3.listObjectsV2(request).promise();
    } else {
      pResult = null;
    }
    for (const item of result.Contents) {
      yield item;
    }
    for (const item of result.CommonPrefixes || []) {
      yield item;
    }
  }
}
async function stat (url) {
  const { Bucket, Key } = parseAddress(url);
  const s3 = await getS3();
  const request = { Bucket, Key };
  const result = await s3.headObject(request).promise();
  return {
    ...result,
    ...unpackMetadata(result.Metadata)
  }
}
async function upload (file, url, opts = {}) {
  const { Bucket, Key } = parseAddress(url);
  const { onProgress, progressInterval = 1000, limit } = opts;
  const s3 = await getS3();
  const {
    size: ContentLength,
    contentType: ContentType,
    ...metadata
  } = await getFileMetadata(file);
  let Body = createReadStream(file);
  if (limit) Body = Body.pipe(throttler__default['default'](limit));
  if (onProgress) {
    Body = Body.pipe(
      progress__default['default']({
        onProgress,
        progressInterval,
        total: ContentLength
      })
    );
  }
  const request = {
    Body,
    Bucket,
    Key,
    ContentLength,
    ContentType,
    ContentMD5: Buffer.from(metadata.md5, 'hex').toString('base64'),
    Metadata: packMetadata(metadata)
  };
  const { ETag } = await s3.putObject(request).promise();
  if (ETag !== `"${metadata.md5}"`) {
    throw new Error(`Upload of ${file} to ${url} failed`)
  }
}
async function download (url, dest, opts = {}) {
  const { onProgress, progressInterval = 1000, limit } = opts;
  const { Bucket, Key } = parseAddress(url);
  const s3 = await getS3();
  const { ETag, ContentLength: total, atime, mtime, mode, md5 } = await stat(
    url
  );
  const hash = md5 || (!ETag.includes('-') && ETag.replace(/"/g, ''));
  const hasher = hashStream__default['default']();
  const streams = [
    s3.getObject({ Bucket, Key }).createReadStream(),
    hasher,
    limit && throttler__default['default'](limit),
    onProgress && progress__default['default']({ onProgress, progressInterval, total }),
    createWriteStream(dest)
  ].filter(Boolean);
  await pipeline(...streams);
  if (hash && hash !== hasher.hash) {
    throw new Error(`Error downloading ${url} to ${dest}`)
  }
  if (mode) await chmod(dest, mode & 0o777);
  if (mtime && atime) await utimes(dest, new Date(atime), new Date(mtime));
}
async function deleteObject (url, opts = {}) {
  const { Bucket, Key } = parseAddress(url);
  const s3 = await getS3();
  const request = { Bucket, Key, ...opts };
  await s3.deleteObject(request).promise();
}

exports.deleteObject = deleteObject;
exports.download = download;
exports.parseAddress = parseAddress;
exports.scan = scan;
exports.stat = stat;
exports.upload = upload;
