'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

function _interopNamespace(e) {
  if (e && e.__esModule) { return e; } else {
    var n = {};
    if (e) {
      Object.keys(e).forEach(function (k) {
        var d = Object.getOwnPropertyDescriptor(e, k);
        Object.defineProperty(n, k, d.get ? d : {
          enumerable: true,
          get: function () {
            return e[k];
          }
        });
      });
    }
    n['default'] = e;
    return n;
  }
}

var fs = _interopDefault(require('fs'));
var throttler = _interopDefault(require('throttler'));
var progress = _interopDefault(require('progress-stream'));
var hashStream = _interopDefault(require('hash-stream'));
var mime = _interopDefault(require('mime'));
var path = _interopDefault(require('path'));
var stream = _interopDefault(require('stream'));
var util = _interopDefault(require('util'));

const pipeline = util.promisify(stream.pipeline);
const finished = util.promisify(stream.finished);
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
  const { mtimeMs, ctimeMs, atimeMs, size, mode } = await fs.promises.stat(file);
  const md5 = await getLocalHash(file);
  const contentType = mime.getType(path.extname(file));
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
  const hs = hashStream();
  fs.createReadStream(file).pipe(hs);
  hs.resume();
  await finished(hs);
  return hs.hash
}

const {
  createReadStream,
  createWriteStream,
  promises: { chmod, utimes }
} = fs;
const getS3 = once(async () => {
  const REGION = 'eu-west-1';
  const AWS = await Promise.resolve().then(function () { return _interopNamespace(require('aws-sdk')); });
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
  if (limit) Body = Body.pipe(throttler(limit));
  if (onProgress) {
    Body = Body.pipe(
      progress({
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
  const hasher = hashStream();
  const streams = [
    s3.getObject({ Bucket, Key }).createReadStream(),
    hasher,
    limit && throttler(limit),
    onProgress && progress({ onProgress, progressInterval, total }),
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
