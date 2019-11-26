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

var stream = _interopDefault(require('stream'));
var fs = _interopDefault(require('fs'));
var util = _interopDefault(require('util'));
var throttler = _interopDefault(require('throttler'));
var progress = _interopDefault(require('progress-stream'));
var crypto = _interopDefault(require('crypto'));
var mime = _interopDefault(require('mime'));
var path = _interopDefault(require('path'));

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
function unpackMetadata (string) {
  if (!string) return {}
  const md = {};
  for (const item of string.split('/')) {
    const [k, v] = item.split(':');
    md[k] = maybeNumber(v);
  }
  return md
}
function packMetadata (obj) {
  return Object.keys(obj)
    .sort()
    .filter(k => obj[k] != null)
    .map(k => `${k}:${obj[k]}`)
    .join('/')
}
function getRemoteHash (stats) {
  if (stats.md5) return stats.md5
  const rgx = /^"([a-f0-9]+)"$/;
  const match = rgx.exec(stats.ETag);
  if (match) return match[1]
  throw new Error(`Cannot extract MD5 Hash from ${stats}`)
}
async function getLocalHash (file, { start = 0, end = Infinity } = {}) {
  const rs = fs.createReadStream(file, { start, end });
  const hasher = crypto.createHash('md5');
  rs.on('data', chunk => hasher.update(chunk));
  await finished(rs);
  return hasher.digest('hex')
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
function maybeNumber (v) {
  const n = parseInt(v, 10);
  if (!isNaN(n) && n.toString() === v) return n
  return v
}

const pipeline = util.promisify(stream.pipeline);
const getS3 = once(async () => {
  const REGION = 'eu-west-1';
  const AWS = await new Promise(function (resolve) { resolve(_interopNamespace(require('aws-sdk'))); });
  return new AWS.S3({ region: REGION })
});
const s3regex = /^s3:\/\/([^/]+)\/?(.*)$/;
function parseAddress (addr) {
  const match = s3regex.exec(addr);
  if (!match) {
    throw new Error(`Bad S3 address: ${addr}`)
  }
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
  const md = unpackMetadata((result.Metadata || {})['s3cmd-attrs']);
  return { ...result, ...md }
}
async function download (url, dest, opts = {}) {
  const { onProgress, progressInterval = 1000, limit } = opts;
  const { Bucket, Key } = parseAddress(url);
  const s3 = await getS3();
  const stats = await stat(url);
  const srcHash = getRemoteHash(stats);
  const streams = [];
  streams.push(s3.getObject({ Bucket, Key }).createReadStream());
  if (limit) {
    streams.push(throttler(limit));
  }
  if (onProgress) {
    streams.push(
      progress({
        onProgress,
        progressInterval,
        total: stats.ContentLength
      })
    );
  }
  streams.push(fs.createWriteStream(dest));
  await pipeline(...streams);
  const destHash = await getLocalHash(dest);
  if (destHash !== srcHash) {
    throw new Error(`Error downloading ${url} to ${dest}`)
  }
  if (stats.mode) {
    await fs.promises.chmod(dest, stats.mode & 0o777);
  }
  if (stats.mtime && stats.atime) {
    await fs.promises.utimes(dest, new Date(stats.atime), new Date(stats.mtime));
  }
}
async function upload (file, url, opts = {}) {
  const { Bucket, Key } = parseAddress(url);
  const { onProgress, progressInterval = 1000, limit } = opts;
  const s3 = await getS3();
  const {
    size: ContentLength,
    contentType: ContentType,
    ...rest
  } = await getFileMetadata(file);
  const input = fs.createReadStream(file);
  let Body = input;
  if (limit) {
    const th = throttler(limit);
    Body.on('error', err => th.emit('error', err));
    Body = Body.pipe(th);
  }
  if (onProgress) {
    const pr = progress({
      onProgress,
      progressInterval,
      total: ContentLength
    });
    Body.on('error', err => pr.emit('error', err));
    Body = Body.pipe(pr);
  }
  const request = {
    Body,
    Bucket,
    Key,
    ContentLength,
    ContentType,
    ContentMD5: Buffer.from(rest.md5, 'hex').toString('base64'),
    Metadata: { 's3cmd-attrs': packMetadata(rest) }
  };
  const { ETag } = await s3.putObject(request).promise();
  if (ETag.split('"')[1] !== rest.md5) {
    throw new Error(`Upload of ${file} to ${url} failed`)
  }
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
