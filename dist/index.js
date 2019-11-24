'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

var stream = require('stream');
var stream__default = _interopDefault(stream);
var fs = _interopDefault(require('fs'));
var util = _interopDefault(require('util'));
var crypto = _interopDefault(require('crypto'));
var mime = _interopDefault(require('mime'));
var path = _interopDefault(require('path'));
var AWS = _interopDefault(require('aws-sdk'));

function throttler (options) {
  if (typeof options !== 'object') options = { rate: options };
  const bytesPerSecond = ensureNumber(options.rate);
  const chunkSize = Math.max(Math.ceil(bytesPerSecond / 10), 1);
  const stream$1 = new stream.Transform({
    ...options,
    transform (data, encoding, callback) {
      takeChunk.call(this, data, callback);
    }
  });
  Object.assign(stream$1, {
    bytesPerSecond,
    chunkSize,
    chunkBytes: 0,
    windowMaxTimeMs: 30 * 1000
  });
  resetWindow.call(stream$1);
  return stream$1
}
function takeChunk (data, done) {
  const chunk = data.slice(0, this.chunkSize - this.chunkBytes);
  const rest = data.slice(chunk.length);
  if (rest.length) {
    processChunk.call(this, chunk, takeChunk.bind(this, rest, done));
  } else {
    processChunk.call(this, chunk, done);
  }
}
function processChunk (data, done) {
  const size = data.length;
  this.chunkBytes += size;
  this.windowBytes += size;
  if (this.chunkBytes < this.chunkSize) {
    return pushChunk.call(this, data, done)
  }
  this.chunkBytes -= this.chunkSize;
  const delay = calculateDelay.call(this);
  if (!delay) {
    pushChunk.call(this, data, done);
  } else {
    setTimeout(pushChunk.bind(this, data, done), delay);
  }
}
function pushChunk (chunk, done) {
  this.push(chunk);
  done();
}
function calculateDelay () {
  const windowTimeMs = getTimeMs() - this.windowStartMs;
  const expectedTimeMs = (1e3 * this.windowBytes) / this.bytesPerSecond;
  if (windowTimeMs > this.windowMaxTimeMs) resetWindow.call(this);
  return Math.max(expectedTimeMs - windowTimeMs, 0)
}
function resetWindow () {
  this.windowStartMs = getTimeMs();
  this.windowBytes = 0;
}
function getTimeMs () {
  const [seconds, nanoseconds] = process.hrtime();
  return seconds * 1e3 + Math.floor(nanoseconds / 1e6)
}
function ensureNumber (value) {
  let n = (value + '').toLowerCase();
  const m = n.endsWith('m') ? 1e6 : n.endsWith('k') ? 1e3 : 1;
  n = parseInt(n.replace(/[mk]$/, ''));
  if (isNaN(n)) throw new Error(`Cannot understand number "${value}"`)
  return n * m
}

function progress (opts = {}) {
  const { onProgress, progressInterval, ...rest } = opts;
  let interval;
  let bytes = 0;
  let done = false;
  const ts = new stream.Transform({
    transform (chunk, encoding, cb) {
      bytes += chunk.length;
      cb(null, chunk);
    },
    flush (cb) {
      if (interval) clearInterval(interval);
      done = true;
      reportProgress();
      cb();
    }
  });
  if (progressInterval) {
    interval = setInterval(reportProgress, progressInterval);
  }
  if (typeof onProgress === 'function') {
    ts.on('progress', onProgress);
  }
  return ts
  function reportProgress () {
    ts.emit('progress', { bytes, done, ...rest });
  }
}

const finished = util.promisify(stream__default.finished);
const pipeline = util.promisify(stream__default.pipeline);
const REGION = 'eu-west-1';
const once = fn => {
  let called = false;
  let value;
  return (...args) => {
    if (called) return value
    value = fn(...args);
    called = true;
    return value
  }
};
const getS3 = once(() => new AWS.S3({ region: REGION }));
const s3regex = /^s3:\/\/([^/]+)\/(.*)$/;
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
  const s3 = getS3();
  const { Bucket, Key: Prefix } = parseAddress(url);
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
  const s3 = getS3();
  const { Bucket, Key } = parseAddress(url);
  const request = { Bucket, Key };
  const result = await s3.headObject(request).promise();
  const md = unpackMetadata((result.Metadata || {})['s3cmd-attrs']);
  return { ...result, ...md }
}
async function download (url, dest, opts = {}) {
  const { onProgress, progressInterval = 1000, limit } = opts;
  const s3 = getS3();
  const { Bucket, Key } = parseAddress(url);
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
  const { onProgress, progressInterval = 1000, limit } = opts;
  const s3 = getS3();
  const { Bucket, Key } = parseAddress(url);
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

exports.download = download;
exports.parseAddress = parseAddress;
exports.scan = scan;
exports.stat = stat;
exports.upload = upload;
