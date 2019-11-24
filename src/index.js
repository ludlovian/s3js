'use strict'

import stream from 'stream'
import fs from 'fs'
import util from 'util'
import crypto from 'crypto'
import mime from 'mime'
import path from 'path'

import throttler from './throttler'
import progress from './progress'

import AWS from 'aws-sdk'

const finished = util.promisify(stream.finished)
const pipeline = util.promisify(stream.pipeline)

const REGION = 'eu-west-1'

const once = fn => {
  let called = false
  let value
  return (...args) => {
    if (called) return value
    value = fn(...args)
    called = true
    return value
  }
}

const getS3 = once(() => new AWS.S3({ region: REGION }))

const s3regex = /^s3:\/\/([^/]+)\/(.*)$/

export function parseAddress (addr) {
  const match = s3regex.exec(addr)
  if (!match) {
    throw new Error(`Bad S3 address: ${addr}`)
  }
  const [, Bucket, Key] = match
  return { Bucket, Key }
}

export async function * scan (url, opts = {}) {
  const { Delimiter, MaxKeys } = opts
  const s3 = getS3()
  const { Bucket, Key: Prefix } = parseAddress(url)
  const request = { Bucket, Prefix, Delimiter, MaxKeys }
  let pResult = s3.listObjectsV2(request).promise()

  while (pResult) {
    const result = await pResult
    // start the next one going if needed
    if (result.IsTruncated) {
      request.ContinuationToken = result.NextContinuationToken
      pResult = s3.listObjectsV2(request).promise()
    } else {
      pResult = null
    }

    for (const item of result.Contents) {
      yield item
    }

    for (const item of result.CommonPrefixes || []) {
      yield item
    }
  }
}

export async function stat (url) {
  const s3 = getS3()
  const { Bucket, Key } = parseAddress(url)
  const request = { Bucket, Key }

  const result = await s3.headObject(request).promise()
  const md = unpackMetadata((result.Metadata || {})['s3cmd-attrs'])
  return { ...result, ...md }
}

export async function download (url, dest, opts = {}) {
  const { onProgress, progressInterval = 1000, limit } = opts
  const s3 = getS3()
  const { Bucket, Key } = parseAddress(url)
  const stats = await stat(url)
  const srcHash = getRemoteHash(stats)
  const streams = []
  // the input stream
  streams.push(s3.getObject({ Bucket, Key }).createReadStream())

  // rate limiter
  if (limit) {
    streams.push(throttler(limit))
  }

  // progress monitor
  if (onProgress) {
    streams.push(
      progress({
        onProgress,
        progressInterval,
        total: stats.ContentLength
      })
    )
  }

  // output
  streams.push(fs.createWriteStream(dest))

  await pipeline(...streams)

  const destHash = await getLocalHash(dest)
  if (destHash !== srcHash) {
    throw new Error(`Error downloading ${url} to ${dest}`)
  }

  if (stats.mode) {
    await fs.promises.chmod(dest, stats.mode & 0o777)
  }

  if (stats.mtime && stats.atime) {
    await fs.promises.utimes(dest, new Date(stats.atime), new Date(stats.mtime))
  }
}

export async function upload (file, url, opts = {}) {
  const { onProgress, progressInterval = 1000, limit } = opts
  const s3 = getS3()
  const { Bucket, Key } = parseAddress(url)
  const {
    size: ContentLength,
    contentType: ContentType,
    ...rest
  } = await getFileMetadata(file)

  const input = fs.createReadStream(file)
  let Body = input

  // rate limiter
  if (limit) {
    const th = throttler(limit)
    Body.on('error', err => th.emit('error', err))
    Body = Body.pipe(th)
  }

  // progress
  if (onProgress) {
    const pr = progress({
      onProgress,
      progressInterval,
      total: ContentLength
    })
    Body.on('error', err => pr.emit('error', err))
    Body = Body.pipe(pr)
  }

  const request = {
    Body,
    Bucket,
    Key,
    ContentLength,
    ContentType,
    ContentMD5: Buffer.from(rest.md5, 'hex').toString('base64'),
    Metadata: { 's3cmd-attrs': packMetadata(rest) }
  }
  const { ETag } = await s3.putObject(request).promise()
  if (ETag.split('"')[1] !== rest.md5) {
    throw new Error(`Upload of ${file} to ${url} failed`)
  }
}

function getRemoteHash (stats) {
  if (stats.md5) return stats.md5
  const rgx = /^"([a-f0-9]+)"$/
  const match = rgx.exec(stats.ETag)
  if (match) return match[1]
  throw new Error(`Cannot extract MD5 Hash from ${stats}`)
}

async function getLocalHash (file, { start = 0, end = Infinity } = {}) {
  const rs = fs.createReadStream(file, { start, end })
  const hasher = crypto.createHash('md5')
  rs.on('data', chunk => hasher.update(chunk))
  await finished(rs)
  return hasher.digest('hex')
}

function unpackMetadata (string) {
  if (!string) return {}
  const md = {}
  for (const item of string.split('/')) {
    const [k, v] = item.split(':')
    md[k] = maybeNumber(v)
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
  const { mtimeMs, ctimeMs, atimeMs, size, mode } = await fs.promises.stat(file)
  const md5 = await getLocalHash(file)
  const contentType = mime.getType(path.extname(file))
  const uid = 1000
  const gid = 1000
  const uname = 'alan'
  const gname = 'alan'
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
  const n = parseInt(v, 10)
  if (!isNaN(n) && n.toString() === v) return n
  return v
}
