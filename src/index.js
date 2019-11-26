'use strict'

import stream from 'stream'
import fs from 'fs'
import util from 'util'

import throttler from 'throttler'
import progress from 'progress-stream'

import {
  once,
  unpackMetadata,
  packMetadata,
  getLocalHash,
  getRemoteHash,
  getFileMetadata
} from './util'

const pipeline = util.promisify(stream.pipeline)

const getS3 = once(async () => {
  const REGION = 'eu-west-1'
  const AWS = await import('aws-sdk')
  return new AWS.S3({ region: REGION })
})

// parseAddress
//
// split an s3 url into Bucket and Key
//
const s3regex = /^s3:\/\/([^/]+)\/?(.*)$/
export function parseAddress (addr) {
  const match = s3regex.exec(addr)
  if (!match) {
    throw new Error(`Bad S3 address: ${addr}`)
  }
  const [, Bucket, Key] = match
  return { Bucket, Key }
}

// scan
//
// List the objects in a bucket in an async generator
//
export async function * scan (url, opts = {}) {
  const { Delimiter, MaxKeys } = opts
  const { Bucket, Key: Prefix } = parseAddress(url)
  const s3 = await getS3()
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

// stat
//
// Perform a stat-like inspection of an object.
// Decode the s3cmd-attrs if given

export async function stat (url) {
  const { Bucket, Key } = parseAddress(url)
  const s3 = await getS3()

  const request = { Bucket, Key }
  const result = await s3.headObject(request).promise()

  const md = unpackMetadata((result.Metadata || {})['s3cmd-attrs'])
  return { ...result, ...md }
}

// download
//
// download an S3 object to a file, with progress and/or rate limiting
//
export async function download (url, dest, opts = {}) {
  const { onProgress, progressInterval = 1000, limit } = opts
  const { Bucket, Key } = parseAddress(url)

  const s3 = await getS3()
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

// upload
//
// uploads a file to S3 with progress and/or rate limiting

export async function upload (file, url, opts = {}) {
  const { Bucket, Key } = parseAddress(url)
  const { onProgress, progressInterval = 1000, limit } = opts

  const s3 = await getS3()
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
    // forward any errors
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
    // forward any errors
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

  // perform the upload
  const { ETag } = await s3.putObject(request).promise()

  // check the etag is the md5 of the source data
  if (ETag.split('"')[1] !== rest.md5) {
    throw new Error(`Upload of ${file} to ${url} failed`)
  }
}

export async function deleteObject (url, opts = {}) {
  const { Bucket, Key } = parseAddress(url)
  const s3 = await getS3()

  const request = { Bucket, Key, ...opts }
  await s3.deleteObject(request).promise()
}
