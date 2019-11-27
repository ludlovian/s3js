'use strict'

import fs from 'fs'

import throttler from 'throttler'
import progress from 'progress-stream'
import hashStream from 'hash-stream'

import { getFileMetadata } from './localFile'
import { once, unpackMetadata, packMetadata, pipeline } from './util'

const {
  createReadStream,
  createWriteStream,
  promises: { chmod, utimes }
} = fs

const getS3 = once(async () => {
  const REGION = 'eu-west-1'
  const AWS = await import('aws-sdk')
  return new AWS.S3({ region: REGION })
})

// parseAddress
//
// split an s3 url into Bucket and Key
//
export function parseAddress (url) {
  const match = /^s3:\/\/([a-zA-Z0-9_-]+)\/?(.*)$/.exec(url)
  if (!match) throw new Error(`Bad S3 URL: ${url}`)
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
  return {
    ...result,
    ...unpackMetadata(result.Metadata)
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
    ...metadata
  } = await getFileMetadata(file)

  let Body = createReadStream(file)

  // rate limiter
  if (limit) Body = Body.pipe(throttler(limit))

  // progress
  if (onProgress) {
    Body = Body.pipe(
      progress({
        onProgress,
        progressInterval,
        total: ContentLength
      })
    )
  }

  const request = {
    Body,
    Bucket,
    Key,
    ContentLength,
    ContentType,
    ContentMD5: Buffer.from(metadata.md5, 'hex').toString('base64'),
    Metadata: packMetadata(metadata)
  }

  // perform the upload
  const { ETag } = await s3.putObject(request).promise()

  // check the etag is the md5 of the source data
  if (ETag !== `"${metadata.md5}"`) {
    throw new Error(`Upload of ${file} to ${url} failed`)
  }
}

// download
//
// download an S3 object to a file, with progress and/or rate limiting
//
export async function download (url, dest, opts = {}) {
  const { onProgress, progressInterval = 1000, limit } = opts
  const { Bucket, Key } = parseAddress(url)

  const s3 = await getS3()
  const { ETag, ContentLength: total, atime, mtime, mode, md5 } = await stat(
    url
  )
  const hash = md5 || (!ETag.includes('-') && ETag.replace(/"/g, ''))

  const hasher = hashStream()
  const streams = [
    // the source read steam
    s3.getObject({ Bucket, Key }).createReadStream(),

    // hasher
    hasher,

    // rate limiter
    limit && throttler(limit),

    // progress monitor
    onProgress && progress({ onProgress, progressInterval, total }),

    // output
    createWriteStream(dest)
  ].filter(Boolean)

  await pipeline(...streams)
  if (hash && hash !== hasher.hash) {
    throw new Error(`Error downloading ${url} to ${dest}`)
  }

  if (mode) await chmod(dest, mode & 0o777)
  if (mtime && atime) await utimes(dest, new Date(atime), new Date(mtime))
}

export async function deleteObject (url, opts = {}) {
  const { Bucket, Key } = parseAddress(url)
  const s3 = await getS3()

  const request = { Bucket, Key, ...opts }
  await s3.deleteObject(request).promise()
}
