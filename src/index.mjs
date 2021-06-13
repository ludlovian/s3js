import { createReadStream, createWriteStream } from 'fs'
import { chmod, utimes } from 'fs/promises'
import { PassThrough } from 'stream'
import { pipeline } from 'stream/promises'
import AWS from 'aws-sdk'

import createSpeedo from 'speedo/gen'
import throttler from 'throttler/gen'
import progressStream from 'progress-stream/gen'
import hashStream from 'hash-stream/gen'
import once from 'pixutil/once'

import { getFileMetadata } from './localFile.mjs'
import { unpackMetadata, packMetadata } from './util.mjs'

const getS3 = once(async () => {
  const REGION = 'eu-west-1'
  return new AWS.S3({ region: REGION })
})

// parseAddress
//
// split an s3 url into Bucket and Key
//
export function parseAddress (url) {
  const m = /^s3:\/\/([^/]+)\/(.*)/.exec(url)
  if (!m) throw new TypeError(`Bad S3 URL: ${url}`)
  const [, Bucket, Key] = m
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
    /* c8 ignore next 4 */
    if (result.IsTruncated) {
      request.ContinuationToken = result.NextContinuationToken
      pResult = s3.listObjectsV2(request).promise()
    } else {
      pResult = null
    }

    for (const item of result.Contents) {
      yield item
    }

    /* c8 ignore next */
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
  const { onProgress, interval = 1000, limit } = opts

  const s3 = await getS3()
  const {
    size: ContentLength,
    contentType: ContentType,
    ...metadata
  } = await getFileMetadata(file)

  // streams
  const speedo = createSpeedo({ total: ContentLength })
  const Body = new PassThrough()

  const pPipeline = pipeline(
    ...[
      createReadStream(file),
      limit && throttler(limit),
      onProgress && speedo,
      onProgress && progressStream({ onProgress, interval, speedo }),
      Body
    ].filter(Boolean)
  )

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
  const pUpload = s3.putObject(request).promise()

  // wait for everything to finish
  await Promise.all([pPipeline, pUpload])
  const { ETag } = await pUpload

  // check the etag is the md5 of the source data
  /* c8 ignore next 3 */
  if (ETag !== `"${metadata.md5}"`) {
    throw new Error(`Upload of ${file} to ${url} failed`)
  }
}

// download
//
// download an S3 object to a file, with progress and/or rate limiting
//
export async function download (url, dest, opts = {}) {
  const { onProgress, interval = 1000, limit } = opts
  const { Bucket, Key } = parseAddress(url)

  const s3 = await getS3()
  const { ETag, ContentLength: total, atime, mtime, mode, md5 } = await stat(
    url
  )
  /* c8 ignore next */
  const hash = md5 || (!ETag.includes('-') && ETag.replace(/"/g, ''))

  const hasher = hashStream()
  const speedo = createSpeedo({ total })
  const streams = [
    s3.getObject({ Bucket, Key }).createReadStream(),
    hasher,
    limit && throttler(limit),
    onProgress && speedo,
    onProgress && progressStream({ onProgress, interval, speedo }),
    createWriteStream(dest)
  ].filter(Boolean)

  await pipeline(...streams)
  /* c8 ignore next 3 */
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
