import { readFileSync } from 'fs'
import { test } from 'uvu'
import * as assert from 'uvu/assert'

import snapshot from './helpers/snapshot.mjs'

import * as s3js from '../src/index.mjs'

const BUCKET = 'test-readersludlow'
const FILE = 'test/assets/data.txt'
const OBJECT = `s3://${BUCKET}/path/data.txt`

test('upload', async () => {
  const calls = []

  await s3js.upload(FILE, OBJECT, {
    limit: '1000k',
    onProgress: ({ bytes, done, speedo: { percent, total } }) =>
      calls.push({ bytes, done, percent, total })
  })

  snapshot('upload.json', calls)
})

test('scan files', async () => {
  const results = []
  const files = s3js.scan(`s3://${BUCKET}/`)
  for await (const { LastModified, ...rest } of files) {
    results.push({ ...rest })
  }

  snapshot('scan-files.json', results)
})

test('scan dirs', async () => {
  const results = []
  const dirs = s3js.scan(`s3://${BUCKET}/`, { Delimiter: '/' })
  for await (const dir of dirs) {
    results.push(dir)
  }

  snapshot('scan-dirs.json', results)
})

test('stat', async () => {
  const data = await s3js.stat(OBJECT)
  delete data.LastModified
  delete data.Metadata
  delete data.atime
  snapshot('stat.json', data)
})

test('download', async () => {
  const calls = []
  await s3js.download(OBJECT, FILE + '.copy', {
    limit: '1000k',
    onProgress: ({ bytes, done, speedo: { percent, total } }) =>
      calls.push({ bytes, done, percent, total })
  })

  snapshot('download-calls.json', calls)
  assert.is(readFileSync(FILE, 'utf8'), readFileSync(FILE + '.copy', 'utf8'))
})

test('delete', async () => {
  await s3js.deleteObject(OBJECT)
  assert.ok(true)
})

test('bad address', () => {
  assert.throws(() => s3js.parseAddress('foobar'), /Bad S3 URL/)
})

test.run()
