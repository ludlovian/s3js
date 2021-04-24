import { writeFileSync, existsSync, readFileSync } from 'fs'
import { test } from 'uvu'
import * as assert from 'uvu/assert'

import * as s3js from '../src/index.mjs'

const BUCKET = 'test-readersludlow'
const FILE = 'test/assets/data.txt'
const OBJECT = `s3://${BUCKET}/path/data.txt`

test('upload', async () => {
  const calls = []

  await s3js.upload(FILE, OBJECT, {
    limit: '1000k',
    onProgress: data => calls.push(data)
  })

  snapshot('upload', calls)
})

test('scan files', async () => {
  const results = []
  const files = s3js.scan(`s3://${BUCKET}/`)
  for await (const { LastModified, ...rest } of files) {
    results.push({ ...rest })
  }

  snapshot('scan-files', results)
})

test('scan dirs', async () => {
  const results = []
  const dirs = s3js.scan(`s3://${BUCKET}/`, { Delimiter: '/' })
  for await (const dir of dirs) {
    results.push(dir)
  }

  snapshot('scan-dirs', results)
})

test('stat', async () => {
  const { LastModified, ...rest } = await s3js.stat(OBJECT)
  snapshot('stat', rest)
})

test('download', async () => {
  const calls = []
  await s3js.download(OBJECT, FILE + '.copy', {
    onProgress: data => calls.push(data)
  })

  snapshot('download-calls', calls)
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

function snapshot (name, data) {
  const file = `test/snapshots/${name}.json`
  if (!existsSync(file)) {
    writeFileSync(file, JSON.stringify(data, undefined, 2))
  }
  const expected = JSON.parse(readFileSync(file, 'utf8'))

  assert.equal(data, expected)
}
