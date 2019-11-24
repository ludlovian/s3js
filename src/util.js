'use strict'

import fs from 'fs'
import crypto from 'crypto'
import stream from 'stream'
import util from 'util'
import mime from 'mime'
import path from 'path'

const finished = util.promisify(stream.finished)

export function once (fn) {
  let called = false
  let value
  return (...args) => {
    if (called) return value
    value = fn(...args)
    called = true
    return value
  }
}

export function unpackMetadata (string) {
  if (!string) return {}
  const md = {}
  for (const item of string.split('/')) {
    const [k, v] = item.split(':')
    md[k] = maybeNumber(v)
  }
  return md
}

export function packMetadata (obj) {
  return Object.keys(obj)
    .sort()
    .filter(k => obj[k] != null)
    .map(k => `${k}:${obj[k]}`)
    .join('/')
}

export function getRemoteHash (stats) {
  if (stats.md5) return stats.md5
  const rgx = /^"([a-f0-9]+)"$/
  const match = rgx.exec(stats.ETag)
  if (match) return match[1]
  throw new Error(`Cannot extract MD5 Hash from ${stats}`)
}

export async function getLocalHash (file, { start = 0, end = Infinity } = {}) {
  const rs = fs.createReadStream(file, { start, end })
  const hasher = crypto.createHash('md5')
  rs.on('data', chunk => hasher.update(chunk))
  await finished(rs)
  return hasher.digest('hex')
}

export async function getFileMetadata (file) {
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
