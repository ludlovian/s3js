import mime from 'mime/lite.js'
import { createReadStream } from 'fs'
import { stat } from 'fs/promises'
import { finished } from 'stream/promises'
import { extname } from 'path'

import hashStream from 'hash-stream'

export async function getFileMetadata (file) {
  const { mtimeMs, ctimeMs, atimeMs, size, mode } = await stat(file)
  const md5 = await getLocalHash(file)
  const contentType = mime.getType(extname(file))
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

async function getLocalHash (file) {
  const hs = hashStream()
  createReadStream(file).pipe(hs)
  // start consuming data
  hs.resume()
  await finished(hs)
  return hs.hash
}
