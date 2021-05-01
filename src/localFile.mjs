import mime from 'mime'
import { stat } from 'fs/promises'
import { extname } from 'path'

import hashFile from 'hash-stream/simple'

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

export const getLocalHash = hashFile
