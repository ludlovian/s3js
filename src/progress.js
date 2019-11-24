'use strict'

import { Transform } from 'stream'

export default function progress (opts = {}) {
  const { onProgress, progressInterval, ...rest } = opts
  let interval
  let bytes = 0
  let done = false

  const ts = new Transform({
    transform (chunk, encoding, cb) {
      bytes += chunk.length
      cb(null, chunk)
    },
    flush (cb) {
      if (interval) clearInterval(interval)
      done = true
      reportProgress()
      cb()
    }
  })

  if (progressInterval) {
    interval = setInterval(reportProgress, progressInterval)
  }
  if (typeof onProgress === 'function') {
    ts.on('progress', onProgress)
  }

  return ts

  function reportProgress () {
    ts.emit('progress', { bytes, done, ...rest })
  }
}
