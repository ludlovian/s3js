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

export function unpackMetadata (md, key = 's3cmd-attrs') {
  /* c8 ignore next */
  if (!md || typeof md !== 'object' || !md[key]) return {}
  return md[key].split('/').reduce((o, item) => {
    const [k, v] = item.split(':')
    o[k] = maybeNumber(v)
    return o
  }, {})
}

export function packMetadata (obj, key = 's3cmd-attrs') {
  return {
    [key]: Object.keys(obj)
      .sort()
      .filter(k => obj[k] != null)
      .map(k => `${k}:${obj[k]}`)
      .join('/')
  }
}

function maybeNumber (v) {
  const n = parseInt(v, 10)
  if (!isNaN(n) && n.toString() === v) return n
  return v
}
