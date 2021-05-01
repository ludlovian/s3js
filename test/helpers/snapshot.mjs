import { existsSync, readFileSync, writeFileSync } from 'fs'
import * as assert from 'uvu/assert'

export default function snap(name, data, { dir = 'test/snapshots' } = {}) {
  const file = `${dir}/${name}`

  if (existsSync(file)) {
    const fileData = readFileSync(file, 'utf8')
    const expected = typeof data === 'string' ? fileData : JSON.parse(fileData)
    assert.equal(data, expected)
  } else {
    const fileData = typeof data === 'string' ? data : JSON.stringify(data, undefined, 2)
    writeFileSync(file, fileData)
    assert.ok(true)
  }
}
