# s3js
Higher level access to S3

## API

### parseAddress
`const { Bucket Key } = parseAddress(s3url)`

Parses an S3 url of the form `s3://<Bucket>/<Key>`

### scan
`for await (const obj of scan(url, opts) {...}`

Provides an async iterator over the objects, fetching continuations if set.

The returned object might be an S3 object, or a CommonPrefix (if Delimiter) is set.

Options:
- `Delimiter` - sets the path delimiter for pseudo directory access
- `MaxKeys` - set the max keys returned

### stat
`const stats = stat(url)`

Performs a HEAD on the object, and also decodes any `s3cmd-attrs` metadata set

### download
`await download(s3url, file, opts)`

Downloads the S3 object to a local file. If the object has `s3cmd-attrs` set
then the downloaded file will have its times & modes set

Options:
- `onProgress` - a progress function to call with `{ bytes, total }`
- `progressInterval` - defaults to 1000ms
- `limit` - limit the transfer rate in bytes/second (can have `k` or `m` suffix)

### upload
`await upload(file, s3url, opts)`

Uploads the file to an S3 object, setting `s3cmd-attrs` metadata.

Options:
- `onProgress` - a progress function to call with `{ bytes, total }`
- `progressInterval` - defaults to 1000ms
- `limit` - limit the transfer rate in bytes/second (can have `k` or `m` suffix)
