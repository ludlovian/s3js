# s3js
Higher level access to S3

## API
### listObjects
`for await (const obj of listObjects(opts)) {...}`

Provides an async iterator over the objects, fethcing continuations if set
