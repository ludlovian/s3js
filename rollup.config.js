import { terser } from 'rollup-plugin-terser'
import cleanup from 'rollup-plugin-cleanup'

export default {
  input: 'src/index.js',
  external: [ 'fs', 'util', 'stream', 'crypto', 'mime', 'path', 'aws-sdk' ],
  plugins: [
    cleanup(),
    process.env.NODE_ENV === 'production' && terser()
  ],
  output: [
    {
      file: 'dist/index.js',
      format: 'cjs',
      sourcemap: false,
    },
    {
      file: 'dist/index.mjs',
      format: 'esm',
      sourcemap: false
    }
  ]
}
