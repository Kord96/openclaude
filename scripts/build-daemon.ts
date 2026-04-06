/**
 * Build script for the agent pod daemon.
 * Produces a standalone dist/daemon.mjs with kafkajs bundled in.
 */

import { readFileSync } from 'fs'

const pkg = JSON.parse(readFileSync('./package.json', 'utf-8'))
const version = pkg.version

const result = await Bun.build({
  entrypoints: ['./src/entrypoints/daemon.ts'],
  outdir: './dist',
  target: 'node',
  format: 'esm',
  splitting: false,
  sourcemap: 'external',
  minify: false,
  naming: 'daemon.mjs',
  define: {
    'MACRO.VERSION': JSON.stringify('99.0.0'),
    'MACRO.DISPLAY_VERSION': JSON.stringify(version),
    'MACRO.BUILD_TIME': JSON.stringify(new Date().toISOString()),
  },
})

if (!result.success) {
  console.error('Daemon build failed:')
  for (const log of result.logs) console.error(log)
  process.exit(1)
}

console.log(`✓ Built openclaude-daemon v${version} → dist/daemon.mjs`)
