/**
 * Packaging guard for the harness split-bundle layout.
 *
 * `pnpm deploy` and `npm pack` honor `package.json.files`, so a new entry or a
 * shared chunk that the allowlist does not name is silently absent from the
 * published package (and from the SEA runtime built out of a deploy). The
 * development checkout never notices because it resolves the complete `lib/`
 * tree on disk.
 */

import { readFileSync, readdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

/** Compile the subset of npm `files` globs this package uses (`*`, `**`). */
function patternToRegExp(pattern: string): RegExp {
  const escaped = pattern.replace(/[.+^${}()|[\]\\]/gu, '\\$&')
  const expanded = escaped.replaceAll('**', '\u0000').replaceAll('*', '[^/]*').replaceAll('\u0000', '.*')
  return new RegExp(`^${expanded}$`, 'u')
}

describe('published package files', () => {
  it('ships every emitted runtime bundle and split chunk', () => {
    const root = fileURLToPath(new URL('..', import.meta.url))
    const manifest = JSON.parse(readFileSync(`${root}/package.json`, 'utf8')) as { files?: string[] }
    const patterns = (manifest.files ?? []).map(patternToRegExp)
    const emitted = readdirSync(`${root}/lib`)
      .filter(name => name.endsWith('.js'))
      .map(name => `lib/${name}`)
    const unshipped = emitted.filter(name => !patterns.some(pattern => pattern.test(name)))
    expect(unshipped).toEqual([])
  })
})
