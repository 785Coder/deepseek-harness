/**
 * Real-runtime service access: the shipped `sdk-jsonrpc-server` entry injects
 * only `agents`, `workingDirectory`, `sdkAppStartup`, and `loader`, so the
 * server must reach every other service through `ctx.get()` rather than a
 * property. A raw test Context does not enforce injects, which is why these
 * paths type-checked and passed while a real profile boot failed at runtime.
 */

import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import * as JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import SqliteSessionQueryEngine from '@deepseek-ai/dsh-session-query-sqlite'
import type { JsonRpcTransportPeer } from '@deepseek-ai/dsh-sdk-protocol'
import { MockAdapter, textResponse } from '../../../core/agent-loop/tests/mock-adapter.ts'
import { HarnessSdkJsonRpcServer } from '../src/index.ts'

const cleanups: (() => Promise<void>)[] = []
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
})

/**
 * Mount the server inside a plugin fiber whose inject list matches the shipped
 * sdk-app entry, so an accidental `ctx.llm`/`ctx.sessions` access throws.
 * @returns the booted context and server.
 */
async function injectedRuntime() {
  const root: string = await mkdtemp(join(tmpdir(), 'sdk-inject-'))
  cleanups.push(() => rm(root, { recursive: true, force: true }))
  const ctx: Context = new Context()
  cleanups.push(() => ctx.fiber.dispose())
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(JsonlSessionPersistence, { root, compression: 'none' })
  await ctx.plugin(SqliteSessionQueryEngine, { path: join(root, 'query.db') })
  await ctx.plugin(AgentLoop, { agents: [] })
  ctx.llm.registerAdapter(['mock'], new MockAdapter([textResponse('first'), textResponse('second')]))

  const transport: JsonRpcTransportPeer = {
    request: async () => { throw new Error('unexpected server request') },
    notify: () => {},
  }
  const holder: { server?: HarnessSdkJsonRpcServer } = {}
  const fiber = ctx.plugin({
    name: 'sdk-jsonrpc-server-entry',
    // The shipped entry injects agents plus launcher services this fixture does
    // not mount; `agents` alone already makes llm/sessions lookup-only here.
    inject: ['agents'],
    apply(pluginCtx: Context) {
      holder.server = new HarnessSdkJsonRpcServer(pluginCtx, transport)
    },
  })
  await fiber.await()
  const server = holder.server
  if (server === undefined) throw new Error('the SDK server plugin did not apply')
  cleanups.push(() => server.shutdown().then(() => {}))
  await server.initialize({ cwd: root, provider: 'mock', model: 'mock' })
  return { ctx, server }
}

describe('SDK server under the shipped inject list', () => {
  it('selects a model without an injected llm service', async () => {
    const { server } = await injectedRuntime()
    await server.prompt({ sessionId: 'inject-model', contentBlocks: [{ type: 'text', text: 'go' }] })
    await server.wait({ sessionId: 'inject-model' })

    const selected = await server.selectModel({ sessionId: 'inject-model', provider: 'mock', model: 'mock' })
    expect(selected.selected).toEqual({ provider: 'mock', model: 'mock' })
  })

  it('lists sessions without an injected sessions registry', async () => {
    const { server } = await injectedRuntime()
    await server.prompt({ sessionId: 'inject-list', contentBlocks: [{ type: 'text', text: 'go' }] })
    await server.wait({ sessionId: 'inject-list' })

    const listed = await server.listSessions({ limit: 10 })
    const row = listed.sessions.find(entry => entry.sessionId === 'inject-list')
    expect(row).toMatchObject({ sessionId: 'inject-list', live: true, blank: false })
  })
})
