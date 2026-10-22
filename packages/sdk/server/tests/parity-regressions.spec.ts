import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import { SessionId } from '@deepseek-ai/dsh-session'
import * as JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import SqliteSessionQueryEngine from '@deepseek-ai/dsh-session-query-sqlite'
import type { JsonRpcTransportPeer, SessionHistoryResult } from '@deepseek-ai/dsh-sdk-protocol'
import { paginateSessionHistory } from '@deepseek-ai/dsh-api-session-controller/shared'
import { MockAdapter, textResponse } from '../../../core/agent-loop/tests/mock-adapter.ts'
import { HarnessSdkJsonRpcServer } from '../src/index.ts'

const cleanup: (() => Promise<void>)[] = []
afterEach(async () => {
  for (const dispose of cleanup.splice(0).reverse()) await dispose()
})

async function runtime(store?: string, sessionIds?: string[]) {
  const root: string = store ?? await mkdtemp(join(tmpdir(), 'sdk-parity-regression-'))
  if (store === undefined) cleanup.push(() => rm(root, { recursive: true, force: true }))
  const ctx: Context = new Context()
  cleanup.push(() => ctx.fiber.dispose())
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(JsonlSessionPersistence, { root, compression: 'none' })
  await ctx.plugin(SqliteSessionQueryEngine, { path: join(root, 'query.db') })
  await ctx.plugin(AgentLoop, { agents: [] })
  ctx.llm.registerAdapter(['mock'], new MockAdapter(Array.from({ length: 8 }, () => textResponse('answer'))))
  const notifications: { method: string; params: object }[] = []
  const peer: JsonRpcTransportPeer = { notify: (method, params) => { notifications.push({ method, params: params as object }) }, request: async () => { throw new Error('unexpected request') } }
  const server: HarnessSdkJsonRpcServer = new HarnessSdkJsonRpcServer(ctx, peer)
  cleanup.push(() => server.shutdown().then(() => {}))
  await server.initialize({ cwd: root, provider: 'mock', model: 'default', ...(sessionIds === undefined ? {} : { sessionIds }) })
  return { root, ctx, server, notifications }
}

async function turn(server: HarnessSdkJsonRpcServer, sessionId: string): Promise<void> {
  await server.prompt({ sessionId, contentBlocks: [{ type: 'text', text: 'continue' }] })
  await server.wait({ sessionId })
}

describe('SDK parity regressions', () => {
  it('refuses foreign ids before attaching, even when their working root matches', async () => {
    const first = await runtime()
    await turn(first.server, 'mine')
    await turn(first.server, 'foreign')
    await first.server.shutdown()
    await first.ctx.fiber.dispose()
    const second = await runtime(first.root, ['mine'])
    for (const method of ['session/create', 'session/resume', 'session/prompt', 'session/rename', 'session/sync']) {
      await expect(second.server.handleRequest(method, {
        sessionId: 'foreign', contentBlocks: [{ type: 'text', text: 'blocked' }], title: 'blocked',
      })).rejects.toMatchObject({ code: -32020, data: { reason: 'session-not-owned' } })
    }
    expect(second.ctx.agents.get(SessionId('foreign'))).toBeUndefined()
    expect((await second.server.listSessions({})).sessions.map(row => row.sessionId)).toEqual(['mine'])
    const foreign = await second.ctx.agents.create({
      sessionId: SessionId('external'), meta: { cwd: first.root }, agentOptions: { provider: 'mock', model: 'default' },
    })
    foreign.agent.session.append('session/title', { title: 'private', messageSeqs: [], source: { kind: 'user' } })
    second.ctx.emit('agent/status', { agent: foreign.agent, status: 'running' })
    expect(second.notifications).toEqual([])
    await foreign.dispose()
    expect(second.notifications).toEqual([])
  })

  it('maps malformed parameters, unavailable services and writer ownership at the wire', async () => {
    const { ctx, server, root } = await runtime()
    await expect(server.handleRequest('session/history', { sessionId: 's', align: 'bad' }))
      .rejects.toMatchObject({ code: -32602, data: { reason: 'invalid-params' } })
    await expect(server.handleRequest('session/projections', { sessionId: 'missing' }))
      .rejects.toMatchObject({ code: -32020, data: { reason: 'not-attached' } })
    const outside = await ctx.agents.create({
      sessionId: SessionId('busy'), meta: { cwd: root },
      agentOptions: { provider: 'mock', model: 'default' },
    })
    await expect(server.handleRequest('session/resume', { sessionId: 'busy' }))
      .rejects.toMatchObject({ code: -32020, data: { reason: 'conflict' } })
    await outside.dispose()
  })

  it('dispatches a backwards message page with the same cut as Web', async () => {
    const { ctx, server } = await runtime()
    await turn(server, 'pages')
    await turn(server, 'pages')
    using source = await ctx.sessionQuery.observeSession(SessionId('pages'))
    const expected = paginateSessionHistory(source.events, undefined, 1, source.cursor)
    const page = await server.handleRequest('session/history', {
      sessionId: 'pages', align: 'message', limit: 1,
    }) as SessionHistoryResult
    expect(page.events.map(event => event.seq)).toEqual(expected.events.map(event => event.seq))
    expect(page.nextCursor).toBe(Number(expected.events[0]?.seq))
    const older = await server.handleRequest('session/history', {
      sessionId: 'pages', align: 'message', limit: 1, beforeSeq: page.nextCursor,
    }) as SessionHistoryResult
    expect(older.events.at(-1)?.seq).toBeLessThan(Number(page.events[0]?.seq))
    await expect(server.handleRequest('session/history', { sessionId: 'pages', align: 'invalid' })).rejects.toThrow(/align/)
  })

  it('never advances sync past events absent from its retained observation', async () => {
    const { ctx, server } = await runtime()
    await turn(server, 'cut')
    const agent = ctx.agents.get(SessionId('cut'))
    if (agent === undefined) throw new Error('missing agent')
    const observed = ctx.sessionQuery.observeSession.bind(ctx.sessionQuery)
    let appended = false
    vi.spyOn(ctx.sessionQuery, 'observeSession').mockImplementation(async (id, options) => {
      const source = await observed(id, options)
      if (!appended) {
        appended = true
        agent.session.append('session/title', { title: 'arrived after cut', messageSeqs: [], source: { kind: 'user' } })
      }
      return source
    })
    const sync = await server.sync({ sessionId: 'cut', limit: 1000 })
    expect(sync.history.hasMore).toBe(false)
    expect(sync.nextSeq).toBe(Number(sync.history.events.at(-1)?.seq) + 1)
    expect(sync.projections?.asOfSeq).toBe(sync.nextSeq - 1)
    const next = await server.history({ sessionId: 'cut', fromSeq: sync.nextSeq })
    expect(next.events[0]?.type).toBe('session/title')
  })

  it.each([false, true])('restores explicit model selection after restart, already-used=%s', async (used) => {
    const first = await runtime()
    await first.server.create({ sessionId: 'model' })
    await first.server.selectModel({ sessionId: 'model', provider: 'mock', model: 'chosen' })
    if (used) await turn(first.server, 'model')
    await first.server.shutdown()
    await first.ctx.fiber.dispose()
    const second = await runtime(first.root)
    await second.server.resume({ sessionId: 'model' })
    await turn(second.server, 'model')
    const agent = second.ctx.agents.get(SessionId('model'))
    expect(agent?.session.requestHeader()?.config.model).toBe('chosen')
  })
})
