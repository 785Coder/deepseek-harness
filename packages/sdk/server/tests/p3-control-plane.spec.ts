import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import { SessionId } from '@deepseek-ai/dsh-session'
import SessionTitleService from '@deepseek-ai/dsh-session-title'
import UserQuestionService from '@deepseek-ai/dsh-user-questions'
import * as JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import SqliteSessionQueryEngine from '@deepseek-ai/dsh-session-query-sqlite'
import type { JsonRpcTransportPeer } from '@deepseek-ai/dsh-sdk-protocol'
import { MockAdapter, textResponse } from '../../../core/agent-loop/tests/mock-adapter.ts'
import { HarnessSdkJsonRpcServer } from '../src/index.ts'
import { expectSdkErrorAsync } from './sdk-error.ts'

const cleanups: (() => Promise<void>)[] = []
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
})

interface Notified { method: string; params: Record<string, unknown> }

async function runtime(options: { query?: boolean; questions?: boolean } = {}) {
  const root: string = await mkdtemp(join(tmpdir(), 'sdk-p3-'))
  cleanups.push(() => rm(root, { recursive: true, force: true }))
  const ctx: Context = new Context()
  cleanups.push(() => ctx.fiber.dispose())
  await mountAgentLoopTestDependencies(ctx)
  if (options.questions ?? true) await ctx.plugin(UserQuestionService)
  await ctx.plugin(JsonlSessionPersistence, { root, compression: 'none' })
  await ctx.plugin(AgentLoop, { agents: [] })
  await ctx.plugin(SessionTitleService, { fallbackMaxWords: 8, fallbackMaxBytes: 64, maxTitleBytes: 200 })
  if (options.query ?? true) await ctx.plugin(SqliteSessionQueryEngine, { path: join(root, 'query.db') })
  ctx.llm.registerAdapter(['mock'], new MockAdapter([textResponse('first'), textResponse('second'), textResponse('third')]))
  ctx.llm.registerAdapter(['alt'], new MockAdapter([textResponse('switched')]))
  const notifications: Notified[] = []
  const transport: JsonRpcTransportPeer = {
    request: async () => { throw new Error('unexpected server request') },
    notify: (method, params) => { notifications.push({ method, params: params as Record<string, unknown> }) },
  }
  const server = new HarnessSdkJsonRpcServer(ctx, transport)
  cleanups.push(() => server.shutdown().then(() => {}))
  await server.initialize({ cwd: root, provider: 'mock', model: 'mock' })
  return { ctx, server, notifications }
}

describe('SDK P3 control plane: attachment read', () => {
  const ref = { attachmentId: 'sha256:image', mediaType: 'image/png', bytes: 3, width: 1, height: 1 }

  /** A session log whose user message carries one durable image reference. */
  function imageRuntime(events: readonly unknown[]) {
    const session = {
      id: SessionId('image'),
      header: { id: 'image', createdAt: 0 },
      snapshotEvents: () => events,
    }
    const agent = { id: SessionId('image'), session } as unknown as Agent
    const readImage = vi.fn(async () => ({ ref, data: Uint8Array.of(1, 2, 3) }))
    const ctx = {
      on: vi.fn(() => () => undefined),
      agents: { get: () => agent, create: vi.fn() },
      sessions: { get: (id: string) => id === 'image' ? session : undefined },
      get: (name: string) => name === 'attachments'
        ? { readImage }
        : name === 'sessionQuery'
          ? { readSession: async () => ({ session: session.header, events }) }
          : undefined,
    } as unknown as Context
    const server = new HarnessSdkJsonRpcServer(ctx, { request: async () => { throw new Error('unexpected') }, notify: () => {} })
    // This isolated fixture begins after the handshake boundary and owns the session.
    const attached = server as unknown as {
      initialized: boolean
      sessions: Map<string, { handle: { agent: Agent; dispose(): Promise<void> }; failure: undefined }>
    }
    attached.initialized = true
    attached.sessions.set('image', { handle: { agent, dispose: async () => {} }, failure: undefined })
    return { server, readImage }
  }

  it('returns the durable ref and base64 payload of a referenced image', async () => {
    const { server, readImage } = imageRuntime([{
      type: 'user/message',
      seq: 0,
      time: 0,
      data: { content: [{ type: 'text', text: 'see' }, { type: 'image', attachment: ref }] },
    }])
    await expect(server.readAttachment({ sessionId: 'image', attachmentId: 'sha256:image' }))
      .resolves.toEqual({ attachment: ref, data: 'AQID' })
    expect(readImage).toHaveBeenCalledWith(ref)
    await server.shutdown()
  })

  it('refuses an attachment the session never referenced', async () => {
    const { server, readImage } = imageRuntime([])
    // 未引用即未授权：不得触达存储层
    await expectSdkErrorAsync(
      server.readAttachment({ sessionId: 'image', attachmentId: 'sha256:image' }),
      'attachment-invalid',
    )
    expect(readImage).not.toHaveBeenCalled()
    await server.shutdown()
  })
})

describe('SDK P3 control plane: model select', () => {
  it('applies a validated route to the next request and records it durably', async () => {
    const { ctx, server } = await runtime()
    await server.prompt({ sessionId: 'routed', contentBlocks: [{ type: 'text', text: 'go' }] })
    await server.wait({ sessionId: 'routed' })

    await expect(server.selectModel({ sessionId: 'routed', provider: 'alt', model: 'alt-model' }))
      .resolves.toEqual({ selected: { provider: 'alt', model: 'alt-model' } })
    await server.prompt({ sessionId: 'routed', contentBlocks: [{ type: 'text', text: 'again' }] })
    await server.wait({ sessionId: 'routed' })

    const agent = ctx.agents.get(SessionId('routed'))
    if (agent === undefined) throw new Error('session was not attached')
    // 下一轮请求头真正走了新路由，而不是只记了一笔选择
    expect(agent.session.requestHeader()?.config.provider).toBe('alt')
    expect(agent.session.requestHeader()?.config.model).toBe('alt-model')
    const selections = agent.session.snapshotEvents().filter(event => event.type === 'model/selection')
    expect(selections.at(-1)?.type === 'model/selection' ? selections.at(-1)?.data : undefined)
      .toMatchObject({ provider: 'alt', model: 'alt-model' })
  })

  it('rejects a route the runtime cannot serve', async () => {
    const { server } = await runtime()
    await server.prompt({ sessionId: 'unroutable', contentBlocks: [{ type: 'text', text: 'go' }] })
    await server.wait({ sessionId: 'unroutable' })
    await expectSdkErrorAsync(
      server.selectModel({ sessionId: 'unroutable', provider: 'nope', model: 'nope' }),
      'model-unavailable',
    )
  })
})

describe('SDK P3 control plane: sync, list, create and fork', () => {
  it('aligns a reconnecting client with one cut, tail and projection baseline', async () => {
    const { ctx, server } = await runtime()
    await server.prompt({ sessionId: 'aligned', contentBlocks: [{ type: 'text', text: 'go' }] })
    await server.wait({ sessionId: 'aligned' })
    const agent = ctx.agents.get(SessionId('aligned'))
    if (agent === undefined) throw new Error('session was not attached')

    const synced = await server.sync({ sessionId: 'aligned', fromSeq: 0, limit: 1000 })
    expect(synced.nextSeq).toBe(Number(agent.session.seq))
    expect(synced.history.events.length).toBeGreaterThan(0)
    expect(synced.history.hasMore).toBe(false)
    expect(synced.projections?.asOfSeq).toBe(Number(agent.session.seq) - 1)
  })

  it('creates one session identity and lists it with its title', async () => {
    const { ctx, server, notifications } = await runtime()
    await expect(server.create({ sessionId: 'listed' })).resolves.toEqual({ sessionId: 'listed' })
    expect(ctx.agents.get(SessionId('listed'))).toBeDefined()
    expect(notifications.some(entry => entry.method === 'session.created'
      && entry.params.sessionId === 'listed')).toBe(true)

    await server.rename({ sessionId: 'listed', title: 'Listed Chat' })
    const listed = await server.listSessions({ limit: 10 })
    const row = listed.sessions.find(session => session.sessionId === 'listed')
    expect(row).toMatchObject({ sessionId: 'listed', title: 'Listed Chat', live: true })

    await server.disposeSession({ sessionId: 'listed' })
    expect(notifications.some(entry => entry.method === 'session.disposed'
      && entry.params.sessionId === 'listed')).toBe(true)
    await expect(server.listSessions({ limit: 0 })).rejects.toThrow(/1\.\.500/)
  })

  it('forks a completed turn into an owned child session', async () => {
    const { ctx, server } = await runtime()
    await server.prompt({ sessionId: 'forked', contentBlocks: [{ type: 'text', text: 'go' }] })
    await server.wait({ sessionId: 'forked' })

    const forked = await server.forkSession({ sessionId: 'forked' })
    expect(forked.sessionId).not.toBe('forked')
    const child = ctx.agents.get(SessionId(forked.sessionId))
    if (child === undefined) throw new Error('forked child was not created')
    expect(child.session.header.parentSession).toBe('forked')
    expect(child.session.header.isSeeded).toBe(true)
    // 分叉出的子会话属于本客户端：可直接继续对话
    await server.prompt({ sessionId: forked.sessionId, contentBlocks: [{ type: 'text', text: 'continue' }] })
    await server.wait({ sessionId: forked.sessionId })

    await expectSdkErrorAsync(server.forkSession({ sessionId: 'forked', atSeq: 9999 }), 'fork-unavailable')
  })
})

describe('SDK P3 control plane: session search', () => {
  it('finds a session by message text and rejects an empty query', async () => {
    const { server } = await runtime()
    await server.prompt({ sessionId: 'searchable', contentBlocks: [{ type: 'text', text: 'photon torpedo' }] })
    await server.wait({ sessionId: 'searchable' })

    const found = await server.searchSessions({ query: 'photon' })
    expect(found.items.map(item => item.sessionId)).toContain('searchable')
    await expect(server.searchSessions({ query: '   ' })).rejects.toThrow(/non-empty query/)
  })

  it('reports search as unavailable without a query engine', async () => {
    const { server } = await runtime({ query: false })
    await expectSdkErrorAsync(server.searchSessions({ query: 'anything' }), 'search-unavailable')
  })
})

describe('SDK P3 control plane: late question answers', () => {
  it('reports a question that is no longer continued instead of failing', async () => {
    const { server } = await runtime()
    await server.prompt({ sessionId: 'answered', contentBlocks: [{ type: 'text', text: 'go' }] })
    await server.wait({ sessionId: 'answered' })
    // 没有继续中的提问：迟到的答案只是无人接收，不是错误
    await expect(server.answerQuestion({
      sessionId: 'answered',
      callId: 'call-missing',
      answer: { answers: [{ id: 'q1', selected: ['yes'] }] },
    })).resolves.toEqual({ accepted: false, state: 'settled' })
    await expect(server.handleRequest('session/answer', { sessionId: 'answered' }))
      .rejects.toThrow(/non-empty callId/)
  })

  it('reports late answers as unavailable without a question service', async () => {
    const { server } = await runtime({ questions: false })
    await expectSdkErrorAsync(server.answerQuestion({
      sessionId: 'noquestions',
      callId: 'call-1',
      answer: { answers: [] },
    }), 'answer-unavailable')
  })
})

describe('SDK P3 control plane: unknown sessions', () => {
  it('distinguishes an absent stored session from an unreadable one', async () => {
    const { ctx, server } = await runtime()
    // 读取未存储会话：明确 not-found，而不是底层驱动的原始报错
    await expectSdkErrorAsync(server.forkSession({ sessionId: 'ghost' }), 'not-attached')
    await expectSdkErrorAsync(
      server.readAttachment({ sessionId: 'ghost', attachmentId: 'sha256:x' }),
      'not-attached',
    )
    // 未创建任何 agent：冷读失败不得顺手造出一个会话
    expect(ctx.agents.get(SessionId('ghost'))).toBeUndefined()
  })
})
