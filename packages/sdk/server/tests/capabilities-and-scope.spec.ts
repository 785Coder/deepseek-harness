import { mkdir, mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import { ToolCallId } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'
import * as JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import SqliteSessionQueryEngine from '@deepseek-ai/dsh-session-query-sqlite'
import UserQuestionService from '@deepseek-ai/dsh-user-questions'
import type { JsonRpcTransportPeer } from '@deepseek-ai/dsh-sdk-protocol'
import { MockAdapter, textResponse } from '../../../core/agent-loop/tests/mock-adapter.ts'
import { HarnessSdkJsonRpcServer } from '../src/index.ts'
import { expectSdkErrorAsync } from './sdk-error.ts'

const cleanups: (() => Promise<void>)[] = []
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
})

interface Requested { method: string; params: Record<string, unknown> }

async function runtime(options: { cwd?: string; store?: string; answer?: unknown } = {}) {
  // Storage root and working root are independent: a shared persistence home
  // serves several deployments, which is exactly the leak this suite guards.
  const store: string = options.store ?? await mkdtemp(join(tmpdir(), 'sdk-scope-store-'))
  if (options.store === undefined) cleanups.push(() => rm(store, { recursive: true, force: true }))
  let root: string = options.cwd ?? store
  if (options.cwd !== undefined) {
    root = options.cwd
    cleanups.push(async () => { await mkdir(root, { recursive: true }) })
  }
  const ctx: Context = new Context()
  cleanups.push(() => ctx.fiber.dispose())
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(JsonlSessionPersistence, { root: store, compression: 'none' })
  await ctx.plugin(AgentLoop, { agents: [] })
  await ctx.plugin(SqliteSessionQueryEngine, { path: join(store, 'query.db') })
  await ctx.plugin(UserQuestionService)
  ctx.llm.registerAdapter(['mock'], new MockAdapter([textResponse('first')]))
  const requested: Requested[] = []
  const transport: JsonRpcTransportPeer = {
    request: async (method, params) => {
      requested.push({ method, params: params as Record<string, unknown> })
      if (options.answer === undefined) throw new Error('unexpected server request')
      return options.answer
    },
    notify: () => {},
  }
  const server = new HarnessSdkJsonRpcServer(ctx, transport)
  cleanups.push(() => server.shutdown().then(() => {}))
  const initialized = await server.initialize({ cwd: root, provider: 'mock', model: 'mock' })
  return { ctx, server, requested, root, initialized }
}

describe('SDK capabilities', () => {
  it('advertises the methods and notifications this build dispatches', async () => {
    const { server, initialized } = await runtime()
    expect(initialized.capabilities.requests).toContain('session/answer')
    expect(initialized.capabilities.notifications).toContain('session.assistant-stream')
    // 每条广告出来的请求都要真的可分发：不许出现"列了但没实现"
    for (const method of initialized.capabilities.requests) {
      if (method === 'initialize' || method === 'shutdown') continue
      const rejection = server.handleRequest(method, {}).then(() => undefined, (error: unknown) => error)
      const error = await rejection
      expect(String(error)).not.toMatch(/unknown DeepSeek Harness SDK runtime method|unknown SDK job method/)
    }
  })
})

describe('SDK working-root scope', () => {
  it('hides sessions stored under another working root', async () => {
    const mine = await mkdtemp(join(tmpdir(), 'sdk-scope-a-'))
    const other = await mkdtemp(join(tmpdir(), 'sdk-scope-b-'))
    const store = await mkdtemp(join(tmpdir(), 'sdk-scope-store-'))
    for (const dir of [mine, other, store]) cleanups.push(() => rm(dir, { recursive: true, force: true }))

    // 同一持久化根下有两个工作根：本运行时只应看见自己的
    const first = await runtime({ cwd: mine, store })
    await first.server.create({ sessionId: 'mine' })
    await first.server.prompt({ sessionId: 'mine', contentBlocks: [{ type: 'text', text: 'inside scope' }] })
    await first.server.wait({ sessionId: 'mine' })
    const listed = await first.server.listSessions({ limit: 50 })
    expect(listed.sessions.map(row => row.sessionId)).toEqual(['mine'])
    const row = listed.sessions[0]!
    expect(row).toMatchObject({ sessionId: 'mine', blank: false })
    expect(row.updatedAt).toBeGreaterThanOrEqual(row.createdAt)
    expect((await first.server.searchSessions({ query: 'inside' })).items.map(item => item.sessionId))
      .toEqual(['mine'])
    await first.server.shutdown()

    // 另一个工作根读同一持久化根：不得看见上面的会话
    const second = await runtime({ cwd: other, store })
    expect((await second.server.listSessions({ limit: 50 })).sessions).toEqual([])
    expect((await second.server.searchSessions({ query: 'inside' })).items).toEqual([])
    // 读面按工作根收敛；写入面（attach/fork/attachment）只对已挂载会话开放。
    // 已知 id 的 create 仍可显式接管——这是 SDK 单租户前提下的既定边界，见设计 §8.2。
    await expectSdkErrorAsync(second.server.forkSession({ sessionId: 'mine' }), 'not-attached')
    await expectSdkErrorAsync(
      second.server.readAttachment({ sessionId: 'mine', attachmentId: 'sha256:x' }),
      'not-attached',
    )
  })
})

describe('SDK history alignment', () => {
  it('closes a message group instead of ending on orphaned sources', async () => {
    const { server } = await runtime()
    await server.prompt({ sessionId: 'aligned', contentBlocks: [{ type: 'text', text: 'go' }] })
    await server.wait({ sessionId: 'aligned' })

    const seqPage = await server.history({ sessionId: 'aligned', limit: 2 })
    const messagePage = await server.history({ sessionId: 'aligned', limit: 2, align: 'message' })
    expect(seqPage.events).toHaveLength(2)
    expect(messagePage.events.filter(event => event.type === 'user/message' || event.type === 'assistant/message')).toHaveLength(2)
    expect(messagePage.hasMore).toBe(true)
    expect(messagePage.nextCursor).toBe(Number(messagePage.events[0]?.seq))
  })
})

describe('SDK timed question relay', () => {
  it('marks a foreground timed wait so the client can count down', async () => {
    const { ctx, server, requested } = await runtime({
      answer: { answers: [{ id: 'q1', selected: ['Yes'] }] },
    })
    await server.create({ sessionId: 'timed' })
    const agent = ctx.agents.get(SessionId('timed'))
    if (agent === undefined) throw new Error('session was not created')
    const questions = ctx.get('userQuestions')
    if (questions === undefined) throw new Error('question service was not mounted')

    const answered = await questions.askTimed(
      { questions: [{ id: 'q1', question: 'Continue?', options: [{ label: 'Yes' }, { label: 'No' }] }], agent },
      ToolCallId('call-timed'),
      5000,
    )
    expect(answered).toEqual({ answers: [{ id: 'q1', selected: ['Yes'] }] })
    // 中继把 timed 标记带给客户端：客户端据此自行倒计时，超时后改走 session/answer
    expect(requested[0]?.method).toBe('session/question')
    expect(requested[0]?.params).toMatchObject({
      sessionId: 'timed',
      callId: 'call-timed',
      timed: true,
    })
  })
})
