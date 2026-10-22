import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import { SessionId } from '@deepseek-ai/dsh-session'
import * as JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import * as AskUser from '@deepseek-ai/dsh-tool-ask-user'
import UserQuestionService from '@deepseek-ai/dsh-user-questions'
import type { JsonRpcTransportPeer, SessionQuestionParams, SessionQuestionWaitResult } from '@deepseek-ai/dsh-sdk-protocol'
import { MockAdapter, textResponse, toolCallResponse } from '../../../core/agent-loop/tests/mock-adapter.ts'
import { HarnessSdkJsonRpcServer } from '../src/index.ts'

afterEach(() => { vi.restoreAllMocks() })

it('claims the Host countdown, releases without answering, then admits a late reply as a new turn', async () => {
  const root: string = await mkdtemp(join(tmpdir(), 'sdk-timed-question-'))
  const ctx: Context = new Context()
  const asked = Promise.withResolvers<SessionQuestionParams>()
  const notifications: { method: string; params: object }[] = []
  let server: HarnessSdkJsonRpcServer | undefined
  try {
    await mountAgentLoopTestDependencies(ctx)
    await ctx.plugin(JsonlSessionPersistence, { root, compression: 'none' })
    await ctx.plugin(UserQuestionService)
    await ctx.plugin(AskUser, { mode: 'timed', timeout: 1 })
    await ctx.plugin(AgentLoop, { agents: [] })
    ctx.llm.registerAdapter(['mock'], new MockAdapter([
      toolCallResponse('ask-timed', 'ask_user_question', { questions: [{ id: 'q', question: 'Continue?' }], timeout: 1 }),
      textResponse('continued without an answer'), textResponse('late answer admitted'),
    ]))
    const peer: JsonRpcTransportPeer = {
      notify: (method, params) => { notifications.push({ method, params: params as object }) },
      request: async (method, params, signal) => {
        if (method !== 'session/question') throw new Error('unexpected request')
        asked.resolve(params as SessionQuestionParams)
        return new Promise((_resolve, reject) => {
          if (signal?.aborted === true) reject(signal.reason instanceof Error ? signal.reason : new Error(String(signal.reason)))
          else signal?.addEventListener('abort', () => { reject(signal.reason instanceof Error ? signal.reason : new Error(String(signal.reason))) }, { once: true })
        })
      },
    }
    server = new HarnessSdkJsonRpcServer(ctx, peer)
    await server.initialize({ cwd: root, provider: 'mock', model: 'model' })
    await server.prompt({ sessionId: 'timed', contentBlocks: [{ type: 'text', text: 'go' }] })
    const question = await asked.promise
    expect(question.timed).toBe(true)
    if (question.callId === undefined) throw new Error('timed request has no call id')
    const params = { sessionId: 'timed', callId: String(question.callId) }
    const [first, second] = await Promise.all([
      server.handleRequest('session/question/wait', { ...params, action: 'attach' }),
      server.handleRequest('session/question/wait', { ...params, action: 'attach' }),
    ]) as SessionQuestionWaitResult[]
    expect(first?.remainingMs).toBeGreaterThan(0)
    expect(second?.remainingMs).toBeGreaterThan(0)
    const afterDeadline = Date.now() + 2000
    vi.spyOn(Date, 'now').mockReturnValue(afterDeadline)
    await server.handleRequest('session/question/wait', { ...params, action: 'release' })
    await server.wait({ sessionId: 'timed' })
    expect(notifications).toContainEqual({ method: 'session.question-state', params: { ...params, state: 'continued' } })
    const answer = { answers: [{ id: 'q', selected: [], custom: 'yes, continue' }] }
    expect(await server.answerQuestion({ ...params, answer })).toMatchObject({ accepted: true })
    await server.wait({ sessionId: 'timed' })
    const events = ctx.agents.get(SessionId('timed'))?.session.snapshotEvents() ?? []
    const late = events.filter(event => event.type === 'user/message' && event.data.source.kind === 'user-question-reply')
    expect(late).toHaveLength(1)
    const message = late[0]
    if (message?.type !== 'user/message') throw new Error('late user message was not admitted')
    const text = message.data.content.filter(block => block.type === 'text').map(block => block.text).join('')
    expect(text).toContain('yes, continue')
  } finally {
    await server?.shutdown()
    await ctx.fiber.dispose()
    await rm(root, { recursive: true, force: true })
  }
}, 10_000)
