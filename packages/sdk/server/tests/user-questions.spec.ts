import type { Agent } from '@deepseek-ai/dsh-agent'
import type { Context } from '@deepseek-ai/cordis'
import { SessionId } from '@deepseek-ai/dsh-session'
import type { JsonRpcTransportPeer } from '@deepseek-ai/dsh-sdk-protocol'
import type { AskUserQuestionAnswer, AskUserQuestionRequest } from '@deepseek-ai/dsh-user-questions'
import { describe, expect, it, vi } from 'vitest'

import { HarnessSdkJsonRpcServer } from '../src/index.ts'

class RecordingTransport implements JsonRpcTransportPeer {
  requests: { method: string; params: object; signal?: AbortSignal }[] = []

  async request(method: string, params: object, signal?: AbortSignal): Promise<unknown> {
    // exactOptionalPropertyTypes 下不可把 undefined 写进可选属性：signal 缺省时不记录。
    this.requests.push({ method, params, ...(signal === undefined ? {} : { signal }) })
    return { answers: [{ id: 'q1', selected: ['yes'] }] }
  }

  notify(_method: string, _params?: object): void {}
}

interface FakeContext {
  ctx: Context
  /** Recorded event listeners by event name (each `ctx.on` call overwrites the previous). */
  handlers: Map<string, (...args: never[]) => unknown>
  on: ReturnType<typeof vi.fn>
}

/**
 * Hand-built context matching every `ctx.on` / `ctx.get` / `ctx.agents`
 * access the server makes. `agents.create` / `agents.get` serve the
 * pre-registered live agents map; the 'llm' key provides the adapter route
 * `initialize` resolves. Ownership comes from `prompt`, which registers the
 * session in `ownedSessionIds`.
 */
function makeFakeContext(agents: ReadonlyMap<string, Agent>): FakeContext {
  const handlers = new Map<string, (...args: never[]) => unknown>()
  const on = vi.fn((name: string, handler: (...args: never[]) => unknown) => {
    handlers.set(name, handler)
    return vi.fn(() => undefined)
  })
  const ctx = {
    on,
    agents: {
      create: vi.fn(async (options: { sessionId: SessionId }) => {
        const agent = agents.get(String(options.sessionId))
        if (agent === undefined) throw new Error(`no fake agent registered for session ${String(options.sessionId)}`)
        return { agent, dispose: vi.fn(() => Promise.resolve()) }
      }),
      get: (id: SessionId) => agents.get(String(id)),
    },
    get: (name: string) => {
      if (name === 'userQuestions') return {}
      if (name === 'llm') return { listProviders: () => [{ id: 'mock', name: 'Mock' }], resolveCallConfig: vi.fn(async (config: unknown) => config) }
      return undefined
    },
  } as unknown as Context
  return { ctx, handlers, on }
}

function makeAgent(id: string): Agent {
  const sessionId = SessionId(id)
  return {
    id: sessionId,
    session: { id: sessionId },
    followup: vi.fn<Agent['followup']>(),
  } as unknown as Agent
}

type QuestionHandler = (
  request: AskUserQuestionRequest,
  next: () => Promise<AskUserQuestionAnswer>,
) => Promise<AskUserQuestionAnswer>

function questionHandlerOf(fake: FakeContext): QuestionHandler {
  const handler = fake.handlers.get('user-questions/request')
  if (handler === undefined) throw new Error('user-questions/request handler was not registered')
  return handler as unknown as QuestionHandler
}

/** Initialize the handshake so `prompt` passes the server's readiness guard. */
async function initializeServer(server: HarnessSdkJsonRpcServer): Promise<void> {
  await server.initialize({ cwd: '.', provider: 'mock', model: 'mock' })
}

describe('HarnessSdkJsonRpcServer user-questions relay', () => {
  it('relays a user-questions/request for an owned session over the wire', async () => {
    const agent = makeAgent('main')
    const fake = makeFakeContext(new Map([['main', agent]]))
    const transport = new RecordingTransport()
    const server = new HarnessSdkJsonRpcServer(fake.ctx, transport)
    await initializeServer(server)
    // createSession 经 prompt 把 'main' 计入 ownedSessionIds。
    await server.prompt({ sessionId: 'main', contentBlocks: [{ type: 'text', text: 'hello' }] })

    const handler = questionHandlerOf(fake)
    const signal = new AbortController().signal
    const next = vi.fn<() => Promise<AskUserQuestionAnswer>>()
    const request: AskUserQuestionRequest = {
      questions: [{ id: 'q1', question: 'Continue?', options: [{ label: 'yes' }, { label: 'no' }] }],
      agent,
      signal,
    }
    const answer = await handler(request, next)

    expect(transport.requests).toHaveLength(1)
    expect(transport.requests[0]?.method).toBe('session/question')
    expect(transport.requests[0]?.params).toEqual({ sessionId: 'main', questions: request.questions })
    // The caller's abort signal passes through to the wire request.
    expect(transport.requests[0]?.signal).toBe(signal)
    // 归属请求不走 next()，返回值即 transport 的应答。
    expect(next).not.toHaveBeenCalled()
    expect(answer).toEqual({ answers: [{ id: 'q1', selected: ['yes'] }] })

    await server.shutdown()
  })

  it('delegates via next() for a session the server does not own', async () => {
    const ghost = makeAgent('ghost')
    const fake = makeFakeContext(new Map())
    const transport = new RecordingTransport()
    const server = new HarnessSdkJsonRpcServer(fake.ctx, transport)
    // 'ghost' 从未 prompt，不在 ownedSessionIds 内。
    const next = vi.fn<() => Promise<AskUserQuestionAnswer>>()
    const answer = await questionHandlerOf(fake)({
      questions: [{ id: 'q1', question: 'Continue?' }],
      agent: ghost,
    }, next)

    expect(next).toHaveBeenCalledOnce()
    expect(answer).toBeUndefined()
    expect(transport.requests).toHaveLength(0)

    await server.shutdown()
  })

  it('delegates via next() when no agent is supplied', async () => {
    const fake = makeFakeContext(new Map())
    const server = new HarnessSdkJsonRpcServer(fake.ctx, new RecordingTransport())
    const next = vi.fn<() => Promise<AskUserQuestionAnswer>>()
    const answer = await questionHandlerOf(fake)({ questions: [{ id: 'q1', question: 'Continue?' }] }, next)

    expect(next).toHaveBeenCalledOnce()
    expect(answer).toBeUndefined()

    await server.shutdown()
  })

  it('registers the relay and disposes it on server shutdown', async () => {
    const fake = makeFakeContext(new Map())
    const server = new HarnessSdkJsonRpcServer(fake.ctx, new RecordingTransport())

    expect(fake.handlers.has('user-questions/request')).toBe(true)
    expect(fake.on).toHaveBeenCalledWith('user-questions/request', expect.any(Function))

    await server.shutdown()
  })
})
