import type { Agent } from '@deepseek-ai/dsh-agent'
import type { Context } from '@deepseek-ai/cordis'
import { ToolCallId } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'
import type { JsonRpcTransportPeer } from '@deepseek-ai/dsh-sdk-protocol'
import type { ApprovalRequest } from '@deepseek-ai/dsh-user-approval'
import type { ApprovalOutcome } from '@deepseek-ai/dsh-user-approval/types'
import { describe, expect, it, vi } from 'vitest'

import { HarnessSdkJsonRpcServer } from '../src/index.ts'

class RecordingTransport implements JsonRpcTransportPeer {
  requests: { method: string; params: object; signal?: AbortSignal }[] = []

  constructor(private readonly result: ApprovalOutcome = 'allowed-once') {}

  async request(method: string, params: object, signal?: AbortSignal): Promise<unknown> {
    // exactOptionalPropertyTypes 下不可把 undefined 写进可选属性：signal 缺省时不记录。
    this.requests.push({ method, params, ...(signal === undefined ? {} : { signal }) })
    return this.result
  }

  notify(_method: string, _params?: object): void {}
}

interface FakeContext {
  ctx: Context
  /** Recorded event listeners by event name (each `ctx.on` call overwrites the previous). */
  handlers: Map<string, (...args: never[]) => unknown>
  /** Disposers returned by every `ctx.on` call, in registration order. */
  disposers: ReturnType<typeof vi.fn>[]
  on: ReturnType<typeof vi.fn>
}

/**
 * Hand-built context matching every `ctx.on` / `ctx.get` / `ctx.agents`
 * access the server makes. `agents.create` serves the pre-registered live
 * agents map; `agents.get` mirrors the same map so `prompt`'s liveness check
 * passes. `approvalMounted=false` makes `ctx.get('approval')` return undefined
 * (the seam is not composed).
 */
function makeFakeContext(agents: ReadonlyMap<string, Agent>, approvalMounted: boolean): FakeContext {
  const handlers = new Map<string, (...args: never[]) => unknown>()
  const disposers: ReturnType<typeof vi.fn>[] = []
  const on = vi.fn((name: string, handler: (...args: never[]) => unknown) => {
    handlers.set(name, handler)
    const disposer = vi.fn(() => undefined)
    disposers.push(disposer)
    return disposer
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
      if (name === 'approval') return approvalMounted ? {} : undefined
      if (name === 'llm') {
        return { listProviders: () => [{ id: 'mock', name: 'Mock' }], resolveCallConfig: vi.fn(async (config: unknown) => config) }
      }
      return undefined
    },
  } as unknown as Context
  return { ctx, handlers, disposers, on }
}

function makeAgent(id: string): Agent {
  const sessionId = SessionId(id)
  return {
    id: sessionId,
    session: { id: sessionId },
    followup: vi.fn<Agent['followup']>(),
  } as unknown as Agent
}

type ApprovalRequestHandler = (
  req: ApprovalRequest,
  next: () => Promise<ApprovalOutcome>,
) => Promise<ApprovalOutcome>

function approvalHandlerOf(fake: FakeContext): ApprovalRequestHandler {
  const handler = fake.handlers.get('approval/request')
  if (handler === undefined) throw new Error('approval/request handler was not registered')
  return handler as unknown as ApprovalRequestHandler
}

interface CreatedSession {
  id: SessionId
  header: { parentSession?: SessionId }
}

function createdHandlerOf(fake: FakeContext): (session: CreatedSession) => void {
  const handler = fake.handlers.get('session/created')
  if (handler === undefined) throw new Error('session/created handler was not registered')
  return handler as unknown as (session: CreatedSession) => void
}

const fallbackNext = (): Promise<ApprovalOutcome> => Promise.resolve('unavailable')

/** Initialize the handshake so `prompt` passes the server's readiness guard. */
async function initializeServer(server: HarnessSdkJsonRpcServer): Promise<void> {
  await server.initialize({ cwd: '.', provider: 'mock', model: 'mock' })
}

describe('HarnessSdkJsonRpcServer approval relay', () => {
  it('relays an approval request for an owned session over the wire', async () => {
    const agent = makeAgent('main')
    const fake = makeFakeContext(new Map([['main', agent]]), true)
    const transport = new RecordingTransport()
    const server = new HarnessSdkJsonRpcServer(fake.ctx, transport)
    // createSession 经 prompt 把 'main' 计入 ownedSessionIds。
    await initializeServer(server)
    await server.prompt({ sessionId: 'main', contentBlocks: [{ type: 'text', text: 'hello' }] })

    expect(fake.on).toHaveBeenCalledWith('approval/request', expect.any(Function))
    const handler = approvalHandlerOf(fake)
    const signal = new AbortController().signal
    const next = vi.fn<() => Promise<ApprovalOutcome>>(fallbackNext)
    const outcome = await handler({
      agent,
      toolName: 'bash',
      callId: ToolCallId('c1'),
      reason: 'escalate',
      signal,
    }, next)

    expect(transport.requests).toHaveLength(1)
    expect(transport.requests[0]?.method).toBe('session/approval')
    expect(transport.requests[0]?.params).toEqual({
      sessionId: 'main',
      toolName: 'bash',
      callId: 'c1',
      reason: 'escalate',
    })
    // 调用方的 abort signal 透传到 wire 请求。
    expect(transport.requests[0]?.signal).toBe(signal)
    // 归属请求不走 next()，返回值即 transport 的 outcome。
    expect(next).not.toHaveBeenCalled()
    expect(outcome).toBe('allowed-once')

    await server.shutdown()
  })

  it('omits callId and reason from the wire params when absent', async () => {
    const agent = makeAgent('main')
    const fake = makeFakeContext(new Map([['main', agent]]), true)
    const transport = new RecordingTransport()
    const server = new HarnessSdkJsonRpcServer(fake.ctx, transport)
    await initializeServer(server)
    await server.prompt({ sessionId: 'main', contentBlocks: [{ type: 'text', text: 'hello' }] })

    const outcome = await approvalHandlerOf(fake)({ agent, toolName: 'bash' }, vi.fn(fallbackNext))

    expect(transport.requests).toHaveLength(1)
    // exactOptionalPropertyTypes 语义：缺省的可选键不写入 params。
    const params = transport.requests[0]?.params as { sessionId: string; toolName: string }
    expect(params).toEqual({ sessionId: 'main', toolName: 'bash' })
    expect(Object.keys(params)).toEqual(['sessionId', 'toolName'])
    expect(outcome).toBe('allowed-once')

    await server.shutdown()
  })

  it('delegates to next() for a session the server does not own', async () => {
    const ghost = makeAgent('ghost')
    const fake = makeFakeContext(new Map(), true)
    const transport = new RecordingTransport()
    const server = new HarnessSdkJsonRpcServer(fake.ctx, transport)
    // 'ghost' 从未 prompt，不在 ownedSessionIds 内。

    const next = vi.fn<() => Promise<ApprovalOutcome>>(fallbackNext)
    const outcome = await approvalHandlerOf(fake)({ agent: ghost, toolName: 'bash' }, next)

    expect(next).toHaveBeenCalledOnce()
    expect(outcome).toBe('unavailable')
    expect(transport.requests).toHaveLength(0)

    await server.shutdown()
  })

  it('relays for a child session whose parent session is owned', async () => {
    const main = makeAgent('main')
    const child = makeAgent('child')
    const fake = makeFakeContext(new Map([['main', main], ['child', child]]), true)
    const transport = new RecordingTransport()
    const server = new HarnessSdkJsonRpcServer(fake.ctx, transport)
    await initializeServer(server)
    await server.prompt({ sessionId: 'main', contentBlocks: [{ type: 'text', text: 'hello' }] })

    // 驱动 session/created：父 session 已归属 → 子 session 继承归属。
    createdHandlerOf(fake)({ id: SessionId('child'), header: { parentSession: SessionId('main') } })

    const outcome = await approvalHandlerOf(fake)({ agent: child, toolName: 'bash' }, vi.fn(fallbackNext))

    expect(transport.requests).toHaveLength(1)
    expect(transport.requests[0]?.params).toEqual({ sessionId: 'child', toolName: 'bash' })
    expect(outcome).toBe('allowed-once')

    await server.shutdown()
  })

  it('does not inherit ownership when the parent session is not owned', async () => {
    const main = makeAgent('main')
    const orphanChild = makeAgent('orphan-child')
    const fake = makeFakeContext(new Map([['main', main], ['orphan-child', orphanChild]]), true)
    const transport = new RecordingTransport()
    const server = new HarnessSdkJsonRpcServer(fake.ctx, transport)
    await initializeServer(server)
    await server.prompt({ sessionId: 'main', contentBlocks: [{ type: 'text', text: 'hello' }] })
    // 父 session 'ghost' 不是归属的 → 子 session 不继承。
    createdHandlerOf(fake)({ id: SessionId('orphan-child'), header: { parentSession: SessionId('ghost') } })

    const next = vi.fn<() => Promise<ApprovalOutcome>>(fallbackNext)
    const outcome = await approvalHandlerOf(fake)({ agent: orphanChild, toolName: 'bash' }, next)

    expect(next).toHaveBeenCalledOnce()
    expect(outcome).toBe('unavailable')
    expect(transport.requests).toHaveLength(0)

    await server.shutdown()
  })

  it('does not register the relay when approval is not mounted', async () => {
    const fake = makeFakeContext(new Map(), false)
    const server = new HarnessSdkJsonRpcServer(fake.ctx, new RecordingTransport())

    expect(fake.handlers.has('approval/request')).toBe(false)
    expect(fake.on).not.toHaveBeenCalledWith('approval/request', expect.any(Function))

    await server.shutdown()
  })

  it('disposes the approval subscription on server shutdown', async () => {
    const agent = makeAgent('main')
    const fake = makeFakeContext(new Map([['main', agent]]), true)
    const server = new HarnessSdkJsonRpcServer(fake.ctx, new RecordingTransport())
    await initializeServer(server)
    await server.prompt({ sessionId: 'main', contentBlocks: [{ type: 'text', text: 'hello' }] })

    await server.shutdown()

    // 5 个注册（session/event、agent/status、session/created、subagent/end、approval/request）
    // 的 disposer 全部被调用。
    expect(fake.disposers).toHaveLength(5)
    for (const disposer of fake.disposers) {
      expect(disposer).toHaveBeenCalledOnce()
    }
  })
})
