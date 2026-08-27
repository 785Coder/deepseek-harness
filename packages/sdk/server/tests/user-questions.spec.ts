import type { Agent } from '@deepseek-ai/dsh-agent'
import type { Context } from '@deepseek-ai/cordis'
import { SessionId } from '@deepseek-ai/dsh-session'
import type { JsonRpcTransportPeer } from '@deepseek-ai/dsh-sdk-protocol'
import {
  UserQuestionError,
  type AskUserQuestionRequest,
  type UserQuestionProvider,
  type UserQuestionService,
} from '@deepseek-ai/dsh-user-questions'
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

function fakeContext(userQuestions: unknown): Context {
  return {
    on: vi.fn(() => () => undefined),
    get: (name: string) => (name === 'userQuestions' ? userQuestions : undefined),
  } as unknown as Context
}

describe('HarnessSdkJsonRpcServer user-questions relay', () => {
  it('registers a provider that relays ask() as a session/question request', async () => {
    const transport = new RecordingTransport()
    let captured: UserQuestionProvider | undefined
    const registerProvider = vi.fn((provider: UserQuestionProvider) => {
      captured = provider
      return () => undefined
    })
    const userQuestions = { registerProvider } as unknown as Pick<UserQuestionService, 'registerProvider'>
    const server = new HarnessSdkJsonRpcServer(fakeContext(userQuestions), transport)

    expect(registerProvider).toHaveBeenCalledOnce()
    if (captured === undefined) throw new Error('provider was not registered')

    const signal = new AbortController().signal
    const request: AskUserQuestionRequest = {
      questions: [{ id: 'q1', question: 'Continue?', options: [{ label: 'yes' }, { label: 'no' }] }],
      agent: { id: SessionId('main') } as unknown as Agent,
      signal,
    }
    const answer = await captured.ask(request)

    expect(transport.requests).toHaveLength(1)
    expect(transport.requests[0]?.method).toBe('session/question')
    expect(transport.requests[0]?.params).toEqual({
      sessionId: 'main',
      questions: request.questions,
    })
    // The caller's abort signal passes through to the wire request.
    expect(transport.requests[0]?.signal).toBe(signal)
    expect(answer).toEqual({ answers: [{ id: 'q1', selected: ['yes'] }] })

    await server.shutdown()
  })

  it('rejects ask without an agent-owned session with ASK_MISSING_AGENT', async () => {
    const transport = new RecordingTransport()
    let captured: UserQuestionProvider | undefined
    const userQuestions = {
      registerProvider: vi.fn((provider: UserQuestionProvider) => {
        captured = provider
        return () => undefined
      }),
    } as unknown as Pick<UserQuestionService, 'registerProvider'>
    const server = new HarnessSdkJsonRpcServer(fakeContext(userQuestions), transport)
    if (captured === undefined) throw new Error('provider was not registered')

    const failure = await captured.ask({ questions: [{ id: 'q1', question: 'Continue?' }] }).then(
      () => { throw new Error('ask unexpectedly succeeded') },
      (error: unknown) => error,
    )

    expect(failure).toBeInstanceOf(UserQuestionError)
    expect(failure).toMatchObject({
      code: 'ASK_MISSING_AGENT',
      message: 'sdk user interaction requires an agent-owned session',
    })
    // Nothing is sent over the wire when there is no session to attribute the question to.
    expect(transport.requests).toHaveLength(0)

    await server.shutdown()
  })

  it('does not register a provider when userQuestions is not mounted', async () => {
    const registerProvider = vi.fn()
    const ctx = {
      on: vi.fn(() => () => undefined),
      get: () => undefined,
    } as unknown as Context
    // The userQuestions service object is never reachable through ctx.get.
    const server = new HarnessSdkJsonRpcServer(ctx, new RecordingTransport())

    expect(registerProvider).not.toHaveBeenCalled()

    await server.shutdown()
  })

  it('disposes the registered provider on server shutdown', async () => {
    const disposer = vi.fn()
    const userQuestions = {
      registerProvider: vi.fn(() => disposer),
    } as unknown as Pick<UserQuestionService, 'registerProvider'>
    const server = new HarnessSdkJsonRpcServer(fakeContext(userQuestions), new RecordingTransport())

    await server.shutdown()

    expect(disposer).toHaveBeenCalledOnce()
  })
})
