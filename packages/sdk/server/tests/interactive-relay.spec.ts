import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Context } from '@deepseek-ai/cordis'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import { LlmAdapter } from '@deepseek-ai/dsh-llm'
import type { GenerateOptions, StreamChunk } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'
import JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import ApprovalService from '@deepseek-ai/dsh-user-approval'
import UserQuestionService from '@deepseek-ai/dsh-user-questions'
import type { JsonRpcTransportPeer } from '@deepseek-ai/dsh-sdk-protocol'
import { expect, it } from 'vitest'
import { HarnessSdkJsonRpcServer } from '../src/server.ts'

class ReplyAdapter extends LlmAdapter {
  async * stream(_options: GenerateOptions): AsyncIterable<StreamChunk> {
    yield { type: 'block-start', index: 0, blockType: 'text' }
    yield { type: 'text-delta', index: 0, text: 'ready' }
    yield { type: 'block-end', index: 0, block: { type: 'text', text: 'ready' } }
  }
}

class InteractiveTransport implements JsonRpcTransportPeer {
  readonly requests: { method: string; params: object; signal?: AbortSignal }[] = []
  async request(method: string, params: object, signal?: AbortSignal): Promise<unknown> {
    this.requests.push({ method, params, ...(signal === undefined ? {} : { signal }) })
    return method === 'session/question'
      ? { answers: [{ id: 'confirm', selected: [], custom: 'yes' }] }
      : 'allowed-once'
  }
  notify(_method: string, _params?: object): void {}
}

it('relays real scoped services mounted after the SDK, including owned child approval', async () => {
  const root: string = await mkdtemp(join(tmpdir(), 'dsh-sdk-interactive-'))
  const ctx: Context = new Context()
  const transport: InteractiveTransport = new InteractiveTransport()
  let server: HarnessSdkJsonRpcServer | undefined
  try {
    await mountAgentLoopTestDependencies(ctx)
    await ctx.plugin(AgentLoop, { agents: [] })
    await ctx.plugin(JsonlSessionPersistence, { root, compression: 'none' })
    const release = ctx.llm.registerAdapter(['interactive-test'], new ReplyAdapter())
    ctx.effect(() => release)
    server = new HarnessSdkJsonRpcServer(ctx, transport)
    await ctx.plugin(UserQuestionService)
    await ctx.plugin(ApprovalService, { policy: 'ask' })
    await server.initialize({ cwd: root, provider: 'interactive-test', model: 'test' })
    await server.prompt({ sessionId: 'owned-root', contentBlocks: [{ type: 'text', text: 'hello' }] })
    const parent = ctx.agents.get(SessionId('owned-root'))
    if (parent === undefined) throw new Error('SDK root was not created')
    await parent.whenIdle()
    const answer = await ctx.userQuestions.ask({
      agent: parent, questions: [{ id: 'confirm', question: 'Continue?' }],
    })
    expect(answer.answers[0]?.custom).toBe('yes')
    parent.session.append('turn/start', { turn: 2 })
    expect(await ctx.approval.request({ agent: parent, toolName: 'bash' })).toBe('allowed-once')
    parent.session.append('turn/end', { turn: 2, reason: { kind: 'completed' } })
    const child = await ctx.agents.create({
      sessionId: SessionId('owned-child'), parentAgent: parent,
      meta: { cwd: root, parentSession: parent.session.id, origin: 'subagent', delegationDepth: 1 },
      agentOptions: { provider: 'interactive-test', model: 'test' },
    })
    child.agent.session.append('turn/start', { turn: 1 })
    expect(await ctx.approval.request({ agent: child.agent, toolName: 'read' })).toBe('allowed-once')
    child.agent.session.append('turn/end', { turn: 1, reason: { kind: 'completed' } })
    expect(transport.requests.map(request => request.method)).toEqual([
      'session/question', 'session/approval', 'session/approval',
    ])
    expect(transport.requests.at(-1)?.params).toEqual({ sessionId: 'owned-child', toolName: 'read' })
    await child.dispose()
  } finally {
    await server?.shutdown()
    await ctx.fiber.dispose()
    await rm(root, { recursive: true, force: true })
  }
}, 30_000)
