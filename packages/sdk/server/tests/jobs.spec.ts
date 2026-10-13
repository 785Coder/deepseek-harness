import { Context } from '@deepseek-ai/cordis'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import LocalJobRegistry from '@deepseek-ai/dsh-jobs-local'
import { LlmAdapter } from '@deepseek-ai/dsh-llm'
import type { GenerateOptions, StreamChunk } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'
import SessionProjectionRegistry from '@deepseek-ai/dsh-session-projection'
import type { JobOutcome } from '@deepseek-ai/dsh-jobs'
import type { JsonRpcTransportPeer, SdkJobFrameNotification } from '@deepseek-ai/dsh-sdk-protocol'
import { expect, it } from 'vitest'
import { HarnessSdkJsonRpcServer } from '../src/server.ts'

class Adapter extends LlmAdapter {
  async * stream(_options: GenerateOptions): AsyncIterable<StreamChunk> {
    yield { type: 'block-start', index: 0, blockType: 'text' }
    yield { type: 'block-end', index: 0, block: { type: 'text', text: 'ready' } }
  }
}
class Transport implements JsonRpcTransportPeer {
  frames: SdkJobFrameNotification[] = []
  closed: object[] = []
  async request(): Promise<unknown> { return {} }
  notify(method: string, params?: object): void {
    if (method === 'job.frame') this.frames.push(params as SdkJobFrameNotification)
    if (method === 'job.closed' && params !== undefined) this.closed.push(params)
  }
}
it('observes owned terminal output with credit, without consuming the model result', { timeout: 15_000 }, async () => {
  const ctx: Context = new Context()
  const transport: Transport = new Transport()
  let server: HarnessSdkJsonRpcServer | undefined
  let settle: (outcome: JobOutcome) => void = () => {}
  try {
    await mountAgentLoopTestDependencies(ctx)
    await ctx.plugin(SessionProjectionRegistry)
    await ctx.plugin(AgentLoop, { agents: [] })
    ctx.effect(() => ctx.llm.registerAdapter(['job-test'], new Adapter()))
    await ctx.plugin(LocalJobRegistry)
    ctx.jobs.attachController('sdk-test')
    server = new HarnessSdkJsonRpcServer(ctx, transport, {
      jobs: { maxSubscriptions: 4, ackTimeoutMs: 1000, flushMs: 0, maxFrameBytes: 65536 },
    })
    await server.initialize({ cwd: process.cwd(), provider: 'job-test', model: 'test' })
    await server.prompt({ sessionId: 'owned', contentBlocks: [{ type: 'text', text: 'hello' }] })
    const agent = ctx.agents.get(SessionId('owned'))!
    await agent.whenIdle()
    const id = ctx.jobs.start({ kind: 'bash', owner: agent, label: 'background', run: () => ({
      done: new Promise<JobOutcome>((resolve) => { settle = resolve }),
      cancel: () => { settle({ status: 'killed' }) },
    }) })
    await expect(server.handleRequest('job/read', { sessionId: 'foreign', jobId: id })).rejects.toThrow('not owned')
    await expect(server.handleRequest('job/read', { sessionId: 'owned', jobId: id, from: true })).rejects.toThrow('Cursor')
    await expect(server.handleRequest('job/read', { sessionId: 'owned', jobId: id, from: 8 })).rejects.toThrow('Cursor')
    await expect(server.handleRequest('job/follow', { subscriptionId: 'bad', sessionId: 'owned', jobId: id, from: 8 })).rejects.toThrow('Cursor')
    await server.handleRequest('job/follow', { subscriptionId: 'follow', sessionId: 'owned', jobId: id })
    await expect.poll(() => transport.frames.length).toBe(1)
    expect(transport.frames[0]?.frame.type).toBe('opened')
    server.handleNotification('job/ack', { subscriptionId: 'follow', seq: 1 })
    settle({ status: 'completed', output: 'producer-output' })
    await expect.poll(() => transport.frames.length).toBe(2)
    expect(transport.frames[1]?.frame).toMatchObject({ type: 'output', next: 15, chunks: [{ at: 0, text: 'producer-output' }] })
    server.handleNotification('job/ack', { subscriptionId: 'follow', seq: 2 })
    await expect.poll(() => transport.frames.length).toBe(3)
    expect(transport.frames[2]?.frame).toMatchObject({ type: 'status', job: { status: 'completed' } })
    server.handleNotification('job/ack', { subscriptionId: 'follow', seq: 3 })
    await expect.poll(() => transport.closed.length).toBe(1)
    expect(transport.closed[0]).toMatchObject({ reason: 'completed' })
    // Observation never consumed the model's read: the consuming read still
    // returns the full terminal output, idempotently.
    expect(ctx.jobs.read(id, agent).text).toBe('producer-output')
    expect(ctx.jobs.read(id, agent).text).toBe('producer-output')
    ctx.emit('agent/disposed', { agent })
    await expect(server.handleRequest('job/list', { sessionId: 'owned' })).rejects.toThrow('not owned')
    // A notification arriving in the shutdown window carries no error channel:
    // it must be dropped quietly instead of becoming an unhandled rejection.
    await server.shutdown()
    const closedServer: HarnessSdkJsonRpcServer = server
    expect(() => { closedServer.handleNotification('job/unsubscribe', {}) }).not.toThrow()
    expect(() => { closedServer.handleNotification('job/unsubscribe', { subscriptionId: 'follow' }) }).not.toThrow()
  } finally {
    settle({ status: 'killed' })
    await server?.shutdown()
    await ctx.fiber.dispose()
  }
})
