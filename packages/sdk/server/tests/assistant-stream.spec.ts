import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import type { AssistantStreamFrame } from '@deepseek-ai/dsh-agent'
import { LlmAdapter } from '@deepseek-ai/dsh-llm'
import type { GenerateOptions, StreamChunk } from '@deepseek-ai/dsh-llm'
import * as JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import type { JsonRpcTransportPeer } from '@deepseek-ai/dsh-sdk-protocol'
import { HarnessSdkJsonRpcServer } from '../src/index.ts'

const cleanups: (() => Promise<void>)[] = []
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
})

/** Streams two text deltas so the agent emits start/chunk/end frames. */
class StreamingAdapter extends LlmAdapter {
  async * stream(_options: GenerateOptions): AsyncIterable<StreamChunk> {
    yield { type: 'block-start', index: 0, blockType: 'text' }
    yield { type: 'text-delta', index: 0, text: 'delta-one ' }
    yield { type: 'text-delta', index: 0, text: 'delta-two' }
    yield { type: 'block-end', index: 0, block: { type: 'text', text: 'delta-one delta-two' } }
  }
}

interface FramePayload {
  sessionId: string
  frame: AssistantStreamFrame
}

describe('SDK assistant stream forwarding', () => {
  it('forwards native start/chunk/end frames for an owned session', async () => {
    const root: string = await mkdtemp(join(tmpdir(), 'sdk-stream-'))
    cleanups.push(() => rm(root, { recursive: true, force: true }))
    const ctx: Context = new Context()
    cleanups.push(() => ctx.fiber.dispose())
    await mountAgentLoopTestDependencies(ctx)
    await ctx.plugin(JsonlSessionPersistence, { root, compression: 'none' })
    await ctx.plugin(AgentLoop, { agents: [] })
    const release = ctx.llm.registerAdapter(['stream-test'], new StreamingAdapter())
    ctx.effect(() => release)

    const frames: FramePayload[] = []
    const transport: JsonRpcTransportPeer = {
      request: async () => { throw new Error('unexpected server request') },
      notify: (method, params) => {
        if (method === 'session.assistant-stream') frames.push(params as unknown as FramePayload)
      },
    }
    const server = new HarnessSdkJsonRpcServer(ctx, transport)
    cleanups.push(() => server.shutdown().then(() => {}))

    await server.initialize({ cwd: root, provider: 'stream-test', model: 'test' })
    await server.prompt({ sessionId: 'streamed', contentBlocks: [{ type: 'text', text: 'hello' }] })
    await server.wait({ sessionId: 'streamed' })

    // 帧原样透传：start → chunk* → end，且都属于被拥有的会话
    const types: string[] = frames.map(payload => payload.frame.type)
    expect(types[0]).toBe('start')
    expect(types.at(-1)).toBe('end')
    expect(types.filter(type => type === 'chunk').length).toBeGreaterThan(0)
    expect(frames.every(payload => payload.sessionId === 'streamed')).toBe(true)

    // revision 由上游按帧递增：同一次尝试内严格单调
    const revisions: number[] = frames.map(payload => payload.frame.revision)
    expect(revisions).toEqual([...revisions].sort((left, right) => left - right))
    expect(new Set(revisions).size).toBe(revisions.length)

    // 文本增量按 index 有序，内容与模型输出一致
    const chunks = frames.filter(payload => payload.frame.type === 'chunk')
    expect(chunks.map(payload => (payload.frame.type === 'chunk' ? payload.frame.index : -1)))
      .toEqual([...chunks.keys()].map(index => index).sort((left, right) => left - right))
    const text: string = chunks
      .map(payload => (payload.frame.type === 'chunk' && payload.frame.chunk.type === 'text-delta'
        ? payload.frame.chunk.text
        : ''))
      .join('')
    expect(text).toBe('delta-one delta-two')
  })
})
