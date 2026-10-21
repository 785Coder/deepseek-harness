import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { spawn } from 'node:child_process'
import { PassThrough } from 'node:stream'
import { Context } from '@deepseek-ai/cordis'
import Loader from '@deepseek-ai/cordis-plugin-loader'
import Include from '@deepseek-ai/cordis-plugin-include'
import LocalJobRegistry from '@deepseek-ai/dsh-jobs-local'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import { LlmAdapter } from '@deepseek-ai/dsh-llm'
import type { GenerateOptions, StreamChunk } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'
import type { JobOutcome } from '@deepseek-ai/dsh-jobs'
import * as SdkPlugin from '../src/index.ts'
import { expect, it } from 'vitest'

class Adapter extends LlmAdapter {
  async * stream(_options: GenerateOptions): AsyncIterable<StreamChunk> {
    yield { type: 'block-start', index: 0, blockType: 'text' }
    yield { type: 'block-end', index: 0, block: { type: 'text', text: 'ready' } }
  }
}
it('serves real process output through Loader-mounted stdio SDK with acknowledgement credit', async () => {
  const root: string = await mkdtemp(join(tmpdir(), 'sdk-jobs-loader-'))
  const ctx: Context = new Context()
  const input: PassThrough = new PassThrough()
  const output: PassThrough = new PassThrough()
  const messages: Record<string, unknown>[] = []
  let buffer: string = ''
  output.on('data', (chunk: Buffer) => {
    buffer += chunk.toString()
    let end: number
    while ((end = buffer.indexOf('\n')) >= 0) {
      messages.push(JSON.parse(buffer.slice(0, end)) as Record<string, unknown>)
      buffer = buffer.slice(end + 1)
    }
  })
  const send = (method: string, params: object, id?: string): void => {
    input.write(JSON.stringify({ jsonrpc: '2.0', method, params, ...(id === undefined ? {} : { id }) }) + '\n')
  }
  try {
    await mountAgentLoopTestDependencies(ctx, { workingDirectory: true })
    ctx.effect(() => ctx.llm.registerAdapter(['job-loader'], new Adapter()))
    ctx.baseUrl = pathToFileURL(root).href + '/'
    await ctx.plugin(Loader)
    ctx.loader.builtins.include = Include
    const importer = { async import(specifier: string): Promise<object> {
      if (specifier === 'test-loop') return AgentLoop
      if (specifier === 'test-jobs') return LocalJobRegistry
      if (specifier === 'test-sdk') return { ...SdkPlugin, apply(context: Context, config: SdkPlugin.JsonRpcConfig) {
        SdkPlugin.apply(context, { ...config, input, output, exit: () => {} })
      } }
      throw new Error(specifier)
    } }
    ctx.loader.internal = Object.assign(ctx.loader.internal ?? {}, importer) as unknown as NonNullable<typeof ctx.loader.internal>
    const path: string = join(root, 'cordis.yml')
    await writeFile(path, '- name: test-loop\n  config:\n    agents: []\n- name: test-jobs\n- name: test-sdk\n  config:\n    jobFlushMs: 0\n')
    await ctx.loader.create({ name: 'cordis:include', config: { path: pathToFileURL(path).href } })
    await ctx.loader.await()
    ctx.jobs.attachController('loader-test')
    send('initialize', { cwd: root, provider: 'job-loader', model: 'test' }, 'init')
    await expect.poll(() => messages.some(message => message.id === 'init')).toBe(true)
    send('session/prompt', { sessionId: 'owned', contentBlocks: [{ type: 'text', text: 'hello' }] }, 'prompt')
    await expect.poll(() => messages.some(message => message.id === 'prompt')).toBe(true)
    const agent = ctx.agents.get(SessionId('owned'))!
    await agent.whenIdle()
    const id = ctx.jobs.start({ owner: agent.id, kind: 'bash', label: 'real process', run(job) {
      const child = spawn(process.execPath, ['-e', 'process.stdout.write("hello中文\\n")'], { env: {} })
      child.stdout.on('data', (chunk: Buffer) => { job.append(chunk.toString(), { channel: 'stdout' }) })
      return {
        done: new Promise<JobOutcome>((resolve) => {
          child.on('close', (code) => { resolve({ status: code === 0 ? 'completed' : 'failed' }) })
        }),
        cancel: () => { child.kill() },
      }
    } })
    send('job/follow', { sessionId: 'owned', jobId: id, subscriptionId: 'follow' }, 'follow')
    await expect.poll(() => messages.filter(message => message.method === 'job.frame').length).toBe(1)
    send('job/ack', { subscriptionId: 'follow', seq: 1 })
    await expect.poll(() => messages.filter(message => message.method === 'job.frame').length).toBe(2)
    expect(JSON.stringify(messages)).toContain('hello中文')
    send('job/ack', { subscriptionId: 'follow', seq: 2 })
    await expect.poll(() => messages.filter(message => message.method === 'job.frame').length).toBe(3)
    send('job/ack', { subscriptionId: 'follow', seq: 3 })
    await expect.poll(() => messages.some(message => message.method === 'job.closed')).toBe(true)
  } finally {
    await ctx.fiber.dispose()
    input.destroy()
    output.destroy()
    await rm(root, { recursive: true, force: true })
  }
}, 30000)
