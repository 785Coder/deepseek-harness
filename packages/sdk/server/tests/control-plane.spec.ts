import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import CommandRuntime from '@deepseek-ai/dsh-commands'
import { SessionId } from '@deepseek-ai/dsh-session'
import SessionTitleService from '@deepseek-ai/dsh-session-title'
import * as JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import SqliteSessionQueryEngine from '@deepseek-ai/dsh-session-query-sqlite'
import type { JsonRpcTransportPeer } from '@deepseek-ai/dsh-sdk-protocol'
import { MockAdapter, textResponse } from '../../../core/agent-loop/tests/mock-adapter.ts'
import { HarnessSdkJsonRpcServer } from '../src/index.ts'
import { expectSdkError, expectSdkErrorAsync } from './sdk-error.ts'

const cleanups: (() => Promise<void>)[] = []
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
})

async function runtime(options: { commands?: boolean; titles?: boolean } = {}) {
  const root: string = await mkdtemp(join(tmpdir(), 'sdk-control-'))
  cleanups.push(() => rm(root, { recursive: true, force: true }))
  const ctx: Context = new Context()
  cleanups.push(() => ctx.fiber.dispose())
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(JsonlSessionPersistence, { root, compression: 'none' })
  // History/attachment/fork read the live-preferred corpus through explicit I/O,
  // so the composition under test mounts the query engine like production does.
  await ctx.plugin(SqliteSessionQueryEngine, { path: join(root, 'query.db') })
  await ctx.plugin(AgentLoop, { agents: [] })
  if (options.commands ?? true) await ctx.plugin(CommandRuntime)
  if (options.titles ?? true) {
    await ctx.plugin(SessionTitleService, { fallbackMaxWords: 8, fallbackMaxBytes: 64, maxTitleBytes: 200 })
  }
  ctx.llm.registerAdapter(['mock'], new MockAdapter([textResponse('first'), textResponse('second')]))
  const transport: JsonRpcTransportPeer = {
    request: async () => { throw new Error('unexpected server request') },
    notify: () => {},
  }
  const server = new HarnessSdkJsonRpcServer(ctx, transport)
  cleanups.push(() => server.shutdown().then(() => {}))
  await server.initialize({ cwd: root, provider: 'mock', model: 'mock' })
  return { ctx, server }
}

describe('SDK control plane: cancel and dispose', () => {
  it('cancels an attached session without dropping it', async () => {
    const { ctx, server } = await runtime()
    await server.prompt({ sessionId: 'cancelable', contentBlocks: [{ type: 'text', text: 'go' }] })
    await server.wait({ sessionId: 'cancelable' })

    // 已空闲时取消是 no-op，但契约成立：会话仍挂载、未被回收
    expect(server.cancel({ sessionId: 'cancelable' })).toEqual({})
    expect(ctx.agents.get(SessionId('cancelable'))).toBeDefined()
    expectSdkError(() => server.cancel({ sessionId: 'missing' }), 'not-attached')
  })

  it('disposes one session and lets the same id be created again', async () => {
    const { ctx, server } = await runtime()
    await server.prompt({ sessionId: 'disposable', contentBlocks: [{ type: 'text', text: 'go' }] })
    await server.wait({ sessionId: 'disposable' })
    const first = ctx.agents.get(SessionId('disposable'))
    if (first === undefined) throw new Error('session was not created')

    await expect(server.disposeSession({ sessionId: 'disposable' })).resolves.toEqual({})
    // 释放后该 id 不再挂在运行时；后续任何隐式访问都可重建（不会报 already bound）
    expect(ctx.agents.get(SessionId('disposable'))).toBeUndefined()
    await expectSdkErrorAsync(server.disposeSession({ sessionId: 'disposable' }), 'not-attached')

    await server.prompt({ sessionId: 'disposable', contentBlocks: [{ type: 'text', text: 'again' }] })
    await server.wait({ sessionId: 'disposable' })
    const second = ctx.agents.get(SessionId('disposable'))
    expect(second).toBeDefined()
    expect(second).not.toBe(first)
  })

  it('pages committed history by sequence without overlap', async () => {
    const { server } = await runtime()
    await server.prompt({ sessionId: 'paged', contentBlocks: [{ type: 'text', text: 'go' }] })
    await server.wait({ sessionId: 'paged' })

    const first = await server.history({ sessionId: 'paged', limit: 2 })
    expect(first.events).toHaveLength(2)
    expect(first.hasMore).toBe(true)
    expect(first.nextCursor).toBe(first.events[1]!.seq + 1)

    const second = await server.history({ sessionId: 'paged', fromSeq: first.nextCursor!, limit: 1000 })
    // 游标含端点、无重叠：第二页首事件严格晚于第一页末事件
    expect(second.events[0]!.seq).toBeGreaterThan(first.events[1]!.seq)
    expect(second.hasMore).toBe(false)
    expect(second.nextCursor).toBeNull()
    await expect(server.history({ sessionId: 'paged', limit: 0 })).rejects.toThrow(/1\.\.1000/)
  })

  it('rejects queue mutations for items that are no longer pending', async () => {
    const { server } = await runtime()
    await server.prompt({ sessionId: 'queued', contentBlocks: [{ type: 'text', text: 'go' }] })
    await server.wait({ sessionId: 'queued' })

    // 轮已结束 → 队列为空：三种动作都必须报 pending 不存在，而不是静默成功
    const actions = [
      { kind: 'remove' as const },
      { kind: 'steer' as const },
      { kind: 'edit' as const, content: [{ type: 'text' as const, text: 'edited' }] },
    ]
    for (const action of actions) {
      expectSdkError(() => server.updateQueue({ sessionId: 'queued', itemId: 'missing', action }), 'queue-item-not-found')
    }
    expectSdkError(() => server.updateQueue({ sessionId: 'missing', itemId: 'x', action: { kind: 'remove' } }), 'not-attached')
  })
})

describe('SDK control plane: command and rename', () => {
  it('runs a registered slash command and logs its own lifecycle', async () => {
    const { ctx, server } = await runtime()
    const dispose = ctx.commands.register({
      name: 'echo',
      description: 'echo the raw input back',
      handler: invocation => ({ kind: 'success', text: invocation.rawInput.trim() }),
    })
    cleanups.push(async () => { dispose() })

    await expect(server.command({ sessionId: 'commanded', line: '/echo hi' })).resolves.toEqual({ matched: true })
    const agent = ctx.agents.get(SessionId('commanded'))
    if (agent === undefined) throw new Error('command session was not created')
    const events = agent.session.snapshotEvents()
    // 命令不经模型：日志里只有 registry 写的 run/done 配对
    expect(events.map(event => event.type)).toEqual(expect.arrayContaining(['command/run', 'command/done']))
    expect(events.some(event => event.type === 'user/message')).toBe(false)
    const done = events.findLast(event => event.type === 'command/done')
    expect(done?.type === 'command/done' ? done.data : undefined).toMatchObject({ kind: 'success', text: 'hi' })
  })

  it('reports an admission miss without touching the log', async () => {
    const { ctx, server } = await runtime()
    // 未知命令名与非命令行都只是未命中：不记 command/run、不启动回合
    await expect(server.command({ sessionId: 'missed', line: '/nope' })).resolves.toEqual({ matched: false })
    await expect(server.command({ sessionId: 'missed', line: 'plain text' })).resolves.toEqual({ matched: false })
    const agent = ctx.agents.get(SessionId('missed'))
    if (agent === undefined) throw new Error('command session was not created')
    expect(agent.session.snapshotEvents().some(event => event.type === 'command/run')).toBe(false)
    // 协议层先拒空 line，再谈命令注册表
    await expect(server.handleRequest('session/command', { sessionId: 'missed', line: '' }))
      .rejects.toThrow(/non-empty line/)
  })

  it('rejects commands when the deployment mounts no registry', async () => {
    const { server } = await runtime({ commands: false })
    await expectSdkErrorAsync(server.command({ sessionId: 'nocommands', line: '/help' }), 'command-unavailable')
  })

  it('normalizes and pins a user title, rejecting an empty one', async () => {
    const { ctx, server } = await runtime()
    const accepted = await server.rename({ sessionId: 'titled', title: '  My   Chat  ' })
    expect(accepted.title).toBe('My Chat')
    expect(Number.isSafeInteger(accepted.seq)).toBe(true)

    const again = await server.rename({ sessionId: 'titled', title: 'Renamed' })
    expect(again.title).toBe('Renamed')
    // 每次接受都追加一条 session/title：序列单调递增，latest-wins
    expect(again.seq).toBeGreaterThan(accepted.seq)

    const agent = ctx.agents.get(SessionId('titled'))
    if (agent === undefined) throw new Error('renamed session was not created')
    const titles = agent.session.snapshotEvents().filter(event => event.type === 'session/title')
    expect(titles).toHaveLength(2)
    // 用户改名钉住标题：source 必须是 user，否则自动生成会覆盖
    expect(titles.at(-1)?.type === 'session/title' ? titles.at(-1)?.data : undefined)
      .toMatchObject({ title: 'Renamed', source: { kind: 'user' } })

    await expectSdkErrorAsync(server.rename({ sessionId: 'titled', title: '   ' }), 'title-invalid')
  })

  it('rejects renames when the deployment mounts no title service', async () => {
    const { server } = await runtime({ titles: false })
    await expectSdkErrorAsync(server.rename({ sessionId: 'untitled', title: 'x' }), 'title-unavailable')
  })
})
