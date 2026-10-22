import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { z } from 'zod'
import { Context } from '@deepseek-ai/cordis'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import * as JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import type { ProjectionDefinition } from '@deepseek-ai/dsh-session-projection'
import type { JsonRpcTransportPeer } from '@deepseek-ai/dsh-sdk-protocol'
import { MockAdapter, textResponse } from '../../../core/agent-loop/tests/mock-adapter.ts'
import { HarnessSdkJsonRpcServer } from '../src/index.ts'

declare module '@deepseek-ai/dsh-session-projection/types' {
  interface SessionProjectionStateMap {
    'parity/probe': number
  }
  interface SessionProjectionMap {
    'parity/probe': { count: number }
  }
}

const cleanups: (() => Promise<void>)[] = []
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
})

/** Counts committed turn starts so both the baseline and the change feed change. */
const probeUnit = (): Omit<ProjectionDefinition<'parity/probe', number>, 'wire'>
  & { wire: NonNullable<ProjectionDefinition<'parity/probe', number>['wire']> } => ({
  key: 'parity/probe',
  stateSchema: z.number().int().nonnegative(),
  init: () => 0,
  apply: (state, event) => (event.type === 'turn/start' ? state + 1 : state),
  wire: {
    viewSchema: z.object({ count: z.number().int().nonnegative() }),
    view: state => ({ count: state }),
  },
  stateVersion: 1,
})

interface ProjectionFramePayload {
  sessionId: string
  projection: string
  asOfSeq: number
  value: unknown
}

describe('SDK projection baseline and change feed', () => {
  it('serves a consistent baseline and streams changed units as frames', async () => {
    const root: string = await mkdtemp(join(tmpdir(), 'sdk-projections-'))
    cleanups.push(() => rm(root, { recursive: true, force: true }))
    const ctx: Context = new Context()
    cleanups.push(() => ctx.fiber.dispose())
    await mountAgentLoopTestDependencies(ctx)
    await ctx.plugin(JsonlSessionPersistence, { root, compression: 'none' })
    // testkit 已挂载投影注册表：直接在其上注册测试 unit，不重复构造服务
    const registry = ctx.get('sessionProjections')
    if (registry === undefined) throw new Error('session projections were not mounted by the testkit')
    registry.register(probeUnit())
    await ctx.plugin(AgentLoop, { agents: [] })
    ctx.llm.registerAdapter(['mock'], new MockAdapter([textResponse('ok')]))

    const frames: ProjectionFramePayload[] = []
    const transport: JsonRpcTransportPeer = {
      request: async () => { throw new Error('unexpected server request') },
      notify: (method, params) => {
        if (method === 'session.projection-frame') frames.push(params as unknown as ProjectionFramePayload)
      },
    }
    const server = new HarnessSdkJsonRpcServer(ctx, transport)
    cleanups.push(() => server.shutdown().then(() => {}))

    await server.initialize({ cwd: root, provider: 'mock', model: 'mock' })
    await server.prompt({ sessionId: 'projected', contentBlocks: [{ type: 'text', text: 'hello' }] })
    await server.wait({ sessionId: 'projected' })

    const baseline = server.projections({ sessionId: 'projected' })
    expect(baseline.asOfSeq).toBeGreaterThan(0)
    expect(baseline.values['parity/probe']).toEqual({ count: 1 })

    // 变更馈送与基线同源：同一 unit 的变化以帧下发，且带事件 seq
    const probeFrames: ProjectionFramePayload[] = frames.filter(frame => frame.projection === 'parity/probe')
    expect(probeFrames.length).toBeGreaterThan(0)
    expect(probeFrames.at(-1)?.value).toEqual({ count: 1 })
    expect(probeFrames.every(frame => frame.sessionId === 'projected')).toBe(true)
    expect(probeFrames.every(frame => frame.asOfSeq > 0)).toBe(true)

    // 未知会话不隐式创建，也不返回空基线
    expect(() => server.projections({ sessionId: 'missing' })).toThrow(/unknown SDK session/)
  })
})
