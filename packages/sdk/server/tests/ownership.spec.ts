import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import type { Agent, AgentRegistry } from '@deepseek-ai/dsh-agent'
import { SessionId } from '@deepseek-ai/dsh-session'
import * as JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import type { JsonRpcTransportPeer } from '@deepseek-ai/dsh-sdk-protocol'
import UserQuestionService from '@deepseek-ai/dsh-user-questions'
import { MockAdapter, textResponse } from '../../../core/agent-loop/tests/mock-adapter.ts'
import {
  generationLogPath,
  logPath,
  sessionDir,
} from '../../../session/session-persistence-jsonl/src/format.ts'
import { HarnessSdkJsonRpcServer } from '../src/index.ts'
import { SessionOwnership } from '../src/ownership.ts'

const cleanups: (() => Promise<void>)[] = []
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
})

function makeAgent(id: string): Agent {
  const sessionId = SessionId(id)
  return { id: sessionId, session: { id: sessionId } } as unknown as Agent
}

describe('SessionOwnership', () => {
  it('authorizes only the exact bound object, not another agent with the same id', () => {
    const ownership = new SessionOwnership()
    const bound = makeAgent('main')
    const impostor = makeAgent('main')
    ownership.bind(bound)
    expect(ownership.ownsAgent(bound)).toBe(true)
    expect(ownership.ownsAgent(impostor)).toBe(false)
    expect(ownership.owns('main', () => bound)).toBe(true)
    expect(ownership.owns('main', () => impostor)).toBe(false)
  })

  it('keeps a newer binding when the reused id\u2019s predecessor is released late', () => {
    const ownership = new SessionOwnership()
    const predecessor = makeAgent('reused')
    const successor = makeAgent('reused')
    ownership.bind(predecessor)
    ownership.release(predecessor)
    ownership.bind(successor)
    ownership.release(predecessor)
    expect(ownership.ownsAgent(successor)).toBe(true)
    expect(ownership.ownsAgent(predecessor)).toBe(false)
  })

  it('rejects binding the same id to two live agents', () => {
    const ownership = new SessionOwnership()
    ownership.bind(makeAgent('twice'))
    expect(() => { ownership.bind(makeAgent('twice')) }).toThrow(/already bound to another live agent/)
  })

  it('derives children from the live runtime parent relation only', () => {
    const ownership = new SessionOwnership()
    const parent = makeAgent('parent')
    const child = makeAgent('child')
    const strangers = makeAgent('strangers')
    const parents = new Map<string, Agent>([['child', parent]])
    const registry = {
      isOwnedBy: (id: SessionId, owner: Agent) => parents.get(String(id)) === owner,
    } as unknown as AgentRegistry
    ownership.bind(parent)
    expect(ownership.isChildOfBound(child.id, registry)).toBe(true)
    expect(ownership.isChildOfBound(strangers.id, registry)).toBe(false)
  })
})

async function storageRoot(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'sdk-ownership-'))
  cleanups.push(() => rm(root, { recursive: true, force: true }))
  return root
}

interface Runtime {
  ctx: Context
  server: HarnessSdkJsonRpcServer
  transport: RecordingTransport
}

class RecordingTransport implements JsonRpcTransportPeer {
  readonly requests: { method: string; params: object; signal?: AbortSignal }[] = []

  async request(method: string, params: object, signal?: AbortSignal): Promise<unknown> {
    this.requests.push({ method, params, ...(signal === undefined ? {} : { signal }) })
    return method === 'session/question'
      ? { answers: [{ id: 'confirm', selected: [], custom: 'yes' }] }
      : 'allowed-once'
  }

  notify(_method: string, _params?: object): void {}
}

async function runtime(root: string): Promise<Runtime> {
  const ctx = new Context()
  cleanups.push(() => ctx.fiber.dispose())
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(JsonlSessionPersistence, { root, compression: 'none' })
  await ctx.plugin(AgentLoop, { agents: [] })
  ctx.llm.registerAdapter(['mock'], new MockAdapter([textResponse('remembered')]))
  const transport = new RecordingTransport()
  const server = new HarnessSdkJsonRpcServer(ctx, transport)
  cleanups.push(() => server.shutdown().then(() => {}))
  await server.initialize({ cwd: root, provider: 'mock', model: 'mock' })
  return { ctx, server, transport }
}

/** Persist one turn, then build an artifact whose filename and header disagree. */
async function writeMismatchedArtifact(root: string, sampleId: string, artifactId: string): Promise<void> {
  const source = await readFile(logPath(root, root, SessionId(sampleId), 'none'), 'utf8')
  const header = JSON.parse(source.split('\n')[0]!) as { id: string; version: number }
  header.id = artifactId
  const id = SessionId(artifactId)
  await mkdir(sessionDir(root, root, id), { recursive: true })
  await writeFile(generationLogPath(root, root, id, header.version - 1, 'none'), `${JSON.stringify(header)}\n`)
}

describe('SDK session opening and ownership', () => {
  it('restores a healthy session while an unrelated stored log is unreadable', async () => {
    const root: string = await storageRoot()
    const first: Runtime = await runtime(root)
    await first.server.prompt({ sessionId: 'healthy', contentBlocks: [{ type: 'text', text: 'save history' }] })
    await first.server.wait({ sessionId: 'healthy' })
    await first.server.shutdown()
    await first.ctx.fiber.dispose()

    await writeMismatchedArtifact(root, 'healthy', 'unrelated')

    const second: Runtime = await runtime(root)
    // 修复前：list() 读到无关损坏日志即整体失败，健康会话无法隐式恢复。
    await expect(second.server.prompt({
      sessionId: 'healthy', contentBlocks: [{ type: 'text', text: 'continue' }],
    })).resolves.toBeDefined()
    await expect(second.server.wait({ sessionId: 'healthy' })).resolves.toEqual({})
  })

  it('surfaces the target session\u2019s own storage error instead of creating a replacement', async () => {
    const root: string = await storageRoot()
    const first: Runtime = await runtime(root)
    await first.server.prompt({ sessionId: 'sample', contentBlocks: [{ type: 'text', text: 'save history' }] })
    await first.server.wait({ sessionId: 'sample' })
    await first.server.shutdown()
    await first.ctx.fiber.dispose()

    await writeMismatchedArtifact(root, 'sample', 'broken-target')

    const second: Runtime = await runtime(root)
    await expect(second.server.prompt({
      sessionId: 'broken-target', contentBlocks: [{ type: 'text', text: 'start' }],
    })).rejects.toThrow(/generation filename identifies/)

    // 没有降级 create：失败后目标仍然不是已注册的活 Agent。
    expect(second.ctx.agents.get(SessionId('broken-target'))).toBeUndefined()
  })

  it('does not authorize a root whose header merely names an owned parent session', async () => {
    const root: string = await storageRoot()
    const state: Runtime = await runtime(root)
    await state.ctx.plugin(UserQuestionService)
    await state.server.prompt({ sessionId: 'main', contentBlocks: [{ type: 'text', text: 'hello' }] })
    const parent = state.ctx.agents.get(SessionId('main'))
    if (parent === undefined) throw new Error('SDK root was not created')
    await parent.whenIdle()
    const outsider = await state.ctx.agents.create({
      sessionId: SessionId('outsider'),
      meta: { cwd: root, parentSession: parent.session.id },
      agentOptions: { provider: 'mock', model: 'mock' },
    })
    cleanups.push(() => outsider.dispose())

    // 只有持久化 lineage，没有运行时父对象：不属于本 SDK 客户端。
    expect(state.ctx.agents.isOwnedBy(outsider.agent.id, parent)).toBe(false)
    await expect(state.ctx.userQuestions.ask({
      agent: outsider.agent, questions: [{ id: 'confirm', question: 'Who answers?' }],
    })).rejects.toThrow(/no user-questions answerer accepted/)
    expect(state.transport.requests).toHaveLength(0)
  })

  it('relays a question asked from an agent/created hook during publication', async () => {
    const root: string = await storageRoot()
    const state: Runtime = await runtime(root)
    await state.ctx.plugin(UserQuestionService)
    const answers: string[] = []
    state.ctx.on('agent/created', async ({ agent }) => {
      if (String(agent.id) !== 'during-publish') return
      const answer = await state.ctx.userQuestions.ask({
        agent, questions: [{ id: 'confirm', question: 'Startup?' }],
      })
      answers.push(answer.answers[0]?.custom ?? '')
    })

    await state.server.prompt({ sessionId: 'during-publish', contentBlocks: [{ type: 'text', text: 'start' }] })
    await state.server.wait({ sessionId: 'during-publish' })

    expect(answers).toEqual(['yes'])
    expect(state.transport.requests.map(request => request.method)).toEqual(['session/question'])
  })
})
