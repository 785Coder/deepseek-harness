import { Context } from '@deepseek-ai/cordis'
import AgentRegistry from '@deepseek-ai/dsh-agent'
import { brandString } from '@deepseek-ai/dsh-brand'
import LocalJobRegistry from '@deepseek-ai/dsh-jobs-local'
import type { JobId, JobSnapshot } from '@deepseek-ai/dsh-jobs'
import type { SessionId } from '@deepseek-ai/dsh-session'
import type { JsonRpcTransportPeer } from '@deepseek-ai/dsh-sdk-protocol'
import { expect, it } from 'vitest'
import { SdkJobObserver } from '../src/jobs.ts'

it('maps a record removal to a terminal frame plus removed close, and keeps unsubscribe throw-free', async () => {
  // A hand-built registry seam pins the observer's termination mapping
  // deterministically; the real-registry removal behavior is covered by the
  // jobs-local suite and the Loader composition spec.
  const frames: string[] = []
  const closed: { reason?: string }[] = []
  type JobEventOwner = unknown
  const listeners: ((owner: JobEventOwner) => void)[] = []
  const snapshot: JobSnapshot = {
    id: brandString<JobId>('bash-1'), kind: 'bash', label: 'x', ownerSession: brandString<SessionId>('owned'),
    status: 'running', startedAt: 0, reported: false,
  }
  let alive = true
  const registry = {
    listBySession: () => [snapshot],
    peek: () => {
      if (!alive) throw new Error('job bash-1 belongs to another session')
      return { text: '', snapshot }
    },
    onJobsChanged(listener: (owner: JobEventOwner) => void) {
      listeners.push(listener)
      return () => {}
    },
  }
  const observer: SdkJobObserver = new SdkJobObserver(
    { get: (name: string) => name === 'jobs' ? registry : undefined } as unknown as Context,
    {
      request: async () => ({}),
      notify(method, params) {
        if (method === 'job.frame' && params !== undefined) frames.push((params as { frame: { type: string } }).frame.type)
        if (method === 'job.closed' && params !== undefined) closed.push(params)
      },
    },
    () => true,
    { maxSubscriptions: 4, ackTimeoutMs: 5000, flushMs: 0, maxFrameBytes: 1024 },
  )
  try {
    observer.handle('job/follow', { subscriptionId: 'f', sessionId: 'owned', jobId: 'bash-1' })
    await expect.poll(() => frames.length).toBe(1)
    expect(frames[0]).toBe('opened')
    observer.acknowledge({ subscriptionId: 'f', seq: 1 })
    // The record leaves the registry (owner or service teardown): the last
    // known projection is announced, then the close carries `removed`.
    alive = false
    for (const listener of listeners) listener(undefined)
    await expect.poll(() => frames.length).toBe(2)
    expect(frames[1]).toBe('status')
    observer.acknowledge({ subscriptionId: 'f', seq: 2 })
    await expect.poll(() => closed.length).toBe(1)
    expect(closed[0]).toMatchObject({ reason: 'removed' })
    // Notifications carry no error channel: unsubscribe must never throw,
    // malformed or not, before or after disposal.
    expect(() => observer.handle('job/unsubscribe', { subscriptionId: 'f' })).not.toThrow()
    expect(() => observer.handle('job/unsubscribe', { subscriptionId: '' })).not.toThrow()
    expect(() => observer.handle('job/unsubscribe', {})).not.toThrow()
    expect(() => observer.handle('job/unsubscribe', { subscriptionId: 42 })).not.toThrow()
    await observer.dispose()
    expect(() => observer.handle('job/unsubscribe', { subscriptionId: 'f' })).not.toThrow()
  } finally {
    await observer.dispose()
  }
})

it('filters unowned jobs, bounds subscriptions and expires slow observers without killing tasks', async () => {
  const ctx: Context = new Context()
  const frames: object[] = []
  const closed: object[] = []
  const transport: JsonRpcTransportPeer = {
    request: async () => ({}),
    notify(method, params) {
      if (method === 'job.frame' && params !== undefined) frames.push(params)
      if (method === 'job.closed' && params !== undefined) closed.push(params)
    },
  }
  const observer: SdkJobObserver = new SdkJobObserver(ctx, transport, id => id === 'owned', {
    maxSubscriptions: 1, ackTimeoutMs: 20, flushMs: 0, maxFrameBytes: 10,
  })
  try {
    expect(() => observer.handle('job/list', { sessionId: 'owned' })).toThrow('not mounted')
    await ctx.plugin(AgentRegistry)
    await ctx.plugin(LocalJobRegistry)
    ctx.jobs.attachController('test')
    const id = ctx.jobs.start({ kind: 'bash', label: 'unowned', run: () => ({ done: Promise.resolve({ status: 'completed' as const }), cancel: () => {} }) })
    expect(observer.handle('job/list', { sessionId: 'owned' })).toEqual({ jobs: [] })
    expect(() => observer.handle('job/read', { sessionId: 'owned', jobId: id })).toThrow('not visible')
    observer.handle('job/subscribe', { sessionId: 'owned', subscriptionId: 's' })
    expect(() => observer.handle('job/subscribe', { sessionId: 'owned', subscriptionId: 'other' })).toThrow('Too many')
    await expect.poll(() => closed.length).toBe(1)
    expect(frames.length).toBe(1)
    expect(closed[0]).toMatchObject({ reason: 'error', error: { kind: 'ack-timeout' } })
    expect(ctx.jobs.get(id).status).toBe('completed')
    observer.handle('job/subscribe', { sessionId: 'owned', subscriptionId: 'again' })
    observer.ownerDisposed('owned')
    await expect.poll(() => closed.length).toBe(2)
    expect(closed[1]).toMatchObject({ reason: 'owner-disposed' })
  } finally {
    await observer.dispose()
    await ctx.fiber.dispose()
  }
})
