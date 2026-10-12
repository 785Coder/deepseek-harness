/** Non-consuming SDK job observations with per-subscription acknowledgement credit. */
import type { Context } from '@deepseek-ai/cordis'
import { brandString } from '@deepseek-ai/dsh-brand'
import type { JobId, JobRegistry, JobView } from '@deepseek-ai/dsh-jobs'
import type { SessionId } from '@deepseek-ai/dsh-session'
import { observeJobOutput, streamJobRows } from '@deepseek-ai/dsh-api-job-controller/observation'
import { JsonRpcResponseError } from '@deepseek-ai/dsh-sdk-protocol'
import type { JsonRpcTransportPeer, SdkJobFrame, SdkJobErrorKind, SdkJobClosedNotification } from '@deepseek-ai/dsh-sdk-protocol'

/** Deployment bounds for task observation. */
export interface SdkJobOptions {
  maxSubscriptions: number
  ackTimeoutMs: number
  flushMs: number
  maxFrameBytes: number
}
interface Observation {
  controller: AbortController
  seq: number
  session: SessionId
  acknowledge?: () => void
  task: Promise<void>
  reason: SdkJobClosedNotification['reason']
}
function failure(kind: SdkJobErrorKind, message: string): JsonRpcResponseError {
  return new JsonRpcResponseError(-32010, message, { kind })
}
/** Subclass the pump can recognize: other JsonRpcResponseErrors are observation failures, not timeouts. */
class AckTimeoutError extends JsonRpcResponseError {
  constructor() {
    super(-32010, 'Job frame acknowledgement timed out', { kind: 'ack-timeout' })
  }
}
function text(params: Record<string, unknown>, key: string): string {
  const value: unknown = params[key]
  if (typeof value !== 'string' || value.length === 0) throw failure('invalid-params', `${key} must be a non-empty string`)
  return value
}

/** Adapts shared registry observers without changing model cursors or task ownership. */
export class SdkJobObserver {
  private readonly observations: Map<string, Observation> = new Map()
  private disposed: boolean = false
  constructor(
    private readonly ctx: Context,
    private readonly transport: JsonRpcTransportPeer,
    private readonly owns: (id: string) => boolean,
    private readonly options: SdkJobOptions,
  ) {}

  /** Dispatch a validated task query or subscription request.
   * @param method - SDK task method.
   * @param params - raw JSON parameters.
   * @returns query data or subscription receipt.
   */
  handle(method: string, params: Record<string, unknown>): object {
    // `job/unsubscribe` also arrives as a notification, where no error
    // response exists: it must stay idempotent and never throw, even after
    // disposal or for a malformed id, or a single bad frame would take the
    // whole runtime down through fail-loud.
    if (method === 'job/unsubscribe') {
      const id: unknown = params.subscriptionId
      if (typeof id === 'string' && id !== '') this.stop(id, 'unsubscribed')
      return {}
    }
    if (this.disposed) throw failure('observation-failed', 'SDK job observer is closed')
    const session: SessionId = brandString<SessionId>(text(params, 'sessionId'))
    if (!this.owns(session)) throw failure('session-not-owned', 'Session is not owned by this SDK client')
    const registry: JobRegistry | undefined = this.ctx.get('jobs')
    if (registry === undefined) throw failure('jobs-unavailable', 'Job registry is not mounted')
    if (method === 'job/list') return { jobs: registry.list(session).filter(job => job.owner === session) }
    let frames: AsyncIterable<SdkJobFrame>
    let removed: boolean = false
    const controller: AbortController = new AbortController()
    if (method === 'job/subscribe') {
      const source = streamJobRows(registry, { sessionId: session }, this.options, controller.signal)
      frames = (async function* (): AsyncIterable<SdkJobFrame> {
        for await (const frame of source) yield { type: 'rows', jobs: frame.jobs.filter(job => job.owner === session) }
      })()
    } else {
      const id: JobId = brandString<JobId>(text(params, 'jobId'))
      let job: JobView
      try { job = registry.get(id, session) } catch (_error) {
        throw failure('job-not-found', 'Job is not visible in this session')
      }
      if (job.owner !== session) throw failure('job-not-found', 'Job is not visible in this session')
      const from: unknown = params.from
      if (from !== undefined && (typeof from !== 'number' || !Number.isSafeInteger(from) || from < 0 || from > job.output.total)) {
        throw failure('invalid-cursor', 'Cursor must be a non-negative safe integer no greater than output.total')
      }
      const offset: number = typeof from === 'number' ? from : job.output.earliest
      if (method === 'job/read') return { job, ...registry.readAt(id, offset, session) }
      frames = observeJobOutput(registry, { sessionId: session, jobId: id, from: offset }, {
        ...this.options, onRemoved: () => { removed = true },
      }, controller.signal)
    }
    const subscriptionId: string = text(params, 'subscriptionId')
    if (this.observations.has(subscriptionId)) throw failure('invalid-params', 'Subscription id is already active')
    if (this.observations.size >= this.options.maxSubscriptions) throw failure('subscription-limit', 'Too many active job subscriptions')
    const observation: Observation = { controller, session, seq: 0, task: Promise.resolve(), reason: 'completed' }
    this.observations.set(subscriptionId, observation)
    observation.task = this.pump(subscriptionId, observation, frames, () => removed)
    return { subscriptionId }
  }

  /** Release credit only for the current frame.
   * @param params - subscription id and frame sequence from the client.
   */
  acknowledge(params: Record<string, unknown>): void {
    if (typeof params.subscriptionId !== 'string') return
    const observation: Observation | undefined = this.observations.get(params.subscriptionId)
    if (observation !== undefined && params.seq === observation.seq) observation.acknowledge?.()
  }

  private stop(id: string, reason: Observation['reason']): void {
    const observation: Observation | undefined = this.observations.get(id)
    if (observation === undefined) return
    observation.reason = reason
    observation.controller.abort(new Error(reason))
  }

  private async pump(id: string, observation: Observation, frames: AsyncIterable<SdkJobFrame>, removed: () => boolean): Promise<void> {
    let errorPayload: SdkJobClosedNotification['error']
    try {
      for await (const frame of frames) {
        if (observation.controller.signal.aborted) break
        observation.seq += 1
        await this.send(id, observation, frame)
      }
      if (removed()) observation.reason = 'removed'
    } catch (error) {
      if (!observation.controller.signal.aborted) {
        observation.reason = 'error'
        errorPayload = { kind: error instanceof AckTimeoutError ? 'ack-timeout' : 'observation-failed', message: error instanceof Error ? error.message : String(error) }
      }
    } finally {
      this.observations.delete(id)
      if (!this.disposed) {
        try { this.transport.notify('job.closed', { subscriptionId: id, reason: observation.reason, ...(errorPayload === undefined ? {} : { error: errorPayload }) }) }
        catch (error) { this.ctx.logger.warn(`Job observation closed after transport failure: ${String(error)}`) }
      }
    }
  }

  private send(id: string, observation: Observation, frame: SdkJobFrame): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const signal: AbortSignal = observation.controller.signal
      const cleanup = (): void => {
        clearTimeout(timer)
        signal.removeEventListener('abort', abort)
        delete observation.acknowledge
      }
      const abort = (): void => { cleanup(); reject(signal.reason instanceof Error ? signal.reason : new Error(String(signal.reason))) }
      const timer: ReturnType<typeof setTimeout> = setTimeout(() => {
        cleanup(); reject(new AckTimeoutError())
      }, this.options.ackTimeoutMs)
      observation.acknowledge = () => { cleanup(); resolve() }
      signal.addEventListener('abort', abort, { once: true })
      if (signal.aborted) { abort(); return }
      try { this.transport.notify('job.frame', { subscriptionId: id, seq: observation.seq, frame }) }
      catch (error) { cleanup(); reject(error instanceof Error ? error : new Error(String(error))) }
    })
  }

  /** End observations when their owning session is disposed.
   * @param session - disposed session id.
   */
  ownerDisposed(session: string): void {
    for (const [id, observation] of this.observations) {
      if (observation.session === session) this.stop(id, 'owner-disposed')
    }
  }

  /** Abort observations and await their listener cleanup. */
  async dispose(): Promise<void> {
    this.disposed = true
    const tasks: Promise<void>[] = [...this.observations.entries()].map(([id, observation]) => {
      this.stop(id, 'unsubscribed')
      return observation.task
    })
    await Promise.allSettled(tasks)
  }
}
