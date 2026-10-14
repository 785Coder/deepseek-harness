/**
 * Non-consuming job observation for SDK clients: session-fenced rosters,
 * terminal-output reads, and acknowledgement-bounded frame pumps. Reads are
 * projections — the model's consuming output cursor and notice state are
 * never touched, so observation cannot starve a model read.
 *
 * Output semantics on this runtime: the registry retains no live ring for
 * stream-kind jobs, so only settled final-output jobs (for example
 * background subagents) expose their terminal text; stream jobs expose
 * status transitions only. `output.total` is the byte length of the settled
 * final output, or 0 while the job is live.
 *
 * @module @deepseek-ai/dsh-sdk-jsonrpc-server/jobs
 */

import type { Context } from '@deepseek-ai/cordis'
import { brandString } from '@deepseek-ai/dsh-brand'
import type { JobId, JobPeek, JobRegistry, JobSnapshot } from '@deepseek-ai/dsh-jobs'
import type { SessionId } from '@deepseek-ai/dsh-session'
import { JsonRpcResponseError } from '@deepseek-ai/dsh-sdk-protocol'
import type {
  JsonRpcTransportPeer, SdkJobChunk, SdkJobClosedNotification, SdkJobErrorKind, SdkJobFrame, SdkJobView,
} from '@deepseek-ai/dsh-sdk-protocol'

/** Deployment bounds for one SDK job observer. */
export interface SdkJobOptions {
  /** Maximum concurrently active subscriptions. */
  readonly maxSubscriptions: number
  /** Milliseconds allowed for consuming one outstanding frame. */
  readonly ackTimeoutMs: number
  /** Coalescing window after a registry change, in milliseconds. */
  readonly flushMs: number
  /** Soft output frame budget in UTF-8 bytes. */
  readonly maxFrameBytes: number
}

/** One active observation generation. */
interface Observation {
  controller: AbortController
  seq: number
  session: SessionId
  acknowledge?: () => void
  task: Promise<void>
  reason: SdkJobClosedNotification['reason']
}

/** A stable observation failure the client can branch on. */
function failure(kind: SdkJobErrorKind, message: string): JsonRpcResponseError {
  return new JsonRpcResponseError(-32010, message, { kind })
}

/** Acknowledgement deadline expired for one outstanding frame. */
class AckTimeoutError extends JsonRpcResponseError {
  constructor() {
    super(-32010, 'Job frame acknowledgement timed out', { kind: 'ack-timeout' })
  }
}

function text(params: Record<string, unknown>, key: string): string {
  const value: unknown = params[key]
  if (typeof value !== 'string' || value.length === 0) {
    throw failure('invalid-params', `${key} must be a non-empty string`)
  }
  return value
}

/** The absolute UTF-8 byte offset the client resuming from, or 0. */
function cursorOf(params: Record<string, unknown>): number {
  const raw: unknown = params.from
  if (raw === undefined) return 0
  if (typeof raw !== 'number' || !Number.isSafeInteger(raw) || raw < 0) {
    throw failure('invalid-cursor', 'Cursor must be a non-negative safe integer no greater than output.total')
  }
  return raw
}

function byteLength(value: string): number {
  return Buffer.byteLength(value, 'utf8')
}

function isTerminal(status: JobSnapshot['status']): boolean {
  return status === 'completed' || status === 'killed' || status === 'failed'
}

/**
 * Cut a settled final-output blob into output chunks from an absolute byte
 * offset, each at most `maxFrameBytes` (split only on code point boundaries;
 * a budget smaller than a code point ships that code point whole).
 * @param value - the full terminal text.
 * @param from - the first byte to emit.
 * @param maxFrameBytes - the soft per-chunk budget.
 * @returns one chunk per emitted slice, with absolute byte coordinates.
 */
function outputChunks(value: string, from: number, maxFrameBytes: number): SdkJobChunk[] {
  const bytes: Uint8Array = new TextEncoder().encode(value)
  const chunks: SdkJobChunk[] = []
  let at = from
  while (at < bytes.length) {
    let end: number = Math.min(at + maxFrameBytes, bytes.length)
    // A budget landing mid-code point resumes at the next boundary.
    while (end > at && ((bytes[end] ?? 0) & 0xc0) === 0x80) end -= 1
    if (end === at) {
      // A budget smaller than the code point at `at`: ship that whole code
      // point (at most four bytes) rather than strand its bytes.
      end = at + 1
      while (end < bytes.length && ((bytes[end] ?? 0) & 0xc0) === 0x80) end += 1
    }
    chunks.push({ at, text: new TextDecoder('utf-8').decode(bytes.subarray(at, end)) })
    at = end
  }
  return chunks
}

/** Split one read into frames along the soft per-frame byte budget. */
function* outputFrames(
  chunks: readonly SdkJobChunk[],
  next: number,
  lossy: boolean,
  maxFrameBytes: number,
): Iterable<SdkJobFrame> {
  let batch: SdkJobChunk[] = []
  let batchBytes = 0
  let flaggedLossy = lossy
  for (const chunk of chunks) {
    batch.push(chunk)
    batchBytes += byteLength(chunk.text)
    if (batchBytes >= maxFrameBytes) {
      const last = batch[batch.length - 1]
      /* v8 ignore start -- a non-empty batch always has a last chunk; the arm only discharges noUncheckedIndexedAccess. */
      const end = last === undefined ? next : last.at + byteLength(last.text)
      /* v8 ignore stop */
      yield {
        type: 'output',
        chunks: batch,
        next: end,
        ...flaggedLossy ? { lossy: true as const } : {},
      }
      flaggedLossy = false
      batch = []
      batchBytes = 0
    }
  }
  if (batch.length > 0 || flaggedLossy) {
    yield {
      type: 'output',
      chunks: batch,
      next,
      ...flaggedLossy ? { lossy: true as const } : {},
    }
  }
}

/** Project one registry snapshot onto the wire view with its output measure. */
function toView(snapshot: JobSnapshot, outputText: string): SdkJobView {
  return {
    id: String(snapshot.id),
    kind: snapshot.kind,
    label: snapshot.label,
    ...(snapshot.ownerSession === undefined ? {} : { owner: String(snapshot.ownerSession) }),
    status: snapshot.status,
    ...(snapshot.detail === undefined ? {} : { detail: snapshot.detail }),
    startedAt: snapshot.startedAt,
    ...(snapshot.finishedAt === undefined ? {} : { finishedAt: snapshot.finishedAt }),
    ...(snapshot.outputLimitBytes === undefined ? {} : { outputLimitBytes: snapshot.outputLimitBytes }),
    output: { total: byteLength(outputText), earliest: 0 },
  }
}

/** Terminal text of one already-listed job, or empty while live. */
function terminalText(registry: JobRegistry, session: SessionId, snapshot: JobSnapshot): string {
  if (!isTerminal(snapshot.status)) return ''
  return registry.peek(snapshot.id, session).text
}

/** A session-fenced peek that maps every rejection to the wire category. */
function peekVisible(registry: JobRegistry, jobId: JobId, session: SessionId): JobPeek {
  try {
    return registry.peek(jobId, session)
  } catch {
    throw failure('job-not-found', 'Job is not visible in this session')
  }
}

/**
 * Single-use wake latch: wait() resolves on wake() or rejects on abort. A
 * wake arriving before the next wait is retained, so a commit between a
 * resolved send and the generator re-entering wait is never lost.
 */
class Wake {
  private waiter: (() => void) | undefined
  private pending = false

  wait(signal: AbortSignal): Promise<void> {
    if (this.pending) {
      this.pending = false
      return Promise.resolve()
    }
    return new Promise<void>((resolve, reject) => {
      const cleanup = (): void => {
        this.waiter = undefined
        signal.removeEventListener('abort', abort)
      }
      const abort = (): void => {
        cleanup()
        reject(signal.reason instanceof Error ? signal.reason : new Error(String(signal.reason)))
      }
      if (signal.aborted) { abort(); return }
      this.waiter = (): void => { cleanup(); resolve() }
      signal.addEventListener('abort', abort, { once: true })
    })
  }

  wake(): void {
    const wait = this.waiter
    if (wait === undefined) { this.pending = true; return }
    this.waiter = undefined
    wait()
  }
}

/** Sleep for a coalescing window; resolves (never rejects) on abort. */
function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise<void>((resolve) => {
    if (ms <= 0) { resolve(); return }
    const cleanup = (): void => {
      clearTimeout(timer)
      signal.removeEventListener('abort', abort)
      resolve()
    }
    const abort = (): void => { cleanup() }
    const timer: ReturnType<typeof setTimeout> = setTimeout(cleanup, ms)
    signal.addEventListener('abort', abort, { once: true })
  })
}

/**
 * Stream one session's job roster: one frame on open, then one after every
 * lifecycle commit that touches a visible job (registration, stopping,
 * settlement, removal), coalesced over `flushMs`. Reads are projections; the
 * model's cursor and notice state never observe them.
 * @param registry - the live job registry.
 * @param session - the session whose owned jobs to mirror.
 * @param options - the coalescing window.
 * @param signal - generation cancellation owned by the pump.
 * @returns the roster frame sequence for one generation.
 */
async function* streamRows(
  registry: JobRegistry,
  session: SessionId,
  options: SdkJobOptions,
  signal: AbortSignal,
): AsyncIterable<SdkJobFrame> {
  signal.throwIfAborted()
  const wake = new Wake()
  const rows = (): SdkJobFrame => ({
    type: 'rows',
    jobs: registry.listBySession(session).map(snapshot => toView(snapshot, terminalText(registry, session, snapshot))),
  })
  // Subscribe before the first read so a commit between the read and the
  // wait cannot be missed.
  const unsubscribe = registry.onJobsChanged((owner) => {
    if (owner === undefined || owner.id === session) wake.wake()
  })
  try {
    yield rows()
    while (true) {
      await wake.wait(signal)
      // Let a burst of commits settle into one frame.
      await sleep(options.flushMs, signal)
      if (signal.aborted) return
      yield rows()
    }
  } finally {
    unsubscribe()
  }
}

/**
 * Follow one job: an opened frame, a status frame per lifecycle transition,
 * the settled final output as it becomes available, one terminal status
 * frame, then a normal close. A record removal surfaces as the last known
 * projection plus a `removed` close.
 * @param registry - the live job registry.
 * @param request - session, target id, resume offset, and the validated open-time peek.
 * @param options - coalescing window and frame budget.
 * @param onRemoved - reports the target leaving the registry mid-follow.
 * @param signal - generation cancellation owned by the pump.
 * @returns the frame sequence for one generation.
 */
async function* followJob(
  registry: JobRegistry,
  request: { session: SessionId; jobId: JobId; from: number; initial: JobPeek },
  options: SdkJobOptions,
  onRemoved: () => void,
  signal: AbortSignal,
): AsyncIterable<SdkJobFrame> {
  signal.throwIfAborted()
  let view: SdkJobView = toView(request.initial.snapshot, request.initial.text)
  // JSON of the view carried by the last emitted opened/status frame; a
  // repeated wake without a change must not re-announce the same projection.
  let lastFrameView: string = ''
  const wake = new Wake()
  const unsubscribe = isTerminal(request.initial.snapshot.status)
    ? () => {}
    : registry.onJobsChanged((owner) => {
      if (owner === undefined || owner.id === request.session) wake.wake()
    })
  const announce = (next: SdkJobView): SdkJobFrame | undefined => {
    const marker = JSON.stringify(next)
    if (marker === lastFrameView) return undefined
    lastFrameView = marker
    return { type: 'status', job: next }
  }
  try {
    yield { type: 'opened', job: view, from: request.from }
    lastFrameView = JSON.stringify(view)
    const emittedThrough = request.from
    if (isTerminal(view.status)) {
      const total: number = byteLength(request.initial.text)
      const chunks = outputChunks(request.initial.text, request.from, options.maxFrameBytes)
      yield* outputFrames(chunks, total, false, options.maxFrameBytes)
      yield { type: 'status', job: view }
      lastFrameView = JSON.stringify(view)
      return
    }
    while (true) {
      await wake.wait(signal)
      await sleep(options.flushMs, signal)
      if (signal.aborted) return
      let peek: JobPeek
      try {
        peek = registry.peek(request.jobId, request.session)
      } catch {
        // The record left the registry after settlement (owner or service
        // teardown). Announce the last known projection, then close removed.
        yield { type: 'status', job: view }
        lastFrameView = JSON.stringify(view)
        onRemoved()
        return
      }
      const next: SdkJobView = toView(peek.snapshot, peek.text)
      if (isTerminal(next.status)) {
        // Drained output first, then one terminal status, then close.
        const total: number = byteLength(peek.text)
        const chunks = outputChunks(peek.text, emittedThrough, options.maxFrameBytes)
        yield* outputFrames(chunks, total, false, options.maxFrameBytes)
        const status = announce(next)
        if (status !== undefined) yield status
        else lastFrameView = JSON.stringify(next)
        view = next
        return
      }
      const status = announce(next)
      if (status !== undefined) yield status
      view = next
    }
  } finally {
    unsubscribe()
  }
}

/**
 * Session-fenced, non-consuming job observation over the `ctx.jobs` registry.
 * Each generation holds one outstanding frame at a time; a slow client stalls
 * its own stream — and, past the ack deadline, closes with `ack-timeout` —
 * without cancelling the job or disturbing the model path.
 */
export class SdkJobObserver {
  private readonly observations: Map<string, Observation> = new Map()
  private disposed: boolean = false

  constructor(
    private readonly ctx: Context,
    private readonly transport: JsonRpcTransportPeer,
    private readonly owns: (sessionId: string) => boolean,
    private readonly options: SdkJobOptions,
  ) {}

  /** Dispatch one job request, or open one bounded observation generation.
   * @param method - job/list, job/read, job/subscribe, job/follow, or job/unsubscribe.
   * @param params - the raw wire params object.
   * @returns the request result to serialize.
   */
  handle(method: string, params: Record<string, unknown>): object {
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
    if (method === 'job/list') {
      return { jobs: registry.listBySession(session).map(snapshot => toView(snapshot, terminalText(registry, session, snapshot))) }
    }
    const controller: AbortController = new AbortController()
    let removed = false
    let frames: AsyncIterable<SdkJobFrame>
    if (method === 'job/subscribe') {
      frames = streamRows(registry, session, this.options, controller.signal)
    } else if (method === 'job/read') {
      const jobId: JobId = brandString<JobId>(text(params, 'jobId'))
      const from: number = cursorOf(params)
      const peek: JobPeek = peekVisible(registry, jobId, session)
      const total: number = byteLength(peek.text)
      if (from > total) throw failure('invalid-cursor', 'Cursor must be a non-negative safe integer no greater than output.total')
      return {
        job: toView(peek.snapshot, peek.text),
        chunks: outputChunks(peek.text, from, this.options.maxFrameBytes),
        next: total,
        lossy: false,
      }
    } else if (method === 'job/follow') {
      const jobId: JobId = brandString<JobId>(text(params, 'jobId'))
      const from: number = cursorOf(params)
      const initial: JobPeek = peekVisible(registry, jobId, session)
      if (from > byteLength(initial.text)) throw failure('invalid-cursor', 'Cursor must be a non-negative safe integer no greater than output.total')
      frames = followJob(registry, { session, jobId, from, initial }, this.options, () => { removed = true }, controller.signal)
    } else {
      throw failure('invalid-params', `Unknown job method: ${method}`)
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
    for (const [id, observation] of [...this.observations]) {
      if (String(observation.session) === session) this.stop(id, 'owner-disposed')
    }
  }

  /** Close every observation and stop serving; idempotent.
   * @returns once every observation task has settled.
   */
  async dispose(): Promise<void> {
    this.disposed = true
    for (const id of [...this.observations.keys()]) this.stop(id, 'unsubscribed')
    await Promise.allSettled([...this.observations.values()].map(observation => observation.task))
  }
}
