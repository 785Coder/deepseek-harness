/** SDK claims on the upstream foreground question wait stream. */
import type { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { ToolCallId } from '@deepseek-ai/dsh-llm'
import { JsonRpcResponseError, SDK_ERROR_CODE } from '@deepseek-ai/dsh-sdk-protocol'
import type { SessionQuestionWaitParams, SessionQuestionWaitResult } from '@deepseek-ai/dsh-sdk-protocol'
import type {} from '@deepseek-ai/dsh-user-questions'

interface WaitLease {
  readonly sessionId: string
  readonly controller: AbortController
  readonly deadline: number
  readonly task: Promise<void>
}

/** Owns one claim per question until release, upstream settlement, or runtime disposal. */
export class SdkQuestionWaits {
  private readonly leases = new Map<string, Promise<WaitLease | undefined>>()
  constructor(private readonly ctx: Context, private readonly agentFor: (id: string) => Agent) {}

  /**
   * Attach to or release the actual business wait, without submitting an answer.
   * @param params - owned session, call id and client-panel action.
   * @returns the Host-computed countdown, or null when no foreground wait exists.
   */
  async handle(params: SessionQuestionWaitParams): Promise<SessionQuestionWaitResult> {
    const agent = this.agentFor(params.sessionId)
    const questions = this.ctx.get('userQuestions')
    if (questions === undefined) throw new JsonRpcResponseError(SDK_ERROR_CODE, 'question service is unavailable', { reason: 'answer-unavailable' })
    const key = JSON.stringify([params.sessionId, params.callId])
    const pending = this.leases.get(key)
    if (params.action === 'release') {
      const previous = await pending
      // Retire the registration before awaiting the stream so an immediate
      // re-attach starts a fresh claim instead of reading this deadline.
      if (this.leases.get(key) === pending) this.leases.delete(key)
      previous?.controller.abort(new Error('question panel released'))
      await previous?.task
      return { remainingMs: null }
    }
    if (pending !== undefined) {
      const previous = await pending
      return { remainingMs: previous === undefined ? null : Math.max(0, previous.deadline - Date.now()) }
    }
    const controller = new AbortController()
    const iterator = questions.attachWait(agent, ToolCallId(params.callId), controller.signal)[Symbol.asyncIterator]()
    const opening: Promise<WaitLease | undefined> = (async () => {
      const opened = await iterator.next()
      if (opened.done === true) return undefined
      const task = iterator.next().then(() => {}, (error: unknown) => {
        this.ctx.logger.warn(`question wait closed with error: ${String(error)}`)
      }).finally(() => {
        if (this.leases.get(key) === opening) this.leases.delete(key)
      })
      return { sessionId: params.sessionId, controller, deadline: Date.now() + opened.value.remainingMs, task }
    })()
    this.leases.set(key, opening)
    try {
      const lease = await opening
      if (lease === undefined) this.leases.delete(key)
      return { remainingMs: lease === undefined ? null : Math.max(0, lease.deadline - Date.now()) }
    } catch (error: unknown) {
      if (this.leases.get(key) === opening) this.leases.delete(key)
      throw error
    }
  }

  /** Abort claims for a released Agent; no synthetic answer is generated. */
  ownerDisposed(sessionId: string): void {
    for (const pending of this.leases.values()) {
      void pending.then((lease) => {
        if (lease?.sessionId === sessionId) lease.controller.abort(new Error('question owner disposed'))
      }, () => {})
    }
  }

  /** Release all claims and await their upstream stream cleanup. */
  async dispose(): Promise<void> {
    // Settle every opening independently: one rejected claim must not skip the
    // abort of the others during shutdown.
    const settled = await Promise.allSettled(this.leases.values())
    const leases = settled.flatMap(result => result.status === 'fulfilled' && result.value !== undefined ? [result.value] : [])
    for (const lease of leases) lease.controller.abort(new Error('SDK server disposed'))
    await Promise.all(leases.map(lease => lease.task))
    this.leases.clear()
  }
}
