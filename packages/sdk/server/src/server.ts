/**
 * JSON-RPC methods and notifications for out-of-process harness SDKs.
 * The surrounding context owns plugins, persistence, and configured adapters.
 *
 * @module @deepseek-ai/dsh-sdk-jsonrpc-server/server
 */

import type { Context } from '@deepseek-ai/cordis'
import { randomUUID } from 'node:crypto'
import { installModelSelectionProjection, installSessionListMetadata, sessionProjectionHints, paginateSessionHistory, referencedImage } from '@deepseek-ai/dsh-api-session-controller/shared'
import { resolve } from 'node:path'
import { brandString } from '@deepseek-ai/dsh-brand'
import { installModelSelection, type Agent, type AgentHandle, type AgentOptions, type AgentSetup, type ModelSelection, type ModelSelectionRef } from '@deepseek-ai/dsh-agent'
import { admitEncodedImages, AttachmentError, type EncodedImageAttachment, type ImageAttachmentRef } from '@deepseek-ai/dsh-attachment'
// Side-effect type import: resolves the optional `commands` lookup without a
// value dependency on the slash-command registry.
import type {} from '@deepseek-ai/dsh-commands'
import { createUserMessage, freezeMessage, ReasoningEffortId, type ContentBlock, type LlmRuntime, type ToolCallId } from '@deepseek-ai/dsh-llm'
import { carrierKeyOf, type Scoped } from '@deepseek-ai/dsh-scope'
import { buildForkSeed } from '@deepseek-ai/dsh-session/fork'
import { SessionLogOffset, SessionSeq } from '@deepseek-ai/dsh-session'
import type { Session, SessionEvent, SessionHeader, SessionId } from '@deepseek-ai/dsh-session'
import { SessionPersistenceNotFoundError, SessionAlreadyOwnedError, SessionAlreadyExistsError, SessionOwnershipLostError } from '@deepseek-ai/dsh-session-persistence'
import type {} from '@deepseek-ai/dsh-session-projection'
// Type-only import: resolves the optional `sessionQuery` list/search lookups
// without a value dependency on the query engine.
import { SessionQueryError, type SessionSearchCursor } from '@deepseek-ai/dsh-session-query'
import { SessionTitleInvalidError } from '@deepseek-ai/dsh-session-title'
import type {} from '@deepseek-ai/dsh-working-directory'
import type SubagentRuntime from '@deepseek-ai/dsh-subagent'
import type { SubagentRunEndInfo } from '@deepseek-ai/dsh-subagent'
import * as LlmDeepSeek from '@deepseek-ai/dsh-llm-deepseek-api-key'
import { JsonRpcResponseError, SDK_ERROR_CODE } from '@deepseek-ai/dsh-sdk-protocol'
import type { SdkErrorReason } from '@deepseek-ai/dsh-sdk-protocol'
import type {
  InitializeParams,
  InitializeResult,
  JsonRpcTransportPeer,
  SessionEventNotification,
  SessionAnswerParams,
  SessionAnswerResult,
  SessionAttachmentParams,
  SessionAttachmentResult,
  SessionCancelParams,
  SessionCommandParams,
  SessionCommandResult,
  SessionCreateParams,
  SessionCreateResult,
  SessionCreatedNotification,
  SessionDisposedNotification,
  SessionDisposeParams,
  SessionForkParams,
  SessionForkResult,
  SessionListParams,
  SessionListItem,
  SessionListResult,
  SessionModelSelectParams,
  SessionModelSelectResult,
  SessionSearchParams,
  SessionSearchResult,
  SessionSyncParams,
  SessionSyncResult,
  SessionHistoryParams,
  SessionHistoryResult,
  SessionPromptParams,
  SessionPromptResult,
  SessionQueueUpdateParams,
  SessionQueueUpdateResult,
  SessionProjectionsParams,
  SessionProjectionsResult,
  SessionRenameParams,
  SessionRenameResult,
  SessionResumeParams,
  SessionWaitParams,
  SessionWorkingDirectoryParams,
  SessionWorkingDirectorySetParams,
  SessionWorkingDirectoryResult,
  SdkEncodedImageBlock,
  SubagentFinishedNotification,
  SubagentStartedNotification,
} from '@deepseek-ai/dsh-sdk-protocol'
import type { ApprovalOutcome } from '@deepseek-ai/dsh-user-approval/types'
// Side-effect type import: resolves the `approval/request` and
// `user-questions/request` waterfalls and their `ctx.get(...)` lookups without
// a value dependency on either seam (optional composition).
import type {} from '@deepseek-ai/dsh-user-approval'
import type {} from '@deepseek-ai/dsh-user-questions'
import { UserQuestionError } from '@deepseek-ai/dsh-user-questions'
import type { AskUserQuestionAnswer } from '@deepseek-ai/dsh-user-questions/types'

import { SdkJobObserver, type SdkJobOptions } from './jobs.ts'
import { SessionOwnership } from './ownership.ts'
import { SdkQuestionWaits } from './questions.ts'

/**
 * Build one control-plane failure response.
 * @param reason - stable classification carried in `error.data.reason`.
 * @param message - human-readable detail.
 * @param data - extra structured fields (identities the client already knows).
 * @returns the error the JSON-RPC layer serializes with {@link SDK_ERROR_CODE}.
 */
function sdkError(reason: SdkErrorReason, message: string, data: Record<string, unknown> = {}): JsonRpcResponseError {
  return new JsonRpcResponseError(SDK_ERROR_CODE, message, { reason, ...data })
}

/** Publication-window retention bounds for unowned session prefixes. */
const MAX_BUFFERED_SESSIONS = 64
const MAX_BUFFERED_EVENTS = 512

/** Request methods this build serves; advertised by `initialize`. */
const SDK_REQUEST_METHODS = [
  'initialize',
  'session/prompt',
  'session/cancel',
  'session/dispose',
  'session/history',
  'session/command',
  'session/rename',
  'session/queue/update',
  'session/projections',
  'session/sync',
  'session/list',
  'session/search',
  'session/create',
  'session/fork',
  'session/answer',
  'session/question/wait',
  'session/attachment/read',
  'session/model/select',
  'session/resume',
  'session/wait',
  'session/working-directory/get',
  'session/working-directory/set',
  'job/list',
  'job/read',
  'job/subscribe',
  'job/follow',
  'job/unsubscribe',
  'shutdown',
] as const

/** Server-to-client notification methods this build emits; advertised by `initialize`. */
const SDK_NOTIFICATION_METHODS = [
  'session.event',
  'session.status',
  'session.question-state',
  'session.created',
  'session.disposed',
  'session.assistant-stream',
  'session.projection-frame',
  'subagent.started',
  'subagent.finished',
  'job.frame',
  'job.closed',
] as const

interface SessionRecord {
  handle: AgentHandle
  /** Latest live failure not superseded by a subsequently committed terminal. */
  failure: { readonly error: unknown; readonly atOffset: SessionLogOffset } | undefined
}

function encodedImage(block: SessionPromptParams['contentBlocks'][number]): block is SdkEncodedImageBlock {
  return block.type === 'image' && 'data' in block
}

async function durablePromptContent(ctx: Context, blocks: SessionPromptParams['contentBlocks']): Promise<ContentBlock[]> {
  const images = blocks.filter(encodedImage)
  if (images.length === 0) return blocks as ContentBlock[]
  const attachments = ctx.get('attachments')
  if (attachments === undefined) throw new Error('SDK image prompt requires an attachment store')
  const refs = await admitEncodedImages(attachments, images.map((image): EncodedImageAttachment => ({
    data: image.data,
    mediaType: image.mimeType,
  })))
  let next = 0
  return blocks.map(block => encodedImage(block)
    ? { type: 'image', attachment: refs[next++] as ImageAttachmentRef }
    : block)
}

/** Inclusive seq of the latest completed-turn prefix, mirroring the Web fork cut. */
function latestCompletedPrefixBoundary(events: readonly SessionEvent[]): number | undefined {
  const lastTurnEnd = events.findLast(event => event.type === 'turn/end')
  if (lastTurnEnd === undefined) return undefined
  let boundary = lastTurnEnd.seq
  for (const next of events.slice(boundary + 1)) {
    if (next.type === 'turn/start'
      || (next.type === 'user/message' && next.surfaceOp === 'append')
      || next.type === 'agent/inbox/spliced') break
    boundary = next.seq
  }
  return boundary
}

/** Recover the delegating parent from the service-owned scoped carrier. */
function subagentParentOf(carrier: Scoped<SubagentRuntime>): Agent {
  return carrierKeyOf(carrier) as Agent
}

/** Deployment-specific status mapping for SDK turn and subagent outcomes. */
export interface HarnessSdkJsonRpcServerOptions {
  /** Report max-token termination as an accepted result instead of an infrastructure error. */
  maxTokensAsSuccess?: boolean
  /** Validated deployment bounds for SDK job observation. */
  jobs?: SdkJobOptions
}

function successStatus(reason: string, options: HarnessSdkJsonRpcServerOptions): 'ok' | 'error' {
  if (reason === 'completed') return 'ok'
  return reason === 'max-tokens' && options.maxTokensAsSuccess === true ? 'ok' : 'error'
}

/**
 * SDK server over one booted harness context and transport peer. Construction
 * subscribes to session, agent, and subagent lifecycle events until shutdown;
 * reinitialization is unsupported.
 */
export class HarnessSdkJsonRpcServer {
  private readonly jobObserver: SdkJobObserver
  private readonly questionWaits: SdkQuestionWaits
  private cwd = process.cwd()
  private provider = 'deepseek-official'
  private model = 'deepseek-official'
  private reasoningEffort: ReturnType<typeof ReasoningEffortId> | undefined
  private maxTokens: number | undefined
  private llmFiber: { dispose(): Promise<void> } | undefined
  private readonly sessions = new Map<string, SessionRecord>()
  private readonly sessionCreations = new Map<string, Promise<SessionRecord>>()
  private readonly ownership = new SessionOwnership()
  private allowedSessionIds: Set<string> | undefined
  private readonly pendingEvents = new Map<string, { session: Session; event: SessionEvent }[]>()
  /** Per-Agent mutable route selection installed by `installModelSelection`. */
  private readonly modelSelections = new Map<Agent, ModelSelectionRef>()
  private modelProjectionInstalled = false
  private projectionsSubscribed = false
  private readonly disposers: (() => void)[] = []
  private shutdownTask: Promise<Record<string, never>> | undefined
  private shuttingDown = false
  private initialized = false

  constructor(
    private readonly ctx: Context,
    private readonly transport: JsonRpcTransportPeer,
    private readonly options: HarnessSdkJsonRpcServerOptions = {},
  ) {
    this.questionWaits = new SdkQuestionWaits(ctx, id => this.requireAttached(id))
    this.jobObserver = new SdkJobObserver(ctx, transport, id => this.ownership.owns(id, sid => this.ctx.agents.get(sid)), options.jobs ?? {
      maxSubscriptions: 32, ackTimeoutMs: 60000, flushMs: 100, maxFrameBytes: 65536,
    })
    this.disposers.push(ctx.on('agent/disposed', ({ agent }) => {
      const sessionId: string = String(agent.session.id)
      // Read ownership before releasing it: only a session this client owned is
      // announced, never another client's agent leaving the shared registry.
      const owned = this.ownership.ownsAgent(agent)
      this.ownership.release(agent)
      this.jobObserver.ownerDisposed(sessionId)
      this.questionWaits.ownerDisposed(sessionId)
      this.modelSelections.delete(agent)
      if (owned) this.transport.notify('session.disposed', { sessionId } satisfies SessionDisposedNotification)
    }))
    const serverOptions = this.options
    this.disposers.push(ctx.on('session/event', (session, event) => {
      const record = this.sessions.get(session.id)
      if (event.type === 'turn/end' && record?.handle.agent.session === session
        && record.failure !== undefined && event.seq >= record.failure.atOffset) {
        record.failure = undefined
      }
      const pending = this.pendingEvents.get(String(session.id))
      if (pending !== undefined) {
        if (pending.length >= MAX_BUFFERED_EVENTS) {
          // The client will read this log through `session/history`; dropping
          // the live prefix beats unbounded retention in a shared runtime.
          this.pendingEvents.delete(String(session.id))
          return
        }
        pending.push({ session, event })
      } else if (this.ownership.owns(String(session.id), id => this.ctx.agents.get(id))) {
        const payload: SessionEventNotification = { sessionId: String(session.id), event }
        this.transport.notify('session.event', payload)
      }
    }))
    this.disposers.push(ctx.on('agent/assistant-stream', ({ agent, frame }) => {
      // Frames are transient model output, never durable facts; only owned
      // sessions are reported so another client's agent cannot leak text here.
      if (!this.ownership.ownsAgent(agent)) return
      this.transport.notify('session.assistant-stream', { sessionId: String(agent.session.id), frame })
    }))
    this.disposers.push(ctx.on('agent/error', ({ agent, error }) => {
      const record = this.sessions.get(agent.session.id)
      if (record?.handle.agent !== agent) return
      record.failure = { error, atOffset: agent.session.seq }
    }))
    this.disposers.push(ctx.on('agent/status', ({ agent, status }) => {
      if (!this.ownership.ownsAgent(agent)) return
      this.transport.notify('session.status', { sessionId: String(agent.session.id), status })
    }))
    this.disposers.push(ctx.on('session/created', (session) => {
      const sessionId = String(session.id)
      if (this.ownership.owns(sessionId, id => ctx.agents.get(id))) return
      // A child may be published before its runtime parent is known, so hold
      // its first facts until `agent/created` decides whether this client owns
      // it. Lineage is never authorization here: the buffer only survives if
      // the runtime parent turns out to be bound.
      if (this.pendingEvents.size >= MAX_BUFFERED_SESSIONS) {
        const oldest = this.pendingEvents.keys().next().value
        if (oldest !== undefined) this.pendingEvents.delete(oldest)
      }
      this.pendingEvents.set(sessionId, [])
    }))
    this.disposers.push(ctx.on('agent/created', ({ agent }) => {
      // A runtime child of an agent this server owns is owned as well: its
      // approvals belong to this client. The registry's live parent relation
      // decides, not the durable parentSession string.
      if (!this.ownership.ownsAgent(agent) && this.ownership.isChildOfBound(agent.id, ctx.agents)) {
        this.ownership.bind(agent)
      }
      // Only agents this client owns are announced: another client's session
      // entering the shared registry is not part of this SDK surface.
      if (!this.ownership.ownsAgent(agent)) {
        this.pendingEvents.delete(String(agent.id))
        return
      }
      this.flushPublishedEvents(agent)
      const parentId = agent.session.header.parentSession
      const parent = parentId === undefined ? undefined : ctx.agents.get(parentId)
      if (parent !== undefined && this.ownership.ownsAgent(parent) && ctx.agents.isOwnedBy(agent.id, parent)) {
        this.transport.notify('subagent.started', {
          parentSessionId: String(parent.id), childSessionId: String(agent.id),
        } satisfies SubagentStartedNotification)
      }
      this.transport.notify('session.created', { sessionId: String(agent.id) } satisfies SessionCreatedNotification)
    }))
    const ownership = this.ownership
    this.disposers.push(ctx.on('subagent/end', function (this: Scoped<SubagentRuntime>, info: SubagentRunEndInfo) {
      const parent = subagentParentOf(this)
      if (!ownership.ownsAgent(parent)) return
      // This protocol reports only in-process child sessions. The service
      // snapshots the provider name and local flag through child disposal;
      // matching ids or parent lineage alone never establishes locality.
      if (!info.local) return
      const payload: SubagentFinishedNotification = {
        provider: info.provider,
        agentId: String(info.id),
        parentSessionId: String(parent.session.id),
        childSessionId: String(info.id),
        status: successStatus(info.stopReason, serverOptions),
        stopReason: info.stopReason,
        ...(info.lastAssistantMessage === undefined
          ? {}
          : { lastAssistantMessage: [...info.lastAssistantMessage] }),
      }
      transport.notify('subagent.finished', payload)
    }))
    // SDK 提问中继：把 agent 作用域的 user-questions/request waterfall 经 JSON-RPC
    // 交给 SDK 客户端应答；非本服务端拥有的 session 委托给下游 answerer 处理。
    // Optional siblings can activate after this server. Register the relay
    // before their first request rather than depending on Loader mount order.
    this.disposers.push(this.ctx.on('user-questions/request', async (request, next) => {
      const agent: Agent | undefined = request.agent
      if (agent === undefined || !this.ownership.ownsAgent(agent)) return next()
      const callId = request.callId ?? request.wait?.callId
      let state: 'answered' | 'continued' | 'closed' = 'closed'
      try {
        const answer = await this.transport.request(
          'session/question',
          {
            sessionId: String(agent.id),
            questions: request.questions,
            // The timed wait carries the call id when no tool call asked directly;
            // either identity keys the client's card, so prefer the explicit one.
            ...(request.callId === undefined
              ? request.wait === undefined ? {} : { callId: request.wait.callId }
              : { callId: request.callId }),
            ...(request.wait?.timed === true ? { timed: true } : {}),
          },
          request.signal,
        ) as AskUserQuestionAnswer
        state = 'answered'
        return answer
      } catch (error: unknown) {
        const reason: unknown = request.signal?.reason
        if (reason instanceof UserQuestionError && reason.code === 'ASK_TIMED_OUT') state = 'continued'
        throw error
      } finally {
        if (request.wait?.timed === true && callId !== undefined) {
          this.transport.notify('session.question-state', { sessionId: String(agent.id), callId, state })
        }
      }
    }))
    // SDK 批准中继：把 ctx.approval 的 waterfall 经 JSON-RPC 交给 SDK 客户端应答。
    this.disposers.push(this.ctx.on('approval/request', (req, next) => {
      if (!this.ownership.ownsAgent(req.agent)) return next()
      return this.transport.request(
        'session/approval',
        {
          sessionId: String(req.agent.session.id),
          toolName: req.toolName,
          ...(req.callId === undefined ? {} : { callId: req.callId }),
          ...(req.reason === undefined ? {} : { reason: req.reason }),
        },
        req.signal,
      ) as Promise<ApprovalOutcome>
    }))
    this.subscribeProjections()
  }

  /**
   * Validate and configure the SDK route, mounting the DeepSeek fallback only when unowned.
   * @param params - SDK handshake parameters.
   * @returns server identity for the handshake.
   */
  async initialize(params: InitializeParams): Promise<InitializeResult> {
    if (this.initialized) throw sdkError('conflict', 'SDK server is already initialized')
    if (params.reasoningEffort !== undefined
      && (typeof params.reasoningEffort !== 'string' || params.reasoningEffort.length === 0)) {
      throw new TypeError('initialize reasoningEffort must be a non-empty string')
    }
    if (params.maxTokens !== undefined
      && (!Number.isSafeInteger(params.maxTokens) || params.maxTokens <= 0)) {
      throw new TypeError('initialize maxTokens must be a positive safe integer')
    }
    if (params.sessionIds !== undefined && (!Array.isArray(params.sessionIds)
      || params.sessionIds.some(id => typeof id !== 'string' || id.length === 0))) {
      throw new TypeError('initialize sessionIds must be non-empty strings')
    }
    const cwd = resolve(params.cwd)
    const provider = params.provider
    const model = params.model
    const reasoningEffort = params.reasoningEffort === undefined
      ? undefined
      : ReasoningEffortId(params.reasoningEffort)
    if (!this.hasAdapterFor(provider)) {
      if (provider !== 'deepseek-official') throw new Error(`no adapter registered for provider "${provider}"`)
      this.llmFiber = await this.ctx.plugin(LlmDeepSeek)
    }
    // Adapter presence was read from this service above; a successful fallback mount also requires it.
    const llm = this.ctx.get('llm') as LlmRuntime
    await llm.resolveCallConfig({
      provider,
      model,
      ...reasoningEffort === undefined ? {} : { reasoningEffort },
      ...params.maxTokens === undefined ? {} : { maxTokens: params.maxTokens },
    })
    this.cwd = cwd
    this.allowedSessionIds = params.sessionIds === undefined ? undefined : new Set(params.sessionIds)
    this.provider = provider
    this.model = model
    this.reasoningEffort = reasoningEffort
    this.maxTokens = params.maxTokens
    this.initialized = true
    return {
      serverInfo: { name: 'deepseek-harness-sdk-runtime', version: '0.0.1' },
      capabilities: { requests: [...SDK_REQUEST_METHODS], notifications: [...SDK_NOTIFICATION_METHODS] },
    }
  }

  /**
   * Queue one identified prompt without assigning later activity to it.
   * @param params - target session and user content.
   * @returns the durable message identity.
   */
  async prompt(params: SessionPromptParams): Promise<SessionPromptResult> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    const rec = await this.getOrCreateSession(params.sessionId)
    // An agent-loop-only reload disposes the loop's agents while this record
    // survives; a retained agent accepts followup() silently, so validate the
    // record against the live registry before delivery.
    this.assertLiveAgent(rec, params.sessionId)
    const content = await durablePromptContent(this.ctx, params.contentBlocks)
    // Attachment admission crosses an async boundary where shutdown or an
    // agent-loop reload may detach the retained handle.
    this.assertLiveAgent(rec, params.sessionId)
    const message = createUserMessage({
      content,
      source: { kind: 'user' },
    })
    rec.handle.agent.followup(message)
    return { messageId: message.id }
  }

  /**
   * Restore a persisted session and repair its interrupted final turn without sending input.
   * Uses the initialized model route while preserving the stored session identity and directory.
   * Rejects missing, unreadable, or already owned sessions; never creates a replacement.
   * @param params - persisted session identity in the configured persistence store.
   * @returns an empty result after the Agent is ready for prompts.
   */
  async resume(params: SessionResumeParams): Promise<Record<string, never>> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    if (this.shuttingDown) throw new Error('SDK server is shutting down')
    const sessionId = params.sessionId
    if (this.sessions.has(sessionId) || this.sessionCreations.has(sessionId)) {
      throw sdkError('conflict', `SDK session is already open or opening: ${sessionId}`)
    }
    await this.openSession(sessionId, true)
    return {}
  }

  /**
   * Wait for an existing owned Agent and its managed descendants to finish.
   * The response follows all Session notifications emitted during the wait.
   * Rejects a live Agent failure that has no subsequent durable terminal.
   * @param params - the SDK-owned session to observe without creating one.
   * @returns an empty result after the root stays idle across the descendant check.
   */
  async wait(params: SessionWaitParams): Promise<Record<string, never>> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    const rec = this.sessions.get(params.sessionId)
    if (rec === undefined) throw sdkError('not-attached', `unknown SDK session: ${params.sessionId}`)
    const agent = rec.handle.agent
    const subagents = this.ctx.get('subagents')
    while (true) {
      this.assertLiveAgent(rec, params.sessionId)
      await agent.whenIdle()
      const idleSeq = agent.session.seq
      const children = await subagents?.waitForChildren(agent)
      this.assertLiveAgent(rec, params.sessionId)
      if (!children && agent.status === 'idle' && agent.session.seq === idleSeq) {
        if (rec.failure !== undefined) throw rec.failure.error
        return {}
      }
    }
  }

  private assertLiveAgent(rec: SessionRecord, sessionId: string): void {
    if (this.ctx.agents.get(rec.handle.agent.id) !== rec.handle.agent) {
      throw sdkError('conflict', `session agent was disposed outside the server: ${sessionId}`)
    }
  }

  /**
   * Cancel the active turn of one attached session, keeping its pending inbox.
   * Mirrors the Web control plane: cancellation interrupts the turn only.
   * @param params - the SDK-owned session to cancel.
   * @returns empty result once cancellation was requested.
   */
  cancel(params: SessionCancelParams): Record<string, never> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    const rec = this.sessions.get(params.sessionId)
    if (rec === undefined) throw sdkError('not-attached', `session "${params.sessionId}" is not attached`, { sessionId: params.sessionId })
    this.assertLiveAgent(rec, params.sessionId)
    rec.handle.agent.cancel({ kind: 'user' }, { keepInbox: true })
    return {}
  }

  /**
   * Release one attached session without shutting the runtime down.
   * Disposal emits `agent/disposed`, which drops ownership and job visibility;
   * a later prompt creates a fresh agent on the same id.
   * @param params - the SDK-owned session to release.
   * @returns empty result after the agent is torn down.
   */
  async disposeSession(params: SessionDisposeParams): Promise<Record<string, never>> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    const rec = this.sessions.get(params.sessionId)
    if (rec === undefined) throw sdkError('not-attached', `session "${params.sessionId}" is not attached`, { sessionId: params.sessionId })
    this.sessions.delete(params.sessionId)
    this.sessionCreations.delete(params.sessionId)
    await rec.handle.dispose()
    return {}
  }

  /**
   * Mutate one still-pending queue item of an attached session.
   * Mirrors the Web control plane: edit replaces text content, remove drops the
   * item, and steer only applies to a next-turn item while the agent is running.
   * @param params - session, pending item id, and requested mutation.
   * @returns acknowledgement once the mutation was applied.
   */
  updateQueue(params: SessionQueueUpdateParams): SessionQueueUpdateResult {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    const rec = this.sessions.get(params.sessionId)
    if (rec === undefined) throw sdkError('not-attached', `session "${params.sessionId}" is not attached`, { sessionId: params.sessionId })
    this.assertLiveAgent(rec, params.sessionId)
    const agent = rec.handle.agent
    const nextTurn = agent.inbox.nextTurn.find(message => String(message.id) === params.itemId)
    const nextStep = agent.inbox.nextStep.find(message => String(message.id) === params.itemId)
    const located = nextTurn === undefined
      ? nextStep === undefined ? undefined : { target: 'next-step' as const, message: nextStep }
      : { target: 'next-turn' as const, message: nextTurn }
    if (located === undefined) {
      throw sdkError('queue-item-not-found', `queued item "${params.itemId}" is no longer pending`, { itemId: params.itemId })
    }
    const { target, message } = located
    switch (params.action.kind) {
      case 'edit': {
        // Same admission rules as the Web control plane: a queue edit replaces
        // text only, and it must still say something. Skipping this would push
        // an unvalidated (even image-encoded) block into the inbox and the log.
        const content: ContentBlock[] = []
        for (const block of params.action.content) {
          if (block.type !== 'text') {
            throw sdkError('queue-edit-non-text', 'queue edits accept text content only', { itemId: params.itemId })
          }
          content.push({ type: 'text', text: block.text })
        }
        if (content.every(block => block.type === 'text' && block.text.trim().length === 0)) {
          throw sdkError('queue-edit-empty', 'queue edit content must include non-whitespace text', { itemId: params.itemId })
        }
        agent.inbox.replace(message.id, freezeMessage({ ...message, content }))
        break
      }
      case 'remove':
        agent.inbox.remove(message.id)
        break
      case 'steer':
        // Steering needs a running turn that has not started its step yet.
        if (target !== 'next-turn' || agent.status !== 'running') {
          throw sdkError('steer-unavailable', 'the current turn no longer accepts steering', { itemId: params.itemId })
        }
        agent.inbox.remove(message.id)
        agent.steer(message)
        break
      default:
        throw new TypeError('session/queue/update action kind must be edit, remove, or steer')
    }
    return { accepted: true }
  }

  /**
   * Read an attached Session forwards by sequence or backwards by append-message count.
   * Message pages use the same source-inclusive cut as the Web controller.
   * @param params - session, inclusive start sequence, and page budget.
   * @returns one chronological page with an inclusive next cursor.
   */
  async history(params: SessionHistoryParams): Promise<SessionHistoryResult> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    this.requireAttached(params.sessionId)
    const query = this.ctx.get('sessionQuery')
    if (query === undefined) throw sdkError('history-unavailable', 'session history requires a query engine')
    using source = await query.observeSession(brandString<SessionId>(params.sessionId), { projectionMode: 'none' })
    return this.historyPage(params, source.events, source.cursor)
  }

  private historyPage(params: SessionHistoryParams, events: readonly SessionEvent[], cursor: number): SessionHistoryResult {
    const limit = params.limit ?? 100
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new TypeError('session/history limit must be an integer within 1..1000')
    if (params.align === 'message') {
      if (params.fromSeq !== undefined) throw new TypeError('message history uses beforeSeq, not fromSeq')
      const before = params.beforeSeq
      if (before !== undefined && (!Number.isSafeInteger(before) || before < 0 || before > cursor + 1)) {
        throw new TypeError('session/history beforeSeq must be within the observed prefix')
      }
      const page = paginateSessionHistory(
        events, before === undefined ? undefined : SessionLogOffset(before), limit, cursor < 0 ? -1 : SessionSeq(cursor),
      )
      return { events: [...page.events], hasMore: page.hasMore, nextCursor: page.hasMore ? Number(page.events[0]?.seq ?? 0) : null }
    }
    const from = params.fromSeq ?? 0
    if (!Number.isSafeInteger(from) || from < 0) throw new TypeError('session/history fromSeq must be a non-negative safe integer')
    const page = events.slice(from, from + limit)
    const last = page.at(-1)
    const next = last === undefined ? null : Number(last.seq) + 1
    const hasMore = next !== null && next <= cursor
    return { events: [...page], hasMore, nextCursor: hasMore ? next : null }
  }

  /**
   * Execute one slash-command line against a session without a model turn.
   * Mirrors the Web control plane: the command registry parses the line, a
   * resolved command logs its own `command/run`/`command/done` lifecycle, and
   * an admission miss (bad syntax or unknown name) logs nothing.
   * @param params - session and the complete command line, slash included.
   * @returns whether a registered command matched the line.
   */
  async command(params: SessionCommandParams): Promise<SessionCommandResult> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    // Resolve through resume-or-create like the working-directory methods: the
    // Web control plane resolves a command's agent from the store, so a client
    // may run a command before its first prompt in this runtime process.
    const rec = await this.getOrCreateSession(params.sessionId)
    this.assertLiveAgent(rec, params.sessionId)
    const commands = this.ctx.get('commands')
    if (commands === undefined) {
      throw sdkError('command-unavailable', 'this deployment mounts no command registry')
    }
    // The JSON-RPC request itself owns the lifetime: there is no separate UI
    // request to abort, so hand the registry a signal that never fires.
    const execution = await commands.execute(rec.handle.agent, params.line, [], new AbortController().signal)
    return { matched: execution !== undefined }
  }

  /**
   * Normalize and append a user-owned session title.
   * Mirrors the Web control plane: an accepted title pins the session, so
   * automatic generation stops revising it.
   * @param params - session and the proposed title.
   * @returns the accepted title and the seq of its durable event.
   */
  async rename(params: SessionRenameParams): Promise<SessionRenameResult> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    const rec = await this.getOrCreateSession(params.sessionId)
    this.assertLiveAgent(rec, params.sessionId)
    const titles = this.ctx.get('sessionTitle')
    if (titles === undefined) {
      throw sdkError('title-unavailable', 'this deployment mounts no session-title service')
    }
    try {
      const accepted = titles.rename(rec.handle.agent.session, params.title)
      return { title: accepted.title, seq: Number(accepted.eventSeq) }
    } catch (error: unknown) {
      // Only an empty-after-normalization title blames the caller's input; a
      // disposed service or a non-live session stays an infrastructure error.
      if (error instanceof SessionTitleInvalidError) {
        throw sdkError('title-invalid', error.message, { sessionId: params.sessionId })
      }
      throw error
    }
  }

  /**
   * Read one image attachment a session already references.
   * Mirrors the Web control plane: the session log must carry the reference,
   * so an attachment id alone never authorizes a store read.
   * @param params - session and the attachment identity from its log.
   * @returns the durable reference and its base64 payload.
   */
  async readAttachment(params: SessionAttachmentParams): Promise<SessionAttachmentResult> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    // Read requires an attached session: a stored log this runtime does not own
    // is outside this client's scope, even when the id is known.
    this.requireAttached(params.sessionId)
    const events = await this.sessionEvents(params.sessionId)
    const ref = referencedImage(events, params.attachmentId)
    if (ref === undefined) {
      throw sdkError('attachment-invalid', 'this image is not referenced by the session', { attachmentId: params.attachmentId })
    }
    const attachments = this.ctx.get('attachments')
    if (attachments === undefined) throw sdkError('attachment-invalid', 'this deployment mounts no attachment store')
    try {
      const stored = await attachments.readImage(ref)
      return { attachment: stored.ref, data: Buffer.from(stored.data).toString('base64') }
    } catch (error: unknown) {
      // Only a durable-reference failure blames the caller; a storage fault
      // stays an infrastructure error instead of masquerading as bad input.
      if (error instanceof AttachmentError) throw sdkError('attachment-invalid', error.message)
      throw error
    }
  }

  /**
   * Select the route one session uses from its next step on.
   * Mirrors the Web control plane: the route is validated before acceptance,
   * recorded durably as `model/selection`, and applied to prompt assembly and
   * request config together so a concurrent switch cannot split the two.
   * @param params - session and the requested provider/model route.
   * @returns the normalized route now in force.
   */
  async selectModel(params: SessionModelSelectParams): Promise<SessionModelSelectResult> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    if (params.provider.length === 0 || params.model.length === 0) {
      throw new TypeError('session/model/select requires a non-empty provider and model')
    }
    const rec = await this.getOrCreateSession(params.sessionId)
    this.assertLiveAgent(rec, params.sessionId)
    const selection = this.routeSelection(rec.handle.agent)
    const llm = this.ctx.get('llm')
    if (llm === undefined) throw sdkError('model-unavailable', 'this deployment mounts no LLM runtime')
    let resolved: { provider: string; model: string; reasoningEffort?: ReturnType<typeof ReasoningEffortId> }
    try {
      resolved = await llm.resolveCallConfig({
        provider: params.provider,
        model: params.model,
        ...(params.reasoningEffort === undefined
          ? {}
          : { reasoningEffort: ReasoningEffortId(params.reasoningEffort) }),
      })
    } catch (error: unknown) {
      throw sdkError('model-unavailable', error instanceof Error ? error.message : String(error), { provider: params.provider, model: params.model })
    }
    const selected: ModelSelection = {
      provider: resolved.provider,
      model: resolved.model,
      ...(resolved.reasoningEffort === undefined ? {} : { reasoningEffort: resolved.reasoningEffort }),
    }
    // Record durably: this event outlives the process and is the record a later
    // client select re-adopts (see routeSelection for why install is lazy).
    rec.handle.agent.session.append('model/selection', selected)
    selection.current = selected
    return { selected: { ...selected } }
  }

  /**
   * Align a reconnecting client with one session in a single round trip.
   * @param params - session, first sequence the client needs, and page budget.
   * @returns the authoritative next-seq cursor, the missed tail, and the
   *   projection baseline when the deployment mounts projections.
   */
  async sync(params: SessionSyncParams): Promise<SessionSyncResult> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    const rec = await this.getOrCreateSession(params.sessionId)
    this.assertLiveAgent(rec, params.sessionId)
    const query = this.ctx.get('sessionQuery')
    if (query === undefined) throw sdkError('history-unavailable', 'session sync requires a query engine')
    // One retained observation owns all three values. Appends during the await
    // belong after this cut and remain available from nextSeq on the next read.
    using source = await query.observeSession(brandString<SessionId>(params.sessionId))
    const history = this.historyPage(params, source.events, source.cursor)
    const values: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(source.projections?.values ?? {})) values[key] = value
    return {
      nextSeq: Number(source.cursor) + 1,
      history,
      ...(source.projections === undefined ? {} : {
        projections: { asOfSeq: Number(source.projections.asOfSeq), values },
      }),
    }
  }

  /**
   * List stored sessions newest first, with their latest log-backed titles.
   * @param params - row budget.
   * @returns bounded list rows, live flag included.
   */
  async listSessions(params: SessionListParams): Promise<SessionListResult> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    const limit = params.limit ?? 50
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 500) {
      throw new TypeError('session/list limit must be an integer within 1..500')
    }
    // Scope first: this runtime serves one working root, and the persistence
    // home may be shared with other deployments, so a foreign cwd is invisible.
    const visible = (await this.sessionRecords()).filter(record => this.canAccessHeader(record.header))
    // Read the full head before folding: the row budget applies to the
    // updated-at order, not to the storage order.
    const page = this.sessionRows(visible).slice(0, limit)
    return { sessions: page }
  }

  /**
   * Full-text search over stored sessions, grouped by session.
   * Mirrors the Web control plane's message-only, current-surface filters.
   * @param params - query text, page budget, and continuation cursor.
   * @returns one page of session hits.
   */
  async searchSessions(params: SessionSearchParams): Promise<SessionSearchResult> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    if (params.query.trim().length === 0) throw new TypeError('session/search requires a non-empty query')
    const limit = params.limit ?? 20
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) {
      throw new TypeError('session/search limit must be an integer within 1..100')
    }
    const query = this.ctx.get('sessionQuery')
    if (query === undefined) {
      throw sdkError('search-unavailable', 'this deployment mounts no session query engine')
    }
    const page = await query.searchSessions({
      query: params.query,
      eventFilters: [
        { kind: 'type', values: ['user/message', 'assistant/message'] },
        { kind: 'surface', values: ['current'] },
      ],
      limit,
      ...(params.cursor === undefined
        ? {}
        : { cursor: brandString<SessionSearchCursor>(params.cursor) }),
    })
    return {
      // Scope every hit to this runtime's working root: the persistence home may
      // be shared, and another deployment's transcript is not this client's data.
      items: page.items
        .filter(hit => this.canAccessHeader(hit.header))
        .map(hit => ({ sessionId: String(hit.header.id), snippet: hit.bestMatch.snippet })),
      ...(page.nextCursor === undefined ? {} : { nextCursor: String(page.nextCursor) }),
    }
  }

  /**
   * Create or adopt one session identity without prompting it.
   * Mirrors the Web control plane's resolve-then-create: an id already in the
   * store is resumed, a fresh id is created under the handshake directory.
   * @param params - explicit identity, or omitted to mint one.
   * @returns the identity now attached to this runtime.
   */
  async create(params: SessionCreateParams): Promise<SessionCreateResult> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    if (params.sessionId !== undefined && params.sessionId.length === 0) {
      throw new TypeError('session/create requires a non-empty sessionId when one is given')
    }
    const sessionId = params.sessionId ?? `session-${randomUUID()}`
    const rec = await this.getOrCreateSession(sessionId)
    this.assertLiveAgent(rec, sessionId)
    return { sessionId }
  }

  /**
   * Fork one session from an exact committed prefix.
   * Mirrors the Web control plane: an omitted cut selects the latest completed
   * turn, and an open cut receives synthetic closers from the seed builder.
   * @param params - source session and optional inclusive cut.
   * @returns the new child session identity.
   */
  async forkSession(params: SessionForkParams): Promise<SessionForkResult> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    if (params.atSeq !== undefined && (!Number.isSafeInteger(params.atSeq) || params.atSeq < 0)) {
      throw new TypeError('session/fork atSeq must be a non-negative safe integer')
    }
    this.requireAttached(params.sessionId)
    const source = await this.sessionSnapshot(params.sessionId)
    const boundary = params.atSeq ?? latestCompletedPrefixBoundary(source.events)
    const lastSeq = source.events.at(-1)?.seq
    if (boundary === undefined || source.events[boundary]?.seq !== boundary) {
      throw sdkError('fork-unavailable', params.atSeq === undefined
        ? `session "${params.sessionId}" has no completed turn to fork from`
        : `event ${String(params.atSeq)} does not exist in session "${params.sessionId}" (last seq: ${String(lastSeq ?? 'none')})`, { sessionId: params.sessionId })
    }
    const childId = brandString<SessionId>(`session-${randomUUID()}`)
    const seed = buildForkSeed(source.events, SessionSeq(boundary))
    const publication = this.ownershipSetup()
    try {
      const handle = await this.ctx.agents.create({
        sessionId: childId,
        seed,
        inheritedEventCount: SessionLogOffset(boundary + 1),
        meta: {
          cwd: source.header.cwd ?? this.cwd,
          parentSession: source.header.id,
          isSeeded: true,
        },
        agentOptions: this.defaultAgentOptions(),
        setup: publication.setup,
      })
      this.register(String(childId), handle)
    } catch (error: unknown) {
      // Commit precedes registry entry: a failure in between emits no
      // agent/disposed, so the stale binding must be released here.
      publication.release()
      throw error
    }
    return { sessionId: String(childId) }
  }

  /**
   * Deliver a late answer to a question whose foreground wait already ended.
   * Mirrors the Web control plane: the reply steers into the agent as the next
   * turn's user message, and answering a question that is no longer continued
   * is a no-op rather than an error.
   * @param params - session, question identity, and the complete answer batch.
   * @returns whether the question was still continued and accepted the reply.
   */
  async answerQuestion(params: SessionAnswerParams): Promise<SessionAnswerResult> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    const rec = await this.getOrCreateSession(params.sessionId)
    this.assertLiveAgent(rec, params.sessionId)
    const questions = this.ctx.get('userQuestions')
    if (questions === undefined) {
      throw sdkError('answer-unavailable', 'this deployment mounts no question service')
    }
    try {
      const accepted = questions.answer(rec.handle.agent, brandString<ToolCallId>(params.callId), params.answer)
      const active = this.ctx.get('sessionProjections')?.stateOf(rec.handle.agent.session, 'userQuestions')?.questions.active
        .find(question => String(question.callId) === params.callId)
      return { accepted, state: active?.state ?? 'settled' }
    } catch (error: unknown) {
      // The batch is caller-owned input; a shape the question never asked for is
      // the caller's fault, while a queued duplicate is a state conflict.
      if (error instanceof UserQuestionError) {
        throw sdkError(error.code === 'REPLY_QUEUED' ? 'reply-queued' : 'answer-invalid', error.message, { callId: params.callId })
      }
      throw error
    }
  }

  /**
   * Read one session's client-visible projection baseline.
   * @param params - the SDK-owned session to read.
   * @returns one consistent cut: every value reflects the same log position.
   */
  projections(params: SessionProjectionsParams): SessionProjectionsResult {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    const rec = this.sessions.get(params.sessionId)
    if (rec === undefined) throw sdkError('not-attached', `unknown SDK session: ${params.sessionId}`)
    this.assertLiveAgent(rec, params.sessionId)
    const registry = this.ctx.get('sessionProjections')
    if (registry === undefined) throw sdkError('projections-unavailable', 'this deployment mounts no session projection registry')
    // Subscribing lazily keeps an optional projection service from forcing a
    // mount; a client that reads the baseline then follows the change feed.
    this.subscribeProjections()
    const snapshot = registry.snapshot(rec.handle.agent.session)
    // Copy the typed partial into a plain record: the wire carries an opened
    // map keyed by projection name, not the compiled unit table.
    const values: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(snapshot.values)) values[key] = value
    return { asOfSeq: Number(snapshot.asOfSeq), values }
  }

  /** Subscribe the projection change feed once; later changes stream as frames. */
  private subscribeProjections(): void {
    if (this.projectionsSubscribed) return
    const registry = this.ctx.get('sessionProjections')
    if (registry === undefined) return
    this.projectionsSubscribed = true
    this.disposers.push(registry.onChanged((session, projection, value, seq) => {
      if (!this.ownership.owns(String(session.id), id => this.ctx.agents.get(id))) return
      this.transport.notify('session.projection-frame', {
        sessionId: String(session.id),
        projection,
        asOfSeq: Number(seq),
        value,
      })
    }))
  }

  /**
   * Read a Session's effective directory, recovering a missing directory to its origin.
   * @param params - target Session; an unknown id creates it.
   * @returns the absolute effective directory.
   */
  async getWorkingDirectory(params: SessionWorkingDirectoryParams): Promise<SessionWorkingDirectoryResult> {
    const agent = await this.workingDirectoryAgent(params.sessionId)
    return { cwd: await this.ctx.workingDirectory.ensure(agent) }
  }

  /**
   * Change a Session's directory and queue its model-visible transition.
   * @param params - target Session and absolute or current-directory-relative path.
   * @returns the validated absolute directory; origin and permissions are unchanged.
   */
  async setWorkingDirectory(params: SessionWorkingDirectorySetParams): Promise<SessionWorkingDirectoryResult> {
    const agent = await this.workingDirectoryAgent(params.sessionId)
    return { cwd: await this.ctx.workingDirectory.set(agent, params.path) }
  }

  private async workingDirectoryAgent(sessionId: string): Promise<Agent> {
    if (!this.initialized) throw new Error('SDK server is not initialized')
    const rec = await this.getOrCreateSession(sessionId)
    this.assertLiveAgent(rec, sessionId)
    return rec.handle.agent
  }

  /**
   * Dispose server-owned agents, adapter, and subscriptions to quiescence.
   * Managed descendants drain before the root Agent handles and adapter are released.
   * The surrounding context remains running.
   * @returns empty JSON-RPC result.
   */
  shutdown(): Promise<Record<string, never>> {
    this.shutdownTask ??= this.performShutdown()
    return this.shutdownTask
  }

  private async performShutdown(): Promise<Record<string, never>> {
    this.shuttingDown = true
    await this.questionWaits.dispose()
    await this.jobObserver.dispose()
    const pendingCreations = [...this.sessionCreations.values()]
    await Promise.allSettled(pendingCreations)
    this.sessionCreations.clear()
    const records = [...this.sessions.values()]
    this.sessions.clear()
    const failures: unknown[] = []
    while (this.disposers.length > 0) {
      try {
        this.disposers.pop()?.()
      } catch (error) {
        failures.push(error)
      }
    }
    try {
      await this.ctx.get('subagents')?.drainDescendants(records.map(rec => rec.handle.agent))
    } catch (error: unknown) {
      failures.push(error)
    }
    const teardownResults = await Promise.allSettled([
      ...records.map(rec => Promise.resolve().then(() => rec.handle.dispose())),
      ...(this.llmFiber === undefined ? [] : [Promise.resolve().then(() => this.llmFiber?.dispose())]),
    ])
    this.llmFiber = undefined
    failures.push(...teardownResults
      .filter((result): result is PromiseRejectedResult => result.status === 'rejected')
      .map(result => result.reason as unknown))
    if (failures.length === 1) throw failures[0]
    if (failures.length > 1) throw new AggregateError(failures, 'SDK server teardown failed')
    return {}
  }

  /**
   * Dispatch one incoming JSON-RPC request to its typed handler. Throws (→ a
   * JSON-RPC error response) on an unknown method.
   * @param method - the JSON-RPC method name.
   * @param params - the raw params object from the wire.
   * @returns the handler's result, to be serialized as the response.
   */
  async handleRequest(method: string, params: Record<string, unknown> | undefined): Promise<unknown> {
    try {
      return await this.dispatchRequest(method, params)
    } catch (error: unknown) {
      if (error instanceof JsonRpcResponseError) throw error
      if (error instanceof TypeError || error instanceof RangeError) {
        throw new JsonRpcResponseError(-32602, error.message, { reason: 'invalid-params', method })
      }
      let cause: unknown = error
      for (let depth = 0; depth < 8; depth++) {
        if (cause instanceof SessionAlreadyOwnedError) throw sdkError('writer-held', cause.message)
        if (cause instanceof SessionAlreadyExistsError || cause instanceof SessionOwnershipLostError) throw sdkError('conflict', cause.message)
        if (cause instanceof SessionPersistenceNotFoundError) throw sdkError('not-found', cause.message)
        if (!(cause instanceof Error) || cause.cause === undefined) break
        cause = cause.cause
      }
      if (error instanceof SessionQueryError) {
        // A query engine's stable codes are the only trustworthy classification:
        // a deployment may disable the full-text index while reads still work.
        if (error.code === 'SESSION_QUERY_SESSION_NOT_FOUND') throw sdkError('not-found', error.message)
        if (error.code === 'SESSION_QUERY_SEARCH_DISABLED') {
          throw sdkError('search-unavailable', error.message)
        }
        if (error.code.startsWith('SESSION_QUERY_INVALID_') || error.code === 'SESSION_QUERY_STALE_CURSOR') {
          throw new JsonRpcResponseError(-32602, error.message, { reason: 'invalid-params', method })
        }
      }
      throw sdkError('internal', error instanceof Error ? error.message : String(error), { method })
    }
  }

  private async dispatchRequest(method: string, params: Record<string, unknown> | undefined): Promise<unknown> {
    if (method.startsWith('job/')) {
      if (!this.initialized || this.shuttingDown) throw new Error('SDK server is not ready')
      if (!['job/list', 'job/read', 'job/subscribe', 'job/follow', 'job/unsubscribe'].includes(method)) {
        throw new JsonRpcResponseError(-32601, `unknown SDK job method: ${method}`, { reason: 'method-not-found' })
      }
      return this.jobObserver.handle(method, params ?? {})
    }
    switch (method) {
      case 'initialize':
        return this.initialize(params as unknown as InitializeParams)
      case 'session/prompt':
        return this.prompt(params as unknown as SessionPromptParams)
      case 'session/cancel': {
        const sessionId = params?.sessionId
        if (typeof sessionId !== 'string' || sessionId.length === 0) {
          throw new TypeError('session/cancel requires a non-empty sessionId string')
        }
        return this.cancel({ sessionId })
      }
      case 'session/dispose': {
        const sessionId = params?.sessionId
        if (typeof sessionId !== 'string' || sessionId.length === 0) {
          throw new TypeError('session/dispose requires a non-empty sessionId string')
        }
        return this.disposeSession({ sessionId })
      }
      case 'session/queue/update': {
        const sessionId = params?.sessionId
        const itemId = params?.itemId
        if (typeof sessionId !== 'string' || sessionId.length === 0) {
          throw new TypeError('session/queue/update requires a non-empty sessionId string')
        }
        if (typeof itemId !== 'string' || itemId.length === 0) {
          throw new TypeError('session/queue/update requires a non-empty itemId string')
        }
        const action = params?.action
        if (typeof action !== 'object' || action === null || !('kind' in action)) {
          throw new TypeError('session/queue/update requires an action object')
        }
        return this.updateQueue({ sessionId, itemId, action: action as unknown as SessionQueueUpdateParams['action'] })
      }
      case 'session/command': {
        const sessionId = params?.sessionId
        if (typeof sessionId !== 'string' || sessionId.length === 0) {
          throw new TypeError('session/command requires a non-empty sessionId string')
        }
        const line = params?.line
        if (typeof line !== 'string' || line.length === 0) {
          throw new TypeError('session/command requires a non-empty line string')
        }
        return this.command({ sessionId, line })
      }
      case 'session/rename': {
        const sessionId = params?.sessionId
        if (typeof sessionId !== 'string' || sessionId.length === 0) {
          throw new TypeError('session/rename requires a non-empty sessionId string')
        }
        const title = params?.title
        if (typeof title !== 'string') throw new TypeError('session/rename requires a title string')
        return this.rename({ sessionId, title })
      }
      case 'session/attachment/read': {
        const sessionId = params?.sessionId
        const attachmentId = params?.attachmentId
        if (typeof sessionId !== 'string' || sessionId.length === 0) {
          throw new TypeError('session/attachment/read requires a non-empty sessionId string')
        }
        if (typeof attachmentId !== 'string' || attachmentId.length === 0) {
          throw new TypeError('session/attachment/read requires a non-empty attachmentId string')
        }
        return this.readAttachment({ sessionId, attachmentId })
      }
      case 'session/model/select': {
        const sessionId = params?.sessionId
        const provider = params?.provider
        const model = params?.model
        if (typeof sessionId !== 'string' || sessionId.length === 0) {
          throw new TypeError('session/model/select requires a non-empty sessionId string')
        }
        if (typeof provider !== 'string' || typeof model !== 'string') {
          throw new TypeError('session/model/select requires provider and model strings')
        }
        const reasoningEffort = params?.reasoningEffort
        if (reasoningEffort !== undefined && typeof reasoningEffort !== 'string') {
          throw new TypeError('session/model/select reasoningEffort must be a string')
        }
        return this.selectModel({
          sessionId,
          provider,
          model,
          ...(reasoningEffort === undefined ? {} : { reasoningEffort }),
        })
      }
      case 'session/sync': {
        const sessionId = params?.sessionId
        if (typeof sessionId !== 'string' || sessionId.length === 0) {
          throw new TypeError('session/sync requires a non-empty sessionId string')
        }
        const fromSeq = this.optionalInteger(params, 'fromSeq')
        const limit = this.optionalInteger(params, 'limit')
        return this.sync({
          sessionId,
          ...(fromSeq === undefined ? {} : { fromSeq }),
          ...(limit === undefined ? {} : { limit }),
        })
      }
      case 'session/question/wait': {
        const sessionId = params?.sessionId
        const callId = params?.callId
        const action = params?.action
        if (typeof sessionId !== 'string' || sessionId.length === 0 || typeof callId !== 'string' || callId.length === 0) {
          throw new TypeError('session/question/wait requires sessionId and callId')
        }
        if (action !== 'attach' && action !== 'release') throw new TypeError('question wait action must be attach or release')
        return this.questionWaits.handle({ sessionId, callId, action })
      }
      case 'session/answer': {
        const sessionId = params?.sessionId
        const callId = params?.callId
        if (typeof sessionId !== 'string' || sessionId.length === 0) {
          throw new TypeError('session/answer requires a non-empty sessionId string')
        }
        if (typeof callId !== 'string' || callId.length === 0) {
          throw new TypeError('session/answer requires a non-empty callId string')
        }
        const answer = params?.answer
        if (typeof answer !== 'object' || answer === null) {
          throw new TypeError('session/answer requires an answer object')
        }
        return this.answerQuestion({ sessionId, callId, answer: answer as unknown as AskUserQuestionAnswer })
      }
      case 'session/list': {
        const limit = this.optionalInteger(params, 'limit')
        return this.listSessions(limit === undefined ? {} : { limit })
      }
      case 'session/search': {
        const query = params?.query
        if (typeof query !== 'string' || query.length === 0) {
          throw new TypeError('session/search requires a non-empty query string')
        }
        const limit = this.optionalInteger(params, 'limit')
        const cursor = params?.cursor
        if (cursor !== undefined && typeof cursor !== 'string') {
          throw new TypeError('session/search cursor must be a string')
        }
        return this.searchSessions({
          query,
          ...(limit === undefined ? {} : { limit }),
          ...(cursor === undefined ? {} : { cursor }),
        })
      }
      case 'session/create': {
        const sessionId = params?.sessionId
        if (sessionId !== undefined && typeof sessionId !== 'string') {
          throw new TypeError('session/create sessionId must be a string when given')
        }
        return this.create(sessionId === undefined ? {} : { sessionId })
      }
      case 'session/fork': {
        const sessionId = params?.sessionId
        if (typeof sessionId !== 'string' || sessionId.length === 0) {
          throw new TypeError('session/fork requires a non-empty sessionId string')
        }
        const atSeq = this.optionalInteger(params, 'atSeq')
        if (atSeq !== undefined && atSeq < 0) {
          throw new TypeError('session/fork atSeq must be a non-negative safe integer')
        }
        return this.forkSession({ sessionId, ...(atSeq === undefined ? {} : { atSeq }) })
      }
      case 'session/history': {
        const sessionId = params?.sessionId
        if (typeof sessionId !== 'string' || sessionId.length === 0) {
          throw new TypeError('session/history requires a non-empty sessionId string')
        }
        const fromSeq = this.optionalInteger(params, 'fromSeq')
        const limit = this.optionalInteger(params, 'limit')
        const beforeSeq = this.optionalInteger(params, 'beforeSeq')
        const align = params?.align
        if (align !== undefined && align !== 'seq' && align !== 'message') {
          throw new TypeError('session/history align must be seq or message')
        }
        return this.history({
          sessionId,
          ...(fromSeq === undefined ? {} : { fromSeq }),
          ...(limit === undefined ? {} : { limit }),
          ...(beforeSeq === undefined ? {} : { beforeSeq }),
          ...(align === undefined ? {} : { align }),
        })
      }
      case 'session/projections': {
        const sessionId = params?.sessionId
        if (typeof sessionId !== 'string' || sessionId.length === 0) {
          throw new TypeError('session/projections requires a non-empty sessionId string')
        }
        return this.projections({ sessionId })
      }
      case 'session/resume': {
        const sessionId = params?.sessionId
        if (typeof sessionId !== 'string' || sessionId.length === 0) {
          throw new TypeError('session/resume requires a non-empty sessionId string')
        }
        return this.resume({ sessionId })
      }
      case 'session/wait': {
        const sessionId = params?.sessionId
        if (typeof sessionId !== 'string') throw new TypeError('session/wait requires a sessionId string')
        return this.wait({ sessionId })
      }
      case 'session/working-directory/get':
      case 'session/working-directory/set': {
        if (typeof params?.sessionId !== 'string' || params.sessionId.length === 0) {
          throw new TypeError('working-directory requests require a non-empty sessionId')
        }
        if (method === 'session/working-directory/get') {
          return this.getWorkingDirectory({ sessionId: params.sessionId })
        }
        if (typeof params.path !== 'string') throw new TypeError('working-directory set requires a path string')
        return this.setWorkingDirectory({ sessionId: params.sessionId, path: params.path })
      }
      case 'shutdown':
        return this.shutdown()
      default:
        throw new JsonRpcResponseError(-32601, `unknown DeepSeek Harness SDK runtime method: ${method}`, { reason: 'method-not-found' })
    }
  }

  /** Route observation acknowledgements without creating request waiters.
   * @param method - notification method.
   * @param params - raw notification parameters.
   */
  handleNotification(method: string, params: Record<string, unknown>): void {
    if (method === 'job/ack') this.jobObserver.acknowledge(params)
    if (method === 'job/unsubscribe') {
      // Notifications carry no error channel: a throw here would surface as an
      // unhandled rejection and fail-loud kill the whole runtime process.
      try {
        this.jobObserver.handle(method, params)
      } catch (error) {
        this.ctx.logger.warn(`job/unsubscribe notification dropped: ${error instanceof Error ? error.message : String(error)}`)
      }
    }
  }

  /**
   * Assert this client owns the live agent for one session.
   * @param sessionId - session the caller wants to read or fork.
   */
  private requireAttached(sessionId: string): Agent {
    const rec = this.sessions.get(sessionId)
    if (rec === undefined) {
      const agent = this.ctx.agents.get(brandString<SessionId>(sessionId))
      if (agent !== undefined && this.ownership.ownsAgent(agent) && String(agent.id) === sessionId) return agent
      throw sdkError('not-attached', `session "${sessionId}" is not attached; call session/create first`, { sessionId })
    }
    this.assertLiveAgent(rec, sessionId)
    return rec.handle.agent
  }

  /**
   * Bind one SDK session id to the agent a publication creates.
   * @returns the setup to hand the factory plus the rollback for a failure
   *   between commit and registry entry.
   */
  private ownershipSetup(): { setup: AgentSetup; release(): void } {
    let candidate: Agent | undefined
    const registry = this.ctx.get('sessionProjections')
    if (registry !== undefined && !this.modelProjectionInstalled) {
      this.disposers.push(installModelSelectionProjection(this.ctx), installSessionListMetadata(this.ctx))
      this.modelProjectionInstalled = true
    }
    return {
      setup: (agentCtx, agent) => {
        candidate = agent
        const state = registry?.stateOf(agent.session, 'modelSelection')
        if (state?.pending !== null && state?.pending !== undefined || state?.selected !== undefined) {
          this.routeSelection(agent, agentCtx)
        }
        return { commit: () => {
          this.ownership.bind(agent)
        } }
      },
      release: () => {
        if (candidate !== undefined) this.ownership.release(candidate)
      },
    }
  }

  /** The route every SDK-created agent starts from. */
  private defaultAgentOptions(): AgentOptions {
    return {
      provider: this.provider,
      model: this.model,
      ...this.reasoningEffort === undefined ? {} : { reasoningEffort: this.reasoningEffort },
      ...this.maxTokens === undefined ? {} : { maxTokens: this.maxTokens },
    }
  }

  /**
   * Adopt one published handle as an SDK session.
   * @param sessionId - the id this client asked for, which keys the record.
   * @param handle - the agent this client now owns.
   * @returns the record tracked for the session.
   */
  private register(sessionId: string, handle: AgentHandle): SessionRecord {
    const rec: SessionRecord = { handle, failure: undefined }
    this.sessions.set(sessionId, rec)
    return rec
  }

  /** Install the route chosen explicitly for this Session before prompt assembly. */
  private routeSelection(agent: Agent, agentCtx: Context = agent.ctx): ModelSelectionRef {
    const existing = this.modelSelections.get(agent)
    if (existing !== undefined) return existing
    const state = this.ctx.get('sessionProjections')?.stateOf(agent.session, 'modelSelection')
    const saved = state?.pending ?? state?.selected
    const ref: ModelSelectionRef = {
      current: saved === undefined
        ? undefined
        : {
          provider: saved.provider,
          model: saved.model,
          ...(saved.reasoningEffort === undefined ? {} : { reasoningEffort: ReasoningEffortId(saved.reasoningEffort) }),
        },
      assembled: undefined,
    }
    installModelSelection(agentCtx, ref)
    this.modelSelections.set(agent, ref)
    return ref
  }

  /**
   * Live-preferred committed events for one session.
   * @param sessionId - session to read, attached or stored.
   * @returns the committed log, or the persisted log through a read handle.
   */
  private async sessionEvents(sessionId: string): Promise<readonly SessionEvent[]> {
    return (await this.sessionSnapshot(sessionId)).events
  }

  /**
   * Live-preferred header and committed events for one session.
   * @param sessionId - session to read, attached or stored.
   * @returns the header and its committed log.
   */
  private async sessionSnapshot(sessionId: string): Promise<{ header: SessionHeader; events: readonly SessionEvent[] }> {
    const query = this.ctx.get('sessionQuery')
    if (query !== undefined) {
      // The query corpus is live-preferred: it reads the attached log when the
      // session is live and the stored log otherwise, through explicit I/O.
      try {
        const snapshot = await query.readSession(brandString<SessionId>(sessionId))
        return { header: snapshot.session, events: snapshot.events }
      } catch (error: unknown) {
        // Only a confirmed absent id is the caller's misunderstanding; a corrupt
        // or unreadable log stays an infrastructure failure.
        if (error instanceof SessionQueryError && error.code === 'SESSION_QUERY_SESSION_NOT_FOUND') {
          throw sdkError('not-found', `session "${sessionId}" is neither attached nor stored`, { sessionId })
        }
        throw error
      }
    }
    const persistence = this.ctx.get('sessionPersistence')
    if (persistence === undefined) {
      throw sdkError('not-found', `session "${sessionId}" is neither attached nor stored (no persistence backend)`, { sessionId })
    }
    let handle: Awaited<ReturnType<typeof persistence.open>>
    try {
      handle = await persistence.open(brandString<SessionId>(sessionId), 'read')
    } catch (error: unknown) {
      if (error instanceof SessionPersistenceNotFoundError) {
        throw sdkError('not-found', `session "${sessionId}" is neither attached nor stored`, { sessionId })
      }
      throw error
    }
    try {
      return { header: handle.header, events: (await handle.read()).events }
    } finally {
      await handle.close()
    }
  }

  /** Stored sessions newest first, from the query engine or the raw backend. */
  private async sessionRecords(): Promise<readonly { header: SessionHeader }[]> {
    const query = this.ctx.get('sessionQuery')
    if (query !== undefined) return query.listSessions()
    const persistence = this.ctx.get('sessionPersistence')
    if (persistence === undefined) {
      throw sdkError('list-unavailable', 'this deployment mounts neither a query engine nor session persistence')
    }
    const snapshots = await persistence.list()
    return [...snapshots].sort((left, right) => right.header.createdAt - left.header.createdAt)
  }

  /** Read list columns from maintained projections without scanning transcripts. */
  private sessionRows(records: readonly { header: SessionHeader }[]): SessionListItem[] {
    const rows: SessionListItem[] = []
    for (const { header } of records) {
      const session = this.ctx.get('sessions')?.get(header.id)
      const hints = sessionProjectionHints(this.ctx, header, session)
      const metadata = hints?.values.sessionListMetadata
      const title = hints?.values.title
      rows.push({
        sessionId: String(header.id),
        createdAt: header.createdAt,
        updatedAt: Math.max(header.createdAt, metadata?.lastPromptAt ?? 0),
        blank: metadata?.blank ?? (session === undefined ? false : session.seq === 0),
        ...(header.cwd === undefined ? {} : { cwd: header.cwd }),
        ...(header.parentSession === undefined ? {} : { parentSession: String(header.parentSession) }),
        ...(typeof title !== 'string' ? {} : { title }),
        live: session !== undefined,
      })
    }
    return rows.sort((left, right) => right.updatedAt - left.updatedAt || left.sessionId.localeCompare(right.sessionId))
  }

  private canAccessHeader(header: SessionHeader): boolean {
    if (this.ownership.owns(String(header.id), id => this.ctx.agents.get(id))) return true
    return this.allowedSessionIds === undefined
      ? header.cwd !== undefined && resolve(header.cwd) === this.cwd
      : this.allowedSessionIds.has(String(header.id))
  }

  private authorizeSession(id: SessionId): Promise<void> | undefined {
    if (this.allowedSessionIds !== undefined) {
      if (!this.allowedSessionIds.has(String(id))) throw sdkError('session-not-owned', 'session is outside the initialized allowlist')
      return undefined
    }
    const live = this.ctx.get('sessions')?.get(id)
    if (live !== undefined) {
      if (!this.canAccessHeader(live.header)) throw sdkError('session-not-owned', 'session belongs to another working root')
      if (this.ctx.agents.get(id) !== undefined) throw sdkError('conflict', 'session is held by another live agent')
      return undefined
    }
    const persistence = this.ctx.get('sessionPersistence')
    if (persistence === undefined) return undefined
    return persistence.stat(id).then((snapshot) => {
      if (snapshot !== undefined && !this.canAccessHeader(snapshot.header)) {
        throw sdkError('session-not-owned', 'stored session belongs to another working root')
      }
    })
  }

  private flushPublishedEvents(agent: Agent): void {
    const events = this.pendingEvents.get(String(agent.id))
    this.pendingEvents.delete(String(agent.id))
    for (const item of events ?? []) {
      if (item.session !== agent.session) continue
      this.transport.notify('session.event', { sessionId: String(agent.id), event: item.event })
    }
  }

  /**
   * Read one optional integer parameter without silently dropping a wrong type.
   * @param params - raw request params.
   * @param key - parameter name checked on the wire.
   * @returns the integer, or undefined when the client omitted it.
   */
  private optionalInteger(params: Record<string, unknown> | undefined, key: string): number | undefined {
    const value = params?.[key]
    if (value === undefined) return undefined
    if (typeof value !== 'number') throw new TypeError(`session request ${key} must be a number`)
    return value
  }

  private async getOrCreateSession(sessionId: string): Promise<SessionRecord> {
    if (this.shuttingDown) throw new Error('SDK server is shutting down')
    const existing = this.sessions.get(sessionId)
    if (existing) return existing
    const pending = this.sessionCreations.get(sessionId)
    if (pending) return pending
    return this.openSession(sessionId, false)
  }

  private openSession(sessionId: string, resume: boolean): Promise<SessionRecord> {
    const creation = this.createSession(sessionId, resume)
    this.sessionCreations.set(sessionId, creation)
    void creation.then(
      () => { this.sessionCreations.delete(sessionId) },
      () => { this.sessionCreations.delete(sessionId) },
    )
    return creation
  }

  private async createSession(sessionId: string, resume: boolean): Promise<SessionRecord> {
    // No preset composition: this server's compositions keep the model-facing
    // rows in the host plane, so this agent reads them from the global layer. A
    // deployment that configures a roster has to join one here first
    // (@deepseek-ai/dsh-agent-preset-registry README, "Composing a child agent").
    const id = brandString<SessionId>(sessionId)
    const authorization = this.authorizeSession(id)
    if (authorization !== undefined) await authorization
    const agentOptions = this.defaultAgentOptions()
    // Bind inside the creation transaction's commit: an agent/created hook may
    // ask its owner before create() resolves, and a rollback disposes the agent,
    // which releases the binding. Never pre-authorize an id we do not own yet.
    this.pendingEvents.set(sessionId, [])
    const publication = this.ownershipSetup()
    let handle: AgentHandle
    try {
      handle = resume
        ? await this.ctx.agents.resume({ resumeSessionId: id, agentOptions, setup: publication.setup })
        : await this.resumeOrCreate(id, agentOptions, publication.setup)
      // Scope check after resume: the persistence home may be shared with other
      // working roots, and a stored session outside this one is not this client's.
    } catch (error: unknown) {
      // Commit runs before the factory enters the registry; a failure between
      // commit and entry emits no agent/disposed, so release the stale binding
      // here or the id could never be created again in this process.
      publication.release()
      this.pendingEvents.delete(sessionId)
      throw error
    }
    return this.register(sessionId, handle)
  }

  /**
   * Restore a stored session through a targeted open, creating only when the
   * store confirms the id is absent. Enumerating the store would let an
   * unrelated unreadable log decide this session's fate.
   * @param id - target session identity.
   * @param agentOptions - model route for the restored or created agent.
   * @param setup - publication-time composition that binds ownership.
   * @returns the published handle.
   */
  private async resumeOrCreate(id: SessionId, agentOptions: AgentOptions, setup: AgentSetup): Promise<AgentHandle> {
    if (this.ctx.get('sessionPersistence') !== undefined) {
      try {
        return await this.ctx.agents.resume({ resumeSessionId: id, agentOptions, setup })
      } catch (error: unknown) {
        // Corruption, unsupported formats, a held writer, and backend failures
        // stay loud: only a confirmed absent id may fall through to creation.
        if (!(error instanceof SessionPersistenceNotFoundError)) throw error
      }
    }
    return this.ctx.agents.create({ sessionId: id, meta: { cwd: this.cwd }, agentOptions, setup })
  }

  private hasAdapterFor(provider: string): boolean {
    return this.ctx.get('llm')?.listProviders().some(entry => entry.id === provider) ?? false
  }
}
