/**
 * Named wire types for the DeepSeek Harness SDK runtime protocol: its
 * request/result pairs and server-to-client notification payloads
 * exchanged over the newline-delimited JSON-RPC stdio transport. The server
 * plugin (`@deepseek-ai/dsh-sdk-jsonrpc-server`) and SDK clients share these shapes;
 * `serverInfo.name` stays the wire-stable `deepseek-harness-sdk-runtime`.
 *
 * @module @deepseek-ai/dsh-sdk-protocol/types
 */

import type { ImageAttachmentRef } from '@deepseek-ai/dsh-attachment'
import type { ContentBlock, ReasoningEffortId, ToolCallId } from '@deepseek-ai/dsh-llm'
import type { AssistantStreamFrame } from '@deepseek-ai/dsh-agent'
import type { SessionEvent } from '@deepseek-ai/dsh-session'
import type { SubagentStopReason } from '@deepseek-ai/dsh-subagent'
import type { ApprovalOutcome } from '@deepseek-ai/dsh-user-approval/types'
import type { AskUserQuestionAnswer, AskUserQuestionItem } from '@deepseek-ai/dsh-user-questions/types'

/**
 * Stable reason token for one control-plane failure, carried as
 * `error.data.reason` on a JSON-RPC error response. The names mirror the Web
 * control plane's classification so a client can branch without parsing text.
 */
export type SdkErrorReason =
  | 'not-found'
  | 'not-attached'
  | 'session-not-owned'
  | 'writer-held'
  | 'conflict'
  | 'invalid-params'
  | 'method-not-found'
  | 'agent-busy'
  | 'queue-item-not-found'
  | 'queue-edit-non-text'
  | 'queue-edit-empty'
  | 'steer-unavailable'
  | 'title-invalid'
  | 'title-unavailable'
  | 'attachment-invalid'
  | 'model-unavailable'
  | 'answer-unavailable'
  | 'answer-invalid'
  | 'reply-queued'
  | 'command-unavailable'
  | 'search-unavailable'
  | 'list-unavailable'
  | 'history-unavailable'
  | 'fork-unavailable'
  | 'projections-unavailable'
  | 'internal'

/** Structured payload of one SDK control-plane error response. */
export interface SdkErrorData {
  /** Stable classification of the failure. */
  reason: SdkErrorReason
  /** Human-readable detail, safe to log and show. */
  [field: string]: unknown
}

/** Parameters for the process-wide SDK handshake. */
export interface InitializeParams {
  /** Working directory recorded on every SDK-created session's header. */
  cwd: string
  /** Trusted host allowlist for this runtime; omitted scopes stored sessions to cwd. */
  sessionIds?: string[]
  /** Provider route every SDK-created agent runs on. */
  provider: string
  /** Model name every SDK-created agent runs on (the server may mount a fallback adapter; see `HarnessSdkJsonRpcServer.initialize`). */
  model: string
  /** Optional adapter-owned reasoning effort for the selected provider/model route. */
  reasoningEffort?: ReasoningEffortId
  /** Optional positive output-token cap inherited by SDK-created agents and their in-process descendants. */
  maxTokens?: number
}

/** Wire-stable server identity returned by initialization. */
export interface InitializeResult {
  /** Wire-stable server identity (`deepseek-harness-sdk-runtime`) and version. */
  serverInfo: { name: string; version: string }
  /**
   * What this build serves, so a client can gate features instead of probing.
   * Both lists are additive: a missing entry means the method is unsupported.
   */
  capabilities: {
    /** Request method names (the same strings `HarnessSdkRequestMap` keys). */
    requests: string[]
    /** Server-to-client notification method names. */
    notifications: string[]
  }
}

/** One user turn on one SDK session. */
export interface SessionPromptParams {
  /** The SDK-side session id; an unknown id lazily creates the agent+session pair. */
  sessionId: string
  /** The prompt content blocks, sent verbatim as the user message. */
  contentBlocks: SdkPromptContentBlock[]
}

/** Inline raster input admitted into the runtime's durable attachment store. */
export interface SdkEncodedImageBlock {
  type: 'image'
  /** Canonical base64-encoded raster bytes. */
  data: string
  /** Declared raster MIME type, verified during admission. */
  mimeType: 'image/png' | 'image/jpeg' | 'image/webp' | 'image/gif'
}

/** SDK prompt input: ordinary durable blocks plus inline images awaiting admission. */
export type SdkPromptContentBlock = ContentBlock | SdkEncodedImageBlock

/** Durable enqueue receipt for one prompt. */
export interface SessionPromptResult {
  /** Identity of the queued user message. */
  messageId: string
}

/** Restore a persisted session without enqueueing a prompt. */
export interface SessionResumeParams {
  /** Persisted session identity; missing or already owned sessions are rejected. */
  sessionId: string
}

/** Wait for an existing SDK session and its managed descendants to finish. */
export interface SessionWaitParams {
  /** An SDK-owned session id; waiting never creates a session. */
  sessionId: string
}

/** Session whose effective working directory is requested; an unknown id creates the Session. */
export interface SessionWorkingDirectoryParams {
  /** SDK-side Session identity. */
  sessionId: string
}

/** Change one Session's effective directory without changing its origin or permissions. */
export interface SessionWorkingDirectorySetParams extends SessionWorkingDirectoryParams {
  /** Directory to enter; relative paths resolve against the Session's current directory. */
  path: string
}

/** Validated effective directory after a read, change, or missing-directory recovery. */
export interface SessionWorkingDirectoryResult {
  /** Absolute directory used by future independent operations and new processes. */
  cwd: string
}

/** Deployment-mapped SDK outcome: `ok` for an accepted result, `error` otherwise. */
export type SdkRunStatus = 'ok' | 'error'

/** `session.event` payload: one session-log event, streamed as it is recorded. */
export interface SessionEventNotification {
  /** Session the event belongs to (every session in the runtime, not only SDK-created ones). */
  sessionId: string
  /** The full session-log event envelope. */
  event: SessionEvent
}

/** One queue mutation: edit text, drop the item, or steer it into the running turn. */
export type SessionQueueAction =
  | { kind: 'edit'; content: SdkPromptContentBlock[] }
  | { kind: 'remove' }
  | { kind: 'steer' }

/** Parameters for mutating one still-pending queue item. */
export interface SessionQueueUpdateParams {
  /** SDK-owned session holding the queued item. */
  sessionId: string
  /** Pending message identity from the prompt result or `agent/inbox/spliced`. */
  itemId: string
  /** Requested mutation. */
  action: SessionQueueAction
}

/** Acknowledgement that a queue mutation was applied. */
export interface SessionQueueUpdateResult { accepted: true }

/** Parameters for reading one attached session's log page by sequence. */
export interface SessionHistoryParams {
  /** SDK-owned session whose committed events are read. */
  sessionId: string
  /** Inclusive first sequence; omitted starts at `0`. */
  fromSeq?: number
  /** Maximum events to return (1..1000, default 100). */
  limit?: number
  /** Exclusive backwards cursor for message-aligned pages; omitted starts at the tail. */
  beforeSeq?: number
  /**
   * `seq` pages forwards by event count using fromSeq. `message` pages backwards
   * by append-message count using beforeSeq, including every cited source.
   */
  align?: 'seq' | 'message'
}

/** One chronological history page over committed events. */
export interface SessionHistoryResult {
  /** Events in sequence order, starting at or after `fromSeq`. */
  events: SessionEvent[]
  /** Inclusive cursor for the next page, or `null` when this page reached the tail. */
  nextCursor: number | null
  /** Whether more committed events follow this page. */
  hasMore: boolean
}

/** Parameters for cancelling the active turn of one attached session. */
export interface SessionCancelParams {
  /** SDK-owned session whose current turn is cancelled. */
  sessionId: string
}

/** Parameters for releasing one attached session while keeping the runtime. */
export interface SessionDisposeParams {
  /** SDK-owned session to dispose; a later prompt may create it again. */
  sessionId: string
}

/** Parameters for executing one slash-command line against a session. */
export interface SessionCommandParams {
  /** SDK-owned session whose agent receives the command. */
  sessionId: string
  /** Complete command line, leading slash included. */
  line: string
}

/** Admission outcome of one slash-command line. */
export interface SessionCommandResult {
  /**
   * Whether a registered command matched the line. A miss (bad syntax or an
   * unknown name) logs nothing and runs nothing; a match logs its own
   * `command/run`/`command/done` pair into the session log.
   */
  matched: boolean
}

/** Parameters for renaming one session. */
export interface SessionRenameParams {
  /** SDK-owned session to retitle. */
  sessionId: string
  /** Proposed title; normalized before acceptance. */
  title: string
}

/** Accepted title and the sequence of its durable `session/title` event. */
export interface SessionRenameResult {
  /** Normalized title actually recorded. */
  title: string
  /** Sequence of the `session/title` event that pinned it. */
  seq: number
}

/** Parameters for reading one image attachment a session references. */
export interface SessionAttachmentParams {
  /** Session that must already reference the attachment. */
  sessionId: string
  /** Attachment identity taken from an image block in this session's log. */
  attachmentId: string
}

/** One durable image reference and its bytes. */
export interface SessionAttachmentResult {
  /** Durable normalized reference the store resolves. */
  attachment: ImageAttachmentRef
  /** Base64-encoded image payload. */
  data: string
}

/** Parameters for selecting the model one session uses from its next step on. */
export interface SessionModelSelectParams {
  /** SDK-owned session whose route changes. */
  sessionId: string
  /** Registered provider route. */
  provider: string
  /** Provider-owned model id. */
  model: string
  /** Adapter-owned reasoning effort; omitted restores the model's own default. */
  reasoningEffort?: string
}

/** Normalized route accepted for the session. */
export interface SessionModelSelectResult {
  selected: { provider: string; model: string; reasoningEffort?: string }
}

/** Parameters for aligning a reconnecting client with one session. */
export interface SessionSyncParams {
  /** Session to align with. */
  sessionId: string
  /** Inclusive first sequence the client still needs; omitted starts at `0`. */
  fromSeq?: number
  /** Maximum events in the returned tail page (1..1000, default 100). */
  limit?: number
}

/** Authoritative cut plus the log tail a reconnecting client missed. */
export interface SessionSyncResult {
  /**
   * Sequence the next committed event will use. Resume `fromSeq` here only when
   * `history.hasMore` is false; otherwise continue from `history.nextCursor`.
   */
  nextSeq: number
  /** Committed events at or after `fromSeq`, in the `session/history` shape. */
  history: SessionHistoryResult
  /** Projection baseline when the deployment mounts projections. */
  projections?: SessionProjectionsResult
}

/** Client panel claim on the upstream foreground question wait. */
export interface SessionQuestionWaitParams {
  sessionId: string
  callId: string
  action: 'attach' | 'release'
}

/** Host countdown; null means the question has no foreground wait. */
export interface SessionQuestionWaitResult { remainingMs: number | null }

/** Foreground request settlement, independent from any later answer. */
export interface SessionQuestionStateNotification {
  sessionId: string
  callId: string
  state: 'answered' | 'continued' | 'closed'
}

/** Parameters for answering one question whose foreground wait already ended. */
export interface SessionAnswerParams {
  /** SDK-owned session that owns the question. */
  sessionId: string
  /** Question identity the answer belongs to. */
  callId: string
  /** Complete structured answer batch, one item per question of the call. */
  answer: AskUserQuestionAnswer
}

/** Whether the question was still continued and accepted the reply. */
export interface SessionAnswerResult {
  /** Current durable question state; an open question may become continued after timeout. */
  state?: 'open' | 'continued' | 'settled'
  /**
   * True when the reply was queued for the session's next turn. False when the
   * question is no longer continued, i.e. there is nothing left to answer.
   */
  accepted: boolean
}

/** Parameters for listing stored sessions. */
export interface SessionListParams {
  /** Maximum rows to return (1..500, default 50), newest first. */
  limit?: number
}

/** One listed session row. */
export interface SessionListItem {
  /** Session identity. */
  sessionId: string
  /** Unix epoch milliseconds from the stored header. */
  createdAt: number
  /** Latest human prompt time, or `createdAt` when the log has none yet. */
  updatedAt: number
  /** Whether the folded prefix contains no turn. */
  blank: boolean
  /** Directory the session was created in, when recorded. */
  cwd?: string
  /** Session this one was forked from, when recorded. */
  parentSession?: string
  /** Latest log-backed title, when the log has one. */
  title?: string
  /** Whether the runtime currently holds a live Agent for it. */
  live: boolean
}

/** Listed sessions, newest first. */
export interface SessionListResult {
  sessions: SessionListItem[]
}

/** Parameters for searching stored sessions. */
export interface SessionSearchParams {
  /** Query text, interpreted as data and never as executable syntax. */
  query: string
  /** Maximum hits in this page (provider default when omitted). */
  limit?: number
  /** Opaque continuation cursor from a previous page of the identical query. */
  cursor?: string
}

/** One session-level search hit. */
export interface SessionSearchItem {
  /** Session holding the best matching message. */
  sessionId: string
  /** Matching message excerpt. */
  snippet: string
}

/** One page of session search hits. */
export interface SessionSearchResult {
  items: SessionSearchItem[]
  /** Opaque cursor for the next page, absent at the end of the result set. */
  nextCursor?: string
}

/** Parameters for creating (or adopting) one session identity. */
export interface SessionCreateParams {
  /** Explicit identity to create or adopt; omitted mints one. */
  sessionId?: string
}

/** Identity of the created or adopted session. */
export interface SessionCreateResult {
  sessionId: string
}

/** Parameters for forking a session from an exact event prefix. */
export interface SessionForkParams {
  /** Source session whose committed events seed the child. */
  sessionId: string
  /** Inclusive cut; omitted uses the latest completed turn. */
  atSeq?: number
}

/** Identity of the forked session. */
export interface SessionForkResult {
  sessionId: string
}

/** `session.created` payload: a session was created (or attached) in this runtime. */
export interface SessionCreatedNotification {
  /** The new session. */
  sessionId: string
}

/** `session.disposed` payload: a session left this runtime. */
export interface SessionDisposedNotification {
  /** The released session. */
  sessionId: string
}

/** Parameters for reading one session's projection baseline. */
export interface SessionProjectionsParams {
  /** Session whose client-visible projections are read. */
  sessionId: string
}

/** One consistent projection cut: every value reflects the same log position. */
export interface SessionProjectionsResult {
  /** Seq of the last event the values reflect (`-1` for an empty log). */
  asOfSeq: number
  /** Client-visible unit values keyed by projection name. */
  values: Readonly<Record<string, unknown>>
}

/** `session.projection-frame` payload: one unit's changed client view. */
export interface SessionProjectionFrameNotification {
  /** Session the projection belongs to. */
  sessionId: string
  /** Registered projection key, e.g. `usage` or `todo`. */
  projection: string
  /** Seq of the event that caused the change. */
  asOfSeq: number
  /** Schema-validated client view. */
  value: unknown
}

/** `session.assistant-stream` payload: one transient model-output frame. */
export interface SessionAssistantStreamNotification {
  /** Session whose attempt produced the frame. */
  sessionId: string
  /**
   * Native frame forwarded unchanged: `start`/`chunk`/`end` carrying attempt,
   * revision, and index. Frames are transient and never persisted; the durable
   * `assistant/attempt` and `assistant/message` events remain the final fact.
   */
  frame: AssistantStreamFrame
}

/** Whole-agent lifecycle state for one session. */
export interface SessionStatusNotification {
  /** Session whose live agent changed status. */
  sessionId: string
  /** The whole-agent state after the transition. */
  status: 'idle' | 'running'
}

/** `subagent.started` payload: an in-runtime child session was created. */
export interface SubagentStartedNotification {
  /** The delegating session. */
  parentSessionId: string
  /** The new child session. */
  childSessionId: string
}

/** `subagent.finished` payload: an in-process subagent run ended (remote runs are not reported). */
export interface SubagentFinishedNotification {
  /** Subagent provider name that ran the child. */
  provider: string
  /** The child agent's id (equals {@link childSessionId} for local runs). */
  agentId: string
  /** The delegating session. */
  parentSessionId: string
  /** The child session. */
  childSessionId: string
  /** Deployment-mapped run outcome. */
  status: SdkRunStatus
  /** The provider-reported stop reason. */
  stopReason: SubagentStopReason
  /** The child's selected assistant output; absent when the child produced none. */
  lastAssistantMessage?: ContentBlock[]
}

/** SDK task projection, independent of Host service imports. */
export interface SdkJobView {
  id: string
  kind: string
  label: string
  owner?: string
  status: 'running' | 'stopping' | 'completed' | 'killed' | 'failed'
  progress?: string
  detail?: string
  startedAt: number
  finishedAt?: number
  outputLimitBytes?: number
  output: { total: number; earliest: number; spillPaths?: readonly string[] }
}

/** Retained output with absolute UTF-8 byte coordinates. */
export interface SdkJobChunk {
  at: number
  text: string
  channel?: string
  gapBefore?: true
}

/** List or non-consuming output observation frame. */
export type SdkJobFrame =
  | { type: 'rows'; jobs: readonly SdkJobView[] }
  | { type: 'opened'; job: SdkJobView; from: number }
  | { type: 'output'; chunks: readonly SdkJobChunk[]; next: number; lossy?: true }
  | { type: 'status'; job: SdkJobView }

/** Stable observation failure categories. */
export type SdkJobErrorKind = 'session-not-owned' | 'job-not-found' | 'invalid-cursor'
  | 'jobs-unavailable' | 'subscription-limit' | 'ack-timeout' | 'invalid-params' | 'observation-failed'

/** Task query target. */
export interface SdkJobTarget { sessionId: string; jobId: string; from?: number }
/** Explicit observation target; client registers this id before sending. */
export interface SdkJobSubscriptionParams { subscriptionId: string; sessionId: string }
/** Single non-consuming ring read. */
export interface SdkJobReadResult {
  job: SdkJobView
  chunks: readonly SdkJobChunk[]
  next: number
  lossy: boolean
}
/** Sequenced frame with a one-frame acknowledgement window. */
export interface SdkJobFrameNotification { subscriptionId: string; seq: number; frame: SdkJobFrame }
/** Observation termination, distinct from a task's terminal status. */
export interface SdkJobClosedNotification {
  subscriptionId: string
  reason: 'completed' | 'unsubscribed' | 'removed' | 'owner-disposed' | 'error'
  error?: { kind: SdkJobErrorKind; message: string }
}

/** Server-to-client notifications by JSON-RPC method name. */
export interface HarnessSdkNotificationMap {
  'job.frame': SdkJobFrameNotification
  'job.closed': SdkJobClosedNotification
  'session.assistant-stream': SessionAssistantStreamNotification
  'session.created': SessionCreatedNotification
  'session.disposed': SessionDisposedNotification
  'session.projection-frame': SessionProjectionFrameNotification
  'session.event': SessionEventNotification
  'session.status': SessionStatusNotification
  'session.question-state': SessionQuestionStateNotification
  'subagent.started': SubagentStartedNotification
  'subagent.finished': SubagentFinishedNotification
}

/** Client-to-server request methods with their param and result shapes. */
export interface HarnessSdkRequestMap {
  'job/list': { params: { sessionId: string }; result: { jobs: readonly SdkJobView[] } }
  'job/read': { params: SdkJobTarget; result: SdkJobReadResult }
  'job/subscribe': { params: SdkJobSubscriptionParams; result: { subscriptionId: string } }
  'job/follow': { params: SdkJobSubscriptionParams & SdkJobTarget; result: { subscriptionId: string } }
  'job/unsubscribe': { params: { subscriptionId: string }; result: Record<string, never> }
  'initialize': { params: InitializeParams; result: InitializeResult }
  'session/prompt': { params: SessionPromptParams; result: SessionPromptResult }
  'session/projections': { params: SessionProjectionsParams; result: SessionProjectionsResult }
  'session/cancel': { params: SessionCancelParams; result: Record<string, never> }
  'session/dispose': { params: SessionDisposeParams; result: Record<string, never> }
  'session/history': { params: SessionHistoryParams; result: SessionHistoryResult }
  'session/command': { params: SessionCommandParams; result: SessionCommandResult }
  'session/rename': { params: SessionRenameParams; result: SessionRenameResult }
  'session/queue/update': { params: SessionQueueUpdateParams; result: SessionQueueUpdateResult }
  'session/attachment/read': { params: SessionAttachmentParams; result: SessionAttachmentResult }
  'session/model/select': { params: SessionModelSelectParams; result: SessionModelSelectResult }
  'session/sync': { params: SessionSyncParams; result: SessionSyncResult }
  'session/list': { params: SessionListParams; result: SessionListResult }
  'session/answer': { params: SessionAnswerParams; result: SessionAnswerResult }
  'session/question/wait': { params: SessionQuestionWaitParams; result: SessionQuestionWaitResult }
  'session/search': { params: SessionSearchParams; result: SessionSearchResult }
  'session/create': { params: SessionCreateParams; result: SessionCreateResult }
  'session/fork': { params: SessionForkParams; result: SessionForkResult }
  'session/resume': { params: SessionResumeParams; result: Record<string, never> }
  'session/wait': { params: SessionWaitParams; result: Record<string, never> }
  'session/working-directory/get': { params: SessionWorkingDirectoryParams; result: SessionWorkingDirectoryResult }
  'session/working-directory/set': { params: SessionWorkingDirectorySetParams; result: SessionWorkingDirectoryResult }
  'shutdown': { params: undefined; result: Record<string, never> }
}

/** Server→client 请求：某 SDK session 的 agent 向人类提问一批问题。 */
export interface SessionQuestionParams {
  /** 提问的 session（仅根 session；DELEGATED_CALLER 已被服务层拒绝）。 */
  sessionId: string
  /** 待渲染的问题批次，答案须覆盖每一项。 */
  questions: AskUserQuestionItem[]
  /** 发起提问的工具调用 id；生命周期直接提问时缺省，客户端不得自行补全。 */
  callId?: ToolCallId
  /**
   * 该提问是否处于"前台计时等待"。为真时客户端应自行倒计时；超时后本运行时把
   * 工具结果收敛为 pending，之后的答案改由 `session/answer` 并入下一轮。
   */
  timed?: boolean
}

/** Server→client 请求：某 SDK session 的 agent 请求一次批准。 */
export interface SessionApprovalParams {
  /** 请求批准的 session（含本服务端创建的子任务 session）。 */
  sessionId: string
  /** 待批准的工具名。 */
  toolName: string
  /** 请求方持有该工具调用时的调用 id。 */
  callId?: ToolCallId
  /** 请求方给出的人性化理由。 */
  reason?: string
}

/** Server→client 请求方法表；运行时铸请求 id，客户端应答时原样回显。 */
export interface HarnessSdkServerRequestMap {
  'session/question': { params: SessionQuestionParams; result: AskUserQuestionAnswer }
  'session/approval': { params: SessionApprovalParams; result: ApprovalOutcome }
}
