/** First-party image references shared by Session transports. */

import type { FileAttachmentRef, ImageAttachmentRef } from '@deepseek-ai/dsh-attachment'
import { assistantStreamChunks } from '@deepseek-ai/dsh-llm'
import type { SessionEvent } from '@deepseek-ai/dsh-session'

type AttachmentRef = ImageAttachmentRef | FileAttachmentRef

function attachmentBlockIn<T extends AttachmentRef>(
  content: unknown,
  blockType: 'image' | 'file',
  match: (ref: T) => boolean,
): T | undefined {
  if (!Array.isArray(content)) return undefined
  for (const value of content) {
    if (typeof value !== 'object' || value === null || Array.isArray(value)) continue
    const block = value as { readonly type?: unknown; readonly attachment?: unknown }
    if (block.type === blockType && typeof block.attachment === 'object' && block.attachment !== null) {
      const ref = block.attachment as T
      if (match(ref)) return ref
    }
  }
  return undefined
}

/** Read only first-party declared content fields; unknown event payloads stay opaque. */
function attachmentInEvent<T extends AttachmentRef>(
  event: SessionEvent,
  blockType: 'image' | 'file',
  match: (ref: T) => boolean,
): T | undefined {
  const blockIn = (content: unknown): T | undefined =>
    attachmentBlockIn<T>(content, blockType, match)
  const data = event.data as {
    readonly content?: unknown
    readonly message?: { readonly content?: unknown }
    readonly inserted?: unknown
    readonly summary?: unknown
    readonly rawOutput?: unknown
  }
  // First-party event payloads can be present without their producer plugin mounted.
  const type: string = event.type
  switch (type) {
    case 'user/message':
    case 'tool/ptc-dispatch':
      return blockIn(data.content)
    case 'system/message':
    case 'developer/message':
    case 'tool/result':
    case 'team/message/queued':
      return blockIn(data.message?.content)
    case 'agent/inbox/spliced': {
      const messages = data.inserted
      if (!Array.isArray(messages)) return undefined
      for (const message of messages as readonly unknown[]) {
        if (typeof message !== 'object' || message === null || Array.isArray(message)) continue
        const found = blockIn((message as { readonly content?: unknown }).content)
        if (found !== undefined) return found
      }
      return undefined
    }
    case 'compaction/summary':
      return blockIn(data.summary) ?? blockIn(data.rawOutput)
    case 'assistant/message': {
      const found = blockIn(data.message?.content)
      if (found !== undefined) return found
      break
    }
    case 'assistant/attempt': break
    default: return undefined
  }
  const assistant = event as SessionEvent<'assistant/message' | 'assistant/attempt'>
  for (const chunk of assistantStreamChunks(assistant.data.stream, 'block-end')) {
    const found = blockIn([chunk.block])
    if (found !== undefined) return found
  }
  return undefined
}

function imageInEvent(
  event: SessionEvent,
  match: (ref: ImageAttachmentRef) => boolean,
): ImageAttachmentRef | undefined {
  return attachmentInEvent<ImageAttachmentRef>(event, 'image', match)
}

function fileInEvent(
  event: SessionEvent,
  match: (ref: FileAttachmentRef) => boolean,
): FileAttachmentRef | undefined {
  return attachmentInEvent<FileAttachmentRef>(event, 'file', match)
}

/**
 * Find a referenced image in declared first-party content fields.
 * @param events - the authorized session observation.
 * @param attachmentId - durable image identity requested by the caller.
 * @returns the reference, or undefined when the session never referenced it.
 */
export function referencedImage(
  events: readonly SessionEvent[],
  attachmentId: string,
): ImageAttachmentRef | undefined {
  for (const event of events) {
    const found = imageInEvent(event, ref => String(ref.attachmentId) === attachmentId)
    if (found !== undefined) return found
  }
  return undefined
}

/**
 * Find a referenced verbatim file in declared first-party content fields.
 * @param events - the authorized session observation.
 * @param attachmentId - durable file identity requested by the caller.
 * @returns the reference, or undefined when the session never referenced it.
 */
export function referencedFile(
  events: readonly SessionEvent[],
  attachmentId: string,
): FileAttachmentRef | undefined {
  for (const event of events) {
    const found = fileInEvent(event, ref => String(ref.attachmentId) === attachmentId)
    if (found !== undefined) return found
  }
  return undefined
}
