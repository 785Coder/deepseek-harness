/** Message-aligned pages shared by Session transports. */

import { isAppendSurfaceEvent, SessionLogOffset } from '@deepseek-ai/dsh-session'
import type { SessionEvent, SessionLogOffset as Offset, SessionSeqCursor } from '@deepseek-ai/dsh-session'

/** Minimum history to retain around complete turns. */
export interface SessionTurnWindow {
  readonly minMessages: number
  readonly minTurns: number
}

/**
 * Read a backwards page, keeping each append message with its cited sources.
 * @param events - dense immutable event prefix at the requested cut.
 * @param beforeSeq - exclusive backwards cursor, or the cut's end.
 * @param maxMessages - number of append user/assistant messages to include.
 * @param throughSeq - inclusive observation cut.
 * @param turnWindow - optional minimum turn and message window.
 * @returns a chronological contiguous page and whether an older prefix remains.
 */
export function paginateSessionHistory(
  events: readonly SessionEvent[],
  beforeSeq: Offset | undefined,
  maxMessages: number,
  throughSeq: SessionSeqCursor,
  turnWindow?: SessionTurnWindow,
): { readonly events: SessionEvent[]; readonly hasMore: boolean } {
  const end = SessionLogOffset(Math.min(throughSeq + 1, beforeSeq ?? throughSeq + 1))
  let count = 0
  let turns = 0
  let cut = SessionLogOffset(0)
  for (let index = end - 1; index >= 0; index--) {
    const event = events[index] as SessionEvent
    if (turnWindow !== undefined && event.type === 'turn/start') {
      turns++
      if (count >= turnWindow.minMessages && turns >= turnWindow.minTurns) {
        cut = SessionLogOffset(index)
        break
      }
    }
    if ((event.type !== 'user/message' && event.type !== 'assistant/message') || !isAppendSurfaceEvent(event)) continue
    count++
    let groupStart = event.seq
    for (const source of event.sourceEventSeqs ?? []) {
      if (source < groupStart) groupStart = source
    }
    if (count >= maxMessages) {
      cut = SessionLogOffset(groupStart)
      break
    }
  }
  return { events: events.slice(cut, end), hasMore: cut > 0 }
}
