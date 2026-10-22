/**
 * Session ownership for the SDK JSON-RPC server.
 *
 * Authorization follows runtime object identity: a session belongs to this
 * server only while the exact Agent it created or restored is still live.
 * Durable session lineage (`header.parentSession`) is history, never a grant.
 *
 * @module @deepseek-ai/dsh-sdk-jsonrpc-server/ownership
 */

import type { Agent, AgentRegistry } from '@deepseek-ai/dsh-agent'
import type { SessionId } from '@deepseek-ai/dsh-session'

export class SessionOwnership {
  private readonly bound = new Map<string, Agent>()

  /**
   * Bind the exact Agent this server created or restored.
   * Called from the creation transaction's commit, i.e. before publication.
   * @param agent - the unpublished Agent about to be published.
   */
  bind(agent: Agent): void {
    const key: string = String(agent.id)
    const existing: Agent | undefined = this.bound.get(key)
    if (existing === agent) return
    if (existing !== undefined) {
      throw new Error(`SDK ownership for "${key}" is already bound to another live agent`)
    }
    this.bound.set(key, agent)
  }

  /**
   * Release a binding by object identity, so a disposed predecessor cannot
   * delete the binding of a newer agent that reused the same session id.
   * @param agent - the Agent being disposed.
   */
  release(agent: Agent): void {
    const key: string = String(agent.id)
    if (this.bound.get(key) === agent) this.bound.delete(key)
  }

  /**
   * Object-identity check for callers that already hold the Agent.
   * @param agent - candidate owner.
   * @returns true only for the bound instance itself.
   */
  ownsAgent(agent: Agent): boolean {
    return this.bound.get(String(agent.id)) === agent
  }

  /**
   * Id-based check for callers that only hold a session id (jobs frames).
   * Resolves the current live Agent first: a reused id must not inherit the
   * previous agent's authorization.
   * @param sessionId - candidate session id.
   * @param lookup - resolves the live Agent for an id.
   * @returns true only when the live Agent is the bound instance.
   */
  owns(sessionId: string, lookup: (id: SessionId) => Agent | undefined): boolean {
    const entry: Agent | undefined = this.bound.get(sessionId)
    if (entry === undefined) return false
    return lookup(entry.id) === entry
  }

  /**
   * Runtime child derivation: a child of a bound agent is bound too, because
   * its approvals belong to the same client. Only the live registry's parent
   * relation counts; durable lineage cannot establish it. An id that is
   * already bound (this agent or an earlier derived ancestor) passes without
   * a registry lookup.
   * @param childId - candidate child session id.
   * @param registry - live agent registry.
   * @returns true when the child or one of its runtime ancestors is bound.
   */
  isChildOfBound(childId: SessionId, registry: AgentRegistry): boolean {
    if (this.bound.has(String(childId))) return true
    for (const owner of this.bound.values()) {
      if (registry.isOwnedBy(childId, owner)) return true
    }
    return false
  }
}
