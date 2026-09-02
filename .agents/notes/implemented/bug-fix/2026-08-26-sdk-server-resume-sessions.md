# Agent Note: Resume persisted sessions in the JSON-RPC SDK server

Status: implemented

English | [中文](2026-08-26-sdk-server-resume-sessions.zh.md)

## Problem

The JSON-RPC SDK server's `createSession` unconditionally created a fresh empty session for every `sessionId`, even when that id already had a persisted log on disk. A new runtime process that reused a session id therefore built an empty session while persistence already held committed history for the same id. The persistence coordinator's `adoptLivePrefix` refused to overwrite the persisted prefix with an empty seed and threw an id-collision error; the server absorbed that error into the write-behind initialization, so every later write rejected and the JSONL never updated, while the model never saw the prior context. A client could only ever persist the first turn of a session, and never resume it across a process boundary.

## Decision

In `createSession`, when the surrounding composition exposes a session-persistence backend and the `sessionId` has a materialized stored log, the server resumes the session through `ctx.agents.resume(...)` instead of creating a fresh one. The resumed agent reloads the stored history and keeps appending to the same log, so a client can continue a session across server restarts. A `sessionId` with no stored log, or a composition without a persistence backend, still creates a fresh session through `ctx.agents.create(...)`.

Existence is checked with the persistence service's lightweight `listSnapshots()`, which reads only headers and never loads full logs. The server guards on the `listSnapshots` capability so contexts that expose other services through `get` never misclassify them as persistence.

## Alternatives considered

**Always create and let the coordinator merge or overwrite.** Rejected: the coordinator's `adoptLivePrefix` deliberately forbids overriding a persisted prefix with an empty live seed, which is exactly the collision that produced the bug.

**Always resume whenever a persistence backend is configured.** Rejected: resuming a session id with no stored log fails the persistence load. The server must check whether a log exists before choosing the resume path.

## Consequences

A client can now persist and resume a session across server restarts: stored history is reloaded into the model context and the log keeps appending instead of colliding. The trade-off is one header-only persistence listing per newly created session, which costs a directory walk per new session id and no full-log parse.
