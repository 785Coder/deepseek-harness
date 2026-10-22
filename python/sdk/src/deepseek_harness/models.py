from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

from pydantic import BaseModel, JsonValue as PydanticJsonValue

# Kept as implicit aliases (not PEP 695 `type` statements): this package declares
# `requires-python >= 3.10`, where the `type` statement is a syntax error.
JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | dict[str, "JsonValue"] | list["JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]


@dataclass(slots=True)
class Notification:
    method: str
    payload: JsonObject


@dataclass(slots=True)
class IncomingRequest:
    id: str | int
    method: str
    payload: JsonObject


class ServerInfo(BaseModel):
    name: str | None = None
    version: str | None = None


class InitializeCapabilities(BaseModel):
    """Request and notification methods the runtime build serves."""

    requests: list[str] = []
    notifications: list[str] = []


class InitializeResponse(BaseModel):
    serverInfo: ServerInfo | None = None
    capabilities: InitializeCapabilities | None = None


class SessionHistoryResult(BaseModel):
    """One sequence-aligned page of committed session events."""

    events: list[dict[str, PydanticJsonValue]]
    nextCursor: int | None = None
    hasMore: bool


class SessionProjections(BaseModel):
    """One consistent projection cut: every value reflects the same log position."""

    asOfSeq: int
    values: dict[str, PydanticJsonValue]


class SessionSyncResult(BaseModel):
    """Authoritative cut plus the log tail a reconnecting client missed."""

    nextSeq: int
    history: SessionHistoryResult
    projections: SessionProjections | None = None


class SessionTitle(BaseModel):
    """Accepted session title and the sequence of its durable event."""

    title: str
    seq: int


class SessionCommandResult(BaseModel):
    """Admission outcome of one slash-command line."""

    matched: bool


class ModelSelection(BaseModel):
    """Validated provider/model/reasoning route installed for one session."""

    provider: str
    model: str
    reasoningEffort: str | None = None


class SessionModelSelectionResult(BaseModel):
    """The route now in force for the session."""

    selected: ModelSelection


class SessionAttachmentResult(BaseModel):
    """One durable file/image reference and one base64 payload slice."""

    attachment: dict[str, PydanticJsonValue]
    data: str
    offset: int = 0
    totalBytes: int = 0
    nextOffset: int | None = None


class SessionAttachmentImportResult(BaseModel):
    """One durable verbatim file reference produced by an import."""

    attachment: dict[str, PydanticJsonValue]


class SessionQuestionWaitResult(BaseModel):
    remainingMs: int | None = None


class SessionAnswerResult(BaseModel):
    """Whether a late answer was accepted for a still-continued question."""

    accepted: bool
    state: str | None = None


class SessionSummary(BaseModel):
    """One listed session row."""

    sessionId: str
    createdAt: int
    updatedAt: int
    blank: bool
    cwd: str | None = None
    parentSession: str | None = None
    title: str | None = None
    live: bool


class SessionListResult(BaseModel):
    """Listed sessions, newest first."""

    sessions: list[SessionSummary]


class SessionSearchItem(BaseModel):
    """One session-level search hit."""

    sessionId: str
    snippet: str


class SessionSearchResult(BaseModel):
    """One page of session search hits."""

    items: list[SessionSearchItem]
    nextCursor: str | None = None


class SessionCreateResult(BaseModel):
    """Identity of the created or adopted session."""

    sessionId: str


class SessionForkResult(BaseModel):
    """Identity of the forked session."""

    sessionId: str
