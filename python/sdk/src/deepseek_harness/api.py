from __future__ import annotations

import queue
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .client import HarnessClient, HarnessConfig
from .errors import SdkProtocolError
from .models import (InitializeCapabilities, InitializeResponse, IncomingRequest, JsonObject, Notification, SessionAttachmentResult, SessionHistoryResult,
                     SessionListResult, SessionModelSelectionResult, SessionProjections, SessionSearchResult,
                     SessionSyncResult, SessionTitle)

# 仅 on_request 启用时轮询请求队列的时间片：会话循环阻塞在等待通知的空闲期，
# 也要周期醒来清空运行时发来的 session/question 请求，否则代理停在等待答案时
# 提问永远不会被处理。
_REQUEST_POLL_SECONDS = 0.05


@dataclass(slots=True)
class DeepSeekHarnessConfig:
    """Configuration for launching the local DeepSeek Harness SDK runtime.

    The runtime inherits the caller's environment by default, so existing
    DEEPSEEK_API_KEY and DEEPSEEK_BASE_URL settings keep working. Use ``env`` to
    intentionally override or inject variables for a subprocess.
    """

    provider: str = "deepseek-official"
    model: str = "deepseek-v4-flash"
    reasoning_effort: str | None = None
    max_tokens: int | None = None
    session_ids: tuple[str, ...] | None = None
    cwd: str | None = None
    runtime_cwd: str | None = None
    dsh_bin: str | None = None
    profile: str = "sdk"
    patches: tuple[str, ...] = ()
    dsh_home: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    initialize_timeout_seconds: float = 30.0
    request_timeout_seconds: float | None = None
    shutdown_timeout_seconds: float | None = 1.0
    base_url: str | None = None
    api_key: str | None = None


@dataclass(slots=True)
class RunResult:
    session_id: str
    final_response: str
    finish_reason: str | None
    events: list[JsonObject]
    notifications: list[Notification]


class DeepSeekHarness:
    """Reusable synchronous SDK for running DeepSeek Harness agent turns.

    The runtime subprocess starts lazily and remains owned by this instance
    across calls to :meth:`run`. Use the instance as a context manager, or call
    :meth:`close` explicitly when finished, so the subprocess is always reaped.
    """

    def __init__(
        self,
        config: DeepSeekHarnessConfig | None = None,
        *,
        _launch_args: tuple[str, ...] | None = None,
        **kwargs: object,
    ) -> None:
        if config is not None and kwargs:
            raise TypeError("pass either DeepSeekHarnessConfig or keyword options, not both")
        self.config = config or DeepSeekHarnessConfig(**kwargs)
        cwd = str(Path(self.config.cwd or Path.cwd()).resolve())
        runtime_cwd = str(Path(self.config.runtime_cwd).resolve()) if self.config.runtime_cwd is not None else cwd
        self._cwd = cwd
        env = dict(self.config.env)
        if self.config.base_url is not None:
            env["DEEPSEEK_BASE_URL"] = self.config.base_url
        if self.config.api_key is not None:
            env["DEEPSEEK_API_KEY"] = self.config.api_key

        self._client = HarnessClient(
            HarnessConfig(
                dsh_bin=self.config.dsh_bin,
                profile=self.config.profile,
                patches=self.config.patches,
                dsh_home=self.config.dsh_home,
                cwd=runtime_cwd,
                env=env,
                initialize_timeout_seconds=self.config.initialize_timeout_seconds,
                request_timeout_seconds=self.config.request_timeout_seconds,
                shutdown_timeout_seconds=self.config.shutdown_timeout_seconds,
            ),
            _launch_args=_launch_args,
        )
        self._initialized = False
        self._initialize_response: InitializeResponse | None = None

    @property
    def initialize_response(self) -> InitializeResponse | None:
        """Initialize handshake result; capabilities are validated by the host."""
        return self._initialize_response

    @property
    def capabilities(self) -> InitializeCapabilities | None:
        """Request/notification methods advertised by the runtime build."""
        response = self._initialize_response
        return None if response is None else response.capabilities

    def __enter__(self) -> "DeepSeekHarness":
        self.start()
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.close()

    @property
    def client(self) -> HarnessClient:
        return self._client

    def start(self) -> None:
        if self._initialized:
            return
        self._client.start()
        self._initialize_response = self._client.initialize(
            cwd=self._cwd,
            provider=self.config.provider,
            model=self.config.model,
            reasoning_effort=self.config.reasoning_effort,
            max_tokens=self.config.max_tokens,
            **({} if self.config.session_ids is None else {"session_ids": self.config.session_ids}),
        )
        self._initialized = True

    def close(self) -> None:
        self._client.close()
        self._initialized = False

    def start_session(self, session_id: str | None = None) -> "Session":
        self.start()
        return Session(self, session_id or f"session-{uuid.uuid4().hex}")

    def resume_session(self, session_id: str) -> "Session":
        """Restore persisted history from the configured Harness home without sending input.

        Missing, unreadable, incompatible, or occupied sessions raise instead of creating a new session.
        """
        self.start()
        self._client.resume_session(session_id)
        return Session(self, session_id)

    def create_session(self, session_id: str | None = None) -> "Session":
        """Create an attached session without submitting model input."""
        self.start()
        return Session(self, self.client.create_session(session_id).sessionId)

    def list_sessions(self, *, limit: int | None = None) -> SessionListResult:
        """List the sessions visible to this initialized runtime."""
        self.start()
        return self.client.list_sessions(limit=limit)

    def search_sessions(self, query: str, *, limit: int | None = None, cursor: str | None = None) -> SessionSearchResult:
        """Search authorized session messages."""
        self.start()
        return self.client.search_sessions(query, limit=limit, cursor=cursor)

    def run(
        self,
        input: str | list[JsonObject],
        *,
        session_id: str | None = None,
        on_notification: Callable[[Notification], None] | None = None,
        on_request: Callable[[IncomingRequest], None] | None = None,
    ) -> RunResult:
        return self.start_session(session_id).run(
            input, on_notification=on_notification, on_request=on_request
        )


class Session:
    def __init__(self, harness: DeepSeekHarness, session_id: str) -> None:
        self.harness = harness
        self.id = session_id

    def _ready_client(self) -> HarnessClient:
        self.harness.start()
        return self.harness.client

    def cancel(self) -> None:
        """Cancel the active turn while retaining queued input."""
        self._ready_client().cancel_session(self.id)

    def dispose(self) -> None:
        """Release this live session without shutting down the runtime."""
        self._ready_client().dispose_session(self.id)

    def rename(self, title: str) -> SessionTitle:
        """Accept a normalized, user-owned durable title."""
        return self._ready_client().rename_session(self.id, title)

    def command(self, line: str) -> bool:
        """Execute a slash command and return whether it matched."""
        return self._ready_client().run_session_command(self.id, line)

    def history(self, *, from_seq: int | None = None, limit: int | None = None,
                align: str | None = None, before_seq: int | None = None) -> SessionHistoryResult:
        """Read forwards by sequence or backwards by message with cited sources."""
        return self._ready_client().session_history(self.id, from_seq=from_seq, limit=limit, align=align, before_seq=before_seq)

    def sync(self, *, from_seq: int | None = None, limit: int | None = None) -> SessionSyncResult:
        """Read one consistent observation and its next sequence."""
        return self._ready_client().sync_session(self.id, from_seq=from_seq, limit=limit)

    def projections(self) -> SessionProjections:
        """Read registered client-visible projection values."""
        return self._ready_client().session_projections(self.id)

    def update_queue(self, item_id: str, action: JsonObject) -> None:
        """Edit, remove, or steer an input that is still pending."""
        self._ready_client().update_session_queue(self.id, item_id, action)

    def read_attachment(self, attachment_id: str) -> SessionAttachmentResult:
        """Read an image referenced by this session."""
        return self._ready_client().read_session_attachment(self.id, attachment_id)

    def select_model(self, provider: str, model: str, *, reasoning_effort: str | None = None) -> SessionModelSelectionResult:
        """Persist a session-local model choice for subsequent steps and resumes."""
        return self._ready_client().select_session_model(self.id, provider, model, reasoning_effort=reasoning_effort)

    def answer_question(self, call_id: str, answer: JsonObject) -> bool:
        """Admit an explicit answer to a continued timed question."""
        return self._ready_client().answer_question(self.id, call_id, answer)

    def fork(self, *, at_seq: int | None = None) -> "Session":
        """Create an attached session from a completed-turn or exact prefix."""
        result = self._ready_client().fork_session(self.id, at_seq=at_seq)
        return Session(self.harness, result.sessionId)

    def get_working_directory(self) -> str:
        """Read the effective directory, recovering a missing directory to the Session origin."""
        self.harness.start()
        return self.harness.client.get_working_directory(self.id)

    def set_working_directory(self, path: str) -> str:
        """Change the effective directory while existing processes retain their own directories."""
        self.harness.start()
        return self.harness.client.set_working_directory(self.id, path)

    def run(
        self,
        input: str | list[JsonObject],
        *,
        on_notification: Callable[[Notification], None] | None = None,
        on_request: Callable[[IncomingRequest], None] | None = None,
    ) -> RunResult:
        content_blocks = normalize_input(input)
        notifications: list[Notification] = []
        events: list[JsonObject] = []

        def collect(notification: Notification) -> None:
            notifications.append(notification)
            if on_notification is not None:
                on_notification(notification)
            if (
                notification.method == "session.event"
                and notification.payload.get("sessionId") == self.id
            ):
                event = notification.payload.get("event")
                if isinstance(event, dict):
                    events.append(event)

        with self.harness.client.subscribe_session_notifications(self.id) as subscription:
            message_id = self.harness.client.session_prompt(
                self.id,
                content_blocks,
                notification_subscription=subscription,
            )

            received = False
            while True:
                if on_request is not None:
                    while True:
                        req = self.harness.client.next_request_nowait()
                        if req is None:
                            break
                        on_request(req)
                    try:
                        notification = subscription.next(timeout=_REQUEST_POLL_SECONDS)
                    except queue.Empty:
                        continue
                else:
                    notification = subscription.next()
                if not received:
                    if not _is_inbox_receipt(notification, self.id, message_id):
                        continue
                    received = True
                collect(notification)
                if (
                    notification.method == "session.status"
                    and notification.payload.get("sessionId") == self.id
                    and notification.payload.get("status") == "idle"
                ):
                    break

        return RunResult(
            session_id=self.id,
            final_response=final_response(events),
            finish_reason=finish_reason(events),
            events=events,
            notifications=notifications,
        )


def _is_inbox_receipt(notification: Notification, session_id: str, message_id: str) -> bool:
    if notification.method != "session.event" or notification.payload.get("sessionId") != session_id:
        return False
    event = notification.payload.get("event")
    if not isinstance(event, dict) or event.get("type") != "agent/inbox/spliced":
        return False
    data = event.get("data")
    inserted = data.get("inserted") if isinstance(data, dict) else None
    return isinstance(inserted, list) and any(
        isinstance(message, dict) and message.get("id") == message_id for message in inserted
    )


def normalize_input(input: str | list[JsonObject]) -> list[JsonObject]:
    if isinstance(input, str):
        return [{"type": "text", "text": input}]
    return input


def final_response(events: list[JsonObject]) -> str:
    for event in reversed(events):
        if event.get("type") != "assistant/message":
            continue
        data = event.get("data")
        if not isinstance(data, dict):
            continue
        message = data.get("message")
        content_owner = message if isinstance(message, dict) else data
        content = content_owner.get("content")
        if not isinstance(content, list):
            continue
        parts: list[str] = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text") or ""))
        return "".join(parts)
    return ""


def finish_reason(events: list[JsonObject]) -> str | None:
    """Return the last turn-ending kind.

    The input must contain root-session events from one owned run interval.

    Raises:
        SdkProtocolError: The last ``turn/end`` has no string reason kind.
    """
    for event in reversed(events):
        if event.get("type") != "turn/end":
            continue
        data = event.get("data")
        reason = data.get("reason") if isinstance(data, dict) else None
        kind = reason.get("kind") if isinstance(reason, dict) else None
        if not isinstance(kind, str):
            raise SdkProtocolError("turn/end event requires a string data.reason.kind")
        return kind
    return None
