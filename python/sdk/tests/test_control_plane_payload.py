"""控制面 RPC 的线上契约：方法名与参数键必须与 harness 侧类型表一致。"""

from __future__ import annotations

from pydantic import BaseModel

from deepseek_harness import HarnessClient, JsonObject
from deepseek_harness.models import SessionAnswerResult, SessionCommandResult, SessionTitle


class _Recorder:
    """Record every request the client would send, without a live runtime."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, JsonObject | None]] = []
        self.result: BaseModel | None = None

    def __call__(
        self,
        method: str,
        params: JsonObject | None,
        *,
        response_model: type[BaseModel],
        **_kwargs: object,
    ) -> BaseModel:
        self.calls.append((method, params))
        return self.result if self.result is not None else response_model.model_construct()

    @property
    def last(self) -> tuple[str, JsonObject | None]:
        return self.calls[-1]


def _client() -> tuple[HarnessClient, _Recorder]:
    client = HarnessClient()
    recorder = _Recorder()
    client.request = recorder  # type: ignore[method-assign]
    return client, recorder


def test_session_lifecycle_requests_use_their_wire_methods() -> None:
    client, recorder = _client()

    client.cancel_session("s1")
    assert recorder.last == ("session/cancel", {"sessionId": "s1"})

    client.dispose_session("s1")
    assert recorder.last == ("session/dispose", {"sessionId": "s1"})

    client.session_history("s1")
    # 省略的游标不得出现：服务端用缺省值，而不是把 None 序列化过去
    assert recorder.last == ("session/history", {"sessionId": "s1"})

    client.session_history("s1", from_seq=7, limit=50)
    assert recorder.last == ("session/history", {"sessionId": "s1", "fromSeq": 7, "limit": 50})

    client.update_session_queue("s1", "m-1", {"kind": "remove"})
    assert recorder.last == ("session/queue/update",
                             {"sessionId": "s1", "itemId": "m-1", "action": {"kind": "remove"}})

    client.session_projections("s1")
    assert recorder.last == ("session/projections", {"sessionId": "s1"})

    client.sync_session("s1", from_seq=3)
    assert recorder.last == ("session/sync", {"sessionId": "s1", "fromSeq": 3})


def test_command_and_rename_return_their_typed_results() -> None:
    client, recorder = _client()

    recorder.result = SessionCommandResult(matched=True)
    assert client.run_session_command("s1", "/compact") is True
    assert recorder.last == ("session/command", {"sessionId": "s1", "line": "/compact"})

    recorder.result = SessionAnswerResult(accepted=False)
    assert client.answer_question("s1", "call-1", {"answers": []}) is False
    assert recorder.last == ("session/answer",
                             {"sessionId": "s1", "callId": "call-1", "answer": {"answers": []}})

    recorder.result = SessionTitle(title="Hi", seq=4)
    title = client.rename_session("s1", "  Hi  ")
    # 客户端不做本地规范化：服务端是唯一权威
    assert recorder.last == ("session/rename", {"sessionId": "s1", "title": "  Hi  "})
    assert (title.title, title.seq) == ("Hi", 4)


def test_model_selection_and_attachment_requests() -> None:
    client, recorder = _client()

    client.select_session_model("s1", "openai", "gpt-x")
    assert recorder.last == ("session/model/select",
                             {"sessionId": "s1", "provider": "openai", "model": "gpt-x"})

    client.select_session_model("s1", "openai", "gpt-x", reasoning_effort="high")
    assert recorder.last == ("session/model/select",
                             {"sessionId": "s1", "provider": "openai", "model": "gpt-x",
                              "reasoningEffort": "high"})

    client.read_session_attachment("s1", "sha256:image")
    assert recorder.last == ("session/attachment/read",
                             {"sessionId": "s1", "attachmentId": "sha256:image"})

    client.read_session_attachment("s1", "sha256:file", offset=5, length=10)
    assert recorder.last == ("session/attachment/read",
                             {"sessionId": "s1", "attachmentId": "sha256:file",
                              "offset": 5, "length": 10})

    client.import_session_attachment("s1", "report.txt", "报告.txt")
    assert recorder.last == ("session/attachment/import",
                             {"sessionId": "s1", "path": "report.txt", "name": "报告.txt"})


def test_catalog_requests_omit_absent_options() -> None:
    client, recorder = _client()

    client.list_sessions()
    assert recorder.last == ("session/list", {})

    client.list_sessions(limit=5)
    assert recorder.last == ("session/list", {"limit": 5})

    client.search_sessions("photon")
    assert recorder.last == ("session/search", {"query": "photon"})

    client.search_sessions("photon", limit=5, cursor="c-1")
    assert recorder.last == ("session/search", {"query": "photon", "limit": 5, "cursor": "c-1"})

    client.create_session()
    assert recorder.last == ("session/create", {})

    client.create_session("s2")
    assert recorder.last == ("session/create", {"sessionId": "s2"})

    client.fork_session("s1")
    assert recorder.last == ("session/fork", {"sessionId": "s1"})

    client.fork_session("s1", at_seq=9)
    assert recorder.last == ("session/fork", {"sessionId": "s1", "atSeq": 9})


def test_client_passes_an_empty_query_through_unchanged() -> None:
    client, recorder = _client()
    # 空串校验由服务端负责：客户端不得静默改写或拦截请求
    client.search_sessions("")
    assert recorder.last == ("session/search", {"query": ""})


def test_error_reason_is_read_from_structured_data() -> None:
    from deepseek_harness.errors import JsonRpcError

    error = JsonRpcError(-32020, "queued item is no longer pending",
                         {"reason": "queue-item-not-found", "itemId": "m-1"})
    assert error.reason == "queue-item-not-found"
    # 无结构化原因时不得猜测：返回 None，而不是从 message 里抠字符串
    assert JsonRpcError(-32603, "boom").reason is None
    assert JsonRpcError(-32603, "boom", {"other": 1}).reason is None


def test_history_alignment_and_list_fields_are_passed_and_parsed() -> None:
    from deepseek_harness.models import SessionListResult, SessionSummary

    client, recorder = _client()
    client.session_history("s1", align="message")
    assert recorder.last == ("session/history", {"sessionId": "s1", "align": "message"})

    rows = SessionListResult.model_validate({
        "sessions": [{
            "sessionId": "s1", "createdAt": 1, "updatedAt": 9, "blank": False,
            "title": "T", "live": True,
        }],
    })
    row: SessionSummary = rows.sessions[0]
    assert (row.createdAt, row.updatedAt, row.blank) == (1, 9, False)


def test_initialize_capabilities_are_typed() -> None:
    from deepseek_harness.models import InitializeResponse

    response = InitializeResponse.model_validate({
        "serverInfo": {"name": "deepseek-harness-sdk-runtime", "version": "0.0.1"},
        "capabilities": {"requests": ["session/prompt"], "notifications": ["session.event"]},
    })
    assert response.capabilities is not None
    assert response.capabilities.requests == ["session/prompt"]
