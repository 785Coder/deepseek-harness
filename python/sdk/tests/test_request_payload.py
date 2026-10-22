"""请求 payload 透传与“每请求只取一次”的回归（LDD P1/P2）。"""

from __future__ import annotations

from deepseek_harness import HarnessClient


def _question(call_id: str | None) -> dict:
    params: dict = {"sessionId": "main", "questions": [{"id": "q1", "question": "Continue?"}]}
    if call_id is not None:
        params["callId"] = call_id
    return {"jsonrpc": "2.0", "id": "q-1", "method": "session/question", "params": params}


def _approval() -> dict:
    return {
        "jsonrpc": "2.0",
        "id": "a-1",
        "method": "session/approval",
        "params": {"sessionId": "main", "toolName": "bash", "callId": "bash-1"},
    }


def test_question_request_preserves_call_id_in_payload() -> None:
    client = HarnessClient()
    client._handle_message(_question("call-1"))
    request = client.next_request_nowait()
    if request is None:
        raise AssertionError("question request was not queued")
    assert request.method == "session/question"
    # 原样透传：SDK 不推断、不补全，应用才能信任该字段
    assert request.payload["callId"] == "call-1"


def test_question_without_call_id_keeps_the_field_absent() -> None:
    client = HarnessClient()
    client._handle_message(_question(None))
    request = client.next_request_nowait()
    if request is None:
        raise AssertionError("question request was not queued")
    assert "callId" not in request.payload


def test_each_request_is_taken_exactly_once() -> None:
    client = HarnessClient()
    client._handle_message(_approval())
    client._handle_message(_question("call-2"))
    first = client.next_request_nowait()
    second = client.next_request_nowait()
    if first is None or second is None:
        raise AssertionError("expected two queued requests")
    assert [first.method, second.method] == ["session/approval", "session/question"]
    assert second.payload["callId"] == "call-2"
    assert client.next_request_nowait() is None
