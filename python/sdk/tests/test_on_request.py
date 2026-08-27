"""api.py 的 ``Session.run(on_request=...)`` 轮询分支测试。

方案 C 让会话循环在等待通知的空闲期周期醒来（``_REQUEST_POLL_SECONDS``），
清空运行时发来的 ``session/question`` 请求并交给 on_request。本文件用两种方式
验证该分支：一个不启动子进程的桩（确定性验证轮询与入队），一个 fake-runtime
子进程（验证 respond 经 wire 到达运行时、循环正常结束）。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from deepseek_harness import (
    DeepSeekHarness,
    DeepSeekHarnessConfig,
    HarnessClient,
    IncomingRequest,
    Session,
)


def _receipt_notification() -> dict[str, object]:
    return {
        "jsonrpc": "2.0",
        "method": "session.event",
        "params": {
            "sessionId": "main",
            "event": {
                "type": "agent/inbox/spliced",
                "data": {"target": "next-turn", "start": 0, "inserted": [{"id": "message-1"}]},
            },
        },
    }


def test_session_run_with_on_request_drains_queued_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """不启动子进程：请求入队后 on_request 被调用，轮询分支正常消费。"""
    client = HarnessClient()
    # 预先建好订阅并推入通知，让循环能拿到 receipt 与 idle。
    subscription = client.subscribe_session_notifications("main")
    client._handle_message(_receipt_notification())
    client._handle_message({
        "jsonrpc": "2.0",
        "method": "session.status",
        "params": {"sessionId": "main", "status": "idle"},
    })
    # 运行时发来的请求已入队。
    client._requests.put(IncomingRequest(
        id="question-1",
        method="session/question",
        payload={"sessionId": "main", "questions": [{"id": "q1", "question": "Continue?"}]},
    ))
    monkeypatch.setattr(client, "session_prompt", lambda _session_id, _content_blocks, **_kwargs: "message-1")
    monkeypatch.setattr(client, "subscribe_session_notifications", lambda _session_id: subscription)

    harness = cast(
        DeepSeekHarness,
        SimpleNamespace(client=client, config=SimpleNamespace(session_root=None)),
    )
    seen: list[IncomingRequest] = []

    result = Session(harness, "main").run("ask me", on_request=seen.append)

    assert [request.id for request in seen] == ["question-1"]
    assert seen[0].method == "session/question"
    assert result.final_response == ""
    assert result.finish_reason is None
    # 请求队列已被清空，不会残留。
    assert client.next_request_nowait() is None


def test_session_run_without_on_request_uses_blocking_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """回归：不传 on_request 时走原阻塞路径（不做队列轮询、不消费请求）。"""
    client = HarnessClient()
    subscription = client.subscribe_session_notifications("main")
    client._handle_message(_receipt_notification())
    client._handle_message({
        "jsonrpc": "2.0",
        "method": "session.status",
        "params": {"sessionId": "main", "status": "idle"},
    })
    client._requests.put(IncomingRequest(
        id="question-1",
        method="session/question",
        payload={"sessionId": "main", "questions": []},
    ))
    monkeypatch.setattr(client, "session_prompt", lambda _session_id, _content_blocks, **_kwargs: "message-1")
    monkeypatch.setattr(client, "subscribe_session_notifications", lambda _session_id: subscription)

    harness = cast(
        DeepSeekHarness,
        SimpleNamespace(client=client, config=SimpleNamespace(session_root=None)),
    )

    result = Session(harness, "main").run("ask me")

    assert result.finish_reason is None
    # 原路径不消费请求队列：请求仍留在队列里。
    remaining = client.next_request_nowait()
    assert remaining is not None
    assert remaining.id == "question-1"


def test_session_run_on_request_answers_question_over_the_wire(tmp_path: Path) -> None:
    """fake-runtime 子进程：session/question 经 wire 到达 on_request，respond 回到运行时。"""
    script = tmp_path / "fake_runtime.py"
    answer_dump = tmp_path / "answer.json"
    script.write_text(
        """
import json
import os
import sys
import time

for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    if method == "initialize":
        print(json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": {"serverInfo": {"name": "fake-runtime"}}}), flush=True)
    elif method == "session/prompt":
        params = msg.get("params") or {}
        session_id = params["sessionId"]
        print(json.dumps({"jsonrpc": "2.0", "method": "session.event", "params": {"sessionId": session_id, "event": {"type": "agent/inbox/spliced", "data": {"target": "next-turn", "start": 0, "inserted": [{"id": "message-1"}]}}}}), flush=True)
        print(json.dumps({"jsonrpc": "2.0", "method": "session.status", "params": {"sessionId": session_id, "status": "running"}}), flush=True)
        print(json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": {"messageId": "message-1"}}), flush=True)
        print(json.dumps({"jsonrpc": "2.0", "id": "question-1", "method": "session/question", "params": {"sessionId": session_id, "questions": [{"id": "q1", "question": "Continue?", "options": [{"label": "Yes"}, {"label": "No"}]}]}}), flush=True)
        answer_seen = open(os.environ["ANSWER_DUMP"], "w")
        deadline = time.monotonic() + 5
        answered = False
        while time.monotonic() < deadline:
            answer_line = sys.stdin.readline()
            if not answer_line:
                break
            answer_msg = json.loads(answer_line)
            if answer_msg.get("id") == "question-1" and "result" in answer_msg:
                json.dump(answer_msg["result"], answer_seen)
                answered = True
                break
        if not answered:
            json.dump({}, answer_seen)
        answer_seen.close()
        print(json.dumps({"jsonrpc": "2.0", "method": "session.event", "params": {"sessionId": session_id, "event": {"type": "assistant/message", "data": {"content": [{"type": "text", "text": "done"}]}}}}), flush=True)
        print(json.dumps({"jsonrpc": "2.0", "method": "session.status", "params": {"sessionId": session_id, "status": "idle"}}), flush=True)
    elif method == "shutdown":
        print(json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": {}}), flush=True)
        break
""".strip()
    )

    seen: list[IncomingRequest] = []

    def on_request(request: IncomingRequest) -> None:
        seen.append(request)
        harness.client.respond(request.id, {"answers": [{"id": "q1", "selected": ["Yes"]}]})

    with DeepSeekHarness(
        DeepSeekHarnessConfig(
            launch_args_override=(sys.executable, str(script)),
            cwd=str(tmp_path),
            env={"ANSWER_DUMP": str(answer_dump)},
        )
    ) as harness:
        result = harness.run("ask me", session_id="main", on_request=on_request)

    assert [request.id for request in seen] == ["question-1"]
    assert json.loads(answer_dump.read_text()) == {"answers": [{"id": "q1", "selected": ["Yes"]}]}
    assert result.final_response == "done"
