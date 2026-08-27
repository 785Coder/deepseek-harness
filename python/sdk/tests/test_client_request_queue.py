"""ask-user（方案 C）在 client 侧新增的两个读取 API。

- ``HarnessClient.next_request_nowait``：非阻塞取运行时发来的请求。
- ``NotificationSubscription.next(timeout)``：带超时的订阅通知读取。

HarnessClient 不需要 start 也能测这两个 API：``_requests`` / 通知队列在
``__init__`` 就独立存在，请求帧经 ``_handle_message`` 入队（与既有
``test_client.py`` 的用法一致），异常项则直接 ``put`` 进队列（与
``_fail_waiters`` 的放异常项路径一致）。
"""

from __future__ import annotations

import queue

import pytest

from deepseek_harness import HarnessClient, IncomingRequest, Notification


def test_next_request_nowait_returns_none_on_empty_queue() -> None:
    client = HarnessClient()

    assert client.next_request_nowait() is None


def test_next_request_nowait_returns_queued_incoming_request() -> None:
    client = HarnessClient()
    client._handle_message({
        "jsonrpc": "2.0",
        "id": "question-1",
        "method": "session/question",
        "params": {"sessionId": "main", "questions": [{"id": "q1", "question": "continue?"}]},
    })

    request = client.next_request_nowait()

    assert request is not None
    assert isinstance(request, IncomingRequest)
    assert request.id == "question-1"
    assert request.method == "session/question"
    assert request.payload["sessionId"] == "main"
    assert request.payload["questions"] == [{"id": "q1", "question": "continue?"}]
    # 队列已取空：第二次返回 None 而非阻塞。
    assert client.next_request_nowait() is None


def test_next_request_nowait_raises_queued_exception() -> None:
    client = HarnessClient()
    # _fail_waiters 会把 TransportClosedError 等异常项放进 _requests，取时应上抛。
    client._requests.put(RuntimeError("runtime died"))

    with pytest.raises(RuntimeError, match="runtime died"):
        client.next_request_nowait()


def test_notification_subscription_next_timeout_raises_empty() -> None:
    client = HarnessClient()
    with client.subscribe_notifications() as subscription:
        with pytest.raises(queue.Empty):
            subscription.next(timeout=0.01)


def test_notification_subscription_next_returns_notification_with_timeout() -> None:
    client = HarnessClient()
    with client.subscribe_notifications() as subscription:
        client._handle_message({
            "jsonrpc": "2.0",
            "method": "session.event",
            "params": {"sessionId": "main", "event": {"type": "assistant/message"}},
        })

        notification = subscription.next(timeout=1)

        assert isinstance(notification, Notification)
        assert notification.method == "session.event"
        assert notification.payload["sessionId"] == "main"


def test_notification_subscription_next_default_timeout_returns_immediately() -> None:
    """无 timeout 调用在有通知时立即返回（不测阻塞语义，避免挂死）。"""
    client = HarnessClient()
    with client.subscribe_notifications() as subscription:
        client._handle_message({
            "jsonrpc": "2.0",
            "method": "session.status",
            "params": {"sessionId": "main", "status": "idle"},
        })

        notification = subscription.next()

        assert notification.method == "session.status"
