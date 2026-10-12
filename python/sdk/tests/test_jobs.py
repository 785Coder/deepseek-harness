from __future__ import annotations

import queue
import threading

import pytest

from deepseek_harness.jobs import JobRowsSubscription, JobSubscriptionError, parse_rows
from deepseek_harness.client import HarnessClient
from deepseek_harness.errors import TransportClosedError


def test_credit_is_acknowledged_only_on_consumption() -> None:
    acknowledgements: list[tuple[str, int]] = []
    cancelled: list[str] = []
    subscription: JobRowsSubscription = JobRowsSubscription("s", parse_rows, lambda id, seq: acknowledgements.append((id, seq)), cancelled.append)
    subscription._receive(1, {"type": "rows", "jobs": []})
    assert acknowledgements == []
    assert subscription.next(timeout=0).jobs == []
    assert acknowledgements == [("s", 1)]
    with pytest.raises(queue.Empty):
        subscription.next(timeout=0)
    subscription._receive(2, {"type": "rows", "jobs": []})
    subscription._finish()
    assert subscription.next(timeout=0).jobs == []
    with pytest.raises(StopIteration):
        subscription.next(timeout=0)


def test_close_wakes_blocked_consumer_without_task_cancel() -> None:
    cancelled: list[str] = []
    subscription: JobRowsSubscription = JobRowsSubscription("s", parse_rows, lambda id, seq: None, cancelled.append)
    exited: threading.Event = threading.Event()
    def consume() -> None:
        with pytest.raises(StopIteration):
            subscription.next()
        exited.set()
    thread: threading.Thread = threading.Thread(target=consume)
    thread.start()
    subscription.close()
    assert exited.wait(1)
    thread.join()
    assert cancelled == ["s"]


def test_transport_failure_drains_cached_frame_then_raises() -> None:
    subscription: JobRowsSubscription = JobRowsSubscription("s", parse_rows, lambda id, seq: None, lambda id: None)
    subscription._receive(1, {"type": "rows", "jobs": []})
    subscription._finish(TransportClosedError("closed"))
    assert subscription.next(timeout=0).jobs == []
    with pytest.raises(TransportClosedError):
        subscription.next(timeout=0)


def test_ack_write_failure_preserves_already_received_frame() -> None:
    def acknowledge(id: str, seq: int) -> None:
        raise TransportClosedError("write failed before reader EOF")
    subscription: JobRowsSubscription = JobRowsSubscription("s", parse_rows, acknowledge, lambda id: None)
    subscription._receive(1, {"type": "rows", "jobs": []})
    assert subscription.next(timeout=0).jobs == []
    with pytest.raises(TransportClosedError):
        subscription.next(timeout=0)


def test_job_frames_never_enter_legacy_notification_queue() -> None:
    client: HarnessClient = HarnessClient()
    client._handle_message({"jsonrpc": "2.0", "method": "job.frame", "params": {"subscriptionId": "closed", "seq": 1, "frame": {"type": "rows", "jobs": []}}})
    assert client._notifications.empty()
