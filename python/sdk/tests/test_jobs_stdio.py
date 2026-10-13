from __future__ import annotations

import sys
from pathlib import Path

import pytest

from deepseek_harness import (HarnessClient, HarnessConfig, JobAccessError, JobCursorError,
                              JobNotFoundError, JobOpenedFrame, JobOutputFrame, JobRemovedError,
                              JobStatusFrame, JobSubscriptionError)


def test_stdio_frames_before_receipt_and_ack_driven_completion(tmp_path: Path) -> None:
    script: Path = tmp_path / "runtime.py"
    script.write_text('''
import json, sys
job = {"id":"bash-1","owner":"root","kind":"bash","label":"test","status":"running","startedAt":0,"output":{"earliest":0,"total":7}}
id = None
for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    def frame(seq, value):
        print(json.dumps({"jsonrpc":"2.0","method":"job.frame","params":{"subscriptionId":id,"seq":seq,"frame":value}}), flush=True)
    if method == "job/follow":
        id = msg["params"]["subscriptionId"]
        frame(1, {"type":"opened","job":job,"from":0})
        print(json.dumps({"jsonrpc":"2.0","id":msg["id"],"result":{"subscriptionId":id}}), flush=True)
    elif method == "job/ack":
        seq = msg["params"]["seq"]
        if seq == 1:
            frame(2, {"type":"output","chunks":[{"at":0,"text":"中文\\n","channel":"stdout"}],"next":7})
        elif seq == 2:
            job["status"] = "completed"
            frame(3, {"type":"status","job":job})
        else:
            print(json.dumps({"jsonrpc":"2.0","method":"job.closed","params":{"subscriptionId":id,"reason":"completed"}}), flush=True)
    elif method == "shutdown":
        print(json.dumps({"jsonrpc":"2.0","id":msg["id"],"result":{}}), flush=True)
        break
''', encoding="utf-8")
    with HarnessClient(_launch_args=(sys.executable, str(script))) as client:
        with client.follow_job("root", "bash-1") as subscription:
            assert isinstance(subscription.next(timeout=2), JobOpenedFrame)
            output = subscription.next(timeout=2)
            assert isinstance(output, JobOutputFrame)
            assert output.chunks[0].text == "中文\n"
            assert output.next == 7
            assert isinstance(subscription.next(timeout=2), JobStatusFrame)
            with pytest.raises(StopIteration):
                subscription.next(timeout=2)
        assert client._notifications.empty()


def test_removed_and_error_closes_raise_typed_errors(tmp_path: Path) -> None:
    script: Path = tmp_path / "runtime.py"
    script.write_text('''
import json, os, sys
job = {"id":"bash-1","owner":"root","kind":"bash","label":"t","status":"completed","startedAt":0,"output":{"earliest":0,"total":7}}
id = None
for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    def frame(seq, value):
        print(json.dumps({"jsonrpc":"2.0","method":"job.frame","params":{"subscriptionId":id,"seq":seq,"frame":value}}), flush=True)
    if method == "job/follow":
        id = msg["params"]["subscriptionId"]
        frame(1, {"type":"opened","job":job,"from":0})
        print(json.dumps({"jsonrpc":"2.0","id":msg["id"],"result":{"subscriptionId":id}}), flush=True)
    elif method == "job/ack":
        seq = msg["params"]["seq"]
        if seq == 1:
            frame(2, {"type":"output","chunks":[{"at":0,"text":"x\\n","channel":"stdout"}],"next":2})
        elif seq == 2:
            frame(3, {"type":"status","job":job})
        elif seq == 3 and os.environ.get("DSD_CLOSE") == "removed":
            print(json.dumps({"jsonrpc":"2.0","method":"job.closed","params":{"subscriptionId":id,"reason":"removed"}}), flush=True)
        elif seq == 3:
            print(json.dumps({"jsonrpc":"2.0","method":"job.closed","params":{"subscriptionId":id,"reason":"error","error":{"kind":"ack-timeout","message":"Job frame acknowledgement timed out"}}}), flush=True)
    elif method == "shutdown":
        print(json.dumps({"jsonrpc":"2.0","id":msg["id"],"result":{}}), flush=True)
        break
''', encoding="utf-8")
    launch_args: tuple[str, ...] = (sys.executable, str(script))
    with HarnessClient(HarnessConfig(env={"DSD_CLOSE": "removed"}), _launch_args=launch_args) as client:
        with client.follow_job("root", "bash-1") as subscription:
            assert isinstance(subscription.next(timeout=2), JobOpenedFrame)
            assert isinstance(subscription.next(timeout=2), JobOutputFrame)
            assert isinstance(subscription.next(timeout=2), JobStatusFrame)
            with pytest.raises(JobRemovedError):
                subscription.next(timeout=2)
    with HarnessClient(HarnessConfig(env={"DSD_CLOSE": "error"}), _launch_args=launch_args) as client:
        with client.follow_job("root", "bash-1") as subscription:
            assert isinstance(subscription.next(timeout=2), JobOpenedFrame)
            assert isinstance(subscription.next(timeout=2), JobOutputFrame)
            assert isinstance(subscription.next(timeout=2), JobStatusFrame)
            with pytest.raises(JobSubscriptionError) as caught:
                subscription.next(timeout=2)
    # Pin the typed "message (kind)" format so a regression to str(dict) is caught.
    assert str(caught.value) == "Job frame acknowledgement timed out (ack-timeout)"


def test_request_errors_map_to_typed_exceptions(tmp_path: Path) -> None:
    script: Path = tmp_path / "runtime.py"
    script.write_text('''
import json, sys
for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    def err(kind, message):
        print(json.dumps({"jsonrpc":"2.0","id":msg["id"],"error":{"code":-32010,"message":message,"data":{"kind":kind}}}), flush=True)
    if method == "job/list":
        err("session-not-owned", "Session is not owned by this SDK client")
    elif method == "job/read":
        err("job-not-found", "Job is not visible in this session")
    elif method == "job/follow":
        err("invalid-cursor", "Cursor must be a non-negative safe integer")
    elif method == "shutdown":
        print(json.dumps({"jsonrpc":"2.0","id":msg["id"],"result":{}}), flush=True)
        break
''', encoding="utf-8")
    launch_args: tuple[str, ...] = (sys.executable, str(script))
    with HarnessClient(_launch_args=launch_args) as client:
        with pytest.raises(JobAccessError) as owned:
            client.list_jobs("foreign")
        assert "not owned" in str(owned.value)
        with pytest.raises(JobNotFoundError) as visible:
            client.read_job_output("root", "bash-1")
        assert "not visible" in str(visible.value)
        with pytest.raises(JobCursorError) as cursor:
            client.follow_job("root", "bash-1", from_offset=99)
        assert "Cursor" in str(cursor.value)
