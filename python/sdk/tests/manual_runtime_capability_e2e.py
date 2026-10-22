"""Manual acceptance: a real dsh runtime + the Python SDK control plane and streaming.

Run it after `pnpm run build:artifacts` (lib mode) or against the source CLI
(src mode, the default). The runtime is launched through the SDK's `dsh_bin`
option, the provider points at a local Messages/SSE mock, and every
control-plane RPC plus the transient assistant-stream frames is exercised.

    cd python/sdk && uv run --offline python tests/manual_runtime_capability_e2e.py
    DSH_E2E_MODE=lib uv run --offline python tests/manual_runtime_capability_e2e.py
    DSH_E2E_MODE=bundled python tests/manual_runtime_capability_e2e.py   # installed runtime wheel

A manual check is intentional: it needs a built CLI (lib mode) or the tsx
workspace loader (src mode) and never runs under CI's plain-Node suite.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from deepseek_harness import HarnessClient, HarnessConfig, Notification
from deepseek_harness.errors import JsonRpcError
from deepseek_harness.models import InitializeResponse

REPO = str(Path(__file__).resolve().parents[3])
CLI_BIN = os.path.join(REPO, "apps", "cli", "lib", "bin.js")

failures: list[str] = []
checks: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        checks.append(name)
        print(f"  ok   {name}")
    else:
        failures.append(f"{name} {detail}".strip())
        print(f"  FAIL {name} {detail}")


# --------------------------------------------------------------------------- mock model

model_requests: list[dict] = []
model_lock = threading.Lock()


def sse(event: str, data: dict) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def turn_frames() -> list[bytes]:
    return [
        sse("message_start", {"type": "message_start", "message": {
            "id": "msg_e2e", "type": "message", "role": "assistant", "model": "deepseek-v4-pro",
            "content": [], "stop_reason": None, "stop_sequence": None,
            "usage": {"input_tokens": 7, "output_tokens": 0}}}),
        sse("content_block_start", {"type": "content_block_start", "index": 0,
                                    "content_block": {"type": "text", "text": ""}}),
        sse("content_block_delta", {"type": "content_block_delta", "index": 0,
                                    "delta": {"type": "text_delta", "text": "Hello"}}),
        sse("content_block_delta", {"type": "content_block_delta", "index": 0,
                                    "delta": {"type": "text_delta", "text": ", SDK."}}),
        sse("content_block_stop", {"type": "content_block_stop", "index": 0}),
        sse("message_delta", {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                              "usage": {"output_tokens": 3}}),
        sse("message_stop", {"type": "message_stop"}),
    ]


class ModelHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        length = int(self.headers.get("content-length") or 0)
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            body = {"raw": raw.decode("utf-8", "replace")}
        with model_lock:
            model_requests.append({"path": self.path, "body": body})
        payload = b"".join(turn_frames())
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
        self.wfile.flush()

    def log_message(self, *_args: object) -> None:  # silence stderr noise
        return


def start_model_server() -> tuple[ThreadingHTTPServer, int]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), ModelHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, server.server_address[1]


# --------------------------------------------------------------------------- helpers

def rpc_error(fn) -> JsonRpcError | None:
    try:
        fn()
    except JsonRpcError as error:
        return error
    return None


def events_of(notifications: list[Notification], session_id: str) -> list[dict]:
    out: list[dict] = []
    for note in notifications:
        if note.method != "session.event":
            continue
        if note.payload.get("sessionId") != session_id:
            continue
        event = note.payload.get("event")
        if isinstance(event, dict):
            out.append(event)
    return out


def frames_of(notifications: list[Notification], session_id: str) -> list[dict]:
    out: list[dict] = []
    for note in notifications:
        if note.method != "session.assistant-stream":
            continue
        if note.payload.get("sessionId") != session_id:
            continue
        frame = note.payload.get("frame")
        if isinstance(frame, dict):
            out.append(frame)
    return out


def run_turn(client: HarnessClient, session_id: str, text: str) -> list[Notification]:
    collected: list[Notification] = []
    with client.subscribe_session_notifications(session_id) as subscription:
        message_id = client.session_prompt(session_id, [{"type": "text", "text": text}])
        check(f"prompt returns a message id ({session_id})", isinstance(message_id, str) and len(message_id) > 0)
        saw_receipt = False
        while True:
            note = subscription.next(timeout=120)
            collected.append(note)
            if not saw_receipt and note.method == "session.event":
                event = note.payload.get("event")
                if isinstance(event, dict) and event.get("type") == "agent/inbox/spliced":
                    saw_receipt = True
            if (note.method == "session.status"
                    and note.payload.get("sessionId") == session_id
                    and note.payload.get("status") == "idle"):
                return collected


def main() -> int:
    work = tempfile.mkdtemp(prefix="dsh-sdk-e2e-")
    home = os.path.join(work, "home")
    cwd = os.path.join(work, "work")
    os.makedirs(cwd)
    mode = os.environ.get("DSH_E2E_MODE", "src")
    launcher = os.path.join(work, "dsh-sdk-launcher.sh")
    extra_env: dict[str, str] = {}
    if mode == "bundled":
        # Use the runtime executable shipped by the installed
        # deepseek-harness-runtime-bin wheel: no dsh_bin override at all.
        command = None
    elif mode == "lib":
        command = "node " + CLI_BIN
    else:
        tsx = subprocess.run(
            ["node", "--input-type=module", "-e", "console.log(import.meta.resolve('tsx/esm'))"],
            cwd=REPO, check=True, capture_output=True, text=True,
        ).stdout.strip()
        command = f"node --import {tsx} " + os.path.join(REPO, "apps", "cli", "src", "bin.ts")
        extra_env["TSX_TSCONFIG_PATH"] = os.path.join(REPO, "tsconfig.json")
    if command is not None:
        with open(launcher, "w", encoding="utf-8") as handle:
            handle.write("#!/bin/sh\nexec " + command + ' "$@"\n')
        os.chmod(launcher, 0o755)
    print(f"runtime mode: {mode}")

    model_server, port = start_model_server()
    print(f"mock model on 127.0.0.1:{port}; workdir {work}")

    env = {
        "DSH_HOME": home,
        "DSH_TELEMETRY_DISABLED": "1",
        "DSH_PERMISSION_MODE": "danger-full-access",
        "DEEPSEEK_API_KEY": "e2e-local-key",
        "DEEPSEEK_BASE_URL": f"http://127.0.0.1:{port}",
        **extra_env,
    }
    client = HarnessClient(HarnessConfig(
        **({} if command is None else {"dsh_bin": launcher}),
        profile="sdk",
        dsh_home=home,
        cwd=cwd,
        env=env,
        initialize_timeout_seconds=180.0,
        request_timeout_seconds=120.0,
        shutdown_timeout_seconds=10.0,
    ))
    exit_code = None
    try:
        client.start()
        init: InitializeResponse = client.initialize(
            cwd=cwd,
            provider="deepseek-official",
            model="deepseek-v4-pro",
            reasoning_effort="max",
            max_tokens=512,
            session_ids=("main",),
        )
        print("\ninitialize")
        check("server identity", bool(init.serverInfo and init.serverInfo.name == "deepseek-harness-sdk-runtime"),
              str(init.serverInfo))
        caps = init.capabilities
        check("capabilities present", caps is not None)
        requests = set(caps.requests if caps else [])
        notifications = set(caps.notifications if caps else [])
        expected_requests = {
            "initialize", "session/prompt", "session/cancel", "session/dispose", "session/resume",
            "session/history", "session/sync", "session/list", "session/search", "session/create",
            "session/fork", "session/rename", "session/command", "session/projections",
            "session/model/select", "session/attachment/read", "session/queue/update",
            "session/question/wait", "session/answer", "session/working-directory/get",
            "session/working-directory/set", "job/list", "job/read", "job/subscribe",
            "job/follow", "job/unsubscribe", "shutdown",
        }
        missing = sorted(expected_requests - requests)
        check("capabilities list every request method", not missing, f"missing={missing}")
        expected_notes = {
            "session.event", "session.status", "session.assistant-stream", "session.projection-frame",
            "session.created", "session.disposed", "session.question-state", "subagent.started",
            "subagent.finished",
        }
        missing_notes = sorted(expected_notes - notifications)
        check("capabilities list every notification method", not missing_notes, f"missing={missing_notes}")

        reinit = rpc_error(lambda: client.initialize(cwd=cwd, provider="deepseek-official", model="deepseek-v4-pro"))
        check("re-initialize is refused as a conflict", bool(reinit and reinit.reason == "conflict"),
              f"{reinit.reason if reinit else None}")

        print("\nsession creation and streaming turn")
        created = client.create_session("main")
        check("session/create attaches the requested id", created.sessionId == "main")
        first = run_turn(client, "main", "say hello")
        events = events_of(first, "main")
        types = [event.get("type") for event in events]
        check("turn reaches a durable assistant message", "assistant/message" in types, str(types))
        check("turn ends durably", "turn/end" in types, str(types))
        frames = frames_of(first, "main")
        if os.environ.get("DSH_E2E_DEBUG") is not None:
            print(json.dumps(frames, ensure_ascii=False)[:4000])
        check("assistant stream frames arrive", len(frames) >= 3, f"{len(frames)} frames")
        kinds = [frame.get("type") for frame in frames]
        check("assistant stream starts", kinds[:1] == ["start"], str(kinds))
        check("assistant stream ends", kinds[-1:] == ["end"], str(kinds))
        if frames:
            attempt = frames[0].get("attemptId")
            revisions = [frame.get("revision") for frame in frames]
            check("start frame carries attempt/revision/turn/step",
                  {"attemptId", "revision", "turn", "step"} <= set(frames[0]), json.dumps(frames[0]))
            check("every frame belongs to the same attempt", all(frame.get("attemptId") == attempt for frame in frames),
                  str({frame.get("attemptId") for frame in frames}))
            check("frame revisions are dense and monotone",
                  revisions == list(range(1, len(frames) + 1)), str(revisions))
            deltas = [frame for frame in frames if frame.get("type") == "chunk"]
            texts = [frame.get("chunk", {}).get("text") for frame in deltas
                     if isinstance(frame.get("chunk"), dict) and frame["chunk"].get("type") == "text-delta"]
            check("chunk frames carry streamed text deltas", "".join(t for t in texts if isinstance(t, str)) == "Hello, SDK.",
                  str(texts))
            end = frames[-1]
            check("end frame reports a committed outcome",
                  isinstance(end.get("outcome"), dict) and end["outcome"].get("kind") == "committed",
                  json.dumps(end))
        model_saw_turn = any("say hello" in json.dumps(request["body"]) for request in model_requests)
        check("the runtime routed the turn to the configured model endpoint", model_saw_turn,
              json.dumps(model_requests[:1])[:400])
        tools = model_requests[-1]["body"].get("tools") if model_requests else None
        check("the model request advertises the harness tool set",
              isinstance(tools, list) and any(tool.get("name") == "read" for tool in tools if isinstance(tool, dict)),
              str(type(tools)))

        print("\nprojections, history, sync")
        projections = client.session_projections("main")
        check("projections return a consistent cut", projections.asOfSeq >= 0, str(projections.asOfSeq))
        check("projections expose at least one unit", len(projections.values) > 0, str(sorted(projections.values)))
        page = client.session_history("main", from_seq=0, limit=2)
        check("history page is bounded", len(page.events) == 2, str(len(page.events)))
        check("history reports more after a short page", page.hasMore and page.nextCursor == 2, str(page.nextCursor))
        last = client.session_history("main", from_seq=0, limit=1000)
        check("history page covers the whole log without more", not last.hasMore, str(last.nextCursor))
        seqs = [event.get("seq") for event in last.events]
        check("history events are ordered by seq", seqs == sorted(seqs), str(seqs[:6]))
        aligned = client.session_history("main", align="message", limit=1)
        aligned_types = [event.get("type") for event in aligned.events]
        check("message-aligned history starts at a message unit",
              aligned_types[:1] in (["assistant/message"], ["user/message"]), str(aligned_types))
        check("message-aligned history keeps exactly one message per unit",
              sum(1 for kind in aligned_types if kind in {"user/message", "assistant/message"}) == 1,
              str(aligned_types))
        sync = client.sync_session("main", from_seq=0)
        check("sync reports the authoritative next seq", sync.nextSeq == max(seqs) + 1, f"{sync.nextSeq} vs {max(seqs) + 1}")
        check("sync returns the missed tail", len(sync.history.events) == len(seqs), str(len(sync.history.events)))
        check("sync folds a projection baseline", sync.projections is not None
              and sync.projections.asOfSeq < sync.nextSeq, str(sync.projections.asOfSeq if sync.projections else None))
        later = client.sync_session("main", from_seq=sync.nextSeq)
        check("sync past the cursor returns an empty tail", later.history.events == [], str(len(later.history.events)))

        print("\ntitles, commands, model selection")
        renamed = client.rename_session("main", "E2E 会话标题")
        check("rename returns the accepted title", renamed.title == "E2E 会话标题", renamed.title)
        check("rename returns a durable seq", renamed.seq >= 0, str(renamed.seq))
        matched_goal = client.run_session_command("main", "/goal")
        check("a registered slash command matches", matched_goal is True, str(matched_goal))
        matched_unknown = client.run_session_command("main", "/definitely-not-a-command")
        check("an unknown slash command does not match", matched_unknown is False, str(matched_unknown))
        selected = client.select_session_model("main", "deepseek-official", "deepseek-v4-pro", reasoning_effort="max")
        check("model selection is accepted", selected.selected.provider == "deepseek-official"
              and selected.selected.reasoningEffort == "max", selected.model_dump_json())
        selected_projections = client.session_projections("main")
        selection_value = selected_projections.values.get("modelSelection")
        check("model selection reaches the projection", isinstance(selection_value, dict)
              and selection_value.get("next", {}).get("model") == "deepseek-v4-pro",
              json.dumps(selection_value)[:200])

        print("\nquestions, errors, working directory")
        remaining = client.question_wait("main", "call-not-asked")
        check("question wait on an unasked call reports no countdown", remaining is None, str(remaining))
        released = client.question_wait("main", "call-not-asked", release=True)
        check("question wait release is idempotent", released is None, str(released))
        answered = client.answer_question_result("main", "call-not-asked", {"answers": []})
        check("answer to a settled question is a no-op", answered.accepted is False and answered.state == "settled",
              answered.model_dump_json())
        unknown = rpc_error(lambda: client.request("session/nope", {"sessionId": "main"}, response_model=dict))
        check("unknown method maps to method-not-found", bool(unknown and unknown.code == -32601
              and unknown.reason == "method-not-found"), f"{unknown.code if unknown else None}/{unknown.reason if unknown else None}")
        bad_history = rpc_error(lambda: client.session_history("main", limit=0))
        check("bad params map to -32602 invalid-params", bool(bad_history and bad_history.code == -32602
              and bad_history.reason == "invalid-params"), f"{bad_history.code if bad_history else None}")
        not_owned = rpc_error(lambda: client.session_prompt("other", [{"type": "text", "text": "hi"}]))
        check("a session outside the allowlist is refused", bool(not_owned and not_owned.reason in {"session-not-owned", "not-attached"}),
              f"{not_owned.reason if not_owned else None}")
        directory = client.get_working_directory("main")
        check("working directory resolves to the handshake cwd", directory == os.path.realpath(cwd), directory)

        print("\nlist, search, fork, dispose")
        listed = client.list_sessions(limit=10)
        row = next((entry for entry in listed.sessions if entry.sessionId == "main"), None)
        check("list includes the live session with its pinned title",
              row is not None and row.title == "E2E 会话标题" and row.live is True,
              json.dumps(row.model_dump() if row else {}))
        # The sdk profile opens the query index with openAt "never", so full-text
        # search must fail as a classified capability error, not an internal fault.
        search_error = rpc_error(lambda: client.search_sessions("hello", limit=5))
        check("search refusal is classified as search-unavailable",
              bool(search_error and search_error.reason == "search-unavailable"),
              f"{search_error.reason if search_error else None}: {search_error.message if search_error else ''}")
        forked = client.fork_session("main")
        check("fork returns a new session id", isinstance(forked.sessionId, str) and forked.sessionId != "main",
              forked.sessionId)
        child_history = client.session_history(forked.sessionId, from_seq=0, limit=500)
        child_types = [event.get("type") for event in child_history.events]
        check("forked session seeds the completed prefix", "assistant/message" in child_types, str(child_types))
        notifications_before = len(first)
        del notifications_before
        with client.subscribe_session_notifications("main") as subscription:
            client.dispose_session("main")
            disposed = None
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                note = subscription.next(timeout=10)
                if note.method == "session.disposed" and note.payload.get("sessionId") == "main":
                    disposed = note
                    break
            check("dispose announces session.disposed", disposed is not None)
        after_dispose = rpc_error(lambda: client.session_history("main", limit=1))
        check("a disposed session is no longer attached", bool(after_dispose and after_dispose.reason in {"not-attached", "not-owned"}),
              f"{after_dispose.reason if after_dispose else None}")
        remaining_rows = client.list_sessions(limit=10)
        check("disposal keeps the stored row", any(entry.sessionId == "main" for entry in remaining_rows.sessions))

        print("\nshutdown")
        client.close()
        exit_code = client._proc  # always None after close
        check("runtime closed", client._proc is None)
    finally:
        if client._stderr_lines:
            print("runtime stderr tail:", *list(client._stderr_lines)[-25:], sep="\n  ")
        try:
            client.close()
        except Exception as error:  # pragma: no cover - diagnostics only
            print("close failed:", error)
        model_server.shutdown()
        model_server.server_close()
        persisted = []
        for root, _dirs, files in os.walk(home):
            persisted.extend(os.path.join(root, name) for name in files if name.endswith(".jsonl.zstd")
                             or name.endswith(".jsonl"))
        print(f"\npersisted session files: {len(persisted)}")
        for path in persisted[:5]:
            print("  ", os.path.relpath(path, home))
        check("the runtime persisted the session log", len(persisted) > 0)
        shutil.rmtree(work, ignore_errors=True)

    print(f"\n{len(checks)} checks passed, {len(failures)} failed")
    for failure in failures:
        print("FAILED:", failure)
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
