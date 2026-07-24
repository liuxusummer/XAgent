from __future__ import annotations

import asyncio
import json
import tempfile
import threading
import unittest
import queue
import json
import time
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from src.core.eval import create_eval_run, import_dataset_content
from src.core.agent_loop import AgentContext, exhaust
from src.core.team_workflows import normalize_team_workflow, run_team_workflow
from src.core.telemetry import Event, JsonlSink, MultiSink, NullSink
from src.core.XAgent import XAgent
from src.core.skills import SkillRegistry
from src.handler import XAgentHandler
from src.main import build_system_prompt, build_team_step_runner, filter_tools_schema
from src.tools.file_ops import write_file
from src.web_ui_new import (
    SubmitTaskRequest,
    ReplyRequest,
    UISession,
    EvalDatasetDownloadRequest,
    EvalDatasetImportRequest,
    EvalRunCreateRequest,
    WorkspaceCreateRequest,
    WorkspaceFileWriteRequest,
    ChatCreateRequest,
    ScheduledTaskRunRequest,
    ScheduledTaskStatusRequest,
    ScheduledTaskWriteRequest,
    TeamWorkflowWriteRequest,
    TeamWriteRequest,
    _resolve_frontend_file_path,
    _agent_runtime_config,
    _cleanup_sessions,
    _drain_background,
    _emit,
    _eval_cancel_events,
    _find_session_by_chat,
    _get_session,
    _persist_chat_state,
    _queue_state,
    _register_session,
    _run_due_scheduled_tasks,
    _resolve_workspace_dir_input,
    _session_lock,
    _sessions,
    _shutdown_sessions,
    _web_runtime_stopping,
    app,
    build_dir,
    create_scheduled_task,
    create_workspace,
    create_chat,
    delete_scheduled_task,
    delete_chat,
    list_scheduled_tasks,
    list_workspace_template_options,
    list_chats,
    api_cancel_eval_run,
    api_create_eval_run,
    api_download_eval_dataset,
    api_get_eval_dataset,
    api_get_eval_run,
    api_import_eval_dataset,
    api_list_eval_datasets,
    api_list_eval_runs,
    parse_agent_markdown,
    read_workspace_file,
    read_workspace_tree,
    read_workspace_index_stats,
    refresh_workspace_index,
    read_agent_profile,
    search_workspace_index,
    serialize_agent_markdown,
    send_reply,
    submit_task,
    stop_task,
    stream_chat,
    preview_workspace_file,
    write_agent_profile,
    write_workspace_file,
    list_teams,
    read_team,
    write_team,
    delete_team,
    read_team_workflow_api,
    write_team_workflow_api,
    AgentProfileWriteRequest,
    WorkspaceIndexRefreshRequest,
    WebSessionUsageSink,
    _resolve_trace_log_dir,
    _usage_summary_from_events,
    _update_session_messages,
    read_trace_session_detail,
    read_trace_sessions,
    read_chat,
    read_usage_summary,
    run_scheduled_task_now,
    set_scheduled_task_status,
    update_scheduled_task,
)


class _FakeAgent:
    def __init__(self, running: bool = False) -> None:
        self.reply_queue = _Queue()
        self._running = running
        self.stopped = False
        self.stop_count = 0

    def is_running(self) -> bool:
        return self._running

    def stop(self) -> None:
        self.stopped = True
        self.stop_count += 1

    def close(self) -> None:
        self.stopped = True


class _Queue:
    def __init__(self) -> None:
        self.items: list[str] = []

    def put(self, item: str) -> None:
        self.items.append(item)


class _BlockingReplyAgent:
    def __init__(self) -> None:
        self.reply_queue: queue.Queue[str] = queue.Queue()
        self.display_queue: queue.Queue[dict] = queue.Queue()
        self.stop_event = threading.Event()
        self._running = True
        self.worker = threading.Thread(target=self._wait_for_reply, daemon=True)
        self.worker.start()

    def _wait_for_reply(self) -> None:
        self.reply_queue.get()
        exit_reason = "INTERRUPTED" if self.stop_event.is_set() else "CURRENT_TASK_DONE"
        self.display_queue.put(
            {
                "done": {
                    "response": "",
                    "exit_reason": exit_reason,
                    "tool_results": [],
                    "turns": 1,
                }
            }
        )
        self._running = False

    def is_running(self) -> bool:
        return self._running

    def stop(self) -> None:
        self.stop_event.set()


class _Request:
    def __init__(self, payload: dict[str, str]) -> None:
        self.payload = payload

    async def json(self) -> dict[str, str]:
        return self.payload


class _StreamRequest:
    def __init__(self, session_id: str) -> None:
        self.query_params = {"session_id": session_id}

    async def is_disconnected(self) -> bool:
        return False


class _NoopThread:
    def __init__(self, *args, **kwargs) -> None:
        del args, kwargs

    def start(self) -> None:
        return None


class _ImmediateThread:
    def __init__(self, *args, **kwargs) -> None:
        self.target = kwargs.get("target") or args[0]
        self.args = kwargs.get("args", ())
        self.kwargs = kwargs.get("kwargs", {})

    def start(self) -> None:
        self.target(*self.args, **self.kwargs)


class _BlockingContextLock:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()

    def __enter__(self):
        self.entered.set()
        self.release.wait(1)
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        return None


class _EvalFakeAgent:
    def __init__(self) -> None:
        self.closed = False

    def run_task(self, task: str) -> dict:
        return {
            "response": f"ok {task}",
            "exit_reason": "CURRENT_TASK_DONE",
            "tool_results": [{"tool_name": "file_read"}],
            "turns": 1,
        }

    def close(self) -> None:
        self.closed = True


class _HistoryBackend:
    def __init__(self, history: list[dict] | None = None) -> None:
        self.history = history or []


class _HistoryFakeAgent(_FakeAgent):
    def __init__(self, history: list[dict] | None = None) -> None:
        super().__init__()
        self.client = type("Client", (), {"backend": _HistoryBackend(history)})()
        self.handler = type("H", (), {"ctx": type("C", (), {"verbose": False, "sink": None})()})()
        self.display_queue = queue.Queue()
        self.ran_tasks: list[str] = []

    def run_task_async(self, task: str) -> None:
        self.ran_tasks.append(task)
        self.client.backend.history.append({"role": "user", "content": task})
        self.display_queue.put(
            {
                "done": {
                    "response": "ok",
                    "exit_reason": "CURRENT_TASK_DONE",
                    "tool_results": [],
                    "turns": 1,
                }
            }
        )


class _FakeDownloadResponse:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0
        self.url = "https://example.test/eval.jsonl"
        self.headers = {"Content-Length": str(len(data))}

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self.data) - self.offset
        chunk = self.data[self.offset : self.offset + size]
        self.offset += len(chunk)
        return chunk

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        return None


class WebUINewSecurityTests(unittest.TestCase):
    def test_frontend_file_resolution_rejects_paths_outside_build_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            frontend_root = root / "dist"
            frontend_root.mkdir()
            asset = frontend_root / "app.js"
            asset.write_text("safe", encoding="utf-8")
            secret = root / "secret.txt"
            secret.write_text("hidden", encoding="utf-8")

            safe_path, safe_contained = _resolve_frontend_file_path(str(frontend_root), "app.js")
            escaped_path, escaped_contained = _resolve_frontend_file_path(
                str(frontend_root),
                "../secret.txt",
            )

        self.assertTrue(safe_contained)
        self.assertEqual(safe_path, str(asset.resolve()))
        self.assertFalse(escaped_contained)
        self.assertIsNone(escaped_path)

    def test_spa_route_rejects_encoded_path_traversal(self) -> None:
        if not (Path(build_dir) / "index.html").is_file():
            self.skipTest("frontend build is not available")
        client = TestClient(app, client=("127.0.0.1", 50000))

        response = client.get("/%2e%2e/%2e%2e/%2e%2e/pyproject.toml")

        self.assertEqual(response.status_code, 404)
        self.assertNotIn('name = "xagent"', response.text)

    def test_cors_rejects_untrusted_origin(self) -> None:
        client = TestClient(app, client=("127.0.0.1", 50000))

        response = client.options(
            "/api/workspace/file",
            headers={
                "Origin": "https://example.invalid",
                "Access-Control-Request-Method": "PUT",
                "Access-Control-Request-Headers": "content-type",
            },
        )

        self.assertEqual(response.status_code, 403)
        self.assertNotEqual(
            response.headers.get("access-control-allow-origin"),
            "https://example.invalid",
        )

    def test_cors_allows_local_vite_origin(self) -> None:
        client = TestClient(app, client=("127.0.0.1", 50000))

        response = client.options(
            "/api/workspace/file",
            headers={
                "Origin": "http://127.0.0.1:5173",
                "Access-Control-Request-Method": "PUT",
                "Access-Control-Request-Headers": "content-type",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers.get("access-control-allow-origin"),
            "http://127.0.0.1:5173",
        )
        self.assertIsNone(response.headers.get("access-control-allow-credentials"))

    def test_web_app_rejects_non_loopback_client(self) -> None:
        client = TestClient(app, client=("192.0.2.10", 50000))

        response = client.get("/api/workspace/tools")

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["error"], "Web UI only accepts local connections")


class WebUINewSessionRoutingTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self) -> None:
        _web_runtime_stopping.clear()
        _sessions.clear()

    async def test_reply_targets_requested_session(self) -> None:
        left_agent = _FakeAgent()
        right_agent = _FakeAgent()
        _sessions["left"] = UISession(agent=left_agent, waiting_for_user=True, session_id="left")
        _sessions["right"] = UISession(agent=right_agent, waiting_for_user=True, session_id="right")

        with patch("src.web_ui_new._drain_background"):
            result = await send_reply(ReplyRequest(reply="ok", session_id="right"))

        self.assertTrue(result["success"])
        self.assertEqual(left_agent.reply_queue.items, [])
        self.assertEqual(right_agent.reply_queue.items, ["ok"])

    async def test_stop_targets_requested_session(self) -> None:
        left_agent = _FakeAgent(running=True)
        right_agent = _FakeAgent(running=True)
        _sessions["left"] = UISession(agent=left_agent, session_id="left")
        _sessions["right"] = UISession(agent=right_agent, session_id="right")

        result = await stop_task(_Request({"session_id": "right"}))

        self.assertTrue(result["success"])
        self.assertFalse(left_agent.stopped)
        self.assertTrue(right_agent.stopped)

    async def test_stop_unblocks_waiting_session_and_is_idempotent(self) -> None:
        agent = _FakeAgent(running=True)
        session = UISession(
            agent=agent,
            session_id="waiting",
            waiting_for_user=True,
            waiting_started_at=time.time(),
            ask_prompt="continue?",
        )
        _sessions[session.session_id] = session

        with (
            patch("src.web_ui_new._drain_background") as drain,
            patch("src.web_ui_new._persist_chat_state", return_value=True) as persist,
            patch("src.web_ui_new.threading.Thread", _ImmediateThread),
        ):
            first = await stop_task(_Request({"session_id": session.session_id}))
            second = await stop_task(_Request({"session_id": session.session_id}))

        self.assertTrue(first["success"])
        self.assertEqual(second, {"success": True, "data": {"already_stopping": True}})
        self.assertEqual(agent.stop_count, 1)
        self.assertEqual(agent.reply_queue.items, [""])
        self.assertFalse(session.waiting_for_user)
        self.assertTrue(session.running)
        self.assertTrue(session.stop_requested)
        self.assertEqual(session.ask_prompt, "")
        self.assertEqual([event["type"] for event in session.events], ["stop"])
        drain.assert_called_once_with(session)
        persist.assert_called_once_with(session, force=True)

    async def test_stop_waiting_session_reaches_interrupted_terminal_state(self) -> None:
        agent = _BlockingReplyAgent()
        session = UISession(
            agent=agent,
            session_id="waiting",
            waiting_for_user=True,
            waiting_started_at=time.time(),
            ask_prompt="continue?",
        )
        _sessions[session.session_id] = session

        result = await stop_task(_Request({"session_id": session.session_id}))
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            with session.lock:
                if not session.running:
                    break
            await asyncio.sleep(0.01)
        agent.worker.join(timeout=1)

        self.assertTrue(result["success"])
        self.assertFalse(agent.worker.is_alive())
        self.assertFalse(session.running)
        self.assertFalse(session.waiting_for_user)
        self.assertFalse(session.stop_requested)
        self.assertEqual([event["type"] for event in session.events], ["stop", "done"])
        self.assertEqual(session.events[-1]["data"]["exit_reason"], "INTERRUPTED")
        self.assertTrue(session.event_queue.get_nowait()["finished"])

    def test_queue_state_is_bounded_wakeup_without_event_copy(self) -> None:
        session = UISession(session_id="bounded")
        session.events.append({"type": "log", "data": "hello"})

        _queue_state(session, finished=False)
        _queue_state(session, finished=False)

        state = session.event_queue.get_nowait()
        self.assertNotIn("events", state)
        self.assertTrue(session.event_queue.empty())

    def test_finished_wakeup_replaces_stale_wakeup(self) -> None:
        session = UISession(session_id="finished")

        _queue_state(session, finished=False)
        _queue_state(session, finished=True)

        state = session.event_queue.get_nowait()
        self.assertTrue(state["finished"])
        self.assertTrue(session.event_queue.empty())

    def test_done_merges_tool_result_error_status(self) -> None:
        session = UISession(session_id="tools")
        _update_session_messages(
            session,
            "tool_call",
            {"id": "t1", "name": "web_download", "arguments": {}, "status": "running"},
        )

        _update_session_messages(
            session,
            "done",
            {
                "response": "done",
                "exit_reason": "CURRENT_TASK_DONE",
                "tool_results": [
                    {
                        "tool_name": "web_download",
                        "tool_call_id": "t1",
                        "data": {"status": "ERROR", "error": "unknown tool"},
                    }
                ],
            },
        )

        tool_call = session.messages[-1]["toolCalls"][0]
        self.assertEqual(tool_call["status"], "error")
        self.assertEqual(tool_call["result"]["error"], "unknown tool")

    def test_done_merges_duplicate_tool_names_in_order(self) -> None:
        session = UISession(session_id="tools")
        for tool_id in ("a", "b"):
            _update_session_messages(
                session,
                "tool_call",
                {"id": tool_id, "name": "web_scan", "arguments": {}, "status": "running"},
            )

        _update_session_messages(
            session,
            "done",
            {
                "tool_results": [
                    {"tool_name": "web_scan", "tool_call_id": "tool_1", "status": "OK"},
                    {"tool_name": "web_scan", "tool_call_id": "tool_1", "status": "OK"},
                ],
            },
        )

        tool_calls = session.messages[-1]["toolCalls"]
        self.assertEqual([item["status"] for item in tool_calls], ["success", "success"])

    async def test_stream_reads_events_from_session_log_not_queue_payload(self) -> None:
        session = UISession(session_id="stream")
        session.events.append({"type": "assistant_delta", "data": "hello"})
        session.running = False
        session.waiting_for_user = False
        _sessions["stream"] = session
        _queue_state(session, finished=True)

        response = await stream_chat(_StreamRequest("stream"))
        chunks: list[str] = []
        async for chunk in response.body_iterator:
            chunks.append(chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk)

        payload = "".join(chunks)
        self.assertIn('"type": "assistant_delta"', payload)
        self.assertIn("hello", payload)

    def test_web_usage_sink_emits_delta_and_done_events(self) -> None:
        session = UISession(session_id="usage")
        sink = WebSessionUsageSink(session)

        sink.emit(
            Event(
                session_id="run-1",
                turn=1,
                kind="llm_end",
                name="stop",
                data={"input_tokens": 3, "output_tokens": 2, "total_tokens": 5},
            )
        )
        sink.emit(
            Event(
                session_id="run-1",
                turn=1,
                kind="run_end",
                name="CURRENT_TASK_DONE",
                data={"input_tokens": 3, "output_tokens": 2, "total_tokens": 5},
            )
        )

        self.assertEqual(session.events[0]["type"], "token_usage_delta")
        self.assertEqual(session.events[0]["data"]["totals"]["total_tokens"], 5)
        self.assertEqual(session.events[1]["type"], "token_usage_done")
        self.assertFalse(session.event_queue.empty())
        self.assertEqual(session.persist_revision, 0)

    def test_drain_wait_does_not_hold_global_session_registry_lock(self) -> None:
        session = UISession(session_id="left", running=True)
        entered = threading.Event()
        release = threading.Event()

        class _BlockingDisplayQueue:
            def get(self, timeout: float):
                del timeout
                entered.set()
                release.wait(1)
                with session.lock:
                    session.running = False
                raise queue.Empty

        agent = _FakeAgent(running=False)
        agent.display_queue = _BlockingDisplayQueue()
        session.agent = agent
        worker = threading.Thread(target=_drain_background, args=(session,), daemon=True)
        worker.start()
        self.assertTrue(entered.wait(1))

        acquired = _session_lock.acquire(timeout=0.05)
        try:
            self.assertTrue(acquired)
        finally:
            if acquired:
                _session_lock.release()
            release.set()
            worker.join(timeout=1)
        self.assertFalse(worker.is_alive())

    def test_session_lookup_does_not_hold_registry_lock_while_waiting_for_session(self) -> None:
        blocker = _BlockingContextLock()
        session = UISession(session_id="blocked")
        session.lock = blocker
        _sessions[session.session_id] = session
        worker = threading.Thread(target=_get_session, args=(session.session_id,), daemon=True)
        worker.start()
        self.assertTrue(blocker.entered.wait(1))

        acquired = _session_lock.acquire(timeout=0.05)
        try:
            self.assertTrue(acquired)
        finally:
            if acquired:
                _session_lock.release()
            blocker.release.set()
            worker.join(timeout=1)
        self.assertFalse(worker.is_alive())

    def test_chat_lookup_does_not_hold_registry_lock_while_waiting_for_session(self) -> None:
        blocker = _BlockingContextLock()
        session = UISession(
            session_id="blocked",
            chat_id="chat",
            chat_ws="default.ws",
            chat_agent="coding",
        )
        session.lock = blocker
        _sessions[session.session_id] = session
        worker = threading.Thread(
            target=_find_session_by_chat,
            args=("default.ws", "coding", "chat"),
            daemon=True,
        )
        worker.start()
        self.assertTrue(blocker.entered.wait(1))

        acquired = _session_lock.acquire(timeout=0.05)
        try:
            self.assertTrue(acquired)
        finally:
            if acquired:
                _session_lock.release()
            blocker.release.set()
            worker.join(timeout=1)
        self.assertFalse(worker.is_alive())

    def test_cleanup_does_not_hold_registry_lock_while_waiting_for_session(self) -> None:
        blocker = _BlockingContextLock()
        session = UISession(
            session_id="blocked",
            running=True,
            running_started_at=time.time(),
        )
        session.lock = blocker
        _sessions[session.session_id] = session
        worker = threading.Thread(target=_cleanup_sessions, daemon=True)
        worker.start()
        self.assertTrue(blocker.entered.wait(1))

        acquired = _session_lock.acquire(timeout=0.05)
        try:
            self.assertTrue(acquired)
        finally:
            if acquired:
                _session_lock.release()
            blocker.release.set()
            worker.join(timeout=1)
        self.assertFalse(worker.is_alive())

    def test_chat_persistence_is_debounced_and_force_flushes(self) -> None:
        session = UISession(
            session_id="persist",
            chat_id="chat",
            chat_ws="default.ws",
            chat_agent="coding",
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(Path(tmp_dir))),
                patch("src.web_ui_new._write_json_file") as write_json,
            ):
                _emit(session, "user_task", "hello")
                self.assertEqual(write_json.call_count, 0)
                self.assertTrue(_persist_chat_state(session, force=True))
                self.assertEqual(write_json.call_count, 2)

                _emit(session, "assistant_delta", "partial")
                self.assertFalse(_persist_chat_state(session))
                self.assertEqual(write_json.call_count, 2)

                self.assertTrue(_persist_chat_state(session, force=True))
                self.assertEqual(write_json.call_count, 4)

    def test_cleanup_removes_only_idle_sessions_and_enforces_capacity(self) -> None:
        expired_agent = _FakeAgent()
        expired = UISession(
            agent=expired_agent,
            session_id="expired",
            last_activity_at=10,
            persist_revision=1,
        )
        active = UISession(
            agent=_FakeAgent(),
            session_id="active",
            running=True,
            last_activity_at=10,
        )
        fresh = UISession(session_id="fresh", last_activity_at=95)
        _sessions.update({"expired": expired, "active": active, "fresh": fresh})

        with patch("src.web_ui_new._persist_chat_state", return_value=True) as persist:
            removed = _cleanup_sessions(now=100, idle_ttl_seconds=20, max_sessions=10)

        self.assertEqual(removed, ["expired"])
        persist.assert_called_once_with(expired, force=True)
        self.assertNotIn("expired", _sessions)
        self.assertIn("active", _sessions)
        self.assertIn("fresh", _sessions)
        self.assertTrue(expired_agent.stopped)

        active.running = False
        active.last_activity_at = 90
        fresh.last_activity_at = 95
        removed = _cleanup_sessions(now=100, idle_ttl_seconds=1000, max_sessions=1)

        self.assertEqual(removed, ["active"])
        self.assertEqual(list(_sessions), ["fresh"])

    def test_cleanup_times_out_running_and_waiting_sessions(self) -> None:
        running_agent = _FakeAgent(running=True)
        waiting_agent = _FakeAgent()
        running = UISession(
            agent=running_agent,
            session_id="running",
            running=True,
            running_started_at=10,
            last_activity_at=90,
        )
        waiting = UISession(
            agent=waiting_agent,
            session_id="waiting",
            waiting_for_user=True,
            waiting_started_at=10,
            last_activity_at=90,
        )
        _sessions.update({"running": running, "waiting": waiting})

        with patch("src.web_ui_new._persist_chat_state", return_value=True) as persist:
            removed = _cleanup_sessions(
                now=100,
                idle_ttl_seconds=1000,
                running_ttl_seconds=20,
                waiting_ttl_seconds=20,
                max_sessions=10,
            )

        self.assertEqual(removed, ["running", "waiting"])
        self.assertEqual(persist.call_count, 2)
        self.assertFalse(running.running)
        self.assertFalse(waiting.waiting_for_user)
        self.assertTrue(running.closing)
        self.assertTrue(waiting.closing)
        self.assertTrue(running_agent.stopped)
        self.assertTrue(waiting_agent.stopped)
        self.assertEqual(waiting_agent.reply_queue.items, [""])
        self.assertEqual(running.events[-1]["type"], "stop")
        self.assertEqual(waiting.events[-1]["type"], "stop")

    def test_register_enforces_hard_capacity_and_evicts_only_inactive_sessions(self) -> None:
        inactive = UISession(session_id="inactive", last_activity_at=10)
        _sessions[inactive.session_id] = inactive

        admitted = UISession(session_id="admitted")
        self.assertTrue(_register_session(admitted, max_sessions=1))
        self.assertNotIn(inactive.session_id, _sessions)
        self.assertIs(_sessions.get(admitted.session_id), admitted)

        _sessions.clear()
        active = UISession(
            session_id="active",
            running=True,
            running_started_at=time.time(),
        )
        _sessions[active.session_id] = active
        rejected = UISession(session_id="rejected")

        self.assertFalse(_register_session(rejected, max_sessions=1))
        self.assertEqual(list(_sessions), [active.session_id])
        self.assertNotIn(rejected.session_id, _sessions)

    def test_runtime_shutdown_rejects_new_session_admission(self) -> None:
        _web_runtime_stopping.set()

        self.assertFalse(_register_session(UISession(session_id="late")))
        self.assertFalse(_sessions)

    def test_shutdown_flushes_stops_and_closes_all_sessions(self) -> None:
        agent = _FakeAgent(running=True)
        session = UISession(
            agent=agent,
            session_id="running",
            running=True,
            running_started_at=time.time(),
        )
        _sessions[session.session_id] = session

        with patch("src.web_ui_new._persist_chat_state", return_value=True) as persist:
            removed = _shutdown_sessions()

        self.assertEqual(removed, [session.session_id])
        persist.assert_called_once_with(session, force=True)
        self.assertFalse(_sessions)
        self.assertFalse(session.running)
        self.assertTrue(session.closing)
        self.assertTrue(agent.stopped)
        self.assertIsNone(session.agent)
        self.assertTrue(session.event_queue.get_nowait()["finished"])


class WebUINewUsageSummaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_default_trace_dir_resolves_to_workspace_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(Path(tmp_dir))),
                patch.dict("os.environ", {"XAGENT_LOG_DIR": ""}, clear=False),
            ):
                log_dir = _resolve_trace_log_dir(ws="default.ws")

        self.assertEqual(log_dir, str(Path(tmp_dir) / "default.ws" / "runtime" / "traces"))

    async def test_env_log_dir_overrides_workspace_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(Path(tmp_dir) / "workspace")),
                patch.dict("os.environ", {"XAGENT_LOG_DIR": tmp_dir}, clear=False),
            ):
                log_dir = _resolve_trace_log_dir(ws="default.ws")

        self.assertEqual(log_dir, tmp_dir)

    async def test_config_log_dir_overrides_workspace_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_log_dir = Path(tmp_dir) / "configured"
            config_path = Path(tmp_dir) / "observability.json"
            config_path.write_text(f'{{"log_dir": "{config_log_dir}"}}', encoding="utf-8")

            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(Path(tmp_dir) / "workspace")),
                patch.dict("os.environ", {"XAGENT_LOG_DIR": ""}, clear=False),
            ):
                log_dir = _resolve_trace_log_dir(
                    observability_config_path=str(config_path),
                    ws="default.ws",
                )

        self.assertEqual(log_dir, str(config_log_dir))

    async def test_summary_api_uses_default_workspace_trace_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(Path(tmp_dir))),
                patch.dict("os.environ", {"XAGENT_LOG_DIR": ""}, clear=False),
            ):
                result = await read_usage_summary(ws="default.ws")

        self.assertTrue(result["success"])
        self.assertTrue(result["data"]["configured"])
        self.assertEqual(
            result["data"]["log_dir"],
            str(Path(tmp_dir) / "default.ws" / "runtime" / "traces"),
        )
        self.assertEqual(result["data"]["totals"]["total_tokens"], 0)

    async def test_summary_api_aggregates_run_end_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_file = Path(tmp_dir) / "s1.jsonl"
            log_file.write_text(
                "\n".join(
                    [
                        '{"session_id":"s1","kind":"run_start","ts":1,"data":{}}',
                        '{"session_id":"s1","kind":"run_end","name":"CURRENT_TASK_DONE","ts":2,"duration_ms":1200,"data":{"turns":2,"input_tokens":10,"output_tokens":4,"total_tokens":14}}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            with patch.dict("os.environ", {"XAGENT_LOG_DIR": tmp_dir}, clear=False):
                result = await read_usage_summary(limit=10)

        self.assertTrue(result["success"])
        self.assertTrue(result["data"]["configured"])
        self.assertEqual(result["data"]["totals"]["total_tokens"], 14)
        self.assertEqual(result["data"]["sessions"][0]["turns"], 2)

    async def test_trace_sessions_api_lists_default_workspace_logs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir) / "default.ws" / "runtime" / "traces"
            log_dir.mkdir(parents=True)
            (log_dir / "s1.jsonl").write_text(
                "\n".join(
                    [
                        '{"session_id":"s1","kind":"run_start","ts":1,"data":{}}',
                        '{"session_id":"s1","kind":"llm_end","ts":1.5,"duration_ms":50,"data":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}',
                        '{"session_id":"s1","kind":"run_end","name":"CURRENT_TASK_DONE","ts":2,"duration_ms":1000,"data":{"turns":1,"input_tokens":2,"output_tokens":1,"total_tokens":3}}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(Path(tmp_dir))),
                patch.dict("os.environ", {"XAGENT_LOG_DIR": ""}, clear=False),
            ):
                result = await read_trace_sessions(ws="default.ws", limit=10)

        self.assertTrue(result["success"])
        self.assertEqual(result["data"]["sessions"][0]["session_id"], "s1")
        self.assertEqual(result["data"]["sessions"][0]["event_count"], 3)
        self.assertEqual(result["data"]["sessions"][0]["usage"]["total_tokens"], 3)

    async def test_trace_session_detail_returns_events_and_rejects_unsafe_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir) / "default.ws" / "runtime" / "traces"
            log_dir.mkdir(parents=True)
            (log_dir / "s1.jsonl").write_text(
                '{"session_id":"s1","kind":"run_end","name":"CURRENT_TASK_DONE","ts":2,"duration_ms":1000,"data":{"turns":1}}\n',
                encoding="utf-8",
            )

            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(Path(tmp_dir))),
                patch.dict("os.environ", {"XAGENT_LOG_DIR": ""}, clear=False),
            ):
                result = await read_trace_session_detail("s1", ws="default.ws")
                unsafe = await read_trace_session_detail("../s1", ws="default.ws")

        self.assertTrue(result["success"])
        self.assertEqual(result["data"]["summary"]["session_id"], "s1")
        self.assertEqual(len(result["data"]["events"]), 1)
        self.assertFalse(unsafe["success"])


class WebUINewAgentTeamTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self) -> None:
        _sessions.clear()

    def _write_agent(self, root: Path, name: str, tools: list[str] | None = None) -> None:
        agent_dir = root / "default.ws" / "system" / "agents" / name
        agent_dir.mkdir(parents=True, exist_ok=True)
        tool_lines = "\n".join(f'  - "{tool}"' for tool in (tools or ["file_read"]))
        agent_dir.joinpath("AGENT.md").write_text(
            (
                f'---\nname: "{name}"\ndescription: ""\ntools:\n{tool_lines}\n'
                'model: "dev-model"\nmaxTurns: 12\nmemory: "project"\nskills: []\nproject_agents: []\n---\n\n'
                f"# {name}\n"
            ),
            encoding="utf-8",
        )

    async def test_team_api_crud_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self._write_agent(root, "main")
            self._write_agent(root, "coding")
            with patch("src.web_ui_new._WORKSPACE_ROOT", str(root)):
                written = await write_team(
                    TeamWriteRequest(
                        ws="default.ws",
                        team={
                            "name": "dev-team",
                            "description": "Dev team",
                            "leader": "main",
                            "mode": "leader_delegates",
                            "members": [{"agent": "coding", "role": "implementation", "autoDelegate": True}],
                        },
                    )
                )
                listed = await list_teams(ws="default.ws")
                read = await read_team(ws="default.ws", team="dev-team")
                deleted = await delete_team(ws="default.ws", team="dev-team")

        self.assertTrue(written["success"])
        self.assertEqual(written["data"]["members"][0]["agent"], "coding")
        self.assertTrue(listed["success"])
        self.assertEqual(listed["data"][0]["name"], "dev-team")
        self.assertTrue(read["success"])
        self.assertEqual(read["data"]["leader"], "main")
        self.assertTrue(deleted["success"])

    async def test_list_teams_ignores_workflow_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self._write_agent(root, "main")
            self._write_agent(root, "coding")
            team_dir = root / "default.ws" / "system" / "teams"
            team_dir.mkdir(parents=True)
            team_dir.joinpath("dev-team.json").write_text(
                json.dumps(
                    {
                        "name": "dev-team",
                        "leader": "main",
                        "mode": "leader_delegates",
                        "members": [{"agent": "coding", "role": "implementation", "autoDelegate": True}],
                    }
                ),
                encoding="utf-8",
            )
            team_dir.joinpath("dev-team.workflow.json").write_text(
                json.dumps(
                    {
                        "name": "dev-workflow",
                        "steps": [{"id": "code", "agent": "coding", "task": "Code"}],
                    }
                ),
                encoding="utf-8",
            )
            with patch("src.web_ui_new._WORKSPACE_ROOT", str(root)):
                listed = await list_teams(ws="default.ws")

        self.assertTrue(listed["success"])
        self.assertEqual([team["name"] for team in listed["data"]], ["dev-team"])

    async def test_delete_team_removes_workflow_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self._write_agent(root, "main")
            self._write_agent(root, "coding")
            team_dir = root / "default.ws" / "system" / "teams"
            team_dir.mkdir(parents=True)
            team_dir.joinpath("dev-team.json").write_text(
                json.dumps(
                    {
                        "name": "dev-team",
                        "leader": "main",
                        "mode": "leader_delegates",
                        "members": [{"agent": "coding", "role": "implementation", "autoDelegate": True}],
                    }
                ),
                encoding="utf-8",
            )
            workflow_path = team_dir / "dev-team.workflow.json"
            workflow_path.write_text(
                json.dumps(
                    {
                        "name": "dev-workflow",
                        "steps": [{"id": "code", "agent": "coding", "task": "Code"}],
                    }
                ),
                encoding="utf-8",
            )
            with patch("src.web_ui_new._WORKSPACE_ROOT", str(root)):
                deleted = await delete_team(ws="default.ws", team="dev-team")
                workflow_exists_after_delete = workflow_path.exists()

        self.assertTrue(deleted["success"])
        self.assertFalse(workflow_exists_after_delete)

    async def test_team_workflow_api_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self._write_agent(root, "main")
            self._write_agent(root, "coding")
            team_dir = root / "default.ws" / "system" / "teams"
            team_dir.mkdir(parents=True)
            team_dir.joinpath("dev-team.json").write_text(
                json.dumps(
                    {
                        "name": "dev-team",
                        "leader": "main",
                        "mode": "leader_delegates",
                        "members": [{"agent": "coding", "role": "implementation", "autoDelegate": True}],
                    }
                ),
                encoding="utf-8",
            )
            with patch("src.web_ui_new._WORKSPACE_ROOT", str(root)):
                written = await write_team_workflow_api(
                    TeamWorkflowWriteRequest(
                        ws="default.ws",
                        team="dev-team",
                        workflow={
                            "name": "dev-workflow",
                            "version": "invalid",
                            "steps": [
                                {"id": "plan", "agent": "main", "task": "Plan {{input}}"},
                                {"id": "code", "agent": "coding", "depends_on": ["plan"], "task": "Code"},
                            ],
                        },
                    )
                )
                read = await read_team_workflow_api(ws="default.ws", team="dev-team")

        self.assertTrue(written["success"])
        self.assertEqual(written["data"]["version"], 1)
        self.assertEqual(written["data"]["steps"][1]["depends_on"], ["plan"])
        self.assertTrue(read["success"])
        self.assertEqual(read["data"]["name"], "dev-workflow")

    async def test_team_workflow_validation_rejects_bad_steps(self) -> None:
        team = {
            "name": "dev-team",
            "leader": "main",
            "members": [{"agent": "coding", "autoDelegate": True}],
        }
        cases = [
            (
                {"steps": [{"id": "final", "agent": "main", "depends_on": ["missing"], "task": "finish"}]},
                "unknown or forward dependency",
            ),
            (
                {
                    "steps": [
                        {"id": "same", "agent": "main", "task": "one"},
                        {"id": "same", "agent": "coding", "task": "two"},
                    ]
                },
                "Duplicate workflow step id",
            ),
            (
                {"steps": [{"id": "hack", "agent": "outsider", "task": "run"}]},
                "not enabled in team",
            ),
        ]

        for workflow, expected_error in cases:
            with self.subTest(expected_error=expected_error):
                normalized, error = normalize_team_workflow(workflow, team_name="dev-team", team_config=team)

            self.assertIsNone(normalized)
            self.assertIn(expected_error, error)

    async def test_run_team_workflow_executes_steps_in_order(self) -> None:
        calls: list[dict] = []
        workflow, error = normalize_team_workflow(
            {
                "name": "dev-workflow",
                "steps": [
                    {"id": "first", "agent": "main", "task": "Find {{input}}"},
                    {
                        "id": "second",
                        "agent": "coding",
                        "depends_on": ["first"],
                        "task": "Write from {{steps.first.response}}",
                    },
                ],
            },
            team_name="dev-team",
        )
        self.assertIsNone(error)

        def _run_step(**kwargs):
            calls.append(kwargs)
            return {
                "status": "OK",
                "agent": kwargs["agent"],
                "response": f"{kwargs['step_id']} response",
                "exit_reason": "CURRENT_TASK_DONE",
                "turns": 1,
            }

        result = run_team_workflow(workflow, "semiconductors", _run_step)

        self.assertEqual([call["step_id"] for call in calls], ["first", "second"])
        self.assertIn("first response", calls[1]["task"])
        self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")
        self.assertEqual(result["response"], "second response")
        self.assertEqual(result["tool_results"][0]["tool_name"], "team_step")

    async def test_submit_task_with_team_uses_team_leader_and_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self._write_agent(root, "main")
            self._write_agent(root, "coding")
            team_dir = root / "default.ws" / "system" / "teams"
            team_dir.mkdir(parents=True)
            team_dir.joinpath("dev-team.json").write_text(
                json.dumps(
                    {
                        "name": "dev-team",
                        "description": "Dev team",
                        "leader": "main",
                        "mode": "leader_delegates",
                        "members": [{"agent": "coding", "role": "implementation", "autoDelegate": True}],
                    }
                ),
                encoding="utf-8",
            )
            team_dir.joinpath("dev-team.workflow.json").write_text(
                json.dumps(
                    {
                        "name": "dev-workflow",
                        "steps": [
                            {"id": "plan", "agent": "main", "task": "Plan {{input}}"},
                            {"id": "code", "agent": "coding", "depends_on": ["plan"], "task": "Code it"},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            fake_agent = _FakeAgent()
            fake_agent.handler = type("H", (), {"ctx": type("C", (), {"verbose": False, "sink": None})()})()
            captured: dict[str, object] = {}

            def _fake_build_agent(**kwargs):
                captured.update(kwargs)
                return fake_agent

            _sessions.clear()
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(root)),
                patch("src.web_ui_new.build_agent", side_effect=_fake_build_agent),
                patch("src.web_ui_new.threading.Thread", _NoopThread),
            ):
                result = await submit_task(
                    SubmitTaskRequest(task="hello", workspace_dir="default.ws", team="dev-team")
                )

        self.assertTrue(result["success"])
        self.assertEqual(captured["agent_name"], "main")
        self.assertEqual(captured["team_config"]["name"], "dev-team")
        session = _sessions[result["data"]["session_id"]]
        self.assertEqual(session.team_workflow["name"], "dev-workflow")
        self.assertTrue(callable(session.team_step_runner))

    async def test_submit_task_with_team_workflow_runs_workflow_instead_of_leader_loop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self._write_agent(root, "main")
            self._write_agent(root, "coding")
            team_dir = root / "default.ws" / "system" / "teams"
            team_dir.mkdir(parents=True)
            team_dir.joinpath("dev-team.json").write_text(
                json.dumps(
                    {
                        "name": "dev-team",
                        "leader": "main",
                        "mode": "leader_delegates",
                        "members": [{"agent": "coding", "role": "implementation", "autoDelegate": True}],
                    }
                ),
                encoding="utf-8",
            )
            team_dir.joinpath("dev-team.workflow.json").write_text(
                json.dumps({"name": "dev-workflow", "steps": [{"id": "code", "agent": "coding", "task": "Do {{input}}"}]}),
                encoding="utf-8",
            )
            fake_agent = _HistoryFakeAgent()
            calls: list[dict] = []

            def _fake_runner(**kwargs):
                calls.append(kwargs)
                kwargs["parent_ctx"].display_fn("[error] child internal error")
                kwargs["parent_ctx"].display_fn('  llm | <tool_use>{"name": "bad_json"')
                return {"status": "OK", "agent": kwargs["agent"], "response": "workflow response", "turns": 1}

            _sessions.clear()
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(root)),
                patch("src.web_ui_new.build_agent", return_value=fake_agent),
                patch("src.web_ui_new.build_team_step_runner", return_value=_fake_runner),
                patch("src.web_ui_new.threading.Thread", _ImmediateThread),
            ):
                result = await submit_task(
                    SubmitTaskRequest(task="hello", workspace_dir="default.ws", team="dev-team")
                )

        self.assertTrue(result["success"])
        self.assertEqual(fake_agent.ran_tasks, [])
        self.assertEqual(calls[0]["agent"], "coding")
        session = _sessions[result["data"]["session_id"]]
        self.assertEqual(session.messages[-1]["content"], "workflow response")
        self.assertIn("[Team Workflow] start dev-workflow", session.messages[-1].get("thinking", ""))
        self.assertNotIn("child internal error", json.dumps(session.messages, ensure_ascii=False))
        self.assertNotIn("bad_json", json.dumps(session.messages, ensure_ascii=False))
        self.assertEqual(session.messages[-1]["metadata"]["exitReason"], "CURRENT_TASK_DONE")
        self.assertEqual(session.messages[-1]["metadata"]["teamWorkflow"]["name"], "dev-workflow")

    async def test_team_step_runner_marks_child_exit_error_as_error(self) -> None:
        class _ChildAgent:
            def __init__(self) -> None:
                self.handler = type("H", (), {"ctx": type("C", (), {})()})()
                self.sink = NullSink()
                self.closed = False

            def run_task(self, _task: str) -> dict:
                return {
                    "response": "[error] child failed",
                    "exit_reason": "ERROR",
                    "tool_results": [{"tool_name": "run_task"}],
                    "turns": 2,
                }

            def close(self) -> None:
                self.closed = True

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self._write_agent(root, "coding")
            child = _ChildAgent()
            runner = build_team_step_runner(
                config_path=None,
                observability_config_path=None,
                skills_dir=None,
                workspace=str(root / "default.ws"),
                team_config={
                    "name": "dev-team",
                    "leader": "main",
                    "members": [{"agent": "coding", "autoDelegate": True}],
                },
            )
            with patch("src.main.build_agent", return_value=child):
                result = runner(agent="coding", task="do it", step_id="code")

        self.assertEqual(result["status"], "ERROR")
        self.assertEqual(result["exit_reason"], "ERROR")
        self.assertTrue(child.closed)

    async def test_submit_task_with_team_workflow_exception_finishes_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self._write_agent(root, "main")
            self._write_agent(root, "coding")
            team_dir = root / "default.ws" / "system" / "teams"
            team_dir.mkdir(parents=True)
            team_dir.joinpath("dev-team.json").write_text(
                json.dumps(
                    {
                        "name": "dev-team",
                        "leader": "main",
                        "mode": "leader_delegates",
                        "members": [{"agent": "coding", "role": "implementation", "autoDelegate": True}],
                    }
                ),
                encoding="utf-8",
            )
            team_dir.joinpath("dev-team.workflow.json").write_text(
                json.dumps({"name": "dev-workflow", "steps": [{"id": "code", "agent": "coding", "task": "Do {{input}}"}]}),
                encoding="utf-8",
            )
            fake_agent = _HistoryFakeAgent()

            _sessions.clear()
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(root)),
                patch("src.web_ui_new.build_agent", return_value=fake_agent),
                patch("src.web_ui_new.build_team_step_runner", return_value=lambda **_kwargs: {}),
                patch("src.web_ui_new.run_team_workflow", side_effect=RuntimeError("workflow exploded")),
                patch("src.web_ui_new.threading.Thread", _ImmediateThread),
            ):
                result = await submit_task(
                    SubmitTaskRequest(task="hello", workspace_dir="default.ws", team="dev-team")
                )

        self.assertTrue(result["success"])
        session = _sessions[result["data"]["session_id"]]
        self.assertFalse(session.running)
        self.assertEqual(session.messages[-1]["metadata"]["exitReason"], "ERROR")
        self.assertIn("workflow exploded", session.messages[-1]["content"])
        self.assertEqual(session.messages[-1]["metadata"]["teamWorkflow"]["name"], "dev-workflow")

    async def test_submit_task_attaches_default_jsonl_trace_sink(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            agent_dir = Path(tmp_dir) / "default.ws" / "system" / "agents" / "main"
            agent_dir.mkdir(parents=True)
            (agent_dir / "AGENT.md").write_text("# Main\n", encoding="utf-8")

            fake_agent = _FakeAgent()
            fake_agent.handler = type("H", (), {"ctx": type("C", (), {"verbose": False, "sink": None})()})()

            def _fake_build_agent(**_kwargs):
                return fake_agent

            _sessions.clear()
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(Path(tmp_dir))),
                patch("src.web_ui_new.build_agent", side_effect=_fake_build_agent),
                patch("src.web_ui_new.threading.Thread", _NoopThread),
                patch.dict("os.environ", {"XAGENT_LOG_DIR": ""}, clear=False),
            ):
                result = await submit_task(SubmitTaskRequest(task="hello", workspace_dir="default.ws", agent="main"))

            self.assertTrue(result["success"])
            self.assertIsInstance(fake_agent.sink, MultiSink)
            self.assertTrue(any(isinstance(sink, JsonlSink) for sink in fake_agent.sink.sinks))
            self.assertTrue((Path(tmp_dir) / "default.ws" / "runtime" / "traces").is_dir())

    def test_summary_falls_back_to_llm_end_tokens(self) -> None:
        summary = _usage_summary_from_events(
            [
                {
                    "session_id": "s1",
                    "kind": "llm_end",
                    "duration_ms": 10,
                    "ts": 1,
                    "data": {"input_tokens": 2, "output_tokens": 3, "total_tokens": 5},
                },
                {
                    "session_id": "s1",
                    "kind": "run_end",
                    "name": "CURRENT_TASK_DONE",
                    "duration_ms": 10,
                    "ts": 2,
                    "data": {"turns": 1},
                },
            ]
        )

        self.assertEqual(summary["totals"]["total_tokens"], 5)
        self.assertEqual(summary["sessions"][0]["usage"]["output_tokens"], 3)


class WebUINewPersistentChatTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self) -> None:
        _sessions.clear()

    async def test_create_list_restore_and_delete_agent_chat(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir()
            with patch("src.web_ui_new._WORKSPACE_ROOT", str(Path(tmp_dir))):
                created = await create_chat(ChatCreateRequest(ws="default.ws", agent="coding"))
                self.assertTrue(created["success"])
                chat_id = created["data"]["metadata"]["chat_id"]

                listed = await list_chats(ws="default.ws", agent="coding")
                restored = await read_chat(chat_id, ws="default.ws", agent="coding")
                deleted = await delete_chat(chat_id, ws="default.ws", agent="coding")

            self.assertEqual(listed["data"][0]["chat_id"], chat_id)
            self.assertEqual(restored["data"]["metadata"]["chat_id"], chat_id)
            self.assertTrue(deleted["success"])
            self.assertFalse((Path(tmp_dir) / "default.ws" / "runtime" / "chats" / "coding" / chat_id).exists())

    async def test_list_chats_is_scoped_by_agent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir()
            with patch("src.web_ui_new._WORKSPACE_ROOT", str(Path(tmp_dir))):
                coding = await create_chat(ChatCreateRequest(ws="default.ws", agent="coding"))
                main = await create_chat(ChatCreateRequest(ws="default.ws", agent="main"))
                coding_list = await list_chats(ws="default.ws", agent="coding")
                main_list = await list_chats(ws="default.ws", agent="main")

            self.assertEqual([item["chat_id"] for item in coding_list["data"]], [coding["data"]["metadata"]["chat_id"]])
            self.assertEqual([item["chat_id"] for item in main_list["data"]], [main["data"]["metadata"]["chat_id"]])

    async def test_chat_restore_rejects_unsafe_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir()
            with patch("src.web_ui_new._WORKSPACE_ROOT", str(Path(tmp_dir))):
                result = await read_chat("../bad", ws="default.ws", agent="coding")

        self.assertFalse(result["success"])

    async def test_submit_task_with_chat_id_persists_messages_and_llm_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            agent_dir = Path(tmp_dir) / "default.ws" / "system" / "agents" / "coding"
            agent_dir.mkdir(parents=True)
            (agent_dir / "AGENT.md").write_text(
                '---\nname: "coding"\ntools: []\nskills: []\nproject_agents: []\n---\n\n# Coding\n',
                encoding="utf-8",
            )
            fake_agent = _HistoryFakeAgent(history=[{"role": "user", "content": "before"}])

            def _fake_build_agent(**_kwargs):
                return fake_agent

            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(Path(tmp_dir))),
                patch("src.web_ui_new.build_agent", side_effect=_fake_build_agent),
                patch("src.web_ui_new.threading.Thread", _ImmediateThread),
                patch.dict("os.environ", {"XAGENT_LOG_DIR": ""}, clear=False),
            ):
                created = await create_chat(ChatCreateRequest(ws="default.ws", agent="coding"))
                chat_id = created["data"]["metadata"]["chat_id"]
                result = await submit_task(
                    SubmitTaskRequest(
                        task="continue this",
                        chat_id=chat_id,
                        workspace_dir="default.ws",
                        agent="coding",
                    )
                )
                restored = await read_chat(chat_id, ws="default.ws", agent="coding")

        self.assertTrue(result["success"])
        messages = restored["data"]["state"]["messages"]
        history = restored["data"]["state"]["llm_history"]
        self.assertEqual(messages[0]["content"], "continue this")
        self.assertTrue(any(item.get("content") == "continue this" for item in history))
        self.assertEqual(restored["data"]["metadata"]["title"], "continue this")

    async def test_restored_chat_history_is_applied_after_memory_session_clear(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            agent_dir = Path(tmp_dir) / "default.ws" / "system" / "agents" / "coding"
            agent_dir.mkdir(parents=True)
            (agent_dir / "AGENT.md").write_text(
                '---\nname: "coding"\ntools: []\nskills: []\nproject_agents: []\n---\n\n# Coding\n',
                encoding="utf-8",
            )
            applied_agents: list[_HistoryFakeAgent] = []

            def _fake_build_agent(**_kwargs):
                agent = _HistoryFakeAgent()
                applied_agents.append(agent)
                return agent

            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(Path(tmp_dir))),
                patch("src.web_ui_new.build_agent", side_effect=_fake_build_agent),
                patch("src.web_ui_new.threading.Thread", _NoopThread),
                patch.dict("os.environ", {"XAGENT_LOG_DIR": ""}, clear=False),
            ):
                created = await create_chat(ChatCreateRequest(ws="default.ws", agent="coding"))
                chat_id = created["data"]["metadata"]["chat_id"]
                state_path = Path(tmp_dir) / "default.ws" / "runtime" / "chats" / "coding" / chat_id / "state.json"
                state = json.loads(state_path.read_text(encoding="utf-8"))
                state["llm_history"] = [{"role": "user", "content": "saved context"}]
                state_path.write_text(json.dumps(state), encoding="utf-8")
                _sessions.clear()
                restored = await read_chat(chat_id, ws="default.ws", agent="coding")
                result = await submit_task(
                    SubmitTaskRequest(
                        task="next",
                        session_id=restored["data"]["state"]["backend_session_id"],
                        chat_id=chat_id,
                        workspace_dir="default.ws",
                        agent="coding",
                    )
                )

        self.assertTrue(result["success"])
        self.assertEqual(applied_agents[0].client.backend.history, [{"role": "user", "content": "saved context"}])

    async def test_global_chat_without_chat_id_does_not_create_chat_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            fake_agent = _HistoryFakeAgent()

            def _fake_build_agent(**_kwargs):
                return fake_agent

            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(Path(tmp_dir))),
                patch("src.web_ui_new.build_agent", side_effect=_fake_build_agent),
                patch("src.web_ui_new.threading.Thread", _NoopThread),
                patch.dict("os.environ", {"XAGENT_LOG_DIR": ""}, clear=False),
            ):
                result = await submit_task(SubmitTaskRequest(task="global", workspace_dir="default.ws"))

        self.assertTrue(result["success"])
        self.assertFalse((Path(tmp_dir) / "default.ws" / "runtime" / "chats").exists())


class WebUINewWorkspaceFileTests(unittest.IsolatedAsyncioTestCase):
    async def test_read_workspace_system_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            ws_root = Path(tmp_dir) / "default.ws"
            target = ws_root / "system" / "agents" / "main" / "AGENT.md"
            target.parent.mkdir(parents=True)
            target.write_text("agent config", encoding="utf-8")

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await read_workspace_file("default.ws", "system/agents/main/AGENT.md")

            self.assertTrue(result["success"])
            self.assertEqual(result["data"], {"path": "system/agents/main/AGENT.md", "content": "agent config"})

    async def test_write_workspace_system_file_creates_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir()

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await write_workspace_file(
                    WorkspaceFileWriteRequest(
                        ws="default.ws",
                        path="system/templates/foo.md",
                        content="hello",
                    )
                )

            target = Path(tmp_dir) / "default.ws" / "system" / "templates" / "foo.md"
            self.assertTrue(result["success"])
            self.assertTrue(result["data"]["created"])
            self.assertEqual(result["data"]["bytes"], 5)
            self.assertEqual(result["data"]["content"], "hello")
            self.assertEqual(target.read_text(encoding="utf-8"), "hello")

    async def test_write_workspace_system_file_overwrites_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "default.ws" / "system" / "templates" / "foo.md"
            target.parent.mkdir(parents=True)
            target.write_text("old", encoding="utf-8")

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await write_workspace_file(
                    WorkspaceFileWriteRequest(
                        ws="default.ws",
                        path="system/templates/foo.md",
                        content="new text",
                    )
                )

            self.assertTrue(result["success"])
            self.assertFalse(result["data"]["created"])
            self.assertEqual(result["data"]["bytes"], len("new text".encode("utf-8")))
            self.assertEqual(target.read_text(encoding="utf-8"), "new text")

    async def test_workspace_file_api_rejects_invalid_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir()
            cases = [
                ("default.ws", "business/foo.txt"),
                ("default.ws", "../escape.txt"),
                ("default.ws", "/tmp/escape.txt"),
                ("plain", "system/foo.txt"),
                ("../bad.ws", "system/foo.txt"),
                ("missing.ws", "system/foo.txt"),
            ]

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                for ws, path in cases:
                    result = await write_workspace_file(
                        WorkspaceFileWriteRequest(ws=ws, path=path, content="bad")
                    )
                    self.assertFalse(result["success"], (ws, path))

    async def test_frontend_write_api_does_not_weaken_agent_file_tools(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "default.ws" / "system" / "agents" / "main" / "AGENT.md"
            target.parent.mkdir(parents=True)

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await write_workspace_file(
                    WorkspaceFileWriteRequest(
                        ws="default.ws",
                        path="system/agents/main/AGENT.md",
                        content="managed by frontend",
                    )
                )

            self.assertTrue(result["success"])
            denied = write_file(
                path="system/agents/main/AGENT.md",
                content="agent write",
                cwd=str(Path(tmp_dir) / "default.ws"),
            )
            self.assertEqual(denied["status"], "ERROR")
            self.assertEqual(target.read_text(encoding="utf-8"), "managed by frontend")

    def test_workspace_name_resolves_to_project_workspace_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                resolved = _resolve_workspace_dir_input("default.ws")

            self.assertEqual(resolved, str(Path(tmp_dir) / "default.ws"))

    async def test_workspace_template_list_includes_requested_templates(self) -> None:
        result = await list_workspace_template_options()

        self.assertTrue(result["success"])
        template_ids = {item["id"] for item in result["data"]}
        self.assertTrue(
            {
                "blank",
                "code_project",
                "research_project",
                "operations",
                "data_analysis",
                "personal_assistant",
            }.issubset(template_ids)
        )

    async def test_create_workspace_from_code_project_template(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await create_workspace(
                    WorkspaceCreateRequest(name="demo", template_id="code_project")
                )

            ws_root = Path(tmp_dir) / "demo.ws"
            self.assertTrue(result["success"])
            self.assertEqual(result["data"]["name"], "demo.ws")
            self.assertTrue((ws_root / "business" / "src").is_dir())
            self.assertTrue((ws_root / "runtime" / "tasks" / "tasks.json").is_file())
            self.assertTrue((ws_root / "system" / "agents" / "review" / "AGENT.md").is_file())
            self.assertTrue((ws_root / "system" / "skills" / "code-review" / "SKILL.md").is_file())
            self.assertIn(
                "Project Memory",
                (ws_root / "system" / "memory" / "project.md").read_text(encoding="utf-8"),
            )
            tasks = json.loads((ws_root / "runtime" / "tasks" / "tasks.json").read_text(encoding="utf-8"))
            self.assertEqual(tasks["tasks"][0]["workspace"], "demo.ws")
            self.assertEqual(tasks["tasks"][0]["agent"], "review")

    async def test_create_workspace_rejects_invalid_duplicate_and_unknown_template(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                invalid = await create_workspace(WorkspaceCreateRequest(name="../bad", template_id="blank"))
                unknown = await create_workspace(WorkspaceCreateRequest(name="unknown", template_id="missing"))
                first = await create_workspace(WorkspaceCreateRequest(name="dupe.ws", template_id="blank"))
                duplicate = await create_workspace(WorkspaceCreateRequest(name="dupe", template_id="blank"))

        self.assertFalse(invalid["success"])
        self.assertIn("Invalid workspace name", invalid["error"])
        self.assertFalse(unknown["success"])
        self.assertIn("Unknown workspace template", unknown["error"])
        self.assertTrue(first["success"])
        self.assertFalse(duplicate["success"])
        self.assertIn("Workspace already exists", duplicate["error"])

    async def test_non_blank_workspace_templates_seed_expected_resources(self) -> None:
        expected = {
            "research_project": ("research", "research-notes", "research.md"),
            "operations": ("ops", "ops-runbook", "operations.md"),
            "data_analysis": ("data", "analysis-report", "data.md"),
            "personal_assistant": ("assistant", "personal-planning", "personal.md"),
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            for template_id, (agent, skill, memory_file) in expected.items():
                with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                    result = await create_workspace(
                        WorkspaceCreateRequest(name=template_id, template_id=template_id)
                    )
                ws_root = Path(tmp_dir) / f"{template_id}.ws"
                self.assertTrue(result["success"], template_id)
                self.assertTrue((ws_root / "system" / "agents" / agent / "AGENT.md").is_file())
                self.assertTrue((ws_root / "system" / "skills" / skill / "SKILL.md").is_file())
                self.assertTrue((ws_root / "system" / "memory" / memory_file).is_file())
                if template_id == "research_project":
                    team_path = ws_root / "system" / "teams" / "deepresearch.json"
                    workflow_path = ws_root / "system" / "teams" / "deepresearch.workflow.json"
                    self.assertTrue(team_path.is_file())
                    self.assertTrue(workflow_path.is_file())
                    team = json.loads(team_path.read_text(encoding="utf-8"))
                    workflow = json.loads(workflow_path.read_text(encoding="utf-8"))
                    self.assertEqual(team["leader"], "main")
                    self.assertEqual(team["mode"], "leader_delegates")
                    self.assertEqual(
                        [step["agent"] for step in workflow["steps"]],
                        ["source_scout", "evidence_analyst", "synthesis_writer", "research_critic", "main"],
                    )
                    self.assertEqual(
                        [member["agent"] for member in team["members"]],
                        ["source_scout", "evidence_analyst", "synthesis_writer", "research_critic"],
                    )
                    for research_agent in team["members"]:
                        self.assertTrue(
                            (ws_root / "system" / "agents" / research_agent["agent"] / "AGENT.md").is_file()
                        )
                tasks = json.loads((ws_root / "runtime" / "tasks" / "tasks.json").read_text(encoding="utf-8"))
                self.assertGreaterEqual(len(tasks["tasks"]), 1)

    def test_parse_agent_markdown_with_frontmatter(self) -> None:
        content = """---
name: "main"
description: "日常对话分析"
tools:
  - "Read"
  - "Write"
model: "minimax-m2.7" # comment
maxTurns: 300
memory: "project"
skills: []
project_agents:
  - "coding"
---

# Main Agent
"""

        result = parse_agent_markdown(content, "main")

        self.assertEqual(result["profile"]["name"], "main")
        self.assertEqual(result["profile"]["description"], "日常对话分析")
        self.assertEqual(result["profile"]["tools"], ["Read", "Write"])
        self.assertEqual(result["profile"]["model"], "minimax-m2.7")
        self.assertEqual(result["profile"]["maxTurns"], 300)
        self.assertEqual(result["profile"]["memory"], "project")
        self.assertEqual(result["profile"]["skills"], [])
        self.assertEqual(result["profile"]["project_agents"], ["coding"])
        self.assertEqual(result["body"], "# Main Agent\n")

    def test_parse_agent_markdown_without_frontmatter_uses_defaults(self) -> None:
        result = parse_agent_markdown("# Legacy Agent\n", "legacy")

        self.assertEqual(result["profile"]["name"], "legacy")
        self.assertEqual(result["profile"]["description"], "")
        self.assertEqual(result["profile"]["tools"], [])
        self.assertEqual(result["body"], "# Legacy Agent\n")

    def test_serialize_agent_markdown_preserves_body(self) -> None:
        profile = {
            "name": "main",
            "description": "日常对话分析",
            "tools": ["Read"],
            "model": "minimax-m2.7",
            "maxTurns": 300,
            "memory": "project",
            "skills": [],
            "project_agents": ["coding"],
        }

        content = serialize_agent_markdown(profile, "# Main Agent\n")

        self.assertIn('name: "main"', content)
        self.assertIn('  - "Read"', content)
        self.assertTrue(content.endswith("# Main Agent\n"))

    async def test_list_agents_returns_profile_description(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "default.ws" / "system" / "agents" / "main" / "AGENT.md"
            target.parent.mkdir(parents=True)
            target.write_text(
                '---\nname: "main"\ndescription: "日常对话分析"\ntools: []\nmodel: ""\nmaxTurns: 300\nmemory: ""\nskills: []\nproject_agents: []\n---\n\n# Main\n',
                encoding="utf-8",
            )

            from src.web_ui_new import list_agents

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await list_agents("default.ws")

            self.assertTrue(result["success"])
            self.assertEqual(result["data"][0]["description"], "日常对话分析")
            self.assertEqual(result["data"][0]["profile"]["name"], "main")

    async def test_agent_profile_endpoint_reads_and_writes_agent_md(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "default.ws" / "system" / "agents" / "main" / "AGENT.md"
            target.parent.mkdir(parents=True)
            target.write_text("# Main\n", encoding="utf-8")

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                read_result = await read_agent_profile("default.ws", "main")
                self.assertTrue(read_result["success"])
                self.assertEqual(read_result["data"]["profile"]["name"], "main")

                profile = dict(read_result["data"]["profile"])
                profile["description"] = "日常对话分析"
                profile["tools"] = ["Read"]
                write_result = await write_agent_profile(
                    AgentProfileWriteRequest(
                        ws="default.ws",
                        agent="main",
                        profile=profile,
                        body="# Updated\n",
                    )
                )

            self.assertTrue(write_result["success"])
            self.assertIn('description: "日常对话分析"', target.read_text(encoding="utf-8"))
            self.assertIn("# Updated\n", target.read_text(encoding="utf-8"))

    def test_agent_runtime_config_reads_prompt_soul_and_frontmatter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            agent_dir = Path(tmp_dir) / "default.ws" / "system" / "agents" / "coding"
            agent_dir.mkdir(parents=True)
            (agent_dir / "AGENT.md").write_text(
                '---\nname: "coding"\ndescription: ""\ntools:\n  - "file_read"\nmodel: "dev-model"\nmaxTurns: 12\nmemory: ""\nskills:\n  - "review"\nproject_agents: []\n---\n\n# Coding Agent\n',
                encoding="utf-8",
            )
            (agent_dir / "SOUL.md").write_text("# Soul\n", encoding="utf-8")

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                runtime, error = _agent_runtime_config("default.ws", "coding")

            self.assertIsNone(error)
            self.assertEqual(runtime["agent_prompt"], "# Coding Agent\n")
            self.assertEqual(runtime["agent_soul"], "# Soul\n")
            self.assertEqual(runtime["tools_allowlist"], ["file_read"])
            self.assertEqual(runtime["skill_allowlist"], ["review"])
            self.assertEqual(runtime["model_override"], "dev-model")
            self.assertEqual(runtime["max_turns"], 12)
            self.assertEqual(runtime["memory_mode"], "project")

    async def test_submit_task_passes_selected_agent_runtime_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            agent_dir = Path(tmp_dir) / "default.ws" / "system" / "agents" / "coding"
            agent_dir.mkdir(parents=True)
            skills_dir = Path(tmp_dir) / "default.ws" / "system" / "skills"
            skills_dir.mkdir(parents=True)
            (agent_dir / "AGENT.md").write_text(
                '---\nname: "coding"\ndescription: ""\ntools:\n  - "file_read"\nmodel: "dev-model"\nmaxTurns: 12\nmemory: ""\nskills: []\nproject_agents: []\n---\n\n# Coding Agent\n',
                encoding="utf-8",
            )
            (agent_dir / "SOUL.md").write_text("# Soul\n", encoding="utf-8")

            fake_agent = _FakeAgent()
            fake_agent.handler = type("H", (), {"ctx": type("C", (), {"verbose": False})()})()
            captured: dict[str, object] = {}

            def _fake_build_agent(**kwargs):
                captured.update(kwargs)
                return fake_agent

            _sessions.clear()
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)),
                patch("src.web_ui_new.build_agent", side_effect=_fake_build_agent),
                patch("src.web_ui_new.threading.Thread", _NoopThread),
            ):
                result = await submit_task(
                    SubmitTaskRequest(task="hello", workspace_dir="default.ws", agent="coding")
                )

            self.assertTrue(result["success"])
            self.assertEqual(captured["agent_name"], "coding")
            self.assertEqual(captured["agent_prompt"], "# Coding Agent\n")
            self.assertEqual(captured["agent_soul"], "# Soul\n")
            self.assertEqual(captured["tools_allowlist"], ["file_read"])
            self.assertEqual(captured["model_override"], "dev-model")
            self.assertEqual(captured["max_turns"], 12)
            self.assertEqual(captured["memory_mode"], "project")
            self.assertEqual(captured["skills_dir"], str(skills_dir))

    async def test_submit_task_rebuilds_agent_when_runtime_config_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            agent_dir = Path(tmp_dir) / "default.ws" / "system" / "agents" / "coding"
            agent_dir.mkdir(parents=True)
            agent_file = agent_dir / "AGENT.md"
            agent_file.write_text(
                '---\nname: "coding"\ndescription: ""\ntools:\n  - "file_read"\nmodel: "dev-model"\nmaxTurns: 12\nmemory: "project"\nskills: []\nproject_agents: []\n---\n\n# Coding Agent\n',
                encoding="utf-8",
            )
            (agent_dir / "SOUL.md").write_text("# Soul\n", encoding="utf-8")

            built_agents = []

            def _fake_build_agent(**_kwargs):
                fake_agent = _FakeAgent()
                fake_agent.handler = type("H", (), {"ctx": type("C", (), {"verbose": False})()})()
                built_agents.append(fake_agent)
                return fake_agent

            _sessions.clear()
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)),
                patch("src.web_ui_new.build_agent", side_effect=_fake_build_agent),
                patch("src.web_ui_new.threading.Thread", _NoopThread),
            ):
                first = await submit_task(
                    SubmitTaskRequest(task="hello", workspace_dir="default.ws", agent="coding")
                )
                agent_file.write_text(
                    '---\nname: "coding"\ndescription: ""\ntools:\n  - "file_read"\nmodel: "dev-model"\nmaxTurns: 12\nmemory: "private"\nskills: []\nproject_agents: []\n---\n\n# Coding Agent\n',
                    encoding="utf-8",
                )
                second = await submit_task(
                    SubmitTaskRequest(
                        task="again",
                        session_id=first["data"]["session_id"],
                        workspace_dir="default.ws",
                        agent="coding",
                    )
                )

            self.assertTrue(first["success"])
            self.assertTrue(second["success"])
            self.assertEqual(len(built_agents), 2)
            self.assertTrue(built_agents[0].stopped)

    async def test_submit_task_rejects_invalid_agent_without_starting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir()
            _sessions.clear()
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)),
                patch("src.web_ui_new.build_agent") as build_agent_mock,
                patch("src.web_ui_new.threading.Thread", _NoopThread),
            ):
                result = await submit_task(
                    SubmitTaskRequest(task="hello", workspace_dir="default.ws", agent="../bad")
                )

            self.assertFalse(result["success"])
            build_agent_mock.assert_not_called()


class WebUINewScheduledTaskTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self) -> None:
        _sessions.clear()

    async def test_scheduled_task_crud_persists_under_workspace_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            ws_root = Path(tmp_dir) / "default.ws"
            ws_root.mkdir(parents=True)

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                created = await create_scheduled_task(
                    ScheduledTaskWriteRequest(
                        ws="default.ws",
                        name="Daily Report",
                        prompt="write report",
                        agent="",
                        repeat="daily",
                        date="2099-01-01",
                        time="09:00",
                    )
                )
                self.assertTrue(created["success"])
                task_id = created["data"]["id"]

                listed = await list_scheduled_tasks("default.ws")
                self.assertTrue(listed["success"])
                self.assertEqual(len(listed["data"]), 1)
                self.assertEqual(listed["data"][0]["name"], "Daily Report")

                paused = await set_scheduled_task_status(
                    task_id,
                    ScheduledTaskStatusRequest(ws="default.ws", status="paused"),
                )
                self.assertTrue(paused["success"])
                self.assertEqual(paused["data"]["status"], "paused")

                updated = await update_scheduled_task(
                    task_id,
                    ScheduledTaskWriteRequest(
                        ws="default.ws",
                        name="Daily Report v2",
                        prompt="write updated report",
                        repeat="weekly",
                        date="2099-01-01",
                        time="10:30",
                        status="running",
                    ),
                )
                self.assertTrue(updated["success"])
                self.assertEqual(updated["data"]["repeat"], "weekly")

                deleted = await delete_scheduled_task(task_id, ws="default.ws")
                self.assertTrue(deleted["success"])
                listed_after = await list_scheduled_tasks("default.ws")
                self.assertEqual(listed_after["data"], [])

                store_path = ws_root / "runtime" / "tasks" / "tasks.json"
                self.assertTrue(store_path.is_file())

    async def test_due_scheduled_task_dispatches_existing_task_runner(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir(parents=True)
            built: list[dict[str, object]] = []

            def _fake_build_agent(**kwargs):
                built.append(kwargs)
                agent = _HistoryFakeAgent()
                agent.handler.ctx.verbose = False
                return agent

            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)),
                patch("src.web_ui_new.build_agent", side_effect=_fake_build_agent),
                patch("src.web_ui_new.threading.Thread", _NoopThread),
            ):
                created = await create_scheduled_task(
                    ScheduledTaskWriteRequest(
                        ws="default.ws",
                        name="Once",
                        prompt="run once",
                        repeat="none",
                        date="2000-01-01",
                        time="00:00",
                    )
                )
                self.assertTrue(created["success"])
                _run_due_scheduled_tasks(now_ts=946684801)
                listed = await list_scheduled_tasks("default.ws")

            self.assertEqual(len(built), 1)
            self.assertEqual(built[0]["workspace_dir"], str(Path(tmp_dir) / "default.ws"))
            task = listed["data"][0]
            self.assertEqual(task["status"], "paused")
            self.assertEqual(task["last_run"], time.strftime("%Y-%m-%d %H:%M", time.localtime(946684801)))
            self.assertTrue(task["last_session_id"])

    async def test_debug_run_scheduled_task_reports_dispatch_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir(parents=True)
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)),
                patch("src.web_ui_new._dispatch_scheduled_task", return_value=(None, "boom")),
            ):
                created = await create_scheduled_task(
                    ScheduledTaskWriteRequest(
                        ws="default.ws",
                        name="Manual",
                        prompt="run now",
                        repeat="none",
                        date="2099-01-01",
                        time="00:00",
                    )
                )
                result = await run_scheduled_task_now(
                    created["data"]["id"],
                    ScheduledTaskRunRequest(ws="default.ws"),
                )

            self.assertFalse(result["success"])
            self.assertEqual(result["error"], "boom")
            self.assertEqual(result["data"]["last_debug_error"], "boom")

    async def test_debug_run_does_not_advance_schedule_or_pause_one_off_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir(parents=True)
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)),
                patch("src.web_ui_new._dispatch_scheduled_task", return_value=("debug-session", None)),
            ):
                created = await create_scheduled_task(
                    ScheduledTaskWriteRequest(
                        ws="default.ws",
                        name="Manual",
                        prompt="debug now",
                        repeat="none",
                        date="2099-01-01",
                        time="00:00",
                        status="running",
                    )
                )
                before = created["data"]
                result = await run_scheduled_task_now(
                    before["id"],
                    ScheduledTaskRunRequest(ws="default.ws"),
                )

            self.assertTrue(result["success"])
            task = result["data"]
            self.assertEqual(task["next_run"], before["next_run"])
            self.assertEqual(task["status"], "running")
            self.assertIsNone(task["last_run"])
            self.assertEqual(task["last_session_id"], "")
            self.assertTrue(task["last_debug_run"])
            self.assertEqual(task["last_debug_session_id"], "debug-session")

    async def test_scheduled_task_rejects_missing_datetime_for_fixed_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir(parents=True)

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await create_scheduled_task(
                    ScheduledTaskWriteRequest(
                        ws="default.ws",
                        name="Bad schedule",
                        prompt="run later",
                        repeat="daily",
                    )
                )

            self.assertFalse(result["success"])
            self.assertEqual(result["error"], "Date and time are required")

    async def test_scheduled_task_rejects_keep_one_chat_without_agent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir(parents=True)

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await create_scheduled_task(
                    ScheduledTaskWriteRequest(
                        ws="default.ws",
                        name="Shared chat",
                        prompt="run together",
                        repeat="custom",
                        keep_one_chat=True,
                    )
                )

            self.assertFalse(result["success"])
            self.assertEqual(result["error"], "Agent is required when keeping task runs in one chat")

    async def test_default_workspace_seeds_preset_tasks_for_project_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            ws_root = Path(tmp_dir) / "default.ws"
            ws_root.mkdir(parents=True)

            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)),
                patch("src.web_ui_new._DEFAULT_WORKSPACE_ROOT", str(tmp_dir)),
            ):
                listed = await list_scheduled_tasks("default.ws")

            self.assertTrue(listed["success"])
            self.assertEqual(
                {task["id"] for task in listed["data"]},
                {
                    "preset-daily-check",
                    "preset-weekly-code-quality-audit",
                    "preset-runtime-observability-review",
                    "preset-docs-implementation-consistency",
                },
            )
            self.assertTrue((ws_root / "runtime" / "tasks" / "tasks.json").is_file())

    async def test_workspace_tree_returns_system_directory_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "default.ws"
            agent_dir = root / "system" / "agents" / "main"
            template_dir = root / "system" / "templates"
            agent_dir.mkdir(parents=True)
            template_dir.mkdir(parents=True)
            (agent_dir / "AGENT.md").write_text("# Agent", encoding="utf-8")
            (template_dir / "note.md").write_text("# Note", encoding="utf-8")
            (template_dir / ".DS_Store").write_text("finder metadata", encoding="utf-8")
            (root / "business").mkdir()
            (root / "business" / "secret.txt").write_text("hidden", encoding="utf-8")

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await read_workspace_tree("default.ws")

            self.assertTrue(result["success"])
            tree = result["data"]
            self.assertEqual(tree["name"], "default.ws")
            self.assertEqual(tree["path"], "default.ws")
            self.assertEqual(tree["type"], "dir")
            system = next(child for child in tree["children"] if child["name"] == "system")
            agents = next(child for child in system["children"] if child["name"] == "agents")
            main = next(child for child in agents["children"] if child["name"] == "main")
            self.assertIn(
                {"name": "AGENT.md", "path": "system/agents/main/AGENT.md", "type": "file"},
                main["children"],
            )
            self.assertNotIn(".DS_Store", str(tree))
            self.assertIn("business", str(tree))

    async def test_workspace_tree_rejects_invalid_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await read_workspace_tree("../bad.ws")

            self.assertFalse(result["success"])
            self.assertEqual(result["error"], "Invalid workspace name")

    async def test_workspace_index_stats_returns_missing_index_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "default.ws"
            root.mkdir()

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await read_workspace_index_stats("default.ws")

            self.assertTrue(result["success"])
            self.assertFalse(result["data"]["exists"])
            self.assertEqual(result["data"]["file_count"], 0)

    async def test_workspace_index_refresh_and_search(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "default.ws"
            root.mkdir()
            (root / "business").mkdir()
            (root / "business" / "note.txt").write_text("needle_token\n", encoding="utf-8")

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                refresh = await refresh_workspace_index(WorkspaceIndexRefreshRequest(ws="default.ws"))
                search = await search_workspace_index(ws="default.ws", q="needle_token")

            self.assertTrue(refresh["success"])
            self.assertGreaterEqual(refresh["data"]["indexed"], 1)
            self.assertTrue(search["success"])
            self.assertEqual(search["data"]["matches"][0]["path"], "business/note.txt")

    async def test_workspace_index_search_rejects_invalid_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await search_workspace_index(ws="../bad.ws", q="anything")

            self.assertFalse(result["success"])
            self.assertEqual(result["error"], "Invalid workspace name")

    async def test_workspace_index_search_passes_embedding_config_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "default.ws"
            root.mkdir()
            config_path = Path(tmp_dir) / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "openai_text": {
                            "apikey": "chat-key",
                            "apibase": "https://example.com/chat/completions",
                            "model": "chat-model",
                            "file_index_embedding": {
                                "enabled": True,
                                "apibase": "https://example.com/v1/embeddings",
                                "model": "embed-model",
                                "dimension": 3,
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)),
                patch("src.web_ui_new.search_file_index") as search_mock,
            ):
                search_mock.return_value = {"status": "OK", "matches": []}
                result = await search_workspace_index(
                    ws="default.ws",
                    q="needle",
                    mode="semantic",
                    config_path=str(config_path),
                )

            self.assertTrue(result["success"])
            embedding_config = search_mock.call_args.kwargs["embedding_config"]
            nested = embedding_config["file_index_embedding"]
            self.assertEqual(nested["apikey"], "chat-key")
            self.assertEqual(nested["model"], "embed-model")

    async def test_workspace_preview_reads_workspace_text_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "default.ws"
            root.mkdir()
            (root / "business").mkdir()
            (root / "business" / "note.txt").write_text("preview text", encoding="utf-8")

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await preview_workspace_file("default.ws", "business/note.txt")

            self.assertTrue(result["success"])
            self.assertEqual(result["data"]["path"], "business/note.txt")
            self.assertEqual(result["data"]["content"], "preview text")
            self.assertTrue(result["data"]["read_only"])

    async def test_workspace_preview_rejects_unsafe_or_unreadable_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "default.ws"
            root.mkdir()
            (root / "dir").mkdir()
            (root / "binary.bin").write_bytes(b"\xff\xfe\x00")

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                outside = await preview_workspace_file("default.ws", "../outside.txt")
                directory = await preview_workspace_file("default.ws", "dir")
                missing = await preview_workspace_file("default.ws", "missing.txt")
                binary = await preview_workspace_file("default.ws", "binary.bin")

            self.assertFalse(outside["success"])
            self.assertIn("traversal", outside["error"])
            self.assertFalse(directory["success"])
            self.assertIn("directory", directory["error"])
            self.assertFalse(missing["success"])
            self.assertEqual(missing["error"], "File not found")
            self.assertFalse(binary["success"])
            self.assertIn("UTF-8", binary["error"])

    async def test_eval_dataset_import_list_and_detail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir()

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                imported = await api_import_eval_dataset(
                    EvalDatasetImportRequest(
                        ws="default.ws",
                        name="basic.jsonl",
                        format="jsonl",
                        content='{"id":"case-1","task":"say ok","assertions":{"contains":["ok"]}}\n',
                    )
                )
                listed = await api_list_eval_datasets("default.ws")
                detail = await api_get_eval_dataset(imported["data"]["id"], ws="default.ws")

            self.assertTrue(imported["success"])
            self.assertTrue(listed["success"])
            self.assertEqual(listed["data"][0]["case_count"], 1)
            self.assertTrue(detail["success"])
            self.assertEqual(detail["data"]["cases"][0]["id"], "case-1")

    async def test_eval_dataset_list_includes_workspace_eval_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            eval_dir = Path(tmp_dir) / "default.ws" / "system" / "eval" / "cranfield-small"
            eval_dir.mkdir(parents=True)
            dataset_path = eval_dir / "xagent-eval-full.jsonl"
            dataset_path.write_text(
                '{"id":"case-1","task":"search","assertions":{"tool_called":["file_search"]}}\n',
                encoding="utf-8",
            )
            (eval_dir / "documents.jsonl").write_text('{"docno":"1","text":"not an eval case"}\n', encoding="utf-8")

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                listed = await api_list_eval_datasets("default.ws")

            self.assertTrue(listed["success"])
            self.assertEqual(len(listed["data"]), 1)
            dataset = listed["data"][0]
            self.assertEqual(dataset["name"], "xagent-eval-full")
            self.assertEqual(dataset["source"]["type"], "workspace_path")
            self.assertEqual(dataset["source"]["path"], "system/eval/cranfield-small/xagent-eval-full.jsonl")
            self.assertFalse(dataset["imported"])
            self.assertEqual(dataset["case_count"], 1)

    async def test_eval_dataset_list_dedupes_imported_workspace_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            ws_root = Path(tmp_dir) / "default.ws"
            eval_dir = ws_root / "system" / "eval" / "cranfield-small"
            eval_dir.mkdir(parents=True)
            dataset_path = eval_dir / "xagent-eval-full.jsonl"
            dataset_path.write_text(
                '{"id":"case-1","task":"search","assertions":{"tool_called":["file_search"]}}\n',
                encoding="utf-8",
            )
            import_dataset_content(
                ws_root,
                name="xagent-eval-full",
                fmt="jsonl",
                content=dataset_path.read_text(encoding="utf-8"),
                source={"type": "path", "path": "system/eval/cranfield-small/xagent-eval-full.jsonl"},
            )

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                listed = await api_list_eval_datasets("default.ws")

            self.assertTrue(listed["success"])
            self.assertEqual(len(listed["data"]), 1)
            self.assertTrue(listed["data"][0]["imported"])

    async def test_eval_dataset_list_prefers_workspace_when_source_newer_than_import(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            ws_root = Path(tmp_dir) / "default.ws"
            eval_dir = ws_root / "system" / "eval" / "cranfield-small"
            eval_dir.mkdir(parents=True)
            dataset_path = eval_dir / "xagent-eval-full.jsonl"
            old_content = '{"id":"case-1","task":"old","assertions":{"tool_called":["file_search"]}}\n'
            new_content = '{"id":"case-1","task":"new","assertions":{"tool_called":["file_search"]}}\n'
            dataset_path.write_text(old_content, encoding="utf-8")
            import_dataset_content(
                ws_root,
                name="xagent-eval-full",
                fmt="jsonl",
                content=old_content,
                source={"type": "path", "path": "system/eval/cranfield-small/xagent-eval-full.jsonl"},
            )
            dataset_path.write_text(new_content, encoding="utf-8")

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                listed = await api_list_eval_datasets("default.ws")

            self.assertTrue(listed["success"])
            self.assertEqual(len(listed["data"]), 1)
            self.assertFalse(listed["data"][0]["imported"])
            self.assertEqual(listed["data"][0]["source"]["type"], "workspace_path")

    async def test_eval_dataset_download_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir()
            payload = b'{"task":"downloaded","assertions":{"contains":["ok"]}}\n'

            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)),
                patch(
                    "src.core.eval.socket.getaddrinfo",
                    return_value=[(None, None, None, "", ("93.184.216.34", 443))],
                ),
                patch("src.core.eval._open_dataset_url", return_value=_FakeDownloadResponse(payload)),
            ):
                result = await api_download_eval_dataset(
                    EvalDatasetDownloadRequest(
                        ws="default.ws",
                        url="https://example.test/eval.jsonl",
                        format="jsonl",
                    )
                )

            self.assertTrue(result["success"])
            self.assertEqual(result["data"]["source"]["type"], "url")

    async def test_eval_run_create_list_get_and_cancel(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            ws_root = Path(tmp_dir) / "default.ws"
            ws_root.mkdir()
            metadata = import_dataset_content(
                ws_root,
                name="basic.jsonl",
                fmt="jsonl",
                content='{"id":"case-1","task":"say ok","assertions":{"contains":["ok"],"tool_called":["file_read"]}}\n',
            )
            built: list[_EvalFakeAgent] = []

            def fake_build_agent(**_kwargs):
                agent = _EvalFakeAgent()
                built.append(agent)
                return agent

            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)),
                patch("src.web_ui_new.build_agent", side_effect=fake_build_agent),
                patch("src.web_ui_new.threading.Thread", _ImmediateThread),
            ):
                created = await api_create_eval_run(
                    EvalRunCreateRequest(ws="default.ws", dataset_id=metadata["id"], agent="")
                )
                listed = await api_list_eval_runs("default.ws")
                detail = await api_get_eval_run(created["data"]["id"], ws="default.ws")

            self.assertTrue(created["success"])
            self.assertEqual(len(built), 1)
            self.assertTrue(built[0].closed)
            self.assertTrue(listed["success"])
            self.assertEqual(listed["data"][0]["id"], created["data"]["id"])
            self.assertTrue(detail["success"])
            self.assertEqual(detail["data"]["status"], "completed")
            self.assertEqual(detail["data"]["summary"]["passed"], 1)

    async def test_eval_cancel_marks_active_run_canceling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            ws_root = Path(tmp_dir) / "default.ws"
            ws_root.mkdir()
            metadata = import_dataset_content(
                ws_root,
                name="basic.jsonl",
                fmt="jsonl",
                content='{"task":"say ok"}\n',
            )
            run = create_eval_run(ws_root, workspace="default.ws", dataset_id=metadata["id"], agent="")
            event = threading.Event()
            _eval_cancel_events[run["id"]] = event

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await api_cancel_eval_run(run["id"], ws="default.ws")
                stored = await api_get_eval_run(run["id"], ws="default.ws")

            self.assertTrue(result["success"])
            self.assertTrue(event.is_set())
            self.assertEqual(stored["data"]["status"], "canceling")
            _eval_cancel_events.pop(run["id"], None)

    async def test_eval_api_rejects_invalid_workspace_and_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir()

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                bad_ws = await api_list_eval_datasets("../bad.ws")
                bad_dataset = await api_get_eval_dataset("../bad", ws="default.ws")
                bad_run = await api_get_eval_run("../bad", ws="default.ws")

            self.assertFalse(bad_ws["success"])
            self.assertFalse(bad_dataset["success"])
            self.assertFalse(bad_run["success"])

    def test_filter_tools_schema_keeps_only_agent_allowed_tools(self) -> None:
        schema = [
            {"type": "function", "function": {"name": "file_read"}},
            {"type": "function", "function": {"name": "code_run"}},
        ]

        filtered = filter_tools_schema(schema, ["file_read"])

        self.assertEqual([item["function"]["name"] for item in filtered], ["file_read"])

    def test_agent_prompt_replaces_global_base_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            assets = root / "assets"
            memory = root / "memory"
            assets.mkdir()
            memory.mkdir()
            (assets / "sys_prompt.txt").write_text("global base", encoding="utf-8")
            (memory / "global_mem_insight.txt").write_text("insight", encoding="utf-8")
            (memory / "insight_fixed_structure.txt").write_text("fixed", encoding="utf-8")

            prompt = build_system_prompt(
                assets,
                root,
                "/workspace",
                agent_name="coding",
                agent_prompt="# Coding Agent",
                agent_soul="# Soul",
            )

            self.assertIn("# Coding Agent\n\n# Soul", prompt)
            self.assertNotIn("global base", prompt)
            self.assertIn("[动态注入]", prompt)
            self.assertIn("[Memory]\ninsight\n\nfixed", prompt)

    def test_config_model_is_preserved_without_agent_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "config.json"
            config_path.write_text(
                '{"openai_text": {"apikey": "", "apibase": "https://example.com/chat/completions", "model": "config-model"}}',
                encoding="utf-8",
            )
            agent = XAgent(
                system_prompt="",
                tools_schema=[],
                config_path=str(config_path),
                model="",
                workspace_dir=str(Path(tmp_dir) / "default.ws"),
            )
            try:
                self.assertEqual(agent.client.backend.model, "config-model")
            finally:
                agent.close()

    def test_agent_model_overrides_config_model_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "config.json"
            config_path.write_text(
                '{"openai_text": {"apikey": "", "apibase": "https://example.com/chat/completions", "model": "config-model"}}',
                encoding="utf-8",
            )
            agent = XAgent(
                system_prompt="",
                tools_schema=[],
                config_path=str(config_path),
                model="agent-model",
                workspace_dir=str(Path(tmp_dir) / "default.ws"),
            )
            try:
                self.assertEqual(agent.client.backend.model, "agent-model")
            finally:
                agent.close()

    def test_file_index_embedding_config_reaches_handler_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "openai_text": {
                            "apikey": "chat-key",
                            "apibase": "https://example.com/chat/completions",
                            "model": "config-model",
                            "file_index_embedding": {
                                "enabled": True,
                                "apibase": "https://example.com/v1/embeddings",
                                "model": "embed-model",
                                "dimension": 3,
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            agent = XAgent(
                system_prompt="",
                tools_schema=[],
                config_path=str(config_path),
                model="",
                workspace_dir=str(Path(tmp_dir) / "default.ws"),
            )
            try:
                cfg = agent.handler.ctx.file_index_embedding or {}
                nested = cfg.get("file_index_embedding", {})
                self.assertEqual(nested["apikey"], "chat-key")
                self.assertEqual(nested["model"], "embed-model")
                self.assertEqual(nested["dimension"], 3)
            finally:
                agent.close()

    def test_handler_rejects_tools_outside_agent_allowlist(self) -> None:
        handler = XAgentHandler(ctx=AgentContext(allowed_tools={"file_read"}))

        result = exhaust(handler.dispatch("code_run", {"script": "print(1)"}))

        self.assertEqual(result.data["status"], "ERROR")
        self.assertIn("not allowed", result.data["error"])

    def test_skill_activate_respects_agent_skill_allowlist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            for name in ("allowed", "blocked"):
                skill_dir = root / name
                skill_dir.mkdir()
                (skill_dir / "SKILL.md").write_text(f"# {name}\nUse for {name}.", encoding="utf-8")
            registry = SkillRegistry.load([root])
            handler = XAgentHandler(ctx=AgentContext(skills=registry, skill_allowlist={"allowed"}))

            result = handler.exec_skill_activate({"names": ["allowed", "blocked"]})

            self.assertEqual(result.data["activated"], ["allowed"])
            self.assertEqual(result.data["disallowed"], ["blocked"])
            self.assertEqual(handler.ctx.active_skills, ["allowed"])


if __name__ == "__main__":
    unittest.main()
