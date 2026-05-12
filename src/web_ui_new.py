"""
XAgent Modern Web UI - FastAPI backend with SSE streaming
"""
from __future__ import annotations

import argparse
import asyncio
import json
import queue
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src.main import build_agent

app = FastAPI(title="XAgent Web UI")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class SubmitTaskRequest(BaseModel):
    task: str
    session_id: str = ""
    config_path: str = ""
    observability_config_path: str = ""
    workspace_dir: str = ""


class ReplyRequest(BaseModel):
    reply: str
    session_id: str = ""


@dataclass
class UISession:
    agent: object | None = None
    events: list[dict[str, Any]] = field(default_factory=list)
    waiting_for_user: bool = False
    ask_prompt: str = ""
    running: bool = False
    config_path: str = ""
    observability_config_path: str = ""
    workspace_dir: str = ""
    session_id: str = ""
    event_queue: queue.Queue[dict[str, Any]] = field(default_factory=lambda: queue.Queue(maxsize=1))
    llm_stream_buffer: str = ""
    assistant_stream_emitted_len: int = 0
    thinking_stream_emitted_len: int = 0
    last_assistant_delta: str = ""
    last_thinking_delta: str = ""
    parsed_tool_use_count: int = 0
    pending_tool_names: list[str] = field(default_factory=list)
    emitted_tool_keys: set[str] = field(default_factory=set)


# Global session storage
_sessions: dict[str, UISession] = {}
_session_lock = threading.Lock()

_TURN_RE = re.compile(r"^\[Turn (\d+)\]$")
_TOOL_RE = re.compile(r"^\s*tool:\s*(.+)$")
_DONE_RE = re.compile(r"^\[Done\]\s+exit_reason=([^,]+),\s*turns=(\d+)")

_THINKING_BLOCK_RE = re.compile(r"<thinking>\s*([\s\S]*?)(?:</thinking>|$)", re.IGNORECASE)
_TOOL_USE_BLOCK_RE = re.compile(r"<tool_use>\s*([\s\S]*?)\s*</tool_use>", re.IGNORECASE)
_HIDDEN_BLOCK_RE = re.compile(
    r"<(?:summary|thinking|tool_use|tool_result|history|key_info|earlier_context)[^>]*>[\s\S]*?(?:</(?:summary|thinking|tool_use|tool_result|history|key_info|earlier_context)>|$)",
    re.IGNORECASE,
)
_TRAILING_PARTIAL_TAG_RE = re.compile(r"<[^>]*$")


def _visible_llm_output(text: str) -> str:
    visible = _HIDDEN_BLOCK_RE.sub("", text)
    visible = _TRAILING_PARTIAL_TAG_RE.sub("", visible)
    return visible


def _tool_key(name: str, arguments: dict[str, Any]) -> str:
    return f"{name}:{json.dumps(arguments, ensure_ascii=False, sort_keys=True)}"


def _emit_tool_call(session: UISession, name: str, arguments: dict[str, Any], tool_id: str | None = None) -> None:
    key = _tool_key(name, arguments)
    if key in session.emitted_tool_keys:
        return
    session.emitted_tool_keys.add(key)
    _emit(
        session,
        "tool_call",
        {
            "id": tool_id or uuid.uuid4().hex[:8],
            "name": name,
            "arguments": arguments,
            "status": "success",
        },
    )


def _emit_tool_uses(session: UISession) -> None:
    matches = list(_TOOL_USE_BLOCK_RE.finditer(session.llm_stream_buffer))
    for match in matches[session.parsed_tool_use_count:]:
        raw_payload = match.group(1).strip()
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            payload = {"name": "bad_json", "arguments": {"raw": raw_payload}}

        name = str(payload.get("name", "")).strip()
        if not name:
            continue
        arguments = payload.get("arguments")
        if not isinstance(arguments, dict):
            arguments = {}
        tool_id = str(payload.get("id") or uuid.uuid4().hex[:8])
        session.pending_tool_names.append(name)
        _emit_tool_call(session, name, arguments, tool_id)
    session.parsed_tool_use_count = len(matches)


def _append_llm_delta(session: UISession, delta: str) -> None:
    session.llm_stream_buffer += delta
    _emit_tool_uses(session)

    thinking = "".join(match.group(1) for match in _THINKING_BLOCK_RE.finditer(session.llm_stream_buffer))
    if len(thinking) > session.thinking_stream_emitted_len:
        _emit(session, "thinking_delta", thinking[session.thinking_stream_emitted_len:])
        session.thinking_stream_emitted_len = len(thinking)

    visible = _visible_llm_output(session.llm_stream_buffer)
    if len(visible) > session.assistant_stream_emitted_len:
        _emit(session, "assistant_delta", visible[session.assistant_stream_emitted_len:])
        session.assistant_stream_emitted_len = len(visible)


def _emit(session: UISession, event_type: str, data: Any = None) -> None:
    if event_type == "assistant_delta" and data == session.last_assistant_delta:
        return
    if event_type == "thinking_delta" and data == session.last_thinking_delta:
        return
    if event_type == "assistant_delta":
        session.last_assistant_delta = str(data)
    elif event_type == "thinking_delta":
        session.last_thinking_delta = str(data)
    session.events.append({"type": event_type, "data": data})


def _append_progress(session: UISession, message: str) -> None:
    if message.startswith("  llm | "):
        _append_llm_delta(session, message[len("  llm | "):])
        return
    if message.startswith("  thinking | "):
        delta = message[len("  thinking | "):]
        _emit(session, "thinking_delta", delta)
        return
    if message.startswith("  thinking: "):
        delta = message[len("  thinking: "):]
        _emit(session, "thinking_delta", delta)
        return
    if message.startswith("  llm tool_call | "):
        name = message[len("  llm tool_call | "):].strip()
        if name:
            _emit_tool_call(session, name, {})
        return

    turn_match = _TURN_RE.match(message)
    if turn_match:
        _emit(session, "turn_start", {"turn": int(turn_match.group(1))})
        return

    tool_match = _TOOL_RE.match(message)
    if tool_match:
        name = tool_match.group(1).strip()
        if name in session.pending_tool_names:
            session.pending_tool_names.remove(name)
            return
        _emit_tool_call(session, name, {})
        return

    done_match = _DONE_RE.match(message)
    if done_match:
        _emit(
            session,
            "run_done",
            {
                "exit_reason": done_match.group(1),
                "turns": int(done_match.group(2)),
            },
        )
        return

    if message.startswith("[error] "):
        _emit(session, "error", message)
        return

    _emit(session, "log", message)


def _close_agent(session: UISession) -> None:
    agent = session.agent
    if agent is not None:
        agent.close()
    session.agent = None


def _normalize_path_input(value: str | None) -> str:
    return value.strip() if isinstance(value, str) else ""


def _ensure_agent(session: UISession, config_path: str, observability_config_path: str, workspace_dir: str):
    if (
        session.agent is None
        or session.config_path != config_path
        or session.observability_config_path != observability_config_path
        or session.workspace_dir != workspace_dir
    ):
        _close_agent(session)
        session.agent = build_agent(
            config_path=config_path or None,
            observability_config_path=observability_config_path or None,
            workspace_dir=workspace_dir or None,
        )
        session.agent.handler.ctx.verbose = True
        session.config_path = config_path
        session.observability_config_path = observability_config_path
        session.workspace_dir = workspace_dir
    return session.agent


def _drain_sync(session: UISession, wait: bool) -> bool:
    """Synchronous drain for background thread"""
    agent = session.agent
    if agent is None:
        return True

    finished = False
    while True:
        timeout = 0.1 if wait and session.running else 0.0
        try:
            msg = agent.display_queue.get(timeout=timeout)
        except queue.Empty:
            break

        if "progress" in msg:
            _append_progress(session, msg["progress"])
        elif "ask_user" in msg:
            session.waiting_for_user = True
            session.running = False
            session.ask_prompt = msg["ask_user"]
            _emit(session, "ask_user", msg["ask_user"])
            break
        elif "done" in msg:
            result = msg["done"]
            _emit(
                session,
                "done",
                {
                    "response": result.get("response", ""),
                    "exit_reason": result.get("exit_reason", ""),
                    "tool_results": result.get("tool_results", []),
                    "turns": result.get("turns", 0),
                },
            )
            session.running = False
            session.waiting_for_user = False
            session.ask_prompt = ""
            finished = True
            break

        if not wait:
            continue
        if not agent.is_running():
            break
    return finished


def _queue_state(session: UISession, finished: bool) -> None:
    payload = {
        "type": "state_update",
        "finished": finished,
    }
    if finished:
        while True:
            try:
                session.event_queue.get_nowait()
            except queue.Empty:
                break
    try:
        session.event_queue.put_nowait(payload)
    except queue.Full:
        pass


def _drain_background(session: UISession) -> None:
    while True:
        with _session_lock:
            finished = _drain_sync(session, wait=True)
            _queue_state(session, finished)

            if finished or session.waiting_for_user or not session.running:
                break
        time.sleep(0.05)


def _run_task_background(session: UISession, task: str) -> None:
    """Run task in background and put events into async queue"""
    with _session_lock:
        if session.running:
            return
        session.events = []
        session.llm_stream_buffer = ""
        session.assistant_stream_emitted_len = 0
        session.thinking_stream_emitted_len = 0
        session.last_assistant_delta = ""
        session.last_thinking_delta = ""
        session.parsed_tool_use_count = 0
        session.pending_tool_names = []
        session.emitted_tool_keys = set()
        session.waiting_for_user = False
        session.ask_prompt = ""
        session.running = True
        if task.strip():
            _emit(session, "user_task", task.strip())
        session.agent.run_task_async(task.strip())

    _drain_background(session)


@app.post("/api/chat")
async def submit_task(request: SubmitTaskRequest):
    """Submit a new task"""
    requested_session_id = _normalize_path_input(request.session_id)
    session = _sessions.get(requested_session_id) if requested_session_id else None
    if session is None:
        session_id = uuid.uuid4().hex[:16]
        session = UISession(session_id=session_id)
        _sessions[session_id] = session
    else:
        session_id = session.session_id

    config_path = _normalize_path_input(request.config_path)
    observability_config_path = _normalize_path_input(request.observability_config_path)
    workspace_dir = _normalize_path_input(request.workspace_dir)

    # Build agent
    _ensure_agent(
        session,
        config_path,
        observability_config_path,
        workspace_dir,
    )

    # Start background task
    thread = threading.Thread(
        target=_run_task_background,
        args=(session, request.task),
        daemon=True,
    )
    thread.start()

    return {"success": True, "data": {"session_id": session_id}}


@app.post("/api/chat/reply")
async def send_reply(request: ReplyRequest):
    """Send reply to agent's ask_user"""
    requested_session_id = _normalize_path_input(request.session_id)
    with _session_lock:
        if requested_session_id:
            active_session = _sessions.get(requested_session_id)
            if active_session is None or not active_session.waiting_for_user:
                return {"success": False, "error": "Session is not waiting for reply"}
        else:
            active_session = next((s for s in _sessions.values() if s.waiting_for_user), None)

        if active_session is None or active_session.agent is None:
            return {"success": False, "error": "No active session waiting for reply"}

        active_session.waiting_for_user = False
        active_session.running = True
        _emit(active_session, "user_reply", request.reply.strip())
        active_session.agent.reply_queue.put(request.reply.strip())

    # Restart drain in background
    thread = threading.Thread(
        target=_drain_background,
        args=(active_session,),
        daemon=True,
    )
    thread.start()

    return {"success": True}


@app.post("/api/chat/stop")
async def stop_task(request: Request):
    """Stop current task"""
    try:
        payload = await request.json()
    except (json.JSONDecodeError, RuntimeError):
        payload = {}
    requested_session_id = _normalize_path_input(
        payload.get("session_id", "") if isinstance(payload, dict) else ""
    )

    with _session_lock:
        if requested_session_id:
            session = _sessions.get(requested_session_id)
            if session is None:
                return {"success": False, "error": "Session not found"}
            sessions = [session]
        else:
            sessions = _sessions.values()
        for session in sessions:
            if session.agent is not None and session.agent.is_running():
                session.agent.stop()
                _emit(session, "stop", "interrupt signal sent")
                _queue_state(session, finished=False)
                return {"success": True}

    return {"success": False, "error": "No running task"}


@app.get("/api/chat/stream")
async def stream_chat(request: Request):
    """SSE stream for chat events"""
    session_id = request.query_params.get("session_id", "")
    session = _sessions.get(session_id)

    if session is None:
        # Return empty stream
        async def empty_stream():
            yield f"data: {json.dumps({'type': 'error', 'data': 'Session not found'})}\n\n"

        return StreamingResponse(
            empty_stream(),
            media_type="text/event-stream",
        )

    async def event_generator():
        last_events_len = 0

        while True:
            state: dict[str, Any] = {}
            finished = False
            timed_out = False
            try:
                state = await asyncio.to_thread(session.event_queue.get, True, 0.5)
                finished = bool(state.get("finished"))
            except queue.Empty:
                timed_out = True

            with _session_lock:
                current_events = session.events
                new_events = current_events[last_events_len:]
                last_events_len = len(current_events)
                session_done = not session.running and not session.waiting_for_user

            for event in new_events:
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

            if finished:
                break

            if timed_out:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

                if await request.is_disconnected():
                    break

                if session_done:
                    break

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


# Serve static files (frontend build)
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

# Check if build directory exists
build_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../frontends/web/dist"))
print(f"Build dir: {build_dir}, exists: {os.path.exists(build_dir)}")
if os.path.exists(build_dir):
    @app.get("/")
    async def serve_index():
        return FileResponse(os.path.join(build_dir, "index.html"))

    @app.get("/{path:path}")
    async def serve_spa(path: str):
        # API routes should not be handled here
        if path.startswith("api/"):
            return {"error": "Not found"}
        file_path = os.path.join(build_dir, path)
        if os.path.exists(file_path) and os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(build_dir, "index.html"))

    app.mount("/assets", StaticFiles(directory=os.path.join(build_dir, "assets")), name="assets")


def main() -> None:
    parser = argparse.ArgumentParser(description="XAgent Modern Web UI")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7860)
    args = parser.parse_args()

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
