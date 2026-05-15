"""
XAgent Modern Web UI - FastAPI backend with SSE streaming
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
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
    agent: str = ""


class ReplyRequest(BaseModel):
    reply: str
    session_id: str = ""


class WorkspaceFileWriteRequest(BaseModel):
    ws: str = "default.ws"
    path: str = ""
    content: str = ""


class AgentProfileWriteRequest(BaseModel):
    ws: str = "default.ws"
    agent: str = ""
    profile: dict[str, Any] = {}
    body: str = ""


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
    agent_name: str = ""
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


def _resolve_workspace_dir_input(value: str) -> str:
    normalized = _normalize_path_input(value)
    if not normalized:
        return ""
    if _is_workspace_name(normalized):
        return os.path.join(_WORKSPACE_ROOT, normalized)
    return normalized


def _ensure_agent(
    session: UISession,
    config_path: str,
    observability_config_path: str,
    workspace_dir: str,
    agent_name: str,
    runtime_config: dict[str, Any] | None = None,
):
    if (
        session.agent is None
        or session.config_path != config_path
        or session.observability_config_path != observability_config_path
        or session.workspace_dir != workspace_dir
        or session.agent_name != agent_name
    ):
        _close_agent(session)
        runtime_config = runtime_config or {}
        session.agent = build_agent(
            config_path=config_path or None,
            observability_config_path=observability_config_path or None,
            workspace_dir=_resolve_workspace_dir_input(workspace_dir) or None,
            agent_name=agent_name,
            agent_prompt=str(runtime_config.get("agent_prompt", "")),
            agent_soul=str(runtime_config.get("agent_soul", "")),
            tools_allowlist=runtime_config.get("tools_allowlist"),
            skill_allowlist=runtime_config.get("skill_allowlist"),
            model_override=str(runtime_config.get("model_override", "")),
            max_turns=runtime_config.get("max_turns"),
        )
        session.agent.handler.ctx.verbose = True
        session.config_path = config_path
        session.observability_config_path = observability_config_path
        session.workspace_dir = workspace_dir
        session.agent_name = agent_name
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
    agent_name = _normalize_path_input(request.agent)
    runtime_config, runtime_error = _agent_runtime_config(workspace_dir, agent_name)
    if runtime_error:
        return {"success": False, "error": runtime_error}

    # Build agent
    _ensure_agent(
        session,
        config_path,
        observability_config_path,
        workspace_dir,
        agent_name,
        runtime_config,
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


# === Workspace Management API ===

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_WORKSPACE_ROOT = os.path.join(_PROJECT_ROOT, "workspace")
_AGENT_PROFILE_FIELDS = (
    "name",
    "description",
    "tools",
    "model",
    "maxTurns",
    "memory",
    "skills",
    "project_agents",
)
_AGENT_LIST_FIELDS = {"tools", "skills", "project_agents"}


def _is_workspace_name(ws: str) -> bool:
    return (
        bool(ws)
        and ws.endswith(".ws")
        and not os.path.isabs(ws)
        and os.path.basename(ws) == ws
        and ws not in {".ws", "..ws"}
        and ".." not in ws.split(".")
    )


def _workspace_root(ws: str) -> tuple[str | None, str | None]:
    if not _is_workspace_name(ws):
        return None, "Invalid workspace name"
    root = os.path.realpath(os.path.join(_WORKSPACE_ROOT, ws))
    workspace_parent = os.path.realpath(_WORKSPACE_ROOT)
    if os.path.commonpath([workspace_parent, root]) != workspace_parent:
        return None, "Path traversal not allowed"
    if not os.path.isdir(root):
        return None, "Workspace not found"
    return root, None


def _resolve_system_file_path(ws: str, path: str) -> tuple[str | None, str | None, str | None]:
    if not path:
        return None, None, "Path is required"
    if os.path.isabs(path):
        return None, None, "Absolute paths are not allowed"
    normalized_path = path.replace("\\", "/")
    if not normalized_path.startswith("system/"):
        return None, None, "Only system/ files can be managed via this endpoint"

    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return None, None, error

    real_path = os.path.realpath(os.path.join(ws_root, normalized_path))
    if os.path.commonpath([ws_root, real_path]) != ws_root:
        return None, None, "Path traversal not allowed"
    system_root = os.path.realpath(os.path.join(ws_root, "system"))
    if os.path.commonpath([system_root, real_path]) != system_root:
        return None, None, "Only system/ files can be managed via this endpoint"
    return real_path, normalized_path, None


def _is_child_name(name: str) -> bool:
    return bool(name) and not os.path.isabs(name) and os.path.basename(name) == name and name not in {".", ".."}


def _agent_dir(ws: str, agent: str) -> tuple[str | None, str | None]:
    if not _is_child_name(agent):
        return None, "Invalid agent name"
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return None, error
    agent_dir = os.path.realpath(os.path.join(ws_root, "system", "agents", agent))
    agents_root = os.path.realpath(os.path.join(ws_root, "system", "agents"))
    if os.path.commonpath([agents_root, agent_dir]) != agents_root:
        return None, "Path traversal not allowed"
    if not os.path.isdir(agent_dir):
        return None, "Agent not found"
    return agent_dir, None


def _default_agent_profile(agent_name: str) -> dict[str, Any]:
    return {
        "name": agent_name,
        "description": "",
        "tools": [],
        "model": "",
        "maxTurns": 300,
        "memory": "",
        "skills": [],
        "project_agents": [],
    }


def _split_agent_frontmatter(content: str) -> tuple[str, str] | None:
    lines = content.splitlines()
    if not lines or lines[0].strip() != "---":
        return None
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            frontmatter = "\n".join(lines[1:index])
            body = "\n".join(lines[index + 1 :]).lstrip("\n")
            if content.endswith("\n"):
                body += "\n"
            return frontmatter, body
    return None


def _strip_yaml_comment(value: str) -> str:
    in_single = False
    in_double = False
    escaped = False
    for index, char in enumerate(value):
        if escaped:
            escaped = False
            continue
        if char == "\\" and in_double:
            escaped = True
            continue
        if char == "'" and not in_double:
            in_single = not in_single
            continue
        if char == '"' and not in_single:
            in_double = not in_double
            continue
        if char == "#" and not in_single and not in_double:
            return value[:index].rstrip()
    return value.strip()


def _parse_scalar(value: str) -> Any:
    value = _strip_yaml_comment(value)
    if value == "":
        return ""
    if value == "[]":
        return []
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        return value


def _parse_inline_list(value: str) -> list[str]:
    value = _strip_yaml_comment(value)
    if value == "[]":
        return []
    if not (value.startswith("[") and value.endswith("]")):
        parsed = _parse_scalar(value)
        return parsed if isinstance(parsed, list) else []
    inner = value[1:-1].strip()
    if not inner:
        return []
    return [str(_parse_scalar(item.strip())) for item in inner.split(",") if item.strip()]


def parse_agent_markdown(content: str, agent_name: str) -> dict[str, Any]:
    profile = _default_agent_profile(agent_name)
    split = _split_agent_frontmatter(content)
    if split is None:
        return {"profile": profile, "body": content}

    frontmatter, body = split
    current_list_key: str | None = None
    for raw_line in frontmatter.splitlines():
        if not raw_line.strip():
            continue
        stripped = raw_line.strip()
        if stripped.startswith("- ") and current_list_key:
            profile[current_list_key].append(str(_parse_scalar(stripped[2:].strip())))
            continue
        if ":" not in raw_line:
            current_list_key = None
            continue
        key, raw_value = raw_line.split(":", 1)
        key = key.strip()
        if key not in _AGENT_PROFILE_FIELDS:
            current_list_key = None
            continue
        value = raw_value.strip()
        if key in _AGENT_LIST_FIELDS:
            if value:
                profile[key] = _parse_inline_list(value)
                current_list_key = None
            else:
                profile[key] = []
                current_list_key = key
        else:
            profile[key] = _parse_scalar(value)
            current_list_key = None
    if not isinstance(profile.get("name"), str) or not profile["name"]:
        profile["name"] = agent_name
    return {"profile": profile, "body": body}


def _yaml_string(value: Any) -> str:
    return json.dumps(str(value), ensure_ascii=False)


def serialize_agent_markdown(profile: dict[str, Any], body: str) -> str:
    normalized = _default_agent_profile(str(profile.get("name") or "agent"))
    for key in _AGENT_PROFILE_FIELDS:
        if key in profile:
            normalized[key] = profile[key]

    lines = ["---"]
    for key in _AGENT_PROFILE_FIELDS:
        value = normalized[key]
        if key in _AGENT_LIST_FIELDS:
            items = value if isinstance(value, list) else []
            if not items:
                lines.append(f"{key}: []")
            else:
                lines.append(f"{key}:")
                lines.extend(f"  - {_yaml_string(item)}" for item in items)
        elif isinstance(value, int):
            lines.append(f"{key}: {value}")
        else:
            lines.append(f"{key}: {_yaml_string(value)}")
    lines.append("---")
    normalized_body = body.lstrip("\n")
    return "\n".join(lines) + "\n\n" + normalized_body


def _read_agent_profile(agent_dir: str, agent_name: str) -> dict[str, Any]:
    agent_file = os.path.join(agent_dir, "AGENT.md")
    if not os.path.isfile(agent_file):
        parsed = {"profile": _default_agent_profile(agent_name), "body": ""}
        content = serialize_agent_markdown(parsed["profile"], parsed["body"])
    else:
        with open(agent_file, "r", encoding="utf-8") as f:
            content = f.read()
        parsed = parse_agent_markdown(content, agent_name)
    return {
        "agent": agent_name,
        "path": f"system/agents/{agent_name}/AGENT.md",
        "profile": parsed["profile"],
        "body": parsed["body"],
        "content": content,
    }


def _agent_dir_from_workspace_input(workspace_dir: str, agent_name: str) -> tuple[str | None, str | None]:
    if not agent_name:
        return None, None
    if not _is_child_name(agent_name):
        return None, "Invalid agent name"
    workspace_input = _normalize_path_input(workspace_dir) or "default.ws"
    if _is_workspace_name(workspace_input):
        return _agent_dir(workspace_input, agent_name)

    ws_root = os.path.realpath(workspace_input)
    if not os.path.isdir(ws_root):
        return None, "Workspace not found"
    agents_root = os.path.realpath(os.path.join(ws_root, "system", "agents"))
    agent_dir = os.path.realpath(os.path.join(agents_root, agent_name))
    if os.path.commonpath([agents_root, agent_dir]) != agents_root:
        return None, "Path traversal not allowed"
    if not os.path.isdir(agent_dir):
        return None, "Agent not found"
    return agent_dir, None


def _read_optional_text(path: str) -> str:
    if not os.path.isfile(path):
        return ""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _agent_runtime_config(workspace_dir: str, agent_name: str) -> tuple[dict[str, Any] | None, str | None]:
    if not agent_name:
        return None, None
    agent_dir, error = _agent_dir_from_workspace_input(workspace_dir, agent_name)
    if error or agent_dir is None:
        return None, error
    try:
        profile_data = _read_agent_profile(agent_dir, agent_name)
        profile = profile_data["profile"]
        model_override = str(profile.get("model") or "").strip()
        return {
            "agent_prompt": profile_data["body"],
            "agent_soul": _read_optional_text(os.path.join(agent_dir, "SOUL.md")),
            "tools_allowlist": _string_list(profile.get("tools")),
            "skill_allowlist": _string_list(profile.get("skills")),
            "model_override": model_override,
            "max_turns": _positive_int(profile.get("maxTurns")),
        }, None
    except (OSError, UnicodeDecodeError) as exc:
        return None, str(exc)


@app.get("/api/workspace/list")
async def list_workspaces():
    """List all .ws workspace folders"""
    workspaces = []
    if os.path.isdir(_WORKSPACE_ROOT):
        for name in sorted(os.listdir(_WORKSPACE_ROOT)):
            if name.endswith(".ws") and os.path.isdir(os.path.join(_WORKSPACE_ROOT, name)):
                workspaces.append(name)
    return {"success": True, "data": workspaces}


@app.get("/api/workspace/agents")
async def list_agents(ws: str = "default.ws"):
    """List agents in a workspace's system/agents/ directory"""
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    agents_dir = os.path.join(ws_root, "system", "agents")
    agents = []
    if os.path.isdir(agents_dir):
        for name in sorted(os.listdir(agents_dir)):
            agent_path = os.path.join(agents_dir, name)
            if os.path.isdir(agent_path):
                profile_data = _read_agent_profile(agent_path, name)
                profile = profile_data["profile"]
                agent_info: dict[str, Any] = {
                    "name": name,
                    "description": profile.get("description", ""),
                    "files": [],
                    "profile": profile,
                }
                for fname in sorted(os.listdir(agent_path)):
                    if fname.endswith(".md"):
                        agent_info["files"].append(fname)
                agents.append(agent_info)
    return {"success": True, "data": agents}


@app.get("/api/workspace/agent-profile")
async def read_agent_profile(ws: str = "default.ws", agent: str = ""):
    """Read AGENT.md frontmatter profile and markdown body."""
    agent_path, error = _agent_dir(ws, agent)
    if error or agent_path is None:
        return {"success": False, "error": error}
    try:
        return {"success": True, "data": _read_agent_profile(agent_path, agent)}
    except (OSError, UnicodeDecodeError) as exc:
        return {"success": False, "error": str(exc)}


@app.put("/api/workspace/agent-profile")
async def write_agent_profile(request: AgentProfileWriteRequest):
    """Write AGENT.md by serializing frontmatter profile and markdown body."""
    agent_path, error = _agent_dir(request.ws, request.agent)
    if error or agent_path is None:
        return {"success": False, "error": error}
    profile = dict(request.profile)
    profile["name"] = request.agent
    content = serialize_agent_markdown(profile, request.body)
    agent_file = os.path.join(agent_path, "AGENT.md")
    try:
        with open(agent_file, "w", encoding="utf-8") as f:
            f.write(content)
        parsed = parse_agent_markdown(content, request.agent)
        return {
            "success": True,
            "data": {
                "agent": request.agent,
                "path": f"system/agents/{request.agent}/AGENT.md",
                "profile": parsed["profile"],
                "body": parsed["body"],
                "content": content,
                "bytes": len(content.encode("utf-8")),
            },
        }
    except OSError as exc:
        return {"success": False, "error": str(exc)}


@app.get("/api/workspace/skills")
async def list_skills(ws: str = "default.ws"):
    """List skills in a workspace's system/skills/ directory"""
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    skills_dir = os.path.join(ws_root, "system", "skills")
    skills = []
    if os.path.isdir(skills_dir):
        for name in sorted(os.listdir(skills_dir)):
            skill_path = os.path.join(skills_dir, name)
            if os.path.isdir(skill_path):
                skills.append({"name": name})
    return {"success": True, "data": skills}


# Built-in tools from schema
_BUILTIN_TOOLS: list[dict[str, str]] | None = None


def _load_builtin_tools() -> list[dict[str, str]]:
    global _BUILTIN_TOOLS
    if _BUILTIN_TOOLS is not None:
        return _BUILTIN_TOOLS
    schema_path = os.path.join(os.path.dirname(__file__), "assets", "tools_schema.json")
    tools: list[dict[str, str]] = []
    if os.path.isfile(schema_path):
        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                schema = json.load(f)
            for item in schema:
                if isinstance(item, dict) and item.get("type") == "function":
                    func = item.get("function", {})
                    name = func.get("name", "")
                    if name:
                        tools.append({"name": name})
        except (OSError, json.JSONDecodeError):
            pass
    _BUILTIN_TOOLS = tools
    return tools


@app.get("/api/workspace/tools")
async def list_tools():
    """List built-in tools available to agents"""
    return {"success": True, "data": _load_builtin_tools()}


@app.get("/api/workspace/file")
async def read_workspace_file(ws: str = "default.ws", path: str = ""):
    """Read a file from workspace system/ directory"""
    real_path, normalized_path, error = _resolve_system_file_path(ws, path)
    if error or real_path is None or normalized_path is None:
        return {"success": False, "error": error}
    if not os.path.isfile(real_path):
        return {"success": False, "error": "File not found"}
    try:
        with open(real_path, "r", encoding="utf-8") as f:
            content = f.read()
        return {"success": True, "data": {"path": normalized_path, "content": content}}
    except (OSError, UnicodeDecodeError) as e:
        return {"success": False, "error": str(e)}


@app.put("/api/workspace/file")
async def write_workspace_file(request: WorkspaceFileWriteRequest):
    """Write a file to workspace system/ directory"""
    real_path, normalized_path, error = _resolve_system_file_path(request.ws, request.path)
    if error or real_path is None or normalized_path is None:
        return {"success": False, "error": error}
    if os.path.isdir(real_path):
        return {"success": False, "error": "Path is a directory"}

    created = not os.path.exists(real_path)
    try:
        os.makedirs(os.path.dirname(real_path), exist_ok=True)
        with open(real_path, "w", encoding="utf-8") as f:
            f.write(request.content)
        return {
            "success": True,
            "data": {
                "path": normalized_path,
                "content": request.content,
                "bytes": len(request.content.encode("utf-8")),
                "created": created,
            },
        }
    except (OSError, UnicodeError) as e:
        return {"success": False, "error": str(e)}


# Serve static files (frontend build)
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

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
