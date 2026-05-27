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
import shutil
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src.config import load_config
from src.core.XAgent import XAgent
from src.core.telemetry import Event, JsonlSink, MultiSink, NullSink
from src.core.eval import (
    EvalError,
    create_eval_run,
    download_dataset,
    execute_eval_run,
    get_dataset_detail,
    import_dataset_content,
    import_dataset_path,
    list_datasets,
    list_workspace_eval_datasets,
    list_eval_runs,
    read_eval_run,
    write_eval_run,
)
from src.main import build_agent, load_observability_config
from src.tools.file_index import get_file_index_stats, refresh_file_index, search_file_index
from src.tools.reflect.reader import group_by_session, load_dir, load_events
from src.tools.reflect.stats import TOKEN_FIELDS

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
    chat_id: str = ""
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


class WorkspaceIndexRefreshRequest(BaseModel):
    ws: str = "default.ws"
    root: str = ""
    semantic: bool = False
    config_path: str = ""


class EvalDatasetImportRequest(BaseModel):
    ws: str = "default.ws"
    name: str = ""
    format: str = ""
    content: str = ""
    path: str = ""


class EvalDatasetDownloadRequest(BaseModel):
    ws: str = "default.ws"
    name: str = ""
    format: str = ""
    url: str = ""


class EvalRunCreateRequest(BaseModel):
    ws: str = "default.ws"
    dataset_id: str = ""
    agent: str = ""
    case_limit: int = 0
    config_path: str = ""
    observability_config_path: str = ""


class AgentProfileWriteRequest(BaseModel):
    ws: str = "default.ws"
    agent: str = ""
    profile: dict[str, Any] = {}
    body: str = ""


class ChatCreateRequest(BaseModel):
    ws: str = "default.ws"
    agent: str = ""


class ScheduledTaskWriteRequest(BaseModel):
    ws: str = "default.ws"
    name: str = ""
    prompt: str = ""
    agent: str = ""
    repeat: str = "none"
    date: str = ""
    time: str = ""
    end_date: str = ""
    interval_minutes: int = 0
    keep_one_chat: bool = False
    status: str = ""
    config_path: str = ""
    observability_config_path: str = ""


class ScheduledTaskStatusRequest(BaseModel):
    ws: str = "default.ws"
    status: str = ""


class ScheduledTaskRunRequest(BaseModel):
    ws: str = "default.ws"


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
    runtime_config_key: str = ""
    session_id: str = ""
    chat_id: str = ""
    chat_ws: str = ""
    chat_agent: str = ""
    messages: list[dict[str, Any]] = field(default_factory=list)
    restored_llm_history: list[dict[str, Any]] = field(default_factory=list)
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
_eval_cancel_events: dict[str, threading.Event] = {}
_eval_lock = threading.Lock()

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
_TRACE_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_CHAT_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_TASK_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_TASK_TIME_RE = re.compile(r"^\d{2}:\d{2}$")
_TASK_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TASK_REPEATS = {"none", "daily", "weekly", "custom"}
_TASK_STATUSES = {"running", "paused"}
_task_lock = threading.Lock()
_task_scheduler_started = False
_task_scheduler_stop = threading.Event()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_json_value(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, ensure_ascii=False))
    except (TypeError, ValueError):
        return json.loads(json.dumps(str(value), ensure_ascii=False))


def _write_json_file(path: str, payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp_path, path)


def _read_json_file(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        value = json.load(f)
    return value if isinstance(value, dict) else {}


def _empty_usage() -> dict[str, int]:
    return {field: 0 for field in TOKEN_FIELDS}


def _event_token_data(event: dict[str, Any]) -> dict[str, int]:
    data = event.get("data") or {}
    tokens: dict[str, int] = {}
    for field in TOKEN_FIELDS:
        value = data.get(field)
        if isinstance(value, int) and not isinstance(value, bool):
            tokens[field] = value
    return tokens


def _add_usage(target: dict[str, int], source: dict[str, int]) -> None:
    for field, value in source.items():
        target[field] = target.get(field, 0) + value


def _config_log_dir(observability_config_path: str = "") -> str:
    env_log_dir = os.environ.get("XAGENT_LOG_DIR", "").strip()
    if env_log_dir:
        return env_log_dir
    config_path = _normalize_path_input(observability_config_path)
    if not config_path:
        return ""
    try:
        config = load_observability_config(config_path)
    except Exception:  # noqa: BLE001
        return ""
    return str(config.get("log_dir", "")).strip()


def _resolve_trace_log_dir(
    observability_config_path: str = "",
    workspace_dir: str = "",
    ws: str = "default.ws",
) -> str:
    configured_log_dir = _config_log_dir(observability_config_path)
    if configured_log_dir:
        return configured_log_dir

    normalized_workspace_dir = _normalize_path_input(workspace_dir)
    if normalized_workspace_dir:
        workspace_root = _resolve_workspace_dir_input(normalized_workspace_dir)
        if workspace_root:
            return os.path.join(workspace_root, "runtime", "traces")

    if not _is_workspace_name(ws):
        return ""
    return os.path.join(_WORKSPACE_ROOT, ws, "runtime", "traces")


def _trace_summary_from_events(events: list[dict[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    groups = group_by_session(events)
    rows: list[dict[str, Any]] = []
    for session_id, session_events in groups.items():
        started_at = 0.0
        ended_at = 0.0
        duration_ms = 0.0
        turns = 0
        exit_reason = ""
        run_usage: dict[str, int] = {}
        llm_usage = _empty_usage()

        for event in session_events:
            ts = float(event.get("ts", 0.0) or 0.0)
            if started_at == 0.0 or (ts and ts < started_at):
                started_at = ts
            kind = str(event.get("kind", ""))
            if kind == "llm_end":
                _add_usage(llm_usage, _event_token_data(event))
            elif kind == "run_end":
                ended_at = ts
                duration_ms = float(event.get("duration_ms", 0.0) or 0.0)
                data = event.get("data") or {}
                turns = int(data.get("turns", event.get("turn", 0)) or 0)
                exit_reason = str(event.get("name", ""))
                run_usage = _event_token_data(event)

        usage = run_usage or {field: value for field, value in llm_usage.items() if value}
        normalized_usage = _empty_usage()
        normalized_usage.update(usage)
        rows.append(
            {
                "session_id": str(session_id),
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_ms": duration_ms,
                "turns": turns,
                "exit_reason": exit_reason,
                "event_count": len(session_events),
                "usage": normalized_usage,
            }
        )

    rows.sort(key=lambda item: item.get("ended_at") or item.get("started_at") or 0, reverse=True)
    return rows[: max(1, min(limit, 200))]


def _usage_summary_from_events(events: list[dict[str, Any]], limit: int = 20) -> dict[str, Any]:
    totals = _empty_usage()
    rows = [row for row in _trace_summary_from_events(events, limit=limit) if any(row["usage"].values())]
    for row in rows:
        _add_usage(totals, row["usage"])
    return {
        "totals": totals,
        "sessions": rows,
    }


def _trace_file_path(log_dir: str, session_id: str) -> tuple[str | None, str | None]:
    if not _TRACE_SESSION_ID_RE.fullmatch(session_id):
        return None, "Invalid session id"
    root = os.path.realpath(log_dir)
    file_path = os.path.realpath(os.path.join(root, f"{session_id}.jsonl"))
    if os.path.commonpath([root, file_path]) != root:
        return None, "Path traversal not allowed"
    if not os.path.isfile(file_path):
        return None, "Trace session not found"
    return file_path, None


class WebSessionUsageSink:
    """Emit token usage events into the Web UI session stream."""

    def __init__(self, session: UISession) -> None:
        self.session = session
        self.totals = _empty_usage()

    def emit(self, event: Event) -> None:
        if event.kind not in {"llm_end", "run_end"}:
            return
        usage = _event_token_data({"data": event.data})
        if not usage:
            return
        if event.kind == "llm_end":
            _add_usage(self.totals, usage)
            event_type = "token_usage_delta"
        else:
            self.totals = _empty_usage()
            self.totals.update(usage)
            event_type = "token_usage_done"
        _emit(
            self.session,
            event_type,
            {
                "session_id": event.session_id,
                "turn": event.turn,
                "usage": {**_empty_usage(), **usage},
                "totals": dict(self.totals),
                "updated_at": event.ts,
            },
        )
        _queue_state(self.session, finished=False)

    def close(self) -> None:
        return None


def _visible_llm_output(text: str) -> str:
    visible = _HIDDEN_BLOCK_RE.sub("", text)
    visible = _TRAILING_PARTIAL_TAG_RE.sub("", visible)
    return visible


def _tool_key(name: str, arguments: dict[str, Any]) -> str:
    return f"{name}:{json.dumps(arguments, ensure_ascii=False, sort_keys=True)}"


def _new_ui_message(role: str, content: str, status: str = "complete") -> dict[str, Any]:
    return {
        "id": uuid.uuid4().hex[:12],
        "role": role,
        "content": content,
        "timestamp": _now_ms(),
        "status": status,
    }


def _ensure_ui_agent_message(session: UISession) -> dict[str, Any]:
    if session.messages and session.messages[-1].get("role") == "agent":
        return session.messages[-1]
    message = _new_ui_message("agent", "", status="streaming")
    session.messages.append(message)
    return message


def _append_ui_delta(current: str, delta: str) -> str:
    if not delta or current.endswith(delta):
        return current
    return current + delta


def _update_session_messages(session: UISession, event_type: str, data: Any = None) -> None:
    if event_type in {"user_task", "user_reply"}:
        session.messages.append(_new_ui_message("user", str(data or "")))
        return
    if event_type == "assistant_delta":
        message = _ensure_ui_agent_message(session)
        message["content"] = _append_ui_delta(str(message.get("content", "")), str(data or ""))
        message["status"] = "streaming"
        return
    if event_type == "thinking_delta":
        message = _ensure_ui_agent_message(session)
        message["thinking"] = _append_ui_delta(str(message.get("thinking", "")), str(data or ""))
        message["status"] = "streaming"
        return
    if event_type == "turn_start":
        message = _ensure_ui_agent_message(session)
        payload = data if isinstance(data, dict) else {}
        metadata = message.get("metadata") if isinstance(message.get("metadata"), dict) else {}
        message["metadata"] = {**metadata, "turn": payload.get("turn", 0)}
        return
    if event_type == "tool_call":
        message = _ensure_ui_agent_message(session)
        tool_calls = message.get("toolCalls")
        if not isinstance(tool_calls, list):
            tool_calls = []
        payload = data if isinstance(data, dict) else {}
        tool_id = str(payload.get("id", uuid.uuid4().hex[:8]))
        if not any(item.get("id") == tool_id for item in tool_calls if isinstance(item, dict)):
            tool_calls.append(
                {
                    "id": tool_id,
                    "name": str(payload.get("name", "")),
                    "arguments": payload.get("arguments") if isinstance(payload.get("arguments"), dict) else {},
                    "status": str(payload.get("status", "success")),
                }
            )
        message["toolCalls"] = tool_calls
        return
    if event_type == "done":
        payload = data if isinstance(data, dict) else {}
        message = _ensure_ui_agent_message(session)
        if not message.get("content") and payload.get("response"):
            message["content"] = str(payload.get("response", ""))
        message["status"] = "complete"
        metadata = message.get("metadata") if isinstance(message.get("metadata"), dict) else {}
        message["metadata"] = {
            **metadata,
            "exitReason": str(payload.get("exit_reason", "")),
            "completedAt": _now_ms(),
        }
        tool_results = payload.get("tool_results") if isinstance(payload.get("tool_results"), list) else []
        fallback_tools = []
        for item in tool_results:
            if not isinstance(item, dict):
                continue
            tool_name = str(item.get("tool_name", ""))
            if not tool_name or tool_name == "no_tool":
                continue
            fallback_tools.append(
                {
                    "id": str(item.get("tool_call_id") or tool_name),
                    "name": tool_name,
                    "arguments": {},
                    "status": "success",
                    "result": item.get("data"),
                }
            )
        if fallback_tools and not message.get("toolCalls"):
            message["toolCalls"] = fallback_tools
        return
    if event_type in {"error", "stop"}:
        session.messages.append(_new_ui_message("system", str(data or ""), status="error" if event_type == "error" else "complete"))


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
    _update_session_messages(session, event_type, data)
    _persist_chat_state(session)


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


def _valid_chat_id(chat_id: str) -> bool:
    return bool(chat_id) and _CHAT_ID_RE.fullmatch(chat_id) is not None and chat_id not in {".", ".."}


def _chat_root(ws: str, agent: str) -> tuple[str | None, str | None]:
    if not _is_workspace_name(ws):
        return None, "Invalid workspace name"
    if not _is_child_name(agent):
        return None, "Invalid agent name"
    ws_root = os.path.realpath(os.path.join(_WORKSPACE_ROOT, ws))
    workspace_parent = os.path.realpath(_WORKSPACE_ROOT)
    if os.path.commonpath([workspace_parent, ws_root]) != workspace_parent:
        return None, "Path traversal not allowed"
    return os.path.join(ws_root, "runtime", "chats", agent), None


def _chat_dir(ws: str, agent: str, chat_id: str) -> tuple[str | None, str | None]:
    if not _valid_chat_id(chat_id):
        return None, "Invalid chat id"
    root, error = _chat_root(ws, agent)
    if error or root is None:
        return None, error
    real_root = os.path.realpath(root)
    chat_path = os.path.realpath(os.path.join(real_root, chat_id))
    if os.path.commonpath([real_root, chat_path]) != real_root:
        return None, "Path traversal not allowed"
    return chat_path, None


def _chat_metadata_path(ws: str, agent: str, chat_id: str) -> tuple[str | None, str | None]:
    chat_path, error = _chat_dir(ws, agent, chat_id)
    if error or chat_path is None:
        return None, error
    return os.path.join(chat_path, "metadata.json"), None


def _chat_state_path(ws: str, agent: str, chat_id: str) -> tuple[str | None, str | None]:
    chat_path, error = _chat_dir(ws, agent, chat_id)
    if error or chat_path is None:
        return None, error
    return os.path.join(chat_path, "state.json"), None


def _default_chat_metadata(ws: str, agent: str, chat_id: str) -> dict[str, Any]:
    now = time.time()
    return {
        "chat_id": chat_id,
        "workspace": ws,
        "agent": agent,
        "title": "New Chat",
        "created_at": now,
        "updated_at": now,
        "last_message_preview": "",
        "message_count": 0,
        "status": "idle",
    }


def _default_chat_state(ws: str, agent: str, chat_id: str, session_id: str = "") -> dict[str, Any]:
    return {
        "chat_id": chat_id,
        "workspace": ws,
        "agent": agent,
        "backend_session_id": session_id,
        "runtime_config_key": "",
        "messages": [],
        "llm_history": [],
        "waiting_for_user": False,
        "ask_prompt": "",
        "status": "idle",
        "updated_at": time.time(),
    }


def _read_chat_metadata(ws: str, agent: str, chat_id: str) -> dict[str, Any] | None:
    path, error = _chat_metadata_path(ws, agent, chat_id)
    if error or path is None or not os.path.isfile(path):
        return None
    try:
        return _read_json_file(path)
    except (OSError, json.JSONDecodeError):
        return None


def _read_chat_state(ws: str, agent: str, chat_id: str) -> dict[str, Any] | None:
    path, error = _chat_state_path(ws, agent, chat_id)
    if error or path is None or not os.path.isfile(path):
        return None
    try:
        return _read_json_file(path)
    except (OSError, json.JSONDecodeError):
        return None


def _extract_llm_history(agent: object | None) -> list[dict[str, Any]]:
    if agent is None:
        return []
    client = getattr(agent, "client", None)
    backend = getattr(client, "backend", None)
    history = getattr(backend, "history", [])
    return _safe_json_value(history) if isinstance(history, list) else []


def _restore_llm_history(agent: object | None, history: list[dict[str, Any]]) -> None:
    if agent is None or not isinstance(history, list):
        return
    client = getattr(agent, "client", None)
    backend = getattr(client, "backend", None)
    if backend is None or not hasattr(backend, "history"):
        return
    try:
        backend.history = _safe_json_value(history)
    except Exception:  # noqa: BLE001
        return


def _chat_preview(messages: list[dict[str, Any]]) -> str:
    for message in reversed(messages):
        content = str(message.get("content", "")).strip()
        if content:
            return content[:80]
    return ""


def _chat_title(metadata: dict[str, Any], messages: list[dict[str, Any]]) -> str:
    current = str(metadata.get("title", "New Chat") or "New Chat")
    if current != "New Chat":
        return current
    for message in messages:
        if message.get("role") == "user":
            content = str(message.get("content", "")).strip()
            if content:
                return content[:40]
    return current


def _persist_chat_state(session: UISession) -> None:
    if not session.chat_id or not session.chat_ws or not session.chat_agent:
        return
    metadata_path, error = _chat_metadata_path(session.chat_ws, session.chat_agent, session.chat_id)
    state_path, state_error = _chat_state_path(session.chat_ws, session.chat_agent, session.chat_id)
    if error or state_error or metadata_path is None or state_path is None:
        return
    metadata = _read_chat_metadata(session.chat_ws, session.chat_agent, session.chat_id)
    if metadata is None:
        metadata = _default_chat_metadata(session.chat_ws, session.chat_agent, session.chat_id)
    status = "waiting_for_user" if session.waiting_for_user else ("running" if session.running else "idle")
    messages = _safe_json_value(session.messages)
    llm_history = _extract_llm_history(session.agent) or _safe_json_value(session.restored_llm_history)
    now = time.time()
    metadata.update(
        {
            "workspace": session.chat_ws,
            "agent": session.chat_agent,
            "title": _chat_title(metadata, messages),
            "updated_at": now,
            "last_message_preview": _chat_preview(messages),
            "message_count": len(messages),
            "status": status,
        }
    )
    state = {
        **_default_chat_state(session.chat_ws, session.chat_agent, session.chat_id, session.session_id),
        "backend_session_id": session.session_id,
        "runtime_config_key": session.runtime_config_key,
        "messages": messages,
        "llm_history": llm_history,
        "waiting_for_user": session.waiting_for_user,
        "ask_prompt": session.ask_prompt,
        "status": status,
        "updated_at": now,
    }
    try:
        _write_json_file(metadata_path, metadata)
        _write_json_file(state_path, state)
    except OSError:
        return


def _load_chat_into_session(ws: str, agent: str, chat_id: str, session: UISession | None = None) -> tuple[UISession | None, str | None]:
    chat_path, error = _chat_dir(ws, agent, chat_id)
    if error or chat_path is None:
        return None, error
    metadata = _read_chat_metadata(ws, agent, chat_id)
    state = _read_chat_state(ws, agent, chat_id)
    if metadata is None or state is None:
        return None, "Chat not found"
    session = session or UISession(session_id=str(state.get("backend_session_id") or uuid.uuid4().hex[:16]))
    session.chat_id = chat_id
    session.chat_ws = ws
    session.chat_agent = agent
    session.session_id = session.session_id or uuid.uuid4().hex[:16]
    messages = state.get("messages")
    session.messages = _safe_json_value(messages) if isinstance(messages, list) else []
    history = state.get("llm_history")
    session.restored_llm_history = _safe_json_value(history) if isinstance(history, list) else []
    session.waiting_for_user = False
    session.running = False
    session.ask_prompt = ""
    _sessions[session.session_id] = session
    return session, None


def _find_session_by_chat(ws: str, agent: str, chat_id: str) -> UISession | None:
    for session in _sessions.values():
        if session.chat_ws == ws and session.chat_agent == agent and session.chat_id == chat_id:
            return session
    return None


def _file_index_embedding_config_from_path(config_path: str) -> dict[str, Any] | None:
    normalized = _normalize_path_input(config_path)
    if not normalized:
        return None
    try:
        configs = load_config(normalized)
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    if not configs:
        return None
    return XAgent._file_index_embedding_config(next(iter(configs.values())))


def _ensure_agent(
    session: UISession,
    config_path: str,
    observability_config_path: str,
    workspace_dir: str,
    agent_name: str,
    runtime_config: dict[str, Any] | None = None,
):
    runtime_config = runtime_config or {}
    runtime_config_key = json.dumps(runtime_config, ensure_ascii=False, sort_keys=True, default=str)
    if (
        session.agent is None
        or session.config_path != config_path
        or session.observability_config_path != observability_config_path
        or session.workspace_dir != workspace_dir
        or session.agent_name != agent_name
        or session.runtime_config_key != runtime_config_key
    ):
        _close_agent(session)
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
            memory_mode=str(runtime_config.get("memory_mode", "project")),
        )
        web_usage_sink = WebSessionUsageSink(session)
        trace_log_dir = ""
        if not _config_log_dir(observability_config_path):
            trace_log_dir = _resolve_trace_log_dir(workspace_dir=workspace_dir)
        trace_sink = JsonlSink(trace_log_dir) if trace_log_dir else NullSink()
        existing_sink = getattr(session.agent, "sink", NullSink())
        session.agent.sink = MultiSink(existing_sink, trace_sink, web_usage_sink)
        session.agent.handler.ctx.sink = session.agent.sink
        session.agent.handler.ctx.verbose = True
        if session.restored_llm_history:
            _restore_llm_history(session.agent, session.restored_llm_history)
            session.restored_llm_history = []
        session.config_path = config_path
        session.observability_config_path = observability_config_path
        session.workspace_dir = workspace_dir
        session.agent_name = agent_name
        session.runtime_config_key = runtime_config_key
    elif session.restored_llm_history:
        _restore_llm_history(session.agent, session.restored_llm_history)
        session.restored_llm_history = []
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
            _persist_chat_state(session)
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
            _persist_chat_state(session)
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
    chat_id = _normalize_path_input(request.chat_id)
    workspace_dir = _normalize_path_input(request.workspace_dir)
    agent_name = _normalize_path_input(request.agent)
    chat_ws = workspace_dir if _is_workspace_name(workspace_dir) else "default.ws"
    if chat_id and not agent_name:
        return {"success": False, "error": "Agent is required for persistent chat"}
    session = _sessions.get(requested_session_id) if requested_session_id else None
    if session is None and chat_id:
        session = _find_session_by_chat(chat_ws, agent_name, chat_id)
    if session is None and chat_id:
        session, load_error = _load_chat_into_session(chat_ws, agent_name, chat_id)
        if load_error or session is None:
            return {"success": False, "error": load_error}
    if session is None:
        session_id = uuid.uuid4().hex[:16]
        session = UISession(session_id=session_id)
        _sessions[session_id] = session
    else:
        session_id = session.session_id

    config_path = _normalize_path_input(request.config_path)
    observability_config_path = _normalize_path_input(request.observability_config_path)
    if chat_id:
        session.chat_id = chat_id
        session.chat_ws = chat_ws
        session.chat_agent = agent_name
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
    _persist_chat_state(session)

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
        _persist_chat_state(active_session)

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
                _persist_chat_state(session)
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


@app.get("/api/chats")
async def list_chats(ws: str = "default.ws", agent: str = ""):
    root, error = _chat_root(ws, agent)
    if error or root is None:
        return {"success": False, "error": error}
    if not os.path.isdir(root):
        return {"success": True, "data": []}
    rows: list[dict[str, Any]] = []
    try:
        for entry in os.scandir(root):
            if not entry.is_dir(follow_symlinks=False) or not _valid_chat_id(entry.name):
                continue
            metadata = _read_chat_metadata(ws, agent, entry.name)
            if metadata is not None:
                rows.append(metadata)
    except OSError as exc:
        return {"success": False, "error": str(exc)}
    rows.sort(key=lambda item: float(item.get("updated_at", 0.0) or 0.0), reverse=True)
    return {"success": True, "data": rows}


@app.post("/api/chats")
async def create_chat(request: ChatCreateRequest):
    ws = _normalize_path_input(request.ws) or "default.ws"
    agent = _normalize_path_input(request.agent)
    if not agent:
        return {"success": False, "error": "Agent is required"}
    root, error = _chat_root(ws, agent)
    if error or root is None:
        return {"success": False, "error": error}
    chat_id = uuid.uuid4().hex[:16]
    metadata = _default_chat_metadata(ws, agent, chat_id)
    session_id = uuid.uuid4().hex[:16]
    state = _default_chat_state(ws, agent, chat_id, session_id=session_id)
    metadata_path, metadata_error = _chat_metadata_path(ws, agent, chat_id)
    state_path, state_error = _chat_state_path(ws, agent, chat_id)
    if metadata_error or state_error or metadata_path is None or state_path is None:
        return {"success": False, "error": metadata_error or state_error}
    try:
        _write_json_file(metadata_path, metadata)
        _write_json_file(state_path, state)
    except OSError as exc:
        return {"success": False, "error": str(exc)}
    session = UISession(
        session_id=session_id,
        chat_id=chat_id,
        chat_ws=ws,
        chat_agent=agent,
    )
    _sessions[session_id] = session
    return {"success": True, "data": {"metadata": metadata, "state": state}}


@app.get("/api/chats/{chat_id}")
async def read_chat(chat_id: str, ws: str = "default.ws", agent: str = ""):
    ws = _normalize_path_input(ws) or "default.ws"
    agent = _normalize_path_input(agent)
    if not agent:
        return {"success": False, "error": "Agent is required"}
    session = _find_session_by_chat(ws, agent, chat_id)
    if session is None:
        session, error = _load_chat_into_session(ws, agent, chat_id)
        if error or session is None:
            return {"success": False, "error": error}
    metadata = _read_chat_metadata(ws, agent, chat_id)
    state = _read_chat_state(ws, agent, chat_id)
    if metadata is None or state is None:
        return {"success": False, "error": "Chat not found"}
    state = {**state, "backend_session_id": session.session_id, "status": "idle"}
    metadata = {**metadata, "status": "idle" if metadata.get("status") == "running" else metadata.get("status", "idle")}
    return {"success": True, "data": {"metadata": metadata, "state": state}}


@app.delete("/api/chats/{chat_id}")
async def delete_chat(chat_id: str, ws: str = "default.ws", agent: str = ""):
    ws = _normalize_path_input(ws) or "default.ws"
    agent = _normalize_path_input(agent)
    if not agent:
        return {"success": False, "error": "Agent is required"}
    session = _find_session_by_chat(ws, agent, chat_id)
    if session is not None and session.running:
        return {"success": False, "error": "Chat is running; stop the task before deleting it"}
    chat_path, error = _chat_dir(ws, agent, chat_id)
    if error or chat_path is None:
        return {"success": False, "error": error}
    if not os.path.isdir(chat_path):
        return {"success": False, "error": "Chat not found"}
    try:
        shutil.rmtree(chat_path)
    except OSError as exc:
        return {"success": False, "error": str(exc)}
    if session is not None:
        _sessions.pop(session.session_id, None)
        _close_agent(session)
    return {"success": True}


@app.get("/api/usage/summary")
async def read_usage_summary(
    observability_config_path: str = "",
    limit: int = 20,
    ws: str = "default.ws",
):
    log_dir = _resolve_trace_log_dir(observability_config_path=observability_config_path, ws=ws)
    if not log_dir:
        return {"success": False, "error": "Invalid workspace name"}

    if not os.path.isdir(log_dir):
        return {
            "success": True,
            "data": {
                "configured": True,
                "log_dir": log_dir,
                "message": "Trace log directory does not exist yet. Run a task to create it.",
                "totals": _empty_usage(),
                "sessions": [],
                "updated_at": time.time(),
            },
        }

    try:
        summary = _usage_summary_from_events(load_dir(log_dir), limit=limit)
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": str(exc)}

    sessions = summary["sessions"]
    return {
        "success": True,
        "data": {
            "configured": True,
            "log_dir": log_dir,
            "message": "" if sessions else "No token usage events found yet.",
            "totals": summary["totals"],
            "sessions": sessions,
            "updated_at": time.time(),
        },
    }


@app.get("/api/trace/sessions")
async def read_trace_sessions(
    observability_config_path: str = "",
    limit: int = 20,
    ws: str = "default.ws",
):
    log_dir = _resolve_trace_log_dir(observability_config_path=observability_config_path, ws=ws)
    if not log_dir:
        return {"success": False, "error": "Invalid workspace name"}
    if not os.path.isdir(log_dir):
        return {
            "success": True,
            "data": {
                "configured": True,
                "log_dir": log_dir,
                "message": "Trace log directory does not exist yet. Run a task to create it.",
                "sessions": [],
                "updated_at": time.time(),
            },
        }

    try:
        sessions = _trace_summary_from_events(load_dir(log_dir), limit=limit)
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": str(exc)}

    return {
        "success": True,
        "data": {
            "configured": True,
            "log_dir": log_dir,
            "message": "" if sessions else "No trace sessions recorded yet.",
            "sessions": sessions,
            "updated_at": time.time(),
        },
    }


@app.get("/api/trace/sessions/{session_id}")
async def read_trace_session_detail(
    session_id: str,
    observability_config_path: str = "",
    ws: str = "default.ws",
):
    log_dir = _resolve_trace_log_dir(observability_config_path=observability_config_path, ws=ws)
    if not log_dir:
        return {"success": False, "error": "Invalid workspace name"}

    file_path, error = _trace_file_path(log_dir, session_id)
    if error or file_path is None:
        return {"success": False, "error": error}

    try:
        events = load_events(file_path)
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": str(exc)}

    summaries = _trace_summary_from_events(events, limit=1)
    summary = summaries[0] if summaries else {
        "session_id": session_id,
        "started_at": 0.0,
        "ended_at": 0.0,
        "duration_ms": 0.0,
        "turns": 0,
        "exit_reason": "",
        "event_count": len(events),
        "usage": _empty_usage(),
    }
    return {
        "success": True,
        "data": {
            "summary": summary,
            "events": events,
            "log_path": file_path,
        },
    }


# === Workspace Management API ===

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_WORKSPACE_ROOT = os.path.join(_PROJECT_ROOT, "workspace")
_DEFAULT_WORKSPACE_ROOT = _WORKSPACE_ROOT
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


def _resolve_workspace_preview_path(ws: str, path: str) -> tuple[str | None, str | None, str | None]:
    if not path:
        return None, None, "Path is required"
    if os.path.isabs(path):
        return None, None, "Absolute paths are not allowed"
    normalized_path = path.replace("\\", "/")

    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return None, None, error

    real_path = os.path.realpath(os.path.join(ws_root, normalized_path))
    if os.path.commonpath([ws_root, real_path]) != ws_root:
        return None, None, "Path traversal not allowed"
    return real_path, normalized_path, None


def _build_dir_tree(root_path: str, current_path: str = "") -> dict[str, Any]:
    name = os.path.basename(current_path) if current_path else os.path.basename(root_path)
    node: dict[str, Any] = {
        "name": name,
        "path": current_path or name,
        "type": "dir",
        "children": [],
    }
    try:
        entries = sorted(
            os.scandir(root_path),
            key=lambda item: (not item.is_dir(follow_symlinks=False), item.name.lower()),
        )
    except OSError:
        return node

    children: list[dict[str, Any]] = []
    for entry in entries:
        if entry.name == ".DS_Store" or entry.is_symlink():
            continue
        child_path = f"{current_path}/{entry.name}" if current_path else entry.name
        if entry.is_dir(follow_symlinks=False):
            children.append(_build_dir_tree(entry.path, child_path))
        elif entry.is_file(follow_symlinks=False):
            children.append(
                {
                    "name": entry.name,
                    "path": child_path,
                    "type": "file",
                }
            )
    node["children"] = children
    return node


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
            "memory_mode": str(profile.get("memory") or "project").strip() or "project",
        }, None
    except (OSError, UnicodeDecodeError) as exc:
        return None, str(exc)


def _valid_task_id(task_id: str) -> bool:
    return bool(task_id) and _TASK_ID_RE.fullmatch(task_id) is not None and task_id not in {".", ".."}


def _task_store_path(ws: str) -> tuple[str | None, str | None]:
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return None, error
    return os.path.join(ws_root, "runtime", "tasks", "tasks.json"), None


def _read_scheduled_tasks(ws: str) -> tuple[list[dict[str, Any]], str | None]:
    path, error = _task_store_path(ws)
    if error or path is None:
        return [], error
    if not os.path.isfile(path):
        if _should_seed_default_tasks(ws):
            tasks = _default_scheduled_tasks()
            write_error = _write_scheduled_tasks(ws, tasks)
            return tasks, write_error
        return [], None
    try:
        payload = _read_json_file(path)
    except (OSError, json.JSONDecodeError) as exc:
        return [], str(exc)
    raw_tasks = payload.get("tasks", [])
    if not isinstance(raw_tasks, list):
        return [], "Invalid task store"
    return [item for item in raw_tasks if isinstance(item, dict)], None


def _write_scheduled_tasks(ws: str, tasks: list[dict[str, Any]]) -> str | None:
    path, error = _task_store_path(ws)
    if error or path is None:
        return error
    try:
        _write_json_file(path, {"tasks": tasks, "updated_at": time.time()})
    except OSError as exc:
        return str(exc)
    return None


def _should_seed_default_tasks(ws: str) -> bool:
    return ws == "default.ws" and os.path.realpath(_WORKSPACE_ROOT) == os.path.realpath(_DEFAULT_WORKSPACE_ROOT)


def _parse_task_datetime(date_value: str, time_value: str) -> float | None:
    date_value = _normalize_path_input(date_value)
    time_value = _normalize_path_input(time_value)
    if not date_value or not time_value:
        return None
    if not _TASK_DATE_RE.fullmatch(date_value) or not _TASK_TIME_RE.fullmatch(time_value):
        raise ValueError("Invalid date or time")
    parsed = datetime.strptime(f"{date_value} {time_value}", "%Y-%m-%d %H:%M")
    return parsed.timestamp()


def _format_task_datetime(timestamp: float | None) -> str | None:
    if timestamp is None:
        return None
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(timestamp))


def _task_end_timestamp(task: dict[str, Any]) -> float | None:
    end_date = str(task.get("end_date") or "").strip()
    if not end_date:
        return None
    if not _TASK_DATE_RE.fullmatch(end_date):
        raise ValueError("Invalid end date")
    return datetime.strptime(f"{end_date} 23:59", "%Y-%m-%d %H:%M").timestamp()


def _next_task_run(task: dict[str, Any], after_ts: float | None = None, first: bool = False) -> str | None:
    after_ts = time.time() if after_ts is None else after_ts
    repeat = str(task.get("repeat") or "none")
    base_ts = _parse_task_datetime(str(task.get("date") or ""), str(task.get("time") or ""))
    next_ts: float | None = None

    if repeat == "none":
        next_ts = base_ts if first else None
    elif repeat == "daily":
        if base_ts is not None:
            candidate = datetime.fromtimestamp(base_ts)
            current = datetime.fromtimestamp(after_ts)
            candidate = candidate.replace(year=current.year, month=current.month, day=current.day)
            while candidate.timestamp() <= after_ts:
                candidate += timedelta(days=1)
            next_ts = candidate.timestamp()
    elif repeat == "weekly":
        if base_ts is not None:
            candidate = datetime.fromtimestamp(base_ts)
            current = datetime.fromtimestamp(after_ts)
            days = (candidate.weekday() - current.weekday()) % 7
            candidate = current.replace(
                hour=candidate.hour,
                minute=candidate.minute,
                second=0,
                microsecond=0,
            ) + timedelta(days=days)
            if candidate.timestamp() <= after_ts:
                candidate += timedelta(days=7)
            next_ts = candidate.timestamp()
    elif repeat == "custom":
        interval = int(task.get("interval_minutes") or 0)
        if interval <= 0:
            interval = 60
        if base_ts is None:
            next_ts = after_ts + interval * 60
        else:
            next_ts = base_ts
            while next_ts <= after_ts:
                next_ts += interval * 60

    end_ts = _task_end_timestamp(task)
    if next_ts is not None and end_ts is not None and next_ts > end_ts:
        return None
    return _format_task_datetime(next_ts)


def _normalize_scheduled_task(
    request: ScheduledTaskWriteRequest,
    *,
    task_id: str | None = None,
    existing: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    ws = _normalize_path_input(request.ws) or "default.ws"
    if not _is_workspace_name(ws):
        return None, "Invalid workspace name"
    name = str(request.name or "").strip()
    prompt = str(request.prompt or "").strip()
    if not name:
        return None, "Name is required"
    if not prompt:
        return None, "Prompt is required"

    repeat = str(request.repeat or "none").strip() or "none"
    if repeat not in _TASK_REPEATS:
        return None, "Invalid repeat"
    status = str(request.status or (existing or {}).get("status") or "").strip()
    if not status:
        status = "running"
    if status not in _TASK_STATUSES:
        return None, "Invalid status"

    agent = _normalize_path_input(request.agent)
    if agent and not _is_child_name(agent):
        return None, "Invalid agent name"
    if request.keep_one_chat and not agent:
        return None, "Agent is required when keeping task runs in one chat"
    if repeat in {"none", "daily", "weekly"} and (
        not _normalize_path_input(request.date) or not _normalize_path_input(request.time)
    ):
        return None, "Date and time are required"
    interval_minutes = int(request.interval_minutes or 0)
    if repeat == "custom" and interval_minutes <= 0:
        interval_minutes = 60

    now = time.time()
    task = {
        **(existing or {}),
        "id": task_id or uuid.uuid4().hex[:16],
        "workspace": ws,
        "name": name,
        "prompt": prompt,
        "agent": agent,
        "repeat": repeat,
        "date": _normalize_path_input(request.date),
        "time": _normalize_path_input(request.time),
        "end_date": _normalize_path_input(request.end_date),
        "interval_minutes": interval_minutes,
        "keep_one_chat": bool(request.keep_one_chat),
        "status": status,
        "config_path": _normalize_path_input(request.config_path),
        "observability_config_path": _normalize_path_input(request.observability_config_path),
        "updated_at": now,
    }
    task.setdefault("created_at", now)
    task.setdefault("chat_id", "")
    task.setdefault("last_run", None)
    task.setdefault("last_session_id", "")
    task.setdefault("last_error", "")
    task.setdefault("last_debug_run", None)
    task.setdefault("last_debug_session_id", "")
    task.setdefault("last_debug_error", "")

    try:
        next_run = _next_task_run(task, after_ts=now, first=True)
    except ValueError as exc:
        return None, str(exc)
    task["next_run"] = next_run
    if not next_run:
        task["status"] = "paused"
    return task, None


def _seed_date_for_weekday(target_weekday: int, now_ts: float) -> str:
    current = datetime.fromtimestamp(now_ts)
    days = (target_weekday - current.weekday()) % 7
    return (current + timedelta(days=days)).strftime("%Y-%m-%d")


def _default_scheduled_tasks(now_ts: float | None = None) -> list[dict[str, Any]]:
    now_ts = time.time() if now_ts is None else now_ts
    today = datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d")
    seeds = [
        {
            "id": "preset-daily-check",
            "name": "Daily Workspace Check",
            "prompt": "检查当前 workspace 的状态，汇总需要用户关注的问题，并给出简短建议。",
            "agent": "main",
            "repeat": "daily",
            "date": today,
            "time": "09:00",
        },
        {
            "id": "preset-weekly-code-quality-audit",
            "name": "Weekly Code Quality Audit",
            "prompt": (
                "对当前仓库做一次代码质量审计：检查未提交改动、关键后端/前端文件的职责是否清晰、"
                "是否存在明显类型/测试/启动风险；输出按严重程度排序的问题列表，并给出最小修复建议。"
                "不要直接修改代码。"
            ),
            "agent": "coding",
            "repeat": "weekly",
            "date": _seed_date_for_weekday(4, now_ts),
            "time": "18:00",
        },
        {
            "id": "preset-runtime-observability-review",
            "name": "Runtime Observability Review",
            "prompt": (
                "检查 workspace/runtime 下最近的 traces、chats、tasks 等运行时产物：总结最近任务是否正常结束、"
                "是否有重复错误或异常增长，并提出需要清理或进一步排查的项目。只读检查，不要删除文件。"
            ),
            "agent": "main",
            "repeat": "custom",
            "date": today,
            "time": "10:30",
            "interval_minutes": 240,
        },
        {
            "id": "preset-docs-implementation-consistency",
            "name": "Docs Implementation Consistency",
            "prompt": (
                "对 docs/ 中的关键设计文档与当前实现做一致性检查，重点比较 scheduled task、workspace、"
                "agent profile、chat persistence 相关契约是否和代码一致；列出文档过期、实现偏离、测试缺口，"
                "并标注建议修复优先级。不要直接修改文件。"
            ),
            "agent": "coding",
            "repeat": "weekly",
            "date": _seed_date_for_weekday(0, now_ts),
            "time": "10:00",
        },
    ]
    tasks: list[dict[str, Any]] = []
    for seed in seeds:
        task, error = _normalize_scheduled_task(
            ScheduledTaskWriteRequest(
                ws="default.ws",
                name=seed["name"],
                prompt=seed["prompt"],
                agent=seed["agent"],
                repeat=seed["repeat"],
                date=seed["date"],
                time=seed["time"],
                interval_minutes=seed.get("interval_minutes", 0),
                keep_one_chat=True,
                status="running",
                config_path="config.json",
            ),
            task_id=seed["id"],
        )
        if task is not None and error is None:
            tasks.append(task)
    return tasks


def _create_or_load_task_chat(task: dict[str, Any]) -> tuple[UISession | None, str | None]:
    ws = str(task.get("workspace") or "default.ws")
    agent = str(task.get("agent") or "")
    if not agent:
        return None, "Agent is required when keeping task runs in one chat"

    chat_id = str(task.get("chat_id") or "")
    if chat_id:
        session = _find_session_by_chat(ws, agent, chat_id)
        if session is None:
            session, error = _load_chat_into_session(ws, agent, chat_id)
            if error or session is None:
                return None, error
        return session, None

    chat_id = uuid.uuid4().hex[:16]
    metadata = _default_chat_metadata(ws, agent, chat_id)
    metadata["title"] = str(task.get("name") or "Scheduled Task")[:40]
    session_id = uuid.uuid4().hex[:16]
    state = _default_chat_state(ws, agent, chat_id, session_id=session_id)
    metadata_path, metadata_error = _chat_metadata_path(ws, agent, chat_id)
    state_path, state_error = _chat_state_path(ws, agent, chat_id)
    if metadata_error or state_error or metadata_path is None or state_path is None:
        return None, metadata_error or state_error
    try:
        _write_json_file(metadata_path, metadata)
        _write_json_file(state_path, state)
    except OSError as exc:
        return None, str(exc)
    session = UISession(session_id=session_id, chat_id=chat_id, chat_ws=ws, chat_agent=agent)
    _sessions[session_id] = session
    task["chat_id"] = chat_id
    return session, None


def _dispatch_scheduled_task(task: dict[str, Any]) -> tuple[str | None, str | None]:
    ws = str(task.get("workspace") or "default.ws")
    agent_name = str(task.get("agent") or "")
    runtime_config, runtime_error = _agent_runtime_config(ws, agent_name)
    if runtime_error:
        return None, runtime_error

    if task.get("keep_one_chat"):
        session, chat_error = _create_or_load_task_chat(task)
        if chat_error or session is None:
            return None, chat_error
        if session.running:
            return None, "Task chat is already running"
    else:
        session = UISession(session_id=uuid.uuid4().hex[:16])
        _sessions[session.session_id] = session

    _ensure_agent(
        session,
        str(task.get("config_path") or ""),
        str(task.get("observability_config_path") or ""),
        ws,
        agent_name,
        runtime_config,
    )
    _persist_chat_state(session)
    thread = threading.Thread(
        target=_run_task_background,
        args=(session, str(task.get("prompt") or "")),
        daemon=True,
    )
    thread.start()
    return session.session_id, None


def _run_scheduled_task_record(task: dict[str, Any], now_ts: float | None = None) -> dict[str, Any]:
    now_ts = time.time() if now_ts is None else now_ts
    session_id, error = _dispatch_scheduled_task(task)
    task["updated_at"] = time.time()
    if error:
        task["last_error"] = error
        return task
    task["last_run"] = _format_task_datetime(now_ts)
    task["last_session_id"] = session_id or ""
    task["last_error"] = ""
    if task.get("repeat") == "none":
        task["next_run"] = None
        task["status"] = "paused"
    else:
        try:
            task["next_run"] = _next_task_run(task, after_ts=now_ts, first=False)
        except ValueError as exc:
            task["next_run"] = None
            task["last_error"] = str(exc)
        if not task.get("next_run"):
            task["status"] = "paused"
    return task


def _debug_scheduled_task_record(task: dict[str, Any], now_ts: float | None = None) -> dict[str, Any]:
    now_ts = time.time() if now_ts is None else now_ts
    session_id, error = _dispatch_scheduled_task(task)
    task["updated_at"] = time.time()
    task["last_debug_run"] = _format_task_datetime(now_ts)
    if error:
        task["last_debug_error"] = error
        return task
    task["last_debug_session_id"] = session_id or ""
    task["last_debug_error"] = ""
    return task


def _run_due_scheduled_tasks(now_ts: float | None = None) -> None:
    now_ts = time.time() if now_ts is None else now_ts
    if not os.path.isdir(_WORKSPACE_ROOT):
        return
    for ws in sorted(os.listdir(_WORKSPACE_ROOT)):
        if not _is_workspace_name(ws):
            continue
        with _task_lock:
            tasks, error = _read_scheduled_tasks(ws)
            if error:
                continue
            changed = False
            for task in tasks:
                next_run = str(task.get("next_run") or "")
                if task.get("status") != "running" or not next_run:
                    continue
                try:
                    due_ts = datetime.strptime(next_run, "%Y-%m-%d %H:%M").timestamp()
                except ValueError:
                    task["last_error"] = "Invalid next_run"
                    task["status"] = "paused"
                    changed = True
                    continue
                if due_ts <= now_ts:
                    _run_scheduled_task_record(task, now_ts=now_ts)
                    changed = True
            if changed:
                _write_scheduled_tasks(ws, tasks)


def _task_scheduler_loop() -> None:
    while not _task_scheduler_stop.wait(30):
        try:
            _run_due_scheduled_tasks()
        except Exception:
            continue


@app.on_event("startup")
def _start_task_scheduler() -> None:
    global _task_scheduler_started
    if _task_scheduler_started:
        return
    _task_scheduler_started = True
    threading.Thread(target=_task_scheduler_loop, daemon=True).start()


@app.get("/api/tasks")
async def list_scheduled_tasks(ws: str = "default.ws"):
    ws = _normalize_path_input(ws) or "default.ws"
    with _task_lock:
        tasks, error = _read_scheduled_tasks(ws)
    if error:
        return {"success": False, "error": error}
    return {"success": True, "data": tasks}


@app.post("/api/tasks")
async def create_scheduled_task(request: ScheduledTaskWriteRequest):
    task, error = _normalize_scheduled_task(request)
    if error or task is None:
        return {"success": False, "error": error}
    ws = task["workspace"]
    with _task_lock:
        tasks, read_error = _read_scheduled_tasks(ws)
        if read_error:
            return {"success": False, "error": read_error}
        tasks.append(task)
        write_error = _write_scheduled_tasks(ws, tasks)
    if write_error:
        return {"success": False, "error": write_error}
    return {"success": True, "data": task}


@app.put("/api/tasks/{task_id}")
async def update_scheduled_task(task_id: str, request: ScheduledTaskWriteRequest):
    if not _valid_task_id(task_id):
        return {"success": False, "error": "Invalid task id"}
    ws = _normalize_path_input(request.ws) or "default.ws"
    with _task_lock:
        tasks, read_error = _read_scheduled_tasks(ws)
        if read_error:
            return {"success": False, "error": read_error}
        for index, existing in enumerate(tasks):
            if existing.get("id") != task_id:
                continue
            task, error = _normalize_scheduled_task(request, task_id=task_id, existing=existing)
            if error or task is None:
                return {"success": False, "error": error}
            tasks[index] = task
            write_error = _write_scheduled_tasks(ws, tasks)
            if write_error:
                return {"success": False, "error": write_error}
            return {"success": True, "data": task}
    return {"success": False, "error": "Task not found"}


@app.post("/api/tasks/{task_id}/status")
async def set_scheduled_task_status(task_id: str, request: ScheduledTaskStatusRequest):
    if not _valid_task_id(task_id):
        return {"success": False, "error": "Invalid task id"}
    ws = _normalize_path_input(request.ws) or "default.ws"
    status = _normalize_path_input(request.status)
    if status not in _TASK_STATUSES:
        return {"success": False, "error": "Invalid status"}
    with _task_lock:
        tasks, read_error = _read_scheduled_tasks(ws)
        if read_error:
            return {"success": False, "error": read_error}
        for task in tasks:
            if task.get("id") != task_id:
                continue
            task["status"] = status
            task["updated_at"] = time.time()
            if status == "running" and not task.get("next_run"):
                try:
                    task["next_run"] = _next_task_run(task, after_ts=time.time(), first=True)
                except ValueError as exc:
                    return {"success": False, "error": str(exc)}
            write_error = _write_scheduled_tasks(ws, tasks)
            if write_error:
                return {"success": False, "error": write_error}
            return {"success": True, "data": task}
    return {"success": False, "error": "Task not found"}


@app.delete("/api/tasks/{task_id}")
async def delete_scheduled_task(task_id: str, ws: str = "default.ws"):
    if not _valid_task_id(task_id):
        return {"success": False, "error": "Invalid task id"}
    ws = _normalize_path_input(ws) or "default.ws"
    with _task_lock:
        tasks, read_error = _read_scheduled_tasks(ws)
        if read_error:
            return {"success": False, "error": read_error}
        remaining = [task for task in tasks if task.get("id") != task_id]
        if len(remaining) == len(tasks):
            return {"success": False, "error": "Task not found"}
        write_error = _write_scheduled_tasks(ws, remaining)
    if write_error:
        return {"success": False, "error": write_error}
    return {"success": True}


@app.post("/api/tasks/{task_id}/run")
async def run_scheduled_task_now(task_id: str, request: ScheduledTaskRunRequest):
    if not _valid_task_id(task_id):
        return {"success": False, "error": "Invalid task id"}
    ws = _normalize_path_input(request.ws) or "default.ws"
    with _task_lock:
        tasks, read_error = _read_scheduled_tasks(ws)
        if read_error:
            return {"success": False, "error": read_error}
        for task in tasks:
            if task.get("id") != task_id:
                continue
            _debug_scheduled_task_record(task, now_ts=time.time())
            write_error = _write_scheduled_tasks(ws, tasks)
            if write_error:
                return {"success": False, "error": write_error}
            if task.get("last_debug_error"):
                return {"success": False, "error": task["last_debug_error"], "data": task}
            return {"success": True, "data": task}
    return {"success": False, "error": "Task not found"}


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


@app.get("/api/workspace/tree")
async def read_workspace_tree(ws: str = "default.ws"):
    """Return the workspace directory tree for the frontend editor."""
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    if not os.path.isdir(ws_root):
        return {"success": True, "data": {"name": ws, "path": ws, "type": "dir", "children": []}}
    try:
        return {"success": True, "data": _build_dir_tree(ws_root)}
    except OSError as exc:
        return {"success": False, "error": str(exc)}


@app.get("/api/workspace/index/stats")
async def read_workspace_index_stats(ws: str = "default.ws"):
    """Return file index status for a workspace."""
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    result = get_file_index_stats(cwd=ws_root)
    if result.get("status") != "OK":
        return {"success": False, "error": result.get("error", "failed to read index stats")}
    return {"success": True, "data": result}


@app.post("/api/workspace/index/refresh")
async def refresh_workspace_index(request: WorkspaceIndexRefreshRequest):
    """Refresh the file index for a workspace."""
    ws_root, error = _workspace_root(request.ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    result = refresh_file_index(
        root=request.root,
        cwd=ws_root,
        semantic=request.semantic,
        embedding_config=_file_index_embedding_config_from_path(request.config_path),
    )
    if result.get("status") != "OK":
        return {"success": False, "error": result.get("error", "failed to refresh index"), "data": result}
    return {"success": True, "data": result}


@app.get("/api/workspace/index/search")
async def search_workspace_index(
    ws: str = "default.ws",
    q: str = "",
    root: str = "",
    limit: int = 20,
    refresh: bool = False,
    path_only: bool = False,
    mode: str = "hybrid",
    config_path: str = "",
):
    """Search the workspace file index."""
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    result = search_file_index(
        query=q,
        cwd=ws_root,
        root=root,
        limit=limit,
        refresh=refresh,
        path_only=path_only,
        mode=mode,
        embedding_config=_file_index_embedding_config_from_path(config_path),
    )
    if result.get("status") != "OK":
        return {"success": False, "error": result.get("error", "failed to search index"), "data": result}
    return {"success": True, "data": result}


@app.get("/api/workspace/preview")
async def preview_workspace_file(ws: str = "default.ws", path: str = ""):
    """Read a workspace text file for read-only preview."""
    real_path, normalized_path, error = _resolve_workspace_preview_path(ws, path)
    if error or real_path is None or normalized_path is None:
        return {"success": False, "error": error}
    if os.path.islink(real_path):
        return {"success": False, "error": "Symlink preview is not supported"}
    if not os.path.exists(real_path):
        return {"success": False, "error": "File not found"}
    if os.path.isdir(real_path):
        return {"success": False, "error": "Path is a directory"}

    try:
        size_bytes = os.path.getsize(real_path)
        if size_bytes > 1_000_000:
            return {"success": False, "error": "File is too large to preview"}
        with open(real_path, "r", encoding="utf-8") as f:
            content = f.read()
        return {
            "success": True,
            "data": {
                "path": normalized_path,
                "content": content,
                "bytes": len(content.encode("utf-8")),
                "size_bytes": size_bytes,
                "mtime": os.path.getmtime(real_path),
                "read_only": True,
            },
        }
    except UnicodeDecodeError:
        return {"success": False, "error": "File is not valid UTF-8 text"}
    except OSError as exc:
        return {"success": False, "error": str(exc)}


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


# === Eval API ===


def _eval_agent_factory(
    *,
    ws: str,
    ws_root: str,
    agent_name: str,
    config_path: str,
    observability_config_path: str,
    runtime_config: dict[str, Any] | None,
):
    runtime_config = runtime_config or {}

    def _factory():
        agent = build_agent(
            config_path=config_path or None,
            observability_config_path=observability_config_path or None,
            workspace_dir=ws_root,
            agent_name=agent_name,
            agent_prompt=str(runtime_config.get("agent_prompt", "")),
            agent_soul=str(runtime_config.get("agent_soul", "")),
            tools_allowlist=runtime_config.get("tools_allowlist"),
            skill_allowlist=runtime_config.get("skill_allowlist"),
            model_override=str(runtime_config.get("model_override", "")),
            max_turns=runtime_config.get("max_turns"),
            memory_mode=str(runtime_config.get("memory_mode", "project")),
        )
        if hasattr(agent, "handler") and hasattr(agent.handler, "ctx"):
            agent.handler.ctx.verbose = True
        return agent

    del ws
    return _factory


def _run_eval_background(
    *,
    ws_root: str,
    run_id: str,
    cancel_event: threading.Event,
    agent_factory,
) -> None:
    try:
        execute_eval_run(
            ws_root,
            run_id,
            agent_factory=agent_factory,
            cancel_event=cancel_event,
        )
    finally:
        with _eval_lock:
            _eval_cancel_events.pop(run_id, None)


@app.get("/api/eval/datasets")
async def api_list_eval_datasets(ws: str = "default.ws"):
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    imported = [{**item, "imported": True} for item in list_datasets(ws_root)]
    workspace_datasets = list_workspace_eval_datasets(ws_root)
    workspace_by_path = {
        str(item.get("source", {}).get("path") or ""): item
        for item in workspace_datasets
    }
    fresh_imported_paths: set[str] = set()
    visible_imported: list[dict[str, Any]] = []
    for item in imported:
        source = item.get("source", {})
        source_path = str(source.get("path") or "") if isinstance(source, dict) else ""
        workspace_item = workspace_by_path.get(source_path)
        if source_path and workspace_item and float(item.get("created_at") or 0) < float(workspace_item.get("created_at") or 0):
            continue
        visible_imported.append(item)
        if source_path:
            fresh_imported_paths.add(source_path)

    visible_workspace_datasets = [
        item
        for item in workspace_datasets
        if str(item.get("source", {}).get("path") or "") not in fresh_imported_paths
    ]
    return {"success": True, "data": visible_imported + visible_workspace_datasets}


@app.post("/api/eval/datasets/import")
async def api_import_eval_dataset(request: EvalDatasetImportRequest):
    ws_root, error = _workspace_root(request.ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    try:
        if request.path.strip():
            data = import_dataset_path(
                ws_root,
                rel_path=request.path,
                name=request.name,
                fmt=request.format,
            )
        else:
            if not request.content:
                return {"success": False, "error": "content or path is required"}
            data = import_dataset_content(
                ws_root,
                name=request.name or "dataset",
                content=request.content,
                fmt=request.format,
                source={"type": "content"},
            )
        return {"success": True, "data": data}
    except (EvalError, OSError, UnicodeError) as exc:
        return {"success": False, "error": str(exc)}


@app.post("/api/eval/datasets/download")
async def api_download_eval_dataset(request: EvalDatasetDownloadRequest):
    ws_root, error = _workspace_root(request.ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    try:
        data = download_dataset(
            ws_root,
            url=request.url,
            name=request.name,
            fmt=request.format,
        )
        return {"success": True, "data": data}
    except (EvalError, OSError, UnicodeError) as exc:
        return {"success": False, "error": str(exc)}


@app.get("/api/eval/datasets/{dataset_id}")
async def api_get_eval_dataset(dataset_id: str, ws: str = "default.ws"):
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    try:
        return {"success": True, "data": get_dataset_detail(ws_root, dataset_id)}
    except (EvalError, OSError, json.JSONDecodeError) as exc:
        return {"success": False, "error": str(exc)}


@app.get("/api/eval/runs")
async def api_list_eval_runs(ws: str = "default.ws"):
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    return {"success": True, "data": list_eval_runs(ws_root)}


@app.post("/api/eval/runs")
async def api_create_eval_run(request: EvalRunCreateRequest):
    ws_root, error = _workspace_root(request.ws)
    if error or ws_root is None:
        return {"success": False, "error": error}

    agent_name = _normalize_path_input(request.agent)
    runtime_config, runtime_error = _agent_runtime_config(request.ws, agent_name)
    if runtime_error:
        return {"success": False, "error": runtime_error}

    case_limit = request.case_limit if request.case_limit > 0 else None
    if case_limit is not None and case_limit > 500:
        return {"success": False, "error": "case_limit cannot exceed 500"}

    try:
        result = create_eval_run(
            ws_root,
            workspace=request.ws,
            dataset_id=request.dataset_id,
            agent=agent_name,
            case_limit=case_limit,
        )
    except (EvalError, OSError, json.JSONDecodeError) as exc:
        return {"success": False, "error": str(exc)}

    cancel_event = threading.Event()
    with _eval_lock:
        _eval_cancel_events[result["id"]] = cancel_event

    agent_factory = _eval_agent_factory(
        ws=request.ws,
        ws_root=ws_root,
        agent_name=agent_name,
        config_path=_normalize_path_input(request.config_path),
        observability_config_path=_normalize_path_input(request.observability_config_path),
        runtime_config=runtime_config,
    )
    thread = threading.Thread(
        target=_run_eval_background,
        kwargs={
            "ws_root": ws_root,
            "run_id": result["id"],
            "cancel_event": cancel_event,
            "agent_factory": agent_factory,
        },
        daemon=True,
    )
    thread.start()
    return {"success": True, "data": result}


@app.get("/api/eval/runs/{run_id}")
async def api_get_eval_run(run_id: str, ws: str = "default.ws"):
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    try:
        return {"success": True, "data": read_eval_run(ws_root, run_id)}
    except (EvalError, OSError, json.JSONDecodeError) as exc:
        return {"success": False, "error": str(exc)}


@app.post("/api/eval/runs/{run_id}/cancel")
async def api_cancel_eval_run(run_id: str, ws: str = "default.ws"):
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    try:
        result = read_eval_run(ws_root, run_id)
    except (EvalError, OSError, json.JSONDecodeError) as exc:
        return {"success": False, "error": str(exc)}

    with _eval_lock:
        cancel_event = _eval_cancel_events.get(run_id)
        if cancel_event is not None:
            cancel_event.set()

    if result.get("status") in {"pending", "running"}:
        result["status"] = "canceling"
        write_eval_run(ws_root, result)
    return {"success": True, "data": result}


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
