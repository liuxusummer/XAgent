"""
XAgent Modern Web UI - FastAPI backend with SSE streaming
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import contextvars
import hashlib
import ipaddress
import json
import logging
import os
import queue
import re
import shutil
import threading
import time
import urllib.parse
import uuid
from contextlib import nullcontext
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from src.config import load_config
from src.core.XAgent import XAgent
from src.core.agent_kernel import Principal
from src.core.agent_profiles import (
    load_agent_runtime_config,
    parse_agent_markdown,
    read_agent_profile as load_agent_profile_document,
    serialize_agent_markdown,
)
from src.core.web_identity import (
    WebIdentity,
    WebIdentityConfigurationError,
    WebIdentityError,
    WebIdentityProvider,
    physical_directory_contains_path,
)
from src.core.checkpoint import load_task_checkpoint
from src.core.memory_store import MEMORY_READ_SCOPE
from src.core.agent_teams import (
    delete_team_config,
    list_team_configs,
    read_team_config,
    write_team_config,
)
from src.core.team_workflows import read_team_workflow, run_team_workflow, write_team_workflow
from src.core.telemetry import Event, JsonlSink, MultiSink, NullSink
from src.core.workspace_storage import atomic_write_json, atomic_write_text, workspace_write_lock
from src.core.workspace_templates import (
    create_workspace_from_template,
    list_workspace_templates,
)
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
from src.core.eval_scenarios import (
    SCENARIO_PACK_V1_SCOPES,
    SCENARIO_PACK_V1_TOOLS,
    EvalCaseRuntime,
)
from src.main import build_agent, build_team_step_runner, load_observability_config
from src.tools.file_index import get_file_index_stats, refresh_file_index, search_file_index
from src.tools.reflect.reader import group_by_session, load_dir, load_events
from src.tools.reflect.stats import TOKEN_FIELDS


_DEFAULT_WEB_ALLOWED_ORIGINS = {
    "http://127.0.0.1:5173",
    "http://localhost:5173",
}
_DEFAULT_WEB_ALLOWED_HOSTS = {
    "127.0.0.1",
    "::1",
    "localhost",
}


def _workspace_index_principal(
    ws: str,
    identity: WebIdentity | None = None,
) -> Principal:
    """Bind index access to the authenticated Web caller without scope growth."""

    caller = identity or _current_web_identity()
    return caller.principal(
        session_id=f"web-index-{ws}",
        agent_id="main",
    )


def _normalize_web_origin(value: str) -> str | None:
    try:
        parsed = urllib.parse.urlsplit(str(value or "").strip())
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            return None
        port = parsed.port
    except ValueError:
        return None
    if parsed.username or parsed.password or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        return None
    host = parsed.hostname.lower()
    authority = f"[{host}]" if ":" in host else host
    default_port = 80 if parsed.scheme == "http" else 443
    if port is not None and port != default_port:
        authority = f"{authority}:{port}"
    return f"{parsed.scheme}://{authority}"


def _configured_web_allowed_origins() -> list[str]:
    origins = set(_DEFAULT_WEB_ALLOWED_ORIGINS)
    for value in os.environ.get("XAGENT_WEB_ALLOWED_ORIGINS", "").split(","):
        normalized = _normalize_web_origin(value)
        if normalized is not None:
            origins.add(normalized)
    return sorted(origins)


def _normalize_web_host(value: str) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = urllib.parse.urlsplit(f"http://{raw}")
    except ValueError:
        return None
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        return None
    return parsed.hostname.split("%", 1)[0].lower()


def _configured_web_allowed_hosts() -> list[str]:
    hosts = set(_DEFAULT_WEB_ALLOWED_HOSTS)
    for value in os.environ.get("XAGENT_WEB_ALLOWED_HOSTS", "").split(","):
        normalized = _normalize_web_host(value)
        if normalized is not None:
            hosts.add(normalized)
    return sorted(hosts)


_WEB_ALLOWED_ORIGINS = frozenset(_configured_web_allowed_origins())
_WEB_ALLOWED_HOSTS = frozenset(_configured_web_allowed_hosts())

app = FastAPI(title="XAgent Web UI")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=sorted(_WEB_ALLOWED_ORIGINS),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Accept", "Authorization", "Content-Type"],
)

logger = logging.getLogger(__name__)
_WEB_IDENTITY_CONTEXT: contextvars.ContextVar[WebIdentity | None] = (
    contextvars.ContextVar("xagent_web_identity", default=None)
)
_web_identity_provider: WebIdentityProvider | None = None
_web_identity_provider_signature = ""
_web_identity_provider_lock = threading.Lock()


def _get_web_identity_provider() -> WebIdentityProvider:
    global _web_identity_provider, _web_identity_provider_signature
    with _web_identity_provider_lock:
        registry_path = os.environ.get("XAGENT_WEB_IDENTITY_REGISTRY", "").strip()
        if registry_path:
            try:
                registry_stat = os.stat(registry_path)
                signature = (
                    f"secure:{os.path.realpath(registry_path)}:"
                    f"{registry_stat.st_dev}:{registry_stat.st_ino}:"
                    f"{registry_stat.st_size}:{registry_stat.st_mtime_ns}"
                )
            except OSError:
                signature = f"secure-unavailable:{os.path.realpath(registry_path)}"
        else:
            signature = f"local:{os.path.realpath(_WORKSPACE_ROOT)}"
        if (
            _web_identity_provider is not None
            and _web_identity_provider_signature == signature
        ):
            return _web_identity_provider
        if registry_path:
            registry_real = os.path.realpath(registry_path)
            workspace_real = os.path.realpath(_WORKSPACE_ROOT)
            try:
                registry_in_workspace = (
                    os.path.commonpath([workspace_real, registry_real])
                    == workspace_real
                )
            except ValueError:
                registry_in_workspace = False
            if registry_in_workspace:
                raise WebIdentityConfigurationError(
                    "Web identity registry must be outside Agent workspaces"
                )
            provider = WebIdentityProvider.from_registry_file(registry_path)
        else:
            provider = WebIdentityProvider.local(_WORKSPACE_ROOT)
        _web_identity_provider = provider
        _web_identity_provider_signature = signature
        return provider


def _current_web_identity() -> WebIdentity:
    identity = _WEB_IDENTITY_CONTEXT.get()
    if isinstance(identity, WebIdentity):
        return identity
    provider = _get_web_identity_provider()
    if provider.is_local:
        return provider.authenticate()
    raise WebIdentityError("authentication context is missing")


def _web_error_message(
    exc: BaseException,
    fallback: str = "Operation failed",
    *,
    prefix_local: bool = False,
) -> str:
    """Keep host paths and backend details out of secure Web responses."""

    try:
        if _get_web_identity_provider().is_local:
            detail = str(exc)
            if prefix_local and detail:
                return f"{fallback}: {detail}"
            return detail or fallback
    except WebIdentityConfigurationError:
        pass
    return fallback


def _request_web_identity(request: Request) -> WebIdentity:
    state = getattr(request, "state", None)
    identity = getattr(state, "web_identity", None)
    if not isinstance(identity, WebIdentity):
        provider = _get_web_identity_provider()
        if provider.is_local:
            return _current_web_identity()
        raise WebIdentityError("authentication context is missing")
    return identity


def _opaque_web_credential(request: Request) -> str | None:
    authorization = request.headers.get("authorization", "")
    if authorization:
        scheme, _, value = authorization.partition(" ")
        if scheme.lower() == "bearer" and value.strip():
            return value.strip()
    cookie = request.cookies.get("xagent_session")
    return cookie if cookie else None


def _required_web_scopes(request: Request) -> tuple[str, ...]:
    path = request.url.path
    method = request.method.upper()
    if path == "/api/auth/session" or method == "OPTIONS":
        return ()
    if path.startswith(("/api/usage/", "/api/trace/")):
        return ("workspace.read",)
    if path == "/api/chat" or path.startswith("/api/chat/"):
        return ("workspace.read",) if method == "GET" else ("user.interact",)
    if path.startswith("/api/chats"):
        if method == "GET":
            return ("workspace.read",)
        if method == "DELETE":
            return ("state.write",)
        return ("state.write",)
    if path.startswith("/api/tasks"):
        return ("workspace.read",) if method == "GET" else ("state.write",)
    if path.startswith("/api/eval"):
        return ("workspace.read",) if method == "GET" else ("state.write",)
    if path.startswith("/api/workspace/index/refresh"):
        return ("workspace.read", "state.write")
    if path.startswith("/api/workspace"):
        if method == "GET":
            return ("workspace.read",)
        if method == "DELETE":
            return ("workspace.delete",)
        return ("workspace.write",)
    return ("workspace.read",)


def _is_loopback_client(host: str | None) -> bool:
    if not host:
        return False
    normalized = host.split("%", 1)[0].strip()
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return normalized.lower() == "localhost"
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return address.is_loopback


def _request_origin(request: Request) -> str | None:
    host = request.headers.get("host", "")
    if not host:
        return None
    return _normalize_web_origin(f"{request.url.scheme}://{host}")


def _is_allowed_web_host(request: Request) -> bool:
    normalized = _normalize_web_host(request.headers.get("host", ""))
    return normalized is not None and normalized in _WEB_ALLOWED_HOSTS


def _is_allowed_web_origin(request: Request, origin: str) -> bool:
    normalized = _normalize_web_origin(origin)
    if normalized is None:
        return False
    return normalized in _WEB_ALLOWED_ORIGINS or (
        _is_allowed_web_host(request) and normalized == _request_origin(request)
    )


@app.middleware("http")
async def enforce_local_web_access(request: Request, call_next):
    client_host = request.client.host if request.client is not None else None
    if not _is_loopback_client(client_host):
        return JSONResponse(
            status_code=403,
            content={"success": False, "error": "Web UI only accepts local connections"},
        )
    if not _is_allowed_web_host(request):
        return JSONResponse(
            status_code=403,
            content={"success": False, "error": "Host is not allowed"},
        )
    origin = request.headers.get("origin")
    if origin and not _is_allowed_web_origin(request, origin):
        return JSONResponse(
            status_code=403,
            content={"success": False, "error": "Origin is not allowed"},
        )
    if request.method.upper() == "OPTIONS":
        return await call_next(request)
    try:
        provider = _get_web_identity_provider()
        await _run_blocking(_revoke_invalid_web_activity, provider)
        identity = provider.authenticate(_opaque_web_credential(request))
    except WebIdentityConfigurationError:
        logger.exception("Web identity provider configuration is invalid")
        # A registry that disappears, becomes corrupt, or contains no enabled
        # identities invalidates every previously trusted secure owner.  There
        # is no provider from which a safe subset can be reconstructed, so
        # active work must be stopped before returning the outage response.
        await _run_blocking(_revoke_all_secure_web_activity)
        return JSONResponse(
            status_code=503,
            content={"success": False, "error": "Web authentication is unavailable"},
        )
    except WebIdentityError:
        return JSONResponse(
            status_code=401,
            content={"success": False, "error": "Authentication required"},
        )
    request.state.web_identity = identity
    request.state.web_credential = _opaque_web_credential(request)
    if any(scope not in identity.scopes for scope in _required_web_scopes(request)):
        return JSONResponse(
            status_code=403,
            content={"success": False, "error": "Access denied"},
        )
    context_token = _WEB_IDENTITY_CONTEXT.set(identity)
    try:
        return await call_next(request)
    finally:
        _WEB_IDENTITY_CONTEXT.reset(context_token)


@app.post("/api/auth/session")
async def create_web_auth_session(
    request: Request,
    response: Response,
):
    """Exchange an authenticated bearer for an HttpOnly cookie used by SSE."""

    provider = _get_web_identity_provider()
    if provider.is_local:
        return {"success": True, "data": {"mode": "local"}}
    credential = getattr(request.state, "web_credential", None)
    if not isinstance(credential, str) or not credential:
        return {"success": False, "error": "Bearer credential is required"}
    response.set_cookie(
        "xagent_session",
        credential,
        httponly=True,
        secure=os.environ.get("XAGENT_WEB_COOKIE_SECURE", "1").strip() != "0",
        samesite="strict",
        path="/",
        max_age=8 * 60 * 60,
    )
    return {"success": True, "data": {"mode": "secure"}}


class SubmitTaskRequest(BaseModel):
    task: str = ""
    session_id: str = ""
    chat_id: str = ""
    config_path: str = ""
    observability_config_path: str = ""
    workspace_dir: str = ""
    agent: str = ""
    team: str = ""
    resume: bool = False


class ReplyRequest(BaseModel):
    reply: str
    session_id: str = ""


class WorkspaceFileWriteRequest(BaseModel):
    ws: str = "default.ws"
    path: str = ""
    content: str = ""


class WorkspaceCreateRequest(BaseModel):
    name: str = ""
    template_id: str = "blank"


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


class TeamWriteRequest(BaseModel):
    ws: str = "default.ws"
    team: dict[str, Any] = {}


class TeamDeleteRequest(BaseModel):
    ws: str = "default.ws"
    team: str = ""


class TeamWorkflowWriteRequest(BaseModel):
    ws: str = "default.ws"
    team: str = ""
    workflow: dict[str, Any] = {}


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
    owner_digest: str = ""
    web_identity: WebIdentity | None = field(default=None, repr=False)
    agent: object | None = None
    events: list[dict[str, Any]] = field(default_factory=list)
    event_sequence: int = 0
    waiting_for_user: bool = False
    interrupted: bool = False
    resume_available: bool = False
    ask_prompt: str = ""
    starting: bool = False
    task_start_token: str = ""
    running: bool = False
    config_path: str = ""
    observability_config_path: str = ""
    workspace_dir: str = ""
    agent_name: str = ""
    team_name: str = ""
    team_config: dict[str, Any] | None = None
    team_workflow: dict[str, Any] | None = None
    team_step_runner: Any | None = None
    runtime_config_key: str = ""
    session_id: str = ""
    checkpoint_id: str = ""
    chat_id: str = ""
    chat_ws: str = ""
    chat_agent: str = ""
    messages: list[dict[str, Any]] = field(default_factory=list)
    restored_llm_history: list[dict[str, Any]] = field(default_factory=list)
    event_queue: queue.Queue[dict[str, Any]] = field(default_factory=lambda: queue.Queue(maxsize=1))
    llm_stream_buffer: str = ""
    llm_parse_buffer: str = ""
    llm_parse_mode: str = "visible"
    llm_hidden_leading_whitespace: bool = False
    llm_tool_payload: str = ""
    llm_tool_payload_truncated: bool = False
    parsed_tool_use_count: int = 0
    pending_tool_names: list[str] = field(default_factory=list)
    emitted_tool_keys: set[str] = field(default_factory=set)
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    agent_init_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    persist_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    last_activity_at: float = field(default_factory=time.time)
    starting_started_at: float = 0.0
    running_started_at: float = 0.0
    waiting_started_at: float = 0.0
    stop_requested: bool = False
    closing: bool = False
    persist_revision: int = 0
    persisted_revision: int = 0
    last_persist_at: float = 0.0


# Global session storage
_sessions: dict[str, UISession] = {}
_session_lock = threading.Lock()
_eval_cancel_events: dict[tuple[str, str], threading.Event] = {}
_eval_lock = threading.Lock()

_TURN_RE = re.compile(r"^\[Turn (\d+)\]$")
_TOOL_RE = re.compile(r"^\s*tool:\s*(.+)$")
_DONE_RE = re.compile(r"^\[Done\]\s+exit_reason=([^,]+),\s*turns=(\d+)")

_LLM_TAG_NAME_RE = re.compile(r"^\s*(/?)\s*([A-Za-z][A-Za-z0-9_:-]*)")
_LLM_HIDDEN_TAGS = {
    "summary",
    "thinking",
    "tool_use",
    "tool_result",
    "history",
    "key_info",
    "earlier_context",
}
_TRACE_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_CHAT_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_TASK_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_TASK_TIME_RE = re.compile(r"^\d{2}:\d{2}$")
_TASK_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TASK_REPEATS = {"none", "daily", "weekly", "custom"}
_TASK_STATUSES = {"running", "paused"}
_PERSISTED_SESSION_EVENT_TYPES = {
    "user_task",
    "user_reply",
    "assistant_delta",
    "thinking_delta",
    "turn_start",
    "tool_call",
    "ask_user",
    "done",
    "error",
    "stop",
}
_task_scheduler_started = False
_task_scheduler_stop = threading.Event()
_task_scheduler_thread: threading.Thread | None = None
_web_runtime_stopping = threading.Event()
_CHAT_PERSIST_INTERVAL_SECONDS = 1.0
_SESSION_IDLE_TTL_SECONDS = 60 * 60
_SESSION_RUNNING_TTL_SECONDS = 6 * 60 * 60
_SESSION_WAITING_TTL_SECONDS = 24 * 60 * 60
_SESSION_MAX_COUNT = 256
_SESSION_EVENT_MAX_COUNT = 2048
_LLM_STREAM_BUFFER_MAX_CHARS = 64 * 1024
_LLM_STREAM_TAG_MAX_CHARS = 4096
_LLM_TOOL_PAYLOAD_MAX_CHARS = 64 * 1024
_SECURE_HOST_PATH_RE = re.compile(
    r"(?<![:\w])/(?:Users|private|var|tmp|home|root|opt|srv|etc|mnt|host)"
    r"(?:/[^\s\"'<>]+)+"
)
_SESSION_CAPACITY_ERROR = "Session capacity reached; stop or close an existing session and retry"
_SESSION_BUSY_ERROR = "Session is busy with another task"
_SESSION_CLOSED_ERROR = "Session is closed"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _new_daemon_thread(*args, **kwargs) -> threading.Thread:
    kwargs["daemon"] = True
    return threading.Thread(*args, **kwargs)


def _consume_background_task_result(task: asyncio.Task[Any]) -> None:
    try:
        task.result()
    except (asyncio.CancelledError, Exception):
        pass


async def _run_blocking(func: Any, /, *args, **kwargs):
    """Run synchronous work off-loop; cancellation does not abandon its result."""
    worker = asyncio.create_task(asyncio.to_thread(func, *args, **kwargs))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        worker.add_done_callback(_consume_background_task_result)
        raise


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


def _secure_trace_payload(value: Any) -> Any:
    if _get_web_identity_provider().is_local:
        return value
    if isinstance(value, dict):
        return {
            key: (
                "Operation failed"
                if str(key).casefold()
                in {
                    "detail",
                    "error",
                    "exception",
                    "stack",
                    "traceback",
                }
                else "<redacted-path>"
                if key
                in {
                    "path",
                    "dataset_path",
                    "download_path",
                    "log_path",
                    "index_path",
                    "latest_path",
                }
                else _secure_trace_payload(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_secure_trace_payload(item) for item in value]
    if isinstance(value, str):
        if os.path.isabs(value):
            return "<redacted-path>"
        return _SECURE_HOST_PATH_RE.sub("<redacted-path>", value)
    return value


def _secure_web_result(value: Any) -> Any:
    if _get_web_identity_provider().is_local or not isinstance(value, dict):
        return value
    result = _secure_trace_payload(value)
    if not isinstance(result, dict):
        return result
    response = str(result.get("response") or "")
    if (
        str(result.get("exit_reason") or "") == "ERROR"
        or response.lstrip().startswith("[error]")
    ):
        result["response"] = "[error] Agent task failed"
    return result


def _secure_eval_run_payload(value: Any) -> Any:
    if _get_web_identity_provider().is_local:
        return value
    result = _secure_trace_payload(value)
    runs = result if isinstance(result, list) else [result]
    for run in runs:
        if not isinstance(run, dict):
            continue
        cases = run.get("cases")
        if not isinstance(cases, list):
            continue
        for case in cases:
            if not isinstance(case, dict):
                continue
            if (
                str(case.get("status") or "").casefold() == "error"
                or str(case.get("exit_reason") or "").upper() == "ERROR"
            ):
                case["failures"] = ["Operation failed"]
                case["response_excerpt"] = "[error] Agent task failed"
    return result


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
    provider = _get_web_identity_provider()
    if provider.is_local:
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
    if provider.is_local:
        return os.path.join(_WORKSPACE_ROOT, ws, "runtime", "traces")
    workspace_root, error = _workspace_root(ws)
    if error or workspace_root is None:
        return ""
    trace_root = os.path.join(workspace_root, "runtime", "traces")
    identity = _current_web_identity()
    if not provider.is_local:
        trace_root = os.path.join(trace_root, "_owners", identity.owner_digest)
    return trace_root


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


class WebIdentityStampingSink:
    """Stamp Web trace records with non-secret authorization boundaries."""

    def __init__(self, session: UISession, sink: Any) -> None:
        self.session = session
        self.sink = sink

    def emit(self, event: Event) -> None:
        with self.session.lock:
            identity = self.session.web_identity
            agent_name = self.session.agent_name or "main"
        if not isinstance(identity, WebIdentity):
            return
        principal = identity.principal(
            session_id=event.session_id or self.session.session_id,
            agent_id=agent_name,
        )
        stamped = Event(
            session_id=event.session_id,
            turn=event.turn,
            kind=event.kind,
            name=event.name,
            ts=event.ts,
            duration_ms=event.duration_ms,
            data={
                **event.data,
                "web_owner_digest": identity.owner_digest,
                "principal_boundary_digest": principal.boundary_digest,
                "tenant_digest": hashlib.sha256(
                    identity.tenant_id.encode("utf-8")
                ).hexdigest(),
            },
        )
        self.sink.emit(stamped)

    def close(self) -> None:
        self.sink.close()


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
    if not delta:
        return current
    return current + delta


def _tool_status_from_data(data: Any) -> str:
    if not isinstance(data, dict):
        return "success"
    status = str(data.get("status", "")).upper()
    if status == "ERROR" or data.get("error"):
        return "error"
    return "success"


def _merge_tool_call_results(tool_calls: list[Any], fallback_tools: list[dict[str, Any]]) -> list[Any]:
    for fallback in fallback_tools:
        exact_match = None
        pending_name_match = None
        any_name_match = None
        for existing in tool_calls:
            if not isinstance(existing, dict):
                continue
            if existing.get("id") == fallback["id"]:
                exact_match = existing
                break
            if existing.get("name") == fallback["name"]:
                if existing.get("status") in {"pending", "running"} and existing.get("result") is None:
                    pending_name_match = pending_name_match or existing
                any_name_match = any_name_match or existing
        target = exact_match or pending_name_match or any_name_match
        if target is not None:
            target.update(
                {
                    "status": fallback["status"],
                    "result": fallback["result"],
                }
            )
        else:
            tool_calls.append(fallback)
    return tool_calls


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
        if isinstance(payload.get("team_workflow"), dict):
            message["metadata"]["teamWorkflow"] = payload["team_workflow"]
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
                    "status": _tool_status_from_data(item.get("data") if "data" in item else item),
                    "result": item.get("data") if "data" in item else item,
                }
            )
        if fallback_tools:
            tool_calls = message.get("toolCalls")
            if not isinstance(tool_calls, list):
                tool_calls = []
            message["toolCalls"] = _merge_tool_call_results(tool_calls, fallback_tools)
        return
    if event_type in {"error", "stop"}:
        session.messages.append(_new_ui_message("system", str(data or ""), status="error" if event_type == "error" else "complete"))


def _emit_tool_call(session: UISession, name: str, arguments: dict[str, Any], tool_id: str | None = None) -> None:
    key = _tool_key(name, arguments)
    with session.lock:
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
                "status": "running",
            },
        )


def _emit_completed_tool_use(session: UISession) -> None:
    raw_payload = session.llm_tool_payload.strip()
    if session.llm_tool_payload_truncated:
        payload: dict[str, Any] = {
            "name": "bad_json",
            "arguments": {
                "error": f"tool payload exceeded {_LLM_TOOL_PAYLOAD_MAX_CHARS} characters",
                "raw_prefix": raw_payload[:1024],
            },
        }
    else:
        try:
            parsed = json.loads(raw_payload)
            payload = parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            payload = {"name": "bad_json", "arguments": {"raw": raw_payload}}

    name = str(payload.get("name", "")).strip()
    if name:
        arguments = payload.get("arguments")
        if not isinstance(arguments, dict):
            arguments = {}
        tool_id = str(payload.get("id") or uuid.uuid4().hex[:8])
        session.pending_tool_names.append(name)
        _emit_tool_call(session, name, arguments, tool_id)
    session.parsed_tool_use_count += 1
    session.llm_tool_payload = ""
    session.llm_tool_payload_truncated = False


def _consume_llm_stream_text(session: UISession, text: str) -> None:
    if not text:
        return
    if session.llm_hidden_leading_whitespace:
        text = text.lstrip()
        if not text:
            return
        session.llm_hidden_leading_whitespace = False
    if session.llm_parse_mode == "visible":
        _emit(session, "assistant_delta", text)
    elif session.llm_parse_mode == "thinking":
        _emit(session, "thinking_delta", text)
    elif session.llm_parse_mode == "tool_use":
        remaining = _LLM_TOOL_PAYLOAD_MAX_CHARS - len(session.llm_tool_payload)
        if remaining > 0:
            session.llm_tool_payload += text[:remaining]
        if len(text) > remaining:
            session.llm_tool_payload_truncated = True


def _parse_llm_stream_tag(raw_tag: str) -> tuple[bool, str] | None:
    match = _LLM_TAG_NAME_RE.match(raw_tag[1:-1])
    if not match:
        return None
    return bool(match.group(1)), match.group(2).lower()


def _process_llm_stream(session: UISession) -> None:
    while session.llm_parse_buffer:
        tag_start = session.llm_parse_buffer.find("<")
        if tag_start < 0:
            _consume_llm_stream_text(session, session.llm_parse_buffer)
            session.llm_parse_buffer = ""
            return
        if tag_start:
            _consume_llm_stream_text(session, session.llm_parse_buffer[:tag_start])
            session.llm_parse_buffer = session.llm_parse_buffer[tag_start:]

        tag_end = session.llm_parse_buffer.find(">")
        if tag_end < 0:
            if len(session.llm_parse_buffer) <= _LLM_STREAM_TAG_MAX_CHARS:
                return
            _consume_llm_stream_text(session, session.llm_parse_buffer[0])
            session.llm_parse_buffer = session.llm_parse_buffer[1:]
            continue

        raw_tag = session.llm_parse_buffer[: tag_end + 1]
        session.llm_parse_buffer = session.llm_parse_buffer[tag_end + 1 :]
        parsed_tag = _parse_llm_stream_tag(raw_tag)
        if parsed_tag is None:
            _consume_llm_stream_text(session, raw_tag)
            continue
        closing, tag_name = parsed_tag

        if session.llm_parse_mode == "visible":
            if not closing and tag_name in _LLM_HIDDEN_TAGS:
                session.llm_parse_mode = tag_name
                session.llm_hidden_leading_whitespace = tag_name in {"thinking", "tool_use"}
                if tag_name == "tool_use":
                    session.llm_tool_payload = ""
                    session.llm_tool_payload_truncated = False
                continue
            _consume_llm_stream_text(session, raw_tag)
            continue

        if closing and tag_name == session.llm_parse_mode:
            if session.llm_parse_mode == "tool_use":
                _emit_completed_tool_use(session)
            session.llm_parse_mode = "visible"
            session.llm_hidden_leading_whitespace = False
            continue
        _consume_llm_stream_text(session, raw_tag)


def _append_llm_delta(session: UISession, delta: str) -> None:
    if not delta:
        return
    with session.lock:
        if len(delta) >= _LLM_STREAM_BUFFER_MAX_CHARS:
            session.llm_stream_buffer = delta[-_LLM_STREAM_BUFFER_MAX_CHARS:]
        else:
            session.llm_stream_buffer = (
                session.llm_stream_buffer + delta
            )[-_LLM_STREAM_BUFFER_MAX_CHARS:]
        session.llm_parse_buffer += delta
        _process_llm_stream(session)


def _emit(session: UISession, event_type: str, data: Any = None) -> None:
    with session.lock:
        if session.closing:
            return
        session.event_sequence += 1
        session.events.append({"id": session.event_sequence, "type": event_type, "data": data})
        overflow = len(session.events) - _SESSION_EVENT_MAX_COUNT
        if overflow > 0:
            del session.events[:overflow]
        _update_session_messages(session, event_type, data)
        if event_type in _PERSISTED_SESSION_EVENT_TYPES:
            session.persist_revision += 1
        session.last_activity_at = time.time()


def _append_progress(session: UISession, message: str) -> None:
    if message.startswith("[Team Workflow]"):
        _emit(session, "thinking_delta", f"{message}\n")
        return
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


def _stop_agent(session: UISession, *, unblock_reply: bool = False) -> None:
    with session.lock:
        agent = session.agent
    if agent is not None:
        agent.stop()
        if unblock_reply:
            reply_queue = getattr(agent, "reply_queue", None)
            if reply_queue is not None:
                reply_queue.put("")


def _close_agent(session: UISession) -> None:
    with session.agent_init_lock:
        with session.lock:
            agent = session.agent
            session.agent = None
    if agent is not None:
        agent.close()


def _normalize_path_input(value: str | None) -> str:
    return value.strip() if isinstance(value, str) else ""


def _effective_web_config_paths(
    config_path: str,
    observability_config_path: str,
) -> tuple[str, str, str | None]:
    requested_config = _normalize_path_input(config_path)
    requested_observability = _normalize_path_input(observability_config_path)
    if _get_web_identity_provider().is_local:
        return requested_config, requested_observability, None
    if requested_config or requested_observability:
        return "", "", "Client-selected configuration paths are not allowed"
    configured = os.environ.get("XAGENT_WEB_CONFIG_PATH", "").strip()
    if not configured:
        return "", "", None
    try:
        resolved = os.path.realpath(configured)
        if not os.path.isfile(resolved):
            raise OSError
    except OSError:
        return "", "", "Trusted Web configuration is unavailable"

    provider = _get_web_identity_provider()
    workspace_roots = {os.path.realpath(_WORKSPACE_ROOT)}
    for identity in provider.identities():
        workspace_roots.update(
            os.path.realpath(path)
            for _alias, path in identity.workspace_aliases
        )
    for workspace_root in workspace_roots:
        try:
            inside_workspace = physical_directory_contains_path(
                Path(workspace_root),
                Path(resolved),
            )
        except OSError:
            return "", "", "Trusted Web configuration is unavailable"
        if inside_workspace:
            return "", "", "Trusted Web configuration is unavailable"
    return resolved, "", None


def _resolve_workspace_dir_input(
    value: str,
    identity: WebIdentity | None = None,
) -> str:
    normalized = _normalize_path_input(value)
    if not normalized:
        return ""
    if _is_workspace_name(normalized):
        if _get_web_identity_provider().is_local:
            return os.path.join(_WORKSPACE_ROOT, normalized)
        workspace_root, error = _workspace_root(normalized, identity=identity)
        return workspace_root if error is None and workspace_root is not None else ""
    if _get_web_identity_provider().is_local:
        return normalized
    return ""


def _workspace_skills_dir_input(
    value: str,
    identity: WebIdentity | None = None,
) -> str | None:
    workspace_root = _resolve_workspace_dir_input(value, identity=identity)
    if not workspace_root:
        return None
    skills_dir = os.path.join(workspace_root, "system", "skills")
    return skills_dir if os.path.isdir(skills_dir) else None


def _valid_chat_id(chat_id: str) -> bool:
    return bool(chat_id) and _CHAT_ID_RE.fullmatch(chat_id) is not None and chat_id not in {".", ".."}


def _chat_root(
    ws: str,
    agent: str,
    identity: WebIdentity | None = None,
) -> tuple[str | None, str | None]:
    if not _is_workspace_name(ws):
        return None, "Invalid workspace name"
    if not _is_child_name(agent):
        return None, "Invalid agent name"
    caller = identity or _current_web_identity()
    if _get_web_identity_provider().is_local:
        ws_root = os.path.realpath(os.path.join(_WORKSPACE_ROOT, ws))
        workspace_parent = os.path.realpath(_WORKSPACE_ROOT)
        if os.path.commonpath([workspace_parent, ws_root]) != workspace_parent:
            return None, "Path traversal not allowed"
    else:
        ws_root, workspace_error = _workspace_root(ws, identity=caller)
        if workspace_error or ws_root is None:
            return None, workspace_error
    chat_root = os.path.join(ws_root, "runtime", "chats")
    if not _get_web_identity_provider().is_local:
        chat_root = os.path.join(chat_root, "_owners", caller.owner_digest)
    return os.path.join(chat_root, agent), None


def _chat_dir(
    ws: str,
    agent: str,
    chat_id: str,
    identity: WebIdentity | None = None,
) -> tuple[str | None, str | None]:
    if not _valid_chat_id(chat_id):
        return None, "Invalid chat id"
    root, error = _chat_root(ws, agent, identity=identity)
    if error or root is None:
        return None, error
    real_root = os.path.realpath(root)
    chat_path = os.path.realpath(os.path.join(real_root, chat_id))
    if os.path.commonpath([real_root, chat_path]) != real_root:
        return None, "Path traversal not allowed"
    return chat_path, None


def _chat_metadata_path(
    ws: str,
    agent: str,
    chat_id: str,
    identity: WebIdentity | None = None,
) -> tuple[str | None, str | None]:
    chat_path, error = _chat_dir(ws, agent, chat_id, identity=identity)
    if error or chat_path is None:
        return None, error
    return os.path.join(chat_path, "metadata.json"), None


def _chat_state_path(
    ws: str,
    agent: str,
    chat_id: str,
    identity: WebIdentity | None = None,
) -> tuple[str | None, str | None]:
    chat_path, error = _chat_dir(ws, agent, chat_id, identity=identity)
    if error or chat_path is None:
        return None, error
    return os.path.join(chat_path, "state.json"), None


def _default_chat_metadata(
    ws: str,
    agent: str,
    chat_id: str,
    identity: WebIdentity | None = None,
) -> dict[str, Any]:
    caller = identity or _current_web_identity()
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
        "owner_digest": caller.owner_digest,
    }


def _default_chat_state(
    ws: str,
    agent: str,
    chat_id: str,
    session_id: str = "",
    identity: WebIdentity | None = None,
) -> dict[str, Any]:
    caller = identity or _current_web_identity()
    return {
        "chat_id": chat_id,
        "workspace": ws,
        "agent": agent,
        "backend_session_id": session_id,
        "checkpoint_id": "",
        "resume_available": False,
        "event_cursor": 0,
        "runtime_config_key": "",
        "messages": [],
        "llm_history": [],
        "waiting_for_user": False,
        "ask_prompt": "",
        "status": "idle",
        "updated_at": time.time(),
        "owner_digest": caller.owner_digest,
    }


def _chat_payload_owned_by(
    payload: dict[str, Any],
    identity: WebIdentity,
) -> bool:
    stored = str(payload.get("owner_digest") or "")
    if stored:
        return stored == identity.owner_digest
    return _get_web_identity_provider().is_local


def _read_chat_metadata(
    ws: str,
    agent: str,
    chat_id: str,
    identity: WebIdentity | None = None,
) -> dict[str, Any] | None:
    caller = identity or _current_web_identity()
    path, error = _chat_metadata_path(ws, agent, chat_id, identity=caller)
    if error or path is None or not os.path.isfile(path):
        return None
    try:
        payload = _read_json_file(path)
    except (OSError, json.JSONDecodeError):
        return None
    return payload if _chat_payload_owned_by(payload, caller) else None


def _read_chat_state(
    ws: str,
    agent: str,
    chat_id: str,
    identity: WebIdentity | None = None,
) -> dict[str, Any] | None:
    caller = identity or _current_web_identity()
    path, error = _chat_state_path(ws, agent, chat_id, identity=caller)
    if error or path is None or not os.path.isfile(path):
        return None
    try:
        payload = _read_json_file(path)
    except (OSError, json.JSONDecodeError):
        return None
    return payload if _chat_payload_owned_by(payload, caller) else None


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


def _set_session_runtime_state(session: UISession, state: str, *, now: float | None = None) -> None:
    current_time = time.time() if now is None else now
    with session.lock:
        session.starting = False
        session.task_start_token = ""
        session.starting_started_at = 0.0
        session.running = state == "running"
        session.waiting_for_user = state == "waiting"
        session.interrupted = state == "interrupted"
        session.resume_available = state == "interrupted" and bool(session.checkpoint_id)
        session.running_started_at = current_time if state == "running" else 0.0
        session.waiting_started_at = current_time if state == "waiting" else 0.0
        if state != "running":
            session.stop_requested = False


def _reserve_task_start(session: UISession) -> tuple[str | None, str | None]:
    with session.lock:
        if session.closing:
            return None, _SESSION_CLOSED_ERROR
        if session.starting or session.running or session.waiting_for_user:
            return None, _SESSION_BUSY_ERROR
        token = uuid.uuid4().hex
        now = time.time()
        session.starting = True
        session.task_start_token = token
        session.starting_started_at = now
        session.last_activity_at = now
        return token, None


def _release_task_start(session: UISession, token: str) -> bool:
    with session.lock:
        if not session.starting or session.task_start_token != token:
            return False
        session.starting = False
        session.task_start_token = ""
        session.starting_started_at = 0.0
        session.last_activity_at = time.time()
        return True


def _task_start_allowed_locked(session: UISession, task_start_token: str) -> bool:
    if session.closing or session.running or session.waiting_for_user:
        return False
    if task_start_token:
        return session.starting and session.task_start_token == task_start_token
    return not session.starting


def _touch_session(session: UISession) -> bool:
    with session.lock:
        if session.closing:
            return False
        session.last_activity_at = time.time()
        return True


def _session_owned_by(session: UISession, identity: WebIdentity) -> bool:
    if (
        not session.owner_digest
        and session.web_identity is None
        and _get_web_identity_provider().is_local
    ):
        return _bind_session_identity(session, identity)
    return (
        bool(session.owner_digest)
        and session.owner_digest == identity.owner_digest
        and isinstance(session.web_identity, WebIdentity)
        and session.web_identity.owner_digest == identity.owner_digest
    )


def _bind_session_identity(session: UISession, identity: WebIdentity) -> bool:
    with session.lock:
        if session.owner_digest and session.owner_digest != identity.owner_digest:
            return False
        if (
            isinstance(session.web_identity, WebIdentity)
            and session.web_identity.owner_digest != identity.owner_digest
        ):
            return False
        session.owner_digest = identity.owner_digest
        session.web_identity = identity
        return True


def _get_session(
    session_id: str,
    *,
    touch: bool = True,
    identity: WebIdentity | None = None,
) -> UISession | None:
    with _session_lock:
        session = _sessions.get(session_id)
    if session is None:
        return None
    with session.lock:
        if session.closing:
            return None
        if identity is not None and not _session_owned_by(session, identity):
            return None
        if touch:
            session.last_activity_at = time.time()
    with _session_lock:
        return session if _sessions.get(session_id) is session else None


def _session_snapshot(identity: WebIdentity | None = None) -> list[UISession]:
    with _session_lock:
        sessions = list(_sessions.values())
    if identity is None:
        return sessions
    return [session for session in sessions if _session_owned_by(session, identity)]


def _register_session(
    session: UISession,
    *,
    max_sessions: int = _SESSION_MAX_COUNT,
    identity: WebIdentity | None = None,
) -> bool:
    if _web_runtime_stopping.is_set():
        return False
    try:
        caller = identity or session.web_identity or _current_web_identity()
    except WebIdentityError:
        return False
    if not isinstance(caller, WebIdentity) or not _bind_session_identity(session, caller):
        return False
    limit = max(1, int(max_sessions))
    with _session_lock:
        if _web_runtime_stopping.is_set():
            return False
        existing = _sessions.get(session.session_id)
        current_count = len(_sessions)
    if existing is session:
        return _touch_session(session)
    if existing is not None:
        return False
    if current_count >= limit:
        _cleanup_sessions(max_sessions=limit - 1)
    if not _touch_session(session):
        return False
    with _session_lock:
        if _web_runtime_stopping.is_set():
            return False
        existing = _sessions.get(session.session_id)
        if existing is not None:
            return existing is session
        if len(_sessions) >= limit:
            return False
        _sessions[session.session_id] = session
        return True


def _remove_session(session: UISession) -> None:
    with session.lock:
        session.closing = True
    with _session_lock:
        if _sessions.get(session.session_id) is session:
            _sessions.pop(session.session_id, None)


def _persist_chat_state(session: UISession, *, force: bool = False) -> bool:
    with session.persist_lock:
        monotonic_now = time.monotonic()
        with session.lock:
            if not session.chat_id or not session.chat_ws or not session.chat_agent:
                return False
            revision = session.persist_revision
            if not force:
                if revision <= session.persisted_revision:
                    return False
                if monotonic_now - session.last_persist_at < _CHAT_PERSIST_INTERVAL_SECONDS:
                    return False
            chat_id = session.chat_id
            chat_ws = session.chat_ws
            chat_agent = session.chat_agent
            session_id = session.session_id
            runtime_config_key = session.runtime_config_key
            team_name = session.team_name
            team_workflow_name = (
                session.team_workflow.get("name")
                if isinstance(session.team_workflow, dict)
                else None
            )
            messages = _safe_json_value(session.messages)
            restored_llm_history = _safe_json_value(session.restored_llm_history)
            waiting_for_user = session.waiting_for_user
            interrupted = session.interrupted
            resume_available = session.resume_available
            starting = session.starting
            running = session.running
            checkpoint_id = session.checkpoint_id
            ask_prompt = session.ask_prompt
            agent = session.agent
            identity = session.web_identity

        if not isinstance(identity, WebIdentity) and _get_web_identity_provider().is_local:
            identity = _current_web_identity()
            if not _bind_session_identity(session, identity):
                return False
        if not isinstance(identity, WebIdentity):
            return False

        metadata_path, error = _chat_metadata_path(
            chat_ws,
            chat_agent,
            chat_id,
            identity=identity,
        )
        state_path, state_error = _chat_state_path(
            chat_ws,
            chat_agent,
            chat_id,
            identity=identity,
        )
        if error or state_error or metadata_path is None or state_path is None:
            return False
        metadata = _read_chat_metadata(
            chat_ws,
            chat_agent,
            chat_id,
            identity=identity,
        )
        if metadata is None:
            metadata = _default_chat_metadata(
                chat_ws,
                chat_agent,
                chat_id,
                identity=identity,
            )
        status = (
            "waiting_for_user"
            if waiting_for_user
            else ("running" if starting or running else ("interrupted" if interrupted else "idle"))
        )
        llm_history = _extract_llm_history(agent) or restored_llm_history
        now = time.time()
        metadata.update(
            {
                "workspace": chat_ws,
                "agent": chat_agent,
                "title": _chat_title(metadata, messages),
                "updated_at": now,
                "last_message_preview": _chat_preview(messages),
                "message_count": len(messages),
                "status": status,
                "owner_digest": identity.owner_digest,
            }
        )
        state = {
            **_default_chat_state(
                chat_ws,
                chat_agent,
                chat_id,
                session_id,
                identity=identity,
            ),
            "backend_session_id": session_id,
            "checkpoint_id": checkpoint_id,
            "resume_available": interrupted and resume_available,
            "runtime_config_key": runtime_config_key,
            "team": team_name or None,
            "team_workflow": team_workflow_name,
            "messages": messages,
            "llm_history": llm_history,
            "waiting_for_user": waiting_for_user,
            "ask_prompt": ask_prompt,
            "status": status,
            "updated_at": now,
            "owner_digest": identity.owner_digest,
        }
        try:
            _write_json_file(metadata_path, metadata)
            _write_json_file(state_path, state)
        except OSError:
            return False

        with session.lock:
            session.last_persist_at = monotonic_now
            session.persisted_revision = max(session.persisted_revision, revision)
        return True


def _session_eviction_reason_locked(
    session: UISession,
    *,
    now: float,
    idle_ttl_seconds: float,
    running_ttl_seconds: float,
    waiting_ttl_seconds: float,
) -> str | None:
    if session.closing:
        return None
    if session.starting:
        started_at = session.starting_started_at or session.last_activity_at
        return "running_timeout" if now - started_at >= running_ttl_seconds else None
    if session.running:
        started_at = session.running_started_at or session.last_activity_at
        return "running_timeout" if now - started_at >= running_ttl_seconds else None
    if session.waiting_for_user:
        started_at = session.waiting_started_at or session.last_activity_at
        return "waiting_timeout" if now - started_at >= waiting_ttl_seconds else None
    return "idle_timeout" if now - session.last_activity_at >= idle_ttl_seconds else None


def _prepare_session_eviction(
    session: UISession,
    reason: str,
    *,
    now: float,
    idle_ttl_seconds: float,
    running_ttl_seconds: float,
    waiting_ttl_seconds: float,
) -> bool:
    with session.lock:
        if session.closing:
            return False
        if reason == "capacity":
            eligible = not session.starting and not session.running and not session.waiting_for_user
        else:
            eligible = _session_eviction_reason_locked(
                session,
                now=now,
                idle_ttl_seconds=idle_ttl_seconds,
                running_ttl_seconds=running_ttl_seconds,
                waiting_ttl_seconds=waiting_ttl_seconds,
            ) == reason
        if not eligible:
            return False
        if reason in {"running_timeout", "waiting_timeout"}:
            _emit(session, "stop", f"session closed after {reason.replace('_', ' ')}")
            _set_session_runtime_state(session, "interrupted", now=now)
            session.ask_prompt = ""
        session.closing = True
        return True


def _cleanup_sessions(
    *,
    now: float | None = None,
    idle_ttl_seconds: float = _SESSION_IDLE_TTL_SECONDS,
    running_ttl_seconds: float = _SESSION_RUNNING_TTL_SECONDS,
    waiting_ttl_seconds: float = _SESSION_WAITING_TTL_SECONDS,
    max_sessions: int = _SESSION_MAX_COUNT,
) -> list[str]:
    current_time = time.time() if now is None else now
    sessions = _session_snapshot()
    candidates: list[tuple[str, UISession, str]] = []
    inactive: list[tuple[float, str, UISession]] = []
    for session in sessions:
        with session.lock:
            reason = _session_eviction_reason_locked(
                session,
                now=current_time,
                idle_ttl_seconds=idle_ttl_seconds,
                running_ttl_seconds=running_ttl_seconds,
                waiting_ttl_seconds=waiting_ttl_seconds,
            )
            if reason is not None:
                candidates.append((session.session_id, session, reason))
            elif not session.closing and not session.starting and not session.running and not session.waiting_for_user:
                inactive.append((session.last_activity_at, session.session_id, session))

    overflow = max(0, len(sessions) - len(candidates) - max(0, int(max_sessions)))
    for _, session_id, session in sorted(inactive)[:overflow]:
        candidates.append((session_id, session, "capacity"))

    removed: list[tuple[str, UISession, str]] = []
    for session_id, session, reason in candidates:
        if not _prepare_session_eviction(
            session,
            reason,
            now=current_time,
            idle_ttl_seconds=idle_ttl_seconds,
            running_ttl_seconds=running_ttl_seconds,
            waiting_ttl_seconds=waiting_ttl_seconds,
        ):
            continue
        with _session_lock:
            if _sessions.get(session_id) is session:
                _sessions.pop(session_id, None)
        removed.append((session_id, session, reason))

    for _, session, reason in removed:
        if reason in {"running_timeout", "waiting_timeout"}:
            _stop_agent(session, unblock_reply=reason == "waiting_timeout")
        with session.lock:
            persist_dirty = session.persist_revision > session.persisted_revision
        if persist_dirty:
            _persist_chat_state(session, force=True)
        _close_agent(session)
        _queue_state(session, finished=True)
    return [session_id for session_id, _, _ in removed]


def _revoke_invalid_web_activity(
    provider: WebIdentityProvider,
) -> list[str]:
    """Stop in-memory work whose trusted identity was removed or changed."""

    if provider.is_local:
        return []
    return _revoke_web_activity_except(
        {
            identity.owner_digest
            for identity in provider.identities()
            if identity.workspace_boundaries_valid()
        },
        revoke_unowned=False,
    )


def _revoke_all_secure_web_activity() -> list[str]:
    """Fail closed when the secure identity registry cannot be trusted."""

    return _revoke_web_activity_except(set(), revoke_unowned=True)


def _revoke_web_activity_except(
    valid_owners: set[str],
    *,
    revoke_unowned: bool,
) -> list[str]:
    """Stop in-memory work outside an explicitly trusted owner set."""

    # Eval workers are not UISessions, so revoke their cancellation handles
    # independently. The worker owns final removal of the handle.
    with _eval_lock:
        for (owner_digest, _run_id), cancel_event in list(
            _eval_cancel_events.items()
        ):
            if owner_digest not in valid_owners:
                cancel_event.set()

    removed: list[tuple[str, UISession]] = []
    for session in _session_snapshot():
        with session.lock:
            if (
                session.closing
                or session.owner_digest in valid_owners
                or (not session.owner_digest and not revoke_unowned)
            ):
                continue
            _emit(
                session,
                "stop",
                "session closed because its Web identity was revoked",
            )
            _set_session_runtime_state(session, "interrupted")
            session.ask_prompt = ""
            session.closing = True
        with _session_lock:
            if _sessions.get(session.session_id) is session:
                _sessions.pop(session.session_id, None)
                removed.append((session.session_id, session))

    for _, session in removed:
        # Do not persist through a revoked identity: its old workspace mapping
        # is no longer an authorized write target.
        try:
            _stop_agent(session, unblock_reply=True)
        except Exception:  # noqa: BLE001
            logger.exception("Failed to stop a session after identity revocation")
        try:
            _close_agent(session)
        except Exception:  # noqa: BLE001
            logger.exception("Failed to close a session after identity revocation")
        _queue_state(session, finished=True)
    if removed:
        logger.warning(
            "Stopped %d Web session(s) after identity revocation",
            len(removed),
        )
    return [session_id for session_id, _ in removed]


def _shutdown_sessions() -> list[str]:
    removed: list[tuple[str, UISession, bool, bool]] = []
    for session in _session_snapshot():
        with session.lock:
            if session.closing:
                continue
            was_active = session.starting or session.running or session.waiting_for_user
            was_waiting = session.waiting_for_user
            if was_active:
                _emit(session, "stop", "session closed because the Web UI is shutting down")
            _set_session_runtime_state(session, "interrupted" if was_active else "idle")
            session.ask_prompt = ""
            session.closing = True
        with _session_lock:
            if _sessions.get(session.session_id) is session:
                _sessions.pop(session.session_id, None)
        removed.append((session.session_id, session, was_active, was_waiting))

    for _, session, stop_agent, unblock_reply in removed:
        if stop_agent:
            _stop_agent(session, unblock_reply=unblock_reply)
        with session.lock:
            persist_dirty = session.persist_revision > session.persisted_revision
        if persist_dirty:
            _persist_chat_state(session, force=True)
        _close_agent(session)
        _queue_state(session, finished=True)
    return [session_id for session_id, _, _, _ in removed]


def _load_bound_chat_checkpoint(
    ws: str,
    agent: str,
    checkpoint_id: str,
    identity: WebIdentity | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    caller = identity or _current_web_identity()
    if not checkpoint_id:
        return None, "Chat has no checkpoint to resume"
    workspace_root, workspace_error = _workspace_root(ws, identity=caller)
    if workspace_error or workspace_root is None:
        return None, workspace_error
    loaded = load_task_checkpoint(workspace_root, checkpoint_id)
    if loaded.get("status") != "OK":
        return None, str(loaded.get("error") or "checkpoint could not be loaded")
    checkpoint = loaded.get("checkpoint")
    if not isinstance(checkpoint, dict):
        return None, "invalid checkpoint payload"
    if str(checkpoint.get("checkpoint_id") or "") != checkpoint_id:
        return None, "checkpoint id does not match this chat"
    checkpoint_agent = str(checkpoint.get("agent_name") or "")
    if checkpoint_agent and checkpoint_agent != agent:
        return None, "checkpoint agent does not match this chat"
    expected_boundary = caller.principal(
        session_id="checkpoint-preflight",
        agent_id=agent or "main",
    ).boundary_digest
    stored_boundary = str(checkpoint.get("principal_boundary_digest") or "")
    if stored_boundary != expected_boundary and not (
        not stored_boundary and _get_web_identity_provider().is_local
    ):
        return None, "checkpoint is not authorized for this identity"
    return checkpoint, None


def _recover_persisted_chat_runtime(
    ws: str,
    agent: str,
    state: dict[str, Any],
    identity: WebIdentity | None = None,
) -> tuple[str, bool]:
    caller = identity or _current_web_identity()
    if not _chat_payload_owned_by(state, caller):
        return "interrupted", False
    persisted_status = str(state.get("status") or "idle")
    was_active = bool(state.get("waiting_for_user")) or persisted_status in {
        "running",
        "waiting_for_user",
        "interrupted",
    }
    if not was_active:
        return "idle", False
    checkpoint, _ = _load_bound_chat_checkpoint(
        ws,
        agent,
        str(state.get("checkpoint_id") or ""),
        caller,
    )
    if checkpoint is None:
        return "interrupted", False
    if str(checkpoint.get("status") or "") in {"completed", "failed"}:
        return "idle", False
    return "interrupted", True


def _load_chat_into_session(
    ws: str,
    agent: str,
    chat_id: str,
    session: UISession | None = None,
    identity: WebIdentity | None = None,
) -> tuple[UISession | None, str | None]:
    caller = identity or _current_web_identity()
    chat_path, error = _chat_dir(ws, agent, chat_id, identity=caller)
    if error or chat_path is None:
        return None, error
    metadata = _read_chat_metadata(ws, agent, chat_id, identity=caller)
    state = _read_chat_state(ws, agent, chat_id, identity=caller)
    if metadata is None or state is None:
        return None, "Chat not found"
    session = session or UISession(
        session_id=str(state.get("backend_session_id") or uuid.uuid4().hex[:16]),
        owner_digest=caller.owner_digest,
        web_identity=caller,
    )
    if not _bind_session_identity(session, caller):
        return None, "Chat not found"
    session.chat_id = chat_id
    session.chat_ws = ws
    session.chat_agent = agent
    session.session_id = session.session_id or uuid.uuid4().hex[:16]
    messages = state.get("messages")
    session.messages = _safe_json_value(messages) if isinstance(messages, list) else []
    history = state.get("llm_history")
    session.restored_llm_history = _safe_json_value(history) if isinstance(history, list) else []
    session.checkpoint_id = str(state.get("checkpoint_id") or "")
    persisted_status = str(state.get("status") or "idle")
    recovered_status, resume_available = _recover_persisted_chat_runtime(
        ws,
        agent,
        state,
        identity=caller,
    )
    if recovered_status == "interrupted":
        _set_session_runtime_state(session, "interrupted")
        session.resume_available = resume_available
    else:
        _set_session_runtime_state(session, "idle")
    session.ask_prompt = ""
    if not _register_session(session, identity=caller):
        return None, _SESSION_CAPACITY_ERROR
    if persisted_status in {"running", "waiting_for_user"} or bool(state.get("waiting_for_user")):
        _persist_chat_state(session, force=True)
    return session, None


def _find_session_by_chat(
    ws: str,
    agent: str,
    chat_id: str,
    identity: WebIdentity | None = None,
) -> UISession | None:
    caller = identity or _current_web_identity()
    matched: UISession | None = None
    for session in _session_snapshot(caller):
        with session.lock:
            if session.closing:
                continue
            if session.chat_ws == ws and session.chat_agent == agent and session.chat_id == chat_id:
                session.last_activity_at = time.time()
                matched = session
                break
    if matched is None:
        return None
    with _session_lock:
        return matched if _sessions.get(matched.session_id) is matched else None


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
    team_name: str = "",
    team_config: dict[str, Any] | None = None,
    team_workflow: dict[str, Any] | None = None,
    task_start_token: str = "",
):
    runtime_config = runtime_config or {}
    with session.lock:
        identity = session.web_identity
    if not isinstance(identity, WebIdentity) and _get_web_identity_provider().is_local:
        identity = _current_web_identity()
        if not _bind_session_identity(session, identity):
            raise WebIdentityError("session identity is invalid")
    if not isinstance(identity, WebIdentity):
        raise WebIdentityError("session identity is missing")
    principal_template = identity.principal(
        session_id=session.session_id,
        agent_id=agent_name or "main",
    )
    resolved_workspace = _resolve_workspace_dir_input(
        workspace_dir,
        identity=identity,
    )
    if not resolved_workspace:
        raise WebIdentityError("workspace is not authorized")
    runtime_key_data = {
        "agent": runtime_config,
        "team": team_config or {},
        "workflow": team_workflow or {},
        "owner_digest": identity.owner_digest,
        "principal_boundary_digest": principal_template.boundary_digest,
    }
    runtime_config_key = json.dumps(runtime_key_data, ensure_ascii=False, sort_keys=True, default=str)
    with session.agent_init_lock:
        with session.lock:
            if not _task_start_allowed_locked(session, task_start_token):
                return None
            rebuild = (
                session.agent is None
                or session.config_path != config_path
                or session.observability_config_path != observability_config_path
                or session.workspace_dir != workspace_dir
                or session.agent_name != agent_name
                or session.team_name != team_name
                or session.runtime_config_key != runtime_config_key
            )
            restored_history = session.restored_llm_history
            current_agent = session.agent

        if not rebuild:
            if restored_history and current_agent is not None:
                _restore_llm_history(current_agent, restored_history)
            with session.lock:
                if not _task_start_allowed_locked(session, task_start_token):
                    return None
                if session.restored_llm_history is restored_history:
                    session.restored_llm_history = []
                session.team_config = team_config
                session.team_workflow = team_workflow
                return session.agent

        new_agent = build_agent(
            config_path=config_path or None,
            observability_config_path=observability_config_path or None,
            skills_dir=_workspace_skills_dir_input(workspace_dir, identity=identity),
            workspace_dir=resolved_workspace,
            agent_name=agent_name,
            agent_prompt=str(runtime_config.get("agent_prompt", "")),
            agent_soul=str(runtime_config.get("agent_soul", "")),
            tools_allowlist=runtime_config.get("tools_allowlist"),
            skill_allowlist=runtime_config.get("skill_allowlist"),
            model_override=str(runtime_config.get("model_override", "")),
            max_turns=runtime_config.get("max_turns"),
            memory_mode=str(runtime_config.get("memory_mode", "project")),
            team_config=team_config,
            principal=principal_template,
        )
        try:
            web_usage_sink = WebSessionUsageSink(session)
            provider = _get_web_identity_provider()
            trace_log_dir = _resolve_trace_log_dir(workspace_dir=workspace_dir)
            raw_trace_sink = JsonlSink(trace_log_dir) if trace_log_dir else NullSink()
            trace_sink = (
                raw_trace_sink
                if provider.is_local
                else WebIdentityStampingSink(session, raw_trace_sink)
            )
            existing_sink = getattr(new_agent, "sink", NullSink())
            if not provider.is_local:
                try:
                    existing_sink.close()
                except Exception:  # noqa: BLE001
                    pass
                existing_sink = NullSink()
            new_agent.sink = MultiSink(existing_sink, trace_sink, web_usage_sink)
            new_agent.handler.ctx.sink = new_agent.sink
            new_agent.handler.ctx.verbose = True
            if restored_history:
                _restore_llm_history(new_agent, restored_history)
            team_step_runner = build_team_step_runner(
                config_path=config_path or None,
                observability_config_path=observability_config_path or None,
                skills_dir=_workspace_skills_dir_input(workspace_dir, identity=identity),
                workspace=resolved_workspace,
                team_config=team_config,
                principal_template=principal_template,
            )
        except Exception:
            new_agent.close()
            raise

        with session.lock:
            if not _task_start_allowed_locked(session, task_start_token):
                discard_new_agent = True
                previous_agent = None
            else:
                discard_new_agent = False
                previous_agent = session.agent
                session.agent = new_agent
                if session.restored_llm_history is restored_history:
                    session.restored_llm_history = []
                session.config_path = config_path
                session.observability_config_path = observability_config_path
                session.workspace_dir = workspace_dir
                session.agent_name = agent_name
                session.team_name = team_name
                session.team_config = team_config
                session.team_workflow = team_workflow
                session.team_step_runner = team_step_runner
                session.runtime_config_key = runtime_config_key

        if discard_new_agent:
            new_agent.close()
            return None
        if previous_agent is not None and previous_agent is not new_agent:
            previous_agent.close()
        return new_agent


def _drain_sync(session: UISession, wait: bool) -> bool:
    """Synchronous drain for background thread"""
    with session.lock:
        if session.closing:
            return True
        agent = session.agent
    if agent is None:
        return True

    finished = False
    while True:
        with session.lock:
            timeout = 0.1 if wait and session.running else 0.0
        try:
            msg = agent.display_queue.get(timeout=timeout)
        except queue.Empty:
            break

        force_persist = False
        stop_draining = False
        with session.lock:
            if session.closing:
                return True
            if "progress" in msg:
                progress = str(msg["progress"])
                if (
                    not _get_web_identity_provider().is_local
                    and progress.startswith("[error] ")
                ):
                    progress = "[error] Agent task failed"
                _append_progress(session, progress)
            elif "ask_user" in msg:
                _set_session_runtime_state(session, "waiting")
                session.ask_prompt = msg["ask_user"]
                _emit(session, "ask_user", msg["ask_user"])
                force_persist = True
                stop_draining = True
            elif "done" in msg:
                result = _secure_web_result(msg["done"])
                _emit(
                    session,
                    "done",
                    {
                        "response": result.get("response", ""),
                        "exit_reason": result.get("exit_reason", ""),
                        "tool_results": result.get("tool_results", []),
                        "turns": result.get("turns", 0),
                        "team_workflow": result.get("team_workflow"),
                    },
                )
                runtime_state = (
                    "interrupted"
                    if str(result.get("exit_reason") or "") == "INTERRUPTED"
                    else "idle"
                )
                _set_session_runtime_state(session, runtime_state)
                session.ask_prompt = ""
                force_persist = True
                finished = True
                stop_draining = True

        _persist_chat_state(session, force=force_persist)
        if stop_draining:
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
        finished = _drain_sync(session, wait=True)
        _queue_state(session, finished)
        with session.lock:
            if finished or session.waiting_for_user or not session.running:
                break
        time.sleep(0.05)


def _workflow_error_result(message: str, error: str, workflow: dict[str, Any] | None = None) -> dict[str, Any]:
    workflow_name = workflow.get("name", "") if isinstance(workflow, dict) else ""
    return {
        "response": message,
        "exit_reason": "ERROR",
        "tool_results": [
            {
                "tool_name": "team_workflow",
                "tool_call_id": "",
                "data": {"status": "ERROR", "error": error},
            }
        ],
        "turns": 0,
        "team_workflow": {
            "name": workflow_name,
            "step_count": 0,
            "duration_ms": 0,
            "steps": [],
        },
    }


def _run_team_workflow_background(
    session: UISession,
    workflow: dict[str, Any],
    task: str,
    step_runner: Any,
    parent_ctx: Any | None,
    agent: Any | None,
) -> None:
    try:
        if agent is None:
            result = _workflow_error_result(
                "[error] active team workflow has no agent",
                "missing parent agent",
                workflow,
            )
        else:

            def _emit_workflow_progress(message: str) -> None:
                agent.display_queue.put({"progress": message})
                _drain_sync(session, wait=False)
                _queue_state(session, finished=False)

            def _suppress_child_progress(_message: str) -> None:
                return None

            workflow_parent_ctx = parent_ctx
            if parent_ctx is not None:
                workflow_parent_ctx = SimpleNamespace(
                    sink=getattr(parent_ctx, "sink", None) or NullSink(),
                    verbose=getattr(parent_ctx, "verbose", False),
                    display_fn=_suppress_child_progress,
                    user_input_fn=getattr(parent_ctx, "user_input_fn", None),
                    stop_signal=getattr(parent_ctx, "stop_signal", None),
                )

            if not callable(step_runner):
                result = _workflow_error_result(
                    "[error] active team workflow has no step runner",
                    "missing team step runner",
                    workflow,
                )
            else:
                result = run_team_workflow(
                    workflow,
                    task.strip(),
                    step_runner,
                    parent_ctx=workflow_parent_ctx,
                    emit=_emit_workflow_progress,
                )
            if not isinstance(result, dict):
                result = _workflow_error_result(
                    "[error] team workflow returned an invalid result",
                    f"invalid workflow result type: {type(result)!r}",
                    workflow,
                )
    except Exception as exc:  # noqa: BLE001
        detail = _web_error_message(
            exc,
            "Team workflow failed",
            prefix_local=True,
        )
        result = _workflow_error_result(f"[error] {detail}", detail, workflow)

    if agent is not None:
        agent.display_queue.put({"done": result})
    else:
        with session.lock:
            _emit(
                session,
                "done",
                {
                    "response": result.get("response", ""),
                    "exit_reason": result.get("exit_reason", ""),
                    "tool_results": result.get("tool_results", []),
                    "turns": result.get("turns", 0),
                    "team_workflow": result.get("team_workflow"),
                },
            )
            _set_session_runtime_state(session, "idle")
            session.ask_prompt = ""
            _queue_state(session, finished=True)
        _persist_chat_state(session, force=True)


def _run_task_background(
    session: UISession,
    task: str,
    task_start_token: str = "",
    checkpoint_id: str = "",
    resume_checkpoint: str = "",
) -> None:
    """Run task in background and put events into async queue"""
    workflow: dict[str, Any] | None = None
    parent_ctx: Any | None = None
    step_runner: Any | None = None
    agent: Any | None = None
    with session.lock:
        if not _task_start_allowed_locked(session, task_start_token):
            return
        session.events = []
        session.llm_stream_buffer = ""
        session.llm_parse_buffer = ""
        session.llm_parse_mode = "visible"
        session.llm_hidden_leading_whitespace = False
        session.llm_tool_payload = ""
        session.llm_tool_payload_truncated = False
        session.parsed_tool_use_count = 0
        session.pending_tool_names = []
        session.emitted_tool_keys = set()
        session.stop_requested = False
        session.checkpoint_id = checkpoint_id
        current_agent = session.agent
        stop_event = getattr(current_agent, "stop_event", None)
        if isinstance(stop_event, threading.Event) and getattr(current_agent, "owns_stop_event", True):
            stop_event.clear()
        _set_session_runtime_state(session, "running")
        session.ask_prompt = ""
        if task.strip():
            _emit(session, "user_task", task.strip())
        workflow = session.team_workflow
        step_runner = session.team_step_runner
        parent_ctx = getattr(getattr(session.agent, "handler", None), "ctx", None)
        agent = session.agent
    _persist_chat_state(session, force=True)

    try:
        if not workflow and agent is not None:
            agent.run_task_async(
                task.strip() or "继续执行 checkpoint 中未完成的任务。",
                resume_checkpoint=resume_checkpoint or None,
                checkpoint_id=checkpoint_id or None,
                reset_stop_event=False,
            )

        if workflow:
            _new_daemon_thread(
                target=_run_team_workflow_background,
                args=(session, workflow, task, step_runner, parent_ctx, agent),
            ).start()

        _drain_background(session)
    except Exception as exc:  # noqa: BLE001
        try:
            _stop_agent(session, unblock_reply=True)
        except Exception:  # noqa: BLE001
            pass
        detail = _web_error_message(
            exc,
            "Failed to drain task result",
            prefix_local=True,
        )
        result = _workflow_error_result(f"[error] {detail}", detail, workflow)
        with session.lock:
            _emit(
                session,
                "done",
                {
                    "response": result.get("response", ""),
                    "exit_reason": result.get("exit_reason", ""),
                    "tool_results": result.get("tool_results", []),
                    "turns": result.get("turns", 0),
                    "team_workflow": result.get("team_workflow"),
                },
            )
            _set_session_runtime_state(session, "idle")
            session.ask_prompt = ""
            _queue_state(session, finished=True)
        _persist_chat_state(session, force=True)


@app.post("/api/chat")
async def submit_task(request: SubmitTaskRequest):
    """Submit a new task"""
    identity = _current_web_identity()
    task = request.task.strip()
    if not request.resume and not task:
        return {"success": False, "error": "Task is required"}
    requested_session_id = _normalize_path_input(request.session_id)
    chat_id = _normalize_path_input(request.chat_id)
    workspace_dir = _normalize_path_input(request.workspace_dir)
    if (
        workspace_dir
        and not _is_workspace_name(workspace_dir)
        and not _get_web_identity_provider().is_local
    ):
        return {"success": False, "error": "Workspace not found"}
    agent_name = _normalize_path_input(request.agent)
    team_name = _normalize_path_input(request.team)
    chat_ws = workspace_dir if _is_workspace_name(workspace_dir) else "default.ws"
    team_config = None
    team_workflow = None
    if team_name:
        ws_root, ws_error = _workspace_root(chat_ws)
        if ws_error or ws_root is None:
            return {"success": False, "error": ws_error}
        team_config, team_error = await _run_blocking(read_team_config, ws_root, team_name)
        if team_error or team_config is None:
            return {"success": False, "error": team_error}
        team_workflow, workflow_error = await _run_blocking(
            read_team_workflow,
            ws_root,
            team_name,
            team_config,
        )
        if workflow_error:
            return {"success": False, "error": workflow_error}
        if not agent_name:
            agent_name = str(team_config.get("leader") or "").strip()
    if chat_id and not agent_name:
        return {"success": False, "error": "Agent is required for persistent chat"}
    session = (
        _get_session(requested_session_id, identity=identity)
        if requested_session_id
        else None
    )
    if session is None and chat_id:
        session = _find_session_by_chat(
            chat_ws,
            agent_name,
            chat_id,
            identity=identity,
        )
    if session is None and chat_id:
        loaded_session, load_error = await _run_blocking(
            _load_chat_into_session,
            chat_ws,
            agent_name,
            chat_id,
            None,
            identity,
        )
        session = loaded_session
        if session is None and load_error == _SESSION_CAPACITY_ERROR:
            session = _find_session_by_chat(
                chat_ws,
                agent_name,
                chat_id,
                identity=identity,
            )
        if load_error and session is None:
            return {"success": False, "error": load_error}
        if session is None:
            return {"success": False, "error": "Chat session could not be loaded"}
    if session is None:
        session_id = uuid.uuid4().hex[:16]
        session = UISession(
            session_id=session_id,
            owner_digest=identity.owner_digest,
            web_identity=identity,
        )
        if not _register_session(session, identity=identity):
            return {"success": False, "error": _SESSION_CAPACITY_ERROR}
    else:
        session_id = session.session_id

    if request.resume:
        with session.lock:
            if not session.interrupted:
                return {"success": False, "error": "Chat has no interrupted task to resume"}
            if (
                not chat_id
                or session.chat_id != chat_id
                or session.chat_ws != chat_ws
                or session.chat_agent != agent_name
            ):
                return {"success": False, "error": "Resume request does not match the checkpoint chat"}
            checkpoint_id = session.checkpoint_id
            checkpoint_ws = session.chat_ws
            checkpoint_agent = session.chat_agent
        if not checkpoint_ws or not checkpoint_agent:
            return {"success": False, "error": "Resume requires a persistent agent chat"}
        _, checkpoint_error = await _run_blocking(
            _load_bound_chat_checkpoint,
            checkpoint_ws,
            checkpoint_agent,
            checkpoint_id,
            identity,
        )
        if checkpoint_error:
            return {"success": False, "error": f"Checkpoint is not resumable: {checkpoint_error}"}
        resume_checkpoint = checkpoint_id
    else:
        checkpoint_id = uuid.uuid4().hex[:16]
        resume_checkpoint = ""

    task_start_token, reservation_error = _reserve_task_start(session)
    if reservation_error or task_start_token is None:
        result = {"success": False, "error": reservation_error or _SESSION_BUSY_ERROR}
        if reservation_error == _SESSION_BUSY_ERROR:
            result["code"] = "SESSION_BUSY"
        return result

    handed_off = False
    try:
        config_path, observability_config_path, config_error = (
            _effective_web_config_paths(
                request.config_path,
                request.observability_config_path,
            )
        )
        if config_error:
            return {"success": False, "error": config_error}
        if chat_id:
            with session.lock:
                session.chat_id = chat_id
                session.chat_ws = chat_ws
                session.chat_agent = agent_name
        runtime_config, runtime_error = await _run_blocking(
            _agent_runtime_config,
            workspace_dir or chat_ws,
            agent_name,
        )
        if runtime_error:
            return {"success": False, "error": runtime_error}

        # Agent construction loads configuration, prompts, skills, and memory from disk.
        try:
            agent = await _run_blocking(
                _ensure_agent,
                session,
                config_path,
                observability_config_path,
                workspace_dir or chat_ws,
                agent_name,
                runtime_config,
                team_name,
                team_config,
                team_workflow,
                task_start_token,
            )
        except Exception as exc:  # noqa: BLE001
            return {
                "success": False,
                "error": _web_error_message(
                    exc,
                    "Failed to initialize Agent",
                    prefix_local=True,
                ),
            }
        if agent is None:
            return {"success": False, "error": "Session became unavailable while initializing Agent"}

        try:
            thread = _new_daemon_thread(
                target=_run_task_background,
                args=(
                    session,
                    task,
                    task_start_token,
                    checkpoint_id,
                    resume_checkpoint,
                ),
            )
            thread.start()
        except Exception as exc:  # noqa: BLE001
            return {
                "success": False,
                "error": _web_error_message(
                    exc,
                    "Failed to start task",
                    prefix_local=True,
                ),
            }

        handed_off = True
        return {
            "success": True,
            "data": {
                "session_id": session_id,
                "checkpoint_id": checkpoint_id,
                "resumed": bool(resume_checkpoint),
            },
        }
    finally:
        if not handed_off:
            _release_task_start(session, task_start_token)


@app.post("/api/chat/reply")
async def send_reply(request: ReplyRequest):
    """Send reply to agent's ask_user"""
    identity = _current_web_identity()
    requested_session_id = _normalize_path_input(request.session_id)
    if requested_session_id:
        active_session = _get_session(requested_session_id, identity=identity)
    else:
        if not _get_web_identity_provider().is_local:
            return {"success": False, "error": "session_id is required"}
        active_session = None
        for candidate in _session_snapshot(identity):
            with candidate.lock:
                if candidate.waiting_for_user:
                    active_session = candidate
                    candidate.last_activity_at = time.time()
                    break

    if active_session is None:
        return {"success": False, "error": "Session is not waiting for reply"}

    with active_session.lock:
        if not active_session.waiting_for_user:
            return {"success": False, "error": "Session is not waiting for reply"}
        if active_session.agent is None:
            return {"success": False, "error": "No active session waiting for reply"}

        _set_session_runtime_state(active_session, "running")
        _emit(active_session, "user_reply", request.reply.strip())
        active_session.agent.reply_queue.put(request.reply.strip())
    await _run_blocking(_persist_chat_state, active_session, force=True)

    # Restart drain in background
    thread = _new_daemon_thread(
        target=_drain_background,
        args=(active_session,),
    )
    thread.start()

    return {"success": True}


@app.post("/api/chat/stop")
async def stop_task(request: Request):
    """Stop current task"""
    identity = _request_web_identity(request)
    try:
        payload = await request.json()
    except (json.JSONDecodeError, RuntimeError):
        payload = {}
    requested_session_id = _normalize_path_input(
        payload.get("session_id", "") if isinstance(payload, dict) else ""
    )

    if requested_session_id:
        session = _get_session(requested_session_id, identity=identity)
        if session is None:
            return {"success": False, "error": "Session not found"}
        sessions = [session]
    else:
        if not _get_web_identity_provider().is_local:
            return {"success": False, "error": "session_id is required"}
        sessions = _session_snapshot(identity)
    for session in sessions:
        with session.lock:
            if session.closing:
                continue
            agent = session.agent
            if agent is None:
                continue
            if session.stop_requested:
                return {"success": True, "data": {"already_stopping": True}}
            waiting_for_user = session.waiting_for_user
            if not waiting_for_user and not session.running and not agent.is_running():
                continue
            if waiting_for_user:
                _set_session_runtime_state(session, "running")
                session.ask_prompt = ""
            session.stop_requested = True
            _emit(session, "stop", "interrupt signal sent")
        _stop_agent(session, unblock_reply=waiting_for_user)
        _queue_state(session, finished=False)
        await _run_blocking(_persist_chat_state, session, force=True)
        if waiting_for_user:
            _new_daemon_thread(
                target=_drain_background,
                args=(session,),
            ).start()
        return {"success": True}

    return {"success": False, "error": "No running task"}


@app.get("/api/chat/stream")
async def stream_chat(request: Request):
    """SSE stream for chat events"""
    identity = _request_web_identity(request)
    session_id = request.query_params.get("session_id", "")
    session = _get_session(session_id, identity=identity)

    if session is None:
        # Return empty stream
        async def empty_stream():
            yield f"data: {json.dumps({'type': 'error', 'data': 'Session not found'})}\n\n"

        return StreamingResponse(
            empty_stream(),
            media_type="text/event-stream",
        )

    cursor = 0
    for value in (
        request.query_params.get("after", ""),
        request.headers.get("last-event-id", ""),
    ):
        try:
            cursor = max(cursor, max(0, int(value)))
        except (TypeError, ValueError):
            continue

    async def event_generator():
        last_event_id = cursor

        while True:
            state: dict[str, Any] = {}
            finished = False
            timed_out = False
            try:
                state = await asyncio.to_thread(session.event_queue.get, True, 0.5)
                finished = bool(state.get("finished"))
            except queue.Empty:
                timed_out = True

            with session.lock:
                first_available_id = (
                    int(session.events[0].get("id", 0))
                    if session.events
                    else session.event_sequence + 1
                )
                if last_event_id < first_available_id - 1:
                    if session.waiting_for_user:
                        snapshot_status = "waiting_for_user"
                    elif session.starting or session.running:
                        snapshot_status = "running"
                    elif session.interrupted:
                        snapshot_status = "interrupted"
                    else:
                        snapshot_status = "idle"
                    new_events = [
                        {
                            "id": session.event_sequence,
                            "type": "session_snapshot",
                            "data": {
                                "messages": copy.deepcopy(session.messages),
                                "status": snapshot_status,
                                "waiting_for_user": session.waiting_for_user,
                                "ask_prompt": session.ask_prompt,
                            },
                        }
                    ]
                else:
                    new_events = [
                        dict(event)
                        for event in session.events
                        if int(event.get("id", 0)) > last_event_id
                    ]
                if new_events:
                    last_event_id = int(new_events[-1]["id"])
                session_done = (
                    not session.starting
                    and not session.running
                    and not session.waiting_for_user
                )

            for event in new_events:
                yield (
                    f"id: {event['id']}\n"
                    f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                )

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


def _list_chats_sync(ws: str, agent: str) -> dict[str, Any]:
    identity = _current_web_identity()
    root, error = _chat_root(ws, agent, identity=identity)
    if error or root is None:
        return {"success": False, "error": error}
    if not os.path.isdir(root):
        return {"success": True, "data": []}
    rows: list[dict[str, Any]] = []
    try:
        for entry in os.scandir(root):
            if not entry.is_dir(follow_symlinks=False) or not _valid_chat_id(entry.name):
                continue
            metadata = _read_chat_metadata(
                ws,
                agent,
                entry.name,
                identity=identity,
            )
            if metadata is not None:
                if (
                    str(metadata.get("status") or "") in {"running", "waiting_for_user"}
                    and _find_session_by_chat(
                        ws,
                        agent,
                        entry.name,
                        identity=identity,
                    ) is None
                ):
                    state = _read_chat_state(
                        ws,
                        agent,
                        entry.name,
                        identity=identity,
                    )
                    if state is not None:
                        recovered_status, _ = _recover_persisted_chat_runtime(
                            ws,
                            agent,
                            state,
                            identity=identity,
                        )
                        metadata = {**metadata, "status": recovered_status}
                rows.append(metadata)
    except OSError as exc:
        return {"success": False, "error": _web_error_message(exc)}
    rows.sort(key=lambda item: float(item.get("updated_at", 0.0) or 0.0), reverse=True)
    return {"success": True, "data": rows}


@app.get("/api/chats")
async def list_chats(ws: str = "default.ws", agent: str = ""):
    return await _run_blocking(_list_chats_sync, ws, agent)


def _create_chat_sync(ws: str, agent: str) -> dict[str, Any]:
    identity = _current_web_identity()
    root, error = _chat_root(ws, agent, identity=identity)
    if error or root is None:
        return {"success": False, "error": error}
    chat_id = uuid.uuid4().hex[:16]
    metadata = _default_chat_metadata(ws, agent, chat_id, identity=identity)
    session_id = uuid.uuid4().hex[:16]
    state = _default_chat_state(
        ws,
        agent,
        chat_id,
        session_id=session_id,
        identity=identity,
    )
    metadata_path, metadata_error = _chat_metadata_path(
        ws,
        agent,
        chat_id,
        identity=identity,
    )
    state_path, state_error = _chat_state_path(
        ws,
        agent,
        chat_id,
        identity=identity,
    )
    if metadata_error or state_error or metadata_path is None or state_path is None:
        return {"success": False, "error": metadata_error or state_error}
    session = UISession(
        session_id=session_id,
        chat_id=chat_id,
        chat_ws=ws,
        chat_agent=agent,
        owner_digest=identity.owner_digest,
        web_identity=identity,
    )
    if not _register_session(session, identity=identity):
        return {"success": False, "error": _SESSION_CAPACITY_ERROR}
    try:
        _write_json_file(metadata_path, metadata)
        _write_json_file(state_path, state)
    except OSError as exc:
        _remove_session(session)
        return {"success": False, "error": _web_error_message(exc)}
    return {"success": True, "data": {"metadata": metadata, "state": state}}


@app.post("/api/chats")
async def create_chat(request: ChatCreateRequest):
    ws = _normalize_path_input(request.ws) or "default.ws"
    agent = _normalize_path_input(request.agent)
    if not agent:
        return {"success": False, "error": "Agent is required"}
    return await _run_blocking(_create_chat_sync, ws, agent)


def _read_chat_sync(chat_id: str, ws: str, agent: str) -> dict[str, Any]:
    identity = _current_web_identity()
    session = _find_session_by_chat(ws, agent, chat_id, identity=identity)
    if session is None:
        session, error = _load_chat_into_session(
            ws,
            agent,
            chat_id,
            identity=identity,
        )
        if error or session is None:
            return {"success": False, "error": error}
    metadata = _read_chat_metadata(ws, agent, chat_id, identity=identity)
    state = _read_chat_state(ws, agent, chat_id, identity=identity)
    if metadata is None or state is None:
        return {"success": False, "error": "Chat not found"}
    with session.lock:
        waiting_for_user = session.waiting_for_user
        status = (
            "waiting_for_user"
            if waiting_for_user
            else (
                "running"
                if session.starting or session.running
                else ("interrupted" if session.interrupted else "idle")
            )
        )
        state = {
            **state,
            "backend_session_id": session.session_id,
            "checkpoint_id": session.checkpoint_id,
            "resume_available": session.interrupted and session.resume_available,
            "event_cursor": session.event_sequence,
            "messages": _safe_json_value(session.messages),
            "waiting_for_user": waiting_for_user,
            "ask_prompt": session.ask_prompt,
            "status": status,
        }
        metadata = {
            **metadata,
            "message_count": len(session.messages),
            "status": status,
        }
    return {"success": True, "data": {"metadata": metadata, "state": state}}


@app.get("/api/chats/{chat_id}")
async def read_chat(chat_id: str, ws: str = "default.ws", agent: str = ""):
    ws = _normalize_path_input(ws) or "default.ws"
    agent = _normalize_path_input(agent)
    if not agent:
        return {"success": False, "error": "Agent is required"}
    return await _run_blocking(_read_chat_sync, chat_id, ws, agent)


def _delete_chat_sync(chat_id: str, ws: str, agent: str) -> dict[str, Any]:
    identity = _current_web_identity()
    session = _find_session_by_chat(ws, agent, chat_id, identity=identity)
    if session is not None:
        with session.lock:
            if session.starting or session.running:
                return {"success": False, "error": "Chat is running; stop the task before deleting it"}
    chat_path, error = _chat_dir(ws, agent, chat_id, identity=identity)
    if error or chat_path is None:
        return {"success": False, "error": error}
    if not os.path.isdir(chat_path):
        return {"success": False, "error": "Chat not found"}
    if session is not None:
        with session.persist_lock:
            try:
                shutil.rmtree(chat_path)
            except OSError as exc:
                return {"success": False, "error": _web_error_message(exc)}
            _remove_session(session)
        _close_agent(session)
    else:
        try:
            shutil.rmtree(chat_path)
        except OSError as exc:
            return {"success": False, "error": _web_error_message(exc)}
    return {"success": True}


@app.delete("/api/chats/{chat_id}")
async def delete_chat(chat_id: str, ws: str = "default.ws", agent: str = ""):
    ws = _normalize_path_input(ws) or "default.ws"
    agent = _normalize_path_input(agent)
    if not agent:
        return {"success": False, "error": "Agent is required"}
    return await _run_blocking(_delete_chat_sync, chat_id, ws, agent)


@app.get("/api/usage/summary")
async def read_usage_summary(
    observability_config_path: str = "",
    limit: int = 20,
    ws: str = "default.ws",
):
    _, observability_config_path, config_error = _effective_web_config_paths(
        "",
        observability_config_path,
    )
    if config_error:
        return {"success": False, "error": config_error}
    log_dir = _resolve_trace_log_dir(observability_config_path=observability_config_path, ws=ws)
    if not log_dir:
        return {"success": False, "error": "Invalid workspace name"}

    if not os.path.isdir(log_dir):
        return {
            "success": True,
            "data": {
                "configured": True,
                **({"log_dir": log_dir} if _get_web_identity_provider().is_local else {}),
                "message": "Trace log directory does not exist yet. Run a task to create it.",
                "totals": _empty_usage(),
                "sessions": [],
                "updated_at": time.time(),
            },
        }

    try:
        events = await _run_blocking(load_dir, log_dir)
        summary = await _run_blocking(_usage_summary_from_events, events, limit=limit)
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": _web_error_message(exc)}

    sessions = summary["sessions"]
    return {
        "success": True,
        "data": {
            "configured": True,
            **({"log_dir": log_dir} if _get_web_identity_provider().is_local else {}),
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
    _, observability_config_path, config_error = _effective_web_config_paths(
        "",
        observability_config_path,
    )
    if config_error:
        return {"success": False, "error": config_error}
    log_dir = _resolve_trace_log_dir(observability_config_path=observability_config_path, ws=ws)
    if not log_dir:
        return {"success": False, "error": "Invalid workspace name"}
    if not os.path.isdir(log_dir):
        return {
            "success": True,
            "data": {
                "configured": True,
                **({"log_dir": log_dir} if _get_web_identity_provider().is_local else {}),
                "message": "Trace log directory does not exist yet. Run a task to create it.",
                "sessions": [],
                "updated_at": time.time(),
            },
        }

    try:
        events = await _run_blocking(load_dir, log_dir)
        sessions = await _run_blocking(_trace_summary_from_events, events, limit=limit)
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": _web_error_message(exc)}

    return {
        "success": True,
        "data": {
            "configured": True,
            **({"log_dir": log_dir} if _get_web_identity_provider().is_local else {}),
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
    _, observability_config_path, config_error = _effective_web_config_paths(
        "",
        observability_config_path,
    )
    if config_error:
        return {"success": False, "error": config_error}
    log_dir = _resolve_trace_log_dir(observability_config_path=observability_config_path, ws=ws)
    if not log_dir:
        return {"success": False, "error": "Invalid workspace name"}

    file_path, error = _trace_file_path(log_dir, session_id)
    if error or file_path is None:
        return {"success": False, "error": error}

    try:
        events = await _run_blocking(load_events, file_path)
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": _web_error_message(exc)}

    summaries = await _run_blocking(_trace_summary_from_events, events, limit=1)
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
            "events": _secure_trace_payload(events),
            **({"log_path": file_path} if _get_web_identity_provider().is_local else {}),
        },
    }


# === Workspace Management API ===

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_WORKSPACE_ROOT = os.path.join(_PROJECT_ROOT, "workspace")
_DEFAULT_WORKSPACE_ROOT = _WORKSPACE_ROOT


def _is_workspace_name(ws: str) -> bool:
    return (
        bool(ws)
        and ws.endswith(".ws")
        and not os.path.isabs(ws)
        and os.path.basename(ws) == ws
        and ws not in {".ws", "..ws"}
        and ".." not in ws.split(".")
    )


def _workspace_root(
    ws: str,
    identity: WebIdentity | None = None,
) -> tuple[str | None, str | None]:
    if not _is_workspace_name(ws):
        return None, "Invalid workspace name"
    try:
        caller = identity or _current_web_identity()
        authorized_root = caller.resolve_workspace(ws)
        root = os.path.realpath(authorized_root)
    except (WebIdentityError, WebIdentityConfigurationError, OSError):
        return None, "Workspace not found"
    if _get_web_identity_provider().is_local:
        workspace_parent = os.path.realpath(_WORKSPACE_ROOT)
        if os.path.commonpath([workspace_parent, root]) != workspace_parent:
            return None, "Path traversal not allowed"
    if not os.path.isdir(root):
        return None, "Workspace not found"
    return root, None


def _secure_workspace_path_error(
    ws_root: str,
    real_path: str,
    *,
    identity: WebIdentity,
    write: bool,
) -> str | None:
    if _get_web_identity_provider().is_local:
        return None
    try:
        relative = os.path.relpath(real_path, ws_root).replace("\\", "/")
    except (OSError, ValueError):
        return "Path traversal not allowed"
    parts = tuple(part.casefold() for part in relative.split("/") if part)
    if not parts or ".." in parts:
        return "Path traversal not allowed"
    if parts[0] == "runtime" or (
        len(parts) == 1
        and parts[0].casefold() in {"_intervene", "_keyinfo", "plan.md"}
    ):
        return "Protected workspace state is unavailable via the file API"
    managed_memory = (
        parts[0] == "memory"
        or parts[:2] == ("system", "memory")
        or (
            len(parts) >= 4
            and parts[:2] == ("system", "agents")
            and parts[-1].casefold() == "memory.md"
        )
    )
    if managed_memory and write:
        return "Managed memory requires the memory candidate workflow"
    if managed_memory and MEMORY_READ_SCOPE not in identity.scopes:
        return "Memory read scope is required"
    return None


def _resolve_system_file_path(
    ws: str,
    path: str,
    *,
    identity: WebIdentity | None = None,
    write: bool = False,
) -> tuple[str | None, str | None, str | None]:
    if not path:
        return None, None, "Path is required"
    if os.path.isabs(path):
        return None, None, "Absolute paths are not allowed"
    normalized_path = path.replace("\\", "/")
    if not normalized_path.startswith("system/"):
        return None, None, "Only system/ files can be managed via this endpoint"

    caller = identity or _current_web_identity()
    ws_root, error = _workspace_root(ws, identity=caller)
    if error or ws_root is None:
        return None, None, error

    real_path = os.path.realpath(os.path.join(ws_root, normalized_path))
    if os.path.commonpath([ws_root, real_path]) != ws_root:
        return None, None, "Path traversal not allowed"
    system_root = os.path.realpath(os.path.join(ws_root, "system"))
    if os.path.commonpath([system_root, real_path]) != system_root:
        return None, None, "Only system/ files can be managed via this endpoint"
    protected_error = _secure_workspace_path_error(
        ws_root,
        real_path,
        identity=caller,
        write=write,
    )
    if protected_error:
        return None, None, protected_error
    return real_path, normalized_path, None


def _resolve_workspace_preview_path(
    ws: str,
    path: str,
    *,
    identity: WebIdentity | None = None,
) -> tuple[str | None, str | None, str | None]:
    if not path:
        return None, None, "Path is required"
    if os.path.isabs(path):
        return None, None, "Absolute paths are not allowed"
    normalized_path = path.replace("\\", "/")

    caller = identity or _current_web_identity()
    ws_root, error = _workspace_root(ws, identity=caller)
    if error or ws_root is None:
        return None, None, error

    real_path = os.path.realpath(os.path.join(ws_root, normalized_path))
    if os.path.commonpath([ws_root, real_path]) != ws_root:
        return None, None, "Path traversal not allowed"
    protected_error = _secure_workspace_path_error(
        ws_root,
        real_path,
        identity=caller,
        write=False,
    )
    if protected_error:
        return None, None, protected_error
    return real_path, normalized_path, None


def _build_dir_tree(
    root_path: str,
    current_path: str = "",
    *,
    include_memory: bool = True,
    include_runtime: bool = True,
) -> dict[str, Any]:
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
        child_parts = tuple(
            part.casefold() for part in child_path.split("/") if part
        )
        if not include_runtime and (
            child_parts[0] == "runtime"
            or (
                len(child_parts) == 1
                and child_parts[0].casefold()
                in {"_intervene", "_keyinfo", "plan.md"}
            )
        ):
            continue
        if not include_memory and (
            child_parts[0] == "memory"
            or child_parts[:2] == ("system", "memory")
            or (
                len(child_parts) >= 4
                and child_parts[:2] == ("system", "agents")
                and child_parts[-1].casefold() == "memory.md"
            )
        ):
            continue
        if entry.is_dir(follow_symlinks=False):
            children.append(
                _build_dir_tree(
                    entry.path,
                    child_path,
                    include_memory=include_memory,
                    include_runtime=include_runtime,
                )
            )
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


def _agent_runtime_config(workspace_dir: str, agent_name: str) -> tuple[dict[str, Any] | None, str | None]:
    if not agent_name:
        return None, None
    workspace_path = _resolve_workspace_dir_input(workspace_dir)
    if not workspace_path:
        workspace_input = _normalize_path_input(workspace_dir) or "default.ws"
        if _is_workspace_name(workspace_input):
            workspace_path, error = _workspace_root(workspace_input)
            if error or workspace_path is None:
                return None, error
        else:
            workspace_path = workspace_input
    return load_agent_runtime_config(workspace_path, agent_name)


def _valid_task_id(task_id: str) -> bool:
    return bool(task_id) and _TASK_ID_RE.fullmatch(task_id) is not None and task_id not in {".", ".."}


def _task_store_path(ws: str) -> tuple[str | None, str | None]:
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return None, error
    task_root = os.path.join(ws_root, "runtime", "tasks")
    if not _get_web_identity_provider().is_local:
        task_root = os.path.join(
            task_root,
            "_owners",
            _current_web_identity().owner_digest,
        )
    return os.path.join(task_root, "tasks.json"), None


def _scheduled_task_store_lock(ws: str):
    path, _ = _task_store_path(ws)
    if path is None:
        return nullcontext()
    workspace_root = os.path.dirname(os.path.dirname(os.path.dirname(path)))
    return workspace_write_lock(workspace_root)


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
        return [], _web_error_message(exc)
    raw_tasks = payload.get("tasks", [])
    if not isinstance(raw_tasks, list):
        return [], "Invalid task store"
    return [item for item in raw_tasks if isinstance(item, dict)], None


def _write_scheduled_tasks(ws: str, tasks: list[dict[str, Any]]) -> str | None:
    path, error = _task_store_path(ws)
    if error or path is None:
        return error
    try:
        atomic_write_json(path, {"tasks": tasks, "updated_at": time.time()})
    except OSError as exc:
        return _web_error_message(exc)
    return None


def _should_seed_default_tasks(ws: str) -> bool:
    return (
        _get_web_identity_provider().is_local
        and ws == "default.ws"
        and os.path.realpath(_WORKSPACE_ROOT) == os.path.realpath(_DEFAULT_WORKSPACE_ROOT)
    )


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
    identity = _current_web_identity()
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
    config_path, observability_config_path, config_error = (
        _effective_web_config_paths(
            request.config_path,
            request.observability_config_path,
        )
    )
    if config_error:
        return None, config_error
    if existing and str(existing.get("owner_digest") or "") not in {
        "",
        identity.owner_digest,
    }:
        return None, "Task not found"

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
        "config_path": config_path,
        "observability_config_path": observability_config_path,
        "owner_digest": identity.owner_digest,
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
        return None, _web_error_message(exc)
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
    identity = _current_web_identity()
    if str(task.get("owner_digest") or "") not in {"", identity.owner_digest}:
        return None, "Task not found"
    ws = str(task.get("workspace") or "default.ws")
    agent = str(task.get("agent") or "")
    if not agent:
        return None, "Agent is required when keeping task runs in one chat"

    chat_id = str(task.get("chat_id") or "")
    if chat_id:
        session = _find_session_by_chat(ws, agent, chat_id, identity=identity)
        if session is None:
            session, error = _load_chat_into_session(
                ws,
                agent,
                chat_id,
                identity=identity,
            )
            if error or session is None:
                return None, error
        return session, None

    chat_id = uuid.uuid4().hex[:16]
    metadata = _default_chat_metadata(ws, agent, chat_id, identity=identity)
    metadata["title"] = str(task.get("name") or "Scheduled Task")[:40]
    session_id = uuid.uuid4().hex[:16]
    state = _default_chat_state(
        ws,
        agent,
        chat_id,
        session_id=session_id,
        identity=identity,
    )
    metadata_path, metadata_error = _chat_metadata_path(
        ws,
        agent,
        chat_id,
        identity=identity,
    )
    state_path, state_error = _chat_state_path(
        ws,
        agent,
        chat_id,
        identity=identity,
    )
    if metadata_error or state_error or metadata_path is None or state_path is None:
        return None, metadata_error or state_error
    session = UISession(
        session_id=session_id,
        chat_id=chat_id,
        chat_ws=ws,
        chat_agent=agent,
        owner_digest=identity.owner_digest,
        web_identity=identity,
    )
    if not _register_session(session, identity=identity):
        return None, _SESSION_CAPACITY_ERROR
    try:
        _write_json_file(metadata_path, metadata)
        _write_json_file(state_path, state)
    except OSError as exc:
        _remove_session(session)
        return None, _web_error_message(exc)
    task["chat_id"] = chat_id
    return session, None


def _dispatch_scheduled_task(task: dict[str, Any]) -> tuple[str | None, str | None]:
    identity = _current_web_identity()
    if str(task.get("owner_digest") or "") not in {"", identity.owner_digest}:
        return None, "Task not found"
    ws = str(task.get("workspace") or "default.ws")
    agent_name = str(task.get("agent") or "")
    runtime_config, runtime_error = _agent_runtime_config(ws, agent_name)
    if runtime_error:
        return None, runtime_error

    if task.get("keep_one_chat"):
        session, chat_error = _create_or_load_task_chat(task)
        if chat_error or session is None:
            return None, chat_error
    else:
        session = UISession(
            session_id=uuid.uuid4().hex[:16],
            owner_digest=identity.owner_digest,
            web_identity=identity,
        )
        if not _register_session(session, identity=identity):
            return None, _SESSION_CAPACITY_ERROR

    task_start_token, reservation_error = _reserve_task_start(session)
    if reservation_error or task_start_token is None:
        return None, reservation_error or _SESSION_BUSY_ERROR

    handed_off = False
    try:
        try:
            agent = _ensure_agent(
                session,
                str(task.get("config_path") or ""),
                str(task.get("observability_config_path") or ""),
                ws,
                agent_name,
                runtime_config,
                task_start_token=task_start_token,
            )
        except Exception as exc:  # noqa: BLE001
            return None, _web_error_message(
                exc,
                "Failed to initialize Agent",
                prefix_local=True,
            )
        if agent is None:
            return None, "Session became unavailable while initializing Agent"

        try:
            thread = _new_daemon_thread(
                target=_run_task_background,
                args=(session, str(task.get("prompt") or ""), task_start_token),
            )
            thread.start()
        except Exception as exc:  # noqa: BLE001
            return None, _web_error_message(
                exc,
                "Failed to start task",
                prefix_local=True,
            )

        handed_off = True
        return session.session_id, None
    finally:
        if not handed_off:
            _release_task_start(session, task_start_token)


def _claim_scheduled_task_record(task: dict[str, Any], now_ts: float) -> None:
    task["updated_at"] = time.time()
    task["last_run"] = _format_task_datetime(now_ts)
    task["last_dispatch_id"] = uuid.uuid4().hex
    task["last_session_id"] = ""
    task["last_error"] = ""
    if task.get("repeat") == "none":
        task["next_run"] = None
        task["status"] = "paused"
        return
    try:
        task["next_run"] = _next_task_run(task, after_ts=now_ts, first=False)
    except ValueError as exc:
        task["next_run"] = None
        task["last_error"] = _web_error_message(exc)
    if not task.get("next_run"):
        task["status"] = "paused"


def _record_scheduled_task_dispatch_result(
    task: dict[str, Any],
    session_id: str | None,
    error: str | None,
) -> None:
    task["updated_at"] = time.time()
    if error:
        task["last_error"] = error
        return
    task["last_session_id"] = session_id or ""
    task["last_error"] = ""


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
    try:
        provider = _get_web_identity_provider()
    except WebIdentityConfigurationError:
        _revoke_all_secure_web_activity()
        raise
    _revoke_invalid_web_activity(provider)
    for identity in provider.identities():
        token = _WEB_IDENTITY_CONTEXT.set(identity)
        try:
            if provider.is_local:
                if not os.path.isdir(_WORKSPACE_ROOT):
                    continue
                workspace_names = sorted(os.listdir(_WORKSPACE_ROOT))
            else:
                workspace_names = sorted(
                    alias for alias, _path in identity.workspace_aliases
                )
            for ws in workspace_names:
                if _is_workspace_name(ws):
                    _run_due_scheduled_tasks_in_workspace(ws, now_ts)
        finally:
            _WEB_IDENTITY_CONTEXT.reset(token)


def _run_due_scheduled_tasks_in_workspace(ws: str, now_ts: float) -> None:
    with _scheduled_task_store_lock(ws):
        tasks, error = _read_scheduled_tasks(ws)
        if error:
            return
        changed = False
        for task in tasks:
            if str(task.get("owner_digest") or "") not in {
                "",
                _current_web_identity().owner_digest,
            }:
                continue
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
                _claim_scheduled_task_record(task, now_ts)
                claim_error = _write_scheduled_tasks(ws, tasks)
                if claim_error:
                    logger.error(
                        "Failed to persist scheduled task claim for workspace %s: %s",
                        ws,
                        claim_error,
                    )
                    changed = False
                    break
                session_id, dispatch_error = _dispatch_scheduled_task(task)
                _record_scheduled_task_dispatch_result(
                    task,
                    session_id,
                    dispatch_error,
                )
                result_error = _write_scheduled_tasks(ws, tasks)
                if result_error:
                    logger.error(
                        "Failed to persist scheduled task result for workspace %s: %s",
                        ws,
                        result_error,
                    )
                changed = False
        if changed:
            write_error = _write_scheduled_tasks(ws, tasks)
            if write_error:
                logger.error(
                    "Failed to persist scheduled task validation for workspace %s: %s",
                    ws,
                    write_error,
                )


def _task_scheduler_loop() -> None:
    while not _task_scheduler_stop.wait(30):
        try:
            _run_due_scheduled_tasks()
        except Exception:
            logger.exception("Scheduled task loop failed")
        try:
            _cleanup_sessions()
        except Exception:
            logger.exception("Session cleanup loop failed")


@app.on_event("startup")
def _start_task_scheduler() -> None:
    global _task_scheduler_started, _task_scheduler_thread
    if _task_scheduler_thread is not None and _task_scheduler_thread.is_alive():
        return
    _web_runtime_stopping.clear()
    _task_scheduler_stop.clear()
    _task_scheduler_started = True
    _task_scheduler_thread = _new_daemon_thread(target=_task_scheduler_loop)
    _task_scheduler_thread.start()


@app.on_event("shutdown")
def _shutdown_web_runtime() -> None:
    global _task_scheduler_started, _task_scheduler_thread
    _web_runtime_stopping.set()
    _task_scheduler_stop.set()
    scheduler_thread = _task_scheduler_thread
    if scheduler_thread is not None and scheduler_thread is not threading.current_thread():
        scheduler_thread.join(timeout=1)
    _task_scheduler_thread = None
    _task_scheduler_started = False
    _shutdown_sessions()


def _list_scheduled_tasks_sync(ws: str) -> dict[str, Any]:
    ws = _normalize_path_input(ws) or "default.ws"
    with _scheduled_task_store_lock(ws):
        tasks, error = _read_scheduled_tasks(ws)
    if error:
        return {"success": False, "error": error}
    return {"success": True, "data": tasks}


@app.get("/api/tasks")
async def list_scheduled_tasks(ws: str = "default.ws"):
    return await _run_blocking(_list_scheduled_tasks_sync, ws)


def _create_scheduled_task_sync(request: ScheduledTaskWriteRequest) -> dict[str, Any]:
    task, error = _normalize_scheduled_task(request)
    if error or task is None:
        return {"success": False, "error": error}
    ws = task["workspace"]
    with _scheduled_task_store_lock(ws):
        tasks, read_error = _read_scheduled_tasks(ws)
        if read_error:
            return {"success": False, "error": read_error}
        tasks.append(task)
        write_error = _write_scheduled_tasks(ws, tasks)
    if write_error:
        return {"success": False, "error": write_error}
    return {"success": True, "data": task}


@app.post("/api/tasks")
async def create_scheduled_task(request: ScheduledTaskWriteRequest):
    return await _run_blocking(_create_scheduled_task_sync, request)


def _update_scheduled_task_sync(
    task_id: str,
    request: ScheduledTaskWriteRequest,
) -> dict[str, Any]:
    if not _valid_task_id(task_id):
        return {"success": False, "error": "Invalid task id"}
    ws = _normalize_path_input(request.ws) or "default.ws"
    with _scheduled_task_store_lock(ws):
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


@app.put("/api/tasks/{task_id}")
async def update_scheduled_task(task_id: str, request: ScheduledTaskWriteRequest):
    return await _run_blocking(_update_scheduled_task_sync, task_id, request)


def _set_scheduled_task_status_sync(
    task_id: str,
    request: ScheduledTaskStatusRequest,
) -> dict[str, Any]:
    if not _valid_task_id(task_id):
        return {"success": False, "error": "Invalid task id"}
    ws = _normalize_path_input(request.ws) or "default.ws"
    status = _normalize_path_input(request.status)
    if status not in _TASK_STATUSES:
        return {"success": False, "error": "Invalid status"}
    with _scheduled_task_store_lock(ws):
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
                    return {
                        "success": False,
                        "error": _web_error_message(exc),
                    }
            write_error = _write_scheduled_tasks(ws, tasks)
            if write_error:
                return {"success": False, "error": write_error}
            return {"success": True, "data": task}
    return {"success": False, "error": "Task not found"}


@app.post("/api/tasks/{task_id}/status")
async def set_scheduled_task_status(task_id: str, request: ScheduledTaskStatusRequest):
    return await _run_blocking(_set_scheduled_task_status_sync, task_id, request)


def _delete_scheduled_task_sync(task_id: str, ws: str) -> dict[str, Any]:
    if not _valid_task_id(task_id):
        return {"success": False, "error": "Invalid task id"}
    ws = _normalize_path_input(ws) or "default.ws"
    with _scheduled_task_store_lock(ws):
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


@app.delete("/api/tasks/{task_id}")
async def delete_scheduled_task(task_id: str, ws: str = "default.ws"):
    return await _run_blocking(_delete_scheduled_task_sync, task_id, ws)


def _run_scheduled_task_now_sync(task_id: str, ws: str) -> dict[str, Any]:
    with _scheduled_task_store_lock(ws):
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


@app.post("/api/tasks/{task_id}/run")
async def run_scheduled_task_now(task_id: str, request: ScheduledTaskRunRequest):
    if not _valid_task_id(task_id):
        return {"success": False, "error": "Invalid task id"}
    ws = _normalize_path_input(request.ws) or "default.ws"
    return await _run_blocking(_run_scheduled_task_now_sync, task_id, ws)


@app.get("/api/workspace/list")
async def list_workspaces():
    """List all .ws workspace folders"""
    identity = _current_web_identity()
    if not _get_web_identity_provider().is_local:
        return {
            "success": True,
            "data": [alias for alias, _ in identity.workspace_aliases],
        }
    workspaces = []
    if os.path.isdir(_WORKSPACE_ROOT):
        for name in sorted(os.listdir(_WORKSPACE_ROOT)):
            if name.endswith(".ws") and os.path.isdir(os.path.join(_WORKSPACE_ROOT, name)):
                workspaces.append(name)
    return {"success": True, "data": workspaces}


@app.get("/api/workspace/templates")
async def list_workspace_template_options():
    """List available workspace templates."""
    return {"success": True, "data": list_workspace_templates()}


@app.post("/api/workspace")
async def create_workspace(request: WorkspaceCreateRequest):
    """Create a new workspace from a template."""
    if not _get_web_identity_provider().is_local:
        return {
            "success": False,
            "error": "Workspace creation requires server-side provisioning",
        }
    try:
        data = await _run_blocking(
            create_workspace_from_template,
            _WORKSPACE_ROOT,
            request.name,
            request.template_id or "blank",
        )
    except ValueError as exc:
        return {"success": False, "error": _web_error_message(exc)}
    except FileExistsError as exc:
        return {"success": False, "error": _web_error_message(exc)}
    except OSError as exc:
        return {"success": False, "error": _web_error_message(exc)}
    return {"success": True, "data": data}


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
                profile_data = load_agent_profile_document(agent_path, name)
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
        return {
            "success": True,
            "data": load_agent_profile_document(agent_path, agent),
        }
    except (OSError, UnicodeDecodeError) as exc:
        return {"success": False, "error": _web_error_message(exc)}


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
        return {"success": False, "error": _web_error_message(exc)}


@app.get("/api/workspace/teams")
async def list_teams(ws: str = "default.ws"):
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    return {"success": True, "data": list_team_configs(ws_root)}


@app.get("/api/workspace/team")
async def read_team(ws: str = "default.ws", team: str = ""):
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    config, error = read_team_config(ws_root, team)
    if error or config is None:
        return {"success": False, "error": error}
    return {"success": True, "data": config}


@app.put("/api/workspace/team")
async def write_team(request: TeamWriteRequest):
    ws_root, error = _workspace_root(request.ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    config, error = write_team_config(ws_root, request.team)
    if error or config is None:
        return {"success": False, "error": error}
    return {"success": True, "data": config}


@app.delete("/api/workspace/team")
async def delete_team(ws: str = "default.ws", team: str = ""):
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    error = delete_team_config(ws_root, team)
    if error:
        return {"success": False, "error": error}
    return {"success": True, "data": None}


@app.get("/api/workspace/team/workflow")
async def read_team_workflow_api(ws: str = "default.ws", team: str = ""):
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    config, error = read_team_config(ws_root, team)
    if error or config is None:
        return {"success": False, "error": error}
    workflow, error = read_team_workflow(ws_root, team, config)
    if error:
        return {"success": False, "error": error}
    return {"success": True, "data": workflow}


@app.put("/api/workspace/team/workflow")
async def write_team_workflow_api(request: TeamWorkflowWriteRequest):
    ws_root, error = _workspace_root(request.ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    config, error = read_team_config(ws_root, request.team)
    if error or config is None:
        return {"success": False, "error": error}
    workflow, error = write_team_workflow(ws_root, request.team, request.workflow, config)
    if error or workflow is None:
        return {"success": False, "error": error}
    return {"success": True, "data": workflow}


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
    identity = _current_web_identity()
    ws_root, error = _workspace_root(ws, identity=identity)
    if error or ws_root is None:
        return {"success": False, "error": error}
    if not os.path.isdir(ws_root):
        return {"success": True, "data": {"name": ws, "path": ws, "type": "dir", "children": []}}
    try:
        secure = not _get_web_identity_provider().is_local
        return {
            "success": True,
            "data": await _run_blocking(
                _build_dir_tree,
                ws_root,
                include_memory=(
                    not secure or MEMORY_READ_SCOPE in identity.scopes
                ),
                include_runtime=not secure,
            ),
        }
    except OSError as exc:
        return {"success": False, "error": _web_error_message(exc)}


@app.get("/api/workspace/index/stats")
async def read_workspace_index_stats(ws: str = "default.ws"):
    """Return file index status for a workspace."""
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    result = await _run_blocking(
        get_file_index_stats,
        cwd=ws_root,
        principal=_workspace_index_principal(ws),
    )
    if result.get("status") != "OK":
        return {"success": False, "error": result.get("error", "failed to read index stats")}
    return {"success": True, "data": result}


@app.post("/api/workspace/index/refresh")
async def refresh_workspace_index(request: WorkspaceIndexRefreshRequest):
    """Refresh the file index for a workspace."""
    ws_root, error = _workspace_root(request.ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    config_path, _, config_error = _effective_web_config_paths(
        request.config_path,
        "",
    )
    if config_error:
        return {"success": False, "error": config_error}
    embedding_config = await _run_blocking(
        _file_index_embedding_config_from_path,
        config_path,
    )
    result = await _run_blocking(
        refresh_file_index,
        root=request.root,
        cwd=ws_root,
        semantic=request.semantic,
        embedding_config=embedding_config,
        principal=_workspace_index_principal(request.ws),
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
    config_path, _, config_error = _effective_web_config_paths(
        config_path,
        "",
    )
    if config_error:
        return {"success": False, "error": config_error}
    embedding_config = await _run_blocking(
        _file_index_embedding_config_from_path,
        config_path,
    )
    result = await _run_blocking(
        search_file_index,
        query=q,
        cwd=ws_root,
        root=root,
        limit=limit,
        refresh=refresh,
        path_only=path_only,
        mode=mode,
        embedding_config=embedding_config,
        principal=_workspace_index_principal(ws),
    )
    if result.get("status") != "OK":
        return {"success": False, "error": result.get("error", "failed to search index"), "data": result}
    return {"success": True, "data": result}


def _preview_workspace_file_sync(
    ws: str,
    path: str,
    identity: WebIdentity | None = None,
) -> dict[str, Any]:
    real_path, normalized_path, error = _resolve_workspace_preview_path(
        ws,
        path,
        identity=identity,
    )
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
        return {"success": False, "error": _web_error_message(exc)}


@app.get("/api/workspace/preview")
async def preview_workspace_file(ws: str = "default.ws", path: str = ""):
    """Read a workspace text file for read-only preview."""
    return await _run_blocking(
        _preview_workspace_file_sync,
        ws,
        path,
        _current_web_identity(),
    )


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


def _read_workspace_file_sync(
    ws: str,
    path: str,
    identity: WebIdentity | None = None,
) -> dict[str, Any]:
    real_path, normalized_path, error = _resolve_system_file_path(
        ws,
        path,
        identity=identity,
    )
    if error or real_path is None or normalized_path is None:
        return {"success": False, "error": error}
    if not os.path.isfile(real_path):
        return {"success": False, "error": "File not found"}
    try:
        with open(real_path, "r", encoding="utf-8") as f:
            content = f.read()
        return {"success": True, "data": {"path": normalized_path, "content": content}}
    except (OSError, UnicodeDecodeError) as e:
        return {"success": False, "error": _web_error_message(e)}


@app.get("/api/workspace/file")
async def read_workspace_file(ws: str = "default.ws", path: str = ""):
    """Read a file from workspace system/ directory"""
    return await _run_blocking(
        _read_workspace_file_sync,
        ws,
        path,
        _current_web_identity(),
    )


def _write_workspace_file_sync(
    ws: str,
    path: str,
    content: str,
    identity: WebIdentity | None = None,
) -> dict[str, Any]:
    real_path, normalized_path, error = _resolve_system_file_path(
        ws,
        path,
        identity=identity,
        write=True,
    )
    if error or real_path is None or normalized_path is None:
        return {"success": False, "error": error}
    if os.path.isdir(real_path):
        return {"success": False, "error": "Path is a directory"}

    ws_root, workspace_error = _workspace_root(ws, identity=identity)
    if workspace_error or ws_root is None:
        return {"success": False, "error": workspace_error}
    try:
        with workspace_write_lock(ws_root):
            created = not os.path.exists(real_path)
            atomic_write_text(real_path, content)
        return {
            "success": True,
            "data": {
                "path": normalized_path,
                "content": content,
                "bytes": len(content.encode("utf-8")),
                "created": created,
            },
        }
    except (OSError, UnicodeError) as e:
        return {"success": False, "error": _web_error_message(e)}


@app.put("/api/workspace/file")
async def write_workspace_file(request: WorkspaceFileWriteRequest):
    """Write a file to workspace system/ directory"""
    return await _run_blocking(
        _write_workspace_file_sync,
        request.ws,
        request.path,
        request.content,
        _current_web_identity(),
    )


def _delete_workspace_file_sync(
    ws: str,
    path: str,
    identity: WebIdentity | None = None,
) -> dict[str, Any]:
    real_path, normalized_path, error = _resolve_system_file_path(
        ws,
        path,
        identity=identity,
        write=True,
    )
    if error or real_path is None or normalized_path is None:
        return {"success": False, "error": error}

    ws_root, workspace_error = _workspace_root(ws, identity=identity)
    if workspace_error or ws_root is None:
        return {"success": False, "error": workspace_error}
    lexical_path = os.path.abspath(os.path.join(ws_root, normalized_path))
    system_root = os.path.abspath(os.path.join(ws_root, "system"))
    if os.path.commonpath([system_root, lexical_path]) != system_root:
        return {"success": False, "error": "Only system/ files can be managed via this endpoint"}

    try:
        with workspace_write_lock(ws_root):
            if os.path.islink(lexical_path):
                return {"success": False, "error": "Symbolic links cannot be deleted via this endpoint"}
            if not os.path.exists(lexical_path):
                return {"success": False, "error": "File not found"}
            if not os.path.isfile(lexical_path):
                return {"success": False, "error": "Path is not a regular file"}
            os.remove(lexical_path)
    except OSError:
        return {"success": False, "error": "Failed to delete file"}
    return {"success": True, "data": {"path": normalized_path}}


@app.delete("/api/workspace/file")
async def delete_workspace_file(ws: str = "default.ws", path: str = ""):
    """Delete a regular file from workspace system/ directory."""
    return await _run_blocking(
        _delete_workspace_file_sync,
        ws,
        path,
        _current_web_identity(),
    )


# === Eval API ===


def _web_eval_storage_root(
    ws_root: str,
    identity: WebIdentity | None = None,
) -> str | None:
    if _get_web_identity_provider().is_local:
        return None
    caller = identity or _current_web_identity()
    return os.path.join(
        ws_root,
        "runtime",
        "eval",
        "_owners",
        caller.owner_digest,
    )


def _eval_agent_factory(
    *,
    ws: str,
    ws_root: str,
    agent_name: str,
    config_path: str,
    observability_config_path: str,
    runtime_config: dict[str, Any] | None,
    principal_template: Principal | None = None,
):
    runtime_config = runtime_config or {}

    def _intersect_allowlist(
        configured: Any,
        requested: tuple[str, ...],
    ) -> list[str]:
        if configured is None:
            return list(requested)
        configured_names = {
            str(item).strip()
            for item in configured
            if str(item).strip()
        }
        return [name for name in requested if name in configured_names]

    def _factory(case_runtime: EvalCaseRuntime | None = None):
        effective_workspace = ws_root
        effective_principal = principal_template
        effective_tools = runtime_config.get("tools_allowlist")
        effective_skills = runtime_config.get("skill_allowlist")
        effective_memory_mode = str(runtime_config.get("memory_mode", "project"))
        effective_max_turns = runtime_config.get("max_turns")
        if case_runtime is not None:
            if not set(case_runtime.scopes).issubset(
                SCENARIO_PACK_V1_SCOPES
            ):
                raise EvalError("Scenario pack contains a non-isolated scope")
            if not set(case_runtime.tools_allowlist).issubset(
                SCENARIO_PACK_V1_TOOLS
            ):
                raise EvalError("Scenario pack contains a non-isolated tool")
            if not isinstance(principal_template, Principal):
                raise EvalError(
                    "Scenario pack execution requires an authenticated principal"
                )
            if not set(case_runtime.scopes).issubset(
                set(principal_template.scopes)
            ):
                raise EvalError(
                    "Scenario pack scopes exceed the caller principal"
                )
            effective_workspace = case_runtime.workspace_root
            effective_principal = replace(
                principal_template,
                scopes=case_runtime.scopes,
            )
            effective_tools = _intersect_allowlist(
                effective_tools,
                case_runtime.tools_allowlist,
            )
            effective_skills = _intersect_allowlist(
                effective_skills,
                case_runtime.skill_allowlist,
            )
            effective_memory_mode = case_runtime.memory_mode
            configured_max_turns = runtime_config.get("max_turns")
            effective_max_turns = (
                min(case_runtime.max_turns, int(configured_max_turns))
                if isinstance(configured_max_turns, int)
                and not isinstance(configured_max_turns, bool)
                and configured_max_turns > 0
                else case_runtime.max_turns
            )
        agent = build_agent(
            config_path=config_path or None,
            observability_config_path=observability_config_path or None,
            workspace_dir=effective_workspace,
            agent_name=agent_name,
            agent_prompt=str(runtime_config.get("agent_prompt", "")),
            agent_soul=str(runtime_config.get("agent_soul", "")),
            tools_allowlist=effective_tools,
            skill_allowlist=effective_skills,
            model_override=str(runtime_config.get("model_override", "")),
            max_turns=effective_max_turns,
            memory_mode=effective_memory_mode,
            principal=effective_principal,
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
    owner_digest: str,
    storage_root: str | None,
    redact_errors: bool,
    cancel_event: threading.Event,
    agent_factory,
) -> None:
    try:
        execute_eval_run(
            ws_root,
            run_id,
            agent_factory=lambda: agent_factory(None),
            case_agent_factory=agent_factory,
            cancel_event=cancel_event,
            storage_root=storage_root,
            redact_errors=redact_errors,
        )
    finally:
        with _eval_lock:
            _eval_cancel_events.pop((owner_digest, run_id), None)


@app.get("/api/eval/datasets")
async def api_list_eval_datasets(ws: str = "default.ws"):
    identity = _current_web_identity()
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    imported_items, workspace_datasets = await asyncio.gather(
        _run_blocking(
            list_datasets,
            ws_root,
            storage_root=_web_eval_storage_root(ws_root, identity),
        ),
        _run_blocking(list_workspace_eval_datasets, ws_root),
    )
    imported = [{**item, "imported": True} for item in imported_items]
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
    return {
        "success": True,
        "data": _secure_trace_payload(
            visible_imported + visible_workspace_datasets
        ),
    }


@app.post("/api/eval/datasets/import")
async def api_import_eval_dataset(request: EvalDatasetImportRequest):
    identity = _current_web_identity()
    ws_root, error = _workspace_root(request.ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    try:
        if request.path.strip():
            data = await _run_blocking(
                import_dataset_path,
                ws_root,
                rel_path=request.path,
                name=request.name,
                fmt=request.format,
                storage_root=_web_eval_storage_root(ws_root, identity),
                owner_digest=identity.owner_digest,
            )
        else:
            if not request.content:
                return {"success": False, "error": "content or path is required"}
            data = await _run_blocking(
                import_dataset_content,
                ws_root,
                name=request.name or "dataset",
                content=request.content,
                fmt=request.format,
                source={"type": "content"},
                storage_root=_web_eval_storage_root(ws_root, identity),
                owner_digest=identity.owner_digest,
            )
        return {"success": True, "data": _secure_trace_payload(data)}
    except (EvalError, OSError, UnicodeError) as exc:
        return {"success": False, "error": _web_error_message(exc)}


@app.post("/api/eval/datasets/download")
async def api_download_eval_dataset(request: EvalDatasetDownloadRequest):
    identity = _current_web_identity()
    ws_root, error = _workspace_root(request.ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    try:
        data = await _run_blocking(
            download_dataset,
            ws_root,
            url=request.url,
            name=request.name,
            fmt=request.format,
            storage_root=_web_eval_storage_root(ws_root, identity),
            owner_digest=identity.owner_digest,
        )
        return {"success": True, "data": _secure_trace_payload(data)}
    except (EvalError, OSError, UnicodeError) as exc:
        return {"success": False, "error": _web_error_message(exc)}


@app.get("/api/eval/datasets/{dataset_id}")
async def api_get_eval_dataset(dataset_id: str, ws: str = "default.ws"):
    identity = _current_web_identity()
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    try:
        data = await _run_blocking(
            get_dataset_detail,
            ws_root,
            dataset_id,
            storage_root=_web_eval_storage_root(ws_root, identity),
        )
        return {"success": True, "data": _secure_trace_payload(data)}
    except (EvalError, OSError, json.JSONDecodeError) as exc:
        return {"success": False, "error": _web_error_message(exc)}


@app.get("/api/eval/runs")
async def api_list_eval_runs(ws: str = "default.ws"):
    identity = _current_web_identity()
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    data = await _run_blocking(
        list_eval_runs,
        ws_root,
        storage_root=_web_eval_storage_root(ws_root, identity),
    )
    return {"success": True, "data": _secure_eval_run_payload(data)}


@app.post("/api/eval/runs")
async def api_create_eval_run(request: EvalRunCreateRequest):
    identity = _current_web_identity()
    ws_root, error = _workspace_root(request.ws)
    if error or ws_root is None:
        return {"success": False, "error": error}

    agent_name = _normalize_path_input(request.agent)
    runtime_config, runtime_error = await _run_blocking(
        _agent_runtime_config,
        request.ws,
        agent_name,
    )
    if runtime_error:
        return {"success": False, "error": runtime_error}

    case_limit = request.case_limit if request.case_limit > 0 else None
    if case_limit is not None and case_limit > 500:
        return {"success": False, "error": "case_limit cannot exceed 500"}
    config_path, observability_config_path, config_error = (
        _effective_web_config_paths(
            request.config_path,
            request.observability_config_path,
        )
    )
    if config_error:
        return {"success": False, "error": config_error}

    try:
        result = await _run_blocking(
            create_eval_run,
            ws_root,
            workspace=request.ws,
            dataset_id=request.dataset_id,
            agent=agent_name,
            case_limit=case_limit,
            storage_root=_web_eval_storage_root(ws_root, identity),
            owner_digest=identity.owner_digest,
        )
    except (EvalError, OSError, json.JSONDecodeError) as exc:
        return {"success": False, "error": _web_error_message(exc)}

    cancel_event = threading.Event()
    with _eval_lock:
        _eval_cancel_events[(identity.owner_digest, result["id"])] = cancel_event

    agent_factory = _eval_agent_factory(
        ws=request.ws,
        ws_root=ws_root,
        agent_name=agent_name,
        config_path=config_path,
        observability_config_path=observability_config_path,
        runtime_config=runtime_config,
        principal_template=identity.principal(
            session_id=f"eval-{result['id']}",
            agent_id=agent_name or "main",
        ),
    )
    thread = _new_daemon_thread(
        target=_run_eval_background,
        kwargs={
            "ws_root": ws_root,
            "run_id": result["id"],
            "owner_digest": identity.owner_digest,
            "storage_root": _web_eval_storage_root(ws_root, identity),
            "redact_errors": not _get_web_identity_provider().is_local,
            "cancel_event": cancel_event,
            "agent_factory": agent_factory,
        },
    )
    thread.start()
    return {"success": True, "data": _secure_eval_run_payload(result)}


@app.get("/api/eval/runs/{run_id}")
async def api_get_eval_run(run_id: str, ws: str = "default.ws"):
    identity = _current_web_identity()
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    try:
        data = await _run_blocking(
            read_eval_run,
            ws_root,
            run_id,
            storage_root=_web_eval_storage_root(ws_root, identity),
        )
        return {"success": True, "data": _secure_eval_run_payload(data)}
    except (EvalError, OSError, json.JSONDecodeError) as exc:
        return {"success": False, "error": _web_error_message(exc)}


@app.post("/api/eval/runs/{run_id}/cancel")
async def api_cancel_eval_run(run_id: str, ws: str = "default.ws"):
    identity = _current_web_identity()
    ws_root, error = _workspace_root(ws)
    if error or ws_root is None:
        return {"success": False, "error": error}
    try:
        result = await _run_blocking(
            read_eval_run,
            ws_root,
            run_id,
            storage_root=_web_eval_storage_root(ws_root, identity),
        )
    except (EvalError, OSError, json.JSONDecodeError) as exc:
        return {"success": False, "error": _web_error_message(exc)}

    with _eval_lock:
        cancel_event = _eval_cancel_events.get((identity.owner_digest, run_id))
        if cancel_event is not None:
            cancel_event.set()

    if result.get("status") in {"pending", "running"}:
        result["status"] = "canceling"
        await _run_blocking(
            write_eval_run,
            ws_root,
            result,
            storage_root=_web_eval_storage_root(ws_root, identity),
        )
    return {"success": True, "data": _secure_eval_run_payload(result)}


# Serve static files (frontend build)
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse


def _resolve_frontend_file_path(root: str, request_path: str) -> tuple[str | None, bool]:
    real_root = os.path.realpath(root)
    candidate = os.path.realpath(os.path.join(real_root, request_path))
    try:
        contained = os.path.commonpath([real_root, candidate]) == real_root
    except ValueError:
        contained = False
    if not contained:
        return None, False
    return (candidate if os.path.isfile(candidate) else None), True


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
        file_path, contained = _resolve_frontend_file_path(build_dir, path)
        if not contained:
            return JSONResponse(status_code=404, content={"error": "Not found"})
        if file_path is not None:
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
