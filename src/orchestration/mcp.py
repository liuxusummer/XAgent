"""Strict stateless MCP 2026-07-28 adapter for orchestration.

The adapter deliberately exposes only the durable orchestration runtime.  It
does not ingest bytes, mint Artifact references, manage HTTP authorization, or
implement the Tasks extension.  Transport code must inject both a stable,
non-secret rate-limit context identifier and the trusted authorization context
for every request.

MCP 2026-07-28 has no initialize handshake or protocol session.  Consequently
this server validates protocol metadata on every request and never infers
client capabilities, identity, or authorization from a previous message.
"""

from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from .artifacts import (
    ARTIFACT_SCHEMA_VERSION,
    ArtifactEncryption,
    ArtifactKind,
    ArtifactSensitivity,
)
from .protocol import (
    MAX_CONTROL_STEPS,
    MAX_EVENT_PAGE_SIZE,
    MAX_RESPONSE_BYTES,
    PROTOCOL_VERSION as RUNTIME_PROTOCOL_VERSION,
    ProtocolResponse,
)

MCP_PROTOCOL_VERSION = "2026-07-28"
SUPPORTED_PROTOCOL_VERSIONS = (MCP_PROTOCOL_VERSION,)
JSON_SCHEMA_2020_12 = "https://json-schema.org/draft/2020-12/schema"
MAX_MCP_MESSAGE_BYTES = 1024 * 1024
MAX_MCP_JSON_DEPTH = 32
MAX_MCP_JSON_ITEMS = 4096
MAX_REQUEST_META_BYTES = 32 * 1024
MAX_CLIENT_CAPABILITIES_BYTES = 16 * 1024
MAX_CLIENT_CAPABILITIES_DEPTH = 8
MAX_CLIENT_CAPABILITIES_ITEMS = 128
TOOLS_CACHE_TTL_MS = 300_000
DISCOVERY_CACHE_TTL_MS = 300_000

_PROTOCOL_VERSION_KEY = "io.modelcontextprotocol/protocolVersion"
_CLIENT_INFO_KEY = "io.modelcontextprotocol/clientInfo"
_CLIENT_CAPABILITIES_KEY = "io.modelcontextprotocol/clientCapabilities"
_SERVER_INFO_KEY = "io.modelcontextprotocol/serverInfo"
_REQUEST_FIELDS = frozenset({"jsonrpc", "id", "method", "params"})
_REQUIRED_META_FIELDS = frozenset(
    {
        _PROTOCOL_VERSION_KEY,
        _CLIENT_CAPABILITIES_KEY,
    }
)
_CLIENT_INFO_FIELDS = frozenset(
    {
        "icons",
        "name",
        "title",
        "version",
        "description",
        "websiteUrl",
    }
)
_ICON_FIELDS = frozenset({"src", "mimeType", "sizes", "theme"})
_LOG_LEVELS = frozenset(
    {
        "debug",
        "info",
        "notice",
        "warning",
        "error",
        "critical",
        "alert",
        "emergency",
    }
)
_TOOL_NAMES = (
    "submit",
    "status",
    "events",
    "cancel",
    "pause",
    "resume",
    "recover",
    "tick",
)
_SAFE_RUNTIME_ERROR_MESSAGES = {
    "authorization_denied": "operation is not authorized",
    "run_not_found": "run was not found",
    "capability_unavailable": "required runtime capability is unavailable",
    "invalid_request": "tool input does not match the runtime protocol",
    "invalid_input": "tool input is invalid",
    "operation_failed": "operation outcome was not confirmed; query durable status",
}
_SERVER_INFO = {
    "name": "xagent-durable-orchestration",
    "version": "1.0.0",
}
_RESULT_META = {_SERVER_INFO_KEY: _SERVER_INFO}


class _RPCFault(Exception):
    def __init__(
        self,
        code: int,
        message: str,
        *,
        data: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = None if data is None else dict(data)


class _UnsupportedVersion(_RPCFault):
    def __init__(self, requested: str) -> None:
        super().__init__(
            -32022,
            "Unsupported protocol version",
            data={
                "supported": list(SUPPORTED_PROTOCOL_VERSIONS),
                "requested": requested,
            },
        )


@runtime_checkable
class OrchestrationRuntimeLike(Protocol):
    def handle(
        self,
        payload: Mapping[str, Any],
        *,
        authorization_context: Any = None,
    ) -> ProtocolResponse: ...


@dataclass(slots=True)
class _RateState:
    window_started: float
    count: int


@dataclass(slots=True)
class _TokenBucketState:
    tokens: float
    updated_at: float


class PerContextRateLimiter:
    """Bounded fixed-window limiter keyed by a digest of transport context.

    Context identifiers are trusted transport inputs, not MCP client metadata.
    Only their digest is retained, avoiding accidental storage of a credential
    if a transport is misconfigured and supplies one as the identifier.
    """

    def __init__(
        self,
        *,
        limit: int = 120,
        window_seconds: float = 60.0,
        max_contexts: int = 2048,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or limit < 1
            or isinstance(max_contexts, bool)
            or not isinstance(max_contexts, int)
            or max_contexts < 1
            or not isinstance(window_seconds, (int, float))
            or isinstance(window_seconds, bool)
            or not math.isfinite(float(window_seconds))
            or window_seconds <= 0
            or not callable(clock)
        ):
            raise ValueError("invalid rate limiter configuration")
        self.limit = limit
        self.window_seconds = float(window_seconds)
        self.max_contexts = max_contexts
        self._clock = clock
        self._states: OrderedDict[str, _RateState] = OrderedDict()
        self._lock = threading.Lock()

    def allow(self, context_id: str | bytes | None) -> tuple[bool, int]:
        key = self._context_digest(context_id)
        now = float(self._clock())
        if not math.isfinite(now):
            now = 0.0
        with self._lock:
            state = self._states.pop(key, None)
            if state is None or now - state.window_started >= self.window_seconds:
                state = _RateState(window_started=now, count=0)
            if state.count >= self.limit:
                self._states[key] = state
                retry = max(
                    1,
                    math.ceil(
                        (self.window_seconds - (now - state.window_started))
                        * 1000
                    ),
                )
                return False, retry
            state.count += 1
            self._states[key] = state
            while len(self._states) > self.max_contexts:
                self._states.popitem(last=False)
            return True, 0

    @staticmethod
    def _context_digest(context_id: str | bytes | None) -> str:
        if context_id is None:
            material = b"anonymous"
        elif isinstance(context_id, bytes):
            material = context_id[:512]
        elif isinstance(context_id, str):
            material = context_id.encode("utf-8")[:512]
        else:
            material = b"invalid-context"
        return hashlib.sha256(material).hexdigest()


class InvalidInputTokenBucket:
    """Independent token bucket for structurally invalid transport input.

    Valid requests refund their speculative token before entering the normal
    request limiter.  Context identifiers are transport-trusted and digested;
    anonymous traffic shares one stable bucket.  Once the bounded context table
    is full, unseen identifiers share an overflow bucket instead of evicting
    state and resetting their budget.
    """

    _OVERFLOW_KEY = hashlib.sha256(b"invalid-input-overflow").hexdigest()

    def __init__(
        self,
        *,
        capacity: int = 30,
        refill_seconds: float = 60.0,
        max_contexts: int = 2048,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if (
            isinstance(capacity, bool)
            or not isinstance(capacity, int)
            or capacity < 1
            or isinstance(max_contexts, bool)
            or not isinstance(max_contexts, int)
            or max_contexts < 1
            or isinstance(refill_seconds, bool)
            or not isinstance(refill_seconds, (int, float))
            or not math.isfinite(float(refill_seconds))
            or refill_seconds <= 0
            or not callable(clock)
        ):
            raise ValueError("invalid input token bucket configuration")
        self.capacity = capacity
        self.refill_seconds = float(refill_seconds)
        self.max_contexts = max_contexts
        self._clock = clock
        self._states: OrderedDict[str, _TokenBucketState] = OrderedDict()
        self._lock = threading.Lock()

    def allow(self, context_id: str | bytes | None) -> tuple[bool, int]:
        context_key = PerContextRateLimiter._context_digest(context_id)
        now = self._now()
        with self._lock:
            key = self._state_key(context_key)
            state = self._refilled_state(key, now)
            if state.tokens < 1.0:
                self._states[key] = state
                retry = max(
                    1,
                    math.ceil(
                        ((1.0 - state.tokens) * self.refill_seconds)
                        / self.capacity
                        * 1000
                    ),
                )
                return False, retry
            state.tokens -= 1.0
            self._states[key] = state
            return True, 0

    def refund(self, context_id: str | bytes | None) -> None:
        context_key = PerContextRateLimiter._context_digest(context_id)
        now = self._now()
        with self._lock:
            key = self._state_key(context_key)
            state = self._refilled_state(key, now)
            state.tokens = min(float(self.capacity), state.tokens + 1.0)
            self._states[key] = state

    def _state_key(self, context_key: str) -> str:
        if context_key in self._states:
            return context_key
        if (
            len(self._states) < self.max_contexts - 1
            and self._OVERFLOW_KEY not in self._states
        ):
            return context_key
        return self._OVERFLOW_KEY

    def _refilled_state(self, key: str, now: float) -> _TokenBucketState:
        state = self._states.pop(key, None)
        if state is None:
            return _TokenBucketState(tokens=float(self.capacity), updated_at=now)
        elapsed = max(0.0, now - state.updated_at)
        state.tokens = min(
            float(self.capacity),
            state.tokens
            + elapsed * (self.capacity / self.refill_seconds),
        )
        state.updated_at = now
        return state

    def _now(self) -> float:
        now = float(self._clock())
        return now if math.isfinite(now) else 0.0


class MCPServer:
    """One-message-at-a-time JSON-RPC server with no connection state."""

    def __init__(
        self,
        runtime: OrchestrationRuntimeLike,
        *,
        rate_limiter: PerContextRateLimiter | None = None,
        invalid_input_limiter: InvalidInputTokenBucket | None = None,
    ) -> None:
        if not isinstance(runtime, OrchestrationRuntimeLike):
            raise TypeError("runtime must provide the orchestration handle contract")
        self._runtime = runtime
        self._rate_limiter = rate_limiter or PerContextRateLimiter()
        self._invalid_input_limiter = (
            invalid_input_limiter or InvalidInputTokenBucket()
        )

    def handle_json(
        self,
        raw_message: str | bytes,
        *,
        context_id: str | bytes | None,
        authorization_context: Any = None,
    ) -> dict[str, Any]:
        """Decode and handle exactly one JSON-RPC message."""

        invalid_allowed, retry_after_ms = self._invalid_input_limiter.allow(
            context_id
        )
        if not invalid_allowed:
            return _invalid_input_rate_limit_response(None, retry_after_ms)
        if isinstance(raw_message, str):
            try:
                raw_bytes = raw_message.encode("utf-8")
            except UnicodeEncodeError:
                return _error_response(None, -32700, "Parse error")
        elif isinstance(raw_message, bytes):
            raw_bytes = raw_message
        else:
            return _error_response(None, -32600, "Invalid Request")
        if len(raw_bytes) > MAX_MCP_MESSAGE_BYTES:
            return _error_response(None, -32600, "Invalid Request")
        try:
            decoded = json.loads(
                raw_bytes.decode("utf-8"),
                object_pairs_hook=_strict_json_object,
            )
        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
            ValueError,
            RecursionError,
        ):
            return _error_response(None, -32700, "Parse error")
        return self.handle(
            decoded,
            context_id=context_id,
            authorization_context=authorization_context,
            _invalid_budget_reserved=True,
        )

    def handle(
        self,
        message: Any,
        *,
        context_id: str | bytes | None,
        authorization_context: Any = None,
        _invalid_budget_reserved: bool = False,
    ) -> dict[str, Any]:
        """Handle one detached JSON object and return one JSON-RPC response."""

        request_id = _readable_request_id(message)
        if not _invalid_budget_reserved:
            invalid_allowed, retry_after_ms = self._invalid_input_limiter.allow(
                context_id
            )
            if not invalid_allowed:
                return _invalid_input_rate_limit_response(
                    request_id,
                    retry_after_ms,
                )
        if not _bounded_json_tree(
            message,
            max_depth=MAX_MCP_JSON_DEPTH,
            max_items=MAX_MCP_JSON_ITEMS,
        ):
            return _error_response(request_id, -32600, "Invalid Request")
        try:
            encoded = _canonical_json(message)
        except ValueError:
            return _error_response(request_id, -32600, "Invalid Request")
        if len(encoded) > MAX_MCP_MESSAGE_BYTES:
            return _error_response(request_id, -32600, "Invalid Request")
        self._invalid_input_limiter.refund(context_id)
        allowed, retry_after_ms = self._rate_limiter.allow(context_id)
        if not allowed:
            # -32020..-32099 is reserved by MCP.  -32900 is deliberately
            # outside JSON-RPC's reserved server-error range.
            return _error_response(
                request_id,
                -32900,
                "Rate limit exceeded",
                data={"retryAfterMs": retry_after_ms},
            )
        try:
            request_id, method, params = _parse_request_envelope(message)
            _validate_request_meta(params)
            if method == "server/discover":
                _exact_fields(params, {"_meta"}, "server/discover params")
                return _result_response(request_id, _discover_result())
            if method == "tools/list":
                _exact_fields(params, {"_meta"}, "tools/list params")
                return _result_response(request_id, _tools_list_result())
            if method == "tools/call":
                _exact_fields(
                    params,
                    {"_meta", "name", "arguments"},
                    "tools/call params",
                )
                return _result_response(
                    request_id,
                    self._call_tool(
                        request_id,
                        params,
                        authorization_context=authorization_context,
                    ),
                )
            raise _RPCFault(-32601, "Method not found")
        except _RPCFault as fault:
            return _error_response(
                request_id,
                fault.code,
                fault.message,
                data=fault.data,
            )
        except Exception:
            return _error_response(request_id, -32603, "Internal error")

    def _call_tool(
        self,
        request_id: str | int,
        params: Mapping[str, Any],
        *,
        authorization_context: Any,
    ) -> dict[str, Any]:
        name = params["name"]
        arguments = params["arguments"]
        if not isinstance(name, str) or name not in _TOOL_NAMES:
            raise _RPCFault(-32602, "Unknown tool")
        if not isinstance(arguments, dict) or not all(
            isinstance(key, str) for key in arguments
        ):
            raise _RPCFault(-32602, "Invalid params")
        try:
            detached_arguments = json.loads(_canonical_json(arguments))
        except (ValueError, json.JSONDecodeError):
            raise _RPCFault(-32602, "Invalid params") from None
        runtime_request_id = _runtime_request_id(request_id, name)
        runtime_payload = {
            "protocol_version": RUNTIME_PROTOCOL_VERSION,
            "request_id": runtime_request_id,
            "operation": name,
            "body": detached_arguments,
        }
        try:
            runtime_response = self._runtime.handle(
                runtime_payload,
                authorization_context=authorization_context,
            )
            if not isinstance(runtime_response, ProtocolResponse):
                return _tool_error(
                    "operation_failed",
                    _SAFE_RUNTIME_ERROR_MESSAGES["operation_failed"],
                )
            response = runtime_response.to_dict()
        except Exception:
            return _tool_error(
                "operation_failed",
                _SAFE_RUNTIME_ERROR_MESSAGES["operation_failed"],
            )
        if response.get("ok") is not True:
            error = response.get("error")
            code = (
                error.get("code")
                if isinstance(error, dict)
                and isinstance(error.get("code"), str)
                else "operation_failed"
            )
            safe_code = (
                code if code in _SAFE_RUNTIME_ERROR_MESSAGES else "operation_failed"
            )
            return _tool_error(
                safe_code,
                _SAFE_RUNTIME_ERROR_MESSAGES[safe_code],
            )
        result = response.get("result")
        projected = _project_runtime_result(name, result)
        return _tool_result(projected, is_error=False)


def _parse_request_envelope(
    message: Any,
) -> tuple[str | int, str, dict[str, Any]]:
    if not isinstance(message, dict):
        raise _RPCFault(-32600, "Invalid Request")
    _exact_fields(message, _REQUEST_FIELDS, "request")
    if message.get("jsonrpc") != "2.0":
        raise _RPCFault(-32600, "Invalid Request")
    request_id = message.get("id")
    if not _valid_request_id(request_id):
        raise _RPCFault(-32600, "Invalid Request")
    method = message.get("method")
    if (
        not isinstance(method, str)
        or not method
        or len(method) > 128
        or _has_control_character(method)
    ):
        raise _RPCFault(-32600, "Invalid Request")
    params = message.get("params")
    if not isinstance(params, dict) or not all(
        isinstance(key, str) for key in params
    ):
        raise _RPCFault(-32602, "Invalid params")
    return request_id, method, params


def _validate_request_meta(params: Mapping[str, Any]) -> None:
    meta = params.get("_meta")
    if not isinstance(meta, dict):
        raise _RPCFault(-32602, "Invalid params")
    try:
        encoded_meta = _canonical_json(meta)
    except ValueError:
        raise _RPCFault(-32602, "Invalid params") from None
    if (
        len(encoded_meta) > MAX_REQUEST_META_BYTES
        or not _bounded_json_tree(
            meta,
            max_depth=MAX_CLIENT_CAPABILITIES_DEPTH,
            max_items=MAX_CLIENT_CAPABILITIES_ITEMS,
        )
        or not all(_valid_meta_key(key) for key in meta)
        or not _REQUIRED_META_FIELDS.issubset(meta)
    ):
        raise _RPCFault(-32602, "Invalid params")
    version = meta.get(_PROTOCOL_VERSION_KEY)
    if not isinstance(version, str) or not version or len(version) > 32:
        raise _RPCFault(-32602, "Invalid params")
    if version not in SUPPORTED_PROTOCOL_VERSIONS:
        raise _UnsupportedVersion(version)
    client_info = meta.get(_CLIENT_INFO_KEY)
    if _CLIENT_INFO_KEY in meta:
        _validate_client_info(client_info)
    progress_token = meta.get("progressToken")
    if "progressToken" in meta and not _valid_progress_token(progress_token):
        raise _RPCFault(-32602, "Invalid params")
    log_level = meta.get("io.modelcontextprotocol/logLevel")
    if (
        "io.modelcontextprotocol/logLevel" in meta
        and log_level not in _LOG_LEVELS
    ):
        raise _RPCFault(-32602, "Invalid params")
    capabilities = meta.get(_CLIENT_CAPABILITIES_KEY)
    if not isinstance(capabilities, dict):
        raise _RPCFault(-32602, "Invalid params")
    try:
        encoded = _canonical_json(capabilities)
    except ValueError:
        raise _RPCFault(-32602, "Invalid params") from None
    if len(encoded) > MAX_CLIENT_CAPABILITIES_BYTES:
        raise _RPCFault(-32602, "Invalid params")
    if not _bounded_json_tree(
        capabilities,
        max_depth=MAX_CLIENT_CAPABILITIES_DEPTH,
        max_items=MAX_CLIENT_CAPABILITIES_ITEMS,
    ):
        raise _RPCFault(-32602, "Invalid params")


def _validate_client_info(value: Any) -> None:
    if not isinstance(value, dict):
        raise _RPCFault(-32602, "Invalid params")
    if (
        not {"name", "version"}.issubset(value)
        or not set(value).issubset(_CLIENT_INFO_FIELDS)
    ):
        raise _RPCFault(-32602, "Invalid params")
    for key in ("name", "version"):
        item = value.get(key)
        if (
            not isinstance(item, str)
            or not item
            or len(item) > 128
            or _has_control_character(item)
        ):
            raise _RPCFault(-32602, "Invalid params")
    for key in ("title", "description", "websiteUrl"):
        item = value.get(key)
        if item is not None and (
            not isinstance(item, str)
            or not item
            or len(item) > 2048
            or _has_control_character(item)
        ):
            raise _RPCFault(-32602, "Invalid params")
    icons = value.get("icons")
    if icons is not None:
        if not isinstance(icons, list) or len(icons) > 8:
            raise _RPCFault(-32602, "Invalid params")
        for icon in icons:
            _validate_client_icon(icon)


def _validate_client_icon(value: Any) -> None:
    if (
        not isinstance(value, dict)
        or "src" not in value
        or not set(value).issubset(_ICON_FIELDS)
    ):
        raise _RPCFault(-32602, "Invalid params")
    source = value.get("src")
    if (
        not isinstance(source, str)
        or not source
        or len(source) > 2048
        or _has_control_character(source)
    ):
        raise _RPCFault(-32602, "Invalid params")
    media_type = value.get("mimeType")
    if media_type is not None and (
        not isinstance(media_type, str)
        or not media_type
        or len(media_type) > 255
        or _has_control_character(media_type)
    ):
        raise _RPCFault(-32602, "Invalid params")
    sizes = value.get("sizes")
    if sizes is not None and (
        not isinstance(sizes, list)
        or len(sizes) > 16
        or any(
            not isinstance(size, str)
            or not size
            or len(size) > 32
            or _has_control_character(size)
            for size in sizes
        )
    ):
        raise _RPCFault(-32602, "Invalid params")
    if "theme" in value and value.get("theme") not in {"light", "dark"}:
        raise _RPCFault(-32602, "Invalid params")


def _discover_result() -> dict[str, Any]:
    return {
        "resultType": "complete",
        "supportedVersions": list(SUPPORTED_PROTOCOL_VERSIONS),
        "capabilities": {"tools": {}},
        "_meta": json.loads(_canonical_json(_RESULT_META)),
        "instructions": (
            "Use Artifact references to submit durable workflows; query explicit "
            "run identifiers for status, events, and control."
        ),
        "ttlMs": DISCOVERY_CACHE_TTL_MS,
        "cacheScope": "public",
    }


def _tools_list_result() -> dict[str, Any]:
    return {
        "resultType": "complete",
        "tools": json.loads(_canonical_json(_TOOL_DEFINITIONS)),
        "ttlMs": TOOLS_CACHE_TTL_MS,
        "cacheScope": "public",
        "_meta": json.loads(_canonical_json(_RESULT_META)),
    }


def _tool_result(structured: Mapping[str, Any], *, is_error: bool) -> dict[str, Any]:
    detached = json.loads(_canonical_json(structured))
    text = _canonical_json(detached).decode("utf-8")
    return {
        "resultType": "complete",
        "content": [{"type": "text", "text": text}],
        "structuredContent": detached,
        "isError": is_error,
        "_meta": json.loads(_canonical_json(_RESULT_META)),
    }


def _tool_error(code: str, message: str) -> dict[str, Any]:
    return _tool_result(
        {"error": {"code": code, "message": message}},
        is_error=True,
    )


def _project_runtime_result(name: str, result: Any) -> dict[str, Any]:
    """Allowlist runtime public projections before they cross MCP."""

    if not isinstance(result, dict):
        return {"outcome": "completed"}
    projected: dict[str, Any] = {}
    for key in (
        "run_id",
        "status",
        "terminal",
        "last_event_sequence",
        "projection_version",
    ):
        value = result.get(key)
        if isinstance(value, (str, int, bool)) and not isinstance(value, float):
            projected[key] = value
    for key in ("node_status_counts", "attempt_status_counts"):
        counts = result.get(key)
        if isinstance(counts, dict):
            projected[key] = {
                state: count
                for state, count in sorted(counts.items())
                if isinstance(state, str)
                and len(state) <= 64
                and not _has_control_character(state)
                and isinstance(count, int)
                and not isinstance(count, bool)
                and count >= 0
            }
    if name == "events":
        events = result.get("events")
        if isinstance(events, list):
            projected["events"] = [
                _project_event(event) for event in events[:MAX_EVENT_PAGE_SIZE]
            ]
        for key in ("next_sequence", "has_more"):
            value = result.get(key)
            if isinstance(value, (int, bool)) and not isinstance(value, float):
                projected[key] = value
    if name == "recover":
        recovery = result.get("recovery")
        if isinstance(recovery, dict):
            projected["recovery"] = _project_control_summary(
                recovery,
                ("attempted", "processed"),
            )
    if name == "tick":
        tick = result.get("tick")
        if isinstance(tick, dict):
            projected["tick"] = _project_control_summary(
                tick,
                ("processed", "external_execution_enabled"),
            )
    if not projected:
        projected["outcome"] = "completed"
    try:
        encoded = _canonical_json(projected)
    except ValueError:
        return {"outcome": "completed"}
    if len(encoded) > MAX_RESPONSE_BYTES:
        return {"outcome": "completed", "truncated": True}
    return projected


def _project_event(event: Any) -> dict[str, Any]:
    if not isinstance(event, dict):
        return {}
    projected: dict[str, Any] = {}
    for key in (
        "sequence",
        "type",
        "occurred_at",
        "has_node",
        "has_attempt",
        "terminal",
    ):
        value = event.get(key)
        if (
            isinstance(value, (str, int, float, bool))
            and not (
                isinstance(value, float)
                and not math.isfinite(value)
            )
            and not (
                isinstance(value, str)
                and (len(value) > 160 or _has_control_character(value))
            )
        ):
            projected[key] = value
    return projected


def _project_control_summary(
    value: Mapping[str, Any],
    allowed_fields: tuple[str, ...],
) -> dict[str, Any]:
    return {
        key: item
        for key in allowed_fields
        if (
            (item := value.get(key)) is not None
            and isinstance(item, (int, bool))
            and not isinstance(item, float)
        )
    }


def _runtime_request_id(request_id: str | int, name: str) -> str:
    digest = hashlib.sha256(
        _canonical_json({"id": request_id, "tool": name})
    ).hexdigest()
    return f"mcp-{name}-{digest[:24]}"


def _result_response(
    request_id: str | int,
    result: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": json.loads(_canonical_json(result)),
    }


def _error_response(
    request_id: str | int | None,
    code: int,
    message: str,
    *,
    data: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "jsonrpc": "2.0",
        "error": {"code": code, "message": message},
    }
    if request_id is not None:
        response["id"] = request_id
    if data is not None:
        response["error"]["data"] = json.loads(_canonical_json(data))
    return response


def _invalid_input_rate_limit_response(
    request_id: str | int | None,
    retry_after_ms: int,
) -> dict[str, Any]:
    return _error_response(
        request_id,
        -32901,
        "Invalid input rate limit exceeded",
        data={"retryAfterMs": retry_after_ms},
    )


def _readable_request_id(message: Any) -> str | int | None:
    if not isinstance(message, dict):
        return None
    value = message.get("id")
    return value if _valid_request_id(value) else None


def _valid_request_id(value: Any) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, int):
        return -(2**53) + 1 <= value <= 2**53 - 1
    return (
        isinstance(value, str)
        and 0 < len(value) <= 256
        and not _has_control_character(value)
    )


def _valid_progress_token(value: Any) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, int):
        return -(2**53) + 1 <= value <= 2**53 - 1
    if isinstance(value, float):
        return math.isfinite(value)
    return (
        isinstance(value, str)
        and 0 < len(value) <= 256
        and not _has_control_character(value)
    )


def _valid_meta_key(value: Any) -> bool:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 256
        or _has_control_character(value)
        or value.count("/") > 1
    ):
        return False
    if "/" in value:
        prefix, name = value.split("/", 1)
        labels = prefix.split(".")
        if not labels or any(not _valid_meta_label(label) for label in labels):
            return False
    else:
        name = value
    if not name:
        return True
    if not (name[0].isalnum() and name[-1].isalnum()):
        return False
    return all(
        character.isascii()
        and (character.isalnum() or character in "-_.")
        for character in name
    )


def _valid_meta_label(value: str) -> bool:
    return (
        bool(value)
        and value[0].isascii()
        and value[0].isalpha()
        and value[-1].isascii()
        and value[-1].isalnum()
        and all(
            character.isascii()
            and (character.isalnum() or character == "-")
            for character in value
        )
    )


def _exact_fields(
    value: Mapping[str, Any],
    expected: set[str] | frozenset[str],
    field: str,
) -> None:
    if set(value) != set(expected):
        if field == "request":
            raise _RPCFault(-32600, "Invalid Request")
        raise _RPCFault(-32602, "Invalid params")


def _has_control_character(value: str) -> bool:
    return any(
        ord(character) < 32 or ord(character) == 127
        for character in value
    )


def _bounded_json_tree(value: Any, *, max_depth: int, max_items: int) -> bool:
    remaining = max_items

    def visit(item: Any, depth: int) -> bool:
        nonlocal remaining
        if depth > max_depth:
            return False
        if isinstance(item, dict):
            remaining -= len(item)
            if remaining < 0:
                return False
            return all(
                isinstance(key, str)
                and len(key) <= 256
                and not _has_control_character(key)
                and visit(child, depth + 1)
                for key, child in item.items()
            )
        if isinstance(item, list):
            remaining -= len(item)
            return remaining >= 0 and all(
                visit(child, depth + 1) for child in item
            )
        if isinstance(item, float):
            return math.isfinite(item)
        return item is None or isinstance(item, (str, int, bool))

    return visit(value, 0)


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ValueError("value must be bounded JSON") from exc


def _object_schema(
    properties: Mapping[str, Any],
    required: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": dict(properties),
        "required": list(required),
        "additionalProperties": False,
    }


_NULLABLE_ID_SCHEMA = {
    "type": ["string", "null"],
    "minLength": 1,
    "maxLength": 512,
}
_ARTIFACT_REF_SCHEMA = _object_schema(
    {
        "schema_version": {"type": "integer", "const": ARTIFACT_SCHEMA_VERSION},
        "artifact_id": {"type": "string", "minLength": 1, "maxLength": 255},
        "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
        "size": {"type": "integer", "minimum": 0},
        "media_type": {"type": "string", "minLength": 1, "maxLength": 255},
        "kind": {
            "type": "string",
            "enum": [item.value for item in ArtifactKind],
        },
        "uri": {
            "type": "string",
            "minLength": 1,
            "maxLength": 1024,
            "pattern": "^(?!/)(?!.*(?:^|/)\\.\\.?/).+$",
        },
        "sensitivity": {
            "type": "string",
            "enum": [item.value for item in ArtifactSensitivity],
        },
        "encryption": {
            "type": "string",
            "enum": [item.value for item in ArtifactEncryption],
        },
        "producer_run_id": dict(_NULLABLE_ID_SCHEMA),
        "producer_node_id": dict(_NULLABLE_ID_SCHEMA),
        "producer_attempt_id": dict(_NULLABLE_ID_SCHEMA),
        "encryption_key_ref": dict(_NULLABLE_ID_SCHEMA),
        "metadata": _object_schema({}, ()),
        "created_at": {"type": "number", "minimum": 0},
    },
    (
        "schema_version",
        "artifact_id",
        "sha256",
        "size",
        "media_type",
        "kind",
        "uri",
        "sensitivity",
        "encryption",
        "producer_run_id",
        "producer_node_id",
        "producer_attempt_id",
        "encryption_key_ref",
        "metadata",
        "created_at",
    ),
)
_RUN_ID_SCHEMA = {
    "type": "string",
    "pattern": "^[A-Za-z0-9][A-Za-z0-9_.:-]{0,254}$",
}
_PARENT_SCHEMA = _object_schema(
    {
        "parent_run_id": dict(_RUN_ID_SCHEMA),
        "parent_node_id": dict(_RUN_ID_SCHEMA),
    },
    ("parent_run_id", "parent_node_id"),
)
_INPUT_RECEIPT_SCHEMA = _object_schema(
    {
        "artifact_refs": {
            "type": "array",
            "items": {"$ref": "#/$defs/artifactRef"},
            "minItems": 1,
            "maxItems": 64,
        }
    },
    ("artifact_refs",),
)
_SUBMIT_SCHEMA = {
    "$schema": JSON_SCHEMA_2020_12,
    **_object_schema(
        {
            "run_id": dict(_RUN_ID_SCHEMA),
            "workflow_ref": {"$ref": "#/$defs/artifactRef"},
            "input_receipt": {
                "oneOf": [
                    {"type": "null"},
                    {"$ref": "#/$defs/inputReceipt"},
                ]
            },
            "parent": {
                "oneOf": [
                    {"type": "null"},
                    {"$ref": "#/$defs/parent"},
                ]
            },
        },
        ("run_id", "workflow_ref", "input_receipt", "parent"),
    ),
    "$defs": {
        "artifactRef": _ARTIFACT_REF_SCHEMA,
        "inputReceipt": _INPUT_RECEIPT_SCHEMA,
        "parent": _PARENT_SCHEMA,
    },
}
_RUN_SCHEMA = {
    "$schema": JSON_SCHEMA_2020_12,
    **_object_schema({"run_id": dict(_RUN_ID_SCHEMA)}, ("run_id",)),
}
_EVENTS_SCHEMA = {
    "$schema": JSON_SCHEMA_2020_12,
    **_object_schema(
        {
            "run_id": dict(_RUN_ID_SCHEMA),
            "after_sequence": {
                "type": "integer",
                "minimum": 0,
                "maximum": 2**63 - 1,
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_EVENT_PAGE_SIZE,
            },
        },
        ("run_id", "after_sequence", "limit"),
    ),
}
_RECOVER_SCHEMA = {
    "$schema": JSON_SCHEMA_2020_12,
    **_object_schema(
        {
            "run_id": dict(_RUN_ID_SCHEMA),
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_CONTROL_STEPS,
            },
        },
        ("run_id", "limit"),
    ),
}
_TICK_SCHEMA = {
    "$schema": JSON_SCHEMA_2020_12,
    **_object_schema(
        {
            "run_id": dict(_RUN_ID_SCHEMA),
            "max_steps": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_CONTROL_STEPS,
            },
        },
        ("run_id", "max_steps"),
    ),
}


def _tool_definition(
    name: str,
    title: str,
    description: str,
    input_schema: Mapping[str, Any],
    *,
    read_only: bool,
    destructive: bool,
    idempotent: bool,
) -> dict[str, Any]:
    return {
        "name": name,
        "title": title,
        "description": description,
        "inputSchema": dict(input_schema),
        "annotations": {
            "title": title,
            "readOnlyHint": read_only,
            "destructiveHint": destructive,
            "idempotentHint": idempotent,
            "openWorldHint": False,
        },
    }


_TOOL_DEFINITIONS = (
    _tool_definition(
        "submit",
        "Submit durable run",
        (
            "Submit one immutable workflow Artifact under a caller-selected stable "
            "run ID. Raw workflow or input data is not accepted."
        ),
        _SUBMIT_SCHEMA,
        read_only=False,
        destructive=False,
        idempotent=True,
    ),
    _tool_definition(
        "status",
        "Get run status",
        "Return the bounded public projection for an explicit run ID.",
        _RUN_SCHEMA,
        read_only=True,
        destructive=False,
        idempotent=True,
    ),
    _tool_definition(
        "events",
        "List run events",
        "Return a bounded page of sanitized durable events for an explicit run ID.",
        _EVENTS_SCHEMA,
        read_only=True,
        destructive=False,
        idempotent=True,
    ),
    _tool_definition(
        "cancel",
        "Cancel run",
        "Durably request cancellation for an explicit run ID.",
        _RUN_SCHEMA,
        read_only=False,
        destructive=True,
        idempotent=True,
    ),
    _tool_definition(
        "pause",
        "Pause run",
        "Durably request a safe pause for an explicit run ID.",
        _RUN_SCHEMA,
        read_only=False,
        destructive=False,
        idempotent=True,
    ),
    _tool_definition(
        "resume",
        "Resume run",
        "Resume a durably paused run with an explicit run ID.",
        _RUN_SCHEMA,
        read_only=False,
        destructive=False,
        idempotent=True,
    ),
    _tool_definition(
        "recover",
        "Recover run",
        "Invoke the injected bounded recovery driver for an explicit run ID.",
        _RECOVER_SCHEMA,
        read_only=False,
        destructive=False,
        idempotent=False,
    ),
    _tool_definition(
        "tick",
        "Advance run",
        "Advance an explicit durable run by a bounded number of scheduler steps.",
        _TICK_SCHEMA,
        read_only=False,
        destructive=False,
        idempotent=False,
    ),
)


__all__ = [
    "DISCOVERY_CACHE_TTL_MS",
    "JSON_SCHEMA_2020_12",
    "MAX_MCP_JSON_DEPTH",
    "MAX_MCP_JSON_ITEMS",
    "MAX_MCP_MESSAGE_BYTES",
    "MCP_PROTOCOL_VERSION",
    "MCPServer",
    "InvalidInputTokenBucket",
    "OrchestrationRuntimeLike",
    "PerContextRateLimiter",
    "SUPPORTED_PROTOCOL_VERSIONS",
    "TOOLS_CACHE_TTL_MS",
]
