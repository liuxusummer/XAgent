"""Strict canonical wire protocol for the durable Agent provider client."""

from __future__ import annotations

import json
import math
import re
from typing import Any

from src.core.llm import ChatResponse, TokenUsage, ToolCall

from .provider_access import (
    MAX_PROVIDER_REQUEST_BYTES,
    MAX_PROVIDER_RESPONSE_BYTES,
)


AGENT_PROVIDER_WIRE_SCHEMA_VERSION = 1
MAX_AGENT_PROVIDER_MESSAGES = 256
MAX_AGENT_PROVIDER_TOOLS = 256
MAX_AGENT_PROVIDER_TOOL_CALLS = 128
MAX_AGENT_PROVIDER_JSON_DEPTH = 24
MAX_AGENT_PROVIDER_JSON_ITEMS = 65_536
MAX_AGENT_PROVIDER_STRING_BYTES = 4 * 1024 * 1024
MAX_AGENT_PROVIDER_OUTPUT_TOKENS = 1_000_000
_MAX_JSON_INTEGER = (1 << 63) - 1
_SAFE_CODE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,254}$")
_USAGE_FIELDS = (
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cache_creation_input_tokens",
    "cache_read_input_tokens",
    "reasoning_tokens",
)
_WIRE_ERROR_REASONS = frozenset(
    {
        "agent_provider_request_invalid",
        "agent_provider_request_too_large",
        "agent_provider_response_invalid",
    }
)


class AgentProviderWireError(ValueError):
    """A canonical Agent provider request or response is invalid."""

    def __init__(self, reason_code: str) -> None:
        if reason_code not in _WIRE_ERROR_REASONS:
            raise ValueError("invalid Agent provider wire reason code")
        self.reason_code = reason_code
        super().__init__(reason_code)


class _WireValidationError(ValueError):
    pass


def encode_agent_provider_request(
    messages: Any,
    tools: Any,
    *,
    retained_system: list[dict[str, Any]],
    retained_history: list[dict[str, Any]],
    invocation_index: int,
    temperature: float,
    max_output_tokens: int,
) -> tuple[
    bytes,
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    """Return canonical bytes plus the validated current history delta."""

    invalid = False
    payload = b""
    current_system: list[dict[str, Any]] = []
    current_non_system: list[dict[str, Any]] = []
    try:
        current = _messages(messages, allow_empty=False)
        normalized_tools = _tools(tools)
        existing_system = _messages(
            retained_system,
            allow_empty=True,
        )
        existing_history = _messages(
            retained_history,
            allow_empty=True,
        )
        current_system = [
            message
            for message in current
            if message["role"] == "system"
        ]
        current_non_system = [
            message
            for message in current
            if message["role"] != "system"
        ]
        request_messages = [
            *(current_system or existing_system),
            *existing_history,
            *current_non_system,
        ]
        if (
            not request_messages
            or len(request_messages) > MAX_AGENT_PROVIDER_MESSAGES
        ):
            raise _WireValidationError
        payload = _canonical_json(
            {
                "schema_version": (
                    AGENT_PROVIDER_WIRE_SCHEMA_VERSION
                ),
                "kind": "agent_provider_request",
                "invocation_index": _bounded_integer(
                    invocation_index,
                    minimum=1,
                    maximum=1_000_000,
                ),
                "generation": {
                    "temperature": _temperature(temperature),
                    "max_output_tokens": _bounded_integer(
                        max_output_tokens,
                        minimum=1,
                        maximum=MAX_AGENT_PROVIDER_OUTPUT_TOKENS,
                    ),
                },
                "messages": request_messages,
                "tools": normalized_tools,
            }
        )
    except _WireValidationError:
        invalid = True
    if invalid:
        raise AgentProviderWireError(
            "agent_provider_request_invalid"
        ) from None
    if len(payload) > MAX_PROVIDER_REQUEST_BYTES:
        raise AgentProviderWireError(
            "agent_provider_request_too_large"
        )
    return payload, current_system, current_non_system


def decode_agent_provider_response(content: bytes) -> ChatResponse:
    """Decode an exact-field canonical response without retaining its body."""

    invalid = False
    response: ChatResponse | None = None
    try:
        if (
            not isinstance(content, bytes)
            or not content
            or len(content) > MAX_PROVIDER_RESPONSE_BYTES
        ):
            raise _WireValidationError
        payload = json.loads(
            content.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_invalid_json_constant,
        )
        normalized = _strict_json(payload)
        if _canonical_json(normalized) != content:
            raise _WireValidationError
        if (
            type(normalized) is not dict
            or set(normalized)
            != {
                "schema_version",
                "kind",
                "thinking",
                "content",
                "tool_calls",
                "stop_reason",
                "usage",
            }
            or normalized["schema_version"]
            != AGENT_PROVIDER_WIRE_SCHEMA_VERSION
            or normalized["kind"] != "agent_provider_response"
            or type(normalized["thinking"]) is not str
            or type(normalized["content"]) is not str
            or type(normalized["stop_reason"]) is not str
            or _SAFE_CODE.fullmatch(normalized["stop_reason"])
            is None
            or type(normalized["tool_calls"]) is not list
            or len(normalized["tool_calls"])
            > MAX_AGENT_PROVIDER_TOOL_CALLS
        ):
            raise _WireValidationError
        response = ChatResponse(
            thinking=normalized["thinking"],
            content=normalized["content"],
            tool_calls=_decode_tool_calls(
                normalized["tool_calls"]
            ),
            raw=normalized["content"],
            stop_reason=normalized["stop_reason"],
            usage=_decode_usage(normalized["usage"]),
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except (
        _WireValidationError,
        TypeError,
        ValueError,
        UnicodeError,
        OverflowError,
        RecursionError,
    ):
        invalid = True
    if invalid or response is None:
        raise AgentProviderWireError(
            "agent_provider_response_invalid"
        ) from None
    return response


def _messages(
    value: Any,
    *,
    allow_empty: bool,
) -> list[dict[str, Any]]:
    if (
        type(value) is not list
        or len(value) > MAX_AGENT_PROVIDER_MESSAGES
        or (not allow_empty and not value)
    ):
        raise _WireValidationError
    normalized = _strict_json(value)
    if type(normalized) is not list:
        raise _WireValidationError
    for message in normalized:
        if (
            type(message) is not dict
            or "role" not in message
            or "content" not in message
            or not set(message).issubset(
                {
                    "role",
                    "content",
                    "thinking",
                    "tool_calls",
                    "tool_results",
                }
            )
            or message["role"]
            not in {"system", "user", "assistant"}
            or type(message["content"]) is not str
            or (
                "thinking" in message
                and type(message["thinking"]) is not str
            )
            or (
                "tool_calls" in message
                and type(message["tool_calls"]) is not list
            )
            or (
                "tool_results" in message
                and type(message["tool_results"]) is not list
            )
        ):
            raise _WireValidationError
        if "tool_calls" in message:
            _validate_wire_tool_calls(message["tool_calls"])
    return normalized


def _tools(value: Any) -> list[dict[str, Any]]:
    if (
        type(value) is not list
        or len(value) > MAX_AGENT_PROVIDER_TOOLS
    ):
        raise _WireValidationError
    normalized = _strict_json(value)
    if (
        type(normalized) is not list
        or not all(type(item) is dict for item in normalized)
    ):
        raise _WireValidationError
    return normalized


def _decode_tool_calls(value: list[Any]) -> list[ToolCall]:
    _validate_wire_tool_calls(value)
    return [
        ToolCall(
            name=item["name"],
            args=item["arguments"],
            id=item["id"],
        )
        for item in value
    ]


def _validate_wire_tool_calls(value: list[Any]) -> None:
    if len(value) > MAX_AGENT_PROVIDER_TOOL_CALLS:
        raise _WireValidationError
    for item in value:
        if (
            type(item) is not dict
            or set(item) != {"name", "arguments", "id"}
            or type(item["name"]) is not str
            or _SAFE_CODE.fullmatch(item["name"]) is None
            or type(item["id"]) is not str
            or not item["id"]
            or len(item["id"]) > 255
            or any(
                ord(character) < 32 or ord(character) == 127
                for character in item["id"]
            )
            or type(item["arguments"]) is not dict
        ):
            raise _WireValidationError


def _decode_usage(value: Any) -> TokenUsage | None:
    if value is None:
        return None
    if type(value) is not dict or set(value) != set(_USAGE_FIELDS):
        raise _WireValidationError
    normalized: dict[str, int | None] = {}
    for field_name in _USAGE_FIELDS:
        count = value[field_name]
        if count is not None:
            count = _bounded_integer(
                count,
                minimum=0,
                maximum=_MAX_JSON_INTEGER,
            )
        normalized[field_name] = count
    return TokenUsage(**normalized)


def _strict_json(value: Any) -> Any:
    return _normalize_json(
        value,
        depth=0,
        item_count=[0],
        active=set(),
    )


def _normalize_json(
    value: Any,
    *,
    depth: int,
    item_count: list[int],
    active: set[int],
) -> Any:
    item_count[0] += 1
    if (
        depth > MAX_AGENT_PROVIDER_JSON_DEPTH
        or item_count[0] > MAX_AGENT_PROVIDER_JSON_ITEMS
    ):
        raise _WireValidationError
    if value is None or type(value) is bool:
        return value
    if type(value) is int:
        if abs(value) > _MAX_JSON_INTEGER:
            raise _WireValidationError
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise _WireValidationError
        return value
    if type(value) is str:
        try:
            encoded = value.encode("utf-8")
        except UnicodeError:
            raise _WireValidationError from None
        if len(encoded) > MAX_AGENT_PROVIDER_STRING_BYTES:
            raise _WireValidationError
        return value
    if type(value) not in {dict, list}:
        raise _WireValidationError
    identity = id(value)
    if (
        identity in active
        or len(value)
        > MAX_AGENT_PROVIDER_JSON_ITEMS - item_count[0]
    ):
        raise _WireValidationError
    active.add(identity)
    try:
        if type(value) is list:
            return [
                _normalize_json(
                    item,
                    depth=depth + 1,
                    item_count=item_count,
                    active=active,
                )
                for item in value
            ]
        keys = tuple(value)
        if any(not _valid_json_key(key) for key in keys):
            raise _WireValidationError
        return {
            key: _normalize_json(
                value[key],
                depth=depth + 1,
                item_count=item_count,
                active=active,
            )
            for key in sorted(keys)
        }
    finally:
        active.remove(identity)


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (
        TypeError,
        ValueError,
        UnicodeError,
        OverflowError,
        RecursionError,
    ):
        raise _WireValidationError from None


def _valid_json_key(value: Any) -> bool:
    if type(value) is not str:
        return False
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        return False
    return (
        len(encoded) <= 512
        and all(
            ord(character) >= 32 and ord(character) != 127
            for character in value
        )
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _WireValidationError
        value[key] = item
    return value


def _invalid_json_constant(_value: str) -> None:
    raise _WireValidationError


def _bounded_integer(
    value: Any,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if (
        type(value) is not int
        or not minimum <= value <= maximum
    ):
        raise _WireValidationError
    return value


def _temperature(value: Any) -> float:
    if (
        type(value) not in {int, float}
        or not math.isfinite(float(value))
        or not 0 <= float(value) <= 2
    ):
        raise _WireValidationError
    return float(value)


__all__ = [
    "AGENT_PROVIDER_WIRE_SCHEMA_VERSION",
    "AgentProviderWireError",
    "MAX_AGENT_PROVIDER_JSON_DEPTH",
    "MAX_AGENT_PROVIDER_JSON_ITEMS",
    "MAX_AGENT_PROVIDER_MESSAGES",
    "MAX_AGENT_PROVIDER_OUTPUT_TOKENS",
    "MAX_AGENT_PROVIDER_STRING_BYTES",
    "MAX_AGENT_PROVIDER_TOOL_CALLS",
    "MAX_AGENT_PROVIDER_TOOLS",
    "decode_agent_provider_response",
    "encode_agent_provider_request",
]
