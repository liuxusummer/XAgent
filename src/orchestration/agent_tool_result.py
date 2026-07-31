"""Canonical durable result envelope for one Agent-internal Tool call."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from src.core.agent_loop import ActionResult

from .artifacts import (
    ArtifactEncryption,
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    ArtifactStore,
    LocalArtifactStore,
)


AGENT_TOOL_RESULT_SCHEMA_VERSION = 1
AGENT_TOOL_RESULT_MEDIA_TYPE = (
    "application/vnd.xagent.agent-tool-result+json"
)
MAX_AGENT_TOOL_RESULT_BYTES = 1024 * 1024
MAX_AGENT_TOOL_RESULT_JSON_DEPTH = 24
MAX_AGENT_TOOL_RESULT_JSON_ITEMS = 65_536
MAX_AGENT_TOOL_RESULT_STRING_BYTES = 1024 * 1024
MAX_AGENT_TOOL_NEXT_PROMPT_BYTES = 64 * 1024
_MAX_JSON_INTEGER = (1 << 63) - 1
_ALLOWED_FLAGS = frozenset({"reset_tools", "retry"})
_ERROR_REASONS = frozenset(
    {
        "agent_tool_result_artifact_invalid",
        "agent_tool_result_artifact_unavailable",
        "agent_tool_result_invalid",
        "agent_tool_result_too_large",
    }
)


class AgentToolResultError(RuntimeError, ValueError):
    """A Tool result envelope or its Artifact binding is invalid."""

    def __init__(self, reason_code: str) -> None:
        if reason_code not in _ERROR_REASONS:
            raise ValueError("invalid Agent Tool result reason code")
        self.reason_code = reason_code
        super().__init__(reason_code)


class _ResultValidationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class AgentToolResult:
    """Bounded LLM-visible outcome installed as an immutable Artifact."""

    data: Any
    next_prompt: str | None
    should_exit: bool = False
    flags: frozenset[str] = frozenset()
    schema_version: int = AGENT_TOOL_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        invalid = False
        normalized_data: Any = None
        normalized_flags: frozenset[str] = frozenset()
        try:
            normalized_data = _strict_json(self.data)
            if self.next_prompt is not None:
                if (
                    type(self.next_prompt) is not str
                    or len(self.next_prompt.encode("utf-8"))
                    > MAX_AGENT_TOOL_NEXT_PROMPT_BYTES
                ):
                    raise _ResultValidationError
            if type(self.should_exit) is not bool:
                raise _ResultValidationError
            if type(self.flags) not in {frozenset, set, tuple, list}:
                raise _ResultValidationError
            normalized_flags = frozenset(self.flags)
            if (
                len(normalized_flags) != len(self.flags)
                or not normalized_flags.issubset(_ALLOWED_FLAGS)
                or not all(type(flag) is str for flag in normalized_flags)
                or type(self.schema_version) is not int
                or self.schema_version
                != AGENT_TOOL_RESULT_SCHEMA_VERSION
            ):
                raise _ResultValidationError
        except (KeyboardInterrupt, SystemExit):
            raise
        except (
            _ResultValidationError,
            TypeError,
            ValueError,
            UnicodeError,
            OverflowError,
            RecursionError,
        ):
            invalid = True
        if invalid:
            raise AgentToolResultError(
                "agent_tool_result_invalid"
            ) from None
        object.__setattr__(self, "data", normalized_data)
        object.__setattr__(self, "flags", normalized_flags)
        if len(self.to_bytes()) > MAX_AGENT_TOOL_RESULT_BYTES:
            raise AgentToolResultError(
                "agent_tool_result_too_large"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": "agent_tool_result",
            "data": self.data,
            "next_prompt": self.next_prompt,
            "should_exit": self.should_exit,
            "flags": sorted(self.flags),
        }

    def to_bytes(self) -> bytes:
        return _canonical_json(self.to_dict())

    def to_action_result(self, *, tool_receipt: object) -> ActionResult:
        return ActionResult(
            data=self.data,
            next_prompt=self.next_prompt,
            should_exit=self.should_exit,
            flags=self.flags,
            tool_receipt=tool_receipt,
        )

    @classmethod
    def from_bytes(cls, content: bytes) -> "AgentToolResult":
        invalid = False
        result: AgentToolResult | None = None
        try:
            if (
                type(content) is not bytes
                or not content
                or len(content) > MAX_AGENT_TOOL_RESULT_BYTES
            ):
                raise _ResultValidationError
            payload = json.loads(
                content.decode("utf-8"),
                object_pairs_hook=_unique_object,
                parse_constant=_invalid_json_constant,
            )
            normalized = _strict_json(payload)
            if (
                _canonical_json(normalized) != content
                or type(normalized) is not dict
                or set(normalized)
                != {
                    "schema_version",
                    "kind",
                    "data",
                    "next_prompt",
                    "should_exit",
                    "flags",
                }
                or normalized["schema_version"]
                != AGENT_TOOL_RESULT_SCHEMA_VERSION
                or normalized["kind"] != "agent_tool_result"
            ):
                raise _ResultValidationError
            result = cls(
                schema_version=normalized["schema_version"],
                data=normalized["data"],
                next_prompt=normalized["next_prompt"],
                should_exit=normalized["should_exit"],
                flags=normalized["flags"],
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except (
            _ResultValidationError,
            AgentToolResultError,
            TypeError,
            ValueError,
            UnicodeError,
            OverflowError,
            RecursionError,
        ):
            invalid = True
        if invalid or result is None:
            raise AgentToolResultError(
                "agent_tool_result_invalid"
            ) from None
        return result


class AgentToolResultArtifactStore:
    """Stage and load exact canonical Agent Tool result Artifacts."""

    @staticmethod
    def stage(
        store: ArtifactStore,
        result: AgentToolResult,
        *,
        sensitivity: ArtifactSensitivity | str,
        run_id: str,
        node_id: str,
        attempt_id: str,
    ) -> ArtifactRef:
        if (
            not isinstance(store, ArtifactStore)
            or type(result) is not AgentToolResult
        ):
            raise AgentToolResultError(
                "agent_tool_result_artifact_invalid"
            )
        try:
            resolved_sensitivity = ArtifactSensitivity(sensitivity)
        except (TypeError, ValueError):
            raise AgentToolResultError(
                "agent_tool_result_artifact_invalid"
            ) from None
        if resolved_sensitivity not in {
            ArtifactSensitivity.SENSITIVE,
            ArtifactSensitivity.SECRET,
        }:
            raise AgentToolResultError(
                "agent_tool_result_artifact_invalid"
            )
        if (
            resolved_sensitivity is ArtifactSensitivity.SECRET
            and isinstance(store, LocalArtifactStore)
        ):
            # Reject before ``put_bytes``: the reference implementation stores
            # plaintext and a post-write encryption check would already have
            # disclosed the secret to disk.
            raise AgentToolResultError(
                "agent_tool_result_artifact_invalid"
            )
        write_failed = False
        ref: ArtifactRef | None = None
        try:
            ref = store.put_bytes(
                result.to_bytes(),
                media_type=AGENT_TOOL_RESULT_MEDIA_TYPE,
                kind=ArtifactKind.TOOL_RESULT,
                sensitivity=resolved_sensitivity,
                producer_run_id=run_id,
                producer_node_id=node_id,
                producer_attempt_id=attempt_id,
                metadata={},
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            write_failed = True
        if write_failed or ref is None:
            raise AgentToolResultError(
                "agent_tool_result_artifact_unavailable"
            ) from None
        AgentToolResultArtifactStore.validate_ref(ref, result)
        verification_failed = False
        try:
            if store.verify(ref) is not True:
                raise ValueError
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            verification_failed = True
        if verification_failed:
            raise AgentToolResultError(
                "agent_tool_result_artifact_unavailable"
            ) from None
        return ref

    @staticmethod
    def validate_ref(
        ref: ArtifactRef,
        result: AgentToolResult | None = None,
    ) -> None:
        if (
            type(ref) is not ArtifactRef
            or ref.kind is not ArtifactKind.TOOL_RESULT
            or ref.media_type != AGENT_TOOL_RESULT_MEDIA_TYPE
            or ref.size <= 0
            or ref.size > MAX_AGENT_TOOL_RESULT_BYTES
            or ref.sensitivity
            not in {
                ArtifactSensitivity.SENSITIVE,
                ArtifactSensitivity.SECRET,
            }
            or (
                ref.sensitivity is ArtifactSensitivity.SECRET
                and ref.encryption
                is not ArtifactEncryption.DEPLOYMENT_MANAGED
            )
            or ref.producer_run_id is None
            or ref.producer_node_id is None
            or ref.producer_attempt_id is None
            or dict(ref.metadata)
        ):
            raise AgentToolResultError(
                "agent_tool_result_artifact_invalid"
            )
        if result is not None:
            if type(result) is not AgentToolResult:
                raise AgentToolResultError(
                    "agent_tool_result_artifact_invalid"
                )
            content = result.to_bytes()
            if (
                ref.size != len(content)
                or ref.sha256
                != hashlib.sha256(content).hexdigest()
            ):
                raise AgentToolResultError(
                    "agent_tool_result_artifact_invalid"
                )

    @staticmethod
    def load(
        store: ArtifactStore,
        ref: ArtifactRef,
    ) -> AgentToolResult:
        if not isinstance(store, ArtifactStore):
            raise AgentToolResultError(
                "agent_tool_result_artifact_invalid"
            )
        AgentToolResultArtifactStore.validate_ref(ref)
        failed = False
        content: bytes | None = None
        try:
            if store.verify(ref) is not True:
                raise ValueError
            content = store.read(ref)
            if (
                type(content) is not bytes
                or len(content) != ref.size
                or hashlib.sha256(content).hexdigest() != ref.sha256
            ):
                raise ValueError
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            failed = True
        if failed or content is None:
            raise AgentToolResultError(
                "agent_tool_result_artifact_unavailable"
            ) from None
        result = AgentToolResult.from_bytes(content)
        AgentToolResultArtifactStore.validate_ref(ref, result)
        return result


def _strict_json(value: Any) -> Any:
    return _normalize_json(
        value,
        depth=0,
        item_count=[0],
        string_bytes=[0],
        active=set(),
    )


def _normalize_json(
    value: Any,
    *,
    depth: int,
    item_count: list[int],
    string_bytes: list[int],
    active: set[int],
) -> Any:
    item_count[0] += 1
    if (
        depth > MAX_AGENT_TOOL_RESULT_JSON_DEPTH
        or item_count[0] > MAX_AGENT_TOOL_RESULT_JSON_ITEMS
    ):
        raise _ResultValidationError
    if value is None or type(value) is bool:
        return value
    if type(value) is int:
        if abs(value) > _MAX_JSON_INTEGER:
            raise _ResultValidationError
        return value
    if type(value) is float:
        if value != value or value in {float("inf"), float("-inf")}:
            raise _ResultValidationError
        return value
    if type(value) is str:
        encoded_size = len(value.encode("utf-8"))
        string_bytes[0] += encoded_size
        if (
            encoded_size > MAX_AGENT_TOOL_RESULT_STRING_BYTES
            or string_bytes[0] > MAX_AGENT_TOOL_RESULT_BYTES
        ):
            raise _ResultValidationError
        return value
    if type(value) not in {dict, list}:
        raise _ResultValidationError
    identity = id(value)
    if (
        identity in active
        or len(value)
        > MAX_AGENT_TOOL_RESULT_JSON_ITEMS - item_count[0]
    ):
        raise _ResultValidationError
    active.add(identity)
    try:
        if type(value) is list:
            return [
                _normalize_json(
                    item,
                    depth=depth + 1,
                    item_count=item_count,
                    string_bytes=string_bytes,
                    active=active,
                )
                for item in value
            ]
        keys = tuple(value)
        if any(not _valid_key(key) for key in keys):
            raise _ResultValidationError
        string_bytes[0] += sum(
            len(key.encode("utf-8")) for key in keys
        )
        if string_bytes[0] > MAX_AGENT_TOOL_RESULT_BYTES:
            raise _ResultValidationError
        return {
            key: _normalize_json(
                value[key],
                depth=depth + 1,
                item_count=item_count,
                string_bytes=string_bytes,
                active=active,
            )
            for key in sorted(keys)
        }
    finally:
        active.remove(identity)


def _valid_key(value: Any) -> bool:
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
        raise _ResultValidationError from None


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _ResultValidationError
        value[key] = item
    return value


def _invalid_json_constant(_value: str) -> None:
    raise _ResultValidationError


__all__ = [
    "AGENT_TOOL_RESULT_MEDIA_TYPE",
    "AGENT_TOOL_RESULT_SCHEMA_VERSION",
    "AgentToolResult",
    "AgentToolResultArtifactStore",
    "AgentToolResultError",
    "MAX_AGENT_TOOL_RESULT_BYTES",
]
