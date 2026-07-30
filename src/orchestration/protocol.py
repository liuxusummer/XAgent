"""Strict transport-neutral protocol for durable orchestration.

HTTP, MCP, or another transport should decode one JSON object and pass it to
``parse_request``.  This module performs no I/O and has no execution authority.
Unknown fields and protocol versions are rejected so a new client cannot
silently acquire semantics an older runtime does not understand.

Submit bodies contain only complete ``ArtifactRef`` objects and an optional
``RunInputReceipt``.  Raw workflow definitions, prompts, tool arguments, file
paths, and arbitrary Run input are intentionally not part of the protocol.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping, TypeAlias

from .artifacts import ArtifactRef
from .recovery import (
    RecoveryDecisionError,
    UnknownOutcomeDecision,
    UnknownOutcomeResolution,
)
from .scheduler import RunInputReceipt

PROTOCOL_VERSION = 1
MAX_REQUEST_BYTES = 1024 * 1024
MAX_RESPONSE_BYTES = 256 * 1024
MAX_EVENT_PAGE_SIZE = 100
MAX_CONTROL_STEPS = 100
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,254}$")
_ARTIFACT_FIELDS = frozenset(ArtifactRef.__dataclass_fields__)


class ProtocolValidationError(ValueError):
    """A transport request violates the strict versioned schema."""


class Operation(StrEnum):
    SUBMIT = "submit"
    STATUS = "status"
    EVENTS = "events"
    CANCEL = "cancel"
    PAUSE = "pause"
    RESUME = "resume"
    RECOVER = "recover"
    RESOLVE_RECOVERY = "resolve_recovery"
    TICK = "tick"

    @property
    def mutation(self) -> bool:
        return self in {
            Operation.SUBMIT,
            Operation.CANCEL,
            Operation.PAUSE,
            Operation.RESUME,
            Operation.RECOVER,
            Operation.RESOLVE_RECOVERY,
            Operation.TICK,
        }


@dataclass(frozen=True, slots=True)
class ParentSubmission:
    parent_run_id: str
    parent_node_id: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "parent_run_id",
            _identifier(self.parent_run_id, "parent_run_id"),
        )
        object.__setattr__(
            self,
            "parent_node_id",
            _identifier(self.parent_node_id, "parent_node_id"),
        )


@dataclass(frozen=True, slots=True)
class SubmitBody:
    run_id: str
    workflow_ref: ArtifactRef
    input_receipt: RunInputReceipt | None
    parent: ParentSubmission | None = None


@dataclass(frozen=True, slots=True)
class RunBody:
    run_id: str


@dataclass(frozen=True, slots=True)
class EventsBody:
    run_id: str
    after_sequence: int = 0
    limit: int = MAX_EVENT_PAGE_SIZE


@dataclass(frozen=True, slots=True)
class RecoverBody:
    run_id: str
    limit: int = MAX_CONTROL_STEPS


@dataclass(frozen=True, slots=True)
class ResolveRecoveryBody:
    decision: UnknownOutcomeDecision

    @property
    def run_id(self) -> str:
        return self.decision.run_id


@dataclass(frozen=True, slots=True)
class TickBody:
    run_id: str
    max_steps: int = 1


RequestBody: TypeAlias = (
    SubmitBody | RunBody | EventsBody | RecoverBody | ResolveRecoveryBody | TickBody
)


@dataclass(frozen=True, slots=True)
class ProtocolRequest:
    protocol_version: int
    request_id: str
    operation: Operation
    body: RequestBody


@dataclass(frozen=True, slots=True)
class ProtocolResponse:
    protocol_version: int
    request_id: str | None
    ok: bool
    result: dict[str, Any] | None = None
    error: dict[str, str] | None = None

    @classmethod
    def success(
        cls,
        request_id: str,
        result: Mapping[str, Any],
    ) -> "ProtocolResponse":
        return cls(
            protocol_version=PROTOCOL_VERSION,
            request_id=request_id,
            ok=True,
            result=_detached_object(result, "response result"),
        )

    @classmethod
    def failure(
        cls,
        request_id: str | None,
        code: str,
        message: str,
    ) -> "ProtocolResponse":
        if not _SAFE_ID.fullmatch(code):
            raise ProtocolValidationError("error code is invalid")
        return cls(
            protocol_version=PROTOCOL_VERSION,
            request_id=request_id,
            ok=False,
            error={"code": code, "message": _bounded_text(message, "error message")},
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "protocol_version": self.protocol_version,
            "request_id": self.request_id,
            "ok": self.ok,
        }
        if self.ok:
            payload["result"] = self.result or {}
        else:
            payload["error"] = self.error or {
                "code": "operation_failed",
                "message": "operation failed",
            }
        encoded = _canonical_json(payload, "response")
        if len(encoded) > MAX_RESPONSE_BYTES:
            raise ProtocolValidationError("response exceeds size limit")
        return json.loads(encoded.decode("utf-8"))


def parse_request(payload: Mapping[str, Any]) -> ProtocolRequest:
    """Parse one strict JSON request without applying authorization."""

    detached = _detached_object(payload, "request")
    encoded = _canonical_json(detached, "request")
    if len(encoded) > MAX_REQUEST_BYTES:
        raise ProtocolValidationError("request exceeds size limit")
    _exact_fields(
        detached,
        {"protocol_version", "request_id", "operation", "body"},
        "request",
    )
    version = _bounded_int(
        detached["protocol_version"],
        "protocol_version",
        minimum=PROTOCOL_VERSION,
        maximum=PROTOCOL_VERSION,
    )
    request_id = _identifier(detached["request_id"], "request_id")
    try:
        operation = Operation(detached["operation"])
    except (TypeError, ValueError) as exc:
        raise ProtocolValidationError("operation is unsupported") from exc
    body = _object(detached["body"], "body")
    return ProtocolRequest(
        protocol_version=version,
        request_id=request_id,
        operation=operation,
        body=_parse_body(operation, body),
    )


def _parse_body(operation: Operation, body: dict[str, Any]) -> RequestBody:
    if operation is Operation.SUBMIT:
        _exact_fields(
            body,
            {"run_id", "workflow_ref", "input_receipt", "parent"},
            "submit body",
        )
        receipt_value = body["input_receipt"]
        parent_value = body["parent"]
        return SubmitBody(
            run_id=_identifier(body["run_id"], "run_id"),
            workflow_ref=_artifact_ref(body["workflow_ref"], "workflow_ref"),
            input_receipt=(
                None
                if receipt_value is None
                else _input_receipt(receipt_value)
            ),
            parent=None if parent_value is None else _parent(parent_value),
        )
    if operation is Operation.EVENTS:
        _exact_fields(body, {"run_id", "after_sequence", "limit"}, "events body")
        return EventsBody(
            run_id=_identifier(body["run_id"], "run_id"),
            after_sequence=_bounded_int(
                body["after_sequence"],
                "after_sequence",
                minimum=0,
                maximum=2**63 - 1,
            ),
            limit=_bounded_int(
                body["limit"],
                "limit",
                minimum=1,
                maximum=MAX_EVENT_PAGE_SIZE,
            ),
        )
    if operation is Operation.RECOVER:
        _exact_fields(body, {"run_id", "limit"}, "recover body")
        return RecoverBody(
            run_id=_identifier(body["run_id"], "run_id"),
            limit=_bounded_int(
                body["limit"],
                "limit",
                minimum=1,
                maximum=MAX_CONTROL_STEPS,
            ),
        )
    if operation is Operation.RESOLVE_RECOVERY:
        _exact_fields(
            body,
            {
                "resolution_id",
                "run_id",
                "node_id",
                "attempt_id",
                "resolution",
                "evidence_ref",
                "result_ref",
            },
            "resolve_recovery body",
        )
        try:
            decision = UnknownOutcomeDecision(
                resolution_id=_identifier(body["resolution_id"], "resolution_id"),
                run_id=_identifier(body["run_id"], "run_id"),
                node_id=_identifier(body["node_id"], "node_id"),
                attempt_id=_identifier(body["attempt_id"], "attempt_id"),
                resolution=UnknownOutcomeResolution(body["resolution"]),
                evidence_ref=_artifact_ref(body["evidence_ref"], "evidence_ref"),
                result_ref=(
                    None
                    if body["result_ref"] is None
                    else _artifact_ref(body["result_ref"], "result_ref")
                ),
            )
        except (RecoveryDecisionError, TypeError, ValueError) as exc:
            raise ProtocolValidationError(
                "resolve_recovery body is invalid"
            ) from exc
        return ResolveRecoveryBody(decision)
    if operation is Operation.TICK:
        _exact_fields(body, {"run_id", "max_steps"}, "tick body")
        return TickBody(
            run_id=_identifier(body["run_id"], "run_id"),
            max_steps=_bounded_int(
                body["max_steps"],
                "max_steps",
                minimum=1,
                maximum=MAX_CONTROL_STEPS,
            ),
        )
    _exact_fields(body, {"run_id"}, f"{operation.value} body")
    return RunBody(run_id=_identifier(body["run_id"], "run_id"))


def _input_receipt(value: Any) -> RunInputReceipt:
    payload = _object(value, "input_receipt")
    _exact_fields(payload, {"artifact_refs"}, "input_receipt")
    refs = payload["artifact_refs"]
    if not isinstance(refs, list) or not refs or len(refs) > 64:
        raise ProtocolValidationError(
            "input_receipt.artifact_refs must contain 1 to 64 entries"
        )
    return RunInputReceipt(
        tuple(
            _artifact_ref(item, f"input_receipt.artifact_refs[{index}]")
            for index, item in enumerate(refs)
        )
    )


def _artifact_ref(value: Any, field: str) -> ArtifactRef:
    payload = _object(value, field)
    _exact_fields(payload, _ARTIFACT_FIELDS, field)
    try:
        return ArtifactRef.from_dict(payload)
    except (TypeError, ValueError) as exc:
        raise ProtocolValidationError(f"{field} is invalid") from exc


def _parent(value: Any) -> ParentSubmission:
    payload = _object(value, "parent")
    _exact_fields(payload, {"parent_run_id", "parent_node_id"}, "parent")
    return ParentSubmission(
        parent_run_id=_identifier(payload["parent_run_id"], "parent_run_id"),
        parent_node_id=_identifier(payload["parent_node_id"], "parent_node_id"),
    )


def _exact_fields(
    payload: Mapping[str, Any],
    expected: set[str] | frozenset[str],
    field: str,
) -> None:
    actual = set(payload)
    if actual != set(expected):
        raise ProtocolValidationError(f"{field} fields do not match the schema")


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProtocolValidationError(f"{field} must be an object")
    if not all(isinstance(key, str) for key in value):
        raise ProtocolValidationError(f"{field} keys must be strings")
    return value


def _detached_object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ProtocolValidationError(f"{field} must be an object")
    encoded = _canonical_json(value, field)
    decoded = json.loads(encoded.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ProtocolValidationError(f"{field} must be an object")
    return decoded


def _canonical_json(value: Any, field: str) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ProtocolValidationError(f"{field} must be valid JSON") from exc


def _identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _SAFE_ID.fullmatch(value):
        raise ProtocolValidationError(f"{field} is invalid")
    return value


def validate_identifier(value: Any, field: str = "identifier") -> str:
    """Validate an ID shared by direct runtimes and serialized transports."""

    return _identifier(value, field)


def _bounded_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 160
        or any(ord(character) < 32 for character in value)
    ):
        raise ProtocolValidationError(f"{field} is invalid")
    return value


def _bounded_int(
    value: Any,
    field: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or value > maximum
    ):
        raise ProtocolValidationError(f"{field} is out of range")
    return value


__all__ = [
    "EventsBody",
    "MAX_CONTROL_STEPS",
    "MAX_EVENT_PAGE_SIZE",
    "MAX_REQUEST_BYTES",
    "MAX_RESPONSE_BYTES",
    "Operation",
    "PROTOCOL_VERSION",
    "ParentSubmission",
    "ProtocolRequest",
    "ProtocolResponse",
    "ProtocolValidationError",
    "RecoverBody",
    "ResolveRecoveryBody",
    "RunBody",
    "SubmitBody",
    "TickBody",
    "parse_request",
    "validate_identifier",
]
