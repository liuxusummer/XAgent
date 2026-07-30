"""Versioned JSON metadata models for durable orchestration.

Only bounded metadata belongs in these records. Prompts, model responses,
stdout, files, and secrets must be stored as artifacts and referenced by ID.
"""

from __future__ import annotations

import json
import math
import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, TypeAlias

from .artifacts import ArtifactRef, ArtifactValidationError
from .metadata_security import (
    contains_sensitive_artifact_metadata,
    contains_sensitive_key,
)

MODEL_SCHEMA_VERSION = 1
MAX_JSON_BYTES = 64 * 1024
MAX_RUN_INPUT_ARTIFACT_REFS = 64
_ARTIFACT_REF_FIELDS = frozenset(ArtifactRef.__dataclass_fields__)

JsonValue: TypeAlias = (
    None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
)

class ModelValidationError(ValueError):
    """A durable model is malformed or cannot be represented as JSON."""


class RunStatus(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    WAITING_INPUT = "waiting_input"
    WAITING_APPROVAL = "waiting_approval"
    WAITING_RECOVERY = "waiting_recovery"
    CANCELLING = "cancelling"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in {self.COMPLETED, self.FAILED, self.CANCELLED}


class NodeStatus(StrEnum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    PAUSED = "paused"
    WAITING_RETRY = "waiting_retry"
    WAITING_INPUT = "waiting_input"
    WAITING_APPROVAL = "waiting_approval"
    WAITING_RECOVERY = "waiting_recovery"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"

    @property
    def is_terminal(self) -> bool:
        return self in {self.SUCCEEDED, self.FAILED, self.CANCELLED, self.SKIPPED}


class AttemptStatus(StrEnum):
    SCHEDULED = "scheduled"
    CLAIMED = "claimed"
    WAITING_APPROVAL = "waiting_approval"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    ABANDONED = "abandoned"
    OUTCOME_UNKNOWN = "outcome_unknown"

    @property
    def is_terminal(self) -> bool:
        return self in {
            self.SUCCEEDED,
            self.FAILED,
            self.TIMED_OUT,
            self.CANCELLED,
            self.ABANDONED,
            self.OUTCOME_UNKNOWN,
        }


class IdempotencyStatus(StrEnum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


class ClaimDisposition(StrEnum):
    ACQUIRED = "acquired"
    COMPLETED = "completed"
    CONFLICT = "conflict"


def utc_timestamp() -> float:
    return time.time()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _required_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ModelValidationError(f"{field_name} must not be empty")
    return text


def _optional_text(value: Any, field_name: str) -> str | None:
    return None if value is None else _required_text(value, field_name)


def _timestamp(value: Any, field_name: str) -> float:
    try:
        timestamp = float(value)
    except (TypeError, ValueError) as exc:
        raise ModelValidationError(f"{field_name} must be a finite timestamp") from exc
    if not math.isfinite(timestamp) or timestamp < 0:
        raise ModelValidationError(f"{field_name} must be a finite timestamp")
    return timestamp


def _optional_timestamp(value: Any, field_name: str) -> float | None:
    return None if value is None else _timestamp(value, field_name)


def _non_negative_int(value: Any, field_name: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ModelValidationError(f"{field_name} must be a non-negative integer") from exc
    if number < 0:
        raise ModelValidationError(f"{field_name} must be a non-negative integer")
    return number


def _schema_version(value: Any) -> int:
    version = _non_negative_int(value, "schema_version")
    if version < 1 or version > MODEL_SCHEMA_VERSION:
        raise ModelValidationError(
            f"unsupported model schema version {version}; current={MODEL_SCHEMA_VERSION}"
        )
    return version


def normalize_json(value: Any, field_name: str = "value") -> JsonValue:
    """Validate, size-bound, and detach a JSON value."""

    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        if len(encoded.encode("utf-8")) > MAX_JSON_BYTES:
            raise ModelValidationError(
                f"{field_name} exceeds the {MAX_JSON_BYTES}-byte metadata limit"
            )
        normalized = json.loads(encoded)
        if contains_sensitive_artifact_metadata(normalized):
            raise ModelValidationError(
                f"{field_name} contains forbidden Artifact metadata"
            )
        return normalized
    except ModelValidationError:
        raise
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ModelValidationError(f"{field_name} must be JSON serializable") from exc


def _json_object(value: Any, field_name: str) -> dict[str, JsonValue]:
    normalized = normalize_json(value, field_name)
    if not isinstance(normalized, dict):
        raise ModelValidationError(f"{field_name} must be a JSON object")
    return normalized


def normalize_metadata(
    value: Any,
    field_name: str = "metadata",
) -> dict[str, JsonValue]:
    """Normalize metadata and reject credential-shaped keys at any depth.

    Metadata is durable control-plane state, not a secret transport.  Values
    under credential-shaped keys are rejected rather than silently redacted so
    canonical identity and replay cannot collapse distinct submissions.
    """

    normalized = _json_object(value, field_name)
    if contains_sensitive_key(normalized):
        raise ModelValidationError(
            f"{field_name} contains a forbidden sensitive key"
        )
    return normalized


def normalize_run_input(value: Any) -> JsonValue:
    """Accept only a bounded canonical Artifact input envelope."""

    if value is None:
        return None
    normalized = normalize_json(value, "input")
    if (
        not isinstance(normalized, dict)
        or set(normalized) != {"kind", "artifact_refs"}
        or normalized.get("kind") != "artifact_input"
    ):
        raise ModelValidationError(
            "input must be null or a canonical artifact_input envelope"
        )
    raw_refs = normalized.get("artifact_refs")
    if (
        not isinstance(raw_refs, list)
        or not 1 <= len(raw_refs) <= MAX_RUN_INPUT_ARTIFACT_REFS
    ):
        raise ModelValidationError(
            "input artifact_refs must contain between 1 and 64 references"
        )
    canonical_refs: list[dict[str, Any]] = []
    for raw_ref in raw_refs:
        if not isinstance(raw_ref, dict) or set(raw_ref) != set(_ARTIFACT_REF_FIELDS):
            raise ModelValidationError(
                "input artifact_refs must contain complete ArtifactRef objects"
            )
        try:
            canonical_ref = ArtifactRef.from_dict(raw_ref).to_dict()
        except ArtifactValidationError as exc:
            raise ModelValidationError(
                "input artifact_refs contain an invalid ArtifactRef"
            ) from exc
        if json.dumps(
            raw_ref,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ) != json.dumps(
            canonical_ref,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ):
            raise ModelValidationError(
                "input artifact_refs must use canonical ArtifactRef values"
            )
        canonical_refs.append(canonical_ref)
    return {
        "kind": "artifact_input",
        "artifact_refs": canonical_refs,
    }


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: str
    workflow_id: str
    workflow_version: int = 1
    definition_digest: str = ""
    status: RunStatus = RunStatus.CREATED
    input: JsonValue = None
    output: JsonValue = None
    error: JsonValue = None
    metadata: dict[str, JsonValue] = field(default_factory=dict)
    created_at: float = field(default_factory=utc_timestamp)
    updated_at: float = field(default_factory=utc_timestamp)
    last_event_sequence: int = 0
    projection_version: int = 0
    schema_version: int = MODEL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _required_text(self.run_id, "run_id"))
        object.__setattr__(self, "workflow_id", _required_text(self.workflow_id, "workflow_id"))
        workflow_version = _non_negative_int(self.workflow_version, "workflow_version")
        if workflow_version < 1:
            raise ModelValidationError("workflow_version must be a positive integer")
        object.__setattr__(self, "workflow_version", workflow_version)
        definition_digest = _required_text(self.definition_digest, "definition_digest")
        if len(definition_digest) != 64 or any(
            char not in "0123456789abcdef" for char in definition_digest.lower()
        ):
            raise ModelValidationError("definition_digest must be a SHA-256 hex digest")
        object.__setattr__(self, "definition_digest", definition_digest.lower())
        try:
            object.__setattr__(self, "status", RunStatus(self.status))
        except ValueError as exc:
            raise ModelValidationError(f"invalid run status: {self.status}") from exc
        object.__setattr__(self, "input", normalize_run_input(self.input))
        object.__setattr__(self, "output", normalize_json(self.output, "output"))
        object.__setattr__(self, "error", normalize_json(self.error, "error"))
        object.__setattr__(
            self,
            "metadata",
            normalize_metadata(self.metadata),
        )
        created_at = _timestamp(self.created_at, "created_at")
        updated_at = _timestamp(self.updated_at, "updated_at")
        if updated_at < created_at:
            raise ModelValidationError("updated_at must not be earlier than created_at")
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(
            self,
            "last_event_sequence",
            _non_negative_int(self.last_event_sequence, "last_event_sequence"),
        )
        object.__setattr__(
            self,
            "projection_version",
            _non_negative_int(self.projection_version, "projection_version"),
        )
        object.__setattr__(self, "schema_version", _schema_version(self.schema_version))

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "workflow_id": self.workflow_id,
            "workflow_version": self.workflow_version,
            "definition_digest": self.definition_digest,
            "status": self.status.value,
            "input": normalize_json(self.input),
            "output": normalize_json(self.output),
            "error": normalize_json(self.error),
            "metadata": normalize_json(self.metadata),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_event_sequence": self.last_event_sequence,
            "projection_version": self.projection_version,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RunRecord":
        return cls(
            run_id=payload.get("run_id", ""),
            workflow_id=payload.get("workflow_id", ""),
            workflow_version=payload.get("workflow_version", 1),
            definition_digest=payload.get("definition_digest", ""),
            status=payload.get("status", RunStatus.CREATED),
            input=payload.get("input"),
            output=payload.get("output"),
            error=payload.get("error"),
            metadata=payload.get("metadata", {}),
            created_at=payload.get("created_at", utc_timestamp()),
            updated_at=payload.get("updated_at", utc_timestamp()),
            last_event_sequence=payload.get("last_event_sequence", 0),
            projection_version=payload.get("projection_version", 0),
            schema_version=payload.get("schema_version", MODEL_SCHEMA_VERSION),
        )


@dataclass(frozen=True, slots=True)
class NodeRecord:
    run_id: str
    node_id: str
    node_type: str
    status: NodeStatus = NodeStatus.PENDING
    input: JsonValue = None
    output: JsonValue = None
    error: JsonValue = None
    metadata: dict[str, JsonValue] = field(default_factory=dict)
    attempt_count: int = 0
    created_at: float = field(default_factory=utc_timestamp)
    updated_at: float = field(default_factory=utc_timestamp)
    last_event_sequence: int = 0
    projection_version: int = 0
    schema_version: int = MODEL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _required_text(self.run_id, "run_id"))
        object.__setattr__(self, "node_id", _required_text(self.node_id, "node_id"))
        object.__setattr__(self, "node_type", _required_text(self.node_type, "node_type"))
        try:
            object.__setattr__(self, "status", NodeStatus(self.status))
        except ValueError as exc:
            raise ModelValidationError(f"invalid node status: {self.status}") from exc
        object.__setattr__(self, "input", normalize_json(self.input, "input"))
        object.__setattr__(self, "output", normalize_json(self.output, "output"))
        object.__setattr__(self, "error", normalize_json(self.error, "error"))
        object.__setattr__(
            self,
            "metadata",
            normalize_metadata(self.metadata),
        )
        object.__setattr__(
            self,
            "attempt_count",
            _non_negative_int(self.attempt_count, "attempt_count"),
        )
        created_at = _timestamp(self.created_at, "created_at")
        updated_at = _timestamp(self.updated_at, "updated_at")
        if updated_at < created_at:
            raise ModelValidationError("updated_at must not be earlier than created_at")
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(
            self,
            "last_event_sequence",
            _non_negative_int(self.last_event_sequence, "last_event_sequence"),
        )
        object.__setattr__(
            self,
            "projection_version",
            _non_negative_int(self.projection_version, "projection_version"),
        )
        object.__setattr__(self, "schema_version", _schema_version(self.schema_version))

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "node_id": self.node_id,
            "node_type": self.node_type,
            "status": self.status.value,
            "input": normalize_json(self.input),
            "output": normalize_json(self.output),
            "error": normalize_json(self.error),
            "metadata": normalize_json(self.metadata),
            "attempt_count": self.attempt_count,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_event_sequence": self.last_event_sequence,
            "projection_version": self.projection_version,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "NodeRecord":
        return cls(
            run_id=payload.get("run_id", ""),
            node_id=payload.get("node_id", ""),
            node_type=payload.get("node_type", ""),
            status=payload.get("status", NodeStatus.PENDING),
            input=payload.get("input"),
            output=payload.get("output"),
            error=payload.get("error"),
            metadata=payload.get("metadata", {}),
            attempt_count=payload.get("attempt_count", 0),
            created_at=payload.get("created_at", utc_timestamp()),
            updated_at=payload.get("updated_at", utc_timestamp()),
            last_event_sequence=payload.get("last_event_sequence", 0),
            projection_version=payload.get("projection_version", 0),
            schema_version=payload.get("schema_version", MODEL_SCHEMA_VERSION),
        )


@dataclass(frozen=True, slots=True, repr=False)
class AttemptRecord:
    attempt_id: str
    run_id: str
    node_id: str
    attempt_number: int
    idempotency_key: str = ""
    activity_kind: str = "agent"
    effect_class: str = "non_idempotent_write"
    status: AttemptStatus = AttemptStatus.SCHEDULED
    worker_id: str | None = None
    lease_id: str | None = None
    fencing_token: int = 0
    result: JsonValue = None
    error: JsonValue = None
    metadata: dict[str, JsonValue] = field(default_factory=dict)
    scheduled_at: float = field(default_factory=utc_timestamp)
    started_at: float | None = None
    finished_at: float | None = None
    last_event_sequence: int = 0
    projection_version: int = 0
    schema_version: int = MODEL_SCHEMA_VERSION

    def __repr__(self) -> str:
        """Summarize durable state without rendering lease credentials or payloads."""

        return (
            f"AttemptRecord(attempt_id={self.attempt_id!r}, "
            f"run_id={self.run_id!r}, node_id={self.node_id!r}, "
            f"attempt_number={self.attempt_number}, status={self.status.value!r}, "
            f"worker_id={self.worker_id!r}, lease_bound={self.lease_id is not None}, "
            f"fencing_token={self.fencing_token})"
        )

    def __post_init__(self) -> None:
        object.__setattr__(self, "attempt_id", _required_text(self.attempt_id, "attempt_id"))
        object.__setattr__(self, "run_id", _required_text(self.run_id, "run_id"))
        object.__setattr__(self, "node_id", _required_text(self.node_id, "node_id"))
        attempt_number = _non_negative_int(self.attempt_number, "attempt_number")
        if attempt_number < 1:
            raise ModelValidationError("attempt_number must be a positive integer")
        object.__setattr__(self, "attempt_number", attempt_number)
        idempotency_key = self.idempotency_key or f"{self.run_id}:{self.node_id}"
        object.__setattr__(
            self,
            "idempotency_key",
            _required_text(idempotency_key, "idempotency_key"),
        )
        object.__setattr__(
            self,
            "activity_kind",
            _required_text(self.activity_kind, "activity_kind"),
        )
        if self.effect_class not in {
            "read_only",
            "idempotent_write",
            "non_idempotent_write",
            "destructive",
        }:
            raise ModelValidationError(f"invalid effect_class: {self.effect_class}")
        try:
            object.__setattr__(self, "status", AttemptStatus(self.status))
        except ValueError as exc:
            raise ModelValidationError(f"invalid attempt status: {self.status}") from exc
        object.__setattr__(self, "worker_id", _optional_text(self.worker_id, "worker_id"))
        object.__setattr__(self, "lease_id", _optional_text(self.lease_id, "lease_id"))
        object.__setattr__(
            self,
            "fencing_token",
            _non_negative_int(self.fencing_token, "fencing_token"),
        )
        object.__setattr__(self, "result", normalize_json(self.result, "result"))
        object.__setattr__(self, "error", normalize_json(self.error, "error"))
        object.__setattr__(
            self,
            "metadata",
            normalize_metadata(self.metadata),
        )
        scheduled_at = _timestamp(self.scheduled_at, "scheduled_at")
        started_at = _optional_timestamp(self.started_at, "started_at")
        finished_at = _optional_timestamp(self.finished_at, "finished_at")
        if started_at is not None and started_at < scheduled_at:
            raise ModelValidationError("started_at must not be earlier than scheduled_at")
        if finished_at is not None and finished_at < (started_at or scheduled_at):
            raise ModelValidationError("finished_at must not precede attempt start")
        if self.status.is_terminal and finished_at is None:
            raise ModelValidationError("terminal attempt must have finished_at")
        if self.status is AttemptStatus.SCHEDULED and any(
            value is not None for value in (self.worker_id, self.lease_id, started_at, finished_at)
        ):
            raise ModelValidationError("SCHEDULED attempt cannot have owner or execution times")
        if self.status is AttemptStatus.WAITING_APPROVAL:
            if (
                self.worker_id is not None
                or self.lease_id is not None
                or started_at is not None
                or finished_at is not None
            ):
                raise ModelValidationError(
                    "WAITING_APPROVAL attempt must not retain execution authority"
                )
        if self.status in {AttemptStatus.CLAIMED, AttemptStatus.RUNNING}:
            if self.worker_id is None or self.lease_id is None or self.fencing_token < 1:
                raise ModelValidationError(
                    f"{self.status.value} attempt requires owner, lease, and fencing token"
                )
        if self.status is AttemptStatus.CLAIMED and (
            started_at is not None or finished_at is not None
        ):
            raise ModelValidationError("CLAIMED attempt cannot have execution times")
        if self.status is AttemptStatus.RUNNING and (
            started_at is None or finished_at is not None
        ):
            raise ModelValidationError("RUNNING attempt requires started_at and no finished_at")
        object.__setattr__(self, "scheduled_at", scheduled_at)
        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "finished_at", finished_at)
        object.__setattr__(
            self,
            "last_event_sequence",
            _non_negative_int(self.last_event_sequence, "last_event_sequence"),
        )
        object.__setattr__(
            self,
            "projection_version",
            _non_negative_int(self.projection_version, "projection_version"),
        )
        object.__setattr__(self, "schema_version", _schema_version(self.schema_version))

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "schema_version": self.schema_version,
            "attempt_id": self.attempt_id,
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_number": self.attempt_number,
            "idempotency_key": self.idempotency_key,
            "activity_kind": self.activity_kind,
            "effect_class": self.effect_class,
            "status": self.status.value,
            "worker_id": self.worker_id,
            "lease_id": self.lease_id,
            "fencing_token": self.fencing_token,
            "result": normalize_json(self.result),
            "error": normalize_json(self.error),
            "metadata": normalize_json(self.metadata),
            "scheduled_at": self.scheduled_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "last_event_sequence": self.last_event_sequence,
            "projection_version": self.projection_version,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "AttemptRecord":
        return cls(
            attempt_id=payload.get("attempt_id", ""),
            run_id=payload.get("run_id", ""),
            node_id=payload.get("node_id", ""),
            attempt_number=payload.get("attempt_number", 0),
            idempotency_key=payload.get("idempotency_key", ""),
            activity_kind=payload.get("activity_kind", "agent"),
            effect_class=payload.get("effect_class", "non_idempotent_write"),
            status=payload.get("status", AttemptStatus.SCHEDULED),
            worker_id=payload.get("worker_id"),
            lease_id=payload.get("lease_id"),
            fencing_token=payload.get("fencing_token", 0),
            result=payload.get("result"),
            error=payload.get("error"),
            metadata=payload.get("metadata", {}),
            scheduled_at=payload.get("scheduled_at", utc_timestamp()),
            started_at=payload.get("started_at"),
            finished_at=payload.get("finished_at"),
            last_event_sequence=payload.get("last_event_sequence", 0),
            projection_version=payload.get("projection_version", 0),
            schema_version=payload.get("schema_version", MODEL_SCHEMA_VERSION),
        )


@dataclass(frozen=True, slots=True)
class EventRecord:
    run_id: str
    seq: int
    event_type: str
    payload: dict[str, JsonValue] = field(default_factory=dict)
    node_id: str | None = None
    attempt_id: str | None = None
    event_id: str = field(default_factory=lambda: new_id("evt"))
    occurred_at: float = field(default_factory=utc_timestamp)
    schema_version: int = MODEL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _required_text(self.run_id, "run_id"))
        seq = _non_negative_int(self.seq, "seq")
        if seq < 1:
            raise ModelValidationError("seq must be a positive integer")
        object.__setattr__(self, "seq", seq)
        object.__setattr__(self, "event_type", _required_text(self.event_type, "event_type"))
        object.__setattr__(self, "payload", _json_object(self.payload, "payload"))
        object.__setattr__(self, "node_id", _optional_text(self.node_id, "node_id"))
        object.__setattr__(self, "attempt_id", _optional_text(self.attempt_id, "attempt_id"))
        object.__setattr__(self, "event_id", _required_text(self.event_id, "event_id"))
        object.__setattr__(self, "occurred_at", _timestamp(self.occurred_at, "occurred_at"))
        object.__setattr__(self, "schema_version", _schema_version(self.schema_version))

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "run_id": self.run_id,
            "seq": self.seq,
            "event_type": self.event_type,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "payload": normalize_json(self.payload),
            "occurred_at": self.occurred_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "EventRecord":
        return cls(
            event_id=payload.get("event_id", ""),
            run_id=payload.get("run_id", ""),
            seq=payload.get("seq", 0),
            event_type=payload.get("event_type", ""),
            node_id=payload.get("node_id"),
            attempt_id=payload.get("attempt_id"),
            payload=payload.get("payload", {}),
            occurred_at=payload.get("occurred_at", utc_timestamp()),
            schema_version=payload.get("schema_version", MODEL_SCHEMA_VERSION),
        )


@dataclass(frozen=True, slots=True, repr=False)
class IdempotencyRecord:
    run_id: str
    key: str
    request_hash: str
    status: IdempotencyStatus
    owner_id: str
    claim_token: str
    lease_expires_at: float
    result: JsonValue = None
    claim_count: int = 1
    created_at: float = field(default_factory=utc_timestamp)
    updated_at: float = field(default_factory=utc_timestamp)
    completed_at: float | None = None
    schema_version: int = MODEL_SCHEMA_VERSION

    def __repr__(self) -> str:
        """Summarize claim state without rendering its live bearer credential."""

        return (
            f"IdempotencyRecord(run_id={self.run_id!r}, "
            f"status={self.status.value!r}, owner_id={self.owner_id!r}, "
            f"claim_bound={bool(self.claim_token)}, claim_count={self.claim_count}, "
            f"lease_expires_at={self.lease_expires_at!r})"
        )

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _required_text(self.run_id, "run_id"))
        object.__setattr__(self, "key", _required_text(self.key, "key"))
        object.__setattr__(
            self,
            "request_hash",
            _required_text(self.request_hash, "request_hash"),
        )
        try:
            object.__setattr__(self, "status", IdempotencyStatus(self.status))
        except ValueError as exc:
            raise ModelValidationError(f"invalid idempotency status: {self.status}") from exc
        object.__setattr__(self, "owner_id", _required_text(self.owner_id, "owner_id"))
        object.__setattr__(
            self,
            "claim_token",
            _required_text(self.claim_token, "claim_token"),
        )
        object.__setattr__(
            self,
            "lease_expires_at",
            _timestamp(self.lease_expires_at, "lease_expires_at"),
        )
        object.__setattr__(self, "result", normalize_json(self.result, "result"))
        claim_count = _non_negative_int(self.claim_count, "claim_count")
        if claim_count < 1:
            raise ModelValidationError("claim_count must be a positive integer")
        object.__setattr__(self, "claim_count", claim_count)
        created_at = _timestamp(self.created_at, "created_at")
        updated_at = _timestamp(self.updated_at, "updated_at")
        completed_at = _optional_timestamp(self.completed_at, "completed_at")
        if updated_at < created_at:
            raise ModelValidationError("updated_at must not be earlier than created_at")
        if completed_at is not None and completed_at < created_at:
            raise ModelValidationError("completed_at must not be earlier than created_at")
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "completed_at", completed_at)
        object.__setattr__(self, "schema_version", _schema_version(self.schema_version))


@dataclass(frozen=True, slots=True)
class IdempotencyClaim:
    disposition: ClaimDisposition
    record: IdempotencyRecord
