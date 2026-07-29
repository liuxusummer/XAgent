"""Deterministic, side-effect-free replay for durable orchestration events.

This module intentionally depends only on read APIs.  Logical replay never
claims work, executes an Activity, resolves an approval, or writes projections.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Protocol, Sequence, runtime_checkable

from .event_types import DURABLE_EVENT_TYPES
from .models import (
    MODEL_SCHEMA_VERSION,
    AttemptRecord,
    JsonValue,
    ModelValidationError,
    NodeRecord,
    RunRecord,
    RunStatus,
    normalize_json,
)
_SENSITIVE_FIELDS = frozenset({"input", "output", "error", "metadata", "result"})
_TIMESTAMP_FIELDS = frozenset(
    {
        "created_at",
        "updated_at",
        "scheduled_at",
        "started_at",
        "finished_at",
        "occurred_at",
    }
)


class ReplayError(RuntimeError):
    """Base class for logical replay failures."""


class ReplayIntegrityError(ReplayError):
    """A persisted event stream cannot be safely interpreted."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        run_id: str,
        sequence: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.run_id = run_id
        self.sequence = sequence


@runtime_checkable
class ReplayStore(Protocol):
    """Minimal read-only store surface consumed by replay and comparison."""

    def list_events(
        self,
        run_id: str,
        *,
        after_seq: int = 0,
        limit: int = 1_000,
    ) -> Sequence[Any]: ...

    def get_run(self, run_id: str) -> RunRecord | None: ...

    def list_nodes(self, run_id: str) -> Sequence[NodeRecord]: ...

    def list_attempts(
        self,
        run_id: str,
        *,
        node_id: str | None = None,
    ) -> Sequence[AttemptRecord]: ...


@dataclass(frozen=True, slots=True)
class ProjectionDiff:
    """One stable, field-level difference between replay and a live projection."""

    object_type: str
    object_id: str
    field: str
    expected: JsonValue
    actual: JsonValue

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "object_type": self.object_type,
            "object_id": self.object_id,
            "field": self.field,
            "expected": normalize_json(self.expected),
            "actual": normalize_json(self.actual),
        }


@dataclass(frozen=True, slots=True)
class ReplaySnapshot:
    """Run/Node/Attempt state reduced from one complete event stream."""

    run: RunRecord
    nodes: tuple[NodeRecord, ...]
    attempts: tuple[AttemptRecord, ...]
    event_count: int
    last_sequence: int

    def to_dict(
        self,
        *,
        sanitized: bool = True,
        normalize_timestamps: bool = False,
    ) -> dict[str, JsonValue]:
        if sanitized:
            return _sanitized_snapshot_dict(
                self,
                normalize_timestamps=normalize_timestamps,
            )
        return {
            "schema_version": MODEL_SCHEMA_VERSION,
            "event_count": self.event_count,
            "last_sequence": self.last_sequence,
            "run": self.run.to_dict(),
            "nodes": [node.to_dict() for node in self.nodes],
            "attempts": [attempt.to_dict() for attempt in self.attempts],
        }


@dataclass(frozen=True, slots=True)
class ReplayReport:
    """A replay snapshot plus its comparison with the online projection."""

    run_id: str
    snapshot: ReplaySnapshot
    diffs: tuple[ProjectionDiff, ...]
    golden_digest: str

    @property
    def matches_live(self) -> bool:
        return not self.diffs

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "run_id": self.run_id,
            "matches_live": self.matches_live,
            "golden_digest": self.golden_digest,
            "diffs": [diff.to_dict() for diff in self.diffs],
            "snapshot": self.snapshot.to_dict(
                sanitized=True,
                normalize_timestamps=True,
            ),
        }


@dataclass(frozen=True, slots=True)
class ForkDescriptor:
    """Data required to initialize a new Run, without execution ownership."""

    source_run_id: str
    initial_run: RunRecord
    excluded_state: tuple[str, ...] = (
        "attempts",
        "idempotency_records",
        "tool_receipts",
        "approvals",
        "leases",
    )

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "source_run_id": self.source_run_id,
            "initial_run": self.initial_run.to_dict(),
            "excluded_state": list(self.excluded_state),
        }


def logical_replay(
    store: ReplayStore,
    run_id: str,
    *,
    page_size: int = 1_000,
    known_event_types: frozenset[str] = DURABLE_EVENT_TYPES,
) -> ReplaySnapshot:
    """Reduce canonical projection snapshots without invoking external work."""

    if page_size < 1:
        raise ValueError("page_size must be positive")

    run: RunRecord | None = None
    nodes: dict[str, NodeRecord] = {}
    attempts: dict[str, AttemptRecord] = {}
    expected_sequence = 1
    after_sequence = 0
    event_count = 0

    while True:
        try:
            batch = tuple(
                store.list_events(
                    run_id,
                    after_seq=after_sequence,
                    limit=page_size,
                )
            )
        except Exception as exc:
            raise ReplayIntegrityError(
                "event_content_invalid",
                "event storage integrity validation failed",
                run_id=run_id,
                sequence=expected_sequence,
            ) from exc
        if not batch:
            break

        for event in batch:
            sequence = _event_int(event, "seq", run_id, expected_sequence)
            if sequence != expected_sequence:
                raise ReplayIntegrityError(
                    "sequence_gap",
                    f"expected event sequence {expected_sequence}, got {sequence}",
                    run_id=run_id,
                    sequence=sequence,
                )
            if _event_text(event, "run_id", run_id, sequence) != run_id:
                raise ReplayIntegrityError(
                    "run_id_mismatch",
                    "event belongs to a different run",
                    run_id=run_id,
                    sequence=sequence,
                )
            schema_version = _event_int(
                event,
                "schema_version",
                run_id,
                sequence,
            )
            if schema_version != MODEL_SCHEMA_VERSION:
                raise ReplayIntegrityError(
                    "unknown_schema",
                    "event schema requires a replay migration",
                    run_id=run_id,
                    sequence=sequence,
                )
            event_type = _event_text(event, "event_type", run_id, sequence)
            if event_type not in known_event_types:
                raise ReplayIntegrityError(
                    "unknown_event_type",
                    f"event type {event_type!r} has no deterministic reducer",
                    run_id=run_id,
                    sequence=sequence,
                )

            payload = getattr(event, "payload", None)
            if not isinstance(payload, dict):
                raise ReplayIntegrityError(
                    "invalid_payload",
                    "event payload must be an object",
                    run_id=run_id,
                    sequence=sequence,
                )
            projection = payload.get("projection")
            if not isinstance(projection, dict):
                raise ReplayIntegrityError(
                    "missing_projection",
                    "event has no canonical projection snapshot",
                    run_id=run_id,
                    sequence=sequence,
                )
            projection_schema = projection.get(
                "schema_version",
                MODEL_SCHEMA_VERSION,
            )
            if projection_schema != MODEL_SCHEMA_VERSION:
                raise ReplayIntegrityError(
                    "unknown_projection_schema",
                    "projection schema requires a replay migration",
                    run_id=run_id,
                    sequence=sequence,
                )

            run = _decode_run(projection.get("run"), run_id, sequence)
            node = _decode_node(
                projection.get("node"),
                run_id,
                sequence,
                getattr(event, "node_id", None),
                required=event_type.startswith("node."),
            )
            if node is not None:
                nodes[node.node_id] = node
            attempt = _decode_attempt(
                projection.get("attempt"),
                run_id,
                sequence,
                getattr(event, "attempt_id", None),
                required=event_type.startswith("attempt."),
            )
            if attempt is not None:
                attempts[attempt.attempt_id] = attempt

            expected_sequence += 1
            event_count += 1
            after_sequence = sequence

    if event_count == 0 or run is None:
        raise ReplayIntegrityError(
            "run_not_found",
            "run has no replayable events",
            run_id=run_id,
        )
    return ReplaySnapshot(
        run=run,
        nodes=tuple(sorted(nodes.values(), key=lambda item: item.node_id)),
        attempts=tuple(
            sorted(
                attempts.values(),
                key=lambda item: (item.node_id, item.attempt_number, item.attempt_id),
            )
        ),
        event_count=event_count,
        last_sequence=expected_sequence - 1,
    )


def compare_live(
    store: ReplayStore,
    snapshot: ReplaySnapshot,
) -> tuple[ProjectionDiff, ...]:
    """Compare replayed state with live projection rows, field by field."""

    run_id = snapshot.run.run_id
    diffs: list[ProjectionDiff] = []
    _compare_record(
        diffs,
        "run",
        run_id,
        snapshot.run,
        store.get_run(run_id),
    )

    expected_nodes = {node.node_id: node for node in snapshot.nodes}
    actual_nodes = {node.node_id: node for node in store.list_nodes(run_id)}
    for node_id in sorted(expected_nodes.keys() | actual_nodes.keys()):
        _compare_record(
            diffs,
            "node",
            node_id,
            expected_nodes.get(node_id),
            actual_nodes.get(node_id),
        )

    expected_attempts = {
        attempt.attempt_id: attempt for attempt in snapshot.attempts
    }
    actual_attempts = {
        attempt.attempt_id: attempt for attempt in store.list_attempts(run_id)
    }
    for attempt_id in sorted(expected_attempts.keys() | actual_attempts.keys()):
        _compare_record(
            diffs,
            "attempt",
            attempt_id,
            expected_attempts.get(attempt_id),
            actual_attempts.get(attempt_id),
        )
    return tuple(
        sorted(
            diffs,
            key=lambda item: (item.object_type, item.object_id, item.field),
        )
    )


def build_replay_report(
    store: ReplayStore,
    run_id: str,
    *,
    page_size: int = 1_000,
) -> ReplayReport:
    snapshot = logical_replay(store, run_id, page_size=page_size)
    return ReplayReport(
        run_id=run_id,
        snapshot=snapshot,
        diffs=compare_live(store, snapshot),
        golden_digest=golden_digest(snapshot),
    )


def canonical_json(
    snapshot: ReplaySnapshot,
    *,
    normalize_timestamps: bool = False,
) -> str:
    """Export deterministic JSON with secret-bearing values replaced by refs."""

    return _canonical_json(
        snapshot.to_dict(
            sanitized=True,
            normalize_timestamps=normalize_timestamps,
        )
    )


def golden_digest(
    snapshot: ReplaySnapshot,
    *,
    normalize_timestamps: bool = True,
) -> str:
    """Digest the sanitized replay projection for golden-stream tests."""

    encoded = canonical_json(
        snapshot,
        normalize_timestamps=normalize_timestamps,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_fork_descriptor(
    snapshot: ReplaySnapshot,
    new_run_id: str,
    *,
    input_override: JsonValue = None,
    metadata: dict[str, JsonValue] | None = None,
    created_at: float = 0.0,
) -> ForkDescriptor:
    """Describe a fresh Run; no Attempt, approval, lease, or receipt is copied."""

    initial_run = RunRecord(
        run_id=new_run_id,
        workflow_id=snapshot.run.workflow_id,
        workflow_version=snapshot.run.workflow_version,
        definition_digest=snapshot.run.definition_digest,
        status=RunStatus.CREATED,
        input=input_override,
        metadata=metadata or {"forked_from_run_id": snapshot.run.run_id},
        created_at=created_at,
        updated_at=created_at,
        last_event_sequence=0,
        projection_version=0,
    )
    return ForkDescriptor(
        source_run_id=snapshot.run.run_id,
        initial_run=initial_run,
    )


def _decode_run(value: Any, run_id: str, sequence: int) -> RunRecord:
    if not isinstance(value, dict):
        raise ReplayIntegrityError(
            "missing_run_projection",
            "event has no Run projection snapshot",
            run_id=run_id,
            sequence=sequence,
        )
    try:
        record = RunRecord.from_dict(value)
    except (ModelValidationError, TypeError, ValueError) as exc:
        raise ReplayIntegrityError(
            "invalid_run_projection",
            "Run projection snapshot is invalid",
            run_id=run_id,
            sequence=sequence,
        ) from exc
    if record.run_id != run_id or record.last_event_sequence != sequence:
        raise ReplayIntegrityError(
            "run_projection_mismatch",
            "Run projection identity or sequence does not match its event",
            run_id=run_id,
            sequence=sequence,
        )
    return record


def _decode_node(
    value: Any,
    run_id: str,
    sequence: int,
    event_node_id: Any,
    *,
    required: bool,
) -> NodeRecord | None:
    if value is None:
        if required:
            raise ReplayIntegrityError(
                "missing_node_projection",
                "node event has no Node projection snapshot",
                run_id=run_id,
                sequence=sequence,
            )
        return None
    if not isinstance(value, dict):
        raise ReplayIntegrityError(
            "invalid_node_projection",
            "Node projection snapshot is invalid",
            run_id=run_id,
            sequence=sequence,
        )
    try:
        record = NodeRecord.from_dict(value)
    except (ModelValidationError, TypeError, ValueError) as exc:
        raise ReplayIntegrityError(
            "invalid_node_projection",
            "Node projection snapshot is invalid",
            run_id=run_id,
            sequence=sequence,
        ) from exc
    if (
        record.run_id != run_id
        or record.last_event_sequence != sequence
        or (event_node_id is not None and record.node_id != event_node_id)
    ):
        raise ReplayIntegrityError(
            "node_projection_mismatch",
            "Node projection identity or sequence does not match its event",
            run_id=run_id,
            sequence=sequence,
        )
    return record


def _decode_attempt(
    value: Any,
    run_id: str,
    sequence: int,
    event_attempt_id: Any,
    *,
    required: bool,
) -> AttemptRecord | None:
    if value is None:
        if required:
            raise ReplayIntegrityError(
                "missing_attempt_projection",
                "attempt event has no Attempt projection snapshot",
                run_id=run_id,
                sequence=sequence,
            )
        return None
    if not isinstance(value, dict):
        raise ReplayIntegrityError(
            "invalid_attempt_projection",
            "Attempt projection snapshot is invalid",
            run_id=run_id,
            sequence=sequence,
        )
    try:
        record = AttemptRecord.from_dict(value)
    except (ModelValidationError, TypeError, ValueError) as exc:
        raise ReplayIntegrityError(
            "invalid_attempt_projection",
            "Attempt projection snapshot is invalid",
            run_id=run_id,
            sequence=sequence,
        ) from exc
    if (
        record.run_id != run_id
        or record.last_event_sequence != sequence
        or (
            event_attempt_id is not None
            and record.attempt_id != event_attempt_id
        )
    ):
        raise ReplayIntegrityError(
            "attempt_projection_mismatch",
            "Attempt projection identity or sequence does not match its event",
            run_id=run_id,
            sequence=sequence,
        )
    return record


def _event_int(
    event: Any,
    field: str,
    run_id: str,
    sequence: int,
) -> int:
    value = getattr(event, field, None)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReplayIntegrityError(
            "invalid_event",
            f"event {field} is invalid",
            run_id=run_id,
            sequence=sequence,
        )
    return value


def _event_text(
    event: Any,
    field: str,
    run_id: str,
    sequence: int,
) -> str:
    value = getattr(event, field, None)
    if not isinstance(value, str) or not value:
        raise ReplayIntegrityError(
            "invalid_event",
            f"event {field} is invalid",
            run_id=run_id,
            sequence=sequence,
        )
    return value


def _compare_record(
    diffs: list[ProjectionDiff],
    object_type: str,
    object_id: str,
    expected: RunRecord | NodeRecord | AttemptRecord | None,
    actual: RunRecord | NodeRecord | AttemptRecord | None,
) -> None:
    if expected is None or actual is None:
        diffs.append(
            ProjectionDiff(
                object_type=object_type,
                object_id=object_id,
                field="$object",
                expected="missing" if expected is None else "present",
                actual="missing" if actual is None else "present",
            )
        )
        return
    expected_fields = expected.to_dict()
    actual_fields = actual.to_dict()
    for field in sorted(expected_fields.keys() | actual_fields.keys()):
        expected_value = expected_fields.get(field)
        actual_value = actual_fields.get(field)
        if expected_value != actual_value:
            diffs.append(
                ProjectionDiff(
                    object_type=object_type,
                    object_id=object_id,
                    field=field,
                    expected=_safe_diff_value(field, expected_value),
                    actual=_safe_diff_value(field, actual_value),
                )
            )


def _safe_diff_value(field: str, value: Any) -> JsonValue:
    if field in _SENSITIVE_FIELDS:
        return _value_ref(value)
    return normalize_json(value)


def _value_ref(value: Any) -> dict[str, JsonValue]:
    encoded = _canonical_json(normalize_json(value)).encode("utf-8")
    return {
        "redacted": True,
        "type": _json_type(value),
        "size": len(encoded),
    }


def _sanitized_snapshot_dict(
    snapshot: ReplaySnapshot,
    *,
    normalize_timestamps: bool,
) -> dict[str, JsonValue]:
    return {
        "schema_version": MODEL_SCHEMA_VERSION,
        "event_count": snapshot.event_count,
        "last_sequence": snapshot.last_sequence,
        "run": _sanitize_record(
            snapshot.run.to_dict(),
            normalize_timestamps=normalize_timestamps,
        ),
        "nodes": [
            _sanitize_record(
                node.to_dict(),
                normalize_timestamps=normalize_timestamps,
            )
            for node in snapshot.nodes
        ],
        "attempts": [
            _sanitize_record(
                attempt.to_dict(),
                normalize_timestamps=normalize_timestamps,
            )
            for attempt in snapshot.attempts
        ],
    }


def _sanitize_record(
    record: dict[str, JsonValue],
    *,
    normalize_timestamps: bool,
) -> dict[str, JsonValue]:
    sanitized: dict[str, JsonValue] = {}
    for field in sorted(record):
        value = record[field]
        if field in _SENSITIVE_FIELDS:
            sanitized[f"{field}_ref"] = _value_ref(value)
        elif field in {"idempotency_key", "worker_id", "lease_id"}:
            sanitized[f"{field}_ref"] = _value_ref(value)
        elif normalize_timestamps and field in _TIMESTAMP_FIELDS:
            sanitized[field] = None if value is None else 0
        else:
            sanitized[field] = normalize_json(value)
    return sanitized


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "string"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, list):
        return "array"
    return "object"


__all__ = [
    "ForkDescriptor",
    "ProjectionDiff",
    "ReplayError",
    "ReplayIntegrityError",
    "ReplayReport",
    "ReplaySnapshot",
    "ReplayStore",
    "build_fork_descriptor",
    "build_replay_report",
    "canonical_json",
    "compare_live",
    "golden_digest",
    "logical_replay",
]
