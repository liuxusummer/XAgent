"""SQLite-backed durable orchestration metadata store.

Every method opens a short-lived connection.  Connections are never shared
between threads, while SQLite WAL and ``BEGIN IMMEDIATE`` serialize writers.
External work must never run inside these transactions.

The store provides *at-least-once* execution support, not exactly-once side
effects.  An operation that may have completed before its receipt was committed
must move to ``WAITING_RECOVERY`` unless it is safe to probe or retry with a
stable idempotency key.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import re
import sqlite3
import threading
import time
from contextlib import closing, contextmanager
from dataclasses import dataclass, replace
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterator, Mapping, Sequence

from .artifacts import ArtifactRef
from .event_types import DURABLE_EVENT_TYPES
from .models import (
    MODEL_SCHEMA_VERSION,
    AttemptRecord,
    AttemptStatus,
    ClaimDisposition,
    EventRecord,
    IdempotencyClaim,
    IdempotencyRecord,
    IdempotencyStatus,
    JsonValue,
    ModelValidationError,
    NodeRecord,
    NodeStatus,
    RunRecord,
    RunStatus,
    new_id,
    normalize_json,
    normalize_run_input,
    utc_timestamp,
)

if TYPE_CHECKING:
    from .executor import ToolReceipt

STORE_SCHEMA_VERSION = 4
DEFAULT_BUSY_TIMEOUT_MS = 10_000
DEFAULT_EVENT_LIMIT = 1_000
MAX_EVENT_LIMIT = 10_000
MAX_PROJECTION_REPLAY_EVENTS = 100_000
MAX_PROJECTION_REPLAY_PAYLOAD_BYTES = 64 * 1024 * 1024
MAX_PROJECTION_REPLAY_SECONDS = 30.0
MAX_PROJECTION_REPLAY_PAGE = 1_000
MAX_ACTIVITY_LEASE_SECONDS = 24 * 60 * 60
_SAFE_RECEIPT_CODE = re.compile(r"^[a-z0-9][a-z0-9_.:-]{0,127}$")
_SHA256_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GC_QUARANTINE_ID = re.compile(r"^q[0-9]{20}_[0-9a-f]{32}$")
_ARTIFACT_REF_FIELDS = frozenset(ArtifactRef.__dataclass_fields__)
_DEADLINE_KINDS = frozenset(
    {"run", "schedule", "start", "execution", "heartbeat"}
)
_ACTIVITY_COMMIT_REJECTION_REASONS = frozenset(
    {
        "claim_expired",
        "claim_owner_mismatch",
        "claim_terminal",
        "claim_token_mismatch",
        "fencing_token_mismatch",
    }
)

_RUN_TRANSITIONS = {
    RunStatus.CREATED: {RunStatus.RUNNING, RunStatus.CANCELLING, RunStatus.FAILED},
    RunStatus.RUNNING: {
        RunStatus.PAUSING,
        RunStatus.WAITING_INPUT,
        RunStatus.WAITING_APPROVAL,
        RunStatus.WAITING_RECOVERY,
        RunStatus.CANCELLING,
        RunStatus.COMPLETED,
        RunStatus.FAILED,
    },
    RunStatus.PAUSING: {RunStatus.PAUSED, RunStatus.CANCELLING},
    RunStatus.PAUSED: {RunStatus.RUNNING, RunStatus.CANCELLING},
    RunStatus.WAITING_INPUT: {RunStatus.RUNNING, RunStatus.CANCELLING},
    RunStatus.WAITING_APPROVAL: {
        RunStatus.RUNNING,
        RunStatus.CANCELLING,
        RunStatus.FAILED,
    },
    RunStatus.WAITING_RECOVERY: {
        RunStatus.RUNNING,
        RunStatus.CANCELLING,
        RunStatus.FAILED,
    },
    RunStatus.CANCELLING: {
        RunStatus.CANCELLED,
        RunStatus.WAITING_RECOVERY,
        RunStatus.FAILED,
    },
    RunStatus.COMPLETED: set(),
    RunStatus.FAILED: set(),
    RunStatus.CANCELLED: set(),
}

_NODE_TRANSITIONS = {
    NodeStatus.PENDING: {NodeStatus.READY, NodeStatus.SKIPPED, NodeStatus.CANCELLED},
    NodeStatus.READY: {
        NodeStatus.RUNNING,
        NodeStatus.WAITING_APPROVAL,
        NodeStatus.SKIPPED,
        NodeStatus.CANCELLED,
    },
    NodeStatus.RUNNING: {
        NodeStatus.SUCCEEDED,
        NodeStatus.PAUSED,
        NodeStatus.WAITING_RETRY,
        NodeStatus.WAITING_INPUT,
        NodeStatus.WAITING_APPROVAL,
        NodeStatus.WAITING_RECOVERY,
        NodeStatus.FAILED,
        NodeStatus.CANCELLED,
    },
    NodeStatus.PAUSED: {NodeStatus.READY, NodeStatus.CANCELLED},
    NodeStatus.WAITING_RETRY: {NodeStatus.READY, NodeStatus.CANCELLED},
    NodeStatus.WAITING_INPUT: {NodeStatus.READY, NodeStatus.CANCELLED},
    NodeStatus.WAITING_APPROVAL: {
        NodeStatus.READY,
        NodeStatus.FAILED,
        NodeStatus.CANCELLED,
    },
    NodeStatus.WAITING_RECOVERY: {
        NodeStatus.READY,
        NodeStatus.FAILED,
        NodeStatus.CANCELLED,
    },
    NodeStatus.SUCCEEDED: set(),
    NodeStatus.FAILED: set(),
    NodeStatus.CANCELLED: set(),
    NodeStatus.SKIPPED: set(),
}

_ATTEMPT_TRANSITIONS = {
    AttemptStatus.SCHEDULED: {
        AttemptStatus.CLAIMED,
        AttemptStatus.WAITING_APPROVAL,
        AttemptStatus.CANCELLED,
    },
    AttemptStatus.CLAIMED: {
        AttemptStatus.WAITING_APPROVAL,
        AttemptStatus.RUNNING,
        AttemptStatus.FAILED,
        AttemptStatus.ABANDONED,
        AttemptStatus.CANCELLED,
    },
    AttemptStatus.WAITING_APPROVAL: {
        AttemptStatus.SCHEDULED,
        AttemptStatus.FAILED,
        AttemptStatus.CANCELLED,
    },
    AttemptStatus.RUNNING: {
        AttemptStatus.SUCCEEDED,
        AttemptStatus.FAILED,
        AttemptStatus.TIMED_OUT,
        AttemptStatus.CANCELLED,
        AttemptStatus.ABANDONED,
        AttemptStatus.OUTCOME_UNKNOWN,
    },
    AttemptStatus.SUCCEEDED: set(),
    AttemptStatus.FAILED: set(),
    AttemptStatus.TIMED_OUT: set(),
    AttemptStatus.CANCELLED: set(),
    AttemptStatus.ABANDONED: set(),
    AttemptStatus.OUTCOME_UNKNOWN: set(),
}

_RUN_EVENT_STATUS = {
    "run.started": RunStatus.RUNNING,
    "run.pausing": RunStatus.PAUSING,
    "run.paused": RunStatus.PAUSED,
    "run.cancelling": RunStatus.CANCELLING,
    "run.completed": RunStatus.COMPLETED,
    "run.failed": RunStatus.FAILED,
    "run.cancelled": RunStatus.CANCELLED,
    "run.waiting_recovery": RunStatus.WAITING_RECOVERY,
    "run.recovery_resolved": RunStatus.RUNNING,
}
_NODE_EVENT_STATUS = {
    "node.created": NodeStatus.PENDING,
    "node.pending": NodeStatus.PENDING,
    "node.ready": NodeStatus.READY,
    "node.started": NodeStatus.RUNNING,
    "node.paused": NodeStatus.PAUSED,
    "node.succeeded": NodeStatus.SUCCEEDED,
    "node.failed": NodeStatus.FAILED,
    "node.cancelled": NodeStatus.CANCELLED,
    "node.waiting_recovery": NodeStatus.WAITING_RECOVERY,
    "node.waiting_retry": NodeStatus.WAITING_RETRY,
    "node.waiting_input": NodeStatus.WAITING_INPUT,
    "node.waiting_approval": NodeStatus.WAITING_APPROVAL,
    "node.skipped": NodeStatus.SKIPPED,
}
_ATTEMPT_EVENT_STATUS = {
    f"attempt.{status.value}": status for status in AttemptStatus
}
_ATTEMPT_EVENT_STATUS["attempt.started"] = AttemptStatus.RUNNING


class OrchestrationStoreError(RuntimeError):
    pass


class StoreSchemaError(OrchestrationStoreError):
    pass


class ProjectionReplayLimitError(StoreSchemaError):
    """A bounded projection replay exhausted an operator-safe budget."""

    _REASON_CODES = frozenset(
        {
            "event_count_limit",
            "payload_bytes_limit",
            "wall_time_limit",
        }
    )

    def __init__(self, reason_code: str) -> None:
        if reason_code not in self._REASON_CODES:
            raise ValueError("invalid projection replay limit reason")
        self.reason_code = reason_code
        super().__init__(reason_code)


class RunAlreadyExistsError(OrchestrationStoreError):
    pass


class RunNotFoundError(OrchestrationStoreError):
    pass


class RunHierarchyLimitError(OrchestrationStoreError):
    """Atomic child Run admission reached a persisted hierarchy limit."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class ProjectionConflictError(OrchestrationStoreError):
    pass


class ConcurrentProjectionUpdate(ProjectionConflictError):
    """A compare-and-swap snapshot lost to a committed concurrent Event."""

    def __init__(
        self,
        scope: str,
        expected_version: int | None,
        current_version: int | None,
    ) -> None:
        self.scope = scope
        self.expected_version = expected_version
        self.current_version = current_version
        super().__init__(
            f"stale {scope} projection: expected {expected_version}, "
            f"current {current_version}"
        )


class InvalidStateTransition(ProjectionConflictError):
    pass


class IdempotencyConflictError(OrchestrationStoreError):
    pass


class ArtifactGCReferenceConflictError(OrchestrationStoreError):
    """A durable Artifact reference raced with a GC claim."""


class WorkflowBindingConflictError(OrchestrationStoreError):
    """A workflow identity/version is already bound to different content."""


class ActivityAdmissionDenied(OrchestrationStoreError):
    """A scheduled Activity did not acquire bounded execution capacity."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


_SCHEMA_LOCK = threading.Lock()


@dataclass(frozen=True, slots=True)
class ArtifactGCClaimRecord:
    sha256: str
    quarantine_id: str
    size: int
    claimed_at: float
    state: str


@dataclass(frozen=True, slots=True)
class WorkflowBindingRecord:
    workflow_id: str
    workflow_version: int
    definition_digest: str
    workflow_ref: ArtifactRef | None
    created_at: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "workflow_version": self.workflow_version,
            "definition_digest": self.definition_digest,
            "workflow_ref": (
                None if self.workflow_ref is None else self.workflow_ref.to_dict()
            ),
            "created_at": self.created_at,
        }


def _json_dump(value: JsonValue) -> str:
    normalized = normalize_json(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_load(value: str | None) -> JsonValue:
    return json.loads(value) if value is not None else None


def _artifact_digests(value: Any) -> frozenset[str]:
    """Conservatively index complete ArtifactRef-shaped JSON objects."""

    digests: set[str] = set()
    stack: list[tuple[Any, int]] = [(value, 0)]
    while stack:
        item, depth = stack.pop()
        if depth > 64:
            raise StoreSchemaError("Artifact reference nesting is too deep")
        if isinstance(item, dict):
            if set(item) == set(_ARTIFACT_REF_FIELDS):
                digest = item.get("sha256")
                if isinstance(digest, str) and _SHA256_DIGEST.fullmatch(digest):
                    digests.add(digest)
                continue
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)
    return frozenset(digests)


def _record_artifact_references_tx(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    event_id: str,
    occurred_at: float,
    value: Any,
) -> None:
    digests = _artifact_digests(value)
    if not digests:
        return
    placeholders = ",".join("?" for _ in digests)
    claimed = conn.execute(
        f"""
        SELECT 1
        FROM artifact_gc_claims
        WHERE sha256 IN ({placeholders})
        LIMIT 1
        """,
        tuple(sorted(digests)),
    ).fetchone()
    if claimed is not None:
        raise ArtifactGCReferenceConflictError(
            "Artifact reference is unavailable during garbage collection"
        )
    conn.executemany(
        """
        INSERT OR IGNORE INTO artifact_references(
            sha256, first_run_id, first_event_id, created_at
        ) VALUES (?, ?, ?, ?)
        """,
        (
            (digest, run_id, event_id, occurred_at)
            for digest in sorted(digests)
        ),
    )


def _metadata_timestamp(
    metadata: dict[str, JsonValue],
    key: str,
) -> float | None:
    value = metadata.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProjectionConflictError(f"{key} must be a finite timestamp")
    timestamp = float(value)
    if not math.isfinite(timestamp) or timestamp < 0:
        raise ProjectionConflictError(f"{key} must be a finite timestamp")
    return timestamp


def _timeout_milliseconds(
    attempt: AttemptRecord,
    key: str,
) -> int | None:
    policy = attempt.metadata.get("timeout_policy")
    if policy is None:
        return None
    if not isinstance(policy, dict):
        raise ProjectionConflictError("Attempt timeout_policy must be an object")
    value = policy.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ProjectionConflictError(f"{key} must be a positive integer")
    return value


def _earliest_deadline(
    *deadlines: float | None,
) -> float | None:
    present = [deadline for deadline in deadlines if deadline is not None]
    return min(present) if present else None


def _heartbeat_deadline(
    attempt: AttemptRecord,
    record: IdempotencyRecord | None,
) -> float | None:
    if record is None or record.status is not IdempotencyStatus.IN_PROGRESS:
        return None
    timeout = _timeout_milliseconds(attempt, "heartbeat_timeout_ms")
    return (
        None
        if timeout is None
        else record.updated_at + timeout / 1_000
    )


def _run_deadline(run: RunRecord) -> float | None:
    return _metadata_timestamp(run.metadata, "deadline_at")


def _attempt_deadline(
    run: RunRecord,
    attempt: AttemptRecord,
    *,
    heartbeat_deadline_at: float | None = None,
) -> tuple[str, float] | None:
    return _attempt_deadline_from_values(
        _run_deadline(run),
        attempt,
        heartbeat_deadline_at=heartbeat_deadline_at,
    )


def _attempt_deadline_from_values(
    run_deadline_at: float | None,
    attempt: AttemptRecord,
    *,
    heartbeat_deadline_at: float | None = None,
) -> tuple[str, float] | None:
    candidates: list[tuple[str, float]] = []
    if run_deadline_at is not None:
        candidates.append(("run", run_deadline_at))
    if attempt.status is AttemptStatus.SCHEDULED:
        deadline = _metadata_timestamp(attempt.metadata, "schedule_deadline_at")
        if deadline is not None:
            candidates.append(("schedule", deadline))
    elif attempt.status is AttemptStatus.CLAIMED:
        deadline = _metadata_timestamp(attempt.metadata, "start_deadline_at")
        if deadline is not None:
            candidates.append(("start", deadline))
    elif attempt.status is AttemptStatus.RUNNING:
        deadline = _metadata_timestamp(attempt.metadata, "execution_deadline_at")
        if deadline is not None:
            candidates.append(("execution", deadline))
    if (
        attempt.status in {AttemptStatus.CLAIMED, AttemptStatus.RUNNING}
        and _timeout_milliseconds(attempt, "heartbeat_timeout_ms") is not None
        and heartbeat_deadline_at is not None
    ):
        candidates.append(("heartbeat", heartbeat_deadline_at))
    if not candidates:
        return None
    priority = {
        "run": 0,
        "schedule": 1,
        "start": 1,
        "execution": 1,
        "heartbeat": 2,
    }
    return min(candidates, key=lambda item: (item[1], priority[item[0]]))


def _event_digest(
    *,
    run_id: str,
    event_type: str,
    node_id: str | None,
    attempt_id: str | None,
    payload: dict[str, JsonValue],
    run_projection: RunRecord | None = None,
    node_projection: NodeRecord | None = None,
    attempt_projection: AttemptRecord | None = None,
    expected_run_version: int | None = None,
    expected_node_version: int | None = None,
    expected_attempt_version: int | None = None,
) -> str:
    canonical = _json_dump(
        {
            "run_id": run_id,
            "event_type": event_type,
            "node_id": node_id,
            "attempt_id": attempt_id,
            "payload": payload,
            "run_projection": run_projection.to_dict() if run_projection else None,
            "node_projection": node_projection.to_dict() if node_projection else None,
            "attempt_projection": attempt_projection.to_dict() if attempt_projection else None,
            "expected_run_version": expected_run_version,
            "expected_node_version": expected_node_version,
            "expected_attempt_version": expected_attempt_version,
        }
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _content_digest(value: JsonValue) -> str:
    return hashlib.sha256(_json_dump(value).encode("utf-8")).hexdigest()


def _finite_timestamp(value: Any, field_name: str) -> float:
    try:
        timestamp = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite timestamp") from exc
    if not math.isfinite(timestamp) or timestamp < 0:
        raise ValueError(f"{field_name} must be a finite timestamp")
    return timestamp


def _lease_duration(value: Any) -> float:
    try:
        duration = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("lease_seconds must be finite and positive") from exc
    if not math.isfinite(duration) or duration <= 0 or duration > MAX_ACTIVITY_LEASE_SECONDS:
        raise ValueError("lease_seconds must be finite and positive")
    return duration


def _safe_receipt_code(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not _SAFE_RECEIPT_CODE.fullmatch(value):
        raise ValueError(f"{field_name} must be a bounded receipt code")
    return value


def _validate_activity_commit_rejection_payload(
    payload: dict[str, JsonValue],
) -> None:
    if set(payload) != {
        "reason_code",
        "current_fencing_token",
        "attempt_terminal",
        "claim_terminal",
    }:
        raise ValueError("activity commit rejection payload is not bounded")
    if payload.get("reason_code") not in _ACTIVITY_COMMIT_REJECTION_REASONS:
        raise ValueError("activity commit rejection reason is invalid")
    fencing = payload.get("current_fencing_token")
    if (
        isinstance(fencing, bool)
        or not isinstance(fencing, int)
        or fencing < 1
    ):
        raise ValueError("activity commit rejection fencing token is invalid")
    if not isinstance(payload.get("attempt_terminal"), bool):
        raise ValueError("activity commit rejection terminal fact is invalid")
    if not isinstance(payload.get("claim_terminal"), bool):
        raise ValueError("activity commit rejection terminal fact is invalid")


def _verified_probe_receipt(value: Any) -> dict[str, JsonValue]:
    normalized = normalize_json(value, "verified probe receipt")
    if not isinstance(normalized, dict):
        raise ValueError("verified probe receipt must be a JSON object")
    allowed = {
        "outcome",
        "verification",
        "artifact_refs",
        "external_operation_id_digest",
    }
    if set(normalized).difference(allowed):
        raise ValueError("verified probe receipt contains non-whitelisted fields")
    if normalized.get("outcome") != "succeeded" or normalized.get("verification") != "verified":
        raise ValueError("verified probe receipt must prove a succeeded outcome")
    refs = normalized.get("artifact_refs")
    if not isinstance(refs, list) or not refs or len(refs) > 64:
        raise ValueError("verified probe receipt requires bounded Artifact refs")
    safe_refs: list[dict[str, JsonValue]] = []
    for ref in refs:
        if not isinstance(ref, dict):
            raise ValueError("verified probe Artifact ref must be an object")
        try:
            artifact_ref = ArtifactRef.from_dict(ref)
        except (TypeError, ValueError) as exc:
            raise ValueError("verified probe Artifact ref is invalid") from exc
        canonical = artifact_ref.to_dict()
        if set(ref) != set(canonical):
            raise ValueError(
                "verified probe Artifact ref must contain the complete schema"
            )
        safe_refs.append(normalize_json(canonical, "verified Artifact ref"))
    operation_digest = normalized.get("external_operation_id_digest")
    if operation_digest is not None:
        operation_digest = _sha256_digest(
            operation_digest,
            "external_operation_id_digest",
        )
    return {
        "outcome": "succeeded",
        "verification": "verified",
        "artifact_refs": safe_refs,
        "external_operation_id_digest": operation_digest,
    }


def _sha256_digest(value: Any, field_name: str) -> str:
    digest = str(value or "")
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return digest


def _canonical_optional_workflow_ref(value: Any) -> ArtifactRef | None:
    if value is None:
        return None
    if isinstance(value, ArtifactRef):
        raw_ref = value.to_dict()
    elif isinstance(value, dict):
        raw_ref = value
    else:
        raise ValueError("workflow_ref must be an ArtifactRef, object, or null")
    if set(raw_ref) != set(_ARTIFACT_REF_FIELDS):
        raise ValueError("workflow_ref must contain a complete ArtifactRef")
    try:
        workflow_ref = ArtifactRef.from_dict(raw_ref)
    except (TypeError, ValueError) as exc:
        raise ValueError("workflow_ref is invalid") from exc
    if workflow_ref.to_dict() != raw_ref:
        raise ValueError("workflow_ref must use canonical ArtifactRef values")
    return workflow_ref


def _workflow_ref_from_run(run: RunRecord) -> ArtifactRef | None:
    return _canonical_optional_workflow_ref(
        run.metadata.get("runtime_workflow_ref")
    )


def _bind_workflow_tx(
    conn: sqlite3.Connection,
    *,
    workflow_id: str,
    workflow_version: int,
    definition_digest: str,
    workflow_ref: ArtifactRef | None,
    created_at: float,
) -> WorkflowBindingRecord:
    if (
        not isinstance(workflow_id, str)
        or not workflow_id.strip()
        or len(workflow_id) > 512
        or any(ord(character) < 32 or ord(character) == 127 for character in workflow_id)
    ):
        raise ValueError("workflow_id must be bounded non-empty text")
    normalized_workflow_id = workflow_id.strip()
    if (
        isinstance(workflow_version, bool)
        or not isinstance(workflow_version, int)
        or workflow_version < 1
    ):
        raise ValueError("workflow_version must be a positive integer")
    digest = _sha256_digest(definition_digest, "definition_digest")
    canonical_ref = _canonical_optional_workflow_ref(workflow_ref)
    ref_json = (
        None if canonical_ref is None else _json_dump(canonical_ref.to_dict())
    )
    timestamp = _finite_timestamp(created_at, "created_at")
    existing = conn.execute(
        """
        SELECT workflow_id, workflow_version, definition_digest,
               workflow_ref_json, created_at
        FROM workflow_bindings
        WHERE workflow_id = ? AND workflow_version = ?
        """,
        (normalized_workflow_id, workflow_version),
    ).fetchone()
    if existing is not None:
        if existing["definition_digest"] != digest:
            raise WorkflowBindingConflictError(
                "workflow identity/version is bound to different content"
            )
        existing_ref_json = existing["workflow_ref_json"]
        if (
            existing_ref_json is not None
            and ref_json is not None
            and existing_ref_json != ref_json
        ):
            raise WorkflowBindingConflictError(
                "workflow identity/version is bound to a different Artifact"
            )
        if existing_ref_json is None and ref_json is not None:
            binding_event_id = "workflow:" + hashlib.sha256(
                f"{normalized_workflow_id}\0{workflow_version}".encode("utf-8")
            ).hexdigest()
            _record_artifact_references_tx(
                conn,
                run_id=binding_event_id,
                event_id=binding_event_id,
                occurred_at=timestamp,
                value=canonical_ref.to_dict(),
            )
            conn.execute(
                """
                UPDATE workflow_bindings
                SET workflow_ref_json = ?
                WHERE workflow_id = ? AND workflow_version = ?
                  AND definition_digest = ? AND workflow_ref_json IS NULL
                """,
                (
                    ref_json,
                    normalized_workflow_id,
                    workflow_version,
                    digest,
                ),
            )
            existing = conn.execute(
                """
                SELECT workflow_id, workflow_version, definition_digest,
                       workflow_ref_json, created_at
                FROM workflow_bindings
                WHERE workflow_id = ? AND workflow_version = ?
                """,
                (normalized_workflow_id, workflow_version),
            ).fetchone()
        assert existing is not None
        return DurableRunStore._workflow_binding_from_row(existing)

    if canonical_ref is not None:
        binding_event_id = "workflow:" + hashlib.sha256(
            f"{normalized_workflow_id}\0{workflow_version}".encode("utf-8")
        ).hexdigest()
        _record_artifact_references_tx(
            conn,
            run_id=binding_event_id,
            event_id=binding_event_id,
            occurred_at=timestamp,
            value=canonical_ref.to_dict(),
        )
    conn.execute(
        """
        INSERT INTO workflow_bindings(
            workflow_id, workflow_version, definition_digest,
            workflow_ref_json, created_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            normalized_workflow_id,
            workflow_version,
            digest,
            ref_json,
            timestamp,
        ),
    )
    row = conn.execute(
        """
        SELECT workflow_id, workflow_version, definition_digest,
               workflow_ref_json, created_at
        FROM workflow_bindings
        WHERE workflow_id = ? AND workflow_version = ?
        """,
        (normalized_workflow_id, workflow_version),
    ).fetchone()
    assert row is not None
    return DurableRunStore._workflow_binding_from_row(row)


def _claim_bound_event_id(kind: str, attempt_id: str, claim_token: str) -> str:
    binding = hashlib.sha256(
        f"{kind}\0{attempt_id}\0{claim_token}".encode("utf-8")
    ).hexdigest()
    return f"evt_{kind}_{binding}"


def _audit_stale_activity_rejection(method):
    """Persist a bounded rejection fact after the failed write Tx rolled back."""

    signature = inspect.signature(method)

    @wraps(method)
    def wrapped(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except IdempotencyConflictError as conflict:
            bound = signature.bind(*args, **kwargs)
            arguments = bound.arguments
            store = arguments["self"]
            try:
                store._record_activity_commit_rejection(
                    str(arguments["run_id"]),
                    str(arguments["node_id"]),
                    str(arguments["attempt_id"]),
                    owner_id=str(arguments["owner_id"]),
                    claim_token=str(arguments["claim_token"]),
                    fencing_token=arguments.get("fencing_token"),
                    now=arguments.get("now"),
                )
            except Exception as audit_error:
                # Preserve the caller-visible stale-claim conflict even if the
                # independent audit transaction cannot be committed.
                conflict.add_note(
                    f"activity rejection audit failed: {type(audit_error).__name__}"
                )
            raise

    return wrapped


class DurableRunStore:
    """Durable Run metadata store backed by one workspace-local SQLite file."""

    def __init__(
        self,
        path: str | Path,
        *,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
    ) -> None:
        raw_path = str(path)
        if raw_path == ":memory:":
            raise ValueError("DurableRunStore requires a file-backed SQLite database")
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.busy_timeout_ms = max(1, int(busy_timeout_ms))
        with _SCHEMA_LOCK:
            self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1000,
            isolation_level=None,
            check_same_thread=True,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        conn.execute("PRAGMA synchronous = FULL")
        return conn

    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.rollback()
                raise
            else:
                conn.commit()

    def _initialize(self) -> None:
        with closing(self._connect()) as conn:
            conn.execute("PRAGMA journal_mode = WAL")
        with self._write_transaction() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at REAL NOT NULL
                )
                """
            )
            row = conn.execute("SELECT MAX(version) AS version FROM schema_migrations").fetchone()
            current = int(row["version"] or 0)
            if current > STORE_SCHEMA_VERSION:
                raise StoreSchemaError(
                    f"database schema {current} is newer than supported {STORE_SCHEMA_VERSION}"
                )
            while current < STORE_SCHEMA_VERSION:
                target = current + 1
                migration = getattr(self, f"_migrate_{current}_to_{target}", None)
                if migration is None:
                    raise StoreSchemaError(f"missing migration {current} -> {target}")
                migration(conn)
                conn.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (target, utc_timestamp()),
                )
                current = target
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS attempts_admission_status_idx
                ON attempts(status)
                WHERE status IN ('claimed', 'running')
                """
            )
            run_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(runs)").fetchall()
            }
            attempt_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(attempts)").fetchall()
            }
            idempotency_columns = {
                row["name"]
                for row in conn.execute(
                    "PRAGMA table_info(idempotency_records)"
                ).fetchall()
            }
            if {
                "status",
                "metadata_json",
                "created_at",
                "run_id",
            }.issubset(run_columns):
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS runs_deadline_status_idx
                    ON runs(
                        status,
                        CAST(json_extract(metadata_json, '$.deadline_at') AS REAL),
                        created_at,
                        run_id
                    )
                    WHERE json_type(metadata_json, '$.deadline_at')
                          IN ('integer', 'real')
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS runs_hierarchy_root_idx
                    ON runs(
                        json_extract(
                            metadata_json,
                            '$.hierarchy_link.root_run_id'
                        )
                    )
                    WHERE json_type(metadata_json, '$.hierarchy_link') = 'object'
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS runs_hierarchy_parent_control_idx
                    ON runs(
                        json_extract(
                            metadata_json,
                            '$.hierarchy_link.parent_run_id'
                        ),
                        json_extract(
                            metadata_json,
                            '$.hierarchy_link.parent_node_id'
                        )
                    )
                    WHERE json_type(metadata_json, '$.hierarchy_link') = 'object'
                    """
                )
            if {
                "status",
                "metadata_json",
                "scheduled_at",
                "run_id",
                "node_id",
                "attempt_number",
            }.issubset(attempt_columns):
                phase_deadline_indexes = (
                    ("schedule", AttemptStatus.SCHEDULED.value),
                    ("start", AttemptStatus.CLAIMED.value),
                    ("execution", AttemptStatus.RUNNING.value),
                )
                for deadline_kind, status in phase_deadline_indexes:
                    conn.execute(
                        f"""
                        CREATE INDEX IF NOT EXISTS attempts_{deadline_kind}_deadline_idx
                        ON attempts(
                            CAST(
                                json_extract(
                                    metadata_json,
                                    '$.{deadline_kind}_deadline_at'
                                ) AS REAL
                            ),
                            run_id,
                            node_id,
                            attempt_number
                        )
                        WHERE status = '{status}'
                          AND json_type(
                              metadata_json,
                              '$.{deadline_kind}_deadline_at'
                          ) IN ('integer', 'real')
                        """
                    )
            if {
                "status",
                "updated_at",
                "run_id",
                "key",
            }.issubset(idempotency_columns):
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idempotency_heartbeat_deadline_idx
                    ON idempotency_records(status, updated_at, run_id, key)
                    WHERE status = 'in_progress'
                    """
                )
            conn.execute(f"PRAGMA user_version = {STORE_SCHEMA_VERSION}")

    @staticmethod
    def _migrate_0_to_1(conn: sqlite3.Connection) -> None:
        statements = (
            """
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                workflow_id TEXT NOT NULL,
                workflow_version INTEGER NOT NULL,
                definition_digest TEXT NOT NULL,
                status TEXT NOT NULL,
                input_json TEXT NOT NULL,
                output_json TEXT NOT NULL,
                error_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                last_event_sequence INTEGER NOT NULL CHECK(last_event_sequence >= 0),
                projection_version INTEGER NOT NULL CHECK(projection_version >= 1)
            )
            """,
            "CREATE INDEX runs_status_updated_idx ON runs(status, updated_at DESC)",
            """
            CREATE TABLE node_runs (
                run_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                node_type TEXT NOT NULL,
                status TEXT NOT NULL,
                input_json TEXT NOT NULL,
                output_json TEXT NOT NULL,
                error_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                attempt_count INTEGER NOT NULL CHECK(attempt_count >= 0),
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                last_event_sequence INTEGER NOT NULL CHECK(last_event_sequence >= 1),
                projection_version INTEGER NOT NULL CHECK(projection_version >= 1),
                PRIMARY KEY(run_id, node_id),
                FOREIGN KEY(run_id) REFERENCES runs(run_id)
            )
            """,
            """
            CREATE TABLE attempts (
                attempt_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                attempt_number INTEGER NOT NULL CHECK(attempt_number >= 1),
                idempotency_key TEXT NOT NULL,
                activity_kind TEXT NOT NULL,
                effect_class TEXT NOT NULL,
                status TEXT NOT NULL,
                worker_id TEXT,
                lease_id TEXT,
                fencing_token INTEGER NOT NULL CHECK(fencing_token >= 0),
                result_json TEXT NOT NULL,
                error_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                scheduled_at REAL NOT NULL,
                started_at REAL,
                finished_at REAL,
                last_event_sequence INTEGER NOT NULL CHECK(last_event_sequence >= 1),
                projection_version INTEGER NOT NULL CHECK(projection_version >= 1),
                UNIQUE(run_id, node_id, attempt_number),
                FOREIGN KEY(run_id, node_id) REFERENCES node_runs(run_id, node_id)
            )
            """,
            """
            CREATE TABLE domain_events (
                event_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                seq INTEGER NOT NULL CHECK(seq >= 1),
                schema_version INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                node_id TEXT,
                attempt_id TEXT,
                payload_json TEXT NOT NULL,
                content_digest TEXT NOT NULL,
                intent_digest TEXT NOT NULL,
                occurred_at REAL NOT NULL,
                UNIQUE(run_id, seq),
                FOREIGN KEY(run_id) REFERENCES runs(run_id)
            )
            """,
            "CREATE INDEX domain_events_run_seq_idx ON domain_events(run_id, seq)",
            """
            CREATE TRIGGER domain_events_no_update
            BEFORE UPDATE ON domain_events
            BEGIN
                SELECT RAISE(ABORT, 'domain events are append-only');
            END
            """,
            """
            CREATE TRIGGER domain_events_no_delete
            BEFORE DELETE ON domain_events
            BEGIN
                SELECT RAISE(ABORT, 'domain events are append-only');
            END
            """,
            """
            CREATE TABLE idempotency_records (
                run_id TEXT NOT NULL,
                key TEXT NOT NULL,
                request_hash TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                status TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                claim_token TEXT NOT NULL,
                lease_expires_at REAL NOT NULL,
                result_json TEXT NOT NULL,
                claim_count INTEGER NOT NULL CHECK(claim_count >= 1),
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                completed_at REAL,
                PRIMARY KEY(run_id, key),
                FOREIGN KEY(run_id) REFERENCES runs(run_id)
            )
            """,
        )
        for statement in statements:
            conn.execute(statement)

    @staticmethod
    def _migrate_1_to_2(conn: sqlite3.Connection) -> None:
        conn.execute("DROP TRIGGER IF EXISTS domain_events_no_update")
        event_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(domain_events)").fetchall()
        }
        if "content_digest" not in event_columns:
            conn.execute(
                "ALTER TABLE domain_events ADD COLUMN content_digest TEXT NOT NULL DEFAULT ''"
            )
        if "intent_digest" not in event_columns:
            conn.execute(
                "ALTER TABLE domain_events ADD COLUMN intent_digest TEXT NOT NULL DEFAULT ''"
            )
        for row in conn.execute(
            "SELECT event_id, payload_json FROM domain_events"
        ).fetchall():
            digest = _content_digest(_json_load(row["payload_json"]))
            conn.execute(
                "UPDATE domain_events SET content_digest=?, intent_digest=? WHERE event_id=?",
                (digest, digest, row["event_id"]),
            )
        conn.execute(
            """
            CREATE TRIGGER domain_events_no_update
            BEFORE UPDATE ON domain_events
            BEGIN
                SELECT RAISE(ABORT, 'domain events are append-only');
            END
            """
        )
        idempotency_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(idempotency_records)").fetchall()
        }
        if "schema_version" not in idempotency_columns:
            conn.execute(
                "ALTER TABLE idempotency_records "
                "ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1"
            )
        try:
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS one_active_attempt_per_node
                ON attempts(run_id, node_id)
                WHERE status IN ('scheduled', 'claimed', 'running')
                """
            )
        except sqlite3.IntegrityError as exc:
            raise StoreSchemaError(
                "cannot migrate: a Node has multiple active Attempts"
            ) from exc

    @staticmethod
    def _migrate_2_to_3(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE artifact_references (
                sha256 TEXT PRIMARY KEY
                    CHECK(length(sha256) = 64),
                first_run_id TEXT NOT NULL,
                first_event_id TEXT NOT NULL,
                created_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE artifact_gc_claims (
                sha256 TEXT PRIMARY KEY
                    CHECK(length(sha256) = 64),
                quarantine_id TEXT NOT NULL UNIQUE,
                size INTEGER NOT NULL CHECK(size >= 0),
                claimed_at REAL NOT NULL,
                state TEXT NOT NULL
                    CHECK(state IN ('moving', 'quarantined'))
            )
            """
        )
        for row in conn.execute(
            """
            SELECT event_id, run_id, payload_json, occurred_at
            FROM domain_events
            ORDER BY run_id, seq
            """
        ).fetchall():
            payload = _json_load(row["payload_json"])
            for digest in _artifact_digests(payload):
                conn.execute(
                    """
                    INSERT OR IGNORE INTO artifact_references(
                        sha256, first_run_id, first_event_id, created_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        digest,
                        row["run_id"],
                        row["event_id"],
                        row["occurred_at"],
                    ),
                )

    @staticmethod
    def _migrate_3_to_4(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE workflow_bindings (
                workflow_id TEXT NOT NULL,
                workflow_version INTEGER NOT NULL
                    CHECK(workflow_version >= 1),
                definition_digest TEXT NOT NULL
                    CHECK(length(definition_digest) = 64),
                workflow_ref_json TEXT,
                created_at REAL NOT NULL,
                PRIMARY KEY(workflow_id, workflow_version)
            )
            """
        )
        has_runs = conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = 'runs'
            """
        ).fetchone()
        if has_runs is None:
            return
        for row in conn.execute(
            """
            SELECT run_id, workflow_id, workflow_version, definition_digest,
                   input_json, metadata_json, created_at
            FROM runs
            ORDER BY created_at, run_id
            """
        ).fetchall():
            try:
                normalize_run_input(_json_load(row["input_json"]))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise StoreSchemaError(
                    "cannot migrate: Run input violates the Artifact input boundary"
                ) from exc
            metadata = _json_load(row["metadata_json"])
            raw_ref = (
                metadata.get("runtime_workflow_ref")
                if isinstance(metadata, dict)
                else None
            )
            try:
                workflow_ref = _canonical_optional_workflow_ref(raw_ref)
                _bind_workflow_tx(
                    conn,
                    workflow_id=row["workflow_id"],
                    workflow_version=int(row["workflow_version"]),
                    definition_digest=row["definition_digest"],
                    workflow_ref=workflow_ref,
                    created_at=float(row["created_at"]),
                )
            except (TypeError, ValueError, WorkflowBindingConflictError) as exc:
                raise StoreSchemaError(
                    "cannot migrate: workflow identity/version is inconsistent"
                ) from exc

    @staticmethod
    def _prepare_new_run(
        run: RunRecord,
    ) -> tuple[RunRecord, EventRecord, str]:
        # Frozen dataclasses can still be mutated through object.__setattr__.
        # Reconstruct at the Store boundary before any durable write.
        run = RunRecord.from_dict(run.to_dict())
        if run.status is not RunStatus.CREATED:
            raise InvalidStateTransition("a new run must start in CREATED")
        if run.last_event_sequence != 0 or run.projection_version != 0:
            raise ProjectionConflictError("a new run must have an uncommitted projection")
        stored_run = replace(run, last_event_sequence=1, projection_version=1)
        event_payload = {
            "projection": {
                "schema_version": MODEL_SCHEMA_VERSION,
                "run": stored_run.to_dict(),
            },
        }
        event_id = new_id("evt")
        event = EventRecord(
            event_id=event_id,
            run_id=run.run_id,
            seq=1,
            event_type="run.created",
            payload=event_payload,
            occurred_at=run.created_at,
        )
        digest = _event_digest(
            run_id=run.run_id,
            event_type="run.created",
            node_id=None,
            attempt_id=None,
            payload=event_payload,
            run_projection=stored_run,
        )
        return stored_run, event, digest

    @staticmethod
    def _insert_new_run_tx(
        conn: sqlite3.Connection,
        stored_run: RunRecord,
        event: EventRecord,
        digest: str,
    ) -> None:
        _record_artifact_references_tx(
            conn,
            run_id=event.run_id,
            event_id=event.event_id,
            occurred_at=event.occurred_at,
            value=event.payload,
        )
        conn.execute(
            """
            INSERT INTO runs(
                run_id, schema_version, workflow_id, workflow_version,
                definition_digest, status,
                input_json, output_json, error_json, metadata_json,
                created_at, updated_at, last_event_sequence, projection_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1)
            """,
            (
                stored_run.run_id,
                stored_run.schema_version,
                stored_run.workflow_id,
                stored_run.workflow_version,
                stored_run.definition_digest,
                stored_run.status.value,
                _json_dump(stored_run.input),
                _json_dump(stored_run.output),
                _json_dump(stored_run.error),
                _json_dump(stored_run.metadata),
                stored_run.created_at,
                stored_run.updated_at,
            ),
        )
        conn.execute(
            """
            INSERT INTO domain_events(
                event_id, run_id, seq, schema_version, event_type, node_id,
                attempt_id, payload_json, content_digest, intent_digest, occurred_at
            ) VALUES (?, ?, 1, ?, 'run.created', NULL, NULL, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.run_id,
                event.schema_version,
                _json_dump(event.payload),
                _content_digest(event.payload),
                digest,
                event.occurred_at,
            ),
        )

    def create_run(self, run: RunRecord) -> RunRecord:
        """Create a Run and its sequence-1 ``run.created`` event atomically."""

        stored_run, event, digest = self._prepare_new_run(run)
        try:
            with self._write_transaction() as conn:
                _bind_workflow_tx(
                    conn,
                    workflow_id=stored_run.workflow_id,
                    workflow_version=stored_run.workflow_version,
                    definition_digest=stored_run.definition_digest,
                    workflow_ref=_workflow_ref_from_run(stored_run),
                    created_at=stored_run.created_at,
                )
                self._insert_new_run_tx(conn, stored_run, event, digest)
        except sqlite3.IntegrityError as exc:
            if "runs.run_id" in str(exc) or "UNIQUE constraint failed: runs.run_id" in str(exc):
                raise RunAlreadyExistsError(run.run_id) from exc
            raise
        return self.get_run(run.run_id)  # type: ignore[return-value]

    def create_child_run(
        self,
        run: RunRecord,
        *,
        max_total_descendants: int,
        max_children_per_control: int,
        expected_parent_run_version: int | None = None,
        expected_parent_node_version: int | None = None,
    ) -> RunRecord:
        """Atomically validate the parent snapshot and create one linked child Run."""

        if (
            isinstance(max_total_descendants, bool)
            or not isinstance(max_total_descendants, int)
            or max_total_descendants < 1
            or max_total_descendants > 10_000
        ):
            raise ValueError(
                "max_total_descendants must be between 1 and 10000"
            )
        if (
            isinstance(max_children_per_control, bool)
            or not isinstance(max_children_per_control, int)
            or max_children_per_control < 1
            or max_children_per_control > 1_000
        ):
            raise ValueError(
                "max_children_per_control must be between 1 and 1000"
            )
        link = run.metadata.get("hierarchy_link")
        if not isinstance(link, dict):
            raise ProjectionConflictError(
                "child Run metadata must contain a hierarchy_link object"
            )
        hierarchy_ids: dict[str, str] = {}
        for key in ("root_run_id", "parent_run_id", "parent_node_id"):
            value = link.get(key)
            if not isinstance(value, str) or not value:
                raise ProjectionConflictError(
                    f"child Run hierarchy_link.{key} must be a non-empty string"
                )
            hierarchy_ids[key] = value
        root_run_id = hierarchy_ids["root_run_id"]
        parent_run_id = hierarchy_ids["parent_run_id"]
        parent_node_id = hierarchy_ids["parent_node_id"]
        if run.run_id in {root_run_id, parent_run_id}:
            raise ProjectionConflictError("child Run hierarchy link contains a cycle")

        stored_run, event, digest = self._prepare_new_run(run)
        try:
            with self._write_transaction() as conn:
                if conn.execute(
                    "SELECT 1 FROM runs WHERE run_id = ?",
                    (run.run_id,),
                ).fetchone() is not None:
                    raise RunAlreadyExistsError(run.run_id)
                parent_row = conn.execute(
                    """
                    SELECT
                        parent.status AS run_status,
                        parent.projection_version AS run_projection_version,
                        parent.metadata_json,
                        control.status AS node_status,
                        control.projection_version AS node_projection_version
                    FROM runs AS parent
                    LEFT JOIN node_runs AS control
                      ON control.run_id = parent.run_id
                     AND control.node_id = ?
                    WHERE parent.run_id = ?
                    """,
                    (parent_node_id, parent_run_id),
                ).fetchone()
                if parent_row is None:
                    raise RunNotFoundError(parent_run_id)
                current_run_version = int(parent_row["run_projection_version"])
                if (
                    expected_parent_run_version is not None
                    and int(expected_parent_run_version) != current_run_version
                ):
                    raise ConcurrentProjectionUpdate(
                        "parent_run",
                        int(expected_parent_run_version),
                        current_run_version,
                    )
                current_node_version = (
                    None
                    if parent_row["node_projection_version"] is None
                    else int(parent_row["node_projection_version"])
                )
                if (
                    expected_parent_node_version is not None
                    and int(expected_parent_node_version) != current_node_version
                ):
                    raise ConcurrentProjectionUpdate(
                        "parent_node",
                        int(expected_parent_node_version),
                        current_node_version,
                    )
                if (
                    parent_row["run_status"] != RunStatus.RUNNING.value
                    or parent_row["node_status"] != NodeStatus.RUNNING.value
                ):
                    raise ProjectionConflictError(
                        "child Run creation requires a RUNNING parent and control Node"
                    )
                self._fault("child_create.after_parent_state")
                if conn.execute(
                    "SELECT 1 FROM runs WHERE run_id = ?",
                    (root_run_id,),
                ).fetchone() is None:
                    raise RunNotFoundError(root_run_id)
                if parent_run_id != root_run_id:
                    parent_metadata = _json_load(parent_row["metadata_json"])
                    parent_link = (
                        parent_metadata.get("hierarchy_link")
                        if isinstance(parent_metadata, dict)
                        else None
                    )
                    if (
                        not isinstance(parent_link, dict)
                        or parent_link.get("root_run_id") != root_run_id
                    ):
                        raise ProjectionConflictError(
                            "child Run hierarchy root does not match its parent"
                        )

                total_limit_hit = conn.execute(
                    """
                    SELECT 1
                    FROM runs INDEXED BY runs_hierarchy_root_idx
                    WHERE json_type(metadata_json, '$.hierarchy_link') = 'object'
                      AND json_extract(
                          metadata_json,
                          '$.hierarchy_link.root_run_id'
                      ) = ?
                    LIMIT 1 OFFSET ?
                    """,
                    (root_run_id, max_total_descendants - 1),
                ).fetchone()
                if total_limit_hit is not None:
                    raise RunHierarchyLimitError("total_descendants")
                control_limit_hit = conn.execute(
                    """
                    SELECT 1
                    FROM runs INDEXED BY runs_hierarchy_parent_control_idx
                    WHERE json_type(metadata_json, '$.hierarchy_link') = 'object'
                      AND json_extract(
                          metadata_json,
                          '$.hierarchy_link.parent_run_id'
                      ) = ?
                      AND json_extract(
                          metadata_json,
                          '$.hierarchy_link.parent_node_id'
                      ) = ?
                    LIMIT 1 OFFSET ?
                    """,
                    (
                        parent_run_id,
                        parent_node_id,
                        max_children_per_control - 1,
                    ),
                ).fetchone()
                if control_limit_hit is not None:
                    raise RunHierarchyLimitError("children_per_control")
                _bind_workflow_tx(
                    conn,
                    workflow_id=stored_run.workflow_id,
                    workflow_version=stored_run.workflow_version,
                    definition_digest=stored_run.definition_digest,
                    workflow_ref=_workflow_ref_from_run(stored_run),
                    created_at=stored_run.created_at,
                )
                self._insert_new_run_tx(conn, stored_run, event, digest)
        except sqlite3.IntegrityError as exc:
            if (
                "runs.run_id" in str(exc)
                or "UNIQUE constraint failed: runs.run_id" in str(exc)
            ):
                raise RunAlreadyExistsError(run.run_id) from exc
            raise
        return self.get_run(run.run_id)  # type: ignore[return-value]

    def get_run(self, run_id: str) -> RunRecord | None:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        return self._run_from_row(row) if row is not None else None

    def bind_workflow(
        self,
        workflow_id: str,
        workflow_version: int,
        definition_digest: str,
        workflow_ref: ArtifactRef | None = None,
        *,
        now: float | None = None,
    ) -> WorkflowBindingRecord:
        """Lock one workflow version to content and optionally attach its Artifact."""

        if (
            not isinstance(workflow_id, str)
            or not workflow_id.strip()
            or len(workflow_id) > 512
            or any(ord(character) < 32 or ord(character) == 127 for character in workflow_id)
        ):
            raise ValueError("workflow_id must be bounded non-empty text")
        normalized_workflow_id = workflow_id.strip()
        if (
            isinstance(workflow_version, bool)
            or not isinstance(workflow_version, int)
            or workflow_version < 1
        ):
            raise ValueError("workflow_version must be a positive integer")
        digest = _sha256_digest(definition_digest, "definition_digest")
        canonical_ref = _canonical_optional_workflow_ref(workflow_ref)
        created_at = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        with self._write_transaction() as conn:
            binding = _bind_workflow_tx(
                conn,
                workflow_id=normalized_workflow_id,
                workflow_version=workflow_version,
                definition_digest=digest,
                workflow_ref=canonical_ref,
                created_at=created_at,
            )
        return binding

    def get_workflow_binding(
        self,
        workflow_id: str,
        workflow_version: int,
    ) -> WorkflowBindingRecord | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT workflow_id, workflow_version, definition_digest,
                       workflow_ref_json, created_at
                FROM workflow_bindings
                WHERE workflow_id = ? AND workflow_version = ?
                """,
                (workflow_id, workflow_version),
            ).fetchone()
        return None if row is None else self._workflow_binding_from_row(row)

    def list_runs(
        self,
        *,
        statuses: Sequence[RunStatus | str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[RunRecord]:
        limit = _bounded_limit(limit, maximum=1_000)
        offset = max(0, int(offset))
        params: list[Any] = []
        where = ""
        if statuses:
            status_values = [RunStatus(status).value for status in statuses]
            placeholders = ",".join("?" for _ in status_values)
            where = f" WHERE status IN ({placeholders})"
            params.extend(status_values)
        params.extend((limit, offset))
        with closing(self._connect()) as conn:
            rows = conn.execute(
                f"SELECT * FROM runs{where} ORDER BY created_at DESC, run_id LIMIT ? OFFSET ?",
                params,
            ).fetchall()
        return [self._run_from_row(row) for row in rows]

    def list_child_runs(
        self,
        parent_run_id: str,
        *,
        parent_node_id: str | None = None,
        limit: int = 1_000,
    ) -> list[RunRecord]:
        """Return bounded direct children using the persisted hierarchy index."""

        bounded_limit = _bounded_limit(limit, maximum=10_001)
        parameters: list[Any] = [parent_run_id]
        node_filter = ""
        if parent_node_id is not None:
            node_filter = (
                " AND json_extract("
                "metadata_json, '$.hierarchy_link.parent_node_id'"
                ") = ?"
            )
            parameters.append(parent_node_id)
        parameters.append(bounded_limit)
        with closing(self._connect()) as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM runs INDEXED BY runs_hierarchy_parent_control_idx
                WHERE json_type(metadata_json, '$.hierarchy_link') = 'object'
                  AND json_extract(
                      metadata_json,
                      '$.hierarchy_link.parent_run_id'
                  ) = ?
                  {node_filter}
                ORDER BY created_at, run_id
                LIMIT ?
                """,
                parameters,
            ).fetchall()
        return [self._run_from_row(row) for row in rows]

    def count_descendant_runs(
        self,
        root_run_id: str,
        *,
        stop_after: int = 10_000,
    ) -> int:
        """Count descendants up to a caller-supplied hard bound."""

        bounded_limit = _bounded_limit(stop_after, maximum=10_000)
        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS descendant_count
                FROM (
                    SELECT 1
                    FROM runs INDEXED BY runs_hierarchy_root_idx
                    WHERE json_type(
                        metadata_json,
                        '$.hierarchy_link'
                    ) = 'object'
                      AND json_extract(
                          metadata_json,
                          '$.hierarchy_link.root_run_id'
                      ) = ?
                    LIMIT ?
                )
                """,
                (root_run_id, bounded_limit),
            ).fetchone()
        return int(row["descendant_count"])

    def claim_artifact_gc_candidate(
        self,
        sha256: str,
        *,
        quarantine_id: str,
        size: int,
        claimed_at: float,
    ) -> bool:
        """Linearize an unreferenced GC candidate against Event commits."""

        digest = _sha256_digest(sha256, "sha256")
        if (
            not isinstance(quarantine_id, str)
            or not _GC_QUARANTINE_ID.fullmatch(quarantine_id)
        ):
            raise ValueError("quarantine_id is invalid")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ValueError("size must be a non-negative integer")
        timestamp = _finite_timestamp(claimed_at, "claimed_at")
        with self._write_transaction() as conn:
            if conn.execute(
                "SELECT 1 FROM artifact_references WHERE sha256 = ?",
                (digest,),
            ).fetchone() is not None:
                return False
            try:
                conn.execute(
                    """
                    INSERT INTO artifact_gc_claims(
                        sha256, quarantine_id, size, claimed_at, state
                    ) VALUES (?, ?, ?, ?, 'moving')
                    """,
                    (digest, quarantine_id, size, timestamp),
                )
            except sqlite3.IntegrityError:
                return False
        return True

    def mark_artifact_gc_quarantined(
        self,
        sha256: str,
        *,
        quarantine_id: str,
    ) -> None:
        digest = _sha256_digest(sha256, "sha256")
        with self._write_transaction() as conn:
            updated = conn.execute(
                """
                UPDATE artifact_gc_claims
                SET state = 'quarantined'
                WHERE sha256 = ?
                  AND quarantine_id = ?
                  AND state = 'moving'
                """,
                (digest, quarantine_id),
            )
            if updated.rowcount != 1:
                raise ProjectionConflictError(
                    "Artifact GC claim changed before quarantine commit"
                )

    def release_artifact_gc_claim(
        self,
        sha256: str,
        *,
        quarantine_id: str,
        expected_state: str | None = None,
    ) -> None:
        digest = _sha256_digest(sha256, "sha256")
        if expected_state not in {None, "moving", "quarantined"}:
            raise ValueError("expected_state is invalid")
        query = (
            "DELETE FROM artifact_gc_claims "
            "WHERE sha256 = ? AND quarantine_id = ?"
        )
        parameters: list[Any] = [digest, quarantine_id]
        if expected_state is not None:
            query += " AND state = ?"
            parameters.append(expected_state)
        with self._write_transaction() as conn:
            deleted = conn.execute(query, parameters)
            if deleted.rowcount != 1:
                raise ProjectionConflictError(
                    "Artifact GC claim changed before release"
                )

    def list_artifact_gc_claims(
        self,
        *,
        limit: int = 1_000,
        offset: int = 0,
    ) -> list[ArtifactGCClaimRecord]:
        bounded_limit = _bounded_limit(limit, maximum=1_000)
        if (
            isinstance(offset, bool)
            or not isinstance(offset, int)
            or offset < 0
            or offset > 1_000_000
        ):
            raise ValueError("offset must be a bounded non-negative integer")
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT sha256, quarantine_id, size, claimed_at, state
                FROM artifact_gc_claims
                ORDER BY claimed_at, sha256
                LIMIT ? OFFSET ?
                """,
                (bounded_limit, offset),
            ).fetchall()
        return [
            ArtifactGCClaimRecord(
                sha256=row["sha256"],
                quarantine_id=row["quarantine_id"],
                size=row["size"],
                claimed_at=row["claimed_at"],
                state=row["state"],
            )
            for row in rows
        ]

    def get_artifact_gc_claim(
        self,
        sha256: str,
    ) -> ArtifactGCClaimRecord | None:
        digest = _sha256_digest(sha256, "sha256")
        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT sha256, quarantine_id, size, claimed_at, state
                FROM artifact_gc_claims
                WHERE sha256 = ?
                """,
                (digest,),
            ).fetchone()
        if row is None:
            return None
        return ArtifactGCClaimRecord(
            sha256=row["sha256"],
            quarantine_id=row["quarantine_id"],
            size=row["size"],
            claimed_at=row["claimed_at"],
            state=row["state"],
        )

    def append_event(
        self,
        run_id: str,
        event_type: str,
        *,
        payload: dict[str, JsonValue] | None = None,
        node_id: str | None = None,
        attempt_id: str | None = None,
        event_id: str | None = None,
        occurred_at: float | None = None,
        expected_run_version: int | None = None,
        expected_node_version: int | None = None,
        expected_attempt_version: int | None = None,
        run_projection: RunRecord | None = None,
        node_projection: NodeRecord | None = None,
        attempt_projection: AttemptRecord | None = None,
    ) -> EventRecord:
        """Append an event and update supplied projections in one transaction.

        Supplying the same ``event_id`` with the same event body is idempotent.
        Reusing it with different content fails closed.
        """

        requested_payload = normalize_json(payload or {}, "payload")
        if not isinstance(requested_payload, dict):
            raise ValueError("payload must be a JSON object")
        if not isinstance(event_type, str) or not event_type.strip():
            raise ModelValidationError("event_type must be non-empty text")
        if event_type not in DURABLE_EVENT_TYPES:
            raise ProjectionConflictError(
                "event type is not registered for durable replay"
            )
        if event_type == "activity.commit_rejected":
            _validate_activity_commit_rejection_payload(requested_payload)
        event_id = str(event_id or new_id("evt"))
        occurred_at = float(occurred_at if occurred_at is not None else utc_timestamp())
        if node_projection is not None:
            if node_id is not None and node_id != node_projection.node_id:
                raise ProjectionConflictError("node_id conflicts with node projection")
            node_id = node_projection.node_id
        if attempt_projection is not None:
            if attempt_id is not None and attempt_id != attempt_projection.attempt_id:
                raise ProjectionConflictError("attempt_id conflicts with attempt projection")
            if node_id is not None and node_id != attempt_projection.node_id:
                raise ProjectionConflictError("node_id conflicts with attempt projection")
            attempt_id = attempt_projection.attempt_id
            node_id = attempt_projection.node_id
        if run_projection is not None and run_projection.run_id != run_id:
            raise ProjectionConflictError("run projection belongs to another run")
        known_transition_event = (
            event_type in _RUN_EVENT_STATUS
            or event_type in _NODE_EVENT_STATUS
            or event_type in _ATTEMPT_EVENT_STATUS
        )
        if (
            not known_transition_event
            and any(
                projection is not None
                for projection in (run_projection, node_projection, attempt_projection)
            )
        ):
            raise ProjectionConflictError(
                f"unknown event {event_type!r} cannot mutate projections"
            )
        self._validate_event_projection_binding(
            event_type,
            run_projection,
            node_projection,
            attempt_projection,
        )
        if (
            attempt_projection is not None
            and attempt_projection.status is AttemptStatus.OUTCOME_UNKNOWN
            and (
                node_projection is None
                or node_projection.status is not NodeStatus.WAITING_RECOVERY
                or run_projection is None
                or run_projection.status is not RunStatus.WAITING_RECOVERY
            )
        ):
            raise ProjectionConflictError(
                "OUTCOME_UNKNOWN must atomically move Node and Run to WAITING_RECOVERY"
            )
        if (
            node_projection is not None
            and node_projection.status is NodeStatus.PAUSED
            and (
                attempt_projection is None
                or attempt_projection.node_id != node_projection.node_id
                or attempt_projection.status is not AttemptStatus.CANCELLED
            )
        ):
            raise ProjectionConflictError(
                "pausing a Node must atomically cancel its active Attempt"
            )
        # Validate all event fields before opening a transaction. The real
        # sequence is allocated below under the write lock.
        EventRecord(
            event_id=event_id,
            run_id=run_id,
            seq=1,
            event_type=event_type,
            node_id=node_id,
            attempt_id=attempt_id,
            payload=requested_payload,
            occurred_at=occurred_at,
        )
        digest = _event_digest(
            run_id=run_id,
            event_type=event_type,
            node_id=node_id,
            attempt_id=attempt_id,
            payload=requested_payload,
            run_projection=run_projection,
            node_projection=node_projection,
            attempt_projection=attempt_projection,
            expected_run_version=expected_run_version,
            expected_node_version=expected_node_version,
            expected_attempt_version=expected_attempt_version,
        )

        with self._write_transaction() as conn:
            duplicate = conn.execute(
                "SELECT * FROM domain_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if duplicate is not None:
                if duplicate["intent_digest"] != digest:
                    raise ProjectionConflictError(
                        f"event_id {event_id!r} already has different content"
                    )
                return self._event_from_row(duplicate)

            current_row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if current_row is None:
                raise RunNotFoundError(run_id)
            current_run = self._run_from_row(current_row)
            if (
                expected_run_version is not None
                and int(expected_run_version) != current_run.projection_version
            ):
                raise ConcurrentProjectionUpdate(
                    "run",
                    int(expected_run_version),
                    current_run.projection_version,
                )
            seq = current_run.last_event_sequence + 1
            if expected_node_version is not None:
                row = conn.execute(
                    "SELECT projection_version FROM node_runs "
                    "WHERE run_id = ? AND node_id = ?",
                    (run_id, node_id),
                ).fetchone()
                current_version = None if row is None else int(row["projection_version"])
                if current_version != int(expected_node_version):
                    raise ConcurrentProjectionUpdate(
                        "node",
                        int(expected_node_version),
                        current_version,
                    )
            if expected_attempt_version is not None:
                row = conn.execute(
                    "SELECT projection_version FROM attempts WHERE attempt_id = ?",
                    (attempt_id,),
                ).fetchone()
                current_version = None if row is None else int(row["projection_version"])
                if current_version != int(expected_attempt_version):
                    raise ConcurrentProjectionUpdate(
                        "attempt",
                        int(expected_attempt_version),
                        current_version,
                    )

            if run_projection is not None:
                self._validate_run_projection(current_run, run_projection, event_type)
                self._validate_terminal_aggregate(
                    conn,
                    run_projection,
                    node_projection,
                    attempt_projection,
                )
                stored_run = replace(
                    run_projection,
                    updated_at=max(occurred_at, current_run.updated_at),
                    last_event_sequence=seq,
                    projection_version=current_run.projection_version + 1,
                )
            else:
                stored_run = replace(
                    current_run,
                    updated_at=max(occurred_at, current_run.updated_at),
                    last_event_sequence=seq,
                    projection_version=current_run.projection_version + 1,
                )

            stored_node = None
            if node_projection is not None:
                stored_node = self._write_node_projection(
                    conn, run_id, node_projection, occurred_at, seq
                )
            stored_attempt = None
            if attempt_projection is not None:
                stored_attempt = self._write_attempt_projection(
                    conn, run_id, attempt_projection, seq
                )

            update = conn.execute(
                """
                UPDATE runs SET
                    schema_version = ?, workflow_id = ?, workflow_version = ?,
                    definition_digest = ?,
                    status = ?, input_json = ?, output_json = ?, error_json = ?,
                    metadata_json = ?, created_at = ?, updated_at = ?,
                    last_event_sequence = ?, projection_version = ?
                WHERE run_id = ? AND projection_version = ?
                """,
                (
                    stored_run.schema_version,
                    stored_run.workflow_id,
                    stored_run.workflow_version,
                    stored_run.definition_digest,
                    stored_run.status.value,
                    _json_dump(stored_run.input),
                    _json_dump(stored_run.output),
                    _json_dump(stored_run.error),
                    _json_dump(stored_run.metadata),
                    stored_run.created_at,
                    stored_run.updated_at,
                    stored_run.last_event_sequence,
                    stored_run.projection_version,
                    run_id,
                    current_run.projection_version,
                ),
            )
            if update.rowcount != 1:
                raise ProjectionConflictError("run projection changed concurrently")
            event_payload = dict(requested_payload)
            event_payload["projection"] = {
                "schema_version": MODEL_SCHEMA_VERSION,
                "run": stored_run.to_dict(),
                "node": stored_node.to_dict() if stored_node else None,
                "attempt": stored_attempt.to_dict() if stored_attempt else None,
            }
            event = EventRecord(
                event_id=event_id,
                run_id=run_id,
                seq=seq,
                event_type=event_type,
                node_id=node_id,
                attempt_id=attempt_id,
                payload=event_payload,
                occurred_at=occurred_at,
            )
            _record_artifact_references_tx(
                conn,
                run_id=event.run_id,
                event_id=event.event_id,
                occurred_at=event.occurred_at,
                value=event.payload,
            )
            conn.execute(
                """
                INSERT INTO domain_events(
                    event_id, run_id, seq, schema_version, event_type, node_id,
                    attempt_id, payload_json, content_digest, intent_digest, occurred_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.run_id,
                    event.seq,
                    event.schema_version,
                    event.event_type,
                    event.node_id,
                    event.attempt_id,
                    _json_dump(event.payload),
                    _content_digest(event.payload),
                    digest,
                    event.occurred_at,
                ),
            )

        return event

    def list_events(
        self,
        run_id: str,
        *,
        after_seq: int = 0,
        limit: int = DEFAULT_EVENT_LIMIT,
    ) -> list[EventRecord]:
        limit = _bounded_limit(limit, maximum=MAX_EVENT_LIMIT)
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT * FROM domain_events
                WHERE run_id = ? AND seq > ?
                ORDER BY seq ASC
                LIMIT ?
                """,
                (run_id, max(0, int(after_seq)), limit),
            ).fetchall()
        return [self._event_from_row(row) for row in rows]

    def rebuild_projections(
        self,
        run_id: str,
    ) -> tuple[RunRecord, list[NodeRecord], list[AttemptRecord]]:
        """Rebuild Run/Node/Attempt projections from canonical event snapshots."""

        return self._rebuild_projections_streaming(
            run_id,
            page_size=MAX_EVENT_LIMIT,
        )

    def verify_projections_bounded(
        self,
        run_id: str,
        *,
        max_events: int,
        max_payload_bytes: int,
        max_wall_seconds: float,
        page_size: int = MAX_PROJECTION_REPLAY_PAGE,
        clock: Callable[[], float] = time.monotonic,
    ) -> bool:
        """Verify one Run with hard input budgets and streaming replay state.

        The wall budget is checked before and after every bounded SQLite page,
        canonical payload encoding, reducer step, and live projection read.  It
        is cooperative rather than an OS-level preemption guarantee.
        """

        _validate_projection_replay_limits(
            max_events=max_events,
            max_payload_bytes=max_payload_bytes,
            max_wall_seconds=max_wall_seconds,
            page_size=page_size,
            clock=clock,
        )
        started_at = _projection_replay_now(clock)
        wall_seconds = float(max_wall_seconds)
        with closing(self._connect()) as conn:
            conn.execute("BEGIN")
            try:
                rebuilt_run, rebuilt_nodes, rebuilt_attempts = (
                    self._rebuild_projections_streaming(
                        run_id,
                        max_events=max_events,
                        max_payload_bytes=max_payload_bytes,
                        max_wall_seconds=wall_seconds,
                        page_size=page_size,
                        clock=clock,
                        started_at=started_at,
                        connection=conn,
                    )
                )
                self._fault("replay.after_events")
                _check_projection_replay_wall(
                    clock,
                    started_at=started_at,
                    max_wall_seconds=wall_seconds,
                )
                run_row = conn.execute(
                    "SELECT * FROM runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                _check_projection_replay_wall(
                    clock,
                    started_at=started_at,
                    max_wall_seconds=wall_seconds,
                )
                current_run = (
                    None if run_row is None else self._run_from_row(run_row)
                )
                nodes_match = self._projection_rows_match(
                    conn,
                    query=(
                        "SELECT * FROM node_runs WHERE run_id = ? "
                        "ORDER BY created_at, node_id"
                    ),
                    params=(run_id,),
                    expected=rebuilt_nodes,
                    decode=self._node_from_row,
                    page_size=page_size,
                    clock=clock,
                    started_at=started_at,
                    max_wall_seconds=wall_seconds,
                )
                attempts_match = self._projection_rows_match(
                    conn,
                    query=(
                        "SELECT * FROM attempts WHERE run_id = ? "
                        "ORDER BY node_id, attempt_number"
                    ),
                    params=(run_id,),
                    expected=rebuilt_attempts,
                    decode=self._attempt_from_row,
                    page_size=page_size,
                    clock=clock,
                    started_at=started_at,
                    max_wall_seconds=wall_seconds,
                )
            except BaseException:
                conn.rollback()
                raise
            else:
                conn.commit()
        return (
            current_run == rebuilt_run
            and nodes_match
            and attempts_match
        )

    def _rebuild_projections_streaming(
        self,
        run_id: str,
        *,
        max_events: int | None = None,
        max_payload_bytes: int | None = None,
        max_wall_seconds: float | None = None,
        page_size: int,
        clock: Callable[[], float] = time.monotonic,
        started_at: float | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> tuple[RunRecord, list[NodeRecord], list[AttemptRecord]]:
        """Reduce bounded pages without retaining the full Event History."""

        if started_at is None:
            started_at = _projection_replay_now(clock)
        after_seq = 0
        run: RunRecord | None = None
        nodes: dict[str, NodeRecord] = {}
        attempts: dict[str, AttemptRecord] = {}
        expected_seq = 1
        event_count = 0
        payload_bytes = 0
        while True:
            if max_wall_seconds is not None:
                _check_projection_replay_wall(
                    clock,
                    started_at=started_at,
                    max_wall_seconds=max_wall_seconds,
                )
            remaining = (
                None if max_events is None else max_events - event_count
            )
            query_limit = (
                page_size
                if remaining is None
                else min(page_size, max(1, remaining + 1))
            )
            if connection is None:
                batch = self.list_events(
                    run_id,
                    after_seq=after_seq,
                    limit=query_limit,
                )
            else:
                rows = connection.execute(
                    """
                    SELECT * FROM domain_events
                    WHERE run_id = ? AND seq > ?
                    ORDER BY seq ASC
                    LIMIT ?
                    """,
                    (run_id, after_seq, query_limit),
                ).fetchall()
                batch = [self._event_from_row(row) for row in rows]
            if max_wall_seconds is not None:
                _check_projection_replay_wall(
                    clock,
                    started_at=started_at,
                    max_wall_seconds=max_wall_seconds,
                )
            if not batch:
                break
            self._fault("replay.after_event_page")
            for event in batch:
                if max_events is not None and event_count >= max_events:
                    raise ProjectionReplayLimitError("event_count_limit")
                encoded_payload_bytes = len(
                    _json_dump(event.payload).encode("utf-8")
                )
                if (
                    max_payload_bytes is not None
                    and encoded_payload_bytes > max_payload_bytes - payload_bytes
                ):
                    raise ProjectionReplayLimitError("payload_bytes_limit")
                payload_bytes += encoded_payload_bytes
                event_count += 1
                if max_wall_seconds is not None:
                    _check_projection_replay_wall(
                        clock,
                        started_at=started_at,
                        max_wall_seconds=max_wall_seconds,
                    )
                if event.seq != expected_seq:
                    raise StoreSchemaError(
                        f"event sequence gap for {run_id}: "
                        f"expected {expected_seq}, got {event.seq}"
                    )
                expected_seq += 1
                if event.event_type not in DURABLE_EVENT_TYPES:
                    raise StoreSchemaError(
                        "event type is not registered for durable replay"
                    )
                projection = event.payload.get("projection")
                if not isinstance(projection, dict):
                    raise StoreSchemaError(
                        f"event {event.event_id} has no canonical projection snapshot"
                    )
                run_payload = projection.get("run")
                if not isinstance(run_payload, dict):
                    raise StoreSchemaError(
                        f"event {event.event_id} has no Run projection"
                    )
                run = RunRecord.from_dict(run_payload)
                if run.last_event_sequence != event.seq:
                    raise StoreSchemaError(
                        f"event {event.event_id} Run sequence does not match "
                        "event sequence"
                    )
                node_payload = projection.get("node")
                if node_payload is not None:
                    if not isinstance(node_payload, dict):
                        raise StoreSchemaError("invalid Node projection snapshot")
                    node = NodeRecord.from_dict(node_payload)
                    if node.last_event_sequence != event.seq:
                        raise StoreSchemaError("Node projection sequence mismatch")
                    nodes[node.node_id] = node
                attempt_payload = projection.get("attempt")
                if attempt_payload is not None:
                    if not isinstance(attempt_payload, dict):
                        raise StoreSchemaError(
                            "invalid Attempt projection snapshot"
                        )
                    attempt = AttemptRecord.from_dict(attempt_payload)
                    if attempt.last_event_sequence != event.seq:
                        raise StoreSchemaError(
                            "Attempt projection sequence mismatch"
                        )
                    attempts[attempt.attempt_id] = attempt
                if max_wall_seconds is not None:
                    _check_projection_replay_wall(
                        clock,
                        started_at=started_at,
                        max_wall_seconds=max_wall_seconds,
                    )
            after_seq = batch[-1].seq
            if len(batch) < query_limit:
                break
        if run is None:
            raise RunNotFoundError(run_id)
        return (
            run,
            sorted(nodes.values(), key=lambda item: (item.created_at, item.node_id)),
            sorted(
                attempts.values(),
                key=lambda item: (item.node_id, item.attempt_number),
            ),
        )

    def _projection_rows_match(
        self,
        connection: sqlite3.Connection,
        *,
        query: str,
        params: tuple[Any, ...],
        expected: Sequence[Any],
        decode: Callable[[sqlite3.Row], Any],
        page_size: int,
        clock: Callable[[], float],
        started_at: float,
        max_wall_seconds: float,
    ) -> bool:
        """Compare one projection table incrementally inside a read snapshot."""

        _check_projection_replay_wall(
            clock,
            started_at=started_at,
            max_wall_seconds=max_wall_seconds,
        )
        cursor = connection.execute(query, params)
        index = 0
        while True:
            rows = cursor.fetchmany(page_size)
            _check_projection_replay_wall(
                clock,
                started_at=started_at,
                max_wall_seconds=max_wall_seconds,
            )
            if not rows:
                return index == len(expected)
            for row in rows:
                if index >= len(expected) or decode(row) != expected[index]:
                    return False
                index += 1
                _check_projection_replay_wall(
                    clock,
                    started_at=started_at,
                    max_wall_seconds=max_wall_seconds,
                )

    def verify_projections(self, run_id: str) -> bool:
        rebuilt_run, rebuilt_nodes, rebuilt_attempts = self.rebuild_projections(run_id)
        current_run = self.get_run(run_id)
        return (
            current_run == rebuilt_run
            and self.list_nodes(run_id) == rebuilt_nodes
            and self.list_attempts(run_id) == rebuilt_attempts
        )

    def get_node(self, run_id: str, node_id: str) -> NodeRecord | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM node_runs WHERE run_id = ? AND node_id = ?",
                (run_id, node_id),
            ).fetchone()
        return self._node_from_row(row) if row is not None else None

    def get_projection_snapshot(
        self,
        run_id: str,
        *,
        node_limit: int,
        node_offset: int = 0,
        attempt_limit: int,
        attempt_offset: int = 0,
    ) -> tuple[
        RunRecord | None,
        list[NodeRecord],
        list[AttemptRecord],
    ]:
        """Read one bounded Run/Node/Attempt view from a SQLite snapshot."""

        node_page = _projection_page(node_limit, node_offset)
        attempt_page = _projection_page(attempt_limit, attempt_offset)
        assert node_page is not None and attempt_page is not None
        with closing(self._connect()) as conn:
            conn.execute("BEGIN")
            try:
                run_row = conn.execute(
                    "SELECT * FROM runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                self._fault("snapshot.after_run")
                if run_row is None:
                    conn.commit()
                    return None, [], []
                node_rows = conn.execute(
                    """
                    SELECT *
                    FROM node_runs
                    WHERE run_id = ?
                    ORDER BY created_at, node_id
                    LIMIT ? OFFSET ?
                    """,
                    (run_id, *node_page),
                ).fetchall()
                self._fault("snapshot.after_nodes")
                attempt_rows = conn.execute(
                    """
                    SELECT *
                    FROM attempts
                    WHERE run_id = ?
                    ORDER BY node_id, attempt_number
                    LIMIT ? OFFSET ?
                    """,
                    (run_id, *attempt_page),
                ).fetchall()
            except BaseException:
                conn.rollback()
                raise
            else:
                conn.commit()
        return (
            self._run_from_row(run_row),
            [self._node_from_row(row) for row in node_rows],
            [self._attempt_from_row(row) for row in attempt_rows],
        )

    def list_nodes(
        self,
        run_id: str,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[NodeRecord]:
        page = _projection_page(limit, offset)
        query = (
            "SELECT * FROM node_runs WHERE run_id = ? "
            "ORDER BY created_at, node_id"
        )
        params: list[Any] = [run_id]
        if page is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend(page)
        with closing(self._connect()) as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._node_from_row(row) for row in rows]

    def get_attempt(self, attempt_id: str) -> AttemptRecord | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
        return self._attempt_from_row(row) if row is not None else None

    def list_attempts(
        self,
        run_id: str,
        *,
        node_id: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[AttemptRecord]:
        page = _projection_page(limit, offset)
        query = "SELECT * FROM attempts WHERE run_id = ?"
        params: list[Any] = [run_id]
        if node_id is not None:
            query += " AND node_id = ?"
            params.append(node_id)
        query += " ORDER BY node_id, attempt_number"
        if page is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend(page)
        with closing(self._connect()) as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._attempt_from_row(row) for row in rows]

    def claim_idempotency(
        self,
        run_id: str,
        key: str,
        request_hash: str,
        owner_id: str,
        *,
        lease_seconds: float = 60.0,
        now: float | None = None,
    ) -> IdempotencyClaim:
        """Claim a stable logical operation key.

        A completed record is returned without re-acquiring it.  An unexpired
        claim by another owner is a conflict.  Expired claims can be acquired,
        but this does not prove that the previous external side effect did not
        occur; callers must still apply effect-class recovery rules.
        """

        now = _finite_timestamp(now if now is not None else utc_timestamp(), "now")
        expires_at = now + _lease_duration(lease_seconds)
        if not all(str(value or "").strip() for value in (run_id, key, request_hash, owner_id)):
            raise ValueError("run_id, key, request_hash, and owner_id are required")
        claim_token = new_id("claim")
        with self._write_transaction() as conn:
            if conn.execute("SELECT 1 FROM runs WHERE run_id = ?", (run_id,)).fetchone() is None:
                raise RunNotFoundError(run_id)
            row = conn.execute(
                "SELECT * FROM idempotency_records WHERE run_id = ? AND key = ?",
                (run_id, key),
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO idempotency_records(
                        run_id, key, request_hash, schema_version, status, owner_id,
                        claim_token, lease_expires_at, result_json, claim_count,
                        created_at, updated_at, completed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'null', 1, ?, ?, NULL)
                    """,
                    (
                        run_id,
                        key,
                        request_hash,
                        MODEL_SCHEMA_VERSION,
                        IdempotencyStatus.IN_PROGRESS.value,
                        owner_id,
                        claim_token,
                        expires_at,
                        now,
                        now,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM idempotency_records WHERE run_id = ? AND key = ?",
                    (run_id, key),
                ).fetchone()
                return IdempotencyClaim(
                    ClaimDisposition.ACQUIRED,
                    self._idempotency_from_row(row),
                )

            record = self._idempotency_from_row(row)
            if record.request_hash != request_hash:
                return IdempotencyClaim(ClaimDisposition.CONFLICT, record)
            if record.status is IdempotencyStatus.COMPLETED:
                return IdempotencyClaim(ClaimDisposition.COMPLETED, record)
            if record.lease_expires_at > now:
                return IdempotencyClaim(ClaimDisposition.CONFLICT, record)
            conn.execute(
                """
                UPDATE idempotency_records SET
                    owner_id = ?, claim_token = ?, lease_expires_at = ?,
                    claim_count = claim_count + 1,
                    updated_at = ?
                WHERE run_id = ? AND key = ?
                """,
                (owner_id, claim_token, expires_at, now, run_id, key),
            )
            row = conn.execute(
                "SELECT * FROM idempotency_records WHERE run_id = ? AND key = ?",
                (run_id, key),
            ).fetchone()
            return IdempotencyClaim(
                ClaimDisposition.ACQUIRED,
                self._idempotency_from_row(row),
            )

    def get_idempotency(self, run_id: str, key: str) -> IdempotencyRecord | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM idempotency_records WHERE run_id = ? AND key = ?",
                (run_id, key),
            ).fetchone()
        return self._idempotency_from_row(row) if row is not None else None

    def list_expired_activity_leases(
        self,
        *,
        now: float | None = None,
        limit: int = 100,
    ) -> list[tuple[AttemptRecord, IdempotencyRecord]]:
        """Read expired active leases from durable state, never worker memory."""

        current_time = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        bounded_limit = _bounded_limit(limit, maximum=1_000)
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT a.attempt_id AS attempt_id, i.key AS idempotency_key
                FROM attempts AS a
                JOIN idempotency_records AS i
                  ON i.run_id = a.run_id AND i.key = a.idempotency_key
                WHERE a.status IN (?, ?)
                  AND i.status = ?
                  AND i.lease_expires_at <= ?
                ORDER BY i.lease_expires_at, a.run_id, a.node_id, a.attempt_number
                LIMIT ?
                """,
                (
                    AttemptStatus.CLAIMED.value,
                    AttemptStatus.RUNNING.value,
                    IdempotencyStatus.IN_PROGRESS.value,
                    current_time,
                    bounded_limit,
                ),
            ).fetchall()
            expired: list[tuple[AttemptRecord, IdempotencyRecord]] = []
            for row in rows:
                attempt_row = conn.execute(
                    "SELECT * FROM attempts WHERE attempt_id = ?",
                    (row["attempt_id"],),
                ).fetchone()
                idempotency_row = conn.execute(
                    "SELECT * FROM idempotency_records "
                    "WHERE run_id = (SELECT run_id FROM attempts WHERE attempt_id = ?) "
                    "AND key = ?",
                    (row["attempt_id"], row["idempotency_key"]),
                ).fetchone()
                if attempt_row is None or idempotency_row is None:
                    continue
                expired.append(
                    (
                        self._attempt_from_row(attempt_row),
                        self._idempotency_from_row(idempotency_row),
                    )
                )
        return expired

    def list_expired_run_deadlines(
        self,
        *,
        now: float | None = None,
        limit: int = 100,
    ) -> list[RunRecord]:
        """Return non-terminal Runs whose persisted absolute deadline elapsed."""

        current_time = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        bounded_limit = _bounded_limit(limit, maximum=1_000)
        active_statuses = tuple(
            status.value for status in RunStatus if not status.is_terminal
        )
        placeholders = ",".join("?" for _ in active_statuses)
        with closing(self._connect()) as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM runs
                WHERE status IN ({placeholders})
                  AND (
                      (
                          json_type(metadata_json, '$.deadline_at')
                              IN ('integer', 'real')
                          AND CAST(
                              json_extract(metadata_json, '$.deadline_at') AS REAL
                          ) <= ?
                      )
                      OR (
                          json_extract(metadata_json, '$.deadline_at') IS NOT NULL
                          AND (
                              json_type(metadata_json, '$.deadline_at')
                                  NOT IN ('integer', 'real')
                              OR CAST(
                                  json_extract(metadata_json, '$.deadline_at') AS REAL
                              ) < 0
                          )
                      )
                  )
                ORDER BY
                    CASE
                        WHEN json_type(metadata_json, '$.deadline_at')
                             IN ('integer', 'real')
                        THEN CAST(
                            json_extract(metadata_json, '$.deadline_at') AS REAL
                        )
                        ELSE -1
                    END,
                    created_at,
                    run_id
                LIMIT ?
                """,
                (*active_statuses, current_time, bounded_limit),
            ).fetchall()
        expired: list[RunRecord] = []
        for row in rows:
            run = self._run_from_row(row)
            deadline = _run_deadline(run)
            if deadline is not None and deadline <= current_time:
                expired.append(run)
        return expired

    def list_expired_activity_deadlines(
        self,
        *,
        now: float | None = None,
        limit: int = 100,
    ) -> list[
        tuple[
            AttemptRecord,
            IdempotencyRecord | None,
            str,
            float,
        ]
    ]:
        """Read elapsed Attempt deadlines from durable projections and leases."""

        current_time = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        bounded_limit = _bounded_limit(limit, maximum=1_000)
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                WITH deadline_candidates(
                    attempt_id,
                    deadline_kind,
                    deadline_at,
                    deadline_priority
                ) AS (
                    SELECT
                        a.attempt_id,
                        'run',
                        CAST(
                            json_extract(r.metadata_json, '$.deadline_at') AS REAL
                        ),
                        0
                    FROM runs AS r
                    JOIN attempts AS a ON a.run_id = r.run_id
                    WHERE a.status IN ('scheduled', 'claimed', 'running')
                      AND json_type(r.metadata_json, '$.deadline_at')
                          IN ('integer', 'real')
                      AND CAST(
                          json_extract(r.metadata_json, '$.deadline_at') AS REAL
                      ) <= ?

                    UNION ALL

                    SELECT
                        attempt_id,
                        'schedule',
                        CAST(
                            json_extract(
                                metadata_json,
                                '$.schedule_deadline_at'
                            ) AS REAL
                        ),
                        1
                    FROM attempts
                    WHERE status = 'scheduled'
                      AND json_type(metadata_json, '$.schedule_deadline_at')
                          IN ('integer', 'real')
                      AND CAST(
                          json_extract(
                              metadata_json,
                              '$.schedule_deadline_at'
                          ) AS REAL
                      ) <= ?

                    UNION ALL

                    SELECT
                        attempt_id,
                        'start',
                        CAST(
                            json_extract(metadata_json, '$.start_deadline_at') AS REAL
                        ),
                        1
                    FROM attempts
                    WHERE status = 'claimed'
                      AND json_type(metadata_json, '$.start_deadline_at')
                          IN ('integer', 'real')
                      AND CAST(
                          json_extract(metadata_json, '$.start_deadline_at') AS REAL
                      ) <= ?

                    UNION ALL

                    SELECT
                        attempt_id,
                        'execution',
                        CAST(
                            json_extract(
                                metadata_json,
                                '$.execution_deadline_at'
                            ) AS REAL
                        ),
                        1
                    FROM attempts
                    WHERE status = 'running'
                      AND json_type(metadata_json, '$.execution_deadline_at')
                          IN ('integer', 'real')
                      AND CAST(
                          json_extract(
                              metadata_json,
                              '$.execution_deadline_at'
                          ) AS REAL
                      ) <= ?

                    UNION ALL

                    SELECT
                        a.attempt_id,
                        'heartbeat',
                        i.updated_at + CAST(
                            json_extract(
                                a.metadata_json,
                                '$.timeout_policy.heartbeat_timeout_ms'
                            ) AS REAL
                        ) / 1000.0,
                        2
                    FROM idempotency_records AS i
                    JOIN attempts AS a
                      ON a.run_id = i.run_id AND a.idempotency_key = i.key
                    WHERE a.status IN ('claimed', 'running')
                      AND i.status = 'in_progress'
                      AND json_type(
                          a.metadata_json,
                          '$.timeout_policy.heartbeat_timeout_ms'
                      ) = 'integer'
                      AND CAST(
                          json_extract(
                              a.metadata_json,
                              '$.timeout_policy.heartbeat_timeout_ms'
                          ) AS INTEGER
                      ) >= 1
                      AND i.updated_at + CAST(
                          json_extract(
                              a.metadata_json,
                              '$.timeout_policy.heartbeat_timeout_ms'
                          ) AS REAL
                      ) / 1000.0 <= ?

                    UNION ALL

                    SELECT
                        a.attempt_id,
                        'invalid',
                        -1,
                        -1
                    FROM attempts AS a
                    JOIN runs AS r ON r.run_id = a.run_id
                    WHERE a.status IN ('scheduled', 'claimed', 'running')
                      AND (
                          (
                              json_extract(r.metadata_json, '$.deadline_at')
                                  IS NOT NULL
                              AND (
                                  json_type(r.metadata_json, '$.deadline_at')
                                      NOT IN ('integer', 'real')
                                  OR CAST(
                                      json_extract(
                                          r.metadata_json,
                                          '$.deadline_at'
                                      ) AS REAL
                                  ) < 0
                              )
                          )
                          OR (
                              a.status = 'scheduled'
                              AND json_extract(
                                  a.metadata_json,
                                  '$.schedule_deadline_at'
                              ) IS NOT NULL
                              AND (
                                  json_type(
                                      a.metadata_json,
                                      '$.schedule_deadline_at'
                                  ) NOT IN ('integer', 'real')
                                  OR CAST(
                                      json_extract(
                                          a.metadata_json,
                                          '$.schedule_deadline_at'
                                      ) AS REAL
                                  ) < 0
                              )
                          )
                          OR (
                              a.status = 'claimed'
                              AND json_extract(
                                  a.metadata_json,
                                  '$.start_deadline_at'
                              ) IS NOT NULL
                              AND (
                                  json_type(
                                      a.metadata_json,
                                      '$.start_deadline_at'
                                  ) NOT IN ('integer', 'real')
                                  OR CAST(
                                      json_extract(
                                          a.metadata_json,
                                          '$.start_deadline_at'
                                      ) AS REAL
                                  ) < 0
                              )
                          )
                          OR (
                              a.status = 'running'
                              AND json_extract(
                                  a.metadata_json,
                                  '$.execution_deadline_at'
                              ) IS NOT NULL
                              AND (
                                  json_type(
                                      a.metadata_json,
                                      '$.execution_deadline_at'
                                  ) NOT IN ('integer', 'real')
                                  OR CAST(
                                      json_extract(
                                          a.metadata_json,
                                          '$.execution_deadline_at'
                                      ) AS REAL
                                  ) < 0
                              )
                          )
                          OR (
                              a.status IN ('claimed', 'running')
                              AND json_extract(
                                  a.metadata_json,
                                  '$.timeout_policy'
                              ) IS NOT NULL
                              AND (
                                  json_type(
                                      a.metadata_json,
                                      '$.timeout_policy'
                                  ) <> 'object'
                                  OR (
                                      json_extract(
                                          a.metadata_json,
                                          '$.timeout_policy.heartbeat_timeout_ms'
                                      ) IS NOT NULL
                                      AND (
                                          json_type(
                                              a.metadata_json,
                                              '$.timeout_policy.heartbeat_timeout_ms'
                                          ) <> 'integer'
                                          OR CAST(
                                              json_extract(
                                                  a.metadata_json,
                                                  '$.timeout_policy.heartbeat_timeout_ms'
                                              ) AS INTEGER
                                          ) < 1
                                      )
                                  )
                              )
                          )
                      )
                ),
                ranked_deadlines AS (
                    SELECT
                        attempt_id,
                        deadline_kind,
                        deadline_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY attempt_id
                            ORDER BY deadline_at, deadline_priority
                        ) AS deadline_rank
                    FROM deadline_candidates
                ),
                expired_attempts AS (
                    SELECT ranked.attempt_id, ranked.deadline_at
                    FROM ranked_deadlines AS ranked
                    JOIN attempts AS attempt
                      ON attempt.attempt_id = ranked.attempt_id
                    WHERE ranked.deadline_rank = 1
                    ORDER BY
                        ranked.deadline_at,
                        attempt.run_id,
                        attempt.node_id,
                        attempt.attempt_number
                    LIMIT ?
                )
                SELECT a.*, r.metadata_json AS run_metadata_json,
                       i.run_id AS lease_run_id, i.key AS lease_key,
                       i.request_hash AS lease_request_hash,
                       i.schema_version AS lease_schema_version,
                       i.status AS lease_status, i.owner_id AS lease_owner_id,
                       i.claim_token AS lease_claim_token,
                       i.lease_expires_at AS lease_expires_at,
                       i.result_json AS lease_result_json,
                       i.claim_count AS lease_claim_count,
                       i.created_at AS lease_created_at,
                       i.updated_at AS lease_updated_at,
                       i.completed_at AS lease_completed_at
                FROM expired_attempts AS expired
                JOIN attempts AS a ON a.attempt_id = expired.attempt_id
                JOIN runs AS r ON r.run_id = a.run_id
                LEFT JOIN idempotency_records AS i
                  ON i.run_id = a.run_id AND i.key = a.idempotency_key
                ORDER BY
                    expired.deadline_at,
                    a.run_id,
                    a.node_id,
                    a.attempt_number
                """,
                (
                    current_time,
                    current_time,
                    current_time,
                    current_time,
                    current_time,
                    bounded_limit,
                ),
            ).fetchall()
        expired: list[
            tuple[AttemptRecord, IdempotencyRecord | None, str, float]
        ] = []
        for row in rows:
            attempt = self._attempt_from_row(row)
            run_metadata = _json_load(row["run_metadata_json"])
            if not isinstance(run_metadata, dict):
                raise ProjectionConflictError("Run metadata must be an object")
            record = None
            if row["lease_run_id"] is not None:
                record = IdempotencyRecord(
                    run_id=row["lease_run_id"],
                    key=row["lease_key"],
                    request_hash=row["lease_request_hash"],
                    schema_version=row["lease_schema_version"],
                    status=row["lease_status"],
                    owner_id=row["lease_owner_id"],
                    claim_token=row["lease_claim_token"],
                    lease_expires_at=row["lease_expires_at"],
                    result=_json_load(row["lease_result_json"]),
                    claim_count=row["lease_claim_count"],
                    created_at=row["lease_created_at"],
                    updated_at=row["lease_updated_at"],
                    completed_at=row["lease_completed_at"],
                )
            deadline = _attempt_deadline_from_values(
                _metadata_timestamp(run_metadata, "deadline_at"),
                attempt,
                heartbeat_deadline_at=_heartbeat_deadline(attempt, record),
            )
            if deadline is not None and deadline[1] <= current_time:
                expired.append((attempt, record, deadline[0], deadline[1]))
        expired.sort(
            key=lambda item: (
                item[3],
                item[0].run_id,
                item[0].node_id,
                item[0].attempt_number,
            )
        )
        return expired

    @_audit_stale_activity_rejection
    def renew_activity_lease(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        request_hash: str,
        owner_id: str,
        *,
        claim_token: str,
        fencing_token: int,
        lease_seconds: float = 60.0,
        now: float | None = None,
    ) -> IdempotencyRecord:
        """Atomically extend the current unexpired owner/fencing lease."""

        current_time = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        duration = _lease_duration(lease_seconds)
        try:
            expected_fencing = int(fencing_token)
        except (TypeError, ValueError) as exc:
            raise IdempotencyConflictError("invalid Activity fencing token") from exc
        with self._write_transaction() as conn:
            run, _node, attempt = self._load_activity_tx(
                conn,
                run_id,
                node_id,
                attempt_id,
            )
            record = self._get_idempotency_tx(conn, run_id, attempt.idempotency_key)
            if record is None:
                raise IdempotencyConflictError("Activity has no idempotency lease")
            self._validate_claim_owner(
                record,
                request_hash=request_hash,
                owner_id=owner_id,
                claim_token=claim_token,
            )
            if attempt.status not in {AttemptStatus.CLAIMED, AttemptStatus.RUNNING}:
                raise InvalidStateTransition("only an active Attempt lease can be renewed")
            if (
                expected_fencing != record.claim_count
                or attempt.fencing_token != expected_fencing
                or attempt.worker_id != owner_id
                or attempt.lease_id != claim_token
            ):
                raise IdempotencyConflictError("stale Activity fencing token")
            if (
                record.status is not IdempotencyStatus.IN_PROGRESS
                or record.lease_expires_at <= current_time
            ):
                raise IdempotencyConflictError("Activity lease is expired or completed")
            phase_deadline = _attempt_deadline(
                run,
                attempt,
            )
            heartbeat_timeout = _timeout_milliseconds(
                attempt,
                "heartbeat_timeout_ms",
            )
            heartbeat_deadline = (
                current_time + heartbeat_timeout / 1_000
                if heartbeat_timeout is not None
                else None
            )
            deadline_cap = _earliest_deadline(
                phase_deadline[1] if phase_deadline is not None else None,
                heartbeat_deadline,
            )
            if deadline_cap is not None and current_time >= deadline_cap:
                raise IdempotencyConflictError(
                    "Activity deadline prevents lease renewal"
                )
            requested_expiry = max(
                record.lease_expires_at + 0.001,
                current_time + duration,
            )
            expires_at = (
                min(requested_expiry, deadline_cap)
                if deadline_cap is not None
                else requested_expiry
            )
            updated = conn.execute(
                """
                UPDATE idempotency_records
                SET lease_expires_at = ?, updated_at = ?
                WHERE run_id = ? AND key = ? AND status = ?
                  AND owner_id = ? AND claim_token = ? AND claim_count = ?
                  AND lease_expires_at > ?
                """,
                (
                    expires_at,
                    current_time,
                    run_id,
                    attempt.idempotency_key,
                    IdempotencyStatus.IN_PROGRESS.value,
                    owner_id,
                    claim_token,
                    expected_fencing,
                    current_time,
                ),
            )
            if updated.rowcount != 1:
                raise IdempotencyConflictError("Activity lease changed concurrently")
            self._fault("lease_renew.after_update")
            renewed = self._get_idempotency_tx(conn, run_id, attempt.idempotency_key)
            assert renewed is not None
            return renewed

    def complete_idempotency(
        self,
        run_id: str,
        key: str,
        request_hash: str,
        owner_id: str,
        *,
        claim_token: str,
        result: JsonValue = None,
        now: float | None = None,
    ) -> IdempotencyRecord:
        now = _finite_timestamp(now if now is not None else utc_timestamp(), "now")
        result_json = _json_dump(result)
        with self._write_transaction() as conn:
            row = conn.execute(
                "SELECT * FROM idempotency_records WHERE run_id = ? AND key = ?",
                (run_id, key),
            ).fetchone()
            if row is None:
                raise IdempotencyConflictError(f"idempotency key not claimed: {key}")
            record = self._idempotency_from_row(row)
            if record.request_hash != request_hash:
                raise IdempotencyConflictError(f"request hash mismatch for key: {key}")
            if record.owner_id != owner_id:
                raise IdempotencyConflictError(f"idempotency key owned by {record.owner_id!r}")
            if record.claim_token != claim_token:
                raise IdempotencyConflictError("stale idempotency claim token")
            if record.status is IdempotencyStatus.COMPLETED:
                if _json_dump(record.result) != result_json:
                    raise IdempotencyConflictError(
                        f"idempotency key {key!r} already completed with another result"
                    )
                return record
            if record.lease_expires_at <= now:
                raise IdempotencyConflictError(
                    "expired idempotency claim cannot be completed"
                )
            conn.execute(
                """
                UPDATE idempotency_records SET
                    status = ?, result_json = ?, updated_at = ?, completed_at = ?,
                    lease_expires_at = ?
                WHERE run_id = ? AND key = ?
                """,
                (
                    IdempotencyStatus.COMPLETED.value,
                    result_json,
                    now,
                    now,
                    now,
                    run_id,
                    key,
                ),
            )
            row = conn.execute(
                "SELECT * FROM idempotency_records WHERE run_id = ? AND key = ?",
                (run_id, key),
            ).fetchone()
            return self._idempotency_from_row(row)

    def register_approval_grant(
        self,
        run_id: str,
        *,
        grant_binding_digest: str,
        action_digest: str,
        policy_digest: str,
        expires_at: float,
        now: float | None = None,
    ) -> EventRecord:
        """Persist a trusted-control-plane grant without retaining actor text.

        Activity execution must never call this issuance API.  The corresponding
        consumption happens only inside ``commit_activity_policy_decision``.
        """

        grant_binding_digest = _sha256_digest(
            grant_binding_digest,
            "grant_binding_digest",
        )
        action_digest = _sha256_digest(action_digest, "action_digest")
        policy_digest = _sha256_digest(policy_digest, "policy_digest")
        expires_at = _finite_timestamp(expires_at, "expires_at")
        occurred_at = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        if expires_at <= occurred_at:
            raise ValueError("approval grant must be unexpired when issued")
        payload: dict[str, JsonValue] = {
            "kind": "grant_issued",
            "grant_binding_digest": grant_binding_digest,
            "action_digest": action_digest,
            "policy_digest": policy_digest,
            "expires_at": expires_at,
        }
        event_id = f"evt_approval_issue_{grant_binding_digest}"
        with self._write_transaction() as conn:
            existing_row = conn.execute(
                "SELECT * FROM domain_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if existing_row is not None:
                existing = self._event_from_row(existing_row)
                existing_payload = dict(existing.payload)
                existing_payload.pop("projection", None)
                if (
                    existing.run_id != run_id
                    or existing.event_type != "approval.resolved"
                    or existing_payload != payload
                ):
                    raise ProjectionConflictError(
                        "approval grant identity was reused with different content"
                    )
                return existing

            run_row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if run_row is None:
                raise RunNotFoundError(run_id)
            run = self._run_from_row(run_row)
            request_rows = conn.execute(
                """
                SELECT * FROM domain_events
                WHERE run_id = ? AND event_type = 'approval.requested'
                  AND json_extract(payload_json, '$.action_digest') = ?
                ORDER BY seq
                LIMIT 2
                """,
                (run_id, action_digest),
            ).fetchall()
            if len(request_rows) > 1:
                raise ProjectionConflictError(
                    "approval action matches multiple pending requests"
                )
            node_id: str | None = None
            attempt_id: str | None = None
            target_run = run
            target_node: NodeRecord | None = None
            target_attempt: AttemptRecord | None = None
            if request_rows:
                request_event = self._event_from_row(request_rows[0])
                request_payload = dict(request_event.payload)
                request_payload.pop("projection", None)
                if (
                    request_event.node_id is None
                    or request_event.attempt_id is None
                    or request_payload.get("kind")
                    != "activity_approval_request"
                    or request_payload.get("outcome") != "require_approval"
                    or request_payload.get("action_digest") != action_digest
                    or request_payload.get("policy_digest") != policy_digest
                ):
                    raise ProjectionConflictError(
                        "approval request binding is malformed"
                    )
                node_id = request_event.node_id
                attempt_id = request_event.attempt_id
                _loaded_run, node, attempt = self._load_activity_tx(
                    conn,
                    run_id,
                    node_id,
                    attempt_id,
                )
                if (
                    run.status is not RunStatus.WAITING_APPROVAL
                    or node.status is not NodeStatus.WAITING_APPROVAL
                    or attempt.status is not AttemptStatus.WAITING_APPROVAL
                ):
                    raise InvalidStateTransition(
                        "approval grant requires a pending Activity request"
                    )
                target_run = replace(run, status=RunStatus.RUNNING)
                target_node = replace(node, status=NodeStatus.READY)
                target_attempt = replace(
                    attempt,
                    status=AttemptStatus.SCHEDULED,
                )

            return self._commit_event_tx(
                conn,
                run,
                "approval.resolved",
                payload=payload,
                event_id=event_id,
                occurred_at=occurred_at,
                node_id=node_id,
                attempt_id=attempt_id,
                run_projection=target_run,
                node_projection=target_node,
                attempt_projection=target_attempt,
            )

    def get_activity_policy_event(
        self,
        run_id: str,
        attempt_id: str,
    ) -> EventRecord | None:
        """Return the durable policy decision for an Attempt, if present."""

        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT * FROM domain_events
                WHERE run_id = ? AND attempt_id = ? AND event_type = 'policy.decided'
                ORDER BY seq DESC
                LIMIT 1
                """,
                (run_id, attempt_id),
            ).fetchone()
        return self._event_from_row(row) if row is not None else None

    def get_tool_receipt(
        self,
        run_id: str,
        attempt_id: str,
    ) -> "ToolReceipt | None":
        """Read and fully verify the latest durable Tool terminal receipt."""

        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT * FROM domain_events
                WHERE run_id = ? AND attempt_id = ?
                  AND event_type IN (
                    'attempt.succeeded',
                    'attempt.failed',
                    'attempt.timed_out',
                    'attempt.cancelled',
                    'attempt.abandoned',
                    'attempt.outcome_unknown'
                  )
                ORDER BY seq DESC
                LIMIT 1
                """,
                (run_id, attempt_id),
            ).fetchone()
        if row is None:
            return None
        event = self._event_from_row(row)
        raw_receipt = event.payload.get("tool_receipt")
        raw_digest = event.payload.get("tool_receipt_digest")
        if raw_receipt is None and raw_digest is None:
            return None
        try:
            from .executor import ToolReceipt, ToolReceiptError

            if not isinstance(raw_receipt, dict):
                raise ToolReceiptError("ToolReceipt payload must be an object")
            receipt = ToolReceipt.from_dict(raw_receipt)
            if (
                receipt.run_id != run_id
                or receipt.attempt_id != attempt_id
                or receipt.receipt_digest
                != _sha256_digest(raw_digest, "tool_receipt_digest")
                or event.event_type
                != f"attempt.{receipt.attempt_status.value}"
            ):
                raise ToolReceiptError(
                    "ToolReceipt does not match its terminal Event"
                )
        except (TypeError, ValueError) as exc:
            raise StoreSchemaError(
                "durable ToolReceipt failed integrity validation"
            ) from exc
        return receipt

    def reject_activity_approval(
        self,
        run_id: str,
        attempt_id: str,
        *,
        reason_code: str = "approval_rejected",
        now: float | None = None,
    ) -> EventRecord:
        """Terminalize one pending Activity approval from a trusted ingress."""

        reason = _safe_receipt_code(reason_code, "reason_code")
        occurred_at = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        event_id = "evt_approval_reject_" + hashlib.sha256(
            f"{run_id}\0{attempt_id}\0{reason}".encode("utf-8")
        ).hexdigest()
        with self._write_transaction() as conn:
            existing_row = conn.execute(
                "SELECT * FROM domain_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if existing_row is not None:
                existing = self._event_from_row(existing_row)
                if (
                    existing.run_id != run_id
                    or existing.attempt_id != attempt_id
                    or existing.event_type != "approval.resolved"
                    or existing.payload.get("kind") != "request_rejected"
                    or existing.payload.get("reason_code") != reason
                ):
                    raise ProjectionConflictError(
                        "approval rejection identity was reused"
                    )
                return existing
            attempt_row = conn.execute(
                "SELECT * FROM attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if attempt_row is None:
                raise ProjectionConflictError("approval Attempt does not exist")
            attempt = self._attempt_from_row(attempt_row)
            run, node, attempt = self._load_activity_tx(
                conn,
                run_id,
                attempt.node_id,
                attempt_id,
            )
            request_row = conn.execute(
                """
                SELECT * FROM domain_events
                WHERE run_id = ? AND attempt_id = ?
                  AND event_type = 'approval.requested'
                ORDER BY seq DESC
                LIMIT 1
                """,
                (run_id, attempt_id),
            ).fetchone()
            if request_row is None:
                raise ProjectionConflictError(
                    "pending Activity has no approval request"
                )
            request = self._event_from_row(request_row)
            request_payload = dict(request.payload)
            request_payload.pop("projection", None)
            if (
                run.status is not RunStatus.WAITING_APPROVAL
                or node.status is not NodeStatus.WAITING_APPROVAL
                or attempt.status is not AttemptStatus.WAITING_APPROVAL
                or request_payload.get("kind") != "activity_approval_request"
            ):
                raise InvalidStateTransition(
                    "approval rejection requires a pending Activity request"
                )
            record = self._get_idempotency_tx(
                conn,
                run_id,
                attempt.idempotency_key,
            )
            if (
                record is not None
                and record.status is not IdempotencyStatus.IN_PROGRESS
            ):
                raise IdempotencyConflictError(
                    "pending approval idempotency record is not resumable"
                )
            result: dict[str, JsonValue] = {
                "outcome": "policy_rejected",
                "reason_code": reason,
                "decision_digest": request_payload.get("decision_digest"),
            }
            result_json = _json_dump(result)
            if record is not None:
                updated = conn.execute(
                    """
                    UPDATE idempotency_records
                    SET status = ?, result_json = ?, updated_at = ?,
                        completed_at = ?, lease_expires_at = ?
                    WHERE run_id = ? AND key = ? AND status = ?
                      AND claim_count = ?
                    """,
                    (
                        IdempotencyStatus.COMPLETED.value,
                        result_json,
                        occurred_at,
                        occurred_at,
                        occurred_at,
                        run_id,
                        attempt.idempotency_key,
                        IdempotencyStatus.IN_PROGRESS.value,
                        record.claim_count,
                    ),
                )
                if updated.rowcount != 1:
                    raise IdempotencyConflictError(
                        "approval rejection changed concurrently"
                    )
            payload: dict[str, JsonValue] = {
                "kind": "request_rejected",
                "reason_code": reason,
                "approval_request_digest": request_payload.get(
                    "approval_request_digest"
                ),
                "action_digest": request_payload.get("action_digest"),
                "policy_digest": request_payload.get("policy_digest"),
            }
            return self._commit_event_tx(
                conn,
                run,
                "approval.resolved",
                payload=payload,
                event_id=event_id,
                occurred_at=occurred_at,
                node_id=node.node_id,
                attempt_id=attempt.attempt_id,
                run_projection=replace(
                    run,
                    status=RunStatus.FAILED,
                    error={"class": "policy", "code": reason},
                ),
                node_projection=replace(
                    node,
                    status=NodeStatus.FAILED,
                    error={"class": "policy", "code": reason},
                ),
                attempt_projection=replace(
                    attempt,
                    status=AttemptStatus.FAILED,
                    result=result,
                    error={"class": "policy", "code": reason},
                    finished_at=max(occurred_at, attempt.scheduled_at),
                ),
            )

    def verify_registered_approval(
        self,
        run_id: str,
        *,
        grant_binding_digest: str,
        action_digest: str,
        policy_digest: str,
        now: float | None = None,
    ) -> bool:
        """Read-only preflight for the append-only trusted issuance record."""

        grant_binding_digest = _sha256_digest(
            grant_binding_digest,
            "grant_binding_digest",
        )
        action_digest = _sha256_digest(action_digest, "action_digest")
        policy_digest = _sha256_digest(policy_digest, "policy_digest")
        current_time = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM domain_events WHERE event_id = ?",
                (f"evt_approval_issue_{grant_binding_digest}",),
            ).fetchone()
        if row is None:
            return False
        event = self._event_from_row(row)
        payload = dict(event.payload)
        payload.pop("projection", None)
        expires_at = payload.get("expires_at")
        return (
            event.run_id == run_id
            and payload.get("kind") == "grant_issued"
            and payload.get("grant_binding_digest") == grant_binding_digest
            and payload.get("action_digest") == action_digest
            and payload.get("policy_digest") == policy_digest
            and isinstance(expires_at, (int, float))
            and float(expires_at) > current_time
        )

    def commit_activity_policy_decision(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        request_hash: str,
        owner_id: str,
        *,
        claim_token: str,
        outcome: str,
        reason_code: str,
        action_digest: str,
        policy_digest: str,
        profile_digest: str,
        decision_digest: str,
        approval_grant_digest: str | None = None,
        now: float | None = None,
    ) -> EventRecord:
        """Atomically authorize, reject, or durably suspend one Activity claim.

        For approval-backed ALLOW decisions, grant validation, single-use
        consumption, and the ``policy.decided`` authorization Event share this
        transaction.  A committed ALLOW Event is therefore the restart-safe
        proof required before ``attempt.started``. ``REQUIRE_APPROVAL`` instead
        releases execution capacity and persists one resumable request without
        terminalizing the Attempt.
        """

        outcome = _safe_receipt_code(outcome, "outcome")
        if outcome not in {"allow", "deny", "require_approval"}:
            raise ValueError("outcome is not a policy decision")
        reason_code = _safe_receipt_code(reason_code, "reason_code")
        action_digest = _sha256_digest(action_digest, "action_digest")
        policy_digest = _sha256_digest(policy_digest, "policy_digest")
        profile_digest = _sha256_digest(profile_digest, "profile_digest")
        decision_digest = _sha256_digest(decision_digest, "decision_digest")
        if approval_grant_digest is not None:
            approval_grant_digest = _sha256_digest(
                approval_grant_digest,
                "approval_grant_digest",
            )
            if outcome != "allow":
                raise ValueError("only an ALLOW decision may consume an approval")

        current_time = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        claim_token_digest = hashlib.sha256(claim_token.encode("utf-8")).hexdigest()
        binding_digest = hashlib.sha256(
            f"{run_id}\0{node_id}\0{attempt_id}\0{claim_token}".encode("utf-8")
        ).hexdigest()
        approval_request_digest = hashlib.sha256(
            (
                f"{run_id}\0{node_id}\0{attempt_id}\0{action_digest}\0"
                f"{policy_digest}\0{profile_digest}"
            ).encode("utf-8")
        ).hexdigest()
        event_type = (
            "approval.requested"
            if outcome == "require_approval"
            else "policy.decided"
        )
        event_id = (
            f"evt_policy_approval_{approval_grant_digest}"
            if approval_grant_digest is not None
            else (
                f"evt_activity_approval_{approval_request_digest}"
                if outcome == "require_approval"
                else f"evt_policy_{binding_digest}"
            )
        )
        payload: dict[str, JsonValue] = {
            "kind": (
                "activity_approval_request"
                if outcome == "require_approval"
                else "activity_authorization"
            ),
            "outcome": outcome,
            "reason_code": reason_code,
            "action_digest": action_digest,
            "policy_digest": policy_digest,
            "profile_digest": profile_digest,
            "decision_digest": decision_digest,
            "claim_token_digest": claim_token_digest,
            "approval_grant_digest": approval_grant_digest,
        }
        if outcome == "require_approval":
            payload["approval_request_digest"] = approval_request_digest

        with self._write_transaction() as conn:
            run, node, attempt = self._load_activity_tx(
                conn,
                run_id,
                node_id,
                attempt_id,
            )
            record = self._get_idempotency_tx(
                conn,
                run_id,
                attempt.idempotency_key,
            )
            if record is None:
                raise IdempotencyConflictError("Activity has no idempotency claim")
            self._validate_claim_owner(
                record,
                request_hash=request_hash,
                owner_id=owner_id,
                claim_token=claim_token,
            )
            if (
                attempt.worker_id not in {None, owner_id}
                or attempt.lease_id not in {None, claim_token}
                or attempt.fencing_token != record.claim_count
            ):
                raise IdempotencyConflictError(
                    "policy decision does not match the current fencing token"
                )

            existing_row = conn.execute(
                "SELECT * FROM domain_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if existing_row is not None:
                existing = self._event_from_row(existing_row)
                existing_payload = dict(existing.payload)
                existing_payload.pop("projection", None)
                if (
                    existing.run_id != run_id
                    or existing.node_id != node_id
                    or existing.attempt_id != attempt_id
                    or existing.event_type != event_type
                    or existing_payload != payload
                ):
                    raise ProjectionConflictError(
                        "policy authorization identity was reused with different content"
                    )
                return existing

            if (
                attempt.status is not AttemptStatus.CLAIMED
                or attempt.worker_id != owner_id
                or attempt.lease_id != claim_token
                or attempt.fencing_token != record.claim_count
            ):
                raise IdempotencyConflictError(
                    "policy decision requires the current CLAIMED fencing token"
                )
            if (
                record.status is not IdempotencyStatus.IN_PROGRESS
                or record.lease_expires_at <= current_time
            ):
                raise IdempotencyConflictError(
                    "policy decision cannot use an expired or completed claim"
                )

            if approval_grant_digest is not None:
                issue_row = conn.execute(
                    "SELECT * FROM domain_events WHERE event_id = ?",
                    (f"evt_approval_issue_{approval_grant_digest}",),
                ).fetchone()
                if issue_row is None:
                    raise IdempotencyConflictError("approval grant was not issued")
                issue = self._event_from_row(issue_row)
                issue_payload = dict(issue.payload)
                issue_payload.pop("projection", None)
                expected_issue = {
                    "kind": "grant_issued",
                    "grant_binding_digest": approval_grant_digest,
                    "action_digest": action_digest,
                    "policy_digest": policy_digest,
                    "expires_at": issue_payload.get("expires_at"),
                }
                if (
                    issue.run_id != run_id
                    or issue_payload != expected_issue
                    or not isinstance(issue_payload["expires_at"], (int, float))
                    or float(issue_payload["expires_at"]) <= current_time
                ):
                    raise IdempotencyConflictError(
                        "approval grant does not match this action or is expired"
                    )

            target_node: NodeRecord | None = None
            target_attempt: AttemptRecord | None = None
            target_run = run
            if outcome == "deny":
                safe_result: dict[str, JsonValue] = {
                    "outcome": "policy_rejected",
                    "reason_code": reason_code,
                    "decision_digest": decision_digest,
                }
                result_json = _json_dump(safe_result)
                completed = conn.execute(
                    """
                    UPDATE idempotency_records
                    SET status = ?, result_json = ?, updated_at = ?,
                        completed_at = ?, lease_expires_at = ?
                    WHERE run_id = ? AND key = ? AND status = ?
                      AND owner_id = ? AND claim_token = ?
                      AND claim_count = ? AND lease_expires_at > ?
                    """,
                    (
                        IdempotencyStatus.COMPLETED.value,
                        result_json,
                        current_time,
                        current_time,
                        current_time,
                        run_id,
                        attempt.idempotency_key,
                        IdempotencyStatus.IN_PROGRESS.value,
                        owner_id,
                        claim_token,
                        record.claim_count,
                        current_time,
                    ),
                )
                if completed.rowcount != 1:
                    raise IdempotencyConflictError(
                        "policy rejection lost the current Activity claim"
                    )
                target_attempt = replace(
                    attempt,
                    status=AttemptStatus.FAILED,
                    worker_id=None,
                    lease_id=None,
                    result=safe_result,
                    error={"class": "policy", "code": reason_code},
                    finished_at=max(current_time, attempt.scheduled_at),
                )
                target_node = replace(
                    node,
                    status=NodeStatus.FAILED,
                    error={"class": "policy", "code": reason_code},
                )
            elif outcome == "require_approval":
                released = conn.execute(
                    """
                    UPDATE idempotency_records
                    SET lease_expires_at = ?, updated_at = ?
                    WHERE run_id = ? AND key = ? AND status = ?
                      AND owner_id = ? AND claim_token = ?
                      AND claim_count = ? AND lease_expires_at > ?
                    """,
                    (
                        current_time,
                        current_time,
                        run_id,
                        attempt.idempotency_key,
                        IdempotencyStatus.IN_PROGRESS.value,
                        owner_id,
                        claim_token,
                        record.claim_count,
                        current_time,
                    ),
                )
                if released.rowcount != 1:
                    raise IdempotencyConflictError(
                        "approval request lost the current Activity claim"
                    )
                target_attempt = replace(
                    attempt,
                    status=AttemptStatus.WAITING_APPROVAL,
                    worker_id=None,
                    lease_id=None,
                )
                target_node = replace(
                    node,
                    status=NodeStatus.WAITING_APPROVAL,
                )
                target_run = replace(
                    run,
                    status=RunStatus.WAITING_APPROVAL,
                )

            return self._commit_event_tx(
                conn,
                run,
                event_type,
                payload=payload,
                event_id=event_id,
                occurred_at=current_time,
                node_id=node_id,
                attempt_id=attempt_id,
                run_projection=target_run,
                node_projection=target_node,
                attempt_projection=target_attempt,
                allow_policy_rejection_transition=outcome == "deny",
            )

    @staticmethod
    def _admit_activity_tx(
        conn: sqlite3.Connection,
        attempt: AttemptRecord,
        *,
        owner_id: str,
        max_active_attempts: int | None,
        worker_capacity: int | None,
    ) -> None:
        """Reserve execution capacity by deriving reservations from active Attempts."""

        if max_active_attempts is None and worker_capacity is None:
            return
        if max_active_attempts is not None:
            row = conn.execute(
                """
                SELECT COUNT(*) AS active_count
                FROM attempts INDEXED BY attempts_admission_status_idx
                WHERE status IN ('claimed', 'running')
                """
            ).fetchone()
            if int(row["active_count"]) >= max_active_attempts:
                raise ActivityAdmissionDenied("global_capacity")
        if worker_capacity is not None:
            row = conn.execute(
                """
                SELECT COUNT(*) AS active_count
                FROM attempts INDEXED BY attempts_admission_status_idx
                WHERE status IN ('claimed', 'running') AND worker_id = ?
                """,
                (owner_id,),
            ).fetchone()
            if int(row["active_count"]) >= worker_capacity:
                raise ActivityAdmissionDenied("worker_capacity")

        resource_keys = attempt.metadata.get("resource_keys")
        concurrency_key = attempt.metadata.get("concurrency_key")
        if (
            not isinstance(resource_keys, list)
            or any(not isinstance(key, str) or not key for key in resource_keys)
        ):
            raise ProjectionConflictError(
                "Attempt resource_keys must be a list of non-empty strings"
            )
        if concurrency_key is not None and (
            not isinstance(concurrency_key, str) or not concurrency_key
        ):
            raise ProjectionConflictError(
                "Attempt concurrency_key must be a non-empty string or null"
            )
        if not resource_keys and concurrency_key is None:
            return

        conflict_clauses = [
            "json_type(active.metadata_json, '$.resource_keys') IS NULL",
            "json_type(active.metadata_json, '$.resource_keys') <> 'array'",
        ]
        parameters: list[Any] = []
        if resource_keys:
            placeholders = ",".join("?" for _ in resource_keys)
            conflict_clauses.append(
                "EXISTS ("
                "SELECT 1 "
                "FROM json_each(active.metadata_json, '$.resource_keys') AS resource "
                f"WHERE resource.type = 'text' AND resource.value IN ({placeholders})"
                ")"
            )
            parameters.extend(resource_keys)
        if concurrency_key is not None:
            conflict_clauses.append(
                "json_extract(active.metadata_json, '$.concurrency_key') = ?"
            )
            parameters.append(concurrency_key)
        row = conn.execute(
            f"""
            SELECT 1
            FROM attempts AS active INDEXED BY attempts_admission_status_idx
            WHERE active.status IN ('claimed', 'running')
              AND ({" OR ".join(conflict_clauses)})
            LIMIT 1
            """,
            parameters,
        ).fetchone()
        if row is not None:
            raise ActivityAdmissionDenied("resource_conflict")

    def claim_activity(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        request_hash: str,
        owner_id: str,
        *,
        lease_seconds: float = 60.0,
        now: float | None = None,
        max_active_attempts: int | None = None,
        worker_capacity: int | None = None,
    ) -> tuple[IdempotencyClaim, EventRecord | None]:
        """Atomically claim an idempotency key and move an Attempt to CLAIMED.

        A crash after this commit leaves an explicit CLAIMED Attempt. It does
        not authorize a second execution: recovery must inspect or expire the
        claim before deciding whether the Activity can be re-claimed.
        """

        now = _finite_timestamp(now if now is not None else utc_timestamp(), "now")
        lease_duration = _lease_duration(lease_seconds)
        if max_active_attempts is not None and max_active_attempts < 1:
            raise ValueError("max_active_attempts must be positive")
        if worker_capacity is not None and worker_capacity < 1:
            raise ValueError("worker_capacity must be positive")
        with self._write_transaction() as conn:
            run, node, attempt = self._load_activity_tx(conn, run_id, node_id, attempt_id)
            if attempt.status is not AttemptStatus.SCHEDULED:
                record = self._get_idempotency_tx(conn, run_id, attempt.idempotency_key)
                if record is None:
                    raise ProjectionConflictError("non-scheduled Attempt has no claim record")
                return IdempotencyClaim(ClaimDisposition.CONFLICT, record), None
            self._admit_activity_tx(
                conn,
                attempt,
                owner_id=owner_id,
                max_active_attempts=max_active_attempts,
                worker_capacity=worker_capacity,
            )
            schedule_deadline = _attempt_deadline(run, attempt)
            if schedule_deadline is not None and now >= schedule_deadline[1]:
                raise IdempotencyConflictError(
                    f"{schedule_deadline[0]} deadline prevents Activity claim"
                )
            start_timeout = _timeout_milliseconds(
                attempt,
                "start_timeout_ms",
            )
            start_deadline = (
                now + start_timeout / 1_000
                if start_timeout is not None
                else None
            )
            heartbeat_timeout = _timeout_milliseconds(
                attempt,
                "heartbeat_timeout_ms",
            )
            heartbeat_deadline = (
                now + heartbeat_timeout / 1_000
                if heartbeat_timeout is not None
                else None
            )
            lease_deadline = _earliest_deadline(
                _run_deadline(run),
                start_deadline,
                heartbeat_deadline,
            )
            claim = self._claim_idempotency_tx(
                conn,
                run_id,
                attempt.idempotency_key,
                request_hash,
                owner_id,
                lease_seconds=lease_duration,
                lease_deadline_at=lease_deadline,
                now=now,
                minimum_fencing_token=attempt.attempt_number,
            )
            if claim.disposition is not ClaimDisposition.ACQUIRED:
                return claim, None
            self._fault("claim.after_idempotency")
            claimed_metadata = dict(attempt.metadata)
            if start_deadline is not None:
                claimed_metadata["start_deadline_at"] = min(
                    start_deadline,
                    _run_deadline(run)
                    if _run_deadline(run) is not None
                    else start_deadline,
                )
            claimed = replace(
                attempt,
                status=AttemptStatus.CLAIMED,
                worker_id=owner_id,
                lease_id=claim.record.claim_token,
                fencing_token=claim.record.claim_count,
                metadata=claimed_metadata,
            )
            event = self._commit_event_tx(
                conn,
                run,
                "attempt.claimed",
                payload={
                    "fencing_token": claim.record.claim_count,
                    "effect_class": attempt.effect_class,
                },
                event_id=f"evt_claim_{attempt.attempt_id}_{claim.record.claim_count}",
                occurred_at=now,
                node_id=node.node_id,
                attempt_id=attempt.attempt_id,
                run_projection=run,
                node_projection=None,
                attempt_projection=claimed,
            )
            return claim, event

    def claim_activity_with_policy(
        self,
        run_id: str,
        node_id: str,
        candidate_attempt: AttemptRecord,
        request_hash: str,
        owner_id: str,
        *,
        definition_digest: str,
        schedule_new: bool,
        expected_run_version: int,
        expected_node_version: int,
        expected_attempt_version: int | None,
        admission_expires_at: float,
        admission_clock: Callable[[], float],
        policy_binding: Mapping[str, str | None],
        lease_seconds: float = 60.0,
        now: float | None = None,
        max_active_attempts: int | None = None,
        worker_capacity: int | None = None,
    ) -> tuple[
        IdempotencyClaim | None,
        EventRecord | None,
        EventRecord,
    ]:
        """Atomically suspend for approval or claim an authorized Activity.

        All external authorization, attestation, Artifact verification, and
        policy evaluation must finish before entering this method.  The write
        transaction only validates the immutable candidate snapshot, capacity,
        claim CAS, and the supplied digest-only policy binding.
        """

        if not isinstance(candidate_attempt, AttemptRecord):
            raise TypeError("candidate_attempt must be an AttemptRecord")
        if (
            candidate_attempt.run_id != run_id
            or candidate_attempt.node_id != node_id
            or candidate_attempt.status is not AttemptStatus.SCHEDULED
        ):
            raise ProjectionConflictError(
                "admission candidate does not identify a SCHEDULED Activity"
            )
        definition_digest = _sha256_digest(
            definition_digest,
            "definition_digest",
        )
        required_policy = {
            "outcome",
            "reason_code",
            "action_digest",
            "policy_digest",
            "profile_digest",
            "decision_digest",
            "approval_grant_digest",
        }
        if set(policy_binding) != required_policy:
            raise ValueError("policy_binding fields are invalid")
        policy_outcome = policy_binding["outcome"]
        if policy_outcome not in {"allow", "require_approval"}:
            raise ValueError(
                "only ALLOW or REQUIRE_APPROVAL may enter remote admission"
            )
        reason_code = _safe_receipt_code(
            policy_binding["reason_code"],
            "reason_code",
        )
        action_digest = _sha256_digest(
            policy_binding["action_digest"],
            "action_digest",
        )
        policy_digest = _sha256_digest(
            policy_binding["policy_digest"],
            "policy_digest",
        )
        profile_digest = _sha256_digest(
            policy_binding["profile_digest"],
            "profile_digest",
        )
        decision_digest = _sha256_digest(
            policy_binding["decision_digest"],
            "decision_digest",
        )
        approval_grant_digest = policy_binding["approval_grant_digest"]
        if approval_grant_digest is not None:
            approval_grant_digest = _sha256_digest(
                approval_grant_digest,
                "approval_grant_digest",
            )
        if (
            policy_outcome == "require_approval"
            and approval_grant_digest is not None
        ):
            raise ValueError(
                "REQUIRE_APPROVAL cannot consume an approval grant"
            )
        current_time = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        admission_deadline = _finite_timestamp(
            admission_expires_at,
            "admission_expires_at",
        )
        if not callable(admission_clock):
            raise TypeError("admission_clock must be callable")
        lease_duration = _lease_duration(lease_seconds)
        if max_active_attempts is not None and max_active_attempts < 1:
            raise ValueError("max_active_attempts must be positive")
        if worker_capacity is not None and worker_capacity < 1:
            raise ValueError("worker_capacity must be positive")

        with self._write_transaction() as conn:
            # This check must run after BEGIN IMMEDIATE acquires the writer
            # lock.  A request waiting behind another writer cannot linearize
            # with authority that expired during the wait.
            current_time = _finite_timestamp(
                admission_clock(),
                "admission linearization time",
            )
            if current_time >= admission_deadline:
                raise IdempotencyConflictError(
                    "remote admission authority expired before claim"
                )
            run_row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            node_row = conn.execute(
                "SELECT * FROM node_runs WHERE run_id = ? AND node_id = ?",
                (run_id, node_id),
            ).fetchone()
            if run_row is None or node_row is None:
                raise ProjectionConflictError("admission candidate disappeared")
            run = self._run_from_row(run_row)
            node = self._node_from_row(node_row)
            if (
                node.projection_version != expected_node_version
                or run.status is not RunStatus.RUNNING
                or node.status is not NodeStatus.READY
                or run.definition_digest != definition_digest
                or candidate_attempt.metadata.get("definition_digest")
                != definition_digest
                or candidate_attempt.metadata.get("request_hash")
                != request_hash
            ):
                raise ConcurrentProjectionUpdate(
                    "remote_admission",
                    expected_node_version,
                    node.projection_version,
                )

            if schedule_new:
                if expected_attempt_version is not None:
                    raise ProjectionConflictError(
                        "new candidate has an Attempt projection version"
                    )
                collision = conn.execute(
                    "SELECT 1 FROM attempts WHERE attempt_id = ?",
                    (candidate_attempt.attempt_id,),
                ).fetchone()
                active = conn.execute(
                    """
                    SELECT 1 FROM attempts
                    WHERE run_id = ? AND node_id = ?
                      AND status IN ('scheduled', 'claimed', 'waiting_approval', 'running')
                    LIMIT 1
                    """,
                    (run_id, node_id),
                ).fetchone()
                maximum_row = conn.execute(
                    """
                    SELECT COALESCE(MAX(attempt_number), 0) AS maximum_number
                    FROM attempts WHERE run_id = ? AND node_id = ?
                    """,
                    (run_id, node_id),
                ).fetchone()
                assert maximum_row is not None
                if (
                    collision is not None
                    or active is not None
                    or candidate_attempt.attempt_number
                    != int(maximum_row["maximum_number"]) + 1
                ):
                    raise ProjectionConflictError(
                        "admission candidate lost the scheduling CAS"
                    )
                self._commit_event_tx(
                    conn,
                    run,
                    "attempt.scheduled",
                    payload={
                        "attempt_number": candidate_attempt.attempt_number,
                        "operation_key_digest": hashlib.sha256(
                            str(
                                candidate_attempt.metadata["operation_key"]
                            ).encode("utf-8")
                        ).hexdigest(),
                    },
                    event_id=f"evt_schedule_{candidate_attempt.attempt_id}",
                    occurred_at=current_time,
                    node_id=node_id,
                    attempt_id=candidate_attempt.attempt_id,
                    run_projection=run,
                    node_projection=replace(
                        node,
                        attempt_count=max(
                            node.attempt_count,
                            candidate_attempt.attempt_number,
                        ),
                    ),
                    attempt_projection=candidate_attempt,
                )
                run_row = conn.execute(
                    "SELECT * FROM runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                node_row = conn.execute(
                    "SELECT * FROM node_runs WHERE run_id = ? AND node_id = ?",
                    (run_id, node_id),
                ).fetchone()
                attempt_row = conn.execute(
                    "SELECT * FROM attempts WHERE attempt_id = ?",
                    (candidate_attempt.attempt_id,),
                ).fetchone()
                assert run_row is not None and node_row is not None
                assert attempt_row is not None
                run = self._run_from_row(run_row)
                node = self._node_from_row(node_row)
                attempt = self._attempt_from_row(attempt_row)
            else:
                attempt_row = conn.execute(
                    "SELECT * FROM attempts WHERE attempt_id = ?",
                    (candidate_attempt.attempt_id,),
                ).fetchone()
                if attempt_row is None:
                    raise ProjectionConflictError(
                        "scheduled admission candidate disappeared"
                    )
                attempt = self._attempt_from_row(attempt_row)
                if (
                    expected_attempt_version is None
                    or attempt.projection_version != expected_attempt_version
                    or attempt != candidate_attempt
                ):
                    raise ProjectionConflictError(
                        "scheduled admission candidate changed"
                    )
                competing = conn.execute(
                    """
                    SELECT 1 FROM attempts
                    WHERE run_id = ? AND node_id = ? AND attempt_id <> ?
                      AND status IN ('scheduled', 'claimed', 'waiting_approval', 'running')
                    LIMIT 1
                    """,
                    (run_id, node_id, attempt.attempt_id),
                ).fetchone()
                if competing is not None:
                    raise ProjectionConflictError(
                        "scheduled admission candidate has a competing Attempt"
                    )

            if policy_outcome == "require_approval":
                approval_request_digest = hashlib.sha256(
                    (
                        f"{run_id}\0{node_id}\0{attempt.attempt_id}\0"
                        f"{action_digest}\0{policy_digest}\0{profile_digest}"
                    ).encode("utf-8")
                ).hexdigest()
                policy_event_id = (
                    f"evt_activity_approval_{approval_request_digest}"
                )
                if conn.execute(
                    "SELECT 1 FROM domain_events WHERE event_id = ?",
                    (policy_event_id,),
                ).fetchone() is not None:
                    raise ProjectionConflictError(
                        "approval request identity was already consumed"
                    )
                policy_event = self._commit_event_tx(
                    conn,
                    run,
                    "approval.requested",
                    payload={
                        "kind": "activity_approval_request",
                        "outcome": policy_outcome,
                        "reason_code": reason_code,
                        "action_digest": action_digest,
                        "policy_digest": policy_digest,
                        "profile_digest": profile_digest,
                        "decision_digest": decision_digest,
                        "approval_grant_digest": None,
                        "approval_request_digest": (
                            approval_request_digest
                        ),
                    },
                    event_id=policy_event_id,
                    occurred_at=current_time,
                    node_id=node_id,
                    attempt_id=attempt.attempt_id,
                    run_projection=replace(
                        run,
                        status=RunStatus.WAITING_APPROVAL,
                    ),
                    node_projection=replace(
                        node,
                        status=NodeStatus.WAITING_APPROVAL,
                    ),
                    attempt_projection=replace(
                        attempt,
                        status=AttemptStatus.WAITING_APPROVAL,
                        worker_id=None,
                        lease_id=None,
                    ),
                )
                return None, None, policy_event

            self._admit_activity_tx(
                conn,
                attempt,
                owner_id=owner_id,
                max_active_attempts=max_active_attempts,
                worker_capacity=worker_capacity,
            )
            schedule_deadline = _attempt_deadline(run, attempt)
            if (
                schedule_deadline is not None
                and current_time >= schedule_deadline[1]
            ):
                raise IdempotencyConflictError(
                    f"{schedule_deadline[0]} deadline prevents Activity claim"
                )
            start_timeout = _timeout_milliseconds(
                attempt,
                "start_timeout_ms",
            )
            start_deadline = (
                current_time + start_timeout / 1_000
                if start_timeout is not None
                else None
            )
            heartbeat_timeout = _timeout_milliseconds(
                attempt,
                "heartbeat_timeout_ms",
            )
            heartbeat_deadline = (
                current_time + heartbeat_timeout / 1_000
                if heartbeat_timeout is not None
                else None
            )
            lease_deadline = _earliest_deadline(
                _run_deadline(run),
                start_deadline,
                heartbeat_deadline,
            )
            claim = self._claim_idempotency_tx(
                conn,
                run_id,
                attempt.idempotency_key,
                request_hash,
                owner_id,
                lease_seconds=lease_duration,
                lease_deadline_at=lease_deadline,
                now=current_time,
                minimum_fencing_token=attempt.attempt_number,
            )
            if claim.disposition is not ClaimDisposition.ACQUIRED:
                raise ProjectionConflictError(
                    "admission candidate lost the claim CAS"
                )
            claimed_metadata = dict(attempt.metadata)
            if start_deadline is not None:
                claimed_metadata["start_deadline_at"] = min(
                    start_deadline,
                    _run_deadline(run)
                    if _run_deadline(run) is not None
                    else start_deadline,
                )
            claimed_attempt = replace(
                attempt,
                status=AttemptStatus.CLAIMED,
                worker_id=owner_id,
                lease_id=claim.record.claim_token,
                fencing_token=claim.record.claim_count,
                metadata=claimed_metadata,
            )
            claimed_event = self._commit_event_tx(
                conn,
                run,
                "attempt.claimed",
                payload={
                    "fencing_token": claim.record.claim_count,
                    "effect_class": attempt.effect_class,
                },
                event_id=(
                    f"evt_claim_{attempt.attempt_id}_{claim.record.claim_count}"
                ),
                occurred_at=current_time,
                node_id=node_id,
                attempt_id=attempt.attempt_id,
                run_projection=run,
                node_projection=None,
                attempt_projection=claimed_attempt,
            )

            claim_token_digest = hashlib.sha256(
                claim.record.claim_token.encode("utf-8")
            ).hexdigest()
            binding_digest = hashlib.sha256(
                (
                    f"{run_id}\0{node_id}\0{attempt.attempt_id}\0"
                    f"{claim.record.claim_token}"
                ).encode("utf-8")
            ).hexdigest()
            policy_event_id = (
                f"evt_policy_approval_{approval_grant_digest}"
                if approval_grant_digest is not None
                else f"evt_policy_{binding_digest}"
            )
            if conn.execute(
                "SELECT 1 FROM domain_events WHERE event_id = ?",
                (policy_event_id,),
            ).fetchone() is not None:
                raise ProjectionConflictError(
                    "policy authorization identity was already consumed"
                )
            if approval_grant_digest is not None:
                issue_row = conn.execute(
                    "SELECT * FROM domain_events WHERE event_id = ?",
                    (f"evt_approval_issue_{approval_grant_digest}",),
                ).fetchone()
                if issue_row is None:
                    raise IdempotencyConflictError(
                        "approval grant was not issued"
                    )
                issue = self._event_from_row(issue_row)
                issue_payload = dict(issue.payload)
                issue_payload.pop("projection", None)
                if (
                    issue.run_id != run_id
                    or issue_payload.get("kind") != "grant_issued"
                    or issue_payload.get("grant_binding_digest")
                    != approval_grant_digest
                    or issue_payload.get("action_digest") != action_digest
                    or issue_payload.get("policy_digest") != policy_digest
                    or not isinstance(
                        issue_payload.get("expires_at"),
                        (int, float),
                    )
                    or float(issue_payload["expires_at"]) <= current_time
                ):
                    raise IdempotencyConflictError(
                        "approval grant does not match or is expired"
                    )
            run_row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            assert run_row is not None
            claimed_run = self._run_from_row(run_row)
            policy_payload: dict[str, JsonValue] = {
                "kind": "activity_authorization",
                "outcome": policy_outcome,
                "reason_code": reason_code,
                "action_digest": action_digest,
                "policy_digest": policy_digest,
                "profile_digest": profile_digest,
                "decision_digest": decision_digest,
                "claim_token_digest": claim_token_digest,
                "approval_grant_digest": approval_grant_digest,
            }
            policy_event = self._commit_event_tx(
                conn,
                claimed_run,
                "policy.decided",
                payload=policy_payload,
                event_id=policy_event_id,
                occurred_at=current_time,
                node_id=node_id,
                attempt_id=attempt.attempt_id,
                run_projection=claimed_run,
                node_projection=None,
                attempt_projection=None,
            )
            return claim, claimed_event, policy_event

    @_audit_stale_activity_rejection
    def complete_activity(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        request_hash: str,
        owner_id: str,
        *,
        claim_token: str,
        result: JsonValue,
        event_payload: dict[str, JsonValue] | None = None,
        attempt_status: AttemptStatus = AttemptStatus.SUCCEEDED,
        node_status: NodeStatus = NodeStatus.SUCCEEDED,
        run_status: RunStatus | None = None,
        now: float | None = None,
    ) -> tuple[IdempotencyRecord, EventRecord]:
        """Atomically persist a receipt, terminal Attempt, Node/Run, and Event."""

        now = _finite_timestamp(now if now is not None else utc_timestamp(), "now")
        attempt_status = AttemptStatus(attempt_status)
        node_status = NodeStatus(node_status)
        if not attempt_status.is_terminal:
            raise InvalidStateTransition("complete_activity requires a terminal Attempt status")
        result_json = _json_dump(result)
        event_id = _claim_bound_event_id("complete", attempt_id, claim_token)
        with self._write_transaction() as conn:
            run, node, attempt = self._load_activity_tx(conn, run_id, node_id, attempt_id)
            record = self._get_idempotency_tx(conn, run_id, attempt.idempotency_key)
            if record is None:
                raise IdempotencyConflictError("activity idempotency key was not claimed")
            self._validate_claim_owner(
                record,
                request_hash=request_hash,
                owner_id=owner_id,
                claim_token=claim_token,
            )
            if record.status is IdempotencyStatus.COMPLETED:
                if _json_dump(record.result) != result_json:
                    raise IdempotencyConflictError(
                        "activity already completed with a different result"
                    )
                row = conn.execute(
                    "SELECT * FROM domain_events WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                if row is None:
                    raise StoreSchemaError("completed activity is missing its Domain Event")
                return record, self._event_from_row(row)
            if record.lease_expires_at <= now:
                raise IdempotencyConflictError(
                    "expired Activity lease cannot submit a completion"
                )
            if attempt.status is not AttemptStatus.RUNNING:
                raise InvalidStateTransition(
                    f"activity completion requires RUNNING Attempt, got {attempt.status.value}"
                )
            if attempt.lease_id != claim_token or attempt.fencing_token != record.claim_count:
                raise IdempotencyConflictError("stale Activity fencing token")
            if attempt_status is AttemptStatus.OUTCOME_UNKNOWN:
                node_status = NodeStatus.WAITING_RECOVERY
                run_status = RunStatus.WAITING_RECOVERY
            self._validate_completion_statuses(
                attempt_status,
                node_status,
                run_status,
            )
            target_attempt = replace(
                attempt,
                status=attempt_status,
                result=normalize_json(result, "result"),
                finished_at=now,
            )
            target_node = replace(
                node,
                status=node_status,
                output=normalize_json(result, "result")
                if node_status is NodeStatus.SUCCEEDED
                else node.output,
            )
            target_run = replace(run, status=run_status) if run_status is not None else run
            if run_status is RunStatus.COMPLETED:
                target_run = replace(target_run, output=normalize_json(result, "result"))
            conn.execute(
                """
                UPDATE idempotency_records SET status = ?, result_json = ?,
                    updated_at = ?, completed_at = ?, lease_expires_at = ?
                WHERE run_id = ? AND key = ? AND claim_token = ?
                """,
                (
                    IdempotencyStatus.COMPLETED.value,
                    result_json,
                    now,
                    now,
                    now,
                    run_id,
                    attempt.idempotency_key,
                    claim_token,
                ),
            )
            self._fault("complete.after_idempotency")
            event_type = f"attempt.{attempt_status.value}"
            event = self._commit_event_tx(
                conn,
                run,
                event_type,
                payload=event_payload
                or {
                    "result_digest": hashlib.sha256(result_json.encode("utf-8")).hexdigest()
                },
                event_id=event_id,
                occurred_at=now,
                node_id=node_id,
                attempt_id=attempt_id,
                run_projection=target_run,
                node_projection=target_node,
                attempt_projection=target_attempt,
            )
            updated = self._get_idempotency_tx(conn, run_id, attempt.idempotency_key)
            assert updated is not None
            return updated, event

    @_audit_stale_activity_rejection
    def complete_retryable_activity(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        request_hash: str,
        owner_id: str,
        *,
        claim_token: str,
        error_class: str,
        error_code: str,
        retry_due_at: float,
        attempt_status: AttemptStatus = AttemptStatus.FAILED,
        now: float | None = None,
    ) -> tuple[IdempotencyRecord, EventRecord]:
        """Atomically close one failed Attempt and persist its retry deadline."""

        current_time = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        due_at = _finite_timestamp(retry_due_at, "retry_due_at")
        if due_at < current_time:
            raise ValueError("retry_due_at must not be earlier than now")
        safe_class = _safe_receipt_code(error_class, "error_class")
        safe_code = _safe_receipt_code(error_code, "error_code")
        status = AttemptStatus(attempt_status)
        if status not in {
            AttemptStatus.FAILED,
            AttemptStatus.TIMED_OUT,
            AttemptStatus.ABANDONED,
        }:
            raise InvalidStateTransition(
                "retryable completion requires FAILED, TIMED_OUT, or ABANDONED"
            )
        receipt: dict[str, JsonValue] = {
            "outcome": status.value,
            "error_class": safe_class,
            "error_code": safe_code,
        }
        result_json = _json_dump(receipt)
        event_id = _claim_bound_event_id("retryable", attempt_id, claim_token)
        with self._write_transaction() as conn:
            run, node, attempt = self._load_activity_tx(
                conn,
                run_id,
                node_id,
                attempt_id,
            )
            if run.status is not RunStatus.RUNNING:
                raise InvalidStateTransition(
                    "retryable completion requires a RUNNING Run"
                )
            if node.status is not NodeStatus.RUNNING:
                raise InvalidStateTransition(
                    "retryable completion requires a RUNNING Node"
                )
            if attempt.status is not AttemptStatus.RUNNING:
                raise InvalidStateTransition(
                    "retryable completion requires a RUNNING Attempt"
                )
            record = self._get_idempotency_tx(conn, run_id, attempt.idempotency_key)
            if record is None:
                raise IdempotencyConflictError("Activity has no idempotency claim")
            self._validate_claim_owner(
                record,
                request_hash=request_hash,
                owner_id=owner_id,
                claim_token=claim_token,
            )
            if (
                record.status is not IdempotencyStatus.IN_PROGRESS
                or record.lease_expires_at <= current_time
                or attempt.worker_id != owner_id
                or attempt.lease_id != claim_token
                or attempt.fencing_token != record.claim_count
            ):
                raise IdempotencyConflictError("stale Activity fencing token")
            updated = conn.execute(
                """
                UPDATE idempotency_records SET
                    status = ?, result_json = ?, updated_at = ?, completed_at = ?,
                    lease_expires_at = ?
                WHERE run_id = ? AND key = ? AND status = ?
                  AND owner_id = ? AND claim_token = ? AND claim_count = ?
                """,
                (
                    IdempotencyStatus.COMPLETED.value,
                    result_json,
                    current_time,
                    current_time,
                    current_time,
                    run_id,
                    attempt.idempotency_key,
                    IdempotencyStatus.IN_PROGRESS.value,
                    owner_id,
                    claim_token,
                    record.claim_count,
                ),
            )
            if updated.rowcount != 1:
                raise IdempotencyConflictError("Activity claim changed concurrently")
            self._fault("complete_retryable.after_idempotency")
            error = normalize_json(receipt, "retryable error")
            target_node = replace(
                node,
                status=NodeStatus.WAITING_RETRY,
                error=error,
                metadata={
                    **node.metadata,
                    "retry_due_at": due_at,
                    "last_error_class": safe_class,
                    "last_error_code": safe_code,
                },
            )
            target_attempt = replace(
                attempt,
                status=status,
                result=receipt,
                error=error,
                finished_at=current_time,
            )
            event = self._commit_event_tx(
                conn,
                run,
                f"attempt.{status.value}",
                payload={
                    "error_class": safe_class,
                    "error_code": safe_code,
                    "retry_due_at": due_at,
                },
                event_id=event_id,
                occurred_at=current_time,
                node_id=node_id,
                attempt_id=attempt_id,
                run_projection=run,
                node_projection=target_node,
                attempt_projection=target_attempt,
            )
            completed = self._get_idempotency_tx(conn, run_id, attempt.idempotency_key)
            assert completed is not None
            return completed, event

    def recover_expired_activity(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        request_hash: str,
        expected_owner_id: str,
        *,
        claim_token: str,
        fencing_token: int,
        resolution: str,
        retry_due_at: float | None = None,
        verified_result: dict[str, JsonValue] | None = None,
        expiry_reason: str = "lease",
        timeout_kind: str | None = None,
        deadline_at: float | None = None,
        now: float | None = None,
    ) -> tuple[IdempotencyRecord, EventRecord]:
        """Atomically fence and resolve one durably expired Activity lease.

        This API makes no external-effect decision.  The recovery reaper must
        choose a conservative resolution before entering this short SQLite
        transaction.
        """

        current_time = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        allowed_resolutions = {
            "abandon_ready",
            "abandon_retry",
            "abandon_failed",
            "timeout_retry",
            "timeout_failed",
            "verified_succeeded",
            "waiting_recovery",
        }
        if resolution not in allowed_resolutions:
            raise ValueError("invalid expired Activity resolution")
        try:
            expected_fencing = int(fencing_token)
        except (TypeError, ValueError) as exc:
            raise IdempotencyConflictError("invalid Activity fencing token") from exc
        due_at = (
            None
            if retry_due_at is None
            else _finite_timestamp(retry_due_at, "retry_due_at")
        )
        retry_resolutions = {"abandon_retry", "timeout_retry"}
        if resolution in retry_resolutions and due_at is None:
            raise ValueError(f"{resolution} requires retry_due_at")
        if resolution not in retry_resolutions and due_at is not None:
            raise ValueError("retry_due_at is only valid for a retry resolution")
        if expiry_reason not in {"lease", "deadline"}:
            raise ValueError("expiry_reason must be lease or deadline")
        if expiry_reason == "deadline":
            if timeout_kind not in _DEADLINE_KINDS:
                raise ValueError("invalid timeout_kind")
            if deadline_at is None:
                raise ValueError("deadline recovery requires deadline_at")
            deadline_at = _finite_timestamp(deadline_at, "deadline_at")
            if deadline_at > current_time:
                raise IdempotencyConflictError("Activity deadline has not elapsed")
            if resolution.startswith("abandon_"):
                raise ValueError("deadline recovery requires a timeout resolution")
        elif timeout_kind is not None or deadline_at is not None:
            raise ValueError("timeout metadata is only valid for deadline recovery")
        verified_receipt = (
            _verified_probe_receipt(verified_result)
            if verified_result is not None
            else None
        )
        if resolution == "verified_succeeded" and verified_receipt is None:
            raise ValueError("verified_succeeded requires a verified receipt")
        if resolution != "verified_succeeded" and verified_receipt is not None:
            raise ValueError("verified_result is only valid for verified_succeeded")

        with self._write_transaction() as conn:
            run, node, attempt = self._load_activity_tx(
                conn,
                run_id,
                node_id,
                attempt_id,
            )
            record = self._get_idempotency_tx(conn, run_id, attempt.idempotency_key)
            if record is None:
                raise IdempotencyConflictError("Activity has no idempotency lease")
            self._validate_claim_owner(
                record,
                request_hash=request_hash,
                owner_id=expected_owner_id,
                claim_token=claim_token,
            )
            if attempt.status not in {AttemptStatus.CLAIMED, AttemptStatus.RUNNING}:
                raise InvalidStateTransition("expired Activity is no longer active")
            if (
                record.status is not IdempotencyStatus.IN_PROGRESS
                or record.claim_count != expected_fencing
                or attempt.fencing_token != expected_fencing
                or attempt.worker_id != expected_owner_id
                or attempt.lease_id != claim_token
            ):
                raise IdempotencyConflictError("stale expired Activity lease")
            if expiry_reason == "lease":
                if record.lease_expires_at > current_time:
                    raise IdempotencyConflictError("Activity lease was renewed")
            else:
                persisted_deadline = _attempt_deadline(
                    run,
                    attempt,
                    heartbeat_deadline_at=_heartbeat_deadline(
                        attempt,
                        record,
                    ),
                )
                if (
                    persisted_deadline is None
                    or persisted_deadline[0] != timeout_kind
                    or persisted_deadline[1] != deadline_at
                    or persisted_deadline[1] > current_time
                ):
                    raise IdempotencyConflictError(
                        "Activity deadline changed or has not elapsed"
                    )

            effective_resolution = resolution
            if expiry_reason == "lease" and run.status is RunStatus.CANCELLING:
                effective_resolution = "cancelled"
            if effective_resolution in {
                "abandon_ready",
                "abandon_retry",
                "abandon_failed",
            }:
                if effective_resolution == "abandon_ready" and (
                    attempt.status is not AttemptStatus.CLAIMED
                    or node.status is not NodeStatus.READY
                ):
                    raise InvalidStateTransition(
                        "only a pre-start CLAIMED Activity can return directly to READY"
                    )
                target_attempt_status = AttemptStatus.ABANDONED
                target_node_status = {
                    "abandon_ready": NodeStatus.READY,
                    "abandon_retry": NodeStatus.WAITING_RETRY,
                    "abandon_failed": NodeStatus.FAILED,
                }[effective_resolution]
                target_run_status = run.status
                receipt: dict[str, JsonValue] = {
                    "outcome": AttemptStatus.ABANDONED.value,
                    "error_class": "lease_expired",
                    "error_code": (
                        "attempts_exhausted"
                        if effective_resolution == "abandon_failed"
                        else "worker_lease_expired"
                    ),
                }
            elif effective_resolution in {"timeout_retry", "timeout_failed"}:
                target_attempt_status = AttemptStatus.TIMED_OUT
                target_node_status = (
                    NodeStatus.WAITING_RETRY
                    if effective_resolution == "timeout_retry"
                    else NodeStatus.FAILED
                )
                target_run_status = run.status
                receipt = {
                    "outcome": AttemptStatus.TIMED_OUT.value,
                    "error_class": "timeout",
                    "error_code": f"{timeout_kind}_timeout",
                }
            elif effective_resolution == "verified_succeeded":
                if attempt.status is not AttemptStatus.RUNNING:
                    raise InvalidStateTransition(
                        "only a started Activity can be recovered as succeeded"
                    )
                target_attempt_status = AttemptStatus.SUCCEEDED
                target_node_status = NodeStatus.SUCCEEDED
                target_run_status = run.status
                assert verified_receipt is not None
                receipt = verified_receipt
            elif effective_resolution == "waiting_recovery":
                if attempt.status is not AttemptStatus.RUNNING:
                    raise InvalidStateTransition(
                        "only a started Activity can have an unknown outcome"
                    )
                target_attempt_status = AttemptStatus.OUTCOME_UNKNOWN
                target_node_status = NodeStatus.WAITING_RECOVERY
                target_run_status = RunStatus.WAITING_RECOVERY
                receipt = {
                    "outcome": AttemptStatus.OUTCOME_UNKNOWN.value,
                    "error_class": (
                        "timeout"
                        if expiry_reason == "deadline"
                        else "lease_expired"
                    ),
                    "error_code": "external_outcome_unknown",
                }
            else:
                target_attempt_status = AttemptStatus.CANCELLED
                target_node_status = NodeStatus.CANCELLED
                target_run_status = run.status
                receipt = {
                    "outcome": AttemptStatus.CANCELLED.value,
                    "error_class": "cancellation",
                    "error_code": "run_cancelling",
                }

            next_fencing = record.claim_count + 1
            recovery_actor = (
                "deadline-scanner"
                if expiry_reason == "deadline"
                else "recovery-reaper"
            )
            recovery_token = f"recovery:{attempt.attempt_id}:{next_fencing}"
            result_json = _json_dump(receipt)
            error = (
                normalize_json(receipt, "recovery error")
                if target_attempt_status is not AttemptStatus.SUCCEEDED
                else attempt.error
            )
            target_attempt = replace(
                attempt,
                status=target_attempt_status,
                worker_id=recovery_actor,
                lease_id=recovery_token,
                fencing_token=next_fencing,
                result=receipt,
                error=error,
                finished_at=current_time,
            )
            node_metadata = dict(node.metadata)
            if effective_resolution in retry_resolutions:
                assert due_at is not None
                node_metadata.update(
                    {
                        "retry_due_at": due_at,
                        "last_error_class": (
                            "timeout"
                            if expiry_reason == "deadline"
                            else "lease_expired"
                        ),
                        "last_error_code": (
                            f"{timeout_kind}_timeout"
                            if expiry_reason == "deadline"
                            else "worker_lease_expired"
                        ),
                    }
                )
            target_node = replace(
                node,
                status=target_node_status,
                metadata=node_metadata,
                output=(
                    receipt
                    if target_node_status is NodeStatus.SUCCEEDED
                    else node.output
                ),
                error=(
                    error
                    if target_node_status
                    in {
                        NodeStatus.WAITING_RETRY,
                        NodeStatus.WAITING_RECOVERY,
                        NodeStatus.FAILED,
                    }
                    else node.error
                ),
            )
            target_run = (
                replace(run, status=target_run_status)
                if target_run_status is not run.status
                else run
            )
            event_type = f"attempt.{target_attempt_status.value}"
            event = self._commit_event_tx(
                conn,
                run,
                event_type,
                payload={
                    "reason": (
                        "deadline_expired"
                        if expiry_reason == "deadline"
                        else "lease_expired"
                    ),
                    "resolution": effective_resolution,
                    "expired_fencing_token": expected_fencing,
                    "recovery_fencing_token": next_fencing,
                    **(
                        {
                            "timeout_kind": timeout_kind,
                            "deadline_at": deadline_at,
                        }
                        if expiry_reason == "deadline"
                        else {}
                    ),
                    **(
                        {"retry_due_at": due_at}
                        if effective_resolution in retry_resolutions
                        else {}
                    ),
                },
                event_id=(
                    (
                        f"evt_deadline_{attempt_id}_{expected_fencing}_{timeout_kind}"
                        if expiry_reason == "deadline"
                        else f"evt_lease_recovery_{attempt_id}_{expected_fencing}"
                    )
                ),
                occurred_at=current_time,
                node_id=node_id,
                attempt_id=attempt_id,
                run_projection=target_run,
                node_projection=target_node,
                attempt_projection=target_attempt,
                allow_claimed_recovery_transition=(
                    expiry_reason == "lease"
                    and
                    attempt.status is AttemptStatus.CLAIMED
                    and node.status is NodeStatus.READY
                    and target_node_status
                    in {NodeStatus.WAITING_RETRY, NodeStatus.FAILED}
                ),
                allow_deadline_transition=(
                    expiry_reason == "deadline"
                    and target_attempt_status is AttemptStatus.TIMED_OUT
                ),
            )
            lease_guard = (
                " AND lease_expires_at <= ?"
                if expiry_reason == "lease"
                else ""
            )
            update_parameters: list[Any] = [
                IdempotencyStatus.COMPLETED.value,
                recovery_actor,
                recovery_token,
                current_time,
                result_json,
                next_fencing,
                current_time,
                current_time,
                run_id,
                attempt.idempotency_key,
                IdempotencyStatus.IN_PROGRESS.value,
                expected_owner_id,
                claim_token,
                expected_fencing,
            ]
            if expiry_reason == "lease":
                update_parameters.append(current_time)
            update = conn.execute(
                """
                UPDATE idempotency_records SET
                    status = ?, owner_id = ?, claim_token = ?,
                    lease_expires_at = ?, result_json = ?,
                    claim_count = ?, updated_at = ?, completed_at = ?
                WHERE run_id = ? AND key = ? AND status = ?
                  AND owner_id = ? AND claim_token = ? AND claim_count = ?
                """
                + lease_guard,
                update_parameters,
            )
            if update.rowcount != 1:
                raise IdempotencyConflictError(
                    "expired Activity changed concurrently"
                )
            self._fault("lease_recovery.after_idempotency")
            completed = self._get_idempotency_tx(conn, run_id, attempt.idempotency_key)
            assert completed is not None
            return completed, event

    def timeout_scheduled_activity(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        *,
        timeout_kind: str,
        deadline_at: float,
        retry_due_at: float | None = None,
        now: float | None = None,
    ) -> EventRecord:
        """Atomically time out work that never acquired an execution claim."""

        if timeout_kind not in _DEADLINE_KINDS:
            raise ValueError("invalid timeout_kind")
        current_time = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        persisted_deadline_at = _finite_timestamp(deadline_at, "deadline_at")
        if persisted_deadline_at > current_time:
            raise IdempotencyConflictError("Activity deadline has not elapsed")
        due_at = (
            None
            if retry_due_at is None
            else _finite_timestamp(retry_due_at, "retry_due_at")
        )
        with self._write_transaction() as conn:
            run, node, attempt = self._load_activity_tx(
                conn,
                run_id,
                node_id,
                attempt_id,
            )
            if (
                attempt.status is not AttemptStatus.SCHEDULED
                or node.status is not NodeStatus.READY
            ):
                raise InvalidStateTransition(
                    "scheduled timeout requires a READY/SCHEDULED Activity"
                )
            deadline = _attempt_deadline(run, attempt)
            if (
                deadline is None
                or deadline[0] != timeout_kind
                or deadline[1] != persisted_deadline_at
                or deadline[1] > current_time
            ):
                raise IdempotencyConflictError(
                    "Activity deadline changed or has not elapsed"
                )
            retrying = due_at is not None
            receipt: dict[str, JsonValue] = {
                "outcome": AttemptStatus.TIMED_OUT.value,
                "error_class": "timeout",
                "error_code": f"{timeout_kind}_timeout",
            }
            node_metadata = dict(node.metadata)
            if retrying:
                node_metadata.update(
                    {
                        "retry_due_at": due_at,
                        "last_error_class": "timeout",
                        "last_error_code": f"{timeout_kind}_timeout",
                    }
                )
            target_node = replace(
                node,
                status=(
                    NodeStatus.WAITING_RETRY
                    if retrying
                    else NodeStatus.FAILED
                ),
                metadata=node_metadata,
                error=receipt,
            )
            target_attempt = replace(
                attempt,
                status=AttemptStatus.TIMED_OUT,
                result=receipt,
                error=receipt,
                finished_at=current_time,
            )
            return self._commit_event_tx(
                conn,
                run,
                "attempt.timed_out",
                payload={
                    "reason": "deadline_expired",
                    "resolution": (
                        "timeout_retry" if retrying else "timeout_failed"
                    ),
                    "timeout_kind": timeout_kind,
                    "deadline_at": persisted_deadline_at,
                    **({"retry_due_at": due_at} if retrying else {}),
                },
                event_id=(
                    f"evt_deadline_{attempt_id}_scheduled_{timeout_kind}"
                ),
                occurred_at=current_time,
                node_id=node_id,
                attempt_id=attempt_id,
                run_projection=run,
                node_projection=target_node,
                attempt_projection=target_attempt,
                allow_deadline_transition=True,
            )

    def mark_run_deadline_cancelling(
        self,
        run_id: str,
        *,
        deadline_at: float,
        now: float | None = None,
    ) -> EventRecord | None:
        """Persist deadline cancellation intent without claiming effect outcome."""

        current_time = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        persisted_deadline_at = _finite_timestamp(deadline_at, "deadline_at")
        if persisted_deadline_at > current_time:
            raise IdempotencyConflictError("Run deadline has not elapsed")
        with self._write_transaction() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise RunNotFoundError(run_id)
            run = self._run_from_row(row)
            actual_deadline = _run_deadline(run)
            if (
                actual_deadline != persisted_deadline_at
                or actual_deadline > current_time
            ):
                raise ProjectionConflictError(
                    "Run deadline changed or has not elapsed"
                )
            if (
                run.status.is_terminal
                or run.status
                in {RunStatus.CANCELLING, RunStatus.WAITING_RECOVERY}
            ):
                return None
            target = replace(run, status=RunStatus.CANCELLING)
            return self._commit_event_tx(
                conn,
                run,
                "run.cancelling",
                payload={
                    "intent": "deadline",
                    "timeout_kind": "run",
                    "deadline_at": persisted_deadline_at,
                },
                event_id=(
                    "evt_run_deadline_"
                    + hashlib.sha256(run_id.encode("utf-8")).hexdigest()
                ),
                occurred_at=current_time,
                node_id=None,
                attempt_id=None,
                run_projection=target,
                node_projection=None,
                attempt_projection=None,
            )

    def pause_activity(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        request_hash: str,
        owner_id: str,
        *,
        claim_token: str,
        now: float | None = None,
    ) -> tuple[IdempotencyRecord, EventRecord]:
        """Stop one Activity at a caller-confirmed safe pause boundary."""

        return self.complete_activity(
            run_id,
            node_id,
            attempt_id,
            request_hash,
            owner_id,
            claim_token=claim_token,
            result={"outcome": "cancelled", "reason": "pause"},
            event_payload={"reason": "pause"},
            attempt_status=AttemptStatus.CANCELLED,
            node_status=NodeStatus.PAUSED,
            run_status=None,
            now=now,
        )

    @_audit_stale_activity_rejection
    def start_activity(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        owner_id: str,
        *,
        claim_token: str,
        now: float | None = None,
    ) -> EventRecord:
        """Cross the side-effect boundary in a durable CLAIMED -> RUNNING Tx."""

        now = _finite_timestamp(now if now is not None else utc_timestamp(), "now")
        with self._write_transaction() as conn:
            run, node, attempt = self._load_activity_tx(conn, run_id, node_id, attempt_id)
            record = self._get_idempotency_tx(conn, run_id, attempt.idempotency_key)
            if record is None:
                raise IdempotencyConflictError("Activity has no idempotency claim")
            self._validate_claim_owner(
                record,
                request_hash=record.request_hash,
                owner_id=owner_id,
                claim_token=claim_token,
            )
            if (
                record.status is not IdempotencyStatus.IN_PROGRESS
                or record.lease_expires_at <= now
            ):
                raise IdempotencyConflictError(
                    "expired Activity lease cannot cross the start gate"
                )
            if attempt.status is not AttemptStatus.CLAIMED:
                raise InvalidStateTransition("start_activity requires a CLAIMED Attempt")
            if node.status is not NodeStatus.READY:
                raise InvalidStateTransition("start_activity requires a READY Node")
            if attempt.lease_id != claim_token or attempt.fencing_token != record.claim_count:
                raise IdempotencyConflictError("stale Activity fencing token")
            start_deadline = _attempt_deadline(run, attempt)
            if start_deadline is not None and now >= start_deadline[1]:
                raise IdempotencyConflictError(
                    f"{start_deadline[0]} deadline prevents Activity start"
                )
            execution_timeout = _timeout_milliseconds(
                attempt,
                "execution_timeout_ms",
            )
            execution_deadline = (
                now + execution_timeout / 1_000
                if execution_timeout is not None
                else None
            )
            heartbeat_timeout = _timeout_milliseconds(
                attempt,
                "heartbeat_timeout_ms",
            )
            heartbeat_deadline = (
                now + heartbeat_timeout / 1_000
                if heartbeat_timeout is not None
                else None
            )
            run_deadline = _run_deadline(run)
            execution_cap = _earliest_deadline(
                run_deadline,
                execution_deadline,
            )
            lease_cap = _earliest_deadline(
                execution_cap,
                heartbeat_deadline,
            )
            new_lease_expiry = (
                min(
                    heartbeat_deadline
                    if heartbeat_deadline is not None
                    else record.lease_expires_at,
                    lease_cap,
                )
                if lease_cap is not None
                else record.lease_expires_at
            )
            if new_lease_expiry <= now:
                raise IdempotencyConflictError(
                    "Activity deadline prevents crossing the start gate"
                )
            lease_update = conn.execute(
                """
                UPDATE idempotency_records
                SET lease_expires_at = ?, updated_at = ?
                WHERE run_id = ? AND key = ? AND status = ?
                  AND owner_id = ? AND claim_token = ? AND claim_count = ?
                  AND lease_expires_at > ?
                """,
                (
                    new_lease_expiry,
                    now,
                    run_id,
                    attempt.idempotency_key,
                    IdempotencyStatus.IN_PROGRESS.value,
                    owner_id,
                    claim_token,
                    record.claim_count,
                    now,
                ),
            )
            if lease_update.rowcount != 1:
                raise IdempotencyConflictError(
                    "Activity lease changed before start"
                )
            started_metadata = dict(attempt.metadata)
            if execution_cap is not None:
                started_metadata["execution_deadline_at"] = execution_cap
            target_run = (
                replace(run, status=RunStatus.RUNNING)
                if run.status is RunStatus.CREATED
                else run
            )
            return self._commit_event_tx(
                conn,
                run,
                "attempt.started",
                payload={
                    "fencing_token": record.claim_count,
                    "effect_class": attempt.effect_class,
                },
                event_id=_claim_bound_event_id("start", attempt_id, claim_token),
                occurred_at=now,
                node_id=node_id,
                attempt_id=attempt_id,
                run_projection=target_run,
                node_projection=replace(node, status=NodeStatus.RUNNING),
                attempt_projection=replace(
                    attempt,
                    status=AttemptStatus.RUNNING,
                    metadata=started_metadata,
                    started_at=now,
                ),
            )

    @staticmethod
    def _fault(_stage: str) -> None:
        """Fault-injection seam used by transaction rollback tests."""

    def _load_activity_tx(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        node_id: str,
        attempt_id: str,
    ) -> tuple[RunRecord, NodeRecord, AttemptRecord]:
        run_row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        node_row = conn.execute(
            "SELECT * FROM node_runs WHERE run_id = ? AND node_id = ?",
            (run_id, node_id),
        ).fetchone()
        attempt_row = conn.execute(
            "SELECT * FROM attempts WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchone()
        if run_row is None:
            raise RunNotFoundError(run_id)
        if node_row is None or attempt_row is None:
            raise ProjectionConflictError("Activity Node or Attempt does not exist")
        run = self._run_from_row(run_row)
        node = self._node_from_row(node_row)
        attempt = self._attempt_from_row(attempt_row)
        if attempt.run_id != run_id or attempt.node_id != node_id:
            raise ProjectionConflictError("Attempt belongs to another Run or Node")
        return run, node, attempt

    def _record_activity_commit_rejection(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        *,
        owner_id: str,
        claim_token: str,
        fencing_token: Any = None,
        now: Any = None,
    ) -> EventRecord | None:
        """Append one inert, idempotent audit Event for a proven stale claim.

        The caller invokes this only after its original write transaction has
        exited and rolled back.  Untrusted claim bindings are compared in
        memory, never copied or digested into durable storage.
        """

        observed_at = _finite_timestamp(
            utc_timestamp() if now is None else now,
            "now",
        )
        with closing(self._connect()) as conn:
            run_row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            node_row = conn.execute(
                "SELECT 1 FROM node_runs WHERE run_id = ? AND node_id = ?",
                (run_id, node_id),
            ).fetchone()
            attempt_row = conn.execute(
                """
                SELECT * FROM attempts
                WHERE attempt_id = ? AND run_id = ? AND node_id = ?
                """,
                (attempt_id, run_id, node_id),
            ).fetchone()
            if run_row is None or node_row is None or attempt_row is None:
                return None
            attempt = self._attempt_from_row(attempt_row)
            record = self._get_idempotency_tx(
                conn,
                run_id,
                attempt.idempotency_key,
            )
        if record is None or record.claim_count != attempt.fencing_token:
            return None

        reason_code: str | None = None
        if (
            claim_token != record.claim_token
            or claim_token != attempt.lease_id
        ):
            reason_code = "claim_token_mismatch"
        elif fencing_token is not None:
            try:
                supplied_fencing = int(fencing_token)
            except (TypeError, ValueError):
                supplied_fencing = -1
            if (
                supplied_fencing != record.claim_count
                or supplied_fencing != attempt.fencing_token
            ):
                reason_code = "fencing_token_mismatch"
        if reason_code is None and (
            owner_id != record.owner_id
            or owner_id != attempt.worker_id
        ):
            reason_code = "claim_owner_mismatch"
        if reason_code is None and (
            record.status is IdempotencyStatus.COMPLETED
            or attempt.status.is_terminal
        ):
            reason_code = "claim_terminal"
        if (
            reason_code is None
            and record.status is IdempotencyStatus.IN_PROGRESS
            and record.lease_expires_at <= observed_at
        ):
            reason_code = "claim_expired"
        if reason_code is None:
            return None

        binding = hashlib.sha256(
            f"{attempt_id}\0{reason_code}".encode("utf-8")
        ).hexdigest()
        event_id = f"evt_activity_commit_rejected_{binding}"
        payload: dict[str, JsonValue] = {
            "reason_code": reason_code,
            "current_fencing_token": record.claim_count,
            "attempt_terminal": attempt.status.is_terminal,
            "claim_terminal": record.status is IdempotencyStatus.COMPLETED,
        }

        def existing_event() -> EventRecord | None:
            with closing(self._connect()) as connection:
                row = connection.execute(
                    "SELECT * FROM domain_events WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
            if row is None:
                return None
            existing = self._event_from_row(row)
            existing_payload = dict(existing.payload)
            existing_payload.pop("projection", None)
            if (
                existing.run_id != run_id
                or existing.node_id != node_id
                or existing.attempt_id != attempt_id
                or existing.event_type != "activity.commit_rejected"
                or set(existing_payload)
                != {
                    "reason_code",
                    "current_fencing_token",
                    "attempt_terminal",
                    "claim_terminal",
                }
                or existing_payload.get("reason_code") != reason_code
                or isinstance(
                    existing_payload.get("current_fencing_token"),
                    bool,
                )
                or not isinstance(
                    existing_payload.get("current_fencing_token"),
                    int,
                )
                or int(existing_payload["current_fencing_token"]) < 1
                or not isinstance(existing_payload.get("attempt_terminal"), bool)
                or not isinstance(existing_payload.get("claim_terminal"), bool)
            ):
                raise ProjectionConflictError(
                    "activity rejection audit identity collision"
                )
            return existing

        existing = existing_event()
        if existing is not None:
            return existing
        try:
            return self.append_event(
                run_id,
                "activity.commit_rejected",
                payload=payload,
                node_id=node_id,
                attempt_id=attempt_id,
                event_id=event_id,
                occurred_at=observed_at,
            )
        except ProjectionConflictError:
            # A concurrent first rejection may have observed an adjacent
            # fencing generation.  First-write-wins for this bounded audit
            # identity; execution state never depends on its snapshot fields.
            existing = existing_event()
            if existing is not None:
                return existing
            raise

    def _claim_idempotency_tx(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        key: str,
        request_hash: str,
        owner_id: str,
        *,
        lease_seconds: float,
        now: float,
        minimum_fencing_token: int = 1,
        lease_deadline_at: float | None = None,
    ) -> IdempotencyClaim:
        if (
            isinstance(minimum_fencing_token, bool)
            or not isinstance(minimum_fencing_token, int)
            or minimum_fencing_token < 1
        ):
            raise ValueError("minimum_fencing_token must be a positive integer")
        record = self._get_idempotency_tx(conn, run_id, key)
        if record is not None:
            if record.request_hash != request_hash:
                return IdempotencyClaim(ClaimDisposition.CONFLICT, record)
            if record.status is IdempotencyStatus.COMPLETED:
                return IdempotencyClaim(ClaimDisposition.COMPLETED, record)
            if record.lease_expires_at > now:
                return IdempotencyClaim(ClaimDisposition.CONFLICT, record)
            claim_count = max(record.claim_count + 1, minimum_fencing_token)
            created_at = record.created_at
        else:
            claim_count = minimum_fencing_token
            created_at = now
        claim_token = new_id("claim")
        expires_at = now + _lease_duration(lease_seconds)
        if lease_deadline_at is not None:
            lease_deadline_at = _finite_timestamp(
                lease_deadline_at,
                "lease_deadline_at",
            )
            if now >= lease_deadline_at:
                raise IdempotencyConflictError(
                    "Activity deadline prevents idempotency claim"
                )
            expires_at = min(expires_at, lease_deadline_at)
        conn.execute(
            """
            INSERT INTO idempotency_records(
                run_id, key, request_hash, schema_version, status, owner_id, claim_token,
                lease_expires_at, result_json, claim_count,
                created_at, updated_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'null', ?, ?, ?, NULL)
            ON CONFLICT(run_id, key) DO UPDATE SET
                schema_version=excluded.schema_version,
                request_hash=excluded.request_hash,
                status=excluded.status,
                owner_id=excluded.owner_id,
                claim_token=excluded.claim_token,
                lease_expires_at=excluded.lease_expires_at,
                claim_count=excluded.claim_count,
                updated_at=excluded.updated_at,
                completed_at=NULL
            """,
            (
                run_id,
                key,
                request_hash,
                MODEL_SCHEMA_VERSION,
                IdempotencyStatus.IN_PROGRESS.value,
                owner_id,
                claim_token,
                expires_at,
                claim_count,
                created_at,
                now,
            ),
        )
        claimed = self._get_idempotency_tx(conn, run_id, key)
        assert claimed is not None
        return IdempotencyClaim(ClaimDisposition.ACQUIRED, claimed)

    def _get_idempotency_tx(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        key: str,
    ) -> IdempotencyRecord | None:
        row = conn.execute(
            "SELECT * FROM idempotency_records WHERE run_id = ? AND key = ?",
            (run_id, key),
        ).fetchone()
        return self._idempotency_from_row(row) if row is not None else None

    @staticmethod
    def _validate_claim_owner(
        record: IdempotencyRecord,
        *,
        request_hash: str,
        owner_id: str,
        claim_token: str,
    ) -> None:
        if record.request_hash != request_hash:
            raise IdempotencyConflictError("activity request hash mismatch")
        if record.owner_id != owner_id or record.claim_token != claim_token:
            raise IdempotencyConflictError("stale or foreign Activity claim")

    @staticmethod
    def _validate_completion_statuses(
        attempt_status: AttemptStatus,
        node_status: NodeStatus,
        run_status: RunStatus | None,
    ) -> None:
        allowed_nodes = {
            AttemptStatus.SUCCEEDED: {NodeStatus.SUCCEEDED},
            AttemptStatus.CANCELLED: {NodeStatus.CANCELLED, NodeStatus.PAUSED},
            AttemptStatus.FAILED: {NodeStatus.FAILED},
            AttemptStatus.TIMED_OUT: {NodeStatus.FAILED},
            AttemptStatus.ABANDONED: {NodeStatus.FAILED},
            AttemptStatus.OUTCOME_UNKNOWN: {NodeStatus.WAITING_RECOVERY},
        }
        if node_status not in allowed_nodes.get(attempt_status, set()):
            raise InvalidStateTransition(
                f"{attempt_status.value} Attempt is incompatible with "
                f"{node_status.value} Node"
            )
        allowed_runs = {
            AttemptStatus.SUCCEEDED: {None, RunStatus.COMPLETED},
            AttemptStatus.CANCELLED: {None, RunStatus.CANCELLED},
            AttemptStatus.FAILED: {None, RunStatus.FAILED},
            AttemptStatus.TIMED_OUT: {None, RunStatus.FAILED},
            AttemptStatus.ABANDONED: {None, RunStatus.FAILED},
            AttemptStatus.OUTCOME_UNKNOWN: {RunStatus.WAITING_RECOVERY},
        }
        if run_status not in allowed_runs.get(attempt_status, set()):
            rendered = run_status.value if run_status is not None else "unchanged"
            raise InvalidStateTransition(
                f"{attempt_status.value} Attempt is incompatible with {rendered} Run"
            )

    def _commit_event_tx(
        self,
        conn: sqlite3.Connection,
        current_run: RunRecord,
        event_type: str,
        *,
        payload: dict[str, JsonValue],
        event_id: str,
        occurred_at: float,
        node_id: str | None,
        attempt_id: str | None,
        run_projection: RunRecord,
        node_projection: NodeRecord | None,
        attempt_projection: AttemptRecord | None,
        allow_claimed_recovery_transition: bool = False,
        allow_policy_rejection_transition: bool = False,
        allow_deadline_transition: bool = False,
    ) -> EventRecord:
        if allow_claimed_recovery_transition and (
            event_type != "attempt.abandoned"
            or attempt_projection is None
            or attempt_projection.status is not AttemptStatus.ABANDONED
            or node_projection is None
            or node_projection.status
            not in {NodeStatus.WAITING_RETRY, NodeStatus.FAILED}
        ):
            raise ProjectionConflictError(
                "claimed recovery transition is restricted to an abandoned Attempt"
            )
        if allow_policy_rejection_transition:
            if (
                event_type != "policy.decided"
                or payload.get("outcome") not in {"deny", "require_approval"}
                or node_projection is None
                or node_projection.status is not NodeStatus.FAILED
                or attempt_projection is None
                or attempt_projection.status is not AttemptStatus.FAILED
            ):
                raise ProjectionConflictError(
                    "policy rejection transition is restricted to a failed "
                    "CLAIMED Activity"
                )
            current_node_row = conn.execute(
                "SELECT * FROM node_runs WHERE run_id = ? AND node_id = ?",
                (current_run.run_id, node_projection.node_id),
            ).fetchone()
            current_attempt_row = conn.execute(
                "SELECT * FROM attempts WHERE attempt_id = ?",
                (attempt_projection.attempt_id,),
            ).fetchone()
            if current_node_row is None or current_attempt_row is None:
                raise ProjectionConflictError(
                    "policy rejection requires persisted Node and Attempt"
                )
            current_node = self._node_from_row(current_node_row)
            current_attempt = self._attempt_from_row(current_attempt_row)
            receipt = self._get_idempotency_tx(
                conn,
                current_run.run_id,
                current_attempt.idempotency_key,
            )
            if (
                current_node.status is not NodeStatus.READY
                or current_attempt.status is not AttemptStatus.CLAIMED
                or current_attempt.node_id != current_node.node_id
                or receipt is None
                or receipt.status is not IdempotencyStatus.COMPLETED
                or not isinstance(receipt.result, dict)
                or receipt.result.get("outcome") != "policy_rejected"
            ):
                raise ProjectionConflictError(
                    "policy rejection requires READY/CLAIMED state and an "
                    "atomically completed rejection receipt"
                )
        if allow_deadline_transition:
            timeout_kind = payload.get("timeout_kind")
            deadline_at = payload.get("deadline_at")
            if (
                event_type != "attempt.timed_out"
                or timeout_kind not in _DEADLINE_KINDS
                or isinstance(deadline_at, bool)
                or not isinstance(deadline_at, (int, float))
                or not math.isfinite(float(deadline_at))
                or float(deadline_at) > occurred_at
                or node_projection is None
                or node_projection.status
                not in {NodeStatus.WAITING_RETRY, NodeStatus.FAILED}
                or attempt_projection is None
                or attempt_projection.status is not AttemptStatus.TIMED_OUT
            ):
                raise ProjectionConflictError(
                    "deadline transition requires an elapsed, bounded timeout"
                )
            current_node_row = conn.execute(
                "SELECT * FROM node_runs WHERE run_id = ? AND node_id = ?",
                (current_run.run_id, node_projection.node_id),
            ).fetchone()
            current_attempt_row = conn.execute(
                "SELECT * FROM attempts WHERE attempt_id = ?",
                (attempt_projection.attempt_id,),
            ).fetchone()
            if current_node_row is None or current_attempt_row is None:
                raise ProjectionConflictError(
                    "deadline transition requires persisted Node and Attempt"
                )
            current_node = self._node_from_row(current_node_row)
            current_attempt = self._attempt_from_row(current_attempt_row)
            receipt = self._get_idempotency_tx(
                conn,
                current_run.run_id,
                current_attempt.idempotency_key,
            )
            persisted_deadline = _attempt_deadline(
                current_run,
                current_attempt,
                heartbeat_deadline_at=_heartbeat_deadline(
                    current_attempt,
                    receipt,
                ),
            )
            if (
                current_attempt.status
                not in {
                    AttemptStatus.SCHEDULED,
                    AttemptStatus.CLAIMED,
                    AttemptStatus.RUNNING,
                }
                or (
                    current_attempt.status
                    in {AttemptStatus.SCHEDULED, AttemptStatus.CLAIMED}
                    and current_node.status is not NodeStatus.READY
                )
                or (
                    current_attempt.status is AttemptStatus.RUNNING
                    and current_node.status is not NodeStatus.RUNNING
                )
                or persisted_deadline is None
                or persisted_deadline[0] != timeout_kind
                or persisted_deadline[1] != float(deadline_at)
                or persisted_deadline[1] > occurred_at
            ):
                raise ProjectionConflictError(
                    "deadline transition does not match persisted active state"
                )
        self._validate_event_projection_binding(
            event_type,
            run_projection,
            node_projection,
            attempt_projection,
        )
        if (
            attempt_projection is not None
            and attempt_projection.status is AttemptStatus.OUTCOME_UNKNOWN
            and (
                node_projection is None
                or node_projection.status is not NodeStatus.WAITING_RECOVERY
                or run_projection.status is not RunStatus.WAITING_RECOVERY
            )
        ):
            raise ProjectionConflictError(
                "OUTCOME_UNKNOWN must atomically move Node and Run to WAITING_RECOVERY"
            )
        self._validate_run_projection(current_run, run_projection, event_type)
        self._validate_terminal_aggregate(
            conn,
            run_projection,
            node_projection,
            attempt_projection,
        )
        if (
            node_projection is not None
            and node_projection.status is NodeStatus.PAUSED
            and (
                attempt_projection is None
                or attempt_projection.node_id != node_projection.node_id
                or attempt_projection.status is not AttemptStatus.CANCELLED
            )
        ):
            raise ProjectionConflictError(
                "pausing a Node must atomically cancel its active Attempt"
            )
        seq = current_run.last_event_sequence + 1
        stored_run = replace(
            run_projection,
            updated_at=max(occurred_at, current_run.updated_at),
            last_event_sequence=seq,
            projection_version=current_run.projection_version + 1,
        )
        stored_node = None
        if node_projection is not None:
            stored_node = self._write_node_projection(
                conn,
                current_run.run_id,
                node_projection,
                occurred_at,
                seq,
                allow_claimed_recovery_transition=allow_claimed_recovery_transition,
                allow_policy_rejection_transition=allow_policy_rejection_transition,
                allow_deadline_transition=allow_deadline_transition,
            )
            self._fault("event.after_node")
        stored_attempt = None
        if attempt_projection is not None:
            stored_attempt = self._write_attempt_projection(
                conn,
                current_run.run_id,
                attempt_projection,
                seq,
                allow_deadline_transition=allow_deadline_transition,
            )
            self._fault("event.after_attempt")
        update = conn.execute(
            """
            UPDATE runs SET schema_version=?, workflow_id=?, workflow_version=?,
                definition_digest=?, status=?, input_json=?, output_json=?,
                error_json=?, metadata_json=?, created_at=?, updated_at=?,
                last_event_sequence=?, projection_version=?
            WHERE run_id=? AND projection_version=?
            """,
            (
                stored_run.schema_version,
                stored_run.workflow_id,
                stored_run.workflow_version,
                stored_run.definition_digest,
                stored_run.status.value,
                _json_dump(stored_run.input),
                _json_dump(stored_run.output),
                _json_dump(stored_run.error),
                _json_dump(stored_run.metadata),
                stored_run.created_at,
                stored_run.updated_at,
                stored_run.last_event_sequence,
                stored_run.projection_version,
                stored_run.run_id,
                current_run.projection_version,
            ),
        )
        if update.rowcount != 1:
            raise ProjectionConflictError("run projection changed concurrently")
        self._fault("event.after_run")
        event_payload = dict(payload)
        event_payload["projection"] = {
            "schema_version": MODEL_SCHEMA_VERSION,
            "run": stored_run.to_dict(),
            "node": stored_node.to_dict() if stored_node else None,
            "attempt": stored_attempt.to_dict() if stored_attempt else None,
        }
        event = EventRecord(
            event_id=event_id,
            run_id=current_run.run_id,
            seq=seq,
            event_type=event_type,
            node_id=node_id,
            attempt_id=attempt_id,
            payload=event_payload,
            occurred_at=occurred_at,
        )
        _record_artifact_references_tx(
            conn,
            run_id=event.run_id,
            event_id=event.event_id,
            occurred_at=event.occurred_at,
            value=event.payload,
        )
        digest = _event_digest(
            run_id=event.run_id,
            event_type=event.event_type,
            node_id=event.node_id,
            attempt_id=event.attempt_id,
            payload=payload,
            run_projection=run_projection,
            node_projection=node_projection,
            attempt_projection=attempt_projection,
            expected_run_version=current_run.projection_version,
            expected_node_version=(
                node_projection.projection_version if node_projection is not None else None
            ),
            expected_attempt_version=(
                attempt_projection.projection_version
                if attempt_projection is not None
                else None
            ),
        )
        conn.execute(
            """
            INSERT INTO domain_events(
                event_id, run_id, seq, schema_version, event_type, node_id,
                attempt_id, payload_json, content_digest, intent_digest, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.run_id,
                event.seq,
                event.schema_version,
                event.event_type,
                event.node_id,
                event.attempt_id,
                _json_dump(event.payload),
                _content_digest(event.payload),
                digest,
                event.occurred_at,
            ),
        )
        self._fault("event.after_insert")
        return event

    def _validate_terminal_aggregate(
        self,
        conn: sqlite3.Connection,
        run: RunRecord,
        node_projection: NodeRecord | None,
        attempt_projection: AttemptRecord | None,
    ) -> None:
        if not run.status.is_terminal:
            return
        nodes = {
            row["node_id"]: self._node_from_row(row)
            for row in conn.execute(
                "SELECT * FROM node_runs WHERE run_id = ?",
                (run.run_id,),
            ).fetchall()
        }
        attempts = {
            row["attempt_id"]: self._attempt_from_row(row)
            for row in conn.execute(
                "SELECT * FROM attempts WHERE run_id = ?",
                (run.run_id,),
            ).fetchall()
        }
        if node_projection is not None:
            nodes[node_projection.node_id] = node_projection
        if attempt_projection is not None:
            attempts[attempt_projection.attempt_id] = attempt_projection
        active_attempts = [
            attempt for attempt in attempts.values() if not attempt.status.is_terminal
        ]
        if active_attempts:
            raise InvalidStateTransition(
                f"terminal Run cannot retain {len(active_attempts)} active Attempt(s)"
            )
        nonterminal_nodes = [node for node in nodes.values() if not node.status.is_terminal]
        if nonterminal_nodes:
            raise InvalidStateTransition(
                f"terminal Run cannot retain {len(nonterminal_nodes)} nonterminal Node(s)"
            )
        if run.status is RunStatus.COMPLETED and any(
            node.status not in {NodeStatus.SUCCEEDED, NodeStatus.SKIPPED}
            for node in nodes.values()
        ):
            raise InvalidStateTransition(
                "COMPLETED Run requires every Node to be SUCCEEDED or SKIPPED"
            )
        if run.status is RunStatus.FAILED and not (
            any(node.status is NodeStatus.FAILED for node in nodes.values())
            or run.error is not None
        ):
            raise InvalidStateTransition(
                "FAILED Run requires a failed Node or a run-level error"
            )
        if run.status is RunStatus.CANCELLED and any(
            node.status
            not in {NodeStatus.SUCCEEDED, NodeStatus.CANCELLED, NodeStatus.SKIPPED}
            for node in nodes.values()
        ):
            raise InvalidStateTransition(
                "CANCELLED Run only permits SUCCEEDED, CANCELLED, or SKIPPED Nodes"
            )

    @staticmethod
    def _validate_run_projection(
        current: RunRecord,
        proposed: RunRecord,
        event_type: str,
    ) -> None:
        if proposed.run_id != current.run_id:
            raise ProjectionConflictError("run projection belongs to another run")
        if (
            proposed.workflow_id != current.workflow_id
            or proposed.workflow_version != current.workflow_version
            or proposed.definition_digest != current.definition_digest
            or proposed.created_at != current.created_at
        ):
            raise ProjectionConflictError("immutable run fields cannot change")
        if (
            proposed.projection_version != current.projection_version
            or proposed.last_event_sequence != current.last_event_sequence
        ):
            raise ProjectionConflictError(
                f"stale run projection version {proposed.projection_version}; "
                f"current {current.projection_version}"
            )
        if proposed.status != current.status and proposed.status not in _RUN_TRANSITIONS[current.status]:
            raise InvalidStateTransition(
                f"invalid run transition {current.status.value} -> {proposed.status.value}"
            )
        if current.status.is_terminal and proposed != current:
            raise InvalidStateTransition("terminal run projection is immutable")
        if (
            current.status is RunStatus.WAITING_RECOVERY
            and proposed.status is RunStatus.RUNNING
            and event_type != "run.recovery_resolved"
        ):
            raise InvalidStateTransition(
                "WAITING_RECOVERY requires an explicit run.recovery_resolved event"
            )

    @staticmethod
    def _validate_event_projection_binding(
        event_type: str,
        run: RunRecord | None,
        node: NodeRecord | None,
        attempt: AttemptRecord | None,
    ) -> None:
        expected_run = _RUN_EVENT_STATUS.get(event_type)
        if expected_run is not None and (run is None or run.status is not expected_run):
            raise ProjectionConflictError(
                f"{event_type} requires run projection status {expected_run.value}"
            )
        expected_node = _NODE_EVENT_STATUS.get(event_type)
        if expected_node is not None and (node is None or node.status is not expected_node):
            raise ProjectionConflictError(
                f"{event_type} requires node projection status {expected_node.value}"
            )
        expected_attempt = _ATTEMPT_EVENT_STATUS.get(event_type)
        if expected_attempt is not None and (
            attempt is None or attempt.status is not expected_attempt
        ):
            raise ProjectionConflictError(
                f"{event_type} requires attempt projection status {expected_attempt.value}"
            )

    def _write_node_projection(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        node: NodeRecord,
        occurred_at: float,
        seq: int,
        *,
        allow_claimed_recovery_transition: bool = False,
        allow_policy_rejection_transition: bool = False,
        allow_deadline_transition: bool = False,
    ) -> NodeRecord:
        if node.run_id != run_id:
            raise ProjectionConflictError("node projection belongs to another run")
        current_row = conn.execute(
            "SELECT * FROM node_runs WHERE run_id = ? AND node_id = ?",
            (run_id, node.node_id),
        ).fetchone()
        if current_row is not None:
            current = self._node_from_row(current_row)
            if current.node_type != node.node_type or current.created_at != node.created_at:
                raise ProjectionConflictError("immutable node fields cannot change")
            if (
                node.projection_version != current.projection_version
                or node.last_event_sequence != current.last_event_sequence
            ):
                raise ProjectionConflictError("stale node projection")
            claimed_recovery_transition = (
                allow_claimed_recovery_transition
                and current.status is NodeStatus.READY
                and node.status in {NodeStatus.WAITING_RETRY, NodeStatus.FAILED}
            )
            policy_rejection_transition = (
                allow_policy_rejection_transition
                and current.status is NodeStatus.READY
                and node.status is NodeStatus.FAILED
            )
            deadline_transition = (
                allow_deadline_transition
                and current.status
                in {NodeStatus.READY, NodeStatus.RUNNING}
                and node.status in {NodeStatus.WAITING_RETRY, NodeStatus.FAILED}
            )
            if (
                node.status != current.status
                and node.status not in _NODE_TRANSITIONS[current.status]
                and not claimed_recovery_transition
                and not policy_rejection_transition
                and not deadline_transition
            ):
                raise InvalidStateTransition(
                    f"invalid node transition {current.status.value} -> {node.status.value}"
                )
            if current.status.is_terminal and node != current:
                raise InvalidStateTransition("terminal node projection is immutable")
            next_version = current.projection_version + 1
            previous_version = current.projection_version
            stored = replace(
                node,
                updated_at=max(occurred_at, current.updated_at),
                last_event_sequence=seq,
                projection_version=next_version,
            )
        else:
            if node.projection_version != 0 or node.last_event_sequence != 0:
                raise ProjectionConflictError("new node must have an uncommitted projection")
            if node.status is not NodeStatus.PENDING:
                raise InvalidStateTransition("new node must start in PENDING")
            previous_version = None
            stored = replace(
                node,
                updated_at=max(occurred_at, node.updated_at),
                last_event_sequence=seq,
                projection_version=1,
            )
        statement = """
            INSERT INTO node_runs(
                run_id, node_id, schema_version, node_type, status,
                input_json, output_json, error_json, metadata_json,
                attempt_count, created_at, updated_at,
                last_event_sequence, projection_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, node_id) DO UPDATE SET
                schema_version=excluded.schema_version,
                node_type=excluded.node_type,
                status=excluded.status,
                input_json=excluded.input_json,
                output_json=excluded.output_json,
                error_json=excluded.error_json,
                metadata_json=excluded.metadata_json,
                attempt_count=excluded.attempt_count,
                updated_at=excluded.updated_at,
                last_event_sequence=excluded.last_event_sequence,
                projection_version=excluded.projection_version
            WHERE node_runs.projection_version = ?
            """
        values = (
            stored.run_id,
            stored.node_id,
            stored.schema_version,
            stored.node_type,
            stored.status.value,
            _json_dump(stored.input),
            _json_dump(stored.output),
            _json_dump(stored.error),
            _json_dump(stored.metadata),
            stored.attempt_count,
            stored.created_at,
            stored.updated_at,
            stored.last_event_sequence,
            stored.projection_version,
            previous_version,
        )
        result = conn.execute(statement, values)
        if current_row is not None and result.rowcount != 1:
            raise ProjectionConflictError("node projection changed concurrently")
        return stored

    def _write_attempt_projection(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        attempt: AttemptRecord,
        seq: int,
        *,
        allow_deadline_transition: bool = False,
    ) -> AttemptRecord:
        if attempt.run_id != run_id:
            raise ProjectionConflictError("attempt projection belongs to another run")
        current_row = conn.execute(
            "SELECT * FROM attempts WHERE attempt_id = ?",
            (attempt.attempt_id,),
        ).fetchone()
        if current_row is not None:
            current = self._attempt_from_row(current_row)
            if (
                current.run_id != attempt.run_id
                or current.node_id != attempt.node_id
                or current.attempt_number != attempt.attempt_number
                or current.idempotency_key != attempt.idempotency_key
                or current.activity_kind != attempt.activity_kind
                or current.effect_class != attempt.effect_class
                or current.scheduled_at != attempt.scheduled_at
            ):
                raise ProjectionConflictError("immutable attempt fields cannot change")
            if (
                attempt.projection_version != current.projection_version
                or attempt.last_event_sequence != current.last_event_sequence
            ):
                raise ProjectionConflictError("stale attempt projection")
            if (
                attempt.status != current.status
                and attempt.status not in _ATTEMPT_TRANSITIONS[current.status]
                and not (
                    allow_deadline_transition
                    and current.status
                    in {AttemptStatus.SCHEDULED, AttemptStatus.CLAIMED}
                    and attempt.status is AttemptStatus.TIMED_OUT
                )
            ):
                raise InvalidStateTransition(
                    f"invalid attempt transition {current.status.value} -> "
                    f"{attempt.status.value}"
                )
            if current.status.is_terminal and attempt != current:
                raise InvalidStateTransition("terminal attempt projection is immutable")
            if attempt.fencing_token < current.fencing_token:
                raise ProjectionConflictError("attempt fencing token is stale")
            previous_version = current.projection_version
            stored = replace(
                attempt,
                last_event_sequence=seq,
                projection_version=current.projection_version + 1,
            )
        else:
            if attempt.projection_version != 0 or attempt.last_event_sequence != 0:
                raise ProjectionConflictError("new attempt must have an uncommitted projection")
            if attempt.status is not AttemptStatus.SCHEDULED:
                raise InvalidStateTransition("new attempt must start in SCHEDULED")
            if attempt.fencing_token != 0:
                raise InvalidStateTransition(
                    "new SCHEDULED attempt must start with fencing_token=0"
                )
            previous_version = None
            stored = replace(attempt, last_event_sequence=seq, projection_version=1)
        try:
            result = conn.execute(
                """
            INSERT INTO attempts(
                attempt_id, run_id, node_id, schema_version, attempt_number,
                idempotency_key, activity_kind, effect_class, status,
                worker_id, lease_id, fencing_token,
                result_json, error_json, metadata_json,
                scheduled_at, started_at, finished_at,
                last_event_sequence, projection_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(attempt_id) DO UPDATE SET
                schema_version=excluded.schema_version,
                status=excluded.status,
                worker_id=excluded.worker_id,
                lease_id=excluded.lease_id,
                fencing_token=excluded.fencing_token,
                result_json=excluded.result_json,
                error_json=excluded.error_json,
                metadata_json=excluded.metadata_json,
                started_at=excluded.started_at,
                finished_at=excluded.finished_at,
                last_event_sequence=excluded.last_event_sequence,
                projection_version=excluded.projection_version
            WHERE attempts.projection_version = ?
            """,
                (
                    stored.attempt_id,
                    stored.run_id,
                    stored.node_id,
                    stored.schema_version,
                    stored.attempt_number,
                    stored.idempotency_key,
                    stored.activity_kind,
                    stored.effect_class,
                    stored.status.value,
                    stored.worker_id,
                    stored.lease_id,
                    stored.fencing_token,
                    _json_dump(stored.result),
                    _json_dump(stored.error),
                    _json_dump(stored.metadata),
                    stored.scheduled_at,
                    stored.started_at,
                    stored.finished_at,
                    stored.last_event_sequence,
                    stored.projection_version,
                    previous_version,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise ProjectionConflictError(
                "Attempt conflicts with an existing number or active Attempt"
            ) from exc
        if current_row is not None and result.rowcount != 1:
            raise ProjectionConflictError("attempt projection changed concurrently")
        return stored

    @staticmethod
    def _run_from_row(row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            run_id=row["run_id"],
            workflow_id=row["workflow_id"],
            workflow_version=row["workflow_version"],
            definition_digest=row["definition_digest"],
            status=row["status"],
            input=_json_load(row["input_json"]),
            output=_json_load(row["output_json"]),
            error=_json_load(row["error_json"]),
            metadata=_json_load(row["metadata_json"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            last_event_sequence=row["last_event_sequence"],
            projection_version=row["projection_version"],
            schema_version=row["schema_version"],
        )

    @staticmethod
    def _node_from_row(row: sqlite3.Row) -> NodeRecord:
        return NodeRecord(
            run_id=row["run_id"],
            node_id=row["node_id"],
            node_type=row["node_type"],
            status=row["status"],
            input=_json_load(row["input_json"]),
            output=_json_load(row["output_json"]),
            error=_json_load(row["error_json"]),
            metadata=_json_load(row["metadata_json"]),
            attempt_count=row["attempt_count"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            last_event_sequence=row["last_event_sequence"],
            projection_version=row["projection_version"],
            schema_version=row["schema_version"],
        )

    @staticmethod
    def _attempt_from_row(row: sqlite3.Row) -> AttemptRecord:
        return AttemptRecord(
            attempt_id=row["attempt_id"],
            run_id=row["run_id"],
            node_id=row["node_id"],
            attempt_number=row["attempt_number"],
            idempotency_key=row["idempotency_key"],
            activity_kind=row["activity_kind"],
            effect_class=row["effect_class"],
            status=row["status"],
            worker_id=row["worker_id"],
            lease_id=row["lease_id"],
            fencing_token=row["fencing_token"],
            result=_json_load(row["result_json"]),
            error=_json_load(row["error_json"]),
            metadata=_json_load(row["metadata_json"]),
            scheduled_at=row["scheduled_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            last_event_sequence=row["last_event_sequence"],
            projection_version=row["projection_version"],
            schema_version=row["schema_version"],
        )

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> EventRecord:
        payload = _json_load(row["payload_json"])
        if row["content_digest"] != _content_digest(payload):
            raise StoreSchemaError(f"event {row['event_id']} content digest mismatch")
        return EventRecord(
            event_id=row["event_id"],
            run_id=row["run_id"],
            seq=row["seq"],
            event_type=row["event_type"],
            node_id=row["node_id"],
            attempt_id=row["attempt_id"],
            payload=payload,
            occurred_at=row["occurred_at"],
            schema_version=row["schema_version"],
        )

    @staticmethod
    def _workflow_binding_from_row(
        row: sqlite3.Row,
    ) -> WorkflowBindingRecord:
        try:
            raw_ref_json = row["workflow_ref_json"]
            workflow_ref = None
            if raw_ref_json is not None:
                raw_ref = _json_load(raw_ref_json)
                workflow_ref = _canonical_optional_workflow_ref(raw_ref)
                if _json_dump(workflow_ref.to_dict()) != raw_ref_json:
                    raise ValueError("workflow_ref_json is not canonical")
            return WorkflowBindingRecord(
                workflow_id=row["workflow_id"],
                workflow_version=row["workflow_version"],
                definition_digest=_sha256_digest(
                    row["definition_digest"],
                    "definition_digest",
                ),
                workflow_ref=workflow_ref,
                created_at=_finite_timestamp(row["created_at"], "created_at"),
            )
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise StoreSchemaError("workflow binding is malformed") from exc

    @staticmethod
    def _idempotency_from_row(row: sqlite3.Row) -> IdempotencyRecord:
        return IdempotencyRecord(
            run_id=row["run_id"],
            key=row["key"],
            request_hash=row["request_hash"],
            status=row["status"],
            owner_id=row["owner_id"],
            claim_token=row["claim_token"],
            lease_expires_at=row["lease_expires_at"],
            result=_json_load(row["result_json"]),
            claim_count=row["claim_count"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
            schema_version=row["schema_version"],
        )


def _bounded_limit(value: int, *, maximum: int) -> int:
    return max(1, min(maximum, int(value)))


def _validate_projection_replay_limits(
    *,
    max_events: int,
    max_payload_bytes: int,
    max_wall_seconds: float,
    page_size: int,
    clock: Callable[[], float],
) -> None:
    if (
        isinstance(max_events, bool)
        or not isinstance(max_events, int)
        or max_events < 1
        or max_events > MAX_PROJECTION_REPLAY_EVENTS
    ):
        raise ValueError("max_events must be a positive bounded integer")
    if (
        isinstance(max_payload_bytes, bool)
        or not isinstance(max_payload_bytes, int)
        or max_payload_bytes < 1
        or max_payload_bytes > MAX_PROJECTION_REPLAY_PAYLOAD_BYTES
    ):
        raise ValueError("max_payload_bytes must be a positive bounded integer")
    if (
        isinstance(max_wall_seconds, bool)
        or not isinstance(max_wall_seconds, (int, float))
        or not math.isfinite(float(max_wall_seconds))
        or max_wall_seconds <= 0
        or max_wall_seconds > MAX_PROJECTION_REPLAY_SECONDS
    ):
        raise ValueError("max_wall_seconds must be a positive bounded duration")
    if (
        isinstance(page_size, bool)
        or not isinstance(page_size, int)
        or page_size < 1
        or page_size > MAX_PROJECTION_REPLAY_PAGE
    ):
        raise ValueError("page_size must be a positive bounded integer")
    if not callable(clock):
        raise ValueError("clock must be callable")


def _projection_replay_now(clock: Callable[[], float]) -> float:
    now = float(clock())
    if not math.isfinite(now):
        raise ValueError("projection replay clock must be finite")
    return now


def _check_projection_replay_wall(
    clock: Callable[[], float],
    *,
    started_at: float,
    max_wall_seconds: float,
) -> None:
    if _projection_replay_now(clock) - started_at >= max_wall_seconds:
        raise ProjectionReplayLimitError("wall_time_limit")


def _projection_page(
    limit: int | None,
    offset: int,
) -> tuple[int, int] | None:
    if (
        isinstance(offset, bool)
        or not isinstance(offset, int)
        or offset < 0
        or offset > 1_000_000
    ):
        raise ValueError("projection offset must be a bounded non-negative integer")
    if limit is None:
        if offset:
            raise ValueError("projection offset requires a limit")
        return None
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or limit < 1
        or limit > 1_000
    ):
        raise ValueError("projection limit must be between 1 and 1000")
    return (limit, offset)
