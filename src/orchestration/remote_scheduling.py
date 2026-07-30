"""Deterministic, bounded admission and scheduling for remote workers.

This module is deliberately independent from the durable Store.  It selects a
compatible worker and accounts for ephemeral capacity; it never decides that a
durable Attempt has started or completed.  The remote protocol must still
commit every authoritative transition through the Store with lease and
fencing checks.

Fairness algorithm
------------------

Within a tenant, no compatible task bypasses an older compatible task.  Across
tenants, each pool has an independent, lexicographically stable round-robin
cursor.  A worker scans tenants from its pool's cursor and selects the oldest
task in the first tenant that is compatible with its pool, tool, capabilities,
runtime version, and current quota.  Continuously enqueueing work for one
tenant or another pool therefore cannot starve a different eligible tenant
while claims are being released.

Only bounded routing metadata is retained here.  Task arguments, Artifact
contents, credentials, and arbitrary labels are intentionally not accepted.
"""

from __future__ import annotations

import bisect
import hashlib
import json
import math
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Callable, Mapping

from .runtime_compatibility import (
    RuntimeCompatibility,
    RuntimeCompatibilityValidationError,
    canonical_runtime_version,
)

REMOTE_SCHEDULING_SCHEMA_VERSION = 1
FLEET_ADMISSION_SCHEMA_VERSION = 1
MAX_IDENTIFIER_CHARS = 64
MAX_CAPABILITIES = 64
MAX_TOOLS = 256
MAX_AUTHORIZED_TENANTS = 64
MAX_RESOURCE_KEYS = 128
MAX_WORKER_CAPACITY = 256
MAX_QUOTA_ENTRIES = 4096
MAX_SEQUENCE = (1 << 63) - 1
MAX_WORKER_IDLE_TTL_SECONDS = 7 * 24 * 60 * 60

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
_RESOURCE_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class RemoteSchedulingError(RuntimeError):
    """Base error for remote scheduling."""


class RemoteSchedulingValidationError(RemoteSchedulingError, ValueError):
    """A descriptor, task, quota, or scheduler bound is invalid."""


class RemoteSchedulingConflict(RemoteSchedulingError):
    """A lifecycle mutation conflicts with active work."""


class WorkerLifecycle(StrEnum):
    ACTIVE = "active"
    DRAINING = "draining"
    EXPIRED = "expired"


class AdmissionOutcome(StrEnum):
    ADMITTED = "admitted"
    DUPLICATE = "duplicate"
    QUEUE_FULL = "queue_full"
    TENANT_QUEUE_FULL = "tenant_queue_full"
    POOL_QUEUE_FULL = "pool_queue_full"
    TENANT_REGISTRY_FULL = "tenant_registry_full"


class PollOutcome(StrEnum):
    CLAIMED = "claimed"
    UNKNOWN_WORKER = "unknown_worker"
    WORKER_DRAINING = "worker_draining"
    WORKER_EXPIRED = "worker_expired"
    STALE_WORKER_GENERATION = "stale_worker_generation"
    WORKER_SESSION_MISMATCH = "worker_session_mismatch"
    WORKER_AT_CAPACITY = "worker_at_capacity"
    CONTROL_PLANE_AT_CAPACITY = "control_plane_at_capacity"
    POOL_AT_CAPACITY = "pool_at_capacity"
    TENANT_AT_CAPACITY = "tenant_at_capacity"
    NO_COMPATIBLE_TASK = "no_compatible_task"


class ReleaseOutcome(StrEnum):
    RELEASED = "released"
    UNKNOWN_ASSIGNMENT = "unknown_assignment"
    UNKNOWN_WORKER = "unknown_worker"
    WRONG_WORKER = "wrong_worker"
    STALE_WORKER_GENERATION = "stale_worker_generation"
    WORKER_SESSION_MISMATCH = "worker_session_mismatch"


class TouchOutcome(StrEnum):
    ACCEPTED = "accepted"
    UNKNOWN_WORKER = "unknown_worker"
    STALE_WORKER_GENERATION = "stale_worker_generation"
    WORKER_SESSION_MISMATCH = "worker_session_mismatch"
    WORKER_NOT_ACTIVE = "worker_not_active"


class WithdrawalOutcome(StrEnum):
    WITHDRAWN = "withdrawn"
    ACTIVE = "active"
    NOT_FOUND = "not_found"


def _identifier(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise RemoteSchedulingValidationError(f"{field_name} must be a string")
    text = value.strip()
    if (
        not text
        or len(text) > MAX_IDENTIFIER_CHARS
        or _IDENTIFIER.fullmatch(text) is None
    ):
        raise RemoteSchedulingValidationError(
            f"{field_name} must be a bounded opaque identifier"
        )
    return text


def _positive_int(value: object, field_name: str, *, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RemoteSchedulingValidationError(
            f"{field_name} must be a positive integer"
        )
    if value < 1 or value > maximum:
        raise RemoteSchedulingValidationError(
            f"{field_name} must be between 1 and {maximum}"
        )
    return value


def _positive_float(value: object, field_name: str, *, maximum: float) -> float:
    if isinstance(value, bool):
        raise RemoteSchedulingValidationError(
            f"{field_name} must be a finite positive number"
        )
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise RemoteSchedulingValidationError(
            f"{field_name} must be a finite positive number"
        ) from exc
    if not math.isfinite(number) or number <= 0 or number > maximum:
        raise RemoteSchedulingValidationError(
            f"{field_name} must be between 0 and {maximum}"
        )
    return number


def _finite_timestamp(value: object, field_name: str) -> float:
    if isinstance(value, bool):
        raise RemoteSchedulingValidationError(
            f"{field_name} must be a finite non-negative timestamp"
        )
    try:
        timestamp = float(value)
    except (TypeError, ValueError) as exc:
        raise RemoteSchedulingValidationError(
            f"{field_name} must be a finite non-negative timestamp"
        ) from exc
    if not math.isfinite(timestamp) or timestamp < 0:
        raise RemoteSchedulingValidationError(
            f"{field_name} must be a finite non-negative timestamp"
        )
    return timestamp


def _names(
    values: object,
    field_name: str,
    *,
    maximum: int,
    allow_empty: bool,
) -> frozenset[str]:
    if not isinstance(values, (tuple, list, set, frozenset)):
        raise RemoteSchedulingValidationError(f"{field_name} must be a collection")
    if len(values) > maximum:
        raise RemoteSchedulingValidationError(
            f"{field_name} exceeds the {maximum}-item limit"
        )
    normalized = frozenset(_identifier(value, field_name) for value in values)
    if len(normalized) != len(values):
        raise RemoteSchedulingValidationError(f"{field_name} contains duplicates")
    if not normalized and not allow_empty:
        raise RemoteSchedulingValidationError(f"{field_name} must not be empty")
    return normalized


def _resource_keys(
    values: object,
    field_name: str,
) -> frozenset[str]:
    if not isinstance(values, (tuple, list, set, frozenset)):
        raise RemoteSchedulingValidationError(
            f"{field_name} must be a collection"
        )
    if len(values) > MAX_RESOURCE_KEYS:
        raise RemoteSchedulingValidationError(
            f"{field_name} exceeds the {MAX_RESOURCE_KEYS}-item limit"
        )
    normalized: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            raise RemoteSchedulingValidationError(
                f"{field_name} must contain strings"
            )
        text = value.strip()
        if (
            not text
            or _RESOURCE_KEY.fullmatch(text) is None
        ):
            raise RemoteSchedulingValidationError(
                f"{field_name} contains an invalid resource key"
            )
        normalized.add(text)
    if len(normalized) != len(values):
        raise RemoteSchedulingValidationError(
            f"{field_name} contains duplicates"
        )
    return frozenset(normalized)


def _canonical_runtime_version(value: object, field_name: str) -> str:
    try:
        return canonical_runtime_version(value, field_name)
    except RuntimeCompatibilityValidationError as exc:
        raise RemoteSchedulingValidationError(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class WorkerDescriptor:
    """Bounded execution compatibility and capacity advertised by a worker."""

    worker_id: str
    session_id: str
    pool_id: str
    runtime_version: str
    capabilities: frozenset[str]
    tools: frozenset[str]
    authorized_tenants: frozenset[str]
    capacity: int = 1
    schema_version: int = REMOTE_SCHEDULING_SCHEMA_VERSION
    resource_keys: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if self.schema_version != REMOTE_SCHEDULING_SCHEMA_VERSION:
            raise RemoteSchedulingValidationError(
                "unsupported WorkerDescriptor schema version"
            )
        object.__setattr__(self, "worker_id", _identifier(self.worker_id, "worker_id"))
        object.__setattr__(self, "session_id", _identifier(self.session_id, "session_id"))
        object.__setattr__(self, "pool_id", _identifier(self.pool_id, "pool_id"))
        object.__setattr__(
            self,
            "runtime_version",
            _canonical_runtime_version(self.runtime_version, "runtime_version"),
        )
        object.__setattr__(
            self,
            "capabilities",
            _names(
                self.capabilities,
                "capabilities",
                maximum=MAX_CAPABILITIES,
                allow_empty=True,
            ),
        )
        object.__setattr__(
            self,
            "tools",
            _names(
                self.tools,
                "tools",
                maximum=MAX_TOOLS,
                allow_empty=False,
            ),
        )
        object.__setattr__(
            self,
            "authorized_tenants",
            _names(
                self.authorized_tenants,
                "authorized_tenants",
                maximum=MAX_AUTHORIZED_TENANTS,
                allow_empty=False,
            ),
        )
        object.__setattr__(
            self,
            "resource_keys",
            _resource_keys(self.resource_keys, "resource_keys"),
        )
        object.__setattr__(
            self,
            "capacity",
            _positive_int(
                self.capacity,
                "capacity",
                maximum=MAX_WORKER_CAPACITY,
            ),
        )


@dataclass(frozen=True, slots=True)
class RemoteTask:
    """Secret-free routing envelope for one durable Activity Attempt."""

    task_id: str
    tenant_id: str
    pool_id: str
    tool_name: str
    required_capabilities: frozenset[str] = frozenset()
    min_runtime_version: str = "0"
    max_runtime_version: str | None = None
    schema_version: int = REMOTE_SCHEDULING_SCHEMA_VERSION
    required_resource_keys: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if self.schema_version != REMOTE_SCHEDULING_SCHEMA_VERSION:
            raise RemoteSchedulingValidationError(
                "unsupported RemoteTask schema version"
            )
        object.__setattr__(self, "task_id", _identifier(self.task_id, "task_id"))
        object.__setattr__(self, "tenant_id", _identifier(self.tenant_id, "tenant_id"))
        object.__setattr__(self, "pool_id", _identifier(self.pool_id, "pool_id"))
        object.__setattr__(self, "tool_name", _identifier(self.tool_name, "tool_name"))
        object.__setattr__(
            self,
            "required_capabilities",
            _names(
                self.required_capabilities,
                "required_capabilities",
                maximum=MAX_CAPABILITIES,
                allow_empty=True,
            ),
        )
        object.__setattr__(
            self,
            "required_resource_keys",
            _resource_keys(
                self.required_resource_keys,
                "required_resource_keys",
            ),
        )
        try:
            compatibility = RuntimeCompatibility(
                min_runtime_version=self.min_runtime_version,
                max_runtime_version=self.max_runtime_version,
            )
        except RuntimeCompatibilityValidationError as exc:
            raise RemoteSchedulingValidationError(str(exc)) from exc
        object.__setattr__(
            self,
            "min_runtime_version",
            compatibility.min_runtime_version,
        )
        object.__setattr__(
            self,
            "max_runtime_version",
            compatibility.max_runtime_version,
        )


@dataclass(frozen=True, slots=True)
class FleetAdmissionScope:
    """Secret-free durable quota binding for one selected Fleet task."""

    task_id: str
    tenant_id: str
    pool_id: str
    routing_policy_digest: str
    quota_policy_digest: str
    max_active_tasks: int
    tenant_concurrency: int
    pool_concurrency: int
    schema_version: int = FLEET_ADMISSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FLEET_ADMISSION_SCHEMA_VERSION:
            raise RemoteSchedulingValidationError(
                "unsupported FleetAdmissionScope schema version"
            )
        object.__setattr__(
            self,
            "task_id",
            _identifier(self.task_id, "task_id"),
        )
        object.__setattr__(
            self,
            "tenant_id",
            _identifier(self.tenant_id, "tenant_id"),
        )
        object.__setattr__(
            self,
            "pool_id",
            _identifier(self.pool_id, "pool_id"),
        )
        for field_name in (
            "routing_policy_digest",
            "quota_policy_digest",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
                raise RemoteSchedulingValidationError(
                    f"{field_name} must be a SHA-256 digest"
                )
        for field_name in (
            "max_active_tasks",
            "tenant_concurrency",
            "pool_concurrency",
        ):
            object.__setattr__(
                self,
                field_name,
                _positive_int(
                    getattr(self, field_name),
                    field_name,
                    maximum=1_000_000,
                ),
            )

    def to_metadata(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "task_id": self.task_id,
            "tenant_id": self.tenant_id,
            "pool_id": self.pool_id,
            "routing_policy_digest": self.routing_policy_digest,
            "quota_policy_digest": self.quota_policy_digest,
            "max_active_tasks": self.max_active_tasks,
            "tenant_concurrency": self.tenant_concurrency,
            "pool_concurrency": self.pool_concurrency,
        }


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    outcome: AdmissionOutcome
    queue_depth: int


@dataclass(frozen=True, slots=True)
class RemoteAssignment:
    assignment_id: str
    task: RemoteTask
    worker_id: str
    worker_generation: int
    worker_session_id: str
    enqueued_at: float
    claimed_at: float
    schedule_to_start_seconds: float


@dataclass(frozen=True, slots=True)
class PollDecision:
    outcome: PollOutcome
    assignment: RemoteAssignment | None = None


@dataclass(frozen=True, slots=True)
class WorkerSnapshot:
    descriptor: WorkerDescriptor
    lifecycle: WorkerLifecycle
    generation: int
    active_count: int
    registered_at: float
    last_seen_at: float
    idle_ttl_seconds: float
    drain_requested_at: float | None
    expired_at: float | None


@dataclass(frozen=True, slots=True)
class WorkerSweepReport:
    """Bounded expiration transitions; assignment ownership is untouched."""

    observed_at: float
    examined_workers: int
    removed_idle_workers: int
    expired_busy_workers: int
    retained_assignments: int


@dataclass(frozen=True, slots=True)
class RemoteSchedulingSnapshot:
    """Bounded point-in-time diagnostics; never an execution fact source."""

    workers: tuple[WorkerSnapshot, ...]
    queued_tasks: int
    active_assignments: int
    queued_tenants: int
    tenant_cursors: tuple[tuple[str, str], ...]
    observed_at: float
    execution_truth: bool = False


@dataclass(slots=True)
class _WorkerState:
    descriptor: WorkerDescriptor
    registered_at: float
    last_seen_at: float
    idle_ttl_seconds: float
    lifecycle: WorkerLifecycle = WorkerLifecycle.ACTIVE
    generation: int = 1
    drain_requested_at: float | None = None
    expired_at: float | None = None
    assignments: set[str] = field(default_factory=set)


@dataclass(frozen=True, slots=True)
class _QueuedTask:
    task: RemoteTask
    enqueued_at: float
    sequence: int


def is_worker_compatible(worker: WorkerDescriptor, task: RemoteTask) -> bool:
    """Return whether immutable routing constraints permit this worker."""

    if (
        worker.pool_id != task.pool_id
        or task.tenant_id not in worker.authorized_tenants
        or task.tool_name not in worker.tools
    ):
        return False
    if not task.required_capabilities.issubset(worker.capabilities):
        return False
    if not task.required_resource_keys.issubset(worker.resource_keys):
        return False
    return RuntimeCompatibility(
        min_runtime_version=task.min_runtime_version,
        max_runtime_version=task.max_runtime_version,
    ).accepts(worker.runtime_version)


class DeterministicRemoteScheduler:
    """Thread-safe bounded registry, admission queue, and fair selector."""

    def __init__(
        self,
        *,
        max_workers: int = 128,
        max_tenants: int = 256,
        max_queued_tasks: int = 4096,
        max_queued_per_tenant: int = 256,
        max_queued_per_pool: int = 1024,
        max_active_tasks: int = 1024,
        default_tenant_concurrency: int = 16,
        tenant_concurrency_quotas: Mapping[str, int] | None = None,
        default_pool_concurrency: int = 128,
        pool_concurrency_quotas: Mapping[str, int] | None = None,
        worker_idle_ttl_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_workers = _positive_int(max_workers, "max_workers", maximum=4096)
        self.max_tenants = _positive_int(max_tenants, "max_tenants", maximum=4096)
        self.max_queued_tasks = _positive_int(
            max_queued_tasks,
            "max_queued_tasks",
            maximum=1_000_000,
        )
        self.max_queued_per_tenant = _positive_int(
            max_queued_per_tenant,
            "max_queued_per_tenant",
            maximum=1_000_000,
        )
        self.max_queued_per_pool = _positive_int(
            max_queued_per_pool,
            "max_queued_per_pool",
            maximum=1_000_000,
        )
        self.max_active_tasks = _positive_int(
            max_active_tasks,
            "max_active_tasks",
            maximum=1_000_000,
        )
        self.default_tenant_concurrency = _positive_int(
            default_tenant_concurrency,
            "default_tenant_concurrency",
            maximum=1_000_000,
        )
        self.default_pool_concurrency = _positive_int(
            default_pool_concurrency,
            "default_pool_concurrency",
            maximum=1_000_000,
        )
        self.worker_idle_ttl_seconds = _positive_float(
            worker_idle_ttl_seconds,
            "worker_idle_ttl_seconds",
            maximum=MAX_WORKER_IDLE_TTL_SECONDS,
        )
        self._tenant_quotas = self._normalize_quotas(
            tenant_concurrency_quotas,
            "tenant_concurrency_quotas",
        )
        self._pool_quotas = self._normalize_quotas(
            pool_concurrency_quotas,
            "pool_concurrency_quotas",
        )
        self._quota_policy_digest = hashlib.sha256(
            json.dumps(
                {
                    "schema": "durable_fleet_quota_policy_v1",
                    "max_active_tasks": self.max_active_tasks,
                    "default_tenant_concurrency": (
                        self.default_tenant_concurrency
                    ),
                    "tenant_concurrency_quotas": sorted(
                        self._tenant_quotas.items()
                    ),
                    "default_pool_concurrency": (
                        self.default_pool_concurrency
                    ),
                    "pool_concurrency_quotas": sorted(
                        self._pool_quotas.items()
                    ),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        if not callable(clock):
            raise RemoteSchedulingValidationError("clock must be callable")
        self._clock = clock
        self._lock = threading.RLock()
        self._workers: dict[str, _WorkerState] = {}
        self._tenant_queues: dict[str, deque[_QueuedTask]] = {}
        self._queued_task_ids: set[str] = set()
        self._queued_by_pool: dict[str, int] = {}
        self._active: dict[str, RemoteAssignment] = {}
        self._active_task_ids: set[str] = set()
        self._active_by_tenant: dict[str, int] = {}
        self._active_by_pool: dict[str, int] = {}
        self._last_served_tenant_by_pool: dict[str, str] = {}
        self._worker_generation_sequence = 0
        self._task_sequence = 0
        self._assignment_sequence = 0

    def _normalize_quotas(
        self,
        raw: Mapping[str, int] | None,
        field_name: str,
    ) -> dict[str, int]:
        if raw is None:
            return {}
        if not isinstance(raw, Mapping) or len(raw) > MAX_QUOTA_ENTRIES:
            raise RemoteSchedulingValidationError(
                f"{field_name} exceeds the bounded quota registry"
            )
        quotas: dict[str, int] = {}
        for key, value in raw.items():
            normalized_key = _identifier(key, field_name)
            if normalized_key in quotas:
                raise RemoteSchedulingValidationError(
                    f"{field_name} contains duplicate normalized identifiers"
                )
            quotas[normalized_key] = _positive_int(
                value,
                field_name,
                maximum=1_000_000,
            )
        return quotas

    def _now(self) -> float:
        return _finite_timestamp(self._clock(), "clock")

    @staticmethod
    def _next_sequence(current: int, field_name: str) -> int:
        if current >= MAX_SEQUENCE:
            raise RemoteSchedulingConflict(f"{field_name} space is exhausted")
        return current + 1

    def _next_worker_generation(self) -> int:
        self._worker_generation_sequence = self._next_sequence(
            self._worker_generation_sequence,
            "worker generation",
        )
        return self._worker_generation_sequence

    def register_worker(self, descriptor: WorkerDescriptor) -> WorkerSnapshot:
        if not isinstance(descriptor, WorkerDescriptor):
            raise RemoteSchedulingValidationError(
                "descriptor must be a WorkerDescriptor"
            )
        now = self._now()
        with self._lock:
            state = self._workers.get(descriptor.worker_id)
            if state is None:
                if len(self._workers) >= self.max_workers:
                    raise RemoteSchedulingConflict("worker registry is full")
                state = _WorkerState(
                    descriptor=descriptor,
                    registered_at=now,
                    last_seen_at=now,
                    idle_ttl_seconds=self.worker_idle_ttl_seconds,
                    generation=self._next_worker_generation(),
                )
                self._workers[descriptor.worker_id] = state
            else:
                if state.assignments:
                    if (
                        state.lifecycle is WorkerLifecycle.ACTIVE
                        and state.descriptor == descriptor
                    ):
                        state.last_seen_at = now
                        return self._worker_snapshot(state)
                    raise RemoteSchedulingConflict(
                        "cannot replace or revive a worker with active assignments"
                    )
                if state.descriptor == descriptor:
                    state.last_seen_at = now
                    return self._worker_snapshot(state)
                if (
                    state.lifecycle is WorkerLifecycle.EXPIRED
                    and descriptor.session_id == state.descriptor.session_id
                ):
                    raise RemoteSchedulingConflict(
                        "expired worker replacement requires a new session"
                    )
                previous_pool = state.descriptor.pool_id
                state.descriptor = descriptor
                state.registered_at = now
                state.last_seen_at = now
                state.idle_ttl_seconds = self.worker_idle_ttl_seconds
                state.lifecycle = WorkerLifecycle.ACTIVE
                state.drain_requested_at = None
                state.expired_at = None
                state.generation = self._next_worker_generation()
                self._cleanup_pool_cursor(previous_pool)
            return self._worker_snapshot(state)

    def request_drain(self, worker_id: str) -> WorkerSnapshot:
        worker_id = _identifier(worker_id, "worker_id")
        now = self._now()
        with self._lock:
            state = self._workers.get(worker_id)
            if state is None:
                raise RemoteSchedulingConflict("unknown worker")
            if state.lifecycle is WorkerLifecycle.ACTIVE:
                state.lifecycle = WorkerLifecycle.DRAINING
                state.drain_requested_at = now
            return self._worker_snapshot(state)

    def reactivate_worker(self, worker_id: str) -> WorkerSnapshot:
        worker_id = _identifier(worker_id, "worker_id")
        now = self._now()
        with self._lock:
            state = self._workers.get(worker_id)
            if state is None:
                raise RemoteSchedulingConflict("unknown worker")
            if state.lifecycle is WorkerLifecycle.ACTIVE:
                return self._worker_snapshot(state)
            if state.lifecycle is WorkerLifecycle.EXPIRED:
                raise RemoteSchedulingConflict(
                    "expired worker requires registration with a new session"
                )
            if state.assignments:
                raise RemoteSchedulingConflict(
                    "cannot reactivate a worker with active assignments"
                )
            state.lifecycle = WorkerLifecycle.ACTIVE
            state.drain_requested_at = None
            state.last_seen_at = now
            state.generation = self._next_worker_generation()
            return self._worker_snapshot(state)

    def remove_worker(self, worker_id: str) -> bool:
        worker_id = _identifier(worker_id, "worker_id")
        with self._lock:
            state = self._workers.get(worker_id)
            if state is None:
                return False
            if (
                state.lifecycle
                not in {WorkerLifecycle.DRAINING, WorkerLifecycle.EXPIRED}
                or state.assignments
            ):
                raise RemoteSchedulingConflict(
                    "worker must be inactive and idle before removal"
                )
            del self._workers[worker_id]
            self._cleanup_pool_cursor(state.descriptor.pool_id)
            return True

    def touch_worker(
        self,
        worker_id: str,
        *,
        worker_generation: int,
        session_id: str,
    ) -> TouchOutcome:
        """Refresh liveness only for the exact registered worker authority."""

        worker_id = _identifier(worker_id, "worker_id")
        session_id = _identifier(session_id, "session_id")
        worker_generation = _positive_int(
            worker_generation,
            "worker_generation",
            maximum=MAX_SEQUENCE,
        )
        now = self._now()
        with self._lock:
            state = self._workers.get(worker_id)
            if state is None:
                return TouchOutcome.UNKNOWN_WORKER
            if state.generation != worker_generation:
                return TouchOutcome.STALE_WORKER_GENERATION
            if state.descriptor.session_id != session_id:
                return TouchOutcome.WORKER_SESSION_MISMATCH
            if state.lifecycle is WorkerLifecycle.EXPIRED:
                return TouchOutcome.WORKER_NOT_ACTIVE
            state.last_seen_at = now
            return TouchOutcome.ACCEPTED

    def sweep_idle_workers(self) -> WorkerSweepReport:
        """Expire idle workers without interpreting or releasing assignments."""

        now = self._now()
        with self._lock:
            examined = len(self._workers)
            removed = 0
            expired = 0
            retained_assignments = 0
            for worker_id in sorted(tuple(self._workers)):
                state = self._workers[worker_id]
                if state.lifecycle is WorkerLifecycle.EXPIRED:
                    continue
                if now < state.last_seen_at + state.idle_ttl_seconds:
                    continue
                if state.assignments:
                    state.lifecycle = WorkerLifecycle.EXPIRED
                    state.drain_requested_at = (
                        now
                        if state.drain_requested_at is None
                        else state.drain_requested_at
                    )
                    state.expired_at = now
                    expired += 1
                    retained_assignments += len(state.assignments)
                    continue
                del self._workers[worker_id]
                self._cleanup_pool_cursor(state.descriptor.pool_id)
                removed += 1
            return WorkerSweepReport(
                observed_at=now,
                examined_workers=examined,
                removed_idle_workers=removed,
                expired_busy_workers=expired,
                retained_assignments=retained_assignments,
            )

    def admit(self, task: RemoteTask) -> AdmissionDecision:
        if not isinstance(task, RemoteTask):
            raise RemoteSchedulingValidationError("task must be a RemoteTask")
        now = self._now()
        with self._lock:
            current_depth = len(self._queued_task_ids)
            if (
                task.task_id in self._queued_task_ids
                or task.task_id in self._active_task_ids
            ):
                return AdmissionDecision(AdmissionOutcome.DUPLICATE, current_depth)
            if current_depth >= self.max_queued_tasks:
                return AdmissionDecision(AdmissionOutcome.QUEUE_FULL, current_depth)
            tenant_queue = self._tenant_queues.get(task.tenant_id)
            if tenant_queue is not None and len(tenant_queue) >= self.max_queued_per_tenant:
                return AdmissionDecision(
                    AdmissionOutcome.TENANT_QUEUE_FULL,
                    current_depth,
                )
            if self._queued_by_pool.get(task.pool_id, 0) >= self.max_queued_per_pool:
                return AdmissionDecision(
                    AdmissionOutcome.POOL_QUEUE_FULL,
                    current_depth,
                )
            if tenant_queue is None:
                tracked_tenants = set(self._tenant_queues)
                tracked_tenants.update(self._active_by_tenant)
                if (
                    task.tenant_id not in tracked_tenants
                    and len(tracked_tenants) >= self.max_tenants
                ):
                    return AdmissionDecision(
                        AdmissionOutcome.TENANT_REGISTRY_FULL,
                        current_depth,
                    )
                tenant_queue = deque()
                self._tenant_queues[task.tenant_id] = tenant_queue
            self._task_sequence = self._next_sequence(
                self._task_sequence,
                "task sequence",
            )
            tenant_queue.append(
                _QueuedTask(
                    task=task,
                    enqueued_at=now,
                    sequence=self._task_sequence,
                )
            )
            self._queued_task_ids.add(task.task_id)
            self._queued_by_pool[task.pool_id] = (
                self._queued_by_pool.get(task.pool_id, 0) + 1
            )
            return AdmissionDecision(
                AdmissionOutcome.ADMITTED,
                current_depth + 1,
            )

    def poll_and_claim(
        self,
        worker_id: str,
        *,
        worker_generation: int,
        session_id: str,
    ) -> PollDecision:
        worker_id = _identifier(worker_id, "worker_id")
        session_id = _identifier(session_id, "session_id")
        worker_generation = _positive_int(
            worker_generation,
            "worker_generation",
            maximum=MAX_SEQUENCE,
        )
        now = self._now()
        with self._lock:
            state = self._workers.get(worker_id)
            if state is None:
                return PollDecision(PollOutcome.UNKNOWN_WORKER)
            if state.generation != worker_generation:
                return PollDecision(PollOutcome.STALE_WORKER_GENERATION)
            if state.descriptor.session_id != session_id:
                return PollDecision(PollOutcome.WORKER_SESSION_MISMATCH)
            if state.lifecycle is WorkerLifecycle.DRAINING:
                state.last_seen_at = now
                return PollDecision(PollOutcome.WORKER_DRAINING)
            if state.lifecycle is WorkerLifecycle.EXPIRED:
                return PollDecision(PollOutcome.WORKER_EXPIRED)
            state.last_seen_at = now
            if len(state.assignments) >= state.descriptor.capacity:
                return PollDecision(PollOutcome.WORKER_AT_CAPACITY)
            if len(self._active) >= self.max_active_tasks:
                return PollDecision(PollOutcome.CONTROL_PLANE_AT_CAPACITY)
            pool_id = state.descriptor.pool_id
            if self._active_by_pool.get(pool_id, 0) >= self._pool_quota(pool_id):
                return PollDecision(PollOutcome.POOL_AT_CAPACITY)

            blocked_by_tenant_quota = False
            for tenant_id in self._tenant_scan_order(pool_id):
                queue = self._tenant_queues[tenant_id]
                if (
                    self._active_by_tenant.get(tenant_id, 0)
                    >= self._tenant_quota(tenant_id)
                ):
                    if any(
                        is_worker_compatible(state.descriptor, item.task)
                        for item in queue
                    ):
                        blocked_by_tenant_quota = True
                    continue
                selected_index = self._first_compatible_index(
                    queue,
                    state.descriptor,
                )
                if selected_index is None:
                    continue
                queued = queue[selected_index]
                del queue[selected_index]
                self._queued_task_ids.remove(queued.task.task_id)
                self._decrement_count(self._queued_by_pool, queued.task.pool_id)
                if not queue:
                    del self._tenant_queues[tenant_id]

                self._assignment_sequence = self._next_sequence(
                    self._assignment_sequence,
                    "assignment sequence",
                )
                assignment_id = f"assignment-{self._assignment_sequence}"
                assignment = RemoteAssignment(
                    assignment_id=assignment_id,
                    task=queued.task,
                    worker_id=worker_id,
                    worker_generation=state.generation,
                    worker_session_id=state.descriptor.session_id,
                    enqueued_at=queued.enqueued_at,
                    claimed_at=now,
                    schedule_to_start_seconds=max(0.0, now - queued.enqueued_at),
                )
                self._active[assignment_id] = assignment
                self._active_task_ids.add(queued.task.task_id)
                state.assignments.add(assignment_id)
                self._active_by_tenant[tenant_id] = (
                    self._active_by_tenant.get(tenant_id, 0) + 1
                )
                self._active_by_pool[pool_id] = (
                    self._active_by_pool.get(pool_id, 0) + 1
                )
                self._last_served_tenant_by_pool[pool_id] = tenant_id
                return PollDecision(PollOutcome.CLAIMED, assignment)

            if blocked_by_tenant_quota:
                return PollDecision(PollOutcome.TENANT_AT_CAPACITY)
            return PollDecision(PollOutcome.NO_COMPATIBLE_TASK)

    def release(
        self,
        assignment_id: str,
        worker_id: str,
        *,
        worker_generation: int,
        session_id: str,
    ) -> ReleaseOutcome:
        assignment_id = _identifier(assignment_id, "assignment_id")
        worker_id = _identifier(worker_id, "worker_id")
        session_id = _identifier(session_id, "session_id")
        worker_generation = _positive_int(
            worker_generation,
            "worker_generation",
            maximum=MAX_SEQUENCE,
        )
        with self._lock:
            assignment = self._active.get(assignment_id)
            if assignment is None:
                return ReleaseOutcome.UNKNOWN_ASSIGNMENT
            if assignment.worker_id != worker_id:
                return ReleaseOutcome.WRONG_WORKER
            state = self._workers.get(worker_id)
            if state is None:
                return ReleaseOutcome.UNKNOWN_WORKER
            if (
                assignment.worker_generation != worker_generation
                or state.generation != worker_generation
            ):
                return ReleaseOutcome.STALE_WORKER_GENERATION
            if (
                assignment.worker_session_id != session_id
                or state.descriptor.session_id != session_id
            ):
                return ReleaseOutcome.WORKER_SESSION_MISMATCH
            del self._active[assignment_id]
            self._active_task_ids.remove(assignment.task.task_id)
            state.assignments.discard(assignment_id)
            self._decrement_count(
                self._active_by_tenant,
                assignment.task.tenant_id,
            )
            self._decrement_count(self._active_by_pool, assignment.task.pool_id)
            if (
                state.lifecycle is WorkerLifecycle.EXPIRED
                and not state.assignments
            ):
                del self._workers[worker_id]
            self._cleanup_pool_cursor(assignment.task.pool_id)
            return ReleaseOutcome.RELEASED

    def withdraw(self, task_id: str) -> WithdrawalOutcome:
        """Atomically remove queued work, for example after durable cancellation.

        If polling won the race, ``ACTIVE`` is returned and the caller must use
        the remote protocol's durable cancellation flow.  This method does not
        cancel or complete an authoritative Attempt.
        """

        task_id = _identifier(task_id, "task_id")
        with self._lock:
            if task_id in self._active_task_ids:
                return WithdrawalOutcome.ACTIVE
            if task_id not in self._queued_task_ids:
                return WithdrawalOutcome.NOT_FOUND
            for tenant_id, queue in tuple(self._tenant_queues.items()):
                for index, queued in enumerate(queue):
                    if queued.task.task_id != task_id:
                        continue
                    del queue[index]
                    self._queued_task_ids.remove(task_id)
                    self._decrement_count(
                        self._queued_by_pool,
                        queued.task.pool_id,
                    )
                    if not queue:
                        del self._tenant_queues[tenant_id]
                    return WithdrawalOutcome.WITHDRAWN
            # Internal accounting is intentionally fail closed rather than
            # silently leaking queue capacity.
            raise RemoteSchedulingConflict("queued task index is inconsistent")

    def worker_snapshot(self, worker_id: str) -> WorkerSnapshot | None:
        worker_id = _identifier(worker_id, "worker_id")
        with self._lock:
            state = self._workers.get(worker_id)
            return None if state is None else self._worker_snapshot(state)

    def snapshot(self) -> RemoteSchedulingSnapshot:
        now = self._now()
        with self._lock:
            return RemoteSchedulingSnapshot(
                workers=tuple(
                    self._worker_snapshot(self._workers[worker_id])
                    for worker_id in sorted(self._workers)
                ),
                queued_tasks=len(self._queued_task_ids),
                active_assignments=len(self._active),
                queued_tenants=len(self._tenant_queues),
                tenant_cursors=tuple(
                    sorted(self._last_served_tenant_by_pool.items())
                ),
                observed_at=now,
            )

    def queue_depth(
        self,
        *,
        tenant_id: str | None = None,
        pool_id: str | None = None,
    ) -> int:
        if tenant_id is not None:
            tenant_id = _identifier(tenant_id, "tenant_id")
        if pool_id is not None:
            pool_id = _identifier(pool_id, "pool_id")
        with self._lock:
            if tenant_id is None and pool_id is None:
                return len(self._queued_task_ids)
            if tenant_id is None:
                assert pool_id is not None
                return self._queued_by_pool.get(pool_id, 0)
            queue = self._tenant_queues.get(tenant_id, ())
            if pool_id is None:
                return len(queue)
            return sum(1 for item in queue if item.task.pool_id == pool_id)

    def durable_admission_scope(
        self,
        task: RemoteTask,
        *,
        routing_policy_digest: str,
    ) -> FleetAdmissionScope:
        """Bind immutable routing policy and effective quotas for Store CAS."""

        if not isinstance(task, RemoteTask):
            raise RemoteSchedulingValidationError(
                "task must be a RemoteTask"
            )
        return FleetAdmissionScope(
            task_id=task.task_id,
            tenant_id=task.tenant_id,
            pool_id=task.pool_id,
            routing_policy_digest=routing_policy_digest,
            quota_policy_digest=self._quota_policy_digest,
            max_active_tasks=self.max_active_tasks,
            tenant_concurrency=self._tenant_quota(task.tenant_id),
            pool_concurrency=self._pool_quota(task.pool_id),
        )

    def _tenant_scan_order(self, pool_id: str) -> tuple[str, ...]:
        tenants = sorted(
            tenant_id
            for tenant_id, queue in self._tenant_queues.items()
            if any(item.task.pool_id == pool_id for item in queue)
        )
        if not tenants:
            return ()
        last_served = self._last_served_tenant_by_pool.get(pool_id)
        if last_served is None:
            return tuple(tenants)
        start = bisect.bisect_right(tenants, last_served)
        if start == len(tenants):
            start = 0
        return tuple(tenants[start:] + tenants[:start])

    @staticmethod
    def _first_compatible_index(
        queue: deque[_QueuedTask],
        worker: WorkerDescriptor,
    ) -> int | None:
        for index, queued in enumerate(queue):
            if is_worker_compatible(worker, queued.task):
                return index
        return None

    def _tenant_quota(self, tenant_id: str) -> int:
        return self._tenant_quotas.get(
            tenant_id,
            self.default_tenant_concurrency,
        )

    def _pool_quota(self, pool_id: str) -> int:
        return self._pool_quotas.get(pool_id, self.default_pool_concurrency)

    def _cleanup_pool_cursor(self, pool_id: str) -> None:
        if (
            self._queued_by_pool.get(pool_id, 0)
            or self._active_by_pool.get(pool_id, 0)
            or any(
                state.descriptor.pool_id == pool_id
                for state in self._workers.values()
            )
        ):
            return
        self._last_served_tenant_by_pool.pop(pool_id, None)

    @staticmethod
    def _decrement_count(counts: dict[str, int], key: str) -> None:
        remaining = counts[key] - 1
        if remaining:
            counts[key] = remaining
        else:
            del counts[key]

    @staticmethod
    def _worker_snapshot(state: _WorkerState) -> WorkerSnapshot:
        return WorkerSnapshot(
            descriptor=state.descriptor,
            lifecycle=state.lifecycle,
            generation=state.generation,
            active_count=len(state.assignments),
            registered_at=state.registered_at,
            last_seen_at=state.last_seen_at,
            idle_ttl_seconds=state.idle_ttl_seconds,
            drain_requested_at=state.drain_requested_at,
            expired_at=state.expired_at,
        )


__all__ = [
    "AdmissionDecision",
    "AdmissionOutcome",
    "DeterministicRemoteScheduler",
    "FLEET_ADMISSION_SCHEMA_VERSION",
    "FleetAdmissionScope",
    "PollDecision",
    "PollOutcome",
    "ReleaseOutcome",
    "RemoteAssignment",
    "RemoteSchedulingConflict",
    "RemoteSchedulingError",
    "RemoteSchedulingSnapshot",
    "RemoteSchedulingValidationError",
    "RemoteTask",
    "RuntimeCompatibility",
    "TouchOutcome",
    "WorkerDescriptor",
    "WorkerLifecycle",
    "WorkerSnapshot",
    "WorkerSweepReport",
    "WithdrawalOutcome",
    "is_worker_compatible",
]
