"""Bounded, sanitized, best-effort observations for remote execution.

The counters in this module are operational hints only.  They must never be
used to claim, complete, recover, or otherwise transition a durable Attempt.
No worker, tenant, task, Run, or Attempt identifier is emitted.  Pool
dimensions are represented by a one-way digest and collapse into a fixed
overflow bucket after the configured cardinality limit.
"""

from __future__ import annotations

import hashlib
import math
import threading
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Callable

from .remote_scheduling import PollOutcome, WorkerLifecycle

REMOTE_OBSERVABILITY_SCHEMA_VERSION = 1
MAX_COUNTER = (1 << 63) - 1
MAX_GAUGE = (1 << 31) - 1
MAX_POOL_DIMENSIONS = 1024
MAX_DIMENSION_CHARS = 128

_OVERFLOW_POOL = "pool_overflow"
_LATENCY_BUCKETS_SECONDS = (0.01, 0.1, 1.0, 10.0, 60.0)


class CancellationObservation(StrEnum):
    REQUESTED = "requested"
    ACKNOWLEDGED = "acknowledged"
    COMPLETED = "completed"
    UNKNOWN = "unknown"


class HeartbeatObservation(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class RemoteMetricPoint:
    """One bounded metric value with fixed, sanitized dimensions."""

    name: str
    pool: str
    outcome: str
    value: int
    kind: str


@dataclass(frozen=True, slots=True)
class RemoteObservabilitySnapshot:
    """A non-authoritative point-in-time copy of operational observations."""

    schema_version: int
    generated_at: float
    points: tuple[RemoteMetricPoint, ...]
    tracked_pool_dimensions: int
    max_pool_dimensions: int
    dropped_dimensions: int
    invalid_observations: int
    delivery_semantics: str = "best_effort"
    execution_truth: bool = False


@dataclass(frozen=True, slots=True)
class RemoteExportReport:
    attempted: int
    failures: int
    delivery_semantics: str = "attempted"
    execution_truth: bool = False


class BoundedRemoteObservability:
    """Thread-safe fixed-cardinality counters and latency histograms."""

    def __init__(
        self,
        *,
        max_pool_dimensions: int = 64,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if (
            isinstance(max_pool_dimensions, bool)
            or not isinstance(max_pool_dimensions, int)
            or not 1 <= max_pool_dimensions <= MAX_POOL_DIMENSIONS
        ):
            raise ValueError(
                f"max_pool_dimensions must be between 1 and {MAX_POOL_DIMENSIONS}"
            )
        if not callable(clock):
            raise ValueError("clock must be callable")
        self.max_pool_dimensions = max_pool_dimensions
        self._clock = clock
        self._lock = threading.RLock()
        # Full digests, rather than caller-controlled labels, are retained.
        self._pool_labels: dict[str, str] = {}
        self._counters: dict[tuple[str, str, str], int] = {}
        self._gauges: dict[tuple[str, str, str], int] = {}
        self._dropped_dimensions = 0
        self._invalid_observations = 0

    def record_poll(self, pool_id: object, outcome: PollOutcome | str) -> None:
        with self._lock:
            pool = self._pool_label(pool_id)
            normalized = self._enum_value(outcome, PollOutcome)
            self._increment("poll", pool, normalized)

    def record_worker_registration(
        self,
        pool_id: object,
        lifecycle: WorkerLifecycle | str,
    ) -> None:
        with self._lock:
            pool = self._pool_label(pool_id)
            normalized = self._enum_value(lifecycle, WorkerLifecycle)
            self._increment("worker_registration", pool, normalized)

    def record_heartbeat(
        self,
        pool_id: object,
        outcome: HeartbeatObservation | str,
    ) -> None:
        with self._lock:
            pool = self._pool_label(pool_id)
            normalized = self._enum_value(outcome, HeartbeatObservation)
            self._increment("heartbeat", pool, normalized)

    def record_claim(
        self,
        pool_id: object,
        *,
        schedule_to_start_seconds: object,
    ) -> None:
        with self._lock:
            pool = self._pool_label(pool_id)
            self._increment("claim", pool, "claimed")
            duration = self._duration(schedule_to_start_seconds)
            if duration is None:
                return
            bounded_duration = min(duration, MAX_COUNTER / 1_000)
            milliseconds = min(
                MAX_COUNTER,
                max(0, int(round(bounded_duration * 1_000))),
            )
            self._increment(
                "schedule_to_start_count",
                pool,
                "observed",
            )
            self._increment(
                "schedule_to_start_sum_ms",
                pool,
                "observed",
                amount=milliseconds,
            )
            self._increment(
                "schedule_to_start_bucket",
                pool,
                self._latency_bucket(duration),
            )

    def record_lease_loss(self, pool_id: object) -> None:
        with self._lock:
            self._increment("lease_loss", self._pool_label(pool_id), "lost")

    def record_stale_commit(self, pool_id: object) -> None:
        with self._lock:
            self._increment(
                "stale_commit",
                self._pool_label(pool_id),
                "rejected",
            )

    def record_cancellation(
        self,
        pool_id: object,
        outcome: CancellationObservation | str,
    ) -> None:
        with self._lock:
            pool = self._pool_label(pool_id)
            normalized = self._enum_value(outcome, CancellationObservation)
            self._increment("cancellation", pool, normalized)

    def record_outcome_unknown(self, pool_id: object) -> None:
        with self._lock:
            self._increment(
                "outcome_unknown",
                self._pool_label(pool_id),
                "recorded",
            )

    def observe_queue_depth(self, pool_id: object, depth: object) -> None:
        with self._lock:
            pool = self._pool_label(pool_id)
            if isinstance(depth, bool) or not isinstance(depth, int) or depth < 0:
                self._mark_invalid()
                return
            self._gauges[("queue_depth", pool, "observed")] = min(
                depth,
                MAX_GAUGE,
            )

    def snapshot(self) -> RemoteObservabilitySnapshot:
        with self._lock:
            generated_at = self._safe_now()
            points = [
                RemoteMetricPoint(
                    name=name,
                    pool=pool,
                    outcome=outcome,
                    value=value,
                    kind="counter",
                )
                for (name, pool, outcome), value in self._counters.items()
            ]
            points.extend(
                RemoteMetricPoint(
                    name=name,
                    pool=pool,
                    outcome=outcome,
                    value=value,
                    kind="gauge",
                )
                for (name, pool, outcome), value in self._gauges.items()
            )
            points.sort(
                key=lambda point: (
                    point.name,
                    point.pool,
                    point.outcome,
                    point.kind,
                )
            )
            return RemoteObservabilitySnapshot(
                schema_version=REMOTE_OBSERVABILITY_SCHEMA_VERSION,
                generated_at=generated_at,
                points=tuple(points),
                tracked_pool_dimensions=len(self._pool_labels),
                max_pool_dimensions=self.max_pool_dimensions,
                dropped_dimensions=self._dropped_dimensions,
                invalid_observations=self._invalid_observations,
            )

    def export_snapshot(
        self,
        exporter: Callable[[RemoteMetricPoint], None],
    ) -> RemoteExportReport:
        """Attempt best-effort export without propagating exporter failures."""

        if not callable(exporter):
            raise ValueError("exporter must be callable")
        snapshot = self.snapshot()
        failures = 0
        for point in snapshot.points:
            try:
                exporter(point)
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                failures += 1
        return RemoteExportReport(
            attempted=len(snapshot.points),
            failures=failures,
        )

    def _pool_label(self, pool_id: object) -> str:
        if (
            not isinstance(pool_id, str)
            or not pool_id
            or len(pool_id) > MAX_DIMENSION_CHARS
            or any(ord(character) < 32 or ord(character) == 127 for character in pool_id)
        ):
            self._mark_invalid()
            return _OVERFLOW_POOL
        try:
            encoded_pool = pool_id.encode("utf-8")
        except UnicodeError:
            self._mark_invalid()
            return _OVERFLOW_POOL
        digest = hashlib.sha256(
            b"xagent.remote.pool.v1\0" + encoded_pool
        ).hexdigest()
        existing = self._pool_labels.get(digest)
        if existing is not None:
            return existing
        if len(self._pool_labels) >= self.max_pool_dimensions:
            self._dropped_dimensions = min(
                MAX_COUNTER,
                self._dropped_dimensions + 1,
            )
            return _OVERFLOW_POOL
        label = f"pool_{digest}"
        self._pool_labels[digest] = label
        return label

    def _increment(
        self,
        name: str,
        pool: str,
        outcome: str,
        *,
        amount: int = 1,
    ) -> None:
        key = (name, pool, outcome)
        self._counters[key] = min(
            MAX_COUNTER,
            self._counters.get(key, 0) + amount,
        )

    def _enum_value(self, value: object, enum_type: type[StrEnum]) -> str:
        try:
            return enum_type(value).value
        except Exception:
            self._mark_invalid()
            return "other"

    def _duration(self, value: object) -> float | None:
        if isinstance(value, bool):
            self._mark_invalid()
            return None
        try:
            duration = float(value)
        except Exception:
            self._mark_invalid()
            return None
        if not math.isfinite(duration) or duration < 0:
            self._mark_invalid()
            return None
        return duration

    @staticmethod
    def _latency_bucket(duration: float) -> str:
        for boundary in _LATENCY_BUCKETS_SECONDS:
            if duration <= boundary:
                return f"le_{boundary:g}s"
        return "gt_60s"

    def _mark_invalid(self) -> None:
        self._invalid_observations = min(
            MAX_COUNTER,
            self._invalid_observations + 1,
        )

    def _safe_now(self) -> float:
        try:
            now = float(self._clock())
        except Exception:
            self._mark_invalid()
            return 0.0
        if not math.isfinite(now) or now < 0:
            self._mark_invalid()
            return 0.0
        return now


__all__ = [
    "BoundedRemoteObservability",
    "CancellationObservation",
    "HeartbeatObservation",
    "RemoteExportReport",
    "RemoteMetricPoint",
    "RemoteObservabilitySnapshot",
]
