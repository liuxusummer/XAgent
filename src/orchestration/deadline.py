"""Restart-safe deadline convergence for durable orchestration.

The scanner owns no timers. It derives elapsed deadlines from SQLite
projections and idempotency leases on every pass, then uses fenced store
transactions to converge one Attempt at a time.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass

from .lease import (
    ActivityProbe,
    ArtifactVerifier,
    DurableLeaseReaper,
    RecoveryRetryPolicy,
)
from .models import AttemptRecord, AttemptStatus, EventRecord
from .store import (
    DurableRunStore,
    IdempotencyConflictError,
    InvalidStateTransition,
    ProjectionConflictError,
)


class DeadlineRecoveryError(RuntimeError):
    """Persisted deadline state cannot be conservatively resolved."""


@dataclass(frozen=True, slots=True)
class DeadlineAction:
    run_id: str
    node_id: str
    attempt_id: str
    timeout_kind: str
    resolution: str
    event: EventRecord


@dataclass(frozen=True, slots=True)
class DeadlineReport:
    scanned: int
    resolved: tuple[DeadlineAction, ...]
    skipped_races: tuple[str, ...]
    triggered_runs: tuple[str, ...]
    runs_to_reconcile: tuple[str, ...]


class DurableDeadlineScanner:
    """Scan durable absolute deadlines and fail closed after restarts."""

    def __init__(
        self,
        store: DurableRunStore,
        *,
        retry_policy_resolver: Callable[
            [AttemptRecord], RecoveryRetryPolicy
        ] | None = None,
        probe_resolver: Callable[[AttemptRecord], ActivityProbe | None]
        | None = None,
        artifact_verifier: ArtifactVerifier | None = None,
        propagation_hook: Callable[[str, float], None] | None = None,
    ) -> None:
        if not isinstance(store, DurableRunStore):
            raise TypeError("store must be a DurableRunStore")
        if propagation_hook is not None and not callable(propagation_hook):
            raise TypeError("propagation_hook must be callable")
        self.store = store
        self._retry_policy_resolver = (
            retry_policy_resolver
            if retry_policy_resolver is not None
            else lambda _attempt: RecoveryRetryPolicy()
        )
        self._effect_recovery = DurableLeaseReaper(
            store,
            retry_policy_resolver=self._retry_policy_resolver,
            probe_resolver=probe_resolver,
            artifact_verifier=artifact_verifier,
        )
        self._propagation_hook = propagation_hook

    def run_once(
        self,
        *,
        now: float,
        limit: int = 100,
    ) -> DeadlineReport:
        current_time = float(now)
        if not math.isfinite(current_time) or current_time < 0:
            raise DeadlineRecoveryError("now must be a finite timestamp")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise DeadlineRecoveryError("limit must be a positive integer")

        # Take the Attempt snapshot before recording Run cancellation intent.
        # This lets the same pass preserve timeout evidence for already-expired
        # work while the Run event prevents any new claims.
        expired = self.store.list_expired_activity_deadlines(
            now=current_time,
            limit=limit,
        )
        expired_runs = self.store.list_expired_run_deadlines(
            now=current_time,
            limit=limit,
        )
        triggered_runs: set[str] = set()
        skipped: list[str] = []
        for run in expired_runs:
            deadline = run.metadata.get("deadline_at")
            if isinstance(deadline, bool) or not isinstance(
                deadline,
                (int, float),
            ):
                continue
            try:
                event = self.store.mark_run_deadline_cancelling(
                    run.run_id,
                    deadline_at=float(deadline),
                    now=current_time,
                )
            except (
                IdempotencyConflictError,
                InvalidStateTransition,
                ProjectionConflictError,
            ):
                skipped.append(f"run:{run.run_id}")
                continue
            if event is not None:
                triggered_runs.add(run.run_id)
            if self._propagation_hook is not None:
                # The hook is intentionally independent of hierarchy storage.
                # It must be idempotent because a restarted scan can call it
                # again for a Run that remains CANCELLING.
                self._propagation_hook(run.run_id, float(deadline))

        resolved: list[DeadlineAction] = []
        run_ids: set[str] = set(triggered_runs)
        for attempt, record, timeout_kind, deadline_at in expired:
            resolution, retry_due_at, verified_result = self._resolution(
                attempt,
                now=current_time,
            )
            try:
                if attempt.status is AttemptStatus.SCHEDULED:
                    event = self.store.timeout_scheduled_activity(
                        attempt.run_id,
                        attempt.node_id,
                        attempt.attempt_id,
                        timeout_kind=timeout_kind,
                        deadline_at=deadline_at,
                        retry_due_at=retry_due_at,
                        now=current_time,
                    )
                else:
                    if record is None:
                        raise DeadlineRecoveryError(
                            "claimed deadline has no durable fencing record"
                        )
                    _completed, event = self.store.recover_expired_activity(
                        attempt.run_id,
                        attempt.node_id,
                        attempt.attempt_id,
                        record.request_hash,
                        record.owner_id,
                        claim_token=record.claim_token,
                        fencing_token=record.claim_count,
                        resolution=resolution,
                        retry_due_at=retry_due_at,
                        verified_result=verified_result,
                        expiry_reason="deadline",
                        timeout_kind=timeout_kind,
                        deadline_at=deadline_at,
                        now=current_time,
                    )
            except (
                IdempotencyConflictError,
                InvalidStateTransition,
                ProjectionConflictError,
            ):
                # Completion, renew, cancellation, or another scanner won.
                skipped.append(attempt.attempt_id)
                continue
            effective_resolution = event.payload.get("resolution")
            resolved.append(
                DeadlineAction(
                    run_id=attempt.run_id,
                    node_id=attempt.node_id,
                    attempt_id=attempt.attempt_id,
                    timeout_kind=timeout_kind,
                    resolution=(
                        str(effective_resolution)
                        if isinstance(effective_resolution, str)
                        else resolution
                    ),
                    event=event,
                )
            )
            run_ids.add(attempt.run_id)
        return DeadlineReport(
            scanned=len(expired),
            resolved=tuple(resolved),
            skipped_races=tuple(sorted(set(skipped))),
            triggered_runs=tuple(sorted(triggered_runs)),
            runs_to_reconcile=tuple(sorted(run_ids)),
        )

    def _resolution(
        self,
        attempt: AttemptRecord,
        *,
        now: float,
    ) -> tuple[str, float | None, dict | None]:
        if attempt.status is AttemptStatus.SCHEDULED:
            policy = self._retry_policy_resolver(attempt)
            if not isinstance(policy, RecoveryRetryPolicy):
                raise DeadlineRecoveryError(
                    "retry_policy_resolver must return RecoveryRetryPolicy"
                )
            if policy.allows(attempt):
                return (
                    "timeout_retry",
                    now + policy.retry_delay_seconds,
                    None,
                )
            return "timeout_failed", None, None
        if attempt.status not in {
            AttemptStatus.CLAIMED,
            AttemptStatus.RUNNING,
        }:
            raise DeadlineRecoveryError(
                "scanner received a non-active Attempt"
            )
        resolution, retry_due_at, verified_result = (
            self._effect_recovery._resolution(attempt, now=now)
        )
        mapped = {
            "abandon_ready": "timeout_retry",
            "abandon_retry": "timeout_retry",
            "abandon_failed": "timeout_failed",
        }.get(resolution, resolution)
        if mapped == "timeout_retry" and retry_due_at is None:
            retry_due_at = now
        return mapped, retry_due_at, verified_result
