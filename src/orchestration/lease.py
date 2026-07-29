"""Durable lease recovery without exactly-once claims.

The reaper reads expired leases from SQLite on every pass.  It never assumes a
timeout means an external operation did not happen: started writes require a
trusted probe or move to ``WAITING_RECOVERY``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum
from typing import Callable, Protocol, runtime_checkable

from .artifacts import ArtifactRef
from .models import AttemptRecord, AttemptStatus, EventRecord
from .store import (
    DurableRunStore,
    IdempotencyConflictError,
    InvalidStateTransition,
    ProjectionConflictError,
)

MAX_RECOVERY_DELAY_SECONDS = 24 * 60 * 60


class LeaseRecoveryError(RuntimeError):
    """Recovery configuration or a trusted probe is malformed."""


class ProbeOutcome(StrEnum):
    COMMITTED = "committed"
    NOT_COMMITTED = "not_committed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ProbeResult:
    outcome: ProbeOutcome
    result_artifact_refs: tuple[ArtifactRef, ...] = ()
    external_operation_id_digest: str | None = None

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "outcome", ProbeOutcome(self.outcome))
        except ValueError as exc:
            raise LeaseRecoveryError("invalid probe outcome") from exc
        refs = tuple(self.result_artifact_refs)
        if len(refs) > 64 or any(not isinstance(ref, ArtifactRef) for ref in refs):
            raise LeaseRecoveryError("probe Artifact refs are invalid or unbounded")
        if self.outcome is ProbeOutcome.COMMITTED and not refs:
            raise LeaseRecoveryError(
                "a committed probe requires durable result Artifact refs"
            )
        if self.outcome is not ProbeOutcome.COMMITTED and refs:
            raise LeaseRecoveryError(
                "only a committed probe may return result Artifact refs"
            )
        object.__setattr__(self, "result_artifact_refs", refs)
        digest = self.external_operation_id_digest
        if digest is not None and (
            len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise LeaseRecoveryError(
                "external_operation_id_digest must be lowercase SHA-256"
            )

    def verified_receipt(self) -> dict:
        if self.outcome is not ProbeOutcome.COMMITTED:
            raise LeaseRecoveryError("only committed probes have verified receipts")
        return {
            "outcome": "succeeded",
            "verification": "verified",
            "artifact_refs": [ref.to_dict() for ref in self.result_artifact_refs],
            "external_operation_id_digest": self.external_operation_id_digest,
        }


@runtime_checkable
class ActivityProbe(Protocol):
    """Trusted effect-specific status probe; invoked outside store transactions."""

    def probe(self, attempt: AttemptRecord) -> ProbeResult: ...


@runtime_checkable
class ArtifactVerifier(Protocol):
    """Verify Artifact existence, digest, and size outside a store transaction."""

    def verify(self, ref: ArtifactRef) -> bool: ...


@dataclass(frozen=True, slots=True)
class RecoveryRetryPolicy:
    """Conservative retry decision supplied by the workflow/scheduler layer."""

    max_attempts: int = 1
    retry_delay_seconds: float = 0.0

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts < 1
            or self.max_attempts > 1_000
        ):
            raise LeaseRecoveryError("max_attempts must be a bounded positive integer")
        try:
            delay = float(self.retry_delay_seconds)
        except (TypeError, ValueError) as exc:
            raise LeaseRecoveryError("retry_delay_seconds must be finite") from exc
        if (
            not math.isfinite(delay)
            or delay < 0
            or delay > MAX_RECOVERY_DELAY_SECONDS
        ):
            raise LeaseRecoveryError("retry_delay_seconds exceeds the safe bound")
        object.__setattr__(self, "retry_delay_seconds", delay)

    def allows(self, attempt: AttemptRecord) -> bool:
        return attempt.attempt_number < self.max_attempts


@dataclass(frozen=True, slots=True)
class RecoveryAction:
    run_id: str
    node_id: str
    attempt_id: str
    resolution: str
    event: EventRecord


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    scanned: int
    resolved: tuple[RecoveryAction, ...]
    skipped_races: tuple[str, ...]
    runs_to_reconcile: tuple[str, ...]


class DurableLeaseReaper:
    """Resolve expired active Attempts from persisted lease state."""

    def __init__(
        self,
        store: DurableRunStore,
        *,
        retry_policy_resolver: Callable[
            [AttemptRecord], RecoveryRetryPolicy
        ] | None = None,
        probe_resolver: Callable[[AttemptRecord], ActivityProbe | None] | None = None,
        artifact_verifier: ArtifactVerifier | None = None,
    ) -> None:
        if not isinstance(store, DurableRunStore):
            raise TypeError("store must be a DurableRunStore")
        self.store = store
        self._retry_policy_resolver = (
            retry_policy_resolver
            if retry_policy_resolver is not None
            else lambda _attempt: RecoveryRetryPolicy()
        )
        if probe_resolver is not None and not callable(probe_resolver):
            raise LeaseRecoveryError("probe_resolver must be callable")
        if artifact_verifier is not None and not isinstance(
            artifact_verifier,
            ArtifactVerifier,
        ):
            raise LeaseRecoveryError("artifact_verifier does not implement verify")
        self._probe_resolver = probe_resolver
        self._artifact_verifier = artifact_verifier

    def run_once(
        self,
        *,
        now: float,
        limit: int = 100,
    ) -> RecoveryReport:
        current_time = float(now)
        if not math.isfinite(current_time) or current_time < 0:
            raise LeaseRecoveryError("now must be a finite timestamp")
        expired = self.store.list_expired_activity_leases(
            now=current_time,
            limit=limit,
        )
        resolved: list[RecoveryAction] = []
        skipped: list[str] = []
        run_ids: set[str] = set()
        for attempt, record in expired:
            resolution, retry_due_at, verified_result = self._resolution(
                attempt,
                now=current_time,
            )
            try:
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
                    now=current_time,
                )
            except (
                IdempotencyConflictError,
                InvalidStateTransition,
                ProjectionConflictError,
            ):
                # A renew, completion, cancellation, or another reaper won the
                # durable transaction. Re-read on the next pass.
                skipped.append(attempt.attempt_id)
                continue
            effective_resolution = event.payload.get("resolution")
            resolved.append(
                RecoveryAction(
                    run_id=attempt.run_id,
                    node_id=attempt.node_id,
                    attempt_id=attempt.attempt_id,
                    resolution=(
                        str(effective_resolution)
                        if isinstance(effective_resolution, str)
                        else resolution
                    ),
                    event=event,
                )
            )
            run_ids.add(attempt.run_id)
        return RecoveryReport(
            scanned=len(expired),
            resolved=tuple(resolved),
            skipped_races=tuple(sorted(skipped)),
            runs_to_reconcile=tuple(sorted(run_ids)),
        )

    def _resolution(
        self,
        attempt: AttemptRecord,
        *,
        now: float,
    ) -> tuple[str, float | None, dict | None]:
        policy = self._retry_policy_resolver(attempt)
        if not isinstance(policy, RecoveryRetryPolicy):
            raise LeaseRecoveryError(
                "retry_policy_resolver must return RecoveryRetryPolicy"
            )
        if attempt.status is AttemptStatus.CLAIMED:
            if not policy.allows(attempt):
                return "abandon_failed", None, None
            if policy.retry_delay_seconds == 0:
                return "abandon_ready", None, None
            return (
                "abandon_retry",
                now + policy.retry_delay_seconds,
                None,
            )
        if attempt.status is not AttemptStatus.RUNNING:
            raise LeaseRecoveryError("reaper received a non-active Attempt")

        if attempt.effect_class == "read_only":
            return self._retry_or_fail(attempt, policy, now=now)
        if attempt.effect_class == "idempotent_write":
            probe_result = self._probe(attempt)
            if probe_result.outcome is ProbeOutcome.COMMITTED:
                receipt = self._verified_receipt(probe_result)
                if receipt is None:
                    return "waiting_recovery", None, None
                return "verified_succeeded", None, receipt
            if probe_result.outcome is ProbeOutcome.NOT_COMMITTED:
                return self._retry_or_fail(attempt, policy, now=now)
            return "waiting_recovery", None, None
        # A non-idempotent or unknown write can have changed the outside world.
        # Timeout is not evidence of non-execution, so never retry automatically.
        return "waiting_recovery", None, None

    @staticmethod
    def _retry_or_fail(
        attempt: AttemptRecord,
        policy: RecoveryRetryPolicy,
        *,
        now: float,
    ) -> tuple[str, float | None, None]:
        if not policy.allows(attempt):
            return "abandon_failed", None, None
        return "abandon_retry", now + policy.retry_delay_seconds, None

    def _probe(self, attempt: AttemptRecord) -> ProbeResult:
        if self._probe_resolver is None:
            return ProbeResult(ProbeOutcome.UNKNOWN)
        try:
            probe = self._probe_resolver(attempt)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            return ProbeResult(ProbeOutcome.UNKNOWN)
        if probe is None or not isinstance(probe, ActivityProbe):
            return ProbeResult(ProbeOutcome.UNKNOWN)
        try:
            result = probe.probe(attempt)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            return ProbeResult(ProbeOutcome.UNKNOWN)
        if not isinstance(result, ProbeResult):
            return ProbeResult(ProbeOutcome.UNKNOWN)
        return result

    def _verified_receipt(self, result: ProbeResult) -> dict | None:
        verifier = self._artifact_verifier
        if verifier is None:
            return None
        try:
            for ref in result.result_artifact_refs:
                if verifier.verify(ref) is not True:
                    return None
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            return None
        return result.verified_receipt()


__all__ = [
    "ActivityProbe",
    "ArtifactVerifier",
    "DurableLeaseReaper",
    "LeaseRecoveryError",
    "ProbeOutcome",
    "ProbeResult",
    "RecoveryAction",
    "RecoveryReport",
    "RecoveryRetryPolicy",
]
