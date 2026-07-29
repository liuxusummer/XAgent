"""Durable boundary for the existing, process-local Agent Loop."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any, Protocol

from .artifacts import ArtifactRef
from .models import (
    AttemptRecord,
    AttemptStatus,
    ClaimDisposition,
    IdempotencyClaim,
    IdempotencyRecord,
    NodeRecord,
    NodeStatus,
    RunRecord,
    RunStatus,
    normalize_json,
)

INLINE_RESULT_LIMIT = 16 * 1024
DEFAULT_INLINE_RESULT_LIMIT = 0
DEFAULT_MAX_ACTIVE_ATTEMPTS = 8
DEFAULT_WORKER_CAPACITY = 1
MAX_LEGACY_REQUEST_BYTES = 256 * 1024
WRITER_RESULT_LIMIT = 16 * 1024
_REQUEST_HASH_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
KNOWN_EXIT_REASONS = {
    "CURRENT_TASK_DONE",
    "INTERRUPTED",
    "EXITED",
    "MAX_TURNS_EXCEEDED",
    "ERROR",
}
TERMINAL_ATTEMPT_STATUSES = {
    AttemptStatus.SUCCEEDED,
    AttemptStatus.FAILED,
    AttemptStatus.TIMED_OUT,
    AttemptStatus.CANCELLED,
    AttemptStatus.ABANDONED,
    AttemptStatus.OUTCOME_UNKNOWN,
}


class LegacyRunStore(Protocol):
    """Store capabilities required by the adapter."""

    def get_run(self, run_id: str) -> RunRecord | None: ...

    def get_node(self, run_id: str, node_id: str) -> NodeRecord | None: ...

    def get_attempt(self, attempt_id: str) -> AttemptRecord | None: ...

    def get_idempotency(
        self,
        run_id: str,
        key: str,
    ) -> IdempotencyRecord | None: ...

    def append_event(
        self,
        run_id: str,
        event_type: str,
        *,
        payload: dict[str, Any] | None = None,
        node_id: str | None = None,
        attempt_id: str | None = None,
        event_id: str | None = None,
        run_projection: RunRecord | None = None,
        node_projection: NodeRecord | None = None,
        attempt_projection: AttemptRecord | None = None,
        expected_run_version: int | None = None,
        expected_node_version: int | None = None,
        expected_attempt_version: int | None = None,
    ) -> Any: ...

    def claim_activity(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        request_hash: str,
        owner_id: str,
        *,
        lease_seconds: float = 60.0,
        max_active_attempts: int,
        worker_capacity: int,
    ) -> tuple[IdempotencyClaim, Any | None]: ...

    def start_activity(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        owner_id: str,
        *,
        claim_token: str,
    ) -> Any: ...

    def complete_activity(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        request_hash: str,
        owner_id: str,
        *,
        claim_token: str,
        result: Any = None,
        event_payload: dict[str, Any] | None = None,
        attempt_status: AttemptStatus = AttemptStatus.SUCCEEDED,
        node_status: NodeStatus = NodeStatus.SUCCEEDED,
        run_status: RunStatus | None = None,
    ) -> tuple[Any, Any]: ...


LegacyRunner = Callable[..., Mapping[str, Any]]
ResultWriter = Callable[[dict[str, Any]], Mapping[str, Any]]


class LegacyActivityError(RuntimeError):
    """Base adapter failure."""


class LegacyRunNotFoundError(LegacyActivityError):
    """The orchestration Run does not exist."""


class LegacyProjectionNotFoundError(LegacyActivityError):
    """The pre-scheduled Node or Attempt does not exist."""


class ActivityClaimConflict(LegacyActivityError):
    """Another owner or a different request already claimed the operation."""


class ActivityRecoveryRequired(LegacyActivityError):
    """The Activity may have produced side effects and must not be replayed."""


class ResultPersistenceError(LegacyActivityError):
    """A successful result cannot be represented durably."""


class DurabilityError(LegacyActivityError):
    """A required lifecycle transition could not be persisted."""

    def __init__(
        self,
        event_type: str,
        cause: BaseException,
        *,
        activity_error: BaseException | None = None,
        recovery_error: BaseException | None = None,
    ) -> None:
        super().__init__(f"failed to persist {event_type}: {type(cause).__name__}")
        self.event_type = event_type
        self.cause = cause
        self.activity_error = activity_error
        self.recovery_error = recovery_error


class LegacyAgentLoopAdapter:
    """Execute one pre-scheduled legacy Agent Attempt.

    The scheduler must create a READY agent Node and SCHEDULED Attempt before
    calling this adapter. The adapter never creates durable schema objects and
    never depends on Web queues or sessions.

    Legacy Agent runs may execute many unreceipted tools. Consequently their
    default ``effect_class`` is ``non_idempotent_write`` and abnormal outcomes
    become ``OUTCOME_UNKNOWN`` instead of being retried blindly.

    ``max_active_attempts`` and ``worker_capacity`` must match the scheduler
    and worker limits governing the pre-scheduled Attempt. They are passed into
    the Store claim transaction rather than checked in process memory.
    """

    def __init__(
        self,
        store: LegacyRunStore,
        runner: LegacyRunner,
        *,
        result_writer: ResultWriter | None = None,
        artifact_verifier: Callable[[ArtifactRef], bool] | None = None,
        inline_result_limit: int = DEFAULT_INLINE_RESULT_LIMIT,
        max_active_attempts: int = DEFAULT_MAX_ACTIVE_ATTEMPTS,
        worker_capacity: int = DEFAULT_WORKER_CAPACITY,
    ) -> None:
        if artifact_verifier is not None and not callable(artifact_verifier):
            raise TypeError("artifact_verifier must be callable")
        self._store = store
        self._runner = runner
        self._result_writer = result_writer
        self._artifact_verifier = artifact_verifier
        self._inline_result_limit = max(
            0,
            min(INLINE_RESULT_LIMIT, int(inline_result_limit)),
        )
        self._max_active_attempts = _positive_capacity(
            max_active_attempts,
            "max_active_attempts",
        )
        self._worker_capacity = _positive_capacity(
            worker_capacity,
            "worker_capacity",
        )

    def run(
        self,
        run_id: str,
        task: str,
        *,
        node_id: str,
        attempt_id: str,
        owner_id: str | None = None,
        runner_kwargs: Mapping[str, Any] | None = None,
        request_hash: str | None = None,
        lease_seconds: float = 60.0,
        finalize_run: bool = False,
    ) -> dict[str, Any]:
        """Run one Attempt and return the legacy result mapping.

        ``finalize_run`` is deliberately false by default: an Agent Activity
        normally completes a Node, not its containing Workflow Run.
        """

        _require_text(run_id, "run_id")
        _require_text(node_id, "node_id")
        _require_text(attempt_id, "attempt_id")
        if not isinstance(task, str):
            raise TypeError("task must be a string")
        owner_id = _require_text(owner_id or attempt_id, "owner_id")
        runner_kwargs = dict(runner_kwargs or {})
        stable_hash = _validated_request_hash(
            task,
            runner_kwargs,
            request_hash=request_hash,
        )

        run, node, attempt = self._load_projections(run_id, node_id, attempt_id)
        self._validate_projection_identity(run_id, node_id, attempt)
        if finalize_run and node.metadata.get("root_node") is not True:
            raise LegacyActivityError(
                "finalize_run requires node metadata root_node=true"
            )

        recovered = self._recover_terminal_attempt(attempt)
        if recovered is not None:
            self._require_terminal_request_binding(
                run_id,
                attempt,
                stable_hash,
            )
            return recovered
        if attempt.status is AttemptStatus.OUTCOME_UNKNOWN:
            raise ActivityRecoveryRequired(
                f"attempt {attempt_id} has outcome unknown; explicit recovery is required"
            )
        if attempt.status is AttemptStatus.RUNNING:
            raise ActivityClaimConflict(
                f"attempt {attempt_id} is already running; it will not be replayed"
            )
        if attempt.status not in {AttemptStatus.SCHEDULED, AttemptStatus.CLAIMED}:
            raise LegacyActivityError(
                f"attempt {attempt_id} cannot start from {attempt.status.value}"
            )
        if node.status is not NodeStatus.READY:
            raise LegacyActivityError(
                f"node {node_id} must be ready, got {node.status.value}"
            )
        if run.status not in {RunStatus.CREATED, RunStatus.RUNNING}:
            raise LegacyActivityError(
                f"run {run_id} cannot start activity from {run.status.value}"
            )

        claim = self._claim(
            run_id,
            node_id,
            attempt_id,
            stable_hash,
            owner_id,
            lease_seconds,
        )
        if claim.disposition is ClaimDisposition.COMPLETED:
            return _recover_legacy_result(claim.record.result)
        if claim.disposition is ClaimDisposition.CONFLICT:
            raise ActivityClaimConflict(
                f"idempotency key for attempt {attempt_id} is already claimed"
            )

        self._persist_started(
            run_id,
            node_id,
            attempt_id,
            owner_id,
            claim.record.claim_token,
        )

        try:
            raw_result = self._runner(task, **runner_kwargs)
            result = _normalize_legacy_result(raw_result)
        except BaseException as exc:
            self._handle_runner_exception(
                run_id,
                node_id,
                attempt_id,
                finalize_run,
                stable_hash,
                owner_id,
                claim.record.claim_token,
                exc,
            )
            raise

        exit_reason = _safe_exit_reason(result.get("exit_reason"))
        if exit_reason == "CURRENT_TASK_DONE":
            try:
                node_result = self._build_success_node_result(result)
            except BaseException as exc:
                self._transition_outcome_unknown(
                    run_id,
                    node_id,
                    attempt_id,
                    reason="result_not_durable",
                    request_hash=stable_hash,
                    owner_id=owner_id,
                    claim_token=claim.record.claim_token,
                    cause=exc,
                )
                raise DurabilityError(
                    "attempt.result_not_durable",
                    exc,
                ) from exc
            self._persist_success(
                run_id,
                node_id,
                attempt_id,
                result,
                node_result,
                finalize_run,
                stable_hash,
                owner_id,
                claim.record.claim_token,
            )
            return result

        if attempt.effect_class != "read_only":
            self._transition_outcome_unknown(
                run_id,
                node_id,
                attempt_id,
                reason=f"legacy_exit_{exit_reason.lower()}",
                request_hash=stable_hash,
                owner_id=owner_id,
                claim_token=claim.record.claim_token,
            )
            raise ActivityRecoveryRequired(
                f"legacy Agent exited with {exit_reason}; side effects are not receipted"
            )

        self._persist_known_failure(
            run_id,
            node_id,
            attempt_id,
            exit_reason,
            finalize_run,
            stable_hash,
            owner_id,
            claim.record.claim_token,
        )
        return result

    def _load_projections(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
    ) -> tuple[RunRecord, NodeRecord, AttemptRecord]:
        try:
            run = self._store.get_run(run_id)
            node = self._store.get_node(run_id, node_id)
            attempt = self._store.get_attempt(attempt_id)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise DurabilityError("projection.read", exc) from exc
        if run is None:
            raise LegacyRunNotFoundError(f"durable run not found: {run_id}")
        if node is None:
            raise LegacyProjectionNotFoundError(f"durable node not found: {run_id}/{node_id}")
        if attempt is None:
            raise LegacyProjectionNotFoundError(f"durable attempt not found: {attempt_id}")
        return run, node, attempt

    @staticmethod
    def _validate_projection_identity(
        run_id: str,
        node_id: str,
        attempt: AttemptRecord,
    ) -> None:
        if attempt.run_id != run_id or attempt.node_id != node_id:
            raise LegacyActivityError("attempt does not belong to the requested run/node")
        if attempt.activity_kind != "agent":
            raise LegacyActivityError(
                f"legacy Agent adapter cannot execute {attempt.activity_kind!r} activity"
            )

    def _claim(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        request_hash: str,
        owner_id: str,
        lease_seconds: float,
    ) -> IdempotencyClaim:
        try:
            claim, _event = self._store.claim_activity(
                run_id,
                node_id,
                attempt_id,
                request_hash,
                owner_id,
                lease_seconds=lease_seconds,
                max_active_attempts=self._max_active_attempts,
                worker_capacity=self._worker_capacity,
            )
            return claim
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise DurabilityError("attempt.claim", exc) from exc

    def _require_terminal_request_binding(
        self,
        run_id: str,
        attempt: AttemptRecord,
        request_hash: str,
    ) -> None:
        try:
            record = self._store.get_idempotency(
                run_id,
                attempt.idempotency_key,
            )
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise DurabilityError("idempotency.read", exc) from exc
        if record is None:
            raise ActivityRecoveryRequired(
                f"terminal attempt {attempt.attempt_id} has no durable request binding"
            )
        if record.request_hash != request_hash:
            raise ActivityClaimConflict(
                f"terminal attempt {attempt.attempt_id} is bound to another request"
            )

    def _persist_started(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        owner_id: str,
        claim_token: str,
    ) -> None:
        try:
            self._store.start_activity(
                run_id,
                node_id,
                attempt_id,
                owner_id,
                claim_token=claim_token,
            )
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise DurabilityError("attempt.started", exc) from exc

    def _handle_runner_exception(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        finalize_run: bool,
        request_hash: str,
        owner_id: str,
        claim_token: str,
        exc: BaseException,
    ) -> None:
        attempt = self._store.get_attempt(attempt_id)
        if attempt is None:
            raise DurabilityError(
                "attempt.failed",
                LegacyProjectionNotFoundError(attempt_id),
                activity_error=exc,
            )
        if attempt.effect_class != "read_only":
            try:
                self._transition_outcome_unknown(
                    run_id,
                    node_id,
                    attempt_id,
                    reason="runner_exception",
                    request_hash=request_hash,
                    owner_id=owner_id,
                    claim_token=claim_token,
                    cause=exc,
                )
            except BaseException as recovery_exc:
                if isinstance(recovery_exc, (KeyboardInterrupt, SystemExit)):
                    raise
                raise DurabilityError(
                    "attempt.outcome_unknown",
                    recovery_exc,
                    activity_error=exc,
                ) from recovery_exc
            return
        try:
            self._persist_known_failure(
                run_id,
                node_id,
                attempt_id,
                "ERROR",
                finalize_run,
                request_hash,
                owner_id,
                claim_token,
                error_type=type(exc).__name__,
            )
        except BaseException as persist_exc:
            if isinstance(persist_exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise DurabilityError(
                "attempt.failed",
                persist_exc,
                activity_error=exc,
            ) from persist_exc

    def _persist_success(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        result: Mapping[str, Any],
        node_result: dict[str, Any],
        finalize_run: bool,
        request_hash: str,
        owner_id: str,
        claim_token: str,
    ) -> None:
        run, node, attempt = self._load_projections(run_id, node_id, attempt_id)
        if attempt.status is not AttemptStatus.RUNNING:
            raise ActivityClaimConflict(
                f"attempt {attempt_id} cannot complete from {attempt.status.value}"
            )
        event_type = "attempt.succeeded"
        try:
            self._store.complete_activity(
                run_id,
                node_id,
                attempt_id,
                request_hash,
                owner_id,
                claim_token=claim_token,
                result=node_result,
                event_payload=_result_summary(result, node_result),
                attempt_status=AttemptStatus.SUCCEEDED,
                node_status=NodeStatus.SUCCEEDED,
                run_status=RunStatus.COMPLETED if finalize_run else None,
            )
        except BaseException as cause:
            if isinstance(cause, (KeyboardInterrupt, SystemExit)):
                raise
            exc = DurabilityError(event_type, cause)
            fresh = self._store.get_attempt(attempt_id)
            if (
                fresh is not None
                and fresh.status is AttemptStatus.SUCCEEDED
                and fresh.result == node_result
            ):
                return
            try:
                self._transition_outcome_unknown(
                    run_id,
                    node_id,
                    attempt_id,
                    reason="terminal_commit_failed",
                    request_hash=request_hash,
                    owner_id=owner_id,
                    claim_token=claim_token,
                    cause=exc,
                )
            except BaseException as recovery_exc:
                raise DurabilityError(
                    event_type,
                    exc.cause,
                    recovery_error=recovery_exc,
                ) from exc
            raise exc from cause

    def _persist_known_failure(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        exit_reason: str,
        finalize_run: bool,
        request_hash: str,
        owner_id: str,
        claim_token: str,
        *,
        error_type: str = "",
    ) -> None:
        run, node, attempt = self._load_projections(run_id, node_id, attempt_id)
        cancelled = exit_reason == "INTERRUPTED"
        error = {
            "error_class": _bounded(error_type or "LegacyAgentExit", 160),
            "error_code": _safe_exit_reason(exit_reason),
        }
        node_result = _validated_node_result(
            {
                "schema_version": 1,
                "outcome": "cancelled" if cancelled else "failed",
                "output": None,
                "artifact_refs": [],
                "tool_receipt_refs": [],
                "metrics": {},
                **error,
            }
        )
        self._complete_activity(
            "attempt.cancelled" if cancelled else "attempt.failed",
            run_id,
            node_id,
            attempt_id,
            request_hash,
            owner_id,
            claim_token=claim_token,
            result=node_result,
            event_payload=error,
            attempt_status=(
                AttemptStatus.CANCELLED if cancelled else AttemptStatus.FAILED
            ),
            node_status=NodeStatus.CANCELLED if cancelled else NodeStatus.FAILED,
            run_status=(
                RunStatus.CANCELLED if cancelled else RunStatus.FAILED
            )
            if finalize_run
            else None,
        )

    def _transition_outcome_unknown(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        *,
        reason: str,
        request_hash: str,
        owner_id: str,
        claim_token: str,
        cause: BaseException | None = None,
    ) -> None:
        run, node, attempt = self._load_projections(run_id, node_id, attempt_id)
        if attempt.status is AttemptStatus.OUTCOME_UNKNOWN:
            return
        if attempt.status in TERMINAL_ATTEMPT_STATUSES:
            raise ActivityClaimConflict(
                f"attempt {attempt_id} already ended as {attempt.status.value}"
            )
        error = {
            "error_class": _bounded(type(cause).__name__ if cause else "OutcomeUnknown", 160),
            "error_code": _bounded(reason, 160),
        }
        self._complete_activity(
            "attempt.outcome_unknown",
            run_id,
            node_id,
            attempt_id,
            request_hash,
            owner_id,
            claim_token=claim_token,
            result=error,
            event_payload=error,
            attempt_status=AttemptStatus.OUTCOME_UNKNOWN,
            node_status=NodeStatus.WAITING_RECOVERY,
            run_status=RunStatus.WAITING_RECOVERY,
        )

    def _build_success_node_result(
        self,
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        normalized = _detach_json(result)
        encoded = _json_bytes(normalized)
        metrics = {
            "turns": _safe_turns(result.get("turns")),
            "response_len": len(result.get("response", ""))
            if isinstance(result.get("response"), str)
            else 0,
            "tool_result_count": len(result.get("tool_results", []))
            if isinstance(result.get("tool_results"), list)
            else 0,
        }
        if self._result_writer is not None:
            written = self._result_writer(normalized)
            persisted = normalize_json(dict(written), "result_writer output")
            if not isinstance(persisted, dict):
                raise ResultPersistenceError("result_writer must return a JSON object")
            if len(_json_bytes(persisted)) > WRITER_RESULT_LIMIT:
                raise ResultPersistenceError("result_writer output exceeds bounded metadata limit")
            if _looks_like_node_result(persisted):
                if persisted.get("outcome") != "succeeded":
                    raise ResultPersistenceError(
                        "result_writer returned mismatched NodeResult outcome"
                    )
                return _validated_node_result(
                    persisted,
                    artifact_verifier=self._artifact_verifier,
                )
            return _validated_node_result(
                {
                    "schema_version": 1,
                    "outcome": "succeeded",
                    "output": None,
                    "artifact_refs": [persisted],
                    "tool_receipt_refs": [],
                    "metrics": metrics,
                },
                artifact_verifier=self._artifact_verifier,
            )
        if self._inline_result_limit > 0 and len(encoded) <= self._inline_result_limit:
            return _validated_node_result(
                {
                    "schema_version": 1,
                    "outcome": "succeeded",
                    "output": {"legacy_result": normalized},
                    "artifact_refs": [],
                    "tool_receipt_refs": [],
                    "metrics": metrics,
                }
            )
        raise ResultPersistenceError(
            "legacy result requires an artifact writer; inline persistence is opt-in"
        )

    def _complete_activity(
        self,
        event_type: str,
        run_id: str,
        node_id: str,
        attempt_id: str,
        request_hash: str,
        owner_id: str,
        **kwargs: Any,
    ) -> None:
        try:
            self._store.complete_activity(
                run_id,
                node_id,
                attempt_id,
                request_hash,
                owner_id,
                **kwargs,
            )
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise DurabilityError(event_type, exc) from exc

    def _append(
        self,
        run_id: str,
        event_type: str,
        **kwargs: Any,
    ) -> Any:
        try:
            return self._store.append_event(run_id, event_type, **kwargs)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise DurabilityError(event_type, exc) from exc

    @staticmethod
    def _recover_terminal_attempt(attempt: AttemptRecord) -> dict[str, Any] | None:
        if attempt.status is AttemptStatus.SUCCEEDED:
            return _recover_legacy_result(attempt.result)
        if attempt.status in {AttemptStatus.FAILED, AttemptStatus.CANCELLED}:
            return _recover_legacy_result(attempt.result)
        return None


def run_legacy_agent_activity(
    store: LegacyRunStore,
    runner: LegacyRunner,
    run_id: str,
    task: str,
    *,
    node_id: str,
    attempt_id: str,
    owner_id: str | None = None,
    runner_kwargs: Mapping[str, Any] | None = None,
    request_hash: str | None = None,
    lease_seconds: float = 60.0,
    finalize_run: bool = False,
    result_writer: ResultWriter | None = None,
    artifact_verifier: Callable[[ArtifactRef], bool] | None = None,
    inline_result_limit: int = DEFAULT_INLINE_RESULT_LIMIT,
    max_active_attempts: int = DEFAULT_MAX_ACTIVE_ATTEMPTS,
    worker_capacity: int = DEFAULT_WORKER_CAPACITY,
) -> dict[str, Any]:
    """Functional convenience wrapper."""

    return LegacyAgentLoopAdapter(
        store,
        runner,
        result_writer=result_writer,
        artifact_verifier=artifact_verifier,
        inline_result_limit=inline_result_limit,
        max_active_attempts=max_active_attempts,
        worker_capacity=worker_capacity,
    ).run(
        run_id,
        task,
        node_id=node_id,
        attempt_id=attempt_id,
        owner_id=owner_id,
        runner_kwargs=runner_kwargs,
        request_hash=request_hash,
        lease_seconds=lease_seconds,
        finalize_run=finalize_run,
    )


def _normalize_legacy_result(result: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        raise TypeError(f"legacy runner must return a mapping, got {type(result)!r}")
    normalized = dict(result)
    if not isinstance(normalized.get("exit_reason"), str):
        raise ValueError("legacy runner result must include exit_reason")
    return normalized


def _safe_exit_reason(value: Any) -> str:
    text = str(value or "")
    return text if text in KNOWN_EXIT_REASONS else "UNKNOWN"


def _validated_request_hash(
    task: str,
    runner_kwargs: Mapping[str, Any],
    *,
    request_hash: str | None,
) -> str:
    if request_hash is not None:
        if not isinstance(request_hash, str) or _REQUEST_HASH_PATTERN.fullmatch(
            request_hash
        ) is None:
            raise LegacyActivityError(
                "request_hash must be a lowercase SHA-256 hexadecimal digest"
            )
        return request_hash
    return _request_hash(task, runner_kwargs)


def _request_hash(task: str, runner_kwargs: Mapping[str, Any]) -> str:
    request = {
        "schema": "legacy_activity_request_v1",
        "task": task,
        "runner_kwargs": dict(runner_kwargs),
    }
    try:
        encoded = json.dumps(
            request,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise LegacyActivityError(
            "task and runner_kwargs must be canonical JSON"
        ) from exc
    if len(encoded) > MAX_LEGACY_REQUEST_BYTES:
        raise LegacyActivityError(
            "task and runner_kwargs exceed the bounded request size"
        )
    return hashlib.sha256(encoded).hexdigest()


def _detach_json(value: Any) -> dict[str, Any]:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        detached = json.loads(encoded)
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ResultPersistenceError("legacy result is not JSON serializable") from exc
    if not isinstance(detached, dict):
        raise ResultPersistenceError("legacy result must be a JSON object")
    return detached


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _validated_node_result(
    value: Mapping[str, Any],
    *,
    artifact_verifier: Callable[[ArtifactRef], bool] | None = None,
) -> dict[str, Any]:
    normalized = normalize_json(dict(value), "NodeResult")
    if not isinstance(normalized, dict):
        raise ResultPersistenceError("NodeResult must be a JSON object")
    required = {"schema_version", "outcome", "output", "artifact_refs", "tool_receipt_refs", "metrics"}
    if not required.issubset(normalized):
        missing = sorted(required.difference(normalized))
        raise ResultPersistenceError(f"NodeResult missing fields: {missing}")
    if normalized.get("schema_version") != 1:
        raise ResultPersistenceError("unsupported NodeResult schema_version")
    if normalized.get("outcome") not in {
        "succeeded",
        "failed",
        "waiting_input",
        "waiting_approval",
        "cancelled",
    }:
        raise ResultPersistenceError("invalid NodeResult outcome")
    for field in ("artifact_refs", "tool_receipt_refs"):
        refs = normalized.get(field)
        if not isinstance(refs, list) or len(refs) > 64:
            raise ResultPersistenceError(
                f"NodeResult {field} must be a bounded list"
            )
        normalized[field] = [
            _verified_artifact_ref(
                ref,
                artifact_verifier=artifact_verifier,
            )
            for ref in refs
        ]
    return normalized


def _looks_like_node_result(value: Mapping[str, Any]) -> bool:
    return "schema_version" in value and "outcome" in value


def _verified_artifact_ref(
    value: Any,
    *,
    artifact_verifier: Callable[[ArtifactRef], bool] | None,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ResultPersistenceError("ArtifactRef must be a JSON object")
    normalized = normalize_json(dict(value), "ArtifactRef")
    if not isinstance(normalized, dict):
        raise ResultPersistenceError("ArtifactRef must be a JSON object")
    try:
        ref = ArtifactRef.from_dict(normalized)
        canonical = ref.to_dict()
    except (TypeError, ValueError) as exc:
        raise ResultPersistenceError("ArtifactRef is invalid") from exc
    if set(normalized) != set(canonical) or normalized != canonical:
        raise ResultPersistenceError(
            "ArtifactRef must contain the complete canonical schema"
        )
    if artifact_verifier is None:
        raise ResultPersistenceError(
            "ArtifactRef persistence requires an artifact_verifier"
        )
    try:
        verified = artifact_verifier(ref)
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        raise ResultPersistenceError("ArtifactRef verification failed") from exc
    if verified is not True:
        raise ResultPersistenceError("Artifact verifier rejected a reference")
    return canonical


def _recover_legacy_result(node_result: Any) -> dict[str, Any]:
    if not isinstance(node_result, Mapping):
        raise ActivityRecoveryRequired("durable attempt has no recoverable NodeResult")
    output = node_result.get("output")
    if isinstance(output, Mapping) and isinstance(output.get("legacy_result"), Mapping):
        return dict(output["legacy_result"])
    outcome = node_result.get("outcome")
    error_code = _safe_exit_reason(node_result.get("error_code"))
    if outcome in {"failed", "cancelled"}:
        return {
            "response": "",
            "exit_reason": error_code if error_code != "UNKNOWN" else "ERROR",
            "tool_results": [],
            "turns": _safe_turns(
                node_result.get("metrics", {}).get("turns")
                if isinstance(node_result.get("metrics"), Mapping)
                else None
            ),
            "durable_node_result": dict(node_result),
            "recovered": True,
        }
    return {
        "response": "",
        "exit_reason": "CURRENT_TASK_DONE",
        "tool_results": [],
        "turns": _safe_turns(
            node_result.get("metrics", {}).get("turns")
            if isinstance(node_result.get("metrics"), Mapping)
            else None
        ),
        "durable_node_result": dict(node_result),
        "recovered": True,
    }


def _result_summary(
    result: Mapping[str, Any],
    node_result: Mapping[str, Any],
) -> dict[str, Any]:
    artifacts = node_result.get("artifact_refs")
    artifact_ids = []
    if isinstance(artifacts, list):
        artifact_ids = [
            str(item.get("artifact_id"))
            for item in artifacts[:16]
            if isinstance(item, Mapping) and item.get("artifact_id")
        ]
    return {
        "exit_reason": _safe_exit_reason(result.get("exit_reason")),
        "response_len": len(result.get("response", ""))
        if isinstance(result.get("response"), str)
        else 0,
        "tool_result_count": len(result.get("tool_results", []))
        if isinstance(result.get("tool_results"), list)
        else 0,
        "turns": _safe_turns(result.get("turns")),
        "artifact_ids": artifact_ids,
    }


def _event_id(attempt_id: str, stage: str) -> str:
    digest = hashlib.sha256(f"{attempt_id}:{stage}".encode("utf-8")).hexdigest()
    return f"legacy_{digest}"


def _callable_name(callback: Callable[..., Any]) -> str:
    name = getattr(callback, "__qualname__", None) or getattr(callback, "__name__", None)
    return _bounded(str(name or type(callback).__name__), 160)


def _safe_turns(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _bounded(value: Any, limit: int) -> str:
    return str(value or "")[:limit]


def _require_text(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} must be a non-empty string")
    return text


def _positive_capacity(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value
