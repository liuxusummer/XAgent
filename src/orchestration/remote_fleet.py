"""Bounded composition of remote routing projection and durable Run claims.

``RemoteFleetCoordinator`` never owns execution truth.  It reserves ephemeral
worker capacity with :mod:`remote_scheduling`, then asks an injected trusted
callback to create the authoritative Run-scoped ``WorkAssignment``.  Routing
or observability state is never allowed to complete, reject, or otherwise
advance a Store record.
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from typing import Callable, Sequence

from .remote_observability import (
    BoundedRemoteObservability,
    HeartbeatObservation,
)
from .remote_protocol import WorkAssignment
from .remote_scheduling import (
    AdmissionDecision,
    AdmissionOutcome,
    DeterministicRemoteScheduler,
    FleetAdmissionScope,
    PollOutcome,
    ReleaseOutcome,
    RemoteAssignment,
    RemoteTask,
    TouchOutcome,
    WorkerDescriptor,
    WorkerLifecycle,
    WorkerSnapshot,
    WorkerSweepReport,
)
from .store import FleetShardOwnership

MAX_FLEET_TASK_BINDINGS = 1_000_000
MAX_FLEET_WORKERS = 4096
MAX_RUN_ID_CHARS = 255
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FLEET_OWNER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")

ClaimRun = Callable[..., WorkAssignment]
SchedulerFactory = Callable[[], DeterministicRemoteScheduler]


class RemoteFleetError(RuntimeError):
    """Base error for the non-authoritative remote fleet projection."""


class RemoteFleetValidationError(RemoteFleetError, ValueError):
    """Fleet input is malformed or exceeds a hard bound."""


class RemoteFleetConflict(RemoteFleetError):
    """Routing state conflicts with trusted durable claim state."""


class RemoteFleetClaimError(RemoteFleetError):
    """A trusted durable claim callback failed after routing reservation."""

    def __init__(self, code: str = "durable_claim_failed") -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class FleetTaskBinding:
    """Caller-provided Store projection for one ready routing task."""

    run_id: str
    task: RemoteTask
    node_id: str | None = None
    activity_config_digest: str | None = None
    routing_policy_digest: str | None = None
    shard_ownership: FleetShardOwnership | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _run_id(self.run_id))
        if not isinstance(self.task, RemoteTask):
            raise RemoteFleetValidationError("task must be a RemoteTask")
        if (self.node_id is None) != (
            self.activity_config_digest is None
        ):
            raise RemoteFleetValidationError(
                "exact Fleet binding requires node_id and config digest"
            )
        if self.node_id is not None:
            object.__setattr__(self, "node_id", _run_id(self.node_id))
            if (
                not isinstance(self.activity_config_digest, str)
                or _DIGEST.fullmatch(self.activity_config_digest) is None
            ):
                raise RemoteFleetValidationError(
                    "activity_config_digest must be a SHA-256 digest"
                )
        if (
            self.routing_policy_digest is not None
            and (
                not isinstance(self.routing_policy_digest, str)
                or _DIGEST.fullmatch(self.routing_policy_digest) is None
            )
        ):
            raise RemoteFleetValidationError(
                "routing_policy_digest must be a SHA-256 digest"
            )
        if (
            self.shard_ownership is not None
            and not isinstance(
                self.shard_ownership,
                FleetShardOwnership,
            )
        ):
            raise RemoteFleetValidationError(
                "shard_ownership must be durable Fleet authority"
            )

    @property
    def exact_for_production(self) -> bool:
        return (
            self.node_id is not None
            and self.activity_config_digest is not None
            and self.routing_policy_digest is not None
        )

    @property
    def exact_for_multi_control(self) -> bool:
        return (
            self.exact_for_production
            and self.shard_ownership is not None
            and self.shard_ownership.tenant_id == self.task.tenant_id
            and self.shard_ownership.pool_id == self.task.pool_id
        )


@dataclass(frozen=True, slots=True)
class FleetAssignment:
    """Ephemeral routing reservation bound to one real durable assignment."""

    routing: RemoteAssignment
    durable: WorkAssignment

    def __post_init__(self) -> None:
        if not isinstance(self.routing, RemoteAssignment):
            raise RemoteFleetValidationError(
                "routing must be a RemoteAssignment"
            )
        if not isinstance(self.durable, WorkAssignment):
            raise RemoteFleetValidationError(
                "durable must be a WorkAssignment"
            )


@dataclass(frozen=True, slots=True)
class FleetRebuildReport:
    workers: int
    tasks: int
    execution_truth: bool = False


@dataclass(frozen=True, slots=True)
class FleetSnapshot:
    worker_count: int
    queued_tasks: int
    active_assignments: int
    task_bindings: int
    inflight_durable_claims: int
    execution_truth: bool = False


class RemoteFleetCoordinator:
    """Thread-safe bounded bridge from fleet routing to durable Run claims."""

    def __init__(
        self,
        scheduler_factory: SchedulerFactory,
        claim_run: ClaimRun,
        *,
        max_task_bindings: int = 4096,
        observability: BoundedRemoteObservability | object | None = None,
        require_durable_ownership: bool = False,
        fleet_owner_id: str | None = None,
    ) -> None:
        if not callable(scheduler_factory):
            raise RemoteFleetValidationError("scheduler_factory must be callable")
        if not callable(claim_run):
            raise RemoteFleetValidationError("claim_run must be callable")
        if (
            isinstance(max_task_bindings, bool)
            or not isinstance(max_task_bindings, int)
            or not 1 <= max_task_bindings <= MAX_FLEET_TASK_BINDINGS
        ):
            raise RemoteFleetValidationError(
                "max_task_bindings exceeds the bounded fleet registry"
            )
        scheduler = scheduler_factory()
        if not isinstance(scheduler, DeterministicRemoteScheduler):
            raise RemoteFleetValidationError(
                "scheduler_factory must return DeterministicRemoteScheduler"
            )
        self._scheduler_factory = scheduler_factory
        self._claim_run = claim_run
        self.max_task_bindings = max_task_bindings
        if not isinstance(require_durable_ownership, bool):
            raise RemoteFleetValidationError(
                "require_durable_ownership must be a bool"
            )
        if fleet_owner_id is not None and (
            not isinstance(fleet_owner_id, str)
            or _FLEET_OWNER_ID.fullmatch(fleet_owner_id) is None
        ):
            raise RemoteFleetValidationError(
                "fleet_owner_id must be a bounded Fleet identifier"
            )
        if require_durable_ownership and fleet_owner_id is None:
            raise RemoteFleetValidationError(
                "strict multi-control Fleet requires fleet_owner_id"
            )
        self.require_durable_ownership = require_durable_ownership
        self.fleet_owner_id = fleet_owner_id
        self._observability = observability
        self._lock = threading.RLock()
        self._scheduler = scheduler
        self._task_bindings: dict[str, FleetTaskBinding] = {}
        self._inflight_durable_claims = 0

    @property
    def production_security_ready(self) -> bool:
        """Whether the durable claim callback is explicitly trusted."""

        try:
            callback_ready = (
                getattr(
                    self._claim_run,
                    "production_security_ready",
                    None,
                )
                is True
                and getattr(
                    self._claim_run,
                    "durable_fleet_admission_ready",
                    None,
                )
                is True
            )
            with self._lock:
                uses_ownership = (
                    self.require_durable_ownership
                    or any(
                        binding.shard_ownership is not None
                        for binding in self._task_bindings.values()
                    )
                )
                if uses_ownership:
                    callback_ready = (
                        callback_ready
                        and getattr(
                            self._claim_run,
                            "durable_fleet_ownership_ready",
                            None,
                        )
                        is True
                        and getattr(
                            self._claim_run,
                            "fleet_owner_id",
                            None,
                        )
                        == self.fleet_owner_id
                    )
                bindings_ready = all(
                    (
                        binding.exact_for_production
                        and (
                            (
                                binding.shard_ownership is not None
                                and binding.exact_for_multi_control
                                and binding.shard_ownership.owner_id
                                == self.fleet_owner_id
                            )
                            if (
                                self.require_durable_ownership
                                or binding.shard_ownership is not None
                            )
                            else True
                        )
                    )
                    for binding in self._task_bindings.values()
                )
            return callback_ready and bindings_ready
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return False

    @property
    def production_multi_control_ready(self) -> bool:
        return (
            self.require_durable_ownership
            and self.production_security_ready
        )

    def register_worker(self, descriptor: WorkerDescriptor) -> WorkerSnapshot:
        with self._lock:
            snapshot = self._scheduler.register_worker(descriptor)
        self._observe(
            "record_worker_registration",
            snapshot.descriptor.pool_id,
            snapshot.lifecycle,
        )
        return snapshot

    def touch_worker(
        self,
        worker_id: str,
        *,
        worker_generation: int,
        session_id: str,
    ) -> TouchOutcome:
        with self._lock:
            before = self._scheduler.worker_snapshot(worker_id)
            outcome = self._scheduler.touch_worker(
                worker_id,
                worker_generation=worker_generation,
                session_id=session_id,
            )
        pool_id = "unknown"
        if before is not None:
            pool_id = before.descriptor.pool_id
        self._observe(
            "record_heartbeat",
            pool_id,
            HeartbeatObservation.ACCEPTED
            if outcome is TouchOutcome.ACCEPTED
            else HeartbeatObservation.REJECTED,
        )
        return outcome

    def drain_worker(self, worker_id: str) -> WorkerSnapshot:
        with self._lock:
            snapshot = self._scheduler.request_drain(worker_id)
        self._observe(
            "record_worker_registration",
            snapshot.descriptor.pool_id,
            WorkerLifecycle.DRAINING,
        )
        return snapshot

    def sweep_workers(self) -> WorkerSweepReport:
        with self._lock:
            before = {
                worker.descriptor.worker_id: worker
                for worker in self._scheduler.snapshot().workers
            }
            report = self._scheduler.sweep_idle_workers()
            after = {
                worker.descriptor.worker_id: worker
                for worker in self._scheduler.snapshot().workers
            }
        for worker_id, previous in before.items():
            current = after.get(worker_id)
            if current is None or (
                previous.lifecycle is not WorkerLifecycle.EXPIRED
                and current.lifecycle is WorkerLifecycle.EXPIRED
            ):
                self._observe(
                    "record_worker_registration",
                    previous.descriptor.pool_id,
                    WorkerLifecycle.EXPIRED,
                )
        return report

    def admit(self, binding: FleetTaskBinding) -> AdmissionDecision:
        if not isinstance(binding, FleetTaskBinding):
            raise RemoteFleetValidationError(
                "binding must be a FleetTaskBinding"
            )
        with self._lock:
            existing = self._task_bindings.get(binding.task.task_id)
            if existing is not None and existing != binding:
                raise RemoteFleetConflict(
                    "task_id is bound to another Run or durable candidate"
                )
            if (
                existing is None
                and len(self._task_bindings) >= self.max_task_bindings
            ):
                raise RemoteFleetConflict("fleet task binding registry is full")
            decision = self._scheduler.admit(binding.task)
            if decision.outcome is AdmissionOutcome.ADMITTED:
                self._task_bindings[binding.task.task_id] = binding
            elif decision.outcome is AdmissionOutcome.DUPLICATE:
                if existing is None:
                    raise RemoteFleetConflict(
                        "routing task exists without a trusted Run binding"
                    )
            elif existing is None:
                self._task_bindings.pop(binding.task.task_id, None)
            queue_depth = self._scheduler.queue_depth(
                pool_id=binding.task.pool_id
            )
        self._observe(
            "observe_queue_depth",
            binding.task.pool_id,
            queue_depth,
        )
        return decision

    def assign_next(
        self,
        worker_id: str,
        *,
        worker_generation: int,
        session_id: str,
    ) -> FleetAssignment | None:
        """Reserve capacity, then obtain and validate the durable assignment."""

        with self._lock:
            scheduler = self._scheduler
            decision = scheduler.poll_and_claim(
                worker_id,
                worker_generation=worker_generation,
                session_id=session_id,
            )
            worker = scheduler.worker_snapshot(worker_id)
            pool_id = (
                worker.descriptor.pool_id if worker is not None else "unknown"
            )
            if decision.outcome is not PollOutcome.CLAIMED:
                routing = None
                run_id = None
            else:
                routing = decision.assignment
                assert routing is not None
                binding = self._task_bindings.get(routing.task.task_id)
                if binding is None:
                    self._rollback_routing(scheduler, routing)
                    raise RemoteFleetConflict(
                        "routing assignment has no trusted Run binding"
                    )
                admission_scope = (
                    scheduler.durable_admission_scope(
                        routing.task,
                        routing_policy_digest=(
                            binding.routing_policy_digest
                        ),
                    )
                    if binding.routing_policy_digest is not None
                    else None
                )
                run_id = binding.run_id
                shard_ownership = binding.shard_ownership
                if (
                    shard_ownership is not None
                    and shard_ownership.owner_id != self.fleet_owner_id
                ):
                    self._rollback_routing(scheduler, routing)
                    raise RemoteFleetConflict(
                        "Fleet shard belongs to another control owner"
                    )
                self._inflight_durable_claims += 1
        self._observe("record_poll", pool_id, decision.outcome)
        if routing is None:
            return None
        assert run_id is not None

        try:
            try:
                if (
                    getattr(
                        self._claim_run,
                        "production_security_ready",
                        None,
                    )
                    is True
                ):
                    if (
                        getattr(
                            self._claim_run,
                            "durable_fleet_admission_ready",
                            None,
                        )
                        is not True
                    ):
                        raise RemoteFleetConflict(
                            "production claim callback lacks durable Fleet admission"
                        )
                    if not isinstance(
                        admission_scope,
                        FleetAdmissionScope,
                    ):
                        raise RemoteFleetConflict(
                            "production Fleet claim lacks durable admission scope"
                        )
                    if (
                        self.require_durable_ownership
                        and shard_ownership is None
                    ):
                        raise RemoteFleetConflict(
                            "multi-control Fleet claim lacks shard ownership"
                        )
                    if shard_ownership is None:
                        durable = self._claim_run(
                            binding,
                            worker_id,
                            routing.worker_session_id,
                            admission_scope,
                        )
                    else:
                        if (
                            getattr(
                                self._claim_run,
                                "durable_fleet_ownership_ready",
                                None,
                            )
                            is not True
                        ):
                            raise RemoteFleetConflict(
                                "claim callback lacks Fleet ownership fencing"
                            )
                        durable = self._claim_run(
                            binding,
                            worker_id,
                            routing.worker_session_id,
                            admission_scope,
                            shard_ownership,
                        )
                else:
                    durable = self._claim_run(run_id, worker_id)
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                self._claim_failed(scheduler, routing, expected_run_id=run_id)
                raise RemoteFleetClaimError() from exc

            if not self._durable_matches(routing, durable, binding):
                self._claim_failed(scheduler, routing, expected_run_id=run_id)
                raise RemoteFleetConflict(
                    "durable assignment conflicts with routing projection"
                )
        finally:
            # ``rebuild`` treats this counter as a generation barrier.  Keep
            # the callback inflight until validation and any rollback have
            # both finished, so an old projection cannot clean up a newly
            # installed task binding.
            with self._lock:
                self._inflight_durable_claims -= 1
        self._observe(
            "record_claim",
            routing.task.pool_id,
            schedule_to_start_seconds=routing.schedule_to_start_seconds,
        )
        self._observe(
            "observe_queue_depth",
            routing.task.pool_id,
            scheduler.queue_depth(pool_id=routing.task.pool_id),
        )
        return FleetAssignment(routing=routing, durable=durable)

    def release_terminal(self, assignment: FleetAssignment) -> ReleaseOutcome:
        return self._release_authoritative(assignment)

    def release_rejected(self, assignment: FleetAssignment) -> ReleaseOutcome:
        return self._release_authoritative(assignment)

    def rebuild(
        self,
        tasks: Sequence[FleetTaskBinding],
        workers: Sequence[WorkerDescriptor],
    ) -> FleetRebuildReport:
        """Atomically rebuild from caller-supplied Store/identity projections."""

        if not isinstance(tasks, Sequence) or isinstance(tasks, (str, bytes)):
            raise RemoteFleetValidationError("tasks must be a bounded sequence")
        if not isinstance(workers, Sequence) or isinstance(workers, (str, bytes)):
            raise RemoteFleetValidationError("workers must be a bounded sequence")
        if len(tasks) > self.max_task_bindings:
            raise RemoteFleetValidationError("tasks exceed max_task_bindings")
        if len(workers) > MAX_FLEET_WORKERS:
            raise RemoteFleetValidationError("workers exceed fleet registry bound")
        with self._lock:
            if self._inflight_durable_claims:
                raise RemoteFleetConflict(
                    "cannot rebuild during durable claim callbacks"
                )
            if self._scheduler.snapshot().active_assignments:
                raise RemoteFleetConflict(
                    "cannot rebuild over active routing assignments"
                )
        candidate = self._scheduler_factory()
        if not isinstance(candidate, DeterministicRemoteScheduler):
            raise RemoteFleetValidationError(
                "scheduler_factory must return DeterministicRemoteScheduler"
            )
        task_bindings: dict[str, FleetTaskBinding] = {}
        worker_ids: set[str] = set()
        for descriptor in workers:
            if not isinstance(descriptor, WorkerDescriptor):
                raise RemoteFleetValidationError(
                    "workers must contain WorkerDescriptor values"
                )
            if descriptor.worker_id in worker_ids:
                raise RemoteFleetConflict("duplicate worker in rebuild projection")
            candidate.register_worker(descriptor)
            worker_ids.add(descriptor.worker_id)
        for binding in tasks:
            if not isinstance(binding, FleetTaskBinding):
                raise RemoteFleetValidationError(
                    "tasks must contain FleetTaskBinding values"
                )
            task_id = binding.task.task_id
            existing = task_bindings.get(task_id)
            if existing is not None:
                raise RemoteFleetConflict("duplicate task in rebuild projection")
            decision = candidate.admit(binding.task)
            if decision.outcome is not AdmissionOutcome.ADMITTED:
                raise RemoteFleetConflict(
                    "rebuild projection exceeds scheduler admission bounds"
                )
            task_bindings[task_id] = binding

        with self._lock:
            if self._inflight_durable_claims:
                raise RemoteFleetConflict(
                    "cannot rebuild during durable claim callbacks"
                )
            if self._scheduler.snapshot().active_assignments:
                raise RemoteFleetConflict(
                    "cannot rebuild over active routing assignments"
                )
            self._scheduler = candidate
            self._task_bindings = task_bindings
        for descriptor in workers:
            self._observe(
                "record_worker_registration",
                descriptor.pool_id,
                WorkerLifecycle.ACTIVE,
            )
        return FleetRebuildReport(workers=len(workers), tasks=len(tasks))

    def snapshot(self) -> FleetSnapshot:
        with self._lock:
            routing = self._scheduler.snapshot()
            return FleetSnapshot(
                worker_count=len(routing.workers),
                queued_tasks=routing.queued_tasks,
                active_assignments=routing.active_assignments,
                task_bindings=len(self._task_bindings),
                inflight_durable_claims=self._inflight_durable_claims,
            )

    def _release_authoritative(
        self,
        assignment: FleetAssignment,
    ) -> ReleaseOutcome:
        if not isinstance(assignment, FleetAssignment):
            raise RemoteFleetValidationError(
                "assignment must be a FleetAssignment"
            )
        routing = assignment.routing
        with self._lock:
            binding = self._task_bindings.get(routing.task.task_id)
            if (
                binding is None
                or not self._durable_matches(
                    routing,
                    assignment.durable,
                    binding,
                )
            ):
                raise RemoteFleetConflict(
                    "terminal assignment conflicts with trusted Run binding"
                )
            outcome = self._scheduler.release(
                routing.assignment_id,
                routing.worker_id,
                worker_generation=routing.worker_generation,
                session_id=routing.worker_session_id,
            )
            if outcome is ReleaseOutcome.RELEASED:
                self._task_bindings.pop(routing.task.task_id, None)
            queue_depth = self._scheduler.queue_depth(
                pool_id=routing.task.pool_id
            )
        if outcome is not ReleaseOutcome.RELEASED:
            self._observe("record_stale_commit", routing.task.pool_id)
        self._observe(
            "observe_queue_depth",
            routing.task.pool_id,
            queue_depth,
        )
        return outcome

    def _claim_failed(
        self,
        scheduler: DeterministicRemoteScheduler,
        routing: RemoteAssignment,
        *,
        expected_run_id: str,
    ) -> None:
        self._rollback_routing(scheduler, routing)
        with self._lock:
            binding = self._task_bindings.get(routing.task.task_id)
            if (
                self._scheduler is scheduler
                and binding is not None
                and binding.run_id == expected_run_id
            ):
                self._task_bindings.pop(routing.task.task_id, None)
        self._observe("record_stale_commit", routing.task.pool_id)
        self._observe(
            "observe_queue_depth",
            routing.task.pool_id,
            scheduler.queue_depth(pool_id=routing.task.pool_id),
        )

    @staticmethod
    def _rollback_routing(
        scheduler: DeterministicRemoteScheduler,
        routing: RemoteAssignment,
    ) -> None:
        outcome = scheduler.release(
            routing.assignment_id,
            routing.worker_id,
            worker_generation=routing.worker_generation,
            session_id=routing.worker_session_id,
        )
        if outcome is not ReleaseOutcome.RELEASED:
            raise RemoteFleetConflict(
                "failed to roll back routing assignment"
            )

    @staticmethod
    def _durable_matches(
        routing: RemoteAssignment,
        durable: object,
        binding: FleetTaskBinding,
    ) -> bool:
        return (
            isinstance(durable, WorkAssignment)
            and durable.claim.run_id == binding.run_id
            and durable.worker_id == routing.worker_id
            and durable.activity_kind == "tool"
            and durable.activity_descriptor.activity_name
            == routing.task.tool_name
            and (
                not binding.exact_for_production
                or (
                    durable.claim.node_id == binding.node_id
                    and durable.activity_descriptor.config_digest
                    == binding.activity_config_digest
                    and frozenset(
                        durable.execution_plan.capabilities
                    ).issubset(
                        routing.task.required_capabilities
                    )
                    and frozenset(durable.resource_keys)
                    == routing.task.required_resource_keys
                )
            )
        )

    def _observe(self, method_name: str, *args, **kwargs) -> None:
        observer = self._observability
        if observer is None:
            return
        try:
            method = getattr(observer, method_name)
            method(*args, **kwargs)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise


def _run_id(value: object) -> str:
    if not isinstance(value, str) or _RUN_ID.fullmatch(value) is None:
        raise RemoteFleetValidationError(
            f"run_id must be a bounded opaque identifier up to {MAX_RUN_ID_CHARS} chars"
        )
    return value


__all__ = [
    "FleetAssignment",
    "FleetRebuildReport",
    "FleetSnapshot",
    "FleetTaskBinding",
    "RemoteFleetClaimError",
    "RemoteFleetConflict",
    "RemoteFleetCoordinator",
    "RemoteFleetError",
    "RemoteFleetValidationError",
]
