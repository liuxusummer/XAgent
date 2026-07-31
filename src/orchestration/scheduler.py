"""Deterministic, durable scheduler for compiled Workflow v2 definitions.

The scheduler only advances persisted control-plane state. External Activity
execution remains the worker/adapter's responsibility and never happens inside
a store transaction.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass, replace
from typing import Any, Protocol

from .artifacts import ArtifactRef
from .models import (
    AttemptRecord,
    AttemptStatus,
    ClaimDisposition,
    EventRecord,
    IdempotencyClaim,
    IdempotencyStatus,
    NodeRecord,
    NodeStatus,
    RunRecord,
    RunStatus,
    new_id,
    normalize_json,
    normalize_metadata,
)
from .policy import MAX_OPERATION_KEY_CHARS
from .store import (
    ActivityAdmissionDenied,
    ConcurrentProjectionUpdate,
    DurableRunStore,
    HierarchyAdmissionScope,
    ProjectionConflictError,
    RunAlreadyExistsError,
    RunNotFoundError,
)
from .workflow import CompiledWorkflow, FrozenDict, NodeDefinition, TEMPLATE_RE

ACTIVITY_KINDS = frozenset({"agent", "tool"})
CONTROL_KINDS = frozenset(
    {"router", "parallel", "join", "map", "approval", "subworkflow"}
)
ACTIVE_ATTEMPT_STATUSES = frozenset(
    {
        AttemptStatus.SCHEDULED,
        AttemptStatus.CLAIMED,
        AttemptStatus.WAITING_APPROVAL,
        AttemptStatus.RUNNING,
    }
)
WAITING_NODE_STATUSES = frozenset(
    {
        NodeStatus.WAITING_RETRY,
        NodeStatus.WAITING_INPUT,
        NodeStatus.WAITING_APPROVAL,
        NodeStatus.WAITING_RECOVERY,
        NodeStatus.PAUSED,
    }
)
MAX_CONTROL_INTENT_CAS_ATTEMPTS = 8
MAX_RECONCILE_CAS_ATTEMPTS = 8


class SchedulerError(RuntimeError):
    """The durable scheduler cannot safely advance the requested Run."""


class DefinitionMismatchError(SchedulerError):
    """A persisted Run or Node belongs to another Workflow definition."""


class SchedulerStateError(SchedulerError):
    """The requested scheduler operation is invalid for current projections."""


class StoreCapabilityError(SchedulerError):
    """The store lacks an atomic primitive required for safe scheduling."""


class ResultPersistenceError(SchedulerError):
    """An Activity result was not converted to a bounded Artifact receipt."""


class InputPersistenceError(SchedulerError):
    """A Run input was not converted to a bounded Artifact receipt."""


class InputMappingError(SchedulerStateError):
    """A declared dataflow input cannot be resolved to verified Artifacts."""


class HierarchyController(Protocol):
    """Optional durable hierarchy extension used by the core scheduler."""

    def bind_store(self, store: DurableRunStore) -> None: ...

    def validate_workflow(
        self,
        workflow: CompiledWorkflow,
    ) -> frozenset[str]: ...

    def reconcile_run(
        self,
        scheduler: "DurableScheduler",
        run_id: str,
    ) -> bool: ...

    def reconcile_control(
        self,
        scheduler: "DurableScheduler",
        run_id: str,
        definition: NodeDefinition,
        node: NodeRecord,
    ) -> bool: ...

    def reconcile_pausing(
        self,
        scheduler: "DurableScheduler",
        run_id: str,
    ) -> bool | None: ...

    def reconcile_cancelling(
        self,
        scheduler: "DurableScheduler",
        run_id: str,
    ) -> bool | None: ...

    def claim_next_child(
        self,
        scheduler: "DurableScheduler",
        run_id: str,
        worker_id: str,
        *,
        capacity: int,
        resource_keys: Sequence[str] | None,
        lease_seconds: float,
    ) -> ActivityClaim | None: ...

    def prepare_next_child_admission(
        self,
        scheduler: "DurableScheduler",
        run_id: str,
        worker_id: str,
        *,
        resource_keys: Sequence[str] | None,
    ) -> "ActivityAdmissionTarget | None": ...

    def hierarchy_admission_scope(
        self,
        scheduler: "DurableScheduler",
        run_id: str,
    ) -> HierarchyAdmissionScope | None: ...

    def scheduler_for_run(
        self,
        scheduler: "DurableScheduler",
        run_id: str,
    ) -> "DurableScheduler": ...


@dataclass(frozen=True, slots=True, repr=False)
class ActivityClaim:
    run_id: str
    node_id: str
    attempt_id: str
    attempt_number: int
    worker_id: str
    request_hash: str
    claim_token: str
    fencing_token: int
    lease_expires_at: float
    operation_key: str
    idempotency_key: str
    claim_key: str
    activity_kind: str
    effect_class: str
    resource_keys: tuple[str, ...]
    config: Mapping[str, Any]
    input_artifact_bindings: tuple[
        tuple[str, tuple[ArtifactRef, ...]],
        ...,
    ]
    input_artifact_refs: tuple[ArtifactRef, ...]

    def __repr__(self) -> str:
        """Return claim diagnostics without exposing its live lease credential."""

        return (
            f"ActivityClaim(run_id={self.run_id!r}, node_id={self.node_id!r}, "
            f"attempt_id={self.attempt_id!r}, attempt_number={self.attempt_number}, "
            f"worker_id={self.worker_id!r}, fencing_token={self.fencing_token}, "
            f"activity_kind={self.activity_kind!r}, effect_class={self.effect_class!r})"
        )


@dataclass(frozen=True, slots=True)
class ActivityAdmissionCandidate:
    """Read-only exact Activity identity with no lease authority."""

    claim: ActivityClaim
    attempt: AttemptRecord
    definition_digest: str
    expected_run_version: int
    expected_node_version: int
    expected_attempt_version: int | None
    new_attempt: bool

    @property
    def candidate_digest(self) -> str:
        return hashlib.sha256(
            json.dumps(
                {
                    "schema": "activity_admission_candidate_v1",
                    "definition_digest": self.definition_digest,
                    "expected_run_version": self.expected_run_version,
                    "expected_node_version": self.expected_node_version,
                    "expected_attempt_version": self.expected_attempt_version,
                    "new_attempt": self.new_attempt,
                    "attempt": self.attempt.to_dict(),
                    "run_id": self.claim.run_id,
                    "node_id": self.claim.node_id,
                    "attempt_id": self.claim.attempt_id,
                    "attempt_number": self.claim.attempt_number,
                    "worker_id": self.claim.worker_id,
                    "request_hash": self.claim.request_hash,
                    "operation_key": self.claim.operation_key,
                    "claim_key": self.claim.claim_key,
                    "activity_kind": self.claim.activity_kind,
                    "effect_class": self.claim.effect_class,
                    "resource_keys": list(self.claim.resource_keys),
                    "config": (
                        self.claim.config.to_dict()
                        if isinstance(self.claim.config, FrozenDict)
                        else normalize_json(dict(self.claim.config))
                    ),
                    "input_artifacts": [
                        ref.to_dict() for ref in self.claim.input_artifact_refs
                    ],
                    "input_artifact_bindings": [
                        {
                            "name": name,
                            "artifacts": [ref.to_dict() for ref in refs],
                        }
                        for name, refs in self.claim.input_artifact_bindings
                    ],
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class ActivityAdmissionTarget:
    """Exact scheduler/candidate plus optional root-to-child authority."""

    scheduler: "DurableScheduler"
    candidate: ActivityAdmissionCandidate
    hierarchy_admission: HierarchyAdmissionScope | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.scheduler, DurableScheduler):
            raise TypeError("scheduler must be a DurableScheduler")
        if not isinstance(
            self.candidate,
            ActivityAdmissionCandidate,
        ):
            raise TypeError(
                "candidate must be an ActivityAdmissionCandidate"
            )
        run = self.scheduler.store.get_run(
            self.candidate.claim.run_id
        )
        if (
            run is None
            or run.definition_digest
            != self.scheduler.workflow.definition_digest
            or self.candidate.definition_digest
            != self.scheduler.workflow.definition_digest
        ):
            raise SchedulerStateError(
                "Activity admission target scheduler is inconsistent"
            )
        linked = isinstance(
            run.metadata.get("hierarchy_link"),
            dict,
        )
        if linked != (self.hierarchy_admission is not None):
            raise SchedulerStateError(
                "Activity admission target hierarchy authority is missing"
            )
        if (
            self.hierarchy_admission is not None
            and self.hierarchy_admission.target_run_id != run.run_id
        ):
            raise SchedulerStateError(
                "Activity admission target hierarchy is inconsistent"
            )


@dataclass(frozen=True, slots=True)
class ActivityReceipt:
    """Bounded durable result containing references, never raw Activity output."""

    artifact_refs: tuple[ArtifactRef, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.artifact_refs, tuple):
            raise TypeError("ActivityReceipt.artifact_refs must be a tuple")
        if not self.artifact_refs:
            raise ValueError("ActivityReceipt requires at least one ArtifactRef")
        if not all(isinstance(ref, ArtifactRef) for ref in self.artifact_refs):
            raise TypeError("ActivityReceipt entries must be ArtifactRef instances")
        if len(self.artifact_refs) > 64:
            raise ValueError("ActivityReceipt exceeds 64 ArtifactRefs")

    def to_dict(self) -> dict[str, Any]:
        return {
            "outcome": "succeeded",
            "artifact_refs": [ref.to_dict() for ref in self.artifact_refs],
        }


@dataclass(frozen=True, slots=True)
class RunInputReceipt:
    """Bounded durable Run input containing immutable Artifact references."""

    artifact_refs: tuple[ArtifactRef, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.artifact_refs, tuple):
            raise TypeError("RunInputReceipt.artifact_refs must be a tuple")
        if not self.artifact_refs:
            raise ValueError("RunInputReceipt requires at least one ArtifactRef")
        if not all(isinstance(ref, ArtifactRef) for ref in self.artifact_refs):
            raise TypeError("RunInputReceipt entries must be ArtifactRef instances")
        if len(self.artifact_refs) > 64:
            raise ValueError("RunInputReceipt exceeds 64 ArtifactRefs")

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "artifact_input",
            "artifact_refs": [ref.to_dict() for ref in self.artifact_refs],
        }


@dataclass(frozen=True, slots=True)
class ApprovalResolution:
    """Ledger-bound approval decision accepted only through a trusted verifier."""

    approval_id: str
    run_id: str
    node_id: str
    definition_digest: str
    approved: bool
    decision_digest: str

    def __post_init__(self) -> None:
        for field_name in ("approval_id", "run_id", "node_id"):
            value = getattr(self, field_name)
            if (
                not isinstance(value, str)
                or not value.strip()
                or len(value) > 255
                or any(ord(character) < 32 for character in value)
            ):
                raise ValueError(f"{field_name} must be bounded text")
        for field_name in ("definition_digest", "decision_digest"):
            value = getattr(self, field_name)
            if (
                not isinstance(value, str)
                or not re.fullmatch(r"[0-9a-f]{64}", value)
            ):
                raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
        if not isinstance(self.approved, bool):
            raise TypeError("approved must be a bool")


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    run: RunRecord
    nodes: tuple[NodeRecord, ...]
    ready: tuple[str, ...]
    active_attempts: tuple[str, ...]


class DurableScheduler:
    """Replay-safe scheduler backed exclusively by durable projections."""

    def __init__(
        self,
        store: DurableRunStore,
        compiled_workflow: CompiledWorkflow,
        *,
        clock: Callable[[], float] | None = None,
        id_factory: Callable[[str], str] | None = None,
        max_active_attempts: int = 8,
        result_writer: Callable[[ActivityClaim, Any], ActivityReceipt] | None = None,
        input_writer: Callable[[str, Any], RunInputReceipt] | None = None,
        artifact_verifier: Callable[[ArtifactRef], bool] | None = None,
        approval_verifier: Callable[[ApprovalResolution], bool] | None = None,
        hierarchy_controller: HierarchyController | None = None,
    ) -> None:
        if max_active_attempts < 1:
            raise ValueError("max_active_attempts must be positive")
        self.store = store
        self.workflow = compiled_workflow
        self._clock = clock or time.time
        self._id_factory = id_factory or new_id
        self.max_active_attempts = int(max_active_attempts)
        self._result_writer = result_writer
        self._input_writer = input_writer
        self._artifact_verifier = artifact_verifier
        self._approval_verifier = approval_verifier
        self._hierarchy_controller = hierarchy_controller
        if hierarchy_controller is not None:
            hierarchy_controller.bind_store(store)
        self._map_template_node_ids = (
            frozenset()
            if hierarchy_controller is None
            else hierarchy_controller.validate_workflow(compiled_workflow)
        )
        self._topology_index = {
            node_id: index
            for index, node_id in enumerate(self.workflow.topological_order)
        }

    def now(self) -> float:
        """Return the Scheduler's validated authoritative timestamp."""

        return self._now()

    def create_run(
        self,
        run_id: str,
        *,
        input: Any = None,
        metadata: Mapping[str, Any] | None = None,
        deadline_at: float | None = None,
    ) -> RunRecord:
        """Create a Run and all PENDING Nodes.

        The current store atomically creates the Run itself, then each Node in
        its own event transaction. A crash in that narrow window is repaired by
        ``reconcile`` without duplicating already-created Nodes.
        """

        run = self._new_run_record(
            run_id,
            input=input,
            metadata=metadata,
            deadline_at=deadline_at,
        )
        # RunAlreadyExistsError deliberately propagates: reusing a caller
        # supplied run_id must fail closed even for an identical definition.
        created = self.store.create_run(run)
        self._ensure_nodes(created.run_id)
        return self._require_run(created.run_id)

    def create_child_run(
        self,
        run_id: str,
        *,
        input: Any,
        metadata: Mapping[str, Any],
        max_total_descendants: int,
        max_children_per_control: int,
        expected_parent_run_version: int,
        expected_parent_node_version: int,
    ) -> RunRecord:
        """Create one hierarchy child with persisted limits in the same commit."""

        run = self._new_run_record(
            run_id,
            input=input,
            metadata=metadata,
            deadline_at=None,
        )
        created = self.store.create_child_run(
            run,
            max_total_descendants=max_total_descendants,
            max_children_per_control=max_children_per_control,
            expected_parent_run_version=expected_parent_run_version,
            expected_parent_node_version=expected_parent_node_version,
        )
        self._ensure_nodes(created.run_id)
        return self._require_run(created.run_id)

    def _new_run_record(
        self,
        run_id: str,
        *,
        input: Any,
        metadata: Mapping[str, Any] | None,
        deadline_at: float | None,
    ) -> RunRecord:
        if self.store.get_run(run_id) is not None:
            raise RunAlreadyExistsError(run_id)
        now = self._now()
        run_metadata = normalize_metadata(
            {} if metadata is None else metadata,
            "run metadata",
        )
        durable_input = self._prepare_run_input(run_id, input)
        if deadline_at is not None:
            try:
                deadline = float(deadline_at)
            except (TypeError, ValueError) as exc:
                raise ValueError("deadline_at must be a finite timestamp") from exc
            if not math.isfinite(deadline) or deadline < 0:
                raise ValueError("deadline_at must be a finite timestamp")
            existing_deadline = run_metadata.get("deadline_at")
            if existing_deadline is not None and float(existing_deadline) != deadline:
                raise ValueError("deadline_at conflicts with metadata.deadline_at")
            run_metadata["deadline_at"] = deadline
        run_metadata.update(
            {
                "workflow_schema_version": self.workflow.schema_version,
                "scheduler": "durable_scheduler_v1",
            }
        )
        return RunRecord(
            run_id=run_id,
            workflow_id=self.workflow.name,
            workflow_version=self.workflow.version,
            definition_digest=self.workflow.definition_digest,
            input=durable_input,
            metadata=run_metadata,
            created_at=now,
            updated_at=now,
        )

    def reconcile(self, run_id: str) -> ReconcileResult:
        """Advance control state and converge committed CAS contention."""

        for attempt in range(MAX_RECONCILE_CAS_ATTEMPTS):
            try:
                return self._reconcile_once(run_id)
            except ConcurrentProjectionUpdate:
                if attempt == MAX_RECONCILE_CAS_ATTEMPTS - 1:
                    raise
        raise AssertionError("unreachable reconcile retry state")

    def _reconcile_once(self, run_id: str) -> ReconcileResult:
        """Advance deterministic control-plane state without external work."""

        run = self._require_run(run_id)
        self._ensure_nodes(run_id)
        run = self._require_run(run_id)
        if run.status.is_terminal:
            return self._snapshot(run_id)
        if run.status is RunStatus.CREATED:
            self._transition_run(
                run_id,
                RunStatus.RUNNING,
                "run.started",
                expected_run=run,
            )

        max_steps = max(16, len(self.workflow.nodes) * 8)
        for _step in range(max_steps):
            run = self._require_run(run_id)
            if run.status.is_terminal or run.status is RunStatus.WAITING_RECOVERY:
                break
            if run.status is RunStatus.CANCELLING:
                if not self._reconcile_cancelling(run):
                    break
                continue
            if run.status is RunStatus.PAUSING:
                if not self._reconcile_pausing(run):
                    break
                continue
            if run.status is RunStatus.PAUSED:
                break

            if (
                self._hierarchy_controller is not None
                and self._hierarchy_controller.reconcile_run(self, run_id)
            ):
                continue
            changed = self._advance_one(run)
            if changed:
                continue
            if self._finalize_if_terminal(run_id):
                continue
            break
        else:
            raise SchedulerStateError("reconcile exceeded deterministic transition bound")
        return self._snapshot(run_id)

    def claim_next(
        self,
        run_id: str,
        worker_id: str,
        *,
        capacity: int = 1,
        resource_keys: Sequence[str] | None = None,
        lease_seconds: float = 60.0,
    ) -> ActivityClaim | None:
        """Claim the first admissible READY Activity in topological order."""

        if not str(worker_id or "").strip():
            raise ValueError("worker_id must not be empty")
        if capacity < 1:
            raise ValueError("capacity must be positive")
        available_resources = (
            None if resource_keys is None else frozenset(str(key) for key in resource_keys)
        )
        self.reconcile(run_id)
        run = self._require_run(run_id)
        if run.status is not RunStatus.RUNNING:
            return None

        nodes = {node.node_id: node for node in self.store.list_nodes(run_id)}
        run_attempts = self.store.list_attempts(run_id)
        by_node: dict[str, list[AttemptRecord]] = {}
        for attempt in run_attempts:
            by_node.setdefault(attempt.node_id, []).append(attempt)

        for node_id in self.workflow.topological_order:
            record = nodes[node_id]
            definition = self.workflow.get_node(node_id)
            if (
                record.status is not NodeStatus.READY
                or definition.kind not in ACTIVITY_KINDS
                or node_id in self._map_template_node_ids
            ):
                continue
            active_for_node = next(
                (
                    attempt
                    for attempt in by_node.get(node_id, ())
                    if attempt.status in ACTIVE_ATTEMPT_STATUSES
                ),
                None,
            )
            if active_for_node is not None:
                if active_for_node.status is not AttemptStatus.SCHEDULED:
                    continue
                if not self._worker_has_resources(
                    definition,
                    available_resources=available_resources,
                ):
                    continue
                try:
                    claimed = self._claim_scheduled(
                        run,
                        definition,
                        active_for_node,
                        worker_id,
                        lease_seconds,
                        capacity=capacity,
                    )
                except ActivityAdmissionDenied as exc:
                    if exc.reason_code in {"global_capacity", "worker_capacity"}:
                        return None
                    continue
                if claimed is not None:
                    return claimed
                run = self._require_run(run_id)
                continue

            if not self._worker_has_resources(
                definition,
                available_resources=available_resources,
            ):
                continue
            attempt = self._schedule_attempt(
                run,
                record,
                definition,
                by_node.get(node_id, ()),
            )
            try:
                claimed = self._claim_scheduled(
                    run,
                    definition,
                    attempt,
                    worker_id,
                    lease_seconds,
                    capacity=capacity,
                )
            except ActivityAdmissionDenied as exc:
                if exc.reason_code in {"global_capacity", "worker_capacity"}:
                    return None
                run = self._require_run(run_id)
                continue
            if claimed is not None:
                return claimed
            run = self._require_run(run_id)
        if self._hierarchy_controller is not None:
            return self._hierarchy_controller.claim_next_child(
                self,
                run_id,
                worker_id,
                capacity=capacity,
                resource_keys=resource_keys,
                lease_seconds=lease_seconds,
            )
        return None

    def prepare_next_admission(
        self,
        run_id: str,
        worker_id: str,
        *,
        resource_keys: Sequence[str] | None = None,
        target_node_id: str | None = None,
    ) -> ActivityAdmissionCandidate | None:
        """Build a read-only exact candidate before worker authorization.

        This deliberately does not call :meth:`reconcile`, schedule an Attempt,
        acquire a lease, or reserve capacity.  The eventual claim path must
        revalidate every snapshot field with Store CAS.
        """

        if not str(worker_id or "").strip():
            raise ValueError("worker_id must not be empty")
        run = self._require_run(run_id)
        if run.status is not RunStatus.RUNNING:
            return None
        available_resources = (
            None
            if resource_keys is None
            else frozenset(str(key) for key in resource_keys)
        )
        if target_node_id is None:
            nodes = {
                node.node_id: node
                for node in self.store.list_nodes(run_id)
            }
            attempts = self.store.list_attempts(run_id)
        else:
            target_node = self.store.get_node(run_id, target_node_id)
            if target_node is None:
                return None
            nodes = {target_node_id: target_node}
            attempts = self.store.list_attempts(
                run_id,
                node_id=target_node_id,
            )
        by_node: dict[str, list[AttemptRecord]] = {}
        for attempt in attempts:
            by_node.setdefault(attempt.node_id, []).append(attempt)

        for node_id in self.workflow.topological_order:
            if target_node_id is not None and node_id != target_node_id:
                continue
            node = nodes[node_id]
            definition = self.workflow.get_node(node_id)
            if (
                definition.kind not in ACTIVITY_KINDS
                or node_id in self._map_template_node_ids
                or not self._worker_has_resources(
                    definition,
                    available_resources=available_resources,
                )
            ):
                continue
            active = next(
                (
                    attempt
                    for attempt in by_node.get(node_id, ())
                    if attempt.status in ACTIVE_ATTEMPT_STATUSES
                ),
                None,
            )
            if active is not None and active.status is not AttemptStatus.SCHEDULED:
                continue
            if active is None and node.status is not NodeStatus.READY:
                continue

            if active is None:
                previous = by_node.get(node_id, ())
                attempt_number = max(
                    (item.attempt_number for item in previous),
                    default=0,
                ) + 1
                input_bindings = self._resolve_input_artifacts(run, definition)
                request_hash, input_digest = self._request_hash(
                    run,
                    definition,
                    input_bindings=input_bindings,
                )
                operation_key = self._render_operation_key(
                    run,
                    definition,
                    input_digest=input_digest,
                )
                claim_key = f"{operation_key}:attempt:{attempt_number}"
                attempt_id = str(self._id_factory("attempt"))
                now = self._now()
                timeout_policy = definition.timeout_policy.to_dict()
                schedule_deadline = node.metadata.get("schedule_deadline_at")
                candidate_attempt = AttemptRecord(
                    attempt_id=attempt_id,
                    run_id=run.run_id,
                    node_id=node.node_id,
                    attempt_number=attempt_number,
                    idempotency_key=claim_key,
                    activity_kind=definition.kind,
                    effect_class=definition.effect_class,
                    metadata={
                        "definition_digest": self.workflow.definition_digest,
                        "request_hash": request_hash,
                        "input_mapping_digest": self._input_mapping_digest(
                            definition,
                            input_bindings,
                        ),
                        "operation_key": operation_key,
                        "resource_keys": list(definition.resource_keys),
                        "concurrency_key": definition.concurrency_key,
                        "timeout_policy": timeout_policy,
                        **(
                            {
                                "schedule_deadline_at": float(
                                    schedule_deadline
                                )
                            }
                            if isinstance(
                                schedule_deadline,
                                (int, float),
                            )
                            else {}
                        ),
                    },
                    scheduled_at=now,
                )
                expected_attempt_version = None
                new_attempt = True
            else:
                request_hash = active.metadata.get("request_hash")
                operation_key = active.metadata.get("operation_key")
                if not isinstance(request_hash, str) or not isinstance(
                    operation_key,
                    str,
                ):
                    raise DefinitionMismatchError(
                        f"attempt {active.attempt_id} lacks scheduler identity metadata"
                    )
                input_bindings = self._resolve_input_artifacts(run, definition)
                attempt_number = active.attempt_number
                claim_key = active.idempotency_key
                attempt_id = active.attempt_id
                expected_attempt_version = active.projection_version
                new_attempt = False
                candidate_attempt = active
            input_refs = tuple(
                ref for _name, refs in input_bindings for ref in refs
            )
            proposal = ActivityClaim(
                run_id=run_id,
                node_id=node_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                worker_id=worker_id,
                request_hash=request_hash,
                claim_token="",
                fencing_token=0,
                lease_expires_at=0.0,
                operation_key=operation_key,
                idempotency_key=operation_key,
                claim_key=claim_key,
                activity_kind=definition.kind,
                effect_class=definition.effect_class,
                resource_keys=definition.resource_keys,
                config=definition.config,
                input_artifact_bindings=input_bindings,
                input_artifact_refs=input_refs,
            )
            return ActivityAdmissionCandidate(
                claim=proposal,
                attempt=candidate_attempt,
                definition_digest=self.workflow.definition_digest,
                expected_run_version=run.projection_version,
                expected_node_version=node.projection_version,
                expected_attempt_version=expected_attempt_version,
                new_attempt=new_attempt,
            )
        # Hierarchy routing is intentionally not delegated to the existing
        # claim_next_child path because that path may reconcile and mutate.
        return None

    def prepare_next_admission_target(
        self,
        run_id: str,
        worker_id: str,
        *,
        resource_keys: Sequence[str] | None = None,
        target_node_id: str | None = None,
    ) -> ActivityAdmissionTarget | None:
        """Resolve one exact local or descendant Activity without mutation."""

        candidate = self.prepare_next_admission(
            run_id,
            worker_id,
            resource_keys=resource_keys,
            target_node_id=target_node_id,
        )
        if candidate is not None:
            hierarchy_admission = (
                None
                if self._hierarchy_controller is None
                else self._hierarchy_controller.hierarchy_admission_scope(
                    self,
                    run_id,
                )
            )
            return ActivityAdmissionTarget(
                scheduler=self,
                candidate=candidate,
                hierarchy_admission=hierarchy_admission,
            )
        if (
            target_node_id is not None
            or self._hierarchy_controller is None
        ):
            return None
        return self._hierarchy_controller.prepare_next_child_admission(
            self,
            run_id,
            worker_id,
            resource_keys=resource_keys,
        )

    def claim_admitted(
        self,
        candidate: ActivityAdmissionCandidate,
        *,
        lease_seconds: float,
        capacity: int,
        admission_expires_at: float,
        policy_binding: Mapping[str, str | None],
        fleet_admission: Mapping[str, object] | None = None,
        fleet_shard_ownership: Mapping[str, object] | None = None,
        hierarchy_admission: HierarchyAdmissionScope | None = None,
        linearization_guard: Callable[[], Any] | None = None,
        linearization_validator: Callable[[], bool] | None = None,
    ) -> tuple[ActivityClaim | None, EventRecord]:
        """Linearize one externally preauthorized candidate in Store."""

        if not isinstance(candidate, ActivityAdmissionCandidate):
            raise TypeError("candidate must be an ActivityAdmissionCandidate")
        if (
            candidate.definition_digest != self.workflow.definition_digest
            or candidate.claim.worker_id == ""
            or capacity < 1
        ):
            raise SchedulerStateError("invalid Activity admission candidate")
        if (
            hierarchy_admission is not None
            and (
                not isinstance(
                    hierarchy_admission,
                    HierarchyAdmissionScope,
                )
                or hierarchy_admission.target_run_id
                != candidate.claim.run_id
            )
        ):
            raise SchedulerStateError(
                "invalid hierarchy admission authority"
            )
        if (linearization_guard is None) != (
            linearization_validator is None
        ):
            raise SchedulerStateError(
                "incomplete admission linearization context"
            )
        guard = (
            nullcontext()
            if linearization_guard is None
            else linearization_guard()
        )
        with guard:
            if linearization_validator is not None:
                try:
                    current = linearization_validator()
                except (KeyboardInterrupt, SystemExit):
                    raise
                except BaseException:
                    current = False
                if current is not True:
                    raise ActivityAdmissionDenied(
                        "admission session is no longer current"
                    )
            run = self._require_run(candidate.claim.run_id)
            node = self._require_node(
                candidate.claim.run_id,
                candidate.claim.node_id,
            )
            definition = self.workflow.get_node(candidate.claim.node_id)
            if (
                run.status is not RunStatus.RUNNING
                or node.status is not NodeStatus.READY
                or definition.kind != candidate.claim.activity_kind
                or definition.effect_class != candidate.claim.effect_class
                or definition.resource_keys != candidate.claim.resource_keys
            ):
                raise SchedulerStateError(
                    "Activity admission candidate is stale"
                )
            if node.projection_version != candidate.expected_node_version:
                raise SchedulerStateError(
                    "Activity admission Node projection changed"
                )
            if run.projection_version != candidate.expected_run_version:
                raise SchedulerStateError(
                    "Activity admission Run projection changed"
                )
            claim, _claimed_event, policy_event = (
                self.store.claim_activity_with_policy(
                    candidate.claim.run_id,
                    candidate.claim.node_id,
                    candidate.attempt,
                    candidate.claim.request_hash,
                    candidate.claim.worker_id,
                    definition_digest=candidate.definition_digest,
                    schedule_new=candidate.new_attempt,
                    expected_run_version=candidate.expected_run_version,
                    expected_node_version=node.projection_version,
                    expected_attempt_version=(
                        candidate.expected_attempt_version
                    ),
                    admission_expires_at=admission_expires_at,
                    admission_clock=self._clock,
                    policy_binding=policy_binding,
                    lease_seconds=lease_seconds,
                    now=self._now(),
                    max_active_attempts=self.max_active_attempts,
                    worker_capacity=capacity,
                    fleet_admission=fleet_admission,
                    fleet_shard_ownership=fleet_shard_ownership,
                    hierarchy_admission=(
                        None
                        if hierarchy_admission is None
                        else hierarchy_admission.to_metadata()
                    ),
                )
            )
            if policy_binding.get("outcome") == "require_approval":
                if claim is not None or _claimed_event is not None:
                    raise SchedulerStateError(
                        "approval admission unexpectedly acquired a claim"
                    )
                return None, policy_event
            if claim is None:
                raise SchedulerStateError(
                    "authorized Activity admission did not return a claim"
                )
            if claim.disposition is not ClaimDisposition.ACQUIRED:
                raise SchedulerStateError(
                    "Activity admission claim was not acquired"
                )
            active_claim = replace(
                candidate.claim,
                claim_token=claim.record.claim_token,
                fencing_token=claim.record.claim_count,
                lease_expires_at=claim.record.lease_expires_at,
            )
            return active_claim, policy_event

    def start_claim(self, claim: ActivityClaim) -> Any:
        routed = self._scheduler_for_claim(claim)
        if routed is not self:
            return routed.start_claim(claim)
        self._validate_claim_handle(claim)
        return self.store.start_activity(
            claim.run_id,
            claim.node_id,
            claim.attempt_id,
            claim.worker_id,
            claim_token=claim.claim_token,
            now=self._now(),
        )

    def restore_claim(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        worker_id: str,
        *,
        request_hash: str,
        claim_token: str,
        fencing_token: int,
    ) -> ActivityClaim:
        """Rebuild an active claim handle from durable state without mutation.

        Remote control-plane processes use this after restart.  Every
        caller-supplied identity component is checked against the current
        Attempt and idempotency lease; terminal, expired, foreign, and stale
        claims fail closed.
        """

        run = self._require_run(run_id)
        try:
            definition = self.workflow.get_node(node_id)
        except KeyError as exc:
            raise SchedulerStateError("claim node is not in the Workflow") from exc
        node = self.store.get_node(run_id, node_id)
        attempt = self.store.get_attempt(attempt_id)
        if (
            node is None
            or attempt is None
            or attempt.run_id != run_id
            or attempt.node_id != node_id
            or attempt.status not in {AttemptStatus.CLAIMED, AttemptStatus.RUNNING}
            or node.status
            not in {
                NodeStatus.READY,
                NodeStatus.RUNNING,
            }
            or run.status.is_terminal
        ):
            raise SchedulerStateError("claim is not active")
        record = self.store.get_idempotency(run_id, attempt.idempotency_key)
        try:
            expected_fencing = int(fencing_token)
        except (TypeError, ValueError) as exc:
            raise SchedulerStateError("invalid claim fencing token") from exc
        if (
            not isinstance(request_hash, str)
            or not request_hash
            or not isinstance(worker_id, str)
            or not worker_id
            or not isinstance(claim_token, str)
            or not claim_token
            or record is None
            or record.status is not IdempotencyStatus.IN_PROGRESS
            or record.lease_expires_at <= self._now()
            or record.request_hash != request_hash
            or record.owner_id != worker_id
            or record.claim_token != claim_token
            or record.claim_count != expected_fencing
            or attempt.metadata.get("request_hash") != request_hash
            or attempt.worker_id != worker_id
            or attempt.lease_id != claim_token
            or attempt.fencing_token != expected_fencing
        ):
            raise SchedulerStateError("claim binding is stale or foreign")
        claim = self._claim_handle(
            definition,
            attempt,
            IdempotencyClaim(ClaimDisposition.ACQUIRED, record),
            worker_id,
        )
        self._validate_claim_handle(claim)
        return claim

    def renew_claim(
        self,
        claim: ActivityClaim,
        *,
        lease_seconds: float = 60.0,
    ) -> Any:
        """Renew one active claim using the Scheduler's trusted clock."""

        routed = self._scheduler_for_claim(claim)
        if routed is not self:
            return routed.renew_claim(claim, lease_seconds=lease_seconds)
        self._validate_claim_handle(claim)
        return self.store.renew_activity_lease(
            claim.run_id,
            claim.node_id,
            claim.attempt_id,
            claim.request_hash,
            claim.worker_id,
            claim_token=claim.claim_token,
            fencing_token=claim.fencing_token,
            lease_seconds=lease_seconds,
            now=self._now(),
        )

    def complete_claim(
        self,
        claim: ActivityClaim,
        result: Any = None,
        *,
        attempt_status: AttemptStatus = AttemptStatus.SUCCEEDED,
        error_class: str | None = None,
    ) -> Any:
        """Commit one worker result and reconcile its downstream DAG."""

        routed = self._scheduler_for_claim(claim)
        if routed is not self:
            return routed.complete_claim(
                claim,
                result,
                attempt_status=attempt_status,
                error_class=error_class,
            )
        self._validate_claim_handle(claim)
        status = AttemptStatus(attempt_status)
        if not status.is_terminal:
            raise ValueError("attempt_status must be terminal")
        durable_result = (
            self._prepare_success_receipt(claim, result)
            if status is AttemptStatus.SUCCEEDED
            else self._safe_failure_receipt(
                status,
                error_class,
                _safe_error_code(result),
            )
        )
        if status is AttemptStatus.OUTCOME_UNKNOWN:
            completion = self.store.complete_activity(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
                claim.request_hash,
                claim.worker_id,
                claim_token=claim.claim_token,
                result=durable_result,
                attempt_status=status,
                node_status=NodeStatus.WAITING_RECOVERY,
                run_status=RunStatus.WAITING_RECOVERY,
                now=self._now(),
            )
        elif status in {
            AttemptStatus.FAILED,
            AttemptStatus.TIMED_OUT,
            AttemptStatus.ABANDONED,
        } and self._retry_allowed(claim, status, error_class):
            completion = self._complete_retryable_failure(
                claim,
                attempt_status=status,
                error_class=str(error_class),
                error_code=_safe_error_code(result),
            )
        else:
            node_status = {
                AttemptStatus.SUCCEEDED: NodeStatus.SUCCEEDED,
                AttemptStatus.CANCELLED: NodeStatus.CANCELLED,
                AttemptStatus.FAILED: NodeStatus.FAILED,
                AttemptStatus.TIMED_OUT: NodeStatus.FAILED,
                AttemptStatus.ABANDONED: NodeStatus.FAILED,
            }.get(status)
            if node_status is None:
                raise SchedulerStateError(f"unsupported completion status: {status.value}")
            completion = self.store.complete_activity(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
                claim.request_hash,
                claim.worker_id,
                claim_token=claim.claim_token,
                result=durable_result,
                attempt_status=status,
                node_status=node_status,
                run_status=None,
                now=self._now(),
            )
        self.reconcile(claim.run_id)
        return completion

    def confirm_pause_claim(self, claim: ActivityClaim) -> Any:
        routed = self._scheduler_for_claim(claim)
        if routed is not self:
            return routed.confirm_pause_claim(claim)
        self._validate_claim_handle(claim)
        completed = self.store.pause_activity(
            claim.run_id,
            claim.node_id,
            claim.attempt_id,
            claim.request_hash,
            claim.worker_id,
            claim_token=claim.claim_token,
            now=self._now(),
        )
        self.reconcile(claim.run_id)
        return completed

    def confirm_cancel_claim(
        self,
        claim: ActivityClaim,
        *,
        event_payload: dict[str, Any] | None = None,
    ) -> Any:
        routed = self._scheduler_for_claim(claim)
        if routed is not self:
            return routed.confirm_cancel_claim(
                claim,
                event_payload=event_payload,
            )
        if event_payload is not None:
            self._validate_claim_handle(claim)
            completed = self.store.complete_activity(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
                claim.request_hash,
                claim.worker_id,
                claim_token=claim.claim_token,
                result=self._safe_failure_receipt(
                    AttemptStatus.CANCELLED,
                    None,
                    _safe_error_code({"outcome": "cancelled"}),
                ),
                event_payload=event_payload,
                attempt_status=AttemptStatus.CANCELLED,
                node_status=NodeStatus.CANCELLED,
                run_status=None,
                now=self._now(),
            )
            self.reconcile(claim.run_id)
            return completed
        return self.complete_claim(
            claim,
            {"outcome": "cancelled"},
            attempt_status=AttemptStatus.CANCELLED,
        )

    def request_cancel(
        self,
        run_id: str,
        *,
        reconcile: bool = True,
    ) -> RunRecord:
        self._ensure_nodes(run_id)
        for attempt in range(MAX_CONTROL_INTENT_CAS_ATTEMPTS):
            run = self._require_run(run_id)
            if run.status.is_terminal or run.status is RunStatus.CANCELLING:
                return run
            try:
                self._transition_run(
                    run_id,
                    RunStatus.CANCELLING,
                    "run.cancelling",
                    payload={"intent": "cancel"},
                    expected_run=run,
                )
                break
            except ConcurrentProjectionUpdate:
                if attempt == MAX_CONTROL_INTENT_CAS_ATTEMPTS - 1:
                    raise
        if reconcile:
            self.reconcile(run_id)
        return self._require_run(run_id)

    def request_pause(self, run_id: str) -> RunRecord:
        self._ensure_nodes(run_id)
        for attempt in range(MAX_CONTROL_INTENT_CAS_ATTEMPTS):
            run = self._require_run(run_id)
            if run.status in {RunStatus.PAUSING, RunStatus.PAUSED}:
                return run
            if run.status not in {RunStatus.CREATED, RunStatus.RUNNING}:
                raise SchedulerStateError(
                    f"cannot pause Run from {run.status.value}"
                )
            target_status = (
                RunStatus.RUNNING
                if run.status is RunStatus.CREATED
                else RunStatus.PAUSING
            )
            event_type = (
                "run.started"
                if run.status is RunStatus.CREATED
                else "run.pausing"
            )
            try:
                self._transition_run(
                    run_id,
                    target_status,
                    event_type,
                    payload=(
                        None
                        if run.status is RunStatus.CREATED
                        else {"intent": "pause"}
                    ),
                    expected_run=run,
                )
            except ConcurrentProjectionUpdate:
                if attempt == MAX_CONTROL_INTENT_CAS_ATTEMPTS - 1:
                    raise
                continue
            if target_status is RunStatus.RUNNING:
                continue
            break
        self.reconcile(run_id)
        return self._require_run(run_id)

    def resume(self, run_id: str) -> RunRecord:
        for attempt in range(MAX_CONTROL_INTENT_CAS_ATTEMPTS):
            run = self._require_run(run_id)
            if run.status is RunStatus.RUNNING:
                return run
            if run.status is not RunStatus.PAUSED:
                raise SchedulerStateError(
                    f"cannot resume Run from {run.status.value}"
                )
            try:
                self._transition_run(
                    run_id,
                    RunStatus.RUNNING,
                    "run.started",
                    expected_run=run,
                )
                break
            except ConcurrentProjectionUpdate:
                if attempt == MAX_CONTROL_INTENT_CAS_ATTEMPTS - 1:
                    raise
        self.reconcile(run_id)
        return self._require_run(run_id)

    def resolve_router(
        self,
        run_id: str,
        node_id: str,
        selection: str,
    ) -> NodeRecord:
        definition = self.workflow.get_node(node_id)
        if definition.kind != "router":
            raise SchedulerStateError(f"node {node_id} is not a router")
        routes = definition.config["routes"]
        assert isinstance(routes, Mapping)
        target = routes.get(selection, selection)
        valid_targets = {str(value) for value in routes.values()}
        default_target = definition.config.get("default_route")
        if default_target is not None:
            valid_targets.add(str(default_target))
        if not isinstance(target, str) or target not in valid_targets:
            raise SchedulerStateError(f"router {node_id} has no route {selection!r}")
        return self._resolve_waiting_node(
            run_id,
            node_id,
            waiting_status=NodeStatus.WAITING_INPUT,
            resolution_metadata={"selected_target": target},
            payload={
                "control_plane": "router_resolved",
                "selected_target": target,
            },
            not_waiting_message=(
                f"router {node_id} is not waiting for a decision"
            ),
            concurrent_message=f"router {node_id} was resolved concurrently",
        )

    def resolve_approval(
        self,
        run_id: str,
        node_id: str,
        resolution: ApprovalResolution,
    ) -> NodeRecord:
        """Apply a decision verified by a trusted ApprovalLedger/Policy adapter.

        This is a state-machine ingress, not an execution-path approval API.
        Raw Activity code cannot self-sign a boolean decision.
        """

        definition = self.workflow.get_node(node_id)
        if definition.kind != "approval":
            raise SchedulerStateError(f"node {node_id} is not an approval")
        if (
            resolution.run_id != run_id
            or resolution.node_id != node_id
            or resolution.definition_digest != self.workflow.definition_digest
        ):
            raise SchedulerStateError(
                "approval resolution is not bound to this Run/Node/definition"
            )
        if self._approval_verifier is None:
            raise SchedulerStateError(
                "approval resolution requires a trusted ApprovalLedger verifier"
            )
        if self._approval_verifier(resolution) is not True:
            raise SchedulerStateError(
                "approval resolution failed trusted verification"
            )
        decision = {
            "approval_granted": resolution.approved,
            "approval_id": resolution.approval_id,
            "approval_decision_digest": resolution.decision_digest,
        }
        return self._resolve_waiting_node(
            run_id,
            node_id,
            waiting_status=NodeStatus.WAITING_APPROVAL,
            resolution_metadata=decision,
            payload={
                "control_plane": "approval_resolved",
                "approved": resolution.approved,
                "approval_id": resolution.approval_id,
                "decision_digest": resolution.decision_digest,
            },
            not_waiting_message=(
                f"approval {node_id} is not waiting for a decision"
            ),
            concurrent_message=(
                f"approval {node_id} was resolved concurrently"
            ),
        )

    def resolve_control(
        self,
        run_id: str,
        node_id: str,
        result: Any,
    ) -> NodeRecord:
        """Resolve an MVP map/subworkflow control-plane wait explicitly."""

        definition = self.workflow.get_node(node_id)
        if definition.kind not in {"map", "subworkflow"}:
            raise SchedulerStateError(
                f"node {node_id} does not support explicit control resolution"
            )
        if self._hierarchy_controller is not None:
            raise SchedulerStateError(
                f"node {node_id} is managed by durable hierarchy reconciliation"
            )
        normalized = normalize_json(result, "control result")
        return self._resolve_waiting_node(
            run_id,
            node_id,
            waiting_status=NodeStatus.WAITING_INPUT,
            resolution_metadata={
                "control_resolved": True,
                "control_result": normalized,
            },
            payload={"control_plane": f"{definition.kind}_resolved"},
            not_waiting_message=f"node {node_id} is not waiting for input",
            concurrent_message=f"node {node_id} was resolved concurrently",
        )

    def _resolve_waiting_node(
        self,
        run_id: str,
        node_id: str,
        *,
        waiting_status: NodeStatus,
        resolution_metadata: Mapping[str, Any],
        payload: Mapping[str, Any],
        not_waiting_message: str,
        concurrent_message: str,
    ) -> NodeRecord:
        """Commit one control decision without losing unrelated Run updates."""

        def matches(node: NodeRecord) -> bool:
            return all(
                node.metadata.get(key) == value
                for key, value in resolution_metadata.items()
            )

        for attempt in range(MAX_CONTROL_INTENT_CAS_ATTEMPTS):
            node = self._require_node(run_id, node_id)
            if node.status is not waiting_status:
                if matches(node):
                    self.reconcile(run_id)
                    return self._require_node(run_id, node_id)
                raise SchedulerStateError(not_waiting_message)
            run = self._require_run(run_id)
            try:
                self._transition_node(
                    run_id,
                    node_id,
                    NodeStatus.READY,
                    "node.ready",
                    metadata_update=resolution_metadata,
                    payload=payload,
                    expected_run=run,
                    expected_node=node,
                )
                break
            except ConcurrentProjectionUpdate as exc:
                current = self._require_node(run_id, node_id)
                if matches(current):
                    self.reconcile(run_id)
                    return self._require_node(run_id, node_id)
                if current.status is not waiting_status:
                    raise SchedulerStateError(concurrent_message) from exc
                if attempt == MAX_CONTROL_INTENT_CAS_ATTEMPTS - 1:
                    raise
        self.reconcile(run_id)
        return self._require_node(run_id, node_id)

    def fork_for_workflow(
        self,
        workflow: CompiledWorkflow,
    ) -> "DurableScheduler":
        """Create a scheduler with identical runtime capabilities for one child definition."""

        return DurableScheduler(
            self.store,
            workflow,
            clock=self._clock,
            id_factory=self._id_factory,
            max_active_attempts=self.max_active_attempts,
            result_writer=self._result_writer,
            input_writer=self._input_writer,
            artifact_verifier=self._artifact_verifier,
            approval_verifier=self._approval_verifier,
            hierarchy_controller=self._hierarchy_controller,
        )

    def current_time(self) -> float:
        return self._now()

    def verify_activity_receipt(
        self,
        receipt: ActivityReceipt,
    ) -> dict[str, Any]:
        if not isinstance(receipt, ActivityReceipt):
            raise ResultPersistenceError("hierarchy result must be an ActivityReceipt")
        verified = ActivityReceipt(
            self._verify_artifact_refs(
                receipt.artifact_refs,
                error_type=ResultPersistenceError,
            )
        )
        normalized = normalize_json(verified.to_dict(), "activity receipt")
        if not isinstance(normalized, dict):
            raise ResultPersistenceError("Activity receipt must be a JSON object")
        return normalized

    def prepare_run_input_receipt(
        self,
        run_id: str,
        raw_input: Any,
    ) -> RunInputReceipt:
        """Artifactize and verify child input before any child Run/Event is created."""

        if isinstance(raw_input, RunInputReceipt):
            receipt = raw_input
        else:
            if self._input_writer is None:
                raise InputPersistenceError(
                    "raw Run input requires an Artifact input_writer"
                )
            receipt = self._input_writer(run_id, raw_input)
        if not isinstance(receipt, RunInputReceipt):
            raise InputPersistenceError(
                "input_writer must return a RunInputReceipt"
            )
        return RunInputReceipt(
            self._verify_artifact_refs(
                receipt.artifact_refs,
                error_type=InputPersistenceError,
            )
        )

    def record_hierarchy_state(
        self,
        run_id: str,
        node_id: str,
        state: Mapping[str, Any],
        *,
        payload: Mapping[str, Any],
    ) -> NodeRecord:
        definition = self.workflow.get_node(node_id)
        if definition.kind not in {"map", "subworkflow"}:
            raise SchedulerStateError(f"node {node_id} is not hierarchical")
        node = self._require_node(run_id, node_id)
        if node.status is not NodeStatus.RUNNING:
            raise SchedulerStateError(
                f"hierarchy state requires RUNNING Node, got {node.status.value}"
            )
        normalized_state = normalize_json(dict(state), "hierarchy state")
        if not isinstance(normalized_state, dict):
            raise SchedulerStateError("hierarchy state must be an object")
        proposed_count = normalized_state.get("created_count")
        if isinstance(proposed_count, bool) or not isinstance(proposed_count, int):
            raise SchedulerStateError(
                "hierarchy created_count must be a non-negative integer"
            )
        if proposed_count < 0:
            raise SchedulerStateError(
                "hierarchy created_count must be a non-negative integer"
            )
        current_state = node.metadata.get("hierarchy")
        if current_state is not None:
            if not isinstance(current_state, dict):
                raise SchedulerStateError("persisted hierarchy state must be an object")
            current_count = current_state.get("created_count")
            if isinstance(current_count, bool) or not isinstance(current_count, int):
                raise SchedulerStateError(
                    "persisted hierarchy created_count is invalid"
                )
            current_identity = {
                key: value
                for key, value in current_state.items()
                if key != "created_count"
            }
            proposed_identity = {
                key: value
                for key, value in normalized_state.items()
                if key != "created_count"
            }
            if current_identity != proposed_identity:
                raise SchedulerStateError(
                    "immutable hierarchy state fields cannot change"
                )
            if proposed_count <= current_count:
                return node
        run = self._require_run(run_id)
        self._transition_node(
            run_id,
            node_id,
            NodeStatus.RUNNING,
            "node.started",
            metadata_update={"hierarchy": normalized_state},
            payload=dict(payload),
            expected_run=run,
            expected_node=node,
        )
        return self._require_node(run_id, node_id)

    def record_hierarchy_child_link(
        self,
        run_id: str,
        node_id: str,
        *,
        event_id: str,
        link: Mapping[str, Any],
    ) -> None:
        definition = self.workflow.get_node(node_id)
        if definition.kind not in {"map", "subworkflow"}:
            raise SchedulerStateError(f"node {node_id} is not hierarchical")
        self._require_run(run_id)
        self.store.append_event(
            run_id,
            "audit.note",
            node_id=node_id,
            event_id=event_id,
            occurred_at=self._now(),
            payload={
                "control_plane": "hierarchy_child_linked",
                "link": normalize_json(dict(link), "hierarchy child link"),
            },
        )

    def resolve_hierarchy_control(
        self,
        run_id: str,
        node_id: str,
        *,
        outcome: str,
        receipt: ActivityReceipt | None = None,
        error_code: str = "child_run_failed",
        map_body_id: str | None = None,
    ) -> NodeRecord:
        """Replay-safely finish a hierarchy control and its unclaimable map template."""

        definition = self.workflow.get_node(node_id)
        if definition.kind not in {"map", "subworkflow"}:
            raise SchedulerStateError(f"node {node_id} is not hierarchical")
        if outcome not in {"succeeded", "failed", "cancelled"}:
            raise ValueError("invalid hierarchy outcome")
        durable_output = (
            self.verify_activity_receipt(receipt)
            if outcome == "succeeded" and receipt is not None
            else None
        )
        if outcome == "succeeded" and durable_output is None:
            raise ResultPersistenceError("successful hierarchy requires an Artifact receipt")

        node = self._require_node(run_id, node_id)
        desired_status = {
            "succeeded": NodeStatus.SUCCEEDED,
            "failed": NodeStatus.FAILED,
            "cancelled": NodeStatus.CANCELLED,
        }[outcome]
        if not node.status.is_terminal:
            if node.status is not NodeStatus.RUNNING:
                raise SchedulerStateError(
                    f"hierarchy resolution requires RUNNING Node, got {node.status.value}"
                )
            run = self._require_run(run_id)
            self._transition_node(
                run_id,
                node_id,
                desired_status,
                f"node.{desired_status.value}",
                output=durable_output,
                error=(
                    {"code": _safe_hierarchy_error_code(error_code)}
                    if outcome == "failed"
                    else None
                ),
                payload={
                    "control_plane": "hierarchy_resolved",
                    "outcome": outcome,
                },
                expected_run=run,
                expected_node=node,
            )
            node = self._require_node(run_id, node_id)
        elif node.status is not desired_status:
            raise SchedulerStateError("hierarchy control has a conflicting terminal state")

        if map_body_id is not None:
            if map_body_id not in self._map_template_node_ids:
                raise SchedulerStateError(
                    f"node {map_body_id} is not a registered map template"
                )
            for _step in range(4):
                body = self._require_node(run_id, map_body_id)
                if body.status.is_terminal:
                    expected_body = (
                        NodeStatus.SUCCEEDED
                        if outcome == "succeeded"
                        else NodeStatus.SKIPPED
                        if outcome == "failed"
                        else NodeStatus.CANCELLED
                    )
                    if body.status is not expected_body:
                        raise SchedulerStateError(
                            "map template has a conflicting terminal state"
                        )
                    break
                if outcome == "failed":
                    run = self._require_run(run_id)
                    self._transition_node(
                        run_id,
                        map_body_id,
                        NodeStatus.SKIPPED,
                        "node.skipped",
                        payload={"reason": "map_child_failed"},
                        expected_run=run,
                        expected_node=body,
                    )
                    continue
                if outcome == "cancelled":
                    run = self._require_run(run_id)
                    self._transition_node(
                        run_id,
                        map_body_id,
                        NodeStatus.CANCELLED,
                        "node.cancelled",
                        payload={"reason": "map_cancelled"},
                        expected_run=run,
                        expected_node=body,
                    )
                    continue
                if body.status is NodeStatus.PENDING:
                    run = self._require_run(run_id)
                    self._transition_node(
                        run_id,
                        map_body_id,
                        NodeStatus.READY,
                        "node.ready",
                        payload={"reason": "map_results_ready"},
                        expected_run=run,
                        expected_node=body,
                    )
                elif body.status is NodeStatus.READY:
                    run = self._require_run(run_id)
                    self._transition_node(
                        run_id,
                        map_body_id,
                        NodeStatus.RUNNING,
                        "node.started",
                        payload={"control_plane": "map_template_projection"},
                        expected_run=run,
                        expected_node=body,
                    )
                elif body.status is NodeStatus.RUNNING:
                    run = self._require_run(run_id)
                    self._transition_node(
                        run_id,
                        map_body_id,
                        NodeStatus.SUCCEEDED,
                        "node.succeeded",
                        output=durable_output,
                        payload={"control_plane": "map_template_projected"},
                        expected_run=run,
                        expected_node=body,
                    )
                else:
                    raise SchedulerStateError(
                        f"cannot resolve map template from {body.status.value}"
                    )
        return node

    def wait_hierarchy_recovery(
        self,
        run_id: str,
        node_id: str,
        *,
        error_code: str,
    ) -> NodeRecord:
        """Atomically fail closed the parent Run and hierarchy Node."""

        run = self._require_run(run_id)
        node = self._require_node(run_id, node_id)
        if (
            run.status is RunStatus.WAITING_RECOVERY
            and node.status is NodeStatus.WAITING_RECOVERY
        ):
            return node
        if node.status is not NodeStatus.RUNNING:
            raise SchedulerStateError(
                f"hierarchy recovery requires RUNNING Node, got {node.status.value}"
            )
        code = _safe_hierarchy_error_code(error_code)
        self.store.append_event(
            run_id,
            "node.waiting_recovery",
            node_id=node_id,
            occurred_at=self._now(),
            expected_run_version=run.projection_version,
            expected_node_version=node.projection_version,
            run_projection=replace(
                run,
                status=RunStatus.WAITING_RECOVERY,
                error={"code": code},
            ),
            node_projection=replace(
                node,
                status=NodeStatus.WAITING_RECOVERY,
                error={"code": code},
            ),
            payload={
                "control_plane": "hierarchy_integrity_failure",
                "error_code": code,
            },
        )
        return self._require_node(run_id, node_id)

    def _advance_one(self, run: RunRecord) -> bool:
        run_id = run.run_id
        nodes = {node.node_id: node for node in self.store.list_nodes(run_id)}
        active = self.store.list_attempts(run_id)
        active_by_node = {
            attempt.node_id
            for attempt in active
            if attempt.status in ACTIVE_ATTEMPT_STATUSES
        }

        fatal_failure = any(
            node.status is NodeStatus.FAILED
            and self.workflow.get_node(node.node_id).on_error == "fail_run"
            for node in nodes.values()
        )
        if fatal_failure:
            for node_id in self.workflow.topological_order:
                node = nodes[node_id]
                if node.node_id in active_by_node:
                    continue
                if node.status in {NodeStatus.PENDING, NodeStatus.READY}:
                    self._transition_node(
                        run_id,
                        node_id,
                        NodeStatus.SKIPPED,
                        "node.skipped",
                        payload={"reason": "upstream_fail_run"},
                        expected_run=run,
                        expected_node=node,
                    )
                    return True
                if node.status in WAITING_NODE_STATUSES:
                    self._transition_node(
                        run_id,
                        node_id,
                        NodeStatus.CANCELLED,
                        "node.cancelled",
                        payload={"reason": "upstream_fail_run"},
                        expected_run=run,
                        expected_node=node,
                    )
                    return True

        now = self._now()
        for node_id in self.workflow.topological_order:
            node = nodes[node_id]
            definition = self.workflow.get_node(node_id)
            if node.status is NodeStatus.PAUSED:
                self._transition_node(
                    run_id,
                    node_id,
                    NodeStatus.READY,
                    "node.ready",
                    payload={"reason": "run_resumed"},
                    expected_run=run,
                    expected_node=node,
                )
                return True
            if node.status is NodeStatus.WAITING_RETRY:
                due_at = node.metadata.get("retry_due_at")
                if (
                    isinstance(due_at, bool)
                    or not isinstance(due_at, (int, float))
                    or not math.isfinite(float(due_at))
                    or float(due_at) < 0
                ):
                    raise SchedulerStateError(
                        "persisted retry deadline is invalid"
                    )
                if now >= float(due_at):
                    self._transition_node(
                        run_id,
                        node_id,
                        NodeStatus.READY,
                        "node.ready",
                        payload={"reason": "retry_due"},
                        expected_run=run,
                        expected_node=node,
                    )
                    return True
                continue
            if node.status is NodeStatus.PENDING:
                action = self._pending_action(definition, nodes)
                if action == "ready":
                    self._transition_node(
                        run_id,
                        node_id,
                        NodeStatus.READY,
                        "node.ready",
                        payload={"reason": "dependencies_satisfied"},
                        expected_run=run,
                        expected_node=node,
                    )
                    return True
                if action == "skip":
                    self._transition_node(
                        run_id,
                        node_id,
                        NodeStatus.SKIPPED,
                        "node.skipped",
                        payload={"reason": "branch_or_dependency_skipped"},
                        expected_run=run,
                        expected_node=node,
                    )
                    return True
                continue
            if definition.kind in CONTROL_KINDS and node_id not in active_by_node:
                if self._advance_control(run, definition, node):
                    return True
        return False

    def _pending_action(
        self,
        definition: NodeDefinition,
        nodes: Mapping[str, NodeRecord],
    ) -> str | None:
        if not definition.depends_on:
            return "ready"
        dependency_records = [nodes[node_id] for node_id in definition.depends_on]

        for dependency in dependency_records:
            dep_definition = self.workflow.get_node(dependency.node_id)
            if dep_definition.kind != "router" or dependency.status is not NodeStatus.SUCCEEDED:
                continue
            route_targets = self._router_targets(dep_definition)
            if definition.node_id in route_targets:
                selected = dependency.metadata.get("selected_target")
                if selected != definition.node_id:
                    return "skip"

        for dependency in dependency_records:
            if dependency.status is NodeStatus.SKIPPED and definition.kind != "join":
                return "skip"
            if dependency.status in {NodeStatus.FAILED, NodeStatus.CANCELLED}:
                policy = self.workflow.get_node(dependency.node_id).on_error
                if policy != "continue":
                    return "skip"
            elif not dependency.status.is_terminal:
                return None

        if definition.kind == "join":
            succeeded = sum(
                dependency.status is NodeStatus.SUCCEEDED
                for dependency in dependency_records
            )
            mode = definition.config.get("mode", "all")
            if mode == "any" and succeeded == 0:
                return "skip"
            if succeeded == 0 and all(
                dependency.status is NodeStatus.SKIPPED
                for dependency in dependency_records
            ):
                return "skip"
        return "ready"

    def _advance_control(
        self,
        run: RunRecord,
        definition: NodeDefinition,
        node: NodeRecord,
    ) -> bool:
        run_id = run.run_id
        if node.status is NodeStatus.READY:
            if definition.kind == "approval" and node.metadata.get(
                "approval_granted"
            ) is False:
                self._transition_node(
                    run_id,
                    node.node_id,
                    NodeStatus.SKIPPED,
                    "node.skipped",
                    payload={"reason": "approval_rejected"},
                    expected_run=run,
                    expected_node=node,
                )
                return True
            self._transition_node(
                run_id,
                node.node_id,
                NodeStatus.RUNNING,
                "node.started",
                payload={"control_plane": definition.kind},
                expected_run=run,
                expected_node=node,
            )
            return True
        if node.status is not NodeStatus.RUNNING:
            return False

        if definition.kind == "router":
            selected = node.metadata.get("selected_target")
            if selected is None:
                self._transition_node(
                    run_id,
                    node.node_id,
                    NodeStatus.WAITING_INPUT,
                    "node.waiting_input",
                    payload={"reason": "router_decision_required"},
                    expected_run=run,
                    expected_node=node,
                )
            else:
                self._succeed_control(
                    run,
                    node,
                    {"selected_target": selected},
                )
            return True
        if definition.kind == "approval":
            approved = node.metadata.get("approval_granted")
            if approved is not True:
                self._transition_node(
                    run_id,
                    node.node_id,
                    NodeStatus.WAITING_APPROVAL,
                    "node.waiting_approval",
                    payload={"reason": "approval_required"},
                    expected_run=run,
                    expected_node=node,
                )
            else:
                self._succeed_control(
                    run,
                    node,
                    {"approved": True},
                )
            return True
        if definition.kind in {"map", "subworkflow"}:
            if self._hierarchy_controller is not None:
                return self._hierarchy_controller.reconcile_control(
                    self,
                    run_id,
                    definition,
                    node,
                )
            if node.metadata.get("control_resolved") is not True:
                self._transition_node(
                    run_id,
                    node.node_id,
                    NodeStatus.WAITING_INPUT,
                    "node.waiting_input",
                    payload={"reason": f"{definition.kind}_runtime_required"},
                    expected_run=run,
                    expected_node=node,
                )
            else:
                self._succeed_control(
                    run,
                    node,
                    node.metadata.get("control_result"),
                )
            return True
        if definition.kind == "join" and definition.input_mapping:
            bindings = self._resolve_input_artifacts(run, definition)
            refs = tuple(
                ref
                for _name, values in bindings
                for ref in values
            )
            self._succeed_control(
                run,
                node,
                {
                    "outcome": "succeeded",
                    "artifact_refs": [ref.to_dict() for ref in refs],
                },
            )
            return True
        self._succeed_control(
            run,
            node,
            {"control_plane": definition.kind, "completed": True},
        )
        return True

    def _succeed_control(
        self,
        run: RunRecord,
        node: NodeRecord,
        output: Any,
    ) -> None:
        self._transition_node(
            run.run_id,
            node.node_id,
            NodeStatus.SUCCEEDED,
            "node.succeeded",
            output=output,
            payload={"control_plane": "completed"},
            expected_run=run,
            expected_node=node,
        )

    def _schedule_attempt(
        self,
        run: RunRecord,
        node: NodeRecord,
        definition: NodeDefinition,
        previous_attempts: Sequence[AttemptRecord],
    ) -> AttemptRecord:
        attempt_number = max(
            (attempt.attempt_number for attempt in previous_attempts),
            default=0,
        ) + 1
        input_bindings = self._resolve_input_artifacts(run, definition)
        request_hash, input_digest = self._request_hash(
            run,
            definition,
            input_bindings=input_bindings,
        )
        operation_key = self._render_operation_key(
            run,
            definition,
            input_digest=input_digest,
        )
        # The existing store uses Attempt.idempotency_key as its claim-record
        # primary key. Keep that claim identity attempt-scoped so a completed
        # failed Attempt cannot block a later retry, while exposing the stable
        # business operation key separately to the Activity adapter.
        claim_key = f"{operation_key}:attempt:{attempt_number}"
        attempt_id = str(self._id_factory("attempt"))
        now = self._now()
        timeout_policy = definition.timeout_policy.to_dict()
        schedule_deadline = node.metadata.get("schedule_deadline_at")
        attempt = AttemptRecord(
            attempt_id=attempt_id,
            run_id=run.run_id,
            node_id=node.node_id,
            attempt_number=attempt_number,
            idempotency_key=claim_key,
            activity_kind=definition.kind,
            effect_class=definition.effect_class,
            metadata={
                "definition_digest": self.workflow.definition_digest,
                "request_hash": request_hash,
                "input_mapping_digest": self._input_mapping_digest(
                    definition,
                    input_bindings,
                ),
                "operation_key": operation_key,
                "resource_keys": list(definition.resource_keys),
                "concurrency_key": definition.concurrency_key,
                "timeout_policy": timeout_policy,
                **(
                    {"schedule_deadline_at": float(schedule_deadline)}
                    if isinstance(schedule_deadline, (int, float))
                    else {}
                ),
            },
            scheduled_at=now,
        )
        try:
            self.store.append_event(
                run.run_id,
                "attempt.scheduled",
                payload={
                    "attempt_number": attempt_number,
                    "operation_key_digest": hashlib.sha256(
                        operation_key.encode("utf-8")
                    ).hexdigest(),
                },
                node_id=node.node_id,
                attempt_id=attempt.attempt_id,
                event_id=f"evt_schedule_{attempt.attempt_id}",
                occurred_at=now,
                expected_run_version=run.projection_version,
                expected_node_version=node.projection_version,
                node_projection=replace(
                    node,
                    attempt_count=max(node.attempt_count, attempt_number),
                ),
                attempt_projection=attempt,
            )
        except ConcurrentProjectionUpdate:
            existing = [
                item
                for item in self.store.list_attempts(run.run_id, node_id=node.node_id)
                if item.status in ACTIVE_ATTEMPT_STATUSES
            ]
            if (
                len(existing) != 1
                or existing[0].attempt_number != attempt_number
                or existing[0].idempotency_key != claim_key
                or existing[0].activity_kind != definition.kind
                or existing[0].effect_class != definition.effect_class
                or existing[0].metadata.get("definition_digest")
                != self.workflow.definition_digest
                or existing[0].metadata.get("request_hash") != request_hash
                or existing[0].metadata.get("operation_key") != operation_key
            ):
                raise
            return existing[0]
        stored = self.store.get_attempt(attempt.attempt_id)
        if stored is None:
            raise SchedulerStateError("scheduled Attempt was not persisted")
        return stored

    def _claim_scheduled(
        self,
        run: RunRecord,
        definition: NodeDefinition,
        attempt: AttemptRecord,
        worker_id: str,
        lease_seconds: float,
        *,
        capacity: int,
    ) -> ActivityClaim | None:
        request_hash = attempt.metadata.get("request_hash")
        operation_key = attempt.metadata.get("operation_key")
        if not isinstance(request_hash, str) or not isinstance(operation_key, str):
            raise DefinitionMismatchError(
                f"attempt {attempt.attempt_id} lacks scheduler identity metadata"
            )
        claim, _event = self.store.claim_activity(
            run.run_id,
            definition.node_id,
            attempt.attempt_id,
            request_hash,
            worker_id,
            lease_seconds=lease_seconds,
            now=self._now(),
            max_active_attempts=self.max_active_attempts,
            worker_capacity=capacity,
        )
        if claim.disposition is not ClaimDisposition.ACQUIRED:
            return None
        return self._claim_handle(definition, attempt, claim, worker_id)

    def _claim_handle(
        self,
        definition: NodeDefinition,
        attempt: AttemptRecord,
        claim: IdempotencyClaim,
        worker_id: str,
    ) -> ActivityClaim:
        request_hash = attempt.metadata["request_hash"]
        operation_key = attempt.metadata["operation_key"]
        assert isinstance(request_hash, str)
        assert isinstance(operation_key, str)
        run = self._require_run(attempt.run_id)
        input_bindings = self._resolve_input_artifacts(run, definition)
        expected_hash, _input_digest = self._request_hash(
            run,
            definition,
            input_bindings=input_bindings,
        )
        if expected_hash != request_hash:
            raise DefinitionMismatchError(
                f"attempt {attempt.attempt_id} input mapping no longer matches "
                "its durable request hash"
            )
        input_refs = tuple(
            ref
            for _name, refs in input_bindings
            for ref in refs
        )
        return ActivityClaim(
            run_id=attempt.run_id,
            node_id=attempt.node_id,
            attempt_id=attempt.attempt_id,
            attempt_number=attempt.attempt_number,
            worker_id=worker_id,
            request_hash=request_hash,
            claim_token=claim.record.claim_token,
            fencing_token=claim.record.claim_count,
            lease_expires_at=claim.record.lease_expires_at,
            operation_key=operation_key,
            idempotency_key=operation_key,
            claim_key=attempt.idempotency_key,
            activity_kind=definition.kind,
            effect_class=definition.effect_class,
            resource_keys=definition.resource_keys,
            config=definition.config,
            input_artifact_bindings=input_bindings,
            input_artifact_refs=input_refs,
        )

    def _retry_allowed(
        self,
        claim: ActivityClaim,
        status: AttemptStatus,
        error_class: str | None,
    ) -> bool:
        if status is AttemptStatus.OUTCOME_UNKNOWN or error_class is None:
            return False
        definition = self.workflow.get_node(claim.node_id)
        if definition.effect_class not in {"read_only", "idempotent_write"}:
            return False
        if error_class not in definition.retry_policy.retry_on:
            return False
        if claim.attempt_number >= definition.retry_policy.max_attempts:
            return False
        attempts = self.store.list_attempts(claim.run_id, node_id=claim.node_id)
        first_scheduled = min(
            (attempt.scheduled_at for attempt in attempts),
            default=self._now(),
        )
        due_at = self._now() + self._retry_delay_seconds(definition, claim)
        max_elapsed_ms = definition.retry_policy.max_elapsed_ms
        return (
            max_elapsed_ms is None
            or due_at <= first_scheduled + max_elapsed_ms / 1000.0
        )

    def _complete_retryable_failure(
        self,
        claim: ActivityClaim,
        *,
        attempt_status: AttemptStatus,
        error_class: str,
        error_code: str,
    ) -> Any:
        now = self._now()
        due_at = now + self._retry_delay_seconds(
            self.workflow.get_node(claim.node_id),
            claim,
        )
        complete_retryable = getattr(
            self.store,
            "complete_retryable_activity",
            None,
        )
        if not callable(complete_retryable):
            raise StoreCapabilityError(
                "store must implement atomic complete_retryable_activity"
            )
        return complete_retryable(
            claim.run_id,
            claim.node_id,
            claim.attempt_id,
            claim.request_hash,
            claim.worker_id,
            claim_token=claim.claim_token,
            error_class=error_class,
            error_code=error_code,
            retry_due_at=due_at,
            attempt_status=attempt_status,
            now=now,
        )

    def _prepare_success_receipt(
        self,
        claim: ActivityClaim,
        raw_result: Any,
    ) -> dict[str, Any]:
        if isinstance(raw_result, ActivityReceipt):
            receipt = raw_result
        else:
            if self._result_writer is None:
                raise ResultPersistenceError(
                    "successful Activity output requires an Artifact result_writer"
                )
            receipt = self._result_writer(claim, raw_result)
        if not isinstance(receipt, ActivityReceipt):
            raise ResultPersistenceError(
                "result_writer must return an ActivityReceipt"
            )
        return self.verify_activity_receipt(receipt)

    def _prepare_run_input(self, run_id: str, raw_input: Any) -> Any:
        if raw_input is None:
            return None
        receipt = self.prepare_run_input_receipt(run_id, raw_input)
        normalized = normalize_json(receipt.to_dict(), "run input receipt")
        if not isinstance(normalized, dict):
            raise InputPersistenceError("Run input receipt must be a JSON object")
        return normalized

    def _verify_artifact_refs(
        self,
        refs: tuple[ArtifactRef, ...],
        *,
        error_type: type[SchedulerError],
    ) -> tuple[ArtifactRef, ...]:
        if self._artifact_verifier is None:
            raise error_type("Artifact receipts require an artifact_verifier")
        verified: list[ArtifactRef] = []
        for ref in refs:
            try:
                detached = ArtifactRef.from_dict(ref.to_dict())
                valid = self._artifact_verifier(detached)
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                raise error_type(
                    f"Artifact verification failed: {type(exc).__name__}"
                ) from exc
            if valid is not True:
                raise error_type("Artifact verifier rejected a reference")
            verified.append(detached)
        return tuple(verified)

    @staticmethod
    def _safe_failure_receipt(
        status: AttemptStatus,
        error_class: str | None,
        error_code: str,
    ) -> dict[str, str]:
        return {
            "outcome": status.value,
            "error_class": _safe_error_class(error_class),
            "error_code": error_code,
        }

    def _retry_delay_seconds(
        self,
        definition: NodeDefinition,
        claim: ActivityClaim,
    ) -> float:
        policy = definition.retry_policy
        exponent = max(0, claim.attempt_number - 1)
        delay_ms = min(
            float(policy.max_delay_ms),
            float(policy.initial_delay_ms)
            * (float(policy.backoff_multiplier) ** exponent),
        )
        if policy.jitter:
            digest = hashlib.sha256(claim.attempt_id.encode("utf-8")).digest()
            unit = int.from_bytes(digest[:8], "big") / float(2**64 - 1)
            delay_ms *= 1.0 + policy.jitter * ((2.0 * unit) - 1.0)
        return max(0.0, delay_ms / 1000.0)

    @staticmethod
    def _worker_has_resources(
        definition: NodeDefinition,
        *,
        available_resources: frozenset[str] | None,
    ) -> bool:
        requested = frozenset(definition.resource_keys)
        return available_resources is None or requested.issubset(available_resources)

    def _reconcile_pausing(self, run: RunRecord) -> bool:
        run_id = run.run_id
        if self._hierarchy_controller is not None:
            decision = self._hierarchy_controller.reconcile_pausing(self, run_id)
            if decision is not None:
                return decision
        for attempt in self.store.list_attempts(run_id):
            if attempt.status is not AttemptStatus.SCHEDULED:
                continue
            now = self._now()
            self.store.append_event(
                run_id,
                "attempt.cancelled",
                payload={"reason": "pause_before_claim"},
                node_id=attempt.node_id,
                attempt_id=attempt.attempt_id,
                occurred_at=now,
                expected_run_version=run.projection_version,
                expected_attempt_version=attempt.projection_version,
                attempt_projection=replace(
                    attempt,
                    status=AttemptStatus.CANCELLED,
                    finished_at=now,
                ),
            )
            return True
        active = [
            attempt
            for attempt in self.store.list_attempts(run_id)
            if attempt.status in ACTIVE_ATTEMPT_STATUSES
        ]
        if active:
            return False
        self._transition_run(
            run_id,
            RunStatus.PAUSED,
            "run.paused",
            expected_run=run,
        )
        return True

    def _reconcile_cancelling(self, run: RunRecord) -> bool:
        run_id = run.run_id
        if self._hierarchy_controller is not None:
            decision = self._hierarchy_controller.reconcile_cancelling(self, run_id)
            if decision is not None:
                return decision
        attempts = self.store.list_attempts(run_id)
        active_by_node = {
            attempt.node_id: attempt
            for attempt in attempts
            if attempt.status in ACTIVE_ATTEMPT_STATUSES
        }
        for node_id in self.workflow.topological_order:
            node = self._require_node(run_id, node_id)
            active = active_by_node.get(node_id)
            if active is not None:
                if active.status in {
                    AttemptStatus.SCHEDULED,
                    AttemptStatus.WAITING_APPROVAL,
                }:
                    now = self._now()
                    self.store.append_event(
                        run_id,
                        "attempt.cancelled",
                        payload={"reason": "run_cancelled_before_claim"},
                        node_id=node_id,
                        attempt_id=active.attempt_id,
                        occurred_at=now,
                        expected_run_version=run.projection_version,
                        expected_node_version=node.projection_version,
                        expected_attempt_version=active.projection_version,
                        node_projection=replace(node, status=NodeStatus.CANCELLED),
                        attempt_projection=replace(
                            active,
                            status=AttemptStatus.CANCELLED,
                            finished_at=now,
                        ),
                    )
                    return True
                continue
            if not node.status.is_terminal:
                self._transition_node(
                    run_id,
                    node_id,
                    NodeStatus.CANCELLED,
                    "node.cancelled",
                    payload={"reason": "run_cancelled"},
                    expected_run=run,
                    expected_node=node,
                )
                return True
        if any(
            attempt.status in ACTIVE_ATTEMPT_STATUSES
            for attempt in self.store.list_attempts(run_id)
        ):
            return False
        self._transition_run(
            run_id,
            RunStatus.CANCELLED,
            "run.cancelled",
            payload={"reason": "cancellation_converged"},
            expected_run=run,
        )
        return True

    def _finalize_if_terminal(self, run_id: str) -> bool:
        run = self._require_run(run_id)
        if run.status is not RunStatus.RUNNING:
            return False
        nodes = self.store.list_nodes(run_id)
        if not nodes or any(not node.status.is_terminal for node in nodes):
            return False
        if any(
            attempt.status in ACTIVE_ATTEMPT_STATUSES
            for attempt in self.store.list_attempts(run_id)
        ):
            return False
        failed = any(
            node.status in {NodeStatus.FAILED, NodeStatus.CANCELLED}
            for node in nodes
        )
        if failed:
            self._transition_run(
                run_id,
                RunStatus.FAILED,
                "run.failed",
                error={"code": "workflow_node_failed"},
                expected_run=run,
            )
        else:
            self._transition_run(
                run_id,
                RunStatus.COMPLETED,
                "run.completed",
                payload={"definition_digest": self.workflow.definition_digest},
                expected_run=run,
            )
        return True

    def _ensure_nodes(self, run_id: str) -> None:
        run = self._require_run(run_id)
        existing = {node.node_id: node for node in self.store.list_nodes(run_id)}
        unknown = sorted(set(existing).difference(self.workflow.topological_order))
        if unknown:
            raise DefinitionMismatchError(
                f"Run {run_id} contains unknown Nodes: {', '.join(unknown)}"
            )
        for node_id in self.workflow.topological_order:
            definition = self.workflow.get_node(node_id)
            current = existing.get(node_id)
            if current is not None:
                if (
                    current.node_type != definition.kind
                    or current.metadata.get("definition_digest")
                    != self.workflow.definition_digest
                ):
                    raise DefinitionMismatchError(
                        f"Node {run_id}/{node_id} does not match compiled definition"
                    )
                continue
            now = self._now()
            node = NodeRecord(
                run_id=run_id,
                node_id=node_id,
                node_type=definition.kind,
                metadata={
                    "definition_digest": self.workflow.definition_digest,
                    "topological_index": self._topology_index[node_id],
                },
                created_at=now,
                updated_at=now,
            )
            run = self._require_run(run_id)
            event_id = "evt_node_created_" + hashlib.sha256(
                f"{run_id}\0{node_id}\0{self.workflow.definition_digest}".encode()
            ).hexdigest()
            try:
                self.store.append_event(
                    run_id,
                    "node.created",
                    payload={"definition_digest": self.workflow.definition_digest},
                    node_id=node_id,
                    event_id=event_id,
                    occurred_at=now,
                    expected_run_version=run.projection_version,
                    node_projection=node,
                )
            except ConcurrentProjectionUpdate:
                raced = self.store.get_node(run_id, node_id)
                if (
                    raced is None
                    or raced.node_type != definition.kind
                    or raced.metadata.get("definition_digest")
                    != self.workflow.definition_digest
                    or raced.metadata.get("topological_index")
                    != self._topology_index[node_id]
                ):
                    raise
            except ProjectionConflictError as exc:
                expected_collision = (
                    f"event_id {event_id!r} already has different content"
                )
                if str(exc) != expected_collision:
                    raise
                raced = self.store.get_node(run_id, node_id)
                if (
                    raced is None
                    or raced.node_type != definition.kind
                    or raced.metadata.get("definition_digest")
                    != self.workflow.definition_digest
                    or raced.metadata.get("topological_index")
                    != self._topology_index[node_id]
                ):
                    raise

    def _require_run(self, run_id: str) -> RunRecord:
        run = self.store.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id)
        if (
            run.workflow_id != self.workflow.name
            or run.workflow_version != self.workflow.version
            or run.definition_digest != self.workflow.definition_digest
        ):
            raise DefinitionMismatchError(
                f"Run {run_id} does not match compiled Workflow definition"
            )
        return run

    def _require_node(self, run_id: str, node_id: str) -> NodeRecord:
        self._require_run(run_id)
        node = self.store.get_node(run_id, node_id)
        if node is None:
            raise SchedulerStateError(f"Node not found: {run_id}/{node_id}")
        return node

    def _transition_run(
        self,
        run_id: str,
        status: RunStatus,
        event_type: str,
        *,
        payload: Mapping[str, Any] | None = None,
        error: Any = None,
        expected_run: RunRecord | None = None,
    ) -> Any:
        run = (
            self._require_run(run_id)
            if expected_run is None
            else expected_run
        )
        target = replace(
            run,
            status=status,
            error=normalize_json(error, "run error") if error is not None else run.error,
        )
        return self.store.append_event(
            run_id,
            event_type,
            payload=dict(payload or {}),
            event_id=str(self._id_factory("event")),
            occurred_at=self._now(),
            expected_run_version=run.projection_version,
            run_projection=target,
        )

    def _transition_node(
        self,
        run_id: str,
        node_id: str,
        status: NodeStatus,
        event_type: str,
        *,
        metadata_update: Mapping[str, Any] | None = None,
        output: Any = None,
        error: Any = None,
        payload: Mapping[str, Any] | None = None,
        expected_run: RunRecord | None = None,
        expected_node: NodeRecord | None = None,
    ) -> Any:
        run = self._require_run(run_id) if expected_run is None else expected_run
        node = (
            self._require_node(run_id, node_id)
            if expected_node is None
            else expected_node
        )
        if run.run_id != run_id:
            raise SchedulerStateError("expected Run snapshot has the wrong identity")
        if node.run_id != run_id or node.node_id != node_id:
            raise SchedulerStateError("expected Node snapshot has the wrong identity")
        metadata = dict(node.metadata)
        metadata.update(metadata_update or {})
        occurred_at = self._now()
        if status is NodeStatus.READY:
            definition = self.workflow.get_node(node_id)
            if definition.kind in ACTIVITY_KINDS:
                metadata["ready_at"] = occurred_at
                schedule_timeout = definition.timeout_policy.schedule_timeout_ms
                if schedule_timeout is not None:
                    schedule_deadline = occurred_at + schedule_timeout / 1_000
                    run_deadline = run.metadata.get("deadline_at")
                    if isinstance(run_deadline, (int, float)):
                        schedule_deadline = min(schedule_deadline, float(run_deadline))
                    metadata["schedule_deadline_at"] = schedule_deadline
                else:
                    metadata.pop("schedule_deadline_at", None)
        target = replace(
            node,
            status=status,
            metadata=metadata,
            output=normalize_json(output, "node output")
            if output is not None
            else node.output,
            error=normalize_json(error, "node error")
            if error is not None
            else node.error,
        )
        return self.store.append_event(
            run_id,
            event_type,
            payload=dict(payload or {}),
            node_id=node_id,
            event_id=str(self._id_factory("event")),
            occurred_at=occurred_at,
            expected_run_version=run.projection_version,
            expected_node_version=node.projection_version,
            node_projection=target,
        )

    def _request_hash(
        self,
        run: RunRecord,
        definition: NodeDefinition,
        *,
        input_bindings: tuple[
            tuple[str, tuple[ArtifactRef, ...]],
            ...,
        ] | None = None,
    ) -> tuple[str, str]:
        if input_bindings is None:
            input_bindings = self._resolve_input_artifacts(run, definition)
        input_json = _canonical_json(run.input)
        run_input_digest = hashlib.sha256(input_json).hexdigest()
        mapping_identity = self._input_mapping_identity(
            definition,
            input_bindings,
        )
        input_digest = (
            run_input_digest
            if not definition.input_mapping
            else hashlib.sha256(_canonical_json(mapping_identity)).hexdigest()
        )
        request = {
            "definition_digest": self.workflow.definition_digest,
            "node_id": definition.node_id,
            "kind": definition.kind,
            "config": definition.config.to_dict(),
            "run_input_digest": run_input_digest,
        }
        if definition.input_mapping:
            request["input_mapping"] = mapping_identity
        return hashlib.sha256(_canonical_json(request)).hexdigest(), input_digest

    def _resolve_input_artifacts(
        self,
        run: RunRecord,
        definition: NodeDefinition,
    ) -> tuple[tuple[str, tuple[ArtifactRef, ...]], ...]:
        bindings: list[tuple[str, tuple[ArtifactRef, ...]]] = []
        total_refs = 0
        for input_name, raw_selector in definition.input_mapping.items():
            if not isinstance(raw_selector, Mapping):
                raise DefinitionMismatchError(
                    f"node {definition.node_id} has an invalid compiled input mapping"
                )
            source = raw_selector.get("source")
            if source == "run_input":
                raw_receipt = run.input
                source_name = "Run input"
            elif source == "node_output":
                source_node_id = raw_selector.get("node_id")
                if not isinstance(source_node_id, str):
                    raise DefinitionMismatchError(
                        f"node {definition.node_id} has an invalid upstream selector"
                    )
                source_node = self._require_node(run.run_id, source_node_id)
                raw_receipt = source_node.output
                source_name = f"Node {source_node_id} output"
            else:
                raise DefinitionMismatchError(
                    f"node {definition.node_id} has an unknown input source"
                )
            refs = self._artifact_refs_from_receipt(
                raw_receipt,
                source_name=source_name,
            )
            index = raw_selector.get("artifact_index")
            if index is not None:
                if (
                    isinstance(index, bool)
                    or not isinstance(index, int)
                    or index < 0
                    or index >= len(refs)
                ):
                    raise InputMappingError(
                        f"node {definition.node_id} input {input_name} "
                        f"Artifact index is out of range"
                    )
                refs = (refs[index],)
            total_refs += len(refs)
            if total_refs > 64:
                raise InputMappingError(
                    f"node {definition.node_id} resolved input exceeds "
                    "64 ArtifactRefs"
                )
            bindings.append((str(input_name), refs))
        return tuple(bindings)

    def _artifact_refs_from_receipt(
        self,
        raw_receipt: Any,
        *,
        source_name: str,
    ) -> tuple[ArtifactRef, ...]:
        if not isinstance(raw_receipt, Mapping):
            raise InputMappingError(f"{source_name} has no Artifact receipt")
        raw_refs = raw_receipt.get("artifact_refs")
        if (
            not isinstance(raw_refs, list)
            or not raw_refs
            or len(raw_refs) > 64
        ):
            raise InputMappingError(
                f"{source_name} must contain 1 to 64 ArtifactRefs"
            )
        try:
            refs = tuple(ArtifactRef.from_dict(value) for value in raw_refs)
        except Exception as exc:
            raise InputMappingError(
                f"{source_name} contains an invalid ArtifactRef"
            ) from exc
        return self._verify_artifact_refs(
            refs,
            error_type=InputMappingError,
        )

    @staticmethod
    def _input_mapping_identity(
        definition: NodeDefinition,
        input_bindings: tuple[
            tuple[str, tuple[ArtifactRef, ...]],
            ...,
        ],
    ) -> dict[str, Any]:
        resolved = {
            name: [_artifact_identity(ref) for ref in refs]
            for name, refs in input_bindings
        }
        return {
            "declaration": definition.input_mapping.to_dict(),
            "resolved": resolved,
        }

    def _input_mapping_digest(
        self,
        definition: NodeDefinition,
        input_bindings: tuple[
            tuple[str, tuple[ArtifactRef, ...]],
            ...,
        ],
    ) -> str:
        identity = self._input_mapping_identity(definition, input_bindings)
        return hashlib.sha256(_canonical_json(identity)).hexdigest()

    def _render_operation_key(
        self,
        run: RunRecord,
        definition: NodeDefinition,
        *,
        input_digest: str,
    ) -> str:
        values = {
            "workflow_id": self.workflow.name,
            "workflow_version": str(self.workflow.version),
            "run_id": run.run_id,
            "node_id": definition.node_id,
            "logical_operation_key": definition.node_id,
            "input_digest": input_digest,
        }
        template = definition.idempotency_key_template
        if template is None:
            template = (
                "{{workflow_id}}:{{workflow_version}}:{{run_id}}:"
                "{{node_id}}:{{logical_operation_key}}"
            )
        rendered = TEMPLATE_RE.sub(lambda match: values[match.group(1)], template)
        if not rendered or len(rendered) > MAX_OPERATION_KEY_CHARS:
            raise SchedulerStateError(
                f"node {definition.node_id} rendered invalid operation key"
            )
        return rendered

    def _router_targets(self, definition: NodeDefinition) -> set[str]:
        routes = definition.config["routes"]
        assert isinstance(routes, Mapping)
        targets = {str(value) for value in routes.values()}
        default = definition.config.get("default_route")
        if default is not None:
            targets.add(str(default))
        return targets

    def _validate_claim_handle(self, claim: ActivityClaim) -> None:
        run = self._require_run(claim.run_id)
        definition = self.workflow.get_node(claim.node_id)
        input_bindings = self._resolve_input_artifacts(run, definition)
        input_refs = tuple(
            ref
            for _name, refs in input_bindings
            for ref in refs
        )
        attempt = self.store.get_attempt(claim.attempt_id)
        claim_record = (
            None
            if attempt is None
            else self.store.get_idempotency(
                claim.run_id,
                attempt.idempotency_key,
            )
        )
        if (
            run.run_id != claim.run_id
            or attempt is None
            or attempt.run_id != claim.run_id
            or attempt.node_id != claim.node_id
            or attempt.attempt_number != claim.attempt_number
            or definition.kind != claim.activity_kind
            or claim.input_artifact_bindings != input_bindings
            or claim.input_artifact_refs != input_refs
            or attempt.metadata.get("request_hash") != claim.request_hash
            or attempt.metadata.get("operation_key") != claim.operation_key
            or claim_record is None
            or claim_record.request_hash != claim.request_hash
            or claim_record.owner_id != claim.worker_id
            or claim_record.claim_token != claim.claim_token
            or claim_record.claim_count != claim.fencing_token
            # A heartbeat may monotonically extend the same token/fencing
            # authority between this method's read steps.  An older deadline
            # is therefore a valid view of the same claim; a worker-supplied
            # future deadline is not.
            or claim.lease_expires_at > claim_record.lease_expires_at
        ):
            raise SchedulerStateError("claim handle does not match durable projections")
        if self._now() >= claim_record.lease_expires_at:
            raise SchedulerStateError(
                "claim lease expired; recovery scanner must resolve it"
            )

    def _scheduler_for_claim(self, claim: ActivityClaim) -> "DurableScheduler":
        run = self.store.get_run(claim.run_id)
        if run is None:
            return self
        if (
            run.workflow_id == self.workflow.name
            and run.workflow_version == self.workflow.version
            and run.definition_digest == self.workflow.definition_digest
        ):
            return self
        if self._hierarchy_controller is None:
            return self
        return self._hierarchy_controller.scheduler_for_run(self, claim.run_id)

    def _snapshot(self, run_id: str) -> ReconcileResult:
        run = self._require_run(run_id)
        nodes_by_id = {
            node.node_id: node for node in self.store.list_nodes(run_id)
        }
        nodes = tuple(
            nodes_by_id[node_id] for node_id in self.workflow.topological_order
        )
        ready = tuple(
            node.node_id for node in nodes if node.status is NodeStatus.READY
        )
        active = tuple(
            attempt.attempt_id
            for attempt in self.store.list_attempts(run_id)
            if attempt.status in ACTIVE_ATTEMPT_STATUSES
        )
        return ReconcileResult(
            run=run,
            nodes=nodes,
            ready=ready,
            active_attempts=active,
        )

    def _now(self) -> float:
        value = float(self._clock())
        if value < 0:
            raise ValueError("clock must return a non-negative timestamp")
        return value


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _artifact_identity(ref: ArtifactRef) -> dict[str, Any]:
    return {
        "artifact_id": ref.artifact_id,
        "sha256": ref.sha256,
        "size": ref.size,
        "kind": ref.kind.value,
        "media_type": ref.media_type,
        "uri": ref.uri,
        "sensitivity": ref.sensitivity.value,
        "encryption": ref.encryption.value,
        "encryption_key_ref": ref.encryption_key_ref,
        "producer_run_id": ref.producer_run_id,
        "producer_node_id": ref.producer_node_id,
        "producer_attempt_id": ref.producer_attempt_id,
    }


def _safe_error_class(value: str | None) -> str:
    if value is None:
        return "unspecified"
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", value):
        raise ValueError(
            "error_class must match [a-z][a-z0-9_]{0,63}"
        )
    return value


def _safe_error_code(raw_result: Any) -> str:
    value = raw_result.get("error_code") if isinstance(raw_result, Mapping) else None
    if value is None:
        return "activity_failed"
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_.-]{0,95}", value):
        return "activity_failed"
    return value


def _safe_hierarchy_error_code(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"[a-z][a-z0-9_.-]{0,95}",
        value,
    ):
        raise ValueError(
            "hierarchy error_code must match [a-z][a-z0-9_.-]{0,95}"
        )
    return value


__all__ = [
    "ActivityClaim",
    "ActivityReceipt",
    "ApprovalResolution",
    "DefinitionMismatchError",
    "DurableScheduler",
    "HierarchyController",
    "InputMappingError",
    "InputPersistenceError",
    "ReconcileResult",
    "ResultPersistenceError",
    "RunInputReceipt",
    "RunAlreadyExistsError",
    "SchedulerError",
    "SchedulerStateError",
    "StoreCapabilityError",
]
