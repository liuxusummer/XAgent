"""Durable subworkflow and bounded-map orchestration.

Hierarchy reconciliation creates deterministic child Runs, persists bounded
parent/child bindings, and delegates each child to the ordinary durable
scheduler.  It never executes an external Activity and never enters a Store
private transaction.
"""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from .artifacts import (
    ArtifactKind,
    ArtifactRef,
    ArtifactStore,
)
from .models import NodeRecord, NodeStatus, RunRecord, RunStatus
from .scheduler import (
    ACTIVITY_KINDS,
    ActivityAdmissionTarget,
    ActivityClaim,
    ActivityReceipt,
    DurableScheduler,
    SchedulerError,
)
from .store import (
    ConcurrentProjectionUpdate,
    DurableRunStore,
    HierarchyAdmissionHop,
    HierarchyAdmissionScope,
    RunAlreadyExistsError,
    RunHierarchyLimitError,
    WorkflowBindingConflictError,
)
from .workflow import (
    MAX_WORKFLOW_BYTES,
    CompiledWorkflow,
    NodeDefinition,
    TEMPLATE_RE,
    compile_workflow,
)

HIERARCHY_SCHEMA_VERSION = 1
DEFAULT_MAX_DEPTH = 8
DEFAULT_MAX_CHILDREN_PER_CONTROL = 256
DEFAULT_MAX_TOTAL_DESCENDANTS = 1024
MAX_HIERARCHY_DEPTH = 64
MAX_CHILDREN_PER_CONTROL = 1_000
MAX_TOTAL_DESCENDANTS = 10_000
_HIERARCHY_KINDS = frozenset({"map", "subworkflow"})
_MAP_BODY_KINDS = frozenset({"agent", "tool", "subworkflow"})


class HierarchyError(RuntimeError):
    """Base error for deterministic hierarchy reconciliation."""


class HierarchyDefinitionError(HierarchyError, ValueError):
    """A Workflow has ambiguous hierarchy semantics."""


class HierarchyIntegrityError(HierarchyError):
    """A persisted hierarchy link or result cannot be trusted."""


class HierarchyLimitError(HierarchyIntegrityError):
    """A hierarchy exceeds a configured hard safety limit."""


class WorkflowRegistry:
    """Immutable registry with optional Store-backed definition recovery."""

    def __init__(self, workflows: Iterable[CompiledWorkflow] = ()) -> None:
        self._lock = threading.Lock()
        self._workflows: dict[tuple[str, int], CompiledWorkflow] = {}
        self._store: DurableRunStore | None = None
        self._artifact_store: ArtifactStore | None = None
        for workflow in workflows:
            self.register(workflow)

    def register(self, workflow: CompiledWorkflow) -> None:
        if not isinstance(workflow, CompiledWorkflow):
            raise TypeError("workflow must be a CompiledWorkflow")
        key = (workflow.name, workflow.version)
        with self._lock:
            existing = self._workflows.get(key)
            if (
                existing is not None
                and existing.definition_digest != workflow.definition_digest
            ):
                raise HierarchyDefinitionError(
                    f"Workflow {workflow.name}@{workflow.version} is already fixed "
                    "to another definition digest"
                )
            self._workflows[key] = workflow
            store = self._store
            artifact_store = self._artifact_store
        if store is not None and artifact_store is not None:
            self._persist(store, artifact_store, workflow)

    def attach(
        self,
        store: DurableRunStore,
        artifact_store: ArtifactStore,
    ) -> None:
        """Attach trusted persistence and durably bind every registered definition."""

        if not isinstance(store, DurableRunStore):
            raise TypeError("store must be a DurableRunStore")
        if not isinstance(artifact_store, ArtifactStore):
            raise TypeError("artifact_store must implement ArtifactStore")
        with self._lock:
            if self._store is not None and self._store.path != store.path:
                raise HierarchyDefinitionError(
                    "WorkflowRegistry cannot attach to a different Store"
                )
            if (
                self._artifact_store is not None
                and self._artifact_store is not artifact_store
            ):
                raise HierarchyDefinitionError(
                    "WorkflowRegistry cannot attach to a different ArtifactStore"
                )
            self._store = store
            self._artifact_store = artifact_store
            workflows = tuple(self._workflows.values())
        for workflow in workflows:
            self._persist(store, artifact_store, workflow)

    def resolve(
        self,
        workflow_id: str,
        workflow_version: int,
        *,
        definition_digest: str | None = None,
    ) -> CompiledWorkflow:
        with self._lock:
            workflow = self._workflows.get((workflow_id, workflow_version))
            store = self._store
            artifact_store = self._artifact_store
        if (
            workflow is None
            and store is not None
            and artifact_store is not None
        ):
            workflow = self._load(
                store,
                artifact_store,
                workflow_id,
                workflow_version,
            )
            self.register(workflow)
        if workflow is None:
            raise HierarchyDefinitionError(
                f"unknown Workflow {workflow_id}@{workflow_version}"
            )
        if (
            definition_digest is not None
            and workflow.definition_digest != definition_digest
        ):
            raise HierarchyIntegrityError(
                f"Workflow {workflow_id}@{workflow_version} digest mismatch"
            )
        return workflow

    def ensure_persisted(self, workflow: CompiledWorkflow) -> ArtifactRef:
        """Return the immutable definition Artifact bound to this Workflow."""

        if not isinstance(workflow, CompiledWorkflow):
            raise TypeError("workflow must be a CompiledWorkflow")
        with self._lock:
            store = self._store
            artifact_store = self._artifact_store
        if store is None or artifact_store is None:
            raise HierarchyDefinitionError(
                "WorkflowRegistry is not attached to durable persistence"
            )
        return self._persist(store, artifact_store, workflow)

    @staticmethod
    def _persist(
        store: DurableRunStore,
        artifact_store: ArtifactStore,
        workflow: CompiledWorkflow,
    ) -> ArtifactRef:
        binding = store.get_workflow_binding(workflow.name, workflow.version)
        if binding is not None:
            if binding.definition_digest != workflow.definition_digest:
                raise HierarchyDefinitionError(
                    f"Workflow {workflow.name}@{workflow.version} conflicts "
                    "with the durable binding"
                )
            if binding.workflow_ref is not None:
                WorkflowRegistry._verify_loaded(
                    artifact_store,
                    binding.workflow_ref,
                    workflow.name,
                    workflow.version,
                    workflow.definition_digest,
                )
                return binding.workflow_ref
        workflow_ref = artifact_store.put_json(
            _canonical_workflow_definition(workflow),
            kind=ArtifactKind.GENERIC,
            metadata={},
        )
        if artifact_store.verify(workflow_ref) is not True:
            raise HierarchyIntegrityError(
                "persisted Workflow definition Artifact failed verification"
            )
        try:
            stored = store.bind_workflow(
                workflow.name,
                workflow.version,
                workflow.definition_digest,
                workflow_ref,
            )
        except WorkflowBindingConflictError as exc:
            raise HierarchyDefinitionError(
                f"Workflow {workflow.name}@{workflow.version} changed concurrently"
            ) from exc
        if stored.workflow_ref is None:
            raise HierarchyIntegrityError(
                "durable Workflow binding did not retain its Artifact"
            )
        return stored.workflow_ref

    @staticmethod
    def _load(
        store: DurableRunStore,
        artifact_store: ArtifactStore,
        workflow_id: str,
        workflow_version: int,
    ) -> CompiledWorkflow:
        binding = store.get_workflow_binding(workflow_id, workflow_version)
        if binding is None or binding.workflow_ref is None:
            raise HierarchyDefinitionError(
                f"unknown Workflow {workflow_id}@{workflow_version}"
            )
        return WorkflowRegistry._verify_loaded(
            artifact_store,
            binding.workflow_ref,
            workflow_id,
            workflow_version,
            binding.definition_digest,
        )

    @staticmethod
    def _verify_loaded(
        artifact_store: ArtifactStore,
        workflow_ref: ArtifactRef,
        workflow_id: str,
        workflow_version: int,
        definition_digest: str,
    ) -> CompiledWorkflow:
        try:
            if (
                workflow_ref.size > MAX_WORKFLOW_BYTES
                or artifact_store.verify(workflow_ref) is not True
            ):
                raise ValueError("Workflow Artifact failed verification")
            raw = artifact_store.read(workflow_ref)
            if len(raw) > MAX_WORKFLOW_BYTES:
                raise ValueError("Workflow Artifact exceeds compiler bound")
            definition = json.loads(raw.decode("utf-8"))
            if not isinstance(definition, dict):
                raise ValueError("Workflow Artifact must contain an object")
            workflow = compile_workflow(definition)
        except Exception as exc:
            raise HierarchyIntegrityError(
                "durable Workflow definition cannot be recovered"
            ) from exc
        if (
            workflow.name != workflow_id
            or workflow.version != workflow_version
            or workflow.definition_digest != definition_digest
        ):
            raise HierarchyIntegrityError(
                "durable Workflow definition identity is inconsistent"
            )
        return workflow


class DurableHierarchy:
    """HierarchyController implementation shared by all schedulers in one Store."""

    def __init__(
        self,
        registry: WorkflowRegistry,
        artifact_store: ArtifactStore,
        *,
        max_depth: int = DEFAULT_MAX_DEPTH,
        max_children_per_control: int = DEFAULT_MAX_CHILDREN_PER_CONTROL,
        max_total_descendants: int = DEFAULT_MAX_TOTAL_DESCENDANTS,
    ) -> None:
        if (
            isinstance(max_depth, bool)
            or not isinstance(max_depth, int)
            or max_depth < 1
            or max_depth > MAX_HIERARCHY_DEPTH
        ):
            raise ValueError("max_depth must be a bounded positive integer")
        if (
            isinstance(max_children_per_control, bool)
            or not isinstance(max_children_per_control, int)
            or max_children_per_control < 1
            or max_children_per_control > MAX_CHILDREN_PER_CONTROL
        ):
            raise ValueError(
                "max_children_per_control must be a bounded positive integer"
            )
        if (
            isinstance(max_total_descendants, bool)
            or not isinstance(max_total_descendants, int)
            or max_total_descendants < 1
            or max_total_descendants > MAX_TOTAL_DESCENDANTS
        ):
            raise ValueError(
                "max_total_descendants must be a bounded positive integer"
            )
        if max_total_descendants < max_children_per_control:
            raise ValueError(
                "max_total_descendants must be >= max_children_per_control"
            )
        self.registry = registry
        self.artifact_store = artifact_store
        self.max_depth = int(max_depth)
        self.max_children_per_control = int(max_children_per_control)
        self.max_total_descendants = int(max_total_descendants)
        self._lock = threading.Lock()
        self._template_ids: dict[str, frozenset[str]] = {}
        self._map_owners: dict[str, dict[str, str]] = {}
        self._derived: dict[tuple[str, int, str], CompiledWorkflow] = {}

    def bind_store(self, store: DurableRunStore) -> None:
        """Persist registry definitions before any hierarchy Run can be created."""

        self.registry.attach(store, self.artifact_store)

    def validate_workflow(
        self,
        workflow: CompiledWorkflow,
    ) -> frozenset[str]:
        # Every scheduler definition participating in a hierarchy must be
        # durably resolvable when traversal later crosses back from a child.
        self.registry.register(workflow)
        cached = self._template_ids.get(workflow.definition_digest)
        if cached is not None:
            return cached

        owners: dict[str, str] = {}
        dependents = {
            node_id: tuple(str(value) for value in workflow.dependents[node_id])
            for node_id in workflow.topological_order
        }
        for definition in workflow.nodes:
            if definition.kind != "map":
                continue
            body_id = str(definition.config["body"])
            if body_id in owners:
                raise HierarchyDefinitionError(
                    f"map template {body_id} is referenced by multiple map nodes"
                )
            body = workflow.get_node(body_id)
            if body.kind not in _MAP_BODY_KINDS:
                raise HierarchyDefinitionError(
                    f"map template {body_id} kind {body.kind} is not supported"
                )
            if body.depends_on != (definition.node_id,):
                raise HierarchyDefinitionError(
                    f"map template {body_id} must depend only on {definition.node_id}"
                )
            if dependents[body_id]:
                raise HierarchyDefinitionError(
                    f"map template {body_id} cannot be a normal upstream dependency"
                )
            owners[body_id] = definition.node_id

        templates = frozenset(owners)
        with self._lock:
            existing = self._template_ids.get(workflow.definition_digest)
            if existing is not None and existing != templates:
                raise HierarchyDefinitionError("Workflow hierarchy validation changed")
            self._template_ids[workflow.definition_digest] = templates
            self._map_owners[workflow.definition_digest] = owners
        return templates

    def reconcile_run(
        self,
        scheduler: DurableScheduler,
        run_id: str,
    ) -> bool:
        """Resume a compound map/body projection after any committed step."""

        owners = self._map_owners_for(scheduler.workflow)
        for body_id, map_id in sorted(owners.items()):
            control = scheduler.store.get_node(run_id, map_id)
            body = scheduler.store.get_node(run_id, body_id)
            if control is None or body is None or not control.status.is_terminal:
                continue
            expected = {
                NodeStatus.SUCCEEDED: NodeStatus.SUCCEEDED,
                NodeStatus.FAILED: NodeStatus.SKIPPED,
                NodeStatus.CANCELLED: NodeStatus.CANCELLED,
            }.get(control.status)
            if expected is None or body.status is expected:
                continue
            receipt = (
                self._receipt_from_projection(control.output)
                if control.status is NodeStatus.SUCCEEDED
                else None
            )
            scheduler.resolve_hierarchy_control(
                run_id,
                map_id,
                outcome=control.status.value,
                receipt=receipt,
                map_body_id=body_id,
            )
            return True
        return False

    def reconcile_control(
        self,
        scheduler: DurableScheduler,
        run_id: str,
        definition: NodeDefinition,
        node: NodeRecord,
    ) -> bool:
        try:
            run = self._require_run(scheduler, run_id)
            deadline = run.metadata.get("deadline_at")
            if (
                isinstance(deadline, (int, float))
                and not isinstance(deadline, bool)
                and scheduler.current_time() >= float(deadline)
            ):
                scheduler.request_cancel(run_id, reconcile=False)
                return True
            if definition.kind == "subworkflow":
                return self._reconcile_subworkflow(
                    scheduler,
                    run,
                    definition,
                    node,
                )
            if definition.kind == "map":
                return self._reconcile_map(
                    scheduler,
                    run,
                    definition,
                    node,
                )
            raise HierarchyDefinitionError(
                f"unsupported hierarchy kind: {definition.kind}"
            )
        except ConcurrentProjectionUpdate:
            return True
        except (HierarchyError, SchedulerError, ValueError, TypeError):
            scheduler.wait_hierarchy_recovery(
                run_id,
                definition.node_id,
                error_code="hierarchy_integrity_failure",
            )
            return True

    def reconcile_pausing(
        self,
        scheduler: DurableScheduler,
        run_id: str,
    ) -> bool | None:
        return self._propagate_intent(scheduler, run_id, intent="pause")

    def reconcile_cancelling(
        self,
        scheduler: DurableScheduler,
        run_id: str,
    ) -> bool | None:
        return self._propagate_intent(scheduler, run_id, intent="cancel")

    def claim_next_child(
        self,
        scheduler: DurableScheduler,
        run_id: str,
        worker_id: str,
        *,
        capacity: int,
        resource_keys: Sequence[str] | None,
        lease_seconds: float,
    ) -> ActivityClaim | None:
        children = sorted(
            self._direct_children(scheduler.store, run_id),
            key=self._child_sort_key,
        )
        for child in children:
            if child.status is not RunStatus.RUNNING:
                continue
            child_scheduler = self.scheduler_for_run(scheduler, child.run_id)
            claim = child_scheduler.claim_next(
                child.run_id,
                worker_id,
                capacity=capacity,
                resource_keys=resource_keys,
                lease_seconds=lease_seconds,
            )
            if claim is not None:
                return claim
        return None

    def prepare_next_child_admission(
        self,
        scheduler: DurableScheduler,
        run_id: str,
        worker_id: str,
        *,
        resource_keys: Sequence[str] | None,
    ) -> ActivityAdmissionTarget | None:
        """Select one descendant candidate without reconciling or claiming."""

        children = sorted(
            self._direct_children(scheduler.store, run_id),
            key=self._child_sort_key,
        )
        for child in children:
            if child.status is not RunStatus.RUNNING:
                continue
            child_scheduler = self.scheduler_for_run(
                scheduler,
                child.run_id,
            )
            target = child_scheduler.prepare_next_admission_target(
                child.run_id,
                worker_id,
                resource_keys=resource_keys,
            )
            if target is not None:
                return target
        return None

    def hierarchy_admission_scope(
        self,
        scheduler: DurableScheduler,
        run_id: str,
    ) -> HierarchyAdmissionScope | None:
        """Reconstruct and verify one exact root-to-child authority chain."""

        child = self._require_run(scheduler, run_id)
        if not isinstance(
            child.metadata.get("hierarchy_link"),
            dict,
        ):
            return None
        reverse_hops: list[HierarchyAdmissionHop] = []
        seen = {child.run_id}
        for _depth in range(self.max_depth):
            link = child.metadata.get("hierarchy_link")
            if not isinstance(link, dict):
                break
            parent_run_id = link.get("parent_run_id")
            parent_node_id = link.get("parent_node_id")
            if (
                not isinstance(parent_run_id, str)
                or not isinstance(parent_node_id, str)
                or parent_run_id in seen
            ):
                raise HierarchyIntegrityError(
                    "hierarchy admission chain is damaged"
                )
            parent = self._require_run(scheduler, parent_run_id)
            parent_scheduler = self.scheduler_for_run(
                scheduler,
                parent.run_id,
            )
            control = parent_scheduler.store.get_node(
                parent.run_id,
                parent_node_id,
            )
            if (
                parent.status is not RunStatus.RUNNING
                or control is None
                or control.status is not NodeStatus.RUNNING
            ):
                raise HierarchyIntegrityError(
                    "hierarchy admission parent is inactive"
                )
            try:
                definition = parent_scheduler.workflow.get_node(
                    parent_node_id
                )
            except KeyError as exc:
                raise HierarchyIntegrityError(
                    "hierarchy admission control is unknown"
                ) from exc

            relation = link.get("relation")
            index = link.get("child_index")
            if relation == "subworkflow":
                if definition.kind != "subworkflow" or index is not None:
                    raise HierarchyIntegrityError(
                        "subworkflow hierarchy admission is damaged"
                    )
                child_workflow = self.registry.resolve(
                    str(definition.config["workflow_id"]),
                    int(definition.config["workflow_version"]),
                )
                expected_child_id = self._child_run_id(
                    parent.run_id,
                    parent_node_id,
                    child_workflow.definition_digest,
                )
            elif relation == "map_item":
                if (
                    definition.kind != "map"
                    or isinstance(index, bool)
                    or not isinstance(index, int)
                    or index < 0
                ):
                    raise HierarchyIntegrityError(
                        "map hierarchy admission is damaged"
                    )
                child_workflow = self._map_workflow(
                    parent_scheduler.workflow,
                    definition,
                )
                expected_child_id = self._map_child_run_id(
                    parent.run_id,
                    parent_node_id,
                    index,
                    child_workflow.definition_digest,
                )
            else:
                raise HierarchyIntegrityError(
                    "hierarchy admission relation is damaged"
                )
            if child.run_id != expected_child_id:
                raise HierarchyIntegrityError(
                    "hierarchy admission child identity is damaged"
                )
            context = self._child_context(parent, child_workflow)
            self._validate_child(
                child,
                parent,
                parent_node_id,
                child_workflow,
                relation=relation,
                index=index,
                context=context,
            )
            receipt_digest = hashlib.sha256(
                _canonical_json(child.input)
            ).hexdigest()
            if link.get("input_receipt_digest") != receipt_digest:
                raise HierarchyIntegrityError(
                    "hierarchy admission input binding is damaged"
                )
            self._input_refs(child)
            reverse_hops.append(
                HierarchyAdmissionHop(
                    parent_run_id=parent.run_id,
                    parent_node_id=parent_node_id,
                    child_run_id=child.run_id,
                    parent_run_version=parent.projection_version,
                    parent_node_version=control.projection_version,
                )
            )
            seen.add(parent.run_id)
            child = parent
            if child.metadata.get("hierarchy_link") is None:
                break
        else:
            raise HierarchyLimitError(
                "hierarchy admission depth exceeds hard limit"
            )

        root_link = child.metadata.get("hierarchy_link")
        if root_link is not None:
            raise HierarchyIntegrityError(
                "hierarchy admission does not terminate at a root Run"
            )
        hops = tuple(reversed(reverse_hops))
        if not hops:
            raise HierarchyIntegrityError(
                "hierarchy admission chain is empty"
            )
        return HierarchyAdmissionScope(
            root_run_id=child.run_id,
            hops=hops,
        )

    def scheduler_for_run(
        self,
        scheduler: DurableScheduler,
        run_id: str,
    ) -> DurableScheduler:
        run = scheduler.store.get_run(run_id)
        if run is None:
            raise HierarchyIntegrityError(f"child Run not found: {run_id}")
        if (
            run.workflow_id == scheduler.workflow.name
            and run.workflow_version == scheduler.workflow.version
            and run.definition_digest == scheduler.workflow.definition_digest
        ):
            return scheduler
        key = (run.workflow_id, run.workflow_version, run.definition_digest)
        workflow = self._derived.get(key)
        if workflow is None:
            workflow = self.registry.resolve(
                run.workflow_id,
                run.workflow_version,
                definition_digest=run.definition_digest,
            )
        return scheduler.fork_for_workflow(workflow)

    def _reconcile_subworkflow(
        self,
        scheduler: DurableScheduler,
        parent: RunRecord,
        definition: NodeDefinition,
        node: NodeRecord,
    ) -> bool:
        workflow_id = str(definition.config["workflow_id"])
        workflow_version = int(definition.config["workflow_version"])
        child_workflow = self.registry.resolve(workflow_id, workflow_version)
        context = self._child_context(parent, child_workflow)
        child_id = self._child_run_id(
            parent.run_id,
            definition.node_id,
            child_workflow.definition_digest,
        )
        state = {
            "schema_version": HIERARCHY_SCHEMA_VERSION,
            "kind": "subworkflow",
            "workflow_id": child_workflow.name,
            "workflow_version": child_workflow.version,
            "definition_digest": child_workflow.definition_digest,
            "child_run_id": child_id,
            "created_count": 0,
            "depth": context["depth"],
        }
        current, node = self._ensure_state(scheduler, parent, node, state)
        parent = self._require_run(scheduler, parent.run_id)
        if current["created_count"] not in {0, 1}:
            raise HierarchyIntegrityError("invalid subworkflow created_count")
        child = scheduler.store.get_run(child_id)
        raw_input = _thaw_json(definition.config["input"])
        if current["created_count"] == 0:
            child = self._ensure_child(
                scheduler,
                parent,
                definition.node_id,
                node.projection_version,
                child_workflow,
                child_id,
                raw_input,
                relation="subworkflow",
                index=None,
                context=context,
            )
            self._record_child_link(
                scheduler,
                parent,
                definition.node_id,
                child,
                index=None,
            )
            current["created_count"] = 1
            scheduler.record_hierarchy_state(
                parent.run_id,
                definition.node_id,
                current,
                payload={
                    "control_plane": "subworkflow_child_created",
                    "child_count": 1,
                    "definition_digest": child_workflow.definition_digest,
                },
            )
            return True
        if child is None:
            raise HierarchyIntegrityError("persisted subworkflow child is missing")
        linked_ids = {
            linked.run_id
            for linked in self._direct_children(
                scheduler.store,
                parent.run_id,
                definition.node_id,
            )
        }
        if linked_ids != {child_id}:
            raise HierarchyIntegrityError("subworkflow has an unknown linked child Run")
        expected_receipt, expected_receipt_digest = self._prepare_child_input(
            scheduler,
            child_id,
            raw_input,
        )
        self._validate_child(
            child,
            parent,
            definition.node_id,
            child_workflow,
            relation="subworkflow",
            index=None,
            context=context,
            input_receipt=expected_receipt,
            input_receipt_digest=expected_receipt_digest,
        )
        child_scheduler = scheduler.fork_for_workflow(child_workflow)
        changed = self._advance_child(child_scheduler, child)
        child = self._require_child(scheduler, child_id)
        if not child.status.is_terminal:
            return changed
        if child.status is RunStatus.COMPLETED:
            receipt = self._aggregate_receipt(
                scheduler,
                parent,
                definition.node_id,
                [(None, child, child_workflow)],
                relation="subworkflow",
            )
            scheduler.resolve_hierarchy_control(
                parent.run_id,
                definition.node_id,
                outcome="succeeded",
                receipt=receipt,
            )
        else:
            scheduler.resolve_hierarchy_control(
                parent.run_id,
                definition.node_id,
                outcome="failed",
                error_code="subworkflow_child_failed",
            )
        return True

    def _reconcile_map(
        self,
        scheduler: DurableScheduler,
        parent: RunRecord,
        definition: NodeDefinition,
        node: NodeRecord,
    ) -> bool:
        body_id = str(definition.config["body"])
        if body_id not in self.validate_workflow(scheduler.workflow):
            raise HierarchyDefinitionError(f"map body {body_id} is not an isolated template")
        items = self._map_items(parent, definition)
        if len(items) > self.max_children_per_control:
            raise HierarchyLimitError("map item count exceeds hard limit")
        child_workflow = self._map_workflow(scheduler.workflow, definition)
        context = self._child_context(parent, child_workflow)
        items_source_digest = self._items_source_digest(
            parent,
            definition,
            scheduler.workflow,
        )
        max_concurrency = int(definition.config["max_concurrency"])
        base_state = {
            "schema_version": HIERARCHY_SCHEMA_VERSION,
            "kind": "map",
            "workflow_id": child_workflow.name,
            "workflow_version": child_workflow.version,
            "definition_digest": child_workflow.definition_digest,
            "body_node_id": body_id,
            "item_count": len(items),
            "items_source_digest": items_source_digest,
            "max_concurrency": max_concurrency,
            "created_count": 0,
            "depth": context["depth"],
        }
        state, node = self._ensure_state(scheduler, parent, node, base_state)
        parent = self._require_run(scheduler, parent.run_id)
        created_count = int(state["created_count"])
        if created_count < 0 or created_count > len(items):
            raise HierarchyIntegrityError("invalid persisted map created_count")

        children_by_index: dict[int, RunRecord] = {}
        for child in self._direct_children(
            scheduler.store,
            parent.run_id,
            definition.node_id,
        ):
            link = child.metadata.get("hierarchy_link")
            if not isinstance(link, dict):
                raise HierarchyIntegrityError("map child link is damaged")
            index = link.get("child_index")
            if (
                isinstance(index, bool)
                or not isinstance(index, int)
                or index < 0
                or index >= len(items)
                or index in children_by_index
            ):
                raise HierarchyIntegrityError("map child index is damaged")
            child_id = self._map_child_run_id(
                parent.run_id,
                definition.node_id,
                index,
                child_workflow.definition_digest,
            )
            if child.run_id != child_id:
                raise HierarchyIntegrityError("map has an unknown linked child Run")
            raw_input = {
                str(definition.config["item_name"]): items[index],
                "index": index,
            }
            expected_receipt, expected_receipt_digest = (
                self._prepare_child_input(scheduler, child_id, raw_input)
            )
            self._validate_child(
                child,
                parent,
                definition.node_id,
                child_workflow,
                relation="map_item",
                index=index,
                context=context,
                input_receipt=expected_receipt,
                input_receipt_digest=expected_receipt_digest,
            )
            children_by_index[index] = child

        linked_indices = sorted(children_by_index)
        if linked_indices != list(range(len(linked_indices))):
            raise HierarchyIntegrityError("map child indices are not a contiguous prefix")
        linked_count = len(linked_indices)
        if linked_count < created_count:
            raise HierarchyIntegrityError("persisted map child is missing")
        if linked_count > created_count:
            state["created_count"] = linked_count
            scheduler.record_hierarchy_state(
                parent.run_id,
                definition.node_id,
                state,
                payload={
                    "control_plane": "map_child_progress_recovered",
                    "created_count": linked_count,
                    "item_count": len(items),
                    "items_source_digest": items_source_digest,
                },
            )
            return True
        children = sorted(children_by_index.items())

        changed = False
        refreshed: list[tuple[int, RunRecord]] = []
        for index, child in children:
            child_scheduler = scheduler.fork_for_workflow(child_workflow)
            changed = self._advance_child(child_scheduler, child) or changed
            refreshed.append((index, self._require_child(scheduler, child.run_id)))
        children = refreshed

        failed = [
            child
            for _index, child in children
            if child.status in {RunStatus.FAILED, RunStatus.CANCELLED}
        ]
        stop_after_failure = bool(failed) and definition.on_error != "continue"
        if stop_after_failure:
            for _index, child in children:
                if child.status.is_terminal:
                    continue
                child_scheduler = scheduler.fork_for_workflow(child_workflow)
                before = child.projection_version
                child_scheduler.request_cancel(child.run_id)
                changed = (
                    self._require_child(scheduler, child.run_id).projection_version != before
                    or changed
                )
        else:
            active_count = sum(
                not child.status.is_terminal for _index, child in children
            )
            while (
                created_count < len(items)
                and active_count < max_concurrency
            ):
                child_id = self._map_child_run_id(
                    parent.run_id,
                    definition.node_id,
                    created_count,
                    child_workflow.definition_digest,
                )
                raw_input = {
                    str(definition.config["item_name"]): items[created_count],
                    "index": created_count,
                }
                child = self._ensure_child(
                    scheduler,
                    parent,
                    definition.node_id,
                    node.projection_version,
                    child_workflow,
                    child_id,
                    raw_input,
                    relation="map_item",
                    index=created_count,
                    context=context,
                )
                self._record_child_link(
                    scheduler,
                    parent,
                    definition.node_id,
                    child,
                    index=created_count,
                )
                children.append((created_count, child))
                created_count += 1
                state["created_count"] = created_count
                node = scheduler.record_hierarchy_state(
                    parent.run_id,
                    definition.node_id,
                    state,
                    payload={
                        "control_plane": "map_child_created",
                        "created_count": created_count,
                        "item_count": len(items),
                        "items_source_digest": items_source_digest,
                    },
                )
                parent = self._require_run(scheduler, parent.run_id)
                child_scheduler = scheduler.fork_for_workflow(child_workflow)
                self._advance_child(child_scheduler, child)
                child = self._require_child(scheduler, child_id)
                children[-1] = (created_count - 1, child)
                active_count += not child.status.is_terminal
                changed = True

        children = [
            (index, self._require_child(scheduler, child.run_id))
            for index, child in children
        ]
        if any(not child.status.is_terminal for _index, child in children):
            return changed
        if not stop_after_failure and created_count < len(items):
            return changed

        failed = [
            child
            for _index, child in children
            if child.status is not RunStatus.COMPLETED
        ]
        if failed:
            scheduler.resolve_hierarchy_control(
                parent.run_id,
                definition.node_id,
                outcome="failed",
                error_code="map_child_failed",
                map_body_id=body_id,
            )
        else:
            receipt = self._aggregate_receipt(
                scheduler,
                parent,
                definition.node_id,
                [
                    (index, child, child_workflow)
                    for index, child in sorted(children)
                ],
                relation="map",
            )
            scheduler.resolve_hierarchy_control(
                parent.run_id,
                definition.node_id,
                outcome="succeeded",
                receipt=receipt,
                map_body_id=body_id,
            )
        return True

    def _propagate_intent(
        self,
        scheduler: DurableScheduler,
        run_id: str,
        *,
        intent: str,
    ) -> bool | None:
        try:
            controls = [
                definition
                for definition in scheduler.workflow.nodes
                if definition.kind in _HIERARCHY_KINDS
            ]
            applicable = False
            changed = False
            for definition in controls:
                node = scheduler.store.get_node(run_id, definition.node_id)
                if node is None or node.status.is_terminal:
                    continue
                if node.status is not NodeStatus.RUNNING:
                    continue
                children = self._direct_children(
                    scheduler.store,
                    run_id,
                    definition.node_id,
                )
                if not children:
                    continue
                applicable = True
                for child in children:
                    if child.status.is_terminal:
                        continue
                    child_scheduler = self.scheduler_for_run(scheduler, child.run_id)
                    before = child.projection_version
                    if intent == "cancel":
                        child_scheduler.request_cancel(child.run_id)
                    elif child.status not in {RunStatus.PAUSING, RunStatus.PAUSED}:
                        child_scheduler.request_pause(child.run_id)
                    current = self._require_child(scheduler, child.run_id)
                    changed = current.projection_version != before or changed
                children = self._direct_children(
                    scheduler.store,
                    run_id,
                    definition.node_id,
                )
                if intent == "pause":
                    if any(
                        not child.status.is_terminal
                        and child.status is not RunStatus.PAUSED
                        for child in children
                    ):
                        return changed
                    continue
                if any(not child.status.is_terminal for child in children):
                    return changed
                body_id = (
                    str(definition.config["body"])
                    if definition.kind == "map"
                    else None
                )
                scheduler.resolve_hierarchy_control(
                    run_id,
                    definition.node_id,
                    outcome="cancelled",
                    map_body_id=body_id,
                )
                return True
            return changed if applicable and changed else None
        except ConcurrentProjectionUpdate:
            return True
        except (HierarchyError, SchedulerError, ValueError, TypeError):
            run = self._require_run(scheduler, run_id)
            if run.status is RunStatus.PAUSING:
                scheduler.request_cancel(run_id, reconcile=False)
            for definition in scheduler.workflow.nodes:
                node = scheduler.store.get_node(run_id, definition.node_id)
                if (
                    definition.kind in _HIERARCHY_KINDS
                    and node is not None
                    and node.status is NodeStatus.RUNNING
                ):
                    scheduler.wait_hierarchy_recovery(
                        run_id,
                        definition.node_id,
                        error_code="hierarchy_integrity_failure",
                    )
                    return True
            raise

    def _advance_child(
        self,
        scheduler: DurableScheduler,
        child: RunRecord,
    ) -> bool:
        before = child.projection_version
        if child.status is RunStatus.PAUSED:
            scheduler.resume(child.run_id)
        elif not child.status.is_terminal:
            scheduler.reconcile(child.run_id)
        return self._require_child(scheduler, child.run_id).projection_version != before

    def _ensure_state(
        self,
        scheduler: DurableScheduler,
        parent: RunRecord,
        node: NodeRecord,
        expected: dict[str, Any],
    ) -> tuple[dict[str, Any], NodeRecord]:
        raw = node.metadata.get("hierarchy")
        if raw is None:
            stored = scheduler.record_hierarchy_state(
                parent.run_id,
                node.node_id,
                expected,
                payload={
                    "control_plane": "hierarchy_initialized",
                    "kind": expected["kind"],
                    "definition_digest": expected["definition_digest"],
                },
            )
            stored_state = stored.metadata.get("hierarchy")
            if not isinstance(stored_state, dict):
                raise HierarchyIntegrityError(
                    "persisted hierarchy state is not an object"
                )
            return dict(stored_state), stored
        if not isinstance(raw, dict):
            raise HierarchyIntegrityError("hierarchy state is not an object")
        current = dict(raw)
        for key, value in expected.items():
            if key == "created_count":
                continue
            if current.get(key) != value:
                raise HierarchyIntegrityError(
                    f"hierarchy state field {key} does not match definition"
                )
        if (
            isinstance(current.get("created_count"), bool)
            or not isinstance(current.get("created_count"), int)
        ):
            raise HierarchyIntegrityError("hierarchy created_count is invalid")
        return current, node

    def _ensure_child(
        self,
        scheduler: DurableScheduler,
        parent: RunRecord,
        parent_node_id: str,
        expected_parent_node_version: int,
        workflow: CompiledWorkflow,
        child_id: str,
        raw_input: Any,
        *,
        relation: str,
        index: int | None,
        context: dict[str, Any],
    ) -> RunRecord:
        input_receipt, input_receipt_digest = self._prepare_child_input(
            scheduler,
            child_id,
            raw_input,
        )
        link = {
            "schema_version": HIERARCHY_SCHEMA_VERSION,
            "relation": relation,
            "root_run_id": context["root_run_id"],
            "parent_run_id": parent.run_id,
            "parent_node_id": parent_node_id,
            "child_index": index,
            "depth": context["depth"],
            "ancestry_digests": context["ancestry_digests"],
            "definition_digest": workflow.definition_digest,
            "input_receipt_digest": input_receipt_digest,
        }
        workflow_ref = self.registry.ensure_persisted(workflow)
        metadata: dict[str, Any] = {
            "hierarchy_link": link,
            "runtime_workflow_ref": workflow_ref.to_dict(),
        }
        deadline = parent.metadata.get("deadline_at")
        if isinstance(deadline, (int, float)) and not isinstance(deadline, bool):
            metadata["deadline_at"] = float(deadline)
        child_scheduler = scheduler.fork_for_workflow(workflow)
        child = scheduler.store.get_run(child_id)
        if child is None:
            try:
                child = child_scheduler.create_child_run(
                    child_id,
                    input=input_receipt,
                    metadata=metadata,
                    max_total_descendants=self.max_total_descendants,
                    max_children_per_control=self.max_children_per_control,
                    expected_parent_run_version=parent.projection_version,
                    expected_parent_node_version=expected_parent_node_version,
                )
            except RunAlreadyExistsError:
                child = scheduler.store.get_run(child_id)
            except RunHierarchyLimitError as exc:
                if exc.reason_code == "total_descendants":
                    message = "hierarchy child count exceeds hard limit"
                else:
                    message = (
                        "hierarchy children per control exceed hard limit"
                    )
                raise HierarchyLimitError(message) from exc
        if child is None:
            raise HierarchyIntegrityError("child Run creation did not converge")
        self._validate_child(
            child,
            parent,
            parent_node_id,
            workflow,
            relation=relation,
            index=index,
            context=context,
            input_receipt=input_receipt,
            input_receipt_digest=input_receipt_digest,
        )
        self._input_refs(child)
        return child

    def _validate_child(
        self,
        child: RunRecord,
        parent: RunRecord,
        parent_node_id: str,
        workflow: CompiledWorkflow,
        *,
        relation: str,
        index: int | None,
        context: dict[str, Any],
        input_receipt: Any = None,
        input_receipt_digest: str | None = None,
    ) -> None:
        if (
            child.workflow_id != workflow.name
            or child.workflow_version != workflow.version
            or child.definition_digest != workflow.definition_digest
        ):
            raise HierarchyIntegrityError("child Run definition identity is damaged")
        expected_workflow_ref = self.registry.ensure_persisted(workflow).to_dict()
        if child.metadata.get("runtime_workflow_ref") != expected_workflow_ref:
            raise HierarchyIntegrityError(
                "child Run Workflow Artifact binding is damaged"
            )
        link = child.metadata.get("hierarchy_link")
        if not isinstance(link, dict):
            raise HierarchyIntegrityError("child Run is missing its hierarchy link")
        expected = {
            "schema_version": HIERARCHY_SCHEMA_VERSION,
            "relation": relation,
            "root_run_id": context["root_run_id"],
            "parent_run_id": parent.run_id,
            "parent_node_id": parent_node_id,
            "child_index": index,
            "depth": context["depth"],
            "ancestry_digests": context["ancestry_digests"],
            "definition_digest": workflow.definition_digest,
        }
        for key, value in expected.items():
            if link.get(key) != value:
                raise HierarchyIntegrityError(f"child link field {key} is damaged")
        if input_receipt_digest is not None:
            if link.get("input_receipt_digest") != input_receipt_digest:
                raise HierarchyIntegrityError("child input receipt digest is damaged")
            normalized_receipt = input_receipt.to_dict()
            if child.input != normalized_receipt:
                raise HierarchyIntegrityError("child input receipt binding is damaged")

    def _record_child_link(
        self,
        scheduler: DurableScheduler,
        parent: RunRecord,
        parent_node_id: str,
        child: RunRecord,
        *,
        index: int | None,
    ) -> None:
        refs = self._input_refs(child)
        receipt_digest = hashlib.sha256(_canonical_json(child.input)).hexdigest()
        event_id = "evt_hierarchy_link_" + hashlib.sha256(
            (
                f"{parent.run_id}\0{parent_node_id}\0{child.run_id}\0"
                f"{index}\0{receipt_digest}"
            ).encode("utf-8")
        ).hexdigest()
        scheduler.record_hierarchy_child_link(
            parent.run_id,
            parent_node_id,
            event_id=event_id,
            link={
                "child_run_id": child.run_id,
                "index": index,
                "definition_digest": child.definition_digest,
                "input_receipt_digest": receipt_digest,
                "input_artifact_refs": [ref.to_dict() for ref in refs],
            },
        )

    def _aggregate_receipt(
        self,
        scheduler: DurableScheduler,
        parent: RunRecord,
        parent_node_id: str,
        children: list[tuple[int | None, RunRecord, CompiledWorkflow]],
        *,
        relation: str,
    ) -> ActivityReceipt:
        results: list[dict[str, Any]] = []
        for index, child, workflow in sorted(
            children,
            key=lambda item: (-1 if item[0] is None else item[0], item[1].run_id),
        ):
            refs = self._child_result_refs(scheduler, child, workflow)
            results.append(
                {
                    "index": index,
                    "child_run_id": child.run_id,
                    "status": child.status.value,
                    "artifact_refs": [ref.to_dict() for ref in refs],
                }
            )
        aggregate = {
            "schema_version": HIERARCHY_SCHEMA_VERSION,
            "kind": "hierarchy_result",
            "relation": relation,
            "parent_run_id": parent.run_id,
            "parent_node_id": parent_node_id,
            "children": results,
        }
        ref = self.artifact_store.put_json(
            aggregate,
            kind=ArtifactKind.GENERIC,
            producer_run_id=parent.run_id,
            producer_node_id=parent_node_id,
            metadata={},
        )
        self._verify_artifact(ref, "aggregate Artifact")
        receipt = ActivityReceipt((ref,))
        scheduler.verify_activity_receipt(receipt)
        return receipt

    def _child_result_refs(
        self,
        scheduler: DurableScheduler,
        child: RunRecord,
        workflow: CompiledWorkflow,
    ) -> tuple[ArtifactRef, ...]:
        if child.status is not RunStatus.COMPLETED:
            raise HierarchyIntegrityError("non-completed child has no success result")
        refs: list[ArtifactRef] = []
        for node_id in workflow.leaves:
            definition = workflow.get_node(node_id)
            node = scheduler.store.get_node(child.run_id, node_id)
            if node is None:
                raise HierarchyIntegrityError("child result Node is missing")
            if node.status is NodeStatus.SKIPPED:
                continue
            if node.status is not NodeStatus.SUCCEEDED:
                raise HierarchyIntegrityError("child result Node is not successful")
            try:
                receipt = self._receipt_from_projection(node.output)
            except HierarchyIntegrityError:
                if definition.kind in {"parallel", "join", "router", "approval"}:
                    continue
                raise
            for ref in receipt.artifact_refs:
                self._verify_artifact(ref, "child result Artifact")
                refs.append(ref)
        return tuple(refs)

    def _receipt_from_projection(self, value: Any) -> ActivityReceipt:
        if not isinstance(value, dict) or value.get("outcome") != "succeeded":
            raise HierarchyIntegrityError("child result is not an Artifact receipt")
        raw_refs = value.get("artifact_refs")
        if not isinstance(raw_refs, list) or not raw_refs:
            raise HierarchyIntegrityError("child result has no Artifact refs")
        try:
            refs = tuple(ArtifactRef.from_dict(raw) for raw in raw_refs)
        except (TypeError, ValueError) as exc:
            raise HierarchyIntegrityError("child result Artifact ref is invalid") from exc
        for ref in refs:
            self._verify_artifact(ref, "child result Artifact")
        return ActivityReceipt(refs)

    def _input_refs(self, run: RunRecord) -> tuple[ArtifactRef, ...]:
        value = run.input
        if not isinstance(value, dict) or value.get("kind") != "artifact_input":
            raise HierarchyIntegrityError("child input is not an Artifact receipt")
        raw_refs = value.get("artifact_refs")
        if not isinstance(raw_refs, list) or not raw_refs:
            raise HierarchyIntegrityError("child input receipt has no Artifact refs")
        try:
            refs = tuple(ArtifactRef.from_dict(raw) for raw in raw_refs)
        except (TypeError, ValueError) as exc:
            raise HierarchyIntegrityError("child input Artifact ref is invalid") from exc
        for ref in refs:
            self._verify_artifact(ref, "child input Artifact")
        return refs

    def _map_items(
        self,
        parent: RunRecord,
        definition: NodeDefinition,
    ) -> list[Any]:
        configured = definition.config["items"]
        if isinstance(configured, tuple):
            detached = _thaw_json(configured)
            if not isinstance(detached, list):
                raise HierarchyDefinitionError("static map items must be a list")
            return detached
        if not isinstance(configured, str):
            raise HierarchyDefinitionError("map items must be static JSON or a template")
        match = TEMPLATE_RE.fullmatch(configured)
        if match is None:
            raise HierarchyDefinitionError("map items template must be one exact reference")
        path = match.group(1).split(".")
        if len(path) < 2 or path[0] != "input":
            raise HierarchyDefinitionError("map items template must reference input")
        refs = self._input_refs(parent)
        if len(refs) != 1:
            raise HierarchyIntegrityError(
                "map Artifact input must contain exactly one JSON Artifact"
            )
        try:
            raw = self._read_artifact(refs[0], "map input Artifact")
            value: Any = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError, OSError) as exc:
            raise HierarchyIntegrityError("map input Artifact is not valid JSON") from exc
        for key in path[1:]:
            if not isinstance(value, dict) or key not in value:
                raise HierarchyIntegrityError("map input template path is missing")
            value = value[key]
        if not isinstance(value, list):
            raise HierarchyIntegrityError("map input template must resolve to a list")
        return json.loads(_canonical_json(value).decode("utf-8"))

    def _verify_artifact(self, ref: ArtifactRef, label: str) -> None:
        try:
            valid = self.artifact_store.verify(ref)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise HierarchyIntegrityError(f"{label} is damaged") from exc
        if valid is not True:
            raise HierarchyIntegrityError(f"{label} is damaged")

    def _read_artifact(self, ref: ArtifactRef, label: str) -> bytes:
        try:
            return self.artifact_store.read(ref)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise HierarchyIntegrityError(f"{label} is damaged") from exc

    def _items_source_digest(
        self,
        parent: RunRecord,
        definition: NodeDefinition,
        workflow: CompiledWorkflow,
    ) -> str:
        configured = definition.config["items"]
        if isinstance(configured, tuple):
            return f"definition:{workflow.definition_digest}"
        self._input_refs(parent)
        return "artifact_receipt:" + hashlib.sha256(
            _canonical_json(parent.input)
        ).hexdigest()

    @staticmethod
    def _prepare_child_input(
        scheduler: DurableScheduler,
        child_id: str,
        raw_input: Any,
    ) -> tuple[Any, str]:
        receipt = scheduler.prepare_run_input_receipt(child_id, raw_input)
        digest = hashlib.sha256(_canonical_json(receipt.to_dict())).hexdigest()
        return receipt, digest

    def _map_workflow(
        self,
        parent_workflow: CompiledWorkflow,
        definition: NodeDefinition,
    ) -> CompiledWorkflow:
        body_id = str(definition.config["body"])
        body = parent_workflow.get_node(body_id)
        raw_body = body.to_dict()
        raw_body["depends_on"] = []
        name = f"{parent_workflow.name}#map:{definition.node_id}"
        compiled = compile_workflow(
            {
                "schema_version": parent_workflow.schema_version,
                "name": name,
                "version": parent_workflow.version,
                "nodes": [raw_body],
                "metadata": {
                    "derived_from": parent_workflow.definition_digest,
                    "map_node_id": definition.node_id,
                    "body_node_id": body_id,
                },
            }
        )
        key = (compiled.name, compiled.version, compiled.definition_digest)
        with self._lock:
            existing = self._derived.get(key)
            if (
                existing is not None
                and existing.definition_digest != compiled.definition_digest
            ):
                raise HierarchyDefinitionError("derived map Workflow changed")
            self._derived[key] = compiled
        return compiled

    def _child_context(
        self,
        parent: RunRecord,
        child_workflow: CompiledWorkflow,
    ) -> dict[str, Any]:
        parent_link = parent.metadata.get("hierarchy_link")
        if parent_link is None:
            root_run_id = parent.run_id
            depth = 1
            ancestry = [parent.definition_digest]
        else:
            if not isinstance(parent_link, dict):
                raise HierarchyIntegrityError("parent hierarchy link is damaged")
            root_run_id = parent_link.get("root_run_id")
            parent_depth = parent_link.get("depth")
            ancestry = parent_link.get("ancestry_digests")
            if (
                not isinstance(root_run_id, str)
                or not isinstance(parent_depth, int)
                or not isinstance(ancestry, list)
                or not all(isinstance(value, str) for value in ancestry)
                or not ancestry
                or ancestry[-1] != parent.definition_digest
            ):
                raise HierarchyIntegrityError("parent hierarchy ancestry is damaged")
            depth = parent_depth + 1
        if depth > self.max_depth:
            raise HierarchyLimitError("hierarchy recursion depth exceeds hard limit")
        if child_workflow.definition_digest in ancestry:
            raise HierarchyLimitError("recursive Workflow cycle detected")
        return {
            "root_run_id": root_run_id,
            "depth": depth,
            "ancestry_digests": [*ancestry, child_workflow.definition_digest],
        }

    def _direct_children(
        self,
        store: Any,
        parent_run_id: str,
        parent_node_id: str | None = None,
    ) -> list[RunRecord]:
        maximum = (
            self.max_total_descendants
            if parent_node_id is None
            else self.max_children_per_control
        )
        children = store.list_child_runs(
            parent_run_id,
            parent_node_id=parent_node_id,
            limit=maximum + 1,
        )
        if len(children) > maximum:
            raise HierarchyLimitError(
                "hierarchy direct child count exceeds hard limit"
            )
        return children

    @staticmethod
    def _child_sort_key(run: RunRecord) -> tuple[str, int, str]:
        link = run.metadata.get("hierarchy_link")
        if not isinstance(link, dict):
            return ("", -1, run.run_id)
        index = link.get("child_index")
        return (
            str(link.get("parent_node_id") or ""),
            -1 if index is None else int(index),
            run.run_id,
        )

    @staticmethod
    def _child_run_id(
        parent_run_id: str,
        parent_node_id: str,
        definition_digest: str,
    ) -> str:
        digest = hashlib.sha256(
            (
                f"subworkflow\0{parent_run_id}\0{parent_node_id}\0"
                f"{definition_digest}"
            ).encode("utf-8")
        ).hexdigest()
        return f"child_{digest}"

    @staticmethod
    def _map_child_run_id(
        parent_run_id: str,
        parent_node_id: str,
        index: int,
        definition_digest: str,
    ) -> str:
        digest = hashlib.sha256(
            (
                f"map_item\0{parent_run_id}\0{parent_node_id}\0{index}\0"
                f"{definition_digest}"
            ).encode("utf-8")
        ).hexdigest()
        return f"child_{digest}"

    @staticmethod
    def _require_run(
        scheduler: DurableScheduler,
        run_id: str,
    ) -> RunRecord:
        run = scheduler.store.get_run(run_id)
        if run is None:
            raise HierarchyIntegrityError(f"Run not found: {run_id}")
        return run

    @staticmethod
    def _require_child(
        scheduler: DurableScheduler,
        run_id: str,
    ) -> RunRecord:
        child = scheduler.store.get_run(run_id)
        if child is None:
            raise HierarchyIntegrityError(f"child Run not found: {run_id}")
        return child

    def _map_owners_for(
        self,
        workflow: CompiledWorkflow,
    ) -> dict[str, str]:
        self.validate_workflow(workflow)
        return self._map_owners[workflow.definition_digest]


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _canonical_workflow_definition(
    workflow: CompiledWorkflow,
) -> dict[str, Any]:
    """Reconstruct the exact compiler input represented by a compiled Workflow."""

    return {
        "schema_version": workflow.schema_version,
        "name": workflow.name,
        "version": workflow.version,
        "nodes": [
            workflow.get_node(node_id).to_dict()
            for node_id in sorted(workflow.topological_order)
        ],
        "metadata": workflow.metadata.to_dict(),
    }


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


__all__ = [
    "DEFAULT_MAX_CHILDREN_PER_CONTROL",
    "DEFAULT_MAX_DEPTH",
    "DEFAULT_MAX_TOTAL_DESCENDANTS",
    "DurableHierarchy",
    "HierarchyDefinitionError",
    "HierarchyError",
    "HierarchyIntegrityError",
    "HierarchyLimitError",
    "WorkflowRegistry",
]
