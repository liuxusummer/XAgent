"""Composition root for durable orchestration transports.

This layer joins injected persistence, Artifact, compiler, scheduler, executor,
replay, and evaluation capabilities.  It is intentionally transport-neutral:
HTTP and MCP adapters should only translate bytes to the strict objects in
``protocol.py``.

Correctness boundaries:

* all requests are authorized with a trusted out-of-band context;
* the default authorizer denies every operation;
* submit accepts only verified immutable Artifact references;
* no Activity is executed unless an explicit trusted execution driver is
  injected;
* hierarchy submission is rejected unless an explicit durable hierarchy hook
  owns that operation;
* public views omit Domain payloads and execution-owner internals.
"""

from __future__ import annotations

import hashlib
import json
import threading
from collections import Counter, OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from .artifacts import ArtifactRef, ArtifactStore
from .evaluation import (
    ReliabilityEvidence,
    ReliabilityReport,
    evaluate_reliability,
)
from .models import EventRecord, RunRecord
from .protocol import (
    EventsBody,
    MAX_CONTROL_STEPS,
    MAX_EVENT_PAGE_SIZE,
    Operation,
    ParentSubmission,
    ProtocolRequest,
    ProtocolResponse,
    ProtocolValidationError,
    RecoverBody,
    RunBody,
    SubmitBody,
    TickBody,
    parse_request,
    validate_identifier,
)
from .replay import ReplayReport, build_replay_report
from .scheduler import DurableScheduler, RunInputReceipt
from .store import (
    DurableRunStore,
    RunAlreadyExistsError,
    RunNotFoundError,
    WorkflowBindingConflictError,
)
from .telemetry import project_domain_event
from .workflow import (
    MAX_WORKFLOW_BYTES,
    CompiledWorkflow,
    WorkflowCompileError,
    compile_workflow,
)


class RuntimeErrorBase(RuntimeError):
    """Base error hidden behind a bounded public protocol code."""


class RuntimeAuthorizationError(RuntimeErrorBase):
    pass


class RuntimeInputError(RuntimeErrorBase):
    pass


class RuntimeCapabilityUnavailable(RuntimeErrorBase):
    pass


@dataclass(frozen=True, slots=True)
class AuthorizationRequest:
    operation: str
    mutation: bool
    request_id: str
    run_id: str | None
    parent_run_id: str | None = None
    parent_node_id: str | None = None


@runtime_checkable
class TrustedAuthorizer(Protocol):
    """Trusted adapter; authorization context never enters Domain Events."""

    def authorize(
        self,
        request: AuthorizationRequest,
        context: Any,
    ) -> bool: ...


@runtime_checkable
class WorkflowCompiler(Protocol):
    def compile(self, definition: Mapping[str, Any]) -> CompiledWorkflow: ...


SchedulerFactory = Callable[[DurableRunStore, CompiledWorkflow], DurableScheduler]
SchedulerResolver = Callable[[RunRecord], DurableScheduler | None]
ExecutorFactory = Callable[[DurableScheduler], Any]
ExecutionDriver = Callable[[DurableScheduler, Any, str, int], int]
RecoveryDriver = Callable[[DurableScheduler, str, int], int]
HierarchySubmitter = Callable[
    [
        ParentSubmission,
        DurableScheduler,
        str,
        RunInputReceipt | None,
        Mapping[str, Any],
    ],
    RunRecord,
]

_DEFAULT_SCHEDULER_CACHE_SIZE = 64
_MAX_SCHEDULER_CACHE_SIZE = 1024


class OrchestrationRuntime:
    """Fail-closed facade over injected durable orchestration capabilities."""

    def __init__(
        self,
        *,
        store: DurableRunStore,
        artifact_store: ArtifactStore,
        compiler: WorkflowCompiler | Callable[
            [Mapping[str, Any]], CompiledWorkflow
        ] = compile_workflow,
        scheduler_factory: SchedulerFactory,
        executor: Any = None,
        executor_factory: ExecutorFactory | None = None,
        replay_builder: Callable[
            [DurableRunStore, str], ReplayReport
        ] = build_replay_report,
        evaluator: Callable[
            [ReliabilityEvidence], ReliabilityReport
        ] = evaluate_reliability,
        authorizer: TrustedAuthorizer | Callable[
            [AuthorizationRequest, Any], bool
        ] | None = None,
        scheduler_resolver: SchedulerResolver | None = None,
        execution_driver: ExecutionDriver | None = None,
        recovery_driver: RecoveryDriver | None = None,
        hierarchy_submitter: HierarchySubmitter | None = None,
        max_cached_schedulers: int = _DEFAULT_SCHEDULER_CACHE_SIZE,
    ) -> None:
        if not callable(scheduler_factory):
            raise TypeError("scheduler_factory must be callable")
        if executor_factory is not None and not callable(executor_factory):
            raise TypeError("executor_factory must be callable")
        if executor is not None and executor_factory is not None:
            raise ValueError("executor and executor_factory are mutually exclusive")
        if not callable(replay_builder) or not callable(evaluator):
            raise TypeError("replay_builder and evaluator must be callable")
        if (
            isinstance(max_cached_schedulers, bool)
            or not isinstance(max_cached_schedulers, int)
            or max_cached_schedulers < 1
            or max_cached_schedulers > _MAX_SCHEDULER_CACHE_SIZE
        ):
            raise ValueError(
                "max_cached_schedulers must be a bounded positive integer"
            )
        self.store = store
        self.artifact_store = artifact_store
        self.compiler = compiler
        self.scheduler_factory = scheduler_factory
        self.executor = executor
        self.executor_factory = executor_factory
        self.replay_builder = replay_builder
        self.evaluator = evaluator
        self.authorizer = authorizer
        self.scheduler_resolver = scheduler_resolver
        self.execution_driver = execution_driver
        self.recovery_driver = recovery_driver
        self.hierarchy_submitter = hierarchy_submitter
        self.max_cached_schedulers = max_cached_schedulers
        self._schedulers: OrderedDict[str, DurableScheduler] = OrderedDict()
        self._scheduler_lock = threading.RLock()

    def submit(
        self,
        run_id: str,
        workflow_ref: ArtifactRef,
        *,
        input_receipt: RunInputReceipt | None = None,
        parent: ParentSubmission | None = None,
        authorization_context: Any = None,
    ) -> dict[str, Any]:
        try:
            target_run_id = validate_identifier(run_id, "run_id")
        except ProtocolValidationError as exc:
            raise RuntimeInputError("run_id is invalid") from exc
        if not isinstance(parent, (ParentSubmission, type(None))):
            raise RuntimeInputError("parent must be a ParentSubmission")
        self._authorize_direct(
            Operation.SUBMIT,
            target_run_id,
            authorization_context,
            parent=parent,
        )
        return self._submit(
            SubmitBody(target_run_id, workflow_ref, input_receipt, parent)
        )

    def status(
        self,
        run_id: str,
        *,
        authorization_context: Any = None,
    ) -> dict[str, Any]:
        self._authorize_direct(Operation.STATUS, run_id, authorization_context)
        return self._status(run_id)

    def events(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
        limit: int = MAX_EVENT_PAGE_SIZE,
        authorization_context: Any = None,
    ) -> dict[str, Any]:
        self._authorize_direct(Operation.EVENTS, run_id, authorization_context)
        if (
            isinstance(after_sequence, bool)
            or not isinstance(after_sequence, int)
            or after_sequence < 0
            or isinstance(limit, bool)
            or not isinstance(limit, int)
            or limit < 1
            or limit > MAX_EVENT_PAGE_SIZE
        ):
            raise RuntimeInputError("event page bounds are invalid")
        return self._events(EventsBody(run_id, after_sequence, limit))

    def cancel(
        self,
        run_id: str,
        *,
        authorization_context: Any = None,
    ) -> dict[str, Any]:
        self._authorize_direct(Operation.CANCEL, run_id, authorization_context)
        self._scheduler_for(run_id).request_cancel(run_id)
        return self._status(run_id)

    def pause(
        self,
        run_id: str,
        *,
        authorization_context: Any = None,
    ) -> dict[str, Any]:
        self._authorize_direct(Operation.PAUSE, run_id, authorization_context)
        self._scheduler_for(run_id).request_pause(run_id)
        return self._status(run_id)

    def resume(
        self,
        run_id: str,
        *,
        authorization_context: Any = None,
    ) -> dict[str, Any]:
        self._authorize_direct(Operation.RESUME, run_id, authorization_context)
        self._scheduler_for(run_id).resume(run_id)
        return self._status(run_id)

    def recover(
        self,
        run_id: str,
        *,
        limit: int = MAX_CONTROL_STEPS,
        authorization_context: Any = None,
    ) -> dict[str, Any]:
        self._authorize_direct(Operation.RECOVER, run_id, authorization_context)
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or limit < 1
            or limit > MAX_CONTROL_STEPS
        ):
            raise RuntimeInputError("recovery bound is invalid")
        return self._recover(RecoverBody(run_id, limit))

    def tick(
        self,
        run_id: str,
        *,
        max_steps: int = 1,
        authorization_context: Any = None,
    ) -> dict[str, Any]:
        self._authorize_direct(Operation.TICK, run_id, authorization_context)
        if (
            isinstance(max_steps, bool)
            or not isinstance(max_steps, int)
            or max_steps < 1
            or max_steps > MAX_CONTROL_STEPS
        ):
            raise RuntimeInputError("tick bound is invalid")
        return self._tick(TickBody(run_id, max_steps))

    def handle(
        self,
        payload: Mapping[str, Any],
        *,
        authorization_context: Any = None,
    ) -> ProtocolResponse:
        """Validate, authorize, and dispatch one bounded protocol request."""

        try:
            request = parse_request(payload)
        except ProtocolValidationError:
            return ProtocolResponse.failure(
                None,
                "invalid_request",
                "request does not match protocol version 1",
            )
        try:
            self._authorize_protocol(request, authorization_context)
            result = self._dispatch(request)
            return ProtocolResponse.success(request.request_id, result)
        except RuntimeAuthorizationError:
            return ProtocolResponse.failure(
                request.request_id,
                "authorization_denied",
                "operation is not authorized",
            )
        except RunNotFoundError:
            return ProtocolResponse.failure(
                request.request_id,
                "run_not_found",
                "run was not found",
            )
        except RuntimeCapabilityUnavailable:
            return ProtocolResponse.failure(
                request.request_id,
                "capability_unavailable",
                "required trusted runtime capability is unavailable",
            )
        except (RuntimeInputError, WorkflowCompileError, ProtocolValidationError):
            return ProtocolResponse.failure(
                request.request_id,
                "invalid_input",
                "submitted references or definition are invalid",
            )
        except Exception:
            return ProtocolResponse.failure(
                request.request_id,
                "operation_failed",
                "operation outcome was not confirmed; query durable status",
            )

    def replay_run(
        self,
        run_id: str,
        *,
        authorization_context: Any = None,
    ) -> dict[str, Any]:
        """Return a bounded replay summary, never its projection payload."""

        self._authorize(
            AuthorizationRequest("replay", False, "internal-replay", run_id),
            authorization_context,
        )
        report = self.replay_builder(self.store, run_id)
        return {
            "run_id": run_id,
            "matches_live": bool(report.matches_live),
            "golden_digest": report.golden_digest,
            "event_count": report.snapshot.event_count,
            "last_sequence": report.snapshot.last_sequence,
            "divergence_count": len(report.diffs),
        }

    def evaluate(
        self,
        evidence: ReliabilityEvidence,
        *,
        authorization_context: Any = None,
    ) -> dict[str, Any]:
        """Evaluate explicit counters; telemetry is not accepted as evidence."""

        self._authorize(
            AuthorizationRequest("evaluate", False, "internal-evaluate", None),
            authorization_context,
        )
        if not isinstance(evidence, ReliabilityEvidence):
            raise RuntimeInputError("evaluation requires explicit evidence")
        report = self.evaluator(evidence)
        if not isinstance(report, ReliabilityReport):
            raise RuntimeCapabilityUnavailable(
                "evaluator returned an unsupported report"
            )
        return report.to_dict()

    def _dispatch(self, request: ProtocolRequest) -> dict[str, Any]:
        operation = request.operation
        if operation is Operation.SUBMIT:
            assert isinstance(request.body, SubmitBody)
            return self._submit(request.body)
        if operation is Operation.STATUS:
            assert isinstance(request.body, RunBody)
            return self._status(request.body.run_id)
        if operation is Operation.EVENTS:
            assert isinstance(request.body, EventsBody)
            return self._events(request.body)
        if operation in {Operation.CANCEL, Operation.PAUSE, Operation.RESUME}:
            assert isinstance(request.body, RunBody)
            scheduler = self._scheduler_for(request.body.run_id)
            if operation is Operation.CANCEL:
                scheduler.request_cancel(request.body.run_id)
            elif operation is Operation.PAUSE:
                scheduler.request_pause(request.body.run_id)
            else:
                scheduler.resume(request.body.run_id)
            return self._status(request.body.run_id)
        if operation is Operation.RECOVER:
            assert isinstance(request.body, RecoverBody)
            return self._recover(request.body)
        if operation is Operation.TICK:
            assert isinstance(request.body, TickBody)
            return self._tick(request.body)
        raise ProtocolValidationError("operation is unsupported")

    def _submit(self, body: SubmitBody) -> dict[str, Any]:
        workflow_ref = self._verified_ref(body.workflow_ref)
        if workflow_ref.size > MAX_WORKFLOW_BYTES:
            raise RuntimeInputError("workflow Artifact exceeds compiler bound")
        try:
            definition_bytes = self.artifact_store.read(workflow_ref)
        except Exception as exc:
            raise RuntimeInputError("workflow Artifact cannot be read") from exc
        if len(definition_bytes) > MAX_WORKFLOW_BYTES:
            raise RuntimeInputError("workflow Artifact exceeds compiler bound")
        try:
            raw_definition = json.loads(definition_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeInputError("workflow Artifact must contain JSON") from exc
        if not isinstance(raw_definition, dict):
            raise RuntimeInputError("workflow definition must be an object")
        workflow = self._compile(raw_definition)
        receipt = self._verified_input_receipt(body.input_receipt)
        try:
            self.store.bind_workflow(
                workflow.name,
                workflow.version,
                workflow.definition_digest,
                workflow_ref,
            )
        except WorkflowBindingConflictError as exc:
            raise RuntimeInputError(
                "workflow identity/version is already bound to different content"
            ) from exc
        binding_digest = self._submission_binding_digest(
            workflow,
            workflow_ref,
            receipt,
            body.parent,
        )
        existing = self.store.get_run(body.run_id)
        if existing is not None:
            self._validate_submission_binding(
                existing,
                workflow,
                workflow_ref,
                binding_digest,
            )
            scheduler = self._new_scheduler(workflow)
            with self._scheduler_lock:
                self._cache_scheduler_locked(
                    body.run_id,
                    scheduler,
                    required=False,
                )
            return self._status(body.run_id)
        scheduler = self._new_scheduler(workflow)
        run_id = body.run_id
        submission_metadata = {
            "runtime_submission_digest": binding_digest,
            "runtime_workflow_ref": workflow_ref.to_dict(),
        }
        with self._scheduler_lock:
            # Recheck under the cache lock: concurrent submits must neither
            # overflow the cache nor persist a Run that cannot be retained.
            existing = self.store.get_run(run_id)
            if existing is not None:
                self._validate_submission_binding(
                    existing,
                    workflow,
                    workflow_ref,
                    binding_digest,
                )
                self._cache_scheduler_locked(
                    run_id,
                    scheduler,
                    required=False,
                )
                return self._status(run_id)
            self._ensure_scheduler_slot_locked(run_id, required=True)
            if body.parent is None:
                try:
                    run = scheduler.create_run(
                        run_id,
                        input=receipt,
                        metadata=submission_metadata,
                    )
                except RunAlreadyExistsError:
                    existing = self.store.get_run(run_id)
                    if existing is None:
                        raise
                    self._validate_submission_binding(
                        existing,
                        workflow,
                        workflow_ref,
                        binding_digest,
                    )
                    self._cache_scheduler_locked(
                        run_id,
                        scheduler,
                        required=True,
                    )
                    return self._status(run_id)
            else:
                if self.hierarchy_submitter is None:
                    raise RuntimeCapabilityUnavailable(
                        "hierarchy submission is not configured"
                    )
                run = self.hierarchy_submitter(
                    body.parent,
                    scheduler,
                    run_id,
                    receipt,
                    submission_metadata,
                )
                if not isinstance(run, RunRecord) or run.run_id != run_id:
                    raise RuntimeCapabilityUnavailable(
                        "hierarchy submitter returned an invalid Run"
                    )
                self._validate_submission_binding(
                    run,
                    workflow,
                    workflow_ref,
                    binding_digest,
                )
            self._cache_scheduler_locked(
                run_id,
                scheduler,
                required=True,
            )
        return self._status(run_id)

    def _status(self, run_id: str) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id)
        node_counts = Counter(
            node.status.value for node in self.store.list_nodes(run_id)
        )
        attempt_counts = Counter(
            attempt.status.value for attempt in self.store.list_attempts(run_id)
        )
        return {
            "run_id": run_id,
            "status": run.status.value,
            "terminal": run.status.is_terminal,
            "last_event_sequence": run.last_event_sequence,
            "projection_version": run.projection_version,
            "node_status_counts": {
                key: node_counts[key] for key in sorted(node_counts)
            },
            "attempt_status_counts": {
                key: attempt_counts[key] for key in sorted(attempt_counts)
            },
        }

    def _events(self, body: EventsBody) -> dict[str, Any]:
        if self.store.get_run(body.run_id) is None:
            raise RunNotFoundError(body.run_id)
        events = tuple(
            self.store.list_events(
                body.run_id,
                after_seq=body.after_sequence,
                limit=body.limit,
            )
        )
        return {
            "run_id": body.run_id,
            "events": [self._public_event(event) for event in events],
            "next_sequence": (
                body.after_sequence if not events else events[-1].seq
            ),
            "has_more": len(events) == body.limit,
        }

    def _recover(self, body: RecoverBody) -> dict[str, Any]:
        scheduler = self._scheduler_for(body.run_id)
        if self.recovery_driver is None:
            raise RuntimeCapabilityUnavailable(
                "recovery driver is not configured"
            )
        processed = self._bounded_processed(
            self.recovery_driver(scheduler, body.run_id, body.limit),
            body.limit,
        )
        scheduler.reconcile(body.run_id)
        result = self._status(body.run_id)
        result["recovery"] = {
            "attempted": True,
            "processed": processed,
        }
        return result

    def _tick(self, body: TickBody) -> dict[str, Any]:
        scheduler = self._scheduler_for(body.run_id)
        processed = 0
        if self.execution_driver is not None:
            supplied_executor = (
                self.executor_factory(scheduler)
                if self.executor_factory is not None
                else self.executor
            )
            if supplied_executor is None:
                raise RuntimeCapabilityUnavailable(
                    "execution driver requires an executor or executor_factory"
                )
            processed = self._bounded_processed(
                self.execution_driver(
                    scheduler,
                    supplied_executor,
                    body.run_id,
                    body.max_steps,
                ),
                body.max_steps,
            )
        scheduler.reconcile(body.run_id)
        result = self._status(body.run_id)
        result["tick"] = {
            "processed": processed,
            "external_execution_enabled": self.execution_driver is not None,
        }
        return result

    def _scheduler_for(self, run_id: str) -> DurableScheduler:
        run = self.store.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id)
        with self._scheduler_lock:
            scheduler = self._schedulers.get(run_id)
            if scheduler is not None:
                self._schedulers.move_to_end(run_id)
        if scheduler is not None:
            return scheduler
        scheduler = (
            self.scheduler_resolver(run)
            if self.scheduler_resolver is not None
            else self._scheduler_from_persisted_definition(run)
        )
        if not self._scheduler_like(scheduler):
            raise RuntimeCapabilityUnavailable(
                "scheduler resolver could not bind the persisted definition"
            )
        with self._scheduler_lock:
            cached = self._schedulers.get(run_id)
            if cached is not None:
                self._schedulers.move_to_end(run_id)
                return cached
            self._cache_scheduler_locked(
                run_id,
                scheduler,
                required=True,
            )
            return scheduler

    def _cache_scheduler_locked(
        self,
        run_id: str,
        scheduler: DurableScheduler,
        *,
        required: bool,
    ) -> bool:
        cached = self._schedulers.get(run_id)
        if cached is not None:
            self._schedulers.move_to_end(run_id)
            return True
        if not self._ensure_scheduler_slot_locked(run_id, required=required):
            return False
        self._schedulers[run_id] = scheduler
        if len(self._schedulers) > self.max_cached_schedulers:
            raise RuntimeCapabilityUnavailable(
                "scheduler cache exceeded its hard bound"
            )
        return True

    def _ensure_scheduler_slot_locked(
        self,
        run_id: str,
        *,
        required: bool,
    ) -> bool:
        if run_id in self._schedulers:
            self._schedulers.move_to_end(run_id)
            return True
        if len(self._schedulers) < self.max_cached_schedulers:
            return True

        terminal_candidate: str | None = None
        for cached_run_id in self._schedulers:
            cached_run = self.store.get_run(cached_run_id)
            if cached_run is None or cached_run.status.is_terminal:
                terminal_candidate = cached_run_id
                break
        if terminal_candidate is not None:
            self._schedulers.pop(terminal_candidate, None)
            return True

        for cached_run_id in self._schedulers:
            cached_run = self.store.get_run(cached_run_id)
            if (
                cached_run is not None
                and (
                    self.scheduler_resolver is not None
                    or self._has_persisted_definition(cached_run)
                )
            ):
                self._schedulers.pop(cached_run_id, None)
                return True
        if required:
            raise RuntimeCapabilityUnavailable(
                "scheduler cache is full of active Runs that cannot be rebound"
            )
        return False

    def _new_scheduler(
        self,
        workflow: CompiledWorkflow,
    ) -> DurableScheduler:
        scheduler = self.scheduler_factory(self.store, workflow)
        if not self._scheduler_like(scheduler):
            raise RuntimeCapabilityUnavailable(
                "scheduler_factory returned an unsupported scheduler"
            )
        return scheduler

    def _scheduler_from_persisted_definition(
        self,
        run: RunRecord,
    ) -> DurableScheduler:
        binding = self.store.get_workflow_binding(
            run.workflow_id,
            run.workflow_version,
        )
        if (
            binding is None
            or binding.definition_digest != run.definition_digest
            or binding.workflow_ref is None
        ):
            raise RuntimeCapabilityUnavailable(
                "persisted workflow binding is missing or inconsistent"
            )
        raw_ref = run.metadata.get("runtime_workflow_ref")
        try:
            if (
                not isinstance(raw_ref, dict)
                or set(raw_ref) != set(ArtifactRef.__dataclass_fields__)
            ):
                raise ValueError("missing canonical workflow ArtifactRef")
            parsed_ref = ArtifactRef.from_dict(raw_ref)
            if parsed_ref.to_dict() != raw_ref:
                raise ValueError("non-canonical workflow ArtifactRef")
            workflow_ref = self._verified_ref(parsed_ref)
            if binding.workflow_ref != workflow_ref:
                raise ValueError(
                    "Run workflow Artifact does not match the global binding"
                )
            if workflow_ref.size > MAX_WORKFLOW_BYTES:
                raise ValueError("workflow Artifact exceeds compiler bound")
            definition_bytes = self.artifact_store.read(workflow_ref)
            if len(definition_bytes) > MAX_WORKFLOW_BYTES:
                raise ValueError("workflow Artifact exceeds compiler bound")
            definition = json.loads(definition_bytes.decode("utf-8"))
            if not isinstance(definition, dict):
                raise ValueError("workflow definition is not an object")
            workflow = self._compile(definition)
        except Exception as exc:
            raise RuntimeCapabilityUnavailable(
                "persisted workflow definition cannot be rebound"
            ) from exc
        if (
            workflow.name != run.workflow_id
            or workflow.version != run.workflow_version
            or workflow.definition_digest != run.definition_digest
        ):
            raise RuntimeCapabilityUnavailable(
                "persisted workflow definition identity does not match Run"
            )
        return self._new_scheduler(workflow)

    @staticmethod
    def _has_persisted_definition(run: RunRecord) -> bool:
        raw_ref = run.metadata.get("runtime_workflow_ref")
        if (
            not isinstance(raw_ref, dict)
            or set(raw_ref) != set(ArtifactRef.__dataclass_fields__)
        ):
            return False
        try:
            ref = ArtifactRef.from_dict(raw_ref)
        except (TypeError, ValueError):
            return False
        return ref.to_dict() == raw_ref

    def _compile(self, definition: Mapping[str, Any]) -> CompiledWorkflow:
        try:
            if isinstance(self.compiler, WorkflowCompiler):
                compiled = self.compiler.compile(definition)
            elif callable(self.compiler):
                compiled = self.compiler(definition)
            else:
                raise RuntimeCapabilityUnavailable("compiler is unavailable")
        except WorkflowCompileError:
            raise
        except Exception as exc:
            raise WorkflowCompileError("workflow compilation failed") from exc
        if not isinstance(compiled, CompiledWorkflow):
            raise RuntimeCapabilityUnavailable(
                "compiler returned an unsupported workflow"
            )
        return compiled

    def _verified_input_receipt(
        self,
        receipt: RunInputReceipt | None,
    ) -> RunInputReceipt | None:
        if receipt is None:
            return None
        if not isinstance(receipt, RunInputReceipt):
            raise RuntimeInputError("Run input must be a RunInputReceipt")
        return RunInputReceipt(
            tuple(self._verified_ref(ref) for ref in receipt.artifact_refs)
        )

    def _verified_ref(self, ref: ArtifactRef) -> ArtifactRef:
        if not isinstance(ref, ArtifactRef):
            raise RuntimeInputError("input is not an ArtifactRef")
        if ref.metadata:
            raise RuntimeInputError(
                "protocol ArtifactRef metadata must be empty"
            )
        try:
            detached = ArtifactRef.from_dict(ref.to_dict())
            verified = self.artifact_store.verify(detached)
        except Exception as exc:
            raise RuntimeInputError("Artifact verification failed") from exc
        if verified is not True:
            raise RuntimeInputError("Artifact verification failed")
        return detached

    def _authorize_protocol(
        self,
        request: ProtocolRequest,
        context: Any,
    ) -> None:
        run_id = getattr(request.body, "run_id", None)
        parent = (
            request.body.parent
            if isinstance(request.body, SubmitBody)
            else None
        )
        self._authorize(
            AuthorizationRequest(
                operation=request.operation.value,
                mutation=request.operation.mutation,
                request_id=request.request_id,
                run_id=run_id if isinstance(run_id, str) else None,
                parent_run_id=(
                    parent.parent_run_id if parent is not None else None
                ),
                parent_node_id=(
                    parent.parent_node_id if parent is not None else None
                ),
            ),
            context,
        )

    def _authorize_direct(
        self,
        operation: Operation,
        run_id: str | None,
        context: Any,
        *,
        parent: ParentSubmission | None = None,
    ) -> None:
        self._authorize(
            AuthorizationRequest(
                operation=operation.value,
                mutation=operation.mutation,
                request_id=f"direct-{operation.value}",
                run_id=run_id,
                parent_run_id=(
                    parent.parent_run_id if parent is not None else None
                ),
                parent_node_id=(
                    parent.parent_node_id if parent is not None else None
                ),
            ),
            context,
        )

    def _authorize(
        self,
        request: AuthorizationRequest,
        context: Any,
    ) -> None:
        authorizer = self.authorizer
        if authorizer is None:
            raise RuntimeAuthorizationError("authorization denied")
        try:
            if isinstance(authorizer, TrustedAuthorizer):
                authorized = authorizer.authorize(request, context)
            elif callable(authorizer):
                authorized = authorizer(request, context)
            else:
                authorized = False
        except Exception as exc:
            raise RuntimeAuthorizationError("authorization denied") from exc
        if authorized is not True:
            raise RuntimeAuthorizationError("authorization denied")

    @staticmethod
    def _public_event(event: EventRecord) -> dict[str, Any]:
        projected = project_domain_event(event)
        return {
            "sequence": event.seq,
            "type": projected.name,
            "occurred_at": event.occurred_at,
            "has_node": bool(projected.data["has_node"]),
            "has_attempt": bool(projected.data["has_attempt"]),
            "terminal": bool(projected.data["terminal"]),
        }

    @staticmethod
    def _bounded_processed(value: Any, maximum: int) -> int:
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            or value > maximum
        ):
            raise RuntimeCapabilityUnavailable(
                "trusted driver returned an invalid processed count"
            )
        return value

    @staticmethod
    def _submission_binding_digest(
        workflow: CompiledWorkflow,
        workflow_ref: ArtifactRef,
        receipt: RunInputReceipt | None,
        parent: ParentSubmission | None,
    ) -> str:
        refs: list[dict[str, Any]] = []
        for ref in () if receipt is None else receipt.artifact_refs:
            refs.append(
                {
                    "artifact_id": ref.artifact_id,
                    "sha256": ref.sha256,
                    "size": ref.size,
                    "kind": ref.kind.value,
                    "producer_run_id": ref.producer_run_id,
                    "producer_node_id": ref.producer_node_id,
                    "producer_attempt_id": ref.producer_attempt_id,
                }
            )
        payload = {
            "workflow_definition_digest": workflow.definition_digest,
            "workflow_name": workflow.name,
            "workflow_version": workflow.version,
            "workflow_artifact_ref": (
                OrchestrationRuntime._artifact_binding_identity(workflow_ref)
            ),
            "input_artifact_refs": refs,
            "parent": (
                None
                if parent is None
                else {
                    "parent_run_id": parent.parent_run_id,
                    "parent_node_id": parent.parent_node_id,
                }
            ),
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _validate_submission_binding(
        run: RunRecord,
        workflow: CompiledWorkflow,
        workflow_ref: ArtifactRef,
        binding_digest: str,
    ) -> None:
        persisted_payload = run.metadata.get("runtime_workflow_ref")
        try:
            if (
                not isinstance(persisted_payload, dict)
                or set(persisted_payload)
                != set(ArtifactRef.__dataclass_fields__)
            ):
                raise ValueError("incomplete workflow ArtifactRef")
            persisted_ref = ArtifactRef.from_dict(persisted_payload)
            if persisted_ref.to_dict() != persisted_payload:
                raise ValueError("non-canonical workflow ArtifactRef")
            same_artifact = (
                OrchestrationRuntime._artifact_binding_identity(
                    persisted_ref
                )
                == OrchestrationRuntime._artifact_binding_identity(
                    workflow_ref
                )
            )
        except Exception:
            same_artifact = False
        if (
            run.workflow_id != workflow.name
            or run.workflow_version != workflow.version
            or run.definition_digest != workflow.definition_digest
            or run.metadata.get("runtime_submission_digest") != binding_digest
            or not same_artifact
        ):
            raise RuntimeInputError(
                "run_id is already bound to another submission"
            )

    @staticmethod
    def _artifact_binding_identity(ref: ArtifactRef) -> dict[str, Any]:
        identity = ref.to_dict()
        identity.pop("created_at")
        return identity

    @staticmethod
    def _scheduler_like(value: Any) -> bool:
        return all(
            callable(getattr(value, method, None))
            for method in (
                "create_run",
                "reconcile",
                "request_cancel",
                "request_pause",
                "resume",
            )
        )


__all__ = [
    "AuthorizationRequest",
    "ExecutorFactory",
    "ExecutionDriver",
    "HierarchySubmitter",
    "OrchestrationRuntime",
    "RecoveryDriver",
    "RuntimeAuthorizationError",
    "RuntimeCapabilityUnavailable",
    "RuntimeErrorBase",
    "RuntimeInputError",
    "SchedulerFactory",
    "SchedulerResolver",
    "TrustedAuthorizer",
    "WorkflowCompiler",
]
