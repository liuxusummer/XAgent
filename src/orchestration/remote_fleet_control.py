"""Trusted bridge from fleet routing to authenticated durable Worker claims.

Fleet state is a bounded, rebuildable projection. Durable claim and terminal
truth remain exclusively in ``RemoteControlPlane`` and ``DurableRunStore``.
"""

from __future__ import annotations

import math
import re
import threading
from dataclasses import dataclass
from typing import Mapping, Protocol, runtime_checkable

from .metadata_security import contains_sensitive_key
from .models import AttemptStatus, NodeStatus, RunStatus
from .remote_control import (
    RemoteControlPlane,
    WorkerRegistration,
)
from .remote_fleet import (
    FleetAssignment,
    FleetTaskBinding,
    RemoteFleetCoordinator,
)
from .remote_protocol import (
    MAX_LEASE_SECONDS,
    MIN_LEASE_SECONDS,
    AuthenticatedWorker,
    ClaimBinding,
    WorkAssignment,
    canonical_digest,
)
from .remote_scheduling import (
    FleetAdmissionScope,
    MAX_WORKER_CAPACITY,
    ReleaseOutcome,
    RemoteTask,
    WorkerDescriptor,
)
from .scheduler import (
    ACTIVE_ATTEMPT_STATUSES,
    DurableScheduler,
)
from .store import FleetShardOwnership

MAX_FLEET_WORKER_POLICIES = 4_096
MAX_TRACKED_FLEET_ASSIGNMENTS = 1_000_000
MAX_PROJECTED_FLEET_TASKS = 100_000
MAX_PROJECTED_FLEET_ATTEMPTS = 1_000_000
MAX_FLEET_TOOL_POLICIES = 4_096
_FLEET_OWNER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")


class RemoteFleetControlError(RuntimeError):
    """Base error for the trusted Fleet/control composition."""


class RemoteFleetControlConfigurationError(
    RemoteFleetControlError,
    ValueError,
):
    """The Fleet/control composition is unsafe or malformed."""


class RemoteFleetControlConflict(RemoteFleetControlError):
    """Fleet projection conflicts with authenticated durable state."""


@dataclass(frozen=True, slots=True)
class FleetToolRoutingPolicy:
    """Versioned server authority for one Tool's routing requirements."""

    tool_name: str
    required_capabilities: frozenset[str]
    policy_version: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.required_capabilities, frozenset)
            or "activity.tool" not in self.required_capabilities
        ):
            raise RemoteFleetControlConfigurationError(
                "Fleet Tool capabilities are invalid"
            )
        try:
            task = RemoteTask(
                task_id="policy-validation",
                tenant_id="policy-validation",
                pool_id="policy-validation",
                tool_name=self.tool_name,
                required_capabilities=self.required_capabilities,
            )
            policy_version = RemoteTask(
                task_id=self.policy_version,
                tenant_id="policy-validation",
                pool_id="policy-validation",
                tool_name=self.tool_name,
            ).task_id
        except (TypeError, ValueError) as exc:
            raise RemoteFleetControlConfigurationError(
                "Fleet Tool routing policy is invalid"
            ) from exc
        object.__setattr__(self, "tool_name", task.tool_name)
        object.__setattr__(
            self,
            "required_capabilities",
            task.required_capabilities,
        )
        object.__setattr__(self, "policy_version", policy_version)

    @property
    def policy_digest(self) -> str:
        return canonical_digest(
            {
                "schema": "fleet_tool_routing_policy_v1",
                "tool_name": self.tool_name,
                "required_capabilities": sorted(
                    self.required_capabilities
                ),
                "policy_version": self.policy_version,
            }
        )


@runtime_checkable
class FleetToolPolicyResolver(Protocol):
    """Resolve complete server-owned routing requirements for a Tool."""

    production_security_ready: bool

    def resolve(self, tool_name: str) -> FleetToolRoutingPolicy: ...


class StaticFleetToolPolicyResolver:
    """Immutable exact Tool routing policy registry."""

    def __init__(
        self,
        policies: Mapping[str, FleetToolRoutingPolicy]
        | tuple[FleetToolRoutingPolicy, ...]
        | list[FleetToolRoutingPolicy],
    ) -> None:
        if isinstance(policies, Mapping):
            values = tuple(policies.values())
            if any(
                key != policy.tool_name
                for key, policy in policies.items()
                if isinstance(policy, FleetToolRoutingPolicy)
            ):
                raise RemoteFleetControlConfigurationError(
                    "Fleet Tool policy key conflicts with its value"
                )
        elif isinstance(policies, (tuple, list)):
            values = tuple(policies)
        else:
            raise RemoteFleetControlConfigurationError(
                "Fleet Tool policies are invalid"
            )
        if not 1 <= len(values) <= MAX_FLEET_TOOL_POLICIES:
            raise RemoteFleetControlConfigurationError(
                "Fleet Tool policies are invalid"
            )
        by_tool: dict[str, FleetToolRoutingPolicy] = {}
        for policy in values:
            if not isinstance(policy, FleetToolRoutingPolicy):
                raise RemoteFleetControlConfigurationError(
                    "Fleet Tool policy is invalid"
                )
            if policy.tool_name in by_tool:
                raise RemoteFleetControlConfigurationError(
                    "duplicate Fleet Tool policy"
                )
            by_tool[policy.tool_name] = policy
        self._policies = by_tool

    @property
    def production_security_ready(self) -> bool:
        return bool(self._policies)

    def resolve(self, tool_name: str) -> FleetToolRoutingPolicy:
        policy = self._policies.get(tool_name)
        if policy is None:
            raise RemoteFleetControlConflict(
                "Tool has no Fleet routing policy"
            )
        return policy


@dataclass(frozen=True, slots=True)
class FleetWorkerPolicy:
    """Server-owned routing authority for one Worker identity."""

    worker_id: str
    tenant_id: str
    pool_id: str
    tools: frozenset[str]
    allowed_capabilities: frozenset[str]
    allowed_resource_keys: frozenset[str] = frozenset()
    max_concurrency: int = 1

    def __post_init__(self) -> None:
        if (
            not isinstance(self.tools, frozenset)
            or not isinstance(self.allowed_capabilities, frozenset)
            or not isinstance(self.allowed_resource_keys, frozenset)
            or "activity.tool" not in self.allowed_capabilities
        ):
            raise RemoteFleetControlConfigurationError(
                "Fleet Worker policy tools and capabilities are invalid"
            )
        try:
            descriptor = WorkerDescriptor(
                worker_id=self.worker_id,
                session_id="policy-validation",
                pool_id=self.pool_id,
                runtime_version="0",
                capabilities=self.allowed_capabilities,
                tools=self.tools,
                authorized_tenants=frozenset({self.tenant_id}),
                resource_keys=self.allowed_resource_keys,
                capacity=self.max_concurrency,
            )
        except (TypeError, ValueError) as exc:
            raise RemoteFleetControlConfigurationError(
                "Fleet Worker policy is invalid"
            ) from exc
        object.__setattr__(
            self,
            "worker_id",
            descriptor.worker_id,
        )
        object.__setattr__(
            self,
            "tenant_id",
            next(iter(descriptor.authorized_tenants)),
        )
        object.__setattr__(self, "pool_id", descriptor.pool_id)
        object.__setattr__(self, "tools", descriptor.tools)
        object.__setattr__(
            self,
            "allowed_capabilities",
            descriptor.capabilities,
        )
        object.__setattr__(
            self,
            "allowed_resource_keys",
            descriptor.resource_keys,
        )
        object.__setattr__(
            self,
            "max_concurrency",
            descriptor.capacity,
        )


@runtime_checkable
class FleetWorkerResolver(Protocol):
    """Resolve trusted server policy plus current authenticated session."""

    production_security_ready: bool

    def resolve(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
    ) -> WorkerDescriptor: ...


class StaticFleetWorkerResolver:
    """Immutable exact Worker policy registry for a bounded Fleet."""

    def __init__(
        self,
        policies: tuple[FleetWorkerPolicy, ...]
        | list[FleetWorkerPolicy],
    ) -> None:
        if (
            not isinstance(policies, (list, tuple))
            or not 1 <= len(policies) <= MAX_FLEET_WORKER_POLICIES
        ):
            raise RemoteFleetControlConfigurationError(
                "Fleet Worker policies are invalid"
            )
        by_worker: dict[str, FleetWorkerPolicy] = {}
        for policy in policies:
            if not isinstance(policy, FleetWorkerPolicy):
                raise RemoteFleetControlConfigurationError(
                    "Fleet Worker policy is invalid"
                )
            if policy.worker_id in by_worker:
                raise RemoteFleetControlConfigurationError(
                    "duplicate Fleet Worker policy"
                )
            by_worker[policy.worker_id] = policy
        self._policies = by_worker

    @property
    def production_security_ready(self) -> bool:
        return bool(self._policies)

    def resolve(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
    ) -> WorkerDescriptor:
        if (
            not isinstance(identity, AuthenticatedWorker)
            or not isinstance(registration, WorkerRegistration)
            or registration.worker_id != identity.worker_id
            or registration.tenant_id != identity.tenant_id
            or registration.identity_digest != identity.identity_digest
        ):
            raise RemoteFleetControlConflict(
                "Fleet identity does not match current registration"
            )
        policy = self._policies.get(identity.worker_id)
        if (
            policy is None
            or policy.tenant_id != identity.tenant_id
        ):
            raise RemoteFleetControlConflict(
                "Worker has no matching Fleet policy"
            )
        try:
            return WorkerDescriptor(
                worker_id=identity.worker_id,
                session_id=registration.session_binding_digest,
                pool_id=policy.pool_id,
                runtime_version=registration.runtime_version,
                capabilities=(
                    frozenset(registration.capabilities)
                    & policy.allowed_capabilities
                ),
                tools=policy.tools,
                authorized_tenants=frozenset({identity.tenant_id}),
                resource_keys=(
                    frozenset(registration.resource_keys)
                    & policy.allowed_resource_keys
                ),
                capacity=min(
                    registration.max_concurrency,
                    policy.max_concurrency,
                    MAX_WORKER_CAPACITY,
                ),
            )
        except (TypeError, ValueError) as exc:
            raise RemoteFleetControlConflict(
                "Worker registration is incompatible with Fleet policy"
            ) from exc


class DurableFleetProjector:
    """Build exact, secret-free Fleet bindings from durable READY nodes."""

    def __init__(
        self,
        routing_policies: FleetToolPolicyResolver,
        *,
        max_tasks: int = 4_096,
        max_attempts: int = 100_000,
    ) -> None:
        if not isinstance(routing_policies, FleetToolPolicyResolver):
            raise TypeError(
                "routing_policies must implement FleetToolPolicyResolver"
            )
        if routing_policies.production_security_ready is not True:
            raise RemoteFleetControlConfigurationError(
                "Fleet Tool policies are not production ready"
            )
        if (
            isinstance(max_tasks, bool)
            or not isinstance(max_tasks, int)
            or not 1 <= max_tasks <= MAX_PROJECTED_FLEET_TASKS
        ):
            raise RemoteFleetControlConfigurationError(
                "Fleet projector max_tasks is invalid"
            )
        if (
            isinstance(max_attempts, bool)
            or not isinstance(max_attempts, int)
            or not 1
            <= max_attempts
            <= MAX_PROJECTED_FLEET_ATTEMPTS
        ):
            raise RemoteFleetControlConfigurationError(
                "Fleet projector max_attempts is invalid"
            )
        self.max_tasks = max_tasks
        self.max_attempts = max_attempts
        self._routing_policies = routing_policies

    def project_ready(
        self,
        scheduler: DurableScheduler,
        run_id: str,
        *,
        tenant_id: str,
        pool_id: str,
        shard_ownership: FleetShardOwnership | None = None,
    ) -> tuple[FleetTaskBinding, ...]:
        if not isinstance(scheduler, DurableScheduler):
            raise TypeError("scheduler must be a DurableScheduler")
        if shard_ownership is not None:
            if not isinstance(
                shard_ownership,
                FleetShardOwnership,
            ):
                raise TypeError(
                    "shard_ownership must be FleetShardOwnership"
                )
            if (
                scheduler.store.get_fleet_shard_ownership(
                    shard_ownership.shard_id
                )
                != shard_ownership
            ):
                raise RemoteFleetControlConflict(
                    "Fleet shard ownership is stale"
                )
            if (
                shard_ownership.pool_id != pool_id
            ):
                raise RemoteFleetControlConflict(
                    "Fleet shard ownership belongs to another pool"
                )
        run = scheduler.store.get_run(run_id)
        if run is None or run.status is not RunStatus.RUNNING:
            return ()
        nodes = {
            node.node_id: node
            for node in scheduler.store.list_nodes(run_id)
        }
        attempts = []
        while len(attempts) <= self.max_attempts:
            page_limit = min(
                1_000,
                self.max_attempts + 1 - len(attempts),
            )
            page = scheduler.store.list_attempts(
                run_id,
                limit=page_limit,
                offset=len(attempts),
            )
            attempts.extend(page)
            if len(page) < page_limit:
                break
        if len(attempts) > self.max_attempts:
            raise RemoteFleetControlConflict(
                "Fleet projection exceeds its Attempt scan bound"
            )
        by_node: dict[str, list] = {}
        for attempt in attempts:
            by_node.setdefault(attempt.node_id, []).append(attempt)
        bindings: list[FleetTaskBinding] = []
        for node_id in scheduler.workflow.topological_order:
            node = nodes.get(node_id)
            definition = scheduler.workflow.get_node(node_id)
            if (
                node is None
                or node.status is not NodeStatus.READY
                or definition.kind != "tool"
            ):
                continue
            active = [
                attempt
                for attempt in by_node.get(node_id, ())
                if attempt.status in ACTIVE_ATTEMPT_STATUSES
            ]
            if len(active) > 1:
                raise RemoteFleetControlConflict(
                    "durable node has multiple active Attempts"
                )
            if (
                active
                and active[0].status is not AttemptStatus.SCHEDULED
            ):
                continue
            config = definition.config.to_dict()
            if contains_sensitive_key(config):
                raise RemoteFleetControlConflict(
                    "sensitive Activity config cannot enter Fleet routing"
                )
            tool_name = config.get("tool")
            if not isinstance(tool_name, str) or not tool_name.strip():
                raise RemoteFleetControlConflict(
                    "Fleet Tool node has no bounded tool identity"
                )
            routing_policy = self._routing_policies.resolve(tool_name)
            config_digest = canonical_digest(config)
            compatibility = definition.runtime_compatibility
            task_identity = canonical_digest(
                {
                    "schema": "durable_fleet_task_v1",
                    "definition_digest": scheduler.workflow.definition_digest,
                    "run_id": run_id,
                    "node_id": node_id,
                    "config_digest": config_digest,
                }
            )
            binding = FleetTaskBinding(
                run_id=run_id,
                node_id=node_id,
                activity_config_digest=config_digest,
                routing_policy_digest=(
                    routing_policy.policy_digest
                ),
                shard_ownership=shard_ownership,
                task=RemoteTask(
                    task_id=f"flt-{task_identity[:60]}",
                    tenant_id=tenant_id,
                    pool_id=pool_id,
                    tool_name=tool_name,
                    required_capabilities=(
                        routing_policy.required_capabilities
                    ),
                    required_resource_keys=frozenset(
                        definition.resource_keys
                    ),
                    min_runtime_version=(
                        "0"
                        if compatibility is None
                        else compatibility.min_runtime_version
                    ),
                    max_runtime_version=(
                        None
                        if compatibility is None
                        else compatibility.max_runtime_version
                    ),
                ),
            )
            bindings.append(binding)
            if len(bindings) > self.max_tasks:
                raise RemoteFleetControlConflict(
                    "Fleet projection exceeds its task bound"
                )
        return tuple(bindings)


class RemoteControlFleetClaimer:
    """Production-marked callback preserving control session authority."""

    durable_fleet_admission_ready = True
    durable_fleet_fairness_ready = True

    def __init__(
        self,
        control: RemoteControlPlane,
        *,
        lease_seconds: float = 60.0,
        fleet_owner_id: str | None = None,
    ) -> None:
        if not isinstance(control, RemoteControlPlane):
            raise TypeError("control must be a RemoteControlPlane")
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(float(lease_seconds))
            or not MIN_LEASE_SECONDS
            <= float(lease_seconds)
            <= MAX_LEASE_SECONDS
        ):
            raise RemoteFleetControlConfigurationError(
                "Fleet lease_seconds is invalid"
            )
        if fleet_owner_id is not None and (
            not isinstance(fleet_owner_id, str)
            or _FLEET_OWNER_ID.fullmatch(fleet_owner_id) is None
        ):
            raise RemoteFleetControlConfigurationError(
                "fleet_owner_id must be a bounded Fleet identifier"
            )
        self._control = control
        self.lease_seconds = float(lease_seconds)
        self.fleet_owner_id = fleet_owner_id

    @property
    def production_security_ready(self) -> bool:
        return self._control.production_security_ready

    @property
    def durable_fleet_ownership_ready(self) -> bool:
        return self.fleet_owner_id is not None

    def __call__(
        self,
        binding: object,
        worker_id: str,
        worker_session_id: str,
        admission_scope: FleetAdmissionScope,
        shard_ownership: FleetShardOwnership | None = None,
    ) -> WorkAssignment:
        if (
            not isinstance(binding, FleetTaskBinding)
            or not binding.exact_for_production
            or binding.node_id is None
            or binding.activity_config_digest is None
            or not isinstance(admission_scope, FleetAdmissionScope)
            or admission_scope.task_id != binding.task.task_id
            or admission_scope.tenant_id != binding.task.tenant_id
            or admission_scope.pool_id != binding.task.pool_id
            or admission_scope.routing_policy_digest
            != binding.routing_policy_digest
            or binding.shard_ownership != shard_ownership
            or (
                shard_ownership is not None
                and shard_ownership.owner_id != self.fleet_owner_id
            )
        ):
            raise RemoteFleetControlConflict(
                "Fleet claim requires an exact durable candidate binding"
            )
        return self._control.claim_for_fleet(
            binding.run_id,
            worker_id,
            lease_seconds=self.lease_seconds,
            node_id=binding.node_id,
            activity_config_digest=binding.activity_config_digest,
            expected_session_binding_digest=worker_session_id,
            fleet_admission=admission_scope.to_metadata(),
            fleet_shard_ownership=(
                None
                if shard_ownership is None
                else shard_ownership.to_metadata()
            ),
        )

    def is_terminal(self, claim: ClaimBinding) -> bool:
        return self._control.fleet_claim_is_terminal(claim)


@runtime_checkable
class FleetTerminalProbe(Protocol):
    production_security_ready: bool

    def is_terminal(self, claim: ClaimBinding) -> bool: ...


@dataclass(frozen=True, slots=True)
class FleetControlSnapshot:
    active_assignments: int
    inflight_polls: int
    max_active_assignments: int
    execution_truth: bool = False


@dataclass(frozen=True, slots=True)
class FleetTerminalReconcileReport:
    examined: int
    released: int
    probe_failures: int
    release_failures: int
    remaining: int
    execution_truth: bool = False


class SecureRemoteFleetPoller:
    """Bind authenticated sessions to cross-Run routing and terminal release."""

    def __init__(
        self,
        fleet: RemoteFleetCoordinator,
        resolver: FleetWorkerResolver,
        terminal_probe: FleetTerminalProbe,
        *,
        max_active_assignments: int = 4_096,
    ) -> None:
        if not isinstance(fleet, RemoteFleetCoordinator):
            raise TypeError("fleet must be a RemoteFleetCoordinator")
        if not isinstance(resolver, FleetWorkerResolver):
            raise TypeError("resolver must implement FleetWorkerResolver")
        if not isinstance(terminal_probe, FleetTerminalProbe):
            raise TypeError(
                "terminal_probe must implement FleetTerminalProbe"
            )
        if (
            isinstance(max_active_assignments, bool)
            or not isinstance(max_active_assignments, int)
            or not 1
            <= max_active_assignments
            <= MAX_TRACKED_FLEET_ASSIGNMENTS
        ):
            raise RemoteFleetControlConfigurationError(
                "max_active_assignments is invalid"
            )
        if (
            fleet.production_security_ready is not True
            or resolver.production_security_ready is not True
            or terminal_probe.production_security_ready is not True
        ):
            raise RemoteFleetControlConfigurationError(
                "Fleet control dependencies are not production ready"
            )
        self._fleet = fleet
        self._resolver = resolver
        self._terminal_probe = terminal_probe
        self.max_active_assignments = max_active_assignments
        self._lock = threading.RLock()
        self._active: dict[tuple[str, str], FleetAssignment] = {}
        self._inflight_polls = 0

    @property
    def production_security_ready(self) -> bool:
        try:
            return (
                self._fleet.production_security_ready is True
                and self._resolver.production_security_ready is True
                and self._terminal_probe.production_security_ready is True
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return False

    def poll(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
    ) -> WorkAssignment | None:
        descriptor = self._resolver.resolve(identity, registration)
        if (
            descriptor.worker_id != registration.worker_id
            or descriptor.session_id
            != registration.session_binding_digest
            or descriptor.authorized_tenants
            != frozenset({identity.tenant_id})
        ):
            raise RemoteFleetControlConflict(
                "Fleet descriptor exceeds authenticated session authority"
            )
        with self._lock:
            if (
                len(self._active) + self._inflight_polls
                >= self.max_active_assignments
            ):
                raise RemoteFleetControlConflict(
                    "Fleet assignment registry is full"
                )
            self._inflight_polls += 1
        assignment: FleetAssignment | None = None
        try:
            worker = self._fleet.register_worker(descriptor)
            assignment = self._fleet.assign_next(
                descriptor.worker_id,
                worker_generation=worker.generation,
                session_id=descriptor.session_id,
            )
            if assignment is None:
                return None
            durable = assignment.durable
            if (
                durable.worker_id != registration.worker_id
                or durable.claim.session_binding_digest
                != registration.session_binding_digest
            ):
                raise RemoteFleetControlConflict(
                    "durable assignment exceeds authenticated session authority"
                )
            with self._lock:
                claim_key = (
                    durable.claim.run_id,
                    durable.claim.attempt_id,
                )
                if claim_key in self._active:
                    raise RemoteFleetControlConflict(
                        "duplicate durable Fleet assignment"
                    )
                self._active[claim_key] = assignment
            return durable
        except BaseException:
            if assignment is not None:
                try:
                    self._fleet.release_rejected(assignment)
                except (KeyboardInterrupt, SystemExit):
                    raise
                except BaseException:
                    pass
            raise
        finally:
            with self._lock:
                self._inflight_polls -= 1

    def release_terminal(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        claim: ClaimBinding,
    ) -> None:
        if (
            not isinstance(identity, AuthenticatedWorker)
            or not isinstance(registration, WorkerRegistration)
            or not isinstance(claim, ClaimBinding)
            or registration.worker_id != identity.worker_id
            or registration.tenant_id != identity.tenant_id
            or registration.identity_digest != identity.identity_digest
        ):
            raise RemoteFleetControlConflict(
                "terminal Fleet identity is invalid"
            )
        claim_key = (claim.run_id, claim.attempt_id)
        with self._lock:
            assignment = self._active.get(claim_key)
        if assignment is None:
            return
        if (
            assignment.durable.claim != claim
            or assignment.durable.worker_id != identity.worker_id
            or claim.session_binding_digest
            != registration.session_binding_digest
        ):
            raise RemoteFleetControlConflict(
                "terminal claim conflicts with Fleet assignment"
            )
        outcome = self._fleet.release_terminal(assignment)
        if outcome is not ReleaseOutcome.RELEASED:
            raise RemoteFleetControlConflict(
                "Fleet terminal release was rejected"
            )
        with self._lock:
            if self._active.get(claim_key) is assignment:
                self._active.pop(claim_key, None)

    def snapshot(self) -> FleetControlSnapshot:
        with self._lock:
            return FleetControlSnapshot(
                active_assignments=len(self._active),
                inflight_polls=self._inflight_polls,
                max_active_assignments=self.max_active_assignments,
            )

    def reconcile_terminals(self) -> FleetTerminalReconcileReport:
        """Release projections only after an exact durable terminal probe."""

        with self._lock:
            active = tuple(self._active.items())
        released = 0
        probe_failures = 0
        release_failures = 0
        for claim_key, assignment in active:
            try:
                terminal = self._terminal_probe.is_terminal(
                    assignment.durable.claim
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                probe_failures += 1
                continue
            if terminal is not True:
                continue
            try:
                outcome = self._fleet.release_terminal(assignment)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                release_failures += 1
                continue
            if outcome is not ReleaseOutcome.RELEASED:
                with self._lock:
                    if self._active.get(claim_key) is not assignment:
                        continue
                release_failures += 1
                continue
            with self._lock:
                if self._active.get(claim_key) is assignment:
                    self._active.pop(claim_key, None)
                    released += 1
        with self._lock:
            remaining = len(self._active)
        return FleetTerminalReconcileReport(
            examined=len(active),
            released=released,
            probe_failures=probe_failures,
            release_failures=release_failures,
            remaining=remaining,
        )


__all__ = [
    "DurableFleetProjector",
    "FleetControlSnapshot",
    "FleetTerminalProbe",
    "FleetTerminalReconcileReport",
    "FleetToolPolicyResolver",
    "FleetToolRoutingPolicy",
    "FleetWorkerPolicy",
    "FleetWorkerResolver",
    "RemoteControlFleetClaimer",
    "RemoteFleetControlConfigurationError",
    "RemoteFleetControlConflict",
    "RemoteFleetControlError",
    "SecureRemoteFleetPoller",
    "StaticFleetToolPolicyResolver",
    "StaticFleetWorkerResolver",
]
