from __future__ import annotations

import asyncio
import json
import unittest
from dataclasses import replace

from src.orchestration.lease import DurableLeaseReaper
from src.orchestration.remote_control import (
    RemoteControlError,
    RemoteControlPlane,
)
from src.orchestration.remote_fleet import (
    FleetTaskBinding,
    RemoteFleetConflict,
    RemoteFleetCoordinator,
)
from src.orchestration.remote_fleet_control import (
    DurableFleetProjector,
    FleetToolRoutingPolicy,
    FleetWorkerPolicy,
    RemoteControlFleetClaimer,
    RemoteFleetControlConfigurationError,
    RemoteFleetControlConflict,
    SecureRemoteFleetPoller,
    StaticFleetToolPolicyResolver,
    StaticFleetWorkerResolver,
)
from src.orchestration.remote_http import (
    AsgiTlsPeerAuthenticator,
    RemoteHttpASGIApp,
)
from src.orchestration.remote_protocol import (
    AuthenticatedWorker,
    RemoteOperation,
    RemoteProtocolError,
    WorkAssignment,
    make_request,
    parse_response,
)
from src.orchestration.remote_journal import RemoteControlJournal
from src.orchestration.remote_scheduling import (
    DeterministicRemoteScheduler,
    WorkerDescriptor,
)
from src.orchestration.remote_worker import (
    RemoteWorkerClient,
    RemoteWorkerError,
)
from src.orchestration.scheduler import DurableScheduler
from src.orchestration.store import DurableRunStore
from src.orchestration.workflow import compile_workflow

from tests import test_orchestration_remote_http as http_tests
from tests import test_orchestration_remote_protocol as protocol_tests


class _WrongSessionResolver:
    production_security_ready = True

    def resolve(self, identity, registration):
        return WorkerDescriptor(
            worker_id=identity.worker_id,
            session_id="wrong-session",
            pool_id="pool-a",
            runtime_version=registration.runtime_version,
            capabilities=frozenset(registration.capabilities),
            tools=frozenset({"inspect"}),
            authorized_tenants=frozenset({identity.tenant_id}),
            capacity=1,
        )


class _NonProductionClaimer:
    production_security_ready = False

    def __call__(self, _run_id, _worker_id):
        raise AssertionError("must not be called")

    def is_terminal(self, _claim):
        raise AssertionError("must not be called")


class _LegacyProductionClaimer(_NonProductionClaimer):
    production_security_ready = True


class _ExplodingTerminalProbe:
    production_security_ready = True

    def is_terminal(self, _claim):
        raise RuntimeError("store-probe-private-path")


class _IdentityVerifier:
    production_security_ready = True

    def __init__(self, identity):
        self.identity = identity

    def verify(self, _evidence):
        return self.identity


class SecureRemoteFleetControlTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = protocol_tests.RemoteProtocolTests(
            methodName="runTest"
        )
        self.harness.setUp()
        self.addCleanup(self.harness.doCleanups)
        self.client = self.harness.client
        self.client.register(
            runtime_version="1.0",
            capabilities=("activity.tool", "artifact.refs"),
            resource_keys=("workspace:project",),
            activity_kinds=("tool",),
            max_concurrency=2,
        )

    @staticmethod
    def _projector() -> DurableFleetProjector:
        return DurableFleetProjector(
            StaticFleetToolPolicyResolver(
                [
                    FleetToolRoutingPolicy(
                        tool_name="inspect",
                        required_capabilities=frozenset(
                            {"activity.tool", "artifact.refs"}
                        ),
                        policy_version="policy-v1",
                    )
                ]
            )
        )

    def _binding(self) -> FleetTaskBinding:
        projected = self._projector().project_ready(
            self.harness.scheduler,
            "run-remote",
            tenant_id="tenant-1",
            pool_id="pool-a",
        )
        self.assertEqual(len(projected), 1)
        return projected[0]

    def _compose(
        self,
        *,
        resolver=None,
        terminal_probe=None,
        max_active_assignments: int = 16,
        require_durable_ownership: bool = False,
        fleet_owner_id: str | None = None,
    ) -> tuple[RemoteFleetCoordinator, SecureRemoteFleetPoller]:
        if require_durable_ownership and fleet_owner_id is None:
            fleet_owner_id = "control-a"
        claimer = RemoteControlFleetClaimer(
            self.harness.control,
            lease_seconds=30.0,
            fleet_owner_id=fleet_owner_id,
        )
        fleet = RemoteFleetCoordinator(
            lambda: DeterministicRemoteScheduler(),
            claimer,
            require_durable_ownership=require_durable_ownership,
            fleet_owner_id=fleet_owner_id,
        )
        worker_resolver = resolver or StaticFleetWorkerResolver(
            [
                FleetWorkerPolicy(
                    worker_id="worker-1",
                    tenant_id="tenant-1",
                    pool_id="pool-a",
                    tools=frozenset({"inspect"}),
                    allowed_capabilities=frozenset(
                        {"activity.tool", "artifact.refs"}
                    ),
                    allowed_resource_keys=frozenset(
                        {"workspace:project"}
                    ),
                    max_concurrency=2,
                )
            ]
        )
        poller = SecureRemoteFleetPoller(
            fleet,
            worker_resolver,
            terminal_probe or claimer,
            max_active_assignments=max_active_assignments,
        )
        self.harness.control.bind_fleet_poller(poller)
        return fleet, poller

    def test_fleet_poll_claims_and_releases_real_durable_assignment(self) -> None:
        fleet, poller = self._compose()
        fleet.admit(self._binding())

        assignment = self.client.poll_fleet()

        self.assertIsNotNone(assignment)
        assert assignment is not None
        self.assertEqual(assignment.claim.run_id, "run-remote")
        self.assertEqual(assignment.worker_id, "worker-1")
        self.assertEqual(
            assignment.claim.session_binding_digest,
            self.harness.control.get_registration(
                "worker-1"
            ).session_binding_digest,
        )
        self.assertEqual(poller.snapshot().active_assignments, 1)
        self.assertEqual(fleet.snapshot().active_assignments, 1)
        attempt = self.harness.store.get_attempt(
            assignment.claim.attempt_id
        )
        assert attempt is not None
        self.assertEqual(
            attempt.metadata["fleet_admission"]["tenant_id"],
            "tenant-1",
        )
        self.assertEqual(
            attempt.metadata["fleet_admission"]["pool_id"],
            "pool-a",
        )
        self.client.start(assignment.claim)
        self.client.complete(
            assignment.claim,
            self.harness._success_outcome(assignment),
        )
        self.assertEqual(poller.snapshot().active_assignments, 0)
        self.assertEqual(fleet.snapshot().active_assignments, 0)
        self.assertEqual(fleet.snapshot().task_bindings, 0)

    def test_fleet_poll_is_exposed_through_authenticated_https_surface(self):
        fleet, _poller = self._compose()
        fleet.admit(self._binding())
        app = RemoteHttpASGIApp(
            self.harness.control,
            AsgiTlsPeerAuthenticator(
                _IdentityVerifier(self.harness.identity)
            ),
        )
        request = make_request(
            RemoteOperation.POLL_FLEET,
            request_id="fleet-https-poll",
            worker_id="worker-1",
            instance_id="instance-1",
            body={},
        )
        wire = json.dumps(
            request.to_wire(),
            separators=(",", ":"),
            sort_keys=True,
        ).encode()

        status, _headers, body = asyncio.run(
            http_tests._exchange(app, wire)
        )
        response = parse_response(json.loads(body))

        self.assertEqual(status, 200)
        self.assertTrue(response.ok)
        assignment = WorkAssignment.from_wire(
            response.body["assignment"]
        )
        self.assertEqual(assignment.claim.run_id, "run-remote")
        self.assertEqual(assignment.worker_id, "worker-1")

    def test_reconciler_releases_lease_reaper_terminal_projection(self) -> None:
        fleet, poller = self._compose()
        fleet.admit(self._binding())
        assignment = self.client.poll_fleet()
        assert assignment is not None
        self.harness.clock.now += 31.0
        recovery = DurableLeaseReaper(self.harness.store).run_once(
            now=self.harness.clock(),
        )
        self.assertEqual(len(recovery.resolved), 1)

        report = poller.reconcile_terminals()

        self.assertEqual(report.examined, 1)
        self.assertEqual(report.released, 1)
        self.assertEqual(report.probe_failures, 0)
        self.assertEqual(report.release_failures, 0)
        self.assertEqual(report.remaining, 0)
        self.assertEqual(fleet.snapshot().active_assignments, 0)
        self.assertFalse(report.execution_truth)

    def test_reconciler_probe_failure_retains_projection(self) -> None:
        fleet, poller = self._compose(
            terminal_probe=_ExplodingTerminalProbe()
        )
        fleet.admit(self._binding())
        assignment = self.client.poll_fleet()
        assert assignment is not None

        report = poller.reconcile_terminals()

        self.assertEqual(report.examined, 1)
        self.assertEqual(report.released, 0)
        self.assertEqual(report.probe_failures, 1)
        self.assertEqual(report.remaining, 1)
        self.assertEqual(fleet.snapshot().active_assignments, 1)

    def test_active_index_is_scoped_by_run_across_store_shards(self) -> None:
        workflow = self.harness.workflow
        schedulers = {}
        for suffix in ("a", "b"):
            counters = {}

            def shard_id(
                kind,
                *,
                _suffix=suffix,
                _counters=counters,
            ):
                _counters[kind] = _counters.get(kind, 0) + 1
                if kind == "attempt":
                    return f"attempt-{_counters[kind]}"
                return f"{kind}-{_suffix}-{_counters[kind]}"

            store = DurableRunStore(
                self.harness.control_root
                / f"fleet-shard-{suffix}.sqlite3"
            )
            scheduler = DurableScheduler(
                store,
                workflow,
                clock=self.harness.clock,
                id_factory=shard_id,
                artifact_verifier=self.harness.artifacts.verify,
            )
            run_id = f"fleet-shard-{suffix}"
            scheduler.create_run(run_id)
            scheduler.reconcile(run_id)
            schedulers[run_id] = scheduler
        control = RemoteControlPlane(
            schedulers.__getitem__,
            authorize_run=lambda identity, run_id: (
                identity.tenant_id == "tenant-1"
                and run_id in schedulers
            ),
            assignment_admitter=self.harness.admitter,
            journal=RemoteControlJournal(
                self.harness.control_root / "fleet-shards.sqlite3"
            ),
        )
        client = RemoteWorkerClient(
            lambda request: control.handle(
                self.harness.identity,
                request,
            ),
            worker_id="worker-1",
            instance_id="fleet-shards-instance",
        )
        client.register(
            runtime_version="1.0",
            capabilities=("activity.tool", "artifact.refs"),
            resource_keys=("workspace:project",),
            activity_kinds=("tool",),
            max_concurrency=2,
        )
        claimer = RemoteControlFleetClaimer(
            control,
            lease_seconds=30.0,
        )
        fleet = RemoteFleetCoordinator(
            lambda: DeterministicRemoteScheduler(),
            claimer,
        )
        poller = SecureRemoteFleetPoller(
            fleet,
            StaticFleetWorkerResolver(
                [
                    FleetWorkerPolicy(
                        worker_id="worker-1",
                        tenant_id="tenant-1",
                        pool_id="pool-a",
                        tools=frozenset({"inspect"}),
                        allowed_capabilities=frozenset(
                            {"activity.tool", "artifact.refs"}
                        ),
                        allowed_resource_keys=frozenset(
                            {"workspace:project"}
                        ),
                        max_concurrency=2,
                    )
                ]
            ),
            claimer,
        )
        control.bind_fleet_poller(poller)
        for run_id, scheduler in schedulers.items():
            for binding in self._projector().project_ready(
                scheduler,
                run_id,
                tenant_id="tenant-1",
                pool_id="pool-a",
            ):
                fleet.admit(binding)

        first = client.poll_fleet()
        second = client.poll_fleet()

        assert first is not None and second is not None
        self.assertNotEqual(first.claim.run_id, second.claim.run_id)
        self.assertEqual(
            first.claim.attempt_id,
            second.claim.attempt_id,
        )
        self.assertEqual(poller.snapshot().active_assignments, 2)
        self.harness.clock.now += 31.0
        for scheduler in schedulers.values():
            DurableLeaseReaper(scheduler.store).run_once(
                now=self.harness.clock(),
            )
        report = poller.reconcile_terminals()
        self.assertEqual(report.released, 2)
        self.assertEqual(report.remaining, 0)

    def test_exact_projection_selects_second_same_tool_node(self) -> None:
        workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": "fleet-exact-node",
                "version": 1,
                "nodes": [
                    {
                        "id": "tool-a",
                        "kind": "tool",
                        "config": {
                            "tool": "inspect",
                            "arguments": {"target": "a"},
                        },
                        "effect_class": "read_only",
                        "resource_keys": ["workspace:project"],
                    },
                    {
                        "id": "tool-b",
                        "kind": "tool",
                        "config": {
                            "tool": "inspect",
                            "arguments": {"target": "b"},
                        },
                        "effect_class": "read_only",
                        "resource_keys": ["workspace:project"],
                    },
                ],
            }
        )
        scheduler = DurableScheduler(
            self.harness.store,
            workflow,
            clock=self.harness.clock,
            artifact_verifier=self.harness.artifacts.verify,
        )
        scheduler.create_run("fleet-two-nodes")
        scheduler.reconcile("fleet-two-nodes")
        control = RemoteControlPlane(
            lambda run_id: (
                scheduler
                if run_id == "fleet-two-nodes"
                else (_ for _ in ()).throw(KeyError(run_id))
            ),
            authorize_run=lambda identity, run_id: (
                identity.tenant_id == "tenant-1"
                and run_id == "fleet-two-nodes"
            ),
            assignment_admitter=self.harness.admitter,
            journal=RemoteControlJournal(
                self.harness.control_root / "fleet-two.sqlite3"
            ),
        )
        client = RemoteWorkerClient(
            lambda request: control.handle(
                self.harness.identity,
                request,
            ),
            worker_id="worker-1",
            instance_id="fleet-two-instance",
        )
        client.register(
            runtime_version="1.0",
            capabilities=("activity.tool", "artifact.refs"),
            resource_keys=("workspace:project",),
            activity_kinds=("tool",),
            max_concurrency=2,
        )
        claimer = RemoteControlFleetClaimer(control)
        fleet = RemoteFleetCoordinator(
            lambda: DeterministicRemoteScheduler(),
            claimer,
        )
        poller = SecureRemoteFleetPoller(
            fleet,
            StaticFleetWorkerResolver(
                [
                    FleetWorkerPolicy(
                        "worker-1",
                        "tenant-1",
                        "pool-a",
                        frozenset({"inspect"}),
                        frozenset({"activity.tool", "artifact.refs"}),
                        frozenset({"workspace:project"}),
                        max_concurrency=2,
                    )
                ]
            ),
            claimer,
        )
        control.bind_fleet_poller(poller)
        projected = self._projector().project_ready(
            scheduler,
            "fleet-two-nodes",
            tenant_id="tenant-1",
            pool_id="pool-a",
        )
        self.assertEqual(
            {binding.node_id for binding in projected},
            {"tool-a", "tool-b"},
        )
        selected = next(
            binding
            for binding in projected
            if binding.node_id == "tool-b"
        )
        fleet.admit(selected)

        assignment = client.poll_fleet()

        self.assertIsNotNone(assignment)
        assert assignment is not None
        self.assertEqual(assignment.claim.node_id, "tool-b")
        attempts = self.harness.store.list_attempts("fleet-two-nodes")
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0].node_id, "tool-b")

    def test_exact_fleet_claim_does_not_require_unrelated_activity_kind(self):
        workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": "fleet-target-activity",
                "version": 1,
                "nodes": [
                    {
                        "id": "tool",
                        "kind": "tool",
                        "config": {
                            "tool": "inspect",
                            "arguments": {"target": "ready"},
                        },
                        "effect_class": "read_only",
                        "resource_keys": ["workspace:project"],
                    },
                    {
                        "id": "later-agent",
                        "kind": "agent",
                        "depends_on": ["tool"],
                        "config": {
                            "agent": "planner",
                            "task": "continue later",
                        },
                        "effect_class": "read_only",
                    },
                ],
            }
        )
        scheduler = DurableScheduler(
            self.harness.store,
            workflow,
            clock=self.harness.clock,
            artifact_verifier=self.harness.artifacts.verify,
        )
        scheduler.create_run("fleet-target-kind")
        scheduler.reconcile("fleet-target-kind")
        control = RemoteControlPlane(
            lambda run_id: (
                scheduler
                if run_id == "fleet-target-kind"
                else (_ for _ in ()).throw(KeyError(run_id))
            ),
            authorize_run=lambda identity, run_id: (
                identity.tenant_id == "tenant-1"
                and run_id == "fleet-target-kind"
            ),
            assignment_admitter=self.harness.admitter,
            journal=RemoteControlJournal(
                self.harness.control_root / "fleet-target-kind.sqlite3"
            ),
        )
        client = RemoteWorkerClient(
            lambda request: control.handle(
                self.harness.identity,
                request,
            ),
            worker_id="worker-1",
            instance_id="fleet-target-kind-instance",
        )
        client.register(
            runtime_version="1.0",
            capabilities=("activity.tool", "artifact.refs"),
            resource_keys=("workspace:project",),
            activity_kinds=("tool",),
            max_concurrency=1,
        )
        binding = self._projector().project_ready(
            scheduler,
            "fleet-target-kind",
            tenant_id="tenant-1",
            pool_id="pool-a",
        )[0]
        registration = control.get_registration("worker-1")
        assert registration is not None
        admission_scope = (
            DeterministicRemoteScheduler().durable_admission_scope(
                binding.task,
                routing_policy_digest=binding.routing_policy_digest,
            )
        )

        assignment = control.claim_for_fleet(
            binding.run_id,
            "worker-1",
            lease_seconds=30.0,
            node_id=binding.node_id,
            activity_config_digest=binding.activity_config_digest,
            expected_session_binding_digest=(
                registration.session_binding_digest
            ),
            fleet_admission=admission_scope.to_metadata(),
        )

        self.assertEqual(assignment.claim.node_id, "tool")
        self.assertEqual(assignment.activity_kind, "tool")

    def test_fleet_scope_cannot_charge_another_tenant(self) -> None:
        binding = self._binding()
        registration = self.harness.control.get_registration("worker-1")
        assert registration is not None
        scope = (
            DeterministicRemoteScheduler().durable_admission_scope(
                binding.task,
                routing_policy_digest=binding.routing_policy_digest,
            ).to_metadata()
        )
        scope["tenant_id"] = "tenant-other"

        with self.assertRaisesRegex(
            RemoteControlError,
            "claim_conflict",
        ):
            self.harness.control.claim_for_fleet(
                binding.run_id,
                "worker-1",
                lease_seconds=30.0,
                node_id=binding.node_id,
                activity_config_digest=binding.activity_config_digest,
                expected_session_binding_digest=(
                    registration.session_binding_digest
                ),
                fleet_admission=scope,
            )

        self.assertEqual(
            self.harness.store.list_attempts("run-remote"),
            [],
        )

    def test_empty_fleet_poll_has_zero_durable_mutation(self) -> None:
        fleet, poller = self._compose()
        before = self.harness.store.list_attempts("run-remote")

        assignment = self.client.poll_fleet()

        self.assertIsNone(assignment)
        self.assertEqual(
            self.harness.store.list_attempts("run-remote"),
            before,
        )
        self.assertEqual(fleet.snapshot().active_assignments, 0)
        self.assertEqual(poller.snapshot().active_assignments, 0)

    def test_unbound_fleet_operation_fails_closed(self) -> None:
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "security_not_ready",
        ):
            self.client.poll_fleet()
        self.assertEqual(
            self.harness.store.list_attempts("run-remote"),
            [],
        )

    def test_reference_binding_without_node_digest_is_not_network_ready(self):
        fleet, poller = self._compose()
        exact = self._binding()
        fleet.admit(
            FleetTaskBinding(
                run_id=exact.run_id,
                task=exact.task,
            )
        )

        self.assertFalse(fleet.production_security_ready)
        self.assertFalse(poller.production_security_ready)
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "security_not_ready",
        ):
            self.client.poll_fleet()
        self.assertEqual(
            self.harness.store.list_attempts("run-remote"),
            [],
        )

    def test_tool_routing_policy_is_complete_versioned_and_fail_closed(self):
        with self.assertRaises(RemoteFleetControlConfigurationError):
            FleetToolRoutingPolicy(
                tool_name="inspect",
                required_capabilities=frozenset({"artifact.refs"}),
                policy_version="policy-v1",
            )
        missing = DurableFleetProjector(
            StaticFleetToolPolicyResolver(
                [
                    FleetToolRoutingPolicy(
                        tool_name="different-tool",
                        required_capabilities=frozenset(
                            {"activity.tool"}
                        ),
                        policy_version="policy-v1",
                    )
                ]
            )
        )
        with self.assertRaises(RemoteFleetControlConflict):
            missing.project_ready(
                self.harness.scheduler,
                "run-remote",
                tenant_id="tenant-1",
                pool_id="pool-a",
            )

        binding = self._binding()
        self.assertRegex(binding.routing_policy_digest, r"^[0-9a-f]{64}$")
        changed = DurableFleetProjector(
            StaticFleetToolPolicyResolver(
                [
                    FleetToolRoutingPolicy(
                        tool_name="inspect",
                        required_capabilities=frozenset(
                            {"activity.tool", "artifact.refs"}
                        ),
                        policy_version="policy-v2",
                    )
                ]
            )
        ).project_ready(
            self.harness.scheduler,
            "run-remote",
            tenant_id="tenant-1",
            pool_id="pool-a",
        )[0]
        self.assertEqual(changed.task.task_id, binding.task.task_id)
        self.assertNotEqual(
            changed.routing_policy_digest,
            binding.routing_policy_digest,
        )
        fleet, _poller = self._compose()
        fleet.admit(binding)
        with self.assertRaises(RemoteFleetConflict):
            fleet.admit(changed)

    def test_exact_candidate_mismatch_has_zero_durable_mutation(self):
        fleet, poller = self._compose()
        binding = self._binding()
        fleet.admit(
            FleetTaskBinding(
                run_id=binding.run_id,
                task=binding.task,
                node_id="different-node",
                activity_config_digest=binding.activity_config_digest,
                routing_policy_digest=binding.routing_policy_digest,
            )
        )

        with self.assertRaisesRegex(
            RemoteWorkerError,
            "control_unavailable",
        ):
            self.client.poll_fleet()

        attempts = self.harness.store.list_attempts("run-remote")
        self.assertEqual(attempts, [])
        self.assertEqual(fleet.snapshot().active_assignments, 0)
        self.assertEqual(fleet.snapshot().task_bindings, 0)
        self.assertEqual(poller.snapshot().active_assignments, 0)

    def test_fleet_body_cannot_choose_run_or_lease(self) -> None:
        for body in (
            {"run_id": "run-other"},
            {"lease_seconds": 3_600},
        ):
            with self.subTest(body=body):
                with self.assertRaises(RemoteProtocolError):
                    make_request(
                        RemoteOperation.POLL_FLEET,
                        request_id="fleet-invalid",
                        worker_id="worker-1",
                        instance_id="instance-1",
                        body=body,
                    )

    def test_server_policy_is_exact_and_cross_tenant_is_denied(self) -> None:
        with self.assertRaises(RemoteFleetControlConfigurationError):
            FleetWorkerPolicy(
                "worker-1",
                "tenant-1",
                "pool-a",
                frozenset({"inspect"}),
                frozenset({"artifact.refs"}),
            )
        with self.assertRaises(RemoteFleetControlConfigurationError):
            StaticFleetWorkerResolver(
                [
                    FleetWorkerPolicy(
                        "worker-1",
                        "tenant-1",
                        "pool-a",
                        frozenset({"inspect"}),
                        frozenset({"activity.tool", "artifact.refs"}),
                    ),
                    FleetWorkerPolicy(
                        "worker-1",
                        "tenant-2",
                        "pool-b",
                        frozenset({"inspect"}),
                        frozenset({"activity.tool", "artifact.refs"}),
                    ),
                ]
            )
        resolver = StaticFleetWorkerResolver(
            [
                FleetWorkerPolicy(
                    "worker-1",
                    "tenant-2",
                    "pool-a",
                    frozenset({"inspect"}),
                    frozenset({"activity.tool", "artifact.refs"}),
                )
            ]
        )
        registration = self.harness.control.get_registration("worker-1")
        assert registration is not None
        with self.assertRaises(RemoteFleetControlConflict):
            resolver.resolve(self.harness.identity, registration)

    def test_server_policy_intersects_untrusted_worker_claims(self) -> None:
        resolver = StaticFleetWorkerResolver(
            [
                FleetWorkerPolicy(
                    "worker-1",
                    "tenant-1",
                    "pool-a",
                    frozenset({"inspect"}),
                    frozenset({"activity.tool"}),
                    frozenset({"workspace:project"}),
                    max_concurrency=1,
                )
            ]
        )
        registration = self.harness.control.get_registration("worker-1")
        assert registration is not None
        overclaimed = replace(
            registration,
            capabilities=(
                *registration.capabilities,
                "host.root",
            ),
            resource_keys=(
                *registration.resource_keys,
                "host:/root",
            ),
            max_concurrency=256,
        )

        descriptor = resolver.resolve(self.harness.identity, overclaimed)

        self.assertEqual(
            descriptor.capabilities,
            frozenset({"activity.tool"}),
        )
        self.assertEqual(
            descriptor.resource_keys,
            frozenset({"workspace:project"}),
        )
        self.assertEqual(descriptor.capacity, 1)

    def test_server_resource_policy_prevents_misrouting_and_task_loss(self):
        resolver = StaticFleetWorkerResolver(
            [
                FleetWorkerPolicy(
                    "worker-1",
                    "tenant-1",
                    "pool-a",
                    frozenset({"inspect"}),
                    frozenset({"activity.tool", "artifact.refs"}),
                    frozenset(),
                    max_concurrency=2,
                )
            ]
        )
        fleet, poller = self._compose(resolver=resolver)
        fleet.admit(self._binding())

        assignment = self.client.poll_fleet()

        self.assertIsNone(assignment)
        self.assertEqual(
            self.harness.store.list_attempts("run-remote"),
            [],
        )
        self.assertEqual(fleet.snapshot().queued_tasks, 1)
        self.assertEqual(fleet.snapshot().task_bindings, 1)
        self.assertEqual(poller.snapshot().active_assignments, 0)

    def test_resolver_cannot_override_authenticated_session(self) -> None:
        fleet, poller = self._compose(resolver=_WrongSessionResolver())
        fleet.admit(self._binding())

        with self.assertRaisesRegex(
            RemoteWorkerError,
            "control_unavailable",
        ):
            self.client.poll_fleet()

        self.assertEqual(
            self.harness.store.list_attempts("run-remote"),
            [],
        )
        self.assertEqual(poller.snapshot().active_assignments, 0)

    def test_claimer_rechecks_exact_session_before_store_mutation(self) -> None:
        binding = self._binding()
        claimer = RemoteControlFleetClaimer(self.harness.control)
        admission_scope = (
            DeterministicRemoteScheduler().durable_admission_scope(
                binding.task,
                routing_policy_digest=binding.routing_policy_digest,
            )
        )

        with self.assertRaisesRegex(
            RemoteControlError,
            "worker_identity_mismatch",
        ):
            claimer(
                binding,
                "worker-1",
                "f" * 64,
                admission_scope,
            )

        self.assertEqual(
            self.harness.store.list_attempts("run-remote"),
            [],
        )

    def test_strict_multi_control_claim_persists_shard_fencing(self) -> None:
        ownership = self.harness.store.claim_fleet_shard(
            "pool-a-shard",
            "control-a",
            "a" * 64,
            pool_id="pool-a",
            now=10,
        )
        binding = self._projector().project_ready(
            self.harness.scheduler,
            "run-remote",
            tenant_id="tenant-1",
            pool_id="pool-a",
            shard_ownership=ownership,
        )[0]
        fleet, _poller = self._compose(
            require_durable_ownership=True,
        )
        cursor = self.harness.store.get_fleet_fairness_cursor(
            "pool-a"
        )
        assert cursor is not None
        fleet.restore_fairness_cursor(cursor)
        fleet.admit(binding)

        self.assertTrue(fleet.production_multi_control_ready)
        assignment = self.client.poll_fleet()
        assert assignment is not None
        attempt = self.harness.store.get_attempt(
            assignment.claim.attempt_id
        )
        assert attempt is not None
        self.assertEqual(
            attempt.metadata["fleet_shard_ownership"],
            ownership.to_metadata(),
        )
        advanced = self.harness.store.get_fleet_fairness_cursor(
            "pool-a"
        )
        assert advanced is not None
        self.assertEqual(advanced.last_served_tenant, "tenant-1")
        self.assertEqual(advanced.selection_sequence, 1)

    def test_shard_transfer_fences_claimed_but_not_started_work(
        self,
    ) -> None:
        ownership = self.harness.store.claim_fleet_shard(
            "claim-start-shard",
            "control-a",
            "a" * 64,
            pool_id="pool-a",
            now=10,
        )
        binding = self._projector().project_ready(
            self.harness.scheduler,
            "run-remote",
            tenant_id="tenant-1",
            pool_id="pool-a",
            shard_ownership=ownership,
        )[0]
        fleet, _poller = self._compose(
            require_durable_ownership=True,
        )
        cursor = self.harness.store.get_fleet_fairness_cursor(
            "pool-a"
        )
        assert cursor is not None
        fleet.restore_fairness_cursor(cursor)
        fleet.admit(binding)
        assignment = self.client.poll_fleet()
        assert assignment is not None
        self.harness.store.transfer_fleet_shard(
            ownership,
            new_owner_id="control-b",
            new_policy_digest="a" * 64,
            now=9,
        )

        with self.assertRaises(RemoteWorkerError):
            self.client.start(assignment.claim)
        attempt = self.harness.store.get_attempt(
            assignment.claim.attempt_id
        )
        assert attempt is not None
        self.assertEqual(attempt.status.value, "claimed")

    def test_running_work_can_finish_after_shard_transfer(self) -> None:
        ownership = self.harness.store.claim_fleet_shard(
            "running-transfer-shard",
            "control-a",
            "a" * 64,
            pool_id="pool-a",
            now=10,
        )
        binding = self._projector().project_ready(
            self.harness.scheduler,
            "run-remote",
            tenant_id="tenant-1",
            pool_id="pool-a",
            shard_ownership=ownership,
        )[0]
        fleet, _poller = self._compose(
            require_durable_ownership=True,
        )
        cursor = self.harness.store.get_fleet_fairness_cursor(
            "pool-a"
        )
        assert cursor is not None
        fleet.restore_fairness_cursor(cursor)
        fleet.admit(binding)
        assignment = self.client.poll_fleet()
        assert assignment is not None
        self.client.start(assignment.claim)
        self.harness.store.transfer_fleet_shard(
            ownership,
            new_owner_id="control-b",
            new_policy_digest="a" * 64,
            now=9,
        )

        self.client.complete(
            assignment.claim,
            self.harness._success_outcome(assignment),
        )

        attempt = self.harness.store.get_attempt(
            assignment.claim.attempt_id
        )
        assert attempt is not None
        self.assertEqual(attempt.status.value, "succeeded")

    def test_projector_rejects_ownership_for_another_scope(self) -> None:
        ownership = self.harness.store.claim_fleet_shard(
            "other-scope-shard",
            "control-a",
            "a" * 64,
            pool_id="pool-other",
            now=10,
        )

        with self.assertRaisesRegex(
            RemoteFleetControlConflict,
            "another pool",
        ):
            self._projector().project_ready(
                self.harness.scheduler,
                "run-remote",
                tenant_id="tenant-1",
                pool_id="pool-a",
                shard_ownership=ownership,
            )

    def test_store_rejects_valid_ownership_for_another_scope(self) -> None:
        binding = self._binding()
        ownership = self.harness.store.claim_fleet_shard(
            "other-tenant-shard",
            "control-a",
            "a" * 64,
            pool_id="pool-other",
            now=10,
        )
        registration = self.harness.control.get_registration("worker-1")
        assert registration is not None
        scope = DeterministicRemoteScheduler().durable_admission_scope(
            binding.task,
            routing_policy_digest=binding.routing_policy_digest,
        )

        with self.assertRaisesRegex(
            RemoteControlError,
            "claim_conflict",
        ):
            self.harness.control.claim_for_fleet(
                binding.run_id,
                "worker-1",
                lease_seconds=30.0,
                node_id=binding.node_id,
                activity_config_digest=binding.activity_config_digest,
                expected_session_binding_digest=(
                    registration.session_binding_digest
                ),
                fleet_admission=scope.to_metadata(),
                fleet_shard_ownership=ownership.to_metadata(),
            )

        self.assertEqual(
            self.harness.store.list_attempts("run-remote"),
            [],
        )

    def test_shard_transfer_fences_stale_queue_then_new_epoch_claims(
        self,
    ) -> None:
        first = self.harness.store.claim_fleet_shard(
            "pool-a-failover",
            "control-a",
            "b" * 64,
            pool_id="pool-a",
            now=10,
        )
        old_binding = self._projector().project_ready(
            self.harness.scheduler,
            "run-remote",
            tenant_id="tenant-1",
            pool_id="pool-a",
            shard_ownership=first,
        )[0]
        fleet, _poller = self._compose(
            require_durable_ownership=True,
        )
        cursor = self.harness.store.get_fleet_fairness_cursor(
            "pool-a"
        )
        assert cursor is not None
        fleet.restore_fairness_cursor(cursor)
        fleet.admit(old_binding)
        second = self.harness.store.transfer_fleet_shard(
            first,
            new_owner_id="control-a",
            new_policy_digest="b" * 64,
            now=11,
        )
        transferred_cursor = (
            self.harness.store.get_fleet_fairness_cursor("pool-a")
        )
        assert transferred_cursor is not None
        with self.assertRaisesRegex(
            RemoteFleetConflict,
            "must be idle",
        ):
            fleet.restore_fairness_cursor(transferred_cursor)

        with self.assertRaisesRegex(
            RemoteWorkerError,
            "control_unavailable",
        ):
            self.client.poll_fleet()
        self.assertEqual(
            self.harness.store.list_attempts("run-remote"),
            [],
        )
        with self.assertRaisesRegex(
            RemoteFleetControlConflict,
            "ownership is stale",
        ):
            self._projector().project_ready(
                self.harness.scheduler,
                "run-remote",
                tenant_id="tenant-1",
                pool_id="pool-a",
                shard_ownership=first,
            )

        new_binding = self._projector().project_ready(
            self.harness.scheduler,
            "run-remote",
            tenant_id="tenant-1",
            pool_id="pool-a",
            shard_ownership=second,
        )[0]
        next_cursor = self.harness.store.get_fleet_fairness_cursor(
            "pool-a"
        )
        assert next_cursor is not None
        self.assertIsNone(next_cursor.last_served_tenant)
        self.assertEqual(next_cursor.selection_sequence, 0)
        fleet.restore_fairness_cursor(next_cursor)
        fleet.admit(new_binding)
        assignment = self.client.poll_fleet()
        assert assignment is not None
        stored = self.harness.store.get_attempt(
            assignment.claim.attempt_id
        )
        assert stored is not None
        self.assertEqual(
            stored.metadata["fleet_shard_ownership"],
            second.to_metadata(),
        )

    def test_strict_control_rejects_another_owners_binding(self) -> None:
        ownership = self.harness.store.claim_fleet_shard(
            "control-b-shard",
            "control-b",
            "b" * 64,
            pool_id="pool-a",
            now=10,
        )
        binding = self._projector().project_ready(
            self.harness.scheduler,
            "run-remote",
            tenant_id="tenant-1",
            pool_id="pool-a",
            shard_ownership=ownership,
        )[0]
        fleet, _poller = self._compose(
            require_durable_ownership=True,
            fleet_owner_id="control-a",
        )
        fleet.admit(binding)

        self.assertFalse(fleet.production_security_ready)
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "security_not_ready",
        ):
            self.client.poll_fleet()
        self.assertEqual(
            self.harness.store.list_attempts("run-remote"),
            [],
        )

    def test_strict_control_requires_an_explicit_owner_identity(self) -> None:
        claimer = RemoteControlFleetClaimer(self.harness.control)

        with self.assertRaisesRegex(
            ValueError,
            "requires fleet_owner_id",
        ):
            RemoteFleetCoordinator(
                lambda: DeterministicRemoteScheduler(),
                claimer,
                require_durable_ownership=True,
            )

    def test_strict_multi_control_readiness_requires_durable_cursor(
        self,
    ) -> None:
        ownership = self.harness.store.claim_fleet_shard(
            "cursor-required-shard",
            "control-a",
            "b" * 64,
            pool_id="pool-a",
            now=10,
        )
        binding = self._projector().project_ready(
            self.harness.scheduler,
            "run-remote",
            tenant_id="tenant-1",
            pool_id="pool-a",
            shard_ownership=ownership,
        )[0]
        fleet, _poller = self._compose(
            require_durable_ownership=True,
        )
        fleet.admit(binding)

        self.assertFalse(fleet.production_multi_control_ready)
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "security_not_ready",
        ):
            self.client.poll_fleet()

    def test_strict_rebuild_restores_matching_pool_cursor_atomically(
        self,
    ) -> None:
        ownership = self.harness.store.claim_fleet_shard(
            "rebuild-cursor-shard",
            "control-a",
            "b" * 64,
            pool_id="pool-a",
            now=10,
        )
        binding = self._projector().project_ready(
            self.harness.scheduler,
            "run-remote",
            tenant_id="tenant-1",
            pool_id="pool-a",
            shard_ownership=ownership,
        )[0]
        cursor = self.harness.store.get_fleet_fairness_cursor(
            "pool-a"
        )
        assert cursor is not None
        fleet, _poller = self._compose(
            require_durable_ownership=True,
        )

        report = fleet.rebuild(
            [binding],
            [],
            fairness_cursors=[cursor],
        )

        self.assertEqual((report.tasks, report.workers), (1, 0))
        self.assertTrue(fleet.production_multi_control_ready)
        self.assertEqual(fleet.snapshot().queued_tasks, 1)

    def test_strict_rebuild_rejects_stale_cursor_before_state_swap(
        self,
    ) -> None:
        first = self.harness.store.claim_fleet_shard(
            "stale-rebuild-shard",
            "control-a",
            "b" * 64,
            pool_id="pool-a",
            now=10,
        )
        stale_cursor = self.harness.store.get_fleet_fairness_cursor(
            "pool-a"
        )
        assert stale_cursor is not None
        current = self.harness.store.transfer_fleet_shard(
            first,
            new_owner_id="control-a",
            new_policy_digest="c" * 64,
            now=11,
        )
        binding = self._projector().project_ready(
            self.harness.scheduler,
            "run-remote",
            tenant_id="tenant-1",
            pool_id="pool-a",
            shard_ownership=current,
        )[0]
        fleet, _poller = self._compose(
            require_durable_ownership=True,
        )

        with self.assertRaisesRegex(
            RemoteFleetConflict,
            "stale pool cursor",
        ):
            fleet.rebuild(
                [binding],
                [],
                fairness_cursors=[stale_cursor],
            )

        self.assertEqual(fleet.snapshot().queued_tasks, 0)
        self.assertEqual(fleet.snapshot().task_bindings, 0)

    def test_owned_scope_blocks_legacy_control_claims(self) -> None:
        self.harness.store.claim_fleet_shard(
            "upgraded-shard",
            "control-a",
            "b" * 64,
            pool_id="pool-a",
            now=10,
        )
        fleet, _poller = self._compose()
        fleet.admit(self._binding())

        with self.assertRaisesRegex(
            RemoteWorkerError,
            "control_unavailable",
        ):
            self.client.poll_fleet()
        self.assertEqual(
            self.harness.store.list_attempts("run-remote"),
            [],
        )

    def test_strict_multi_control_readiness_rejects_unowned_binding(
        self,
    ) -> None:
        fleet, _poller = self._compose(
            require_durable_ownership=True,
        )
        fleet.admit(self._binding())

        self.assertFalse(fleet.production_security_ready)
        self.assertFalse(fleet.production_multi_control_ready)
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "security_not_ready",
        ):
            self.client.poll_fleet()
        self.assertEqual(
            self.harness.store.list_attempts("run-remote"),
            [],
        )

    def test_non_production_claim_callback_cannot_form_secure_poller(self):
        fleet = RemoteFleetCoordinator(
            lambda: DeterministicRemoteScheduler(),
            _NonProductionClaimer(),
        )
        resolver = StaticFleetWorkerResolver(
            [
                FleetWorkerPolicy(
                    "worker-1",
                    "tenant-1",
                    "pool-a",
                    frozenset({"inspect"}),
                    frozenset({"activity.tool", "artifact.refs"}),
                )
            ]
        )

        with self.assertRaises(RemoteFleetControlConfigurationError):
            SecureRemoteFleetPoller(
                fleet,
                resolver,
                _NonProductionClaimer(),
            )

    def test_legacy_production_callback_without_durable_quota_is_rejected(
        self,
    ) -> None:
        fleet = RemoteFleetCoordinator(
            lambda: DeterministicRemoteScheduler(),
            _LegacyProductionClaimer(),
        )

        self.assertFalse(fleet.production_security_ready)

    def test_fleet_poller_binding_is_one_time_and_readiness_is_explicit(self):
        _fleet, poller = self._compose()

        self.assertTrue(self.harness.control.production_fleet_ready)
        with self.assertRaisesRegex(
            RemoteControlError,
            "already_registered",
        ):
            self.harness.control.bind_fleet_poller(poller)

    def test_active_registry_capacity_fails_before_second_durable_claim(self):
        fleet, poller = self._compose(max_active_assignments=1)
        fleet.admit(self._binding())
        first = self.client.poll_fleet()
        self.assertIsNotNone(first)
        attempts_after_first = self.harness.store.list_attempts("run-remote")

        with self.assertRaisesRegex(
            RemoteWorkerError,
            "control_unavailable",
        ):
            self.client.poll_fleet()

        self.assertEqual(
            self.harness.store.list_attempts("run-remote"),
            attempts_after_first,
        )
        self.assertEqual(poller.snapshot().active_assignments, 1)

    def test_terminal_release_rejects_forged_session_without_mutating_fleet(self):
        fleet, poller = self._compose()
        fleet.admit(self._binding())
        assignment = self.client.poll_fleet()
        assert assignment is not None
        registration = self.harness.control.get_registration("worker-1")
        assert registration is not None
        attacker = AuthenticatedWorker(
            "worker-1",
            "tenant-1",
            "f" * 64,
        )

        with self.assertRaises(RemoteFleetControlConflict):
            poller.release_terminal(
                attacker,
                registration,
                assignment.claim,
            )

        self.assertEqual(poller.snapshot().active_assignments, 1)
        self.assertEqual(fleet.snapshot().active_assignments, 1)


if __name__ == "__main__":
    unittest.main()
