from __future__ import annotations

import os
import sqlite3
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from unittest import mock

from src.orchestration.lease import DurableLeaseReaper
from src.orchestration.remote_fleet import RemoteFleetCoordinator
from src.orchestration.remote_fleet_control import (
    DurableFleetProjector,
    FleetToolRoutingPolicy,
    FleetWorkerPolicy,
    RemoteControlFleetClaimer,
    SecureRemoteFleetPoller,
    StaticFleetToolPolicyResolver,
    StaticFleetWorkerResolver,
)
from src.orchestration.remote_fleet_reconcile import (
    DurableFleetReconciler,
    DurableStoreFleetRunSource,
    FleetRunRoute,
    FleetRunSource,
    StaticFleetRunSource,
)
from src.orchestration.remote_scheduling import (
    DeterministicRemoteScheduler,
)
from src.orchestration.scheduler import DurableScheduler
from src.orchestration.store import DurableRunStore
from src.orchestration.remote_worker import RemoteWorkerError

from tests import test_orchestration_remote_protocol as protocol_tests


class _UnsafeRunSource:
    production_security_ready = False

    def snapshot(self, _limit):
        return ()


class _RevocableRunSource:
    def __init__(self, routes):
        self.routes = tuple(routes)
        self.production_security_ready = True

    def snapshot(self, limit):
        return self.routes[:limit]


class DurableFleetReconcilerTests(unittest.TestCase):
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
        self.run_route = self.harness.store.register_fleet_run_route(
            "run-remote",
            "tenant-1",
            "pool-a",
            now=1,
        )

    @staticmethod
    def _projector(
        policy_version: str = "policy-v1",
    ) -> tuple[DurableFleetProjector, FleetToolRoutingPolicy]:
        policy = FleetToolRoutingPolicy(
            tool_name="inspect",
            required_capabilities=frozenset(
                {"activity.tool", "artifact.refs"}
            ),
            policy_version=policy_version,
        )
        return (
            DurableFleetProjector(
                StaticFleetToolPolicyResolver([policy])
            ),
            policy,
        )

    def _compose(
        self,
        *,
        strict: bool = False,
        policy_version: str = "policy-v1",
        run_source: FleetRunSource | None = None,
        max_routes: int = 16,
    ):
        owner_id = "control-a" if strict else None
        claimer = RemoteControlFleetClaimer(
            self.harness.control,
            lease_seconds=30.0,
            fleet_owner_id=owner_id,
        )
        fleet = RemoteFleetCoordinator(
            lambda: DeterministicRemoteScheduler(),
            claimer,
            require_durable_ownership=strict,
            fleet_owner_id=owner_id,
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
        self.harness.control.bind_fleet_poller(poller)
        projector, policy = self._projector(policy_version)
        source = run_source or StaticFleetRunSource(
            [
                FleetRunRoute(
                    "run-remote",
                    "tenant-1",
                    "pool-a",
                    self.run_route,
                    self.harness.store.path,
                )
            ]
        )
        reconciler = DurableFleetReconciler(
            fleet,
            poller,
            projector,
            source,
            lambda run_id: self.harness.scheduler
            if run_id == "run-remote"
            else (_ for _ in ()).throw(KeyError(run_id)),
            max_routes=max_routes,
            allow_reference_source=True,
        )
        return fleet, poller, reconciler, source, policy

    def test_run_source_converges_queue_idempotently(self) -> None:
        fleet, _poller, reconciler, _source, _policy = (
            self._compose()
        )

        first = reconciler.run_once()
        repeated = reconciler.run_once()

        self.assertEqual(first.routes_examined, 1)
        self.assertEqual(first.schedulers_resolved, 1)
        self.assertEqual(first.desired_tasks, 1)
        self.assertEqual(first.queue.admitted_tasks, 1)
        self.assertEqual(repeated.queue.retained_tasks, 1)
        self.assertEqual(fleet.snapshot().queued_tasks, 1)
        self.assertFalse(first.execution_truth)

    def test_durable_store_source_recovers_registered_running_routes(
        self,
    ) -> None:
        source = DurableStoreFleetRunSource(
            [self.harness.store],
            control_plane_root=self.harness.control_root,
            agent_roots=(self.harness.agent_root,),
        )

        first = source.snapshot(2)
        restarted = DurableStoreFleetRunSource(
            [DurableRunStore(self.harness.store.path)],
            control_plane_root=self.harness.control_root,
            agent_roots=(self.harness.agent_root,),
        ).snapshot(2)

        self.assertTrue(source.production_security_ready)
        self.assertEqual(first, restarted)
        self.assertEqual(
            first,
            (
                FleetRunRoute(
                    "run-remote",
                    "tenant-1",
                    "pool-a",
                    self.run_route,
                    self.harness.store.path,
                ),
            ),
        )

    def test_route_withdrawal_fences_stale_queue_before_reconcile(
        self,
    ) -> None:
        source = DurableStoreFleetRunSource(
            [self.harness.store],
            control_plane_root=self.harness.control_root,
            agent_roots=(self.harness.agent_root,),
        )
        fleet, _poller, reconciler, _source, _policy = self._compose(
            run_source=source
        )
        reconciler.run_once()
        disabled = self.harness.store.withdraw_fleet_run_route(
            self.run_route,
            now=2,
        )

        with self.assertRaisesRegex(
            RemoteWorkerError,
            "control_unavailable",
        ):
            self.client.poll_fleet()

        self.assertEqual(
            self.harness.store.list_attempts("run-remote"),
            [],
        )
        report = reconciler.run_once()
        self.assertEqual(report.routes_examined, 0)
        self.assertEqual(report.queue.task_bindings, 0)

        enabled = self.harness.store.register_fleet_run_route(
            "run-remote",
            "tenant-1",
            "pool-a",
            expected=disabled,
            now=3,
        )
        report = reconciler.run_once()
        self.assertEqual(report.queue.admitted_tasks, 1)
        assignment = self.client.poll_fleet()
        assert assignment is not None
        attempt = self.harness.store.get_attempt(
            assignment.claim.attempt_id
        )
        assert attempt is not None
        self.assertEqual(
            attempt.metadata["fleet_admission"][
                "run_route_digest"
            ],
            enabled.route_digest,
        )

    def test_route_disable_enable_aba_replaces_queued_binding(
        self,
    ) -> None:
        source = DurableStoreFleetRunSource(
            [self.harness.store],
            control_plane_root=self.harness.control_root,
            agent_roots=(self.harness.agent_root,),
        )
        fleet, _poller, reconciler, _source, _policy = self._compose(
            run_source=source
        )
        reconciler.run_once()
        disabled = self.harness.store.withdraw_fleet_run_route(
            self.run_route,
            now=2,
        )
        enabled = self.harness.store.register_fleet_run_route(
            "run-remote",
            "tenant-1",
            "pool-a",
            expected=disabled,
            now=3,
        )

        report = reconciler.run_once()
        assignment = self.client.poll_fleet()

        self.assertEqual(report.queue.withdrawn_tasks, 1)
        self.assertEqual(report.queue.admitted_tasks, 1)
        assert assignment is not None
        attempt = self.harness.store.get_attempt(
            assignment.claim.attempt_id
        )
        assert attempt is not None
        self.assertEqual(
            attempt.metadata["fleet_admission"][
                "run_route_digest"
            ],
            enabled.route_digest,
        )
        self.assertNotEqual(
            enabled.route_digest,
            self.run_route.route_digest,
        )

    def test_static_run_source_requires_explicit_reference_opt_in(
        self,
    ) -> None:
        fleet, poller, _reconciler, source, _policy = self._compose()
        projector, _policy = self._projector()

        with self.assertRaisesRegex(
            ValueError,
            "not production ready",
        ):
            DurableFleetReconciler(
                fleet,
                poller,
                projector,
                source,
                lambda _run_id: self.harness.scheduler,
            )

    def test_durable_source_rejects_duplicate_store_registration(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "duplicate Store",
        ):
            DurableStoreFleetRunSource(
                [
                    self.harness.store,
                    DurableRunStore(self.harness.store.path),
                ],
                control_plane_root=self.harness.control_root,
                agent_roots=(self.harness.agent_root,),
            )

    def test_durable_source_rejects_physical_store_alias(
        self,
    ) -> None:
        with sqlite3.connect(self.harness.store.path) as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        alias_path = self.harness.control_root / "runs-alias.sqlite3"
        try:
            os.link(self.harness.store.path, alias_path)
        except OSError as exc:
            self.skipTest(f"hard links unavailable: {type(exc).__name__}")
        alias_store = DurableRunStore(alias_path)

        with self.assertRaisesRegex(
            ValueError,
            "physical alias",
        ):
            DurableStoreFleetRunSource(
                [self.harness.store, alias_store],
                control_plane_root=self.harness.control_root,
                agent_roots=(self.harness.agent_root,),
            )

    def test_durable_source_requires_non_overlapping_control_storage(
        self,
    ) -> None:
        outside_store = DurableRunStore(
            self.harness.root / "outside.sqlite3"
        )
        with self.assertRaisesRegex(
            ValueError,
            "outside the control-plane root",
        ):
            DurableStoreFleetRunSource(
                [outside_store],
                control_plane_root=self.harness.control_root,
                agent_roots=(self.harness.agent_root,),
            )
        with self.assertRaisesRegex(
            ValueError,
            "overlaps an Agent root",
        ):
            DurableStoreFleetRunSource(
                [self.harness.store],
                control_plane_root=self.harness.control_root,
                agent_roots=(self.harness.root,),
            )

    def test_durable_source_detects_control_path_replacement(
        self,
    ) -> None:
        source = DurableStoreFleetRunSource(
            [self.harness.store],
            control_plane_root=self.harness.control_root,
            agent_roots=(self.harness.agent_root,),
        )
        moved = self.harness.root / "old-control"
        self.harness.control_root.rename(moved)
        self.harness.control_root.mkdir()

        self.assertFalse(source.production_security_ready)
        with self.assertRaisesRegex(
            RuntimeError,
            "isolation changed",
        ):
            source.snapshot(2)

    def test_durable_source_rejects_run_registered_in_two_stores(
        self,
    ) -> None:
        second_store = DurableRunStore(
            self.harness.control_root / "duplicate-runs.sqlite3"
        )
        second = DurableScheduler(
            second_store,
            self.harness.workflow,
            clock=self.harness.clock,
            artifact_verifier=self.harness.artifacts.verify,
        )
        second.create_run("run-remote")
        second.reconcile("run-remote")
        second_store.register_fleet_run_route(
            "run-remote",
            "tenant-1",
            "pool-a",
            now=1,
        )
        source = DurableStoreFleetRunSource(
            [self.harness.store, second_store],
            control_plane_root=self.harness.control_root,
            agent_roots=(self.harness.agent_root,),
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "multiple Stores",
        ):
            source.snapshot(3)

    def test_durable_route_cannot_resolve_to_a_cloned_store(
        self,
    ) -> None:
        second_store = DurableRunStore(
            self.harness.control_root / "cloned-runs.sqlite3"
        )
        second = DurableScheduler(
            second_store,
            self.harness.workflow,
            clock=self.harness.clock,
            artifact_verifier=self.harness.artifacts.verify,
        )
        second.create_run("run-remote")
        second.reconcile("run-remote")
        cloned = second_store.register_fleet_run_route(
            "run-remote",
            "tenant-1",
            "pool-a",
            now=1,
        )
        self.assertEqual(cloned, self.run_route)
        source = DurableStoreFleetRunSource(
            [self.harness.store],
            control_plane_root=self.harness.control_root,
            agent_roots=(self.harness.agent_root,),
        )
        fleet, poller, _unused, _source, _policy = self._compose(
            run_source=source
        )
        projector, _policy = self._projector()
        reconciler = DurableFleetReconciler(
            fleet,
            poller,
            projector,
            source,
            lambda _run_id: second,
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "different Store",
        ):
            reconciler.run_once()

        self.assertEqual(fleet.snapshot().queued_tasks, 0)

    def test_corrupt_durable_route_quarantines_existing_queue(
        self,
    ) -> None:
        source = DurableStoreFleetRunSource(
            [self.harness.store],
            control_plane_root=self.harness.control_root,
            agent_roots=(self.harness.agent_root,),
        )
        fleet, _poller, reconciler, _source, _policy = self._compose(
            run_source=source
        )
        reconciler.run_once()
        with sqlite3.connect(self.harness.store.path) as conn:
            conn.execute(
                """
                UPDATE fleet_run_routes
                SET route_digest = ?
                WHERE run_id = ?
                """,
                ("z" * 64, "run-remote"),
            )

        with self.assertRaisesRegex(
            RuntimeError,
            "snapshot failed",
        ):
            reconciler.run_once()

        self.assertEqual(fleet.snapshot().queued_tasks, 0)
        self.assertEqual(fleet.snapshot().task_bindings, 0)

    def test_concurrent_control_rounds_converge_to_one_binding(
        self,
    ) -> None:
        fleet, _poller, reconciler, _source, _policy = (
            self._compose()
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            reports = list(
                executor.map(
                    lambda _index: reconciler.run_once(),
                    range(2),
                )
            )

        self.assertEqual(fleet.snapshot().queued_tasks, 1)
        self.assertEqual(fleet.snapshot().task_bindings, 1)
        self.assertEqual(
            sum(report.queue.admitted_tasks for report in reports),
            1,
        )
        self.assertEqual(
            sum(report.queue.retained_tasks for report in reports),
            1,
        )

    def test_policy_change_withdraws_old_queue_before_replacement(
        self,
    ) -> None:
        fleet, poller, first, source, _old_policy = self._compose()
        first.run_once()
        projector, current_policy = self._projector("policy-v2")
        changed = DurableFleetReconciler(
            fleet,
            poller,
            projector,
            source,
            lambda _run_id: self.harness.scheduler,
            allow_reference_source=True,
        )

        report = changed.run_once()
        assignment = self.client.poll_fleet()

        self.assertEqual(report.queue.withdrawn_tasks, 1)
        self.assertEqual(report.queue.admitted_tasks, 1)
        assert assignment is not None
        attempt = self.harness.store.get_attempt(
            assignment.claim.attempt_id
        )
        assert attempt is not None
        self.assertEqual(
            attempt.metadata["fleet_admission"][
                "routing_policy_digest"
            ],
            current_policy.policy_digest,
        )

    def test_run_exit_withdraws_queued_projection(self) -> None:
        fleet, _poller, reconciler, _source, _policy = (
            self._compose()
        )
        reconciler.run_once()
        self.harness.scheduler.request_cancel(
            "run-remote",
            reconcile=False,
        )

        report = reconciler.run_once()

        self.assertEqual(report.desired_tasks, 0)
        self.assertEqual(report.queue.withdrawn_tasks, 1)
        self.assertEqual(fleet.snapshot().queued_tasks, 0)
        self.assertEqual(fleet.snapshot().task_bindings, 0)

    def test_terminal_probe_and_retry_projection_converge_together(
        self,
    ) -> None:
        fleet, _poller, reconciler, _source, _policy = (
            self._compose()
        )
        reconciler.run_once()
        assignment = self.client.poll_fleet()
        assert assignment is not None
        self.harness.clock.now += 31.0
        DurableLeaseReaper(self.harness.store).run_once(
            now=self.harness.clock(),
        )

        report = reconciler.run_once()

        self.assertEqual(report.terminal.released, 1)
        self.assertEqual(report.terminal.remaining, 0)
        self.assertEqual(fleet.snapshot().active_assignments, 0)
        self.assertEqual(report.queue.deferred_active_tasks, 0)

    def test_strict_bootstrap_restores_cursor_once(self) -> None:
        self.harness.store.claim_fleet_shard(
            "pool-a-shard",
            "control-a",
            "a" * 64,
            pool_id="pool-a",
            now=10,
        )
        fleet, _poller, reconciler, _source, _policy = (
            self._compose(strict=True)
        )

        first = reconciler.run_once()
        repeated = reconciler.run_once()

        self.assertEqual(first.restored_pools, 1)
        self.assertEqual(repeated.restored_pools, 0)
        self.assertTrue(fleet.production_multi_control_ready)
        self.assertEqual(repeated.queue.retained_tasks, 1)

    def test_authority_change_quarantines_old_queue_then_retries(
        self,
    ) -> None:
        first = self.harness.store.claim_fleet_shard(
            "pool-a-shard",
            "control-a",
            "a" * 64,
            pool_id="pool-a",
            now=10,
        )
        fleet, _poller, reconciler, _source, _policy = (
            self._compose(strict=True)
        )
        reconciler.run_once()
        self.harness.store.transfer_fleet_shard(
            first,
            new_owner_id="control-a",
            new_policy_digest="b" * 64,
            now=11,
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "must be idle",
        ):
            reconciler.run_once()

        self.assertEqual(fleet.snapshot().queued_tasks, 0)
        self.assertEqual(fleet.snapshot().task_bindings, 0)
        recovered = reconciler.run_once()
        self.assertEqual(recovered.restored_pools, 1)
        self.assertEqual(recovered.queue.admitted_tasks, 1)
        self.assertTrue(fleet.production_multi_control_ready)

    def test_source_overflow_fails_before_queue_mutation(self) -> None:
        source = StaticFleetRunSource(
            [
                FleetRunRoute(
                    "run-remote",
                    "tenant-1",
                    "pool-a",
                ),
                FleetRunRoute(
                    "run-overflow",
                    "tenant-1",
                    "pool-a",
                ),
            ]
        )
        fleet, _poller, reconciler, _source, _policy = (
            self._compose(
                run_source=source,
                max_routes=1,
            )
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "exceeded its bound",
        ):
            reconciler.run_once()

        self.assertEqual(fleet.snapshot().queued_tasks, 0)
        self.assertEqual(fleet.snapshot().task_bindings, 0)

    def test_strict_pool_cannot_span_independent_stores(self) -> None:
        self.harness.store.claim_fleet_shard(
            "primary-shard",
            "control-a",
            "a" * 64,
            pool_id="pool-a",
            now=10,
        )
        second_store = DurableRunStore(
            self.harness.control_root / "second-runs.sqlite3"
        )
        second = DurableScheduler(
            second_store,
            self.harness.workflow,
            clock=self.harness.clock,
            artifact_verifier=self.harness.artifacts.verify,
        )
        second.create_run("run-second")
        second.reconcile("run-second")
        second_store.claim_fleet_shard(
            "secondary-shard",
            "control-a",
            "a" * 64,
            pool_id="pool-a",
            now=10,
        )
        fleet, poller, _unused, _source, _policy = self._compose(
            strict=True
        )
        projector, _policy = self._projector()
        source = StaticFleetRunSource(
            [
                FleetRunRoute(
                    "run-remote",
                    "tenant-1",
                    "pool-a",
                ),
                FleetRunRoute(
                    "run-second",
                    "tenant-1",
                    "pool-a",
                ),
            ]
        )
        schedulers = {
            "run-remote": self.harness.scheduler,
            "run-second": second,
        }
        reconciler = DurableFleetReconciler(
            fleet,
            poller,
            projector,
            source,
            schedulers.__getitem__,
            allow_reference_source=True,
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "spans multiple Stores",
        ):
            reconciler.run_once()

        self.assertEqual(fleet.snapshot().queued_tasks, 0)
        self.assertEqual(fleet.snapshot().task_bindings, 0)

    def test_cursor_progress_during_multi_route_snapshot_is_allowed(
        self,
    ) -> None:
        self.harness.store.claim_fleet_shard(
            "pool-a-shard",
            "control-a",
            "a" * 64,
            pool_id="pool-a",
            now=10,
        )
        second = DurableScheduler(
            self.harness.store,
            self.harness.workflow,
            clock=self.harness.clock,
            artifact_verifier=self.harness.artifacts.verify,
        )
        second.create_run("run-second")
        second.reconcile("run-second")
        fleet, poller, _unused, _source, _policy = self._compose(
            strict=True
        )
        projector, _policy = self._projector()
        source = StaticFleetRunSource(
            [
                FleetRunRoute(
                    "run-remote",
                    "tenant-1",
                    "pool-a",
                ),
                FleetRunRoute(
                    "run-second",
                    "tenant-1",
                    "pool-a",
                ),
            ]
        )
        schedulers = {
            "run-remote": self.harness.scheduler,
            "run-second": second,
        }
        reconciler = DurableFleetReconciler(
            fleet,
            poller,
            projector,
            source,
            schedulers.__getitem__,
            allow_reference_source=True,
        )
        cursor = self.harness.store.get_fleet_fairness_cursor(
            "pool-a"
        )
        assert cursor is not None
        first = replace(
            cursor,
            last_served_tenant="tenant-1",
            selection_sequence=1,
        )
        advanced = replace(first, selection_sequence=2)

        with mock.patch.object(
            DurableRunStore,
            "get_fleet_fairness_cursor",
            side_effect=(first, advanced),
        ):
            report = reconciler.run_once()

        self.assertEqual(report.routes_examined, 2)
        self.assertEqual(report.desired_tasks, 2)
        self.assertEqual(report.restored_pools, 1)
        self.assertEqual(fleet.snapshot().queued_tasks, 2)

    def test_non_production_run_source_is_rejected(self) -> None:
        fleet, poller, _unused, _source, _policy = self._compose()
        projector, _policy = self._projector()

        with self.assertRaisesRegex(
            ValueError,
            "not production ready",
        ):
            DurableFleetReconciler(
                fleet,
                poller,
                projector,
                _UnsafeRunSource(),
                lambda _run_id: self.harness.scheduler,
            )

        self.assertEqual(fleet.snapshot().queued_tasks, 0)

    def test_revoked_run_source_quarantines_queued_work(self) -> None:
        source = _RevocableRunSource(
            [
                FleetRunRoute(
                    "run-remote",
                    "tenant-1",
                    "pool-a",
                    self.run_route,
                    self.harness.store.path,
                )
            ]
        )
        fleet, _poller, reconciler, _source, _policy = (
            self._compose(run_source=source)
        )
        reconciler.run_once()
        source.production_security_ready = False

        with self.assertRaisesRegex(
            RuntimeError,
            "not production ready",
        ):
            reconciler.run_once()

        self.assertEqual(fleet.snapshot().queued_tasks, 0)
        self.assertEqual(fleet.snapshot().task_bindings, 0)

    def test_scheduler_resolution_failure_quarantines_queued_work(
        self,
    ) -> None:
        fleet, poller, initial, source, _policy = self._compose()
        initial.run_once()
        projector, _policy = self._projector()
        reconciler = DurableFleetReconciler(
            fleet,
            poller,
            projector,
            source,
            lambda _run_id: (_ for _ in ()).throw(
                RuntimeError("resolver unavailable")
            ),
            allow_reference_source=True,
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "resolution failed",
        ):
            reconciler.run_once()

        self.assertEqual(fleet.snapshot().queued_tasks, 0)
        self.assertEqual(fleet.snapshot().task_bindings, 0)


if __name__ == "__main__":
    unittest.main()
