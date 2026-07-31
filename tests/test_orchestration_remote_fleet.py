from __future__ import annotations

import hashlib
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

from src.orchestration.remote_fleet import (
    FleetAssignment,
    FleetTaskBinding,
    RemoteFleetClaimError,
    RemoteFleetConflict,
    RemoteFleetCoordinator,
)
from src.orchestration.remote_protocol import (
    ClaimBinding,
    RemoteActivityDescriptor,
    RemoteExecutionPlan,
    WorkAssignment,
    grant_binding_digest,
)
from src.orchestration.remote_scheduling import (
    AdmissionOutcome,
    DeterministicRemoteScheduler,
    ReleaseOutcome,
    RemoteTask,
    WorkerDescriptor,
    WorkerLifecycle,
)
from src.orchestration.store import FleetShardOwnership


class _Clock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self.now

    def advance(self, seconds: float) -> None:
        with self._lock:
            self.now += seconds


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _worker(
    worker_id: str,
    *,
    session_id: str | None = None,
    capacity: int = 1,
) -> WorkerDescriptor:
    return WorkerDescriptor(
        worker_id=worker_id,
        session_id=session_id or f"session-{worker_id}",
        pool_id="pool-a",
        runtime_version="2.1",
        capabilities=frozenset({"workspace.read"}),
        tools=frozenset({"file.read", "repo.inspect"}),
        authorized_tenants=frozenset({"tenant-a", "tenant-b", "tenant-c"}),
        capacity=capacity,
    )


def _binding(
    task_id: str,
    run_id: str,
    tenant_id: str = "tenant-a",
    *,
    tool_name: str = "file.read",
) -> FleetTaskBinding:
    return FleetTaskBinding(
        run_id=run_id,
        task=RemoteTask(
            task_id=task_id,
            tenant_id=tenant_id,
            pool_id="pool-a",
            tool_name=tool_name,
            required_capabilities=frozenset({"workspace.read"}),
            min_runtime_version="2",
            max_runtime_version="2.9",
        ),
    )


def _work_assignment(
    run_id: str,
    worker_id: str,
    *,
    tool_name: str = "file.read",
    activity_kind: str = "tool",
) -> WorkAssignment:
    plan = RemoteExecutionPlan(
        argv=("/usr/bin/true",),
        container_cwd="/workspace",
        limits={
            "schema_version": 2,
            "timeout_seconds": 30.0,
            "cpu_seconds": 30.0,
            "memory_bytes": 128 * 1024 * 1024,
            "output_bytes": 1024 * 1024,
            "process_count": 8,
        },
        profile_id="fleet-test",
        profile_digest=_digest("profile"),
        policy_version="policy-v1",
        request_digest=_digest(f"request:{run_id}"),
        capabilities=("activity.tool",),
    )
    claim = ClaimBinding(
        run_id=run_id,
        node_id="node-1",
        attempt_id=f"attempt-{run_id}",
        activity_request_digest=_digest(f"activity:{run_id}"),
        action_digest=_digest(f"action:{run_id}"),
        authorization_digest=_digest(f"authorization:{run_id}"),
        profile_digest=plan.profile_digest,
        request_digest=plan.request_digest,
        session_binding_digest=_digest(f"session:{worker_id}"),
        grant_binding_digest=grant_binding_digest((), ()),
        execution_plan_digest=plan.plan_digest,
        runtime_attestation_digest=_digest("runtime"),
        claim_token=f"claim-{run_id}",
        fencing_token=1,
    )
    return WorkAssignment(
        claim=claim,
        worker_id=worker_id,
        attempt_number=1,
        lease_expires_at=200.0,
        activity_kind=activity_kind,
        effect_class="read_only",
        resource_keys=(),
        activity_descriptor=RemoteActivityDescriptor(
            activity_name=tool_name,
            config_digest=_digest(f"config:{tool_name}"),
        ),
        execution_plan=plan,
        input_grants=(),
        output_grants=(),
    )


class _ExplodingObservability:
    def __getattr__(self, _name):
        def explode(*_args, **_kwargs):
            raise RuntimeError("telemetry unavailable")

        return explode


class _BlockingObservability:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()

    def record_poll(self, *_args, **_kwargs) -> None:
        self.entered.set()
        if not self.release.wait(timeout=5):
            raise RuntimeError("test observer timeout")

    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: None


class RemoteFleetCoordinatorTests(unittest.TestCase):
    def _coordinator(
        self,
        claim_run,
        *,
        clock: _Clock | None = None,
        observability=None,
        max_task_bindings: int = 32,
        tenant_quota: int = 8,
        pool_quota: int = 8,
    ) -> RemoteFleetCoordinator:
        clock = clock or _Clock()
        return RemoteFleetCoordinator(
            lambda: DeterministicRemoteScheduler(
                default_tenant_concurrency=tenant_quota,
                default_pool_concurrency=pool_quota,
                worker_idle_ttl_seconds=5.0,
                clock=clock,
            ),
            claim_run,
            max_task_bindings=max_task_bindings,
            observability=observability,
        )

    def test_pool_owner_authorizes_multiple_tenants_but_not_another_pool(
        self,
    ) -> None:
        ownership = FleetShardOwnership(
            shard_id="pool-a-shard",
            pool_id="pool-a",
            owner_id="control-a",
            fencing_epoch=1,
            policy_digest=_digest("ownership-policy"),
        )

        def exact_binding(
            task_id: str,
            tenant_id: str,
            pool_id: str,
        ) -> FleetTaskBinding:
            return FleetTaskBinding(
                run_id=f"run-{task_id}",
                node_id="node-1",
                activity_config_digest=_digest("config"),
                routing_policy_digest=_digest("routing"),
                run_route_digest=_digest("run-route"),
                shard_ownership=ownership,
                task=RemoteTask(
                    task_id=task_id,
                    tenant_id=tenant_id,
                    pool_id=pool_id,
                    tool_name="file.read",
                ),
            )

        self.assertTrue(
            exact_binding("tenant-a-task", "tenant-a", "pool-a")
            .exact_for_multi_control
        )
        self.assertTrue(
            exact_binding("tenant-b-task", "tenant-b", "pool-a")
            .exact_for_multi_control
        )
        self.assertFalse(
            exact_binding("other-pool-task", "tenant-a", "pool-b")
            .exact_for_multi_control
        )

    def test_three_workers_claim_durable_runs_in_parallel(self) -> None:
        callback_barrier = threading.Barrier(3)
        callback_runs: list[str] = []
        callback_lock = threading.Lock()

        def claim_run(run_id: str, worker_id: str) -> WorkAssignment:
            callback_barrier.wait(timeout=5)
            with callback_lock:
                callback_runs.append(run_id)
            return _work_assignment(run_id, worker_id)

        fleet = self._coordinator(claim_run)
        snapshots = [
            fleet.register_worker(_worker(f"worker-{index}"))
            for index in range(3)
        ]
        for index, tenant in enumerate(("tenant-a", "tenant-b", "tenant-c")):
            self.assertEqual(
                fleet.admit(_binding(f"task-{index}", f"run-{index}", tenant)).outcome,
                AdmissionOutcome.ADMITTED,
            )

        def assign(index: int):
            snapshot = snapshots[index]
            return fleet.assign_next(
                snapshot.descriptor.worker_id,
                worker_generation=snapshot.generation,
                session_id=snapshot.descriptor.session_id,
            )

        with ThreadPoolExecutor(max_workers=3) as executor:
            assignments = list(executor.map(assign, range(3)))

        self.assertTrue(all(isinstance(item, FleetAssignment) for item in assignments))
        self.assertEqual(set(callback_runs), {"run-0", "run-1", "run-2"})
        self.assertEqual(fleet.snapshot().active_assignments, 3)
        for assignment in assignments:
            assert assignment is not None
            self.assertEqual(
                fleet.release_terminal(assignment),
                ReleaseOutcome.RELEASED,
            )
        self.assertEqual(fleet.snapshot().active_assignments, 0)
        self.assertEqual(fleet.snapshot().task_bindings, 0)

    def test_multi_tenant_fairness_capacity_and_quota_are_preserved(self) -> None:
        calls: list[tuple[str, str]] = []

        def claim_run(run_id, worker_id):
            calls.append((run_id, worker_id))
            return _work_assignment(run_id, worker_id)

        fleet = self._coordinator(claim_run, tenant_quota=1, pool_quota=3)
        workers = [fleet.register_worker(_worker(f"worker-{i}")) for i in range(3)]
        for binding in (
            _binding("a-1", "run-a-1", "tenant-a"),
            _binding("a-2", "run-a-2", "tenant-a"),
            _binding("b-1", "run-b-1", "tenant-b"),
        ):
            fleet.admit(binding)

        first = fleet.assign_next(
            "worker-0",
            worker_generation=workers[0].generation,
            session_id=workers[0].descriptor.session_id,
        )
        second = fleet.assign_next(
            "worker-1",
            worker_generation=workers[1].generation,
            session_id=workers[1].descriptor.session_id,
        )
        third = fleet.assign_next(
            "worker-2",
            worker_generation=workers[2].generation,
            session_id=workers[2].descriptor.session_id,
        )

        assert first is not None and second is not None
        self.assertEqual(first.routing.task.task_id, "a-1")
        self.assertEqual(second.routing.task.task_id, "b-1")
        self.assertIsNone(third)
        fleet.release_terminal(first)
        next_assignment = fleet.assign_next(
            "worker-0",
            worker_generation=workers[0].generation,
            session_id=workers[0].descriptor.session_id,
        )
        assert next_assignment is not None
        self.assertEqual(next_assignment.routing.task.task_id, "a-2")

    def test_drain_and_busy_ttl_never_silently_release_durable_work(self) -> None:
        clock = _Clock(0.0)
        calls: list[str] = []

        def claim_run(run_id, worker_id):
            calls.append(run_id)
            return _work_assignment(run_id, worker_id)

        fleet = self._coordinator(claim_run, clock=clock)
        drained = fleet.register_worker(_worker("worker-drain"))
        fleet.drain_worker("worker-drain")
        fleet.admit(_binding("drain-task", "run-drain"))
        self.assertIsNone(
            fleet.assign_next(
                "worker-drain",
                worker_generation=drained.generation,
                session_id=drained.descriptor.session_id,
            )
        )
        self.assertEqual(calls, [])

        live = fleet.register_worker(_worker("worker-live"))
        assignment = fleet.assign_next(
            "worker-live",
            worker_generation=live.generation,
            session_id=live.descriptor.session_id,
        )
        assert assignment is not None
        clock.advance(5.0)
        report = fleet.sweep_workers()
        self.assertEqual(report.expired_busy_workers, 1)
        self.assertEqual(report.retained_assignments, 1)
        self.assertEqual(fleet.snapshot().active_assignments, 1)
        self.assertIsNone(
            fleet.assign_next(
                "worker-live",
                worker_generation=live.generation,
                session_id=live.descriptor.session_id,
            )
        )
        self.assertEqual(
            fleet.release_terminal(assignment),
            ReleaseOutcome.RELEASED,
        )

    def test_durable_claim_failure_rolls_back_projection_capacity(self) -> None:
        fail = True

        def claim_run(run_id, worker_id):
            if fail:
                raise RuntimeError("known claim rejection")
            return _work_assignment(run_id, worker_id)

        fleet = self._coordinator(claim_run)
        worker = fleet.register_worker(_worker("worker-1"))
        binding = _binding("task-1", "run-1")
        fleet.admit(binding)

        with self.assertRaises(RemoteFleetClaimError):
            fleet.assign_next(
                "worker-1",
                worker_generation=worker.generation,
                session_id=worker.descriptor.session_id,
            )
        self.assertEqual(fleet.snapshot().active_assignments, 0)
        self.assertEqual(fleet.snapshot().task_bindings, 0)

        fail = False
        fleet.admit(binding)
        self.assertIsNotNone(
            fleet.assign_next(
                "worker-1",
                worker_generation=worker.generation,
                session_id=worker.descriptor.session_id,
            )
        )

    def test_real_tool_name_is_validated_against_activity_descriptor(self) -> None:
        def claim_run(run_id, worker_id):
            return _work_assignment(
                run_id,
                worker_id,
                tool_name="repo.inspect",
                activity_kind="tool",
            )

        fleet = self._coordinator(claim_run)
        worker = fleet.register_worker(_worker("worker-1"))
        fleet.admit(
            _binding(
                "task-1",
                "run-1",
                tool_name="repo.inspect",
            )
        )

        assignment = fleet.assign_next(
            "worker-1",
            worker_generation=worker.generation,
            session_id=worker.descriptor.session_id,
        )

        assert assignment is not None
        self.assertEqual(assignment.durable.activity_kind, "tool")
        self.assertEqual(
            assignment.durable.activity_descriptor.activity_name,
            "repo.inspect",
        )

    def test_durable_projection_conflict_rolls_back_without_advancing_store(self) -> None:
        durable_calls: list[tuple[str, str]] = []

        def claim_run(run_id, worker_id):
            durable_calls.append((run_id, worker_id))
            return _work_assignment("different-run", worker_id)

        fleet = self._coordinator(claim_run)
        worker = fleet.register_worker(_worker("worker-1"))
        fleet.admit(_binding("task-1", "run-1"))

        with self.assertRaisesRegex(RemoteFleetConflict, "conflicts"):
            fleet.assign_next(
                "worker-1",
                worker_generation=worker.generation,
                session_id=worker.descriptor.session_id,
            )

        self.assertEqual(durable_calls, [("run-1", "worker-1")])
        self.assertEqual(fleet.snapshot().active_assignments, 0)
        self.assertEqual(fleet.snapshot().task_bindings, 0)

    def test_telemetry_explosion_does_not_affect_fleet_state(self) -> None:
        fleet = self._coordinator(
            lambda run_id, worker_id: _work_assignment(run_id, worker_id),
            observability=_ExplodingObservability(),
        )
        worker = fleet.register_worker(_worker("worker-1"))
        self.assertEqual(
            fleet.admit(_binding("task-1", "run-1")).outcome,
            AdmissionOutcome.ADMITTED,
        )
        self.assertEqual(
            fleet.touch_worker(
                "worker-1",
                worker_generation=worker.generation,
                session_id=worker.descriptor.session_id,
            ).value,
            "accepted",
        )
        assignment = fleet.assign_next(
            "worker-1",
            worker_generation=worker.generation,
            session_id=worker.descriptor.session_id,
        )
        assert assignment is not None
        self.assertEqual(
            fleet.release_terminal(assignment),
            ReleaseOutcome.RELEASED,
        )

    def test_blocked_observer_does_not_hold_fleet_lock(self) -> None:
        observer = _BlockingObservability()
        fleet = self._coordinator(
            lambda run_id, worker_id: _work_assignment(run_id, worker_id),
            observability=observer,
        )
        worker = fleet.register_worker(_worker("worker-1"))
        fleet.admit(_binding("task-1", "run-1"))
        result: dict[str, object] = {}

        def assign() -> None:
            result["assignment"] = fleet.assign_next(
                "worker-1",
                worker_generation=worker.generation,
                session_id=worker.descriptor.session_id,
            )

        thread = threading.Thread(target=assign)
        thread.start()
        self.assertTrue(observer.entered.wait(timeout=5))
        # snapshot and touch must not wait for the injected observer.
        self.assertEqual(fleet.snapshot().active_assignments, 1)
        self.assertEqual(
            fleet.touch_worker(
                "worker-1",
                worker_generation=worker.generation,
                session_id=worker.descriptor.session_id,
            ).value,
            "accepted",
        )
        observer.release.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertIsNotNone(result["assignment"])

    def test_restart_rebuild_is_explicit_atomic_and_non_authoritative(self) -> None:
        workers = [_worker("worker-1"), _worker("worker-2")]
        tasks = [
            _binding("task-1", "run-1", "tenant-a"),
            _binding("task-2", "run-2", "tenant-b"),
        ]
        fleet = self._coordinator(
            lambda run_id, worker_id: _work_assignment(run_id, worker_id)
        )

        report = fleet.rebuild(tasks, workers)
        snapshot = fleet.snapshot()

        self.assertEqual((report.workers, report.tasks), (2, 2))
        self.assertFalse(report.execution_truth)
        self.assertEqual(snapshot.queued_tasks, 2)
        self.assertEqual(snapshot.task_bindings, 2)
        registered = fleet.register_worker(workers[0])
        assignment = fleet.assign_next(
            "worker-1",
            worker_generation=registered.generation,
            session_id=registered.descriptor.session_id,
        )
        self.assertIsNotNone(assignment)

        with self.assertRaisesRegex(RemoteFleetConflict, "active routing"):
            fleet.rebuild(tasks, workers)

    def test_queue_reconcile_replaces_stale_policy_binding(self) -> None:
        fleet = self._coordinator(
            lambda run_id, worker_id: _work_assignment(
                run_id,
                worker_id,
            )
        )
        stale = _binding("task-1", "run-1")
        current = replace(
            stale,
            routing_policy_digest=_digest("routing-policy-v2"),
        )
        fleet.admit(stale)

        changed = fleet.reconcile_queued([current])
        repeated = fleet.reconcile_queued([current])

        self.assertEqual(changed.desired_tasks, 1)
        self.assertEqual(changed.withdrawn_tasks, 1)
        self.assertEqual(changed.admitted_tasks, 1)
        self.assertEqual(changed.deferred_active_tasks, 0)
        self.assertEqual(changed.rejected_tasks, 0)
        self.assertEqual(changed.task_bindings, 1)
        self.assertEqual(repeated.retained_tasks, 1)
        self.assertEqual(repeated.withdrawn_tasks, 0)
        self.assertFalse(changed.execution_truth)

    def test_queue_reconcile_defers_active_authority_until_terminal(
        self,
    ) -> None:
        fleet = self._coordinator(
            lambda run_id, worker_id: _work_assignment(
                run_id,
                worker_id,
            )
        )
        worker = fleet.register_worker(_worker("worker-1"))
        fleet.admit(_binding("task-1", "run-1"))
        assignment = fleet.assign_next(
            "worker-1",
            worker_generation=worker.generation,
            session_id=worker.descriptor.session_id,
        )
        assert assignment is not None

        deferred = fleet.reconcile_queued([])

        self.assertEqual(deferred.deferred_active_tasks, 1)
        self.assertEqual(deferred.withdrawn_tasks, 0)
        self.assertEqual(deferred.task_bindings, 1)
        self.assertEqual(
            fleet.release_terminal(assignment),
            ReleaseOutcome.RELEASED,
        )
        converged = fleet.reconcile_queued([])
        self.assertEqual(converged.task_bindings, 0)

    def test_queue_reconcile_validates_entire_projection_before_mutation(
        self,
    ) -> None:
        fleet = self._coordinator(
            lambda run_id, worker_id: _work_assignment(
                run_id,
                worker_id,
            )
        )
        existing = _binding("task-1", "run-1")
        fleet.admit(existing)

        with self.assertRaisesRegex(
            RemoteFleetConflict,
            "duplicate task",
        ):
            fleet.reconcile_queued([existing, existing])

        self.assertEqual(fleet.snapshot().queued_tasks, 1)
        self.assertEqual(fleet.snapshot().task_bindings, 1)

    def test_queue_reconcile_never_evicts_active_binding_for_capacity(
        self,
    ) -> None:
        fleet = self._coordinator(
            lambda run_id, worker_id: _work_assignment(
                run_id,
                worker_id,
            ),
            max_task_bindings=1,
        )
        worker = fleet.register_worker(_worker("worker-1"))
        fleet.admit(_binding("active-task", "active-run"))
        assignment = fleet.assign_next(
            "worker-1",
            worker_generation=worker.generation,
            session_id=worker.descriptor.session_id,
        )
        assert assignment is not None

        report = fleet.reconcile_queued(
            [_binding("new-task", "new-run")]
        )

        self.assertEqual(report.deferred_active_tasks, 1)
        self.assertEqual(report.rejected_tasks, 1)
        self.assertEqual(report.task_bindings, 1)
        self.assertEqual(fleet.snapshot().active_assignments, 1)
        self.assertEqual(
            fleet.release_terminal(assignment),
            ReleaseOutcome.RELEASED,
        )

    def test_policy_reconcile_and_poll_have_one_queue_linearization(
        self,
    ) -> None:
        fleet = self._coordinator(
            lambda run_id, worker_id: _work_assignment(
                run_id,
                worker_id,
            ),
            max_task_bindings=1,
        )
        worker = fleet.register_worker(_worker("worker-1"))
        stale = _binding("task-1", "run-1")
        current = replace(
            stale,
            routing_policy_digest=_digest("routing-policy-v2"),
        )
        fleet.admit(stale)
        barrier = threading.Barrier(2)

        def assign():
            barrier.wait(timeout=5)
            return fleet.assign_next(
                "worker-1",
                worker_generation=worker.generation,
                session_id=worker.descriptor.session_id,
            )

        def reconcile():
            barrier.wait(timeout=5)
            return fleet.reconcile_queued([current])

        with ThreadPoolExecutor(max_workers=2) as executor:
            assignment_future = executor.submit(assign)
            reconcile_future = executor.submit(reconcile)
            assignment = assignment_future.result(timeout=5)
            report = reconcile_future.result(timeout=5)

        assert assignment is not None
        self.assertEqual(fleet.snapshot().active_assignments, 1)
        self.assertEqual(fleet.snapshot().task_bindings, 1)
        self.assertEqual(report.rejected_tasks, 0)
        self.assertIn(
            (
                report.deferred_active_tasks,
                report.withdrawn_tasks,
                report.admitted_tasks,
            ),
            {
                (1, 0, 0),
                (0, 1, 1),
            },
        )
        self.assertEqual(
            fleet.release_terminal(assignment),
            ReleaseOutcome.RELEASED,
        )

    def test_queue_reconcile_ignores_observer_failures(self) -> None:
        fleet = self._coordinator(
            lambda run_id, worker_id: _work_assignment(
                run_id,
                worker_id,
            ),
            observability=_ExplodingObservability(),
        )
        stale = _binding("task-1", "run-1")
        current = replace(
            stale,
            routing_policy_digest=_digest("routing-policy-v2"),
        )
        fleet.admit(stale)

        report = fleet.reconcile_queued([current])

        self.assertEqual(report.withdrawn_tasks, 1)
        self.assertEqual(report.admitted_tasks, 1)
        self.assertEqual(fleet.snapshot().queued_tasks, 1)

    def test_rebuild_rejects_during_inflight_durable_claim(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        def claim_run(run_id, worker_id):
            entered.set()
            if not release.wait(timeout=5):
                raise RuntimeError("test callback timeout")
            return _work_assignment(run_id, worker_id)

        fleet = self._coordinator(claim_run)
        worker = fleet.register_worker(_worker("worker-1"))
        binding = _binding("task-1", "run-1")
        fleet.admit(binding)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                fleet.assign_next,
                "worker-1",
                worker_generation=worker.generation,
                session_id=worker.descriptor.session_id,
            )
            self.assertTrue(entered.wait(timeout=5))
            with self.assertRaisesRegex(RemoteFleetConflict, "claim callbacks"):
                fleet.rebuild([binding], [_worker("worker-1")])
            release.set()
            self.assertIsNotNone(future.result(timeout=5))

    def test_task_binding_registry_and_terminal_generation_are_fail_closed(self) -> None:
        fleet = self._coordinator(
            lambda run_id, worker_id: _work_assignment(run_id, worker_id),
            max_task_bindings=1,
        )
        worker = fleet.register_worker(_worker("worker-1"))
        first = _binding("task-1", "run-1")
        fleet.admit(first)
        with self.assertRaisesRegex(RemoteFleetConflict, "registry is full"):
            fleet.admit(_binding("task-2", "run-2"))
        with self.assertRaisesRegex(RemoteFleetConflict, "another Run"):
            fleet.admit(_binding("task-1", "run-other"))
        assignment = fleet.assign_next(
            "worker-1",
            worker_generation=worker.generation,
            session_id=worker.descriptor.session_id,
        )
        assert assignment is not None
        forged = FleetAssignment(
            routing=replace(
                assignment.routing,
                worker_generation=assignment.routing.worker_generation + 1,
            ),
            durable=assignment.durable,
        )

        self.assertEqual(
            fleet.release_rejected(forged),
            ReleaseOutcome.STALE_WORKER_GENERATION,
        )
        self.assertEqual(fleet.snapshot().active_assignments, 1)
        self.assertEqual(fleet.snapshot().task_bindings, 1)
        self.assertEqual(
            fleet.release_rejected(assignment),
            ReleaseOutcome.RELEASED,
        )


if __name__ == "__main__":
    unittest.main()
