from __future__ import annotations

import dataclasses
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from src.orchestration.remote_scheduling import (
    AdmissionOutcome,
    DeterministicRemoteScheduler,
    PollOutcome,
    ReleaseOutcome,
    RemoteSchedulingConflict,
    RemoteSchedulingValidationError,
    RemoteTask,
    TouchOutcome,
    WorkerDescriptor,
    WorkerLifecycle,
    WithdrawalOutcome,
    is_worker_compatible,
)


class _FakeClock:
    def __init__(self, initial: float = 100.0) -> None:
        self._value = initial
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self._value

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._value += seconds


_TEST_TENANTS = frozenset(
    {"tenant-a", "tenant-b", "tenant-z"}
    | {f"tenant-{index:02d}" for index in range(32)}
)


def worker(
    worker_id: str,
    *,
    session_id: str | None = None,
    pool_id: str = "pool-a",
    runtime_version: str = "2.1",
    capabilities: frozenset[str] = frozenset({"workspace.read", "workspace.write"}),
    tools: frozenset[str] = frozenset({"file.read", "file.write"}),
    authorized_tenants: frozenset[str] = _TEST_TENANTS,
    resource_keys: frozenset[str] = frozenset(),
    capacity: int = 1,
) -> WorkerDescriptor:
    return WorkerDescriptor(
        worker_id=worker_id,
        session_id=f"session-{worker_id}" if session_id is None else session_id,
        pool_id=pool_id,
        runtime_version=runtime_version,
        capabilities=capabilities,
        tools=tools,
        authorized_tenants=authorized_tenants,
        resource_keys=resource_keys,
        capacity=capacity,
    )


def poll_current(
    scheduler: DeterministicRemoteScheduler,
    worker_id: str,
):
    snapshot = scheduler.worker_snapshot(worker_id)
    assert snapshot is not None
    return scheduler.poll_and_claim(
        worker_id,
        worker_generation=snapshot.generation,
        session_id=snapshot.descriptor.session_id,
    )


def release_current(
    scheduler: DeterministicRemoteScheduler,
    assignment_id: str,
    worker_id: str,
):
    snapshot = scheduler.worker_snapshot(worker_id)
    assert snapshot is not None
    return scheduler.release(
        assignment_id,
        worker_id,
        worker_generation=snapshot.generation,
        session_id=snapshot.descriptor.session_id,
    )


def task(
    task_id: str,
    tenant_id: str = "tenant-a",
    *,
    pool_id: str = "pool-a",
    tool_name: str = "file.read",
    required_capabilities: frozenset[str] = frozenset({"workspace.read"}),
    required_resource_keys: frozenset[str] = frozenset(),
    min_runtime_version: str = "2",
    max_runtime_version: str | None = "2.9",
) -> RemoteTask:
    return RemoteTask(
        task_id=task_id,
        tenant_id=tenant_id,
        pool_id=pool_id,
        tool_name=tool_name,
        required_capabilities=required_capabilities,
        required_resource_keys=required_resource_keys,
        min_runtime_version=min_runtime_version,
        max_runtime_version=max_runtime_version,
    )


class RemoteSchedulingTests(unittest.TestCase):
    def test_descriptor_and_task_matching_is_fail_closed(self) -> None:
        descriptor = worker("worker-1")
        self.assertTrue(is_worker_compatible(descriptor, task("task-1")))
        self.assertFalse(
            is_worker_compatible(
                descriptor,
                task("task-2", required_capabilities=frozenset({"host.root"})),
            )
        )
        self.assertFalse(
            is_worker_compatible(
                descriptor,
                task("task-3", tool_name="shell.exec"),
            )
        )
        self.assertFalse(
            is_worker_compatible(
                descriptor,
                task(
                    "task-4",
                    min_runtime_version="3",
                    max_runtime_version=None,
                ),
            )
        )
        self.assertFalse(
            is_worker_compatible(
                descriptor,
                task("task-5", pool_id="pool-b"),
            )
        )
        self.assertFalse(
            is_worker_compatible(
                descriptor,
                task("task-tenant", "tenant-c"),
            )
        )

        with self.assertRaises(RemoteSchedulingValidationError):
            worker("worker-2", runtime_version="02.1")
        with self.assertRaises(RemoteSchedulingValidationError):
            task("task-6", min_runtime_version="3", max_runtime_version="2")
        with self.assertRaises(RemoteSchedulingValidationError):
            WorkerDescriptor(
                worker_id="worker-3",
                session_id="session-worker-3",
                pool_id="pool-a",
                runtime_version="1",
                capabilities=frozenset(),
                tools=frozenset(),
                authorized_tenants=frozenset({"tenant-a"}),
            )

    def test_routing_envelopes_have_no_payload_or_metadata_escape_hatch(self) -> None:
        self.assertEqual(
            {field.name for field in dataclasses.fields(RemoteTask)},
            {
                "task_id",
                "tenant_id",
                "pool_id",
                "tool_name",
                "required_capabilities",
                "required_resource_keys",
                "min_runtime_version",
                "max_runtime_version",
                "schema_version",
            },
        )
        self.assertNotIn("metadata", WorkerDescriptor.__dataclass_fields__)
        self.assertNotIn("arguments", RemoteTask.__dataclass_fields__)

    def test_resource_requirements_are_part_of_worker_compatibility(self) -> None:
        requirement = task(
            "task-resource",
            required_resource_keys=frozenset({"workspace:/project"}),
        )
        self.assertFalse(
            is_worker_compatible(worker("worker-no-resource"), requirement)
        )
        self.assertTrue(
            is_worker_compatible(
                worker(
                    "worker-resource",
                    resource_keys=frozenset(
                        {"workspace:/project", "workspace:/other"}
                    ),
                ),
                requirement,
            )
        )
        legacy_worker = WorkerDescriptor(
            "legacy-worker",
            "legacy-session",
            "pool-a",
            "2.1",
            frozenset({"workspace.read"}),
            frozenset({"file.read"}),
            frozenset({"tenant-a"}),
            3,
        )
        legacy_task = RemoteTask(
            "legacy-task",
            "tenant-a",
            "pool-a",
            "file.read",
            frozenset({"workspace.read"}),
            "2",
            "2.9",
        )
        self.assertEqual(legacy_worker.capacity, 3)
        self.assertEqual(legacy_worker.resource_keys, frozenset())
        self.assertEqual(
            legacy_task.required_resource_keys,
            frozenset(),
        )
        self.assertTrue(is_worker_compatible(legacy_worker, legacy_task))

    def test_fifo_with_tenant_round_robin_resists_starvation(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=_FakeClock())
        scheduler.register_worker(worker("worker-1"))
        for queued in (
            task("a-1", "tenant-a"),
            task("a-2", "tenant-a"),
            task("b-1", "tenant-b"),
            task("b-2", "tenant-b"),
        ):
            self.assertEqual(scheduler.admit(queued).outcome, AdmissionOutcome.ADMITTED)

        claimed: list[str] = []
        for _index in range(4):
            decision = poll_current(scheduler, "worker-1")
            self.assertEqual(decision.outcome, PollOutcome.CLAIMED)
            assert decision.assignment is not None
            claimed.append(decision.assignment.task.task_id)
            self.assertEqual(
                release_current(
                    scheduler,
                    decision.assignment.assignment_id,
                    "worker-1",
                ),
                ReleaseOutcome.RELEASED,
            )

        self.assertEqual(claimed, ["a-1", "b-1", "a-2", "b-2"])

    def test_continuous_hot_tenant_cannot_starve_an_eligible_tenant(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=_FakeClock())
        scheduler.register_worker(worker("worker-1"))
        scheduler.admit(task("a-0", "tenant-a"))
        scheduler.admit(task("b-0", "tenant-b"))

        served: list[str] = []
        for index in range(6):
            decision = poll_current(scheduler, "worker-1")
            self.assertEqual(decision.outcome, PollOutcome.CLAIMED)
            assert decision.assignment is not None
            served.append(decision.assignment.task.tenant_id)
            release_current(
                scheduler,
                decision.assignment.assignment_id,
                "worker-1",
            )
            scheduler.admit(task(f"a-hot-{index}", "tenant-a"))
            if index % 2 == 1:
                scheduler.admit(task(f"b-next-{index}", "tenant-b"))

        self.assertEqual(served[:2], ["tenant-a", "tenant-b"])
        self.assertIn("tenant-b", served[2:])
        for left, right in zip(served, served[1:]):
            self.assertFalse(left == right == "tenant-a")

    def test_restored_pool_cursor_preserves_restart_fairness(self) -> None:
        restarted = DeterministicRemoteScheduler(clock=_FakeClock())
        restarted.restore_tenant_cursor("pool-a", "tenant-a")
        restarted.register_worker(worker("worker-1"))
        restarted.admit(task("a-after-restart", "tenant-a"))
        restarted.admit(task("b-after-restart", "tenant-b"))

        decision = poll_current(restarted, "worker-1")

        assert decision.assignment is not None
        self.assertEqual(
            decision.assignment.task.task_id,
            "b-after-restart",
        )
        self.assertTrue(restarted.pool_cursor_is_restored("pool-a"))

    def test_cursor_restore_requires_an_idle_pool(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=_FakeClock())
        scheduler.admit(task("queued", "tenant-a"))

        with self.assertRaisesRegex(
            RemoteSchedulingConflict,
            "before queue admission",
        ):
            scheduler.restore_tenant_cursor("pool-a", "tenant-a")

    def test_restored_pool_cursor_registry_is_bounded(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=_FakeClock())

        with patch(
            "src.orchestration.remote_scheduling."
            "MAX_RESTORED_POOL_CURSORS",
            1,
        ):
            scheduler.restore_tenant_cursor("pool-a", "tenant-a")
            scheduler.restore_tenant_cursor("pool-a", "tenant-b")
            with self.assertRaisesRegex(
                RemoteSchedulingConflict,
                "registry is full",
            ):
                scheduler.restore_tenant_cursor("pool-b", "tenant-a")

        self.assertTrue(scheduler.pool_cursor_is_restored("pool-a"))
        self.assertFalse(scheduler.pool_cursor_is_restored("pool-b"))

    def test_unrelated_pool_claims_cannot_reset_another_pools_fair_cursor(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=_FakeClock())
        scheduler.register_worker(worker("worker-a", pool_id="pool-a"))
        scheduler.register_worker(worker("worker-b", pool_id="pool-b"))
        scheduler.admit(task("b-a-1", "tenant-a", pool_id="pool-b"))
        scheduler.admit(task("b-a-2", "tenant-a", pool_id="pool-b"))
        scheduler.admit(task("b-b-1", "tenant-b", pool_id="pool-b"))

        first = poll_current(scheduler, "worker-b")
        assert first.assignment is not None
        self.assertEqual(first.assignment.task.task_id, "b-a-1")
        release_current(scheduler, first.assignment.assignment_id, "worker-b")

        for index in range(3):
            scheduler.admit(task(f"a-z-{index}", "tenant-z", pool_id="pool-a"))
            unrelated = poll_current(scheduler, "worker-a")
            assert unrelated.assignment is not None
            release_current(
                scheduler,
                unrelated.assignment.assignment_id,
                "worker-a",
            )

        second = poll_current(scheduler, "worker-b")
        assert second.assignment is not None
        self.assertEqual(second.assignment.task.task_id, "b-b-1")

    def test_tenant_quota_isolation_skips_hot_tenant(self) -> None:
        scheduler = DeterministicRemoteScheduler(
            tenant_concurrency_quotas={"tenant-a": 1, "tenant-b": 2},
            default_pool_concurrency=3,
            clock=_FakeClock(),
        )
        for worker_id in ("worker-1", "worker-2", "worker-3"):
            scheduler.register_worker(worker(worker_id))
        for queued in (
            task("a-1", "tenant-a"),
            task("a-2", "tenant-a"),
            task("b-1", "tenant-b"),
        ):
            scheduler.admit(queued)

        first = poll_current(scheduler, "worker-1")
        second = poll_current(scheduler, "worker-2")
        third = poll_current(scheduler, "worker-3")

        assert first.assignment is not None and second.assignment is not None
        self.assertEqual(first.assignment.task.task_id, "a-1")
        self.assertEqual(second.assignment.task.task_id, "b-1")
        self.assertEqual(third.outcome, PollOutcome.TENANT_AT_CAPACITY)

    def test_pool_quota_applies_across_workers(self) -> None:
        scheduler = DeterministicRemoteScheduler(
            pool_concurrency_quotas={"pool-a": 1},
            clock=_FakeClock(),
        )
        scheduler.register_worker(worker("worker-1"))
        scheduler.register_worker(worker("worker-2"))
        scheduler.admit(task("task-1"))
        scheduler.admit(task("task-2"))

        self.assertEqual(
            poll_current(scheduler, "worker-1").outcome,
            PollOutcome.CLAIMED,
        )
        self.assertEqual(
            poll_current(scheduler, "worker-2").outcome,
            PollOutcome.POOL_AT_CAPACITY,
        )

    def test_durable_quota_policy_digest_is_mapping_order_independent(
        self,
    ) -> None:
        left = DeterministicRemoteScheduler(
            tenant_concurrency_quotas={
                "tenant-a": 1,
                "tenant-b": 2,
            },
            pool_concurrency_quotas={
                "pool-a": 3,
                "pool-b": 4,
            },
        )
        right = DeterministicRemoteScheduler(
            tenant_concurrency_quotas={
                "tenant-b": 2,
                "tenant-a": 1,
            },
            pool_concurrency_quotas={
                "pool-b": 4,
                "pool-a": 3,
            },
        )
        routing_digest = "a" * 64

        self.assertEqual(
            left.durable_admission_scope(
                task("task-1"),
                routing_policy_digest=routing_digest,
            ).quota_policy_digest,
            right.durable_admission_scope(
                task("task-1"),
                routing_policy_digest=routing_digest,
            ).quota_policy_digest,
        )

    def test_atomic_poll_never_oversubscribes_worker_capacity(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=_FakeClock())
        scheduler.register_worker(worker("worker-1", capacity=4))
        for index in range(24):
            scheduler.admit(task(f"task-{index}", f"tenant-{index:02d}"))
        barrier = threading.Barrier(25)

        def poll() -> PollOutcome:
            barrier.wait(timeout=5)
            return poll_current(scheduler, "worker-1").outcome

        with ThreadPoolExecutor(max_workers=24) as executor:
            futures = [executor.submit(poll) for _index in range(24)]
            barrier.wait(timeout=5)
            outcomes = [future.result(timeout=5) for future in futures]

        self.assertEqual(outcomes.count(PollOutcome.CLAIMED), 4)
        self.assertEqual(
            outcomes.count(PollOutcome.WORKER_AT_CAPACITY),
            20,
        )
        self.assertEqual(scheduler.worker_snapshot("worker-1").active_count, 4)

    def test_drain_race_is_linearized_without_losing_active_assignment(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=_FakeClock())
        scheduler.register_worker(worker("worker-1"))
        scheduler.admit(task("task-1"))
        barrier = threading.Barrier(3)
        results: dict[str, object] = {}

        def poll() -> None:
            barrier.wait(timeout=5)
            results["poll"] = poll_current(scheduler, "worker-1")

        def drain() -> None:
            barrier.wait(timeout=5)
            results["drain"] = scheduler.request_drain("worker-1")

        poll_thread = threading.Thread(target=poll)
        drain_thread = threading.Thread(target=drain)
        poll_thread.start()
        drain_thread.start()
        barrier.wait(timeout=5)
        poll_thread.join(timeout=5)
        drain_thread.join(timeout=5)

        snapshot = scheduler.worker_snapshot("worker-1")
        assert snapshot is not None
        self.assertEqual(snapshot.lifecycle, WorkerLifecycle.DRAINING)
        self.assertIn(snapshot.active_count, {0, 1})
        self.assertEqual(
            poll_current(scheduler, "worker-1").outcome,
            PollOutcome.WORKER_DRAINING,
        )
        poll_decision = results["poll"]
        assert hasattr(poll_decision, "assignment")
        if poll_decision.assignment is not None:
            self.assertEqual(snapshot.active_count, 1)
            self.assertEqual(
                release_current(
                    scheduler,
                    poll_decision.assignment.assignment_id,
                    "worker-1",
                ),
                ReleaseOutcome.RELEASED,
            )
        else:
            self.assertEqual(poll_decision.outcome, PollOutcome.WORKER_DRAINING)

    def test_worker_descriptor_cannot_change_under_active_claim(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=_FakeClock())
        initial = scheduler.register_worker(worker("worker-1"))
        scheduler.admit(task("task-1"))
        self.assertEqual(
            poll_current(scheduler, "worker-1").outcome,
            PollOutcome.CLAIMED,
        )

        repeated = scheduler.register_worker(worker("worker-1"))
        self.assertEqual(repeated.generation, initial.generation)
        with self.assertRaisesRegex(RemoteSchedulingConflict, "active assignments"):
            scheduler.register_worker(worker("worker-1", capacity=2))
        scheduler.request_drain("worker-1")
        with self.assertRaisesRegex(RemoteSchedulingConflict, "active assignments"):
            scheduler.reactivate_worker("worker-1")

    def test_drain_remove_and_explicit_reactivation_lifecycle(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=_FakeClock())
        registered = scheduler.register_worker(worker("worker-1"))
        drained = scheduler.request_drain("worker-1")
        self.assertGreaterEqual(drained.drain_requested_at, registered.registered_at)
        reactivated = scheduler.reactivate_worker("worker-1")
        self.assertEqual(reactivated.lifecycle, WorkerLifecycle.ACTIVE)
        self.assertGreater(reactivated.generation, registered.generation)
        scheduler.request_drain("worker-1")
        self.assertTrue(scheduler.remove_worker("worker-1"))
        self.assertIsNone(scheduler.worker_snapshot("worker-1"))

    def test_queue_backpressure_and_duplicates_are_explicit(self) -> None:
        total = DeterministicRemoteScheduler(
            max_queued_tasks=2,
            max_queued_per_tenant=2,
            max_queued_per_pool=2,
            clock=_FakeClock(),
        )
        self.assertEqual(total.admit(task("task-1")).outcome, AdmissionOutcome.ADMITTED)
        self.assertEqual(total.admit(task("task-1")).outcome, AdmissionOutcome.DUPLICATE)
        self.assertEqual(total.admit(task("task-2")).outcome, AdmissionOutcome.ADMITTED)
        self.assertEqual(total.admit(task("task-3")).outcome, AdmissionOutcome.QUEUE_FULL)

        per_tenant = DeterministicRemoteScheduler(
            max_queued_per_tenant=1,
            clock=_FakeClock(),
        )
        per_tenant.admit(task("task-1", "tenant-a"))
        self.assertEqual(
            per_tenant.admit(task("task-2", "tenant-a")).outcome,
            AdmissionOutcome.TENANT_QUEUE_FULL,
        )

        per_pool = DeterministicRemoteScheduler(
            max_queued_per_pool=1,
            clock=_FakeClock(),
        )
        per_pool.admit(task("task-1", "tenant-a"))
        self.assertEqual(
            per_pool.admit(task("task-2", "tenant-b")).outcome,
            AdmissionOutcome.POOL_QUEUE_FULL,
        )

    def test_small_global_bounds_do_not_require_repeating_higher_local_defaults(self) -> None:
        scheduler = DeterministicRemoteScheduler(
            max_queued_tasks=1,
            max_active_tasks=1,
            clock=_FakeClock(),
        )
        scheduler.register_worker(worker("worker-1", capacity=2))
        self.assertEqual(
            scheduler.admit(task("task-1")).outcome,
            AdmissionOutcome.ADMITTED,
        )
        self.assertEqual(
            poll_current(scheduler, "worker-1").outcome,
            PollOutcome.CLAIMED,
        )

    def test_worker_and_tenant_registries_are_bounded_and_reclaim_entries(self) -> None:
        scheduler = DeterministicRemoteScheduler(
            max_workers=1,
            max_tenants=1,
            clock=_FakeClock(),
        )
        scheduler.register_worker(worker("worker-1"))
        with self.assertRaisesRegex(RemoteSchedulingConflict, "registry is full"):
            scheduler.register_worker(worker("worker-2"))

        self.assertEqual(
            scheduler.admit(task("task-1", "tenant-a")).outcome,
            AdmissionOutcome.ADMITTED,
        )
        self.assertEqual(
            scheduler.admit(task("task-2", "tenant-b")).outcome,
            AdmissionOutcome.TENANT_REGISTRY_FULL,
        )
        decision = poll_current(scheduler, "worker-1")
        assert decision.assignment is not None
        # tenant-a is still tracked while active, so a second identity cannot
        # bypass max_tenants merely because its queue was drained.
        self.assertEqual(
            scheduler.admit(task("task-2", "tenant-b")).outcome,
            AdmissionOutcome.TENANT_REGISTRY_FULL,
        )
        release_current(scheduler, decision.assignment.assignment_id, "worker-1")
        self.assertEqual(
            scheduler.admit(task("task-2", "tenant-b")).outcome,
            AdmissionOutcome.ADMITTED,
        )

    def test_repeated_pool_replacement_does_not_leak_fairness_cursors(self) -> None:
        tenant = "tenant-a"
        scheduler = DeterministicRemoteScheduler(clock=_FakeClock())
        for index in range(50):
            pool_id = f"pool-{index}"
            scheduler.register_worker(
                worker(
                    "worker-1",
                    pool_id=pool_id,
                    authorized_tenants=frozenset({tenant}),
                )
            )
            scheduler.admit(task(f"task-{index}", tenant, pool_id=pool_id))
            decision = poll_current(scheduler, "worker-1")
            assert decision.assignment is not None
            release_current(
                scheduler,
                decision.assignment.assignment_id,
                "worker-1",
            )

        snapshot = scheduler.snapshot()
        self.assertEqual(len(snapshot.workers), 1)
        self.assertEqual(len(snapshot.tenant_cursors), 1)
        self.assertEqual(snapshot.tenant_cursors[0][0], "pool-49")

    def test_fake_clock_produces_deterministic_schedule_to_start_evidence(self) -> None:
        clock = _FakeClock(10.0)
        scheduler = DeterministicRemoteScheduler(clock=clock)
        scheduler.register_worker(worker("worker-1"))
        scheduler.admit(task("task-1"))
        clock.advance(2.5)
        decision = poll_current(scheduler, "worker-1")

        assert decision.assignment is not None
        self.assertEqual(decision.assignment.enqueued_at, 10.0)
        self.assertEqual(decision.assignment.claimed_at, 12.5)
        self.assertEqual(decision.assignment.schedule_to_start_seconds, 2.5)

    def test_wrong_worker_cannot_release_capacity(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=_FakeClock())
        scheduler.register_worker(worker("worker-1"))
        scheduler.register_worker(worker("worker-2"))
        scheduler.admit(task("task-1"))
        decision = poll_current(scheduler, "worker-1")
        assert decision.assignment is not None

        self.assertEqual(
            release_current(
                scheduler,
                decision.assignment.assignment_id,
                "worker-2",
            ),
            ReleaseOutcome.WRONG_WORKER,
        )
        self.assertEqual(scheduler.worker_snapshot("worker-1").active_count, 1)
        self.assertEqual(
            release_current(
                scheduler,
                decision.assignment.assignment_id,
                "worker-1",
            ),
            ReleaseOutcome.RELEASED,
        )
        self.assertEqual(
            release_current(
                scheduler,
                decision.assignment.assignment_id,
                "worker-1",
            ),
            ReleaseOutcome.UNKNOWN_ASSIGNMENT,
        )

    def test_withdraw_is_linearized_against_claim_and_releases_queue_capacity(self) -> None:
        scheduler = DeterministicRemoteScheduler(
            max_queued_tasks=1,
            clock=_FakeClock(),
        )
        scheduler.register_worker(worker("worker-1"))
        scheduler.admit(task("task-1"))
        barrier = threading.Barrier(3)
        results: dict[str, object] = {}

        def claim() -> None:
            barrier.wait(timeout=5)
            results["claim"] = poll_current(scheduler, "worker-1")

        def withdraw() -> None:
            barrier.wait(timeout=5)
            results["withdraw"] = scheduler.withdraw("task-1")

        claim_thread = threading.Thread(target=claim)
        withdraw_thread = threading.Thread(target=withdraw)
        claim_thread.start()
        withdraw_thread.start()
        barrier.wait(timeout=5)
        claim_thread.join(timeout=5)
        withdraw_thread.join(timeout=5)

        decision = results["claim"]
        withdrawal = results["withdraw"]
        if decision.assignment is None:
            self.assertEqual(decision.outcome, PollOutcome.NO_COMPATIBLE_TASK)
            self.assertEqual(withdrawal, WithdrawalOutcome.WITHDRAWN)
        else:
            self.assertEqual(decision.outcome, PollOutcome.CLAIMED)
            self.assertEqual(withdrawal, WithdrawalOutcome.ACTIVE)
            release_current(
                scheduler,
                decision.assignment.assignment_id,
                "worker-1",
            )
        self.assertEqual(
            scheduler.admit(task("task-2")).outcome,
            AdmissionOutcome.ADMITTED,
        )
        self.assertEqual(
            scheduler.withdraw("missing-task"),
            WithdrawalOutcome.NOT_FOUND,
        )

    def test_idle_ttl_touch_and_sweep_are_exact_with_fake_clock(self) -> None:
        clock = _FakeClock(10.0)
        scheduler = DeterministicRemoteScheduler(
            worker_idle_ttl_seconds=5.0,
            clock=clock,
        )
        registered = scheduler.register_worker(worker("worker-1"))
        self.assertEqual(registered.last_seen_at, 10.0)
        self.assertEqual(registered.idle_ttl_seconds, 5.0)

        clock.advance(4.0)
        self.assertEqual(scheduler.sweep_idle_workers().removed_idle_workers, 0)
        self.assertEqual(
            scheduler.touch_worker(
                "worker-1",
                worker_generation=registered.generation,
                session_id=registered.descriptor.session_id,
            ),
            TouchOutcome.ACCEPTED,
        )
        self.assertEqual(scheduler.worker_snapshot("worker-1").last_seen_at, 14.0)

        clock.advance(4.999)
        self.assertEqual(scheduler.sweep_idle_workers().removed_idle_workers, 0)
        clock.advance(0.001)
        report = scheduler.sweep_idle_workers()
        self.assertEqual(report.removed_idle_workers, 1)
        self.assertEqual(report.expired_busy_workers, 0)
        self.assertIsNone(scheduler.worker_snapshot("worker-1"))

    def test_busy_idle_worker_expires_without_releasing_assignment(self) -> None:
        clock = _FakeClock(0.0)
        scheduler = DeterministicRemoteScheduler(
            worker_idle_ttl_seconds=5.0,
            clock=clock,
        )
        registered = scheduler.register_worker(worker("worker-1"))
        scheduler.admit(task("task-1"))
        decision = poll_current(scheduler, "worker-1")
        assert decision.assignment is not None
        clock.advance(5.0)

        report = scheduler.sweep_idle_workers()
        expired = scheduler.worker_snapshot("worker-1")
        assert expired is not None
        self.assertEqual(report.expired_busy_workers, 1)
        self.assertEqual(report.retained_assignments, 1)
        self.assertEqual(expired.lifecycle, WorkerLifecycle.EXPIRED)
        self.assertEqual(expired.active_count, 1)
        self.assertEqual(scheduler.snapshot().active_assignments, 1)
        self.assertEqual(
            scheduler.poll_and_claim(
                "worker-1",
                worker_generation=registered.generation,
                session_id=registered.descriptor.session_id,
            ).outcome,
            PollOutcome.WORKER_EXPIRED,
        )
        with self.assertRaisesRegex(RemoteSchedulingConflict, "active assignments"):
            scheduler.register_worker(
                worker("worker-1", session_id="replacement-session")
            )

        self.assertEqual(
            release_current(
                scheduler,
                decision.assignment.assignment_id,
                "worker-1",
            ),
            ReleaseOutcome.RELEASED,
        )
        self.assertIsNone(scheduler.worker_snapshot("worker-1"))
        self.assertEqual(scheduler.snapshot().active_assignments, 0)

    def test_old_generation_and_wrong_session_cannot_touch_or_release(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=_FakeClock())
        first = scheduler.register_worker(worker("worker-1"))
        scheduler.request_drain("worker-1")
        current = scheduler.reactivate_worker("worker-1")
        self.assertGreater(current.generation, first.generation)
        scheduler.admit(task("task-1"))
        decision = poll_current(scheduler, "worker-1")
        assert decision.assignment is not None

        self.assertEqual(
            scheduler.touch_worker(
                "worker-1",
                worker_generation=first.generation,
                session_id=first.descriptor.session_id,
            ),
            TouchOutcome.STALE_WORKER_GENERATION,
        )
        self.assertEqual(
            scheduler.touch_worker(
                "worker-1",
                worker_generation=current.generation,
                session_id="wrong-session",
            ),
            TouchOutcome.WORKER_SESSION_MISMATCH,
        )
        self.assertEqual(
            scheduler.release(
                decision.assignment.assignment_id,
                "worker-1",
                worker_generation=first.generation,
                session_id=first.descriptor.session_id,
            ),
            ReleaseOutcome.STALE_WORKER_GENERATION,
        )
        self.assertEqual(
            scheduler.release(
                decision.assignment.assignment_id,
                "worker-1",
                worker_generation=current.generation,
                session_id="wrong-session",
            ),
            ReleaseOutcome.WORKER_SESSION_MISMATCH,
        )
        self.assertEqual(scheduler.snapshot().active_assignments, 1)
        self.assertEqual(
            release_current(
                scheduler,
                decision.assignment.assignment_id,
                "worker-1",
            ),
            ReleaseOutcome.RELEASED,
        )

    def test_registration_replacement_and_sweep_do_not_reuse_generation(self) -> None:
        clock = _FakeClock()
        scheduler = DeterministicRemoteScheduler(
            worker_idle_ttl_seconds=1.0,
            clock=clock,
        )
        first = scheduler.register_worker(
            worker("worker-1", session_id="session-1")
        )
        same = scheduler.register_worker(
            worker("worker-1", session_id="session-1")
        )
        self.assertEqual(same.generation, first.generation)
        replacement = scheduler.register_worker(
            worker("worker-1", session_id="session-2")
        )
        self.assertGreater(replacement.generation, first.generation)
        self.assertEqual(
            scheduler.touch_worker(
                "worker-1",
                worker_generation=first.generation,
                session_id="session-1",
            ),
            TouchOutcome.STALE_WORKER_GENERATION,
        )

        clock.advance(1.0)
        self.assertEqual(scheduler.sweep_idle_workers().removed_idle_workers, 1)
        after_sweep = scheduler.register_worker(
            worker("worker-1", session_id="session-3")
        )
        self.assertGreater(after_sweep.generation, replacement.generation)

    def test_touch_and_sweep_race_has_only_two_linearizable_outcomes(self) -> None:
        clock = _FakeClock(0.0)
        scheduler = DeterministicRemoteScheduler(
            worker_idle_ttl_seconds=1.0,
            clock=clock,
        )
        registered = scheduler.register_worker(worker("worker-1"))
        clock.advance(1.0)
        barrier = threading.Barrier(3)
        results: dict[str, object] = {}

        def touch() -> None:
            barrier.wait(timeout=5)
            results["touch"] = scheduler.touch_worker(
                "worker-1",
                worker_generation=registered.generation,
                session_id=registered.descriptor.session_id,
            )

        def sweep() -> None:
            barrier.wait(timeout=5)
            results["sweep"] = scheduler.sweep_idle_workers()

        touch_thread = threading.Thread(target=touch)
        sweep_thread = threading.Thread(target=sweep)
        touch_thread.start()
        sweep_thread.start()
        barrier.wait(timeout=5)
        touch_thread.join(timeout=5)
        sweep_thread.join(timeout=5)

        report = results["sweep"]
        if results["touch"] is TouchOutcome.ACCEPTED:
            self.assertEqual(report.removed_idle_workers, 0)
            self.assertIsNotNone(scheduler.worker_snapshot("worker-1"))
        else:
            self.assertEqual(results["touch"], TouchOutcome.UNKNOWN_WORKER)
            self.assertEqual(report.removed_idle_workers, 1)
            self.assertIsNone(scheduler.worker_snapshot("worker-1"))


if __name__ == "__main__":
    unittest.main()
