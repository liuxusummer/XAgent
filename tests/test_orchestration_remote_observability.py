from __future__ import annotations

import dataclasses
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from src.orchestration.remote_observability import (
    BoundedRemoteObservability,
    CancellationObservation,
    HeartbeatObservation,
)
from src.orchestration.remote_scheduling import PollOutcome, WorkerLifecycle


class _FakeClock:
    def __init__(self, value: float = 50.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


def _point_map(snapshot):
    return {
        (point.name, point.pool, point.outcome, point.kind): point.value
        for point in snapshot.points
    }


class RemoteObservabilityTests(unittest.TestCase):
    def test_snapshot_covers_required_signals_without_becoming_truth(self) -> None:
        clock = _FakeClock()
        observations = BoundedRemoteObservability(clock=clock)
        observations.record_poll("pool-a", PollOutcome.CLAIMED)
        observations.record_worker_registration("pool-a", WorkerLifecycle.ACTIVE)
        observations.record_worker_registration("pool-a", WorkerLifecycle.DRAINING)
        observations.record_worker_registration("pool-a", WorkerLifecycle.EXPIRED)
        observations.record_heartbeat("pool-a", HeartbeatObservation.ACCEPTED)
        observations.record_heartbeat("pool-a", HeartbeatObservation.REJECTED)
        observations.record_claim(
            "pool-a",
            schedule_to_start_seconds=0.25,
        )
        observations.record_lease_loss("pool-a")
        observations.record_stale_commit("pool-a")
        observations.record_cancellation(
            "pool-a",
            CancellationObservation.ACKNOWLEDGED,
        )
        observations.record_outcome_unknown("pool-a")
        observations.observe_queue_depth("pool-a", 17)

        snapshot = observations.snapshot()
        points = _point_map(snapshot)
        pool = snapshot.points[0].pool

        self.assertFalse(snapshot.execution_truth)
        self.assertEqual(snapshot.delivery_semantics, "best_effort")
        self.assertEqual(snapshot.generated_at, 50.0)
        self.assertEqual(points[("poll", pool, "claimed", "counter")], 1)
        self.assertEqual(
            points[("worker_registration", pool, "active", "counter")],
            1,
        )
        self.assertEqual(
            points[("worker_registration", pool, "draining", "counter")],
            1,
        )
        self.assertEqual(
            points[("worker_registration", pool, "expired", "counter")],
            1,
        )
        self.assertEqual(
            points[("heartbeat", pool, "accepted", "counter")],
            1,
        )
        self.assertEqual(
            points[("heartbeat", pool, "rejected", "counter")],
            1,
        )
        self.assertEqual(points[("claim", pool, "claimed", "counter")], 1)
        self.assertEqual(points[("lease_loss", pool, "lost", "counter")], 1)
        self.assertEqual(
            points[("stale_commit", pool, "rejected", "counter")],
            1,
        )
        self.assertEqual(
            points[("cancellation", pool, "acknowledged", "counter")],
            1,
        )
        self.assertEqual(
            points[("outcome_unknown", pool, "recorded", "counter")],
            1,
        )
        self.assertEqual(
            points[("queue_depth", pool, "observed", "gauge")],
            17,
        )

    def test_caller_identifiers_are_never_exposed_in_snapshot(self) -> None:
        secret_like_pool = "pool-SUPER-SECRET-TOKEN"
        observations = BoundedRemoteObservability()
        observations.record_poll(secret_like_pool, PollOutcome.NO_COMPATIBLE_TASK)
        observations.observe_queue_depth(secret_like_pool, 1)

        rendered = repr(observations.snapshot())

        self.assertNotIn(secret_like_pool, rendered)
        self.assertNotIn("SUPER-SECRET-TOKEN", rendered)
        self.assertIn("pool_", rendered)

    def test_pool_cardinality_is_bounded_with_fixed_overflow_bucket(self) -> None:
        observations = BoundedRemoteObservability(max_pool_dimensions=2)
        for pool_id in ("pool-a", "pool-b", "pool-c", "pool-d"):
            observations.record_poll(pool_id, PollOutcome.NO_COMPATIBLE_TASK)
            observations.observe_queue_depth(pool_id, 1)

        snapshot = observations.snapshot()
        pools = {point.pool for point in snapshot.points}

        self.assertEqual(snapshot.tracked_pool_dimensions, 2)
        self.assertEqual(snapshot.max_pool_dimensions, 2)
        self.assertEqual(snapshot.dropped_dimensions, 4)
        self.assertLessEqual(len(pools), 3)
        self.assertIn("pool_overflow", pools)
        self.assertEqual(len(snapshot.points), 6)

    def test_many_untrusted_pool_values_keep_snapshot_cardinality_constant(self) -> None:
        observations = BoundedRemoteObservability(max_pool_dimensions=2)
        for index in range(10_000):
            observations.record_poll(
                f"untrusted-pool-{index}",
                PollOutcome.NO_COMPATIBLE_TASK,
            )

        snapshot = observations.snapshot()
        self.assertEqual(snapshot.tracked_pool_dimensions, 2)
        self.assertEqual(len(snapshot.points), 3)
        self.assertEqual(snapshot.dropped_dimensions, 9_998)

    def test_schedule_to_start_uses_fixed_histogram_not_raw_samples(self) -> None:
        observations = BoundedRemoteObservability()
        for duration in (0.005, 0.1, 1.1, 61.0):
            observations.record_claim(
                "pool-a",
                schedule_to_start_seconds=duration,
            )
        snapshot = observations.snapshot()
        points = _point_map(snapshot)
        pool = snapshot.points[0].pool

        self.assertEqual(points[("claim", pool, "claimed", "counter")], 4)
        self.assertEqual(
            points[("schedule_to_start_count", pool, "observed", "counter")],
            4,
        )
        self.assertEqual(
            points[("schedule_to_start_sum_ms", pool, "observed", "counter")],
            62_205,
        )
        self.assertEqual(
            points[("schedule_to_start_bucket", pool, "le_0.01s", "counter")],
            1,
        )
        self.assertEqual(
            points[("schedule_to_start_bucket", pool, "le_0.1s", "counter")],
            1,
        )
        self.assertEqual(
            points[("schedule_to_start_bucket", pool, "le_10s", "counter")],
            1,
        )
        self.assertEqual(
            points[("schedule_to_start_bucket", pool, "gt_60s", "counter")],
            1,
        )
        self.assertNotIn("0.005", repr(snapshot))
        self.assertNotIn("61.0", repr(snapshot))

    def test_invalid_observations_are_contained_and_counted(self) -> None:
        observations = BoundedRemoteObservability(clock=lambda: float("nan"))

        observations.record_poll("", "invented")
        observations.record_claim(
            "pool-a",
            schedule_to_start_seconds=float("inf"),
        )
        observations.record_cancellation("pool-a", "invented")
        observations.observe_queue_depth("pool-a", -1)
        snapshot = observations.snapshot()

        self.assertFalse(snapshot.execution_truth)
        self.assertEqual(snapshot.generated_at, 0.0)
        self.assertGreaterEqual(snapshot.invalid_observations, 6)
        rendered = repr(snapshot)
        self.assertNotIn("invented", rendered)

    def test_pathological_values_cannot_escape_or_create_unbounded_integer(self) -> None:
        class _ExplodingClock:
            def __call__(self):
                raise RuntimeError("telemetry clock unavailable")

        observations = BoundedRemoteObservability(clock=_ExplodingClock())
        observations.record_poll("\ud800", PollOutcome.CLAIMED)
        observations.record_claim(
            "pool-a",
            schedule_to_start_seconds=1e308,
        )

        snapshot = observations.snapshot()
        points = _point_map(snapshot)
        sum_point = next(
            value
            for (name, _pool, _outcome, _kind), value in points.items()
            if name == "schedule_to_start_sum_ms"
        )
        self.assertEqual(sum_point, (1 << 63) - 1)
        self.assertEqual(snapshot.generated_at, 0.0)
        self.assertGreaterEqual(snapshot.invalid_observations, 2)

    def test_concurrent_updates_are_exact_and_thread_safe(self) -> None:
        observations = BoundedRemoteObservability()
        barrier = threading.Barrier(17)

        def update() -> None:
            barrier.wait(timeout=5)
            for _index in range(250):
                observations.record_poll("pool-a", PollOutcome.CLAIMED)
                observations.record_stale_commit("pool-a")

        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(update) for _index in range(16)]
            barrier.wait(timeout=5)
            for future in futures:
                future.result(timeout=5)

        snapshot = observations.snapshot()
        points = _point_map(snapshot)
        pool = snapshot.points[0].pool
        self.assertEqual(points[("poll", pool, "claimed", "counter")], 4_000)
        self.assertEqual(
            points[("stale_commit", pool, "rejected", "counter")],
            4_000,
        )

    def test_queue_depth_is_a_replaceable_gauge_and_is_capped(self) -> None:
        observations = BoundedRemoteObservability()
        observations.observe_queue_depth("pool-a", 2)
        observations.observe_queue_depth("pool-a", 7)
        observations.observe_queue_depth("pool-a", 1 << 40)

        snapshot = observations.snapshot()
        point = next(point for point in snapshot.points if point.name == "queue_depth")
        self.assertEqual(point.kind, "gauge")
        self.assertEqual(point.value, (1 << 31) - 1)

    def test_snapshot_is_immutable_and_has_no_identity_fields(self) -> None:
        observations = BoundedRemoteObservability()
        observations.record_poll("pool-a", PollOutcome.CLAIMED)
        snapshot = observations.snapshot()

        with self.assertRaises(dataclasses.FrozenInstanceError):
            snapshot.execution_truth = True
        metric_fields = {field.name for field in dataclasses.fields(snapshot.points[0])}
        self.assertFalse(
            {"tenant_id", "worker_id", "task_id", "run_id", "attempt_id"}
            & metric_fields
        )

    def test_exporter_exception_is_contained_and_does_not_mutate_counters(self) -> None:
        observations = BoundedRemoteObservability()
        observations.record_worker_registration("pool-a", WorkerLifecycle.ACTIVE)
        observations.record_heartbeat("pool-a", HeartbeatObservation.ACCEPTED)
        before = observations.snapshot()

        def failing_exporter(_point) -> None:
            raise RuntimeError("collector unavailable")

        report = observations.export_snapshot(failing_exporter)
        after = observations.snapshot()

        self.assertEqual(report.attempted, len(before.points))
        self.assertEqual(report.failures, len(before.points))
        self.assertFalse(report.execution_truth)
        self.assertEqual(after.points, before.points)


if __name__ == "__main__":
    unittest.main()
