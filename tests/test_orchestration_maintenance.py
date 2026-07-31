from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from src.orchestration.deadline import (
    DeadlineReport,
    DurableDeadlineScanner,
)
from src.orchestration.lease import (
    DurableLeaseReaper,
    RecoveryReport,
    RecoveryRetryPolicy,
)
from src.orchestration.maintenance import (
    DurableMaintenanceSupervisor,
    MaintenanceConfigurationError,
    MaintenanceCycleConflict,
    MaintenanceNotBootstrapped,
)
from src.orchestration.models import NodeStatus, RunRecord, RunStatus
from src.orchestration.remote_fleet import FleetQueueReconcileReport
from src.orchestration.remote_fleet_control import (
    FleetTerminalReconcileReport,
)
from src.orchestration.remote_fleet_reconcile import (
    FleetProjectionReconcileReport,
)
from src.orchestration.scheduler import DurableScheduler
from src.orchestration.store import DurableRunStore
from src.orchestration.workflow import compile_workflow


class _Clock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class _Ids:
    def __init__(self) -> None:
        self._value = 0

    def __call__(self, prefix: str) -> str:
        self._value += 1
        return f"{prefix}-maintenance-{self._value}"


class _DeadlineScanner(DurableDeadlineScanner):
    def __init__(
        self,
        store: DurableRunStore,
        events: list[str],
        *,
        report: DeadlineReport | None = None,
        error: BaseException | None = None,
        entered: threading.Event | None = None,
        release: threading.Event | None = None,
    ) -> None:
        super().__init__(store)
        self.events = events
        self.report = report
        self.error = error
        self.entered = entered
        self.release = release

    def run_once(self, *, now: float, limit: int = 100) -> DeadlineReport:
        self.events.append("deadline")
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            self.release.wait(timeout=5)
        if self.error is not None:
            raise self.error
        if self.report is not None:
            return self.report
        return super().run_once(now=now, limit=limit)


class _LeaseReaper(DurableLeaseReaper):
    def __init__(
        self,
        store: DurableRunStore,
        events: list[str],
    ) -> None:
        super().__init__(store)
        self.events = events

    def run_once(self, *, now: float, limit: int = 100) -> RecoveryReport:
        self.events.append("lease")
        return super().run_once(now=now, limit=limit)


class _FleetControl:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.ready = True
        self.fail_run = False
        self.fail_quarantine = False
        self.queued = 3

    @property
    def production_security_ready(self) -> bool:
        return self.ready

    def run_once(self) -> FleetProjectionReconcileReport:
        self.events.append("fleet")
        if self.fail_run:
            raise RuntimeError("fleet-secret-should-not-escape")
        return FleetProjectionReconcileReport(
            routes_examined=2,
            schedulers_resolved=2,
            desired_tasks=self.queued,
            restored_pools=1,
            terminal=FleetTerminalReconcileReport(0, 0, 0, 0, 0),
            queue=FleetQueueReconcileReport(
                self.queued,
                0,
                0,
                self.queued,
                0,
                0,
                self.queued,
            ),
        )

    def quarantine(self) -> FleetQueueReconcileReport:
        self.events.append("quarantine")
        if self.fail_quarantine:
            raise RuntimeError("quarantine-secret-should-not-escape")
        withdrawn = self.queued
        self.queued = 0
        return FleetQueueReconcileReport(
            0,
            0,
            withdrawn,
            0,
            0,
            0,
            0,
        )


class DurableMaintenanceSupervisorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.store = DurableRunStore(self.root / "orchestration.sqlite3")
        self.wall_clock = _Clock(10)
        self.monotonic = _Clock(100)
        self.workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": "maintenance-test",
                "version": 1,
                "nodes": [
                    {
                        "id": "activity",
                        "kind": "tool",
                        "config": {
                            "tool": "inspect",
                            "arguments": {},
                        },
                        "effect_class": "read_only",
                    }
                ],
            }
        )
        self.scheduler = DurableScheduler(
            self.store,
            self.workflow,
            clock=self.wall_clock,
            id_factory=_Ids(),
        )

    def _supervisor(
        self,
        *,
        deadline_scanner: DurableDeadlineScanner | None = None,
        lease_reaper: DurableLeaseReaper | None = None,
        fleet_control: _FleetControl | None = None,
        resolver=None,
        scan_limit: int = 100,
        max_active_runs: int = 100,
        max_staleness_seconds: float = 30,
    ) -> DurableMaintenanceSupervisor:
        return DurableMaintenanceSupervisor(
            self.store,
            resolver or (lambda _run: self.scheduler),
            deadline_scanner=deadline_scanner,
            lease_reaper=lease_reaper,
            fleet_control=fleet_control,
            wall_clock=self.wall_clock,
            monotonic_clock=self.monotonic,
            scan_limit=scan_limit,
            max_active_runs=max_active_runs,
            max_staleness_seconds=max_staleness_seconds,
        )

    def test_bootstrap_recovers_all_active_runs_before_readiness(self) -> None:
        self.scheduler.create_run("restart-run")
        supervisor = self._supervisor()

        self.assertFalse(supervisor.production_security_ready)
        report = supervisor.bootstrap()

        self.assertTrue(report.healthy)
        self.assertEqual(report.mode, "bootstrap")
        self.assertEqual(report.active_runs_scanned, 1)
        self.assertEqual(report.runs_reconciled, 1)
        self.assertEqual(
            self.store.get_run("restart-run").status,
            RunStatus.RUNNING,
        )
        self.assertTrue(supervisor.production_security_ready)

        restarted = self._supervisor()
        self.assertFalse(restarted.production_security_ready)
        with self.assertRaises(MaintenanceNotBootstrapped):
            restarted.run_once()
        self.assertTrue(restarted.bootstrap().healthy)

    def test_steady_cycle_recovers_late_created_and_cancel_intents(
        self,
    ) -> None:
        supervisor = self._supervisor(scan_limit=10)
        self.assertTrue(supervisor.bootstrap().healthy)
        self.scheduler.create_run("late-created")
        self.scheduler.create_run("late-cancelling")
        self.scheduler.request_cancel(
            "late-cancelling",
            reconcile=False,
        )
        self.monotonic.now = 101

        report = supervisor.run_once()

        self.assertTrue(report.healthy)
        self.assertEqual(report.control_runs_scanned, 2)
        self.assertEqual(report.runs_reconciled, 2)
        self.assertEqual(
            self.store.get_run("late-created").status,
            RunStatus.RUNNING,
        )
        self.assertEqual(
            self.store.get_run("late-cancelling").status,
            RunStatus.CANCELLED,
        )

    def test_cycle_order_is_deadline_lease_domain_then_fleet(self) -> None:
        events: list[str] = []
        self.scheduler.create_run("ordered-run")
        scanner = _DeadlineScanner(self.store, events)
        reaper = _LeaseReaper(self.store, events)
        fleet = _FleetControl(events)

        def resolve(_run):
            events.append("domain")
            return self.scheduler

        report = self._supervisor(
            deadline_scanner=scanner,
            lease_reaper=reaper,
            fleet_control=fleet,
            resolver=resolve,
        ).bootstrap()

        self.assertTrue(report.healthy)
        self.assertEqual(events, ["deadline", "lease", "domain", "fleet"])
        self.assertEqual(report.fleet_routes_examined, 2)
        self.assertEqual(report.fleet_desired_tasks, 3)

    def test_expired_lease_is_recovered_before_domain_reconcile(self) -> None:
        self.wall_clock.now = 1
        self.scheduler.create_run("expired-run")
        claim = self.scheduler.claim_next(
            "expired-run",
            "worker",
            lease_seconds=5,
        )
        self.assertIsNotNone(claim)
        self.wall_clock.now = 10

        report = self._supervisor(scan_limit=10).bootstrap()

        self.assertTrue(report.healthy)
        self.assertEqual(report.leases_scanned, 1)
        self.assertEqual(report.leases_resolved, 1)
        self.assertEqual(report.runs_reconciled, 1)
        self.assertEqual(
            self.store.get_run("expired-run").status,
            RunStatus.FAILED,
        )

    def test_steady_cycle_wakes_due_retry_without_another_lease(self) -> None:
        self.wall_clock.now = 1
        self.scheduler.create_run("retry-run")
        claim = self.scheduler.claim_next(
            "retry-run",
            "worker",
            lease_seconds=5,
        )
        self.assertIsNotNone(claim)
        self.wall_clock.now = 10
        reaper = DurableLeaseReaper(
            self.store,
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(
                max_attempts=2,
                retry_delay_seconds=5,
            ),
        )
        supervisor = self._supervisor(
            lease_reaper=reaper,
            scan_limit=10,
        )

        recovered = supervisor.bootstrap()
        self.assertTrue(recovered.healthy)
        self.assertEqual(
            self.store.get_node("retry-run", "activity").status,
            NodeStatus.WAITING_RETRY,
        )

        self.wall_clock.now = 15
        self.monotonic.now = 101
        due = supervisor.run_once()

        self.assertTrue(due.healthy)
        self.assertEqual(due.leases_scanned, 0)
        self.assertEqual(due.due_retry_runs_scanned, 1)
        self.assertEqual(due.runs_reconciled, 1)
        self.assertEqual(
            self.store.get_node("retry-run", "activity").status,
            NodeStatus.READY,
        )

    def test_malformed_retry_projection_fails_closed_without_leaking(
        self,
    ) -> None:
        self.wall_clock.now = 1
        self.scheduler.create_run("corrupt-retry-run")
        self.scheduler.claim_next(
            "corrupt-retry-run",
            "worker",
            lease_seconds=5,
        )
        self.wall_clock.now = 10
        reaper = DurableLeaseReaper(
            self.store,
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(
                max_attempts=2,
                retry_delay_seconds=5,
            ),
        )
        supervisor = self._supervisor(
            lease_reaper=reaper,
            scan_limit=10,
        )
        self.assertTrue(supervisor.bootstrap().healthy)
        with sqlite3.connect(self.store.path) as conn:
            conn.execute(
                """
                UPDATE node_runs
                SET metadata_json = ?
                WHERE run_id = ? AND node_id = ?
                """,
                (
                    '{"retry_due_at":"api_key=retry-super-secret"}',
                    "corrupt-retry-run",
                    "activity",
                ),
            )

        self.monotonic.now = 101
        report = supervisor.run_once()

        self.assertFalse(report.healthy)
        self.assertEqual(report.due_retry_runs_scanned, 1)
        self.assertEqual(
            report.failures[0].code,
            "domain_reconcile_failed",
        )
        self.assertNotIn("retry-super-secret", repr(report))

    def test_scheduler_clock_skew_cannot_report_due_retry_healthy(
        self,
    ) -> None:
        scheduler_clock = _Clock(1)
        skewed_scheduler = DurableScheduler(
            self.store,
            self.workflow,
            clock=scheduler_clock,
            id_factory=_Ids(),
        )
        skewed_scheduler.create_run("skewed-retry-run")
        skewed_scheduler.claim_next(
            "skewed-retry-run",
            "worker",
            lease_seconds=5,
        )
        self.wall_clock.now = 10
        reaper = DurableLeaseReaper(
            self.store,
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(
                max_attempts=2,
                retry_delay_seconds=5,
            ),
        )
        supervisor = self._supervisor(
            lease_reaper=reaper,
            resolver=lambda _run: skewed_scheduler,
            scan_limit=10,
        )
        self.assertTrue(supervisor.bootstrap().healthy)
        self.wall_clock.now = 15
        self.monotonic.now = 101

        report = supervisor.run_once()

        self.assertFalse(report.healthy)
        self.assertEqual(
            report.failures[0].code,
            "due_retry_not_converged",
        )
        self.assertEqual(
            self.store.get_node(
                "skewed-retry-run",
                "activity",
            ).status,
            NodeStatus.WAITING_RETRY,
        )

    def test_scan_limit_quarantines_fleet_until_a_clear_pass(self) -> None:
        events: list[str] = []
        saturated = DeadlineReport(1, (), (), (), ())
        scanner = _DeadlineScanner(
            self.store,
            events,
            report=saturated,
        )
        fleet = _FleetControl(events)
        supervisor = self._supervisor(
            deadline_scanner=scanner,
            fleet_control=fleet,
            scan_limit=1,
        )

        report = supervisor.bootstrap()

        self.assertFalse(report.healthy)
        self.assertTrue(report.backlog_possible)
        self.assertTrue(report.quarantined)
        self.assertEqual(report.quarantine_withdrawn_tasks, 3)
        self.assertEqual(
            report.failures[0].code,
            "scan_limit_reached",
        )
        self.assertFalse(supervisor.production_security_ready)
        self.assertNotIn("fleet", events)

        scanner.report = DeadlineReport(0, (), (), (), ())
        self.monotonic.now = 101
        recovered = supervisor.bootstrap()
        self.assertTrue(recovered.healthy)
        self.assertTrue(supervisor.production_security_ready)

    def test_run_deadline_saturation_also_quarantines_fleet(self) -> None:
        events: list[str] = []
        self.wall_clock.now = 1
        self.scheduler.create_run("deadline-run-1", deadline_at=5)
        self.scheduler.create_run("deadline-run-2", deadline_at=5)
        self.wall_clock.now = 10
        fleet = _FleetControl(events)

        report = self._supervisor(
            fleet_control=fleet,
            scan_limit=1,
        ).bootstrap()

        self.assertFalse(report.healthy)
        self.assertEqual(report.deadline_scanned, 0)
        self.assertEqual(report.run_deadlines_scanned, 1)
        self.assertTrue(report.backlog_possible)
        self.assertTrue(report.quarantined)
        self.assertNotIn("fleet", events)

    def test_failure_report_redacts_exception_and_quarantines(self) -> None:
        events: list[str] = []
        scanner = _DeadlineScanner(
            self.store,
            events,
            error=RuntimeError(
                "api_key=maintenance-super-secret /private/workspace"
            ),
        )
        fleet = _FleetControl(events)
        report = self._supervisor(
            deadline_scanner=scanner,
            fleet_control=fleet,
        ).bootstrap()

        rendered = repr(report)
        self.assertFalse(report.healthy)
        self.assertTrue(report.quarantined)
        self.assertEqual(
            report.failures[0].code,
            "deadline_scan_failed",
        )
        self.assertNotIn("maintenance-super-secret", rendered)
        self.assertNotIn("/private/workspace", rendered)
        self.assertEqual(events, ["deadline", "quarantine"])

    def test_quarantine_failure_is_bounded_and_visible(self) -> None:
        events: list[str] = []
        scanner = _DeadlineScanner(
            self.store,
            events,
            error=RuntimeError("failure"),
        )
        fleet = _FleetControl(events)
        fleet.fail_quarantine = True

        report = self._supervisor(
            deadline_scanner=scanner,
            fleet_control=fleet,
        ).bootstrap()

        self.assertFalse(report.quarantined)
        self.assertEqual(
            tuple(failure.code for failure in report.failures),
            ("deadline_scan_failed", "quarantine_failed"),
        )
        self.assertNotIn("quarantine-secret", repr(report))

    def test_fleet_failure_runs_its_explicit_quarantine(self) -> None:
        events: list[str] = []
        fleet = _FleetControl(events)
        fleet.fail_run = True

        report = self._supervisor(
            fleet_control=fleet,
        ).bootstrap()

        self.assertFalse(report.healthy)
        self.assertTrue(report.quarantined)
        self.assertEqual(
            report.failures[0].code,
            "fleet_reconcile_failed",
        )
        self.assertEqual(events, ["fleet", "quarantine"])

    def test_active_run_capacity_fails_closed_before_resolution(self) -> None:
        events: list[str] = []
        self.scheduler.create_run("run-1")
        self.scheduler.create_run("run-2")
        fleet = _FleetControl(events)
        resolver_calls = 0

        def resolve(_run):
            nonlocal resolver_calls
            resolver_calls += 1
            return self.scheduler

        report = self._supervisor(
            fleet_control=fleet,
            resolver=resolve,
            max_active_runs=1,
        ).bootstrap()

        self.assertFalse(report.healthy)
        self.assertEqual(resolver_calls, 0)
        self.assertEqual(
            report.failures[0].code,
            "active_run_scan_failed",
        )
        self.assertEqual(events, ["quarantine"])

    def test_foreign_scheduler_store_fails_closed(self) -> None:
        self.scheduler.create_run("foreign-run")
        foreign_store = DurableRunStore(self.root / "foreign.sqlite3")
        foreign_scheduler = DurableScheduler(
            foreign_store,
            self.workflow,
        )

        report = self._supervisor(
            resolver=lambda _run: foreign_scheduler,
        ).bootstrap()

        self.assertFalse(report.healthy)
        self.assertEqual(
            report.failures[0].code,
            "domain_reconcile_failed",
        )

    def test_freshness_and_fleet_readiness_are_rechecked(self) -> None:
        events: list[str] = []
        fleet = _FleetControl(events)
        supervisor = self._supervisor(
            fleet_control=fleet,
            max_staleness_seconds=30,
        )
        self.assertTrue(supervisor.bootstrap().healthy)
        self.assertTrue(supervisor.production_security_ready)

        self.monotonic.now = 131
        self.assertFalse(supervisor.production_security_ready)
        self.monotonic.now = 100
        fleet.ready = False
        self.assertFalse(supervisor.production_security_ready)

    def test_overlapping_cycles_are_rejected_without_corrupting_state(
        self,
    ) -> None:
        events: list[str] = []
        entered = threading.Event()
        release = threading.Event()
        scanner = _DeadlineScanner(
            self.store,
            events,
            entered=entered,
            release=release,
        )
        supervisor = self._supervisor(deadline_scanner=scanner)
        reports = []

        thread = threading.Thread(
            target=lambda: reports.append(supervisor.bootstrap()),
        )
        thread.start()
        self.assertTrue(entered.wait(timeout=5))
        with self.assertRaises(MaintenanceCycleConflict):
            supervisor.bootstrap()
        release.set()
        thread.join(timeout=5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(reports), 1)
        self.assertTrue(reports[0].healthy)
        self.assertTrue(supervisor.production_security_ready)

    def test_parent_runs_are_ordered_before_children(self) -> None:
        parent = RunRecord(
            "parent",
            "workflow",
            definition_digest="a" * 64,
            created_at=2,
            updated_at=2,
        )
        child = RunRecord(
            "child",
            "workflow",
            definition_digest="b" * 64,
            metadata={
                "hierarchy_link": {
                    "parent_run_id": "parent",
                }
            },
            created_at=1,
            updated_at=1,
        )

        ordered = DurableMaintenanceSupervisor._ordered_runs(
            {"child": child, "parent": parent}
        )

        self.assertEqual(
            tuple(run.run_id for run in ordered),
            ("parent", "child"),
        )

    def test_constructor_rejects_unbounded_or_foreign_components(self) -> None:
        foreign = DurableRunStore(self.root / "foreign.sqlite3")
        with self.assertRaises(MaintenanceConfigurationError):
            self._supervisor(
                deadline_scanner=DurableDeadlineScanner(foreign)
            )
        with self.assertRaises(MaintenanceConfigurationError):
            self._supervisor(scan_limit=0)
        with self.assertRaises(MaintenanceConfigurationError):
            self._supervisor(max_active_runs=10_001)
        with self.assertRaises(MaintenanceConfigurationError):
            self._supervisor(max_staleness_seconds=float("inf"))


if __name__ == "__main__":
    unittest.main()
