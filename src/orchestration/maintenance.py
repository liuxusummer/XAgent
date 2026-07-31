"""Explicit production maintenance cycles for durable orchestration.

The supervisor owns no Domain truth and starts no background thread.  A
deployment must call :meth:`bootstrap` before admitting work, then invoke
:meth:`run_once` often enough for ``production_security_ready`` to remain
fresh.  Every cycle derives work from the durable Store and fails closed by
quarantining queued Fleet projections when recovery cannot be proved.
"""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from .deadline import DeadlineReport, DurableDeadlineScanner
from .lease import DurableLeaseReaper, RecoveryReport
from .models import RunRecord, RunStatus
from .remote_fleet import FleetQueueReconcileReport
from .remote_fleet_reconcile import (
    FleetProjectionReconcileReport,
)
from .scheduler import DurableScheduler
from .store import DurableRunStore

MAX_MAINTENANCE_SCAN_LIMIT = 1_000
MAX_MAINTENANCE_ACTIVE_RUNS = 10_000
MAX_MAINTENANCE_STALENESS_SECONDS = 24 * 60 * 60


class MaintenanceConfigurationError(ValueError):
    """A maintenance component or safety bound is invalid."""


class MaintenanceCycleConflict(RuntimeError):
    """The same supervisor instance already has a cycle in flight."""


class MaintenanceNotBootstrapped(RuntimeError):
    """Steady-state maintenance was requested before restart recovery."""


@runtime_checkable
class MaintenanceFleetControl(Protocol):
    """Minimal trusted Fleet surface used by the maintenance supervisor."""

    @property
    def production_security_ready(self) -> bool: ...

    def run_once(self) -> FleetProjectionReconcileReport: ...

    def quarantine(self) -> FleetQueueReconcileReport: ...


@dataclass(frozen=True, slots=True)
class MaintenanceFailure:
    """Bounded operator-safe failure evidence without exception text."""

    stage: str
    code: str


@dataclass(frozen=True, slots=True)
class MaintenanceCycleReport:
    """Content-free summary of one explicit maintenance cycle."""

    mode: str
    sequence: int
    healthy: bool
    active_runs_scanned: int
    deadline_scanned: int
    run_deadlines_scanned: int
    deadline_resolved: int
    deadline_races: int
    deadline_triggered_runs: int
    leases_scanned: int
    leases_resolved: int
    lease_races: int
    due_retry_runs_scanned: int
    control_runs_scanned: int
    hierarchy_runs_scanned: int
    runs_reconciled: int
    fleet_routes_examined: int
    fleet_desired_tasks: int
    backlog_possible: bool
    quarantined: bool
    quarantine_withdrawn_tasks: int
    quarantine_deferred_active_tasks: int
    failures: tuple[MaintenanceFailure, ...]
    execution_truth: bool = False


class _ActiveRunLimitExceeded(RuntimeError):
    pass


class _InvalidComponentReport(RuntimeError):
    pass


class DurableMaintenanceSupervisor:
    """Compose restart recovery and steady-state maintenance fail closed.

    ``sequence`` and freshness are process-local diagnostics only.  Events,
    projections, leases, hierarchy links, and Fleet admission authority remain
    exclusively durable Store state.
    """

    def __init__(
        self,
        store: DurableRunStore,
        scheduler_resolver: Callable[[RunRecord], DurableScheduler],
        *,
        deadline_scanner: DurableDeadlineScanner | None = None,
        lease_reaper: DurableLeaseReaper | None = None,
        fleet_control: MaintenanceFleetControl | None = None,
        wall_clock: Callable[[], float] = time.time,
        monotonic_clock: Callable[[], float] = time.monotonic,
        scan_limit: int = 100,
        max_active_runs: int = 4_096,
        max_staleness_seconds: float = 30.0,
    ) -> None:
        if not isinstance(store, DurableRunStore):
            raise TypeError("store must be a DurableRunStore")
        if not callable(scheduler_resolver):
            raise TypeError("scheduler_resolver must be callable")
        scanner = deadline_scanner or DurableDeadlineScanner(store)
        reaper = lease_reaper or DurableLeaseReaper(store)
        if (
            not isinstance(scanner, DurableDeadlineScanner)
            or scanner.store.path != store.path
        ):
            raise MaintenanceConfigurationError(
                "deadline scanner must use the supervisor Store"
            )
        if (
            not isinstance(reaper, DurableLeaseReaper)
            or reaper.store.path != store.path
        ):
            raise MaintenanceConfigurationError(
                "lease reaper must use the supervisor Store"
            )
        if fleet_control is not None and not isinstance(
            fleet_control,
            MaintenanceFleetControl,
        ):
            raise MaintenanceConfigurationError(
                "fleet control does not implement the maintenance contract"
            )
        if not callable(wall_clock) or not callable(monotonic_clock):
            raise TypeError("maintenance clocks must be callable")
        if (
            isinstance(scan_limit, bool)
            or not isinstance(scan_limit, int)
            or not 1 <= scan_limit <= MAX_MAINTENANCE_SCAN_LIMIT
        ):
            raise MaintenanceConfigurationError(
                "scan_limit exceeds the safe bound"
            )
        if (
            isinstance(max_active_runs, bool)
            or not isinstance(max_active_runs, int)
            or not 1 <= max_active_runs <= MAX_MAINTENANCE_ACTIVE_RUNS
        ):
            raise MaintenanceConfigurationError(
                "max_active_runs exceeds the safe bound"
            )
        try:
            staleness = float(max_staleness_seconds)
        except (TypeError, ValueError) as exc:
            raise MaintenanceConfigurationError(
                "max_staleness_seconds must be finite"
            ) from exc
        if (
            not math.isfinite(staleness)
            or staleness <= 0
            or staleness > MAX_MAINTENANCE_STALENESS_SECONDS
        ):
            raise MaintenanceConfigurationError(
                "max_staleness_seconds exceeds the safe bound"
            )

        self.store = store
        self._scheduler_resolver = scheduler_resolver
        self._deadline_scanner = scanner
        self._lease_reaper = reaper
        self._fleet_control = fleet_control
        self._wall_clock = wall_clock
        self._monotonic_clock = monotonic_clock
        self.scan_limit = scan_limit
        self.max_active_runs = max_active_runs
        self.max_staleness_seconds = staleness

        self._cycle_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._bootstrapped = False
        self._healthy = False
        self._sequence = 0
        self._last_success_monotonic: float | None = None

    @property
    def production_security_ready(self) -> bool:
        """Whether bootstrap, last cycle, freshness, and Fleet are all safe."""

        try:
            current = self._finite_time(
                self._monotonic_clock(),
                "monotonic clock",
            )
            with self._state_lock:
                bootstrapped = self._bootstrapped
                healthy = self._healthy
                last_success = self._last_success_monotonic
            if (
                not bootstrapped
                or not healthy
                or last_success is None
                or current < last_success
                or current - last_success > self.max_staleness_seconds
            ):
                return False
            return (
                self._fleet_control is None
                or self._fleet_control.production_security_ready is True
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return False

    def bootstrap(self) -> MaintenanceCycleReport:
        """Recover all bounded non-terminal Runs before admitting new work."""

        return self._cycle(mode="bootstrap", include_active_runs=True)

    def run_once(self) -> MaintenanceCycleReport:
        """Run one bounded steady-state maintenance pass."""

        with self._state_lock:
            if not self._bootstrapped:
                raise MaintenanceNotBootstrapped(
                    "bootstrap must succeed before run_once"
                )
        return self._cycle(mode="steady", include_active_runs=False)

    def admission_linearization_guard(self) -> threading.Lock:
        """Hold maintenance health stable through one remote Store claim."""

        return self._state_lock

    def admission_linearization_ready(self) -> bool:
        """Recheck bootstrap, health, freshness, and Fleet under the guard."""

        try:
            current = self._finite_time(
                self._monotonic_clock(),
                "monotonic clock",
            )
            last_success = self._last_success_monotonic
            return (
                self._bootstrapped
                and self._healthy
                and last_success is not None
                and current >= last_success
                and current - last_success
                <= self.max_staleness_seconds
                and (
                    self._fleet_control is None
                    or self._fleet_control.production_security_ready
                    is True
                )
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return False

    def _cycle(
        self,
        *,
        mode: str,
        include_active_runs: bool,
    ) -> MaintenanceCycleReport:
        if not self._cycle_lock.acquire(blocking=False):
            raise MaintenanceCycleConflict(
                "a maintenance cycle is already running"
            )
        try:
            with self._state_lock:
                self._sequence += 1
                sequence = self._sequence

            stage = "clock"
            active_runs: dict[str, RunRecord] = {}
            deadline_report: DeadlineReport | None = None
            recovery_report: RecoveryReport | None = None
            fleet_report: FleetProjectionReconcileReport | None = None
            reconciled = 0
            due_retry_runs = 0
            control_runs = 0
            hierarchy_runs = 0
            backlog_possible = False
            try:
                now = self._finite_time(self._wall_clock(), "wall clock")
                if include_active_runs:
                    stage = "active_run_scan"
                    active_runs = self._active_runs()

                stage = "deadline_scan"
                deadline_report = self._deadline_scanner.run_once(
                    now=now,
                    limit=self.scan_limit,
                )
                if not isinstance(deadline_report, DeadlineReport):
                    raise _InvalidComponentReport
                backlog_possible = (
                    deadline_report.scanned >= self.scan_limit
                    or deadline_report.run_deadlines_scanned
                    >= self.scan_limit
                )

                stage = "lease_recovery"
                recovery_report = self._lease_reaper.run_once(
                    now=now,
                    limit=self.scan_limit,
                )
                if not isinstance(recovery_report, RecoveryReport):
                    raise _InvalidComponentReport
                backlog_possible = backlog_possible or (
                    recovery_report.scanned >= self.scan_limit
                )

                stage = "due_retry_scan"
                due_runs = self.store.list_runs_with_due_retries(
                    now=now,
                    limit=self.scan_limit,
                )
                due_retry_runs = len(due_runs)
                backlog_possible = backlog_possible or (
                    due_retry_runs >= self.scan_limit
                )

                stage = "hierarchy_run_scan"
                hierarchy_candidates = (
                    self.store.list_runs_with_active_hierarchy(
                        limit=self.scan_limit,
                    )
                )
                hierarchy_runs = len(hierarchy_candidates)
                backlog_possible = backlog_possible or (
                    hierarchy_runs >= self.scan_limit
                )

                affected_run_ids = set(
                    deadline_report.runs_to_reconcile
                ).union(recovery_report.runs_to_reconcile)
                for run in due_runs:
                    active_runs[run.run_id] = run
                for run in hierarchy_candidates:
                    active_runs[run.run_id] = run
                if not include_active_runs:
                    stage = "control_run_scan"
                    control_candidates = self.store.list_runs(
                        statuses=(
                            RunStatus.CREATED,
                            RunStatus.PAUSING,
                            RunStatus.CANCELLING,
                        ),
                        limit=self.scan_limit,
                    )
                    control_runs = len(control_candidates)
                    backlog_possible = backlog_possible or (
                        control_runs >= self.scan_limit
                    )
                    for run in control_candidates:
                        active_runs[run.run_id] = run
                for run_id in affected_run_ids:
                    if run_id not in active_runs:
                        run = self.store.get_run(run_id)
                        if run is not None:
                            active_runs[run_id] = run
                if len(active_runs) > self.max_active_runs:
                    raise _ActiveRunLimitExceeded

                stage = "domain_reconcile"
                for run in self._ordered_runs(active_runs):
                    current = self.store.get_run(run.run_id)
                    if current is None:
                        continue
                    scheduler = self._resolve_scheduler(current)
                    scheduler.reconcile(current.run_id)
                    reconciled += 1

                stage = "due_retry_verify"
                if self.store.list_runs_with_due_retries(
                    now=now,
                    limit=1,
                ):
                    raise _InvalidComponentReport

                if backlog_possible:
                    failures = (
                        MaintenanceFailure(
                            "recovery_backlog",
                            "scan_limit_reached",
                        ),
                    )
                    return self._failed_report(
                        mode=mode,
                        sequence=sequence,
                        active_runs=len(active_runs),
                        deadline=deadline_report,
                        recovery=recovery_report,
                        due_retry_runs=due_retry_runs,
                        control_runs=control_runs,
                        hierarchy_runs=hierarchy_runs,
                        reconciled=reconciled,
                        backlog_possible=True,
                        failures=failures,
                    )

                stage = "fleet_reconcile"
                if self._fleet_control is not None:
                    if (
                        self._fleet_control.production_security_ready
                        is not True
                    ):
                        raise _InvalidComponentReport
                    fleet_report = self._fleet_control.run_once()
                    if not isinstance(
                        fleet_report,
                        FleetProjectionReconcileReport,
                    ):
                        raise _InvalidComponentReport

                completed_at = self._finite_time(
                    self._monotonic_clock(),
                    "monotonic clock",
                )
            except (KeyboardInterrupt, SystemExit):
                self._mark_failed()
                self._quarantine()
                raise
            except BaseException:
                failure = MaintenanceFailure(
                    stage,
                    self._failure_code(stage),
                )
                return self._failed_report(
                    mode=mode,
                    sequence=sequence,
                    active_runs=len(active_runs),
                    deadline=deadline_report,
                    recovery=recovery_report,
                    due_retry_runs=due_retry_runs,
                    control_runs=control_runs,
                    hierarchy_runs=hierarchy_runs,
                    reconciled=reconciled,
                    backlog_possible=backlog_possible,
                    failures=(failure,),
                )

            with self._state_lock:
                if mode == "bootstrap":
                    self._bootstrapped = True
                self._healthy = True
                self._last_success_monotonic = completed_at
            return self._report(
                mode=mode,
                sequence=sequence,
                healthy=True,
                active_runs=len(active_runs),
                deadline=deadline_report,
                recovery=recovery_report,
                due_retry_runs=due_retry_runs,
                control_runs=control_runs,
                hierarchy_runs=hierarchy_runs,
                reconciled=reconciled,
                fleet=fleet_report,
                backlog_possible=False,
                quarantined=False,
                quarantine_report=None,
                failures=(),
            )
        finally:
            self._cycle_lock.release()

    def _failed_report(
        self,
        *,
        mode: str,
        sequence: int,
        active_runs: int,
        deadline: DeadlineReport | None,
        recovery: RecoveryReport | None,
        due_retry_runs: int,
        control_runs: int,
        hierarchy_runs: int,
        reconciled: int,
        backlog_possible: bool,
        failures: tuple[MaintenanceFailure, ...],
    ) -> MaintenanceCycleReport:
        self._mark_failed()
        quarantined, quarantine_report = self._quarantine()
        all_failures = list(failures)
        if self._fleet_control is not None and not quarantined:
            all_failures.append(
                MaintenanceFailure(
                    "fleet_quarantine",
                    "quarantine_failed",
                )
            )
        return self._report(
            mode=mode,
            sequence=sequence,
            healthy=False,
            active_runs=active_runs,
            deadline=deadline,
            recovery=recovery,
            due_retry_runs=due_retry_runs,
            control_runs=control_runs,
            hierarchy_runs=hierarchy_runs,
            reconciled=reconciled,
            fleet=None,
            backlog_possible=backlog_possible,
            quarantined=quarantined,
            quarantine_report=quarantine_report,
            failures=tuple(all_failures),
        )

    def _mark_failed(self) -> None:
        with self._state_lock:
            self._healthy = False

    def _quarantine(
        self,
    ) -> tuple[bool, FleetQueueReconcileReport | None]:
        if self._fleet_control is None:
            return False, None
        try:
            report = self._fleet_control.quarantine()
            if not isinstance(report, FleetQueueReconcileReport):
                return False, None
            return True, report
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return False, None

    def _active_runs(self) -> dict[str, RunRecord]:
        runs = {
            run.run_id: run
            for run in self.store.list_nonterminal_runs(
                limit=self.max_active_runs + 1,
            )
        }
        if len(runs) > self.max_active_runs:
            raise _ActiveRunLimitExceeded
        return runs

    def _resolve_scheduler(self, run: RunRecord) -> DurableScheduler:
        scheduler = self._scheduler_resolver(run)
        if (
            not isinstance(scheduler, DurableScheduler)
            or scheduler.store.path != self.store.path
            or scheduler.workflow.name != run.workflow_id
            or scheduler.workflow.version != run.workflow_version
            or scheduler.workflow.definition_digest != run.definition_digest
        ):
            raise _InvalidComponentReport
        return scheduler

    @staticmethod
    def _ordered_runs(
        runs: dict[str, RunRecord],
    ) -> tuple[RunRecord, ...]:
        """Order parent Runs before children without trusting hierarchy text."""

        pending = dict(runs)
        ordered: list[RunRecord] = []
        while pending:
            ready = []
            for run in pending.values():
                link = run.metadata.get("hierarchy_link")
                parent_id = (
                    link.get("parent_run_id")
                    if isinstance(link, dict)
                    else None
                )
                if parent_id not in pending:
                    ready.append(run)
            if not ready:
                raise _InvalidComponentReport
            ready.sort(key=lambda run: (run.created_at, run.run_id))
            for run in ready:
                ordered.append(run)
                pending.pop(run.run_id)
        return tuple(ordered)

    @staticmethod
    def _finite_time(value: object, field_name: str) -> float:
        try:
            timestamp = float(value)
        except (TypeError, ValueError) as exc:
            raise MaintenanceConfigurationError(
                f"{field_name} must be finite"
            ) from exc
        if not math.isfinite(timestamp) or timestamp < 0:
            raise MaintenanceConfigurationError(
                f"{field_name} must be finite"
            )
        return timestamp

    @staticmethod
    def _failure_code(stage: str) -> str:
        return {
            "clock": "clock_failed",
            "active_run_scan": "active_run_scan_failed",
            "deadline_scan": "deadline_scan_failed",
            "lease_recovery": "lease_recovery_failed",
            "due_retry_scan": "due_retry_scan_failed",
            "hierarchy_run_scan": "hierarchy_run_scan_failed",
            "control_run_scan": "control_run_scan_failed",
            "due_retry_verify": "due_retry_not_converged",
            "domain_reconcile": "domain_reconcile_failed",
            "fleet_reconcile": "fleet_reconcile_failed",
        }.get(stage, "maintenance_failed")

    @staticmethod
    def _report(
        *,
        mode: str,
        sequence: int,
        healthy: bool,
        active_runs: int,
        deadline: DeadlineReport | None,
        recovery: RecoveryReport | None,
        due_retry_runs: int,
        control_runs: int,
        hierarchy_runs: int,
        reconciled: int,
        fleet: FleetProjectionReconcileReport | None,
        backlog_possible: bool,
        quarantined: bool,
        quarantine_report: FleetQueueReconcileReport | None,
        failures: tuple[MaintenanceFailure, ...],
    ) -> MaintenanceCycleReport:
        return MaintenanceCycleReport(
            mode=mode,
            sequence=sequence,
            healthy=healthy,
            active_runs_scanned=active_runs,
            deadline_scanned=0 if deadline is None else deadline.scanned,
            run_deadlines_scanned=(
                0
                if deadline is None
                else deadline.run_deadlines_scanned
            ),
            deadline_resolved=(
                0 if deadline is None else len(deadline.resolved)
            ),
            deadline_races=(
                0 if deadline is None else len(deadline.skipped_races)
            ),
            deadline_triggered_runs=(
                0 if deadline is None else len(deadline.triggered_runs)
            ),
            leases_scanned=0 if recovery is None else recovery.scanned,
            leases_resolved=(
                0 if recovery is None else len(recovery.resolved)
            ),
            lease_races=(
                0 if recovery is None else len(recovery.skipped_races)
            ),
            due_retry_runs_scanned=due_retry_runs,
            control_runs_scanned=control_runs,
            hierarchy_runs_scanned=hierarchy_runs,
            runs_reconciled=reconciled,
            fleet_routes_examined=(
                0 if fleet is None else fleet.routes_examined
            ),
            fleet_desired_tasks=(
                0 if fleet is None else fleet.desired_tasks
            ),
            backlog_possible=backlog_possible,
            quarantined=quarantined,
            quarantine_withdrawn_tasks=(
                0
                if quarantine_report is None
                else quarantine_report.withdrawn_tasks
            ),
            quarantine_deferred_active_tasks=(
                0
                if quarantine_report is None
                else quarantine_report.deferred_active_tasks
            ),
            failures=failures,
        )


__all__ = [
    "DurableMaintenanceSupervisor",
    "MaintenanceConfigurationError",
    "MaintenanceCycleConflict",
    "MaintenanceCycleReport",
    "MaintenanceFailure",
    "MaintenanceFleetControl",
    "MaintenanceNotBootstrapped",
]
