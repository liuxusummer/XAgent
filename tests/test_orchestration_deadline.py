from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

from src.orchestration.artifacts import ArtifactKind, LocalArtifactStore
from src.orchestration.deadline import DurableDeadlineScanner
from src.orchestration.lease import (
    ProbeOutcome,
    ProbeResult,
    RecoveryRetryPolicy,
)
from src.orchestration.models import AttemptStatus, NodeStatus, RunStatus
from src.orchestration.scheduler import DurableScheduler
from src.orchestration.store import (
    DurableRunStore,
    IdempotencyConflictError,
    InvalidStateTransition,
)
from src.orchestration.workflow import compile_workflow


class _Clock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class _Ids:
    def __init__(self) -> None:
        self.value = 0

    def __call__(self, prefix: str) -> str:
        self.value += 1
        return f"{prefix}-deadline-{self.value}"


class _Probe:
    def __init__(self, result: ProbeResult) -> None:
        self.result = result
        self.calls = 0

    def probe(self, _attempt):
        self.calls += 1
        return self.result


class DurableDeadlineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.db_path = self.root / "orchestration.sqlite3"
        self.clock = _Clock()
        self.artifacts = LocalArtifactStore(self.root / "artifacts")

    def _scheduler(
        self,
        *,
        effect_class: str = "read_only",
        timeout: dict | None = None,
        retry: dict | None = None,
    ) -> DurableScheduler:
        node = {
            "id": "activity",
            "kind": "tool",
            "config": {"tool": "deadline-test", "arguments": {}},
            "effect_class": effect_class,
            "timeout": timeout or {},
        }
        if effect_class == "idempotent_write":
            node["idempotency_key_template"] = "deadline:{{run_id}}:{{node_id}}"
        if retry is not None:
            node["retry"] = retry
        workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": "deadline-test",
                "version": 1,
                "nodes": [node],
            }
        )
        return DurableScheduler(
            DurableRunStore(self.db_path),
            workflow,
            clock=self.clock,
            id_factory=_Ids(),
        )

    def _scheduled(self, scheduler: DurableScheduler, run_id: str = "run"):
        scheduler.create_run(run_id)
        scheduler.reconcile(run_id)
        run = scheduler.store.get_run(run_id)
        node = scheduler.store.get_node(run_id, "activity")
        assert run is not None and node is not None
        return scheduler._schedule_attempt(
            run,
            node,
            scheduler.workflow.get_node("activity"),
            (),
        )

    def test_persists_phase_deadlines_and_caps_lease(self) -> None:
        scheduler = self._scheduler(
            timeout={
                "schedule_timeout_ms": 10_000,
                "start_timeout_ms": 5_000,
                "execution_timeout_ms": 20_000,
                "heartbeat_timeout_ms": 2_000,
            }
        )
        scheduler.create_run("run", deadline_at=150)
        claim = scheduler.claim_next("run", "worker", lease_seconds=60)
        assert claim is not None
        attempt = scheduler.store.get_attempt(claim.attempt_id)
        assert attempt is not None
        self.assertEqual(attempt.metadata["schedule_deadline_at"], 110)
        self.assertEqual(attempt.metadata["start_deadline_at"], 105)
        self.assertEqual(claim.lease_expires_at, 102)

        self.clock.now = 101
        scheduler.start_claim(claim)
        attempt = scheduler.store.get_attempt(claim.attempt_id)
        record = scheduler.store.get_idempotency("run", attempt.idempotency_key)
        assert attempt is not None and record is not None
        self.assertEqual(attempt.metadata["execution_deadline_at"], 121)
        self.assertEqual(record.lease_expires_at, 103)

        self.clock.now = 102
        renewed = scheduler.store.renew_activity_lease(
            "run",
            "activity",
            claim.attempt_id,
            claim.request_hash,
            claim.worker_id,
            claim_token=claim.claim_token,
            fencing_token=claim.fencing_token,
            lease_seconds=60,
            now=self.clock.now,
        )
        self.assertEqual(renewed.lease_expires_at, 104)

    def test_schedule_timeout_survives_restart_and_duplicate_scan(self) -> None:
        scheduler = self._scheduler(
            timeout={"schedule_timeout_ms": 5_000},
        )
        attempt = self._scheduled(scheduler)

        restarted = DurableRunStore(self.db_path)
        report = DurableDeadlineScanner(restarted).run_once(now=105)
        self.assertEqual(report.scanned, 1)
        self.assertEqual(report.resolved[0].timeout_kind, "schedule")
        self.assertEqual(
            restarted.get_attempt(attempt.attempt_id).status,
            AttemptStatus.TIMED_OUT,
        )
        self.assertEqual(
            restarted.get_node("run", "activity").status,
            NodeStatus.FAILED,
        )
        duplicate = DurableDeadlineScanner(restarted).run_once(now=106)
        self.assertEqual(duplicate.scanned, 0)
        self.assertEqual(duplicate.resolved, ())

    def test_start_deadline_is_fail_closed_at_boundary(self) -> None:
        scheduler = self._scheduler(timeout={"start_timeout_ms": 5_000})
        scheduler.create_run("run")
        claim = scheduler.claim_next("run", "worker", lease_seconds=60)
        assert claim is not None

        with self.assertRaises(IdempotencyConflictError):
            scheduler.store.start_activity(
                "run",
                "activity",
                claim.attempt_id,
                claim.worker_id,
                claim_token=claim.claim_token,
                now=105,
            )
        report = DurableDeadlineScanner(scheduler.store).run_once(now=105)
        self.assertEqual(report.resolved[0].timeout_kind, "start")
        self.assertEqual(
            scheduler.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.TIMED_OUT,
        )

    def test_heartbeat_is_distinct_from_shorter_worker_lease(self) -> None:
        scheduler = self._scheduler(
            timeout={
                "execution_timeout_ms": 200_000,
                "heartbeat_timeout_ms": 100_000,
            }
        )
        scheduler.create_run("run")
        claim = scheduler.claim_next("run", "worker", lease_seconds=60)
        assert claim is not None
        scheduler.start_claim(claim)
        self.assertEqual(
            DurableDeadlineScanner(scheduler.store).run_once(now=160).scanned,
            0,
        )
        report = DurableDeadlineScanner(scheduler.store).run_once(now=200)
        self.assertEqual(report.resolved[0].timeout_kind, "heartbeat")

    def test_heartbeat_shorter_than_lease_and_renew_resets_it(self) -> None:
        scheduler = self._scheduler(
            timeout={
                "execution_timeout_ms": 100_000,
                "heartbeat_timeout_ms": 10_000,
            }
        )
        scheduler.create_run("run")
        claim = scheduler.claim_next("run", "worker", lease_seconds=60)
        assert claim is not None
        scheduler.start_claim(claim)
        renewed = scheduler.store.renew_activity_lease(
            "run",
            "activity",
            claim.attempt_id,
            claim.request_hash,
            claim.worker_id,
            claim_token=claim.claim_token,
            fencing_token=claim.fencing_token,
            lease_seconds=60,
            now=105,
        )
        self.assertEqual(renewed.lease_expires_at, 115)
        self.assertEqual(
            DurableDeadlineScanner(scheduler.store).run_once(now=110).scanned,
            0,
        )
        report = DurableDeadlineScanner(scheduler.store).run_once(now=115)
        self.assertEqual(report.resolved[0].timeout_kind, "heartbeat")

    def test_run_and_start_tie_prefers_run_deadline(self) -> None:
        scheduler = self._scheduler(timeout={"start_timeout_ms": 5_000})
        scheduler.create_run("run", deadline_at=105)
        claim = scheduler.claim_next("run", "worker", lease_seconds=60)
        assert claim is not None
        report = DurableDeadlineScanner(scheduler.store).run_once(now=105)
        self.assertEqual(report.resolved[0].timeout_kind, "run")
        self.assertIn("run", report.triggered_runs)

    def test_execution_and_heartbeat_tie_prefers_execution(self) -> None:
        scheduler = self._scheduler(
            timeout={
                "execution_timeout_ms": 10_000,
                "heartbeat_timeout_ms": 10_000,
            }
        )
        scheduler.create_run("run")
        claim = scheduler.claim_next("run", "worker", lease_seconds=60)
        assert claim is not None
        scheduler.start_claim(claim)
        report = DurableDeadlineScanner(scheduler.store).run_once(now=110)
        self.assertEqual(report.resolved[0].timeout_kind, "execution")

    def test_prestart_timeout_cannot_be_forged_with_generic_append(self) -> None:
        scheduler = self._scheduler(
            timeout={"schedule_timeout_ms": 5_000},
        )
        attempt = self._scheduled(scheduler)
        run = scheduler.store.get_run("run")
        node = scheduler.store.get_node("run", "activity")
        assert run is not None and node is not None
        receipt = {
            "outcome": "timed_out",
            "error_class": "timeout",
            "error_code": "schedule_timeout",
        }
        with self.assertRaises(InvalidStateTransition):
            scheduler.store.append_event(
                "run",
                "attempt.timed_out",
                payload={
                    "timeout_kind": "schedule",
                    "deadline_at": 105,
                },
                occurred_at=105,
                run_projection=run,
                node_projection=replace(
                    node,
                    status=NodeStatus.FAILED,
                    error=receipt,
                ),
                attempt_projection=replace(
                    attempt,
                    status=AttemptStatus.TIMED_OUT,
                    error=receipt,
                    result=receipt,
                    finished_at=105,
                ),
            )

    def test_concurrent_duplicate_scanners_resolve_once(self) -> None:
        scheduler = self._scheduler(
            timeout={"schedule_timeout_ms": 5_000},
        )
        attempt = self._scheduled(scheduler)

        def scan():
            return DurableDeadlineScanner(
                DurableRunStore(self.db_path)
            ).run_once(now=105)

        with ThreadPoolExecutor(max_workers=2) as pool:
            reports = list(pool.map(lambda _value: scan(), range(2)))
        self.assertEqual(sum(len(report.resolved) for report in reports), 1)
        self.assertEqual(
            scheduler.store.get_attempt(attempt.attempt_id).status,
            AttemptStatus.TIMED_OUT,
        )

    def test_running_effect_classes_are_conservative_and_probe_writes(self) -> None:
        for effect_class in ("non_idempotent_write", "destructive"):
            with self.subTest(effect_class=effect_class):
                path = self.root / f"{effect_class}.sqlite3"
                old_path = self.db_path
                self.db_path = path
                scheduler = self._scheduler(
                    effect_class=effect_class,
                    timeout={"execution_timeout_ms": 5_000},
                )
                scheduler.create_run("run")
                claim = scheduler.claim_next("run", "worker", lease_seconds=60)
                assert claim is not None
                scheduler.start_claim(claim)
                report = DurableDeadlineScanner(scheduler.store).run_once(now=105)
                self.assertEqual(report.resolved[0].resolution, "waiting_recovery")
                self.assertEqual(
                    scheduler.store.get_attempt(claim.attempt_id).status,
                    AttemptStatus.OUTCOME_UNKNOWN,
                )
                self.assertEqual(
                    scheduler.store.get_run("run").status,
                    RunStatus.WAITING_RECOVERY,
                )
                self.db_path = old_path

        self.db_path = self.root / "idempotent.sqlite3"
        scheduler = self._scheduler(
            effect_class="idempotent_write",
            timeout={"execution_timeout_ms": 5_000},
        )
        scheduler.create_run("run")
        claim = scheduler.claim_next("run", "worker", lease_seconds=60)
        assert claim is not None
        scheduler.start_claim(claim)
        ref = self.artifacts.put_json(
            {"ok": True},
            kind=ArtifactKind.TOOL_RESULT,
            producer_run_id="run",
            producer_node_id="activity",
            producer_attempt_id=claim.attempt_id,
        )
        probe = _Probe(ProbeResult(ProbeOutcome.COMMITTED, (ref,)))
        report = DurableDeadlineScanner(
            scheduler.store,
            probe_resolver=lambda _attempt: probe,
            artifact_verifier=self.artifacts,
        ).run_once(now=105)
        self.assertEqual(probe.calls, 1)
        self.assertEqual(report.resolved[0].resolution, "verified_succeeded")
        self.assertEqual(
            scheduler.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.SUCCEEDED,
        )

    def test_read_only_execution_timeout_retries(self) -> None:
        scheduler = self._scheduler(
            timeout={"execution_timeout_ms": 5_000},
        )
        scheduler.create_run("run")
        claim = scheduler.claim_next("run", "worker", lease_seconds=60)
        assert claim is not None
        scheduler.start_claim(claim)
        report = DurableDeadlineScanner(
            scheduler.store,
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(
                max_attempts=2,
                retry_delay_seconds=3,
            ),
        ).run_once(now=105)
        self.assertEqual(report.resolved[0].resolution, "timeout_retry")
        self.assertEqual(
            scheduler.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.TIMED_OUT,
        )
        node = scheduler.store.get_node("run", "activity")
        self.assertEqual(node.status, NodeStatus.WAITING_RETRY)
        self.assertEqual(node.metadata["retry_due_at"], 108)

    def test_run_deadline_hook_and_running_write_never_looks_cancelled(self) -> None:
        scheduler = self._scheduler(effect_class="non_idempotent_write")
        scheduler.create_run("run", deadline_at=105)
        claim = scheduler.claim_next("run", "worker", lease_seconds=60)
        assert claim is not None
        scheduler.start_claim(claim)
        propagated: list[tuple[str, float]] = []
        report = DurableDeadlineScanner(
            scheduler.store,
            propagation_hook=lambda run_id, deadline: propagated.append(
                (run_id, deadline)
            ),
        ).run_once(now=105)
        self.assertEqual(propagated, [("run", 105)])
        self.assertIn("run", report.triggered_runs)
        self.assertEqual(
            scheduler.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.OUTCOME_UNKNOWN,
        )
        self.assertEqual(
            scheduler.store.get_run("run").status,
            RunStatus.WAITING_RECOVERY,
        )
