from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.orchestration.artifacts import ArtifactKind, LocalArtifactStore
from src.orchestration.executor import TrustedActivityExecutor
from src.orchestration.lease import (
    DurableLeaseReaper,
    ProbeOutcome,
    ProbeResult,
    RecoveryRetryPolicy,
)
from src.orchestration.legacy_checkpoint import (
    LegacyCheckpointImportError,
    LegacyCheckpointImporter,
)
from src.orchestration.models import (
    AttemptStatus,
    NodeStatus,
    RunRecord,
    RunStatus,
)
from src.orchestration.policy import (
    EffectClass,
    PolicyEngine,
    ToolPolicy,
    ToolTimeoutBehavior,
)
from src.orchestration.process_backend import LocalProcessSupervisorBackend
from src.orchestration.replay import ReplayIntegrityError, logical_replay
from src.orchestration.scheduler import (
    ActivityReceipt,
    ApprovalResolution,
    DurableScheduler,
)
from src.orchestration.sandbox import (
    ResourceLimits,
    SandboxDispatcher,
    SandboxOutcome,
    SandboxProfile,
    SecurityLevel,
)
from src.orchestration.store import (
    DurableRunStore,
    IdempotencyConflictError,
    ProjectionConflictError,
)
from src.orchestration.telemetry import (
    DomainTelemetryBridge,
    SQLiteTelemetryCursorStore,
    project_domain_event,
)
from src.orchestration.workflow import compile_workflow


class _Clock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class _Ids:
    def __init__(self, namespace: str = "fault") -> None:
        self.namespace = namespace
        self.value = 0

    def __call__(self, prefix: str) -> str:
        self.value += 1
        return f"{prefix}-{self.namespace}-{self.value}"


class _CrashBeforeCreateStore(DurableRunStore):
    crash_before_create = True

    def create_run(self, run: RunRecord) -> RunRecord:
        if self.crash_before_create:
            raise RuntimeError("F01 crash before run.created transaction")
        return super().create_run(run)


class _Sink:
    def __init__(self) -> None:
        self.events = []

    def emit(self, event) -> None:
        self.events.append(event)


class _Probe:
    def __init__(self, result: ProbeResult) -> None:
        self.result = result
        self.calls = 0

    def probe(self, _attempt):
        self.calls += 1
        return self.result


class _SingleEventStore:
    def __init__(self, event) -> None:
        self.event = event

    def list_events(self, run_id: str, *, after_seq: int = 0, limit: int = 1000):
        if (
            self.event.run_id == run_id
            and self.event.seq > after_seq
            and limit > 0
        ):
            return [self.event]
        return []


class FaultMatrixAcceptanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.control_root = self.root / "control-plane"
        self.agent_root = self.root / "agent-workspace"
        self.control_root.mkdir()
        self.agent_root.mkdir()
        self.database = self.control_root / "orchestration.sqlite3"
        self.store = DurableRunStore(self.database)
        self.artifacts = LocalArtifactStore(self.control_root / "artifacts")
        self.clock = _Clock()

    def _workflow(
        self,
        *,
        effect_class: str = "read_only",
        approval: bool = False,
    ):
        node = (
            {
                "id": "approval",
                "kind": "approval",
                "config": {"prompt": "Approve bounded test operation"},
            }
            if approval
            else {
                "id": "work",
                "kind": "tool",
                "config": {"tool": "fault-test", "arguments": {}},
                "effect_class": effect_class,
            }
        )
        if (
            not approval
            and effect_class == "idempotent_write"
        ):
            node["idempotency_key_template"] = "{{run_id}}:{{node_id}}"
        return compile_workflow(
            {
                "schema_version": 2,
                "name": "fault-matrix",
                "version": 1,
                "nodes": [node],
            }
        )

    def _scheduler(
        self,
        *,
        store: DurableRunStore | None = None,
        effect_class: str = "read_only",
        approval: bool = False,
        namespace: str = "fault",
    ) -> DurableScheduler:
        target = store or self.store

        def result_writer(claim, result) -> ActivityReceipt:
            ref = self.artifacts.put_json(
                result,
                kind=ArtifactKind.TOOL_RESULT,
                producer_run_id=claim.run_id,
                producer_node_id=claim.node_id,
                producer_attempt_id=claim.attempt_id,
            )
            return ActivityReceipt((ref,))

        return DurableScheduler(
            target,
            self._workflow(effect_class=effect_class, approval=approval),
            clock=self.clock,
            id_factory=_Ids(namespace),
            result_writer=result_writer,
            artifact_verifier=self.artifacts.verify,
            approval_verifier=lambda _resolution: True,
        )

    def _scheduled_attempt(
        self,
        scheduler: DurableScheduler,
        run_id: str,
    ):
        scheduler.create_run(run_id)
        scheduler.reconcile(run_id)
        run = scheduler.store.get_run(run_id)
        node = scheduler.store.get_node(run_id, "work")
        assert run is not None and node is not None
        return scheduler._schedule_attempt(
            run,
            node,
            scheduler.workflow.get_node("work"),
            (),
        )

    def test_f01_crash_before_run_created_leaves_no_partial_run_and_retry_works(self) -> None:
        store = _CrashBeforeCreateStore(self.root / "f01.sqlite3")
        scheduler = self._scheduler(store=store, namespace="f01")

        with self.assertRaisesRegex(RuntimeError, "F01"):
            scheduler.create_run("f01-run")
        self.assertIsNone(store.get_run("f01-run"))
        self.assertEqual(store.list_events("f01-run"), [])

        store.crash_before_create = False
        created = scheduler.create_run("f01-run")
        self.assertEqual(created.run_id, "f01-run")
        self.assertEqual(len(store.list_runs()), 1)
        self.assertTrue(store.verify_projections("f01-run"))

    def test_f02_fault_after_event_insert_rolls_back_projection_and_has_no_gap(self) -> None:
        scheduler = self._scheduler(namespace="f02")
        attempt = self._scheduled_attempt(scheduler, "f02-run")
        before_events = self.store.list_events("f02-run")

        def fail(stage: str) -> None:
            if stage == "event.after_insert":
                raise RuntimeError("F02 injected")

        self.store._fault = fail
        with self.assertRaisesRegex(RuntimeError, "F02"):
            self.store.claim_activity(
                "f02-run",
                "work",
                attempt.attempt_id,
                attempt.metadata["request_hash"],
                "worker",
                now=101,
            )
        self.store._fault = lambda _stage: None

        self.assertIsNone(
            self.store.get_idempotency("f02-run", attempt.idempotency_key)
        )
        self.assertEqual(self.store.get_attempt(attempt.attempt_id), attempt)
        self.assertEqual(self.store.list_events("f02-run"), before_events)
        _claim, committed = self.store.claim_activity(
            "f02-run",
            "work",
            attempt.attempt_id,
            attempt.metadata["request_hash"],
            "worker",
            now=102,
        )
        assert committed is not None
        self.assertEqual(committed.seq, before_events[-1].seq + 1)
        self.assertTrue(self.store.verify_projections("f02-run"))

    def test_f03_restart_between_schedule_and_claim_reuses_same_attempt(self) -> None:
        scheduler = self._scheduler(namespace="f03-first")
        scheduled = self._scheduled_attempt(scheduler, "f03-run")

        restarted = self._scheduler(namespace="f03-restart")
        claim = restarted.claim_next("f03-run", "worker", lease_seconds=10)

        assert claim is not None
        self.assertEqual(claim.attempt_id, scheduled.attempt_id)
        self.assertEqual(len(self.store.list_attempts("f03-run")), 1)
        self.assertEqual(
            self.store.get_attempt(scheduled.attempt_id).status,
            AttemptStatus.CLAIMED,
        )

    def test_f07_committed_idempotent_write_is_probed_without_duplicate_effect(
        self,
    ) -> None:
        scheduler = self._scheduler(
            effect_class="idempotent_write",
            namespace="f07",
        )
        scheduler.create_run("f07-run")
        claim = scheduler.claim_next("f07-run", "worker", lease_seconds=5)
        assert claim is not None
        scheduler.start_claim(claim)
        external_results: dict[str, object] = {}
        external_results.setdefault(claim.operation_key, {"visible": "once"})
        ref = self.artifacts.put_json(
            external_results[claim.operation_key],
            kind=ArtifactKind.TOOL_RESULT,
            producer_run_id=claim.run_id,
            producer_node_id=claim.node_id,
            producer_attempt_id=claim.attempt_id,
        )
        probe = _Probe(ProbeResult(ProbeOutcome.COMMITTED, (ref,)))

        report = DurableLeaseReaper(
            self.store,
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(
                max_attempts=2,
            ),
            probe_resolver=lambda _attempt: probe,
            artifact_verifier=self.artifacts,
        ).run_once(now=106)

        self.assertEqual(len(report.resolved), 1)
        self.assertEqual(report.resolved[0].resolution, "verified_succeeded")
        self.assertEqual(probe.calls, 1)
        self.assertEqual(len(external_results), 1)
        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.SUCCEEDED,
        )
        self.assertEqual(len(self.store.list_attempts("f07-run")), 1)

    def test_f10_receipt_commit_fault_leaves_orphan_and_recovers_by_effect_class(
        self,
    ) -> None:
        written = []

        def result_writer(claim, result) -> ActivityReceipt:
            ref = self.artifacts.put_json(
                result,
                kind=ArtifactKind.TOOL_RESULT,
                producer_run_id=claim.run_id,
                producer_node_id=claim.node_id,
                producer_attempt_id=claim.attempt_id,
            )
            written.append(ref)
            return ActivityReceipt((ref,))

        scheduler = DurableScheduler(
            self.store,
            self._workflow(),
            clock=self.clock,
            id_factory=_Ids("f10"),
            result_writer=result_writer,
            artifact_verifier=self.artifacts.verify,
        )
        scheduler.create_run("f10-run")
        claim = scheduler.claim_next("f10-run", "worker", lease_seconds=5)
        assert claim is not None
        scheduler.start_claim(claim)

        def fail(stage: str) -> None:
            if stage == "complete.after_idempotency":
                raise RuntimeError("F10 receipt transaction failed")

        self.store._fault = fail
        with self.assertRaisesRegex(RuntimeError, "F10"):
            scheduler.complete_claim(claim, {"visible": "artifact-only"})
        self.store._fault = lambda _stage: None

        self.assertEqual(len(written), 1)
        self.assertTrue(self.artifacts.verify(written[0]))
        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )
        self.assertNotIn(
            written[0].artifact_id,
            str(self.store.list_events("f10-run")),
        )
        report = DurableLeaseReaper(
            self.store,
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(
                max_attempts=2,
            ),
        ).run_once(now=106)
        self.assertEqual(report.resolved[0].resolution, "abandon_retry")
        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.ABANDONED,
        )
        self.assertEqual(
            self.store.get_node("f10-run", "work").status,
            NodeStatus.WAITING_RETRY,
        )

    def test_f11_completion_response_loss_is_idempotent(self) -> None:
        scheduler = self._scheduler(namespace="f11")
        scheduler.create_run("f11-run")
        claim = scheduler.claim_next("f11-run", "worker", lease_seconds=30)
        assert claim is not None
        scheduler.start_claim(claim)
        external_calls: list[str] = []
        artifact_writes: list[str] = []

        def perform_external_once() -> ActivityReceipt:
            external_calls.append(claim.operation_key)
            ref = self.artifacts.put_json(
                {"external_result": "visible-once"},
                kind=ArtifactKind.TOOL_RESULT,
                producer_run_id=claim.run_id,
                producer_node_id=claim.node_id,
                producer_attempt_id=claim.attempt_id,
            )
            self.assertTrue(self.artifacts.verify(ref))
            artifact_writes.append(ref.artifact_id)
            return ActivityReceipt((ref,))

        receipt = perform_external_once().to_dict()

        first_record, first_event = self.store.complete_activity(
            claim.run_id,
            claim.node_id,
            claim.attempt_id,
            claim.request_hash,
            claim.worker_id,
            claim_token=claim.claim_token,
            result=receipt,
            now=101,
        )
        event_count = len(self.store.list_events(claim.run_id))
        second_record, second_event = self.store.complete_activity(
            claim.run_id,
            claim.node_id,
            claim.attempt_id,
            claim.request_hash,
            claim.worker_id,
            claim_token=claim.claim_token,
            result=receipt,
            now=102,
        )

        self.assertEqual(second_record, first_record)
        self.assertEqual(second_event, first_event)
        self.assertEqual(len(self.store.list_events(claim.run_id)), event_count)
        self.assertEqual(external_calls, [claim.operation_key])
        self.assertEqual(len(artifact_writes), 1)
        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.SUCCEEDED,
        )

    def test_f12_stale_worker_rejection_is_audited_once_without_secret_echo(
        self,
    ) -> None:
        scheduler = self._scheduler(namespace="f12")
        scheduler.create_run("f12-run")
        old = scheduler.claim_next(
            "f12-run",
            "F12-OLD-WORKER-SECRET",
            lease_seconds=5,
        )
        assert old is not None
        scheduler.start_claim(old)
        report = DurableLeaseReaper(
            self.store,
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(
                max_attempts=2,
            ),
        ).run_once(now=106)
        self.assertEqual(len(report.resolved), 1)
        self.clock.now = 106
        scheduler.reconcile("f12-run")
        current = scheduler.claim_next("f12-run", "new-worker", lease_seconds=5)
        assert current is not None
        self.assertGreater(current.fencing_token, old.fencing_token)
        before_events = self.store.list_events("f12-run")
        before_run = self.store.get_run("f12-run")
        before_node = self.store.get_node("f12-run", "work")
        before_attempts = self.store.list_attempts("f12-run")
        before_record = self.store.get_idempotency(
            old.run_id,
            before_attempts[0].idempotency_key,
        )
        result_secret = "F12-REJECTED-RESULT-SECRET"
        barrier = threading.Barrier(8)

        def stale_completion(_index: int) -> str:
            barrier.wait()
            try:
                self.store.complete_activity(
                    old.run_id,
                    old.node_id,
                    old.attempt_id,
                    old.request_hash,
                    old.worker_id,
                    claim_token=old.claim_token,
                    result={"outcome": "succeeded", "secret": result_secret},
                    now=107 + _index / 1_000,
                )
            except IdempotencyConflictError as conflict:
                return str(conflict)
            return "accepted"

        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(stale_completion, range(8)))

        self.assertEqual(
            outcomes,
            ["stale or foreign Activity claim"] * 8,
        )
        events = self.store.list_events("f12-run")
        rejected = [
            event
            for event in events
            if event.event_type == "activity.commit_rejected"
        ]
        self.assertEqual(len(rejected), 1)
        self.assertEqual(len(events), len(before_events) + 1)
        audit = rejected[0]
        self.assertEqual(
            set(audit.payload),
            {
                "reason_code",
                "current_fencing_token",
                "attempt_terminal",
                "claim_terminal",
                "projection",
            },
        )
        self.assertEqual(audit.payload["reason_code"], "claim_token_mismatch")
        self.assertEqual(
            audit.payload["current_fencing_token"],
            old.fencing_token + 1,
        )
        self.assertTrue(audit.payload["attempt_terminal"])
        self.assertTrue(audit.payload["claim_terminal"])

        # A later replay of the same Attempt + reason returns the original
        # conflict and does not amplify the audit stream.
        with self.assertRaises(IdempotencyConflictError):
            self.store.complete_activity(
                old.run_id,
                old.node_id,
                old.attempt_id,
                old.request_hash,
                old.worker_id,
                claim_token=old.claim_token,
                result={"outcome": "succeeded", "secret": result_secret},
                now=108,
            )
        self.assertEqual(len(self.store.list_events("f12-run")), len(events))
        with self.assertRaises(IdempotencyConflictError):
            self.store.renew_activity_lease(
                old.run_id,
                old.node_id,
                old.attempt_id,
                old.request_hash,
                old.worker_id,
                claim_token=old.claim_token,
                fencing_token=old.fencing_token,
                now=108,
            )
        with self.assertRaises(IdempotencyConflictError):
            self.store.start_activity(
                old.run_id,
                old.node_id,
                old.attempt_id,
                old.worker_id,
                claim_token=old.claim_token,
                now=108,
            )
        self.assertEqual(len(self.store.list_events("f12-run")), len(events))

        # These return-path canaries were never part of the legitimate claim.
        # A stale process cannot smuggle them into Event storage, including as
        # an offline-enumerable digest.
        rejected_worker_canary = "F12-UNPERSISTED-OLD-WORKER-CANARY"
        rejected_token_canary = "F12-UNPERSISTED-OLD-TOKEN-CANARY"
        rejected_request_canary = "F12-UNPERSISTED-REQUEST-HASH-CANARY"
        rejected_result_canary = "F12-UNPERSISTED-RESULT-CANARY"
        with self.assertRaises(IdempotencyConflictError):
            self.store.complete_activity(
                old.run_id,
                old.node_id,
                old.attempt_id,
                rejected_request_canary,
                rejected_worker_canary,
                claim_token=rejected_token_canary,
                result={"secret": rejected_result_canary},
                now=109,
            )
        self.assertEqual(len(self.store.list_events("f12-run")), len(events))

        after_run = self.store.get_run("f12-run")
        after_node = self.store.get_node("f12-run", "work")
        after_attempts = self.store.list_attempts("f12-run")
        after_record = self.store.get_idempotency(
            old.run_id,
            before_attempts[0].idempotency_key,
        )
        assert before_run is not None and after_run is not None
        before_run_business = before_run.to_dict()
        after_run_business = after_run.to_dict()
        for field in ("updated_at", "last_event_sequence", "projection_version"):
            before_run_business.pop(field)
            after_run_business.pop(field)
        self.assertEqual(after_run_business, before_run_business)
        self.assertEqual(after_node, before_node)
        self.assertEqual(after_attempts, before_attempts)
        self.assertEqual(after_record, before_record)

        with sqlite3.connect(self.database) as connection:
            audit_row = connection.execute(
                """
                SELECT event_id, event_type, node_id, attempt_id, payload_json,
                       intent_digest, content_digest
                FROM domain_events
                WHERE event_type = 'activity.commit_rejected'
                """
            ).fetchone()
        assert audit_row is not None
        persisted_audit = "|".join(str(value) for value in audit_row)
        for secret in (
            old.worker_id,
            old.claim_token,
            old.request_hash,
            result_secret,
        ):
            self.assertNotIn(secret, persisted_audit)
            self.assertNotIn(
                hashlib.sha256(secret.encode("utf-8")).hexdigest(),
                persisted_audit,
            )
        database_bytes = b"".join(
            candidate.read_bytes()
            for candidate in (
                self.database,
                Path(f"{self.database}-wal"),
                Path(f"{self.database}-shm"),
            )
            if candidate.exists()
        )
        for secret in (
            rejected_worker_canary,
            rejected_token_canary,
            rejected_request_canary,
            rejected_result_canary,
        ):
            encoded = secret.encode("utf-8")
            self.assertNotIn(encoded, database_bytes)
            self.assertNotIn(hashlib.sha256(encoded).hexdigest().encode(), database_bytes)

        projected = project_domain_event(audit)
        self.assertEqual(projected.name, "activity.commit_rejected")
        projected_text = repr(projected)
        self.assertNotIn(old.worker_id, projected_text)
        self.assertNotIn(old.claim_token, projected_text)
        self.assertNotIn(result_secret, projected_text)
        self.assertTrue(self.store.verify_projections("f12-run"))
        self.assertEqual(logical_replay(self.store, "f12-run").last_sequence, audit.seq)
        recovery_event = next(
            event
            for event in self.store.list_events("f12-run")
            if event.event_id.startswith("evt_lease_recovery_")
        )
        self.assertEqual(
            recovery_event.payload["recovery_fencing_token"],
            old.fencing_token + 1,
        )

    def test_f12_audit_dedup_survives_later_fencing_generation(self) -> None:
        scheduler = self._scheduler(namespace="f12-generation")
        scheduler.create_run("f12-generation-run")
        old = scheduler.claim_next(
            "f12-generation-run",
            "generation-old-worker",
            lease_seconds=5,
        )
        assert old is not None
        scheduler.start_claim(old)

        with self.assertRaises(IdempotencyConflictError):
            self.store.complete_activity(
                old.run_id,
                old.node_id,
                old.attempt_id,
                old.request_hash,
                old.worker_id,
                claim_token="F12-NEVER-PERSISTED-FOREIGN-TOKEN",
                result={"secret": "F12-PRE-RECOVERY-RESULT"},
                now=101,
            )
        first_audit = next(
            event
            for event in self.store.list_events(old.run_id)
            if event.event_type == "activity.commit_rejected"
        )
        self.assertEqual(
            first_audit.payload["current_fencing_token"],
            old.fencing_token,
        )
        self.assertFalse(first_audit.payload["attempt_terminal"])
        self.assertFalse(first_audit.payload["claim_terminal"])

        report = DurableLeaseReaper(
            self.store,
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(
                max_attempts=3,
            ),
        ).run_once(now=106)
        self.assertEqual(len(report.resolved), 1)
        self.clock.now = 106
        scheduler.reconcile(old.run_id)
        new_claim = scheduler.claim_next(
            old.run_id,
            "generation-new-worker",
            lease_seconds=5,
        )
        assert new_claim is not None
        self.assertGreater(new_claim.fencing_token, old.fencing_token)
        event_count = len(self.store.list_events(old.run_id))

        with self.assertRaisesRegex(
            IdempotencyConflictError,
            "stale or foreign Activity claim",
        ):
            self.store.complete_activity(
                old.run_id,
                old.node_id,
                old.attempt_id,
                old.request_hash,
                old.worker_id,
                claim_token=old.claim_token,
                result={"secret": "F12-POST-RECOVERY-RESULT"},
                now=107,
            )

        audits = [
            event
            for event in self.store.list_events(old.run_id)
            if event.event_type == "activity.commit_rejected"
        ]
        self.assertEqual(len(audits), 1)
        self.assertEqual(audits[0], first_audit)
        self.assertEqual(len(self.store.list_events(old.run_id)), event_count)
        self.assertTrue(self.store.verify_projections(old.run_id))
        self.assertEqual(
            logical_replay(self.store, old.run_id).last_sequence,
            self.store.get_run(old.run_id).last_event_sequence,
        )

    def test_f13_retry_backoff_restart_schedules_once_at_persisted_due_time(
        self,
    ) -> None:
        workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": "f13-retry",
                "version": 1,
                "nodes": [
                    {
                        "id": "work",
                        "kind": "tool",
                        "config": {"tool": "fault-test", "arguments": {}},
                        "effect_class": "read_only",
                        "retry": {
                            "max_attempts": 2,
                            "retry_on": ["transient"],
                            "initial_delay_ms": 1_000,
                            "max_delay_ms": 1_000,
                        },
                    }
                ],
            }
        )
        first = DurableScheduler(
            self.store,
            workflow,
            clock=self.clock,
            id_factory=_Ids("f13-first"),
        )
        first.create_run("f13-run")
        claim = first.claim_next("f13-run", "worker-one", lease_seconds=30)
        assert claim is not None
        first.start_claim(claim)
        first.complete_claim(
            claim,
            {"error_code": "temporary"},
            attempt_status=AttemptStatus.FAILED,
            error_class="transient",
        )
        node = self.store.get_node("f13-run", "work")
        assert node is not None
        self.assertEqual(node.status, NodeStatus.WAITING_RETRY)
        self.assertEqual(node.metadata["retry_due_at"], 101)

        restarted = DurableScheduler(
            DurableRunStore(self.database),
            workflow,
            clock=self.clock,
            id_factory=_Ids("f13-restart"),
        )
        self.clock.now = 100.999
        self.assertIsNone(restarted.claim_next("f13-run", "too-early"))
        self.clock.now = 101
        peers = (
            restarted,
            DurableScheduler(
                DurableRunStore(self.database),
                workflow,
                clock=self.clock,
                id_factory=_Ids("f13-peer"),
            ),
        )
        barrier = threading.Barrier(2)

        def claim_once(item):
            barrier.wait()
            try:
                return item.claim_next(
                    "f13-run",
                    "worker-two",
                    lease_seconds=30,
                )
            except ProjectionConflictError:
                # The losing scheduler may observe the winner's projection
                # CAS.  It still must not create or claim a third Attempt.
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            claims = list(pool.map(claim_once, peers))
        self.assertEqual(sum(item is not None for item in claims), 1)
        self.assertEqual(len(self.store.list_attempts("f13-run")), 2)

    def test_f16_cancel_intent_survives_restart_and_converges(self) -> None:
        scheduler = self._scheduler(namespace="f16-first")
        attempt = self._scheduled_attempt(scheduler, "f16-run")
        run = self.store.get_run("f16-run")
        assert run is not None
        self.store.append_event(
            "f16-run",
            "run.cancelling",
            payload={"intent": "cancel"},
            run_projection=replace(run, status=RunStatus.CANCELLING),
            occurred_at=101,
        )

        restarted = self._scheduler(namespace="f16-restart")
        restarted.reconcile("f16-run")

        self.assertEqual(
            self.store.get_attempt(attempt.attempt_id).status,
            AttemptStatus.CANCELLED,
        )
        self.assertEqual(
            self.store.get_run("f16-run").status,
            RunStatus.CANCELLED,
        )

    @unittest.skipUnless(os.name == "posix", "requires POSIX process groups")
    def test_f16_running_worker_observes_restarted_store_cancel_intent(
        self,
    ) -> None:
        scheduler = self._scheduler(namespace="f16-live")
        scheduler.create_run("f16-live-run")
        claim = scheduler.claim_next(
            "f16-live-run",
            "live-worker",
            lease_seconds=30,
        )
        assert claim is not None
        policy = PolicyEngine(
            (
                ToolPolicy(
                    "fault-test",
                    EffectClass.READ_ONLY,
                    supports_idempotency_key=False,
                    supports_status_probe=False,
                    supports_compensation=False,
                    timeout_behavior=ToolTimeoutBehavior.SAFE_TO_RETRY,
                ),
            )
        )
        backend = LocalProcessSupervisorBackend(self.artifacts)
        profile = SandboxProfile(
            "f16-local-supervision",
            (self.agent_root,),
            (),
            minimum_security_level=SecurityLevel.DEVELOPMENT_UNSAFE,
            limits=ResourceLimits(timeout_seconds=10, output_bytes=4096),
        )
        executor = TrustedActivityExecutor(
            scheduler,
            policy,
            SandboxDispatcher(
                (backend,),
                policy_version=policy.policy_version,
            ),
            artifact_verifier=self.artifacts.verify,
            artifact_reader=self.artifacts.read,
            clock=self.clock,
        )
        script = self.agent_root / "f16_cancellable_tree.py"
        pid_file = self.agent_root / "f16_tree.pids"
        ready_file = self.agent_root / "f16_tree.ready"
        script.write_text(
            "\n".join(
                (
                    "import os, signal, subprocess, sys, time",
                    "from pathlib import Path",
                    "role, pid_path, ready_path = sys.argv[1:4]",
                    "signal.signal(signal.SIGTERM, lambda *_: None)",
                    "with open(pid_path, 'a', encoding='utf-8') as stream:",
                    "    stream.write(f'{role}:{os.getpid()}\\n')",
                    "    stream.flush()",
                    "if role == 'parent':",
                    "    subprocess.Popen((sys.executable, __file__, 'child', pid_path, ready_path))",
                    "elif role == 'child':",
                    "    subprocess.Popen((sys.executable, __file__, 'grandchild', pid_path, ready_path))",
                    "if role == 'parent':",
                    "    deadline = time.monotonic() + 5",
                    "    while time.monotonic() < deadline:",
                    "        if Path(pid_path).exists() and len(Path(pid_path).read_text().splitlines()) >= 3:",
                    "            Path(ready_path).write_text('ready', encoding='utf-8')",
                    "            break",
                    "        time.sleep(0.01)",
                    "time.sleep(60)",
                )
            ),
            encoding="utf-8",
        )
        restarted_store = DurableRunStore(self.database)
        restarted = self._scheduler(
            store=restarted_store,
            namespace="f16-live-restarted",
        )
        barrier = threading.Barrier(2)
        controller_errors: list[BaseException] = []
        real_popen = subprocess.Popen

        def barrier_popen(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            deadline = time.monotonic() + 5
            while not ready_file.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            if not ready_file.exists():
                raise AssertionError("process tree did not reach the barrier")
            barrier.wait(timeout=5)
            return process

        def request_cancel_from_restarted_controller() -> None:
            try:
                barrier.wait(timeout=5)
                restarted.request_cancel(
                    claim.run_id,
                    reconcile=False,
                )
            except BaseException as exc:
                controller_errors.append(exc)

        controller = threading.Thread(
            target=request_cancel_from_restarted_controller
        )
        controller.start()
        with patch(
            "src.orchestration.process_backend.subprocess.Popen",
            side_effect=barrier_popen,
        ):
            result = executor.execute(
                claim,
                argv=(
                    sys.executable,
                    str(script),
                    "parent",
                    str(pid_file),
                    str(ready_file),
                ),
                cwd=str(self.agent_root),
                profile=profile,
                limits=profile.limits,
            )
        controller.join(timeout=5)

        self.assertFalse(controller.is_alive())
        self.assertEqual(controller_errors, [])
        self.assertEqual(result.sandbox_outcome, SandboxOutcome.CANCELLED.value)
        self.assertEqual(result.attempt_status, AttemptStatus.CANCELLED)
        replayed = executor.execute(
            claim,
            argv=(
                sys.executable,
                str(script),
                "parent",
                str(pid_file),
                str(ready_file),
            ),
            cwd=str(self.agent_root),
            profile=profile,
            limits=profile.limits,
        )
        self.assertTrue(replayed.replayed)
        self.assertEqual(
            replayed.completion_event.event_id,
            result.completion_event.event_id,
        )
        self.assertEqual(
            replayed.tool_receipt_digest,
            result.tool_receipt_digest,
        )
        roles = {
            role: int(pid)
            for role, pid in re.findall(
                r"^(parent|child|grandchild):(\d+)$",
                pid_file.read_text(encoding="utf-8"),
                flags=re.MULTILINE,
            )
        }
        self.assertEqual(set(roles), {"parent", "child", "grandchild"})
        for role, pid in roles.items():
            with self.subTest(role=role, pid=pid):
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    try:
                        os.kill(pid, 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.02)
                else:
                    self.fail(f"{role} process {pid} survived cancellation")

        rebuilt = DurableRunStore(self.database)
        self.assertEqual(
            rebuilt.get_attempt(claim.attempt_id).status,
            AttemptStatus.CANCELLED,
        )
        self.assertEqual(
            rebuilt.get_node(claim.run_id, claim.node_id).status,
            NodeStatus.CANCELLED,
        )
        self.assertEqual(
            rebuilt.get_run(claim.run_id).status,
            RunStatus.CANCELLED,
        )
        self.assertTrue(rebuilt.verify_projections(claim.run_id))

    def test_f17_cancel_success_race_has_one_legal_order_and_keeps_receipt(self) -> None:
        scheduler = self._scheduler(namespace="f17")
        scheduler.create_run("f17-run")
        claim = scheduler.claim_next("f17-run", "worker", lease_seconds=30)
        assert claim is not None
        scheduler.start_claim(claim)
        result_ref = self.artifacts.put_json(
            {"receipt": "bounded"},
            kind=ArtifactKind.TOOL_RESULT,
            producer_run_id=claim.run_id,
            producer_node_id=claim.node_id,
            producer_attempt_id=claim.attempt_id,
        )
        self.assertTrue(self.artifacts.verify(result_ref))
        receipt = ActivityReceipt((result_ref,)).to_dict()
        barrier = threading.Barrier(2)
        outcomes: list[str] = []

        def succeed() -> None:
            barrier.wait()
            try:
                self.store.complete_activity(
                    claim.run_id,
                    claim.node_id,
                    claim.attempt_id,
                    claim.request_hash,
                    claim.worker_id,
                    claim_token=claim.claim_token,
                    result=receipt,
                    now=101,
                )
                outcomes.append("success")
            except (IdempotencyConflictError, ProjectionConflictError):
                outcomes.append("stale")

        def cancel() -> None:
            barrier.wait()
            scheduler.request_cancel("f17-run")
            outcomes.append("cancel")

        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda fn: fn(), (succeed, cancel)))
        scheduler.reconcile("f17-run")

        self.assertIn("cancel", outcomes)
        attempt = self.store.get_attempt(claim.attempt_id)
        assert attempt is not None
        if "success" in outcomes:
            self.assertEqual(attempt.status, AttemptStatus.SUCCEEDED)
            self.assertEqual(
                attempt.result["artifact_refs"][0]["artifact_id"],
                result_ref.artifact_id,
            )
        else:
            self.assertEqual(outcomes.count("stale"), 1)
        self.assertIn(
            self.store.get_run("f17-run").status,
            {RunStatus.CANCELLING, RunStatus.CANCELLED},
        )
        self.assertTrue(self.store.verify_projections("f17-run"))

    def test_duplicate_cancel_intents_converge_after_projection_race(self) -> None:
        scheduler = self._scheduler(namespace="cancel-race")
        scheduler.create_run("cancel-race-run")
        claim = scheduler.claim_next(
            "cancel-race-run",
            "worker",
            lease_seconds=30,
        )
        assert claim is not None
        scheduler.start_claim(claim)

        append_barrier = threading.Barrier(2)
        gate_lock = threading.Lock()
        gated_calls = 0
        original_append_event = self.store.append_event

        def gated_append_event(*args, **kwargs):
            nonlocal gated_calls
            event_type = (
                args[1] if len(args) > 1 else kwargs.get("event_type")
            )
            should_wait = False
            if event_type == "run.cancelling":
                with gate_lock:
                    if gated_calls < 2:
                        gated_calls += 1
                        should_wait = True
            if should_wait:
                append_barrier.wait(timeout=5)
            return original_append_event(*args, **kwargs)

        self.store.append_event = gated_append_event
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda _index: scheduler.request_cancel(
                        "cancel-race-run"
                    ),
                    range(2),
                )
            )

        self.assertEqual(
            {result.status for result in results},
            {RunStatus.CANCELLING},
        )
        self.assertEqual(
            [
                event.event_type
                for event in self.store.list_events("cancel-race-run")
            ].count("run.cancelling"),
            1,
        )
        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )
        self.assertTrue(self.store.verify_projections("cancel-race-run"))

    def test_pause_retries_when_completion_advances_run_projection(self) -> None:
        scheduler = self._scheduler(namespace="pause-complete-race")
        scheduler.create_run("pause-complete-race-run")
        claim = scheduler.claim_next(
            "pause-complete-race-run",
            "worker",
            lease_seconds=30,
        )
        assert claim is not None
        scheduler.start_claim(claim)
        result_ref = self.artifacts.put_json(
            {"receipt": "pause-race"},
            kind=ArtifactKind.TOOL_RESULT,
            producer_run_id=claim.run_id,
            producer_node_id=claim.node_id,
            producer_attempt_id=claim.attempt_id,
        )
        receipt = ActivityReceipt((result_ref,)).to_dict()

        pause_entered = threading.Event()
        release_pause = threading.Event()
        gate_lock = threading.Lock()
        gated = False
        original_append_event = self.store.append_event

        def gated_append_event(*args, **kwargs):
            nonlocal gated
            event_type = (
                args[1] if len(args) > 1 else kwargs.get("event_type")
            )
            should_wait = False
            if event_type == "run.pausing":
                with gate_lock:
                    if not gated:
                        gated = True
                        should_wait = True
            if should_wait:
                pause_entered.set()
                if not release_pause.wait(timeout=5):
                    raise AssertionError("completion did not release pause gate")
            return original_append_event(*args, **kwargs)

        self.store.append_event = gated_append_event
        with ThreadPoolExecutor(max_workers=1) as pool:
            pause_future = pool.submit(
                scheduler.request_pause,
                "pause-complete-race-run",
            )
            self.assertTrue(pause_entered.wait(timeout=5))
            self.store.complete_activity(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
                claim.request_hash,
                claim.worker_id,
                claim_token=claim.claim_token,
                result=receipt,
                now=101,
            )
            release_pause.set()
            paused = pause_future.result(timeout=5)

        self.assertEqual(paused.status, RunStatus.PAUSED)
        attempt = self.store.get_attempt(claim.attempt_id)
        assert attempt is not None
        self.assertEqual(attempt.status, AttemptStatus.SUCCEEDED)
        self.assertEqual(
            attempt.result["artifact_refs"][0]["artifact_id"],
            result_ref.artifact_id,
        )
        event_types = [
            event.event_type
            for event in self.store.list_events("pause-complete-race-run")
        ]
        self.assertLess(
            event_types.index("attempt.succeeded"),
            event_types.index("run.pausing"),
        )
        self.assertEqual(event_types.count("run.pausing"), 1)
        self.assertTrue(
            self.store.verify_projections("pause-complete-race-run")
        )

    def test_duplicate_resume_intents_converge_after_projection_race(self) -> None:
        scheduler = self._scheduler(namespace="resume-race")
        scheduler.create_run("resume-race-run")
        scheduler.reconcile("resume-race-run")
        paused = scheduler.request_pause("resume-race-run")
        self.assertEqual(paused.status, RunStatus.PAUSED)
        before_started = [
            event.event_type
            for event in self.store.list_events("resume-race-run")
        ].count("run.started")

        append_barrier = threading.Barrier(2)
        gate_lock = threading.Lock()
        gated_calls = 0
        original_append_event = self.store.append_event

        def gated_append_event(*args, **kwargs):
            nonlocal gated_calls
            event_type = (
                args[1] if len(args) > 1 else kwargs.get("event_type")
            )
            should_wait = False
            if event_type == "run.started":
                with gate_lock:
                    if gated_calls < 2:
                        gated_calls += 1
                        should_wait = True
            if should_wait:
                append_barrier.wait(timeout=5)
            return original_append_event(*args, **kwargs)

        self.store.append_event = gated_append_event
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda _index: scheduler.resume("resume-race-run"),
                    range(2),
                )
            )

        self.assertEqual(
            {result.status for result in results},
            {RunStatus.RUNNING},
        )
        after_started = [
            event.event_type
            for event in self.store.list_events("resume-race-run")
        ].count("run.started")
        self.assertEqual(after_started, before_started + 1)
        self.assertTrue(self.store.verify_projections("resume-race-run"))

    def test_control_intent_does_not_swallow_unadvanced_conflict(self) -> None:
        scheduler = self._scheduler(namespace="control-real-conflict")
        scheduler.create_run("control-real-conflict-run")
        scheduler.reconcile("control-real-conflict-run")
        before = self.store.get_run("control-real-conflict-run")
        assert before is not None
        calls = 0
        original_append_event = self.store.append_event

        def reject_without_projection_change(*args, **kwargs):
            nonlocal calls
            event_type = (
                args[1] if len(args) > 1 else kwargs.get("event_type")
            )
            if event_type == "run.cancelling":
                calls += 1
                raise ProjectionConflictError("non-stale projection conflict")
            return original_append_event(*args, **kwargs)

        self.store.append_event = reject_without_projection_change
        with self.assertRaisesRegex(
            ProjectionConflictError,
            "non-stale projection conflict",
        ):
            scheduler.request_cancel("control-real-conflict-run")

        after = self.store.get_run("control-real-conflict-run")
        assert after is not None
        self.assertEqual(calls, 1)
        self.assertEqual(after, before)

    def test_control_intent_does_not_retry_real_conflict_after_version_advance(
        self,
    ) -> None:
        scheduler = self._scheduler(namespace="control-advanced-conflict")
        scheduler.create_run("control-advanced-conflict-run")
        scheduler.reconcile("control-advanced-conflict-run")
        before = self.store.get_run("control-advanced-conflict-run")
        assert before is not None
        calls = 0
        original_append_event = self.store.append_event

        def reject_after_projection_change(*args, **kwargs):
            nonlocal calls
            event_type = (
                args[1] if len(args) > 1 else kwargs.get("event_type")
            )
            if event_type == "run.cancelling":
                calls += 1
                current = self.store.get_run(
                    "control-advanced-conflict-run"
                )
                assert current is not None
                original_append_event(
                    current.run_id,
                    "audit.note",
                    expected_run_version=current.projection_version,
                    payload={"reason": "independent committed update"},
                )
                raise ProjectionConflictError(
                    "advanced non-CAS projection conflict"
                )
            return original_append_event(*args, **kwargs)

        self.store.append_event = reject_after_projection_change
        with self.assertRaisesRegex(
            ProjectionConflictError,
            "advanced non-CAS projection conflict",
        ):
            scheduler.request_cancel("control-advanced-conflict-run")

        after = self.store.get_run("control-advanced-conflict-run")
        assert after is not None
        self.assertEqual(calls, 1)
        self.assertEqual(after.status, RunStatus.RUNNING)
        self.assertEqual(
            after.projection_version,
            before.projection_version + 1,
        )

    def test_f18_pause_restart_blocks_new_claim_until_safe_boundary(self) -> None:
        scheduler = self._scheduler(namespace="f18-first")
        scheduler.create_run("f18-run")
        claim = scheduler.claim_next("f18-run", "worker", lease_seconds=30)
        assert claim is not None
        scheduler.start_claim(claim)
        run = self.store.get_run("f18-run")
        assert run is not None
        self.store.append_event(
            "f18-run",
            "run.pausing",
            payload={"intent": "pause"},
            run_projection=replace(run, status=RunStatus.PAUSING),
            occurred_at=101,
        )

        restarted = self._scheduler(namespace="f18-restart")
        self.assertIsNone(restarted.claim_next("f18-run", "other-worker"))
        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )
        restarted.confirm_pause_claim(claim)
        self.assertEqual(
            self.store.get_run("f18-run").status,
            RunStatus.PAUSED,
        )

    def test_f19_waiting_approval_restart_keeps_single_resolution(self) -> None:
        scheduler = self._scheduler(approval=True, namespace="f19-first")
        scheduler.create_run("f19-run")
        scheduler.reconcile("f19-run")
        before = len(self.store.list_events("f19-run"))
        self.assertEqual(
            self.store.get_node("f19-run", "approval").status,
            NodeStatus.WAITING_APPROVAL,
        )

        restarted = self._scheduler(approval=True, namespace="f19-restart")
        restarted.reconcile("f19-run")
        self.assertEqual(len(self.store.list_events("f19-run")), before)
        resolution = ApprovalResolution(
            approval_id="approval-f19",
            run_id="f19-run",
            node_id="approval",
            definition_digest=restarted.workflow.definition_digest,
            approved=True,
            decision_digest="a" * 64,
        )
        restarted.resolve_approval("f19-run", "approval", resolution)
        events = self.store.list_events("f19-run")
        self.assertEqual(
            sum(
                event.payload.get("control_plane") == "approval_resolved"
                for event in events
            ),
            1,
        )
        self.assertEqual(self.store.list_attempts("f19-run"), [])

    def test_f23_f24_telemetry_neither_controls_nor_invents_domain_state(self) -> None:
        scheduler = self._scheduler(namespace="f24")
        attempt = self._scheduled_attempt(scheduler, "telemetry-run")
        sink = _Sink()
        cursor = SQLiteTelemetryCursorStore(self.root / "telemetry.sqlite3")
        bridge = DomainTelemetryBridge(
            self.store,
            sink,
            cursor,
            exporter_id="fault-matrix",
        )
        first = bridge.export_run("telemetry-run")
        self.assertEqual(
            first.attempted,
            len(self.store.list_events("telemetry-run")),
        )
        before_domain = self.store.get_attempt(attempt.attempt_id)
        before_events = len(self.store.list_events("telemetry-run"))
        before_telemetry = len(sink.events)

        def fail(stage: str) -> None:
            if stage == "event.after_insert":
                raise RuntimeError("domain commit failed")

        self.store._fault = fail
        with self.assertRaisesRegex(RuntimeError, "domain commit failed"):
            self.store.claim_activity(
                "telemetry-run",
                "work",
                attempt.attempt_id,
                attempt.metadata["request_hash"],
                "worker",
                now=101,
            )
        self.store._fault = lambda _stage: None
        second = bridge.export_run("telemetry-run")

        self.assertEqual(second.attempted, 0)
        self.assertEqual(len(sink.events), before_telemetry)
        self.assertEqual(
            self.store.get_attempt(attempt.attempt_id),
            before_domain,
        )
        self.assertEqual(
            len(self.store.list_events("telemetry-run")),
            before_events,
        )
        self.assertTrue(self.store.verify_projections("telemetry-run"))

    def test_f25_projection_damage_rebuilds_from_unchanged_events(self) -> None:
        scheduler = self._scheduler(namespace="f25")
        scheduler.create_run("f25-run")
        scheduler.reconcile("f25-run")
        canonical = self.store.rebuild_projections("f25-run")
        event_count = len(self.store.list_events("f25-run"))
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "UPDATE runs SET status=? WHERE run_id=?",
                (RunStatus.WAITING_RECOVERY.value, "f25-run"),
            )

        self.assertFalse(self.store.verify_projections("f25-run"))
        rebuilt = self.store.rebuild_projections("f25-run")
        self.assertEqual(rebuilt, canonical)
        self.assertEqual(len(self.store.list_events("f25-run")), event_count)

    def test_f26_unknown_event_schema_requires_migration(self) -> None:
        run = RunRecord(
            "f26-run",
            "workflow",
            definition_digest="c" * 64,
            last_event_sequence=1,
            projection_version=1,
            created_at=1,
            updated_at=1,
        )
        event = SimpleNamespace(
            run_id="f26-run",
            seq=1,
            event_type="run.created",
            payload={"projection": {"run": run.to_dict()}},
            occurred_at=1,
            schema_version=99,
            node_id=None,
            attempt_id=None,
        )

        with self.assertRaises(ReplayIntegrityError) as caught:
            logical_replay(_SingleEventStore(event), "f26-run")
        self.assertEqual(caught.exception.code, "unknown_schema")
        self.assertIn("migration", str(caught.exception))

    def test_f27_f28_event_id_replay_or_conflict_is_fail_closed(self) -> None:
        run = self.store.create_run(
            RunRecord(
                "event-id-run",
                "workflow",
                definition_digest="d" * 64,
                created_at=1,
                updated_at=1,
            )
        )
        intended = replace(run, status=RunStatus.RUNNING)
        first = self.store.append_event(
            run.run_id,
            "run.started",
            event_id="fault-stable-event",
            payload={"code": "bounded"},
            run_projection=intended,
            occurred_at=2,
        )
        duplicate = self.store.append_event(
            run.run_id,
            "run.started",
            event_id="fault-stable-event",
            payload={"code": "bounded"},
            run_projection=intended,
            occurred_at=2,
        )
        self.assertEqual(duplicate, first)
        self.assertEqual(len(self.store.list_events(run.run_id)), 2)

        with self.assertRaises(ProjectionConflictError):
            self.store.append_event(
                run.run_id,
                "run.started",
                event_id="fault-stable-event",
                payload={"code": "different"},
                run_projection=intended,
                occurred_at=2,
            )
        self.assertEqual(len(self.store.list_events(run.run_id)), 2)

    def test_f30_explicit_legacy_checkpoint_import_never_uses_latest_or_receipts(
        self,
    ) -> None:
        task_secret = "F30-TASK-SECRET-7c91"
        tool_secret = "F30-TOOL-SECRET-41ac"
        query_secret = "F30-QUERY-SECRET-5e22"
        checkpoint = {
            "schema_version": 1,
            "checkpoint_id": "selected-checkpoint",
            "agent_name": "legacy-agent",
            "status": "interrupted",
            "task": f"Continue selected work {task_secret}",
            "turn": 7,
            "tool_results": [
                {
                    "tool_name": f"dangerous-write-{tool_secret}",
                    "status": "OK",
                }
            ],
        }
        importer = LegacyCheckpointImporter(
            self.store,
            self.artifacts,
            clock=self.clock,
        )
        imported = importer.import_checkpoint(
            "legacy-import-run",
            checkpoint_id="selected-checkpoint",
            checkpoint=checkpoint,
            query=query_secret,
        )
        event_count = len(self.store.list_events("legacy-import-run"))
        replayed = importer.import_checkpoint(
            "legacy-import-run",
            checkpoint_id="selected-checkpoint",
            checkpoint=checkpoint,
            query=query_secret,
        )

        self.assertFalse(imported.replayed)
        self.assertTrue(replayed.replayed)
        self.assertEqual(len(self.store.list_events("legacy-import-run")), event_count)
        self.assertEqual(imported.run.metadata["recovery_mode"], "legacy_prompt")
        self.assertFalse(imported.run.metadata["exact_recovery"])
        self.assertEqual(
            imported.run.metadata["source_checkpoint_id"],
            "selected-checkpoint",
        )
        prompt = self.artifacts.read(imported.resume_prompt_ref).decode("utf-8")
        self.assertIn("selected-checkpoint", prompt)
        self.assertIn(task_secret, prompt)
        self.assertIn(tool_secret, prompt)
        self.assertIn(query_secret, prompt)
        self.assertEqual(self.store.list_attempts("legacy-import-run"), [])
        self.assertNotIn(
            "attempt.succeeded",
            {
                event.event_type
                for event in self.store.list_events("legacy-import-run")
            },
        )
        durable_bytes = b"".join(
            path.read_bytes()
            for path in (
                self.database,
                Path(str(self.database) + "-wal"),
                Path(str(self.database) + "-shm"),
            )
            if path.exists()
        )
        for secret in (task_secret, tool_secret, query_secret):
            self.assertNotIn(secret.encode("utf-8"), durable_bytes)
        self.assertNotIn(b'"verification":"verified"', durable_bytes)
        with self.assertRaisesRegex(
            LegacyCheckpointImportError,
            "explicit",
        ):
            importer.import_checkpoint(
                "latest-import",
                checkpoint_id="latest",
                checkpoint={**checkpoint, "checkpoint_id": "latest"},
            )
        with self.assertRaisesRegex(
            LegacyCheckpointImportError,
            "does not match",
        ):
            importer.import_checkpoint(
                "wrong-import",
                checkpoint_id="selected-checkpoint",
                checkpoint={**checkpoint, "checkpoint_id": "other-checkpoint"},
            )
        with self.assertRaisesRegex(
            LegacyCheckpointImportError,
            "query exceeds",
        ):
            importer.import_checkpoint(
                "oversized-query",
                checkpoint_id="selected-checkpoint",
                checkpoint=checkpoint,
                query="x" * (16 * 1024 + 1),
            )
        with self.assertRaisesRegex(
            LegacyCheckpointImportError,
            "rendered resume prompt exceeds",
        ):
            importer.import_checkpoint(
                "oversized-prompt",
                checkpoint_id="selected-checkpoint",
                checkpoint={**checkpoint, "plan": "p" * (128 * 1024)},
            )


if __name__ == "__main__":
    unittest.main()
