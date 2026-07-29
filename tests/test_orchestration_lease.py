from __future__ import annotations

import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path

from src.orchestration.artifacts import LocalArtifactStore
from src.orchestration.lease import (
    DurableLeaseReaper,
    ProbeOutcome,
    ProbeResult,
    RecoveryRetryPolicy,
)
from src.orchestration.models import (
    AttemptRecord,
    AttemptStatus,
    IdempotencyStatus,
    NodeRecord,
    NodeStatus,
    RunRecord,
    RunStatus,
)
from src.orchestration.store import (
    DurableRunStore,
    IdempotencyConflictError,
    InvalidStateTransition,
)


DEFINITION_DIGEST = "a" * 64
REQUEST_HASH = "request-hash"


class _Probe:
    def __init__(self, result: ProbeResult | BaseException) -> None:
        self.result = result
        self.calls = 0

    def probe(self, _attempt: AttemptRecord) -> ProbeResult:
        self.calls += 1
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


class _FaultStore(DurableRunStore):
    fail_stage: str | None = None

    def _fault(self, stage: str) -> None:
        if stage == self.fail_stage:
            raise RuntimeError(f"injected fault at {stage}")


class DurableLeaseRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "orchestration.sqlite3"
        self.store = DurableRunStore(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _create_ready_node(self, run_id: str) -> None:
        created = self.store.create_run(
            RunRecord(
                run_id,
                "workflow",
                definition_digest=DEFINITION_DIGEST,
                created_at=1,
                updated_at=1,
            )
        )
        self.store.append_event(
            run_id,
            "run.started",
            occurred_at=2,
            run_projection=replace(created, status=RunStatus.RUNNING),
        )
        run = self.store.get_run(run_id)
        assert run is not None
        self.store.append_event(
            run_id,
            "node.created",
            occurred_at=3,
            run_projection=run,
            node_projection=NodeRecord(
                run_id,
                "node",
                "activity",
                created_at=3,
                updated_at=3,
            ),
        )
        run = self.store.get_run(run_id)
        node = self.store.get_node(run_id, "node")
        assert run is not None and node is not None
        self.store.append_event(
            run_id,
            "node.ready",
            occurred_at=4,
            run_projection=run,
            node_projection=replace(node, status=NodeStatus.READY),
        )

    def _schedule(
        self,
        run_id: str,
        *,
        attempt_number: int = 1,
        effect_class: str = "read_only",
        activity_kind: str = "tool",
        tool_name: str = "default-tool",
        scheduled_at: float = 5,
    ) -> AttemptRecord:
        if self.store.get_run(run_id) is None:
            self._create_ready_node(run_id)
        run = self.store.get_run(run_id)
        node = self.store.get_node(run_id, "node")
        assert run is not None and node is not None
        attempt = AttemptRecord(
            f"{run_id}-attempt-{attempt_number}",
            run_id,
            node.node_id,
            attempt_number,
            idempotency_key=f"operation:attempt:{attempt_number}",
            activity_kind=activity_kind,
            effect_class=effect_class,
            metadata={
                "request_hash": REQUEST_HASH,
                "operation_key": "operation",
                "tool_name": tool_name,
            },
            scheduled_at=scheduled_at,
        )
        self.store.append_event(
            run_id,
            "attempt.scheduled",
            occurred_at=scheduled_at,
            run_projection=run,
            attempt_projection=attempt,
        )
        stored = self.store.get_attempt(attempt.attempt_id)
        assert stored is not None
        return stored

    def _claim(
        self,
        attempt: AttemptRecord,
        *,
        now: float = 10,
        lease_seconds: float = 5,
        owner: str = "worker-1",
    ):
        claim, event = self.store.claim_activity(
            attempt.run_id,
            attempt.node_id,
            attempt.attempt_id,
            REQUEST_HASH,
            owner,
            lease_seconds=lease_seconds,
            now=now,
        )
        self.assertIsNotNone(event)
        return claim

    def _start(
        self,
        attempt: AttemptRecord,
        claim,
        *,
        now: float = 11,
        owner: str = "worker-1",
    ) -> None:
        self.store.start_activity(
            attempt.run_id,
            attempt.node_id,
            attempt.attempt_id,
            owner,
            claim_token=claim.record.claim_token,
            now=now,
        )

    def test_renew_is_atomic_and_reaper_wins_expired_concurrency(self) -> None:
        attempt = self._schedule("renew-race")
        claim = self._claim(attempt)
        renewed = self.store.renew_activity_lease(
            attempt.run_id,
            attempt.node_id,
            attempt.attempt_id,
            REQUEST_HASH,
            "worker-1",
            claim_token=claim.record.claim_token,
            fencing_token=claim.record.claim_count,
            lease_seconds=10,
            now=12,
        )
        self.assertEqual(renewed.lease_expires_at, 22)
        self.assertEqual(
            DurableLeaseReaper(self.store).run_once(now=16).scanned,
            0,
        )
        with self.assertRaises(IdempotencyConflictError):
            self.store.renew_activity_lease(
                attempt.run_id,
                attempt.node_id,
                attempt.attempt_id,
                REQUEST_HASH,
                "another-worker",
                claim_token=claim.record.claim_token,
                fencing_token=claim.record.claim_count,
                now=13,
            )
        with self.assertRaises(IdempotencyConflictError):
            self.store.renew_activity_lease(
                attempt.run_id,
                attempt.node_id,
                attempt.attempt_id,
                REQUEST_HASH,
                "worker-1",
                claim_token="stale-token",
                fencing_token=claim.record.claim_count,
                now=13,
            )
        with self.assertRaises(IdempotencyConflictError):
            self.store.renew_activity_lease(
                attempt.run_id,
                attempt.node_id,
                attempt.attempt_id,
                REQUEST_HASH,
                "worker-1",
                claim_token=claim.record.claim_token,
                fencing_token=claim.record.claim_count + 1,
                now=13,
            )

        barrier = threading.Barrier(6)
        reports = []
        renew_conflicts = []
        unexpected: list[BaseException] = []

        def reap() -> None:
            try:
                barrier.wait()
                reports.append(DurableLeaseReaper(self.store).run_once(now=23))
            except BaseException as exc:
                unexpected.append(exc)

        def renew() -> None:
            try:
                barrier.wait()
                self.store.renew_activity_lease(
                    attempt.run_id,
                    attempt.node_id,
                    attempt.attempt_id,
                    REQUEST_HASH,
                    "worker-1",
                    claim_token=claim.record.claim_token,
                    fencing_token=claim.record.claim_count,
                    now=23,
                )
            except IdempotencyConflictError:
                renew_conflicts.append(True)
            except BaseException as exc:
                unexpected.append(exc)

        threads = [threading.Thread(target=reap) for _ in range(3)]
        threads.extend(threading.Thread(target=renew) for _ in range(3))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(unexpected, [])
        self.assertEqual(len(renew_conflicts), 3)
        self.assertEqual(sum(len(report.resolved) for report in reports), 1)
        recovered = self.store.get_attempt(attempt.attempt_id)
        record = self.store.get_idempotency(attempt.run_id, attempt.idempotency_key)
        assert recovered is not None and record is not None
        self.assertEqual(recovered.status, AttemptStatus.ABANDONED)
        self.assertEqual(record.status, IdempotencyStatus.COMPLETED)
        self.assertEqual(recovered.fencing_token, claim.record.claim_count + 1)

    def test_claimed_crash_retries_once_and_reaper_restart_is_idempotent(self) -> None:
        attempt = self._schedule("claimed-crash")
        self._claim(attempt)
        reaper = DurableLeaseReaper(
            DurableRunStore(self.db_path),
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(
                max_attempts=3,
                retry_delay_seconds=5,
            ),
        )

        report = reaper.run_once(now=20)
        self.assertEqual(len(report.resolved), 1)
        self.assertEqual(report.runs_to_reconcile, (attempt.run_id,))
        recovered = self.store.get_attempt(attempt.attempt_id)
        node = self.store.get_node(attempt.run_id, attempt.node_id)
        assert recovered is not None and node is not None
        self.assertEqual(recovered.status, AttemptStatus.ABANDONED)
        self.assertEqual(node.status, NodeStatus.WAITING_RETRY)
        self.assertEqual(node.metadata["retry_due_at"], 25)
        event_count = len(self.store.list_events(attempt.run_id))

        restarted = DurableLeaseReaper(
            DurableRunStore(self.db_path),
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(max_attempts=3),
        )
        repeat = restarted.run_once(now=30)
        self.assertEqual(repeat.scanned, 0)
        self.assertEqual(len(self.store.list_events(attempt.run_id)), event_count)

    def test_claimed_retry_exhaustion_uses_narrow_ready_to_failed_exception(self) -> None:
        attempt = self._schedule("claimed-exhausted")
        claim = self._claim(attempt)
        claimed = self.store.get_attempt(attempt.attempt_id)
        run = self.store.get_run(attempt.run_id)
        node = self.store.get_node(attempt.run_id, attempt.node_id)
        assert claimed is not None and run is not None and node is not None
        with self.assertRaises(InvalidStateTransition):
            self.store.append_event(
                attempt.run_id,
                "attempt.abandoned",
                occurred_at=20,
                run_projection=run,
                node_projection=replace(node, status=NodeStatus.FAILED),
                attempt_projection=replace(
                    claimed,
                    status=AttemptStatus.ABANDONED,
                    finished_at=20,
                ),
            )

        report = DurableLeaseReaper(self.store).run_once(now=20)
        self.assertEqual(report.resolved[0].resolution, "abandon_failed")
        recovered = self.store.get_attempt(attempt.attempt_id)
        node = self.store.get_node(attempt.run_id, attempt.node_id)
        record = self.store.get_idempotency(attempt.run_id, attempt.idempotency_key)
        assert recovered is not None and node is not None and record is not None
        self.assertEqual(recovered.status, AttemptStatus.ABANDONED)
        self.assertEqual(node.status, NodeStatus.FAILED)
        self.assertEqual(record.claim_count, claim.record.claim_count + 1)
        self.assertEqual(record.result["error_code"], "attempts_exhausted")

    def test_running_effect_classes_recover_conservatively(self) -> None:
        artifact_store = LocalArtifactStore(
            Path(self.temp_dir.name) / "artifacts",
        )
        committed_ref = artifact_store.put_bytes(b"durable")
        cases = (
            (
                "read-only",
                "read_only",
                None,
                AttemptStatus.ABANDONED,
                NodeStatus.WAITING_RETRY,
                RunStatus.RUNNING,
            ),
            (
                "idempotent-unknown",
                "idempotent_write",
                _Probe(ProbeResult(ProbeOutcome.UNKNOWN)),
                AttemptStatus.OUTCOME_UNKNOWN,
                NodeStatus.WAITING_RECOVERY,
                RunStatus.WAITING_RECOVERY,
            ),
            (
                "idempotent-not-committed",
                "idempotent_write",
                _Probe(ProbeResult(ProbeOutcome.NOT_COMMITTED)),
                AttemptStatus.ABANDONED,
                NodeStatus.WAITING_RETRY,
                RunStatus.RUNNING,
            ),
            (
                "idempotent-committed",
                "idempotent_write",
                _Probe(
                    ProbeResult(
                        ProbeOutcome.COMMITTED,
                        (committed_ref,),
                        "c" * 64,
                    )
                ),
                AttemptStatus.SUCCEEDED,
                NodeStatus.SUCCEEDED,
                RunStatus.RUNNING,
            ),
            (
                "non-idempotent",
                "non_idempotent_write",
                _Probe(
                    ProbeResult(
                        ProbeOutcome.COMMITTED,
                        (committed_ref,),
                    )
                ),
                AttemptStatus.OUTCOME_UNKNOWN,
                NodeStatus.WAITING_RECOVERY,
                RunStatus.WAITING_RECOVERY,
            ),
        )
        for (
            run_id,
            effect_class,
            probe,
            expected_attempt,
            expected_node,
            expected_run,
        ) in cases:
            with self.subTest(effect_class=effect_class, run_id=run_id):
                attempt = self._schedule(run_id, effect_class=effect_class)
                claim = self._claim(attempt)
                self._start(attempt, claim)
                reaper = DurableLeaseReaper(
                    self.store,
                    retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(
                        max_attempts=3
                    ),
                    probe_resolver=(
                        None if probe is None else lambda _attempt, probe=probe: probe
                    ),
                    artifact_verifier=artifact_store,
                )
                report = reaper.run_once(now=20)
                self.assertEqual(len(report.resolved), 1)
                recovered = self.store.get_attempt(attempt.attempt_id)
                node = self.store.get_node(run_id, attempt.node_id)
                run = self.store.get_run(run_id)
                assert recovered is not None and node is not None and run is not None
                self.assertEqual(recovered.status, expected_attempt)
                self.assertEqual(node.status, expected_node)
                self.assertEqual(run.status, expected_run)
                if run_id == "idempotent-committed":
                    self.assertEqual(
                        recovered.result["artifact_refs"][0]["artifact_id"],
                        committed_ref.artifact_id,
                    )
                    self.assertEqual(
                        recovered.result["artifact_refs"][0],
                        committed_ref.to_dict(),
                    )
                if run_id == "non-idempotent":
                    assert probe is not None
                    self.assertEqual(probe.calls, 0)

    def test_probe_resolution_uses_attempt_identity_and_artifacts_fail_closed(self) -> None:
        artifact_store = LocalArtifactStore(
            Path(self.temp_dir.name) / "identity-artifacts",
        )
        valid_ref = artifact_store.put_bytes(b"valid-result")
        corrupt_ref = artifact_store.put_bytes(b"will-be-corrupted")
        (Path(self.temp_dir.name) / "identity-artifacts" / corrupt_ref.uri).write_bytes(
            b"different-size-and-digest"
        )
        service_a_probe = _Probe(
            ProbeResult(ProbeOutcome.COMMITTED, (valid_ref,))
        )
        service_c_probe = _Probe(
            ProbeResult(ProbeOutcome.COMMITTED, (corrupt_ref,))
        )
        attempts = {}
        for run_id, tool_name in (
            ("identity-a", "service-a"),
            ("identity-b", "service-b"),
            ("identity-c", "service-c"),
        ):
            attempt = self._schedule(
                run_id,
                effect_class="idempotent_write",
                tool_name=tool_name,
            )
            claim = self._claim(attempt)
            self._start(attempt, claim)
            attempts[tool_name] = attempt

        def resolve(attempt: AttemptRecord):
            return {
                "service-a": service_a_probe,
                "service-c": service_c_probe,
            }.get(attempt.metadata.get("tool_name"))

        report = DurableLeaseReaper(
            self.store,
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(max_attempts=3),
            probe_resolver=resolve,
            artifact_verifier=artifact_store,
        ).run_once(now=20)
        self.assertEqual(len(report.resolved), 3)
        self.assertEqual(
            self.store.get_attempt(attempts["service-a"].attempt_id).status,
            AttemptStatus.SUCCEEDED,
        )
        self.assertEqual(
            self.store.get_attempt(attempts["service-b"].attempt_id).status,
            AttemptStatus.OUTCOME_UNKNOWN,
        )
        self.assertEqual(
            self.store.get_attempt(attempts["service-c"].attempt_id).status,
            AttemptStatus.OUTCOME_UNKNOWN,
        )
        self.assertEqual(service_a_probe.calls, 1)
        self.assertEqual(service_c_probe.calls, 1)

    def test_store_rejects_incomplete_verified_artifact_receipts(self) -> None:
        attempt = self._schedule(
            "incomplete-artifact-receipt",
            effect_class="idempotent_write",
        )
        claim = self._claim(attempt)
        self._start(attempt, claim)
        with self.assertRaises(ValueError):
            self.store.recover_expired_activity(
                attempt.run_id,
                attempt.node_id,
                attempt.attempt_id,
                REQUEST_HASH,
                "worker-1",
                claim_token=claim.record.claim_token,
                fencing_token=claim.record.claim_count,
                resolution="verified_succeeded",
                verified_result={
                    "outcome": "succeeded",
                    "verification": "verified",
                    "artifact_refs": [
                        {
                            "artifact_id": "fake",
                            "sha256": "a" * 64,
                            "size": 1,
                            "kind": "generic",
                            "uri": "sha256/aa/aa/" + "a" * 64,
                        }
                    ],
                    "external_operation_id_digest": None,
                },
                now=20,
            )
        current = self.store.get_attempt(attempt.attempt_id)
        record = self.store.get_idempotency(attempt.run_id, attempt.idempotency_key)
        assert current is not None and record is not None
        self.assertEqual(current.status, AttemptStatus.RUNNING)
        self.assertEqual(record.status, IdempotencyStatus.IN_PROGRESS)

    def test_expired_workers_cannot_start_complete_pause_or_retry(self) -> None:
        claimed = self._schedule("expired-start")
        claimed_claim = self._claim(claimed)
        with self.assertRaises(IdempotencyConflictError):
            self.store.start_activity(
                claimed.run_id,
                claimed.node_id,
                claimed.attempt_id,
                "worker-1",
                claim_token=claimed_claim.record.claim_token,
                now=16,
            )

        running = self._schedule("expired-complete")
        running_claim = self._claim(running)
        self._start(running, running_claim)
        common = (
            running.run_id,
            running.node_id,
            running.attempt_id,
            REQUEST_HASH,
            "worker-1",
        )
        with self.assertRaises(IdempotencyConflictError):
            self.store.complete_activity(
                *common,
                claim_token=running_claim.record.claim_token,
                result={"outcome": "succeeded"},
                now=16,
            )
        with self.assertRaises(IdempotencyConflictError):
            self.store.pause_activity(
                *common,
                claim_token=running_claim.record.claim_token,
                now=16,
            )
        with self.assertRaises(IdempotencyConflictError):
            self.store.complete_retryable_activity(
                *common,
                claim_token=running_claim.record.claim_token,
                error_class="transient",
                error_code="timeout",
                retry_due_at=20,
                now=16,
            )

    def test_retryable_completion_is_atomic_and_receipt_is_bounded(self) -> None:
        attempt = self._schedule("retryable")
        claim = self._claim(attempt)
        self._start(attempt, claim)
        completed, event = self.store.complete_retryable_activity(
            attempt.run_id,
            attempt.node_id,
            attempt.attempt_id,
            REQUEST_HASH,
            "worker-1",
            claim_token=claim.record.claim_token,
            error_class="transport",
            error_code="upstream_timeout",
            retry_due_at=30,
            attempt_status=AttemptStatus.TIMED_OUT,
            now=12,
        )
        self.assertEqual(completed.status, IdempotencyStatus.COMPLETED)
        self.assertEqual(
            completed.result,
            {
                "outcome": "timed_out",
                "error_class": "transport",
                "error_code": "upstream_timeout",
            },
        )
        self.assertEqual(event.event_type, "attempt.timed_out")
        stored = self.store.get_attempt(attempt.attempt_id)
        node = self.store.get_node(attempt.run_id, attempt.node_id)
        run = self.store.get_run(attempt.run_id)
        assert stored is not None and node is not None and run is not None
        self.assertEqual(stored.status, AttemptStatus.TIMED_OUT)
        self.assertEqual(node.status, NodeStatus.WAITING_RETRY)
        self.assertEqual(node.metadata["retry_due_at"], 30)
        self.assertEqual(run.status, RunStatus.RUNNING)

    def test_recovery_and_retryable_faults_roll_back_entire_transaction(self) -> None:
        self.store = _FaultStore(self.db_path)
        attempt = self._schedule("rollback-recovery")
        claim = self._claim(attempt)
        self._start(attempt, claim)
        before_events = len(self.store.list_events(attempt.run_id))
        self.store.fail_stage = "lease_recovery.after_idempotency"
        with self.assertRaises(RuntimeError):
            DurableLeaseReaper(self.store).run_once(now=20)
        current = self.store.get_attempt(attempt.attempt_id)
        record = self.store.get_idempotency(attempt.run_id, attempt.idempotency_key)
        assert current is not None and record is not None
        self.assertEqual(current.status, AttemptStatus.RUNNING)
        self.assertEqual(record.status, IdempotencyStatus.IN_PROGRESS)
        self.assertEqual(len(self.store.list_events(attempt.run_id)), before_events)

        self.store.fail_stage = None
        retryable = self._schedule("rollback-retryable")
        retry_claim = self._claim(retryable)
        self._start(retryable, retry_claim)
        before_events = len(self.store.list_events(retryable.run_id))
        self.store.fail_stage = "complete_retryable.after_idempotency"
        with self.assertRaises(RuntimeError):
            self.store.complete_retryable_activity(
                retryable.run_id,
                retryable.node_id,
                retryable.attempt_id,
                REQUEST_HASH,
                "worker-1",
                claim_token=retry_claim.record.claim_token,
                error_class="transport",
                error_code="timeout",
                retry_due_at=30,
                now=12,
            )
        current = self.store.get_attempt(retryable.attempt_id)
        record = self.store.get_idempotency(
            retryable.run_id,
            retryable.idempotency_key,
        )
        assert current is not None and record is not None
        self.assertEqual(current.status, AttemptStatus.RUNNING)
        self.assertEqual(record.status, IdempotencyStatus.IN_PROGRESS)
        self.assertEqual(len(self.store.list_events(retryable.run_id)), before_events)

    def test_reaper_honors_cancellation_race(self) -> None:
        attempt = self._schedule("cancel-race")
        self._claim(attempt)
        run = self.store.get_run(attempt.run_id)
        assert run is not None
        self.store.append_event(
            attempt.run_id,
            "run.cancelling",
            occurred_at=16,
            run_projection=replace(run, status=RunStatus.CANCELLING),
        )

        report = DurableLeaseReaper(self.store).run_once(now=20)
        self.assertEqual(report.resolved[0].resolution, "cancelled")
        recovered = self.store.get_attempt(attempt.attempt_id)
        node = self.store.get_node(attempt.run_id, attempt.node_id)
        run = self.store.get_run(attempt.run_id)
        assert recovered is not None and node is not None and run is not None
        self.assertEqual(recovered.status, AttemptStatus.CANCELLED)
        self.assertEqual(node.status, NodeStatus.CANCELLED)
        self.assertEqual(run.status, RunStatus.CANCELLING)

    def test_fencing_is_monotonic_across_attempt_scoped_idempotency_keys(self) -> None:
        first = self._schedule("fencing")
        first_claim = self._claim(first)
        self._start(first, first_claim)
        DurableLeaseReaper(
            self.store,
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(max_attempts=3),
        ).run_once(now=20)
        run = self.store.get_run(first.run_id)
        node = self.store.get_node(first.run_id, first.node_id)
        assert run is not None and node is not None
        self.store.append_event(
            first.run_id,
            "node.ready",
            occurred_at=21,
            run_projection=run,
            node_projection=replace(node, status=NodeStatus.READY),
        )

        self.store = DurableRunStore(self.db_path)
        second = self._schedule(
            first.run_id,
            attempt_number=2,
            scheduled_at=22,
        )
        second_claim = self._claim(second, now=23)
        self.assertEqual(first_claim.record.claim_count, 1)
        self.assertEqual(second_claim.record.claim_count, 2)
        stored_second = self.store.get_attempt(second.attempt_id)
        assert stored_second is not None
        self.assertEqual(stored_second.fencing_token, 2)

        with self.assertRaises(IdempotencyConflictError):
            self.store.complete_activity(
                first.run_id,
                first.node_id,
                first.attempt_id,
                REQUEST_HASH,
                "worker-1",
                claim_token=first_claim.record.claim_token,
                result={"outcome": "succeeded"},
                now=24,
            )


if __name__ == "__main__":
    unittest.main()
