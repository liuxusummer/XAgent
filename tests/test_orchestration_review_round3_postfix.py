from __future__ import annotations

import hashlib
import sqlite3
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.orchestration.artifact_broker import (
    ArtifactGrantBroker,
    ArtifactGrantConsumed,
    ArtifactGrantDenied,
)
from src.orchestration.artifacts import (
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    LocalArtifactStore,
)
from src.orchestration.executor import TrustedActivityExecutor
from src.orchestration.models import (
    AttemptStatus,
    NodeStatus,
    RunStatus,
)
from src.orchestration.remote_control import (
    RemoteCompletionCandidate,
    RemoteControlPlane,
)
from src.orchestration.remote_execution import RemoteExecutionPreparation
from src.orchestration.remote_journal import (
    RemoteControlJournal,
    RemoteJournalError,
)
from src.orchestration.remote_scheduling import (
    RemoteTask,
    WorkerDescriptor,
    is_worker_compatible,
)
from src.orchestration.remote_worker import RemoteWorkerClient
from src.orchestration.sandbox import SandboxOutcome
from src.orchestration.scheduler import DurableScheduler
from src.orchestration.workflow import compile_workflow

from tests import test_orchestration_remote_execution as secure_tests


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class _BlockingArtifactStore(LocalArtifactStore):
    def __init__(self, root: Path) -> None:
        self.write_started = threading.Event()
        self.allow_write = threading.Event()
        self.put_calls = 0
        super().__init__(root)

    def put_bytes(self, content: bytes, **metadata) -> ArtifactRef:
        self.write_started.set()
        if not self.allow_write.wait(timeout=5):
            raise TimeoutError("postfix Store barrier timed out")
        self.put_calls += 1
        return super().put_bytes(content, **metadata)


class Round3PostfixAdversarialReviewTests(unittest.TestCase):
    """Independent post-fix recovery and operations canaries."""

    def setUp(self) -> None:
        self.harness = secure_tests.SecureRemoteExecutionTests(
            methodName="runTest"
        )
        self.harness.setUp()
        self.addCleanup(self.harness.doCleanups)

    def _scheduler(
        self,
        *,
        name: str,
        metadata: dict[str, object] | None = None,
    ) -> tuple[DurableScheduler, TrustedActivityExecutor]:
        workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": name,
                "version": 1,
                "nodes": [
                    {
                        "id": "tool",
                        "kind": "tool",
                        "config": {
                            "tool": "exec",
                            "arguments": {"command": "fixed"},
                        },
                        "metadata": metadata or {},
                        "effect_class": "read_only",
                    }
                ],
            }
        )
        scheduler = DurableScheduler(
            self.harness.store,
            workflow,
            clock=self.harness.clock,
            artifact_verifier=self.harness.artifacts.verify,
            max_active_attempts=16,
        )
        executor = TrustedActivityExecutor(
            scheduler,
            self.harness.executor.policy,
            self.harness.executor.sandbox,
            artifact_verifier=self.harness.artifacts.verify,
            artifact_reader=self.harness.artifacts.read,
            clock=self.harness.clock,
        )
        return scheduler, executor

    def _worker_authorization(self, run_id: str):
        claim = self.harness._claim(run_id)
        admitter = self.harness._admitter()
        admitter.admit(
            self.harness.identity,
            self.harness.registration,
            self.harness.scheduler,
            claim,
        )
        return (
            claim,
            admitter._prepared[claim.attempt_id].worker_authorization,
        )

    @staticmethod
    def _register_journal(
        journal: RemoteControlJournal,
        *,
        request_id: str,
        now: float,
    ):
        return journal.register_session(
            worker_id="worker-postfix",
            tenant_id="tenant-postfix",
            identity_digest=_digest("postfix-identity"),
            instance_id="instance-postfix",
            request_id=request_id,
            request_digest=_digest(request_id),
            operation="register",
            now=now,
        )

    def test_broker_inflight_expiry_purge_and_clock_rollback_keep_budget_linear(
        self,
    ) -> None:
        """Long Store I/O cannot release budget early or revive an expired grant."""

        claim, authorization = self._worker_authorization(
            "postfix-broker-inflight"
        )
        store = _BlockingArtifactStore(
            self.harness.control_root / "postfix-blocking-artifacts"
        )
        broker = ArtifactGrantBroker(
            store,
            authorization_verifier=self.harness.worker_gate,
            clock=self.harness.clock,
            maximum_artifact_bytes=8,
            maximum_resident_bytes=4,
            maximum_active_grants=4,
        )
        content = b"hold"

        def issue(ttl: float):
            return broker.issue_write_grant(
                authorization,
                tenant_id=self.harness.identity.tenant_id,
                run_id=claim.run_id,
                node_id=claim.node_id,
                attempt_id=claim.attempt_id,
                kind=ArtifactKind.TOOL_RESULT,
                sensitivity=ArtifactSensitivity.INTERNAL,
                media_type="application/octet-stream",
                maximum_bytes=len(content),
                declared_sha256=hashlib.sha256(content).hexdigest(),
                ttl_seconds=ttl,
            )

        finalizing = issue(5)
        queued = issue(30)
        staging = broker.stage_write(
            finalizing,
            authorization,
            content=content,
            declared_sha256=finalizing.declared_sha256,
        )
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                broker.finalize_write,
                finalizing,
                authorization,
                staging,
            )
            self.assertTrue(store.write_started.wait(timeout=2))
            self.harness.clock.value = finalizing.expires_at
            # Issuance runs global purge while the first Store call is blocked.
            issue(30)
            record = broker._write_grants[finalizing.grant_id]
            self.assertEqual("expired", record.state)
            self.assertTrue(record.store_write_in_flight)
            self.assertEqual(len(content), broker._resident_bytes)
            with self.assertRaisesRegex(
                ArtifactGrantDenied,
                "resident_byte_limit_reached",
            ):
                broker.stage_write(
                    queued,
                    authorization,
                    content=content,
                    declared_sha256=queued.declared_sha256,
                )
            self.harness.clock.value = finalizing.expires_at - 1
            with self.assertRaises(ArtifactGrantConsumed):
                broker.resolve_output_handle(finalizing.output_handle)
            store.allow_write.set()
            with self.assertRaises(ArtifactGrantConsumed):
                future.result(timeout=2)

        self.assertEqual(1, store.put_calls)
        self.assertEqual(0, broker._resident_bytes)
        broker.stage_write(
            queued,
            authorization,
            content=content,
            declared_sha256=queued.declared_sha256,
        )
        self.assertEqual(len(content), broker._resident_bytes)

    def test_runtime_upper_bound_blocks_store_then_rolling_upgrade_claims(
        self,
    ) -> None:
        """Direct secure poll and RemoteTask must share rolling-version semantics."""

        requirement = {"min_runtime_version": "2", "max_runtime_version": "2.9"}
        scheduler, executor = self._scheduler(
            name="postfix-runtime-rolling",
            metadata=requirement,
        )
        preparation = RemoteExecutionPreparation(
            argv=("trusted-runner",),
            cwd=str(self.harness.agent_root),
            container_cwd="/workspace",
            profile=self.harness.profile,
            resource_locks=(),
        )
        admitter = self.harness._admitter(
            executor=executor,
            plan_resolver=secure_tests._PlanResolver(preparation),
        )
        run_id = "postfix-runtime-run"
        scheduler.create_run(run_id)
        scheduler.reconcile(run_id)
        journal_path = (
            self.harness.control_root / "postfix-runtime-journal.sqlite3"
        )
        control = RemoteControlPlane(
            lambda requested: scheduler if requested == run_id else None,
            authorize_run=lambda identity, requested: (
                identity == self.harness.identity and requested == run_id
            ),
            assignment_admitter=admitter,
            journal=RemoteControlJournal(journal_path),
        )
        task = RemoteTask(
            task_id="postfix-runtime-task",
            tenant_id="tenant-a",
            pool_id="pool-a",
            tool_name="exec",
            min_runtime_version="2",
            max_runtime_version="2.9",
        )
        too_new = WorkerDescriptor(
            worker_id=self.harness.identity.worker_id,
            session_id="postfix-too-new",
            pool_id="pool-a",
            runtime_version="2.10",
            capabilities=frozenset({"activity.tool"}),
            tools=frozenset({"exec"}),
            authorized_tenants=frozenset({"tenant-a"}),
        )
        self.assertFalse(is_worker_compatible(too_new, task))
        before_events = scheduler.store.list_events(run_id)
        old_client = RemoteWorkerClient(
            lambda request: control.handle(self.harness.identity, request),
            worker_id=self.harness.identity.worker_id,
            instance_id="postfix-runtime-old",
        )
        old_client.register(
            runtime_version=too_new.runtime_version,
            capabilities=("activity.tool",),
            resource_keys=(),
            activity_kinds=("tool",),
            max_concurrency=1,
        )
        self.assertIsNone(old_client.poll(run_id))
        self.assertEqual([], scheduler.store.list_attempts(run_id))
        self.assertEqual(before_events, scheduler.store.list_events(run_id))

        compatible = WorkerDescriptor(
            worker_id=self.harness.identity.worker_id,
            session_id="postfix-compatible",
            pool_id="pool-a",
            runtime_version="2.9",
            capabilities=frozenset({"activity.tool"}),
            tools=frozenset({"exec"}),
            authorized_tenants=frozenset({"tenant-a"}),
        )
        self.assertTrue(is_worker_compatible(compatible, task))
        upgraded = RemoteWorkerClient(
            lambda request: control.handle(self.harness.identity, request),
            worker_id=self.harness.identity.worker_id,
            instance_id="postfix-runtime-upgraded",
        )
        upgraded.register(
            runtime_version=compatible.runtime_version,
            capabilities=("activity.tool",),
            resource_keys=(),
            activity_kinds=("tool",),
            max_concurrency=1,
        )
        assignment = upgraded.poll(run_id)
        self.assertIsNotNone(assignment)
        self.assertEqual(1, len(scheduler.store.list_attempts(run_id)))
        different = compile_workflow(
            {
                "schema_version": 2,
                "name": "postfix-runtime-rolling",
                "version": 1,
                "nodes": [
                    {
                        "id": "tool",
                        "kind": "tool",
                        "config": {
                            "tool": "exec",
                            "arguments": {"command": "fixed"},
                        },
                        "metadata": {
                            "min_runtime_version": "2",
                            "max_runtime_version": "2.10",
                        },
                        "effect_class": "read_only",
                    }
                ],
            }
        )
        self.assertNotEqual(
            scheduler.workflow.definition_digest,
            different.definition_digest,
        )

    def test_legal_v1_journal_survives_restart_and_concurrent_open(self) -> None:
        """A valid existing journal remains usable during parallel process startup."""

        path = self.harness.control_root / "postfix-legal-journal.sqlite3"
        bootstrap = RemoteControlJournal(path)
        session = self._register_journal(
            bootstrap,
            request_id="register-bootstrap",
            now=1,
        )
        bootstrap.close()
        barrier = threading.Barrier(9)

        def reopen(_index: int) -> int:
            barrier.wait(timeout=5)
            journal = RemoteControlJournal(path)
            try:
                journal.assert_current(
                    worker_id=session.worker_id,
                    tenant_id=session.tenant_id,
                    identity_digest=session.identity_digest,
                    instance_id=session.instance_id,
                    epoch=session.epoch,
                )
                return session.epoch
            finally:
                journal.close()

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(reopen, index) for index in range(8)]
            barrier.wait(timeout=5)
            epochs = [future.result(timeout=10) for future in futures]
        self.assertEqual([session.epoch] * 8, epochs)
        restarted = RemoteControlJournal(path)
        recovered = self._register_journal(
            restarted,
            request_id="register-after-concurrent-open",
            now=2,
        )
        self.assertEqual(session.epoch, recovered.epoch)

    def test_current_schema_missing_request_table_fails_instead_of_recreating(
        self,
    ) -> None:
        """A partial v1 DB must not silently erase durable request identities."""

        path = self.harness.control_root / "postfix-missing-table.sqlite3"
        journal = RemoteControlJournal(path)
        session = self._register_journal(
            journal,
            request_id="register-before-loss",
            now=1,
        )
        self.assertTrue(
            journal.record_request(
                worker_id=session.worker_id,
                tenant_id=session.tenant_id,
                identity_digest=session.identity_digest,
                instance_id=session.instance_id,
                epoch=session.epoch,
                request_id="poll-before-loss",
                request_digest=_digest("poll-before-loss"),
                operation="poll",
                now=2,
            )
        )
        journal.close()
        connection = sqlite3.connect(path)
        try:
            connection.execute("DROP TABLE remote_request_identities")
            connection.commit()
        finally:
            connection.close()

        try:
            reopened = RemoteControlJournal(path)
        except RemoteJournalError as exc:
            self.assertRegex(
                str(exc),
                "invalid_schema|journal_integrity_failed",
            )
            return
        try:
            replay_was_accepted_as_new = reopened.record_request(
                worker_id=session.worker_id,
                tenant_id=session.tenant_id,
                identity_digest=session.identity_digest,
                instance_id=session.instance_id,
                epoch=session.epoch,
                request_id="poll-before-loss",
                request_digest=_digest("poll-before-loss"),
                operation="poll",
                now=3,
            )
        finally:
            reopened.close()
        self.assertFalse(
            replay_was_accepted_as_new,
            "P1: metadata still declares schema v1, but startup silently "
            "recreated the missing request-identity table and accepted a "
            "previously durable request id as new",
        )

    def test_journal_foreign_key_orphan_fails_integrity_validation(self) -> None:
        """Startup quick validation must reject a structurally valid orphan row."""

        path = self.harness.control_root / "postfix-orphan-journal.sqlite3"
        journal = RemoteControlJournal(path)
        self._register_journal(
            journal,
            request_id="register-valid",
            now=1,
        )
        journal.close()
        connection = sqlite3.connect(path)
        try:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute(
                """
                INSERT INTO remote_request_identities(
                    worker_id, epoch, request_id, request_digest, operation,
                    accepted_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "ghost-worker",
                    999,
                    "ghost-request",
                    _digest("ghost-request"),
                    "poll",
                    2.0,
                ),
            )
            connection.commit()
        finally:
            connection.close()

        with self.assertRaisesRegex(
            RemoteJournalError,
            "journal_integrity_failed",
        ):
            RemoteControlJournal(path)

    def test_signed_cancellation_unknown_never_becomes_retryable(self) -> None:
        """A verified unknown remote outcome must remain a recovery boundary."""

        claim = self.harness._claim("postfix-outcome-unknown")
        admitter = self.harness._admitter()
        authorization = admitter.admit(
            self.harness.identity,
            self.harness.registration,
            self.harness.scheduler,
            claim,
        )
        self.harness.scheduler.start_claim(claim)
        receipt = {
            "schema_version": 2,
            "backend_id": "postfix-attested-gvisor",
            "security_level": "container",
            "profile_id": self.harness.profile.profile_id,
            "profile_digest": authorization.profile_digest,
            "action_digest": authorization.action_digest,
            "policy_version": self.harness.executor.policy.policy_version,
            "request_digest": authorization.request_digest,
            "outcome": SandboxOutcome.CANCELLATION_UNKNOWN.value,
            "exit_code": None,
            "timed_out": False,
            "output_artifact_refs": [],
            "error_code": "cancellation_unknown",
        }
        proof = self.harness.runtime_verifier.make_proof(
            self.harness._binding(claim, authorization),
            receipt,
            output_handles=(),
            outcome=AttemptStatus.OUTCOME_UNKNOWN.value,
        )
        candidate = RemoteCompletionCandidate(
            outcome=AttemptStatus.OUTCOME_UNKNOWN,
            output_handles=(),
            runtime_proof=proof,
            error_class="sandbox",
            error_code="cancellation_unknown",
        )
        admitter.complete(
            self.harness.identity,
            self.harness.registration,
            self.harness.scheduler,
            claim,
            authorization,
            candidate,
            proof,
        )

        attempt = self.harness.store.get_attempt(claim.attempt_id)
        node = self.harness.store.get_node(claim.run_id, claim.node_id)
        run = self.harness.store.get_run(claim.run_id)
        assert attempt is not None and node is not None and run is not None
        self.assertEqual(AttemptStatus.OUTCOME_UNKNOWN, attempt.status)
        self.assertEqual(NodeStatus.WAITING_RECOVERY, node.status)
        self.assertEqual(RunStatus.WAITING_RECOVERY, run.status)
        self.harness.scheduler.reconcile(claim.run_id)
        self.assertIsNone(
            self.harness.scheduler.claim_next(
                claim.run_id,
                self.harness.registration.session_owner_id,
                resource_keys=self.harness.registration.resource_keys,
            )
        )
        self.assertEqual(1, len(self.harness.store.list_attempts(claim.run_id)))


if __name__ == "__main__":
    unittest.main()
