from __future__ import annotations

import hashlib
import sqlite3
import unittest

from src.orchestration.artifact_broker import (
    ArtifactGrantBroker,
    ArtifactGrantConsumed,
    ArtifactGrantDenied,
    ArtifactKind,
    ArtifactOutputHandle,
)
from src.orchestration.executor import TrustedActivityExecutor
from src.orchestration.remote_control import RemoteControlPlane
from src.orchestration.remote_execution import RemoteExecutionPreparation
from src.orchestration.remote_journal import (
    REMOTE_JOURNAL_SCHEMA_VERSION,
    RemoteControlJournal,
    RemoteJournalError,
)
from src.orchestration.remote_scheduling import (
    RemoteTask,
    WorkerDescriptor,
    is_worker_compatible,
)
from src.orchestration.remote_worker import RemoteWorkerClient
from src.orchestration.scheduler import DurableScheduler
from src.orchestration.workflow import compile_workflow

from tests import test_orchestration_remote_execution as secure_tests


class Round3RecoveryAndOperationsReviewTests(unittest.TestCase):
    """Independent recovery/operations attacks against the secure remote path."""

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
        config: dict[str, object],
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
                        "config": config,
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

    def test_p2_secure_poll_has_no_supported_runtime_requirement_path(
        self,
    ) -> None:
        """ADR runtime compatibility has no durable declaration/enforcement path."""

        requirement = RemoteTask(
            task_id="runtime-required-task",
            tenant_id="tenant-a",
            pool_id="pool-a",
            tool_name="exec",
            min_runtime_version="2",
            max_runtime_version="2.9",
        )
        old_worker = WorkerDescriptor(
            worker_id=self.harness.identity.worker_id,
            session_id="old-runtime-session",
            pool_id="pool-a",
            runtime_version="1.9",
            capabilities=frozenset({"activity.tool"}),
            tools=frozenset({"exec"}),
            authorized_tenants=frozenset({"tenant-a"}),
        )
        self.assertFalse(
            is_worker_compatible(old_worker, requirement),
            "the attack fixture must be rejected by the real RemoteTask range",
        )

        scheduler, executor = self._scheduler(
            name="round3-runtime-compatibility",
            config={
                "tool": requirement.tool_name,
                "arguments": {"command": "fixed"},
            },
            metadata={
                # Bind the exact existing RemoteTask compatibility requirement
                # into the immutable durable Workflow definition.
                "min_runtime_version": requirement.min_runtime_version,
                "max_runtime_version": requirement.max_runtime_version,
            },
        )
        durable_node = scheduler.workflow.nodes[0]
        self.assertEqual(
            requirement.min_runtime_version,
            durable_node.metadata["min_runtime_version"],
        )
        self.assertEqual(
            requirement.max_runtime_version,
            durable_node.metadata["max_runtime_version"],
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
        run_id = "round3-runtime-bypass"
        scheduler.create_run(run_id)
        scheduler.reconcile(run_id)
        control = RemoteControlPlane(
            lambda requested: scheduler if requested == run_id else None,
            authorize_run=lambda identity, requested: (
                identity == self.harness.identity and requested == run_id
            ),
            assignment_admitter=admitter,
            journal=RemoteControlJournal(
                self.harness.control_root / "round3-runtime-journal.sqlite3"
            ),
        )
        client = RemoteWorkerClient(
            lambda request: control.handle(self.harness.identity, request),
            worker_id=self.harness.identity.worker_id,
            instance_id="old-runtime-instance",
        )
        client.register(
            runtime_version=old_worker.runtime_version,
            capabilities=("activity.tool",),
            resource_keys=(),
            activity_kinds=("tool",),
            max_concurrency=1,
        )
        assignment = client.poll(run_id)

        self.assertIsNone(
            assignment,
            "an incompatible old runtime bypassed RemoteTask compatibility "
            "by polling the durable secure endpoint directly",
        )
        self.assertEqual(
            [],
            scheduler.store.list_attempts(run_id),
            "runtime incompatibility must be denied before schedule/claim",
        )

    def test_p1_broker_has_a_global_staged_byte_budget_across_attempts(
        self,
    ) -> None:
        """Legal per-Attempt staging must not multiply the broker memory bound."""

        scheduler, executor = self._scheduler(
            name="round3-broker-budget",
            config={"tool": "exec", "arguments": {"command": "fixed"}},
        )
        broker = ArtifactGrantBroker(
            self.harness.artifacts,
            authorization_verifier=self.harness.worker_gate,
            clock=self.harness.clock,
            maximum_artifact_bytes=4,
            maximum_active_grants=8,
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
            artifact_broker=broker,
            plan_resolver=secure_tests._PlanResolver(preparation),
            maximum_prepared=8,
        )

        retained = []
        attempt_ids = []
        for index, content in enumerate((b"AAAA", b"BBBB")):
            run_id = f"round3-broker-attempt-{index}"
            scheduler.create_run(run_id)
            scheduler.reconcile(run_id)
            claim = scheduler.claim_next(
                run_id,
                self.harness.registration.session_owner_id,
                capacity=8,
                resource_keys=(),
            )
            self.assertIsNotNone(claim)
            assert claim is not None
            authorization = admitter.admit(
                self.harness.identity,
                self.harness.registration,
                scheduler,
                claim,
            )
            worker_authorization = admitter._prepared[
                claim.attempt_id
            ].worker_authorization
            digest = hashlib.sha256(content).hexdigest()
            grant = broker.issue_write_grant(
                worker_authorization,
                tenant_id=self.harness.identity.tenant_id,
                run_id=run_id,
                node_id=claim.node_id,
                attempt_id=claim.attempt_id,
                kind=ArtifactKind.TOOL_RESULT,
                sensitivity="internal",
                media_type="application/octet-stream",
                maximum_bytes=len(content),
                declared_sha256=digest,
            )
            attempt_ids.append(claim.attempt_id)
            if index == 0:
                broker.stage_write(
                    grant,
                    worker_authorization,
                    content=content,
                    declared_sha256=digest,
                )
                retained.append(content)
            else:
                with self.assertRaises(
                    ArtifactGrantDenied,
                    msg="the broker accepted bytes beyond its global budget",
                ):
                    broker.stage_write(
                        grant,
                        worker_authorization,
                        content=content,
                        declared_sha256=digest,
                    )

        self.assertEqual(2, len(set(attempt_ids)))
        retained_bytes = sum(
            len(record.content or b"")
            for record in broker._write_grants.values()
        )
        self.assertEqual(sum(map(len, retained)), retained_bytes)
        self.assertLessEqual(
            retained_bytes,
            broker._maximum_artifact_bytes,
            "two individually legal Attempts multiplied the configured broker "
            "byte bound because only grant count is globally accounted",
        )

    def test_p1_finalized_write_records_do_not_permanently_exhaust_broker(
        self,
    ) -> None:
        """Successful expired writes must release active-grant capacity."""

        claim = self.harness._claim("round3-finalized-capacity")
        admitter = self.harness._admitter()
        admitter.admit(
            self.harness.identity,
            self.harness.registration,
            self.harness.scheduler,
            claim,
        )
        worker_authorization = admitter._prepared[
            claim.attempt_id
        ].worker_authorization
        broker = ArtifactGrantBroker(
            self.harness.artifacts,
            authorization_verifier=self.harness.worker_gate,
            clock=self.harness.clock,
            maximum_artifact_bytes=8,
            maximum_active_grants=2,
        )

        for content in (b"one", b"two"):
            digest = hashlib.sha256(content).hexdigest()
            grant = broker.issue_write_grant(
                worker_authorization,
                tenant_id=self.harness.identity.tenant_id,
                run_id=claim.run_id,
                node_id=claim.node_id,
                attempt_id=claim.attempt_id,
                kind=ArtifactKind.TOOL_RESULT,
                sensitivity="internal",
                media_type="application/octet-stream",
                maximum_bytes=len(content),
                declared_sha256=digest,
                ttl_seconds=1,
            )
            staging = broker.stage_write(
                grant,
                worker_authorization,
                content=content,
                declared_sha256=digest,
            )
            broker.finalize_write(grant, worker_authorization, staging)

        self.harness.clock.value += 2
        content = b"new"
        digest = hashlib.sha256(content).hexdigest()
        try:
            broker.issue_write_grant(
                worker_authorization,
                tenant_id=self.harness.identity.tenant_id,
                run_id=claim.run_id,
                node_id=claim.node_id,
                attempt_id=claim.attempt_id,
                kind=ArtifactKind.TOOL_RESULT,
                sensitivity="internal",
                media_type="application/octet-stream",
                maximum_bytes=len(content),
                declared_sha256=digest,
                ttl_seconds=1,
            )
        except ArtifactGrantDenied as exc:
            self.fail(
                "expired finalized records permanently consumed the active "
                f"grant registry: {exc.reason_code}"
            )

    def test_p1_expired_write_grant_cannot_revive_after_clock_rollback(
        self,
    ) -> None:
        """Once observed expired, a bearer grant must remain expired forever."""

        claim = self.harness._claim("round3-clock-rollback")
        admitter = self.harness._admitter()
        admitter.admit(
            self.harness.identity,
            self.harness.registration,
            self.harness.scheduler,
            claim,
        )
        worker_authorization = admitter._prepared[
            claim.attempt_id
        ].worker_authorization
        content = b"clock"
        grant = self.harness.broker.issue_write_grant(
            worker_authorization,
            tenant_id=self.harness.identity.tenant_id,
            run_id=claim.run_id,
            node_id=claim.node_id,
            attempt_id=claim.attempt_id,
            kind=ArtifactKind.TOOL_RESULT,
            sensitivity="internal",
            media_type="application/octet-stream",
            maximum_bytes=len(content),
            declared_sha256=hashlib.sha256(content).hexdigest(),
            ttl_seconds=5,
        )
        self.harness.clock.value = grant.expires_at
        with self.assertRaises(ArtifactGrantConsumed):
            self.harness.broker.resolve_output_handle(grant.output_handle)

        self.harness.clock.value = grant.expires_at - 1
        with self.assertRaises(
            ArtifactGrantConsumed,
            msg="an already-observed expired bearer credential revived",
        ):
            self.harness.broker.resolve_output_handle(grant.output_handle)

    def test_p2_partial_v1_journal_schema_must_fail_at_startup(self) -> None:
        """A process must not advertise readiness with a malformed v1 journal."""

        path = self.harness.control_root / "round3-partial-journal.sqlite3"
        connection = sqlite3.connect(path)
        try:
            connection.executescript(
                f"""
                CREATE TABLE remote_journal_metadata(
                    singleton INTEGER PRIMARY KEY,
                    schema_version INTEGER NOT NULL
                );
                INSERT INTO remote_journal_metadata VALUES
                    (1, {REMOTE_JOURNAL_SCHEMA_VERSION});
                CREATE TABLE remote_session_heads(
                    worker_id TEXT PRIMARY KEY
                );
                """
            )
        finally:
            connection.close()

        with self.assertRaises(
            RemoteJournalError,
            msg="malformed journal schema was accepted until first live request",
        ):
            RemoteControlJournal(path)


if __name__ == "__main__":
    unittest.main()
