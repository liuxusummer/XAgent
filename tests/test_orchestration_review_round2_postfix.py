from __future__ import annotations

import hashlib
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

from src.orchestration.executor import TrustedActivityExecutor
from src.orchestration.models import AttemptStatus, NodeStatus, RunStatus
from src.orchestration.remote_control import RemoteControlPlane
from src.orchestration.remote_execution import (
    MAX_PREPARED_MATERIALIZED_BYTES,
    SecureRemoteExecutionError,
)
from src.orchestration.remote_journal import RemoteControlJournal
from src.orchestration.remote_worker import RemoteWorkerClient, RemoteWorkerError
from src.orchestration.scheduler import DurableScheduler
from src.orchestration.workflow import compile_workflow

from tests import test_orchestration_remote_execution as secure_tests


class Round2PostfixAdversarialReviewTests(unittest.TestCase):
    """Independent attacks against the repaired two-stage admission path."""

    def setUp(self) -> None:
        self.harness = secure_tests.SecureRemoteExecutionTests(
            methodName="runTest"
        )
        self.harness.setUp()
        self.addCleanup(self.harness.doCleanups)

    @staticmethod
    def _register(client: RemoteWorkerClient) -> None:
        client.register(
            runtime_version="runsc-1.2",
            capabilities=("activity.tool",),
            resource_keys=("workspace:/project",),
            activity_kinds=("tool",),
            max_concurrency=32,
        )

    def _client(
        self,
        control: RemoteControlPlane,
        *,
        instance_id: str,
    ) -> RemoteWorkerClient:
        return RemoteWorkerClient(
            lambda request: control.handle(self.harness.identity, request),
            worker_id=self.harness.identity.worker_id,
            instance_id=instance_id,
        )

    def test_superseded_session_cannot_linearize_an_inflight_poll(self) -> None:
        """P1: session replacement must fence a poll before Store mutation."""

        run_id = "postfix-session-supersession"
        self.harness.scheduler.create_run(run_id)
        self.harness.scheduler.reconcile(run_id)
        admitter = self.harness._admitter()
        control = RemoteControlPlane(
            lambda _run_id: self.harness.scheduler,
            authorize_run=lambda identity, requested: (
                identity == self.harness.identity and requested == run_id
            ),
            assignment_admitter=admitter,
            journal=RemoteControlJournal(
                self.harness.control_root / "postfix-session.sqlite3"
            ),
        )
        old_client = self._client(control, instance_id="old-instance")
        new_client = self._client(control, instance_id="new-instance")
        self._register(old_client)

        before_attempts = self.harness.store.list_attempts(run_id)
        before_events = self.harness.store.list_events(run_id)
        entered_store_boundary = threading.Event()
        release_store_boundary = threading.Event()
        original_claim_admitted = self.harness.scheduler.claim_admitted

        def blocked_claim_admitted(*args, **kwargs):
            entered_store_boundary.set()
            if not release_store_boundary.wait(5.0):
                raise RuntimeError("session supersession barrier timed out")
            return original_claim_admitted(*args, **kwargs)

        self.harness.scheduler.claim_admitted = blocked_claim_admitted
        self.addCleanup(
            setattr,
            self.harness.scheduler,
            "claim_admitted",
            original_claim_admitted,
        )

        assignment = None
        poll_error: RemoteWorkerError | None = None
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(old_client.poll, run_id)
            self.assertTrue(entered_store_boundary.wait(2.0))
            try:
                # This commits a higher session epoch before the old poll is
                # allowed to enter the durable claim transaction.
                self._register(new_client)
            finally:
                release_store_boundary.set()
            try:
                assignment = pending.result(timeout=5.0)
            except RemoteWorkerError as exc:
                poll_error = exc

        current = control.get_registration(self.harness.identity.worker_id)
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual("new-instance", current.instance_id)
        self.assertTrue(
            assignment is None or poll_error is not None,
            "a durably superseded Worker session received a new assignment",
        )
        self.assertEqual(
            before_attempts,
            self.harness.store.list_attempts(run_id),
            "a superseded session created a durable Attempt",
        )
        self.assertEqual(
            before_events,
            self.harness.store.list_events(run_id),
            "a superseded session changed the Domain event stream",
        )
        self.assertEqual(
            0,
            len(admitter._prepared),
            "a superseded session retained executable or broker authority",
        )

    def test_committed_preparations_remain_inside_materialized_byte_bound(
        self,
    ) -> None:
        """P1: ticket commit must not erase bytes that remain in memory."""

        script = b"x" * (4 * 1024 * 1024)
        script_ref = self.harness.artifacts.put_bytes(script)
        preparation = replace(
            self.harness.preparation,
            script_artifact_ref=script_ref,
            resource_locks=(),
        )
        no_resource_workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": "postfix-materialized-memory",
                "version": 1,
                "nodes": [
                    {
                        "id": "tool",
                        "kind": "tool",
                        "config": {
                            "tool": "exec",
                            "arguments": {"command": "fixed"},
                        },
                        "effect_class": "read_only",
                    }
                ],
            }
        )
        scheduler = DurableScheduler(
            self.harness.store,
            no_resource_workflow,
            clock=self.harness.clock,
            artifact_verifier=self.harness.artifacts.verify,
            max_active_attempts=32,
        )
        executor = TrustedActivityExecutor(
            scheduler,
            self.harness.executor.policy,
            self.harness.executor.sandbox,
            artifact_verifier=self.harness.artifacts.verify,
            artifact_reader=self.harness.artifacts.read,
            clock=self.harness.clock,
        )
        admitter = self.harness._admitter(
            executor=executor,
            plan_resolver=secure_tests._PlanResolver(preparation),
            maximum_prepared=32,
        )
        denied_before_mutation = False

        for index in range(
            MAX_PREPARED_MATERIALIZED_BYTES // len(script) + 1
        ):
            run_id = f"postfix-materialized-{index}"
            scheduler.create_run(run_id)
            scheduler.reconcile(run_id)
            candidate = scheduler.prepare_next_admission(
                run_id,
                self.harness.registration.session_owner_id,
                resource_keys=self.harness.registration.resource_keys,
            )
            self.assertIsNotNone(candidate)
            assert candidate is not None
            before_attempts = self.harness.store.list_attempts(run_id)
            before_events = self.harness.store.list_events(run_id)
            try:
                admission = admitter.prepare_admission(
                    self.harness.identity,
                    self.harness.registration,
                    scheduler,
                    candidate,
                )
            except SecureRemoteExecutionError:
                denied_before_mutation = True
                self.assertEqual(
                    before_attempts,
                    self.harness.store.list_attempts(run_id),
                )
                self.assertEqual(
                    before_events,
                    self.harness.store.list_events(run_id),
                )
                break
            expires_at = admitter.begin_admission(
                self.harness.identity,
                self.harness.registration,
                scheduler,
                candidate,
                admission,
            )
            claim, policy_event = scheduler.claim_admitted(
                candidate,
                lease_seconds=30.0,
                capacity=32,
                admission_expires_at=expires_at,
                policy_binding=admission.policy_binding,
            )
            self.assertIsNotNone(claim)
            assert claim is not None
            admitter.commit_admission(
                self.harness.identity,
                self.harness.registration,
                scheduler,
                candidate,
                claim,
                admission,
                policy_event,
            )

        retained_bytes = sum(
            len(record.prepared.request.materialized_script or b"")
            for record in admitter._prepared.values()
        )
        self.assertTrue(
            denied_before_mutation,
            "committing tickets bypassed the materialized-script hard bound",
        )
        self.assertLessEqual(
            retained_bytes,
            MAX_PREPARED_MATERIALIZED_BYTES,
            "prepared execution registry retained bytes beyond its hard bound",
        )

    def test_schedule_claim_policy_faults_rollback_one_atomic_unit(self) -> None:
        """All three durable writes roll back at every Event fault seam."""

        admitter = self.harness._admitter()
        original_fault = self.harness.store._fault
        self.addCleanup(setattr, self.harness.store, "_fault", original_fault)

        for failure_index in (1, 2, 3):
            with self.subTest(failure_index=failure_index):
                run_id = f"postfix-store-fault-{failure_index}"
                self.harness.scheduler.create_run(run_id)
                self.harness.scheduler.reconcile(run_id)
                candidate = self.harness.scheduler.prepare_next_admission(
                    run_id,
                    self.harness.registration.session_owner_id,
                    resource_keys=self.harness.registration.resource_keys,
                )
                self.assertIsNotNone(candidate)
                assert candidate is not None
                admission = admitter.prepare_admission(
                    self.harness.identity,
                    self.harness.registration,
                    self.harness.scheduler,
                    candidate,
                )
                expires_at = admitter.begin_admission(
                    self.harness.identity,
                    self.harness.registration,
                    self.harness.scheduler,
                    candidate,
                    admission,
                )
                before_attempts = self.harness.store.list_attempts(run_id)
                before_events = self.harness.store.list_events(run_id)
                observed = 0

                def fail_at_event_insert(stage: str) -> None:
                    nonlocal observed
                    if stage != "event.after_insert":
                        return
                    observed += 1
                    if observed == failure_index:
                        raise RuntimeError("postfix injected Store fault")

                self.harness.store._fault = fail_at_event_insert
                try:
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "postfix injected Store fault",
                    ):
                        self.harness.scheduler.claim_admitted(
                            candidate,
                            lease_seconds=30.0,
                            capacity=1,
                            admission_expires_at=expires_at,
                            policy_binding=admission.policy_binding,
                        )
                finally:
                    self.harness.store._fault = original_fault
                    admitter.cancel_admission(admission)

                self.assertEqual(failure_index, observed)
                self.assertEqual(
                    before_attempts,
                    self.harness.store.list_attempts(run_id),
                )
                self.assertEqual(
                    before_events,
                    self.harness.store.list_events(run_id),
                )
                self.assertIsNone(
                    self.harness.store.get_idempotency(
                        run_id,
                        candidate.attempt.idempotency_key,
                    )
                )

    def test_postcommit_publish_failure_keeps_authorized_claim(self) -> None:
        """Grant publication failure is lease-recoverable, not zero mutation."""

        run_id = "postfix-postcommit-failure"
        self.harness.scheduler.create_run(run_id)
        self.harness.scheduler.reconcile(run_id)
        admitter = self.harness._admitter()

        def fail_grant_publication(*_args, **_kwargs):
            raise RuntimeError("postfix grant publication failed")

        admitter._issue_input_grants = fail_grant_publication
        control = RemoteControlPlane(
            lambda _run_id: self.harness.scheduler,
            authorize_run=lambda identity, requested: (
                identity == self.harness.identity and requested == run_id
            ),
            assignment_admitter=admitter,
            journal=RemoteControlJournal(
                self.harness.control_root / "postfix-postcommit.sqlite3"
            ),
        )
        client = self._client(control, instance_id="postcommit-instance")
        self._register(client)

        with self.assertRaisesRegex(
            RemoteWorkerError,
            "control_unavailable",
        ):
            client.poll(run_id)

        attempts = self.harness.store.list_attempts(run_id)
        self.assertEqual(1, len(attempts))
        self.assertEqual(AttemptStatus.CLAIMED, attempts[0].status)
        event_types = [
            event.event_type for event in self.harness.store.list_events(run_id)
        ]
        self.assertEqual(
            ["attempt.scheduled", "attempt.claimed", "policy.decided"],
            [
                event_type
                for event_type in event_types
                if event_type
                in {
                    "attempt.scheduled",
                    "attempt.claimed",
                    "policy.decided",
                }
            ],
        )
        self.assertEqual({}, admitter._prepared)
        self.assertEqual({}, admitter._admission_tickets)
        self.assertEqual(0, len(self.harness.worker_gate._issued))
        self.assertTrue(self.harness.store.verify_projections(run_id))

    def test_concurrent_approval_and_rejection_have_one_decision(self) -> None:
        """Competing trusted decisions cannot both resume and terminalize."""

        run_id = "postfix-approval-race"
        self.harness.scheduler.create_run(run_id)
        self.harness.scheduler.reconcile(run_id)
        candidate = self.harness.scheduler.prepare_next_admission(
            run_id,
            self.harness.registration.session_owner_id,
            resource_keys=self.harness.registration.resource_keys,
        )
        self.assertIsNotNone(candidate)
        assert candidate is not None
        action_digest = hashlib.sha256(b"postfix-action").hexdigest()
        policy_digest = hashlib.sha256(b"postfix-policy").hexdigest()
        claim, request_event = self.harness.scheduler.claim_admitted(
            candidate,
            lease_seconds=30.0,
            capacity=1,
            admission_expires_at=200.0,
            policy_binding={
                "outcome": "require_approval",
                "reason_code": "postfix_approval_required",
                "action_digest": action_digest,
                "policy_digest": policy_digest,
                "profile_digest": hashlib.sha256(
                    b"postfix-profile"
                ).hexdigest(),
                "decision_digest": hashlib.sha256(
                    b"postfix-decision"
                ).hexdigest(),
                "approval_grant_digest": None,
            },
        )
        self.assertIsNone(claim)
        self.assertEqual("approval.requested", request_event.event_type)
        barrier = threading.Barrier(2)

        def approve() -> str:
            barrier.wait(timeout=2.0)
            self.harness.store.register_approval_grant(
                run_id,
                grant_binding_digest=hashlib.sha256(
                    b"postfix-grant"
                ).hexdigest(),
                action_digest=action_digest,
                policy_digest=policy_digest,
                expires_at=200.0,
                now=100.0,
            )
            return "approved"

        def reject() -> str:
            barrier.wait(timeout=2.0)
            self.harness.store.reject_activity_approval(
                run_id,
                candidate.attempt.attempt_id,
                now=100.0,
            )
            return "rejected"

        outcomes: list[str] = []
        failures: list[BaseException] = []
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = (pool.submit(approve), pool.submit(reject))
            for future in futures:
                try:
                    outcomes.append(future.result(timeout=5.0))
                except BaseException as exc:
                    failures.append(exc)

        self.assertEqual(1, len(outcomes))
        self.assertEqual(1, len(failures))
        attempt = self.harness.store.get_attempt(
            candidate.attempt.attempt_id
        )
        self.assertIsNotNone(attempt)
        assert attempt is not None
        run = self.harness.store.get_run(run_id)
        node = self.harness.store.get_node(run_id, candidate.claim.node_id)
        if outcomes == ["approved"]:
            self.assertEqual(AttemptStatus.SCHEDULED, attempt.status)
            self.assertEqual(NodeStatus.READY, node.status)
            self.assertEqual(RunStatus.RUNNING, run.status)
        else:
            self.assertEqual(["rejected"], outcomes)
            self.assertEqual(AttemptStatus.FAILED, attempt.status)
            self.assertEqual(NodeStatus.FAILED, node.status)
            self.assertEqual(RunStatus.FAILED, run.status)
        self.assertIsNone(
            self.harness.store.get_idempotency(
                run_id,
                candidate.attempt.idempotency_key,
            )
        )
        self.assertEqual(
            1,
            sum(
                event.event_type == "approval.resolved"
                for event in self.harness.store.list_events(run_id)
            ),
        )
        self.assertTrue(self.harness.store.verify_projections(run_id))


if __name__ == "__main__":
    unittest.main()
