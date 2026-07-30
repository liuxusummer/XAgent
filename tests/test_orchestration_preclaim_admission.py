from __future__ import annotations

import hashlib
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

from src.orchestration.executor import (
    DurableApprovalRegistry,
    TrustedActivityExecutor,
)
from src.orchestration.models import AttemptStatus, NodeStatus, RunStatus
from src.orchestration.policy import (
    ApprovalGrant,
    PolicyEngine,
    PolicyOutcome,
    PolicyRule,
)
from src.orchestration.remote_control import RemoteControlPlane
from src.orchestration.remote_journal import RemoteControlJournal
from src.orchestration.remote_worker import RemoteWorkerClient
from src.orchestration.sandbox import SandboxDispatcher

from tests import test_orchestration_remote_execution as secure_tests


class PreclaimAdmissionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = secure_tests.SecureRemoteExecutionTests(
            methodName="runTest"
        )
        self.harness.setUp()
        self.addCleanup(self.harness.doCleanups)

    def _ready(self, run_id: str) -> None:
        self.harness.scheduler.create_run(run_id)
        self.harness.scheduler.reconcile(run_id)

    def test_secure_poll_uses_atomic_candidate_ticket_claim_and_policy(self):
        run_id = "two-phase-run"
        self._ready(run_id)
        admitter = self.harness._admitter()
        control = RemoteControlPlane(
            lambda _run_id: self.harness.scheduler,
            authorize_run=lambda identity, requested: (
                identity == self.harness.identity and requested == run_id
            ),
            assignment_admitter=admitter,
            journal=RemoteControlJournal(
                self.harness.control_root / "two-phase.sqlite3"
            ),
        )
        client = RemoteWorkerClient(
            lambda request: control.handle(self.harness.identity, request),
            worker_id=self.harness.identity.worker_id,
            instance_id="two-phase-instance",
        )
        client.register(
            runtime_version="runsc-1.2",
            capabilities=("activity.tool",),
            resource_keys=("workspace:/project",),
            activity_kinds=("tool",),
            max_concurrency=1,
        )

        assignment = client.poll(run_id)

        self.assertIsNotNone(assignment)
        events = self.harness.store.list_events(run_id)
        self.assertEqual(
            [
                "attempt.scheduled",
                "attempt.claimed",
                "policy.decided",
            ],
            [
                event.event_type
                for event in events
                if event.event_type
                in {
                    "attempt.scheduled",
                    "attempt.claimed",
                    "policy.decided",
                }
            ],
        )

    def test_remote_approval_suspends_without_claim_then_resumes_same_attempt(
        self,
    ):
        run_id = "two-phase-approval-run"
        approval_policy = PolicyEngine(
            (self.harness.tool_policy,),
            (
                PolicyRule(
                    "require-exec-approval",
                    PolicyOutcome.REQUIRE_APPROVAL,
                    tool_name="exec",
                    reason_code="approval_required_by_test",
                ),
            ),
            approval_actors=("trusted-operator",),
        )
        approval_executor = TrustedActivityExecutor(
            self.harness.scheduler,
            approval_policy,
            SandboxDispatcher(
                (secure_tests._UnusedBackend(),),
                policy_version=approval_policy.policy_version,
            ),
            artifact_verifier=self.harness.artifacts.verify,
            artifact_reader=self.harness.artifacts.read,
            clock=self.harness.clock,
        )
        resolver = secure_tests._PlanResolver(self.harness.preparation)
        admitter = self.harness._admitter(
            executor=approval_executor,
            plan_resolver=resolver,
        )
        self._ready(run_id)
        control = RemoteControlPlane(
            lambda _run_id: self.harness.scheduler,
            authorize_run=lambda identity, requested: (
                identity == self.harness.identity and requested == run_id
            ),
            assignment_admitter=admitter,
            journal=RemoteControlJournal(
                self.harness.control_root / "approval.sqlite3"
            ),
        )
        client = RemoteWorkerClient(
            lambda request: control.handle(self.harness.identity, request),
            worker_id=self.harness.identity.worker_id,
            instance_id="approval-instance",
        )
        client.register(
            runtime_version="runsc-1.2",
            capabilities=("activity.tool",),
            resource_keys=("workspace:/project",),
            activity_kinds=("tool",),
            max_concurrency=1,
        )

        self.assertIsNone(client.poll(run_id))
        attempt = self.harness.store.list_attempts(run_id)[0]
        self.assertEqual(AttemptStatus.WAITING_APPROVAL, attempt.status)
        self.assertEqual(
            RunStatus.WAITING_APPROVAL,
            self.harness.store.get_run(run_id).status,
        )
        self.assertEqual(
            NodeStatus.WAITING_APPROVAL,
            self.harness.store.get_node(run_id, "tool").status,
        )
        self.assertIsNone(attempt.worker_id)
        self.assertIsNone(attempt.lease_id)
        self.assertIsNone(
            self.harness.store.get_idempotency(
                run_id,
                attempt.idempotency_key,
            )
        )
        event_types = [
            event.event_type
            for event in self.harness.store.list_events(run_id)
        ]
        self.assertNotIn("attempt.claimed", event_types)
        self.assertEqual(0, len(self.harness.worker_gate._issued))
        self.assertEqual(0, len(self.harness.broker._grants))
        request = next(
            event
            for event in self.harness.store.list_events(run_id)
            if event.event_type == "approval.requested"
        )
        self.assertNotIn("claim_token_digest", request.payload)

        grant = ApprovalGrant(
            approval_id="remote-approval-1",
            action_digest=request.payload["action_digest"],
            run_id=run_id,
            node_id="tool",
            policy_version=approval_policy.policy_version,
            actor="trusted-operator",
            expires_at=200.0,
        )
        DurableApprovalRegistry(
            self.harness.store,
            trusted_actors=("trusted-operator",),
            clock=self.harness.clock,
        ).register_issued(grant)
        resumed = self.harness.store.get_attempt(attempt.attempt_id)
        assert resumed is not None
        self.assertEqual(AttemptStatus.SCHEDULED, resumed.status)
        resolver.preparation = replace(
            self.harness.preparation,
            approval_grant=grant,
        )

        assignment = client.poll(run_id)

        self.assertIsNotNone(assignment)
        assert assignment is not None
        self.assertEqual(attempt.attempt_id, assignment.claim.attempt_id)
        claimed = self.harness.store.get_attempt(attempt.attempt_id)
        assert claimed is not None
        self.assertEqual(AttemptStatus.CLAIMED, claimed.status)
        self.assertEqual(
            1,
            sum(
                event.event_type == "attempt.claimed"
                for event in self.harness.store.list_events(run_id)
            ),
        )
        self.assertEqual(
            1,
            sum(
                event.event_type == "policy.decided"
                for event in self.harness.store.list_events(run_id)
            ),
        )

    def test_preclaim_approval_rejection_needs_no_idempotency_record(self):
        run_id = "preclaim-approval-rejected"
        self._ready(run_id)
        candidate = self.harness.scheduler.prepare_next_admission(
            run_id,
            self.harness.registration.session_owner_id,
            resource_keys=self.harness.registration.resource_keys,
        )
        assert candidate is not None
        digest = hashlib.sha256(b"approval-binding").hexdigest()
        claim, event = self.harness.scheduler.claim_admitted(
            candidate,
            lease_seconds=30.0,
            capacity=1,
            admission_expires_at=200.0,
            policy_binding={
                "outcome": "require_approval",
                "reason_code": "approval_required_by_test",
                "action_digest": digest,
                "policy_digest": hashlib.sha256(b"policy").hexdigest(),
                "profile_digest": hashlib.sha256(b"profile").hexdigest(),
                "decision_digest": hashlib.sha256(b"decision").hexdigest(),
                "approval_grant_digest": None,
            },
        )
        self.assertIsNone(claim)
        attempt = self.harness.store.get_attempt(candidate.claim.attempt_id)
        assert attempt is not None
        self.assertEqual("approval.requested", event.event_type)
        self.assertIsNone(
            self.harness.store.get_idempotency(
                run_id,
                attempt.idempotency_key,
            )
        )

        DurableApprovalRegistry(
            self.harness.store,
            trusted_actors=("trusted-operator",),
            clock=self.harness.clock,
        ).reject_requested(
            run_id,
            attempt.attempt_id,
            actor="trusted-operator",
        )

        rejected = self.harness.store.get_attempt(attempt.attempt_id)
        assert rejected is not None
        self.assertEqual(AttemptStatus.FAILED, rejected.status)
        self.assertEqual(RunStatus.FAILED, self.harness.store.get_run(run_id).status)
        self.assertTrue(self.harness.store.verify_projections(run_id))

    def test_admission_ticket_is_single_use_before_store_claim(self):
        run_id = "single-use-ticket"
        self._ready(run_id)
        admitter = self.harness._admitter()
        candidate = self.harness.scheduler.prepare_next_admission(
            run_id,
            self.harness.registration.session_owner_id,
            resource_keys=self.harness.registration.resource_keys,
        )
        assert candidate is not None
        admission = admitter.prepare_admission(
            self.harness.identity,
            self.harness.registration,
            self.harness.scheduler,
            candidate,
        )

        admitter.begin_admission(
            self.harness.identity,
            self.harness.registration,
            self.harness.scheduler,
            candidate,
            admission,
        )
        with self.assertRaisesRegex(Exception, "invalid_admission_ticket"):
            admitter.begin_admission(
                self.harness.identity,
                self.harness.registration,
                self.harness.scheduler,
                candidate,
                admission,
            )
        admitter.cancel_admission(admission)
        self.assertEqual([], self.harness.store.list_attempts(run_id))
        self.assertEqual(0, len(self.harness.worker_gate._issued))

    def test_expired_committing_ticket_does_not_leak_capacity(self):
        run_id = "expired-committing-ticket"
        self._ready(run_id)
        admitter = self.harness._admitter(
            prepared_ttl_seconds=1.0,
            maximum_prepared=1,
        )
        candidate = self.harness.scheduler.prepare_next_admission(
            run_id,
            self.harness.registration.session_owner_id,
            resource_keys=self.harness.registration.resource_keys,
        )
        assert candidate is not None
        first = admitter.prepare_admission(
            self.harness.identity,
            self.harness.registration,
            self.harness.scheduler,
            candidate,
        )
        admitter.begin_admission(
            self.harness.identity,
            self.harness.registration,
            self.harness.scheduler,
            candidate,
            first,
        )
        self.harness.clock.value = first.expires_at

        second = admitter.prepare_admission(
            self.harness.identity,
            self.harness.registration,
            self.harness.scheduler,
            candidate,
        )

        self.assertNotEqual(first.ticket_id, second.ticket_id)
        self.assertEqual(1, len(admitter._admission_tickets))
        self.assertEqual(1, len(self.harness.worker_gate._issued))
        admitter.cancel_admission(second)
        self.assertEqual(0, len(self.harness.worker_gate._issued))

    def test_sqlite_lock_wait_past_ticket_expiry_has_zero_mutation(self):
        run_id = "expired-linearization-run"
        self._ready(run_id)
        admitter = self.harness._admitter()
        candidate = self.harness.scheduler.prepare_next_admission(
            run_id,
            self.harness.registration.session_owner_id,
            resource_keys=self.harness.registration.resource_keys,
        )
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
        writer_locked = threading.Event()
        release_writer = threading.Event()

        def hold_writer() -> None:
            with self.harness.store._write_transaction():
                writer_locked.set()
                if not release_writer.wait(5.0):
                    raise RuntimeError("writer barrier timed out")

        def claim() -> None:
            self.harness.scheduler.claim_admitted(
                candidate,
                lease_seconds=30.0,
                capacity=1,
                admission_expires_at=expires_at,
                policy_binding=admission.policy_binding,
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            held = pool.submit(hold_writer)
            self.assertTrue(writer_locked.wait(2.0))
            pending = pool.submit(claim)
            time.sleep(0.05)
            self.harness.clock.value = expires_at
            release_writer.set()
            held.result(timeout=3.0)
            with self.assertRaises(Exception):
                pending.result(timeout=3.0)
        admitter.cancel_admission(admission)
        self.assertEqual(before_attempts, self.harness.store.list_attempts(run_id))
        self.assertEqual(before_events, self.harness.store.list_events(run_id))

    def test_concurrent_candidate_cas_has_one_winner(self):
        run_id = "candidate-cas-run"
        self._ready(run_id)
        admitter = self.harness._admitter()
        candidates = tuple(
            self.harness.scheduler.prepare_next_admission(
                run_id,
                self.harness.registration.session_owner_id,
                resource_keys=self.harness.registration.resource_keys,
            )
            for _ in range(2)
        )
        assert all(candidate is not None for candidate in candidates)
        admissions = tuple(
            admitter.prepare_admission(
                self.harness.identity,
                self.harness.registration,
                self.harness.scheduler,
                candidate,
            )
            for candidate in candidates
        )
        expirations = tuple(
            admitter.begin_admission(
                self.harness.identity,
                self.harness.registration,
                self.harness.scheduler,
                candidate,
                admission,
            )
            for candidate, admission in zip(candidates, admissions)
        )
        barrier = threading.Barrier(2)

        def claim(index: int):
            barrier.wait(timeout=2.0)
            candidate = candidates[index]
            assert candidate is not None
            return self.harness.scheduler.claim_admitted(
                candidate,
                lease_seconds=30.0,
                capacity=1,
                admission_expires_at=expirations[index],
                policy_binding=admissions[index].policy_binding,
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(claim, index) for index in range(2)]
            outcomes = []
            for future in futures:
                try:
                    outcomes.append(future.result(timeout=3.0))
                except Exception:
                    outcomes.append(None)
        for admission in admissions:
            admitter.cancel_admission(admission)
        self.assertEqual(1, sum(outcome is not None for outcome in outcomes))
        self.assertEqual(1, len(self.harness.store.list_attempts(run_id)))


if __name__ == "__main__":
    unittest.main()
