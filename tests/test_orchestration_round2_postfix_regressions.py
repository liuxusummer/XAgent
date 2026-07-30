from __future__ import annotations

import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

from src.orchestration.executor import TrustedActivityExecutor
from src.orchestration.models import AttemptStatus
from src.orchestration.remote_control import RemoteControlPlane
from src.orchestration.remote_execution import SecureRemoteExecutionError
from src.orchestration.remote_journal import RemoteControlJournal
from src.orchestration.remote_worker import RemoteWorkerClient, RemoteWorkerError

from tests import test_orchestration_remote_execution as secure_tests
from tests import test_orchestration_remote_protocol as protocol_tests


class Round2PostfixRegressionTests(unittest.TestCase):
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

    def test_worker_session_guard_only_serializes_the_same_worker(self) -> None:
        control = RemoteControlPlane(
            lambda _run_id: self.harness.scheduler,
            authorize_run=lambda _identity, _run_id: True,
        )
        first_entered = threading.Event()
        release_first = threading.Event()
        same_worker_entered = threading.Event()
        other_worker_entered = threading.Event()

        def hold_first() -> None:
            with control._worker_session_guard("worker-a"):
                first_entered.set()
                self.assertTrue(release_first.wait(3.0))

        def enter(worker_id: str, entered: threading.Event) -> None:
            with control._worker_session_guard(worker_id):
                entered.set()

        with ThreadPoolExecutor(max_workers=3) as pool:
            first = pool.submit(hold_first)
            self.assertTrue(first_entered.wait(1.0))
            same = pool.submit(enter, "worker-a", same_worker_entered)
            other = pool.submit(enter, "worker-b", other_worker_entered)
            self.assertTrue(other_worker_entered.wait(1.0))
            self.assertFalse(same_worker_entered.wait(0.05))
            release_first.set()
            first.result(timeout=2.0)
            same.result(timeout=2.0)
            other.result(timeout=2.0)
        self.assertEqual({}, control._worker_session_guards)

    def test_replacement_after_store_claim_cannot_publish_old_grants(
        self,
    ) -> None:
        run_id = "postfix-session-postclaim"
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
                self.harness.control_root / "postfix-postclaim.sqlite3"
            ),
        )
        old_client = self._client(control, instance_id="old-postclaim")
        new_client = self._client(control, instance_id="new-postclaim")
        self._register(old_client)
        store_committed = threading.Event()
        release_store_result = threading.Event()
        original_claim_admitted = self.harness.scheduler.claim_admitted

        def blocked_after_claim(*args, **kwargs):
            result = original_claim_admitted(*args, **kwargs)
            store_committed.set()
            if not release_store_result.wait(3.0):
                raise RuntimeError("postclaim barrier timed out")
            return result

        self.harness.scheduler.claim_admitted = blocked_after_claim
        self.addCleanup(
            setattr,
            self.harness.scheduler,
            "claim_admitted",
            original_claim_admitted,
        )

        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                pending = pool.submit(old_client.poll, run_id)
                self.assertTrue(store_committed.wait(2.0))
                replacement = pool.submit(self._register, new_client)
                replacement.result(timeout=1.0)
                release_store_result.set()
                with self.assertRaisesRegex(
                    RemoteWorkerError,
                    "worker_identity_mismatch",
                ):
                    pending.result(timeout=2.0)
        finally:
            release_store_result.set()

        attempts = self.harness.store.list_attempts(run_id)
        self.assertEqual(1, len(attempts))
        self.assertEqual(AttemptStatus.CLAIMED, attempts[0].status)
        self.assertEqual({}, admitter._prepared)
        self.assertEqual({}, admitter._admission_tickets)
        self.assertEqual({}, admitter._registry_reservations)
        self.assertEqual(0, len(self.harness.worker_gate._issued))

    def test_materialized_bytes_are_reserved_before_concurrent_read(
        self,
    ) -> None:
        script = b"x" * (33 * 1024 * 1024)
        script_ref = self.harness.artifacts.put_bytes(script)
        preparation = replace(
            self.harness.preparation,
            script_artifact_ref=script_ref,
        )
        reader_entered = threading.Event()
        release_reader = threading.Event()
        reader_calls = 0
        reader_lock = threading.Lock()

        def blocking_reader(ref):
            nonlocal reader_calls
            with reader_lock:
                reader_calls += 1
            reader_entered.set()
            if not release_reader.wait(3.0):
                raise RuntimeError("artifact reader barrier timed out")
            return self.harness.artifacts.read(ref)

        executor = TrustedActivityExecutor(
            self.harness.scheduler,
            self.harness.executor.policy,
            self.harness.executor.sandbox,
            artifact_verifier=self.harness.artifacts.verify,
            artifact_reader=blocking_reader,
            clock=self.harness.clock,
        )
        admitter = self.harness._admitter(
            executor=executor,
            plan_resolver=secure_tests._PlanResolver(preparation),
            maximum_prepared=2,
        )
        candidates = []
        for index in range(2):
            run_id = f"postfix-reservation-{index}"
            self.harness.scheduler.create_run(run_id)
            self.harness.scheduler.reconcile(run_id)
            candidate = self.harness.scheduler.prepare_next_admission(
                run_id,
                self.harness.registration.session_owner_id,
                resource_keys=self.harness.registration.resource_keys,
            )
            self.assertIsNotNone(candidate)
            candidates.append(candidate)
        first_candidate, second_candidate = candidates
        assert first_candidate is not None
        assert second_candidate is not None

        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                first = pool.submit(
                    admitter.prepare_admission,
                    self.harness.identity,
                    self.harness.registration,
                    self.harness.scheduler,
                    first_candidate,
                )
                self.assertTrue(reader_entered.wait(2.0))
                with self.assertRaisesRegex(
                    SecureRemoteExecutionError,
                    "prepared_registry_capacity",
                ):
                    admitter.prepare_admission(
                        self.harness.identity,
                        self.harness.registration,
                        self.harness.scheduler,
                        second_candidate,
                    )
                with reader_lock:
                    self.assertEqual(1, reader_calls)
                release_reader.set()
                admission = first.result(timeout=3.0)
        finally:
            release_reader.set()
        admitter.cancel_admission(admission)
        self.assertEqual({}, admitter._registry_reservations)
        self.assertEqual(0, admitter._registry_materialized_bytes)

    def test_direct_admit_uses_the_same_reservation_ledger(self) -> None:
        script = b"direct-reservation"
        script_ref = self.harness.artifacts.put_bytes(script)
        preparation = replace(
            self.harness.preparation,
            script_artifact_ref=script_ref,
        )
        admitter = self.harness._admitter(
            plan_resolver=secure_tests._PlanResolver(preparation),
            maximum_prepared=1,
        )
        first_claim = self.harness._claim("direct-reservation-first")
        admitter.admit(
            self.harness.identity,
            self.harness.registration,
            self.harness.scheduler,
            first_claim,
        )
        self.assertEqual(1, len(admitter._registry_reservations))
        self.assertEqual(
            len(script),
            admitter._registry_materialized_bytes,
        )
        record = admitter._prepared[first_claim.attempt_id]
        self.harness.worker_gate.discard(record.worker_authorization)
        admitter._drop_prepared(first_claim.attempt_id)
        self.assertEqual({}, admitter._registry_reservations)
        self.assertEqual(0, admitter._registry_materialized_bytes)

    def test_empty_poll_fails_closed_for_non_two_phase_admitter(self) -> None:
        run_id = "postfix-empty-misconfiguration"
        self.harness.scheduler.create_run(run_id)
        before_attempts = self.harness.store.list_attempts(run_id)
        before_events = self.harness.store.list_events(run_id)
        control = RemoteControlPlane(
            lambda _run_id: self.harness.scheduler,
            authorize_run=lambda _identity, requested: requested == run_id,
            assignment_admitter=protocol_tests._UnmarkedLegacyAdmitter(
                self.harness.artifacts
            ),
            journal=RemoteControlJournal(
                self.harness.control_root / "postfix-empty.sqlite3"
            ),
        )
        client = self._client(control, instance_id="empty-misconfigured")
        self._register(client)
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "security_not_ready",
        ):
            client.poll(run_id)
        self.assertEqual(
            before_attempts,
            self.harness.store.list_attempts(run_id),
        )
        self.assertEqual(
            before_events,
            self.harness.store.list_events(run_id),
        )


if __name__ == "__main__":
    unittest.main()
