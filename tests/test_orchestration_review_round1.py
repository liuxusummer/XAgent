from __future__ import annotations

import threading
import unittest

from src.orchestration.models import AttemptStatus
from src.orchestration.remote_control import RemoteControlPlane
from src.orchestration.remote_journal import RemoteControlJournal
from src.orchestration.remote_fleet import (
    RemoteFleetConflict,
    RemoteFleetCoordinator,
)
from src.orchestration.remote_protocol import (
    AuthenticatedWorker,
    RemoteOperation,
    make_request,
    parse_response,
)
from src.orchestration.remote_scheduling import (
    DeterministicRemoteScheduler,
    ReleaseOutcome,
)
from src.orchestration.remote_worker import RemoteWorkerClient, RemoteWorkerError

from tests import test_orchestration_remote_execution as remote_execution_tests
from tests import test_orchestration_remote_fleet as remote_fleet_tests
from tests import test_orchestration_remote_protocol as remote_protocol_tests


class _BlockingJournal(RemoteControlJournal):
    def __init__(self, path) -> None:
        super().__init__(path)
        self.request_entered = threading.Event()
        self.request_release = threading.Event()
        self.registration_entered = threading.Event()
        self.registration_release = threading.Event()

    def record_request(self, **kwargs):
        if kwargs["request_id"] == "blocked-poll":
            self.request_entered.set()
            if not self.request_release.wait(timeout=5):
                raise RuntimeError("request journal barrier timed out")
        return super().record_request(**kwargs)

    def register_session(self, **kwargs):
        if kwargs["instance_id"] == "blocked-registration":
            self.registration_entered.set()
            if not self.registration_release.wait(timeout=5):
                raise RuntimeError("registration journal barrier timed out")
        return super().register_session(**kwargs)


class OrchestrationReviewRound1Tests(unittest.TestCase):
    def _protocol_harness(self) -> remote_protocol_tests.RemoteProtocolTests:
        harness = remote_protocol_tests.RemoteProtocolTests(methodName="runTest")
        harness.setUp()
        self.addCleanup(harness.doCleanups)
        return harness

    def _secure_harness(
        self,
    ) -> remote_execution_tests.SecureRemoteExecutionTests:
        harness = remote_execution_tests.SecureRemoteExecutionTests(
            methodName="runTest"
        )
        harness.setUp()
        self.addCleanup(harness.doCleanups)
        return harness

    def test_session_instance_aba_restores_superseded_claim_authority(self) -> None:
        """A -> B -> A registration revives A's supposedly stale claim."""

        harness = self._protocol_harness()
        client_a = harness._client(harness.control, instance_id="instance-a")
        client_b = harness._client(harness.control, instance_id="instance-b")
        harness._register(client_a)
        assignment = client_a.poll("run-remote", lease_seconds=30)
        self.assertIsNotNone(assignment)
        assert assignment is not None

        harness._register(client_b)
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "worker_identity_mismatch|claim_conflict",
        ):
            client_a.start(assignment.claim)

        # A durable tombstone rejects the retired caller-chosen instance id;
        # it cannot allocate another epoch after A -> B, even after restart.
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "worker_identity_mismatch",
        ):
            harness._register(client_a)

        restarted = harness._control()
        recovered_b = harness._client(
            restarted,
            instance_id="instance-b",
        )
        harness._register(recovered_b)
        retired_a = harness._client(
            restarted,
            instance_id="instance-a",
        )
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "worker_identity_mismatch",
        ):
            harness._register(retired_a)
        self.assertEqual(
            harness.store.get_attempt(assignment.claim.attempt_id).status,
            AttemptStatus.CLAIMED,
        )

    def test_prepared_ttl_rotates_authority_for_a_live_running_claim(self) -> None:
        """An absolute prepared TTL makes the original assignment unusable."""

        harness = self._secure_harness()
        admitter = harness._admitter(prepared_ttl_seconds=1.0)
        control = RemoteControlPlane(
            lambda run_id: harness.scheduler,
            authorize_run=lambda identity, run_id: (
                identity == harness.identity and run_id == "ttl-run"
            ),
            assignment_admitter=admitter,
            clock=harness.clock,
            journal=RemoteControlJournal(
                harness.control_root / "ttl-review-remote-control.sqlite3"
            ),
        )
        client = RemoteWorkerClient(
            lambda request: control.handle(harness.identity, request),
            worker_id=harness.identity.worker_id,
            instance_id="ttl-instance",
        )
        client.register(
            runtime_version="runsc-1.2",
            capabilities=("activity.tool",),
            resource_keys=("workspace:/project",),
            activity_kinds=("tool",),
            max_concurrency=1,
        )
        harness.scheduler.create_run("ttl-run")
        harness.scheduler.reconcile("ttl-run")
        assignment = client.poll("ttl-run", lease_seconds=30)
        self.assertIsNotNone(assignment)
        assert assignment is not None
        client.start(assignment.claim)

        harness.clock.value += 2.0
        renewed = client.heartbeat(assignment.claim, lease_seconds=30)
        self.assertTrue(renewed["renewed"])
        self.assertEqual(
            harness.store.get_attempt(assignment.claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )

    def test_request_id_conflict_survives_response_cache_eviction(self) -> None:
        """Eviction must not turn one request id into a second mutation."""

        harness = self._protocol_harness()
        harness.scheduler.create_run("run-second")
        control = RemoteControlPlane(
            lambda run_id: harness.scheduler,
            authorize_run=lambda identity, run_id: (
                identity == harness.identity
                and run_id in {"run-remote", "run-second"}
            ),
            assignment_admitter=harness.admitter,
            max_request_cache=1,
            journal=RemoteControlJournal(harness.journal_path),
        )
        register = make_request(
            RemoteOperation.REGISTER,
            request_id="register",
            worker_id=harness.identity.worker_id,
            instance_id="cache-instance",
            body={
                "runtime_version": "worker-runtime/1.0",
                "capabilities": ["activity.tool", "artifact.refs"],
                "resource_keys": ["workspace:project"],
                "activity_kinds": ["tool"],
                "max_concurrency": 2,
            },
        )
        self.assertTrue(
            parse_response(
                control.handle(harness.identity, register.to_wire())
            ).ok
        )
        first = make_request(
            RemoteOperation.POLL,
            request_id="reused-poll-id",
            worker_id=harness.identity.worker_id,
            instance_id="cache-instance",
            body={"run_id": "run-remote", "lease_seconds": 30.0},
        )
        first_response = parse_response(
            control.handle(harness.identity, first.to_wire())
        )
        self.assertTrue(first_response.ok)
        self.assertIsNotNone(first_response.body["assignment"])

        # Any different request evicts the first poll response.
        evict = make_request(
            RemoteOperation.REGISTER,
            request_id="evict",
            worker_id=harness.identity.worker_id,
            instance_id="cache-instance",
            body={
                "runtime_version": "worker-runtime/1.0",
                "capabilities": ["activity.tool", "artifact.refs"],
                "resource_keys": ["workspace:project"],
                "activity_kinds": ["tool"],
                "max_concurrency": 2,
            },
        )
        self.assertTrue(
            parse_response(
                control.handle(harness.identity, evict.to_wire())
            ).ok
        )

        conflicting = make_request(
            RemoteOperation.POLL,
            request_id="reused-poll-id",
            worker_id=harness.identity.worker_id,
            instance_id="cache-instance",
            body={"run_id": "run-second", "lease_seconds": 30.0},
        )
        response = parse_response(
            control.handle(harness.identity, conflicting.to_wire())
        )
        self.assertFalse(response.ok)
        self.assertEqual(response.body["error_code"], "request_id_conflict")

        restarted = RemoteControlPlane(
            lambda run_id: harness.scheduler,
            authorize_run=lambda identity, run_id: (
                identity == harness.identity
                and run_id in {"run-remote", "run-second"}
            ),
            assignment_admitter=harness.admitter,
            max_request_cache=1,
            journal=RemoteControlJournal(harness.journal_path),
        )
        restart_register = make_request(
            RemoteOperation.REGISTER,
            request_id="restart-register",
            worker_id=harness.identity.worker_id,
            instance_id="cache-instance",
            body=dict(register.body),
        )
        self.assertTrue(
            parse_response(
                restarted.handle(
                    harness.identity,
                    restart_register.to_wire(),
                )
            ).ok
        )
        after_restart = parse_response(
            restarted.handle(harness.identity, conflicting.to_wire())
        )
        self.assertFalse(after_restart.ok)
        self.assertEqual(
            after_restart.body["error_code"],
            "request_id_conflict",
        )
        self.assertEqual(
            len(harness.store.list_attempts("run-remote"))
            + len(harness.store.list_attempts("run-second")),
            1,
        )

    def test_journal_io_never_holds_the_control_plane_global_lock(self) -> None:
        harness = self._protocol_harness()
        journal = _BlockingJournal(
            harness.control_root / "blocking-remote-control.sqlite3"
        )
        control = RemoteControlPlane(
            lambda _run_id: harness.scheduler,
            authorize_run=lambda identity, _run_id: (
                identity.tenant_id == "tenant-1"
            ),
            assignment_admitter=harness.admitter,
            journal=journal,
        )
        first = harness._client(
            control,
            instance_id="first-instance",
        )
        harness._register(first)
        blocked_poll = make_request(
            RemoteOperation.POLL,
            request_id="blocked-poll",
            worker_id=harness.identity.worker_id,
            instance_id="first-instance",
            body={"run_id": "run-remote", "lease_seconds": 30.0},
        )
        results: dict[str, object] = {}

        def poll() -> None:
            results["poll"] = control.handle(
                harness.identity,
                blocked_poll.to_wire(),
            )

        poll_thread = threading.Thread(target=poll)
        poll_thread.start()
        self.assertTrue(journal.request_entered.wait(timeout=2))
        self.assertIsNotNone(
            control.get_registration(harness.identity.worker_id),
            "a blocked request journal held the global control lock",
        )
        journal.request_release.set()
        poll_thread.join(timeout=5)
        self.assertFalse(poll_thread.is_alive())

        second_identity = AuthenticatedWorker(
            worker_id="worker-2",
            tenant_id="tenant-1",
            identity_digest="2" * 64,
        )
        second = harness._client(
            control,
            identity=second_identity,
            worker_id="worker-2",
            instance_id="blocked-registration",
        )
        def register() -> None:
            try:
                harness._register(second)
                results["register"] = True
            except BaseException as exc:
                results["register_error"] = exc

        register_thread = threading.Thread(target=register)
        register_thread.start()
        self.assertTrue(journal.registration_entered.wait(timeout=2))
        self.assertIsNotNone(
            control.get_registration(harness.identity.worker_id),
            "a blocked registration journal held the global control lock",
        )
        journal.registration_release.set()
        register_thread.join(timeout=5)
        self.assertFalse(register_thread.is_alive())
        self.assertNotIn("register_error", results)
        self.assertTrue(results.get("register"))

    def test_crash_after_request_journal_before_dispatch_fails_closed(self) -> None:
        harness = self._protocol_harness()
        journal_path = (
            harness.control_root / "crash-window-remote-control.sqlite3"
        )
        journal = RemoteControlJournal(journal_path)
        control = RemoteControlPlane(
            lambda _run_id: harness.scheduler,
            authorize_run=lambda identity, _run_id: (
                identity == harness.identity
            ),
            assignment_admitter=harness.admitter,
            journal=journal,
        )
        client = harness._client(
            control,
            instance_id="crash-window-instance",
        )
        harness._register(client)
        registration = control.get_registration(harness.identity.worker_id)
        assert registration is not None
        request = make_request(
            RemoteOperation.POLL,
            request_id="journaled-before-dispatch",
            worker_id=harness.identity.worker_id,
            instance_id=registration.instance_id,
            body={"run_id": "run-remote", "lease_seconds": 30.0},
        )
        self.assertTrue(
            journal.record_request(
                worker_id=registration.worker_id,
                tenant_id=registration.tenant_id,
                identity_digest=registration.identity_digest,
                instance_id=registration.instance_id,
                epoch=registration.session_epoch,
                request_id=request.request_id,
                request_digest=request.request_digest,
                operation=request.operation.value,
                now=1.0,
            )
        )

        response = parse_response(
            control.handle(harness.identity, request.to_wire())
        )
        self.assertFalse(response.ok)
        self.assertEqual(response.body["error_code"], "control_unavailable")
        self.assertEqual(harness.store.list_attempts("run-remote"), [])

        restarted = RemoteControlPlane(
            lambda _run_id: harness.scheduler,
            authorize_run=lambda identity, _run_id: (
                identity == harness.identity
            ),
            assignment_admitter=harness.admitter,
            journal=RemoteControlJournal(journal_path),
        )
        restarted_client = harness._client(
            restarted,
            instance_id="crash-window-instance",
        )
        harness._register(restarted_client)
        restarted_response = parse_response(
            restarted.handle(harness.identity, request.to_wire())
        )
        self.assertFalse(restarted_response.ok)
        self.assertEqual(
            restarted_response.body["error_code"],
            "control_unavailable",
        )
        self.assertEqual(harness.store.list_attempts("run-remote"), [])

    def test_fleet_failed_projection_pop_races_rebuild_of_same_task_id(self) -> None:
        """Old rollback removes a newly rebuilt trusted task binding."""

        release_entered = threading.Event()
        release_continue = threading.Event()
        old_scheduler = DeterministicRemoteScheduler()
        rebuilt_scheduler = DeterministicRemoteScheduler()
        schedulers = iter((old_scheduler, rebuilt_scheduler))

        original_release = old_scheduler.release

        def blocking_release(*args, **kwargs):
            outcome = original_release(*args, **kwargs)
            if outcome is ReleaseOutcome.RELEASED:
                release_entered.set()
                if not release_continue.wait(timeout=5):
                    raise RuntimeError("release barrier timed out")
            return outcome

        old_scheduler.release = blocking_release  # type: ignore[method-assign]
        fleet = RemoteFleetCoordinator(
            lambda: next(schedulers),
            lambda run_id, worker_id: remote_fleet_tests._work_assignment(
                "different-run",
                worker_id,
            ),
        )
        worker = fleet.register_worker(remote_fleet_tests._worker("worker-1"))
        fleet.admit(remote_fleet_tests._binding("task-1", "old-run"))
        result: dict[str, BaseException] = {}

        def assign() -> None:
            try:
                fleet.assign_next(
                    "worker-1",
                    worker_generation=worker.generation,
                    session_id=worker.descriptor.session_id,
                )
            except BaseException as exc:
                result["error"] = exc

        thread = threading.Thread(target=assign)
        thread.start()
        self.assertTrue(release_entered.wait(timeout=5))

        # Rebuild must remain excluded through the complete rollback.  Once
        # that generation finishes, a retry may install the new projection.
        with self.assertRaisesRegex(
            RemoteFleetConflict,
            "claim callbacks",
        ):
            fleet.rebuild(
                [remote_fleet_tests._binding("task-1", "new-run")],
                [remote_fleet_tests._worker("worker-1")],
            )
        release_continue.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(result.get("error"), RemoteFleetConflict)

        fleet.rebuild(
            [remote_fleet_tests._binding("task-1", "new-run")],
            [remote_fleet_tests._worker("worker-1")],
        )
        snapshot = fleet.snapshot()
        self.assertEqual(snapshot.queued_tasks, 1)
        self.assertEqual(
            snapshot.task_bindings,
            1,
            "old rollback deleted the newly rebuilt task binding",
        )


if __name__ == "__main__":
    unittest.main()
