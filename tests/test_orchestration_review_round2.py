from __future__ import annotations

import unittest
from dataclasses import replace

from src.orchestration.models import AttemptStatus
from src.orchestration.remote_control import RemoteControlPlane
from src.orchestration.remote_execution import SecureRemoteExecutionError
from src.orchestration.remote_journal import RemoteControlJournal
from src.orchestration.remote_protocol import (
    RemoteOperation,
    make_request,
    parse_response,
)
from src.orchestration.remote_worker import RemoteWorkerError
from src.orchestration.sandbox import ResourceLimits

from tests import test_orchestration_remote_execution as remote_execution_tests
from tests import test_orchestration_remote_protocol as remote_protocol_tests


class _DenyAfterPreflightAdmitter(remote_protocol_tests._TestAdmitter):
    """A coarse preflight pass followed by exact authorization denial."""

    def admit(self, identity, registration, scheduler, claim):
        del identity, registration, scheduler, claim
        raise RuntimeError("exact worker authorization denied")


class OrchestrationReviewRound2Tests(unittest.TestCase):
    """Independent security and authorization-boundary attacks."""

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

    def test_response_repr_never_exposes_claim_or_artifact_bearer_tokens(
        self,
    ) -> None:
        """P1: ordinary response logging must not disclose live credentials."""

        harness = self._protocol_harness()
        harness._register(harness.client)
        request = make_request(
            RemoteOperation.POLL,
            request_id="security-review-poll",
            worker_id=harness.identity.worker_id,
            instance_id="instance-1",
            body={"run_id": "run-remote", "lease_seconds": 30.0},
        )
        response = parse_response(
            harness.control.handle(harness.identity, request.to_wire())
        )
        self.assertTrue(response.ok)
        assignment = response.body["assignment"]
        self.assertIsNotNone(assignment)
        assert assignment is not None
        claim_token = assignment["claim"]["claim_token"]
        artifact_token = assignment["output_grants"][0]["handle"]["token"]

        rendered = repr(response)
        leaked = tuple(
            value
            for value in (claim_token, artifact_token)
            if value in rendered
        )
        self.assertEqual(
            0,
            len(leaked),
            "RemoteResponse repr disclosed live claim/broker credentials",
        )

    def test_control_record_repr_never_exposes_live_claim_tokens(self) -> None:
        """P1: internal diagnostics must preserve the same redaction boundary."""

        harness = self._protocol_harness()
        harness._register(harness.client)
        assignment = harness.client.poll("run-remote", lease_seconds=30.0)
        self.assertIsNotNone(assignment)
        assert assignment is not None
        registration = harness.control.get_registration(
            harness.identity.worker_id
        )
        self.assertIsNotNone(registration)
        assert registration is not None
        restored = harness.scheduler.restore_claim(
            assignment.claim.run_id,
            assignment.claim.node_id,
            assignment.claim.attempt_id,
            registration.session_owner_id,
            request_hash=assignment.claim.activity_request_digest,
            claim_token=assignment.claim.claim_token,
            fencing_token=assignment.claim.fencing_token,
        )
        attempt = harness.store.get_attempt(assignment.claim.attempt_id)
        self.assertIsNotNone(attempt)
        assert attempt is not None
        idempotency = harness.store.get_idempotency(
            assignment.claim.run_id,
            attempt.idempotency_key,
        )
        self.assertIsNotNone(idempotency)
        assert idempotency is not None

        leaked = tuple(
            rendered
            for rendered in (
                repr(restored),
                repr(attempt),
                repr(idempotency),
            )
            if assignment.claim.claim_token in rendered
        )
        self.assertEqual(
            0,
            len(leaked),
            "control-side claim records disclosed a live claim credential",
        )

    def test_attempt_cumulative_output_budget_rejects_before_second_store_write(
        self,
    ) -> None:
        """P1: a Worker cannot amplify one Attempt beyond authorized output."""

        harness = self._secure_harness()
        limits = ResourceLimits(
            timeout_seconds=60.0,
            cpu_seconds=60.0,
            memory_bytes=256 * 1024 * 1024,
            output_bytes=4,
            process_count=16,
        )
        preparation = replace(harness.preparation, limits=limits)
        admitter = harness._admitter(
            plan_resolver=remote_execution_tests._PlanResolver(preparation)
        )
        claim = harness._claim("output-budget-run")
        authorization = admitter.admit(
            harness.identity,
            harness.registration,
            harness.scheduler,
            claim,
        )
        harness.scheduler.start_claim(claim)

        admitter.stage_output(
            harness.identity,
            harness.registration,
            harness.scheduler,
            claim,
            authorization,
            content=b"abc",
        )
        before_second = {
            path
            for path in harness.artifacts.root.rglob("*")
            if path.is_file()
        }
        denied = False
        try:
            admitter.stage_output(
                harness.identity,
                harness.registration,
                harness.scheduler,
                claim,
                authorization,
                content=b"def",
            )
        except SecureRemoteExecutionError:
            denied = True
        after_second = {
            path
            for path in harness.artifacts.root.rglob("*")
            if path.is_file()
        }

        self.assertTrue(
            denied,
            "one Attempt staged cumulative bytes beyond its authorized output budget",
        )
        self.assertEqual(
            before_second,
            after_second,
            "output-budget denial happened after ArtifactStore mutation",
        )

    def test_exact_authorization_denial_precedes_durable_claim_mutation(
        self,
    ) -> None:
        """P1: coarse preflight cannot let a denied Worker reserve work."""

        harness = self._protocol_harness()
        control = RemoteControlPlane(
            lambda run_id: harness.scheduler,
            authorize_run=lambda identity, run_id: (
                identity == harness.identity and run_id == "run-remote"
            ),
            assignment_admitter=_DenyAfterPreflightAdmitter(
                harness.artifacts
            ),
            journal=RemoteControlJournal(
                harness.control_root / "post-claim-denial.sqlite3"
            ),
        )
        client = harness._client(control)
        harness._register(client)
        before_attempt_count = len(harness.store.list_attempts("run-remote"))
        before_event_count = len(harness.store.list_events("run-remote"))

        with self.assertRaisesRegex(
            RemoteWorkerError,
            "authorization_conflict",
        ):
            client.poll("run-remote", lease_seconds=30.0)

        self.assertEqual(
            before_attempt_count,
            len(harness.store.list_attempts("run-remote")),
            "exact authorization denial left a durable claimed Attempt",
        )
        self.assertEqual(
            before_event_count,
            len(harness.store.list_events("run-remote")),
            "exact authorization denial changed the durable event stream",
        )

    def test_claim_authority_field_substitution_never_starts_attempt(
        self,
    ) -> None:
        """Exact action/session/grant/attestation/fencing fields are inseparable."""

        harness = self._protocol_harness()
        harness._register(harness.client)
        assignment = harness.client.poll("run-remote", lease_seconds=30.0)
        self.assertIsNotNone(assignment)
        assert assignment is not None
        claim = assignment.claim
        alternate_digest = "0" * 64
        substitutions = (
            replace(claim, activity_request_digest=alternate_digest),
            replace(claim, action_digest=alternate_digest),
            replace(claim, authorization_digest=alternate_digest),
            replace(claim, profile_digest=alternate_digest),
            replace(claim, request_digest=alternate_digest),
            replace(claim, session_binding_digest=alternate_digest),
            replace(claim, grant_binding_digest=alternate_digest),
            replace(claim, execution_plan_digest=alternate_digest),
            replace(claim, runtime_attestation_digest=alternate_digest),
            replace(claim, claim_token="forged-claim-token"),
            replace(claim, fencing_token=claim.fencing_token + 1),
        )
        before_event_count = len(harness.store.list_events("run-remote"))

        for forged in substitutions:
            with self.assertRaisesRegex(
                RemoteWorkerError,
                "authorization_conflict|claim_conflict",
            ):
                harness.client.start(forged)

        attempt = harness.store.get_attempt(claim.attempt_id)
        self.assertIsNotNone(attempt)
        assert attempt is not None
        self.assertIs(attempt.status, AttemptStatus.CLAIMED)
        self.assertEqual(
            before_event_count,
            len(harness.store.list_events("run-remote")),
        )


if __name__ == "__main__":
    unittest.main()
