from __future__ import annotations

import hashlib
import json
import secrets
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

from src.orchestration.artifact_broker import ArtifactWriteGrant
from src.orchestration.artifacts import (
    ArtifactKind,
    ArtifactSensitivity,
    LocalArtifactStore,
)
from src.orchestration.models import AttemptStatus, RunStatus
from src.orchestration.remote_control import (
    RemoteAdmissionAuthorization,
    RemoteControlPlane,
)
from src.orchestration.remote_journal import RemoteControlJournal
from src.orchestration.remote_protocol import (
    AuthenticatedWorker,
    ClaimBinding,
    ExecutionAuthorization,
    MAX_REMOTE_MESSAGE_BYTES,
    RemoteExecutionPlan,
    RemoteOperation,
    RemoteProtocolError,
    RemoteRequest,
    RemoteResponse,
    RemoteRuntimeProof,
    WorkAssignment,
    canonical_digest,
    grant_binding_digest,
    make_request,
    parse_request,
    parse_response,
    runtime_binding_digest,
)
from src.orchestration.remote_worker import (
    RemoteExecutionGrant,
    RemoteExecutionOutcome,
    RemoteWorkerClient,
    RemoteWorkerDaemon,
    RemoteWorkerError,
)
from src.orchestration.scheduler import (
    ActivityReceipt,
    DurableScheduler,
    SchedulerStateError,
)
from src.orchestration.store import DurableRunStore
from src.orchestration.workflow import compile_workflow


class _Clock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class _TestAdmitter:
    # Protocol harness only. Production signature/sandbox verification is
    # exercised by the worker-security integration tests.
    production_security_ready = True
    secure_two_phase_admission = True
    runtime_attestation_digest = hashlib.sha256(b"test-runtime").hexdigest()

    def __init__(self, artifacts: LocalArtifactStore | None = None) -> None:
        self.artifacts = artifacts
        self._admission_lock = threading.Lock()
        self._admissions = {}

    def preflight(self, identity, registration, scheduler):
        del identity, registration, scheduler

    def admit(self, identity, registration, _scheduler, claim):
        action_digest = hashlib.sha256(
            f"{claim.request_hash}\0{claim.run_id}\0{claim.node_id}".encode()
        ).hexdigest()
        authorization_digest = hashlib.sha256(
            (
                f"{action_digest}\0{identity.identity_digest}\0"
                f"{registration.runtime_version}"
            ).encode()
        ).hexdigest()
        profile_digest = hashlib.sha256(b"test-profile").hexdigest()
        request_digest = hashlib.sha256(
            f"request:{claim.request_hash}".encode()
        ).hexdigest()
        execution_plan = RemoteExecutionPlan(
            argv=("/usr/bin/true",),
            container_cwd="/workspace",
            limits={
                "schema_version": 2,
                "timeout_seconds": 60.0,
                "cpu_seconds": 60.0,
                "memory_bytes": 256 * 1024 * 1024,
                "output_bytes": 1024 * 1024,
                "process_count": 16,
            },
            profile_id="test-profile",
            profile_digest=profile_digest,
            policy_version="test-policy-v1",
            request_digest=request_digest,
            capabilities=("activity.tool",),
        )
        output_grant = ArtifactWriteGrant(
            grant_id=f"grant-{claim.attempt_id}",
            token=f"token-{claim.attempt_id}",
            tenant_id=identity.tenant_id,
            worker_id=identity.worker_id,
            run_id=claim.run_id,
            node_id=claim.node_id,
            attempt_id=claim.attempt_id,
            action_digest=action_digest,
            authorization_digest=authorization_digest,
            kind=ArtifactKind.TOOL_RESULT,
            sensitivity=ArtifactSensitivity.INTERNAL,
            media_type="application/json",
            maximum_bytes=1024,
            declared_sha256=hashlib.sha256(b'{"status":"ok"}').hexdigest(),
            issued_at=100.0,
            expires_at=200.0,
        )
        outputs = (output_grant,)
        return ExecutionAuthorization(
            action_digest=action_digest,
            authorization_digest=authorization_digest,
            profile_digest=profile_digest,
            request_digest=request_digest,
            session_binding_digest=registration.session_binding_digest,
            grant_binding_digest=grant_binding_digest((), outputs),
            execution_plan_digest=execution_plan.plan_digest,
            runtime_attestation_digest=self.runtime_attestation_digest,
            execution_plan=execution_plan,
            output_grants=outputs,
        )

    def prepare_admission(
        self,
        identity,
        registration,
        scheduler,
        candidate,
    ):
        authorization = self.admit(
            identity,
            registration,
            scheduler,
            candidate.claim,
        )
        ticket_id = secrets.token_urlsafe(24)
        policy_digest = hashlib.sha256(
            authorization.execution_plan.policy_version.encode()
        ).hexdigest()
        policy_binding = {
            "outcome": "allow",
            "reason_code": "test_reference_allow",
            "action_digest": authorization.action_digest,
            "policy_digest": policy_digest,
            "profile_digest": authorization.profile_digest,
            "decision_digest": hashlib.sha256(
                (
                    f"{authorization.action_digest}\0"
                    f"{authorization.authorization_digest}"
                ).encode()
            ).hexdigest(),
            "approval_grant_digest": None,
        }
        with self._admission_lock:
            self._admissions[ticket_id] = {
                "candidate_digest": candidate.candidate_digest,
                "authorization": authorization,
                "policy_binding": policy_binding,
                "state": "prepared",
            }
        return RemoteAdmissionAuthorization(
            ticket_id=ticket_id,
            candidate_digest=candidate.candidate_digest,
            expires_at=200.0,
            policy_binding=policy_binding,
        )

    def begin_admission(
        self,
        _identity,
        _registration,
        _scheduler,
        candidate,
        admission,
    ):
        with self._admission_lock:
            record = self._admissions.get(admission.ticket_id)
            if (
                record is None
                or record["state"] != "prepared"
                or record["candidate_digest"] != candidate.candidate_digest
                or record["policy_binding"] != admission.policy_binding
            ):
                raise RuntimeError("invalid test admission ticket")
            record["state"] = "committing"
        return admission.expires_at

    def commit_admission(
        self,
        _identity,
        _registration,
        _scheduler,
        candidate,
        claim,
        admission,
        _policy_event,
    ):
        with self._admission_lock:
            record = self._admissions.pop(admission.ticket_id, None)
        if (
            record is None
            or record["state"] != "committing"
            or record["candidate_digest"] != candidate.candidate_digest
            or claim.attempt_id != candidate.claim.attempt_id
        ):
            raise RuntimeError("invalid test admission commit")
        return record["authorization"]

    def cancel_admission(self, admission):
        with self._admission_lock:
            self._admissions.pop(admission.ticket_id, None)

    def complete(
        self,
        _identity,
        _registration,
        scheduler,
        claim,
        _authorization,
        candidate,
        runtime_proof,
    ):
        self.assert_runtime_binding(candidate, runtime_proof)
        if candidate.outcome is AttemptStatus.SUCCEEDED:
            if self.artifacts is None:
                raise RuntimeError("test artifact store unavailable")
            expected_handles = tuple(
                grant.output_handle for grant in _authorization.output_grants
            )
            if candidate.output_handles != expected_handles:
                raise RuntimeError("unexpected output handle")
            reference = self.artifacts.put_json(
                {"status": "ok"},
                kind=ArtifactKind.TOOL_RESULT,
                producer_run_id=claim.run_id,
                producer_node_id=claim.node_id,
                producer_attempt_id=claim.attempt_id,
            )
            scheduler.complete_claim(
                claim,
                ActivityReceipt((reference,)),
                attempt_status=candidate.outcome,
            )
        else:
            scheduler.complete_claim(
                claim,
                {"error_code": candidate.error_code},
                attempt_status=candidate.outcome,
                error_class=candidate.error_class,
            )

    def cancel(
        self,
        _identity,
        _registration,
        scheduler,
        claim,
        _authorization,
        runtime_proof,
    ):
        if runtime_proof.sandbox_receipt["outcome"] != "cancelled":
            raise RuntimeError("invalid cancellation proof")
        scheduler.confirm_cancel_claim(claim)

    @staticmethod
    def assert_runtime_binding(candidate, runtime_proof) -> None:
        if candidate.runtime_proof != runtime_proof:
            raise RuntimeError("runtime proof mismatch")


class _TestExecutionAdapter:
    production_security_ready = True
    runtime_attestation_digest = _TestAdmitter.runtime_attestation_digest

    def __init__(self, execute):
        self._execute = execute

    def prepare(self, assignment):
        claim = assignment.claim
        return RemoteExecutionGrant(
            assignment=assignment,
            action_digest=claim.action_digest,
            authorization_digest=claim.authorization_digest,
            profile_digest=claim.profile_digest,
            request_digest=claim.request_digest,
            session_binding_digest=claim.session_binding_digest,
            grant_binding_digest=claim.grant_binding_digest,
            execution_plan_digest=claim.execution_plan_digest,
            runtime_attestation_digest=claim.runtime_attestation_digest,
        )

    def execute(self, grant, context):
        return self._execute(grant.assignment, context)

    def verify_candidate(self, grant, candidate):
        if grant.runtime_attestation_digest != self.runtime_attestation_digest:
            raise ValueError("attestation mismatch")
        return candidate

    def cancel(self, grant, _context):
        return _runtime_proof(
            grant.assignment,
            outcome="cancelled",
            output_handles=(),
        )


class _NoCommitAdmitter(_TestAdmitter):
    def complete(
        self,
        _identity,
        _registration,
        _scheduler,
        _claim,
        _authorization,
        _candidate,
        _runtime_proof,
    ):
        return None


class _DenyPreflightAdmitter(_TestAdmitter):
    def preflight(self, identity, registration, scheduler):
        del identity, registration, scheduler
        raise RuntimeError("preflight denied")


class _UnmarkedLegacyAdmitter(_TestAdmitter):
    secure_two_phase_admission = False
    reference_admission_only = False


class _MarkedLegacyAdmitter(_TestAdmitter):
    secure_two_phase_admission = False
    reference_admission_only = True


class _BlockingAdmitter(_TestAdmitter):
    def __init__(self, artifacts: LocalArtifactStore) -> None:
        super().__init__(artifacts)
        self.entered = threading.Event()
        self.release = threading.Event()

    def admit(self, identity, registration, scheduler, claim):
        self.entered.set()
        if not self.release.wait(5.0):
            raise RuntimeError("test admission barrier timed out")
        return super().admit(identity, registration, scheduler, claim)


def _runtime_proof(
    assignment: WorkAssignment,
    *,
    outcome: str,
    output_handles: tuple = (),
    error_code: str | None = None,
) -> RemoteRuntimeProof:
    sandbox_outcome = {
        "succeeded": "succeeded",
        "failed": "failed",
        "timed_out": "timed_out",
        "abandoned": "backend_error",
        "outcome_unknown": "cancellation_unknown",
        "cancelled": "cancelled",
    }[outcome]
    receipt_outputs = []
    if outcome == "succeeded":
        receipt_outputs = [
            {
                "artifact_id": f"result-{index}",
                "sha256": hashlib.sha256(
                    f"result-{index}".encode()
                ).hexdigest(),
                "size": 1,
                "kind": ArtifactKind.TOOL_RESULT.value,
            }
            for index, _handle in enumerate(output_handles)
        ]
    receipt = {
        "schema_version": 2,
        "backend_id": "test-container",
        "security_level": "container",
        "profile_id": assignment.execution_plan.profile_id,
        "profile_digest": assignment.claim.profile_digest,
        "action_digest": assignment.claim.action_digest,
        "policy_version": assignment.execution_plan.policy_version,
        "request_digest": assignment.claim.request_digest,
        "outcome": sandbox_outcome,
        "exit_code": 0 if outcome == "succeeded" else None,
        "timed_out": outcome == "timed_out",
        "output_artifact_refs": receipt_outputs,
        "error_code": error_code,
    }
    sandbox_spec_digest = hashlib.sha256(b"test-sandbox-spec").hexdigest()
    signed_binding_digest = runtime_binding_digest(
        assignment.claim,
        outcome=outcome,
        output_handles=tuple(output_handles),
        sandbox_receipt_digest=canonical_digest(receipt),
        sandbox_spec_digest=sandbox_spec_digest,
    )
    return RemoteRuntimeProof(
        proof_id=f"proof-{assignment.claim.attempt_id}-{outcome}",
        verifier_key_id="test-verifier-key",
        signed_binding_digest=signed_binding_digest,
        signature="dGVzdA",
        sandbox_spec_digest=sandbox_spec_digest,
        sandbox_receipt=receipt,
    )


class RemoteProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.control_root = self.root / "control"
        self.agent_root = self.root / "agent"
        self.control_root.mkdir()
        self.agent_root.mkdir()
        self.journal_path = self.control_root / "remote-control.sqlite3"
        self.clock = _Clock()
        self.artifacts = LocalArtifactStore(self.control_root / "artifacts")
        self.store = DurableRunStore(self.control_root / "runs.sqlite3")
        self.workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": "remote-protocol",
                "version": 1,
                "nodes": [
                    {
                        "id": "tool",
                        "kind": "tool",
                        "config": {
                            "tool": "inspect",
                            "arguments": {"mode": "summary"},
                        },
                        "effect_class": "read_only",
                        "resource_keys": ["workspace:project"],
                    }
                ],
            }
        )
        self.scheduler = DurableScheduler(
            self.store,
            self.workflow,
            clock=self.clock,
            artifact_verifier=self.artifacts.verify,
        )
        self.scheduler.create_run("run-remote")
        self.scheduler.reconcile("run-remote")
        self.identity = AuthenticatedWorker(
            worker_id="worker-1",
            tenant_id="tenant-1",
            identity_digest=hashlib.sha256(b"worker-1-identity").hexdigest(),
        )
        self.admitter = _TestAdmitter(self.artifacts)
        self.control = self._control()
        self.client = self._client(self.control)

    def _control(self) -> RemoteControlPlane:
        return RemoteControlPlane(
            lambda run_id: self.scheduler
            if run_id == "run-remote"
            else (_ for _ in ()).throw(KeyError(run_id)),
            authorize_run=lambda identity, run_id: (
                identity.tenant_id == "tenant-1" and run_id == "run-remote"
            ),
            assignment_admitter=self.admitter,
            journal=RemoteControlJournal(self.journal_path),
        )

    def _client(
        self,
        control: RemoteControlPlane,
        *,
        identity: AuthenticatedWorker | None = None,
        worker_id: str = "worker-1",
        instance_id: str = "instance-1",
    ) -> RemoteWorkerClient:
        principal = identity or self.identity
        return RemoteWorkerClient(
            lambda request: control.handle(principal, request),
            worker_id=worker_id,
            instance_id=instance_id,
        )

    @staticmethod
    def _success_outcome(assignment: WorkAssignment) -> RemoteExecutionOutcome:
        handles = tuple(grant.output_handle for grant in assignment.output_grants)
        return RemoteExecutionOutcome(
            "succeeded",
            handles,
            _runtime_proof(
                assignment,
                outcome="succeeded",
                output_handles=handles,
            ),
        )

    @staticmethod
    def _register(client: RemoteWorkerClient) -> None:
        client.register(
            runtime_version="worker-runtime/1.0",
            capabilities=("activity.tool", "artifact.refs"),
            resource_keys=("workspace:project",),
            activity_kinds=("tool",),
            max_concurrency=2,
        )

    def test_strict_versioned_request_digest_and_bounds(self) -> None:
        request = make_request(
            RemoteOperation.REGISTER,
            request_id="request-1",
            worker_id="worker-1",
            instance_id="instance-1",
            body={
                "runtime_version": "runtime/1",
                "capabilities": ["activity.tool"],
                "resource_keys": ["workspace:project"],
                "activity_kinds": ["tool"],
                "max_concurrency": 1,
            },
        )
        parsed = parse_request(request.to_wire())
        self.assertEqual(parsed.request_digest, request.request_digest)
        self.assertNotIn("identity", request.to_wire()["body"])

        tampered = request.to_wire()
        tampered["body"]["max_concurrency"] = 2
        with self.assertRaisesRegex(
            RemoteProtocolError,
            "request_digest_mismatch",
        ):
            parse_request(tampered)

        extra = request.to_wire()
        extra["unexpected"] = True
        with self.assertRaisesRegex(RemoteProtocolError, "invalid_request"):
            parse_request(extra)

        too_large = request.to_wire()
        too_large["body"]["runtime_version"] = "x" * MAX_REMOTE_MESSAGE_BYTES
        with self.assertRaises(RemoteProtocolError):
            parse_request(too_large)

    def test_frozen_wire_models_do_not_expose_nested_mutable_aliases(self) -> None:
        request = make_request(
            RemoteOperation.REGISTER,
            request_id="immutable-request",
            worker_id="worker-1",
            instance_id="instance-1",
            body={
                "runtime_version": "runtime/1",
                "capabilities": ["activity.tool"],
                "resource_keys": ["workspace:project"],
                "activity_kinds": ["tool"],
                "max_concurrency": 1,
            },
        )
        capabilities = request.body["capabilities"]
        capabilities.append("activity.shell")
        self.assertEqual(
            request.body["capabilities"],
            ["activity.tool"],
        )
        self.assertEqual(
            parse_request(request.to_wire()).request_digest,
            request.request_digest,
        )
        direct_request_body = request.to_wire()["body"]
        direct_request = RemoteRequest(
            protocol=request.protocol,
            protocol_version=request.protocol_version,
            operation=request.operation,
            request_id=request.request_id,
            worker_id=request.worker_id,
            instance_id=request.instance_id,
            body=direct_request_body,
            request_digest=request.request_digest,
        )
        direct_request_body["capabilities"].append("activity.shell")
        self.assertEqual(
            direct_request.body["capabilities"],
            ["activity.tool"],
        )

        self._register(self.client)
        poll = make_request(
            RemoteOperation.POLL,
            request_id="immutable-response",
            worker_id="worker-1",
            instance_id="instance-1",
            body={"run_id": "run-remote", "lease_seconds": 30.0},
        )
        response = parse_response(
            self.control.handle(self.identity, poll.to_wire())
        )
        assignment = response.body["assignment"]
        original_token = assignment["claim"]["claim_token"]
        assignment["claim"]["claim_token"] = "tampered"
        self.assertEqual(
            response.body["assignment"]["claim"]["claim_token"],
            original_token,
        )
        self.assertEqual(
            parse_response(response.to_wire()).response_digest,
            response.response_digest,
        )
        direct_response_body = response.to_wire()["body"]
        direct_response = RemoteResponse(
            operation=response.operation,
            request_id=response.request_id,
            ok=response.ok,
            body=direct_response_body,
            response_digest=response.response_digest,
        )
        direct_response_body["assignment"]["claim"]["claim_token"] = "tampered"
        self.assertEqual(
            direct_response.body["assignment"]["claim"]["claim_token"],
            original_token,
        )

        work = WorkAssignment.from_wire(response.body["assignment"])
        handles = tuple(grant.output_handle for grant in work.output_grants)
        proof = _runtime_proof(
            work,
            outcome="succeeded",
            output_handles=handles,
        )
        proof_digest = proof.proof_digest
        refs = proof.sandbox_receipt["output_artifact_refs"]
        refs[0]["artifact_id"] = "tampered"
        self.assertNotEqual(
            proof.sandbox_receipt["output_artifact_refs"][0]["artifact_id"],
            "tampered",
        )
        self.assertEqual(proof.proof_digest, proof_digest)

    def test_preflight_denial_creates_no_attempt_or_event(self) -> None:
        control = RemoteControlPlane(
            lambda run_id: self.scheduler,
            authorize_run=lambda identity, run_id: True,
            assignment_admitter=_DenyPreflightAdmitter(self.artifacts),
            journal=RemoteControlJournal(self.journal_path),
        )
        client = self._client(control)
        self._register(client)
        before_attempts = self.store.list_attempts("run-remote")
        before_events = self.store.list_events("run-remote")

        with self.assertRaisesRegex(
            RemoteWorkerError,
            "authorization_conflict",
        ):
            client.poll("run-remote")

        self.assertEqual(
            self.store.list_attempts("run-remote"),
            before_attempts,
        )
        self.assertEqual(
            self.store.list_events("run-remote"),
            before_events,
        )

    def test_duplicate_poll_is_idempotent_and_conflicting_request_id_fails(self) -> None:
        self._register(self.client)
        request = make_request(
            RemoteOperation.POLL,
            request_id="poll-fixed",
            worker_id="worker-1",
            instance_id="instance-1",
            body={"run_id": "run-remote", "lease_seconds": 30.0},
        )

        first = self.control.handle(self.identity, request.to_wire())
        second = self.control.handle(self.identity, request.to_wire())

        self.assertEqual(first, second)
        self.assertIsNotNone(parse_response(first).body["assignment"])
        self.assertEqual(len(self.store.list_attempts("run-remote")), 1)

        conflict = make_request(
            RemoteOperation.POLL,
            request_id="poll-fixed",
            worker_id="worker-1",
            instance_id="instance-1",
            body={"run_id": "run-remote", "lease_seconds": 31.0},
        )
        rejected = parse_response(
            self.control.handle(self.identity, conflict.to_wire())
        )
        self.assertFalse(rejected.ok)
        self.assertEqual(rejected.body, {"error_code": "request_id_conflict"})

    def test_concurrent_duplicate_poll_claims_exactly_one_attempt(self) -> None:
        self._register(self.client)
        request = make_request(
            RemoteOperation.POLL,
            request_id="poll-concurrent",
            worker_id="worker-1",
            instance_id="instance-1",
            body={"run_id": "run-remote", "lease_seconds": 30.0},
        ).to_wire()

        with ThreadPoolExecutor(max_workers=16) as pool:
            responses = list(
                pool.map(
                    lambda _index: self.control.handle(self.identity, request),
                    range(32),
                )
            )

        self.assertTrue(all(response == responses[0] for response in responses))
        self.assertIsNotNone(parse_response(responses[0]).body["assignment"])
        self.assertEqual(len(self.store.list_attempts("run-remote")), 1)

    def test_control_restart_restores_claim_but_stale_and_terminal_fail_closed(self) -> None:
        self._register(self.client)
        assignment = self.client.poll("run-remote", lease_seconds=30.0)
        assert assignment is not None

        restarted = self._control()
        restarted_client = self._client(restarted)
        self._register(restarted_client)
        restarted_client.start(assignment.claim)

        restored = self.scheduler.restore_claim(
            assignment.claim.run_id,
            assignment.claim.node_id,
            assignment.claim.attempt_id,
            restarted.get_registration("worker-1").session_owner_id,
            request_hash=assignment.claim.activity_request_digest,
            claim_token=assignment.claim.claim_token,
            fencing_token=assignment.claim.fencing_token,
        )
        self.assertEqual(restored.attempt_id, assignment.claim.attempt_id)

        with self.assertRaises(SchedulerStateError):
            self.scheduler.restore_claim(
                assignment.claim.run_id,
                assignment.claim.node_id,
                assignment.claim.attempt_id,
                "foreign-worker",
                request_hash=assignment.claim.activity_request_digest,
                claim_token=assignment.claim.claim_token,
                fencing_token=assignment.claim.fencing_token,
            )
        with self.assertRaises(SchedulerStateError):
            self.scheduler.restore_claim(
                assignment.claim.run_id,
                assignment.claim.node_id,
                assignment.claim.attempt_id,
                restarted.get_registration("worker-1").session_owner_id,
                request_hash=assignment.claim.activity_request_digest,
                claim_token=assignment.claim.claim_token,
                fencing_token=assignment.claim.fencing_token + 1,
            )

        restarted_client.complete(
            assignment.claim,
            self._success_outcome(assignment),
        )
        self.assertEqual(
            self.store.get_attempt(assignment.claim.attempt_id).status,
            AttemptStatus.SUCCEEDED,
        )
        with self.assertRaises(SchedulerStateError):
            self.scheduler.restore_claim(
                assignment.claim.run_id,
                assignment.claim.node_id,
                assignment.claim.attempt_id,
                restarted.get_registration("worker-1").session_owner_id,
                request_hash=assignment.claim.activity_request_digest,
                claim_token=assignment.claim.claim_token,
                fencing_token=assignment.claim.fencing_token,
            )

    def test_r06_new_instance_cannot_take_over_existing_session_claim(self) -> None:
        self._register(self.client)
        assignment = self.client.poll("run-remote", lease_seconds=30.0)
        assert assignment is not None
        attempt = self.store.get_attempt(assignment.claim.attempt_id)
        self.assertNotEqual(attempt.worker_id, self.identity.worker_id)
        self.assertEqual(assignment.worker_id, self.identity.worker_id)

        replacement = self._client(self.control, instance_id="instance-2")
        self._register(replacement)
        with self.assertRaisesRegex(RemoteWorkerError, "claim_conflict"):
            replacement.start(assignment.claim)
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "worker_identity_mismatch",
        ):
            self.client.start(assignment.claim)

    def test_inflight_poll_reregister_cannot_rebind_claim_owner(self) -> None:
        blocker = _BlockingAdmitter(self.artifacts)
        control = RemoteControlPlane(
            lambda _run_id: self.scheduler,
            authorize_run=lambda _identity, _run_id: True,
            assignment_admitter=blocker,
            journal=RemoteControlJournal(self.journal_path),
        )
        original = self._client(control)
        self._register(original)
        before_attempts = self.store.list_attempts("run-remote")
        before_events = self.store.list_events("run-remote")
        with ThreadPoolExecutor(max_workers=2) as pool:
            pending = pool.submit(
                original.poll,
                "run-remote",
                lease_seconds=30.0,
            )
            self.assertTrue(blocker.entered.wait(2.0))
            replacement = self._client(control, instance_id="instance-2")
            self._register(replacement)
            blocker.release.set()
            with self.assertRaisesRegex(
                RemoteWorkerError,
                "worker_identity_mismatch",
            ):
                pending.result(timeout=3.0)
        self.assertEqual(
            before_attempts,
            self.store.list_attempts("run-remote"),
        )
        self.assertEqual(
            before_events,
            self.store.list_events("run-remote"),
        )

    def test_expired_claim_cannot_be_restored_or_mutated(self) -> None:
        self._register(self.client)
        assignment = self.client.poll("run-remote", lease_seconds=5.0)
        assert assignment is not None
        self.clock.now += 5.0

        with self.assertRaises(SchedulerStateError):
            self.scheduler.restore_claim(
                assignment.claim.run_id,
                assignment.claim.node_id,
                assignment.claim.attempt_id,
                self.control.get_registration("worker-1").session_owner_id,
                request_hash=assignment.claim.activity_request_digest,
                claim_token=assignment.claim.claim_token,
                fencing_token=assignment.claim.fencing_token,
            )
        with self.assertRaisesRegex(RemoteWorkerError, "claim_conflict"):
            self.client.start(assignment.claim)

    def test_foreign_worker_and_stale_fencing_are_publicly_rejected(self) -> None:
        self._register(self.client)
        assignment = self.client.poll("run-remote", lease_seconds=30.0)
        assert assignment is not None
        foreign_identity = AuthenticatedWorker(
            worker_id="worker-2",
            tenant_id="tenant-1",
            identity_digest=hashlib.sha256(b"worker-2-identity").hexdigest(),
        )
        foreign = self._client(
            self.control,
            identity=foreign_identity,
            worker_id="worker-2",
            instance_id="instance-2",
        )
        self._register(foreign)

        with self.assertRaisesRegex(RemoteWorkerError, "claim_conflict"):
            foreign.start(assignment.claim)
        stale = ClaimBinding(
            run_id=assignment.claim.run_id,
            node_id=assignment.claim.node_id,
            attempt_id=assignment.claim.attempt_id,
            activity_request_digest=assignment.claim.activity_request_digest,
            action_digest=assignment.claim.action_digest,
            authorization_digest=assignment.claim.authorization_digest,
            profile_digest=assignment.claim.profile_digest,
            request_digest=assignment.claim.request_digest,
            session_binding_digest=assignment.claim.session_binding_digest,
            grant_binding_digest=assignment.claim.grant_binding_digest,
            execution_plan_digest=assignment.claim.execution_plan_digest,
            runtime_attestation_digest=(
                assignment.claim.runtime_attestation_digest
            ),
            claim_token=assignment.claim.claim_token,
            fencing_token=assignment.claim.fencing_token + 1,
        )
        with self.assertRaisesRegex(RemoteWorkerError, "claim_conflict"):
            self.client.start(stale)

    def test_heartbeat_start_complete_and_receipt_only_wire_contract(self) -> None:
        self._register(self.client)
        assignment = self.client.poll("run-remote", lease_seconds=20.0)
        assert assignment is not None
        self.assertFalse(hasattr(assignment, "store"))
        self.assertFalse(hasattr(assignment, "scheduler"))
        self.assertNotIn(str(self.store.path), json.dumps(assignment.to_wire()))

        self.client.start(assignment.claim)
        heartbeat = self.client.heartbeat(
            assignment.claim,
            lease_seconds=40.0,
        )
        self.assertGreater(heartbeat["lease_expires_at"], 120.0)
        result_canary = "raw-result-must-stay-in-artifact"
        self.client.complete(
            assignment.claim,
            self._success_outcome(assignment),
        )

        self.assertEqual(self.store.get_run("run-remote").status, RunStatus.COMPLETED)
        durable = json.dumps(
            [event.to_dict() for event in self.store.list_events("run-remote")],
            ensure_ascii=False,
            sort_keys=True,
        )
        self.assertNotIn(result_canary, durable)
        self.assertNotIn(assignment.claim.claim_token, repr(assignment))

    def test_assignment_grants_plan_and_runtime_proof_are_strict_and_path_free(
        self,
    ) -> None:
        self._register(self.client)
        assignment = self.client.poll("run-remote", lease_seconds=30.0)
        assert assignment is not None
        wire = assignment.to_wire()
        serialized = json.dumps(wire, sort_keys=True)
        self.assertNotIn('"artifact_refs"', serialized)
        self.assertNotIn('"uri"', serialized)
        self.assertNotIn('"encryption_key_ref"', serialized)
        self.assertNotIn('"config"', serialized)
        self.assertEqual(wire["worker_id"], "worker-1")
        self.assertEqual(
            set(wire["output_grants"][0]["handle"]),
            {"schema_version", "grant_id", "token"},
        )

        injected_path = json.loads(serialized)
        injected_path["output_grants"][0]["uri"] = "/control/store/secret"
        with self.assertRaisesRegex(RemoteProtocolError, "invalid_output_grant"):
            WorkAssignment.from_wire(injected_path)

        handles = tuple(grant.output_handle for grant in assignment.output_grants)
        proof = _runtime_proof(
            assignment,
            outcome="succeeded",
            output_handles=handles,
        )
        malformed = proof.to_wire()
        malformed["sandbox_receipt"]["host_path"] = "/control/store/secret"
        with self.assertRaisesRegex(RemoteProtocolError, "invalid_sandbox_receipt"):
            RemoteRuntimeProof.from_wire(malformed)

        invalid_signature = proof.to_wire()
        invalid_signature["signature"] = "not=base64url"
        with self.assertRaisesRegex(RemoteProtocolError, "invalid_runtime_signature"):
            RemoteRuntimeProof.from_wire(invalid_signature)

        rebound_claim = replace(
            assignment.claim,
            session_binding_digest="f" * 64,
        )
        with self.assertRaisesRegex(
            RemoteProtocolError,
            "runtime_proof_binding_mismatch",
        ):
            proof.validate_binding(
                rebound_claim,
                outcome="succeeded",
                output_handles=handles,
            )

    def test_cancellation_poll_and_ack_close_running_attempt(self) -> None:
        self._register(self.client)
        assignment = self.client.poll("run-remote", lease_seconds=30.0)
        assert assignment is not None
        self.client.start(assignment.claim)

        self.control.request_cancel(self.identity, "run-remote")

        self.assertTrue(self.client.cancellation_requested(assignment.claim))
        self.client.acknowledge_cancel(
            assignment.claim,
            _runtime_proof(
                assignment,
                outcome="cancelled",
            ),
        )
        self.assertEqual(
            self.store.get_attempt(assignment.claim.attempt_id).status,
            AttemptStatus.CANCELLED,
        )
        self.assertEqual(self.store.get_run("run-remote").status, RunStatus.CANCELLED)

    def test_worker_daemon_executes_without_store_access(self) -> None:
        seen: list[WorkAssignment] = []

        def execute(assignment, context):
            seen.append(assignment)
            self.assertFalse(hasattr(context.client, "store"))
            self.assertFalse(context.cancellation_requested())
            return self._success_outcome(assignment)

        daemon = RemoteWorkerDaemon(
            self.client,
            _TestExecutionAdapter(execute),
            runtime_version="worker-runtime/1.0",
            capabilities=("activity.tool", "artifact.refs"),
            resource_keys=("workspace:project",),
            activity_kinds=("tool",),
            lease_seconds=30.0,
        )

        self.assertTrue(daemon.execute_one("run-remote"))
        self.assertEqual(len(seen), 1)
        self.assertEqual(self.store.get_run("run-remote").status, RunStatus.COMPLETED)

    def test_security_adapters_are_required_and_arbitrary_callable_is_rejected(self) -> None:
        memory_journal_control = RemoteControlPlane(
            lambda _run_id: self.scheduler,
            authorize_run=lambda _identity, _run_id: True,
            assignment_admitter=self.admitter,
        )
        memory_client = self._client(
            memory_journal_control,
            instance_id="memory-journal-instance",
        )
        self._register(memory_client)
        with self.assertRaisesRegex(RemoteWorkerError, "security_not_ready"):
            memory_client.poll("run-remote")
        self.assertEqual(self.store.list_attempts("run-remote"), [])

        unguarded_control = RemoteControlPlane(
            lambda _run_id: self.scheduler,
            authorize_run=lambda _identity, _run_id: True,
            journal=RemoteControlJournal(self.journal_path),
        )
        unguarded_client = self._client(unguarded_control)
        self._register(unguarded_client)
        with self.assertRaisesRegex(RemoteWorkerError, "security_not_ready"):
            unguarded_client.poll("run-remote")
        self.assertEqual(self.store.list_attempts("run-remote"), [])

        legacy_control = RemoteControlPlane(
            lambda _run_id: self.scheduler,
            authorize_run=lambda _identity, _run_id: True,
            assignment_admitter=_UnmarkedLegacyAdmitter(self.artifacts),
            journal=RemoteControlJournal(
                self.control_root / "legacy-disabled.sqlite3"
            ),
        )
        legacy_client = self._client(
            legacy_control,
            instance_id="legacy-disabled-instance",
        )
        self._register(legacy_client)
        with self.assertRaisesRegex(RemoteWorkerError, "security_not_ready"):
            legacy_client.poll("run-remote")
        self.assertEqual(self.store.list_attempts("run-remote"), [])

        marked_default = RemoteControlPlane(
            lambda _run_id: self.scheduler,
            authorize_run=lambda _identity, _run_id: True,
            assignment_admitter=_MarkedLegacyAdmitter(self.artifacts),
            journal=RemoteControlJournal(
                self.control_root / "legacy-marked-default.sqlite3"
            ),
        )
        marked_default_client = self._client(
            marked_default,
            instance_id="legacy-marked-default-instance",
        )
        self._register(marked_default_client)
        with self.assertRaisesRegex(RemoteWorkerError, "security_not_ready"):
            marked_default_client.poll("run-remote")
        self.assertEqual(self.store.list_attempts("run-remote"), [])

        unmarked_enabled = RemoteControlPlane(
            lambda _run_id: self.scheduler,
            authorize_run=lambda _identity, _run_id: True,
            assignment_admitter=_UnmarkedLegacyAdmitter(self.artifacts),
            allow_reference_admission=True,
            journal=RemoteControlJournal(
                self.control_root / "legacy-unmarked-enabled.sqlite3"
            ),
        )
        unmarked_enabled_client = self._client(
            unmarked_enabled,
            instance_id="legacy-unmarked-enabled-instance",
        )
        self._register(unmarked_enabled_client)
        with self.assertRaisesRegex(RemoteWorkerError, "security_not_ready"):
            unmarked_enabled_client.poll("run-remote")
        self.assertEqual(self.store.list_attempts("run-remote"), [])

        marked_enabled = RemoteControlPlane(
            lambda _run_id: self.scheduler,
            authorize_run=lambda _identity, _run_id: True,
            assignment_admitter=_MarkedLegacyAdmitter(self.artifacts),
            allow_reference_admission=True,
            journal=RemoteControlJournal(
                self.control_root / "legacy-marked-enabled.sqlite3"
            ),
        )
        marked_enabled_client = self._client(
            marked_enabled,
            instance_id="legacy-marked-enabled-instance",
        )
        self._register(marked_enabled_client)
        self.assertIsNotNone(marked_enabled_client.poll("run-remote"))
        self.assertEqual(len(self.store.list_attempts("run-remote")), 1)

        before_daemon = self.store.list_attempts("run-remote")
        daemon = RemoteWorkerDaemon(
            self.client,
            None,
            runtime_version="worker-runtime/1.0",
            capabilities=("activity.tool",),
            resource_keys=("workspace:project",),
            activity_kinds=("tool",),
        )
        with self.assertRaisesRegex(RemoteWorkerError, "security_not_ready"):
            daemon.execute_one("run-remote")
        self.assertEqual(
            self.store.list_attempts("run-remote"),
            before_daemon,
        )
        with self.assertRaises(TypeError):
            RemoteWorkerDaemon(
                self.client,
                lambda _assignment, _context: None,
                runtime_version="worker-runtime/1.0",
                capabilities=("activity.tool",),
                resource_keys=("workspace:project",),
            )

    def test_control_never_commits_unverified_worker_completion_candidate(self) -> None:
        control = RemoteControlPlane(
            lambda _run_id: self.scheduler,
            authorize_run=lambda _identity, _run_id: True,
            assignment_admitter=_NoCommitAdmitter(self.artifacts),
            journal=RemoteControlJournal(self.journal_path),
        )
        client = self._client(control)
        self._register(client)
        assignment = client.poll("run-remote", lease_seconds=30.0)
        assert assignment is not None
        client.start(assignment.claim)
        with self.assertRaisesRegex(RemoteWorkerError, "authorization_conflict"):
            client.complete(
                assignment.claim,
                self._success_outcome(assignment),
            )
        self.assertEqual(
            self.store.get_attempt(assignment.claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )

    def test_executor_exception_is_reduced_to_safe_error_codes(self) -> None:
        secret = "exception-secret-that-must-not-persist"

        def explode(_assignment, _context):
            raise RuntimeError(secret)

        daemon = RemoteWorkerDaemon(
            self.client,
            _TestExecutionAdapter(explode),
            runtime_version="worker-runtime/1.0",
            capabilities=("activity.tool",),
            resource_keys=("workspace:project",),
            activity_kinds=("tool",),
            lease_seconds=30.0,
        )
        with self.assertRaisesRegex(RemoteWorkerError, "runtime_proof_unavailable"):
            daemon.execute_one("run-remote")
        attempt = self.store.list_attempts("run-remote")[0]
        self.assertEqual(attempt.status, AttemptStatus.RUNNING)
        serialized = json.dumps(
            {
                "attempt": attempt.to_dict(),
                "events": [
                    event.to_dict()
                    for event in self.store.list_events("run-remote")
                ],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        self.assertNotIn(secret, serialized)
        self.assertNotIn("worker_execution_error", serialized)

    def test_sensitive_workflow_config_is_rejected_before_claim(self) -> None:
        workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": "secret-config",
                "version": 1,
                "nodes": [
                    {
                        "id": "tool",
                        "kind": "tool",
                        "config": {
                            "tool": "inspect",
                            "arguments": {"api_token": "never-on-wire"},
                        },
                        "effect_class": "read_only",
                    }
                ],
            }
        )
        store = DurableRunStore(self.control_root / "secret.sqlite3")
        scheduler = DurableScheduler(store, workflow, clock=self.clock)
        scheduler.create_run("run-secret")
        scheduler.reconcile("run-secret")
        control = RemoteControlPlane(
            lambda _run_id: scheduler,
            authorize_run=lambda _identity, _run_id: True,
            assignment_admitter=self.admitter,
            journal=RemoteControlJournal(
                self.control_root / "secret-remote-control.sqlite3"
            ),
        )
        client = self._client(control)
        self._register(client)

        with self.assertRaisesRegex(RemoteWorkerError, "invalid_assignment"):
            client.poll("run-secret")
        self.assertEqual(store.list_attempts("run-secret"), [])

    def test_wrong_tenant_and_unsupported_capability_fail_closed(self) -> None:
        wrong_tenant = AuthenticatedWorker(
            worker_id="worker-denied",
            tenant_id="tenant-denied",
            identity_digest=hashlib.sha256(b"denied").hexdigest(),
        )
        denied = self._client(
            self.control,
            identity=wrong_tenant,
            worker_id="worker-denied",
            instance_id="denied-instance",
        )
        self._register(denied)
        with self.assertRaisesRegex(RemoteWorkerError, "forbidden"):
            denied.poll("run-remote")

        unsupported = self._client(
            self.control,
            worker_id="worker-1",
            instance_id="instance-unsupported",
        )
        unsupported.register(
            runtime_version="runtime/1",
            capabilities=("artifact.refs",),
            resource_keys=("workspace:project",),
            activity_kinds=("tool",),
            max_concurrency=1,
        )
        with self.assertRaisesRegex(RemoteWorkerError, "unsupported_activity"):
            unsupported.poll("run-remote")

    def test_registration_cache_has_capacity_and_idle_ttl(self) -> None:
        session_clock = _Clock(10.0)
        control = RemoteControlPlane(
            lambda _run_id: self.scheduler,
            authorize_run=lambda identity, _run_id: identity.tenant_id == "tenant-1",
            max_registrations=1,
            registration_idle_seconds=5.0,
            clock=session_clock,
            assignment_admitter=self.admitter,
            journal=RemoteControlJournal(
                self.control_root / "ttl-remote-control.sqlite3"
            ),
        )
        first = self._client(control)
        self._register(first)

        second_identity = AuthenticatedWorker(
            worker_id="worker-2",
            tenant_id="tenant-1",
            identity_digest=hashlib.sha256(b"worker-2").hexdigest(),
        )
        second = self._client(
            control,
            identity=second_identity,
            worker_id="worker-2",
            instance_id="instance-2",
        )
        with self.assertRaisesRegex(RemoteWorkerError, "registration_capacity"):
            self._register(second)
        self.assertIsNotNone(control.get_registration("worker-1"))

        session_clock.now += 5.0
        self._register(second)
        self.assertIsNone(control.get_registration("worker-1"))
        self.assertIsNotNone(control.get_registration("worker-2"))

        with self.assertRaisesRegex(RemoteWorkerError, "not_registered"):
            first.poll("run-remote")


if __name__ == "__main__":
    unittest.main()
