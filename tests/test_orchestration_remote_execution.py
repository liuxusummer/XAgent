from __future__ import annotations

import base64
import hashlib
import hmac
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path

from src.orchestration.artifacts import ArtifactKind, LocalArtifactStore
from src.orchestration.executor import TrustedActivityExecutor
from src.orchestration.models import AttemptStatus
from src.orchestration.policy import (
    EffectClass,
    PolicyEngine,
    PolicyOutcome,
    PolicyRule,
    ToolPolicy,
    ToolTimeoutBehavior,
)
from src.orchestration.remote_control import (
    RemoteCompletionCandidate,
    RemoteControlPlane,
    WorkerRegistration,
)
from src.orchestration.remote_journal import RemoteControlJournal
from src.orchestration.remote_execution import (
    RemoteExecutionPreparation,
    RemoteOutputPayload,
    RemoteSandboxExecution,
    SecureRemoteExecutionAdapter,
    SecureRemoteAssignmentAdmitter,
    SecureRemoteExecutionError,
    VerifiedRemoteRuntimeAttestation,
)
from src.orchestration.remote_protocol import (
    AuthenticatedWorker,
    ClaimBinding,
    ExecutionAuthorization,
    RemoteActivityDescriptor,
    RemoteRuntimeProof,
    WorkAssignment,
    canonical_digest,
    runtime_binding_digest,
)
from src.orchestration.remote_worker import RemoteExecutionContext
from src.orchestration.remote_worker import (
    RemoteExecutionOutcome,
    RemoteWorkerClient,
)
from src.orchestration.sandbox import (
    SandboxDispatcher,
    SandboxOutcome,
    SandboxProfile,
    SecurityLevel,
)
from src.orchestration.scheduler import DurableScheduler
from src.orchestration.store import DurableRunStore
from src.orchestration.worker_security import (
    WorkerAccessRule,
    WorkerAuthorizationGate,
    WorkerIdentity,
)
from src.orchestration.workflow import compile_workflow
from src.orchestration.artifact_broker import (
    ArtifactGrantBroker,
    ArtifactOutputHandle,
)


class _Clock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class _UnusedBackend:
    backend_id = "unused-container"
    security_level = SecurityLevel.CONTAINER
    capabilities = ()
    supports_materialized_script = True

    def execute(self, request, profile):
        raise AssertionError("remote admission must not execute a local backend")


class _PlanResolver:
    def __init__(self, preparation: RemoteExecutionPreparation) -> None:
        self.preparation = preparation

    def resolve(self, scheduler, claim):
        del scheduler, claim
        return self.preparation


class _FailingPlanResolver:
    def resolve(self, scheduler, claim):
        del scheduler, claim
        raise RuntimeError("untrusted resolver diagnostic")


class _PresentationResolver:
    def resolve(self, identity, registration):
        return {
            "worker_id": identity.worker_id,
            "session_binding_digest": registration.session_binding_digest,
        }


class _PreflightAuthorizer:
    production_security_ready = True

    def authorize(
        self,
        identity,
        registration,
        scheduler,
        *,
        pool_id,
        presentation,
        now,
    ):
        del scheduler, now
        return (
            identity.worker_id == registration.worker_id
            and pool_id == "pool-a"
            and presentation["worker_id"] == identity.worker_id
        )


class _Attestor:
    def __init__(self, clock: _Clock) -> None:
        self.clock = clock

    def attest(self, presentation, *, now):
        assert now == self.clock.value
        return WorkerIdentity(
            worker_id=presentation["worker_id"],
            subject="spiffe://example.test/worker",
            issuer="test-trust-domain",
            tenant_id="tenant-a",
            pool_id="pool-a",
            capabilities=(),
            transport_binding_digest=(
                presentation["session_binding_digest"]
            ),
            issued_at=now,
            not_before=now,
            expires_at=now + 120,
            attestation_id="test-workload-attestation",
        )


class _RuntimeVerifier:
    """Deterministic test trust anchor; it is not a deployment verifier."""

    production_security_ready = True

    def __init__(self, clock: _Clock) -> None:
        self.clock = clock
        self.key = b"unit-test-runtime-proof-key"
        self.attestation_digest = hashlib.sha256(
            b"unit-test-runtime-attestation"
        ).hexdigest()

    def attest(self, identity, registration, *, now):
        return VerifiedRemoteRuntimeAttestation(
            worker_id=identity.worker_id,
            instance_id=registration.instance_id,
            identity_digest=identity.identity_digest,
            runtime_version=registration.runtime_version,
            runtime_attestation_digest=self.attestation_digest,
            security_level=SecurityLevel.CONTAINER,
            verifier_id="unit-test-runtime-verifier",
            issued_at=now,
            expires_at=now + 120,
        )

    def verify(
        self,
        proof,
        *,
        identity,
        registration,
        claim,
        authorization,
        attestation,
        now,
    ):
        del identity, registration, claim, authorization
        if now >= attestation.expires_at:
            return False
        expected = base64.urlsafe_b64encode(
            hmac.new(
                self.key,
                proof.signed_binding_digest.encode("ascii"),
                hashlib.sha256,
            ).digest()
        ).rstrip(b"=").decode("ascii")
        return (
            proof.verifier_key_id == "unit-test-key"
            and hmac.compare_digest(proof.signature, expected)
        )

    def make_proof(
        self,
        claim: ClaimBinding,
        receipt: dict,
        *,
        output_handles,
        outcome: str,
    ) -> RemoteRuntimeProof:
        spec_digest = hashlib.sha256(b"unit-test-oci-spec").hexdigest()
        binding = runtime_binding_digest(
            claim,
            outcome=outcome,
            output_handles=tuple(output_handles),
            sandbox_receipt_digest=canonical_digest(receipt),
            sandbox_spec_digest=spec_digest,
        )
        signature = base64.urlsafe_b64encode(
            hmac.new(
                self.key,
                binding.encode("ascii"),
                hashlib.sha256,
            ).digest()
        ).rstrip(b"=").decode("ascii")
        return RemoteRuntimeProof(
            proof_id="unit-test-proof",
            verifier_key_id="unit-test-key",
            signed_binding_digest=binding,
            signature=signature,
            sandbox_spec_digest=spec_digest,
            sandbox_receipt=receipt,
        )


class _WorkerArtifactTransport:
    production_security_ready = True

    def __init__(
        self,
        admitter,
        identity,
        registration,
        scheduler,
        claim,
        authorization,
    ) -> None:
        self.admitter = admitter
        self.identity = identity
        self.registration = registration
        self.scheduler = scheduler
        self.claim = claim
        self.authorization = authorization

    def fetch(self, grant):
        raise AssertionError("this fixture has no input grants")

    def stage(self, grant, output):
        self.assert_grant = grant
        return self.admitter.stage_output(
            self.identity,
            self.registration,
            self.scheduler,
            self.claim,
            self.authorization,
            content=output.content,
            kind=output.kind,
            sensitivity=output.sensitivity,
            media_type=output.media_type,
        )


class _WorkerSandboxAdapter:
    production_security_ready = True
    security_level = SecurityLevel.CONTAINER

    def __init__(self, attestation_digest: str) -> None:
        self.runtime_attestation_digest = attestation_digest

    def execute(self, grant, input_payloads, context):
        del grant, context
        assert input_payloads == ()
        return RemoteSandboxExecution(
            backend_id="attested-gvisor",
            sandbox_spec_digest=hashlib.sha256(
                b"unit-test-oci-spec"
            ).hexdigest(),
            outcome=SandboxOutcome.SUCCEEDED,
            exit_code=0,
            timed_out=False,
            outputs=(
                RemoteOutputPayload(
                    b'{"worker":"ok"}',
                    media_type="application/json",
                ),
            ),
            runtime_evidence={"test_only": True},
        )

    def cancel(self, grant, context):
        del grant, context
        return RemoteSandboxExecution(
            backend_id="attested-gvisor",
            sandbox_spec_digest=hashlib.sha256(
                b"unit-test-oci-spec"
            ).hexdigest(),
            outcome=SandboxOutcome.CANCELLED,
            exit_code=None,
            timed_out=False,
            error_code="cancelled",
            runtime_evidence={"test_only": True},
        )


class _WorkerProofSigner:
    """Test-only signer standing in for a deployment runtime trust service."""

    production_security_ready = True

    def __init__(self, verifier: _RuntimeVerifier) -> None:
        self.runtime_attestation_digest = verifier.attestation_digest
        self.verifier = verifier

    def sign(
        self,
        *,
        signed_binding_digest,
        sandbox_spec_digest,
        sandbox_receipt,
        runtime_evidence,
    ):
        assert runtime_evidence == {"test_only": True}
        signature = base64.urlsafe_b64encode(
            hmac.new(
                self.verifier.key,
                signed_binding_digest.encode("ascii"),
                hashlib.sha256,
            ).digest()
        ).rstrip(b"=").decode("ascii")
        return RemoteRuntimeProof(
            proof_id="unit-test-worker-proof",
            verifier_key_id="unit-test-key",
            signed_binding_digest=signed_binding_digest,
            signature=signature,
            sandbox_spec_digest=sandbox_spec_digest,
            sandbox_receipt=sandbox_receipt,
        )


class SecureRemoteExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.control_root = root / "control"
        self.agent_root = root / "agent"
        self.control_root.mkdir()
        self.agent_root.mkdir()
        self.clock = _Clock()
        self.artifacts = LocalArtifactStore(self.control_root / "artifacts")
        workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": "remote-secure",
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
                        "resource_keys": ["workspace:/project"],
                    }
                ],
            }
        )
        self.store = DurableRunStore(self.control_root / "runs.sqlite3")
        self.scheduler = DurableScheduler(
            self.store,
            workflow,
            clock=self.clock,
            artifact_verifier=self.artifacts.verify,
        )
        self.tool_policy = ToolPolicy(
            "exec",
            EffectClass.READ_ONLY,
            supports_idempotency_key=False,
            supports_status_probe=False,
            supports_compensation=False,
            timeout_behavior=ToolTimeoutBehavior.SAFE_TO_RETRY,
            allowed_resource_keys=("workspace:/project",),
        )
        policy = PolicyEngine((self.tool_policy,))
        sandbox = SandboxDispatcher(
            (_UnusedBackend(),),
            policy_version=policy.policy_version,
        )
        self.profile = SandboxProfile(
            "remote-container",
            (self.agent_root,),
            (),
            minimum_security_level=SecurityLevel.CONTAINER,
        )
        self.executor = TrustedActivityExecutor(
            self.scheduler,
            policy,
            sandbox,
            artifact_verifier=self.artifacts.verify,
            artifact_reader=self.artifacts.read,
            clock=self.clock,
        )
        self.identity = AuthenticatedWorker(
            "worker-a",
            "tenant-a",
            hashlib.sha256(b"trusted-transport").hexdigest(),
        )
        session_binding = hashlib.sha256(
            b"worker-a-instance-a-session"
        ).hexdigest()
        self.registration = WorkerRegistration(
            worker_id="worker-a",
            tenant_id="tenant-a",
            identity_digest=self.identity.identity_digest,
            instance_id="instance-a",
            runtime_version="runsc-1.2",
            capabilities=("activity.tool",),
            resource_keys=("workspace:/project",),
            activity_kinds=("tool",),
            max_concurrency=1,
            session_epoch=1,
            session_owner_id=f"remote-session:{session_binding}",
            session_binding_digest=session_binding,
        )
        self.worker_gate = WorkerAuthorizationGate(
            _Attestor(self.clock),
            (
                WorkerAccessRule(
                    "exec-read",
                    "tenant-a",
                    "pool-a",
                    ("exec",),
                    (),
                    (EffectClass.READ_ONLY,),
                ),
            ),
            clock=self.clock,
        )
        self.broker = ArtifactGrantBroker(
            self.artifacts,
            authorization_verifier=self.worker_gate,
            clock=self.clock,
        )
        self.runtime_verifier = _RuntimeVerifier(self.clock)
        self.preparation = RemoteExecutionPreparation(
            argv=("trusted-runner",),
            cwd=str(self.agent_root),
            container_cwd="/workspace",
            profile=self.profile,
            resource_locks=("workspace:/project",),
        )

    def _admitter(self, **overrides):
        values = {
            "executor": self.executor,
            "worker_authorization_gate": self.worker_gate,
            "artifact_broker": self.broker,
            "plan_resolver": _PlanResolver(self.preparation),
            "presentation_resolver": _PresentationResolver(),
            "runtime_proof_verifier": self.runtime_verifier,
            "preflight_authorizer": _PreflightAuthorizer(),
            "pool_resolver": lambda registration: "pool-a",
            "clock": self.clock,
        }
        values.update(overrides)
        return SecureRemoteAssignmentAdmitter(**values)

    def _claim(self, run_id: str = "run"):
        self.scheduler.create_run(run_id)
        claim = self.scheduler.claim_next(
            run_id,
            self.registration.session_owner_id,
            resource_keys=self.registration.resource_keys,
        )
        assert claim is not None
        return claim

    @staticmethod
    def _binding(claim, authorization):
        return ClaimBinding(
            run_id=claim.run_id,
            node_id=claim.node_id,
            attempt_id=claim.attempt_id,
            activity_request_digest=claim.request_hash,
            action_digest=authorization.action_digest,
            authorization_digest=authorization.authorization_digest,
            profile_digest=authorization.profile_digest,
            request_digest=authorization.request_digest,
            session_binding_digest=authorization.session_binding_digest,
            grant_binding_digest=authorization.grant_binding_digest,
            execution_plan_digest=authorization.execution_plan_digest,
            runtime_attestation_digest=(
                authorization.runtime_attestation_digest
            ),
            claim_token=claim.claim_token,
            fencing_token=claim.fencing_token,
        )

    def _success_candidate(
        self,
        admitter,
        claim,
        authorization,
        *,
        registration=None,
    ):
        active_registration = registration or self.registration
        staged = admitter.stage_output(
            self.identity,
            active_registration,
            self.scheduler,
            claim,
            authorization,
            content=b'{"ok":true}',
            media_type="application/json",
        )
        receipt = {
            "schema_version": 2,
            "backend_id": "attested-gvisor",
            "security_level": "container",
            "profile_id": self.profile.profile_id,
            "profile_digest": authorization.profile_digest,
            "action_digest": authorization.action_digest,
            "policy_version": self.executor.policy.policy_version,
            "request_digest": authorization.request_digest,
            "outcome": SandboxOutcome.SUCCEEDED.value,
            "exit_code": 0,
            "timed_out": False,
            "output_artifact_refs": [
                {
                    "artifact_id": staged.descriptor.artifact_id,
                    "sha256": staged.descriptor.sha256,
                    "size": staged.descriptor.size,
                    "kind": staged.descriptor.kind,
                }
            ],
            "error_code": None,
        }
        proof = self.runtime_verifier.make_proof(
            self._binding(claim, authorization),
            receipt,
            output_handles=(staged.handle,),
            outcome="succeeded",
        )
        return RemoteCompletionCandidate(
            outcome=AttemptStatus.SUCCEEDED,
            output_handles=(staged.handle,),
            runtime_proof=proof,
            error_class=None,
            error_code=None,
        )

    def test_success_commits_verified_receipts_and_remote_evidence(self):
        claim = self._claim()
        admitter = self._admitter()
        authorization = admitter.admit(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
        )
        self.scheduler.start_claim(claim)
        candidate = self._success_candidate(
            admitter,
            claim,
            authorization,
        )

        admitter.complete(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
            authorization,
            candidate,
            candidate.runtime_proof,
        )

        attempt = self.store.get_attempt(claim.attempt_id)
        self.assertEqual(attempt.status, AttemptStatus.SUCCEEDED)
        receipt = self.store.get_tool_receipt("run", claim.attempt_id)
        self.assertIsNotNone(receipt)
        terminal = [
            event
            for event in self.store.list_events("run")
            if event.attempt_id == claim.attempt_id
            and event.event_type == "attempt.succeeded"
        ][0]
        evidence = terminal.payload["remote_evidence"]
        self.assertEqual(
            evidence["runtime_proof_digest"],
            candidate.runtime_proof.proof_digest,
        )
        self.assertNotIn(candidate.runtime_proof.signature, str(terminal.payload))

    def test_worker_adapter_to_control_plane_is_path_free_end_to_end(self):
        claim = self._claim()
        admitter = self._admitter()
        authorization = admitter.admit(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
        )
        assignment = WorkAssignment(
            claim=self._binding(claim, authorization),
            worker_id=self.identity.worker_id,
            attempt_number=claim.attempt_number,
            lease_expires_at=claim.lease_expires_at,
            activity_kind=claim.activity_kind,
            effect_class=claim.effect_class,
            resource_keys=claim.resource_keys,
            activity_descriptor=RemoteActivityDescriptor(
                activity_name="exec",
                config_digest=canonical_digest(dict(claim.config)),
            ),
            execution_plan=authorization.execution_plan,
            input_grants=authorization.input_grants,
            output_grants=authorization.output_grants,
        )
        worker = SecureRemoteExecutionAdapter(
            _WorkerArtifactTransport(
                admitter,
                self.identity,
                self.registration,
                self.scheduler,
                claim,
                authorization,
            ),
            _WorkerSandboxAdapter(
                authorization.runtime_attestation_digest,
            ),
            _WorkerProofSigner(self.runtime_verifier),
        )
        grant = worker.prepare(assignment)
        self.scheduler.start_claim(claim)

        outcome = worker.execute(grant, object())
        verified = worker.verify_candidate(grant, outcome)
        candidate = RemoteCompletionCandidate(
            outcome=AttemptStatus.SUCCEEDED,
            output_handles=verified.output_handles,
            runtime_proof=verified.runtime_proof,
            error_class=None,
            error_code=None,
        )
        admitter.complete(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
            authorization,
            candidate,
            verified.runtime_proof,
        )

        wire = assignment.to_wire()
        self.assertNotIn("uri", str(wire))
        self.assertNotIn(str(self.control_root), str(wire))
        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.SUCCEEDED,
        )

    def test_terminal_completion_replays_after_cache_eviction_and_restart(self):
        admitter = self._admitter()
        control = RemoteControlPlane(
            lambda run_id: self.scheduler,
            authorize_run=lambda identity, run_id: (
                identity == self.identity and run_id == "replay-run"
            ),
            assignment_admitter=admitter,
            max_request_cache=1,
            clock=self.clock,
            journal=RemoteControlJournal(
                self.control_root / "replay-remote-control.sqlite3"
            ),
        )
        client = RemoteWorkerClient(
            lambda request: control.handle(self.identity, request),
            worker_id=self.identity.worker_id,
            instance_id="replay-instance",
        )
        client.register(
            runtime_version="runsc-1.2",
            capabilities=("activity.tool",),
            resource_keys=("workspace:/project",),
            activity_kinds=("tool",),
            max_concurrency=1,
        )
        self.scheduler.create_run("replay-run")
        self.scheduler.reconcile("replay-run")
        assignment = client.poll("replay-run")
        assert assignment is not None
        registration = control.get_registration(self.identity.worker_id)
        assert registration is not None
        authorization = ExecutionAuthorization(
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
            execution_plan=assignment.execution_plan,
            input_grants=assignment.input_grants,
            output_grants=assignment.output_grants,
        )
        claim = self.scheduler.restore_claim(
            assignment.claim.run_id,
            assignment.claim.node_id,
            assignment.claim.attempt_id,
            registration.session_owner_id,
            request_hash=assignment.claim.activity_request_digest,
            claim_token=assignment.claim.claim_token,
            fencing_token=assignment.claim.fencing_token,
        )
        staged = admitter.stage_output(
            self.identity,
            registration,
            self.scheduler,
            claim,
            authorization,
            content=b'{"replay":true}',
            media_type="application/json",
        )
        receipt = {
            "schema_version": 2,
            "backend_id": "attested-gvisor",
            "security_level": "container",
            "profile_id": assignment.execution_plan.profile_id,
            "profile_digest": assignment.claim.profile_digest,
            "action_digest": assignment.claim.action_digest,
            "policy_version": assignment.execution_plan.policy_version,
            "request_digest": assignment.claim.request_digest,
            "outcome": "succeeded",
            "exit_code": 0,
            "timed_out": False,
            "output_artifact_refs": [
                {
                    "artifact_id": staged.descriptor.artifact_id,
                    "sha256": staged.descriptor.sha256,
                    "size": staged.descriptor.size,
                    "kind": staged.descriptor.kind,
                }
            ],
            "error_code": None,
        }
        proof = self.runtime_verifier.make_proof(
            assignment.claim,
            receipt,
            output_handles=(staged.handle,),
            outcome="succeeded",
        )
        outcome = RemoteExecutionOutcome(
            "succeeded",
            (staged.handle,),
            proof,
        )
        client.start(assignment.claim)
        first = client.complete(assignment.claim, outcome)

        # max_request_cache=1 ensures the new request does not depend on the
        # original completion response entry.
        evicted_replay = client.complete(assignment.claim, outcome)
        self.assertEqual(evicted_replay, first)

        restarted_admitter = self._admitter()
        restarted = RemoteControlPlane(
            lambda run_id: self.scheduler,
            authorize_run=lambda identity, run_id: (
                identity == self.identity and run_id == "replay-run"
            ),
            assignment_admitter=restarted_admitter,
            max_request_cache=1,
            clock=self.clock,
            journal=RemoteControlJournal(
                self.control_root / "replay-remote-control.sqlite3"
            ),
        )
        restarted_client = RemoteWorkerClient(
            lambda request: restarted.handle(self.identity, request),
            worker_id=self.identity.worker_id,
            instance_id="replay-instance",
        )
        restarted_client.register(
            runtime_version="runsc-1.2",
            capabilities=("activity.tool",),
            resource_keys=("workspace:/project",),
            activity_kinds=("tool",),
            max_concurrency=1,
        )

        restarted_replay = restarted_client.complete(
            assignment.claim,
            outcome,
        )
        self.assertEqual(restarted_replay, first)

        tampered_handle = ArtifactOutputHandle(
            grant_id=staged.handle.grant_id,
            token="replay-tampered-token",
        )
        tampered_handle_proof = self.runtime_verifier.make_proof(
            assignment.claim,
            receipt,
            output_handles=(tampered_handle,),
            outcome="succeeded",
        )
        with self.assertRaisesRegex(Exception, "authorization_conflict"):
            restarted_client.complete(
                assignment.claim,
                RemoteExecutionOutcome(
                    "succeeded",
                    (tampered_handle,),
                    tampered_handle_proof,
                ),
            )

        conflicting_claim = replace(
            assignment.claim,
            execution_plan_digest="f" * 64,
        )
        conflicting_proof = self.runtime_verifier.make_proof(
            conflicting_claim,
            receipt,
            output_handles=(staged.handle,),
            outcome="succeeded",
        )
        conflicting_outcome = RemoteExecutionOutcome(
            "succeeded",
            (staged.handle,),
            conflicting_proof,
        )
        with self.assertRaisesRegex(Exception, "authorization_conflict"):
            restarted_client.complete(
                conflicting_claim,
                conflicting_outcome,
            )

    def test_forged_runtime_signature_cannot_commit(self):
        claim = self._claim()
        admitter = self._admitter()
        authorization = admitter.admit(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
        )
        self.scheduler.start_claim(claim)
        candidate = self._success_candidate(
            admitter,
            claim,
            authorization,
        )
        forged_proof = replace(
            candidate.runtime_proof,
            signature=base64.urlsafe_b64encode(b"forged").rstrip(b"=").decode(),
        )
        forged = replace(
            candidate,
            runtime_proof=forged_proof,
        )

        with self.assertRaisesRegex(
            SecureRemoteExecutionError,
            "runtime_proof_unverified",
        ):
            admitter.complete(
                self.identity,
                self.registration,
                self.scheduler,
                claim,
                authorization,
                forged,
                forged_proof,
            )

        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )

    def test_heartbeat_renewed_deadline_does_not_change_claim_authority(self):
        claim = self._claim()
        admitter = self._admitter()
        authorization = admitter.admit(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
        )
        self.scheduler.start_claim(claim)
        self.scheduler.renew_claim(claim, lease_seconds=180)
        renewed = self.scheduler.restore_claim(
            claim.run_id,
            claim.node_id,
            claim.attempt_id,
            claim.worker_id,
            request_hash=claim.request_hash,
            claim_token=claim.claim_token,
            fencing_token=claim.fencing_token,
        )
        self.assertNotEqual(renewed.lease_expires_at, claim.lease_expires_at)
        candidate = self._success_candidate(
            admitter,
            renewed,
            authorization,
        )

        admitter.complete(
            self.identity,
            self.registration,
            self.scheduler,
            renewed,
            authorization,
            candidate,
            candidate.runtime_proof,
        )

        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.SUCCEEDED,
        )

    def test_multiple_heartbeats_cross_idle_and_credential_ttls_without_drift(
        self,
    ):
        admitter = self._admitter(prepared_ttl_seconds=1.0)
        control = RemoteControlPlane(
            lambda run_id: self.scheduler,
            authorize_run=lambda identity, run_id: (
                identity == self.identity and run_id == "long-running"
            ),
            assignment_admitter=admitter,
            clock=self.clock,
            journal=RemoteControlJournal(
                self.control_root / "long-running-remote-control.sqlite3"
            ),
        )
        client = RemoteWorkerClient(
            lambda request: control.handle(self.identity, request),
            worker_id=self.identity.worker_id,
            instance_id=self.registration.instance_id,
        )
        client.register(
            runtime_version=self.registration.runtime_version,
            capabilities=self.registration.capabilities,
            resource_keys=self.registration.resource_keys,
            activity_kinds=self.registration.activity_kinds,
            max_concurrency=1,
        )
        self.scheduler.create_run("long-running")
        self.scheduler.reconcile("long-running")
        assignment = client.poll("long-running", lease_seconds=300)
        assert assignment is not None
        original = assignment.claim
        original_digests = (
            original.action_digest,
            original.authorization_digest,
            original.profile_digest,
            original.request_digest,
            original.session_binding_digest,
            original.grant_binding_digest,
            original.execution_plan_digest,
            original.runtime_attestation_digest,
        )
        original_authorization = admitter._prepared[
            original.attempt_id
        ].authorization
        client.start(original)

        # Each heartbeat arrives while the durable lease is valid, but after
        # the prior 120-second identity/attestation credential expired.
        for _ in range(2):
            self.clock.value += 121
            response = client.heartbeat(original, lease_seconds=300)
            self.assertTrue(response["renewed"])
            record = admitter._prepared[original.attempt_id]
            self.assertEqual(record.authorization, original_authorization)
            self.assertEqual(
                (
                    record.authorization.action_digest,
                    record.authorization.authorization_digest,
                    record.authorization.profile_digest,
                    record.authorization.request_digest,
                    record.authorization.session_binding_digest,
                    record.authorization.grant_binding_digest,
                    record.authorization.execution_plan_digest,
                    record.authorization.runtime_attestation_digest,
                ),
                original_digests,
            )
            self.assertGreater(
                record.worker_authorization.expires_at,
                self.clock.value,
            )
            self.assertGreater(
                record.runtime_attestation.expires_at,
                self.clock.value,
            )

        registration = control.get_registration(self.identity.worker_id)
        assert registration is not None
        renewed = self.scheduler.restore_claim(
            original.run_id,
            original.node_id,
            original.attempt_id,
            registration.session_owner_id,
            request_hash=original.activity_request_digest,
            claim_token=original.claim_token,
            fencing_token=original.fencing_token,
        )
        record = admitter._prepared[original.attempt_id]
        candidate = self._success_candidate(
            admitter,
            renewed,
            record.authorization,
            registration=registration,
        )
        admitter.complete(
            self.identity,
            registration,
            self.scheduler,
            renewed,
            record.authorization,
            candidate,
            candidate.runtime_proof,
        )
        self.assertEqual(
            self.store.get_attempt(original.attempt_id).status,
            AttemptStatus.SUCCEEDED,
        )

    def test_cancel_crosses_credential_ttl_with_same_authority_lineage(self):
        claim = self._claim("long-cancel")
        admitter = self._admitter(prepared_ttl_seconds=1.0)
        authorization = admitter.admit(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
        )
        self.scheduler.start_claim(claim)
        self.scheduler.renew_claim(claim, lease_seconds=300)
        self.clock.value += 121
        renewed_record = admitter.admit(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
        )
        self.assertEqual(renewed_record, authorization)
        renewed = self.scheduler.restore_claim(
            claim.run_id,
            claim.node_id,
            claim.attempt_id,
            claim.worker_id,
            request_hash=claim.request_hash,
            claim_token=claim.claim_token,
            fencing_token=claim.fencing_token,
        )
        self.scheduler.request_cancel(claim.run_id)
        receipt = {
            "schema_version": 2,
            "backend_id": "attested-gvisor",
            "security_level": "container",
            "profile_id": self.profile.profile_id,
            "profile_digest": authorization.profile_digest,
            "action_digest": authorization.action_digest,
            "policy_version": self.executor.policy.policy_version,
            "request_digest": authorization.request_digest,
            "outcome": SandboxOutcome.CANCELLED.value,
            "exit_code": None,
            "timed_out": False,
            "output_artifact_refs": [],
            "error_code": "cancelled",
        }
        proof = self.runtime_verifier.make_proof(
            self._binding(renewed, authorization),
            receipt,
            output_handles=(),
            outcome="cancelled",
        )
        admitter.cancel(
            self.identity,
            self.registration,
            self.scheduler,
            renewed,
            authorization,
            proof,
        )
        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.CANCELLED,
        )

    def test_registry_purge_separates_idle_ttl_from_running_lease(self):
        admitter = self._admitter(
            prepared_ttl_seconds=1.0,
            maximum_prepared=2,
        )
        active = self._claim("purge-active")
        active_authorization = admitter.admit(
            self.identity,
            self.registration,
            self.scheduler,
            active,
        )
        self.scheduler.start_claim(active)
        self.scheduler.renew_claim(active, lease_seconds=300)

        # Model an abandoned process-local admission whose durable claim no
        # longer exists.  It consumes both prepared and claim-lock capacity.
        active_record = admitter._prepared[active.attempt_id]
        stale_id = "purge-stale-attempt"
        stale_record = replace(
            active_record,
            prepared=replace(
                active_record.prepared,
                claim=replace(
                    active_record.prepared.claim,
                    attempt_id=stale_id,
                ),
            ),
            idle_expires_at=self.clock.value + 1,
        )
        with admitter._lock:
            admitter._prepared[stale_id] = stale_record
        admitter._claim_lock(stale_id)
        self.assertEqual(len(admitter._prepared), 2)
        self.clock.value += 2

        admitter._purge_registry(self.clock.value)
        self.assertEqual(
            set(admitter._prepared),
            {active.attempt_id},
        )
        self.assertEqual(
            admitter._prepared[active.attempt_id].authorization,
            active_authorization,
        )
        capacity_lock = admitter._claim_lock("reclaimed-capacity-slot")
        self.assertIsNotNone(capacity_lock)

        # Once the exact durable leases expire, both stale records become
        # reclaimable and cannot consume the bounded registry forever.
        self.clock.value += 301
        admitter._purge_registry(self.clock.value)
        self.assertEqual(admitter._prepared, {})
        self.assertLessEqual(
            len(admitter._claim_locks),
            admitter._maximum_prepared,
        )

    def test_registry_purge_never_holds_global_lock_during_store_io(self):
        claim = self._claim("purge-lock")
        admitter = self._admitter()
        admitter.admit(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
        )
        entered = threading.Event()
        proceed = threading.Event()
        errors: list[BaseException] = []
        original_restore = self.scheduler.restore_claim

        def blocking_restore(*args, **kwargs):
            entered.set()
            if not proceed.wait(timeout=5):
                raise RuntimeError("purge barrier timed out")
            return original_restore(*args, **kwargs)

        self.scheduler.restore_claim = blocking_restore  # type: ignore[method-assign]
        self.addCleanup(
            setattr,
            self.scheduler,
            "restore_claim",
            original_restore,
        )

        def purge():
            try:
                admitter._purge_registry(self.clock.value)
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=purge)
        thread.start()
        self.assertTrue(entered.wait(timeout=5))
        acquired = admitter._lock.acquire(timeout=1)
        self.assertTrue(acquired, "Store I/O ran under the global registry lock")
        if acquired:
            admitter._lock.release()
        proceed.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])

    def test_registry_purge_does_not_drop_claim_during_heartbeat_renewal(self):
        claim = self._claim("purge-heartbeat-race")
        admitter = self._admitter()
        admitter.admit(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
        )
        self.scheduler.start_claim(claim)
        entered = threading.Event()
        proceed = threading.Event()
        original_claim_handle = self.scheduler._claim_handle

        def blocking_claim_handle(*args, **kwargs):
            entered.set()
            if not proceed.wait(timeout=5):
                raise RuntimeError("claim restore barrier timed out")
            return original_claim_handle(*args, **kwargs)

        self.scheduler._claim_handle = blocking_claim_handle  # type: ignore[method-assign]
        self.addCleanup(
            setattr,
            self.scheduler,
            "_claim_handle",
            original_claim_handle,
        )
        errors: list[BaseException] = []

        def purge():
            try:
                admitter._purge_registry(self.clock.value)
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=purge)
        thread.start()
        self.assertTrue(entered.wait(timeout=5))
        self.scheduler.renew_claim(claim, lease_seconds=180)
        proceed.set()
        thread.join(timeout=5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertIn(claim.attempt_id, admitter._prepared)

    def test_restart_loses_prepared_authority_and_fails_closed(self):
        claim = self._claim()
        original = self._admitter()
        authorization = original.admit(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
        )
        self.scheduler.start_claim(claim)
        candidate = self._success_candidate(
            original,
            claim,
            authorization,
        )
        restarted = self._admitter()

        with self.assertRaisesRegex(
            SecureRemoteExecutionError,
            "prepared_registry_miss",
        ):
            restarted.complete(
                self.identity,
                self.registration,
                self.scheduler,
                claim,
                authorization,
                candidate,
                candidate.runtime_proof,
            )

        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )

    def test_new_worker_session_cannot_take_over_old_claim(self):
        claim = self._claim()
        admitter = self._admitter()
        authorization = admitter.admit(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
        )
        self.scheduler.start_claim(claim)
        takeover_registration = replace(
            self.registration,
            instance_id="instance-b",
            session_owner_id="remote-session:" + ("b" * 64),
            session_binding_digest="b" * 64,
        )

        with self.assertRaisesRegex(
            SecureRemoteExecutionError,
            "control_context_mismatch",
        ):
            admitter.complete(
                self.identity,
                takeover_registration,
                self.scheduler,
                claim,
                authorization,
                self._success_candidate(admitter, claim, authorization),
                self._success_candidate(
                    admitter,
                    claim,
                    authorization,
                ).runtime_proof,
            )

        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )

    def test_output_handle_tamper_is_rejected_by_broker(self):
        claim = self._claim()
        admitter = self._admitter()
        authorization = admitter.admit(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
        )
        self.scheduler.start_claim(claim)
        candidate = self._success_candidate(
            admitter,
            claim,
            authorization,
        )
        tampered_handle = ArtifactOutputHandle(
            grant_id=candidate.output_handles[0].grant_id,
            token="tampered-output-token",
        )
        receipt = dict(candidate.runtime_proof.sandbox_receipt)
        proof = self.runtime_verifier.make_proof(
            self._binding(claim, authorization),
            receipt,
            output_handles=(tampered_handle,),
            outcome="succeeded",
        )
        tampered = RemoteCompletionCandidate(
            outcome=AttemptStatus.SUCCEEDED,
            output_handles=(tampered_handle,),
            runtime_proof=proof,
            error_class=None,
            error_code=None,
        )

        with self.assertRaisesRegex(Exception, "grant_binding_mismatch"):
            admitter.complete(
                self.identity,
                self.registration,
                self.scheduler,
                claim,
                authorization,
                tampered,
                proof,
            )

        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )

    def test_finalized_artifact_tamper_blocks_domain_reference(self):
        claim = self._claim()
        admitter = self._admitter()
        authorization = admitter.admit(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
        )
        self.scheduler.start_claim(claim)
        candidate = self._success_candidate(
            admitter,
            claim,
            authorization,
        )
        digest = candidate.runtime_proof.sandbox_receipt[
            "output_artifact_refs"
        ][0]["sha256"]
        artifact_path = (
            self.artifacts.root
            / "sha256"
            / digest[:2]
            / digest[2:4]
            / digest
        )
        artifact_path.write_bytes(b"tampered")

        with self.assertRaises(Exception):
            admitter.complete(
                self.identity,
                self.registration,
                self.scheduler,
                claim,
                authorization,
                candidate,
                candidate.runtime_proof,
            )

        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )
        self.assertIsNone(
            self.store.get_tool_receipt(claim.run_id, claim.attempt_id)
        )

    def test_signed_cancel_receipt_reaches_cancelled(self):
        claim = self._claim()
        admitter = self._admitter()
        authorization = admitter.admit(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
        )
        self.scheduler.start_claim(claim)
        self.scheduler.request_cancel(claim.run_id)
        receipt = {
            "schema_version": 2,
            "backend_id": "attested-gvisor",
            "security_level": "container",
            "profile_id": self.profile.profile_id,
            "profile_digest": authorization.profile_digest,
            "action_digest": authorization.action_digest,
            "policy_version": self.executor.policy.policy_version,
            "request_digest": authorization.request_digest,
            "outcome": SandboxOutcome.CANCELLED.value,
            "exit_code": None,
            "timed_out": False,
            "output_artifact_refs": [],
            "error_code": "cancelled",
        }
        proof = self.runtime_verifier.make_proof(
            self._binding(claim, authorization),
            receipt,
            output_handles=(),
            outcome="cancelled",
        )

        admitter.cancel(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
            authorization,
            proof,
        )

        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.CANCELLED,
        )

    def test_heartbeat_then_signed_cancel_uses_renewed_claim(self):
        claim = self._claim()
        admitter = self._admitter()
        authorization = admitter.admit(
            self.identity,
            self.registration,
            self.scheduler,
            claim,
        )
        self.scheduler.start_claim(claim)
        self.scheduler.renew_claim(claim, lease_seconds=180)
        renewed = self.scheduler.restore_claim(
            claim.run_id,
            claim.node_id,
            claim.attempt_id,
            claim.worker_id,
            request_hash=claim.request_hash,
            claim_token=claim.claim_token,
            fencing_token=claim.fencing_token,
        )
        self.scheduler.request_cancel(claim.run_id)
        receipt = {
            "schema_version": 2,
            "backend_id": "attested-gvisor",
            "security_level": "container",
            "profile_id": self.profile.profile_id,
            "profile_digest": authorization.profile_digest,
            "action_digest": authorization.action_digest,
            "policy_version": self.executor.policy.policy_version,
            "request_digest": authorization.request_digest,
            "outcome": SandboxOutcome.CANCELLED.value,
            "exit_code": None,
            "timed_out": False,
            "output_artifact_refs": [],
            "error_code": "cancelled",
        }
        proof = self.runtime_verifier.make_proof(
            self._binding(renewed, authorization),
            receipt,
            output_handles=(),
            outcome="cancelled",
        )

        admitter.cancel(
            self.identity,
            self.registration,
            self.scheduler,
            renewed,
            authorization,
            proof,
        )

        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.CANCELLED,
        )

    def test_policy_deny_commits_no_remote_authorization(self):
        deny_policy = PolicyEngine(
            (self.tool_policy,),
            (
                PolicyRule(
                    "deny-exec",
                    PolicyOutcome.DENY,
                    tool_name="exec",
                    effect_classes=(EffectClass.READ_ONLY,),
                    reason_code="denied_by_test_policy",
                ),
            ),
        )
        deny_executor = TrustedActivityExecutor(
            self.scheduler,
            deny_policy,
            SandboxDispatcher(
                (_UnusedBackend(),),
                policy_version=deny_policy.policy_version,
            ),
            artifact_verifier=self.artifacts.verify,
            artifact_reader=self.artifacts.read,
            clock=self.clock,
        )
        claim = self._claim("policy-deny")
        admitter = self._admitter(executor=deny_executor)

        with self.assertRaisesRegex(
            SecureRemoteExecutionError,
            "policy_not_allowed",
        ):
            admitter.admit(
                self.identity,
                self.registration,
                self.scheduler,
                claim,
            )

        self.assertEqual(
            self.store.get_attempt(claim.attempt_id).status,
            AttemptStatus.FAILED,
        )
        self.assertEqual(admitter._prepared, {})

    def test_failed_admissions_do_not_exhaust_claim_lock_registry(self):
        admitter = self._admitter(
            plan_resolver=_FailingPlanResolver(),
            maximum_prepared=2,
        )
        base_claim = self._claim("failure-base")

        for index in range(5):
            claim = replace(
                base_claim,
                attempt_id=f"failed-admission-{index}",
            )
            with self.assertRaises(RuntimeError):
                admitter.admit(
                    self.identity,
                    self.registration,
                    self.scheduler,
                    claim,
                )

        self.assertEqual(admitter._claim_locks, {})


if __name__ == "__main__":
    unittest.main()
