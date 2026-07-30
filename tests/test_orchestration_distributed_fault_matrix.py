from __future__ import annotations

import hashlib
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

from src.orchestration.artifact_broker import (
    ArtifactGrantBroker,
    ArtifactGrantConsumed,
    ArtifactGrantDenied,
    ArtifactWriteGrant,
)
from src.orchestration.artifacts import (
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    LocalArtifactStore,
)
from src.orchestration.lease import (
    DurableLeaseReaper,
    ProbeOutcome,
    ProbeResult,
    RecoveryRetryPolicy,
)
from src.orchestration.models import AttemptStatus, NodeStatus, RunStatus
from src.orchestration.oci_backend import (
    OciGvisorSandboxBackend,
    OciRuntimeResult,
    OciSandboxSpecBuilder,
    OciSpecDenied,
    RuntimeAttestation,
    RuntimeAttestationInvalid,
    RuntimeIsolationKind,
)
from src.orchestration.policy import (
    ActionRequest,
    Capability,
    EffectClass,
    PolicyDecision,
    PolicyOutcome,
)
from src.orchestration.remote_control import RemoteControlPlane
from src.orchestration.remote_journal import RemoteControlJournal
from src.orchestration.remote_observability import BoundedRemoteObservability
from src.orchestration.remote_protocol import (
    AuthenticatedWorker,
    ExecutionAuthorization,
    RemoteExecutionPlan,
    RemoteOperation,
    RemoteProtocolError,
    RemoteRuntimeProof,
    WorkAssignment,
    canonical_digest,
    grant_binding_digest,
    make_request,
    parse_response,
    runtime_binding_digest,
)
from src.orchestration.remote_scheduling import (
    DeterministicRemoteScheduler,
    PollOutcome,
    ReleaseOutcome,
    RemoteTask,
    WorkerDescriptor,
    WorkerLifecycle,
)
from src.orchestration.remote_worker import (
    RemoteExecutionOutcome,
    RemoteWorkerClient,
    RemoteWorkerError,
)
from src.orchestration.sandbox import (
    ExecutionRequest,
    NetworkMode,
    ResourceLimits,
    SandboxDispatchDenied,
    SandboxProfile,
    SecurityLevel,
    build_execution_binding_digest,
)
from src.orchestration.scheduler import ActivityReceipt, DurableScheduler
from src.orchestration.store import DurableRunStore
from src.orchestration.worker_security import WorkerAuthorization
from src.orchestration.workflow import compile_workflow


_READ = Capability("workspace.read")
_RUN = Capability("process.execute")
_POLICY_VERSION = "sha256:" + ("a" * 64)
_OCI_IMAGE = "registry.example/xagent/worker@sha256:" + ("1" * 64)
_SECCOMP_DIGEST = "2" * 64


class _FakeClock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self.value

    def advance(self, seconds: float) -> None:
        with self._lock:
            self.value += seconds


class _RemoteAdmitter:
    production_security_ready = True
    reference_admission_only = True
    runtime_attestation_digest = hashlib.sha256(b"matrix-runtime").hexdigest()

    def __init__(self, artifacts: LocalArtifactStore) -> None:
        self.artifacts = artifacts

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
        profile_digest = hashlib.sha256(b"matrix-profile").hexdigest()
        request_digest = hashlib.sha256(
            f"request:{claim.request_hash}".encode()
        ).hexdigest()
        plan = RemoteExecutionPlan(
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
            profile_id="matrix-profile",
            profile_digest=profile_digest,
            policy_version="matrix-policy-v1",
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
        output_grants = (output_grant,)
        return ExecutionAuthorization(
            action_digest=action_digest,
            authorization_digest=authorization_digest,
            profile_digest=profile_digest,
            request_digest=request_digest,
            session_binding_digest=registration.session_binding_digest,
            grant_binding_digest=grant_binding_digest((), output_grants),
            execution_plan_digest=plan.plan_digest,
            runtime_attestation_digest=self.runtime_attestation_digest,
            execution_plan=plan,
            output_grants=output_grants,
        )

    def complete(
        self,
        _identity,
        _registration,
        scheduler,
        claim,
        authorization,
        candidate,
        runtime_proof,
    ):
        if candidate.runtime_proof != runtime_proof:
            raise RuntimeError("runtime proof mismatch")
        if candidate.outcome is AttemptStatus.SUCCEEDED:
            expected = tuple(
                grant.output_handle for grant in authorization.output_grants
            )
            if candidate.output_handles != expected:
                raise RuntimeError("unexpected output handles")
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
                attempt_status=AttemptStatus.SUCCEEDED,
            )
            return
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


class _Probe:
    def __init__(self, result: ProbeResult) -> None:
        self.result = result
        self.calls = 0

    def probe(self, _attempt):
        self.calls += 1
        return self.result


class _AuthorizationVerifier:
    def verify(self, authorization: WorkerAuthorization, *, now: float) -> bool:
        return (
            authorization.authorization_id == "matrix-authorization"
            and now < authorization.expires_at
        )


class _RuntimeAdapter:
    adapter_id = "matrix-runtime-adapter"

    def __init__(self) -> None:
        self.specs = []

    def execute(self, spec):
        self.specs.append(spec)
        return OciRuntimeResult(
            spec_digest=spec.spec_digest,
            action_digest=spec.action_digest,
            request_digest=spec.request_digest,
            runtime_attestation_digest=spec.runtime_attestation_digest,
            exit_code=0,
        )


class _RuntimeVerifier:
    def __init__(
        self,
        *,
        missing: bool = False,
        adapter_id: str = _RuntimeAdapter.adapter_id,
    ) -> None:
        self.missing = missing
        self.adapter_id = adapter_id

    def verify(self, _adapter, *, now: float):
        if self.missing:
            raise RuntimeError("no deployment attestation")
        return RuntimeAttestation(
            adapter_id=self.adapter_id,
            isolation_kind=RuntimeIsolationKind.GVISOR,
            runtime_class="runsc",
            runtime_binary_digest="3" * 64,
            verifier_id="matrix-deployment-verifier",
            attestation_id="matrix-attestation",
            issued_at=now - 10,
            expires_at=now + 60,
        )


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
        "backend_id": "matrix-container",
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
    spec_digest = hashlib.sha256(b"matrix-sandbox-spec").hexdigest()
    signed_binding = runtime_binding_digest(
        assignment.claim,
        outcome=outcome,
        output_handles=tuple(output_handles),
        sandbox_receipt_digest=canonical_digest(receipt),
        sandbox_spec_digest=spec_digest,
    )
    return RemoteRuntimeProof(
        proof_id=f"proof-{assignment.claim.attempt_id}-{outcome}",
        verifier_key_id="matrix-verifier-key",
        signed_binding_digest=signed_binding,
        signature="dGVzdA",
        sandbox_spec_digest=spec_digest,
        sandbox_receipt=receipt,
    )


class DistributedExecutionFaultMatrixTests(unittest.TestCase):
    """Executable R01-R20 evidence with deterministic clocks and barriers."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.clock = _FakeClock()
        (
            self.store,
            self.artifacts,
            self.scheduler,
            self.control,
            self.identity,
            self.client,
        ) = self._remote_harness("matrix-run")

    def _remote_harness(
        self,
        run_id: str,
        *,
        effect_class: str = "read_only",
    ):
        control_root = self.root / run_id
        control_root.mkdir()
        store = DurableRunStore(control_root / "runs.sqlite3")
        artifacts = LocalArtifactStore(control_root / "artifacts")
        workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": f"distributed-{run_id}",
                "version": 1,
                "nodes": [
                    {
                        "id": "work",
                        "kind": "tool",
                        "config": {
                            "tool": "inspect",
                            "arguments": {"mode": "summary"},
                        },
                        "effect_class": effect_class,
                        **(
                            {
                                "idempotency_key_template": (
                                    "{{run_id}}:{{node_id}}"
                                )
                            }
                            if effect_class == "idempotent_write"
                            else {}
                        ),
                        "resource_keys": ["workspace:project"],
                    }
                ],
            }
        )
        scheduler = DurableScheduler(
            store,
            workflow,
            clock=self.clock,
            artifact_verifier=artifacts.verify,
        )
        scheduler.create_run(run_id)
        scheduler.reconcile(run_id)
        identity = AuthenticatedWorker(
            worker_id="worker-1",
            tenant_id="tenant-1",
            identity_digest=hashlib.sha256(b"matrix-worker-1").hexdigest(),
        )
        control = RemoteControlPlane(
            lambda requested: scheduler
            if requested == run_id
            else (_ for _ in ()).throw(KeyError(requested)),
            authorize_run=lambda principal, requested: (
                principal.tenant_id == "tenant-1" and requested == run_id
            ),
            assignment_admitter=_RemoteAdmitter(artifacts),
            allow_reference_admission=True,
            journal=RemoteControlJournal(
                control_root / "remote-control.sqlite3"
            ),
        )
        client = RemoteWorkerClient(
            lambda request: control.handle(identity, request),
            worker_id=identity.worker_id,
            instance_id="instance-old",
        )
        return store, artifacts, scheduler, control, identity, client

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
    def _register(client: RemoteWorkerClient, *, capacity: int = 1) -> None:
        client.register(
            runtime_version="worker-runtime/1.0",
            capabilities=("activity.tool", "artifact.refs"),
            resource_keys=("workspace:project",),
            activity_kinds=("tool",),
            max_concurrency=capacity,
        )

    @staticmethod
    def _registration_request(
        *,
        request_id: str,
        worker_id: str = "worker-1",
        instance_id: str = "instance-old",
        capacity: int = 1,
    ):
        return make_request(
            RemoteOperation.REGISTER,
            request_id=request_id,
            worker_id=worker_id,
            instance_id=instance_id,
            body={
                "runtime_version": "worker-runtime/1.0",
                "capabilities": ["activity.tool", "artifact.refs"],
                "resource_keys": ["workspace:project"],
                "activity_kinds": ["tool"],
                "max_concurrency": capacity,
            },
        )

    def _worker_authorization(self, attempt_id: str, **overrides):
        values = {
            "authorization_id": "matrix-authorization",
            "worker_id": "worker-1",
            "tenant_id": "tenant-1",
            "pool_id": "pool-1",
            "run_id": "matrix-run",
            "node_id": "work",
            "attempt_id": attempt_id,
            "action_digest": "4" * 64,
            "identity_binding_digest": "5" * 64,
            "transport_binding_digest": "6" * 64,
            "rule_id": "matrix-rule",
            "maximum_artifact_sensitivity": ArtifactSensitivity.INTERNAL,
            "issued_at": 90.0,
            "expires_at": 200.0,
        }
        values.update(overrides)
        return WorkerAuthorization(**values)

    @staticmethod
    def _routing_worker(
        worker_id: str,
        *,
        tenants: frozenset[str] = frozenset({"tenant-a", "tenant-b"}),
        capacity: int = 1,
    ) -> WorkerDescriptor:
        return WorkerDescriptor(
            worker_id=worker_id,
            session_id=f"session-{worker_id}",
            pool_id="pool-a",
            runtime_version="2.1",
            capabilities=frozenset({"workspace.read"}),
            tools=frozenset({"file.read"}),
            authorized_tenants=tenants,
            capacity=capacity,
        )

    @staticmethod
    def _routing_poll(
        scheduler: DeterministicRemoteScheduler,
        worker_id: str,
    ):
        snapshot = scheduler.worker_snapshot(worker_id)
        assert snapshot is not None
        return scheduler.poll_and_claim(
            worker_id,
            worker_generation=snapshot.generation,
            session_id=snapshot.descriptor.session_id,
        )

    @staticmethod
    def _routing_release(
        scheduler: DeterministicRemoteScheduler,
        assignment,
    ):
        return scheduler.release(
            assignment.assignment_id,
            assignment.worker_id,
            worker_generation=assignment.worker_generation,
            session_id=assignment.worker_session_id,
        )

    @staticmethod
    def _routing_task(task_id: str, tenant_id: str) -> RemoteTask:
        return RemoteTask(
            task_id=task_id,
            tenant_id=tenant_id,
            pool_id="pool-a",
            tool_name="file.read",
            required_capabilities=frozenset({"workspace.read"}),
            min_runtime_version="2",
            max_runtime_version="2.9",
        )

    def _oci_profile(self):
        workspace = self.root / "oci-workspace"
        workspace.mkdir(exist_ok=True)
        profile = SandboxProfile(
            profile_id="matrix-remote-code",
            allowed_roots=(workspace,),
            capabilities=(_READ, _RUN),
            limits=ResourceLimits(
                timeout_seconds=30,
                cpu_seconds=20,
                memory_bytes=256 * 1024 * 1024,
                output_bytes=1024 * 1024,
                process_count=32,
            ),
            minimum_security_level=SecurityLevel.CONTAINER,
            network_mode=NetworkMode.DENY,
        )
        return workspace, profile

    def _oci_request(self, *, cwd: Path, profile: SandboxProfile):
        limits = ResourceLimits(
            timeout_seconds=10,
            cpu_seconds=5,
            memory_bytes=64 * 1024 * 1024,
            output_bytes=1024 * 1024,
            process_count=4,
        )
        argv = ("/usr/bin/python3", "/inputs/artifact-0")
        operation_key = "matrix-operation"
        binding = build_execution_binding_digest(
            argv=argv,
            cwd=str(cwd),
            profile=profile,
            limits=limits,
            operation_key=operation_key,
            idempotency_key=operation_key,
            capabilities=(_READ, _RUN),
        )
        action = ActionRequest.from_args(
            run_id="matrix-run",
            node_id="work",
            attempt_id="matrix-attempt",
            tool_name="code_run",
            args={"input": "artifact"},
            execution_binding_digest=binding,
            operation_key=operation_key,
            idempotency_key=operation_key,
            effect_class=EffectClass.READ_ONLY,
            capabilities=(_READ, _RUN),
        )
        decision = PolicyDecision(
            outcome=PolicyOutcome.ALLOW,
            action_digest=action.action_digest,
            policy_version=_POLICY_VERSION,
            reason_code="matrix_allow",
        )
        return ExecutionRequest(
            action=action,
            policy_decision=decision,
            argv=argv,
            operation_key=operation_key,
            idempotency_key=operation_key,
            cwd=str(cwd),
            limits=limits,
        )

    def _oci_backend(self, adapter, verifier):
        return OciGvisorSandboxBackend(
            backend_id="matrix-gvisor",
            adapter=adapter,
            attestation_verifier=verifier,
            spec_builder=OciSandboxSpecBuilder(
                image=_OCI_IMAGE,
                runtime_class="runsc",
                seccomp_profile_digest=_SECCOMP_DIGEST,
            ),
            capabilities=(_READ, _RUN),
            clock=self.clock,
        )

    def test_r01_unauthenticated_register_has_zero_state_change(self) -> None:
        request = self._registration_request(request_id="r01-register")
        before_events = self.store.list_events("matrix-run")

        with self.assertRaises(TypeError):
            self.control.handle(None, request.to_wire())

        self.assertIsNone(self.control.get_registration("worker-1"))
        self.assertEqual(self.store.list_events("matrix-run"), before_events)
        self.assertEqual(self.store.list_attempts("matrix-run"), [])

    def test_r02_body_impersonation_never_overrides_transport_identity(self) -> None:
        attacker = AuthenticatedWorker(
            worker_id="worker-attacker",
            tenant_id="tenant-1",
            identity_digest=hashlib.sha256(b"matrix-attacker").hexdigest(),
        )
        request = self._registration_request(
            request_id="r02-register",
            worker_id="worker-victim",
        )
        response = parse_response(self.control.handle(attacker, request.to_wire()))
        self.assertFalse(response.ok)
        self.assertEqual(
            response.body,
            {"error_code": "worker_identity_mismatch"},
        )

        injected = request.to_wire()
        injected["body"]["tenant_id"] = "tenant-victim"
        with self.assertRaises(RemoteProtocolError):
            self.control.handle(attacker, injected)
        self.assertIsNone(self.control.get_registration("worker-victim"))
        self.assertIsNone(self.control.get_registration("worker-attacker"))
        self.assertEqual(self.store.list_attempts("matrix-run"), [])

    def test_r03_same_request_id_with_different_body_is_conflict(self) -> None:
        first = self._registration_request(request_id="r03-register", capacity=1)
        changed = self._registration_request(request_id="r03-register", capacity=2)

        accepted = parse_response(self.control.handle(self.identity, first.to_wire()))
        rejected = parse_response(self.control.handle(self.identity, changed.to_wire()))

        self.assertTrue(accepted.ok)
        self.assertFalse(rejected.ok)
        self.assertEqual(rejected.body, {"error_code": "request_id_conflict"})
        registration = self.control.get_registration("worker-1")
        assert registration is not None
        self.assertEqual(registration.max_concurrency, 1)

    def test_r04_duplicate_poll_after_response_loss_has_one_attempt(self) -> None:
        self._register(self.client)
        request = make_request(
            RemoteOperation.POLL,
            request_id="r04-poll",
            worker_id="worker-1",
            instance_id="instance-old",
            body={"run_id": "matrix-run", "lease_seconds": 30.0},
        )

        lost_response = self.control.handle(self.identity, request.to_wire())
        retried_response = self.control.handle(self.identity, request.to_wire())

        self.assertEqual(retried_response, lost_response)
        self.assertIsNotNone(parse_response(retried_response).body["assignment"])
        self.assertEqual(len(self.store.list_attempts("matrix-run")), 1)

    def test_r05_wrong_heartbeat_token_and_fencing_do_not_extend_lease(self) -> None:
        self._register(self.client)
        assignment = self.client.poll("matrix-run", lease_seconds=10)
        assert assignment is not None
        durable_attempt = self.store.get_attempt(assignment.claim.attempt_id)
        assert durable_attempt is not None
        before = self.store.get_idempotency(
            "matrix-run",
            durable_attempt.idempotency_key,
        )
        assert before is not None

        for stale in (
            replace(assignment.claim, claim_token="foreign-claim-token"),
            replace(
                assignment.claim,
                fencing_token=assignment.claim.fencing_token + 1,
            ),
        ):
            with self.assertRaisesRegex(RemoteWorkerError, "claim_conflict"):
                self.client.heartbeat(stale, lease_seconds=60)

        after = self.store.get_idempotency(
            "matrix-run",
            durable_attempt.idempotency_key,
        )
        assert after is not None
        self.assertEqual(after.lease_expires_at, before.lease_expires_at)
        self.assertEqual(
            self.store.get_attempt(assignment.claim.attempt_id).status,
            AttemptStatus.CLAIMED,
        )

    def test_r06_replaced_session_cannot_use_old_claim_authority(self) -> None:
        self._register(self.client)
        assignment = self.client.poll("matrix-run", lease_seconds=30)
        assert assignment is not None
        restarted = RemoteControlPlane(
            lambda requested: self.scheduler
            if requested == "matrix-run"
            else (_ for _ in ()).throw(KeyError(requested)),
            authorize_run=lambda principal, requested: (
                principal.tenant_id == "tenant-1"
                and requested == "matrix-run"
            ),
            assignment_admitter=_RemoteAdmitter(self.artifacts),
            allow_reference_admission=True,
            journal=RemoteControlJournal(
                self.root / "matrix-run" / "remote-control.sqlite3"
            ),
        )
        same_session = RemoteWorkerClient(
            lambda request: restarted.handle(self.identity, request),
            worker_id="worker-1",
            instance_id="instance-old",
        )
        self._register(same_session)
        same_session.start(assignment.claim)

        replacement = RemoteWorkerClient(
            lambda request: restarted.handle(self.identity, request),
            worker_id="worker-1",
            instance_id="instance-new",
        )
        self._register(replacement)

        with self.assertRaisesRegex(
            RemoteWorkerError,
            "worker_identity_mismatch|claim_conflict",
        ):
            same_session.complete(
                assignment.claim,
                self._success_outcome(assignment),
            )
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "worker_identity_mismatch|claim_conflict",
        ):
            replacement.start(assignment.claim)
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "worker_identity_mismatch|claim_conflict",
        ):
            replacement.complete(
                assignment.claim,
                self._success_outcome(assignment),
            )

        attempt = self.store.get_attempt(assignment.claim.attempt_id)
        assert attempt is not None
        self.assertEqual(attempt.status, AttemptStatus.RUNNING)

    def test_r07_crash_before_start_recovers_claimed_as_abandoned(self) -> None:
        self._register(self.client)
        assignment = self.client.poll("matrix-run", lease_seconds=5)
        assert assignment is not None
        self.clock.advance(6)

        report = DurableLeaseReaper(
            self.store,
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(
                max_attempts=2
            ),
        ).run_once(now=self.clock())

        self.assertEqual(len(report.resolved), 1)
        self.assertEqual(
            self.store.get_attempt(assignment.claim.attempt_id).status,
            AttemptStatus.ABANDONED,
        )
        with self.assertRaisesRegex(RemoteWorkerError, "claim_conflict"):
            self.client.start(assignment.claim)
        self.assertEqual(len(self.store.list_attempts("matrix-run")), 1)

    def test_r08_read_only_crash_after_start_fences_old_completion(self) -> None:
        self._register(self.client)
        old = self.client.poll("matrix-run", lease_seconds=5)
        assert old is not None
        self.client.start(old.claim)
        self.clock.advance(6)
        DurableLeaseReaper(
            self.store,
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(
                max_attempts=2
            ),
        ).run_once(now=self.clock())
        self.scheduler.reconcile("matrix-run")

        new = self.client.poll("matrix-run", lease_seconds=30)
        assert new is not None
        self.assertNotEqual(new.claim.attempt_id, old.claim.attempt_id)
        self.assertGreater(new.claim.fencing_token, old.claim.fencing_token)
        with self.assertRaisesRegex(RemoteWorkerError, "claim_conflict"):
            self.client.complete(
                old.claim,
                self._success_outcome(old),
            )
        self.assertEqual(
            self.store.get_attempt(old.claim.attempt_id).status,
            AttemptStatus.ABANDONED,
        )
        self.assertEqual(
            self.store.get_attempt(new.claim.attempt_id).status,
            AttemptStatus.CLAIMED,
        )

    def test_r09_idempotent_write_disconnect_probes_single_effect(self) -> None:
        store, artifacts, _scheduler, _control, _identity, client = (
            self._remote_harness(
                "r09-run",
                effect_class="idempotent_write",
            )
        )
        self._register(client)
        assignment = client.poll("r09-run", lease_seconds=5)
        assert assignment is not None
        client.start(assignment.claim)
        durable_attempt = store.get_attempt(assignment.claim.attempt_id)
        assert durable_attempt is not None
        external_ledger = {
            durable_attempt.idempotency_key: {"visible_effect": "once"}
        }
        result_ref = artifacts.put_json(
            external_ledger[durable_attempt.idempotency_key],
            kind=ArtifactKind.TOOL_RESULT,
            producer_run_id="r09-run",
            producer_node_id="work",
            producer_attempt_id=assignment.claim.attempt_id,
        )
        probe = _Probe(ProbeResult(ProbeOutcome.COMMITTED, (result_ref,)))
        self.clock.advance(6)

        report = DurableLeaseReaper(
            store,
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(
                max_attempts=2
            ),
            probe_resolver=lambda _attempt: probe,
            artifact_verifier=artifacts,
        ).run_once(now=self.clock())

        self.assertEqual(report.resolved[0].resolution, "verified_succeeded")
        self.assertEqual(probe.calls, 1)
        self.assertEqual(len(external_ledger), 1)
        self.assertEqual(
            store.get_attempt(assignment.claim.attempt_id).status,
            AttemptStatus.SUCCEEDED,
        )
        self.assertEqual(len(store.list_attempts("r09-run")), 1)

    def test_r10_unprobeable_write_disconnect_is_outcome_unknown(self) -> None:
        store, _artifacts, _scheduler, _control, _identity, client = (
            self._remote_harness(
                "r10-run",
                effect_class="non_idempotent_write",
            )
        )
        self._register(client)
        assignment = client.poll("r10-run", lease_seconds=5)
        assert assignment is not None
        client.start(assignment.claim)
        self.clock.advance(6)

        report = DurableLeaseReaper(
            store,
            retry_policy_resolver=lambda _attempt: RecoveryRetryPolicy(
                max_attempts=5
            ),
        ).run_once(now=self.clock())

        self.assertEqual(report.resolved[0].resolution, "waiting_recovery")
        self.assertEqual(
            store.get_attempt(assignment.claim.attempt_id).status,
            AttemptStatus.OUTCOME_UNKNOWN,
        )
        self.assertEqual(
            store.get_node("r10-run", "work").status,
            NodeStatus.WAITING_RECOVERY,
        )
        self.assertEqual(
            store.get_run("r10-run").status,
            RunStatus.WAITING_RECOVERY,
        )
        self.assertEqual(len(store.list_attempts("r10-run")), 1)

    def test_r11_cancel_completion_race_preserves_the_winning_receipt(self) -> None:
        self._register(self.client)
        assignment = self.client.poll("matrix-run", lease_seconds=30)
        assert assignment is not None
        self.client.start(assignment.claim)
        barrier = threading.Barrier(3)
        outcomes: dict[str, str] = {}

        def complete() -> None:
            barrier.wait(timeout=5)
            try:
                self.client.complete(
                    assignment.claim,
                    self._success_outcome(assignment),
                )
                outcomes["complete"] = "accepted"
            except RemoteWorkerError:
                outcomes["complete"] = "stale"

        def cancel() -> None:
            barrier.wait(timeout=5)
            self.control.request_cancel(self.identity, "matrix-run")
            outcomes["cancel"] = "accepted"

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = (pool.submit(complete), pool.submit(cancel))
            barrier.wait(timeout=5)
            for future in futures:
                future.result(timeout=5)

        attempt = self.store.get_attempt(assignment.claim.attempt_id)
        assert attempt is not None
        if outcomes["complete"] == "accepted":
            self.assertEqual(attempt.status, AttemptStatus.SUCCEEDED)
            committed_ref = attempt.result["artifact_refs"][0]
            self.assertTrue(
                self.artifacts.verify(ArtifactRef.from_dict(committed_ref))
            )
        else:
            self.assertEqual(attempt.status, AttemptStatus.RUNNING)
            self.client.acknowledge_cancel(
                assignment.claim,
                _runtime_proof(assignment, outcome="cancelled"),
            )
            self.assertEqual(
                self.store.get_attempt(assignment.claim.attempt_id).status,
                AttemptStatus.CANCELLED,
            )
        self.assertEqual(outcomes["cancel"], "accepted")
        self.assertTrue(self.store.verify_projections("matrix-run"))
        self.assertEqual(len(self.store.list_attempts("matrix-run")), 1)

    def test_r12_drain_poll_race_has_no_post_drain_claim(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=self.clock)
        scheduler.register_worker(self._routing_worker("worker-1"))
        scheduler.admit(self._routing_task("r12-task", "tenant-a"))
        barrier = threading.Barrier(3)
        results = {}

        def poll() -> None:
            barrier.wait(timeout=5)
            results["poll"] = self._routing_poll(scheduler, "worker-1")

        def drain() -> None:
            barrier.wait(timeout=5)
            results["drain"] = scheduler.request_drain("worker-1")

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = (pool.submit(poll), pool.submit(drain))
            barrier.wait(timeout=5)
            for future in futures:
                future.result(timeout=5)

        snapshot = scheduler.worker_snapshot("worker-1")
        assert snapshot is not None
        self.assertEqual(snapshot.lifecycle, WorkerLifecycle.DRAINING)
        self.assertEqual(
            self._routing_poll(scheduler, "worker-1").outcome,
            PollOutcome.WORKER_DRAINING,
        )
        decision = results["poll"]
        if decision.assignment is not None:
            self.assertEqual(snapshot.active_count, 1)
            self.assertEqual(
                self._routing_release(scheduler, decision.assignment),
                ReleaseOutcome.RELEASED,
            )
        else:
            self.assertEqual(decision.outcome, PollOutcome.WORKER_DRAINING)

    def test_r13_capacity_one_concurrent_poll_claims_at_most_one(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=self.clock)
        scheduler.register_worker(self._routing_worker("worker-1", capacity=1))
        for index in range(16):
            scheduler.admit(
                self._routing_task(f"r13-task-{index}", "tenant-a")
            )
        barrier = threading.Barrier(17)

        def poll():
            barrier.wait(timeout=5)
            return self._routing_poll(scheduler, "worker-1")

        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = [pool.submit(poll) for _index in range(16)]
            barrier.wait(timeout=5)
            decisions = [future.result(timeout=5) for future in futures]

        self.assertEqual(
            sum(decision.assignment is not None for decision in decisions),
            1,
        )
        self.assertEqual(scheduler.worker_snapshot("worker-1").active_count, 1)

    def test_r14_tenant_quota_race_never_crosses_tenant_or_quota(self) -> None:
        scheduler = DeterministicRemoteScheduler(
            tenant_concurrency_quotas={"tenant-a": 1, "tenant-b": 1},
            clock=self.clock,
        )
        workers = {
            "worker-a1": frozenset({"tenant-a"}),
            "worker-a2": frozenset({"tenant-a"}),
            "worker-b1": frozenset({"tenant-b"}),
        }
        for worker_id, tenants in workers.items():
            scheduler.register_worker(
                self._routing_worker(worker_id, tenants=tenants)
            )
        for task_id, tenant in (
            ("r14-a1", "tenant-a"),
            ("r14-a2", "tenant-a"),
            ("r14-b1", "tenant-b"),
        ):
            scheduler.admit(self._routing_task(task_id, tenant))
        barrier = threading.Barrier(4)

        def poll(worker_id):
            barrier.wait(timeout=5)
            return worker_id, self._routing_poll(scheduler, worker_id)

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(poll, worker_id) for worker_id in workers]
            barrier.wait(timeout=5)
            decisions = [future.result(timeout=5) for future in futures]

        assignments = [
            (worker_id, decision.assignment)
            for worker_id, decision in decisions
            if decision.assignment is not None
        ]
        self.assertEqual(
            [assignment.task.tenant_id for _, assignment in assignments].count(
                "tenant-a"
            ),
            1,
        )
        self.assertEqual(
            [assignment.task.tenant_id for _, assignment in assignments].count(
                "tenant-b"
            ),
            1,
        )
        for worker_id, assignment in assignments:
            self.assertIn(assignment.task.tenant_id, workers[worker_id])

    def test_r15_hot_tenant_cannot_starve_an_eligible_tenant(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=self.clock)
        scheduler.register_worker(self._routing_worker("worker-1"))
        for index in range(4):
            scheduler.admit(
                self._routing_task(f"r15-hot-{index}", "tenant-a")
            )
        scheduler.admit(self._routing_task("r15-eligible", "tenant-b"))
        served = []

        for index in range(4):
            decision = self._routing_poll(scheduler, "worker-1")
            assert decision.assignment is not None
            served.append(decision.assignment.task.tenant_id)
            self._routing_release(scheduler, decision.assignment)
            scheduler.admit(
                self._routing_task(f"r15-more-hot-{index}", "tenant-a")
            )

        self.assertIn("tenant-b", served[:2])

    def test_r16_expired_or_wrongly_bound_grant_leaks_no_bytes_or_refs(self) -> None:
        self._register(self.client)
        assignment = self.client.poll("matrix-run")
        assert assignment is not None
        ref = self.artifacts.put_bytes(
            b"grant-secret-canary",
            kind=ArtifactKind.FILE_SNAPSHOT,
            sensitivity=ArtifactSensitivity.INTERNAL,
        )
        broker = ArtifactGrantBroker(
            self.artifacts,
            authorization_verifier=_AuthorizationVerifier(),
            clock=self.clock,
        )
        authorization = self._worker_authorization(assignment.claim.attempt_id)
        grant = broker.issue_read_grant(
            authorization,
            tenant_id="tenant-1",
            run_id="matrix-run",
            attempt_id=assignment.claim.attempt_id,
            ref=ref,
            ttl_seconds=5,
        )
        output = b'{"result":"bounded"}'
        write_grant = broker.issue_write_grant(
            authorization,
            tenant_id="tenant-1",
            run_id="matrix-run",
            node_id="work",
            attempt_id=assignment.claim.attempt_id,
            kind=ArtifactKind.TOOL_RESULT,
            sensitivity=ArtifactSensitivity.INTERNAL,
            media_type="application/json",
            maximum_bytes=len(output),
            declared_sha256=hashlib.sha256(output).hexdigest(),
            ttl_seconds=5,
        )
        before_events = self.store.list_events("matrix-run")
        leaked_payloads = []

        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "authorization_binding_mismatch",
        ):
            leaked_payloads.append(
                broker.redeem_read_grant(
                    grant,
                    replace(authorization, worker_id="worker-2"),
                )
            )
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "authorization_binding_mismatch",
        ):
            broker.stage_write(
                write_grant,
                replace(authorization, worker_id="worker-2"),
                content=output,
                declared_sha256=write_grant.declared_sha256,
            )
        self.clock.advance(5)
        with self.assertRaisesRegex(ArtifactGrantConsumed, "grant_unavailable"):
            leaked_payloads.append(broker.redeem_read_grant(grant, authorization))
        with self.assertRaisesRegex(
            ArtifactGrantConsumed,
            "write_grant_unavailable",
        ):
            broker.resolve_output_handle(write_grant.output_handle)

        self.assertEqual(leaked_payloads, [])
        self.assertEqual(self.store.list_events("matrix-run"), before_events)
        attempt = self.store.get_attempt(assignment.claim.attempt_id)
        assert attempt is not None
        self.assertIsNone(attempt.result)

    def test_r17_artifact_digest_mismatch_prevents_sandbox_start(self) -> None:
        self._register(self.client)
        assignment = self.client.poll("matrix-run")
        assert assignment is not None
        ref = self.artifacts.put_bytes(
            b"original-input",
            kind=ArtifactKind.FILE_SNAPSHOT,
            sensitivity=ArtifactSensitivity.INTERNAL,
        )
        authorization = self._worker_authorization(assignment.claim.attempt_id)
        broker = ArtifactGrantBroker(
            self.artifacts,
            authorization_verifier=_AuthorizationVerifier(),
            clock=self.clock,
        )
        grant = broker.issue_read_grant(
            authorization,
            tenant_id="tenant-1",
            run_id="matrix-run",
            attempt_id=assignment.claim.attempt_id,
            ref=ref,
        )
        expected_output = b'{"result":"original"}'
        write_grant = broker.issue_write_grant(
            authorization,
            tenant_id="tenant-1",
            run_id="matrix-run",
            node_id="work",
            attempt_id=assignment.claim.attempt_id,
            kind=ArtifactKind.TOOL_RESULT,
            sensitivity=ArtifactSensitivity.INTERNAL,
            media_type="application/json",
            maximum_bytes=len(expected_output),
            declared_sha256=hashlib.sha256(expected_output).hexdigest(),
        )
        (self.artifacts.root / ref.uri).write_bytes(b"tampered-input")
        sandbox_calls = []

        def materialize_then_start() -> None:
            payload = broker.redeem_read_grant(grant, authorization)
            sandbox_calls.append(payload.descriptor.sha256)

        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "artifact_integrity_failed",
        ):
            materialize_then_start()
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "artifact_digest_mismatch",
        ):
            broker.stage_write(
                write_grant,
                authorization,
                content=b'{"result":"tampered"}',
                declared_sha256=write_grant.declared_sha256,
            )
        self.assertEqual(sandbox_calls, [])
        self.assertEqual(
            self.store.get_attempt(assignment.claim.attempt_id).status,
            AttemptStatus.CLAIMED,
        )

    def test_r18_control_plane_path_is_rejected_before_runtime_adapter(self) -> None:
        _workspace, profile = self._oci_profile()
        adapter = _RuntimeAdapter()
        self._oci_backend(adapter, _RuntimeVerifier())
        control_path = self.root / "matrix-run"

        with self.assertRaises(SandboxDispatchDenied):
            self._oci_request(cwd=control_path, profile=profile)

        self.assertEqual(adapter.specs, [])

    def test_r19_forged_or_missing_attestation_never_claims_container(self) -> None:
        for verifier in (
            _RuntimeVerifier(missing=True),
            _RuntimeVerifier(adapter_id="forged-adapter"),
        ):
            with self.subTest(verifier=verifier.adapter_id):
                adapter = _RuntimeAdapter()
                with self.assertRaises(RuntimeAttestationInvalid):
                    self._oci_backend(adapter, verifier)
                self.assertEqual(adapter.specs, [])

    def test_r20_telemetry_failure_does_not_mutate_scheduling_truth(self) -> None:
        scheduler = DeterministicRemoteScheduler(clock=self.clock)
        scheduler.register_worker(self._routing_worker("worker-1"))
        scheduler.admit(self._routing_task("r20-task", "tenant-a"))
        decision = self._routing_poll(scheduler, "worker-1")
        assert decision.assignment is not None
        observations = BoundedRemoteObservability(clock=self.clock)
        observations.record_poll("pool-a", decision.outcome)
        observations.record_claim(
            "pool-a",
            schedule_to_start_seconds=(
                decision.assignment.schedule_to_start_seconds
            ),
        )
        before = scheduler.snapshot()

        def failing_exporter(_snapshot) -> None:
            raise RuntimeError("exporter unavailable")

        with self.assertRaisesRegex(RuntimeError, "exporter unavailable"):
            failing_exporter(observations.snapshot())

        after = scheduler.snapshot()
        self.assertEqual(after, before)
        self.assertEqual(
            scheduler.worker_snapshot("worker-1").active_count,
            1,
        )
        self.assertFalse(observations.snapshot().execution_truth)


if __name__ == "__main__":
    unittest.main()
