#!/usr/bin/env python3
"""Credential-free Phase 2 distributed-execution reference demo.

This executable uses the real durable Store, Scheduler, policy gate, Artifact
broker, ToolReceipt, remote protocol client/daemon, and logical replay.  Its
transport, workload identity, and runtime proof adapters are deterministic
in-process references for acceptance testing.  They are not mTLS, SPIFFE,
gVisor, Kubernetes, or production isolation evidence.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import sys
import threading
import time
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from src.orchestration.artifact_broker import ArtifactGrantBroker  # noqa: E402
from src.orchestration.artifacts import LocalArtifactStore  # noqa: E402
from src.orchestration.executor import TrustedActivityExecutor  # noqa: E402
from src.orchestration.models import AttemptStatus, NodeStatus  # noqa: E402
from src.orchestration.policy import (  # noqa: E402
    EffectClass,
    PolicyEngine,
    ToolPolicy,
    ToolTimeoutBehavior,
)
from src.orchestration.remote_control import (  # noqa: E402
    RemoteControlPlane,
    WorkerRegistration,
)
from src.orchestration.remote_journal import RemoteControlJournal  # noqa: E402
from src.orchestration.remote_execution_journal import (  # noqa: E402
    RemoteExecutionJournal,
)
from src.orchestration.remote_execution import (  # noqa: E402
    RemoteExecutionPreparation,
    SecureRemoteAssignmentAdmitter,
    VerifiedRemoteRuntimeAttestation,
)
from src.orchestration.remote_protocol import (  # noqa: E402
    AuthenticatedWorker,
    ClaimBinding,
    ExecutionAuthorization,
    RemoteRuntimeProof,
    WorkAssignment,
    canonical_digest,
    runtime_binding_digest,
)
from src.orchestration.remote_worker import (  # noqa: E402
    RemoteExecutionContext,
    RemoteExecutionGrant,
    RemoteExecutionOutcome,
    RemoteWorkerClient,
    RemoteWorkerDaemon,
    RemoteWorkerError,
)
from src.orchestration.replay import build_replay_report  # noqa: E402
from src.orchestration.sandbox import (  # noqa: E402
    SandboxDispatcher,
    SandboxOutcome,
    SandboxProfile,
    SecurityLevel,
)
from src.orchestration.scheduler import (  # noqa: E402
    ActivityClaim,
    ApprovalResolution,
    DurableScheduler,
)
from src.orchestration.store import DurableRunStore  # noqa: E402
from src.orchestration.worker_security import (  # noqa: E402
    WorkerAccessRule,
    WorkerAuthorizationGate,
    WorkerIdentity,
)
from src.orchestration.workflow import compile_workflow  # noqa: E402

WORKFLOW_PATH = (
    Path(__file__).resolve().parent
    / "workflows"
    / "distributed_execution_demo.json"
)
PRIMARY_RUN_ID = "distributed-reference-primary"
CANCEL_RUN_ID = "distributed-reference-cancel"
WORKER_IDS = ("worker-a", "worker-b", "worker-c")
TENANT_ID = "tenant-reference"
POOL_ID = "pool-reference"


class _UnusedContainerBackend:
    """Dispatcher placeholder: remote completion never calls this backend."""

    backend_id = "reference-container-placeholder"
    security_level = SecurityLevel.CONTAINER
    capabilities = ()
    supports_materialized_script = True

    def execute(self, request, profile):
        del request, profile
        raise AssertionError("the control plane must not execute remote work locally")


class _ReferencePresentationResolver:
    def resolve(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
    ) -> object:
        return {
            "worker_id": identity.worker_id,
            "transport_binding_digest": registration.session_binding_digest,
        }


class _ReferenceWorkloadAttestor:
    """Acceptance-only identity adapter, not certificate verification."""

    def attest(self, presentation: object, *, now: float) -> WorkerIdentity:
        if not isinstance(presentation, Mapping):
            raise ValueError("reference presentation must be an object")
        return WorkerIdentity(
            worker_id=str(presentation["worker_id"]),
            subject=f"reference://{presentation['worker_id']}",
            issuer="in-process-reference",
            tenant_id=TENANT_ID,
            pool_id=POOL_ID,
            capabilities=(),
            transport_binding_digest=str(
                presentation["transport_binding_digest"]
            ),
            issued_at=now,
            not_before=now,
            expires_at=now + 120,
            attestation_id=f"reference-{presentation['worker_id']}",
        )


class _ReferencePreflightAuthorizer:
    """Coarse acceptance-only authorization before Store claim mutation."""

    production_security_ready = True

    def authorize(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        *,
        pool_id: str,
        presentation: object,
        now: float,
    ) -> bool:
        del scheduler, now
        return (
            isinstance(presentation, Mapping)
            and identity.worker_id == registration.worker_id
            and identity.tenant_id == TENANT_ID
            and pool_id == POOL_ID
            and presentation.get("worker_id") == identity.worker_id
        )


class _ReferencePlanResolver:
    def __init__(self, agent_root: Path, profile: SandboxProfile) -> None:
        self.agent_root = agent_root
        self.profile = profile

    def resolve(
        self,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
    ) -> RemoteExecutionPreparation:
        del scheduler
        return RemoteExecutionPreparation(
            argv=("reference-runner", claim.node_id),
            cwd=str(self.agent_root),
            container_cwd="/workspace",
            profile=self.profile,
            resource_locks=claim.resource_keys,
            input_artifact_refs=claim.input_artifact_refs,
        )


class _ReferenceRuntimeProofVerifier:
    """HMAC reference seam; it proves binding logic, not runtime isolation."""

    production_security_ready = True
    _KEY = b"xagent-distributed-reference-proof-v1"

    @staticmethod
    def attestation_digest(worker_id: str, instance_id: str) -> str:
        return hashlib.sha256(
            f"reference-runtime\0{worker_id}\0{instance_id}".encode("utf-8")
        ).hexdigest()

    def attest(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        *,
        now: float,
    ) -> VerifiedRemoteRuntimeAttestation:
        return VerifiedRemoteRuntimeAttestation(
            worker_id=identity.worker_id,
            instance_id=registration.instance_id,
            identity_digest=identity.identity_digest,
            runtime_version=registration.runtime_version,
            runtime_attestation_digest=self.attestation_digest(
                identity.worker_id,
                registration.instance_id,
            ),
            security_level=SecurityLevel.CONTAINER,
            verifier_id="in-process-reference-verifier",
            issued_at=now,
            expires_at=now + 120,
        )

    def verify(
        self,
        proof: RemoteRuntimeProof,
        *,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        claim: ClaimBinding,
        authorization: ExecutionAuthorization,
        attestation: VerifiedRemoteRuntimeAttestation,
        now: float,
    ) -> bool:
        del identity, registration, claim, authorization
        expected = self._signature(proof.signed_binding_digest)
        return (
            now < attestation.expires_at
            and proof.verifier_key_id == "reference-hmac-key"
            and hmac.compare_digest(proof.signature, expected)
        )

    def make_proof(
        self,
        claim: ClaimBinding,
        receipt: Mapping[str, Any],
        *,
        outcome: str,
        output_handles: tuple,
        sandbox_spec_digest: str,
    ) -> RemoteRuntimeProof:
        binding = runtime_binding_digest(
            claim,
            outcome=outcome,
            output_handles=output_handles,
            sandbox_receipt_digest=canonical_digest(receipt),
            sandbox_spec_digest=sandbox_spec_digest,
        )
        return RemoteRuntimeProof(
            proof_id=f"reference-proof-{claim.attempt_id}",
            verifier_key_id="reference-hmac-key",
            signed_binding_digest=binding,
            signature=self._signature(binding),
            sandbox_spec_digest=sandbox_spec_digest,
            sandbox_receipt=receipt,
        )

    @classmethod
    def _signature(cls, binding: str) -> str:
        return base64.urlsafe_b64encode(
            hmac.new(cls._KEY, binding.encode("ascii"), hashlib.sha256).digest()
        ).rstrip(b"=").decode("ascii")


class _ConcurrencyEvidence:
    """Barrier-backed proof that all three worker calls overlap logically."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._barriers: dict[str, threading.Barrier] = {}
        self._active: dict[str, int] = {}
        self._maximum: dict[str, int] = {}
        self._participants: dict[str, set[str]] = {}

    def enable(self, run_id: str) -> None:
        with self._lock:
            self._barriers[run_id] = threading.Barrier(len(WORKER_IDS))

    def enter(self, run_id: str, worker_id: str) -> None:
        with self._lock:
            self._active[run_id] = self._active.get(run_id, 0) + 1
            self._maximum[run_id] = max(
                self._maximum.get(run_id, 0),
                self._active[run_id],
            )
            self._participants.setdefault(run_id, set()).add(worker_id)
            barrier = self._barriers.get(run_id)
        if barrier is not None:
            barrier.wait(timeout=10)

    def leave(self, run_id: str) -> None:
        with self._lock:
            self._active[run_id] -= 1

    def maximum(self, run_id: str) -> int:
        with self._lock:
            return self._maximum.get(run_id, 0)

    def participants(self, run_id: str) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._participants.get(run_id, ())))


@dataclass(frozen=True, slots=True)
class _ControlExecutionContext:
    registration: WorkerRegistration
    claim: ActivityClaim
    authorization: ExecutionAuthorization


class _ReferenceWorkerAdapter:
    """Worker-side reference adapter using authenticated in-process broker RPC."""

    production_security_ready = True
    supported_activity_kinds = frozenset({"tool"})

    def __init__(
        self,
        plane: "_ReferenceDistributedPlane",
        identity: AuthenticatedWorker,
        instance_id: str,
    ) -> None:
        self.plane = plane
        self.identity = identity
        self.instance_id = instance_id
        self.runtime_attestation_digest = (
            plane.runtime_verifier.attestation_digest(
                identity.worker_id,
                instance_id,
            )
        )
        self._control_contexts: dict[str, _ControlExecutionContext] = {}

    def prepare(self, assignment: WorkAssignment) -> RemoteExecutionGrant:
        claim = assignment.claim
        grant = RemoteExecutionGrant(
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
        # Capture the exact pre-heartbeat durable claim. Heartbeat renewal
        # changes only the lease deadline and must not reconstruct prepared
        # authority from worker data.
        self._control_contexts[claim.attempt_id] = self.plane.control_context(
            self.identity,
            claim,
        )
        return grant

    def execute(
        self,
        grant: RemoteExecutionGrant,
        context: RemoteExecutionContext,
    ) -> RemoteExecutionOutcome:
        run_id = grant.assignment.claim.run_id
        worker_id = self.identity.worker_id
        if run_id == CANCEL_RUN_ID:
            self.plane.cancel_execution_entered.set()
            if not self.plane.cancel_execution_release.wait(timeout=10):
                raise RuntimeError("cancellation reference barrier timed out")
            if context.cancellation_requested():
                return RemoteExecutionOutcome(
                    outcome="cancelled",
                    output_handles=(),
                    runtime_proof=self._cancel_proof(grant),
                )

        self.plane.concurrency.enter(run_id, worker_id)
        try:
            control = self._control_contexts[grant.assignment.claim.attempt_id]
            payload = json.dumps(
                {
                    "node_id": control.claim.node_id,
                    "worker_id": worker_id,
                    "result": "reference-complete",
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            staged = self.plane.admitter.stage_output(
                self.identity,
                control.registration,
                self.plane.scheduler,
                control.claim,
                control.authorization,
                content=payload,
                media_type="application/json",
            )
            receipt = self.plane.receipt(
                grant,
                outcome=SandboxOutcome.SUCCEEDED,
                output_identities=(
                    {
                        "artifact_id": staged.descriptor.artifact_id,
                        "sha256": staged.descriptor.sha256,
                        "size": staged.descriptor.size,
                        "kind": staged.descriptor.kind,
                    },
                ),
            )
            proof = self.plane.runtime_verifier.make_proof(
                grant.assignment.claim,
                receipt,
                outcome="succeeded",
                output_handles=(staged.handle,),
                sandbox_spec_digest=self.plane.spec_digest(grant),
            )
            return RemoteExecutionOutcome(
                outcome="succeeded",
                output_handles=(staged.handle,),
                runtime_proof=proof,
            )
        finally:
            self.plane.concurrency.leave(run_id)

    def verify_candidate(
        self,
        grant: RemoteExecutionGrant,
        candidate: RemoteExecutionOutcome,
    ) -> RemoteExecutionOutcome:
        del grant
        return candidate

    def cancel(
        self,
        grant: RemoteExecutionGrant,
        context: RemoteExecutionContext,
    ) -> RemoteRuntimeProof:
        del context
        return self._cancel_proof(grant)

    def _cancel_proof(self, grant: RemoteExecutionGrant) -> RemoteRuntimeProof:
        receipt = self.plane.receipt(
            grant,
            outcome=SandboxOutcome.CANCELLED,
            output_identities=(),
        )
        return self.plane.runtime_verifier.make_proof(
            grant.assignment.claim,
            receipt,
            outcome="cancelled",
            output_handles=(),
            sandbox_spec_digest=self.plane.spec_digest(grant),
        )


class _ReferenceDistributedPlane:
    def __init__(self, runtime_root: Path, workflow_path: Path) -> None:
        control_root = (runtime_root / "control-plane").resolve()
        self.agent_root = (runtime_root / "agent-workspace").resolve()
        control_root.mkdir(parents=True, exist_ok=True)
        self.agent_root.mkdir(parents=True, exist_ok=True)
        workflow = compile_workflow(
            json.loads(workflow_path.read_text(encoding="utf-8"))
        )
        self.artifacts = LocalArtifactStore(control_root / "artifacts")
        self.store = DurableRunStore(control_root / "orchestration.sqlite3")
        self.scheduler = DurableScheduler(
            self.store,
            workflow,
            max_active_attempts=8,
            artifact_verifier=self.artifacts.verify,
            approval_verifier=self.verify_approval,
        )
        policy = PolicyEngine(
            (
                ToolPolicy(
                    "reference_task",
                    EffectClass.READ_ONLY,
                    supports_idempotency_key=False,
                    supports_status_probe=False,
                    supports_compensation=False,
                    timeout_behavior=ToolTimeoutBehavior.SAFE_TO_RETRY,
                ),
            )
        )
        self.profile = SandboxProfile(
            "reference-remote-container",
            (self.agent_root,),
            (),
            minimum_security_level=SecurityLevel.CONTAINER,
        )
        self.executor = TrustedActivityExecutor(
            self.scheduler,
            policy,
            SandboxDispatcher(
                (_UnusedContainerBackend(),),
                policy_version=policy.policy_version,
            ),
            artifact_verifier=self.artifacts.verify,
            artifact_reader=self.artifacts.read,
        )
        self.worker_gate = WorkerAuthorizationGate(
            _ReferenceWorkloadAttestor(),
            (
                WorkerAccessRule(
                    "reference-read-only",
                    TENANT_ID,
                    POOL_ID,
                    ("reference_task",),
                    (),
                    (EffectClass.READ_ONLY,),
                ),
            ),
        )
        execution_journal = RemoteExecutionJournal(
            control_root / "remote-execution.sqlite3"
        )
        self.broker = ArtifactGrantBroker(
            self.artifacts,
            authorization_verifier=self.worker_gate,
            recovery_journal=execution_journal,
        )
        self.runtime_verifier = _ReferenceRuntimeProofVerifier()
        self.admitter = SecureRemoteAssignmentAdmitter(
            self.executor,
            self.worker_gate,
            self.broker,
            _ReferencePlanResolver(self.agent_root, self.profile),
            _ReferencePresentationResolver(),
            self.runtime_verifier,
            _ReferencePreflightAuthorizer(),
            pool_resolver=lambda _registration: POOL_ID,
            recovery_journal=execution_journal,
        )
        self.identities = {
            worker_id: AuthenticatedWorker(
                worker_id=worker_id,
                tenant_id=TENANT_ID,
                identity_digest=hashlib.sha256(
                    f"reference-transport\0{worker_id}".encode("utf-8")
                ).hexdigest(),
            )
            for worker_id in WORKER_IDS
        }
        self.control = RemoteControlPlane(
            lambda run_id: self.scheduler,
            authorize_run=lambda identity, run_id: (
                identity.tenant_id == TENANT_ID
                and self.store.get_run(run_id) is not None
            ),
            assignment_admitter=self.admitter,
            journal=RemoteControlJournal(
                control_root / "remote-control.sqlite3"
            ),
        )
        self.concurrency = _ConcurrencyEvidence()
        self.cancel_execution_entered = threading.Event()
        self.cancel_execution_release = threading.Event()
        self.daemons: dict[str, RemoteWorkerDaemon] = {}
        for worker_id in WORKER_IDS:
            identity = self.identities[worker_id]
            instance_id = f"{worker_id}-instance-1"
            client = RemoteWorkerClient(
                lambda request, identity=identity: self.control.handle(
                    identity,
                    request,
                ),
                worker_id=worker_id,
                instance_id=instance_id,
            )
            adapter = _ReferenceWorkerAdapter(self, identity, instance_id)
            self.daemons[worker_id] = RemoteWorkerDaemon(
                client,
                adapter,
                runtime_version="1.0",
                capabilities=("activity.tool",),
                resource_keys=(),
                activity_kinds=("tool",),
                max_concurrency=1,
                lease_seconds=60,
            )

    def execute_completed_run(self, run_id: str) -> dict[str, Any]:
        self.scheduler.create_run(run_id)
        self.scheduler.reconcile(run_id)
        self.concurrency.enable(run_id)
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(self._execute_with_retry, worker_id, run_id)
                for worker_id in WORKER_IDS
            ]
            if [future.result(timeout=20) for future in futures] != [
                True,
                True,
                True,
            ]:
                raise RuntimeError("not all reference workers received an assignment")
        approval = self.store.get_node(run_id, "operator_approval")
        if approval is None or approval.status is not NodeStatus.WAITING_APPROVAL:
            raise RuntimeError("approval node did not become durably waiting")
        self.scheduler.resolve_approval(
            run_id,
            "operator_approval",
            self.approval_resolution(run_id, "operator_approval"),
        )
        replay = build_replay_report(self.store, run_id)
        attempts = self.store.list_attempts(run_id)
        receipts = [
            self.store.get_tool_receipt(run_id, attempt.attempt_id)
            for attempt in attempts
        ]
        artifact_count = sum(
            len(attempt.result.get("artifact_refs", ()))
            if isinstance(attempt.result, Mapping)
            else 0
            for attempt in attempts
        )
        run = self.store.get_run(run_id)
        assert run is not None
        maximum = self.concurrency.maximum(run_id)
        return {
            "run_id": run_id,
            "status": run.status.value,
            "worker_sessions": len(self.daemons),
            "participating_workers": list(
                self.concurrency.participants(run_id)
            ),
            "maximum_concurrency": maximum,
            "concurrent_overlap_proved": maximum >= 2,
            "approval_status": self.store.get_node(
                run_id,
                "operator_approval",
            ).status.value,
            "artifact_count": artifact_count,
            "tool_receipt_count": sum(receipt is not None for receipt in receipts),
            "event_count": replay.snapshot.event_count,
            "replay_matches_live": replay.matches_live,
            "replay_digest": replay.golden_digest,
        }

    def _execute_with_retry(self, worker_id: str, run_id: str) -> bool:
        """Retry only transient claim CAS conflicts, never execution failures."""

        for _attempt in range(20):
            try:
                if self.daemons[worker_id].execute_one(run_id):
                    return True
            except RemoteWorkerError as exc:
                if exc.code != "claim_conflict":
                    raise
        raise RuntimeError("reference worker exhausted claim conflict retries")

    def execute_cancel_run(self) -> dict[str, Any]:
        self.scheduler.create_run(CANCEL_RUN_ID)
        self.scheduler.reconcile(CANCEL_RUN_ID)
        future = ThreadPoolExecutor(max_workers=1)
        try:
            result = future.submit(
                self.daemons["worker-c"].execute_one,
                CANCEL_RUN_ID,
            )
            if not self.cancel_execution_entered.wait(timeout=10):
                raise RuntimeError("cancel worker did not enter execution")
            self.control.request_cancel(
                self.identities["worker-c"],
                CANCEL_RUN_ID,
            )
            self.cancel_execution_release.set()
            if result.result(timeout=20) is not True:
                raise RuntimeError("cancel worker did not acknowledge cancellation")
        finally:
            self.cancel_execution_release.set()
            future.shutdown(wait=True)
        attempts = self.store.list_attempts(CANCEL_RUN_ID)
        if len(attempts) != 1:
            raise RuntimeError("cancel reference run must start exactly one Attempt")
        replay = build_replay_report(self.store, CANCEL_RUN_ID)
        run = self.store.get_run(CANCEL_RUN_ID)
        assert run is not None
        return {
            "run_id": CANCEL_RUN_ID,
            "status": run.status.value,
            "worker": "worker-c",
            "attempt_status": attempts[0].status.value,
            "event_count": replay.snapshot.event_count,
            "replay_matches_live": replay.matches_live,
            "replay_digest": replay.golden_digest,
        }

    def control_context(
        self,
        identity: AuthenticatedWorker,
        binding: ClaimBinding,
    ) -> _ControlExecutionContext:
        registration = self.control.get_registration(identity.worker_id)
        if registration is None:
            raise RuntimeError("reference worker is not registered")
        claim = self.scheduler.restore_claim(
            binding.run_id,
            binding.node_id,
            binding.attempt_id,
            registration.session_owner_id,
            request_hash=binding.activity_request_digest,
            claim_token=binding.claim_token,
            fencing_token=binding.fencing_token,
        )
        authorization = self.admitter.admit(
            identity,
            registration,
            self.scheduler,
            claim,
        )
        return _ControlExecutionContext(registration, claim, authorization)

    def receipt(
        self,
        grant: RemoteExecutionGrant,
        *,
        outcome: SandboxOutcome,
        output_identities: tuple[Mapping[str, Any], ...],
    ) -> dict[str, Any]:
        succeeded = outcome is SandboxOutcome.SUCCEEDED
        return {
            "schema_version": 2,
            "backend_id": "in-process-reference-runtime",
            "security_level": SecurityLevel.CONTAINER.value,
            "profile_id": self.profile.profile_id,
            "profile_digest": grant.profile_digest,
            "action_digest": grant.action_digest,
            "policy_version": self.executor.policy.policy_version,
            "request_digest": grant.request_digest,
            "outcome": outcome.value,
            "exit_code": 0 if succeeded else None,
            "timed_out": False,
            "output_artifact_refs": [dict(value) for value in output_identities],
            "error_code": None if succeeded else "cancelled",
        }

    @staticmethod
    def spec_digest(grant: RemoteExecutionGrant) -> str:
        return canonical_digest(
            {
                "schema": "reference_execution_spec_v1",
                "plan": grant.assignment.execution_plan.to_wire(),
            }
        )

    @staticmethod
    def verify_approval(resolution: ApprovalResolution) -> bool:
        expected = hashlib.sha256(
            (
                f"{resolution.approval_id}\0{resolution.run_id}\0"
                f"{resolution.node_id}\0{resolution.definition_digest}\0"
                f"{resolution.approved}"
            ).encode("utf-8")
        ).hexdigest()
        return hmac.compare_digest(resolution.decision_digest, expected)

    def approval_resolution(
        self,
        run_id: str,
        node_id: str,
    ) -> ApprovalResolution:
        approval_id = f"reference-ledger-{run_id}-{node_id}"
        digest = hashlib.sha256(
            (
                f"{approval_id}\0{run_id}\0{node_id}\0"
                f"{self.scheduler.workflow.definition_digest}\0True"
            ).encode("utf-8")
        ).hexdigest()
        return ApprovalResolution(
            approval_id=approval_id,
            run_id=run_id,
            node_id=node_id,
            definition_digest=self.scheduler.workflow.definition_digest,
            approved=True,
            decision_digest=digest,
        )


def run_demo(
    runtime_root: Path,
    *,
    workflow_path: Path = WORKFLOW_PATH,
) -> dict[str, Any]:
    """Run the flagship success and cancellation scenarios."""

    plane = _ReferenceDistributedPlane(runtime_root.resolve(), workflow_path)
    primary = plane.execute_completed_run(PRIMARY_RUN_ID)
    cancelled = plane.execute_cancel_run()
    return {
        "schema_version": 1,
        "primary_run": primary,
        "cancel_run": cancelled,
        "security_boundary": {
            "transport": "in_process_reference",
            "identity": "deterministic_reference_attestor",
            "runtime_proof": "hmac_binding_reference",
            "digest_only_execution_recovery": (
                plane.control.production_recovery_ready
            ),
            "bearer_free_artifact_recovery": (
                plane.broker.durable_recovery_ready
            ),
            "mtls_deployed": False,
            "spiffe_deployed": False,
            "gvisor_deployed": False,
            "kubernetes_deployed": False,
        },
    }


def run_soak(
    runtime_root: Path,
    *,
    logical_runs: int = 5,
    workflow_path: Path = WORKFLOW_PATH,
) -> dict[str, Any]:
    """Run a bounded logical workload and report wall time without an SLO claim."""

    if (
        isinstance(logical_runs, bool)
        or not isinstance(logical_runs, int)
        or not 1 <= logical_runs <= 100
    ):
        raise ValueError("logical_runs must be between 1 and 100")
    plane = _ReferenceDistributedPlane(runtime_root.resolve(), workflow_path)
    started = time.perf_counter()
    completed = 0
    for index in range(logical_runs):
        summary = plane.execute_completed_run(f"distributed-soak-{index + 1:04d}")
        completed += summary["status"] == "completed"
    wall_time = max(0.0, time.perf_counter() - started)
    return {
        "schema_version": 1,
        "logical_runs": logical_runs,
        "remote_attempts": logical_runs * len(WORKER_IDS),
        "worker_sessions": len(WORKER_IDS),
        "completed_runs": completed,
        "wall_time_seconds": round(wall_time, 6),
        "claim": "reference_workload_not_production_slo",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-dir", type=Path)
    parser.add_argument("--workflow", type=Path, default=WORKFLOW_PATH)
    parser.add_argument("--soak-runs", type=int, default=0)
    arguments = parser.parse_args()
    if arguments.runtime_dir is not None:
        root = arguments.runtime_dir
        result = (
            run_soak(
                root,
                logical_runs=arguments.soak_runs,
                workflow_path=arguments.workflow,
            )
            if arguments.soak_runs
            else run_demo(root, workflow_path=arguments.workflow)
        )
    else:
        with tempfile.TemporaryDirectory(
            prefix="xagent-distributed-reference-"
        ) as directory:
            root = Path(directory)
            result = (
                run_soak(
                    root,
                    logical_runs=arguments.soak_runs,
                    workflow_path=arguments.workflow,
                )
                if arguments.soak_runs
                else run_demo(root, workflow_path=arguments.workflow)
            )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
