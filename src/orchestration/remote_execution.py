"""Secure composition for remote Tool Activity execution.

The control-plane admitter is the only component in this module that may hold
the durable executor, Worker authorization gate, and Artifact broker together.
Workers receive path-free plans and opaque grants; their runtime proof remains
an untrusted candidate until the injected control-plane trust anchor verifies
it.
"""

from __future__ import annotations

import hashlib
import json
import math
import secrets
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Iterable, Mapping, Protocol, runtime_checkable

from .artifact_broker import (
    ArtifactDescriptor,
    ArtifactGrantBroker,
    ArtifactOutputHandle,
    ArtifactReadGrant,
)
from .artifacts import ArtifactKind, ArtifactRef, ArtifactSensitivity
from .executor import (
    ActivityExecutionResult,
    ActivityExecutionConflict,
    PreauthorizedActivityExecution,
    PreparedActivityExecution,
    TrustedActivityExecutor,
)
from .models import AttemptStatus, EventRecord
from .policy import ApprovalGrant, Capability, EffectClass, PolicyOutcome
from .remote_control import (
    RemoteAdmissionAuthorization,
    RemoteCompletionCandidate,
    WorkerRegistration,
)
from .remote_protocol import (
    MAX_OUTPUT_HANDLES,
    AuthenticatedWorker,
    ClaimBinding,
    ExecutionAuthority,
    ExecutionAuthorization,
    ExecutionAuthorizationBinding,
    RemoteExecutionPlan,
    RemoteProtocolError,
    RemoteRuntimeProof,
    canonical_digest,
    grant_binding_digest,
    runtime_binding_digest,
)
from .remote_execution_journal import (
    RemoteExecutionBindingRecord,
    RemoteExecutionJournal,
    RemoteExecutionJournalError,
)
from .remote_worker import (
    RemoteExecutionContext,
    RemoteExecutionGrant,
    RemoteExecutionOutcome,
)
from .sandbox import (
    EnvironmentBinding,
    ResourceLimits,
    SandboxOutcome,
    SandboxProfile,
    SandboxReceipt,
    SecurityLevel,
)
from .scheduler import (
    ActivityAdmissionCandidate,
    ActivityClaim,
    DurableScheduler,
    SchedulerError,
)
from .worker_security import (
    WorkerAuthorization,
    WorkerAuthorizationGate,
)

MAX_PREPARED_REMOTE_EXECUTIONS = 4_096
MAX_PREPARED_TTL_SECONDS = 5 * 60.0
MAX_PREPARED_MATERIALIZED_BYTES = 64 * 1024 * 1024
MAX_RUNTIME_ATTESTATION_TTL_SECONDS = 5 * 60.0
_TOOL_ACTIVITY_KINDS = frozenset({"tool"})


class SecureRemoteExecutionError(RuntimeError):
    """Fail-closed control-plane rejection with a bounded safe code."""

    def __init__(self, reason_code: str) -> None:
        if (
            not isinstance(reason_code, str)
            or not reason_code
            or len(reason_code) > 128
            or any(
                not (
                    character.islower()
                    or character.isdigit()
                    or character in "_.:-"
                )
                for character in reason_code
            )
        ):
            raise ValueError("invalid secure remote execution reason code")
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class RemoteExecutionPreparation:
    """Trusted host-side inputs used to build one exact remote execution."""

    argv: tuple[str, ...]
    cwd: str
    container_cwd: str
    profile: SandboxProfile
    approval_grant: ApprovalGrant | None = None
    capabilities: tuple[Capability | str, ...] = ()
    resource_locks: tuple[str, ...] = ()
    sensitive_keys: tuple[str, ...] = ()
    limits: ResourceLimits | None = None
    input_artifact_refs: tuple[ArtifactRef, ...] = ()
    script_artifact_ref: ArtifactRef | None = None
    environment: tuple[EnvironmentBinding, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.argv, tuple) or not self.argv:
            raise ValueError("remote preparation requires argv")
        if not isinstance(self.cwd, str) or not self.cwd:
            raise ValueError("remote preparation requires a host cwd")
        if (
            not isinstance(self.container_cwd, str)
            or not self.container_cwd.startswith("/")
        ):
            raise ValueError("remote preparation requires an absolute container cwd")
        if not isinstance(self.profile, SandboxProfile):
            raise TypeError("profile must be SandboxProfile")
        object.__setattr__(self, "capabilities", tuple(self.capabilities))
        object.__setattr__(self, "resource_locks", tuple(self.resource_locks))
        object.__setattr__(self, "sensitive_keys", tuple(self.sensitive_keys))
        object.__setattr__(
            self,
            "input_artifact_refs",
            tuple(self.input_artifact_refs),
        )
        object.__setattr__(self, "environment", tuple(self.environment))


@runtime_checkable
class RemoteExecutionPlanResolver(Protocol):
    """Resolve trusted execution controls; Workflow config is not a worker plan."""

    def resolve(
        self,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
    ) -> RemoteExecutionPreparation: ...


@runtime_checkable
class WorkerPresentationResolver(Protocol):
    """Recover an opaque transport presentation from trusted server context."""

    def resolve(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
    ) -> object: ...


@dataclass(frozen=True, slots=True)
class VerifiedRemoteRuntimeAttestation:
    """Control-plane trust-anchor result for one registered Worker session."""

    worker_id: str
    instance_id: str
    identity_digest: str
    runtime_version: str
    runtime_attestation_digest: str
    security_level: SecurityLevel
    verifier_id: str
    issued_at: float
    expires_at: float

    def __post_init__(self) -> None:
        for field_name in (
            "worker_id",
            "instance_id",
            "runtime_version",
            "verifier_id",
        ):
            value = getattr(self, field_name)
            if (
                not isinstance(value, str)
                or not value.strip()
                or len(value) > 256
                or any(ord(character) < 32 for character in value)
            ):
                raise ValueError(f"{field_name} is invalid")
        for field_name in (
            "identity_digest",
            "runtime_attestation_digest",
        ):
            _digest(getattr(self, field_name), field_name)
        object.__setattr__(
            self,
            "security_level",
            SecurityLevel(self.security_level),
        )
        issued_at = _timestamp(self.issued_at)
        expires_at = _timestamp(self.expires_at)
        object.__setattr__(self, "issued_at", issued_at)
        object.__setattr__(self, "expires_at", expires_at)
        if (
            expires_at <= issued_at
            or expires_at - issued_at > MAX_RUNTIME_ATTESTATION_TTL_SECONDS
        ):
            raise ValueError("runtime attestation validity window is invalid")

    def validate_for(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        *,
        now: float,
    ) -> None:
        if (
            self.worker_id != identity.worker_id
            or self.worker_id != registration.worker_id
            or self.instance_id != registration.instance_id
            or self.identity_digest != identity.identity_digest
            or self.identity_digest != registration.identity_digest
            or self.runtime_version != registration.runtime_version
        ):
            raise SecureRemoteExecutionError(
                "runtime_attestation_binding_mismatch"
            )
        if self.security_level is not SecurityLevel.CONTAINER:
            raise SecureRemoteExecutionError("strong_runtime_required")
        current = _timestamp(now)
        if self.issued_at > current or current >= self.expires_at:
            raise SecureRemoteExecutionError("runtime_attestation_expired")


@runtime_checkable
class RuntimeProofVerifier(Protocol):
    """Injected control-plane trust anchor; telemetry is never a verifier."""

    production_security_ready: bool

    def attest(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        *,
        now: float,
    ) -> VerifiedRemoteRuntimeAttestation: ...

    def verify(
        self,
        proof: RemoteRuntimeProof,
        *,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        claim: ClaimBinding,
        authorization: ExecutionAuthority,
        attestation: VerifiedRemoteRuntimeAttestation,
        now: float,
    ) -> bool: ...


@runtime_checkable
class WorkerPreflightAuthorizer(Protocol):
    """Trusted coarse authorization used before a durable claim is created."""

    production_security_ready: bool

    def authorize(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        *,
        pool_id: str,
        presentation: object,
        now: float,
    ) -> bool: ...


@dataclass(slots=True)
class _PreparedRemoteExecution:
    scheduler: DurableScheduler
    prepared: PreparedActivityExecution
    worker_authorization: WorkerAuthorization
    runtime_attestation: VerifiedRemoteRuntimeAttestation
    authorization: ExecutionAuthority
    identity: AuthenticatedWorker
    registration: WorkerRegistration
    idle_expires_at: float
    reservation_id: str
    materialized_bytes: int
    staged_output_count: int = 0
    staged_output_bytes: int = 0


@dataclass(slots=True)
class _RemoteAdmissionTicketRecord:
    ticket_digest: str
    candidate_digest: str
    scheduler: DurableScheduler
    preparation: RemoteExecutionPreparation
    preview: PreauthorizedActivityExecution
    worker_authorization: WorkerAuthorization | None
    runtime_attestation: VerifiedRemoteRuntimeAttestation | None
    identity: AuthenticatedWorker
    registration: WorkerRegistration
    expires_at: float
    reservation_id: str
    materialized_bytes: int
    state: str = "prepared"


@dataclass(frozen=True, slots=True)
class StagedRemoteOutput:
    """Path-free finalized identity plus the opaque completion handle."""

    handle: ArtifactOutputHandle
    descriptor: ArtifactDescriptor


@dataclass(frozen=True, slots=True)
class RemoteExecutionRecoveryReport:
    """Bounded maintenance result for digest-only recovery evidence."""

    scanned: int
    retained: int
    discarded: int


@dataclass(frozen=True, slots=True)
class RemoteOutputPayload:
    """Worker-local bytes plus their intended non-secret classification."""

    content: bytes = field(repr=False)
    kind: ArtifactKind = ArtifactKind.TOOL_RESULT
    sensitivity: ArtifactSensitivity = ArtifactSensitivity.INTERNAL
    media_type: str = "application/octet-stream"

    def __post_init__(self) -> None:
        if not isinstance(self.content, bytes):
            raise ValueError("remote output content must be bytes")
        object.__setattr__(self, "kind", ArtifactKind(self.kind))
        object.__setattr__(
            self,
            "sensitivity",
            ArtifactSensitivity(self.sensitivity),
        )
        if (
            not isinstance(self.media_type, str)
            or not self.media_type
            or len(self.media_type) > 255
        ):
            raise ValueError("remote output media_type is invalid")


@dataclass(frozen=True, slots=True)
class RemoteSandboxExecution:
    """Runtime-native result before broker staging and proof signing."""

    backend_id: str
    sandbox_spec_digest: str
    outcome: SandboxOutcome
    exit_code: int | None
    timed_out: bool
    outputs: tuple[RemoteOutputPayload, ...] = ()
    error_code: str | None = None
    runtime_evidence: object = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.backend_id, str)
            or not self.backend_id
            or len(self.backend_id) > 256
        ):
            raise ValueError("remote sandbox backend_id is invalid")
        _digest(self.sandbox_spec_digest, "sandbox_spec_digest")
        object.__setattr__(self, "outcome", SandboxOutcome(self.outcome))
        if self.exit_code is not None and (
            isinstance(self.exit_code, bool)
            or not isinstance(self.exit_code, int)
            or not -65535 <= self.exit_code <= 65535
        ):
            raise ValueError("remote sandbox exit_code is invalid")
        if not isinstance(self.timed_out, bool):
            raise ValueError("remote sandbox timed_out is invalid")
        outputs = tuple(self.outputs)
        if (
            len(outputs) > 64
            or any(not isinstance(item, RemoteOutputPayload) for item in outputs)
        ):
            raise ValueError("remote sandbox outputs are invalid")
        object.__setattr__(self, "outputs", outputs)
        if self.outcome is SandboxOutcome.SUCCEEDED:
            if not outputs or self.error_code is not None:
                raise ValueError("successful remote sandbox result is invalid")
        elif outputs:
            raise ValueError("failed remote sandbox result cannot carry outputs")
        if self.error_code is not None and (
            not isinstance(self.error_code, str)
            or not self.error_code
            or len(self.error_code) > 128
        ):
            raise ValueError("remote sandbox error_code is invalid")


@runtime_checkable
class WorkerArtifactTransport(Protocol):
    """Authenticated path-free broker transport; it exposes no Store object."""

    production_security_ready: bool

    def fetch(self, grant: ArtifactReadGrant) -> bytes: ...

    def stage(
        self,
        grant: RemoteExecutionGrant,
        output: RemoteOutputPayload,
    ) -> StagedRemoteOutput: ...


@runtime_checkable
class RemoteWorkerSandboxAdapter(Protocol):
    """Injected OCI/runtime adapter that returns native evidence."""

    production_security_ready: bool
    runtime_attestation_digest: str
    security_level: SecurityLevel

    def execute(
        self,
        grant: RemoteExecutionGrant,
        input_payloads: tuple[bytes, ...],
        context: RemoteExecutionContext,
    ) -> RemoteSandboxExecution: ...

    def cancel(
        self,
        grant: RemoteExecutionGrant,
        context: RemoteExecutionContext,
    ) -> RemoteSandboxExecution: ...


@runtime_checkable
class RuntimeProofSigner(Protocol):
    """Worker-local signer backed by deployment runtime evidence."""

    production_security_ready: bool
    runtime_attestation_digest: str

    def sign(
        self,
        *,
        signed_binding_digest: str,
        sandbox_spec_digest: str,
        sandbox_receipt: Mapping[str, Any],
        runtime_evidence: object,
    ) -> RemoteRuntimeProof: ...


class SecureRemoteExecutionAdapter:
    """Path-free worker composition implementing RemoteExecutionAdapter."""

    def __init__(
        self,
        artifact_transport: WorkerArtifactTransport,
        sandbox_adapter: RemoteWorkerSandboxAdapter,
        proof_signer: RuntimeProofSigner,
    ) -> None:
        if not isinstance(artifact_transport, WorkerArtifactTransport):
            raise TypeError("artifact_transport is invalid")
        if not isinstance(sandbox_adapter, RemoteWorkerSandboxAdapter):
            raise TypeError("sandbox_adapter is invalid")
        if not isinstance(proof_signer, RuntimeProofSigner):
            raise TypeError("proof_signer is invalid")
        if (
            sandbox_adapter.runtime_attestation_digest
            != proof_signer.runtime_attestation_digest
        ):
            raise ValueError("worker runtime attestation bindings differ")
        _digest(
            sandbox_adapter.runtime_attestation_digest,
            "runtime_attestation_digest",
        )
        self.artifact_transport = artifact_transport
        self.sandbox_adapter = sandbox_adapter
        self.proof_signer = proof_signer

    @property
    def production_security_ready(self) -> bool:
        return (
            getattr(
                self.artifact_transport,
                "production_security_ready",
                None,
            )
            is True
            and getattr(
                self.sandbox_adapter,
                "production_security_ready",
                None,
            )
            is True
            and getattr(
                self.proof_signer,
                "production_security_ready",
                None,
            )
            is True
            and self.sandbox_adapter.security_level
            is SecurityLevel.CONTAINER
        )

    @property
    def runtime_attestation_digest(self) -> str:
        return self.sandbox_adapter.runtime_attestation_digest

    @property
    def supported_activity_kinds(self) -> frozenset[str]:
        return _TOOL_ACTIVITY_KINDS

    def prepare(self, assignment) -> RemoteExecutionGrant:
        authorization = assignment.claim
        if (
            assignment.activity_kind not in self.supported_activity_kinds
            or authorization.runtime_attestation_digest
            != self.runtime_attestation_digest
            or assignment.execution_plan.plan_digest
            != authorization.execution_plan_digest
        ):
            raise SecureRemoteExecutionError("worker_assignment_mismatch")
        return RemoteExecutionGrant(
            assignment=assignment,
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
        )

    def execute(
        self,
        grant: RemoteExecutionGrant,
        context: RemoteExecutionContext,
    ) -> RemoteExecutionOutcome:
        self._require_ready(grant)
        inputs = tuple(
            self._fetch_input(input_grant)
            for input_grant in grant.assignment.input_grants
        )
        execution = self.sandbox_adapter.execute(grant, inputs, context)
        return self._seal_execution(grant, execution)

    def cancel(
        self,
        grant: RemoteExecutionGrant,
        context: RemoteExecutionContext,
    ) -> RemoteRuntimeProof:
        self._require_ready(grant)
        execution = self.sandbox_adapter.cancel(grant, context)
        outcome = self._seal_execution(grant, execution)
        if outcome.outcome != AttemptStatus.CANCELLED.value:
            raise SecureRemoteExecutionError(
                "worker_cancellation_receipt_mismatch"
            )
        return outcome.runtime_proof

    def verify_candidate(
        self,
        grant: RemoteExecutionGrant,
        candidate: RemoteExecutionOutcome,
    ) -> RemoteExecutionOutcome:
        self._require_ready(grant)
        if not isinstance(candidate, RemoteExecutionOutcome):
            raise SecureRemoteExecutionError("invalid_worker_candidate")
        try:
            candidate.runtime_proof.validate_binding(
                grant.assignment.claim,
                outcome=candidate.outcome,
                output_handles=candidate.output_handles,
            )
        except (RemoteProtocolError, TypeError, ValueError) as exc:
            raise SecureRemoteExecutionError(
                "worker_candidate_binding_mismatch"
            ) from exc
        return candidate

    def _fetch_input(self, grant: ArtifactReadGrant) -> bytes:
        payload = self.artifact_transport.fetch(grant)
        if not isinstance(payload, bytes):
            raise SecureRemoteExecutionError("invalid_input_payload")
        descriptor = grant.descriptor
        if (
            descriptor is None
            or len(payload) != descriptor.size
            or hashlib.sha256(payload).hexdigest() != descriptor.sha256
        ):
            raise SecureRemoteExecutionError("input_artifact_tamper")
        return payload

    def _seal_execution(
        self,
        grant: RemoteExecutionGrant,
        execution: RemoteSandboxExecution,
    ) -> RemoteExecutionOutcome:
        if not isinstance(execution, RemoteSandboxExecution):
            raise SecureRemoteExecutionError("invalid_runtime_result")
        staged = tuple(
            self.artifact_transport.stage(grant, output)
            for output in execution.outputs
        )
        handles = tuple(item.handle for item in staged)
        receipt = _worker_sandbox_receipt(
            grant,
            execution,
            tuple(item.descriptor for item in staged),
        )
        remote_outcome = _remote_outcome(execution.outcome)
        signed_binding = runtime_binding_digest(
            grant.assignment.claim,
            outcome=remote_outcome,
            output_handles=handles,
            sandbox_receipt_digest=canonical_digest(receipt),
            sandbox_spec_digest=execution.sandbox_spec_digest,
        )
        proof = self.proof_signer.sign(
            signed_binding_digest=signed_binding,
            sandbox_spec_digest=execution.sandbox_spec_digest,
            sandbox_receipt=receipt,
            runtime_evidence=execution.runtime_evidence,
        )
        if (
            not isinstance(proof, RemoteRuntimeProof)
            or proof.signed_binding_digest != signed_binding
            or proof.sandbox_spec_digest != execution.sandbox_spec_digest
            or dict(proof.sandbox_receipt) != receipt
        ):
            raise SecureRemoteExecutionError("runtime_signer_mismatch")
        proof.validate_binding(
            grant.assignment.claim,
            outcome=remote_outcome,
            output_handles=handles,
        )
        error_code = execution.error_code
        if remote_outcome not in {
            AttemptStatus.SUCCEEDED.value,
            AttemptStatus.CANCELLED.value,
        } and error_code is None:
            error_code = "runtime_failure"
        return RemoteExecutionOutcome(
            outcome=remote_outcome,
            output_handles=handles,
            runtime_proof=proof,
            error_class=(
                None
                if remote_outcome
                in {
                    AttemptStatus.SUCCEEDED.value,
                    AttemptStatus.CANCELLED.value,
                }
                else "sandbox"
            ),
            error_code=error_code,
        )

    def _require_ready(self, grant: RemoteExecutionGrant) -> None:
        if (
            not self.production_security_ready
            or not isinstance(grant, RemoteExecutionGrant)
            or grant.assignment.activity_kind
            not in self.supported_activity_kinds
            or grant.runtime_attestation_digest
            != self.runtime_attestation_digest
        ):
            raise SecureRemoteExecutionError("worker_security_not_ready")


class SecureRemoteAssignmentAdmitter:
    """RemoteAssignmentAdmitter backed by durable policy and trusted proofs."""

    secure_two_phase_admission = True

    def __init__(
        self,
        executor: TrustedActivityExecutor,
        worker_authorization_gate: WorkerAuthorizationGate,
        artifact_broker: ArtifactGrantBroker,
        plan_resolver: RemoteExecutionPlanResolver,
        presentation_resolver: WorkerPresentationResolver,
        runtime_proof_verifier: RuntimeProofVerifier,
        preflight_authorizer: WorkerPreflightAuthorizer,
        *,
        pool_resolver: Callable[[WorkerRegistration], str],
        recovery_journal: RemoteExecutionJournal | None = None,
        clock: Callable[[], float] = time.time,
        prepared_ttl_seconds: float = MAX_PREPARED_TTL_SECONDS,
        maximum_prepared: int = 1_024,
    ) -> None:
        if not isinstance(executor, TrustedActivityExecutor):
            raise TypeError("executor must be TrustedActivityExecutor")
        if not isinstance(worker_authorization_gate, WorkerAuthorizationGate):
            raise TypeError("worker_authorization_gate is invalid")
        if not isinstance(artifact_broker, ArtifactGrantBroker):
            raise TypeError("artifact_broker is invalid")
        if not isinstance(plan_resolver, RemoteExecutionPlanResolver):
            raise TypeError("plan_resolver is invalid")
        if not isinstance(presentation_resolver, WorkerPresentationResolver):
            raise TypeError("presentation_resolver is invalid")
        if not isinstance(runtime_proof_verifier, RuntimeProofVerifier):
            raise TypeError("runtime_proof_verifier is invalid")
        if not isinstance(preflight_authorizer, WorkerPreflightAuthorizer):
            raise TypeError("preflight_authorizer is invalid")
        if not callable(pool_resolver) or not callable(clock):
            raise TypeError("pool_resolver and clock must be callable")
        if recovery_journal is not None and not isinstance(
            recovery_journal,
            RemoteExecutionJournal,
        ):
            raise TypeError(
                "recovery_journal must be a RemoteExecutionJournal"
            )
        ttl = _timestamp(prepared_ttl_seconds)
        if ttl <= 0 or ttl > MAX_PREPARED_TTL_SECONDS:
            raise ValueError("prepared_ttl_seconds exceeds policy")
        if (
            isinstance(maximum_prepared, bool)
            or not isinstance(maximum_prepared, int)
            or not 1 <= maximum_prepared <= MAX_PREPARED_REMOTE_EXECUTIONS
        ):
            raise ValueError("maximum_prepared exceeds policy")
        self.executor = executor
        self.worker_authorization_gate = worker_authorization_gate
        self.artifact_broker = artifact_broker
        self.plan_resolver = plan_resolver
        self.presentation_resolver = presentation_resolver
        self.runtime_proof_verifier = runtime_proof_verifier
        self.preflight_authorizer = preflight_authorizer
        self.pool_resolver = pool_resolver
        self.recovery_journal = (
            recovery_journal or RemoteExecutionJournal()
        )
        self._clock = clock
        self._prepared_ttl_seconds = ttl
        self._maximum_prepared = maximum_prepared
        self._lock = threading.Lock()
        self._prepared: dict[str, _PreparedRemoteExecution] = {}
        self._admission_tickets: dict[str, _RemoteAdmissionTicketRecord] = {}
        self._registry_reservations: dict[str, int] = {}
        self._registry_materialized_bytes = 0
        self._claim_locks: dict[str, threading.Lock] = {}

    @property
    def production_security_ready(self) -> bool:
        return (
            getattr(
                self.runtime_proof_verifier,
                "production_security_ready",
                None,
            )
            is True
            and getattr(
                self.preflight_authorizer,
                "production_security_ready",
                None,
            )
            is True
        )

    @property
    def supported_activity_kinds(self) -> frozenset[str]:
        return _TOOL_ACTIVITY_KINDS

    @property
    def durable_recovery_ready(self) -> bool:
        """Whether claim and Artifact authority survive process restart."""

        return (
            self.recovery_journal.durable
            and self.artifact_broker.durable_recovery_ready
        )

    def reconcile_recovery_journal(
        self,
        *,
        limit: int | None = None,
    ) -> RemoteExecutionRecoveryReport:
        """Delete only terminal, missing, or durably fenced generations."""

        try:
            records = self.recovery_journal.list_records(limit=limit)
        except (KeyboardInterrupt, SystemExit):
            raise
        except RemoteExecutionJournalError:
            raise SecureRemoteExecutionError(
                "recovery_journal_unavailable"
            ) from None
        retained = 0
        discarded = 0
        for record in records:
            attempt = self.executor.store.get_attempt(record.attempt_id)
            idempotency = (
                None
                if attempt is None
                else self.executor.store.get_idempotency(
                    record.run_id,
                    attempt.idempotency_key,
                )
            )
            live = (
                attempt is not None
                and attempt.run_id == record.run_id
                and attempt.node_id == record.node_id
                and attempt.status
                in {AttemptStatus.CLAIMED, AttemptStatus.RUNNING}
                and idempotency is not None
                and idempotency.claim_count == record.fencing_token
                and hashlib.sha256(
                    idempotency.claim_token.encode("utf-8")
                ).hexdigest()
                == record.claim_token_digest
            )
            if live:
                retained += 1
                continue
            try:
                removed = self.recovery_journal.discard(
                    record.run_id,
                    record.attempt_id,
                    record.fencing_token,
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except RemoteExecutionJournalError:
                raise SecureRemoteExecutionError(
                    "recovery_journal_unavailable"
                ) from None
            discarded += int(removed)
        return RemoteExecutionRecoveryReport(
            scanned=len(records),
            retained=retained,
            discarded=discarded,
        )

    def preflight(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
    ) -> None:
        """Reject incompatible or unauthorized sessions before ``claim_next``."""

        self._require_ready()
        if (
            not isinstance(identity, AuthenticatedWorker)
            or not isinstance(registration, WorkerRegistration)
            or not isinstance(scheduler, DurableScheduler)
            or scheduler is not self.executor.scheduler
            or registration.worker_id != identity.worker_id
            or registration.tenant_id != identity.tenant_id
            or registration.identity_digest != identity.identity_digest
            or not frozenset(registration.activity_kinds).issubset(
                self.supported_activity_kinds
            )
            or not {
                f"activity.{kind}"
                for kind in registration.activity_kinds
            }.issubset(registration.capabilities)
        ):
            raise SecureRemoteExecutionError("control_context_mismatch")
        now = self._now()
        presentation = self.presentation_resolver.resolve(
            identity,
            registration,
        )
        pool_id = self.pool_resolver(registration)
        try:
            allowed = self.preflight_authorizer.authorize(
                identity,
                registration,
                scheduler,
                pool_id=pool_id,
                presentation=presentation,
                now=now,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise SecureRemoteExecutionError(
                "worker_preflight_denied"
            ) from None
        if allowed is not True:
            raise SecureRemoteExecutionError("worker_preflight_denied")
        attestation = self.runtime_proof_verifier.attest(
            identity,
            registration,
            now=now,
        )
        if not isinstance(attestation, VerifiedRemoteRuntimeAttestation):
            raise SecureRemoteExecutionError(
                "runtime_attestation_unverified"
            )
        attestation.validate_for(identity, registration, now=now)

    def prepare_admission(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        candidate: ActivityAdmissionCandidate,
    ) -> RemoteAdmissionAuthorization:
        """Create a bounded, process-local, single-use preclaim ticket."""

        self._require_ready()
        if not isinstance(candidate, ActivityAdmissionCandidate):
            raise SecureRemoteExecutionError("invalid_admission_candidate")
        claim = candidate.claim
        if (
            claim.claim_token
            or claim.fencing_token != 0
            or claim.lease_expires_at != 0.0
        ):
            raise SecureRemoteExecutionError("invalid_admission_candidate")
        self._validate_control_context(
            identity,
            registration,
            scheduler,
            claim,
            require_durable_claim=False,
        )
        now = self._now()
        self._purge_registry(now)
        with self._claim_guard(claim.attempt_id):
            resolved = self.plan_resolver.resolve(scheduler, claim)
            if not isinstance(resolved, RemoteExecutionPreparation):
                raise SecureRemoteExecutionError("invalid_execution_plan")
            script_ref = resolved.script_artifact_ref
            if script_ref is not None and not isinstance(
                script_ref,
                ArtifactRef,
            ):
                raise SecureRemoteExecutionError("invalid_execution_plan")
            materialized_bytes = (
                0 if script_ref is None else script_ref.size
            )
            reservation_id = self._reserve_registry(materialized_bytes)
            try:
                return self._prepare_reserved_admission(
                    identity,
                    registration,
                    scheduler,
                    candidate,
                    resolved,
                    reservation_id=reservation_id,
                    materialized_bytes=materialized_bytes,
                    now=now,
                )
            except BaseException:
                self._release_registry_reservation(reservation_id)
                raise

    def _prepare_reserved_admission(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        candidate: ActivityAdmissionCandidate,
        resolved: RemoteExecutionPreparation,
        *,
        reservation_id: str,
        materialized_bytes: int,
        now: float,
    ) -> RemoteAdmissionAuthorization:
        claim = candidate.claim
        worker_authorization: WorkerAuthorization | None = None
        runtime_attestation: VerifiedRemoteRuntimeAttestation | None = None
        try:
            preview = self.executor.preauthorize_execution(
                claim,
                argv=resolved.argv,
                cwd=resolved.cwd,
                profile=resolved.profile,
                approval_grant=resolved.approval_grant,
                capabilities=resolved.capabilities,
                resource_locks=resolved.resource_locks,
                sensitive_keys=resolved.sensitive_keys,
                limits=resolved.limits,
                input_artifact_refs=resolved.input_artifact_refs,
                script_artifact_ref=resolved.script_artifact_ref,
                environment=resolved.environment,
            )
            if len(preview.materialized_script or b"") != materialized_bytes:
                raise SecureRemoteExecutionError(
                    "invalid_materialized_script"
                )
            if preview.decision.outcome is PolicyOutcome.ALLOW:
                presentation = self.presentation_resolver.resolve(
                    identity,
                    registration,
                )
                worker_authorization = (
                    self.worker_authorization_gate.authorize(
                        presentation,
                        tenant_id=identity.tenant_id,
                        pool_id=self.pool_resolver(registration),
                        action=preview.action,
                        expected_transport_binding_digest=(
                            registration.session_binding_digest
                        ),
                        expected_worker_id=identity.worker_id,
                    )
                )
                runtime_attestation = self.runtime_proof_verifier.attest(
                    identity,
                    registration,
                    now=now,
                )
        except (KeyboardInterrupt, SystemExit):
            if worker_authorization is not None:
                self.worker_authorization_gate.discard(
                    worker_authorization
                )
            raise
        except SecureRemoteExecutionError:
            if worker_authorization is not None:
                self.worker_authorization_gate.discard(
                    worker_authorization
                )
            raise
        except BaseException:
            if worker_authorization is not None:
                self.worker_authorization_gate.discard(
                    worker_authorization
                )
            raise SecureRemoteExecutionError(
                "candidate_authorization_denied"
            ) from None
        if preview.decision.outcome is PolicyOutcome.ALLOW:
            if (
                worker_authorization is None
                or not isinstance(
                    runtime_attestation,
                    VerifiedRemoteRuntimeAttestation,
                )
            ):
                if worker_authorization is not None:
                    self.worker_authorization_gate.discard(
                        worker_authorization
                    )
                raise SecureRemoteExecutionError(
                    "runtime_attestation_unverified"
                )
            try:
                runtime_attestation.validate_for(
                    identity,
                    registration,
                    now=now,
                )
            except BaseException:
                self.worker_authorization_gate.discard(
                    worker_authorization
                )
                raise
            expires_at = min(
                now + self._prepared_ttl_seconds,
                worker_authorization.expires_at,
                runtime_attestation.expires_at,
            )
        else:
            expires_at = now + self._prepared_ttl_seconds
        if expires_at <= now:
            if worker_authorization is not None:
                self.worker_authorization_gate.discard(
                    worker_authorization
                )
            raise SecureRemoteExecutionError(
                "candidate_authorization_expired"
            )
        ticket_id = secrets.token_urlsafe(32)
        ticket_digest = hashlib.sha256(
            ticket_id.encode("utf-8")
        ).hexdigest()
        record = _RemoteAdmissionTicketRecord(
            ticket_digest=ticket_digest,
            candidate_digest=candidate.candidate_digest,
            scheduler=scheduler,
            preparation=resolved,
            preview=preview,
            worker_authorization=worker_authorization,
            runtime_attestation=runtime_attestation,
            identity=identity,
            registration=registration,
            expires_at=expires_at,
            reservation_id=reservation_id,
            materialized_bytes=materialized_bytes,
        )
        with self._lock:
            if ticket_digest in self._admission_tickets:
                registry_error = "admission_ticket_collision"
            else:
                self._admission_tickets[ticket_digest] = record
                registry_error = None
        if registry_error is not None:
            if worker_authorization is not None:
                self.worker_authorization_gate.discard(
                    worker_authorization
                )
            raise SecureRemoteExecutionError(registry_error)
        return RemoteAdmissionAuthorization(
            ticket_id=ticket_id,
            candidate_digest=candidate.candidate_digest,
            expires_at=expires_at,
            policy_binding=preview.durable_policy_binding,
        )

    def begin_admission(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        candidate: ActivityAdmissionCandidate,
        admission: RemoteAdmissionAuthorization,
    ) -> float:
        """Atomically reserve a fresh ticket for the Store linearization."""

        if not isinstance(admission, RemoteAdmissionAuthorization):
            raise SecureRemoteExecutionError("invalid_admission_ticket")
        ticket_digest = hashlib.sha256(
            admission.ticket_id.encode("utf-8")
        ).hexdigest()
        now = self._now()
        with self._lock:
            record = self._admission_tickets.get(ticket_digest)
            if (
                record is None
                or record.state != "prepared"
                or record.ticket_digest != ticket_digest
                or record.candidate_digest != candidate.candidate_digest
                or admission.candidate_digest != candidate.candidate_digest
                or admission.expires_at != record.expires_at
                or now >= record.expires_at
                or record.identity != identity
                or record.registration != registration
                or record.scheduler is not scheduler
                or admission.policy_binding
                != record.preview.durable_policy_binding
            ):
                raise SecureRemoteExecutionError(
                    "invalid_admission_ticket"
                )
            record.state = "committing"
        try:
            if record.preview.decision.outcome is PolicyOutcome.ALLOW:
                if (
                    record.runtime_attestation is None
                    or record.worker_authorization is None
                ):
                    raise SecureRemoteExecutionError(
                        "invalid_admission_ticket"
                    )
                record.runtime_attestation.validate_for(
                    identity,
                    registration,
                    now=now,
                )
                if not self.worker_authorization_gate.verify(
                    record.worker_authorization,
                    now=now,
                ):
                    raise SecureRemoteExecutionError(
                        "candidate_authorization_expired"
                    )
        except BaseException:
            self.cancel_admission(admission)
            raise
        return record.expires_at

    def cancel_admission(
        self,
        admission: RemoteAdmissionAuthorization,
    ) -> None:
        """Cancel an unconsumed ticket after a candidate/claim CAS loss."""

        if not isinstance(admission, RemoteAdmissionAuthorization):
            return
        ticket_digest = hashlib.sha256(
            admission.ticket_id.encode("utf-8")
        ).hexdigest()
        with self._lock:
            record = self._admission_tickets.pop(ticket_digest, None)
            if record is not None:
                record.state = "cancelled"
                self._release_registry_reservation_locked(
                    record.reservation_id
                )
        if record is not None and record.worker_authorization is not None:
            self.worker_authorization_gate.discard(
                record.worker_authorization
            )

    def commit_admission(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        candidate: ActivityAdmissionCandidate,
        claim: ActivityClaim,
        admission: RemoteAdmissionAuthorization,
        policy_event: EventRecord,
    ) -> ExecutionAuthorization:
        """Consume one exact ticket and install post-claim broker grants."""

        if not isinstance(admission, RemoteAdmissionAuthorization):
            raise SecureRemoteExecutionError("invalid_admission_ticket")
        ticket_digest = hashlib.sha256(
            admission.ticket_id.encode("utf-8")
        ).hexdigest()
        now = self._now()
        with self._lock:
            record = self._admission_tickets.get(ticket_digest)
            if (
                record is None
                or record.state != "committing"
                or record.preview.decision.outcome is not PolicyOutcome.ALLOW
                or record.worker_authorization is None
                or record.runtime_attestation is None
                or record.ticket_digest != ticket_digest
                or record.candidate_digest != candidate.candidate_digest
                or admission.candidate_digest != candidate.candidate_digest
                or now >= record.expires_at
                or record.identity != identity
                or record.registration != registration
                or record.scheduler is not scheduler
                or admission.policy_binding
                != record.preview.durable_policy_binding
            ):
                raise SecureRemoteExecutionError(
                    "invalid_admission_ticket"
                )
        try:
            prepared = self.executor.activate_preauthorized(
                record.preview,
                claim,
                policy_event,
            )
            input_grants = self._issue_input_grants(
                record.worker_authorization,
                identity=identity,
                claim=claim,
                prepared=prepared,
            )
            execution_plan = RemoteExecutionPlan(
                argv=prepared.request.argv,
                container_cwd=record.preparation.container_cwd,
                limits=prepared.request.limits.to_dict(),
                profile_id=prepared.profile.profile_id,
                profile_digest=prepared.profile.profile_digest,
                policy_version=self.executor.policy.policy_version,
                request_digest=prepared.request.request_digest,
                capabilities=tuple(
                    capability.name
                    for capability in prepared.action.capabilities
                ),
            )
            authorization = ExecutionAuthorization(
                action_digest=prepared.action.action_digest,
                authorization_digest=(
                    record.worker_authorization.authorization_digest
                ),
                profile_digest=prepared.profile.profile_digest,
                request_digest=prepared.request.request_digest,
                session_binding_digest=registration.session_binding_digest,
                grant_binding_digest=grant_binding_digest(
                    input_grants,
                    (),
                ),
                execution_plan_digest=execution_plan.plan_digest,
                runtime_attestation_digest=(
                    record.runtime_attestation.runtime_attestation_digest
                ),
                execution_plan=execution_plan,
                input_grants=input_grants,
                output_grants=(),
            )
            prepared_record = _PreparedRemoteExecution(
                scheduler=scheduler,
                prepared=prepared,
                worker_authorization=record.worker_authorization,
                runtime_attestation=record.runtime_attestation,
                authorization=authorization,
                identity=identity,
                registration=registration,
                idle_expires_at=now + self._prepared_ttl_seconds,
                reservation_id=record.reservation_id,
                materialized_bytes=record.materialized_bytes,
            )
            self._record_recovery_binding(
                identity,
                registration,
                claim,
                authorization,
                record.runtime_attestation,
                now=now,
            )
            with self._lock:
                current = self._admission_tickets.get(ticket_digest)
                if current is not record or current.state != "committing":
                    raise SecureRemoteExecutionError(
                        "invalid_admission_ticket"
                    )
                if claim.attempt_id in self._prepared:
                    raise SecureRemoteExecutionError(
                        "prepared_execution_conflict"
                    )
                record.state = "consumed"
                self._prepared[claim.attempt_id] = prepared_record
                self._admission_tickets.pop(ticket_digest, None)
            return authorization
        except BaseException:
            with self._lock:
                current = self._admission_tickets.get(ticket_digest)
                if current is record:
                    self._admission_tickets.pop(ticket_digest, None)
                    record.state = "cancelled"
                    self._release_registry_reservation_locked(
                        record.reservation_id
                    )
            if record.worker_authorization is not None:
                self.worker_authorization_gate.discard(
                    record.worker_authorization
                )
            raise

    def admit(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
    ) -> ExecutionAuthorization:
        """Prepare policy once and return the exact session-bound authority."""

        self._require_ready()
        self._validate_control_context(identity, registration, scheduler, claim)
        self._purge_registry(self._now())
        with self._claim_guard(claim.attempt_id):
            now = self._now()
            existing = self._get_prepared(claim.attempt_id)
            if existing is not None:
                refreshed = self._refresh_record(
                    existing,
                    identity,
                    registration,
                    scheduler,
                    claim,
                    existing.authorization,
                    now=now,
                )
                return refreshed.authorization

            resolved = self.plan_resolver.resolve(scheduler, claim)
            if not isinstance(resolved, RemoteExecutionPreparation):
                raise SecureRemoteExecutionError("invalid_execution_plan")
            script_ref = resolved.script_artifact_ref
            if script_ref is not None and not isinstance(
                script_ref,
                ArtifactRef,
            ):
                raise SecureRemoteExecutionError("invalid_execution_plan")
            materialized_bytes = (
                0 if script_ref is None else script_ref.size
            )
            reservation_id = self._reserve_registry(materialized_bytes)
            try:
                return self._admit_reserved(
                    identity,
                    registration,
                    scheduler,
                    claim,
                    resolved,
                    reservation_id=reservation_id,
                    materialized_bytes=materialized_bytes,
                    now=now,
                )
            except BaseException:
                self._release_registry_reservation(reservation_id)
                raise

    def restore_authorization(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        expected: ClaimBinding,
    ) -> ExecutionAuthority:
        """Restore exact issued authority without recreating bearer grants."""

        self._require_ready()
        if not isinstance(expected, ClaimBinding):
            raise SecureRemoteExecutionError(
                "invalid_recovery_binding"
            )
        self._validate_control_context(
            identity,
            registration,
            scheduler,
            claim,
        )
        self._purge_registry(self._now())
        with self._claim_guard(claim.attempt_id):
            now = self._now()
            existing = self._get_prepared(claim.attempt_id)
            if existing is not None:
                refreshed = self._refresh_record(
                    existing,
                    identity,
                    registration,
                    scheduler,
                    claim,
                    existing.authorization,
                    now=now,
                )
                self._require_expected_authority(
                    refreshed.authorization,
                    expected,
                )
                return refreshed.authorization
            return self._recover_authorization_under_guard(
                identity,
                registration,
                scheduler,
                claim,
                expected,
                now=now,
            )

    def _recover_authorization_under_guard(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        expected: ClaimBinding,
        *,
        now: float,
    ) -> ExecutionAuthorizationBinding:
        try:
            durable = self.recovery_journal.get(
                claim.run_id,
                claim.attempt_id,
                claim.fencing_token,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except RemoteExecutionJournalError:
            raise SecureRemoteExecutionError(
                "recovery_journal_unavailable"
            ) from None
        if durable is None:
            raise SecureRemoteExecutionError("prepared_registry_miss")
        if (
            durable.node_id != claim.node_id
            or durable.worker_id != identity.worker_id
            or durable.tenant_id != identity.tenant_id
            or durable.identity_digest != identity.identity_digest
            or durable.session_binding_digest
            != registration.session_binding_digest
            or durable.claim_token_digest
            != hashlib.sha256(
                claim.claim_token.encode("utf-8")
            ).hexdigest()
        ):
            raise SecureRemoteExecutionError(
                "recovery_binding_mismatch"
            )
        self._require_expected_record(durable, expected)

        resolved = self.plan_resolver.resolve(scheduler, claim)
        if not isinstance(resolved, RemoteExecutionPreparation):
            raise SecureRemoteExecutionError("invalid_execution_plan")
        script_ref = resolved.script_artifact_ref
        if script_ref is not None and not isinstance(script_ref, ArtifactRef):
            raise SecureRemoteExecutionError("invalid_execution_plan")
        materialized_bytes = 0 if script_ref is None else script_ref.size
        reservation_id = self._reserve_registry(materialized_bytes)
        worker_authorization: WorkerAuthorization | None = None
        try:
            prepared = self.executor.restore_prepared_execution(
                claim,
                argv=resolved.argv,
                cwd=resolved.cwd,
                profile=resolved.profile,
                capabilities=resolved.capabilities,
                resource_locks=resolved.resource_locks,
                sensitive_keys=resolved.sensitive_keys,
                limits=resolved.limits,
                input_artifact_refs=resolved.input_artifact_refs,
                script_artifact_ref=resolved.script_artifact_ref,
                environment=resolved.environment,
            )
            if (
                len(prepared.request.materialized_script or b"")
                != materialized_bytes
            ):
                raise SecureRemoteExecutionError(
                    "invalid_materialized_script"
                )
            presentation = self.presentation_resolver.resolve(
                identity,
                registration,
            )
            worker_authorization = (
                self.worker_authorization_gate.authorize(
                    presentation,
                    tenant_id=identity.tenant_id,
                    pool_id=self.pool_resolver(registration),
                    action=prepared.action,
                    expected_transport_binding_digest=(
                        registration.session_binding_digest
                    ),
                    expected_worker_id=identity.worker_id,
                )
            )
            if (
                worker_authorization.authorization_digest
                != durable.authorization_digest
            ):
                raise SecureRemoteExecutionError(
                    "authorization_lineage_changed"
                )
            runtime_attestation = self.runtime_proof_verifier.attest(
                identity,
                registration,
                now=now,
            )
            if not isinstance(
                runtime_attestation,
                VerifiedRemoteRuntimeAttestation,
            ):
                raise SecureRemoteExecutionError(
                    "runtime_attestation_unverified"
                )
            runtime_attestation.validate_for(
                identity,
                registration,
                now=now,
            )
            if (
                runtime_attestation.runtime_attestation_digest
                != durable.runtime_attestation_digest
                or runtime_attestation.verifier_id
                != durable.runtime_verifier_id
                or runtime_attestation.security_level.value
                != durable.runtime_security_level
            ):
                raise SecureRemoteExecutionError(
                    "runtime_attestation_lineage_changed"
                )
            execution_plan = RemoteExecutionPlan(
                argv=prepared.request.argv,
                container_cwd=resolved.container_cwd,
                limits=prepared.request.limits.to_dict(),
                profile_id=prepared.profile.profile_id,
                profile_digest=prepared.profile.profile_digest,
                policy_version=self.executor.policy.policy_version,
                request_digest=prepared.request.request_digest,
                capabilities=tuple(
                    capability.name
                    for capability in prepared.action.capabilities
                ),
            )
            authorization = ExecutionAuthorizationBinding(
                action_digest=prepared.action.action_digest,
                authorization_digest=(
                    worker_authorization.authorization_digest
                ),
                profile_digest=prepared.profile.profile_digest,
                request_digest=prepared.request.request_digest,
                session_binding_digest=(
                    registration.session_binding_digest
                ),
                grant_binding_digest=durable.grant_binding_digest,
                execution_plan_digest=execution_plan.plan_digest,
                runtime_attestation_digest=(
                    runtime_attestation.runtime_attestation_digest
                ),
                execution_plan=execution_plan,
            )
            self._require_expected_authority(authorization, expected)
            if (
                authorization.action_digest != durable.action_digest
                or authorization.profile_digest != durable.profile_digest
                or authorization.request_digest != durable.request_digest
                or authorization.execution_plan_digest
                != durable.execution_plan_digest
            ):
                raise SecureRemoteExecutionError(
                    "recovery_binding_mismatch"
                )
            record = _PreparedRemoteExecution(
                scheduler=scheduler,
                prepared=prepared,
                worker_authorization=worker_authorization,
                runtime_attestation=runtime_attestation,
                authorization=authorization,
                identity=identity,
                registration=registration,
                idle_expires_at=(
                    durable.created_at + self._prepared_ttl_seconds
                ),
                reservation_id=reservation_id,
                materialized_bytes=materialized_bytes,
            )
            attempt = scheduler.store.get_attempt(claim.attempt_id)
            if (
                attempt is not None
                and attempt.status is AttemptStatus.CLAIMED
                and now >= record.idle_expires_at
            ):
                raise SecureRemoteExecutionError(
                    "prepared_execution_idle_expired"
                )
            with self._lock:
                if claim.attempt_id in self._prepared:
                    raise SecureRemoteExecutionError(
                        "prepared_execution_conflict"
                    )
                self._prepared[claim.attempt_id] = record
            return authorization
        except BaseException:
            if worker_authorization is not None:
                self.worker_authorization_gate.discard(
                    worker_authorization
                )
            self._release_registry_reservation(reservation_id)
            raise

    @staticmethod
    def _require_expected_record(
        record: RemoteExecutionBindingRecord,
        expected: ClaimBinding,
    ) -> None:
        if (
            record.run_id != expected.run_id
            or record.node_id != expected.node_id
            or record.attempt_id != expected.attempt_id
            or record.fencing_token != expected.fencing_token
            or record.action_digest != expected.action_digest
            or record.authorization_digest
            != expected.authorization_digest
            or record.profile_digest != expected.profile_digest
            or record.request_digest != expected.request_digest
            or record.session_binding_digest
            != expected.session_binding_digest
            or record.grant_binding_digest
            != expected.grant_binding_digest
            or record.execution_plan_digest
            != expected.execution_plan_digest
            or record.runtime_attestation_digest
            != expected.runtime_attestation_digest
        ):
            raise SecureRemoteExecutionError(
                "recovery_binding_mismatch"
            )

    @staticmethod
    def _require_expected_authority(
        authorization: ExecutionAuthority,
        expected: ClaimBinding,
    ) -> None:
        if (
            authorization.action_digest != expected.action_digest
            or authorization.authorization_digest
            != expected.authorization_digest
            or authorization.profile_digest != expected.profile_digest
            or authorization.request_digest != expected.request_digest
            or authorization.session_binding_digest
            != expected.session_binding_digest
            or authorization.grant_binding_digest
            != expected.grant_binding_digest
            or authorization.execution_plan_digest
            != expected.execution_plan_digest
            or authorization.runtime_attestation_digest
            != expected.runtime_attestation_digest
        ):
            raise SecureRemoteExecutionError(
                "recovery_binding_mismatch"
            )

    def _admit_reserved(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        resolved: RemoteExecutionPreparation,
        *,
        reservation_id: str,
        materialized_bytes: int,
        now: float,
    ) -> ExecutionAuthorization:
        worker_authorization: WorkerAuthorization | None = None
        try:
            prepared = self.executor.prepare_execution(
                claim,
                argv=resolved.argv,
                cwd=resolved.cwd,
                profile=resolved.profile,
                approval_grant=resolved.approval_grant,
                capabilities=resolved.capabilities,
                resource_locks=resolved.resource_locks,
                sensitive_keys=resolved.sensitive_keys,
                limits=resolved.limits,
                input_artifact_refs=resolved.input_artifact_refs,
                script_artifact_ref=resolved.script_artifact_ref,
                environment=resolved.environment,
            )
            if isinstance(prepared, ActivityExecutionResult):
                raise SecureRemoteExecutionError("policy_not_allowed")
            if (
                len(prepared.request.materialized_script or b"")
                != materialized_bytes
            ):
                raise SecureRemoteExecutionError(
                    "invalid_materialized_script"
                )
            presentation = self.presentation_resolver.resolve(
                identity,
                registration,
            )
            pool_id = self.pool_resolver(registration)
            worker_authorization = (
                self.worker_authorization_gate.authorize(
                    presentation,
                    tenant_id=identity.tenant_id,
                    pool_id=pool_id,
                    action=prepared.action,
                    expected_transport_binding_digest=(
                        registration.session_binding_digest
                    ),
                    expected_worker_id=identity.worker_id,
                )
            )
            input_grants = self._issue_input_grants(
                worker_authorization,
                identity=identity,
                claim=claim,
                prepared=prepared,
            )
            runtime_attestation = self.runtime_proof_verifier.attest(
                identity,
                registration,
                now=now,
            )
            if not isinstance(
                runtime_attestation,
                VerifiedRemoteRuntimeAttestation,
            ):
                raise SecureRemoteExecutionError(
                    "runtime_attestation_unverified"
                )
            runtime_attestation.validate_for(
                identity,
                registration,
                now=now,
            )
            execution_plan = RemoteExecutionPlan(
                argv=prepared.request.argv,
                container_cwd=resolved.container_cwd,
                limits=prepared.request.limits.to_dict(),
                profile_id=prepared.profile.profile_id,
                profile_digest=prepared.profile.profile_digest,
                policy_version=self.executor.policy.policy_version,
                request_digest=prepared.request.request_digest,
                capabilities=tuple(
                    capability.name
                    for capability in prepared.action.capabilities
                ),
            )
            grants_digest = grant_binding_digest(input_grants, ())
            authorization = ExecutionAuthorization(
                action_digest=prepared.action.action_digest,
                authorization_digest=(
                    worker_authorization.authorization_digest
                ),
                profile_digest=prepared.profile.profile_digest,
                request_digest=prepared.request.request_digest,
                session_binding_digest=registration.session_binding_digest,
                grant_binding_digest=grants_digest,
                execution_plan_digest=execution_plan.plan_digest,
                runtime_attestation_digest=(
                    runtime_attestation.runtime_attestation_digest
                ),
                execution_plan=execution_plan,
                input_grants=input_grants,
                output_grants=(),
            )
            record = _PreparedRemoteExecution(
                scheduler=scheduler,
                prepared=prepared,
                worker_authorization=worker_authorization,
                runtime_attestation=runtime_attestation,
                authorization=authorization,
                identity=identity,
                registration=registration,
                idle_expires_at=now + self._prepared_ttl_seconds,
                reservation_id=reservation_id,
                materialized_bytes=materialized_bytes,
            )
            self._record_recovery_binding(
                identity,
                registration,
                claim,
                authorization,
                runtime_attestation,
                now=now,
            )
            with self._lock:
                if claim.attempt_id in self._prepared:
                    raise SecureRemoteExecutionError(
                        "prepared_execution_conflict"
                    )
                self._prepared[claim.attempt_id] = record
            return authorization
        except BaseException:
            if worker_authorization is not None:
                self.worker_authorization_gate.discard(
                    worker_authorization
                )
            raise

    def stage_output(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        authorization: ExecutionAuthorization,
        *,
        content: bytes,
        kind: ArtifactKind | str = ArtifactKind.TOOL_RESULT,
        sensitivity: ArtifactSensitivity | str = ArtifactSensitivity.INTERNAL,
        media_type: str = "application/octet-stream",
        maximum_bytes: int | None = None,
    ) -> StagedRemoteOutput:
        """Authenticated broker endpoint for dynamic, non-predeclared output.

        Finalization installs immutable bytes before the Domain Event exists.
        If the Worker crashes or its later proof is rejected, the Artifact is
        intentionally unreferenced and remains eligible for normal GC.
        """

        if not isinstance(content, bytes):
            raise SecureRemoteExecutionError("invalid_output_payload")
        with self._claim_guard(claim.attempt_id):
            record = self._require_record_under_guard(
                identity,
                registration,
                scheduler,
                claim,
                authorization,
                now=self._now(),
            )
            content_bytes = len(content)
            output_limit = record.prepared.request.limits.output_bytes
            if (
                record.staged_output_count >= MAX_OUTPUT_HANDLES
                or record.staged_output_bytes + content_bytes > output_limit
            ):
                raise SecureRemoteExecutionError("output_budget_exceeded")

            # Charge before broker or ArtifactStore mutation and never roll it
            # back.  A validation/Store exception is therefore fail-closed:
            # retrying cannot amplify one Attempt after an ambiguous write.
            record.staged_output_count += 1
            record.staged_output_bytes += content_bytes

            declared_digest = hashlib.sha256(content).hexdigest()
            size_limit = (
                content_bytes if maximum_bytes is None else maximum_bytes
            )
            if size_limit <= 0:
                # Empty artifacts are valid, while the grant API requires a
                # positive upper bound.
                size_limit = 1
            grant = self.artifact_broker.issue_write_grant(
                record.worker_authorization,
                tenant_id=identity.tenant_id,
                run_id=claim.run_id,
                node_id=claim.node_id,
                attempt_id=claim.attempt_id,
                kind=kind,
                sensitivity=sensitivity,
                media_type=media_type,
                maximum_bytes=size_limit,
                declared_sha256=declared_digest,
            )
            staging = self.artifact_broker.stage_write(
                grant,
                record.worker_authorization,
                content=content,
                declared_sha256=declared_digest,
            )
            ref = self.artifact_broker.finalize_write(
                grant,
                record.worker_authorization,
                staging,
            )
            return StagedRemoteOutput(
                handle=grant.output_handle,
                descriptor=ArtifactDescriptor.from_ref(ref),
            )

    def complete(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        authorization: ExecutionAuthority,
        candidate: RemoteCompletionCandidate,
        runtime_proof: RemoteRuntimeProof,
    ) -> None:
        """Verify runtime evidence, resolve outputs, and commit one receipt."""

        self._require_ready()
        now = self._now()
        record = self._require_record(
            identity,
            registration,
            scheduler,
            claim,
            authorization,
            now=now,
        )
        if (
            not isinstance(candidate, RemoteCompletionCandidate)
            or not isinstance(runtime_proof, RemoteRuntimeProof)
            or candidate.runtime_proof != runtime_proof
        ):
            raise SecureRemoteExecutionError("runtime_proof_mismatch")
        if (
            candidate.outcome is AttemptStatus.SUCCEEDED
            and (
                candidate.error_class is not None
                or candidate.error_code is not None
            )
        ) or (
            candidate.outcome is not AttemptStatus.SUCCEEDED
            and (
                candidate.error_class != "sandbox"
                or candidate.error_code
                != runtime_proof.sandbox_receipt.get("error_code")
            )
        ):
            raise SecureRemoteExecutionError(
                "runtime_error_binding_mismatch"
            )
        binding = _claim_binding(claim, authorization)
        try:
            runtime_proof.validate_binding(
                binding,
                outcome=candidate.outcome.value,
                output_handles=candidate.output_handles,
            )
        except (RemoteProtocolError, TypeError, ValueError) as exc:
            raise SecureRemoteExecutionError(
                "runtime_proof_binding_mismatch"
            ) from exc
        self._verify_runtime_proof(
            runtime_proof,
            identity=identity,
            registration=registration,
            claim=binding,
            authorization=authorization,
            record=record,
            now=now,
        )
        final_refs = tuple(
            self.artifact_broker.finalize_output_handle(
                handle,
                record.worker_authorization,
            )
            for handle in candidate.output_handles
        )
        receipt = _trusted_receipt(runtime_proof, final_refs)
        _validate_remote_receipt(
            receipt,
            record,
            candidate.outcome,
        )
        result = self.executor.complete_prepared(
            record.prepared,
            receipt,
            completion_evidence=_completion_evidence(
                runtime_proof,
                authorization,
            ),
            claim=claim,
        )
        if result.attempt_status is not candidate.outcome:
            raise ActivityExecutionConflict(
                "remote outcome does not match durable completion"
            )
        self._drop_prepared(claim.attempt_id)

    def cancel(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        authorization: ExecutionAuthority,
        runtime_proof: RemoteRuntimeProof,
    ) -> None:
        """Confirm cancellation only with a signed, bound CANCELLED receipt."""

        self._require_ready()
        now = self._now()
        record = self._require_record(
            identity,
            registration,
            scheduler,
            claim,
            authorization,
            now=now,
        )
        binding = _claim_binding(claim, authorization)
        try:
            runtime_proof.validate_binding(
                binding,
                outcome=AttemptStatus.CANCELLED.value,
                output_handles=(),
            )
        except (RemoteProtocolError, TypeError, ValueError) as exc:
            raise SecureRemoteExecutionError(
                "runtime_proof_binding_mismatch"
            ) from exc
        self._verify_runtime_proof(
            runtime_proof,
            identity=identity,
            registration=registration,
            claim=binding,
            authorization=authorization,
            record=record,
            now=now,
        )
        receipt = _trusted_receipt(runtime_proof, ())
        _validate_remote_receipt(
            receipt,
            record,
            AttemptStatus.CANCELLED,
        )
        result = self.executor.complete_prepared(
            record.prepared,
            receipt,
            completion_evidence=_completion_evidence(
                runtime_proof,
                authorization,
            ),
            claim=claim,
        )
        if result.attempt_status is not AttemptStatus.CANCELLED:
            raise ActivityExecutionConflict(
                "remote cancellation did not reach CANCELLED"
            )
        self._drop_prepared(claim.attempt_id)

    def _record_recovery_binding(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        claim: ActivityClaim,
        authorization: ExecutionAuthorization,
        attestation: VerifiedRemoteRuntimeAttestation,
        *,
        now: float,
    ) -> None:
        """Publish only non-secret recovery evidence before assignment return."""

        try:
            self.recovery_journal.record(
                RemoteExecutionBindingRecord(
                    run_id=claim.run_id,
                    attempt_id=claim.attempt_id,
                    node_id=claim.node_id,
                    worker_id=identity.worker_id,
                    tenant_id=identity.tenant_id,
                    identity_digest=identity.identity_digest,
                    session_binding_digest=(
                        registration.session_binding_digest
                    ),
                    claim_token_digest=hashlib.sha256(
                        claim.claim_token.encode("utf-8")
                    ).hexdigest(),
                    fencing_token=claim.fencing_token,
                    action_digest=authorization.action_digest,
                    authorization_digest=(
                        authorization.authorization_digest
                    ),
                    profile_digest=authorization.profile_digest,
                    request_digest=authorization.request_digest,
                    grant_binding_digest=(
                        authorization.grant_binding_digest
                    ),
                    execution_plan_digest=(
                        authorization.execution_plan_digest
                    ),
                    runtime_attestation_digest=(
                        authorization.runtime_attestation_digest
                    ),
                    runtime_verifier_id=attestation.verifier_id,
                    runtime_security_level=(
                        attestation.security_level.value
                    ),
                    created_at=now,
                )
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except RemoteExecutionJournalError:
            raise SecureRemoteExecutionError(
                "recovery_journal_unavailable"
            ) from None

    def _issue_input_grants(
        self,
        authorization: WorkerAuthorization,
        *,
        identity: AuthenticatedWorker,
        claim: ActivityClaim,
        prepared: PreparedActivityExecution,
    ) -> tuple[ArtifactReadGrant, ...]:
        refs = list(prepared.request.input_artifact_refs)
        if prepared.request.script_artifact_ref is not None:
            refs.append(prepared.request.script_artifact_ref)
        unique: dict[str, ArtifactRef] = {}
        for ref in refs:
            digest = hashlib.sha256(
                json.dumps(
                    ref.to_dict(),
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest()
            unique[digest] = ref
        return tuple(
            self.artifact_broker.issue_read_grant(
                authorization,
                tenant_id=identity.tenant_id,
                run_id=claim.run_id,
                attempt_id=claim.attempt_id,
                ref=unique[key],
            )
            for key in sorted(unique)
        )

    def _verify_runtime_proof(
        self,
        proof: RemoteRuntimeProof,
        *,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        claim: ClaimBinding,
        authorization: ExecutionAuthority,
        record: _PreparedRemoteExecution,
        now: float,
    ) -> None:
        record.runtime_attestation.validate_for(
            identity,
            registration,
            now=now,
        )
        if (
            record.runtime_attestation.runtime_attestation_digest
            != authorization.runtime_attestation_digest
        ):
            raise SecureRemoteExecutionError(
                "runtime_attestation_binding_mismatch"
            )
        try:
            verified = self.runtime_proof_verifier.verify(
                proof,
                identity=identity,
                registration=registration,
                claim=claim,
                authorization=authorization,
                attestation=record.runtime_attestation,
                now=now,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise SecureRemoteExecutionError(
                "runtime_proof_unverified"
            ) from None
        if verified is not True:
            raise SecureRemoteExecutionError("runtime_proof_unverified")

    def _require_record(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        authorization: ExecutionAuthority,
        *,
        now: float,
    ) -> _PreparedRemoteExecution:
        with self._claim_guard(claim.attempt_id):
            return self._require_record_under_guard(
                identity,
                registration,
                scheduler,
                claim,
                authorization,
                now=now,
            )

    def _require_record_under_guard(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        authorization: ExecutionAuthority,
        *,
        now: float,
    ) -> _PreparedRemoteExecution:
        record = self._get_prepared(claim.attempt_id)
        if record is None:
            raise SecureRemoteExecutionError("prepared_registry_miss")
        return self._refresh_record(
            record,
            identity,
            registration,
            scheduler,
            claim,
            authorization,
            now=now,
        )

    def _refresh_record(
        self,
        record: _PreparedRemoteExecution,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        authorization: ExecutionAuthority,
        *,
        now: float,
    ) -> _PreparedRemoteExecution:
        self._validate_control_context(
            identity,
            registration,
            scheduler,
            claim,
        )
        if (
            record.scheduler is not scheduler
            or record.identity != identity
            or record.registration != registration
            or record.authorization != authorization
            or _stable_claim_authority(record.prepared.claim)
            != _stable_claim_authority(claim)
        ):
            raise SecureRemoteExecutionError(
                "prepared_execution_binding_mismatch"
            )

        try:
            restored = scheduler.restore_claim(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
                claim.worker_id,
                request_hash=claim.request_hash,
                claim_token=claim.claim_token,
                fencing_token=claim.fencing_token,
            )
        except SchedulerError as exc:
            self._remove_record_if_current(claim.attempt_id, record)
            raise SecureRemoteExecutionError(
                "prepared_execution_stale"
            ) from exc
        if _stable_claim_authority(restored) != _stable_claim_authority(claim):
            raise SecureRemoteExecutionError(
                "prepared_execution_binding_mismatch"
            )
        attempt = scheduler.store.get_attempt(claim.attempt_id)
        if attempt is None or attempt.status not in {
            AttemptStatus.CLAIMED,
            AttemptStatus.RUNNING,
        }:
            self._remove_record_if_current(claim.attempt_id, record)
            raise SecureRemoteExecutionError("prepared_execution_stale")
        if (
            attempt.status is AttemptStatus.CLAIMED
            and now >= record.idle_expires_at
        ):
            self._remove_record_if_current(claim.attempt_id, record)
            raise SecureRemoteExecutionError(
                "prepared_execution_idle_expired"
            )

        presentation = self.presentation_resolver.resolve(
            identity,
            registration,
        )
        runtime_attestation = self.runtime_proof_verifier.attest(
            identity,
            registration,
            now=now,
        )
        if not isinstance(
            runtime_attestation,
            VerifiedRemoteRuntimeAttestation,
        ):
            raise SecureRemoteExecutionError(
                "runtime_attestation_unverified"
            )
        runtime_attestation.validate_for(
            identity,
            registration,
            now=now,
        )
        if (
            runtime_attestation.runtime_attestation_digest
            != authorization.runtime_attestation_digest
            or runtime_attestation.verifier_id
            != record.runtime_attestation.verifier_id
            or runtime_attestation.security_level
            is not record.runtime_attestation.security_level
        ):
            raise SecureRemoteExecutionError(
                "runtime_attestation_lineage_changed"
            )

        pool_id = self.pool_resolver(registration)
        try:
            worker_authorization = self.worker_authorization_gate.renew(
                record.worker_authorization,
                presentation,
                tenant_id=identity.tenant_id,
                pool_id=pool_id,
                action=record.prepared.action,
                expected_transport_binding_digest=(
                    registration.session_binding_digest
                ),
                expected_worker_id=identity.worker_id,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise SecureRemoteExecutionError(
                "worker_authorization_renewal_denied"
            ) from None
        if (
            worker_authorization.authorization_digest
            != authorization.authorization_digest
        ):
            raise SecureRemoteExecutionError(
                "authorization_lineage_changed"
            )

        refreshed = replace(
            record,
            worker_authorization=worker_authorization,
            runtime_attestation=runtime_attestation,
        )
        with self._lock:
            if self._prepared.get(claim.attempt_id) is not record:
                raise SecureRemoteExecutionError(
                    "prepared_execution_superseded"
                )
            self._prepared[claim.attempt_id] = refreshed
        return refreshed

    def _validate_control_context(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        *,
        require_durable_claim: bool = True,
    ) -> None:
        if (
            not isinstance(identity, AuthenticatedWorker)
            or not isinstance(registration, WorkerRegistration)
            or not isinstance(scheduler, DurableScheduler)
            or not isinstance(claim, ActivityClaim)
            or scheduler is not self.executor.scheduler
            or registration.worker_id != identity.worker_id
            or registration.tenant_id != identity.tenant_id
            or registration.identity_digest != identity.identity_digest
            or claim.worker_id != registration.session_owner_id
            or claim.activity_kind not in self.supported_activity_kinds
            or claim.activity_kind not in registration.activity_kinds
            or f"activity.{claim.activity_kind}"
            not in registration.capabilities
            or (
                require_durable_claim
                and (
                    not claim.claim_token
                    or claim.fencing_token < 1
                    or claim.lease_expires_at <= 0
                )
            )
            or (
                not require_durable_claim
                and (
                    claim.claim_token
                    or claim.fencing_token != 0
                    or claim.lease_expires_at != 0.0
                )
            )
        ):
            raise SecureRemoteExecutionError("control_context_mismatch")

    def _reserve_registry(self, materialized_bytes: int) -> str:
        """Reserve one unified ticket/prepared slot before materialization."""

        if (
            isinstance(materialized_bytes, bool)
            or not isinstance(materialized_bytes, int)
            or materialized_bytes < 0
        ):
            raise SecureRemoteExecutionError("invalid_execution_plan")
        reservation_id = secrets.token_urlsafe(24)
        with self._lock:
            if (
                len(self._registry_reservations) >= self._maximum_prepared
                or self._registry_materialized_bytes + materialized_bytes
                > MAX_PREPARED_MATERIALIZED_BYTES
            ):
                raise SecureRemoteExecutionError(
                    "prepared_registry_capacity"
                )
            if reservation_id in self._registry_reservations:
                raise SecureRemoteExecutionError(
                    "prepared_registry_capacity"
                )
            self._registry_reservations[reservation_id] = (
                materialized_bytes
            )
            self._registry_materialized_bytes += materialized_bytes
        return reservation_id

    def _release_registry_reservation(self, reservation_id: str) -> None:
        with self._lock:
            self._release_registry_reservation_locked(reservation_id)

    def _release_registry_reservation_locked(
        self,
        reservation_id: str,
    ) -> None:
        materialized_bytes = self._registry_reservations.pop(
            reservation_id,
            None,
        )
        if materialized_bytes is None:
            return
        self._registry_materialized_bytes -= materialized_bytes
        if self._registry_materialized_bytes < 0:
            raise AssertionError("remote registry byte accounting underflow")

    def _claim_lock(self, attempt_id: str) -> threading.Lock:
        with self._lock:
            lock = self._claim_locks.get(attempt_id)
            if lock is None:
                if len(self._claim_locks) >= self._maximum_prepared:
                    raise SecureRemoteExecutionError(
                        "prepared_registry_capacity"
                    )
                lock = threading.Lock()
                self._claim_locks[attempt_id] = lock
            return lock

    @contextmanager
    def _claim_guard(self, attempt_id: str) -> Iterable[None]:
        lock = self._claim_lock(attempt_id)
        lock.acquire()
        try:
            yield
        finally:
            lock.release()
            with self._lock:
                if (
                    attempt_id not in self._prepared
                    and self._claim_locks.get(attempt_id) is lock
                ):
                    self._claim_locks.pop(attempt_id, None)

    def _get_prepared(
        self,
        attempt_id: str,
    ) -> _PreparedRemoteExecution | None:
        with self._lock:
            return self._prepared.get(attempt_id)

    def _drop_prepared(self, attempt_id: str) -> None:
        with self._lock:
            record = self._prepared.pop(attempt_id, None)
            if record is not None:
                self._release_registry_reservation_locked(
                    record.reservation_id
                )
        if record is not None:
            self.worker_authorization_gate.discard(
                record.worker_authorization
            )
            try:
                self.recovery_journal.discard(
                    record.prepared.claim.run_id,
                    record.prepared.claim.attempt_id,
                    record.prepared.claim.fencing_token,
                )
            except RemoteExecutionJournalError:
                # The durable terminal Receipt is execution truth.  A stale
                # digest-only recovery row is harmless and may be reconciled
                # later; cleanup failure must not roll back completion.
                pass

    def _remove_record_if_current(
        self,
        attempt_id: str,
        record: _PreparedRemoteExecution,
    ) -> None:
        removed = False
        with self._lock:
            if self._prepared.get(attempt_id) is record:
                self._prepared.pop(attempt_id, None)
                self._release_registry_reservation_locked(
                    record.reservation_id
                )
                removed = True
        if removed:
            self.worker_authorization_gate.discard(
                record.worker_authorization
            )

    def _purge_registry(self, now: float) -> None:
        """Remove only idle or no-longer-durable authority.

        Store inspection deliberately happens outside ``self._lock``.  The
        final deletion is an identity CAS so a concurrent refresh cannot be
        removed by a stale purge decision.
        """

        with self._lock:
            expired_tickets = [
                (ticket_digest, record)
                for ticket_digest, record in self._admission_tickets.items()
                if now >= record.expires_at
            ]
            for ticket_digest, record in expired_tickets:
                self._admission_tickets.pop(ticket_digest, None)
                record.state = "cancelled"
                self._release_registry_reservation_locked(
                    record.reservation_id
                )
            snapshot = tuple(
                (
                    attempt_id,
                    record,
                    self._claim_locks.get(attempt_id),
                )
                for attempt_id, record in self._prepared.items()
            )
        for _ticket_digest, record in expired_tickets:
            if record.worker_authorization is not None:
                self.worker_authorization_gate.discard(
                    record.worker_authorization
                )
        stale: list[tuple[str, _PreparedRemoteExecution]] = []
        for attempt_id, record, claim_lock in snapshot:
            if claim_lock is not None and claim_lock.locked():
                continue
            attempt = record.scheduler.store.get_attempt(attempt_id)
            if (
                attempt is not None
                and attempt.status is AttemptStatus.CLAIMED
                and now >= record.idle_expires_at
            ):
                stale.append((attempt_id, record))
                continue
            try:
                restored = record.scheduler.restore_claim(
                    record.prepared.claim.run_id,
                    record.prepared.claim.node_id,
                    record.prepared.claim.attempt_id,
                    record.prepared.claim.worker_id,
                    request_hash=record.prepared.claim.request_hash,
                    claim_token=record.prepared.claim.claim_token,
                    fencing_token=record.prepared.claim.fencing_token,
                )
            except SchedulerError:
                stale.append((attempt_id, record))
                continue
            if _stable_claim_authority(restored) != _stable_claim_authority(
                record.prepared.claim
            ):
                stale.append((attempt_id, record))
        removed_stale: list[_PreparedRemoteExecution] = []
        with self._lock:
            for attempt_id, record in stale:
                claim_lock = self._claim_locks.get(attempt_id)
                if (
                    self._prepared.get(attempt_id) is record
                    and (claim_lock is None or not claim_lock.locked())
                ):
                    self._prepared.pop(attempt_id, None)
                    self._release_registry_reservation_locked(
                        record.reservation_id
                    )
                    self._claim_locks.pop(attempt_id, None)
                    removed_stale.append(record)
            orphaned_locks = [
                attempt_id
                for attempt_id, claim_lock in self._claim_locks.items()
                if attempt_id not in self._prepared
                and not claim_lock.locked()
            ]
            for attempt_id in orphaned_locks:
                self._claim_locks.pop(attempt_id, None)
        for record in removed_stale:
            self.worker_authorization_gate.discard(
                record.worker_authorization
            )

    def _require_ready(self) -> None:
        if not self.production_security_ready:
            raise SecureRemoteExecutionError("security_not_ready")

    def _now(self) -> float:
        return _timestamp(self._clock())


def _claim_binding(
    claim: ActivityClaim,
    authorization: ExecutionAuthority,
) -> ClaimBinding:
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


def _stable_claim_authority(claim: ActivityClaim) -> tuple[Any, ...]:
    """Compare every durable authority field except the renewable deadline."""

    return (
        claim.run_id,
        claim.node_id,
        claim.attempt_id,
        claim.attempt_number,
        claim.worker_id,
        claim.request_hash,
        claim.claim_token,
        claim.fencing_token,
        claim.operation_key,
        claim.idempotency_key,
        claim.claim_key,
        claim.activity_kind,
        claim.effect_class,
        claim.resource_keys,
        dict(claim.config),
        claim.input_artifact_bindings,
        claim.input_artifact_refs,
    )


def _trusted_receipt(
    proof: RemoteRuntimeProof,
    refs: tuple[ArtifactRef, ...],
) -> SandboxReceipt:
    raw = proof.sandbox_receipt
    identities = raw.get("output_artifact_refs")
    expected = [_artifact_identity(ref) for ref in refs]
    if identities != expected:
        raise SecureRemoteExecutionError("artifact_receipt_mismatch")
    try:
        return SandboxReceipt(
            schema_version=raw["schema_version"],
            backend_id=raw["backend_id"],
            security_level=raw["security_level"],
            profile_id=raw["profile_id"],
            profile_digest=raw["profile_digest"],
            action_digest=raw["action_digest"],
            policy_version=raw["policy_version"],
            request_digest=raw["request_digest"],
            outcome=raw["outcome"],
            exit_code=raw["exit_code"],
            timed_out=raw["timed_out"],
            output_artifact_refs=refs,
            error_code=raw["error_code"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SecureRemoteExecutionError("invalid_sandbox_receipt") from exc


def _validate_remote_receipt(
    receipt: SandboxReceipt,
    record: _PreparedRemoteExecution,
    candidate_outcome: AttemptStatus,
) -> None:
    prepared = record.prepared
    if (
        receipt.security_level is not SecurityLevel.CONTAINER
        or receipt.action_digest != prepared.action.action_digest
        or receipt.profile_id != prepared.profile.profile_id
        or receipt.profile_digest != prepared.profile.profile_digest
        or receipt.policy_version
        != prepared.request.policy_decision.policy_version
        or receipt.request_digest != prepared.request.request_digest
    ):
        raise SecureRemoteExecutionError("sandbox_receipt_binding_mismatch")
    expected = _attempt_status_for_receipt(
        receipt,
        prepared.action.effect_class,
    )
    if expected is not candidate_outcome:
        raise SecureRemoteExecutionError("remote_outcome_mismatch")


def _attempt_status_for_receipt(
    receipt: SandboxReceipt,
    effect_class: EffectClass,
) -> AttemptStatus:
    if receipt.outcome is SandboxOutcome.SUCCEEDED:
        return AttemptStatus.SUCCEEDED
    if receipt.outcome is SandboxOutcome.CANCELLED:
        return AttemptStatus.CANCELLED
    if receipt.outcome is SandboxOutcome.CANCELLATION_UNKNOWN:
        return AttemptStatus.OUTCOME_UNKNOWN
    if effect_class in {
        EffectClass.NON_IDEMPOTENT_WRITE,
        EffectClass.DESTRUCTIVE,
    }:
        return AttemptStatus.OUTCOME_UNKNOWN
    if receipt.outcome is SandboxOutcome.TIMED_OUT:
        return AttemptStatus.TIMED_OUT
    return AttemptStatus.FAILED


def _completion_evidence(
    proof: RemoteRuntimeProof,
    authorization: ExecutionAuthority,
) -> dict[str, str]:
    return {
        "authorization_digest": authorization.authorization_digest,
        "execution_plan_digest": authorization.execution_plan_digest,
        "grant_binding_digest": authorization.grant_binding_digest,
        "runtime_proof_digest": proof.proof_digest,
        "session_binding_digest": authorization.session_binding_digest,
        "runtime_attestation_digest": (
            authorization.runtime_attestation_digest
        ),
        "sandbox_spec_digest": proof.sandbox_spec_digest,
    }


def _artifact_identity(ref: ArtifactRef) -> dict[str, Any]:
    return {
        "artifact_id": ref.artifact_id,
        "sha256": ref.sha256,
        "size": ref.size,
        "kind": ref.kind.value,
    }


def _worker_sandbox_receipt(
    grant: RemoteExecutionGrant,
    execution: RemoteSandboxExecution,
    descriptors: tuple[ArtifactDescriptor, ...],
) -> dict[str, Any]:
    plan = grant.assignment.execution_plan
    receipt = {
        "schema_version": 2,
        "backend_id": execution.backend_id,
        "security_level": SecurityLevel.CONTAINER.value,
        "profile_id": plan.profile_id,
        "profile_digest": plan.profile_digest,
        "action_digest": grant.action_digest,
        "policy_version": plan.policy_version,
        "request_digest": plan.request_digest,
        "outcome": execution.outcome.value,
        "exit_code": execution.exit_code,
        "timed_out": execution.timed_out,
        "output_artifact_refs": [
            {
                "artifact_id": descriptor.artifact_id,
                "sha256": descriptor.sha256,
                "size": descriptor.size,
                "kind": descriptor.kind,
            }
            for descriptor in descriptors
        ],
        "error_code": execution.error_code,
    }
    # Reuse the strict durable shape validator before asking a signer to attest.
    SandboxReceipt.validate_serialized(receipt)
    return receipt


def _remote_outcome(outcome: SandboxOutcome) -> str:
    return {
        SandboxOutcome.SUCCEEDED: AttemptStatus.SUCCEEDED.value,
        SandboxOutcome.FAILED: AttemptStatus.FAILED.value,
        SandboxOutcome.TIMED_OUT: AttemptStatus.TIMED_OUT.value,
        SandboxOutcome.CANCELLED: AttemptStatus.CANCELLED.value,
        SandboxOutcome.CANCELLATION_UNKNOWN: (
            AttemptStatus.OUTCOME_UNKNOWN.value
        ),
        SandboxOutcome.BACKEND_ERROR: AttemptStatus.FAILED.value,
    }[SandboxOutcome(outcome)]


def _digest(value: Any, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field_name} must be a SHA-256 digest")
    return value


def _timestamp(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("clock must return a finite timestamp") from exc
    if not math.isfinite(result) or result < 0:
        raise ValueError("clock must return a finite timestamp")
    return result


__all__ = [
    "RemoteExecutionPlanResolver",
    "RemoteExecutionPreparation",
    "RemoteExecutionRecoveryReport",
    "RemoteOutputPayload",
    "RemoteSandboxExecution",
    "RemoteWorkerSandboxAdapter",
    "RuntimeProofVerifier",
    "RuntimeProofSigner",
    "SecureRemoteExecutionAdapter",
    "SecureRemoteAssignmentAdmitter",
    "SecureRemoteExecutionError",
    "StagedRemoteOutput",
    "VerifiedRemoteRuntimeAttestation",
    "WorkerArtifactTransport",
    "WorkerPreflightAuthorizer",
    "WorkerPresentationResolver",
]
