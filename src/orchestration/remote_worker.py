"""Store-blind reference worker daemon for the remote worker protocol."""

from __future__ import annotations

import itertools
import re
import secrets
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, runtime_checkable

from .artifact_broker import ArtifactOutputHandle
from .remote_protocol import (
    ClaimBinding,
    RemoteOperation,
    RemoteProtocolError,
    RemoteRuntimeProof,
    WorkAssignment,
    claim_body,
    make_request,
    parse_response,
)


class RemoteTransport(Protocol):
    """Injected authenticated transport; it exposes no Store or scheduler."""

    def __call__(self, request: Mapping[str, Any]) -> Mapping[str, Any]: ...


@runtime_checkable
class RemoteExecutionAdapter(Protocol):
    """Explicit trusted worker-side sandbox/attestation composition."""

    production_security_ready: bool
    runtime_attestation_digest: str

    def prepare(
        self,
        assignment: WorkAssignment,
    ) -> "RemoteExecutionGrant": ...

    def execute(
        self,
        grant: "RemoteExecutionGrant",
        context: "RemoteExecutionContext",
    ) -> "RemoteExecutionOutcome": ...

    def verify_candidate(
        self,
        grant: "RemoteExecutionGrant",
        candidate: "RemoteExecutionOutcome",
    ) -> "RemoteExecutionOutcome": ...

    def cancel(
        self,
        grant: "RemoteExecutionGrant",
        context: "RemoteExecutionContext",
    ) -> RemoteRuntimeProof: ...


class RemoteWorkerError(RuntimeError):
    """Safe remote worker failure identified only by a bounded error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class RemoteExecutionOutcome:
    outcome: str
    output_handles: tuple[ArtifactOutputHandle, ...]
    runtime_proof: RemoteRuntimeProof
    error_class: str | None = None
    error_code: str | None = None

    def __post_init__(self) -> None:
        handles = tuple(self.output_handles)
        if not all(isinstance(handle, ArtifactOutputHandle) for handle in handles):
            raise ValueError("invalid remote output handles")
        object.__setattr__(self, "output_handles", handles)
        if not isinstance(self.runtime_proof, RemoteRuntimeProof):
            raise ValueError("remote execution outcome requires runtime proof")
        if self.outcome == "succeeded":
            if (
                not handles
                or self.error_class is not None
                or self.error_code is not None
            ):
                raise ValueError(
                    "successful outcome requires output handles and no error codes"
                )
        elif self.outcome in {
            "failed",
            "timed_out",
            "abandoned",
            "outcome_unknown",
        }:
            if (
                handles
                or self.error_class is None
                or self.error_code is None
            ):
                raise ValueError(
                    "non-success outcome requires safe error_class and error_code"
                )
        elif self.outcome == "cancelled":
            if (
                handles
                or self.error_class is not None
                or self.error_code is not None
            ):
                raise ValueError("cancelled outcome cannot carry result or error data")
        else:
            raise ValueError("unsupported remote execution outcome")


@dataclass(frozen=True, slots=True)
class RemoteExecutionGrant:
    """Worker-local validation of a control-plane authorization binding."""

    assignment: WorkAssignment
    action_digest: str
    authorization_digest: str
    profile_digest: str
    request_digest: str
    session_binding_digest: str
    grant_binding_digest: str
    execution_plan_digest: str
    runtime_attestation_digest: str

    def __post_init__(self) -> None:
        for field_name in (
            "action_digest",
            "authorization_digest",
            "profile_digest",
            "request_digest",
            "session_binding_digest",
            "grant_binding_digest",
            "execution_plan_digest",
            "runtime_attestation_digest",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not re.fullmatch(
                r"[0-9a-f]{64}",
                value,
            ):
                raise ValueError("execution grant contains an invalid digest")
        claim = self.assignment.claim
        if (
            self.action_digest != claim.action_digest
            or self.authorization_digest != claim.authorization_digest
            or self.profile_digest != claim.profile_digest
            or self.request_digest != claim.request_digest
            or self.session_binding_digest != claim.session_binding_digest
            or self.grant_binding_digest != claim.grant_binding_digest
            or self.execution_plan_digest != claim.execution_plan_digest
            or self.runtime_attestation_digest != claim.runtime_attestation_digest
            or self.assignment.execution_plan.plan_digest
            != self.execution_plan_digest
        ):
            raise ValueError("execution grant does not match assignment authorization")


class RemoteWorkerClient:
    """Typed protocol client suitable for an HTTP, queue, or in-process adapter."""

    def __init__(
        self,
        transport: RemoteTransport,
        *,
        worker_id: str,
        instance_id: str,
    ) -> None:
        if not callable(transport):
            raise TypeError("transport must be callable")
        self._transport = transport
        self.worker_id = worker_id
        self.instance_id = instance_id
        self._sequence = itertools.count(1)
        # A client process restart must not recycle request ids inside the same
        # durable worker session epoch.
        self._request_namespace = secrets.token_hex(8)

    def register(
        self,
        *,
        runtime_version: str,
        capabilities: tuple[str, ...],
        resource_keys: tuple[str, ...],
        activity_kinds: tuple[str, ...],
        max_concurrency: int,
    ) -> Mapping[str, Any]:
        return self._call(
            RemoteOperation.REGISTER,
            {
                "runtime_version": runtime_version,
                "capabilities": sorted(set(capabilities)),
                "resource_keys": sorted(set(resource_keys)),
                "activity_kinds": sorted(set(activity_kinds)),
                "max_concurrency": max_concurrency,
            },
        )

    def poll(
        self,
        run_id: str,
        *,
        lease_seconds: float = 60.0,
    ) -> WorkAssignment | None:
        body = self._call(
            RemoteOperation.POLL,
            {"run_id": run_id, "lease_seconds": lease_seconds},
        )
        assignment = body.get("assignment")
        if assignment is None:
            return None
        return WorkAssignment.from_wire(assignment)

    def poll_fleet(self) -> WorkAssignment | None:
        """Claim the next server-selected compatible Run, if one is ready."""

        body = self._call(RemoteOperation.POLL_FLEET, {})
        assignment = body.get("assignment")
        if assignment is None:
            return None
        return WorkAssignment.from_wire(assignment)

    def start(self, claim: ClaimBinding) -> Mapping[str, Any]:
        return self._call(RemoteOperation.START, claim_body(claim))

    def heartbeat(
        self,
        claim: ClaimBinding,
        *,
        lease_seconds: float = 60.0,
    ) -> Mapping[str, Any]:
        return self._call(
            RemoteOperation.HEARTBEAT,
            claim_body(claim, lease_seconds=lease_seconds),
        )

    def cancellation_requested(self, claim: ClaimBinding) -> bool:
        body = self._call(
            RemoteOperation.CANCELLATION_STATUS,
            claim_body(claim),
        )
        requested = body.get("cancel_requested")
        if not isinstance(requested, bool):
            raise RemoteWorkerError("invalid_response")
        return requested

    def complete(
        self,
        claim: ClaimBinding,
        outcome: RemoteExecutionOutcome,
    ) -> Mapping[str, Any]:
        return self._call(
            RemoteOperation.COMPLETE,
            claim_body(
                claim,
                outcome=outcome.outcome,
                output_handles=[
                    handle.to_wire_dict() for handle in outcome.output_handles
                ],
                error_class=outcome.error_class,
                error_code=outcome.error_code,
                runtime_proof=outcome.runtime_proof.to_wire(),
            ),
        )

    def acknowledge_cancel(
        self,
        claim: ClaimBinding,
        runtime_proof: RemoteRuntimeProof,
    ) -> Mapping[str, Any]:
        return self._call(
            RemoteOperation.ACK_CANCEL,
            claim_body(claim, runtime_proof=runtime_proof.to_wire()),
        )

    def _call(
        self,
        operation: RemoteOperation,
        body: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        request = make_request(
            operation,
            request_id=(
                f"req-{self._request_namespace}-{next(self._sequence)}"
            ),
            worker_id=self.worker_id,
            instance_id=self.instance_id,
            body=body,
        )
        try:
            raw_response = self._transport(request.to_wire())
            response = parse_response(raw_response)
        except RemoteProtocolError as exc:
            raise RemoteWorkerError(exc.code) from exc
        if (
            response.operation is not operation
            or response.request_id != request.request_id
        ):
            raise RemoteWorkerError("response_binding_mismatch")
        if not response.ok:
            code = response.body.get("error_code")
            if not isinstance(code, str):
                raise RemoteWorkerError("invalid_response")
            raise RemoteWorkerError(code)
        return response.body


@dataclass(frozen=True, slots=True)
class RemoteExecutionContext:
    """Cooperative heartbeat/cancellation API passed to an injected executor."""

    client: RemoteWorkerClient
    claim: ClaimBinding
    lease_seconds: float

    def heartbeat(self) -> float:
        response = self.client.heartbeat(
            self.claim,
            lease_seconds=self.lease_seconds,
        )
        expires_at = response.get("lease_expires_at")
        if (
            isinstance(expires_at, bool)
            or not isinstance(expires_at, (int, float))
        ):
            raise RemoteWorkerError("invalid_response")
        return float(expires_at)

    def cancellation_requested(self) -> bool:
        return self.client.cancellation_requested(self.claim)


class RemoteWorkerDaemon:
    """One-worker reference loop with injected execution and Artifact handling."""

    def __init__(
        self,
        client: RemoteWorkerClient,
        adapter: RemoteExecutionAdapter | None = None,
        *,
        runtime_version: str,
        capabilities: tuple[str, ...],
        resource_keys: tuple[str, ...],
        activity_kinds: tuple[str, ...] = ("agent", "tool"),
        max_concurrency: int = 1,
        lease_seconds: float = 60.0,
    ) -> None:
        if adapter is not None and not isinstance(adapter, RemoteExecutionAdapter):
            raise TypeError("adapter must implement RemoteExecutionAdapter")
        self.client = client
        self._adapter = adapter
        self.runtime_version = runtime_version
        self.capabilities = capabilities
        self.resource_keys = resource_keys
        self.activity_kinds = activity_kinds
        self.max_concurrency = max_concurrency
        self.lease_seconds = lease_seconds
        self._registered = False

    def register(self) -> None:
        self.client.register(
            runtime_version=self.runtime_version,
            capabilities=self.capabilities,
            resource_keys=self.resource_keys,
            activity_kinds=self.activity_kinds,
            max_concurrency=self.max_concurrency,
        )
        self._registered = True

    def execute_one(self, run_id: str) -> bool:
        """Execute at most one assignment and commit only a bounded receipt."""

        adapter = self._require_adapter()
        if not self._registered:
            self.register()
        assignment = self.client.poll(
            run_id,
            lease_seconds=self.lease_seconds,
        )
        if assignment is None:
            return False
        if adapter.runtime_attestation_digest != (
            assignment.claim.runtime_attestation_digest
        ):
            raise RemoteWorkerError("runtime_attestation_mismatch")
        try:
            grant = adapter.prepare(assignment)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise RemoteWorkerError("assignment_admission_failed") from exc
        if not isinstance(grant, RemoteExecutionGrant):
            raise RemoteWorkerError("assignment_admission_failed")
        self.client.start(assignment.claim)
        context = RemoteExecutionContext(
            self.client,
            assignment.claim,
            self.lease_seconds,
        )
        if context.cancellation_requested():
            try:
                proof = adapter.cancel(grant, context)
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                raise RemoteWorkerError("cancellation_proof_failed") from exc
            if not isinstance(proof, RemoteRuntimeProof):
                raise RemoteWorkerError("cancellation_proof_failed")
            try:
                proof.validate_binding(
                    assignment.claim,
                    outcome="cancelled",
                    output_handles=(),
                )
            except RemoteProtocolError as exc:
                raise RemoteWorkerError("cancellation_proof_failed") from exc
            self.client.acknowledge_cancel(assignment.claim, proof)
            return True
        context.heartbeat()
        try:
            candidate = adapter.execute(grant, context)
            if not isinstance(candidate, RemoteExecutionOutcome):
                raise TypeError("executor returned an invalid outcome")
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            # A control-plane completion must never fabricate unsigned runtime
            # evidence for a sandbox failure.
            raise RemoteWorkerError("runtime_proof_unavailable") from exc
        try:
            outcome = adapter.verify_candidate(grant, candidate)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise RemoteWorkerError("candidate_verification_failed") from exc
        if not isinstance(outcome, RemoteExecutionOutcome):
            raise RemoteWorkerError("candidate_verification_failed")
        try:
            outcome.runtime_proof.validate_binding(
                assignment.claim,
                outcome=outcome.outcome,
                output_handles=outcome.output_handles,
            )
        except RemoteProtocolError as exc:
            raise RemoteWorkerError("candidate_verification_failed") from exc
        if outcome.outcome == "cancelled":
            if not context.cancellation_requested():
                raise RemoteWorkerError("cancel_not_requested")
            self.client.acknowledge_cancel(
                assignment.claim,
                outcome.runtime_proof,
            )
        else:
            self.client.complete(assignment.claim, outcome)
        return True

    def _require_adapter(self) -> RemoteExecutionAdapter:
        adapter = self._adapter
        if (
            adapter is None
            or getattr(adapter, "production_security_ready", None) is not True
        ):
            raise RemoteWorkerError("security_not_ready")
        if (
            not isinstance(adapter.runtime_attestation_digest, str)
            or re.fullmatch(
                r"[0-9a-f]{64}",
                adapter.runtime_attestation_digest,
            )
            is None
        ):
            raise RemoteWorkerError("runtime_attestation_invalid")
        return adapter


__all__ = [
    "RemoteExecutionContext",
    "RemoteExecutionAdapter",
    "RemoteExecutionGrant",
    "RemoteExecutionOutcome",
    "RemoteTransport",
    "RemoteWorkerClient",
    "RemoteWorkerDaemon",
    "RemoteWorkerError",
]
