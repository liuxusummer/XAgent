"""Versioned, transport-neutral protocol for untrusted remote workers.

The protocol deliberately carries only bounded control metadata, opaque broker
grants, and output handles. Artifact bytes, Store paths, complete
``ArtifactRef`` objects, exception text, and Store credentials are never valid
wire fields.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any, Mapping

from .artifact_broker import (
    MAX_BROKER_ARTIFACT_BYTES,
    MAX_GRANT_TTL_SECONDS,
    ArtifactDescriptor,
    ArtifactGrantDenied,
    ArtifactOutputHandle,
    ArtifactReadGrant,
    ArtifactWriteGrant,
)
from .sandbox import SandboxReceipt, SandboxValidationError

REMOTE_PROTOCOL = "xagent.remote-worker"
REMOTE_PROTOCOL_VERSION = 1
MAX_REMOTE_MESSAGE_BYTES = 64 * 1024
MAX_REMOTE_CONTAINER_ITEMS = 1_024
MAX_REMOTE_DEPTH = 16
MAX_REMOTE_STRING_CHARS = 8_192
MAX_CAPABILITIES = 128
MAX_RESOURCE_KEYS = 128
MAX_ARTIFACT_GRANTS = 64
MAX_OUTPUT_HANDLES = 64
MAX_CONCURRENCY = 1_024
MAX_RUNTIME_SIGNATURE_CHARS = 8 * 1024
MIN_LEASE_SECONDS = 1.0
MAX_LEASE_SECONDS = 3_600.0

_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}")
_CAPABILITY_RE = re.compile(r"[a-z][a-z0-9_.:-]{0,127}")
_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_SAFE_CODE_RE = re.compile(r"[a-z][a-z0-9_.:-]{0,127}")
_BASE64URL_RE = re.compile(r"[A-Za-z0-9_-]+")


class RemoteOperation(StrEnum):
    REGISTER = "register"
    POLL = "poll"
    POLL_FLEET = "poll_fleet"
    START = "start"
    HEARTBEAT = "heartbeat"
    CANCELLATION_STATUS = "cancellation_status"
    COMPLETE = "complete"
    ACK_CANCEL = "ack_cancel"


class RemoteProtocolError(ValueError):
    """A wire message is invalid and must be rejected without side effects."""

    def __init__(self, code: str) -> None:
        if not _SAFE_CODE_RE.fullmatch(code):
            raise ValueError("RemoteProtocolError code must be safe bounded text")
        super().__init__(code)
        self.code = code


class _FrozenJsonMapping(Mapping[str, Any]):
    """Expose bounded JSON objects by detached value, not mutable aliases."""

    __slots__ = ("_values",)

    def __init__(self, value: Mapping[str, Any]) -> None:
        self._values = MappingProxyType(
            {key: _freeze_json(child) for key, child in value.items()}
        )

    def __getitem__(self, key: str) -> Any:
        return _thaw_json(self._values[key])

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


@dataclass(frozen=True, slots=True)
class AuthenticatedWorker:
    """Identity asserted by a trusted transport, never by request payload alone."""

    worker_id: str
    tenant_id: str
    identity_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "worker_id",
            _identifier(self.worker_id, "worker_id"),
        )
        object.__setattr__(
            self,
            "tenant_id",
            _identifier(self.tenant_id, "tenant_id"),
        )
        object.__setattr__(
            self,
            "identity_digest",
            _digest(self.identity_digest, "identity_digest"),
        )


@dataclass(frozen=True, slots=True)
class RemoteExecutionPlan:
    """Path-free control-authorized execution plan consumed by the worker."""

    argv: tuple[str, ...] = field(repr=False)
    container_cwd: str
    limits: Mapping[str, Any]
    profile_id: str
    profile_digest: str
    policy_version: str
    request_digest: str
    capabilities: tuple[str, ...]
    plan_version: int = 1

    def __post_init__(self) -> None:
        if self.plan_version != 1:
            raise RemoteProtocolError("unsupported_execution_plan")
        if not isinstance(self.argv, tuple) or not 1 <= len(self.argv) <= 128:
            raise RemoteProtocolError("invalid_execution_plan")
        argv = tuple(
            _bounded_text(value, "argv", maximum=4_096) for value in self.argv
        )
        object.__setattr__(self, "argv", argv)
        cwd = _bounded_text(self.container_cwd, "container_cwd", maximum=1_024)
        path = PurePosixPath(cwd)
        if (
            not path.is_absolute()
            or cwd != str(path)
            or ".." in path.parts
            or cwd.startswith("/../")
        ):
            raise RemoteProtocolError("invalid_container_cwd")
        object.__setattr__(self, "container_cwd", cwd)
        raw_limits = _exact_object(
            self.limits,
            "resource_limits",
            {
                "schema_version",
                "timeout_seconds",
                "cpu_seconds",
                "memory_bytes",
                "output_bytes",
                "process_count",
            },
        )
        if raw_limits["schema_version"] != 2:
            raise RemoteProtocolError("invalid_resource_limits")
        limits = {
            "schema_version": 2,
            "timeout_seconds": _positive_number(
                raw_limits["timeout_seconds"],
                "timeout_seconds",
                maximum=86_400.0,
            ),
            "cpu_seconds": _positive_number(
                raw_limits["cpu_seconds"],
                "cpu_seconds",
                maximum=86_400.0,
            ),
            "memory_bytes": _integer(
                raw_limits["memory_bytes"],
                "memory_bytes",
                minimum=1,
                maximum=16 * 1024 * 1024 * 1024,
            ),
            "output_bytes": _integer(
                raw_limits["output_bytes"],
                "output_bytes",
                minimum=1,
                maximum=64 * 1024 * 1024,
            ),
            "process_count": _integer(
                raw_limits["process_count"],
                "process_count",
                minimum=1,
                maximum=1_024,
            ),
        }
        object.__setattr__(self, "limits", _FrozenJsonMapping(limits))
        object.__setattr__(
            self,
            "profile_id",
            _identifier(self.profile_id, "profile_id"),
        )
        object.__setattr__(
            self,
            "profile_digest",
            _digest(self.profile_digest, "profile_digest"),
        )
        object.__setattr__(
            self,
            "policy_version",
            _bounded_text(self.policy_version, "policy_version", maximum=128),
        )
        object.__setattr__(
            self,
            "request_digest",
            _digest(self.request_digest, "request_digest"),
        )
        capabilities = _bounded_unique_texts(
            self.capabilities,
            "capabilities",
            maximum=MAX_CAPABILITIES,
            capability=True,
        )
        object.__setattr__(self, "capabilities", capabilities)
        _validate_wire_value(self.to_wire(), field_name="execution_plan")

    @property
    def plan_digest(self) -> str:
        return canonical_digest(self.to_wire())

    def to_wire(self) -> dict[str, Any]:
        return {
            "plan_version": self.plan_version,
            "argv": list(self.argv),
            "container_cwd": self.container_cwd,
            "limits": dict(self.limits),
            "profile_id": self.profile_id,
            "profile_digest": self.profile_digest,
            "policy_version": self.policy_version,
            "request_digest": self.request_digest,
            "capabilities": list(self.capabilities),
        }

    @classmethod
    def from_wire(cls, value: Any) -> "RemoteExecutionPlan":
        payload = _exact_object(
            value,
            "execution_plan",
            {
                "plan_version",
                "argv",
                "container_cwd",
                "limits",
                "profile_id",
                "profile_digest",
                "policy_version",
                "request_digest",
                "capabilities",
            },
        )
        if not isinstance(payload["argv"], list):
            raise RemoteProtocolError("invalid_execution_plan")
        if not isinstance(payload["capabilities"], list):
            raise RemoteProtocolError("invalid_execution_plan")
        return cls(
            plan_version=payload["plan_version"],
            argv=tuple(payload["argv"]),
            container_cwd=payload["container_cwd"],
            limits=payload["limits"],
            profile_id=payload["profile_id"],
            profile_digest=payload["profile_digest"],
            policy_version=payload["policy_version"],
            request_digest=payload["request_digest"],
            capabilities=tuple(payload["capabilities"]),
        )


@dataclass(frozen=True, slots=True)
class RemoteActivityDescriptor:
    """Non-secret Activity identity; raw Workflow config never crosses the wire."""

    activity_name: str
    config_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "activity_name",
            _bounded_text(self.activity_name, "activity_name", maximum=255),
        )
        object.__setattr__(
            self,
            "config_digest",
            _digest(self.config_digest, "config_digest"),
        )

    def to_wire(self) -> dict[str, str]:
        return {
            "activity_name": self.activity_name,
            "config_digest": self.config_digest,
        }

    @classmethod
    def from_wire(cls, value: Any) -> "RemoteActivityDescriptor":
        payload = _exact_object(
            value,
            "activity_descriptor",
            {"activity_name", "config_digest"},
        )
        return cls(
            activity_name=payload["activity_name"],
            config_digest=payload["config_digest"],
        )


@dataclass(frozen=True, slots=True)
class ExecutionAuthorization:
    """Control-plane policy/sandbox binding attached to one remote claim."""

    action_digest: str
    authorization_digest: str
    profile_digest: str
    request_digest: str
    session_binding_digest: str
    grant_binding_digest: str
    execution_plan_digest: str
    runtime_attestation_digest: str
    execution_plan: RemoteExecutionPlan
    input_grants: tuple[ArtifactReadGrant, ...] = ()
    output_grants: tuple[ArtifactWriteGrant, ...] = ()

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
            object.__setattr__(
                self,
                field_name,
                _digest(getattr(self, field_name), field_name),
            )
        if not isinstance(self.execution_plan, RemoteExecutionPlan):
            raise RemoteProtocolError("invalid_execution_plan")
        if (
            self.execution_plan.profile_digest != self.profile_digest
            or self.execution_plan.request_digest != self.request_digest
            or self.execution_plan.plan_digest != self.execution_plan_digest
        ):
            raise RemoteProtocolError("authorization_plan_mismatch")
        inputs = tuple(self.input_grants)
        outputs = tuple(self.output_grants)
        if (
            len(inputs) > MAX_ARTIFACT_GRANTS
            or len(outputs) > MAX_ARTIFACT_GRANTS
            or not all(isinstance(grant, ArtifactReadGrant) for grant in inputs)
            or not all(isinstance(grant, ArtifactWriteGrant) for grant in outputs)
        ):
            raise RemoteProtocolError("invalid_artifact_grants")
        detached_inputs = tuple(_read_grant(_read_grant_to_wire(grant)) for grant in inputs)
        detached_outputs = tuple(
            _write_grant(_write_grant_to_wire(grant)) for grant in outputs
        )
        object.__setattr__(self, "input_grants", detached_inputs)
        object.__setattr__(self, "output_grants", detached_outputs)
        if grant_binding_digest(detached_inputs, detached_outputs) != (
            self.grant_binding_digest
        ):
            raise RemoteProtocolError("grant_binding_mismatch")

    def to_wire(self) -> dict[str, Any]:
        return {
            "action_digest": self.action_digest,
            "authorization_digest": self.authorization_digest,
            "profile_digest": self.profile_digest,
            "request_digest": self.request_digest,
            "session_binding_digest": self.session_binding_digest,
            "grant_binding_digest": self.grant_binding_digest,
            "execution_plan_digest": self.execution_plan_digest,
            "runtime_attestation_digest": self.runtime_attestation_digest,
            "execution_plan": self.execution_plan.to_wire(),
            "input_grants": [
                _read_grant_to_wire(grant) for grant in self.input_grants
            ],
            "output_grants": [
                _write_grant_to_wire(grant) for grant in self.output_grants
            ],
        }


@dataclass(frozen=True, slots=True)
class ExecutionAuthorizationBinding:
    """Control-local recovery view with no replayable Artifact bearer.

    This type is never serialized into a new assignment.  It lets a restarted
    control plane validate and complete an already-published claim from a
    durable digest-only record without persisting or recreating grant tokens.
    """

    action_digest: str
    authorization_digest: str
    profile_digest: str
    request_digest: str
    session_binding_digest: str
    grant_binding_digest: str
    execution_plan_digest: str
    runtime_attestation_digest: str
    execution_plan: RemoteExecutionPlan

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
            object.__setattr__(
                self,
                field_name,
                _digest(getattr(self, field_name), field_name),
            )
        if not isinstance(self.execution_plan, RemoteExecutionPlan):
            raise RemoteProtocolError("invalid_execution_plan")
        if (
            self.execution_plan.profile_digest != self.profile_digest
            or self.execution_plan.request_digest != self.request_digest
            or self.execution_plan.plan_digest != self.execution_plan_digest
        ):
            raise RemoteProtocolError("authorization_plan_mismatch")


ExecutionAuthority = ExecutionAuthorization | ExecutionAuthorizationBinding


@dataclass(frozen=True, slots=True)
class ClaimBinding:
    """Complete durable identity required for every claim mutation."""

    run_id: str
    node_id: str
    attempt_id: str
    activity_request_digest: str
    action_digest: str
    authorization_digest: str
    profile_digest: str
    request_digest: str
    session_binding_digest: str
    grant_binding_digest: str
    execution_plan_digest: str
    runtime_attestation_digest: str
    claim_token: str = field(repr=False)
    fencing_token: int

    def __post_init__(self) -> None:
        for name in ("run_id", "node_id", "attempt_id"):
            object.__setattr__(self, name, _identifier(getattr(self, name), name))
        object.__setattr__(
            self,
            "activity_request_digest",
            _digest(self.activity_request_digest, "activity_request_digest"),
        )
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
            object.__setattr__(
                self,
                field_name,
                _digest(getattr(self, field_name), field_name),
            )
        object.__setattr__(
            self,
            "claim_token",
            _identifier(self.claim_token, "claim_token"),
        )
        object.__setattr__(
            self,
            "fencing_token",
            _integer(
                self.fencing_token,
                "fencing_token",
                minimum=1,
                maximum=2**63 - 1,
            ),
        )

    def to_wire(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "activity_request_digest": self.activity_request_digest,
            "action_digest": self.action_digest,
            "authorization_digest": self.authorization_digest,
            "profile_digest": self.profile_digest,
            "request_digest": self.request_digest,
            "session_binding_digest": self.session_binding_digest,
            "grant_binding_digest": self.grant_binding_digest,
            "execution_plan_digest": self.execution_plan_digest,
            "runtime_attestation_digest": self.runtime_attestation_digest,
            "claim_token": self.claim_token,
            "fencing_token": self.fencing_token,
        }

    @classmethod
    def from_wire(cls, value: Any) -> "ClaimBinding":
        payload = _exact_object(
            value,
            "claim",
            {
                "run_id",
                "node_id",
                "attempt_id",
                "activity_request_digest",
                "action_digest",
                "authorization_digest",
                "profile_digest",
                "request_digest",
                "session_binding_digest",
                "grant_binding_digest",
                "execution_plan_digest",
                "runtime_attestation_digest",
                "claim_token",
                "fencing_token",
            },
        )
        return cls(
            run_id=payload["run_id"],
            node_id=payload["node_id"],
            attempt_id=payload["attempt_id"],
            activity_request_digest=payload["activity_request_digest"],
            action_digest=payload["action_digest"],
            authorization_digest=payload["authorization_digest"],
            profile_digest=payload["profile_digest"],
            request_digest=payload["request_digest"],
            session_binding_digest=payload["session_binding_digest"],
            grant_binding_digest=payload["grant_binding_digest"],
            execution_plan_digest=payload["execution_plan_digest"],
            runtime_attestation_digest=payload["runtime_attestation_digest"],
            claim_token=payload["claim_token"],
            fencing_token=payload["fencing_token"],
        )


@dataclass(frozen=True, slots=True)
class RemoteRuntimeProof:
    """Bounded signed runtime evidence; verification stays control-side."""

    proof_id: str
    verifier_key_id: str
    signed_binding_digest: str
    signature: str = field(repr=False)
    sandbox_spec_digest: str
    sandbox_receipt: Mapping[str, Any]
    proof_version: int = 1

    def __post_init__(self) -> None:
        if self.proof_version != 1:
            raise RemoteProtocolError("unsupported_runtime_proof")
        object.__setattr__(
            self,
            "proof_id",
            _identifier(self.proof_id, "proof_id"),
        )
        object.__setattr__(
            self,
            "verifier_key_id",
            _identifier(self.verifier_key_id, "verifier_key_id"),
        )
        object.__setattr__(
            self,
            "signed_binding_digest",
            _digest(self.signed_binding_digest, "signed_binding_digest"),
        )
        object.__setattr__(
            self,
            "sandbox_spec_digest",
            _digest(self.sandbox_spec_digest, "sandbox_spec_digest"),
        )
        if (
            not isinstance(self.signature, str)
            or not 1 <= len(self.signature) <= MAX_RUNTIME_SIGNATURE_CHARS
            or not _BASE64URL_RE.fullmatch(self.signature)
        ):
            raise RemoteProtocolError("invalid_runtime_signature")
        try:
            decoded_signature = base64.b64decode(
                self.signature + ("=" * (-len(self.signature) % 4)),
                altchars=b"-_",
                validate=True,
            )
        except (binascii.Error, ValueError) as exc:
            raise RemoteProtocolError("invalid_runtime_signature") from exc
        if (
            not decoded_signature
            or base64.urlsafe_b64encode(decoded_signature)
            .rstrip(b"=")
            .decode("ascii")
            != self.signature
        ):
            raise RemoteProtocolError("invalid_runtime_signature")
        try:
            receipt = SandboxReceipt.validate_serialized(self.sandbox_receipt)
        except (SandboxValidationError, TypeError, ValueError) as exc:
            raise RemoteProtocolError("invalid_sandbox_receipt") from exc
        object.__setattr__(
            self,
            "sandbox_receipt",
            _FrozenJsonMapping(receipt),
        )
        _validate_wire_value(self.to_wire(), field_name="runtime_proof")

    @property
    def sandbox_receipt_digest(self) -> str:
        return canonical_digest(dict(self.sandbox_receipt))

    @property
    def proof_digest(self) -> str:
        return canonical_digest(self.to_wire())

    def to_wire(self) -> dict[str, Any]:
        return {
            "proof_version": self.proof_version,
            "proof_id": self.proof_id,
            "verifier_key_id": self.verifier_key_id,
            "signed_binding_digest": self.signed_binding_digest,
            "signature": self.signature,
            "sandbox_spec_digest": self.sandbox_spec_digest,
            "sandbox_receipt": dict(self.sandbox_receipt),
        }

    @classmethod
    def from_wire(cls, value: Any) -> "RemoteRuntimeProof":
        payload = _exact_object(
            value,
            "runtime_proof",
            {
                "proof_version",
                "proof_id",
                "verifier_key_id",
                "signed_binding_digest",
                "signature",
                "sandbox_spec_digest",
                "sandbox_receipt",
            },
        )
        return cls(
            proof_version=payload["proof_version"],
            proof_id=payload["proof_id"],
            verifier_key_id=payload["verifier_key_id"],
            signed_binding_digest=payload["signed_binding_digest"],
            signature=payload["signature"],
            sandbox_spec_digest=payload["sandbox_spec_digest"],
            sandbox_receipt=payload["sandbox_receipt"],
        )

    def validate_binding(
        self,
        claim: ClaimBinding,
        *,
        outcome: str,
        output_handles: tuple[ArtifactOutputHandle, ...],
    ) -> None:
        receipt = self.sandbox_receipt
        if (
            receipt.get("action_digest") != claim.action_digest
            or receipt.get("profile_digest") != claim.profile_digest
            or receipt.get("request_digest") != claim.request_digest
            or not _sandbox_outcome_matches(outcome, receipt.get("outcome"))
        ):
            raise RemoteProtocolError("runtime_proof_binding_mismatch")
        receipt_outputs = receipt.get("output_artifact_refs")
        if not isinstance(receipt_outputs, list):
            raise RemoteProtocolError("runtime_proof_binding_mismatch")
        if outcome == "succeeded":
            if not output_handles or len(receipt_outputs) != len(output_handles):
                raise RemoteProtocolError("runtime_proof_binding_mismatch")
        elif output_handles or receipt_outputs:
            raise RemoteProtocolError("runtime_proof_binding_mismatch")
        expected = runtime_binding_digest(
            claim,
            outcome=outcome,
            output_handles=output_handles,
            sandbox_receipt_digest=self.sandbox_receipt_digest,
            sandbox_spec_digest=self.sandbox_spec_digest,
        )
        if self.signed_binding_digest != expected:
            raise RemoteProtocolError("runtime_proof_binding_mismatch")


@dataclass(frozen=True, slots=True, repr=False)
class RemoteRequest:
    protocol: str
    protocol_version: int
    operation: RemoteOperation
    request_id: str
    worker_id: str
    instance_id: str
    body: Mapping[str, Any]
    request_digest: str = field(repr=False)

    def __post_init__(self) -> None:
        if self.protocol != REMOTE_PROTOCOL:
            raise RemoteProtocolError("unsupported_protocol")
        if self.protocol_version != REMOTE_PROTOCOL_VERSION:
            raise RemoteProtocolError("unsupported_version")
        try:
            operation = RemoteOperation(self.operation)
        except (TypeError, ValueError) as exc:
            raise RemoteProtocolError("unsupported_operation") from exc
        request_id = _identifier(self.request_id, "request_id")
        worker_id = _identifier(self.worker_id, "worker_id")
        instance_id = _identifier(self.instance_id, "instance_id")
        body = _validated_body(operation, self.body)
        request_digest = _digest(self.request_digest, "request_digest")
        expected_digest = canonical_digest(
            {
                "protocol": REMOTE_PROTOCOL,
                "protocol_version": REMOTE_PROTOCOL_VERSION,
                "operation": operation.value,
                "request_id": request_id,
                "worker_id": worker_id,
                "instance_id": instance_id,
                "body": body,
            }
        )
        if request_digest != expected_digest:
            raise RemoteProtocolError("request_digest_mismatch")
        object.__setattr__(self, "operation", operation)
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "worker_id", worker_id)
        object.__setattr__(self, "instance_id", instance_id)
        object.__setattr__(self, "body", _FrozenJsonMapping(body))
        object.__setattr__(self, "request_digest", request_digest)

    def __repr__(self) -> str:
        return (
            f"RemoteRequest(operation={self.operation.value!r}, "
            f"request_id={self.request_id!r}, worker_id={self.worker_id!r})"
        )

    def to_wire(self) -> dict[str, Any]:
        return {
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "operation": self.operation.value,
            "request_id": self.request_id,
            "worker_id": self.worker_id,
            "instance_id": self.instance_id,
            "body": _detach_json(self.body),
            "request_digest": self.request_digest,
        }


@dataclass(frozen=True, slots=True, repr=False)
class RemoteResponse:
    operation: RemoteOperation
    request_id: str
    ok: bool
    body: Mapping[str, Any]
    response_digest: str
    protocol: str = REMOTE_PROTOCOL
    protocol_version: int = REMOTE_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        if self.protocol != REMOTE_PROTOCOL:
            raise RemoteProtocolError("unsupported_protocol")
        if self.protocol_version != REMOTE_PROTOCOL_VERSION:
            raise RemoteProtocolError("unsupported_version")
        try:
            operation = RemoteOperation(self.operation)
        except (TypeError, ValueError) as exc:
            raise RemoteProtocolError("unsupported_operation") from exc
        request_id = _identifier(self.request_id, "request_id")
        if not isinstance(self.ok, bool):
            raise RemoteProtocolError("invalid_response")
        body = _detach_json(self.body)
        if not isinstance(body, dict):
            raise RemoteProtocolError("invalid_response")
        if self.ok:
            body = _validated_response_body(operation, body)
        else:
            _exact_object(body, "error", {"error_code"})
            _safe_code(body["error_code"], "error_code")
        response_digest = _digest(self.response_digest, "response_digest")
        expected_digest = canonical_digest(
            {
                "protocol": REMOTE_PROTOCOL,
                "protocol_version": REMOTE_PROTOCOL_VERSION,
                "operation": operation.value,
                "request_id": request_id,
                "ok": self.ok,
                "body": body,
            }
        )
        if response_digest != expected_digest:
            raise RemoteProtocolError("response_digest_mismatch")
        object.__setattr__(self, "operation", operation)
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "body", _FrozenJsonMapping(body))
        object.__setattr__(self, "response_digest", response_digest)

    def __repr__(self) -> str:
        """Return a useful summary without recursively rendering wire credentials."""

        return (
            f"RemoteResponse(operation={self.operation.value!r}, "
            f"request_id={self.request_id!r}, ok={self.ok!r}, "
            f"body_field_count={len(self.body)}, "
            f"response_digest={self.response_digest!r})"
        )

    def to_wire(self) -> dict[str, Any]:
        return {
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "operation": self.operation.value,
            "request_id": self.request_id,
            "ok": self.ok,
            "body": _detach_json(self.body),
            "response_digest": self.response_digest,
        }


@dataclass(frozen=True, slots=True, repr=False)
class WorkAssignment:
    """Path-free execution plan plus broker grants, never ArtifactRefs."""

    claim: ClaimBinding
    worker_id: str
    attempt_number: int
    lease_expires_at: float
    activity_kind: str
    effect_class: str
    resource_keys: tuple[str, ...]
    activity_descriptor: RemoteActivityDescriptor
    execution_plan: RemoteExecutionPlan
    input_grants: tuple[ArtifactReadGrant, ...]
    output_grants: tuple[ArtifactWriteGrant, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "worker_id",
            _identifier(self.worker_id, "worker_id"),
        )
        object.__setattr__(
            self,
            "attempt_number",
            _integer(
                self.attempt_number,
                "attempt_number",
                minimum=1,
                maximum=2**31 - 1,
            ),
        )
        object.__setattr__(
            self,
            "lease_expires_at",
            _finite_number(self.lease_expires_at, "lease_expires_at"),
        )
        object.__setattr__(
            self,
            "activity_kind",
            _safe_code(self.activity_kind, "activity_kind"),
        )
        object.__setattr__(
            self,
            "effect_class",
            _safe_code(self.effect_class, "effect_class"),
        )
        resource_keys = _bounded_unique_texts(
            self.resource_keys,
            "resource_keys",
            maximum=MAX_RESOURCE_KEYS,
            identifier=False,
        )
        object.__setattr__(self, "resource_keys", resource_keys)
        if not isinstance(self.activity_descriptor, RemoteActivityDescriptor):
            raise RemoteProtocolError("invalid_activity_descriptor")
        if not isinstance(self.execution_plan, RemoteExecutionPlan):
            raise RemoteProtocolError("invalid_execution_plan")
        if self.execution_plan.plan_digest != self.claim.execution_plan_digest:
            raise RemoteProtocolError("authorization_plan_mismatch")
        inputs = tuple(self.input_grants)
        outputs = tuple(self.output_grants)
        if (
            len(inputs) > MAX_ARTIFACT_GRANTS
            or len(outputs) > MAX_ARTIFACT_GRANTS
        ):
            raise RemoteProtocolError("assignment_too_large")
        detached_inputs = tuple(_read_grant(_read_grant_to_wire(grant)) for grant in inputs)
        detached_outputs = tuple(
            _write_grant(_write_grant_to_wire(grant)) for grant in outputs
        )
        if grant_binding_digest(detached_inputs, detached_outputs) != (
            self.claim.grant_binding_digest
        ):
            raise RemoteProtocolError("grant_binding_mismatch")
        object.__setattr__(self, "input_grants", detached_inputs)
        object.__setattr__(self, "output_grants", detached_outputs)
        _validate_wire_value(self.to_wire(), field_name="assignment")

    def __repr__(self) -> str:
        return (
            f"WorkAssignment(run_id={self.claim.run_id!r}, "
            f"node_id={self.claim.node_id!r}, attempt_id={self.claim.attempt_id!r}, "
            f"worker_id={self.worker_id!r}, fencing_token={self.claim.fencing_token})"
        )

    def to_wire(self) -> dict[str, Any]:
        return {
            "claim": self.claim.to_wire(),
            "worker_id": self.worker_id,
            "attempt_number": self.attempt_number,
            "lease_expires_at": self.lease_expires_at,
            "activity_kind": self.activity_kind,
            "effect_class": self.effect_class,
            "resource_keys": list(self.resource_keys),
            "activity_descriptor": self.activity_descriptor.to_wire(),
            "execution_plan": self.execution_plan.to_wire(),
            "input_grants": [
                _read_grant_to_wire(grant) for grant in self.input_grants
            ],
            "output_grants": [
                _write_grant_to_wire(grant) for grant in self.output_grants
            ],
        }

    @classmethod
    def from_wire(cls, value: Any) -> "WorkAssignment":
        payload = _exact_object(
            value,
            "assignment",
            {
                "claim",
                "worker_id",
                "attempt_number",
                "lease_expires_at",
                "activity_kind",
                "effect_class",
                "resource_keys",
                "activity_descriptor",
                "execution_plan",
                "input_grants",
                "output_grants",
            },
        )
        raw_inputs = payload["input_grants"]
        raw_outputs = payload["output_grants"]
        if (
            not isinstance(raw_inputs, list)
            or not isinstance(raw_outputs, list)
            or len(raw_inputs) > MAX_ARTIFACT_GRANTS
            or len(raw_outputs) > MAX_ARTIFACT_GRANTS
        ):
            raise RemoteProtocolError("invalid_assignment")
        return cls(
            claim=ClaimBinding.from_wire(payload["claim"]),
            worker_id=payload["worker_id"],
            attempt_number=payload["attempt_number"],
            lease_expires_at=payload["lease_expires_at"],
            activity_kind=payload["activity_kind"],
            effect_class=payload["effect_class"],
            resource_keys=tuple(payload["resource_keys"])
            if isinstance(payload["resource_keys"], list)
            else payload["resource_keys"],
            activity_descriptor=RemoteActivityDescriptor.from_wire(
                payload["activity_descriptor"]
            ),
            execution_plan=RemoteExecutionPlan.from_wire(payload["execution_plan"]),
            input_grants=tuple(_read_grant(grant) for grant in raw_inputs),
            output_grants=tuple(_write_grant(grant) for grant in raw_outputs),
        )


def make_request(
    operation: RemoteOperation | str,
    *,
    request_id: str,
    worker_id: str,
    instance_id: str,
    body: Mapping[str, Any],
) -> RemoteRequest:
    """Create a request with a canonical self-authenticating body digest."""

    op = RemoteOperation(operation)
    base = {
        "protocol": REMOTE_PROTOCOL,
        "protocol_version": REMOTE_PROTOCOL_VERSION,
        "operation": op.value,
        "request_id": _identifier(request_id, "request_id"),
        "worker_id": _identifier(worker_id, "worker_id"),
        "instance_id": _identifier(instance_id, "instance_id"),
        "body": _validated_body(op, body),
    }
    digest = canonical_digest(base)
    return RemoteRequest(
        protocol=REMOTE_PROTOCOL,
        protocol_version=REMOTE_PROTOCOL_VERSION,
        operation=op,
        request_id=base["request_id"],
        worker_id=base["worker_id"],
        instance_id=base["instance_id"],
        body=_FrozenJsonMapping(base["body"]),
        request_digest=digest,
    )


def parse_request(value: Mapping[str, Any]) -> RemoteRequest:
    payload = _exact_object(
        value,
        "request",
        {
            "protocol",
            "protocol_version",
            "operation",
            "request_id",
            "worker_id",
            "instance_id",
            "body",
            "request_digest",
        },
    )
    if payload["protocol"] != REMOTE_PROTOCOL:
        raise RemoteProtocolError("unsupported_protocol")
    if payload["protocol_version"] != REMOTE_PROTOCOL_VERSION:
        raise RemoteProtocolError("unsupported_version")
    try:
        operation = RemoteOperation(payload["operation"])
    except (TypeError, ValueError) as exc:
        raise RemoteProtocolError("unsupported_operation") from exc
    request_id = _identifier(payload["request_id"], "request_id")
    worker_id = _identifier(payload["worker_id"], "worker_id")
    instance_id = _identifier(payload["instance_id"], "instance_id")
    body = _validated_body(operation, payload["body"])
    supplied_digest = _digest(payload["request_digest"], "request_digest")
    expected_digest = canonical_digest(
        {
            "protocol": REMOTE_PROTOCOL,
            "protocol_version": REMOTE_PROTOCOL_VERSION,
            "operation": operation.value,
            "request_id": request_id,
            "worker_id": worker_id,
            "instance_id": instance_id,
            "body": body,
        }
    )
    if supplied_digest != expected_digest:
        raise RemoteProtocolError("request_digest_mismatch")
    return RemoteRequest(
        protocol=REMOTE_PROTOCOL,
        protocol_version=REMOTE_PROTOCOL_VERSION,
        operation=operation,
        request_id=request_id,
        worker_id=worker_id,
        instance_id=instance_id,
        body=_FrozenJsonMapping(body),
        request_digest=supplied_digest,
    )


def make_response(
    request: RemoteRequest,
    *,
    ok: bool,
    body: Mapping[str, Any],
) -> RemoteResponse:
    detached_body = _detach_json(body)
    if not isinstance(detached_body, dict):
        raise RemoteProtocolError("invalid_response")
    if not ok:
        _exact_object(detached_body, "error", {"error_code"})
        _safe_code(detached_body["error_code"], "error_code")
    else:
        detached_body = _validated_response_body(request.operation, detached_body)
    base = {
        "protocol": REMOTE_PROTOCOL,
        "protocol_version": REMOTE_PROTOCOL_VERSION,
        "operation": request.operation.value,
        "request_id": request.request_id,
        "ok": bool(ok),
        "body": detached_body,
    }
    return RemoteResponse(
        operation=request.operation,
        request_id=request.request_id,
        ok=bool(ok),
        body=_FrozenJsonMapping(detached_body),
        response_digest=canonical_digest(base),
    )


def parse_response(value: Mapping[str, Any]) -> RemoteResponse:
    payload = _exact_object(
        value,
        "response",
        {
            "protocol",
            "protocol_version",
            "operation",
            "request_id",
            "ok",
            "body",
            "response_digest",
        },
    )
    if payload["protocol"] != REMOTE_PROTOCOL:
        raise RemoteProtocolError("unsupported_protocol")
    if payload["protocol_version"] != REMOTE_PROTOCOL_VERSION:
        raise RemoteProtocolError("unsupported_version")
    try:
        operation = RemoteOperation(payload["operation"])
    except (TypeError, ValueError) as exc:
        raise RemoteProtocolError("unsupported_operation") from exc
    if not isinstance(payload["ok"], bool):
        raise RemoteProtocolError("invalid_response")
    request_id = _identifier(payload["request_id"], "request_id")
    body = _detach_json(payload["body"])
    if not isinstance(body, dict):
        raise RemoteProtocolError("invalid_response")
    if not payload["ok"]:
        _exact_object(body, "error", {"error_code"})
        _safe_code(body["error_code"], "error_code")
    else:
        body = _validated_response_body(operation, body)
    supplied_digest = _digest(payload["response_digest"], "response_digest")
    expected_digest = canonical_digest(
        {
            "protocol": REMOTE_PROTOCOL,
            "protocol_version": REMOTE_PROTOCOL_VERSION,
            "operation": operation.value,
            "request_id": request_id,
            "ok": payload["ok"],
            "body": body,
        }
    )
    if supplied_digest != expected_digest:
        raise RemoteProtocolError("response_digest_mismatch")
    return RemoteResponse(
        operation=operation,
        request_id=request_id,
        ok=payload["ok"],
        body=_FrozenJsonMapping(body),
        response_digest=supplied_digest,
    )


def canonical_digest(value: Any) -> str:
    encoded = canonical_bytes(value)
    return hashlib.sha256(encoded).hexdigest()


def canonical_bytes(value: Any) -> bytes:
    detached = _detach_json(value)
    _validate_wire_value(detached)
    try:
        encoded = json.dumps(
            detached,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise RemoteProtocolError("invalid_json") from exc
    if len(encoded) > MAX_REMOTE_MESSAGE_BYTES:
        raise RemoteProtocolError("message_too_large")
    return encoded


def claim_body(claim: ClaimBinding, **extra: Any) -> dict[str, Any]:
    body = {"claim": claim.to_wire()}
    body.update(extra)
    return body


def grant_binding_digest(
    input_grants: tuple[ArtifactReadGrant, ...],
    output_grants: tuple[ArtifactWriteGrant, ...],
) -> str:
    return canonical_digest(
        {
            "schema": "remote_artifact_grants_v1",
            "input_grants": [
                _read_grant_to_wire(grant) for grant in tuple(input_grants)
            ],
            "output_grants": [
                _write_grant_to_wire(grant) for grant in tuple(output_grants)
            ],
        }
    )


def runtime_binding_digest(
    claim: ClaimBinding,
    *,
    outcome: str,
    output_handles: tuple[ArtifactOutputHandle, ...],
    sandbox_receipt_digest: str,
    sandbox_spec_digest: str,
) -> str:
    return canonical_digest(
        {
            "schema": "remote_runtime_binding_v1",
            "claim": claim.to_wire(),
            "outcome": outcome,
            "output_handles": [
                handle.to_wire_dict() for handle in tuple(output_handles)
            ],
            "sandbox_receipt_digest": _digest(
                sandbox_receipt_digest,
                "sandbox_receipt_digest",
            ),
            "sandbox_spec_digest": _digest(
                sandbox_spec_digest,
                "sandbox_spec_digest",
            ),
        }
    )


def output_handles_from_completion(
    body: Mapping[str, Any],
) -> tuple[ArtifactOutputHandle, ...]:
    raw_handles = body.get("output_handles")
    if not isinstance(raw_handles, list) or len(raw_handles) > MAX_OUTPUT_HANDLES:
        raise RemoteProtocolError("invalid_output_handles")
    return tuple(_output_handle(raw_handle) for raw_handle in raw_handles)


def _validated_body(
    operation: RemoteOperation,
    body: Any,
) -> dict[str, Any]:
    if operation is RemoteOperation.REGISTER:
        payload = _exact_object(
            body,
            "body",
            {
                "runtime_version",
                "capabilities",
                "resource_keys",
                "activity_kinds",
                "max_concurrency",
            },
        )
        runtime_version = _bounded_text(
            payload["runtime_version"],
            "runtime_version",
            maximum=128,
        )
        capabilities = _bounded_unique_texts(
            payload["capabilities"],
            "capabilities",
            maximum=MAX_CAPABILITIES,
            capability=True,
        )
        resources = _bounded_unique_texts(
            payload["resource_keys"],
            "resource_keys",
            maximum=MAX_RESOURCE_KEYS,
            identifier=False,
        )
        activity_kinds = _bounded_unique_texts(
            payload["activity_kinds"],
            "activity_kinds",
            maximum=16,
            capability=True,
        )
        if not activity_kinds or not set(activity_kinds).issubset({"agent", "tool"}):
            raise RemoteProtocolError("invalid_activity_kinds")
        return {
            "runtime_version": runtime_version,
            "capabilities": list(capabilities),
            "resource_keys": list(resources),
            "activity_kinds": list(activity_kinds),
            "max_concurrency": _integer(
                payload["max_concurrency"],
                "max_concurrency",
                minimum=1,
                maximum=MAX_CONCURRENCY,
            ),
        }
    if operation is RemoteOperation.POLL:
        payload = _exact_object(body, "body", {"run_id", "lease_seconds"})
        return {
            "run_id": _identifier(payload["run_id"], "run_id"),
            "lease_seconds": _lease_seconds(payload["lease_seconds"]),
        }
    if operation is RemoteOperation.POLL_FLEET:
        _exact_object(body, "body", set())
        return {}
    if operation is RemoteOperation.HEARTBEAT:
        payload = _exact_object(body, "body", {"claim", "lease_seconds"})
        return {
            "claim": ClaimBinding.from_wire(payload["claim"]).to_wire(),
            "lease_seconds": _lease_seconds(payload["lease_seconds"]),
        }
    if operation in {
        RemoteOperation.START,
        RemoteOperation.CANCELLATION_STATUS,
    }:
        payload = _exact_object(body, "body", {"claim"})
        return {"claim": ClaimBinding.from_wire(payload["claim"]).to_wire()}
    if operation is RemoteOperation.ACK_CANCEL:
        payload = _exact_object(
            body,
            "body",
            {"claim", "runtime_proof"},
        )
        claim = ClaimBinding.from_wire(payload["claim"])
        proof = RemoteRuntimeProof.from_wire(payload["runtime_proof"])
        proof.validate_binding(claim, outcome="cancelled", output_handles=())
        return {
            "claim": claim.to_wire(),
            "runtime_proof": proof.to_wire(),
        }
    if operation is RemoteOperation.COMPLETE:
        payload = _exact_object(
            body,
            "body",
            {
                "claim",
                "outcome",
                "output_handles",
                "error_class",
                "error_code",
                "runtime_proof",
            },
        )
        claim = ClaimBinding.from_wire(payload["claim"])
        outcome = _safe_code(payload["outcome"], "outcome")
        if outcome not in {
            "succeeded",
            "failed",
            "timed_out",
            "abandoned",
            "outcome_unknown",
        }:
            raise RemoteProtocolError("invalid_outcome")
        if not isinstance(payload["output_handles"], list):
            raise RemoteProtocolError("invalid_output_handles")
        handles = tuple(
            _output_handle(raw_handle)
            for raw_handle in payload["output_handles"]
        )
        if len(handles) > MAX_OUTPUT_HANDLES:
            raise RemoteProtocolError("invalid_output_handles")
        error_class = payload["error_class"]
        error_code = payload["error_code"]
        if outcome == "succeeded":
            if not handles or error_class is not None or error_code is not None:
                raise RemoteProtocolError("invalid_completion")
        else:
            if handles or error_class is None or error_code is None:
                raise RemoteProtocolError("invalid_completion")
            error_class = _safe_code(error_class, "error_class")
            error_code = _safe_code(error_code, "error_code")
        proof = RemoteRuntimeProof.from_wire(payload["runtime_proof"])
        proof.validate_binding(claim, outcome=outcome, output_handles=handles)
        return {
            "claim": claim.to_wire(),
            "outcome": outcome,
            "output_handles": [
                handle.to_wire_dict() for handle in handles
            ],
            "error_class": error_class,
            "error_code": error_code,
            "runtime_proof": proof.to_wire(),
        }
    raise RemoteProtocolError("unsupported_operation")


def _validated_response_body(
    operation: RemoteOperation,
    body: Mapping[str, Any],
) -> dict[str, Any]:
    if operation is RemoteOperation.REGISTER:
        payload = _exact_object(
            body,
            "response",
            {
                "accepted",
                "protocol_version",
                "worker_id",
                "instance_id",
                "registration_digest",
            },
        )
        if payload["accepted"] is not True:
            raise RemoteProtocolError("invalid_response")
        if payload["protocol_version"] != REMOTE_PROTOCOL_VERSION:
            raise RemoteProtocolError("invalid_response")
        return {
            "accepted": True,
            "protocol_version": REMOTE_PROTOCOL_VERSION,
            "worker_id": _identifier(payload["worker_id"], "worker_id"),
            "instance_id": _identifier(payload["instance_id"], "instance_id"),
            "registration_digest": _digest(
                payload["registration_digest"],
                "registration_digest",
            ),
        }
    if operation in {
        RemoteOperation.POLL,
        RemoteOperation.POLL_FLEET,
    }:
        payload = _exact_object(body, "response", {"assignment"})
        assignment = payload["assignment"]
        return {
            "assignment": None
            if assignment is None
            else WorkAssignment.from_wire(assignment).to_wire()
        }
    if operation in {
        RemoteOperation.START,
        RemoteOperation.HEARTBEAT,
        RemoteOperation.CANCELLATION_STATUS,
        RemoteOperation.COMPLETE,
        RemoteOperation.ACK_CANCEL,
    }:
        common = {"run_id", "node_id", "attempt_id", "fencing_token"}
        if operation is RemoteOperation.START:
            fields = common | {"started"}
        elif operation is RemoteOperation.HEARTBEAT:
            fields = common | {"renewed", "lease_expires_at"}
        elif operation is RemoteOperation.CANCELLATION_STATUS:
            fields = common | {"cancel_requested"}
        else:
            fields = common | {"accepted", "outcome"}
        payload = _exact_object(body, "response", fields)
        normalized: dict[str, Any] = {
            "run_id": _identifier(payload["run_id"], "run_id"),
            "node_id": _identifier(payload["node_id"], "node_id"),
            "attempt_id": _identifier(payload["attempt_id"], "attempt_id"),
            "fencing_token": _integer(
                payload["fencing_token"],
                "fencing_token",
                minimum=1,
                maximum=2**63 - 1,
            ),
        }
        if operation is RemoteOperation.START:
            if payload["started"] is not True:
                raise RemoteProtocolError("invalid_response")
            normalized["started"] = True
        elif operation is RemoteOperation.HEARTBEAT:
            if payload["renewed"] is not True:
                raise RemoteProtocolError("invalid_response")
            normalized["renewed"] = True
            normalized["lease_expires_at"] = _finite_number(
                payload["lease_expires_at"],
                "lease_expires_at",
            )
        elif operation is RemoteOperation.CANCELLATION_STATUS:
            if not isinstance(payload["cancel_requested"], bool):
                raise RemoteProtocolError("invalid_response")
            normalized["cancel_requested"] = payload["cancel_requested"]
        else:
            if payload["accepted"] is not True:
                raise RemoteProtocolError("invalid_response")
            outcome = _safe_code(payload["outcome"], "outcome")
            allowed = (
                {"cancelled"}
                if operation is RemoteOperation.ACK_CANCEL
                else {
                    "succeeded",
                    "failed",
                    "timed_out",
                    "abandoned",
                    "outcome_unknown",
                }
            )
            if outcome not in allowed:
                raise RemoteProtocolError("invalid_response")
            normalized["accepted"] = True
            normalized["outcome"] = outcome
        return normalized
    raise RemoteProtocolError("unsupported_operation")


def _read_grant(value: Any) -> ArtifactReadGrant:
    payload = _exact_object(
        value,
        "input_grant",
        {
            "schema_version",
            "grant_id",
            "token",
            "tenant_id",
            "worker_id",
            "run_id",
            "attempt_id",
            "action_digest",
            "authorization_digest",
            "artifact_binding_digest",
            "descriptor",
            "issued_at",
            "expires_at",
        },
    )
    descriptor_payload = _exact_object(
        payload["descriptor"],
        "artifact_descriptor",
        {
            "schema_version",
            "artifact_id",
            "sha256",
            "size",
            "media_type",
            "kind",
            "sensitivity",
        },
    )
    try:
        descriptor = ArtifactDescriptor(
            schema_version=descriptor_payload["schema_version"],
            artifact_id=descriptor_payload["artifact_id"],
            sha256=descriptor_payload["sha256"],
            size=descriptor_payload["size"],
            media_type=descriptor_payload["media_type"],
            kind=descriptor_payload["kind"],
            sensitivity=descriptor_payload["sensitivity"],
        )
        grant = ArtifactReadGrant(
            schema_version=payload["schema_version"],
            grant_id=payload["grant_id"],
            token=payload["token"],
            tenant_id=payload["tenant_id"],
            worker_id=payload["worker_id"],
            run_id=payload["run_id"],
            attempt_id=payload["attempt_id"],
            action_digest=payload["action_digest"],
            authorization_digest=payload["authorization_digest"],
            artifact_binding_digest=payload["artifact_binding_digest"],
            descriptor=descriptor,
            issued_at=payload["issued_at"],
            expires_at=payload["expires_at"],
        )
    except (ArtifactGrantDenied, TypeError, ValueError) as exc:
        raise RemoteProtocolError("invalid_input_grant") from exc
    if (
        descriptor.size > MAX_BROKER_ARTIFACT_BYTES
        or grant.expires_at - grant.issued_at > MAX_GRANT_TTL_SECONDS
    ):
        raise RemoteProtocolError("invalid_input_grant")
    if _read_grant_to_wire(grant) != _detach_json(payload):
        raise RemoteProtocolError("noncanonical_input_grant")
    return grant


def _read_grant_to_wire(grant: ArtifactReadGrant) -> dict[str, Any]:
    if not isinstance(grant, ArtifactReadGrant):
        raise RemoteProtocolError("invalid_input_grant")
    return grant.to_wire_dict()


def _write_grant(value: Any) -> ArtifactWriteGrant:
    payload = _exact_object(
        value,
        "output_grant",
        {
            "schema_version",
            "handle",
            "tenant_id",
            "worker_id",
            "run_id",
            "node_id",
            "attempt_id",
            "action_digest",
            "authorization_digest",
            "kind",
            "sensitivity",
            "media_type",
            "maximum_bytes",
            "declared_sha256",
            "issued_at",
            "expires_at",
        },
    )
    handle = _output_handle(payload["handle"])
    try:
        grant = ArtifactWriteGrant(
            schema_version=payload["schema_version"],
            grant_id=handle.grant_id,
            token=handle.token,
            tenant_id=payload["tenant_id"],
            worker_id=payload["worker_id"],
            run_id=payload["run_id"],
            node_id=payload["node_id"],
            attempt_id=payload["attempt_id"],
            action_digest=payload["action_digest"],
            authorization_digest=payload["authorization_digest"],
            kind=payload["kind"],
            sensitivity=payload["sensitivity"],
            media_type=payload["media_type"],
            maximum_bytes=payload["maximum_bytes"],
            declared_sha256=payload["declared_sha256"],
            issued_at=payload["issued_at"],
            expires_at=payload["expires_at"],
        )
    except (ArtifactGrantDenied, TypeError, ValueError) as exc:
        raise RemoteProtocolError("invalid_output_grant") from exc
    if grant.expires_at - grant.issued_at > MAX_GRANT_TTL_SECONDS:
        raise RemoteProtocolError("invalid_output_grant")
    if _write_grant_to_wire(grant) != _detach_json(payload):
        raise RemoteProtocolError("noncanonical_output_grant")
    return grant


def _write_grant_to_wire(grant: ArtifactWriteGrant) -> dict[str, Any]:
    if not isinstance(grant, ArtifactWriteGrant):
        raise RemoteProtocolError("invalid_output_grant")
    return {
        "schema_version": grant.schema_version,
        "handle": grant.output_handle.to_wire_dict(),
        "tenant_id": grant.tenant_id,
        "worker_id": grant.worker_id,
        "run_id": grant.run_id,
        "node_id": grant.node_id,
        "attempt_id": grant.attempt_id,
        "action_digest": grant.action_digest,
        "authorization_digest": grant.authorization_digest,
        "kind": grant.kind.value,
        "sensitivity": grant.sensitivity.value,
        "media_type": grant.media_type,
        "maximum_bytes": grant.maximum_bytes,
        "declared_sha256": grant.declared_sha256,
        "issued_at": grant.issued_at,
        "expires_at": grant.expires_at,
    }


def _output_handle(value: Any) -> ArtifactOutputHandle:
    try:
        handle = ArtifactOutputHandle.from_wire_dict(value)
    except (ArtifactGrantDenied, TypeError, ValueError) as exc:
        raise RemoteProtocolError("invalid_output_handle") from exc
    if handle.to_wire_dict() != _detach_json(value):
        raise RemoteProtocolError("noncanonical_output_handle")
    return handle


def _sandbox_outcome_matches(remote_outcome: str, sandbox_outcome: Any) -> bool:
    allowed = {
        "succeeded": {"succeeded"},
        "failed": {"failed", "backend_error"},
        "timed_out": {"timed_out"},
        "abandoned": {"failed", "backend_error"},
        "outcome_unknown": {"cancellation_unknown", "backend_error"},
        "cancelled": {"cancelled"},
    }
    return sandbox_outcome in allowed.get(remote_outcome, set())


def _exact_object(
    value: Any,
    field_name: str,
    fields: set[str],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise RemoteProtocolError(f"invalid_{field_name}")
    detached = dict(value)
    if set(detached) != fields or not all(isinstance(key, str) for key in detached):
        raise RemoteProtocolError(f"invalid_{field_name}")
    return detached


def _detach_json(value: Any) -> Any:
    try:
        encoded = json.dumps(
            _plain_json(value),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        detached = json.loads(encoded)
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise RemoteProtocolError("invalid_json") from exc
    _validate_wire_value(detached)
    return detached


def _plain_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _plain_json(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_json(child) for child in value]
    return value


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {key: _freeze_json(child) for key, child in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_json(child) for child in value)
    return value


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(child) for child in value]
    return value


def _validate_wire_value(value: Any, *, field_name: str = "message") -> None:
    stack = [(value, 1)]
    item_count = 0
    while stack:
        item, depth = stack.pop()
        if depth > MAX_REMOTE_DEPTH:
            raise RemoteProtocolError("message_too_deep")
        if isinstance(item, str):
            if len(item) > MAX_REMOTE_STRING_CHARS:
                raise RemoteProtocolError("string_too_large")
            if any(ord(character) < 32 or ord(character) == 127 for character in item):
                raise RemoteProtocolError(f"invalid_{field_name}")
        elif item is None or isinstance(item, bool):
            continue
        elif isinstance(item, int):
            if abs(item) > 2**63 - 1:
                raise RemoteProtocolError(f"invalid_{field_name}")
        elif isinstance(item, float):
            if not math.isfinite(item):
                raise RemoteProtocolError(f"invalid_{field_name}")
        elif isinstance(item, list):
            item_count += len(item)
            if item_count > MAX_REMOTE_CONTAINER_ITEMS:
                raise RemoteProtocolError("message_too_many_items")
            stack.extend((child, depth + 1) for child in item)
        elif isinstance(item, dict):
            item_count += len(item)
            if item_count > MAX_REMOTE_CONTAINER_ITEMS:
                raise RemoteProtocolError("message_too_many_items")
            if not all(isinstance(key, str) for key in item):
                raise RemoteProtocolError(f"invalid_{field_name}")
            stack.extend((child, depth + 1) for child in item.values())
        else:
            raise RemoteProtocolError("invalid_json")
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise RemoteProtocolError("invalid_json") from exc
    if len(encoded) > MAX_REMOTE_MESSAGE_BYTES:
        raise RemoteProtocolError("message_too_large")


def _bounded_text(value: Any, field_name: str, *, maximum: int) -> str:
    if not isinstance(value, str):
        raise RemoteProtocolError(f"invalid_{field_name}")
    text = value.strip()
    if (
        not text
        or len(text) > maximum
        or any(ord(character) < 32 or ord(character) == 127 for character in text)
    ):
        raise RemoteProtocolError(f"invalid_{field_name}")
    return text


def _identifier(value: Any, field_name: str) -> str:
    text = _bounded_text(value, field_name, maximum=255)
    if not _IDENTIFIER_RE.fullmatch(text):
        raise RemoteProtocolError(f"invalid_{field_name}")
    return text


def _digest(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise RemoteProtocolError(f"invalid_{field_name}")
    return value


def _safe_code(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not _SAFE_CODE_RE.fullmatch(value):
        raise RemoteProtocolError(f"invalid_{field_name}")
    return value


def _integer(
    value: Any,
    field_name: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RemoteProtocolError(f"invalid_{field_name}")
    if value < minimum or value > maximum:
        raise RemoteProtocolError(f"invalid_{field_name}")
    return value


def _finite_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RemoteProtocolError(f"invalid_{field_name}")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise RemoteProtocolError(f"invalid_{field_name}")
    return number


def _positive_number(
    value: Any,
    field_name: str,
    *,
    maximum: float,
) -> float:
    number = _finite_number(value, field_name)
    if number <= 0 or number > maximum:
        raise RemoteProtocolError(f"invalid_{field_name}")
    return number


def _lease_seconds(value: Any) -> float:
    number = _finite_number(value, "lease_seconds")
    if number < MIN_LEASE_SECONDS or number > MAX_LEASE_SECONDS:
        raise RemoteProtocolError("invalid_lease_seconds")
    return number


def _bounded_unique_texts(
    value: Any,
    field_name: str,
    *,
    maximum: int,
    capability: bool = False,
    identifier: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or len(value) > maximum:
        raise RemoteProtocolError(f"invalid_{field_name}")
    normalized: list[str] = []
    for item in value:
        if capability:
            text = _safe_code(item, field_name)
        elif identifier:
            text = _identifier(item, field_name)
        else:
            text = _bounded_text(item, field_name, maximum=255)
        normalized.append(text)
    if len(set(normalized)) != len(normalized) or normalized != sorted(normalized):
        raise RemoteProtocolError(f"noncanonical_{field_name}")
    return tuple(normalized)


__all__ = [
    "AuthenticatedWorker",
    "ClaimBinding",
    "ExecutionAuthority",
    "ExecutionAuthorization",
    "ExecutionAuthorizationBinding",
    "MAX_REMOTE_MESSAGE_BYTES",
    "REMOTE_PROTOCOL",
    "REMOTE_PROTOCOL_VERSION",
    "RemoteActivityDescriptor",
    "RemoteExecutionPlan",
    "RemoteOperation",
    "RemoteProtocolError",
    "RemoteRequest",
    "RemoteResponse",
    "RemoteRuntimeProof",
    "WorkAssignment",
    "canonical_bytes",
    "canonical_digest",
    "claim_body",
    "grant_binding_digest",
    "make_request",
    "make_response",
    "output_handles_from_completion",
    "parse_request",
    "parse_response",
    "runtime_binding_digest",
]
