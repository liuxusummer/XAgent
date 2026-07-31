"""Reference-only model gateway grants that never expose provider credentials."""

from __future__ import annotations

import hashlib
import json
import math
import re
import secrets
import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Callable, Iterable, Mapping, Protocol, runtime_checkable

from .artifacts import (
    ArtifactEncryption,
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    ArtifactStore,
    LocalArtifactStore,
)
from .remote_execution_journal import (
    MAX_PROVIDER_RECOVERY_EVIDENCE_PER_INVOCATION,
    RemoteExecutionJournal,
    RemoteExecutionJournalCapacityError,
    RemoteExecutionJournalError,
    RemoteProviderGrantConflict,
    RemoteProviderGrantRecord,
    RemoteProviderGrantUnavailable,
    RemoteProviderInvocationClaim,
    RemoteProviderInvocationConflict,
    RemoteProviderInvocationRecord,
    RemoteProviderInvocationUnknown,
    RemoteProviderRecoveryEvidenceRecord,
)
from .worker_security import (
    WorkerAuthorization,
    WorkerAuthorizationVerifier,
)

PROVIDER_ACCESS_SCHEMA_VERSION = 2
PROVIDER_OPERATION_RECOVERY_SCHEMA_VERSION = 1
PROVIDER_INVOCATION_RECEIPT_SCHEMA_VERSION = 1
MAX_PROVIDER_GRANT_TTL_SECONDS = 5 * 60.0
MAX_PROVIDER_REQUEST_BYTES = 16 * 1024 * 1024
MAX_PROVIDER_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_PROVIDER_INVOCATION_INDEX = 1_000_000
MAX_ACTIVE_PROVIDER_GRANTS = 100_000
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_CODE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,254}$")
_TOKEN = re.compile(r"^[A-Za-z0-9_-]{32,512}$")
_ARTIFACT_REF_KEYS = frozenset(
    {
        "schema_version",
        "artifact_id",
        "sha256",
        "size",
        "media_type",
        "kind",
        "uri",
        "sensitivity",
        "encryption",
        "producer_run_id",
        "producer_node_id",
        "producer_attempt_id",
        "encryption_key_ref",
        "metadata",
        "created_at",
    }
)


class ProviderAccessDenied(RuntimeError):
    """A model gateway grant operation failed closed."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = _code(reason_code, "invalid_reason_code")
        super().__init__(self.reason_code)


class ProviderOperationState(StrEnum):
    """Trusted gateway view of one stable provider operation id."""

    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    UNKNOWN = "unknown"


class ProviderInvocationCompletionMode(StrEnum):
    """How durable provider completion was established."""

    INVOKED = "invoked"
    RECOVERED_COMPLETED = "recovered_completed"


@dataclass(frozen=True, slots=True)
class ProviderRouteDescriptor:
    """Deployment-owned, credential-free model gateway route metadata."""

    route_id: str
    tenant_id: str
    pool_id: str
    worker_rule_id: str
    provider: str
    model: str
    gateway_binding_digest: str
    maximum_request_bytes: int
    maximum_response_bytes: int
    schema_version: int = PROVIDER_ACCESS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in ("route_id", "provider", "model"):
            object.__setattr__(
                self,
                field_name,
                _code(
                    getattr(self, field_name),
                    "invalid_provider_route",
                ),
            )
        for field_name in (
            "tenant_id",
            "pool_id",
            "worker_rule_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(
                    getattr(self, field_name),
                    "invalid_provider_route",
                ),
            )
        object.__setattr__(
            self,
            "gateway_binding_digest",
            _digest(
                self.gateway_binding_digest,
                "invalid_gateway_binding_digest",
            ),
        )
        for field_name, maximum in (
            ("maximum_request_bytes", MAX_PROVIDER_REQUEST_BYTES),
            ("maximum_response_bytes", MAX_PROVIDER_RESPONSE_BYTES),
        ):
            value = getattr(self, field_name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 1 <= value <= maximum
            ):
                raise ProviderAccessDenied(
                    "invalid_provider_route_limits"
                )
        if (
            isinstance(self.schema_version, bool)
            or self.schema_version != PROVIDER_ACCESS_SCHEMA_VERSION
        ):
            raise ProviderAccessDenied(
                "unsupported_provider_access_schema"
            )

    @property
    def route_digest(self) -> str:
        return _canonical_digest(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "route_id": self.route_id,
            "tenant_id": self.tenant_id,
            "pool_id": self.pool_id,
            "worker_rule_id": self.worker_rule_id,
            "provider": self.provider,
            "model": self.model,
            "gateway_binding_digest": self.gateway_binding_digest,
            "maximum_request_bytes": self.maximum_request_bytes,
            "maximum_response_bytes": self.maximum_response_bytes,
        }

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> "ProviderRouteDescriptor":
        required = {
            "schema_version",
            "route_id",
            "tenant_id",
            "pool_id",
            "worker_rule_id",
            "provider",
            "model",
            "gateway_binding_digest",
            "maximum_request_bytes",
            "maximum_response_bytes",
        }
        if not isinstance(value, Mapping) or set(value) != required:
            raise ProviderAccessDenied("invalid_provider_route")
        return cls(
            schema_version=value["schema_version"],
            route_id=value["route_id"],
            tenant_id=value["tenant_id"],
            pool_id=value["pool_id"],
            worker_rule_id=value["worker_rule_id"],
            provider=value["provider"],
            model=value["model"],
            gateway_binding_digest=value["gateway_binding_digest"],
            maximum_request_bytes=value["maximum_request_bytes"],
            maximum_response_bytes=value["maximum_response_bytes"],
        )


@dataclass(frozen=True, slots=True)
class ProviderAccessGrant:
    """Short-lived one-call gateway credential, never an upstream API key."""

    grant_id: str
    token: str = field(repr=False)
    tenant_id: str = ""
    worker_id: str = ""
    run_id: str = ""
    node_id: str = ""
    attempt_id: str = ""
    action_digest: str = ""
    authorization_digest: str = ""
    request_digest: str = ""
    request_artifact_digest: str = ""
    invocation_index: int = 0
    response_sensitivity: ArtifactSensitivity = (
        ArtifactSensitivity.SENSITIVE
    )
    route: ProviderRouteDescriptor | None = None
    issued_at: float = 0
    expires_at: float = 0
    schema_version: int = PROVIDER_ACCESS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "grant_id",
            _code(self.grant_id, "invalid_provider_grant"),
        )
        object.__setattr__(
            self,
            "token",
            _opaque_token(self.token),
        )
        for field_name in (
            "tenant_id",
            "worker_id",
            "run_id",
            "node_id",
            "attempt_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(
                    getattr(self, field_name),
                    "invalid_provider_grant_binding",
                ),
            )
        for field_name in (
            "action_digest",
            "authorization_digest",
            "request_digest",
            "request_artifact_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _digest(
                    getattr(self, field_name),
                    "invalid_provider_grant_binding",
                ),
            )
        if (
            isinstance(self.invocation_index, bool)
            or not isinstance(self.invocation_index, int)
            or not 1
            <= self.invocation_index
            <= MAX_PROVIDER_INVOCATION_INDEX
        ):
            raise ProviderAccessDenied(
                "invalid_provider_invocation_index"
            )
        if not isinstance(self.route, ProviderRouteDescriptor):
            raise ProviderAccessDenied("invalid_provider_route")
        try:
            object.__setattr__(
                self,
                "response_sensitivity",
                ArtifactSensitivity(self.response_sensitivity),
            )
        except (TypeError, ValueError):
            raise ProviderAccessDenied(
                "invalid_provider_result_sensitivity"
            ) from None
        object.__setattr__(
            self,
            "issued_at",
            _timestamp(self.issued_at),
        )
        object.__setattr__(
            self,
            "expires_at",
            _timestamp(self.expires_at),
        )
        if (
            self.expires_at <= self.issued_at
            or self.expires_at - self.issued_at
            > MAX_PROVIDER_GRANT_TTL_SECONDS
        ):
            raise ProviderAccessDenied("invalid_provider_grant_window")
        if (
            isinstance(self.schema_version, bool)
            or self.schema_version != PROVIDER_ACCESS_SCHEMA_VERSION
        ):
            raise ProviderAccessDenied(
                "unsupported_provider_access_schema"
            )

    @property
    def binding_digest(self) -> str:
        value = self.to_wire_dict()
        value.pop("token")
        return _canonical_digest(value)

    def to_wire_dict(self) -> dict[str, Any]:
        """Serialize the gateway bearer, never an upstream credential."""

        return {
            "schema_version": self.schema_version,
            "grant_id": self.grant_id,
            "token": self.token,
            "tenant_id": self.tenant_id,
            "worker_id": self.worker_id,
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "action_digest": self.action_digest,
            "authorization_digest": self.authorization_digest,
            "request_digest": self.request_digest,
            "request_artifact_digest": (
                self.request_artifact_digest
            ),
            "invocation_index": self.invocation_index,
            "response_sensitivity": self.response_sensitivity.value,
            "route": self.route.to_dict(),
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_wire_dict(
        cls,
        value: Mapping[str, Any],
    ) -> "ProviderAccessGrant":
        required = {
            "schema_version",
            "grant_id",
            "token",
            "tenant_id",
            "worker_id",
            "run_id",
            "node_id",
            "attempt_id",
            "action_digest",
            "authorization_digest",
            "request_digest",
            "request_artifact_digest",
            "invocation_index",
            "response_sensitivity",
            "route",
            "issued_at",
            "expires_at",
        }
        if not isinstance(value, Mapping) or set(value) != required:
            raise ProviderAccessDenied("invalid_provider_grant")
        invalid = False
        try:
            grant = cls(
                schema_version=value["schema_version"],
                grant_id=value["grant_id"],
                token=value["token"],
                tenant_id=value["tenant_id"],
                worker_id=value["worker_id"],
                run_id=value["run_id"],
                node_id=value["node_id"],
                attempt_id=value["attempt_id"],
                action_digest=value["action_digest"],
                authorization_digest=value["authorization_digest"],
                request_digest=value["request_digest"],
                request_artifact_digest=value[
                    "request_artifact_digest"
                ],
                invocation_index=value["invocation_index"],
                response_sensitivity=value["response_sensitivity"],
                route=ProviderRouteDescriptor.from_dict(
                    value["route"]
                ),
                issued_at=value["issued_at"],
                expires_at=value["expires_at"],
            )
        except ProviderAccessDenied:
            raise
        except (TypeError, ValueError, KeyError):
            invalid = True
            grant = None
        if invalid or not isinstance(grant, cls):
            raise ProviderAccessDenied(
                "invalid_provider_grant"
            )
        if grant.to_wire_dict() != dict(value):
            raise ProviderAccessDenied("noncanonical_provider_grant")
        return grant


@dataclass(frozen=True, slots=True)
class ProviderRecoveryEvidenceBinding:
    """Payload-free recovery evidence included in a provider receipt."""

    sequence: int
    decision: str
    evidence_digest: str
    verifier_id: str

    def __post_init__(self) -> None:
        if (
            isinstance(self.sequence, bool)
            or not isinstance(self.sequence, int)
            or not 1
            <= self.sequence
            <= MAX_PROVIDER_RECOVERY_EVIDENCE_PER_INVOCATION
        ):
            raise ProviderAccessDenied(
                "invalid_provider_receipt_evidence"
            )
        if self.decision not in {"not_started", "completed"}:
            raise ProviderAccessDenied(
                "invalid_provider_receipt_evidence"
            )
        object.__setattr__(
            self,
            "evidence_digest",
            _digest(
                self.evidence_digest,
                "invalid_provider_receipt_evidence",
            ),
        )
        object.__setattr__(
            self,
            "verifier_id",
            _code(
                self.verifier_id,
                "invalid_provider_receipt_evidence",
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "decision": self.decision,
            "evidence_digest": self.evidence_digest,
            "verifier_id": self.verifier_id,
        }

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> "ProviderRecoveryEvidenceBinding":
        required = {
            "sequence",
            "decision",
            "evidence_digest",
            "verifier_id",
        }
        if not isinstance(value, Mapping) or set(value) != required:
            raise ProviderAccessDenied(
                "invalid_provider_receipt_evidence"
            )
        return cls(
            sequence=value["sequence"],
            decision=value["decision"],
            evidence_digest=value["evidence_digest"],
            verifier_id=value["verifier_id"],
        )


@dataclass(frozen=True, slots=True)
class ProviderInvocationReceipt:
    """Canonical, payload-free evidence for one completed provider call."""

    grant_id: str
    run_id: str
    node_id: str
    attempt_id: str
    action_digest: str
    authorization_digest: str
    request_digest: str
    request_artifact_digest: str
    request_payload_digest: str
    invocation_index: int
    route_id: str
    route_digest: str
    grant_binding_digest: str
    response_digest: str
    response_artifact_ref_digest: str | None
    response_sensitivity: ArtifactSensitivity
    completion_mode: ProviderInvocationCompletionMode
    recovery_evidence: tuple[
        ProviderRecoveryEvidenceBinding,
        ...,
    ] = ()
    schema_version: int = PROVIDER_INVOCATION_RECEIPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "grant_id",
            _code(self.grant_id, "invalid_provider_receipt"),
        )
        for field_name in ("run_id", "node_id", "attempt_id"):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(
                    getattr(self, field_name),
                    "invalid_provider_receipt",
                ),
            )
        for field_name in (
            "action_digest",
            "authorization_digest",
            "request_digest",
            "request_artifact_digest",
            "request_payload_digest",
            "route_digest",
            "grant_binding_digest",
            "response_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _digest(
                    getattr(self, field_name),
                    "invalid_provider_receipt",
                ),
            )
        if self.response_artifact_ref_digest is not None:
            object.__setattr__(
                self,
                "response_artifact_ref_digest",
                _digest(
                    self.response_artifact_ref_digest,
                    "invalid_provider_receipt",
                ),
            )
        if (
            isinstance(self.invocation_index, bool)
            or not isinstance(self.invocation_index, int)
            or not 1
            <= self.invocation_index
            <= MAX_PROVIDER_INVOCATION_INDEX
        ):
            raise ProviderAccessDenied(
                "invalid_provider_receipt"
            )
        object.__setattr__(
            self,
            "route_id",
            _code(self.route_id, "invalid_provider_receipt"),
        )
        try:
            sensitivity = ArtifactSensitivity(
                self.response_sensitivity
            )
            completion_mode = ProviderInvocationCompletionMode(
                self.completion_mode
            )
        except (TypeError, ValueError):
            raise ProviderAccessDenied(
                "invalid_provider_receipt"
            ) from None
        object.__setattr__(
            self,
            "response_sensitivity",
            sensitivity,
        )
        object.__setattr__(
            self,
            "completion_mode",
            completion_mode,
        )
        evidence = tuple(self.recovery_evidence)
        if (
            len(evidence)
            > MAX_PROVIDER_RECOVERY_EVIDENCE_PER_INVOCATION
            or not all(
                type(item) is ProviderRecoveryEvidenceBinding
                for item in evidence
            )
            or tuple(item.sequence for item in evidence)
            != tuple(range(1, len(evidence) + 1))
            or len({item.evidence_digest for item in evidence})
            != len(evidence)
        ):
            raise ProviderAccessDenied(
                "invalid_provider_receipt_evidence"
            )
        completed_indexes = tuple(
            index
            for index, item in enumerate(evidence)
            if item.decision == "completed"
        )
        if (
            completion_mode
            is ProviderInvocationCompletionMode.RECOVERED_COMPLETED
            and completed_indexes != (len(evidence) - 1,)
        ) or (
            completion_mode
            is ProviderInvocationCompletionMode.INVOKED
            and completed_indexes
        ):
            raise ProviderAccessDenied(
                "invalid_provider_receipt_completion"
            )
        object.__setattr__(self, "recovery_evidence", evidence)
        if (
            isinstance(self.schema_version, bool)
            or self.schema_version
            != PROVIDER_INVOCATION_RECEIPT_SCHEMA_VERSION
        ):
            raise ProviderAccessDenied(
                "unsupported_provider_receipt_schema"
            )

    @property
    def receipt_digest(self) -> str:
        return _canonical_digest(self.to_dict())

    def validate_binding(
        self,
        grant: ProviderAccessGrant,
        *,
        request_payload_digest: str,
        result: "ProviderInvocationResult",
    ) -> None:
        if (
            type(self) is not ProviderInvocationReceipt
            or type(grant) is not ProviderAccessGrant
            or type(result) is not ProviderInvocationResult
        ):
            raise ProviderAccessDenied(
                "provider_receipt_binding_mismatch"
            )
        expected_artifact_digest = (
            None
            if result.artifact_ref is None
            else _canonical_digest(result.artifact_ref.to_dict())
        )
        if (
            self.grant_id != grant.grant_id
            or self.run_id != grant.run_id
            or self.node_id != grant.node_id
            or self.attempt_id != grant.attempt_id
            or self.action_digest != grant.action_digest
            or self.authorization_digest
            != grant.authorization_digest
            or self.request_digest != grant.request_digest
            or self.request_artifact_digest
            != grant.request_artifact_digest
            or self.request_payload_digest
            != _digest(
                request_payload_digest,
                "provider_receipt_binding_mismatch",
            )
            or self.invocation_index != grant.invocation_index
            or self.route_id != grant.route.route_id
            or self.route_digest != grant.route.route_digest
            or self.grant_binding_digest != grant.binding_digest
            or self.response_digest != result.response_digest
            or self.response_artifact_ref_digest
            != expected_artifact_digest
            or self.response_sensitivity
            is not grant.response_sensitivity
            or result.receipt != self
        ):
            raise ProviderAccessDenied(
                "provider_receipt_binding_mismatch"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": "provider_invocation_receipt",
            "grant_id": self.grant_id,
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "action_digest": self.action_digest,
            "authorization_digest": self.authorization_digest,
            "request_digest": self.request_digest,
            "request_artifact_digest": (
                self.request_artifact_digest
            ),
            "request_payload_digest": self.request_payload_digest,
            "invocation_index": self.invocation_index,
            "route_id": self.route_id,
            "route_digest": self.route_digest,
            "grant_binding_digest": self.grant_binding_digest,
            "response_digest": self.response_digest,
            "response_artifact_ref_digest": (
                self.response_artifact_ref_digest
            ),
            "response_sensitivity": self.response_sensitivity.value,
            "completion_mode": self.completion_mode.value,
            "recovery_evidence": [
                item.to_dict() for item in self.recovery_evidence
            ],
        }

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> "ProviderInvocationReceipt":
        required = {
            "schema_version",
            "kind",
            "grant_id",
            "run_id",
            "node_id",
            "attempt_id",
            "action_digest",
            "authorization_digest",
            "request_digest",
            "request_artifact_digest",
            "request_payload_digest",
            "invocation_index",
            "route_id",
            "route_digest",
            "grant_binding_digest",
            "response_digest",
            "response_artifact_ref_digest",
            "response_sensitivity",
            "completion_mode",
            "recovery_evidence",
        }
        if (
            not isinstance(value, Mapping)
            or set(value) != required
            or value.get("kind")
            != "provider_invocation_receipt"
            or not isinstance(
                value.get("recovery_evidence"),
                list,
            )
        ):
            raise ProviderAccessDenied(
                "invalid_provider_receipt"
            )
        return cls(
            schema_version=value["schema_version"],
            grant_id=value["grant_id"],
            run_id=value["run_id"],
            node_id=value["node_id"],
            attempt_id=value["attempt_id"],
            action_digest=value["action_digest"],
            authorization_digest=value["authorization_digest"],
            request_digest=value["request_digest"],
            request_artifact_digest=value[
                "request_artifact_digest"
            ],
            request_payload_digest=value[
                "request_payload_digest"
            ],
            invocation_index=value["invocation_index"],
            route_id=value["route_id"],
            route_digest=value["route_digest"],
            grant_binding_digest=value[
                "grant_binding_digest"
            ],
            response_digest=value["response_digest"],
            response_artifact_ref_digest=value[
                "response_artifact_ref_digest"
            ],
            response_sensitivity=value[
                "response_sensitivity"
            ],
            completion_mode=value["completion_mode"],
            recovery_evidence=tuple(
                ProviderRecoveryEvidenceBinding.from_dict(item)
                for item in value["recovery_evidence"]
            ),
        )


@dataclass(frozen=True, slots=True)
class ProviderInvocationResult:
    """Bounded ephemeral gateway response with a payload-safe repr."""

    grant_id: str
    route_id: str
    response_digest: str
    content: bytes = field(repr=False)
    receipt: ProviderInvocationReceipt = field(repr=False)
    artifact_ref: ArtifactRef | None = field(
        default=None,
        repr=False,
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "grant_id",
            _code(self.grant_id, "invalid_provider_result"),
        )
        object.__setattr__(
            self,
            "route_id",
            _code(self.route_id, "invalid_provider_result"),
        )
        object.__setattr__(
            self,
            "response_digest",
            _digest(
                self.response_digest,
                "invalid_provider_result",
            ),
        )
        if (
            not isinstance(self.content, bytes)
            or len(self.content) > MAX_PROVIDER_RESPONSE_BYTES
            or hashlib.sha256(self.content).hexdigest()
            != self.response_digest
        ):
            raise ProviderAccessDenied("invalid_provider_result")
        if self.artifact_ref is not None:
            if (
                not isinstance(self.artifact_ref, ArtifactRef)
                or self.artifact_ref.sha256 != self.response_digest
                or self.artifact_ref.size != len(self.content)
            ):
                raise ProviderAccessDenied(
                    "invalid_provider_result"
                )
        if (
            type(self.receipt) is not ProviderInvocationReceipt
            or self.receipt.grant_id != self.grant_id
            or self.receipt.route_id != self.route_id
            or self.receipt.response_digest != self.response_digest
            or self.receipt.response_artifact_ref_digest
            != (
                None
                if self.artifact_ref is None
                else _canonical_digest(
                    self.artifact_ref.to_dict()
                )
            )
        ):
            raise ProviderAccessDenied("invalid_provider_result")


@dataclass(frozen=True, slots=True)
class ProviderOperationRecovery:
    """Strict evidence returned by a deployment-owned provider gateway."""

    operation_id: str
    route_id: str
    request_payload_digest: str
    state: ProviderOperationState
    evidence_digest: str
    verifier_id: str
    response_digest: str | None = None
    content: bytes | None = field(default=None, repr=False)
    schema_version: int = PROVIDER_OPERATION_RECOVERY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "operation_id",
            _code(
                self.operation_id,
                "invalid_provider_operation_recovery",
            ),
        )
        object.__setattr__(
            self,
            "route_id",
            _code(
                self.route_id,
                "invalid_provider_operation_recovery",
            ),
        )
        object.__setattr__(
            self,
            "request_payload_digest",
            _digest(
                self.request_payload_digest,
                "invalid_provider_operation_recovery",
            ),
        )
        try:
            state = ProviderOperationState(self.state)
        except (TypeError, ValueError):
            raise ProviderAccessDenied(
                "invalid_provider_operation_recovery"
            ) from None
        object.__setattr__(self, "state", state)
        object.__setattr__(
            self,
            "evidence_digest",
            _digest(
                self.evidence_digest,
                "invalid_provider_operation_recovery",
            ),
        )
        object.__setattr__(
            self,
            "verifier_id",
            _code(
                self.verifier_id,
                "invalid_provider_operation_recovery",
            ),
        )
        if state is ProviderOperationState.COMPLETED:
            if self.response_digest is None:
                raise ProviderAccessDenied(
                    "invalid_provider_operation_recovery"
                )
            response_digest = _digest(
                self.response_digest,
                "invalid_provider_operation_recovery",
            )
            if (
                not isinstance(self.content, bytes)
                or not self.content
                or len(self.content) > MAX_PROVIDER_RESPONSE_BYTES
                or hashlib.sha256(self.content).hexdigest()
                != response_digest
            ):
                raise ProviderAccessDenied(
                    "invalid_provider_operation_recovery"
                )
        elif self.response_digest is not None or self.content is not None:
            raise ProviderAccessDenied(
                "invalid_provider_operation_recovery"
            )
        if (
            isinstance(self.schema_version, bool)
            or self.schema_version
            != PROVIDER_OPERATION_RECOVERY_SCHEMA_VERSION
        ):
            raise ProviderAccessDenied(
                "unsupported_provider_operation_recovery_schema"
            )


@runtime_checkable
class ProviderInvoker(Protocol):
    """Deployment-owned gateway adapter that holds upstream credentials."""

    def invoke(
        self,
        route: ProviderRouteDescriptor,
        payload: bytes,
        *,
        request_id: str,
    ) -> bytes: ...


@runtime_checkable
class RecoverableProviderInvoker(ProviderInvoker, Protocol):
    """Trusted adapter that can verify a stable operation after failure."""

    @property
    def operation_recovery_ready(self) -> bool: ...

    def recover(
        self,
        route: ProviderRouteDescriptor,
        *,
        operation_id: str,
        request_payload_digest: str,
    ) -> ProviderOperationRecovery: ...


class ProviderAccessBroker:
    """Reference one-call model gateway broker with durable tombstones."""

    def __init__(
        self,
        routes: Iterable[ProviderRouteDescriptor],
        *,
        authorization_verifier: WorkerAuthorizationVerifier,
        invoker: ProviderInvoker,
        clock: Callable[[], float] = time.time,
        maximum_grant_ttl_seconds: float = (
            MAX_PROVIDER_GRANT_TTL_SECONDS
        ),
        maximum_active_grants: int = MAX_ACTIVE_PROVIDER_GRANTS,
        recovery_journal: RemoteExecutionJournal | None = None,
        result_store: ArtifactStore | None = None,
    ) -> None:
        if not isinstance(
            authorization_verifier,
            WorkerAuthorizationVerifier,
        ):
            raise ProviderAccessDenied(
                "invalid_authorization_verifier"
            )
        if not isinstance(invoker, ProviderInvoker):
            raise ProviderAccessDenied("invalid_provider_invoker")
        route_map: dict[str, ProviderRouteDescriptor] = {}
        for route in routes:
            if (
                not isinstance(route, ProviderRouteDescriptor)
                or route.route_id in route_map
            ):
                raise ProviderAccessDenied("invalid_provider_routes")
            route_map[route.route_id] = route
        if not route_map:
            raise ProviderAccessDenied("invalid_provider_routes")
        requested_ttl = _timestamp(maximum_grant_ttl_seconds)
        if (
            requested_ttl <= 0
            or requested_ttl > MAX_PROVIDER_GRANT_TTL_SECONDS
        ):
            raise ProviderAccessDenied("invalid_provider_grant_window")
        if (
            isinstance(maximum_active_grants, bool)
            or not isinstance(maximum_active_grants, int)
            or not 1
            <= maximum_active_grants
            <= MAX_ACTIVE_PROVIDER_GRANTS
        ):
            raise ProviderAccessDenied(
                "invalid_provider_grant_capacity"
            )
        self._routes = route_map
        self._authorization_verifier = authorization_verifier
        self._invoker = invoker
        self._clock = clock
        self._maximum_grant_ttl_seconds = requested_ttl
        if recovery_journal is None:
            recovery_journal = RemoteExecutionJournal(
                maximum_provider_grants=maximum_active_grants,
            )
        elif not isinstance(recovery_journal, RemoteExecutionJournal):
            raise ProviderAccessDenied(
                "invalid_provider_recovery_journal"
            )
        elif (
            maximum_active_grants != MAX_ACTIVE_PROVIDER_GRANTS
            and recovery_journal.maximum_provider_grants
            != maximum_active_grants
        ):
            raise ProviderAccessDenied(
                "provider_grant_capacity_mismatch"
            )
        self._recovery_journal = recovery_journal
        if result_store is not None and not isinstance(
            result_store,
            ArtifactStore,
        ):
            raise ProviderAccessDenied(
                "invalid_provider_result_store"
            )
        self._result_store = result_store

    @property
    def production_security_ready(self) -> bool:
        """External ledger semantics and gateway attestation remain unproven."""

        return False

    @property
    def durable_recovery_ready(self) -> bool:
        return self._recovery_journal.durable

    @property
    def durable_result_recovery_ready(self) -> bool:
        return (
            self._recovery_journal.durable
            and isinstance(self._result_store, LocalArtifactStore)
        )

    @property
    def provider_operation_recovery_ready(self) -> bool:
        try:
            return (
                isinstance(
                    self._invoker,
                    RecoverableProviderInvoker,
                )
                and self._invoker.operation_recovery_ready is True
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return False

    def issue(
        self,
        authorization: WorkerAuthorization,
        *,
        route_id: str,
        request_digest: str,
        request_artifact_digest: str,
        invocation_index: int,
        response_sensitivity: ArtifactSensitivity | str = (
            ArtifactSensitivity.SENSITIVE
        ),
        ttl_seconds: float | None = None,
    ) -> ProviderAccessGrant:
        now = _timestamp(self._clock())
        self._verify_authorization(authorization, now)
        route_key = _code(route_id, "invalid_provider_route")
        route = self._routes.get(route_key)
        if route is None:
            raise ProviderAccessDenied("provider_route_unavailable")
        if (
            route.tenant_id != authorization.tenant_id
            or route.pool_id != authorization.pool_id
            or route.worker_rule_id != authorization.rule_id
        ):
            raise ProviderAccessDenied("provider_route_unauthorized")
        requested_ttl = (
            self._maximum_grant_ttl_seconds
            if ttl_seconds is None
            else _timestamp(ttl_seconds)
        )
        if (
            requested_ttl <= 0
            or requested_ttl > self._maximum_grant_ttl_seconds
        ):
            raise ProviderAccessDenied("provider_grant_ttl_exceeded")
        expires_at = min(
            authorization.expires_at,
            now + requested_ttl,
        )
        if expires_at <= now:
            raise ProviderAccessDenied("authorization_expired")
        try:
            requested_sensitivity = ArtifactSensitivity(
                response_sensitivity
            )
        except (TypeError, ValueError):
            raise ProviderAccessDenied(
                "invalid_provider_result_sensitivity"
            ) from None
        if _sensitivity_rank(
            requested_sensitivity
        ) > _sensitivity_rank(
            authorization.maximum_artifact_sensitivity
        ):
            raise ProviderAccessDenied(
                "provider_result_sensitivity_exceeds_policy"
            )
        if (
            requested_sensitivity
            is not authorization.maximum_artifact_sensitivity
        ):
            raise ProviderAccessDenied(
                "provider_result_sensitivity_mismatch"
            )
        if (
            requested_sensitivity is ArtifactSensitivity.SECRET
            and isinstance(self._result_store, LocalArtifactStore)
        ):
            raise ProviderAccessDenied(
                "secret_provider_result_requires_encrypted_store"
            )
        token = secrets.token_urlsafe(32)
        grant = ProviderAccessGrant(
            grant_id=f"provider-grant-{uuid.uuid4()}",
            token=token,
            tenant_id=authorization.tenant_id,
            worker_id=authorization.worker_id,
            run_id=authorization.run_id,
            node_id=authorization.node_id,
            attempt_id=authorization.attempt_id,
            action_digest=authorization.action_digest,
            authorization_digest=(
                authorization.authorization_digest
            ),
            request_digest=request_digest,
            request_artifact_digest=request_artifact_digest,
            invocation_index=invocation_index,
            response_sensitivity=requested_sensitivity,
            route=route,
            issued_at=now,
            expires_at=expires_at,
        )
        logical_invocation_digest = _canonical_digest(
            {
                "schema": "provider_logical_invocation_v1",
                "tenant_id": grant.tenant_id,
                "run_id": grant.run_id,
                "node_id": grant.node_id,
                "attempt_id": grant.attempt_id,
                "action_digest": grant.action_digest,
                "request_digest": grant.request_digest,
                "request_artifact_digest": (
                    grant.request_artifact_digest
                ),
                "invocation_index": grant.invocation_index,
            }
        )
        record = RemoteProviderGrantRecord(
            grant_id=grant.grant_id,
            token_digest=_token_digest(token),
            binding_digest=grant.binding_digest,
            logical_invocation_digest=logical_invocation_digest,
            route_id=route.route_id,
            state="issued",
            expires_at=grant.expires_at,
            updated_at=now,
        )
        failure_reason: str | None = None
        try:
            self._recovery_journal.record_provider_grant(record)
        except RemoteProviderGrantConflict as exc:
            failure_reason = (
                "provider_invocation_already_issued"
                if exc.args
                == ("provider_invocation_already_issued",)
                else "provider_grant_registry_conflict"
            )
        except RemoteExecutionJournalCapacityError:
            failure_reason = "active_provider_grant_limit_reached"
        except RemoteExecutionJournalError:
            failure_reason = "provider_grant_registry_unavailable"
        if failure_reason is not None:
            raise ProviderAccessDenied(failure_reason)
        return grant

    def invoke(
        self,
        grant: ProviderAccessGrant,
        authorization: WorkerAuthorization,
        payload: bytes,
    ) -> ProviderInvocationResult:
        if (
            not isinstance(grant, ProviderAccessGrant)
            or not isinstance(grant.route, ProviderRouteDescriptor)
        ):
            raise ProviderAccessDenied("invalid_provider_grant")
        now = _timestamp(self._clock())
        self._verify_authorization(authorization, now)
        route = self._routes.get(grant.route.route_id)
        if route is None or route != grant.route:
            raise ProviderAccessDenied("provider_route_changed")
        if (
            authorization.tenant_id != grant.tenant_id
            or authorization.worker_id != grant.worker_id
            or authorization.run_id != grant.run_id
            or authorization.node_id != grant.node_id
            or authorization.attempt_id != grant.attempt_id
            or authorization.action_digest != grant.action_digest
            or authorization.authorization_digest
            != grant.authorization_digest
        ):
            raise ProviderAccessDenied(
                "provider_authorization_binding_mismatch"
            )
        if now >= grant.expires_at or now >= authorization.expires_at:
            raise ProviderAccessDenied("provider_grant_expired")
        if (
            not isinstance(payload, bytes)
            or not payload
            or len(payload) > route.maximum_request_bytes
        ):
            raise ProviderAccessDenied(
                "provider_request_exceeds_policy"
            )
        request_payload_digest = hashlib.sha256(payload).hexdigest()
        failure_reason: str | None = None
        invocation_unknown = False
        claim = None
        try:
            claim = self._recovery_journal.claim_provider_invocation(
                grant_id=grant.grant_id,
                token_digest=_token_digest(grant.token),
                binding_digest=grant.binding_digest,
                route_id=route.route_id,
                expires_at=grant.expires_at,
                request_payload_digest=request_payload_digest,
                now=now,
            )
        except RemoteProviderGrantUnavailable:
            failure_reason = "provider_grant_unavailable"
        except RemoteProviderInvocationUnknown:
            invocation_unknown = True
        except RemoteProviderInvocationConflict:
            failure_reason = "provider_grant_registry_unavailable"
        except RemoteExecutionJournalError:
            failure_reason = "provider_grant_registry_unavailable"
        if failure_reason is not None:
            raise ProviderAccessDenied(failure_reason)
        if invocation_unknown:
            recovered = self._recover_provider_operation(
                grant,
                route,
                request_payload_digest=request_payload_digest,
            )
            if isinstance(recovered, ProviderInvocationResult):
                return recovered
            claim = recovered
        if claim is None:
            raise ProviderAccessDenied(
                "provider_grant_registry_unavailable"
            )
        if not claim.execute:
            return self._replay_provider_result(
                grant,
                route,
                claim.record,
            )
        invocation_failed = False
        try:
            response = self._invoker.invoke(
                route,
                bytes(payload),
                request_id=grant.grant_id,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            invocation_failed = True
            response = None
        if invocation_failed:
            self._mark_invocation_unknown(
                grant,
                route,
                request_payload_digest=request_payload_digest,
            )
            raise ProviderAccessDenied(
                "provider_invocation_failed"
            )
        if (
            not isinstance(response, bytes)
            or not response
            or len(response) > route.maximum_response_bytes
        ):
            self._mark_invocation_unknown(
                grant,
                route,
                request_payload_digest=request_payload_digest,
            )
            raise ProviderAccessDenied(
                "provider_response_exceeds_policy"
            )
        response_digest = hashlib.sha256(response).hexdigest()
        artifact_ref = self._persist_provider_result(
            grant,
            route,
            response,
            response_digest=response_digest,
            request_payload_digest=request_payload_digest,
        )
        completion_failed = False
        try:
            self._recovery_journal.complete_provider_invocation(
                grant_id=grant.grant_id,
                token_digest=_token_digest(grant.token),
                binding_digest=grant.binding_digest,
                route_id=route.route_id,
                expires_at=grant.expires_at,
                request_payload_digest=request_payload_digest,
                response_digest=response_digest,
                response_artifact_ref=(
                    None
                    if artifact_ref is None
                    else _provider_result_ref_json(artifact_ref)
                ),
                now=_timestamp(self._clock()),
            )
        except (
            RemoteProviderGrantUnavailable,
            RemoteProviderInvocationConflict,
            RemoteProviderInvocationUnknown,
            RemoteExecutionJournalError,
        ):
            completion_failed = True
        if completion_failed:
            raise ProviderAccessDenied(
                "provider_grant_registry_unavailable"
            )
        return self._invocation_result(
            grant,
            route,
            request_payload_digest=request_payload_digest,
            response=response,
            artifact_ref=artifact_ref,
        )

    def _recover_provider_operation(
        self,
        grant: ProviderAccessGrant,
        route: ProviderRouteDescriptor,
        *,
        request_payload_digest: str,
    ) -> RemoteProviderInvocationClaim | ProviderInvocationResult:
        invocation_failed = False
        try:
            invocation = self._recovery_journal.get_provider_invocation(
                grant.grant_id
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            invocation_failed = True
            invocation = None
        if invocation_failed:
            raise ProviderAccessDenied(
                "provider_grant_registry_unavailable"
            )
        if (
            invocation is None
            or invocation.request_payload_digest
            != request_payload_digest
            or invocation.state
            not in {"invoking", "outcome_unknown"}
        ):
            raise ProviderAccessDenied(
                "provider_invocation_outcome_unknown"
            )

        readiness_failed = False
        try:
            recoverable = isinstance(
                self._invoker,
                RecoverableProviderInvoker,
            )
            ready = (
                recoverable
                and self._invoker.operation_recovery_ready is True
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            readiness_failed = True
            recoverable = False
            ready = False
        if readiness_failed:
            raise ProviderAccessDenied(
                "provider_operation_recovery_failed"
            )
        if not ready or not recoverable:
            raise ProviderAccessDenied(
                "provider_invocation_outcome_unknown"
            )

        recovery_failed = False
        try:
            recovery = self._invoker.recover(
                route,
                operation_id=grant.grant_id,
                request_payload_digest=request_payload_digest,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            recovery_failed = True
            recovery = None
        if recovery_failed:
            raise ProviderAccessDenied(
                "provider_operation_recovery_failed"
            )
        if (
            type(recovery) is not ProviderOperationRecovery
            or recovery.operation_id != grant.grant_id
            or recovery.route_id != route.route_id
            or recovery.request_payload_digest
            != request_payload_digest
        ):
            raise ProviderAccessDenied(
                "invalid_provider_operation_recovery"
            )

        if recovery.state in {
            ProviderOperationState.IN_PROGRESS,
            ProviderOperationState.UNKNOWN,
        }:
            raise ProviderAccessDenied(
                "provider_invocation_outcome_unknown"
            )
        if recovery.state is ProviderOperationState.NOT_STARTED:
            if invocation.state != "outcome_unknown":
                # A raw `invoking` row may still have a live/zombie caller.
                raise ProviderAccessDenied(
                    "provider_invocation_outcome_unknown"
                )
            retry_failed = False
            retry_unknown = False
            retry_unavailable = False
            retry_replayed = False
            try:
                claim = (
                    self._recovery_journal
                    .claim_provider_retry_from_evidence(
                        grant_id=grant.grant_id,
                        token_digest=_token_digest(grant.token),
                        binding_digest=grant.binding_digest,
                        route_id=route.route_id,
                        expires_at=grant.expires_at,
                        request_payload_digest=(
                            request_payload_digest
                        ),
                        evidence_digest=(
                            recovery.evidence_digest
                        ),
                        verifier_id=recovery.verifier_id,
                        now=_timestamp(self._clock()),
                    )
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except RemoteProviderInvocationUnknown:
                retry_unknown = True
                claim = None
            except RemoteProviderGrantUnavailable:
                retry_unavailable = True
                claim = None
            except RemoteProviderInvocationConflict as exc:
                retry_replayed = (
                    exc.args
                    == ("provider_recovery_evidence_replayed",)
                )
                retry_failed = not retry_replayed
                claim = None
            except BaseException:
                retry_failed = True
                claim = None
            if retry_unavailable:
                raise ProviderAccessDenied(
                    "provider_grant_unavailable"
                )
            if retry_replayed:
                raise ProviderAccessDenied(
                    "provider_operation_recovery_replayed"
                )
            if retry_unknown:
                raise ProviderAccessDenied(
                    "provider_invocation_outcome_unknown"
                )
            if retry_failed or claim is None:
                raise ProviderAccessDenied(
                    "provider_grant_registry_unavailable"
                )
            return claim

        if recovery.state is not ProviderOperationState.COMPLETED:
            raise ProviderAccessDenied(
                "invalid_provider_operation_recovery"
            )
        response = recovery.content
        if (
            not isinstance(response, bytes)
            or not response
            or len(response) > route.maximum_response_bytes
            or recovery.response_digest is None
            or hashlib.sha256(response).hexdigest()
            != recovery.response_digest
        ):
            raise ProviderAccessDenied(
                "invalid_provider_operation_recovery"
            )
        response_digest = recovery.response_digest
        artifact_ref = self._persist_provider_result(
            grant,
            route,
            response,
            response_digest=response_digest,
            request_payload_digest=request_payload_digest,
        )
        completion_failed = False
        try:
            self._recovery_journal.complete_provider_invocation_from_evidence(
                grant_id=grant.grant_id,
                token_digest=_token_digest(grant.token),
                binding_digest=grant.binding_digest,
                route_id=route.route_id,
                expires_at=grant.expires_at,
                request_payload_digest=request_payload_digest,
                response_digest=response_digest,
                response_artifact_ref=(
                    None
                    if artifact_ref is None
                    else _provider_result_ref_json(artifact_ref)
                ),
                evidence_digest=recovery.evidence_digest,
                verifier_id=recovery.verifier_id,
                now=_timestamp(self._clock()),
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            completion_failed = True
        if completion_failed:
            raise ProviderAccessDenied(
                "provider_grant_registry_unavailable"
            )
        return self._invocation_result(
            grant,
            route,
            request_payload_digest=request_payload_digest,
            response=response,
            artifact_ref=artifact_ref,
        )

    def _invocation_result(
        self,
        grant: ProviderAccessGrant,
        route: ProviderRouteDescriptor,
        *,
        request_payload_digest: str,
        response: bytes,
        artifact_ref: ArtifactRef | None,
    ) -> ProviderInvocationResult:
        try:
            invocation = (
                self._recovery_journal.get_provider_invocation(
                    grant.grant_id
                )
            )
            evidence_records = (
                self._recovery_journal
                .list_provider_recovery_evidence(
                    grant.grant_id
                )
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise ProviderAccessDenied(
                "provider_invocation_receipt_unavailable"
            ) from None
        response_digest = hashlib.sha256(response).hexdigest()
        expected_artifact_ref = (
            None
            if artifact_ref is None
            else _provider_result_ref_json(artifact_ref)
        )
        if (
            type(invocation) is not RemoteProviderInvocationRecord
            or invocation.grant_id != grant.grant_id
            or invocation.state != "completed"
            or invocation.request_payload_digest
            != request_payload_digest
            or invocation.response_digest != response_digest
            or invocation.response_artifact_ref
            != expected_artifact_ref
            or not isinstance(evidence_records, tuple)
            or any(
                type(item)
                is not RemoteProviderRecoveryEvidenceRecord
                or item.grant_id != grant.grant_id
                or item.request_payload_digest
                != request_payload_digest
                for item in evidence_records
            )
        ):
            raise ProviderAccessDenied(
                "provider_invocation_receipt_unavailable"
            )
        evidence = tuple(
            ProviderRecoveryEvidenceBinding(
                sequence=item.sequence,
                decision=item.decision,
                evidence_digest=item.evidence_digest,
                verifier_id=item.verifier_id,
            )
            for item in evidence_records
        )
        completion_mode = (
            ProviderInvocationCompletionMode.RECOVERED_COMPLETED
            if evidence and evidence[-1].decision == "completed"
            else ProviderInvocationCompletionMode.INVOKED
        )
        receipt = ProviderInvocationReceipt(
            grant_id=grant.grant_id,
            run_id=grant.run_id,
            node_id=grant.node_id,
            attempt_id=grant.attempt_id,
            action_digest=grant.action_digest,
            authorization_digest=grant.authorization_digest,
            request_digest=grant.request_digest,
            request_artifact_digest=(
                grant.request_artifact_digest
            ),
            request_payload_digest=request_payload_digest,
            invocation_index=grant.invocation_index,
            route_id=route.route_id,
            route_digest=route.route_digest,
            grant_binding_digest=grant.binding_digest,
            response_digest=response_digest,
            response_artifact_ref_digest=(
                None
                if artifact_ref is None
                else _canonical_digest(artifact_ref.to_dict())
            ),
            response_sensitivity=grant.response_sensitivity,
            completion_mode=completion_mode,
            recovery_evidence=evidence,
        )
        result = ProviderInvocationResult(
            grant_id=grant.grant_id,
            route_id=route.route_id,
            response_digest=response_digest,
            content=response,
            receipt=receipt,
            artifact_ref=artifact_ref,
        )
        receipt.validate_binding(
            grant,
            request_payload_digest=request_payload_digest,
            result=result,
        )
        return result

    def _persist_provider_result(
        self,
        grant: ProviderAccessGrant,
        route: ProviderRouteDescriptor,
        response: bytes,
        *,
        response_digest: str,
        request_payload_digest: str,
    ) -> ArtifactRef | None:
        store = self._result_store
        if store is None:
            return None
        persistence_failed = False
        try:
            ref = store.put_bytes(
                response,
                media_type="application/octet-stream",
                kind=ArtifactKind.MODEL_RESPONSE,
                sensitivity=grant.response_sensitivity,
                producer_run_id=grant.run_id,
                producer_node_id=grant.node_id,
                producer_attempt_id=grant.attempt_id,
                metadata={},
            )
            valid = self._provider_result_ref_is_valid(
                grant,
                route,
                ref,
                response_digest=response_digest,
                response_size=len(response),
            )
            if valid:
                valid = store.verify(ref) is True
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            persistence_failed = True
            ref = None
            valid = False
        if persistence_failed or not valid or ref is None:
            self._mark_invocation_unknown(
                grant,
                route,
                request_payload_digest=request_payload_digest,
            )
            raise ProviderAccessDenied(
                "provider_result_persistence_failed"
            )
        return ref

    def _replay_provider_result(
        self,
        grant: ProviderAccessGrant,
        route: ProviderRouteDescriptor,
        invocation: RemoteProviderInvocationRecord,
    ) -> ProviderInvocationResult:
        store = self._result_store
        if (
            store is None
            or invocation.state != "completed"
            or invocation.response_digest is None
            or invocation.response_artifact_ref is None
        ):
            raise ProviderAccessDenied(
                "provider_result_unavailable"
            )
        replay_failed = False
        try:
            ref = _provider_result_ref_from_json(
                invocation.response_artifact_ref
            )
            if not self._provider_result_ref_is_valid(
                grant,
                route,
                ref,
                response_digest=invocation.response_digest,
                response_size=ref.size,
            ):
                replay_failed = True
                content = None
            else:
                content = store.read(ref)
                if (
                    not isinstance(content, bytes)
                    or not content
                    or len(content) != ref.size
                    or len(content) > route.maximum_response_bytes
                    or hashlib.sha256(content).hexdigest()
                    != invocation.response_digest
                ):
                    replay_failed = True
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            replay_failed = True
            ref = None
            content = None
        if replay_failed or ref is None or content is None:
            raise ProviderAccessDenied(
                "provider_result_integrity_failed"
            )
        return self._invocation_result(
            grant,
            route,
            request_payload_digest=(
                invocation.request_payload_digest
            ),
            response=content,
            artifact_ref=ref,
        )

    def _provider_result_ref_is_valid(
        self,
        grant: ProviderAccessGrant,
        route: ProviderRouteDescriptor,
        ref: object,
        *,
        response_digest: str,
        response_size: int,
    ) -> bool:
        if not isinstance(ref, ArtifactRef):
            return False
        if (
            ref.sha256 != response_digest
            or ref.size != response_size
            or ref.size <= 0
            or ref.size > route.maximum_response_bytes
            or ref.media_type != "application/octet-stream"
            or ref.kind is not ArtifactKind.MODEL_RESPONSE
            or ref.sensitivity is not grant.response_sensitivity
            or ref.producer_run_id != grant.run_id
            or ref.producer_node_id != grant.node_id
            or ref.producer_attempt_id != grant.attempt_id
            or dict(ref.metadata)
        ):
            return False
        if (
            ref.sensitivity is ArtifactSensitivity.SECRET
            and ref.encryption
            is not ArtifactEncryption.DEPLOYMENT_MANAGED
        ):
            return False
        return True

    def _mark_invocation_unknown(
        self,
        grant: ProviderAccessGrant,
        route: ProviderRouteDescriptor,
        *,
        request_payload_digest: str,
    ) -> None:
        try:
            now = _timestamp(self._clock())
            self._recovery_journal.mark_provider_invocation_unknown(
                grant_id=grant.grant_id,
                token_digest=_token_digest(grant.token),
                binding_digest=grant.binding_digest,
                route_id=route.route_id,
                expires_at=grant.expires_at,
                request_payload_digest=request_payload_digest,
                now=now,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            # The durable `invoking` row is already an unknown-outcome
            # tombstone if this best-effort refinement cannot be written.
            return

    def purge_expired(self) -> int:
        now = _timestamp(self._clock())
        purge_failed = False
        try:
            purged = self._recovery_journal.purge_expired_provider_grants(
                now=now,
            )
        except RemoteExecutionJournalError:
            purge_failed = True
            purged = 0
        if purge_failed:
            raise ProviderAccessDenied(
                "provider_grant_registry_unavailable"
            )
        return purged

    def _verify_authorization(
        self,
        authorization: WorkerAuthorization,
        now: float,
    ) -> None:
        if not isinstance(authorization, WorkerAuthorization):
            raise ProviderAccessDenied(
                "invalid_worker_authorization"
            )
        verification_failed = False
        try:
            verified = self._authorization_verifier.verify(
                authorization,
                now=now,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            verification_failed = True
            verified = False
        if verification_failed:
            raise ProviderAccessDenied(
                "worker_authorization_invalid"
            )
        if verified is not True:
            raise ProviderAccessDenied(
                "worker_authorization_invalid"
            )


def _bounded_text(value: Any, reason_code: str) -> str:
    if not isinstance(value, str):
        raise ProviderAccessDenied(reason_code)
    text = value.strip()
    if (
        not text
        or len(text) > 512
        or any(ord(character) < 32 or ord(character) == 127 for character in text)
    ):
        raise ProviderAccessDenied(reason_code)
    return text


def _code(value: Any, reason_code: str) -> str:
    if not isinstance(value, str) or _CODE.fullmatch(value) is None:
        if reason_code == "invalid_reason_code":
            raise ValueError("invalid provider access reason code")
        raise ProviderAccessDenied(reason_code)
    return value


def _digest(value: Any, reason_code: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ProviderAccessDenied(reason_code)
    return value


def _opaque_token(value: Any) -> str:
    if not isinstance(value, str) or _TOKEN.fullmatch(value) is None:
        raise ProviderAccessDenied("invalid_provider_grant")
    return value


def _timestamp(value: Any) -> float:
    if isinstance(value, bool):
        raise ProviderAccessDenied("invalid_provider_time")
    invalid = False
    try:
        result = float(value)
    except (TypeError, ValueError):
        invalid = True
        result = 0.0
    if invalid or not math.isfinite(result) or result < 0:
        raise ProviderAccessDenied("invalid_provider_time")
    return result


def _canonical_digest(value: Any) -> str:
    invalid = False
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        invalid = True
        encoded = b""
    if invalid:
        raise ProviderAccessDenied(
            "invalid_provider_metadata"
        )
    return hashlib.sha256(encoded).hexdigest()


def _token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _provider_result_ref_json(ref: ArtifactRef) -> str:
    if not isinstance(ref, ArtifactRef):
        raise ProviderAccessDenied(
            "invalid_provider_result_reference"
        )
    return _canonical_json(ref.to_dict())


def _provider_result_ref_from_json(value: str) -> ArtifactRef:
    invalid = False
    try:
        payload = json.loads(
            value,
            parse_constant=_reject_json_constant,
        )
        if (
            not isinstance(payload, dict)
            or set(payload) != _ARTIFACT_REF_KEYS
        ):
            invalid = True
            ref = None
        else:
            ref = ArtifactRef.from_dict(payload)
            if _provider_result_ref_json(ref) != value:
                invalid = True
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        invalid = True
        ref = None
    if invalid or not isinstance(ref, ArtifactRef):
        raise ProviderAccessDenied(
            "invalid_provider_result_reference"
        )
    return ref


def _canonical_json(value: Any) -> str:
    invalid = False
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError, UnicodeError, RecursionError):
        invalid = True
        encoded = ""
    if invalid:
        raise ProviderAccessDenied(
            "invalid_provider_metadata"
        )
    return encoded


def _reject_json_constant(constant: str) -> object:
    raise ValueError(f"invalid JSON constant: {constant}")


def _sensitivity_rank(value: ArtifactSensitivity) -> int:
    return {
        ArtifactSensitivity.PUBLIC: 0,
        ArtifactSensitivity.INTERNAL: 1,
        ArtifactSensitivity.SENSITIVE: 2,
        ArtifactSensitivity.SECRET: 3,
    }[value]


__all__ = [
    "MAX_ACTIVE_PROVIDER_GRANTS",
    "MAX_PROVIDER_GRANT_TTL_SECONDS",
    "MAX_PROVIDER_INVOCATION_INDEX",
    "MAX_PROVIDER_REQUEST_BYTES",
    "MAX_PROVIDER_RESPONSE_BYTES",
    "PROVIDER_ACCESS_SCHEMA_VERSION",
    "PROVIDER_INVOCATION_RECEIPT_SCHEMA_VERSION",
    "PROVIDER_OPERATION_RECOVERY_SCHEMA_VERSION",
    "ProviderAccessBroker",
    "ProviderAccessDenied",
    "ProviderAccessGrant",
    "ProviderInvocationCompletionMode",
    "ProviderInvocationReceipt",
    "ProviderInvocationResult",
    "ProviderInvoker",
    "ProviderOperationRecovery",
    "ProviderOperationState",
    "ProviderRecoveryEvidenceBinding",
    "ProviderRouteDescriptor",
    "RecoverableProviderInvoker",
]
