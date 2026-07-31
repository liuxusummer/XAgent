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
    RemoteExecutionJournal,
    RemoteExecutionJournalCapacityError,
    RemoteExecutionJournalError,
    RemoteProviderGrantConflict,
    RemoteProviderGrantRecord,
    RemoteProviderGrantUnavailable,
    RemoteProviderInvocationConflict,
    RemoteProviderInvocationRecord,
    RemoteProviderInvocationUnknown,
)
from .worker_security import (
    WorkerAuthorization,
    WorkerAuthorizationVerifier,
)

PROVIDER_ACCESS_SCHEMA_VERSION = 2
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
class ProviderInvocationResult:
    """Bounded ephemeral gateway response with a payload-safe repr."""

    grant_id: str
    route_id: str
    response_digest: str
    content: bytes = field(repr=False)
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
        """Upstream idempotency and a hardened gateway are still required."""

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
            failure_reason = "provider_invocation_outcome_unknown"
        except RemoteProviderInvocationConflict:
            failure_reason = "provider_grant_registry_unavailable"
        except RemoteExecutionJournalError:
            failure_reason = "provider_grant_registry_unavailable"
        if failure_reason is not None:
            raise ProviderAccessDenied(failure_reason)
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
        return ProviderInvocationResult(
            grant_id=grant.grant_id,
            route_id=route.route_id,
            response_digest=response_digest,
            content=response,
            artifact_ref=artifact_ref,
        )

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
        return ProviderInvocationResult(
            grant_id=grant.grant_id,
            route_id=route.route_id,
            response_digest=invocation.response_digest,
            content=content,
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
    "ProviderAccessBroker",
    "ProviderAccessDenied",
    "ProviderAccessGrant",
    "ProviderInvocationResult",
    "ProviderInvoker",
    "ProviderRouteDescriptor",
]
