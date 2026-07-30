"""Fail-closed workload identity and authorization for remote workers.

The worker never supplies a trusted :class:`WorkerIdentity` directly.  A
deployment-owned :class:`WorkloadAttestor` receives an opaque, out-of-band
presentation (for example an mTLS peer certificate or a SPIFFE workload API
context) and returns a short-lived identity.  This module only validates and
authorizes that result; it does not claim to implement certificate or workload
attestation itself.
"""

from __future__ import annotations

import hashlib
import json
import math
import threading
import time
import uuid
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Iterable, Protocol, runtime_checkable

from .artifacts import ArtifactSensitivity
from .policy import ActionRequest, Capability, EffectClass

WORKER_SECURITY_SCHEMA_VERSION = 1
MAX_IDENTITY_TTL_SECONDS = 15 * 60.0
MAX_AUTHORIZATION_TTL_SECONDS = 5 * 60.0
MAX_CLOCK_SKEW_SECONDS = 30.0
MAX_RULES = 1024
MAX_CAPABILITIES = 64
MAX_ACTIONS = 128
MAX_ACTIVE_AUTHORIZATIONS = 100_000


class WorkerSecurityError(RuntimeError):
    """Base error for the trusted remote-worker boundary."""


class WorkerIdentityInvalid(WorkerSecurityError, ValueError):
    """The deployment attestor returned an invalid or stale identity."""


class WorkerAuthorizationDenied(WorkerSecurityError):
    """The worker was not authorized; no remote execution may occur."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = _bounded_text(reason_code, "reason_code", max_chars=128)
        super().__init__(self.reason_code)


def _bounded_text(value: Any, field_name: str, *, max_chars: int = 256) -> str:
    if not isinstance(value, str):
        raise WorkerIdentityInvalid(f"{field_name} must be a string")
    text = value.strip()
    if not text:
        raise WorkerIdentityInvalid(f"{field_name} must not be empty")
    if len(text) > max_chars:
        raise WorkerIdentityInvalid(f"{field_name} exceeds its bound")
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise WorkerIdentityInvalid(f"{field_name} contains control characters")
    return text


def _sha256(value: Any, field_name: str) -> str:
    digest = str(value or "")
    if (
        len(digest) != 64
        or digest != digest.lower()
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise WorkerIdentityInvalid(f"{field_name} must be a lowercase SHA-256 digest")
    return digest


def _timestamp(value: Any, field_name: str) -> float:
    try:
        timestamp = float(value)
    except (TypeError, ValueError) as exc:
        raise WorkerIdentityInvalid(f"{field_name} must be a timestamp") from exc
    if not math.isfinite(timestamp) or timestamp < 0:
        raise WorkerIdentityInvalid(f"{field_name} must be a timestamp")
    return timestamp


def _canonical_digest(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise WorkerIdentityInvalid("identity metadata must be canonical JSON") from exc
    return hashlib.sha256(encoded).hexdigest()


def _capabilities(values: Iterable[Capability | str]) -> tuple[Capability, ...]:
    try:
        normalized = {
            (value if isinstance(value, Capability) else Capability(value)).name
            for value in values
        }
    except (TypeError, ValueError) as exc:
        raise WorkerIdentityInvalid("worker capabilities are invalid") from exc
    if len(normalized) > MAX_CAPABILITIES:
        raise WorkerIdentityInvalid("worker capabilities exceed their bound")
    return tuple(Capability(name) for name in sorted(normalized))


@dataclass(frozen=True, slots=True)
class WorkerIdentity:
    """A trusted attestor result, never a worker request payload."""

    worker_id: str
    subject: str
    issuer: str
    tenant_id: str
    pool_id: str
    capabilities: tuple[Capability, ...]
    transport_binding_digest: str
    issued_at: float
    not_before: float
    expires_at: float
    attestation_id: str
    schema_version: int = WORKER_SECURITY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in (
            "worker_id",
            "subject",
            "issuer",
            "tenant_id",
            "pool_id",
            "attestation_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(getattr(self, field_name), field_name),
            )
        object.__setattr__(
            self,
            "capabilities",
            _capabilities(self.capabilities),
        )
        object.__setattr__(
            self,
            "transport_binding_digest",
            _sha256(self.transport_binding_digest, "transport_binding_digest"),
        )
        for field_name in ("issued_at", "not_before", "expires_at"):
            object.__setattr__(
                self,
                field_name,
                _timestamp(getattr(self, field_name), field_name),
            )
        if self.not_before < self.issued_at:
            raise WorkerIdentityInvalid("not_before precedes issued_at")
        if self.expires_at <= self.not_before:
            raise WorkerIdentityInvalid("identity validity window is empty")
        if self.schema_version != WORKER_SECURITY_SCHEMA_VERSION:
            raise WorkerIdentityInvalid("unsupported worker identity schema_version")

    @property
    def identity_binding_digest(self) -> str:
        return _canonical_digest(
            {
                "schema": "worker_identity_binding_v1",
                "worker_id": self.worker_id,
                "subject": self.subject,
                "issuer": self.issuer,
                "tenant_id": self.tenant_id,
                "pool_id": self.pool_id,
                "capabilities": [
                    capability.name for capability in self.capabilities
                ],
                "transport_binding_digest": self.transport_binding_digest,
                "issued_at": self.issued_at,
                "not_before": self.not_before,
                "expires_at": self.expires_at,
                "attestation_id": self.attestation_id,
            }
        )

    def validate_current(
        self,
        *,
        now: float,
        maximum_ttl_seconds: float,
        clock_skew_seconds: float,
    ) -> None:
        current = _timestamp(now, "current time")
        if self.expires_at - self.issued_at > maximum_ttl_seconds:
            raise WorkerIdentityInvalid("worker identity lifetime exceeds policy")
        if self.issued_at > current + clock_skew_seconds:
            raise WorkerIdentityInvalid("worker identity was issued in the future")
        if current + clock_skew_seconds < self.not_before:
            raise WorkerIdentityInvalid("worker identity is not active")
        # Expiration is strict.  Clock skew must never extend credential life.
        if current >= self.expires_at:
            raise WorkerIdentityInvalid("worker identity expired")


@runtime_checkable
class WorkloadAttestor(Protocol):
    """Deployment-owned verification of an opaque transport presentation."""

    def attest(self, presentation: object, *, now: float) -> WorkerIdentity: ...


@dataclass(frozen=True, slots=True)
class WorkerAccessRule:
    """Explicit allow rule; absence of a matching rule is a denial."""

    rule_id: str
    tenant_id: str
    pool_id: str
    action_names: tuple[str, ...]
    allowed_capabilities: tuple[Capability, ...]
    allowed_effect_classes: tuple[EffectClass, ...]
    maximum_artifact_sensitivity: ArtifactSensitivity = ArtifactSensitivity.INTERNAL
    schema_version: int = WORKER_SECURITY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in ("rule_id", "tenant_id", "pool_id"):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(getattr(self, field_name), field_name),
            )
        try:
            names = {
                _bounded_text(name, "action_name", max_chars=256)
                for name in self.action_names
            }
        except TypeError as exc:
            raise WorkerIdentityInvalid("action_names must be iterable") from exc
        if not names or len(names) > MAX_ACTIONS:
            raise WorkerIdentityInvalid("action_names must be non-empty and bounded")
        if "*" in names:
            raise WorkerIdentityInvalid("wildcard worker action rules are forbidden")
        object.__setattr__(self, "action_names", tuple(sorted(names)))
        object.__setattr__(
            self,
            "allowed_capabilities",
            _capabilities(self.allowed_capabilities),
        )
        try:
            effects = {
                EffectClass(effect) for effect in self.allowed_effect_classes
            }
        except (TypeError, ValueError) as exc:
            raise WorkerIdentityInvalid("allowed_effect_classes are invalid") from exc
        if not effects:
            raise WorkerIdentityInvalid(
                "allowed_effect_classes must be explicit and non-empty"
            )
        object.__setattr__(
            self,
            "allowed_effect_classes",
            tuple(sorted(effects, key=lambda effect: effect.value)),
        )
        try:
            object.__setattr__(
                self,
                "maximum_artifact_sensitivity",
                ArtifactSensitivity(self.maximum_artifact_sensitivity),
            )
        except ValueError as exc:
            raise WorkerIdentityInvalid(
                "maximum_artifact_sensitivity is invalid"
            ) from exc
        if self.schema_version != WORKER_SECURITY_SCHEMA_VERSION:
            raise WorkerIdentityInvalid("unsupported worker access rule schema_version")


@dataclass(frozen=True, slots=True)
class WorkerAuthorization:
    """Short-lived authorization bound to one exact ActionRequest."""

    authorization_id: str
    worker_id: str
    tenant_id: str
    pool_id: str
    run_id: str
    node_id: str
    attempt_id: str
    action_digest: str
    identity_binding_digest: str
    transport_binding_digest: str
    rule_id: str
    maximum_artifact_sensitivity: ArtifactSensitivity
    issued_at: float
    expires_at: float
    schema_version: int = WORKER_SECURITY_SCHEMA_VERSION
    _issuer_binding: object | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    _identity_lineage: str = field(
        default="",
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        for field_name in (
            "authorization_id",
            "worker_id",
            "tenant_id",
            "pool_id",
            "run_id",
            "node_id",
            "attempt_id",
            "rule_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(getattr(self, field_name), field_name),
            )
        for field_name in (
            "action_digest",
            "identity_binding_digest",
            "transport_binding_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(getattr(self, field_name), field_name),
            )
        try:
            object.__setattr__(
                self,
                "maximum_artifact_sensitivity",
                ArtifactSensitivity(self.maximum_artifact_sensitivity),
            )
        except ValueError as exc:
            raise WorkerIdentityInvalid(
                "maximum_artifact_sensitivity is invalid"
            ) from exc
        object.__setattr__(self, "issued_at", _timestamp(self.issued_at, "issued_at"))
        object.__setattr__(
            self,
            "expires_at",
            _timestamp(self.expires_at, "expires_at"),
        )
        if self.expires_at <= self.issued_at:
            raise WorkerIdentityInvalid("authorization validity window is empty")
        if self.schema_version != WORKER_SECURITY_SCHEMA_VERSION:
            raise WorkerIdentityInvalid("unsupported worker authorization schema_version")

    @property
    def authorization_digest(self) -> str:
        """Stable identity of one exact authorization lineage.

        Credential ids, exact attestation timestamps, ``issued_at`` and
        ``expires_at`` are freshness, not durable execution identity.  A
        trusted gate may re-attest the same workload/session after a control
        restart without changing the digest already bound into an active
        remote claim.
        """

        identity_lineage = (
            self._identity_lineage or self.identity_binding_digest
        )
        return _canonical_digest(
            {
                "schema": "worker_authorization_lineage_v2",
                "worker_id": self.worker_id,
                "tenant_id": self.tenant_id,
                "pool_id": self.pool_id,
                "run_id": self.run_id,
                "node_id": self.node_id,
                "attempt_id": self.attempt_id,
                "action_digest": self.action_digest,
                "identity_lineage_digest": identity_lineage,
                "transport_binding_digest": self.transport_binding_digest,
                "rule_id": self.rule_id,
                "maximum_artifact_sensitivity": (
                    self.maximum_artifact_sensitivity.value
                ),
            }
        )

    def validate_for(
        self,
        *,
        action: ActionRequest,
        tenant_id: str,
        pool_id: str,
        now: float,
    ) -> None:
        if not isinstance(action, ActionRequest):
            raise WorkerAuthorizationDenied("invalid_action")
        if (
            self.tenant_id != tenant_id
            or self.pool_id != pool_id
            or self.run_id != action.run_id
            or self.node_id != action.node_id
            or self.attempt_id != action.attempt_id
            or self.action_digest != action.action_digest
        ):
            raise WorkerAuthorizationDenied("authorization_binding_mismatch")
        if _timestamp(now, "current time") >= self.expires_at:
            raise WorkerAuthorizationDenied("authorization_expired")


@dataclass(frozen=True, slots=True)
class _IssuedAuthorization:
    authorization: WorkerAuthorization
    identity_lineage_digest: str


class WorkerAuthorizationGate:
    """Attest and authorize one worker for one exact ActionRequest."""

    def __init__(
        self,
        attestor: WorkloadAttestor,
        rules: Iterable[WorkerAccessRule],
        *,
        clock: Callable[[], float] = time.time,
        maximum_identity_ttl_seconds: float = MAX_IDENTITY_TTL_SECONDS,
        authorization_ttl_seconds: float = MAX_AUTHORIZATION_TTL_SECONDS,
        clock_skew_seconds: float = MAX_CLOCK_SKEW_SECONDS,
    ) -> None:
        if not isinstance(attestor, WorkloadAttestor):
            raise WorkerIdentityInvalid("attestor does not implement WorkloadAttestor")
        self._attestor = attestor
        self._clock = clock
        self._maximum_identity_ttl_seconds = _positive_duration(
            maximum_identity_ttl_seconds,
            "maximum_identity_ttl_seconds",
            maximum=MAX_IDENTITY_TTL_SECONDS,
        )
        self._authorization_ttl_seconds = _positive_duration(
            authorization_ttl_seconds,
            "authorization_ttl_seconds",
            maximum=MAX_AUTHORIZATION_TTL_SECONDS,
        )
        self._clock_skew_seconds = _nonnegative_duration(
            clock_skew_seconds,
            "clock_skew_seconds",
            maximum=MAX_CLOCK_SKEW_SECONDS,
        )
        rule_map: dict[str, WorkerAccessRule] = {}
        for rule in rules:
            if not isinstance(rule, WorkerAccessRule):
                raise WorkerIdentityInvalid("rules must contain WorkerAccessRule values")
            if rule.rule_id in rule_map:
                raise WorkerIdentityInvalid("worker access rule IDs must be unique")
            rule_map[rule.rule_id] = rule
        if len(rule_map) > MAX_RULES:
            raise WorkerIdentityInvalid("worker access rules exceed their bound")
        self._rules = tuple(rule_map[key] for key in sorted(rule_map))
        self._issuer_binding = object()
        self._issued_lock = threading.Lock()
        self._issued: dict[str, _IssuedAuthorization] = {}

    def authorize(
        self,
        presentation: object,
        *,
        tenant_id: str,
        pool_id: str,
        action: ActionRequest,
        expected_transport_binding_digest: str,
        expected_worker_id: str | None = None,
    ) -> WorkerAuthorization:
        if not isinstance(action, ActionRequest):
            raise WorkerAuthorizationDenied("invalid_action")
        try:
            requested_tenant = _bounded_text(tenant_id, "tenant_id")
            requested_pool = _bounded_text(pool_id, "pool_id")
            expected_binding = _sha256(
                expected_transport_binding_digest,
                "expected_transport_binding_digest",
            )
            expected_worker = (
                None
                if expected_worker_id is None
                else _bounded_text(expected_worker_id, "expected_worker_id")
            )
            now = _timestamp(self._clock(), "current time")
        except WorkerIdentityInvalid as exc:
            raise WorkerAuthorizationDenied("invalid_authorization_context") from exc
        try:
            identity = self._attestor.attest(presentation, now=now)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise WorkerAuthorizationDenied("attestation_failed") from None
        if not isinstance(identity, WorkerIdentity):
            raise WorkerAuthorizationDenied("attestation_failed")
        try:
            identity.validate_current(
                now=now,
                maximum_ttl_seconds=self._maximum_identity_ttl_seconds,
                clock_skew_seconds=self._clock_skew_seconds,
            )
        except WorkerIdentityInvalid as exc:
            raise WorkerAuthorizationDenied("identity_invalid") from exc
        if identity.transport_binding_digest != expected_binding:
            raise WorkerAuthorizationDenied("transport_binding_mismatch")
        if expected_worker is not None and identity.worker_id != expected_worker:
            raise WorkerAuthorizationDenied("worker_binding_mismatch")
        if (
            identity.tenant_id != requested_tenant
            or identity.pool_id != requested_pool
        ):
            raise WorkerAuthorizationDenied("tenant_pool_mismatch")

        identity_capabilities = {
            capability.name for capability in identity.capabilities
        }
        action_capabilities = {
            capability.name for capability in action.capabilities
        }
        if not action_capabilities.issubset(identity_capabilities):
            raise WorkerAuthorizationDenied("identity_capability_mismatch")

        matches: list[WorkerAccessRule] = []
        for rule in self._rules:
            if (
                rule.tenant_id == requested_tenant
                and rule.pool_id == requested_pool
                and action.tool_name in rule.action_names
                and action.effect_class in rule.allowed_effect_classes
                and action_capabilities.issubset(
                    {
                        capability.name
                        for capability in rule.allowed_capabilities
                    }
                )
            ):
                matches.append(rule)
        if not matches:
            raise WorkerAuthorizationDenied("no_matching_worker_rule")
        # Deterministic and least-privileged when multiple explicit rules match.
        matches.sort(
            key=lambda rule: (
                _sensitivity_rank(rule.maximum_artifact_sensitivity),
                len(rule.allowed_capabilities),
                rule.rule_id,
            )
        )
        rule = matches[0]
        expires_at = min(
            identity.expires_at,
            now + self._authorization_ttl_seconds,
        )
        if expires_at <= now:
            raise WorkerAuthorizationDenied("identity_expired")
        authorization = WorkerAuthorization(
            authorization_id=f"worker-authorization-{uuid.uuid4()}",
            worker_id=identity.worker_id,
            tenant_id=requested_tenant,
            pool_id=requested_pool,
            run_id=action.run_id,
            node_id=action.node_id,
            attempt_id=action.attempt_id,
            action_digest=action.action_digest,
            identity_binding_digest=identity.identity_binding_digest,
            transport_binding_digest=identity.transport_binding_digest,
            rule_id=rule.rule_id,
            maximum_artifact_sensitivity=rule.maximum_artifact_sensitivity,
            issued_at=now,
            expires_at=expires_at,
            _issuer_binding=self._issuer_binding,
            _identity_lineage=_identity_lineage_digest(identity),
        )
        with self._issued_lock:
            self._purge_issued_locked(now)
            if len(self._issued) >= MAX_ACTIVE_AUTHORIZATIONS:
                raise WorkerAuthorizationDenied("active_authorization_limit_reached")
            self._issued[authorization.authorization_digest] = (
                _IssuedAuthorization(
                    authorization=authorization,
                    identity_lineage_digest=_identity_lineage_digest(identity),
                )
            )
        return authorization

    def renew(
        self,
        authorization: WorkerAuthorization,
        presentation: object,
        *,
        tenant_id: str,
        pool_id: str,
        action: ActionRequest,
        expected_transport_binding_digest: str,
        expected_worker_id: str | None = None,
    ) -> WorkerAuthorization:
        """Renew freshness for one exact, already-issued authorization.

        Renewal never creates a new durable authority.  It requires a fresh
        attestation for the same workload lineage, transport binding, rule,
        Action and authorization ID, then replaces only the in-memory
        credential timestamps.  The returned ``authorization_digest`` is
        therefore identical to the original digest.
        """

        if not isinstance(authorization, WorkerAuthorization):
            raise WorkerAuthorizationDenied("invalid_authorization")
        if not isinstance(action, ActionRequest):
            raise WorkerAuthorizationDenied("invalid_action")
        try:
            requested_tenant = _bounded_text(tenant_id, "tenant_id")
            requested_pool = _bounded_text(pool_id, "pool_id")
            expected_binding = _sha256(
                expected_transport_binding_digest,
                "expected_transport_binding_digest",
            )
            expected_worker = (
                None
                if expected_worker_id is None
                else _bounded_text(expected_worker_id, "expected_worker_id")
            )
            now = _timestamp(self._clock(), "current time")
        except WorkerIdentityInvalid as exc:
            raise WorkerAuthorizationDenied(
                "invalid_authorization_context"
            ) from exc

        if authorization._issuer_binding is not self._issuer_binding:
            raise WorkerAuthorizationDenied("authorization_unissued")
        digest = authorization.authorization_digest
        with self._issued_lock:
            issued = self._issued.get(digest)
        identity_lineage = (
            authorization._identity_lineage
            if issued is None
            else issued.identity_lineage_digest
        )
        if (
            not identity_lineage
            or (
                issued is not None
                and issued.authorization != authorization
            )
        ):
            raise WorkerAuthorizationDenied("authorization_unissued")
        if (
            authorization.tenant_id != requested_tenant
            or authorization.pool_id != requested_pool
            or authorization.worker_id
            != (expected_worker or authorization.worker_id)
            or authorization.transport_binding_digest != expected_binding
            or authorization.run_id != action.run_id
            or authorization.node_id != action.node_id
            or authorization.attempt_id != action.attempt_id
            or authorization.action_digest != action.action_digest
        ):
            raise WorkerAuthorizationDenied("authorization_binding_mismatch")

        try:
            identity = self._attestor.attest(presentation, now=now)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise WorkerAuthorizationDenied("attestation_failed") from None
        if not isinstance(identity, WorkerIdentity):
            raise WorkerAuthorizationDenied("attestation_failed")
        try:
            identity.validate_current(
                now=now,
                maximum_ttl_seconds=self._maximum_identity_ttl_seconds,
                clock_skew_seconds=self._clock_skew_seconds,
            )
        except WorkerIdentityInvalid as exc:
            raise WorkerAuthorizationDenied("identity_invalid") from exc
        if (
            identity.worker_id != authorization.worker_id
            or identity.tenant_id != requested_tenant
            or identity.pool_id != requested_pool
            or identity.transport_binding_digest != expected_binding
            or _identity_lineage_digest(identity)
            != identity_lineage
        ):
            raise WorkerAuthorizationDenied("authorization_lineage_mismatch")

        rule = next(
            (
                candidate
                for candidate in self._rules
                if candidate.rule_id == authorization.rule_id
            ),
            None,
        )
        action_capabilities = {
            capability.name for capability in action.capabilities
        }
        identity_capabilities = {
            capability.name for capability in identity.capabilities
        }
        if (
            rule is None
            or rule.tenant_id != requested_tenant
            or rule.pool_id != requested_pool
            or action.tool_name not in rule.action_names
            or action.effect_class not in rule.allowed_effect_classes
            or not action_capabilities.issubset(identity_capabilities)
            or not action_capabilities.issubset(
                {
                    capability.name
                    for capability in rule.allowed_capabilities
                }
            )
            or rule.maximum_artifact_sensitivity
            != authorization.maximum_artifact_sensitivity
        ):
            raise WorkerAuthorizationDenied("authorization_rule_changed")

        expires_at = min(
            identity.expires_at,
            now + self._authorization_ttl_seconds,
        )
        if expires_at <= now:
            raise WorkerAuthorizationDenied("identity_expired")
        renewed = replace(
            authorization,
            issued_at=now,
            expires_at=expires_at,
        )
        if renewed.authorization_digest != digest:
            raise WorkerAuthorizationDenied("authorization_lineage_changed")
        with self._issued_lock:
            current = self._issued.get(digest)
            if current is not None and current != issued:
                raise WorkerAuthorizationDenied("authorization_superseded")
            self._issued[digest] = _IssuedAuthorization(
                authorization=renewed,
                identity_lineage_digest=identity_lineage,
            )
        return renewed

    def verify(
        self,
        authorization: WorkerAuthorization,
        *,
        now: float,
    ) -> bool:
        """Verify an authorization was actually issued by this trusted gate."""

        if not isinstance(authorization, WorkerAuthorization):
            return False
        try:
            current = _timestamp(now, "current time")
        except WorkerIdentityInvalid:
            return False
        if current >= authorization.expires_at:
            return False
        digest = authorization.authorization_digest
        with self._issued_lock:
            self._purge_issued_locked(current)
            issued = self._issued.get(digest)
            return (
                issued is not None
                and issued.authorization == authorization
            )

    def discard(self, authorization: WorkerAuthorization) -> None:
        """Revoke one exact unconsumed process-local authorization."""

        if not isinstance(authorization, WorkerAuthorization):
            return
        digest = authorization.authorization_digest
        with self._issued_lock:
            issued = self._issued.get(digest)
            if issued is not None and issued.authorization == authorization:
                self._issued.pop(digest, None)

    def _purge_issued_locked(self, now: float) -> None:
        stale = [
            digest
            for digest, issued in self._issued.items()
            if now >= issued.authorization.expires_at
        ]
        for digest in stale:
            self._issued.pop(digest, None)


@runtime_checkable
class WorkerAuthorizationVerifier(Protocol):
    """Trusted issuer check used by Artifact/RPC control-plane adapters."""

    def verify(
        self,
        authorization: WorkerAuthorization,
        *,
        now: float,
    ) -> bool: ...


def _sensitivity_rank(value: ArtifactSensitivity) -> int:
    return {
        ArtifactSensitivity.PUBLIC: 0,
        ArtifactSensitivity.INTERNAL: 1,
        ArtifactSensitivity.SENSITIVE: 2,
        ArtifactSensitivity.SECRET: 3,
    }[ArtifactSensitivity(value)]


def _identity_lineage_digest(identity: WorkerIdentity) -> str:
    """Bind renewable credentials to one workload and transport lineage."""

    return _canonical_digest(
        {
            "schema": "worker_identity_lineage_v1",
            "worker_id": identity.worker_id,
            "subject": identity.subject,
            "issuer": identity.issuer,
            "tenant_id": identity.tenant_id,
            "pool_id": identity.pool_id,
            "capabilities": [
                capability.name for capability in identity.capabilities
            ],
            "transport_binding_digest": identity.transport_binding_digest,
        }
    )


def _positive_duration(value: Any, field_name: str, *, maximum: float) -> float:
    number = _timestamp(value, field_name)
    if number <= 0 or number > maximum:
        raise WorkerIdentityInvalid(f"{field_name} exceeds policy")
    return number


def _nonnegative_duration(value: Any, field_name: str, *, maximum: float) -> float:
    number = _timestamp(value, field_name)
    if number > maximum:
        raise WorkerIdentityInvalid(f"{field_name} exceeds policy")
    return number


__all__ = [
    "MAX_AUTHORIZATION_TTL_SECONDS",
    "MAX_IDENTITY_TTL_SECONDS",
    "WorkerAccessRule",
    "WorkerAuthorization",
    "WorkerAuthorizationDenied",
    "WorkerAuthorizationGate",
    "WorkerAuthorizationVerifier",
    "WorkerIdentity",
    "WorkerIdentityInvalid",
    "WorkerSecurityError",
    "WorkloadAttestor",
]
