"""Bounded one-time Artifact delivery for authorized remote workers.

The broker is a reference control-plane composition.  It keeps the
``ArtifactStore`` and complete ``ArtifactRef`` server-side and gives workers an
opaque grant plus a path-free descriptor.  It is intentionally not an HTTP
server and does not expose a filesystem URI or Store root.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import secrets
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from .artifacts import (
    ArtifactEncryption,
    ArtifactIntegrityError,
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    ArtifactStore,
)
from .remote_execution_journal import (
    RemoteArtifactGrantConflict,
    RemoteArtifactGrantUnavailable,
    RemoteArtifactReadGrantRecord,
    RemoteArtifactWriteGrantRecord,
    RemoteExecutionJournal,
    RemoteExecutionJournalCapacityError,
    RemoteExecutionJournalError,
)
from .worker_security import WorkerAuthorization, WorkerAuthorizationVerifier

ARTIFACT_BROKER_SCHEMA_VERSION = 1
MAX_GRANT_TTL_SECONDS = 5 * 60.0
MAX_BROKER_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_BROKER_RESIDENT_BYTES = MAX_BROKER_ARTIFACT_BYTES
MAX_ACTIVE_GRANTS = 100_000


class ArtifactBrokerError(RuntimeError):
    """Base broker error with path-free diagnostics."""


class ArtifactGrantDenied(ArtifactBrokerError):
    """The grant operation failed closed."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = _bounded_text(reason_code, "reason_code", max_chars=128)
        super().__init__(self.reason_code)


class ArtifactGrantConsumed(ArtifactGrantDenied):
    """The grant is unknown, expired, or has already been consumed."""


def _bounded_text(value: Any, field_name: str, *, max_chars: int = 256) -> str:
    if not isinstance(value, str):
        raise ArtifactGrantDenied("invalid_broker_metadata")
    text = value.strip()
    if (
        not text
        or len(text) > max_chars
        or any(ord(character) < 32 or ord(character) == 127 for character in text)
    ):
        raise ArtifactGrantDenied("invalid_broker_metadata")
    return text


def _timestamp(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ArtifactGrantDenied("invalid_broker_time") from exc
    if not math.isfinite(result) or result < 0:
        raise ArtifactGrantDenied("invalid_broker_time")
    return result


def _sha256(value: Any) -> str:
    digest = str(value or "")
    if (
        len(digest) != 64
        or digest != digest.lower()
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise ArtifactGrantDenied("invalid_broker_digest")
    return digest


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
        raise ArtifactGrantDenied("invalid_broker_metadata") from exc
    return hashlib.sha256(encoded).hexdigest()


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ArtifactGrantDenied("invalid_broker_metadata") from exc


def _decode_canonical_object(value: str) -> dict[str, Any]:
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError, RecursionError) as exc:
        raise ArtifactGrantDenied("invalid_broker_metadata") from exc
    if (
        not isinstance(decoded, dict)
        or _canonical_json(decoded) != value
    ):
        raise ArtifactGrantDenied("invalid_broker_metadata")
    return decoded


def _artifact_binding_digest(ref: ArtifactRef) -> str:
    if not isinstance(ref, ArtifactRef):
        raise ArtifactGrantDenied("invalid_artifact_reference")
    # The URI participates in the exact server-side binding but is never
    # included in a worker-visible descriptor or grant.
    return _canonical_digest(
        {
            "schema": "artifact_grant_binding_v1",
            "artifact_ref": ref.to_dict(),
        }
    )


def _sensitivity_rank(value: ArtifactSensitivity) -> int:
    return {
        ArtifactSensitivity.PUBLIC: 0,
        ArtifactSensitivity.INTERNAL: 1,
        ArtifactSensitivity.SENSITIVE: 2,
        ArtifactSensitivity.SECRET: 3,
    }[ArtifactSensitivity(value)]


@dataclass(frozen=True, slots=True)
class ArtifactDescriptor:
    """Path-free immutable content identity returned to a worker."""

    artifact_id: str
    sha256: str
    size: int
    media_type: str
    kind: str
    sensitivity: ArtifactSensitivity
    schema_version: int = ARTIFACT_BROKER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_id",
            _bounded_text(self.artifact_id, "artifact_id", max_chars=255),
        )
        object.__setattr__(self, "sha256", _sha256(self.sha256))
        if isinstance(self.size, bool) or not isinstance(self.size, int) or self.size < 0:
            raise ArtifactGrantDenied("invalid_artifact_size")
        object.__setattr__(
            self,
            "media_type",
            _bounded_text(self.media_type, "media_type", max_chars=255),
        )
        object.__setattr__(
            self,
            "kind",
            _bounded_text(self.kind, "kind", max_chars=128),
        )
        try:
            object.__setattr__(
                self,
                "sensitivity",
                ArtifactSensitivity(self.sensitivity),
            )
        except ValueError as exc:
            raise ArtifactGrantDenied("invalid_artifact_sensitivity") from exc
        if self.schema_version != ARTIFACT_BROKER_SCHEMA_VERSION:
            raise ArtifactGrantDenied("unsupported_broker_schema")

    @classmethod
    def from_ref(cls, ref: ArtifactRef) -> "ArtifactDescriptor":
        if not isinstance(ref, ArtifactRef):
            raise ArtifactGrantDenied("invalid_artifact_reference")
        return cls(
            artifact_id=ref.artifact_id,
            sha256=ref.sha256,
            size=ref.size,
            media_type=ref.media_type,
            kind=ref.kind.value,
            sensitivity=ref.sensitivity,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "artifact_id": self.artifact_id,
            "sha256": self.sha256,
            "size": self.size,
            "media_type": self.media_type,
            "kind": self.kind,
            "sensitivity": self.sensitivity.value,
        }


@dataclass(frozen=True, slots=True)
class ArtifactReadGrant:
    """Opaque single-use credential bound to one worker authorization."""

    grant_id: str
    token: str = field(repr=False)
    tenant_id: str = ""
    worker_id: str = ""
    run_id: str = ""
    attempt_id: str = ""
    action_digest: str = ""
    authorization_digest: str = ""
    artifact_binding_digest: str = ""
    descriptor: ArtifactDescriptor | None = None
    issued_at: float = 0
    expires_at: float = 0
    schema_version: int = ARTIFACT_BROKER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in (
            "grant_id",
            "token",
            "tenant_id",
            "worker_id",
            "run_id",
            "attempt_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(getattr(self, field_name), field_name, max_chars=512),
            )
        for field_name in (
            "action_digest",
            "authorization_digest",
            "artifact_binding_digest",
        ):
            object.__setattr__(self, field_name, _sha256(getattr(self, field_name)))
        if not isinstance(self.descriptor, ArtifactDescriptor):
            raise ArtifactGrantDenied("invalid_artifact_descriptor")
        object.__setattr__(self, "issued_at", _timestamp(self.issued_at))
        object.__setattr__(self, "expires_at", _timestamp(self.expires_at))
        if self.expires_at <= self.issued_at:
            raise ArtifactGrantDenied("invalid_grant_window")
        if self.schema_version != ARTIFACT_BROKER_SCHEMA_VERSION:
            raise ArtifactGrantDenied("unsupported_broker_schema")

    def to_wire_dict(self) -> dict[str, Any]:
        """Serialize only the bearer token and path-free bounded identity."""

        return {
            "schema_version": self.schema_version,
            "grant_id": self.grant_id,
            "token": self.token,
            "tenant_id": self.tenant_id,
            "worker_id": self.worker_id,
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "action_digest": self.action_digest,
            "authorization_digest": self.authorization_digest,
            "artifact_binding_digest": self.artifact_binding_digest,
            "descriptor": self.descriptor.to_dict(),
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
        }


@dataclass(frozen=True, slots=True)
class ArtifactPayload:
    """One integrity-checked broker response."""

    descriptor: ArtifactDescriptor
    content: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.descriptor, ArtifactDescriptor):
            raise ArtifactGrantDenied("invalid_artifact_descriptor")
        if not isinstance(self.content, bytes):
            raise ArtifactGrantDenied("invalid_artifact_payload")
        if (
            len(self.content) != self.descriptor.size
            or hashlib.sha256(self.content).hexdigest() != self.descriptor.sha256
        ):
            raise ArtifactGrantDenied("artifact_integrity_failed")


@dataclass(frozen=True, slots=True)
class ArtifactWriteGrant:
    """Opaque permission to stage one exact, bounded Artifact payload."""

    grant_id: str
    token: str = field(repr=False)
    tenant_id: str = ""
    worker_id: str = ""
    run_id: str = ""
    node_id: str = ""
    attempt_id: str = ""
    action_digest: str = ""
    authorization_digest: str = ""
    kind: ArtifactKind = ArtifactKind.GENERIC
    sensitivity: ArtifactSensitivity = ArtifactSensitivity.INTERNAL
    media_type: str = "application/octet-stream"
    maximum_bytes: int = 0
    declared_sha256: str = ""
    issued_at: float = 0
    expires_at: float = 0
    schema_version: int = ARTIFACT_BROKER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in (
            "grant_id",
            "token",
            "tenant_id",
            "worker_id",
            "run_id",
            "node_id",
            "attempt_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(getattr(self, field_name), field_name, max_chars=512),
            )
        object.__setattr__(self, "action_digest", _sha256(self.action_digest))
        object.__setattr__(
            self,
            "authorization_digest",
            _sha256(self.authorization_digest),
        )
        try:
            object.__setattr__(self, "kind", ArtifactKind(self.kind))
            object.__setattr__(
                self,
                "sensitivity",
                ArtifactSensitivity(self.sensitivity),
            )
        except ValueError as exc:
            raise ArtifactGrantDenied("invalid_artifact_write_classification") from exc
        object.__setattr__(
            self,
            "media_type",
            _bounded_text(self.media_type, "media_type", max_chars=255),
        )
        if (
            isinstance(self.maximum_bytes, bool)
            or not isinstance(self.maximum_bytes, int)
            or self.maximum_bytes <= 0
            or self.maximum_bytes > MAX_BROKER_ARTIFACT_BYTES
        ):
            raise ArtifactGrantDenied("invalid_artifact_size_limit")
        object.__setattr__(
            self,
            "declared_sha256",
            _sha256(self.declared_sha256),
        )
        object.__setattr__(self, "issued_at", _timestamp(self.issued_at))
        object.__setattr__(self, "expires_at", _timestamp(self.expires_at))
        if self.expires_at <= self.issued_at:
            raise ArtifactGrantDenied("invalid_grant_window")
        if self.schema_version != ARTIFACT_BROKER_SCHEMA_VERSION:
            raise ArtifactGrantDenied("unsupported_broker_schema")

    @property
    def output_handle(self) -> "ArtifactOutputHandle":
        return ArtifactOutputHandle(grant_id=self.grant_id, token=self.token)

    def to_wire_dict(self) -> dict[str, Any]:
        """Return only bounded intent metadata; no Store URI is accepted."""

        return {
            "schema_version": self.schema_version,
            "handle": self.output_handle.to_wire_dict(),
            "tenant_id": self.tenant_id,
            "worker_id": self.worker_id,
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "action_digest": self.action_digest,
            "authorization_digest": self.authorization_digest,
            "kind": self.kind.value,
            "sensitivity": self.sensitivity.value,
            "media_type": self.media_type,
            "maximum_bytes": self.maximum_bytes,
            "declared_sha256": self.declared_sha256,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
        }


@dataclass(frozen=True, slots=True)
class ArtifactOutputHandle:
    """Untrusted wire pointer resolved against the broker's trusted registry."""

    grant_id: str
    token: str = field(repr=False)
    schema_version: int = ARTIFACT_BROKER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "grant_id",
            _bounded_text(self.grant_id, "grant_id", max_chars=512),
        )
        object.__setattr__(
            self,
            "token",
            _bounded_text(self.token, "token", max_chars=512),
        )
        if self.schema_version != ARTIFACT_BROKER_SCHEMA_VERSION:
            raise ArtifactGrantDenied("unsupported_broker_schema")

    def to_wire_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "grant_id": self.grant_id,
            "token": self.token,
        }

    @classmethod
    def from_wire_dict(cls, payload: Mapping[str, Any]) -> "ArtifactOutputHandle":
        if not isinstance(payload, Mapping) or set(payload) != {
            "schema_version",
            "grant_id",
            "token",
        }:
            raise ArtifactGrantDenied("invalid_output_handle")
        return cls(
            schema_version=payload.get("schema_version", 0),
            grant_id=payload.get("grant_id", ""),
            token=payload.get("token", ""),
        )


@dataclass(frozen=True, slots=True)
class ArtifactStagingReceipt:
    """Path-free proof that bytes passed pre-Store validation."""

    grant_id: str
    sha256: str
    size: int
    staging_digest: str
    schema_version: int = ARTIFACT_BROKER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "grant_id",
            _bounded_text(self.grant_id, "grant_id", max_chars=512),
        )
        object.__setattr__(self, "sha256", _sha256(self.sha256))
        object.__setattr__(self, "staging_digest", _sha256(self.staging_digest))
        if isinstance(self.size, bool) or not isinstance(self.size, int) or self.size < 0:
            raise ArtifactGrantDenied("invalid_artifact_size")
        if self.schema_version != ARTIFACT_BROKER_SCHEMA_VERSION:
            raise ArtifactGrantDenied("unsupported_broker_schema")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "grant_id": self.grant_id,
            "sha256": self.sha256,
            "size": self.size,
            "staging_digest": self.staging_digest,
        }


_WRITE_GRANT_METADATA_KEYS = frozenset(
    {
        "schema_version",
        "grant_id",
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
    }
)
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


def _read_grant_metadata(grant: ArtifactReadGrant) -> str:
    payload = grant.to_wire_dict()
    payload.pop("token")
    return _canonical_json(payload)


def _write_grant_metadata(grant: ArtifactWriteGrant) -> str:
    payload = grant.to_wire_dict()
    handle = payload.pop("handle")
    if not isinstance(handle, dict):
        raise ArtifactGrantDenied("invalid_artifact_write_grant")
    payload["grant_id"] = handle.get("grant_id")
    if set(payload) != _WRITE_GRANT_METADATA_KEYS:
        raise ArtifactGrantDenied("invalid_artifact_write_grant")
    return _canonical_json(payload)


def _write_grant_from_metadata(
    metadata: str,
    *,
    token: str,
) -> ArtifactWriteGrant:
    payload = _decode_canonical_object(metadata)
    if set(payload) != _WRITE_GRANT_METADATA_KEYS:
        raise ArtifactGrantDenied("invalid_artifact_write_grant")
    grant = ArtifactWriteGrant(
        grant_id=payload["grant_id"],
        token=token,
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
        schema_version=payload["schema_version"],
    )
    if _write_grant_metadata(grant) != metadata:
        raise ArtifactGrantDenied("invalid_artifact_write_grant")
    return grant


def _artifact_ref_json(ref: ArtifactRef) -> str:
    if not isinstance(ref, ArtifactRef):
        raise ArtifactGrantDenied("invalid_artifact_reference")
    return _canonical_json(ref.to_dict())


def _artifact_ref_from_json(payload: str) -> ArtifactRef:
    decoded = _decode_canonical_object(payload)
    if set(decoded) != _ARTIFACT_REF_KEYS:
        raise ArtifactGrantDenied("invalid_artifact_reference")
    try:
        ref = ArtifactRef.from_dict(decoded)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        raise ArtifactGrantDenied("invalid_artifact_reference") from None
    if _artifact_ref_json(ref) != payload:
        raise ArtifactGrantDenied("invalid_artifact_reference")
    return ref


def _staging_digest(
    grant: ArtifactWriteGrant,
    *,
    sha256: str,
    size: int,
) -> str:
    return _canonical_digest(
        {
            "schema": "artifact_write_staging_v1",
            "grant_id": grant.grant_id,
            "authorization_digest": grant.authorization_digest,
            "action_digest": grant.action_digest,
            "sha256": sha256,
            "size": size,
        }
    )


@dataclass(slots=True)
class _GrantRecord:
    grant: ArtifactReadGrant
    ref: ArtifactRef
    token_digest: str
    consumed: bool = False


@dataclass(slots=True)
class _WriteGrantRecord:
    grant: ArtifactWriteGrant
    token_digest: str
    state: str = "issued"
    content: bytes | None = field(default=None, repr=False)
    resident_bytes: int = 0
    store_write_in_flight: bool = False
    staging_receipt: ArtifactStagingReceipt | None = None
    final_ref: ArtifactRef | None = None


class ArtifactGrantBroker:
    """Bounded bearer-free grant registry around a trusted ArtifactStore."""

    def __init__(
        self,
        store: ArtifactStore,
        *,
        authorization_verifier: WorkerAuthorizationVerifier,
        clock: Callable[[], float] = time.time,
        maximum_grant_ttl_seconds: float = MAX_GRANT_TTL_SECONDS,
        maximum_artifact_bytes: int = MAX_BROKER_ARTIFACT_BYTES,
        maximum_resident_bytes: int | None = None,
        maximum_active_grants: int = MAX_ACTIVE_GRANTS,
        recovery_journal: RemoteExecutionJournal | None = None,
    ) -> None:
        if not isinstance(store, ArtifactStore):
            raise ArtifactGrantDenied("invalid_artifact_store")
        if not isinstance(authorization_verifier, WorkerAuthorizationVerifier):
            raise ArtifactGrantDenied("invalid_authorization_verifier")
        self._store = store
        self._authorization_verifier = authorization_verifier
        self._clock = clock
        ttl = _timestamp(maximum_grant_ttl_seconds)
        if ttl <= 0 or ttl > MAX_GRANT_TTL_SECONDS:
            raise ArtifactGrantDenied("invalid_grant_ttl")
        self._maximum_grant_ttl_seconds = ttl
        if (
            isinstance(maximum_artifact_bytes, bool)
            or not isinstance(maximum_artifact_bytes, int)
            or maximum_artifact_bytes <= 0
            or maximum_artifact_bytes > MAX_BROKER_ARTIFACT_BYTES
        ):
            raise ArtifactGrantDenied("invalid_artifact_size_limit")
        self._maximum_artifact_bytes = maximum_artifact_bytes
        resident_limit = (
            maximum_artifact_bytes
            if maximum_resident_bytes is None
            else maximum_resident_bytes
        )
        if (
            isinstance(resident_limit, bool)
            or not isinstance(resident_limit, int)
            or resident_limit <= 0
            or resident_limit > MAX_BROKER_RESIDENT_BYTES
        ):
            raise ArtifactGrantDenied("invalid_resident_byte_limit")
        self._maximum_resident_bytes = resident_limit
        if (
            isinstance(maximum_active_grants, bool)
            or not isinstance(maximum_active_grants, int)
            or maximum_active_grants <= 0
            or maximum_active_grants > MAX_ACTIVE_GRANTS
        ):
            raise ArtifactGrantDenied("invalid_active_grant_limit")
        self._maximum_active_grants = maximum_active_grants
        if recovery_journal is not None and not isinstance(
            recovery_journal,
            RemoteExecutionJournal,
        ):
            raise ArtifactGrantDenied("invalid_grant_registry")
        self._recovery_journal = recovery_journal or RemoteExecutionJournal(
            maximum_artifact_grants=maximum_active_grants,
        )
        self._lock = threading.Lock()
        self._grants: dict[str, _GrantRecord] = {}
        self._write_grants: dict[str, _WriteGrantRecord] = {}
        self._resident_bytes = 0

    @property
    def durable_recovery_ready(self) -> bool:
        return self._recovery_journal.durable

    def purge_expired_grants(self) -> int:
        """Remove expired durable tombstones without weakening replay safety."""

        now = _timestamp(self._clock())
        try:
            removed = (
                self._recovery_journal.purge_expired_artifact_grants(
                    now=now
                )
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except RemoteExecutionJournalError:
            raise ArtifactGrantDenied("grant_registry_unavailable") from None
        with self._lock:
            self._purge_locked(now)
        return removed

    def issue_read_grant(
        self,
        authorization: WorkerAuthorization,
        *,
        tenant_id: str,
        run_id: str,
        attempt_id: str,
        ref: ArtifactRef,
        ttl_seconds: float | None = None,
    ) -> ArtifactReadGrant:
        if not isinstance(authorization, WorkerAuthorization):
            raise ArtifactGrantDenied("invalid_worker_authorization")
        requested_tenant = _bounded_text(tenant_id, "tenant_id")
        requested_run = _bounded_text(run_id, "run_id")
        requested_attempt = _bounded_text(attempt_id, "attempt_id")
        now = _timestamp(self._clock())
        self._verify_authorization(authorization, now)
        if (
            authorization.tenant_id != requested_tenant
            or authorization.run_id != requested_run
            or authorization.attempt_id != requested_attempt
        ):
            raise ArtifactGrantDenied("authorization_binding_mismatch")
        if now >= authorization.expires_at:
            raise ArtifactGrantDenied("authorization_expired")
        if not isinstance(ref, ArtifactRef):
            raise ArtifactGrantDenied("invalid_artifact_reference")
        if ref.size > self._maximum_artifact_bytes:
            raise ArtifactGrantDenied("artifact_size_exceeds_policy")
        if _sensitivity_rank(ref.sensitivity) > _sensitivity_rank(
            authorization.maximum_artifact_sensitivity
        ):
            raise ArtifactGrantDenied("artifact_sensitivity_exceeds_policy")
        if (
            ref.sensitivity is ArtifactSensitivity.SECRET
            and ref.encryption is ArtifactEncryption.NONE
        ):
            raise ArtifactGrantDenied("unencrypted_secret_artifact")
        try:
            verified = self._store.verify(ref)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise ArtifactGrantDenied("artifact_integrity_failed") from None
        if not verified:
            raise ArtifactGrantDenied("artifact_integrity_failed")

        requested_ttl = (
            self._maximum_grant_ttl_seconds
            if ttl_seconds is None
            else _timestamp(ttl_seconds)
        )
        if requested_ttl <= 0 or requested_ttl > self._maximum_grant_ttl_seconds:
            raise ArtifactGrantDenied("grant_ttl_exceeds_policy")
        expires_at = min(
            authorization.expires_at,
            now + requested_ttl,
        )
        if expires_at <= now:
            raise ArtifactGrantDenied("authorization_expired")

        token = secrets.token_urlsafe(32)
        token_digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
        grant = ArtifactReadGrant(
            grant_id=f"artifact-grant-{uuid.uuid4()}",
            token=token,
            tenant_id=requested_tenant,
            worker_id=authorization.worker_id,
            run_id=requested_run,
            attempt_id=requested_attempt,
            action_digest=authorization.action_digest,
            authorization_digest=authorization.authorization_digest,
            artifact_binding_digest=_artifact_binding_digest(ref),
            descriptor=ArtifactDescriptor.from_ref(ref),
            issued_at=now,
            expires_at=expires_at,
        )
        record = _GrantRecord(
            grant=grant,
            ref=ref,
            token_digest=token_digest,
        )
        with self._lock:
            self._purge_locked(now)
            if (
                len(self._grants) + len(self._write_grants)
                >= self._maximum_active_grants
            ):
                raise ArtifactGrantDenied("active_grant_limit_reached")
            self._grants[grant.grant_id] = record
        try:
            persisted = self._recovery_journal.record_read_grant(
                RemoteArtifactReadGrantRecord(
                    grant_id=grant.grant_id,
                    token_digest=token_digest,
                    grant_metadata=_read_grant_metadata(grant),
                    artifact_ref=_artifact_ref_json(ref),
                    state="issued",
                    expires_at=grant.expires_at,
                    updated_at=now,
                )
            )
            if persisted.state != "issued":
                raise RemoteArtifactGrantConflict(
                    "read_grant_conflict"
                )
        except (KeyboardInterrupt, SystemExit):
            with self._lock:
                if self._grants.get(grant.grant_id) is record:
                    self._grants.pop(grant.grant_id, None)
            raise
        except RemoteExecutionJournalCapacityError:
            with self._lock:
                if self._grants.get(grant.grant_id) is record:
                    self._grants.pop(grant.grant_id, None)
            raise ArtifactGrantDenied("active_grant_limit_reached") from None
        except RemoteArtifactGrantConflict:
            with self._lock:
                if self._grants.get(grant.grant_id) is record:
                    self._grants.pop(grant.grant_id, None)
            raise ArtifactGrantDenied("grant_binding_mismatch") from None
        except RemoteExecutionJournalError:
            with self._lock:
                if self._grants.get(grant.grant_id) is record:
                    self._grants.pop(grant.grant_id, None)
            raise ArtifactGrantDenied("grant_registry_unavailable") from None
        return grant

    def redeem_read_grant(
        self,
        grant: ArtifactReadGrant,
        authorization: WorkerAuthorization,
    ) -> ArtifactPayload:
        if not isinstance(grant, ArtifactReadGrant):
            raise ArtifactGrantDenied("invalid_artifact_grant")
        if not isinstance(authorization, WorkerAuthorization):
            raise ArtifactGrantDenied("invalid_worker_authorization")
        now = _timestamp(self._clock())
        token_digest = hashlib.sha256(
            grant.token.encode("utf-8")
        ).hexdigest()
        if now >= grant.expires_at:
            self._consume_durable_read_grant(
                grant,
                token_digest=token_digest,
                now=now,
            )
            raise ArtifactGrantConsumed("grant_unavailable")
        self._verify_authorization(authorization, now)
        if (
            now >= authorization.expires_at
            or authorization.worker_id != grant.worker_id
            or authorization.tenant_id != grant.tenant_id
            or authorization.run_id != grant.run_id
            or authorization.attempt_id != grant.attempt_id
            or authorization.action_digest != grant.action_digest
            or authorization.authorization_digest
            != grant.authorization_digest
        ):
            raise ArtifactGrantDenied("authorization_binding_mismatch")
        durable = self._consume_durable_read_grant(
            grant,
            token_digest=token_digest,
            now=now,
        )
        ref = _artifact_ref_from_json(durable.artifact_ref)
        descriptor = grant.descriptor
        assert descriptor is not None
        if (
            _artifact_binding_digest(ref) != grant.artifact_binding_digest
            or ArtifactDescriptor.from_ref(ref) != descriptor
        ):
            raise ArtifactGrantDenied("grant_binding_mismatch")
        # Consume before bytes leave the trusted boundary.  Any subsequent
        # failure requires a newly authorized grant instead of replay.
        with self._lock:
            record = self._grants.get(grant.grant_id)
            if record is not None:
                record.consumed = True

        try:
            if not self._store.verify(ref):
                raise ArtifactGrantDenied("artifact_integrity_failed")
            content = self._store.read(ref)
        except (KeyboardInterrupt, SystemExit):
            raise
        except ArtifactIntegrityError:
            raise ArtifactGrantDenied("artifact_integrity_failed") from None
        except ArtifactGrantDenied:
            raise
        except BaseException:
            # Never chain a Store exception that could contain an absolute path.
            raise ArtifactGrantDenied("artifact_read_failed") from None
        if (
            len(content) != descriptor.size
            or hashlib.sha256(content).hexdigest() != descriptor.sha256
        ):
            raise ArtifactGrantDenied("artifact_integrity_failed")
        return ArtifactPayload(descriptor=descriptor, content=content)

    def issue_write_grant(
        self,
        authorization: WorkerAuthorization,
        *,
        tenant_id: str,
        run_id: str,
        node_id: str,
        attempt_id: str,
        kind: ArtifactKind | str,
        sensitivity: ArtifactSensitivity | str,
        media_type: str,
        maximum_bytes: int,
        declared_sha256: str,
        ttl_seconds: float | None = None,
    ) -> ArtifactWriteGrant:
        """Authorize one output without accepting a worker-created ArtifactRef."""

        if not isinstance(authorization, WorkerAuthorization):
            raise ArtifactGrantDenied("invalid_worker_authorization")
        requested_tenant = _bounded_text(tenant_id, "tenant_id")
        requested_run = _bounded_text(run_id, "run_id")
        requested_node = _bounded_text(node_id, "node_id")
        requested_attempt = _bounded_text(attempt_id, "attempt_id")
        now = _timestamp(self._clock())
        self._verify_authorization(authorization, now)
        if (
            authorization.tenant_id != requested_tenant
            or authorization.run_id != requested_run
            or authorization.node_id != requested_node
            or authorization.attempt_id != requested_attempt
        ):
            raise ArtifactGrantDenied("authorization_binding_mismatch")
        try:
            requested_kind = ArtifactKind(kind)
            requested_sensitivity = ArtifactSensitivity(sensitivity)
        except ValueError as exc:
            raise ArtifactGrantDenied(
                "invalid_artifact_write_classification"
            ) from exc
        if _sensitivity_rank(requested_sensitivity) > _sensitivity_rank(
            authorization.maximum_artifact_sensitivity
        ):
            raise ArtifactGrantDenied("artifact_sensitivity_exceeds_policy")
        # The generic ArtifactStore protocol cannot require encryption on write.
        # A deployment-specific encrypted broker must provide a separate adapter.
        if requested_sensitivity is ArtifactSensitivity.SECRET:
            raise ArtifactGrantDenied("secret_write_requires_encrypted_broker")
        requested_media_type = _bounded_text(
            media_type,
            "media_type",
            max_chars=255,
        )
        if (
            isinstance(maximum_bytes, bool)
            or not isinstance(maximum_bytes, int)
            or maximum_bytes <= 0
            or maximum_bytes > self._maximum_artifact_bytes
        ):
            raise ArtifactGrantDenied("artifact_size_exceeds_policy")
        requested_digest = _sha256(declared_sha256)
        requested_ttl = (
            self._maximum_grant_ttl_seconds
            if ttl_seconds is None
            else _timestamp(ttl_seconds)
        )
        if requested_ttl <= 0 or requested_ttl > self._maximum_grant_ttl_seconds:
            raise ArtifactGrantDenied("grant_ttl_exceeds_policy")
        expires_at = min(authorization.expires_at, now + requested_ttl)
        if expires_at <= now:
            raise ArtifactGrantDenied("authorization_expired")

        token = secrets.token_urlsafe(32)
        token_digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
        grant = ArtifactWriteGrant(
            grant_id=f"artifact-write-grant-{uuid.uuid4()}",
            token=token,
            tenant_id=requested_tenant,
            worker_id=authorization.worker_id,
            run_id=requested_run,
            node_id=requested_node,
            attempt_id=requested_attempt,
            action_digest=authorization.action_digest,
            authorization_digest=authorization.authorization_digest,
            kind=requested_kind,
            sensitivity=requested_sensitivity,
            media_type=requested_media_type,
            maximum_bytes=maximum_bytes,
            declared_sha256=requested_digest,
            issued_at=now,
            expires_at=expires_at,
        )
        record = _WriteGrantRecord(
            grant=grant,
            token_digest=token_digest,
        )
        with self._lock:
            self._purge_locked(now)
            if (
                len(self._grants) + len(self._write_grants)
                >= self._maximum_active_grants
            ):
                raise ArtifactGrantDenied("active_grant_limit_reached")
            self._write_grants[grant.grant_id] = record
        try:
            persisted = self._recovery_journal.record_write_grant(
                RemoteArtifactWriteGrantRecord(
                    grant_id=grant.grant_id,
                    token_digest=token_digest,
                    grant_metadata=_write_grant_metadata(grant),
                    state="issued",
                    staging_digest=None,
                    final_ref=None,
                    expires_at=grant.expires_at,
                    updated_at=now,
                )
            )
            if persisted.state != "issued":
                raise RemoteArtifactGrantConflict(
                    "write_grant_conflict"
                )
        except (KeyboardInterrupt, SystemExit):
            with self._lock:
                if self._write_grants.get(grant.grant_id) is record:
                    self._write_grants.pop(grant.grant_id, None)
            raise
        except RemoteExecutionJournalCapacityError:
            with self._lock:
                if self._write_grants.get(grant.grant_id) is record:
                    self._write_grants.pop(grant.grant_id, None)
            raise ArtifactGrantDenied("active_grant_limit_reached") from None
        except RemoteArtifactGrantConflict:
            with self._lock:
                if self._write_grants.get(grant.grant_id) is record:
                    self._write_grants.pop(grant.grant_id, None)
            raise ArtifactGrantDenied("grant_binding_mismatch") from None
        except RemoteExecutionJournalError:
            with self._lock:
                if self._write_grants.get(grant.grant_id) is record:
                    self._write_grants.pop(grant.grant_id, None)
            raise ArtifactGrantDenied("grant_registry_unavailable") from None
        return grant

    def stage_write(
        self,
        grant: ArtifactWriteGrant,
        authorization: WorkerAuthorization,
        *,
        content: bytes,
        declared_sha256: str,
    ) -> ArtifactStagingReceipt:
        """Validate and hold bytes in memory without touching ArtifactStore."""

        if not isinstance(grant, ArtifactWriteGrant):
            raise ArtifactGrantDenied("invalid_artifact_write_grant")
        if not isinstance(authorization, WorkerAuthorization):
            raise ArtifactGrantDenied("invalid_worker_authorization")
        if not isinstance(content, bytes):
            raise ArtifactGrantDenied("write_payload_must_be_bytes")
        token_digest = hashlib.sha256(grant.token.encode("utf-8")).hexdigest()
        now = _timestamp(self._clock())
        self._observe_write_grant_expiry(
            grant.grant_id,
            token_digest=token_digest,
            now=now,
        )
        self._recover_write_record(
            grant.grant_id,
            token=grant.token,
            now=now,
        )
        self._verify_authorization(authorization, now)
        # Reject a stolen or cross-Action grant before hashing attacker-sized
        # content.  State is checked again after hashing to close stage races.
        with self._lock:
            self._bound_write_record_locked(
                grant,
                authorization,
                now=now,
                required_state="issued",
                token_digest=token_digest,
            )
        declared_digest = _sha256(declared_sha256)
        if declared_digest != grant.declared_sha256:
            raise ArtifactGrantDenied("declared_digest_mismatch")
        if len(content) > grant.maximum_bytes:
            raise ArtifactGrantDenied("artifact_size_exceeds_grant")
        content_digest = hashlib.sha256(content).hexdigest()
        if content_digest != declared_digest:
            raise ArtifactGrantDenied("artifact_digest_mismatch")
        staging_digest = _staging_digest(
            grant,
            sha256=content_digest,
            size=len(content),
        )
        receipt = ArtifactStagingReceipt(
            grant_id=grant.grant_id,
            sha256=content_digest,
            size=len(content),
            staging_digest=staging_digest,
        )
        now = _timestamp(self._clock())
        with self._lock:
            record = self._bound_write_record_locked(
                grant,
                authorization,
                now=now,
                required_state="issued",
                token_digest=token_digest,
            )
            self._retain_content_locked(record, content)
            record.staging_receipt = receipt
            record.state = "staged"
        return receipt

    def resolve_output_handle(
        self,
        handle: ArtifactOutputHandle,
    ) -> ArtifactWriteGrant:
        """Resolve untrusted wire identity to the exact server-issued grant."""

        if not isinstance(handle, ArtifactOutputHandle):
            raise ArtifactGrantDenied("invalid_output_handle")
        token_digest = hashlib.sha256(handle.token.encode("utf-8")).hexdigest()
        now = _timestamp(self._clock())
        self._observe_write_grant_expiry(
            handle.grant_id,
            token_digest=token_digest,
            now=now,
        )
        self._recover_write_record(
            handle.grant_id,
            token=handle.token,
            now=now,
        )
        with self._lock:
            record = self._live_write_record_locked(handle.grant_id, now=now)
            if record.state not in {"issued", "staged"}:
                raise ArtifactGrantConsumed("write_grant_unavailable")
            if not hmac.compare_digest(
                record.token_digest,
                token_digest,
            ):
                raise ArtifactGrantDenied("grant_binding_mismatch")
            return record.grant

    def finalize_write(
        self,
        grant: ArtifactWriteGrant,
        authorization: WorkerAuthorization,
        staging_receipt: ArtifactStagingReceipt,
    ) -> ArtifactRef:
        """Install staged bytes once and return only a Store-generated ref."""

        if not isinstance(grant, ArtifactWriteGrant):
            raise ArtifactGrantDenied("invalid_artifact_write_grant")
        if not isinstance(authorization, WorkerAuthorization):
            raise ArtifactGrantDenied("invalid_worker_authorization")
        if not isinstance(staging_receipt, ArtifactStagingReceipt):
            raise ArtifactGrantDenied("invalid_staging_receipt")
        token_digest = hashlib.sha256(grant.token.encode("utf-8")).hexdigest()
        now = _timestamp(self._clock())
        self._observe_write_grant_expiry(
            grant.grant_id,
            token_digest=token_digest,
            now=now,
        )
        self._recover_write_record(
            grant.grant_id,
            token=grant.token,
            now=now,
        )
        self._verify_authorization(authorization, now)
        return self._finalize_trusted_write(
            grant,
            authorization,
            staging_receipt,
            now=now,
            token_digest=token_digest,
        )

    def finalize_output_handle(
        self,
        handle: ArtifactOutputHandle,
        authorization: WorkerAuthorization,
    ) -> ArtifactRef:
        """Finalize server-side staging and replay the same verified final ref."""

        if not isinstance(handle, ArtifactOutputHandle):
            raise ArtifactGrantDenied("invalid_output_handle")
        if not isinstance(authorization, WorkerAuthorization):
            raise ArtifactGrantDenied("invalid_worker_authorization")
        token_digest = hashlib.sha256(handle.token.encode("utf-8")).hexdigest()
        now = _timestamp(self._clock())
        self._observe_write_grant_expiry(
            handle.grant_id,
            token_digest=token_digest,
            now=now,
        )
        record = self._recover_write_record(
            handle.grant_id,
            token=handle.token,
            now=now,
        )
        self._verify_authorization(authorization, now)
        with self._lock:
            record = self._write_record_for_handle_locked(
                handle,
                authorization,
                now=now,
                token_digest=token_digest,
            )
            grant = record.grant
            if record.state == "finalized":
                if record.final_ref is None:
                    raise ArtifactGrantDenied("finalized_ref_missing")
                final_ref = record.final_ref
            else:
                final_ref = None
            staging_receipt = record.staging_receipt
            if final_ref is None and staging_receipt is None:
                raise ArtifactGrantDenied("write_grant_not_staged")
        if final_ref is not None:
            return self._verified_final_ref(grant, final_ref)
        assert staging_receipt is not None
        return self._finalize_trusted_write(
            grant,
            authorization,
            staging_receipt,
            now=now,
            token_digest=token_digest,
        )

    def _finalize_trusted_write(
        self,
        grant: ArtifactWriteGrant,
        authorization: WorkerAuthorization,
        staging_receipt: ArtifactStagingReceipt,
        *,
        now: float,
        token_digest: str,
    ) -> ArtifactRef:
        staging_integrity_failed = False
        with self._lock:
            record = self._write_record_locked(
                grant,
                authorization,
                now=now,
                token_digest=token_digest,
            )
            if record.staging_receipt != staging_receipt:
                raise ArtifactGrantDenied("staging_binding_mismatch")
            if record.state == "finalized":
                if record.final_ref is None:
                    raise ArtifactGrantDenied("finalized_ref_missing")
                final_ref = record.final_ref
                content = None
            elif record.state == "staged":
                if record.content is None:
                    raise ArtifactGrantDenied("staging_binding_mismatch")
                content = record.content
                if (
                    len(content) != staging_receipt.size
                    or len(content) > grant.maximum_bytes
                    or staging_receipt.sha256 != grant.declared_sha256
                ):
                    record.state = "failed"
                    self._release_content_locked(record)
                    staging_integrity_failed = True
                    final_ref = None
                    content = None
                else:
                    # Claim finalization before crossing the trusted Store
                    # boundary.
                    record.state = "finalizing"
                    record.store_write_in_flight = True
                    final_ref = None
            else:
                raise ArtifactGrantConsumed("write_grant_unavailable")

        if staging_integrity_failed:
            self._persist_write_failure(
                grant.grant_id,
                token_digest=token_digest,
                now=_timestamp(self._clock()),
            )
            raise ArtifactGrantDenied("staging_integrity_failed")
        if final_ref is not None:
            return self._verified_final_ref(grant, final_ref)
        assert content is not None
        try:
            if hashlib.sha256(content).hexdigest() != staging_receipt.sha256:
                raise ArtifactGrantDenied("staging_integrity_failed")
            ref = self._store.put_bytes(
                content,
                media_type=grant.media_type,
                kind=grant.kind,
                sensitivity=grant.sensitivity,
                producer_run_id=grant.run_id,
                producer_node_id=grant.node_id,
                producer_attempt_id=grant.attempt_id,
                metadata={},
            )
            ref = self._verified_final_ref(
                grant,
                ref,
                expected_size=len(content),
            )
        except (KeyboardInterrupt, SystemExit):
            with self._lock:
                self._fail_finalization_locked(record)
            raise
        except ArtifactGrantDenied:
            with self._lock:
                self._fail_finalization_locked(record)
            self._persist_write_failure(
                grant.grant_id,
                token_digest=token_digest,
                now=_timestamp(self._clock()),
            )
            raise
        except BaseException:
            with self._lock:
                self._fail_finalization_locked(record)
            self._persist_write_failure(
                grant.grant_id,
                token_digest=token_digest,
                now=_timestamp(self._clock()),
            )
            raise ArtifactGrantDenied("artifact_write_failed") from None
        try:
            finalized_at = _timestamp(self._clock())
            durable = self._recovery_journal.finalize_write_grant(
                grant_id=grant.grant_id,
                token_digest=token_digest,
                grant_metadata=_write_grant_metadata(grant),
                staging_digest=staging_receipt.staging_digest,
                final_ref=_artifact_ref_json(ref),
                now=finalized_at,
            )
            if durable.final_ref is None:
                raise RemoteArtifactGrantConflict(
                    "write_finalization_conflict"
                )
            durable_ref = _artifact_ref_from_json(durable.final_ref)
            if durable_ref != ref:
                raise RemoteArtifactGrantConflict(
                    "write_finalization_conflict"
                )
        except (KeyboardInterrupt, SystemExit):
            with self._lock:
                self._fail_finalization_locked(record)
            raise
        except RemoteArtifactGrantUnavailable:
            with self._lock:
                self._fail_finalization_locked(record)
            raise ArtifactGrantConsumed(
                "write_grant_unavailable"
            ) from None
        except RemoteArtifactGrantConflict:
            with self._lock:
                self._fail_finalization_locked(record)
            raise ArtifactGrantDenied("grant_binding_mismatch") from None
        except ArtifactGrantDenied:
            with self._lock:
                self._fail_finalization_locked(record)
            raise
        except RemoteExecutionJournalError:
            with self._lock:
                self._retry_finalization_locked(record)
            raise ArtifactGrantDenied("grant_registry_unavailable") from None
        with self._lock:
            if record.state != "finalizing":
                record.store_write_in_flight = False
                self._release_content_locked(record)
                if self._write_grants.get(grant.grant_id) is record:
                    self._write_grants.pop(grant.grant_id, None)
                raise ArtifactGrantConsumed("write_grant_unavailable")
            record.final_ref = ref
            record.state = "finalized"
            record.store_write_in_flight = False
            self._release_content_locked(record)
        return ref

    def _verified_final_ref(
        self,
        grant: ArtifactWriteGrant,
        ref: ArtifactRef,
        *,
        expected_size: int | None = None,
    ) -> ArtifactRef:
        if not isinstance(ref, ArtifactRef):
            raise ArtifactGrantDenied("artifact_store_returned_invalid_ref")
        if (
            ref.sha256 != grant.declared_sha256
            or ref.size > grant.maximum_bytes
            or (expected_size is not None and ref.size != expected_size)
            or ref.media_type != grant.media_type
            or ref.kind is not grant.kind
            or ref.sensitivity is not grant.sensitivity
            or ref.producer_run_id != grant.run_id
            or ref.producer_node_id != grant.node_id
            or ref.producer_attempt_id != grant.attempt_id
            or bool(ref.metadata)
        ):
            raise ArtifactGrantDenied("artifact_store_ref_binding_mismatch")
        if (
            grant.sensitivity is ArtifactSensitivity.SECRET
            and ref.encryption is ArtifactEncryption.NONE
        ):
            raise ArtifactGrantDenied("unencrypted_secret_artifact")
        try:
            verified = self._store.verify(ref)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise ArtifactGrantDenied("artifact_integrity_failed") from None
        if not verified:
            raise ArtifactGrantDenied("artifact_integrity_failed")
        return ref

    def _bound_write_record_locked(
        self,
        grant: ArtifactWriteGrant,
        authorization: WorkerAuthorization,
        *,
        now: float,
        required_state: str,
        token_digest: str,
    ) -> _WriteGrantRecord:
        record = self._write_record_locked(
            grant,
            authorization,
            now=now,
            token_digest=token_digest,
        )
        if record.state != required_state:
            raise ArtifactGrantConsumed("write_grant_unavailable")
        return record

    def _write_record_locked(
        self,
        grant: ArtifactWriteGrant,
        authorization: WorkerAuthorization,
        *,
        now: float,
        token_digest: str,
    ) -> _WriteGrantRecord:
        record = self._live_write_record_locked(grant.grant_id, now=now)
        if now >= authorization.expires_at:
            raise ArtifactGrantConsumed("write_grant_unavailable")
        if not hmac.compare_digest(
            record.token_digest,
            token_digest,
        ):
            raise ArtifactGrantDenied("grant_binding_mismatch")
        if record.grant != grant:
            raise ArtifactGrantDenied("grant_binding_mismatch")
        if (
            authorization.worker_id != grant.worker_id
            or authorization.tenant_id != grant.tenant_id
            or authorization.run_id != grant.run_id
            or authorization.node_id != grant.node_id
            or authorization.attempt_id != grant.attempt_id
            or authorization.action_digest != grant.action_digest
            or authorization.authorization_digest != grant.authorization_digest
        ):
            raise ArtifactGrantDenied("authorization_binding_mismatch")
        return record

    def _write_record_for_handle_locked(
        self,
        handle: ArtifactOutputHandle,
        authorization: WorkerAuthorization,
        *,
        now: float,
        token_digest: str,
    ) -> _WriteGrantRecord:
        record = self._live_write_record_locked(handle.grant_id, now=now)
        if now >= authorization.expires_at:
            raise ArtifactGrantConsumed("write_grant_unavailable")
        if not hmac.compare_digest(
            record.token_digest,
            token_digest,
        ):
            raise ArtifactGrantDenied("grant_binding_mismatch")
        grant = record.grant
        if (
            authorization.worker_id != grant.worker_id
            or authorization.tenant_id != grant.tenant_id
            or authorization.run_id != grant.run_id
            or authorization.node_id != grant.node_id
            or authorization.attempt_id != grant.attempt_id
            or authorization.action_digest != grant.action_digest
            or authorization.authorization_digest != grant.authorization_digest
        ):
            raise ArtifactGrantDenied("authorization_binding_mismatch")
        return record

    def _recover_write_record(
        self,
        grant_id: str,
        *,
        token: str,
        now: float,
    ) -> _WriteGrantRecord:
        token_digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
        with self._lock:
            existing = self._write_grants.get(grant_id)
            if existing is not None:
                if not hmac.compare_digest(
                    existing.token_digest,
                    token_digest,
                ):
                    raise ArtifactGrantDenied("grant_binding_mismatch")
                return existing
        try:
            durable = self._recovery_journal.get_write_grant(
                grant_id=grant_id,
                token_digest=token_digest,
                now=now,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except RemoteArtifactGrantUnavailable:
            raise ArtifactGrantConsumed(
                "write_grant_unavailable"
            ) from None
        except RemoteArtifactGrantConflict:
            raise ArtifactGrantDenied("grant_binding_mismatch") from None
        except RemoteExecutionJournalError:
            raise ArtifactGrantDenied("grant_registry_unavailable") from None
        grant = _write_grant_from_metadata(
            durable.grant_metadata,
            token=token,
        )
        if (
            grant.grant_id != grant_id
            or grant.expires_at != durable.expires_at
        ):
            raise ArtifactGrantDenied("grant_binding_mismatch")
        if now >= grant.expires_at:
            raise ArtifactGrantConsumed("write_grant_unavailable")
        final_ref: ArtifactRef | None = None
        staging_receipt: ArtifactStagingReceipt | None = None
        if durable.state == "finalized":
            if (
                durable.final_ref is None
                or durable.staging_digest is None
            ):
                raise ArtifactGrantDenied("finalized_ref_missing")
            final_ref = _artifact_ref_from_json(durable.final_ref)
            if durable.staging_digest != _staging_digest(
                grant,
                sha256=final_ref.sha256,
                size=final_ref.size,
            ):
                raise ArtifactGrantDenied("staging_binding_mismatch")
            staging_receipt = ArtifactStagingReceipt(
                grant_id=grant.grant_id,
                sha256=final_ref.sha256,
                size=final_ref.size,
                staging_digest=durable.staging_digest,
            )
        recovered = _WriteGrantRecord(
            grant=grant,
            token_digest=token_digest,
            state=durable.state,
            staging_receipt=staging_receipt,
            final_ref=final_ref,
        )
        with self._lock:
            existing = self._write_grants.get(grant_id)
            if existing is not None:
                if (
                    existing.grant != grant
                    or not hmac.compare_digest(
                        existing.token_digest,
                        token_digest,
                    )
                ):
                    raise ArtifactGrantDenied("grant_binding_mismatch")
                return existing
            self._purge_locked(now)
            if (
                len(self._grants) + len(self._write_grants)
                >= self._maximum_active_grants
            ):
                raise ArtifactGrantDenied("active_grant_limit_reached")
            self._write_grants[grant_id] = recovered
            return recovered

    def _consume_durable_read_grant(
        self,
        grant: ArtifactReadGrant,
        *,
        token_digest: str,
        now: float,
    ) -> RemoteArtifactReadGrantRecord:
        try:
            return self._recovery_journal.consume_read_grant(
                grant_id=grant.grant_id,
                token_digest=token_digest,
                grant_metadata=_read_grant_metadata(grant),
                expires_at=grant.expires_at,
                now=now,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except RemoteArtifactGrantUnavailable:
            raise ArtifactGrantConsumed("grant_unavailable") from None
        except RemoteArtifactGrantConflict:
            raise ArtifactGrantDenied("grant_binding_mismatch") from None
        except RemoteExecutionJournalError:
            raise ArtifactGrantDenied("grant_registry_unavailable") from None

    def _persist_write_failure(
        self,
        grant_id: str,
        *,
        token_digest: str,
        now: float,
    ) -> None:
        try:
            self._recovery_journal.fail_write_grant(
                grant_id=grant_id,
                token_digest=token_digest,
                now=now,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except RemoteArtifactGrantConflict:
            raise ArtifactGrantDenied("grant_binding_mismatch") from None
        except RemoteArtifactGrantUnavailable:
            raise ArtifactGrantConsumed(
                "write_grant_unavailable"
            ) from None
        except RemoteExecutionJournalError:
            raise ArtifactGrantDenied("grant_registry_unavailable") from None

    def _verify_authorization(
        self,
        authorization: WorkerAuthorization,
        now: float,
    ) -> None:
        try:
            verified = self._authorization_verifier.verify(
                authorization,
                now=now,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise ArtifactGrantDenied("worker_authorization_unverified") from None
        if verified is not True:
            raise ArtifactGrantDenied("worker_authorization_unverified")

    def _live_write_record_locked(
        self,
        grant_id: str,
        *,
        now: float,
    ) -> _WriteGrantRecord:
        record = self._write_grants.get(grant_id)
        if record is None:
            raise ArtifactGrantConsumed("write_grant_unavailable")
        if now >= record.grant.expires_at:
            self._expire_write_record_locked(record)
        if record.state in {"expired", "failed", "consumed"}:
            if not record.store_write_in_flight:
                self._release_content_locked(record)
            raise ArtifactGrantConsumed("write_grant_unavailable")
        return record

    def _observe_write_grant_expiry(
        self,
        grant_id: str,
        *,
        token_digest: str,
        now: float,
    ) -> None:
        expired = False
        with self._lock:
            record = self._write_grants.get(grant_id)
            if record is not None and now >= record.grant.expires_at:
                self._expire_write_record_locked(record)
                expired = True
        if not expired:
            return
        try:
            self._recovery_journal.get_write_grant(
                grant_id=grant_id,
                token_digest=token_digest,
                now=now,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except RemoteArtifactGrantUnavailable:
            raise ArtifactGrantConsumed(
                "write_grant_unavailable"
            ) from None
        except RemoteArtifactGrantConflict:
            raise ArtifactGrantDenied("grant_binding_mismatch") from None
        except RemoteExecutionJournalError:
            raise ArtifactGrantDenied("grant_registry_unavailable") from None
        raise ArtifactGrantConsumed("write_grant_unavailable")

    def _retain_content_locked(
        self,
        record: _WriteGrantRecord,
        content: bytes,
    ) -> None:
        if record.content is not None or record.resident_bytes:
            raise ArtifactGrantDenied("write_grant_unavailable")
        content_size = len(content)
        if self._resident_bytes + content_size > self._maximum_resident_bytes:
            raise ArtifactGrantDenied("resident_byte_limit_reached")
        record.content = content
        record.resident_bytes = content_size
        self._resident_bytes += content_size

    def _release_content_locked(self, record: _WriteGrantRecord) -> None:
        resident_bytes = record.resident_bytes
        record.content = None
        record.resident_bytes = 0
        if resident_bytes:
            self._resident_bytes -= resident_bytes
            if self._resident_bytes < 0:
                raise RuntimeError("artifact broker resident accounting underflow")

    def _expire_write_record_locked(self, record: _WriteGrantRecord) -> None:
        record.state = "expired"
        if not record.store_write_in_flight:
            self._release_content_locked(record)

    def _fail_finalization_locked(self, record: _WriteGrantRecord) -> None:
        record.store_write_in_flight = False
        if record.state != "expired":
            record.state = "failed"
        self._release_content_locked(record)

    def _retry_finalization_locked(self, record: _WriteGrantRecord) -> None:
        record.store_write_in_flight = False
        if record.state == "expired":
            self._release_content_locked(record)
        else:
            record.state = "staged"

    def _purge_locked(self, now: float) -> None:
        stale = [
            grant_id
            for grant_id, record in self._grants.items()
            if record.consumed or now >= record.grant.expires_at
        ]
        for grant_id in stale:
            self._grants.pop(grant_id, None)
        stale_writes = []
        for grant_id, record in self._write_grants.items():
            if now >= record.grant.expires_at:
                self._expire_write_record_locked(record)
            if (
                record.state in {"failed", "expired", "consumed"}
                and not record.store_write_in_flight
            ):
                stale_writes.append(grant_id)
        for grant_id in stale_writes:
            record = self._write_grants.pop(grant_id)
            self._release_content_locked(record)


__all__ = [
    "ArtifactBrokerError",
    "ArtifactDescriptor",
    "ArtifactGrantBroker",
    "ArtifactGrantConsumed",
    "ArtifactGrantDenied",
    "ArtifactPayload",
    "ArtifactReadGrant",
    "ArtifactOutputHandle",
    "ArtifactStagingReceipt",
    "ArtifactWriteGrant",
    "MAX_BROKER_RESIDENT_BYTES",
]
