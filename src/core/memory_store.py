"""Validated two-stage long-term memory store.

Untrusted inputs can only create pending MemoryCandidate values.  A separate,
scope-checked review converts a candidate into an active MemoryRecord.  Both
collections share one workspace-locked JSON document so review state and the
active record commit atomically.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.core.agent_kernel import (
    DataSensitivity,
    KernelValidationError,
    KnowledgeItem,
    KnowledgeKind,
    Principal,
    TrustLevel,
    canonical_digest,
    namespace_tenant_id,
)
from src.core.workspace_storage import atomic_write_json, workspace_write_lock


MEMORY_STORE_SCHEMA_VERSION = 1
MEMORY_STORE_PATH = Path("runtime") / "agent_kernel" / "memory-store.json"
MAX_MEMORY_CONTENT_CHARS = 8_000
MAX_MEMORY_ITEMS = 2_000
MEMORY_PROPOSE_SCOPE = "memory.propose"
MEMORY_READ_SCOPE = "memory.read"
MEMORY_REVIEW_SCOPE = "memory.review"


class MemoryKind(StrEnum):
    SEMANTIC = "semantic"
    EPISODIC = "episodic"
    PROCEDURAL = "procedural"


class MemoryReviewStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"


class MemoryStoreError(RuntimeError):
    """Memory store state is unavailable or violates its trust contract."""


def _text(value: Any, field_name: str, *, max_chars: int = 256) -> str:
    if not isinstance(value, str):
        raise KernelValidationError(f"{field_name} must be a string")
    text = value.strip()
    if not text:
        raise KernelValidationError(f"{field_name} must not be empty")
    if len(text) > max_chars:
        raise KernelValidationError(f"{field_name} exceeds its bound")
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise KernelValidationError(f"{field_name} contains control characters")
    return text


def _strings(
    values: Iterable[str],
    field_name: str,
    *,
    max_items: int = 64,
    preserve_order: bool = False,
) -> tuple[str, ...]:
    try:
        normalized = tuple(_text(value, field_name) for value in values)
    except TypeError as exc:
        raise KernelValidationError(f"{field_name} must be iterable") from exc
    if len(normalized) > max_items:
        raise KernelValidationError(f"{field_name} exceeds its bound")
    if len(set(normalized)) != len(normalized):
        raise KernelValidationError(f"{field_name} values must be unique")
    return normalized if preserve_order else tuple(sorted(normalized))


def _timestamp(value: Any, field_name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise KernelValidationError(f"{field_name} must be a finite timestamp") from exc
    if not math.isfinite(result) or result < 0:
        raise KernelValidationError(f"{field_name} must be a finite timestamp")
    return result


@dataclass(frozen=True, slots=True)
class MemoryCandidate:
    candidate_id: str
    namespace: tuple[str, ...]
    kind: MemoryKind
    content: str
    source_refs: tuple[str, ...]
    trust: TrustLevel
    confidence: float
    sensitivity: DataSensitivity
    acl: tuple[str, ...]
    proposed_by: str
    created_at: float
    expires_at: float | None = None
    version: int = 1
    review_status: MemoryReviewStatus = MemoryReviewStatus.PENDING
    review_reason: str = ""
    reviewed_by: str = ""
    reviewed_at: float | None = None
    schema_version: int = MEMORY_STORE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "candidate_id",
            _text(self.candidate_id, "candidate_id"),
        )
        object.__setattr__(
            self,
            "namespace",
            _strings(
                self.namespace,
                "namespace",
                max_items=16,
                preserve_order=True,
            ),
        )
        if not self.namespace:
            raise KernelValidationError("namespace must not be empty")
        try:
            object.__setattr__(self, "kind", MemoryKind(self.kind))
            object.__setattr__(self, "trust", TrustLevel(self.trust))
            object.__setattr__(
                self,
                "sensitivity",
                DataSensitivity(self.sensitivity),
            )
            object.__setattr__(
                self,
                "review_status",
                MemoryReviewStatus(self.review_status),
            )
        except ValueError as exc:
            raise KernelValidationError("invalid memory enum value") from exc
        content = str(self.content or "").strip()
        if not content:
            raise KernelValidationError("memory content must not be empty")
        if len(content) > MAX_MEMORY_CONTENT_CHARS:
            raise KernelValidationError("memory content exceeds its bound")
        object.__setattr__(self, "content", content)
        object.__setattr__(
            self,
            "source_refs",
            _strings(self.source_refs, "source_ref"),
        )
        if not self.source_refs:
            raise KernelValidationError("source_refs must not be empty")
        try:
            confidence = float(self.confidence)
        except (TypeError, ValueError) as exc:
            raise KernelValidationError("confidence must be numeric") from exc
        if not math.isfinite(confidence) or not 0 <= confidence <= 1:
            raise KernelValidationError("confidence must be between 0 and 1")
        object.__setattr__(self, "confidence", confidence)
        object.__setattr__(self, "acl", _strings(self.acl, "acl"))
        if not self.acl:
            raise KernelValidationError("acl must be explicit")
        object.__setattr__(
            self,
            "proposed_by",
            _text(self.proposed_by, "proposed_by"),
        )
        object.__setattr__(
            self,
            "created_at",
            _timestamp(self.created_at, "created_at"),
        )
        if self.expires_at is not None:
            expires_at = _timestamp(self.expires_at, "expires_at")
            if expires_at <= self.created_at:
                raise KernelValidationError("expires_at must follow created_at")
            object.__setattr__(self, "expires_at", expires_at)
        if not isinstance(self.version, int) or isinstance(self.version, bool) or self.version < 1:
            raise KernelValidationError("version must be a positive integer")
        if self.review_reason:
            object.__setattr__(
                self,
                "review_reason",
                _text(self.review_reason, "review_reason", max_chars=1_000),
            )
        if self.reviewed_by:
            object.__setattr__(
                self,
                "reviewed_by",
                _text(self.reviewed_by, "reviewed_by"),
            )
        if self.reviewed_at is not None:
            object.__setattr__(
                self,
                "reviewed_at",
                _timestamp(self.reviewed_at, "reviewed_at"),
            )
        if self.schema_version != MEMORY_STORE_SCHEMA_VERSION:
            raise KernelValidationError("unsupported MemoryCandidate schema_version")

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.content.encode("utf-8")).hexdigest()

    def is_expired(self, *, now: float | None = None) -> bool:
        current = time.time() if now is None else _timestamp(now, "now")
        return self.expires_at is not None and current >= self.expires_at

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "namespace": list(self.namespace),
            "kind": self.kind.value,
            "content": self.content,
            "source_refs": list(self.source_refs),
            "trust": self.trust.value,
            "confidence": self.confidence,
            "sensitivity": self.sensitivity.value,
            "acl": list(self.acl),
            "proposed_by": self.proposed_by,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "version": self.version,
            "review_status": self.review_status.value,
            "review_reason": self.review_reason,
            "reviewed_by": self.reviewed_by,
            "reviewed_at": self.reviewed_at,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MemoryCandidate":
        return cls(
            candidate_id=value.get("candidate_id"),
            namespace=tuple(value.get("namespace", ())),
            kind=value.get("kind"),
            content=value.get("content"),
            source_refs=tuple(value.get("source_refs", ())),
            trust=value.get("trust"),
            confidence=value.get("confidence"),
            sensitivity=value.get("sensitivity"),
            acl=tuple(value.get("acl", ())),
            proposed_by=value.get("proposed_by"),
            created_at=value.get("created_at"),
            expires_at=value.get("expires_at"),
            version=value.get("version", 1),
            review_status=value.get("review_status", MemoryReviewStatus.PENDING),
            review_reason=value.get("review_reason", ""),
            reviewed_by=value.get("reviewed_by", ""),
            reviewed_at=value.get("reviewed_at"),
            schema_version=value.get("schema_version", MEMORY_STORE_SCHEMA_VERSION),
        )


@dataclass(frozen=True, slots=True)
class MemoryRecord:
    record_id: str
    candidate_id: str
    namespace: tuple[str, ...]
    kind: MemoryKind
    content: str
    source_refs: tuple[str, ...]
    trust: TrustLevel
    confidence: float
    sensitivity: DataSensitivity
    acl: tuple[str, ...]
    version: int
    created_at: float
    expires_at: float | None
    reviewed_by: str
    reviewed_at: float
    review_status: MemoryReviewStatus = MemoryReviewStatus.APPROVED
    schema_version: int = MEMORY_STORE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        # Reuse candidate validation so read paths reject malformed active data.
        validated = MemoryCandidate(
            candidate_id=self.candidate_id,
            namespace=self.namespace,
            kind=self.kind,
            content=self.content,
            source_refs=self.source_refs,
            trust=self.trust,
            confidence=self.confidence,
            sensitivity=self.sensitivity,
            acl=self.acl,
            proposed_by=self.reviewed_by,
            created_at=self.created_at,
            expires_at=self.expires_at,
            version=self.version,
            review_status=self.review_status,
            reviewed_by=self.reviewed_by,
            reviewed_at=self.reviewed_at,
        )
        for field_name in (
            "candidate_id",
            "namespace",
            "kind",
            "content",
            "source_refs",
            "trust",
            "confidence",
            "sensitivity",
            "acl",
            "version",
            "created_at",
            "expires_at",
            "reviewed_by",
            "reviewed_at",
            "review_status",
        ):
            object.__setattr__(self, field_name, getattr(validated, field_name))
        object.__setattr__(self, "record_id", _text(self.record_id, "record_id"))
        if self.review_status is not MemoryReviewStatus.APPROVED:
            raise KernelValidationError("active MemoryRecord must be approved")
        if self.reviewed_at < self.created_at:
            raise KernelValidationError("reviewed_at must not precede created_at")
        if self.schema_version != MEMORY_STORE_SCHEMA_VERSION:
            raise KernelValidationError("unsupported MemoryRecord schema_version")

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.content.encode("utf-8")).hexdigest()

    def to_knowledge_item(self) -> KnowledgeItem:
        return KnowledgeItem(
            item_id=self.record_id,
            kind=KnowledgeKind.MEMORY,
            namespace=self.namespace,
            content_sha256=self.content_sha256,
            source_refs=self.source_refs,
            trust=self.trust,
            sensitivity=self.sensitivity,
            acl=self.acl,
            version=self.version,
            created_at=self.created_at,
            valid_until=self.expires_at,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_id": self.record_id,
            "candidate_id": self.candidate_id,
            "namespace": list(self.namespace),
            "kind": self.kind.value,
            "content": self.content,
            "source_refs": list(self.source_refs),
            "trust": self.trust.value,
            "confidence": self.confidence,
            "sensitivity": self.sensitivity.value,
            "acl": list(self.acl),
            "version": self.version,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "reviewed_by": self.reviewed_by,
            "reviewed_at": self.reviewed_at,
            "review_status": self.review_status.value,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MemoryRecord":
        return cls(
            record_id=value.get("record_id"),
            candidate_id=value.get("candidate_id"),
            namespace=tuple(value.get("namespace", ())),
            kind=value.get("kind"),
            content=value.get("content"),
            source_refs=tuple(value.get("source_refs", ())),
            trust=value.get("trust"),
            confidence=value.get("confidence"),
            sensitivity=value.get("sensitivity"),
            acl=tuple(value.get("acl", ())),
            version=value.get("version"),
            created_at=value.get("created_at"),
            expires_at=value.get("expires_at"),
            reviewed_by=value.get("reviewed_by"),
            reviewed_at=value.get("reviewed_at"),
            review_status=value.get("review_status", MemoryReviewStatus.APPROVED),
            schema_version=value.get("schema_version", MEMORY_STORE_SCHEMA_VERSION),
        )


class MemoryStore:
    def __init__(self, workspace_root: str | Path) -> None:
        self.workspace_root = Path(workspace_root).expanduser().resolve()
        self.path = self.workspace_root / MEMORY_STORE_PATH

    def propose(
        self,
        *,
        principal: Principal,
        content: str,
        namespace: tuple[str, ...],
        kind: MemoryKind,
        source_refs: tuple[str, ...],
        trust: TrustLevel,
        confidence: float,
        sensitivity: DataSensitivity,
        acl: tuple[str, ...],
        ttl_seconds: float | None = None,
        now: float | None = None,
    ) -> MemoryCandidate:
        if (
            not isinstance(principal, Principal)
            or MEMORY_PROPOSE_SCOPE not in principal.scopes
        ):
            raise PermissionError("memory propose scope is required")
        created_at = time.time() if now is None else _timestamp(now, "now")
        expires_at = None
        if ttl_seconds is not None:
            ttl = _timestamp(ttl_seconds, "ttl_seconds")
            if ttl <= 0:
                raise KernelValidationError("ttl_seconds must be positive")
            expires_at = created_at + ttl
        fingerprint = canonical_digest(
            {
                "namespace": list(namespace),
                "kind": str(MemoryKind(kind).value),
                "content_sha256": hashlib.sha256(
                    str(content).encode("utf-8")
                ).hexdigest(),
                "source_refs": sorted(source_refs),
                "trust": str(TrustLevel(trust).value),
                "principal": principal.principal_digest,
            },
            "MemoryCandidate fingerprint",
        )
        candidate = MemoryCandidate(
            candidate_id=f"memcand-{fingerprint[:32]}",
            namespace=namespace,
            kind=kind,
            content=content,
            source_refs=source_refs,
            trust=trust,
            confidence=confidence,
            sensitivity=sensitivity,
            acl=acl,
            proposed_by=principal.principal_digest,
            created_at=created_at,
            expires_at=expires_at,
        )
        allowed_acl = {
            f"subject:{principal.subject}",
            f"tenant:{principal.tenant_id}",
        }
        if not set(candidate.acl).issubset(allowed_acl):
            raise PermissionError(
                "memory candidate ACL exceeds the proposing principal boundary"
            )
        if namespace_tenant_id(candidate.namespace) != principal.tenant_id:
            raise PermissionError(
                "memory candidate namespace must be bound to the principal tenant"
            )
        with workspace_write_lock(self.workspace_root):
            state = self._load()
            existing = state["candidates"].get(candidate.candidate_id)
            if existing is not None:
                return MemoryCandidate.from_dict(existing)
            if len(state["candidates"]) >= MAX_MEMORY_ITEMS:
                raise MemoryStoreError("memory candidate capacity exceeded")
            state["candidates"][candidate.candidate_id] = candidate.to_dict()
            self._write(state)
        return candidate

    def review(
        self,
        candidate_id: str,
        *,
        reviewer: Principal,
        approve: bool,
        reason: str,
        now: float | None = None,
    ) -> MemoryCandidate:
        if not isinstance(reviewer, Principal) or MEMORY_REVIEW_SCOPE not in reviewer.scopes:
            raise PermissionError("memory review scope is required")
        reviewed_at = time.time() if now is None else _timestamp(now, "now")
        with workspace_write_lock(self.workspace_root):
            state = self._load()
            raw = state["candidates"].get(candidate_id)
            if raw is None:
                raise KeyError(candidate_id)
            candidate = MemoryCandidate.from_dict(raw)
            if namespace_tenant_id(candidate.namespace) != reviewer.tenant_id:
                raise PermissionError(
                    "memory candidate tenant does not authorize this reviewer"
                )
            reviewer_grants = {
                f"subject:{reviewer.subject}",
                f"tenant:{reviewer.tenant_id}",
            }
            if not set(candidate.acl).intersection(reviewer_grants):
                raise PermissionError(
                    "memory candidate ACL does not authorize this reviewer"
                )
            if candidate.review_status is not MemoryReviewStatus.PENDING:
                raise MemoryStoreError("memory candidate is already resolved")
            if candidate.is_expired(now=reviewed_at):
                resolved = replace(
                    candidate,
                    review_status=MemoryReviewStatus.EXPIRED,
                    review_reason="candidate_expired",
                    reviewed_by=reviewer.principal_digest,
                    reviewed_at=reviewed_at,
                )
            else:
                resolved = replace(
                    candidate,
                    review_status=(
                        MemoryReviewStatus.APPROVED
                        if approve
                        else MemoryReviewStatus.REJECTED
                    ),
                    review_reason=reason,
                    reviewed_by=reviewer.principal_digest,
                    reviewed_at=reviewed_at,
                )
            state["candidates"][candidate_id] = resolved.to_dict()
            if resolved.review_status is MemoryReviewStatus.APPROVED:
                record = MemoryRecord(
                    record_id=f"mem-{candidate_id.removeprefix('memcand-')}",
                    candidate_id=candidate_id,
                    namespace=resolved.namespace,
                    kind=resolved.kind,
                    content=resolved.content,
                    source_refs=resolved.source_refs,
                    trust=resolved.trust,
                    confidence=resolved.confidence,
                    sensitivity=resolved.sensitivity,
                    acl=resolved.acl,
                    version=resolved.version,
                    created_at=resolved.created_at,
                    expires_at=resolved.expires_at,
                    reviewed_by=reviewer.principal_digest,
                    reviewed_at=reviewed_at,
                )
                if len(state["records"]) >= MAX_MEMORY_ITEMS:
                    raise MemoryStoreError("active memory capacity exceeded")
                state["records"][record.record_id] = record.to_dict()
            self._write(state)
        return resolved

    def active_records(
        self,
        *,
        principal: Principal,
        namespace_prefix: tuple[str, ...] = (),
        now: float | None = None,
    ) -> tuple[MemoryRecord, ...]:
        if (
            not isinstance(principal, Principal)
            or MEMORY_READ_SCOPE not in principal.scopes
        ):
            raise PermissionError("memory read scope is required")
        current = time.time() if now is None else _timestamp(now, "now")
        state = self._load()
        records: list[MemoryRecord] = []
        for raw in state["records"].values():
            record = MemoryRecord.from_dict(raw)
            if namespace_prefix and record.namespace[: len(namespace_prefix)] != namespace_prefix:
                continue
            if record.expires_at is not None and current >= record.expires_at:
                continue
            if not record.to_knowledge_item().is_authorized(principal, now=current):
                continue
            records.append(record)
        return tuple(
            sorted(records, key=lambda item: (item.namespace, item.created_at, item.record_id))
        )

    def get_candidate(
        self,
        candidate_id: str,
        *,
        principal: Principal,
    ) -> MemoryCandidate | None:
        if (
            not isinstance(principal, Principal)
            or not {
                MEMORY_PROPOSE_SCOPE,
                MEMORY_REVIEW_SCOPE,
            }.intersection(principal.scopes)
        ):
            raise PermissionError("memory candidate read scope is required")
        raw = self._load()["candidates"].get(candidate_id)
        if raw is None:
            return None
        candidate = MemoryCandidate.from_dict(raw)
        if namespace_tenant_id(candidate.namespace) != principal.tenant_id:
            raise PermissionError("memory candidate tenant denies this principal")
        grants = {
            f"subject:{principal.subject}",
            f"tenant:{principal.tenant_id}",
        }
        if not set(candidate.acl).intersection(grants):
            raise PermissionError("memory candidate ACL denies this principal")
        return candidate

    def _load(self) -> dict[str, Any]:
        if not self.path.is_file():
            return {
                "schema_version": MEMORY_STORE_SCHEMA_VERSION,
                "candidates": {},
                "records": {},
            }
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise MemoryStoreError("memory store is unreadable") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != MEMORY_STORE_SCHEMA_VERSION
            or not isinstance(payload.get("candidates"), dict)
            or not isinstance(payload.get("records"), dict)
        ):
            raise MemoryStoreError("memory store is invalid")
        return payload

    def _write(self, state: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.path, state)


__all__ = [
    "MAX_MEMORY_CONTENT_CHARS",
    "MEMORY_PROPOSE_SCOPE",
    "MEMORY_READ_SCOPE",
    "MEMORY_REVIEW_SCOPE",
    "MEMORY_STORE_PATH",
    "MemoryCandidate",
    "MemoryKind",
    "MemoryRecord",
    "MemoryReviewStatus",
    "MemoryStore",
    "MemoryStoreError",
]
