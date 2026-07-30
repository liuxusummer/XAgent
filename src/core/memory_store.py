"""Validated reviewed-memory lifecycle store.

Untrusted inputs can only create pending MemoryCandidate values.  A separate,
scope-checked review converts a candidate into an active MemoryRecord and must
explicitly supersede overlapping facts.  Candidate, record, and retention audit
state share one workspace-locked JSON document so lifecycle changes commit
atomically.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
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
from src.core.safe_fs import (
    FileSizeLimitExceededError,
    atomic_write_text_beneath,
    open_regular_file_beneath,
    read_stable_text,
)
from src.core.workspace_storage import workspace_write_lock


MEMORY_STORE_SCHEMA_VERSION = 2
LEGACY_MEMORY_STORE_SCHEMA_VERSION = 1
MEMORY_STORE_PATH = Path("runtime") / "agent_kernel" / "memory-store.json"
MAX_MEMORY_CONTENT_CHARS = 8_000
MAX_MEMORY_KEY_CHARS = 128
MAX_MEMORY_ITEMS = 2_000
MAX_MEMORY_MAINTENANCE_COUNT = (1 << 63) - 1
MAX_MEMORY_STORE_BYTES = 64 * 1024 * 1024
MEMORY_PROPOSE_SCOPE = "memory.propose"
MEMORY_READ_SCOPE = "memory.read"
MEMORY_REVIEW_SCOPE = "memory.review"
EMPTY_DIGEST = "0" * 64
_LEGACY_CANDIDATE_FIELDS = frozenset(
    {
        "schema_version",
        "candidate_id",
        "namespace",
        "kind",
        "content",
        "source_refs",
        "trust",
        "confidence",
        "sensitivity",
        "acl",
        "proposed_by",
        "created_at",
        "expires_at",
        "version",
        "review_status",
        "review_reason",
        "reviewed_by",
        "reviewed_at",
    }
)
_LEGACY_RECORD_FIELDS = frozenset(
    {
        "schema_version",
        "record_id",
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
    }
)


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


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise MemoryStoreError("memory store contains duplicate keys")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> None:
    raise MemoryStoreError("memory store contains a non-standard number")


def _has_hex_id(value: str, prefix: str) -> bool:
    suffix = value.removeprefix(prefix)
    return (
        value.startswith(prefix)
        and len(suffix) == 32
        and all(character in "0123456789abcdef" for character in suffix)
    )


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
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise KernelValidationError(f"{field_name} must be valid UTF-8") from exc
    return text


def _strings(
    values: Iterable[str],
    field_name: str,
    *,
    max_items: int = 64,
    preserve_order: bool = False,
) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise KernelValidationError(f"{field_name} must be an array")
    try:
        iterator = iter(values)
    except TypeError as exc:
        raise KernelValidationError(f"{field_name} must be iterable") from exc
    normalized_items: list[str] = []
    for value in iterator:
        if len(normalized_items) >= max_items:
            raise KernelValidationError(f"{field_name} exceeds its bound")
        normalized_items.append(_text(value, field_name))
    normalized = tuple(normalized_items)
    if len(set(normalized)) != len(normalized):
        raise KernelValidationError(f"{field_name} values must be unique")
    return normalized if preserve_order else tuple(sorted(normalized))


def _timestamp(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise KernelValidationError(f"{field_name} must be a finite timestamp")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise KernelValidationError(f"{field_name} must be a finite timestamp") from exc
    if not math.isfinite(result) or result < 0:
        raise KernelValidationError(f"{field_name} must be a finite timestamp")
    return result


def _memory_key(
    value: str,
    *,
    namespace: tuple[str, ...],
    kind: MemoryKind,
    content_sha256: str,
) -> str:
    if not isinstance(value, str):
        raise KernelValidationError("memory_key must be a string")
    if value:
        return _explicit_memory_key(value)
    identity = canonical_digest(
        {
            "namespace": list(namespace),
            "kind": kind.value,
            "content_sha256": content_sha256,
        },
        "default memory key",
    )
    return f"content:{identity[:32]}"


def _explicit_memory_key(value: Any) -> str:
    key = _text(value, "memory_key", max_chars=MAX_MEMORY_KEY_CHARS)
    if any(
        character
        not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:/-"
        for character in key
    ):
        raise KernelValidationError("memory_key contains unsupported characters")
    return key


def _digest(value: Any, field_name: str) -> str:
    digest = _text(value, field_name)
    if (
        len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise KernelValidationError(
            f"{field_name} must be a lowercase SHA-256 digest"
        )
    return digest


def _purge_chain_digest(
    previous: str,
    *,
    compacted_at: float,
    compacted_by: str,
    removed: Iterable[Mapping[str, Any]],
) -> str:
    digest = hashlib.sha256(b"XAgent Memory Purge v1\0")
    values: Iterable[Mapping[str, Any]] = (
        {
            "previous": previous,
            "compacted_at": compacted_at,
            "compacted_by": compacted_by,
        },
        *removed,
    )
    for value in values:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


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
    memory_key: str = ""
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
        if not isinstance(self.content, str):
            raise KernelValidationError("memory content must be text")
        content = self.content.strip()
        if not content:
            raise KernelValidationError("memory content must not be empty")
        if len(content) > MAX_MEMORY_CONTENT_CHARS:
            raise KernelValidationError("memory content exceeds its bound")
        try:
            content.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise KernelValidationError(
                "memory content must be valid UTF-8"
            ) from exc
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
            _digest(self.proposed_by, "proposed_by"),
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
        if not isinstance(self.review_reason, str):
            raise KernelValidationError("review_reason must be a string")
        if self.review_reason:
            object.__setattr__(
                self,
                "review_reason",
                _text(self.review_reason, "review_reason", max_chars=1_000),
            )
        if not isinstance(self.reviewed_by, str):
            raise KernelValidationError("reviewed_by must be a string")
        if self.reviewed_by:
            object.__setattr__(
                self,
                "reviewed_by",
                _digest(self.reviewed_by, "reviewed_by"),
            )
        if self.reviewed_at is not None:
            object.__setattr__(
                self,
                "reviewed_at",
                _timestamp(self.reviewed_at, "reviewed_at"),
            )
        object.__setattr__(
            self,
            "memory_key",
            _memory_key(
                self.memory_key,
                namespace=self.namespace,
                kind=self.kind,
                content_sha256=self.content_sha256,
            ),
        )
        if (
            not isinstance(self.schema_version, int)
            or isinstance(self.schema_version, bool)
            or self.schema_version != MEMORY_STORE_SCHEMA_VERSION
        ):
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
            "memory_key": self.memory_key,
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
            memory_key=value.get("memory_key", ""),
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
    memory_key: str = ""
    supersedes: tuple[str, ...] = ()
    superseded_by: str = ""
    superseded_at: float | None = None
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
            memory_key=self.memory_key,
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
            "memory_key",
        ):
            object.__setattr__(self, field_name, getattr(validated, field_name))
        object.__setattr__(self, "record_id", _text(self.record_id, "record_id"))
        if self.review_status is not MemoryReviewStatus.APPROVED:
            raise KernelValidationError("MemoryRecord must be approved")
        if self.reviewed_at < self.created_at:
            raise KernelValidationError("reviewed_at must not precede created_at")
        supersedes = _strings(self.supersedes, "supersedes")
        if any(not _has_hex_id(item, "mem-") for item in supersedes):
            raise KernelValidationError("supersedes contains an invalid record id")
        if self.record_id in supersedes:
            raise KernelValidationError("memory record cannot supersede itself")
        object.__setattr__(self, "supersedes", supersedes)
        if not isinstance(self.superseded_by, str):
            raise KernelValidationError("superseded_by must be a string")
        superseded_by = self.superseded_by
        if superseded_by:
            superseded_by = _text(superseded_by, "superseded_by")
            if not _has_hex_id(superseded_by, "mem-"):
                raise KernelValidationError(
                    "superseded_by must be a memory record id"
                )
            if superseded_by == self.record_id:
                raise KernelValidationError(
                    "memory record cannot be superseded by itself"
                )
        object.__setattr__(self, "superseded_by", superseded_by)
        if self.superseded_at is not None:
            superseded_at = _timestamp(self.superseded_at, "superseded_at")
            if superseded_at < self.reviewed_at:
                raise KernelValidationError(
                    "superseded_at must not precede reviewed_at"
                )
            object.__setattr__(self, "superseded_at", superseded_at)
        if bool(self.superseded_by) != (self.superseded_at is not None):
            raise KernelValidationError(
                "superseded_by and superseded_at must be set together"
            )
        if self.schema_version != MEMORY_STORE_SCHEMA_VERSION:
            raise KernelValidationError("unsupported MemoryRecord schema_version")

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.content.encode("utf-8")).hexdigest()

    def is_active(self, *, now: float | None = None) -> bool:
        current = time.time() if now is None else _timestamp(now, "now")
        return (
            not self.superseded_by
            and (self.expires_at is None or current < self.expires_at)
        )

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
            "memory_key": self.memory_key,
            "supersedes": list(self.supersedes),
            "superseded_by": self.superseded_by,
            "superseded_at": self.superseded_at,
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
            memory_key=value.get("memory_key", ""),
            supersedes=tuple(value.get("supersedes", ())),
            superseded_by=value.get("superseded_by", ""),
            superseded_at=value.get("superseded_at"),
            review_status=value.get("review_status", MemoryReviewStatus.APPROVED),
            schema_version=value.get("schema_version", MEMORY_STORE_SCHEMA_VERSION),
        )


@dataclass(frozen=True, slots=True)
class MemoryCandidateAssessment:
    candidate_id: str
    memory_key: str
    duplicate_record_ids: tuple[str, ...]
    conflict_record_ids: tuple[str, ...]
    inaccessible_overlap_count: int = 0

    @property
    def required_supersedes(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                set(self.duplicate_record_ids).union(self.conflict_record_ids)
            )
        )


@dataclass(frozen=True, slots=True)
class MemoryCompactionReport:
    purged_candidates: int
    purged_records: int
    purge_chain_sha256: str


@dataclass(frozen=True, slots=True)
class _MemoryMaintenance:
    compaction_count: int = 0
    purged_candidates: int = 0
    purged_records: int = 0
    last_compacted_at: float | None = None
    last_compacted_by: str = ""
    purge_chain_sha256: str = EMPTY_DIGEST

    def __post_init__(self) -> None:
        for field_name in (
            "compaction_count",
            "purged_candidates",
            "purged_records",
        ):
            value = getattr(self, field_name)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or not 0 <= value <= MAX_MEMORY_MAINTENANCE_COUNT
            ):
                raise KernelValidationError(
                    f"{field_name} is outside its supported bound"
                )
        if self.last_compacted_at is None:
            if (
                self.compaction_count
                or self.purged_candidates
                or self.purged_records
                or self.last_compacted_by
                or self.purge_chain_sha256 != EMPTY_DIGEST
            ):
                raise KernelValidationError(
                    "empty maintenance state contains compaction metadata"
                )
        else:
            object.__setattr__(
                self,
                "last_compacted_at",
                _timestamp(self.last_compacted_at, "last_compacted_at"),
            )
            object.__setattr__(
                self,
                "last_compacted_by",
                _digest(self.last_compacted_by, "last_compacted_by"),
            )
            if self.compaction_count < 1:
                raise KernelValidationError(
                    "maintenance compaction_count is inconsistent"
                )
            object.__setattr__(
                self,
                "purge_chain_sha256",
                _digest(self.purge_chain_sha256, "purge_chain_sha256"),
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "compaction_count": self.compaction_count,
            "purged_candidates": self.purged_candidates,
            "purged_records": self.purged_records,
            "last_compacted_at": self.last_compacted_at,
            "last_compacted_by": self.last_compacted_by,
            "purge_chain_sha256": self.purge_chain_sha256,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "_MemoryMaintenance":
        return cls(
            compaction_count=value.get("compaction_count", 0),
            purged_candidates=value.get("purged_candidates", 0),
            purged_records=value.get("purged_records", 0),
            last_compacted_at=value.get("last_compacted_at"),
            last_compacted_by=value.get("last_compacted_by", ""),
            purge_chain_sha256=value.get(
                "purge_chain_sha256",
                EMPTY_DIGEST,
            ),
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
        memory_key: str = "",
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
        ttl = None
        if ttl_seconds is not None:
            ttl = _timestamp(ttl_seconds, "ttl_seconds")
            if ttl <= 0:
                raise KernelValidationError("ttl_seconds must be positive")
            expires_at = created_at + ttl
        candidate = MemoryCandidate(
            candidate_id="pending",
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
            memory_key=memory_key,
        )
        fingerprint = canonical_digest(
            {
                "namespace": list(candidate.namespace),
                "kind": candidate.kind.value,
                "content_sha256": candidate.content_sha256,
                "source_refs": list(candidate.source_refs),
                "trust": candidate.trust.value,
                "confidence": candidate.confidence,
                "sensitivity": candidate.sensitivity.value,
                "acl": list(candidate.acl),
                "memory_key": candidate.memory_key,
                "ttl_seconds": ttl,
                "version": candidate.version,
                "principal": principal.principal_digest,
            },
            "MemoryCandidate fingerprint",
        )
        candidate = replace(
            candidate,
            candidate_id=f"memcand-{fingerprint[:32]}",
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
        with workspace_write_lock(
            self.workspace_root,
            require_secure_path=True,
        ):
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
        supersede_record_ids: tuple[str, ...] = (),
        now: float | None = None,
    ) -> MemoryCandidate:
        if (
            not isinstance(reviewer, Principal)
            or MEMORY_REVIEW_SCOPE not in reviewer.scopes
        ):
            raise PermissionError("memory review scope is required")
        candidate_key = _text(candidate_id, "candidate_id")
        if not isinstance(approve, bool):
            raise KernelValidationError("approve must be boolean")
        normalized_reason = _text(
            reason,
            "review_reason",
            max_chars=1_000,
        )
        normalized_supersedes = _strings(
            supersede_record_ids,
            "supersede_record_ids",
        )
        if any(
            not _has_hex_id(record_id, "mem-")
            for record_id in normalized_supersedes
        ):
            raise KernelValidationError(
                "supersede_record_ids contains an invalid record id"
            )
        if normalized_supersedes and not approve:
            raise KernelValidationError(
                "rejected candidates cannot supersede memory records"
            )
        reviewed_at = time.time() if now is None else _timestamp(now, "now")
        with workspace_write_lock(
            self.workspace_root,
            require_secure_path=True,
        ):
            state = self._load()
            raw = state["candidates"].get(candidate_key)
            if raw is None:
                raise KeyError(candidate_key)
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
            if reviewed_at < candidate.created_at:
                raise KernelValidationError(
                    "review time must not precede candidate creation"
                )
            if candidate.is_expired(now=reviewed_at):
                if normalized_supersedes:
                    raise MemoryStoreError(
                        "expired candidates cannot supersede memory records"
                    )
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
                    review_reason=normalized_reason,
                    reviewed_by=reviewer.principal_digest,
                    reviewed_at=reviewed_at,
                )
                if approve:
                    assessment = self._assess_candidate_in_state(
                        state,
                        candidate=candidate,
                        reviewer=reviewer,
                        now=reviewed_at,
                    )
                    if assessment.inaccessible_overlap_count:
                        raise PermissionError(
                            "reviewer cannot authorize every overlapping "
                            "active memory record"
                        )
                    if (
                        normalized_supersedes
                        != assessment.required_supersedes
                    ):
                        raise MemoryStoreError(
                            "approval must supersede every active duplicate "
                            "and conflict reported by assess_candidate"
                        )
            state["candidates"][candidate_key] = resolved.to_dict()
            if resolved.review_status is MemoryReviewStatus.APPROVED:
                record = MemoryRecord(
                    record_id=f"mem-{candidate_key.removeprefix('memcand-')}",
                    candidate_id=candidate_key,
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
                    memory_key=resolved.memory_key,
                    supersedes=normalized_supersedes,
                )
                if len(state["records"]) >= MAX_MEMORY_ITEMS:
                    raise MemoryStoreError("active memory capacity exceeded")
                for prior_record_id in normalized_supersedes:
                    prior_record = MemoryRecord.from_dict(
                        state["records"][prior_record_id]
                    )
                    state["records"][prior_record_id] = replace(
                        prior_record,
                        superseded_by=record.record_id,
                        superseded_at=reviewed_at,
                    ).to_dict()
                state["records"][record.record_id] = record.to_dict()
            self._write(state)
        return resolved

    def assess_candidate(
        self,
        candidate_id: str,
        *,
        reviewer: Principal,
        now: float | None = None,
    ) -> MemoryCandidateAssessment:
        if (
            not isinstance(reviewer, Principal)
            or MEMORY_REVIEW_SCOPE not in reviewer.scopes
        ):
            raise PermissionError("memory review scope is required")
        candidate_key = _text(candidate_id, "candidate_id")
        current = time.time() if now is None else _timestamp(now, "now")
        state = self._load()
        raw = state["candidates"].get(candidate_key)
        if raw is None:
            raise KeyError(candidate_key)
        candidate = MemoryCandidate.from_dict(raw)
        self._authorize_candidate(candidate, reviewer)
        return self._assess_candidate_in_state(
            state,
            candidate=candidate,
            reviewer=reviewer,
            now=current,
        )

    def provenance_records(
        self,
        *,
        principal: Principal,
        memory_key: str,
        namespace_prefix: tuple[str, ...] = (),
    ) -> tuple[MemoryRecord, ...]:
        if (
            not isinstance(principal, Principal)
            or MEMORY_READ_SCOPE not in principal.scopes
        ):
            raise PermissionError("memory read scope is required")
        normalized_key = _explicit_memory_key(memory_key)
        prefix = (
            _strings(
                namespace_prefix,
                "namespace_prefix",
                max_items=16,
                preserve_order=True,
            )
            if namespace_prefix
            else ()
        )
        authorized: dict[str, MemoryRecord] = {}
        for raw in self._load()["records"].values():
            record = MemoryRecord.from_dict(raw)
            if prefix and record.namespace[: len(prefix)] != prefix:
                continue
            if not self._record_acl_authorizes(record, principal):
                continue
            authorized[record.record_id] = record
        selected_ids = {
            record.record_id
            for record in authorized.values()
            if record.memory_key == normalized_key
        }
        while True:
            related_ids = set(selected_ids)
            for record_id in selected_ids:
                record = authorized[record_id]
                related_ids.update(record.supersedes)
                if record.superseded_by:
                    related_ids.add(record.superseded_by)
            expanded = related_ids.intersection(authorized)
            if expanded == selected_ids:
                break
            selected_ids = expanded
        return tuple(
            sorted(
                (authorized[record_id] for record_id in selected_ids),
                key=lambda item: (
                    item.reviewed_at,
                    item.record_id,
                ),
            )
        )

    def compact(
        self,
        *,
        reviewer: Principal,
        retention_seconds: float,
        now: float | None = None,
    ) -> MemoryCompactionReport:
        if (
            not isinstance(reviewer, Principal)
            or MEMORY_REVIEW_SCOPE not in reviewer.scopes
        ):
            raise PermissionError("memory review scope is required")
        retention = _timestamp(retention_seconds, "retention_seconds")
        if retention <= 0:
            raise KernelValidationError("retention_seconds must be positive")
        current = time.time() if now is None else _timestamp(now, "now")
        cutoff = current - retention
        with workspace_write_lock(
            self.workspace_root,
            require_secure_path=True,
        ):
            state = self._load()
            maintenance = _MemoryMaintenance.from_dict(state["maintenance"])
            if (
                maintenance.last_compacted_at is not None
                and current < maintenance.last_compacted_at
            ):
                raise MemoryStoreError(
                    "compaction time precedes the previous compaction"
                )

            removable_records: dict[str, MemoryRecord] = {}
            for record_id, raw_record in state["records"].items():
                record = MemoryRecord.from_dict(raw_record)
                if not self._record_acl_authorizes(record, reviewer):
                    continue
                retirement_times = tuple(
                    timestamp
                    for timestamp in (
                        record.superseded_at,
                        record.expires_at,
                    )
                    if timestamp is not None
                )
                retired_at = min(retirement_times, default=None)
                if retired_at is not None and retired_at <= cutoff:
                    removable_records[record_id] = record

            while True:
                retained_predecessors = {
                    record.superseded_by
                    for record_id, raw_record in state["records"].items()
                    if record_id not in removable_records
                    for record in (MemoryRecord.from_dict(raw_record),)
                    if record.superseded_by in removable_records
                }
                if not retained_predecessors:
                    break
                for successor_id in retained_predecessors:
                    removable_records.pop(successor_id, None)

            removable_candidate_ids = {
                record.candidate_id for record in removable_records.values()
            }
            standalone_candidates: dict[str, MemoryCandidate] = {}
            for candidate_id, raw_candidate in state["candidates"].items():
                if candidate_id in removable_candidate_ids:
                    continue
                candidate = MemoryCandidate.from_dict(raw_candidate)
                if not self._candidate_acl_authorizes(candidate, reviewer):
                    continue
                retired_at = candidate.reviewed_at
                if (
                    candidate.review_status is MemoryReviewStatus.PENDING
                    and candidate.expires_at is not None
                ):
                    retired_at = candidate.expires_at
                if (
                    candidate.review_status
                    in {
                        MemoryReviewStatus.REJECTED,
                        MemoryReviewStatus.EXPIRED,
                    }
                    or (
                        candidate.review_status is MemoryReviewStatus.PENDING
                        and candidate.expires_at is not None
                    )
                ) and retired_at is not None and retired_at <= cutoff:
                    standalone_candidates[candidate_id] = candidate

            if not removable_records and not standalone_candidates:
                return MemoryCompactionReport(
                    purged_candidates=0,
                    purged_records=0,
                    purge_chain_sha256=maintenance.purge_chain_sha256,
                )

            removed_record_ids = set(removable_records)
            summaries: list[dict[str, Any]] = []
            for record_id, record in sorted(removable_records.items()):
                summaries.append(
                    {
                        "type": "record",
                        "record_id": record_id,
                        "candidate_id": record.candidate_id,
                        "memory_key": record.memory_key,
                        "content_sha256": record.content_sha256,
                        "superseded_by": record.superseded_by,
                    }
                )
                state["records"].pop(record_id)
                state["candidates"].pop(record.candidate_id)
            for candidate_id, candidate in sorted(
                standalone_candidates.items()
            ):
                summaries.append(
                    {
                        "type": "candidate",
                        "candidate_id": candidate_id,
                        "memory_key": candidate.memory_key,
                        "content_sha256": candidate.content_sha256,
                        "review_status": candidate.review_status.value,
                    }
                )
                state["candidates"].pop(candidate_id)

            for record_id, raw_record in tuple(state["records"].items()):
                record = MemoryRecord.from_dict(raw_record)
                retained_predecessors = tuple(
                    predecessor
                    for predecessor in record.supersedes
                    if predecessor not in removed_record_ids
                )
                if retained_predecessors != record.supersedes:
                    state["records"][record_id] = replace(
                        record,
                        supersedes=retained_predecessors,
                    ).to_dict()

            purge_chain_sha256 = _purge_chain_digest(
                maintenance.purge_chain_sha256,
                compacted_at=current,
                compacted_by=reviewer.principal_digest,
                removed=summaries,
            )
            removed_candidate_count = (
                len(removable_records) + len(standalone_candidates)
            )
            state["maintenance"] = _MemoryMaintenance(
                compaction_count=maintenance.compaction_count + 1,
                purged_candidates=(
                    maintenance.purged_candidates + removed_candidate_count
                ),
                purged_records=(
                    maintenance.purged_records + len(removable_records)
                ),
                last_compacted_at=current,
                last_compacted_by=reviewer.principal_digest,
                purge_chain_sha256=purge_chain_sha256,
            ).to_dict()
            self._write(state)
        return MemoryCompactionReport(
            purged_candidates=removed_candidate_count,
            purged_records=len(removable_records),
            purge_chain_sha256=purge_chain_sha256,
        )

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
            if not record.is_active(now=current):
                continue
            if not record.to_knowledge_item().is_authorized(principal, now=current):
                continue
            records.append(record)
        return tuple(
            sorted(records, key=lambda item: (item.namespace, item.created_at, item.record_id))
        )

    def quality_metrics(
        self,
        *,
        principal: Principal,
        namespace_prefix: tuple[str, ...] = (),
        expected_content_sha256_by_key: Mapping[str, str] | None = None,
        now: float | None = None,
    ) -> dict[str, int | float]:
        if (
            not isinstance(principal, Principal)
            or MEMORY_READ_SCOPE not in principal.scopes
        ):
            raise PermissionError("memory read scope is required")
        current = time.time() if now is None else _timestamp(now, "now")
        raw_expected = (
            {}
            if expected_content_sha256_by_key is None
            else expected_content_sha256_by_key
        )
        if (
            not isinstance(raw_expected, Mapping)
            or len(raw_expected) > 64
        ):
            raise KernelValidationError(
                "expected memory mapping must contain at most 64 entries"
            )
        expected = {
            _explicit_memory_key(key): _digest(
                content_sha256,
                "expected content_sha256",
            )
            for key, content_sha256 in raw_expected.items()
        }
        if len(expected) != len(raw_expected):
            raise KernelValidationError(
                "expected memory keys must remain unique after normalization"
            )
        active: list[MemoryRecord] = []
        superseded = 0
        expired = 0
        for raw_record in self._load()["records"].values():
            record = MemoryRecord.from_dict(raw_record)
            if (
                namespace_prefix
                and record.namespace[: len(namespace_prefix)]
                != namespace_prefix
            ):
                continue
            if not self._record_acl_authorizes(record, principal):
                continue
            if record.superseded_by:
                superseded += 1
            elif record.expires_at is not None and current >= record.expires_at:
                expired += 1
            else:
                active.append(record)

        duplicate_groups: dict[tuple[Any, ...], int] = {}
        conflict_groups: dict[tuple[Any, ...], set[str]] = {}
        for record in active:
            duplicate_key = (
                record.namespace,
                record.kind,
                record.content_sha256,
            )
            duplicate_groups[duplicate_key] = (
                duplicate_groups.get(duplicate_key, 0) + 1
            )
            conflict_key = (
                record.namespace,
                record.kind,
                record.memory_key,
            )
            conflict_groups.setdefault(conflict_key, set()).add(
                record.content_sha256
            )
        expected_pairs = set(expected.items())
        correct = sum(
            expected.get(record.memory_key) == record.content_sha256
            for record in active
        )
        recalled_keys = {
            record.memory_key
            for record in active
            if expected.get(record.memory_key) == record.content_sha256
        }
        return {
            "active_records": len(active),
            "active_memory_keys": len(
                {
                    (record.namespace, record.kind, record.memory_key)
                    for record in active
                }
            ),
            "active_content_chars": sum(len(record.content) for record in active),
            "duplicate_active_records": sum(
                count - 1 for count in duplicate_groups.values() if count > 1
            ),
            "conflicting_active_records": sum(
                len(content_digests) - 1
                for content_digests in conflict_groups.values()
                if len(content_digests) > 1
            ),
            "superseded_suppressed": superseded,
            "expired_suppressed": expired,
            "expected_records": len(expected_pairs),
            "correct_expected_records": correct,
            "memory_precision": correct / len(active) if active else 0.0,
            "memory_recall": (
                len(recalled_keys) / len(expected_pairs)
                if expected_pairs
                else 0.0
            ),
        }

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
        candidate_key = _text(candidate_id, "candidate_id")
        raw = self._load()["candidates"].get(candidate_key)
        if raw is None:
            return None
        candidate = MemoryCandidate.from_dict(raw)
        self._authorize_candidate(candidate, principal)
        return candidate

    @staticmethod
    def _candidate_acl_authorizes(
        candidate: MemoryCandidate,
        principal: Principal,
    ) -> bool:
        if namespace_tenant_id(candidate.namespace) != principal.tenant_id:
            return False
        grants = {
            f"subject:{principal.subject}",
            f"tenant:{principal.tenant_id}",
        }
        return bool(set(candidate.acl).intersection(grants))

    @classmethod
    def _authorize_candidate(
        cls,
        candidate: MemoryCandidate,
        principal: Principal,
    ) -> None:
        if namespace_tenant_id(candidate.namespace) != principal.tenant_id:
            raise PermissionError("memory candidate tenant denies this principal")
        if not cls._candidate_acl_authorizes(candidate, principal):
            raise PermissionError("memory candidate ACL denies this principal")

    @staticmethod
    def _record_acl_authorizes(
        record: MemoryRecord,
        principal: Principal,
    ) -> bool:
        if namespace_tenant_id(record.namespace) != principal.tenant_id:
            return False
        grants = {
            f"subject:{principal.subject}",
            f"tenant:{principal.tenant_id}",
        }
        return bool(set(record.acl).intersection(grants))

    @staticmethod
    def _acl_audiences_overlap(
        first: tuple[str, ...],
        second: tuple[str, ...],
        *,
        tenant_id: str,
    ) -> bool:
        if set(first).intersection(second):
            return True
        tenant_grant = f"tenant:{tenant_id}"
        return tenant_grant in first or tenant_grant in second

    @classmethod
    def _assess_candidate_in_state(
        cls,
        state: Mapping[str, Any],
        *,
        candidate: MemoryCandidate,
        reviewer: Principal,
        now: float,
    ) -> MemoryCandidateAssessment:
        duplicate_record_ids: list[str] = []
        conflict_record_ids: list[str] = []
        inaccessible_overlap_count = 0
        for raw_record in state["records"].values():
            record = MemoryRecord.from_dict(raw_record)
            if (
                not record.is_active(now=now)
                or record.namespace != candidate.namespace
                or record.kind is not candidate.kind
            ):
                continue
            is_duplicate = record.content_sha256 == candidate.content_sha256
            is_conflict = (
                not is_duplicate and record.memory_key == candidate.memory_key
            )
            if not is_duplicate and not is_conflict:
                continue
            if not cls._record_acl_authorizes(record, reviewer):
                if cls._acl_audiences_overlap(
                    record.acl,
                    candidate.acl,
                    tenant_id=reviewer.tenant_id,
                ):
                    inaccessible_overlap_count += 1
                continue
            if is_duplicate:
                duplicate_record_ids.append(record.record_id)
            else:
                conflict_record_ids.append(record.record_id)
        return MemoryCandidateAssessment(
            candidate_id=candidate.candidate_id,
            memory_key=candidate.memory_key,
            duplicate_record_ids=tuple(sorted(duplicate_record_ids)),
            conflict_record_ids=tuple(sorted(conflict_record_ids)),
            inaccessible_overlap_count=inaccessible_overlap_count,
        )

    def _load(self) -> dict[str, Any]:
        try:
            file_descriptor, initial_stat = open_regular_file_beneath(
                self.workspace_root,
                MEMORY_STORE_PATH,
            )
        except FileNotFoundError:
            return {
                "schema_version": MEMORY_STORE_SCHEMA_VERSION,
                "candidates": {},
                "records": {},
                "maintenance": _MemoryMaintenance().to_dict(),
            }
        except OSError as exc:
            raise MemoryStoreError("memory store is unreadable") from exc
        try:
            encoded = read_stable_text(
                file_descriptor,
                initial_stat,
                max_bytes=MAX_MEMORY_STORE_BYTES,
            )
        except (
            FileSizeLimitExceededError,
            OSError,
            UnicodeError,
        ) as exc:
            raise MemoryStoreError("memory store is unreadable") from exc
        finally:
            os.close(file_descriptor)
        try:
            payload = json.loads(
                encoded,
                object_pairs_hook=_strict_json_object,
                parse_constant=_reject_json_constant,
            )
        except MemoryStoreError:
            raise
        except (
            TypeError,
            ValueError,
            json.JSONDecodeError,
            RecursionError,
        ) as exc:
            raise MemoryStoreError("memory store is unreadable") from exc
        try:
            if (
                isinstance(payload, dict)
                and payload.get("schema_version")
                == LEGACY_MEMORY_STORE_SCHEMA_VERSION
            ):
                payload = self._migrate_legacy_state(payload)
            self._validate_state(payload)
        except MemoryStoreError:
            raise
        except (KernelValidationError, TypeError, ValueError) as exc:
            raise MemoryStoreError("memory store is invalid") from exc
        return payload

    @staticmethod
    def _migrate_legacy_state(payload: Mapping[str, Any]) -> dict[str, Any]:
        if (
            set(payload) != {"schema_version", "candidates", "records"}
            or type(payload.get("schema_version")) is not int
            or payload.get("schema_version")
            != LEGACY_MEMORY_STORE_SCHEMA_VERSION
            or not isinstance(payload.get("candidates"), dict)
            or not isinstance(payload.get("records"), dict)
        ):
            raise MemoryStoreError("legacy memory store is invalid")

        migrated_candidates: dict[str, dict[str, Any]] = {}
        for candidate_id, raw_candidate in payload["candidates"].items():
            if (
                not isinstance(candidate_id, str)
                or not isinstance(raw_candidate, dict)
                or set(raw_candidate) != _LEGACY_CANDIDATE_FIELDS
                or raw_candidate.get("schema_version")
                != LEGACY_MEMORY_STORE_SCHEMA_VERSION
            ):
                raise MemoryStoreError(
                    "legacy memory store candidate is invalid"
                )
            candidate_payload = dict(raw_candidate)
            candidate_payload["schema_version"] = MEMORY_STORE_SCHEMA_VERSION
            candidate_payload["memory_key"] = ""
            candidate = MemoryCandidate.from_dict(candidate_payload)
            expected_legacy = candidate.to_dict()
            expected_legacy.pop("memory_key")
            expected_legacy["schema_version"] = (
                LEGACY_MEMORY_STORE_SCHEMA_VERSION
            )
            if expected_legacy != raw_candidate:
                raise MemoryStoreError(
                    "legacy memory store candidate is non-canonical"
                )
            migrated_candidates[candidate_id] = candidate.to_dict()

        migrated_records: dict[str, dict[str, Any]] = {}
        for record_id, raw_record in payload["records"].items():
            if (
                not isinstance(record_id, str)
                or not isinstance(raw_record, dict)
                or set(raw_record) != _LEGACY_RECORD_FIELDS
                or raw_record.get("schema_version")
                != LEGACY_MEMORY_STORE_SCHEMA_VERSION
            ):
                raise MemoryStoreError(
                    "legacy memory store record is invalid"
                )
            candidate_payload = migrated_candidates.get(
                raw_record.get("candidate_id")
            )
            if candidate_payload is None:
                raise MemoryStoreError(
                    "legacy memory record lacks an approved candidate"
                )
            record_payload = dict(raw_record)
            record_payload.update(
                {
                    "schema_version": MEMORY_STORE_SCHEMA_VERSION,
                    "memory_key": candidate_payload["memory_key"],
                    "supersedes": [],
                    "superseded_by": "",
                    "superseded_at": None,
                }
            )
            record = MemoryRecord.from_dict(record_payload)
            expected_legacy = record.to_dict()
            for field_name in (
                "memory_key",
                "supersedes",
                "superseded_by",
                "superseded_at",
            ):
                expected_legacy.pop(field_name)
            expected_legacy["schema_version"] = (
                LEGACY_MEMORY_STORE_SCHEMA_VERSION
            )
            if expected_legacy != raw_record:
                raise MemoryStoreError(
                    "legacy memory store record is non-canonical"
                )
            migrated_records[record_id] = record.to_dict()

        return {
            "schema_version": MEMORY_STORE_SCHEMA_VERSION,
            "candidates": migrated_candidates,
            "records": migrated_records,
            "maintenance": _MemoryMaintenance().to_dict(),
        }

    @staticmethod
    def _validate_state(payload: Any) -> None:
        if (
            not isinstance(payload, dict)
            or set(payload)
            != {"schema_version", "candidates", "records", "maintenance"}
            or type(payload.get("schema_version")) is not int
            or payload.get("schema_version") != MEMORY_STORE_SCHEMA_VERSION
            or not isinstance(payload.get("candidates"), dict)
            or not isinstance(payload.get("records"), dict)
            or not isinstance(payload.get("maintenance"), dict)
        ):
            raise MemoryStoreError("memory store is invalid")
        maintenance = _MemoryMaintenance.from_dict(payload["maintenance"])
        if maintenance.to_dict() != payload["maintenance"]:
            raise MemoryStoreError("memory store maintenance is non-canonical")
        raw_candidates = payload["candidates"]
        raw_records = payload["records"]
        if (
            len(raw_candidates) > MAX_MEMORY_ITEMS
            or len(raw_records) > MAX_MEMORY_ITEMS
        ):
            raise MemoryStoreError("memory store exceeds its capacity")

        candidates: dict[str, MemoryCandidate] = {}
        for candidate_id, raw_candidate in raw_candidates.items():
            if not isinstance(candidate_id, str) or not isinstance(raw_candidate, dict):
                raise MemoryStoreError("memory store candidate is invalid")
            candidate = MemoryCandidate.from_dict(raw_candidate)
            if (
                not _has_hex_id(candidate_id, "memcand-")
                or candidate_id != candidate.candidate_id
                or candidate.to_dict() != raw_candidate
            ):
                raise MemoryStoreError("memory store candidate is non-canonical")
            if candidate.review_status is MemoryReviewStatus.PENDING:
                if (
                    candidate.review_reason
                    or candidate.reviewed_by
                    or candidate.reviewed_at is not None
                ):
                    raise MemoryStoreError(
                        "pending memory candidate has review metadata"
                    )
            elif (
                not candidate.reviewed_by
                or candidate.reviewed_at is None
                or candidate.reviewed_at < candidate.created_at
            ):
                raise MemoryStoreError(
                    "resolved memory candidate lacks review metadata"
                )
            candidates[candidate_id] = candidate

        records: dict[str, MemoryRecord] = {}
        for record_id, raw_record in raw_records.items():
            if not isinstance(record_id, str) or not isinstance(raw_record, dict):
                raise MemoryStoreError("memory store record is invalid")
            record = MemoryRecord.from_dict(raw_record)
            if (
                not _has_hex_id(record_id, "mem-")
                or record_id != record.record_id
                or record.to_dict() != raw_record
            ):
                raise MemoryStoreError("memory store record is non-canonical")
            candidate = candidates.get(record.candidate_id)
            if (
                candidate is None
                or candidate.review_status is not MemoryReviewStatus.APPROVED
                or record.record_id
                != f"mem-{candidate.candidate_id.removeprefix('memcand-')}"
            ):
                raise MemoryStoreError(
                    "active memory record lacks an approved candidate"
                )
            expected_record = MemoryRecord(
                record_id=record.record_id,
                candidate_id=candidate.candidate_id,
                namespace=candidate.namespace,
                kind=candidate.kind,
                content=candidate.content,
                source_refs=candidate.source_refs,
                trust=candidate.trust,
                confidence=candidate.confidence,
                sensitivity=candidate.sensitivity,
                acl=candidate.acl,
                version=candidate.version,
                created_at=candidate.created_at,
                expires_at=candidate.expires_at,
                reviewed_by=candidate.reviewed_by,
                reviewed_at=candidate.reviewed_at,
                memory_key=candidate.memory_key,
                supersedes=record.supersedes,
                superseded_by=record.superseded_by,
                superseded_at=record.superseded_at,
            )
            if record != expected_record:
                raise MemoryStoreError(
                    "active memory record does not match its candidate"
                )
            records[record_id] = record

        for candidate in candidates.values():
            expected_record_id = (
                f"mem-{candidate.candidate_id.removeprefix('memcand-')}"
            )
            has_record = expected_record_id in records
            if (
                candidate.review_status is MemoryReviewStatus.APPROVED
            ) != has_record:
                raise MemoryStoreError(
                    "memory candidate and record state are inconsistent"
                )

        for record in records.values():
            for predecessor_id in record.supersedes:
                predecessor = records.get(predecessor_id)
                if (
                    predecessor is None
                    or predecessor.superseded_by != record.record_id
                    or predecessor.superseded_at != record.reviewed_at
                    or predecessor.namespace != record.namespace
                    or predecessor.kind is not record.kind
                    or (
                        predecessor.content_sha256 != record.content_sha256
                        and predecessor.memory_key != record.memory_key
                    )
                ):
                    raise MemoryStoreError(
                        "memory supersession relation is inconsistent"
                    )

        verified: set[str] = set()
        for record_id in records:
            path: set[str] = set()
            current_id = record_id
            while current_id not in verified:
                if current_id in path:
                    raise MemoryStoreError(
                        "memory supersession graph contains a cycle"
                    )
                path.add(current_id)
                successor_id = records[current_id].superseded_by
                if not successor_id:
                    break
                current_id = successor_id
            verified.update(path)
            if record.superseded_by:
                successor = records.get(record.superseded_by)
                if (
                    successor is None
                    or record.record_id not in successor.supersedes
                    or record.superseded_at != successor.reviewed_at
                ):
                    raise MemoryStoreError(
                        "memory supersession relation is inconsistent"
                    )

    def _write(self, state: dict[str, Any]) -> None:
        self._validate_state(state)
        try:
            content = json.dumps(
                state,
                ensure_ascii=False,
                allow_nan=False,
                indent=2,
                sort_keys=True,
            ) + "\n"
        except (TypeError, ValueError, OverflowError, RecursionError) as exc:
            raise MemoryStoreError("memory store is not serializable") from exc
        if len(content.encode("utf-8")) > MAX_MEMORY_STORE_BYTES:
            raise MemoryStoreError("memory store exceeds its size bound")
        atomic_write_text_beneath(
            self.workspace_root,
            MEMORY_STORE_PATH,
            content,
            default_mode=0o600,
            maximum_mode=0o600,
        )


__all__ = [
    "MAX_MEMORY_CONTENT_CHARS",
    "MAX_MEMORY_KEY_CHARS",
    "MAX_MEMORY_STORE_BYTES",
    "MEMORY_PROPOSE_SCOPE",
    "MEMORY_READ_SCOPE",
    "MEMORY_REVIEW_SCOPE",
    "MEMORY_STORE_PATH",
    "MemoryCandidate",
    "MemoryCandidateAssessment",
    "MemoryCompactionReport",
    "MemoryKind",
    "MemoryRecord",
    "MemoryReviewStatus",
    "MemoryStore",
    "MemoryStoreError",
]
