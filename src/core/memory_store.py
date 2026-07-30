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


MEMORY_STORE_SCHEMA_VERSION = 1
MEMORY_STORE_PATH = Path("runtime") / "agent_kernel" / "memory-store.json"
MAX_MEMORY_CONTENT_CHARS = 8_000
MAX_MEMORY_ITEMS = 2_000
MAX_MEMORY_STORE_BYTES = 64 * 1024 * 1024
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
        normalized = tuple(_text(value, field_name) for value in values)
    except TypeError as exc:
        raise KernelValidationError(f"{field_name} must be iterable") from exc
    if len(normalized) > max_items:
        raise KernelValidationError(f"{field_name} exceeds its bound")
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
        proposed_by = _text(self.proposed_by, "proposed_by")
        if (
            len(proposed_by) != 64
            or any(character not in "0123456789abcdef" for character in proposed_by)
        ):
            raise KernelValidationError(
                "proposed_by must be a lowercase SHA-256 digest"
            )
        object.__setattr__(self, "proposed_by", proposed_by)
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
            reviewed_by = _text(self.reviewed_by, "reviewed_by")
            if (
                len(reviewed_by) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in reviewed_by
                )
            ):
                raise KernelValidationError(
                    "reviewed_by must be a lowercase SHA-256 digest"
                )
            object.__setattr__(self, "reviewed_by", reviewed_by)
        if self.reviewed_at is not None:
            object.__setattr__(
                self,
                "reviewed_at",
                _timestamp(self.reviewed_at, "reviewed_at"),
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
                    review_reason=normalized_reason,
                    reviewed_by=reviewer.principal_digest,
                    reviewed_at=reviewed_at,
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
        candidate_key = _text(candidate_id, "candidate_id")
        raw = self._load()["candidates"].get(candidate_key)
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
            self._validate_state(payload)
        except MemoryStoreError:
            raise
        except (KernelValidationError, TypeError, ValueError) as exc:
            raise MemoryStoreError("memory store is invalid") from exc
        return payload

    @staticmethod
    def _validate_state(payload: Any) -> None:
        if (
            not isinstance(payload, dict)
            or set(payload) != {"schema_version", "candidates", "records"}
            or type(payload.get("schema_version")) is not int
            or payload.get("schema_version") != MEMORY_STORE_SCHEMA_VERSION
            or not isinstance(payload.get("candidates"), dict)
            or not isinstance(payload.get("records"), dict)
        ):
            raise MemoryStoreError("memory store is invalid")
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
    "MAX_MEMORY_STORE_BYTES",
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
