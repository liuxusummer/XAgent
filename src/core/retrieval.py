"""Auditable evidence envelopes returned by retrieval providers."""

from __future__ import annotations

import hashlib
import math
import time
from dataclasses import dataclass
from typing import Any, Iterable

from src.core.agent_kernel import (
    KernelValidationError,
    KnowledgeItem,
    Principal,
    canonical_digest,
)


EVIDENCE_SCHEMA_VERSION = 1
MAX_EVIDENCE_ITEMS = 100


def _text(value: Any, field_name: str, *, max_chars: int = 1_024) -> str:
    if not isinstance(value, str):
        raise KernelValidationError(f"{field_name} must be a string")
    result = value.strip()
    if not result or len(result) > max_chars:
        raise KernelValidationError(f"{field_name} is empty or exceeds its bound")
    if any(ord(character) < 32 or ord(character) == 127 for character in result):
        raise KernelValidationError(f"{field_name} contains control characters")
    return result


def _digest(value: Any, field_name: str) -> str:
    result = str(value or "")
    if (
        len(result) != 64
        or result != result.lower()
        or any(character not in "0123456789abcdef" for character in result)
    ):
        raise KernelValidationError(f"{field_name} must be a SHA-256 digest")
    return result


@dataclass(frozen=True, slots=True)
class EvidenceItem:
    evidence_id: str
    knowledge: KnowledgeItem
    path: str
    start_line: int | None
    end_line: int | None
    snippet_sha256: str
    index_version: str
    schema_version: int = EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "evidence_id",
            _text(self.evidence_id, "evidence_id", max_chars=256),
        )
        if not isinstance(self.knowledge, KnowledgeItem):
            raise KernelValidationError("knowledge must be a KnowledgeItem")
        object.__setattr__(self, "path", _text(self.path, "path"))
        for field_name in ("start_line", "end_line"):
            value = getattr(self, field_name)
            if value is not None and (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 1
            ):
                raise KernelValidationError(f"{field_name} must be a positive integer")
        if (
            self.start_line is not None
            and self.end_line is not None
            and self.end_line < self.start_line
        ):
            raise KernelValidationError("end_line must not precede start_line")
        object.__setattr__(
            self,
            "snippet_sha256",
            _digest(self.snippet_sha256, "snippet_sha256"),
        )
        object.__setattr__(
            self,
            "index_version",
            _text(self.index_version, "index_version", max_chars=256),
        )
        if self.schema_version != EVIDENCE_SCHEMA_VERSION:
            raise KernelValidationError("unsupported EvidenceItem schema_version")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "evidence_id": self.evidence_id,
            "knowledge": self.knowledge.to_dict(),
            "path": self.path,
            "start_line": self.start_line,
            "end_line": self.end_line,
            "snippet_sha256": self.snippet_sha256,
            "index_version": self.index_version,
        }


@dataclass(frozen=True, slots=True)
class EvidenceBundle:
    bundle_id: str
    query_sha256: str
    principal_digest: str
    index_version: str
    items: tuple[EvidenceItem, ...]
    generated_at: float
    schema_version: int = EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "bundle_id",
            _text(self.bundle_id, "bundle_id", max_chars=256),
        )
        object.__setattr__(
            self,
            "query_sha256",
            _digest(self.query_sha256, "query_sha256"),
        )
        object.__setattr__(
            self,
            "principal_digest",
            _digest(self.principal_digest, "principal_digest"),
        )
        object.__setattr__(
            self,
            "index_version",
            _text(self.index_version, "index_version", max_chars=256),
        )
        object.__setattr__(self, "items", tuple(self.items))
        if len(self.items) > MAX_EVIDENCE_ITEMS:
            raise KernelValidationError("EvidenceBundle exceeds its item bound")
        if not all(isinstance(item, EvidenceItem) for item in self.items):
            raise KernelValidationError("EvidenceBundle contains an invalid item")
        generated_at = float(self.generated_at)
        if not math.isfinite(generated_at) or generated_at < 0:
            raise KernelValidationError("generated_at must be a finite timestamp")
        object.__setattr__(self, "generated_at", generated_at)
        if self.schema_version != EVIDENCE_SCHEMA_VERSION:
            raise KernelValidationError("unsupported EvidenceBundle schema_version")

    @classmethod
    def build(
        cls,
        *,
        query: str,
        principal: Principal,
        index_version: str,
        items: Iterable[EvidenceItem],
        now: float | None = None,
    ) -> "EvidenceBundle":
        if not isinstance(principal, Principal):
            raise KernelValidationError("principal is required")
        selected = tuple(items)
        for item in selected:
            if not item.knowledge.is_authorized(principal, now=now):
                raise PermissionError("evidence item is not authorized for the caller")
        query_sha256 = hashlib.sha256(str(query).encode("utf-8")).hexdigest()
        payload = {
            "query_sha256": query_sha256,
            "principal_digest": principal.principal_digest,
            "index_version": index_version,
            "evidence_ids": [item.evidence_id for item in selected],
        }
        return cls(
            bundle_id=f"evidence-{canonical_digest(payload, 'EvidenceBundle')[:32]}",
            query_sha256=query_sha256,
            principal_digest=principal.principal_digest,
            index_version=index_version,
            items=selected,
            generated_at=time.time() if now is None else now,
        )

    @property
    def bundle_digest(self) -> str:
        return canonical_digest(self.to_dict(), "EvidenceBundle")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "bundle_id": self.bundle_id,
            "query_sha256": self.query_sha256,
            "principal_digest": self.principal_digest,
            "index_version": self.index_version,
            "items": [item.to_dict() for item in self.items],
            "generated_at": self.generated_at,
        }


__all__ = [
    "EVIDENCE_SCHEMA_VERSION",
    "EvidenceBundle",
    "EvidenceItem",
    "MAX_EVIDENCE_ITEMS",
]
