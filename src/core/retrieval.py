"""Auditable evidence envelopes returned by retrieval providers."""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import re
import time
from dataclasses import dataclass
from typing import Any, Iterable

from src.core.agent_kernel import (
    KernelValidationError,
    KnowledgeItem,
    KnowledgeKind,
    Principal,
    canonical_digest,
)


EVIDENCE_SCHEMA_VERSION = 1
MAX_EVIDENCE_ITEMS = 100
QUERY_PLAN_VERSION = 1
RERANK_VERSION = 1
MAX_QUERY_CHARS = 4_096
MAX_QUERY_UTF8_BYTES = 16_384
MAX_QUERY_COMPONENTS = 8
MAX_RERANK_CANDIDATES = 500
QUERY_TOKEN_PATTERN = re.compile(r"[\w\u4e00-\u9fff]+", re.UNICODE)


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
class QueryPlan:
    normalized_query: str
    terms: tuple[str, ...]
    components: tuple[str, ...]
    truncated: bool

    def __post_init__(self) -> None:
        normalized = _text(
            self.normalized_query,
            "normalized_query",
            max_chars=MAX_QUERY_CHARS,
        )
        if normalized != " ".join(normalized.split()):
            raise KernelValidationError(
                "normalized_query must use canonical whitespace"
            )
        try:
            if len(normalized.encode("utf-8")) > MAX_QUERY_UTF8_BYTES:
                raise KernelValidationError(
                    "normalized_query exceeds its UTF-8 byte bound"
                )
        except UnicodeEncodeError as exc:
            raise KernelValidationError(
                "normalized_query is not valid UTF-8 text"
            ) from exc
        if (
            not isinstance(self.terms, tuple)
            or len(self.terms) > MAX_QUERY_COMPONENTS
            or not all(
                isinstance(term, str)
                and QUERY_TOKEN_PATTERN.fullmatch(term)
                for term in self.terms
            )
            or len({term.casefold() for term in self.terms})
            != len(self.terms)
        ):
            raise KernelValidationError(
                "query terms are invalid, duplicated, or exceed their bound"
            )
        allowed_components = {
            normalized.casefold(),
            *(term.casefold() for term in self.terms),
        }
        if (
            not isinstance(self.components, tuple)
            or not 1 <= len(self.components) <= MAX_QUERY_COMPONENTS
            or self.components[0] != normalized
            or not all(
                isinstance(component, str)
                and component
                and len(component) <= MAX_QUERY_CHARS
                for component in self.components
            )
            or len({component.casefold() for component in self.components})
            != len(self.components)
            or any(
                component.casefold() not in allowed_components
                for component in self.components
            )
        ):
            raise KernelValidationError(
                "query components are invalid, duplicated, or exceed their bound"
            )
        if not isinstance(self.truncated, bool):
            raise KernelValidationError("truncated must be a boolean")
        object.__setattr__(self, "normalized_query", normalized)

    @property
    def digest(self) -> str:
        payload = {
            "version": QUERY_PLAN_VERSION,
            "normalized_query": self.normalized_query,
            "terms": list(self.terms),
            "components": list(self.components),
            "truncated": self.truncated,
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": QUERY_PLAN_VERSION,
            "terms": list(self.terms),
            "components": list(self.components),
            "truncated": self.truncated,
            "digest": self.digest,
        }


def build_query_plan(query: Any) -> QueryPlan:
    if not isinstance(query, str):
        raise TypeError("query must be a string")
    normalized = " ".join(query.strip().split())
    if not normalized:
        raise ValueError("query is required")
    if len(normalized) > MAX_QUERY_CHARS:
        raise ValueError(f"query exceeds {MAX_QUERY_CHARS} characters")
    if any(
        (ord(character) < 32 and not character.isspace())
        or ord(character) == 127
        for character in normalized
    ):
        raise ValueError("query contains control characters")
    try:
        encoded = normalized.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("query is not valid UTF-8 text") from exc
    if len(encoded) > MAX_QUERY_UTF8_BYTES:
        raise ValueError(f"query exceeds {MAX_QUERY_UTF8_BYTES} UTF-8 bytes")

    all_terms = QUERY_TOKEN_PATTERN.findall(normalized)
    terms: list[str] = []
    seen: set[str] = set()
    for term in all_terms:
        folded = term.casefold()
        if folded in seen:
            continue
        seen.add(folded)
        terms.append(term)
    selected_terms = terms[:MAX_QUERY_COMPONENTS]
    components: list[str] = [normalized]
    component_keys = {normalized.casefold()}
    for term in selected_terms:
        key = term.casefold()
        if key in component_keys:
            continue
        components.append(term)
        component_keys.add(key)
        if len(components) >= MAX_QUERY_COMPONENTS:
            break
    return QueryPlan(
        normalized_query=normalized,
        terms=tuple(selected_terms),
        components=tuple(components),
        truncated=(
            len(terms) > len(selected_terms)
            or len(components) < 1 + sum(
                1
                for term in selected_terms
                if term.casefold() != normalized.casefold()
            )
        ),
    )


def _finite_nonnegative(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, number) if math.isfinite(number) else 0.0


def deterministic_rerank(
    matches: list[dict[str, Any]],
    query_plan: QueryPlan,
    *,
    mode: str,
    limit: int,
) -> list[dict[str, Any]]:
    if (
        not isinstance(matches, list)
        or len(matches) > MAX_RERANK_CANDIDATES
        or not all(isinstance(match, dict) for match in matches)
    ):
        raise KernelValidationError(
            "rerank candidates must be a bounded list of objects"
        )
    if mode not in {"keyword", "semantic", "hybrid"}:
        raise KernelValidationError("unsupported rerank mode")
    if (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or not 1 <= limit <= MAX_RERANK_CANDIDATES
    ):
        raise KernelValidationError(
            "rerank limit must be a bounded positive integer"
        )
    if not matches:
        return []
    finite_scores = [
        _finite_nonnegative(match.get("score", 0.0))
        for match in matches
    ]
    maximum_score = max(finite_scores, default=0.0)
    query_folded = query_plan.normalized_query.casefold()
    ranked: list[dict[str, Any]] = []
    for match in matches:
        raw_score = _finite_nonnegative(match.get("score", 0.0))
        source_score = (
            raw_score / maximum_score
            if maximum_score > 0.0
            else 0.0
        )
        path = str(match.get("path", ""))
        snippet = str(match.get("snippet", ""))
        path_folded = path.casefold()
        searchable = f"{path}\n{snippet}".casefold()
        matched_terms = [
            term
            for term in query_plan.terms
            if term.casefold() in searchable
        ]
        term_coverage = (
            len(matched_terms) / len(query_plan.terms)
            if query_plan.terms
            else 0.0
        )
        exact_phrase = 1.0 if query_folded in searchable else 0.0
        path_match = (
            1.0
            if query_folded in path_folded
            or any(term.casefold() in path_folded for term in query_plan.terms)
            else 0.0
        )
        raw_signals = match.get("signals")
        signals = dict(raw_signals) if isinstance(raw_signals, dict) else {}
        keyword_score = max(
            _finite_nonnegative(signals.get("keyword", 0.0)),
            _finite_nonnegative(signals.get("path", 0.0)),
        )
        semantic_score = _finite_nonnegative(
            signals.get("semantic", 0.0)
        )
        if mode == "semantic":
            rerank_score = (
                0.70 * source_score
                + 0.20 * term_coverage
                + 0.05 * path_match
                + 0.05 * exact_phrase
            )
        elif mode == "hybrid":
            rerank_score = (
                0.55 * source_score
                + 0.10 * max(0.0, min(1.0, keyword_score))
                + 0.10 * max(0.0, min(1.0, semantic_score))
                + 0.15 * term_coverage
                + 0.05 * path_match
                + 0.05 * exact_phrase
            )
        else:
            rerank_score = (
                0.45 * source_score
                + 0.25 * term_coverage
                + 0.20 * path_match
                + 0.10 * exact_phrase
            )
        reranked = dict(match)
        reranked["score"] = round(rerank_score, 12)
        reranked["ranking_signals"] = {
            "version": RERANK_VERSION,
            "source_score": round(source_score, 12),
            "keyword_score": round(
                max(0.0, min(1.0, keyword_score)),
                12,
            ),
            "semantic_score": round(
                max(0.0, min(1.0, semantic_score)),
                12,
            ),
            "term_coverage": round(term_coverage, 12),
            "exact_phrase": bool(exact_phrase),
            "path_match": bool(path_match),
            "matched_terms": matched_terms,
        }
        ranked.append(reranked)
    return sorted(
        ranked,
        key=lambda item: (
            -float(item["score"]),
            item["path"],
            item.get("line") or 0,
        ),
    )[:limit]


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
        if self.knowledge.kind is not KnowledgeKind.RETRIEVAL:
            raise KernelValidationError(
                "retrieval evidence must reference retrieval knowledge"
            )
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
        try:
            selected_items = tuple(
                itertools.islice(
                    iter(self.items),
                    MAX_EVIDENCE_ITEMS + 1,
                )
            )
        except TypeError as exc:
            raise KernelValidationError(
                "EvidenceBundle items must be iterable"
            ) from exc
        if len(selected_items) > MAX_EVIDENCE_ITEMS:
            raise KernelValidationError("EvidenceBundle exceeds its item bound")
        object.__setattr__(self, "items", selected_items)
        if not all(isinstance(item, EvidenceItem) for item in self.items):
            raise KernelValidationError("EvidenceBundle contains an invalid item")
        evidence_ids = [item.evidence_id for item in self.items]
        if len(evidence_ids) != len(set(evidence_ids)):
            raise KernelValidationError(
                "EvidenceBundle contains duplicate evidence ids"
            )
        if any(item.index_version != self.index_version for item in self.items):
            raise KernelValidationError(
                "EvidenceBundle mixes index versions"
            )
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
        query = _text(query, "query", max_chars=4_096)
        try:
            selected = tuple(
                itertools.islice(
                    iter(items),
                    MAX_EVIDENCE_ITEMS + 1,
                )
            )
        except TypeError as exc:
            raise KernelValidationError(
                "items must be iterable"
            ) from exc
        if len(selected) > MAX_EVIDENCE_ITEMS:
            raise KernelValidationError(
                "EvidenceBundle exceeds its item bound"
            )
        if not all(isinstance(item, EvidenceItem) for item in selected):
            raise KernelValidationError(
                "EvidenceBundle contains an invalid item"
            )
        for item in selected:
            if not item.knowledge.is_authorized(principal, now=now):
                raise PermissionError("evidence item is not authorized for the caller")
        query_sha256 = hashlib.sha256(query.encode("utf-8")).hexdigest()
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
    "MAX_QUERY_CHARS",
    "MAX_QUERY_COMPONENTS",
    "MAX_QUERY_UTF8_BYTES",
    "MAX_RERANK_CANDIDATES",
    "MAX_EVIDENCE_ITEMS",
    "QUERY_PLAN_VERSION",
    "QUERY_TOKEN_PATTERN",
    "QueryPlan",
    "RERANK_VERSION",
    "build_query_plan",
    "deterministic_rerank",
]
