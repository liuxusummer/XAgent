"""Shared trust, identity, knowledge, and context contracts for the agent kernel.

These models deliberately contain only bounded metadata.  Raw prompts, tool
arguments, credentials, and artifact contents belong in their existing stores
and are referenced here by stable identifiers and SHA-256 digests.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any, Iterable, Mapping


KERNEL_SCHEMA_VERSION = 1
MAX_TEXT_CHARS = 256
MAX_NAMESPACE_PARTS = 16
MAX_COLLECTION_ITEMS = 256
MAX_METADATA_BYTES = 64 * 1024
MAX_TOKEN_BUDGET = 10_000_000


class KernelValidationError(ValueError):
    """Kernel metadata is malformed, unbounded, or internally inconsistent."""


class TrustLevel(StrEnum):
    SYSTEM = "system"
    USER = "user"
    WORKSPACE = "workspace"
    RETRIEVED = "retrieved"
    AGENT_DERIVED = "agent_derived"
    TOOL_UNTRUSTED = "tool_untrusted"


class DataSensitivity(StrEnum):
    PUBLIC = "public"
    INTERNAL = "internal"
    SENSITIVE = "sensitive"
    SECRET = "secret"


class KnowledgeKind(StrEnum):
    SOURCE = "source"
    RETRIEVAL = "retrieval"
    MEMORY = "memory"
    ARTIFACT = "artifact"
    CHECKPOINT = "checkpoint"


class ContextKind(StrEnum):
    SYSTEM = "system"
    TASK_STATE = "task_state"
    RECENT_HISTORY = "recent_history"
    COMPACTED_HISTORY = "compacted_history"
    RETRIEVAL_EVIDENCE = "retrieval_evidence"
    MEMORY = "memory"
    TOOL_RESULT = "tool_result"
    SKILL = "skill"


def _bounded_text(
    value: Any,
    field_name: str,
    *,
    max_chars: int = MAX_TEXT_CHARS,
) -> str:
    if not isinstance(value, str):
        raise KernelValidationError(f"{field_name} must be a string")
    text = value.strip()
    if not text:
        raise KernelValidationError(f"{field_name} must not be empty")
    if len(text) > max_chars:
        raise KernelValidationError(f"{field_name} exceeds the metadata bound")
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise KernelValidationError(f"{field_name} contains control characters")
    return text


def _digest(value: Any, field_name: str) -> str:
    digest = str(value or "")
    if (
        len(digest) != 64
        or digest != digest.lower()
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise KernelValidationError(
            f"{field_name} must be a lowercase SHA-256 digest"
        )
    return digest


def _timestamp(value: Any, field_name: str) -> float:
    try:
        timestamp = float(value)
    except (TypeError, ValueError) as exc:
        raise KernelValidationError(f"{field_name} must be a finite timestamp") from exc
    if not math.isfinite(timestamp) or timestamp < 0:
        raise KernelValidationError(f"{field_name} must be a finite timestamp")
    return timestamp


def _bounded_strings(
    values: Iterable[str],
    field_name: str,
    *,
    max_items: int = MAX_COLLECTION_ITEMS,
) -> tuple[str, ...]:
    try:
        normalized = {
            _bounded_text(value, field_name)
            for value in values
        }
    except TypeError as exc:
        raise KernelValidationError(f"{field_name} must be iterable") from exc
    if len(normalized) > max_items:
        raise KernelValidationError(f"{field_name} exceeds the collection bound")
    return tuple(sorted(normalized))


def _bounded_sequence(
    values: Iterable[str],
    field_name: str,
    *,
    max_items: int,
) -> tuple[str, ...]:
    try:
        normalized = tuple(_bounded_text(value, field_name) for value in values)
    except TypeError as exc:
        raise KernelValidationError(f"{field_name} must be iterable") from exc
    if len(normalized) > max_items:
        raise KernelValidationError(f"{field_name} exceeds the collection bound")
    if len(set(normalized)) != len(normalized):
        raise KernelValidationError(f"{field_name} values must be unique")
    return normalized


def _canonical_json(value: Any, field_name: str) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise KernelValidationError(f"{field_name} must be canonical JSON") from exc
    if len(encoded) > MAX_METADATA_BYTES:
        raise KernelValidationError(f"{field_name} exceeds the metadata bound")
    return encoded


def canonical_digest(value: Mapping[str, Any], field_name: str) -> str:
    return hashlib.sha256(_canonical_json(dict(value), field_name)).hexdigest()


def namespace_tenant_id(namespace: tuple[str, ...]) -> str:
    """Return the tenant bound by a canonical knowledge namespace."""

    if not namespace:
        raise KernelValidationError("namespace must not be empty")
    if namespace[0] == "tenant":
        if len(namespace) < 2:
            raise KernelValidationError(
                "tenant-prefixed namespace must include a tenant id"
            )
        return namespace[1]
    return namespace[0]


@dataclass(frozen=True, slots=True)
class Principal:
    """Authenticated application identity propagated through one agent run."""

    subject: str
    tenant_id: str
    session_id: str
    run_id: str
    agent_id: str = "main"
    scopes: tuple[str, ...] = ()
    schema_version: int = KERNEL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in (
            "subject",
            "tenant_id",
            "session_id",
            "run_id",
            "agent_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(getattr(self, field_name), field_name),
            )
        object.__setattr__(
            self,
            "scopes",
            _bounded_strings(self.scopes, "scope", max_items=64),
        )
        if self.schema_version != KERNEL_SCHEMA_VERSION:
            raise KernelValidationError("unsupported Principal schema_version")

    @classmethod
    def local(
        cls,
        *,
        session_id: str,
        run_id: str | None = None,
        agent_id: str = "main",
    ) -> "Principal":
        return cls(
            subject="local-user",
            tenant_id="local",
            session_id=session_id,
            run_id=run_id or session_id,
            agent_id=agent_id,
            scopes=("workspace.read",),
        )

    @property
    def principal_digest(self) -> str:
        return canonical_digest(self.to_dict(), "Principal")

    @property
    def boundary_digest(self) -> str:
        """Stable authorization boundary across sessions and resumed runs."""

        return canonical_digest(
            {
                "schema_version": self.schema_version,
                "subject": self.subject,
                "tenant_id": self.tenant_id,
                "agent_id": self.agent_id,
                "scopes": list(self.scopes),
            },
            "Principal boundary",
        )

    def bind_run(
        self,
        *,
        session_id: str,
        run_id: str | None = None,
        agent_id: str | None = None,
    ) -> "Principal":
        """Return the same authenticated subject bound to a new local run."""

        return replace(
            self,
            session_id=session_id,
            run_id=run_id or session_id,
            agent_id=self.agent_id if agent_id is None else agent_id,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "subject": self.subject,
            "tenant_id": self.tenant_id,
            "session_id": self.session_id,
            "run_id": self.run_id,
            "agent_id": self.agent_id,
            "scopes": list(self.scopes),
        }


@dataclass(frozen=True, slots=True)
class KnowledgeItem:
    """A provenance and access envelope shared by retrieval and memory."""

    item_id: str
    kind: KnowledgeKind
    namespace: tuple[str, ...]
    content_sha256: str
    source_refs: tuple[str, ...]
    trust: TrustLevel
    sensitivity: DataSensitivity
    acl: tuple[str, ...]
    version: int = 1
    created_at: float = 0.0
    valid_until: float | None = None
    schema_version: int = KERNEL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "item_id", _bounded_text(self.item_id, "item_id"))
        try:
            object.__setattr__(self, "kind", KnowledgeKind(self.kind))
            object.__setattr__(self, "trust", TrustLevel(self.trust))
            object.__setattr__(
                self,
                "sensitivity",
                DataSensitivity(self.sensitivity),
            )
        except ValueError as exc:
            raise KernelValidationError("invalid KnowledgeItem enum value") from exc
        object.__setattr__(
            self,
            "namespace",
            _bounded_sequence(
                self.namespace,
                "namespace",
                max_items=MAX_NAMESPACE_PARTS,
            ),
        )
        if not self.namespace:
            raise KernelValidationError("namespace must not be empty")
        namespace_tenant_id(self.namespace)
        object.__setattr__(
            self,
            "content_sha256",
            _digest(self.content_sha256, "content_sha256"),
        )
        object.__setattr__(
            self,
            "source_refs",
            _bounded_strings(self.source_refs, "source_ref"),
        )
        if not self.source_refs:
            raise KernelValidationError("source_refs must not be empty")
        object.__setattr__(self, "acl", _bounded_strings(self.acl, "acl"))
        if not self.acl:
            raise KernelValidationError("acl must be explicit")
        if not isinstance(self.version, int) or isinstance(self.version, bool) or self.version < 1:
            raise KernelValidationError("version must be a positive integer")
        created_at = time.time() if self.created_at == 0.0 else self.created_at
        object.__setattr__(self, "created_at", _timestamp(created_at, "created_at"))
        if self.valid_until is not None:
            valid_until = _timestamp(self.valid_until, "valid_until")
            if valid_until <= self.created_at:
                raise KernelValidationError("valid_until must follow created_at")
            object.__setattr__(self, "valid_until", valid_until)
        if self.schema_version != KERNEL_SCHEMA_VERSION:
            raise KernelValidationError("unsupported KnowledgeItem schema_version")

    def is_active(self, *, now: float | None = None) -> bool:
        current = time.time() if now is None else _timestamp(now, "now")
        return self.valid_until is None or current < self.valid_until

    def is_authorized(self, principal: Principal, *, now: float | None = None) -> bool:
        if not isinstance(principal, Principal) or not self.is_active(now=now):
            return False
        if namespace_tenant_id(self.namespace) != principal.tenant_id:
            return False
        grants = {
            "*",
            f"subject:{principal.subject}",
            f"tenant:{principal.tenant_id}",
            f"agent:{principal.agent_id}",
            *(f"scope:{scope}" for scope in principal.scopes),
        }
        return bool(set(self.acl).intersection(grants))

    @property
    def envelope_digest(self) -> str:
        return canonical_digest(self.to_dict(), "KnowledgeItem")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "item_id": self.item_id,
            "kind": self.kind.value,
            "namespace": list(self.namespace),
            "content_sha256": self.content_sha256,
            "source_refs": list(self.source_refs),
            "trust": self.trust.value,
            "sensitivity": self.sensitivity.value,
            "acl": list(self.acl),
            "version": self.version,
            "created_at": self.created_at,
            "valid_until": self.valid_until,
        }


@dataclass(frozen=True, slots=True)
class ContextItem:
    """One token-accounted reference selected for an LLM context."""

    ref_id: str
    kind: ContextKind
    token_count: int
    priority: int
    trust: TrustLevel
    source_sha256: str
    llm_visible: bool = True
    schema_version: int = KERNEL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "ref_id", _bounded_text(self.ref_id, "ref_id"))
        try:
            object.__setattr__(self, "kind", ContextKind(self.kind))
            object.__setattr__(self, "trust", TrustLevel(self.trust))
        except ValueError as exc:
            raise KernelValidationError("invalid ContextItem enum value") from exc
        if (
            not isinstance(self.token_count, int)
            or isinstance(self.token_count, bool)
            or self.token_count < 0
            or self.token_count > MAX_TOKEN_BUDGET
        ):
            raise KernelValidationError("token_count is outside the supported bound")
        if (
            not isinstance(self.priority, int)
            or isinstance(self.priority, bool)
            or not 0 <= self.priority <= 100
        ):
            raise KernelValidationError("priority must be an integer from 0 to 100")
        object.__setattr__(
            self,
            "source_sha256",
            _digest(self.source_sha256, "source_sha256"),
        )
        if not isinstance(self.llm_visible, bool):
            raise KernelValidationError("llm_visible must be boolean")
        if self.schema_version != KERNEL_SCHEMA_VERSION:
            raise KernelValidationError("unsupported ContextItem schema_version")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "ref_id": self.ref_id,
            "kind": self.kind.value,
            "token_count": self.token_count,
            "priority": self.priority,
            "trust": self.trust.value,
            "source_sha256": self.source_sha256,
            "llm_visible": self.llm_visible,
        }


@dataclass(frozen=True, slots=True)
class ContextManifest:
    """Auditable, token-bounded selection of local references exposed to an LLM."""

    manifest_id: str
    principal_digest: str
    max_input_tokens: int
    reserved_output_tokens: int
    items: tuple[ContextItem, ...] = ()
    schema_version: int = KERNEL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "manifest_id",
            _bounded_text(self.manifest_id, "manifest_id"),
        )
        object.__setattr__(
            self,
            "principal_digest",
            _digest(self.principal_digest, "principal_digest"),
        )
        for field_name in ("max_input_tokens", "reserved_output_tokens"):
            value = getattr(self, field_name)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
                or value > MAX_TOKEN_BUDGET
            ):
                raise KernelValidationError(
                    f"{field_name} is outside the supported bound"
                )
        if self.max_input_tokens <= 0:
            raise KernelValidationError("max_input_tokens must be positive")
        if self.reserved_output_tokens >= self.max_input_tokens:
            raise KernelValidationError(
                "reserved_output_tokens must be smaller than max_input_tokens"
            )
        try:
            items = tuple(self.items)
        except TypeError as exc:
            raise KernelValidationError("items must be iterable") from exc
        if len(items) > MAX_COLLECTION_ITEMS:
            raise KernelValidationError("items exceeds the collection bound")
        if not all(isinstance(item, ContextItem) for item in items):
            raise KernelValidationError("items must contain ContextItem values")
        if len({item.ref_id for item in items}) != len(items):
            raise KernelValidationError("ContextItem ref_id values must be unique")
        object.__setattr__(self, "items", items)
        if self.visible_token_count > self.available_input_tokens:
            raise KernelValidationError("visible context exceeds its token budget")
        if self.schema_version != KERNEL_SCHEMA_VERSION:
            raise KernelValidationError("unsupported ContextManifest schema_version")

    @property
    def available_input_tokens(self) -> int:
        return self.max_input_tokens - self.reserved_output_tokens

    @property
    def visible_token_count(self) -> int:
        return sum(item.token_count for item in self.items if item.llm_visible)

    @property
    def manifest_digest(self) -> str:
        return canonical_digest(self.to_dict(), "ContextManifest")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "principal_digest": self.principal_digest,
            "max_input_tokens": self.max_input_tokens,
            "reserved_output_tokens": self.reserved_output_tokens,
            "items": [item.to_dict() for item in self.items],
        }


__all__ = [
    "ContextItem",
    "ContextKind",
    "ContextManifest",
    "DataSensitivity",
    "KERNEL_SCHEMA_VERSION",
    "KernelValidationError",
    "KnowledgeItem",
    "KnowledgeKind",
    "Principal",
    "TrustLevel",
    "canonical_digest",
]
