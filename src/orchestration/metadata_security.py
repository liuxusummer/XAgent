"""Shared fail-closed checks for durable metadata keys."""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from typing import Any

_SENSITIVE_KEY_MARKERS = (
    "apikey",
    "authorization",
    "bearer",
    "cookie",
    "credential",
    "password",
    "passwd",
    "privatekey",
    "secret",
    "token",
)
_ARTIFACT_REF_FIELDS = frozenset(
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


def canonical_metadata_key(key: str) -> str:
    """Canonicalize a metadata key for security-sensitive comparisons."""

    if not isinstance(key, str):
        return ""
    return "".join(
        character
        for character in unicodedata.normalize("NFKC", key).casefold()
        if character.isalnum()
    )


def is_sensitive_metadata_key(
    key: str,
    *,
    additional_keys: frozenset[str] = frozenset(),
) -> bool:
    """Return whether a key has a built-in or explicitly sensitive shape."""

    canonical_key = canonical_metadata_key(key)
    return any(marker in canonical_key for marker in _SENSITIVE_KEY_MARKERS) or (
        canonical_key in additional_keys
    )


def contains_sensitive_key(value: Any) -> bool:
    """Return whether a nested JSON-like value has a credential-shaped key."""

    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, Mapping):
            for key, child in item.items():
                if isinstance(key, str) and is_sensitive_metadata_key(key):
                    return True
                stack.append(child)
        elif isinstance(item, (list, tuple)):
            stack.extend(item)
    return False


def contains_sensitive_artifact_metadata(value: Any) -> bool:
    """Detect complete ArtifactRef dictionaries carrying sensitive metadata."""

    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, Mapping):
            if set(item) == set(_ARTIFACT_REF_FIELDS):
                metadata = item.get("metadata")
                if isinstance(metadata, Mapping) and contains_sensitive_key(
                    metadata
                ):
                    return True
            stack.extend(item.values())
        elif isinstance(item, (list, tuple)):
            stack.extend(item)
    return False


__all__ = [
    "canonical_metadata_key",
    "contains_sensitive_artifact_metadata",
    "contains_sensitive_key",
    "is_sensitive_metadata_key",
]
