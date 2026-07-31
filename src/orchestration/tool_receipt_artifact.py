"""Canonical immutable Artifact envelope for one durable ToolReceipt."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .artifacts import (
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    ArtifactStore,
    canonical_json_bytes,
)
from .executor import ToolReceipt, ToolReceiptError


TOOL_RECEIPT_ARTIFACT_MEDIA_TYPE = (
    "application/vnd.xagent.tool-receipt+json"
)
MAX_TOOL_RECEIPT_ARTIFACT_BYTES = 256 * 1024
_SCHEMA_METADATA = {"schema": "tool_receipt_v1"}
_ERROR_REASONS = frozenset(
    {
        "tool_receipt_artifact_invalid",
        "tool_receipt_artifact_unavailable",
    }
)


class ToolReceiptArtifactError(RuntimeError, ValueError):
    """A ToolReceipt Artifact is malformed, unavailable, or mismatched."""

    def __init__(self, reason_code: str) -> None:
        if reason_code not in _ERROR_REASONS:
            raise ValueError("invalid ToolReceipt Artifact reason code")
        self.reason_code = reason_code
        super().__init__(reason_code)


class ToolReceiptArtifactStore:
    """Stage and load exact payload-free ToolReceipt evidence."""

    def __init__(self, store: ArtifactStore) -> None:
        if not isinstance(store, ArtifactStore):
            raise ToolReceiptArtifactError(
                "tool_receipt_artifact_invalid"
            )
        self._store = store

    def stage(
        self,
        receipt: ToolReceipt,
        *,
        sensitivity: ArtifactSensitivity | str,
    ) -> ArtifactRef:
        if type(receipt) is not ToolReceipt:
            raise ToolReceiptArtifactError(
                "tool_receipt_artifact_invalid"
            )
        try:
            resolved_sensitivity = ArtifactSensitivity(sensitivity)
        except (TypeError, ValueError):
            raise ToolReceiptArtifactError(
                "tool_receipt_artifact_invalid"
            ) from None
        if resolved_sensitivity not in {
            ArtifactSensitivity.SENSITIVE,
            ArtifactSensitivity.SECRET,
        }:
            raise ToolReceiptArtifactError(
                "tool_receipt_artifact_invalid"
            )
        content = canonical_json_bytes(receipt.to_dict())
        if not content or len(content) > MAX_TOOL_RECEIPT_ARTIFACT_BYTES:
            raise ToolReceiptArtifactError(
                "tool_receipt_artifact_invalid"
            )
        try:
            ref = self._store.put_bytes(
                content,
                media_type=TOOL_RECEIPT_ARTIFACT_MEDIA_TYPE,
                kind=ArtifactKind.TOOL_RECEIPT,
                sensitivity=resolved_sensitivity,
                producer_run_id=receipt.run_id,
                producer_node_id=receipt.node_id,
                producer_attempt_id=receipt.attempt_id,
                metadata=_SCHEMA_METADATA,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise ToolReceiptArtifactError(
                "tool_receipt_artifact_unavailable"
            ) from None
        self.validate_ref(ref, receipt=receipt)
        try:
            verified = self._store.verify(ref)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise ToolReceiptArtifactError(
                "tool_receipt_artifact_unavailable"
            ) from None
        if verified is not True:
            raise ToolReceiptArtifactError(
                "tool_receipt_artifact_unavailable"
            )
        return ref

    def load(self, ref: ArtifactRef) -> ToolReceipt:
        self.validate_ref(ref)
        try:
            if self._store.verify(ref) is not True:
                raise ValueError
            content = self._store.read(ref)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise ToolReceiptArtifactError(
                "tool_receipt_artifact_unavailable"
            ) from None
        try:
            if (
                type(content) is not bytes
                or len(content) != ref.size
                or hashlib.sha256(content).hexdigest() != ref.sha256
            ):
                raise ValueError
            payload = json.loads(content.decode("utf-8"))
            if (
                not isinstance(payload, dict)
                or canonical_json_bytes(payload) != content
            ):
                raise ValueError
            receipt = ToolReceipt.from_dict(payload)
        except (ToolReceiptError, TypeError, ValueError, UnicodeError):
            raise ToolReceiptArtifactError(
                "tool_receipt_artifact_invalid"
            ) from None
        self.validate_ref(ref, receipt=receipt)
        return receipt

    @staticmethod
    def validate_ref(
        ref: ArtifactRef,
        *,
        receipt: ToolReceipt | None = None,
    ) -> None:
        if (
            type(ref) is not ArtifactRef
            or ref.kind is not ArtifactKind.TOOL_RECEIPT
            or ref.media_type != TOOL_RECEIPT_ARTIFACT_MEDIA_TYPE
            or ref.sensitivity
            not in {
                ArtifactSensitivity.SENSITIVE,
                ArtifactSensitivity.SECRET,
            }
            or not 0 < ref.size <= MAX_TOOL_RECEIPT_ARTIFACT_BYTES
            or dict(ref.metadata) != _SCHEMA_METADATA
        ):
            raise ToolReceiptArtifactError(
                "tool_receipt_artifact_invalid"
            )
        if receipt is not None:
            if type(receipt) is not ToolReceipt:
                raise ToolReceiptArtifactError(
                    "tool_receipt_artifact_invalid"
                )
            content = canonical_json_bytes(receipt.to_dict())
            if (
                ref.sha256 != receipt.receipt_digest
                or ref.sha256 != hashlib.sha256(content).hexdigest()
                or ref.size != len(content)
                or ref.producer_run_id != receipt.run_id
                or ref.producer_node_id != receipt.node_id
                or ref.producer_attempt_id != receipt.attempt_id
            ):
                raise ToolReceiptArtifactError(
                    "tool_receipt_artifact_invalid"
                )


__all__ = [
    "MAX_TOOL_RECEIPT_ARTIFACT_BYTES",
    "TOOL_RECEIPT_ARTIFACT_MEDIA_TYPE",
    "ToolReceiptArtifactError",
    "ToolReceiptArtifactStore",
]
