"""Trusted, artifact-backed resolution for durable unknown tool outcomes."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .artifacts import ArtifactRef

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,254}$")


class RecoveryDecisionError(ValueError):
    """An operator recovery decision is malformed or under-evidenced."""


class UnknownOutcomeResolution(StrEnum):
    CONFIRMED_SUCCEEDED = "confirmed_succeeded"
    CONFIRMED_FAILED = "confirmed_failed"


def _identifier(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not _SAFE_ID.fullmatch(value):
        raise RecoveryDecisionError(f"{field_name} is invalid")
    return value


def _canonical_ref(value: ArtifactRef, field_name: str) -> ArtifactRef:
    if not isinstance(value, ArtifactRef):
        raise RecoveryDecisionError(f"{field_name} must be an ArtifactRef")
    if value.metadata:
        raise RecoveryDecisionError(f"{field_name} metadata must be empty")
    try:
        detached = ArtifactRef.from_dict(value.to_dict())
    except (TypeError, ValueError) as exc:
        raise RecoveryDecisionError(f"{field_name} is invalid") from exc
    if detached.to_dict() != value.to_dict():
        raise RecoveryDecisionError(f"{field_name} must be canonical")
    return detached


@dataclass(frozen=True, slots=True)
class UnknownOutcomeDecision:
    """A trusted operator conclusion backed by immutable evidence Artifacts."""

    resolution_id: str
    run_id: str
    node_id: str
    attempt_id: str
    resolution: UnknownOutcomeResolution
    evidence_ref: ArtifactRef
    result_ref: ArtifactRef | None = None

    def __post_init__(self) -> None:
        for field_name in ("resolution_id", "run_id", "node_id", "attempt_id"):
            object.__setattr__(
                self,
                field_name,
                _identifier(getattr(self, field_name), field_name),
            )
        try:
            object.__setattr__(
                self,
                "resolution",
                UnknownOutcomeResolution(self.resolution),
            )
        except ValueError as exc:
            raise RecoveryDecisionError("resolution is invalid") from exc
        object.__setattr__(
            self,
            "evidence_ref",
            _canonical_ref(self.evidence_ref, "evidence_ref"),
        )
        if self.result_ref is not None:
            object.__setattr__(
                self,
                "result_ref",
                _canonical_ref(self.result_ref, "result_ref"),
            )
        if (
            self.resolution is UnknownOutcomeResolution.CONFIRMED_SUCCEEDED
            and self.result_ref is None
        ):
            raise RecoveryDecisionError(
                "confirmed success requires an immutable result Artifact"
            )
        if (
            self.resolution is UnknownOutcomeResolution.CONFIRMED_FAILED
            and self.result_ref is not None
        ):
            raise RecoveryDecisionError(
                "confirmed failure must not provide a result Artifact"
            )

    @property
    def decision_digest(self) -> str:
        encoded = json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "resolution_id": self.resolution_id,
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "resolution": self.resolution.value,
            "evidence_ref": self.evidence_ref.to_dict(),
            "result_ref": (
                None if self.result_ref is None else self.result_ref.to_dict()
            ),
        }


__all__ = [
    "RecoveryDecisionError",
    "UnknownOutcomeDecision",
    "UnknownOutcomeResolution",
]
