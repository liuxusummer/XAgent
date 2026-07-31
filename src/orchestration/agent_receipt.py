"""Typed, payload-free receipt for one whole Agent Activity Attempt.

The receipt proves what the orchestration runtime observed at the Agent Loop
boundary. It never upgrades unreceipted tools inside the loop to verified
external side effects.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping

from .models import AttemptStatus
from .policy import EffectClass

AGENT_ACTIVITY_RECEIPT_SCHEMA_VERSION = 1
MAX_AGENT_RECEIPT_REFS = 64
MAX_AGENT_RECEIPT_TURNS = 1_000_000
MAX_AGENT_RECEIPT_TOOL_RESULTS = 1_000_000
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_ACTIVITY_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SAFE_CODE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


class AgentActivityReceiptError(ValueError):
    """An Agent receipt is malformed or disagrees with durable truth."""


class AgentActivityVerification(StrEnum):
    """Strength of whole-loop evidence, not external side-effect evidence."""

    RUNTIME_OBSERVED = "runtime_observed"
    UNVERIFIED = "unverified"


@dataclass(frozen=True, slots=True)
class AgentActivityReceipt:
    """Secret-free proof of one terminal Agent Activity boundary."""

    run_id: str
    node_id: str
    attempt_id: str
    activity_name: str
    effect_class: EffectClass
    attempt_status: AttemptStatus
    request_digest: str
    result_digest: str
    exit_reason: str
    turns: int | None
    observed_tool_results: int
    result_artifact_digests: tuple[str, ...] = ()
    tool_receipt_digests: tuple[str, ...] = ()
    internal_tool_receipts_complete: bool = False
    verification: AgentActivityVerification = (
        AgentActivityVerification.UNVERIFIED
    )
    schema_version: int = AGENT_ACTIVITY_RECEIPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in (
            "run_id",
            "node_id",
            "attempt_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(
                    getattr(self, field_name),
                    field_name,
                    maximum=255,
                ),
            )
        if (
            not isinstance(self.activity_name, str)
            or _ACTIVITY_NAME.fullmatch(self.activity_name) is None
        ):
            raise AgentActivityReceiptError(
                "activity_name must be a bounded identifier"
            )
        if (
            not isinstance(self.exit_reason, str)
            or _SAFE_CODE.fullmatch(self.exit_reason) is None
        ):
            raise AgentActivityReceiptError(
                "exit_reason must be a bounded safe code"
            )
        object.__setattr__(
            self,
            "request_digest",
            _digest(self.request_digest, "request_digest"),
        )
        object.__setattr__(
            self,
            "result_digest",
            _digest(self.result_digest, "result_digest"),
        )
        try:
            effect_class = EffectClass(self.effect_class)
            attempt_status = AttemptStatus(self.attempt_status)
            verification = AgentActivityVerification(self.verification)
        except (TypeError, ValueError) as exc:
            raise AgentActivityReceiptError(
                "AgentActivityReceipt enum is invalid"
            ) from exc
        if not attempt_status.is_terminal:
            raise AgentActivityReceiptError(
                "AgentActivityReceipt requires a terminal Attempt"
            )
        if (
            attempt_status is AttemptStatus.SUCCEEDED
            and verification
            is not AgentActivityVerification.RUNTIME_OBSERVED
        ):
            raise AgentActivityReceiptError(
                "successful Agent Activity requires runtime-observed evidence"
            )
        if (
            attempt_status
            in {AttemptStatus.OUTCOME_UNKNOWN, AttemptStatus.ABANDONED}
            and verification is not AgentActivityVerification.UNVERIFIED
        ):
            raise AgentActivityReceiptError(
                "uncertain Agent Activity cannot claim observed completion"
            )
        object.__setattr__(self, "effect_class", effect_class)
        object.__setattr__(self, "attempt_status", attempt_status)
        object.__setattr__(self, "verification", verification)
        object.__setattr__(
            self,
            "turns",
            _optional_count(
                self.turns,
                "turns",
                maximum=MAX_AGENT_RECEIPT_TURNS,
            ),
        )
        object.__setattr__(
            self,
            "observed_tool_results",
            _count(
                self.observed_tool_results,
                "observed_tool_results",
                maximum=MAX_AGENT_RECEIPT_TOOL_RESULTS,
            ),
        )
        result_refs = _digests(
            self.result_artifact_digests,
            "result_artifact_digests",
        )
        tool_refs = _digests(
            self.tool_receipt_digests,
            "tool_receipt_digests",
        )
        object.__setattr__(self, "result_artifact_digests", result_refs)
        object.__setattr__(self, "tool_receipt_digests", tool_refs)
        if not isinstance(self.internal_tool_receipts_complete, bool):
            raise AgentActivityReceiptError(
                "internal_tool_receipts_complete must be a bool"
            )
        if (
            self.internal_tool_receipts_complete
            and (
                verification
                is not AgentActivityVerification.RUNTIME_OBSERVED
                or self.observed_tool_results != len(tool_refs)
            )
        ):
            raise AgentActivityReceiptError(
                "complete internal receipts require exact observed coverage"
            )
        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version
            != AGENT_ACTIVITY_RECEIPT_SCHEMA_VERSION
        ):
            raise AgentActivityReceiptError(
                "unsupported AgentActivityReceipt schema version"
            )

    @property
    def receipt_digest(self) -> str:
        return canonical_agent_result_digest(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": "agent_activity_receipt",
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "activity_name": self.activity_name,
            "effect_class": self.effect_class.value,
            "attempt_status": self.attempt_status.value,
            "request_digest": self.request_digest,
            "result_digest": self.result_digest,
            "exit_reason": self.exit_reason,
            "turns": self.turns,
            "observed_tool_results": self.observed_tool_results,
            "result_artifact_digests": list(
                self.result_artifact_digests
            ),
            "tool_receipt_digests": list(self.tool_receipt_digests),
            "internal_tool_receipts_complete": (
                self.internal_tool_receipts_complete
            ),
            "verification": self.verification.value,
        }

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> "AgentActivityReceipt":
        if not isinstance(payload, Mapping):
            raise AgentActivityReceiptError(
                "AgentActivityReceipt must be an object"
            )
        required = {
            "schema_version",
            "kind",
            "run_id",
            "node_id",
            "attempt_id",
            "activity_name",
            "effect_class",
            "attempt_status",
            "request_digest",
            "result_digest",
            "exit_reason",
            "turns",
            "observed_tool_results",
            "result_artifact_digests",
            "tool_receipt_digests",
            "internal_tool_receipts_complete",
            "verification",
        }
        if (
            set(payload) != required
            or payload.get("kind") != "agent_activity_receipt"
        ):
            raise AgentActivityReceiptError(
                "AgentActivityReceipt contains unknown or missing fields"
            )
        result_refs = payload.get("result_artifact_digests")
        tool_refs = payload.get("tool_receipt_digests")
        if not isinstance(result_refs, list) or not isinstance(
            tool_refs,
            list,
        ):
            raise AgentActivityReceiptError(
                "AgentActivityReceipt refs must be arrays"
            )
        return cls(
            schema_version=payload.get("schema_version"),
            run_id=payload.get("run_id"),
            node_id=payload.get("node_id"),
            attempt_id=payload.get("attempt_id"),
            activity_name=payload.get("activity_name"),
            effect_class=payload.get("effect_class"),
            attempt_status=payload.get("attempt_status"),
            request_digest=payload.get("request_digest"),
            result_digest=payload.get("result_digest"),
            exit_reason=payload.get("exit_reason"),
            turns=payload.get("turns"),
            observed_tool_results=payload.get(
                "observed_tool_results"
            ),
            result_artifact_digests=tuple(result_refs),
            tool_receipt_digests=tuple(tool_refs),
            internal_tool_receipts_complete=payload.get(
                "internal_tool_receipts_complete"
            ),
            verification=payload.get("verification"),
        )


def canonical_agent_result_digest(value: Any) -> str:
    """Hash canonical JSON without executing arbitrary coercion hooks."""

    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (
        TypeError,
        ValueError,
        OverflowError,
        RecursionError,
    ) as exc:
        raise AgentActivityReceiptError(
            "Agent receipt binding must be canonical JSON"
        ) from exc
    return hashlib.sha256(encoded).hexdigest()


def _bounded_text(
    value: Any,
    field_name: str,
    *,
    maximum: int,
) -> str:
    if not isinstance(value, str):
        raise AgentActivityReceiptError(f"{field_name} must be a string")
    text = value.strip()
    if (
        not text
        or len(text) > maximum
        or any(
            ord(character) < 32 or ord(character) == 127
            for character in text
        )
    ):
        raise AgentActivityReceiptError(f"{field_name} is invalid")
    return text


def _digest(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise AgentActivityReceiptError(
            f"{field_name} must be a SHA-256 digest"
        )
    return value


def _digests(values: Any, field_name: str) -> tuple[str, ...]:
    if (
        not isinstance(values, (tuple, list))
        or len(values) > MAX_AGENT_RECEIPT_REFS
    ):
        raise AgentActivityReceiptError(
            f"{field_name} must be a bounded sequence"
        )
    return tuple(_digest(value, field_name) for value in values)


def _count(value: Any, field_name: str, *, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= maximum
    ):
        raise AgentActivityReceiptError(f"{field_name} is invalid")
    return value


def _optional_count(
    value: Any,
    field_name: str,
    *,
    maximum: int,
) -> int | None:
    if value is None:
        return None
    return _count(value, field_name, maximum=maximum)


__all__ = [
    "AGENT_ACTIVITY_RECEIPT_SCHEMA_VERSION",
    "AgentActivityReceipt",
    "AgentActivityReceiptError",
    "AgentActivityVerification",
    "canonical_agent_result_digest",
]
