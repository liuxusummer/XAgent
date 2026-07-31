"""Canonical, payload-free evidence for one complete Agent Activity runtime."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from .artifacts import (
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    ArtifactStore,
    canonical_json_bytes,
)
from .executor import ToolReceipt
from .provider_access import (
    ProviderAccessDenied,
    ProviderInvocationReceipt,
)

if TYPE_CHECKING:
    from .agent_request import AgentActivityRequest
    from .agent_receipt import AgentActivityReceipt


AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION = 2
LEGACY_AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION = 1
AGENT_EXECUTION_MANIFEST_MEDIA_TYPE = (
    "application/vnd.xagent.agent-execution-manifest+json"
)
MAX_AGENT_EXECUTION_MANIFEST_BYTES = 256 * 1024
MAX_AGENT_EXECUTION_TOOL_RECEIPTS = 64
MAX_AGENT_EXECUTION_PROVIDER_RECEIPTS = 64
MAX_AGENT_EXECUTION_TURNS = 1_000_000
_SCHEMA_NAMES = {
    LEGACY_AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION: (
        "agent_execution_manifest_v1"
    ),
    AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION: (
        "agent_execution_manifest_v2"
    ),
}
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$")
_SAFE_CODE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


class AgentExecutionManifestError(RuntimeError, ValueError):
    """An Agent execution manifest is malformed or misbound."""

    def __init__(self, reason_code: str) -> None:
        if (
            not isinstance(reason_code, str)
            or _SAFE_CODE.fullmatch(reason_code) is None
        ):
            raise ValueError("Agent execution manifest reason code is invalid")
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class AgentToolReceiptBinding:
    """One ordered child ToolReceipt bound to its parent Agent execution."""

    sequence: int
    turn: int
    run_id: str
    node_id: str
    attempt_id: str
    tool_name: str
    action_digest: str
    operation_key_digest: str
    idempotency_key_digest: str
    tool_call_digest: str
    receipt_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "sequence",
            _count(
                self.sequence,
                "invalid_tool_sequence",
                minimum=1,
                maximum=MAX_AGENT_EXECUTION_TOOL_RECEIPTS,
            ),
        )
        object.__setattr__(
            self,
            "turn",
            _count(
                self.turn,
                "invalid_tool_turn",
                minimum=1,
                maximum=MAX_AGENT_EXECUTION_TURNS,
            ),
        )
        for field_name in ("run_id", "node_id", "attempt_id"):
            object.__setattr__(
                self,
                field_name,
                _identifier(
                    getattr(self, field_name),
                    "invalid_tool_receipt_identity",
                ),
            )
        object.__setattr__(
            self,
            "tool_name",
            _safe_code(
                self.tool_name,
                "invalid_tool_name",
            ),
        )
        for field_name in (
            "action_digest",
            "operation_key_digest",
            "idempotency_key_digest",
            "tool_call_digest",
            "receipt_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _digest(
                    getattr(self, field_name),
                    "invalid_tool_receipt_digest",
                ),
            )

    @classmethod
    def from_receipt(
        cls,
        receipt: ToolReceipt,
        *,
        sequence: int,
        turn: int,
    ) -> "AgentToolReceiptBinding":
        if not isinstance(receipt, ToolReceipt):
            raise AgentExecutionManifestError("invalid_tool_receipt")
        return cls(
            sequence=sequence,
            turn=turn,
            run_id=receipt.run_id,
            node_id=receipt.node_id,
            attempt_id=receipt.attempt_id,
            tool_name=receipt.tool_name,
            action_digest=receipt.action_digest,
            operation_key_digest=receipt.operation_key_digest,
            idempotency_key_digest=receipt.idempotency_key_digest,
            tool_call_digest=canonical_tool_call_digest(receipt),
            receipt_digest=receipt.receipt_digest,
        )

    def validate_receipt(self, receipt: ToolReceipt) -> None:
        if (
            not isinstance(receipt, ToolReceipt)
            or self.run_id != receipt.run_id
            or self.node_id != receipt.node_id
            or self.attempt_id != receipt.attempt_id
            or self.tool_name != receipt.tool_name
            or self.action_digest != receipt.action_digest
            or self.operation_key_digest
            != receipt.operation_key_digest
            or self.idempotency_key_digest
            != receipt.idempotency_key_digest
            or self.tool_call_digest
            != canonical_tool_call_digest(receipt)
            or self.receipt_digest != receipt.receipt_digest
        ):
            raise AgentExecutionManifestError(
                "tool_receipt_binding_mismatch"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "turn": self.turn,
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "tool_name": self.tool_name,
            "action_digest": self.action_digest,
            "operation_key_digest": self.operation_key_digest,
            "idempotency_key_digest": self.idempotency_key_digest,
            "tool_call_digest": self.tool_call_digest,
            "receipt_digest": self.receipt_digest,
        }

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> "AgentToolReceiptBinding":
        required = {
            "sequence",
            "turn",
            "run_id",
            "node_id",
            "attempt_id",
            "tool_name",
            "action_digest",
            "operation_key_digest",
            "idempotency_key_digest",
            "tool_call_digest",
            "receipt_digest",
        }
        if not isinstance(payload, Mapping) or set(payload) != required:
            raise AgentExecutionManifestError(
                "invalid_tool_receipt_binding"
            )
        return cls(
            sequence=payload["sequence"],
            turn=payload["turn"],
            run_id=payload["run_id"],
            node_id=payload["node_id"],
            attempt_id=payload["attempt_id"],
            tool_name=payload["tool_name"],
            action_digest=payload["action_digest"],
            operation_key_digest=payload["operation_key_digest"],
            idempotency_key_digest=payload[
                "idempotency_key_digest"
            ],
            tool_call_digest=payload["tool_call_digest"],
            receipt_digest=payload["receipt_digest"],
        )


@dataclass(frozen=True, slots=True)
class AgentProviderReceiptBinding:
    """One ordered provider invocation observed by the Agent runtime."""

    sequence: int
    turn: int
    receipt: ProviderInvocationReceipt = field(repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "sequence",
            _count(
                self.sequence,
                "invalid_provider_sequence",
                minimum=1,
                maximum=MAX_AGENT_EXECUTION_PROVIDER_RECEIPTS,
            ),
        )
        object.__setattr__(
            self,
            "turn",
            _count(
                self.turn,
                "invalid_provider_turn",
                minimum=1,
                maximum=MAX_AGENT_EXECUTION_TURNS,
            ),
        )
        if (
            type(self.receipt) is not ProviderInvocationReceipt
            or self.receipt.invocation_index != self.sequence
        ):
            raise AgentExecutionManifestError(
                "invalid_provider_receipt_binding"
            )

    @property
    def receipt_digest(self) -> str:
        return self.receipt.receipt_digest

    def validate_receipt(
        self,
        receipt: ProviderInvocationReceipt,
    ) -> None:
        if (
            type(receipt) is not ProviderInvocationReceipt
            or receipt != self.receipt
            or receipt.receipt_digest != self.receipt_digest
        ):
            raise AgentExecutionManifestError(
                "provider_receipt_binding_mismatch"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "turn": self.turn,
            "receipt": self.receipt.to_dict(),
        }

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> "AgentProviderReceiptBinding":
        if (
            not isinstance(payload, Mapping)
            or set(payload) != {"sequence", "turn", "receipt"}
            or not isinstance(payload.get("receipt"), Mapping)
        ):
            raise AgentExecutionManifestError(
                "invalid_provider_receipt_binding"
            )
        try:
            receipt = ProviderInvocationReceipt.from_dict(
                payload["receipt"]
            )
        except ProviderAccessDenied:
            raise AgentExecutionManifestError(
                "invalid_provider_receipt_binding"
            ) from None
        return cls(
            sequence=payload["sequence"],
            turn=payload["turn"],
            receipt=receipt,
        )


@dataclass(frozen=True, slots=True, repr=False)
class AgentActivityExecutionManifest:
    """Immutable evidence joining an Agent request to ordered ToolReceipts."""

    run_id: str
    node_id: str
    attempt_id: str
    request_digest: str
    request_artifact_digest: str
    definition_digest: str
    artifact_sensitivity: ArtifactSensitivity
    exit_reason: str
    turns: int | None
    observed_tool_results: int
    tool_receipts_complete: bool
    tool_receipts: tuple[AgentToolReceiptBinding, ...] = ()
    observed_provider_invocations: int = 0
    provider_receipts_complete: bool = False
    provider_receipts: tuple[AgentProviderReceiptBinding, ...] = ()
    schema_version: int = AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in ("run_id", "node_id", "attempt_id"):
            object.__setattr__(
                self,
                field_name,
                _identifier(
                    getattr(self, field_name),
                    "invalid_agent_execution_identity",
                ),
            )
        for field_name in (
            "request_digest",
            "request_artifact_digest",
            "definition_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _digest(
                    getattr(self, field_name),
                    "invalid_agent_execution_digest",
                ),
            )
        try:
            sensitivity = ArtifactSensitivity(
                self.artifact_sensitivity
            )
        except (TypeError, ValueError):
            raise AgentExecutionManifestError(
                "invalid_agent_execution_sensitivity"
            ) from None
        if sensitivity not in {
            ArtifactSensitivity.SENSITIVE,
            ArtifactSensitivity.SECRET,
        }:
            raise AgentExecutionManifestError(
                "invalid_agent_execution_sensitivity"
            )
        object.__setattr__(
            self,
            "artifact_sensitivity",
            sensitivity,
        )
        object.__setattr__(
            self,
            "exit_reason",
            _safe_code(
                self.exit_reason,
                "invalid_agent_exit_reason",
            ),
        )
        if self.turns is not None:
            object.__setattr__(
                self,
                "turns",
                _count(
                    self.turns,
                    "invalid_agent_turn_count",
                    minimum=0,
                    maximum=MAX_AGENT_EXECUTION_TURNS,
                ),
            )
        object.__setattr__(
            self,
            "observed_tool_results",
            _count(
                self.observed_tool_results,
                "invalid_observed_tool_result_count",
                minimum=0,
                maximum=MAX_AGENT_EXECUTION_TURNS,
            ),
        )
        if not isinstance(self.tool_receipts_complete, bool):
            raise AgentExecutionManifestError(
                "invalid_tool_receipt_completeness"
            )
        receipts = tuple(self.tool_receipts)
        if (
            len(receipts) > MAX_AGENT_EXECUTION_TOOL_RECEIPTS
            or not all(
                isinstance(item, AgentToolReceiptBinding)
                for item in receipts
            )
            or tuple(item.sequence for item in receipts)
            != tuple(range(1, len(receipts) + 1))
            or tuple(item.turn for item in receipts)
            != tuple(sorted(item.turn for item in receipts))
            or len({item.receipt_digest for item in receipts})
            != len(receipts)
            or len({item.tool_call_digest for item in receipts})
            != len(receipts)
            or len(
                {
                    (item.run_id, item.node_id, item.attempt_id)
                    for item in receipts
                }
            )
            != len(receipts)
        ):
            raise AgentExecutionManifestError(
                "invalid_ordered_tool_receipts"
            )
        for item in receipts:
            expected_operation_digest = hashlib.sha256(
                agent_tool_operation_key(
                    run_id=self.run_id,
                    node_id=self.node_id,
                    attempt_id=self.attempt_id,
                    request_digest=self.request_digest,
                    sequence=item.sequence,
                ).encode("utf-8")
            ).hexdigest()
            if (
                item.run_id != self.run_id
                or item.operation_key_digest
                != expected_operation_digest
                or item.idempotency_key_digest
                != expected_operation_digest
            ):
                raise AgentExecutionManifestError(
                    "tool_receipt_parent_binding_mismatch"
                )
        if self.turns is None and receipts:
            raise AgentExecutionManifestError(
                "tool_receipts_require_turn_count"
            )
        if (
            self.turns is not None
            and any(item.turn > self.turns for item in receipts)
        ):
            raise AgentExecutionManifestError(
                "tool_receipt_turn_out_of_range"
            )
        if len(receipts) > self.observed_tool_results:
            raise AgentExecutionManifestError(
                "tool_receipts_exceed_observed_results"
            )
        if (
            self.tool_receipts_complete
            and len(receipts) != self.observed_tool_results
        ):
            raise AgentExecutionManifestError(
                "complete_tool_receipts_require_exact_coverage"
            )
        object.__setattr__(self, "tool_receipts", receipts)
        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version
            not in {
                LEGACY_AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION,
                AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION,
            }
        ):
            raise AgentExecutionManifestError(
                "unsupported_agent_execution_manifest_schema"
            )
        observed_provider_invocations = _count(
            self.observed_provider_invocations,
            "invalid_observed_provider_invocation_count",
            minimum=0,
            maximum=MAX_AGENT_EXECUTION_TURNS,
        )
        object.__setattr__(
            self,
            "observed_provider_invocations",
            observed_provider_invocations,
        )
        if not isinstance(self.provider_receipts_complete, bool):
            raise AgentExecutionManifestError(
                "invalid_provider_receipt_completeness"
            )
        provider_receipts = tuple(self.provider_receipts)
        if (
            len(provider_receipts)
            > MAX_AGENT_EXECUTION_PROVIDER_RECEIPTS
            or not all(
                type(item) is AgentProviderReceiptBinding
                for item in provider_receipts
            )
            or tuple(item.sequence for item in provider_receipts)
            != tuple(range(1, len(provider_receipts) + 1))
            or tuple(item.turn for item in provider_receipts)
            != tuple(
                sorted(item.turn for item in provider_receipts)
            )
            or len(
                {
                    item.receipt.grant_id
                    for item in provider_receipts
                }
            )
            != len(provider_receipts)
            or len(
                {
                    item.receipt_digest
                    for item in provider_receipts
                }
            )
            != len(provider_receipts)
        ):
            raise AgentExecutionManifestError(
                "invalid_ordered_provider_receipts"
            )
        if (
            self.schema_version
            == LEGACY_AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION
            and (
                observed_provider_invocations != 0
                or self.provider_receipts_complete
                or provider_receipts
            )
        ):
            raise AgentExecutionManifestError(
                "legacy_manifest_cannot_bind_provider_receipts"
            )
        for item in provider_receipts:
            receipt = item.receipt
            if (
                receipt.run_id != self.run_id
                or receipt.node_id != self.node_id
                or receipt.attempt_id != self.attempt_id
                or receipt.request_digest != self.request_digest
                or receipt.request_artifact_digest
                != self.request_artifact_digest
                or receipt.response_artifact_ref_digest is None
                or _sensitivity_rank(sensitivity)
                < _sensitivity_rank(
                    receipt.response_sensitivity
                )
            ):
                raise AgentExecutionManifestError(
                    "provider_receipt_parent_binding_mismatch"
                )
        if self.turns is None and provider_receipts:
            raise AgentExecutionManifestError(
                "provider_receipts_require_turn_count"
            )
        if (
            self.turns is not None
            and any(
                item.turn > self.turns
                for item in provider_receipts
            )
        ):
            raise AgentExecutionManifestError(
                "provider_receipt_turn_out_of_range"
            )
        if (
            len(provider_receipts)
            > observed_provider_invocations
        ):
            raise AgentExecutionManifestError(
                "provider_receipts_exceed_observed_invocations"
            )
        if (
            self.provider_receipts_complete
            and len(provider_receipts)
            != observed_provider_invocations
        ):
            raise AgentExecutionManifestError(
                "complete_provider_receipts_require_exact_coverage"
            )
        object.__setattr__(
            self,
            "provider_receipts",
            provider_receipts,
        )
        self.to_bytes()

    def __repr__(self) -> str:
        return (
            "AgentActivityExecutionManifest("
            f"run_id={self.run_id!r}, node_id={self.node_id!r}, "
            f"attempt_id={self.attempt_id!r}, "
            f"request_digest={self.request_digest!r}, "
            f"tool_receipt_count={len(self.tool_receipts)}, "
            "provider_receipt_count="
            f"{len(self.provider_receipts)})"
        )

    @property
    def manifest_digest(self) -> str:
        return hashlib.sha256(self.to_bytes()).hexdigest()

    @property
    def tool_receipt_digests(self) -> tuple[str, ...]:
        return tuple(item.receipt_digest for item in self.tool_receipts)

    @property
    def provider_receipt_digests(self) -> tuple[str, ...]:
        return tuple(
            item.receipt_digest for item in self.provider_receipts
        )

    @property
    def has_complete_provider_receipt_lineage(self) -> bool:
        return (
            self.schema_version
            == AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION
            and self.provider_receipts_complete
            and len(self.provider_receipts)
            == self.observed_provider_invocations
        )

    def validate_tool_receipts(
        self,
        receipts: Sequence[ToolReceipt],
    ) -> None:
        if (
            not isinstance(receipts, (tuple, list))
            or len(receipts) != len(self.tool_receipts)
        ):
            raise AgentExecutionManifestError(
                "tool_receipt_set_mismatch"
            )
        for binding, receipt in zip(
            self.tool_receipts,
            receipts,
            strict=True,
        ):
            binding.validate_receipt(receipt)

    def validate_provider_receipts(
        self,
        receipts: Sequence[ProviderInvocationReceipt],
        *,
        require_complete: bool = False,
    ) -> None:
        if (
            not isinstance(require_complete, bool)
            or (
                require_complete
                and not self.has_complete_provider_receipt_lineage
            )
        ):
            raise AgentExecutionManifestError(
                "provider_receipt_lineage_incomplete"
            )
        if (
            not isinstance(receipts, (tuple, list))
            or len(receipts) != len(self.provider_receipts)
        ):
            raise AgentExecutionManifestError(
                "provider_receipt_set_mismatch"
            )
        for binding, receipt in zip(
            self.provider_receipts,
            receipts,
            strict=True,
        ):
            binding.validate_receipt(receipt)

    def validate_request(
        self,
        request: "AgentActivityRequest",
        request_ref: ArtifactRef,
    ) -> None:
        from .agent_request import (
            AgentActivityRequest,
            AgentActivityRequestArtifactStore,
        )

        if not isinstance(request, AgentActivityRequest):
            raise AgentExecutionManifestError(
                "invalid_agent_request"
            )
        try:
            AgentActivityRequestArtifactStore.validate_ref(
                request_ref,
                request,
            )
        except ValueError:
            raise AgentExecutionManifestError(
                "agent_request_manifest_binding_mismatch"
            ) from None
        if (
            self.run_id != request.run_id
            or self.node_id != request.node_id
            or self.attempt_id != request.attempt_id
            or self.request_digest != request.request_digest
            or self.request_artifact_digest
            != request.artifact_digest
            or self.definition_digest != request.definition_digest
            or _sensitivity_rank(self.artifact_sensitivity)
            < _sensitivity_rank(request_ref.sensitivity)
            or (
                self.schema_version
                == LEGACY_AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION
                and self.artifact_sensitivity
                is not request_ref.sensitivity
            )
        ):
            raise AgentExecutionManifestError(
                "agent_request_manifest_binding_mismatch"
            )

    def validate_agent_receipt(
        self,
        receipt: "AgentActivityReceipt",
    ) -> None:
        from .agent_receipt import AgentActivityReceipt

        if (
            not isinstance(receipt, AgentActivityReceipt)
            or receipt.run_id != self.run_id
            or receipt.node_id != self.node_id
            or receipt.attempt_id != self.attempt_id
            or receipt.request_digest != self.request_digest
            or receipt.exit_reason != self.exit_reason
            or receipt.turns != self.turns
            or receipt.observed_tool_results
            != self.observed_tool_results
            or receipt.tool_receipt_digests
            != self.tool_receipt_digests
            or receipt.internal_tool_receipts_complete
            is not self.tool_receipts_complete
            or receipt.execution_manifest_digest
            != self.manifest_digest
        ):
            raise AgentExecutionManifestError(
                "agent_receipt_manifest_binding_mismatch"
            )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "schema_version": self.schema_version,
            "kind": "agent_activity_execution_manifest",
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "request_digest": self.request_digest,
            "request_artifact_digest": self.request_artifact_digest,
            "definition_digest": self.definition_digest,
            "artifact_sensitivity": self.artifact_sensitivity.value,
            "exit_reason": self.exit_reason,
            "turns": self.turns,
            "observed_tool_results": self.observed_tool_results,
            "tool_receipts_complete": self.tool_receipts_complete,
            "tool_receipts": [
                item.to_dict() for item in self.tool_receipts
            ],
        }
        if self.schema_version == AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION:
            payload.update(
                {
                    "observed_provider_invocations": (
                        self.observed_provider_invocations
                    ),
                    "provider_receipts_complete": (
                        self.provider_receipts_complete
                    ),
                    "provider_receipts": [
                        item.to_dict()
                        for item in self.provider_receipts
                    ],
                }
            )
        return payload

    def to_bytes(self) -> bytes:
        try:
            content = canonical_json_bytes(self.to_dict())
        except (
            TypeError,
            ValueError,
            OverflowError,
            RecursionError,
        ):
            raise AgentExecutionManifestError(
                "agent_execution_manifest_not_canonical"
            ) from None
        if len(content) > MAX_AGENT_EXECUTION_MANIFEST_BYTES:
            raise AgentExecutionManifestError(
                "agent_execution_manifest_too_large"
            )
        return content

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> "AgentActivityExecutionManifest":
        if not isinstance(payload, Mapping):
            raise AgentExecutionManifestError(
                "invalid_agent_execution_manifest_schema"
            )
        version = payload.get("schema_version")
        if (
            isinstance(version, bool)
            or not isinstance(version, int)
            or version
            not in {
                LEGACY_AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION,
                AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION,
            }
        ):
            raise AgentExecutionManifestError(
                "unsupported_agent_execution_manifest_schema"
            )
        required = {
            "schema_version",
            "kind",
            "run_id",
            "node_id",
            "attempt_id",
            "request_digest",
            "request_artifact_digest",
            "definition_digest",
            "artifact_sensitivity",
            "exit_reason",
            "turns",
            "observed_tool_results",
            "tool_receipts_complete",
            "tool_receipts",
        }
        if version == AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION:
            required.update(
                {
                    "observed_provider_invocations",
                    "provider_receipts_complete",
                    "provider_receipts",
                }
            )
        if (
            set(payload) != required
            or payload.get("kind")
            != "agent_activity_execution_manifest"
            or not isinstance(payload.get("tool_receipts"), list)
            or len(payload["tool_receipts"])
            > MAX_AGENT_EXECUTION_TOOL_RECEIPTS
            or (
                version == AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION
                and (
                    not isinstance(
                        payload.get("provider_receipts"),
                        list,
                    )
                    or len(payload["provider_receipts"])
                    > MAX_AGENT_EXECUTION_PROVIDER_RECEIPTS
                )
            )
        ):
            raise AgentExecutionManifestError(
                "invalid_agent_execution_manifest_schema"
            )
        return cls(
            schema_version=payload["schema_version"],
            run_id=payload["run_id"],
            node_id=payload["node_id"],
            attempt_id=payload["attempt_id"],
            request_digest=payload["request_digest"],
            request_artifact_digest=payload[
                "request_artifact_digest"
            ],
            definition_digest=payload["definition_digest"],
            artifact_sensitivity=payload["artifact_sensitivity"],
            exit_reason=payload["exit_reason"],
            turns=payload["turns"],
            observed_tool_results=payload[
                "observed_tool_results"
            ],
            tool_receipts_complete=payload[
                "tool_receipts_complete"
            ],
            tool_receipts=tuple(
                AgentToolReceiptBinding.from_dict(item)
                for item in payload["tool_receipts"]
            ),
            observed_provider_invocations=payload.get(
                "observed_provider_invocations",
                0,
            ),
            provider_receipts_complete=payload.get(
                "provider_receipts_complete",
                False,
            ),
            provider_receipts=tuple(
                AgentProviderReceiptBinding.from_dict(item)
                for item in payload.get("provider_receipts", ())
            ),
        )

    @classmethod
    def from_bytes(
        cls,
        content: bytes,
    ) -> "AgentActivityExecutionManifest":
        if (
            not isinstance(content, bytes)
            or not content
            or len(content) > MAX_AGENT_EXECUTION_MANIFEST_BYTES
        ):
            raise AgentExecutionManifestError(
                "invalid_agent_execution_manifest_payload"
            )
        try:
            payload = json.loads(
                content.decode("utf-8"),
                parse_constant=_reject_json_constant,
            )
            manifest = cls.from_dict(payload)
        except AgentExecutionManifestError:
            raise
        except (
            TypeError,
            ValueError,
            UnicodeError,
            RecursionError,
        ):
            raise AgentExecutionManifestError(
                "invalid_agent_execution_manifest_payload"
            ) from None
        if manifest.to_bytes() != content:
            raise AgentExecutionManifestError(
                "noncanonical_agent_execution_manifest_payload"
            )
        return manifest


class AgentExecutionManifestArtifactStore:
    """Stage and verify immutable Agent execution manifest Artifacts."""

    def __init__(self, store: ArtifactStore) -> None:
        if not isinstance(store, ArtifactStore):
            raise AgentExecutionManifestError("invalid_artifact_store")
        self._store = store

    def stage(
        self,
        manifest: AgentActivityExecutionManifest,
    ) -> ArtifactRef:
        if not isinstance(manifest, AgentActivityExecutionManifest):
            raise AgentExecutionManifestError(
                "invalid_agent_execution_manifest"
            )
        try:
            ref = self._store.put_bytes(
                manifest.to_bytes(),
                media_type=AGENT_EXECUTION_MANIFEST_MEDIA_TYPE,
                kind=ArtifactKind.AGENT_EXECUTION_MANIFEST,
                sensitivity=manifest.artifact_sensitivity,
                producer_run_id=manifest.run_id,
                producer_node_id=manifest.node_id,
                producer_attempt_id=manifest.attempt_id,
                metadata={
                    "schema": _SCHEMA_NAMES[
                        manifest.schema_version
                    ]
                },
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise AgentExecutionManifestError(
                "agent_execution_manifest_write_failed"
            ) from None
        self.validate_ref(ref, manifest=manifest)
        try:
            verified = self._store.verify(ref)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise AgentExecutionManifestError(
                "agent_execution_manifest_verify_failed"
            ) from None
        if verified is not True:
            raise AgentExecutionManifestError(
                "agent_execution_manifest_verify_failed"
            )
        return ref

    def load(
        self,
        ref: ArtifactRef,
        *,
        expected_receipt: "AgentActivityReceipt | None" = None,
        tool_receipts: Sequence[ToolReceipt] | None = None,
        provider_receipts: Sequence[
            ProviderInvocationReceipt
        ]
        | None = None,
        require_complete_provider_receipts: bool = False,
    ) -> AgentActivityExecutionManifest:
        if not isinstance(
            require_complete_provider_receipts,
            bool,
        ):
            raise AgentExecutionManifestError(
                "invalid_provider_receipt_completeness_requirement"
            )
        self.validate_ref(ref)
        try:
            verified = self._store.verify(ref)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise AgentExecutionManifestError(
                "agent_execution_manifest_verify_failed"
            ) from None
        if verified is not True:
            raise AgentExecutionManifestError(
                "agent_execution_manifest_verify_failed"
            )
        try:
            content = self._store.read(ref)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise AgentExecutionManifestError(
                "agent_execution_manifest_read_failed"
            ) from None
        if (
            not isinstance(content, bytes)
            or len(content) != ref.size
            or hashlib.sha256(content).hexdigest() != ref.sha256
        ):
            raise AgentExecutionManifestError(
                "agent_execution_manifest_integrity_failed"
            )
        manifest = AgentActivityExecutionManifest.from_bytes(content)
        self.validate_ref(ref, manifest=manifest)
        if expected_receipt is not None:
            manifest.validate_agent_receipt(expected_receipt)
        if tool_receipts is not None:
            manifest.validate_tool_receipts(tool_receipts)
        if provider_receipts is not None:
            manifest.validate_provider_receipts(
                provider_receipts,
                require_complete=(
                    require_complete_provider_receipts
                ),
            )
        elif require_complete_provider_receipts:
            raise AgentExecutionManifestError(
                "provider_receipt_set_mismatch"
            )
        return manifest

    @staticmethod
    def validate_ref(
        ref: ArtifactRef,
        *,
        manifest: AgentActivityExecutionManifest | None = None,
    ) -> None:
        if (
            not isinstance(ref, ArtifactRef)
            or ref.kind is not ArtifactKind.AGENT_EXECUTION_MANIFEST
            or ref.media_type
            != AGENT_EXECUTION_MANIFEST_MEDIA_TYPE
            or ref.sensitivity
            not in {
                ArtifactSensitivity.SENSITIVE,
                ArtifactSensitivity.SECRET,
            }
            or not 0 < ref.size <= MAX_AGENT_EXECUTION_MANIFEST_BYTES
            or dict(ref.metadata)
            not in (
                {"schema": schema}
                for schema in _SCHEMA_NAMES.values()
            )
        ):
            raise AgentExecutionManifestError(
                "invalid_agent_execution_manifest_artifact"
            )
        if manifest is not None and (
            ref.sha256 != manifest.manifest_digest
            or ref.size != len(manifest.to_bytes())
            or ref.sensitivity is not manifest.artifact_sensitivity
            or dict(ref.metadata)
            != {
                "schema": _SCHEMA_NAMES[
                    manifest.schema_version
                ]
            }
            or ref.producer_run_id != manifest.run_id
            or ref.producer_node_id != manifest.node_id
            or ref.producer_attempt_id != manifest.attempt_id
        ):
            raise AgentExecutionManifestError(
                "agent_execution_manifest_artifact_binding_mismatch"
            )


def canonical_tool_call_digest(receipt: ToolReceipt) -> str:
    """Bind the exact logical Tool call represented by one ToolReceipt."""

    if not isinstance(receipt, ToolReceipt):
        raise AgentExecutionManifestError("invalid_tool_receipt")
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "run_id": receipt.run_id,
                "node_id": receipt.node_id,
                "attempt_id": receipt.attempt_id,
                "tool_name": receipt.tool_name,
                "args_digest": receipt.args_digest,
                "action_digest": receipt.action_digest,
                "execution_binding_digest": (
                    receipt.execution_binding_digest
                ),
                "operation_key_digest": (
                    receipt.operation_key_digest
                ),
                "idempotency_key_digest": (
                    receipt.idempotency_key_digest
                ),
            }
        )
    ).hexdigest()


def agent_tool_operation_key(
    *,
    run_id: str,
    node_id: str,
    attempt_id: str,
    request_digest: str,
    sequence: int,
) -> str:
    """Derive the raw idempotent child-operation key for one Agent tool call."""

    parent = {
        "schema_version": 1,
        "kind": "agent_tool_child_operation",
        "run_id": _identifier(
            run_id,
            "invalid_agent_execution_identity",
        ),
        "node_id": _identifier(
            node_id,
            "invalid_agent_execution_identity",
        ),
        "attempt_id": _identifier(
            attempt_id,
            "invalid_agent_execution_identity",
        ),
        "request_digest": _digest(
            request_digest,
            "invalid_agent_execution_digest",
        ),
        "sequence": _count(
            sequence,
            "invalid_tool_sequence",
            minimum=1,
            maximum=MAX_AGENT_EXECUTION_TOOL_RECEIPTS,
        ),
    }
    return "agent-tool:" + hashlib.sha256(
        canonical_json_bytes(parent)
    ).hexdigest()


def _identifier(value: Any, reason_code: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise AgentExecutionManifestError(reason_code)
    return value


def _safe_code(value: Any, reason_code: str) -> str:
    if not isinstance(value, str) or _SAFE_CODE.fullmatch(value) is None:
        raise AgentExecutionManifestError(reason_code)
    return value


def _digest(value: Any, reason_code: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise AgentExecutionManifestError(reason_code)
    return value


def _count(
    value: Any,
    reason_code: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise AgentExecutionManifestError(reason_code)
    return value


def _reject_json_constant(_value: str) -> object:
    raise ValueError("non-finite JSON constant")


def _sensitivity_rank(value: ArtifactSensitivity) -> int:
    return {
        ArtifactSensitivity.PUBLIC: 0,
        ArtifactSensitivity.INTERNAL: 1,
        ArtifactSensitivity.SENSITIVE: 2,
        ArtifactSensitivity.SECRET: 3,
    }[value]


__all__ = [
    "AGENT_EXECUTION_MANIFEST_MEDIA_TYPE",
    "AGENT_EXECUTION_MANIFEST_SCHEMA_VERSION",
    "AgentActivityExecutionManifest",
    "AgentExecutionManifestArtifactStore",
    "AgentExecutionManifestError",
    "AgentProviderReceiptBinding",
    "AgentToolReceiptBinding",
    "agent_tool_operation_key",
    "canonical_tool_call_digest",
]
