"""Crash-consistent terminal composition for one successful Agent Attempt."""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from .agent_execution_evidence import (
    AgentExecutionEvidenceCollector,
    AgentExecutionEvidenceCollectorError,
)
from .agent_execution_manifest import (
    AgentActivityExecutionManifest,
    AgentExecutionManifestArtifactStore,
    AgentExecutionManifestError,
)
from .agent_receipt import (
    AgentActivityReceipt,
    AgentActivityReceiptError,
    AgentActivityVerification,
    canonical_agent_result_digest,
)
from .agent_request import (
    AgentActivityRequestArtifactStore,
    AgentActivityRequestError,
)
from .artifacts import (
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    ArtifactStore,
    LocalArtifactStore,
    canonical_json_bytes,
)
from .executor import ToolReceipt
from .models import AttemptStatus, JsonValue, normalize_json
from .policy import EffectClass
from .scheduler import ActivityClaim, DurableScheduler
from .store import DurableRunStore, OrchestrationStoreError
from .tool_receipt_artifact import (
    ToolReceiptArtifactError,
    ToolReceiptArtifactStore,
)


MAX_AGENT_TERMINAL_METADATA_BYTES = 16 * 1024
_SAFE_METRIC_NAME = re.compile(
    r"^[A-Za-z][A-Za-z0-9_.:-]{0,63}$"
)
_ERROR_REASONS = frozenset(
    {
        "agent_terminal_artifact_invalid",
        "agent_terminal_claim_invalid",
        "agent_terminal_configuration_invalid",
        "agent_terminal_evidence_incomplete",
        "agent_terminal_request_invalid",
        "agent_terminal_result_invalid",
        "agent_terminal_store_unavailable",
    }
)


class AgentTerminalCommitError(RuntimeError, ValueError):
    """A successful Agent terminal could not cross the trusted boundary."""

    def __init__(self, reason_code: str) -> None:
        if reason_code not in _ERROR_REASONS:
            raise ValueError("invalid Agent terminal reason code")
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True, repr=False)
class AgentActivityTerminalCommit:
    """Durable facts returned only after Store receipt re-verification."""

    receipt: AgentActivityReceipt
    manifest_ref: ArtifactRef
    tool_receipt_refs: tuple[ArtifactRef, ...]
    node_result: Mapping[str, JsonValue] = field(repr=False)
    replayed: bool = False
    reconcile_pending: bool = False

    def __repr__(self) -> str:
        return (
            "AgentActivityTerminalCommit("
            f"run_id={self.receipt.run_id!r}, "
            f"attempt_id={self.receipt.attempt_id!r}, "
            f"receipt_digest={self.receipt.receipt_digest!r}, "
            f"replayed={self.replayed}, "
            f"reconcile_pending={self.reconcile_pending})"
        )


class DurableAgentTerminalCommitter:
    """Stage immutable evidence, then atomically accept the parent terminal."""

    def __init__(
        self,
        scheduler: DurableScheduler,
        artifact_store: ArtifactStore,
        *,
        clock: Callable[[], float] | None = None,
        fault_hook: Callable[[str], None] | None = None,
    ) -> None:
        if (
            not isinstance(scheduler, DurableScheduler)
            or not isinstance(scheduler.store, DurableRunStore)
            or not isinstance(artifact_store, ArtifactStore)
        ):
            raise AgentTerminalCommitError(
                "agent_terminal_configuration_invalid"
            )
        try:
            durable_artifacts = (
                isinstance(artifact_store, LocalArtifactStore)
                or getattr(
                    artifact_store,
                    "durable_result_recovery_ready",
                    False,
                )
                is True
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            durable_artifacts = False
        if not durable_artifacts:
            raise AgentTerminalCommitError(
                "agent_terminal_configuration_invalid"
            )
        self.scheduler = scheduler
        self.store = scheduler.store
        self.artifact_store = artifact_store
        self._request_artifacts = AgentActivityRequestArtifactStore(
            artifact_store
        )
        self._manifest_artifacts = AgentExecutionManifestArtifactStore(
            artifact_store
        )
        self._tool_receipt_artifacts = ToolReceiptArtifactStore(
            artifact_store
        )
        self._clock = clock or scheduler.now
        self._fault_hook = fault_hook

    @property
    def durable_result_recovery_ready(self) -> bool:
        return True

    @property
    def production_security_ready(self) -> bool:
        """Remote runtime attestation and resumable history remain external."""

        return False

    def __repr__(self) -> str:
        return "DurableAgentTerminalCommitter(production_security_ready=False)"

    def commit_success(
        self,
        claim: ActivityClaim,
        request_ref: ArtifactRef,
        collector: AgentExecutionEvidenceCollector,
        *,
        exit_reason: str,
        turns: int,
        result_artifact_refs: Iterable[ArtifactRef] = (),
        metrics: Mapping[str, Any] | None = None,
    ) -> AgentActivityTerminalCommit:
        """Commit one complete, manifest-bound successful Agent Attempt."""

        self._validate_claim(claim)
        if exit_reason != "CURRENT_TASK_DONE":
            raise AgentTerminalCommitError(
                "agent_terminal_result_invalid"
            )
        request = self._load_request(claim, request_ref)
        if type(collector) is not AgentExecutionEvidenceCollector:
            raise AgentTerminalCommitError(
                "agent_terminal_evidence_incomplete"
            )
        try:
            collector.validate_parent_binding(
                run_id=claim.run_id,
                node_id=claim.node_id,
                attempt_id=claim.attempt_id,
                request_digest=claim.request_hash,
                request_artifact_digest=request_ref.sha256,
                definition_digest=request.definition_digest,
                request_sensitivity=request_ref.sensitivity,
            )
            manifest = collector.finalize(
                exit_reason=exit_reason,
                turns=turns,
            )
        except AgentExecutionEvidenceCollectorError:
            raise AgentTerminalCommitError(
                "agent_terminal_evidence_incomplete"
            ) from None
        self._require_complete_manifest(manifest)
        tool_receipts = self._load_tool_receipts(manifest)
        provider_receipts = tuple(
            binding.receipt for binding in manifest.provider_receipts
        )
        try:
            tool_refs = tuple(
                self._tool_receipt_artifacts.stage(
                    receipt,
                    sensitivity=manifest.artifact_sensitivity,
                )
                for receipt in tool_receipts
            )
            manifest_ref = self._manifest_artifacts.stage(manifest)
        except (
            AgentExecutionManifestError,
            ToolReceiptArtifactError,
        ):
            raise AgentTerminalCommitError(
                "agent_terminal_artifact_invalid"
            ) from None
        result_refs = self._result_refs(
            result_artifact_refs,
            claim=claim,
            manifest_ref=manifest_ref,
            minimum_sensitivity=manifest.artifact_sensitivity,
        )
        self._validate_provider_result_refs(manifest, result_refs)
        node_result = self._node_result(
            metrics=metrics,
            result_refs=result_refs,
            tool_receipt_refs=tool_refs,
        )
        try:
            receipt = AgentActivityReceipt(
                run_id=claim.run_id,
                node_id=claim.node_id,
                attempt_id=claim.attempt_id,
                activity_name=request.agent_name,
                effect_class=EffectClass(claim.effect_class),
                attempt_status=AttemptStatus.SUCCEEDED,
                request_digest=claim.request_hash,
                result_digest=canonical_agent_result_digest(node_result),
                exit_reason=manifest.exit_reason,
                turns=manifest.turns,
                observed_tool_results=manifest.observed_tool_results,
                result_artifact_digests=tuple(
                    ref.sha256 for ref in result_refs
                ),
                tool_receipt_digests=tuple(
                    ref.sha256 for ref in tool_refs
                ),
                internal_tool_receipts_complete=True,
                execution_manifest_digest=manifest.manifest_digest,
                verification=AgentActivityVerification.RUNTIME_OBSERVED,
            )
            manifest.validate_agent_receipt(receipt)
            manifest.validate_request(request, request_ref)
            self._manifest_artifacts.load(
                manifest_ref,
                expected_receipt=receipt,
                tool_receipts=tool_receipts,
                provider_receipts=provider_receipts,
                require_complete_provider_receipts=True,
            )
        except (
            AgentActivityReceiptError,
            AgentExecutionManifestError,
        ):
            raise AgentTerminalCommitError(
                "agent_terminal_evidence_incomplete"
            ) from None
        self._fault("agent_terminal.artifacts_staged")
        try:
            prior = self.store.get_agent_activity_receipt(
                claim.run_id,
                claim.attempt_id,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise AgentTerminalCommitError(
                "agent_terminal_store_unavailable"
            ) from None
        replayed = prior is not None
        try:
            self.store.complete_verified_agent_activity(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
                claim.request_hash,
                claim.worker_id,
                claim_token=claim.claim_token,
                result=node_result,
                receipt=receipt,
                now=self._now(),
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            durable = self._durable_receipt(claim)
            if durable != receipt:
                raise AgentTerminalCommitError(
                    "agent_terminal_store_unavailable"
                ) from None
            replayed = True
        self._fault("agent_terminal.store_committed")
        durable = self._durable_receipt(claim)
        if durable != receipt:
            raise AgentTerminalCommitError(
                "agent_terminal_store_unavailable"
            )
        reconcile_pending = False
        try:
            self.scheduler.reconcile(claim.run_id)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            reconcile_pending = True
        return AgentActivityTerminalCommit(
            receipt=durable,
            manifest_ref=manifest_ref,
            tool_receipt_refs=tool_refs,
            node_result=_freeze_json(node_result),
            replayed=replayed,
            reconcile_pending=reconcile_pending,
        )

    def _load_request(
        self,
        claim: ActivityClaim,
        request_ref: ArtifactRef,
    ):
        try:
            request = self._request_artifacts.load(request_ref)
        except AgentActivityRequestError:
            raise AgentTerminalCommitError(
                "agent_terminal_request_invalid"
            ) from None
        if (
            request.run_id != claim.run_id
            or request.node_id != claim.node_id
            or request.attempt_id != claim.attempt_id
            or request.attempt_number != claim.attempt_number
            or request.request_digest != claim.request_hash
            or request.agent_name != claim.config.get("agent")
        ):
            raise AgentTerminalCommitError(
                "agent_terminal_request_invalid"
            )
        return request

    def _load_tool_receipts(
        self,
        manifest: AgentActivityExecutionManifest,
    ) -> tuple[ToolReceipt, ...]:
        receipts: list[ToolReceipt] = []
        for binding in manifest.tool_receipts:
            try:
                receipt = self.store.get_tool_receipt(
                    manifest.run_id,
                    binding.attempt_id,
                )
                if receipt is None:
                    raise ValueError
                binding.validate_receipt(receipt)
            except (OrchestrationStoreError, ValueError):
                raise AgentTerminalCommitError(
                    "agent_terminal_evidence_incomplete"
                ) from None
            receipts.append(receipt)
        return tuple(receipts)

    def _result_refs(
        self,
        values: Iterable[ArtifactRef],
        *,
        claim: ActivityClaim,
        manifest_ref: ArtifactRef,
        minimum_sensitivity: ArtifactSensitivity,
    ) -> tuple[ArtifactRef, ...]:
        try:
            refs = tuple(values)
        except TypeError:
            raise AgentTerminalCommitError(
                "agent_terminal_artifact_invalid"
            ) from None
        if len(refs) > 63 or any(type(ref) is not ArtifactRef for ref in refs):
            raise AgentTerminalCommitError(
                "agent_terminal_artifact_invalid"
            )
        detached: list[ArtifactRef] = []
        for ref in refs:
            try:
                value = ArtifactRef.from_dict(ref.to_dict())
                verified = self.artifact_store.verify(value)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                raise AgentTerminalCommitError(
                    "agent_terminal_artifact_invalid"
                ) from None
            if (
                verified is not True
                or _sensitivity_rank(value.sensitivity)
                < _sensitivity_rank(minimum_sensitivity)
                or value.metadata
                or value.producer_run_id != claim.run_id
                or value.producer_node_id != claim.node_id
                or value.producer_attempt_id != claim.attempt_id
            ):
                raise AgentTerminalCommitError(
                    "agent_terminal_artifact_invalid"
                )
            detached.append(value)
        combined = (*detached, manifest_ref)
        if len({ref.sha256 for ref in combined}) != len(combined):
            raise AgentTerminalCommitError(
                "agent_terminal_artifact_invalid"
            )
        return combined

    @staticmethod
    def _validate_provider_result_refs(
        manifest: AgentActivityExecutionManifest,
        refs: tuple[ArtifactRef, ...],
    ) -> None:
        by_identity = {
            hashlib.sha256(
                canonical_json_bytes(ref.to_dict())
            ).hexdigest(): ref
            for ref in refs
        }
        expected = {
            binding.receipt.response_artifact_ref_digest
            for binding in manifest.provider_receipts
        }
        actual = {
            identity
            for identity, ref in by_identity.items()
            if (
                ref.kind is ArtifactKind.MODEL_RESPONSE
                and ref.media_type == "application/octet-stream"
            )
        }
        if actual != expected:
            raise AgentTerminalCommitError(
                "agent_terminal_evidence_incomplete"
            )
        for binding in manifest.provider_receipts:
            receipt = binding.receipt
            ref = by_identity.get(
                receipt.response_artifact_ref_digest or ""
            )
            if (
                ref is None
                or ref.kind is not ArtifactKind.MODEL_RESPONSE
                or ref.media_type != "application/octet-stream"
                or ref.sha256 != receipt.response_digest
                or ref.sensitivity is not receipt.response_sensitivity
            ):
                raise AgentTerminalCommitError(
                    "agent_terminal_evidence_incomplete"
                )

    @staticmethod
    def _node_result(
        *,
        metrics: Mapping[str, Any] | None,
        result_refs: tuple[ArtifactRef, ...],
        tool_receipt_refs: tuple[ArtifactRef, ...],
    ) -> dict[str, JsonValue]:
        try:
            normalized_metrics = _metrics(metrics)
            node_result = normalize_json(
                {
                    "schema_version": 1,
                    "outcome": "succeeded",
                    "artifact_refs": [
                        ref.to_dict() for ref in result_refs
                    ],
                    "tool_receipt_refs": [
                        ref.to_dict() for ref in tool_receipt_refs
                    ],
                    "metrics": normalized_metrics,
                },
                "Agent terminal NodeResult",
            )
            if (
                not isinstance(node_result, dict)
                or len(canonical_json_bytes(normalized_metrics))
                > MAX_AGENT_TERMINAL_METADATA_BYTES
            ):
                raise ValueError
        except (TypeError, ValueError, OverflowError, RecursionError):
            raise AgentTerminalCommitError(
                "agent_terminal_result_invalid"
            ) from None
        return node_result

    @staticmethod
    def _validate_claim(claim: ActivityClaim) -> None:
        if (
            type(claim) is not ActivityClaim
            or claim.activity_kind != "agent"
            or not claim.claim_token
            or claim.fencing_token < 1
            or claim.lease_expires_at <= 0
        ):
            raise AgentTerminalCommitError(
                "agent_terminal_claim_invalid"
            )

    @staticmethod
    def _require_complete_manifest(
        manifest: AgentActivityExecutionManifest,
    ) -> None:
        if (
            manifest.observed_provider_invocations < 1
            or not manifest.has_complete_provider_receipt_lineage
            or not manifest.tool_receipts_complete
            or len(manifest.tool_receipts)
            != manifest.observed_tool_results
        ):
            raise AgentTerminalCommitError(
                "agent_terminal_evidence_incomplete"
            )

    def _durable_receipt(
        self,
        claim: ActivityClaim,
    ) -> AgentActivityReceipt | None:
        try:
            return self.store.get_agent_activity_receipt(
                claim.run_id,
                claim.attempt_id,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return None

    def _fault(self, stage: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(stage)

    def _now(self) -> float:
        try:
            current = float(self._clock())
        except (TypeError, ValueError):
            raise AgentTerminalCommitError(
                "agent_terminal_store_unavailable"
            ) from None
        if not math.isfinite(current) or current < 0:
            raise AgentTerminalCommitError(
                "agent_terminal_store_unavailable"
            )
        return current


def _sensitivity_rank(value: ArtifactSensitivity) -> int:
    return {
        ArtifactSensitivity.PUBLIC: 0,
        ArtifactSensitivity.INTERNAL: 1,
        ArtifactSensitivity.SENSITIVE: 2,
        ArtifactSensitivity.SECRET: 3,
    }[ArtifactSensitivity(value)]


def _metrics(value: Mapping[str, Any] | None) -> dict[str, JsonValue]:
    if value is None:
        return {}
    if not isinstance(value, Mapping) or len(value) > 64:
        raise ValueError
    detached: dict[str, JsonValue] = {}
    for key, item in value.items():
        if (
            type(key) is not str
            or _SAFE_METRIC_NAME.fullmatch(key) is None
            or type(item) not in {bool, int, float}
            or (
                type(item) is float
                and not math.isfinite(item)
            )
        ):
            raise ValueError
        detached[key] = item
    normalized = normalize_json(detached, "Agent terminal metrics")
    if not isinstance(normalized, dict):
        raise ValueError
    return normalized


def _freeze_json(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType(
            {key: _freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


__all__ = [
    "AgentActivityTerminalCommit",
    "AgentTerminalCommitError",
    "DurableAgentTerminalCommitter",
    "MAX_AGENT_TERMINAL_METADATA_BYTES",
]
