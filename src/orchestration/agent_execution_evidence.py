"""Fail-closed receipt collection at the real Agent Loop boundary."""

from __future__ import annotations

import hashlib
import re
import threading
from dataclasses import dataclass, field as dataclass_field
from typing import Any

from .agent_execution_manifest import (
    AgentActivityExecutionManifest,
    AgentExecutionManifestError,
    AgentProviderReceiptBinding,
    AgentToolReceiptBinding,
    MAX_AGENT_EXECUTION_PROVIDER_RECEIPTS,
    MAX_AGENT_EXECUTION_TOOL_RECEIPTS,
    MAX_AGENT_EXECUTION_TURNS,
    agent_tool_operation_key,
)
from .artifacts import ArtifactSensitivity
from .executor import ToolReceipt
from .provider_access import ProviderInvocationReceipt


MAX_EVIDENCE_OBSERVATION_ID_CHARS = 255
_SAFE_REASON = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


class AgentExecutionEvidenceCollectorError(RuntimeError, ValueError):
    """A runtime receipt observation is invalid or out of order."""

    def __init__(self, reason_code: str) -> None:
        if (
            not isinstance(reason_code, str)
            or _SAFE_REASON.fullmatch(reason_code) is None
        ):
            raise ValueError(
                "Agent execution evidence reason code is invalid"
            )
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class _OpenProviderCall:
    sequence: int
    turn: int


@dataclass(frozen=True, slots=True)
class _OpenToolCall:
    sequence: int
    turn: int
    tool_name: str
    tool_call_id: str


@dataclass(frozen=True, slots=True)
class AgentProviderInvocationContext:
    """Collector-assigned provider sequence available during client.chat."""

    sequence: int
    turn: int


@dataclass(frozen=True, slots=True)
class AgentToolInvocationContext:
    """Collector-assigned child operation available during dispatch."""

    sequence: int
    turn: int
    tool_name: str
    tool_call_id: str
    operation_key: str = dataclass_field(repr=False)


class AgentExecutionEvidenceCollector:
    """Collect only explicit typed receipts; missing evidence stays partial.

    The collector implements the optional observer protocol consumed by
    ``run_agent_loop``. Local compatibility clients and handlers return no
    typed receipts, so attaching this collector to them records observations
    but can never claim complete lineage.
    """

    def __init__(
        self,
        *,
        run_id: str,
        node_id: str,
        attempt_id: str,
        request_digest: str,
        request_artifact_digest: str,
        definition_digest: str,
        request_sensitivity: ArtifactSensitivity | str,
    ) -> None:
        try:
            probe = AgentActivityExecutionManifest(
                run_id=run_id,
                node_id=node_id,
                attempt_id=attempt_id,
                request_digest=request_digest,
                request_artifact_digest=request_artifact_digest,
                definition_digest=definition_digest,
                artifact_sensitivity=request_sensitivity,
                exit_reason="ERROR",
                turns=0,
                observed_tool_results=0,
                tool_receipts_complete=True,
                observed_provider_invocations=0,
                provider_receipts_complete=True,
            )
        except AgentExecutionManifestError:
            raise AgentExecutionEvidenceCollectorError(
                "invalid_execution_evidence_parent"
            ) from None
        self._run_id = probe.run_id
        self._node_id = probe.node_id
        self._attempt_id = probe.attempt_id
        self._request_digest = probe.request_digest
        self._request_artifact_digest = (
            probe.request_artifact_digest
        )
        self._definition_digest = probe.definition_digest
        self._request_sensitivity = probe.artifact_sensitivity
        self._lock = threading.RLock()
        self._last_turn = 0
        self._observed_provider_calls = 0
        self._observed_tool_calls = 0
        self._provider_prefix_complete = True
        self._tool_prefix_complete = True
        self._provider_bindings: list[
            AgentProviderReceiptBinding
        ] = []
        self._tool_bindings: list[AgentToolReceiptBinding] = []
        self._open_provider: _OpenProviderCall | None = None
        self._open_tool: _OpenToolCall | None = None
        self._finalized: AgentActivityExecutionManifest | None = None

    def __repr__(self) -> str:
        return (
            "AgentExecutionEvidenceCollector("
            f"run_id={self._run_id!r}, node_id={self._node_id!r}, "
            f"attempt_id={self._attempt_id!r}, "
            "observed_provider_calls="
            f"{self._observed_provider_calls}, "
            f"observed_tool_calls={self._observed_tool_calls}, "
            f"finalized={self._finalized is not None})"
        )

    @property
    def observed_provider_calls(self) -> int:
        with self._lock:
            return self._observed_provider_calls

    @property
    def observed_tool_calls(self) -> int:
        with self._lock:
            return self._observed_tool_calls

    def validate_parent_binding(
        self,
        *,
        run_id: str,
        node_id: str,
        attempt_id: str,
        request_digest: str,
        request_artifact_digest: str,
        definition_digest: str,
        request_sensitivity: ArtifactSensitivity | str,
    ) -> None:
        invalid_sensitivity = False
        try:
            sensitivity = ArtifactSensitivity(request_sensitivity)
        except (TypeError, ValueError):
            invalid_sensitivity = True
            sensitivity = None
        if invalid_sensitivity:
            raise AgentExecutionEvidenceCollectorError(
                "execution_evidence_parent_mismatch"
            ) from None
        with self._lock:
            if (
                run_id != self._run_id
                or node_id != self._node_id
                or attempt_id != self._attempt_id
                or request_digest != self._request_digest
                or request_artifact_digest
                != self._request_artifact_digest
                or definition_digest != self._definition_digest
                or sensitivity is not self._request_sensitivity
            ):
                raise AgentExecutionEvidenceCollectorError(
                    "execution_evidence_parent_mismatch"
                )

    def active_provider_invocation(
        self,
    ) -> AgentProviderInvocationContext:
        with self._lock:
            opened = self._open_provider
            if opened is None:
                raise AgentExecutionEvidenceCollectorError(
                    "provider_execution_observation_unavailable"
                )
            return AgentProviderInvocationContext(
                sequence=opened.sequence,
                turn=opened.turn,
            )

    def active_tool_invocation(
        self,
    ) -> AgentToolInvocationContext:
        with self._lock:
            opened = self._open_tool
            if opened is None:
                raise AgentExecutionEvidenceCollectorError(
                    "tool_execution_observation_unavailable"
                )
            return AgentToolInvocationContext(
                sequence=opened.sequence,
                turn=opened.turn,
                tool_name=opened.tool_name,
                tool_call_id=opened.tool_call_id,
                operation_key=agent_tool_operation_key(
                    run_id=self._run_id,
                    node_id=self._node_id,
                    attempt_id=self._attempt_id,
                    request_digest=self._request_digest,
                    sequence=opened.sequence,
                ),
            )

    def provider_call_started(self, *, turn: int) -> None:
        with self._lock:
            if (
                self._observed_provider_calls
                >= MAX_AGENT_EXECUTION_TURNS
            ):
                raise AgentExecutionEvidenceCollectorError(
                    "provider_execution_observation_capacity"
                )
            current_turn = self._start_observation(turn)
            self._observed_provider_calls += 1
            self._open_provider = _OpenProviderCall(
                sequence=self._observed_provider_calls,
                turn=current_turn,
            )

    def provider_call_finished(
        self,
        *,
        turn: int,
        receipt: object | None,
    ) -> None:
        with self._lock:
            opened = self._close_provider(turn)
            if receipt is None:
                self._provider_prefix_complete = False
                return
            if type(receipt) is not ProviderInvocationReceipt:
                self._provider_prefix_complete = False
                raise AgentExecutionEvidenceCollectorError(
                    "invalid_provider_execution_receipt"
                )
            if (
                opened.sequence
                > MAX_AGENT_EXECUTION_PROVIDER_RECEIPTS
            ):
                self._provider_prefix_complete = False
                return
            try:
                binding = AgentProviderReceiptBinding(
                    sequence=opened.sequence,
                    turn=opened.turn,
                    receipt=receipt,
                )
            except AgentExecutionManifestError:
                self._provider_prefix_complete = False
                raise AgentExecutionEvidenceCollectorError(
                    "invalid_provider_execution_receipt"
                ) from None
            if (
                receipt.run_id != self._run_id
                or receipt.node_id != self._node_id
                or receipt.attempt_id != self._attempt_id
                or receipt.request_digest != self._request_digest
                or receipt.request_artifact_digest
                != self._request_artifact_digest
                or receipt.response_artifact_ref_digest is None
            ):
                self._provider_prefix_complete = False
                raise AgentExecutionEvidenceCollectorError(
                    "provider_execution_receipt_parent_mismatch"
                )
            if self._provider_prefix_complete:
                self._provider_bindings.append(binding)

    def provider_call_failed(self, *, turn: int) -> None:
        with self._lock:
            self._close_provider(turn)
            self._provider_prefix_complete = False

    def tool_call_started(
        self,
        *,
        turn: int,
        tool_name: str,
        tool_call_id: str,
    ) -> None:
        with self._lock:
            if self._observed_tool_calls >= MAX_AGENT_EXECUTION_TURNS:
                raise AgentExecutionEvidenceCollectorError(
                    "tool_execution_observation_capacity"
                )
            name = _bounded_observation_id(
                tool_name,
                "invalid_observed_tool_name",
            )
            call_id = _bounded_observation_id(
                tool_call_id,
                "invalid_observed_tool_call_id",
            )
            current_turn = self._start_observation(turn)
            self._observed_tool_calls += 1
            self._open_tool = _OpenToolCall(
                sequence=self._observed_tool_calls,
                turn=current_turn,
                tool_name=name,
                tool_call_id=call_id,
            )

    def tool_call_finished(
        self,
        *,
        turn: int,
        tool_name: str,
        tool_call_id: str,
        receipt: object | None,
    ) -> None:
        with self._lock:
            opened = self._close_tool(
                turn,
                tool_name,
                tool_call_id,
            )
            if receipt is None:
                self._tool_prefix_complete = False
                return
            if type(receipt) is not ToolReceipt:
                self._tool_prefix_complete = False
                raise AgentExecutionEvidenceCollectorError(
                    "invalid_tool_execution_receipt"
                )
            if opened.sequence > MAX_AGENT_EXECUTION_TOOL_RECEIPTS:
                self._tool_prefix_complete = False
                return
            try:
                binding = AgentToolReceiptBinding.from_receipt(
                    receipt,
                    sequence=opened.sequence,
                    turn=opened.turn,
                )
                expected_operation_digest = hashlib.sha256(
                    agent_tool_operation_key(
                        run_id=self._run_id,
                        node_id=self._node_id,
                        attempt_id=self._attempt_id,
                        request_digest=self._request_digest,
                        sequence=opened.sequence,
                    ).encode("utf-8")
                ).hexdigest()
            except AgentExecutionManifestError:
                self._tool_prefix_complete = False
                raise AgentExecutionEvidenceCollectorError(
                    "invalid_tool_execution_receipt"
                ) from None
            if (
                receipt.tool_name != opened.tool_name
                or receipt.run_id != self._run_id
                or receipt.operation_key_digest
                != expected_operation_digest
                or receipt.idempotency_key_digest
                != expected_operation_digest
            ):
                self._tool_prefix_complete = False
                raise AgentExecutionEvidenceCollectorError(
                    "tool_execution_receipt_parent_mismatch"
                )
            if self._tool_prefix_complete:
                self._tool_bindings.append(binding)

    def tool_call_failed(
        self,
        *,
        turn: int,
        tool_name: str,
        tool_call_id: str,
    ) -> None:
        with self._lock:
            self._close_tool(turn, tool_name, tool_call_id)
            self._tool_prefix_complete = False

    def finalize(
        self,
        *,
        exit_reason: str,
        turns: int,
    ) -> AgentActivityExecutionManifest:
        with self._lock:
            final_exit_reason = _exit_reason(exit_reason)
            final_turns = _final_turns(turns, self._last_turn)
            if self._open_provider is not None or self._open_tool is not None:
                raise AgentExecutionEvidenceCollectorError(
                    "execution_evidence_observation_in_progress"
                )
            if self._finalized is not None:
                if (
                    self._finalized.exit_reason
                    == final_exit_reason
                    and self._finalized.turns == final_turns
                ):
                    return self._finalized
                raise AgentExecutionEvidenceCollectorError(
                    "execution_evidence_finalization_conflict"
                )
            sensitivity = self._manifest_sensitivity()
            try:
                manifest = AgentActivityExecutionManifest(
                    run_id=self._run_id,
                    node_id=self._node_id,
                    attempt_id=self._attempt_id,
                    request_digest=self._request_digest,
                    request_artifact_digest=(
                        self._request_artifact_digest
                    ),
                    definition_digest=self._definition_digest,
                    artifact_sensitivity=sensitivity,
                    exit_reason=final_exit_reason,
                    turns=final_turns,
                    observed_tool_results=(
                        self._observed_tool_calls
                    ),
                    tool_receipts_complete=(
                        self._tool_prefix_complete
                        and len(self._tool_bindings)
                        == self._observed_tool_calls
                    ),
                    tool_receipts=tuple(self._tool_bindings),
                    observed_provider_invocations=(
                        self._observed_provider_calls
                    ),
                    provider_receipts_complete=(
                        self._provider_prefix_complete
                        and len(self._provider_bindings)
                        == self._observed_provider_calls
                    ),
                    provider_receipts=tuple(
                        self._provider_bindings
                    ),
                )
            except AgentExecutionManifestError:
                raise AgentExecutionEvidenceCollectorError(
                    "execution_evidence_manifest_invalid"
                ) from None
            self._finalized = manifest
            return manifest

    def _start_observation(self, turn: int) -> int:
        if self._finalized is not None:
            raise AgentExecutionEvidenceCollectorError(
                "execution_evidence_already_finalized"
            )
        current_turn = _turn(turn)
        if (
            self._open_provider is not None
            or self._open_tool is not None
        ):
            raise AgentExecutionEvidenceCollectorError(
                "execution_evidence_observation_overlap"
            )
        if current_turn < self._last_turn:
            raise AgentExecutionEvidenceCollectorError(
                "execution_evidence_turn_regression"
            )
        self._last_turn = current_turn
        return current_turn

    def _close_provider(self, turn: int) -> _OpenProviderCall:
        opened = self._open_provider
        if (
            opened is None
            or opened.turn != _turn(turn)
        ):
            raise AgentExecutionEvidenceCollectorError(
                "provider_execution_observation_mismatch"
            )
        self._open_provider = None
        return opened

    def _close_tool(
        self,
        turn: int,
        tool_name: str,
        tool_call_id: str,
    ) -> _OpenToolCall:
        opened = self._open_tool
        if (
            opened is None
            or opened.turn != _turn(turn)
            or opened.tool_name != tool_name
            or opened.tool_call_id != tool_call_id
        ):
            raise AgentExecutionEvidenceCollectorError(
                "tool_execution_observation_mismatch"
            )
        self._open_tool = None
        return opened

    def _manifest_sensitivity(self) -> ArtifactSensitivity:
        if not self._provider_prefix_complete:
            return ArtifactSensitivity.SECRET
        if any(
            item.receipt.response_sensitivity
            is ArtifactSensitivity.SECRET
            for item in self._provider_bindings
        ):
            return ArtifactSensitivity.SECRET
        return self._request_sensitivity


def _turn(value: Any) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= 1_000_000
    ):
        raise AgentExecutionEvidenceCollectorError(
            "invalid_execution_evidence_turn"
        )
    return value


def _final_turns(value: Any, last_turn: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not last_turn <= value <= 1_000_000
    ):
        raise AgentExecutionEvidenceCollectorError(
            "invalid_execution_evidence_final_turns"
        )
    return value


def _exit_reason(value: Any) -> str:
    if (
        not isinstance(value, str)
        or _SAFE_REASON.fullmatch(value) is None
    ):
        raise AgentExecutionEvidenceCollectorError(
            "invalid_execution_evidence_exit_reason"
        )
    return value


def _bounded_observation_id(value: Any, reason_code: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > MAX_EVIDENCE_OBSERVATION_ID_CHARS
        or any(
            ord(character) < 32 or ord(character) == 127
            for character in value
        )
    ):
        raise AgentExecutionEvidenceCollectorError(reason_code)
    return value


__all__ = [
    "AgentExecutionEvidenceCollector",
    "AgentExecutionEvidenceCollectorError",
    "AgentProviderInvocationContext",
    "AgentToolInvocationContext",
    "MAX_EVIDENCE_OBSERVATION_ID_CHARS",
]
