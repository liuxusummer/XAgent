"""Exact, immutable recovery state for one closed Agent Loop turn."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from .agent_execution_manifest import (
    AgentActivityExecutionManifest,
    AgentExecutionManifestError,
    MAX_AGENT_EXECUTION_PROVIDER_RECEIPTS,
)
from .artifacts import (
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    ArtifactStore,
    canonical_json_bytes,
)


AGENT_TURN_CHECKPOINT_SCHEMA_VERSION = 1
AGENT_TURN_CHECKPOINT_MEDIA_TYPE = (
    "application/vnd.xagent.agent-turn-checkpoint+json"
)
MAX_AGENT_TURN_CHECKPOINT_BYTES = 16 * 1024 * 1024
MAX_AGENT_TURN = MAX_AGENT_EXECUTION_PROVIDER_RECEIPTS
_SCHEMA_NAME = "agent_turn_checkpoint_v1"
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REASON = re.compile(r"^[a-z0-9][a-z0-9_.:-]{0,127}$")
_USAGE_FIELDS = frozenset(
    {
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "reasoning_tokens",
    }
)
_LOOP_STATE_FIELDS = frozenset(
    {
        "schema_version",
        "completed_turn",
        "next_messages",
        "final_response",
        "tool_results",
        "usage",
        "context",
    }
)
_CONTEXT_FIELDS = frozenset(
    {
        "working",
        "history_info",
        "done_hooks",
        "empty_count",
        "active_skills",
        "context_state",
        "pending_approval",
        "last_policy_decision",
        "session_id",
        "agent_name",
        "principal_digest",
        "principal_boundary_digest",
    }
)


class AgentTurnCheckpointError(RuntimeError, ValueError):
    """A turn checkpoint is malformed, unbound, or unavailable."""

    def __init__(self, reason_code: str) -> None:
        if not isinstance(reason_code, str) or _REASON.fullmatch(reason_code) is None:
            raise ValueError("Agent turn checkpoint reason code is invalid")
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True, repr=False)
class AgentTurnCheckpoint:
    """Canonical state sufficient to continue immediately before a provider call."""

    run_id: str
    node_id: str
    attempt_id: str
    request_digest: str
    request_artifact_digest: str
    definition_digest: str
    runtime_configuration_digest: str
    completed_turn: int
    previous_checkpoint_digest: str | None
    loop_state: Mapping[str, Any] = field(repr=False)
    provider_state: Mapping[str, Any] = field(repr=False)
    evidence_manifest: AgentActivityExecutionManifest = field(repr=False)
    artifact_sensitivity: ArtifactSensitivity = ArtifactSensitivity.SENSITIVE
    schema_version: int = AGENT_TURN_CHECKPOINT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        try:
            for field_name in ("run_id", "node_id", "attempt_id"):
                object.__setattr__(
                    self,
                    field_name,
                    _identifier(getattr(self, field_name)),
                )
            for field_name in (
                "request_digest",
                "request_artifact_digest",
                "definition_digest",
                "runtime_configuration_digest",
            ):
                object.__setattr__(
                    self,
                    field_name,
                    _digest(getattr(self, field_name)),
                )
            previous = self.previous_checkpoint_digest
            if previous is not None:
                previous = _digest(previous)
            object.__setattr__(self, "previous_checkpoint_digest", previous)
            if (
                self.schema_version != AGENT_TURN_CHECKPOINT_SCHEMA_VERSION
                or type(self.completed_turn) is not int
                or not 1 <= self.completed_turn <= MAX_AGENT_TURN
            ):
                raise ValueError
            sensitivity = ArtifactSensitivity(self.artifact_sensitivity)
            if sensitivity not in {
                ArtifactSensitivity.SENSITIVE,
                ArtifactSensitivity.SECRET,
            }:
                raise ValueError
            object.__setattr__(self, "artifact_sensitivity", sensitivity)
            manifest = self.evidence_manifest
            if (
                type(manifest) is not AgentActivityExecutionManifest
                or manifest.exit_reason != "CHECKPOINT"
                or manifest.turns != self.completed_turn
                or manifest.run_id != self.run_id
                or manifest.node_id != self.node_id
                or manifest.attempt_id != self.attempt_id
                or manifest.request_digest != self.request_digest
                or manifest.request_artifact_digest
                != self.request_artifact_digest
                or manifest.definition_digest != self.definition_digest
                or manifest.artifact_sensitivity is not sensitivity
                or not manifest.tool_receipts_complete
                or not manifest.has_complete_provider_receipt_lineage
                or manifest.observed_provider_invocations
                != self.completed_turn
            ):
                raise ValueError
            loop_state = _canonical_object(self.loop_state)
            provider_state = _canonical_object(self.provider_state)
            _validate_loop_state(loop_state, completed_turn=self.completed_turn)
            if provider_state.get("request_count") != self.completed_turn:
                raise ValueError
            object.__setattr__(self, "loop_state", loop_state)
            object.__setattr__(self, "provider_state", provider_state)
            self.to_bytes()
        except (KeyboardInterrupt, SystemExit):
            raise
        except AgentTurnCheckpointError:
            raise
        except (AgentExecutionManifestError, TypeError, ValueError, OverflowError, RecursionError):
            raise AgentTurnCheckpointError("invalid_agent_turn_checkpoint") from None

    def __repr__(self) -> str:
        return (
            "AgentTurnCheckpoint("
            f"run_id={self.run_id!r}, node_id={self.node_id!r}, "
            f"attempt_id={self.attempt_id!r}, "
            f"completed_turn={self.completed_turn}, "
            f"checkpoint_digest={self.checkpoint_digest!r})"
        )

    @property
    def checkpoint_digest(self) -> str:
        return hashlib.sha256(self.to_bytes()).hexdigest()

    @property
    def dependency_artifact_refs(self) -> tuple[ArtifactRef, ...]:
        refs = self.provider_state.get("result_artifact_refs")
        if type(refs) is not list:
            raise AgentTurnCheckpointError("invalid_agent_turn_checkpoint")
        try:
            return tuple(ArtifactRef.from_dict(item) for item in refs)
        except (TypeError, ValueError):
            raise AgentTurnCheckpointError("invalid_agent_turn_checkpoint") from None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": "agent_turn_checkpoint",
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "request_digest": self.request_digest,
            "request_artifact_digest": self.request_artifact_digest,
            "definition_digest": self.definition_digest,
            "runtime_configuration_digest": self.runtime_configuration_digest,
            "completed_turn": self.completed_turn,
            "previous_checkpoint_digest": self.previous_checkpoint_digest,
            "artifact_sensitivity": self.artifact_sensitivity.value,
            "loop_state": dict(self.loop_state),
            "provider_state": dict(self.provider_state),
            "evidence_manifest": self.evidence_manifest.to_dict(),
        }

    def to_bytes(self) -> bytes:
        try:
            content = canonical_json_bytes(self.to_dict())
        except (TypeError, ValueError, OverflowError, RecursionError):
            raise AgentTurnCheckpointError("invalid_agent_turn_checkpoint") from None
        if not content or len(content) > MAX_AGENT_TURN_CHECKPOINT_BYTES:
            raise AgentTurnCheckpointError("agent_turn_checkpoint_too_large")
        return content

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "AgentTurnCheckpoint":
        required = {
            "schema_version",
            "kind",
            "run_id",
            "node_id",
            "attempt_id",
            "request_digest",
            "request_artifact_digest",
            "definition_digest",
            "runtime_configuration_digest",
            "completed_turn",
            "previous_checkpoint_digest",
            "artifact_sensitivity",
            "loop_state",
            "provider_state",
            "evidence_manifest",
        }
        if (
            not isinstance(payload, Mapping)
            or set(payload) != required
            or payload.get("kind") != "agent_turn_checkpoint"
        ):
            raise AgentTurnCheckpointError("invalid_agent_turn_checkpoint")
        try:
            manifest = AgentActivityExecutionManifest.from_dict(
                payload["evidence_manifest"]
            )
            return cls(
                schema_version=payload["schema_version"],
                run_id=payload["run_id"],
                node_id=payload["node_id"],
                attempt_id=payload["attempt_id"],
                request_digest=payload["request_digest"],
                request_artifact_digest=payload["request_artifact_digest"],
                definition_digest=payload["definition_digest"],
                runtime_configuration_digest=payload[
                    "runtime_configuration_digest"
                ],
                completed_turn=payload["completed_turn"],
                previous_checkpoint_digest=payload["previous_checkpoint_digest"],
                artifact_sensitivity=payload["artifact_sensitivity"],
                loop_state=payload["loop_state"],
                provider_state=payload["provider_state"],
                evidence_manifest=manifest,
            )
        except AgentTurnCheckpointError:
            raise
        except (AgentExecutionManifestError, TypeError, ValueError):
            raise AgentTurnCheckpointError("invalid_agent_turn_checkpoint") from None

    @classmethod
    def from_bytes(cls, content: bytes) -> "AgentTurnCheckpoint":
        if (
            not isinstance(content, bytes)
            or not content
            or len(content) > MAX_AGENT_TURN_CHECKPOINT_BYTES
        ):
            raise AgentTurnCheckpointError("invalid_agent_turn_checkpoint_payload")
        try:
            payload = json.loads(
                content.decode("utf-8"),
                parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
            )
            checkpoint = cls.from_dict(payload)
        except AgentTurnCheckpointError:
            raise
        except (TypeError, ValueError, UnicodeError, RecursionError):
            raise AgentTurnCheckpointError("invalid_agent_turn_checkpoint_payload") from None
        if checkpoint.to_bytes() != content:
            raise AgentTurnCheckpointError("noncanonical_agent_turn_checkpoint_payload")
        return checkpoint


class AgentTurnCheckpointArtifactStore:
    """Stage and load verified, content-addressed Agent turn checkpoints."""

    def __init__(self, store: ArtifactStore) -> None:
        if not isinstance(store, ArtifactStore):
            raise AgentTurnCheckpointError("invalid_agent_turn_checkpoint_store")
        self._store = store

    def stage(self, checkpoint: AgentTurnCheckpoint) -> ArtifactRef:
        if type(checkpoint) is not AgentTurnCheckpoint:
            raise AgentTurnCheckpointError("invalid_agent_turn_checkpoint")
        try:
            ref = self._store.put_bytes(
                checkpoint.to_bytes(),
                media_type=AGENT_TURN_CHECKPOINT_MEDIA_TYPE,
                kind=ArtifactKind.AGENT_TURN_CHECKPOINT,
                sensitivity=checkpoint.artifact_sensitivity,
                producer_run_id=checkpoint.run_id,
                producer_node_id=checkpoint.node_id,
                producer_attempt_id=checkpoint.attempt_id,
                metadata={"schema": _SCHEMA_NAME},
            )
            self.validate_ref(ref, checkpoint=checkpoint)
            verified = self._store.verify(ref)
        except (KeyboardInterrupt, SystemExit):
            raise
        except AgentTurnCheckpointError:
            raise
        except BaseException:
            raise AgentTurnCheckpointError("agent_turn_checkpoint_write_failed") from None
        if verified is not True:
            raise AgentTurnCheckpointError("agent_turn_checkpoint_verify_failed")
        return ref

    def load(self, ref: ArtifactRef) -> AgentTurnCheckpoint:
        self.validate_ref(ref)
        try:
            if self._store.verify(ref) is not True:
                raise AgentTurnCheckpointError("agent_turn_checkpoint_verify_failed")
            content = self._store.read(ref)
        except (KeyboardInterrupt, SystemExit):
            raise
        except AgentTurnCheckpointError:
            raise
        except BaseException:
            raise AgentTurnCheckpointError("agent_turn_checkpoint_read_failed") from None
        if (
            not isinstance(content, bytes)
            or len(content) != ref.size
            or hashlib.sha256(content).hexdigest() != ref.sha256
        ):
            raise AgentTurnCheckpointError("agent_turn_checkpoint_integrity_failed")
        checkpoint = AgentTurnCheckpoint.from_bytes(content)
        self.validate_ref(ref, checkpoint=checkpoint)
        return checkpoint

    @staticmethod
    def validate_ref(
        ref: ArtifactRef,
        *,
        checkpoint: AgentTurnCheckpoint | None = None,
    ) -> None:
        if (
            type(ref) is not ArtifactRef
            or ref.kind is not ArtifactKind.AGENT_TURN_CHECKPOINT
            or ref.media_type != AGENT_TURN_CHECKPOINT_MEDIA_TYPE
            or ref.sensitivity
            not in {ArtifactSensitivity.SENSITIVE, ArtifactSensitivity.SECRET}
            or not 0 < ref.size <= MAX_AGENT_TURN_CHECKPOINT_BYTES
            or dict(ref.metadata) != {"schema": _SCHEMA_NAME}
        ):
            raise AgentTurnCheckpointError("invalid_agent_turn_checkpoint_artifact")
        if checkpoint is not None and (
            ref.sha256 != checkpoint.checkpoint_digest
            or ref.size != len(checkpoint.to_bytes())
            or ref.sensitivity is not checkpoint.artifact_sensitivity
            or ref.producer_run_id != checkpoint.run_id
            or ref.producer_node_id != checkpoint.node_id
            or ref.producer_attempt_id != checkpoint.attempt_id
        ):
            raise AgentTurnCheckpointError("agent_turn_checkpoint_artifact_binding_mismatch")


def _identifier(value: Any) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError
    return value


def _digest(value: Any) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError
    return value


def _canonical_object(value: Any) -> dict[str, Any]:
    detached = json.loads(canonical_json_bytes(value).decode("utf-8"))
    if type(detached) is not dict:
        raise ValueError
    return detached


def _validate_loop_state(state: dict[str, Any], *, completed_turn: int) -> None:
    context = state.get("context")
    usage = state.get("usage")
    if (
        set(state) != _LOOP_STATE_FIELDS
        or state.get("schema_version") != 1
        or state.get("completed_turn") != completed_turn
        or type(state.get("next_messages")) is not list
        or not state["next_messages"]
        or any(type(message) is not dict for message in state["next_messages"])
        or type(state.get("final_response")) is not str
        or type(state.get("tool_results")) is not list
        or any(type(item) is not dict for item in state["tool_results"])
        or type(usage) is not dict
        or set(usage).difference(_USAGE_FIELDS)
        or any(type(value) is not int or value < 0 for value in usage.values())
        or type(context) is not dict
        or set(context) != _CONTEXT_FIELDS
        or type(context.get("working")) is not dict
        or any(
            type(key) is not str or type(value) is not str
            for key, value in context["working"].items()
        )
        or any(
            type(context.get(field_name)) is not list
            or any(type(item) is not str for item in context[field_name])
            for field_name in ("history_info", "done_hooks", "active_skills")
        )
        or type(context.get("empty_count")) is not int
        or context["empty_count"] < 0
        or type(context.get("context_state")) is not dict
        or context.get("pending_approval") is not None
        and type(context["pending_approval"]) is not dict
        or type(context.get("last_policy_decision")) is not dict
        or type(context.get("session_id")) is not str
        or type(context.get("agent_name")) is not str
        or any(
            value != "" and _DIGEST.fullmatch(value) is None
            for value in (
                context.get("principal_digest"),
                context.get("principal_boundary_digest"),
            )
            if isinstance(value, str)
        )
        or any(
            not isinstance(context.get(field_name), str)
            for field_name in (
                "principal_digest",
                "principal_boundary_digest",
            )
        )
    ):
        raise ValueError


__all__ = [
    "AGENT_TURN_CHECKPOINT_MEDIA_TYPE",
    "AGENT_TURN_CHECKPOINT_SCHEMA_VERSION",
    "AgentTurnCheckpoint",
    "AgentTurnCheckpointArtifactStore",
    "AgentTurnCheckpointError",
    "MAX_AGENT_TURN_CHECKPOINT_BYTES",
]
