"""Deterministic, sensitive Artifact boundary for one Agent Activity request."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from .artifact_broker import (
    MAX_BROKER_ARTIFACT_BYTES,
    ArtifactDescriptor,
    ArtifactGrantDenied,
)
from .artifacts import (
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    ArtifactStore,
    canonical_json_bytes,
)
from .models import AttemptRecord, AttemptStatus
from .scheduler import ActivityAdmissionCandidate, ActivityClaim

AGENT_ACTIVITY_REQUEST_SCHEMA_VERSION = 1
AGENT_ACTIVITY_REQUEST_MEDIA_TYPE = (
    "application/vnd.xagent.agent-activity-request+json"
)
MAX_AGENT_ACTIVITY_REQUEST_BYTES = 256 * 1024
# One of the remote protocol's 64 input grants is reserved for this request.
MAX_AGENT_ACTIVITY_INPUT_REFS = 63
MAX_AGENT_ACTIVITY_TEXT_CHARS = 64 * 1024
_SCHEMA_NAME = "agent_activity_request_v1"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$")
_SHORT_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class AgentActivityRequestError(RuntimeError, ValueError):
    """A request Artifact is malformed, unsafe, or misbound."""

    def __init__(self, reason_code: str) -> None:
        if (
            not isinstance(reason_code, str)
            or _SHORT_IDENTIFIER.fullmatch(reason_code) is None
        ):
            raise ValueError("Agent request error code is invalid")
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class AgentActivityInputBinding:
    """Logical input name and its exact ordered path-free descriptors."""

    name: str
    artifacts: tuple[ArtifactDescriptor, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "name",
            _bounded_code(
                self.name,
                "invalid_input_binding_name",
                maximum=63,
            ),
        )
        artifacts = tuple(self.artifacts)
        if (
            not 1 <= len(artifacts) <= MAX_AGENT_ACTIVITY_INPUT_REFS
            or not all(
                isinstance(item, ArtifactDescriptor)
                for item in artifacts
            )
            or any(
                item.size > MAX_BROKER_ARTIFACT_BYTES
                for item in artifacts
            )
        ):
            raise AgentActivityRequestError(
                "invalid_input_binding_artifacts"
            )
        object.__setattr__(self, "artifacts", artifacts)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "artifacts": [item.to_dict() for item in self.artifacts],
        }

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> "AgentActivityInputBinding":
        if (
            not isinstance(payload, Mapping)
            or set(payload) != {"name", "artifacts"}
            or not isinstance(payload.get("artifacts"), list)
            or not (
                1
                <= len(payload["artifacts"])
                <= MAX_AGENT_ACTIVITY_INPUT_REFS
            )
        ):
            raise AgentActivityRequestError("invalid_input_binding")
        return cls(
            name=payload["name"],
            artifacts=tuple(
                _input_descriptor(item)
                for item in payload["artifacts"]
            ),
        )


@dataclass(frozen=True, slots=True, repr=False)
class AgentActivityRequest:
    """Canonical Agent request content; raw instructions never enter repr."""

    run_id: str
    node_id: str
    attempt_id: str
    attempt_number: int
    agent_name: str
    request_digest: str
    definition_digest: str
    task: str | None = field(default=None, repr=False)
    context: str | None = field(default=None, repr=False)
    expected_output: str | None = field(default=None, repr=False)
    output: str | None = field(default=None, repr=False)
    input_bindings: tuple[AgentActivityInputBinding, ...] = ()
    schema_version: int = AGENT_ACTIVITY_REQUEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in ("run_id", "node_id", "attempt_id"):
            object.__setattr__(
                self,
                field_name,
                _identifier(
                    getattr(self, field_name),
                    f"invalid_{field_name}",
                ),
            )
        if (
            isinstance(self.attempt_number, bool)
            or not isinstance(self.attempt_number, int)
            or not 1 <= self.attempt_number <= 1_000_000
        ):
            raise AgentActivityRequestError("invalid_attempt_number")
        object.__setattr__(
            self,
            "agent_name",
            _bounded_code(
                self.agent_name,
                "invalid_agent_name",
                maximum=63,
            ),
        )
        for field_name in (
            "request_digest",
            "definition_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _digest(
                    getattr(self, field_name),
                    f"invalid_{field_name}",
                ),
            )
        for field_name in (
            "task",
            "context",
            "expected_output",
            "output",
        ):
            object.__setattr__(
                self,
                field_name,
                _optional_instruction(
                    getattr(self, field_name),
                    f"invalid_{field_name}",
                ),
            )
        bindings = tuple(self.input_bindings)
        if (
            len(bindings) > MAX_AGENT_ACTIVITY_INPUT_REFS
            or not all(
                isinstance(item, AgentActivityInputBinding)
                for item in bindings
            )
            or tuple(item.name for item in bindings)
            != tuple(sorted(item.name for item in bindings))
            or len({item.name for item in bindings}) != len(bindings)
            or sum(len(item.artifacts) for item in bindings)
            > MAX_AGENT_ACTIVITY_INPUT_REFS
        ):
            raise AgentActivityRequestError("invalid_input_bindings")
        object.__setattr__(self, "input_bindings", bindings)
        if self.task is None and self.context is None and not bindings:
            raise AgentActivityRequestError(
                "agent_request_has_no_instruction"
            )
        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version
            != AGENT_ACTIVITY_REQUEST_SCHEMA_VERSION
        ):
            raise AgentActivityRequestError(
                "unsupported_agent_request_schema"
            )
        self.to_bytes()

    def __repr__(self) -> str:
        return (
            "AgentActivityRequest("
            f"run_id={self.run_id!r}, node_id={self.node_id!r}, "
            f"attempt_id={self.attempt_id!r}, agent_name={self.agent_name!r}, "
            f"request_digest={self.request_digest!r}, "
            f"input_binding_count={len(self.input_bindings)})"
        )

    @property
    def artifact_digest(self) -> str:
        return hashlib.sha256(self.to_bytes()).hexdigest()

    @property
    def artifact_sensitivity(self) -> ArtifactSensitivity:
        """Conservatively inherit the highest referenced sensitivity."""

        values = (
            ArtifactSensitivity.SENSITIVE,
            *(
                descriptor.sensitivity
                for binding in self.input_bindings
                for descriptor in binding.artifacts
            ),
        )
        return max(values, key=_sensitivity_rank)

    @property
    def config_digest(self) -> str:
        return hashlib.sha256(
            canonical_json_bytes(self._config_dict())
        ).hexdigest()

    def validate_runtime_binding(
        self,
        *,
        run_id: str,
        node_id: str,
        attempt_id: str,
        attempt_number: int,
        activity_kind: str,
        activity_request_digest: str,
        activity_name: str,
        config_digest: str,
    ) -> None:
        """Fail closed against the path-free fields carried by an assignment."""

        if (
            run_id != self.run_id
            or node_id != self.node_id
            or attempt_id != self.attempt_id
            or isinstance(attempt_number, bool)
            or attempt_number != self.attempt_number
            or activity_kind != "agent"
            or activity_request_digest != self.request_digest
            or activity_name != self.agent_name
            or config_digest != self.config_digest
        ):
            raise AgentActivityRequestError(
                "agent_request_runtime_binding_mismatch"
            )

    def validate_grant_descriptors(
        self,
        descriptors: tuple[ArtifactDescriptor, ...],
    ) -> None:
        """Bind one request grant plus the exact deduplicated context grants."""

        if not isinstance(descriptors, tuple):
            raise AgentActivityRequestError(
                "agent_request_grant_binding_mismatch"
            )
        values = descriptors
        if (
            not 1 <= len(values) <= MAX_AGENT_ACTIVITY_INPUT_REFS + 1
            or not all(
                isinstance(item, ArtifactDescriptor)
                for item in values
            )
        ):
            raise AgentActivityRequestError(
                "agent_request_grant_binding_mismatch"
            )
        request_descriptors = tuple(
            item
            for item in values
            if (
                item.kind == ArtifactKind.AGENT_REQUEST.value
                and item.sha256 == self.artifact_digest
                and item.size == len(self.to_bytes())
                and item.media_type
                == AGENT_ACTIVITY_REQUEST_MEDIA_TYPE
                and item.sensitivity
                is self.artifact_sensitivity
            )
        )
        if len(request_descriptors) != 1:
            raise AgentActivityRequestError(
                "agent_request_grant_binding_mismatch"
            )
        context = tuple(
            item for item in values if item is not request_descriptors[0]
        )
        actual = {
            canonical_json_bytes(item.to_dict()) for item in context
        }
        expected = {
            canonical_json_bytes(item.to_dict())
            for binding in self.input_bindings
            for item in binding.artifacts
        }
        if len(actual) != len(context) or actual != expected:
            raise AgentActivityRequestError(
                "agent_request_grant_binding_mismatch"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": "agent_activity_request",
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "attempt_number": self.attempt_number,
            "agent_name": self.agent_name,
            "request_digest": self.request_digest,
            "definition_digest": self.definition_digest,
            "task": self.task,
            "context": self.context,
            "expected_output": self.expected_output,
            "output": self.output,
            "input_bindings": [
                binding.to_dict() for binding in self.input_bindings
            ],
        }

    def to_bytes(self) -> bytes:
        try:
            content = canonical_json_bytes(self.to_dict())
        except (
            TypeError,
            ValueError,
            OverflowError,
            RecursionError,
        ):
            raise AgentActivityRequestError(
                "agent_request_not_canonical"
            ) from None
        if len(content) > MAX_AGENT_ACTIVITY_REQUEST_BYTES:
            raise AgentActivityRequestError("agent_request_too_large")
        return content

    def _config_dict(self) -> dict[str, str]:
        config = {"agent": self.agent_name}
        for field_name in (
            "task",
            "context",
            "expected_output",
            "output",
        ):
            value = getattr(self, field_name)
            if value is not None:
                config[field_name] = value
        return config

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> "AgentActivityRequest":
        required = {
            "schema_version",
            "kind",
            "run_id",
            "node_id",
            "attempt_id",
            "attempt_number",
            "agent_name",
            "request_digest",
            "definition_digest",
            "task",
            "context",
            "expected_output",
            "output",
            "input_bindings",
        }
        if (
            not isinstance(payload, Mapping)
            or set(payload) != required
            or payload.get("kind") != "agent_activity_request"
            or not isinstance(payload.get("input_bindings"), list)
            or len(payload["input_bindings"])
            > MAX_AGENT_ACTIVITY_INPUT_REFS
        ):
            raise AgentActivityRequestError(
                "invalid_agent_request_schema"
            )
        return cls(
            schema_version=payload["schema_version"],
            run_id=payload["run_id"],
            node_id=payload["node_id"],
            attempt_id=payload["attempt_id"],
            attempt_number=payload["attempt_number"],
            agent_name=payload["agent_name"],
            request_digest=payload["request_digest"],
            definition_digest=payload["definition_digest"],
            task=payload["task"],
            context=payload["context"],
            expected_output=payload["expected_output"],
            output=payload["output"],
            input_bindings=tuple(
                AgentActivityInputBinding.from_dict(item)
                for item in payload["input_bindings"]
            ),
        )

    @classmethod
    def from_bytes(cls, content: bytes) -> "AgentActivityRequest":
        if (
            not isinstance(content, bytes)
            or not content
            or len(content) > MAX_AGENT_ACTIVITY_REQUEST_BYTES
        ):
            raise AgentActivityRequestError(
                "invalid_agent_request_payload"
            )
        try:
            payload = json.loads(content.decode("utf-8"))
            request = cls.from_dict(payload)
        except AgentActivityRequestError:
            raise
        except (
            TypeError,
            ValueError,
            UnicodeError,
            RecursionError,
        ):
            raise AgentActivityRequestError(
                "invalid_agent_request_payload"
            ) from None
        if request.to_bytes() != content:
            raise AgentActivityRequestError(
                "noncanonical_agent_request_payload"
            )
        return request

    @classmethod
    def from_candidate(
        cls,
        candidate: ActivityAdmissionCandidate,
    ) -> "AgentActivityRequest":
        if not isinstance(candidate, ActivityAdmissionCandidate):
            raise AgentActivityRequestError(
                "invalid_activity_candidate"
            )
        claim = candidate.claim
        attempt = candidate.attempt
        if (
            not isinstance(claim, ActivityClaim)
            or not isinstance(attempt, AttemptRecord)
        ):
            raise AgentActivityRequestError(
                "invalid_activity_candidate"
            )
        try:
            config = dict(claim.config)
        except Exception:
            raise AgentActivityRequestError(
                "invalid_agent_candidate_config"
            ) from None
        allowed_config = {
            "agent",
            "task",
            "context",
            "expected_output",
            "output",
        }
        if (
            claim.activity_kind != "agent"
            or attempt.activity_kind != "agent"
            or claim.run_id != attempt.run_id
            or claim.node_id != attempt.node_id
            or claim.attempt_id != attempt.attempt_id
            or claim.attempt_number != attempt.attempt_number
            or claim.effect_class != attempt.effect_class
            or claim.request_hash
            != attempt.metadata.get("request_hash")
            or candidate.definition_digest
            != attempt.metadata.get("definition_digest")
            or claim.operation_key
            != attempt.metadata.get("operation_key")
            or claim.idempotency_key != claim.operation_key
            or claim.claim_key != attempt.idempotency_key
            or attempt.status is not AttemptStatus.SCHEDULED
            or attempt.worker_id is not None
            or attempt.lease_id is not None
            or attempt.fencing_token != 0
            or attempt.result is not None
            or attempt.error is not None
            or attempt.started_at is not None
            or attempt.finished_at is not None
            or claim.claim_token != ""
            or isinstance(claim.fencing_token, bool)
            or claim.fencing_token != 0
            or isinstance(claim.lease_expires_at, bool)
            or claim.lease_expires_at != 0.0
            or set(config).difference(allowed_config)
            or not isinstance(config.get("agent"), str)
        ):
            raise AgentActivityRequestError(
                "agent_candidate_binding_mismatch"
            )
        raw_bindings = claim.input_artifact_bindings
        raw_refs = claim.input_artifact_refs
        if (
            not isinstance(raw_bindings, tuple)
            or len(raw_bindings) > MAX_AGENT_ACTIVITY_INPUT_REFS
            or not isinstance(raw_refs, tuple)
            or len(raw_refs) > MAX_AGENT_ACTIVITY_INPUT_REFS
            or not all(isinstance(ref, ArtifactRef) for ref in raw_refs)
            or not all(
                isinstance(item, tuple)
                and len(item) == 2
                and isinstance(item[0], str)
                and isinstance(item[1], tuple)
                and 1 <= len(item[1]) <= MAX_AGENT_ACTIVITY_INPUT_REFS
                and all(
                    isinstance(ref, ArtifactRef)
                    for ref in item[1]
                )
                for item in raw_bindings
            )
            or sum(len(item[1]) for item in raw_bindings)
            > MAX_AGENT_ACTIVITY_INPUT_REFS
        ):
            raise AgentActivityRequestError(
                "invalid_agent_candidate_inputs"
            )
        flattened = tuple(
            ref
            for _name, refs in raw_bindings
            for ref in refs
        )
        if flattened != raw_refs:
            raise AgentActivityRequestError(
                "agent_candidate_input_mismatch"
            )
        try:
            bindings = tuple(
                AgentActivityInputBinding(
                    name=name,
                    artifacts=tuple(
                        ArtifactDescriptor.from_ref(ref)
                        for ref in refs
                    ),
                )
                for name, refs in raw_bindings
            )
        except ArtifactGrantDenied:
            raise AgentActivityRequestError(
                "invalid_input_artifact_descriptor"
            ) from None
        return cls(
            run_id=claim.run_id,
            node_id=claim.node_id,
            attempt_id=claim.attempt_id,
            attempt_number=claim.attempt_number,
            agent_name=config["agent"],
            request_digest=claim.request_hash,
            definition_digest=candidate.definition_digest,
            task=config.get("task"),
            context=config.get("context"),
            expected_output=config.get("expected_output"),
            output=config.get("output"),
            input_bindings=bindings,
        )


class AgentActivityRequestArtifactStore:
    """Stage and verify deterministic sensitive Agent request Artifacts."""

    def __init__(self, store: ArtifactStore) -> None:
        if not isinstance(store, ArtifactStore):
            raise AgentActivityRequestError("invalid_artifact_store")
        self._store = store

    def stage(
        self,
        candidate: ActivityAdmissionCandidate,
    ) -> ArtifactRef:
        request = AgentActivityRequest.from_candidate(candidate)
        content = request.to_bytes()
        try:
            ref = self._store.put_bytes(
                content,
                media_type=AGENT_ACTIVITY_REQUEST_MEDIA_TYPE,
                kind=ArtifactKind.AGENT_REQUEST,
                sensitivity=request.artifact_sensitivity,
                producer_run_id=request.run_id,
                producer_node_id=request.node_id,
                producer_attempt_id=request.attempt_id,
                metadata={"schema": _SCHEMA_NAME},
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise AgentActivityRequestError(
                "agent_request_artifact_write_failed"
            ) from None
        self._validate_ref(ref, request)
        try:
            verified = self._store.verify(ref)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise AgentActivityRequestError(
                "agent_request_artifact_verify_failed"
            ) from None
        if verified is not True:
            raise AgentActivityRequestError(
                "agent_request_artifact_verify_failed"
            )
        return ref

    def load(
        self,
        ref: ArtifactRef,
        *,
        expected_candidate: ActivityAdmissionCandidate | None = None,
    ) -> AgentActivityRequest:
        self._validate_ref(ref)
        try:
            verified = self._store.verify(ref)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise AgentActivityRequestError(
                "agent_request_artifact_verify_failed"
            ) from None
        if verified is not True:
            raise AgentActivityRequestError(
                "agent_request_artifact_verify_failed"
            )
        try:
            content = self._store.read(ref)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise AgentActivityRequestError(
                "agent_request_artifact_read_failed"
            ) from None
        if (
            not isinstance(content, bytes)
            or len(content) != ref.size
            or hashlib.sha256(content).hexdigest() != ref.sha256
        ):
            raise AgentActivityRequestError(
                "agent_request_artifact_integrity_failed"
            )
        request = AgentActivityRequest.from_bytes(content)
        self._validate_ref(ref, request)
        if (
            expected_candidate is not None
            and request
            != AgentActivityRequest.from_candidate(expected_candidate)
        ):
            raise AgentActivityRequestError(
                "agent_request_candidate_mismatch"
            )
        return request

    @staticmethod
    def _validate_ref(
        ref: ArtifactRef,
        request: AgentActivityRequest | None = None,
    ) -> None:
        if (
            not isinstance(ref, ArtifactRef)
            or ref.kind is not ArtifactKind.AGENT_REQUEST
            or ref.media_type != AGENT_ACTIVITY_REQUEST_MEDIA_TYPE
            or ref.sensitivity
            not in {
                ArtifactSensitivity.SENSITIVE,
                ArtifactSensitivity.SECRET,
            }
            or ref.size > MAX_AGENT_ACTIVITY_REQUEST_BYTES
            or dict(ref.metadata) != {"schema": _SCHEMA_NAME}
        ):
            raise AgentActivityRequestError(
                "invalid_agent_request_artifact"
            )
        if request is not None and (
            ref.sha256 != request.artifact_digest
            or ref.size != len(request.to_bytes())
            or ref.sensitivity is not request.artifact_sensitivity
            or ref.producer_run_id != request.run_id
            or ref.producer_node_id != request.node_id
            or ref.producer_attempt_id != request.attempt_id
        ):
            raise AgentActivityRequestError(
                "agent_request_artifact_binding_mismatch"
            )


def _identifier(value: Any, reason_code: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise AgentActivityRequestError(reason_code)
    return value


def _digest(value: Any, reason_code: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise AgentActivityRequestError(reason_code)
    return value


def _bounded_code(
    value: Any,
    reason_code: str,
    *,
    maximum: int,
) -> str:
    if not isinstance(value, str):
        raise AgentActivityRequestError(reason_code)
    text = value.strip()
    if (
        not text
        or len(text) > maximum
        or not text[0].isascii()
        or not text[0].isalnum()
        or any(
            not (
                character.isascii()
                and (character.isalnum() or character in "._:-")
            )
            for character in text
        )
    ):
        raise AgentActivityRequestError(reason_code)
    return text


def _optional_instruction(value: Any, reason_code: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise AgentActivityRequestError(reason_code)
    if (
        not value.strip()
        or len(value) > MAX_AGENT_ACTIVITY_TEXT_CHARS
        or any(
            ord(character) < 32
            and character not in {"\t", "\n", "\r"}
            for character in value
        )
        or "\x7f" in value
    ):
        raise AgentActivityRequestError(reason_code)
    return value


def _input_descriptor(value: Any) -> ArtifactDescriptor:
    required = {
        "schema_version",
        "artifact_id",
        "sha256",
        "size",
        "media_type",
        "kind",
        "sensitivity",
    }
    if not isinstance(value, Mapping) or set(value) != required:
        raise AgentActivityRequestError(
            "invalid_input_artifact_descriptor"
        )
    try:
        return ArtifactDescriptor(
            schema_version=value["schema_version"],
            artifact_id=value["artifact_id"],
            sha256=value["sha256"],
            size=value["size"],
            media_type=value["media_type"],
            kind=value["kind"],
            sensitivity=value["sensitivity"],
        )
    except (ArtifactGrantDenied, TypeError, ValueError):
        raise AgentActivityRequestError(
            "invalid_input_artifact_descriptor"
        ) from None


def _sensitivity_rank(value: ArtifactSensitivity) -> int:
    return {
        ArtifactSensitivity.PUBLIC: 0,
        ArtifactSensitivity.INTERNAL: 1,
        ArtifactSensitivity.SENSITIVE: 2,
        ArtifactSensitivity.SECRET: 3,
    }[value]


__all__ = [
    "AGENT_ACTIVITY_REQUEST_MEDIA_TYPE",
    "AGENT_ACTIVITY_REQUEST_SCHEMA_VERSION",
    "MAX_AGENT_ACTIVITY_INPUT_REFS",
    "MAX_AGENT_ACTIVITY_REQUEST_BYTES",
    "AgentActivityInputBinding",
    "AgentActivityRequest",
    "AgentActivityRequestArtifactStore",
    "AgentActivityRequestError",
]
