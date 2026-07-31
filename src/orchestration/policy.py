"""Deterministic policy and single-use approval gates for orchestration.

This module deliberately performs no tool execution, network access, or
sandboxing.  It decides whether a fully described action may cross the
side-effect boundary.  Raw action arguments are never retained: callers create
an :class:`ActionRequest` through ``from_args`` and only a canonical digest is
stored.
"""

from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from dataclasses import dataclass, field
from enum import StrEnum
from itertools import islice
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Protocol, runtime_checkable

from .artifacts import ArtifactRef
from .metadata_security import (
    canonical_metadata_key,
    is_sensitive_metadata_key,
)

POLICY_SCHEMA_VERSION = 2
ACTION_SCHEMA_VERSION = 3
MAX_ACTION_ARGS_BYTES = 64 * 1024
MAX_CAPABILITIES = 32
MAX_RESOURCE_LOCKS = 32
MAX_RULES = 1024
MAX_TOOLS = 1024
MAX_APPROVAL_ACTORS = 1024
MAX_SENSITIVE_KEYS = 64
MAX_ACTION_JSON_DEPTH = 32
MAX_ACTION_JSON_NODES = 4096
MAX_TEXT_CHARS = 256
MAX_OPERATION_KEY_CHARS = 1024

class PolicyError(RuntimeError):
    """Base policy error."""


class PolicyValidationError(PolicyError, ValueError):
    """A policy-domain value is malformed or unbounded."""


class EffectClass(StrEnum):
    READ_ONLY = "read_only"
    IDEMPOTENT_WRITE = "idempotent_write"
    NON_IDEMPOTENT_WRITE = "non_idempotent_write"
    DESTRUCTIVE = "destructive"


class PolicyOutcome(StrEnum):
    ALLOW = "allow"
    DENY = "deny"
    REQUIRE_APPROVAL = "require_approval"


class PolicyResolutionSource(StrEnum):
    CONTRACT = "contract"
    RULE = "rule"
    DEFAULT = "default"
    DESTRUCTIVE_FLOOR = "destructive_floor"


class ToolTimeoutBehavior(StrEnum):
    SAFE_TO_RETRY = "safe_to_retry"
    PROBE_BEFORE_RETRY = "probe_before_retry"
    OUTCOME_UNKNOWN = "outcome_unknown"


def _bounded_text(value: Any, field_name: str, *, max_chars: int = MAX_TEXT_CHARS) -> str:
    if not isinstance(value, str):
        raise PolicyValidationError(f"{field_name} must be a string")
    text = value.strip()
    if not text:
        raise PolicyValidationError(f"{field_name} must not be empty")
    if len(text) > max_chars:
        raise PolicyValidationError(f"{field_name} exceeds the bounded metadata limit")
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise PolicyValidationError(f"{field_name} contains control characters")
    return text


def _digest(value: Any, field_name: str) -> str:
    digest = str(value or "")
    if len(digest) != 64 or digest != digest.lower():
        raise PolicyValidationError(f"{field_name} must be a lowercase SHA-256 digest")
    if any(character not in "0123456789abcdef" for character in digest):
        raise PolicyValidationError(f"{field_name} must be a lowercase SHA-256 digest")
    return digest


def _canonical_json(value: Any, *, field_name: str, size_limit: int | None = None) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise PolicyValidationError(f"{field_name} must be canonical JSON") from exc
    if size_limit is not None and len(encoded) > size_limit:
        raise PolicyValidationError(f"{field_name} exceeds the bounded metadata limit")
    return encoded


def _bounded_iterable(
    values: Iterable[Any],
    limit: int,
    field_name: str,
) -> tuple[Any, ...]:
    try:
        items = tuple(islice(iter(values), limit + 1))
    except TypeError as exc:
        raise PolicyValidationError(f"{field_name} must be iterable") from exc
    if len(items) > limit:
        raise PolicyValidationError(
            f"{field_name} exceeds the bounded metadata limit"
        )
    return items


def _validate_bounded_json_tree(value: Any, field_name: str) -> None:
    stack: list[tuple[Any, int]] = [(value, 0)]
    visited = 0
    while stack:
        item, depth = stack.pop()
        visited += 1
        if visited > MAX_ACTION_JSON_NODES or depth > MAX_ACTION_JSON_DEPTH:
            raise PolicyValidationError(
                f"{field_name} exceeds the bounded metadata limit"
            )
        if isinstance(item, dict):
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)


def _redact_sensitive(value: Any, explicit: frozenset[str]) -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            if is_sensitive_metadata_key(key, additional_keys=explicit):
                # Do not retain an offline-verifiable hash of a password, token,
                # or other low-entropy secret.  A deployment that must bind the
                # exact value needs a trusted-boundary HMAC contract; the local
                # deterministic policy core intentionally does not implement it.
                redacted[key] = {"$redacted": True}
            else:
                redacted[key] = _redact_sensitive(item, explicit)
        return redacted
    if isinstance(value, list):
        return [_redact_sensitive(item, explicit) for item in value]
    return value


def canonical_action_args_digest(
    args: Mapping[str, Any],
    *,
    sensitive_keys: Iterable[str] = (),
) -> str:
    """Return the bounded redacted digest used by ``ActionRequest``.

    Raw sensitive values are intentionally replaced rather than directly
    hashed, so callers can validate a receipt binding without creating an
    offline-verifiable secret digest.
    """

    if not isinstance(args, Mapping):
        raise PolicyValidationError("args must be a JSON object")
    raw_encoded = _canonical_json(
        dict(args),
        field_name="args",
        size_limit=MAX_ACTION_ARGS_BYTES,
    )
    normalized = json.loads(raw_encoded.decode("utf-8"))
    _validate_bounded_json_tree(normalized, "args")
    explicit_keys = frozenset(
        canonical_metadata_key(
            _bounded_text(key, "sensitive_key", max_chars=128)
        )
        for key in _bounded_iterable(
            sensitive_keys,
            MAX_SENSITIVE_KEYS,
            "sensitive_keys",
        )
    )
    redacted = _redact_sensitive(normalized, explicit_keys)
    return hashlib.sha256(
        _canonical_json(
            redacted,
            field_name="redacted args",
            size_limit=MAX_ACTION_ARGS_BYTES,
        )
    ).hexdigest()


def sensitive_argument_bytes(
    args: Mapping[str, Any],
    sensitive_keys: Iterable[str],
) -> tuple[bytes, ...]:
    """Extract only explicitly/structurally sensitive scalar values.

    The values are transient validation inputs. Callers must never persist,
    hash, echo, or include them in diagnostics.
    """

    explicit = frozenset(
        canonical_metadata_key(
            _bounded_text(key, "sensitive_key", max_chars=128)
        )
        for key in _bounded_iterable(
            sensitive_keys,
            MAX_SENSITIVE_KEYS,
            "sensitive_keys",
        )
    )
    collected: set[bytes] = set()
    collected_bytes = 0
    visited_containers: set[tuple[int, bool]] = set()
    stack: list[tuple[Any, bool, int]] = [(args, False, 0)]
    visited_nodes = 0
    while stack:
        value, sensitive, depth = stack.pop()
        visited_nodes += 1
        if (
            visited_nodes > MAX_ACTION_JSON_NODES
            or depth > MAX_ACTION_JSON_DEPTH
        ):
            raise PolicyValidationError(
                "args exceeds the bounded metadata limit"
            )
        if isinstance(value, Mapping):
            identity = (id(value), sensitive)
            if identity in visited_containers:
                continue
            visited_containers.add(identity)
            items = _bounded_iterable(
                value.items(),
                MAX_ACTION_JSON_NODES,
                "args",
            )
            for key, item in reversed(items):
                nested_sensitive = sensitive or (
                    isinstance(key, str)
                    and is_sensitive_metadata_key(
                        key,
                        additional_keys=explicit,
                    )
                )
                stack.append((item, nested_sensitive, depth + 1))
            continue
        if isinstance(value, (list, tuple)):
            identity = (id(value), sensitive)
            if identity in visited_containers:
                continue
            visited_containers.add(identity)
            items = _bounded_iterable(
                value,
                MAX_ACTION_JSON_NODES,
                "args",
            )
            stack.extend(
                (item, sensitive, depth + 1)
                for item in reversed(items)
            )
            continue
        if not sensitive:
            continue
        if isinstance(value, str):
            if len(value) > MAX_ACTION_ARGS_BYTES:
                raise PolicyValidationError(
                    "sensitive scalar exceeds the bounded metadata limit"
                )
            encoded = value.encode("utf-8")
        elif isinstance(value, (bytes, bytearray, memoryview)):
            if len(value) > MAX_ACTION_ARGS_BYTES:
                raise PolicyValidationError(
                    "sensitive scalar exceeds the bounded metadata limit"
                )
            encoded = bytes(value)
        elif value is None:
            continue
        elif isinstance(value, (bool, int, float)):
            encoded = _canonical_json(
                value,
                field_name="sensitive scalar",
            )
        else:
            continue
        if len(encoded) > MAX_ACTION_ARGS_BYTES:
            raise PolicyValidationError(
                "sensitive scalar exceeds the bounded metadata limit"
            )
        if encoded and encoded not in collected:
            if collected_bytes + len(encoded) > MAX_ACTION_ARGS_BYTES:
                raise PolicyValidationError(
                    "sensitive values exceed the bounded metadata limit"
                )
            collected.add(encoded)
            collected_bytes += len(encoded)
    return tuple(sorted(collected))


@dataclass(frozen=True, slots=True, order=True)
class Capability:
    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _bounded_text(self.name, "capability", max_chars=128))

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name}


def _capabilities(values: Iterable[Capability | str]) -> tuple[Capability, ...]:
    normalized: dict[str, Capability] = {}
    for value in _bounded_iterable(values, MAX_CAPABILITIES, "capabilities"):
        capability = value if isinstance(value, Capability) else Capability(value)
        normalized[capability.name] = capability
    if len(normalized) > MAX_CAPABILITIES:
        raise PolicyValidationError("capabilities exceed the bounded metadata limit")
    return tuple(normalized[name] for name in sorted(normalized))


def _resource_locks(values: Iterable[str]) -> tuple[str, ...]:
    normalized: set[str] = set()
    for value in _bounded_iterable(values, MAX_RESOURCE_LOCKS, "resource_locks"):
        normalized.add(_bounded_text(value, "resource_lock", max_chars=256))
    if len(normalized) > MAX_RESOURCE_LOCKS:
        raise PolicyValidationError("resource_locks exceed the bounded metadata limit")
    return tuple(sorted(normalized))


def _script_artifact_sha256(ref: ArtifactRef) -> str:
    if not isinstance(ref, ArtifactRef):
        raise PolicyValidationError("script_artifact_ref must be an ArtifactRef")
    if ref.metadata:
        raise PolicyValidationError("script ArtifactRef metadata must be empty")
    return _digest(ref.sha256, "script ArtifactRef sha256")


@dataclass(frozen=True, slots=True)
class ActionRequest:
    """A bounded action identity that never retains raw arguments."""

    run_id: str
    node_id: str
    attempt_id: str
    tool_name: str
    args_digest: str
    execution_binding_digest: str
    operation_key_digest: str
    idempotency_key_digest: str
    script_artifact_sha256: str | None
    requires_script_artifact: bool
    effect_class: EffectClass
    capabilities: tuple[Capability, ...] = ()
    resource_locks: tuple[str, ...] = ()
    schema_version: int = ACTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in ("run_id", "node_id", "attempt_id", "tool_name"):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(getattr(self, field_name), field_name),
            )
        object.__setattr__(self, "args_digest", _digest(self.args_digest, "args_digest"))
        object.__setattr__(
            self,
            "execution_binding_digest",
            _digest(
                self.execution_binding_digest,
                "execution_binding_digest",
            ),
        )
        object.__setattr__(
            self,
            "operation_key_digest",
            _digest(self.operation_key_digest, "operation_key_digest"),
        )
        object.__setattr__(
            self,
            "idempotency_key_digest",
            _digest(self.idempotency_key_digest, "idempotency_key_digest"),
        )
        if self.script_artifact_sha256 is not None:
            object.__setattr__(
                self,
                "script_artifact_sha256",
                _digest(
                    self.script_artifact_sha256,
                    "script_artifact_sha256",
                ),
            )
        if not isinstance(self.requires_script_artifact, bool):
            raise PolicyValidationError("requires_script_artifact must be boolean")
        try:
            object.__setattr__(self, "effect_class", EffectClass(self.effect_class))
        except ValueError as exc:
            raise PolicyValidationError("invalid effect_class") from exc
        object.__setattr__(self, "capabilities", _capabilities(self.capabilities))
        object.__setattr__(self, "resource_locks", _resource_locks(self.resource_locks))
        if self.schema_version != ACTION_SCHEMA_VERSION:
            raise PolicyValidationError("unsupported action schema_version")

    @classmethod
    def from_args(
        cls,
        *,
        run_id: str,
        node_id: str,
        attempt_id: str,
        tool_name: str,
        args: Mapping[str, Any],
        execution_binding_digest: str,
        operation_key: str,
        idempotency_key: str,
        script_artifact_ref: ArtifactRef | None = None,
        requires_script_artifact: bool = False,
        effect_class: EffectClass | str,
        capabilities: Iterable[Capability | str] = (),
        resource_locks: Iterable[str] = (),
        sensitive_keys: Iterable[str] = (),
    ) -> "ActionRequest":
        args_digest = canonical_action_args_digest(
            args,
            sensitive_keys=sensitive_keys,
        )
        return cls(
            run_id=run_id,
            node_id=node_id,
            attempt_id=attempt_id,
            tool_name=tool_name,
            args_digest=args_digest,
            execution_binding_digest=execution_binding_digest,
            operation_key_digest=hashlib.sha256(
                _bounded_text(
                    operation_key,
                    "operation_key",
                    max_chars=MAX_OPERATION_KEY_CHARS,
                ).encode("utf-8")
            ).hexdigest(),
            idempotency_key_digest=hashlib.sha256(
                _bounded_text(
                    idempotency_key,
                    "idempotency_key",
                    max_chars=MAX_OPERATION_KEY_CHARS,
                ).encode("utf-8")
            ).hexdigest(),
            script_artifact_sha256=(
                None
                if script_artifact_ref is None
                else _script_artifact_sha256(script_artifact_ref)
            ),
            requires_script_artifact=requires_script_artifact,
            effect_class=effect_class,
            capabilities=_bounded_iterable(
                capabilities,
                MAX_CAPABILITIES,
                "capabilities",
            ),
            resource_locks=_bounded_iterable(
                resource_locks,
                MAX_RESOURCE_LOCKS,
                "resource_locks",
            ),
        )

    @property
    def action_digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_dict(), field_name="action")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "tool_name": self.tool_name,
            "args_digest": self.args_digest,
            "execution_binding_digest": self.execution_binding_digest,
            "operation_key_digest": self.operation_key_digest,
            "idempotency_key_digest": self.idempotency_key_digest,
            "script_artifact_sha256": self.script_artifact_sha256,
            "requires_script_artifact": self.requires_script_artifact,
            "effect_class": self.effect_class.value,
            "capabilities": [capability.name for capability in self.capabilities],
            "resource_locks": list(self.resource_locks),
        }


@dataclass(frozen=True, slots=True)
class ToolPolicy:
    tool_name: str
    effect_class: EffectClass
    capabilities: tuple[Capability, ...] = ()
    supports_idempotency_key: bool | None = None
    supports_status_probe: bool | None = None
    supports_compensation: bool | None = None
    requires_script_artifact: bool = False
    timeout_behavior: ToolTimeoutBehavior | None = None
    allowed_resource_keys: tuple[str, ...] = ()
    required_resource_keys: tuple[str, ...] = ()
    schema_version: int = POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "tool_name", _bounded_text(self.tool_name, "tool_name"))
        try:
            object.__setattr__(self, "effect_class", EffectClass(self.effect_class))
        except ValueError as exc:
            raise PolicyValidationError("invalid tool effect_class") from exc
        object.__setattr__(self, "capabilities", _capabilities(self.capabilities))
        write = self.effect_class is not EffectClass.READ_ONLY
        declarations = (
            self.supports_idempotency_key,
            self.supports_status_probe,
            self.supports_compensation,
            self.timeout_behavior,
        )
        if write and any(value is None for value in declarations):
            raise PolicyValidationError(
                "write tools must explicitly declare idempotency, probe, "
                "compensation, and timeout behavior"
            )
        for field_name in (
            "supports_idempotency_key",
            "supports_status_probe",
            "supports_compensation",
            "requires_script_artifact",
        ):
            value = getattr(self, field_name)
            if value is None:
                value = False
            if not isinstance(value, bool):
                raise PolicyValidationError(f"{field_name} must be boolean")
            object.__setattr__(self, field_name, value)
        timeout_behavior = self.timeout_behavior
        if timeout_behavior is None:
            timeout_behavior = ToolTimeoutBehavior.SAFE_TO_RETRY
        try:
            timeout_behavior = ToolTimeoutBehavior(timeout_behavior)
        except ValueError as exc:
            raise PolicyValidationError("invalid tool timeout_behavior") from exc
        object.__setattr__(self, "timeout_behavior", timeout_behavior)
        allowed = _resource_locks(self.allowed_resource_keys)
        required = _resource_locks(self.required_resource_keys)
        if not set(required).issubset(allowed):
            raise PolicyValidationError(
                "required_resource_keys must be a subset of allowed_resource_keys"
            )
        object.__setattr__(self, "allowed_resource_keys", allowed)
        object.__setattr__(self, "required_resource_keys", required)
        if self.schema_version != POLICY_SCHEMA_VERSION:
            raise PolicyValidationError("unsupported tool policy schema_version")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "tool_name": self.tool_name,
            "effect_class": self.effect_class.value,
            "capabilities": [capability.name for capability in self.capabilities],
            "supports_idempotency_key": self.supports_idempotency_key,
            "supports_status_probe": self.supports_status_probe,
            "supports_compensation": self.supports_compensation,
            "requires_script_artifact": self.requires_script_artifact,
            "timeout_behavior": self.timeout_behavior.value,
            "allowed_resource_keys": list(self.allowed_resource_keys),
            "required_resource_keys": list(self.required_resource_keys),
        }


@dataclass(frozen=True, slots=True)
class PolicyRule:
    rule_id: str
    outcome: PolicyOutcome
    tool_name: str = "*"
    effect_classes: tuple[EffectClass, ...] = ()
    required_capabilities: tuple[Capability, ...] = ()
    required_resource_locks: tuple[str, ...] = ()
    reason_code: str = "policy_rule"
    schema_version: int = POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "rule_id", _bounded_text(self.rule_id, "rule_id"))
        try:
            object.__setattr__(self, "outcome", PolicyOutcome(self.outcome))
        except ValueError as exc:
            raise PolicyValidationError("invalid policy outcome") from exc
        if self.tool_name != "*":
            object.__setattr__(
                self,
                "tool_name",
                _bounded_text(self.tool_name, "rule tool_name"),
            )
        effects: set[EffectClass] = set()
        try:
            for effect in _bounded_iterable(
                self.effect_classes,
                len(EffectClass),
                "effect_classes",
            ):
                effects.add(EffectClass(effect))
        except ValueError as exc:
            raise PolicyValidationError("invalid rule effect_classes") from exc
        object.__setattr__(
            self,
            "effect_classes",
            tuple(sorted(effects, key=lambda effect: effect.value)),
        )
        object.__setattr__(
            self,
            "required_capabilities",
            _capabilities(self.required_capabilities),
        )
        object.__setattr__(
            self,
            "required_resource_locks",
            _resource_locks(self.required_resource_locks),
        )
        object.__setattr__(
            self,
            "reason_code",
            _bounded_text(self.reason_code, "reason_code", max_chars=128),
        )
        if self.schema_version != POLICY_SCHEMA_VERSION:
            raise PolicyValidationError("unsupported policy rule schema_version")

    def matches(self, action: ActionRequest) -> bool:
        if self.tool_name != "*" and self.tool_name != action.tool_name:
            return False
        if self.effect_classes and action.effect_class not in self.effect_classes:
            return False
        action_capabilities = set(action.capabilities)
        if not set(self.required_capabilities).issubset(action_capabilities):
            return False
        if not set(self.required_resource_locks).issubset(action.resource_locks):
            return False
        return True

    @property
    def specificity(self) -> tuple[int, int, int, int]:
        return (
            int(self.tool_name != "*"),
            int(bool(self.effect_classes)),
            len(self.required_capabilities),
            len(self.required_resource_locks),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "rule_id": self.rule_id,
            "outcome": self.outcome.value,
            "tool_name": self.tool_name,
            "effect_classes": [effect.value for effect in self.effect_classes],
            "required_capabilities": [
                capability.name for capability in self.required_capabilities
            ],
            "required_resource_locks": list(self.required_resource_locks),
            "reason_code": self.reason_code,
        }


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    outcome: PolicyOutcome
    action_digest: str
    policy_version: str
    reason_code: str
    matched_rule_ids: tuple[str, ...] = ()
    approval_id: str | None = None
    approved_by: str | None = None
    schema_version: int = POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "outcome", PolicyOutcome(self.outcome))
        except ValueError as exc:
            raise PolicyValidationError("invalid policy decision outcome") from exc
        object.__setattr__(self, "action_digest", _digest(self.action_digest, "action_digest"))
        object.__setattr__(
            self,
            "policy_version",
            _bounded_text(self.policy_version, "policy_version"),
        )
        object.__setattr__(
            self,
            "reason_code",
            _bounded_text(self.reason_code, "reason_code", max_chars=128),
        )
        object.__setattr__(
            self,
            "matched_rule_ids",
            tuple(
                sorted(
                    {
                        _bounded_text(rule_id, "matched_rule_id")
                        for rule_id in self.matched_rule_ids
                    }
                )
            ),
        )
        for field_name in ("approval_id", "approved_by"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _bounded_text(value, field_name))
        if self.schema_version != POLICY_SCHEMA_VERSION:
            raise PolicyValidationError("unsupported policy decision schema_version")

    @property
    def is_allowed(self) -> bool:
        return self.outcome is PolicyOutcome.ALLOW

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "outcome": self.outcome.value,
            "action_digest": self.action_digest,
            "policy_version": self.policy_version,
            "reason_code": self.reason_code,
            "matched_rule_ids": list(self.matched_rule_ids),
            "approval_id": self.approval_id,
            "approved_by": self.approved_by,
        }


@dataclass(frozen=True, slots=True)
class PolicyCheck:
    """One payload-free constraint evaluated by the policy engine."""

    code: str
    passed: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "code",
            _bounded_text(self.code, "policy check code", max_chars=128),
        )
        if not isinstance(self.passed, bool):
            raise PolicyValidationError("policy check result must be boolean")

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "passed": self.passed}


@dataclass(frozen=True, slots=True)
class PolicySimulation:
    """Side-effect-free explanation of the exact runtime policy path.

    A simulation is evidence about a decision, never execution authority.
    Callers must evaluate the action again at the execution boundary.
    """

    decision: PolicyDecision
    checks: tuple[PolicyCheck, ...]
    resolution_source: PolicyResolutionSource
    tool_registered: bool
    matching_rule_ids: tuple[str, ...] = ()
    finalist_rule_ids: tuple[str, ...] = ()
    winning_rule_id: str | None = None
    timeout_behavior: ToolTimeoutBehavior | None = None
    supports_idempotency_key: bool | None = None
    supports_status_probe: bool | None = None
    supports_compensation: bool | None = None
    required_resource_lock_count: int = 0
    provided_resource_lock_count: int = 0
    schema_version: int = POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.decision, PolicyDecision):
            raise PolicyValidationError("simulation decision must be a PolicyDecision")
        if not self.checks or not all(
            isinstance(check, PolicyCheck) for check in self.checks
        ):
            raise PolicyValidationError("simulation checks must contain PolicyCheck values")
        try:
            object.__setattr__(
                self,
                "resolution_source",
                PolicyResolutionSource(self.resolution_source),
            )
        except ValueError as exc:
            raise PolicyValidationError("invalid policy resolution source") from exc
        if not isinstance(self.tool_registered, bool):
            raise PolicyValidationError("tool_registered must be boolean")
        matching = _bounded_rule_ids(self.matching_rule_ids, "matching_rule_id")
        finalists = _bounded_rule_ids(self.finalist_rule_ids, "finalist_rule_id")
        if not set(finalists).issubset(matching):
            raise PolicyValidationError(
                "simulation finalists must be a subset of matching rules"
            )
        object.__setattr__(self, "matching_rule_ids", matching)
        object.__setattr__(self, "finalist_rule_ids", finalists)
        if tuple(self.decision.matched_rule_ids) != finalists:
            raise PolicyValidationError(
                "simulation finalists must match the decision rule binding"
            )
        if self.winning_rule_id is not None:
            winner = _bounded_text(self.winning_rule_id, "winning_rule_id")
            if winner not in finalists:
                raise PolicyValidationError(
                    "simulation winner must be a finalist rule"
                )
            object.__setattr__(self, "winning_rule_id", winner)
        if (
            self.resolution_source is PolicyResolutionSource.RULE
            and self.winning_rule_id is None
        ):
            raise PolicyValidationError("rule resolution requires a winning rule")
        if self.timeout_behavior is not None:
            try:
                object.__setattr__(
                    self,
                    "timeout_behavior",
                    ToolTimeoutBehavior(self.timeout_behavior),
                )
            except ValueError as exc:
                raise PolicyValidationError(
                    "invalid simulation timeout behavior"
                ) from exc
        for field_name in (
            "supports_idempotency_key",
            "supports_status_probe",
            "supports_compensation",
        ):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, bool):
                raise PolicyValidationError(
                    f"{field_name} simulation value must be boolean or null"
                )
        for field_name in (
            "required_resource_lock_count",
            "provided_resource_lock_count",
        ):
            value = getattr(self, field_name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
                or value > MAX_RESOURCE_LOCKS
            ):
                raise PolicyValidationError(
                    f"{field_name} exceeds the bounded metadata limit"
                )
        if self.schema_version != POLICY_SCHEMA_VERSION:
            raise PolicyValidationError("unsupported policy simulation schema_version")

    @property
    def simulation_digest(self) -> str:
        return hashlib.sha256(
            _canonical_json(self.to_dict(), field_name="policy simulation")
        ).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "decision": self.decision.to_dict(),
            "checks": [check.to_dict() for check in self.checks],
            "resolution_source": self.resolution_source.value,
            "tool_registered": self.tool_registered,
            "matching_rule_ids": list(self.matching_rule_ids),
            "finalist_rule_ids": list(self.finalist_rule_ids),
            "winning_rule_id": self.winning_rule_id,
            "timeout_behavior": (
                None
                if self.timeout_behavior is None
                else self.timeout_behavior.value
            ),
            "supports_idempotency_key": self.supports_idempotency_key,
            "supports_status_probe": self.supports_status_probe,
            "supports_compensation": self.supports_compensation,
            "required_resource_lock_count": self.required_resource_lock_count,
            "provided_resource_lock_count": self.provided_resource_lock_count,
            "authorizes_execution": False,
            "runtime_recheck_required": True,
        }


def _bounded_rule_ids(values: Iterable[str], field_name: str) -> tuple[str, ...]:
    normalized = tuple(
        sorted(
            {
                _bounded_text(value, field_name)
                for value in _bounded_iterable(values, MAX_RULES, f"{field_name}s")
            }
        )
    )
    if len(normalized) > MAX_RULES:
        raise PolicyValidationError("rule IDs exceed the bounded policy limit")
    return normalized


@dataclass(frozen=True, slots=True)
class ApprovalGrant:
    approval_id: str
    action_digest: str
    run_id: str
    node_id: str
    policy_version: str
    actor: str
    expires_at: float
    schema_version: int = POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in ("approval_id", "run_id", "node_id", "policy_version", "actor"):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(getattr(self, field_name), field_name),
            )
        object.__setattr__(self, "action_digest", _digest(self.action_digest, "action_digest"))
        try:
            expires_at = float(self.expires_at)
        except (TypeError, ValueError) as exc:
            raise PolicyValidationError("expires_at must be a finite timestamp") from exc
        if not math.isfinite(expires_at) or expires_at < 0:
            raise PolicyValidationError("expires_at must be a finite timestamp")
        object.__setattr__(self, "expires_at", expires_at)
        if self.schema_version != POLICY_SCHEMA_VERSION:
            raise PolicyValidationError("unsupported approval schema_version")

    @property
    def grant_digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_dict(), field_name="approval")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "approval_id": self.approval_id,
            "action_digest": self.action_digest,
            "run_id": self.run_id,
            "node_id": self.node_id,
            "policy_version": self.policy_version,
            "actor": self.actor,
            "expires_at": self.expires_at,
        }


@runtime_checkable
class ApprovalLedger(Protocol):
    """Trusted issuance registry plus atomic single-use grant consumption.

    ``register_issued`` belongs to a trusted UI/control-plane path.  Activity
    execution code must receive only the ledger through ``PolicyEngine`` and
    must never register its own grants.
    """

    def register_issued(self, grant: ApprovalGrant) -> bool: ...

    def consume(self, grant: ApprovalGrant) -> "ApprovalConsumeResult": ...


class ApprovalConsumeResult(StrEnum):
    CONSUMED = "consumed"
    UNKNOWN = "unknown"
    GRANT_MISMATCH = "grant_mismatch"
    REPLAYED = "replayed"


class InMemoryApprovalLedger:
    """Thread-safe process-local ledger for a trusted local control plane."""

    def __init__(self) -> None:
        self._issued: dict[str, str] = {}
        self._consumed: set[str] = set()
        self._lock = threading.Lock()

    def register_issued(self, grant: ApprovalGrant) -> bool:
        """Register an authentic grant; call only from trusted issuance code."""

        if not isinstance(grant, ApprovalGrant):
            raise PolicyValidationError("grant must be an ApprovalGrant")
        digest = grant.grant_digest
        with self._lock:
            existing = self._issued.get(grant.approval_id)
            if existing is not None:
                return existing == digest
            self._issued[grant.approval_id] = digest
            return True

    def consume(self, grant: ApprovalGrant) -> ApprovalConsumeResult:
        if not isinstance(grant, ApprovalGrant):
            raise PolicyValidationError("grant must be an ApprovalGrant")
        digest = grant.grant_digest
        with self._lock:
            issued_digest = self._issued.get(grant.approval_id)
            if issued_digest is None:
                return ApprovalConsumeResult.UNKNOWN
            if issued_digest != digest:
                return ApprovalConsumeResult.GRANT_MISMATCH
            if grant.approval_id in self._consumed:
                return ApprovalConsumeResult.REPLAYED
            self._consumed.add(grant.approval_id)
            return ApprovalConsumeResult.CONSUMED

    def was_issued(self, approval_id: str) -> bool:
        bounded_id = _bounded_text(approval_id, "approval_id")
        with self._lock:
            return bounded_id in self._issued

    def was_consumed(self, approval_id: str) -> bool:
        bounded_id = _bounded_text(approval_id, "approval_id")
        with self._lock:
            return bounded_id in self._consumed


class PolicyEngine:
    """Evaluate registered tools and consume approval grants fail closed."""

    _OUTCOME_PRIORITY = {
        PolicyOutcome.ALLOW: 0,
        PolicyOutcome.REQUIRE_APPROVAL: 1,
        PolicyOutcome.DENY: 2,
    }

    def __init__(
        self,
        tools: Iterable[ToolPolicy],
        rules: Iterable[PolicyRule] = (),
        *,
        approval_actors: Iterable[str] = (),
        ledger: ApprovalLedger | None = None,
    ) -> None:
        tool_map: dict[str, ToolPolicy] = {}
        for tool in _bounded_iterable(tools, MAX_TOOLS, "tools"):
            if not isinstance(tool, ToolPolicy):
                raise PolicyValidationError("tools must contain ToolPolicy values")
            if tool.tool_name in tool_map:
                raise PolicyValidationError("tool policy names must be unique")
            tool_map[tool.tool_name] = tool
        if not tool_map:
            raise PolicyValidationError("at least one tool policy is required")

        normalized_rules = _bounded_iterable(rules, MAX_RULES, "rules")
        rule_ids: set[str] = set()
        known_capabilities = {
            capability.name
            for tool in tool_map.values()
            for capability in tool.capabilities
        }
        for rule in normalized_rules:
            if not isinstance(rule, PolicyRule):
                raise PolicyValidationError("rules must contain PolicyRule values")
            if rule.rule_id in rule_ids:
                raise PolicyValidationError("policy rule IDs must be unique")
            rule_ids.add(rule.rule_id)
            if rule.tool_name != "*" and rule.tool_name not in tool_map:
                raise PolicyValidationError("policy rule references an unknown tool")
            if any(
                capability.name not in known_capabilities
                for capability in rule.required_capabilities
            ):
                raise PolicyValidationError("policy rule references an unknown capability")

        actors = frozenset(
            _bounded_text(actor, "approval_actor")
            for actor in _bounded_iterable(
                approval_actors,
                MAX_APPROVAL_ACTORS,
                "approval_actors",
            )
        )
        self._tools = MappingProxyType(dict(sorted(tool_map.items())))
        self._rules = tuple(sorted(normalized_rules, key=lambda rule: rule.rule_id))
        self._known_capabilities = frozenset(known_capabilities)
        self._approval_actors = actors
        self._ledger = ledger if ledger is not None else InMemoryApprovalLedger()
        if not isinstance(self._ledger, ApprovalLedger):
            raise PolicyValidationError("ledger does not implement ApprovalLedger")

        document = {
            "schema_version": POLICY_SCHEMA_VERSION,
            "tools": [tool.to_dict() for tool in self._tools.values()],
            "rules": [rule.to_dict() for rule in self._rules],
            "approval_actors": sorted(self._approval_actors),
        }
        self.policy_digest = hashlib.sha256(
            _canonical_json(document, field_name="policy document")
        ).hexdigest()
        self.policy_version = f"sha256:{self.policy_digest}"

    def evaluate(self, action: ActionRequest) -> PolicyDecision:
        """Evaluate one action through the same path used by simulation."""

        return self.simulate(action).decision

    def simulate(self, action: ActionRequest) -> PolicySimulation:
        """Explain a policy outcome without granting execution authority."""

        if not isinstance(action, ActionRequest):
            raise PolicyValidationError("action must be an ActionRequest")
        checks: list[PolicyCheck] = []
        profile = self._tools.get(action.tool_name)
        if profile is None:
            checks.append(PolicyCheck("tool_registered", False))
            return self._simulation(
                action,
                self._decision(action, PolicyOutcome.DENY, "unknown_tool"),
                checks,
                PolicyResolutionSource.CONTRACT,
                profile=None,
            )
        checks.append(PolicyCheck("tool_registered", True))

        action_capabilities = {capability.name for capability in action.capabilities}
        if not action_capabilities.issubset(self._known_capabilities):
            checks.append(PolicyCheck("capabilities_known", False))
            return self._simulation(
                action,
                self._decision(action, PolicyOutcome.DENY, "unknown_capability"),
                checks,
                PolicyResolutionSource.CONTRACT,
                profile=profile,
            )
        checks.append(PolicyCheck("capabilities_known", True))
        if action.effect_class is not profile.effect_class:
            checks.append(PolicyCheck("effect_class_matches", False))
            return self._simulation(
                action,
                self._decision(action, PolicyOutcome.DENY, "effect_class_mismatch"),
                checks,
                PolicyResolutionSource.CONTRACT,
                profile=profile,
            )
        checks.append(PolicyCheck("effect_class_matches", True))
        profile_capabilities = {capability.name for capability in profile.capabilities}
        if action_capabilities != profile_capabilities:
            checks.append(PolicyCheck("capability_contract_matches", False))
            return self._simulation(
                action,
                self._decision(action, PolicyOutcome.DENY, "capability_mismatch"),
                checks,
                PolicyResolutionSource.CONTRACT,
                profile=profile,
            )
        checks.append(PolicyCheck("capability_contract_matches", True))
        if action.requires_script_artifact != profile.requires_script_artifact:
            checks.append(PolicyCheck("script_artifact_contract_matches", False))
            return self._simulation(
                action,
                self._decision(
                    action,
                    PolicyOutcome.DENY,
                    "script_artifact_contract_mismatch",
                ),
                checks,
                PolicyResolutionSource.CONTRACT,
                profile=profile,
            )
        checks.append(PolicyCheck("script_artifact_contract_matches", True))
        if (
            profile.requires_script_artifact
            and action.script_artifact_sha256 is None
        ):
            checks.append(PolicyCheck("script_artifact_present", False))
            return self._simulation(
                action,
                self._decision(
                    action,
                    PolicyOutcome.DENY,
                    "script_artifact_required",
                ),
                checks,
                PolicyResolutionSource.CONTRACT,
                profile=profile,
            )
        if profile.requires_script_artifact:
            checks.append(PolicyCheck("script_artifact_present", True))
        action_resource_locks = set(action.resource_locks)
        if not set(profile.required_resource_keys).issubset(action_resource_locks):
            checks.append(PolicyCheck("required_resource_locks_present", False))
            return self._simulation(
                action,
                self._decision(
                    action,
                    PolicyOutcome.DENY,
                    "required_resource_lock_missing",
                ),
                checks,
                PolicyResolutionSource.CONTRACT,
                profile=profile,
            )
        checks.append(PolicyCheck("required_resource_locks_present", True))
        if not action_resource_locks.issubset(profile.allowed_resource_keys):
            checks.append(PolicyCheck("resource_locks_allowed", False))
            return self._simulation(
                action,
                self._decision(
                    action,
                    PolicyOutcome.DENY,
                    "resource_lock_not_allowed",
                ),
                checks,
                PolicyResolutionSource.CONTRACT,
                profile=profile,
            )
        checks.append(PolicyCheck("resource_locks_allowed", True))
        if action.effect_class is not EffectClass.READ_ONLY:
            if not profile.required_resource_keys:
                checks.append(PolicyCheck("write_resource_contract_declared", False))
                return self._simulation(
                    action,
                    self._decision(
                        action,
                        PolicyOutcome.DENY,
                        "write_resource_contract_missing",
                    ),
                    checks,
                    PolicyResolutionSource.CONTRACT,
                    profile=profile,
                )
            checks.append(PolicyCheck("write_resource_contract_declared", True))
            if profile.timeout_behavior is ToolTimeoutBehavior.SAFE_TO_RETRY:
                checks.append(PolicyCheck("write_timeout_contract_safe", False))
                return self._simulation(
                    action,
                    self._decision(
                        action,
                        PolicyOutcome.DENY,
                        "write_timeout_contract_unsafe",
                    ),
                    checks,
                    PolicyResolutionSource.CONTRACT,
                    profile=profile,
                )
            checks.append(PolicyCheck("write_timeout_contract_safe", True))
            if (
                profile.timeout_behavior is ToolTimeoutBehavior.PROBE_BEFORE_RETRY
                and not profile.supports_status_probe
            ):
                checks.append(PolicyCheck("status_probe_contract_satisfied", False))
                return self._simulation(
                    action,
                    self._decision(
                        action,
                        PolicyOutcome.DENY,
                        "status_probe_unsupported",
                    ),
                    checks,
                    PolicyResolutionSource.CONTRACT,
                    profile=profile,
                )
            if profile.timeout_behavior is ToolTimeoutBehavior.PROBE_BEFORE_RETRY:
                checks.append(PolicyCheck("status_probe_contract_satisfied", True))
            if (
                action.effect_class is EffectClass.IDEMPOTENT_WRITE
                and not profile.supports_idempotency_key
            ):
                checks.append(PolicyCheck("idempotency_contract_satisfied", False))
                return self._simulation(
                    action,
                    self._decision(
                        action,
                        PolicyOutcome.DENY,
                        "idempotency_key_unsupported",
                    ),
                    checks,
                    PolicyResolutionSource.CONTRACT,
                    profile=profile,
                )
            if action.effect_class is EffectClass.IDEMPOTENT_WRITE:
                checks.append(PolicyCheck("idempotency_contract_satisfied", True))

        matching = [rule for rule in self._rules if rule.matches(action)]
        matching_rule_ids = tuple(rule.rule_id for rule in matching)
        matched_rule_ids: tuple[str, ...] = ()
        winning_rule_id: str | None = None
        if matching:
            highest_specificity = max(rule.specificity for rule in matching)
            finalists = [
                rule for rule in matching if rule.specificity == highest_specificity
            ]
            chosen = max(
                finalists,
                key=lambda rule: (
                    self._OUTCOME_PRIORITY[rule.outcome],
                    # Stable tie-break only affects the reason code.
                    rule.rule_id,
                ),
            )
            outcome = chosen.outcome
            reason_code = chosen.reason_code
            matched_rule_ids = tuple(rule.rule_id for rule in finalists)
            winning_rule_id = chosen.rule_id
            source = PolicyResolutionSource.RULE
        elif action.effect_class is EffectClass.READ_ONLY:
            outcome = PolicyOutcome.ALLOW
            reason_code = "known_read_only"
            source = PolicyResolutionSource.DEFAULT
        else:
            outcome = PolicyOutcome.REQUIRE_APPROVAL
            reason_code = "write_requires_approval"
            source = PolicyResolutionSource.DEFAULT
        checks.append(PolicyCheck("policy_rules_resolved", True))

        # Destructive actions always retain an approval gate; an ALLOW rule
        # cannot silently remove the strongest built-in safety boundary.
        if action.effect_class is EffectClass.DESTRUCTIVE and outcome is PolicyOutcome.ALLOW:
            outcome = PolicyOutcome.REQUIRE_APPROVAL
            reason_code = "destructive_requires_approval"
            source = PolicyResolutionSource.DESTRUCTIVE_FLOOR
            checks.append(PolicyCheck("destructive_approval_floor_applied", True))
        return self._simulation(
            action,
            self._decision(
                action,
                outcome,
                reason_code,
                matched_rule_ids=matched_rule_ids,
            ),
            checks,
            source,
            profile=profile,
            matching_rule_ids=matching_rule_ids,
            finalist_rule_ids=matched_rule_ids,
            winning_rule_id=winning_rule_id,
        )

    def tool_policy(self, tool_name: str) -> ToolPolicy | None:
        """Return the immutable registered execution contract for one tool."""

        return self._tools.get(_bounded_text(tool_name, "tool_name"))

    def consume_approval(
        self,
        action: ActionRequest,
        grant: ApprovalGrant,
        *,
        now: float | None = None,
    ) -> PolicyDecision:
        if not isinstance(grant, ApprovalGrant):
            raise PolicyValidationError("grant must be an ApprovalGrant")
        current = self.evaluate(action)
        if current.outcome is PolicyOutcome.DENY:
            return self._decision(
                action,
                PolicyOutcome.DENY,
                "approval_cannot_expand_policy",
                matched_rule_ids=current.matched_rule_ids,
            )
        if current.outcome is PolicyOutcome.ALLOW:
            return self._decision(
                action,
                PolicyOutcome.DENY,
                "approval_not_required",
                matched_rule_ids=current.matched_rule_ids,
            )
        if grant.action_digest != action.action_digest:
            return self._decision(action, PolicyOutcome.DENY, "approval_action_mismatch")
        if grant.run_id != action.run_id or grant.node_id != action.node_id:
            return self._decision(action, PolicyOutcome.DENY, "approval_scope_mismatch")
        if grant.policy_version != self.policy_version:
            return self._decision(action, PolicyOutcome.DENY, "approval_policy_version_mismatch")
        if grant.actor not in self._approval_actors:
            return self._decision(action, PolicyOutcome.DENY, "approval_actor_untrusted")
        current_time = time.time() if now is None else float(now)
        if not math.isfinite(current_time) or current_time < 0:
            raise PolicyValidationError("now must be a finite timestamp")
        if grant.expires_at <= current_time:
            return self._decision(action, PolicyOutcome.DENY, "approval_expired")
        consume_result = self._ledger.consume(grant)
        if consume_result is not ApprovalConsumeResult.CONSUMED:
            reason_codes = {
                ApprovalConsumeResult.UNKNOWN: "approval_unissued",
                ApprovalConsumeResult.GRANT_MISMATCH: "approval_grant_mismatch",
                ApprovalConsumeResult.REPLAYED: "approval_replayed",
            }
            reason_code = reason_codes.get(consume_result, "approval_ledger_denied")
            return self._decision(action, PolicyOutcome.DENY, reason_code)
        return self._decision(
            action,
            PolicyOutcome.ALLOW,
            "approval_consumed",
            matched_rule_ids=current.matched_rule_ids,
            approval_id=grant.approval_id,
            approved_by=grant.actor,
        )

    def _decision(
        self,
        action: ActionRequest,
        outcome: PolicyOutcome,
        reason_code: str,
        *,
        matched_rule_ids: tuple[str, ...] = (),
        approval_id: str | None = None,
        approved_by: str | None = None,
    ) -> PolicyDecision:
        return PolicyDecision(
            outcome=outcome,
            action_digest=action.action_digest,
            policy_version=self.policy_version,
            reason_code=reason_code,
            matched_rule_ids=matched_rule_ids,
            approval_id=approval_id,
            approved_by=approved_by,
        )

    def _simulation(
        self,
        action: ActionRequest,
        decision: PolicyDecision,
        checks: Iterable[PolicyCheck],
        resolution_source: PolicyResolutionSource,
        *,
        profile: ToolPolicy | None,
        matching_rule_ids: tuple[str, ...] = (),
        finalist_rule_ids: tuple[str, ...] = (),
        winning_rule_id: str | None = None,
    ) -> PolicySimulation:
        return PolicySimulation(
            decision=decision,
            checks=tuple(checks),
            resolution_source=resolution_source,
            tool_registered=profile is not None,
            matching_rule_ids=matching_rule_ids,
            finalist_rule_ids=finalist_rule_ids,
            winning_rule_id=winning_rule_id,
            timeout_behavior=(
                None if profile is None else profile.timeout_behavior
            ),
            supports_idempotency_key=(
                None if profile is None else profile.supports_idempotency_key
            ),
            supports_status_probe=(
                None if profile is None else profile.supports_status_probe
            ),
            supports_compensation=(
                None if profile is None else profile.supports_compensation
            ),
            required_resource_lock_count=(
                0 if profile is None else len(profile.required_resource_keys)
            ),
            provided_resource_lock_count=len(action.resource_locks),
        )
