"""Fail-closed dispatch boundary for externally provided sandbox backends.

This module is a control plane, not a sandbox implementation.  In particular,
it never treats a normal ``subprocess`` as isolation.  A backend must explicitly
declare its security level and capabilities, and deployment code is responsible
for supplying an implementation that actually enforces the selected profile.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass, field, replace
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol, runtime_checkable

from .artifacts import ArtifactKind, ArtifactRef
from .policy import (
    MAX_OPERATION_KEY_CHARS,
    ActionRequest,
    Capability,
    EffectClass,
    PolicyDecision,
    PolicyOutcome,
)

SANDBOX_SCHEMA_VERSION = 2
MAX_ARGV_ITEMS = 128
MAX_ARGV_ITEM_CHARS = 4096
MAX_ARGV_BYTES = 32 * 1024
MAX_ENV_BINDINGS = 64
MAX_ALLOWED_ROOTS = 16
MAX_ALLOWLIST_HOSTS = 128
MAX_TIMEOUT_SECONDS = 24 * 60 * 60
MAX_CPU_SECONDS = 24 * 60 * 60
MAX_OUTPUT_BYTES = 64 * 1024 * 1024
MAX_MEMORY_BYTES = 16 * 1024 * 1024 * 1024
MAX_PROCESSES = 1024
MAX_SCRIPT_BYTES = 4 * 1024 * 1024
MAX_RECEIPT_ARTIFACT_REFS = 64


class SandboxError(RuntimeError):
    """Base sandbox control-plane error."""


class SandboxValidationError(SandboxError, ValueError):
    """Sandbox metadata is malformed or exceeds a safety bound."""


class SandboxDispatchDenied(SandboxError):
    """No execution occurred because a dispatch gate failed closed."""

    def __init__(self, reason_code: str):
        self.reason_code = _bounded_text(reason_code, "reason_code", max_chars=128)
        super().__init__(self.reason_code)


class SecurityLevel(StrEnum):
    CONTAINER = "container"
    OS_SANDBOX = "os_sandbox"
    # Pre-registered trusted code with no arbitrary-program isolation claim.
    # Profiles must opt in explicitly; the default OS_SANDBOX profile rejects it.
    TRUSTED_FUNCTION = "trusted_function"
    DEVELOPMENT_UNSAFE = "development_unsafe"


class NetworkMode(StrEnum):
    DENY = "deny"
    PUBLIC_PROXY = "public_proxy"
    ALLOWLIST = "allowlist"


class SandboxOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    CANCELLATION_UNKNOWN = "cancellation_unknown"
    BACKEND_ERROR = "backend_error"


class CancellationSignal(StrEnum):
    CONTINUE = "continue"
    CANCEL_REQUESTED = "cancel_requested"
    UNKNOWN = "unknown"


_SECURITY_RANK = {
    SecurityLevel.DEVELOPMENT_UNSAFE: 0,
    SecurityLevel.TRUSTED_FUNCTION: 1,
    SecurityLevel.OS_SANDBOX: 2,
    SecurityLevel.CONTAINER: 3,
}

_NETWORK_CAPABILITY = {
    NetworkMode.PUBLIC_PROXY: "network.public_proxy",
    NetworkMode.ALLOWLIST: "network.allowlist",
}


def _bounded_text(value: Any, field_name: str, *, max_chars: int = 256) -> str:
    if not isinstance(value, str):
        raise SandboxValidationError(f"{field_name} must be a string")
    text = value.strip()
    if not text:
        raise SandboxValidationError(f"{field_name} must not be empty")
    if len(text) > max_chars:
        raise SandboxValidationError(f"{field_name} exceeds the bounded metadata limit")
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise SandboxValidationError(f"{field_name} contains control characters")
    return text


def _sha256(value: Any, field_name: str) -> str:
    digest = str(value or "")
    if (
        len(digest) != 64
        or digest != digest.lower()
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise SandboxValidationError(f"{field_name} must be a lowercase SHA-256 digest")
    return digest


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise SandboxValidationError("sandbox metadata must be canonical JSON") from exc


def _normalized_capabilities(
    values: Iterable[Capability | str],
) -> tuple[Capability, ...]:
    try:
        normalized = {
            (value if isinstance(value, Capability) else Capability(value)).name
            for value in values
        }
    except (TypeError, ValueError) as exc:
        raise SandboxValidationError("capabilities are invalid") from exc
    if len(normalized) > 64:
        raise SandboxValidationError("capabilities exceed the bounded metadata limit")
    return tuple(Capability(name) for name in sorted(normalized))


def _artifact_identity(ref: ArtifactRef) -> dict[str, Any]:
    if not isinstance(ref, ArtifactRef):
        raise SandboxValidationError("artifact_refs must contain ArtifactRef values")
    return {
        "artifact_id": ref.artifact_id,
        "sha256": ref.sha256,
        "size": ref.size,
        "kind": ref.kind.value,
    }


def _execution_artifact_identity(ref: ArtifactRef) -> dict[str, Any]:
    identity = _artifact_identity(ref)
    identity.update(
        {
            "media_type": ref.media_type,
            "uri": ref.uri,
            "sensitivity": ref.sensitivity.value,
            "encryption": ref.encryption.value,
            "encryption_key_ref": ref.encryption_key_ref,
            "producer_run_id": ref.producer_run_id,
            "producer_node_id": ref.producer_node_id,
            "producer_attempt_id": ref.producer_attempt_id,
        }
    )
    return identity


def _validated_cwd(cwd: str, allowed_roots: tuple[str, ...]) -> Path:
    path = Path(cwd).expanduser()
    if not path.is_absolute():
        raise SandboxDispatchDenied("cwd_must_be_absolute")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise SandboxDispatchDenied("cwd_missing") from exc
    if not resolved.is_dir():
        raise SandboxDispatchDenied("cwd_not_directory")
    for root_text in allowed_roots:
        root = Path(root_text)
        if resolved == root or root in resolved.parents:
            return resolved
    raise SandboxDispatchDenied("cwd_outside_allowed_roots")


@dataclass(frozen=True, slots=True)
class CancellationProbe:
    """Trusted, identity-bound callback for durable cooperative cancellation."""

    run_id: str
    node_id: str
    attempt_id: str
    callback: Callable[[], CancellationSignal] = field(
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        for field_name in ("run_id", "node_id", "attempt_id"):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(getattr(self, field_name), field_name),
            )
        if not callable(self.callback):
            raise SandboxValidationError(
                "cancellation probe callback must be callable"
            )

    @property
    def binding_digest(self) -> str:
        return hashlib.sha256(
            _canonical_json(
                {
                    "schema": "cancellation_probe_binding_v1",
                    "run_id": self.run_id,
                    "node_id": self.node_id,
                    "attempt_id": self.attempt_id,
                }
            )
        ).hexdigest()

    def validate_for(self, action: ActionRequest) -> None:
        if (
            self.run_id != action.run_id
            or self.node_id != action.node_id
            or self.attempt_id != action.attempt_id
        ):
            raise SandboxValidationError(
                "cancellation probe does not match the Action identity"
            )

    def poll(self) -> CancellationSignal:
        try:
            return CancellationSignal(self.callback())
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return CancellationSignal.UNKNOWN


@dataclass(frozen=True, slots=True)
class ResourceLimits:
    timeout_seconds: float = 60.0
    cpu_seconds: float = 60.0
    memory_bytes: int = 512 * 1024 * 1024
    output_bytes: int = 1024 * 1024
    process_count: int = 16
    schema_version: int = SANDBOX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name, maximum in (
            ("timeout_seconds", MAX_TIMEOUT_SECONDS),
            ("cpu_seconds", MAX_CPU_SECONDS),
        ):
            try:
                number = float(getattr(self, field_name))
            except (TypeError, ValueError) as exc:
                raise SandboxValidationError(f"{field_name} must be finite") from exc
            if not math.isfinite(number) or number <= 0 or number > maximum:
                raise SandboxValidationError(f"{field_name} exceeds the safe bound")
            object.__setattr__(self, field_name, number)
        for field_name, maximum in (
            ("memory_bytes", MAX_MEMORY_BYTES),
            ("output_bytes", MAX_OUTPUT_BYTES),
            ("process_count", MAX_PROCESSES),
        ):
            number = getattr(self, field_name)
            if isinstance(number, bool) or not isinstance(number, int):
                raise SandboxValidationError(f"{field_name} must be an integer")
            if number <= 0 or number > maximum:
                raise SandboxValidationError(f"{field_name} exceeds the safe bound")
        if self.schema_version != SANDBOX_SCHEMA_VERSION:
            raise SandboxValidationError("unsupported resource-limit schema_version")

    def is_within(self, ceiling: "ResourceLimits") -> bool:
        return (
            self.timeout_seconds <= ceiling.timeout_seconds
            and self.cpu_seconds <= ceiling.cpu_seconds
            and self.memory_bytes <= ceiling.memory_bytes
            and self.output_bytes <= ceiling.output_bytes
            and self.process_count <= ceiling.process_count
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "timeout_seconds": self.timeout_seconds,
            "cpu_seconds": self.cpu_seconds,
            "memory_bytes": self.memory_bytes,
            "output_bytes": self.output_bytes,
            "process_count": self.process_count,
        }


@dataclass(frozen=True, slots=True)
class EnvironmentBinding:
    """An environment name plus a trusted-boundary redacted/HMAC digest."""

    name: str
    redacted_digest: str
    schema_version: int = SANDBOX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        name = _bounded_text(self.name, "environment name", max_chars=128)
        if not (name[0].isalpha() or name[0] == "_"):
            raise SandboxValidationError("environment name is invalid")
        if any(not (character.isalnum() or character == "_") for character in name):
            raise SandboxValidationError("environment name is invalid")
        object.__setattr__(self, "name", name)
        object.__setattr__(
            self,
            "redacted_digest",
            _sha256(self.redacted_digest, "environment redacted_digest"),
        )
        if self.schema_version != SANDBOX_SCHEMA_VERSION:
            raise SandboxValidationError("unsupported environment-binding schema_version")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "name": self.name,
            "redacted_digest": self.redacted_digest,
        }


@dataclass(frozen=True, slots=True)
class SandboxProfile:
    profile_id: str
    allowed_roots: tuple[str | os.PathLike[str], ...]
    capabilities: tuple[Capability, ...]
    limits: ResourceLimits = field(default_factory=ResourceLimits)
    minimum_security_level: SecurityLevel = SecurityLevel.OS_SANDBOX
    network_mode: NetworkMode = NetworkMode.DENY
    network_allowlist: tuple[str, ...] = ()
    environment_allowlist: tuple[str, ...] = ()
    schema_version: int = SANDBOX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "profile_id", _bounded_text(self.profile_id, "profile_id"))
        if not self.allowed_roots or len(self.allowed_roots) > MAX_ALLOWED_ROOTS:
            raise SandboxValidationError("allowed_roots must be non-empty and bounded")
        roots: set[str] = set()
        for value in self.allowed_roots:
            path = Path(value).expanduser()
            try:
                resolved = path.resolve(strict=True)
            except OSError as exc:
                raise SandboxValidationError("allowed root does not exist") from exc
            if not resolved.is_dir():
                raise SandboxValidationError("allowed root must be a directory")
            roots.add(str(resolved))
        object.__setattr__(self, "allowed_roots", tuple(sorted(roots)))
        object.__setattr__(self, "capabilities", _normalized_capabilities(self.capabilities))
        if not isinstance(self.limits, ResourceLimits):
            raise SandboxValidationError("limits must be ResourceLimits")
        try:
            object.__setattr__(
                self,
                "minimum_security_level",
                SecurityLevel(self.minimum_security_level),
            )
            object.__setattr__(self, "network_mode", NetworkMode(self.network_mode))
        except ValueError as exc:
            raise SandboxValidationError("invalid sandbox profile enum") from exc

        hosts = tuple(
            sorted(
                {
                    _bounded_text(host, "network host", max_chars=253).lower()
                    for host in self.network_allowlist
                }
            )
        )
        if len(hosts) > MAX_ALLOWLIST_HOSTS:
            raise SandboxValidationError("network allowlist exceeds the safe bound")
        if self.network_mode is NetworkMode.ALLOWLIST and not hosts:
            raise SandboxValidationError("allowlist network mode requires hosts")
        if self.network_mode is not NetworkMode.ALLOWLIST and hosts:
            raise SandboxValidationError("network hosts require allowlist mode")
        object.__setattr__(self, "network_allowlist", hosts)

        env_names = tuple(
            sorted(
                {
                    EnvironmentBinding(name, "0" * 64).name
                    for name in self.environment_allowlist
                }
            )
        )
        if len(env_names) > MAX_ENV_BINDINGS:
            raise SandboxValidationError("environment allowlist exceeds the safe bound")
        object.__setattr__(self, "environment_allowlist", env_names)
        if self.schema_version != SANDBOX_SCHEMA_VERSION:
            raise SandboxValidationError("unsupported sandbox-profile schema_version")

    @property
    def profile_digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_dict())).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "profile_id": self.profile_id,
            "allowed_roots": list(self.allowed_roots),
            "capabilities": [capability.name for capability in self.capabilities],
            "limits": self.limits.to_dict(),
            "minimum_security_level": self.minimum_security_level.value,
            "network_mode": self.network_mode.value,
            "network_allowlist": list(self.network_allowlist),
            "environment_allowlist": list(self.environment_allowlist),
        }


@dataclass(frozen=True, slots=True)
class ExecutionRequest:
    """A concrete authorized request.

    ``argv`` is non-secret control data.  Secrets are forbidden there because
    its deterministic digest is offline-verifiable; use an
    :class:`EnvironmentBinding` carrying a trusted-boundary redacted/HMAC
    digest instead.
    """

    action: ActionRequest
    policy_decision: PolicyDecision
    argv: tuple[str, ...] = field(repr=False)
    operation_key: str = field(repr=False)
    idempotency_key: str = field(repr=False)
    cwd: str = ""
    limits: ResourceLimits = field(default_factory=ResourceLimits)
    input_artifact_refs: tuple[ArtifactRef, ...] = ()
    script_artifact_ref: ArtifactRef | None = None
    materialized_script: bytes | None = field(default=None, repr=False)
    environment: tuple[EnvironmentBinding, ...] = field(default=(), repr=False)
    cancellation_probe: CancellationProbe | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    schema_version: int = SANDBOX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.action, ActionRequest):
            raise SandboxValidationError("action must be ActionRequest")
        if not isinstance(self.policy_decision, PolicyDecision):
            raise SandboxValidationError("policy_decision must be PolicyDecision")
        for field_name in ("operation_key", "idempotency_key"):
            value = _bounded_text(
                getattr(self, field_name),
                field_name,
                max_chars=MAX_OPERATION_KEY_CHARS,
            )
            digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
            if digest != getattr(self.action, f"{field_name}_digest"):
                raise SandboxValidationError(f"{field_name} does not match action binding")
            object.__setattr__(self, field_name, value)
        if not isinstance(self.argv, tuple) or not self.argv:
            raise SandboxValidationError("argv must be a non-empty tuple, never a shell string")
        argv: list[str] = []
        for item in self.argv:
            argv.append(_bounded_text(item, "argv item", max_chars=MAX_ARGV_ITEM_CHARS))
        if len(argv) > MAX_ARGV_ITEMS:
            raise SandboxValidationError("argv exceeds the safe item bound")
        if len(_canonical_json(argv)) > MAX_ARGV_BYTES:
            raise SandboxValidationError("argv exceeds the safe byte bound")
        object.__setattr__(self, "argv", tuple(argv))
        if not isinstance(self.cwd, (str, os.PathLike)):
            raise SandboxValidationError("cwd must be a path")
        cwd = os.fspath(self.cwd)
        if "\x00" in cwd or len(cwd) > 4096:
            raise SandboxValidationError("cwd is invalid")
        object.__setattr__(self, "cwd", cwd)
        if not isinstance(self.limits, ResourceLimits):
            raise SandboxValidationError("limits must be ResourceLimits")
        refs = tuple(self.input_artifact_refs)
        for ref in refs:
            _artifact_identity(ref)
        object.__setattr__(self, "input_artifact_refs", refs)
        script_ref = self.script_artifact_ref
        materialized_script = self.materialized_script
        if (script_ref is None) != (materialized_script is None):
            raise SandboxValidationError(
                "script_artifact_ref and materialized_script must be supplied together"
            )
        if self.action.requires_script_artifact and script_ref is None:
            raise SandboxValidationError(
                "authorized tool requires a verified script Artifact"
            )
        script_sha256 = None if script_ref is None else script_ref.sha256
        if script_sha256 != self.action.script_artifact_sha256:
            raise SandboxValidationError(
                "script Artifact does not match the authorized action"
            )
        if script_ref is not None:
            _execution_artifact_identity(script_ref)
            if script_ref.metadata:
                raise SandboxValidationError("script ArtifactRef metadata must be empty")
            if not isinstance(materialized_script, bytes):
                raise SandboxValidationError("materialized_script must be immutable bytes")
            if len(materialized_script) > MAX_SCRIPT_BYTES:
                raise SandboxValidationError("materialized_script exceeds the safe bound")
            if (
                len(materialized_script) != script_ref.size
                or hashlib.sha256(materialized_script).hexdigest() != script_ref.sha256
            ):
                raise SandboxValidationError(
                    "materialized_script bytes do not match script ArtifactRef"
                )
        environment = tuple(self.environment)
        if len(environment) > MAX_ENV_BINDINGS:
            raise SandboxValidationError("environment exceeds the safe bound")
        by_name: dict[str, EnvironmentBinding] = {}
        for binding in environment:
            if not isinstance(binding, EnvironmentBinding):
                raise SandboxValidationError(
                    "environment must contain EnvironmentBinding values"
                )
            if binding.name in by_name:
                raise SandboxValidationError("environment names must be unique")
            by_name[binding.name] = binding
        object.__setattr__(
            self,
            "environment",
            tuple(by_name[name] for name in sorted(by_name)),
        )
        if self.cancellation_probe is not None:
            if not isinstance(self.cancellation_probe, CancellationProbe):
                raise SandboxValidationError(
                    "cancellation_probe must be a CancellationProbe"
                )
            self.cancellation_probe.validate_for(self.action)
        if self.schema_version != SANDBOX_SCHEMA_VERSION:
            raise SandboxValidationError("unsupported execution-request schema_version")

    @property
    def argv_digest(self) -> str:
        return hashlib.sha256(_canonical_json(list(self.argv))).hexdigest()

    @property
    def request_digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_dict())).hexdigest()

    def validated_for_profile(self, profile: SandboxProfile) -> "ExecutionRequest":
        if not isinstance(profile, SandboxProfile):
            raise SandboxValidationError("profile must be SandboxProfile")
        normalized = replace(
            self,
            cwd=str(_validated_cwd(self.cwd, profile.allowed_roots)),
        )
        expected = build_execution_binding_digest(
            argv=normalized.argv,
            cwd=normalized.cwd,
            profile=profile,
            limits=normalized.limits,
            input_artifact_refs=normalized.input_artifact_refs,
            script_artifact_ref=normalized.script_artifact_ref,
            operation_key=normalized.operation_key,
            idempotency_key=normalized.idempotency_key,
            environment=normalized.environment,
            capabilities=normalized.action.capabilities,
            resource_locks=normalized.action.resource_locks,
        )
        if normalized.action.execution_binding_digest != expected:
            raise SandboxDispatchDenied("execution_binding_mismatch")
        return normalized

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "action_digest": self.action.action_digest,
            "policy_version": self.policy_decision.policy_version,
            "argv_digest": self.argv_digest,
            "argv_count": len(self.argv),
            "operation_key_digest": hashlib.sha256(
                self.operation_key.encode("utf-8")
            ).hexdigest(),
            "idempotency_key_digest": hashlib.sha256(
                self.idempotency_key.encode("utf-8")
            ).hexdigest(),
            "cwd": self.cwd,
            "limits": self.limits.to_dict(),
            "input_artifact_refs": [
                _artifact_identity(ref) for ref in self.input_artifact_refs
            ],
            "script_artifact_ref": (
                None
                if self.script_artifact_ref is None
                else _execution_artifact_identity(self.script_artifact_ref)
            ),
            "environment": [binding.to_dict() for binding in self.environment],
            "cancellation_probe_binding_digest": (
                None
                if self.cancellation_probe is None
                else self.cancellation_probe.binding_digest
            ),
        }


def build_execution_binding_digest(
    *,
    argv: tuple[str, ...],
    cwd: str,
    profile: SandboxProfile,
    limits: ResourceLimits,
    input_artifact_refs: tuple[ArtifactRef, ...] = (),
    script_artifact_ref: ArtifactRef | None = None,
    operation_key: str,
    idempotency_key: str,
    environment: tuple[EnvironmentBinding, ...] = (),
    capabilities: Iterable[Capability | str] = (),
    resource_locks: Iterable[str] = (),
) -> str:
    """Bind every execution-relevant value without retaining raw argv or cwd."""

    if not isinstance(profile, SandboxProfile):
        raise SandboxValidationError("profile must be SandboxProfile")
    if not isinstance(argv, tuple) or not argv:
        raise SandboxValidationError("argv must be a non-empty tuple, never a shell string")
    normalized_argv = tuple(
        _bounded_text(item, "argv item", max_chars=MAX_ARGV_ITEM_CHARS)
        for item in argv
    )
    if len(normalized_argv) > MAX_ARGV_ITEMS:
        raise SandboxValidationError("argv exceeds the safe item bound")
    encoded_argv = _canonical_json(list(normalized_argv))
    if len(encoded_argv) > MAX_ARGV_BYTES:
        raise SandboxValidationError("argv exceeds the safe byte bound")
    if not isinstance(limits, ResourceLimits):
        raise SandboxValidationError("limits must be ResourceLimits")

    refs = tuple(input_artifact_refs)
    if len(refs) > 64:
        raise SandboxValidationError("input ArtifactRefs exceed the safe bound")
    ref_identities = [_execution_artifact_identity(ref) for ref in refs]

    script_identity = (
        None
        if script_artifact_ref is None
        else _execution_artifact_identity(script_artifact_ref)
    )
    operation_key_value = _bounded_text(
        operation_key,
        "operation_key",
        max_chars=MAX_OPERATION_KEY_CHARS,
    )
    idempotency_key_value = _bounded_text(
        idempotency_key,
        "idempotency_key",
        max_chars=MAX_OPERATION_KEY_CHARS,
    )
    bindings = tuple(environment)
    if len(bindings) > MAX_ENV_BINDINGS:
        raise SandboxValidationError("environment exceeds the safe bound")
    by_name: dict[str, EnvironmentBinding] = {}
    for binding in bindings:
        if not isinstance(binding, EnvironmentBinding):
            raise SandboxValidationError(
                "environment must contain EnvironmentBinding values"
            )
        if binding.name in by_name:
            raise SandboxValidationError("environment names must be unique")
        by_name[binding.name] = binding

    normalized_capabilities = _normalized_capabilities(capabilities)
    normalized_locks = tuple(
        sorted(
            {
                _bounded_text(lock, "resource_lock", max_chars=256)
                for lock in resource_locks
            }
        )
    )
    if len(normalized_locks) > 32:
        raise SandboxValidationError("resource_locks exceed the bounded metadata limit")

    intent = {
        "schema": "execution_binding_v1",
        "argv_digest": hashlib.sha256(encoded_argv).hexdigest(),
        "argv_count": len(normalized_argv),
        "cwd_digest": hashlib.sha256(
            str(_validated_cwd(cwd, profile.allowed_roots)).encode("utf-8")
        ).hexdigest(),
        "profile_digest": profile.profile_digest,
        "limits": limits.to_dict(),
        "input_artifact_refs": ref_identities,
        "script_artifact_ref": script_identity,
        "operation_key_digest": hashlib.sha256(
            operation_key_value.encode("utf-8")
        ).hexdigest(),
        "idempotency_key_digest": hashlib.sha256(
            idempotency_key_value.encode("utf-8")
        ).hexdigest(),
        "environment": [
            by_name[name].to_dict() for name in sorted(by_name)
        ],
        "capabilities": [
            capability.name for capability in normalized_capabilities
        ],
        "resource_locks": list(normalized_locks),
    }
    return hashlib.sha256(_canonical_json(intent)).hexdigest()


@dataclass(frozen=True, slots=True)
class BackendExecutionResult:
    exit_code: int | None
    timed_out: bool = False
    cancelled: bool = False
    cancellation_uncertain: bool = False
    output_artifact_refs: tuple[ArtifactRef, ...] = ()
    schema_version: int = SANDBOX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.exit_code is not None:
            if isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int):
                raise SandboxValidationError("exit_code must be an integer or null")
            if self.exit_code < -65535 or self.exit_code > 65535:
                raise SandboxValidationError("exit_code exceeds the safe bound")
        if not isinstance(self.timed_out, bool):
            raise SandboxValidationError("timed_out must be boolean")
        if not isinstance(self.cancelled, bool):
            raise SandboxValidationError("cancelled must be boolean")
        if not isinstance(self.cancellation_uncertain, bool):
            raise SandboxValidationError(
                "cancellation_uncertain must be boolean"
            )
        if sum(
            (
                self.timed_out,
                self.cancelled,
                self.cancellation_uncertain,
            )
        ) > 1:
            raise SandboxValidationError(
                "backend result terminal signals are mutually exclusive"
            )
        refs = tuple(self.output_artifact_refs)
        for ref in refs:
            _artifact_identity(ref)
        object.__setattr__(self, "output_artifact_refs", refs)
        if self.schema_version != SANDBOX_SCHEMA_VERSION:
            raise SandboxValidationError("unsupported backend-result schema_version")


@runtime_checkable
class SandboxBackend(Protocol):
    """An externally supplied backend that actually enforces isolation."""

    backend_id: str
    security_level: SecurityLevel
    capabilities: tuple[Capability, ...]
    supports_materialized_script: bool

    def execute(
        self,
        request: ExecutionRequest,
        profile: SandboxProfile,
    ) -> BackendExecutionResult: ...


@dataclass(frozen=True, slots=True)
class SandboxReceipt:
    backend_id: str
    security_level: SecurityLevel
    profile_id: str
    profile_digest: str
    action_digest: str
    policy_version: str
    request_digest: str
    outcome: SandboxOutcome
    exit_code: int | None
    timed_out: bool
    output_artifact_refs: tuple[ArtifactRef, ...] = ()
    error_code: str | None = None
    schema_version: int = SANDBOX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in ("backend_id", "profile_id", "policy_version"):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(getattr(self, field_name), field_name),
            )
        try:
            object.__setattr__(self, "security_level", SecurityLevel(self.security_level))
            object.__setattr__(self, "outcome", SandboxOutcome(self.outcome))
        except ValueError as exc:
            raise SandboxValidationError("invalid sandbox receipt enum") from exc
        for field_name in ("profile_digest", "action_digest", "request_digest"):
            object.__setattr__(
                self,
                field_name,
                _sha256(getattr(self, field_name), field_name),
            )
        if self.exit_code is not None and (
            isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int)
        ):
            raise SandboxValidationError("receipt exit_code must be an integer or null")
        if not isinstance(self.timed_out, bool):
            raise SandboxValidationError("receipt timed_out must be boolean")
        refs = tuple(self.output_artifact_refs)
        if len(refs) > MAX_RECEIPT_ARTIFACT_REFS:
            raise SandboxValidationError(
                "sandbox receipt exceeds the ArtifactRef bound"
            )
        for ref in refs:
            _artifact_identity(ref)
        object.__setattr__(self, "output_artifact_refs", refs)
        if self.error_code is not None:
            object.__setattr__(
                self,
                "error_code",
                _bounded_text(self.error_code, "error_code", max_chars=128),
            )
        if self.schema_version != SANDBOX_SCHEMA_VERSION:
            raise SandboxValidationError("unsupported sandbox-receipt schema_version")

    @property
    def receipt_digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_dict())).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "backend_id": self.backend_id,
            "security_level": self.security_level.value,
            "profile_id": self.profile_id,
            "profile_digest": self.profile_digest,
            "action_digest": self.action_digest,
            "policy_version": self.policy_version,
            "request_digest": self.request_digest,
            "outcome": self.outcome.value,
            "exit_code": self.exit_code,
            "timed_out": self.timed_out,
            "output_artifact_refs": [
                _artifact_identity(ref) for ref in self.output_artifact_refs
            ],
            "error_code": self.error_code,
        }

    @classmethod
    def validate_serialized(
        cls,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Validate the allowlisted, secret-free durable receipt shape."""

        if not isinstance(payload, Mapping):
            raise SandboxValidationError("sandbox receipt must be an object")
        required = {
            "schema_version",
            "backend_id",
            "security_level",
            "profile_id",
            "profile_digest",
            "action_digest",
            "policy_version",
            "request_digest",
            "outcome",
            "exit_code",
            "timed_out",
            "output_artifact_refs",
            "error_code",
        }
        if set(payload) != required:
            raise SandboxValidationError(
                "sandbox receipt contains unknown or missing fields"
            )
        if payload.get("schema_version") != SANDBOX_SCHEMA_VERSION:
            raise SandboxValidationError("unsupported sandbox-receipt schema_version")
        backend_id = _bounded_text(payload.get("backend_id"), "backend_id")
        profile_id = _bounded_text(payload.get("profile_id"), "profile_id")
        policy_version = _bounded_text(
            payload.get("policy_version"),
            "policy_version",
        )
        try:
            security_level = SecurityLevel(payload.get("security_level"))
            outcome = SandboxOutcome(payload.get("outcome"))
        except ValueError as exc:
            raise SandboxValidationError("invalid sandbox receipt enum") from exc
        profile_digest = _sha256(
            payload.get("profile_digest"),
            "profile_digest",
        )
        action_digest = _sha256(
            payload.get("action_digest"),
            "action_digest",
        )
        request_digest = _sha256(
            payload.get("request_digest"),
            "request_digest",
        )
        exit_code = payload.get("exit_code")
        if exit_code is not None and (
            isinstance(exit_code, bool)
            or not isinstance(exit_code, int)
            or exit_code < -65535
            or exit_code > 65535
        ):
            raise SandboxValidationError(
                "receipt exit_code must be a bounded integer or null"
            )
        timed_out = payload.get("timed_out")
        if not isinstance(timed_out, bool):
            raise SandboxValidationError("receipt timed_out must be boolean")
        refs = payload.get("output_artifact_refs")
        if (
            not isinstance(refs, list)
            or len(refs) > MAX_RECEIPT_ARTIFACT_REFS
        ):
            raise SandboxValidationError(
                "sandbox receipt output ArtifactRefs must be a bounded list"
            )
        safe_refs: list[dict[str, Any]] = []
        for raw_ref in refs:
            if not isinstance(raw_ref, Mapping) or set(raw_ref) != {
                "artifact_id",
                "sha256",
                "size",
                "kind",
            }:
                raise SandboxValidationError(
                    "sandbox receipt ArtifactRef identity is invalid"
                )
            artifact_id = _bounded_text(
                raw_ref.get("artifact_id"),
                "artifact_id",
                max_chars=255,
            )
            artifact_digest = _sha256(
                raw_ref.get("sha256"),
                "artifact sha256",
            )
            size = raw_ref.get("size")
            if isinstance(size, bool) or not isinstance(size, int) or size < 0:
                raise SandboxValidationError(
                    "sandbox receipt ArtifactRef size is invalid"
                )
            try:
                kind = ArtifactKind(raw_ref.get("kind"))
            except ValueError as exc:
                raise SandboxValidationError(
                    "sandbox receipt ArtifactRef kind is invalid"
                ) from exc
            safe_refs.append(
                {
                    "artifact_id": artifact_id,
                    "sha256": artifact_digest,
                    "size": size,
                    "kind": kind.value,
                }
            )
        error_code = payload.get("error_code")
        if error_code is not None:
            error_code = _bounded_text(
                error_code,
                "error_code",
                max_chars=128,
            )
        return {
            "schema_version": SANDBOX_SCHEMA_VERSION,
            "backend_id": backend_id,
            "security_level": security_level.value,
            "profile_id": profile_id,
            "profile_digest": profile_digest,
            "action_digest": action_digest,
            "policy_version": policy_version,
            "request_digest": request_digest,
            "outcome": outcome.value,
            "exit_code": exit_code,
            "timed_out": timed_out,
            "output_artifact_refs": safe_refs,
            "error_code": error_code,
        }


class SandboxDispatcher:
    """Select one qualified backend and never fall back after execution starts."""

    def __init__(
        self,
        backends: Iterable[SandboxBackend],
        *,
        policy_version: str,
    ) -> None:
        self.policy_version = _bounded_text(policy_version, "policy_version")
        backend_map: dict[str, SandboxBackend] = {}
        for backend in backends:
            if not isinstance(backend, SandboxBackend):
                raise SandboxValidationError("backend does not implement SandboxBackend")
            backend_id = _bounded_text(backend.backend_id, "backend_id")
            if backend_id in backend_map:
                raise SandboxValidationError("backend IDs must be unique")
            try:
                SecurityLevel(backend.security_level)
            except ValueError as exc:
                raise SandboxValidationError("backend security_level is invalid") from exc
            _normalized_capabilities(backend.capabilities)
            if not isinstance(backend.supports_materialized_script, bool):
                raise SandboxValidationError(
                    "backend supports_materialized_script must be boolean"
                )
            backend_map[backend_id] = backend
        self._backends = tuple(backend_map.values())

    def dispatch(
        self,
        request: ExecutionRequest,
        profile: SandboxProfile,
    ) -> SandboxReceipt:
        if not isinstance(request, ExecutionRequest):
            raise SandboxValidationError("request must be ExecutionRequest")
        if not isinstance(profile, SandboxProfile):
            raise SandboxValidationError("profile must be SandboxProfile")
        decision = request.policy_decision
        if decision.outcome is not PolicyOutcome.ALLOW:
            raise SandboxDispatchDenied("policy_not_allowed")
        if decision.action_digest != request.action.action_digest:
            raise SandboxDispatchDenied("policy_action_mismatch")
        if decision.policy_version != self.policy_version:
            raise SandboxDispatchDenied("policy_version_mismatch")
        if not request.limits.is_within(profile.limits):
            raise SandboxDispatchDenied("resource_limits_exceed_profile")

        # The backend receives a normalized copy and cannot accidentally use an
        # unresolved caller path.  This also rechecks the policy-bound intent.
        request = request.validated_for_profile(profile)

        profile_capabilities = {capability.name for capability in profile.capabilities}
        action_capabilities = {
            capability.name for capability in request.action.capabilities
        }
        if not action_capabilities.issubset(profile_capabilities):
            raise SandboxDispatchDenied("profile_capability_mismatch")
        environment_names = {binding.name for binding in request.environment}
        if not environment_names.issubset(profile.environment_allowlist):
            raise SandboxDispatchDenied("environment_not_allowed")

        required_capabilities = set(profile_capabilities)
        network_capability = _NETWORK_CAPABILITY.get(profile.network_mode)
        if network_capability is not None:
            required_capabilities.add(network_capability)

        candidates: list[SandboxBackend] = []
        for backend in self._backends:
            level = SecurityLevel(backend.security_level)
            if _SECURITY_RANK[level] < _SECURITY_RANK[profile.minimum_security_level]:
                continue
            if (
                level is SecurityLevel.DEVELOPMENT_UNSAFE
                and request.action.effect_class is not EffectClass.READ_ONLY
            ):
                continue
            backend_capabilities = {
                capability.name
                for capability in _normalized_capabilities(backend.capabilities)
            }
            if not required_capabilities.issubset(backend_capabilities):
                continue
            if (
                request.script_artifact_ref is not None
                and not backend.supports_materialized_script
            ):
                continue
            candidates.append(backend)
        if not candidates:
            raise SandboxDispatchDenied("no_qualified_backend")
        candidates.sort(
            key=lambda backend: (
                -_SECURITY_RANK[SecurityLevel(backend.security_level)],
                backend.backend_id,
            )
        )
        backend = candidates[0]

        try:
            result = backend.execute(request, profile)
            if not isinstance(result, BackendExecutionResult):
                raise SandboxValidationError("backend returned an invalid result")
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            return self._receipt(
                backend,
                request,
                profile,
                outcome=SandboxOutcome.BACKEND_ERROR,
                exit_code=None,
                timed_out=False,
                error_code="backend_exception",
            )

        if result.cancellation_uncertain:
            outcome = SandboxOutcome.CANCELLATION_UNKNOWN
        elif result.cancelled:
            outcome = SandboxOutcome.CANCELLED
        elif result.timed_out:
            outcome = SandboxOutcome.TIMED_OUT
        elif result.exit_code == 0:
            outcome = SandboxOutcome.SUCCEEDED
        else:
            outcome = SandboxOutcome.FAILED
        return self._receipt(
            backend,
            request,
            profile,
            outcome=outcome,
            exit_code=result.exit_code,
            timed_out=result.timed_out,
            output_artifact_refs=result.output_artifact_refs,
        )

    @staticmethod
    def _receipt(
        backend: SandboxBackend,
        request: ExecutionRequest,
        profile: SandboxProfile,
        *,
        outcome: SandboxOutcome,
        exit_code: int | None,
        timed_out: bool,
        output_artifact_refs: tuple[ArtifactRef, ...] = (),
        error_code: str | None = None,
    ) -> SandboxReceipt:
        return SandboxReceipt(
            backend_id=backend.backend_id,
            security_level=backend.security_level,
            profile_id=profile.profile_id,
            profile_digest=profile.profile_digest,
            action_digest=request.action.action_digest,
            policy_version=request.policy_decision.policy_version,
            request_digest=request.request_digest,
            outcome=outcome,
            exit_code=exit_code,
            timed_out=timed_out,
            output_artifact_refs=output_artifact_refs,
            error_code=error_code,
        )
