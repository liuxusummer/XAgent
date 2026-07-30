"""OCI/gVisor-oriented reference backend with fail-closed attestation.

This module builds a restrictive, path-free execution specification.  It does
not start containers itself and cannot prove isolation merely from a
``runtimeClassName`` string.  Construction and every execution require a
deployment-injected :class:`RuntimeAttestationVerifier` to attest the exact
adapter as a live gVisor runtime.
"""

from __future__ import annotations

import hashlib
import json
import math
import posixpath
import re
import time
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping, Protocol, runtime_checkable

from .artifacts import ArtifactRef
from .policy import Capability, PolicyOutcome
from .sandbox import (
    BackendExecutionResult,
    ExecutionRequest,
    NetworkMode,
    ResourceLimits,
    SandboxProfile,
    SecurityLevel,
)

OCI_BACKEND_SCHEMA_VERSION = 1
MAX_RUNTIME_ATTESTATION_TTL_SECONDS = 5 * 60.0
MAX_EPHEMERAL_MOUNTS = 128
MAX_TMPFS_BYTES = 4 * 1024 * 1024 * 1024
_IMAGE_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,510}@sha256:([0-9a-f]{64})$"
)
_FORBIDDEN_SHELLS = frozenset(
    {
        "ash",
        "bash",
        "csh",
        "dash",
        "fish",
        "ksh",
        "powershell",
        "pwsh",
        "sh",
        "tcsh",
        "zsh",
    }
)


class OciBackendError(RuntimeError):
    """Base reference-backend error."""


class OciSpecDenied(OciBackendError, ValueError):
    """The request cannot be represented by the restrictive OCI profile."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = _bounded_text(reason_code, "reason_code", max_chars=128)
        super().__init__(self.reason_code)


class RuntimeAttestationInvalid(OciBackendError):
    """No current trusted proof exists for the injected runtime adapter."""


class OciReceiptBindingError(OciBackendError):
    """The runtime result did not bind the exact authorized specification."""


def _bounded_text(value: Any, field_name: str, *, max_chars: int = 256) -> str:
    if not isinstance(value, str):
        raise OciSpecDenied("invalid_oci_metadata")
    text = value.strip()
    if (
        not text
        or len(text) > max_chars
        or any(ord(character) < 32 or ord(character) == 127 for character in text)
    ):
        raise OciSpecDenied("invalid_oci_metadata")
    return text


def _sha256(value: Any) -> str:
    digest = str(value or "")
    if (
        len(digest) != 64
        or digest != digest.lower()
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise OciSpecDenied("invalid_oci_digest")
    return digest


def _timestamp(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise OciSpecDenied("invalid_oci_time") from exc
    if not math.isfinite(result) or result < 0:
        raise OciSpecDenied("invalid_oci_time")
    return result


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise OciSpecDenied("invalid_oci_metadata") from exc


def _canonical_digest(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _container_path(value: Any, field_name: str) -> str:
    text = _bounded_text(value, field_name, max_chars=1024)
    if not text.startswith("/") or "\\" in text or "\x00" in text:
        raise OciSpecDenied("invalid_container_path")
    normalized = posixpath.normpath(text)
    parts = PurePosixPath(text).parts
    if normalized != text or ".." in parts or text == "/":
        raise OciSpecDenied("invalid_container_path")
    return text


def _artifact_binding_digest(ref: ArtifactRef) -> str:
    if not isinstance(ref, ArtifactRef):
        raise OciSpecDenied("invalid_artifact_reference")
    return _canonical_digest(
        {
            "schema": "oci_artifact_mount_v1",
            "artifact_ref": ref.to_dict(),
        }
    )


class OciMountKind(StrEnum):
    TMPFS = "tmpfs"
    ARTIFACT = "artifact"


class RuntimeIsolationKind(StrEnum):
    GVISOR = "gvisor"


@dataclass(frozen=True, slots=True)
class RuntimeAttestation:
    """Trusted verifier output for one exact injected adapter."""

    adapter_id: str
    isolation_kind: RuntimeIsolationKind
    runtime_class: str
    runtime_binary_digest: str
    verifier_id: str
    attestation_id: str
    issued_at: float
    expires_at: float
    schema_version: int = OCI_BACKEND_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in (
            "adapter_id",
            "runtime_class",
            "verifier_id",
            "attestation_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(getattr(self, field_name), field_name),
            )
        try:
            object.__setattr__(
                self,
                "isolation_kind",
                RuntimeIsolationKind(self.isolation_kind),
            )
        except ValueError as exc:
            raise RuntimeAttestationInvalid("unsupported runtime isolation") from exc
        object.__setattr__(
            self,
            "runtime_binary_digest",
            _sha256(self.runtime_binary_digest),
        )
        object.__setattr__(self, "issued_at", _timestamp(self.issued_at))
        object.__setattr__(self, "expires_at", _timestamp(self.expires_at))
        if self.expires_at <= self.issued_at:
            raise RuntimeAttestationInvalid("runtime attestation window is empty")
        if self.schema_version != OCI_BACKEND_SCHEMA_VERSION:
            raise RuntimeAttestationInvalid(
                "unsupported runtime attestation schema_version"
            )

    @property
    def attestation_digest(self) -> str:
        return _canonical_digest(
            {
                "schema": "oci_runtime_attestation_v1",
                "adapter_id": self.adapter_id,
                "isolation_kind": self.isolation_kind.value,
                "runtime_class": self.runtime_class,
                "runtime_binary_digest": self.runtime_binary_digest,
                "verifier_id": self.verifier_id,
                "attestation_id": self.attestation_id,
                "issued_at": self.issued_at,
                "expires_at": self.expires_at,
            }
        )

    def validate_current(
        self,
        *,
        adapter_id: str,
        runtime_class: str,
        now: float,
    ) -> None:
        current = _timestamp(now)
        if self.adapter_id != adapter_id:
            raise RuntimeAttestationInvalid("runtime adapter binding mismatch")
        if self.runtime_class != runtime_class:
            raise RuntimeAttestationInvalid("runtime class binding mismatch")
        if self.isolation_kind is not RuntimeIsolationKind.GVISOR:
            raise RuntimeAttestationInvalid("gVisor attestation is required")
        if (
            self.expires_at - self.issued_at
            > MAX_RUNTIME_ATTESTATION_TTL_SECONDS
        ):
            raise RuntimeAttestationInvalid("runtime attestation lifetime exceeds policy")
        if self.issued_at > current or current >= self.expires_at:
            raise RuntimeAttestationInvalid("runtime attestation is not current")


@runtime_checkable
class OciRuntimeAdapter(Protocol):
    """Injected adapter; execute receives argv, never a shell command string."""

    adapter_id: str

    def execute(self, spec: "OciSandboxSpec") -> "OciRuntimeResult": ...


@runtime_checkable
class RuntimeAttestationVerifier(Protocol):
    """Deployment trust anchor that verifies the exact adapter out of band."""

    def verify(
        self,
        adapter: OciRuntimeAdapter,
        *,
        now: float,
    ) -> RuntimeAttestation: ...


@dataclass(frozen=True, slots=True)
class EphemeralMount:
    kind: OciMountKind
    target: str
    read_only: bool
    source_binding_digest: str | None = None
    size_bytes: int | None = None
    schema_version: int = OCI_BACKEND_SCHEMA_VERSION

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "kind", OciMountKind(self.kind))
        except ValueError as exc:
            raise OciSpecDenied("invalid_mount_kind") from exc
        object.__setattr__(
            self,
            "target",
            _container_path(self.target, "mount target"),
        )
        if not isinstance(self.read_only, bool):
            raise OciSpecDenied("invalid_mount_read_only")
        if self.kind is OciMountKind.ARTIFACT:
            if not self.read_only or self.source_binding_digest is None:
                raise OciSpecDenied("artifact_mount_must_be_bound_read_only")
            object.__setattr__(
                self,
                "source_binding_digest",
                _sha256(self.source_binding_digest),
            )
            if self.size_bytes is not None:
                raise OciSpecDenied("artifact_mount_size_is_implicit")
        else:
            if self.source_binding_digest is not None or self.read_only:
                raise OciSpecDenied("tmpfs_mount_must_be_ephemeral_writable")
            if (
                isinstance(self.size_bytes, bool)
                or not isinstance(self.size_bytes, int)
                or self.size_bytes <= 0
                or self.size_bytes > MAX_TMPFS_BYTES
            ):
                raise OciSpecDenied("tmpfs_size_exceeds_policy")
        if self.schema_version != OCI_BACKEND_SCHEMA_VERSION:
            raise OciSpecDenied("unsupported_oci_schema")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "target": self.target,
            "read_only": self.read_only,
            "source_binding_digest": self.source_binding_digest,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class MaterializedFile:
    """Verified bytes installed only into an ephemeral container mount."""

    target: str
    sha256: str
    content: bytes = field(repr=False)
    schema_version: int = OCI_BACKEND_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "target",
            _container_path(self.target, "materialized file target"),
        )
        object.__setattr__(self, "sha256", _sha256(self.sha256))
        if not isinstance(self.content, bytes):
            raise OciSpecDenied("materialized_file_must_be_bytes")
        if hashlib.sha256(self.content).hexdigest() != self.sha256:
            raise OciSpecDenied("materialized_file_digest_mismatch")
        if self.schema_version != OCI_BACKEND_SCHEMA_VERSION:
            raise OciSpecDenied("unsupported_oci_schema")

    def identity_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "target": self.target,
            "sha256": self.sha256,
            "size": len(self.content),
        }


@dataclass(frozen=True, slots=True)
class OciSandboxSpec:
    """Immutable, restrictive OCI request consumed by an attested adapter."""

    image: str
    image_digest: str
    runtime_class: str
    runtime_attestation_digest: str
    action_digest: str
    request_digest: str
    profile_digest: str
    argv: tuple[str, ...] = field(repr=False)
    working_directory: str = "/workspace"
    rootfs_read_only: bool = True
    run_as_uid: int = 65532
    run_as_gid: int = 65532
    no_new_privileges: bool = True
    dropped_capabilities: tuple[str, ...] = ("ALL",)
    seccomp_profile_digest: str = ""
    network_mode: NetworkMode = NetworkMode.DENY
    limits: ResourceLimits = field(default_factory=ResourceLimits)
    mounts: tuple[EphemeralMount, ...] = ()
    materialized_files: tuple[MaterializedFile, ...] = field(
        default=(),
        repr=False,
    )
    environment_bindings: tuple[Mapping[str, Any], ...] = ()
    schema_version: int = OCI_BACKEND_SCHEMA_VERSION

    def __post_init__(self) -> None:
        match = _IMAGE_PATTERN.fullmatch(self.image)
        if match is None or match.group(1) != self.image_digest:
            raise OciSpecDenied("image_must_use_immutable_digest")
        object.__setattr__(self, "image_digest", _sha256(self.image_digest))
        object.__setattr__(
            self,
            "runtime_class",
            _bounded_text(self.runtime_class, "runtime_class"),
        )
        for field_name in (
            "runtime_attestation_digest",
            "action_digest",
            "request_digest",
            "profile_digest",
            "seccomp_profile_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(getattr(self, field_name)),
            )
        if not isinstance(self.argv, tuple) or not self.argv:
            raise OciSpecDenied("argv_must_be_nonempty_tuple")
        argv = tuple(
            _bounded_text(item, "argv item", max_chars=4096)
            for item in self.argv
        )
        executable = PurePosixPath(argv[0])
        if (
            not executable.is_absolute()
            or executable.name.lower() in _FORBIDDEN_SHELLS
        ):
            raise OciSpecDenied("direct_non_shell_executable_required")
        object.__setattr__(self, "argv", argv)
        object.__setattr__(
            self,
            "working_directory",
            _container_path(self.working_directory, "working_directory"),
        )
        if (
            self.rootfs_read_only is not True
            or self.no_new_privileges is not True
            or self.dropped_capabilities != ("ALL",)
        ):
            raise OciSpecDenied("mandatory_oci_hardening_missing")
        for field_name in ("run_as_uid", "run_as_gid"):
            value = getattr(self, field_name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
                or value > 2**31 - 1
            ):
                raise OciSpecDenied("non_root_identity_required")
        try:
            object.__setattr__(self, "network_mode", NetworkMode(self.network_mode))
        except ValueError as exc:
            raise OciSpecDenied("invalid_network_mode") from exc
        if self.network_mode is not NetworkMode.DENY:
            raise OciSpecDenied("network_default_deny_required")
        if not isinstance(self.limits, ResourceLimits):
            raise OciSpecDenied("resource_limits_required")
        mounts = tuple(self.mounts)
        if not mounts or len(mounts) > MAX_EPHEMERAL_MOUNTS:
            raise OciSpecDenied("ephemeral_mounts_must_be_bounded")
        targets: set[str] = set()
        for mount in mounts:
            if not isinstance(mount, EphemeralMount):
                raise OciSpecDenied("invalid_ephemeral_mount")
            if mount.target in targets:
                raise OciSpecDenied("duplicate_mount_target")
            targets.add(mount.target)
        object.__setattr__(self, "mounts", mounts)
        files = tuple(self.materialized_files)
        file_targets: set[str] = set()
        for materialized in files:
            if not isinstance(materialized, MaterializedFile):
                raise OciSpecDenied("invalid_materialized_file")
            if materialized.target in file_targets:
                raise OciSpecDenied("duplicate_materialized_file")
            file_targets.add(materialized.target)
        object.__setattr__(self, "materialized_files", files)
        try:
            bindings = tuple(
                MappingProxyType(
                    json.loads(_canonical_bytes(dict(binding)).decode("utf-8"))
                )
                for binding in self.environment_bindings
            )
        except (TypeError, ValueError) as exc:
            raise OciSpecDenied("invalid_environment_binding") from exc
        if len(bindings) > 64:
            raise OciSpecDenied("environment_bindings_exceed_policy")
        object.__setattr__(self, "environment_bindings", bindings)
        if self.schema_version != OCI_BACKEND_SCHEMA_VERSION:
            raise OciSpecDenied("unsupported_oci_schema")

    @property
    def spec_digest(self) -> str:
        return _canonical_digest(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        """Return a path-free spec identity; materialized bytes stay transient."""

        return {
            "schema_version": self.schema_version,
            "image": self.image,
            "image_digest": self.image_digest,
            "runtime_class": self.runtime_class,
            "runtime_attestation_digest": self.runtime_attestation_digest,
            "action_digest": self.action_digest,
            "request_digest": self.request_digest,
            "profile_digest": self.profile_digest,
            "argv": list(self.argv),
            "working_directory": self.working_directory,
            "rootfs_read_only": self.rootfs_read_only,
            "run_as_uid": self.run_as_uid,
            "run_as_gid": self.run_as_gid,
            "no_new_privileges": self.no_new_privileges,
            "dropped_capabilities": list(self.dropped_capabilities),
            "seccomp_profile_digest": self.seccomp_profile_digest,
            "network_mode": self.network_mode.value,
            "limits": self.limits.to_dict(),
            "mounts": [mount.to_dict() for mount in self.mounts],
            "materialized_files": [
                item.identity_dict() for item in self.materialized_files
            ],
            "environment_bindings": [
                dict(binding) for binding in self.environment_bindings
            ],
        }


class OciSandboxSpecBuilder:
    """Translate an authorized request into a fixed hardened OCI shape."""

    def __init__(
        self,
        *,
        image: str,
        runtime_class: str,
        seccomp_profile_digest: str,
        run_as_uid: int = 65532,
        run_as_gid: int = 65532,
        workspace_tmpfs_bytes: int = 512 * 1024 * 1024,
        temp_tmpfs_bytes: int = 128 * 1024 * 1024,
        output_tmpfs_bytes: int = 512 * 1024 * 1024,
    ) -> None:
        match = _IMAGE_PATTERN.fullmatch(str(image))
        if match is None:
            raise OciSpecDenied("image_must_use_immutable_digest")
        self.image = str(image)
        self.image_digest = match.group(1)
        self.runtime_class = _bounded_text(runtime_class, "runtime_class")
        self.seccomp_profile_digest = _sha256(seccomp_profile_digest)
        self.run_as_uid = run_as_uid
        self.run_as_gid = run_as_gid
        self._tmpfs_sizes = (
            workspace_tmpfs_bytes,
            temp_tmpfs_bytes,
            output_tmpfs_bytes,
        )
        # Reuse mount validation for all configured bounds.
        for target, size in zip(
            ("/workspace", "/tmp", "/outputs"),
            self._tmpfs_sizes,
            strict=True,
        ):
            EphemeralMount(
                kind=OciMountKind.TMPFS,
                target=target,
                read_only=False,
                size_bytes=size,
            )

    def build(
        self,
        request: ExecutionRequest,
        profile: SandboxProfile,
        *,
        runtime_attestation_digest: str,
    ) -> OciSandboxSpec:
        if not isinstance(request, ExecutionRequest):
            raise OciSpecDenied("invalid_execution_request")
        if not isinstance(profile, SandboxProfile):
            raise OciSpecDenied("invalid_sandbox_profile")
        if request.policy_decision.outcome is not PolicyOutcome.ALLOW:
            raise OciSpecDenied("policy_not_allowed")
        if request.policy_decision.action_digest != request.action.action_digest:
            raise OciSpecDenied("policy_action_mismatch")
        if profile.network_mode is not NetworkMode.DENY:
            raise OciSpecDenied("network_default_deny_required")
        if not request.limits.is_within(profile.limits):
            raise OciSpecDenied("resource_limits_exceed_profile")
        if not {
            capability.name for capability in request.action.capabilities
        }.issubset({capability.name for capability in profile.capabilities}):
            raise OciSpecDenied("profile_capability_mismatch")
        normalized = request.validated_for_profile(profile)

        mounts: list[EphemeralMount] = [
            EphemeralMount(
                kind=OciMountKind.TMPFS,
                target=target,
                read_only=False,
                size_bytes=size,
            )
            for target, size in zip(
                ("/workspace", "/tmp", "/outputs"),
                self._tmpfs_sizes,
                strict=True,
            )
        ]
        for index, ref in enumerate(normalized.input_artifact_refs):
            mounts.append(
                EphemeralMount(
                    kind=OciMountKind.ARTIFACT,
                    target=f"/inputs/artifact-{index}",
                    read_only=True,
                    source_binding_digest=_artifact_binding_digest(ref),
                )
            )
        materialized: tuple[MaterializedFile, ...] = ()
        if normalized.script_artifact_ref is not None:
            mounts.append(
                EphemeralMount(
                    kind=OciMountKind.ARTIFACT,
                    target="/xagent/script",
                    read_only=True,
                    source_binding_digest=_artifact_binding_digest(
                        normalized.script_artifact_ref
                    ),
                )
            )
            assert normalized.materialized_script is not None
            materialized = (
                MaterializedFile(
                    target="/xagent/script/payload",
                    sha256=normalized.script_artifact_ref.sha256,
                    content=normalized.materialized_script,
                ),
            )
        return OciSandboxSpec(
            image=self.image,
            image_digest=self.image_digest,
            runtime_class=self.runtime_class,
            runtime_attestation_digest=runtime_attestation_digest,
            action_digest=normalized.action.action_digest,
            request_digest=normalized.request_digest,
            profile_digest=profile.profile_digest,
            argv=normalized.argv,
            run_as_uid=self.run_as_uid,
            run_as_gid=self.run_as_gid,
            seccomp_profile_digest=self.seccomp_profile_digest,
            limits=normalized.limits,
            mounts=tuple(mounts),
            materialized_files=materialized,
            environment_bindings=tuple(
                binding.to_dict() for binding in normalized.environment
            ),
        )


@dataclass(frozen=True, slots=True)
class OciRuntimeResult:
    """Adapter result bound to one exact spec and runtime attestation."""

    spec_digest: str
    action_digest: str
    request_digest: str
    runtime_attestation_digest: str
    exit_code: int | None
    timed_out: bool = False
    cancelled: bool = False
    cancellation_uncertain: bool = False
    output_artifact_refs: tuple[ArtifactRef, ...] = ()
    schema_version: int = OCI_BACKEND_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in (
            "spec_digest",
            "action_digest",
            "request_digest",
            "runtime_attestation_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(getattr(self, field_name)),
            )
        if self.exit_code is not None and (
            isinstance(self.exit_code, bool)
            or not isinstance(self.exit_code, int)
            or self.exit_code < -65535
            or self.exit_code > 65535
        ):
            raise OciReceiptBindingError("invalid runtime exit code")
        for field_name in (
            "timed_out",
            "cancelled",
            "cancellation_uncertain",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise OciReceiptBindingError("invalid runtime terminal signal")
        if sum(
            (
                self.timed_out,
                self.cancelled,
                self.cancellation_uncertain,
            )
        ) > 1:
            raise OciReceiptBindingError("runtime terminal signals conflict")
        refs = tuple(self.output_artifact_refs)
        if len(refs) > 64 or any(not isinstance(ref, ArtifactRef) for ref in refs):
            raise OciReceiptBindingError("invalid runtime output ArtifactRefs")
        object.__setattr__(self, "output_artifact_refs", refs)
        if self.schema_version != OCI_BACKEND_SCHEMA_VERSION:
            raise OciReceiptBindingError("unsupported runtime result schema_version")


class OciGvisorSandboxBackend:
    """SandboxBackend adapter that requires fresh deployment attestation."""

    security_level = SecurityLevel.CONTAINER
    supports_materialized_script = True

    def __init__(
        self,
        *,
        backend_id: str,
        adapter: OciRuntimeAdapter,
        attestation_verifier: RuntimeAttestationVerifier,
        spec_builder: OciSandboxSpecBuilder,
        capabilities: Iterable[Capability | str] = (),
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.backend_id = _bounded_text(backend_id, "backend_id")
        if not isinstance(adapter, OciRuntimeAdapter):
            raise RuntimeAttestationInvalid(
                "adapter does not implement OciRuntimeAdapter"
            )
        if not isinstance(attestation_verifier, RuntimeAttestationVerifier):
            raise RuntimeAttestationInvalid(
                "verifier does not implement RuntimeAttestationVerifier"
            )
        if not isinstance(spec_builder, OciSandboxSpecBuilder):
            raise OciSpecDenied("invalid_oci_spec_builder")
        self._adapter = adapter
        self._attestation_verifier = attestation_verifier
        self._spec_builder = spec_builder
        self._clock = clock
        normalized: dict[str, Capability] = {}
        for value in capabilities:
            capability = (
                value if isinstance(value, Capability) else Capability(value)
            )
            normalized[capability.name] = capability
        if len(normalized) > 64:
            raise OciSpecDenied("backend_capabilities_exceed_policy")
        self.capabilities = tuple(normalized[key] for key in sorted(normalized))
        # Refuse to expose CONTAINER security_level unless the adapter already
        # passes the deployment trust anchor at construction time.
        self._verify_attestation()

    def execute(
        self,
        request: ExecutionRequest,
        profile: SandboxProfile,
    ) -> BackendExecutionResult:
        attestation = self._verify_attestation()
        spec = self._spec_builder.build(
            request,
            profile,
            runtime_attestation_digest=attestation.attestation_digest,
        )
        result = self._adapter.execute(spec)
        if not isinstance(result, OciRuntimeResult):
            raise OciReceiptBindingError("runtime returned an invalid result")
        if (
            result.spec_digest != spec.spec_digest
            or result.action_digest != spec.action_digest
            or result.request_digest != spec.request_digest
            or result.runtime_attestation_digest
            != spec.runtime_attestation_digest
        ):
            raise OciReceiptBindingError("runtime result binding mismatch")
        return BackendExecutionResult(
            exit_code=result.exit_code,
            timed_out=result.timed_out,
            cancelled=result.cancelled,
            cancellation_uncertain=result.cancellation_uncertain,
            output_artifact_refs=result.output_artifact_refs,
        )

    def _verify_attestation(self) -> RuntimeAttestation:
        now = _timestamp(self._clock())
        try:
            attestation = self._attestation_verifier.verify(
                self._adapter,
                now=now,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise RuntimeAttestationInvalid("runtime attestation failed") from None
        if not isinstance(attestation, RuntimeAttestation):
            raise RuntimeAttestationInvalid("runtime attestation failed")
        attestation.validate_current(
            adapter_id=_bounded_text(self._adapter.adapter_id, "adapter_id"),
            runtime_class=self._spec_builder.runtime_class,
            now=now,
        )
        return attestation


__all__ = [
    "EphemeralMount",
    "MaterializedFile",
    "OciBackendError",
    "OciGvisorSandboxBackend",
    "OciMountKind",
    "OciReceiptBindingError",
    "OciRuntimeAdapter",
    "OciRuntimeResult",
    "OciSandboxSpec",
    "OciSandboxSpecBuilder",
    "OciSpecDenied",
    "RuntimeAttestation",
    "RuntimeAttestationInvalid",
    "RuntimeAttestationVerifier",
    "RuntimeIsolationKind",
]
