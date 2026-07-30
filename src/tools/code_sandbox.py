"""Verified execution plans for untrusted ``code_run`` payloads.

Environment filtering, a working directory and process-group termination are
useful hardening, but they are not an operating-system sandbox.  This module
keeps that distinction explicit:

* safe plans are created only after a functional backend probe succeeds;
* an unavailable backend fails closed and never falls back to ``Popen``;
* the legacy host process backend is available only as explicit
  ``development_unsafe`` compatibility;
* the complete public plan is immutable and can be included in a policy
  action digest before the user approves execution.
"""

from __future__ import annotations

import functools
import hashlib
import json
import os
import platform
import socket
import stat
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


SAFE_BACKENDS = frozenset({"bubblewrap", "sandbox-exec"})
SUPPORTED_BACKENDS = SAFE_BACKENDS | {"auto", "deny", "unsafe"}
PROTECTED_WORKSPACE_DIRS = ("system", "runtime", "memory")
PROTECTED_WORKSPACE_FILES = ("_intervene", "_keyinfo", "plan.md")
FIXED_SANDBOX_PATH = "/usr/bin:/bin"
PROBE_TIMEOUT_SECONDS = 8
MAX_CODE_SCRIPT_BYTES = 1024 * 1024
_SYSTEM_BACKEND_PATHS = {
    "bubblewrap": (Path("/usr/bin/bwrap"), Path("/bin/bwrap")),
    "sandbox-exec": (Path("/usr/bin/sandbox-exec"),),
}


class CodeSandboxError(RuntimeError):
    """A stable, non-sensitive code sandbox planning error."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True, slots=True)
class ExecutionLimits:
    cpu_seconds: int = 30
    memory_bytes: int = 512 * 1024 * 1024
    file_bytes: int = 64 * 1024 * 1024
    open_files: int = 64
    processes: int = 32
    scratch_bytes: int = 128 * 1024 * 1024
    scratch_entries: int = 4096

    def public_dict(self) -> dict[str, int]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SandboxCapability:
    available: bool
    backend: str
    security_level: str
    reason_code: str
    binary_identity_digest: str
    probe_digest: str
    filesystem_mode: str
    network_mode: str
    process_mode: str

    def public_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CodeExecutionPlan:
    script_sha256: str
    language: str
    timeout: int
    backend: str
    security_level: str
    filesystem_mode: str
    network_mode: str
    process_mode: str
    limits: ExecutionLimits
    workspace_digest: str
    workspace_identity: tuple[tuple[str, int, int], ...]
    binary_identity_digest: str
    probe_digest: str
    binding_digest: str
    workspace_path: str
    payload_command: tuple[str, ...]
    launch_command: tuple[str, ...]
    environment: tuple[tuple[str, str], ...]
    scratch_handle: Any = field(default=None, repr=False, compare=False)

    @property
    def unsafe(self) -> bool:
        return self.security_level == "development_unsafe"

    def approval_binding(self) -> dict[str, Any]:
        return {
            "script_sha256": self.script_sha256,
            "language": self.language,
            "timeout": self.timeout,
            "backend": self.backend,
            "security_level": self.security_level,
            "filesystem_mode": self.filesystem_mode,
            "network_mode": self.network_mode,
            "process_mode": self.process_mode,
            "limits": self.limits.public_dict(),
            "workspace_digest": self.workspace_digest,
            "binary_identity_digest": self.binary_identity_digest,
            "probe_digest": self.probe_digest,
            "binding_digest": self.binding_digest,
        }

    def security_receipt(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "security_level": self.security_level,
            "filesystem_mode": self.filesystem_mode,
            "network_mode": self.network_mode,
            "process_mode": self.process_mode,
            "limits": self.limits.public_dict(),
            "probe_digest": self.probe_digest,
            "plan_digest": self.binding_digest,
            "unsafe": self.unsafe,
        }


_LIMIT_SPECS = {
    "cpu_seconds": ("XAGENT_CODE_RUN_CPU_SECONDS", 30, 1, 300),
    "memory_bytes": (
        "XAGENT_CODE_RUN_MEMORY_BYTES",
        512 * 1024 * 1024,
        64 * 1024 * 1024,
        4 * 1024 * 1024 * 1024,
    ),
    "file_bytes": (
        "XAGENT_CODE_RUN_FILE_BYTES",
        64 * 1024 * 1024,
        1024 * 1024,
        1024 * 1024 * 1024,
    ),
    "open_files": ("XAGENT_CODE_RUN_OPEN_FILES", 64, 16, 1024),
    "processes": ("XAGENT_CODE_RUN_PROCESSES", 32, 1, 256),
    "scratch_bytes": (
        "XAGENT_CODE_RUN_SCRATCH_BYTES",
        128 * 1024 * 1024,
        16 * 1024 * 1024,
        1024 * 1024 * 1024,
    ),
    "scratch_entries": (
        "XAGENT_CODE_RUN_SCRATCH_ENTRIES",
        4096,
        64,
        65536,
    ),
}


def load_execution_limits() -> ExecutionLimits:
    values: dict[str, int] = {}
    for field_name, (environment_name, default, minimum, maximum) in _LIMIT_SPECS.items():
        raw = os.environ.get(environment_name)
        try:
            value = default if raw is None or not raw.strip() else int(raw)
        except (TypeError, ValueError) as exc:
            raise CodeSandboxError(
                "SANDBOX_CONFIG_INVALID",
                f"{environment_name} must be an integer",
            ) from exc
        if value < minimum or value > maximum:
            raise CodeSandboxError(
                "SANDBOX_CONFIG_INVALID",
                f"{environment_name} is outside the supported range",
            )
        values[field_name] = value
    return ExecutionLimits(**values)


def configured_backend(value: str | None = None) -> str:
    raw = (
        value
        if value is not None
        else os.environ.get(
            "XAGENT_CODE_RUN_BACKEND",
            os.environ.get("XAGENT_CODE_RUN_ISOLATION", "auto"),
        )
    )
    backend = str(raw or "auto").strip().lower()
    aliases = {
        "required": "auto",
        "safe": "auto",
        "bubblewrap": "bubblewrap",
        "bwrap": "bubblewrap",
        "sandbox_exec": "sandbox-exec",
        "sandbox-exec": "sandbox-exec",
        "development_unsafe": "unsafe",
    }
    backend = aliases.get(backend, backend)
    if backend not in SUPPORTED_BACKENDS:
        raise CodeSandboxError(
            "SANDBOX_CONFIG_INVALID",
            "XAGENT_CODE_RUN_BACKEND has an unsupported value",
        )
    return backend


def prepare_code_execution(
    *,
    script: str,
    language: str,
    timeout: int,
    cwd: str | None,
    backend: str | None = None,
    unsafe_authorized: bool = False,
) -> CodeExecutionPlan:
    if not isinstance(script, str) or not script.strip():
        raise CodeSandboxError("SCRIPT_EMPTY", "script is empty")
    try:
        script_bytes = script.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise CodeSandboxError(
            "SCRIPT_INVALID",
            "script is not valid Unicode text",
        ) from exc
    if len(script_bytes) > MAX_CODE_SCRIPT_BYTES:
        raise CodeSandboxError(
            "SCRIPT_TOO_LARGE",
            "script exceeds the code execution size limit",
        )
    if language not in {"python", "shell", "bash", "sh"}:
        raise CodeSandboxError("LANGUAGE_UNSUPPORTED", f"unsupported language: {language}")
    if timeout <= 0 or timeout > 3600:
        raise CodeSandboxError("TIMEOUT_INVALID", "timeout must be between 1 and 3600 seconds")

    try:
        workspace = Path(cwd or Path.cwd()).expanduser().resolve()
        if not workspace.exists():
            raise CodeSandboxError("WORKSPACE_INVALID", "cwd does not exist")
        if not workspace.is_dir():
            raise CodeSandboxError("WORKSPACE_INVALID", "cwd is not a directory")
    except CodeSandboxError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise CodeSandboxError(
            "WORKSPACE_INVALID",
            "cwd cannot be resolved",
        ) from exc
    workspace_identity = _workspace_identity(workspace)
    limits = load_execution_limits()
    requested = configured_backend(backend)
    if requested == "deny":
        raise CodeSandboxError("CODE_RUN_DENIED", "code execution is disabled")

    script_sha256 = hashlib.sha256(script_bytes).hexdigest()
    workspace_digest = hashlib.sha256(str(workspace).encode("utf-8")).hexdigest()
    if requested == "unsafe":
        if not unsafe_authorized:
            raise CodeSandboxError(
                "UNSAFE_EXECUTION_NOT_AUTHORIZED",
                "unsafe code execution requires explicit trusted configuration",
            )
        capability = SandboxCapability(
            available=True,
            backend="local-process",
            security_level="development_unsafe",
            reason_code="UNSAFE_EXPLICITLY_ENABLED",
            binary_identity_digest=_binary_identity_digest(Path(sys.executable)),
            probe_digest="",
            filesystem_mode="host_read_write",
            network_mode="host",
            process_mode="host_process_group_resource_limits_best_effort",
        )
        interpreter = sys.executable
    else:
        capability = probe_code_sandbox(requested)
        if not capability.available:
            raise CodeSandboxError(
                capability.reason_code or "SANDBOX_UNAVAILABLE",
                "verified code sandbox is unavailable",
            )
        interpreter = _safe_python_executable()

    payload = _payload_command(script, language, interpreter)
    launcher = _resource_launcher_command(
        payload,
        limits,
        strict=not capability.security_level == "development_unsafe",
    )
    scratch_handle: tempfile.TemporaryDirectory[str] | None = None
    if capability.backend == "bubblewrap":
        binary = _backend_binary("bubblewrap")
        scratch_handle = tempfile.TemporaryDirectory(
            prefix="xagent-code-sandbox-",
        )
        scratch = Path(scratch_handle.name)
        launch = _bubblewrap_command(
            binary,
            workspace,
            launcher,
            limits,
            scratch,
        )
        environment = _minimal_environment(
            "/tmp",
            "/tmp",
            workspace="/workspace",
        )
    elif capability.backend == "sandbox-exec":
        binary = _backend_binary("sandbox-exec")
        scratch_handle = tempfile.TemporaryDirectory(
            prefix="xagent-code-sandbox-",
        )
        scratch = Path(scratch_handle.name)
        profile = _sandbox_exec_profile(workspace, scratch)
        launch = (str(binary), "-p", profile, "--", *launcher)
        environment = _minimal_environment(
            str(scratch),
            str(scratch),
            workspace=str(workspace),
        )
    else:
        launch = launcher
        environment = _minimal_environment(
            str(workspace),
            str(workspace),
            workspace=str(workspace),
        )

    public = {
        "script_sha256": script_sha256,
        "language": language,
        "timeout": timeout,
        "backend": capability.backend,
        "security_level": capability.security_level,
        "filesystem_mode": capability.filesystem_mode,
        "network_mode": capability.network_mode,
        "process_mode": capability.process_mode,
        "limits": limits.public_dict(),
        "workspace_digest": workspace_digest,
        "binary_identity_digest": capability.binary_identity_digest,
        "probe_digest": capability.probe_digest,
    }
    binding_digest = _canonical_digest(public)
    return CodeExecutionPlan(
        script_sha256=script_sha256,
        language=language,
        timeout=timeout,
        backend=capability.backend,
        security_level=capability.security_level,
        filesystem_mode=capability.filesystem_mode,
        network_mode=capability.network_mode,
        process_mode=capability.process_mode,
        limits=limits,
        workspace_digest=workspace_digest,
        workspace_identity=workspace_identity,
        binary_identity_digest=capability.binary_identity_digest,
        probe_digest=capability.probe_digest,
        binding_digest=binding_digest,
        workspace_path=str(workspace),
        payload_command=payload,
        launch_command=tuple(launch),
        environment=tuple(sorted(environment.items())),
        scratch_handle=scratch_handle,
    )


def validate_execution_plan(
    plan: CodeExecutionPlan,
    *,
    script: str,
    language: str,
    timeout: int,
    cwd: str | None,
) -> None:
    if not isinstance(plan, CodeExecutionPlan) or not isinstance(
        plan.limits,
        ExecutionLimits,
    ):
        raise CodeSandboxError(
            "EXECUTION_PLAN_MISMATCH",
            "execution plan is invalid",
        )
    try:
        script_sha256 = hashlib.sha256(script.encode("utf-8")).hexdigest()
        workspace = Path(cwd or Path.cwd()).expanduser().resolve()
    except (OSError, TypeError, UnicodeEncodeError, ValueError) as exc:
        raise CodeSandboxError(
            "EXECUTION_PLAN_MISMATCH",
            "execution inputs cannot be verified",
        ) from exc
    if script_sha256 != plan.script_sha256:
        raise CodeSandboxError("EXECUTION_PLAN_MISMATCH", "script changed after authorization")
    if language != plan.language or timeout != plan.timeout:
        raise CodeSandboxError("EXECUTION_PLAN_MISMATCH", "execution parameters changed after authorization")
    if hashlib.sha256(str(workspace).encode("utf-8")).hexdigest() != plan.workspace_digest:
        raise CodeSandboxError("EXECUTION_PLAN_MISMATCH", "workspace changed after authorization")
    if _workspace_identity(workspace) != plan.workspace_identity:
        raise CodeSandboxError("EXECUTION_PLAN_MISMATCH", "workspace boundary changed after authorization")
    if plan.limits != load_execution_limits():
        raise CodeSandboxError(
            "EXECUTION_PLAN_MISMATCH",
            "execution limits changed after authorization",
        )

    public = {
        "script_sha256": plan.script_sha256,
        "language": plan.language,
        "timeout": plan.timeout,
        "backend": plan.backend,
        "security_level": plan.security_level,
        "filesystem_mode": plan.filesystem_mode,
        "network_mode": plan.network_mode,
        "process_mode": plan.process_mode,
        "limits": plan.limits.public_dict(),
        "workspace_digest": plan.workspace_digest,
        "binary_identity_digest": plan.binary_identity_digest,
        "probe_digest": plan.probe_digest,
    }
    if _canonical_digest(public) != plan.binding_digest:
        raise CodeSandboxError(
            "EXECUTION_PLAN_MISMATCH",
            "execution plan binding is invalid",
        )

    if plan.backend in SAFE_BACKENDS:
        capability = probe_code_sandbox(plan.backend)
        if not capability.available:
            raise CodeSandboxError(
                "EXECUTION_PLAN_MISMATCH",
                "sandbox capability is no longer available",
            )
        binary = _backend_binary(plan.backend)
        if _binary_identity_digest(binary) != plan.binary_identity_digest:
            raise CodeSandboxError("EXECUTION_PLAN_MISMATCH", "sandbox backend changed after authorization")
        expected_capability = capability
        interpreter = _safe_python_executable()
    elif plan.backend == "local-process" and plan.unsafe:
        expected_capability = SandboxCapability(
            available=True,
            backend="local-process",
            security_level="development_unsafe",
            reason_code="UNSAFE_EXPLICITLY_ENABLED",
            binary_identity_digest=_binary_identity_digest(
                Path(sys.executable)
            ),
            probe_digest="",
            filesystem_mode="host_read_write",
            network_mode="host",
            process_mode="host_process_group_resource_limits_best_effort",
        )
        interpreter = sys.executable
        binary = None
    else:
        raise CodeSandboxError(
            "EXECUTION_PLAN_MISMATCH",
            "execution backend is not authorized",
        )

    for field_name in (
        "backend",
        "security_level",
        "binary_identity_digest",
        "probe_digest",
        "filesystem_mode",
        "network_mode",
        "process_mode",
    ):
        if getattr(plan, field_name) != getattr(
            expected_capability,
            field_name,
        ):
            raise CodeSandboxError(
                "EXECUTION_PLAN_MISMATCH",
                "sandbox capability changed after authorization",
            )

    expected_payload = _payload_command(script, language, interpreter)
    expected_launcher = _resource_launcher_command(
        expected_payload,
        plan.limits,
        strict=not plan.unsafe,
    )
    if plan.backend == "bubblewrap":
        scratch = getattr(plan.scratch_handle, "name", "")
        if not scratch or not Path(scratch).is_dir():
            raise CodeSandboxError(
                "EXECUTION_PLAN_MISMATCH",
                "sandbox scratch directory is unavailable",
            )
        expected_launch = _bubblewrap_command(
            binary,
            workspace,
            expected_launcher,
            plan.limits,
            Path(scratch),
        )
        expected_environment = _minimal_environment(
            "/tmp",
            "/tmp",
            workspace="/workspace",
        )
    elif plan.backend == "sandbox-exec":
        scratch = getattr(plan.scratch_handle, "name", "")
        if not scratch or not Path(scratch).is_dir():
            raise CodeSandboxError(
                "EXECUTION_PLAN_MISMATCH",
                "sandbox scratch directory is unavailable",
            )
        expected_launch = (
            str(binary),
            "-p",
            _sandbox_exec_profile(workspace, Path(scratch)),
            "--",
            *expected_launcher,
        )
        expected_environment = _minimal_environment(
            scratch,
            scratch,
            workspace=str(workspace),
        )
    else:
        expected_launch = expected_launcher
        expected_environment = _minimal_environment(
            str(workspace),
            str(workspace),
            workspace=str(workspace),
        )
        if plan.scratch_handle is not None:
            raise CodeSandboxError(
                "EXECUTION_PLAN_MISMATCH",
                "execution scratch binding is invalid",
            )

    if (
        plan.workspace_path != str(workspace)
        or plan.payload_command != expected_payload
        or plan.launch_command != tuple(expected_launch)
        or plan.environment != tuple(sorted(expected_environment.items()))
    ):
        raise CodeSandboxError(
            "EXECUTION_PLAN_MISMATCH",
            "execution plan implementation changed after authorization",
        )


def cleanup_execution_plan(plan: CodeExecutionPlan | None) -> None:
    if not isinstance(plan, CodeExecutionPlan):
        return
    handle = plan.scratch_handle
    if handle is not None:
        try:
            handle.cleanup()
        except OSError:
            pass


def probe_code_sandbox(requested: str = "auto") -> SandboxCapability:
    backend = configured_backend(requested)
    if backend in {"deny", "unsafe"}:
        return _unavailable_capability(
            backend,
            "SANDBOX_UNAVAILABLE",
        )
    candidates: tuple[str, ...]
    if backend == "auto":
        system = platform.system()
        if system == "Linux":
            candidates = ("bubblewrap",)
        elif system == "Darwin":
            candidates = ("sandbox-exec",)
        else:
            candidates = ()
    else:
        candidates = (backend,)
    for candidate in candidates:
        try:
            binary = _backend_binary(candidate)
        except CodeSandboxError:
            continue
        identity = _binary_identity_digest(binary)
        capability = _probe_cached(candidate, str(binary), identity)
        if capability.available:
            return capability
    return _unavailable_capability(
        backend if backend != "auto" else "none",
        "SANDBOX_UNAVAILABLE",
    )


@functools.lru_cache(maxsize=8)
def _probe_cached(
    backend: str,
    binary_path: str,
    binary_identity_digest: str,
) -> SandboxCapability:
    binary = Path(binary_path)
    try:
        if backend == "bubblewrap":
            ok = _probe_bubblewrap(binary)
            process_mode = "private_pid_namespace"
        elif backend == "sandbox-exec":
            ok = _probe_sandbox_exec(binary)
            process_mode = "fork_denied_seatbelt"
        else:
            ok = False
            process_mode = "unknown"
    except (CodeSandboxError, OSError, subprocess.SubprocessError):
        ok = False
        process_mode = "unknown"
    if not ok:
        return _unavailable_capability(
            backend,
            "SANDBOX_PROBE_FAILED",
            binary_identity_digest=binary_identity_digest,
        )
    probe_payload = {
        "backend": backend,
        "binary_identity_digest": binary_identity_digest,
        "profile_version": 1,
        "filesystem_mode": "workspace_read_only_control_hidden",
        "network_mode": "deny",
        "process_mode": process_mode,
    }
    return SandboxCapability(
        available=True,
        backend=backend,
        security_level="os_sandbox",
        reason_code="SANDBOX_VERIFIED",
        binary_identity_digest=binary_identity_digest,
        probe_digest=_canonical_digest(probe_payload),
        filesystem_mode="workspace_read_only_control_hidden",
        network_mode="deny",
        process_mode=process_mode,
    )


def _probe_bubblewrap(binary: Path) -> bool:
    python = _safe_python_executable()
    if not Path(python).is_file():
        return False
    with tempfile.TemporaryDirectory(prefix="xagent-bwrap-probe-") as temp_root:
        root = Path(temp_root)
        workspace = root / "workspace"
        workspace.mkdir(mode=0o755)
        scratch = root / "scratch"
        scratch.mkdir(mode=0o700)
        (workspace / "marker.txt").write_text("workspace-marker", encoding="utf-8")
        control = workspace / "runtime" / "agent_kernel"
        control.mkdir(parents=True)
        control_sentinel = control / "sentinel.txt"
        control_sentinel.write_text("control-secret", encoding="utf-8")
        root_control = workspace / "plan.md"
        root_control.write_text("root-control-secret", encoding="utf-8")
        outside = root / "outside-secret.txt"
        outside.write_text("outside-secret", encoding="utf-8")
        port, listener = _loopback_listener()
        script = _probe_script(str(outside), port)
        payload = _payload_command(script, "python", python)
        launcher = _resource_launcher_command(
            payload,
            ExecutionLimits(cpu_seconds=3),
            strict=True,
        )
        command = _bubblewrap_command(
            binary,
            workspace,
            launcher,
            ExecutionLimits(cpu_seconds=3),
            scratch,
        )
        try:
            completed = subprocess.run(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=_minimal_environment(
                    "/tmp",
                    "/tmp",
                    workspace="/workspace",
                ),
                close_fds=True,
                timeout=PROBE_TIMEOUT_SECONDS,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        finally:
            listener.close()
        return (
            completed.returncode == 0
            and completed.stdout.strip() == "PROBE_OK"
            and control_sentinel.read_text(encoding="utf-8") == "control-secret"
            and root_control.read_text(encoding="utf-8") == "root-control-secret"
            and outside.read_text(encoding="utf-8") == "outside-secret"
            and not (workspace / "host-write.txt").exists()
        )


def _probe_sandbox_exec(binary: Path) -> bool:
    python = _safe_python_executable()
    if not Path(python).is_file():
        return False
    with tempfile.TemporaryDirectory(prefix="xagent-seatbelt-probe-") as temp_root:
        root = Path(temp_root)
        workspace = root / "workspace"
        workspace.mkdir()
        (workspace / "marker.txt").write_text("workspace-marker", encoding="utf-8")
        control = workspace / "runtime" / "agent_kernel"
        control.mkdir(parents=True)
        control_sentinel = control / "sentinel.txt"
        control_sentinel.write_text("control-secret", encoding="utf-8")
        root_control = workspace / "plan.md"
        root_control.write_text("root-control-secret", encoding="utf-8")
        outside = root / "outside-secret.txt"
        outside.write_text("outside-secret", encoding="utf-8")
        scratch = root / "scratch"
        scratch.mkdir()
        port, listener = _loopback_listener()
        script = _probe_script(str(outside), port, scratch=str(scratch))
        payload = _payload_command(script, "python", python)
        launcher = _resource_launcher_command(
            payload,
            ExecutionLimits(cpu_seconds=3),
            strict=True,
        )
        profile = _sandbox_exec_profile(workspace, scratch)
        command = (str(binary), "-p", profile, "--", *launcher)
        try:
            completed = subprocess.run(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=_minimal_environment(
                    str(scratch),
                    str(scratch),
                    workspace=str(workspace),
                ),
                cwd=workspace,
                close_fds=True,
                timeout=PROBE_TIMEOUT_SECONDS,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        finally:
            listener.close()
        return (
            completed.returncode == 0
            and completed.stdout.strip() == "PROBE_OK"
            and control_sentinel.read_text(encoding="utf-8") == "control-secret"
            and root_control.read_text(encoding="utf-8") == "root-control-secret"
            and outside.read_text(encoding="utf-8") == "outside-secret"
            and not (workspace / "host-write.txt").exists()
        )


def _probe_script(outside_path: str, port: int, *, scratch: str = "/tmp") -> str:
    return (
        "import os,socket,sys\n"
        "workspace=os.environ['XAGENT_CODE_RUN_WORKSPACE']\n"
        "assert open(os.path.join(workspace,'marker.txt')).read()=='workspace-marker'\n"
        "for path in ("
        "os.path.join(workspace,'runtime','agent_kernel','sentinel.txt'),"
        "os.path.join(workspace,'RUNTIME','AGENT_KERNEL','sentinel.txt'),"
        "os.path.join(workspace,'plan.md'),"
        "os.path.join(workspace,'PLAN.MD'),"
        f"{outside_path!r}):\n"
        "  try:\n"
        "    if open(path).read() in ('control-secret','root-control-secret'): "
        "raise SystemExit(20)\n"
        "  except (FileNotFoundError,PermissionError,OSError): pass\n"
        "try:\n"
        "  open(os.path.join(workspace,'host-write.txt'),'w').write('bad'); raise SystemExit(21)\n"
        "except (PermissionError,OSError): pass\n"
        "try:\n"
        "  open('/sandbox-root-write.txt','w').write('bad'); raise SystemExit(23)\n"
        "except (PermissionError,OSError): pass\n"
        f"open(os.path.join({scratch!r},'scratch-ok'),'w').write('ok')\n"
        "sock=socket.socket(); sock.settimeout(.3)\n"
        "try:\n"
        f"  sock.connect(('127.0.0.1',{port})); raise SystemExit(22)\n"
        "except OSError: pass\n"
        "finally: sock.close()\n"
        "print('PROBE_OK')\n"
    )


def _loopback_listener() -> tuple[int, socket.socket]:
    listener = socket.socket()
    try:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
    except OSError:
        listener.close()
        raise
    return int(listener.getsockname()[1]), listener


def _safe_python_executable() -> str:
    candidates = (Path("/usr/bin/python3"), Path("/bin/python3"))
    for candidate in candidates:
        if _is_trusted_system_file(candidate):
            return str(candidate.resolve())
    raise CodeSandboxError(
        "SANDBOX_RUNTIME_UNAVAILABLE",
        "a system Python runtime is required by the verified sandbox",
    )


def _payload_command(script: str, language: str, python: str) -> tuple[str, ...]:
    if language == "python":
        return (python, "-c", script)
    return ("/bin/bash", "--noprofile", "--norc", "-c", script)


def _resource_launcher_command(
    payload: tuple[str, ...],
    limits: ExecutionLimits,
    *,
    strict: bool,
) -> tuple[str, ...]:
    launcher = (
        "import os,resource,sys\n"
        "strict,cpu,mem,fsize,nofile,nproc=map(int,sys.argv[1:7])\n"
        "def limit(kind,value):\n"
        "  try: resource.setrlimit(kind,(value,value))\n"
        "  except (OSError,ValueError):\n"
        "    if strict: raise\n"
        "limit(resource.RLIMIT_CORE,0)\n"
        "limit(resource.RLIMIT_CPU,cpu)\n"
        "limit(resource.RLIMIT_AS,mem)\n"
        "limit(resource.RLIMIT_FSIZE,fsize)\n"
        "limit(resource.RLIMIT_NOFILE,nofile)\n"
        "if strict and hasattr(resource,'RLIMIT_NPROC'): limit(resource.RLIMIT_NPROC,nproc)\n"
        "os.umask(0o077)\n"
        "os.execv(sys.argv[7],sys.argv[7:])\n"
    )
    # A Python payload already carries the exact interpreter selected by the
    # execution plan (including a virtualenv in explicit unsafe mode). Shell
    # payloads still use a trusted system interpreter for the launcher itself.
    python = payload[0] if payload[0] != "/bin/bash" else _safe_python_executable()
    return (
        python,
        "-c",
        launcher,
        "1" if strict else "0",
        str(limits.cpu_seconds),
        str(limits.memory_bytes),
        str(limits.file_bytes),
        str(limits.open_files),
        str(limits.processes),
        *payload,
    )


def _bubblewrap_command(
    binary: Path,
    workspace: Path,
    command: tuple[str, ...],
    limits: ExecutionLimits,
    scratch: Path,
) -> tuple[str, ...]:
    protected_dirs, protected_files = _protected_workspace_entries(workspace)
    args: list[str] = [
        str(binary),
        "--die-with-parent",
        "--new-session",
        "--unshare-all",
        "--disable-userns",
        "--hostname",
        "xagent-sandbox",
        "--cap-drop",
        "ALL",
        "--clearenv",
    ]
    for root in ("/usr", "/bin", "/lib", "/lib64"):
        if Path(root).exists():
            args.extend(("--ro-bind", root, root))
    mask_usr_local = Path("/usr/local").is_dir()
    if mask_usr_local:
        args.extend(
            (
                "--size",
                str(1024 * 1024),
                "--tmpfs",
                "/usr/local",
            )
        )
    args.extend(
        (
            "--proc",
            "/proc",
            "--dev",
            "/dev",
            "--perms",
            "0700",
            "--bind",
            str(scratch),
            "/tmp",
            "--ro-bind",
            str(workspace),
            "/workspace",
        )
    )
    for name in sorted(set(PROTECTED_WORKSPACE_DIRS).union(protected_dirs)):
        # bubblewrap creates missing mount points. Masking the fixed control
        # paths unconditionally closes the validate-to-launch race too.
        args.extend(
            (
                "--perms",
                "0700",
                "--size",
                str(min(limits.scratch_bytes, 1024 * 1024)),
                "--tmpfs",
                f"/workspace/{name}",
            )
        )
    for name in sorted(set(PROTECTED_WORKSPACE_FILES).union(protected_files)):
        args.extend(
            (
                "--ro-bind",
                "/dev/null",
                f"/workspace/{name}",
            )
        )
    args.extend(
        (
            "--remount-ro",
            "/",
            "--remount-ro",
            "/dev",
            *(
                ("--remount-ro", "/usr/local")
                if mask_usr_local
                else ()
            ),
            "--setenv",
            "HOME",
            "/tmp",
            "--setenv",
            "TMPDIR",
            "/tmp",
            "--setenv",
            "PATH",
            FIXED_SANDBOX_PATH,
            "--setenv",
            "PYTHONIOENCODING",
            "utf-8",
            "--setenv",
            "XAGENT_CODE_RUN_WORKSPACE",
            "/workspace",
            "--chdir",
            "/workspace",
            "--",
            *command,
        )
    )
    return tuple(args)


def _sandbox_exec_profile(workspace: Path, scratch: Path) -> str:
    workspace_literal = json.dumps(str(workspace))
    scratch_literal = json.dumps(str(scratch))
    protected_dirs, protected_root_files = _protected_workspace_entries(
        workspace
    )
    protected = "\n".join(
        f"(deny file-read* file-write* (subpath {json.dumps(str(workspace / name))}))"
        for name in sorted(set(PROTECTED_WORKSPACE_DIRS).union(protected_dirs))
    )
    protected_files = "\n".join(
        f"(deny file-read* file-write* (literal {json.dumps(str(workspace / name))}))"
        for name in sorted(
            set(PROTECTED_WORKSPACE_FILES).union(protected_root_files)
        )
    )
    return (
        "(version 1)\n"
        "(deny default)\n"
        "(allow process-exec)\n"
        "(allow sysctl-read)\n"
        "(allow file-read* (subpath \"/System\"))\n"
        "(allow file-read* (subpath \"/usr\"))\n"
        "(allow file-read* (subpath \"/bin\"))\n"
        "(allow file-read* (subpath \"/sbin\"))\n"
        "(allow file-read* (subpath \"/Library/Apple\"))\n"
        "(allow file-read* (subpath \"/Library/Developer/CommandLineTools\"))\n"
        "(allow file-read* (subpath \"/private/var/db/dyld\"))\n"
        "(allow file-read* (subpath \"/dev\"))\n"
        f"(allow file-read* (subpath {workspace_literal}))\n"
        f"(allow file-read* file-write* (subpath {scratch_literal}))\n"
        f"{protected}\n"
        f"{protected_files}\n"
        "(deny network*)\n"
    )


def _minimal_environment(
    home: str,
    tmpdir: str,
    *,
    workspace: str,
) -> dict[str, str]:
    env = {
        "HOME": home,
        "TMPDIR": tmpdir,
        "PATH": FIXED_SANDBOX_PATH,
        "PYTHONIOENCODING": "utf-8",
        "XAGENT_CODE_RUN_WORKSPACE": workspace,
        "LANG": "C",
        "LC_ALL": "C",
        "TZ": "UTC",
    }
    return env


def _workspace_identity(workspace: Path) -> tuple[tuple[str, int, int], ...]:
    identities: list[tuple[str, int, int]] = []
    try:
        stat = workspace.stat()
    except OSError as exc:
        raise CodeSandboxError("WORKSPACE_INVALID", "workspace cannot be inspected") from exc
    identities.append((".", int(stat.st_dev), int(stat.st_ino)))
    protected_dirs, protected_files = _protected_workspace_entries(workspace)
    for name in protected_dirs:
        path = workspace / name
        stat = path.stat()
        identities.append((name, int(stat.st_dev), int(stat.st_ino)))
    for name in protected_files:
        stat = (workspace / name).stat()
        identities.append((name, int(stat.st_dev), int(stat.st_ino)))
    return tuple(identities)


def _protected_workspace_entries(
    workspace: Path,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    protected_dir_names = {name.casefold() for name in PROTECTED_WORKSPACE_DIRS}
    protected_file_names = {
        name.casefold() for name in PROTECTED_WORKSPACE_FILES
    }
    directories: list[str] = []
    files: list[str] = []
    try:
        entries = tuple(workspace.iterdir())
    except OSError as exc:
        raise CodeSandboxError(
            "WORKSPACE_INVALID",
            "workspace cannot be inspected",
        ) from exc
    for entry in entries:
        folded = entry.name.casefold()
        if folded in protected_dir_names:
            if entry.is_symlink() or not entry.is_dir():
                raise CodeSandboxError(
                    "WORKSPACE_BOUNDARY_INVALID",
                    "protected workspace paths must be real directories",
                )
            directories.append(entry.name)
        elif folded in protected_file_names:
            if entry.is_symlink() or not entry.is_file():
                raise CodeSandboxError(
                    "WORKSPACE_BOUNDARY_INVALID",
                    "protected workspace control paths must be regular files",
                )
            files.append(entry.name)
    return tuple(sorted(directories)), tuple(sorted(files))


def _backend_binary(backend: str) -> Path:
    for candidate in _SYSTEM_BACKEND_PATHS.get(backend, ()):
        if _is_trusted_system_file(candidate):
            return candidate.resolve()
    raise CodeSandboxError("SANDBOX_UNAVAILABLE", "sandbox backend is unavailable")


def _is_trusted_system_file(path: Path) -> bool:
    try:
        current = path.resolve(strict=True)
        if not current.is_file():
            return False
        while True:
            metadata = current.stat()
            if metadata.st_uid != 0 or metadata.st_mode & (
                stat.S_IWGRP | stat.S_IWOTH
            ):
                return False
            if current.parent == current:
                return True
            current = current.parent
    except OSError:
        return False


def _binary_identity_digest(path: Path) -> str:
    try:
        stat = path.stat()
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise CodeSandboxError("SANDBOX_UNAVAILABLE", "sandbox binary cannot be verified") from exc
    return _canonical_digest(
        {
            "path": str(path),
            "device": int(stat.st_dev),
            "inode": int(stat.st_ino),
            "size": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
            "sha256": digest.hexdigest(),
        }
    )


def _unavailable_capability(
    backend: str,
    reason_code: str,
    *,
    binary_identity_digest: str = "",
) -> SandboxCapability:
    return SandboxCapability(
        available=False,
        backend=backend,
        security_level="unavailable",
        reason_code=reason_code,
        binary_identity_digest=binary_identity_digest,
        probe_digest="",
        filesystem_mode="none",
        network_mode="none",
        process_mode="none",
    )


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
