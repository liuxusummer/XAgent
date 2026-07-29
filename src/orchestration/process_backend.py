"""Local process-tree supervision without an isolation attestation.

This backend exists to enforce the F14 lifecycle contract: no shell, a fresh
process group, bounded captured output, a minimal environment, and TERM/KILL
reaping on timeout.  It deliberately advertises ``DEVELOPMENT_UNSAFE`` and is
therefore eligible only for actions declared read-only by the dispatcher.  It
does not make untrusted code safe and is not an OS sandbox.

Cooperative cancellation is available only while the supervising worker is
alive: its selector loop polls a trusted callback backed by durable Run state.
After a controller/worker crash this backend cannot discover or re-attach an
already orphaned local process; deployments requiring that guarantee need an
external supervisor with durable process identity.
"""

from __future__ import annotations

import os
import selectors
import signal
import subprocess
import time
from pathlib import Path
from typing import Iterable

from .artifacts import (
    ArtifactKind,
    ArtifactSensitivity,
    ArtifactStore,
)
from .policy import Capability, EffectClass
from .sandbox import (
    BackendExecutionResult,
    CancellationProbe,
    CancellationSignal,
    ExecutionRequest,
    SandboxProfile,
    SecurityLevel,
)

_READ_CHUNK_BYTES = 64 * 1024
_POLL_SECONDS = 0.02
_TERM_GRACE_SECONDS = 0.2
_KILL_WAIT_SECONDS = 1.0


class LocalProcessSupervisorBackend:
    """Supervise one local POSIX process group and persist output as an Artifact."""

    backend_id = "local-process-supervisor"
    security_level = SecurityLevel.DEVELOPMENT_UNSAFE
    supports_materialized_script = False

    def __init__(
        self,
        artifacts: ArtifactStore,
        *,
        capabilities: Iterable[Capability | str] = (),
    ) -> None:
        if not callable(getattr(artifacts, "put_json", None)):
            raise TypeError("artifacts must provide put_json")
        self.artifacts = artifacts
        self.capabilities = tuple(
            sorted(
                {
                    value if isinstance(value, Capability) else Capability(value)
                    for value in capabilities
                },
                key=lambda value: value.name,
            )
        )

    def execute(
        self,
        request: ExecutionRequest,
        profile: SandboxProfile,
    ) -> BackendExecutionResult:
        del profile
        if os.name != "posix" or not hasattr(os, "killpg"):
            raise RuntimeError("local process supervision requires POSIX process groups")
        if request.action.effect_class is not EffectClass.READ_ONLY:
            raise RuntimeError("development process backend accepts read-only actions only")
        if request.environment:
            raise RuntimeError(
                "local process backend has no trusted EnvironmentBinding resolver"
            )
        initial_cancellation = self._poll_cancellation(
            request.cancellation_probe
        )
        if initial_cancellation is CancellationSignal.CANCEL_REQUESTED:
            return BackendExecutionResult(exit_code=None, cancelled=True)
        if initial_cancellation is CancellationSignal.UNKNOWN:
            return BackendExecutionResult(
                exit_code=None,
                cancellation_uncertain=True,
            )

        process = subprocess.Popen(
            request.argv,
            cwd=request.cwd,
            env=self._minimal_environment(Path(request.cwd)),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            start_new_session=True,
            close_fds=True,
        )
        try:
            try:
                process_group = os.getpgid(process.pid)
            except ProcessLookupError:
                # A very short-lived group leader can exit before this
                # diagnostic lookup while descendants still retain its
                # process group and pipe descriptors. ``start_new_session``
                # already made the child's PID the intended PGID.
                process_group = process.pid
            if process_group != process.pid:
                process.kill()
                process.wait(timeout=_KILL_WAIT_SECONDS)
                raise RuntimeError(
                    "child process did not enter a dedicated process group"
                )

            (
                stdout,
                stderr,
                timed_out,
                truncated,
                cancelled,
                cancellation_uncertain,
            ) = self._collect_output(
                process,
                timeout_seconds=request.limits.timeout_seconds,
                output_limit=request.limits.output_bytes,
                cancellation_probe=request.cancellation_probe,
            )
            exit_code = process.returncode
            if exit_code is None:
                raise RuntimeError("supervised process was not reaped")
            if (
                truncated
                and not timed_out
                and not cancelled
                and not cancellation_uncertain
                and exit_code == 0
            ):
                exit_code = 125

            output_ref = self.artifacts.put_json(
                {
                    "exit_code": exit_code,
                    "output_truncated": truncated,
                    "stderr": stderr.decode("utf-8", errors="replace"),
                    "stdout": stdout.decode("utf-8", errors="replace"),
                    "timed_out": timed_out,
                    "cancelled": cancelled,
                    "cancellation_uncertain": cancellation_uncertain,
                },
                kind=ArtifactKind.TOOL_RESULT,
                sensitivity=ArtifactSensitivity.SENSITIVE,
                producer_run_id=request.action.run_id,
                producer_node_id=request.action.node_id,
                producer_attempt_id=request.action.attempt_id,
            )
            return BackendExecutionResult(
                exit_code=exit_code,
                timed_out=timed_out,
                cancelled=cancelled,
                cancellation_uncertain=cancellation_uncertain,
                output_artifact_refs=(output_ref,),
            )
        except BaseException:
            self._terminate_process_group(process)
            raise
        finally:
            for stream in (process.stdout, process.stderr):
                if stream is not None and not stream.closed:
                    stream.close()

    @staticmethod
    def _minimal_environment(cwd: Path) -> dict[str, str]:
        return {
            "HOME": str(cwd),
            "LANG": "C",
            "LC_ALL": "C",
            "PATH": os.defpath,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONIOENCODING": "utf-8",
            "PYTHONUNBUFFERED": "1",
            "TMPDIR": str(cwd),
        }

    def _collect_output(
        self,
        process: subprocess.Popen[bytes],
        *,
        timeout_seconds: float,
        output_limit: int,
        cancellation_probe: CancellationProbe | None,
    ) -> tuple[bytes, bytes, bool, bool, bool, bool]:
        assert process.stdout is not None
        assert process.stderr is not None
        selector = selectors.DefaultSelector()
        chunks: dict[str, list[bytes]] = {"stdout": [], "stderr": []}
        retained = 0
        timed_out = False
        truncated = False
        cancelled = False
        cancellation_uncertain = False
        deadline = time.monotonic() + timeout_seconds

        try:
            selector.register(
                process.stdout,
                selectors.EVENT_READ,
                "stdout",
            )
            selector.register(
                process.stderr,
                selectors.EVENT_READ,
                "stderr",
            )
            while selector.get_map() or process.poll() is None:
                cancellation = self._poll_cancellation(cancellation_probe)
                if cancellation is CancellationSignal.CANCEL_REQUESTED:
                    cancelled = True
                    self._terminate_process_group(process)
                    break
                if cancellation is CancellationSignal.UNKNOWN:
                    cancellation_uncertain = True
                    self._terminate_process_group(process)
                    break
                remaining_time = deadline - time.monotonic()
                if remaining_time <= 0:
                    timed_out = True
                    self._terminate_process_group(process)
                    break
                if not selector.get_map():
                    try:
                        process.wait(timeout=min(_POLL_SECONDS, remaining_time))
                    except subprocess.TimeoutExpired:
                        pass
                    continue
                events = selector.select(
                    timeout=min(_POLL_SECONDS, remaining_time)
                )
                for key, _mask in events:
                    data = os.read(key.fd, _READ_CHUNK_BYTES)
                    if not data:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                        continue
                    available = max(0, output_limit - retained)
                    if len(data) > available:
                        if available:
                            chunks[key.data].append(data[:available])
                            retained += available
                        truncated = True
                        self._terminate_process_group(process)
                        break
                    chunks[key.data].append(data)
                    retained += len(data)
                if truncated:
                    break

            if process.poll() is None:
                self._terminate_process_group(process)
            else:
                process.wait(timeout=_KILL_WAIT_SECONDS)
                # The group leader may exit after spawning descendants that
                # close inherited stdout/stderr.  Pipe EOF is therefore not
                # proof that the process group is empty.
                if self._process_group_exists(process.pid):
                    self._terminate_process_group(process)
            self._drain_closed_group(selector, chunks, output_limit, retained)
        finally:
            selector.close()
            for stream in (process.stdout, process.stderr):
                if not stream.closed:
                    stream.close()

        return (
            b"".join(chunks["stdout"]),
            b"".join(chunks["stderr"]),
            timed_out,
            truncated,
            cancelled,
            cancellation_uncertain,
        )

    @staticmethod
    def _poll_cancellation(
        probe: CancellationProbe | None,
    ) -> CancellationSignal:
        return (
            CancellationSignal.CONTINUE
            if probe is None
            else probe.poll()
        )

    @staticmethod
    def _drain_closed_group(
        selector: selectors.BaseSelector,
        chunks: dict[str, list[bytes]],
        output_limit: int,
        retained: int,
    ) -> None:
        deadline = time.monotonic() + _KILL_WAIT_SECONDS
        while selector.get_map() and time.monotonic() < deadline:
            events = selector.select(_POLL_SECONDS)
            for key, _mask in events:
                data = os.read(key.fd, _READ_CHUNK_BYTES)
                if not data:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                available = max(0, output_limit - retained)
                if available:
                    kept = data[:available]
                    chunks[key.data].append(kept)
                    retained += len(kept)

    @staticmethod
    def _process_group_exists(process_group: int) -> bool:
        try:
            os.killpg(process_group, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    @staticmethod
    def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
        process_group = process.pid
        if process_group <= 1 or process_group == os.getpgrp():
            raise RuntimeError("refusing to signal an unsafe process group")
        try:
            os.killpg(process_group, signal.SIGTERM)
        except ProcessLookupError:
            pass

        deadline = time.monotonic() + _TERM_GRACE_SECONDS
        while time.monotonic() < deadline:
            try:
                os.killpg(process_group, 0)
            except ProcessLookupError:
                break
            except PermissionError:
                # Some managed runtimes deny signal-0 probes even for a child
                # group after its leader has already exited.  A reaped leader
                # with closed inherited pipes proves this group has no live
                # output-holding descendants; otherwise retain uncertainty and
                # proceed to KILL.
                if process.poll() is not None:
                    break
            time.sleep(_POLL_SECONDS)
        else:
            try:
                os.killpg(process_group, signal.SIGKILL)
            except ProcessLookupError:
                pass

        try:
            process.wait(timeout=_KILL_WAIT_SECONDS)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process_group, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=_KILL_WAIT_SECONDS)


__all__ = ["LocalProcessSupervisorBackend"]
