from __future__ import annotations

import codecs
import os
import queue
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Generator

from src.tools.code_sandbox import (
    CodeExecutionPlan,
    CodeSandboxError,
    cleanup_execution_plan,
    prepare_code_execution,
    validate_execution_plan,
)


HEADER_FILE = Path(__file__).resolve().parent.parent / "assets" / "code_run_header.py"
STREAM_POLL_INTERVAL = 0.05
STREAM_READ_BYTE_SIZE = 8192
CODE_OUTPUT_CHAR_LIMIT = 200_000


def run_code(
    script: str,
    language: str = "python",
    timeout: int = 60,
    cwd: str | None = None,
    *,
    allow_unsafe: bool = False,
    isolation_mode: str | None = None,
) -> dict[str, Any]:
    stdout_chunks: list[str] = []
    result: dict[str, Any] = {
        "status": "ERROR",
        "error": "code execution did not produce a final result",
    }
    for event in run_code_stream(
        script=script,
        language=language,
        timeout=timeout,
        cwd=cwd,
        allow_unsafe=allow_unsafe,
        isolation_mode=isolation_mode,
    ):
        event_type = event.get("type")
        data = event.get("data")
        if event_type == "stdout" and isinstance(data, str):
            stdout_chunks.append(data)
        elif event_type in {"result", "error"} and isinstance(data, dict):
            result = dict(data)
    result["stdout"] = "".join(stdout_chunks)
    return result


def _inject_header(script: str) -> str:
    if not HEADER_FILE.exists():
        return script
    header = HEADER_FILE.read_text(encoding="utf-8").strip()
    if not header:
        return script
    return header + "\n" + script


def _start_process(plan: CodeExecutionPlan) -> subprocess.Popen[str]:
    return subprocess.Popen(
        list(plan.launch_command),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=plan.workspace_path,
        env=dict(plan.environment),
        start_new_session=True,
        close_fds=True,
    )

def _kill_process_tree(process: subprocess.Popen[Any]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (OSError, ProcessLookupError):
        pass
    if process.poll() is None:
        try:
            process.kill()
        except OSError:
            pass


def _command_preview(command: list[str]) -> list[str]:
    if len(command) >= 3 and command[1] == "-c":
        return [command[0], command[1], f"<script:{len(command[2])} chars>"]
    if (
        len(command) >= 5
        and command[0] == "/bin/bash"
        and command[1:4] == ["--noprofile", "--norc", "-c"]
    ):
        return [*command[:4], f"<script:{len(command[4])} chars>"]
    return command


def prepare_code_run_execution(
    *,
    script: str,
    language: str,
    timeout: int,
    cwd: str | None,
    backend: str | None = None,
    unsafe_authorized: bool = False,
) -> CodeExecutionPlan:
    executed_script = _inject_header(script) if language == "python" else script
    return prepare_code_execution(
        script=executed_script,
        language=language,
        timeout=timeout,
        cwd=cwd,
        backend=backend,
        unsafe_authorized=unsafe_authorized,
    )


def _close_popen_streams(process: subprocess.Popen[Any]) -> None:
    for stream in (process.stdout, process.stderr):
        if stream is not None:
            stream.close()


def _read_stream_chunks(stream: Any) -> Generator[str, None, None]:
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    while True:
        data = os.read(stream.fileno(), STREAM_READ_BYTE_SIZE)
        if not data:
            break
        chunk = decoder.decode(data)
        if chunk:
            yield chunk
    tail = decoder.decode(b"", final=True)
    if tail:
        yield tail


def _scratch_limit_exceeded(plan: CodeExecutionPlan) -> bool:
    if plan.backend not in {"bubblewrap", "sandbox-exec"}:
        return False
    scratch = Path(str(getattr(plan.scratch_handle, "name", "") or ""))
    if not scratch.is_dir():
        return True
    total_bytes = 0
    entries = 0
    pending = [scratch]
    try:
        while pending:
            current = pending.pop()
            with os.scandir(current) as iterator:
                for entry in iterator:
                    entries += 1
                    if entries > plan.limits.scratch_entries:
                        return True
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            total_bytes += entry.stat(
                                follow_symlinks=False
                            ).st_size
                            if total_bytes > plan.limits.scratch_bytes:
                                return True
                    except FileNotFoundError:
                        continue
    except OSError:
        return True
    return False


def run_code_stream(
    script: str,
    language: str = "python",
    timeout: int = 60,
    cwd: str | None = None,
    stop_signal: threading.Event | None = None,
    *,
    allow_unsafe: bool = False,
    isolation_mode: str | None = None,
    execution_plan: CodeExecutionPlan | None = None,
) -> Generator[dict[str, Any], None, None]:
    if not script.strip():
        yield {"type": "error", "data": {"status": "ERROR", "error": "script is empty"}}
        return
    if timeout <= 0:
        yield {
            "type": "error",
            "data": {"status": "ERROR", "error": f"timeout must be positive: {timeout}"},
        }
        return
    if execution_plan is None and not allow_unsafe and isolation_mode is None:
        yield {
            "type": "error",
            "data": {
                "status": "ERROR",
                "error": "unsafe code execution requires explicit authorization",
                "reason_code": "EXECUTION_AUTHORIZATION_REQUIRED",
            },
        }
        return

    executed_script = _inject_header(script) if language == "python" else script
    plan: CodeExecutionPlan | None = None
    plan_created_here = execution_plan is None
    try:
        if execution_plan is not None and not isinstance(
            execution_plan,
            CodeExecutionPlan,
        ):
            raise CodeSandboxError(
                "EXECUTION_PLAN_MISMATCH",
                "execution plan is invalid",
            )
        if execution_plan is not None and execution_plan.unsafe and not allow_unsafe:
            raise CodeSandboxError(
                "EXECUTION_AUTHORIZATION_REQUIRED",
                "unsafe code execution requires explicit authorization",
            )
        plan = execution_plan or prepare_code_execution(
            script=executed_script,
            language=language,
            timeout=timeout,
            cwd=cwd,
            backend=("unsafe" if allow_unsafe and isolation_mode is None else isolation_mode),
            unsafe_authorized=allow_unsafe,
        )
        validate_execution_plan(
            plan,
            script=executed_script,
            language=language,
            timeout=timeout,
            cwd=cwd,
        )
    except CodeSandboxError as exc:
        if plan_created_here:
            cleanup_execution_plan(plan)
        yield {
            "type": "error",
            "data": {
                "status": "ERROR",
                "error": str(exc),
                "reason_code": exc.reason_code,
                "security": {
                    "security_level": "unavailable",
                    "unsafe": False,
                },
            },
        }
        return

    try:
        process = _start_process(plan)
    except (OSError, ValueError) as exc:
        if plan_created_here:
            cleanup_execution_plan(plan)
        yield {
            "type": "error",
            "data": {
                "status": "ERROR",
                "error": "failed to start the authorized execution plan",
                "reason_code": "EXECUTION_START_FAILED",
                "security": plan.security_receipt(),
            },
        }
        return

    assert process.stdout is not None
    assert process.stderr is not None

    stdout_queue: queue.Queue[str] = queue.Queue()
    stderr_chunks: list[str] = []
    stdout_chars = 0
    stderr_chars = 0
    stdout_truncated = False
    stderr_truncated = False
    output_limit_reached = threading.Event()

    def read_stdout() -> None:
        nonlocal stdout_chars, stdout_truncated
        for chunk in _read_stream_chunks(process.stdout):
            remaining = CODE_OUTPUT_CHAR_LIMIT - stdout_chars
            if remaining > 0:
                retained = chunk[:remaining]
                stdout_chars += len(retained)
                stdout_queue.put(retained)
            if len(chunk) > remaining:
                stdout_truncated = True
                output_limit_reached.set()

    def read_stderr() -> None:
        nonlocal stderr_chars, stderr_truncated
        for chunk in _read_stream_chunks(process.stderr):
            remaining = CODE_OUTPUT_CHAR_LIMIT - stderr_chars
            if remaining > 0:
                retained = chunk[:remaining]
                stderr_chars += len(retained)
                stderr_chunks.append(retained)
            if len(chunk) > remaining:
                stderr_truncated = True
                output_limit_reached.set()

    stdout_thread = threading.Thread(target=read_stdout, daemon=True)
    stderr_thread = threading.Thread(target=read_stderr, daemon=True)
    deadline = time.monotonic() + timeout
    interrupted = False
    timed_out = False
    output_limited = False
    scratch_limited = False
    reader_start_failed = False
    stdout_thread_started = False
    stderr_thread_started = False
    try:
        try:
            stdout_thread.start()
            stdout_thread_started = True
            stderr_thread.start()
            stderr_thread_started = True
        except RuntimeError:
            reader_start_failed = True
        if not reader_start_failed:
            while True:
                if _scratch_limit_exceeded(plan):
                    scratch_limited = True
                    _kill_process_tree(process)
                    break
                if output_limit_reached.is_set():
                    output_limited = True
                    _kill_process_tree(process)
                    break
                if stop_signal is not None and stop_signal.is_set():
                    interrupted = True
                    _kill_process_tree(process)
                    break
                if time.monotonic() >= deadline:
                    timed_out = True
                    _kill_process_tree(process)
                    break
                try:
                    line = stdout_queue.get(timeout=STREAM_POLL_INTERVAL)
                except queue.Empty:
                    if process.poll() is not None:
                        break
                    continue
                yield {"type": "stdout", "data": line}
    finally:
        # ``Generator.close()`` injects GeneratorExit at a suspended stdout
        # yield.  Cleanup must therefore live in ``finally`` rather than an
        # ``except Exception`` block, otherwise abandoning a stream can leave
        # its process group running.
        _kill_process_tree(process)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            timed_out = True
            _kill_process_tree(process)
            try:
                process.wait(timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                pass
        except OSError:
            pass
        if stdout_thread_started:
            stdout_thread.join(timeout=5)
        if stderr_thread_started:
            stderr_thread.join(timeout=5)
        _close_popen_streams(process)
        if plan_created_here:
            cleanup_execution_plan(plan)

    if reader_start_failed:
        yield {
            "type": "error",
            "data": {
                "status": "ERROR",
                "error": "failed to initialize execution output supervision",
                "reason_code": "EXECUTION_SUPERVISOR_START_FAILED",
                "security": plan.security_receipt(),
            },
        }
        return

    while True:
        try:
            line = stdout_queue.get_nowait()
        except queue.Empty:
            break
        yield {"type": "stdout", "data": line}

    output_limited = output_limited or output_limit_reached.is_set()
    while True:
        try:
            line = stdout_queue.get_nowait()
        except queue.Empty:
            break
        yield {"type": "stdout", "data": line}
    stderr_text = "".join(stderr_chunks)

    if interrupted:
        status = "INTERRUPTED"
    elif timed_out:
        status = "TIMEOUT"
    elif output_limited:
        status = "ERROR"
    elif scratch_limited:
        status = "ERROR"
    elif process.returncode == 0:
        status = "OK"
    else:
        status = "ERROR"

    yield {
        "type": "result",
        "data": {
            "status": status,
            "language": language,
            "command": _command_preview(list(plan.payload_command)),
            "stderr": stderr_text,
            "stdout_truncated": stdout_truncated,
            "stderr_truncated": stderr_truncated,
            "output_char_limit": CODE_OUTPUT_CHAR_LIMIT,
            "exit_code": process.returncode,
            "security": plan.security_receipt(),
            **(
                {
                    "error": f"process exceeded timeout: {timeout}s",
                    "reason_code": "TIMEOUT",
                }
                if timed_out
                else (
                    {
                        "error": "process exceeded output limit",
                        "reason_code": "OUTPUT_LIMIT_EXCEEDED",
                    }
                    if output_limited
                    else (
                        {
                            "error": "process exceeded scratch space limit",
                            "reason_code": "SCRATCH_LIMIT_EXCEEDED",
                        }
                        if scratch_limited
                        else {}
                    )
                )
            ),
        },
    }
