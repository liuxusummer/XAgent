from __future__ import annotations

import codecs
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Generator


HEADER_FILE = Path(__file__).resolve().parent.parent / "assets" / "code_run_header.py"
STREAM_POLL_INTERVAL = 0.05
STREAM_READ_BYTE_SIZE = 8192
CODE_OUTPUT_CHAR_LIMIT = 200_000
SAFE_SUBPROCESS_ENV_KEYS = {
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "PATH",
    "PATHEXT",
    "PYTHONIOENCODING",
    "PYTHONUTF8",
    "SYSTEMROOT",
    "TERM",
    "TZ",
}


def run_code(
    script: str,
    language: str = "python",
    timeout: int = 60,
    cwd: str | None = None,
    *,
    allow_unsafe: bool = False,
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


def _build_command(script: str, language: str) -> tuple[list[str], str | None]:
    if language == "python":
        return [sys.executable, "-c", _inject_header(script)], None
    if language in {"shell", "bash", "sh"}:
        return ["/bin/bash", "-lc", script], None
    return [], f"unsupported language: {language}"


def _start_process(command: list[str], cwd: str | None) -> subprocess.Popen[str]:
    if cwd:
        cwd_path = Path(cwd)
        if not cwd_path.exists():
            raise ValueError(f"cwd does not exist: {cwd}")
        if not cwd_path.is_dir():
            raise ValueError(f"cwd is not a directory: {cwd}")
    return subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=cwd or None,
        env=_sanitized_subprocess_env(cwd),
        start_new_session=True,
    )


def _sanitized_subprocess_env(cwd: str | None) -> dict[str, str]:
    """Build a minimal environment without forwarding host credentials."""

    workspace = Path(cwd or Path.cwd()).resolve()
    env = {
        key: value
        for key in SAFE_SUBPROCESS_ENV_KEYS
        if (value := os.environ.get(key))
    }
    env["PATH"] = _sanitized_path(env.get("PATH", os.defpath))
    env["HOME"] = str(workspace)
    env["TMPDIR"] = str(workspace)
    env["PYTHONIOENCODING"] = "utf-8"
    env["XAGENT_CODE_RUN_WORKSPACE"] = str(workspace)
    return env


def _sanitized_path(value: str) -> str:
    entries: list[str] = []
    for entry in value.split(os.pathsep):
        if not entry:
            continue
        candidate = Path(entry).expanduser()
        if not candidate.is_absolute():
            continue
        normalized = str(candidate.resolve())
        if normalized not in entries:
            entries.append(normalized)
    return os.pathsep.join(entries) or os.defpath


def _kill_process_tree(process: subprocess.Popen[Any]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (OSError, ProcessLookupError):
        try:
            process.kill()
        except OSError:
            pass


def _command_preview(command: list[str]) -> list[str]:
    if len(command) >= 3 and command[1] == "-c":
        return [command[0], command[1], f"<script:{len(command[2])} chars>"]
    if len(command) >= 3 and command[0] == "/bin/bash" and command[1] == "-lc":
        return [command[0], command[1], f"<script:{len(command[2])} chars>"]
    return command


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


def run_code_stream(
    script: str,
    language: str = "python",
    timeout: int = 60,
    cwd: str | None = None,
    stop_signal: threading.Event | None = None,
    *,
    allow_unsafe: bool = False,
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
    if not allow_unsafe:
        yield {
            "type": "error",
            "data": {
                "status": "ERROR",
                "error": "unsafe code execution requires explicit authorization",
            },
        }
        return

    command, error = _build_command(script, language)
    if error:
        yield {"type": "error", "data": {"status": "ERROR", "error": f"unsupported language: {language}"}}
        return

    try:
        process = _start_process(command, cwd)
    except (OSError, ValueError) as exc:
        yield {"type": "error", "data": {"status": "ERROR", "error": str(exc)}}
        return

    assert process.stdout is not None
    assert process.stderr is not None

    stdout_queue: queue.Queue[str] = queue.Queue()
    stderr_chunks: list[str] = []
    stdout_chars = 0
    stderr_chars = 0
    stdout_truncated = False
    stderr_truncated = False

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

    stdout_thread = threading.Thread(target=read_stdout, daemon=True)
    stderr_thread = threading.Thread(target=read_stderr, daemon=True)
    stdout_thread.start()
    stderr_thread.start()

    deadline = time.monotonic() + timeout
    interrupted = False
    timed_out = False
    try:
        while True:
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
    except Exception:
        _kill_process_tree(process)
        raise

    while True:
        try:
            line = stdout_queue.get_nowait()
        except queue.Empty:
            break
        yield {"type": "stdout", "data": line}

    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        timed_out = True
        _kill_process_tree(process)
        process.wait()

    stdout_thread.join(timeout=5)
    stderr_thread.join(timeout=5)
    while True:
        try:
            line = stdout_queue.get_nowait()
        except queue.Empty:
            break
        yield {"type": "stdout", "data": line}
    _close_popen_streams(process)

    stderr_text = "".join(stderr_chunks)

    if interrupted:
        status = "INTERRUPTED"
    elif timed_out:
        status = "TIMEOUT"
    elif process.returncode == 0:
        status = "OK"
    else:
        status = "ERROR"

    yield {
        "type": "result",
        "data": {
            "status": status,
            "language": language,
            "command": _command_preview(command),
            "stderr": stderr_text,
            "stdout_truncated": stdout_truncated,
            "stderr_truncated": stderr_truncated,
            "output_char_limit": CODE_OUTPUT_CHAR_LIMIT,
            "exit_code": process.returncode,
            **({"error": f"process exceeded timeout: {timeout}s"} if timed_out else {}),
        },
    }
