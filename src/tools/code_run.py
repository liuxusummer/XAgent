from __future__ import annotations

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


def run_code(
    script: str,
    language: str = "python",
    timeout: int = 60,
    cwd: str | None = None,
) -> dict[str, Any]:
    if not script.strip():
        return {"status": "ERROR", "error": "script is empty"}
    if timeout <= 0:
        return {"status": "ERROR", "error": f"timeout must be positive: {timeout}"}

    command, error = _build_command(script, language)
    if error:
        return {"status": "ERROR", "error": f"unsupported language: {language}"}

    try:
        process = _start_process(command, cwd)
    except (OSError, ValueError) as exc:
        return {"status": "ERROR", "error": str(exc)}

    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []

    def read_stdout() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            stdout_chunks.append(line)

    def read_stderr() -> None:
        assert process.stderr is not None
        for line in process.stderr:
            stderr_chunks.append(line)

    stdout_thread = threading.Thread(target=read_stdout, daemon=True)
    stderr_thread = threading.Thread(target=read_stderr, daemon=True)
    stdout_thread.start()
    stderr_thread.start()

    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        _kill_process_tree(process)
        process.wait()
        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)
        _close_popen_streams(process)
        return {
            "status": "TIMEOUT",
            "language": language,
            "command": _command_preview(command),
            "stdout": "".join(stdout_chunks),
            "stderr": "".join(stderr_chunks),
            "exit_code": None,
            "error": f"process exceeded timeout: {timeout}s",
        }

    stdout_thread.join(timeout=5)
    stderr_thread.join(timeout=5)
    _close_popen_streams(process)

    return {
        "status": "OK" if process.returncode == 0 else "ERROR",
        "language": language,
        "command": _command_preview(command),
        "stdout": "".join(stdout_chunks),
        "stderr": "".join(stderr_chunks),
        "exit_code": process.returncode,
    }


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
        start_new_session=True,
    )


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


def run_code_stream(
    script: str,
    language: str = "python",
    timeout: int = 60,
    cwd: str | None = None,
    stop_signal: threading.Event | None = None,
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

    def read_stdout() -> None:
        for line in process.stdout:
            stdout_queue.put(line)

    def read_stderr() -> None:
        for line in process.stderr:
            stderr_chunks.append(line)

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
            "exit_code": process.returncode,
            **({"error": f"process exceeded timeout: {timeout}s"} if timed_out else {}),
        },
    }
