from __future__ import annotations

import json
import os
import stat
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from src.core.safe_fs import open_lock_file_beneath

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None  # type: ignore[assignment]


_LOCK_FILE = Path("runtime") / ".workspace-write.lock"
_lock_registry_guard = threading.Lock()
_workspace_locks: dict[str, threading.RLock] = {}
_thread_state = threading.local()


def _workspace_key(workspace_root: str | Path) -> tuple[str, Path]:
    root = Path(workspace_root).expanduser().resolve()
    return str(root), root


def _process_lock(key: str) -> threading.RLock:
    with _lock_registry_guard:
        lock = _workspace_locks.get(key)
        if lock is None:
            lock = threading.RLock()
            _workspace_locks[key] = lock
        return lock


def _thread_depths() -> dict[str, int]:
    depths = getattr(_thread_state, "workspace_write_depths", None)
    if depths is None:
        depths = {}
        _thread_state.workspace_write_depths = depths
    return depths


def _thread_secure_locks() -> set[str]:
    secure_locks = getattr(_thread_state, "secure_workspace_locks", None)
    if secure_locks is None:
        secure_locks = set()
        _thread_state.secure_workspace_locks = secure_locks
    return secure_locks


@contextmanager
def workspace_write_lock(
    workspace_root: str | Path,
    *,
    require_secure_path: bool = False,
) -> Iterator[None]:
    """Serialize read-modify-write operations for one workspace."""

    key, root = _workspace_key(workspace_root)
    lock = _process_lock(key)
    with lock:
        depths = _thread_depths()
        secure_locks = _thread_secure_locks()
        depth = depths.get(key, 0)
        if depth:
            if require_secure_path and key not in secure_locks:
                raise OSError(
                    "cannot upgrade an active path-based workspace lock"
                )
            depths[key] = depth + 1
            try:
                yield
            finally:
                depths[key] -= 1
            return

        lock_handle = None
        try:
            if require_secure_path:
                lock_descriptor = open_lock_file_beneath(
                    root,
                    _LOCK_FILE,
                )
                try:
                    lock_handle = os.fdopen(lock_descriptor, "a+b")
                except BaseException:
                    os.close(lock_descriptor)
                    raise
            else:
                lock_path = root / _LOCK_FILE
                lock_path.parent.mkdir(parents=True, exist_ok=True)
                lock_handle = lock_path.open("a+b")
            if fcntl is not None:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            depths[key] = 1
            if require_secure_path:
                secure_locks.add(key)
            yield
        finally:
            depths.pop(key, None)
            secure_locks.discard(key)
            if lock_handle is not None:
                if fcntl is not None:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
                lock_handle.close()


def atomic_write_text(path: str | Path, content: str, *, encoding: str = "utf-8") -> None:
    """Durably replace a text file using a unique same-directory temp file."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        mode = stat.S_IMODE(target.stat().st_mode)
    except FileNotFoundError:
        mode = 0o644

    fd, temp_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    temp_path = Path(temp_name)
    try:
        os.chmod(temp_path, mode)
        with os.fdopen(fd, "w", encoding=encoding) as handle:
            fd = -1
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, target)
        _fsync_directory(target.parent)
    finally:
        if fd >= 0:
            os.close(fd)
        temp_path.unlink(missing_ok=True)


def atomic_write_json(path: str | Path, payload: dict[str, Any]) -> None:
    content = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    atomic_write_text(path, content)


def _fsync_directory(directory: Path) -> None:
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)
