from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


BOOT_MEMORY_FILES = ("global_mem_insight.txt", "insight_fixed_structure.txt")
GLOBAL_MEMORY_FILE = "global_mem.txt"
DEFAULT_MEMORY_SOP = "memory_management_sop.md"


@dataclass(frozen=True)
class MemoryFileStatus:
    name: str
    path: str
    exists: bool
    empty: bool
    chars: int
    error: str | None = None


@dataclass(frozen=True)
class MemoryReadResult:
    content: str
    files: tuple[MemoryFileStatus, ...]


def _memory_root_path(memory_root: str | Path) -> Path:
    if not isinstance(memory_root, (str, Path)):
        raise TypeError(f"memory_root must be str or Path, got {type(memory_root)!r}")
    return Path(memory_root)


def _empty_status(name: str, path: Path, *, exists: bool = False, error: str | None = None) -> MemoryFileStatus:
    return MemoryFileStatus(
        name=name,
        path=str(path),
        exists=exists,
        empty=True,
        chars=0,
        error=error,
    )


def _read_memory_file(root: Path, name: str) -> tuple[str, MemoryFileStatus]:
    path = root / name
    if not path.is_file():
        return "", _empty_status(name, path, exists=False)
    try:
        content = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        return "", _empty_status(name, path, exists=True, error=type(exc).__name__)
    except UnicodeError as exc:
        return "", _empty_status(name, path, exists=True, error=type(exc).__name__)
    return content, MemoryFileStatus(
        name=name,
        path=str(path),
        exists=True,
        empty=not content,
        chars=len(content),
    )


def _read_files(memory_root: str | Path, names: tuple[str, ...]) -> MemoryReadResult:
    root = _memory_root_path(memory_root)
    contents: list[str] = []
    statuses: list[MemoryFileStatus] = []
    for name in names:
        content, status = _read_memory_file(root, name)
        statuses.append(status)
        if content:
            contents.append(content)
    return MemoryReadResult(content="\n\n".join(contents), files=tuple(statuses))


def load_boot_memory(memory_root: str | Path) -> MemoryReadResult:
    return _read_files(memory_root, BOOT_MEMORY_FILES)


def load_global_memory(memory_root: str | Path) -> MemoryReadResult:
    return _read_files(memory_root, (GLOBAL_MEMORY_FILE,))


def load_memory_sop(
    memory_root: str | Path,
    name: str = DEFAULT_MEMORY_SOP,
) -> MemoryReadResult:
    root = _memory_root_path(memory_root)
    candidate = Path(name)
    if (
        not name
        or name in {".", ".."}
        or candidate.is_absolute()
        or len(candidate.parts) != 1
        or candidate.name != name
    ):
        status = _empty_status(name, root / name, exists=False, error="invalid_name")
        return MemoryReadResult(content="", files=(status,))
    return _read_files(root, (name,))
