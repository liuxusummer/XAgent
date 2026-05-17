from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


BOOT_MEMORY_FILES = ("global_mem_insight.txt", "insight_fixed_structure.txt")
GLOBAL_MEMORY_FILE = "global_mem.txt"
DEFAULT_MEMORY_SOP = "memory_management_sop.md"
AGENT_MEMORY_FILE = "MEMORY.md"
WORKSPACE_MEMORY_DIR = Path("system") / "memory"
WORKSPACE_AGENT_DIR = Path("system") / "agents"
WORKSPACE_MEMORY_SUFFIXES = {".md", ".txt"}
MEMORY_MODE_PROJECT = "project"
MEMORY_MODE_PRIVATE = "private"
MEMORY_MODE_GLOBAL = "global"
MEMORY_MODE_NONE = "none"


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


def _safe_child_name(name: str) -> bool:
    candidate = Path(name)
    return bool(
        name
        and name not in {".", ".."}
        and not candidate.is_absolute()
        and len(candidate.parts) == 1
        and candidate.name == name
    )


def _read_absolute_files(paths: list[tuple[str, Path]]) -> MemoryReadResult:
    contents: list[str] = []
    statuses: list[MemoryFileStatus] = []
    for name, path in paths:
        if not path.is_file():
            status = _empty_status(name, path, exists=False)
            statuses.append(status)
            continue
        try:
            content = path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            status = _empty_status(name, path, exists=True, error=type(exc).__name__)
        except UnicodeError as exc:
            status = _empty_status(name, path, exists=True, error=type(exc).__name__)
        else:
            status = MemoryFileStatus(
                name=name,
                path=str(path),
                exists=True,
                empty=not content,
                chars=len(content),
            )
            if content:
                contents.append(content)
        statuses.append(status)
    return MemoryReadResult(content="\n\n".join(contents), files=tuple(statuses))


def load_boot_memory(memory_root: str | Path) -> MemoryReadResult:
    return _read_files(memory_root, BOOT_MEMORY_FILES)


def load_global_memory(memory_root: str | Path) -> MemoryReadResult:
    return _read_files(memory_root, (GLOBAL_MEMORY_FILE,))


def load_workspace_memory(workspace_root: str | Path) -> MemoryReadResult:
    root = _memory_root_path(workspace_root)
    memory_dir = root / WORKSPACE_MEMORY_DIR
    paths: list[tuple[str, Path]] = []
    if memory_dir.is_dir():
        for path in sorted(memory_dir.iterdir(), key=lambda item: item.name):
            if path.is_file() and path.suffix.lower() in WORKSPACE_MEMORY_SUFFIXES:
                paths.append((path.name, path))
    return _read_absolute_files(paths)


def load_agent_memory(workspace_root: str | Path, agent_name: str) -> MemoryReadResult:
    root = _memory_root_path(workspace_root)
    if not _safe_child_name(agent_name):
        status = _empty_status(
            AGENT_MEMORY_FILE,
            root / WORKSPACE_AGENT_DIR / agent_name / AGENT_MEMORY_FILE,
            exists=False,
            error="invalid_agent_name",
        )
        return MemoryReadResult(content="", files=(status,))
    return _read_absolute_files([
        (
            f"agents/{agent_name}/{AGENT_MEMORY_FILE}",
            root / WORKSPACE_AGENT_DIR / agent_name / AGENT_MEMORY_FILE,
        )
    ])


def load_effective_memory(
    workspace_root: str | Path,
    agent_name: str = "",
    mode: str = MEMORY_MODE_PROJECT,
) -> MemoryReadResult:
    normalized = str(mode or MEMORY_MODE_PROJECT).strip().lower()
    if normalized not in {
        MEMORY_MODE_PROJECT,
        MEMORY_MODE_PRIVATE,
        MEMORY_MODE_GLOBAL,
        MEMORY_MODE_NONE,
    }:
        normalized = MEMORY_MODE_PROJECT
    if normalized == MEMORY_MODE_NONE:
        return MemoryReadResult(content="", files=())

    results: list[MemoryReadResult] = []
    if normalized in {MEMORY_MODE_PROJECT, MEMORY_MODE_GLOBAL}:
        results.append(load_workspace_memory(workspace_root))
    if normalized in {MEMORY_MODE_PROJECT, MEMORY_MODE_PRIVATE} and agent_name:
        results.append(load_agent_memory(workspace_root, agent_name))

    contents = [result.content for result in results if result.content]
    files: list[MemoryFileStatus] = []
    for result in results:
        files.extend(result.files)
    return MemoryReadResult(content="\n\n".join(contents), files=tuple(files))


def load_memory_sop(
    memory_root: str | Path,
    name: str = DEFAULT_MEMORY_SOP,
) -> MemoryReadResult:
    root = _memory_root_path(memory_root)
    if not _safe_child_name(name):
        status = _empty_status(name, root / name, exists=False, error="invalid_name")
        return MemoryReadResult(content="", files=(status,))
    return _read_files(root, (name,))
