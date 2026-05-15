from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any


FILE_READ_CHAR_LIMIT = 20000
FILE_REF_PATTERN = re.compile(r"\{\{file:(.+?):(\d+):(\d+)}}")
KEYWORD_CONTEXT_LINES = 3
READ_OPERATIONS = {"read"}
WRITE_OPERATIONS = {"create", "update", "write", "patch", "delete"}
SUPPORTED_OPERATIONS = READ_OPERATIONS | WRITE_OPERATIONS
WORKSPACE_SYSTEM_DIR = "system"


class WorkspacePermissionError(ValueError):
    def __init__(self, message: str, path: Path, workspace: Path, operation: str) -> None:
        super().__init__(message)
        self.path = path
        self.workspace = workspace
        self.operation = operation

    def to_result(self) -> dict[str, Any]:
        return {
            "status": "ERROR",
            "error": str(self),
            "path": str(self.path),
            "workspace": str(self.workspace),
            "operation": self.operation,
        }


def read_file(
    path: str,
    cwd: str | None = None,
    start_line: int | None = None,
    end_line: int | None = None,
    keyword: str | None = None,
) -> dict[str, Any]:
    try:
        file_path = resolve_path(path, cwd, operation="read")
    except WorkspacePermissionError as exc:
        return exc.to_result()
    except (OSError, ValueError) as exc:
        return {"status": "ERROR", "error": str(exc)}
    if not file_path.exists():
        candidates = fuzzy_match_path(file_path)
        error = f"file not found: {file_path}"
        if candidates:
            error += f"; did you mean: {', '.join(candidates[:5])}?"
        return {"status": "ERROR", "error": error}
    if file_path.is_dir():
        try:
            entries = sorted(child.name for child in file_path.iterdir())
        except OSError as exc:
            return {"status": "ERROR", "error": f"failed to list directory: {exc}", "path": str(file_path)}
        content = "\n".join(entries)
        return {
            "status": "OK",
            "path": str(file_path),
            "is_directory": True,
            "content": truncate_text(content, FILE_READ_CHAR_LIMIT),
        }

    try:
        content = file_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        return {"status": "ERROR", "error": f"failed to read file: {exc}", "path": str(file_path)}
    lines = content.splitlines(keepends=True)

    if keyword:
        matches = search_keyword(lines, keyword)
        if not matches:
            return {
                "status": "OK",
                "path": str(file_path),
                "is_directory": False,
                "content": f"(keyword '{keyword}' not found in file)",
            }
        matched_lines: list[str] = []
        seen: set[int] = set()
        for line_no in matches:
            ctx_start = max(1, line_no - KEYWORD_CONTEXT_LINES)
            ctx_end = min(len(lines), line_no + KEYWORD_CONTEXT_LINES)
            for i in range(ctx_start, ctx_end + 1):
                if i not in seen:
                    seen.add(i)
                    matched_lines.append(f"{i:>{_digit_width(len(lines))}}→{lines[i - 1]}")
        result_content = "".join(matched_lines)
        return {
            "status": "OK",
            "path": str(file_path),
            "is_directory": False,
            "keyword": keyword,
            "match_count": len(matches),
            "content": truncate_text(result_content, FILE_READ_CHAR_LIMIT),
        }

    if start_line is not None or end_line is not None:
        if start_line is None:
            start_line = 1
        if end_line is None:
            end_line = max(start_line, len(lines) or start_line)
        if start_line <= 0 or end_line < start_line:
            return {
                "status": "ERROR",
                "error": f"invalid line range: start_line={start_line}, end_line={end_line}",
                "path": str(file_path),
            }
        lines_slice = lines[start_line - 1 : end_line]
    else:
        lines_slice = lines

    truncated_lines = truncate_long_lines(lines_slice)
    result_content = "".join(truncated_lines)
    return {
        "status": "OK",
        "path": str(file_path),
        "is_directory": False,
        "content": truncate_text(result_content, FILE_READ_CHAR_LIMIT),
    }


def write_file(
    path: str,
    content: str,
    mode: str = "overwrite",
    cwd: str | None = None,
) -> dict[str, Any]:
    try:
        file_path = resolve_path(path, cwd, operation="write")
    except WorkspacePermissionError as exc:
        return exc.to_result()
    except (OSError, ValueError) as exc:
        return {"status": "ERROR", "error": str(exc)}
    if file_path.exists() and file_path.is_dir():
        return {"status": "ERROR", "error": f"path is a directory: {file_path}", "path": str(file_path)}
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        content = expand_file_refs(content, cwd)
    except (FileNotFoundError, ValueError, OSError) as exc:
        return {"status": "ERROR", "error": str(exc), "path": str(file_path)}

    try:
        if mode == "overwrite":
            final_content = content
        elif mode == "append":
            previous = file_path.read_text(encoding="utf-8") if file_path.exists() else ""
            final_content = previous + content
        elif mode == "prepend":
            previous = file_path.read_text(encoding="utf-8") if file_path.exists() else ""
            final_content = content + previous
        else:
            return {"status": "ERROR", "error": f"unsupported mode: {mode}", "path": str(file_path)}

        file_path.write_text(final_content, encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        return {"status": "ERROR", "error": f"failed to write file: {exc}", "path": str(file_path)}

    return {
        "status": "OK",
        "path": str(file_path),
        "written_bytes": len(final_content.encode("utf-8")),
    }


def patch_file(
    path: str,
    old_content: str,
    new_content: str,
    cwd: str | None = None,
) -> dict[str, Any]:
    try:
        file_path = resolve_path(path, cwd, operation="patch")
    except WorkspacePermissionError as exc:
        return exc.to_result()
    except (OSError, ValueError) as exc:
        return {"status": "ERROR", "error": str(exc)}
    if not file_path.exists():
        return {"status": "ERROR", "error": f"file not found: {file_path}"}
    if file_path.is_dir():
        return {"status": "ERROR", "error": f"path is a directory: {file_path}"}

    try:
        old_content = expand_file_refs(old_content, cwd)
        new_content = expand_file_refs(new_content, cwd)
    except (FileNotFoundError, ValueError, OSError, UnicodeDecodeError) as exc:
        return {"status": "ERROR", "error": str(exc), "path": str(file_path)}

    if old_content == "":
        return {
            "status": "ERROR",
            "error": "old_content must not be empty; patch requires exactly one non-empty match",
            "path": str(file_path),
        }

    try:
        current = file_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        return {"status": "ERROR", "error": f"failed to read file: {exc}", "path": str(file_path)}

    match_count = current.count(old_content)
    if match_count == 0:
        return {
            "status": "ERROR",
            "error": "patch matched 0 times; please use file_read to confirm current content first",
            "path": str(file_path),
        }
    if match_count > 1:
        return {
            "status": "ERROR",
            "error": f"patch matched {match_count} times; please provide longer old_content to make it unique",
            "path": str(file_path),
            "match_count": match_count,
        }

    updated = current.replace(old_content, new_content, 1)
    try:
        file_path.write_text(updated, encoding="utf-8")
    except OSError as exc:
        return {"status": "ERROR", "error": f"failed to write file: {exc}", "path": str(file_path)}
    return {
        "status": "OK",
        "path": str(file_path),
        "match_count": 1,
        "written_bytes": len(updated.encode("utf-8")),
    }


def delete_file(path: str, cwd: str | None = None, recursive: bool = False) -> dict[str, Any]:
    try:
        file_path = resolve_path(path, cwd, operation="delete")
    except WorkspacePermissionError as exc:
        return exc.to_result()
    except (OSError, ValueError) as exc:
        return {"status": "ERROR", "error": str(exc)}
    if not file_path.exists():
        return {"status": "ERROR", "error": f"file not found: {file_path}", "path": str(file_path)}

    try:
        if file_path.is_dir():
            if not recursive:
                return {
                    "status": "ERROR",
                    "error": f"path is a directory; recursive=true is required: {file_path}",
                    "path": str(file_path),
                }
            shutil.rmtree(file_path)
            return {"status": "OK", "path": str(file_path), "deleted": True, "recursive": True}
        file_path.unlink()
    except OSError as exc:
        return {"status": "ERROR", "error": f"failed to delete file: {exc}", "path": str(file_path)}
    return {"status": "OK", "path": str(file_path), "deleted": True, "recursive": False}


def resolve_path(path: str, cwd: str | None = None, operation: str = "write") -> Path:
    return resolve_path_for_operation(path, cwd, operation)


def resolve_path_for_operation(path: str, cwd: str | None = None, operation: str = "read") -> Path:
    if operation not in SUPPORTED_OPERATIONS:
        raise ValueError(f"unsupported operation: {operation}")
    base = Path(cwd or Path.cwd()).resolve()
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        resolved = candidate.resolve()
    else:
        resolved = (base / candidate).resolve()
    try:
        resolved.relative_to(base)
    except ValueError as exc:
        if operation in READ_OPERATIONS:
            return resolved
        if operation in WRITE_OPERATIONS:
            raise WorkspacePermissionError(
                f"path outside workspace is read-only: {resolved} (workspace: {base}, operation: {operation})",
                resolved,
                base,
                operation,
            ) from exc
    if operation in WRITE_OPERATIONS and _is_workspace_system_path(resolved, base):
        if operation == "delete":
            message = "workspace system files cannot be deleted by agent file tools"
        else:
            message = "workspace system files are read-only for agent file tools"
        raise WorkspacePermissionError(
            f"{message}: {resolved} (workspace: {base}, operation: {operation})",
            resolved,
            base,
            operation,
        )
    return resolved


def _is_workspace_system_path(path: Path, workspace: Path) -> bool:
    system_root = workspace / WORKSPACE_SYSTEM_DIR
    try:
        path.relative_to(system_root)
    except ValueError:
        return False
    return True


def expand_file_refs(content: str, cwd: str | None = None) -> str:
    def replace(match: re.Match[str]) -> str:
        ref_path = match.group(1)
        start_line = int(match.group(2))
        end_line = int(match.group(3))
        if start_line <= 0 or end_line < start_line:
            raise ValueError(
                f"invalid file ref range: {{file:{ref_path}:{start_line}:{end_line}}}"
            )

        file_path = resolve_path(ref_path, cwd, operation="read")
        if not file_path.exists():
            raise FileNotFoundError(f"file ref not found: {file_path}")
        if file_path.is_dir():
            raise ValueError(f"file ref points to a directory: {file_path}")

        try:
            lines = file_path.read_text(encoding="utf-8").splitlines(keepends=True)
        except (OSError, UnicodeDecodeError) as exc:
            raise ValueError(f"failed to read file ref: {exc}") from exc
        return "".join(lines[start_line - 1 : end_line])

    return FILE_REF_PATTERN.sub(replace, content)


def truncate_text(content: str, limit: int) -> str:
    if len(content) <= limit:
        return content
    head = content[: limit // 2]
    tail = content[-(limit // 2) :]
    return f"{head}\n\n...[truncated]...\n\n{tail}"


def search_keyword(lines: list[str], keyword: str) -> list[int]:
    keyword_lower = keyword.lower()
    return [
        idx + 1
        for idx, line in enumerate(lines)
        if keyword_lower in line.lower()
    ]


def truncate_long_lines(lines: list[str]) -> list[str]:
    line_count = len(lines)
    if line_count == 0:
        return lines
    l_max = min(max(100, 256000 // line_count), 8000)
    result: list[str] = []
    for line in lines:
        if len(line.rstrip("\n\r")) > l_max:
            head = line[: l_max // 2]
            tail = line.rstrip("\n\r")[-(l_max // 2) :]
            ending = line[len(line.rstrip("\n\r")):]
            result.append(f"{head}...[truncated]...{tail}{ending}")
        else:
            result.append(line)
    return result


def fuzzy_match_path(file_path: Path) -> list[str]:
    parent = file_path.parent
    target = file_path.name.lower()
    if not parent.exists() or not parent.is_dir():
        return []
    candidates: list[str] = []
    for child in parent.iterdir():
        if child.is_dir():
            continue
        name = child.name
        name_lower = name.lower()
        if name_lower == target:
            continue
        if name_lower.startswith(target) or name_lower.endswith(target) or target in name_lower:
            candidates.append(name)
    return sorted(candidates)


def _digit_width(n: int) -> int:
    return len(str(n))
