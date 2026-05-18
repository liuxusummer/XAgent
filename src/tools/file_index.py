from __future__ import annotations

import fnmatch
import re
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any, Iterable


INDEX_DB_RELATIVE_PATH = Path("runtime") / "file_index.sqlite3"
DEFAULT_EXCLUDE_GLOBS = (
    ".DS_Store",
    "**/.DS_Store",
    ".git/**",
    "node_modules/**",
    ".venv/**",
    "venv/**",
    "__pycache__/**",
    "dist/**",
    "build/**",
    "runtime/**",
)
DEFAULT_MAX_FILE_BYTES: int | None = None
SNIPPET_CONTEXT_CHARS = 80
QUERY_TOKEN_PATTERN = re.compile(r"[\w\u4e00-\u9fff]+", re.UNICODE)


def refresh_file_index(
    root: str = "",
    cwd: str | None = None,
    include_globs: Iterable[str] | None = None,
    exclude_globs: Iterable[str] | None = None,
    max_file_bytes: int | None = DEFAULT_MAX_FILE_BYTES,
) -> dict[str, Any]:
    try:
        workspace = _workspace(cwd)
        scan_root = _resolve_index_root(root, workspace)
        db_path = _index_db_path(workspace)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        include = tuple(include_globs or ())
        exclude = tuple(DEFAULT_EXCLUDE_GLOBS) + tuple(exclude_globs or ())
    except (OSError, ValueError) as exc:
        return {"status": "ERROR", "error": str(exc)}

    stats: dict[str, Any] = {
        "status": "OK",
        "workspace": str(workspace),
        "root": _relative_path(scan_root, workspace),
        "index_path": str(db_path),
        "scanned": 0,
        "indexed": 0,
        "updated": 0,
        "unchanged": 0,
        "removed": 0,
        "skipped": {
            "directories": 0,
            "excluded": 0,
            "too_large": 0,
            "binary_or_non_utf8": 0,
            "errors": 0,
        },
    }

    try:
        with closing(sqlite3.connect(db_path)) as conn:
            _ensure_schema(conn)
            existing = _load_existing(conn)
            seen: set[str] = set()
            for file_path in _walk_files(scan_root, workspace, exclude, stats):
                rel_path = _relative_path(file_path, workspace)
                if _matches_any(rel_path, exclude) or (include and not _matches_any(rel_path, include)):
                    stats["skipped"]["excluded"] += 1
                    continue
                stats["scanned"] += 1
                try:
                    stat = file_path.stat()
                except OSError:
                    stats["skipped"]["errors"] += 1
                    continue
                if max_file_bytes is not None and max_file_bytes > 0 and stat.st_size > max_file_bytes:
                    stats["skipped"]["too_large"] += 1
                    continue

                seen.add(rel_path)
                old = existing.get(rel_path)
                if old and old == (stat.st_mtime_ns, stat.st_size):
                    stats["unchanged"] += 1
                    continue
                try:
                    content = file_path.read_text(encoding="utf-8")
                except (OSError, UnicodeDecodeError):
                    stats["skipped"]["binary_or_non_utf8"] += 1
                    _delete_path(conn, rel_path)
                    continue
                if "\x00" in content:
                    stats["skipped"]["binary_or_non_utf8"] += 1
                    _delete_path(conn, rel_path)
                    continue

                row_id = _upsert_file(conn, rel_path, content, stat.st_size, stat.st_mtime_ns, stat.st_mtime)
                _replace_fts(conn, row_id, rel_path, content)
                if old:
                    stats["updated"] += 1
                else:
                    stats["indexed"] += 1

            root_prefix = _relative_path(scan_root, workspace)
            for rel_path in list(existing):
                if _is_under_root(rel_path, root_prefix) and rel_path not in seen:
                    _delete_path(conn, rel_path)
                    stats["removed"] += 1
            conn.commit()
    except sqlite3.Error as exc:
        return {"status": "ERROR", "error": f"index database error: {exc}", "index_path": str(db_path)}

    return stats


def search_file_index(
    query: str,
    cwd: str | None = None,
    root: str = "",
    limit: int = 20,
    refresh: bool = False,
    path_only: bool = False,
) -> dict[str, Any]:
    query = str(query or "").strip()
    if not query:
        return {"status": "ERROR", "error": "query is required"}

    try:
        workspace = _workspace(cwd)
        search_root = _resolve_index_root(root, workspace)
        root_prefix = _relative_path(search_root, workspace)
        db_path = _index_db_path(workspace)
        limit = max(1, min(int(limit), 100))
    except (OSError, ValueError, TypeError) as exc:
        return {"status": "ERROR", "error": str(exc)}

    refreshed = False
    refresh_result: dict[str, Any] | None = None
    if refresh or not db_path.exists():
        refresh_result = refresh_file_index(root=root_prefix, cwd=str(workspace))
        if refresh_result.get("status") != "OK":
            return {
                "status": "ERROR",
                "error": "failed to refresh file index",
                "refresh": refresh_result,
            }
        refreshed = True

    try:
        with closing(sqlite3.connect(db_path)) as conn:
            _ensure_schema(conn)
            matches = _path_matches(conn, query, root_prefix, limit)
            if not path_only:
                matches.extend(_content_matches(conn, query, root_prefix, limit))
            deduped = _dedupe_matches(matches, query, limit)
            index_stats = _index_stats(conn, root_prefix)
    except sqlite3.Error as exc:
        return {"status": "ERROR", "error": f"index database error: {exc}", "index_path": str(db_path)}

    return {
        "status": "OK",
        "query": query,
        "root": root_prefix,
        "refreshed": refreshed,
        "index_stats": index_stats,
        "refresh_stats": refresh_result,
        "matches": deduped,
    }


def get_file_index_stats(cwd: str | None = None, root: str = "") -> dict[str, Any]:
    try:
        workspace = _workspace(cwd)
        search_root = _resolve_index_root(root, workspace)
        root_prefix = _relative_path(search_root, workspace)
        db_path = _index_db_path(workspace)
    except (OSError, ValueError) as exc:
        return {"status": "ERROR", "error": str(exc)}

    if not db_path.exists():
        return {
            "status": "OK",
            "exists": False,
            "workspace": str(workspace),
            "root": root_prefix,
            "index_path": str(db_path),
            "db_size_bytes": 0,
            "file_count": 0,
            "bytes": 0,
            "last_indexed_at": None,
        }

    try:
        with closing(sqlite3.connect(db_path)) as conn:
            _ensure_schema(conn)
            stats = _index_stats(conn, root_prefix)
            row = conn.execute("SELECT max(indexed_at) FROM file_index_files").fetchone()
    except sqlite3.Error as exc:
        return {"status": "ERROR", "error": f"index database error: {exc}", "index_path": str(db_path)}

    return {
        "status": "OK",
        "exists": True,
        "workspace": str(workspace),
        "root": root_prefix,
        "index_path": str(db_path),
        "db_size_bytes": db_path.stat().st_size,
        "file_count": stats["file_count"],
        "bytes": stats["bytes"],
        "last_indexed_at": float(row[0]) if row and row[0] is not None else None,
    }


def _workspace(cwd: str | None) -> Path:
    workspace = Path(cwd or Path.cwd()).expanduser().resolve()
    if not workspace.exists():
        raise ValueError(f"workspace does not exist: {workspace}")
    if not workspace.is_dir():
        raise ValueError(f"workspace is not a directory: {workspace}")
    return workspace


def _resolve_index_root(root: str, workspace: Path) -> Path:
    candidate = Path(root or ".").expanduser()
    resolved = candidate.resolve() if candidate.is_absolute() else (workspace / candidate).resolve()
    try:
        resolved.relative_to(workspace)
    except ValueError as exc:
        raise ValueError(f"file index root must be inside workspace: {resolved}") from exc
    if not resolved.exists():
        raise ValueError(f"file index root does not exist: {resolved}")
    if not resolved.is_dir():
        raise ValueError(f"file index root is not a directory: {resolved}")
    return resolved


def _index_db_path(workspace: Path) -> Path:
    return workspace / INDEX_DB_RELATIVE_PATH


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS file_index_files (
            id INTEGER PRIMARY KEY,
            path TEXT NOT NULL UNIQUE,
            content TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
            mtime REAL NOT NULL,
            indexed_at REAL NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS file_index_fts
        USING fts5(path, content)
        """
    )


def _load_existing(conn: sqlite3.Connection) -> dict[str, tuple[int, int]]:
    return {
        str(row[0]): (int(row[1]), int(row[2]))
        for row in conn.execute("SELECT path, mtime_ns, size_bytes FROM file_index_files")
    }


def _walk_files(root: Path, workspace: Path, exclude: Iterable[str], stats: dict[str, Any]):
    for child in root.iterdir():
        rel_path = _relative_path(child, workspace)
        if child.is_dir():
            if _matches_any(rel_path, exclude) or _matches_any(rel_path + "/", exclude):
                stats["skipped"]["directories"] += 1
                continue
            yield from _walk_files(child, workspace, exclude, stats)
        elif child.is_file():
            yield child


def _matches_any(path: str, patterns: Iterable[str]) -> bool:
    normalized = path.replace("\\", "/")
    return any(fnmatch.fnmatch(normalized, pattern) for pattern in patterns)


def _relative_path(path: Path, workspace: Path) -> str:
    rel = path.resolve().relative_to(workspace)
    return "." if str(rel) == "." else rel.as_posix()


def _is_under_root(path: str, root: str) -> bool:
    if root in {"", "."}:
        return True
    return path == root or path.startswith(root.rstrip("/") + "/")


def _upsert_file(
    conn: sqlite3.Connection,
    path: str,
    content: str,
    size_bytes: int,
    mtime_ns: int,
    mtime: float,
) -> int:
    conn.execute(
        """
        INSERT INTO file_index_files(path, content, size_bytes, mtime_ns, mtime, indexed_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            content=excluded.content,
            size_bytes=excluded.size_bytes,
            mtime_ns=excluded.mtime_ns,
            mtime=excluded.mtime,
            indexed_at=excluded.indexed_at
        """,
        (path, content, size_bytes, mtime_ns, mtime, time.time()),
    )
    row = conn.execute("SELECT id FROM file_index_files WHERE path = ?", (path,)).fetchone()
    return int(row[0])


def _replace_fts(conn: sqlite3.Connection, row_id: int, path: str, content: str) -> None:
    conn.execute("DELETE FROM file_index_fts WHERE rowid = ?", (row_id,))
    conn.execute(
        "INSERT INTO file_index_fts(rowid, path, content) VALUES (?, ?, ?)",
        (row_id, path, content),
    )


def _delete_path(conn: sqlite3.Connection, path: str) -> None:
    row = conn.execute("SELECT id FROM file_index_files WHERE path = ?", (path,)).fetchone()
    if row:
        conn.execute("DELETE FROM file_index_fts WHERE rowid = ?", (int(row[0]),))
    conn.execute("DELETE FROM file_index_files WHERE path = ?", (path,))


def _path_matches(conn: sqlite3.Connection, query: str, root: str, limit: int) -> list[dict[str, Any]]:
    like = f"%{_escape_like(query.lower())}%"
    root_sql, root_params = _root_sql_filter(root, "path")
    rows = conn.execute(
        f"""
        SELECT path, content, size_bytes, mtime
        FROM file_index_files
        WHERE lower(path) LIKE ? ESCAPE '\\'
          AND {root_sql}
        ORDER BY length(path), path
        LIMIT ?
        """,
        (like, *root_params, limit * 2),
    ).fetchall()
    matches = []
    for path, content, size_bytes, mtime in rows:
        line, snippet = _line_snippet(str(content), query)
        matches.append(
            {
                "path": str(path),
                "score": 1.0,
                "match_type": "path",
                "line": line,
                "snippet": snippet,
                "size_bytes": int(size_bytes),
                "mtime": float(mtime),
            }
        )
        if len(matches) >= limit:
            break
    return matches


def _content_matches(conn: sqlite3.Connection, query: str, root: str, limit: int) -> list[dict[str, Any]]:
    fts_query = _to_fts_query(query)
    if not fts_query:
        return []
    root_sql, root_params = _root_sql_filter(root, "f.path")
    rows = conn.execute(
        f"""
        SELECT f.path, files.content, files.size_bytes, files.mtime, bm25(file_index_fts) AS rank
        FROM file_index_fts AS f
        JOIN file_index_files AS files ON files.id = f.rowid
        WHERE file_index_fts MATCH ?
          AND {root_sql}
        ORDER BY rank
        LIMIT ?
        """,
        (fts_query, *root_params, limit * 3),
    ).fetchall()
    matches = []
    for path, content, size_bytes, mtime, rank in rows:
        line, snippet = _line_snippet(str(content), query)
        matches.append(
            {
                "path": str(path),
                "score": _rank_to_score(float(rank)),
                "match_type": "content",
                "line": line,
                "snippet": snippet,
                "size_bytes": int(size_bytes),
                "mtime": float(mtime),
            }
        )
        if len(matches) >= limit:
            break
    return matches


def _root_sql_filter(root: str, column: str) -> tuple[str, tuple[str, ...]]:
    if root in {"", "."}:
        return "1 = 1", ()
    return f"({column} = ? OR {column} LIKE ?)", (root, root.rstrip("/") + "/%")


def _dedupe_matches(matches: list[dict[str, Any]], query: str, limit: int) -> list[dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for match in matches:
        path = str(match["path"])
        current = best.get(path)
        if current is None or _match_priority(match, query) > _match_priority(current, query):
            best[path] = match
    return sorted(best.values(), key=lambda item: (-float(item["score"]), item["path"]))[:limit]


def _match_priority(match: dict[str, Any], query: str) -> tuple[float, int]:
    path_bonus = 1 if query.lower() in str(match["path"]).lower() else 0
    return (float(match["score"]), path_bonus)


def _index_stats(conn: sqlite3.Connection, root: str) -> dict[str, Any]:
    rows = conn.execute("SELECT path, size_bytes FROM file_index_files").fetchall()
    count = 0
    bytes_total = 0
    for path, size_bytes in rows:
        if _is_under_root(str(path), root):
            count += 1
            bytes_total += int(size_bytes)
    return {"file_count": count, "bytes": bytes_total}


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _to_fts_query(query: str) -> str:
    terms = QUERY_TOKEN_PATTERN.findall(query)
    if not terms:
        return ""
    quoted = ['"' + term.replace('"', '""') + '"' for term in terms[:8]]
    return " OR ".join(quoted)


def _line_snippet(content: str, query: str) -> tuple[int | None, str]:
    lowered = content.lower()
    pos = lowered.find(query.lower())
    if pos < 0:
        for term in QUERY_TOKEN_PATTERN.findall(query):
            pos = lowered.find(term.lower())
            if pos >= 0:
                break
    if pos < 0:
        first_line = content.splitlines()[0] if content.splitlines() else ""
        return None, _trim_snippet(first_line)
    line_no = content.count("\n", 0, pos) + 1
    line_start = content.rfind("\n", 0, pos) + 1
    line_end = content.find("\n", pos)
    if line_end < 0:
        line_end = len(content)
    return line_no, _trim_snippet(content[line_start:line_end])


def _trim_snippet(line: str) -> str:
    line = line.strip()
    if len(line) <= SNIPPET_CONTEXT_CHARS * 2:
        return line
    return f"{line[:SNIPPET_CONTEXT_CHARS]}...[truncated]...{line[-SNIPPET_CONTEXT_CHARS:]}"


def _rank_to_score(rank: float) -> float:
    return max(0.0, 1.0 / (1.0 + abs(rank)))
