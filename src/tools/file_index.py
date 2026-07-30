from __future__ import annotations

import fnmatch
import hashlib
import json
import math
import os
import re
import sqlite3
import struct
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Protocol
from urllib import error as urlerror
from urllib import request as urlrequest

from src.core.agent_kernel import (
    DataSensitivity,
    KnowledgeItem,
    KnowledgeKind,
    Principal,
    TrustLevel,
)
from src.core.retrieval import EvidenceBundle, EvidenceItem
from src.core.safe_fs import (
    FileChangedDuringReadError,
    FileSizeLimitExceededError,
    UnsafeFileContentError,
    open_regular_file_beneath,
    read_stable_text,
    secure_descriptor_reads_supported,
)


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
    "memory/**",
    "system/memory/**",
    "system/agents/*/MEMORY.md",
)
DEFAULT_MAX_FILE_BYTES = 5 * 1024 * 1024
SNIPPET_CONTEXT_CHARS = 80
QUERY_TOKEN_PATTERN = re.compile(r"[\w\u4e00-\u9fff]+", re.UNICODE)
SYMBOL_QUERY_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:[.:/][A-Za-z0-9_]+)*")
SCHEMA_VERSION = "4"
MIGRATABLE_SCHEMA_VERSIONS = {"1", "2", "3", SCHEMA_VERSION}
INDEX_OWNER_META = "owner_tenant_id"
INDEX_ACL_META = "owner_acl"
INDEX_REVISION_META = "index_revision"
DEFAULT_CHUNK_TARGET_CHARS = 3000
DEFAULT_CHUNK_OVERLAP_CHARS = 300
DEFAULT_LINE_WINDOW = 120
DEFAULT_SEMANTIC_OVERFETCH = 5
SUPPORTED_SEARCH_MODES = {"keyword", "semantic", "hybrid"}
FILE_INDEX_READ_SCOPE = "workspace.read"


class EmbeddingProvider(Protocol):
    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        ...


@dataclass(frozen=True)
class EmbeddingConfig:
    enabled: bool
    apikey: str
    apibase: str
    model: str
    dimension: int
    batch_size: int = 32
    timeout: int = 60

    @property
    def fingerprint(self) -> str:
        return f"openai-compatible:{self.apibase}:{self.model}:{self.dimension}"


def refresh_file_index(
    root: str = "",
    cwd: str | None = None,
    include_globs: Iterable[str] | None = None,
    exclude_globs: Iterable[str] | None = None,
    max_file_bytes: int | None = DEFAULT_MAX_FILE_BYTES,
    semantic: bool = False,
    embedding_config: dict[str, Any] | EmbeddingConfig | None = None,
    embedding_provider: EmbeddingProvider | None = None,
    principal: Principal | None = None,
) -> dict[str, Any]:
    if not isinstance(principal, Principal):
        return {
            "status": "ERROR",
            "error": "authenticated principal is required to build the file index",
            "reason_code": "principal_required",
        }
    if FILE_INDEX_READ_SCOPE not in principal.scopes:
        return {
            "status": "ERROR",
            "error": "workspace.read scope is required to build the file index",
            "reason_code": "principal_scope_missing",
        }
    if not secure_descriptor_reads_supported():
        return {
            "status": "ERROR",
            "error": "secure descriptor-relative file reads are unavailable",
            "reason_code": "secure_file_read_unavailable",
        }
    try:
        workspace = _workspace(cwd)
        scan_root = _resolve_index_root(root, workspace)
        db_path = _index_db_path(workspace)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        include = tuple(include_globs or ())
        exclude = tuple(DEFAULT_EXCLUDE_GLOBS) + tuple(exclude_globs or ())
        semantic_config = _resolve_embedding_config(embedding_config)
    except (OSError, ValueError) as exc:
        return {"status": "ERROR", "error": str(exc)}

    semantic_state = _semantic_state(semantic, semantic_config, embedding_provider)
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
            "symlinks": 0,
            "too_large": 0,
            "binary_or_non_utf8": 0,
            "unsafe_or_changed": 0,
            "errors": 0,
        },
        "semantic_status": semantic_state.get("status", semantic_state),
        "semantic_indexed": 0,
        "semantic_skipped": 0,
    }

    try:
        with closing(sqlite3.connect(db_path)) as conn:
            _ensure_schema(conn)
            _bind_index_owner(conn, principal, reset_unowned=True)
            if semantic_state["enabled"] and semantic_config:
                _reset_semantic_if_config_changed(conn, semantic_config)
                sqlite_vec_status = _ensure_semantic_schema(conn, semantic_config)
                if sqlite_vec_status.get("status") not in {"OK", "UNAVAILABLE"}:
                    semantic_state = {"enabled": False, "status": sqlite_vec_status}
                else:
                    semantic_state["status"] = sqlite_vec_status
            existing = _load_existing(conn)
            seen: set[str] = set()
            for file_path in _walk_files(scan_root, workspace, exclude, stats):
                try:
                    rel_path = _relative_path(file_path, workspace)
                except (OSError, ValueError):
                    stats["skipped"]["errors"] += 1
                    continue
                if _matches_any(rel_path, exclude) or (include and not _matches_any(rel_path, include)):
                    stats["skipped"]["excluded"] += 1
                    continue
                stats["scanned"] += 1
                try:
                    file_descriptor, current_stat = (
                        open_regular_file_beneath(workspace, rel_path)
                    )
                except OSError:
                    stats["skipped"]["errors"] += 1
                    continue

                try:
                    if (
                        max_file_bytes is not None
                        and max_file_bytes > 0
                        and current_stat.st_size > max_file_bytes
                    ):
                        stats["skipped"]["too_large"] += 1
                        continue
                    old = existing.get(rel_path)
                    if old and old == (
                        current_stat.st_mtime_ns,
                        current_stat.st_size,
                    ):
                        seen.add(rel_path)
                        if (
                            semantic_state["enabled"]
                            and semantic_config
                            and not _file_semantic_complete(
                                conn,
                                rel_path,
                                semantic_config,
                            )
                        ):
                            try:
                                content = read_stable_text(
                                    file_descriptor,
                                    current_stat,
                                    max_bytes=max_file_bytes,
                                )
                            except FileSizeLimitExceededError:
                                stats["skipped"]["too_large"] += 1
                                seen.discard(rel_path)
                                _delete_path(conn, rel_path)
                                continue
                            except FileChangedDuringReadError:
                                stats["skipped"]["unsafe_or_changed"] += 1
                                seen.discard(rel_path)
                                _delete_path(conn, rel_path)
                                continue
                            except (
                                OSError,
                                UnicodeDecodeError,
                                UnsafeFileContentError,
                            ):
                                stats["skipped"]["binary_or_non_utf8"] += 1
                                seen.discard(rel_path)
                                _delete_path(conn, rel_path)
                                continue
                            row = conn.execute(
                                "SELECT id FROM file_index_files WHERE path = ?",
                                (rel_path,),
                            ).fetchone()
                            if row and _refresh_file_semantics(
                                conn,
                                int(row[0]),
                                rel_path,
                                content,
                                semantic_config,
                                embedding_provider,
                                semantic_state,
                            ):
                                stats["semantic_indexed"] += 1
                            else:
                                stats["semantic_skipped"] += 1
                        else:
                            stats["unchanged"] += 1
                        continue

                    try:
                        content = read_stable_text(
                            file_descriptor,
                            current_stat,
                            max_bytes=max_file_bytes,
                        )
                    except FileSizeLimitExceededError:
                        stats["skipped"]["too_large"] += 1
                        _delete_path(conn, rel_path)
                        continue
                    except FileChangedDuringReadError:
                        stats["skipped"]["unsafe_or_changed"] += 1
                        _delete_path(conn, rel_path)
                        continue
                    except (
                        OSError,
                        UnicodeDecodeError,
                        UnsafeFileContentError,
                    ):
                        stats["skipped"]["binary_or_non_utf8"] += 1
                        _delete_path(conn, rel_path)
                        continue
                finally:
                    os.close(file_descriptor)

                if "\x00" in content:
                    stats["skipped"]["binary_or_non_utf8"] += 1
                    _delete_path(conn, rel_path)
                    continue
                seen.add(rel_path)

                row_id = _upsert_file(
                    conn,
                    rel_path,
                    content,
                    current_stat.st_size,
                    current_stat.st_mtime_ns,
                    current_stat.st_mtime,
                )
                _replace_fts(conn, row_id, rel_path, content)
                if semantic_state["enabled"] and semantic_config:
                    if _refresh_file_semantics(
                        conn,
                        row_id,
                        rel_path,
                        content,
                        semantic_config,
                        embedding_provider,
                        semantic_state,
                    ):
                        stats["semantic_indexed"] += 1
                    else:
                        stats["semantic_skipped"] += 1
                if old:
                    stats["updated"] += 1
                else:
                    stats["indexed"] += 1

            root_prefix = _relative_path(scan_root, workspace)
            for rel_path in list(existing):
                if _is_under_root(rel_path, root_prefix) and rel_path not in seen:
                    _delete_path(conn, rel_path)
                    stats["removed"] += 1
            revision = _next_index_revision(conn)
            conn.commit()
            stats["index_version"] = f"{SCHEMA_VERSION}:{revision}"
            stats["semantic_status"] = semantic_state.get("status", semantic_state)
    except (PermissionError, ValueError) as exc:
        return {
            "status": "ERROR",
            "error": str(exc),
            "reason_code": "index_acl_denied",
            "index_path": str(db_path),
        }
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
    mode: str = "hybrid",
    embedding_config: dict[str, Any] | EmbeddingConfig | None = None,
    embedding_provider: EmbeddingProvider | None = None,
    principal: Principal | None = None,
) -> dict[str, Any]:
    if not isinstance(principal, Principal):
        return {
            "status": "ERROR",
            "error": "authenticated principal is required to search the file index",
            "reason_code": "principal_required",
            "matches": [],
        }
    if FILE_INDEX_READ_SCOPE not in principal.scopes:
        return {
            "status": "ERROR",
            "error": "workspace.read scope is required to search the file index",
            "reason_code": "principal_scope_missing",
            "matches": [],
        }
    query = str(query or "").strip()
    if not query:
        return {"status": "ERROR", "error": "query is required"}
    mode = str(mode or "hybrid").strip().lower()
    if mode not in SUPPORTED_SEARCH_MODES:
        return {"status": "ERROR", "error": f"invalid search mode: {mode}"}
    if path_only:
        mode = "keyword"

    try:
        workspace = _workspace(cwd)
        search_root = _resolve_index_root(root, workspace)
        root_prefix = _relative_path(search_root, workspace)
        db_path = _index_db_path(workspace)
        limit = max(1, min(int(limit), 100))
        semantic_config = _resolve_embedding_config(embedding_config)
    except (OSError, ValueError, TypeError) as exc:
        return {"status": "ERROR", "error": str(exc)}

    refreshed = False
    refresh_result: dict[str, Any] | None = None
    existing_schema = _stored_index_schema(db_path)
    if (
        refresh
        or not db_path.exists()
        or (
            existing_schema in MIGRATABLE_SCHEMA_VERSIONS
            and existing_schema != SCHEMA_VERSION
        )
    ):
        refresh_result = refresh_file_index(
            root=root_prefix,
            cwd=str(workspace),
            semantic=mode != "keyword",
            embedding_config=semantic_config,
            embedding_provider=embedding_provider,
            principal=principal,
        )
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
            acl = _bind_index_owner(conn, principal, reset_unowned=False)
            index_version = _current_index_version(conn)
            semantic_status: dict[str, Any] = {"enabled": False, "reason": "mode is keyword"}
            keyword_matches: list[dict[str, Any]] = []
            semantic_matches: list[dict[str, Any]] = []
            if mode in {"keyword", "hybrid"}:
                keyword_matches = _path_matches(conn, query, root_prefix, limit)
                keyword_matches.extend(_content_matches(conn, query, root_prefix, limit))
            if mode in {"semantic", "hybrid"}:
                semantic_matches, semantic_status = _semantic_matches(
                    conn,
                    query,
                    root_prefix,
                    limit,
                    semantic_config,
                    embedding_provider,
                )
            if mode == "semantic":
                deduped = _dedupe_chunk_matches(semantic_matches, query, limit)
            elif mode == "hybrid":
                deduped = _hybrid_matches(keyword_matches, semantic_matches, query, limit)
            else:
                deduped = _dedupe_matches(keyword_matches, query, limit)
            deduped, evidence_bundle = _attach_evidence(
                conn,
                deduped,
                query=query,
                principal=principal,
                acl=acl,
                index_version=index_version,
            )
            index_stats = _index_stats(conn, root_prefix)
    except (PermissionError, ValueError) as exc:
        return {
            "status": "ERROR",
            "error": str(exc),
            "reason_code": "index_acl_denied",
            "index_path": str(db_path),
            "matches": [],
        }
    except sqlite3.Error as exc:
        return {"status": "ERROR", "error": f"index database error: {exc}", "index_path": str(db_path)}

    return {
        "status": "OK",
        "query": query,
        "root": root_prefix,
        "mode": mode,
        "refreshed": refreshed,
        "index_stats": index_stats,
        "refresh_stats": refresh_result,
        "semantic_status": semantic_status,
        "index_version": index_version,
        "evidence_bundle": evidence_bundle.to_dict(),
        "evidence_bundle_digest": evidence_bundle.bundle_digest,
        "matches": deduped,
    }


def get_file_index_stats(
    cwd: str | None = None,
    root: str = "",
    *,
    principal: Principal | None = None,
) -> dict[str, Any]:
    if not isinstance(principal, Principal):
        return {
            "status": "ERROR",
            "error": "authenticated principal is required to inspect the file index",
            "reason_code": "principal_required",
        }
    if FILE_INDEX_READ_SCOPE not in principal.scopes:
        return {
            "status": "ERROR",
            "error": "workspace.read scope is required to inspect the file index",
            "reason_code": "principal_scope_missing",
        }
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
            _bind_index_owner(conn, principal, reset_unowned=False)
            stats = _index_stats(conn, root_prefix)
            row = conn.execute("SELECT max(indexed_at) FROM file_index_files").fetchone()
    except (PermissionError, ValueError) as exc:
        return {
            "status": "ERROR",
            "error": str(exc),
            "reason_code": "index_acl_denied",
            "index_path": str(db_path),
        }
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
        relative = resolved.relative_to(workspace)
    except ValueError as exc:
        raise ValueError(f"file index root must be inside workspace: {resolved}") from exc
    parts = tuple(part.casefold() for part in relative.parts)
    if (
        parts
        and (
            parts[0] in {"memory", "runtime"}
            or parts[:2] == ("system", "memory")
        )
    ):
        raise ValueError("file index root is a protected workspace path")
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
        CREATE TABLE IF NOT EXISTS file_index_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    existing_schema = _get_meta(conn, "schema_version")
    if existing_schema is None:
        existing_tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type IN ('table', 'view') "
                "AND name NOT LIKE 'sqlite_%' "
                "AND name != 'file_index_meta'"
            ).fetchall()
        }
        if existing_tables:
            raise PermissionError(
                "file index schema metadata is missing"
            )
    if (
        existing_schema is not None
        and existing_schema not in MIGRATABLE_SCHEMA_VERSIONS
    ):
        raise PermissionError(
            "file index schema is unsupported; an explicit migration is required"
        )
    conn.execute(
        "INSERT OR REPLACE INTO file_index_meta(key, value) VALUES ('schema_version', ?)",
        (SCHEMA_VERSION,),
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS file_index_files (
            id INTEGER PRIMARY KEY,
            path TEXT NOT NULL UNIQUE,
            content TEXT NOT NULL,
            content_sha256 TEXT NOT NULL,
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS file_index_chunks (
            id INTEGER PRIMARY KEY,
            file_id INTEGER NOT NULL,
            path TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            start_line INTEGER NOT NULL,
            end_line INTEGER NOT NULL,
            content TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            token_estimate INTEGER NOT NULL,
            embedded_at REAL,
            embedding_model TEXT,
            UNIQUE(path, chunk_index)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS file_index_chunk_embeddings (
            chunk_id INTEGER NOT NULL,
            model TEXT NOT NULL,
            dimension INTEGER NOT NULL,
            embedding BLOB NOT NULL,
            content_hash TEXT NOT NULL,
            embedded_at REAL NOT NULL,
            PRIMARY KEY(chunk_id, model, dimension)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_index_chunks_path ON file_index_chunks(path)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_file_index_embeddings_model ON file_index_chunk_embeddings(model, dimension)"
    )
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(file_index_files)").fetchall()
    }
    if "content_sha256" not in columns:
        conn.execute(
            "ALTER TABLE file_index_files "
            "ADD COLUMN content_sha256 TEXT NOT NULL DEFAULT ''"
        )
    if existing_schema is not None and existing_schema != SCHEMA_VERSION:
        # Exclusion and evidence-integrity rules are security boundaries.
        # Never carry old indexed content across such a schema change.
        _clear_index_content(conn)
        if _get_meta(conn, INDEX_REVISION_META) is not None:
            _set_meta(conn, INDEX_REVISION_META, "0")


def _stored_index_schema(path: Path) -> str | None:
    if not path.is_file():
        return None
    try:
        with closing(sqlite3.connect(path)) as conn:
            row = conn.execute(
                "SELECT value FROM file_index_meta "
                "WHERE key = 'schema_version'"
            ).fetchone()
    except sqlite3.Error:
        return None
    return str(row[0]) if row is not None else None


def _bind_index_owner(
    conn: sqlite3.Connection,
    principal: Principal,
    *,
    reset_unowned: bool,
) -> tuple[str, ...]:
    owner = _get_meta(conn, INDEX_OWNER_META)
    raw_acl = _get_meta(conn, INDEX_ACL_META)
    if owner is None:
        count_row = conn.execute("SELECT COUNT(*) FROM file_index_files").fetchone()
        has_legacy_content = bool(count_row and int(count_row[0]) > 0)
        if has_legacy_content and not reset_unowned:
            raise PermissionError(
                "file index owner is unknown; an authenticated refresh is required"
            )
        if has_legacy_content:
            _clear_index_content(conn)
        acl = (f"tenant:{principal.tenant_id}",)
        _set_meta(conn, INDEX_OWNER_META, principal.tenant_id)
        _set_meta(conn, INDEX_ACL_META, json.dumps(list(acl), separators=(",", ":")))
        _set_meta(conn, INDEX_REVISION_META, "0")
        return acl
    if owner != principal.tenant_id:
        raise PermissionError("file index belongs to a different tenant")
    if raw_acl is None:
        raise PermissionError("file index ACL metadata is missing")
    try:
        decoded_acl = json.loads(raw_acl)
    except json.JSONDecodeError as exc:
        raise PermissionError("file index ACL metadata is invalid") from exc
    if (
        not isinstance(decoded_acl, list)
        or not decoded_acl
        or not all(isinstance(item, str) and item.strip() for item in decoded_acl)
    ):
        raise PermissionError("file index ACL metadata is invalid")
    acl = tuple(sorted(set(decoded_acl)))
    if f"tenant:{principal.tenant_id}" not in acl:
        raise PermissionError("file index ACL does not authorize the caller")
    revision = _get_meta(conn, INDEX_REVISION_META)
    if revision is None or not revision.isdigit():
        raise PermissionError("file index revision metadata is invalid")
    return acl


def _clear_index_content(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM file_index_chunk_embeddings")
    conn.execute("DELETE FROM file_index_chunks")
    conn.execute("DELETE FROM file_index_fts")
    conn.execute("DELETE FROM file_index_files")
    try:
        conn.execute("DELETE FROM file_index_vec")
    except sqlite3.Error:
        pass


def _next_index_revision(conn: sqlite3.Connection) -> int:
    raw = _get_meta(conn, INDEX_REVISION_META)
    if raw is None or not raw.isdigit():
        raise PermissionError("file index revision metadata is invalid")
    revision = int(raw) + 1
    _set_meta(conn, INDEX_REVISION_META, str(revision))
    return revision


def _current_index_version(conn: sqlite3.Connection) -> str:
    raw = _get_meta(conn, INDEX_REVISION_META)
    if raw is None or not raw.isdigit():
        raise PermissionError("file index revision metadata is invalid")
    return f"{SCHEMA_VERSION}:{int(raw)}"


def _load_existing(conn: sqlite3.Connection) -> dict[str, tuple[int, int]]:
    return {
        str(row[0]): (int(row[1]), int(row[2]))
        for row in conn.execute("SELECT path, mtime_ns, size_bytes FROM file_index_files")
    }


def _walk_files(root: Path, workspace: Path, exclude: Iterable[str], stats: dict[str, Any]):
    try:
        children = root.iterdir()
        for child in children:
            try:
                if child.is_symlink():
                    stats["skipped"]["symlinks"] += 1
                    continue
                rel_path = _relative_path(child, workspace)
                if child.is_dir():
                    if _matches_any(rel_path, exclude) or _matches_any(rel_path + "/", exclude):
                        stats["skipped"]["directories"] += 1
                        continue
                    yield from _walk_files(child, workspace, exclude, stats)
                elif child.is_file():
                    yield child
            except (OSError, ValueError):
                stats["skipped"]["errors"] += 1
    except OSError:
        stats["skipped"]["errors"] += 1


def _matches_any(path: str, patterns: Iterable[str]) -> bool:
    normalized = path.replace("\\", "/").casefold()
    return any(
        fnmatch.fnmatch(normalized, str(pattern).casefold())
        for pattern in patterns
    )


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
    content_sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()
    conn.execute(
        """
        INSERT INTO file_index_files(
            path, content, content_sha256, size_bytes, mtime_ns, mtime, indexed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            content=excluded.content,
            content_sha256=excluded.content_sha256,
            size_bytes=excluded.size_bytes,
            mtime_ns=excluded.mtime_ns,
            mtime=excluded.mtime,
            indexed_at=excluded.indexed_at
        """,
        (
            path,
            content,
            content_sha256,
            size_bytes,
            mtime_ns,
            mtime,
            time.time(),
        ),
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
    _delete_chunks(conn, path)
    conn.execute("DELETE FROM file_index_files WHERE path = ?", (path,))


def _delete_chunks(conn: sqlite3.Connection, path: str) -> None:
    rows = conn.execute("SELECT id FROM file_index_chunks WHERE path = ?", (path,)).fetchall()
    for (chunk_id,) in rows:
        conn.execute("DELETE FROM file_index_chunk_embeddings WHERE chunk_id = ?", (int(chunk_id),))
        _delete_vec_row(conn, int(chunk_id))
    conn.execute("DELETE FROM file_index_chunks WHERE path = ?", (path,))


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
                "start_line": line,
                "end_line": line,
                "snippet": snippet,
                "size_bytes": int(size_bytes),
                "mtime": float(mtime),
                "signals": {"path": 1.0, "keyword": 0.0, "semantic": 0.0},
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
                "start_line": line,
                "end_line": line,
                "snippet": snippet,
                "size_bytes": int(size_bytes),
                "mtime": float(mtime),
                "signals": {"path": 0.0, "keyword": _rank_to_score(float(rank)), "semantic": 0.0},
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


def _dedupe_chunk_matches(matches: list[dict[str, Any]], query: str, limit: int) -> list[dict[str, Any]]:
    best: dict[tuple[str, int | None], dict[str, Any]] = {}
    for match in matches:
        key = (str(match["path"]), match.get("start_line") or match.get("line"))
        current = best.get(key)
        if current is None or _match_priority(match, query) > _match_priority(current, query):
            best[key] = match
    return sorted(best.values(), key=lambda item: (-float(item["score"]), item["path"], item.get("line") or 0))[:limit]


def _attach_evidence(
    conn: sqlite3.Connection,
    matches: list[dict[str, Any]],
    *,
    query: str,
    principal: Principal,
    acl: tuple[str, ...],
    index_version: str,
) -> tuple[list[dict[str, Any]], EvidenceBundle]:
    paths = tuple(sorted({str(match.get("path", "")) for match in matches if match.get("path")}))
    metadata: dict[str, tuple[str, float, str]] = {}
    if paths:
        placeholders = ",".join("?" for _ in paths)
        rows = conn.execute(
            (
                "SELECT path, content_sha256, indexed_at, content "
                f"FROM file_index_files WHERE path IN ({placeholders})"
            ),
            paths,
        ).fetchall()
        metadata = {
            str(path): (
                str(content_sha256),
                float(indexed_at),
                str(content),
            )
            for path, content_sha256, indexed_at, content in rows
        }

    authorized_matches: list[dict[str, Any]] = []
    evidence_items: list[EvidenceItem] = []
    for match in matches:
        path = str(match.get("path", ""))
        file_metadata = metadata.get(path)
        if file_metadata is None:
            continue
        content_sha256, indexed_at, indexed_content = file_metadata
        if (
            len(content_sha256) != 64
            or any(character not in "0123456789abcdef" for character in content_sha256)
        ):
            raise PermissionError("file index content digest is invalid")
        if (
            hashlib.sha256(indexed_content.encode("utf-8")).hexdigest()
            != content_sha256
        ):
            raise PermissionError("file index content integrity check failed")
        path_sha256 = hashlib.sha256(path.encode("utf-8")).hexdigest()
        knowledge = KnowledgeItem(
            item_id=f"file-{path_sha256[:24]}-{content_sha256[:24]}",
            kind=KnowledgeKind.RETRIEVAL,
            namespace=("tenant", principal.tenant_id, "workspace"),
            content_sha256=content_sha256,
            source_refs=(f"workspace-path-sha256:{path_sha256}",),
            trust=TrustLevel.RETRIEVED,
            sensitivity=DataSensitivity.INTERNAL,
            acl=acl,
            created_at=indexed_at,
        )
        if not knowledge.is_authorized(principal):
            continue
        snippet = str(match.get("snippet", ""))
        snippet_sha256 = hashlib.sha256(snippet.encode("utf-8")).hexdigest()
        start_line = match.get("start_line") or match.get("line")
        end_line = match.get("end_line") or start_line
        evidence_key = (
            f"{knowledge.envelope_digest}\0{path}\0{start_line}\0{end_line}\0"
            f"{snippet_sha256}\0{index_version}"
        )
        evidence_id = f"ev-{hashlib.sha256(evidence_key.encode('utf-8')).hexdigest()[:32]}"
        evidence = EvidenceItem(
            evidence_id=evidence_id,
            knowledge=knowledge,
            path=path,
            start_line=int(start_line) if start_line is not None else None,
            end_line=int(end_line) if end_line is not None else None,
            snippet_sha256=snippet_sha256,
            index_version=index_version,
        )
        enriched = dict(match)
        enriched.update(
            {
                "evidence_id": evidence_id,
                "content_sha256": content_sha256,
                "snippet_sha256": snippet_sha256,
                "index_version": index_version,
            }
        )
        authorized_matches.append(enriched)
        evidence_items.append(evidence)
    bundle = EvidenceBundle.build(
        query=query,
        principal=principal,
        index_version=index_version,
        items=evidence_items,
    )
    return authorized_matches, bundle


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


def _resolve_embedding_config(config: dict[str, Any] | EmbeddingConfig | None) -> EmbeddingConfig | None:
    if isinstance(config, EmbeddingConfig):
        return config if config.enabled else None
    raw: dict[str, Any] = {}
    parent: dict[str, Any] = {}
    if isinstance(config, dict):
        parent = config
        nested = config.get("file_index_embedding")
        raw = dict(nested if isinstance(nested, dict) else config)

    enabled = _truthy(os.environ.get("XAGENT_FILE_INDEX_EMBEDDING", raw.get("enabled", False)))
    if not enabled:
        return None

    apikey = str(
        raw.get("apikey")
        or raw.get("api_key")
        or os.environ.get("XAGENT_EMBEDDING_API_KEY", "")
        or parent.get("apikey", "")
    )
    apibase = str(
        raw.get("apibase")
        or raw.get("base_url")
        or os.environ.get("XAGENT_EMBEDDING_BASE_URL", "")
    )
    model = str(raw.get("model") or os.environ.get("XAGENT_EMBEDDING_MODEL", ""))
    dimension_value = raw.get("dimension") or os.environ.get("XAGENT_EMBEDDING_DIMENSION", 0)
    try:
        dimension = int(dimension_value)
    except (TypeError, ValueError):
        dimension = 0
    try:
        batch_size = max(1, min(int(raw.get("batch_size", 32)), 128))
    except (TypeError, ValueError):
        batch_size = 32
    try:
        timeout = max(1, int(raw.get("timeout", 60)))
    except (TypeError, ValueError):
        timeout = 60

    if not apibase or not model or dimension <= 0:
        return EmbeddingConfig(
            enabled=False,
            apikey=apikey,
            apibase=_normalize_embedding_apibase(apibase),
            model=model,
            dimension=dimension,
            batch_size=batch_size,
            timeout=timeout,
        )
    return EmbeddingConfig(
        enabled=True,
        apikey=apikey,
        apibase=_normalize_embedding_apibase(apibase),
        model=model,
        dimension=dimension,
        batch_size=batch_size,
        timeout=timeout,
    )


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _normalize_embedding_apibase(apibase: str) -> str:
    apibase = str(apibase or "").strip()
    if not apibase:
        return ""
    if not apibase.startswith(("http://", "https://")):
        apibase = f"https://{apibase}"
    apibase = apibase.rstrip("/")
    if apibase.endswith("/chat/completions"):
        return apibase[: -len("/chat/completions")] + "/embeddings"
    if not apibase.endswith("/embeddings"):
        if apibase.endswith("/v1"):
            return apibase + "/embeddings"
        return apibase + "/v1/embeddings"
    return apibase


def _semantic_state(
    semantic: bool,
    config: EmbeddingConfig | None,
    provider: EmbeddingProvider | None,
) -> dict[str, Any]:
    if not semantic:
        return {"enabled": False, "reason": "semantic indexing disabled"}
    if config is None:
        return {"enabled": False, "reason": "embedding configuration disabled"}
    if not config.enabled:
        return {
            "enabled": False,
            "reason": "embedding configuration incomplete",
            "model": config.model,
            "dimension": config.dimension,
        }
    if provider is None and (not config.apibase or not config.model):
        return {"enabled": False, "reason": "embedding provider unavailable"}
    return {"enabled": True, "reason": "semantic indexing enabled", "model": config.model, "dimension": config.dimension}


def _ensure_semantic_schema(conn: sqlite3.Connection, config: EmbeddingConfig) -> dict[str, Any]:
    try:
        import sqlite_vec  # type: ignore[import-not-found]

        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
        conn.execute(
            f"""
            CREATE VIRTUAL TABLE IF NOT EXISTS file_index_vec
            USING vec0(embedding float[{config.dimension}] distance_metric=cosine)
            """
        )
        return {"status": "OK", "enabled": True, "vec": True, "model": config.model, "dimension": config.dimension}
    except Exception as exc:  # sqlite-vec is optional; manual blob scan remains available.
        try:
            conn.enable_load_extension(False)
        except Exception:
            pass
        return {
            "status": "UNAVAILABLE",
            "enabled": True,
            "vec": False,
            "reason": str(exc),
            "model": config.model,
            "dimension": config.dimension,
        }


def _reset_semantic_if_config_changed(conn: sqlite3.Connection, config: EmbeddingConfig) -> None:
    old = _get_meta(conn, "semantic_fingerprint")
    if old in {None, config.fingerprint}:
        _set_meta(conn, "semantic_fingerprint", config.fingerprint)
        return
    conn.execute("DELETE FROM file_index_chunk_embeddings")
    conn.execute("UPDATE file_index_chunks SET embedded_at = NULL, embedding_model = NULL")
    conn.execute("DROP TABLE IF EXISTS file_index_vec")
    _set_meta(conn, "semantic_fingerprint", config.fingerprint)


def _get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM file_index_meta WHERE key = ?", (key,)).fetchone()
    return str(row[0]) if row else None


def _set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("INSERT OR REPLACE INTO file_index_meta(key, value) VALUES (?, ?)", (key, value))


def _file_semantic_complete(conn: sqlite3.Connection, path: str, config: EmbeddingConfig) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM file_index_chunks AS c
        LEFT JOIN file_index_chunk_embeddings AS e
          ON e.chunk_id = c.id AND e.model = ? AND e.dimension = ?
        WHERE c.path = ? AND e.chunk_id IS NULL
        """,
        (config.model, config.dimension, path),
    ).fetchone()
    chunk_count = conn.execute("SELECT COUNT(*) FROM file_index_chunks WHERE path = ?", (path,)).fetchone()
    return bool(chunk_count and int(chunk_count[0]) > 0 and row and int(row[0]) == 0)


def _refresh_file_semantics(
    conn: sqlite3.Connection,
    file_id: int,
    path: str,
    content: str,
    config: EmbeddingConfig,
    provider: EmbeddingProvider | None,
    semantic_state: dict[str, Any],
) -> bool:
    chunks = _chunk_file(path, content)
    if not chunks:
        return False
    reusable = _load_reusable_embeddings(conn, path, config)
    _delete_chunks(conn, path)
    chunk_ids: list[int] = []
    texts: list[str] = []
    hashes: list[str] = []
    for index, chunk in enumerate(chunks):
        content_hash = _content_hash(chunk["content"])
        cur = conn.execute(
            """
            INSERT INTO file_index_chunks(
                file_id, path, chunk_index, start_line, end_line, content, content_hash, token_estimate
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                file_id,
                path,
                index,
                int(chunk["start_line"]),
                int(chunk["end_line"]),
                chunk["content"],
                content_hash,
                max(1, len(chunk["content"]) // 4),
            ),
        )
        chunk_ids.append(int(cur.lastrowid))
        texts.append(str(chunk["content"]))
        hashes.append(content_hash)

    vectors_by_hash: dict[str, list[float]] = dict(reusable)
    missing_hashes: list[str] = []
    missing_texts: list[str] = []
    for content_hash, text in zip(hashes, texts):
        if content_hash not in vectors_by_hash:
            missing_hashes.append(content_hash)
            missing_texts.append(text)
    try:
        new_vectors = _embed_texts(missing_texts, config, provider) if missing_texts else []
    except Exception as exc:
        _delete_chunks(conn, path)
        semantic_state["status"] = {
            "status": "ERROR",
            "enabled": True,
            "reason": f"embedding failed: {exc}",
            "model": config.model,
            "dimension": config.dimension,
        }
        return False
    if len(new_vectors) != len(missing_texts):
        _delete_chunks(conn, path)
        semantic_state["status"] = {
            "status": "ERROR",
            "enabled": True,
            "reason": f"embedding count mismatch: expected {len(missing_texts)}, got {len(new_vectors)}",
            "model": config.model,
            "dimension": config.dimension,
        }
        return False
    for content_hash, vector in zip(missing_hashes, new_vectors):
        vectors_by_hash[content_hash] = vector
    now = time.time()
    for chunk_id, content_hash in zip(chunk_ids, hashes):
        vector = vectors_by_hash[content_hash]
        if len(vector) != config.dimension:
            _delete_chunks(conn, path)
            semantic_state["status"] = {
                "status": "ERROR",
                "enabled": True,
                "reason": f"embedding dimension mismatch: expected {config.dimension}, got {len(vector)}",
                "model": config.model,
                "dimension": config.dimension,
            }
            return False
        blob = _pack_vector(vector)
        conn.execute(
            """
            INSERT OR REPLACE INTO file_index_chunk_embeddings(
                chunk_id, model, dimension, embedding, content_hash, embedded_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (chunk_id, config.model, config.dimension, blob, content_hash, now),
        )
        conn.execute(
            "UPDATE file_index_chunks SET embedded_at = ?, embedding_model = ? WHERE id = ?",
            (now, config.model, chunk_id),
        )
        if not _insert_vec_row(conn, chunk_id, blob):
            status = semantic_state.get("status")
            if isinstance(status, dict) and status.get("vec"):
                status["vec_write_errors"] = int(status.get("vec_write_errors", 0)) + 1
    return True


def _embed_texts(
    texts: list[str],
    config: EmbeddingConfig,
    provider: EmbeddingProvider | None,
) -> list[list[float]]:
    if provider is not None:
        return provider.embed_texts(texts)
    client = OpenAICompatibleEmbeddingProvider(config)
    vectors: list[list[float]] = []
    for start in range(0, len(texts), config.batch_size):
        vectors.extend(client.embed_texts(texts[start : start + config.batch_size]))
    return vectors


def _load_reusable_embeddings(
    conn: sqlite3.Connection,
    path: str,
    config: EmbeddingConfig,
) -> dict[str, list[float]]:
    rows = conn.execute(
        """
        SELECT e.content_hash, e.embedding
        FROM file_index_chunks AS c
        JOIN file_index_chunk_embeddings AS e
          ON e.chunk_id = c.id AND e.model = ? AND e.dimension = ?
        WHERE c.path = ?
        """,
        (config.model, config.dimension, path),
    ).fetchall()
    reusable: dict[str, list[float]] = {}
    for content_hash, embedding in rows:
        reusable[str(content_hash)] = _unpack_vector(bytes(embedding))
    return reusable


class OpenAICompatibleEmbeddingProvider:
    def __init__(self, config: EmbeddingConfig) -> None:
        self.config = config

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        payload = json.dumps({"model": self.config.model, "input": texts}).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.config.apikey:
            headers["Authorization"] = f"Bearer {self.config.apikey}"
        req = urlrequest.Request(self.config.apibase, data=payload, headers=headers, method="POST")
        try:
            with urlrequest.urlopen(req, timeout=self.config.timeout) as resp:
                raw = resp.read().decode("utf-8")
        except urlerror.URLError as exc:
            raise RuntimeError(str(exc)) from exc
        data = json.loads(raw)
        items = data.get("data", [])
        if not isinstance(items, list):
            raise RuntimeError("embedding response missing data list")
        vectors: list[list[float]] = []
        for item in sorted(items, key=lambda value: int(value.get("index", len(vectors)))):
            embedding = item.get("embedding") if isinstance(item, dict) else None
            if not isinstance(embedding, list):
                raise RuntimeError("embedding response item missing embedding")
            vectors.append([float(value) for value in embedding])
        if len(vectors) != len(texts):
            raise RuntimeError(f"embedding response count mismatch: expected {len(texts)}, got {len(vectors)}")
        return vectors


def _chunk_file(path: str, content: str) -> list[dict[str, Any]]:
    suffix = Path(path).suffix.lower()
    if suffix in {".md", ".markdown"}:
        return _chunk_markdown(content)
    if suffix in {".py", ".js", ".jsx", ".ts", ".tsx"}:
        return _chunk_code(content)
    return _chunk_text(content)


def _chunk_markdown(content: str) -> list[dict[str, Any]]:
    lines = content.splitlines()
    if not lines:
        return []
    starts = [idx for idx, line in enumerate(lines) if line.lstrip().startswith("#")]
    if not starts:
        return _chunk_text(content)
    if starts[0] != 0:
        starts.insert(0, 0)
    chunks: list[dict[str, Any]] = []
    for pos, start in enumerate(starts):
        end = starts[pos + 1] - 1 if pos + 1 < len(starts) else len(lines) - 1
        chunks.extend(_split_line_range(lines, start, end))
    return chunks


def _chunk_code(content: str) -> list[dict[str, Any]]:
    lines = content.splitlines()
    if not lines:
        return []
    pattern = re.compile(r"^\s*(class\s+\w+|def\s+\w+|async\s+def\s+\w+|function\s+\w+|export\s+function\s+\w+|const\s+\w+\s*=)")
    starts = [idx for idx, line in enumerate(lines) if pattern.search(line)]
    if not starts:
        return _chunk_by_lines(lines)
    if starts[0] != 0:
        starts.insert(0, 0)
    chunks: list[dict[str, Any]] = []
    for pos, start in enumerate(starts):
        end = starts[pos + 1] - 1 if pos + 1 < len(starts) else len(lines) - 1
        chunks.extend(_split_line_range(lines, start, end))
    return chunks


def _chunk_text(content: str) -> list[dict[str, Any]]:
    if any(len(line) > DEFAULT_CHUNK_TARGET_CHARS for line in content.splitlines()):
        return _chunk_by_chars(content)
    lines = content.splitlines()
    if not lines:
        return []
    return _chunk_by_lines(lines)


def _chunk_by_lines(lines: list[str]) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    start = 0
    while start < len(lines):
        end = min(len(lines) - 1, start + DEFAULT_LINE_WINDOW - 1)
        chunks.append(_make_chunk(lines, start, end))
        if end == len(lines) - 1:
            break
        start = max(end + 1 - 12, start + 1)
    return chunks


def _split_line_range(lines: list[str], start: int, end: int) -> list[dict[str, Any]]:
    text = "\n".join(lines[start : end + 1])
    if len(text) <= DEFAULT_CHUNK_TARGET_CHARS:
        return [_make_chunk(lines, start, end)]
    chunks: list[dict[str, Any]] = []
    cursor = start
    while cursor <= end:
        collected: list[str] = []
        line_end = cursor
        for idx in range(cursor, end + 1):
            candidate = "\n".join(collected + [lines[idx]])
            if collected and len(candidate) > DEFAULT_CHUNK_TARGET_CHARS:
                break
            collected.append(lines[idx])
            line_end = idx
        chunks.append(_make_chunk(lines, cursor, line_end))
        if line_end >= end:
            break
        overlap_start = max(cursor + 1, line_end - max(1, DEFAULT_CHUNK_OVERLAP_CHARS // 80))
        cursor = overlap_start
    return chunks


def _make_chunk(lines: list[str], start: int, end: int) -> dict[str, Any]:
    return {
        "start_line": start + 1,
        "end_line": end + 1,
        "content": "\n".join(lines[start : end + 1]).strip(),
    }


def _chunk_by_chars(content: str) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    start = 0
    length = len(content)
    while start < length:
        end = min(length, start + DEFAULT_CHUNK_TARGET_CHARS)
        chunk_text = content[start:end].strip()
        if chunk_text:
            chunks.append(
                {
                    "start_line": content.count("\n", 0, start) + 1,
                    "end_line": content.count("\n", 0, end) + 1,
                    "content": chunk_text,
                }
            )
        if end >= length:
            break
        start = max(start + 1, end - DEFAULT_CHUNK_OVERLAP_CHARS)
    return chunks


def _content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _pack_vector(vector: list[float]) -> bytes:
    return struct.pack(f"<{len(vector)}f", *[float(value) for value in vector])


def _unpack_vector(blob: bytes) -> list[float]:
    if len(blob) % 4 != 0:
        return []
    return list(struct.unpack(f"<{len(blob) // 4}f", blob))


def _insert_vec_row(conn: sqlite3.Connection, chunk_id: int, embedding: bytes) -> bool:
    try:
        conn.execute("DELETE FROM file_index_vec WHERE rowid = ?", (chunk_id,))
        conn.execute("INSERT INTO file_index_vec(rowid, embedding) VALUES (?, ?)", (chunk_id, embedding))
    except sqlite3.Error:
        return False
    return True


def _delete_vec_row(conn: sqlite3.Connection, chunk_id: int) -> None:
    try:
        conn.execute("DELETE FROM file_index_vec WHERE rowid = ?", (chunk_id,))
    except sqlite3.Error:
        return


def _semantic_matches(
    conn: sqlite3.Connection,
    query: str,
    root: str,
    limit: int,
    config: EmbeddingConfig | None,
    provider: EmbeddingProvider | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    state = _semantic_state(True, config, provider)
    if not state["enabled"] or config is None:
        return [], state
    vec_status = _ensure_semantic_schema(conn, config)
    embedding_count = _semantic_embedding_count(conn, root, config)
    if embedding_count == 0:
        return [], {
            "enabled": True,
            "status": "EMPTY",
            "reason": "semantic index has no embeddings for this root",
            "vec": vec_status.get("vec", False),
            "model": config.model,
            "dimension": config.dimension,
        }
    try:
        query_vector = _embed_texts([query], config, provider)[0]
    except Exception as exc:
        return [], {
            "enabled": True,
            "status": "ERROR",
            "reason": f"query embedding failed: {exc}",
            "model": config.model,
            "dimension": config.dimension,
        }
    if len(query_vector) != config.dimension:
        return [], {
            "enabled": True,
            "status": "ERROR",
            "reason": f"query embedding dimension mismatch: expected {config.dimension}, got {len(query_vector)}",
            "model": config.model,
            "dimension": config.dimension,
        }
    matches: list[dict[str, Any]] = []
    if vec_status.get("vec"):
        matches = _semantic_matches_vec(conn, query_vector, root, limit, config)
    if len(matches) < limit:
        matches = _semantic_matches_manual(conn, query_vector, root, limit, config)
    return matches, {
        "enabled": True,
        "status": "OK",
        "vec": vec_status.get("vec", False),
        "model": config.model,
        "dimension": config.dimension,
    }


def _semantic_embedding_count(conn: sqlite3.Connection, root: str, config: EmbeddingConfig) -> int:
    root_sql, root_params = _root_sql_filter(root, "c.path")
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM file_index_chunks AS c
        JOIN file_index_chunk_embeddings AS e
          ON e.chunk_id = c.id AND e.model = ? AND e.dimension = ?
        WHERE {root_sql}
        """,
        (config.model, config.dimension, *root_params),
    ).fetchone()
    return int(row[0]) if row else 0


def _semantic_matches_vec(
    conn: sqlite3.Connection,
    query_vector: list[float],
    root: str,
    limit: int,
    config: EmbeddingConfig,
) -> list[dict[str, Any]]:
    root_sql, root_params = _root_sql_filter(root, "c.path")
    overfetch = limit * DEFAULT_SEMANTIC_OVERFETCH
    try:
        rows = conn.execute(
            f"""
            WITH knn AS (
                SELECT rowid AS chunk_id, distance
                FROM file_index_vec
                WHERE embedding MATCH ? AND k = ?
            )
            SELECT
                c.id, c.path, c.start_line, c.end_line, c.content,
                files.size_bytes, files.mtime, knn.distance
            FROM knn
            JOIN file_index_chunks AS c ON c.id = knn.chunk_id
            JOIN file_index_chunk_embeddings AS e
              ON e.chunk_id = c.id AND e.model = ? AND e.dimension = ?
            JOIN file_index_files AS files ON files.id = c.file_id
            WHERE {root_sql}
            ORDER BY knn.distance
            LIMIT ?
            """,
            (_pack_vector(query_vector), overfetch, config.model, config.dimension, *root_params, limit),
        ).fetchall()
    except sqlite3.Error:
        return []
    matches: list[dict[str, Any]] = []
    for _chunk_id, path, start_line, end_line, content, size_bytes, mtime, distance in rows:
        score = max(0.0, 1.0 - float(distance))
        matches.append(
            {
                "path": str(path),
                "score": score,
                "match_type": "semantic",
                "line": int(start_line),
                "start_line": int(start_line),
                "end_line": int(end_line),
                "snippet": _trim_snippet(str(content).splitlines()[0] if str(content).splitlines() else str(content)),
                "size_bytes": int(size_bytes),
                "mtime": float(mtime),
                "signals": {"path": 0.0, "keyword": 0.0, "semantic": score},
            }
        )
    return matches


def _semantic_matches_manual(
    conn: sqlite3.Connection,
    query_vector: list[float],
    root: str,
    limit: int,
    config: EmbeddingConfig,
) -> list[dict[str, Any]]:
    root_sql, root_params = _root_sql_filter(root, "c.path")
    rows = conn.execute(
        f"""
        SELECT
            c.id, c.path, c.start_line, c.end_line, c.content,
            files.size_bytes, files.mtime, e.embedding
        FROM file_index_chunks AS c
        JOIN file_index_chunk_embeddings AS e
          ON e.chunk_id = c.id AND e.model = ? AND e.dimension = ?
        JOIN file_index_files AS files ON files.id = c.file_id
        WHERE {root_sql}
        """,
        (config.model, config.dimension, *root_params),
    ).fetchall()
    ranked: list[tuple[float, dict[str, Any]]] = []
    for _chunk_id, path, start_line, end_line, content, size_bytes, mtime, embedding_blob in rows:
        vector = _unpack_vector(bytes(embedding_blob))
        if len(vector) != len(query_vector):
            continue
        distance = _cosine_distance(query_vector, vector)
        score = max(0.0, 1.0 - distance)
        ranked.append(
            (
                score,
                {
                    "path": str(path),
                    "score": score,
                    "match_type": "semantic",
                    "line": int(start_line),
                    "start_line": int(start_line),
                    "end_line": int(end_line),
                    "snippet": _trim_snippet(str(content).splitlines()[0] if str(content).splitlines() else str(content)),
                    "size_bytes": int(size_bytes),
                    "mtime": float(mtime),
                    "signals": {"path": 0.0, "keyword": 0.0, "semantic": score},
                },
            )
        )
    ranked.sort(key=lambda item: (-item[0], item[1]["path"], item[1]["line"]))
    return [item for _score, item in ranked[: limit * DEFAULT_SEMANTIC_OVERFETCH]]


def _cosine_distance(left: list[float], right: list[float]) -> float:
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm == 0.0 or right_norm == 0.0:
        return 1.0
    similarity = max(-1.0, min(1.0, dot / (left_norm * right_norm)))
    return 1.0 - similarity


def _hybrid_matches(
    keyword_matches: list[dict[str, Any]],
    semantic_matches: list[dict[str, Any]],
    query: str,
    limit: int,
) -> list[dict[str, Any]]:
    scores: dict[tuple[str, int | None], dict[str, Any]] = {}
    symbol_weight = 0.7 if _looks_like_symbol_query(query) else 0.55
    semantic_weight = 1.0 - symbol_weight
    for rank, match in enumerate(_dedupe_matches(keyword_matches, query, limit * DEFAULT_SEMANTIC_OVERFETCH), start=1):
        key = (str(match["path"]), match.get("start_line") or match.get("line"))
        item = dict(match)
        item["score"] = symbol_weight * _rrf(rank)
        item["match_type"] = "hybrid"
        signals = dict(item.get("signals") or {})
        signals["keyword"] = max(float(signals.get("keyword", 0.0)), float(match.get("score", 0.0)))
        item["signals"] = signals
        scores[key] = item
    for rank, match in enumerate(semantic_matches, start=1):
        key = (str(match["path"]), match.get("start_line") or match.get("line"))
        current = scores.get(key)
        semantic_score = semantic_weight * _rrf(rank)
        if current is None:
            item = dict(match)
            item["score"] = semantic_score
            item["match_type"] = "hybrid"
            scores[key] = item
            continue
        current["score"] = float(current.get("score", 0.0)) + semantic_score
        current["snippet"] = current.get("snippet") or match.get("snippet", "")
        signals = dict(current.get("signals") or {})
        signals["semantic"] = max(float(signals.get("semantic", 0.0)), float(match.get("score", 0.0)))
        current["signals"] = signals
    return sorted(scores.values(), key=lambda item: (-float(item["score"]), item["path"], item.get("line") or 0))[:limit]


def _rrf(rank: int, k: int = 60) -> float:
    return 1.0 / (k + rank)


def _looks_like_symbol_query(query: str) -> bool:
    return bool(SYMBOL_QUERY_PATTERN.fullmatch(query.strip()))
