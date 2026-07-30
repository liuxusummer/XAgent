"""Durable digest-only recovery authority for remote executions.

The journal proves which authorization binding the control plane actually
issued for one durable Attempt.  It intentionally stores no claim token,
bearer grant, response body, script bytes, Artifact content, environment
value, runtime proof, or raw execution plan.
"""

from __future__ import annotations

import fcntl
import math
import os
import re
import sqlite3
import stat
import threading
from contextlib import contextmanager
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Iterator

REMOTE_EXECUTION_JOURNAL_SCHEMA_VERSION = 1
DEFAULT_EXECUTION_JOURNAL_BUSY_TIMEOUT_MS = 10_000
MAX_EXECUTION_JOURNAL_RECORDS = 100_000

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_SECURITY_LEVELS = frozenset({"container"})
_SCHEMA_COLUMNS = {
    "remote_execution_journal_metadata": (
        ("singleton", "INTEGER", 0, 1),
        ("schema_version", "INTEGER", 1, 0),
    ),
    "remote_execution_bindings": (
        ("run_id", "TEXT", 1, 1),
        ("attempt_id", "TEXT", 1, 2),
        ("node_id", "TEXT", 1, 0),
        ("worker_id", "TEXT", 1, 0),
        ("tenant_id", "TEXT", 1, 0),
        ("identity_digest", "TEXT", 1, 0),
        ("session_binding_digest", "TEXT", 1, 0),
        ("claim_token_digest", "TEXT", 1, 0),
        ("fencing_token", "INTEGER", 1, 3),
        ("action_digest", "TEXT", 1, 0),
        ("authorization_digest", "TEXT", 1, 0),
        ("profile_digest", "TEXT", 1, 0),
        ("request_digest", "TEXT", 1, 0),
        ("grant_binding_digest", "TEXT", 1, 0),
        ("execution_plan_digest", "TEXT", 1, 0),
        ("runtime_attestation_digest", "TEXT", 1, 0),
        ("runtime_verifier_id", "TEXT", 1, 0),
        ("runtime_security_level", "TEXT", 1, 0),
        ("created_at", "REAL", 1, 0),
    ),
}
_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE remote_execution_journal_metadata(
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        schema_version INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE remote_execution_bindings(
        run_id TEXT NOT NULL,
        attempt_id TEXT NOT NULL,
        node_id TEXT NOT NULL,
        worker_id TEXT NOT NULL,
        tenant_id TEXT NOT NULL,
        identity_digest TEXT NOT NULL,
        session_binding_digest TEXT NOT NULL,
        claim_token_digest TEXT NOT NULL,
        fencing_token INTEGER NOT NULL CHECK(fencing_token >= 1),
        action_digest TEXT NOT NULL,
        authorization_digest TEXT NOT NULL,
        profile_digest TEXT NOT NULL,
        request_digest TEXT NOT NULL,
        grant_binding_digest TEXT NOT NULL,
        execution_plan_digest TEXT NOT NULL,
        runtime_attestation_digest TEXT NOT NULL,
        runtime_verifier_id TEXT NOT NULL,
        runtime_security_level TEXT NOT NULL
            CHECK(runtime_security_level = 'container'),
        created_at REAL NOT NULL,
        PRIMARY KEY(run_id, attempt_id, fencing_token)
    )
    """,
)


class RemoteExecutionJournalError(RuntimeError):
    """Base fail-closed recovery journal error."""


class RemoteExecutionJournalCapacityError(RemoteExecutionJournalError):
    """The hard live-record bound was reached."""


class RemoteExecutionJournalConflict(RemoteExecutionJournalError):
    """An Attempt was rebound to different execution authority."""


@dataclass(frozen=True, slots=True)
class RemoteExecutionBindingRecord:
    """Non-secret immutable proof of one published remote assignment."""

    run_id: str
    attempt_id: str
    node_id: str
    worker_id: str
    tenant_id: str
    identity_digest: str
    session_binding_digest: str
    claim_token_digest: str
    fencing_token: int
    action_digest: str
    authorization_digest: str
    profile_digest: str
    request_digest: str
    grant_binding_digest: str
    execution_plan_digest: str
    runtime_attestation_digest: str
    runtime_verifier_id: str
    runtime_security_level: str
    created_at: float

    def __post_init__(self) -> None:
        for name in (
            "run_id",
            "attempt_id",
            "node_id",
            "worker_id",
            "tenant_id",
            "runtime_verifier_id",
        ):
            _identifier(getattr(self, name), name)
        for name in (
            "identity_digest",
            "session_binding_digest",
            "claim_token_digest",
            "action_digest",
            "authorization_digest",
            "profile_digest",
            "request_digest",
            "grant_binding_digest",
            "execution_plan_digest",
            "runtime_attestation_digest",
        ):
            _digest(getattr(self, name), name)
        if (
            isinstance(self.fencing_token, bool)
            or not isinstance(self.fencing_token, int)
            or not 1 <= self.fencing_token <= 2**63 - 1
        ):
            raise RemoteExecutionJournalError("invalid_fencing_token")
        if self.runtime_security_level not in _SECURITY_LEVELS:
            raise RemoteExecutionJournalError(
                "invalid_runtime_security_level"
            )
        _finite_time(self.created_at)

    @property
    def immutable_values(self) -> tuple[object, ...]:
        return tuple(
            getattr(self, item.name)
            for item in fields(self)
            if item.name != "created_at"
        )


class RemoteExecutionJournal:
    """SQLite-backed exact authorization binding registry.

    Rows are immutable.  Re-recording the same binding is idempotent; changing
    any authority field for the same Run/Attempt is a conflict.  Live rows are
    never evicted to make room because doing so would erase recovery evidence.
    """

    def __init__(
        self,
        path: str | Path = ":memory:",
        *,
        maximum_records: int = MAX_EXECUTION_JOURNAL_RECORDS,
        busy_timeout_ms: int = DEFAULT_EXECUTION_JOURNAL_BUSY_TIMEOUT_MS,
    ) -> None:
        self.path = str(path)
        if not self.path:
            raise ValueError("execution journal path must not be empty")
        self.maximum_records = _bounded_int(
            maximum_records,
            "maximum_records",
            1,
            MAX_EXECUTION_JOURNAL_RECORDS,
        )
        self.busy_timeout_ms = _bounded_int(
            busy_timeout_ms,
            "busy_timeout_ms",
            1,
            120_000,
        )
        self._memory_lock = threading.RLock()
        self._memory_connection: sqlite3.Connection | None = None
        if self.path == ":memory:":
            self._bootstrap_new_database = True
            self._memory_connection = self._new_connection()
        else:
            journal_path = Path(self.path)
            journal_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                self._prepare_durable_database(journal_path)
            except RemoteExecutionJournalError:
                raise
            except OSError as exc:
                raise RemoteExecutionJournalError(
                    "execution_journal_unavailable"
                ) from exc
            self._bootstrap_new_database = False
        self._initialize()

    @property
    def durable(self) -> bool:
        return self.path != ":memory:"

    def close(self) -> None:
        with self._memory_lock:
            connection = self._memory_connection
            self._memory_connection = None
            if connection is not None:
                connection.close()

    def record(
        self,
        binding: RemoteExecutionBindingRecord,
    ) -> RemoteExecutionBindingRecord:
        if not isinstance(binding, RemoteExecutionBindingRecord):
            raise TypeError(
                "binding must be a RemoteExecutionBindingRecord"
            )
        with self._transaction() as connection:
            existing = self._get_locked(
                connection,
                binding.run_id,
                binding.attempt_id,
                binding.fencing_token,
            )
            if existing is not None:
                if existing.immutable_values != binding.immutable_values:
                    raise RemoteExecutionJournalConflict(
                        "execution_binding_conflict"
                    )
                return existing
            count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM remote_execution_bindings"
                ).fetchone()[0]
            )
            if count >= self.maximum_records:
                raise RemoteExecutionJournalCapacityError(
                    "execution_binding_capacity"
                )
            connection.execute(
                """
                INSERT INTO remote_execution_bindings(
                    run_id, attempt_id, node_id, worker_id, tenant_id,
                    identity_digest, session_binding_digest,
                    claim_token_digest, fencing_token, action_digest,
                    authorization_digest, profile_digest, request_digest,
                    grant_binding_digest, execution_plan_digest,
                    runtime_attestation_digest, runtime_verifier_id,
                    runtime_security_level, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(getattr(binding, item.name) for item in fields(binding)),
            )
            return binding

    def get(
        self,
        run_id: str,
        attempt_id: str,
        fencing_token: int,
    ) -> RemoteExecutionBindingRecord | None:
        _identifier(run_id, "run_id")
        _identifier(attempt_id, "attempt_id")
        bounded_fencing = _bounded_int(
            fencing_token,
            "fencing_token",
            1,
            2**63 - 1,
        )
        try:
            with self._connection() as connection:
                return self._get_locked(
                    connection,
                    run_id,
                    attempt_id,
                    bounded_fencing,
                )
        except sqlite3.Error as exc:
            raise RemoteExecutionJournalError(
                "execution_journal_unavailable"
            ) from exc

    def list_run(
        self,
        run_id: str,
        *,
        limit: int | None = None,
    ) -> tuple[RemoteExecutionBindingRecord, ...]:
        _identifier(run_id, "run_id")
        bounded_limit = _bounded_int(
            self.maximum_records if limit is None else limit,
            "limit",
            1,
            self.maximum_records,
        )
        try:
            with self._connection() as connection:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM remote_execution_bindings
                    WHERE run_id = ?
                    ORDER BY attempt_id, fencing_token
                    LIMIT ?
                    """,
                    (run_id, bounded_limit),
                ).fetchall()
        except sqlite3.Error as exc:
            raise RemoteExecutionJournalError(
                "execution_journal_unavailable"
            ) from exc
        return tuple(_record_from_row(row) for row in rows)

    def list_records(
        self,
        *,
        limit: int | None = None,
    ) -> tuple[RemoteExecutionBindingRecord, ...]:
        bounded_limit = _bounded_int(
            self.maximum_records if limit is None else limit,
            "limit",
            1,
            self.maximum_records,
        )
        try:
            with self._connection() as connection:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM remote_execution_bindings
                    ORDER BY run_id, attempt_id, fencing_token
                    LIMIT ?
                    """,
                    (bounded_limit,),
                ).fetchall()
        except sqlite3.Error as exc:
            raise RemoteExecutionJournalError(
                "execution_journal_unavailable"
            ) from exc
        return tuple(_record_from_row(row) for row in rows)

    def discard(
        self,
        run_id: str,
        attempt_id: str,
        fencing_token: int,
    ) -> bool:
        _identifier(run_id, "run_id")
        _identifier(attempt_id, "attempt_id")
        bounded_fencing = _bounded_int(
            fencing_token,
            "fencing_token",
            1,
            2**63 - 1,
        )
        with self._transaction() as connection:
            cursor = connection.execute(
                """
                DELETE FROM remote_execution_bindings
                WHERE run_id = ? AND attempt_id = ? AND fencing_token = ?
                """,
                (run_id, attempt_id, bounded_fencing),
            )
            return cursor.rowcount == 1

    @staticmethod
    def _get_locked(
        connection: sqlite3.Connection,
        run_id: str,
        attempt_id: str,
        fencing_token: int,
    ) -> RemoteExecutionBindingRecord | None:
        row = connection.execute(
            """
            SELECT *
            FROM remote_execution_bindings
            WHERE run_id = ? AND attempt_id = ? AND fencing_token = ?
            """,
            (run_id, attempt_id, fencing_token),
        ).fetchone()
        return None if row is None else _record_from_row(row)

    def _initialize(self) -> None:
        with self._transaction() as connection:
            expected_tables = frozenset(_SCHEMA_COLUMNS)
            existing_tables = frozenset(
                str(row["name"])
                for row in connection.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                    """
                )
            )
            if existing_tables and existing_tables != expected_tables:
                raise RemoteExecutionJournalError("invalid_execution_schema")
            is_new = not existing_tables and self._bootstrap_new_database
            if not existing_tables and not is_new:
                raise RemoteExecutionJournalError("invalid_execution_schema")
            if is_new:
                for statement in _SCHEMA_STATEMENTS:
                    connection.execute(statement)
            else:
                self._validate_schema(connection)
            row = connection.execute(
                """
                SELECT schema_version
                FROM remote_execution_journal_metadata
                WHERE singleton = 1
                """
            ).fetchone()
            if row is None:
                if not is_new:
                    raise RemoteExecutionJournalError(
                        "invalid_execution_schema"
                    )
                connection.execute(
                    """
                    INSERT INTO remote_execution_journal_metadata(
                        singleton, schema_version
                    ) VALUES (1, ?)
                    """,
                    (REMOTE_EXECUTION_JOURNAL_SCHEMA_VERSION,),
                )
            elif (
                int(row["schema_version"])
                != REMOTE_EXECUTION_JOURNAL_SCHEMA_VERSION
            ):
                raise RemoteExecutionJournalError(
                    "unsupported_execution_schema"
                )
            if is_new:
                self._validate_schema(connection)

    def _prepare_durable_database(self, journal_path: Path) -> None:
        lock_path = (
            journal_path.parent
            / f".{journal_path.name}.bootstrap.lock"
        )
        bootstrap_path = (
            journal_path.parent
            / f".{journal_path.name}.bootstrap.sqlite3"
        )
        lock_flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_CLOEXEC"):
            lock_flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            lock_flags |= os.O_NOFOLLOW
        try:
            lock_fd = os.open(lock_path, lock_flags, 0o600)
        except OSError as exc:
            raise RemoteExecutionJournalError(
                "execution_journal_unavailable"
            ) from exc
        connection: sqlite3.Connection | None = None
        try:
            lock_stat = os.fstat(lock_fd)
            if (
                not stat.S_ISREG(lock_stat.st_mode)
                or stat.S_IMODE(lock_stat.st_mode) & 0o077
            ):
                raise RemoteExecutionJournalError(
                    "execution_journal_permissions"
                )
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            marker = self._read_bootstrap_marker(lock_fd)
            if journal_path.is_symlink():
                raise RemoteExecutionJournalError(
                    "invalid_execution_schema"
                )
            if journal_path.exists():
                self._validate_durable_database(journal_path)
                self._write_bootstrap_marker(lock_fd, b"initialized\n")
                self._cleanup_bootstrap_files(bootstrap_path)
                return
            if marker == b"initialized\n":
                raise RemoteExecutionJournalError(
                    "invalid_execution_schema"
                )
            self._write_bootstrap_marker(lock_fd, b"bootstrapping\n")
            self._cleanup_bootstrap_files(bootstrap_path)
            connection = sqlite3.connect(
                bootstrap_path,
                timeout=self.busy_timeout_ms / 1_000,
                isolation_level=None,
                check_same_thread=False,
            )
            os.chmod(bootstrap_path, 0o600)
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA journal_mode = DELETE")
            connection.execute("BEGIN IMMEDIATE")
            try:
                for statement in _SCHEMA_STATEMENTS:
                    connection.execute(statement)
                connection.execute(
                    """
                    INSERT INTO remote_execution_journal_metadata(
                        singleton, schema_version
                    ) VALUES (1, ?)
                    """,
                    (REMOTE_EXECUTION_JOURNAL_SCHEMA_VERSION,),
                )
                self._validate_schema(connection)
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()
            mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()
            if mode is None or str(mode[0]).lower() != "wal":
                raise RemoteExecutionJournalError(
                    "execution_journal_unavailable"
                )
            connection.close()
            connection = None
            with bootstrap_path.open("rb") as stream:
                os.fsync(stream.fileno())
            os.link(bootstrap_path, journal_path)
            self._write_bootstrap_marker(lock_fd, b"initialized\n")
            try:
                directory_fd = os.open(journal_path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            except OSError:
                pass
        except sqlite3.Error as exc:
            raise RemoteExecutionJournalError(
                "execution_journal_unavailable"
            ) from exc
        finally:
            if connection is not None:
                connection.close()
            self._cleanup_bootstrap_files(bootstrap_path)
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    def _validate_durable_database(self, journal_path: Path) -> None:
        try:
            journal_stat = journal_path.lstat()
        except OSError as exc:
            raise RemoteExecutionJournalError(
                "execution_journal_unavailable"
            ) from exc
        if (
            not stat.S_ISREG(journal_stat.st_mode)
            or stat.S_IMODE(journal_stat.st_mode) & 0o077
        ):
            raise RemoteExecutionJournalError(
                "execution_journal_permissions"
            )
        connection = sqlite3.connect(
            journal_path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            connection.execute("PRAGMA synchronous = FULL")
            self._validate_schema(connection)
            row = connection.execute(
                """
                SELECT schema_version
                FROM remote_execution_journal_metadata
                WHERE singleton = 1
                """
            ).fetchone()
            if row is None:
                raise RemoteExecutionJournalError(
                    "invalid_execution_schema"
                )
            if (
                int(row["schema_version"])
                != REMOTE_EXECUTION_JOURNAL_SCHEMA_VERSION
            ):
                raise RemoteExecutionJournalError(
                    "unsupported_execution_schema"
                )
            mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()
            if mode is None or str(mode[0]).lower() != "wal":
                raise RemoteExecutionJournalError(
                    "execution_journal_unavailable"
                )
        finally:
            connection.close()

    @staticmethod
    def _validate_schema(connection: sqlite3.Connection) -> None:
        quick_check = connection.execute("PRAGMA quick_check(1)").fetchone()
        if quick_check is None or str(quick_check[0]).lower() != "ok":
            raise RemoteExecutionJournalError(
                "execution_journal_integrity_failed"
            )
        for table, expected_columns in _SCHEMA_COLUMNS.items():
            actual_columns = tuple(
                (
                    str(row["name"]),
                    str(row["type"]).upper(),
                    int(row["notnull"]),
                    int(row["pk"]),
                )
                for row in connection.execute(
                    f"PRAGMA table_info({table})"
                )
            )
            if actual_columns != expected_columns:
                raise RemoteExecutionJournalError(
                    "invalid_execution_schema"
                )

    def _new_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA synchronous = FULL")
        if self.path != ":memory:":
            mode = connection.execute("PRAGMA journal_mode").fetchone()
            if mode is None or str(mode[0]).lower() != "wal":
                connection.close()
                raise RemoteExecutionJournalError(
                    "execution_journal_unavailable"
                )
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        if self.path == ":memory:":
            with self._memory_lock:
                connection = self._memory_connection
                if connection is None:
                    raise RemoteExecutionJournalError(
                        "execution_journal_closed"
                    )
                yield connection
            return
        connection = self._new_connection()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        try:
            with self._connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    yield connection
                except BaseException:
                    connection.rollback()
                    raise
                else:
                    connection.commit()
        except sqlite3.Error as exc:
            raise RemoteExecutionJournalError(
                "execution_journal_unavailable"
            ) from exc

    @staticmethod
    def _read_bootstrap_marker(lock_fd: int) -> bytes:
        os.lseek(lock_fd, 0, os.SEEK_SET)
        return os.read(lock_fd, 64)

    @staticmethod
    def _write_bootstrap_marker(lock_fd: int, marker: bytes) -> None:
        os.lseek(lock_fd, 0, os.SEEK_SET)
        os.ftruncate(lock_fd, 0)
        os.write(lock_fd, marker)
        os.fsync(lock_fd)

    @staticmethod
    def _cleanup_bootstrap_files(bootstrap_path: Path) -> None:
        for suffix in ("", "-journal", "-wal", "-shm"):
            candidate = Path(f"{bootstrap_path}{suffix}")
            try:
                candidate.unlink()
            except FileNotFoundError:
                pass


def _record_from_row(row: sqlite3.Row) -> RemoteExecutionBindingRecord:
    try:
        return RemoteExecutionBindingRecord(
            **{
                item.name: row[item.name]
                for item in fields(RemoteExecutionBindingRecord)
            }
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RemoteExecutionJournalError(
            "invalid_execution_record"
        ) from exc


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise RemoteExecutionJournalError(f"invalid_{name}")
    return value


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise RemoteExecutionJournalError(f"invalid_{name}")
    return value


def _finite_time(value: object) -> float:
    try:
        timestamp = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RemoteExecutionJournalError("invalid_created_at") from exc
    if timestamp < 0 or not math.isfinite(timestamp):
        raise RemoteExecutionJournalError("invalid_created_at")
    return timestamp


def _bounded_int(
    value: int,
    name: str,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


__all__ = [
    "MAX_EXECUTION_JOURNAL_RECORDS",
    "REMOTE_EXECUTION_JOURNAL_SCHEMA_VERSION",
    "RemoteExecutionBindingRecord",
    "RemoteExecutionJournal",
    "RemoteExecutionJournalCapacityError",
    "RemoteExecutionJournalConflict",
    "RemoteExecutionJournalError",
]
