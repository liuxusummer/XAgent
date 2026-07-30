"""Durable, bounded control-plane identity journal for remote workers.

This journal is deliberately separate from the orchestration Domain Store:
session epochs and protocol request identities are control-plane admission
facts, not Run events.  It stores no response bodies, bearer grants, runtime
proofs, claim tokens, or telemetry labels.
"""

from __future__ import annotations

import fcntl
import math
import os
import re
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

REMOTE_JOURNAL_SCHEMA_VERSION = 1
DEFAULT_JOURNAL_BUSY_TIMEOUT_MS = 10_000
MAX_JOURNAL_WORKERS = 65_536
MAX_JOURNAL_INSTANCES_PER_WORKER = 1_024
MAX_JOURNAL_REQUESTS_PER_SESSION = 65_536
_SCHEMA_COLUMNS = {
    "remote_journal_metadata": (
        ("singleton", "INTEGER", 0, 1),
        ("schema_version", "INTEGER", 1, 0),
    ),
    "remote_session_heads": (
        ("worker_id", "TEXT", 0, 1),
        ("tenant_id", "TEXT", 1, 0),
        ("identity_digest", "TEXT", 1, 0),
        ("instance_id", "TEXT", 1, 0),
        ("epoch", "INTEGER", 1, 0),
        ("updated_at", "REAL", 1, 0),
    ),
    "remote_session_instances": (
        ("worker_id", "TEXT", 1, 1),
        ("instance_id", "TEXT", 1, 2),
        ("epoch", "INTEGER", 1, 0),
        ("retired", "INTEGER", 1, 0),
        ("created_at", "REAL", 1, 0),
    ),
    "remote_request_identities": (
        ("worker_id", "TEXT", 1, 1),
        ("epoch", "INTEGER", 1, 2),
        ("request_id", "TEXT", 1, 3),
        ("request_digest", "TEXT", 1, 0),
        ("operation", "TEXT", 1, 0),
        ("accepted_at", "REAL", 1, 0),
    ),
}
_SCHEMA_FOREIGN_KEYS = {
    "remote_journal_metadata": frozenset(),
    "remote_session_heads": frozenset(),
    "remote_session_instances": frozenset(
        {("remote_session_heads", "worker_id", "worker_id", "RESTRICT")}
    ),
    "remote_request_identities": frozenset(
        {
            ("remote_session_instances", "worker_id", "worker_id", "CASCADE"),
            ("remote_session_instances", "epoch", "epoch", "CASCADE"),
        }
    ),
}
_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE remote_journal_metadata(
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        schema_version INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE remote_session_heads(
        worker_id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        identity_digest TEXT NOT NULL,
        instance_id TEXT NOT NULL,
        epoch INTEGER NOT NULL CHECK(epoch >= 1),
        updated_at REAL NOT NULL
    )
    """,
    """
    CREATE TABLE remote_session_instances(
        worker_id TEXT NOT NULL,
        instance_id TEXT NOT NULL,
        epoch INTEGER NOT NULL CHECK(epoch >= 1),
        retired INTEGER NOT NULL CHECK(retired IN (0, 1)),
        created_at REAL NOT NULL,
        PRIMARY KEY(worker_id, instance_id),
        UNIQUE(worker_id, epoch),
        FOREIGN KEY(worker_id)
            REFERENCES remote_session_heads(worker_id)
            ON DELETE RESTRICT
    )
    """,
    """
    CREATE TABLE remote_request_identities(
        worker_id TEXT NOT NULL,
        epoch INTEGER NOT NULL CHECK(epoch >= 1),
        request_id TEXT NOT NULL,
        request_digest TEXT NOT NULL,
        operation TEXT NOT NULL,
        accepted_at REAL NOT NULL,
        PRIMARY KEY(worker_id, epoch, request_id),
        FOREIGN KEY(worker_id, epoch)
            REFERENCES remote_session_instances(worker_id, epoch)
            ON DELETE CASCADE
    )
    """,
)
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_OPERATIONS = frozenset(
    {
        "register",
        "poll",
        "poll_fleet",
        "start",
        "heartbeat",
        "cancellation_status",
        "complete",
        "ack_cancel",
    }
)


class RemoteJournalError(RuntimeError):
    """Base class for fail-closed journal failures."""


class RemoteJournalCapacityError(RemoteJournalError):
    """A durable bound was reached; callers must not discard live identity."""


class RemoteJournalIdentityError(RemoteJournalError):
    """The logical worker was rebound to a different trusted identity."""


class RemoteJournalRequestConflict(RemoteJournalError):
    """One request id was presented with a different canonical intent."""


class RemoteJournalSessionSuperseded(RemoteJournalError):
    """An old instance attempted to regain a retired session."""


@dataclass(frozen=True, slots=True)
class RemoteSessionEpoch:
    worker_id: str
    tenant_id: str
    identity_digest: str
    instance_id: str
    epoch: int
    request_is_new: bool


class RemoteControlJournal:
    """SQLite journal with transactional epoch allocation and request identity.

    A logical worker's first instance receives epoch 1.  A never-before-seen
    replacement receives the next epoch and atomically retires the old one.
    Retired instance ids are tombstones: they are intentionally not expired,
    because deleting one would recreate the A -> B -> A authority bug.

    Request identities are retained only for the current epoch.  Superseding a
    session makes the old epoch unusable and deletes its request rows.  The
    active epoch has a hard capacity and fails closed when full; it never
    evicts a digest merely to accept a reused request id.
    """

    def __init__(
        self,
        path: str | Path = ":memory:",
        *,
        max_workers: int = MAX_JOURNAL_WORKERS,
        max_instances_per_worker: int = MAX_JOURNAL_INSTANCES_PER_WORKER,
        max_requests_per_session: int = MAX_JOURNAL_REQUESTS_PER_SESSION,
        busy_timeout_ms: int = DEFAULT_JOURNAL_BUSY_TIMEOUT_MS,
    ) -> None:
        self.path = str(path)
        if not self.path:
            raise ValueError("journal path must not be empty")
        self.max_workers = _bounded_int(
            max_workers,
            "max_workers",
            1,
            MAX_JOURNAL_WORKERS,
        )
        self.max_instances_per_worker = _bounded_int(
            max_instances_per_worker,
            "max_instances_per_worker",
            1,
            MAX_JOURNAL_INSTANCES_PER_WORKER,
        )
        self.max_requests_per_session = _bounded_int(
            max_requests_per_session,
            "max_requests_per_session",
            1,
            MAX_JOURNAL_REQUESTS_PER_SESSION,
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
            except RemoteJournalError:
                raise
            except OSError as exc:
                raise RemoteJournalError("journal_unavailable") from exc
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

    def register_session(
        self,
        *,
        worker_id: str,
        tenant_id: str,
        identity_digest: str,
        instance_id: str,
        request_id: str,
        request_digest: str,
        operation: str,
        now: float,
    ) -> RemoteSessionEpoch:
        """Allocate/recover an epoch and record its registration request."""

        _journal_identity(
            worker_id,
            tenant_id,
            identity_digest,
            instance_id,
        )
        _request_identity(request_id, request_digest, operation)
        if operation != "register":
            raise RemoteJournalError("invalid_registration_operation")
        timestamp = _finite_time(now)
        with self._transaction() as connection:
            head = connection.execute(
                """
                SELECT tenant_id, identity_digest, instance_id, epoch
                FROM remote_session_heads
                WHERE worker_id = ?
                """,
                (worker_id,),
            ).fetchone()
            if head is None:
                worker_count = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM remote_session_heads"
                    ).fetchone()[0]
                )
                if worker_count >= self.max_workers:
                    raise RemoteJournalCapacityError("worker_capacity")
                epoch = 1
                connection.execute(
                    """
                    INSERT INTO remote_session_heads(
                        worker_id, tenant_id, identity_digest, instance_id,
                        epoch, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        worker_id,
                        tenant_id,
                        identity_digest,
                        instance_id,
                        epoch,
                        timestamp,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO remote_session_instances(
                        worker_id, instance_id, epoch, retired, created_at
                    ) VALUES (?, ?, ?, 0, ?)
                    """,
                    (worker_id, instance_id, epoch, timestamp),
                )
            else:
                if (
                    str(head["tenant_id"]) != tenant_id
                    or str(head["identity_digest"]) != identity_digest
                ):
                    raise RemoteJournalIdentityError("identity_mismatch")
                current_instance = str(head["instance_id"])
                epoch = int(head["epoch"])
                if current_instance == instance_id:
                    connection.execute(
                        """
                        UPDATE remote_session_heads
                        SET updated_at = ?
                        WHERE worker_id = ? AND epoch = ?
                        """,
                        (timestamp, worker_id, epoch),
                    )
                else:
                    seen = connection.execute(
                        """
                        SELECT epoch
                        FROM remote_session_instances
                        WHERE worker_id = ? AND instance_id = ?
                        """,
                        (worker_id, instance_id),
                    ).fetchone()
                    if seen is not None:
                        raise RemoteJournalSessionSuperseded(
                            "session_superseded"
                        )
                    instance_count = int(
                        connection.execute(
                            """
                            SELECT COUNT(*)
                            FROM remote_session_instances
                            WHERE worker_id = ?
                            """,
                            (worker_id,),
                        ).fetchone()[0]
                    )
                    if instance_count >= self.max_instances_per_worker:
                        raise RemoteJournalCapacityError("instance_capacity")
                    if epoch >= 2**63 - 1:
                        raise RemoteJournalCapacityError("epoch_capacity")
                    previous_epoch = epoch
                    epoch += 1
                    connection.execute(
                        """
                        UPDATE remote_session_instances
                        SET retired = 1
                        WHERE worker_id = ? AND epoch = ?
                        """,
                        (worker_id, previous_epoch),
                    )
                    connection.execute(
                        """
                        INSERT INTO remote_session_instances(
                            worker_id, instance_id, epoch, retired, created_at
                        ) VALUES (?, ?, ?, 0, ?)
                        """,
                        (worker_id, instance_id, epoch, timestamp),
                    )
                    connection.execute(
                        """
                        UPDATE remote_session_heads
                        SET instance_id = ?, epoch = ?, updated_at = ?
                        WHERE worker_id = ? AND epoch = ?
                        """,
                        (
                            instance_id,
                            epoch,
                            timestamp,
                            worker_id,
                            previous_epoch,
                        ),
                    )
                    # Retired epochs can never authenticate again, so their
                    # request identities no longer consume the active bound.
                    connection.execute(
                        """
                        DELETE FROM remote_request_identities
                        WHERE worker_id = ? AND epoch <> ?
                        """,
                        (worker_id, epoch),
                    )
            request_is_new = self._record_request_locked(
                connection,
                worker_id=worker_id,
                epoch=epoch,
                request_id=request_id,
                request_digest=request_digest,
                operation=operation,
                now=timestamp,
            )
            return RemoteSessionEpoch(
                worker_id=worker_id,
                tenant_id=tenant_id,
                identity_digest=identity_digest,
                instance_id=instance_id,
                epoch=epoch,
                request_is_new=request_is_new,
            )

    def record_request(
        self,
        *,
        worker_id: str,
        tenant_id: str,
        identity_digest: str,
        instance_id: str,
        epoch: int,
        request_id: str,
        request_digest: str,
        operation: str,
        now: float,
    ) -> bool:
        """Record one non-registration request for the exact current epoch."""

        _journal_identity(
            worker_id,
            tenant_id,
            identity_digest,
            instance_id,
        )
        _request_identity(request_id, request_digest, operation)
        _bounded_int(epoch, "epoch", 1, 2**63 - 1)
        timestamp = _finite_time(now)
        with self._transaction() as connection:
            head = connection.execute(
                """
                SELECT tenant_id, identity_digest, instance_id, epoch
                FROM remote_session_heads
                WHERE worker_id = ?
                """,
                (worker_id,),
            ).fetchone()
            if head is None:
                raise RemoteJournalSessionSuperseded("session_missing")
            if (
                str(head["tenant_id"]) != tenant_id
                or str(head["identity_digest"]) != identity_digest
            ):
                raise RemoteJournalIdentityError("identity_mismatch")
            if (
                str(head["instance_id"]) != instance_id
                or int(head["epoch"]) != epoch
            ):
                raise RemoteJournalSessionSuperseded("session_superseded")
            connection.execute(
                """
                UPDATE remote_session_heads
                SET updated_at = ?
                WHERE worker_id = ? AND epoch = ?
                """,
                (timestamp, worker_id, epoch),
            )
            return self._record_request_locked(
                connection,
                worker_id=worker_id,
                epoch=epoch,
                request_id=request_id,
                request_digest=request_digest,
                operation=operation,
                now=timestamp,
            )

    def assert_current(
        self,
        *,
        worker_id: str,
        tenant_id: str,
        identity_digest: str,
        instance_id: str,
        epoch: int,
    ) -> None:
        """Fail closed unless the supplied session is the durable head."""

        _journal_identity(
            worker_id,
            tenant_id,
            identity_digest,
            instance_id,
        )
        _bounded_int(epoch, "epoch", 1, 2**63 - 1)
        try:
            with self._connection() as connection:
                head = connection.execute(
                    """
                    SELECT tenant_id, identity_digest, instance_id, epoch
                    FROM remote_session_heads
                    WHERE worker_id = ?
                    """,
                    (worker_id,),
                ).fetchone()
        except sqlite3.Error as exc:
            raise RemoteJournalError("journal_unavailable") from exc
        if head is None:
            raise RemoteJournalSessionSuperseded("session_missing")
        if (
            str(head["tenant_id"]) != tenant_id
            or str(head["identity_digest"]) != identity_digest
        ):
            raise RemoteJournalIdentityError("identity_mismatch")
        if (
            str(head["instance_id"]) != instance_id
            or int(head["epoch"]) != epoch
        ):
            raise RemoteJournalSessionSuperseded("session_superseded")

    def _record_request_locked(
        self,
        connection: sqlite3.Connection,
        *,
        worker_id: str,
        epoch: int,
        request_id: str,
        request_digest: str,
        operation: str,
        now: float,
    ) -> bool:
        existing = connection.execute(
            """
            SELECT request_digest
            FROM remote_request_identities
            WHERE worker_id = ? AND epoch = ? AND request_id = ?
            """,
            (worker_id, epoch, request_id),
        ).fetchone()
        if existing is not None:
            if str(existing["request_digest"]) != request_digest:
                raise RemoteJournalRequestConflict("request_id_conflict")
            return False
        request_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM remote_request_identities
                WHERE worker_id = ? AND epoch = ?
                """,
                (worker_id, epoch),
            ).fetchone()[0]
        )
        if request_count >= self.max_requests_per_session:
            raise RemoteJournalCapacityError("request_capacity")
        connection.execute(
            """
            INSERT INTO remote_request_identities(
                worker_id, epoch, request_id, request_digest, operation,
                accepted_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                worker_id,
                epoch,
                request_id,
                request_digest,
                operation,
                now,
            ),
        )
        return True

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
                raise RemoteJournalError("invalid_schema")
            is_new = not existing_tables and self._bootstrap_new_database
            if not existing_tables and not is_new:
                raise RemoteJournalError("invalid_schema")
            if is_new:
                for statement in _SCHEMA_STATEMENTS:
                    connection.execute(statement)
            else:
                self._validate_schema(connection)
            row = connection.execute(
                """
                SELECT schema_version
                FROM remote_journal_metadata
                WHERE singleton = 1
                """
            ).fetchone()
            if row is None:
                if not is_new:
                    raise RemoteJournalError("invalid_schema")
                connection.execute(
                    """
                    INSERT INTO remote_journal_metadata(singleton, schema_version)
                    VALUES (1, ?)
                    """,
                    (REMOTE_JOURNAL_SCHEMA_VERSION,),
                )
            elif int(row["schema_version"]) != REMOTE_JOURNAL_SCHEMA_VERSION:
                raise RemoteJournalError("unsupported_schema")
            if is_new:
                self._validate_schema(connection)

    def _prepare_durable_database(self, journal_path: Path) -> None:
        """Serialize bootstrap and publish only a complete, validated database."""

        lock_path = journal_path.parent / f".{journal_path.name}.bootstrap.lock"
        bootstrap_path = (
            journal_path.parent / f".{journal_path.name}.bootstrap.sqlite3"
        )
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        connection: sqlite3.Connection | None = None
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            marker = self._read_bootstrap_marker(lock_fd)
            if journal_path.is_symlink():
                raise RemoteJournalError("invalid_schema")
            if journal_path.exists():
                self._validate_durable_database(journal_path)
                self._write_bootstrap_marker(lock_fd, b"initialized\n")
                self._cleanup_bootstrap_files(bootstrap_path)
                return
            if marker == b"initialized\n":
                raise RemoteJournalError("invalid_schema")
            self._write_bootstrap_marker(lock_fd, b"bootstrapping\n")
            self._cleanup_bootstrap_files(bootstrap_path)
            connection = sqlite3.connect(
                bootstrap_path,
                timeout=self.busy_timeout_ms / 1_000,
                isolation_level=None,
                check_same_thread=False,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA journal_mode = DELETE")
            connection.execute("BEGIN IMMEDIATE")
            try:
                for statement in _SCHEMA_STATEMENTS:
                    connection.execute(statement)
                connection.execute(
                    """
                    INSERT INTO remote_journal_metadata(singleton, schema_version)
                    VALUES (1, ?)
                    """,
                    (REMOTE_JOURNAL_SCHEMA_VERSION,),
                )
                self._validate_schema(connection)
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()
            mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()
            if mode is None or str(mode[0]).lower() != "wal":
                raise RemoteJournalError("journal_unavailable")
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
                # The database file itself was durably synced. Some platforms
                # do not expose directory fsync to Python.
                pass
        except sqlite3.Error as exc:
            raise RemoteJournalError("journal_unavailable") from exc
        finally:
            if connection is not None:
                connection.close()
            self._cleanup_bootstrap_files(bootstrap_path)
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    def _validate_durable_database(self, journal_path: Path) -> None:
        connection = sqlite3.connect(
            journal_path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            connection.execute("PRAGMA synchronous = FULL")
            self._validate_schema(connection)
            row = connection.execute(
                """
                SELECT schema_version
                FROM remote_journal_metadata
                WHERE singleton = 1
                """
            ).fetchone()
            if row is None:
                raise RemoteJournalError("invalid_schema")
            if int(row["schema_version"]) != REMOTE_JOURNAL_SCHEMA_VERSION:
                raise RemoteJournalError("unsupported_schema")
            mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()
            if mode is None or str(mode[0]).lower() != "wal":
                raise RemoteJournalError("journal_unavailable")
        finally:
            connection.close()

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

    @staticmethod
    def _validate_schema(connection: sqlite3.Connection) -> None:
        quick_check = connection.execute("PRAGMA quick_check(1)").fetchone()
        if quick_check is None or str(quick_check[0]).lower() != "ok":
            raise RemoteJournalError("journal_integrity_failed")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise RemoteJournalError("journal_integrity_failed")
        for table, expected_columns in _SCHEMA_COLUMNS.items():
            actual_columns = tuple(
                (
                    str(row["name"]),
                    str(row["type"]).upper(),
                    int(row["notnull"]),
                    int(row["pk"]),
                )
                for row in connection.execute(f"PRAGMA table_info({table})")
            )
            if actual_columns != expected_columns:
                raise RemoteJournalError("invalid_schema")
            actual_foreign_keys = frozenset(
                (
                    str(row["table"]),
                    str(row["from"]),
                    str(row["to"]),
                    str(row["on_delete"]).upper(),
                )
                for row in connection.execute(
                    f"PRAGMA foreign_key_list({table})"
                )
            )
            if actual_foreign_keys != _SCHEMA_FOREIGN_KEYS[table]:
                raise RemoteJournalError("invalid_schema")
        unique_indexes = {
            tuple(
                str(column["name"])
                for column in connection.execute(
                    f"PRAGMA index_info({index['name']})"
                )
            )
            for index in connection.execute(
                "PRAGMA index_list(remote_session_instances)"
            )
            if int(index["unique"]) == 1
        }
        if ("worker_id", "epoch") not in unique_indexes:
            raise RemoteJournalError("invalid_schema")

    def _new_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA synchronous = FULL")
        if self.path != ":memory:":
            mode = connection.execute("PRAGMA journal_mode").fetchone()
            if mode is None or str(mode[0]).lower() != "wal":
                connection.close()
                raise RemoteJournalError("journal_unavailable")
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        if self.path == ":memory:":
            with self._memory_lock:
                connection = self._memory_connection
                if connection is None:
                    raise RemoteJournalError("journal_closed")
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
            raise RemoteJournalError("journal_unavailable") from exc


def _bounded_int(value: int, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _finite_time(value: float) -> float:
    try:
        timestamp = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RemoteJournalError("invalid_time") from exc
    if timestamp < 0 or not math.isfinite(timestamp):
        raise RemoteJournalError("invalid_time")
    return timestamp


def _journal_identity(
    worker_id: str,
    tenant_id: str,
    identity_digest: str,
    instance_id: str,
) -> None:
    for value, name in (
        (worker_id, "worker_id"),
        (tenant_id, "tenant_id"),
        (instance_id, "instance_id"),
    ):
        if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
            raise RemoteJournalError(f"invalid_{name}")
    if (
        not isinstance(identity_digest, str)
        or _DIGEST.fullmatch(identity_digest) is None
    ):
        raise RemoteJournalError("invalid_identity_digest")


def _request_identity(
    request_id: str,
    request_digest: str,
    operation: str,
) -> None:
    if (
        not isinstance(request_id, str)
        or _IDENTIFIER.fullmatch(request_id) is None
    ):
        raise RemoteJournalError("invalid_request_id")
    if (
        not isinstance(request_digest, str)
        or _DIGEST.fullmatch(request_digest) is None
    ):
        raise RemoteJournalError("invalid_request_digest")
    if not isinstance(operation, str) or operation not in _OPERATIONS:
        raise RemoteJournalError("invalid_operation")


__all__ = [
    "MAX_JOURNAL_INSTANCES_PER_WORKER",
    "MAX_JOURNAL_REQUESTS_PER_SESSION",
    "MAX_JOURNAL_WORKERS",
    "REMOTE_JOURNAL_SCHEMA_VERSION",
    "RemoteControlJournal",
    "RemoteJournalCapacityError",
    "RemoteJournalError",
    "RemoteJournalIdentityError",
    "RemoteJournalRequestConflict",
    "RemoteJournalSessionSuperseded",
    "RemoteSessionEpoch",
]
