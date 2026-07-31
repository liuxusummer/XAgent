"""Durable bearer-free recovery authority for remote executions.

The journal proves which authorization binding the control plane actually
issued for one durable Attempt and tracks monotonic Artifact/provider grant
state plus payload-free provider invocation receipts.  It intentionally stores
no claim token, bearer plaintext, prompt, response body, script bytes, Artifact
content, environment value, runtime proof, or raw execution plan.
"""

from __future__ import annotations

import fcntl
import hmac
import json
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

REMOTE_EXECUTION_JOURNAL_SCHEMA_VERSION = 4
DEFAULT_EXECUTION_JOURNAL_BUSY_TIMEOUT_MS = 10_000
MAX_EXECUTION_JOURNAL_RECORDS = 100_000
MAX_ARTIFACT_GRANT_JOURNAL_RECORDS = 100_000
MAX_PROVIDER_GRANT_JOURNAL_RECORDS = 100_000
MAX_ARTIFACT_GRANT_METADATA_BYTES = 16 * 1024

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_SECURITY_LEVELS = frozenset({"container"})
_V1_SCHEMA_COLUMNS = {
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
_V2_SCHEMA_COLUMNS = {
    **_V1_SCHEMA_COLUMNS,
    "remote_artifact_read_grants": (
        ("grant_id", "TEXT", 1, 1),
        ("token_digest", "TEXT", 1, 0),
        ("grant_metadata", "TEXT", 1, 0),
        ("artifact_ref", "TEXT", 1, 0),
        ("state", "TEXT", 1, 0),
        ("expires_at", "REAL", 1, 0),
        ("updated_at", "REAL", 1, 0),
    ),
    "remote_artifact_write_grants": (
        ("grant_id", "TEXT", 1, 1),
        ("token_digest", "TEXT", 1, 0),
        ("grant_metadata", "TEXT", 1, 0),
        ("state", "TEXT", 1, 0),
        ("staging_digest", "TEXT", 0, 0),
        ("final_ref", "TEXT", 0, 0),
        ("expires_at", "REAL", 1, 0),
        ("updated_at", "REAL", 1, 0),
    ),
}
_V2_SCHEMA_INDEXES = {
    "idx_remote_artifact_read_grants_expires": (
        "remote_artifact_read_grants",
        0,
        ("expires_at",),
    ),
    "idx_remote_artifact_write_grants_expires": (
        "remote_artifact_write_grants",
        0,
        ("expires_at",),
    ),
}
_V3_SCHEMA_COLUMNS = {
    **_V2_SCHEMA_COLUMNS,
    "remote_provider_grants": (
        ("grant_id", "TEXT", 1, 1),
        ("logical_invocation_digest", "TEXT", 1, 0),
        ("token_digest", "TEXT", 1, 0),
        ("binding_digest", "TEXT", 1, 0),
        ("route_id", "TEXT", 1, 0),
        ("state", "TEXT", 1, 0),
        ("expires_at", "REAL", 1, 0),
        ("updated_at", "REAL", 1, 0),
    ),
    "remote_provider_grant_clock": (
        ("singleton", "INTEGER", 0, 1),
        ("purge_watermark", "REAL", 1, 0),
    ),
}
_V3_SCHEMA_INDEXES = {
    **_V2_SCHEMA_INDEXES,
    "idx_remote_provider_grants_logical": (
        "remote_provider_grants",
        1,
        ("logical_invocation_digest",),
    ),
    "idx_remote_provider_grants_expires": (
        "remote_provider_grants",
        0,
        ("expires_at",),
    ),
}
_SCHEMA_COLUMNS = {
    **_V3_SCHEMA_COLUMNS,
    "remote_provider_invocations": (
        ("grant_id", "TEXT", 1, 1),
        ("request_payload_digest", "TEXT", 1, 0),
        ("state", "TEXT", 1, 0),
        ("response_digest", "TEXT", 0, 0),
        ("response_artifact_ref", "TEXT", 0, 0),
        ("updated_at", "REAL", 1, 0),
    ),
}
_SCHEMA_INDEXES = dict(_V3_SCHEMA_INDEXES)
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
    """
    CREATE TABLE remote_artifact_read_grants(
        grant_id TEXT NOT NULL PRIMARY KEY,
        token_digest TEXT NOT NULL,
        grant_metadata TEXT NOT NULL,
        artifact_ref TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('issued', 'consumed')),
        expires_at REAL NOT NULL,
        updated_at REAL NOT NULL
    )
    """,
    """
    CREATE TABLE remote_artifact_write_grants(
        grant_id TEXT NOT NULL PRIMARY KEY,
        token_digest TEXT NOT NULL,
        grant_metadata TEXT NOT NULL,
        state TEXT NOT NULL
            CHECK(state IN ('issued', 'finalized', 'failed')),
        staging_digest TEXT,
        final_ref TEXT,
        expires_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        CHECK(
            (state = 'finalized'
                AND staging_digest IS NOT NULL
                AND final_ref IS NOT NULL)
            OR
            (state <> 'finalized'
                AND staging_digest IS NULL
                AND final_ref IS NULL)
        )
    )
    """,
    """
    CREATE INDEX idx_remote_artifact_read_grants_expires
    ON remote_artifact_read_grants(expires_at)
    """,
    """
    CREATE INDEX idx_remote_artifact_write_grants_expires
    ON remote_artifact_write_grants(expires_at)
    """,
    """
    CREATE TABLE remote_provider_grants(
        grant_id TEXT NOT NULL PRIMARY KEY,
        logical_invocation_digest TEXT NOT NULL,
        token_digest TEXT NOT NULL,
        binding_digest TEXT NOT NULL,
        route_id TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('issued', 'consumed')),
        expires_at REAL NOT NULL,
        updated_at REAL NOT NULL
    )
    """,
    """
    CREATE UNIQUE INDEX idx_remote_provider_grants_logical
    ON remote_provider_grants(logical_invocation_digest)
    """,
    """
    CREATE INDEX idx_remote_provider_grants_expires
    ON remote_provider_grants(expires_at)
    """,
    """
    CREATE TABLE remote_provider_grant_clock(
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        purge_watermark REAL NOT NULL CHECK(purge_watermark >= 0)
    )
    """,
    """
    INSERT INTO remote_provider_grant_clock(
        singleton, purge_watermark
    ) VALUES (1, 0)
    """,
    """
    CREATE TABLE remote_provider_invocations(
        grant_id TEXT NOT NULL PRIMARY KEY,
        request_payload_digest TEXT NOT NULL,
        state TEXT NOT NULL
            CHECK(state IN ('invoking', 'completed', 'outcome_unknown')),
        response_digest TEXT,
        response_artifact_ref TEXT,
        updated_at REAL NOT NULL,
        CHECK(
            (state = 'completed' AND response_digest IS NOT NULL)
            OR
            (state <> 'completed'
                AND response_digest IS NULL
                AND response_artifact_ref IS NULL)
        ),
        CHECK(
            response_artifact_ref IS NULL OR state = 'completed'
        )
    )
    """,
)
_V1_UPGRADE_SCHEMA_STATEMENTS = _SCHEMA_STATEMENTS[2:]
_V2_UPGRADE_SCHEMA_STATEMENTS = _SCHEMA_STATEMENTS[6:]
_V3_UPGRADE_SCHEMA_STATEMENTS = _SCHEMA_STATEMENTS[-1:]
_READ_GRANT_STATES = frozenset({"issued", "consumed"})
_WRITE_GRANT_STATES = frozenset({"issued", "finalized", "failed"})
_PROVIDER_GRANT_STATES = frozenset({"issued", "consumed"})
_PROVIDER_INVOCATION_STATES = frozenset(
    {"invoking", "completed", "outcome_unknown"}
)


class RemoteExecutionJournalError(RuntimeError):
    """Base fail-closed recovery journal error."""


class RemoteExecutionJournalCapacityError(RemoteExecutionJournalError):
    """The hard live-record bound was reached."""


class RemoteExecutionJournalConflict(RemoteExecutionJournalError):
    """An Attempt was rebound to different execution authority."""


class RemoteArtifactGrantUnavailable(RemoteExecutionJournalError):
    """A persisted grant is absent, consumed, failed, or expired."""


class RemoteArtifactGrantConflict(RemoteExecutionJournalError):
    """A grant id or finalization was rebound to different metadata."""


class RemoteProviderGrantUnavailable(RemoteExecutionJournalError):
    """A provider grant is absent, consumed, mismatched, or expired."""


class RemoteProviderGrantConflict(RemoteExecutionJournalError):
    """A provider grant or logical invocation was rebound."""


class RemoteProviderInvocationUnknown(RemoteExecutionJournalError):
    """A consumed provider invocation has no replayable terminal result."""


class RemoteProviderInvocationConflict(RemoteExecutionJournalError):
    """A provider invocation receipt was rebound to different evidence."""


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


@dataclass(frozen=True, slots=True)
class RemoteArtifactReadGrantRecord:
    """Bearer-free durable read grant state."""

    grant_id: str
    token_digest: str
    grant_metadata: str
    artifact_ref: str
    state: str
    expires_at: float
    updated_at: float

    def __post_init__(self) -> None:
        _identifier(self.grant_id, "grant_id")
        _digest(self.token_digest, "token_digest")
        _canonical_json_text(self.grant_metadata, "grant_metadata")
        _canonical_json_text(self.artifact_ref, "artifact_ref")
        if self.state not in _READ_GRANT_STATES:
            raise RemoteExecutionJournalError("invalid_read_grant_state")
        expires_at = _finite_time(self.expires_at)
        updated_at = _finite_time(self.updated_at)
        if expires_at <= 0 or updated_at > expires_at:
            raise RemoteExecutionJournalError("invalid_read_grant_time")


@dataclass(frozen=True, slots=True)
class RemoteArtifactWriteGrantRecord:
    """Bearer-free durable write/finalization state."""

    grant_id: str
    token_digest: str
    grant_metadata: str
    state: str
    staging_digest: str | None
    final_ref: str | None
    expires_at: float
    updated_at: float

    def __post_init__(self) -> None:
        _identifier(self.grant_id, "grant_id")
        _digest(self.token_digest, "token_digest")
        _canonical_json_text(self.grant_metadata, "grant_metadata")
        if self.state not in _WRITE_GRANT_STATES:
            raise RemoteExecutionJournalError("invalid_write_grant_state")
        if self.state == "finalized":
            if self.staging_digest is None or self.final_ref is None:
                raise RemoteExecutionJournalError(
                    "invalid_write_grant_finalization"
                )
            _digest(self.staging_digest, "staging_digest")
            _canonical_json_text(self.final_ref, "final_ref")
        elif self.staging_digest is not None or self.final_ref is not None:
            raise RemoteExecutionJournalError(
                "invalid_write_grant_finalization"
            )
        expires_at = _finite_time(self.expires_at)
        updated_at = _finite_time(self.updated_at)
        if expires_at <= 0 or updated_at > expires_at:
            raise RemoteExecutionJournalError("invalid_write_grant_time")


@dataclass(frozen=True, slots=True)
class RemoteProviderGrantRecord:
    """Bearer-free durable state for one logical provider invocation."""

    grant_id: str
    logical_invocation_digest: str
    token_digest: str
    binding_digest: str
    route_id: str
    state: str
    expires_at: float
    updated_at: float

    def __post_init__(self) -> None:
        _identifier(self.grant_id, "grant_id")
        _identifier(self.route_id, "route_id")
        for name in (
            "logical_invocation_digest",
            "token_digest",
            "binding_digest",
        ):
            _digest(getattr(self, name), name)
        if self.state not in _PROVIDER_GRANT_STATES:
            raise RemoteExecutionJournalError(
                "invalid_provider_grant_state"
            )
        expires_at = _finite_time(self.expires_at)
        updated_at = _finite_time(self.updated_at)
        if expires_at <= 0 or updated_at > expires_at:
            raise RemoteExecutionJournalError(
                "invalid_provider_grant_time"
            )


@dataclass(frozen=True, slots=True)
class RemoteProviderInvocationRecord:
    """Payload-free durable state for one provider call."""

    grant_id: str
    request_payload_digest: str
    state: str
    response_digest: str | None
    response_artifact_ref: str | None
    updated_at: float

    def __post_init__(self) -> None:
        _identifier(self.grant_id, "grant_id")
        _digest(
            self.request_payload_digest,
            "request_payload_digest",
        )
        if self.state not in _PROVIDER_INVOCATION_STATES:
            raise RemoteExecutionJournalError(
                "invalid_provider_invocation_state"
            )
        if self.state == "completed":
            if self.response_digest is None:
                raise RemoteExecutionJournalError(
                    "invalid_provider_invocation_result"
                )
            _digest(self.response_digest, "response_digest")
            if self.response_artifact_ref is not None:
                _canonical_json_text(
                    self.response_artifact_ref,
                    "response_artifact_ref",
                )
        elif (
            self.response_digest is not None
            or self.response_artifact_ref is not None
        ):
            raise RemoteExecutionJournalError(
                "invalid_provider_invocation_result"
            )
        _finite_time(self.updated_at)


@dataclass(frozen=True, slots=True)
class RemoteProviderInvocationClaim:
    """Journal decision: invoke upstream now or replay a completed receipt."""

    record: RemoteProviderInvocationRecord
    execute: bool

    def __post_init__(self) -> None:
        if not isinstance(self.record, RemoteProviderInvocationRecord):
            raise RemoteExecutionJournalError(
                "invalid_provider_invocation_claim"
            )
        if not isinstance(self.execute, bool):
            raise RemoteExecutionJournalError(
                "invalid_provider_invocation_claim"
            )
        if self.execute != (self.record.state == "invoking"):
            raise RemoteExecutionJournalError(
                "invalid_provider_invocation_claim"
            )


class RemoteExecutionJournal:
    """SQLite-backed exact authorization binding registry.

    Execution rows are immutable.  Artifact/provider rows move only through
    explicit state transitions.  Rebinding is a conflict and unexpired rows
    are never evicted to make room because doing so would erase recovery or
    anti-replay evidence.
    """

    def __init__(
        self,
        path: str | Path = ":memory:",
        *,
        maximum_records: int = MAX_EXECUTION_JOURNAL_RECORDS,
        maximum_artifact_grants: int = (
            MAX_ARTIFACT_GRANT_JOURNAL_RECORDS
        ),
        maximum_provider_grants: int = (
            MAX_PROVIDER_GRANT_JOURNAL_RECORDS
        ),
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
        self.maximum_artifact_grants = _bounded_int(
            maximum_artifact_grants,
            "maximum_artifact_grants",
            1,
            MAX_ARTIFACT_GRANT_JOURNAL_RECORDS,
        )
        self.maximum_provider_grants = _bounded_int(
            maximum_provider_grants,
            "maximum_provider_grants",
            1,
            MAX_PROVIDER_GRANT_JOURNAL_RECORDS,
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

    def record_read_grant(
        self,
        record: RemoteArtifactReadGrantRecord,
    ) -> RemoteArtifactReadGrantRecord:
        if not isinstance(record, RemoteArtifactReadGrantRecord):
            raise TypeError(
                "record must be a RemoteArtifactReadGrantRecord"
            )
        if record.state != "issued" or record.updated_at >= record.expires_at:
            raise RemoteExecutionJournalError("invalid_read_grant_state")
        with self._transaction() as connection:
            self._delete_expired_grants_locked(
                connection,
                now=record.updated_at,
            )
            existing = self._read_grant_locked(connection, record.grant_id)
            if existing is not None:
                if (
                    existing.token_digest != record.token_digest
                    or existing.grant_metadata != record.grant_metadata
                    or existing.artifact_ref != record.artifact_ref
                    or existing.expires_at != record.expires_at
                    or existing.state != "issued"
                ):
                    raise RemoteArtifactGrantConflict(
                        "read_grant_conflict"
                    )
                return existing
            self._require_artifact_capacity_locked(connection)
            connection.execute(
                """
                INSERT INTO remote_artifact_read_grants(
                    grant_id, token_digest, grant_metadata, artifact_ref,
                    state, expires_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(
                    getattr(record, item.name)
                    for item in fields(record)
                ),
            )
            return record

    def consume_read_grant(
        self,
        *,
        grant_id: str,
        token_digest: str,
        grant_metadata: str,
        expires_at: float,
        now: float,
    ) -> RemoteArtifactReadGrantRecord:
        _identifier(grant_id, "grant_id")
        _digest(token_digest, "token_digest")
        _canonical_json_text(grant_metadata, "grant_metadata")
        expected_expires_at = _finite_time(expires_at)
        current = _finite_time(now)
        consumed: RemoteArtifactReadGrantRecord | None = None
        expired = False
        with self._transaction() as connection:
            record = self._read_grant_locked(connection, grant_id)
            if record is None:
                raise RemoteArtifactGrantUnavailable(
                    "read_grant_unavailable"
                )
            if (
                not hmac.compare_digest(
                    record.token_digest,
                    token_digest,
                )
                or record.grant_metadata != grant_metadata
                or record.expires_at != expected_expires_at
            ):
                raise RemoteArtifactGrantConflict(
                    "read_grant_binding_mismatch"
                )
            if current >= expected_expires_at:
                connection.execute(
                    """
                    DELETE FROM remote_artifact_read_grants
                    WHERE grant_id = ?
                    """,
                    (grant_id,),
                )
                expired = True
            elif record.state != "issued":
                raise RemoteArtifactGrantUnavailable(
                    "read_grant_unavailable"
                )
            else:
                cursor = connection.execute(
                    """
                    UPDATE remote_artifact_read_grants
                    SET state = 'consumed', updated_at = ?
                    WHERE grant_id = ? AND state = 'issued'
                    """,
                    (current, grant_id),
                )
                if cursor.rowcount != 1:
                    raise RemoteArtifactGrantUnavailable(
                        "read_grant_unavailable"
                    )
                consumed = RemoteArtifactReadGrantRecord(
                    grant_id=record.grant_id,
                    token_digest=record.token_digest,
                    grant_metadata=record.grant_metadata,
                    artifact_ref=record.artifact_ref,
                    state="consumed",
                    expires_at=record.expires_at,
                    updated_at=current,
                )
        if expired:
            raise RemoteArtifactGrantUnavailable("read_grant_unavailable")
        if consumed is None:
            raise RemoteExecutionJournalError(
                "invalid_artifact_grant_record"
            )
        return consumed

    def record_write_grant(
        self,
        record: RemoteArtifactWriteGrantRecord,
    ) -> RemoteArtifactWriteGrantRecord:
        if not isinstance(record, RemoteArtifactWriteGrantRecord):
            raise TypeError(
                "record must be a RemoteArtifactWriteGrantRecord"
            )
        if record.state != "issued" or record.updated_at >= record.expires_at:
            raise RemoteExecutionJournalError("invalid_write_grant_state")
        with self._transaction() as connection:
            self._delete_expired_grants_locked(
                connection,
                now=record.updated_at,
            )
            existing = self._write_grant_locked(
                connection,
                record.grant_id,
            )
            if existing is not None:
                if (
                    existing.token_digest != record.token_digest
                    or existing.grant_metadata != record.grant_metadata
                    or existing.expires_at != record.expires_at
                    or existing.state != "issued"
                ):
                    raise RemoteArtifactGrantConflict(
                        "write_grant_conflict"
                    )
                return existing
            self._require_artifact_capacity_locked(connection)
            connection.execute(
                """
                INSERT INTO remote_artifact_write_grants(
                    grant_id, token_digest, grant_metadata, state,
                    staging_digest, final_ref, expires_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(
                    getattr(record, item.name)
                    for item in fields(record)
                ),
            )
            return record

    def get_write_grant(
        self,
        *,
        grant_id: str,
        token_digest: str,
        now: float,
    ) -> RemoteArtifactWriteGrantRecord:
        _identifier(grant_id, "grant_id")
        _digest(token_digest, "token_digest")
        current = _finite_time(now)
        result: RemoteArtifactWriteGrantRecord | None = None
        expired = False
        with self._transaction() as connection:
            record = self._write_grant_locked(connection, grant_id)
            if record is None:
                raise RemoteArtifactGrantUnavailable(
                    "write_grant_unavailable"
                )
            if current >= record.expires_at:
                connection.execute(
                    """
                    DELETE FROM remote_artifact_write_grants
                    WHERE grant_id = ?
                    """,
                    (grant_id,),
                )
                expired = True
            elif not hmac.compare_digest(
                record.token_digest,
                token_digest,
            ):
                raise RemoteArtifactGrantConflict(
                    "write_grant_binding_mismatch"
                )
            elif record.state == "failed":
                raise RemoteArtifactGrantUnavailable(
                    "write_grant_unavailable"
                )
            else:
                result = record
        if expired:
            raise RemoteArtifactGrantUnavailable("write_grant_unavailable")
        if result is None:
            raise RemoteExecutionJournalError(
                "invalid_artifact_grant_record"
            )
        return result

    def finalize_write_grant(
        self,
        *,
        grant_id: str,
        token_digest: str,
        grant_metadata: str,
        staging_digest: str,
        final_ref: str,
        now: float,
    ) -> RemoteArtifactWriteGrantRecord:
        _identifier(grant_id, "grant_id")
        _digest(token_digest, "token_digest")
        _canonical_json_text(grant_metadata, "grant_metadata")
        _digest(staging_digest, "staging_digest")
        _canonical_json_text(final_ref, "final_ref")
        current = _finite_time(now)
        finalized: RemoteArtifactWriteGrantRecord | None = None
        expired = False
        with self._transaction() as connection:
            record = self._write_grant_locked(connection, grant_id)
            if record is None:
                raise RemoteArtifactGrantUnavailable(
                    "write_grant_unavailable"
                )
            if current >= record.expires_at:
                connection.execute(
                    """
                    DELETE FROM remote_artifact_write_grants
                    WHERE grant_id = ?
                    """,
                    (grant_id,),
                )
                expired = True
            elif (
                not hmac.compare_digest(
                    record.token_digest,
                    token_digest,
                )
                or record.grant_metadata != grant_metadata
            ):
                raise RemoteArtifactGrantConflict(
                    "write_grant_binding_mismatch"
                )
            elif record.state == "failed":
                raise RemoteArtifactGrantUnavailable(
                    "write_grant_unavailable"
                )
            elif record.state == "finalized":
                if (
                    record.staging_digest != staging_digest
                    or record.final_ref != final_ref
                ):
                    raise RemoteArtifactGrantConflict(
                        "write_finalization_conflict"
                    )
                finalized = record
            else:
                cursor = connection.execute(
                    """
                    UPDATE remote_artifact_write_grants
                    SET state = 'finalized', staging_digest = ?,
                        final_ref = ?, updated_at = ?
                    WHERE grant_id = ? AND state = 'issued'
                    """,
                    (staging_digest, final_ref, current, grant_id),
                )
                if cursor.rowcount != 1:
                    raise RemoteArtifactGrantUnavailable(
                        "write_grant_unavailable"
                    )
                finalized = RemoteArtifactWriteGrantRecord(
                    grant_id=record.grant_id,
                    token_digest=record.token_digest,
                    grant_metadata=record.grant_metadata,
                    state="finalized",
                    staging_digest=staging_digest,
                    final_ref=final_ref,
                    expires_at=record.expires_at,
                    updated_at=current,
                )
        if expired:
            raise RemoteArtifactGrantUnavailable("write_grant_unavailable")
        if finalized is None:
            raise RemoteExecutionJournalError(
                "invalid_artifact_grant_record"
            )
        return finalized

    def fail_write_grant(
        self,
        *,
        grant_id: str,
        token_digest: str,
        now: float,
    ) -> bool:
        _identifier(grant_id, "grant_id")
        _digest(token_digest, "token_digest")
        current = _finite_time(now)
        with self._transaction() as connection:
            record = self._write_grant_locked(connection, grant_id)
            if record is None:
                return False
            if not hmac.compare_digest(
                record.token_digest,
                token_digest,
            ):
                raise RemoteArtifactGrantConflict(
                    "write_grant_binding_mismatch"
                )
            if record.state == "finalized":
                return False
            if record.state == "failed":
                raise RemoteArtifactGrantUnavailable(
                    "write_grant_unavailable"
                )
            if current >= record.expires_at:
                connection.execute(
                    """
                    DELETE FROM remote_artifact_write_grants
                    WHERE grant_id = ?
                    """,
                    (grant_id,),
                )
                return True
            connection.execute(
                """
                UPDATE remote_artifact_write_grants
                SET state = 'failed', staging_digest = NULL,
                    final_ref = NULL, updated_at = ?
                WHERE grant_id = ? AND state = 'issued'
                """,
                (current, grant_id),
            )
            return True

    def purge_expired_artifact_grants(self, *, now: float) -> int:
        current = _finite_time(now)
        with self._transaction() as connection:
            return self._delete_expired_grants_locked(
                connection,
                now=current,
            )

    def record_provider_grant(
        self,
        record: RemoteProviderGrantRecord,
    ) -> RemoteProviderGrantRecord:
        if not isinstance(record, RemoteProviderGrantRecord):
            raise TypeError(
                "record must be a RemoteProviderGrantRecord"
            )
        if record.state != "issued" or record.updated_at >= record.expires_at:
            raise RemoteExecutionJournalError(
                "invalid_provider_grant_state"
            )
        with self._transaction() as connection:
            self._require_provider_time_floor_locked(
                connection,
                now=record.updated_at,
            )
            existing = self._provider_grant_locked(
                connection,
                record.grant_id,
            )
            if existing is not None:
                if existing != record:
                    raise RemoteProviderGrantConflict(
                        "provider_grant_conflict"
                    )
                return existing
            logical = self._provider_grant_by_logical_locked(
                connection,
                record.logical_invocation_digest,
            )
            if logical is not None:
                raise RemoteProviderGrantConflict(
                    "provider_invocation_already_issued"
                )
            count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM remote_provider_grants"
                ).fetchone()[0]
            )
            if count >= self.maximum_provider_grants:
                raise RemoteExecutionJournalCapacityError(
                    "provider_grant_capacity"
                )
            connection.execute(
                """
                INSERT INTO remote_provider_grants(
                    grant_id, logical_invocation_digest, token_digest,
                    binding_digest, route_id, state, expires_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(
                    getattr(record, item.name)
                    for item in fields(record)
                ),
            )
            return record

    def get_provider_grant(
        self,
        grant_id: str,
    ) -> RemoteProviderGrantRecord | None:
        _identifier(grant_id, "grant_id")
        try:
            with self._connection() as connection:
                return self._provider_grant_locked(
                    connection,
                    grant_id,
                )
        except sqlite3.Error as exc:
            raise RemoteExecutionJournalError(
                "execution_journal_unavailable"
            ) from exc

    def consume_provider_grant(
        self,
        *,
        grant_id: str,
        token_digest: str,
        binding_digest: str,
        route_id: str,
        expires_at: float,
        now: float,
    ) -> RemoteProviderGrantRecord:
        _identifier(grant_id, "grant_id")
        _digest(token_digest, "token_digest")
        _digest(binding_digest, "binding_digest")
        _identifier(route_id, "route_id")
        expected_expires_at = _finite_time(expires_at)
        current = _finite_time(now)
        consumed: RemoteProviderGrantRecord | None = None
        expired = False
        with self._transaction() as connection:
            self._require_provider_time_floor_locked(
                connection,
                now=current,
            )
            record = self._provider_grant_locked(
                connection,
                grant_id,
            )
            if record is None:
                raise RemoteProviderGrantUnavailable(
                    "provider_grant_unavailable"
                )
            if current >= record.expires_at:
                expired = True
            elif (
                record.state != "issued"
                or record.route_id != route_id
                or record.binding_digest != binding_digest
                or record.expires_at != expected_expires_at
                or not hmac.compare_digest(
                    record.token_digest,
                    token_digest,
                )
            ):
                raise RemoteProviderGrantUnavailable(
                    "provider_grant_unavailable"
                )
            else:
                cursor = connection.execute(
                    """
                    UPDATE remote_provider_grants
                    SET state = 'consumed', updated_at = ?
                    WHERE grant_id = ? AND state = 'issued'
                    """,
                    (current, grant_id),
                )
                if cursor.rowcount != 1:
                    raise RemoteProviderGrantUnavailable(
                        "provider_grant_unavailable"
                    )
                consumed = RemoteProviderGrantRecord(
                    grant_id=record.grant_id,
                    logical_invocation_digest=(
                        record.logical_invocation_digest
                    ),
                    token_digest=record.token_digest,
                    binding_digest=record.binding_digest,
                    route_id=record.route_id,
                    state="consumed",
                    expires_at=record.expires_at,
                    updated_at=current,
                )
        if expired:
            raise RemoteProviderGrantUnavailable(
                "provider_grant_unavailable"
            )
        if consumed is None:
            raise RemoteExecutionJournalError(
                "invalid_provider_grant_record"
            )
        return consumed

    def claim_provider_invocation(
        self,
        *,
        grant_id: str,
        token_digest: str,
        binding_digest: str,
        route_id: str,
        expires_at: float,
        request_payload_digest: str,
        now: float,
    ) -> RemoteProviderInvocationClaim:
        """Atomically consume a grant or recover its completed receipt."""

        _identifier(grant_id, "grant_id")
        _digest(token_digest, "token_digest")
        _digest(binding_digest, "binding_digest")
        _identifier(route_id, "route_id")
        expected_expires_at = _finite_time(expires_at)
        _digest(request_payload_digest, "request_payload_digest")
        current = _finite_time(now)
        with self._transaction() as connection:
            self._require_provider_time_floor_locked(
                connection,
                now=current,
            )
            grant = self._provider_grant_locked(
                connection,
                grant_id,
            )
            if (
                grant is None
                or current >= grant.expires_at
                or grant.route_id != route_id
                or grant.binding_digest != binding_digest
                or grant.expires_at != expected_expires_at
                or not hmac.compare_digest(
                    grant.token_digest,
                    token_digest,
                )
            ):
                raise RemoteProviderGrantUnavailable(
                    "provider_grant_unavailable"
                )
            invocation = self._provider_invocation_locked(
                connection,
                grant_id,
            )
            if grant.state == "issued":
                if invocation is not None:
                    raise RemoteProviderInvocationConflict(
                        "provider_invocation_state_conflict"
                    )
                cursor = connection.execute(
                    """
                    UPDATE remote_provider_grants
                    SET state = 'consumed', updated_at = ?
                    WHERE grant_id = ? AND state = 'issued'
                    """,
                    (current, grant_id),
                )
                if cursor.rowcount != 1:
                    raise RemoteProviderGrantUnavailable(
                        "provider_grant_unavailable"
                    )
                claimed = RemoteProviderInvocationRecord(
                    grant_id=grant_id,
                    request_payload_digest=request_payload_digest,
                    state="invoking",
                    response_digest=None,
                    response_artifact_ref=None,
                    updated_at=current,
                )
                connection.execute(
                    """
                    INSERT INTO remote_provider_invocations(
                        grant_id, request_payload_digest, state,
                        response_digest, response_artifact_ref, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    tuple(
                        getattr(claimed, item.name)
                        for item in fields(claimed)
                    ),
                )
                return RemoteProviderInvocationClaim(
                    record=claimed,
                    execute=True,
                )
            if (
                grant.state != "consumed"
                or invocation is None
            ):
                raise RemoteProviderInvocationUnknown(
                    "provider_invocation_outcome_unknown"
                )
            if (
                invocation.request_payload_digest
                != request_payload_digest
            ):
                raise RemoteProviderGrantUnavailable(
                    "provider_grant_unavailable"
                )
            if invocation.state == "completed":
                return RemoteProviderInvocationClaim(
                    record=invocation,
                    execute=False,
                )
            raise RemoteProviderInvocationUnknown(
                "provider_invocation_outcome_unknown"
            )

    def complete_provider_invocation(
        self,
        *,
        grant_id: str,
        token_digest: str,
        binding_digest: str,
        route_id: str,
        expires_at: float,
        request_payload_digest: str,
        response_digest: str,
        response_artifact_ref: str | None,
        now: float,
    ) -> RemoteProviderInvocationRecord:
        """Persist an exact terminal response receipt after Artifact write."""

        _identifier(grant_id, "grant_id")
        _digest(token_digest, "token_digest")
        _digest(binding_digest, "binding_digest")
        _identifier(route_id, "route_id")
        expected_expires_at = _finite_time(expires_at)
        _digest(request_payload_digest, "request_payload_digest")
        _digest(response_digest, "response_digest")
        if response_artifact_ref is not None:
            _canonical_json_text(
                response_artifact_ref,
                "response_artifact_ref",
            )
        current = _finite_time(now)
        with self._transaction() as connection:
            self._require_provider_time_floor_locked(
                connection,
                now=current,
            )
            self._require_provider_grant_binding_locked(
                connection,
                grant_id=grant_id,
                token_digest=token_digest,
                binding_digest=binding_digest,
                route_id=route_id,
                expires_at=expected_expires_at,
                required_state="consumed",
            )
            invocation = self._provider_invocation_locked(
                connection,
                grant_id,
            )
            if (
                invocation is None
                or invocation.request_payload_digest
                != request_payload_digest
            ):
                raise RemoteProviderInvocationConflict(
                    "provider_invocation_binding_mismatch"
                )
            completed = RemoteProviderInvocationRecord(
                grant_id=grant_id,
                request_payload_digest=request_payload_digest,
                state="completed",
                response_digest=response_digest,
                response_artifact_ref=response_artifact_ref,
                updated_at=current,
            )
            if invocation.state == "completed":
                if (
                    invocation.response_digest
                    != completed.response_digest
                    or invocation.response_artifact_ref
                    != completed.response_artifact_ref
                ):
                    raise RemoteProviderInvocationConflict(
                        "provider_invocation_result_conflict"
                    )
                return invocation
            if invocation.state != "invoking":
                raise RemoteProviderInvocationUnknown(
                    "provider_invocation_outcome_unknown"
                )
            cursor = connection.execute(
                """
                UPDATE remote_provider_invocations
                SET state = 'completed', response_digest = ?,
                    response_artifact_ref = ?, updated_at = ?
                WHERE grant_id = ? AND state = 'invoking'
                    AND request_payload_digest = ?
                """,
                (
                    response_digest,
                    response_artifact_ref,
                    current,
                    grant_id,
                    request_payload_digest,
                ),
            )
            if cursor.rowcount != 1:
                raise RemoteProviderInvocationConflict(
                    "provider_invocation_state_conflict"
                )
            return completed

    def mark_provider_invocation_unknown(
        self,
        *,
        grant_id: str,
        token_digest: str,
        binding_digest: str,
        route_id: str,
        expires_at: float,
        request_payload_digest: str,
        now: float,
    ) -> RemoteProviderInvocationRecord:
        """Irreversibly record a call whose upstream outcome is uncertain."""

        _identifier(grant_id, "grant_id")
        _digest(token_digest, "token_digest")
        _digest(binding_digest, "binding_digest")
        _identifier(route_id, "route_id")
        expected_expires_at = _finite_time(expires_at)
        _digest(request_payload_digest, "request_payload_digest")
        current = _finite_time(now)
        with self._transaction() as connection:
            self._require_provider_time_floor_locked(
                connection,
                now=current,
            )
            self._require_provider_grant_binding_locked(
                connection,
                grant_id=grant_id,
                token_digest=token_digest,
                binding_digest=binding_digest,
                route_id=route_id,
                expires_at=expected_expires_at,
                required_state="consumed",
            )
            invocation = self._provider_invocation_locked(
                connection,
                grant_id,
            )
            if (
                invocation is None
                or invocation.request_payload_digest
                != request_payload_digest
            ):
                raise RemoteProviderInvocationConflict(
                    "provider_invocation_binding_mismatch"
                )
            if invocation.state in {"completed", "outcome_unknown"}:
                return invocation
            cursor = connection.execute(
                """
                UPDATE remote_provider_invocations
                SET state = 'outcome_unknown', updated_at = ?
                WHERE grant_id = ? AND state = 'invoking'
                    AND request_payload_digest = ?
                """,
                (current, grant_id, request_payload_digest),
            )
            if cursor.rowcount != 1:
                raise RemoteProviderInvocationConflict(
                    "provider_invocation_state_conflict"
                )
            return RemoteProviderInvocationRecord(
                grant_id=grant_id,
                request_payload_digest=request_payload_digest,
                state="outcome_unknown",
                response_digest=None,
                response_artifact_ref=None,
                updated_at=current,
            )

    def get_provider_invocation(
        self,
        grant_id: str,
    ) -> RemoteProviderInvocationRecord | None:
        _identifier(grant_id, "grant_id")
        try:
            with self._connection() as connection:
                return self._provider_invocation_locked(
                    connection,
                    grant_id,
                )
        except sqlite3.Error as exc:
            raise RemoteExecutionJournalError(
                "execution_journal_unavailable"
            ) from exc

    def purge_expired_provider_grants(self, *, now: float) -> int:
        current = _finite_time(now)
        with self._transaction() as connection:
            self._require_provider_time_floor_locked(
                connection,
                now=current,
            )
            deleted = self._delete_expired_provider_grants_locked(
                connection,
                now=current,
            )
            cursor = connection.execute(
                """
                UPDATE remote_provider_grant_clock
                SET purge_watermark = ?
                WHERE singleton = 1
                """,
                (current,),
            )
            if cursor.rowcount != 1:
                raise RemoteExecutionJournalError(
                    "invalid_provider_grant_clock"
                )
            return deleted

    def _require_artifact_capacity_locked(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        count = int(
            connection.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM remote_artifact_read_grants)
                    +
                    (SELECT COUNT(*) FROM remote_artifact_write_grants)
                """
            ).fetchone()[0]
        )
        if count >= self.maximum_artifact_grants:
            raise RemoteExecutionJournalCapacityError(
                "artifact_grant_capacity"
            )

    @staticmethod
    def _delete_expired_grants_locked(
        connection: sqlite3.Connection,
        *,
        now: float,
    ) -> int:
        read_cursor = connection.execute(
            """
            DELETE FROM remote_artifact_read_grants
            WHERE expires_at <= ?
            """,
            (now,),
        )
        write_cursor = connection.execute(
            """
            DELETE FROM remote_artifact_write_grants
            WHERE expires_at <= ?
            """,
            (now,),
        )
        return read_cursor.rowcount + write_cursor.rowcount

    @staticmethod
    def _delete_expired_provider_grants_locked(
        connection: sqlite3.Connection,
        *,
        now: float,
    ) -> int:
        connection.execute(
            """
            DELETE FROM remote_provider_invocations
            WHERE grant_id IN (
                SELECT grant_id
                FROM remote_provider_grants
                WHERE expires_at <= ?
            )
            """,
            (now,),
        )
        cursor = connection.execute(
            """
            DELETE FROM remote_provider_grants
            WHERE expires_at <= ?
            """,
            (now,),
        )
        return cursor.rowcount

    @staticmethod
    def _require_provider_time_floor_locked(
        connection: sqlite3.Connection,
        *,
        now: float,
    ) -> None:
        row = connection.execute(
            """
            SELECT purge_watermark
            FROM remote_provider_grant_clock
            WHERE singleton = 1
            """
        ).fetchone()
        if row is None:
            raise RemoteExecutionJournalError(
                "invalid_provider_grant_clock"
            )
        watermark = row["purge_watermark"]
        if (
            isinstance(watermark, bool)
            or not isinstance(watermark, (int, float))
            or not math.isfinite(float(watermark))
            or float(watermark) < 0
        ):
            raise RemoteExecutionJournalError(
                "invalid_provider_grant_clock"
            )
        if now < float(watermark):
            raise RemoteExecutionJournalError(
                "provider_grant_clock_rollback"
            )

    @classmethod
    def _require_provider_grant_binding_locked(
        cls,
        connection: sqlite3.Connection,
        *,
        grant_id: str,
        token_digest: str,
        binding_digest: str,
        route_id: str,
        expires_at: float,
        required_state: str,
    ) -> RemoteProviderGrantRecord:
        grant = cls._provider_grant_locked(connection, grant_id)
        if (
            grant is None
            or grant.state != required_state
            or grant.route_id != route_id
            or grant.binding_digest != binding_digest
            or grant.expires_at != expires_at
            or not hmac.compare_digest(
                grant.token_digest,
                token_digest,
            )
        ):
            raise RemoteProviderGrantUnavailable(
                "provider_grant_unavailable"
            )
        return grant

    @staticmethod
    def _read_grant_locked(
        connection: sqlite3.Connection,
        grant_id: str,
    ) -> RemoteArtifactReadGrantRecord | None:
        row = connection.execute(
            """
            SELECT *
            FROM remote_artifact_read_grants
            WHERE grant_id = ?
            """,
            (grant_id,),
        ).fetchone()
        return None if row is None else _read_grant_from_row(row)

    @staticmethod
    def _write_grant_locked(
        connection: sqlite3.Connection,
        grant_id: str,
    ) -> RemoteArtifactWriteGrantRecord | None:
        row = connection.execute(
            """
            SELECT *
            FROM remote_artifact_write_grants
            WHERE grant_id = ?
            """,
            (grant_id,),
        ).fetchone()
        return None if row is None else _write_grant_from_row(row)

    @staticmethod
    def _provider_grant_locked(
        connection: sqlite3.Connection,
        grant_id: str,
    ) -> RemoteProviderGrantRecord | None:
        row = connection.execute(
            """
            SELECT *
            FROM remote_provider_grants
            WHERE grant_id = ?
            """,
            (grant_id,),
        ).fetchone()
        return None if row is None else _provider_grant_from_row(row)

    @staticmethod
    def _provider_grant_by_logical_locked(
        connection: sqlite3.Connection,
        logical_invocation_digest: str,
    ) -> RemoteProviderGrantRecord | None:
        row = connection.execute(
            """
            SELECT *
            FROM remote_provider_grants
            WHERE logical_invocation_digest = ?
            """,
            (logical_invocation_digest,),
        ).fetchone()
        return None if row is None else _provider_grant_from_row(row)

    @staticmethod
    def _provider_invocation_locked(
        connection: sqlite3.Connection,
        grant_id: str,
    ) -> RemoteProviderInvocationRecord | None:
        row = connection.execute(
            """
            SELECT *
            FROM remote_provider_invocations
            WHERE grant_id = ?
            """,
            (grant_id,),
        ).fetchone()
        return (
            None
            if row is None
            else _provider_invocation_from_row(row)
        )

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
            existing_tables = self._table_names(connection)
            is_new = not existing_tables and self._bootstrap_new_database
            if not existing_tables and not is_new:
                raise RemoteExecutionJournalError("invalid_execution_schema")
            if is_new:
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
                self._validate_schema(
                    connection,
                    _SCHEMA_COLUMNS,
                    schema_indexes=_SCHEMA_INDEXES,
                )
                return

            if existing_tables == frozenset(_V1_SCHEMA_COLUMNS):
                self._validate_schema(connection, _V1_SCHEMA_COLUMNS)
                self._require_schema_version(connection, expected=1)
                for statement in _V1_UPGRADE_SCHEMA_STATEMENTS:
                    connection.execute(statement)
                cursor = connection.execute(
                    """
                    UPDATE remote_execution_journal_metadata
                    SET schema_version = ?
                    WHERE singleton = 1 AND schema_version = 1
                    """,
                    (REMOTE_EXECUTION_JOURNAL_SCHEMA_VERSION,),
                )
                if cursor.rowcount != 1:
                    raise RemoteExecutionJournalError(
                        "unsupported_execution_schema"
                    )
                self._validate_schema(
                    connection,
                    _SCHEMA_COLUMNS,
                    schema_indexes=_SCHEMA_INDEXES,
                )
                return

            if existing_tables == frozenset(_V2_SCHEMA_COLUMNS):
                self._validate_schema(
                    connection,
                    _V2_SCHEMA_COLUMNS,
                    schema_indexes=_V2_SCHEMA_INDEXES,
                )
                self._require_schema_version(connection, expected=2)
                for statement in _V2_UPGRADE_SCHEMA_STATEMENTS:
                    connection.execute(statement)
                cursor = connection.execute(
                    """
                    UPDATE remote_execution_journal_metadata
                    SET schema_version = ?
                    WHERE singleton = 1 AND schema_version = 2
                    """,
                    (REMOTE_EXECUTION_JOURNAL_SCHEMA_VERSION,),
                )
                if cursor.rowcount != 1:
                    raise RemoteExecutionJournalError(
                        "unsupported_execution_schema"
                    )
                self._validate_schema(
                    connection,
                    _SCHEMA_COLUMNS,
                    schema_indexes=_SCHEMA_INDEXES,
                )
                return

            if existing_tables == frozenset(_V3_SCHEMA_COLUMNS):
                self._validate_schema(
                    connection,
                    _V3_SCHEMA_COLUMNS,
                    schema_indexes=_V3_SCHEMA_INDEXES,
                )
                self._require_schema_version(connection, expected=3)
                for statement in _V3_UPGRADE_SCHEMA_STATEMENTS:
                    connection.execute(statement)
                cursor = connection.execute(
                    """
                    UPDATE remote_execution_journal_metadata
                    SET schema_version = ?
                    WHERE singleton = 1 AND schema_version = 3
                    """,
                    (REMOTE_EXECUTION_JOURNAL_SCHEMA_VERSION,),
                )
                if cursor.rowcount != 1:
                    raise RemoteExecutionJournalError(
                        "unsupported_execution_schema"
                    )
                self._validate_schema(
                    connection,
                    _SCHEMA_COLUMNS,
                    schema_indexes=_SCHEMA_INDEXES,
                )
                return

            if existing_tables != frozenset(_SCHEMA_COLUMNS):
                raise RemoteExecutionJournalError("invalid_execution_schema")
            self._validate_schema(
                connection,
                _SCHEMA_COLUMNS,
                schema_indexes=_SCHEMA_INDEXES,
            )
            self._require_schema_version(
                connection,
                expected=REMOTE_EXECUTION_JOURNAL_SCHEMA_VERSION,
            )

    @staticmethod
    def _require_schema_version(
        connection: sqlite3.Connection,
        *,
        expected: int,
    ) -> None:
        row = connection.execute(
            """
            SELECT schema_version
            FROM remote_execution_journal_metadata
            WHERE singleton = 1
            """
        ).fetchone()
        if row is None:
            raise RemoteExecutionJournalError("invalid_execution_schema")
        version = row["schema_version"]
        if (
            isinstance(version, bool)
            or not isinstance(version, int)
            or version != expected
        ):
            raise RemoteExecutionJournalError(
                "unsupported_execution_schema"
            )

    @staticmethod
    def _table_names(connection: sqlite3.Connection) -> frozenset[str]:
        return frozenset(
            str(row["name"])
            for row in connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                """
            )
        )

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
                self._validate_schema(
                    connection,
                    _SCHEMA_COLUMNS,
                    schema_indexes=_SCHEMA_INDEXES,
                )
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
            tables = self._table_names(connection)
            if tables == frozenset(_V1_SCHEMA_COLUMNS):
                self._validate_schema(connection, _V1_SCHEMA_COLUMNS)
                self._require_schema_version(connection, expected=1)
            elif tables == frozenset(_V2_SCHEMA_COLUMNS):
                self._validate_schema(
                    connection,
                    _V2_SCHEMA_COLUMNS,
                    schema_indexes=_V2_SCHEMA_INDEXES,
                )
                self._require_schema_version(
                    connection,
                    expected=2,
                )
            elif tables == frozenset(_V3_SCHEMA_COLUMNS):
                self._validate_schema(
                    connection,
                    _V3_SCHEMA_COLUMNS,
                    schema_indexes=_V3_SCHEMA_INDEXES,
                )
                self._require_schema_version(
                    connection,
                    expected=3,
                )
            elif tables == frozenset(_SCHEMA_COLUMNS):
                self._validate_schema(
                    connection,
                    _SCHEMA_COLUMNS,
                    schema_indexes=_SCHEMA_INDEXES,
                )
                self._require_schema_version(
                    connection,
                    expected=REMOTE_EXECUTION_JOURNAL_SCHEMA_VERSION,
                )
            else:
                raise RemoteExecutionJournalError(
                    "invalid_execution_schema"
                )
            mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()
            if mode is None or str(mode[0]).lower() != "wal":
                raise RemoteExecutionJournalError(
                    "execution_journal_unavailable"
                )
        finally:
            connection.close()

    @staticmethod
    def _validate_schema(
        connection: sqlite3.Connection,
        schema_columns: dict[
            str,
            tuple[tuple[str, str, int, int], ...],
        ],
        *,
        schema_indexes: dict[
            str,
            tuple[str, int, tuple[str, ...]],
        ] | None = None,
    ) -> None:
        quick_check = connection.execute("PRAGMA quick_check(1)").fetchone()
        if quick_check is None or str(quick_check[0]).lower() != "ok":
            raise RemoteExecutionJournalError(
                "execution_journal_integrity_failed"
            )
        for table, expected_columns in schema_columns.items():
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
        expected_indexes = schema_indexes or {}
        auxiliary = {
            str(row["name"]): (
                str(row["type"]),
                str(row["tbl_name"]),
            )
            for row in connection.execute(
                """
                SELECT type, name, tbl_name
                FROM sqlite_master
                WHERE type IN ('index', 'trigger', 'view')
                    AND name NOT LIKE 'sqlite_%'
                """
            )
        }
        if set(auxiliary) != set(expected_indexes):
            raise RemoteExecutionJournalError("invalid_execution_schema")
        for index_name, (
            expected_table,
            expected_unique,
            expected_index_columns,
        ) in expected_indexes.items():
            if auxiliary[index_name] != ("index", expected_table):
                raise RemoteExecutionJournalError(
                    "invalid_execution_schema"
                )
        if "remote_provider_grant_clock" in schema_columns:
            clock_rows = connection.execute(
                """
                SELECT singleton, purge_watermark
                FROM remote_provider_grant_clock
                """
            ).fetchall()
            if len(clock_rows) != 1:
                raise RemoteExecutionJournalError(
                    "invalid_execution_schema"
                )
        if "remote_provider_invocations" in schema_columns:
            invocation_rows = connection.execute(
                """
                SELECT invocation.*, grant.state AS grant_state
                FROM remote_provider_invocations AS invocation
                LEFT JOIN remote_provider_grants AS grant
                    ON grant.grant_id = invocation.grant_id
                """
            ).fetchall()
            for invocation_row in invocation_rows:
                invalid_invocation = (
                    invocation_row["grant_state"] != "consumed"
                )
                if not invalid_invocation:
                    try:
                        _provider_invocation_from_row(invocation_row)
                    except RemoteExecutionJournalError:
                        invalid_invocation = True
                if invalid_invocation:
                    raise RemoteExecutionJournalError(
                        "invalid_execution_schema"
                    )
            clock_row = clock_rows[0]
            watermark = clock_row["purge_watermark"]
            if (
                clock_row["singleton"] != 1
                or isinstance(watermark, bool)
                or not isinstance(watermark, (int, float))
                or not math.isfinite(float(watermark))
                or float(watermark) < 0
            ):
                raise RemoteExecutionJournalError(
                    "invalid_execution_schema"
                )
            index_list = {
                str(row["name"]): (
                    int(row["unique"]),
                    str(row["origin"]),
                    int(row["partial"]),
                )
                for row in connection.execute(
                    f"PRAGMA index_list({expected_table})"
                )
            }
            actual_columns = tuple(
                str(row["name"])
                for row in connection.execute(
                    f"PRAGMA index_info({index_name})"
                )
            )
            if (
                index_list.get(index_name)
                != (expected_unique, "c", 0)
                or actual_columns != expected_index_columns
            ):
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


def _read_grant_from_row(
    row: sqlite3.Row,
) -> RemoteArtifactReadGrantRecord:
    try:
        return RemoteArtifactReadGrantRecord(
            **{
                item.name: row[item.name]
                for item in fields(RemoteArtifactReadGrantRecord)
            }
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RemoteExecutionJournalError(
            "invalid_artifact_grant_record"
        ) from exc


def _write_grant_from_row(
    row: sqlite3.Row,
) -> RemoteArtifactWriteGrantRecord:
    try:
        return RemoteArtifactWriteGrantRecord(
            **{
                item.name: row[item.name]
                for item in fields(RemoteArtifactWriteGrantRecord)
            }
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RemoteExecutionJournalError(
            "invalid_artifact_grant_record"
        ) from exc


def _provider_grant_from_row(
    row: sqlite3.Row,
) -> RemoteProviderGrantRecord:
    try:
        return RemoteProviderGrantRecord(
            **{
                item.name: row[item.name]
                for item in fields(RemoteProviderGrantRecord)
            }
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RemoteExecutionJournalError(
            "invalid_provider_grant_record"
        ) from exc


def _provider_invocation_from_row(
    row: sqlite3.Row,
) -> RemoteProviderInvocationRecord:
    try:
        return RemoteProviderInvocationRecord(
            **{
                item.name: row[item.name]
                for item in fields(RemoteProviderInvocationRecord)
            }
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RemoteExecutionJournalError(
            "invalid_provider_invocation_record"
        ) from exc


def _canonical_json_text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise RemoteExecutionJournalError(f"invalid_{name}")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError as exc:
        raise RemoteExecutionJournalError(f"invalid_{name}") from exc
    if not encoded or len(encoded) > MAX_ARTIFACT_GRANT_METADATA_BYTES:
        raise RemoteExecutionJournalError(f"invalid_{name}")
    try:
        decoded = json.loads(
            value,
            parse_constant=_reject_json_constant,
        )
        canonical = json.dumps(
            decoded,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError, RecursionError) as exc:
        raise RemoteExecutionJournalError(f"invalid_{name}") from exc
    if not isinstance(decoded, dict) or canonical != value:
        raise RemoteExecutionJournalError(f"invalid_{name}")
    return value


def _reject_json_constant(constant: str) -> object:
    raise ValueError(f"invalid JSON constant: {constant}")


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise RemoteExecutionJournalError(f"invalid_{name}")
    return value


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise RemoteExecutionJournalError(f"invalid_{name}")
    return value


def _finite_time(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RemoteExecutionJournalError("invalid_created_at")
    timestamp = float(value)
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
    "MAX_ARTIFACT_GRANT_JOURNAL_RECORDS",
    "MAX_EXECUTION_JOURNAL_RECORDS",
    "MAX_PROVIDER_GRANT_JOURNAL_RECORDS",
    "REMOTE_EXECUTION_JOURNAL_SCHEMA_VERSION",
    "RemoteArtifactGrantConflict",
    "RemoteArtifactGrantUnavailable",
    "RemoteArtifactReadGrantRecord",
    "RemoteArtifactWriteGrantRecord",
    "RemoteExecutionBindingRecord",
    "RemoteExecutionJournal",
    "RemoteExecutionJournalCapacityError",
    "RemoteExecutionJournalConflict",
    "RemoteExecutionJournalError",
    "RemoteProviderGrantConflict",
    "RemoteProviderGrantRecord",
    "RemoteProviderGrantUnavailable",
    "RemoteProviderInvocationClaim",
    "RemoteProviderInvocationConflict",
    "RemoteProviderInvocationRecord",
    "RemoteProviderInvocationUnknown",
]
