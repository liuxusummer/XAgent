from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

from src.orchestration.remote_execution_journal import (
    MAX_PROVIDER_RECOVERY_EVIDENCE_PER_INVOCATION,
    RemoteArtifactGrantConflict,
    RemoteArtifactGrantUnavailable,
    RemoteArtifactReadGrantRecord,
    RemoteArtifactWriteGrantRecord,
    RemoteExecutionBindingRecord,
    RemoteExecutionJournal,
    RemoteExecutionJournalCapacityError,
    RemoteExecutionJournalConflict,
    RemoteExecutionJournalError,
    RemoteProviderGrantRecord,
    RemoteProviderGrantUnavailable,
    RemoteProviderInvocationConflict,
    RemoteProviderInvocationUnknown,
)


def _canonical(payload: dict[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _record(
    *,
    run_id: str = "run-1",
    attempt_id: str = "attempt-1",
    fencing_token: int = 1,
) -> RemoteExecutionBindingRecord:
    return RemoteExecutionBindingRecord(
        run_id=run_id,
        attempt_id=attempt_id,
        node_id="node-1",
        worker_id="worker-1",
        tenant_id="tenant-1",
        identity_digest="1" * 64,
        session_binding_digest="2" * 64,
        claim_token_digest="3" * 64,
        fencing_token=fencing_token,
        action_digest="4" * 64,
        authorization_digest="5" * 64,
        profile_digest="6" * 64,
        request_digest="7" * 64,
        grant_binding_digest="8" * 64,
        execution_plan_digest="9" * 64,
        runtime_attestation_digest="a" * 64,
        runtime_verifier_id="runtime-verifier",
        runtime_security_level="container",
        created_at=100.0,
    )


def _provider_record(
    *,
    grant_id: str = "provider-grant-1",
    logical_digest: str = "a" * 64,
    expires_at: float = 200.0,
    updated_at: float = 100.0,
) -> RemoteProviderGrantRecord:
    return RemoteProviderGrantRecord(
        grant_id=grant_id,
        logical_invocation_digest=logical_digest,
        token_digest="b" * 64,
        binding_digest="c" * 64,
        route_id="primary-route",
        state="issued",
        expires_at=expires_at,
        updated_at=updated_at,
    )


class RemoteExecutionJournalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "execution.sqlite3"

    def test_restart_recovers_exact_digest_only_binding(self) -> None:
        journal = RemoteExecutionJournal(self.path)
        expected = _record()
        self.assertEqual(journal.record(expected), expected)
        journal.close()

        restarted = RemoteExecutionJournal(self.path)
        self.assertEqual(restarted.get("run-1", "attempt-1", 1), expected)
        payload = self.path.read_bytes()
        self.assertNotIn(b"claim-token", payload)
        self.assertNotIn(b"bearer", payload)
        self.assertNotIn(b"script", payload)

    def test_same_generation_is_idempotent_but_rebinding_conflicts(self) -> None:
        journal = RemoteExecutionJournal(self.path)
        expected = _record()
        self.assertEqual(journal.record(expected), expected)
        self.assertEqual(
            journal.record(replace(expected, created_at=200.0)),
            expected,
        )
        with self.assertRaisesRegex(
            RemoteExecutionJournalConflict,
            "execution_binding_conflict",
        ):
            journal.record(
                replace(expected, action_digest="b" * 64)
            )

        next_generation = replace(
            expected,
            fencing_token=2,
            authorization_digest="c" * 64,
        )
        self.assertEqual(journal.record(next_generation), next_generation)
        self.assertEqual(
            journal.get("run-1", "attempt-1", 1),
            expected,
        )
        self.assertEqual(
            journal.get("run-1", "attempt-1", 2),
            next_generation,
        )

    def test_concurrent_writers_publish_one_exact_record(self) -> None:
        RemoteExecutionJournal(self.path).close()
        errors: list[BaseException] = []
        barrier = threading.Barrier(8)

        def write() -> None:
            try:
                journal = RemoteExecutionJournal(self.path)
                barrier.wait(timeout=5)
                journal.record(_record())
                journal.close()
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=write) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        journal = RemoteExecutionJournal(self.path)
        self.assertEqual(journal.list_run("run-1"), (_record(),))

    def test_capacity_never_evicts_live_recovery_evidence(self) -> None:
        journal = RemoteExecutionJournal(self.path, maximum_records=1)
        journal.record(_record())
        with self.assertRaisesRegex(
            RemoteExecutionJournalCapacityError,
            "execution_binding_capacity",
        ):
            journal.record(_record(attempt_id="attempt-2"))
        self.assertEqual(
            journal.get("run-1", "attempt-1", 1),
            _record(),
        )
        self.assertTrue(journal.discard("run-1", "attempt-1", 1))
        journal.record(_record(attempt_id="attempt-2"))

    def test_partial_or_unknown_schema_fails_closed(self) -> None:
        connection = sqlite3.connect(self.path)
        connection.execute("CREATE TABLE attacker_owned(value TEXT)")
        connection.commit()
        connection.close()
        os.chmod(self.path, 0o600)
        with self.assertRaisesRegex(
            RemoteExecutionJournalError,
            "invalid_execution_schema",
        ):
            RemoteExecutionJournal(self.path)

    def test_insecure_database_mode_and_symlink_lock_fail_closed(self) -> None:
        RemoteExecutionJournal(self.path).close()
        os.chmod(self.path, 0o644)
        with self.assertRaisesRegex(
            RemoteExecutionJournalError,
            "execution_journal_permissions",
        ):
            RemoteExecutionJournal(self.path)

        other_path = Path(self.temporary.name) / "other.sqlite3"
        lock_path = (
            other_path.parent / f".{other_path.name}.bootstrap.lock"
        )
        lock_path.symlink_to(self.path)
        with self.assertRaisesRegex(
            RemoteExecutionJournalError,
            "execution_journal_unavailable",
        ):
            RemoteExecutionJournal(other_path)

    def test_read_grant_is_consumed_once_across_process_registries(self) -> None:
        token = "read-bearer-canary"
        digest = hashlib.sha256(token.encode()).hexdigest()
        metadata = _canonical({"grant_id": "read-grant-1"})
        artifact_ref = _canonical({"sha256": "a" * 64})
        first = RemoteExecutionJournal(self.path)
        first.record_read_grant(
            RemoteArtifactReadGrantRecord(
                grant_id="read-grant-1",
                token_digest=digest,
                grant_metadata=metadata,
                artifact_ref=artifact_ref,
                state="issued",
                expires_at=200.0,
                updated_at=100.0,
            )
        )
        second = RemoteExecutionJournal(self.path)

        def consume(journal: RemoteExecutionJournal) -> str:
            try:
                journal.consume_read_grant(
                    grant_id="read-grant-1",
                    token_digest=digest,
                    grant_metadata=metadata,
                    expires_at=200.0,
                    now=101.0,
                )
            except RemoteArtifactGrantUnavailable:
                return "unavailable"
            return "consumed"

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(consume, (first, second)))
        self.assertEqual(outcomes.count("consumed"), 1)
        self.assertEqual(outcomes.count("unavailable"), 1)
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(f"{self.path}{suffix}")
            if candidate.exists():
                self.assertNotIn(token.encode(), candidate.read_bytes())

    def test_write_finalization_replays_exact_ref_after_restart(self) -> None:
        token = "write-bearer-canary"
        digest = hashlib.sha256(token.encode()).hexdigest()
        metadata = _canonical({"grant_id": "write-grant-1"})
        final_ref = _canonical(
            {"artifact_id": "artifact-1", "sha256": "b" * 64}
        )
        journal = RemoteExecutionJournal(self.path)
        journal.record_write_grant(
            RemoteArtifactWriteGrantRecord(
                grant_id="write-grant-1",
                token_digest=digest,
                grant_metadata=metadata,
                state="issued",
                staging_digest=None,
                final_ref=None,
                expires_at=200.0,
                updated_at=100.0,
            )
        )
        expected = journal.finalize_write_grant(
            grant_id="write-grant-1",
            token_digest=digest,
            grant_metadata=metadata,
            staging_digest="c" * 64,
            final_ref=final_ref,
            now=101.0,
        )
        restarted = RemoteExecutionJournal(self.path)
        self.assertEqual(
            restarted.get_write_grant(
                grant_id="write-grant-1",
                token_digest=digest,
                now=102.0,
            ),
            expected,
        )
        self.assertEqual(
            restarted.finalize_write_grant(
                grant_id="write-grant-1",
                token_digest=digest,
                grant_metadata=metadata,
                staging_digest="c" * 64,
                final_ref=final_ref,
                now=103.0,
            ),
            expected,
        )
        with self.assertRaisesRegex(
            RemoteArtifactGrantConflict,
            "write_finalization_conflict",
        ):
            restarted.finalize_write_grant(
                grant_id="write-grant-1",
                token_digest=digest,
                grant_metadata=metadata,
                staging_digest="d" * 64,
                final_ref=final_ref,
                now=103.0,
            )
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(f"{self.path}{suffix}")
            if candidate.exists():
                self.assertNotIn(token.encode(), candidate.read_bytes())

    def test_artifact_capacity_never_evicts_unexpired_tombstones(self) -> None:
        journal = RemoteExecutionJournal(
            self.path,
            maximum_artifact_grants=1,
        )
        first = RemoteArtifactReadGrantRecord(
            grant_id="read-grant-1",
            token_digest="1" * 64,
            grant_metadata=_canonical({"grant_id": "read-grant-1"}),
            artifact_ref=_canonical({"sha256": "2" * 64}),
            state="issued",
            expires_at=200.0,
            updated_at=100.0,
        )
        journal.record_read_grant(first)
        journal.consume_read_grant(
            grant_id=first.grant_id,
            token_digest=first.token_digest,
            grant_metadata=first.grant_metadata,
            expires_at=first.expires_at,
            now=101.0,
        )
        with self.assertRaisesRegex(
            RemoteExecutionJournalCapacityError,
            "artifact_grant_capacity",
        ):
            journal.record_read_grant(
                replace(
                    first,
                    grant_id="read-grant-2",
                    token_digest="3" * 64,
                    grant_metadata=_canonical(
                        {"grant_id": "read-grant-2"}
                    ),
                    updated_at=102.0,
                )
            )
        self.assertEqual(
            journal.purge_expired_artifact_grants(now=200.0),
            1,
        )
        journal.record_read_grant(
            replace(
                first,
                grant_id="read-grant-2",
                token_digest="3" * 64,
                grant_metadata=_canonical({"grant_id": "read-grant-2"}),
                expires_at=300.0,
                updated_at=201.0,
            )
        )

    def test_provider_tombstone_capacity_and_purge_clock_are_monotonic(
        self,
    ) -> None:
        journal = RemoteExecutionJournal(
            self.path,
            maximum_provider_grants=1,
        )
        first = _provider_record()
        journal.record_provider_grant(first)
        consumed = journal.consume_provider_grant(
            grant_id=first.grant_id,
            token_digest=first.token_digest,
            binding_digest=first.binding_digest,
            route_id=first.route_id,
            expires_at=first.expires_at,
            now=101.0,
        )
        self.assertEqual(consumed.state, "consumed")
        with self.assertRaisesRegex(
            RemoteExecutionJournalCapacityError,
            "provider_grant_capacity",
        ):
            journal.record_provider_grant(
                _provider_record(
                    grant_id="provider-grant-2",
                    logical_digest="d" * 64,
                    updated_at=102.0,
                )
            )
        with self.assertRaises(RemoteProviderGrantUnavailable):
            journal.consume_provider_grant(
                grant_id=first.grant_id,
                token_digest=first.token_digest,
                binding_digest=first.binding_digest,
                route_id=first.route_id,
                expires_at=first.expires_at,
                now=200.0,
            )
        self.assertIsNotNone(
            journal.get_provider_grant(first.grant_id)
        )
        self.assertEqual(
            journal.purge_expired_provider_grants(now=200.0),
            1,
        )
        second = _provider_record(
            grant_id="provider-grant-2",
            logical_digest="d" * 64,
            expires_at=300.0,
            updated_at=199.0,
        )
        with self.assertRaisesRegex(
            RemoteExecutionJournalError,
            "provider_grant_clock_rollback",
        ):
            journal.record_provider_grant(second)
        journal.record_provider_grant(
            replace(second, updated_at=201.0)
        )

    def test_exact_v1_schema_migrates_atomically_to_v5(self) -> None:
        connection = sqlite3.connect(self.path)
        connection.executescript(
            """
            CREATE TABLE remote_execution_journal_metadata(
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                schema_version INTEGER NOT NULL
            );
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
            );
            INSERT INTO remote_execution_journal_metadata(
                singleton, schema_version
            ) VALUES (1, 1);
            """
        )
        connection.execute("PRAGMA journal_mode = WAL")
        connection.close()
        os.chmod(self.path, 0o600)

        migrated = RemoteExecutionJournal(self.path)
        migrated.record(_record())
        connection = sqlite3.connect(self.path)
        version = connection.execute(
            """
            SELECT schema_version
            FROM remote_execution_journal_metadata
            """
        ).fetchone()[0]
        tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                """
            )
        }
        connection.close()
        self.assertEqual(version, 5)
        self.assertIn("remote_artifact_read_grants", tables)
        self.assertIn("remote_artifact_write_grants", tables)
        self.assertIn("remote_provider_grants", tables)
        self.assertIn("remote_provider_grant_clock", tables)
        self.assertIn("remote_provider_invocations", tables)
        self.assertIn("remote_provider_recovery_evidence", tables)

    def test_exact_v2_schema_migrates_atomically_to_v5(self) -> None:
        previous = RemoteArtifactReadGrantRecord(
            grant_id="v2-read-grant",
            token_digest="e" * 64,
            grant_metadata=_canonical(
                {"grant_id": "v2-read-grant"}
            ),
            artifact_ref=_canonical({"sha256": "f" * 64}),
            state="issued",
            expires_at=200.0,
            updated_at=100.0,
        )
        original = RemoteExecutionJournal(self.path)
        original.record_read_grant(previous)
        original.close()
        connection = sqlite3.connect(self.path)
        connection.execute(
            "DROP TABLE remote_provider_recovery_evidence"
        )
        connection.execute("DROP TABLE remote_provider_invocations")
        connection.execute("DROP TABLE remote_provider_grants")
        connection.execute("DROP TABLE remote_provider_grant_clock")
        connection.execute(
            """
            UPDATE remote_execution_journal_metadata
            SET schema_version = 2
            WHERE singleton = 1
            """
        )
        connection.commit()
        connection.close()

        migrated = RemoteExecutionJournal(self.path)
        consumed = migrated.consume_read_grant(
            grant_id=previous.grant_id,
            token_digest=previous.token_digest,
            grant_metadata=previous.grant_metadata,
            expires_at=previous.expires_at,
            now=101.0,
        )
        self.assertEqual(consumed.state, "consumed")
        migrated.close()
        connection = sqlite3.connect(self.path)
        version = connection.execute(
            """
            SELECT schema_version
            FROM remote_execution_journal_metadata
            """
        ).fetchone()[0]
        tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                """
            )
        }
        connection.close()
        self.assertEqual(version, 5)
        self.assertIn("remote_provider_grants", tables)
        self.assertIn("remote_provider_grant_clock", tables)
        self.assertIn("remote_provider_invocations", tables)
        self.assertIn("remote_provider_recovery_evidence", tables)

    def test_exact_v3_schema_migrates_atomically_to_v5(self) -> None:
        original = RemoteExecutionJournal(self.path)
        previous = _provider_record()
        original.record_provider_grant(previous)
        consumed = original.consume_provider_grant(
            grant_id=previous.grant_id,
            token_digest=previous.token_digest,
            binding_digest=previous.binding_digest,
            route_id=previous.route_id,
            expires_at=previous.expires_at,
            now=101.0,
        )
        original.close()
        connection = sqlite3.connect(self.path)
        connection.execute(
            "DROP TABLE remote_provider_recovery_evidence"
        )
        connection.execute("DROP TABLE remote_provider_invocations")
        connection.execute(
            """
            UPDATE remote_execution_journal_metadata
            SET schema_version = 3
            WHERE singleton = 1
            """
        )
        connection.commit()
        connection.close()

        migrated = RemoteExecutionJournal(self.path)
        self.assertEqual(
            migrated.get_provider_grant("provider-grant-1"),
            consumed,
        )
        with self.assertRaises(RemoteProviderInvocationUnknown):
            migrated.claim_provider_invocation(
                grant_id=previous.grant_id,
                token_digest=previous.token_digest,
                binding_digest=previous.binding_digest,
                route_id=previous.route_id,
                expires_at=previous.expires_at,
                request_payload_digest="1" * 64,
                now=102.0,
            )
        migrated.close()
        connection = sqlite3.connect(self.path)
        version = connection.execute(
            """
            SELECT schema_version
            FROM remote_execution_journal_metadata
            """
        ).fetchone()[0]
        tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                """
            )
        }
        connection.close()
        self.assertEqual(version, 5)
        self.assertIn("remote_provider_invocations", tables)
        self.assertIn("remote_provider_recovery_evidence", tables)

    def test_exact_v4_schema_migrates_atomically_to_v5(self) -> None:
        original = RemoteExecutionJournal(self.path)
        previous = _provider_record()
        original.record_provider_grant(previous)
        original.claim_provider_invocation(
            grant_id=previous.grant_id,
            token_digest=previous.token_digest,
            binding_digest=previous.binding_digest,
            route_id=previous.route_id,
            expires_at=previous.expires_at,
            request_payload_digest="1" * 64,
            now=101.0,
        )
        original.complete_provider_invocation(
            grant_id=previous.grant_id,
            token_digest=previous.token_digest,
            binding_digest=previous.binding_digest,
            route_id=previous.route_id,
            expires_at=previous.expires_at,
            request_payload_digest="1" * 64,
            response_digest="2" * 64,
            response_artifact_ref=None,
            now=102.0,
        )
        original.close()
        connection = sqlite3.connect(self.path)
        connection.execute(
            "DROP TABLE remote_provider_recovery_evidence"
        )
        connection.execute(
            """
            UPDATE remote_execution_journal_metadata
            SET schema_version = 4
            WHERE singleton = 1
            """
        )
        connection.commit()
        connection.close()

        migrated = RemoteExecutionJournal(self.path)
        replay = migrated.claim_provider_invocation(
            grant_id=previous.grant_id,
            token_digest=previous.token_digest,
            binding_digest=previous.binding_digest,
            route_id=previous.route_id,
            expires_at=previous.expires_at,
            request_payload_digest="1" * 64,
            now=103.0,
        )
        self.assertFalse(replay.execute)
        self.assertEqual(replay.record.response_digest, "2" * 64)
        self.assertEqual(
            migrated.list_provider_recovery_evidence(
                previous.grant_id
            ),
            (),
        )
        migrated.close()
        connection = sqlite3.connect(self.path)
        version = connection.execute(
            """
            SELECT schema_version
            FROM remote_execution_journal_metadata
            """
        ).fetchone()[0]
        tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                """
            )
        }
        connection.close()
        self.assertEqual(version, 5)
        self.assertIn("remote_provider_recovery_evidence", tables)

    def test_expiry_indexes_exist_and_unexpected_trigger_fails_closed(
        self,
    ) -> None:
        RemoteExecutionJournal(self.path).close()
        connection = sqlite3.connect(self.path)
        indexes = {
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'index' AND name NOT LIKE 'sqlite_%'
                """
            )
        }
        read_plan = " ".join(
            str(column)
            for row in connection.execute(
                """
                EXPLAIN QUERY PLAN
                DELETE FROM remote_artifact_read_grants
                WHERE expires_at <= 100
                """
            )
            for column in row
        )
        self.assertEqual(
            indexes,
            {
                "idx_remote_artifact_read_grants_expires",
                "idx_remote_artifact_write_grants_expires",
                "idx_remote_provider_grants_expires",
                "idx_remote_provider_grants_logical",
            },
        )
        self.assertIn(
            "idx_remote_artifact_read_grants_expires",
            read_plan,
        )
        connection.execute(
            """
            CREATE TRIGGER unexpected_grant_trigger
            AFTER INSERT ON remote_artifact_read_grants
            BEGIN
                SELECT 1;
            END
            """
        )
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(
            RemoteExecutionJournalError,
            "invalid_execution_schema",
        ):
            RemoteExecutionJournal(self.path)

    def test_missing_provider_clock_singleton_fails_at_startup(self) -> None:
        RemoteExecutionJournal(self.path).close()
        connection = sqlite3.connect(self.path)
        connection.execute("DELETE FROM remote_provider_grant_clock")
        connection.commit()
        connection.close()

        with self.assertRaisesRegex(
            RemoteExecutionJournalError,
            "invalid_execution_schema",
        ):
            RemoteExecutionJournal(self.path)

    def test_rebound_expected_index_fails_at_startup(self) -> None:
        RemoteExecutionJournal(self.path).close()
        connection = sqlite3.connect(self.path)
        connection.execute(
            "DROP INDEX idx_remote_provider_grants_expires"
        )
        connection.execute(
            """
            CREATE INDEX idx_remote_provider_grants_expires
            ON remote_provider_grants(updated_at)
            """
        )
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(
            RemoteExecutionJournalError,
            "invalid_execution_schema",
        ):
            RemoteExecutionJournal(self.path)

    def test_orphan_provider_invocation_fails_at_startup(self) -> None:
        RemoteExecutionJournal(self.path).close()
        connection = sqlite3.connect(self.path)
        connection.execute(
            """
            INSERT INTO remote_provider_invocations(
                grant_id, request_payload_digest, state,
                response_digest, response_artifact_ref, updated_at
            ) VALUES (?, ?, 'invoking', NULL, NULL, ?)
            """,
            ("orphan-provider-grant", "f" * 64, 100.0),
        )
        connection.commit()
        connection.close()

        with self.assertRaisesRegex(
            RemoteExecutionJournalError,
            "invalid_execution_schema",
        ):
            RemoteExecutionJournal(self.path)

    def test_malformed_provider_invocation_fails_at_startup(self) -> None:
        journal = RemoteExecutionJournal(self.path)
        grant = _provider_record()
        journal.record_provider_grant(grant)
        journal.claim_provider_invocation(
            grant_id=grant.grant_id,
            token_digest=grant.token_digest,
            binding_digest=grant.binding_digest,
            route_id=grant.route_id,
            expires_at=grant.expires_at,
            request_payload_digest="d" * 64,
            now=101.0,
        )
        journal.close()
        connection = sqlite3.connect(self.path)
        connection.execute(
            """
            UPDATE remote_provider_invocations
            SET request_payload_digest = 'not-a-digest'
            """
        )
        connection.commit()
        connection.close()

        with self.assertRaisesRegex(
            RemoteExecutionJournalError,
            "invalid_execution_schema",
        ):
            RemoteExecutionJournal(self.path)

    def test_not_started_evidence_only_retries_durable_unknown(
        self,
    ) -> None:
        journal = RemoteExecutionJournal(self.path)
        grant = _provider_record()
        journal.record_provider_grant(grant)
        journal.claim_provider_invocation(
            grant_id=grant.grant_id,
            token_digest=grant.token_digest,
            binding_digest=grant.binding_digest,
            route_id=grant.route_id,
            expires_at=grant.expires_at,
            request_payload_digest="d" * 64,
            now=101.0,
        )
        retry_kwargs = {
            "grant_id": grant.grant_id,
            "token_digest": grant.token_digest,
            "binding_digest": grant.binding_digest,
            "route_id": grant.route_id,
            "expires_at": grant.expires_at,
            "request_payload_digest": "d" * 64,
            "evidence_digest": "e" * 64,
            "verifier_id": "provider-verifier",
            "now": 103.0,
        }
        with self.assertRaisesRegex(
            RemoteProviderInvocationUnknown,
            "provider_invocation_outcome_unknown",
        ):
            journal.claim_provider_retry_from_evidence(**retry_kwargs)
        self.assertEqual(
            journal.list_provider_recovery_evidence(grant.grant_id),
            (),
        )

        journal.mark_provider_invocation_unknown(
            grant_id=grant.grant_id,
            token_digest=grant.token_digest,
            binding_digest=grant.binding_digest,
            route_id=grant.route_id,
            expires_at=grant.expires_at,
            request_payload_digest="d" * 64,
            now=102.0,
        )
        retry = journal.claim_provider_retry_from_evidence(
            **retry_kwargs
        )
        self.assertTrue(retry.execute)
        self.assertEqual(retry.record.state, "invoking")
        evidence = journal.list_provider_recovery_evidence(
            grant.grant_id
        )
        self.assertEqual(len(evidence), 1)
        self.assertEqual(evidence[0].sequence, 1)
        self.assertEqual(evidence[0].decision, "not_started")
        with self.assertRaises(RemoteProviderInvocationUnknown):
            journal.claim_provider_retry_from_evidence(**retry_kwargs)
        self.assertEqual(
            journal.list_provider_recovery_evidence(grant.grant_id),
            evidence,
        )

    def test_completed_evidence_resolves_invoking_idempotently(
        self,
    ) -> None:
        journal = RemoteExecutionJournal(self.path)
        grant = _provider_record()
        journal.record_provider_grant(grant)
        journal.claim_provider_invocation(
            grant_id=grant.grant_id,
            token_digest=grant.token_digest,
            binding_digest=grant.binding_digest,
            route_id=grant.route_id,
            expires_at=grant.expires_at,
            request_payload_digest="d" * 64,
            now=101.0,
        )
        completed_kwargs = {
            "grant_id": grant.grant_id,
            "token_digest": grant.token_digest,
            "binding_digest": grant.binding_digest,
            "route_id": grant.route_id,
            "expires_at": grant.expires_at,
            "request_payload_digest": "d" * 64,
            "response_digest": "f" * 64,
            "response_artifact_ref": None,
            "evidence_digest": "e" * 64,
            "verifier_id": "provider-verifier",
            "now": 102.0,
        }
        completed = (
            journal.complete_provider_invocation_from_evidence(
                **completed_kwargs
            )
        )
        self.assertEqual(completed.state, "completed")
        self.assertEqual(completed.response_digest, "f" * 64)
        journal.complete_provider_invocation_from_evidence(
            **completed_kwargs
        )
        evidence = journal.list_provider_recovery_evidence(
            grant.grant_id
        )
        self.assertEqual(len(evidence), 1)
        self.assertEqual(evidence[0].decision, "completed")

    def test_retry_evidence_cannot_claim_at_or_after_expiry(
        self,
    ) -> None:
        journal = RemoteExecutionJournal(self.path)
        grant = _provider_record(expires_at=102.0)
        journal.record_provider_grant(grant)
        journal.claim_provider_invocation(
            grant_id=grant.grant_id,
            token_digest=grant.token_digest,
            binding_digest=grant.binding_digest,
            route_id=grant.route_id,
            expires_at=grant.expires_at,
            request_payload_digest="d" * 64,
            now=100.5,
        )
        journal.mark_provider_invocation_unknown(
            grant_id=grant.grant_id,
            token_digest=grant.token_digest,
            binding_digest=grant.binding_digest,
            route_id=grant.route_id,
            expires_at=grant.expires_at,
            request_payload_digest="d" * 64,
            now=101.0,
        )
        with self.assertRaisesRegex(
            RemoteProviderGrantUnavailable,
            "provider_grant_unavailable",
        ):
            journal.claim_provider_retry_from_evidence(
                grant_id=grant.grant_id,
                token_digest=grant.token_digest,
                binding_digest=grant.binding_digest,
                route_id=grant.route_id,
                expires_at=grant.expires_at,
                request_payload_digest="d" * 64,
                evidence_digest="e" * 64,
                verifier_id="provider-verifier",
                now=102.0,
            )
        self.assertEqual(
            journal.list_provider_recovery_evidence(grant.grant_id),
            (),
        )

    def test_not_started_evidence_digest_is_single_use(
        self,
    ) -> None:
        journal = RemoteExecutionJournal(self.path)
        grant = _provider_record()
        journal.record_provider_grant(grant)
        common = {
            "grant_id": grant.grant_id,
            "token_digest": grant.token_digest,
            "binding_digest": grant.binding_digest,
            "route_id": grant.route_id,
            "expires_at": grant.expires_at,
            "request_payload_digest": "d" * 64,
        }
        journal.claim_provider_invocation(
            **common,
            now=101.0,
        )
        journal.mark_provider_invocation_unknown(
            **common,
            now=102.0,
        )
        journal.claim_provider_retry_from_evidence(
            **common,
            evidence_digest="e" * 64,
            verifier_id="provider-verifier",
            now=103.0,
        )
        journal.mark_provider_invocation_unknown(
            **common,
            now=104.0,
        )
        with self.assertRaisesRegex(
            RemoteProviderInvocationConflict,
            "provider_recovery_evidence_replayed",
        ):
            journal.claim_provider_retry_from_evidence(
                **common,
                evidence_digest="e" * 64,
                verifier_id="provider-verifier",
                now=105.0,
            )
        self.assertEqual(
            len(
                journal.list_provider_recovery_evidence(
                    grant.grant_id
                )
            ),
            1,
        )
        invocation = journal.get_provider_invocation(
            grant.grant_id
        )
        self.assertIsNotNone(invocation)
        assert invocation is not None
        self.assertEqual(invocation.state, "outcome_unknown")

    def test_recovery_evidence_is_bounded_and_purged_with_grant(
        self,
    ) -> None:
        journal = RemoteExecutionJournal(self.path)
        grant = _provider_record()
        journal.record_provider_grant(grant)
        journal.claim_provider_invocation(
            grant_id=grant.grant_id,
            token_digest=grant.token_digest,
            binding_digest=grant.binding_digest,
            route_id=grant.route_id,
            expires_at=grant.expires_at,
            request_payload_digest="d" * 64,
            now=101.0,
        )
        base_kwargs = {
            "grant_id": grant.grant_id,
            "token_digest": grant.token_digest,
            "binding_digest": grant.binding_digest,
            "route_id": grant.route_id,
            "expires_at": grant.expires_at,
            "request_payload_digest": "d" * 64,
            "response_digest": "f" * 64,
            "response_artifact_ref": None,
            "verifier_id": "provider-verifier",
        }
        for index in range(
            MAX_PROVIDER_RECOVERY_EVIDENCE_PER_INVOCATION
        ):
            journal.complete_provider_invocation_from_evidence(
                **base_kwargs,
                evidence_digest=hashlib.sha256(
                    f"evidence-{index}".encode()
                ).hexdigest(),
                now=102.0 + index,
            )
        with self.assertRaisesRegex(
            RemoteExecutionJournalCapacityError,
            "provider_recovery_evidence_capacity",
        ):
            journal.complete_provider_invocation_from_evidence(
                **base_kwargs,
                evidence_digest=hashlib.sha256(
                    b"evidence-overflow"
                ).hexdigest(),
                now=150.0,
            )
        self.assertEqual(
            len(
                journal.list_provider_recovery_evidence(
                    grant.grant_id
                )
            ),
            MAX_PROVIDER_RECOVERY_EVIDENCE_PER_INVOCATION,
        )
        self.assertEqual(
            journal.purge_expired_provider_grants(now=200.0),
            1,
        )
        self.assertEqual(
            journal.list_provider_recovery_evidence(grant.grant_id),
            (),
        )

    def test_orphan_or_noncontiguous_recovery_evidence_fails_startup(
        self,
    ) -> None:
        RemoteExecutionJournal(self.path).close()
        connection = sqlite3.connect(self.path)
        connection.execute(
            """
            INSERT INTO remote_provider_recovery_evidence(
                grant_id, sequence, request_payload_digest,
                decision, evidence_digest, verifier_id, created_at
            ) VALUES (?, 2, ?, 'not_started', ?, ?, ?)
            """,
            (
                "orphan-provider-grant",
                "d" * 64,
                "e" * 64,
                "provider-verifier",
                100.0,
            ),
        )
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(
            RemoteExecutionJournalError,
            "invalid_execution_schema",
        ):
            RemoteExecutionJournal(self.path)

    def test_expired_not_started_evidence_fails_startup(
        self,
    ) -> None:
        journal = RemoteExecutionJournal(self.path)
        grant = _provider_record()
        journal.record_provider_grant(grant)
        journal.claim_provider_invocation(
            grant_id=grant.grant_id,
            token_digest=grant.token_digest,
            binding_digest=grant.binding_digest,
            route_id=grant.route_id,
            expires_at=grant.expires_at,
            request_payload_digest="d" * 64,
            now=101.0,
        )
        journal.mark_provider_invocation_unknown(
            grant_id=grant.grant_id,
            token_digest=grant.token_digest,
            binding_digest=grant.binding_digest,
            route_id=grant.route_id,
            expires_at=grant.expires_at,
            request_payload_digest="d" * 64,
            now=102.0,
        )
        journal.claim_provider_retry_from_evidence(
            grant_id=grant.grant_id,
            token_digest=grant.token_digest,
            binding_digest=grant.binding_digest,
            route_id=grant.route_id,
            expires_at=grant.expires_at,
            request_payload_digest="d" * 64,
            evidence_digest="e" * 64,
            verifier_id="provider-verifier",
            now=103.0,
        )
        journal.close()
        connection = sqlite3.connect(self.path)
        connection.execute(
            """
            UPDATE remote_provider_recovery_evidence
            SET created_at = 200
            WHERE grant_id = ?
            """,
            (grant.grant_id,),
        )
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(
            RemoteExecutionJournalError,
            "invalid_execution_schema",
        ):
            RemoteExecutionJournal(self.path)

    def test_duplicate_recovery_evidence_digest_fails_startup(
        self,
    ) -> None:
        journal = RemoteExecutionJournal(self.path)
        grant = _provider_record()
        journal.record_provider_grant(grant)
        journal.claim_provider_invocation(
            grant_id=grant.grant_id,
            token_digest=grant.token_digest,
            binding_digest=grant.binding_digest,
            route_id=grant.route_id,
            expires_at=grant.expires_at,
            request_payload_digest="d" * 64,
            now=101.0,
        )
        journal.complete_provider_invocation_from_evidence(
            grant_id=grant.grant_id,
            token_digest=grant.token_digest,
            binding_digest=grant.binding_digest,
            route_id=grant.route_id,
            expires_at=grant.expires_at,
            request_payload_digest="d" * 64,
            response_digest="f" * 64,
            response_artifact_ref=None,
            evidence_digest="e" * 64,
            verifier_id="provider-verifier",
            now=102.0,
        )
        journal.close()
        connection = sqlite3.connect(self.path)
        connection.execute(
            """
            INSERT INTO remote_provider_recovery_evidence(
                grant_id, sequence, request_payload_digest,
                decision, evidence_digest, verifier_id, created_at
            ) VALUES (?, 2, ?, 'completed', ?, ?, 103)
            """,
            (
                grant.grant_id,
                "d" * 64,
                "e" * 64,
                "provider-verifier",
            ),
        )
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(
            RemoteExecutionJournalError,
            "invalid_execution_schema",
        ):
            RemoteExecutionJournal(self.path)

    def test_expiry_deletion_cannot_be_reversed_by_clock_rollback(self) -> None:
        journal = RemoteExecutionJournal(self.path)
        record = RemoteArtifactWriteGrantRecord(
            grant_id="write-grant-expiring",
            token_digest="4" * 64,
            grant_metadata=_canonical(
                {"grant_id": "write-grant-expiring"}
            ),
            state="issued",
            staging_digest=None,
            final_ref=None,
            expires_at=110.0,
            updated_at=100.0,
        )
        journal.record_write_grant(record)
        with self.assertRaises(RemoteArtifactGrantUnavailable):
            journal.get_write_grant(
                grant_id=record.grant_id,
                token_digest=record.token_digest,
                now=110.0,
            )
        with self.assertRaises(RemoteArtifactGrantUnavailable):
            journal.get_write_grant(
                grant_id=record.grant_id,
                token_digest=record.token_digest,
                now=105.0,
            )
        read = RemoteArtifactReadGrantRecord(
            grant_id="read-grant-expiring",
            token_digest="5" * 64,
            grant_metadata=_canonical(
                {"grant_id": "read-grant-expiring"}
            ),
            artifact_ref=_canonical({"sha256": "6" * 64}),
            state="issued",
            expires_at=110.0,
            updated_at=100.0,
        )
        journal.record_read_grant(read)
        for current in (110.0, 105.0):
            with self.assertRaises(RemoteArtifactGrantUnavailable):
                journal.consume_read_grant(
                    grant_id=read.grant_id,
                    token_digest=read.token_digest,
                    grant_metadata=read.grant_metadata,
                    expires_at=read.expires_at,
                    now=current,
                )


if __name__ == "__main__":
    unittest.main()
