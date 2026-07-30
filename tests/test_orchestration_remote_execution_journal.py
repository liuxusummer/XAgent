from __future__ import annotations

import sqlite3
import os
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path

from src.orchestration.remote_execution_journal import (
    RemoteExecutionBindingRecord,
    RemoteExecutionJournal,
    RemoteExecutionJournalCapacityError,
    RemoteExecutionJournalConflict,
    RemoteExecutionJournalError,
)


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


if __name__ == "__main__":
    unittest.main()
