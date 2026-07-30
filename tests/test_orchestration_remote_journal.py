from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from src.orchestration.remote_journal import (
    RemoteControlJournal,
    RemoteJournalCapacityError,
    RemoteJournalError,
    RemoteJournalRequestConflict,
    RemoteJournalSessionSuperseded,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class RemoteControlJournalTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "remote-control.sqlite3"
        self.identity_digest = _digest("trusted-worker")

    def _register(
        self,
        journal: RemoteControlJournal,
        instance_id: str,
        *,
        request_id: str,
        request_digest: str,
        now: float,
    ):
        return journal.register_session(
            worker_id="worker-1",
            tenant_id="tenant-1",
            identity_digest=self.identity_digest,
            instance_id=instance_id,
            request_id=request_id,
            request_digest=request_digest,
            operation="register",
            now=now,
        )

    def test_epoch_recovers_current_instance_and_tombstones_retired_aba(self):
        first = RemoteControlJournal(self.path)
        session_a = self._register(
            first,
            "instance-a",
            request_id="register-a",
            request_digest=_digest("register-a"),
            now=1,
        )
        self.assertEqual(session_a.epoch, 1)
        first.close()

        restarted = RemoteControlJournal(self.path)
        recovered_a = self._register(
            restarted,
            "instance-a",
            request_id="recover-a",
            request_digest=_digest("recover-a"),
            now=2,
        )
        self.assertEqual(recovered_a.epoch, session_a.epoch)
        session_b = self._register(
            restarted,
            "instance-b",
            request_id="register-b",
            request_digest=_digest("register-b"),
            now=3,
        )
        self.assertEqual(session_b.epoch, session_a.epoch + 1)
        restarted.close()

        after_supersession = RemoteControlJournal(self.path)
        recovered_b = self._register(
            after_supersession,
            "instance-b",
            request_id="recover-b",
            request_digest=_digest("recover-b"),
            now=4,
        )
        self.assertEqual(recovered_b.epoch, session_b.epoch)
        with self.assertRaises(RemoteJournalSessionSuperseded):
            self._register(
                after_supersession,
                "instance-a",
                request_id="aba-a",
                request_digest=_digest("aba-a"),
                now=5,
            )

    def test_request_digest_survives_reopen_and_never_uses_lru_eviction(self):
        first = RemoteControlJournal(
            self.path,
            max_requests_per_session=3,
        )
        session = self._register(
            first,
            "instance-a",
            request_id="register",
            request_digest=_digest("register"),
            now=1,
        )
        self.assertTrue(
            first.record_request(
                worker_id="worker-1",
                tenant_id="tenant-1",
                identity_digest=self.identity_digest,
                instance_id="instance-a",
                epoch=session.epoch,
                request_id="poll",
                request_digest=_digest("poll-a"),
                operation="poll",
                now=2,
            )
        )
        first.close()

        restarted = RemoteControlJournal(
            self.path,
            max_requests_per_session=3,
        )
        self.assertFalse(
            restarted.record_request(
                worker_id="worker-1",
                tenant_id="tenant-1",
                identity_digest=self.identity_digest,
                instance_id="instance-a",
                epoch=session.epoch,
                request_id="poll",
                request_digest=_digest("poll-a"),
                operation="poll",
                now=3,
            )
        )
        with self.assertRaises(RemoteJournalRequestConflict):
            restarted.record_request(
                worker_id="worker-1",
                tenant_id="tenant-1",
                identity_digest=self.identity_digest,
                instance_id="instance-a",
                epoch=session.epoch,
                request_id="poll",
                request_digest=_digest("poll-b"),
                operation="poll",
                now=4,
            )

    def test_request_and_instance_capacities_fail_closed(self):
        journal = RemoteControlJournal(
            self.path,
            max_instances_per_worker=2,
            max_requests_per_session=2,
        )
        session_a = self._register(
            journal,
            "instance-a",
            request_id="register-a",
            request_digest=_digest("register-a"),
            now=1,
        )
        self.assertTrue(
            journal.record_request(
                worker_id="worker-1",
                tenant_id="tenant-1",
                identity_digest=self.identity_digest,
                instance_id="instance-a",
                epoch=session_a.epoch,
                request_id="poll-a",
                request_digest=_digest("poll-a"),
                operation="poll",
                now=2,
            )
        )
        with self.assertRaises(RemoteJournalCapacityError):
            journal.record_request(
                worker_id="worker-1",
                tenant_id="tenant-1",
                identity_digest=self.identity_digest,
                instance_id="instance-a",
                epoch=session_a.epoch,
                request_id="poll-over-capacity",
                request_digest=_digest("poll-over-capacity"),
                operation="poll",
                now=3,
            )
        with self.assertRaises(RemoteJournalRequestConflict):
            journal.record_request(
                worker_id="worker-1",
                tenant_id="tenant-1",
                identity_digest=self.identity_digest,
                instance_id="instance-a",
                epoch=session_a.epoch,
                request_id="poll-a",
                request_digest=_digest("different-intent"),
                operation="poll",
                now=4,
            )

        self._register(
            journal,
            "instance-b",
            request_id="register-b",
            request_digest=_digest("register-b"),
            now=5,
        )
        with self.assertRaises(RemoteJournalCapacityError):
            self._register(
                journal,
                "instance-c",
                request_id="register-c",
                request_digest=_digest("register-c"),
                now=6,
            )
        with self.assertRaises(RemoteJournalSessionSuperseded):
            self._register(
                journal,
                "instance-a",
                request_id="aba",
                request_digest=_digest("aba"),
                now=7,
            )

    def test_concurrent_first_observation_has_one_insert_linearization(self):
        bootstrap = RemoteControlJournal(self.path)
        session = self._register(
            bootstrap,
            "instance-a",
            request_id="register",
            request_digest=_digest("register"),
            now=1,
        )
        bootstrap.close()
        barrier = threading.Barrier(16)

        def observe(_index: int) -> bool:
            journal = RemoteControlJournal(self.path)
            try:
                barrier.wait(timeout=5)
                return journal.record_request(
                    worker_id="worker-1",
                    tenant_id="tenant-1",
                    identity_digest=self.identity_digest,
                    instance_id="instance-a",
                    epoch=session.epoch,
                    request_id="concurrent-poll",
                    request_digest=_digest("concurrent-poll"),
                    operation="poll",
                    now=2,
                )
            finally:
                journal.close()

        with ThreadPoolExecutor(max_workers=16) as pool:
            observed = list(pool.map(observe, range(16)))
        self.assertEqual(observed.count(True), 1)
        self.assertEqual(observed.count(False), 15)

    def test_unknown_schema_fails_closed(self):
        journal = RemoteControlJournal(self.path)
        journal.close()
        connection = sqlite3.connect(self.path)
        try:
            connection.execute(
                """
                UPDATE remote_journal_metadata
                SET schema_version = 999
                WHERE singleton = 1
                """
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaisesRegex(RemoteJournalError, "unsupported_schema"):
            RemoteControlJournal(self.path)

    def test_startup_rejects_partial_current_version_schema(self):
        connection = sqlite3.connect(self.path)
        try:
            connection.executescript(
                """
                CREATE TABLE remote_journal_metadata(
                    singleton INTEGER PRIMARY KEY,
                    schema_version INTEGER NOT NULL
                );
                INSERT INTO remote_journal_metadata VALUES (1, 1);
                CREATE TABLE remote_session_heads(worker_id TEXT PRIMARY KEY);
                """
            )
        finally:
            connection.close()

        with self.assertRaisesRegex(RemoteJournalError, "invalid_schema"):
            RemoteControlJournal(self.path)

    def test_startup_rejects_existing_database_after_total_schema_loss(self):
        journal = RemoteControlJournal(self.path)
        self._register(
            journal,
            "instance-a",
            request_id="register",
            request_digest=_digest("register"),
            now=1,
        )
        journal.close()
        connection = sqlite3.connect(self.path)
        try:
            connection.executescript(
                """
                DROP TABLE remote_request_identities;
                DROP TABLE remote_session_instances;
                DROP TABLE remote_session_heads;
                DROP TABLE remote_journal_metadata;
                """
            )
        finally:
            connection.close()

        with self.assertRaisesRegex(RemoteJournalError, "invalid_schema"):
            RemoteControlJournal(self.path)

    def test_failed_atomic_bootstrap_never_publishes_partial_database(self):
        with patch(
            "src.orchestration.remote_journal.os.link",
            side_effect=OSError("injected publish failure"),
        ):
            with self.assertRaisesRegex(RemoteJournalError, "journal_unavailable"):
                RemoteControlJournal(self.path)

        self.assertFalse(self.path.exists())
        recovered = RemoteControlJournal(self.path)
        self._register(
            recovered,
            "instance-a",
            request_id="register",
            request_digest=_digest("register"),
            now=1,
        )


if __name__ == "__main__":
    unittest.main()
