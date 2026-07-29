from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.orchestration.artifacts import LocalArtifactStore
from src.orchestration.artifacts_gc import (
    ArtifactGCIntegrityError,
    ArtifactGCRestoreError,
    LocalArtifactGarbageCollector,
)
from src.orchestration.runtime import OrchestrationRuntime
from src.orchestration.scheduler import DurableScheduler, RunInputReceipt
from src.orchestration.store import (
    ArtifactGCReferenceConflictError,
    DurableRunStore,
)
from src.orchestration.workflow import compile_workflow


def _workflow(marker: str = "workflow-marker") -> dict:
    return {
        "schema_version": 2,
        "name": "artifact-gc-workflow",
        "version": 1,
        "nodes": [
            {
                "id": "inspect",
                "kind": "tool",
                "depends_on": [],
                "config": {
                    "tool": "inspect",
                    "arguments": {"credential": marker},
                },
                "effect_class": "read_only",
            }
        ],
    }


class ArtifactGarbageCollectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = self.root / "orchestration.sqlite3"
        self.store = DurableRunStore(self.database)
        self.artifacts = LocalArtifactStore(self.root / "artifacts")
        self.now = time.time()
        self.gc = LocalArtifactGarbageCollector(
            self.artifacts,
            self.store,
            clock=lambda: self.now,
        )

    def _age(self, ref, seconds: float = 120.0) -> Path:
        path = self.artifacts._path_for_digest(ref.sha256)
        modified_at = self.now - seconds
        os.utime(path, (modified_at, modified_at))
        return path

    def _scheduler(self) -> DurableScheduler:
        return DurableScheduler(
            self.store,
            compile_workflow(_workflow()),
            clock=lambda: self.now,
            artifact_verifier=self.artifacts.verify,
        )

    def _temporary_artifact_file(
        self,
        content: bytes,
    ) -> Path:
        intended_digest = hashlib.sha256(b"intended-final-content").hexdigest()
        parent = (
            self.artifacts.root
            / "sha256"
            / intended_digest[:2]
            / intended_digest[2:4]
        )
        self.artifacts._ensure_parent(parent)
        descriptor, name = tempfile.mkstemp(
            prefix=f".{intended_digest}.",
            suffix=".tmp",
            dir=parent,
        )
        with os.fdopen(descriptor, "wb") as temporary:
            temporary.write(content)
            temporary.flush()
            os.fsync(temporary.fileno())
        path = Path(name)
        modified_at = self.now - 120
        os.utime(path, (modified_at, modified_at))
        return path

    def test_f09_old_orphan_is_dry_run_by_default_then_restorable(self) -> None:
        secret = b"F09-ORPHAN-CONTENT-MUST-NOT-BE-REPORTED"
        orphan = self.artifacts.put_bytes(secret)
        self._age(orphan)

        preview = self.gc.collect(grace_seconds=60)

        self.assertTrue(preview.dry_run)
        self.assertEqual(preview.eligible_orphans, 1)
        self.assertTrue(self.artifacts.exists(orphan))

        report = self.gc.collect(
            dry_run=False,
            grace_seconds=60,
            limit=10,
        )

        self.assertTrue(report.second_scan_performed)
        self.assertEqual(len(report.quarantined), 1)
        self.assertFalse(self.artifacts.exists(orphan))
        rendered = json.dumps(report.to_dict(), sort_keys=True)
        self.assertNotIn(str(self.artifacts.root), rendered)
        self.assertNotIn(secret.decode("utf-8"), rendered)
        self.assertEqual(self.gc.list_quarantined(), report.quarantined)

        restored = self.gc.restore(report.quarantined[0], orphan)

        self.assertEqual(restored, orphan)
        self.assertTrue(self.artifacts.verify(orphan))
        self.assertEqual(self.gc.list_quarantined(), ())

    def test_committed_complete_ref_is_retained_but_digest_string_is_not(self) -> None:
        retained = self.artifacts.put_bytes(b"committed-content")
        arbitrary = self.artifacts.put_bytes(b"arbitrary-digest-string")
        self._age(retained)
        self._age(arbitrary)
        scheduler = self._scheduler()
        scheduler.create_run(
            "committed-ref-run",
            input=RunInputReceipt((retained,)),
        )
        scheduler.create_run(
            "digest-string-run",
            metadata={"looks_like_reference": arbitrary.sha256},
        )

        report = self.gc.collect(dry_run=False, grace_seconds=60)

        self.assertTrue(self.artifacts.verify(retained))
        self.assertFalse(self.artifacts.exists(arbitrary))
        self.assertEqual(
            {entry.sha256 for entry in report.quarantined},
            {arbitrary.sha256},
        )

    def test_recent_orphan_is_retained(self) -> None:
        recent = self.artifacts.put_bytes(b"recent-orphan")

        report = self.gc.collect(dry_run=False, grace_seconds=60)

        self.assertEqual(report.retained_recent, 1)
        self.assertEqual(report.quarantined, ())
        self.assertTrue(self.artifacts.verify(recent))

    def test_second_scan_prevents_quarantine_of_newly_reachable_digest(self) -> None:
        registered_during_scan = self.artifacts.put_bytes(
            b"registered-between-reachability-scans"
        )
        self._age(registered_during_scan)
        scans = iter(
            (
                frozenset(),
                frozenset({registered_during_scan.sha256}),
            )
        )
        self.gc.scan_reachable_digests = lambda: next(scans)

        report = self.gc.collect(dry_run=False, grace_seconds=60)

        self.assertTrue(report.second_scan_performed)
        self.assertEqual(report.quarantined, ())
        self.assertTrue(self.artifacts.verify(registered_during_scan))

    def test_gc_claim_linearizes_against_concurrent_run_reference(self) -> None:
        candidate = self.artifacts.put_bytes(b"claim-linearization")
        self._age(candidate)
        claim_committed = threading.Event()
        release_gc = threading.Event()

        def pause_after_claim(stage: str, digest: str) -> None:
            if stage == "artifact_gc.after_claim":
                self.assertEqual(digest, candidate.sha256)
                claim_committed.set()
                self.assertTrue(release_gc.wait(5))

        self.gc._fault = pause_after_claim
        with ThreadPoolExecutor(max_workers=1) as executor:
            collection = executor.submit(
                self.gc.collect,
                dry_run=False,
                grace_seconds=60,
            )
            self.assertTrue(claim_committed.wait(5))
            # The claim is committed before the move, so bytes remain readable
            # while a new canonical reference is rejected atomically.
            self.assertTrue(self.artifacts.verify(candidate))
            with self.assertRaises(ArtifactGCReferenceConflictError):
                self._scheduler().create_run(
                    "racing-reference",
                    input=RunInputReceipt((candidate,)),
                )
            self.assertIsNone(self.store.get_run("racing-reference"))
            release_gc.set()
            report = collection.result(timeout=5)

        self.assertEqual(
            tuple(entry.sha256 for entry in report.quarantined),
            (candidate.sha256,),
        )
        claim = self.store.get_artifact_gc_claim(candidate.sha256)
        self.assertIsNotNone(claim)
        self.assertEqual(claim.state, "quarantined")
        self.gc.restore(report.quarantined[0], candidate)
        self.assertIsNone(self.store.get_artifact_gc_claim(candidate.sha256))
        created = self._scheduler().create_run(
            "reference-after-restore",
            input=RunInputReceipt((candidate,)),
        )
        self.assertEqual(created.run_id, "reference-after-restore")

    def test_crash_after_claim_is_recovered_without_losing_source(self) -> None:
        candidate = self.artifacts.put_bytes(b"crash-after-claim")
        self._age(candidate)

        def crash_after_claim(stage: str, _digest: str) -> None:
            if stage == "artifact_gc.after_claim":
                raise SystemExit("injected crash")

        self.gc._fault = crash_after_claim
        with self.assertRaises(SystemExit):
            self.gc.collect(dry_run=False, grace_seconds=60)

        claim = self.store.get_artifact_gc_claim(candidate.sha256)
        self.assertIsNotNone(claim)
        self.assertEqual(claim.state, "moving")
        self.assertTrue(self.artifacts.verify(candidate))

        restarted = LocalArtifactGarbageCollector(
            self.artifacts,
            self.store,
            clock=lambda: self.now,
        )
        self.assertEqual(restarted.list_quarantined_temporary(), ())
        self.assertIsNone(self.store.get_artifact_gc_claim(candidate.sha256))
        self.assertEqual(restarted.list_quarantined(), ())
        self.assertTrue(self.artifacts.verify(candidate))

    def test_crash_after_move_is_recovered_and_restorable(self) -> None:
        candidate = self.artifacts.put_bytes(b"crash-after-move")
        self._age(candidate)

        def crash_after_move(stage: str, _digest: str) -> None:
            if stage == "artifact_gc.after_move":
                raise SystemExit("injected crash")

        self.gc._fault = crash_after_move
        with self.assertRaises(SystemExit):
            self.gc.collect(dry_run=False, grace_seconds=60)

        claim = self.store.get_artifact_gc_claim(candidate.sha256)
        self.assertIsNotNone(claim)
        self.assertEqual(claim.state, "moving")
        self.assertFalse(self.artifacts.exists(candidate))

        restarted = LocalArtifactGarbageCollector(
            self.artifacts,
            self.store,
            clock=lambda: self.now,
        )
        entries = restarted.list_quarantined()
        self.assertEqual(len(entries), 1)
        claim = self.store.get_artifact_gc_claim(candidate.sha256)
        self.assertIsNotNone(claim)
        self.assertEqual(claim.state, "quarantined")
        self.assertTrue(restarted.artifact_store.verify(
            restarted.restore(entries[0], candidate)
        ))
        self.assertIsNone(self.store.get_artifact_gc_claim(candidate.sha256))

    def test_failure_after_move_rolls_back_file_and_claim(self) -> None:
        candidate = self.artifacts.put_bytes(b"failure-after-move")
        self._age(candidate)

        def fail_after_move(stage: str, _digest: str) -> None:
            if stage == "artifact_gc.after_move":
                raise RuntimeError("injected failure")

        self.gc._fault = fail_after_move
        with self.assertRaisesRegex(
            ArtifactGCIntegrityError,
            "claim was rolled back",
        ):
            self.gc.collect(dry_run=False, grace_seconds=60)

        self.assertTrue(self.artifacts.verify(candidate))
        self.assertIsNone(self.store.get_artifact_gc_claim(candidate.sha256))
        self.assertEqual(self.gc.list_quarantined(), ())

    def test_corrupt_artifact_is_not_quarantined_or_deleted(self) -> None:
        corrupt = self.artifacts.put_bytes(b"original-content")
        path = self._age(corrupt)
        path.write_bytes(b"tampered-content")
        self._age(corrupt)

        report = self.gc.collect(dry_run=False, grace_seconds=60)

        self.assertEqual(report.skipped_integrity, 1)
        self.assertEqual(report.quarantined, ())
        self.assertEqual(path.read_bytes(), b"tampered-content")

    def test_symlink_in_managed_tree_fails_closed_without_deleting_target(self) -> None:
        ref = self.artifacts.put_bytes(b"original")
        path = self._age(ref)
        target = self.root / "external-target"
        target.write_bytes(b"external-content")
        path.unlink()
        path.symlink_to(target)

        with self.assertRaises(ArtifactGCIntegrityError):
            self.gc.collect(dry_run=False, grace_seconds=60)

        self.assertTrue(path.is_symlink())
        self.assertEqual(target.read_bytes(), b"external-content")

    def test_restore_verification_failure_rolls_back_to_quarantine(self) -> None:
        ref = self.artifacts.put_bytes(b"restore-rollback")
        self._age(ref)
        report = self.gc.collect(dry_run=False, grace_seconds=60)
        entry = report.quarantined[0]
        original_verify = self.artifacts.verify

        def fail_verification(_ref):
            raise RuntimeError("injected restore verification fault")

        self.artifacts.verify = fail_verification
        try:
            with self.assertRaises(ArtifactGCRestoreError):
                self.gc.restore(entry, ref)
        finally:
            self.artifacts.verify = original_verify

        self.assertFalse(self.artifacts.exists(ref))
        self.assertEqual(self.gc.list_quarantined(), (entry,))
        self.assertTrue(self.artifacts.verify(self.gc.restore(entry, ref)))

    def test_restore_release_uncertainty_keeps_restored_bytes_available(
        self,
    ) -> None:
        ref = self.artifacts.put_bytes(b"restore-release-uncertainty")
        self._age(ref)
        entry = self.gc.collect(
            dry_run=False,
            grace_seconds=60,
        ).quarantined[0]
        real_release = self.store.release_artifact_gc_claim

        def release_then_fail(*args, **kwargs):
            real_release(*args, **kwargs)
            raise RuntimeError("injected ambiguous commit response")

        self.store.release_artifact_gc_claim = release_then_fail
        try:
            with self.assertRaisesRegex(
                ArtifactGCRestoreError,
                "restored bytes remain available",
            ):
                self.gc.restore(entry, ref)
        finally:
            self.store.release_artifact_gc_claim = real_release

        self.assertTrue(self.artifacts.verify(ref))
        self.assertIsNone(self.store.get_artifact_gc_claim(ref.sha256))
        self.assertEqual(self.gc.list_quarantined(), ())

    def test_old_crashed_write_temp_is_quarantined_and_restorable(self) -> None:
        content = b"F09-PARTIAL-TEMP-CONTENT"
        temporary_path = self._temporary_artifact_file(content)

        report = self.gc.collect(dry_run=False, grace_seconds=60)

        self.assertEqual(report.eligible_temporary_files, 1)
        self.assertEqual(len(report.quarantined_temporary), 1)
        self.assertFalse(temporary_path.exists())
        entry = report.quarantined_temporary[0]
        self.assertEqual(
            self.gc.list_quarantined_temporary(),
            (entry,),
        )
        rendered = json.dumps(report.to_dict(), sort_keys=True)
        self.assertNotIn(str(self.artifacts.root), rendered)
        self.assertNotIn(content.decode("utf-8"), rendered)

        restored = self.gc.restore_temporary(entry)

        self.assertEqual(restored, entry)
        self.assertEqual(temporary_path.read_bytes(), content)
        self.assertEqual(self.gc.list_quarantined_temporary(), ())

    def test_temporary_list_and_restore_share_exclusive_gc_lock(self) -> None:
        temporary_path = self._temporary_artifact_file(b"temporary-lock")
        report = self.gc.collect(dry_run=False, grace_seconds=60)
        entry = report.quarantined_temporary[0]
        list_started = threading.Event()
        list_entered = threading.Event()
        restore_started = threading.Event()
        restore_entered = threading.Event()
        real_list = self.gc._list_quarantined_temporary_locked
        real_restore = self.gc._restore_temporary_locked

        def observed_list(*, limit):
            list_entered.set()
            return real_list(limit=limit)

        def observed_restore(restored_entry):
            restore_entered.set()
            return real_restore(restored_entry)

        def list_temporary():
            list_started.set()
            return self.gc.list_quarantined_temporary()

        def restore_temporary():
            restore_started.set()
            return self.gc.restore_temporary(entry)

        self.gc._list_quarantined_temporary_locked = observed_list
        self.gc._restore_temporary_locked = observed_restore
        with ThreadPoolExecutor(max_workers=1) as executor:
            with self.gc._exclusive_gc_lock():
                listing = executor.submit(list_temporary)
                self.assertTrue(list_started.wait(5))
                self.assertFalse(list_entered.wait(0.1))
            self.assertEqual(listing.result(timeout=5), (entry,))
            self.assertTrue(list_entered.is_set())

            with self.gc._exclusive_gc_lock():
                restoring = executor.submit(restore_temporary)
                self.assertTrue(restore_started.wait(5))
                self.assertFalse(restore_entered.wait(0.1))
            self.assertEqual(restoring.result(timeout=5), entry)
            self.assertTrue(restore_entered.is_set())

        self.assertEqual(temporary_path.read_bytes(), b"temporary-lock")

    def test_candidate_replacement_rolls_back_and_reports_unconfirmed_state(
        self,
    ) -> None:
        original = self.artifacts.put_bytes(b"original-candidate")
        source = self._age(original)
        real_quarantine = self.gc._quarantine

        def replace_before_move(candidate, *, now, entry=None):
            replacement = self.root / "concurrent-replacement"
            replacement.write_bytes(b"concurrent-replace")
            os.replace(replacement, source)
            return real_quarantine(candidate, now=now, entry=entry)

        self.gc._quarantine = replace_before_move

        with self.assertRaisesRegex(
            ArtifactGCIntegrityError,
            "state was not confirmed",
        ):
            self.gc.collect(dry_run=False, grace_seconds=60)

        self.assertTrue(source.exists())
        self.assertEqual(source.read_bytes(), b"concurrent-replace")
        quarantined_files = [
            path
            for path in (
                self.artifacts.root / ".artifact-quarantine" / "v1"
            ).rglob("*")
            if path.is_file()
        ]
        self.assertEqual(quarantined_files, [])

    def test_runtime_restart_persists_workflow_ref_and_gc_retains_inputs(self) -> None:
        marker = "F09-WORKFLOW-CONTENT-MUST-STAY-OUT-OF-SQLITE"
        workflow_ref = self.artifacts.put_json(_workflow(marker))
        input_ref = self.artifacts.put_bytes(b"F09-INPUT-CONTENT")
        self._age(workflow_ref)
        self._age(input_ref)

        def scheduler_factory(store, workflow):
            return DurableScheduler(
                store,
                workflow,
                artifact_verifier=self.artifacts.verify,
            )

        def runtime():
            return OrchestrationRuntime(
                store=self.store,
                artifact_store=self.artifacts,
                scheduler_factory=scheduler_factory,
                authorizer=lambda _request, _context: True,
            )

        payload = {
            "protocol_version": 1,
            "request_id": "f09-submit",
            "operation": "submit",
            "body": {
                "run_id": "f09-runtime-run",
                "workflow_ref": workflow_ref.to_dict(),
                "input_receipt": {
                    "artifact_refs": [input_ref.to_dict()],
                },
                "parent": None,
            },
        }
        first = runtime().handle(payload).to_dict()
        restarted = runtime().handle(payload).to_dict()
        alternate_workflow_ref = self.artifacts.put_bytes(
            json.dumps(_workflow(marker), indent=2).encode("utf-8"),
            media_type="application/json",
        )
        changed_binding = json.loads(json.dumps(payload))
        changed_binding["body"]["workflow_ref"] = (
            alternate_workflow_ref.to_dict()
        )
        rejected = runtime().handle(changed_binding).to_dict()
        run = self.store.get_run("f09-runtime-run")

        self.assertTrue(first["ok"])
        self.assertEqual(first["result"], restarted["result"])
        self.assertFalse(rejected["ok"])
        self.assertEqual(rejected["error"]["code"], "invalid_input")
        self.assertIsNotNone(run)
        self.assertEqual(
            run.metadata["runtime_workflow_ref"],
            workflow_ref.to_dict(),
        )

        report = self.gc.collect(dry_run=False, grace_seconds=60)

        self.assertEqual(report.quarantined, ())
        self.assertTrue(self.artifacts.verify(workflow_ref))
        self.assertTrue(self.artifacts.verify(input_ref))
        sqlite_bytes = b"".join(
            path.read_bytes()
            for path in (
                self.database,
                Path(f"{self.database}-wal"),
                Path(f"{self.database}-shm"),
            )
            if path.exists()
        )
        self.assertNotIn(marker.encode("utf-8"), sqlite_bytes)
        self.assertNotIn(b"F09-INPUT-CONTENT", sqlite_bytes)
        events = json.dumps(
            [
                event.to_dict()
                for event in self.store.list_events("f09-runtime-run")
            ],
            sort_keys=True,
        )
        self.assertNotIn(marker, events)
        self.assertNotIn("F09-INPUT-CONTENT", events)


if __name__ == "__main__":
    unittest.main()
