from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from src.orchestration.artifacts import (
    MAX_ARTIFACT_METADATA_BYTES,
    ArtifactEncryption,
    ArtifactIntegrityError,
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    ArtifactValidationError,
    ArtifactWriteError,
    JsonArtifactResultWriter,
    LocalArtifactStore,
    canonical_json_bytes,
)


class LocalArtifactStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name) / "runtime" / "artifacts"
        self.store = LocalArtifactStore(self.root)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_empty_and_large_content_round_trip(self) -> None:
        empty_ref = self.store.put_bytes(b"")
        large_content = (b"xagent-artifact-" * 100_000) + b"end"
        large_ref = self.store.put_bytes(
            large_content,
            media_type="application/octet-stream",
            kind=ArtifactKind.LOG,
        )

        self.assertEqual(self.store.read(empty_ref), b"")
        self.assertEqual(self.store.read(large_ref), large_content)
        self.assertTrue(self.store.verify(empty_ref))
        self.assertTrue(self.store.verify(large_ref))
        self.assertEqual(empty_ref.encryption, ArtifactEncryption.NONE)
        self.assertEqual(empty_ref.metadata, {})

    def test_duplicate_content_is_idempotently_deduplicated(self) -> None:
        content = b"same immutable content"
        first = self.store.put_bytes(content, metadata={"source": "test"})
        second = self.store.put_bytes(content, metadata={"source": "test"})

        self.assertEqual(first, second)
        self.assertEqual(first.artifact_id, second.artifact_id)
        self.assertEqual(first.uri, second.uri)
        files = [
            path
            for path in self.root.rglob("*")
            if path.is_file() and not path.name.startswith(".")
        ]
        self.assertEqual(files, [self.root / first.uri])

    def test_concurrent_same_content_writes_are_safe(self) -> None:
        content = b"concurrent-content" * 20_000
        barrier = threading.Barrier(12)
        refs: list[ArtifactRef] = []
        errors: list[BaseException] = []
        result_lock = threading.Lock()

        def write() -> None:
            try:
                barrier.wait()
                ref = self.store.put_bytes(content, kind=ArtifactKind.TOOL_RESULT)
                with result_lock:
                    refs.append(ref)
            except BaseException as exc:
                with result_lock:
                    errors.append(exc)

        threads = [threading.Thread(target=write) for _ in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(refs), 12)
        self.assertEqual({ref.artifact_id for ref in refs}, {refs[0].artifact_id})
        self.assertEqual(self.store.read(refs[0]), content)
        self.assertFalse(list(self.root.rglob("*.tmp")))

    def test_corruption_fails_closed_for_read_and_verify(self) -> None:
        ref = self.store.put_bytes(b"trusted")
        path = self.root / ref.uri
        path.write_bytes(b"corrupt")

        with self.assertRaises(ArtifactIntegrityError):
            self.store.read(ref)
        with self.assertRaises(ArtifactIntegrityError):
            self.store.verify(ref)
        with self.assertRaises(ArtifactIntegrityError):
            self.store.put_bytes(b"trusted")

    def test_missing_content_fails_closed_and_exists_is_non_verifying(self) -> None:
        ref = self.store.put_bytes(b"temporary")
        (self.root / ref.uri).unlink()

        self.assertFalse(self.store.exists(ref))
        with self.assertRaises(ArtifactIntegrityError):
            self.store.read(ref)

    def test_invalid_digest_and_uri_traversal_are_rejected(self) -> None:
        valid_digest = hashlib.sha256(b"x").hexdigest()
        common = {
            "artifact_id": "artifact-valid",
            "sha256": valid_digest,
            "size": 1,
        }
        with self.assertRaises(ArtifactValidationError):
            ArtifactRef(**common, uri="../../outside")
        with self.assertRaises(ArtifactValidationError):
            ArtifactRef(
                artifact_id="artifact-invalid",
                sha256="../../outside",
                size=1,
                uri="../../outside",
            )
        with self.assertRaises(ArtifactValidationError):
            ArtifactRef(
                artifact_id="artifact-uppercase",
                sha256=valid_digest.upper(),
                size=1,
                uri=f"sha256/{valid_digest[:2]}/{valid_digest[2:4]}/{valid_digest}",
            )

    def test_symlink_content_is_not_followed(self) -> None:
        ref = self.store.put_bytes(b"inside")
        path = self.root / ref.uri
        outside = Path(self.temp_dir.name) / "outside"
        outside.write_bytes(b"inside")
        path.unlink()
        try:
            path.symlink_to(outside)
        except (NotImplementedError, OSError):
            self.skipTest("symlinks are unavailable")

        with self.assertRaises(ArtifactIntegrityError):
            self.store.exists(ref)
        with self.assertRaises(ArtifactIntegrityError):
            self.store.read(ref)

    def test_parent_symlink_cannot_create_directories_outside_store(self) -> None:
        outside = Path(self.temp_dir.name) / "outside"
        outside.mkdir()
        managed_parent = self.root / "sha256"
        try:
            managed_parent.symlink_to(outside, target_is_directory=True)
        except (NotImplementedError, OSError):
            self.skipTest("symlinks are unavailable")

        with self.assertRaises(ArtifactValidationError):
            self.store.put_bytes(b"must stay inside")

        self.assertEqual(list(outside.iterdir()), [])

    def test_json_is_canonical_and_rejects_non_finite_values(self) -> None:
        left = self.store.put_json({"b": 2, "a": ["中文", 1]})
        right = self.store.put_json({"a": ["中文", 1], "b": 2})

        expected = b'{"a":["\xe4\xb8\xad\xe6\x96\x87",1],"b":2}'
        self.assertEqual(canonical_json_bytes({"b": 2, "a": ["中文", 1]}), expected)
        self.assertEqual(self.store.read(left), expected)
        self.assertEqual(left.artifact_id, right.artifact_id)
        self.assertEqual(left.media_type, "application/json")
        with self.assertRaises(ArtifactValidationError):
            self.store.put_json({"bad": float("nan")})

    def test_replace_failure_leaves_no_final_or_temporary_file(self) -> None:
        real_replace = os.replace

        def fail_replace(source: str | os.PathLike[str], target: str | os.PathLike[str]) -> None:
            self.assertTrue(Path(source).exists())
            self.assertFalse(Path(target).exists())
            raise OSError("fault injection")

        with mock.patch("src.orchestration.artifacts.os.replace", side_effect=fail_replace):
            with self.assertRaises(ArtifactWriteError):
                self.store.put_bytes(b"never visible")

        digest = hashlib.sha256(b"never visible").hexdigest()
        expected = self.root / "sha256" / digest[:2] / digest[2:4] / digest
        self.assertFalse(expected.exists())
        self.assertFalse(list(self.root.rglob("*.tmp")))
        self.assertIs(os.replace, real_replace)

    def test_fault_before_replace_never_exposes_partial_content(self) -> None:
        stages: list[str] = []

        def inject(stage: str, path: Path) -> None:
            stages.append(stage)
            if stage == "before_replace":
                raise RuntimeError("crash")

        store = LocalArtifactStore(self.root, fault_hook=inject)
        with self.assertRaises(ArtifactWriteError):
            store.put_bytes(b"crash-safe")

        digest = hashlib.sha256(b"crash-safe").hexdigest()
        expected = self.root / "sha256" / digest[:2] / digest[2:4] / digest
        self.assertEqual(stages, ["after_temp_fsync", "before_replace"])
        self.assertFalse(expected.exists())
        self.assertFalse(list(self.root.rglob("*.tmp")))

    def test_metadata_is_bounded_detached_and_defaults_to_no_secret(self) -> None:
        original = {"label": "safe"}
        ref = self.store.put_bytes(
            b"metadata",
            sensitivity=ArtifactSensitivity.SECRET,
            metadata=original,
        )
        original["label"] = "mutated"

        self.assertEqual(ref.metadata, {"label": "safe"})
        self.assertEqual(ref.encryption, ArtifactEncryption.NONE)
        with self.assertRaises(TypeError):
            ref.metadata["new"] = "value"  # type: ignore[index]
        with self.assertRaises(ArtifactValidationError):
            self.store.put_bytes(
                b"too-large",
                metadata={"value": "x" * MAX_ARTIFACT_METADATA_BYTES},
            )
        with self.assertRaises(ArtifactValidationError):
            self.store.put_bytes(b"bad", metadata={"value": object()})
        with self.assertRaises(ArtifactValidationError):
            self.store.put_bytes(b"bad-type", metadata=[])  # type: ignore[arg-type]

        # Even if a caller mutates a nested object through the public mapping,
        # the next durable serialization boundary revalidates it.
        nested = self.store.put_bytes(b"nested", metadata={"value": {"safe": True}})
        nested.metadata["value"]["unsafe"] = object()  # type: ignore[index,union-attr]
        with self.assertRaises(ArtifactValidationError):
            nested.to_dict()

    def test_reference_round_trip_and_open_validate_content(self) -> None:
        ref = self.store.put_bytes(
            b"payload",
            kind=ArtifactKind.MODEL_RESPONSE,
            producer_run_id="run-1",
            producer_node_id="node-1",
            producer_attempt_id="attempt-1",
        )
        restored = ArtifactRef.from_dict(json.loads(json.dumps(ref.to_dict())))

        self.assertEqual(restored, ref)
        with self.store.open(restored) as opened:
            self.assertEqual(opened.read(), b"payload")
        self.assertTrue(self.store.exists(restored))

    def test_local_reference_cannot_claim_managed_encryption(self) -> None:
        digest = hashlib.sha256(b"encrypted").hexdigest()
        with self.assertRaises(ArtifactValidationError):
            ArtifactRef(
                artifact_id=f"artifact_sha256_{digest}",
                sha256=digest,
                size=9,
                uri=f"sha256/{digest[:2]}/{digest[2:4]}/{digest}",
                encryption=ArtifactEncryption.DEPLOYMENT_MANAGED,
            )
        managed = ArtifactRef(
            artifact_id="artifact-managed",
            sha256=digest,
            size=9,
            uri=f"sha256/{digest[:2]}/{digest[2:4]}/{digest}",
            encryption=ArtifactEncryption.DEPLOYMENT_MANAGED,
            encryption_key_ref="kms-key-alias",
        )
        self.assertEqual(managed.encryption, ArtifactEncryption.DEPLOYMENT_MANAGED)

    def test_same_bytes_with_different_security_metadata_share_content_not_identity(self) -> None:
        public = self.store.put_bytes(
            b"shared bytes",
            sensitivity=ArtifactSensitivity.PUBLIC,
        )
        secret = self.store.put_bytes(
            b"shared bytes",
            sensitivity=ArtifactSensitivity.SECRET,
        )

        self.assertEqual(public.uri, secret.uri)
        self.assertEqual(public.sha256, secret.sha256)
        self.assertNotEqual(public.artifact_id, secret.artifact_id)
        self.assertEqual(self.store.read(public), b"shared bytes")
        self.assertEqual(self.store.read(secret), b"shared bytes")

    def test_json_result_writer_stores_only_producer_metadata(self) -> None:
        writer = JsonArtifactResultWriter(
            self.store,
            producer_run_id="run-1",
            producer_node_id="node-1",
            producer_attempt_id="attempt-1",
        )
        normalized_result = {
            "response": "complete model response",
            "exit_reason": "CURRENT_TASK_DONE",
            "tool_results": [],
            "turns": 2,
        }

        persisted = writer(normalized_result)
        ref = ArtifactRef.from_dict(persisted)

        self.assertEqual(ref.kind, ArtifactKind.MODEL_RESPONSE)
        self.assertEqual(ref.sensitivity, ArtifactSensitivity.SENSITIVE)
        self.assertEqual(ref.metadata, {})
        self.assertEqual(ref.producer_run_id, "run-1")
        self.assertEqual(ref.producer_node_id, "node-1")
        self.assertEqual(ref.producer_attempt_id, "attempt-1")
        self.assertNotIn("task", persisted)
        self.assertNotIn("prompt", persisted)
        self.assertNotIn("secret", persisted)
        self.assertEqual(
            json.loads(self.store.read(ref)),
            normalized_result,
        )


if __name__ == "__main__":
    unittest.main()
