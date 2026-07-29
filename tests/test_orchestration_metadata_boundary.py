from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.orchestration.artifacts import (
    ArtifactValidationError,
    LocalArtifactStore,
)
from src.orchestration.models import (
    AttemptRecord,
    ModelValidationError,
    NodeRecord,
    RunRecord,
)
from src.orchestration.scheduler import DurableScheduler
from src.orchestration.store import DurableRunStore
from src.orchestration.workflow import compile_workflow

_DEFINITION_DIGEST = "a" * 64
_CANARY = "P1-METADATA-CREDENTIAL-CANARY-91a7"


def _workflow():
    return compile_workflow(
        {
            "schema_version": 2,
            "name": "metadata-boundary",
            "version": 1,
            "nodes": [
                {
                    "id": "work",
                    "kind": "tool",
                    "depends_on": [],
                    "config": {"tool": "inspect", "arguments": {}},
                    "effect_class": "read_only",
                }
            ],
        }
    )


class DurableMetadataBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "orchestration.sqlite3"
        self.store = DurableRunStore(self.database)
        self.artifacts = LocalArtifactStore(
            Path(self.temporary.name) / "artifacts"
        )

    @staticmethod
    def _attacks():
        return (
            {"credential": _CANARY},
            {"COOKIE": _CANARY},
            {"accessToken": _CANARY},
            {"Pass-Word": _CANARY},
            {"private_key": _CANARY},
            {"outer": [{"ＰＡＳＳＷＯＲＤ": _CANARY}]},
            {"nested": {"zero\u200bwidth_token": _CANARY}},
            {"connection": {"api.key": _CANARY}},
        )

    def _assert_database_has_no_canary(self) -> None:
        database_bytes = b"".join(
            path.read_bytes()
            for path in (
                self.database,
                Path(f"{self.database}-wal"),
                Path(f"{self.database}-shm"),
            )
            if path.exists()
        )
        self.assertNotIn(_CANARY.encode("utf-8"), database_bytes)

    def test_run_node_and_attempt_models_reject_nested_key_variants(self) -> None:
        factories = (
            lambda metadata: RunRecord(
                "run",
                "workflow",
                definition_digest=_DEFINITION_DIGEST,
                metadata=metadata,
            ),
            lambda metadata: NodeRecord(
                "run",
                "node",
                "tool",
                metadata=metadata,
            ),
            lambda metadata: AttemptRecord(
                "attempt",
                "run",
                "node",
                1,
                metadata=metadata,
            ),
        )

        for factory in factories:
            for metadata in self._attacks():
                with self.subTest(factory=factory, metadata=metadata):
                    with self.assertRaises(ModelValidationError) as caught:
                        factory(metadata)
                    self.assertNotIn(_CANARY, str(caught.exception))

        self._assert_database_has_no_canary()

    def test_store_revalidates_mutated_projection_metadata(self) -> None:
        safe_run = RunRecord(
            "safe-run",
            "workflow",
            definition_digest=_DEFINITION_DIGEST,
        )
        object.__setattr__(
            safe_run,
            "metadata",
            {"nested": {"privateKeyPem": _CANARY}},
        )

        with self.assertRaises(ModelValidationError):
            self.store.create_run(safe_run)
        self.assertIsNone(self.store.get_run("safe-run"))

        committed = self.store.create_run(
            RunRecord(
                "committed-run",
                "workflow",
                definition_digest=_DEFINITION_DIGEST,
            )
        )
        unsafe_node = NodeRecord("committed-run", "node", "tool")
        object.__setattr__(
            unsafe_node,
            "metadata",
            {"headers": [{"Set-Cookie": _CANARY}]},
        )
        with self.assertRaises(ModelValidationError):
            self.store.append_event(
                committed.run_id,
                "node.created",
                node_projection=unsafe_node,
                expected_run_version=committed.projection_version,
            )
        self.assertEqual(self.store.list_nodes(committed.run_id), [])

        unsafe_attempt = AttemptRecord(
            "attempt",
            committed.run_id,
            "node",
            1,
        )
        object.__setattr__(
            unsafe_attempt,
            "metadata",
            {"authToken": _CANARY},
        )
        with self.assertRaises(ModelValidationError):
            self.store.append_event(
                committed.run_id,
                "attempt.scheduled",
                attempt_projection=unsafe_attempt,
            )
        self.assertEqual(self.store.list_attempts(committed.run_id), [])
        self._assert_database_has_no_canary()

    def test_scheduler_rejects_metadata_before_writing_input(self) -> None:
        writes = []
        scheduler = DurableScheduler(
            self.store,
            _workflow(),
            input_writer=lambda run_id, value: writes.append(
                (run_id, value)
            ),
        )

        with self.assertRaises(ModelValidationError):
            scheduler.create_run(
                "rejected-run",
                input={"value": "must-not-be-written"},
                metadata={
                    "safe": [
                        {"dbPassword": _CANARY},
                    ]
                },
            )

        self.assertEqual(writes, [])
        self.assertIsNone(self.store.get_run("rejected-run"))
        self._assert_database_has_no_canary()

    def test_artifact_metadata_uses_the_same_nested_sensitive_key_rule(
        self,
    ) -> None:
        for metadata in self._attacks():
            with self.subTest(metadata=metadata):
                with self.assertRaises(ArtifactValidationError) as caught:
                    self.artifacts.put_bytes(
                        b"not-persisted",
                        metadata=metadata,
                    )
                self.assertNotIn(_CANARY, str(caught.exception))

        safe = self.artifacts.put_bytes(
            b"safe-metadata",
            metadata={
                "source": "unit-test",
                "scenario": "metadata-boundary",
                "lineage": {"parent": "none"},
            },
        )
        self.assertTrue(self.artifacts.verify(safe))
        payload = safe.to_dict()
        payload["metadata"] = {
            "nested": {"private-key": _CANARY},
        }
        with self.assertRaises(ModelValidationError):
            RunRecord(
                "artifact-input-run",
                "workflow",
                definition_digest=_DEFINITION_DIGEST,
                input={
                    "kind": "artifact_input",
                    "artifact_refs": [payload],
                },
            )

        self._assert_database_has_no_canary()


if __name__ == "__main__":
    unittest.main()
