from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from src.orchestration.artifacts import ArtifactRef
from src.orchestration.event_types import DURABLE_EVENT_TYPES, PUBLIC_EVENT_TYPES
from src.orchestration.models import (
    AttemptRecord,
    AttemptStatus,
    EventRecord,
    NodeRecord,
    NodeStatus,
    RunRecord,
    RunStatus,
)
from src.orchestration.replay import (
    ReplayIntegrityError,
    ReplaySnapshot,
    build_fork_descriptor,
    build_replay_report,
    canonical_json,
    compare_live,
    golden_digest,
    logical_replay,
)
from src.orchestration.store import DurableRunStore

_DIGEST = "a" * 64


class _EventStore:
    def __init__(self, events: list[object]) -> None:
        self.events = events
        self.list_calls = 0

    def list_events(
        self,
        run_id: str,
        *,
        after_seq: int = 0,
        limit: int = 1_000,
    ) -> list[object]:
        self.list_calls += 1
        return [
            event
            for event in self.events
            if getattr(event, "run_id") == run_id
            and getattr(event, "seq") > after_seq
        ][:limit]

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"logical replay called non-event API: {name}")


def _created_run(
    store: DurableRunStore,
    run_id: str = "run-1",
) -> RunRecord:
    raw_input = json.dumps(
        {"credential": "secret-input"},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    digest = hashlib.sha256(raw_input).hexdigest()
    input_ref = ArtifactRef(
        artifact_id=f"artifact_{digest}",
        sha256=digest,
        size=len(raw_input),
        uri=f"sha256/{digest[:2]}/{digest[2:4]}/{digest}",
        created_at=1,
    )
    return store.create_run(
        RunRecord(
            run_id=run_id,
            workflow_id="workflow",
            definition_digest=_DIGEST,
            input={
                "kind": "artifact_input",
                "artifact_refs": [input_ref.to_dict()],
            },
            metadata={"source": "replay-test"},
            created_at=1,
            updated_at=1,
        )
    )


def _scheduled_attempt(
    store: DurableRunStore,
    run_id: str = "run-1",
) -> tuple[RunRecord, NodeRecord, AttemptRecord]:
    created = _created_run(store, run_id)
    store.append_event(
        run_id,
        "run.started",
        run_projection=replace(created, status=RunStatus.RUNNING),
        occurred_at=2,
    )
    running = store.get_run(run_id)
    assert running is not None
    store.append_event(
        run_id,
        "node.created",
        node_projection=NodeRecord(
            run_id,
            "agent",
            "agent",
            created_at=3,
            updated_at=3,
        ),
        occurred_at=3,
    )
    node = store.get_node(run_id, "agent")
    assert node is not None
    store.append_event(
        run_id,
        "node.ready",
        node_projection=replace(node, status=NodeStatus.READY),
        occurred_at=4,
    )
    attempt = AttemptRecord(
        "attempt-1",
        run_id,
        "agent",
        1,
        idempotency_key="sensitive-idempotency-key",
        scheduled_at=5,
        metadata={"label": "replay-attempt"},
    )
    store.append_event(
        run_id,
        "attempt.scheduled",
        attempt_projection=attempt,
        occurred_at=5,
    )
    current_run = store.get_run(run_id)
    current_node = store.get_node(run_id, "agent")
    current_attempt = store.get_attempt("attempt-1")
    assert current_run and current_node and current_attempt
    return current_run, current_node, current_attempt


class OrchestrationReplayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "orchestration.sqlite3"
        self.store = DurableRunStore(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_replay_is_paginated_deterministic_and_calls_no_activity(self) -> None:
        run_one = RunRecord(
            "run",
            "workflow",
            definition_digest=_DIGEST,
            last_event_sequence=1,
            projection_version=1,
            created_at=1,
            updated_at=1,
        )
        run_two = replace(
            run_one,
            status=RunStatus.RUNNING,
            last_event_sequence=2,
            projection_version=2,
            updated_at=2,
        )
        events = [
            EventRecord(
                "run",
                1,
                "run.created",
                payload={"projection": {"run": run_one.to_dict()}},
                occurred_at=1,
            ),
            EventRecord(
                "run",
                2,
                "run.started",
                payload={"projection": {"run": run_two.to_dict()}},
                occurred_at=2,
            ),
        ]
        store = _EventStore(events)

        snapshot = logical_replay(store, "run", page_size=1)

        self.assertEqual(snapshot.run, run_two)
        self.assertEqual(snapshot.event_count, 2)
        self.assertEqual(snapshot.last_sequence, 2)
        self.assertEqual(store.list_calls, 3)

    def test_real_compound_activity_lifecycle_replays_and_has_schema(self) -> None:
        _, node, attempt = _scheduled_attempt(self.store)
        claim, _ = self.store.claim_activity(
            attempt.run_id,
            node.node_id,
            attempt.attempt_id,
            "request-hash",
            "worker",
            now=6,
        )
        self.store.start_activity(
            attempt.run_id,
            node.node_id,
            attempt.attempt_id,
            "worker",
            claim_token=claim.record.claim_token,
            now=7,
        )
        self.store.complete_activity(
            attempt.run_id,
            node.node_id,
            attempt.attempt_id,
            "request-hash",
            "worker",
            claim_token=claim.record.claim_token,
            result={"artifact_ref": "sha256:result"},
            run_status=RunStatus.COMPLETED,
            now=8,
        )

        report = build_replay_report(self.store, attempt.run_id)

        self.assertTrue(report.matches_live)
        self.assertIs(report.snapshot.run.status, RunStatus.COMPLETED)
        self.assertIs(
            report.snapshot.attempts[0].status,
            AttemptStatus.SUCCEEDED,
        )
        self.assertTrue(
            all(
                event.payload["projection"]["schema_version"] == 1
                for event in self.store.list_events(attempt.run_id)
            )
        )

    def test_inert_audit_replays_but_custom_unknown_fails(self) -> None:
        run = _created_run(self.store)
        self.store.append_event(
            run.run_id,
            "audit.note",
            payload={"kind": "test"},
        )
        self.assertEqual(logical_replay(self.store, run.run_id).last_sequence, 2)

        with sqlite3.connect(self.db_path) as connection:
            connection.execute("DROP TRIGGER domain_events_no_update")
            connection.execute(
                """
                UPDATE domain_events
                SET event_type = 'custom.unknown'
                WHERE run_id = ? AND seq = 2
                """,
                (run.run_id,),
            )
        with self.assertRaises(ReplayIntegrityError) as caught:
            logical_replay(self.store, run.run_id)
        self.assertEqual(caught.exception.code, "unknown_event_type")

    def test_event_registry_keeps_policy_decisions_non_public(self) -> None:
        self.assertEqual(
            PUBLIC_EVENT_TYPES,
            DURABLE_EVENT_TYPES - {"policy.decided"},
        )

    def test_malformed_streams_fail_closed(self) -> None:
        malformed = [
            (
                EventRecord(
                    "run",
                    1,
                    "run.created",
                    payload={},
                    occurred_at=1,
                ),
                "missing_projection",
            ),
            (
                SimpleNamespace(
                    run_id="run",
                    seq=1,
                    event_type="run.created",
                    payload={"projection": {}},
                    occurred_at=1,
                    schema_version=99,
                    node_id=None,
                    attempt_id=None,
                ),
                "unknown_schema",
            ),
            (
                EventRecord(
                    "run",
                    2,
                    "run.created",
                    payload={
                        "projection": {
                            "run": RunRecord(
                                "run",
                                "workflow",
                                definition_digest=_DIGEST,
                                last_event_sequence=2,
                                projection_version=1,
                                created_at=1,
                                updated_at=1,
                            ).to_dict()
                        }
                    },
                    occurred_at=1,
                ),
                "sequence_gap",
            ),
        ]
        for event, code in malformed:
            with self.subTest(code=code):
                with self.assertRaises(ReplayIntegrityError) as caught:
                    logical_replay(_EventStore([event]), "run")
                self.assertEqual(caught.exception.code, code)

    def test_store_content_tamper_is_reported_fail_closed(self) -> None:
        run = _created_run(self.store)
        with sqlite3.connect(self.db_path) as connection:
            connection.execute("DROP TRIGGER domain_events_no_update")
            connection.execute(
                "UPDATE domain_events SET payload_json='{}' WHERE run_id=?",
                (run.run_id,),
            )

        with self.assertRaises(ReplayIntegrityError) as caught:
            logical_replay(self.store, run.run_id)
        self.assertEqual(caught.exception.code, "event_content_invalid")

    def test_export_is_redacted_and_golden_is_timestamp_stable(self) -> None:
        _created_run(self.store)
        snapshot = logical_replay(self.store, "run-1")
        shifted = ReplaySnapshot(
            run=replace(snapshot.run, created_at=100, updated_at=101),
            nodes=snapshot.nodes,
            attempts=snapshot.attempts,
            event_count=snapshot.event_count,
            last_sequence=snapshot.last_sequence,
        )

        exported = canonical_json(snapshot)
        raw_input = json.dumps(
            {"credential": "secret-input"},
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        known_raw_digest = hashlib.sha256(raw_input).hexdigest()

        self.assertNotIn("secret-input", exported)
        self.assertNotIn(known_raw_digest, exported)
        self.assertIn('"input_ref"', exported)
        self.assertIn('"redacted":true', exported)
        self.assertEqual(golden_digest(snapshot), golden_digest(shifted))
        self.assertNotEqual(
            golden_digest(snapshot, normalize_timestamps=False),
            golden_digest(shifted, normalize_timestamps=False),
        )

    def test_compare_live_reports_stable_redacted_field_drift(self) -> None:
        run = _created_run(self.store)
        snapshot = logical_replay(self.store, run.run_id)
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                "UPDATE runs SET metadata_json=? WHERE run_id=?",
                (json.dumps({"opaque-drift": True}), run.run_id),
            )

        first = compare_live(self.store, snapshot)
        second = compare_live(self.store, snapshot)

        self.assertEqual(first, second)
        self.assertEqual(
            [(diff.object_type, diff.field) for diff in first],
            [("run", "metadata")],
        )
        serialized = json.dumps([diff.to_dict() for diff in first])
        self.assertNotIn("opaque-drift", serialized)
        self.assertNotIn(
            hashlib.sha256(b'{"opaque-drift":true}').hexdigest(),
            serialized,
        )

    def test_fork_has_fresh_identity_and_no_execution_state(self) -> None:
        _scheduled_attempt(self.store)
        snapshot = logical_replay(self.store, "run-1")
        fork_bytes = b'{"fork":"input"}'
        fork_digest = hashlib.sha256(fork_bytes).hexdigest()
        fork_ref = ArtifactRef(
            artifact_id=f"artifact_{fork_digest}",
            sha256=fork_digest,
            size=len(fork_bytes),
            uri=f"sha256/{fork_digest[:2]}/{fork_digest[2:4]}/{fork_digest}",
            created_at=20,
        )

        descriptor = build_fork_descriptor(
            snapshot,
            "run-fork",
            input_override={
                "kind": "artifact_input",
                "artifact_refs": [fork_ref.to_dict()],
            },
            created_at=20,
        )
        payload = descriptor.to_dict()
        serialized = json.dumps(payload, sort_keys=True)

        self.assertEqual(descriptor.initial_run.run_id, "run-fork")
        self.assertIs(descriptor.initial_run.status, RunStatus.CREATED)
        self.assertEqual(descriptor.initial_run.last_event_sequence, 0)
        self.assertEqual(descriptor.initial_run.projection_version, 0)
        self.assertNotIn("attempts", payload)
        self.assertNotIn("sensitive-idempotency-key", serialized)
        self.assertNotIn("approval", payload["initial_run"])
        self.assertGreaterEqual(
            set(descriptor.excluded_state),
            {
                "attempts",
                "idempotency_records",
                "tool_receipts",
                "approvals",
            },
        )


if __name__ == "__main__":
    unittest.main()
