from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.orchestration.artifacts import ArtifactKind, ArtifactSensitivity, LocalArtifactStore
from src.orchestration.models import (
    AttemptRecord,
    AttemptStatus,
    NodeRecord,
    NodeStatus,
    RunRecord,
    RunStatus,
)
from src.orchestration.store import (
    DurableRunStore,
    ProjectionReplayLimitError,
)
from src.orchestration.web_api import (
    INTEGRITY_REPLAY_PAGE_SIZE,
    MAX_INTEGRITY_REPLAY_EVENTS,
    MAX_INTEGRITY_REPLAY_PAYLOAD_BYTES,
    MAX_INTEGRITY_REPLAY_SECONDS,
    WorkspaceStoreRegistry,
    _load_snapshot,
    _sanitize_attempt,
    _stream_events,
    create_projection_router,
)

DEFINITION_DIGEST = "d" * 64


class NeverDisconnectedRequest:
    async def is_disconnected(self) -> bool:
        return False


class OrchestrationWebApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.root = Path(self._temporary.name)
        self.workspace = self.root / "alpha.ws"
        self.workspace.mkdir()
        self.artifacts = LocalArtifactStore(self.root / "artifacts")
        control_plane = self.root / "external-control-plane" / "alpha.ws"
        control_plane.mkdir(parents=True)
        self.database = control_plane / "orchestration.sqlite3"
        self.store = DurableRunStore(self.database)
        self.secrets = self._seed_run()
        self.registry = WorkspaceStoreRegistry(
            lambda ws: self.database if ws == "alpha.ws" else None
        )
        app = FastAPI()
        app.include_router(
            create_projection_router(
                self.registry,
                integrity_authorizer=lambda _request: True,
            )
        )
        self.client = TestClient(app)
        self.addCleanup(self.client.close)

    def _seed_run(self) -> list[str]:
        secrets = [
            "run-input-secret-91a",
            "run-metadata-label-72b",
            "node-metadata-label-53c",
            "attempt-idempotency-secret-34d",
            "attempt-metadata-label-15e",
            "worker-owner-secret-f60",
            "request-hash-secret-e41",
            "event-payload-secret-c22",
        ]
        input_ref = self.artifacts.put_bytes(
            secrets[0].encode("utf-8"),
            kind=ArtifactKind.GENERIC,
            sensitivity=ArtifactSensitivity.SECRET,
            metadata={},
        )
        self.store.create_run(
            RunRecord(
                run_id="run-web-1",
                workflow_id="web-test-workflow",
                workflow_version=2,
                definition_digest=DEFINITION_DIGEST,
                input={
                    "kind": "artifact_input",
                    "artifact_refs": [input_ref.to_dict()],
                },
                metadata={"label": secrets[1]},
                created_at=100.0,
                updated_at=100.0,
            )
        )
        run = self.store.get_run("run-web-1")
        assert run is not None
        self.store.append_event(
            run.run_id,
            "node.created",
            node_id="work",
            node_projection=NodeRecord(
                run_id=run.run_id,
                node_id="work",
                node_type="tool",
                metadata={"label": secrets[2]},
                created_at=100.0,
                updated_at=100.0,
            ),
            expected_run_version=run.projection_version,
            occurred_at=100.0,
        )
        run = self.store.get_run(run.run_id)
        node = self.store.get_node(run.run_id, "work")
        assert run is not None and node is not None
        self.store.append_event(
            run.run_id,
            "node.ready",
            node_id=node.node_id,
            node_projection=replace(node, status=NodeStatus.READY),
            expected_run_version=run.projection_version,
            expected_node_version=node.projection_version,
            occurred_at=100.5,
        )
        run = self.store.get_run(run.run_id)
        node = self.store.get_node(run.run_id, node.node_id)
        assert run is not None and node is not None
        attempt = AttemptRecord(
            attempt_id="attempt-web-1",
            run_id=run.run_id,
            node_id=node.node_id,
            attempt_number=1,
            idempotency_key=secrets[3],
            activity_kind="tool",
            effect_class="read_only",
            metadata={"label": secrets[4]},
            scheduled_at=101.0,
        )
        self.store.append_event(
            run.run_id,
            "attempt.scheduled",
            node_id=node.node_id,
            attempt_id=attempt.attempt_id,
            node_projection=replace(node, attempt_count=1),
            attempt_projection=attempt,
            expected_run_version=run.projection_version,
            expected_node_version=node.projection_version,
            occurred_at=101.0,
        )
        claim, _event = self.store.claim_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            secrets[6],
            secrets[5],
            lease_seconds=60.0,
            now=101.5,
        )
        self.owner_id = secrets[5]
        self.request_hash = secrets[6]
        self.claim_token = claim.record.claim_token
        secrets.append(claim.record.claim_token)
        self.store.start_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            secrets[5],
            claim_token=claim.record.claim_token,
            now=102.0,
        )
        self.store.append_event(
            run.run_id,
            "policy.decided",
            payload={"secret": secrets[7]},
            occurred_at=103.0,
        )
        self.assertTrue(self.store.verify_projections(run.run_id))
        return secrets

    def _assert_no_secrets(self, response_text: str) -> None:
        for secret in self.secrets:
            self.assertNotIn(secret, response_text)

    def test_list_detail_and_events_are_strictly_redacted(self) -> None:
        responses = [
            self.client.get(
                "/api/orchestration/runs",
                params={"ws": "alpha.ws"},
            ),
            self.client.get(
                "/api/orchestration/runs/run-web-1",
                params={"ws": "alpha.ws"},
            ),
            self.client.get(
                "/api/orchestration/runs/run-web-1/events",
                params={"ws": "alpha.ws"},
            ),
        ]

        for response in responses:
            self.assertEqual(response.status_code, 200)
            self._assert_no_secrets(response.text)

        detail = responses[1].json()
        self.assertNotIn("input", detail["run"])
        self.assertNotIn("output", detail["run"])
        self.assertNotIn("error", detail["run"])
        self.assertNotIn("metadata", detail["run"])
        attempt = detail["attempts"][0]
        for key in (
            "idempotency_key",
            "worker_id",
            "lease_id",
            "fencing_token",
            "result",
            "error",
            "metadata",
        ):
            self.assertNotIn(key, attempt)
        event = responses[2].json()["events"][0]
        self.assertNotIn("payload", event)
        self.assertNotIn("event_id", event)
        self.assertNotIn("projection", response.text)
        self.assertNotIn("policy.decided", responses[2].text)
        self.assertEqual(
            responses[2].json()["events"][-1]["event_type"],
            "orchestration.unknown",
        )

    def test_list_is_bounded_and_integrity_replay_is_explicit(self) -> None:
        read_store = self.registry.get("alpha.ws")
        assert read_store is not None
        with (
            mock.patch.object(
                read_store,
                "verify_projections",
                side_effect=AssertionError(
                    "list must not replay full Event History"
                ),
            ),
            mock.patch.object(
                read_store,
                "list_events",
                side_effect=AssertionError(
                    "list must not scan Event History"
                ),
            ),
        ):
            response = self.client.get(
                "/api/orchestration/runs",
                params={"ws": "alpha.ws", "limit": 1},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()["runs"]), 1)
        with mock.patch.object(
            read_store,
            "verify_projections_bounded",
            wraps=read_store.verify_projections_bounded,
        ) as verify:
            integrity = self.client.get(
                "/api/orchestration/runs/run-web-1/integrity",
                params={"ws": "alpha.ws"},
            )

        self.assertEqual(integrity.status_code, 200)
        self.assertTrue(integrity.json()["projection_matches_events"])
        verify.assert_called_once_with(
            "run-web-1",
            max_events=MAX_INTEGRITY_REPLAY_EVENTS,
            max_payload_bytes=MAX_INTEGRITY_REPLAY_PAYLOAD_BYTES,
            max_wall_seconds=MAX_INTEGRITY_REPLAY_SECONDS,
            page_size=INTEGRITY_REPLAY_PAGE_SIZE,
        )
        self._assert_no_secrets(integrity.text)

    def test_integrity_is_operator_only_and_limits_fail_with_fixed_error(self) -> None:
        app = FastAPI()
        app.include_router(create_projection_router(self.registry))
        with TestClient(app) as default_client:
            routine = default_client.get(
                "/api/orchestration/runs",
                params={"ws": "alpha.ws", "limit": 1},
            )
            denied = default_client.get(
                "/api/orchestration/runs/run-web-1/integrity",
                params={"ws": "alpha.ws"},
            )

        self.assertEqual(routine.status_code, 200)
        self.assertEqual(denied.status_code, 403)
        self.assertEqual(
            denied.json()["detail"],
            "operator authorization required",
        )

        read_store = self.registry.get("alpha.ws")
        assert read_store is not None
        for reason_code in (
            "event_count_limit",
            "payload_bytes_limit",
            "wall_time_limit",
        ):
            with (
                self.subTest(reason_code=reason_code),
                mock.patch.object(
                    read_store,
                    "verify_projections_bounded",
                    side_effect=ProjectionReplayLimitError(reason_code),
                ),
            ):
                limited = self.client.get(
                    "/api/orchestration/runs/run-web-1/integrity",
                    params={"ws": "alpha.ws"},
                )
                self.assertEqual(limited.status_code, 413)
                self.assertEqual(
                    limited.json()["detail"],
                    "orchestration_integrity_limit_exceeded",
                )
                self.assertNotIn(reason_code, limited.text)
                self._assert_no_secrets(limited.text)

    def test_integrity_rejects_persisted_unknown_event_type(self) -> None:
        event_id = self.store.list_events("run-web-1")[-1].event_id
        with sqlite3.connect(self.database) as conn:
            conn.execute("DROP TRIGGER domain_events_no_update")
            conn.execute(
                "UPDATE domain_events SET event_type=? WHERE event_id=?",
                ("unregistered.secret-event", event_id),
            )

        response = self.client.get(
            "/api/orchestration/runs/run-web-1/integrity",
            params={"ws": "alpha.ws"},
        )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(
            response.json()["detail"],
            "orchestration integrity check failed",
        )
        self.assertNotIn("unregistered.secret-event", response.text)
        self.assertNotIn(event_id, response.text)

    def test_detail_and_sse_snapshots_use_bounded_projection_pages(self) -> None:
        read_store = self.registry.get("alpha.ws")
        assert read_store is not None
        nodes = [
            NodeRecord(
                run_id="run-web-1",
                node_id=f"node-{index:03d}",
                node_type="tool",
            )
            for index in range(101)
        ]
        attempts = [
            AttemptRecord(
                attempt_id=f"attempt-{index:03d}",
                run_id="run-web-1",
                node_id="work",
                attempt_number=index + 1,
                idempotency_key=f"idempotency-{index:03d}",
                activity_kind="tool",
                effect_class="read_only",
            )
            for index in range(101)
        ]
        run = read_store.get_run("run-web-1")
        assert run is not None
        with mock.patch.object(
            read_store,
            "get_projection_snapshot",
            return_value=(run, nodes[:4], attempts[:3]),
        ) as get_snapshot:
            detail = self.client.get(
                "/api/orchestration/runs/run-web-1",
                params={
                    "ws": "alpha.ws",
                    "node_limit": 3,
                    "node_offset": 7,
                    "attempt_limit": 2,
                    "attempt_offset": 9,
                },
            )

        self.assertEqual(detail.status_code, 200)
        self.assertEqual(len(detail.json()["nodes"]), 3)
        self.assertEqual(len(detail.json()["attempts"]), 2)
        self.assertEqual(
            detail.json()["node_page"],
            {
                "limit": 3,
                "offset": 7,
                "has_more": True,
                "next_offset": 10,
            },
        )
        self.assertEqual(
            detail.json()["attempt_page"],
            {
                "limit": 2,
                "offset": 9,
                "has_more": True,
                "next_offset": 11,
            },
        )
        get_snapshot.assert_called_once_with(
            "run-web-1",
            node_limit=4,
            node_offset=7,
            attempt_limit=3,
            attempt_offset=9,
        )

        with mock.patch.object(
            read_store,
            "get_projection_snapshot",
            return_value=(run, nodes[:101], attempts[:101]),
        ) as get_snapshot:
            stream = self.client.get(
                "/api/orchestration/runs/run-web-1/stream",
                params={
                    "ws": "alpha.ws",
                    "node_limit": 100,
                    "attempt_limit": 100,
                    "once": "true",
                },
            )

        self.assertEqual(stream.status_code, 200)
        snapshot = _parse_sse(stream.text)[0]["data"]
        self.assertEqual(len(snapshot["nodes"]), 100)
        self.assertEqual(len(snapshot["attempts"]), 100)
        self.assertTrue(snapshot["node_page"]["has_more"])
        self.assertTrue(snapshot["attempt_page"]["has_more"])
        get_snapshot.assert_called_once_with(
            "run-web-1",
            node_limit=101,
            node_offset=0,
            attempt_limit=101,
            attempt_offset=0,
        )

    def test_sse_snapshot_then_reconnect_resumes_from_sequence(self) -> None:
        first = self.client.get(
            "/api/orchestration/runs/run-web-1/stream",
            params={"ws": "alpha.ws", "after_seq": 0, "once": "true"},
        )
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.headers["x-accel-buffering"], "no")
        self._assert_no_secrets(first.text)
        first_messages = _parse_sse(first.text)
        self.assertEqual(first_messages[0]["event"], "snapshot")
        event_sequences = [
            message["data"]["sequence"]
            for message in first_messages
            if message["event"] == "event"
        ]
        self.assertEqual(event_sequences, sorted(event_sequences))
        previous_sequence = event_sequences[-1]

        self.store.append_event(
            "run-web-1",
            "audit.note",
            payload={"secret": "reconnect-secret-not-visible"},
            occurred_at=104.0,
        )
        second = self.client.get(
            "/api/orchestration/runs/run-web-1/stream",
            params={
                "ws": "alpha.ws",
                "after_seq": previous_sequence,
                "once": "true",
            },
        )

        self.assertEqual(second.status_code, 200)
        self.assertNotIn("reconnect-secret-not-visible", second.text)
        second_messages = _parse_sse(second.text)
        resumed = [
            message["data"]["sequence"]
            for message in second_messages
            if message["event"] == "event"
        ]
        self.assertEqual(resumed, [previous_sequence + 1])
        self.assertEqual(
            second_messages[0]["data"]["run"]["last_event_sequence"],
            previous_sequence + 1,
        )
        self.assertNotIn("reconnect-secret-not-visible", second.text)

    def test_activity_commit_rejected_type_is_visible_but_payload_is_redacted(
        self,
    ) -> None:
        event = self.store.append_event(
            "run-web-1",
            "activity.commit_rejected",
            node_id="work",
            attempt_id="attempt-web-1",
            payload={
                "reason_code": "claim_token_mismatch",
                "current_fencing_token": 2,
                "attempt_terminal": True,
                "claim_terminal": True,
            },
            occurred_at=104.0,
        )

        response = self.client.get(
            "/api/orchestration/runs/run-web-1/events",
            params={"ws": "alpha.ws"},
        )

        self.assertEqual(response.status_code, 200)
        projected = next(
            item
            for item in response.json()["events"]
            if item["sequence"] == event.seq
        )
        self.assertEqual(projected["event_type"], "activity.commit_rejected")
        self.assertEqual(
            set(projected),
            {
                "schema_version",
                "sequence",
                "event_type",
                "node_id",
                "attempt_id",
                "occurred_at",
            },
        )
        self.assertNotIn("claim_token_mismatch", response.text)
        self.assertNotIn("current_fencing_token", response.text)
        self.assertNotIn("payload", projected)

    def test_unknown_outcome_diagnostic_is_payload_free(self) -> None:
        secret = "web-diagnostic-secret"
        projected = _sanitize_attempt(
            AttemptRecord(
                "attempt-unknown",
                "run-web-1",
                "work",
                2,
                status=AttemptStatus.OUTCOME_UNKNOWN,
                error={
                    "error_class": "timeout",
                    "error_code": "external_outcome_unknown",
                    "detail": secret,
                },
                scheduled_at=100,
                finished_at=101,
            )
        )
        serialized = json.dumps(projected, sort_keys=True)

        self.assertEqual(
            projected["diagnostic"]["state"],
            "manual_resolution_required",
        )
        self.assertFalse(
            projected["diagnostic"]["automatic_retry_allowed"],
        )
        self.assertNotIn(secret, serialized)

    def test_expired_approval_diagnostic_uses_only_allowlisted_codes(self) -> None:
        secret = "credential_canary_123"
        projected = _sanitize_attempt(
            AttemptRecord(
                "attempt-expired",
                "run-web-1",
                "work",
                2,
                status=AttemptStatus.FAILED,
                error={
                    "class": "policy",
                    "code": secret,
                    "credential": "web-approval-secret",
                },
                scheduled_at=100,
                finished_at=101,
            )
        )
        serialized = json.dumps(projected, sort_keys=True)

        self.assertEqual(
            projected["diagnostic"]["reason_code"],
            "policy_denied",
        )
        self.assertNotIn(secret, serialized)
        self.assertNotIn("web-approval-secret", serialized)

    def test_slow_sse_consumer_does_not_hold_a_writer_lock(self) -> None:
        read_store = self.registry.get("alpha.ws")
        assert read_store is not None
        snapshot = _load_snapshot(read_store, "run-web-1")

        async def scenario() -> None:
            stream = _stream_events(
                NeverDisconnectedRequest(),
                read_store,
                snapshot,
                cursor=snapshot["run"]["last_event_sequence"],
                batch_size=10,
                once=False,
            )
            first = await anext(stream)
            self.assertIn("event: snapshot", first)
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    self.store.append_event,
                    "run-web-1",
                    "audit.note",
                    payload={"visible": False},
                    occurred_at=105.0,
                )
                event = future.result(timeout=2.0)
            self.assertGreater(event.seq, snapshot["run"]["last_event_sequence"])
            await stream.aclose()

        asyncio.run(scenario())

    def test_sse_stops_after_run_becomes_terminal(self) -> None:
        read_store = self.registry.get("alpha.ws")
        assert read_store is not None
        snapshot = _load_snapshot(read_store, "run-web-1")

        async def scenario() -> None:
            stream = _stream_events(
                NeverDisconnectedRequest(),
                read_store,
                snapshot,
                cursor=snapshot["run"]["last_event_sequence"],
                batch_size=10,
                once=False,
            )
            self.assertIn("event: snapshot", await anext(stream))
            self.store.complete_activity(
                "run-web-1",
                "work",
                "attempt-web-1",
                self.request_hash,
                self.owner_id,
                claim_token=self.claim_token,
                result={"artifact_ref": "artifact-safe"},
                attempt_status=AttemptStatus.SUCCEEDED,
                node_status=NodeStatus.SUCCEEDED,
                run_status=RunStatus.COMPLETED,
                now=106.0,
            )
            terminal_event = await anext(stream)
            self.assertIn("event: event", terminal_event)
            self.assertIn('"event_type":"attempt.succeeded"', terminal_event)
            with self.assertRaises(StopAsyncIteration):
                await asyncio.wait_for(anext(stream), timeout=1.0)

        asyncio.run(scenario())

    def test_event_tamper_fails_closed_without_leaking_payload(self) -> None:
        event_id = self.store.list_events("run-web-1")[-1].event_id
        with sqlite3.connect(self.database) as conn:
            conn.execute("DROP TRIGGER domain_events_no_update")
            conn.execute(
                "UPDATE domain_events SET payload_json=? WHERE event_id=?",
                ('{"secret":"tamper-secret-value"}', event_id),
            )

        response = self.client.get(
            "/api/orchestration/runs/run-web-1/events",
            params={"ws": "alpha.ws"},
        )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(
            response.json()["detail"],
            "orchestration integrity check failed",
        )
        self.assertNotIn("tamper-secret-value", response.text)
        self.assertNotIn(event_id, response.text)

    def test_parameters_missing_resources_and_database_paths_fail_closed(self) -> None:
        missing_run = self.client.get(
            "/api/orchestration/runs/missing",
            params={"ws": "alpha.ws"},
        )
        invalid_workspace = self.client.get(
            "/api/orchestration/runs",
            params={"ws": "../alpha.ws"},
        )
        invalid_limit = self.client.get(
            "/api/orchestration/runs/run-web-1/events",
            params={"ws": "alpha.ws", "limit": 501},
        )
        invalid_snapshot_limit = self.client.get(
            "/api/orchestration/runs/run-web-1",
            params={"ws": "alpha.ws", "node_limit": 101},
        )
        injected_path = self.client.get(
            "/api/orchestration/runs",
            params={
                "ws": "alpha.ws",
                "database": "/tmp/attacker.sqlite3",
            },
        )

        self.assertEqual(missing_run.status_code, 404)
        self.assertEqual(invalid_workspace.status_code, 422)
        self.assertEqual(invalid_limit.status_code, 422)
        self.assertEqual(invalid_snapshot_limit.status_code, 422)
        self.assertEqual(injected_path.status_code, 422)

    def test_registry_uses_trusted_external_database_and_is_thread_safe(
        self,
    ) -> None:
        missing_database = (
            self.root
            / "external-control-plane"
            / "missing.ws"
            / "orchestration.sqlite3"
        )
        registry = WorkspaceStoreRegistry(
            lambda ws: {
                "missing.ws": missing_database,
                "alpha.ws": self.database,
            }.get(ws)
        )

        self.assertIsNone(registry.get("missing.ws"))
        self.assertFalse(missing_database.parent.exists())
        with ThreadPoolExecutor(max_workers=4) as pool:
            stores = list(pool.map(lambda _index: registry.get("alpha.ws"), range(8)))
        self.assertTrue(all(store is stores[0] for store in stores))
        with self.assertRaises(Exception):
            registry.get("../alpha.ws")

    def test_registry_rejects_legacy_agent_workspace_database(self) -> None:
        legacy_database = (
            self.workspace / "runtime" / "orchestration.sqlite3"
        )
        legacy_database.parent.mkdir()
        DurableRunStore(legacy_database)
        registry = WorkspaceStoreRegistry(
            lambda ws: legacy_database if ws == "alpha.ws" else None
        )

        with self.assertRaisesRegex(
            Exception,
            "outside Agent workspaces",
        ):
            registry.get("alpha.ws")

    def test_registry_cache_is_bounded_and_evicts_least_recent_store(self) -> None:
        databases: dict[str, Path] = {}
        for name in ("one.ws", "two.ws", "three.ws"):
            database = (
                self.root
                / "external-control-plane-cache"
                / name
                / "orchestration.sqlite3"
            )
            database.parent.mkdir(parents=True)
            DurableRunStore(database)
            databases[name] = database
        created: list[Path] = []

        def factory(path: Path) -> DurableRunStore:
            created.append(path)
            return DurableRunStore(path)

        registry = WorkspaceStoreRegistry(
            databases.get,
            store_factory=factory,
            max_stores=2,
        )
        first = registry.get("one.ws")
        registry.get("two.ws")
        self.assertIs(registry.get("one.ws"), first)
        registry.get("three.ws")
        registry.get("two.ws")

        self.assertEqual(len(registry._stores), 2)
        self.assertEqual(len(created), 4)

    def test_registry_does_not_hold_global_lock_during_store_construction(
        self,
    ) -> None:
        databases: dict[str, Path] = {}
        for name in ("one.ws", "two.ws"):
            database = (
                self.root
                / "external-control-plane-parallel"
                / name
                / "orchestration.sqlite3"
            )
            database.parent.mkdir(parents=True)
            DurableRunStore(database)
            databases[name] = database
        construction_started = threading.Barrier(2)

        def factory(path: Path) -> DurableRunStore:
            construction_started.wait(timeout=2)
            return DurableRunStore(path)

        registry = WorkspaceStoreRegistry(
            databases.get,
            store_factory=factory,
            max_stores=2,
        )
        with ThreadPoolExecutor(max_workers=2) as pool:
            stores = tuple(
                pool.map(registry.get, ("one.ws", "two.ws"))
            )

        self.assertTrue(all(store is not None for store in stores))
        self.assertEqual(len(registry._stores), 2)

    def test_registry_rejects_unbounded_cache_configuration(self) -> None:
        for value in (0, True, 65):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    WorkspaceStoreRegistry(lambda _ws: None, max_stores=value)


def _parse_sse(payload: str) -> list[dict]:
    messages = []
    for block in payload.strip().split("\n\n"):
        if not block or block.startswith(":"):
            continue
        message: dict = {}
        for line in block.splitlines():
            if line.startswith("id: "):
                message["id"] = int(line[4:])
            elif line.startswith("event: "):
                message["event"] = line[7:]
            elif line.startswith("data: "):
                message["data"] = json.loads(line[6:])
        messages.append(message)
    return messages


if __name__ == "__main__":
    unittest.main()
