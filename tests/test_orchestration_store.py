from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from unittest import mock

from src.orchestration import (
    AttemptRecord,
    AttemptStatus,
    ArtifactGCReferenceConflictError,
    ClaimDisposition,
    DurableRunStore,
    FleetShardOwnershipCapacityError,
    FleetShardOwnershipConflict,
    FleetRunRouteCapacityError,
    FleetRunRouteConflict,
    FleetRunRouteRecord,
    IdempotencyConflictError,
    InvalidStateTransition,
    LocalArtifactStore,
    ModelValidationError,
    NodeRecord,
    NodeStatus,
    ProjectionConflictError,
    ProjectionReplayLimitError,
    RunRecord,
    RunHierarchyLimitError,
    RunStatus,
    StoreSchemaError,
    UnknownOutcomeDecision,
    UnknownOutcomeResolution,
    WorkflowBindingConflictError,
)
from src.orchestration.store import ConcurrentProjectionUpdate

TEST_DEFINITION_DIGEST = "a" * 64


class DurableRunStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "runtime" / "orchestration.sqlite3"
        self.store = DurableRunStore(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _install_v5_fleet_owners(self, rows) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                DROP TRIGGER attempts_fleet_route_insert;
                DROP TRIGGER attempts_fleet_route_update;
                DROP TABLE fleet_run_routes;
                DROP TRIGGER attempts_fleet_ownership_insert;
                DROP TRIGGER attempts_fleet_ownership_update;
                DROP TABLE fleet_shard_owners;
                CREATE TABLE fleet_shard_owners (
                    shard_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    pool_id TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    fencing_epoch INTEGER NOT NULL,
                    policy_digest TEXT NOT NULL,
                    assigned_at REAL NOT NULL,
                    UNIQUE(tenant_id, pool_id)
                );
                DELETE FROM schema_migrations WHERE version >= 6;
                PRAGMA user_version = 5;
                """
            )
            conn.executemany(
                """
                INSERT INTO fleet_shard_owners(
                    shard_id, tenant_id, pool_id, owner_id,
                    fencing_epoch, policy_digest, assigned_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def _create_running_run(self, run_id: str = "run-1") -> RunRecord:
        created = self.store.create_run(
            RunRecord(run_id, "workflow", definition_digest=TEST_DEFINITION_DIGEST)
        )
        self.store.append_event(
            run_id,
            "run.started",
            run_projection=replace(created, status=RunStatus.RUNNING),
        )
        run = self.store.get_run(run_id)
        assert run is not None
        return run

    def _schedule_attempt(
        self,
        run_id: str = "run-1",
    ) -> tuple[RunRecord, NodeRecord, AttemptRecord]:
        run = self._create_running_run(run_id)
        self.store.append_event(
            run.run_id,
            "node.created",
            expected_run_version=run.projection_version,
            node_projection=NodeRecord(run.run_id, "agent", "agent"),
        )
        run = self.store.get_run(run.run_id)
        node = self.store.get_node(run.run_id, "agent")
        assert run is not None and node is not None
        self.store.append_event(
            run.run_id,
            "node.ready",
            run_projection=run,
            node_projection=replace(node, status=NodeStatus.READY),
        )
        run = self.store.get_run(run.run_id)
        node = self.store.get_node(run.run_id, node.node_id)
        assert run is not None and node is not None
        attempt = AttemptRecord(
            f"attempt-{run_id}",
            run.run_id,
            node.node_id,
            1,
            scheduled_at=1,
        )
        self.store.append_event(
            run.run_id,
            "attempt.scheduled",
            expected_run_version=run.projection_version,
            attempt_projection=attempt,
        )
        stored_attempt = self.store.get_attempt(attempt.attempt_id)
        run = self.store.get_run(run.run_id)
        assert run is not None and stored_attempt is not None
        return run, node, stored_attempt

    def _create_running_controls(
        self,
        run_id: str,
        *node_ids: str,
    ) -> RunRecord:
        run = self._create_running_run(run_id)
        for node_id in node_ids:
            self.store.append_event(
                run_id,
                "node.created",
                expected_run_version=run.projection_version,
                node_projection=NodeRecord(run_id, node_id, "subworkflow"),
            )
            run = self.store.get_run(run_id)
            node = self.store.get_node(run_id, node_id)
            assert run is not None and node is not None
            self.store.append_event(
                run_id,
                "node.ready",
                expected_run_version=run.projection_version,
                expected_node_version=node.projection_version,
                node_projection=replace(node, status=NodeStatus.READY),
            )
            run = self.store.get_run(run_id)
            node = self.store.get_node(run_id, node_id)
            assert run is not None and node is not None
            self.store.append_event(
                run_id,
                "node.started",
                expected_run_version=run.projection_version,
                expected_node_version=node.projection_version,
                node_projection=replace(node, status=NodeStatus.RUNNING),
            )
            run = self.store.get_run(run_id)
            assert run is not None
        return run

    @staticmethod
    def _child_run(
        child_id: str,
        root_run_id: str,
        parent_node_id: str,
    ) -> RunRecord:
        return RunRecord(
            child_id,
            "child-workflow",
            definition_digest="b" * 64,
            metadata={
                "hierarchy_link": {
                    "root_run_id": root_run_id,
                    "parent_run_id": root_run_id,
                    "parent_node_id": parent_node_id,
                }
            },
        )

    def test_create_run_sets_wal_and_sequence_one_event(self) -> None:
        run = self.store.create_run(
            RunRecord(
                "run-1",
                "workflow",
                definition_digest=TEST_DEFINITION_DIGEST,
                metadata={"source": "test"},
            )
        )

        with sqlite3.connect(self.db_path) as conn:
            journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]

        self.assertEqual(journal_mode, "wal")
        self.assertEqual(run.last_event_sequence, 1)
        self.assertEqual(run.projection_version, 1)
        events = self.store.list_events(run.run_id)
        self.assertEqual([(event.seq, event.event_type) for event in events], [(1, "run.created")])
        self.assertTrue(self.store.verify_projections(run.run_id))

    def test_model_rejects_unbounded_or_non_json_metadata(self) -> None:
        with self.assertRaises(ModelValidationError):
            RunRecord("run", "workflow")
        with self.assertRaises(ModelValidationError):
            RunRecord(
                "run",
                "workflow",
                definition_digest=TEST_DEFINITION_DIGEST,
                metadata={"bad": object()},
            )
        with self.assertRaises(ModelValidationError):
            RunRecord(
                "run",
                "workflow",
                definition_digest=TEST_DEFINITION_DIGEST,
                metadata={"large": "x" * (65 * 1024)},
            )

    def test_attempt_outcome_unknown_is_terminal(self) -> None:
        attempt = AttemptRecord(
            "attempt",
            "run",
            "node",
            1,
            status=AttemptStatus.OUTCOME_UNKNOWN,
            finished_at=10,
            scheduled_at=1,
        )
        self.assertTrue(attempt.status.is_terminal)
        with self.assertRaises(ModelValidationError):
            AttemptRecord(
                "bad-running",
                "run",
                "node",
                1,
                status=AttemptStatus.RUNNING,
                scheduled_at=1,
                started_at=2,
            )
        with self.assertRaises(ModelValidationError):
            AttemptRecord(
                "bad-scheduled",
                "run",
                "node",
                1,
                status=AttemptStatus.SCHEDULED,
                worker_id="worker",
            )

    def test_append_allocates_strict_sequence_under_concurrency(self) -> None:
        run = self._create_running_run()
        barrier = threading.Barrier(32)
        errors: list[BaseException] = []

        def append(index: int) -> None:
            try:
                barrier.wait()
                self.store.append_event(run.run_id, "audit.note", payload={"index": index})
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=append, args=(index,)) for index in range(32)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        events = self.store.list_events(run.run_id)
        self.assertEqual([event.seq for event in events], list(range(1, 35)))
        self.assertTrue(self.store.verify_projections(run.run_id))

    def test_projection_snapshot_cannot_mix_a_concurrent_event_commit(self) -> None:
        before_run = self._create_running_controls("snapshot", "existing")
        before_node = self.store.get_node("snapshot", "existing")
        assert before_node is not None
        snapshot_open = threading.Event()
        finish_snapshot = threading.Event()
        result: list[
            tuple[RunRecord | None, list[NodeRecord], list[AttemptRecord]]
        ] = []

        class BlockingSnapshotStore(DurableRunStore):
            def _fault(self, stage: str) -> None:
                if stage != "snapshot.after_run":
                    return
                snapshot_open.set()
                if not finish_snapshot.wait(timeout=2):
                    raise RuntimeError("snapshot barrier timed out")

        def read_snapshot() -> None:
            result.append(
                BlockingSnapshotStore(
                    self.db_path
                ).get_projection_snapshot(
                    "snapshot",
                    node_limit=10,
                    attempt_limit=10,
                )
            )

        reader = threading.Thread(target=read_snapshot)
        reader.start()
        self.assertTrue(snapshot_open.wait(timeout=2))
        committed = self.store.append_event(
            "snapshot",
            "node.created",
            expected_run_version=before_run.projection_version,
            node_projection=NodeRecord(
                "snapshot",
                "created-concurrently",
                "tool",
            ),
        )
        finish_snapshot.set()
        reader.join(timeout=5)
        self.assertFalse(reader.is_alive())

        old_run, old_nodes, old_attempts = result[0]
        assert old_run is not None
        self.assertEqual(old_run.last_event_sequence, before_run.last_event_sequence)
        self.assertEqual(
            [(node.node_id, node.last_event_sequence) for node in old_nodes],
            [("existing", before_node.last_event_sequence)],
        )
        self.assertEqual(old_attempts, [])

        new_run, new_nodes, new_attempts = self.store.get_projection_snapshot(
            "snapshot",
            node_limit=10,
            attempt_limit=10,
        )
        assert new_run is not None
        self.assertEqual(new_run.last_event_sequence, committed.seq)
        self.assertEqual(
            [node.node_id for node in new_nodes],
            ["existing", "created-concurrently"],
        )
        self.assertEqual(new_attempts, [])
        _run, second_node_page, _attempts = (
            self.store.get_projection_snapshot(
                "snapshot",
                node_limit=1,
                node_offset=1,
                attempt_limit=1,
            )
        )
        self.assertEqual(
            [node.node_id for node in second_node_page],
            ["created-concurrently"],
        )
        with self.assertRaises(ValueError):
            self.store.get_projection_snapshot(
                "snapshot",
                node_limit=1_001,
                attempt_limit=1,
            )

    def test_child_limits_are_atomic_across_store_connections(self) -> None:
        self._create_running_controls("root", "left", "right")
        child = self._child_run("invalid-limit", "root", "left")
        for limits in (
            {
                "max_total_descendants": 10_001,
                "max_children_per_control": 1,
            },
            {
                "max_total_descendants": 1,
                "max_children_per_control": 1_001,
            },
        ):
            with self.subTest(limits=limits):
                with self.assertRaises(ValueError):
                    self.store.create_child_run(child, **limits)

        def race(
            child_ids: tuple[str, str],
            node_ids: tuple[str, str],
            *,
            total_limit: int,
            control_limit: int,
        ) -> list[str]:
            barrier = threading.Barrier(2)
            outcomes: list[str] = []

            def create(child_id: str, node_id: str) -> None:
                store = DurableRunStore(self.db_path)
                barrier.wait()
                try:
                    store.create_child_run(
                        self._child_run(child_id, "root", node_id),
                        max_total_descendants=total_limit,
                        max_children_per_control=control_limit,
                    )
                    outcomes.append("created")
                except RunHierarchyLimitError as exc:
                    outcomes.append(exc.reason_code)

            threads = [
                threading.Thread(target=create, args=(child_id, node_id))
                for child_id, node_id in zip(child_ids, node_ids, strict=True)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
                self.assertFalse(thread.is_alive())
            return outcomes

        total = race(
            ("total-a", "total-b"),
            ("left", "right"),
            total_limit=1,
            control_limit=10,
        )
        self.assertCountEqual(total, ["created", "total_descendants"])
        self.assertEqual(
            self.store.count_descendant_runs("root", stop_after=10),
            1,
        )

        self._create_running_controls("root-control", "control")

        def control_child(child_id: str) -> RunRecord:
            return RunRecord(
                child_id,
                "child-workflow",
                definition_digest="b" * 64,
                metadata={
                    "hierarchy_link": {
                        "root_run_id": "root-control",
                        "parent_run_id": "root-control",
                        "parent_node_id": "control",
                    }
                },
            )

        barrier = threading.Barrier(2)
        outcomes: list[str] = []

        def create_control_child(child_id: str) -> None:
            store = DurableRunStore(self.db_path)
            barrier.wait()
            try:
                store.create_child_run(
                    control_child(child_id),
                    max_total_descendants=10,
                    max_children_per_control=1,
                )
                outcomes.append("created")
            except RunHierarchyLimitError as exc:
                outcomes.append(exc.reason_code)

        threads = [
            threading.Thread(target=create_control_child, args=(child_id,))
            for child_id in ("control-a", "control-b")
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        self.assertCountEqual(outcomes, ["created", "children_per_control"])
        self.assertEqual(
            len(
                self.store.list_child_runs(
                    "root-control",
                    parent_node_id="control",
                    limit=10,
                )
            ),
            1,
        )

    def test_child_create_and_parent_cancel_have_safe_linearization(self) -> None:
        def scenario(root_run_id: str, *, cancel_first: bool) -> list[str]:
            parent = self._create_running_controls(root_run_id, "control")
            start = threading.Barrier(2)
            first_commit = threading.Event()
            child_has_lock = threading.Event()
            allow_child_commit = threading.Event()
            cancel_attempting = threading.Event()
            outcomes: list[str] = []

            class BlockingChildStore(DurableRunStore):
                def _fault(self, stage: str) -> None:
                    if stage != "child_create.after_parent_state":
                        return
                    child_has_lock.set()
                    if not allow_child_commit.wait(timeout=2):
                        raise RuntimeError("child commit barrier timed out")

            def create_child() -> None:
                store_type = DurableRunStore if cancel_first else BlockingChildStore
                store = store_type(self.db_path)
                start.wait()
                if cancel_first and not first_commit.wait(timeout=2):
                    outcomes.append("timeout")
                    return
                try:
                    store.create_child_run(
                        self._child_run(
                            f"{root_run_id}-child",
                            root_run_id,
                            "control",
                        ),
                        max_total_descendants=10,
                        max_children_per_control=10,
                    )
                    outcomes.append("child_created")
                except ProjectionConflictError:
                    outcomes.append("child_rejected")
                finally:
                    if not cancel_first:
                        allow_child_commit.set()

            def cancel_parent() -> None:
                store = DurableRunStore(self.db_path)
                start.wait()
                if not cancel_first and not child_has_lock.wait(timeout=2):
                    outcomes.append("timeout")
                    return
                if not cancel_first:
                    cancel_attempting.set()
                try:
                    store.append_event(
                        parent.run_id,
                        "run.cancelling",
                        expected_run_version=parent.projection_version,
                        run_projection=replace(
                            parent,
                            status=RunStatus.CANCELLING,
                        ),
                    )
                    outcomes.append("parent_cancelled")
                finally:
                    if cancel_first:
                        first_commit.set()

            threads = [
                threading.Thread(target=create_child),
                threading.Thread(target=cancel_parent),
            ]
            for thread in threads:
                thread.start()
            if not cancel_first:
                self.assertTrue(child_has_lock.wait(timeout=2))
                self.assertTrue(cancel_attempting.wait(timeout=2))
                allow_child_commit.set()
            for thread in threads:
                thread.join(timeout=5)
                self.assertFalse(thread.is_alive())
            return outcomes

        self.assertCountEqual(
            scenario("cancel-first", cancel_first=True),
            ["parent_cancelled", "child_rejected"],
        )
        self.assertEqual(
            self.store.list_child_runs("cancel-first", limit=10),
            [],
        )

        self.assertCountEqual(
            scenario("child-first", cancel_first=False),
            ["child_created", "parent_cancelled"],
        )
        self.assertEqual(
            [
                run.run_id
                for run in self.store.list_child_runs(
                    "child-first",
                    parent_node_id="control",
                    limit=10,
                )
            ],
            ["child-first-child"],
        )

    def test_child_create_distinguishes_stale_parent_snapshots_from_invalid_state(
        self,
    ) -> None:
        parent = self._create_running_controls(
            "child-parent-cas",
            "control",
        )
        node = self.store.get_node(parent.run_id, "control")
        assert node is not None
        child = self._child_run(
            "child-parent-cas-child",
            parent.run_id,
            node.node_id,
        )

        self.store.append_event(
            parent.run_id,
            "audit.note",
            expected_run_version=parent.projection_version,
            payload={"reason": "advance parent only"},
        )
        with self.assertRaises(ConcurrentProjectionUpdate) as stale_run:
            self.store.create_child_run(
                child,
                max_total_descendants=10,
                max_children_per_control=10,
                expected_parent_run_version=parent.projection_version,
                expected_parent_node_version=node.projection_version,
            )
        self.assertEqual(stale_run.exception.scope, "parent_run")

        current_run = self.store.get_run(parent.run_id)
        assert current_run is not None
        self.store.append_event(
            parent.run_id,
            "node.started",
            expected_run_version=current_run.projection_version,
            expected_node_version=node.projection_version,
            node_projection=replace(
                node,
                metadata={**node.metadata, "advanced": True},
            ),
        )
        current_run = self.store.get_run(parent.run_id)
        assert current_run is not None
        with self.assertRaises(ConcurrentProjectionUpdate) as stale_node:
            self.store.create_child_run(
                child,
                max_total_descendants=10,
                max_children_per_control=10,
                expected_parent_run_version=current_run.projection_version,
                expected_parent_node_version=node.projection_version,
            )
        self.assertEqual(stale_node.exception.scope, "parent_node")

        current_node = self.store.get_node(parent.run_id, node.node_id)
        assert current_node is not None
        self.store.append_event(
            parent.run_id,
            "run.cancelling",
            expected_run_version=current_run.projection_version,
            run_projection=replace(
                current_run,
                status=RunStatus.CANCELLING,
            ),
        )
        cancelling = self.store.get_run(parent.run_id)
        assert cancelling is not None
        with self.assertRaises(ProjectionConflictError) as invalid_state:
            self.store.create_child_run(
                child,
                max_total_descendants=10,
                max_children_per_control=10,
                expected_parent_run_version=cancelling.projection_version,
                expected_parent_node_version=current_node.projection_version,
            )
        self.assertNotIsInstance(
            invalid_state.exception,
            ConcurrentProjectionUpdate,
        )
        self.assertIn(
            "requires a RUNNING parent",
            str(invalid_state.exception),
        )

    def test_stale_run_projection_cannot_overwrite_newer_state(self) -> None:
        stale = self._create_running_run()
        self.store.append_event(
            stale.run_id,
            "run.pausing",
            run_projection=replace(stale, status=RunStatus.PAUSING),
        )

        with self.assertRaises(ProjectionConflictError):
            self.store.append_event(
                stale.run_id,
                "audit.stale",
                run_projection=replace(stale, metadata={"stale": True}),
            )

        current = self.store.get_run(stale.run_id)
        self.assertEqual(current.status, RunStatus.PAUSING)
        self.assertNotIn("stale", current.metadata)

    def test_concurrent_cas_allows_only_one_state_transition(self) -> None:
        stale = self._create_running_run()
        barrier = threading.Barrier(2)
        outcomes: list[str] = []

        def transition(status: RunStatus, event_type: str) -> None:
            barrier.wait()
            try:
                self.store.append_event(
                    stale.run_id,
                    event_type,
                    run_projection=replace(stale, status=status),
                )
                outcomes.append("committed")
            except ProjectionConflictError:
                outcomes.append("conflict")

        threads = [
            threading.Thread(
                target=transition,
                args=(RunStatus.PAUSING, "run.pausing"),
            ),
            threading.Thread(
                target=transition,
                args=(RunStatus.CANCELLING, "run.cancelling"),
            ),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertCountEqual(outcomes, ["committed", "conflict"])

    def test_transition_matrix_rejects_created_to_completed_and_cancelling_to_running(self) -> None:
        created = self.store.create_run(
            RunRecord("created", "workflow", definition_digest=TEST_DEFINITION_DIGEST)
        )
        with self.assertRaises(InvalidStateTransition):
            self.store.append_event(
                created.run_id,
                "run.completed",
                run_projection=replace(created, status=RunStatus.COMPLETED),
            )

        running = self._create_running_run("cancel")
        self.store.append_event(
            running.run_id,
            "run.cancelling",
            run_projection=replace(running, status=RunStatus.CANCELLING),
        )
        cancelling = self.store.get_run(running.run_id)
        with self.assertRaises(InvalidStateTransition):
            self.store.append_event(
                running.run_id,
                "run.started",
                run_projection=replace(cancelling, status=RunStatus.RUNNING),
            )

    def test_waiting_recovery_requires_explicit_resolution_event(self) -> None:
        running = self._create_running_run()
        self.store.append_event(
            running.run_id,
            "run.waiting_recovery",
            run_projection=replace(running, status=RunStatus.WAITING_RECOVERY),
        )
        waiting = self.store.get_run(running.run_id)
        with self.assertRaises(InvalidStateTransition):
            self.store.append_event(
                running.run_id,
                "run.started",
                run_projection=replace(waiting, status=RunStatus.RUNNING),
            )
        with self.assertRaises(ValueError):
            self.store.append_event(
                running.run_id,
                "run.recovery_resolved",
                run_projection=replace(waiting, status=RunStatus.RUNNING),
            )
        self.assertEqual(
            self.store.get_run(running.run_id).status,
            RunStatus.WAITING_RECOVERY,
        )

    def test_invalid_event_is_rejected_before_transaction(self) -> None:
        run = self._create_running_run()
        before = self.store.get_run(run.run_id)
        with self.assertRaises(ModelValidationError):
            self.store.append_event(run.run_id, "", occurred_at=-1)
        self.assertEqual(self.store.get_run(run.run_id), before)
        self.assertEqual(len(self.store.list_events(run.run_id)), 2)

    def test_run_input_requires_a_canonical_artifact_envelope(self) -> None:
        artifacts = LocalArtifactStore(Path(self.temp_dir.name) / "input-artifacts")
        ref = artifacts.put_bytes(b"bounded input")
        accepted = self.store.create_run(
            RunRecord(
                "canonical-input",
                "workflow",
                definition_digest=TEST_DEFINITION_DIGEST,
                input={
                    "kind": "artifact_input",
                    "artifact_refs": [ref.to_dict()],
                },
            )
        )
        self.assertEqual(
            accepted.input,
            {
                "kind": "artifact_input",
                "artifact_refs": [ref.to_dict()],
            },
        )
        for invalid in (
            {},
            {"kind": "artifact_input", "artifact_refs": []},
            {"kind": "artifact_input", "artifact_refs": [{"sha256": ref.sha256}]},
            {"kind": "raw", "artifact_refs": [ref.to_dict()]},
            {"credential": "must-not-persist"},
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ModelValidationError):
                    RunRecord(
                        "invalid-input",
                        "workflow",
                        definition_digest=TEST_DEFINITION_DIGEST,
                        input=invalid,
                    )

    def test_store_revalidates_mutated_run_before_sqlite_write(self) -> None:
        canary = "store-input-canary-secret-7b43"
        run = RunRecord(
            "mutated-input",
            "workflow",
            definition_digest=TEST_DEFINITION_DIGEST,
        )
        object.__setattr__(run, "input", {"api_key": canary})

        with self.assertRaises(ModelValidationError):
            self.store.create_run(run)

        self.assertIsNone(self.store.get_run("mutated-input"))
        database_bytes = b"".join(
            path.read_bytes()
            for path in (
                self.db_path,
                Path(f"{self.db_path}-wal"),
                Path(f"{self.db_path}-shm"),
            )
            if path.exists()
        )
        self.assertNotIn(canary.encode("utf-8"), database_bytes)

    def test_projection_and_event_roll_back_together(self) -> None:
        run = self._create_running_run()
        with self.assertRaises(InvalidStateTransition):
            self.store.append_event(
                run.run_id,
                "node.succeeded",
                node_projection=NodeRecord(
                    run.run_id,
                    "node",
                    "agent",
                    status=NodeStatus.SUCCEEDED,
                ),
            )
        self.assertIsNone(self.store.get_node(run.run_id, "node"))
        self.assertEqual(len(self.store.list_events(run.run_id)), 2)

    def test_unknown_event_cannot_mutate_projection(self) -> None:
        run = self._create_running_run()
        with self.assertRaises(ProjectionConflictError):
            self.store.append_event(
                run.run_id,
                "custom.note",
                payload={"note": "not a transition"},
                run_projection=replace(run, status=RunStatus.COMPLETED),
            )
        self.assertEqual(self.store.get_run(run.run_id), run)

    def test_unknown_event_is_rejected_without_a_projection(self) -> None:
        run = self._create_running_run()
        before = self.store.list_events(run.run_id)
        with self.assertRaises(ProjectionConflictError):
            self.store.append_event(run.run_id, "custom.note")
        self.assertEqual(self.store.list_events(run.run_id), before)

    def test_event_id_replay_covers_projection_intent(self) -> None:
        run = self._create_running_run()
        event_id = "stable-event"
        event = self.store.append_event(
            run.run_id,
            "run.pausing",
            event_id=event_id,
            run_projection=replace(run, status=RunStatus.PAUSING),
        )
        replay = self.store.append_event(
            run.run_id,
            "run.pausing",
            event_id=event_id,
            run_projection=replace(run, status=RunStatus.PAUSING),
        )
        self.assertEqual(replay, event)
        with self.assertRaises(ProjectionConflictError):
            self.store.append_event(
                run.run_id,
                "run.pausing",
                event_id=event_id,
                run_projection=replace(run, status=RunStatus.CANCELLING),
            )

    def test_domain_events_are_append_only_even_via_sql(self) -> None:
        run = self._create_running_run()
        event_id = self.store.list_events(run.run_id)[0].event_id
        with sqlite3.connect(self.db_path) as conn:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    "UPDATE domain_events SET event_type='changed' WHERE event_id=?",
                    (event_id,),
                )
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("DELETE FROM domain_events WHERE event_id=?", (event_id,))

    def test_event_content_tampering_fails_closed(self) -> None:
        run = self._create_running_run()
        event_id = self.store.list_events(run.run_id)[-1].event_id
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DROP TRIGGER domain_events_no_update")
            conn.execute(
                "UPDATE domain_events SET payload_json='{\"tampered\":true}' "
                "WHERE event_id=?",
                (event_id,),
            )
        with self.assertRaisesRegex(StoreSchemaError, "content digest mismatch"):
            self.store.list_events(run.run_id)

    def test_event_sequence_gap_fails_rebuild(self) -> None:
        run = self._create_running_run()
        event_id = self.store.list_events(run.run_id)[-1].event_id
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DROP TRIGGER domain_events_no_update")
            conn.execute(
                "UPDATE domain_events SET seq=99 WHERE event_id=?",
                (event_id,),
            )
        with self.assertRaisesRegex(StoreSchemaError, "event sequence gap"):
            self.store.rebuild_projections(run.run_id)

    def test_projection_tampering_is_detected_without_corrupting_offline_rebuild(self) -> None:
        run = self._create_running_run()
        canonical_run, _, _ = self.store.rebuild_projections(run.run_id)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE runs SET metadata_json='{\"tampered\":true}' WHERE run_id=?",
                (run.run_id,),
            )
        self.assertFalse(self.store.verify_projections(run.run_id))
        rebuilt_run, _, _ = self.store.rebuild_projections(run.run_id)
        self.assertEqual(rebuilt_run, canonical_run)
        self.assertEqual(rebuilt_run.metadata, {})

    def test_bounded_projection_verification_streams_pages(self) -> None:
        run = self._create_running_run()
        stages: list[str] = []
        with mock.patch.object(
            self.store,
            "_fault",
            side_effect=stages.append,
        ):
            matches = self.store.verify_projections_bounded(
                run.run_id,
                max_events=10,
                max_payload_bytes=1024 * 1024,
                max_wall_seconds=1.0,
                page_size=1,
            )

        self.assertTrue(matches)
        self.assertEqual(
            stages.count("replay.after_event_page"),
            len(self.store.list_events(run.run_id)),
        )

    def test_bounded_verification_uses_one_snapshot_during_concurrent_commit(
        self,
    ) -> None:
        run = self._create_running_run()
        replay_complete = threading.Event()
        release_reader = threading.Event()
        result: list[bool] = []
        errors: list[BaseException] = []

        def barrier(stage: str) -> None:
            if stage == "replay.after_events":
                replay_complete.set()
                if not release_reader.wait(timeout=5):
                    raise AssertionError("reader barrier timed out")

        def verify() -> None:
            try:
                result.append(
                    self.store.verify_projections_bounded(
                        run.run_id,
                        max_events=10,
                        max_payload_bytes=1024 * 1024,
                        max_wall_seconds=10.0,
                        page_size=1,
                    )
                )
            except BaseException as exc:
                errors.append(exc)

        with mock.patch.object(self.store, "_fault", side_effect=barrier):
            reader = threading.Thread(target=verify)
            reader.start()
            try:
                self.assertTrue(replay_complete.wait(timeout=5))
                current = self.store.get_run(run.run_id)
                assert current is not None
                self.store.append_event(
                    run.run_id,
                    "audit.note",
                    payload={"index": 1},
                    expected_run_version=current.projection_version,
                )
            finally:
                release_reader.set()
                reader.join(timeout=5)

        self.assertFalse(reader.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(result, [True])
        self.assertTrue(
            self.store.verify_projections_bounded(
                run.run_id,
                max_events=10,
                max_payload_bytes=1024 * 1024,
                max_wall_seconds=1.0,
                page_size=1,
            )
        )

    def test_bounded_projection_verification_enforces_all_budgets(self) -> None:
        run = self._create_running_run()
        first_event = self.store.list_events(run.run_id, limit=1)[0]
        first_payload_bytes = len(
            json.dumps(
                first_event.payload,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )

        with self.assertRaisesRegex(
            ProjectionReplayLimitError,
            "event_count_limit",
        ):
            self.store.verify_projections_bounded(
                run.run_id,
                max_events=1,
                max_payload_bytes=1024 * 1024,
                max_wall_seconds=1.0,
                page_size=1,
            )
        with self.assertRaisesRegex(
            ProjectionReplayLimitError,
            "payload_bytes_limit",
        ):
            self.store.verify_projections_bounded(
                run.run_id,
                max_events=10,
                max_payload_bytes=first_payload_bytes - 1,
                max_wall_seconds=1.0,
                page_size=1,
            )
        clock_values = iter((0.0, 0.0, 2.0))
        with self.assertRaisesRegex(
            ProjectionReplayLimitError,
            "wall_time_limit",
        ):
            self.store.verify_projections_bounded(
                run.run_id,
                max_events=10,
                max_payload_bytes=1024 * 1024,
                max_wall_seconds=1.0,
                page_size=1,
                clock=lambda: next(clock_values),
            )

    def test_node_attempt_projections_and_rebuild(self) -> None:
        run, node, attempt = self._schedule_attempt()
        self.assertEqual(node.status, NodeStatus.READY)
        self.assertEqual(attempt.status, AttemptStatus.SCHEDULED)
        self.assertEqual(attempt.last_event_sequence, run.last_event_sequence)
        self.assertTrue(self.store.verify_projections(run.run_id))

    def test_idempotency_claim_distinguishes_acquired_completed_and_conflict(self) -> None:
        run = self._create_running_run()
        first = self.store.claim_idempotency(
            run.run_id,
            "logical-operation",
            "request-hash",
            "worker-1",
            now=10,
            lease_seconds=10,
        )
        duplicate = self.store.claim_idempotency(
            run.run_id,
            "logical-operation",
            "request-hash",
            "worker-1",
            now=11,
        )
        self.assertEqual(first.disposition, ClaimDisposition.ACQUIRED)
        self.assertEqual(duplicate.disposition, ClaimDisposition.CONFLICT)

        completed = self.store.complete_idempotency(
            run.run_id,
            "logical-operation",
            "request-hash",
            "worker-1",
            claim_token=first.record.claim_token,
            result={"receipt": "ok"},
            now=12,
        )
        self.assertEqual(completed.result, {"receipt": "ok"})
        replay = self.store.claim_idempotency(
            run.run_id,
            "logical-operation",
            "request-hash",
            "worker-2",
            now=30,
        )
        self.assertEqual(replay.disposition, ClaimDisposition.COMPLETED)

    def test_expired_claim_gets_new_token_and_stale_owner_cannot_complete(self) -> None:
        run = self._create_running_run()
        first = self.store.claim_idempotency(
            run.run_id, "operation", "hash", "worker-1", now=1, lease_seconds=1
        )
        second = self.store.claim_idempotency(
            run.run_id, "operation", "hash", "worker-2", now=3, lease_seconds=10
        )
        self.assertEqual(second.disposition, ClaimDisposition.ACQUIRED)
        self.assertNotEqual(first.record.claim_token, second.record.claim_token)
        with self.assertRaises(IdempotencyConflictError):
            self.store.complete_idempotency(
                run.run_id,
                "operation",
                "hash",
                "worker-1",
                claim_token=first.record.claim_token,
            )

    def test_compound_activity_lifecycle_is_atomic_and_replayable(self) -> None:
        run, node, attempt = self._schedule_attempt()
        claim, claimed_event = self.store.claim_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            now=10,
        )
        self.assertEqual(claim.disposition, ClaimDisposition.ACQUIRED)
        self.assertEqual(claimed_event.event_type, "attempt.claimed")
        self.store.start_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "worker",
            claim_token=claim.record.claim_token,
            now=11,
        )
        completed, event = self.store.complete_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            claim_token=claim.record.claim_token,
            result={"artifact_refs": ["artifact-1"]},
            now=12,
        )
        self.assertEqual(completed.result, {"artifact_refs": ["artifact-1"]})
        self.assertEqual(event.event_type, "attempt.succeeded")
        self.assertEqual(
            self.store.get_attempt(attempt.attempt_id).status,
            AttemptStatus.SUCCEEDED,
        )
        self.assertEqual(
            self.store.get_node(run.run_id, node.node_id).status,
            NodeStatus.SUCCEEDED,
        )
        replayed, replayed_event = self.store.complete_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            claim_token=claim.record.claim_token,
            result={"artifact_refs": ["artifact-1"]},
            now=13,
        )
        self.assertEqual(replayed, completed)
        self.assertEqual(replayed_event, event)
        with self.assertRaises(IdempotencyConflictError):
            self.store.complete_activity(
                run.run_id,
                node.node_id,
                attempt.attempt_id,
                "request",
                "worker",
                claim_token=claim.record.claim_token,
                result={"artifact_refs": ["different"]},
                now=14,
            )

    def test_compound_claim_faults_roll_back_every_write(self) -> None:
        stages = [
            "claim.after_idempotency",
            "event.after_attempt",
            "event.after_run",
            "event.after_insert",
        ]
        for index, stage in enumerate(stages):
            with self.subTest(stage=stage):
                run, node, attempt = self._schedule_attempt(f"claim-fault-{index}")
                original_events = len(self.store.list_events(run.run_id))
                original_run = self.store.get_run(run.run_id)

                def fail(current: str) -> None:
                    if current == stage:
                        raise RuntimeError(stage)

                self.store._fault = fail
                with self.assertRaises(RuntimeError):
                    self.store.claim_activity(
                        run.run_id,
                        node.node_id,
                        attempt.attempt_id,
                        "request",
                        "worker",
                        now=10,
                    )
                self.store._fault = lambda _stage: None
                self.assertIsNone(
                    self.store.get_idempotency(run.run_id, attempt.idempotency_key)
                )
                self.assertEqual(self.store.get_attempt(attempt.attempt_id), attempt)
                self.assertEqual(self.store.get_run(run.run_id), original_run)
                self.assertEqual(len(self.store.list_events(run.run_id)), original_events)

    def test_compound_complete_faults_roll_back_receipt_and_projections(self) -> None:
        stages = [
            "complete.after_idempotency",
            "event.after_node",
            "event.after_attempt",
            "event.after_run",
            "event.after_insert",
        ]
        for index, stage in enumerate(stages):
            with self.subTest(stage=stage):
                run, node, attempt = self._schedule_attempt(f"complete-fault-{index}")
                claim, _ = self.store.claim_activity(
                    run.run_id,
                    node.node_id,
                    attempt.attempt_id,
                    "request",
                    "worker",
                    now=10,
                )
                self.store.start_activity(
                    run.run_id,
                    node.node_id,
                    attempt.attempt_id,
                    "worker",
                    claim_token=claim.record.claim_token,
                    now=11,
                )
                original_run = self.store.get_run(run.run_id)
                original_node = self.store.get_node(run.run_id, node.node_id)
                original_attempt = self.store.get_attempt(attempt.attempt_id)
                original_events = len(self.store.list_events(run.run_id))

                def fail(current: str) -> None:
                    if current == stage:
                        raise RuntimeError(stage)

                self.store._fault = fail
                with self.assertRaises(RuntimeError):
                    self.store.complete_activity(
                        run.run_id,
                        node.node_id,
                        attempt.attempt_id,
                        "request",
                        "worker",
                        claim_token=claim.record.claim_token,
                        result={"ok": True},
                        now=12,
                    )
                self.store._fault = lambda _stage: None
                record = self.store.get_idempotency(run.run_id, attempt.idempotency_key)
                self.assertEqual(record.status.value, "in_progress")
                self.assertEqual(self.store.get_run(run.run_id), original_run)
                self.assertEqual(self.store.get_node(run.run_id, node.node_id), original_node)
                self.assertEqual(self.store.get_attempt(attempt.attempt_id), original_attempt)
                self.assertEqual(len(self.store.list_events(run.run_id)), original_events)

    def test_stale_fencing_token_cannot_complete_activity(self) -> None:
        run, node, attempt = self._schedule_attempt()
        claim, _ = self.store.claim_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            now=10,
        )
        self.store.start_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "worker",
            claim_token=claim.record.claim_token,
            now=11,
        )
        with self.assertRaises(IdempotencyConflictError):
            self.store.complete_activity(
                run.run_id,
                node.node_id,
                attempt.attempt_id,
                "request",
                "worker",
                claim_token="stale-token",
                result={"ok": True},
                now=12,
            )

    def test_outcome_unknown_wins_over_concurrent_cancelling_state(self) -> None:
        run, node, attempt = self._schedule_attempt()
        claim, _ = self.store.claim_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            now=10,
        )
        self.store.start_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "worker",
            claim_token=claim.record.claim_token,
            now=11,
        )
        run = self.store.get_run(run.run_id)
        self.store.append_event(
            run.run_id,
            "run.cancelling",
            run_projection=replace(run, status=RunStatus.CANCELLING),
        )
        self.store.complete_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            claim_token=claim.record.claim_token,
            result={"reason": "external outcome unknown"},
            attempt_status=AttemptStatus.OUTCOME_UNKNOWN,
            now=12,
        )
        self.assertEqual(
            self.store.get_run(run.run_id).status,
            RunStatus.WAITING_RECOVERY,
        )
        self.assertEqual(
            self.store.get_attempt(attempt.attempt_id).status,
            AttemptStatus.OUTCOME_UNKNOWN,
        )

    def test_unknown_outcome_resolution_is_artifact_backed_and_idempotent(
        self,
    ) -> None:
        run, node, attempt = self._schedule_attempt("recovery-success")
        claim, _ = self.store.claim_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            now=10,
        )
        self.store.start_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "worker",
            claim_token=claim.record.claim_token,
            now=11,
        )
        self.store.complete_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            claim_token=claim.record.claim_token,
            result={"reason": "external outcome unknown"},
            attempt_status=AttemptStatus.OUTCOME_UNKNOWN,
            now=12,
        )
        artifacts = LocalArtifactStore(
            Path(self.temp_dir.name) / "recovery-artifacts"
        )
        evidence = artifacts.put_json({"probe": "confirmed"})
        result = artifacts.put_json({"external_operation": "completed"})
        decision = UnknownOutcomeDecision(
            resolution_id="resolution-1",
            run_id=run.run_id,
            node_id=node.node_id,
            attempt_id=attempt.attempt_id,
            resolution=UnknownOutcomeResolution.CONFIRMED_SUCCEEDED,
            evidence_ref=evidence,
            result_ref=result,
        )

        first = self.store.resolve_unknown_outcome(decision, now=13)
        second = self.store.resolve_unknown_outcome(decision, now=99)
        recovered_run = self.store.get_run(run.run_id)
        recovered_node = self.store.get_node(run.run_id, node.node_id)
        preserved_attempt = self.store.get_attempt(attempt.attempt_id)

        self.assertEqual(first, second)
        self.assertEqual(first.event_type, "run.recovery_resolved")
        self.assertEqual(recovered_run.status, RunStatus.RUNNING)
        self.assertEqual(recovered_node.status, NodeStatus.SUCCEEDED)
        self.assertEqual(
            recovered_node.output["artifact_refs"][0]["sha256"],
            result.sha256,
        )
        self.assertEqual(
            preserved_attempt.status,
            AttemptStatus.OUTCOME_UNKNOWN,
        )
        self.assertTrue(self.store.verify_projections(run.run_id))

    def test_unknown_outcome_confirmed_failure_is_auditable_and_fail_closed(
        self,
    ) -> None:
        run, node, attempt = self._schedule_attempt("recovery-failure")
        claim, _ = self.store.claim_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            now=10,
        )
        self.store.start_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "worker",
            claim_token=claim.record.claim_token,
            now=11,
        )
        self.store.complete_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            claim_token=claim.record.claim_token,
            result={"reason": "unknown"},
            attempt_status=AttemptStatus.OUTCOME_UNKNOWN,
            now=12,
        )
        artifacts = LocalArtifactStore(
            Path(self.temp_dir.name) / "failure-recovery-artifacts"
        )
        decision = UnknownOutcomeDecision(
            resolution_id="resolution-failed",
            run_id=run.run_id,
            node_id=node.node_id,
            attempt_id=attempt.attempt_id,
            resolution=UnknownOutcomeResolution.CONFIRMED_FAILED,
            evidence_ref=artifacts.put_json({"probe": "failed"}),
        )

        event = self.store.resolve_unknown_outcome(decision, now=13)
        recovered_node = self.store.get_node(run.run_id, node.node_id)

        self.assertEqual(event.payload["decision_digest"], decision.decision_digest)
        self.assertEqual(recovered_node.status, NodeStatus.FAILED)
        self.assertEqual(
            recovered_node.error,
            {"class": "recovery", "code": "external_failure_confirmed"},
        )
        with self.assertRaises(InvalidStateTransition):
            self.store.resolve_unknown_outcome(
                UnknownOutcomeDecision(
                    resolution_id="resolution-second",
                    run_id=run.run_id,
                    node_id=node.node_id,
                    attempt_id=attempt.attempt_id,
                    resolution=UnknownOutcomeResolution.CONFIRMED_FAILED,
                    evidence_ref=decision.evidence_ref,
                ),
                now=14,
            )

    def test_conflicting_recovery_decisions_linearize_exactly_once(self) -> None:
        run, node, attempt = self._schedule_attempt("recovery-race")
        claim, _ = self.store.claim_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            now=10,
        )
        self.store.start_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "worker",
            claim_token=claim.record.claim_token,
            now=11,
        )
        self.store.complete_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            claim_token=claim.record.claim_token,
            result={"reason": "unknown"},
            attempt_status=AttemptStatus.OUTCOME_UNKNOWN,
            now=12,
        )
        artifacts = LocalArtifactStore(
            Path(self.temp_dir.name) / "race-recovery-artifacts"
        )
        evidence = artifacts.put_json({"probe": "race"})
        decisions = (
            UnknownOutcomeDecision(
                "race-success",
                run.run_id,
                node.node_id,
                attempt.attempt_id,
                UnknownOutcomeResolution.CONFIRMED_SUCCEEDED,
                evidence,
                artifacts.put_json({"result": "success"}),
            ),
            UnknownOutcomeDecision(
                "race-failure",
                run.run_id,
                node.node_id,
                attempt.attempt_id,
                UnknownOutcomeResolution.CONFIRMED_FAILED,
                evidence,
            ),
        )
        barrier = threading.Barrier(2)

        def resolve(decision: UnknownOutcomeDecision) -> str:
            barrier.wait()
            try:
                self.store.resolve_unknown_outcome(decision, now=13)
            except InvalidStateTransition:
                return "conflict"
            return "committed"

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(resolve, decisions))

        self.assertCountEqual(outcomes, ["committed", "conflict"])
        resolution_events = [
            event
            for event in self.store.list_events(run.run_id)
            if event.event_type == "run.recovery_resolved"
        ]
        self.assertEqual(len(resolution_events), 1)
        self.assertEqual(
            self.store.get_attempt(attempt.attempt_id).status,
            AttemptStatus.OUTCOME_UNKNOWN,
        )

    def test_resolving_one_of_multiple_unknowns_keeps_run_waiting(self) -> None:
        run = self._create_running_run("multi-recovery")
        active: list[tuple[NodeRecord, AttemptRecord, object]] = []
        for index, node_id in enumerate(("first", "second"), start=1):
            self.store.append_event(
                run.run_id,
                "node.created",
                node_projection=NodeRecord(run.run_id, node_id, "tool"),
            )
            current_run = self.store.get_run(run.run_id)
            node = self.store.get_node(run.run_id, node_id)
            assert current_run is not None and node is not None
            self.store.append_event(
                run.run_id,
                "node.ready",
                run_projection=current_run,
                node_projection=replace(node, status=NodeStatus.READY),
            )
            current_run = self.store.get_run(run.run_id)
            node = self.store.get_node(run.run_id, node_id)
            assert current_run is not None and node is not None
            attempt = AttemptRecord(
                f"attempt-{node_id}",
                run.run_id,
                node_id,
                1,
                idempotency_key=f"key-{node_id}",
                scheduled_at=float(index),
            )
            self.store.append_event(
                run.run_id,
                "attempt.scheduled",
                run_projection=current_run,
                attempt_projection=attempt,
            )
            stored = self.store.get_attempt(attempt.attempt_id)
            assert stored is not None
            claim, _ = self.store.claim_activity(
                run.run_id,
                node_id,
                stored.attempt_id,
                f"request-{node_id}",
                f"worker-{node_id}",
                now=10,
            )
            self.store.start_activity(
                run.run_id,
                node_id,
                stored.attempt_id,
                f"worker-{node_id}",
                claim_token=claim.record.claim_token,
                now=11,
            )
            active.append((node, stored, claim))
        for node, attempt, claim in active:
            self.store.complete_activity(
                run.run_id,
                node.node_id,
                attempt.attempt_id,
                f"request-{node.node_id}",
                f"worker-{node.node_id}",
                claim_token=claim.record.claim_token,
                result={"reason": "unknown"},
                attempt_status=AttemptStatus.OUTCOME_UNKNOWN,
                now=12,
            )
        artifacts = LocalArtifactStore(
            Path(self.temp_dir.name) / "multi-recovery-artifacts"
        )
        evidence = artifacts.put_json({"probe": "confirmed"})
        result = artifacts.put_json({"result": "success"})

        for index, (node, attempt, _claim) in enumerate(active, start=1):
            self.store.resolve_unknown_outcome(
                UnknownOutcomeDecision(
                    f"multi-resolution-{index}",
                    run.run_id,
                    node.node_id,
                    attempt.attempt_id,
                    UnknownOutcomeResolution.CONFIRMED_SUCCEEDED,
                    evidence,
                    result,
                ),
                now=12 + index,
            )
            expected = (
                RunStatus.WAITING_RECOVERY
                if index == 1
                else RunStatus.RUNNING
            )
            self.assertEqual(self.store.get_run(run.run_id).status, expected)

        self.assertTrue(self.store.verify_projections(run.run_id))

    def test_node_pause_resumes_through_ready(self) -> None:
        run, node, attempt = self._schedule_attempt()
        claim, _ = self.store.claim_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            now=10,
        )
        self.store.start_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "worker",
            claim_token=claim.record.claim_token,
            now=11,
        )
        self.store.pause_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            claim_token=claim.record.claim_token,
            now=12,
        )
        run = self.store.get_run(run.run_id)
        node = self.store.get_node(run.run_id, node.node_id)
        paused_attempt = self.store.get_attempt(attempt.attempt_id)
        self.assertEqual(node.status, NodeStatus.PAUSED)
        self.assertEqual(paused_attempt.status, AttemptStatus.CANCELLED)
        self.store.append_event(
            run.run_id,
            "node.ready",
            run_projection=run,
            node_projection=replace(node, status=NodeStatus.READY),
        )
        run = self.store.get_run(run.run_id)
        next_attempt = AttemptRecord(
            "attempt-resumed",
            run.run_id,
            node.node_id,
            2,
            idempotency_key=f"{run.run_id}:{node.node_id}:resume-2",
            scheduled_at=13,
        )
        self.store.append_event(
            run.run_id,
            "attempt.scheduled",
            expected_run_version=run.projection_version,
            attempt_projection=next_attempt,
        )
        self.assertEqual(
            self.store.get_node(run.run_id, node.node_id).status,
            NodeStatus.READY,
        )
        self.assertEqual(
            self.store.get_attempt(next_attempt.attempt_id).status,
            AttemptStatus.SCHEDULED,
        )

    def test_direct_node_pause_without_cancelling_attempt_is_rejected(self) -> None:
        run, node, attempt = self._schedule_attempt()
        claim, _ = self.store.claim_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            now=10,
        )
        self.store.start_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "worker",
            claim_token=claim.record.claim_token,
            now=11,
        )
        run = self.store.get_run(run.run_id)
        node = self.store.get_node(run.run_id, node.node_id)
        with self.assertRaises(ProjectionConflictError):
            self.store.append_event(
                run.run_id,
                "node.paused",
                run_projection=run,
                node_projection=replace(node, status=NodeStatus.PAUSED),
            )

    def test_completion_statuses_must_be_consistent(self) -> None:
        run, node, attempt = self._schedule_attempt()
        claim, _ = self.store.claim_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            now=10,
        )
        self.store.start_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "worker",
            claim_token=claim.record.claim_token,
            now=11,
        )
        with self.assertRaises(InvalidStateTransition):
            self.store.complete_activity(
                run.run_id,
                node.node_id,
                attempt.attempt_id,
                "request",
                "worker",
                claim_token=claim.record.claim_token,
                result={"ok": False},
                attempt_status=AttemptStatus.FAILED,
                node_status=NodeStatus.SUCCEEDED,
                now=12,
            )

    def test_terminal_run_rejects_active_or_incompatible_children(self) -> None:
        run, node, attempt = self._schedule_attempt()
        run = self.store.get_run(run.run_id)
        with self.assertRaises(InvalidStateTransition):
            self.store.append_event(
                run.run_id,
                "run.completed",
                run_projection=replace(run, status=RunStatus.COMPLETED),
            )
        self.assertEqual(
            self.store.get_attempt(attempt.attempt_id).status,
            AttemptStatus.SCHEDULED,
        )

    def test_failed_run_rejects_nonterminal_node_even_without_active_attempt(self) -> None:
        run = self._create_running_run()
        self.store.append_event(
            run.run_id,
            "node.created",
            expected_run_version=run.projection_version,
            node_projection=NodeRecord(run.run_id, "pending-node", "agent"),
        )
        run = self.store.get_run(run.run_id)
        with self.assertRaisesRegex(InvalidStateTransition, "nonterminal Node"):
            self.store.append_event(
                run.run_id,
                "run.failed",
                run_projection=replace(
                    run,
                    status=RunStatus.FAILED,
                    error={"error_code": "run_failure"},
                ),
            )

    def test_cancelled_run_allows_node_that_completed_before_cancel(self) -> None:
        run, node, attempt = self._schedule_attempt()
        claim, _ = self.store.claim_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            now=10,
        )
        self.store.start_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "worker",
            claim_token=claim.record.claim_token,
            now=11,
        )
        self.store.complete_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            claim_token=claim.record.claim_token,
            result={"ok": True},
            now=12,
        )
        run = self.store.get_run(run.run_id)
        self.store.append_event(
            run.run_id,
            "run.cancelling",
            run_projection=replace(run, status=RunStatus.CANCELLING),
        )
        run = self.store.get_run(run.run_id)
        self.store.append_event(
            run.run_id,
            "run.cancelled",
            run_projection=replace(run, status=RunStatus.CANCELLED),
        )
        self.assertEqual(
            self.store.get_node(run.run_id, node.node_id).status,
            NodeStatus.SUCCEEDED,
        )

    def test_only_one_active_attempt_per_node_including_scheduled(self) -> None:
        run, node, first = self._schedule_attempt()
        run = self.store.get_run(run.run_id)
        second = AttemptRecord(
            "attempt-second",
            run.run_id,
            node.node_id,
            2,
            idempotency_key=f"{run.run_id}:{node.node_id}:second",
            scheduled_at=2,
        )
        with self.assertRaises(ProjectionConflictError):
            self.store.append_event(
                run.run_id,
                "attempt.scheduled",
                expected_run_version=run.projection_version,
                attempt_projection=second,
            )
        self.assertEqual(
            self.store.get_attempt(first.attempt_id).status,
            AttemptStatus.SCHEDULED,
        )

    def test_reopening_database_restores_projection_and_events(self) -> None:
        run, _, _ = self._schedule_attempt()
        reopened = DurableRunStore(self.db_path)
        self.assertEqual(reopened.get_run(run.run_id), run)
        self.assertTrue(reopened.verify_projections(run.run_id))

    def test_fleet_run_route_is_durable_immutable_and_aba_safe(
        self,
    ) -> None:
        self._create_running_run("fleet-routed-run")
        first = self.store.register_fleet_run_route(
            "fleet-routed-run",
            "tenant-a",
            "pool-a",
            now=10,
        )

        self.assertIsInstance(first, FleetRunRouteRecord)
        self.assertTrue(first.enabled)
        self.assertEqual(first.generation, 1)
        self.assertEqual(
            DurableRunStore(self.db_path).register_fleet_run_route(
                "fleet-routed-run",
                "tenant-a",
                "pool-a",
                now=20,
            ),
            first,
        )
        with self.assertRaisesRegex(
            FleetRunRouteConflict,
            "immutable",
        ):
            self.store.register_fleet_run_route(
                "fleet-routed-run",
                "tenant-b",
                "pool-a",
                now=20,
            )

        disabled = self.store.withdraw_fleet_run_route(first, now=5)
        self.assertFalse(disabled.enabled)
        self.assertEqual(disabled.generation, 2)
        self.assertEqual(disabled.updated_at, 10)
        self.assertNotEqual(disabled.route_digest, first.route_digest)
        self.assertEqual(
            self.store.withdraw_fleet_run_route(first, now=12),
            disabled,
        )
        with self.assertRaisesRegex(
            FleetRunRouteConflict,
            "current authority",
        ):
            self.store.register_fleet_run_route(
                "fleet-routed-run",
                "tenant-a",
                "pool-a",
                now=12,
            )

        enabled = DurableRunStore(
            self.db_path
        ).register_fleet_run_route(
            "fleet-routed-run",
            "tenant-a",
            "pool-a",
            expected=disabled,
            now=4,
        )
        self.assertTrue(enabled.enabled)
        self.assertEqual(enabled.generation, 3)
        self.assertEqual(enabled.updated_at, 10)
        self.assertNotEqual(enabled.route_digest, first.route_digest)
        self.assertEqual(
            self.store.get_fleet_run_route("fleet-routed-run"),
            enabled,
        )

    def test_fleet_run_route_listing_is_bounded_and_running_only(
        self,
    ) -> None:
        self._create_running_run("route-running")
        self.store.create_run(
            RunRecord(
                "route-created",
                "workflow",
                definition_digest=TEST_DEFINITION_DIGEST,
            )
        )
        running = self.store.register_fleet_run_route(
            "route-running",
            "tenant-b",
            "pool-b",
            now=1,
        )
        created = self.store.register_fleet_run_route(
            "route-created",
            "tenant-a",
            "pool-a",
            now=1,
        )

        self.assertEqual(
            self.store.list_fleet_run_routes(running_only=True),
            [running],
        )
        self.assertEqual(
            self.store.list_fleet_run_routes(limit=1),
            [created],
        )
        self.assertEqual(
            self.store.list_fleet_run_routes(limit=1, offset=1),
            [running],
        )
        self.store.withdraw_fleet_run_route(created, now=2)
        self.assertEqual(
            self.store.list_fleet_run_routes(enabled_only=True),
            [running],
        )
        self.assertEqual(
            len(
                self.store.list_fleet_run_routes(
                    enabled_only=False,
                )
            ),
            2,
        )
        with self.assertRaises(ValueError):
            self.store.list_fleet_run_routes(offset=100_001)

    def test_fleet_run_route_capacity_and_faults_never_evict(
        self,
    ) -> None:
        self._create_running_run("route-retained")
        self._create_running_run("route-overflow")
        with mock.patch(
            "src.orchestration.store.MAX_FLEET_RUN_ROUTES",
            1,
        ):
            retained = self.store.register_fleet_run_route(
                "route-retained",
                "tenant-a",
                "pool-a",
                now=1,
            )
            with self.assertRaises(FleetRunRouteCapacityError):
                self.store.register_fleet_run_route(
                    "route-overflow",
                    "tenant-b",
                    "pool-b",
                    now=1,
                )
        self.assertEqual(
            self.store.get_fleet_run_route("route-retained"),
            retained,
        )
        self.assertIsNone(
            self.store.get_fleet_run_route("route-overflow")
        )

        def fail(stage: str) -> None:
            if stage == "fleet_run_route.after_withdraw":
                raise RuntimeError("route withdrawal fault")

        self.store._fault = fail
        with self.assertRaisesRegex(RuntimeError, "withdrawal fault"):
            self.store.withdraw_fleet_run_route(retained, now=2)
        self.store._fault = lambda _stage: None
        self.assertEqual(
            self.store.get_fleet_run_route("route-retained"),
            retained,
        )

    def test_fleet_run_route_insert_fault_and_generation_exhaustion(
        self,
    ) -> None:
        self._create_running_run("route-insert-fault")

        def fail(stage: str) -> None:
            if stage == "fleet_run_route.after_insert":
                raise RuntimeError("route insert fault")

        self.store._fault = fail
        with self.assertRaisesRegex(RuntimeError, "insert fault"):
            self.store.register_fleet_run_route(
                "route-insert-fault",
                "tenant-a",
                "pool-a",
                now=1,
            )
        self.store._fault = lambda _stage: None
        self.assertIsNone(
            self.store.get_fleet_run_route("route-insert-fault")
        )
        route = self.store.register_fleet_run_route(
            "route-insert-fault",
            "tenant-a",
            "pool-a",
            now=2,
        )
        with mock.patch(
            "src.orchestration.store.MAX_FLEET_RUN_ROUTE_GENERATION",
            route.generation,
        ):
            with self.assertRaisesRegex(
                FleetRunRouteConflict,
                "generation is exhausted",
            ):
                self.store.withdraw_fleet_run_route(route, now=3)
        self.assertEqual(
            self.store.get_fleet_run_route(route.run_id),
            route,
        )

    def test_terminal_run_cannot_enter_fleet_routing(
        self,
    ) -> None:
        run = self._create_running_run("terminal-route")
        self.store.append_event(
            run.run_id,
            "run.completed",
            expected_run_version=run.projection_version,
            run_projection=replace(
                run,
                status=RunStatus.COMPLETED,
            ),
        )

        with self.assertRaisesRegex(
            FleetRunRouteConflict,
            "terminal Run",
        ):
            self.store.register_fleet_run_route(
                run.run_id,
                "tenant-a",
                "pool-a",
                now=3,
            )

    def test_concurrent_fleet_run_route_enable_has_one_winner(
        self,
    ) -> None:
        self._create_running_run("route-enable-race")
        first = self.store.register_fleet_run_route(
            "route-enable-race",
            "tenant-a",
            "pool-a",
            now=1,
        )
        disabled = self.store.withdraw_fleet_run_route(first, now=2)
        barrier = threading.Barrier(2)

        def enable(_index: int):
            contender = DurableRunStore(self.db_path)
            barrier.wait(timeout=5)
            try:
                return contender.register_fleet_run_route(
                    disabled.run_id,
                    disabled.tenant_id,
                    disabled.pool_id,
                    expected=disabled,
                    now=3,
                )
            except FleetRunRouteConflict:
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(enable, range(2)))

        self.assertEqual(
            sum(outcome is not None for outcome in outcomes),
            1,
        )
        winner = next(
            outcome for outcome in outcomes if outcome is not None
        )
        self.assertEqual(winner.generation, 3)
        self.assertEqual(
            self.store.get_fleet_run_route(disabled.run_id),
            winner,
        )

    def test_schema_trigger_blocks_legacy_claim_on_registered_run(
        self,
    ) -> None:
        _run, _node, attempt = self._schedule_attempt(
            "fleet-route-trigger"
        )
        route = self.store.register_fleet_run_route(
            "fleet-route-trigger",
            "tenant-a",
            "pool-a",
            now=1,
        )
        legacy_scope = {
            "schema_version": 1,
            "task_id": "trigger-task",
            "tenant_id": route.tenant_id,
            "pool_id": route.pool_id,
            "routing_policy_digest": "a" * 64,
            "quota_policy_digest": "b" * 64,
            "max_active_tasks": 8,
            "tenant_concurrency": 4,
            "pool_concurrency": 4,
        }
        metadata = dict(attempt.metadata)
        metadata["fleet_admission"] = legacy_scope

        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "Run route is required or stale",
        ):
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    UPDATE attempts
                    SET status = 'claimed', worker_id = ?, metadata_json = ?
                    WHERE attempt_id = ?
                    """,
                    (
                        "remote-session:" + ("d" * 64),
                        json.dumps(metadata),
                        attempt.attempt_id,
                    ),
                )
        self.assertEqual(
            self.store.get_attempt(attempt.attempt_id).status,
            AttemptStatus.SCHEDULED,
        )

    def test_fleet_shard_transfer_is_clock_free_monotonic_and_aba_safe(
        self,
    ) -> None:
        policy = "b" * 64
        first = self.store.claim_fleet_shard(
            "shard-a",
            "control-a",
            policy,
            pool_id="pool-a",
            now=10,
        )
        recovered = DurableRunStore(self.db_path).claim_fleet_shard(
            "shard-a",
            "control-a",
            policy,
            pool_id="pool-a",
            now=11,
        )
        self.assertEqual(recovered, first)

        second = self.store.transfer_fleet_shard(
            first,
            new_owner_id="control-b",
            new_policy_digest=policy,
            now=12,
        )
        self.assertEqual(second.fencing_epoch, 2)
        with self.assertRaises(FleetShardOwnershipConflict):
            self.store.transfer_fleet_shard(
                first,
                new_owner_id="control-stale",
                new_policy_digest=policy,
                now=13,
            )
        third = self.store.transfer_fleet_shard(
            second,
            new_owner_id="control-a",
            new_policy_digest=policy,
            now=9,
        )

        self.assertEqual(third.fencing_epoch, 3)
        self.assertNotEqual(third, first)
        self.assertEqual(
            DurableRunStore(self.db_path).claim_fleet_shard(
                "shard-a",
                "control-a",
                policy,
                pool_id="pool-a",
                now=8,
            ),
            third,
        )
        self.assertEqual(
            DurableRunStore(self.db_path).get_fleet_shard_ownership(
                "shard-a"
            ),
            third,
        )
        with self.assertRaises(FleetShardOwnershipConflict):
            self.store.claim_fleet_shard(
                "shard-alias",
                "control-a",
                policy,
                pool_id="pool-a",
                now=15,
            )

    def test_concurrent_shard_aliases_cannot_share_a_routing_scope(
        self,
    ) -> None:
        barrier = threading.Barrier(2)

        def claim(shard_id: str):
            contender = DurableRunStore(self.db_path)
            barrier.wait(timeout=5)
            try:
                return contender.claim_fleet_shard(
                    shard_id,
                    f"owner-{shard_id}",
                    "b" * 64,
                    pool_id="pool-shared",
                    now=10,
                )
            except FleetShardOwnershipConflict:
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(
                pool.map(claim, ("shard-left", "shard-right"))
            )

        self.assertEqual(
            sum(outcome is not None for outcome in outcomes),
            1,
        )

    def test_concurrent_fleet_shard_transfer_has_one_winner(self) -> None:
        current = self.store.claim_fleet_shard(
            "shard-race",
            "control-a",
            "c" * 64,
            pool_id="pool-a",
            now=10,
        )
        barrier = threading.Barrier(2)

        def transfer(owner_id: str):
            contender = DurableRunStore(self.db_path)
            barrier.wait(timeout=5)
            try:
                return contender.transfer_fleet_shard(
                    current,
                    new_owner_id=owner_id,
                    new_policy_digest="c" * 64,
                    now=11,
                )
            except FleetShardOwnershipConflict:
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(
                pool.map(transfer, ("control-b", "control-c"))
            )

        self.assertEqual(
            sum(outcome is not None for outcome in outcomes),
            1,
        )
        winner = next(
            outcome for outcome in outcomes if outcome is not None
        )
        self.assertEqual(
            self.store.get_fleet_shard_ownership("shard-race"),
            winner,
        )

    def test_schema_trigger_fences_already_running_legacy_processes(
        self,
    ) -> None:
        _run, _node, attempt = self._schedule_attempt(
            "fleet-trigger-guard"
        )
        first = self.store.claim_fleet_shard(
            "trigger-shard",
            "control-a",
            "c" * 64,
            pool_id="pool-a",
            now=10,
        )
        scope = {
            "schema_version": 1,
            "task_id": "trigger-task",
            "tenant_id": "tenant-a",
            "pool_id": "pool-a",
            "routing_policy_digest": "a" * 64,
            "quota_policy_digest": "b" * 64,
            "max_active_tasks": 8,
            "tenant_concurrency": 4,
            "pool_concurrency": 4,
        }

        def legacy_claim(metadata: dict) -> None:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    UPDATE attempts
                    SET status = 'claimed', worker_id = ?, metadata_json = ?
                    WHERE attempt_id = ?
                    """,
                    (
                        "remote-session:" + ("d" * 64),
                        json.dumps(metadata),
                        attempt.attempt_id,
                    ),
                )

        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "ownership is required or stale",
        ):
            legacy_claim(dict(attempt.metadata))
        missing = dict(attempt.metadata)
        missing["fleet_admission"] = scope
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "ownership is required or stale",
        ):
            legacy_claim(missing)
        legacy_v1 = dict(missing)
        legacy_v1["fleet_shard_ownership"] = {
            "schema_version": 1,
            "shard_id": first.shard_id,
            "tenant_id": "tenant-a",
            "pool_id": first.pool_id,
            "owner_id": first.owner_id,
            "fencing_epoch": first.fencing_epoch,
            "policy_digest": first.policy_digest,
        }
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "ownership is required or stale",
        ):
            legacy_claim(legacy_v1)

        second = self.store.transfer_fleet_shard(
            first,
            new_owner_id="control-a",
            new_policy_digest="c" * 64,
            now=9,
        )
        stale = dict(missing)
        stale["fleet_shard_ownership"] = first.to_metadata()
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "ownership is required or stale",
        ):
            legacy_claim(stale)
        self.assertEqual(second.fencing_epoch, 2)
        self.assertEqual(
            self.store.get_attempt(attempt.attempt_id).status,
            AttemptStatus.SCHEDULED,
        )

    def test_fleet_shard_faults_roll_back_and_capacity_never_evicts(
        self,
    ) -> None:
        def fail(stage: str) -> None:
            if stage == "fleet_owner.after_insert":
                raise RuntimeError("injected Fleet owner failure")

        self.store._fault = fail
        with self.assertRaisesRegex(RuntimeError, "injected"):
            self.store.claim_fleet_shard(
                "shard-fault",
                "control-a",
                "d" * 64,
                pool_id="pool-a",
                now=10,
            )
        self.store._fault = lambda _stage: None
        self.assertIsNone(
            self.store.get_fleet_shard_ownership("shard-fault")
        )
        transferable = self.store.claim_fleet_shard(
            "shard-transfer-fault",
            "control-a",
            "d" * 64,
            pool_id="pool-transfer",
            now=10,
        )

        def fail_transfer(stage: str) -> None:
            if stage == "fleet_owner.after_transfer":
                raise RuntimeError("injected Fleet transfer failure")

        self.store._fault = fail_transfer
        with self.assertRaisesRegex(RuntimeError, "transfer failure"):
            self.store.transfer_fleet_shard(
                transferable,
                new_owner_id="control-b",
                new_policy_digest="d" * 64,
                now=11,
            )
        self.store._fault = lambda _stage: None
        self.assertEqual(
            self.store.get_fleet_shard_ownership(
                "shard-transfer-fault"
            ),
            transferable,
        )

        with mock.patch(
            "src.orchestration.store.MAX_FLEET_SHARD_OWNERS",
            2,
        ):
            retained = self.store.claim_fleet_shard(
                "shard-retained",
                "control-a",
                "d" * 64,
                pool_id="pool-a",
                now=11,
            )
            with self.assertRaises(FleetShardOwnershipCapacityError):
                self.store.claim_fleet_shard(
                    "shard-overflow",
                    "control-b",
                    "d" * 64,
                    pool_id="pool-b",
                    now=12,
                )
        self.assertEqual(
            self.store.get_fleet_shard_ownership("shard-retained"),
            retained,
        )

    def test_newer_schema_is_rejected(self) -> None:
        other = Path(self.temp_dir.name) / "newer.sqlite3"
        with sqlite3.connect(other) as conn:
            conn.execute(
                "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at REAL)"
            )
            conn.execute("INSERT INTO schema_migrations VALUES (999, 0)")
        with self.assertRaises(StoreSchemaError):
            DurableRunStore(other)

    def test_version_six_migration_installs_fleet_run_route_fencing(
        self,
    ) -> None:
        self._create_running_run("route-v6-migration")
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                DROP TRIGGER attempts_fleet_route_insert;
                DROP TRIGGER attempts_fleet_route_update;
                DROP TABLE fleet_run_routes;
                DELETE FROM schema_migrations WHERE version >= 7;
                PRAGMA user_version = 6;
                """
            )

        migrated = DurableRunStore(self.db_path)
        route = migrated.register_fleet_run_route(
            "route-v6-migration",
            "tenant-a",
            "pool-a",
            now=1,
        )

        self.assertTrue(route.enabled)
        with sqlite3.connect(self.db_path) as conn:
            version = conn.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0]
            triggers = {
                row[0]
                for row in conn.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'trigger'
                      AND name LIKE 'attempts_fleet_route_%'
                    """
                )
            }
        self.assertEqual(version, 7)
        self.assertEqual(
            triggers,
            {
                "attempts_fleet_route_insert",
                "attempts_fleet_route_update",
            },
        )

    def test_version_two_migration_backfills_artifact_references(self) -> None:
        artifacts = LocalArtifactStore(
            Path(self.temp_dir.name) / "migration-artifacts"
        )
        ref = artifacts.put_bytes(b"migration-reference")
        self.store.create_run(
            RunRecord(
                "migration-ref-run",
                "workflow",
                definition_digest=TEST_DEFINITION_DIGEST,
                input={
                    "kind": "artifact_input",
                    "artifact_refs": [ref.to_dict()],
                },
            )
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                DROP TABLE workflow_bindings;
                DROP TRIGGER attempts_fleet_ownership_insert;
                DROP TRIGGER attempts_fleet_ownership_update;
                DROP TABLE fleet_shard_owners;
                DROP TABLE artifact_references;
                DROP TABLE artifact_gc_claims;
                DROP TRIGGER attempts_fleet_route_insert;
                DROP TRIGGER attempts_fleet_route_update;
                DROP TABLE fleet_run_routes;
                DELETE FROM schema_migrations WHERE version >= 3;
                PRAGMA user_version = 2;
                """
            )

        migrated = DurableRunStore(self.db_path)

        self.assertFalse(
            migrated.claim_artifact_gc_candidate(
                ref.sha256,
                quarantine_id="q00000000001000000000_0123456789abcdef0123456789abcdef",
                size=ref.size,
                claimed_at=1,
            )
        )
        with sqlite3.connect(self.db_path) as conn:
            version = conn.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0]
            indexed = conn.execute(
                "SELECT first_run_id FROM artifact_references WHERE sha256 = ?",
                (ref.sha256,),
            ).fetchone()
        self.assertEqual(version, 7)
        self.assertEqual(indexed[0], "migration-ref-run")

    def test_version_three_migration_rejects_legacy_raw_run_input(self) -> None:
        old = Path(self.temp_dir.name) / "invalid-input-v3.sqlite3"
        old_store = DurableRunStore(old)
        old_store.create_run(
            RunRecord(
                "legacy-raw-input",
                "workflow",
                definition_digest=TEST_DEFINITION_DIGEST,
            )
        )
        with sqlite3.connect(old) as conn:
            conn.executescript(
                """
                DROP TABLE workflow_bindings;
                DELETE FROM schema_migrations WHERE version >= 4;
                PRAGMA user_version = 3;
                """
            )
            conn.execute(
                "UPDATE runs SET input_json=? WHERE run_id=?",
                (
                    json.dumps({"credential": "legacy-secret"}),
                    "legacy-raw-input",
                ),
            )

        with self.assertRaisesRegex(
            StoreSchemaError,
            "Run input violates the Artifact input boundary",
        ):
            DurableRunStore(old)

    def test_version_four_database_adds_fleet_shard_ownership(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                DROP TRIGGER attempts_fleet_ownership_insert;
                DROP TRIGGER attempts_fleet_ownership_update;
                DROP TABLE fleet_shard_owners;
                DROP TRIGGER attempts_fleet_route_insert;
                DROP TRIGGER attempts_fleet_route_update;
                DROP TABLE fleet_run_routes;
                DELETE FROM schema_migrations WHERE version >= 5;
                PRAGMA user_version = 4;
                """
            )

        migrated = DurableRunStore(self.db_path)
        ownership = migrated.claim_fleet_shard(
            "shard-migrated",
            "control-a",
            "e" * 64,
            pool_id="pool-a",
            now=10,
        )

        self.assertEqual(ownership.fencing_epoch, 1)
        with sqlite3.connect(self.db_path) as conn:
            version = conn.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0]
            triggers = {
                row[0]
                for row in conn.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'trigger'
                      AND name LIKE 'attempts_fleet_ownership_%'
                    """
                )
            }
        self.assertEqual(version, 7)
        self.assertEqual(
            triggers,
            {
                "attempts_fleet_ownership_insert",
                "attempts_fleet_ownership_update",
            },
        )

    def test_version_five_pool_owners_consolidate_and_fence_legacy(
        self,
    ) -> None:
        self._install_v5_fleet_owners(
            (
                (
                    "shard-b",
                    "tenant-b",
                    "pool-a",
                    "control-a",
                    3,
                    "a" * 64,
                    12,
                ),
                (
                    "shard-a",
                    "tenant-a",
                    "pool-a",
                    "control-a",
                    1,
                    "a" * 64,
                    10,
                ),
            )
        )

        migrated = DurableRunStore(self.db_path)
        ownership = migrated.get_fleet_pool_ownership("pool-a")
        cursor = migrated.get_fleet_fairness_cursor("pool-a")

        assert ownership is not None and cursor is not None
        self.assertEqual(ownership.shard_id, "shard-a")
        self.assertEqual(ownership.fencing_epoch, 4)
        self.assertEqual(ownership.schema_version, 2)
        self.assertIsNone(cursor.last_served_tenant)
        self.assertEqual(cursor.selection_sequence, 0)
        self.assertEqual(
            migrated.get_fleet_shard_ownership("shard-b"),
            None,
        )

    def test_version_five_split_pool_ownership_fails_migration(
        self,
    ) -> None:
        self._install_v5_fleet_owners(
            (
                (
                    "shard-a",
                    "tenant-a",
                    "pool-a",
                    "control-a",
                    1,
                    "a" * 64,
                    10,
                ),
                (
                    "shard-b",
                    "tenant-b",
                    "pool-a",
                    "control-b",
                    1,
                    "a" * 64,
                    10,
                ),
            )
        )

        with self.assertRaisesRegex(
            StoreSchemaError,
            "split ownership",
        ):
            DurableRunStore(self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            columns = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(fleet_shard_owners)"
                )
            }
            owners = conn.execute(
                "SELECT COUNT(*) FROM fleet_shard_owners"
            ).fetchone()[0]
        self.assertEqual(version, 5)
        self.assertIn("tenant_id", columns)
        self.assertEqual(owners, 2)

    def test_version_five_exhausted_epoch_fails_migration_atomically(
        self,
    ) -> None:
        self._install_v5_fleet_owners(
            (
                (
                    "shard-a",
                    "tenant-a",
                    "pool-a",
                    "control-a",
                    (1 << 63) - 1,
                    "a" * 64,
                    10,
                ),
            )
        )

        with self.assertRaisesRegex(
            StoreSchemaError,
            "epoch is exhausted",
        ):
            DurableRunStore(self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            epoch = conn.execute(
                """
                SELECT fencing_epoch
                FROM fleet_shard_owners
                WHERE shard_id = 'shard-a'
                """
            ).fetchone()[0]
        self.assertEqual(version, 5)
        self.assertEqual(epoch, (1 << 63) - 1)

    def test_version_five_malformed_owner_fails_migration_atomically(
        self,
    ) -> None:
        self._install_v5_fleet_owners(
            (
                (
                    "shard-a",
                    "tenant-a",
                    "pool-a",
                    "control-a",
                    1,
                    "not-a-sha256-digest",
                    10,
                ),
            )
        )

        with self.assertRaisesRegex(
            StoreSchemaError,
            "ownership is malformed",
        ):
            DurableRunStore(self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            policy = conn.execute(
                """
                SELECT policy_digest
                FROM fleet_shard_owners
                WHERE shard_id = 'shard-a'
                """
            ).fetchone()[0]
        self.assertEqual(version, 5)
        self.assertEqual(policy, "not-a-sha256-digest")

    def test_workflow_binding_is_idempotent_and_conflicts_fail_closed(self) -> None:
        artifacts = LocalArtifactStore(
            Path(self.temp_dir.name) / "workflow-binding-artifacts"
        )
        first_ref = artifacts.put_json({"workflow": "first"})
        second_ref = artifacts.put_json({"workflow": "second"})
        first = self.store.bind_workflow(
            "workflow",
            1,
            "1" * 64,
            first_ref,
            now=10,
        )
        repeated = self.store.bind_workflow(
            "workflow",
            1,
            "1" * 64,
            first_ref,
            now=20,
        )
        self.assertEqual(repeated, first)
        self.assertEqual(
            self.store.get_workflow_binding("workflow", 1),
            first,
        )
        for digest, ref in (
            ("2" * 64, first_ref),
            ("1" * 64, second_ref),
        ):
            with self.subTest(digest=digest, ref=ref.sha256):
                with self.assertRaises(WorkflowBindingConflictError):
                    self.store.bind_workflow(
                        "workflow",
                        1,
                        digest,
                        ref,
                    )

    def test_concurrent_workflow_binding_has_one_immutable_winner(self) -> None:
        database = Path(self.temp_dir.name) / "binding-race.sqlite3"
        stores = (DurableRunStore(database), DurableRunStore(database))
        artifacts = LocalArtifactStore(
            Path(self.temp_dir.name) / "binding-race-artifacts"
        )
        refs = (
            artifacts.put_json({"workflow": "left"}),
            artifacts.put_json({"workflow": "right"}),
        )

        def bind(index: int) -> object:
            try:
                return stores[index].bind_workflow(
                    "workflow-race",
                    7,
                    str(index + 1) * 64,
                    refs[index],
                    now=100 + index,
                )
            except WorkflowBindingConflictError as exc:
                return exc

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(bind, (0, 1)))

        winners = [
            result
            for result in results
            if not isinstance(result, WorkflowBindingConflictError)
        ]
        conflicts = [
            result
            for result in results
            if isinstance(result, WorkflowBindingConflictError)
        ]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(conflicts), 1)
        persisted = stores[0].get_workflow_binding("workflow-race", 7)
        self.assertEqual(persisted, winners[0])

    def test_create_run_atomically_locks_workflow_version_and_later_fills_ref(
        self,
    ) -> None:
        digest = "3" * 64
        self.store.create_run(
            RunRecord(
                "low-level-binding",
                "recoverable-workflow",
                workflow_version=4,
                definition_digest=digest,
            )
        )
        locked = self.store.get_workflow_binding("recoverable-workflow", 4)
        assert locked is not None
        self.assertEqual(locked.definition_digest, digest)
        self.assertIsNone(locked.workflow_ref)

        artifacts = LocalArtifactStore(
            Path(self.temp_dir.name) / "recoverable-workflow-artifacts"
        )
        workflow_ref = artifacts.put_json({"name": "recoverable-workflow"})
        filled = self.store.bind_workflow(
            "recoverable-workflow",
            4,
            digest,
            workflow_ref,
        )
        self.assertEqual(filled.workflow_ref, workflow_ref)
        self.assertEqual(filled.created_at, locked.created_at)

        created = self.store.create_run(
            RunRecord(
                "runtime-binding",
                "recoverable-workflow",
                workflow_version=4,
                definition_digest=digest,
                metadata={"runtime_workflow_ref": workflow_ref.to_dict()},
            )
        )
        self.assertEqual(created.definition_digest, digest)

    def test_concurrent_direct_run_creation_has_one_workflow_digest_winner(
        self,
    ) -> None:
        database = Path(self.temp_dir.name) / "run-binding-race.sqlite3"
        stores = (DurableRunStore(database), DurableRunStore(database))
        barrier = threading.Barrier(2)

        def create(index: int) -> object:
            barrier.wait()
            try:
                return stores[index].create_run(
                    RunRecord(
                        f"binding-run-{index}",
                        "shared-workflow",
                        workflow_version=9,
                        definition_digest=str(index + 4) * 64,
                    )
                )
            except WorkflowBindingConflictError as exc:
                return exc

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(create, (0, 1)))

        self.assertEqual(
            sum(isinstance(result, RunRecord) for result in results),
            1,
        )
        self.assertEqual(
            sum(
                isinstance(result, WorkflowBindingConflictError)
                for result in results
            ),
            1,
        )
        self.assertEqual(len(stores[0].list_runs()), 1)

    def test_child_creation_cannot_bypass_global_workflow_binding(self) -> None:
        self._create_running_controls("binding-root", "control")
        first = self._child_run("binding-child-one", "binding-root", "control")
        self.store.create_child_run(
            first,
            max_total_descendants=10,
            max_children_per_control=10,
        )
        conflicting = replace(
            self._child_run(
                "binding-child-two",
                "binding-root",
                "control",
            ),
            definition_digest="c" * 64,
        )
        with self.assertRaises(WorkflowBindingConflictError):
            self.store.create_child_run(
                conflicting,
                max_total_descendants=10,
                max_children_per_control=10,
            )

    def test_append_event_rejects_reference_to_claimed_artifact(self) -> None:
        artifacts = LocalArtifactStore(
            Path(self.temp_dir.name) / "claimed-artifacts"
        )
        ref = artifacts.put_bytes(b"claimed-reference")
        run = self.store.create_run(
            RunRecord(
                "claimed-ref-run",
                "workflow",
                definition_digest=TEST_DEFINITION_DIGEST,
            )
        )
        quarantine_id = (
            "q00000000001000000000_0123456789abcdef0123456789abcdef"
        )
        self.assertTrue(
            self.store.claim_artifact_gc_candidate(
                ref.sha256,
                quarantine_id=quarantine_id,
                size=ref.size,
                claimed_at=1,
            )
        )

        with self.assertRaises(ArtifactGCReferenceConflictError):
            self.store.append_event(
                run.run_id,
                "audit.note",
                payload={"artifact": ref.to_dict()},
            )

        stored = self.store.get_run(run.run_id)
        self.assertEqual(stored.last_event_sequence, 1)
        self.assertEqual(len(self.store.list_events(run.run_id)), 1)

    def test_compound_event_rolls_back_reference_to_claimed_artifact(self) -> None:
        run, node, attempt = self._schedule_attempt("claimed-compound-ref")
        claim, _event = self.store.claim_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "request",
            "worker",
            now=10,
        )
        self.store.start_activity(
            run.run_id,
            node.node_id,
            attempt.attempt_id,
            "worker",
            claim_token=claim.record.claim_token,
            now=11,
        )
        artifacts = LocalArtifactStore(
            Path(self.temp_dir.name) / "claimed-compound-artifacts"
        )
        ref = artifacts.put_bytes(b"claimed-compound-reference")
        quarantine_id = (
            "q00000000001000000000_0123456789abcdef0123456789abcdef"
        )
        self.assertTrue(
            self.store.claim_artifact_gc_candidate(
                ref.sha256,
                quarantine_id=quarantine_id,
                size=ref.size,
                claimed_at=1,
            )
        )
        events_before = self.store.list_events(run.run_id)

        with self.assertRaises(ArtifactGCReferenceConflictError):
            self.store.complete_activity(
                run.run_id,
                node.node_id,
                attempt.attempt_id,
                "request",
                "worker",
                claim_token=claim.record.claim_token,
                result={"artifact_refs": [ref.to_dict()]},
                now=12,
            )

        stored_attempt = self.store.get_attempt(attempt.attempt_id)
        self.assertEqual(stored_attempt.status, AttemptStatus.RUNNING)
        self.assertEqual(self.store.list_events(run.run_id), events_before)

    def test_version_one_database_is_migrated_to_current_version(self) -> None:
        old = Path(self.temp_dir.name) / "version-one.sqlite3"
        with sqlite3.connect(old) as conn:
            conn.row_factory = sqlite3.Row
            conn.executescript(
                """
                CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at REAL);
                INSERT INTO schema_migrations VALUES (1, 0);
                CREATE TABLE domain_events(
                    event_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    schema_version INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    node_id TEXT,
                    attempt_id TEXT,
                    payload_json TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    occurred_at REAL NOT NULL
                );
                INSERT INTO domain_events VALUES(
                    'event-1','run-1',1,1,'run.created',NULL,NULL,'{}','old',1
                );
                CREATE TRIGGER domain_events_no_update BEFORE UPDATE ON domain_events
                BEGIN SELECT RAISE(ABORT, 'domain events are append-only'); END;
                CREATE TABLE idempotency_records(
                    run_id TEXT, key TEXT, request_hash TEXT, status TEXT,
                    owner_id TEXT, claim_token TEXT, lease_expires_at REAL,
                    result_json TEXT, claim_count INTEGER, created_at REAL,
                    updated_at REAL, completed_at REAL,
                    PRIMARY KEY(run_id,key)
                );
                CREATE TABLE attempts(
                    attempt_id TEXT PRIMARY KEY, run_id TEXT, node_id TEXT, status TEXT
                );
                """
            )
        DurableRunStore(old)
        with sqlite3.connect(old) as conn:
            version = conn.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0]
            event_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(domain_events)")
            }
            idempotency_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(idempotency_records)")
            }
            indexes = {
                row[1] for row in conn.execute("PRAGMA index_list(attempts)")
            }
        self.assertEqual(version, 7)
        self.assertIn("content_digest", event_columns)
        self.assertIn("intent_digest", event_columns)
        self.assertIn("schema_version", idempotency_columns)
        self.assertIn("one_active_attempt_per_node", indexes)

    def test_migration_fails_closed_on_duplicate_active_attempts(self) -> None:
        old = Path(self.temp_dir.name) / "duplicate-active-v1.sqlite3"
        with sqlite3.connect(old) as conn:
            conn.executescript(
                """
                CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at REAL);
                INSERT INTO schema_migrations VALUES (1, 0);
                CREATE TABLE domain_events(
                    event_id TEXT PRIMARY KEY, run_id TEXT, seq INTEGER,
                    schema_version INTEGER, event_type TEXT, node_id TEXT,
                    attempt_id TEXT, payload_json TEXT, payload_digest TEXT,
                    occurred_at REAL
                );
                CREATE TABLE idempotency_records(
                    run_id TEXT, key TEXT, request_hash TEXT, status TEXT,
                    owner_id TEXT, claim_token TEXT, lease_expires_at REAL,
                    result_json TEXT, claim_count INTEGER, created_at REAL,
                    updated_at REAL, completed_at REAL,
                    PRIMARY KEY(run_id,key)
                );
                CREATE TABLE attempts(
                    attempt_id TEXT PRIMARY KEY, run_id TEXT, node_id TEXT, status TEXT
                );
                INSERT INTO attempts VALUES('attempt-1','run-1','node-1','scheduled');
                INSERT INTO attempts VALUES('attempt-2','run-1','node-1','claimed');
                """
            )
        with self.assertRaisesRegex(
            StoreSchemaError,
            "multiple active Attempts",
        ):
            DurableRunStore(old)


if __name__ == "__main__":
    unittest.main()
