from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from src.orchestration.artifacts import ArtifactKind, ArtifactRef, LocalArtifactStore
from src.orchestration.hierarchy import (
    DurableHierarchy,
    HierarchyDefinitionError,
    WorkflowRegistry,
)
from src.orchestration.models import AttemptStatus, NodeStatus, RunStatus
from src.orchestration.scheduler import (
    ActivityReceipt,
    DurableScheduler,
    RunInputReceipt,
)
from src.orchestration.store import (
    DurableRunStore,
    ProjectionConflictError,
)
from src.orchestration.workflow import compile_workflow


class FakeClock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class IdFactory:
    def __init__(self) -> None:
        self.value = 0

    def __call__(self, prefix: str) -> str:
        self.value += 1
        return f"{prefix}-hierarchy-{self.value}"


def _agent(node_id: str, depends_on=(), **extra):
    value = {
        "id": node_id,
        "kind": "agent",
        "depends_on": list(depends_on),
        "config": {"agent": "main", "task": f"execute {node_id}"},
        "effect_class": "read_only",
    }
    value.update(extra)
    return value


def _workflow(name: str, nodes, *, version: int = 1):
    return compile_workflow(
        {
            "schema_version": 2,
            "name": name,
            "version": version,
            "nodes": nodes,
        }
    )


class DurableHierarchyTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.database = self.root / "orchestration.sqlite3"
        self.artifacts = LocalArtifactStore(self.root / "artifacts")
        self.clock = FakeClock()
        self.ids = IdFactory()

    def input_writer(self, run_id, raw_input) -> RunInputReceipt:
        ref = self.artifacts.put_json(
            raw_input,
            kind=ArtifactKind.GENERIC,
            producer_run_id=run_id,
            metadata={},
        )
        return RunInputReceipt((ref,))

    def result_writer(self, claim, raw_result) -> ActivityReceipt:
        ref = self.artifacts.put_json(
            raw_result,
            kind=ArtifactKind.TOOL_RESULT,
            producer_run_id=claim.run_id,
            producer_node_id=claim.node_id,
            producer_attempt_id=claim.attempt_id,
            metadata={},
        )
        return ActivityReceipt((ref,))

    def scheduler(
        self,
        workflow,
        registry,
        *,
        store=None,
        hierarchy=None,
        with_input_writer: bool = True,
        max_children_per_control: int = 256,
    ):
        store = store or DurableRunStore(self.database)
        hierarchy = hierarchy or DurableHierarchy(
            registry,
            self.artifacts,
            max_children_per_control=max_children_per_control,
            max_total_descendants=max(1024, max_children_per_control),
        )
        scheduler = DurableScheduler(
            store,
            workflow,
            clock=self.clock,
            id_factory=self.ids,
            result_writer=self.result_writer,
            input_writer=self.input_writer if with_input_writer else None,
            artifact_verifier=self.artifacts.verify,
            hierarchy_controller=hierarchy,
        )
        return store, hierarchy, scheduler

    @staticmethod
    def children(store, parent_run_id, parent_node_id=None):
        values = []
        for run in store.list_runs(limit=1000):
            link = run.metadata.get("hierarchy_link")
            if not isinstance(link, dict):
                continue
            if link.get("parent_run_id") != parent_run_id:
                continue
            if (
                parent_node_id is not None
                and link.get("parent_node_id") != parent_node_id
            ):
                continue
            values.append(run)
        return sorted(
            values,
            key=lambda run: (
                -1
                if run.metadata["hierarchy_link"]["child_index"] is None
                else run.metadata["hierarchy_link"]["child_index"]
            ),
        )

    def complete_next(self, scheduler, parent_run_id, worker, result):
        claim = scheduler.claim_next(parent_run_id, worker, capacity=8)
        self.assertIsNotNone(claim)
        scheduler.start_claim(claim)
        scheduler.complete_claim(claim, result)
        return claim

    def test_subworkflow_restart_and_duplicate_reconcile_are_idempotent(self) -> None:
        child = _workflow("child-fixed", [_agent("child-work")], version=7)
        parent = _workflow(
            "parent",
            [
                {
                    "id": "sub",
                    "kind": "subworkflow",
                    "config": {
                        "workflow_id": "child-fixed",
                        "workflow_version": 7,
                        "input": {"ticket": 11},
                    },
                }
            ],
        )
        registry = WorkflowRegistry([child])
        store, _hierarchy, scheduler = self.scheduler(parent, registry)
        scheduler.create_run("parent-run")
        scheduler.reconcile("parent-run")

        children = self.children(store, "parent-run", "sub")
        self.assertEqual(len(children), 1)
        child_run = children[0]
        link = child_run.metadata["hierarchy_link"]
        self.assertEqual(link["parent_run_id"], "parent-run")
        self.assertEqual(link["parent_node_id"], "sub")
        self.assertEqual(link["definition_digest"], child.definition_digest)
        first_child_id = child_run.run_id

        restarted_store = DurableRunStore(self.database)
        # A fresh process has no in-memory child definitions. Recovery must
        # resolve the immutable definition through Store + Artifact binding.
        restarted_registry = WorkflowRegistry()
        restarted_hierarchy = DurableHierarchy(restarted_registry, self.artifacts)
        _store, _hierarchy, restarted = self.scheduler(
            parent,
            restarted_registry,
            store=restarted_store,
            hierarchy=restarted_hierarchy,
        )
        restarted.reconcile("parent-run")
        restarted.reconcile("parent-run")
        self.assertEqual(
            [run.run_id for run in self.children(restarted_store, "parent-run", "sub")],
            [first_child_id],
        )

        claim = self.complete_next(
            restarted,
            "parent-run",
            "worker-child",
            {"answer": 42},
        )
        self.assertEqual(claim.run_id, first_child_id)
        restarted.reconcile("parent-run")
        self.assertEqual(
            restarted_store.get_node("parent-run", "sub").status,
            NodeStatus.SUCCEEDED,
        )
        self.assertEqual(
            restarted_store.get_run("parent-run").status,
            RunStatus.COMPLETED,
        )
        event_count = len(restarted_store.list_events("parent-run", limit=1000))
        restarted.reconcile("parent-run")
        self.assertEqual(
            len(restarted_store.list_events("parent-run", limit=1000)),
            event_count,
        )

    def test_map_enforces_concurrency_and_merges_out_of_order_by_index(self) -> None:
        secrets = [
            "secret-map-item-charlie-391",
            "secret-map-item-alpha-284",
            "secret-map-item-bravo-175",
        ]
        parent = _workflow(
            "map-parent",
            [
                {
                    "id": "fanout",
                    "kind": "map",
                    "config": {
                        "items": secrets,
                        "body": "body",
                        "item_name": "work_item",
                        "max_concurrency": 2,
                    },
                },
                _agent("body", ["fanout"]),
            ],
        )
        registry = WorkflowRegistry()
        store, _hierarchy, scheduler = self.scheduler(parent, registry)
        scheduler.create_run("map-run")
        scheduler.reconcile("map-run")
        children = self.children(store, "map-run", "fanout")
        self.assertEqual(len(children), 2)
        self.assertEqual(sum(not run.status.is_terminal for run in children), 2)

        claims = {}
        for worker in ("worker-a", "worker-b"):
            claim = scheduler.claim_next("map-run", worker, capacity=8)
            self.assertIsNotNone(claim)
            scheduler.start_claim(claim)
            index = store.get_run(claim.run_id).metadata["hierarchy_link"]["child_index"]
            claims[index] = claim
        self.assertEqual(set(claims), {0, 1})

        scheduler.complete_claim(claims[1], {"completed": 1})
        scheduler.reconcile("map-run")
        children = self.children(store, "map-run", "fanout")
        self.assertEqual(len(children), 3)
        self.assertLessEqual(sum(not run.status.is_terminal for run in children), 2)

        third = scheduler.claim_next("map-run", "worker-c", capacity=8)
        self.assertIsNotNone(third)
        scheduler.start_claim(third)
        self.assertEqual(
            store.get_run(third.run_id).metadata["hierarchy_link"]["child_index"],
            2,
        )
        scheduler.complete_claim(third, {"completed": 2})
        scheduler.complete_claim(claims[0], {"completed": 0})
        scheduler.reconcile("map-run")

        map_node = store.get_node("map-run", "fanout")
        body = store.get_node("map-run", "body")
        self.assertEqual(map_node.status, NodeStatus.SUCCEEDED)
        self.assertEqual(body.status, NodeStatus.SUCCEEDED)
        self.assertEqual(store.get_run("map-run").status, RunStatus.COMPLETED)
        self.assertEqual(store.list_attempts("map-run"), [])
        aggregate_ref = ArtifactRef.from_dict(map_node.output["artifact_refs"][0])
        aggregate = json.loads(self.artifacts.read(aggregate_ref))
        self.assertEqual(
            [child["index"] for child in aggregate["children"]],
            [0, 1, 2],
        )

        database_bytes = b"".join(
            path.read_bytes()
            for path in (
                self.database,
                Path(f"{self.database}-wal"),
                Path(f"{self.database}-shm"),
            )
            if path.exists()
        )
        for secret in secrets:
            self.assertNotIn(secret.encode(), database_bytes)
        for run in store.list_runs(limit=1000):
            self.assertNotIn("secret-map-item", json.dumps(run.metadata))
            for event in store.list_events(run.run_id, limit=1000):
                self.assertNotIn("secret-map-item", json.dumps(event.payload))

    def test_cancel_and_pause_propagate_and_parent_waits(self) -> None:
        child = _workflow("child-intent", [_agent("work")])
        parent = _workflow(
            "parent-intent",
            [
                {
                    "id": "sub",
                    "kind": "subworkflow",
                    "config": {"workflow_id": "child-intent"},
                }
            ],
        )
        registry = WorkflowRegistry([child])
        store, _hierarchy, scheduler = self.scheduler(parent, registry)

        scheduler.create_run("pause-parent")
        scheduler.reconcile("pause-parent")
        pause_claim = scheduler.claim_next("pause-parent", "pause-worker", capacity=8)
        scheduler.start_claim(pause_claim)
        paused = scheduler.request_pause("pause-parent")
        pause_child = store.get_run(pause_claim.run_id)
        self.assertEqual(paused.status, RunStatus.PAUSING)
        self.assertEqual(pause_child.status, RunStatus.PAUSING)
        scheduler.confirm_pause_claim(pause_claim)
        scheduler.reconcile("pause-parent")
        self.assertEqual(store.get_run(pause_claim.run_id).status, RunStatus.PAUSED)
        self.assertEqual(store.get_run("pause-parent").status, RunStatus.PAUSED)
        scheduler.resume("pause-parent")
        resumed_child = store.get_run(pause_claim.run_id)
        self.assertEqual(resumed_child.status, RunStatus.RUNNING)

        scheduler.create_run("cancel-parent")
        scheduler.reconcile("cancel-parent")
        cancel_claim = scheduler.claim_next("cancel-parent", "cancel-worker", capacity=8)
        scheduler.start_claim(cancel_claim)
        cancelling = scheduler.request_cancel("cancel-parent")
        self.assertEqual(cancelling.status, RunStatus.CANCELLING)
        self.assertEqual(
            store.get_run(cancel_claim.run_id).status,
            RunStatus.CANCELLING,
        )
        self.assertEqual(
            store.get_node("cancel-parent", "sub").status,
            NodeStatus.RUNNING,
        )
        scheduler.confirm_cancel_claim(cancel_claim)
        scheduler.reconcile("cancel-parent")
        self.assertEqual(
            store.get_run(cancel_claim.run_id).status,
            RunStatus.CANCELLED,
        )
        self.assertEqual(store.get_run("cancel-parent").status, RunStatus.CANCELLED)
        self.assertEqual(
            store.get_node("cancel-parent", "sub").status,
            NodeStatus.CANCELLED,
        )

    def test_child_failure_and_damaged_link_fail_closed(self) -> None:
        child = _workflow("child-failure", [_agent("work")])
        parent = _workflow(
            "parent-failure",
            [
                {
                    "id": "sub",
                    "kind": "subworkflow",
                    "config": {"workflow_id": "child-failure"},
                    "on_error": "fail_run",
                }
            ],
        )
        registry = WorkflowRegistry([child])
        store, _hierarchy, scheduler = self.scheduler(parent, registry)
        scheduler.create_run("failure-parent")
        scheduler.reconcile("failure-parent")
        claim = scheduler.claim_next("failure-parent", "worker", capacity=8)
        scheduler.start_claim(claim)
        scheduler.complete_claim(
            claim,
            {"error_code": "permanent"},
            attempt_status=AttemptStatus.FAILED,
            error_class="permanent",
        )
        scheduler.reconcile("failure-parent")
        self.assertEqual(store.get_run(claim.run_id).status, RunStatus.FAILED)
        self.assertEqual(
            store.get_node("failure-parent", "sub").status,
            NodeStatus.FAILED,
        )
        self.assertEqual(store.get_run("failure-parent").status, RunStatus.FAILED)

        scheduler.create_run("artifact-damaged-parent")
        scheduler.reconcile("artifact-damaged-parent")
        damaged_claim = self.complete_next(
            scheduler,
            "artifact-damaged-parent",
            "artifact-worker",
            {"result": "durable"},
        )
        result_node = store.get_node(damaged_claim.run_id, "work")
        result_ref = ArtifactRef.from_dict(result_node.output["artifact_refs"][0])
        (self.artifacts.root / result_ref.uri).write_bytes(b"tampered")
        scheduler.reconcile("artifact-damaged-parent")
        self.assertEqual(
            store.get_run("artifact-damaged-parent").status,
            RunStatus.WAITING_RECOVERY,
        )
        self.assertEqual(
            store.get_node("artifact-damaged-parent", "sub").status,
            NodeStatus.WAITING_RECOVERY,
        )

        scheduler.create_run("damaged-parent")
        scheduler.reconcile("damaged-parent")
        damaged = self.children(store, "damaged-parent", "sub")[0]
        metadata = dict(damaged.metadata)
        metadata["hierarchy_link"] = {
            **metadata["hierarchy_link"],
            "parent_node_id": "attacker",
        }
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "UPDATE runs SET metadata_json=? WHERE run_id=?",
                (
                    json.dumps(metadata, separators=(",", ":"), sort_keys=True),
                    damaged.run_id,
                ),
            )
        scheduler.reconcile("damaged-parent")
        self.assertEqual(
            store.get_run("damaged-parent").status,
            RunStatus.WAITING_RECOVERY,
        )
        self.assertEqual(
            store.get_node("damaged-parent", "sub").status,
            NodeStatus.WAITING_RECOVERY,
        )

    def test_map_requires_input_writer_and_rejects_ambiguous_template(self) -> None:
        valid = _workflow(
            "no-writer",
            [
                {
                    "id": "fanout",
                    "kind": "map",
                    "config": {"items": [1], "body": "body"},
                },
                _agent("body", ["fanout"]),
            ],
        )
        registry = WorkflowRegistry()
        store, _hierarchy, scheduler = self.scheduler(
            valid,
            registry,
            with_input_writer=False,
        )
        scheduler.create_run("no-writer-run")
        scheduler.reconcile("no-writer-run")
        self.assertEqual(
            store.get_run("no-writer-run").status,
            RunStatus.WAITING_RECOVERY,
        )
        self.assertEqual(self.children(store, "no-writer-run"), [])

        ambiguous = _workflow(
            "ambiguous-map",
            [
                {
                    "id": "fanout",
                    "kind": "map",
                    "config": {"items": [1], "body": "body"},
                },
                _agent("body", ["fanout"]),
                _agent("downstream", ["body"]),
            ],
        )
        hierarchy = DurableHierarchy(registry, self.artifacts)
        with self.assertRaises(HierarchyDefinitionError):
            DurableScheduler(
                DurableRunStore(self.root / "ambiguous.sqlite3"),
                ambiguous,
                result_writer=self.result_writer,
                input_writer=self.input_writer,
                artifact_verifier=self.artifacts.verify,
                hierarchy_controller=hierarchy,
            )

    def test_artifact_sourced_map_and_hard_limits_fail_closed(self) -> None:
        secret = "artifact-source-secret-item-947"
        dynamic = _workflow(
            "dynamic-map",
            [
                {
                    "id": "fanout",
                    "kind": "map",
                    "config": {
                        "items": "{{input.items}}",
                        "body": "body",
                        "max_concurrency": 1,
                    },
                },
                _agent("body", ["fanout"]),
            ],
        )
        store, _hierarchy, scheduler = self.scheduler(
            dynamic,
            WorkflowRegistry(),
        )
        scheduler.create_run(
            "dynamic-run",
            input={"items": [secret, "safe-second"]},
        )
        scheduler.reconcile("dynamic-run")
        children = self.children(store, "dynamic-run", "fanout")
        self.assertEqual(len(children), 1)
        self.assertEqual(
            store.get_node("dynamic-run", "fanout")
            .metadata["hierarchy"]["items_source_digest"]
            .split(":", 1)[0],
            "artifact_receipt",
        )
        self.assertNotIn(
            secret,
            json.dumps(store.get_node("dynamic-run", "fanout").metadata),
        )

        limited = _workflow(
            "limited-map",
            [
                {
                    "id": "fanout",
                    "kind": "map",
                    "config": {"items": [1, 2, 3], "body": "body"},
                },
                _agent("body", ["fanout"]),
            ],
        )
        limited_store = DurableRunStore(self.root / "limited.sqlite3")
        limited_hierarchy = DurableHierarchy(
            WorkflowRegistry(),
            self.artifacts,
            max_children_per_control=2,
            max_total_descendants=2,
        )
        _store, _hierarchy, limited_scheduler = self.scheduler(
            limited,
            WorkflowRegistry(),
            store=limited_store,
            hierarchy=limited_hierarchy,
        )
        limited_scheduler.create_run("limited-run")
        limited_scheduler.reconcile("limited-run")
        self.assertEqual(
            limited_store.get_run("limited-run").status,
            RunStatus.WAITING_RECOVERY,
        )
        self.assertEqual(self.children(limited_store, "limited-run"), [])

        recursive = _workflow(
            "recursive",
            [
                {
                    "id": "again",
                    "kind": "subworkflow",
                    "config": {"workflow_id": "recursive"},
                }
            ],
        )
        recursive_registry = WorkflowRegistry([recursive])
        recursive_store = DurableRunStore(self.root / "recursive.sqlite3")
        _store, _hierarchy, recursive_scheduler = self.scheduler(
            recursive,
            recursive_registry,
            store=recursive_store,
        )
        recursive_scheduler.create_run("recursive-run")
        recursive_scheduler.reconcile("recursive-run")
        self.assertEqual(
            recursive_store.get_run("recursive-run").status,
            RunStatus.WAITING_RECOVERY,
        )
        self.assertEqual(self.children(recursive_store, "recursive-run"), [])

    def test_deadline_propagates_and_cancels_child_before_parent(self) -> None:
        child = _workflow("deadline-child", [_agent("work")])
        parent = _workflow(
            "deadline-parent",
            [
                {
                    "id": "sub",
                    "kind": "subworkflow",
                    "config": {"workflow_id": "deadline-child"},
                }
            ],
        )
        registry = WorkflowRegistry([child])
        store, _hierarchy, scheduler = self.scheduler(parent, registry)
        scheduler.create_run("deadline-run", metadata={"deadline_at": 120.0})
        scheduler.reconcile("deadline-run")
        child_run = self.children(store, "deadline-run", "sub")[0]
        self.assertEqual(child_run.metadata["deadline_at"], 120.0)

        self.clock.now = 121.0
        scheduler.reconcile("deadline-run")

        self.assertEqual(
            store.get_run(child_run.run_id).status,
            RunStatus.CANCELLED,
        )
        self.assertEqual(store.get_run("deadline-run").status, RunStatus.CANCELLED)

    def test_resource_lock_is_global_across_map_child_runs(self) -> None:
        parent = _workflow(
            "resource-map",
            [
                {
                    "id": "fanout",
                    "kind": "map",
                    "config": {
                        "items": [1, 2],
                        "body": "body",
                        "max_concurrency": 2,
                    },
                },
                _agent(
                    "body",
                    ["fanout"],
                    resource_keys=["workspace:shared"],
                ),
            ],
        )
        store, _hierarchy, scheduler = self.scheduler(
            parent,
            WorkflowRegistry(),
        )
        scheduler.create_run("resource-run")
        scheduler.reconcile("resource-run")
        self.assertEqual(len(self.children(store, "resource-run", "fanout")), 2)

        first = scheduler.claim_next("resource-run", "worker-one", capacity=8)
        self.assertIsNotNone(first)
        scheduler.start_claim(first)
        self.assertIsNone(
            scheduler.claim_next("resource-run", "worker-two", capacity=8)
        )
        scheduler.complete_claim(first, {"done": first.run_id})
        scheduler.reconcile("resource-run")
        second = scheduler.claim_next("resource-run", "worker-two", capacity=8)
        self.assertIsNotNone(second)
        self.assertNotEqual(first.run_id, second.run_id)

    def test_map_compound_resolution_recovers_before_template_can_be_claimed(self) -> None:
        parent = _workflow(
            "map-crash",
            [
                {
                    "id": "fanout",
                    "kind": "map",
                    "config": {"items": [1], "body": "body"},
                },
                _agent("body", ["fanout"]),
            ],
        )
        registry = WorkflowRegistry()
        store, hierarchy, scheduler = self.scheduler(parent, registry)
        scheduler.create_run("crash-run")
        scheduler.reconcile("crash-run")
        claim = self.complete_next(
            scheduler,
            "crash-run",
            "worker",
            {"done": True},
        )
        child = store.get_run(claim.run_id)
        derived = hierarchy.scheduler_for_run(scheduler, child.run_id).workflow
        receipt = hierarchy._aggregate_receipt(
            scheduler,
            store.get_run("crash-run"),
            "fanout",
            [(0, child, derived)],
            relation="map",
        )

        original_transition = scheduler._transition_node

        def crash_before_body(run_id, node_id, *args, **kwargs):
            if node_id == "body":
                raise RuntimeError("simulated kill between compound steps")
            return original_transition(run_id, node_id, *args, **kwargs)

        with patch.object(
            scheduler,
            "_transition_node",
            side_effect=crash_before_body,
        ):
            with self.assertRaisesRegex(RuntimeError, "simulated kill"):
                scheduler.resolve_hierarchy_control(
                    "crash-run",
                    "fanout",
                    outcome="succeeded",
                    receipt=receipt,
                    map_body_id="body",
                )

        self.assertEqual(
            store.get_node("crash-run", "fanout").status,
            NodeStatus.SUCCEEDED,
        )
        self.assertEqual(
            store.get_node("crash-run", "body").status,
            NodeStatus.PENDING,
        )
        restarted_store = DurableRunStore(self.database)
        restarted_registry = WorkflowRegistry()
        restarted_hierarchy = DurableHierarchy(
            restarted_registry,
            self.artifacts,
        )
        _store, _hierarchy, restarted = self.scheduler(
            parent,
            restarted_registry,
            store=restarted_store,
            hierarchy=restarted_hierarchy,
        )
        self.assertIsNone(
            restarted.claim_next("crash-run", "attacker-worker", capacity=8)
        )
        self.assertEqual(
            restarted_store.get_node("crash-run", "body").status,
            NodeStatus.SUCCEEDED,
        )
        self.assertEqual(
            restarted_store.get_run("crash-run").status,
            RunStatus.COMPLETED,
        )
        self.assertEqual(restarted_store.list_attempts("crash-run"), [])

    def test_map_recovers_child_created_ahead_of_monotonic_progress(self) -> None:
        parent = _workflow(
            "map-progress-race",
            [
                {
                    "id": "fanout",
                    "kind": "map",
                    "config": {
                        "items": [{"work": 1}],
                        "body": "body",
                        "item_name": "work_item",
                        "max_concurrency": 1,
                    },
                },
                _agent("body", ["fanout"]),
            ],
        )
        registry = WorkflowRegistry()
        store_a, _hierarchy_a, scheduler_a = self.scheduler(parent, registry)
        store_b, _hierarchy_b, scheduler_b = self.scheduler(
            parent,
            registry,
            store=DurableRunStore(self.database),
            hierarchy=DurableHierarchy(registry, self.artifacts),
        )
        scheduler_a.create_run("map-progress-race-run")
        child_committed = threading.Barrier(2)
        recovery_committed = threading.Barrier(2)
        original_record = scheduler_b.record_hierarchy_state

        def pause_before_progress(run_id, node_id, state, *, payload):
            if (
                payload.get("control_plane") == "map_child_created"
                and state.get("created_count") == 1
            ):
                child_committed.wait(timeout=5)
                recovery_committed.wait(timeout=5)
            return original_record(
                run_id,
                node_id,
                state,
                payload=payload,
            )

        with patch.object(
            scheduler_b,
            "record_hierarchy_state",
            side_effect=pause_before_progress,
        ):
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    scheduler_b.reconcile,
                    "map-progress-race-run",
                )
                child_committed.wait(timeout=5)
                scheduler_a.reconcile("map-progress-race-run")
                recovered = store_a.get_node(
                    "map-progress-race-run",
                    "fanout",
                )
                self.assertEqual(
                    recovered.metadata["hierarchy"]["created_count"],
                    1,
                )
                self.assertNotEqual(
                    store_a.get_run("map-progress-race-run").status,
                    RunStatus.WAITING_RECOVERY,
                )
                recovery_committed.wait(timeout=5)
                future.result(timeout=5)

        children = self.children(
            store_a,
            "map-progress-race-run",
            "fanout",
        )
        self.assertEqual(len(children), 1)
        node = store_a.get_node("map-progress-race-run", "fanout")
        before_version = node.projection_version
        stale_state = dict(node.metadata["hierarchy"])
        stale_state["created_count"] = 0
        scheduler_a.record_hierarchy_state(
            "map-progress-race-run",
            "fanout",
            stale_state,
            payload={"control_plane": "stale_progress"},
        )
        node = store_a.get_node("map-progress-race-run", "fanout")
        self.assertEqual(node.projection_version, before_version)
        self.assertEqual(node.metadata["hierarchy"]["created_count"], 1)
        self.assertTrue(store_a.verify_projections("map-progress-race-run"))
        self.assertTrue(store_b.verify_projections("map-progress-race-run"))

    def test_hierarchy_reconcile_does_not_swallow_real_projection_conflict(
        self,
    ) -> None:
        parent = _workflow(
            "map-real-conflict",
            [
                {
                    "id": "fanout",
                    "kind": "map",
                    "config": {
                        "items": [1],
                        "body": "body",
                        "item_name": "item",
                        "max_concurrency": 1,
                    },
                },
                _agent("body", ["fanout"]),
            ],
        )
        registry = WorkflowRegistry()
        store, _hierarchy, scheduler = self.scheduler(parent, registry)
        scheduler.create_run("map-real-conflict-run")

        with patch.object(
            scheduler,
            "record_hierarchy_state",
            side_effect=ProjectionConflictError("injected real conflict"),
        ):
            with self.assertRaisesRegex(
                ProjectionConflictError,
                "injected real conflict",
            ):
                scheduler.reconcile("map-real-conflict-run")

        self.assertEqual(
            self.children(store, "map-real-conflict-run", "fanout"),
            [],
        )
        self.assertNotEqual(
            store.get_run("map-real-conflict-run").status,
            RunStatus.WAITING_RECOVERY,
        )
        scheduler.reconcile("map-real-conflict-run")
        self.assertEqual(
            len(self.children(store, "map-real-conflict-run", "fanout")),
            1,
        )
        self.assertTrue(store.verify_projections("map-real-conflict-run"))

    def test_child_create_losing_to_parent_cancel_is_a_converged_cas_race(
        self,
    ) -> None:
        parent = _workflow(
            "map-child-cancel-race",
            [
                {
                    "id": "fanout",
                    "kind": "map",
                    "config": {
                        "items": [1],
                        "body": "body",
                        "item_name": "item",
                        "max_concurrency": 1,
                    },
                },
                _agent("body", ["fanout"]),
            ],
        )
        registry = WorkflowRegistry()
        store_a, _hierarchy_a, scheduler_a = self.scheduler(parent, registry)
        store_b, _hierarchy_b, scheduler_b = self.scheduler(
            parent,
            registry,
            store=DurableRunStore(self.database),
            hierarchy=DurableHierarchy(registry, self.artifacts),
        )
        scheduler_a.create_run("map-child-cancel-race-run")
        child_create_entered = threading.Event()
        release_child_create = threading.Event()
        original_create_child = store_a.create_child_run

        def pause_before_child_create(*args, **kwargs):
            child_create_entered.set()
            if not release_child_create.wait(timeout=5):
                raise AssertionError("parent cancellation did not finish")
            return original_create_child(*args, **kwargs)

        with patch.object(
            store_a,
            "create_child_run",
            side_effect=pause_before_child_create,
        ):
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    scheduler_a.reconcile,
                    "map-child-cancel-race-run",
                )
                self.assertTrue(child_create_entered.wait(timeout=5))
                cancelled = scheduler_b.request_cancel(
                    "map-child-cancel-race-run"
                )
                release_child_create.set()
                future.result(timeout=5)

        self.assertEqual(cancelled.status, RunStatus.CANCELLED)
        self.assertEqual(
            store_a.get_run("map-child-cancel-race-run").status,
            RunStatus.CANCELLED,
        )
        self.assertEqual(
            store_a.get_node(
                "map-child-cancel-race-run",
                "fanout",
            ).status,
            NodeStatus.CANCELLED,
        )
        self.assertEqual(
            self.children(
                store_a,
                "map-child-cancel-race-run",
                "fanout",
            ),
            [],
        )
        self.assertTrue(
            store_a.verify_projections("map-child-cancel-race-run")
        )
        self.assertTrue(
            store_b.verify_projections("map-child-cancel-race-run")
        )

    def test_hierarchy_intent_does_not_swallow_real_projection_conflict(
        self,
    ) -> None:
        child = _workflow("intent-child-conflict", [_agent("work")])
        parent = _workflow(
            "intent-parent-conflict",
            [
                {
                    "id": "sub",
                    "kind": "subworkflow",
                    "config": {
                        "workflow_id": "intent-child-conflict",
                        "input": {},
                    },
                }
            ],
        )
        registry = WorkflowRegistry([child])
        store, hierarchy, scheduler = self.scheduler(parent, registry)
        scheduler.create_run("intent-real-conflict-run")
        scheduler.reconcile("intent-real-conflict-run")
        child_run = self.children(
            store,
            "intent-real-conflict-run",
            "sub",
        )[0]
        child_scheduler = hierarchy.scheduler_for_run(
            scheduler,
            child_run.run_id,
        )

        with patch.object(
            hierarchy,
            "scheduler_for_run",
            return_value=child_scheduler,
        ), patch.object(
            child_scheduler,
            "request_cancel",
            side_effect=ProjectionConflictError("injected intent conflict"),
        ):
            with self.assertRaisesRegex(
                ProjectionConflictError,
                "injected intent conflict",
            ):
                scheduler.request_cancel("intent-real-conflict-run")

        self.assertEqual(
            store.get_run("intent-real-conflict-run").status,
            RunStatus.CANCELLING,
        )
        self.assertFalse(store.get_run(child_run.run_id).status.is_terminal)
        scheduler.reconcile("intent-real-conflict-run")
        self.assertEqual(
            store.get_run("intent-real-conflict-run").status,
            RunStatus.CANCELLED,
        )
        self.assertEqual(
            store.get_run(child_run.run_id).status,
            RunStatus.CANCELLED,
        )
        self.assertTrue(store.verify_projections("intent-real-conflict-run"))


if __name__ == "__main__":
    unittest.main()
