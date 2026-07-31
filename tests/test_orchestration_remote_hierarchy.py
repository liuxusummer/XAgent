from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.orchestration.artifacts import ArtifactKind, LocalArtifactStore
from src.orchestration.hierarchy import DurableHierarchy, WorkflowRegistry
from src.orchestration.models import AttemptStatus, RunStatus
from src.orchestration.remote_control import RemoteControlPlane
from src.orchestration.remote_journal import RemoteControlJournal
from src.orchestration.remote_protocol import AuthenticatedWorker
from src.orchestration.remote_worker import (
    RemoteExecutionOutcome,
    RemoteWorkerClient,
    RemoteWorkerError,
)
from src.orchestration.scheduler import DurableScheduler, RunInputReceipt
from src.orchestration.store import (
    DurableRunStore,
    ProjectionConflictError,
    StoreSchemaError,
)
from src.orchestration.workflow import compile_workflow

from tests.test_orchestration_remote_protocol import (
    _Clock,
    _TestAdmitter,
    _runtime_proof,
)


class _AdmissionRaceAdmitter(_TestAdmitter):
    def __init__(self, artifacts, cancel_parent) -> None:
        super().__init__(artifacts)
        self._cancel_parent = cancel_parent

    def begin_admission(
        self,
        identity,
        registration,
        scheduler,
        candidate,
        admission,
    ):
        expires_at = super().begin_admission(
            identity,
            registration,
            scheduler,
            candidate,
            admission,
        )
        self._cancel_parent()
        return expires_at


class RemoteHierarchyAdmissionTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.artifacts = LocalArtifactStore(self.root / "artifacts")
        self.store = DurableRunStore(self.root / "runs.sqlite3")
        self.clock = _Clock()
        self.identity = AuthenticatedWorker(
            worker_id="hierarchy-worker",
            tenant_id="tenant-a",
            identity_digest=hashlib.sha256(
                b"hierarchy-worker"
            ).hexdigest(),
        )

    @staticmethod
    def _tool_workflow(name: str):
        return compile_workflow(
            {
                "schema_version": 2,
                "name": name,
                "version": 1,
                "nodes": [
                    {
                        "id": "tool",
                        "kind": "tool",
                        "config": {
                            "tool": "inspect",
                            "arguments": {"mode": "summary"},
                        },
                        "effect_class": "read_only",
                        "resource_keys": ["workspace:project"],
                    }
                ],
            }
        )

    @staticmethod
    def _parent_workflow(
        name: str,
        child_name: str,
    ):
        return compile_workflow(
            {
                "schema_version": 2,
                "name": name,
                "version": 1,
                "nodes": [
                    {
                        "id": "sub",
                        "kind": "subworkflow",
                        "config": {
                            "workflow_id": child_name,
                            "workflow_version": 1,
                            "input": {"ticket": 7},
                        },
                    }
                ],
            }
        )

    def _input_writer(self, run_id, raw_input):
        reference = self.artifacts.put_json(
            raw_input,
            kind=ArtifactKind.GENERIC,
            producer_run_id=run_id,
            metadata={},
        )
        return RunInputReceipt((reference,))

    def _runtime(self, *, nested: bool = False):
        leaf = self._tool_workflow("hierarchy-leaf")
        if nested:
            middle = self._parent_workflow(
                "hierarchy-middle",
                leaf.name,
            )
            parent = self._parent_workflow(
                "hierarchy-root",
                middle.name,
            )
            workflows = (leaf, middle)
        else:
            parent = self._parent_workflow(
                "hierarchy-root",
                leaf.name,
            )
            workflows = (leaf,)
        registry = WorkflowRegistry(workflows)
        hierarchy = DurableHierarchy(registry, self.artifacts)
        scheduler = DurableScheduler(
            self.store,
            parent,
            clock=self.clock,
            input_writer=self._input_writer,
            artifact_verifier=self.artifacts.verify,
            hierarchy_controller=hierarchy,
        )
        scheduler.create_run("root-run")
        for _ in range(12):
            scheduler.reconcile("root-run")
        return hierarchy, scheduler

    def _map_runtime(self):
        parent = compile_workflow(
            {
                "schema_version": 2,
                "name": "hierarchy-map-root",
                "version": 1,
                "nodes": [
                    {
                        "id": "fanout",
                        "kind": "map",
                        "config": {
                            "items": [1, 2],
                            "body": "body",
                            "item_name": "item",
                            "max_concurrency": 2,
                        },
                    },
                    {
                        "id": "body",
                        "kind": "tool",
                        "depends_on": ["fanout"],
                        "config": {
                            "tool": "inspect",
                            "arguments": {"mode": "summary"},
                        },
                        "effect_class": "read_only",
                        "resource_keys": ["workspace:project"],
                    },
                ],
            }
        )
        hierarchy = DurableHierarchy(
            WorkflowRegistry(),
            self.artifacts,
        )
        scheduler = DurableScheduler(
            self.store,
            parent,
            clock=self.clock,
            input_writer=self._input_writer,
            artifact_verifier=self.artifacts.verify,
            hierarchy_controller=hierarchy,
        )
        scheduler.create_run("root-run")
        for _ in range(8):
            scheduler.reconcile("root-run")
        return hierarchy, scheduler

    def _root_activity_runtime(self):
        workflow = self._tool_workflow("hierarchy-root-activity")
        hierarchy = DurableHierarchy(
            WorkflowRegistry(),
            self.artifacts,
        )
        scheduler = DurableScheduler(
            self.store,
            workflow,
            clock=self.clock,
            artifact_verifier=self.artifacts.verify,
            hierarchy_controller=hierarchy,
        )
        scheduler.create_run("root-run")
        scheduler.reconcile("root-run")
        return hierarchy, scheduler

    def _client(
        self,
        hierarchy,
        scheduler,
        *,
        authorize_run=None,
        admitter=None,
        journal_name: str = "remote.sqlite3",
    ):
        authorizer = authorize_run or (
            lambda identity, run_id: (
                identity == self.identity
                and self.store.get_run(run_id) is not None
            )
        )
        control = RemoteControlPlane(
            lambda run_id: hierarchy.scheduler_for_run(
                scheduler,
                run_id,
            ),
            authorize_run=authorizer,
            assignment_admitter=admitter
            or _TestAdmitter(self.artifacts),
            journal=RemoteControlJournal(
                self.root / journal_name
            ),
        )
        client = RemoteWorkerClient(
            lambda request: control.handle(self.identity, request),
            worker_id=self.identity.worker_id,
            instance_id=f"instance-{journal_name}",
        )
        client.register(
            runtime_version="worker-runtime/1.0",
            capabilities=("activity.tool", "artifact.refs"),
            resource_keys=("workspace:project",),
            activity_kinds=("tool",),
            max_concurrency=2,
        )
        return control, client

    def test_root_poll_claims_child_with_durable_chain_and_completes(self):
        hierarchy, scheduler = self._runtime()
        _control, client = self._client(hierarchy, scheduler)

        assignment = client.poll("root-run")

        self.assertIsNotNone(assignment)
        child_run_id = assignment.claim.run_id
        self.assertNotEqual(child_run_id, "root-run")
        self.assertEqual(self.store.list_attempts("root-run"), [])
        attempt = self.store.get_attempt(
            assignment.claim.attempt_id
        )
        self.assertEqual(attempt.status, AttemptStatus.CLAIMED)
        scope = attempt.metadata["hierarchy_admission"]
        self.assertEqual(scope["root_run_id"], "root-run")
        self.assertEqual(
            scope["hops"][-1]["child_run_id"],
            child_run_id,
        )

        client.start(assignment.claim)
        handles = tuple(
            grant.output_handle
            for grant in assignment.output_grants
        )
        client.complete(
            assignment.claim,
            RemoteExecutionOutcome(
                "succeeded",
                handles,
                _runtime_proof(
                    assignment,
                    outcome="succeeded",
                    output_handles=handles,
                ),
            ),
        )
        for _ in range(8):
            scheduler.reconcile("root-run")
        self.assertEqual(
            self.store.get_run("root-run").status,
            RunStatus.COMPLETED,
        )

    def test_nested_poll_carries_every_ancestor_hop(self):
        hierarchy, scheduler = self._runtime(nested=True)
        _control, client = self._client(hierarchy, scheduler)

        assignment = client.poll("root-run")

        self.assertIsNotNone(assignment)
        attempt = self.store.get_attempt(
            assignment.claim.attempt_id
        )
        scope = attempt.metadata["hierarchy_admission"]
        self.assertEqual(len(scope["hops"]), 2)
        self.assertEqual(scope["hops"][0]["parent_run_id"], "root-run")
        self.assertEqual(
            scope["hops"][0]["child_run_id"],
            scope["hops"][1]["parent_run_id"],
        )
        self.assertEqual(
            scope["hops"][1]["child_run_id"],
            assignment.claim.run_id,
        )

    def test_map_poll_uses_deterministic_child_and_exact_scope(self):
        hierarchy, scheduler = self._map_runtime()
        _control, client = self._client(hierarchy, scheduler)

        assignment = client.poll("root-run")

        self.assertIsNotNone(assignment)
        child = self.store.get_run(assignment.claim.run_id)
        self.assertEqual(
            child.metadata["hierarchy_link"]["relation"],
            "map_item",
        )
        self.assertEqual(
            child.metadata["hierarchy_link"]["child_index"],
            0,
        )
        scope = self.store.get_attempt(
            assignment.claim.attempt_id
        ).metadata["hierarchy_admission"]
        self.assertEqual(
            scope["hops"][0]["parent_node_id"],
            "fanout",
        )

    def test_every_ancestor_and_target_require_authorization(self):
        hierarchy, scheduler = self._runtime()
        _control, client = self._client(
            hierarchy,
            scheduler,
            authorize_run=lambda identity, run_id: (
                identity == self.identity and run_id == "root-run"
            ),
        )

        with self.assertRaisesRegex(RemoteWorkerError, "forbidden"):
            client.poll("root-run")
        child = self.store.list_child_runs("root-run")[0]
        self.assertEqual(self.store.list_attempts(child.run_id), [])

    def test_direct_child_poll_cannot_bypass_root_authorization(self):
        hierarchy, scheduler = self._runtime()
        child = self.store.list_child_runs("root-run")[0]
        _control, client = self._client(
            hierarchy,
            scheduler,
            authorize_run=lambda identity, run_id: (
                identity == self.identity
                and run_id == child.run_id
            ),
        )

        with self.assertRaisesRegex(RemoteWorkerError, "forbidden"):
            client.poll(child.run_id)
        self.assertEqual(self.store.list_attempts(child.run_id), [])

    def test_parent_cancellation_between_prepare_and_claim_rolls_back_child(self):
        hierarchy, scheduler = self._runtime()
        admitter = _AdmissionRaceAdmitter(
            self.artifacts,
            lambda: scheduler.request_cancel(
                "root-run",
                reconcile=False,
            ),
        )
        _control, client = self._client(
            hierarchy,
            scheduler,
            admitter=admitter,
        )

        self.assertIsNone(client.poll("root-run"))
        child = self.store.list_child_runs("root-run")[0]
        self.assertEqual(self.store.list_attempts(child.run_id), [])
        self.assertEqual(
            self.store.get_run("root-run").status,
            RunStatus.CANCELLING,
        )

    def test_parent_projection_change_between_prepare_and_claim_is_stale(self):
        hierarchy, scheduler = self._runtime()
        admitter = _AdmissionRaceAdmitter(
            self.artifacts,
            lambda: self.store.append_event(
                "root-run",
                "audit.note",
                event_id="evt-parent-admission-race",
                payload={"kind": "admission_race"},
                occurred_at=self.clock(),
            ),
        )
        _control, client = self._client(
            hierarchy,
            scheduler,
            admitter=admitter,
        )

        self.assertIsNone(client.poll("root-run"))
        child = self.store.list_child_runs("root-run")[0]
        self.assertEqual(self.store.list_attempts(child.run_id), [])
        self.assertEqual(
            self.store.get_run("root-run").status,
            RunStatus.RUNNING,
        )

    def test_child_link_damage_during_admission_is_rechecked_in_store(self):
        hierarchy, scheduler = self._runtime()
        child = self.store.list_child_runs("root-run")[0]

        def damage_link():
            current = self.store.get_run(child.run_id)
            metadata = current.to_dict()["metadata"]
            metadata["hierarchy_link"]["ancestry_digests"][0] = (
                hashlib.sha256(b"forged-ancestor").hexdigest()
            )
            with sqlite3.connect(self.store.path) as conn:
                conn.execute(
                    "UPDATE runs SET metadata_json = ? WHERE run_id = ?",
                    (
                        json.dumps(
                            metadata,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        child.run_id,
                    ),
                )

        admitter = _AdmissionRaceAdmitter(
            self.artifacts,
            damage_link,
        )
        _control, client = self._client(
            hierarchy,
            scheduler,
            admitter=admitter,
        )

        self.assertIsNone(client.poll("root-run"))
        self.assertEqual(self.store.list_attempts(child.run_id), [])

    def test_parent_cancellation_after_claim_blocks_start_gate(self):
        hierarchy, scheduler = self._runtime()
        _control, client = self._client(hierarchy, scheduler)
        assignment = client.poll("root-run")
        self.assertIsNotNone(assignment)
        scheduler.request_cancel("root-run", reconcile=False)

        self.assertTrue(
            client.cancellation_requested(assignment.claim)
        )
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "claim_conflict",
        ):
            client.heartbeat(assignment.claim)
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "claim_conflict",
        ):
            client.start(assignment.claim)
        self.assertEqual(
            self.store.get_attempt(
                assignment.claim.attempt_id
            ).status,
            AttemptStatus.CLAIMED,
        )

    def test_heartbeat_rechecks_parent_after_read_probe_race(self):
        hierarchy, scheduler = self._runtime()
        _control, client = self._client(hierarchy, scheduler)
        assignment = client.poll("root-run")
        self.assertIsNotNone(assignment)

        def cancel_after_probe(_attempt_id):
            scheduler.request_cancel(
                "root-run",
                reconcile=False,
            )
            return True

        with patch.object(
            self.store,
            "hierarchy_authority_is_active",
            side_effect=cancel_after_probe,
        ):
            with self.assertRaisesRegex(
                RemoteWorkerError,
                "claim_conflict",
            ):
                client.heartbeat(assignment.claim)
        attempt = self.store.get_attempt(
            assignment.claim.attempt_id
        )
        lease = self.store.get_idempotency(
            attempt.run_id,
            attempt.idempotency_key,
        )
        self.assertEqual(
            lease.lease_expires_at,
            assignment.lease_expires_at,
        )

    def test_parent_cancel_allows_exact_child_cancel_acknowledgement(self):
        hierarchy, scheduler = self._runtime()
        _control, client = self._client(hierarchy, scheduler)
        assignment = client.poll("root-run")
        self.assertIsNotNone(assignment)
        client.start(assignment.claim)

        scheduler.request_cancel("root-run")
        child = self.store.get_run(assignment.claim.run_id)
        self.assertEqual(child.status, RunStatus.CANCELLING)
        client.acknowledge_cancel(
            assignment.claim,
            _runtime_proof(
                assignment,
                outcome="cancelled",
                output_handles=(),
            ),
        )
        for _ in range(8):
            scheduler.reconcile("root-run")
        self.assertEqual(
            self.store.get_attempt(
                assignment.claim.attempt_id
            ).status,
            AttemptStatus.CANCELLED,
        )
        self.assertEqual(
            self.store.get_run("root-run").status,
            RunStatus.CANCELLED,
        )

    def test_root_remote_cancellation_also_fences_lease_renewal(self):
        hierarchy, scheduler = self._root_activity_runtime()
        _control, client = self._client(hierarchy, scheduler)
        assignment = client.poll("root-run")
        self.assertIsNotNone(assignment)
        scheduler.request_cancel("root-run", reconcile=False)

        self.assertTrue(
            client.cancellation_requested(assignment.claim)
        )
        with self.assertRaisesRegex(
            RemoteWorkerError,
            "claim_conflict",
        ):
            client.heartbeat(assignment.claim)

    def test_restart_recovers_child_scheduler_and_claim_authority(self):
        hierarchy, scheduler = self._runtime()
        _control, client = self._client(hierarchy, scheduler)
        assignment = client.poll("root-run")
        self.assertIsNotNone(assignment)
        attempt = self.store.get_attempt(
            assignment.claim.attempt_id
        )

        restarted_store = DurableRunStore(self.store.path)
        restarted_registry = WorkflowRegistry()
        restarted_hierarchy = DurableHierarchy(
            restarted_registry,
            self.artifacts,
        )
        restarted_root = DurableScheduler(
            restarted_store,
            scheduler.workflow,
            clock=self.clock,
            input_writer=self._input_writer,
            artifact_verifier=self.artifacts.verify,
            hierarchy_controller=restarted_hierarchy,
        )
        child_scheduler = restarted_hierarchy.scheduler_for_run(
            restarted_root,
            assignment.claim.run_id,
        )
        restored = child_scheduler.restore_claim(
            assignment.claim.run_id,
            assignment.claim.node_id,
            assignment.claim.attempt_id,
            attempt.worker_id,
            request_hash=assignment.claim.activity_request_digest,
            claim_token=assignment.claim.claim_token,
            fencing_token=assignment.claim.fencing_token,
        )

        child_scheduler.start_claim(restored)
        self.assertEqual(
            restarted_store.get_attempt(restored.attempt_id).status,
            AttemptStatus.RUNNING,
        )

    def test_corrupt_child_link_fails_closed_without_attempt(self):
        hierarchy, scheduler = self._runtime()
        child = self.store.list_child_runs("root-run")[0]
        damaged = child.to_dict()["metadata"]
        damaged["hierarchy_link"]["root_run_id"] = "forged-root"
        with sqlite3.connect(self.store.path) as conn:
            conn.execute(
                "UPDATE runs SET metadata_json = ? WHERE run_id = ?",
                (
                    json.dumps(
                        damaged,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    child.run_id,
                ),
            )
        _control, client = self._client(hierarchy, scheduler)

        with self.assertRaisesRegex(
            RemoteWorkerError,
            "internal_error",
        ):
            client.poll("root-run")
        self.assertEqual(self.store.list_attempts(child.run_id), [])

    def test_schema_eight_fences_legacy_unscoped_remote_child_claim(self):
        hierarchy, scheduler = self._runtime()
        child = self.store.list_child_runs("root-run")[0]
        child_scheduler = hierarchy.scheduler_for_run(
            scheduler,
            child.run_id,
        )

        with self.assertRaisesRegex(
            ProjectionConflictError,
            "Attempt conflicts",
        ):
            child_scheduler.claim_next(
                child.run_id,
                "remote-session:legacy:session",
                capacity=1,
                resource_keys=("workspace:project",),
            )
        attempt = self.store.list_attempts(child.run_id)[0]
        self.assertEqual(attempt.status, AttemptStatus.SCHEDULED)

        local_claim = child_scheduler.claim_next(
            child.run_id,
            "local-worker",
            capacity=1,
            resource_keys=("workspace:project",),
        )
        self.assertIsNotNone(local_claim)
        self.assertEqual(
            self.store.get_attempt(local_claim.attempt_id).status,
            AttemptStatus.CLAIMED,
        )

    def test_schema_eight_migration_requires_legacy_child_drain(self):
        hierarchy, scheduler = self._runtime()
        child = self.store.list_child_runs("root-run")[0]
        child_scheduler = hierarchy.scheduler_for_run(
            scheduler,
            child.run_id,
        )
        with sqlite3.connect(self.store.path) as conn:
            conn.executescript(
                """
                DROP TRIGGER attempts_hierarchy_admission_insert;
                DROP TRIGGER attempts_hierarchy_admission_update;
                DELETE FROM schema_migrations WHERE version = 8;
                PRAGMA user_version = 7;
                """
            )
        claim = child_scheduler.claim_next(
            child.run_id,
            "remote-session:legacy:session",
            capacity=1,
            resource_keys=("workspace:project",),
        )
        self.assertIsNotNone(claim)

        with self.assertRaisesRegex(
            StoreSchemaError,
            "requires draining unsafe",
        ):
            DurableRunStore(self.store.path)
        with sqlite3.connect(self.store.path) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT MAX(version) FROM schema_migrations"
                ).fetchone()[0],
                7,
            )
            self.assertEqual(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM sqlite_master
                    WHERE type = 'trigger'
                      AND name LIKE 'attempts_hierarchy_admission_%'
                    """
                ).fetchone()[0],
                0,
            )


if __name__ == "__main__":
    unittest.main()
