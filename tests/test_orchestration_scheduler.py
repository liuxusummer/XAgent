from __future__ import annotations

import hashlib
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from src.orchestration.artifacts import ArtifactKind, LocalArtifactStore
from src.orchestration.models import (
    AttemptRecord,
    AttemptStatus,
    IdempotencyStatus,
    NodeStatus,
    RunStatus,
)
from src.orchestration.scheduler import (
    ActivityReceipt,
    ApprovalResolution,
    DurableScheduler,
    InputPersistenceError,
    ResultPersistenceError,
    RunInputReceipt,
    SchedulerStateError,
)
from src.orchestration.store import (
    ConcurrentProjectionUpdate,
    DurableRunStore,
    ProjectionConflictError,
    RunAlreadyExistsError,
)
from src.orchestration.workflow import compile_workflow


class FakeClock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class IdFactory:
    def __init__(self, namespace: str = "test") -> None:
        self.value = 0
        self.namespace = namespace

    def __call__(self, prefix: str) -> str:
        self.value += 1
        return f"{prefix}-{self.namespace}-{self.value}"


def _agent(
    node_id: str,
    depends_on=None,
    *,
    effect_class: str = "read_only",
    **extra,
):
    node = {
        "id": node_id,
        "kind": "agent",
        "depends_on": list(depends_on or []),
        "config": {"agent": "main", "task": f"run {node_id}"},
        "effect_class": effect_class,
    }
    node.update(extra)
    return node


def _workflow(nodes):
    return compile_workflow(
        {
            "schema_version": 2,
            "name": "scheduler-test",
            "version": 3,
            "nodes": nodes,
        }
    )


class DurableSchedulerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.root = Path(self._temporary.name)
        self.clock = FakeClock()
        self.ids = IdFactory()
        self.artifacts = LocalArtifactStore(self.root / "artifacts")

    def result_writer(self, claim, raw_result) -> ActivityReceipt:
        ref = self.artifacts.put_json(
            raw_result,
            kind=ArtifactKind.TOOL_RESULT,
            producer_run_id=claim.run_id,
            producer_node_id=claim.node_id,
            producer_attempt_id=claim.attempt_id,
        )
        return ActivityReceipt((ref,))

    def input_writer(self, run_id, raw_input) -> RunInputReceipt:
        ref = self.artifacts.put_json(
            raw_input,
            kind=ArtifactKind.GENERIC,
            producer_run_id=run_id,
        )
        return RunInputReceipt((ref,))

    @staticmethod
    def verify_approval(resolution: ApprovalResolution) -> bool:
        expected = hashlib.sha256(
            (
                f"{resolution.approval_id}\0{resolution.run_id}\0"
                f"{resolution.node_id}\0{resolution.definition_digest}\0"
                f"{resolution.approved}"
            ).encode("utf-8")
        ).hexdigest()
        return resolution.decision_digest == expected

    @staticmethod
    def approval_resolution(
        scheduler: DurableScheduler,
        run_id: str,
        node_id: str,
        *,
        approved: bool,
    ) -> ApprovalResolution:
        approval_id = f"ledger-{run_id}-{node_id}"
        digest = hashlib.sha256(
            (
                f"{approval_id}\0{run_id}\0{node_id}\0"
                f"{scheduler.workflow.definition_digest}\0{approved}"
            ).encode("utf-8")
        ).hexdigest()
        return ApprovalResolution(
            approval_id=approval_id,
            run_id=run_id,
            node_id=node_id,
            definition_digest=scheduler.workflow.definition_digest,
            approved=approved,
            decision_digest=digest,
        )

    def scheduler(self, nodes, *, max_active_attempts: int = 8):
        store = DurableRunStore(self.root / "orchestration.sqlite3")
        scheduler = DurableScheduler(
            store,
            _workflow(nodes),
            clock=self.clock,
            id_factory=self.ids,
            max_active_attempts=max_active_attempts,
            result_writer=self.result_writer,
            input_writer=self.input_writer,
            artifact_verifier=self.artifacts.verify,
            approval_verifier=self.verify_approval,
        )
        return store, scheduler

    def test_create_run_persists_definition_and_pending_nodes_fail_closed(self) -> None:
        store, scheduler = self.scheduler(
            [_agent("finish", ["start"]), _agent("start")]
        )

        token = "run-input-secret-token-7f31"
        run = scheduler.create_run(
            "run-create",
            input={"ticket": 7, "token": token},
        )

        self.assertEqual(run.definition_digest, scheduler.workflow.definition_digest)
        self.assertEqual(run.input["kind"], "artifact_input")
        sqlite_bytes = b"".join(
            path.read_bytes()
            for path in (
                store.path,
                Path(f"{store.path}-wal"),
                Path(f"{store.path}-shm"),
            )
            if path.exists()
        )
        self.assertNotIn(token.encode("utf-8"), sqlite_bytes)
        self.assertEqual(
            [node.status for node in store.list_nodes(run.run_id)],
            [NodeStatus.PENDING, NodeStatus.PENDING],
        )
        with self.assertRaises(RunAlreadyExistsError):
            scheduler.create_run("run-create")

        snapshot = scheduler.reconcile(run.run_id)
        self.assertEqual(snapshot.run.status, RunStatus.RUNNING)
        self.assertEqual(snapshot.ready, ("start",))

    def test_diamond_dag_parallel_ready_and_join_completion(self) -> None:
        store, scheduler = self.scheduler(
            [
                _agent("join-left", ["root"]),
                {
                    "id": "join",
                    "kind": "join",
                    "depends_on": ["join-left", "join-right"],
                    "config": {"mode": "all"},
                },
                _agent("root"),
                _agent("join-right", ["root"]),
            ]
        )
        scheduler.create_run("run-diamond")
        scheduler.reconcile("run-diamond")

        root = scheduler.claim_next("run-diamond", "worker-root")
        self.assertIsNotNone(root)
        scheduler.start_claim(root)
        scheduler.complete_claim(root, {"root": "done"})

        self.assertEqual(
            scheduler.reconcile("run-diamond").ready,
            ("join-left", "join-right"),
        )
        left = scheduler.claim_next("run-diamond", "worker-left")
        right = scheduler.claim_next("run-diamond", "worker-right")
        self.assertIsNotNone(left)
        self.assertIsNotNone(right)
        scheduler.start_claim(left)
        scheduler.start_claim(right)
        scheduler.complete_claim(left, {"left": "done"})
        scheduler.complete_claim(right, {"right": "done"})

        self.assertEqual(store.get_node("run-diamond", "join").status, NodeStatus.SUCCEEDED)
        self.assertEqual(store.get_run("run-diamond").status, RunStatus.COMPLETED)

    def test_parallel_control_node_releases_all_branches(self) -> None:
        store, scheduler = self.scheduler(
            [
                {
                    "id": "fork",
                    "kind": "parallel",
                    "depends_on": [],
                    "config": {"branches": ["b", "a"]},
                },
                _agent("a", ["fork"]),
                _agent("b", ["fork"]),
            ]
        )
        scheduler.create_run("run-parallel")

        snapshot = scheduler.reconcile("run-parallel")

        self.assertEqual(store.get_node("run-parallel", "fork").status, NodeStatus.SUCCEEDED)
        self.assertEqual(snapshot.ready, ("a", "b"))
        self.assertEqual(store.list_attempts("run-parallel"), [])

    def test_resource_and_concurrency_locks_are_derived_from_active_attempts(self) -> None:
        store, scheduler = self.scheduler(
            [
                _agent(
                    "a",
                    resource_keys=["workspace:one"],
                    concurrency_key="browser:one",
                ),
                _agent(
                    "b",
                    resource_keys=["workspace:one"],
                    concurrency_key="browser:two",
                ),
                _agent("c", concurrency_key="browser:one"),
            ]
        )
        scheduler.create_run("run-locks")
        scheduler.reconcile("run-locks")

        first = scheduler.claim_next(
            "run-locks",
            "worker-a",
            resource_keys=["workspace:one"],
        )
        self.assertEqual(first.node_id, "a")
        self.assertIsNone(
            scheduler.claim_next(
                "run-locks",
                "worker-b",
                resource_keys=["workspace:one"],
            )
        )

        restarted = DurableScheduler(
            store,
            scheduler.workflow,
            clock=self.clock,
            id_factory=self.ids,
            result_writer=self.result_writer,
            input_writer=self.input_writer,
            artifact_verifier=self.artifacts.verify,
            approval_verifier=self.verify_approval,
        )
        self.assertIsNone(
            restarted.claim_next(
                "run-locks",
                "worker-b",
                resource_keys=["workspace:one"],
            )
        )
        scheduler.start_claim(first)
        scheduler.complete_claim(first, {"done": True})

        second = restarted.claim_next(
            "run-locks",
            "worker-b",
            resource_keys=["workspace:one"],
        )
        self.assertEqual(second.node_id, "b")

    def test_global_and_worker_capacity_bound_admission(self) -> None:
        _store, scheduler = self.scheduler(
            [_agent("a"), _agent("b")],
            max_active_attempts=1,
        )
        scheduler.create_run("run-capacity")
        scheduler.reconcile("run-capacity")

        claim = scheduler.claim_next("run-capacity", "worker", capacity=1)

        self.assertIsNotNone(claim)
        self.assertIsNone(
            scheduler.claim_next("run-capacity", "other-worker", capacity=2)
        )

    def test_concurrent_schedulers_atomically_enforce_global_capacity(self) -> None:
        workflow = _workflow([_agent("activity")])
        schedulers = tuple(
            DurableScheduler(
                DurableRunStore(self.root / "global-capacity.sqlite3"),
                workflow,
                clock=self.clock,
                id_factory=IdFactory(f"global-{index}"),
                max_active_attempts=1,
                result_writer=self.result_writer,
                input_writer=self.input_writer,
                artifact_verifier=self.artifacts.verify,
                approval_verifier=self.verify_approval,
            )
            for index in range(2)
        )
        run_ids = ("run-global-a", "run-global-b")
        for scheduler, run_id in zip(schedulers, run_ids, strict=True):
            scheduler.create_run(run_id)
            scheduler.reconcile(run_id)
        barrier = threading.Barrier(2)

        def compete(index: int):
            barrier.wait()
            return schedulers[index].claim_next(
                run_ids[index],
                f"worker-{index}",
                capacity=2,
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            claims = list(pool.map(compete, range(2)))
        self.assertEqual(sum(claim is not None for claim in claims), 1)

        winner = next(index for index, claim in enumerate(claims) if claim is not None)
        loser = 1 - winner
        restarted = DurableScheduler(
            DurableRunStore(self.root / "global-capacity.sqlite3"),
            workflow,
            clock=self.clock,
            id_factory=IdFactory("global-restart"),
            max_active_attempts=1,
            result_writer=self.result_writer,
            input_writer=self.input_writer,
            artifact_verifier=self.artifacts.verify,
            approval_verifier=self.verify_approval,
        )
        self.assertIsNone(
            restarted.claim_next(run_ids[loser], "restart-worker", capacity=2)
        )
        schedulers[winner].start_claim(claims[winner])
        schedulers[winner].complete_claim(claims[winner], {"done": True})
        self.assertIsNotNone(
            restarted.claim_next(run_ids[loser], "restart-worker", capacity=2)
        )

    def test_concurrent_schedulers_atomically_enforce_worker_capacity(self) -> None:
        workflow = _workflow([_agent("activity")])
        schedulers = tuple(
            DurableScheduler(
                DurableRunStore(self.root / "worker-capacity.sqlite3"),
                workflow,
                clock=self.clock,
                id_factory=IdFactory(f"worker-{index}"),
                max_active_attempts=8,
                result_writer=self.result_writer,
                input_writer=self.input_writer,
                artifact_verifier=self.artifacts.verify,
                approval_verifier=self.verify_approval,
            )
            for index in range(2)
        )
        run_ids = ("run-worker-a", "run-worker-b")
        for scheduler, run_id in zip(schedulers, run_ids, strict=True):
            scheduler.create_run(run_id)
            scheduler.reconcile(run_id)
        barrier = threading.Barrier(2)

        def compete(index: int):
            barrier.wait()
            return schedulers[index].claim_next(
                run_ids[index],
                "shared-worker",
                capacity=1,
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            claims = list(pool.map(compete, range(2)))
        self.assertEqual(sum(claim is not None for claim in claims), 1)

    def test_concurrent_schedulers_atomically_enforce_resource_locks(self) -> None:
        cases = (
            (
                "workspace",
                {"resource_keys": ["workspace:shared"]},
                ["workspace:shared"],
            ),
            (
                "browser",
                {"concurrency_key": "browser:shared"},
                None,
            ),
        )
        for label, node_options, worker_resources in cases:
            with self.subTest(lock=label):
                database = self.root / f"{label}-lock.sqlite3"
                workflow = _workflow([_agent("activity", **node_options)])
                schedulers = tuple(
                    DurableScheduler(
                        DurableRunStore(database),
                        workflow,
                        clock=self.clock,
                        id_factory=IdFactory(f"{label}-{index}"),
                        max_active_attempts=8,
                        result_writer=self.result_writer,
                        input_writer=self.input_writer,
                        artifact_verifier=self.artifacts.verify,
                        approval_verifier=self.verify_approval,
                    )
                    for index in range(2)
                )
                run_ids = (f"run-{label}-a", f"run-{label}-b")
                for scheduler, run_id in zip(schedulers, run_ids, strict=True):
                    scheduler.create_run(run_id)
                    scheduler.reconcile(run_id)
                barrier = threading.Barrier(2)

                def compete(index: int):
                    barrier.wait()
                    return schedulers[index].claim_next(
                        run_ids[index],
                        f"{label}-worker-{index}",
                        capacity=2,
                        resource_keys=worker_resources,
                    )

                with ThreadPoolExecutor(max_workers=2) as pool:
                    claims = list(pool.map(compete, range(2)))
                self.assertEqual(sum(claim is not None for claim in claims), 1)

                winner = next(
                    index for index, claim in enumerate(claims) if claim is not None
                )
                loser = 1 - winner
                schedulers[winner].start_claim(claims[winner])
                schedulers[winner].complete_claim(claims[winner], {"done": True})
                restarted = DurableScheduler(
                    DurableRunStore(database),
                    workflow,
                    clock=self.clock,
                    id_factory=IdFactory(f"{label}-restart"),
                    max_active_attempts=8,
                    result_writer=self.result_writer,
                    input_writer=self.input_writer,
                    artifact_verifier=self.artifacts.verify,
                    approval_verifier=self.verify_approval,
                )
                self.assertIsNotNone(
                    restarted.claim_next(
                        run_ids[loser],
                        f"{label}-restart-worker",
                        capacity=2,
                        resource_keys=worker_resources,
                    )
                )

    def test_restart_does_not_duplicate_claimed_attempt(self) -> None:
        store, scheduler = self.scheduler([_agent("only")])
        scheduler.create_run("run-restart")
        claim = scheduler.claim_next("run-restart", "owner")
        self.assertIsNotNone(claim)

        restarted = DurableScheduler(
            store,
            scheduler.workflow,
            clock=self.clock,
            id_factory=self.ids,
            result_writer=self.result_writer,
            input_writer=self.input_writer,
            artifact_verifier=self.artifacts.verify,
            approval_verifier=self.verify_approval,
        )
        restarted.reconcile("run-restart")

        self.assertIsNone(restarted.claim_next("run-restart", "other"))
        self.assertEqual(len(store.list_attempts("run-restart")), 1)
        self.assertEqual(
            store.list_attempts("run-restart")[0].status,
            AttemptStatus.CLAIMED,
        )

    def test_expired_claim_is_not_reclaimed_or_started_without_reaper(self) -> None:
        store, scheduler = self.scheduler([_agent("only")])
        scheduler.create_run("run-expired")
        claim = scheduler.claim_next(
            "run-expired",
            "owner",
            lease_seconds=1.0,
        )
        self.clock.advance(2.0)

        self.assertIsNone(scheduler.claim_next("run-expired", "other"))
        with self.assertRaisesRegex(Exception, "lease expired"):
            scheduler.start_claim(claim)
        attempts = store.list_attempts("run-expired")
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0].status, AttemptStatus.CLAIMED)

    def test_claim_start_complete_delegates_compound_lifecycle(self) -> None:
        store, scheduler = self.scheduler([_agent("work")])
        scheduler.create_run("run-lifecycle")

        claim = scheduler.claim_next("run-lifecycle", "worker")
        attempt = store.get_attempt(claim.attempt_id)
        self.assertEqual(attempt.status, AttemptStatus.CLAIMED)

        scheduler.start_claim(claim)
        self.assertEqual(
            store.get_attempt(claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )

        scheduler.complete_claim(claim, {"value": 42})
        self.assertEqual(
            store.get_attempt(claim.attempt_id).status,
            AttemptStatus.SUCCEEDED,
        )
        self.assertEqual(
            store.get_node("run-lifecycle", "work").status,
            NodeStatus.SUCCEEDED,
        )
        self.assertEqual(store.get_run("run-lifecycle").status, RunStatus.COMPLETED)

    def test_retry_waits_for_deterministic_due_time_and_keeps_operation_key(self) -> None:
        store, scheduler = self.scheduler(
            [
                _agent(
                    "retry",
                    retry={
                        "max_attempts": 2,
                        "retry_on": ["transient"],
                        "initial_delay_ms": 1000,
                        "max_delay_ms": 1000,
                    },
                )
            ]
        )
        scheduler.create_run("run-retry")
        first = scheduler.claim_next("run-retry", "worker-one")
        scheduler.start_claim(first)

        scheduler.complete_claim(
            first,
            {
                "error_code": "temporary_failure",
                "raw_error": "retry-secret-must-not-persist",
            },
            attempt_status=AttemptStatus.FAILED,
            error_class="transient",
        )

        idempotency = store.get_idempotency("run-retry", first.claim_key)
        self.assertEqual(idempotency.status, IdempotencyStatus.COMPLETED)
        self.assertNotIn(
            "retry-secret-must-not-persist",
            str(store.list_events("run-retry")),
        )
        self.assertEqual(
            store.get_node("run-retry", "retry").status,
            NodeStatus.WAITING_RETRY,
        )
        self.clock.advance(0.999)
        scheduler.reconcile("run-retry")
        self.assertEqual(
            store.get_node("run-retry", "retry").status,
            NodeStatus.WAITING_RETRY,
        )
        self.clock.advance(0.001)
        scheduler.reconcile("run-retry")
        second = scheduler.claim_next("run-retry", "worker-two")

        self.assertEqual(second.attempt_number, 2)
        self.assertEqual(second.operation_key, first.operation_key)
        self.assertEqual(second.idempotency_key, first.idempotency_key)
        self.assertNotEqual(second.claim_key, first.claim_key)
        self.assertEqual(len(store.list_attempts("run-retry")), 2)

    def test_outcome_unknown_never_retries(self) -> None:
        store, scheduler = self.scheduler(
            [
                _agent(
                    "unsafe",
                    effect_class="non_idempotent_write",
                    retry={
                        "max_attempts": 3,
                        "retry_on": ["transient"],
                    },
                )
            ]
        )
        scheduler.create_run("run-unknown")
        claim = scheduler.claim_next("run-unknown", "worker")
        scheduler.start_claim(claim)

        scheduler.complete_claim(
            claim,
            {"code": "unknown"},
            attempt_status=AttemptStatus.OUTCOME_UNKNOWN,
            error_class="transient",
        )

        self.assertEqual(store.get_run("run-unknown").status, RunStatus.WAITING_RECOVERY)
        self.assertEqual(
            store.get_node("run-unknown", "unsafe").status,
            NodeStatus.WAITING_RECOVERY,
        )
        self.assertIsNone(scheduler.claim_next("run-unknown", "other"))
        self.assertEqual(len(store.list_attempts("run-unknown")), 1)

    def test_failed_dependency_obeys_continue_skip_and_fail_run(self) -> None:
        cases = [
            ("continue", NodeStatus.READY, RunStatus.RUNNING),
            ("skip_dependents", NodeStatus.SKIPPED, RunStatus.FAILED),
            ("fail_run", NodeStatus.SKIPPED, RunStatus.FAILED),
        ]
        for index, (on_error, child_status, run_status) in enumerate(cases):
            with self.subTest(on_error=on_error):
                root = self.root / f"case-{index}"
                store = DurableRunStore(root / "orchestration.sqlite3")
                scheduler = DurableScheduler(
                    store,
                    _workflow(
                        [
                            _agent("upstream", on_error=on_error),
                            _agent("downstream", ["upstream"]),
                        ]
                    ),
                    clock=self.clock,
                    id_factory=self.ids,
                    result_writer=self.result_writer,
                    input_writer=self.input_writer,
                    artifact_verifier=self.artifacts.verify,
                    approval_verifier=self.verify_approval,
                )
                run_id = f"run-error-{index}"
                scheduler.create_run(run_id)
                claim = scheduler.claim_next(run_id, "worker")
                scheduler.start_claim(claim)
                scheduler.complete_claim(
                    claim,
                    {"code": "permanent"},
                    attempt_status=AttemptStatus.FAILED,
                    error_class="permanent",
                )

                self.assertEqual(
                    store.get_node(run_id, "downstream").status,
                    child_status,
                )
                self.assertEqual(store.get_run(run_id).status, run_status)

    def test_approval_waits_without_activity_then_resumes_explicitly(self) -> None:
        store, scheduler = self.scheduler(
            [
                {
                    "id": "approve",
                    "kind": "approval",
                    "depends_on": [],
                    "config": {"prompt": "continue?"},
                }
            ]
        )
        scheduler.create_run("run-approval")

        scheduler.reconcile("run-approval")

        self.assertEqual(
            store.get_node("run-approval", "approve").status,
            NodeStatus.WAITING_APPROVAL,
        )
        self.assertEqual(store.list_attempts("run-approval"), [])
        resolution = self.approval_resolution(
            scheduler,
            "run-approval",
            "approve",
            approved=True,
        )
        untrusted_scheduler = DurableScheduler(
            store,
            scheduler.workflow,
            clock=self.clock,
            id_factory=self.ids,
            result_writer=self.result_writer,
            input_writer=self.input_writer,
            artifact_verifier=self.artifacts.verify,
        )
        with self.assertRaisesRegex(
            Exception,
            "trusted ApprovalLedger verifier",
        ):
            untrusted_scheduler.resolve_approval(
                "run-approval",
                "approve",
                resolution,
            )
        scheduler.resolve_approval("run-approval", "approve", resolution)
        self.assertEqual(
            store.get_node("run-approval", "approve").status,
            NodeStatus.SUCCEEDED,
        )
        self.assertEqual(store.get_run("run-approval").status, RunStatus.COMPLETED)

    def test_concurrent_identical_approval_resolution_commits_once(self) -> None:
        store, scheduler = self.scheduler(
            [
                {
                    "id": "approve",
                    "kind": "approval",
                    "depends_on": [],
                    "config": {"prompt": "continue?"},
                }
            ]
        )
        scheduler.create_run("run-approval-race")
        scheduler.reconcile("run-approval-race")
        resolution = self.approval_resolution(
            scheduler,
            "run-approval-race",
            "approve",
            approved=True,
        )
        barrier = threading.Barrier(2)
        gate_lock = threading.Lock()
        gated_calls = 0
        original_append_event = store.append_event

        def gated_append_event(*args, **kwargs):
            nonlocal gated_calls
            payload = kwargs.get("payload") or {}
            should_wait = False
            if payload.get("control_plane") == "approval_resolved":
                with gate_lock:
                    if gated_calls < 2:
                        gated_calls += 1
                        should_wait = True
            if should_wait:
                barrier.wait(timeout=5)
            return original_append_event(*args, **kwargs)

        store.append_event = gated_append_event
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda _index: scheduler.resolve_approval(
                        "run-approval-race",
                        "approve",
                        resolution,
                    ),
                    range(2),
                )
            )

        self.assertEqual(len(results), 2)
        conflicting = self.approval_resolution(
            scheduler,
            "run-approval-race",
            "approve",
            approved=False,
        )
        with self.assertRaisesRegex(
            SchedulerStateError,
            "not waiting for a decision",
        ):
            scheduler.resolve_approval(
                "run-approval-race",
                "approve",
                conflicting,
            )
        self.assertEqual(
            [
                event.payload.get("control_plane")
                for event in store.list_events("run-approval-race")
            ].count("approval_resolved"),
            1,
        )
        self.assertEqual(
            store.get_run("run-approval-race").status,
            RunStatus.COMPLETED,
        )
        self.assertTrue(store.verify_projections("run-approval-race"))

    def test_approval_retries_after_unrelated_run_projection_update(self) -> None:
        store, scheduler = self.scheduler(
            [
                {
                    "id": "approve",
                    "kind": "approval",
                    "depends_on": [],
                    "config": {"prompt": "continue?"},
                },
                _agent("independent"),
            ]
        )
        scheduler.create_run("run-approval-unrelated-race")
        scheduler.reconcile("run-approval-unrelated-race")
        claim = scheduler.claim_next(
            "run-approval-unrelated-race",
            "worker",
        )
        assert claim is not None
        scheduler.start_claim(claim)
        result_ref = self.artifacts.put_json(
            {"done": True},
            kind=ArtifactKind.TOOL_RESULT,
            producer_run_id=claim.run_id,
            producer_node_id=claim.node_id,
            producer_attempt_id=claim.attempt_id,
        )
        resolution = self.approval_resolution(
            scheduler,
            "run-approval-unrelated-race",
            "approve",
            approved=True,
        )
        approval_entered = threading.Event()
        release_approval = threading.Event()
        gated = False
        gate_lock = threading.Lock()
        original_append_event = store.append_event

        def gated_append_event(*args, **kwargs):
            nonlocal gated
            payload = kwargs.get("payload") or {}
            should_wait = False
            if payload.get("control_plane") == "approval_resolved":
                with gate_lock:
                    if not gated:
                        gated = True
                        should_wait = True
            if should_wait:
                approval_entered.set()
                if not release_approval.wait(timeout=5):
                    raise AssertionError("unrelated completion did not run")
            return original_append_event(*args, **kwargs)

        store.append_event = gated_append_event
        with ThreadPoolExecutor(max_workers=1) as pool:
            approval_future = pool.submit(
                scheduler.resolve_approval,
                "run-approval-unrelated-race",
                "approve",
                resolution,
            )
            self.assertTrue(approval_entered.wait(timeout=5))
            store.complete_activity(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
                claim.request_hash,
                claim.worker_id,
                claim_token=claim.claim_token,
                result=ActivityReceipt((result_ref,)).to_dict(),
                now=101,
            )
            release_approval.set()
            resolved = approval_future.result(timeout=5)

        self.assertEqual(resolved.status, NodeStatus.SUCCEEDED)
        self.assertEqual(
            store.get_run("run-approval-unrelated-race").status,
            RunStatus.COMPLETED,
        )
        self.assertEqual(
            [
                event.payload.get("control_plane")
                for event in store.list_events(
                    "run-approval-unrelated-race"
                )
            ].count("approval_resolved"),
            1,
        )
        self.assertTrue(
            store.verify_projections("run-approval-unrelated-race")
        )

    def test_approval_does_not_swallow_non_concurrent_store_conflict(self) -> None:
        store, scheduler = self.scheduler(
            [
                {
                    "id": "approve",
                    "kind": "approval",
                    "depends_on": [],
                    "config": {"prompt": "continue?"},
                }
            ]
        )
        scheduler.create_run("run-approval-real-conflict")
        scheduler.reconcile("run-approval-real-conflict")
        resolution = self.approval_resolution(
            scheduler,
            "run-approval-real-conflict",
            "approve",
            approved=True,
        )
        original_append_event = store.append_event

        def reject_resolution(*args, **kwargs):
            payload = kwargs.get("payload") or {}
            if payload.get("control_plane") == "approval_resolved":
                raise ProjectionConflictError("injected real conflict")
            return original_append_event(*args, **kwargs)

        store.append_event = reject_resolution
        with self.assertRaisesRegex(
            ProjectionConflictError,
            "injected real conflict",
        ):
            scheduler.resolve_approval(
                "run-approval-real-conflict",
                "approve",
                resolution,
            )

    def test_approval_completion_converges_when_cancel_wins_terminal_cas(
        self,
    ) -> None:
        store, scheduler = self.scheduler(
            [
                {
                    "id": "approve",
                    "kind": "approval",
                    "depends_on": [],
                    "config": {"prompt": "continue?"},
                }
            ]
        )
        scheduler.create_run("run-approval-cancel-race")
        scheduler.reconcile("run-approval-cancel-race")
        resolution = self.approval_resolution(
            scheduler,
            "run-approval-cancel-race",
            "approve",
            approved=True,
        )
        completion_entered = threading.Event()
        release_completion = threading.Event()
        gated = False
        gate_lock = threading.Lock()
        original_append_event = store.append_event

        def gated_append_event(*args, **kwargs):
            nonlocal gated
            event_type = (
                args[1] if len(args) > 1 else kwargs.get("event_type")
            )
            should_wait = False
            if event_type == "run.completed":
                with gate_lock:
                    if not gated:
                        gated = True
                        should_wait = True
            if should_wait:
                completion_entered.set()
                if not release_completion.wait(timeout=5):
                    raise AssertionError("cancel did not release completion")
            return original_append_event(*args, **kwargs)

        store.append_event = gated_append_event
        cancelling = DurableScheduler(
            DurableRunStore(store.path),
            scheduler.workflow,
            clock=self.clock,
            id_factory=IdFactory("approval-cancel"),
            result_writer=self.result_writer,
            input_writer=self.input_writer,
            artifact_verifier=self.artifacts.verify,
            approval_verifier=self.verify_approval,
        )
        with ThreadPoolExecutor(max_workers=1) as pool:
            resolution_future = pool.submit(
                scheduler.resolve_approval,
                "run-approval-cancel-race",
                "approve",
                resolution,
            )
            self.assertTrue(completion_entered.wait(timeout=5))
            cancelled = cancelling.request_cancel(
                "run-approval-cancel-race"
            )
            self.assertEqual(cancelled.status, RunStatus.CANCELLED)
            release_completion.set()
            resolved = resolution_future.result(timeout=5)

        self.assertEqual(resolved.status, NodeStatus.SUCCEEDED)
        self.assertEqual(
            store.get_run("run-approval-cancel-race").status,
            RunStatus.CANCELLED,
        )
        event_types = [
            event.event_type
            for event in store.list_events("run-approval-cancel-race")
        ]
        self.assertEqual(event_types.count("run.completed"), 0)
        self.assertEqual(event_types.count("run.cancelled"), 1)
        self.assertEqual(
            [
                event.payload.get("control_plane")
                for event in store.list_events(
                    "run-approval-cancel-race"
                )
            ].count("approval_resolved"),
            1,
        )
        self.assertTrue(
            store.verify_projections("run-approval-cancel-race")
        )

    def test_router_only_releases_selected_branch(self) -> None:
        store, scheduler = self.scheduler(
            [
                {
                    "id": "choose",
                    "kind": "router",
                    "depends_on": [],
                    "config": {"routes": {"yes": "a", "no": "b"}},
                },
                _agent("a", ["choose"]),
                _agent("b", ["choose"]),
            ]
        )
        scheduler.create_run("run-router")
        scheduler.reconcile("run-router")

        self.assertEqual(
            store.get_node("run-router", "choose").status,
            NodeStatus.WAITING_INPUT,
        )
        scheduler.resolve_router("run-router", "choose", "yes")

        self.assertEqual(store.get_node("run-router", "a").status, NodeStatus.READY)
        self.assertEqual(store.get_node("run-router", "b").status, NodeStatus.SKIPPED)
        self.assertEqual(store.list_attempts("run-router"), [])

    def test_concurrent_conflicting_router_resolution_accepts_one(self) -> None:
        store, scheduler = self.scheduler(
            [
                {
                    "id": "choose",
                    "kind": "router",
                    "depends_on": [],
                    "config": {"routes": {"yes": "a", "no": "b"}},
                },
                _agent("a", ["choose"]),
                _agent("b", ["choose"]),
            ]
        )
        scheduler.create_run("run-router-race")
        scheduler.reconcile("run-router-race")
        barrier = threading.Barrier(2)
        gate_lock = threading.Lock()
        gated_calls = 0
        original_append_event = store.append_event

        def gated_append_event(*args, **kwargs):
            nonlocal gated_calls
            payload = kwargs.get("payload") or {}
            should_wait = False
            if payload.get("control_plane") == "router_resolved":
                with gate_lock:
                    if gated_calls < 2:
                        gated_calls += 1
                        should_wait = True
            if should_wait:
                barrier.wait(timeout=5)
            return original_append_event(*args, **kwargs)

        def resolve(selection: str):
            try:
                return scheduler.resolve_router(
                    "run-router-race",
                    "choose",
                    selection,
                )
            except SchedulerStateError as exc:
                return exc

        store.append_event = gated_append_event
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(resolve, ("yes", "no")))

        self.assertEqual(
            sum(isinstance(result, SchedulerStateError) for result in results),
            1,
        )
        events = [
            event
            for event in store.list_events("run-router-race")
            if event.payload.get("control_plane") == "router_resolved"
        ]
        self.assertEqual(len(events), 1)
        selected = events[0].payload["selected_target"]
        rejected = "b" if selected == "a" else "a"
        self.assertEqual(
            store.get_node("run-router-race", selected).status,
            NodeStatus.READY,
        )
        self.assertEqual(
            store.get_node("run-router-race", rejected).status,
            NodeStatus.SKIPPED,
        )
        self.assertTrue(store.verify_projections("run-router-race"))

    def test_map_and_subworkflow_wait_in_control_plane(self) -> None:
        store, scheduler = self.scheduler(
            [
                {
                    "id": "map",
                    "kind": "map",
                    "depends_on": [],
                    "config": {"items": [1, 2], "body": "body"},
                },
                _agent("body", ["map"]),
                {
                    "id": "child",
                    "kind": "subworkflow",
                    "depends_on": [],
                    "config": {"workflow_id": "child-workflow"},
                },
            ]
        )
        scheduler.create_run("run-map")
        scheduler.reconcile("run-map")

        self.assertEqual(
            store.get_node("run-map", "map").status,
            NodeStatus.WAITING_INPUT,
        )
        self.assertEqual(
            store.get_node("run-map", "child").status,
            NodeStatus.WAITING_INPUT,
        )
        self.assertEqual(store.list_attempts("run-map"), [])
        scheduler.resolve_control("run-map", "map", {"expanded": 2})
        self.assertEqual(store.get_node("run-map", "body").status, NodeStatus.READY)
        scheduler.resolve_control("run-map", "child", {"child_run": "done"})
        self.assertEqual(
            store.get_node("run-map", "child").status,
            NodeStatus.SUCCEEDED,
        )

    def test_pause_resume_and_cancel_are_persisted_intents(self) -> None:
        store, scheduler = self.scheduler([_agent("work")])
        scheduler.create_run("run-intents")
        scheduler.reconcile("run-intents")

        paused = scheduler.request_pause("run-intents")
        self.assertEqual(paused.status, RunStatus.PAUSED)
        self.assertIsNone(scheduler.claim_next("run-intents", "worker"))

        resumed = scheduler.resume("run-intents")
        self.assertEqual(resumed.status, RunStatus.RUNNING)
        claim = scheduler.claim_next("run-intents", "worker")
        scheduler.start_claim(claim)

        cancelling = scheduler.request_cancel("run-intents")
        self.assertEqual(cancelling.status, RunStatus.CANCELLING)
        self.assertEqual(
            store.get_attempt(claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )
        scheduler.confirm_cancel_claim(claim)
        self.assertEqual(store.get_run("run-intents").status, RunStatus.CANCELLED)

    def test_cancel_before_claim_converges_without_external_execution(self) -> None:
        store, scheduler = self.scheduler([_agent("a"), _agent("b")])
        scheduler.create_run("run-cancel")
        scheduler.reconcile("run-cancel")

        run = scheduler.request_cancel("run-cancel")

        self.assertEqual(run.status, RunStatus.CANCELLED)
        self.assertEqual(
            {node.status for node in store.list_nodes("run-cancel")},
            {NodeStatus.CANCELLED},
        )
        self.assertEqual(store.list_attempts("run-cancel"), [])

    def test_concurrent_schedulers_claim_only_one_active_attempt(self) -> None:
        store, scheduler = self.scheduler([_agent("work")])
        scheduler.create_run("run-race")
        scheduler.reconcile("run-race")
        barrier = threading.Barrier(2)
        schedulers = [
            DurableScheduler(
                store,
                scheduler.workflow,
                clock=self.clock,
                id_factory=IdFactory("left"),
                result_writer=self.result_writer,
                input_writer=self.input_writer,
                artifact_verifier=self.artifacts.verify,
                approval_verifier=self.verify_approval,
            ),
            DurableScheduler(
                store,
                scheduler.workflow,
                clock=self.clock,
                id_factory=IdFactory("right"),
                result_writer=self.result_writer,
                input_writer=self.input_writer,
                artifact_verifier=self.artifacts.verify,
                approval_verifier=self.verify_approval,
            ),
        ]

        def compete(candidate: DurableScheduler):
            barrier.wait()
            return candidate.claim_next("run-race", "worker", capacity=2)

        with ThreadPoolExecutor(max_workers=2) as pool:
            claims = list(pool.map(compete, schedulers))

        self.assertEqual(sum(claim is not None for claim in claims), 1)
        attempts = store.list_attempts("run-race")
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0].status, AttemptStatus.CLAIMED)

    def test_schedule_attempt_rejects_unrelated_active_attempt_after_cas_loss(
        self,
    ) -> None:
        store, scheduler = self.scheduler([_agent("work")])
        scheduler.create_run("run-schedule-identity")
        scheduler.reconcile("run-schedule-identity")
        stale_run = store.get_run("run-schedule-identity")
        stale_node = store.get_node("run-schedule-identity", "work")
        assert stale_run is not None and stale_node is not None
        unrelated = AttemptRecord(
            attempt_id="attempt-unrelated",
            run_id=stale_run.run_id,
            node_id=stale_node.node_id,
            attempt_number=1,
            idempotency_key="unrelated-claim-key",
            activity_kind="agent",
            effect_class="read_only",
            metadata={
                "definition_digest": scheduler.workflow.definition_digest,
                "request_hash": "unrelated-request",
                "operation_key": "unrelated-operation",
            },
            scheduled_at=self.clock(),
        )
        store.append_event(
            stale_run.run_id,
            "attempt.scheduled",
            node_id=stale_node.node_id,
            attempt_id=unrelated.attempt_id,
            expected_run_version=stale_run.projection_version,
            expected_node_version=stale_node.projection_version,
            node_projection=replace(stale_node, attempt_count=1),
            attempt_projection=unrelated,
        )

        with self.assertRaises(ConcurrentProjectionUpdate):
            scheduler._schedule_attempt(
                stale_run,
                stale_node,
                scheduler.workflow.get_node("work"),
                (),
            )
        self.assertEqual(
            [
                attempt.attempt_id
                for attempt in store.list_attempts(
                    "run-schedule-identity"
                )
            ],
            [unrelated.attempt_id],
        )

    def test_ensure_nodes_does_not_swallow_unrelated_real_conflict(self) -> None:
        store, scheduler = self.scheduler([_agent("work")])
        scheduler.create_run("run-node-real-conflict")

        with patch.object(store, "list_nodes", return_value=[]):
            with patch.object(
                store,
                "append_event",
                side_effect=ProjectionConflictError(
                    "injected non-idempotent conflict"
                ),
            ):
                with self.assertRaisesRegex(
                    ProjectionConflictError,
                    "injected non-idempotent conflict",
                ):
                    scheduler._ensure_nodes("run-node-real-conflict")

    def test_large_success_result_is_replaced_by_bounded_artifact_receipt(self) -> None:
        store, scheduler = self.scheduler([_agent("work")])
        scheduler.create_run("run-artifact")
        claim = scheduler.claim_next("run-artifact", "worker")
        scheduler.start_claim(claim)
        secret = "large-secret-value-" * 10_000

        scheduler.complete_claim(claim, {"response": secret})

        attempt = store.get_attempt(claim.attempt_id)
        self.assertIsNone(attempt.result.get("output"))
        self.assertEqual(attempt.result["outcome"], "succeeded")
        self.assertEqual(len(attempt.result["artifact_refs"]), 1)
        sqlite_bytes = b"".join(
            path.read_bytes()
            for path in (
                store.path,
                Path(f"{store.path}-wal"),
                Path(f"{store.path}-shm"),
            )
            if path.exists()
        )
        self.assertNotIn(secret.encode("utf-8"), sqlite_bytes)

    def test_success_without_result_writer_fails_before_projection_change(self) -> None:
        store, scheduler = self.scheduler([_agent("work")])
        scheduler.create_run("run-no-writer")
        claim = scheduler.claim_next("run-no-writer", "worker")
        scheduler.start_claim(claim)
        without_writer = DurableScheduler(
            store,
            scheduler.workflow,
            clock=self.clock,
            id_factory=self.ids,
            artifact_verifier=self.artifacts.verify,
        )

        with self.assertRaises(ResultPersistenceError):
            without_writer.complete_claim(claim, {"raw": "must-not-persist"})

        self.assertEqual(
            store.get_attempt(claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )

    def test_raw_input_without_writer_fails_before_run_creation(self) -> None:
        store = DurableRunStore(self.root / "no-input-writer.sqlite3")
        scheduler = DurableScheduler(
            store,
            _workflow([_agent("work")]),
            clock=self.clock,
            id_factory=self.ids,
            artifact_verifier=self.artifacts.verify,
        )

        with self.assertRaises(InputPersistenceError):
            scheduler.create_run("run-raw-input", input={"secret": "not-durable"})

        self.assertIsNone(store.get_run("run-raw-input"))

    def test_missing_result_artifact_never_completes_node_or_event(self) -> None:
        store, scheduler = self.scheduler([_agent("work")])
        scheduler.create_run("run-missing-artifact")
        claim = scheduler.claim_next("run-missing-artifact", "worker")
        scheduler.start_claim(claim)

        def missing_writer(active_claim, raw_result) -> ActivityReceipt:
            receipt = self.result_writer(active_claim, raw_result)
            ref = receipt.artifact_refs[0]
            (self.artifacts.root / ref.uri).unlink()
            return receipt

        broken = DurableScheduler(
            store,
            scheduler.workflow,
            clock=self.clock,
            id_factory=self.ids,
            result_writer=missing_writer,
            input_writer=self.input_writer,
            artifact_verifier=self.artifacts.verify,
        )
        with self.assertRaises(ResultPersistenceError):
            broken.complete_claim(claim, {"output": "lost"})

        self.assertEqual(
            store.get_attempt(claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )
        self.assertNotIn(
            "attempt.succeeded",
            [event.event_type for event in store.list_events("run-missing-artifact")],
        )

    def test_rejected_written_artifact_is_orphaned_not_referenced(self) -> None:
        store, scheduler = self.scheduler([_agent("work")])
        scheduler.create_run("run-orphan")
        claim = scheduler.claim_next("run-orphan", "worker")
        scheduler.start_claim(claim)
        written = []

        def writer(active_claim, raw_result) -> ActivityReceipt:
            receipt = self.result_writer(active_claim, raw_result)
            written.extend(receipt.artifact_refs)
            return receipt

        rejecting = DurableScheduler(
            store,
            scheduler.workflow,
            clock=self.clock,
            id_factory=self.ids,
            result_writer=writer,
            input_writer=self.input_writer,
            artifact_verifier=lambda _ref: False,
        )
        with self.assertRaises(ResultPersistenceError):
            rejecting.complete_claim(claim, {"output": "orphan"})

        self.assertEqual(len(written), 1)
        self.assertTrue(self.artifacts.exists(written[0]))
        self.assertNotIn(
            written[0].sha256,
            str(store.list_events("run-orphan")),
        )


if __name__ == "__main__":
    unittest.main()
