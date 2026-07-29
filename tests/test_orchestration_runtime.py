from __future__ import annotations

import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.orchestration.artifacts import LocalArtifactStore
from src.orchestration.evaluation import ReliabilityEvidence
from src.orchestration.protocol import ParentSubmission
from src.orchestration.runtime import (
    AuthorizationRequest,
    OrchestrationRuntime,
    RuntimeAuthorizationError,
    RuntimeCapabilityUnavailable,
)
from src.orchestration.scheduler import DurableScheduler, RunInputReceipt
from src.orchestration.store import DurableRunStore
from src.orchestration.workflow import compile_workflow


class _Ids:
    def __init__(self) -> None:
        self.value = 0

    def __call__(self, prefix: str) -> str:
        self.value += 1
        return f"{prefix}-runtime-{self.value}"


class _Authorizer:
    def __init__(self) -> None:
        self.requests: list[AuthorizationRequest] = []

    def authorize(self, request: AuthorizationRequest, context) -> bool:
        self.requests.append(request)
        return context == "trusted-secret-context"


class OrchestrationRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.store = DurableRunStore(self.root / "orchestration.sqlite3")
        self.artifacts = LocalArtifactStore(self.root / "artifacts")
        self.workflow = {
            "schema_version": 2,
            "name": "runtime-workflow",
            "version": 1,
            "nodes": [
                {
                    "id": "step",
                    "kind": "tool",
                    "depends_on": [],
                    "config": {
                        "tool": "inspect",
                        "arguments": {"credential": "WORKFLOW-SECRET"},
                    },
                    "effect_class": "read_only",
                }
            ],
        }
        self.workflow_ref = self.artifacts.put_json(self.workflow)
        self.input_ref = self.artifacts.put_json(
            {"secret": "INPUT-SECRET"},
        )
        self.authorizer = _Authorizer()
        self.ids = _Ids()

    def scheduler_factory(self, store, workflow) -> DurableScheduler:
        return DurableScheduler(
            store,
            workflow,
            clock=lambda: 100.0,
            id_factory=self.ids,
            artifact_verifier=self.artifacts.verify,
        )

    def runtime(self, **overrides) -> OrchestrationRuntime:
        options = {
            "store": self.store,
            "artifact_store": self.artifacts,
            "compiler": compile_workflow,
            "scheduler_factory": self.scheduler_factory,
            "authorizer": self.authorizer,
        }
        options.update(overrides)
        return OrchestrationRuntime(**options)

    def submit_payload(
        self,
        *,
        run_id: str = "stable-run",
        workflow_ref=None,
        input_ref=None,
        parent=None,
    ) -> dict:
        workflow_ref = workflow_ref or self.workflow_ref
        input_ref = self.input_ref if input_ref is None else input_ref
        return {
            "protocol_version": 1,
            "request_id": "submit-request",
            "operation": "submit",
            "body": {
                "run_id": run_id,
                "workflow_ref": workflow_ref.to_dict(),
                "input_receipt": (
                    None
                    if input_ref is False
                    else {"artifact_refs": [input_ref.to_dict()]}
                ),
                "parent": parent,
            },
        }

    def handle(self, runtime, payload):
        return runtime.handle(
            payload,
            authorization_context="trusted-secret-context",
        ).to_dict()

    def test_default_authorizer_denies_mutation_before_store_or_artifact_work(self) -> None:
        runtime = OrchestrationRuntime(
            store=self.store,
            artifact_store=self.artifacts,
            scheduler_factory=self.scheduler_factory,
        )

        response = runtime.handle(self.submit_payload()).to_dict()

        self.assertFalse(response["ok"])
        self.assertEqual(
            response["error"]["code"],
            "authorization_denied",
        )
        self.assertEqual(self.store.list_runs(), [])

    def test_submit_is_idempotent_after_response_loss_and_restart(self) -> None:
        first_runtime = self.runtime()
        first = self.handle(first_runtime, self.submit_payload())
        event_count = len(self.store.list_events("stable-run"))

        repeated = self.handle(first_runtime, self.submit_payload())
        restarted = self.runtime()
        after_restart = self.handle(restarted, self.submit_payload())

        self.assertTrue(first["ok"])
        self.assertEqual(first["result"], repeated["result"])
        self.assertEqual(first["result"], after_restart["result"])
        self.assertEqual(len(self.store.list_runs()), 1)
        self.assertEqual(len(self.store.list_events("stable-run")), event_count)
        cancelled = self.handle(
            restarted,
            {
                "protocol_version": 1,
                "request_id": "cancel-after-rebind",
                "operation": "cancel",
                "body": {"run_id": "stable-run"},
            },
        )
        self.assertTrue(cancelled["ok"])

    def test_scheduler_cache_configuration_is_hard_bounded(self) -> None:
        self.assertEqual(self.runtime().max_cached_schedulers, 64)
        for value in (0, True, 1025, "4"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    self.runtime(max_cached_schedulers=value)

    def test_scheduler_cache_prefers_terminal_eviction(self) -> None:
        runtime = self.runtime(max_cached_schedulers=2)
        receipt = RunInputReceipt((self.input_ref,))
        for run_id in ("active-a", "terminal-b"):
            runtime.submit(
                run_id,
                self.workflow_ref,
                input_receipt=receipt,
                authorization_context="trusted-secret-context",
            )
        runtime.tick(
            "active-a",
            authorization_context="trusted-secret-context",
        )
        runtime.cancel(
            "terminal-b",
            authorization_context="trusted-secret-context",
        )

        runtime.submit(
            "active-c",
            self.workflow_ref,
            input_receipt=receipt,
            authorization_context="trusted-secret-context",
        )

        self.assertEqual(
            tuple(runtime._schedulers),
            ("active-a", "active-c"),
        )
        self.assertTrue(self.store.get_run("terminal-b").status.is_terminal)

    def test_full_active_cache_evicts_rebindable_scheduler(self) -> None:
        runtime = self.runtime(max_cached_schedulers=1)
        receipt = RunInputReceipt((self.input_ref,))
        first = runtime.submit(
            "active-one",
            self.workflow_ref,
            input_receipt=receipt,
            authorization_context="trusted-secret-context",
        )
        event_count = len(self.store.list_events("active-one"))

        repeated = runtime.submit(
            "active-one",
            self.workflow_ref,
            input_receipt=receipt,
            authorization_context="trusted-secret-context",
        )
        created = runtime.submit(
            "created-after-eviction",
            self.workflow_ref,
            input_receipt=receipt,
            authorization_context="trusted-secret-context",
        )

        self.assertEqual(first, repeated)
        self.assertEqual(len(self.store.list_events("active-one")), event_count)
        self.assertEqual(created["run_id"], "created-after-eviction")
        self.assertIsNotNone(self.store.get_run("created-after-eviction"))
        self.assertEqual(tuple(runtime._schedulers), ("created-after-eviction",))

    def test_uncached_existing_submit_stays_idempotent_when_active_cache_is_full(
        self,
    ) -> None:
        receipt = RunInputReceipt((self.input_ref,))
        seed_runtime = self.runtime()
        seeded = seed_runtime.submit(
            "persisted-before-restart",
            self.workflow_ref,
            input_receipt=receipt,
            authorization_context="trusted-secret-context",
        )
        event_count = len(self.store.list_events("persisted-before-restart"))

        restarted = self.runtime(max_cached_schedulers=1)
        restarted.submit(
            "restart-cache-occupant",
            self.workflow_ref,
            input_receipt=receipt,
            authorization_context="trusted-secret-context",
        )
        rebound = restarted.submit(
            "persisted-before-restart",
            self.workflow_ref,
            input_receipt=receipt,
            authorization_context="trusted-secret-context",
        )

        self.assertEqual(seeded, rebound)
        self.assertEqual(
            len(self.store.list_events("persisted-before-restart")),
            event_count,
        )
        self.assertEqual(
            tuple(restarted._schedulers),
            ("persisted-before-restart",),
        )

    def test_resolver_allows_active_eviction_and_restart_rebind(self) -> None:
        compiled = compile_workflow(self.workflow)
        resolved: list[str] = []

        def resolver(run):
            resolved.append(run.run_id)
            return self.scheduler_factory(self.store, compiled)

        runtime = self.runtime(
            max_cached_schedulers=1,
            scheduler_resolver=resolver,
        )
        receipt = RunInputReceipt((self.input_ref,))
        for run_id in ("evicted-active", "cached-active"):
            runtime.submit(
                run_id,
                self.workflow_ref,
                input_receipt=receipt,
                authorization_context="trusted-secret-context",
            )
        self.assertEqual(tuple(runtime._schedulers), ("cached-active",))

        runtime.cancel(
            "evicted-active",
            authorization_context="trusted-secret-context",
        )

        self.assertEqual(resolved, ["evicted-active"])
        self.assertEqual(tuple(runtime._schedulers), ("evicted-active",))
        self.assertTrue(self.store.get_run("evicted-active").status.is_terminal)

    def test_concurrent_submit_bounds_cache_without_dropping_durable_runs(self) -> None:
        runtime = self.runtime(max_cached_schedulers=2)
        receipt = RunInputReceipt((self.input_ref,))
        start = threading.Barrier(8)

        def submit(index: int) -> bool:
            start.wait()
            try:
                runtime.submit(
                    f"concurrent-{index}",
                    self.workflow_ref,
                    input_receipt=receipt,
                    authorization_context="trusted-secret-context",
                )
            except RuntimeCapabilityUnavailable:
                return False
            return True

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(submit, range(8)))

        self.assertEqual(sum(results), 8)
        self.assertEqual(len(runtime._schedulers), 2)
        self.assertEqual(len(self.store.list_runs()), 8)

    def test_concurrent_same_submit_remains_idempotent_at_capacity(self) -> None:
        runtime = self.runtime(max_cached_schedulers=1)
        receipt = RunInputReceipt((self.input_ref,))
        start = threading.Barrier(8)

        def submit(_index: int) -> dict:
            start.wait()
            return runtime.submit(
                "one-idempotent-run",
                self.workflow_ref,
                input_receipt=receipt,
                authorization_context="trusted-secret-context",
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(submit, range(8)))

        self.assertTrue(all(result == results[0] for result in results))
        self.assertEqual(len(self.store.list_runs()), 1)
        self.assertEqual(tuple(runtime._schedulers), ("one-idempotent-run",))

    def test_concurrent_ticks_converge_committed_projection_contention(self) -> None:
        runtimes = (
            self.runtime(),
            self.runtime(store=DurableRunStore(self.store.path)),
        )
        receipt = RunInputReceipt((self.input_ref,))
        runtimes[0].submit(
            "concurrent-tick-run",
            self.workflow_ref,
            input_receipt=receipt,
            authorization_context="trusted-secret-context",
        )
        runtimes[1].submit(
            "concurrent-tick-run",
            self.workflow_ref,
            input_receipt=receipt,
            authorization_context="trusted-secret-context",
        )
        start = threading.Barrier(8)

        def tick(index: int) -> dict:
            start.wait()
            return runtimes[index % len(runtimes)].tick(
                "concurrent-tick-run",
                max_steps=4,
                authorization_context="trusted-secret-context",
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(tick, range(8)))

        self.assertEqual(
            {result["status"] for result in results},
            {"running"},
        )
        self.assertEqual(
            self.store.get_run("concurrent-tick-run").status.value,
            "running",
        )
        event_types = [
            event.event_type
            for event in self.store.list_events("concurrent-tick-run")
        ]
        self.assertEqual(event_types.count("run.started"), 1)
        self.assertEqual(event_types.count("node.ready"), 1)
        self.assertTrue(self.store.verify_projections("concurrent-tick-run"))

    def test_existing_run_rejects_changed_workflow_input_or_parent_binding(self) -> None:
        runtime = self.runtime()
        self.assertTrue(self.handle(runtime, self.submit_payload())["ok"])
        other_input = self.artifacts.put_json({"different": True})
        other_workflow = dict(self.workflow)
        other_workflow["version"] = 2
        other_workflow_ref = self.artifacts.put_json(other_workflow)
        mismatches = (
            self.submit_payload(input_ref=other_input),
            self.submit_payload(workflow_ref=other_workflow_ref),
            self.submit_payload(
                parent={
                    "parent_run_id": "parent",
                    "parent_node_id": "node",
                }
            ),
        )

        for payload in mismatches:
            with self.subTest(payload=payload):
                response = self.handle(self.runtime(), payload)
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], "invalid_input")
        self.assertEqual(len(self.store.list_runs()), 1)

    def test_parent_scope_reaches_authorizer_and_hierarchy_defaults_closed(self) -> None:
        runtime = self.runtime()
        payload = self.submit_payload(
            run_id="child",
            parent={
                "parent_run_id": "parent-run",
                "parent_node_id": "parent-node",
            },
        )

        response = self.handle(runtime, payload)

        self.assertFalse(response["ok"])
        self.assertEqual(
            response["error"]["code"],
            "capability_unavailable",
        )
        authorization = self.authorizer.requests[-1]
        self.assertEqual(authorization.run_id, "child")
        self.assertEqual(authorization.parent_run_id, "parent-run")
        self.assertEqual(authorization.parent_node_id, "parent-node")
        self.assertEqual(self.store.list_runs(), [])

    def test_explicit_hierarchy_submitter_owns_parent_submission(self) -> None:
        seen = []

        def submit_parent(parent, scheduler, run_id, receipt, metadata):
            seen.append((parent, run_id))
            return scheduler.create_run(
                run_id,
                input=receipt,
                metadata=metadata,
            )

        runtime = self.runtime(hierarchy_submitter=submit_parent)
        result = runtime.submit(
            "child-run",
            self.workflow_ref,
            input_receipt=RunInputReceipt((self.input_ref,)),
            parent=ParentSubmission("parent-run", "parent-node"),
            authorization_context="trusted-secret-context",
        )

        self.assertEqual(result["run_id"], "child-run")
        self.assertEqual(seen[0][0].parent_run_id, "parent-run")

    def test_protocol_rejects_unsafe_run_id_before_creation(self) -> None:
        payload = self.submit_payload(run_id="../unsafe")
        response = self.handle(self.runtime(), payload)

        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "invalid_request")
        self.assertEqual(self.store.list_runs(), [])

    def test_artifact_metadata_is_rejected_without_event_or_response_leakage(self) -> None:
        token = "LOW-ENTROPY-METADATA-SECRET"
        input_with_metadata = self.artifacts.put_json(
            {"safe": "content"},
            metadata={"caller_text": token},
        )
        workflow_with_metadata = self.artifacts.put_json(
            self.workflow,
            metadata={"caller_text": token},
        )

        responses = (
            self.handle(
                self.runtime(),
                self.submit_payload(
                    run_id="metadata-input",
                    input_ref=input_with_metadata,
                ),
            ),
            self.handle(
                self.runtime(),
                self.submit_payload(
                    run_id="metadata-workflow",
                    workflow_ref=workflow_with_metadata,
                ),
            ),
        )

        for response in responses:
            self.assertFalse(response["ok"])
            self.assertEqual(response["error"]["code"], "invalid_input")
            self.assertNotIn(token, repr(response))
        self.assertEqual(self.store.list_runs(), [])

    def test_public_status_and_events_omit_payload_and_execution_internals(self) -> None:
        runtime = self.runtime()
        self.assertTrue(self.handle(runtime, self.submit_payload())["ok"])
        self.store.append_event(
            "stable-run",
            "audit.note",
            node_id="step",
            payload={
                "path": "/private/secret",
                "worker_id": "worker-secret",
                "lease_id": "lease-secret",
                "idempotency_key": "idempotency-secret",
                "actor": "actor-secret",
                "payload": "INPUT-SECRET",
            },
        )
        response = self.handle(
            runtime,
            {
                "protocol_version": 1,
                "request_id": "events",
                "operation": "events",
                "body": {
                    "run_id": "stable-run",
                    "after_sequence": 0,
                    "limit": 100,
                },
            },
        )
        rendered = repr(response)

        self.assertTrue(response["ok"])
        for secret in (
            "WORKFLOW-SECRET",
            "INPUT-SECRET",
            "/private/secret",
            "worker-secret",
            "lease-secret",
            "idempotency-secret",
            "actor-secret",
        ):
            self.assertNotIn(secret, rendered)
        for event in response["result"]["events"]:
            self.assertEqual(
                set(event),
                {
                    "sequence",
                    "type",
                    "occurred_at",
                    "has_node",
                    "has_attempt",
                    "terminal",
                },
            )

    def test_tick_is_bounded_and_recover_without_driver_is_not_attempted(self) -> None:
        executor = object()
        calls = []

        def execute(scheduler, supplied_executor, run_id, maximum):
            calls.append((scheduler, supplied_executor, run_id, maximum))
            return 2

        runtime = self.runtime(
            executor=executor,
            execution_driver=execute,
        )
        self.assertTrue(self.handle(runtime, self.submit_payload())["ok"])
        tick = self.handle(
            runtime,
            {
                "protocol_version": 1,
                "request_id": "tick",
                "operation": "tick",
                "body": {"run_id": "stable-run", "max_steps": 2},
            },
        )
        recover = self.handle(
            runtime,
            {
                "protocol_version": 1,
                "request_id": "recover",
                "operation": "recover",
                "body": {"run_id": "stable-run", "limit": 5},
            },
        )

        self.assertTrue(tick["ok"])
        self.assertEqual(tick["result"]["tick"]["processed"], 2)
        self.assertIs(calls[0][1], executor)
        self.assertEqual(calls[0][2], "stable-run")
        self.assertEqual(calls[0][3], 2)
        self.assertFalse(recover["ok"])
        self.assertEqual(
            recover["error"]["code"],
            "capability_unavailable",
        )
        self.assertNotIn("attempted", repr(recover))

    def test_executor_factory_and_driver_are_bound_to_requested_run(self) -> None:
        factory_calls = []
        driver_calls = []

        def factory(scheduler):
            executor = object()
            factory_calls.append((scheduler, executor))
            return executor

        def execute(scheduler, executor, run_id, maximum):
            self.assertIsNotNone(scheduler.store.get_run(run_id))
            driver_calls.append((scheduler, executor, run_id, maximum))
            return 0

        runtime = self.runtime(
            executor_factory=factory,
            execution_driver=execute,
        )
        for run_id in ("factory-run-a", "factory-run-b"):
            runtime.submit(
                run_id,
                self.workflow_ref,
                input_receipt=RunInputReceipt((self.input_ref,)),
                authorization_context="trusted-secret-context",
            )
            runtime.tick(
                run_id,
                max_steps=3,
                authorization_context="trusted-secret-context",
            )

        self.assertEqual(
            [call[2] for call in driver_calls],
            ["factory-run-a", "factory-run-b"],
        )
        self.assertEqual(
            [call[3] for call in driver_calls],
            [3, 3],
        )
        for factory_call, driver_call in zip(
            factory_calls,
            driver_calls,
            strict=True,
        ):
            self.assertIs(factory_call[0], driver_call[0])
            self.assertIs(factory_call[1], driver_call[1])
        with self.assertRaises(ValueError):
            self.runtime(
                executor=object(),
                executor_factory=factory,
            )

    def test_recovery_driver_is_bound_to_requested_run(self) -> None:
        calls = []

        def recover(scheduler, run_id, limit):
            self.assertIsNotNone(scheduler.store.get_run(run_id))
            calls.append((scheduler, run_id, limit))
            return 1

        runtime = self.runtime(recovery_driver=recover)
        runtime.submit(
            "recovery-bound-run",
            self.workflow_ref,
            input_receipt=RunInputReceipt((self.input_ref,)),
            authorization_context="trusted-secret-context",
        )

        result = runtime.recover(
            "recovery-bound-run",
            limit=4,
            authorization_context="trusted-secret-context",
        )

        self.assertEqual(result["recovery"], {"attempted": True, "processed": 1})
        self.assertEqual(calls[0][1:], ("recovery-bound-run", 4))

    def test_restart_scheduler_resolver_supports_control_mutation(self) -> None:
        first = self.runtime()
        self.assertTrue(self.handle(first, self.submit_payload())["ok"])
        compiled = compile_workflow(self.workflow)
        restarted = self.runtime(
            scheduler_resolver=lambda _run: self.scheduler_factory(
                self.store,
                compiled,
            )
        )

        cancelled = self.handle(
            restarted,
            {
                "protocol_version": 1,
                "request_id": "cancel",
                "operation": "cancel",
                "body": {"run_id": "stable-run"},
            },
        )

        self.assertTrue(cancelled["ok"])
        self.assertIn(cancelled["result"]["status"], {"cancelling", "cancelled"})

    def test_restart_rebinds_persisted_workflow_without_external_resolver(
        self,
    ) -> None:
        first = self.runtime()
        receipt = RunInputReceipt((self.input_ref,))
        first.submit(
            "autonomous-restart-run",
            self.workflow_ref,
            input_receipt=receipt,
            authorization_context="trusted-secret-context",
        )

        restarted = self.runtime()
        cancelled = restarted.cancel(
            "autonomous-restart-run",
            authorization_context="trusted-secret-context",
        )

        self.assertEqual(cancelled["status"], "cancelled")
        self.assertTrue(
            self.store.verify_projections("autonomous-restart-run")
        )

    def test_restart_fails_closed_when_persisted_workflow_artifact_is_missing(
        self,
    ) -> None:
        first = self.runtime()
        receipt = RunInputReceipt((self.input_ref,))
        first.submit(
            "missing-definition-run",
            self.workflow_ref,
            input_receipt=receipt,
            authorization_context="trusted-secret-context",
        )
        workflow_path = self.artifacts.root / self.workflow_ref.uri
        workflow_path.unlink()

        restarted = self.runtime()
        with self.assertRaisesRegex(
            RuntimeCapabilityUnavailable,
            "cannot be rebound",
        ):
            restarted.cancel(
                "missing-definition-run",
                authorization_context="trusted-secret-context",
            )

    def test_replay_and_evaluation_are_read_only_authorized_capabilities(self) -> None:
        runtime = self.runtime()
        self.assertTrue(self.handle(runtime, self.submit_payload())["ok"])

        replay = runtime.replay_run(
            "stable-run",
            authorization_context="trusted-secret-context",
        )
        evaluation = runtime.evaluate(
            ReliabilityEvidence(
                recovery_successes=1,
                recovery_attempts=1,
            ),
            authorization_context="trusted-secret-context",
        )

        self.assertTrue(replay["matches_live"])
        self.assertEqual(
            evaluation["recovery_success_rate"]["value"],
            1.0,
        )
        with self.assertRaises(RuntimeAuthorizationError):
            runtime.evaluate(ReliabilityEvidence())


if __name__ == "__main__":
    unittest.main()
