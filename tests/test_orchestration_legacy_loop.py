from __future__ import annotations

import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any

from src.orchestration import (
    AttemptRecord,
    ActivityAdmissionDenied,
    AttemptStatus,
    DurableRunStore,
    NodeRecord,
    NodeStatus,
    RunRecord,
    RunStatus,
)
from src.orchestration.artifacts import JsonArtifactResultWriter, LocalArtifactStore
from src.orchestration.legacy_loop import (
    ActivityClaimConflict,
    ActivityRecoveryRequired,
    DurabilityError,
    LegacyActivityError,
    LegacyAgentLoopAdapter,
    ResultPersistenceError,
)

TEST_DEFINITION_DIGEST = "b" * 64


def _completed_result(response: str = "done") -> dict[str, Any]:
    return {
        "response": response,
        "exit_reason": "CURRENT_TASK_DONE",
        "tool_results": [{"tool_name": "echo", "data": {"status": "OK"}}],
        "turns": 2,
    }


class StoreFixture:
    def __init__(
        self,
        root: Path,
        *,
        effect_class: str = "non_idempotent_write",
        root_node: bool = False,
        run_id: str = "run-1",
        node_id: str = "legacy-agent",
        attempt_id: str = "attempt-1",
        resource_keys: tuple[str, ...] = (),
        concurrency_key: str | None = None,
    ) -> None:
        self.store = DurableRunStore(root / "orchestration.sqlite3")
        self.artifact_store = LocalArtifactStore(root / "artifacts")
        self.run_id = run_id
        self.node_id = node_id
        self.attempt_id = attempt_id
        self.store.create_run(
            RunRecord(
                self.run_id,
                "legacy-workflow",
                definition_digest=TEST_DEFINITION_DIGEST,
            )
        )
        run = self.store.get_run(self.run_id)
        assert run is not None
        self.store.append_event(
            self.run_id,
            "node.pending",
            node_id=self.node_id,
            node_projection=NodeRecord(
                self.run_id,
                self.node_id,
                "agent",
                status=NodeStatus.PENDING,
                metadata={"root_node": root_node},
            ),
            expected_run_version=run.projection_version,
        )
        run = self.store.get_run(self.run_id)
        node = self.store.get_node(self.run_id, self.node_id)
        assert run is not None
        assert node is not None
        self.store.append_event(
            self.run_id,
            "node.ready",
            node_id=self.node_id,
            node_projection=replace(
                node,
                status=NodeStatus.READY,
                attempt_count=1,
            ),
            expected_run_version=run.projection_version,
            expected_node_version=node.projection_version,
        )
        run = self.store.get_run(self.run_id)
        node = self.store.get_node(self.run_id, self.node_id)
        assert run is not None
        assert node is not None
        self.store.append_event(
            self.run_id,
            "attempt.scheduled",
            node_id=self.node_id,
            attempt_id=self.attempt_id,
            attempt_projection=AttemptRecord(
                self.attempt_id,
                self.run_id,
                self.node_id,
                1,
                effect_class=effect_class,
                metadata={
                    "resource_keys": list(resource_keys),
                    "concurrency_key": concurrency_key,
                },
            ),
            expected_run_version=run.projection_version,
            expected_node_version=node.projection_version,
        )

    def execute(
        self,
        runner,
        **kwargs: Any,
    ) -> dict[str, Any]:
        adapter_kwargs = kwargs.pop("adapter_kwargs", {})
        if "result_writer" not in adapter_kwargs:
            adapter_kwargs["result_writer"] = JsonArtifactResultWriter(
                self.artifact_store,
                producer_run_id=self.run_id,
                producer_node_id=self.node_id,
                producer_attempt_id=self.attempt_id,
            )
        if (
            adapter_kwargs.get("result_writer") is not None
            and "artifact_verifier" not in adapter_kwargs
        ):
            adapter_kwargs["artifact_verifier"] = self.artifact_store.verify
        return LegacyAgentLoopAdapter(self.store, runner, **adapter_kwargs).run(
            self.run_id,
            "do work",
            node_id=self.node_id,
            attempt_id=self.attempt_id,
            **kwargs,
        )


class LegacyAgentLoopAdapterTests(unittest.TestCase):
    def test_success_persists_node_and_attempt_without_finalizing_run(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))

            result = fixture.execute(lambda _task: _completed_result())

            self.assertEqual(result, _completed_result())
            self.assertEqual(fixture.store.get_run(fixture.run_id).status, RunStatus.RUNNING)
            node = fixture.store.get_node(fixture.run_id, fixture.node_id)
            attempt = fixture.store.get_attempt(fixture.attempt_id)
            self.assertEqual(node.status, NodeStatus.SUCCEEDED)
            self.assertEqual(attempt.status, AttemptStatus.SUCCEEDED)
            self.assertIsNone(attempt.result["output"])
            self.assertEqual(
                attempt.result["artifact_refs"][0]["kind"],
                "model_response",
            )
            events = fixture.store.list_events(fixture.run_id)
            self.assertEqual(
                [event.event_type for event in events],
                [
                    "run.created",
                    "node.pending",
                    "node.ready",
                    "attempt.scheduled",
                    "attempt.claimed",
                    "attempt.started",
                    "attempt.succeeded",
                ],
            )
            self.assertNotIn("response", events[-1].payload)

    def test_default_artifact_mode_keeps_sensitive_result_out_of_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            fixture = StoreFixture(root)
            secret_response = "sqlite-secret-response-7f419"
            secret_tool_arg = "sqlite-secret-tool-arg-19ac2"

            fixture.execute(
                lambda _task: {
                    "response": secret_response,
                    "exit_reason": "CURRENT_TASK_DONE",
                    "tool_results": [
                        {
                            "tool_name": "browser_js",
                            "args": {"token": secret_tool_arg},
                            "data": {"status": "OK"},
                        }
                    ],
                    "turns": 2,
                }
            )

            sqlite_bytes = b"".join(
                path.read_bytes()
                for path in (
                    fixture.store.path,
                    Path(f"{fixture.store.path}-wal"),
                    Path(f"{fixture.store.path}-shm"),
                )
                if path.exists()
            )
            self.assertNotIn(secret_response.encode(), sqlite_bytes)
            self.assertNotIn(secret_tool_arg.encode(), sqlite_bytes)

    def test_positive_inline_limit_is_explicit_compatibility_opt_in(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))

            fixture.execute(
                lambda _task: _completed_result("explicit-inline"),
                adapter_kwargs={
                    "result_writer": None,
                    "inline_result_limit": 4096,
                },
            )

            attempt = fixture.store.get_attempt(fixture.attempt_id)
            self.assertEqual(
                attempt.result["output"]["legacy_result"]["response"],
                "explicit-inline",
            )

    def test_finalize_run_is_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw), root_node=True)

            fixture.execute(lambda _task: _completed_result(), finalize_run=True)

            run = fixture.store.get_run(fixture.run_id)
            self.assertEqual(run.status, RunStatus.COMPLETED)
            self.assertEqual(run.output["outcome"], "succeeded")

    def test_finalize_run_rejects_non_root_node(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))

            with self.assertRaisesRegex(LegacyActivityError, "root_node"):
                fixture.execute(lambda _task: _completed_result(), finalize_run=True)

    def test_completed_attempt_is_recovered_without_rerunning(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            fixture.execute(lambda _task: _completed_result())
            called = False

            def runner(_task: str) -> dict[str, Any]:
                nonlocal called
                called = True
                return _completed_result("duplicate")

            recovered = fixture.execute(runner)

            self.assertFalse(called)
            self.assertTrue(recovered["recovered"])
            self.assertEqual(
                len([e for e in fixture.store.list_events(fixture.run_id) if e.event_type == "attempt.started"]),
                1,
            )

    def test_completed_attempt_replays_only_the_same_canonical_request(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            request_kwargs = {
                "mode": "safe",
                "limits": [1, 2],
                "options": {"dry_run": True},
            }
            fixture.execute(
                lambda _task, **_kwargs: _completed_result(),
                runner_kwargs=request_kwargs,
            )
            called = False

            def should_not_run(_task: str, **_kwargs: Any) -> dict[str, Any]:
                nonlocal called
                called = True
                return _completed_result("duplicate")

            recovered = fixture.execute(
                should_not_run,
                runner_kwargs={
                    "options": {"dry_run": True},
                    "limits": [1, 2],
                    "mode": "safe",
                },
            )

            self.assertFalse(called)
            self.assertTrue(recovered["recovered"])

    def test_completed_attempt_rejects_a_different_request(self) -> None:
        cases = (
            ("task", "different task", {"mode": "safe"}),
            ("runner_kwargs", "do work", {"mode": "unsafe"}),
        )
        for name, task, runner_kwargs in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as raw:
                fixture = StoreFixture(Path(raw))
                fixture.execute(
                    lambda _task, **_kwargs: _completed_result(),
                    runner_kwargs={"mode": "safe"},
                )
                called = False

                def should_not_run(
                    _task: str,
                    **_kwargs: Any,
                ) -> dict[str, Any]:
                    nonlocal called
                    called = True
                    return _completed_result("wrong request")

                adapter = LegacyAgentLoopAdapter(fixture.store, should_not_run)
                with self.assertRaises(ActivityClaimConflict):
                    adapter.run(
                        fixture.run_id,
                        task,
                        node_id=fixture.node_id,
                        attempt_id=fixture.attempt_id,
                        runner_kwargs=runner_kwargs,
                    )

                self.assertFalse(called)

    def test_non_json_request_is_rejected_before_claiming(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            called = False

            def should_not_run(_task: str, **_kwargs: Any) -> dict[str, Any]:
                nonlocal called
                called = True
                return _completed_result()

            with self.assertRaisesRegex(LegacyActivityError, "canonical JSON"):
                fixture.execute(
                    should_not_run,
                    runner_kwargs={"path": Path(raw)},
                )

            self.assertFalse(called)
            attempt = fixture.store.get_attempt(fixture.attempt_id)
            self.assertEqual(attempt.status, AttemptStatus.SCHEDULED)
            self.assertIsNone(
                fixture.store.get_idempotency(
                    fixture.run_id,
                    attempt.idempotency_key,
                )
            )

    def test_non_idempotent_abnormal_exit_enters_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))

            with self.assertRaises(ActivityRecoveryRequired):
                fixture.execute(
                    lambda _task: {
                        "response": "partial",
                        "exit_reason": "MAX_TURNS_EXCEEDED",
                        "tool_results": [],
                        "turns": 40,
                    }
                )

            self.assertEqual(
                fixture.store.get_run(fixture.run_id).status,
                RunStatus.WAITING_RECOVERY,
            )
            self.assertEqual(
                fixture.store.get_node(fixture.run_id, fixture.node_id).status,
                NodeStatus.WAITING_RECOVERY,
            )
            self.assertEqual(
                fixture.store.get_attempt(fixture.attempt_id).status,
                AttemptStatus.OUTCOME_UNKNOWN,
            )

            called = False

            def should_not_run(_task: str) -> dict[str, Any]:
                nonlocal called
                called = True
                return _completed_result()

            with self.assertRaises(ActivityRecoveryRequired):
                fixture.execute(should_not_run)
            self.assertFalse(called)

    def test_non_idempotent_runner_exception_enters_recovery_and_reraises(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            expected = LookupError("sensitive detail")

            def runner(_task: str) -> dict[str, Any]:
                raise expected

            with self.assertRaises(LookupError) as raised:
                fixture.execute(runner)

            self.assertIs(raised.exception, expected)
            attempt = fixture.store.get_attempt(fixture.attempt_id)
            self.assertEqual(attempt.status, AttemptStatus.OUTCOME_UNKNOWN)
            event = fixture.store.list_events(fixture.run_id)[-1]
            self.assertNotIn("sensitive detail", str(event.payload))

    def test_interrupted_non_idempotent_attempt_is_conservatively_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))

            with self.assertRaises(ActivityRecoveryRequired):
                fixture.execute(
                    lambda _task: {
                        "response": "",
                        "exit_reason": "INTERRUPTED",
                        "tool_results": [],
                        "turns": 1,
                    }
                )

            self.assertEqual(
                fixture.store.get_attempt(fixture.attempt_id).status,
                AttemptStatus.OUTCOME_UNKNOWN,
            )

    def test_unknown_exit_reason_is_not_written_to_event_store(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            untrusted_reason = "secret-" * 10_000

            with self.assertRaises(ActivityRecoveryRequired):
                fixture.execute(
                    lambda _task: {
                        "response": "",
                        "exit_reason": untrusted_reason,
                        "tool_results": [],
                        "turns": 1,
                    }
                )

            payload = fixture.store.list_events(fixture.run_id)[-1].payload
            self.assertEqual(payload["error_code"], "legacy_exit_unknown")
            self.assertNotIn("secret-", str(payload))

    def test_read_only_known_failure_is_not_outcome_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw), effect_class="read_only")

            result = fixture.execute(
                lambda _task: {
                    "response": "",
                    "exit_reason": "MAX_TURNS_EXCEEDED",
                    "tool_results": [],
                    "turns": 40,
                }
            )

            self.assertEqual(result["exit_reason"], "MAX_TURNS_EXCEEDED")
            self.assertEqual(
                fixture.store.get_attempt(fixture.attempt_id).status,
                AttemptStatus.FAILED,
            )
            self.assertEqual(
                fixture.store.get_node(fixture.run_id, fixture.node_id).status,
                NodeStatus.FAILED,
            )

    def test_large_result_requires_writer_and_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))

            with self.assertRaises(DurabilityError) as raised:
                fixture.execute(
                    lambda _task: _completed_result("x" * 20_000),
                    adapter_kwargs={
                        "inline_result_limit": 512,
                        "result_writer": None,
                    },
                )

            self.assertIsInstance(raised.exception.cause, ResultPersistenceError)
            self.assertEqual(
                fixture.store.get_attempt(fixture.attempt_id).status,
                AttemptStatus.OUTCOME_UNKNOWN,
            )

    def test_concurrent_legacy_claims_obey_atomic_resource_admission(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            first = StoreFixture(
                root,
                run_id="run-a",
                node_id="agent-a",
                attempt_id="attempt-a",
                resource_keys=("workspace:shared",),
            )
            second = StoreFixture(
                root,
                run_id="run-b",
                node_id="agent-b",
                attempt_id="attempt-b",
                resource_keys=("workspace:shared",),
            )
            runner_started = threading.Event()
            release_runner = threading.Event()
            worker_errors: list[BaseException] = []

            def blocking_runner(_task: str) -> dict[str, Any]:
                runner_started.set()
                if not release_runner.wait(timeout=5):
                    raise TimeoutError("test did not release the first runner")
                return _completed_result("first")

            def run_first() -> None:
                try:
                    first.execute(
                        blocking_runner,
                        owner_id="worker-a",
                        adapter_kwargs={
                            "max_active_attempts": 8,
                            "worker_capacity": 1,
                        },
                    )
                except BaseException as exc:
                    worker_errors.append(exc)

            thread = threading.Thread(target=run_first)
            thread.start()
            self.assertTrue(runner_started.wait(timeout=5))
            try:
                with self.assertRaises(DurabilityError) as raised:
                    second.execute(
                        lambda _task: _completed_result("second"),
                        owner_id="worker-b",
                        adapter_kwargs={
                            "max_active_attempts": 8,
                            "worker_capacity": 1,
                        },
                    )
                self.assertIsInstance(
                    raised.exception.cause,
                    ActivityAdmissionDenied,
                )
                self.assertEqual(
                    raised.exception.cause.reason_code,
                    "resource_conflict",
                )
                self.assertEqual(
                    second.store.get_attempt(second.attempt_id).status,
                    AttemptStatus.SCHEDULED,
                )
            finally:
                release_runner.set()
                thread.join(timeout=5)

            self.assertFalse(thread.is_alive())
            self.assertEqual(worker_errors, [])
            self.assertEqual(
                first.store.get_attempt(first.attempt_id).status,
                AttemptStatus.SUCCEEDED,
            )

    def test_legacy_claim_inherits_global_and_worker_capacity(self) -> None:
        cases = (
            ("global_capacity", "worker-a", "worker-b", 1, 2),
            ("worker_capacity", "worker-a", "worker-a", 8, 1),
        )
        for reason, first_owner, second_owner, maximum, capacity in cases:
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as raw:
                root = Path(raw)
                first = StoreFixture(
                    root,
                    run_id="run-a",
                    node_id="agent-a",
                    attempt_id="attempt-a",
                    resource_keys=("workspace:a",),
                )
                second = StoreFixture(
                    root,
                    run_id="run-b",
                    node_id="agent-b",
                    attempt_id="attempt-b",
                    resource_keys=("workspace:b",),
                )
                first_claim, _event = first.store.claim_activity(
                    first.run_id,
                    first.node_id,
                    first.attempt_id,
                    "request-a",
                    first_owner,
                    max_active_attempts=maximum,
                    worker_capacity=capacity,
                )
                first.store.start_activity(
                    first.run_id,
                    first.node_id,
                    first.attempt_id,
                    first_owner,
                    claim_token=first_claim.record.claim_token,
                )

                with self.assertRaises(DurabilityError) as raised:
                    second.execute(
                        lambda _task: _completed_result("must-not-run"),
                        owner_id=second_owner,
                        adapter_kwargs={
                            "max_active_attempts": maximum,
                            "worker_capacity": capacity,
                        },
                    )

                self.assertIsInstance(
                    raised.exception.cause,
                    ActivityAdmissionDenied,
                )
                self.assertEqual(raised.exception.cause.reason_code, reason)
                self.assertEqual(
                    second.store.get_attempt(second.attempt_id).status,
                    AttemptStatus.SCHEDULED,
                )

    def test_result_writer_requires_bound_artifact_verifier(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            writer = JsonArtifactResultWriter(
                fixture.artifact_store,
                producer_run_id=fixture.run_id,
                producer_node_id=fixture.node_id,
                producer_attempt_id=fixture.attempt_id,
            )

            with self.assertRaises(DurabilityError) as raised:
                fixture.execute(
                    lambda _task: _completed_result(),
                    adapter_kwargs={
                        "result_writer": writer,
                        "artifact_verifier": None,
                    },
                )

            self.assertIsInstance(
                raised.exception.cause,
                ResultPersistenceError,
            )
            self.assertEqual(
                fixture.store.get_attempt(fixture.attempt_id).status,
                AttemptStatus.OUTCOME_UNKNOWN,
            )

    def test_result_writer_rejects_forged_artifact_reference(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            real_ref = fixture.artifact_store.put_bytes(
                b"real-content",
                producer_run_id=fixture.run_id,
                producer_node_id=fixture.node_id,
                producer_attempt_id=fixture.attempt_id,
            )
            forged_ref = real_ref.to_dict()
            forged_ref["sha256"] = "f" * 64

            with self.assertRaises(DurabilityError) as raised:
                fixture.execute(
                    lambda _task: _completed_result(),
                    adapter_kwargs={
                        "result_writer": lambda _result: forged_ref,
                    },
                )

            self.assertIsInstance(
                raised.exception.cause,
                ResultPersistenceError,
            )
            self.assertEqual(
                fixture.store.get_attempt(fixture.attempt_id).status,
                AttemptStatus.OUTCOME_UNKNOWN,
            )
            self.assertNotIn(
                forged_ref["sha256"],
                str(fixture.store.list_events(fixture.run_id)[-1].payload),
            )

    def test_result_writer_rejects_incomplete_artifact_schema(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            incomplete_ref = {
                "artifact_id": "artifact-fake",
                "kind": "generic",
                "uri": "sha256/aa/bb/" + ("a" * 64),
                "sha256": "a" * 64,
                "size": 1,
            }

            with self.assertRaises(DurabilityError) as raised:
                fixture.execute(
                    lambda _task: _completed_result(),
                    adapter_kwargs={
                        "result_writer": lambda _result: incomplete_ref,
                    },
                )

            self.assertIsInstance(
                raised.exception.cause,
                ResultPersistenceError,
            )
            self.assertEqual(
                fixture.store.get_attempt(fixture.attempt_id).status,
                AttemptStatus.OUTCOME_UNKNOWN,
            )

    def test_large_result_writer_persists_only_artifact_reference(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            seen: list[dict[str, Any]] = []
            written_refs = []

            def writer(result: dict[str, Any]) -> dict[str, Any]:
                seen.append(result)
                ref = fixture.artifact_store.put_json(
                    result,
                    producer_run_id=fixture.run_id,
                    producer_node_id=fixture.node_id,
                    producer_attempt_id=fixture.attempt_id,
                )
                written_refs.append(ref)
                return ref.to_dict()

            result = fixture.execute(
                lambda _task: _completed_result("x" * 20_000),
                adapter_kwargs={
                    "inline_result_limit": 512,
                    "result_writer": writer,
                },
            )

            self.assertEqual(len(seen), 1)
            self.assertEqual(result["response"], "x" * 20_000)
            attempt = fixture.store.get_attempt(fixture.attempt_id)
            self.assertIsNone(attempt.result["output"])
            self.assertEqual(
                attempt.result["artifact_refs"][0]["artifact_id"],
                written_refs[0].artifact_id,
            )
            self.assertNotIn(
                "x" * 100,
                str(fixture.store.list_events(fixture.run_id)[-1].payload),
            )

            called = False

            def should_not_run(_task: str) -> dict[str, Any]:
                nonlocal called
                called = True
                return _completed_result()

            recovered = fixture.execute(should_not_run)
            self.assertFalse(called)
            self.assertTrue(recovered["recovered"])
            self.assertEqual(
                recovered["durable_node_result"]["artifact_refs"][0]["artifact_id"],
                attempt.result["artifact_refs"][0]["artifact_id"],
            )

    def test_terminal_commit_failure_enters_waiting_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            injected = False

            def fail_after_receipt(stage: str) -> None:
                nonlocal injected
                if stage == "complete.after_idempotency" and not injected:
                    injected = True
                    raise OSError("injected commit failure")

            fixture.store._fault = fail_after_receipt

            with self.assertRaises(DurabilityError):
                fixture.execute(lambda _task: _completed_result())

            self.assertEqual(
                fixture.store.get_run(fixture.run_id).status,
                RunStatus.WAITING_RECOVERY,
            )
            self.assertEqual(
                fixture.store.get_node(fixture.run_id, fixture.node_id).status,
                NodeStatus.WAITING_RECOVERY,
            )
            self.assertEqual(
                fixture.store.get_attempt(fixture.attempt_id).status,
                AttemptStatus.OUTCOME_UNKNOWN,
            )
            called = False

            def should_not_run(_task: str) -> dict[str, Any]:
                nonlocal called
                called = True
                return _completed_result()

            with self.assertRaises(ActivityRecoveryRequired):
                fixture.execute(should_not_run)
            self.assertFalse(called)

    def test_running_attempt_is_never_replayed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            adapter = LegacyAgentLoopAdapter(
                fixture.store,
                lambda _task: _completed_result(),
            )
            run = fixture.store.get_run(fixture.run_id)
            node = fixture.store.get_node(fixture.run_id, fixture.node_id)
            attempt = fixture.store.get_attempt(fixture.attempt_id)
            fixture.store.append_event(
                fixture.run_id,
                "attempt.claimed",
                node_id=fixture.node_id,
                attempt_id=fixture.attempt_id,
                attempt_projection=replace(
                    attempt,
                    status=AttemptStatus.CLAIMED,
                    worker_id="worker",
                    lease_id="lease",
                    fencing_token=1,
                ),
                expected_run_version=run.projection_version,
                expected_attempt_version=attempt.projection_version,
            )
            run = fixture.store.get_run(fixture.run_id)
            node = fixture.store.get_node(fixture.run_id, fixture.node_id)
            attempt = fixture.store.get_attempt(fixture.attempt_id)
            fixture.store.append_event(
                fixture.run_id,
                "attempt.started",
                node_id=fixture.node_id,
                attempt_id=fixture.attempt_id,
                run_projection=replace(run, status=RunStatus.RUNNING),
                node_projection=replace(node, status=NodeStatus.RUNNING),
                attempt_projection=replace(
                    attempt,
                    status=AttemptStatus.RUNNING,
                    worker_id="worker",
                    lease_id="lease",
                    fencing_token=1,
                    started_at=time.time(),
                ),
                expected_run_version=run.projection_version,
                expected_node_version=node.projection_version,
                expected_attempt_version=attempt.projection_version,
            )

            with self.assertRaises(ActivityClaimConflict):
                adapter.run(
                    fixture.run_id,
                    "do work",
                    node_id=fixture.node_id,
                    attempt_id=fixture.attempt_id,
                )


if __name__ == "__main__":
    unittest.main()
