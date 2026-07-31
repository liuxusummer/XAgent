from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import unittest
from pathlib import Path

from src.orchestration.agent_activity_executor import (
    AgentActivityExecutionError,
    DurableAgentActivityExecutor,
)
from src.orchestration.agent_provider_client import (
    DurableAgentProviderClient,
)
from src.orchestration.agent_tool_handler import AgentToolSpec
from src.orchestration.artifacts import (
    ArtifactRef,
    ArtifactSensitivity,
    LocalArtifactStore,
)
from src.orchestration.models import AttemptStatus, RunStatus
from src.orchestration.provider_access import (
    ProviderAccessBroker,
    ProviderRouteDescriptor,
)
from src.orchestration.remote_execution_journal import (
    RemoteExecutionJournal,
)
from src.orchestration.scheduler import DurableScheduler, RunInputReceipt
from src.orchestration.store import DurableRunStore
from src.orchestration.worker_security import WorkerAuthorization
from src.orchestration.workflow import compile_workflow


_LEAK = "provider-secret-that-must-not-reach-durable-state"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _response_bytes(content: str = "verified final answer") -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "kind": "agent_provider_response",
            "thinking": "",
            "content": content,
            "tool_calls": [],
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 10,
                "output_tokens": 4,
                "total_tokens": 14,
                "cache_creation_input_tokens": None,
                "cache_read_input_tokens": None,
                "reasoning_tokens": None,
            },
        },
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class _Ids:
    def __init__(self) -> None:
        self.value = 0

    def __call__(self, prefix: str) -> str:
        self.value += 1
        return f"{prefix}-agent-executor-{self.value}"


class _Verifier:
    def verify(
        self,
        authorization: WorkerAuthorization,
        *,
        now: float,
    ) -> bool:
        return (
            type(authorization) is WorkerAuthorization
            and authorization.authorization_id.startswith("auth-")
            and now < authorization.expires_at
        )


class _Invoker:
    def __init__(self, response: bytes) -> None:
        self.response = response
        self.calls = 0

    def invoke(self, route, payload: bytes, *, request_id: str) -> bytes:
        self.calls += 1
        return self.response


class _FailingInvoker(_Invoker):
    def invoke(self, route, payload: bytes, *, request_id: str) -> bytes:
        self.calls += 1
        raise RuntimeError(_LEAK)


class _BlockingInvoker(_Invoker):
    def __init__(self, response: bytes) -> None:
        super().__init__(response)
        self.entered = threading.Event()
        self.release = threading.Event()

    def invoke(self, route, payload: bytes, *, request_id: str) -> bytes:
        self.calls += 1
        self.entered.set()
        if not self.release.wait(timeout=3.0):
            raise TimeoutError("blocking provider test timed out")
        return self.response


class _UnusedToolExecutor:
    durable_result_recovery_ready = True

    def execute(self, request):
        raise AssertionError("the provider did not request a Tool")


class _Fixture:
    def __init__(
        self,
        root: Path,
        *,
        invoker: _Invoker,
        heartbeat_interval_seconds: float = 20.0,
        lease_renewal_seconds: float = 60.0,
        input_sensitivity: ArtifactSensitivity | None = None,
    ) -> None:
        self.root = root
        self.clock = _Clock()
        self.ids = _Ids()
        self.store = DurableRunStore(root / "domain.sqlite3")
        self.artifacts = LocalArtifactStore(root / "artifacts")
        node = {
            "id": "agent",
            "kind": "agent",
            "effect_class": "read_only",
            "config": {
                "agent": "main",
                "task": "perform the verified task",
            },
        }
        if input_sensitivity is not None:
            node["input_mapping"] = {
                "evidence": {"source": "run_input"}
            }
        self._input_sensitivity = input_sensitivity
        self.workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": "durable-agent-executor",
                "version": 1,
                "nodes": [node],
            }
        )
        self.scheduler = DurableScheduler(
            self.store,
            self.workflow,
            clock=self.clock,
            id_factory=self.ids,
            input_writer=(
                self._input_writer
                if input_sensitivity is not None
                else None
            ),
            artifact_verifier=self.artifacts.verify,
        )
        self.route = ProviderRouteDescriptor(
            route_id="primary-route",
            tenant_id="tenant-1",
            pool_id="pool-1",
            worker_rule_id="agent-rule",
            provider="openai-compatible",
            model="model-deployment",
            gateway_binding_digest=_digest("gateway"),
            maximum_request_bytes=1024 * 1024,
            maximum_response_bytes=1024 * 1024,
        )
        self.invoker = invoker
        self.broker = ProviderAccessBroker(
            (self.route,),
            authorization_verifier=_Verifier(),
            invoker=invoker,
            clock=self.clock,
            recovery_journal=RemoteExecutionJournal(
                root / "provider.sqlite3"
            ),
            result_store=self.artifacts,
        )
        self.executor = DurableAgentActivityExecutor(
            self.scheduler,
            self.artifacts,
            provider_client_factory=self._provider_client,
            tool_executor=_UnusedToolExecutor(),
            tool_specs=(AgentToolSpec("echo"),),
            tools_schema=(
                {
                    "name": "echo",
                    "description": "Echo a value.",
                    "input_schema": {"type": "object"},
                },
            ),
            system_prompt_source=lambda _request: (
                "Complete the task and return the final answer."
            ),
            max_turns=3,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            lease_renewal_seconds=lease_renewal_seconds,
        )
        self.scheduler.create_run(
            "run-1",
            input=(
                {"evidence": "classified input"}
                if input_sensitivity is not None
                else None
            ),
        )
        self.scheduler.reconcile("run-1")

    def _input_writer(self, run_id: str, value) -> RunInputReceipt:
        assert self._input_sensitivity is not None
        ref = self.artifacts.put_json(
            value,
            sensitivity=self._input_sensitivity,
            producer_run_id=run_id,
        )
        return RunInputReceipt((ref,))

    def _authorization(self, request) -> WorkerAuthorization:
        return WorkerAuthorization(
            authorization_id="auth-1",
            worker_id="worker-1",
            tenant_id=self.route.tenant_id,
            pool_id=self.route.pool_id,
            run_id=request.run_id,
            node_id=request.node_id,
            attempt_id=request.attempt_id,
            action_digest=_digest("agent-action"),
            identity_binding_digest=_digest("identity"),
            transport_binding_digest=_digest("transport"),
            rule_id=self.route.worker_rule_id,
            maximum_artifact_sensitivity=(
                ArtifactSensitivity.SENSITIVE
            ),
            issued_at=90.0,
            expires_at=200.0,
        )

    def _provider_client(self, request, request_ref, collector):
        return DurableAgentProviderClient(
            broker=self.broker,
            authorization_source=lambda: self._authorization(request),
            request=request,
            request_ref=request_ref,
            collector=collector,
            route_id=self.route.route_id,
            grant_ttl_seconds=30.0,
        )

    def claim(self):
        candidate = self.scheduler.prepare_next_admission(
            "run-1",
            "worker-1",
        )
        assert candidate is not None
        request_ref = self.executor.prepare(candidate)
        claim, _event = self.scheduler.claim_admitted(
            candidate,
            lease_seconds=60.0,
            capacity=1,
            admission_expires_at=200.0,
            policy_binding={
                "outcome": "allow",
                "reason_code": "agent_executor_test",
                "action_digest": _digest("policy-action"),
                "policy_digest": _digest("policy"),
                "profile_digest": _digest("profile"),
                "decision_digest": _digest("decision"),
                "approval_grant_digest": None,
            },
        )
        assert claim is not None
        return claim, request_ref

    def durable_database_bytes(self) -> bytes:
        values = []
        for database in (
            self.root / "domain.sqlite3",
            self.root / "provider.sqlite3",
        ):
            for path in (
                database,
                Path(f"{database}-wal"),
                Path(f"{database}-shm"),
            ):
                if path.exists():
                    values.append(path.read_bytes())
        return b"".join(values)


class DurableAgentActivityExecutorTests(unittest.TestCase):
    def test_success_commits_payload_free_verified_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_Invoker(_response_bytes()),
            )
            claim, request_ref = fixture.claim()

            result = fixture.executor.execute(claim, request_ref)

            attempt = fixture.store.get_attempt(claim.attempt_id)
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertIs(attempt.status, AttemptStatus.SUCCEEDED)
            self.assertIs(
                fixture.store.get_run(claim.run_id).status,
                RunStatus.COMPLETED,
            )
            self.assertEqual(fixture.invoker.calls, 1)
            self.assertTrue(
                fixture.artifacts.verify(result.result_artifact_ref)
            )
            payload = json.loads(
                fixture.artifacts.read(
                    result.result_artifact_ref
                ).decode("utf-8")
            )
            self.assertEqual(payload["response"], "verified final answer")
            self.assertEqual(payload["exit_reason"], "CURRENT_TASK_DONE")
            self.assertEqual(
                result.terminal.receipt.result_artifact_digests[0],
                result.result_artifact_ref.sha256,
            )
            self.assertEqual(
                len(result.terminal.receipt.result_artifact_digests),
                3,
            )
            self.assertNotIn("verified final answer", repr(result))
            self.assertNotIn(
                b"verified final answer",
                fixture.durable_database_bytes(),
            )
            terminal_events = [
                event
                for event in fixture.store.list_events(claim.run_id)
                if event.attempt_id == claim.attempt_id
                and event.event_type == "attempt.succeeded"
            ]
            self.assertEqual(len(terminal_events), 1)
            self.assertFalse(fixture.executor.durable_result_recovery_ready)
            self.assertFalse(fixture.executor.production_security_ready)

    def test_blocking_provider_call_keeps_parent_lease_alive(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            invoker = _BlockingInvoker(_response_bytes())
            fixture = _Fixture(
                Path(directory),
                invoker=invoker,
                heartbeat_interval_seconds=0.01,
                lease_renewal_seconds=1.0,
            )
            claim, request_ref = fixture.claim()
            renewed_twice = threading.Event()
            renew_count = 0
            renew_lock = threading.Lock()
            original_renew = fixture.scheduler.renew_claim

            def observed_renew(*args, **kwargs):
                nonlocal renew_count
                result = original_renew(*args, **kwargs)
                with renew_lock:
                    renew_count += 1
                    if renew_count >= 2:
                        renewed_twice.set()
                return result

            fixture.scheduler.renew_claim = observed_renew
            results = []
            failures = []

            def execute() -> None:
                try:
                    results.append(
                        fixture.executor.execute(claim, request_ref)
                    )
                except BaseException as exc:
                    failures.append(exc)

            thread = threading.Thread(target=execute)
            thread.start()
            try:
                self.assertTrue(invoker.entered.wait(timeout=2.0))
                self.assertTrue(renewed_twice.wait(timeout=2.0))
            finally:
                invoker.release.set()
                thread.join(timeout=3.0)

            self.assertFalse(thread.is_alive())
            self.assertEqual(failures, [])
            self.assertEqual(len(results), 1)
            self.assertGreaterEqual(renew_count, 2)
            attempt = fixture.store.get_attempt(claim.attempt_id)
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertIs(attempt.status, AttemptStatus.SUCCEEDED)

    def test_heartbeat_failure_prevents_false_success(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            invoker = _BlockingInvoker(_response_bytes())
            fixture = _Fixture(
                Path(directory),
                invoker=invoker,
                heartbeat_interval_seconds=0.01,
                lease_renewal_seconds=1.0,
            )
            claim, request_ref = fixture.claim()
            heartbeat_failed = threading.Event()
            renew_count = 0
            original_renew = fixture.scheduler.renew_claim

            def fail_second_renew(*args, **kwargs):
                nonlocal renew_count
                renew_count += 1
                if renew_count >= 2:
                    heartbeat_failed.set()
                    raise RuntimeError(_LEAK)
                return original_renew(*args, **kwargs)

            fixture.scheduler.renew_claim = fail_second_renew
            failures = []

            def execute() -> None:
                try:
                    fixture.executor.execute(claim, request_ref)
                except BaseException as exc:
                    failures.append(exc)

            thread = threading.Thread(target=execute)
            thread.start()
            try:
                self.assertTrue(invoker.entered.wait(timeout=2.0))
                self.assertTrue(heartbeat_failed.wait(timeout=2.0))
            finally:
                invoker.release.set()
                thread.join(timeout=3.0)

            self.assertFalse(thread.is_alive())
            self.assertEqual(len(failures), 1)
            self.assertIs(type(failures[0]), AgentActivityExecutionError)
            self.assertEqual(
                failures[0].reason_code,
                "agent_activity_execution_failed",
            )
            attempt = fixture.store.get_attempt(claim.attempt_id)
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertIs(attempt.status, AttemptStatus.OUTCOME_UNKNOWN)
            self.assertNotIn(_LEAK.encode(), fixture.durable_database_bytes())

    def test_same_attempt_cannot_enter_two_local_loops(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            invoker = _BlockingInvoker(_response_bytes())
            fixture = _Fixture(Path(directory), invoker=invoker)
            claim, request_ref = fixture.claim()
            results = []
            failures = []

            def execute() -> None:
                try:
                    results.append(
                        fixture.executor.execute(claim, request_ref)
                    )
                except BaseException as exc:
                    failures.append(exc)

            thread = threading.Thread(target=execute)
            thread.start()
            try:
                self.assertTrue(invoker.entered.wait(timeout=2.0))
                with self.assertRaises(
                    AgentActivityExecutionError
                ) as raised:
                    fixture.executor.execute(claim, request_ref)
                self.assertEqual(
                    raised.exception.reason_code,
                    "agent_activity_claim_invalid",
                )
            finally:
                invoker.release.set()
                thread.join(timeout=3.0)

            self.assertFalse(thread.is_alive())
            self.assertEqual(failures, [])
            self.assertEqual(len(results), 1)
            self.assertEqual(invoker.calls, 1)

    def test_provider_failure_becomes_outcome_unknown_without_leak(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_FailingInvoker(_response_bytes()),
            )
            claim, request_ref = fixture.claim()

            with self.assertRaises(
                AgentActivityExecutionError
            ) as raised:
                fixture.executor.execute(claim, request_ref)

            self.assertEqual(
                raised.exception.reason_code,
                "agent_activity_execution_failed",
            )
            self.assertIsNone(raised.exception.__cause__)
            self.assertIsNone(raised.exception.__context__)
            attempt = fixture.store.get_attempt(claim.attempt_id)
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertIs(attempt.status, AttemptStatus.OUTCOME_UNKNOWN)
            self.assertEqual(
                attempt.result["error_code"],
                "agent_activity_execution_uncertain",
            )
            self.assertEqual(attempt.result["outcome"], "outcome_unknown")
            self.assertNotIn(_LEAK.encode(), fixture.durable_database_bytes())

    def test_preflight_failure_does_not_start_or_invoke_provider(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_Invoker(_response_bytes()),
            )
            claim, request_ref = fixture.claim()

            def fail_prompt(_request):
                raise RuntimeError(_LEAK)

            fixture.executor._system_prompt_source = fail_prompt

            with self.assertRaises(
                AgentActivityExecutionError
            ) as raised:
                fixture.executor.execute(claim, request_ref)

            self.assertEqual(
                raised.exception.reason_code,
                "agent_activity_preflight_failed",
            )
            self.assertIsNone(raised.exception.__context__)
            self.assertNotIn(_LEAK, repr(raised.exception))
            attempt = fixture.store.get_attempt(claim.attempt_id)
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertIs(attempt.status, AttemptStatus.CLAIMED)
            self.assertEqual(fixture.invoker.calls, 0)

    def test_terminal_failure_after_provider_becomes_outcome_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_Invoker(_response_bytes()),
            )
            claim, request_ref = fixture.claim()

            def fail_terminal(stage: str) -> None:
                if stage == "agent_terminal.artifacts_staged":
                    raise RuntimeError(_LEAK)

            fixture.executor._terminal._fault_hook = fail_terminal
            with self.assertRaises(
                AgentActivityExecutionError
            ) as raised:
                fixture.executor.execute(claim, request_ref)

            self.assertEqual(
                raised.exception.reason_code,
                "agent_activity_terminal_failed",
            )
            self.assertIsNone(raised.exception.__context__)
            attempt = fixture.store.get_attempt(claim.attempt_id)
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertIs(attempt.status, AttemptStatus.OUTCOME_UNKNOWN)
            self.assertEqual(
                attempt.result["error_code"],
                "agent_activity_execution_uncertain",
            )
            self.assertNotIn(_LEAK.encode(), fixture.durable_database_bytes())

    def test_foreign_request_ref_is_rejected_before_start(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_Invoker(_response_bytes()),
            )
            claim, request_ref = fixture.claim()
            foreign = ArtifactRef.from_dict(
                {
                    **request_ref.to_dict(),
                    "producer_attempt_id": "foreign-attempt",
                }
            )

            with self.assertRaises(
                AgentActivityExecutionError
            ) as raised:
                fixture.executor.execute(claim, foreign)

            self.assertEqual(
                raised.exception.reason_code,
                "agent_activity_request_invalid",
            )
            attempt = fixture.store.get_attempt(claim.attempt_id)
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertIs(attempt.status, AttemptStatus.CLAIMED)
            self.assertEqual(fixture.invoker.calls, 0)

    def test_secret_request_is_not_staged_in_local_store(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_Invoker(_response_bytes()),
                input_sensitivity=ArtifactSensitivity.SECRET,
            )
            candidate = fixture.scheduler.prepare_next_admission(
                "run-1",
                "worker-1",
            )
            self.assertIsNotNone(candidate)
            assert candidate is not None
            before = {
                path
                for path in fixture.artifacts.root.rglob("*")
                if path.is_file()
            }

            with self.assertRaises(
                AgentActivityExecutionError
            ) as raised:
                fixture.executor.prepare(candidate)

            self.assertEqual(
                raised.exception.reason_code,
                "agent_activity_request_invalid",
            )
            after = {
                path
                for path in fixture.artifacts.root.rglob("*")
                if path.is_file()
            }
            self.assertEqual(after, before)
            attempt = fixture.store.get_attempt(
                candidate.claim.attempt_id
            )
            self.assertIsNone(attempt)
            self.assertEqual(fixture.invoker.calls, 0)


if __name__ == "__main__":
    unittest.main()
