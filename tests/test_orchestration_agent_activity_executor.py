from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path

from src.core.agent_loop import AgentContext
from src.orchestration.agent_activity_executor import (
    AgentActivityExecutionError,
    DurableAgentActivityExecutor,
)
from src.orchestration.agent_provider_client import (
    DurableAgentProviderClient,
)
from src.orchestration.agent_tool_handler import (
    AgentToolInvocationRequest,
    AgentToolSpec,
)
from src.orchestration.agent_execution_manifest import (
    agent_tool_operation_key,
)
from src.orchestration.agent_turn_checkpoint import (
    AgentTurnCheckpointArtifactStore,
)
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
from src.orchestration.scheduler import (
    DurableScheduler,
    RunInputReceipt,
    SchedulerStateError,
)
from src.orchestration.store import (
    AgentTurnCheckpointConflict,
    AgentTurnTakeoverConflict,
    DurableRunStore,
)
from src.orchestration.worker_security import WorkerAuthorization
from src.orchestration.workflow import compile_workflow
from src.orchestration.policy import canonical_action_args_digest


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


class _SequenceInvoker(_Invoker):
    def __init__(self, responses: list[bytes]) -> None:
        super().__init__(responses[-1])
        self.responses = list(responses)

    def invoke(self, route, payload: bytes, *, request_id: str) -> bytes:
        response = self.responses[self.calls]
        self.calls += 1
        return response


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
        timeout: dict[str, int] | None = None,
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
        if timeout is not None:
            node["timeout"] = timeout
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
        attempt = self.store.get_attempt(request.attempt_id)
        assert attempt is not None and attempt.worker_id is not None
        return WorkerAuthorization(
            authorization_id="auth-1",
            worker_id=attempt.worker_id,
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
            issued_at=max(0.0, self.clock.now - 10.0),
            expires_at=self.clock.now + 100.0,
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


def _leave_safe_turn_checkpoint(
    fixture: _Fixture,
    claim,
    request_ref: ArtifactRef,
) -> None:
    def crash_before_second_provider(message: str) -> None:
        if message == "[Turn 2]":
            raise RuntimeError("simulated process death")

    fixture.executor._context_factory = lambda _request: AgentContext(
        display_fn=crash_before_second_provider
    )
    fixture.executor._mark_uncertain = lambda _claim: None
    try:
        fixture.executor.execute(claim, request_ref)
    except AgentActivityExecutionError:
        return
    raise AssertionError("simulated worker death did not stop execution")


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
            self.assertTrue(fixture.executor.durable_result_recovery_ready)
            self.assertFalse(fixture.executor.production_security_ready)

    def test_safe_turn_checkpoint_resumes_before_next_provider_call(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            invoker = _SequenceInvoker(
                [
                    _response_bytes(""),
                    _response_bytes("resumed final answer"),
                ]
            )
            fixture = _Fixture(Path(directory), invoker=invoker)
            claim, request_ref = fixture.claim()

            _leave_safe_turn_checkpoint(fixture, claim, request_ref)

            checkpoint_record = (
                fixture.store.get_latest_agent_turn_checkpoint(
                    claim.run_id,
                    claim.node_id,
                    claim.attempt_id,
                )
            )
            self.assertIsNotNone(checkpoint_record)
            assert checkpoint_record is not None
            self.assertEqual(checkpoint_record.completed_turn, 1)
            self.assertEqual(invoker.calls, 1)
            self.assertIs(
                fixture.store.get_attempt(claim.attempt_id).status,
                AttemptStatus.RUNNING,
            )
            for index, ref in enumerate(
                (
                    checkpoint_record.checkpoint_ref,
                    *checkpoint_record.dependency_artifact_refs,
                )
            ):
                self.assertFalse(
                    fixture.store.claim_artifact_gc_candidate(
                        ref.sha256,
                        quarantine_id=(
                            f"q{index + 1:020d}_"
                            "0123456789abcdef0123456789abcdef"
                        ),
                        size=ref.size,
                        claimed_at=fixture.clock.now,
                    )
                )

            fixture.executor._context_factory = lambda _request: AgentContext(
                display_fn=lambda _message: None
            )
            restored_claim = fixture.scheduler.restore_claim(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
                claim.worker_id,
                request_hash=claim.request_hash,
                claim_token=claim.claim_token,
                fencing_token=claim.fencing_token,
            )
            result = fixture.executor.resume(
                restored_claim,
                request_ref,
            )

            self.assertEqual(invoker.calls, 2)
            self.assertIs(
                fixture.store.get_attempt(claim.attempt_id).status,
                AttemptStatus.SUCCEEDED,
            )
            payload = json.loads(
                fixture.artifacts.read(
                    result.result_artifact_ref
                ).decode("utf-8")
            )
            self.assertEqual(payload["response"], "resumed final answer")
            self.assertEqual(payload["turns"], 2)

    def test_expired_claim_takeover_resumes_with_new_owner_and_fence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            invoker = _SequenceInvoker(
                [
                    _response_bytes(""),
                    _response_bytes("resumed final answer"),
                ]
            )
            fixture = _Fixture(Path(directory), invoker=invoker)
            claim, request_ref = fixture.claim()
            _leave_safe_turn_checkpoint(fixture, claim, request_ref)
            fixture.clock.now = fixture.store.get_idempotency(
                claim.run_id,
                claim.claim_key,
            ).lease_expires_at

            adopted, event = (
                fixture.scheduler.takeover_expired_agent_claim(
                    claim,
                    "worker-2",
                    takeover_id="recovery-controller-1",
                    lease_seconds=30.0,
                    capacity=1,
                )
            )

            self.assertEqual(adopted.worker_id, "worker-2")
            self.assertEqual(
                adopted.fencing_token,
                claim.fencing_token + 1,
            )
            self.assertNotEqual(adopted.claim_token, claim.claim_token)
            self.assertEqual(event.event_type, "attempt.claim_taken_over")
            self.assertNotIn(claim.claim_token, repr(adopted))
            with self.assertRaises(SchedulerStateError):
                fixture.scheduler.renew_claim(claim)

            fixture.executor._context_factory = (
                lambda _request: AgentContext(
                    display_fn=lambda _message: None
                )
            )
            result = fixture.executor.resume(adopted, request_ref)

            self.assertEqual(invoker.calls, 2)
            self.assertIs(
                fixture.store.get_attempt(claim.attempt_id).status,
                AttemptStatus.SUCCEEDED,
            )
            self.assertEqual(
                json.loads(
                    fixture.artifacts.read(
                        result.result_artifact_ref
                    ).decode("utf-8")
                )["response"],
                "resumed final answer",
            )

    def test_takeover_retry_returns_same_live_claim_and_event(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_SequenceInvoker(
                    [_response_bytes(""), _response_bytes("final")]
                ),
            )
            claim, request_ref = fixture.claim()
            _leave_safe_turn_checkpoint(fixture, claim, request_ref)
            fixture.clock.now = fixture.store.get_idempotency(
                claim.run_id,
                claim.claim_key,
            ).lease_expires_at

            first, first_event = (
                fixture.scheduler.takeover_expired_agent_claim(
                    claim,
                    "worker-2",
                    takeover_id="lost-response-retry",
                )
            )
            second, second_event = (
                fixture.scheduler.takeover_expired_agent_claim(
                    claim,
                    "worker-2",
                    takeover_id="lost-response-retry",
                )
            )

            self.assertEqual(first, second)
            self.assertEqual(first_event, second_event)
            self.assertNotIn(
                b"lost-response-retry",
                fixture.durable_database_bytes(),
            )
            self.assertEqual(
                len(
                    [
                        event
                        for event in fixture.store.list_events(
                            claim.run_id
                        )
                        if event.event_type
                        == "attempt.claim_taken_over"
                    ]
                ),
                1,
            )

    def test_repeated_takeovers_preserve_provider_lineage_segments(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_SequenceInvoker(
                    [
                        _response_bytes(""),
                        _response_bytes(""),
                        _response_bytes("final after two takeovers"),
                    ]
                ),
            )
            original, request_ref = fixture.claim()
            _leave_safe_turn_checkpoint(
                fixture,
                original,
                request_ref,
            )
            first_record = fixture.store.get_idempotency(
                original.run_id,
                original.claim_key,
            )
            assert first_record is not None
            fixture.clock.now = first_record.lease_expires_at
            second, _event = (
                fixture.scheduler.takeover_expired_agent_claim(
                    original,
                    "worker-2",
                    takeover_id="first-hop",
                )
            )

            def crash_before_third_provider(message: str) -> None:
                if message == "[Turn 3]":
                    raise RuntimeError("second worker stopped")

            fixture.executor._context_factory = (
                lambda _request: AgentContext(
                    display_fn=crash_before_third_provider
                )
            )
            with self.assertRaises(AgentActivityExecutionError):
                fixture.executor.resume(second, request_ref)

            second_checkpoint = (
                fixture.store.get_latest_agent_turn_checkpoint(
                    original.run_id,
                    original.node_id,
                    original.attempt_id,
                )
            )
            assert second_checkpoint is not None
            self.assertEqual(second_checkpoint.completed_turn, 2)
            self.assertEqual(
                second_checkpoint.fencing_token,
                second.fencing_token,
            )
            checkpoint_payload = AgentTurnCheckpointArtifactStore(
                fixture.artifacts
            ).load(second_checkpoint.checkpoint_ref)
            self.assertEqual(
                checkpoint_payload.provider_state[
                    "authorization_lineage_start_index"
                ],
                1,
            )
            authorization_digests = [
                binding.receipt.authorization_digest
                for binding in checkpoint_payload.evidence_manifest.provider_receipts
            ]
            self.assertEqual(len(set(authorization_digests)), 2)

            second_record = fixture.store.get_idempotency(
                second.run_id,
                second.claim_key,
            )
            assert second_record is not None
            fixture.clock.now = second_record.lease_expires_at
            third, _event = (
                fixture.scheduler.takeover_expired_agent_claim(
                    second,
                    "worker-3",
                    takeover_id="second-hop",
                )
            )
            fixture.executor._context_factory = (
                lambda _request: AgentContext(
                    display_fn=lambda _message: None
                )
            )

            result = fixture.executor.resume(third, request_ref)

            self.assertEqual(fixture.invoker.calls, 3)
            self.assertEqual(
                json.loads(
                    fixture.artifacts.read(
                        result.result_artifact_ref
                    ).decode("utf-8")
                )["response"],
                "final after two takeovers",
            )

    def test_live_or_checkpointless_claim_cannot_be_taken_over(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_SequenceInvoker(
                    [_response_bytes(""), _response_bytes("final")]
                ),
            )
            claim, request_ref = fixture.claim()
            _leave_safe_turn_checkpoint(fixture, claim, request_ref)
            with self.assertRaisesRegex(
                AgentTurnTakeoverConflict,
                "agent_turn_takeover_not_expired",
            ):
                fixture.scheduler.takeover_expired_agent_claim(
                    claim,
                    "worker-2",
                    takeover_id="too-early",
                )

        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_Invoker(_response_bytes()),
            )
            claim, _request_ref = fixture.claim()
            fixture.scheduler.start_claim(claim)
            fixture.clock.now = claim.lease_expires_at
            with self.assertRaisesRegex(
                AgentTurnTakeoverConflict,
                "agent_turn_takeover_checkpoint_unavailable",
            ):
                fixture.scheduler.takeover_expired_agent_claim(
                    claim,
                    "worker-2",
                    takeover_id="no-checkpoint",
                )

    def test_elapsed_execution_deadline_blocks_checkpoint_takeover(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_SequenceInvoker(
                    [_response_bytes(""), _response_bytes("final")]
                ),
                timeout={"execution_timeout_ms": 30_000},
            )
            claim, request_ref = fixture.claim()
            _leave_safe_turn_checkpoint(fixture, claim, request_ref)
            record = fixture.store.get_idempotency(
                claim.run_id,
                claim.claim_key,
            )
            assert record is not None
            fixture.clock.now = record.lease_expires_at

            with self.assertRaisesRegex(
                AgentTurnTakeoverConflict,
                "agent_turn_takeover_deadline_elapsed",
            ):
                fixture.scheduler.takeover_expired_agent_claim(
                    claim,
                    "worker-2",
                    takeover_id="deadline-wins",
                )

    def test_active_tool_tail_blocks_checkpoint_takeover(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_SequenceInvoker(
                    [_response_bytes(""), _response_bytes("final")]
                ),
            )
            claim, request_ref = fixture.claim()
            _leave_safe_turn_checkpoint(fixture, claim, request_ref)
            args = {"value": "tail"}
            operation_key = agent_tool_operation_key(
                run_id=claim.run_id,
                node_id=claim.node_id,
                attempt_id=claim.attempt_id,
                request_digest=claim.request_hash,
                sequence=1,
            )
            child = AgentToolInvocationRequest(
                run_id=claim.run_id,
                node_id=claim.node_id,
                attempt_id=claim.attempt_id,
                request_digest=claim.request_hash,
                request_artifact_digest=request_ref.sha256,
                sequence=1,
                turn=2,
                tool_name="echo",
                tool_call_id_digest=_digest("tail-call"),
                args_digest=canonical_action_args_digest(
                    args,
                    sensitive_keys=(),
                ),
                sensitivity=request_ref.sensitivity,
                sensitive_keys=(),
                operation_key=operation_key,
                args=args,
            )
            fixture.store.reserve_agent_tool_invocation(
                child,
                now=fixture.clock.now,
            )
            record = fixture.store.get_idempotency(
                claim.run_id,
                claim.claim_key,
            )
            assert record is not None
            fixture.clock.now = record.lease_expires_at

            with self.assertRaisesRegex(
                AgentTurnTakeoverConflict,
                "agent_turn_takeover_child_active",
            ):
                fixture.scheduler.takeover_expired_agent_claim(
                    claim,
                    "worker-2",
                    takeover_id="child-tail",
                )

    def test_concurrent_takeovers_have_one_winner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_SequenceInvoker(
                    [_response_bytes(""), _response_bytes("final")]
                ),
            )
            claim, request_ref = fixture.claim()
            _leave_safe_turn_checkpoint(fixture, claim, request_ref)
            record = fixture.store.get_idempotency(
                claim.run_id,
                claim.claim_key,
            )
            assert record is not None
            fixture.clock.now = record.lease_expires_at
            barrier = threading.Barrier(2)
            winners = []
            failures = []

            def compete(index: int) -> None:
                barrier.wait()
                try:
                    winners.append(
                        fixture.scheduler.takeover_expired_agent_claim(
                            claim,
                            f"worker-{index + 2}",
                            takeover_id=f"race-{index}",
                        )[0]
                    )
                except AgentTurnTakeoverConflict as exc:
                    failures.append(exc.reason_code)

            threads = [
                threading.Thread(target=compete, args=(index,))
                for index in range(2)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5.0)

            self.assertTrue(all(not thread.is_alive() for thread in threads))
            self.assertEqual(len(winners), 1)
            self.assertEqual(len(failures), 1)
            self.assertEqual(
                winners[0].fencing_token,
                claim.fencing_token + 1,
            )
            self.assertEqual(
                len(
                    [
                        event
                        for event in fixture.store.list_events(
                            claim.run_id
                        )
                        if event.event_type
                        == "attempt.claim_taken_over"
                    ]
                ),
                1,
            )

    def test_takeover_fault_rolls_back_claim_and_event(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_SequenceInvoker(
                    [_response_bytes(""), _response_bytes("final")]
                ),
            )
            claim, request_ref = fixture.claim()
            _leave_safe_turn_checkpoint(fixture, claim, request_ref)
            record = fixture.store.get_idempotency(
                claim.run_id,
                claim.claim_key,
            )
            assert record is not None
            fixture.clock.now = record.lease_expires_at

            def fail(stage: str) -> None:
                if stage == "agent_turn_takeover.after_idempotency":
                    raise RuntimeError("injected takeover fault")

            fixture.store._fault = fail
            with self.assertRaisesRegex(
                RuntimeError,
                "injected takeover fault",
            ):
                fixture.scheduler.takeover_expired_agent_claim(
                    claim,
                    "worker-2",
                    takeover_id="rollback",
                )
            fixture.store._fault = lambda _stage: None

            stored_attempt = fixture.store.get_attempt(
                claim.attempt_id
            )
            stored_claim = fixture.store.get_idempotency(
                claim.run_id,
                claim.claim_key,
            )
            assert stored_attempt is not None and stored_claim is not None
            self.assertEqual(stored_attempt.worker_id, claim.worker_id)
            self.assertEqual(stored_attempt.lease_id, claim.claim_token)
            self.assertEqual(stored_claim.owner_id, claim.worker_id)
            self.assertEqual(stored_claim.claim_token, claim.claim_token)
            self.assertFalse(
                any(
                    event.event_type == "attempt.claim_taken_over"
                    for event in fixture.store.list_events(
                        claim.run_id
                    )
                )
            )

    def test_tampered_takeover_binding_cannot_resume(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_SequenceInvoker(
                    [_response_bytes(""), _response_bytes("final")]
                ),
            )
            claim, request_ref = fixture.claim()
            _leave_safe_turn_checkpoint(fixture, claim, request_ref)
            record = fixture.store.get_idempotency(
                claim.run_id,
                claim.claim_key,
            )
            assert record is not None
            fixture.clock.now = record.lease_expires_at
            adopted, _event = (
                fixture.scheduler.takeover_expired_agent_claim(
                    claim,
                    "worker-2",
                    takeover_id="tamper-test",
                )
            )
            attempt = fixture.store.get_attempt(claim.attempt_id)
            assert attempt is not None
            metadata = dict(attempt.metadata)
            takeover = dict(metadata["agent_turn_takeover"])
            takeover["binding_digest"] = "0" * 64
            metadata["agent_turn_takeover"] = takeover
            with closing(sqlite3.connect(fixture.store.path)) as conn:
                conn.execute(
                    "UPDATE attempts SET metadata_json = ? "
                    "WHERE attempt_id = ?",
                    (
                        json.dumps(
                            metadata,
                            ensure_ascii=False,
                            allow_nan=False,
                            separators=(",", ":"),
                            sort_keys=True,
                        ),
                        claim.attempt_id,
                    ),
                )
                conn.commit()

            with self.assertRaisesRegex(
                AgentActivityExecutionError,
                "agent_activity_recovery_unavailable",
            ):
                fixture.executor.resume(adopted, request_ref)

            self.assertEqual(fixture.invoker.calls, 1)

    def test_resume_rejects_runtime_configuration_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            invoker = _SequenceInvoker(
                [_response_bytes(""), _response_bytes("final")]
            )
            fixture = _Fixture(Path(directory), invoker=invoker)
            claim, request_ref = fixture.claim()
            _leave_safe_turn_checkpoint(fixture, claim, request_ref)
            fixture.executor._context_factory = lambda _request: AgentContext(
                display_fn=lambda _message: None
            )
            fixture.executor._max_turns = 2

            with self.assertRaisesRegex(
                AgentActivityExecutionError,
                "agent_activity_preflight_failed",
            ):
                fixture.executor.resume(claim, request_ref)

            self.assertEqual(invoker.calls, 1)
            self.assertIs(
                fixture.store.get_attempt(claim.attempt_id).status,
                AttemptStatus.RUNNING,
            )

    def test_checkpoint_same_turn_is_append_only_and_claim_fenced(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            invoker = _SequenceInvoker(
                [_response_bytes(""), _response_bytes("final")]
            )
            fixture = _Fixture(Path(directory), invoker=invoker)
            claim, request_ref = fixture.claim()
            _leave_safe_turn_checkpoint(fixture, claim, request_ref)
            record = fixture.store.get_latest_agent_turn_checkpoint(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
            )
            assert record is not None
            checkpoint_store = AgentTurnCheckpointArtifactStore(
                fixture.artifacts
            )
            checkpoint = checkpoint_store.load(record.checkpoint_ref)
            changed_state = dict(checkpoint.loop_state)
            changed_state["final_response"] = "alternate"
            alternate = replace(checkpoint, loop_state=changed_state)
            alternate_ref = checkpoint_store.stage(alternate)

            with self.assertRaisesRegex(
                AgentTurnCheckpointConflict,
                "agent_turn_checkpoint_state_conflict",
            ):
                fixture.store.commit_agent_turn_checkpoint(
                    claim.run_id,
                    claim.node_id,
                    claim.attempt_id,
                    claim.request_hash,
                    claim.worker_id,
                    claim_token=claim.claim_token,
                    fencing_token=claim.fencing_token,
                    completed_turn=checkpoint.completed_turn,
                    previous_checkpoint_digest=(
                        checkpoint.previous_checkpoint_digest
                    ),
                    checkpoint_ref=alternate_ref,
                    request_ref=request_ref,
                    provider_response_refs=(
                        alternate.dependency_artifact_refs
                    ),
                    now=fixture.clock.now,
                )
            with self.assertRaisesRegex(
                AgentTurnCheckpointConflict,
                "agent_turn_checkpoint_claim_mismatch",
            ):
                fixture.store.commit_agent_turn_checkpoint(
                    claim.run_id,
                    claim.node_id,
                    claim.attempt_id,
                    claim.request_hash,
                    claim.worker_id,
                    claim_token=claim.claim_token,
                    fencing_token=claim.fencing_token + 1,
                    completed_turn=checkpoint.completed_turn,
                    previous_checkpoint_digest=(
                        checkpoint.previous_checkpoint_digest
                    ),
                    checkpoint_ref=record.checkpoint_ref,
                    request_ref=request_ref,
                    provider_response_refs=(
                        checkpoint.dependency_artifact_refs
                    ),
                    now=fixture.clock.now,
                )
            self.assertEqual(
                fixture.store.get_latest_agent_turn_checkpoint(
                    claim.run_id,
                    claim.node_id,
                    claim.attempt_id,
                ),
                record,
            )

    def test_equal_provider_payloads_preserve_ordered_checkpoint_chain(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repeated = _response_bytes("")
            invoker = _SequenceInvoker(
                [
                    repeated,
                    repeated,
                    _response_bytes("final"),
                ]
            )
            fixture = _Fixture(Path(directory), invoker=invoker)
            claim, request_ref = fixture.claim()

            result = fixture.executor.execute(claim, request_ref)

            latest = fixture.store.get_latest_agent_turn_checkpoint(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
            )
            self.assertIsNotNone(latest)
            assert latest is not None
            self.assertEqual(latest.completed_turn, 2)
            self.assertIsNotNone(latest.previous_checkpoint_digest)
            self.assertEqual(
                latest.dependency_artifact_refs[1].sha256,
                latest.dependency_artifact_refs[2].sha256,
            )
            self.assertEqual(invoker.calls, 3)
            self.assertEqual(
                json.loads(
                    fixture.artifacts.read(
                        result.result_artifact_ref
                    ).decode("utf-8")
                )["response"],
                "final",
            )

    def test_checkpoint_payload_stays_outside_control_databases(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            invoker = _SequenceInvoker(
                [
                    _response_bytes(f"```text\n{_LEAK}\n```"),
                    _response_bytes("final"),
                ]
            )
            fixture = _Fixture(Path(directory), invoker=invoker)
            claim, request_ref = fixture.claim()
            _leave_safe_turn_checkpoint(fixture, claim, request_ref)
            record = fixture.store.get_latest_agent_turn_checkpoint(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
            )
            self.assertIsNotNone(record)
            assert record is not None

            self.assertNotIn(_LEAK, repr(record))
            self.assertNotIn(
                _LEAK.encode("utf-8"),
                fixture.durable_database_bytes(),
            )
            self.assertIn(
                _LEAK.encode("utf-8"),
                fixture.artifacts.read(record.checkpoint_ref),
            )
            self.assertIs(
                record.checkpoint_ref.sensitivity,
                ArtifactSensitivity.SENSITIVE,
            )

    def test_unknown_child_tool_outcome_cannot_become_known_parent_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                invoker=_Invoker(_response_bytes()),
            )
            claim, _request_ref = fixture.claim()

            class Binding:
                run_id = claim.run_id
                attempt_id = "child-attempt"

                @staticmethod
                def validate_receipt(_receipt) -> None:
                    return None

            class Collector:
                @staticmethod
                def finalize(*, exit_reason, turns):
                    self.assertEqual(exit_reason, "EXITED")
                    self.assertEqual(turns, 1)
                    return type(
                        "Manifest",
                        (),
                        {"tool_receipts": (Binding(),)},
                    )()

            unknown_receipt = type(
                "UnknownReceipt",
                (),
                {"attempt_status": AttemptStatus.OUTCOME_UNKNOWN},
            )()
            fixture.store.get_tool_receipt = (
                lambda _run_id, _attempt_id: unknown_receipt
            )
            marked = []
            fixture.executor._mark_uncertain = marked.append

            fixture.executor._complete_known_non_success(
                claim,
                {"exit_reason": "EXITED", "turns": 1},
                Collector(),
            )

            self.assertEqual(marked, [claim])
            self.assertIs(
                fixture.store.get_attempt(claim.attempt_id).status,
                AttemptStatus.CLAIMED,
            )

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
