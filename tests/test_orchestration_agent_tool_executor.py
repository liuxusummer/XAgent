from __future__ import annotations

import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.core.agent_loop import AgentContext, run_agent_loop
from src.core.llm import ChatResponse, ToolCall
from src.orchestration.agent_execution_evidence import (
    AgentExecutionEvidenceCollector,
)
from src.orchestration.agent_request import (
    AGENT_ACTIVITY_REQUEST_MEDIA_TYPE,
    AgentActivityRequest,
)
from src.orchestration.agent_tool_executor import (
    AgentToolExecutionError,
    AgentToolExecutionSpec,
    DurableAgentToolExecutor,
)
from src.orchestration.agent_tool_handler import (
    AgentToolSpec,
    DurableAgentToolHandler,
)
from src.orchestration.agent_tool_result import (
    AgentToolResult,
    AgentToolResultArtifactStore,
)
from src.orchestration.artifacts import (
    ArtifactKind,
    ArtifactSensitivity,
)
from src.orchestration.models import AttemptStatus
from src.orchestration.policy import (
    EffectClass,
    PolicyEngine,
    PolicyOutcome,
    PolicyRule,
    ToolTimeoutBehavior,
    ToolPolicy,
)
from src.orchestration.sandbox import (
    BackendExecutionResult,
    SandboxDispatcher,
    SandboxProfile,
    SecurityLevel,
)

from tests.test_orchestration_agent_tool_authority import _Fixture


class _Backend:
    backend_id = "dynamic-agent-tool"
    security_level = SecurityLevel.OS_SANDBOX
    capabilities = ()
    supports_materialized_script = True

    def __init__(
        self,
        fixture: _Fixture,
        *,
        invalid_result: bool = False,
        wait: threading.Event | None = None,
        release: threading.Event | None = None,
    ) -> None:
        self.fixture = fixture
        self.invalid_result = invalid_result
        self.wait = wait
        self.release = release
        self.calls = 0

    def execute(self, request, profile) -> BackendExecutionResult:
        del profile
        self.calls += 1
        if self.wait is not None:
            self.wait.set()
        if self.release is not None:
            self.release.wait(timeout=5)
        action = request.action
        if self.invalid_result:
            ref = self.fixture.artifacts.put_json(
                {"not": "an AgentToolResult"},
                kind=ArtifactKind.TOOL_RESULT,
                sensitivity=ArtifactSensitivity.SENSITIVE,
                producer_run_id=action.run_id,
                producer_node_id=action.node_id,
                producer_attempt_id=action.attempt_id,
            )
        else:
            ref = AgentToolResultArtifactStore.stage(
                self.fixture.artifacts,
                AgentToolResult(
                    data={"status": "OK", "value": "sandboxed"},
                    next_prompt="continue",
                ),
                sensitivity=ArtifactSensitivity.SENSITIVE,
                run_id=action.run_id,
                node_id=action.node_id,
                attempt_id=action.attempt_id,
            )
        return BackendExecutionResult(
            exit_code=0,
            output_artifact_refs=(ref,),
        )


class _Client:
    def __init__(self) -> None:
        self.responses = [
            ChatResponse(
                thinking="",
                content="",
                tool_calls=[
                    ToolCall(
                        name="echo",
                        args={"value": "safe", "token": None},
                        id="call-1",
                    )
                ],
                raw="",
                stop_reason="tool_use",
            ),
            ChatResponse(
                thinking="",
                content="complete",
                tool_calls=[],
                raw="complete",
                stop_reason="end_turn",
            ),
        ]
        self.history: list[dict] = []
        self.history_compaction: list[dict] = []
        self.context_window_chars = 24_000
        self.last_tools = ""

    @property
    def backend(self):
        return self

    def chat(self, messages, tools):
        del messages, tools
        return self.responses.pop(0)


class _MemoryArtifacts:
    def put_bytes(self, content, **metadata):
        del content, metadata
        raise NotImplementedError

    def put_json(self, value, **metadata):
        del value, metadata
        raise NotImplementedError

    def open(self, ref):
        del ref
        raise NotImplementedError

    def read(self, ref):
        del ref
        raise NotImplementedError

    def verify(self, ref):
        del ref
        return False

    def exists(self, ref):
        del ref
        return False


class DurableAgentToolExecutorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.fixture = _Fixture(self.root)
        self.agent_root = self.root / "agent-workspace"
        self.agent_root.mkdir()

    def _runtime(
        self,
        *,
        effect_class: EffectClass = EffectClass.READ_ONLY,
        outcome: PolicyOutcome | None = None,
        invalid_result: bool = False,
        wait: threading.Event | None = None,
        release: threading.Event | None = None,
        fault_hook=None,
    ):
        locks = (
            ()
            if effect_class is EffectClass.READ_ONLY
            else ("workspace:/dynamic",)
        )
        tool_policy = ToolPolicy(
            "echo",
            effect_class,
            supports_idempotency_key=(
                effect_class is EffectClass.IDEMPOTENT_WRITE
            ),
            supports_status_probe=False,
            supports_compensation=False,
            timeout_behavior=(
                ToolTimeoutBehavior.SAFE_TO_RETRY
                if effect_class is EffectClass.READ_ONLY
                else ToolTimeoutBehavior.OUTCOME_UNKNOWN
            ),
            allowed_resource_keys=locks,
            required_resource_keys=locks,
        )
        rules = (
            ()
            if outcome is None
            else (
                PolicyRule(
                    "dynamic-tool-rule",
                    outcome,
                    tool_name="echo",
                    effect_classes=(effect_class,),
                    reason_code=f"dynamic_{outcome.value}",
                ),
            )
        )
        policy = PolicyEngine((tool_policy,), rules)
        backend = _Backend(
            self.fixture,
            invalid_result=invalid_result,
            wait=wait,
            release=release,
        )
        sandbox = SandboxDispatcher(
            (backend,),
            policy_version=policy.policy_version,
        )
        profile = SandboxProfile(
            "dynamic-agent-tool-profile",
            (self.agent_root,),
            (),
            minimum_security_level=SecurityLevel.OS_SANDBOX,
        )
        spec = AgentToolExecutionSpec(
            tool_name="echo",
            effect_class=effect_class,
            profile=profile,
            cwd=str(self.agent_root),
            argv_builder=lambda args: (
                "echo-runner",
                str(args.get("value", "")),
            ),
            resource_locks=locks,
        )
        executor = DurableAgentToolExecutor(
            self.fixture.store,
            policy,
            sandbox,
            self.fixture.artifacts,
            (spec,),
            worker_id="dynamic-worker",
            lease_grace_seconds=10,
            clock=self.fixture.clock,
            fault_hook=fault_hook,
        )
        return executor, backend, policy, sandbox, spec

    def test_success_is_committed_before_return_and_replayed(self) -> None:
        executor, backend, policy, sandbox, spec = self._runtime()
        request = self.fixture.request()

        first = executor.execute(request)
        restarted = DurableAgentToolExecutor(
            self.fixture.store,
            policy,
            sandbox,
            self.fixture.artifacts,
            (spec,),
            worker_id="restarted-worker",
            lease_grace_seconds=10,
            clock=self.fixture.clock,
        )
        second = restarted.execute(request)

        self.assertEqual(first, second)
        self.assertEqual(first.receipt.attempt_status, AttemptStatus.SUCCEEDED)
        self.assertIsNotNone(first.result_artifact_ref)
        self.assertEqual(backend.calls, 1)
        self.assertEqual(
            self.fixture.store.get_tool_receipt(
                request.run_id,
                first.receipt.attempt_id,
            ),
            first.receipt,
        )

    def test_real_agent_loop_uses_durable_dynamic_child_authority(self) -> None:
        executor, backend, _policy, _sandbox, _spec = self._runtime()
        parent = self.fixture.parent
        request = AgentActivityRequest(
            run_id=parent.run_id,
            node_id=parent.node_id,
            attempt_id=parent.attempt_id,
            attempt_number=parent.attempt_number,
            agent_name="main",
            request_digest=parent.request_hash,
            definition_digest=self.fixture.workflow.definition_digest,
            task="perform the task",
        )
        request_ref = self.fixture.artifacts.put_bytes(
            request.to_bytes(),
            media_type=AGENT_ACTIVITY_REQUEST_MEDIA_TYPE,
            kind=ArtifactKind.AGENT_REQUEST,
            sensitivity=request.artifact_sensitivity,
            producer_run_id=request.run_id,
            producer_node_id=request.node_id,
            producer_attempt_id=request.attempt_id,
            metadata={"schema": "agent_activity_request_v1"},
        )
        collector = AgentExecutionEvidenceCollector(
            run_id=request.run_id,
            node_id=request.node_id,
            attempt_id=request.attempt_id,
            request_digest=request.request_digest,
            request_artifact_digest=request_ref.sha256,
            definition_digest=request.definition_digest,
            request_sensitivity=request_ref.sensitivity,
        )
        handler = DurableAgentToolHandler(
            ctx=AgentContext(
                execution_evidence_observer=collector,
                display_fn=lambda _message: None,
            ),
            executor=executor,
            receipt_store=self.fixture.store,
            result_store=self.fixture.artifacts,
            request=request,
            request_ref=request_ref,
            collector=collector,
            tool_specs=(
                AgentToolSpec("echo", sensitive_keys=("token",)),
            ),
        )

        loop_result = run_agent_loop(
            client=_Client(),
            system_prompt="system",
            user_input="task",
            handler=handler,
            tools_schema=[{"name": "echo"}],
            max_turns=3,
        )
        manifest = collector.finalize(
            exit_reason=loop_result["exit_reason"],
            turns=loop_result["turns"],
        )

        self.assertEqual(loop_result["response"], "complete")
        self.assertEqual(backend.calls, 1)
        self.assertTrue(manifest.tool_receipts_complete)
        tool_events = [
            event.event_type
            for event in self.fixture.store.list_events(parent.run_id)
            if event.event_type.startswith("agent_tool.")
        ]
        self.assertEqual(
            tool_events,
            [
                "agent_tool.scheduled",
                "agent_tool.started",
                "agent_tool.succeeded",
            ],
        )
        self.assertTrue(
            self.fixture.store.verify_projections(parent.run_id)
        )

    def test_policy_denial_never_dispatches_backend(self) -> None:
        executor, backend, _policy, _sandbox, _spec = self._runtime(
            outcome=PolicyOutcome.DENY,
        )

        result = executor.execute(self.fixture.request())

        self.assertEqual(result.receipt.attempt_status, AttemptStatus.FAILED)
        self.assertEqual(
            result.receipt.sandbox_receipt_absence_reason,
            "policy_denied_before_backend",
        )
        self.assertEqual(backend.calls, 0)

    def test_concurrent_duplicate_executes_backend_once(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        executor, backend, _policy, _sandbox, _spec = self._runtime(
            wait=entered,
            release=release,
        )
        request = self.fixture.request()

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(executor.execute, request)
            self.assertTrue(entered.wait(timeout=5))
            second = pool.submit(executor.execute, request)
            with self.assertRaisesRegex(
                AgentToolExecutionError,
                "agent_tool_active",
            ):
                second.result(timeout=5)
            release.set()
            completed = first.result(timeout=5)

        self.assertEqual(
            completed.receipt.attempt_status,
            AttemptStatus.SUCCEEDED,
        )
        self.assertEqual(backend.calls, 1)

    def test_restart_after_authorization_expires_to_unknown(self) -> None:
        def crash(stage: str) -> None:
            if stage == "agent_tool.authorization_committed":
                raise RuntimeError("simulated process crash")

        executor, backend, policy, sandbox, spec = self._runtime(
            fault_hook=crash,
        )
        request = self.fixture.request()

        with self.assertRaisesRegex(RuntimeError, "simulated process crash"):
            executor.execute(request)
        self.assertEqual(backend.calls, 0)

        self.fixture.clock.now = 171.0
        restarted = DurableAgentToolExecutor(
            self.fixture.store,
            policy,
            sandbox,
            self.fixture.artifacts,
            (spec,),
            worker_id="recovery-worker",
            lease_grace_seconds=10,
            clock=self.fixture.clock,
        )
        recovered = restarted.execute(request)

        self.assertEqual(
            recovered.receipt.attempt_status,
            AttemptStatus.OUTCOME_UNKNOWN,
        )
        self.assertEqual(
            recovered.receipt.sandbox_receipt_absence_reason,
            "agent_tool_lease_expired",
        )
        self.assertEqual(backend.calls, 0)

    def test_crash_after_backend_return_does_not_repeat_side_effect(self) -> None:
        def crash(stage: str) -> None:
            if stage == "agent_tool.backend_returned":
                raise RuntimeError("crash after backend return")

        executor, backend, policy, sandbox, spec = self._runtime(
            effect_class=EffectClass.NON_IDEMPOTENT_WRITE,
            outcome=PolicyOutcome.ALLOW,
            fault_hook=crash,
        )
        request = self.fixture.request()

        with self.assertRaisesRegex(
            RuntimeError,
            "crash after backend return",
        ):
            executor.execute(request)
        self.assertEqual(backend.calls, 1)

        self.fixture.clock.now = 171.0
        restarted = DurableAgentToolExecutor(
            self.fixture.store,
            policy,
            sandbox,
            self.fixture.artifacts,
            (spec,),
            worker_id="recovery-worker",
            lease_grace_seconds=10,
            clock=self.fixture.clock,
        )
        recovered = restarted.execute(request)

        self.assertEqual(
            recovered.receipt.attempt_status,
            AttemptStatus.OUTCOME_UNKNOWN,
        )
        self.assertEqual(backend.calls, 1)

    def test_invalid_success_artifact_is_fail_closed(self) -> None:
        executor, backend, _policy, _sandbox, _spec = self._runtime(
            invalid_result=True,
        )

        result = executor.execute(self.fixture.request())

        self.assertEqual(result.receipt.attempt_status, AttemptStatus.FAILED)
        self.assertIsNone(result.result_artifact_ref)
        self.assertEqual(result.receipt.error_code, "artifact_integrity")
        self.assertEqual(backend.calls, 1)

    def test_uncertain_write_with_invalid_result_stops_recovery(self) -> None:
        executor, backend, _policy, _sandbox, _spec = self._runtime(
            effect_class=EffectClass.NON_IDEMPOTENT_WRITE,
            outcome=PolicyOutcome.ALLOW,
            invalid_result=True,
        )

        result = executor.execute(self.fixture.request())

        self.assertEqual(
            result.receipt.attempt_status,
            AttemptStatus.OUTCOME_UNKNOWN,
        )
        self.assertEqual(backend.calls, 1)

    def test_control_plane_overlap_is_rejected_at_construction(self) -> None:
        _executor, _backend, policy, sandbox, _spec = self._runtime()
        overlapping_profile = SandboxProfile(
            "overlapping-profile",
            (self.root,),
            (),
            minimum_security_level=SecurityLevel.OS_SANDBOX,
        )
        overlapping_spec = AgentToolExecutionSpec(
            tool_name="echo",
            effect_class=EffectClass.READ_ONLY,
            profile=overlapping_profile,
            cwd=str(self.root),
            argv_builder=lambda _args: ("echo-runner",),
        )

        with self.assertRaisesRegex(
            AgentToolExecutionError,
            "agent_tool_configuration_invalid",
        ):
            DurableAgentToolExecutor(
                self.fixture.store,
                policy,
                sandbox,
                self.fixture.artifacts,
                (overlapping_spec,),
                worker_id="dynamic-worker",
            )

    def test_ephemeral_result_store_cannot_claim_recovery_readiness(
        self,
    ) -> None:
        _executor, _backend, policy, sandbox, spec = self._runtime()

        with self.assertRaisesRegex(
            AgentToolExecutionError,
            "agent_tool_configuration_invalid",
        ):
            DurableAgentToolExecutor(
                self.fixture.store,
                policy,
                sandbox,
                _MemoryArtifacts(),
                (spec,),
                worker_id="dynamic-worker",
            )


if __name__ == "__main__":
    unittest.main()
