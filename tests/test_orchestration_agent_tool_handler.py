from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path

from src.core.agent_loop import (
    AgentContext,
    exhaust,
    run_agent_loop,
)
from src.core.llm import ChatResponse, ToolCall
from src.orchestration.agent_execution_evidence import (
    AgentExecutionEvidenceCollector,
)
from src.orchestration.agent_request import (
    AGENT_ACTIVITY_REQUEST_MEDIA_TYPE,
    AgentActivityRequest,
)
from src.orchestration.agent_tool_handler import (
    AgentToolHandlerError,
    AgentToolInvocationResult,
    AgentToolSpec,
    DurableAgentToolHandler,
)
from src.orchestration.agent_tool_result import (
    AGENT_TOOL_RESULT_MEDIA_TYPE,
    AgentToolResult,
    AgentToolResultArtifactStore,
    AgentToolResultError,
)
from src.orchestration.artifacts import (
    ArtifactKind,
    ArtifactSensitivity,
    LocalArtifactStore,
)
from src.orchestration.executor import (
    ToolReceipt,
    ToolReceiptVerification,
)
from src.orchestration.models import AttemptStatus
from src.orchestration.policy import EffectClass
from src.orchestration.sandbox import (
    SandboxOutcome,
    SandboxReceipt,
    SecurityLevel,
)
from src.orchestration.store import DurableRunStore


_EXECUTOR_SECRET = "durable-tool-executor-secret"


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


class _ReceiptStore(DurableRunStore):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.receipts: dict[tuple[str, str], ToolReceipt] = {}

    def get_tool_receipt(
        self,
        run_id: str,
        attempt_id: str,
    ) -> ToolReceipt | None:
        return self.receipts.get((run_id, attempt_id))


class _UnverifiedArtifactStore(LocalArtifactStore):
    def verify(self, ref) -> bool:
        del ref
        return False


class _Executor:
    durable_result_recovery_ready = True

    def __init__(
        self,
        receipt_store: _ReceiptStore,
        result_store: LocalArtifactStore,
        *,
        status: AttemptStatus = AttemptStatus.SUCCEEDED,
        explode: bool = False,
        mutation=None,
        block: bool = False,
        persist_receipt: bool = True,
    ) -> None:
        self.receipt_store = receipt_store
        self.result_store = result_store
        self.status = status
        self.explode = explode
        self.mutation = mutation
        self.requests = []
        self.results = []
        self.entered = threading.Event()
        self.release = threading.Event()
        self.block = block
        self.persist_receipt = persist_receipt

    def execute(self, request):
        self.requests.append(request)
        if self.explode:
            raise RuntimeError(_EXECUTOR_SECRET)
        if self.block:
            self.entered.set()
            if not self.release.wait(timeout=2):
                raise TimeoutError("blocking executor test timed out")
        child_node_id = f"tool-node-{request.sequence}"
        child_attempt_id = f"tool-attempt-{request.sequence}"
        result_ref = None
        sandbox_receipt = None
        if self.status is AttemptStatus.SUCCEEDED:
            tool_result = AgentToolResult(
                data={"status": "OK", "value": "safe"},
                next_prompt="continue",
            )
            result_ref = AgentToolResultArtifactStore.stage(
                self.result_store,
                tool_result,
                sensitivity=request.sensitivity,
                run_id=request.run_id,
                node_id=child_node_id,
                attempt_id=child_attempt_id,
            )
            sandbox_receipt = SandboxReceipt(
                backend_id="isolated-tool-gateway",
                security_level=SecurityLevel.OS_SANDBOX,
                profile_id="agent-tool-profile",
                profile_digest=_digest("profile"),
                action_digest=_digest(request.invocation_digest),
                policy_version=f"sha256:{_digest('policy-version')}",
                request_digest=_digest(request.invocation_digest),
                outcome=SandboxOutcome.SUCCEEDED,
                exit_code=0,
                timed_out=False,
                output_artifact_refs=(result_ref,),
            ).to_dict()
        policy_version = f"sha256:{_digest('policy-version')}"
        receipt = ToolReceipt(
            run_id=request.run_id,
            node_id=child_node_id,
            attempt_id=child_attempt_id,
            tool_name=request.tool_name,
            effect_class=EffectClass.READ_ONLY,
            attempt_status=self.status,
            args_digest=request.args_digest,
            action_digest=_digest(request.invocation_digest),
            execution_binding_digest=_digest(
                f"execution:{request.invocation_digest}"
            ),
            operation_key_digest=hashlib.sha256(
                request.operation_key.encode("utf-8")
            ).hexdigest(),
            idempotency_key_digest=hashlib.sha256(
                request.operation_key.encode("utf-8")
            ).hexdigest(),
            policy_version=policy_version,
            policy_digest=_digest("policy-version"),
            profile_id="agent-tool-profile",
            profile_digest=_digest("profile"),
            verification=(
                ToolReceiptVerification.VERIFIED
                if self.status is AttemptStatus.SUCCEEDED
                else ToolReceiptVerification.UNVERIFIED
            ),
            sandbox_receipt=sandbox_receipt,
            sandbox_receipt_absence_reason=(
                None
                if sandbox_receipt is not None
                else "tool_execution_failed"
            ),
            error_code=(
                None
                if self.status is AttemptStatus.SUCCEEDED
                else "fixed_tool_error"
            ),
        )
        result = AgentToolInvocationResult(
            receipt=receipt,
            result_artifact_ref=result_ref,
        )
        if self.mutation is not None:
            result = self.mutation(request, result)
        if self.persist_receipt:
            self.receipt_store.receipts[
                (result.receipt.run_id, result.receipt.attempt_id)
            ] = result.receipt
        self.results.append(result)
        return result


class _Client:
    def __init__(self) -> None:
        self.responses = [
            ChatResponse(
                thinking="",
                content="",
                tool_calls=[
                    ToolCall(
                        name="echo",
                        args={
                            "value": "safe",
                            "token": None,
                        },
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

    @property
    def backend(self):
        return self

    history: list[dict] = []
    history_compaction: list[dict] = []
    context_window_chars = 24_000
    last_tools = ""

    def chat(self, messages, tools):
        del messages, tools
        return self.responses.pop(0)


class _Fixture:
    def __init__(
        self,
        root: Path,
        *,
        executor_options: dict | None = None,
        result_store_type=LocalArtifactStore,
    ) -> None:
        self.result_store = result_store_type(root / "artifacts")
        self.receipt_store = _ReceiptStore(root / "domain.sqlite3")
        self.request = AgentActivityRequest(
            run_id="run-1",
            node_id="agent-node",
            attempt_id="agent-attempt",
            attempt_number=1,
            agent_name="main",
            request_digest=_digest("agent-request"),
            definition_digest=_digest("definition"),
            task="sensitive task",
        )
        self.request_ref = self.result_store.put_bytes(
            self.request.to_bytes(),
            media_type=AGENT_ACTIVITY_REQUEST_MEDIA_TYPE,
            kind=ArtifactKind.AGENT_REQUEST,
            sensitivity=self.request.artifact_sensitivity,
            producer_run_id=self.request.run_id,
            producer_node_id=self.request.node_id,
            producer_attempt_id=self.request.attempt_id,
            metadata={"schema": "agent_activity_request_v1"},
        )
        self.collector = AgentExecutionEvidenceCollector(
            run_id=self.request.run_id,
            node_id=self.request.node_id,
            attempt_id=self.request.attempt_id,
            request_digest=self.request.request_digest,
            request_artifact_digest=self.request_ref.sha256,
            definition_digest=self.request.definition_digest,
            request_sensitivity=self.request_ref.sensitivity,
        )
        self.ctx = AgentContext(
            execution_evidence_observer=self.collector,
            display_fn=lambda _message: None,
        )
        self.executor = _Executor(
            self.receipt_store,
            self.result_store,
            **(executor_options or {}),
        )
        self.handler = DurableAgentToolHandler(
            ctx=self.ctx,
            executor=self.executor,
            receipt_store=self.receipt_store,
            result_store=self.result_store,
            request=self.request,
            request_ref=self.request_ref,
            collector=self.collector,
            tool_specs=(
                AgentToolSpec("echo", sensitive_keys=("token",)),
            ),
        )


class AgentToolResultTests(unittest.TestCase):
    def test_canonical_round_trip_and_artifact_binding(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalArtifactStore(Path(directory) / "artifacts")
            result = AgentToolResult(
                data={"items": [1, True, None], "text": "结果"},
                next_prompt="continue",
                flags=frozenset({"retry"}),
            )
            ref = AgentToolResultArtifactStore.stage(
                store,
                result,
                sensitivity=ArtifactSensitivity.SENSITIVE,
                run_id="run-1",
                node_id="tool-node",
                attempt_id="tool-attempt",
            )

            self.assertEqual(
                AgentToolResult.from_bytes(result.to_bytes()),
                result,
            )
            self.assertEqual(
                AgentToolResultArtifactStore.load(store, ref),
                result,
            )
            self.assertEqual(ref.media_type, AGENT_TOOL_RESULT_MEDIA_TYPE)
            self.assertEqual(dict(ref.metadata), {})

    def test_malformed_results_are_rejected_without_context(self) -> None:
        base = AgentToolResult(
            data={"status": "OK"},
            next_prompt=None,
        ).to_bytes()
        payload = json.loads(base.decode("utf-8"))
        unknown = dict(payload)
        unknown["unknown"] = True
        attacks = (
            base + b"\n",
            base.replace(
                b'"schema_version":1',
                b'"schema_version":1,"schema_version":1',
            ),
            json.dumps(unknown, separators=(",", ":"), sort_keys=True).encode(),
            base.replace(b'"schema_version":1', b'"schema_version":true'),
        )
        for attack in attacks:
            with self.subTest(attack=attack[:24]):
                with self.assertRaises(AgentToolResultError) as raised:
                    AgentToolResult.from_bytes(attack)
                self.assertEqual(
                    str(raised.exception),
                    "agent_tool_result_invalid",
                )
                self.assertIsNone(raised.exception.__context__)

        cyclic = []
        cyclic.append(cyclic)
        with self.assertRaisesRegex(
            AgentToolResultError,
            "agent_tool_result_invalid",
        ):
            AgentToolResult(data=cyclic, next_prompt=None)
        with self.assertRaisesRegex(
            AgentToolResultError,
            "agent_tool_result_invalid",
        ):
            AgentToolResult(
                data={},
                next_prompt=None,
                flags=frozenset({"unbounded-control"}),
            )
        with self.assertRaisesRegex(
            AgentToolResultError,
            "agent_tool_result_invalid",
        ):
            AgentToolResult(
                data={},
                next_prompt="x" * (64 * 1024 + 1),
            )
        with self.assertRaisesRegex(
            AgentToolResultError,
            "agent_tool_result_invalid",
        ):
            AgentToolResult(
                data={
                    "items": ["x" * (384 * 1024)] * 3,
                },
                next_prompt=None,
            )

    def test_stage_requires_store_integrity_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = _UnverifiedArtifactStore(
                Path(directory) / "artifacts"
            )
            with self.assertRaises(AgentToolResultError) as raised:
                AgentToolResultArtifactStore.stage(
                    store,
                    AgentToolResult(data={}, next_prompt=None),
                    sensitivity=ArtifactSensitivity.SENSITIVE,
                    run_id="run-1",
                    node_id="tool-node",
                    attempt_id="tool-attempt",
                )
            self.assertEqual(
                str(raised.exception),
                "agent_tool_result_artifact_unavailable",
            )
            self.assertIsNone(raised.exception.__context__)

    def test_plaintext_secret_result_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalArtifactStore(Path(directory) / "artifacts")
            with self.assertRaisesRegex(
                AgentToolResultError,
                "agent_tool_result_artifact_invalid",
            ):
                AgentToolResultArtifactStore.stage(
                    store,
                    AgentToolResult(data={}, next_prompt=None),
                    sensitivity=ArtifactSensitivity.SECRET,
                    run_id="run-1",
                    node_id="tool-node",
                    attempt_id="tool-attempt",
                )
            self.assertEqual(
                tuple(
                    path
                    for path in store.root.rglob("*")
                    if path.is_file()
                ),
                (),
            )


class DurableAgentToolHandlerTests(unittest.TestCase):
    def test_real_loop_attaches_verified_tool_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))

            loop_result = run_agent_loop(
                client=_Client(),
                system_prompt="system",
                user_input="task",
                handler=fixture.handler,
                tools_schema=[{"name": "echo"}],
                max_turns=3,
            )
            manifest = fixture.collector.finalize(
                exit_reason=loop_result["exit_reason"],
                turns=loop_result["turns"],
            )

            self.assertEqual(loop_result["response"], "complete")
            self.assertEqual(len(fixture.executor.requests), 1)
            request = fixture.executor.requests[0]
            self.assertNotIn(request.operation_key, repr(request))
            self.assertTrue(manifest.tool_receipts_complete)
            self.assertEqual(manifest.observed_tool_results, 1)
            self.assertFalse(manifest.provider_receipts_complete)
            self.assertFalse(fixture.handler.production_security_ready)
            self.assertNotIn("sensitive task", repr(fixture.handler))

    def test_inline_sensitive_arguments_fail_before_executor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            fixture.collector.tool_call_started(
                turn=1,
                tool_name="echo",
                tool_call_id="call-1",
            )
            with self.assertRaises(AgentToolHandlerError) as raised:
                exhaust(
                    fixture.handler.dispatch(
                        "echo",
                        {
                            "value": "safe",
                            "token": "must-not-cross-boundary",
                        },
                    )
                )
            fixture.collector.tool_call_failed(
                turn=1,
                tool_name="echo",
                tool_call_id="call-1",
            )

            self.assertEqual(
                str(raised.exception),
                "agent_tool_invocation_binding_mismatch",
            )
            self.assertIsNone(raised.exception.__context__)
            self.assertEqual(fixture.executor.requests, [])

    def test_invocation_arguments_are_detached_and_deeply_read_only(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            arguments = {
                "value": {"items": ["safe"]},
                "token": None,
            }
            fixture.collector.tool_call_started(
                turn=1,
                tool_name="echo",
                tool_call_id="call-1",
            )
            result = exhaust(fixture.handler.dispatch("echo", arguments))
            fixture.collector.tool_call_finished(
                turn=1,
                tool_name="echo",
                tool_call_id="call-1",
                receipt=result.tool_receipt,
            )

            request = fixture.executor.requests[0]
            arguments["value"]["items"][0] = "changed"
            self.assertEqual(
                request.args["value"]["items"],
                ("safe",),
            )
            with self.assertRaises(TypeError):
                request.args["value"]["items"][0] = "changed"

    def test_parent_request_must_be_store_verified(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(
                AgentToolHandlerError,
                "agent_tool_parent_binding_mismatch",
            ):
                _Fixture(
                    Path(directory),
                    result_store_type=_UnverifiedArtifactStore,
                )

    def test_missing_observation_and_executor_errors_are_sanitized(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            missing = _Fixture(root / "missing")
            with self.assertRaises(AgentToolHandlerError) as raised:
                exhaust(
                    missing.handler.dispatch(
                        "echo",
                        {"value": "safe"},
                    )
                )
            self.assertEqual(
                str(raised.exception),
                "agent_tool_invocation_unavailable",
            )
            self.assertEqual(missing.executor.requests, [])

            exploding = _Fixture(
                root / "exploding",
                executor_options={"explode": True},
            )
            exploding.collector.tool_call_started(
                turn=1,
                tool_name="echo",
                tool_call_id="call-1",
            )
            with self.assertRaises(AgentToolHandlerError) as raised:
                exhaust(
                    exploding.handler.dispatch(
                        "echo",
                        {"value": "safe"},
                    )
                )
            exploding.collector.tool_call_failed(
                turn=1,
                tool_name="echo",
                tool_call_id="call-1",
            )
            self.assertEqual(
                str(raised.exception),
                "agent_tool_executor_unavailable",
            )
            self.assertNotIn(_EXECUTOR_SECRET, repr(raised.exception))
            self.assertIsNone(raised.exception.__context__)

    def test_argument_receipt_and_result_substitutions_fail_closed(
        self,
    ) -> None:
        def wrong_args(_request, result):
            return AgentToolInvocationResult(
                receipt=replace(
                    result.receipt,
                    args_digest=_digest("wrong-args"),
                ),
                result_artifact_ref=result.result_artifact_ref,
            )

        def wrong_operation(_request, result):
            return AgentToolInvocationResult(
                receipt=replace(
                    result.receipt,
                    operation_key_digest=_digest("wrong-operation"),
                    idempotency_key_digest=_digest("wrong-operation"),
                ),
                result_artifact_ref=result.result_artifact_ref,
            )

        def missing_result(_request, result):
            return AgentToolInvocationResult(
                receipt=result.receipt,
                result_artifact_ref=None,
            )

        def downgraded_result(_request, result):
            return AgentToolInvocationResult(
                receipt=result.receipt,
                result_artifact_ref=replace(
                    result.result_artifact_ref,
                    sensitivity=ArtifactSensitivity.INTERNAL,
                ),
            )

        def wrong_producer(_request, result):
            return AgentToolInvocationResult(
                receipt=result.receipt,
                result_artifact_ref=replace(
                    result.result_artifact_ref,
                    producer_attempt_id="other-attempt",
                ),
            )

        def missing_output_binding(_request, result):
            sandbox_receipt = dict(result.receipt.sandbox_receipt)
            sandbox_receipt["output_artifact_refs"] = []
            return AgentToolInvocationResult(
                receipt=replace(
                    result.receipt,
                    sandbox_receipt=sandbox_receipt,
                ),
                result_artifact_ref=result.result_artifact_ref,
            )

        for name, mutation, reason in (
            (
                "args",
                wrong_args,
                "agent_tool_invocation_binding_mismatch",
            ),
            (
                "operation",
                wrong_operation,
                "agent_tool_invocation_binding_mismatch",
            ),
            ("result", missing_result, "agent_tool_result_invalid"),
            (
                "classification",
                downgraded_result,
                "agent_tool_result_invalid",
            ),
            (
                "producer",
                wrong_producer,
                "agent_tool_result_invalid",
            ),
            (
                "sandbox-output",
                missing_output_binding,
                "agent_tool_result_invalid",
            ),
        ):
            with self.subTest(name=name):
                with tempfile.TemporaryDirectory() as directory:
                    fixture = _Fixture(
                        Path(directory),
                        executor_options={"mutation": mutation},
                    )
                    fixture.collector.tool_call_started(
                        turn=1,
                        tool_name="echo",
                        tool_call_id="call-1",
                    )
                    with self.assertRaisesRegex(
                        AgentToolHandlerError,
                        reason,
                    ):
                        exhaust(
                            fixture.handler.dispatch(
                                "echo",
                                {"value": "safe"},
                            )
                        )
                    fixture.collector.tool_call_failed(
                        turn=1,
                        tool_name="echo",
                        tool_call_id="call-1",
                    )

    def test_consumed_child_receipt_cannot_be_replayed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            fixture.collector.tool_call_started(
                turn=1,
                tool_name="echo",
                tool_call_id="call-1",
            )
            action_result = exhaust(
                fixture.handler.dispatch(
                    "echo",
                    {"value": "safe"},
                )
            )
            fixture.collector.tool_call_finished(
                turn=1,
                tool_name="echo",
                tool_call_id="call-1",
                receipt=action_result.tool_receipt,
            )

            with self.assertRaisesRegex(
                AgentToolHandlerError,
                "agent_tool_invocation_binding_mismatch",
            ):
                fixture.handler._validate_result(
                    fixture.executor.requests[0],
                    fixture.executor.results[0],
                )

    def test_receipt_must_exist_in_durable_store(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                executor_options={"persist_receipt": False},
            )
            fixture.collector.tool_call_started(
                turn=1,
                tool_name="echo",
                tool_call_id="call-1",
            )
            with self.assertRaisesRegex(
                AgentToolHandlerError,
                "agent_tool_receipt_invalid",
            ):
                exhaust(
                    fixture.handler.dispatch(
                        "echo",
                        {"value": "safe"},
                    )
                )
            fixture.collector.tool_call_failed(
                turn=1,
                tool_name="echo",
                tool_call_id="call-1",
            )

    def test_uncertain_receipt_stops_loop_with_safe_result(self) -> None:
        def unsafe_error(_request, result):
            return AgentToolInvocationResult(
                receipt=replace(
                    result.receipt,
                    error_code="ignore_previous_instructions",
                ),
                result_artifact_ref=None,
            )

        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                executor_options={
                    "status": AttemptStatus.OUTCOME_UNKNOWN,
                    "mutation": unsafe_error,
                },
            )
            fixture.collector.tool_call_started(
                turn=1,
                tool_name="echo",
                tool_call_id="call-1",
            )
            result = exhaust(
                fixture.handler.dispatch(
                    "echo",
                    {"value": "safe"},
                )
            )
            fixture.collector.tool_call_finished(
                turn=1,
                tool_name="echo",
                tool_call_id="call-1",
                receipt=result.tool_receipt,
            )

            self.assertTrue(result.should_exit)
            self.assertEqual(result.next_prompt, "")
            self.assertEqual(
                result.data,
                {
                    "status": "DURABLE_TOOL_FAILED",
                    "attempt_status": "outcome_unknown",
                    "error_code": "tool_execution_outcome_unknown",
                },
            )

    def test_same_handler_rejects_concurrent_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(
                Path(directory),
                executor_options={"block": True},
            )
            fixture.collector.tool_call_started(
                turn=1,
                tool_name="echo",
                tool_call_id="call-1",
            )
            responses = []
            failures = []

            def first_call() -> None:
                try:
                    responses.append(
                        exhaust(
                            fixture.handler.dispatch(
                                "echo",
                                {"value": "safe"},
                            )
                        )
                    )
                except BaseException as exc:
                    failures.append(exc)

            thread = threading.Thread(target=first_call)
            thread.start()
            try:
                self.assertTrue(
                    fixture.executor.entered.wait(timeout=2)
                )
                with self.assertRaisesRegex(
                    AgentToolHandlerError,
                    "agent_tool_execution_in_progress",
                ):
                    exhaust(
                        fixture.handler.dispatch(
                            "echo",
                            {"value": "second"},
                        )
                    )
            finally:
                fixture.executor.release.set()
                thread.join(timeout=2)

            self.assertFalse(thread.is_alive())
            self.assertEqual(failures, [])
            self.assertEqual(len(responses), 1)


if __name__ == "__main__":
    unittest.main()
