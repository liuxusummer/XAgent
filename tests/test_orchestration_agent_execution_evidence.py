from __future__ import annotations

import hashlib
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from src.core.agent_loop import (
    ActionResult,
    AgentContext,
    BaseHandler,
    run_agent_loop,
)
from src.core.llm import ChatResponse, ToolCall
from src.orchestration.agent_execution_evidence import (
    AgentExecutionEvidenceCollector,
    AgentExecutionEvidenceCollectorError,
)
from src.orchestration.agent_execution_manifest import (
    agent_tool_operation_key,
)
from src.orchestration.artifacts import ArtifactSensitivity
from src.orchestration.executor import (
    ToolReceipt,
    ToolReceiptVerification,
)
from src.orchestration.models import AttemptStatus
from src.orchestration.policy import EffectClass
from src.orchestration.provider_access import (
    ProviderInvocationCompletionMode,
    ProviderInvocationReceipt,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _collector() -> AgentExecutionEvidenceCollector:
    return AgentExecutionEvidenceCollector(
        run_id="run-1",
        node_id="agent-node",
        attempt_id="agent-attempt",
        request_digest=_digest("request"),
        request_artifact_digest=_digest("request-artifact"),
        definition_digest=_digest("definition"),
        request_sensitivity=ArtifactSensitivity.SENSITIVE,
    )


def _provider_receipt(
    sequence: int,
    *,
    sensitivity: ArtifactSensitivity = ArtifactSensitivity.SENSITIVE,
    **overrides,
) -> ProviderInvocationReceipt:
    values = {
        "grant_id": f"provider-grant-{sequence}",
        "run_id": "run-1",
        "node_id": "agent-node",
        "attempt_id": "agent-attempt",
        "action_digest": _digest("agent-action"),
        "authorization_digest": _digest("authorization"),
        "request_digest": _digest("request"),
        "request_artifact_digest": _digest("request-artifact"),
        "request_payload_digest": _digest(
            f"provider-payload-{sequence}"
        ),
        "invocation_index": sequence,
        "route_id": "primary-route",
        "route_digest": _digest("primary-route"),
        "grant_binding_digest": _digest(
            f"provider-grant-{sequence}"
        ),
        "response_digest": _digest(
            f"provider-response-{sequence}"
        ),
        "response_artifact_ref_digest": _digest(
            f"provider-response-ref-{sequence}"
        ),
        "response_sensitivity": sensitivity,
        "completion_mode": ProviderInvocationCompletionMode.INVOKED,
    }
    values.update(overrides)
    return ProviderInvocationReceipt(**values)


def _tool_receipt(sequence: int) -> ToolReceipt:
    operation_key = agent_tool_operation_key(
        run_id="run-1",
        node_id="agent-node",
        attempt_id="agent-attempt",
        request_digest=_digest("request"),
        sequence=sequence,
    )
    operation_digest = hashlib.sha256(
        operation_key.encode("utf-8")
    ).hexdigest()
    policy_digest = _digest("policy")
    return ToolReceipt(
        run_id="run-1",
        node_id=f"tool-node-{sequence}",
        attempt_id=f"tool-attempt-{sequence}",
        tool_name=f"tool_{sequence}",
        effect_class=EffectClass.READ_ONLY,
        attempt_status=AttemptStatus.FAILED,
        args_digest=_digest(f"args-{sequence}"),
        action_digest=_digest(f"action-{sequence}"),
        execution_binding_digest=_digest(
            f"execution-{sequence}"
        ),
        operation_key_digest=operation_digest,
        idempotency_key_digest=operation_digest,
        policy_version=f"sha256:{policy_digest}",
        policy_digest=policy_digest,
        profile_id="container",
        profile_digest=_digest("profile"),
        verification=ToolReceiptVerification.UNVERIFIED,
        sandbox_receipt=None,
        sandbox_receipt_absence_reason=(
            "dispatch_denied_before_backend"
        ),
        error_code="no_qualified_backend",
    )


class AgentExecutionEvidenceCollectorTests(unittest.TestCase):
    def test_real_loop_wires_typed_receipts_into_manifest(self) -> None:
        collector = _collector()
        tool_receipt = _tool_receipt(1)

        class Client:
            last_tools = ""
            backend = type("Backend", (), {"history": []})()

            def __init__(self) -> None:
                self.invocation_sequences: list[int] = []
                self.responses = [
                    ChatResponse(
                        thinking="",
                        content="",
                        tool_calls=[
                            ToolCall(
                                name=tool_receipt.tool_name,
                                args={"value": "safe"},
                                id="call-1",
                            )
                        ],
                        provider_receipt=_provider_receipt(1),
                    ),
                    ChatResponse(
                        thinking="",
                        content="done",
                        tool_calls=[],
                        provider_receipt=_provider_receipt(2),
                    ),
                ]

            def chat(self, messages, tools):
                del messages, tools
                self.invocation_sequences.append(
                    collector.active_provider_invocation().sequence
                )
                return self.responses.pop(0)

        class Handler(BaseHandler):
            def exec_tool_1(self, args):
                self.assert_safe_args(args)
                invocation = collector.active_tool_invocation()
                if (
                    invocation.sequence != 1
                    or invocation.turn != 1
                    or invocation.tool_name != "tool_1"
                    or invocation.tool_call_id != "call-1"
                    or invocation.operation_key
                    != agent_tool_operation_key(
                        run_id="run-1",
                        node_id="agent-node",
                        attempt_id="agent-attempt",
                        request_digest=_digest("request"),
                        sequence=1,
                    )
                ):
                    raise AssertionError(
                        "unexpected tool invocation context"
                    )
                if invocation.operation_key in repr(invocation):
                    raise AssertionError(
                        "operation key leaked through repr"
                    )
                return ActionResult(
                    data={"status": "OK"},
                    next_prompt="continue",
                    tool_receipt=tool_receipt,
                )

            @staticmethod
            def assert_safe_args(args) -> None:
                if args != {"value": "safe"}:
                    raise AssertionError("unexpected args")

        client = Client()
        handler = Handler(
            AgentContext(
                execution_evidence_observer=collector,
                display_fn=lambda _message: None,
            )
        )
        result = run_agent_loop(
            client=client,
            system_prompt="system",
            user_input="task",
            handler=handler,
            tools_schema=[],
            max_turns=3,
        )
        manifest = collector.finalize(
            exit_reason=result["exit_reason"],
            turns=result["turns"],
        )

        self.assertTrue(manifest.tool_receipts_complete)
        self.assertTrue(manifest.provider_receipts_complete)
        self.assertEqual(client.invocation_sequences, [1, 2])
        self.assertEqual(
            tuple(
                item.receipt.invocation_index
                for item in manifest.provider_receipts
            ),
            (1, 2),
        )
        self.assertEqual(
            manifest.tool_receipts[0].tool_name,
            "tool_1",
        )
        with self.assertRaisesRegex(
            AgentExecutionEvidenceCollectorError,
            "observation_unavailable",
        ):
            collector.active_provider_invocation()
        with self.assertRaisesRegex(
            AgentExecutionEvidenceCollectorError,
            "observation_unavailable",
        ):
            collector.active_tool_invocation()

    def test_complete_ordered_lineage_finalizes_idempotently(
        self,
    ) -> None:
        collector = _collector()
        first_provider = _provider_receipt(1)
        second_provider = _provider_receipt(2)
        tool = _tool_receipt(1)

        collector.provider_call_started(turn=1)
        collector.provider_call_finished(
            turn=1,
            receipt=first_provider,
        )
        collector.tool_call_started(
            turn=1,
            tool_name=tool.tool_name,
            tool_call_id="call-1",
        )
        collector.tool_call_finished(
            turn=1,
            tool_name=tool.tool_name,
            tool_call_id="call-1",
            receipt=tool,
        )
        collector.provider_call_started(turn=2)
        collector.provider_call_finished(
            turn=2,
            receipt=second_provider,
        )

        manifest = collector.finalize(
            exit_reason="CURRENT_TASK_DONE",
            turns=2,
        )

        self.assertTrue(manifest.tool_receipts_complete)
        self.assertTrue(manifest.provider_receipts_complete)
        self.assertTrue(
            manifest.has_complete_provider_receipt_lineage
        )
        self.assertEqual(manifest.observed_tool_results, 1)
        self.assertEqual(manifest.observed_provider_invocations, 2)
        self.assertEqual(
            tuple(item.turn for item in manifest.provider_receipts),
            (1, 2),
        )
        self.assertIs(
            collector.finalize(
                exit_reason="CURRENT_TASK_DONE",
                turns=2,
            ),
            manifest,
        )
        with self.assertRaisesRegex(
            AgentExecutionEvidenceCollectorError,
            "already_finalized",
        ):
            collector.provider_call_started(turn=3)

    def test_missing_receipts_remain_partial_and_promote_secret(
        self,
    ) -> None:
        collector = _collector()
        collector.provider_call_started(turn=1)
        collector.provider_call_finished(turn=1, receipt=None)
        collector.tool_call_started(
            turn=1,
            tool_name="tool_1",
            tool_call_id="call-1",
        )
        collector.tool_call_finished(
            turn=1,
            tool_name="tool_1",
            tool_call_id="call-1",
            receipt=None,
        )
        collector.provider_call_started(turn=2)
        collector.provider_call_finished(
            turn=2,
            receipt=_provider_receipt(2),
        )

        manifest = collector.finalize(
            exit_reason="CURRENT_TASK_DONE",
            turns=2,
        )

        self.assertEqual(manifest.observed_provider_invocations, 2)
        self.assertEqual(manifest.provider_receipts, ())
        self.assertFalse(manifest.provider_receipts_complete)
        self.assertEqual(manifest.observed_tool_results, 1)
        self.assertEqual(manifest.tool_receipts, ())
        self.assertFalse(manifest.tool_receipts_complete)
        self.assertIs(
            manifest.artifact_sensitivity,
            ArtifactSensitivity.SECRET,
        )
        self.assertNotIn("provider-payload", repr(collector))

    def test_wrong_parent_and_wrong_type_close_as_partial(
        self,
    ) -> None:
        collector = _collector()
        collector.provider_call_started(turn=1)
        with self.assertRaisesRegex(
            AgentExecutionEvidenceCollectorError,
            "parent_mismatch",
        ):
            collector.provider_call_finished(
                turn=1,
                receipt=_provider_receipt(
                    1,
                    node_id="other-node",
                ),
            )
        collector.tool_call_started(
            turn=1,
            tool_name="tool_1",
            tool_call_id="call-1",
        )
        with self.assertRaisesRegex(
            AgentExecutionEvidenceCollectorError,
            "invalid_tool_execution_receipt",
        ):
            collector.tool_call_finished(
                turn=1,
                tool_name="tool_1",
                tool_call_id="call-1",
                receipt=object(),
            )

        manifest = collector.finalize(
            exit_reason="ERROR",
            turns=1,
        )

        self.assertFalse(manifest.provider_receipts_complete)
        self.assertFalse(manifest.tool_receipts_complete)
        self.assertIs(
            manifest.artifact_sensitivity,
            ArtifactSensitivity.SECRET,
        )

    def test_observation_order_and_terminal_turn_fail_closed(
        self,
    ) -> None:
        collector = _collector()
        collector.provider_call_started(turn=2)
        with self.assertRaisesRegex(
            AgentExecutionEvidenceCollectorError,
            "overlap",
        ):
            collector.tool_call_started(
                turn=2,
                tool_name="tool",
                tool_call_id="call",
            )
        with self.assertRaisesRegex(
            AgentExecutionEvidenceCollectorError,
            "observation_in_progress",
        ):
            collector.finalize(exit_reason="ERROR", turns=2)
        collector.provider_call_failed(turn=2)
        with self.assertRaisesRegex(
            AgentExecutionEvidenceCollectorError,
            "final_turns",
        ):
            collector.finalize(exit_reason="ERROR", turns=1)

        manifest = collector.finalize(
            exit_reason="ERROR",
            turns=2,
        )
        with self.assertRaisesRegex(
            AgentExecutionEvidenceCollectorError,
            "finalization_conflict",
        ):
            collector.finalize(
                exit_reason="CURRENT_TASK_DONE",
                turns=2,
            )
        self.assertFalse(manifest.provider_receipts_complete)

    def test_concurrent_observation_and_finalization_have_one_winner(
        self,
    ) -> None:
        collector = _collector()
        barrier = threading.Barrier(2)

        def start_provider() -> str:
            barrier.wait(timeout=5)
            try:
                collector.provider_call_started(turn=1)
            except AgentExecutionEvidenceCollectorError as exc:
                return exc.reason_code
            return "started"

        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = tuple(
                executor.map(
                    lambda _index: start_provider(),
                    range(2),
                )
            )

        self.assertEqual(outcomes.count("started"), 1)
        self.assertEqual(
            outcomes.count(
                "execution_evidence_observation_overlap"
            ),
            1,
        )
        self.assertEqual(collector.observed_provider_calls, 1)
        collector.provider_call_failed(turn=1)

        with ThreadPoolExecutor(max_workers=2) as executor:
            manifests = tuple(
                executor.map(
                    lambda _index: collector.finalize(
                        exit_reason="ERROR",
                        turns=1,
                    ),
                    range(2),
                )
            )
        self.assertIs(manifests[0], manifests[1])

    def test_receipt_capacity_degrades_to_bounded_partial_prefix(
        self,
    ) -> None:
        collector = _collector()
        for sequence in range(1, 66):
            collector.provider_call_started(turn=sequence)
            collector.provider_call_finished(
                turn=sequence,
                receipt=_provider_receipt(sequence),
            )

        manifest = collector.finalize(
            exit_reason="CURRENT_TASK_DONE",
            turns=65,
        )

        self.assertEqual(
            manifest.observed_provider_invocations,
            65,
        )
        self.assertEqual(len(manifest.provider_receipts), 64)
        self.assertFalse(manifest.provider_receipts_complete)
        self.assertIs(
            manifest.artifact_sensitivity,
            ArtifactSensitivity.SECRET,
        )

    def test_secret_provider_receipt_promotes_complete_manifest(
        self,
    ) -> None:
        collector = _collector()
        collector.provider_call_started(turn=1)
        collector.provider_call_finished(
            turn=1,
            receipt=_provider_receipt(
                1,
                sensitivity=ArtifactSensitivity.SECRET,
            ),
        )

        manifest = collector.finalize(
            exit_reason="CURRENT_TASK_DONE",
            turns=1,
        )

        self.assertTrue(manifest.provider_receipts_complete)
        self.assertIs(
            manifest.artifact_sensitivity,
            ArtifactSensitivity.SECRET,
        )


if __name__ == "__main__":
    unittest.main()
