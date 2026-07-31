from __future__ import annotations

import threading
import unittest

from src.core.agent_loop import (
    ActionResult,
    AgentContext,
    BaseHandler,
    ExecutionEvidenceObservationError,
    exhaust,
    run_agent_loop,
)
from src.core.llm import ChatResponse, TokenUsage, ToolCall
from src.handler import XAgentHandler


class DummyClient:
    def __init__(self, responses: list[ChatResponse]) -> None:
        self.responses = responses
        self.last_tools = "cached"
        self.calls = 0
        self.messages_seen = []
        self.backend = type("Backend", (), {"history": []})()

    def chat(self, messages, tools):
        _ = (messages, tools)
        self.messages_seen.append(messages)
        self.backend.history.append({"role": "user", "content": str(messages)})
        response = self.responses[self.calls]
        self.calls += 1
        return response


class DummyHandler(BaseHandler):
    def __init__(self) -> None:
        super().__init__(ctx=AgentContext())

    def exec_echo(self, args):
        return ActionResult(
            data={"echo": args["value"]},
            next_prompt="继续",
        )

    def exec_interrupt(self, args):
        _ = args
        return ActionResult(data={"status": "INTERRUPT"}, next_prompt="")


class RecordingEvidenceObserver:
    def __init__(self) -> None:
        self.events: list[tuple] = []

    def provider_call_started(self, *, turn: int) -> None:
        self.events.append(("provider_start", turn))

    def provider_call_finished(
        self,
        *,
        turn: int,
        receipt,
    ) -> None:
        self.events.append(("provider_finish", turn, receipt))

    def provider_call_failed(self, *, turn: int) -> None:
        self.events.append(("provider_failed", turn))

    def tool_call_started(
        self,
        *,
        turn: int,
        tool_name: str,
        tool_call_id: str,
    ) -> None:
        self.events.append(
            ("tool_start", turn, tool_name, tool_call_id)
        )

    def tool_call_finished(
        self,
        *,
        turn: int,
        tool_name: str,
        tool_call_id: str,
        receipt,
    ) -> None:
        self.events.append(
            (
                "tool_finish",
                turn,
                tool_name,
                tool_call_id,
                receipt,
            )
        )

    def tool_call_failed(
        self,
        *,
        turn: int,
        tool_name: str,
        tool_call_id: str,
    ) -> None:
        self.events.append(
            ("tool_failed", turn, tool_name, tool_call_id)
        )


class AgentLoopTests(unittest.TestCase):
    def test_preflight_short_circuit_always_runs_tool_finalizer(self) -> None:
        finalized: list[str] = []

        class _PreflightHandler(DummyHandler):
            def tool_before_callback(self, tool_name, args):  # noqa: ANN001
                del args
                return ActionResult(
                    data={"status": "SKIP"},
                    next_prompt="denied",
                )

            def tool_finally_callback(self, tool_name):  # noqa: ANN001
                finalized.append(tool_name)

        result = exhaust(
            _PreflightHandler().dispatch(
                "echo",
                {"value": "must-not-run"},
            )
        )

        self.assertEqual(result.data["status"], "SKIP")
        self.assertEqual(finalized, ["echo"])

    def test_run_agent_loop_exits_on_plain_text_response(self) -> None:
        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="任务完成",
                    tool_calls=[],
                    usage=TokenUsage(
                        input_tokens=4,
                        output_tokens=2,
                        total_tokens=6,
                    ),
                )
            ]
        )
        handler = DummyHandler()

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="finish",
            handler=handler,
            tools_schema=[],
            max_turns=5,
        )

        self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")
        self.assertEqual(result["response"], "任务完成")
        self.assertEqual(
            result["usage"],
            {"input_tokens": 4, "output_tokens": 2, "total_tokens": 6},
        )

    def test_receipts_are_observed_out_of_band_not_in_tool_result(
        self,
    ) -> None:
        provider_receipt_1 = object()
        provider_receipt_2 = object()
        tool_receipt = object()
        observer = RecordingEvidenceObserver()

        class ReceiptedHandler(DummyHandler):
            def exec_echo(self, args):
                return ActionResult(
                    data={"echo": args["value"]},
                    next_prompt="continue",
                    tool_receipt=tool_receipt,
                )

        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="",
                    tool_calls=[
                        ToolCall(
                            name="echo",
                            args={"value": "safe"},
                            id="call-1",
                        )
                    ],
                    provider_receipt=provider_receipt_1,
                ),
                ChatResponse(
                    thinking="",
                    content="done",
                    tool_calls=[],
                    provider_receipt=provider_receipt_2,
                ),
            ]
        )
        handler = ReceiptedHandler()
        handler.ctx.execution_evidence_observer = observer

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=3,
        )

        self.assertEqual(
            observer.events,
            [
                ("provider_start", 1),
                ("provider_finish", 1, provider_receipt_1),
                ("tool_start", 1, "echo", "call-1"),
                (
                    "tool_finish",
                    1,
                    "echo",
                    "call-1",
                    tool_receipt,
                ),
                ("provider_start", 2),
                ("provider_finish", 2, provider_receipt_2),
            ],
        )
        self.assertNotIn("tool_receipt", result["tool_results"][0])
        self.assertNotIn(
            repr(tool_receipt),
            repr(result["tool_results"]),
        )
        self.assertNotIn(
            repr(provider_receipt_1),
            repr(client.responses[0]),
        )
        self.assertNotIn(
            repr(tool_receipt),
            repr(
                ActionResult(
                    data={"status": "OK"},
                    next_prompt=None,
                    tool_receipt=tool_receipt,
                )
            ),
        )

    def test_evidence_observer_failure_is_sanitized_before_provider(
        self,
    ) -> None:
        secret = "observer-secret"

        class ExplodingObserver(RecordingEvidenceObserver):
            def provider_call_started(self, *, turn: int) -> None:
                del turn
                raise RuntimeError(secret)

        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="done",
                    tool_calls=[],
                )
            ]
        )
        handler = DummyHandler()
        handler.ctx.execution_evidence_observer = ExplodingObserver()

        with self.assertRaises(
            ExecutionEvidenceObservationError
        ) as raised:
            run_agent_loop(
                client=client,
                system_prompt="sys",
                user_input="go",
                handler=handler,
                tools_schema=[],
                max_turns=1,
            )

        self.assertEqual(
            str(raised.exception),
            "execution_evidence_observation_failed",
        )
        self.assertNotIn(secret, repr(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)
        self.assertEqual(client.calls, 0)

        class LookupExplodingObserver:
            def __getattribute__(self, name):
                if name == "provider_call_started":
                    raise RuntimeError(secret)
                return super().__getattribute__(name)

        lookup_client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="done",
                    tool_calls=[],
                )
            ]
        )
        lookup_handler = DummyHandler()
        lookup_handler.ctx.execution_evidence_observer = (
            LookupExplodingObserver()
        )
        with self.assertRaises(
            ExecutionEvidenceObservationError
        ) as lookup_raised:
            run_agent_loop(
                client=lookup_client,
                system_prompt="sys",
                user_input="go",
                handler=lookup_handler,
                tools_schema=[],
                max_turns=1,
            )

        self.assertEqual(
            str(lookup_raised.exception),
            "execution_evidence_observation_failed",
        )
        self.assertNotIn(secret, repr(lookup_raised.exception))
        self.assertIsNone(lookup_raised.exception.__cause__)
        self.assertIsNone(lookup_raised.exception.__context__)
        self.assertEqual(lookup_client.calls, 0)

    def test_process_control_exceptions_bypass_evidence_callbacks(
        self,
    ) -> None:
        class InterruptingClient(DummyClient):
            def chat(self, messages, tools):
                del messages, tools
                self.calls += 1
                raise KeyboardInterrupt

        observer = RecordingEvidenceObserver()
        handler = DummyHandler()
        handler.ctx.execution_evidence_observer = observer

        with self.assertRaises(KeyboardInterrupt):
            run_agent_loop(
                client=InterruptingClient([]),
                system_prompt="sys",
                user_input="go",
                handler=handler,
                tools_schema=[],
                max_turns=1,
            )

        self.assertEqual(observer.events, [("provider_start", 1)])

    def test_provider_and_tool_failures_close_observations(self) -> None:
        class FailingClient(DummyClient):
            def chat(self, messages, tools):
                del messages, tools
                self.calls += 1
                raise RuntimeError("provider failed")

        provider_observer = RecordingEvidenceObserver()
        provider_handler = DummyHandler()
        provider_handler.ctx.execution_evidence_observer = (
            provider_observer
        )
        with self.assertRaisesRegex(RuntimeError, "provider failed"):
            run_agent_loop(
                client=FailingClient([]),
                system_prompt="sys",
                user_input="go",
                handler=provider_handler,
                tools_schema=[],
                max_turns=1,
            )
        self.assertEqual(
            provider_observer.events,
            [
                ("provider_start", 1),
                ("provider_failed", 1),
            ],
        )

        class FailingHandler(DummyHandler):
            def exec_boom(self, args):
                del args
                raise RuntimeError("tool failed")

        tool_observer = RecordingEvidenceObserver()
        tool_handler = FailingHandler()
        tool_handler.ctx.execution_evidence_observer = tool_observer
        with self.assertRaisesRegex(RuntimeError, "tool failed"):
            run_agent_loop(
                client=DummyClient(
                    [
                        ChatResponse(
                            thinking="",
                            content="",
                            tool_calls=[
                                ToolCall(
                                    name="boom",
                                    args={},
                                    id="call-1",
                                )
                            ],
                        )
                    ]
                ),
                system_prompt="sys",
                user_input="go",
                handler=tool_handler,
                tools_schema=[],
                max_turns=1,
            )
        self.assertEqual(
            tool_observer.events,
            [
                ("provider_start", 1),
                ("provider_finish", 1, None),
                ("tool_start", 1, "boom", "call-1"),
                ("tool_failed", 1, "boom", "call-1"),
            ],
        )

    def test_dual_failures_do_not_retain_sensitive_exception_context(
        self,
    ) -> None:
        provider_secret = "provider-sensitive-detail"
        tool_secret = "tool-sensitive-detail"
        observer_secret = "observer-sensitive-detail"

        class FailingClient(DummyClient):
            def chat(self, messages, tools):
                del messages, tools
                self.calls += 1
                raise RuntimeError(provider_secret)

        class FailingHandler(DummyHandler):
            def exec_boom(self, args):
                del args
                raise RuntimeError(tool_secret)

        class DualFailureObserver(RecordingEvidenceObserver):
            def provider_call_failed(self, *, turn: int) -> None:
                del turn
                raise RuntimeError(observer_secret)

            def tool_call_failed(
                self,
                *,
                turn: int,
                tool_name: str,
                tool_call_id: str,
            ) -> None:
                del turn, tool_name, tool_call_id
                raise RuntimeError(observer_secret)

        provider_handler = DummyHandler()
        provider_handler.ctx.execution_evidence_observer = (
            DualFailureObserver()
        )
        with self.assertRaises(
            ExecutionEvidenceObservationError
        ) as provider_raised:
            run_agent_loop(
                client=FailingClient([]),
                system_prompt="sys",
                user_input="go",
                handler=provider_handler,
                tools_schema=[],
                max_turns=1,
            )

        tool_handler = FailingHandler()
        tool_handler.ctx.execution_evidence_observer = DualFailureObserver()
        with self.assertRaises(
            ExecutionEvidenceObservationError
        ) as tool_raised:
            run_agent_loop(
                client=DummyClient(
                    [
                        ChatResponse(
                            thinking="",
                            content="",
                            tool_calls=[
                                ToolCall(
                                    name="boom",
                                    args={},
                                    id="call-1",
                                )
                            ],
                        )
                    ]
                ),
                system_prompt="sys",
                user_input="go",
                handler=tool_handler,
                tools_schema=[],
                max_turns=1,
            )

        for raised in (provider_raised, tool_raised):
            self.assertEqual(
                str(raised.exception),
                "execution_evidence_observation_failed",
            )
            self.assertIsNone(raised.exception.__cause__)
            self.assertIsNone(raised.exception.__context__)
            for secret in (
                provider_secret,
                tool_secret,
                observer_secret,
            ):
                self.assertNotIn(secret, repr(raised.exception))

    def test_run_agent_loop_resets_tools_on_unknown_tool(self) -> None:
        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="",
                    tool_calls=[ToolCall(name="missing_tool", args={}, id="1")],
                ),
                ChatResponse(thinking="", content="done", tool_calls=[]),
            ]
        )
        handler = DummyHandler()

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
        )

        self.assertEqual(client.last_tools, "")
        self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")

    def test_run_agent_loop_retries_tool_intent_without_call(self) -> None:
        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="web_download 不可用，通过 code_run 下载报告后继续分析。",
                    tool_calls=[],
                ),
                ChatResponse(thinking="", content="done", tool_calls=[]),
            ]
        )
        handler = DummyHandler()

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[{"type": "function", "function": {"name": "code_run"}}],
            max_turns=5,
        )

        self.assertEqual(client.calls, 2)
        self.assertEqual(result["tool_results"][0]["data"]["status"], "TOOL_INTENT_WITHOUT_CALL")
        self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")

    def test_run_agent_loop_retries_unclosed_tool_use_block(self) -> None:
        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content='<tool_use>{"name":"echo","arguments":{"value":"ok"}}',
                    tool_calls=[],
                ),
                ChatResponse(thinking="", content="done", tool_calls=[]),
            ]
        )
        handler = DummyHandler()

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[{"type": "function", "function": {"name": "echo"}}],
            max_turns=5,
        )

        self.assertEqual(client.calls, 2)
        self.assertEqual(result["tool_results"][0]["data"]["status"], "TOOL_INTENT_WITHOUT_CALL")
        self.assertEqual(result["tool_results"][0]["data"]["tool"], "malformed_tool_use")

    def test_run_agent_loop_collects_tool_results(self) -> None:
        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="",
                    tool_calls=[ToolCall(name="echo", args={"value": "ok"}, id="1")],
                ),
                ChatResponse(thinking="", content="done", tool_calls=[]),
            ]
        )
        handler = DummyHandler()

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
        )

        self.assertEqual(result["tool_results"][0]["data"]["echo"], "ok")
        self.assertEqual(result["response"], "done")

    def test_recorded_policy_metadata_does_not_enter_model_context(self) -> None:
        class _PolicyHandler(DummyHandler):
            def exec_echo(self, args):  # noqa: ANN001
                self.ctx.last_policy_decision = {
                    "tool_name": "echo",
                    "outcome": "allow",
                    "outcomes": ["require_approval", "allow"],
                    "reason_code": "approved",
                }
                return super().exec_echo(args)

        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="",
                    tool_calls=[ToolCall(name="echo", args={"value": "ok"}, id="1")],
                ),
                ChatResponse(thinking="", content="done", tool_calls=[]),
            ]
        )

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=_PolicyHandler(),
            tools_schema=[],
            max_turns=5,
        )

        self.assertEqual(
            result["tool_results"][0]["policy"]["outcomes"],
            ["require_approval", "allow"],
        )
        self.assertNotIn("policy", client.messages_seen[1][0]["tool_results"][0])

    def test_run_agent_loop_emits_progress_messages(self) -> None:
        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="",
                    tool_calls=[ToolCall(name="echo", args={"value": "ok"}, id="1")],
                ),
                ChatResponse(thinking="", content="done", tool_calls=[]),
            ]
        )
        progress: list[str] = []
        handler = DummyHandler()
        handler.ctx.display_fn = progress.append

        run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
        )

        self.assertIn("[Turn 1]", progress)
        self.assertIn("  tool: echo", progress)
        self.assertIn("[Done] exit_reason=CURRENT_TASK_DONE, turns=2", progress)

    def test_run_agent_loop_retries_on_empty_response(self) -> None:
        client = DummyClient(
            [
                ChatResponse(thinking="", content="", tool_calls=[]),
                ChatResponse(thinking="", content="任务完成", tool_calls=[]),
            ]
        )
        handler = DummyHandler()

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
        )

        self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")
        self.assertEqual(result["tool_results"][0]["data"]["status"], "EMPTY_RESPONSE")
        self.assertEqual(result["tool_results"][0]["data"]["empty_count"], 1)
        self.assertEqual(result["response"], "任务完成")

    def test_run_agent_loop_exits_after_three_empty_responses(self) -> None:
        client = DummyClient(
            [
                ChatResponse(thinking="", content="", tool_calls=[]),
                ChatResponse(thinking="", content="", tool_calls=[]),
                ChatResponse(thinking="", content="", tool_calls=[]),
            ]
        )
        handler = DummyHandler()

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
        )

        self.assertEqual(result["exit_reason"], "EXITED")
        self.assertEqual(result["tool_results"][-1]["data"]["empty_count"], 3)

    def test_run_agent_loop_retries_on_truncated_response(self) -> None:
        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="半截结果",
                    tool_calls=[],
                    raw="半截结果",
                    stop_reason="max_tokens",
                ),
                ChatResponse(thinking="", content="done", tool_calls=[]),
            ]
        )
        handler = DummyHandler()

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
        )

        self.assertEqual(result["tool_results"][0]["data"]["status"], "RETRYABLE_RESPONSE_ERROR")
        self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")
        self.assertEqual(result["response"], "done")

    def test_run_agent_loop_retries_when_code_block_has_no_tool_call(self) -> None:
        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="```python\nprint(1)\n```",
                    tool_calls=[],
                ),
                ChatResponse(thinking="", content="done", tool_calls=[]),
            ]
        )
        handler = DummyHandler()

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
        )

        self.assertEqual(result["tool_results"][0]["data"]["status"], "CODE_BLOCK_WITHOUT_TOOL")
        self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")

    def test_run_agent_loop_exits_on_empty_next_prompt(self) -> None:
        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="",
                    tool_calls=[ToolCall(name="interrupt", args={}, id="1")],
                )
            ]
        )
        handler = DummyHandler()

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
        )

        self.assertEqual(result["exit_reason"], "EXITED")

    def test_run_agent_loop_injects_anchor_prompt_from_summary_history(self) -> None:
        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="",
                    tool_calls=[ToolCall(name="echo", args={"value": "ok"}, id="1")],
                    raw="<summary>第一轮摘要</summary>",
                ),
                ChatResponse(thinking="", content="done", tool_calls=[]),
            ]
        )
        handler = XAgentHandler(ctx=AgentContext())
        handler.ctx.working["key_info"] = "关键上下文"

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
        )

        second_turn_message = client.messages_seen[1][0]["content"]
        self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")
        self.assertIn("<current_turn>1</current_turn>", second_turn_message)
        self.assertIn("<key_info>\n关键上下文\n</key_info>", second_turn_message)
        self.assertIn("<history>\n[Agent] 第一轮摘要\n</history>", second_turn_message)
        self.assertEqual(handler.ctx.history_info, ["[Agent] 第一轮摘要"])

    def test_align_history_info_keeps_recent_entries_only(self) -> None:
        handler = XAgentHandler(ctx=AgentContext())
        handler.ctx.history_info = [f"[Agent] {index}" for index in range(20)]

        handler.align_history_info(session_history_size=5)

        self.assertEqual(len(handler.ctx.history_info), 10)
        self.assertEqual(handler.ctx.history_info[0], "[Agent] 10")

    def test_get_anchor_prompt_folds_earlier_history(self) -> None:
        handler = XAgentHandler(ctx=AgentContext(current_turn=7))
        handler.ctx.history_info = [f"[Agent] {index}" for index in range(35)]

        prompt = handler.get_anchor_prompt()

        self.assertIsNotNone(prompt)
        assert prompt is not None
        self.assertIn("<current_turn>7</current_turn>", prompt)
        self.assertIn("[...前 5 条摘要已折叠]", prompt)
        self.assertIn("[Agent] 34", prompt)


    def test_run_agent_loop_interrupts_on_stop_event(self) -> None:
        stop_event = threading.Event()
        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="",
                    tool_calls=[ToolCall(name="echo", args={"value": "ok"}, id="1")],
                ),
                ChatResponse(thinking="", content="done", tool_calls=[]),
            ]
        )
        handler = DummyHandler()
        stop_event.set()

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
            stop_event=stop_event,
        )

        self.assertEqual(result["exit_reason"], "INTERRUPTED")

    def test_stop_set_during_llm_call_blocks_returned_tool_calls(self) -> None:
        stop_event = threading.Event()
        handler = DummyHandler()
        dispatched: list[dict] = []

        def _exec_echo(args):
            dispatched.append(args)
            return ActionResult(data={"status": "OK"}, next_prompt="continue")

        handler.exec_echo = _exec_echo  # type: ignore[method-assign]

        class _StoppingClient:
            def chat(self, messages, tools):
                del messages, tools
                stop_event.set()
                return ChatResponse(
                    thinking="",
                    content="",
                    tool_calls=[ToolCall(name="echo", args={"value": "late"}, id="1")],
                )

        result = run_agent_loop(
            client=_StoppingClient(),
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
            stop_event=stop_event,
        )

        self.assertEqual(result["exit_reason"], "INTERRUPTED")
        self.assertEqual(dispatched, [])

    def test_run_agent_loop_interrupts_on_code_stop_signal(self) -> None:
        client = DummyClient(
            [
                ChatResponse(
                    thinking="",
                    content="",
                    tool_calls=[ToolCall(name="echo", args={"value": "ok"}, id="1")],
                ),
                ChatResponse(thinking="", content="done", tool_calls=[]),
            ]
        )
        handler = DummyHandler()
        handler.ctx.code_stop_signal = True

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
        )

        self.assertEqual(result["exit_reason"], "INTERRUPTED")

    def test_code_stop_signal_is_reset_after_interrupt(self) -> None:
        client = DummyClient(
            [
                ChatResponse(thinking="", content="first", tool_calls=[]),
            ]
        )
        handler = DummyHandler()
        handler.ctx.code_stop_signal = True

        result = run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=3,
        )

        self.assertEqual(result["exit_reason"], "INTERRUPTED")
        self.assertFalse(handler.ctx.code_stop_signal)


if __name__ == "__main__":
    unittest.main()
