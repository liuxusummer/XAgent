from __future__ import annotations

import unittest

from src.core.agent_loop import (
    ActionResult,
    AgentContext,
    BaseHandler,
    DurableTurnCommitError,
    DurableTurnRecoveryError,
    run_agent_loop,
)
from src.core.llm import ChatResponse, TokenUsage, ToolCall


_LEAK = "durable-turn-secret-diagnostic"


class _Client:
    def __init__(self) -> None:
        self.responses = [
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
                stop_reason="tool_use",
                usage=TokenUsage(
                    input_tokens=4,
                    output_tokens=2,
                    total_tokens=6,
                ),
            ),
            ChatResponse(
                thinking="",
                content="done",
                tool_calls=[],
                stop_reason="end_turn",
                usage=TokenUsage(
                    input_tokens=5,
                    output_tokens=1,
                    total_tokens=6,
                ),
            ),
        ]
        self.calls: list[list[dict]] = []
        self.last_tools = ""

    def chat(self, messages, tools):
        self.calls.append(messages)
        return self.responses.pop(0)


class _Handler(BaseHandler):
    def exec_echo(self, args):
        return ActionResult(
            data={"echo": args["value"]},
            next_prompt="continue safely",
        )


class DurableTurnCallbackTests(unittest.TestCase):
    def test_safe_turn_snapshot_is_exact_detached_and_ordered(self) -> None:
        snapshots = []

        def commit(snapshot):
            snapshots.append(snapshot)
            snapshot["next_messages"][0]["content"] = "mutated"

        client = _Client()
        handler = _Handler(
            AgentContext(
                display_fn=lambda _message: None,
                durable_turn_callback=commit,
            )
        )

        result = run_agent_loop(
            client=client,
            system_prompt="system",
            user_input="task",
            handler=handler,
            tools_schema=[{"name": "echo"}],
            max_turns=3,
        )

        self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")
        self.assertEqual(len(snapshots), 1)
        snapshot = snapshots[0]
        self.assertEqual(snapshot["schema_version"], 1)
        self.assertEqual(snapshot["completed_turn"], 1)
        self.assertEqual(snapshot["usage"]["total_tokens"], 6)
        self.assertEqual(snapshot["context"]["empty_count"], 0)
        self.assertEqual(len(client.calls), 2)
        self.assertIn(
            "continue safely",
            client.calls[1][-1]["content"],
        )
        self.assertNotIn("mutated", client.calls[1][-1]["content"])

    def test_callback_failure_stops_before_next_provider_call(self) -> None:
        def fail(_snapshot):
            raise RuntimeError(_LEAK)

        client = _Client()
        handler = _Handler(
            AgentContext(
                display_fn=lambda _message: None,
                durable_turn_callback=fail,
            )
        )

        with self.assertRaises(DurableTurnCommitError) as raised:
            run_agent_loop(
                client=client,
                system_prompt="system",
                user_input="task",
                handler=handler,
                tools_schema=[{"name": "echo"}],
                max_turns=3,
            )

        self.assertEqual(str(raised.exception), "durable_turn_commit_failed")
        self.assertIsNone(raised.exception.__context__)
        self.assertNotIn(_LEAK, repr(raised.exception))
        self.assertEqual(len(client.calls), 1)

    def test_resume_restores_usage_results_and_exact_next_boundary(self) -> None:
        snapshots = []

        def stop_after_commit(snapshot):
            snapshots.append(snapshot)
            raise RuntimeError("simulated worker death")

        first_client = _Client()
        first_handler = _Handler(
            AgentContext(
                display_fn=lambda _message: None,
                session_id="session-1",
                agent_name="main",
                durable_turn_callback=stop_after_commit,
            )
        )
        with self.assertRaises(DurableTurnCommitError):
            run_agent_loop(
                client=first_client,
                system_prompt="system",
                user_input="task",
                handler=first_handler,
                tools_schema=[{"name": "echo"}],
                max_turns=3,
            )

        resumed_client = _Client()
        resumed_client.responses = [resumed_client.responses[1]]
        resumed_handler = _Handler(
            AgentContext(
                display_fn=lambda _message: None,
                session_id="session-1",
                agent_name="main",
            )
        )
        result = run_agent_loop(
            client=resumed_client,
            system_prompt="system",
            user_input="task",
            handler=resumed_handler,
            tools_schema=[{"name": "echo"}],
            max_turns=3,
            resume_state=snapshots[0],
        )

        self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")
        self.assertEqual(result["turns"], 2)
        self.assertEqual(result["usage"]["total_tokens"], 12)
        self.assertEqual(len(result["tool_results"]), 2)
        self.assertEqual(
            result["tool_results"][0]["data"],
            {"echo": "safe"},
        )
        self.assertEqual(len(resumed_client.calls), 1)
        self.assertIn(
            "continue safely",
            resumed_client.calls[0][0]["content"],
        )

        foreign = dict(snapshots[0])
        foreign["context"] = dict(foreign["context"])
        foreign["context"]["session_id"] = "foreign-session"
        rejecting_client = _Client()
        with self.assertRaises(DurableTurnRecoveryError):
            run_agent_loop(
                client=rejecting_client,
                system_prompt="system",
                user_input="task",
                handler=_Handler(
                    AgentContext(
                        display_fn=lambda _message: None,
                        session_id="session-1",
                        agent_name="main",
                    )
                ),
                tools_schema=[{"name": "echo"}],
                max_turns=3,
                resume_state=foreign,
            )
        self.assertEqual(rejecting_client.calls, [])


if __name__ == "__main__":
    unittest.main()
