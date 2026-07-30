from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.core.agent_kernel import ContextKind, Principal, TrustLevel
from src.core.agent_loop import (
    ActionResult,
    AgentContext,
    BaseHandler,
    _prepare_context_messages,
    exhaust,
    run_agent_loop,
)
from src.core.checkpoint import build_task_checkpoint, render_resume_prompt
from src.core.context_builder import ContextBuilder, ContextSource, estimate_tokens
from src.core.llm import BaseSession, ChatResponse, ToolCall


class ContextBuilderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.principal = Principal(
            subject="user-1",
            tenant_id="tenant-1",
            session_id="session-1",
            run_id="run-1",
            scopes=("workspace.read",),
        )

    def test_component_budgets_bound_each_visible_class(self) -> None:
        builder = ContextBuilder(
            max_input_tokens=120,
            reserved_output_tokens=20,
            component_budgets={
                ContextKind.SYSTEM: 30,
                ContextKind.TASK_STATE: 20,
                ContextKind.RECENT_HISTORY: 10,
                ContextKind.COMPACTED_HISTORY: 5,
                ContextKind.RETRIEVAL_EVIDENCE: 10,
                ContextKind.MEMORY: 5,
                ContextKind.TOOL_RESULT: 15,
                ContextKind.SKILL: 5,
            },
        )
        sources = (
            ContextSource(
                "system",
                ContextKind.SYSTEM,
                "system " * 100,
                TrustLevel.SYSTEM,
                100,
            ),
            ContextSource(
                "tool",
                ContextKind.TOOL_RESULT,
                "untrusted " * 100,
                TrustLevel.TOOL_UNTRUSTED,
                50,
            ),
        )

        result = builder.build(
            principal=self.principal,
            manifest_id="manifest-1",
            sources=sources,
        )

        self.assertLessEqual(result.component_usage["system"], 30)
        self.assertLessEqual(result.component_usage["tool_result"], 15)
        self.assertLessEqual(result.manifest.visible_token_count, 100)
        self.assertEqual(len(result.compaction), 2)
        self.assertNotIn("untrusted " * 20, str(result.context_state()))

    def test_local_state_is_recorded_but_never_rendered(self) -> None:
        builder = ContextBuilder(max_input_tokens=100, reserved_output_tokens=20)
        result = builder.build(
            principal=self.principal,
            manifest_id="manifest-2",
            sources=(
                ContextSource(
                    "checkpoint-local",
                    ContextKind.TASK_STATE,
                    "secret local recovery state",
                    TrustLevel.SYSTEM,
                    100,
                    llm_visible=False,
                ),
            ),
        )

        self.assertEqual(result.visible_content, {})
        self.assertFalse(result.manifest.items[0].llm_visible)
        self.assertEqual(
            result.manifest.items[0].token_count,
            estimate_tokens("secret local recovery state"),
        )
        self.assertNotIn("secret local recovery state", str(result.context_state()))

    def test_higher_priority_source_wins_shared_component_budget(self) -> None:
        budgets = {kind: 0 for kind in ContextKind}
        budgets[ContextKind.RETRIEVAL_EVIDENCE] = 10
        builder = ContextBuilder(
            max_input_tokens=30,
            reserved_output_tokens=10,
            component_budgets=budgets,
        )
        result = builder.build(
            principal=self.principal,
            manifest_id="manifest-3",
            sources=(
                ContextSource(
                    "low",
                    ContextKind.RETRIEVAL_EVIDENCE,
                    "low " * 20,
                    TrustLevel.RETRIEVED,
                    10,
                ),
                ContextSource(
                    "high",
                    ContextKind.RETRIEVAL_EVIDENCE,
                    "high " * 20,
                    TrustLevel.RETRIEVED,
                    90,
                ),
            ),
        )

        self.assertIn("high", result.visible_content)
        self.assertNotIn("low", result.visible_content)
        self.assertEqual(result.manifest.principal_digest, self.principal.principal_digest)

    def test_agent_loop_compacts_large_tool_output_and_checkpoints_metadata(self) -> None:
        class Handler(BaseHandler):
            def exec_echo(self, args):
                del args
                return ActionResult(
                    data={"status": "OK", "content": "secret-payload-" * 20_000},
                    next_prompt="continue",
                )

        class Client:
            def __init__(self):
                self.calls = []

            def chat(self, messages, tools):
                del tools
                self.calls.append(messages)
                if len(self.calls) == 1:
                    return ChatResponse(
                        thinking="",
                        content="",
                        tool_calls=[ToolCall("echo", {}, "call-1")],
                    )
                return ChatResponse(
                    thinking="",
                    content="done",
                    tool_calls=[],
                )

        snapshots: list[dict] = []
        ctx = AgentContext(
            principal=self.principal,
            session_id="session-1",
            checkpoint_callback=snapshots.append,
        )
        client = Client()
        result = run_agent_loop(
            client=client,
            system_prompt="system",
            user_input="task",
            handler=Handler(ctx),
            tools_schema=[
                {"type": "function", "function": {"name": "echo"}}
            ],
            max_turns=2,
        )

        self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")
        rendered_data = client.calls[1][0]["tool_results"][0]["data"]
        self.assertEqual(rendered_data["status"], "CONTEXT_TRUNCATED")
        self.assertNotIn("secret-payload-" * 1_000, str(rendered_data))
        self.assertTrue(
            any(
                item["kind"] == ContextKind.TOOL_RESULT.value
                for item in ctx.context_state["compaction"]
            )
        )
        with tempfile.TemporaryDirectory() as tmp:
            checkpoint = build_task_checkpoint(
                Path(tmp),
                checkpoint_id="context",
                session_id="session-1",
                task="task",
                context_manifest_digest=ctx.context_manifest.manifest_digest,
                context_state=ctx.context_state,
            )
        serialized = json.dumps(checkpoint)
        self.assertNotIn("secret-payload", serialized)
        self.assertIn("Context recovery", render_resume_prompt(checkpoint))

    def test_session_history_uses_token_budget_and_records_hashes(self) -> None:
        session = BaseSession(
            api_key="",
            base_url="",
            model="",
            context_window_chars=120,
        )
        session.history = [
            {"role": "user", "content": f"message-{index}-" + ("x" * 90)}
            for index in range(8)
        ]

        session._trim_history()

        remaining = sum(
            estimate_tokens(str(item.get("content", "")))
            for item in session.history
        )
        self.assertLessEqual(remaining, 40)
        self.assertTrue(session.history_compaction)
        self.assertTrue(
            all(
                len(item["source_sha256"]) == 64
                and "message-" not in str(item)
                for item in session.history_compaction
            )
        )

    def test_context_safely_compacts_malformed_and_excess_tool_results(self) -> None:
        class Client:
            backend = None

        results = [
            {
                "tool_name": "external",
                "tool_call_id": f"call-{index}",
                "data": (
                    {"score": float("nan")}
                    if index == 0
                    else {"value": index}
                ),
            }
            for index in range(300)
        ]
        ctx = AgentContext(principal=self.principal, session_id="session-1")

        prepared = _prepare_context_messages(
            [{"role": "user", "content": "continue", "tool_results": results}],
            ctx=ctx,
            client=Client(),
            turn=2,
        )

        rendered_results = prepared[0]["tool_results"]
        self.assertEqual(
            rendered_results[0]["data"]["status"],
            "CONTEXT_UNSERIALIZABLE",
        )
        self.assertEqual(
            rendered_results[-1]["data"]["status"],
            "CONTEXT_SOURCE_LIMIT",
        )
        self.assertGreater(rendered_results[-1]["data"]["omitted_count"], 0)
        self.assertLessEqual(len(ctx.context_manifest.items), 240)
        json.dumps(prepared, allow_nan=False)

    def test_tool_history_is_never_marked_as_user_trusted(self) -> None:
        class Backend:
            history = [{"role": "tool", "content": "external result"}]
            context_window_chars = 24_000
            max_tokens = 4_096

        class Client:
            backend = Backend()

        ctx = AgentContext(principal=self.principal, session_id="session-1")
        _prepare_context_messages(
            [{"role": "user", "content": "continue"}],
            ctx=ctx,
            client=Client(),
            turn=2,
        )

        history_item = next(
            item
            for item in ctx.context_manifest.items
            if "session-history" in item.ref_id
        )
        self.assertEqual(history_item.trust, TrustLevel.TOOL_UNTRUSTED)

    def test_truncated_tool_history_stays_untrusted_on_next_turn(self) -> None:
        class Backend:
            history = [{"role": "tool", "content": "external " * 10_000}]
            context_window_chars = 1_000
            max_tokens = 256

        class Client:
            backend = Backend()

        ctx = AgentContext(principal=self.principal, session_id="session-1")
        for turn in (2, 3):
            _prepare_context_messages(
                [{"role": "user", "content": "continue"}],
                ctx=ctx,
                client=Client(),
                turn=turn,
            )
            history_item = next(
                item
                for item in ctx.context_manifest.items
                if "session-history" in item.ref_id
            )
            self.assertEqual(
                history_item.trust,
                TrustLevel.TOOL_UNTRUSTED,
            )
        self.assertEqual(Client.backend.history[0]["role"], "user")
        self.assertIn(
            "<untrusted_tool_history>",
            Client.backend.history[0]["content"],
        )

    def test_oversized_working_memory_is_rejected_without_mutation(self) -> None:
        from src.handler import XAgentHandler

        principal = Principal(
            subject="user-1",
            tenant_id="tenant-1",
            session_id="session-1",
            run_id="run-1",
            scopes=("state.write",),
        )
        ctx = AgentContext(principal=principal, session_id="session-1")
        handler = XAgentHandler(ctx=ctx)

        result = exhaust(
            handler.dispatch(
                "update_working_checkpoint",
                {"key_info": "x" * 8_001},
            )
        )

        self.assertEqual(result.data["status"], "ERROR")
        self.assertEqual(ctx.working["key_info"], "")


if __name__ == "__main__":
    unittest.main()
