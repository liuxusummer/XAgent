from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.core.agent_kernel import Principal
from src.core.agent_loop import AgentContext, run_agent_loop
from src.core.llm import ChatResponse, ToolCall
from src.core.local_policy import LOCAL_PRINCIPAL_SCOPES
from src.core.memory_store import MemoryReviewStatus, MemoryStore
from src.core.telemetry import Event
from src.handler import XAgentHandler


class _Sink:
    def __init__(self) -> None:
        self.events: list[Event] = []

    def emit(self, event: Event) -> None:
        self.events.append(event)

    def close(self) -> None:
        return None


class AgentKernelEndToEndTests(unittest.TestCase):
    def test_source_to_context_to_policy_to_memory_trace_is_complete(self) -> None:
        class Client:
            def __init__(self) -> None:
                self.calls = 0
                self.evidence_id = ""

            def chat(self, messages, tools):  # noqa: ANN001
                del tools
                self.calls += 1
                if self.calls == 1:
                    return ChatResponse(
                        thinking="",
                        content="",
                        tool_calls=[
                            ToolCall(
                                name="file_search",
                                args={"query": "atomic-marker", "mode": "keyword"},
                                id="search-1",
                            )
                        ],
                    )
                if self.calls == 2:
                    result = messages[0]["tool_results"][0]["data"]
                    self.evidence_id = result["matches"][0]["evidence_id"]
                    return ChatResponse(
                        thinking="",
                        content="",
                        tool_calls=[
                            ToolCall(
                                name="memory_propose",
                                args={
                                    "content": "Use atomic writes for shared state.",
                                    "kind": "procedural",
                                    "confidence": 0.9,
                                    "source_refs": [self.evidence_id],
                                },
                                id="memory-1",
                            )
                        ],
                    )
                return ChatResponse(
                    thinking="",
                    content="done",
                    tool_calls=[],
                )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "source.txt").write_text(
                "atomic-marker: shared state uses atomic writes\n",
                encoding="utf-8",
            )
            principal = Principal(
                subject="user-1",
                tenant_id="tenant-1",
                session_id="session-1",
                run_id="run-1",
                scopes=LOCAL_PRINCIPAL_SCOPES,
            )
            sink = _Sink()
            handler = XAgentHandler(
                ctx=AgentContext(
                    cwd=tmp,
                    memory_root=tmp,
                    session_id="session-1",
                    principal=principal,
                    sink=sink,
                )
            )
            client = Client()

            result = run_agent_loop(
                client=client,
                system_prompt="system",
                user_input="Find a supported procedure and propose it as memory.",
                handler=handler,
                tools_schema=[
                    {
                        "type": "function",
                        "function": {"name": "file_search"},
                    },
                    {
                        "type": "function",
                        "function": {"name": "memory_propose"},
                    },
                ],
                max_turns=3,
            )

            self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")
            memory_result = next(
                item
                for item in result["tool_results"]
                if item["tool_name"] == "memory_propose"
            )["data"]
            candidate = MemoryStore(tmp).get_candidate(
                memory_result["candidate_id"],
                principal=principal,
            )
            self.assertIsNotNone(candidate)
            assert candidate is not None
            self.assertEqual(candidate.review_status, MemoryReviewStatus.PENDING)
            self.assertIn(client.evidence_id, candidate.source_refs)
            self.assertEqual(
                MemoryStore(tmp).active_records(principal=principal),
                (),
            )

            retrieval = next(
                event for event in sink.events if event.kind == "retrieval_evidence"
            )
            self.assertEqual(len(retrieval.data["query_plan_digest"]), 64)
            self.assertEqual(retrieval.data["rerank_version"], 1)
            self.assertEqual(retrieval.data["stale_rejected"], 0)
            self.assertGreaterEqual(
                retrieval.data["covered_query_terms"],
                1,
            )
            context = next(
                event
                for event in sink.events
                if event.kind == "context_manifest" and event.turn == 2
            )
            memory = next(
                event
                for event in sink.events
                if event.kind == "memory_candidate_created"
            )
            policy = [
                event
                for event in sink.events
                if event.kind == "policy_decision"
                and event.name == "memory_propose"
            ][-1]
            tool_end = next(
                event
                for event in sink.events
                if event.kind == "tool_end"
                and event.name == "memory_propose"
            )

            self.assertTrue(retrieval.data["bundle_digest"])
            self.assertEqual(
                memory.data["context_manifest_digest"],
                context.data["manifest_digest"],
            )
            self.assertEqual(
                memory.data["action_digest"],
                policy.data["action_digest"],
            )
            self.assertEqual(
                tool_end.data["action_digest"],
                policy.data["action_digest"],
            )
            self.assertEqual(
                tool_end.data["context_manifest_digest"],
                context.data["manifest_digest"],
            )


if __name__ == "__main__":
    unittest.main()
