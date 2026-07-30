"""Phase 8 观测性加固：telemetry 模块测试。"""
from __future__ import annotations

import json
import re
import tempfile
import threading
import unittest
from pathlib import Path

from src.core.telemetry import (
    Event,
    JsonlSink,
    MultiSink,
    NullSink,
    StderrSink,
)


class _MemorySink:
    """测试专用 sink：收集事件到 list。"""

    def __init__(self) -> None:
        self.events: list[Event] = []

    def emit(self, event: Event) -> None:
        self.events.append(event)

    def close(self) -> None:
        return None


class _RaisingSink:
    def emit(self, event: Event) -> None:  # noqa: ARG002
        raise RuntimeError("boom")

    def close(self) -> None:
        raise RuntimeError("boom close")


def _mk_event(session_id: str = "s1", turn: int = 1, kind: str = "test") -> Event:
    return Event(session_id=session_id, turn=turn, kind=kind, name="n")


class NullSinkTests(unittest.TestCase):
    def test_null_sink_is_noop(self) -> None:
        sink = NullSink()
        for _ in range(100):
            sink.emit(_mk_event())
        sink.close()


class JsonlSinkTests(unittest.TestCase):
    def test_jsonl_sink_writes_parseable_line(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sink = JsonlSink(tmp)
            event = _mk_event(session_id="abc")
            sink.emit(event)
            sink.close()

            path = Path(tmp) / "abc.jsonl"
            self.assertTrue(path.exists())
            lines = path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 1)
            parsed = json.loads(lines[0])
            self.assertEqual(parsed["session_id"], "abc")
            self.assertEqual(parsed["kind"], "test")

    def test_jsonl_sink_splits_by_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sink = JsonlSink(tmp)
            sink.emit(_mk_event(session_id="s1"))
            sink.emit(_mk_event(session_id="s2"))
            sink.close()
            self.assertTrue((Path(tmp) / "s1.jsonl").exists())
            self.assertTrue((Path(tmp) / "s2.jsonl").exists())

    def test_jsonl_sink_thread_safe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sink = JsonlSink(tmp)

            def writer(idx: int) -> None:
                for _ in range(50):
                    sink.emit(_mk_event(session_id="shared", turn=idx))

            threads = [threading.Thread(target=writer, args=(i,)) for i in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            sink.close()

            lines = (Path(tmp) / "shared.jsonl").read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 500)
            # 每行可独立 json 解析（无错行/交错）
            for line in lines:
                parsed = json.loads(line)
                self.assertEqual(parsed["session_id"], "shared")


class MultiSinkTests(unittest.TestCase):
    def test_multi_sink_broadcasts(self) -> None:
        mem1, mem2 = _MemorySink(), _MemorySink()
        multi = MultiSink(mem1, mem2)
        multi.emit(_mk_event())
        self.assertEqual(len(mem1.events), 1)
        self.assertEqual(len(mem2.events), 1)

    def test_multi_sink_isolates_exceptions(self) -> None:
        mem = _MemorySink()
        multi = MultiSink(_RaisingSink(), mem)
        # 第一个 sink 抛异常，第二个仍收到事件
        multi.emit(_mk_event())
        self.assertEqual(len(mem.events), 1)
        # close 同样不上抛
        multi.close()


class StderrSinkTests(unittest.TestCase):
    def test_stderr_sink_emit_does_not_raise(self) -> None:
        sink = StderrSink()
        sink.emit(_mk_event())
        sink.emit(Event(session_id="s", turn=2, kind="k", name="n", duration_ms=12.5))
        sink.close()


class AgentLoopTelemetryTests(unittest.TestCase):
    """Step 4/5：验证 run_agent_loop + dispatch 的埋点序列。"""

    def test_run_agent_loop_emits_expected_event_sequence(self) -> None:
        from src.core.agent_loop import (
            ActionResult,
            AgentContext,
            BaseHandler,
            run_agent_loop,
        )
        from src.core.llm import ChatResponse, TokenUsage, ToolCall

        class _Client:
            last_tools = "cached"

            def __init__(self, responses):
                self.responses = responses
                self.calls = 0
                self.backend = type("B", (), {"history": []})()

            def chat(self, messages, tools):
                _ = (messages, tools)
                response = self.responses[self.calls]
                self.calls += 1
                return response

        class _Handler(BaseHandler):
            def __init__(self, sink):
                super().__init__(ctx=AgentContext(session_id="sess-01", sink=sink))

            def exec_echo(self, args):
                return ActionResult(data={"echo": args.get("v")}, next_prompt="继续")

        mem = _MemorySink()
        client = _Client(
            [
                ChatResponse(
                    thinking="",
                    content="",
                    tool_calls=[ToolCall(name="echo", args={"v": 1}, id="1")],
                    usage=TokenUsage(input_tokens=10, output_tokens=5, total_tokens=15),
                ),
                ChatResponse(
                    thinking="",
                    content="done",
                    tool_calls=[],
                    usage=TokenUsage(input_tokens=7, output_tokens=3, total_tokens=10),
                ),
            ]
        )
        run_agent_loop(
            client=client,
            system_prompt="sys",
            user_input="go",
            handler=_Handler(mem),
            tools_schema=[],
            max_turns=5,
        )

        kinds = [e.kind for e in mem.events]
        # 应该包含 run_start / turn_start / llm_end / tool_start / tool_end / turn_end / run_end
        self.assertIn("run_start", kinds)
        self.assertIn("turn_start", kinds)
        self.assertIn("llm_end", kinds)
        self.assertIn("tool_start", kinds)
        self.assertIn("tool_end", kinds)
        self.assertIn("run_end", kinds)
        # session_id 贯穿
        for event in mem.events:
            self.assertEqual(event.session_id, "sess-01")
        # 第一个是 run_start，最后一个是 run_end
        self.assertEqual(mem.events[0].kind, "run_start")
        self.assertEqual(mem.events[-1].kind, "run_end")
        self.assertEqual(mem.events[-1].name, "CURRENT_TASK_DONE")
        # tool_end 的 duration_ms 不为 None
        tool_end = next(e for e in mem.events if e.kind == "tool_end")
        self.assertIsNotNone(tool_end.duration_ms)
        self.assertEqual(tool_end.name, "echo")
        llm_end = next(e for e in mem.events if e.kind == "llm_end")
        self.assertEqual(llm_end.data["total_tokens"], 15)
        self.assertEqual(mem.events[-1].data["input_tokens"], 17)
        self.assertEqual(mem.events[-1].data["output_tokens"], 8)
        self.assertEqual(mem.events[-1].data["total_tokens"], 25)

    def test_agent_events_include_active_skill_state(self) -> None:
        from src.core.agent_loop import (
            ActionResult,
            AgentContext,
            BaseHandler,
            run_agent_loop,
        )
        from src.core.llm import ChatResponse, ToolCall

        class _Client:
            last_tools = "cached"
            backend = type("B", (), {"history": []})()

            def __init__(self):
                self.calls = 0

            def chat(self, messages, tools):
                _ = (messages, tools)
                self.calls += 1
                if self.calls == 1:
                    return ChatResponse(thinking="", content="", tool_calls=[ToolCall(name="echo", args={}, id="1")])
                return ChatResponse(thinking="", content="done", tool_calls=[])

        class _Handler(BaseHandler):
            def exec_echo(self, args):
                _ = args
                return ActionResult(data={"status": "OK"}, next_prompt="continue")

        mem = _MemorySink()
        handler = _Handler(ctx=AgentContext(session_id="skills", sink=mem, active_skills=["code-review"]))

        run_agent_loop(
            client=_Client(),
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=3,
        )

        for event in mem.events:
            if event.kind in {"run_start", "run_end", "turn_start", "turn_end", "llm_end", "tool_start", "tool_end"}:
                self.assertEqual(event.data["active_skills"], ["code-review"])
                self.assertNotIn("active_skill_count", event.data)

    def test_hook_inject_event_emitted_when_hook_returns_prompt(self) -> None:
        from src.core.agent_loop import AgentContext, BaseHandler, TurnEndHook
        from src.core.llm import ChatResponse

        mem = _MemorySink()
        handler = BaseHandler(ctx=AgentContext(session_id="h", sink=mem))
        handler._turn_end_hooks.append(
            TurnEndHook(name="demo", fn=lambda **_: "PROMPT", priority=1)
        )
        handler.ctx.current_turn = 3
        result = handler.turn_end_callback(
            ChatResponse(thinking="", content="", tool_calls=[])
        )
        self.assertEqual(result, "PROMPT")
        hook_events = [e for e in mem.events if e.kind == "hook_inject"]
        self.assertEqual(len(hook_events), 1)
        self.assertEqual(hook_events[0].name, "demo")
        self.assertEqual(hook_events[0].turn, 3)

    def test_no_tool_completion_emits_turn_end(self) -> None:
        from src.core.agent_loop import AgentContext, BaseHandler, run_agent_loop
        from src.core.llm import ChatResponse

        class _Client:
            def chat(self, messages, tools):
                _ = (messages, tools)
                return ChatResponse(thinking="", content="done", tool_calls=[])

        mem = _MemorySink()
        handler = BaseHandler(ctx=AgentContext(session_id="sess-no-tool", sink=mem))

        run_agent_loop(
            client=_Client(),
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
        )

        turn_end_events = [e for e in mem.events if e.kind == "turn_end"]
        self.assertEqual(len(turn_end_events), 1)
        self.assertEqual(turn_end_events[0].data["tool_count"], 0)
        self.assertLess(
            [e.kind for e in mem.events].index("turn_end"),
            [e.kind for e in mem.events].index("run_end"),
        )

    def test_tool_exit_emits_turn_end(self) -> None:
        from src.core.agent_loop import ActionResult, AgentContext, BaseHandler, run_agent_loop
        from src.core.llm import ChatResponse, ToolCall

        class _Client:
            def chat(self, messages, tools):
                _ = (messages, tools)
                return ChatResponse(
                    thinking="",
                    content="",
                    tool_calls=[ToolCall(name="finish", args={}, id="1")],
                )

        class _Handler(BaseHandler):
            def exec_finish(self, args):
                _ = args
                return ActionResult(data={"ok": True}, next_prompt=None)

        mem = _MemorySink()
        handler = _Handler(ctx=AgentContext(session_id="sess-tool-exit", sink=mem))

        run_agent_loop(
            client=_Client(),
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
        )

        turn_end_events = [e for e in mem.events if e.kind == "turn_end"]
        self.assertEqual(len(turn_end_events), 1)
        self.assertEqual(turn_end_events[0].data["tool_count"], 1)
        kinds = [e.kind for e in mem.events]
        self.assertLess(kinds.index("tool_end"), kinds.index("turn_end"))
        self.assertLess(kinds.index("turn_end"), kinds.index("run_end"))


class XAgentSessionIdTests(unittest.TestCase):
    """Step 3：run_task 入口会刷新 session_id。"""

    def test_run_task_generates_session_id(self) -> None:
        from src.core.agent_kernel import Principal
        from src.core.agent_loop import ActionResult, AgentContext
        from src.core.XAgent import XAgent

        agent = XAgent.__new__(XAgent)
        agent.handler = type("H", (), {})()
        agent.handler.ctx = AgentContext()
        agent._running = threading.Event()
        agent.stop_event = threading.Event()
        agent.client = None
        agent.system_prompt = ""
        agent.tools_schema = []
        agent.display_queue = __import__("queue").Queue()
        agent.principal_template = Principal.local(
            session_id="telemetry-test",
        )

        # 仅测 session_id 注入逻辑，跳过真实 run_agent_loop
        import src.core.XAgent as xa_mod

        original = xa_mod.run_agent_loop
        xa_mod.run_agent_loop = lambda **_: {"exit_reason": "CURRENT_TASK_DONE"}
        try:
            agent.run_task("hello")
        finally:
            xa_mod.run_agent_loop = original

        session_id = agent.handler.ctx.session_id
        # 不依赖具体长度（私有实现细节），只要求非空十六进制串
        self.assertTrue(session_id)
        self.assertIsNotNone(re.fullmatch(r"[0-9a-f]+", session_id))
        _ = ActionResult  # 占位避免未用警告

    def test_xagent_close_closes_sink(self) -> None:
        from src.core.XAgent import XAgent

        class _CloseTrackingSink:
            def __init__(self) -> None:
                self.closed = False

            def emit(self, event):
                _ = event

            def close(self) -> None:
                self.closed = True

        sink = _CloseTrackingSink()
        agent = XAgent.__new__(XAgent)
        agent.sink = sink

        agent.close()

        self.assertTrue(sink.closed)

    def test_xagent_attach_stream_callback_to_backend(self) -> None:
        from src.core.XAgent import XAgent

        agent = XAgent(
            system_prompt="test",
            tools_schema=[],
            api_key="sk-test",
            base_url="https://api.openai.com/v1/chat/completions",
            model="gpt-4o",
        )

        backend = agent.client.backend
        self.assertTrue(callable(getattr(backend, "stream_callback", None)))

    def test_xagent_uses_instance_scoped_display_and_input_bridge(self) -> None:
        from src.core.XAgent import XAgent

        agent = XAgent(
            system_prompt="test",
            tools_schema=[],
            api_key="sk-test",
            base_url="https://api.openai.com/v1/chat/completions",
            model="gpt-4o",
        )

        agent.handler.ctx.display_fn("hello")
        progress = agent.display_queue.get_nowait()
        self.assertEqual(progress, {"progress": "hello"})

        agent.reply_queue.put("approved")
        reply = agent.handler.ctx.user_input_fn("question")
        ask = agent.display_queue.get_nowait()
        self.assertEqual(ask, {"ask_user": "question"})
        self.assertEqual(reply, "approved")

    def test_run_task_returns_error_result_when_loop_raises(self) -> None:
        from src.core.agent_kernel import Principal
        from src.core.agent_loop import AgentContext
        from src.core.XAgent import XAgent

        agent = XAgent.__new__(XAgent)
        agent.handler = type("H", (), {})()
        agent.handler.ctx = AgentContext()
        agent._running = threading.Event()
        agent.stop_event = threading.Event()
        agent.client = None
        agent.system_prompt = ""
        agent.tools_schema = []
        agent.display_queue = __import__("queue").Queue()
        agent.principal_template = Principal.local(
            session_id="telemetry-test",
        )

        import src.core.XAgent as xa_mod

        original = xa_mod.run_agent_loop

        def _raise(**_):
            raise RuntimeError("boom")

        xa_mod.run_agent_loop = _raise
        try:
            result = agent.run_task("hello")
        finally:
            xa_mod.run_agent_loop = original

        self.assertEqual(result["exit_reason"], "ERROR")
        self.assertEqual(result["response"], "[error] agent runtime failed")
        self.assertNotIn("boom", str(result))
        self.assertEqual(
            result["tool_results"][0]["data"]["reason_code"],
            "AGENT_RUNTIME_FAILED",
        )
        self.assertEqual(
            result["tool_results"][0]["data"]["exception_type"],
            "RuntimeError",
        )
        done = agent.display_queue.get_nowait()
        self.assertEqual(done["done"]["exit_reason"], "ERROR")

    def test_run_task_auto_selects_skills(self) -> None:
        from src.core.agent_kernel import Principal
        from src.core.agent_loop import AgentContext
        from src.core.XAgent import XAgent
        from src.core.skills import SkillManifest, SkillRegistry

        agent = XAgent.__new__(XAgent)
        agent.handler = type("H", (), {})()
        agent.handler.ctx = AgentContext()
        agent._running = threading.Event()
        agent.stop_event = threading.Event()
        agent.client = None
        agent.system_prompt = ""
        agent.tools_schema = []
        agent.display_queue = __import__("queue").Queue()
        agent.principal_template = Principal.local(
            session_id="telemetry-test",
        )
        agent.skill_registry = SkillRegistry(
            {
                "code-review": SkillManifest(
                    name="code-review",
                    description="Review code changes",
                    path=Path("/tmp"),
                    triggers=("review",),
                )
            }
        )

        import src.core.XAgent as xa_mod

        original = xa_mod.run_agent_loop
        xa_mod.run_agent_loop = lambda **_: {"exit_reason": "CURRENT_TASK_DONE"}
        try:
            agent.run_task("please review this")
        finally:
            xa_mod.run_agent_loop = original

        self.assertEqual(agent.handler.ctx.active_skills, ["code-review"])


class BuildSinkTests(unittest.TestCase):
    """Step 6：main.build_sink 按环境变量装配 sink。"""

    def _set_env(self, **kv: str) -> None:
        import os

        for k in ("XAGENT_LOG_DIR", "XAGENT_LOG_STDERR"):
            os.environ.pop(k, None)
        for k, v in kv.items():
            os.environ[k] = v

    def test_build_sink_defaults_to_null(self) -> None:
        from src.main import build_sink

        self._set_env()
        sink = build_sink()
        self.assertIsInstance(sink, NullSink)

    def test_build_sink_jsonl_when_log_dir_set(self) -> None:
        from src.main import build_sink

        with tempfile.TemporaryDirectory() as tmp:
            self._set_env(XAGENT_LOG_DIR=tmp)
            sink = build_sink()
            self.assertIsInstance(sink, JsonlSink)
            sink.close()

    def test_build_sink_multi_when_both_set(self) -> None:
        from src.main import build_sink

        with tempfile.TemporaryDirectory() as tmp:
            self._set_env(XAGENT_LOG_DIR=tmp, XAGENT_LOG_STDERR="1")
            sink = build_sink()
            self.assertIsInstance(sink, MultiSink)
            sink.close()


if __name__ == "__main__":
    unittest.main()
