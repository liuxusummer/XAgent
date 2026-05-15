from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.core.agent_loop import AgentContext, exhaust
from src.core.XAgent import XAgent
from src.core.skills import SkillRegistry
from src.handler import XAgentHandler
from src.main import build_system_prompt, filter_tools_schema
from src.tools.file_ops import write_file
from src.web_ui_new import (
    SubmitTaskRequest,
    ReplyRequest,
    UISession,
    WorkspaceFileWriteRequest,
    _agent_runtime_config,
    _queue_state,
    _resolve_workspace_dir_input,
    _sessions,
    parse_agent_markdown,
    read_workspace_file,
    read_agent_profile,
    serialize_agent_markdown,
    send_reply,
    submit_task,
    stop_task,
    stream_chat,
    write_agent_profile,
    write_workspace_file,
    AgentProfileWriteRequest,
)


class _FakeAgent:
    def __init__(self, running: bool = False) -> None:
        self.reply_queue = _Queue()
        self._running = running
        self.stopped = False

    def is_running(self) -> bool:
        return self._running

    def stop(self) -> None:
        self.stopped = True


class _Queue:
    def __init__(self) -> None:
        self.items: list[str] = []

    def put(self, item: str) -> None:
        self.items.append(item)


class _Request:
    def __init__(self, payload: dict[str, str]) -> None:
        self.payload = payload

    async def json(self) -> dict[str, str]:
        return self.payload


class _StreamRequest:
    def __init__(self, session_id: str) -> None:
        self.query_params = {"session_id": session_id}

    async def is_disconnected(self) -> bool:
        return False


class _NoopThread:
    def __init__(self, *args, **kwargs) -> None:
        del args, kwargs

    def start(self) -> None:
        return None


class WebUINewSessionRoutingTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self) -> None:
        _sessions.clear()

    async def test_reply_targets_requested_session(self) -> None:
        left_agent = _FakeAgent()
        right_agent = _FakeAgent()
        _sessions["left"] = UISession(agent=left_agent, waiting_for_user=True, session_id="left")
        _sessions["right"] = UISession(agent=right_agent, waiting_for_user=True, session_id="right")

        with patch("src.web_ui_new._drain_background"):
            result = await send_reply(ReplyRequest(reply="ok", session_id="right"))

        self.assertTrue(result["success"])
        self.assertEqual(left_agent.reply_queue.items, [])
        self.assertEqual(right_agent.reply_queue.items, ["ok"])

    async def test_stop_targets_requested_session(self) -> None:
        left_agent = _FakeAgent(running=True)
        right_agent = _FakeAgent(running=True)
        _sessions["left"] = UISession(agent=left_agent, session_id="left")
        _sessions["right"] = UISession(agent=right_agent, session_id="right")

        result = await stop_task(_Request({"session_id": "right"}))

        self.assertTrue(result["success"])
        self.assertFalse(left_agent.stopped)
        self.assertTrue(right_agent.stopped)

    def test_queue_state_is_bounded_wakeup_without_event_copy(self) -> None:
        session = UISession(session_id="bounded")
        session.events.append({"type": "log", "data": "hello"})

        _queue_state(session, finished=False)
        _queue_state(session, finished=False)

        state = session.event_queue.get_nowait()
        self.assertNotIn("events", state)
        self.assertTrue(session.event_queue.empty())

    def test_finished_wakeup_replaces_stale_wakeup(self) -> None:
        session = UISession(session_id="finished")

        _queue_state(session, finished=False)
        _queue_state(session, finished=True)

        state = session.event_queue.get_nowait()
        self.assertTrue(state["finished"])
        self.assertTrue(session.event_queue.empty())

    async def test_stream_reads_events_from_session_log_not_queue_payload(self) -> None:
        session = UISession(session_id="stream")
        session.events.append({"type": "assistant_delta", "data": "hello"})
        session.running = False
        session.waiting_for_user = False
        _sessions["stream"] = session
        _queue_state(session, finished=True)

        response = await stream_chat(_StreamRequest("stream"))
        chunks: list[str] = []
        async for chunk in response.body_iterator:
            chunks.append(chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk)

        payload = "".join(chunks)
        self.assertIn('"type": "assistant_delta"', payload)
        self.assertIn("hello", payload)


class WebUINewWorkspaceFileTests(unittest.IsolatedAsyncioTestCase):
    async def test_read_workspace_system_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            ws_root = Path(tmp_dir) / "default.ws"
            target = ws_root / "system" / "agents" / "main" / "AGENT.md"
            target.parent.mkdir(parents=True)
            target.write_text("agent config", encoding="utf-8")

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await read_workspace_file("default.ws", "system/agents/main/AGENT.md")

            self.assertTrue(result["success"])
            self.assertEqual(result["data"], {"path": "system/agents/main/AGENT.md", "content": "agent config"})

    async def test_write_workspace_system_file_creates_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir()

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await write_workspace_file(
                    WorkspaceFileWriteRequest(
                        ws="default.ws",
                        path="system/templates/foo.md",
                        content="hello",
                    )
                )

            target = Path(tmp_dir) / "default.ws" / "system" / "templates" / "foo.md"
            self.assertTrue(result["success"])
            self.assertTrue(result["data"]["created"])
            self.assertEqual(result["data"]["bytes"], 5)
            self.assertEqual(result["data"]["content"], "hello")
            self.assertEqual(target.read_text(encoding="utf-8"), "hello")

    async def test_write_workspace_system_file_overwrites_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "default.ws" / "system" / "templates" / "foo.md"
            target.parent.mkdir(parents=True)
            target.write_text("old", encoding="utf-8")

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await write_workspace_file(
                    WorkspaceFileWriteRequest(
                        ws="default.ws",
                        path="system/templates/foo.md",
                        content="new text",
                    )
                )

            self.assertTrue(result["success"])
            self.assertFalse(result["data"]["created"])
            self.assertEqual(result["data"]["bytes"], len("new text".encode("utf-8")))
            self.assertEqual(target.read_text(encoding="utf-8"), "new text")

    async def test_workspace_file_api_rejects_invalid_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir()
            cases = [
                ("default.ws", "business/foo.txt"),
                ("default.ws", "../escape.txt"),
                ("default.ws", "/tmp/escape.txt"),
                ("plain", "system/foo.txt"),
                ("../bad.ws", "system/foo.txt"),
                ("missing.ws", "system/foo.txt"),
            ]

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                for ws, path in cases:
                    result = await write_workspace_file(
                        WorkspaceFileWriteRequest(ws=ws, path=path, content="bad")
                    )
                    self.assertFalse(result["success"], (ws, path))

    async def test_frontend_write_api_does_not_weaken_agent_file_tools(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "default.ws" / "system" / "agents" / "main" / "AGENT.md"
            target.parent.mkdir(parents=True)

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await write_workspace_file(
                    WorkspaceFileWriteRequest(
                        ws="default.ws",
                        path="system/agents/main/AGENT.md",
                        content="managed by frontend",
                    )
                )

            self.assertTrue(result["success"])
            denied = write_file(
                path="system/agents/main/AGENT.md",
                content="agent write",
                cwd=str(Path(tmp_dir) / "default.ws"),
            )
            self.assertEqual(denied["status"], "ERROR")
            self.assertEqual(target.read_text(encoding="utf-8"), "managed by frontend")

    def test_workspace_name_resolves_to_project_workspace_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                resolved = _resolve_workspace_dir_input("default.ws")

            self.assertEqual(resolved, str(Path(tmp_dir) / "default.ws"))

    def test_parse_agent_markdown_with_frontmatter(self) -> None:
        content = """---
name: "main"
description: "日常对话分析"
tools:
  - "Read"
  - "Write"
model: "minimax-m2.7" # comment
maxTurns: 300
memory: "project"
skills: []
project_agents:
  - "coding"
---

# Main Agent
"""

        result = parse_agent_markdown(content, "main")

        self.assertEqual(result["profile"]["name"], "main")
        self.assertEqual(result["profile"]["description"], "日常对话分析")
        self.assertEqual(result["profile"]["tools"], ["Read", "Write"])
        self.assertEqual(result["profile"]["model"], "minimax-m2.7")
        self.assertEqual(result["profile"]["maxTurns"], 300)
        self.assertEqual(result["profile"]["memory"], "project")
        self.assertEqual(result["profile"]["skills"], [])
        self.assertEqual(result["profile"]["project_agents"], ["coding"])
        self.assertEqual(result["body"], "# Main Agent\n")

    def test_parse_agent_markdown_without_frontmatter_uses_defaults(self) -> None:
        result = parse_agent_markdown("# Legacy Agent\n", "legacy")

        self.assertEqual(result["profile"]["name"], "legacy")
        self.assertEqual(result["profile"]["description"], "")
        self.assertEqual(result["profile"]["tools"], [])
        self.assertEqual(result["body"], "# Legacy Agent\n")

    def test_serialize_agent_markdown_preserves_body(self) -> None:
        profile = {
            "name": "main",
            "description": "日常对话分析",
            "tools": ["Read"],
            "model": "minimax-m2.7",
            "maxTurns": 300,
            "memory": "project",
            "skills": [],
            "project_agents": ["coding"],
        }

        content = serialize_agent_markdown(profile, "# Main Agent\n")

        self.assertIn('name: "main"', content)
        self.assertIn('  - "Read"', content)
        self.assertTrue(content.endswith("# Main Agent\n"))

    async def test_list_agents_returns_profile_description(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "default.ws" / "system" / "agents" / "main" / "AGENT.md"
            target.parent.mkdir(parents=True)
            target.write_text(
                '---\nname: "main"\ndescription: "日常对话分析"\ntools: []\nmodel: ""\nmaxTurns: 300\nmemory: ""\nskills: []\nproject_agents: []\n---\n\n# Main\n',
                encoding="utf-8",
            )

            from src.web_ui_new import list_agents

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                result = await list_agents("default.ws")

            self.assertTrue(result["success"])
            self.assertEqual(result["data"][0]["description"], "日常对话分析")
            self.assertEqual(result["data"][0]["profile"]["name"], "main")

    async def test_agent_profile_endpoint_reads_and_writes_agent_md(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "default.ws" / "system" / "agents" / "main" / "AGENT.md"
            target.parent.mkdir(parents=True)
            target.write_text("# Main\n", encoding="utf-8")

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                read_result = await read_agent_profile("default.ws", "main")
                self.assertTrue(read_result["success"])
                self.assertEqual(read_result["data"]["profile"]["name"], "main")

                profile = dict(read_result["data"]["profile"])
                profile["description"] = "日常对话分析"
                profile["tools"] = ["Read"]
                write_result = await write_agent_profile(
                    AgentProfileWriteRequest(
                        ws="default.ws",
                        agent="main",
                        profile=profile,
                        body="# Updated\n",
                    )
                )

            self.assertTrue(write_result["success"])
            self.assertIn('description: "日常对话分析"', target.read_text(encoding="utf-8"))
            self.assertIn("# Updated\n", target.read_text(encoding="utf-8"))

    def test_agent_runtime_config_reads_prompt_soul_and_frontmatter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            agent_dir = Path(tmp_dir) / "default.ws" / "system" / "agents" / "coding"
            agent_dir.mkdir(parents=True)
            (agent_dir / "AGENT.md").write_text(
                '---\nname: "coding"\ndescription: ""\ntools:\n  - "file_read"\nmodel: "dev-model"\nmaxTurns: 12\nmemory: ""\nskills:\n  - "review"\nproject_agents: []\n---\n\n# Coding Agent\n',
                encoding="utf-8",
            )
            (agent_dir / "SOUL.md").write_text("# Soul\n", encoding="utf-8")

            with patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)):
                runtime, error = _agent_runtime_config("default.ws", "coding")

            self.assertIsNone(error)
            self.assertEqual(runtime["agent_prompt"], "# Coding Agent\n")
            self.assertEqual(runtime["agent_soul"], "# Soul\n")
            self.assertEqual(runtime["tools_allowlist"], ["file_read"])
            self.assertEqual(runtime["skill_allowlist"], ["review"])
            self.assertEqual(runtime["model_override"], "dev-model")
            self.assertEqual(runtime["max_turns"], 12)

    async def test_submit_task_passes_selected_agent_runtime_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            agent_dir = Path(tmp_dir) / "default.ws" / "system" / "agents" / "coding"
            agent_dir.mkdir(parents=True)
            (agent_dir / "AGENT.md").write_text(
                '---\nname: "coding"\ndescription: ""\ntools:\n  - "file_read"\nmodel: "dev-model"\nmaxTurns: 12\nmemory: ""\nskills: []\nproject_agents: []\n---\n\n# Coding Agent\n',
                encoding="utf-8",
            )
            (agent_dir / "SOUL.md").write_text("# Soul\n", encoding="utf-8")

            fake_agent = _FakeAgent()
            fake_agent.handler = type("H", (), {"ctx": type("C", (), {"verbose": False})()})()
            captured: dict[str, object] = {}

            def _fake_build_agent(**kwargs):
                captured.update(kwargs)
                return fake_agent

            _sessions.clear()
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)),
                patch("src.web_ui_new.build_agent", side_effect=_fake_build_agent),
                patch("src.web_ui_new.threading.Thread", _NoopThread),
            ):
                result = await submit_task(
                    SubmitTaskRequest(task="hello", workspace_dir="default.ws", agent="coding")
                )

            self.assertTrue(result["success"])
            self.assertEqual(captured["agent_name"], "coding")
            self.assertEqual(captured["agent_prompt"], "# Coding Agent\n")
            self.assertEqual(captured["agent_soul"], "# Soul\n")
            self.assertEqual(captured["tools_allowlist"], ["file_read"])
            self.assertEqual(captured["model_override"], "dev-model")
            self.assertEqual(captured["max_turns"], 12)

    async def test_submit_task_rejects_invalid_agent_without_starting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir()
            _sessions.clear()
            with (
                patch("src.web_ui_new._WORKSPACE_ROOT", str(tmp_dir)),
                patch("src.web_ui_new.build_agent") as build_agent_mock,
                patch("src.web_ui_new.threading.Thread", _NoopThread),
            ):
                result = await submit_task(
                    SubmitTaskRequest(task="hello", workspace_dir="default.ws", agent="../bad")
                )

            self.assertFalse(result["success"])
            build_agent_mock.assert_not_called()

    def test_filter_tools_schema_keeps_only_agent_allowed_tools(self) -> None:
        schema = [
            {"type": "function", "function": {"name": "file_read"}},
            {"type": "function", "function": {"name": "code_run"}},
        ]

        filtered = filter_tools_schema(schema, ["file_read"])

        self.assertEqual([item["function"]["name"] for item in filtered], ["file_read"])

    def test_agent_prompt_replaces_global_base_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            assets = root / "assets"
            memory = root / "memory"
            assets.mkdir()
            memory.mkdir()
            (assets / "sys_prompt.txt").write_text("global base", encoding="utf-8")
            (memory / "global_mem_insight.txt").write_text("insight", encoding="utf-8")
            (memory / "insight_fixed_structure.txt").write_text("fixed", encoding="utf-8")

            prompt = build_system_prompt(
                assets,
                root,
                "/workspace",
                agent_name="coding",
                agent_prompt="# Coding Agent",
                agent_soul="# Soul",
            )

            self.assertEqual(prompt, "# Coding Agent\n\n# Soul")
            self.assertNotIn("global base", prompt)
            self.assertNotIn("[动态注入]", prompt)
            self.assertNotIn("[Memory]", prompt)

    def test_config_model_is_preserved_without_agent_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "config.json"
            config_path.write_text(
                '{"openai_text": {"apikey": "", "apibase": "https://example.com/chat/completions", "model": "config-model"}}',
                encoding="utf-8",
            )
            agent = XAgent(
                system_prompt="",
                tools_schema=[],
                config_path=str(config_path),
                model="",
                workspace_dir=str(Path(tmp_dir) / "default.ws"),
            )
            try:
                self.assertEqual(agent.client.backend.model, "config-model")
            finally:
                agent.close()

    def test_agent_model_overrides_config_model_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "config.json"
            config_path.write_text(
                '{"openai_text": {"apikey": "", "apibase": "https://example.com/chat/completions", "model": "config-model"}}',
                encoding="utf-8",
            )
            agent = XAgent(
                system_prompt="",
                tools_schema=[],
                config_path=str(config_path),
                model="agent-model",
                workspace_dir=str(Path(tmp_dir) / "default.ws"),
            )
            try:
                self.assertEqual(agent.client.backend.model, "agent-model")
            finally:
                agent.close()

    def test_handler_rejects_tools_outside_agent_allowlist(self) -> None:
        handler = XAgentHandler(ctx=AgentContext(allowed_tools={"file_read"}))

        result = exhaust(handler.dispatch("code_run", {"script": "print(1)"}))

        self.assertEqual(result.data["status"], "ERROR")
        self.assertIn("not allowed", result.data["error"])

    def test_skill_activate_respects_agent_skill_allowlist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            for name in ("allowed", "blocked"):
                skill_dir = root / name
                skill_dir.mkdir()
                (skill_dir / "SKILL.md").write_text(f"# {name}\nUse for {name}.", encoding="utf-8")
            registry = SkillRegistry.load([root])
            handler = XAgentHandler(ctx=AgentContext(skills=registry, skill_allowlist={"allowed"}))

            result = handler.exec_skill_activate({"names": ["allowed", "blocked"]})

            self.assertEqual(result.data["activated"], ["allowed"])
            self.assertEqual(result.data["disallowed"], ["blocked"])
            self.assertEqual(handler.ctx.active_skills, ["allowed"])


if __name__ == "__main__":
    unittest.main()
