from __future__ import annotations

import queue
import threading
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

from src.core.agent_loop import AgentContext
from src.core.skills import SkillRegistry
from src.handler import XAgentHandler
from src.tools.interaction import make_progress_emitter


class HandlerToolStreamingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.display_queue: queue.Queue[dict] = queue.Queue()

    def test_exec_code_run_streams_stdout_progress_and_collects_result(self) -> None:
        handler = XAgentHandler(
            ctx=AgentContext(
                cwd="",
                stop_signal=threading.Event(),
                display_fn=make_progress_emitter(self.display_queue),
                user_input_fn=lambda _prompt: "yes",
            )
        )

        chunks = iter(
            [
                {"type": "stdout", "data": "line 1\n"},
                {"type": "stdout", "data": "line 2\n"},
                {"type": "result", "data": {"status": "OK", "exit_code": 0, "stderr": ""}},
            ]
        )

        with (
            patch.dict(
                "os.environ",
                {
                    "XAGENT_CODE_RUN_POLICY": "allow",
                    "XAGENT_CODE_RUN_BACKEND": "unsafe",
                },
            ),
            patch("src.handler.XAgentHandler.XAgentHandler.run_code_stream", return_value=chunks),
        ):
            result = handler.exec_code_run({"script": "print(1)", "timeout": 5})

        progress = []
        while not self.display_queue.empty():
            msg = self.display_queue.get_nowait()
            if "progress" in msg:
                progress.append(msg["progress"])

        self.assertEqual(result.data["status"], "OK")
        self.assertEqual(result.data["stdout"], "line 1\nline 2\n")
        self.assertIn("  code_run: start (python, timeout=5s)", progress)
        self.assertIn("  code_run | line 1", progress)
        self.assertIn("  code_run | line 2", progress)
        self.assertIn("  code_run: done (OK)", progress)

    def test_exec_code_run_requires_explicit_authorization_by_default(self) -> None:
        prompts: list[str] = []
        handler = XAgentHandler(
            ctx=AgentContext(user_input_fn=lambda prompt: prompts.append(prompt) or "no")
        )

        with (
            patch.dict(
                "os.environ",
                {
                    "XAGENT_CODE_RUN_POLICY": "confirm",
                    "XAGENT_CODE_RUN_BACKEND": "unsafe",
                },
            ),
            patch("src.handler.XAgentHandler.XAgentHandler.run_code_stream") as run_stream,
        ):
            result = handler.exec_code_run({"script": "print(1)", "timeout": 5})

        self.assertEqual(result.data["status"], "SKIP")
        self.assertIn("script_sha256", result.data)
        self.assertIn("开发级非隔离宿主进程", prompts[0])
        run_stream.assert_not_called()

    def test_exec_ask_user_uses_instance_input_fn(self) -> None:
        replies: list[str] = []

        def _input(prompt: str) -> str:
            replies.append(prompt)
            return "confirmed"

        handler = XAgentHandler(ctx=AgentContext(user_input_fn=_input))

        result = handler.exec_ask_user({"message": "continue?"})

        self.assertEqual(result.data["status"], "OK")
        self.assertEqual(result.data["user_reply"], "confirmed")
        self.assertEqual(result.next_prompt, "用户回复：confirmed")
        self.assertEqual(replies, ["\n🤖 continue?\n👤 "])

    def test_exec_ask_user_supports_numbered_options(self) -> None:
        prompts: list[str] = []

        def _input(prompt: str) -> str:
            prompts.append(prompt)
            return "2"

        handler = XAgentHandler(ctx=AgentContext(user_input_fn=_input))

        result = handler.exec_ask_user({"message": "choose mode", "options": ["fast", "safe"]})

        self.assertEqual(result.data["status"], "OK")
        self.assertEqual(result.data["options"], ["fast", "safe"])
        self.assertEqual(result.data["raw_user_reply"], "2")
        self.assertEqual(result.data["selected_option"], "safe")
        self.assertEqual(result.data["user_reply"], "safe")
        self.assertIn("1. fast", prompts[0])
        self.assertIn("2. safe", prompts[0])
        self.assertIn("请回复：fast / safe", prompts[0])
        self.assertEqual(result.next_prompt, "用户回复：safe")

    def test_exec_agent_delegate_rejects_without_team(self) -> None:
        handler = XAgentHandler(ctx=AgentContext())

        result = handler.exec_agent_delegate({"agent": "coding", "task": "implement"})

        self.assertEqual(result.data["status"], "ERROR")
        self.assertIn("no active team", result.data["error"])

    def test_exec_agent_delegate_uses_context_runner(self) -> None:
        calls: list[dict] = []

        def _runner(**kwargs):
            calls.append(kwargs)
            return {
                "status": "OK",
                "agent": kwargs["agent"],
                "response": "done",
                "exit_reason": "CURRENT_TASK_DONE",
                "turns": 1,
                "tool_result_count": 0,
            }

        handler = XAgentHandler(ctx=AgentContext(delegate_runner=_runner))

        result = handler.exec_agent_delegate(
            {
                "agent": "coding",
                "task": "implement feature",
                "context": "repo context",
                "expected_output": "patch summary",
            }
        )

        self.assertEqual(result.data["status"], "OK")
        self.assertEqual(result.data["response"], "done")
        self.assertEqual(calls[0]["agent"], "coding")
        self.assertEqual(calls[0]["task"], "implement feature")
        self.assertIs(calls[0]["parent_ctx"], handler.ctx)

    def test_exec_file_delete_skips_without_user_authorization(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "delete-me.txt"
            target.write_text("keep", encoding="utf-8")

            prompts: list[str] = []

            def _input(prompt: str) -> str:
                prompts.append(prompt)
                return "no"

            handler = XAgentHandler(ctx=AgentContext(cwd=str(root), user_input_fn=_input))

            result = handler.exec_file_delete({"path": "delete-me.txt"})

            self.assertEqual(result.data["status"], "SKIP")
            self.assertTrue(target.exists())
            self.assertIn(str(target), prompts[0])
            self.assertIn("删除后系统无法自动回滚", prompts[0])

    def test_exec_file_delete_deletes_inside_workspace_after_authorization(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "delete-me.txt"
            target.write_text("bye", encoding="utf-8")
            handler = XAgentHandler(ctx=AgentContext(cwd=str(root), user_input_fn=lambda _: "确认"))

            result = handler.exec_file_delete({"path": "delete-me.txt"})

            self.assertEqual(result.data["status"], "OK")
            self.assertFalse(target.exists())

    def test_exec_file_delete_denies_outside_workspace_after_authorization(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            outside = Path(tmp) / "outside.txt"
            root.mkdir()
            outside.write_text("keep", encoding="utf-8")
            handler = XAgentHandler(ctx=AgentContext(cwd=str(root), user_input_fn=lambda _: "yes"))

            result = handler.exec_file_delete({"path": str(outside)})

            self.assertEqual(result.data["status"], "ERROR")
            self.assertIn("outside-workspace deletion", result.data["error"])
            self.assertTrue(outside.exists())

    def test_exec_file_search_uses_context_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "target.txt").write_text("needle_value\n", encoding="utf-8")
            handler = XAgentHandler(ctx=AgentContext(cwd=str(root)))

            result = handler.exec_file_search({"query": "needle_value"})

            self.assertEqual(result.data["status"], "OK")
            self.assertEqual(result.data["matches"][0]["path"], "target.txt")
            self.assertIn("file_read", result.next_prompt or "")
            self.assertIn("evidence_id", result.next_prompt or "")

    def test_exec_skill_activate_updates_active_skills(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            skill_dir = root / "demo"
            skill_dir.mkdir()
            (skill_dir / "SKILL.md").write_text("# Demo\nUse for demo.", encoding="utf-8")
            registry = SkillRegistry.load([root])
            handler = XAgentHandler(ctx=AgentContext(skills=registry, session_id="s1"))

            result = handler.exec_skill_activate({"names": ["demo", "missing", "../bad"]})

            self.assertEqual(result.data["activated"], ["demo"])
            self.assertEqual(result.data["missing"], ["missing"])
            self.assertEqual(handler.ctx.active_skills, ["demo"])

    def test_get_anchor_prompt_injects_active_skills(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            skill_dir = root / "demo"
            skill_dir.mkdir()
            (skill_dir / "SKILL.md").write_text("# Demo\nSkill body.", encoding="utf-8")
            registry = SkillRegistry.load([root])
            handler = XAgentHandler(ctx=AgentContext(skills=registry, active_skills=["demo"]))

            prompt = handler.get_anchor_prompt()

            self.assertIn("<active_skills>", prompt)
            self.assertIn('<skill name="demo">', prompt)
            self.assertIn("Skill body", prompt)

if __name__ == "__main__":
    unittest.main()
