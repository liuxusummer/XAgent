from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.core.agent_loop import AgentContext, BaseHandler, TurnEndHook, run_agent_loop
from src.core.llm import ChatResponse
from src.core.memory import (
    load_agent_memory,
    load_boot_memory,
    load_effective_memory,
    load_global_memory,
    load_memory_sop,
    load_workspace_memory,
    record_self_evolution_lesson,
)
from src.handler import XAgentHandler
from src.main import build_system_prompt
from src.tools import interaction


class MemoryProviderTests(unittest.TestCase):
    def test_load_boot_memory_joins_files_in_fixed_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "insight_fixed_structure.txt").write_text("fixed", encoding="utf-8")
            (root / "global_mem_insight.txt").write_text("insight", encoding="utf-8")

            result = load_boot_memory(root)

            self.assertEqual(result.content, "insight\n\nfixed")
            self.assertEqual([item.name for item in result.files], [
                "global_mem_insight.txt",
                "insight_fixed_structure.txt",
            ])
            self.assertTrue(all(item.exists for item in result.files))
            self.assertTrue(all(not item.empty for item in result.files))

    def test_load_boot_memory_skips_missing_and_empty_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "global_mem_insight.txt").write_text("  insight  ", encoding="utf-8")
            (root / "insight_fixed_structure.txt").write_text("  \n", encoding="utf-8")

            result = load_boot_memory(root)

            self.assertEqual(result.content, "insight")
            self.assertEqual(result.files[0].chars, len("insight"))
            self.assertFalse(result.files[0].empty)
            self.assertTrue(result.files[1].exists)
            self.assertTrue(result.files[1].empty)

    def test_load_boot_memory_handles_all_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = load_boot_memory(Path(tmp_dir))

            self.assertEqual(result.content, "")
            self.assertEqual(len(result.files), 2)
            self.assertTrue(all(not item.exists for item in result.files))

    def test_load_global_memory_reads_global_mem(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "global_mem.txt").write_text("\nlong term\n", encoding="utf-8")

            result = load_global_memory(root)

            self.assertEqual(result.content, "long term")
            self.assertEqual(result.files[0].name, "global_mem.txt")

    def test_load_global_memory_missing_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = load_global_memory(Path(tmp_dir))

            self.assertEqual(result.content, "")
            self.assertFalse(result.files[0].exists)
            self.assertTrue(result.files[0].empty)

    def test_load_workspace_memory_reads_system_memory_files_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            memory = root / "system" / "memory"
            memory.mkdir(parents=True)
            (memory / "b.md").write_text("bravo", encoding="utf-8")
            (memory / "a.txt").write_text("alpha", encoding="utf-8")
            (memory / ".DS_Store").write_text("ignored", encoding="utf-8")

            result = load_workspace_memory(root)

            self.assertEqual(result.content, "alpha\n\nbravo")
            self.assertEqual([item.name for item in result.files], ["a.txt", "b.md"])

    def test_load_agent_memory_reads_only_named_agent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            main_dir = root / "system" / "agents" / "main"
            coding_dir = root / "system" / "agents" / "coding"
            main_dir.mkdir(parents=True)
            coding_dir.mkdir(parents=True)
            (main_dir / "MEMORY.md").write_text("main private", encoding="utf-8")
            (coding_dir / "MEMORY.md").write_text("coding private", encoding="utf-8")

            result = load_agent_memory(root, "main")

            self.assertEqual(result.content, "main private")
            self.assertNotIn("coding private", result.content)

    def test_load_agent_memory_missing_is_non_fatal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = load_agent_memory(Path(tmp_dir), "main")

            self.assertEqual(result.content, "")
            self.assertFalse(result.files[0].exists)

    def test_load_effective_memory_project_combines_global_and_private(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            memory = root / "system" / "memory"
            agent = root / "system" / "agents" / "main"
            memory.mkdir(parents=True)
            agent.mkdir(parents=True)
            (memory / "global.md").write_text("workspace global", encoding="utf-8")
            (agent / "MEMORY.md").write_text("main private", encoding="utf-8")

            result = load_effective_memory(root, "main", "project")

            self.assertEqual(result.content, "workspace global\n\nmain private")

    def test_load_effective_memory_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            memory = root / "system" / "memory"
            agent = root / "system" / "agents" / "main"
            memory.mkdir(parents=True)
            agent.mkdir(parents=True)
            (memory / "global.md").write_text("workspace global", encoding="utf-8")
            (agent / "MEMORY.md").write_text("main private", encoding="utf-8")

            self.assertEqual(load_effective_memory(root, "main", "private").content, "main private")
            self.assertEqual(load_effective_memory(root, "main", "global").content, "workspace global")
            self.assertEqual(load_effective_memory(root, "main", "none").content, "")

    def test_load_memory_sop_reads_default_sop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "memory_management_sop.md").write_text("sop", encoding="utf-8")

            result = load_memory_sop(root)

            self.assertEqual(result.content, "sop")
            self.assertEqual(result.files[0].name, "memory_management_sop.md")

    def test_load_memory_sop_rejects_path_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = load_memory_sop(Path(tmp_dir), "../secret.md")

            self.assertEqual(result.content, "")
            self.assertEqual(result.files[0].error, "invalid_name")

    def test_record_self_evolution_lesson_uses_workspace_memory_without_agent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)

            result = record_self_evolution_lesson(root, "  避免重复失败  ")

            target = root / "system" / "memory" / "self_evolution.md"
            self.assertEqual(result["status"], "OK")
            self.assertTrue(result["written"])
            self.assertIn("## 自我进化经验", target.read_text(encoding="utf-8"))
            self.assertIn("- 避免重复失败", target.read_text(encoding="utf-8"))

    def test_record_self_evolution_lesson_uses_agent_private_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "system" / "agents" / "main" / "MEMORY.md"
            target.parent.mkdir(parents=True)
            target.write_text("# Main Memory\n\n## 稳定事实\n- keep\n", encoding="utf-8")

            result = record_self_evolution_lesson(root, "先探测再重试", agent_name="main")

            content = target.read_text(encoding="utf-8")
            self.assertEqual(result["status"], "OK")
            self.assertIn("# Main Memory", content)
            self.assertIn("## 稳定事实\n- keep", content)
            self.assertIn("## 自我进化经验\n\n- 先探测再重试", content)

    def test_record_self_evolution_lesson_dedupes_and_caps_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)

            first = record_self_evolution_lesson(root, "lesson-1", max_lessons=2)
            duplicate = record_self_evolution_lesson(root, "lesson-1", max_lessons=2)
            second = record_self_evolution_lesson(root, "lesson-2", max_lessons=2)
            third = record_self_evolution_lesson(root, "lesson-3", max_lessons=2)

            content = (root / "system" / "memory" / "self_evolution.md").read_text(encoding="utf-8")
            self.assertTrue(first["written"])
            self.assertFalse(duplicate["written"])
            self.assertTrue(second["written"])
            self.assertTrue(third["written"])
            self.assertNotIn("lesson-1", content)
            self.assertIn("- lesson-2", content)
            self.assertIn("- lesson-3", content)


class MemoryProviderIntegrationTests(unittest.TestCase):
    def test_build_system_prompt_keeps_memory_block_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            assets = root / "assets"
            memory = root / "memory"
            assets.mkdir()
            memory.mkdir()
            (assets / "sys_prompt.txt").write_text("base", encoding="utf-8")
            (memory / "global_mem_insight.txt").write_text("insight", encoding="utf-8")
            (memory / "insight_fixed_structure.txt").write_text("fixed", encoding="utf-8")

            prompt = build_system_prompt(assets, root, "/workspace")

            self.assertIn("[Memory]\ninsight\n\nfixed", prompt)
            self.assertIn("workspace = /workspace", prompt)

    def test_build_system_prompt_prefers_workspace_effective_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            assets = root / "assets"
            workspace = root / "workspace" / "default.ws"
            global_memory = workspace / "system" / "memory"
            agent_memory = workspace / "system" / "agents" / "main"
            assets.mkdir()
            global_memory.mkdir(parents=True)
            agent_memory.mkdir(parents=True)
            (assets / "sys_prompt.txt").write_text("base", encoding="utf-8")
            (global_memory / "global.md").write_text("workspace global", encoding="utf-8")
            (agent_memory / "MEMORY.md").write_text("main private", encoding="utf-8")

            prompt = build_system_prompt(assets, root, str(workspace), agent_name="main", memory_mode="project")

            self.assertIn("[Memory]\nworkspace global\n\nmain private", prompt)

    def test_build_system_prompt_agent_prompt_keeps_dynamic_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            assets = root / "assets"
            workspace = root / "workspace" / "default.ws"
            agent_memory = workspace / "system" / "agents" / "coding"
            assets.mkdir()
            agent_memory.mkdir(parents=True)
            (assets / "sys_prompt.txt").write_text("base", encoding="utf-8")
            (agent_memory / "MEMORY.md").write_text("coding private", encoding="utf-8")

            prompt = build_system_prompt(
                assets,
                root,
                str(workspace),
                agent_name="coding",
                agent_prompt="# Coding Agent",
                agent_soul="# Soul",
                memory_mode="private",
            )

            self.assertTrue(prompt.startswith("# Coding Agent\n\n# Soul"))
            self.assertIn("[Memory]\ncoding private", prompt)

    def test_build_system_prompt_private_mode_does_not_fallback_to_repo_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            assets = root / "assets"
            repo_memory = root / "memory"
            workspace = root / "workspace" / "default.ws"
            (workspace / "system").mkdir(parents=True)
            assets.mkdir()
            repo_memory.mkdir()
            (assets / "sys_prompt.txt").write_text("base", encoding="utf-8")
            (repo_memory / "global_mem_insight.txt").write_text("repo insight", encoding="utf-8")

            prompt = build_system_prompt(assets, root, str(workspace), agent_name="main", memory_mode="private")

            self.assertIn("[Memory]\n", prompt)
            self.assertNotIn("repo insight", prompt)

    def test_periodic_inject_uses_global_memory_when_non_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            memory = root / "system" / "memory"
            agent = root / "system" / "agents" / "main"
            memory.mkdir(parents=True)
            agent.mkdir(parents=True)
            (memory / "global.md").write_text("remember this", encoding="utf-8")
            (agent / "MEMORY.md").write_text("main private", encoding="utf-8")
            handler = XAgentHandler(
                ctx=AgentContext(
                    memory_root=str(root),
                    agent_name="main",
                    memory_mode="project",
                    current_turn=10,
                )
            )

            prompt = handler._periodic_inject_hook(  # noqa: SLF001
                response=ChatResponse(thinking="", content="", tool_calls=[]),
                tool_results=[],
                ctx=handler.ctx,
            )

            self.assertEqual(prompt, "[Memory Refresh]\nremember this\n\nmain private")

    def test_periodic_inject_skips_empty_global_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "system" / "memory").mkdir(parents=True)
            handler = XAgentHandler(ctx=AgentContext(memory_root=str(root), current_turn=10))

            prompt = handler._periodic_inject_hook(  # noqa: SLF001
                response=ChatResponse(thinking="", content="", tool_calls=[]),
                tool_results=[],
                ctx=handler.ctx,
            )

            self.assertIsNone(prompt)

    def test_start_long_term_update_uses_default_sop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "memory_management_sop.md").write_text("sop body", encoding="utf-8")
            with patch.object(interaction, "MEMORY_DIR", root):
                result = interaction.start_long_term_update()

            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["sop_content"], "sop body")

    def test_start_long_term_update_allows_missing_sop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.object(interaction, "MEMORY_DIR", Path(tmp_dir)):
                result = interaction.start_long_term_update()

            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["sop_content"], "")

    def test_self_evolution_hook_persists_lesson_and_injects_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            handler = XAgentHandler(ctx=AgentContext(memory_root=str(root), agent_name="main"))

            prompt = handler._self_evolution_hook(  # noqa: SLF001
                response=ChatResponse(thinking="", content="", tool_calls=[]),
                tool_results=[
                    {
                        "tool_name": "file_patch",
                        "tool_call_id": "1",
                        "data": {"status": "ERROR", "error": "no match"},
                    }
                ],
                ctx=handler.ctx,
            )

            memory = root / "system" / "agents" / "main" / "MEMORY.md"
            self.assertIsNotNone(prompt)
            assert prompt is not None
            self.assertIn("[Self Evolution]", prompt)
            self.assertIn("file_patch 返回 ERROR", prompt)
            self.assertIn("file_read 精读目标片段", memory.read_text(encoding="utf-8"))

    def test_self_evolution_hook_skips_ok_tool_results(self) -> None:
        handler = XAgentHandler(ctx=AgentContext())

        prompt = handler._self_evolution_hook(  # noqa: SLF001
            response=ChatResponse(thinking="", content="", tool_calls=[]),
            tool_results=[
                {
                    "tool_name": "file_read",
                    "tool_call_id": "1",
                    "data": {"status": "OK", "content": "done"},
                }
            ],
            ctx=handler.ctx,
        )

        self.assertIsNone(prompt)

    def test_run_agent_loop_passes_no_tool_result_to_turn_end_hooks(self) -> None:
        class DummyClient:
            def __init__(self) -> None:
                self.responses = [
                    ChatResponse(thinking="", content="", tool_calls=[]),
                    ChatResponse(thinking="", content="任务完成", tool_calls=[]),
                ]
                self.calls = 0
                self.backend = type("Backend", (), {"history": []})()

            def chat(self, messages, tools):  # noqa: ANN001
                del messages, tools
                response = self.responses[self.calls]
                self.calls += 1
                return response

        class CaptureTurnEndHandler(BaseHandler):
            def __init__(self) -> None:
                super().__init__(ctx=AgentContext())
                self.seen_tool_results: list[list[dict]] = []
                self._turn_end_hooks.append(TurnEndHook(name="capture", fn=self._capture, priority=1))

            def _capture(self, response, tool_results, ctx):  # noqa: ANN001
                del response, ctx
                self.seen_tool_results.append(tool_results)
                return None

        handler = CaptureTurnEndHandler()

        run_agent_loop(
            client=DummyClient(),
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=5,
        )

        self.assertEqual(handler.seen_tool_results[0][0]["tool_name"], "no_tool")
        self.assertEqual(handler.seen_tool_results[0][0]["data"]["status"], "EMPTY_RESPONSE")


if __name__ == "__main__":
    unittest.main()
