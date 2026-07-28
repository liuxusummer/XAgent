from __future__ import annotations

import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from src.core.XAgent import XAgent
from src.core.agent_loop import AgentContext, BaseHandler, TurnEndHook, run_agent_loop
from src.core.checkpoint import (
    build_task_checkpoint,
    load_task_checkpoint,
    render_resume_prompt,
    write_task_checkpoint,
)
from src.core.llm import ChatResponse, ToolCall
from src.core.memory import (
    load_agent_memory,
    load_boot_memory,
    load_effective_memory,
    load_global_memory,
    load_memory_sop,
    load_workspace_memory,
    record_self_evolution_lesson,
)
from src.core.runbook import (
    RUNBOOK_SKILL_NAME,
    RUNBOOK_TASK_ID,
    distill_runbook_from_task,
    ensure_runbook_template,
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

    def test_concurrent_self_evolution_writes_preserve_all_lessons(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "demo.ws"
            root.mkdir()

            with ThreadPoolExecutor(max_workers=20) as pool:
                records = list(
                    pool.map(
                        lambda index: record_self_evolution_lesson(
                            root,
                            f"lesson-{index}",
                            max_lessons=30,
                        ),
                        range(20),
                    )
                )

            content = (root / "system" / "memory" / "self_evolution.md").read_text(encoding="utf-8")
            lessons = {line[2:] for line in content.splitlines() if line.startswith("- lesson-")}
            self.assertTrue(all(record["status"] == "OK" for record in records))
            self.assertEqual(lessons, {f"lesson-{index}" for index in range(20)})


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


class RunbookDistillationTests(unittest.TestCase):
    def test_short_task_below_interaction_threshold_skips_without_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir) / "demo.ws"
            workspace.mkdir()

            record = distill_runbook_from_task(
                workspace,
                "Quick answer",
                {"exit_reason": "CURRENT_TASK_DONE", "turns": 1, "tool_results": []},
            )

            self.assertEqual(record["status"], "SKIP")
            self.assertEqual(record["error"], "insufficient_interaction_records")
            self.assertEqual(record["interaction_records"], 1)
            self.assertEqual(record["min_interaction_records"], 10)
            self.assertFalse((workspace / "system" / "skills" / RUNBOOK_SKILL_NAME / "SKILL.md").exists())
            self.assertFalse((workspace / "runtime" / "tasks" / "tasks.json").exists())

    def test_successful_task_creates_skill_template_and_review_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir) / "demo.ws"
            workspace.mkdir()
            result = {
                "exit_reason": "CURRENT_TASK_DONE",
                "turns": 8,
                "tool_results": [
                    {
                        "tool_name": "file_read",
                        "tool_call_id": "1",
                        "data": {"status": "OK", "content": "secret full file content"},
                    },
                    {"tool_name": "file_patch", "tool_call_id": "2", "data": {"status": "OK"}},
                ],
            }

            record = distill_runbook_from_task(workspace, "Update config safely", result, agent_name="coding")

            self.assertEqual(record["status"], "OK")
            self.assertEqual(record["outcome"], "success")
            skill = workspace / "system" / "skills" / RUNBOOK_SKILL_NAME / "SKILL.md"
            meta = workspace / "system" / "skills" / RUNBOOK_SKILL_NAME / "_meta.json"
            template = workspace / "system" / "templates" / "runbook-sop.md"
            task_store = workspace / "runtime" / "tasks" / "tasks.json"
            skill_text = skill.read_text(encoding="utf-8")
            self.assertIn("Outcome: `success`", skill_text)
            self.assertIn("file_read:OK -> file_patch:OK", skill_text)
            self.assertNotIn("secret full file content", skill_text)
            self.assertTrue(template.exists())
            self.assertEqual(json.loads(meta.read_text(encoding="utf-8"))["name"], RUNBOOK_SKILL_NAME)
            tasks = json.loads(task_store.read_text(encoding="utf-8"))["tasks"]
            review = next(item for item in tasks if item["id"] == RUNBOOK_TASK_ID)
            self.assertEqual(review["name"], "Runbook Auto Distillation Review")
            self.assertEqual(review["agent"], "coding")
            self.assertEqual(review["workspace"], "demo.ws")

    def test_failed_task_creates_failure_caution_without_raw_error_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir) / "demo.ws"
            workspace.mkdir()
            result = {
                "exit_reason": "CURRENT_TASK_DONE",
                "turns": 9,
                "tool_results": [
                    {
                        "tool_name": "code_run",
                        "tool_call_id": "1",
                        "data": {
                            "status": "ERROR",
                            "error": "secret stack trace",
                            "stdout": "secret stdout",
                        },
                    }
                ],
            }

            record = distill_runbook_from_task(workspace, "Run migration", result, agent_name="main")

            self.assertEqual(record["outcome"], "failure")
            skill_text = (workspace / "system" / "skills" / RUNBOOK_SKILL_NAME / "SKILL.md").read_text(
                encoding="utf-8"
            )
            self.assertIn("Outcome: `failure`", skill_text)
            self.assertIn("code_run:ERROR", skill_text)
            self.assertIn("Failure trace distilled", skill_text)
            self.assertNotIn("secret stack trace", skill_text)
            self.assertNotIn("secret stdout", skill_text)

    def test_entries_are_deduped_and_capped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir) / "demo.ws"
            workspace.mkdir()
            ok_result = {"exit_reason": "CURRENT_TASK_DONE", "turns": 10, "tool_results": []}

            distill_runbook_from_task(workspace, "Task A", ok_result, max_entries=2)
            distill_runbook_from_task(workspace, "Task A", ok_result, max_entries=2)
            distill_runbook_from_task(workspace, "Task B", ok_result, max_entries=2)
            distill_runbook_from_task(workspace, "Task C", ok_result, max_entries=2)

            skill_text = (workspace / "system" / "skills" / RUNBOOK_SKILL_NAME / "SKILL.md").read_text(
                encoding="utf-8"
            )
            self.assertEqual(skill_text.count("- Key: `"), 2)
            self.assertNotIn("### Task A", skill_text)
            self.assertIn("### Task B", skill_text)
            self.assertIn("### Task C", skill_text)

    def test_concurrent_distillation_preserves_all_runbook_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir) / "demo.ws"
            workspace.mkdir()
            result = {"exit_reason": "CURRENT_TASK_DONE", "turns": 10, "tool_results": []}

            with ThreadPoolExecutor(max_workers=20) as pool:
                records = list(
                    pool.map(
                        lambda index: distill_runbook_from_task(
                            workspace,
                            f"Task {index}",
                            result,
                            max_entries=30,
                        ),
                        range(20),
                    )
                )

            skill_text = (workspace / "system" / "skills" / RUNBOOK_SKILL_NAME / "SKILL.md").read_text(
                encoding="utf-8"
            )
            self.assertTrue(all(record["status"] == "OK" for record in records))
            self.assertEqual(skill_text.count("- Key: `"), 20)

    def test_template_creation_preserves_existing_user_template(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir)
            template = workspace / "system" / "templates" / "runbook-sop.md"
            template.parent.mkdir(parents=True)
            template.write_text("custom template", encoding="utf-8")

            ensure_runbook_template(workspace)

            self.assertEqual(template.read_text(encoding="utf-8"), "custom template")

    def test_review_task_upsert_preserves_unrelated_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir) / "demo.ws"
            task_store = workspace / "runtime" / "tasks" / "tasks.json"
            task_store.parent.mkdir(parents=True)
            task_store.write_text(
                json.dumps({"tasks": [{"id": "keep", "name": "Keep me"}]}, ensure_ascii=False),
                encoding="utf-8",
            )

            distill_runbook_from_task(
                workspace,
                "Task",
                {"exit_reason": "CURRENT_TASK_DONE", "turns": 10, "tool_results": []},
            )
            distill_runbook_from_task(
                workspace,
                "Task again",
                {"exit_reason": "CURRENT_TASK_DONE", "turns": 10, "tool_results": []},
            )

            tasks = json.loads(task_store.read_text(encoding="utf-8"))["tasks"]
            self.assertEqual(len([item for item in tasks if item["id"] == RUNBOOK_TASK_ID]), 1)
            self.assertTrue(any(item["id"] == "keep" for item in tasks))


class XAgentRunbookIntegrationTests(unittest.TestCase):
    def test_run_task_skips_distillation_when_interaction_records_are_low(self) -> None:
        class DummyClient:
            backend = type("Backend", (), {"history": []})()

            def chat(self, messages, tools):  # noqa: ANN001
                del messages, tools
                return ChatResponse(thinking="", content="done", tool_calls=[])

        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir) / "demo.ws"
            agent = XAgent(
                system_prompt="sys",
                tools_schema=[],
                workspace_dir=str(workspace),
                skills_dir=str(workspace / "system" / "skills"),
            )
            agent.client = DummyClient()

            result = agent.run_task("Summarize workspace")

            self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")
            self.assertFalse((workspace / "system" / "skills" / RUNBOOK_SKILL_NAME / "SKILL.md").exists())

    def test_run_task_distills_after_enough_interactions_and_reloads_workspace_skill(self) -> None:
        class DummyClient:
            backend = type("Backend", (), {"history": []})()

            def chat(self, messages, tools):  # noqa: ANN001
                del messages, tools
                return ChatResponse(thinking="", content="done", tool_calls=[])

        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir) / "demo.ws"
            agent = XAgent(
                system_prompt="sys",
                tools_schema=[],
                workspace_dir=str(workspace),
                skills_dir=str(workspace / "system" / "skills"),
                runbook_min_interaction_records=2,
            )
            agent.client = DummyClient()

            result = agent.run_task("Summarize workspace")

            self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")
            skill_text = (workspace / "system" / "skills" / RUNBOOK_SKILL_NAME / "SKILL.md").read_text(
                encoding="utf-8"
            )
            self.assertIn("llm_end:1", skill_text)
            self.assertIn("run_end:1", skill_text)
            self.assertIn(RUNBOOK_SKILL_NAME, agent.skill_registry.skills)


class TaskCheckpointTests(unittest.TestCase):
    def test_checkpoint_writes_latest_with_plan_tools_files_and_pending_steps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir) / "demo.ws"
            workspace.mkdir()
            target = workspace / "business" / "config.txt"
            target.parent.mkdir()
            target.write_text("value=1", encoding="utf-8")
            (workspace / "plan.md").write_text("- [x] inspect\n- [ ] patch config\n", encoding="utf-8")
            checkpoint = build_task_checkpoint(
                workspace,
                checkpoint_id="abc",
                session_id="abc",
                task="Update config",
                turn=3,
                status="running",
                tool_results=[
                    {
                        "tool_name": "file_read",
                        "tool_call_id": "1",
                        "data": {
                            "status": "OK",
                            "path": str(target),
                            "content": "secret full content",
                        },
                    }
                ],
                pending_prompts=["继续验证 patch"],
            )

            record = write_task_checkpoint(workspace, checkpoint)
            loaded = load_task_checkpoint(workspace, "latest")

            self.assertEqual(record["status"], "OK")
            self.assertEqual(loaded["status"], "OK")
            payload = loaded["checkpoint"]
            self.assertEqual(payload["checkpoint_id"], "abc")
            self.assertIn("- [ ] patch config", payload["plan"])
            self.assertIn("patch config", payload["pending_steps"][0])
            self.assertIn("Loop prompt", payload["pending_steps"][1])
            self.assertEqual(payload["tool_results"][0]["tool_name"], "file_read")
            self.assertNotIn("content", payload["tool_results"][0])
            self.assertEqual(payload["file_states"][0]["path"], "business/config.txt")
            self.assertEqual(payload["file_states"][0]["sha256"], "a777d5a2d0a4836c7b44b4514048e97ef61b5b127fb8b6479f0337d0b160fe0b")

    def test_resume_prompt_renders_checkpoint_context(self) -> None:
        checkpoint = {
            "checkpoint_id": "abc",
            "status": "interrupted",
            "turn": 5,
            "exit_reason": "INTERRUPTED",
            "task": "Original long task",
            "plan": "- [ ] finish",
            "pending_steps": ["finish"],
            "tool_results": [{"tool_name": "file_read", "status": "OK", "path": "business/a.txt"}],
            "file_states": [{"path": "business/a.txt", "exists": True, "size": 3, "mtime_ns": 1, "sha256": "hash"}],
        }

        prompt = render_resume_prompt(checkpoint, "用户补充")

        self.assertIn("[Resume Checkpoint]", prompt)
        self.assertIn("Original long task", prompt)
        self.assertIn("不要重放已完成工具动作", prompt)
        self.assertIn("用户补充", prompt)

    def test_agent_loop_invokes_checkpoint_callback_during_run(self) -> None:
        class DummyClient:
            backend = type("Backend", (), {"history": []})()

            def __init__(self) -> None:
                self.calls = 0

            def chat(self, messages, tools):  # noqa: ANN001
                del messages, tools
                self.calls += 1
                if self.calls == 1:
                    return ChatResponse(
                        thinking="",
                        content="",
                        tool_calls=[ToolCall(name="echo", args={}, id="1")],
                    )
                return ChatResponse(thinking="", content="done", tool_calls=[])

        class Handler(BaseHandler):
            def exec_echo(self, args):  # noqa: ANN001
                del args
                return ActionResult(data={"status": "OK", "path": "business/a.txt"}, next_prompt="continue")

        from src.core.agent_loop import ActionResult

        snapshots: list[dict[str, object]] = []
        handler = Handler(ctx=AgentContext(checkpoint_callback=snapshots.append))

        result = run_agent_loop(
            client=DummyClient(),
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=3,
        )

        self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")
        self.assertGreaterEqual(len(snapshots), 3)
        self.assertEqual(snapshots[0]["status"], "running")
        self.assertEqual(snapshots[-1]["status"], "completed")
        self.assertEqual(snapshots[-1]["exit_reason"], "CURRENT_TASK_DONE")
        self.assertEqual(snapshots[-1]["tool_results"][0]["tool_name"], "echo")

    def test_agent_loop_marks_interrupted_checkpoint(self) -> None:
        class DummyClient:
            backend = type("Backend", (), {"history": []})()

            def chat(self, messages, tools):  # noqa: ANN001
                del messages, tools
                return ChatResponse(thinking="", content="done", tool_calls=[])

        snapshots: list[dict[str, object]] = []
        handler = BaseHandler(ctx=AgentContext(checkpoint_callback=snapshots.append, code_stop_signal=True))

        result = run_agent_loop(
            client=DummyClient(),
            system_prompt="sys",
            user_input="go",
            handler=handler,
            tools_schema=[],
            max_turns=3,
        )

        self.assertEqual(result["exit_reason"], "INTERRUPTED")
        self.assertEqual(snapshots[-1]["status"], "interrupted")
        self.assertEqual(snapshots[-1]["exit_reason"], "INTERRUPTED")

    def test_xagent_resume_task_injects_latest_checkpoint(self) -> None:
        class DummyClient:
            backend = type("Backend", (), {"history": []})()

            def __init__(self) -> None:
                self.messages = None

            def chat(self, messages, tools):  # noqa: ANN001
                del tools
                self.messages = messages
                return ChatResponse(thinking="", content="done", tool_calls=[])

        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir) / "demo.ws"
            workspace.mkdir()
            checkpoint = build_task_checkpoint(
                workspace,
                checkpoint_id="abc",
                session_id="abc",
                task="Original task",
                turn=2,
                status="interrupted",
                exit_reason="INTERRUPTED",
            )
            write_task_checkpoint(workspace, checkpoint)
            agent = XAgent(
                system_prompt="sys",
                tools_schema=[],
                workspace_dir=str(workspace),
                runbook_min_interaction_records=99,
            )
            dummy = DummyClient()
            agent.client = dummy

            result = agent.resume_task("latest", "继续")

            self.assertEqual(result["exit_reason"], "CURRENT_TASK_DONE")
            assert dummy.messages is not None
            self.assertIn("[Resume Checkpoint]", dummy.messages[1]["content"])
            self.assertIn("Original task", dummy.messages[1]["content"])

    def test_xagent_writes_failed_checkpoint_when_loop_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir) / "demo.ws"
            agent = XAgent(
                system_prompt="sys",
                tools_schema=[],
                workspace_dir=str(workspace),
                runbook_min_interaction_records=99,
            )

            with patch("src.core.XAgent.run_agent_loop", side_effect=RuntimeError("boom")):
                result = agent.run_task("Long task")

            loaded = load_task_checkpoint(workspace, "latest")
            self.assertEqual(result["exit_reason"], "ERROR")
            self.assertEqual(loaded["status"], "OK")
            checkpoint = loaded["checkpoint"]
            self.assertEqual(checkpoint["status"], "failed")
            self.assertEqual(checkpoint["exit_reason"], "ERROR")
            self.assertEqual(checkpoint["tool_results"][0]["tool_name"], "run_task")


if __name__ == "__main__":
    unittest.main()
