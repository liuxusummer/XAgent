from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.core.agent_loop import AgentContext
from src.core.llm import ChatResponse
from src.core.memory import load_boot_memory, load_global_memory, load_memory_sop
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

    def test_periodic_inject_uses_global_memory_when_non_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "global_mem.txt").write_text("remember this", encoding="utf-8")
            handler = XAgentHandler(ctx=AgentContext(memory_root=str(root), current_turn=10))

            prompt = handler._periodic_inject_hook(  # noqa: SLF001
                response=ChatResponse(thinking="", content="", tool_calls=[]),
                tool_results=[],
                ctx=handler.ctx,
            )

            self.assertEqual(prompt, "[Memory Refresh]\nremember this")

    def test_periodic_inject_skips_empty_global_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "global_mem.txt").write_text("  \n", encoding="utf-8")
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


if __name__ == "__main__":
    unittest.main()
