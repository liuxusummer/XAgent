from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.tools.file_index import refresh_file_index, search_file_index


class FileIndexTests(unittest.TestCase):
    def test_first_search_creates_index_and_returns_content_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "src").mkdir()
            (root / "src" / "app.py").write_text("def target_symbol():\n    return 1\n", encoding="utf-8")

            result = search_file_index(query="target_symbol", cwd=str(root))

            self.assertEqual(result["status"], "OK")
            self.assertTrue((root / "runtime" / "file_index.sqlite3").exists())
            self.assertTrue(result["refreshed"])
            self.assertEqual(result["matches"][0]["path"], "src/app.py")
            self.assertEqual(result["matches"][0]["line"], 1)
            self.assertIn("target_symbol", result["matches"][0]["snippet"])

    def test_path_only_search_uses_file_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "docs").mkdir()
            (root / "docs" / "file-index-plan.md").write_text("no keyword here\n", encoding="utf-8")

            result = search_file_index(query="file-index", cwd=str(root), path_only=True)

            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["matches"][0]["path"], "docs/file-index-plan.md")
            self.assertEqual(result["matches"][0]["match_type"], "path")

    def test_refresh_updates_modified_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "note.txt"
            target.write_text("old phrase\n", encoding="utf-8")
            self.assertEqual(search_file_index(query="old", cwd=str(root))["status"], "OK")

            target.write_text("new phrase\n", encoding="utf-8")
            result = search_file_index(query="new", cwd=str(root), refresh=True)

            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["matches"][0]["path"], "note.txt")
            self.assertIn("new phrase", result["matches"][0]["snippet"])

    def test_refresh_removes_deleted_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "gone.txt"
            target.write_text("vanishing_token\n", encoding="utf-8")
            self.assertEqual(search_file_index(query="vanishing_token", cwd=str(root))["status"], "OK")

            target.unlink()
            result = search_file_index(query="vanishing_token", cwd=str(root), refresh=True)

            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["matches"], [])

    def test_refresh_skips_binary_large_and_excluded_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "node_modules").mkdir()
            (root / "node_modules" / "pkg.txt").write_text("hidden_token\n", encoding="utf-8")
            (root / "binary.bin").write_bytes(b"\x00\x01\x02")
            (root / "large.txt").write_text("x" * 50, encoding="utf-8")
            (root / "visible.txt").write_text("visible_token\n", encoding="utf-8")

            refresh = refresh_file_index(cwd=str(root), max_file_bytes=20)
            result = search_file_index(query="token", cwd=str(root))

            self.assertEqual(refresh["status"], "OK")
            self.assertGreaterEqual(refresh["skipped"]["directories"], 1)
            self.assertEqual(refresh["skipped"]["too_large"], 1)
            self.assertEqual([match["path"] for match in result["matches"]], ["visible.txt"])

    def test_root_outside_workspace_returns_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp) / "workspace"
            outside = Path(tmp) / "outside"
            workspace.mkdir()
            outside.mkdir()

            result = search_file_index(query="anything", cwd=str(workspace), root=str(outside))

            self.assertEqual(result["status"], "ERROR")
            self.assertIn("inside workspace", result["error"])

    def test_special_character_query_does_not_crash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "sample.txt").write_text("alpha beta\n", encoding="utf-8")

            result = search_file_index(query="alpha/beta:(", cwd=str(root))

            self.assertEqual(result["status"], "OK")

    def test_search_root_filters_results_before_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a").mkdir()
            (root / "b").mkdir()
            for idx in range(5):
                (root / "a" / f"item{idx}.txt").write_text("needle\n", encoding="utf-8")
            (root / "b" / "target.txt").write_text("needle\n", encoding="utf-8")
            self.assertEqual(refresh_file_index(cwd=str(root))["status"], "OK")

            result = search_file_index(query="needle", cwd=str(root), root="b", limit=1)

            self.assertEqual(result["status"], "OK")
            self.assertEqual([match["path"] for match in result["matches"]], ["b/target.txt"])


if __name__ == "__main__":
    unittest.main()
