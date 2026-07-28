from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.tools.file_ops import delete_file, patch_file, read_file, write_file


class FilePatchTests(unittest.TestCase):
    def test_patch_file_replaces_unique_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "sample.txt"
            target.write_text("alpha\nbeta\ngamma\n", encoding="utf-8")

            result = patch_file(
                path="sample.txt",
                old_content="beta\n",
                new_content="delta\n",
                cwd=str(root),
            )

            self.assertEqual(result["status"], "OK")
            self.assertEqual(target.read_text(encoding="utf-8"), "alpha\ndelta\ngamma\n")

    def test_patch_file_returns_error_when_no_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "sample.txt").write_text("alpha\nbeta\n", encoding="utf-8")

            result = patch_file(
                path="sample.txt",
                old_content="missing\n",
                new_content="delta\n",
                cwd=str(root),
            )

            self.assertEqual(result["status"], "ERROR")
            self.assertIn("matched 0 times", result["error"])

    def test_patch_file_returns_error_when_match_is_not_unique(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "sample.txt").write_text("same\nsame\n", encoding="utf-8")

            result = patch_file(
                path="sample.txt",
                old_content="same\n",
                new_content="delta\n",
                cwd=str(root),
            )

            self.assertEqual(result["status"], "ERROR")
            self.assertEqual(result["match_count"], 2)

    def test_patch_file_rejects_empty_old_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "sample.txt"
            target.write_text("alpha\n", encoding="utf-8")

            result = patch_file(
                path="sample.txt",
                old_content="",
                new_content="delta\n",
                cwd=str(root),
            )

            self.assertEqual(result["status"], "ERROR")
            self.assertIn("must not be empty", result["error"])
            self.assertEqual(target.read_text(encoding="utf-8"), "alpha\n")

    def test_patch_file_expands_file_refs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "source.txt").write_text("line1\nline2\nline3\n", encoding="utf-8")
            target = root / "sample.txt"
            target.write_text("before\nline2\nafter\n", encoding="utf-8")

            result = patch_file(
                path="sample.txt",
                old_content="{{file:source.txt:2:2}}",
                new_content="patched\n",
                cwd=str(root),
            )

            self.assertEqual(result["status"], "OK")
            self.assertEqual(target.read_text(encoding="utf-8"), "before\npatched\nafter\n")

    def test_read_file_supports_line_range(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "sample.txt"
            target.write_text("line1\nline2\nline3\nline4\n", encoding="utf-8")

            result = read_file(
                path="sample.txt",
                cwd=str(root),
                start_line=2,
                end_line=3,
            )

            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["content"], "line2\nline3\n")

    def test_write_file_expands_file_refs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "source.txt").write_text("A\nB\nC\n", encoding="utf-8")

            result = write_file(
                path="out.txt",
                content="head\n{{file:source.txt:2:3}}tail\n",
                cwd=str(root),
            )

            self.assertEqual(result["status"], "OK")
            self.assertEqual(
                (root / "out.txt").read_text(encoding="utf-8"),
                "head\nB\nC\ntail\n",
            )

    def test_write_file_defaults_to_workspace_root_not_business(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "business").mkdir()

            result = write_file(path="foo.txt", content="root file", cwd=str(root))

            self.assertEqual(result["status"], "OK")
            self.assertEqual((root / "foo.txt").read_text(encoding="utf-8"), "root file")
            self.assertFalse((root / "business" / "foo.txt").exists())

    def test_write_file_returns_error_for_directory_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "out").mkdir()

            result = write_file(path="out", content="text", cwd=str(root))

            self.assertEqual(result["status"], "ERROR")
            self.assertIn("directory", result["error"])

    def test_write_file_returns_error_when_parent_is_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "parent").write_text("not a dir", encoding="utf-8")

            result = write_file(path="parent/out.txt", content="text", cwd=str(root))

            self.assertEqual(result["status"], "ERROR")

    def test_read_file_denies_outside_workspace_path_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "workspace"
            outside = Path(tmp_dir) / "outside.txt"
            root.mkdir()
            outside.write_text("secret", encoding="utf-8")

            result = read_file(path="../outside.txt", cwd=str(root))

            self.assertEqual(result["status"], "ERROR")
            self.assertEqual(result["operation"], "read")
            self.assertIn("explicit read authorization", result["error"])

    def test_read_file_allows_explicitly_authorized_outside_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "workspace"
            outside = Path(tmp_dir) / "outside"
            root.mkdir()
            outside.mkdir()
            (outside / "item.txt").write_text("ok", encoding="utf-8")

            result = read_file(path=str(outside), cwd=str(root), allow_outside=True)

            self.assertEqual(result["status"], "OK")
            self.assertTrue(result["is_directory"])
            self.assertIn("item.txt", result["content"])

    def test_write_file_denies_outside_workspace_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "workspace"
            outside = Path(tmp_dir) / "outside"
            root.mkdir()

            result = write_file(path=str(outside / "created.txt"), content="nope", cwd=str(root))

            self.assertEqual(result["status"], "ERROR")
            self.assertEqual(result["operation"], "write")
            self.assertIn("read-only", result["error"])
            self.assertFalse(outside.exists())

    def test_patch_file_denies_outside_workspace_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "workspace"
            outside = Path(tmp_dir) / "outside.txt"
            root.mkdir()
            outside.write_text("before\n", encoding="utf-8")

            result = patch_file(
                path=str(outside),
                old_content="before\n",
                new_content="after\n",
                cwd=str(root),
            )

            self.assertEqual(result["status"], "ERROR")
            self.assertEqual(result["operation"], "patch")
            self.assertEqual(outside.read_text(encoding="utf-8"), "before\n")

    def test_write_file_cannot_silently_expand_outside_workspace_file_ref(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "workspace"
            outside = Path(tmp_dir) / "outside.txt"
            root.mkdir()
            outside.write_text("A\nB\n", encoding="utf-8")

            result = write_file(
                path="inside.txt",
                content=f"copied\n{{{{file:{outside}:1:2}}}}",
                cwd=str(root),
            )

            self.assertEqual(result["status"], "ERROR")
            self.assertIn("explicit read authorization", result["error"])
            self.assertFalse((root / "inside.txt").exists())

    def test_concurrent_appends_do_not_lose_updates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            writes = 64

            def append(index: int) -> dict[str, object]:
                return write_file(
                    path="events.log",
                    content=f"{index}\n",
                    mode="append",
                    cwd=str(root),
                )

            with ThreadPoolExecutor(max_workers=16) as pool:
                results = list(pool.map(append, range(writes)))

            self.assertTrue(all(result["status"] == "OK" for result in results))
            lines = (root / "events.log").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), writes)
            self.assertEqual({int(line) for line in lines}, set(range(writes)))

    def test_delete_file_inside_workspace_deletes_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "delete-me.txt"
            target.write_text("bye", encoding="utf-8")

            result = delete_file(path="delete-me.txt", cwd=str(root))

            self.assertEqual(result["status"], "OK")
            self.assertFalse(target.exists())

    def test_delete_file_outside_workspace_is_denied(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "workspace"
            outside = Path(tmp_dir) / "outside.txt"
            root.mkdir()
            outside.write_text("keep", encoding="utf-8")

            result = delete_file(path=str(outside), cwd=str(root))

            self.assertEqual(result["status"], "ERROR")
            self.assertEqual(result["operation"], "delete")
            self.assertTrue(outside.exists())

    def test_file_read_allows_workspace_system_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "system" / "agents" / "main" / "AGENT.md"
            target.parent.mkdir(parents=True)
            target.write_text("system config", encoding="utf-8")

            result = read_file(path="system/agents/main/AGENT.md", cwd=str(root))

            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["content"], "system config")

    def test_write_file_denies_workspace_system_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "system" / "agents" / "main" / "AGENT.md"
            target.parent.mkdir(parents=True)
            target.write_text("keep", encoding="utf-8")

            result = write_file(path="system/agents/main/AGENT.md", content="change", cwd=str(root))

            self.assertEqual(result["status"], "ERROR")
            self.assertEqual(result["operation"], "write")
            self.assertIn("read-only", result["error"])
            self.assertEqual(target.read_text(encoding="utf-8"), "keep")

    def test_patch_file_denies_workspace_system_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "system" / "agents" / "main" / "AGENT.md"
            target.parent.mkdir(parents=True)
            target.write_text("before", encoding="utf-8")

            result = patch_file(
                path="system/agents/main/AGENT.md",
                old_content="before",
                new_content="after",
                cwd=str(root),
            )

            self.assertEqual(result["status"], "ERROR")
            self.assertEqual(result["operation"], "patch")
            self.assertIn("read-only", result["error"])
            self.assertEqual(target.read_text(encoding="utf-8"), "before")

    def test_delete_file_denies_workspace_system_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "system" / "agents" / "main" / "AGENT.md"
            target.parent.mkdir(parents=True)
            target.write_text("keep", encoding="utf-8")

            result = delete_file(path="system/agents/main/AGENT.md", cwd=str(root))

            self.assertEqual(result["status"], "ERROR")
            self.assertEqual(result["operation"], "delete")
            self.assertIn("cannot be deleted", result["error"])
            self.assertTrue(target.exists())

    def test_file_ops_allow_absolute_path_inside_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "inside.txt"
            target.write_text("ok", encoding="utf-8")

            result = read_file(path=str(target), cwd=str(root))

            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["content"], "ok")


if __name__ == "__main__":
    unittest.main()
