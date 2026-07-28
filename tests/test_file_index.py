from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.tools.file_index import (
    DEFAULT_MAX_FILE_BYTES,
    refresh_file_index,
    search_file_index,
)


class FakeEmbeddingProvider:
    def __init__(self) -> None:
        self.calls = 0

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        self.calls += 1
        vectors = []
        for text in texts:
            lowered = text.lower()
            if any(term in lowered for term in ("login", "signin", "sign in", "auth", "authentication")):
                vectors.append([1.0, 0.0, 0.0])
            elif any(term in lowered for term in ("billing", "invoice", "payment")):
                vectors.append([0.0, 1.0, 0.0])
            else:
                vectors.append([0.0, 0.0, 1.0])
        return vectors


EMBEDDING_CONFIG = {
    "file_index_embedding": {
        "enabled": True,
        "apibase": "https://example.com/v1/embeddings",
        "model": "fake-embedding",
        "dimension": 3,
    }
}


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

    def test_refresh_skips_external_and_recursive_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            container = Path(tmp)
            root = container / "workspace"
            root.mkdir()
            outside = container / "outside.txt"
            outside.write_text("outside_secret\n", encoding="utf-8")
            (root / "visible.txt").write_text("visible_token\n", encoding="utf-8")
            (root / "external.txt").symlink_to(outside)
            (root / "recursive").symlink_to(root, target_is_directory=True)

            refresh = refresh_file_index(cwd=str(root))
            outside_result = search_file_index(query="outside_secret", cwd=str(root))
            visible_result = search_file_index(query="visible_token", cwd=str(root))

            self.assertEqual(refresh["status"], "OK")
            self.assertEqual(refresh["skipped"]["symlinks"], 2)
            self.assertEqual(outside_result["matches"], [])
            self.assertEqual(visible_result["matches"][0]["path"], "visible.txt")

    def test_refresh_applies_default_file_size_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            large = root / "large.txt"
            with large.open("wb") as stream:
                stream.truncate(DEFAULT_MAX_FILE_BYTES + 1)

            refresh = refresh_file_index(cwd=str(root))

            self.assertEqual(refresh["status"], "OK")
            self.assertEqual(refresh["skipped"]["too_large"], 1)
            self.assertEqual(refresh["indexed"], 0)

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

    def test_semantic_search_uses_fake_embedding_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "auth.md").write_text("Authentication module accepts login credentials.\n", encoding="utf-8")
            (root / "billing.md").write_text("Invoices and payment collection live here.\n", encoding="utf-8")

            result = search_file_index(
                query="sign in flow",
                cwd=str(root),
                refresh=True,
                mode="semantic",
                embedding_config=EMBEDDING_CONFIG,
                embedding_provider=FakeEmbeddingProvider(),
            )

            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["semantic_status"]["status"], "OK")
            self.assertEqual(result["matches"][0]["path"], "auth.md")
            self.assertEqual(result["matches"][0]["match_type"], "semantic")
            self.assertIn("signals", result["matches"][0])

    def test_semantic_search_can_return_multiple_chunks_from_same_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "auth.md").write_text(
                "# Login\nAuthentication module accepts login credentials.\n"
                "# Sign in\nSign in flow validates authentication tokens.\n",
                encoding="utf-8",
            )

            result = search_file_index(
                query="sign in flow",
                cwd=str(root),
                refresh=True,
                mode="semantic",
                limit=2,
                embedding_config=EMBEDDING_CONFIG,
                embedding_provider=FakeEmbeddingProvider(),
            )

            self.assertEqual(result["status"], "OK")
            self.assertEqual([match["path"] for match in result["matches"]], ["auth.md", "auth.md"])
            self.assertEqual([match["start_line"] for match in result["matches"]], [1, 3])

    def test_hybrid_search_merges_keyword_and_semantic_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "auth.md").write_text("Authentication module accepts login credentials.\n", encoding="utf-8")
            (root / "billing.md").write_text("Invoices and payment collection live here.\n", encoding="utf-8")

            result = search_file_index(
                query="billing sign in",
                cwd=str(root),
                refresh=True,
                mode="hybrid",
                embedding_config=EMBEDDING_CONFIG,
                embedding_provider=FakeEmbeddingProvider(),
            )

            self.assertEqual(result["status"], "OK")
            paths = [match["path"] for match in result["matches"]]
            self.assertIn("auth.md", paths)
            self.assertIn("billing.md", paths)
            self.assertTrue(all(match["match_type"] == "hybrid" for match in result["matches"]))

    def test_semantic_root_filters_results_before_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a").mkdir()
            (root / "b").mkdir()
            (root / "a" / "auth.md").write_text("Authentication module accepts login credentials.\n", encoding="utf-8")
            (root / "b" / "auth.md").write_text("Authentication module accepts login credentials.\n", encoding="utf-8")

            result = search_file_index(
                query="sign in",
                cwd=str(root),
                root="b",
                refresh=True,
                mode="semantic",
                limit=1,
                embedding_config=EMBEDDING_CONFIG,
                embedding_provider=FakeEmbeddingProvider(),
            )

            self.assertEqual(result["status"], "OK")
            self.assertEqual([match["path"] for match in result["matches"]], ["b/auth.md"])

    def test_semantic_refresh_removes_deleted_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "auth.md"
            target.write_text("Authentication module accepts login credentials.\n", encoding="utf-8")
            self.assertEqual(
                search_file_index(
                    query="sign in",
                    cwd=str(root),
                    refresh=True,
                    mode="semantic",
                    embedding_config=EMBEDDING_CONFIG,
                    embedding_provider=FakeEmbeddingProvider(),
                )["status"],
                "OK",
            )

            target.unlink()
            result = search_file_index(
                query="sign in",
                cwd=str(root),
                refresh=True,
                mode="semantic",
                embedding_config=EMBEDDING_CONFIG,
                embedding_provider=FakeEmbeddingProvider(),
            )

            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["matches"], [])

    def test_hybrid_search_does_not_embed_query_when_semantic_index_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "target.txt").write_text("needle\n", encoding="utf-8")
            self.assertEqual(search_file_index(query="needle", cwd=str(root), mode="keyword")["status"], "OK")
            provider = FakeEmbeddingProvider()

            result = search_file_index(
                query="needle",
                cwd=str(root),
                mode="hybrid",
                embedding_config=EMBEDDING_CONFIG,
                embedding_provider=provider,
            )

            self.assertEqual(result["status"], "OK")
            self.assertEqual(provider.calls, 0)
            self.assertEqual(result["semantic_status"]["status"], "EMPTY")
            self.assertEqual(result["matches"][0]["path"], "target.txt")

    def test_long_single_line_text_is_split_into_multiple_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "long.txt").write_text("authentication " * 700, encoding="utf-8")

            refresh = refresh_file_index(
                cwd=str(root),
                semantic=True,
                embedding_config=EMBEDDING_CONFIG,
                embedding_provider=FakeEmbeddingProvider(),
            )

            self.assertEqual(refresh["status"], "OK")
            with sqlite3.connect(root / "runtime" / "file_index.sqlite3") as conn:
                count, max_len = conn.execute(
                    "SELECT COUNT(*), max(length(content)) FROM file_index_chunks WHERE path = 'long.txt'"
                ).fetchone()
            self.assertGreater(count, 1)
            self.assertLessEqual(max_len, 3000)

    def test_failed_embedding_refresh_removes_partial_chunks(self) -> None:
        class FailingProvider:
            def embed_texts(self, texts: list[str]) -> list[list[float]]:
                raise RuntimeError("boom")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "target.txt").write_text("needle\n", encoding="utf-8")

            refresh = refresh_file_index(
                cwd=str(root),
                semantic=True,
                embedding_config=EMBEDDING_CONFIG,
                embedding_provider=FailingProvider(),
            )

            self.assertEqual(refresh["status"], "OK")
            self.assertEqual(refresh["semantic_status"]["status"], "ERROR")
            with sqlite3.connect(root / "runtime" / "file_index.sqlite3") as conn:
                count = conn.execute("SELECT COUNT(*) FROM file_index_chunks WHERE path = 'target.txt'").fetchone()[0]
            self.assertEqual(count, 0)

    def test_semantic_failure_degrades_to_keyword_results(self) -> None:
        class FailingProvider:
            def embed_texts(self, texts: list[str]) -> list[list[float]]:
                raise RuntimeError("boom")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "target.txt").write_text("needle\n", encoding="utf-8")

            result = search_file_index(
                query="needle",
                cwd=str(root),
                refresh=True,
                mode="hybrid",
                embedding_config=EMBEDDING_CONFIG,
                embedding_provider=FailingProvider(),
            )

            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["matches"][0]["path"], "target.txt")
            self.assertEqual(result["refresh_stats"]["semantic_status"]["status"], "ERROR")
            self.assertEqual(result["semantic_status"]["status"], "EMPTY")


if __name__ == "__main__":
    unittest.main()
