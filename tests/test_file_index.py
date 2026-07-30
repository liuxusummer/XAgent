from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.core.agent_kernel import Principal
from src.tools.file_index import (
    DEFAULT_MAX_FILE_BYTES,
    refresh_file_index as _refresh_file_index,
    search_file_index as _search_file_index,
)


_TEST_PRINCIPAL = Principal(
    subject="file-index-test",
    tenant_id="tenant-test",
    session_id="session-test",
    run_id="run-test",
    scopes=("workspace.read",),
)


def refresh_file_index(*args, **kwargs):
    kwargs.setdefault("principal", _TEST_PRINCIPAL)
    return _refresh_file_index(*args, **kwargs)


def search_file_index(*args, **kwargs):
    kwargs.setdefault("principal", _TEST_PRINCIPAL)
    return _search_file_index(*args, **kwargs)


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

    def test_index_excludes_all_managed_memory_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "memory").mkdir()
            (root / "system" / "memory").mkdir(parents=True)
            (root / "system" / "agents" / "main").mkdir(parents=True)
            (root / "memory" / "global_mem.txt").write_text(
                "LEGACY_MEMORY_SECRET",
                encoding="utf-8",
            )
            (root / "system" / "memory" / "project.md").write_text(
                "PROJECT_MEMORY_SECRET",
                encoding="utf-8",
            )
            (
                root / "system" / "agents" / "main" / "MEMORY.md"
            ).write_text(
                "AGENT_MEMORY_SECRET",
                encoding="utf-8",
            )
            (root / "public.txt").write_text(
                "PUBLIC_INDEX_TOKEN",
                encoding="utf-8",
            )

            refresh = refresh_file_index(cwd=str(root))
            secret = search_file_index(
                query="MEMORY_SECRET",
                cwd=str(root),
            )
            public = search_file_index(
                query="PUBLIC_INDEX_TOKEN",
                cwd=str(root),
            )

            self.assertEqual(refresh["status"], "OK")
            self.assertEqual(secret["matches"], [])
            self.assertEqual(
                public["matches"][0]["path"],
                "public.txt",
            )

    def test_mixed_case_memory_paths_are_never_indexed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            secret = root / "SYSTEM" / "MEMORY" / "secret.md"
            secret.parent.mkdir(parents=True)
            secret.write_text("MEMORY-INDEX-SECRET", encoding="utf-8")

            refresh = refresh_file_index(cwd=str(root))
            result = search_file_index(
                query="MEMORY-INDEX-SECRET",
                cwd=str(root),
            )
            protected_root = refresh_file_index(
                cwd=str(root),
                root="SYSTEM/MEMORY",
            )

            self.assertEqual(refresh["status"], "OK")
            self.assertEqual(result["matches"], [])
            self.assertEqual(protected_root["status"], "ERROR")
            self.assertIn("protected", protected_root["error"])

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

    def test_search_returns_content_bound_evidence_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            content = "evidence needle\n"
            (root / "evidence.txt").write_text(content, encoding="utf-8")

            result = search_file_index(
                query="needle",
                cwd=str(root),
                mode="keyword",
            )

            self.assertEqual(result["status"], "OK")
            match = result["matches"][0]
            bundle = result["evidence_bundle"]
            self.assertEqual(match["evidence_id"], bundle["items"][0]["evidence_id"])
            self.assertEqual(
                match["content_sha256"],
                hashlib.sha256(content.encode("utf-8")).hexdigest(),
            )
            self.assertEqual(match["index_version"], result["index_version"])
            self.assertEqual(
                bundle["principal_digest"],
                _TEST_PRINCIPAL.principal_digest,
            )
            self.assertEqual(
                bundle["items"][0]["knowledge"]["acl"],
                ["tenant:tenant-test"],
            )

    def test_missing_principal_fails_closed_without_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "secret.txt").write_text("tenant secret", encoding="utf-8")

            result = _search_file_index(query="secret", cwd=str(root))

            self.assertEqual(result["status"], "ERROR")
            self.assertEqual(result["reason_code"], "principal_required")
            self.assertEqual(result["matches"], [])
            self.assertFalse((root / "runtime" / "file_index.sqlite3").exists())

    def test_missing_workspace_read_scope_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "secret.txt").write_text("tenant secret", encoding="utf-8")
            principal = Principal(
                subject="user",
                tenant_id="tenant-test",
                session_id="session",
                run_id="run",
                scopes=(),
            )

            result = _search_file_index(
                query="secret",
                cwd=str(root),
                principal=principal,
            )

            self.assertEqual(result["status"], "ERROR")
            self.assertEqual(result["reason_code"], "principal_scope_missing")
            self.assertEqual(result["matches"], [])
            self.assertFalse((root / "runtime" / "file_index.sqlite3").exists())

    def test_different_tenant_cannot_search_existing_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "secret.txt").write_text("tenant secret", encoding="utf-8")
            self.assertEqual(
                search_file_index(query="secret", cwd=str(root))["status"],
                "OK",
            )
            other = Principal(
                subject="other-user",
                tenant_id="other-tenant",
                session_id="other-session",
                run_id="other-run",
                scopes=("workspace.read",),
            )

            denied = _search_file_index(
                query="secret",
                cwd=str(root),
                principal=other,
            )

            self.assertEqual(denied["status"], "ERROR")
            self.assertEqual(denied["reason_code"], "index_acl_denied")
            self.assertEqual(denied["matches"], [])
            self.assertNotIn("tenant secret", str(denied))

    def test_missing_acl_metadata_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "secret.txt").write_text("tenant secret", encoding="utf-8")
            self.assertEqual(
                search_file_index(query="secret", cwd=str(root))["status"],
                "OK",
            )
            with sqlite3.connect(root / "runtime" / "file_index.sqlite3") as conn:
                conn.execute(
                    "DELETE FROM file_index_meta WHERE key = 'owner_acl'"
                )
                conn.commit()

            denied = search_file_index(query="secret", cwd=str(root))

            self.assertEqual(denied["status"], "ERROR")
            self.assertEqual(denied["reason_code"], "index_acl_denied")
            self.assertEqual(denied["matches"], [])

    def test_unknown_schema_version_fails_closed_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "secret.txt").write_text("tenant secret", encoding="utf-8")
            self.assertEqual(
                search_file_index(query="secret", cwd=str(root))["status"],
                "OK",
            )
            db_path = root / "runtime" / "file_index.sqlite3"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "UPDATE file_index_meta SET value = '999' "
                    "WHERE key = 'schema_version'"
                )
                conn.commit()

            denied = search_file_index(query="secret", cwd=str(root))

            self.assertEqual(denied["status"], "ERROR")
            self.assertEqual(denied["reason_code"], "index_acl_denied")
            self.assertEqual(denied["matches"], [])
            with sqlite3.connect(db_path) as conn:
                stored = conn.execute(
                    "SELECT value FROM file_index_meta "
                    "WHERE key = 'schema_version'"
                ).fetchone()[0]
            self.assertEqual(stored, "999")

    def test_schema_v4_rebuild_drops_content_now_classified_as_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy = root / "legacy.txt"
            legacy.write_text("MIGRATED_MEMORY_SECRET", encoding="utf-8")
            self.assertTrue(
                search_file_index(
                    query="MIGRATED_MEMORY_SECRET",
                    cwd=str(root),
                )["matches"]
            )
            legacy.unlink()
            memory = root / "system" / "memory"
            memory.mkdir(parents=True)
            (memory / "project.md").write_text(
                "MIGRATED_MEMORY_SECRET",
                encoding="utf-8",
            )
            db_path = root / "runtime" / "file_index.sqlite3"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "UPDATE file_index_meta SET value = '3' "
                    "WHERE key = 'schema_version'"
                )
                conn.commit()

            result = search_file_index(
                query="MIGRATED_MEMORY_SECRET",
                cwd=str(root),
            )

            self.assertEqual(result["status"], "OK")
            self.assertTrue(result["refreshed"])
            self.assertEqual(result["matches"], [])
            with sqlite3.connect(db_path) as conn:
                stored = conn.execute(
                    "SELECT value FROM file_index_meta "
                    "WHERE key = 'schema_version'"
                ).fetchone()[0]
            self.assertEqual(stored, "4")

    def test_missing_schema_metadata_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "secret.txt").write_text(
                "tenant secret",
                encoding="utf-8",
            )
            self.assertEqual(
                search_file_index(query="secret", cwd=str(root))["status"],
                "OK",
            )
            db_path = root / "runtime" / "file_index.sqlite3"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "DELETE FROM file_index_meta "
                    "WHERE key = 'schema_version'"
                )
                conn.commit()

            denied = search_file_index(query="secret", cwd=str(root))

            self.assertEqual(denied["status"], "ERROR")
            self.assertEqual(denied["reason_code"], "index_acl_denied")
            self.assertEqual(denied["matches"], [])

    def test_content_digest_mismatch_fails_closed_without_snippet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "evidence.txt").write_text(
                "trusted evidence\n",
                encoding="utf-8",
            )
            self.assertEqual(
                search_file_index(query="evidence", cwd=str(root))["status"],
                "OK",
            )
            with sqlite3.connect(root / "runtime" / "file_index.sqlite3") as conn:
                conn.execute(
                    "UPDATE file_index_files SET content = ? "
                    "WHERE path = 'evidence.txt'",
                    ("tampered secret",),
                )
                conn.commit()

            denied = search_file_index(
                query="evidence.txt",
                cwd=str(root),
                path_only=True,
            )

            self.assertEqual(denied["status"], "ERROR")
            self.assertEqual(denied["reason_code"], "index_acl_denied")
            self.assertEqual(denied["matches"], [])
            self.assertNotIn("tampered secret", str(denied))


if __name__ == "__main__":
    unittest.main()
