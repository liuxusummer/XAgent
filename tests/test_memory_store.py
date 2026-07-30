from __future__ import annotations

import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.core.agent_kernel import DataSensitivity, Principal, TrustLevel
from src.core.memory_store import (
    MEMORY_PROPOSE_SCOPE,
    MEMORY_READ_SCOPE,
    MEMORY_REVIEW_SCOPE,
    MAX_MEMORY_STORE_BYTES,
    MEMORY_STORE_PATH,
    MemoryKind,
    MemoryReviewStatus,
    MemoryStore,
    MemoryStoreError,
)


class MemoryStoreTests(unittest.TestCase):
    def _principal(
        self,
        subject: str = "user-1",
        tenant: str = "tenant-1",
        *,
        reviewer: bool = False,
    ) -> Principal:
        scopes = (
            (
                "workspace.read",
                MEMORY_PROPOSE_SCOPE,
                MEMORY_READ_SCOPE,
                MEMORY_REVIEW_SCOPE,
            )
            if reviewer
            else ("workspace.read", MEMORY_PROPOSE_SCOPE, MEMORY_READ_SCOPE)
        )
        return Principal(
            subject=subject,
            tenant_id=tenant,
            session_id=f"session-{subject}",
            run_id=f"run-{subject}",
            scopes=scopes,
        )

    def _propose(self, store: MemoryStore, *, trust: TrustLevel = TrustLevel.USER):
        return store.propose(
            principal=self._principal(),
            content="The repository uses file-backed state.",
            namespace=("tenant-1", "project-1"),
            kind=MemoryKind.SEMANTIC,
            source_refs=("event:run-1:turn-2",),
            trust=trust,
            confidence=0.9,
            sensitivity=DataSensitivity.INTERNAL,
            acl=("tenant:tenant-1",),
            now=10,
        )

    def test_candidate_is_not_active_until_separately_reviewed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(tmp)
            candidate = self._propose(store)
            self.assertEqual(candidate.review_status, MemoryReviewStatus.PENDING)
            self.assertEqual(
                store.active_records(principal=self._principal(), now=11),
                (),
            )

            resolved = store.review(
                candidate.candidate_id,
                reviewer=self._principal(reviewer=True),
                approve=True,
                reason="verified against repository source",
                now=12,
            )
            records = store.active_records(principal=self._principal(), now=13)
            self.assertEqual(resolved.review_status, MemoryReviewStatus.APPROVED)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].content, candidate.content)
            self.assertEqual(
                records[0].to_knowledge_item().content_sha256,
                candidate.content_sha256,
            )

    def test_tool_content_remains_quarantined_without_human_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(tmp)
            candidate = self._propose(store, trust=TrustLevel.TOOL_UNTRUSTED)
            self.assertEqual(candidate.trust, TrustLevel.TOOL_UNTRUSTED)
            self.assertEqual(
                store.active_records(principal=self._principal(), now=11),
                (),
            )
            persisted = json.loads(
                (Path(tmp) / MEMORY_STORE_PATH).read_text(encoding="utf-8")
            )
            self.assertEqual(persisted["records"], {})

    def test_review_requires_a_separate_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(tmp)
            candidate = self._propose(store)
            with self.assertRaisesRegex(PermissionError, "review scope"):
                store.review(
                    candidate.candidate_id,
                    reviewer=self._principal(),
                    approve=True,
                    reason="self approval must fail",
                    now=12,
                )

    def test_review_requires_boolean_decision_and_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(tmp)
            candidate = self._propose(store)
            reviewer = self._principal(reviewer=True)

            with self.assertRaisesRegex(
                ValueError,
                "approve must be boolean",
            ):
                store.review(
                    candidate.candidate_id,
                    reviewer=reviewer,
                    approve="yes",  # type: ignore[arg-type]
                    reason="verified",
                    now=12,
                )
            with self.assertRaisesRegex(
                ValueError,
                "review_reason must not be empty",
            ):
                store.review(
                    candidate.candidate_id,
                    reviewer=reviewer,
                    approve=True,
                    reason="",
                    now=12,
                )
            self.assertEqual(
                store.get_candidate(
                    candidate.candidate_id,
                    principal=self._principal(),
                ).review_status,
                MemoryReviewStatus.PENDING,
            )

    def test_proposal_does_not_coerce_arbitrary_content_or_acl(self) -> None:
        class Hostile:
            def __str__(self) -> str:
                raise AssertionError("__str__ must not run")

        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(tmp)
            with self.assertRaisesRegex(
                ValueError,
                "memory content must be text",
            ):
                store.propose(
                    principal=self._principal(),
                    content=Hostile(),  # type: ignore[arg-type]
                    namespace=("tenant-1",),
                    kind=MemoryKind.SEMANTIC,
                    source_refs=("event-1",),
                    trust=TrustLevel.USER,
                    confidence=1,
                    sensitivity=DataSensitivity.INTERNAL,
                    acl=("tenant:tenant-1",),
                )
            with self.assertRaisesRegex(ValueError, "acl must be an array"):
                store.propose(
                    principal=self._principal(),
                    content="bounded",
                    namespace=("tenant-1",),
                    kind=MemoryKind.SEMANTIC,
                    source_refs=("event-1",),
                    trust=TrustLevel.USER,
                    confidence=1,
                    sensitivity=DataSensitivity.INTERNAL,
                    acl="tenant:tenant-1",  # type: ignore[arg-type]
                )
            self.assertFalse(store.path.exists())

    def test_proposal_requires_explicit_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            principal = Principal(
                subject="user-1",
                tenant_id="tenant-1",
                session_id="session-1",
                run_id="run-1",
                scopes=("workspace.read",),
            )
            with self.assertRaisesRegex(PermissionError, "propose scope"):
                MemoryStore(tmp).propose(
                    principal=principal,
                    content="must not persist",
                    namespace=("tenant-1",),
                    kind=MemoryKind.SEMANTIC,
                    source_refs=("event-1",),
                    trust=TrustLevel.USER,
                    confidence=1,
                    sensitivity=DataSensitivity.INTERNAL,
                    acl=("tenant:tenant-1",),
                )

    def test_candidate_acl_cannot_escape_proposer_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(PermissionError, "ACL exceeds"):
                MemoryStore(tmp).propose(
                    principal=self._principal(),
                    content="cross tenant poison",
                    namespace=("tenant-1",),
                    kind=MemoryKind.SEMANTIC,
                    source_refs=("event-1",),
                    trust=TrustLevel.USER,
                    confidence=1,
                    sensitivity=DataSensitivity.INTERNAL,
                    acl=("tenant:tenant-2",),
                )

    def test_reviewer_from_another_tenant_cannot_activate_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(tmp)
            candidate = self._propose(store)
            with self.assertRaisesRegex(PermissionError, "authorize this reviewer"):
                store.review(
                    candidate.candidate_id,
                    reviewer=self._principal(
                        subject="reviewer-2",
                        tenant="tenant-2",
                        reviewer=True,
                    ),
                    approve=True,
                    reason="must not cross tenant",
                    now=12,
                )
            self.assertEqual(
                store.get_candidate(
                    candidate.candidate_id,
                    principal=self._principal(),
                ).review_status,
                MemoryReviewStatus.PENDING,
            )
            with self.assertRaisesRegex(PermissionError, "tenant denies"):
                store.get_candidate(
                    candidate.candidate_id,
                    principal=self._principal(
                        subject="reader-2",
                        tenant="tenant-2",
                    ),
                )

    def test_same_subject_cannot_review_or_read_across_tenants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(tmp)
            candidate = store.propose(
                principal=self._principal(subject="same-user", tenant="tenant-1"),
                content="tenant-1 secret",
                namespace=("tenant", "tenant-1", "agent", "main"),
                kind=MemoryKind.SEMANTIC,
                source_refs=("event-1",),
                trust=TrustLevel.USER,
                confidence=1,
                sensitivity=DataSensitivity.SECRET,
                acl=("subject:same-user",),
                now=10,
            )
            other_tenant = self._principal(
                subject="same-user",
                tenant="tenant-2",
                reviewer=True,
            )

            with self.assertRaisesRegex(PermissionError, "tenant"):
                store.review(
                    candidate.candidate_id,
                    reviewer=other_tenant,
                    approve=True,
                    reason="must not cross tenant",
                    now=11,
                )
            with self.assertRaisesRegex(PermissionError, "tenant"):
                store.get_candidate(
                    candidate.candidate_id,
                    principal=other_tenant,
                )

            store.review(
                candidate.candidate_id,
                reviewer=self._principal(
                    subject="same-user",
                    tenant="tenant-1",
                    reviewer=True,
                ),
                approve=True,
                reason="same tenant",
                now=12,
            )
            self.assertEqual(
                store.active_records(principal=other_tenant, now=13),
                (),
            )

    def test_active_memory_read_requires_explicit_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            principal = Principal(
                subject="user-1",
                tenant_id="tenant-1",
                session_id="session-1",
                run_id="run-1",
                scopes=(MEMORY_PROPOSE_SCOPE,),
            )
            with self.assertRaisesRegex(PermissionError, "read scope"):
                MemoryStore(tmp).active_records(principal=principal)

    def test_acl_and_expiry_are_enforced_on_read(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(tmp)
            candidate = store.propose(
                principal=self._principal(),
                content="Temporary tenant fact",
                namespace=("tenant-1",),
                kind=MemoryKind.SEMANTIC,
                source_refs=("event-1",),
                trust=TrustLevel.USER,
                confidence=1,
                sensitivity=DataSensitivity.INTERNAL,
                acl=("tenant:tenant-1",),
                ttl_seconds=5,
                now=10,
            )
            store.review(
                candidate.candidate_id,
                reviewer=self._principal(reviewer=True),
                approve=True,
                reason="verified",
                now=11,
            )
            self.assertEqual(
                len(store.active_records(principal=self._principal(), now=14)),
                1,
            )
            self.assertEqual(
                store.active_records(
                    principal=self._principal("user-2", "tenant-2"),
                    now=14,
                ),
                (),
            )
            self.assertEqual(
                store.active_records(principal=self._principal(), now=15),
                (),
            )

    def test_expired_candidate_cannot_be_activated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(tmp)
            candidate = store.propose(
                principal=self._principal(),
                content="Short-lived fact",
                namespace=("tenant-1",),
                kind=MemoryKind.EPISODIC,
                source_refs=("event-1",),
                trust=TrustLevel.USER,
                confidence=0.7,
                sensitivity=DataSensitivity.INTERNAL,
                acl=("tenant:tenant-1",),
                ttl_seconds=1,
                now=10,
            )
            resolved = store.review(
                candidate.candidate_id,
                reviewer=self._principal(reviewer=True),
                approve=True,
                reason="too late",
                now=11,
            )
            self.assertEqual(resolved.review_status, MemoryReviewStatus.EXPIRED)
            self.assertEqual(
                store.active_records(principal=self._principal(), now=11),
                (),
            )

    def test_concurrent_proposals_do_not_lose_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(tmp)

            def propose(index: int) -> str:
                return store.propose(
                    principal=self._principal(),
                    content=f"fact-{index}",
                    namespace=("tenant-1",),
                    kind=MemoryKind.EPISODIC,
                    source_refs=(f"event-{index}",),
                    trust=TrustLevel.USER,
                    confidence=0.8,
                    sensitivity=DataSensitivity.INTERNAL,
                    acl=("tenant:tenant-1",),
                    now=10 + index,
                ).candidate_id

            with ThreadPoolExecutor(max_workers=12) as pool:
                candidate_ids = set(pool.map(propose, range(20)))
            payload = json.loads(
                (Path(tmp) / MEMORY_STORE_PATH).read_text(encoding="utf-8")
            )
            self.assertEqual(set(payload["candidates"]), candidate_ids)
            self.assertEqual(len(candidate_ids), 20)

    def test_malformed_store_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / MEMORY_STORE_PATH
            path.parent.mkdir(parents=True)
            path.write_text('{"schema_version":1,"candidates":[],"records":{}}', encoding="utf-8")
            with self.assertRaisesRegex(MemoryStoreError, "invalid"):
                MemoryStore(tmp).active_records(principal=self._principal())

    def test_active_record_must_match_its_reviewed_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(tmp)
            candidate = self._propose(store)
            store.review(
                candidate.candidate_id,
                reviewer=self._principal(reviewer=True),
                approve=True,
                reason="verified",
                now=12,
            )
            payload = json.loads(store.path.read_text(encoding="utf-8"))
            record = next(iter(payload["records"].values()))
            record["content"] = "injected approved memory"
            store.path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(MemoryStoreError, "does not match"):
                store.active_records(principal=self._principal(), now=13)

    def test_orphan_approved_record_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(tmp)
            candidate = self._propose(store)
            store.review(
                candidate.candidate_id,
                reviewer=self._principal(reviewer=True),
                approve=True,
                reason="verified",
                now=12,
            )
            payload = json.loads(store.path.read_text(encoding="utf-8"))
            payload["candidates"] = {}
            store.path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(
                MemoryStoreError,
                "lacks an approved candidate",
            ):
                store.active_records(principal=self._principal(), now=13)

    def test_store_read_does_not_follow_replaced_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = MemoryStore(root)
            candidate = self._propose(store)
            store.review(
                candidate.candidate_id,
                reviewer=self._principal(reviewer=True),
                approve=True,
                reason="verified",
                now=12,
            )
            outside = root / "outside-memory-store.json"
            store.path.rename(outside)
            store.path.symlink_to(outside)

            with self.assertRaisesRegex(MemoryStoreError, "unreadable"):
                store.active_records(principal=self._principal(), now=13)

    def test_store_write_does_not_follow_runtime_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            outside = root / "outside"
            outside.mkdir()
            (root / "runtime").symlink_to(
                outside,
                target_is_directory=True,
            )

            with self.assertRaises(OSError):
                self._propose(MemoryStore(root))

            self.assertFalse((outside / ".workspace-write.lock").exists())
            self.assertFalse(
                (outside / "agent_kernel" / "memory-store.json").exists()
            )

    def test_store_is_private_and_rejects_oversized_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = MemoryStore(root)
            self._propose(store)
            self.assertEqual(store.path.stat().st_mode & 0o077, 0)

            with store.path.open("wb") as file:
                file.truncate(MAX_MEMORY_STORE_BYTES + 1)
            with self.assertRaisesRegex(MemoryStoreError, "unreadable"):
                store.active_records(principal=self._principal())

    def test_candidate_identity_binds_review_relevant_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(tmp)
            first = self._propose(store)
            second = store.propose(
                principal=self._principal(),
                content=first.content,
                namespace=first.namespace,
                kind=first.kind,
                source_refs=first.source_refs,
                trust=first.trust,
                confidence=0.1,
                sensitivity=DataSensitivity.SECRET,
                acl=("subject:user-1",),
                ttl_seconds=60,
                now=10,
            )

            self.assertNotEqual(first.candidate_id, second.candidate_id)


if __name__ == "__main__":
    unittest.main()
