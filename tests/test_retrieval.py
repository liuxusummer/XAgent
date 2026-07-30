from __future__ import annotations

import unittest

from src.core.agent_kernel import (
    DataSensitivity,
    KernelValidationError,
    KnowledgeItem,
    KnowledgeKind,
    Principal,
    TrustLevel,
)
from src.core.retrieval import (
    EvidenceBundle,
    EvidenceItem,
    QueryPlan,
    build_query_plan,
    deterministic_rerank,
)


class RetrievalEvidenceContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.principal = Principal(
            subject="retrieval-user",
            tenant_id="tenant-a",
            session_id="session-a",
            run_id="run-a",
            scopes=("workspace.read",),
        )

    def _knowledge(
        self,
        *,
        kind: KnowledgeKind = KnowledgeKind.RETRIEVAL,
        tenant_id: str = "tenant-a",
    ) -> KnowledgeItem:
        return KnowledgeItem(
            item_id=f"item-{kind.value}-{tenant_id}",
            kind=kind,
            namespace=("tenant", tenant_id, "workspace"),
            content_sha256="a" * 64,
            source_refs=("workspace-path-sha256:" + "b" * 64,),
            trust=TrustLevel.RETRIEVED,
            sensitivity=DataSensitivity.INTERNAL,
            acl=(f"tenant:{tenant_id}",),
            created_at=1,
        )

    def _item(
        self,
        evidence_id: str,
        *,
        index_version: str = "4:1",
        knowledge: KnowledgeItem | None = None,
    ) -> EvidenceItem:
        return EvidenceItem(
            evidence_id=evidence_id,
            knowledge=knowledge or self._knowledge(),
            path="business/reference.txt",
            start_line=1,
            end_line=1,
            snippet_sha256="c" * 64,
            index_version=index_version,
        )

    def test_bundle_rejects_duplicate_ids_and_mixed_index_versions(self) -> None:
        first = self._item("ev-first")
        duplicate = self._item("ev-first")
        mixed = self._item("ev-second", index_version="4:2")

        with self.assertRaisesRegex(
            KernelValidationError,
            "duplicate evidence ids",
        ):
            EvidenceBundle.build(
                query="reference",
                principal=self.principal,
                index_version="4:1",
                items=(first, duplicate),
                now=2,
            )
        with self.assertRaisesRegex(
            KernelValidationError,
            "mixes index versions",
        ):
            EvidenceBundle.build(
                query="reference",
                principal=self.principal,
                index_version="4:1",
                items=(first, mixed),
                now=2,
            )

    def test_bundle_stops_consuming_items_at_the_declared_bound(self) -> None:
        consumed = 0
        item = self._item("ev-bounded")

        def unbounded_items():
            nonlocal consumed
            while True:
                consumed += 1
                yield item

        with self.assertRaisesRegex(
            KernelValidationError,
            "exceeds its item bound",
        ):
            EvidenceBundle.build(
                query="reference",
                principal=self.principal,
                index_version="4:1",
                items=unbounded_items(),
                now=2,
            )
        self.assertEqual(consumed, 101)

    def test_evidence_requires_retrieval_knowledge(self) -> None:
        with self.assertRaisesRegex(
            KernelValidationError,
            "retrieval knowledge",
        ):
            self._item(
                "ev-memory",
                knowledge=self._knowledge(kind=KnowledgeKind.MEMORY),
            )

    def test_bundle_query_is_bounded_and_authorization_is_rechecked(self) -> None:
        with self.assertRaisesRegex(
            KernelValidationError,
            "query",
        ):
            EvidenceBundle.build(
                query="q" * 4_097,
                principal=self.principal,
                index_version="4:1",
                items=(),
                now=2,
            )
        foreign = self._item(
            "ev-foreign",
            knowledge=self._knowledge(tenant_id="tenant-b"),
        )
        with self.assertRaises(PermissionError):
            EvidenceBundle.build(
                query="reference",
                principal=self.principal,
                index_version="4:1",
                items=(foreign,),
                now=2,
            )

    def test_query_plan_is_deduplicated_bounded_and_content_digested(self) -> None:
        plan = build_query_plan(
            "  Authentication   flow authentication token policy archive current extra  "
        )
        same = build_query_plan(
            "Authentication flow authentication token policy archive current extra"
        )

        self.assertEqual(plan.normalized_query, same.normalized_query)
        self.assertEqual(plan.digest, same.digest)
        self.assertEqual(plan.terms[0:2], ("Authentication", "flow"))
        self.assertEqual(plan.terms.count("Authentication"), 1)
        self.assertLessEqual(len(plan.components), 8)
        with self.assertRaises(KernelValidationError):
            QueryPlan(
                normalized_query="query",
                terms=("query", "QUERY"),
                components=("query",),
                truncated=False,
            )

    def test_rerank_prefers_visible_term_coverage_and_normalizes_bad_scores(self) -> None:
        plan = build_query_plan("authentication token refresh")
        matches = [
            {
                "path": "z.txt",
                "line": 1,
                "snippet": "authentication",
                "score": float("nan"),
                "signals": {"keyword": "invalid", "semantic": float("inf")},
            },
            {
                "path": "a.txt",
                "line": 1,
                "snippet": "authentication token refresh",
                "score": 0.5,
                "signals": {"keyword": 0.5, "semantic": 0.0},
            },
        ]

        ranked = deterministic_rerank(
            matches,
            plan,
            mode="keyword",
            limit=2,
        )

        self.assertEqual(ranked[0]["path"], "a.txt")
        self.assertEqual(
            ranked[0]["ranking_signals"]["term_coverage"],
            1.0,
        )
        self.assertEqual(
            ranked[1]["ranking_signals"]["source_score"],
            0.0,
        )
        with self.assertRaisesRegex(
            KernelValidationError,
            "bounded list",
        ):
            deterministic_rerank(
                [{}] * 501,
                plan,
                mode="keyword",
                limit=1,
            )


if __name__ == "__main__":
    unittest.main()
