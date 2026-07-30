from __future__ import annotations

import unittest

from src.core.agent_kernel import (
    ContextItem,
    ContextKind,
    ContextManifest,
    DataSensitivity,
    KernelValidationError,
    KnowledgeItem,
    KnowledgeKind,
    Principal,
    TrustLevel,
)


class AgentKernelContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.principal = Principal(
            subject="user-1",
            tenant_id="tenant-1",
            session_id="session-1",
            run_id="run-1",
            agent_id="main",
            scopes=("workspace.read",),
        )

    def test_principal_digest_is_stable_and_does_not_depend_on_scope_order(self) -> None:
        reordered = Principal(
            subject="user-1",
            tenant_id="tenant-1",
            session_id="session-1",
            run_id="run-1",
            agent_id="main",
            scopes=("workspace.read", "workspace.read"),
        )
        self.assertEqual(self.principal.principal_digest, reordered.principal_digest)
        rebound = self.principal.bind_run(session_id="session-2", run_id="run-2")
        self.assertEqual(rebound.subject, self.principal.subject)
        self.assertEqual(rebound.tenant_id, self.principal.tenant_id)
        self.assertEqual(rebound.session_id, "session-2")
        self.assertNotEqual(rebound.principal_digest, self.principal.principal_digest)

    def test_knowledge_acl_is_explicit_and_tenant_bound(self) -> None:
        item = KnowledgeItem(
            item_id="item-1",
            kind=KnowledgeKind.RETRIEVAL,
            namespace=("tenant-1", "workspace-1"),
            content_sha256="a" * 64,
            source_refs=("src/file.py:1",),
            trust=TrustLevel.WORKSPACE,
            sensitivity=DataSensitivity.INTERNAL,
            acl=("tenant:tenant-1",),
            created_at=10,
            valid_until=20,
        )
        self.assertTrue(item.is_authorized(self.principal, now=19))
        self.assertEqual(item.namespace, ("tenant-1", "workspace-1"))
        self.assertFalse(item.is_authorized(self.principal, now=20))
        self.assertFalse(
            item.is_authorized(
                Principal(
                    subject="user-2",
                    tenant_id="tenant-2",
                    session_id="session-2",
                    run_id="run-2",
                ),
                now=19,
            )
        )
        self.assertFalse(
            KnowledgeItem(
                item_id="item-same-subject",
                kind=KnowledgeKind.MEMORY,
                namespace=("tenant-1",),
                content_sha256="c" * 64,
                source_refs=("event-1",),
                trust=TrustLevel.USER,
                sensitivity=DataSensitivity.SECRET,
                acl=("subject:user-1",),
                created_at=10,
            ).is_authorized(
                Principal(
                    subject="user-1",
                    tenant_id="tenant-2",
                    session_id="session-2",
                    run_id="run-2",
                ),
                now=19,
            )
        )

        with self.assertRaisesRegex(KernelValidationError, "acl must be explicit"):
            KnowledgeItem(
                item_id="item-2",
                kind=KnowledgeKind.MEMORY,
                namespace=("tenant-1",),
                content_sha256="b" * 64,
                source_refs=("event-1",),
                trust=TrustLevel.AGENT_DERIVED,
                sensitivity=DataSensitivity.INTERNAL,
                acl=(),
                created_at=10,
            )

    def test_context_manifest_counts_only_llm_visible_tokens(self) -> None:
        visible = ContextItem(
            ref_id="evidence-1",
            kind=ContextKind.RETRIEVAL_EVIDENCE,
            token_count=60,
            priority=80,
            trust=TrustLevel.RETRIEVED,
            source_sha256="c" * 64,
        )
        local = ContextItem(
            ref_id="principal-local",
            kind=ContextKind.TASK_STATE,
            token_count=1000,
            priority=100,
            trust=TrustLevel.SYSTEM,
            source_sha256="d" * 64,
            llm_visible=False,
        )
        manifest = ContextManifest(
            manifest_id="manifest-1",
            principal_digest=self.principal.principal_digest,
            max_input_tokens=100,
            reserved_output_tokens=30,
            items=(visible, local),
        )
        self.assertEqual(manifest.available_input_tokens, 70)
        self.assertEqual(manifest.visible_token_count, 60)

    def test_principal_boundary_is_stable_across_runs_but_not_tenants(self) -> None:
        rebound = self.principal.bind_run(
            session_id="session-2",
            run_id="run-2",
        )
        other_tenant = Principal(
            subject=self.principal.subject,
            tenant_id="tenant-2",
            session_id="session-2",
            run_id="run-2",
            scopes=self.principal.scopes,
        )

        self.assertNotEqual(
            self.principal.principal_digest,
            rebound.principal_digest,
        )
        self.assertEqual(
            self.principal.boundary_digest,
            rebound.boundary_digest,
        )
        self.assertNotEqual(
            self.principal.boundary_digest,
            other_tenant.boundary_digest,
        )

    def test_context_manifest_fails_when_visible_budget_is_exceeded(self) -> None:
        item = ContextItem(
            ref_id="history-1",
            kind=ContextKind.RECENT_HISTORY,
            token_count=71,
            priority=50,
            trust=TrustLevel.USER,
            source_sha256="e" * 64,
        )
        with self.assertRaisesRegex(KernelValidationError, "exceeds its token budget"):
            ContextManifest(
                manifest_id="manifest-2",
                principal_digest=self.principal.principal_digest,
                max_input_tokens=100,
                reserved_output_tokens=30,
                items=(item,),
            )

    def test_contracts_reject_unbounded_or_control_character_metadata(self) -> None:
        with self.assertRaises(KernelValidationError):
            Principal(
                subject="user\nadmin",
                tenant_id="tenant-1",
                session_id="session-1",
                run_id="run-1",
            )
        with self.assertRaises(KernelValidationError):
            ContextItem(
                ref_id="item",
                kind=ContextKind.SYSTEM,
                token_count=10_000_001,
                priority=100,
                trust=TrustLevel.SYSTEM,
                source_sha256="f" * 64,
            )


if __name__ == "__main__":
    unittest.main()
