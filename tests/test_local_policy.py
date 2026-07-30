from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.core.agent_kernel import Principal
from src.core.local_policy import (
    LOCAL_PRINCIPAL_SCOPES,
    LOCAL_TOOL_CONTRACT_MAP,
    DurableLocalApprovalLedger,
    LocalPolicyGate,
)
from src.handler import XAgentHandler
from src.orchestration.policy import (
    ApprovalConsumeResult,
    ApprovalGrant,
    PolicyOutcome,
)


class LocalPolicyGateTests(unittest.TestCase):
    def _principal(self, scopes: tuple[str, ...] = LOCAL_PRINCIPAL_SCOPES) -> Principal:
        return Principal(
            subject="user-1",
            tenant_id="tenant-1",
            session_id="session-1",
            run_id="run-1",
            scopes=scopes,
        )

    def test_known_reads_allow_and_unknown_tools_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gate = LocalPolicyGate(tmp, actor="user-1")
            allowed = gate.evaluate(
                principal=self._principal(),
                tool_name="file_read",
                args={"path": "README.md"},
                turn=1,
            )
            denied = gate.evaluate(
                principal=self._principal(),
                tool_name="undeclared",
                args={},
                turn=1,
            )
        self.assertEqual(allowed.decision.outcome, PolicyOutcome.ALLOW)
        self.assertEqual(denied.decision.outcome, PolicyOutcome.DENY)
        self.assertEqual(denied.decision.reason_code, "unknown_tool")

    def test_principal_scope_cannot_be_inferred_from_tool_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gate = LocalPolicyGate(tmp, actor="user-1")
            denied = gate.evaluate(
                principal=self._principal(("workspace.read",)),
                tool_name="file_write",
                args={"path": "notes.txt", "content": "safe"},
                turn=1,
            )
        self.assertEqual(denied.decision.outcome, PolicyOutcome.DENY)
        self.assertEqual(denied.decision.reason_code, "principal_scope_missing")

    def test_outside_read_requires_local_operator_capability(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp) / "workspace"
            workspace.mkdir()
            outside = Path(tmp) / "host.txt"
            outside.write_text("host-owned", encoding="utf-8")
            gate = LocalPolicyGate(workspace, actor="user-1")
            tenant_decision = gate.evaluate(
                principal=self._principal(("workspace.read",)),
                tool_name="file_read",
                args={"path": str(outside)},
                turn=1,
            ).decision
            local_decision = gate.evaluate(
                principal=self._principal(),
                tool_name="file_read",
                args={"path": str(outside)},
                turn=1,
            ).decision

        self.assertEqual(tenant_decision.outcome, PolicyOutcome.DENY)
        self.assertEqual(
            tenant_decision.reason_code,
            "host_read_scope_required",
        )
        self.assertEqual(local_decision.outcome, PolicyOutcome.ALLOW)

    def test_gate_cannot_issue_approval_for_a_different_principal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gate = LocalPolicyGate(tmp, actor="user-1")
            other = Principal(
                subject="user-2",
                tenant_id="tenant-1",
                session_id="session-2",
                run_id="run-2",
                scopes=LOCAL_PRINCIPAL_SCOPES,
            )
            denied = gate.evaluate(
                principal=other,
                tool_name="code_run",
                args={"script": "echo safe"},
                turn=1,
            )
        self.assertEqual(denied.decision.outcome, PolicyOutcome.DENY)
        self.assertEqual(denied.decision.reason_code, "principal_actor_mismatch")

    def test_high_risk_approval_is_bound_to_exact_arguments_and_single_use(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gate = LocalPolicyGate(tmp, actor="user-1")
            principal = self._principal()
            pending = gate.evaluate(
                principal=principal,
                tool_name="code_run",
                args={"language": "shell", "script": "echo safe"},
                turn=2,
            )
            changed = gate.evaluate(
                principal=principal,
                tool_name="code_run",
                args={"language": "shell", "script": "echo changed"},
                turn=2,
            )
            self.assertEqual(pending.decision.outcome, PolicyOutcome.REQUIRE_APPROVAL)
            self.assertNotEqual(
                pending.action.action_digest,
                changed.action.action_digest,
            )
            approved = gate.approve(
                pending,
                principal=principal,
                ttl_seconds=60,
                now=100,
            )
            self.assertEqual(approved.outcome, PolicyOutcome.ALLOW)
            self.assertEqual(approved.action_digest, pending.action.action_digest)

            # Reconstructing the durable ledger proves the grant cannot replay
            # across a process boundary.
            ledger = DurableLocalApprovalLedger(tmp)
            replay = ApprovalGrant(
                approval_id=approved.approval_id,
                action_digest=pending.action.action_digest,
                run_id=pending.action.run_id,
                node_id=pending.action.node_id,
                policy_version=approved.policy_version,
                actor="principal:user-1",
                expires_at=160,
            )
            self.assertEqual(
                ledger.consume(replay),
                ApprovalConsumeResult.REPLAYED,
            )

    def test_workspace_writes_need_an_explicit_policy_rule(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gate = LocalPolicyGate(tmp, actor="user-1")
            decision = gate.evaluate(
                principal=self._principal(),
                tool_name="file_write",
                args={"path": "notes.txt", "content": "safe"},
                turn=1,
            ).decision
        self.assertEqual(decision.outcome, PolicyOutcome.ALLOW)
        self.assertEqual(decision.reason_code, "local_explicit_allow")

    def test_every_default_handler_tool_has_a_policy_contract(self) -> None:
        handler_tools = {
            name.removeprefix("exec_")
            for name in XAgentHandler.__dict__
            if name.startswith("exec_")
        }
        self.assertEqual(handler_tools, set(LOCAL_TOOL_CONTRACT_MAP))

    def test_managed_memory_cannot_be_modified_with_file_tools(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gate = LocalPolicyGate(tmp, actor="user-1")
            principal = self._principal()
            for tool_name, path in (
                ("file_write", "memory/global_mem.txt"),
                ("file_patch", "system/agents/main/MEMORY.md"),
            ):
                with self.subTest(tool_name=tool_name, path=path):
                    decision = gate.evaluate(
                        principal=principal,
                        tool_name=tool_name,
                        args={"path": path, "content": "poison"},
                        turn=1,
                    ).decision
                    self.assertEqual(decision.outcome, PolicyOutcome.DENY)
                    self.assertEqual(
                        decision.reason_code,
                        "managed_memory_requires_candidate",
                    )

    def test_managed_memory_read_requires_memory_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gate = LocalPolicyGate(tmp, actor="user-1")
            without_memory = Principal(
                subject="user-1",
                tenant_id="tenant-1",
                session_id="session-1",
                run_id="run-1",
                scopes=("workspace.read",),
            )
            denied = gate.evaluate(
                principal=without_memory,
                tool_name="file_read",
                args={"path": "system/agents/main/MEMORY.md"},
                turn=1,
            ).decision
            allowed = gate.evaluate(
                principal=self._principal(),
                tool_name="file_read",
                args={"path": "system/agents/main/MEMORY.md"},
                turn=1,
            ).decision

        self.assertEqual(denied.outcome, PolicyOutcome.DENY)
        self.assertEqual(
            denied.reason_code,
            "memory_read_scope_required",
        )
        self.assertEqual(allowed.outcome, PolicyOutcome.ALLOW)

    def test_file_reference_cannot_bypass_memory_read_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gate = LocalPolicyGate(tmp, actor="user-1")
            principal = Principal(
                subject="user-1",
                tenant_id="tenant-1",
                session_id="session-1",
                run_id="run-1",
                scopes=("workspace.read", "workspace.write"),
            )
            for tool_name, args in (
                (
                    "file_write",
                    {
                        "path": "business/leak.txt",
                        "content": "{{file:system/memory/secret.md:1:1}}",
                    },
                ),
                (
                    "file_patch",
                    {
                        "path": "business/target.txt",
                        "old_content": "before",
                        "new_content": "{{file:memory/secret.md:1:1}}",
                    },
                ),
            ):
                with self.subTest(tool_name=tool_name):
                    decision = gate.evaluate(
                        principal=principal,
                        tool_name=tool_name,
                        args=args,
                        turn=1,
                    ).decision
                    self.assertEqual(decision.outcome, PolicyOutcome.DENY)
                    self.assertEqual(
                        decision.reason_code,
                        "protected_file_reference",
                    )

    def test_mixed_case_protected_paths_are_classified(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gate = LocalPolicyGate(tmp, actor="user-1")
            principal = Principal(
                subject="user-1",
                tenant_id="tenant-1",
                session_id="session-1",
                run_id="run-1",
                scopes=("workspace.read", "workspace.write"),
            )
            memory_read = gate.evaluate(
                principal=principal,
                tool_name="file_read",
                args={"path": "SYSTEM/MEMORY/secret.md"},
                turn=1,
            ).decision
            checkpoint_read = gate.evaluate(
                principal=principal,
                tool_name="file_read",
                args={"path": "RUNTIME/CHECKPOINTS/latest.json"},
                turn=1,
            ).decision
            memory_reference = gate.evaluate(
                principal=principal,
                tool_name="file_write",
                args={
                    "path": "business/leak.txt",
                    "content": "{{file:SYSTEM/MEMORY/secret.md:1:1}}",
                },
                turn=1,
            ).decision

            self.assertEqual(
                memory_read.reason_code,
                "memory_read_scope_required",
            )
            self.assertEqual(
                checkpoint_read.reason_code,
                "protected_control_plane",
            )
            self.assertEqual(
                memory_reference.reason_code,
                "protected_file_reference",
            )

    def test_browser_result_save_requires_write_scope_and_business_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gate = LocalPolicyGate(tmp, actor="user-1")
            browser_only = Principal(
                subject="user-1",
                tenant_id="tenant-1",
                session_id="session-1",
                run_id="run-1",
                scopes=("browser.execute",),
            )
            no_write = gate.evaluate(
                principal=browser_only,
                tool_name="web_execute_js",
                args={
                    "script": "return document.title",
                    "save_to_file": "business/title.txt",
                },
                turn=1,
            ).decision
            runtime_target = gate.evaluate(
                principal=self._principal(),
                tool_name="web_execute_js",
                args={
                    "script": "return document.title",
                    "save_to_file": "runtime/chats/state.json",
                },
                turn=1,
            ).decision

        self.assertEqual(no_write.outcome, PolicyOutcome.DENY)
        self.assertEqual(no_write.reason_code, "principal_scope_missing")
        self.assertEqual(runtime_target.outcome, PolicyOutcome.DENY)
        self.assertEqual(
            runtime_target.reason_code,
            "workspace_runtime_read_only",
        )

    def test_control_plane_and_system_paths_are_denied_by_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gate = LocalPolicyGate(tmp, actor="user-1")
            principal = self._principal()
            for path, reason in (
                ("runtime/agent_kernel/memory-store.json", "protected_control_plane"),
                ("runtime/checkpoints/latest.json", "protected_control_plane"),
                ("runtime/file_index.sqlite3", "protected_control_plane"),
                ("_intervene", "protected_control_plane"),
                ("_keyinfo", "protected_control_plane"),
                ("plan.md", "protected_control_plane"),
                ("system/agents/main/AGENT.md", "workspace_system_read_only"),
            ):
                with self.subTest(path=path):
                    decision = gate.evaluate(
                        principal=principal,
                        tool_name="file_write",
                        args={"path": path, "content": "poison"},
                        turn=1,
                    ).decision
                    self.assertEqual(decision.outcome, PolicyOutcome.DENY)
                    self.assertEqual(decision.reason_code, reason)
            read_decision = gate.evaluate(
                principal=principal,
                tool_name="file_read",
                args={"path": "runtime/agent_kernel/approval-ledger.json"},
                turn=1,
            ).decision
            self.assertEqual(read_decision.outcome, PolicyOutcome.DENY)
            self.assertEqual(
                read_decision.reason_code,
                "protected_control_plane",
            )

    def test_noncanonical_or_oversized_arguments_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gate = LocalPolicyGate(tmp, actor="user-1")
            for args in (
                {"path": "notes.txt", "score": float("nan")},
                {
                    "path": "notes.txt",
                    "metadata": {
                        f"key-{index}": "value" * 100
                        for index in range(1_000)
                    },
                },
            ):
                with self.subTest(size=len(str(args))):
                    authorization = gate.evaluate(
                        principal=self._principal(),
                        tool_name="file_write",
                        args=args,
                        turn=1,
                    )
                    self.assertIsNone(authorization.action)
                    self.assertEqual(
                        authorization.decision.outcome,
                        PolicyOutcome.DENY,
                    )
                    self.assertEqual(
                        authorization.decision.reason_code,
                        "invalid_action_args",
                    )


if __name__ == "__main__":
    unittest.main()
