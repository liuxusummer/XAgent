from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

from src.orchestration.agent_execution_manifest import (
    agent_tool_operation_key,
)
from src.orchestration.agent_tool_handler import (
    AgentToolInvocationRequest,
)
from src.orchestration.agent_tool_result import (
    AgentToolResult,
    AgentToolResultArtifactStore,
)
from src.orchestration.artifacts import (
    ArtifactSensitivity,
    LocalArtifactStore,
)
from src.orchestration.executor import (
    ToolReceipt,
    ToolReceiptVerification,
)
from src.orchestration.models import AttemptStatus
from src.orchestration.policy import (
    EffectClass,
    canonical_action_args_digest,
)
from src.orchestration.sandbox import (
    SandboxOutcome,
    SandboxReceipt,
    SecurityLevel,
)
from src.orchestration.scheduler import DurableScheduler
from src.orchestration.store import (
    AgentToolInvocationConflict,
    DurableRunStore,
    InvalidStateTransition,
    STORE_SCHEMA_VERSION,
    StoreSchemaError,
)
from src.orchestration.workflow import compile_workflow


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class _Ids:
    def __init__(self) -> None:
        self.value = 0

    def __call__(self, prefix: str) -> str:
        self.value += 1
        return f"{prefix}-agent-tool-authority-{self.value}"


class _Fixture:
    def __init__(self, root: Path) -> None:
        self.clock = _Clock()
        self.store = DurableRunStore(root / "domain.sqlite3")
        self.artifacts = LocalArtifactStore(root / "artifacts")
        self.workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": "agent-tool-authority",
                "version": 1,
                "nodes": [
                    {
                        "id": "agent",
                        "kind": "agent",
                        "config": {
                            "agent": "main",
                            "task": "perform the task",
                        },
                        "effect_class": "read_only",
                    }
                ],
            }
        )
        self.scheduler = DurableScheduler(
            self.store,
            self.workflow,
            clock=self.clock,
            id_factory=_Ids(),
        )
        self.scheduler.create_run("run-1")
        self.scheduler.reconcile("run-1")
        parent = self.scheduler.claim_next("run-1", "agent-worker")
        assert parent is not None
        self.scheduler.start_claim(parent)
        self.parent = parent

    def request(
        self,
        *,
        sequence: int = 1,
        turn: int = 1,
        tool_name: str = "echo",
        args: dict | None = None,
    ) -> AgentToolInvocationRequest:
        arguments = {"value": "safe"} if args is None else args
        operation_key = agent_tool_operation_key(
            run_id=self.parent.run_id,
            node_id=self.parent.node_id,
            attempt_id=self.parent.attempt_id,
            request_digest=self.parent.request_hash,
            sequence=sequence,
        )
        return AgentToolInvocationRequest(
            run_id=self.parent.run_id,
            node_id=self.parent.node_id,
            attempt_id=self.parent.attempt_id,
            request_digest=self.parent.request_hash,
            request_artifact_digest=_digest("agent-request-artifact"),
            sequence=sequence,
            turn=turn,
            tool_name=tool_name,
            tool_call_id_digest=_digest(f"call-{sequence}"),
            args_digest=canonical_action_args_digest(arguments),
            sensitivity=ArtifactSensitivity.SENSITIVE,
            sensitive_keys=(),
            operation_key=operation_key,
            args=arguments,
        )

    def start(
        self,
        request: AgentToolInvocationRequest,
        *,
        lease_seconds: float = 300.0,
    ):
        invocation = self.store.reserve_agent_tool_invocation(
            request,
            now=self.clock.now,
        )
        policy_digest = _digest("policy")
        claim = self.store.start_agent_tool_invocation(
            invocation.invocation_id,
            request.invocation_digest,
            "tool-worker",
            action_digest=_digest("action"),
            execution_binding_digest=_digest("execution"),
            effect_class=EffectClass.READ_ONLY.value,
            policy_version=f"sha256:{policy_digest}",
            policy_digest=policy_digest,
            profile_id="agent-tool-profile",
            profile_digest=_digest("profile"),
            decision_digest=_digest("decision"),
            lease_seconds=lease_seconds,
            now=self.clock.now,
        )
        return invocation, claim

    def terminal_payload(self, claim):
        invocation = claim.invocation
        result = AgentToolResult(
            data={"status": "OK", "value": "safe"},
            next_prompt="continue",
        )
        ref = AgentToolResultArtifactStore.stage(
            self.artifacts,
            result,
            sensitivity=ArtifactSensitivity.SENSITIVE,
            run_id=invocation.run_id,
            node_id=invocation.child_node_id,
            attempt_id=invocation.child_attempt_id,
        )
        sandbox_receipt = SandboxReceipt(
            backend_id="isolated-agent-tool",
            security_level=SecurityLevel.OS_SANDBOX,
            profile_id=invocation.profile_id,
            profile_digest=invocation.profile_digest,
            action_digest=invocation.action_digest,
            policy_version=invocation.policy_version,
            request_digest=_digest("sandbox-request"),
            outcome=SandboxOutcome.SUCCEEDED,
            exit_code=0,
            timed_out=False,
            output_artifact_refs=(ref,),
        )
        receipt = ToolReceipt(
            run_id=invocation.run_id,
            node_id=invocation.child_node_id,
            attempt_id=invocation.child_attempt_id,
            tool_name=invocation.tool_name,
            effect_class=EffectClass.READ_ONLY,
            attempt_status=AttemptStatus.SUCCEEDED,
            args_digest=invocation.args_digest,
            action_digest=invocation.action_digest,
            execution_binding_digest=(
                invocation.execution_binding_digest
            ),
            operation_key_digest=invocation.operation_key_digest,
            idempotency_key_digest=invocation.operation_key_digest,
            policy_version=invocation.policy_version,
            policy_digest=invocation.policy_digest,
            profile_id=invocation.profile_id,
            profile_digest=invocation.profile_digest,
            verification=ToolReceiptVerification.VERIFIED,
            sandbox_receipt=sandbox_receipt.to_dict(),
        )
        return receipt, ref

    def complete(self, request, claim):
        receipt, ref = self.terminal_payload(claim)
        stored = self.store.complete_agent_tool_invocation(
            claim,
            receipt,
            result_artifact_ref=ref,
            now=self.clock.now,
        )
        return stored, receipt, ref


class AgentToolAuthorityStoreTests(unittest.TestCase):
    def test_reservation_is_deterministic_and_concurrent_safe(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            request = fixture.request()

            with ThreadPoolExecutor(max_workers=8) as pool:
                records = list(
                    pool.map(
                        lambda _index: (
                            fixture.store.reserve_agent_tool_invocation(
                                request,
                                now=fixture.clock.now,
                            )
                        ),
                        range(16),
                    )
                )

            self.assertEqual(len(set(records)), 1)
            record = records[0]
            self.assertEqual(record.status, AttemptStatus.SCHEDULED)
            self.assertNotIn(request.operation_key, repr(record))
            events = [
                event
                for event in fixture.store.list_events("run-1")
                if event.event_type == "agent_tool.scheduled"
            ]
            self.assertEqual(len(events), 1)

            changed = fixture.request(tool_name="different")
            with self.assertRaisesRegex(
                AgentToolInvocationConflict,
                "agent_tool_binding_mismatch",
            ):
                fixture.store.reserve_agent_tool_invocation(changed)

    def test_start_and_terminal_receipt_are_durable_and_replayable(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            request = fixture.request()
            _reserved, claim = fixture.start(request)

            self.assertEqual(
                claim.invocation.status,
                AttemptStatus.RUNNING,
            )
            self.assertNotIn(claim.claim_token, repr(claim))
            stored, receipt, ref = fixture.complete(request, claim)

            self.assertEqual(stored.status, AttemptStatus.SUCCEEDED)
            self.assertEqual(stored.result_artifact_ref, ref)
            self.assertEqual(
                fixture.store.get_tool_receipt(
                    stored.run_id,
                    stored.child_attempt_id,
                ),
                receipt,
            )
            self.assertEqual(
                fixture.store.complete_agent_tool_invocation(
                    claim,
                    receipt,
                    result_artifact_ref=ref,
                    now=fixture.clock.now,
                ),
                stored,
            )

    def test_store_rejects_noncanonical_success_artifact_shape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            request = fixture.request()
            _reserved, claim = fixture.start(request)
            receipt, ref = fixture.terminal_payload(claim)

            forged_ref = replace(ref, media_type="application/json")
            with self.assertRaisesRegex(
                AgentToolInvocationConflict,
                "agent_tool_receipt_invalid",
            ):
                fixture.store.complete_agent_tool_invocation(
                    claim,
                    receipt,
                    result_artifact_ref=forged_ref,
                    now=fixture.clock.now,
                )

            current = fixture.store.get_agent_tool_invocation(
                claim.invocation.invocation_id
            )
            self.assertIsNotNone(current)
            self.assertEqual(current.status, AttemptStatus.RUNNING)

    def test_policy_rejection_is_terminal_without_execution_claim(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            request = fixture.request()
            reserved = fixture.store.reserve_agent_tool_invocation(
                request,
                now=fixture.clock.now,
            )
            policy_digest = _digest("denying-policy")
            rejected = fixture.store.reject_agent_tool_invocation(
                reserved.invocation_id,
                request.invocation_digest,
                action_digest=_digest("denied-action"),
                execution_binding_digest=_digest("denied-execution"),
                effect_class=EffectClass.READ_ONLY.value,
                policy_outcome="deny",
                policy_version=f"sha256:{policy_digest}",
                policy_digest=policy_digest,
                profile_id="agent-tool-profile",
                profile_digest=_digest("profile"),
                decision_digest=_digest("denial"),
                now=fixture.clock.now,
            )

            self.assertEqual(rejected.status, AttemptStatus.FAILED)
            self.assertIsNone(rejected.owner_id)
            self.assertIsNone(rejected.claim_token_digest)
            self.assertIsNone(rejected.lease_expires_at)
            receipt = fixture.store.get_tool_receipt(
                rejected.run_id,
                rejected.child_attempt_id,
            )
            self.assertIsNotNone(receipt)
            self.assertEqual(
                receipt.sandbox_receipt_absence_reason,
                "policy_denied_before_backend",
            )

    def test_preflight_failure_can_abandon_unstarted_child(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            request = fixture.request()
            reserved = fixture.store.reserve_agent_tool_invocation(
                request,
                now=fixture.clock.now,
            )

            abandoned = (
                fixture.store.abandon_scheduled_agent_tool_invocation(
                    reserved.invocation_id,
                    request.invocation_digest,
                    now=fixture.clock.now,
                )
            )

            self.assertEqual(abandoned.status, AttemptStatus.ABANDONED)
            self.assertIsNone(
                fixture.store.get_tool_receipt(
                    abandoned.run_id,
                    abandoned.child_attempt_id,
                )
            )
            fixture.scheduler.complete_claim(
                fixture.parent,
                attempt_status=AttemptStatus.FAILED,
                error_class="preflight_failed",
            )

    def test_parent_lease_recovery_abandons_scheduled_child_atomically(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            request = fixture.request()
            reserved = fixture.store.reserve_agent_tool_invocation(
                request,
                now=fixture.clock.now,
            )

            fixture.store.recover_expired_activity(
                fixture.parent.run_id,
                fixture.parent.node_id,
                fixture.parent.attempt_id,
                fixture.parent.request_hash,
                fixture.parent.worker_id,
                claim_token=fixture.parent.claim_token,
                fencing_token=fixture.parent.fencing_token,
                resolution="abandon_failed",
                now=200.0,
            )

            child = fixture.store.get_agent_tool_invocation(
                reserved.invocation_id
            )
            self.assertIsNotNone(child)
            self.assertEqual(child.status, AttemptStatus.ABANDONED)
            self.assertEqual(
                fixture.store.get_attempt(
                    fixture.parent.attempt_id
                ).status,
                AttemptStatus.ABANDONED,
            )
            events = fixture.store.list_events(request.run_id)
            child_index = next(
                index
                for index, event in enumerate(events)
                if event.event_type == "agent_tool.abandoned"
            )
            parent_index = next(
                index
                for index, event in enumerate(events)
                if event.event_type == "attempt.abandoned"
            )
            self.assertLess(child_index, parent_index)

    def test_parent_recovery_resolves_only_expired_running_child(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            request = fixture.request()
            _reserved, claim = fixture.start(
                request,
                lease_seconds=100.0,
            )

            with self.assertRaisesRegex(
                InvalidStateTransition,
                "live Tool invocation",
            ):
                fixture.store.recover_expired_activity(
                    fixture.parent.run_id,
                    fixture.parent.node_id,
                    fixture.parent.attempt_id,
                    fixture.parent.request_hash,
                    fixture.parent.worker_id,
                    claim_token=fixture.parent.claim_token,
                    fencing_token=fixture.parent.fencing_token,
                    resolution="waiting_recovery",
                    now=170.0,
                )
            self.assertEqual(
                fixture.store.get_agent_tool_invocation(
                    claim.invocation.invocation_id
                ).status,
                AttemptStatus.RUNNING,
            )

            fixture.store.recover_expired_activity(
                fixture.parent.run_id,
                fixture.parent.node_id,
                fixture.parent.attempt_id,
                fixture.parent.request_hash,
                fixture.parent.worker_id,
                claim_token=fixture.parent.claim_token,
                fencing_token=fixture.parent.fencing_token,
                resolution="abandon_failed",
                now=200.0,
            )
            self.assertEqual(
                fixture.store.get_agent_tool_invocation(
                    claim.invocation.invocation_id
                ).status,
                AttemptStatus.OUTCOME_UNKNOWN,
            )
            self.assertEqual(
                fixture.store.get_attempt(
                    fixture.parent.attempt_id
                ).status,
                AttemptStatus.OUTCOME_UNKNOWN,
            )

    def test_parent_cannot_finish_with_active_child(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            request = fixture.request()
            fixture.store.reserve_agent_tool_invocation(request)

            with self.assertRaisesRegex(
                InvalidStateTransition,
                "active Tool invocation",
            ):
                fixture.scheduler.complete_claim(
                    fixture.parent,
                    attempt_status=AttemptStatus.FAILED,
                    error_class="test_failure",
                )
            self.assertEqual(
                fixture.store.get_attempt(fixture.parent.attempt_id).status,
                AttemptStatus.RUNNING,
            )

    def test_receipt_substitution_and_store_tampering_fail_closed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            request = fixture.request()
            _reserved, claim = fixture.start(request)
            stored, receipt, ref = fixture.complete(request, claim)

            with sqlite3.connect(fixture.store.path) as conn:
                conn.execute(
                    """
                    UPDATE agent_tool_invocations
                    SET args_digest = ? WHERE invocation_id = ?
                    """,
                    (_digest("tampered"), stored.invocation_id),
                )
            with self.assertRaises(StoreSchemaError):
                fixture.store.get_agent_tool_invocation(
                    stored.invocation_id
                )
            with self.assertRaises(StoreSchemaError):
                fixture.store.get_tool_receipt(
                    stored.run_id,
                    stored.child_attempt_id,
                )
            self.assertIsNotNone(receipt)
            self.assertIsNotNone(ref)

    def test_expired_claim_converges_unknown_and_rejects_late_receipt(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            request = fixture.request()
            _reserved, claim = fixture.start(
                request,
                lease_seconds=10.0,
            )
            receipt, ref = fixture.terminal_payload(claim)

            fixture.clock.now = 109.0
            with self.assertRaisesRegex(
                AgentToolInvocationConflict,
                "agent_tool_active",
            ):
                fixture.store.expire_agent_tool_invocation(
                    claim.invocation.invocation_id,
                    request.invocation_digest,
                    now=fixture.clock.now,
                )

            fixture.clock.now = 110.0
            expired = fixture.store.expire_agent_tool_invocation(
                claim.invocation.invocation_id,
                request.invocation_digest,
                now=fixture.clock.now,
            )
            self.assertEqual(
                expired.status,
                AttemptStatus.OUTCOME_UNKNOWN,
            )
            stored_receipt = fixture.store.get_tool_receipt(
                expired.run_id,
                expired.child_attempt_id,
            )
            self.assertIsNotNone(stored_receipt)
            self.assertEqual(
                stored_receipt.sandbox_receipt_absence_reason,
                "agent_tool_lease_expired",
            )
            with self.assertRaisesRegex(
                AgentToolInvocationConflict,
                "agent_tool_state_conflict",
            ):
                fixture.store.complete_agent_tool_invocation(
                    claim,
                    receipt,
                    result_artifact_ref=ref,
                    now=fixture.clock.now,
                )

            fixture.scheduler.complete_claim(
                fixture.parent,
                attempt_status=AttemptStatus.FAILED,
                error_class="recovered_unknown_child",
            )

    def test_expiry_wins_race_at_exact_lease_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            request = fixture.request()
            _reserved, claim = fixture.start(
                request,
                lease_seconds=10.0,
            )
            receipt, ref = fixture.terminal_payload(claim)
            fixture.clock.now = 110.0

            def complete():
                try:
                    return fixture.store.complete_agent_tool_invocation(
                        claim,
                        receipt,
                        result_artifact_ref=ref,
                        now=fixture.clock.now,
                    )
                except AgentToolInvocationConflict as exc:
                    return exc.reason_code

            def expire():
                try:
                    return fixture.store.expire_agent_tool_invocation(
                        claim.invocation.invocation_id,
                        request.invocation_digest,
                        now=fixture.clock.now,
                    )
                except AgentToolInvocationConflict as exc:
                    return exc.reason_code

            with ThreadPoolExecutor(max_workers=2) as pool:
                completion = pool.submit(complete)
                expiration = pool.submit(expire)
                outcomes = (completion.result(), expiration.result())

            stored = fixture.store.get_agent_tool_invocation(
                claim.invocation.invocation_id
            )
            self.assertIsNotNone(stored)
            self.assertEqual(
                stored.status,
                AttemptStatus.OUTCOME_UNKNOWN,
            )
            self.assertTrue(
                {
                    "agent_tool_claim_mismatch",
                    "agent_tool_state_conflict",
                }.intersection(outcomes)
            )
            terminal_events = [
                event
                for event in fixture.store.list_events(request.run_id)
                if event.event_type == "agent_tool.outcome_unknown"
            ]
            self.assertEqual(len(terminal_events), 1)

    def test_raw_claim_token_is_never_persisted_or_rendered(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = _Fixture(root)
            request = fixture.request()
            _reserved, claim = fixture.start(request)

            token = claim.claim_token
            self.assertNotIn(token, repr(claim))
            self.assertNotIn(token, repr(claim.invocation))
            for event in fixture.store.list_events(request.run_id):
                self.assertNotIn(token, str(event.to_dict()))
            for path in root.rglob("*"):
                if path.is_file():
                    self.assertNotIn(token.encode("utf-8"), path.read_bytes())

    def test_large_valid_turn_is_not_narrowed_by_store_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            request = fixture.request(turn=999_999)
            stored = fixture.store.reserve_agent_tool_invocation(request)
            self.assertEqual(stored.turn, 999_999)

    def test_schema_nine_migration_creates_dynamic_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            with sqlite3.connect(fixture.store.path) as conn:
                conn.executescript(
                    """
                    DROP TABLE agent_turn_checkpoints;
                    DROP TABLE agent_tool_invocations;
                    DELETE FROM schema_migrations WHERE version >= 9;
                    PRAGMA user_version = 8;
                    """
                )

            DurableRunStore(fixture.store.path)
            with sqlite3.connect(fixture.store.path) as conn:
                version = conn.execute(
                    "SELECT MAX(version) FROM schema_migrations"
                ).fetchone()[0]
                table_count = conn.execute(
                    """
                    SELECT COUNT(*) FROM sqlite_master
                    WHERE type = 'table'
                      AND name = 'agent_tool_invocations'
                    """
                ).fetchone()[0]
            self.assertEqual(version, STORE_SCHEMA_VERSION)
            self.assertEqual(table_count, 1)

    def test_current_schema_cannot_silently_omit_dynamic_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture(Path(directory))
            with sqlite3.connect(fixture.store.path) as conn:
                conn.execute("DROP TABLE agent_tool_invocations")

            with self.assertRaisesRegex(
                StoreSchemaError,
                "ledger schema is incomplete",
            ):
                DurableRunStore(fixture.store.path)


if __name__ == "__main__":
    unittest.main()
