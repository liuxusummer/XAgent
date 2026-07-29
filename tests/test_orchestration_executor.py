from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from src.orchestration.artifacts import ArtifactKind, LocalArtifactStore
from src.orchestration.executor import (
    ActivityExecutionConflict,
    ArtifactReceiptError,
    ControlPlaneIsolationError,
    DurableApprovalRegistry,
    ToolReceipt,
    ToolReceiptVerification,
    TrustedActivityExecutor,
    _decision_digest,
)
from src.orchestration.models import AttemptStatus, IdempotencyStatus, NodeStatus, RunStatus
from src.orchestration.policy import (
    ApprovalGrant,
    Capability,
    EffectClass,
    PolicyEngine,
    PolicyDecision,
    PolicyOutcome,
    PolicyRule,
    ToolTimeoutBehavior,
    ToolPolicy,
)
from src.orchestration.replay import build_replay_report
from src.orchestration.sandbox import (
    BackendExecutionResult,
    EnvironmentBinding,
    ResourceLimits,
    SandboxDispatcher,
    SandboxProfile,
    SecurityLevel,
)
from src.orchestration.scheduler import DurableScheduler, SchedulerStateError
from src.orchestration.store import DurableRunStore
from src.orchestration.workflow import compile_workflow


class _Clock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class _Backend:
    backend_id = "isolated"
    security_level = SecurityLevel.OS_SANDBOX
    capabilities = ()
    supports_materialized_script = True

    def __init__(
        self,
        artifacts: LocalArtifactStore,
        *,
        raises: bool = False,
        corrupts_artifact: bool = False,
        cancellation_result: str | None = None,
    ) -> None:
        self.artifacts = artifacts
        self.raises = raises
        self.corrupts_artifact = corrupts_artifact
        self.cancellation_result = cancellation_result
        self.calls = 0
        self.materialized_scripts: list[bytes | None] = []

    def execute(self, request, profile) -> BackendExecutionResult:
        del profile
        self.calls += 1
        self.materialized_scripts.append(request.materialized_script)
        if self.cancellation_result == "cancelled":
            return BackendExecutionResult(exit_code=None, cancelled=True)
        if self.cancellation_result == "unknown":
            return BackendExecutionResult(
                exit_code=None,
                cancellation_uncertain=True,
            )
        if self.raises:
            raise RuntimeError("raw backend diagnostic must not persist")
        action = request.action
        ref = self.artifacts.put_json(
            {"raw_output": "backend-output-secret"},
            kind=ArtifactKind.TOOL_RESULT,
            producer_run_id=action.run_id,
            producer_node_id=action.node_id,
            producer_attempt_id=action.attempt_id,
        )
        if self.corrupts_artifact:
            (self.artifacts.root / ref.uri).write_bytes(b"corrupted")
        return BackendExecutionResult(
            exit_code=0,
            output_artifact_refs=(ref,),
        )


class _ExplodingDispatcher:
    def __init__(self, policy_version: str) -> None:
        self.policy_version = policy_version

    def dispatch(self, request, profile):
        del request, profile
        raise RuntimeError("dispatcher-secret-diagnostic")


class TrustedActivityExecutorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.control_root = self.root / "control-plane"
        self.agent_root = self.root / "agent-workspace"
        self.control_root.mkdir()
        self.agent_root.mkdir()
        self.clock = _Clock()
        self.artifacts = LocalArtifactStore(self.control_root / "artifacts")

    def _runtime(
        self,
        *,
        effect_class: EffectClass = EffectClass.READ_ONLY,
        policy_outcome: PolicyOutcome | None = None,
        backend_raises: bool = False,
        corrupts_artifact: bool = False,
        backend_cancellation_result: str | None = None,
        requires_script_artifact: bool = False,
        authorization_value: object = "token-secret",
        sensitive_argument_key: str = "authorization",
        idempotency_key_template: str = "{{run_id}}:{{node_id}}",
        fault_hook=None,
        run_id: str = "run",
    ):
        node = {
            "id": "tool",
            "kind": "tool",
            "config": {
                "tool": "exec",
                "arguments": {
                    "command": "argument-secret",
                    sensitive_argument_key: authorization_value,
                },
            },
            "effect_class": effect_class.value,
            "resource_keys": ["workspace:/project"],
        }
        if effect_class is EffectClass.IDEMPOTENT_WRITE:
            node["idempotency_key_template"] = idempotency_key_template
        workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": f"executor-{effect_class.value}",
                "version": 1,
                "nodes": [node],
            }
        )
        store = DurableRunStore(self.control_root / f"{run_id}.sqlite3")
        scheduler = DurableScheduler(
            store,
            workflow,
            clock=self.clock,
            artifact_verifier=self.artifacts.verify,
        )
        scheduler.create_run(run_id)
        claim = scheduler.claim_next(run_id, "worker")
        assert claim is not None

        rules = ()
        if policy_outcome is not None:
            rules = (
                PolicyRule(
                    "tool-rule",
                    policy_outcome,
                    tool_name="exec",
                    effect_classes=(effect_class,),
                    reason_code=f"rule_{policy_outcome.value}",
                ),
            )
        tool_policy = ToolPolicy(
            "exec",
            effect_class,
            supports_idempotency_key=(
                True if effect_class is EffectClass.IDEMPOTENT_WRITE else False
            ),
            supports_status_probe=(
                True if effect_class is EffectClass.IDEMPOTENT_WRITE else False
            ),
            supports_compensation=False,
            requires_script_artifact=requires_script_artifact,
            timeout_behavior=(
                ToolTimeoutBehavior.SAFE_TO_RETRY
                if effect_class is EffectClass.READ_ONLY
                else (
                    ToolTimeoutBehavior.PROBE_BEFORE_RETRY
                    if effect_class is EffectClass.IDEMPOTENT_WRITE
                    else ToolTimeoutBehavior.OUTCOME_UNKNOWN
                )
            ),
            allowed_resource_keys=("workspace:/project",),
            required_resource_keys=(
                () if effect_class is EffectClass.READ_ONLY else ("workspace:/project",)
            ),
        )
        policy = PolicyEngine(
            (tool_policy,),
            rules,
        )
        backend = _Backend(
            self.artifacts,
            raises=backend_raises,
            corrupts_artifact=corrupts_artifact,
            cancellation_result=backend_cancellation_result,
        )
        dispatcher = SandboxDispatcher(
            (backend,),
            policy_version=policy.policy_version,
        )
        profile = SandboxProfile(
            "profile",
            (self.agent_root,),
            (),
            minimum_security_level=SecurityLevel.OS_SANDBOX,
        )
        executor = TrustedActivityExecutor(
            scheduler,
            policy,
            dispatcher,
            artifact_verifier=self.artifacts.verify,
            artifact_reader=self.artifacts.read,
            clock=self.clock,
            fault_hook=fault_hook,
        )
        return store, scheduler, policy, backend, profile, executor, claim

    @staticmethod
    def _execute(executor, claim, profile, *, grant=None):
        return executor.execute(
            claim,
            argv=("runner", "--secret-argv"),
            cwd=profile.allowed_roots[0],
            profile=profile,
            approval_grant=grant,
            resource_locks=claim.resource_keys,
            sensitive_keys=("authorization",),
        )

    def _grant(
        self,
        executor,
        claim,
        policy,
        profile,
        *,
        approval_id="approval-1",
    ):
        action = executor.build_action(
            claim,
            argv=("runner", "--secret-argv"),
            cwd=profile.allowed_roots[0],
            profile=profile,
            resource_locks=claim.resource_keys,
            sensitive_keys=("authorization",),
        )
        return ApprovalGrant(
            approval_id=approval_id,
            action_digest=action.action_digest,
            run_id=claim.run_id,
            node_id=claim.node_id,
            policy_version=policy.policy_version,
            actor="trusted-operator",
            expires_at=200,
        )

    def _register(self, store, grant):
        registry = DurableApprovalRegistry(
            store,
            trusted_actors=("trusted-operator",),
            clock=self.clock,
        )
        registry.register_issued(grant)

    def test_deny_is_atomic_and_executes_nothing(self) -> None:
        store, _, _, backend, profile, executor, claim = self._runtime(
            policy_outcome=PolicyOutcome.DENY,
        )

        result = self._execute(executor, claim, profile)

        self.assertEqual(result.attempt_status, AttemptStatus.FAILED)
        self.assertEqual(backend.calls, 0)
        self.assertEqual(store.get_node("run", "tool").status, NodeStatus.FAILED)
        attempt = store.get_attempt(claim.attempt_id)
        self.assertEqual(attempt.status, AttemptStatus.FAILED)
        self.assertIsNone(attempt.started_at)
        record = store.get_idempotency("run", attempt.idempotency_key)
        self.assertEqual(record.status, IdempotencyStatus.COMPLETED)
        event_types = [event.event_type for event in store.list_events("run")]
        self.assertIn("policy.decided", event_types)
        self.assertNotIn("attempt.started", event_types)

    def test_approval_is_single_use_and_duplicate_execute_is_replayed(self) -> None:
        store, _, policy, backend, profile, executor, claim = self._runtime(
            effect_class=EffectClass.IDEMPOTENT_WRITE,
        )
        grant = self._grant(executor, claim, policy, profile)
        self._register(store, grant)
        issue = next(
            event
            for event in store.list_events("run")
            if event.event_type == "approval.resolved"
        )
        issue_payload = dict(issue.payload)
        issue_payload.pop("projection", None)
        self.assertNotIn("approval_id_digest", issue_payload)
        self.assertNotIn("grant_digest", issue_payload)
        self.assertNotIn(grant.actor, str(issue_payload))
        self.assertNotIn(grant.approval_id, str(issue_payload))

        first = self._execute(executor, claim, profile, grant=grant)
        second = self._execute(executor, claim, profile, grant=grant)

        self.assertTrue(first.succeeded)
        self.assertTrue(second.replayed)
        self.assertEqual(second.completion_event, first.completion_event)
        self.assertEqual(
            second.tool_receipt_digest,
            first.tool_receipt_digest,
        )
        self.assertEqual(
            second.sandbox_receipt_digest,
            first.sandbox_receipt_digest,
        )
        stored_receipt = store.get_tool_receipt("run", claim.attempt_id)
        assert stored_receipt is not None
        self.assertEqual(
            stored_receipt.receipt_digest,
            first.tool_receipt_digest,
        )
        self.assertEqual(backend.calls, 1)
        events = store.list_events("run")
        self.assertEqual(
            sum(event.event_type == "policy.decided" for event in events),
            1,
        )
        self.assertEqual(
            sum(event.event_type == "attempt.started" for event in events),
            1,
        )
        self.assertTrue(build_replay_report(store, "run").matches_live)

        stale = replace(claim, claim_token="stale-token")
        with self.assertRaises(ActivityExecutionConflict):
            self._execute(executor, stale, profile, grant=grant)
        self.assertEqual(backend.calls, 1)

    def test_required_approval_waits_durably_and_resumes_same_attempt(
        self,
    ) -> None:
        store, scheduler, policy, backend, profile, executor, claim = self._runtime(
            effect_class=EffectClass.IDEMPOTENT_WRITE,
            run_id="durable-approval-wait",
        )
        grant = self._grant(
            executor,
            claim,
            policy,
            profile,
            approval_id="restart-safe-approval",
        )

        waiting = self._execute(executor, claim, profile)

        self.assertEqual(
            waiting.attempt_status,
            AttemptStatus.WAITING_APPROVAL,
        )
        self.assertIsNone(waiting.completion_event)
        self.assertEqual(backend.calls, 0)
        self.assertEqual(
            store.get_run(claim.run_id).status,
            RunStatus.WAITING_APPROVAL,
        )
        self.assertEqual(
            store.get_node(claim.run_id, claim.node_id).status,
            NodeStatus.WAITING_APPROVAL,
        )
        persisted_attempt = store.get_attempt(claim.attempt_id)
        self.assertEqual(
            persisted_attempt.status,
            AttemptStatus.WAITING_APPROVAL,
        )
        self.assertIsNone(persisted_attempt.worker_id)
        self.assertIsNone(persisted_attempt.lease_id)
        request_events = [
            event
            for event in store.list_events(claim.run_id)
            if event.event_type == "approval.requested"
        ]
        self.assertEqual(len(request_events), 1)
        request_digest = request_events[0].payload["approval_request_digest"]
        self.assertTrue(store.verify_projections(claim.run_id))

        restarted_store = DurableRunStore(store.path)
        restarted_scheduler = DurableScheduler(
            restarted_store,
            scheduler.workflow,
            clock=self.clock,
            artifact_verifier=self.artifacts.verify,
        )
        self.assertIsNone(
            restarted_scheduler.claim_next(claim.run_id, "worker-before-grant")
        )
        registry = DurableApprovalRegistry(
            restarted_store,
            trusted_actors=("trusted-operator",),
            clock=self.clock,
        )
        registry.register_issued(grant)

        resumed_claim = restarted_scheduler.claim_next(
            claim.run_id,
            "worker-after-grant",
        )
        assert resumed_claim is not None
        self.assertEqual(resumed_claim.attempt_id, claim.attempt_id)
        self.assertGreater(resumed_claim.fencing_token, claim.fencing_token)
        restarted_executor = TrustedActivityExecutor(
            restarted_scheduler,
            policy,
            executor.sandbox,
            artifact_verifier=self.artifacts.verify,
            artifact_reader=self.artifacts.read,
            clock=self.clock,
        )
        completed = self._execute(
            restarted_executor,
            resumed_claim,
            profile,
            grant=grant,
        )

        self.assertTrue(completed.succeeded)
        self.assertEqual(backend.calls, 1)
        self.assertEqual(
            restarted_store.get_run(claim.run_id).status,
            RunStatus.COMPLETED,
        )
        self.assertEqual(
            [
                event.payload["approval_request_digest"]
                for event in restarted_store.list_events(claim.run_id)
                if event.event_type == "approval.requested"
            ],
            [request_digest],
        )
        self.assertTrue(
            restarted_store.verify_projections(claim.run_id)
        )

    def test_trusted_rejection_terminalizes_pending_approval(self) -> None:
        store, _, _, backend, profile, executor, claim = self._runtime(
            effect_class=EffectClass.IDEMPOTENT_WRITE,
            run_id="durable-approval-reject",
        )
        waiting = self._execute(executor, claim, profile)
        self.assertEqual(
            waiting.attempt_status,
            AttemptStatus.WAITING_APPROVAL,
        )
        registry = DurableApprovalRegistry(
            store,
            trusted_actors=("trusted-operator",),
            clock=self.clock,
        )
        with self.assertRaises(ActivityExecutionConflict):
            registry.reject_requested(
                claim.run_id,
                claim.attempt_id,
                actor="untrusted",
            )
        rejected = registry.reject_requested(
            claim.run_id,
            claim.attempt_id,
            actor="trusted-operator",
        )
        repeated = registry.reject_requested(
            claim.run_id,
            claim.attempt_id,
            actor="trusted-operator",
        )

        self.assertEqual(repeated, rejected)
        self.assertEqual(backend.calls, 0)
        self.assertEqual(
            store.get_attempt(claim.attempt_id).status,
            AttemptStatus.FAILED,
        )
        self.assertEqual(
            store.get_node(claim.run_id, claim.node_id).status,
            NodeStatus.FAILED,
        )
        self.assertEqual(
            store.get_run(claim.run_id).status,
            RunStatus.FAILED,
        )
        self.assertTrue(store.verify_projections(claim.run_id))

    def test_pending_activity_approval_can_be_cancelled_durably(self) -> None:
        store, scheduler, _, backend, profile, executor, claim = self._runtime(
            effect_class=EffectClass.IDEMPOTENT_WRITE,
            run_id="cancel-pending-approval",
        )
        waiting = self._execute(executor, claim, profile)
        self.assertEqual(
            waiting.attempt_status,
            AttemptStatus.WAITING_APPROVAL,
        )

        scheduler.request_cancel(claim.run_id)

        self.assertEqual(backend.calls, 0)
        self.assertEqual(
            store.get_attempt(claim.attempt_id).status,
            AttemptStatus.CANCELLED,
        )
        self.assertEqual(
            store.get_node(claim.run_id, claim.node_id).status,
            NodeStatus.CANCELLED,
        )
        self.assertEqual(
            store.get_run(claim.run_id).status,
            RunStatus.CANCELLED,
        )
        self.assertTrue(store.verify_projections(claim.run_id))

    def test_unissued_approval_is_rejected_without_hanging_claim(self) -> None:
        store, _, policy, backend, profile, executor, claim = self._runtime(
            effect_class=EffectClass.IDEMPOTENT_WRITE,
        )
        unissued = self._grant(executor, claim, policy, profile)

        result = self._execute(executor, claim, profile, grant=unissued)

        self.assertEqual(result.attempt_status, AttemptStatus.FAILED)
        self.assertEqual(backend.calls, 0)
        attempt = store.get_attempt(claim.attempt_id)
        self.assertEqual(attempt.status, AttemptStatus.FAILED)
        record = store.get_idempotency("run", attempt.idempotency_key)
        self.assertEqual(record.status, IdempotencyStatus.COMPLETED)
        self.assertEqual(
            result.policy_event.payload["reason_code"],
            "approval_unissued",
        )

    def test_decision_digest_excludes_approval_and_actor_identity(self) -> None:
        common = {
            "outcome": PolicyOutcome.ALLOW,
            "action_digest": "a" * 64,
            "policy_version": f"sha256:{'b' * 64}",
            "reason_code": "approval_consumed",
        }
        first = PolicyDecision(
            **common,
            approval_id="approval-one",
            approved_by="operator-one",
        )
        second = PolicyDecision(
            **common,
            approval_id="approval-two",
            approved_by="operator-two",
        )

        self.assertEqual(_decision_digest(first), _decision_digest(second))

    def test_policy_decision_is_committed_before_start_and_is_sanitized(self) -> None:
        store, _, _, backend, profile, executor, claim = self._runtime()

        result = self._execute(executor, claim, profile)

        self.assertTrue(result.succeeded)
        self.assertEqual(backend.calls, 1)
        events = store.list_events("run")
        decision = next(event for event in events if event.event_type == "policy.decided")
        started = next(event for event in events if event.event_type == "attempt.started")
        completed = next(event for event in events if event.event_type == "attempt.succeeded")
        self.assertLess(decision.seq, started.seq)
        self.assertLess(started.seq, completed.seq)
        tool_receipt = ToolReceipt.from_dict(
            completed.payload["tool_receipt"]
        )
        self.assertEqual(tool_receipt.schema_version, 1)
        self.assertEqual(tool_receipt.tool_name, "exec")
        self.assertEqual(tool_receipt.effect_class, EffectClass.READ_ONLY)
        self.assertEqual(
            tool_receipt.verification,
            ToolReceiptVerification.VERIFIED,
        )
        self.assertIsNotNone(tool_receipt.sandbox_receipt)
        self.assertEqual(
            tool_receipt.sandbox_receipt["outcome"],
            "succeeded",
        )
        self.assertEqual(
            completed.payload["receipt_digest"],
            tool_receipt.receipt_digest,
        )
        self.assertEqual(
            completed.payload["sandbox_receipt_digest"],
            tool_receipt.sandbox_receipt_digest,
        )

        safe_payloads = []
        for event in (decision, completed):
            payload = dict(event.payload)
            payload.pop("projection", None)
            safe_payloads.append(payload)
        serialized = json.dumps(safe_payloads, sort_keys=True)
        for raw in (
            "argument-secret",
            "token-secret",
            "--secret-argv",
            "backend-output-secret",
            "trusted-operator",
            "worker",
        ):
            self.assertNotIn(raw, serialized)
        self.assertIn("tool_receipt", safe_payloads[1])
        for secret in ("argument-secret", "token-secret", "--secret-argv"):
            direct_digest = hashlib.sha256(secret.encode("utf-8")).hexdigest()
            for sqlite_path in self.control_root.glob("run.sqlite3*"):
                stored = sqlite_path.read_bytes()
                self.assertNotIn(secret.encode("utf-8"), stored)
                self.assertNotIn(direct_digest.encode("ascii"), stored)

    def test_backend_exception_fails_read_only_attempt(self) -> None:
        store, _, _, backend, profile, executor, claim = self._runtime(
            backend_raises=True,
        )

        result = self._execute(executor, claim, profile)

        self.assertEqual(backend.calls, 1)
        self.assertEqual(result.attempt_status, AttemptStatus.FAILED)
        self.assertEqual(
            store.get_attempt(claim.attempt_id).status,
            AttemptStatus.FAILED,
        )
        payload = dict(result.completion_event.payload)
        payload.pop("projection", None)
        self.assertNotIn("raw backend diagnostic", json.dumps(payload))
        tool_receipt = ToolReceipt.from_dict(payload["tool_receipt"])
        self.assertEqual(
            tool_receipt.verification,
            ToolReceiptVerification.UNVERIFIED,
        )
        self.assertEqual(
            tool_receipt.sandbox_receipt["outcome"],
            "backend_error",
        )
        self.assertIsNone(tool_receipt.sandbox_receipt_absence_reason)
        self.assertEqual(
            payload["sandbox_receipt_digest"],
            tool_receipt.sandbox_receipt_digest,
        )

    def test_backend_cannot_forge_cancellation_without_durable_intent(self) -> None:
        store, _, _, backend, profile, executor, claim = self._runtime(
            backend_cancellation_result="cancelled",
            run_id="forged-cancellation",
        )

        with self.assertRaisesRegex(
            ActivityExecutionConflict,
            "no matching durable Run intent",
        ):
            self._execute(executor, claim, profile)

        self.assertEqual(backend.calls, 1)
        self.assertEqual(
            store.get_attempt(claim.attempt_id).status,
            AttemptStatus.RUNNING,
        )
        self.assertEqual(
            store.get_run(claim.run_id).status,
            RunStatus.RUNNING,
        )

    def test_cancellation_probe_uncertainty_never_becomes_failed(self) -> None:
        store, _, _, backend, profile, executor, claim = self._runtime(
            backend_cancellation_result="unknown",
            run_id="cancellation-unknown",
        )

        result = self._execute(executor, claim, profile)

        self.assertEqual(backend.calls, 1)
        self.assertEqual(
            result.attempt_status,
            AttemptStatus.OUTCOME_UNKNOWN,
        )
        self.assertEqual(
            store.get_run(claim.run_id).status,
            RunStatus.WAITING_RECOVERY,
        )
        tool_receipt = ToolReceipt.from_dict(
            result.completion_event.payload["tool_receipt"]
        )
        self.assertEqual(
            tool_receipt.sandbox_receipt["outcome"],
            "cancellation_unknown",
        )
        self.assertEqual(
            tool_receipt.verification,
            ToolReceiptVerification.UNVERIFIED,
        )

    def test_dispatch_denial_records_explicit_receipt_absence(self) -> None:
        store, _, _, backend, _, executor, claim = self._runtime(
            run_id="dispatch-denied",
        )
        profile = SandboxProfile(
            "container-only",
            (self.agent_root,),
            (),
            minimum_security_level=SecurityLevel.CONTAINER,
        )

        result = self._execute(executor, claim, profile)

        self.assertEqual(result.attempt_status, AttemptStatus.FAILED)
        self.assertEqual(backend.calls, 0)
        self.assertIsNone(result.sandbox_receipt_digest)
        self.assertEqual(
            result.receipt_absence_reason,
            "dispatch_denied_before_backend",
        )
        payload = dict(result.completion_event.payload)
        payload.pop("projection", None)
        tool_receipt = ToolReceipt.from_dict(payload["tool_receipt"])
        self.assertIsNone(tool_receipt.sandbox_receipt)
        self.assertEqual(
            tool_receipt.sandbox_receipt_absence_reason,
            "dispatch_denied_before_backend",
        )
        self.assertEqual(
            tool_receipt.verification,
            ToolReceiptVerification.UNVERIFIED,
        )
        self.assertEqual(payload["error_code"], "no_qualified_backend")

    def test_agent_scope_cannot_overlap_control_plane_storage(self) -> None:
        _, _, _, backend, _, executor, claim = self._runtime(
            run_id="control-plane-overlap",
        )
        for allowed_root in (self.control_root, self.root):
            with self.subTest(allowed_root=allowed_root):
                profile = SandboxProfile(
                    f"overlap-{allowed_root.name}",
                    (allowed_root,),
                    (),
                    minimum_security_level=SecurityLevel.OS_SANDBOX,
                )
                with self.assertRaisesRegex(
                    ControlPlaneIsolationError,
                    "overlaps trusted control-plane",
                ):
                    executor.execute(
                        claim,
                        argv=("runner", "--overlap"),
                        cwd=str(allowed_root),
                        profile=profile,
                        resource_locks=claim.resource_keys,
                    )
        self.assertEqual(backend.calls, 0)

    def test_uncertain_dispatcher_exception_records_receipt_absence(self) -> None:
        (
            store,
            scheduler,
            policy,
            backend,
            profile,
            executor,
            claim,
        ) = self._runtime(
            effect_class=EffectClass.DESTRUCTIVE,
            run_id="uncertain-no-receipt",
        )
        del backend
        exploding = TrustedActivityExecutor(
            scheduler,
            policy,
            _ExplodingDispatcher(policy.policy_version),
            artifact_verifier=self.artifacts.verify,
            artifact_reader=self.artifacts.read,
            clock=self.clock,
        )
        grant = self._grant(executor, claim, policy, profile)
        self._register(store, grant)

        result = self._execute(exploding, claim, profile, grant=grant)

        self.assertEqual(
            result.attempt_status,
            AttemptStatus.OUTCOME_UNKNOWN,
        )
        self.assertIsNone(result.sandbox_receipt_digest)
        self.assertEqual(
            result.receipt_absence_reason,
            "dispatcher_exception_before_receipt",
        )
        payload = dict(result.completion_event.payload)
        payload.pop("projection", None)
        tool_receipt = ToolReceipt.from_dict(payload["tool_receipt"])
        self.assertIsNone(tool_receipt.sandbox_receipt)
        self.assertEqual(
            tool_receipt.sandbox_receipt_absence_reason,
            "dispatcher_exception_before_receipt",
        )
        for sqlite_path in self.control_root.glob("uncertain-no-receipt.sqlite3*"):
            self.assertNotIn(
                b"dispatcher-secret-diagnostic",
                sqlite_path.read_bytes(),
            )

    def test_destructive_backend_uncertainty_moves_run_to_recovery(self) -> None:
        store, _, policy, backend, profile, executor, claim = self._runtime(
            effect_class=EffectClass.DESTRUCTIVE,
            backend_raises=True,
        )
        grant = self._grant(executor, claim, policy, profile)
        self._register(store, grant)

        result = self._execute(executor, claim, profile, grant=grant)

        self.assertEqual(backend.calls, 1)
        self.assertEqual(result.attempt_status, AttemptStatus.OUTCOME_UNKNOWN)
        self.assertEqual(store.get_run("run").status, RunStatus.WAITING_RECOVERY)
        self.assertEqual(
            store.get_node("run", "tool").status,
            NodeStatus.WAITING_RECOVERY,
        )

    def test_corrupt_artifact_fails_closed(self) -> None:
        store, _, _, backend, profile, executor, claim = self._runtime(
            corrupts_artifact=True,
        )

        result = self._execute(executor, claim, profile)

        self.assertEqual(backend.calls, 1)
        self.assertEqual(result.attempt_status, AttemptStatus.FAILED)
        self.assertEqual(
            store.get_attempt(claim.attempt_id).status,
            AttemptStatus.FAILED,
        )
        self.assertEqual(
            result.completion_event.payload["error_code"],
            "artifact_integrity",
        )

    def test_f20_restart_reuses_authorization_then_executes_once(self) -> None:
        crashed = False

        def crash_after_authorization(stage: str) -> None:
            nonlocal crashed
            if stage == "authorization.committed" and not crashed:
                crashed = True
                raise RuntimeError("simulated process crash")

        store, scheduler, policy, backend, profile, executor, claim = self._runtime(
            effect_class=EffectClass.IDEMPOTENT_WRITE,
            fault_hook=crash_after_authorization,
        )
        grant = self._grant(executor, claim, policy, profile)
        self._register(store, grant)

        with self.assertRaisesRegex(RuntimeError, "simulated process crash"):
            self._execute(executor, claim, profile, grant=grant)
        self.assertEqual(backend.calls, 0)
        self.assertEqual(
            store.get_attempt(claim.attempt_id).status,
            AttemptStatus.CLAIMED,
        )
        self.assertEqual(
            sum(
                event.event_type == "policy.decided"
                for event in store.list_events("run")
            ),
            1,
        )

        restarted = TrustedActivityExecutor(
            scheduler,
            policy,
            SandboxDispatcher(
                (backend,),
                policy_version=policy.policy_version,
            ),
            artifact_verifier=self.artifacts.verify,
            artifact_reader=self.artifacts.read,
            clock=self.clock,
        )
        with self.assertRaises(ActivityExecutionConflict):
            restarted.execute(
                claim,
                argv=("runner", "--replaced-after-restart"),
                cwd=profile.allowed_roots[0],
                profile=profile,
                resource_locks=claim.resource_keys,
                sensitive_keys=("authorization",),
            )
        self.assertEqual(backend.calls, 0)
        self.assertNotIn(
            "attempt.started",
            [event.event_type for event in store.list_events("run")],
        )

        result = self._execute(restarted, claim, profile)

        self.assertTrue(result.succeeded)
        self.assertEqual(backend.calls, 1)
        events = store.list_events("run")
        self.assertEqual(
            sum(event.event_type == "policy.decided" for event in events),
            1,
        )
        self.assertEqual(
            sum(event.event_type == "attempt.started" for event in events),
            1,
        )

    def test_approval_binds_every_execution_intent_field(self) -> None:
        cwd_variant = self.agent_root / "alternate-cwd"
        cwd_variant.mkdir()
        cases = (
            "argv",
            "input_artifact_refs",
            "script_artifact_ref",
            "environment",
            "limits",
            "profile",
            "cwd",
        )

        for index, field_name in enumerate(cases, start=1):
            with self.subTest(field=field_name):
                run_id = f"intent-{index}"
                store, _, policy, backend, _, executor, claim = self._runtime(
                    effect_class=EffectClass.IDEMPOTENT_WRITE,
                    run_id=run_id,
                )
                profile = SandboxProfile(
                    "intent-profile",
                    (self.agent_root,),
                    (),
                    minimum_security_level=SecurityLevel.OS_SANDBOX,
                    environment_allowlist=("DEMO_BINDING",),
                )
                primary = self.artifacts.put_json(
                    {"input": "primary", "case": index},
                    producer_run_id=run_id,
                )
                alternate = self.artifacts.put_json(
                    {"input": "alternate", "case": index},
                    producer_run_id=run_id,
                )
                approved_script = self.artifacts.put_bytes(
                    b"approved script bytes",
                    producer_run_id=run_id,
                )
                substituted_script = self.artifacts.put_bytes(
                    b"substituted script bytes",
                    producer_run_id=run_id,
                )
                original = {
                    "argv": ("runner", "--approved"),
                    "cwd": profile.allowed_roots[0],
                    "profile": profile,
                    "limits": ResourceLimits(timeout_seconds=10),
                    "input_artifact_refs": (primary,),
                    "script_artifact_ref": approved_script,
                    "environment": (EnvironmentBinding("DEMO_BINDING", "2" * 64),),
                    "resource_locks": claim.resource_keys,
                    "sensitive_keys": ("authorization",),
                }
                action = executor.build_action(claim, **original)
                grant = ApprovalGrant(
                    approval_id=f"approval-intent-{index}",
                    action_digest=action.action_digest,
                    run_id=run_id,
                    node_id=claim.node_id,
                    policy_version=policy.policy_version,
                    actor="trusted-operator",
                    expires_at=200,
                )
                self._register(store, grant)

                changed = dict(original)
                replacements = {
                    "argv": ("runner", "--substituted"),
                    "input_artifact_refs": (alternate,),
                    "script_artifact_ref": substituted_script,
                    "environment": (
                        EnvironmentBinding("DEMO_BINDING", "4" * 64),
                    ),
                    "limits": ResourceLimits(timeout_seconds=11),
                    "profile": SandboxProfile(
                        "substituted-profile",
                        (self.agent_root,),
                        (),
                        minimum_security_level=SecurityLevel.OS_SANDBOX,
                        environment_allowlist=("DEMO_BINDING",),
                    ),
                    "cwd": str(cwd_variant),
                }
                changed[field_name] = replacements[field_name]

                result = executor.execute(
                    claim,
                    approval_grant=grant,
                    **changed,
                )

                self.assertEqual(result.attempt_status, AttemptStatus.FAILED)
                self.assertEqual(backend.calls, 0)
                event_types = [
                    event.event_type for event in store.list_events(run_id)
                ]
                self.assertNotIn("attempt.started", event_types)

    def test_verified_script_bytes_are_exactly_what_backend_receives(self) -> None:
        _, _, _, backend, profile, executor, claim = self._runtime(
            run_id="script-bytes",
        )
        script_bytes = b"print('immutable script payload')"
        script_ref = self.artifacts.put_bytes(
            script_bytes,
            producer_run_id=claim.run_id,
        )

        result = executor.execute(
            claim,
            argv=("runner", "--script-artifact"),
            cwd=profile.allowed_roots[0],
            profile=profile,
            resource_locks=claim.resource_keys,
            script_artifact_ref=script_ref,
        )

        self.assertTrue(result.succeeded)
        self.assertEqual(backend.calls, 1)
        self.assertEqual(backend.materialized_scripts, [script_bytes])

    def test_required_script_cannot_fall_back_to_mutable_argv_path(self) -> None:
        store, _, _, backend, profile, executor, claim = self._runtime(
            run_id="mutable-script-path",
            requires_script_artifact=True,
        )
        mutable_path = self.agent_root / "mutable.py"
        mutable_path.write_text("print('approved')", encoding="utf-8")

        result = executor.execute(
            claim,
            argv=("python", str(mutable_path)),
            cwd=profile.allowed_roots[0],
            profile=profile,
            resource_locks=claim.resource_keys,
        )

        self.assertEqual(result.attempt_status, AttemptStatus.FAILED)
        self.assertEqual(result.policy_event.payload["reason_code"], "script_artifact_required")
        self.assertEqual(backend.calls, 0)
        event_types = [event.event_type for event in store.list_events(claim.run_id)]
        self.assertNotIn("attempt.started", event_types)

    def test_script_replacement_after_approval_fails_before_backend(self) -> None:
        store, _, policy, backend, profile, executor, claim = self._runtime(
            effect_class=EffectClass.IDEMPOTENT_WRITE,
            run_id="script-replacement",
        )
        script_ref = self.artifacts.put_bytes(
            b"approved immutable bytes",
            producer_run_id=claim.run_id,
        )
        action = executor.build_action(
            claim,
            argv=("runner", "--script-artifact"),
            cwd=profile.allowed_roots[0],
            profile=profile,
            resource_locks=claim.resource_keys,
            script_artifact_ref=script_ref,
        )
        grant = ApprovalGrant(
            approval_id="script-replacement-approval",
            action_digest=action.action_digest,
            run_id=claim.run_id,
            node_id=claim.node_id,
            policy_version=policy.policy_version,
            actor="trusted-operator",
            expires_at=200,
        )
        self._register(store, grant)
        (self.artifacts.root / script_ref.uri).write_bytes(
            b"substituted mutable bytes"
        )

        with self.assertRaises(ArtifactReceiptError):
            executor.execute(
                claim,
                argv=("runner", "--script-artifact"),
                cwd=profile.allowed_roots[0],
                profile=profile,
                approval_grant=grant,
                resource_locks=claim.resource_keys,
                script_artifact_ref=script_ref,
            )

        self.assertEqual(backend.calls, 0)
        event_types = [event.event_type for event in store.list_events(claim.run_id)]
        self.assertNotIn("policy.decided", event_types)
        self.assertNotIn("attempt.started", event_types)

    def test_operation_and_idempotency_key_substitution_fails_before_backend(self) -> None:
        for index, field_name in enumerate(
            ("operation_key", "idempotency_key"),
            start=1,
        ):
            with self.subTest(field=field_name):
                store, _, _, backend, profile, executor, claim = self._runtime(
                    run_id=f"key-substitution-{index}",
                )
                substituted = replace(
                    claim,
                    **{field_name: f"attacker-key-{index}"},
                )

                with self.assertRaises(ActivityExecutionConflict):
                    self._execute(executor, substituted, profile)

                self.assertEqual(backend.calls, 0)
                event_types = [
                    event.event_type for event in store.list_events(claim.run_id)
                ]
                self.assertNotIn("policy.decided", event_types)
                self.assertNotIn("attempt.started", event_types)

    def test_sensitive_argument_cannot_cross_in_argv_or_durable_hash(self) -> None:
        attacks = (
            ("accessToken", "low-entropy-canary-token"),
            ("ＰＡＳＳＷＯＲＤ", "fullwidth-password-canary"),
            ("api.key", "api-key-canary"),
            ("zero\u200bwidth_token", "zero-width-token-canary"),
            ("pin", 1234),
        )
        for index, (key, canary) in enumerate(attacks):
            with self.subTest(key=key):
                run_id = f"sensitive-argv-{index}"
                store, _, _, backend, profile, executor, claim = self._runtime(
                    run_id=run_id,
                    authorization_value=canary,
                    sensitive_argument_key=key,
                )
                canary_text = str(canary)

                with self.assertRaisesRegex(
                    ActivityExecutionConflict,
                    "sensitive argument values are forbidden",
                ):
                    executor.execute(
                        claim,
                        argv=("runner", f"--secret={canary_text}"),
                        cwd=profile.allowed_roots[0],
                        profile=profile,
                        resource_locks=claim.resource_keys,
                        sensitive_keys=("pin",),
                    )

                self.assertEqual(backend.calls, 0)
                event_types = [
                    event.event_type
                    for event in store.list_events(claim.run_id)
                ]
                self.assertNotIn("policy.decided", event_types)
                self.assertNotIn("attempt.started", event_types)
                forbidden = (
                    canary_text.encode("utf-8"),
                    hashlib.sha256(canary_text.encode("utf-8"))
                    .hexdigest()
                    .encode(),
                )
                for sqlite_path in self.control_root.glob(f"{run_id}.sqlite3*"):
                    stored = sqlite_path.read_bytes()
                    for marker in forbidden:
                        self.assertNotIn(marker, stored)

    def test_operation_key_limit_is_consistent_before_and_during_execution(self) -> None:
        near_limit_run_id = "r" * 63
        near_limit_template = ("{{run_id}}" * 16) + "{{node_id}}"
        (
            store,
            _,
            policy,
            backend,
            profile,
            executor,
            claim,
        ) = self._runtime(
            effect_class=EffectClass.IDEMPOTENT_WRITE,
            run_id=near_limit_run_id,
            idempotency_key_template=near_limit_template,
        )
        self.assertLessEqual(len(claim.operation_key), 1024)
        self.assertGreater(len(claim.operation_key), 1000)
        grant = self._grant(executor, claim, policy, profile)
        self._register(store, grant)

        result = self._execute(executor, claim, profile, grant=grant)

        self.assertTrue(result.succeeded)
        self.assertEqual(backend.calls, 1)

        oversized_run_id = "s" * 63
        oversized_template = ("{{run_id}}" * 17) + "{{node_id}}"
        with self.assertRaises(SchedulerStateError):
            self._runtime(
                effect_class=EffectClass.IDEMPOTENT_WRITE,
                run_id=oversized_run_id,
                idempotency_key_template=oversized_template,
            )
        oversized_store = DurableRunStore(
            self.control_root / f"{oversized_run_id}.sqlite3"
        )
        self.assertEqual(oversized_store.list_attempts(oversized_run_id), [])

    def test_input_artifacts_fail_closed_before_authorization(self) -> None:
        cases = ("missing", "corrupt", "metadata", "producer", "count")
        for index, case in enumerate(cases, start=1):
            with self.subTest(case=case):
                run_id = f"bad-input-{index}"
                store, _, _, backend, profile, executor, claim = self._runtime(
                    run_id=run_id,
                )
                ref = self.artifacts.put_json(
                    {"case": case, "unique": index},
                    producer_run_id=run_id,
                )
                if case == "missing":
                    (self.artifacts.root / ref.uri).unlink()
                    refs = (ref,)
                elif case == "corrupt":
                    (self.artifacts.root / ref.uri).write_bytes(b"corrupt")
                    refs = (ref,)
                elif case == "metadata":
                    refs = (replace(ref, metadata={"untrusted": "value"}),)
                elif case == "producer":
                    refs = (replace(ref, producer_run_id="another-run"),)
                else:
                    refs = (ref,) * 65

                with self.assertRaises(ArtifactReceiptError):
                    executor.execute(
                        claim,
                        argv=("runner", "--input-check"),
                        cwd=profile.allowed_roots[0],
                        profile=profile,
                        input_artifact_refs=refs,
                        resource_locks=claim.resource_keys,
                    )

                self.assertEqual(backend.calls, 0)
                self.assertEqual(
                    store.get_attempt(claim.attempt_id).status,
                    AttemptStatus.CLAIMED,
                )
                event_types = [
                    event.event_type for event in store.list_events(run_id)
                ]
                self.assertNotIn("policy.decided", event_types)
                self.assertNotIn("attempt.started", event_types)

    def test_generator_capabilities_and_resource_locks_are_bound_once(self) -> None:
        _, _, _, _, _, executor, claim = self._runtime(run_id="generators")
        capability = Capability("workspace.read")
        profile = SandboxProfile(
            "generator-profile",
            (self.agent_root,),
            (capability,),
            minimum_security_level=SecurityLevel.OS_SANDBOX,
        )
        common = {
            "argv": ("runner", "--generator-check"),
            "cwd": profile.allowed_roots[0],
            "profile": profile,
        }

        generated = executor.build_action(
            claim,
            capabilities=(item for item in (capability,)),
            resource_locks=(item for item in claim.resource_keys),
            **common,
        )
        concrete = executor.build_action(
            claim,
            capabilities=(capability,),
            resource_locks=claim.resource_keys,
            **common,
        )

        self.assertEqual(generated.capabilities, (capability,))
        self.assertEqual(generated.resource_locks, claim.resource_keys)
        self.assertEqual(generated.action_digest, concrete.action_digest)


if __name__ == "__main__":
    unittest.main()
