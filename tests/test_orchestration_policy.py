from __future__ import annotations

import hashlib
import json
import threading
import unittest
from dataclasses import replace

from src.orchestration.policy import (
    ActionRequest,
    ApprovalGrant,
    Capability,
    EffectClass,
    InMemoryApprovalLedger,
    PolicyEngine,
    PolicyOutcome,
    PolicyRule,
    PolicyValidationError,
    ToolTimeoutBehavior,
    ToolPolicy,
)


READ = Capability("workspace.read")
WRITE = Capability("workspace.write")
DELETE = Capability("workspace.delete")


def action(
    tool_name: str,
    effect: EffectClass,
    capabilities: tuple[Capability, ...],
    *,
    run_id: str = "run-1",
    node_id: str = "node-1",
    attempt_id: str = "attempt-1",
    args: dict | None = None,
) -> ActionRequest:
    return ActionRequest.from_args(
        run_id=run_id,
        node_id=node_id,
        attempt_id=attempt_id,
        tool_name=tool_name,
        args={} if args is None else args,
        execution_binding_digest="e" * 64,
        operation_key="operation-1",
        idempotency_key="operation-1",
        effect_class=effect,
        capabilities=capabilities,
        resource_locks=("workspace:/project",),
    )


def engine(
    rules: tuple[PolicyRule, ...] = (),
    *,
    ledger: InMemoryApprovalLedger | None = None,
) -> PolicyEngine:
    return PolicyEngine(
        (
            ToolPolicy(
                "file_read",
                EffectClass.READ_ONLY,
                (READ,),
                allowed_resource_keys=("workspace:/project",),
            ),
            ToolPolicy(
                "file_write",
                EffectClass.IDEMPOTENT_WRITE,
                (WRITE,),
                supports_idempotency_key=True,
                supports_status_probe=True,
                supports_compensation=False,
                timeout_behavior=ToolTimeoutBehavior.PROBE_BEFORE_RETRY,
                allowed_resource_keys=("workspace:/project",),
                required_resource_keys=("workspace:/project",),
            ),
            ToolPolicy(
                "delete_tree",
                EffectClass.DESTRUCTIVE,
                (DELETE,),
                supports_idempotency_key=False,
                supports_status_probe=False,
                supports_compensation=False,
                timeout_behavior=ToolTimeoutBehavior.OUTCOME_UNKNOWN,
                allowed_resource_keys=("workspace:/project",),
                required_resource_keys=("workspace:/project",),
            ),
        ),
        rules,
        approval_actors=("reviewer",),
        ledger=ledger,
    )


def grant_for(
    policy: PolicyEngine,
    request: ActionRequest,
    *,
    approval_id: str = "approval-1",
    run_id: str | None = None,
    node_id: str | None = None,
    actor: str = "reviewer",
    expires_at: float = 200.0,
    policy_version: str | None = None,
) -> ApprovalGrant:
    return ApprovalGrant(
        approval_id=approval_id,
        action_digest=request.action_digest,
        run_id=request.run_id if run_id is None else run_id,
        node_id=request.node_id if node_id is None else node_id,
        policy_version=policy.policy_version if policy_version is None else policy_version,
        actor=actor,
        expires_at=expires_at,
    )


class PolicyEngineTests(unittest.TestCase):
    def test_known_read_only_action_is_allowed(self) -> None:
        request = action("file_read", EffectClass.READ_ONLY, (READ,))

        decision = engine().evaluate(request)

        self.assertEqual(decision.outcome, PolicyOutcome.ALLOW)
        self.assertEqual(decision.reason_code, "known_read_only")

    def test_unknown_tool_and_capability_fail_closed(self) -> None:
        unknown_tool = action("unregistered", EffectClass.READ_ONLY, (READ,))
        unknown_capability = action(
            "file_read",
            EffectClass.READ_ONLY,
            (Capability("host.root"),),
        )

        self.assertEqual(engine().evaluate(unknown_tool).reason_code, "unknown_tool")
        decision = engine().evaluate(unknown_capability)
        self.assertEqual(decision.outcome, PolicyOutcome.DENY)
        self.assertEqual(decision.reason_code, "unknown_capability")

    def test_effect_or_capability_underdeclaration_is_denied(self) -> None:
        wrong_effect = action("file_write", EffectClass.READ_ONLY, (WRITE,))
        missing_capability = action("file_write", EffectClass.IDEMPOTENT_WRITE, ())

        self.assertEqual(engine().evaluate(wrong_effect).reason_code, "effect_class_mismatch")
        self.assertEqual(
            engine().evaluate(missing_capability).reason_code,
            "capability_mismatch",
        )

    def test_write_tool_contracts_are_explicit_and_fail_closed(self) -> None:
        with self.assertRaisesRegex(
            PolicyValidationError,
            "must explicitly declare",
        ):
            ToolPolicy("implicit-write", EffectClass.IDEMPOTENT_WRITE)

        request = action(
            "bad-idempotent",
            EffectClass.IDEMPOTENT_WRITE,
            (WRITE,),
        )
        policy = PolicyEngine(
            (
                ToolPolicy(
                    "bad-idempotent",
                    EffectClass.IDEMPOTENT_WRITE,
                    (WRITE,),
                    supports_idempotency_key=False,
                    supports_status_probe=True,
                    supports_compensation=False,
                    timeout_behavior=ToolTimeoutBehavior.PROBE_BEFORE_RETRY,
                    allowed_resource_keys=("workspace:/project",),
                    required_resource_keys=("workspace:/project",),
                ),
            )
        )

        decision = policy.evaluate(request)

        self.assertEqual(decision.outcome, PolicyOutcome.DENY)
        self.assertEqual(decision.reason_code, "idempotency_key_unsupported")

    def test_write_resource_and_timeout_contracts_fail_closed(self) -> None:
        request = action(
            "unsafe-write",
            EffectClass.NON_IDEMPOTENT_WRITE,
            (WRITE,),
        )
        policy = PolicyEngine(
            (
                ToolPolicy(
                    "unsafe-write",
                    EffectClass.NON_IDEMPOTENT_WRITE,
                    (WRITE,),
                    supports_idempotency_key=False,
                    supports_status_probe=False,
                    supports_compensation=False,
                    timeout_behavior=ToolTimeoutBehavior.SAFE_TO_RETRY,
                    allowed_resource_keys=("workspace:/project",),
                    required_resource_keys=("workspace:/project",),
                ),
            )
        )
        self.assertEqual(
            policy.evaluate(request).reason_code,
            "write_timeout_contract_unsafe",
        )

        missing_lock = replace(request, resource_locks=())
        self.assertEqual(
            policy.evaluate(missing_lock).reason_code,
            "required_resource_lock_missing",
        )

    def test_operation_keys_are_digest_bound_without_raw_persistence(self) -> None:
        operation_key = "stable-operation-key-never-persist-raw"
        request = ActionRequest.from_args(
            run_id="run",
            node_id="node",
            attempt_id="attempt",
            tool_name="file_read",
            args={},
            execution_binding_digest="e" * 64,
            operation_key=operation_key,
            idempotency_key=operation_key,
            effect_class=EffectClass.READ_ONLY,
            capabilities=(READ,),
            resource_locks=("workspace:/project",),
        )

        serialized = json.dumps(request.to_dict(), sort_keys=True)
        self.assertNotIn(operation_key, serialized)
        self.assertNotIn(operation_key, repr(request))
        self.assertEqual(
            request.operation_key_digest,
            request.idempotency_key_digest,
        )

    def test_most_specific_rule_wins_and_deny_wins_a_tie(self) -> None:
        request = action("file_read", EffectClass.READ_ONLY, (READ,))
        broad_deny = PolicyRule("broad-deny", PolicyOutcome.DENY)
        exact_allow = PolicyRule(
            "exact-allow",
            PolicyOutcome.ALLOW,
            tool_name="file_read",
        )
        exact_deny = PolicyRule(
            "exact-deny",
            PolicyOutcome.DENY,
            tool_name="file_read",
        )

        specific = engine((broad_deny, exact_allow)).evaluate(request)
        tied = engine((exact_allow, exact_deny)).evaluate(request)

        self.assertEqual(specific.outcome, PolicyOutcome.ALLOW)
        self.assertEqual(specific.matched_rule_ids, ("exact-allow",))
        self.assertEqual(tied.outcome, PolicyOutcome.DENY)
        self.assertEqual(tied.matched_rule_ids, ("exact-allow", "exact-deny"))

    def test_capability_constraint_increases_rule_specificity(self) -> None:
        request = action("file_write", EffectClass.IDEMPOTENT_WRITE, (WRITE,))
        broad_allow = PolicyRule(
            "write-allow",
            PolicyOutcome.ALLOW,
            tool_name="file_write",
        )
        capability_deny = PolicyRule(
            "write-capability-deny",
            PolicyOutcome.DENY,
            tool_name="file_write",
            required_capabilities=(WRITE,),
        )

        decision = engine((broad_allow, capability_deny)).evaluate(request)

        self.assertEqual(decision.outcome, PolicyOutcome.DENY)
        self.assertEqual(decision.matched_rule_ids, ("write-capability-deny",))

    def test_destructive_action_always_requires_approval(self) -> None:
        request = action("delete_tree", EffectClass.DESTRUCTIVE, (DELETE,))
        explicit_allow = PolicyRule(
            "allow-delete",
            PolicyOutcome.ALLOW,
            tool_name="delete_tree",
        )

        default_decision = engine().evaluate(request)
        explicit_decision = engine((explicit_allow,)).evaluate(request)

        self.assertEqual(default_decision.outcome, PolicyOutcome.REQUIRE_APPROVAL)
        self.assertEqual(explicit_decision.outcome, PolicyOutcome.REQUIRE_APPROVAL)
        self.assertEqual(explicit_decision.reason_code, "destructive_requires_approval")

    def test_valid_approval_is_single_use(self) -> None:
        ledger = InMemoryApprovalLedger()
        policy = engine(ledger=ledger)
        request = action("file_write", EffectClass.IDEMPOTENT_WRITE, (WRITE,))
        grant = grant_for(policy, request)
        self.assertTrue(ledger.register_issued(grant))

        first = policy.consume_approval(request, grant, now=100)
        second = policy.consume_approval(request, grant, now=100)

        self.assertEqual(first.outcome, PolicyOutcome.ALLOW)
        self.assertEqual(first.approval_id, grant.approval_id)
        self.assertEqual(second.outcome, PolicyOutcome.DENY)
        self.assertEqual(second.reason_code, "approval_replayed")

    def test_unissued_grant_is_rejected(self) -> None:
        ledger = InMemoryApprovalLedger()
        policy = engine(ledger=ledger)
        request = action("file_write", EffectClass.IDEMPOTENT_WRITE, (WRITE,))
        self_issued = grant_for(policy, request)

        decision = policy.consume_approval(request, self_issued, now=100)

        self.assertEqual(decision.outcome, PolicyOutcome.DENY)
        self.assertEqual(decision.reason_code, "approval_unissued")
        self.assertFalse(ledger.was_issued(self_issued.approval_id))
        self.assertFalse(ledger.was_consumed(self_issued.approval_id))

    def test_tampered_same_id_does_not_consume_authentic_grant(self) -> None:
        ledger = InMemoryApprovalLedger()
        policy = engine(ledger=ledger)
        request = action("file_write", EffectClass.IDEMPOTENT_WRITE, (WRITE,))
        authentic = grant_for(policy, request, expires_at=200)
        tampered = grant_for(policy, request, expires_at=300)
        self.assertTrue(ledger.register_issued(authentic))
        self.assertFalse(ledger.register_issued(tampered))

        rejected = policy.consume_approval(request, tampered, now=100)
        accepted = policy.consume_approval(request, authentic, now=100)

        self.assertEqual(rejected.outcome, PolicyOutcome.DENY)
        self.assertEqual(rejected.reason_code, "approval_grant_mismatch")
        self.assertEqual(accepted.outcome, PolicyOutcome.ALLOW)

    def test_approval_wrong_scope_expired_actor_and_action_fail_closed(self) -> None:
        policy = engine()
        request = action("file_write", EffectClass.IDEMPOTENT_WRITE, (WRITE,))
        another = action(
            "file_write",
            EffectClass.IDEMPOTENT_WRITE,
            (WRITE,),
            attempt_id="attempt-2",
        )

        cases = (
            (
                grant_for(policy, request, approval_id="wrong-run", run_id="run-2"),
                "approval_scope_mismatch",
            ),
            (
                grant_for(policy, request, approval_id="wrong-node", node_id="node-2"),
                "approval_scope_mismatch",
            ),
            (
                grant_for(policy, request, approval_id="expired", expires_at=100),
                "approval_expired",
            ),
            (
                grant_for(policy, request, approval_id="actor", actor="untrusted"),
                "approval_actor_untrusted",
            ),
            (
                grant_for(policy, another, approval_id="action"),
                "approval_action_mismatch",
            ),
        )
        for grant, reason in cases:
            with self.subTest(reason=reason):
                decision = policy.consume_approval(request, grant, now=100)
                self.assertEqual(decision.outcome, PolicyOutcome.DENY)
                self.assertEqual(decision.reason_code, reason)

    def test_approval_cannot_expand_a_deny_rule(self) -> None:
        deny = PolicyRule(
            "deny-write",
            PolicyOutcome.DENY,
            tool_name="file_write",
        )
        ledger = InMemoryApprovalLedger()
        policy = engine((deny,), ledger=ledger)
        request = action("file_write", EffectClass.IDEMPOTENT_WRITE, (WRITE,))
        grant = grant_for(policy, request)

        decision = policy.consume_approval(request, grant, now=100)

        self.assertEqual(decision.outcome, PolicyOutcome.DENY)
        self.assertEqual(decision.reason_code, "approval_cannot_expand_policy")
        self.assertFalse(ledger.was_consumed(grant.approval_id))

    def test_policy_version_is_canonical_and_drift_invalidates_grant(self) -> None:
        first = engine()
        equivalent = engine()
        changed = engine(
            (
                PolicyRule(
                    "write-approval",
                    PolicyOutcome.REQUIRE_APPROVAL,
                    tool_name="file_write",
                ),
            )
        )
        request = action("file_write", EffectClass.IDEMPOTENT_WRITE, (WRITE,))
        old_grant = grant_for(first, request)

        self.assertEqual(first.policy_digest, equivalent.policy_digest)
        self.assertNotEqual(first.policy_digest, changed.policy_digest)
        decision = changed.consume_approval(request, old_grant, now=100)
        self.assertEqual(decision.outcome, PolicyOutcome.DENY)
        self.assertEqual(decision.reason_code, "approval_policy_version_mismatch")

    def test_concurrent_consumption_allows_exactly_one(self) -> None:
        ledger = InMemoryApprovalLedger()
        policy = engine(ledger=ledger)
        request = action("file_write", EffectClass.IDEMPOTENT_WRITE, (WRITE,))
        grant = grant_for(policy, request)
        ledger.register_issued(grant)
        barrier = threading.Barrier(20)
        outcomes: list[PolicyOutcome] = []
        result_lock = threading.Lock()

        def consume() -> None:
            barrier.wait()
            decision = policy.consume_approval(request, grant, now=100)
            with result_lock:
                outcomes.append(decision.outcome)

        threads = [threading.Thread(target=consume) for _ in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(outcomes.count(PolicyOutcome.ALLOW), 1)
        self.assertEqual(outcomes.count(PolicyOutcome.DENY), 19)

    def test_args_digest_is_canonical_and_raw_secrets_are_not_retained(self) -> None:
        secret = "super-secret-value-that-must-not-leak"
        left = action(
            "file_write",
            EffectClass.IDEMPOTENT_WRITE,
            (WRITE,),
            args={
                "path": "a.txt",
                "options": {"mode": "atomic", "api_key": secret},
            },
        )
        right = action(
            "file_write",
            EffectClass.IDEMPOTENT_WRITE,
            (WRITE,),
            args={
                "options": {"api_key": secret, "mode": "atomic"},
                "path": "a.txt",
            },
        )

        self.assertEqual(left.args_digest, right.args_digest)
        self.assertEqual(left.action_digest, right.action_digest)
        self.assertNotIn(secret, repr(left))
        self.assertNotIn(secret, json.dumps(left.to_dict()))
        self.assertNotIn(secret, left.args_digest)

        different_secret = action(
            "file_write",
            EffectClass.IDEMPOTENT_WRITE,
            (WRITE,),
            args={
                "path": "a.txt",
                "options": {
                    "mode": "atomic",
                    "api_key": "a-completely-different-secret",
                },
            },
        )
        self.assertEqual(left.args_digest, different_secret.args_digest)
        self.assertEqual(left.action_digest, different_secret.action_digest)

    def test_sensitive_key_canonicalization_and_numeric_scalar_redaction(self) -> None:
        attacks = (
            ("accessToken", "access-token-canary"),
            ("ＰＡＳＳＷＯＲＤ", "fullwidth-password-canary"),
            ("api.key", "api-key-canary"),
            ("zero\u200bwidth_token", "zero-width-token-canary"),
            ("pin", 1234),
        )
        for key, secret in attacks:
            with self.subTest(key=key):
                sensitive_keys = ("pin",) if key == "pin" else ()
                left = ActionRequest.from_args(
                    run_id="run",
                    node_id="node",
                    attempt_id="attempt",
                    tool_name="file_write",
                    args={key: secret, "path": "a"},
                    execution_binding_digest="e" * 64,
                    operation_key="operation-1",
                    idempotency_key="operation-1",
                    effect_class=EffectClass.IDEMPOTENT_WRITE,
                    sensitive_keys=sensitive_keys,
                )
                right = ActionRequest.from_args(
                    run_id="run",
                    node_id="node",
                    attempt_id="attempt",
                    tool_name="file_write",
                    args={key: "different", "path": "a"},
                    execution_binding_digest="e" * 64,
                    operation_key="operation-1",
                    idempotency_key="operation-1",
                    effect_class=EffectClass.IDEMPOTENT_WRITE,
                    sensitive_keys=sensitive_keys,
                )

                self.assertEqual(left.args_digest, right.args_digest)
                serialized = json.dumps(left.to_dict(), ensure_ascii=False)
                self.assertNotIn(str(secret), serialized)
                self.assertNotIn(
                    hashlib.sha256(str(secret).encode("utf-8")).hexdigest(),
                    serialized,
                )

    def test_custom_sensitive_key_is_redacted_without_changing_order_stability(self) -> None:
        secret = "private-value"
        request = ActionRequest.from_args(
            run_id="run",
            node_id="node",
            attempt_id="attempt",
            tool_name="file_write",
            args={"custom_auth": secret, "path": "a"},
            execution_binding_digest="e" * 64,
            operation_key="operation-1",
            idempotency_key="operation-1",
            effect_class=EffectClass.IDEMPOTENT_WRITE,
            capabilities=(WRITE,),
            resource_locks=("workspace:/project",),
            sensitive_keys=("custom_auth",),
        )

        self.assertNotIn(secret, repr(request))
        self.assertNotIn(secret, json.dumps(request.to_dict()))

    def test_unbounded_or_non_json_args_are_rejected_without_echoing_values(self) -> None:
        secret = "do-not-echo-this"
        with self.assertRaises(PolicyValidationError) as raised:
            ActionRequest.from_args(
                run_id="run",
                node_id="node",
                attempt_id="attempt",
                tool_name="file_write",
                args={"password": secret, "bad": object()},
                execution_binding_digest="e" * 64,
                operation_key="operation-1",
                idempotency_key="operation-1",
                effect_class=EffectClass.IDEMPOTENT_WRITE,
            )
        self.assertNotIn(secret, str(raised.exception))


if __name__ == "__main__":
    unittest.main()
