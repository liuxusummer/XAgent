from __future__ import annotations

import json
import itertools
import unittest

from src.orchestration.policy import (
    ActionRequest,
    Capability,
    EffectClass,
    PolicyEngine,
    PolicyOutcome,
    PolicyValidationError,
    ToolPolicy,
    ToolTimeoutBehavior,
)
from src.orchestration.policy_conformance import (
    PolicyConformanceCase,
    evaluate_policy_conformance,
)


def _action(
    tool_name: str,
    effect_class: EffectClass,
    capability: Capability,
) -> ActionRequest:
    return ActionRequest.from_args(
        run_id="conformance-run",
        node_id="conformance-node",
        attempt_id=f"case-{tool_name}",
        tool_name=tool_name,
        args={"credential": "conformance-secret"},
        execution_binding_digest="a" * 64,
        operation_key=f"operation-{tool_name}",
        idempotency_key=f"operation-{tool_name}",
        effect_class=effect_class,
        capabilities=(capability,),
        resource_locks=("workspace",),
    )


class PolicyConformanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.read = Capability("workspace.read")
        self.write = Capability("workspace.write")
        self.policy = PolicyEngine(
            (
                ToolPolicy(
                    "read",
                    EffectClass.READ_ONLY,
                    (self.read,),
                    allowed_resource_keys=("workspace",),
                ),
                ToolPolicy(
                    "write",
                    EffectClass.NON_IDEMPOTENT_WRITE,
                    (self.write,),
                    supports_idempotency_key=False,
                    supports_status_probe=False,
                    supports_compensation=False,
                    timeout_behavior=ToolTimeoutBehavior.OUTCOME_UNKNOWN,
                    allowed_resource_keys=("workspace",),
                    required_resource_keys=("workspace",),
                ),
            )
        )

    def test_complete_pack_is_deterministic_and_payload_free(self) -> None:
        cases = (
            PolicyConformanceCase(
                "read-default",
                _action("read", EffectClass.READ_ONLY, self.read),
                PolicyOutcome.ALLOW,
                "known_read_only",
            ),
            PolicyConformanceCase(
                "write-default",
                _action("write", EffectClass.NON_IDEMPOTENT_WRITE, self.write),
                PolicyOutcome.REQUIRE_APPROVAL,
                "write_requires_approval",
            ),
        )

        first = evaluate_policy_conformance(
            self.policy,
            cases,
            required_tools=("read", "write"),
        )
        second = evaluate_policy_conformance(
            self.policy,
            reversed(cases),
            required_tools=("write", "read"),
        )
        serialized = json.dumps(first.to_dict(), sort_keys=True)

        self.assertTrue(first.passed)
        self.assertEqual(first.report_digest, second.report_digest)
        self.assertEqual(first.covered_tools, ("read", "write"))
        self.assertNotIn("conformance-secret", serialized)
        self.assertNotIn("operation-read", serialized)

    def test_drift_and_missing_tool_fail_the_report_without_execution(self) -> None:
        report = evaluate_policy_conformance(
            self.policy,
            (
                PolicyConformanceCase(
                    "drift",
                    _action("read", EffectClass.READ_ONLY, self.read),
                    PolicyOutcome.DENY,
                    "policy_changed",
                ),
            ),
            required_tools=("read", "write"),
        )

        self.assertFalse(report.passed)
        self.assertEqual(report.missing_tools, ("write",))
        self.assertFalse(report.results[0].passed)
        self.assertEqual(report.results[0].actual_reason_code, "known_read_only")

    def test_unbounded_case_iterable_is_rejected_without_exhaustion(self) -> None:
        case = PolicyConformanceCase(
            "read-default",
            _action("read", EffectClass.READ_ONLY, self.read),
            PolicyOutcome.ALLOW,
            "known_read_only",
        )

        with self.assertRaisesRegex(PolicyValidationError, "bounded"):
            evaluate_policy_conformance(
                self.policy,
                itertools.repeat(case),
            )


if __name__ == "__main__":
    unittest.main()
