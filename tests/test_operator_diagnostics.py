from __future__ import annotations

import json
import unittest

from src.orchestration.models import (
    AttemptRecord,
    AttemptStatus,
    NodeRecord,
    NodeStatus,
    RunRecord,
    RunStatus,
)
from src.orchestration.operator_diagnostics import (
    diagnose_attempt,
    summarize_operator_diagnostics,
)


class OperatorDiagnosticsTests(unittest.TestCase):
    def test_unknown_outcome_is_visible_without_leaking_error_payload(self) -> None:
        secret = "diagnostic-backend-secret"
        attempt = AttemptRecord(
            "attempt",
            "run",
            "node",
            1,
            status=AttemptStatus.OUTCOME_UNKNOWN,
            error={
                "error_class": "timeout",
                "error_code": "external_outcome_unknown",
                "backend_detail": secret,
            },
            scheduled_at=1,
            finished_at=2,
        )

        diagnostic = diagnose_attempt(attempt)
        serialized = json.dumps(diagnostic.to_dict(), sort_keys=True)

        self.assertEqual(diagnostic.category, "recovery")
        self.assertEqual(diagnostic.state, "manual_resolution_required")
        self.assertFalse(diagnostic.automatic_retry_allowed)
        self.assertEqual(
            diagnostic.allowed_actions,
            (
                "resolve_recovery.confirmed_succeeded",
                "resolve_recovery.confirmed_failed",
            ),
        )
        self.assertNotIn(secret, serialized)
        self.assertNotIn("backend_detail", serialized)

    def test_approval_expiry_and_untrusted_codes_are_bounded(self) -> None:
        expired = AttemptRecord(
            "expired",
            "run",
            "node",
            1,
            status=AttemptStatus.FAILED,
            error={
                "class": "policy",
                "code": "approval_expired",
                "credential": "approval-secret",
            },
            scheduled_at=1,
            finished_at=2,
        )
        malformed = AttemptRecord(
            "malformed",
            "run",
            "node",
            2,
            status=AttemptStatus.FAILED,
            error={"class": "policy", "code": "secret\ninjected"},
            scheduled_at=1,
            finished_at=2,
        )
        canary = AttemptRecord(
            "canary",
            "run",
            "node",
            3,
            status=AttemptStatus.FAILED,
            error={
                "class": "policy",
                "code": "credential_canary_123",
            },
            scheduled_at=1,
            finished_at=2,
        )

        diagnostic = diagnose_attempt(expired)

        self.assertEqual(diagnostic.state, "expired")
        self.assertEqual(
            diagnostic.allowed_actions,
            ("request_new_approval",),
        )
        self.assertEqual(
            diagnose_attempt(malformed).reason_code,
            "policy_denied",
        )
        self.assertEqual(
            diagnose_attempt(canary).reason_code,
            "policy_denied",
        )

    def test_run_summary_counts_operator_attention_without_payloads(self) -> None:
        run = RunRecord(
            "run",
            "workflow",
            definition_digest="a" * 64,
            status=RunStatus.WAITING_RECOVERY,
        )
        node = NodeRecord(
            "run",
            "node",
            "tool",
            status=NodeStatus.WAITING_RECOVERY,
        )
        attempt = AttemptRecord(
            "attempt",
            "run",
            "node",
            1,
            status=AttemptStatus.OUTCOME_UNKNOWN,
            scheduled_at=1,
            finished_at=2,
        )

        summary = summarize_operator_diagnostics(run, [node], [attempt])

        self.assertTrue(summary["attention_required"])
        self.assertTrue(summary["automatic_retry_blocked"])
        self.assertEqual(summary["category_counts"], {"recovery": 3})


if __name__ == "__main__":
    unittest.main()
