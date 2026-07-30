"""Bounded, payload-free operator diagnostics for durable projections."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .models import (
    AttemptRecord,
    AttemptStatus,
    NodeRecord,
    NodeStatus,
    RunRecord,
    RunStatus,
)

_SAFE_CODE = re.compile(r"^[a-z0-9][a-z0-9_.:-]{0,127}$")
_SAFE_ERROR_CLASSES = frozenset({"policy", "recovery"})
_SAFE_REASON_CODES = frozenset(
    {
        "approval_action_mismatch",
        "approval_actor_untrusted",
        "approval_cannot_expand_policy",
        "approval_expired",
        "approval_grant_mismatch",
        "approval_ledger_denied",
        "approval_not_required",
        "approval_policy_version_mismatch",
        "approval_rejected",
        "approval_replayed",
        "approval_required",
        "approval_scope_mismatch",
        "approval_unissued",
        "external_failure_confirmed",
        "external_outcome_unknown",
        "policy_denied",
        "unknown_outcome_pending",
    }
)


@dataclass(frozen=True, slots=True)
class OperatorDiagnostic:
    category: str
    state: str
    reason_code: str
    operator_action_required: bool
    automatic_retry_allowed: bool
    allowed_actions: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "state": self.state,
            "reason_code": self.reason_code,
            "operator_action_required": self.operator_action_required,
            "automatic_retry_allowed": self.automatic_retry_allowed,
            "allowed_actions": list(self.allowed_actions),
        }


def diagnose_run(run: RunRecord) -> OperatorDiagnostic | None:
    if run.status is RunStatus.WAITING_APPROVAL:
        return _approval_pending()
    if run.status is RunStatus.WAITING_RECOVERY:
        return OperatorDiagnostic(
            category="recovery",
            state="waiting_evidence",
            reason_code=_error_code(run.error) or "unknown_outcome_pending",
            operator_action_required=True,
            automatic_retry_allowed=False,
            allowed_actions=("inspect_attempts",),
        )
    return _error_diagnostic(run.error)


def diagnose_node(node: NodeRecord) -> OperatorDiagnostic | None:
    if node.status is NodeStatus.WAITING_APPROVAL:
        return _approval_pending()
    if node.status is NodeStatus.WAITING_RECOVERY:
        return OperatorDiagnostic(
            category="recovery",
            state="waiting_evidence",
            reason_code=_error_code(node.error) or "unknown_outcome_pending",
            operator_action_required=True,
            automatic_retry_allowed=False,
            allowed_actions=("inspect_attempts",),
        )
    return _error_diagnostic(node.error)


def diagnose_attempt(attempt: AttemptRecord) -> OperatorDiagnostic | None:
    if attempt.status is AttemptStatus.WAITING_APPROVAL:
        return _approval_pending()
    if attempt.status is AttemptStatus.OUTCOME_UNKNOWN:
        return OperatorDiagnostic(
            category="recovery",
            state="manual_resolution_required",
            reason_code=_error_code(attempt.error) or "external_outcome_unknown",
            operator_action_required=True,
            automatic_retry_allowed=False,
            allowed_actions=(
                "resolve_recovery.confirmed_succeeded",
                "resolve_recovery.confirmed_failed",
            ),
        )
    return _error_diagnostic(attempt.error)


def summarize_operator_diagnostics(
    run: RunRecord,
    nodes: tuple[NodeRecord, ...] | list[NodeRecord],
    attempts: tuple[AttemptRecord, ...] | list[AttemptRecord],
) -> dict[str, Any]:
    diagnostics = [
        item
        for item in (
            diagnose_run(run),
            *(diagnose_node(node) for node in nodes),
            *(diagnose_attempt(attempt) for attempt in attempts),
        )
        if item is not None
    ]
    category_counts: dict[str, int] = {}
    for diagnostic in diagnostics:
        category_counts[diagnostic.category] = (
            category_counts.get(diagnostic.category, 0) + 1
        )
    return {
        "attention_required": any(
            item.operator_action_required for item in diagnostics
        ),
        "diagnostic_count": len(diagnostics),
        "category_counts": {
            key: category_counts[key] for key in sorted(category_counts)
        },
        "automatic_retry_blocked": any(
            not item.automatic_retry_allowed
            and item.category == "recovery"
            for item in diagnostics
        ),
    }


def _approval_pending() -> OperatorDiagnostic:
    return OperatorDiagnostic(
        category="approval",
        state="pending",
        reason_code="approval_required",
        operator_action_required=True,
        automatic_retry_allowed=False,
        allowed_actions=("approve", "reject"),
    )


def _error_diagnostic(error: Any) -> OperatorDiagnostic | None:
    code = _error_code(error)
    error_class = _error_class(error)
    if code is None:
        if error_class == "policy":
            code = "policy_denied"
        elif error_class == "recovery":
            code = "external_failure_confirmed"
        else:
            return None
    if code == "approval_expired":
        return OperatorDiagnostic(
            category="approval",
            state="expired",
            reason_code=code,
            operator_action_required=True,
            automatic_retry_allowed=False,
            allowed_actions=("request_new_approval",),
        )
    if code.startswith("approval_") or error_class == "policy":
        return OperatorDiagnostic(
            category="approval" if code.startswith("approval_") else "policy",
            state="denied",
            reason_code=code,
            operator_action_required=False,
            automatic_retry_allowed=False,
        )
    if error_class == "recovery":
        return OperatorDiagnostic(
            category="recovery",
            state="resolved_failed",
            reason_code=code,
            operator_action_required=False,
            automatic_retry_allowed=False,
        )
    return OperatorDiagnostic(
        category="execution",
        state="failed",
        reason_code=code,
        operator_action_required=False,
        automatic_retry_allowed=False,
    )


def _error_code(error: Any) -> str | None:
    if not isinstance(error, dict):
        return None
    value = error.get("code") or error.get("error_code")
    if (
        not isinstance(value, str)
        or not _SAFE_CODE.fullmatch(value)
        or value not in _SAFE_REASON_CODES
    ):
        return None
    return value


def _error_class(error: Any) -> str | None:
    if not isinstance(error, dict):
        return None
    value = error.get("class") or error.get("error_class")
    if (
        not isinstance(value, str)
        or not _SAFE_CODE.fullmatch(value)
        or value not in _SAFE_ERROR_CLASSES
    ):
        return None
    return value


__all__ = [
    "OperatorDiagnostic",
    "diagnose_attempt",
    "diagnose_node",
    "diagnose_run",
    "summarize_operator_diagnostics",
]
