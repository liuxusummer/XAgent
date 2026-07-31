"""Canonical durable and public orchestration Event registries."""

from __future__ import annotations

DURABLE_EVENT_TYPES = frozenset(
    {
        "run.created",
        "run.started",
        "run.pause_requested",
        "run.pausing",
        "run.paused",
        "run.resumed",
        "run.waiting_input",
        "run.waiting_approval",
        "run.waiting_recovery",
        "run.recovery_resolved",
        "run.cancel_requested",
        "run.cancelling",
        "run.completed",
        "run.failed",
        "run.cancelled",
        "node.created",
        "node.pending",
        "node.ready",
        "node.started",
        "node.waiting_input",
        "node.waiting_approval",
        "node.waiting_retry",
        "node.waiting_recovery",
        "node.paused",
        "node.succeeded",
        "node.failed",
        "node.cancelled",
        "node.skipped",
        "attempt.scheduled",
        "attempt.claimed",
        "attempt.claim_taken_over",
        "attempt.started",
        "attempt.running",
        "attempt.succeeded",
        "attempt.failed",
        "attempt.timed_out",
        "attempt.cancelled",
        "attempt.abandoned",
        "attempt.outcome_unknown",
        "agent_tool.scheduled",
        "agent_tool.started",
        "agent_tool.succeeded",
        "agent_tool.failed",
        "agent_tool.timed_out",
        "agent_tool.cancelled",
        "agent_tool.abandoned",
        "agent_tool.outcome_unknown",
        "activity.commit_rejected",
        "tool.receipt_recorded",
        "artifact.created",
        "approval.requested",
        "approval.resolved",
        "policy.decided",
        "input.requested",
        "input.received",
        "lease.acquired",
        "lease.expired",
        "lease.released",
        "audit.note",
    }
)

# Policy decisions may contain control-plane detail and remain intentionally
# masked at the read-only Web boundary.
PUBLIC_EVENT_TYPES = DURABLE_EVENT_TYPES - {"policy.decided"}

__all__ = ["DURABLE_EVENT_TYPES", "PUBLIC_EVENT_TYPES"]
