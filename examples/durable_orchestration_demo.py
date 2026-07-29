#!/usr/bin/env python3
"""Deterministic, credential-free acceptance demo for durable orchestration."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from src.orchestration.artifacts import (  # noqa: E402
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    LocalArtifactStore,
)
from src.orchestration.evaluation import (  # noqa: E402
    ReliabilityEvidence,
    evaluate_reliability,
)
from src.orchestration.executor import (  # noqa: E402
    ActivityExecutionConflict,
    DurableApprovalRegistry,
    TrustedActivityExecutor,
)
from src.orchestration.lease import (  # noqa: E402
    DurableLeaseReaper,
    ProbeOutcome,
    ProbeResult,
)
from src.orchestration.models import (  # noqa: E402
    AttemptRecord,
    AttemptStatus,
    NodeStatus,
    RunStatus,
)
from src.orchestration.policy import (  # noqa: E402
    ApprovalGrant,
    EffectClass,
    PolicyEngine,
    PolicyOutcome,
    PolicyRule,
    ToolTimeoutBehavior,
    ToolPolicy,
)
from src.orchestration.replay import build_replay_report  # noqa: E402
from src.orchestration.sandbox import (  # noqa: E402
    BackendExecutionResult,
    ExecutionRequest,
    SandboxDispatcher,
    SandboxProfile,
    SecurityLevel,
)
from src.orchestration.scheduler import (  # noqa: E402
    ActivityClaim,
    DurableScheduler,
    RunInputReceipt,
)
from src.orchestration.store import DurableRunStore  # noqa: E402
from src.orchestration.workflow import compile_workflow  # noqa: E402

WORKFLOW_PATH = (
    Path(__file__).resolve().parent
    / "workflows"
    / "durable_orchestration_demo.json"
)
RUN_ID = "durable-demo-run"
DEMO_SECRET_MARKER = "demo-private-marker-never-print"
_APPROVAL_ACTOR = "local-demo-operator"


class _LogicalClock:
    """A fixed logical clock keeps the acceptance result reproducible."""

    def __call__(self) -> float:
        return 100.0


class _DeterministicIds:
    def __init__(self, namespace: str = "demo") -> None:
        self._counts: dict[str, int] = {}
        self._namespace = namespace

    def __call__(self, prefix: str) -> str:
        count = self._counts.get(prefix, 0) + 1
        self._counts[prefix] = count
        return f"{self._namespace}-{prefix}-{count:04d}"


class _VisibleEffectLedger:
    """A tiny external-system stand-in with an operation-key uniqueness contract."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS published_effects (
                    operation_key TEXT PRIMARY KEY,
                    execution_binding_digest TEXT NOT NULL,
                    result_ref_json TEXT NOT NULL
                )
                """
            )

    def publish(
        self,
        operation_key: str,
        execution_binding_digest: str,
        result_ref: ArtifactRef,
    ) -> ArtifactRef:
        serialized_ref = json.dumps(
            result_ref.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT OR IGNORE INTO published_effects (
                    operation_key, execution_binding_digest, result_ref_json
                ) VALUES (?, ?, ?)
                """,
                (operation_key, execution_binding_digest, serialized_ref),
            )
            row = connection.execute(
                """
                SELECT execution_binding_digest, result_ref_json
                FROM published_effects
                WHERE operation_key = ?
                """,
                (operation_key,),
            ).fetchone()
            if row is None or row[0] != execution_binding_digest:
                raise RuntimeError("operation key was reused for a different execution intent")
            return ArtifactRef.from_dict(json.loads(row[1]))

    def probe(self, operation_key: str) -> ArtifactRef | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT result_ref_json
                FROM published_effects
                WHERE operation_key = ?
                """,
                (operation_key,),
            ).fetchone()
        return None if row is None else ArtifactRef.from_dict(json.loads(row[0]))

    def count(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) FROM published_effects"
            ).fetchone()
        return int(row[0])

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, isolation_level=None)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection


class _ClosedFunctionBackend:
    """Closed local functions used only to exercise the trusted execution gate.

    The adapter accepts no shell command, environment variable, network target,
    or user-provided executable.  It is intentionally small and deterministic;
    production deployments must replace it with their real isolation backend.
    """

    backend_id = "demo-closed-functions"
    security_level = SecurityLevel.TRUSTED_FUNCTION
    capabilities = ()
    supports_materialized_script = True

    def __init__(
        self,
        artifacts: LocalArtifactStore,
        visible_effects: _VisibleEffectLedger,
    ) -> None:
        self.artifacts = artifacts
        self.visible_effects = visible_effects
        self.calls = 0

    def execute(
        self,
        request: ExecutionRequest,
        profile: SandboxProfile,
    ) -> BackendExecutionResult:
        del profile
        self.calls += 1
        tool_name = request.action.tool_name
        expected_script = f"xagent-closed-function:{tool_name}:v1".encode("utf-8")
        if (
            request.script_artifact_ref is None
            or request.materialized_script != expected_script
        ):
            raise ValueError(
                "the demo backend requires the exact verified function Artifact"
            )
        if tool_name == "analyze_fragment":
            output = self._analyze(request)
        elif tool_name == "publish_report":
            output = self._publish(request)
        else:
            raise ValueError("the demo backend received an unknown closed function")
        candidate_ref = _logical_ref(self.artifacts.put_json(
            output,
            kind=ArtifactKind.TOOL_RESULT,
            producer_run_id=request.action.run_id,
            producer_node_id=request.action.node_id,
            producer_attempt_id=request.action.attempt_id,
        ))
        ref = (
            self.visible_effects.publish(
                request.operation_key,
                request.action.execution_binding_digest,
                candidate_ref,
            )
            if tool_name == "publish_report"
            else candidate_ref
        )
        return BackendExecutionResult(
            exit_code=0,
            output_artifact_refs=(ref,),
        )

    def probe(self, attempt: AttemptRecord) -> ProbeResult:
        operation_key = attempt.metadata.get("operation_key")
        if not isinstance(operation_key, str):
            return ProbeResult(ProbeOutcome.UNKNOWN)
        ref = self.visible_effects.probe(operation_key)
        if ref is None:
            return ProbeResult(ProbeOutcome.NOT_COMMITTED)
        return ProbeResult(
            ProbeOutcome.COMMITTED,
            (ref,),
            external_operation_id_digest=hashlib.sha256(
                operation_key.encode("utf-8")
            ).hexdigest(),
        )

    def _analyze(self, request: ExecutionRequest) -> dict[str, Any]:
        if len(request.input_artifact_refs) != 1:
            raise ValueError("analysis requires exactly one Artifact input")
        raw = json.loads(
            self.artifacts.read(request.input_artifact_refs[0]).decode("utf-8")
        )
        shard = str(request.action.node_id).removeprefix("analyze_")
        values = raw["shards"][shard]
        return {
            "item_count": len(values),
            "shard": shard,
            "subtotal": sum(int(value) for value in values),
        }

    def _publish(self, request: ExecutionRequest) -> dict[str, Any]:
        if len(request.input_artifact_refs) != 2:
            raise ValueError("publication requires both branch Artifacts")
        fragments = [
            json.loads(self.artifacts.read(ref).decode("utf-8"))
            for ref in request.input_artifact_refs
        ]
        report = {
            "item_count": sum(fragment["item_count"] for fragment in fragments),
            "status": "published",
            "total": sum(fragment["subtotal"] for fragment in fragments),
        }
        return report


def _workflow() -> Any:
    return compile_workflow(json.loads(WORKFLOW_PATH.read_text(encoding="utf-8")))


def _logical_ref(ref: ArtifactRef) -> ArtifactRef:
    """Replace filesystem mtime with the scenario's deterministic logical time."""

    return replace(ref, created_at=100.0)


def _policy() -> PolicyEngine:
    return PolicyEngine(
        (
            ToolPolicy(
                "analyze_fragment",
                EffectClass.READ_ONLY,
                requires_script_artifact=True,
            ),
            ToolPolicy(
                "publish_report",
                EffectClass.IDEMPOTENT_WRITE,
                supports_idempotency_key=True,
                supports_status_probe=True,
                supports_compensation=False,
                requires_script_artifact=True,
                timeout_behavior=ToolTimeoutBehavior.PROBE_BEFORE_RETRY,
                allowed_resource_keys=("ledger:demo-publication",),
                required_resource_keys=("ledger:demo-publication",),
            ),
        ),
        (
            PolicyRule(
                "publish-needs-human-approval",
                PolicyOutcome.REQUIRE_APPROVAL,
                tool_name="publish_report",
                effect_classes=(EffectClass.IDEMPOTENT_WRITE,),
                reason_code="publish_requires_approval",
            ),
        ),
    )


def _runtime(
    control_plane_root: Path,
    agent_workspace_root: Path,
    artifacts: LocalArtifactStore,
    visible_effects: _VisibleEffectLedger,
    *,
    ids: _DeterministicIds | None = None,
    fault_hook: Any | None = None,
) -> tuple[
    DurableRunStore,
    DurableScheduler,
    PolicyEngine,
    _ClosedFunctionBackend,
    SandboxProfile,
    TrustedActivityExecutor,
]:
    clock = _LogicalClock()
    store = DurableRunStore(control_plane_root / "durable-demo.sqlite3")
    scheduler = DurableScheduler(
        store,
        _workflow(),
        clock=clock,
        id_factory=ids or _DeterministicIds("restarted"),
        artifact_verifier=artifacts.verify,
    )
    policy = _policy()
    backend = _ClosedFunctionBackend(artifacts, visible_effects)
    dispatcher = SandboxDispatcher(
        (backend,),
        policy_version=policy.policy_version,
    )
    profile = SandboxProfile(
        "demo-local-profile",
        (agent_workspace_root,),
        (),
        minimum_security_level=SecurityLevel.TRUSTED_FUNCTION,
    )
    executor = TrustedActivityExecutor(
        scheduler,
        policy,
        dispatcher,
        artifact_verifier=artifacts.verify,
        artifact_reader=artifacts.read,
        clock=clock,
        fault_hook=fault_hook,
    )
    return store, scheduler, policy, backend, profile, executor


def _execute(
    executor: TrustedActivityExecutor,
    claim: ActivityClaim,
    profile: SandboxProfile,
    *,
    script_ref: ArtifactRef,
    grant: ApprovalGrant | None = None,
) -> Any:
    return executor.execute(
        claim,
        argv=("durable-demo-closed-function", claim.node_id),
        cwd=profile.allowed_roots[0],
        profile=profile,
        approval_grant=grant,
        input_artifact_refs=claim.input_artifact_refs,
        script_artifact_ref=script_ref,
        resource_locks=claim.resource_keys,
    )

def _known_metrics(report: dict[str, Any]) -> bool:
    return bool(report) and all(
        isinstance(metric, dict) and metric.get("status") == "known"
        for metric in report.values()
    )


def run_demo(runtime_dir: str | os.PathLike[str]) -> dict[str, Any]:
    """Run the complete local scenario and return only a sanitized summary."""

    root = Path(runtime_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    control_plane_root = (root / "control-plane").resolve()
    agent_workspace_root = (root / "agent-workspace").resolve()
    if (
        control_plane_root.parent != root
        or agent_workspace_root.parent != root
    ):
        raise ValueError(
            "runtime layout roots must remain direct children of runtime_dir"
        )
    control_plane_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    agent_workspace_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if (
        control_plane_root == agent_workspace_root
        or control_plane_root.is_relative_to(agent_workspace_root)
        or agent_workspace_root.is_relative_to(control_plane_root)
    ):
        raise ValueError(
            "control-plane and agent-workspace roots must not overlap"
        )
    artifacts = LocalArtifactStore(control_plane_root / "artifacts")
    visible_effects = _VisibleEffectLedger(
        control_plane_root / "visible-effects.sqlite3"
    )
    crash_armed = False
    crash_observed = False

    def crash_after_external_write(stage: str) -> None:
        nonlocal crash_observed
        if crash_armed and stage == "backend.returned" and not crash_observed:
            crash_observed = True
            raise RuntimeError("simulated crash after external write before Tx D")

    store, scheduler, policy, backend, profile, executor = _runtime(
        control_plane_root,
        agent_workspace_root,
        artifacts,
        visible_effects,
        ids=_DeterministicIds(),
        fault_hook=crash_after_external_write,
    )
    script_refs = {
        tool_name: _logical_ref(
            artifacts.put_bytes(
                f"xagent-closed-function:{tool_name}:v1".encode("utf-8"),
                producer_run_id=RUN_ID,
            )
        )
        for tool_name in ("analyze_fragment", "publish_report")
    }

    input_ref = _logical_ref(
        artifacts.put_json(
            {
                "private_marker": DEMO_SECRET_MARKER,
                "shards": {"alpha": [2, 3], "beta": [5, 7, 11]},
            },
            kind=ArtifactKind.GENERIC,
            sensitivity=ArtifactSensitivity.SECRET,
            producer_run_id=RUN_ID,
        )
    )
    scheduler.create_run(
        RUN_ID,
        input=RunInputReceipt((input_ref,)),
        metadata={"scenario": "response-loss-recovery"},
    )

    # Both branch claims exist concurrently before either worker executes.
    alpha_claim = scheduler.claim_next(RUN_ID, "worker-alpha", capacity=2)
    beta_claim = scheduler.claim_next(RUN_ID, "worker-beta", capacity=2)
    if alpha_claim is None or beta_claim is None:
        raise AssertionError("parallel branches were not both claimable")
    parallel_claimed = {
        alpha_claim.node_id,
        beta_claim.node_id,
    } == {"analyze_alpha", "analyze_beta"}

    _execute(
        executor,
        alpha_claim,
        profile,
        script_ref=script_refs["analyze_fragment"],
    )
    # Tx D committed but the caller response is discarded. Retrying the exact
    # durable claim must replay without a second backend dispatch.
    retry_result = _execute(
        executor,
        alpha_claim,
        profile,
        script_ref=script_refs["analyze_fragment"],
    )
    _execute(
        executor,
        beta_claim,
        profile,
        script_ref=script_refs["analyze_fragment"],
    )
    if store.get_node(RUN_ID, "join_analysis").status is not NodeStatus.SUCCEEDED:
        raise AssertionError("join did not wait for and combine both branches")

    publish_claim = scheduler.claim_next(RUN_ID, "worker-publisher")
    if publish_claim is None or publish_claim.node_id != "publish_report":
        raise AssertionError("publication was not scheduled after the join")

    action = executor.build_action(
        publish_claim,
        argv=("durable-demo-closed-function", publish_claim.node_id),
        cwd=profile.allowed_roots[0],
        profile=profile,
        input_artifact_refs=publish_claim.input_artifact_refs,
        script_artifact_ref=script_refs["publish_report"],
        resource_locks=publish_claim.resource_keys,
    )
    grant = ApprovalGrant(
        approval_id="demo-publication-approval",
        action_digest=action.action_digest,
        run_id=RUN_ID,
        node_id=publish_claim.node_id,
        policy_version=policy.policy_version,
        actor=_APPROVAL_ACTOR,
        expires_at=200.0,
    )
    DurableApprovalRegistry(
        store,
        trusted_actors=(_APPROVAL_ACTOR,),
        clock=_LogicalClock(),
    ).register_issued(grant)

    # The external write commits, then the worker crashes before Tx D can record
    # the receipt.  The Activity remains RUNNING and must be resolved by probe.
    crash_armed = True
    try:
        _execute(
            executor,
            publish_claim,
            profile,
            script_ref=script_refs["publish_report"],
            grant=grant,
        )
    except RuntimeError as exc:
        if str(exc) != "simulated crash after external write before Tx D":
            raise
    else:
        raise AssertionError("the Tx D crash window was not exercised")
    dispatches_before_restart = backend.calls
    if store.get_attempt(publish_claim.attempt_id).status is not AttemptStatus.RUNNING:
        raise AssertionError("pre-Tx-D crash did not leave a recoverable RUNNING Attempt")

    # Reconstruct every service object over the same durable files, then let the
    # client retry the exact claim.  No process-local executor state is reused.
    (
        restarted_store,
        _restarted_scheduler,
        _restarted_policy,
        restarted_backend,
        restarted_profile,
        restarted_executor,
    ) = _runtime(
        control_plane_root,
        agent_workspace_root,
        artifacts,
        visible_effects,
    )
    recovery_report = DurableLeaseReaper(
        restarted_store,
        probe_resolver=lambda attempt: (
            restarted_backend if attempt.node_id == "publish_report" else None
        ),
        artifact_verifier=artifacts,
    ).run_once(now=publish_claim.lease_expires_at + 1.0)
    _restarted_scheduler.reconcile(RUN_ID)
    probe_recovered = (
        len(recovery_report.resolved) == 1
        and recovery_report.resolved[0].resolution == "verified_succeeded"
    )
    stale_rejected = False
    try:
        _execute(
            restarted_executor,
            replace(publish_claim, claim_token="stale-demo-token"),
            restarted_profile,
            script_ref=script_refs["publish_report"],
            grant=grant,
        )
    except ActivityExecutionConflict:
        stale_rejected = True

    final_run = restarted_store.get_run(RUN_ID)
    if final_run is None:
        raise AssertionError("durable Run disappeared after restart")
    nodes = restarted_store.list_nodes(RUN_ID)
    attempts = restarted_store.list_attempts(RUN_ID)
    side_effect_count = visible_effects.count()
    replay = build_replay_report(restarted_store, RUN_ID)
    recovery_succeeded = (
        retry_result.replayed
        and probe_recovered
        and crash_observed
        and final_run.status is RunStatus.COMPLETED
        and side_effect_count == 1
    )
    terminal_unknown = sum(
        attempt.status is AttemptStatus.OUTCOME_UNKNOWN for attempt in attempts
    )
    cancellation_leaks = int(
        final_run.status is RunStatus.CANCELLING
        or any(node.status is NodeStatus.CANCELLED for node in nodes)
    )
    reliability = evaluate_reliability(
        ReliabilityEvidence(
            recovery_successes=int(recovery_succeeded),
            recovery_attempts=1,
            duplicate_visible_side_effects=max(0, side_effect_count - 1),
            visible_side_effect_checks=1,
            outcome_unknown_attempts=terminal_unknown,
            terminal_attempt_checks=len(attempts),
            projection_replay_divergences=int(not replay.matches_live),
            projection_replay_checks=1,
            cancellation_leaks=cancellation_leaks,
            cancellation_checks=1,
            stale_worker_commit_rejections=int(stale_rejected),
            stale_worker_commit_attempts=1,
            # This local restart advances no logical time; production evidence
            # should instead use measured monotonic recovery latency.
            resume_latencies_ms=(0.0,),
        )
    ).to_dict()

    approval_events = [
        event
        for event in restarted_store.list_events(RUN_ID)
        if event.event_type == "policy.decided"
        and event.node_id == "publish_report"
    ]
    approval_consumed = (
        len(approval_events) == 1
        and approval_events[0].payload.get("reason_code") == "approval_consumed"
    )
    summary: dict[str, Any] = {
        "schema_version": 1,
        "run": {
            "attempt_count": len(attempts),
            "node_statuses": {
                node.node_id: node.status.value
                for node in sorted(nodes, key=lambda item: item.node_id)
            },
            "status": final_run.status.value,
        },
        "execution": {
            "approval_gate": "consumed" if approval_consumed else "failed",
            "backend_dispatch_count": (
                dispatches_before_restart + restarted_backend.calls
            ),
            "duplicate_retry_replayed": retry_result.replayed,
            "external_write_before_tx_d_recovered": probe_recovered,
            "operation_key_deduplicated": side_effect_count == 1,
            "parallel_claimed_before_execution": parallel_claimed,
            "response_loss_recovered": recovery_succeeded,
            "stale_claim_rejected": stale_rejected,
            "visible_side_effect_count": side_effect_count,
        },
        "replay": {
            "event_count": replay.snapshot.event_count,
            "golden_digest": replay.golden_digest,
            "matches_live": replay.matches_live,
        },
        "reliability": reliability,
    }

    if (
        final_run.status is not RunStatus.COMPLETED
        or not parallel_claimed
        or not approval_consumed
        or not recovery_succeeded
        or not stale_rejected
        or not replay.matches_live
        or not _known_metrics(reliability)
    ):
        raise AssertionError("durable orchestration acceptance invariants failed")
    if DEMO_SECRET_MARKER in json.dumps(summary, ensure_ascii=False):
        raise AssertionError("raw input escaped the Artifact boundary")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the local durable-orchestration acceptance scenario."
    )
    parser.add_argument(
        "--runtime-dir",
        type=Path,
        help="Keep durable demo files in this directory (default: temporary).",
    )
    args = parser.parse_args(argv)
    if args.runtime_dir is not None:
        summary = run_demo(args.runtime_dir)
    else:
        with tempfile.TemporaryDirectory(prefix="xagent-durable-demo-") as temporary:
            summary = run_demo(temporary)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
