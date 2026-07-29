"""Minimal durable Runtime composition with one read-only local Activity.

This example is intentionally small. The larger
``durable_orchestration_demo.py`` is the fault-injection acceptance scenario.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from src.orchestration import (  # noqa: E402
    DurableRunStore,
    DurableScheduler,
    EffectClass,
    LocalArtifactStore,
    LocalProcessSupervisorBackend,
    OrchestrationRuntime,
    PolicyEngine,
    ResourceLimits,
    RunInputReceipt,
    SandboxDispatcher,
    SandboxProfile,
    SecurityLevel,
    ToolPolicy,
    ToolTimeoutBehavior,
    TrustedActivityExecutor,
    compile_workflow,
)

AUTH_CONTEXT = "minimal-local-control-plane"
RUN_ID = "minimal-runtime-run"


def _workflow() -> dict:
    return {
        "schema_version": 2,
        "name": "minimal-runtime-workflow",
        "version": 1,
        "nodes": [
            {
                "id": "inspect",
                "kind": "tool",
                "depends_on": [],
                "input_mapping": {
                    "source": {"source": "run_input"},
                },
                "config": {
                    "tool": "python-read-only",
                    "arguments": {"mode": "fixed-demo"},
                },
                "effect_class": "read_only",
            }
        ],
    }


def run(runtime_root: Path) -> dict:
    control_root = (runtime_root / "control-plane").resolve()
    agent_root = (runtime_root / "agent-workspace").resolve()
    control_root.mkdir(parents=True, exist_ok=True)
    agent_root.mkdir(parents=True, exist_ok=True)

    store = DurableRunStore(control_root / "orchestration.sqlite3")
    artifacts = LocalArtifactStore(control_root / "artifacts")
    workflow_ref = artifacts.put_json(_workflow())
    input_ref = artifacts.put_json({"message": "immutable input"})

    policy = PolicyEngine(
        (
            ToolPolicy(
                "python-read-only",
                EffectClass.READ_ONLY,
                supports_idempotency_key=False,
                supports_status_probe=False,
                supports_compensation=False,
                timeout_behavior=ToolTimeoutBehavior.SAFE_TO_RETRY,
            ),
        )
    )
    dispatcher = SandboxDispatcher(
        (LocalProcessSupervisorBackend(artifacts),),
        policy_version=policy.policy_version,
    )
    profile = SandboxProfile(
        "minimal-local-process",
        (agent_root,),
        (),
        limits=ResourceLimits(timeout_seconds=5, output_bytes=16 * 1024),
        minimum_security_level=SecurityLevel.DEVELOPMENT_UNSAFE,
    )

    def scheduler_factory(
        target_store: DurableRunStore,
        workflow,
    ) -> DurableScheduler:
        return DurableScheduler(
            target_store,
            workflow,
            artifact_verifier=artifacts.verify,
        )

    def executor_factory(scheduler: DurableScheduler):
        return TrustedActivityExecutor(
            scheduler,
            policy,
            dispatcher,
            artifact_verifier=artifacts.verify,
            artifact_reader=artifacts.read,
        )

    def execute(
        scheduler: DurableScheduler,
        executor: TrustedActivityExecutor,
        run_id: str,
        maximum: int,
    ) -> int:
        processed = 0
        for _ in range(maximum):
            claim = scheduler.claim_next(run_id, "minimal-worker")
            if claim is None:
                break
            executor.execute(
                claim,
                argv=(
                    sys.executable,
                    "-c",
                    "print('minimal durable activity completed')",
                ),
                cwd=str(agent_root),
                profile=profile,
                input_artifact_refs=claim.input_artifact_refs,
                resource_locks=claim.resource_keys,
            )
            processed += 1
        return processed

    runtime = OrchestrationRuntime(
        store=store,
        artifact_store=artifacts,
        compiler=compile_workflow,
        scheduler_factory=scheduler_factory,
        executor_factory=executor_factory,
        execution_driver=execute,
        authorizer=lambda _request, context: context == AUTH_CONTEXT,
    )
    runtime.submit(
        RUN_ID,
        workflow_ref,
        input_receipt=RunInputReceipt((input_ref,)),
        authorization_context=AUTH_CONTEXT,
    )
    status = runtime.tick(
        RUN_ID,
        max_steps=1,
        authorization_context=AUTH_CONTEXT,
    )
    replay = runtime.replay_run(
        RUN_ID,
        authorization_context=AUTH_CONTEXT,
    )
    return {
        "run_id": status["run_id"],
        "status": status["status"],
        "processed": status["tick"]["processed"],
        "event_count": replay["event_count"],
        "replay_matches_live": replay["matches_live"],
        "control_plane": str(control_root),
        "agent_workspace": str(agent_root),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-dir", type=Path)
    arguments = parser.parse_args()
    if arguments.runtime_dir is not None:
        summary = run(arguments.runtime_dir)
    else:
        with tempfile.TemporaryDirectory(prefix="xagent-runtime-minimal-") as path:
            summary = run(Path(path))
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
