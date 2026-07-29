from __future__ import annotations

import subprocess
import sys
import textwrap
import unittest

import src.orchestration as orchestration


class OrchestrationPublicApiTests(unittest.TestCase):
    def test_public_surface_is_unique_and_covers_composition_entries(self) -> None:
        expected = {
            # Artifact / Store.
            "ArtifactRef",
            "ArtifactStore",
            "LocalArtifactStore",
            "ArtifactGCReport",
            "LocalArtifactGarbageCollector",
            "QuarantinedArtifact",
            "QuarantinedTemporaryArtifact",
            "DurableRunStore",
            "ClaimDisposition",
            "IdempotencyConflictError",
            "InvalidStateTransition",
            "ModelValidationError",
            "RunRecord",
            "NodeRecord",
            "AttemptRecord",
            "ProjectionConflictError",
            "ProjectionReplayLimitError",
            "StoreSchemaError",
            # Workflow / Scheduler / Runtime.
            "CompiledWorkflow",
            "compile_workflow",
            "DurableScheduler",
            "RunInputReceipt",
            "OrchestrationRuntime",
            "ExecutionDriver",
            "ExecutorFactory",
            "RecoveryDriver",
            "SchedulerFactory",
            "SchedulerResolver",
            "WorkflowCompiler",
            # Deadline / Hierarchy.
            "DurableDeadlineScanner",
            "DurableHierarchy",
            "WorkflowRegistry",
            # Policy / Executor.
            "PolicyEngine",
            "ApprovalGrant",
            "TrustedActivityExecutor",
            "DurableApprovalRegistry",
            # Replay / Evaluation.
            "ReplayReport",
            "build_replay_report",
            "Suite",
            "fault_scenarios",
            "ReliabilityEvidence",
            "evaluate_reliability",
            # MCP and conservative legacy adapters.
            "InvalidInputTokenBucket",
            "MCPServer",
            "PerContextRateLimiter",
            "LegacyAgentLoopAdapter",
            "LegacyCheckpointImporter",
        }

        self.assertEqual(len(orchestration.__all__), len(set(orchestration.__all__)))
        self.assertTrue(expected.issubset(orchestration.__all__))
        for name in orchestration.__all__:
            with self.subTest(name=name):
                self.assertTrue(hasattr(orchestration, name))

    def test_entries_are_the_objects_from_their_defining_modules(self) -> None:
        from src.orchestration.artifacts import ArtifactRef
        from src.orchestration.artifacts_gc import LocalArtifactGarbageCollector
        from src.orchestration.evaluation import Suite
        from src.orchestration.executor import TrustedActivityExecutor
        from src.orchestration.hierarchy import DurableHierarchy
        from src.orchestration.mcp import MCPServer
        from src.orchestration.policy import PolicyEngine
        from src.orchestration.replay import ReplayReport
        from src.orchestration.runtime import OrchestrationRuntime
        from src.orchestration.runtime import ExecutorFactory
        from src.orchestration.scheduler import DurableScheduler
        from src.orchestration.store import DurableRunStore
        from src.orchestration.workflow import compile_workflow

        pairs = (
            ("ArtifactRef", ArtifactRef),
            ("LocalArtifactGarbageCollector", LocalArtifactGarbageCollector),
            ("DurableRunStore", DurableRunStore),
            ("compile_workflow", compile_workflow),
            ("DurableScheduler", DurableScheduler),
            ("OrchestrationRuntime", OrchestrationRuntime),
            ("ExecutorFactory", ExecutorFactory),
            ("DurableHierarchy", DurableHierarchy),
            ("PolicyEngine", PolicyEngine),
            ("TrustedActivityExecutor", TrustedActivityExecutor),
            ("ReplayReport", ReplayReport),
            ("Suite", Suite),
            ("MCPServer", MCPServer),
        )
        for name, direct in pairs:
            with self.subTest(name=name):
                self.assertIs(getattr(orchestration, name), direct)

    def test_fresh_import_does_not_load_optional_frontend_dependencies(self) -> None:
        script = textwrap.dedent(
            """
            import importlib.abc
            import sys

            blocked = {
                "fastapi",
                "gradio",
                "langfuse",
                "opentelemetry",
                "selenium",
            }

            class Blocker(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path=None, target=None):
                    if fullname.split(".", 1)[0] in blocked:
                        raise AssertionError(f"optional import attempted: {fullname}")
                    return None

            sys.meta_path.insert(0, Blocker())
            import src.orchestration as api

            assert "src.orchestration.web_api" not in sys.modules
            assert "create_projection_router" not in api.__all__
            assert "WorkspaceStoreRegistry" not in api.__all__
            assert not blocked.intersection(sys.modules)
            """
        )

        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=".",
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(
            completed.returncode,
            0,
            msg=completed.stderr or completed.stdout,
        )


if __name__ == "__main__":
    unittest.main()
