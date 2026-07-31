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
            "AgentProviderClientError",
            "DurableAgentProviderClient",
            "AgentToolInvocationRequest",
            "AgentToolInvocationResult",
            "DurableAgentToolHandler",
            "AgentToolExecutionSpec",
            "DurableAgentToolExecutor",
            "DurableAgentTerminalCommitter",
            "ToolReceiptArtifactStore",
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
            "DurableMaintenanceSupervisor",
            "DurableHierarchy",
            "WorkflowRegistry",
            # Policy / Executor.
            "PolicyEngine",
            "ApprovalGrant",
            "TrustedActivityExecutor",
            "DurableApprovalRegistry",
            # Explicit secure-distributed composition.
            "ArtifactGrantBroker",
            "AuthenticatedWorker",
            "BoundedRemoteObservability",
            "DeterministicRemoteScheduler",
            "DurableFleetProjector",
            "FleetToolRoutingPolicy",
            "FleetWorkerPolicy",
            "HttpsRemoteTransport",
            "OciGvisorSandboxBackend",
            "PinnedCertificateIdentityVerifier",
            "PinnedWorkerCertificate",
            "RemoteControlJournal",
            "RemoteControlFleetClaimer",
            "RemoteControlPlane",
            "RemoteFleetCoordinator",
            "RemoteHttpASGIApp",
            "SecureRemoteFleetPoller",
            "StaticFleetToolPolicyResolver",
            "RemoteWorkerClient",
            "RemoteWorkerDaemon",
            "SecureRemoteAssignmentAdmitter",
            "SecureRemoteExecutionAdapter",
            "WorkerAuthorizationGate",
            "WorkerDescriptor",
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
        from src.orchestration.agent_provider_client import (
            AgentProviderClientError,
            DurableAgentProviderClient,
        )
        from src.orchestration.agent_tool_handler import (
            AgentToolInvocationRequest,
            AgentToolInvocationResult,
            DurableAgentToolHandler,
        )
        from src.orchestration.agent_tool_executor import (
            AgentToolExecutionSpec,
            DurableAgentToolExecutor,
        )
        from src.orchestration.agent_terminal import (
            DurableAgentTerminalCommitter,
        )
        from src.orchestration.artifacts_gc import LocalArtifactGarbageCollector
        from src.orchestration.evaluation import Suite
        from src.orchestration.executor import TrustedActivityExecutor
        from src.orchestration.remote_control import RemoteControlPlane
        from src.orchestration.remote_journal import RemoteControlJournal
        from src.orchestration.remote_http import (
            HttpsRemoteTransport,
            PinnedCertificateIdentityVerifier,
            RemoteHttpASGIApp,
        )
        from src.orchestration.remote_fleet_control import (
            DurableFleetProjector,
            FleetToolRoutingPolicy,
            RemoteControlFleetClaimer,
            SecureRemoteFleetPoller,
            StaticFleetToolPolicyResolver,
        )
        from src.orchestration.remote_execution import (
            SecureRemoteAssignmentAdmitter,
        )
        from src.orchestration.remote_worker import RemoteWorkerDaemon
        from src.orchestration.hierarchy import DurableHierarchy
        from src.orchestration.mcp import MCPServer
        from src.orchestration.policy import PolicyEngine
        from src.orchestration.replay import ReplayReport
        from src.orchestration.runtime import OrchestrationRuntime
        from src.orchestration.runtime import ExecutorFactory
        from src.orchestration.scheduler import DurableScheduler
        from src.orchestration.store import DurableRunStore
        from src.orchestration.tool_receipt_artifact import (
            ToolReceiptArtifactStore,
        )
        from src.orchestration.workflow import compile_workflow

        pairs = (
            ("AgentProviderClientError", AgentProviderClientError),
            ("DurableAgentProviderClient", DurableAgentProviderClient),
            ("AgentToolInvocationRequest", AgentToolInvocationRequest),
            ("AgentToolInvocationResult", AgentToolInvocationResult),
            ("DurableAgentToolHandler", DurableAgentToolHandler),
            ("AgentToolExecutionSpec", AgentToolExecutionSpec),
            ("DurableAgentToolExecutor", DurableAgentToolExecutor),
            (
                "DurableAgentTerminalCommitter",
                DurableAgentTerminalCommitter,
            ),
            ("ToolReceiptArtifactStore", ToolReceiptArtifactStore),
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
            ("RemoteControlPlane", RemoteControlPlane),
            ("RemoteControlJournal", RemoteControlJournal),
            ("DurableFleetProjector", DurableFleetProjector),
            ("FleetToolRoutingPolicy", FleetToolRoutingPolicy),
            ("RemoteControlFleetClaimer", RemoteControlFleetClaimer),
            ("SecureRemoteFleetPoller", SecureRemoteFleetPoller),
            (
                "StaticFleetToolPolicyResolver",
                StaticFleetToolPolicyResolver,
            ),
            ("HttpsRemoteTransport", HttpsRemoteTransport),
            (
                "PinnedCertificateIdentityVerifier",
                PinnedCertificateIdentityVerifier,
            ),
            ("RemoteHttpASGIApp", RemoteHttpASGIApp),
            ("RemoteWorkerDaemon", RemoteWorkerDaemon),
            (
                "SecureRemoteAssignmentAdmitter",
                SecureRemoteAssignmentAdmitter,
            ),
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

    def test_default_policy_import_does_not_eagerly_load_control_plane(self) -> None:
        script = textwrap.dedent(
            """
            import sys

            import src.core.local_policy

            unexpected = {
                "src.orchestration.legacy_loop",
                "src.orchestration.mcp",
                "src.orchestration.remote_control",
                "src.orchestration.remote_execution",
                "src.orchestration.remote_fleet",
                "src.orchestration.runtime",
                "src.orchestration.scheduler",
                "src.orchestration.store",
            }.intersection(sys.modules)

            assert "src.orchestration.policy" in sys.modules
            assert not unexpected, sorted(unexpected)
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
