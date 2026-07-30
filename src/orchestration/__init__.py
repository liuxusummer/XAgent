"""Stable public surface for the optional durable orchestration control plane.

The package does not enable orchestration or alter the existing CLI/Web paths
on import.  It also intentionally excludes Web projection adapters so importing
``src.orchestration`` never requires FastAPI.

Attempts are at-least-once.  External side effects and the local Event Store do
not share a transaction, so callers must use idempotency/probe contracts where
available and preserve ``OUTCOME_UNKNOWN`` when completion cannot be proved.
Advanced implementation types remain available from their defining submodules;
only the composition entries below are covered by the top-level API.
"""

from .artifacts import (
    ArtifactEncryption,
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    ArtifactStore,
    JsonArtifactResultWriter,
    LocalArtifactStore,
)
from .artifacts_gc import (
    ArtifactGCError,
    ArtifactGCIntegrityError,
    ArtifactGCReport,
    ArtifactGCRestoreError,
    LocalArtifactGarbageCollector,
    QuarantinedArtifact,
    QuarantinedTemporaryArtifact,
)
from .artifact_broker import ArtifactGrantBroker
from .deadline import (
    DeadlineAction,
    DeadlineReport,
    DurableDeadlineScanner,
)
from .evaluation import (
    ReliabilityEvidence,
    ReliabilityReport,
    Suite,
    SuiteReport,
    evaluate_reliability,
    fault_scenarios,
)
from .executor import (
    ActivityExecutionConflict,
    ActivityExecutionResult,
    ActivityExecutorError,
    ArtifactReceiptError,
    ControlPlaneIsolationError,
    DurableApprovalRegistry,
    ToolReceipt,
    ToolReceiptError,
    ToolReceiptVerification,
    TrustedActivityExecutor,
)
from .hierarchy import DurableHierarchy, WorkflowRegistry
from .legacy_checkpoint import (
    LegacyCheckpointImport,
    LegacyCheckpointImporter,
    LegacyCheckpointImportError,
)
from .legacy_loop import LegacyAgentLoopAdapter, run_legacy_agent_activity
from .mcp import (
    InvalidInputTokenBucket,
    MCP_PROTOCOL_VERSION,
    MCPServer,
    PerContextRateLimiter,
)
from .models import (
    AttemptRecord,
    AttemptStatus,
    ClaimDisposition,
    EventRecord,
    ModelValidationError,
    NodeRecord,
    NodeStatus,
    RunRecord,
    RunStatus,
)
from .policy import (
    ActionRequest,
    ApprovalGrant,
    Capability,
    EffectClass,
    PolicyDecision,
    PolicyEngine,
    PolicyOutcome,
    PolicyRule,
    ToolTimeoutBehavior,
    ToolPolicy,
)
from .replay import ReplayReport, build_replay_report
from .runtime import (
    AuthorizationRequest,
    ExecutionDriver,
    ExecutorFactory,
    HierarchySubmitter,
    OrchestrationRuntime,
    RecoveryDriver,
    RuntimeAuthorizationError,
    RuntimeCapabilityUnavailable,
    RuntimeErrorBase,
    RuntimeInputError,
    SchedulerFactory,
    SchedulerResolver,
    TrustedAuthorizer,
    WorkflowCompiler,
)
from .sandbox import (
    BackendExecutionResult,
    CancellationProbe,
    CancellationSignal,
    EnvironmentBinding,
    NetworkMode,
    ResourceLimits,
    SandboxDispatchDenied,
    SandboxDispatcher,
    SandboxOutcome,
    SandboxProfile,
    SandboxReceipt,
    SandboxValidationError,
    SecurityLevel,
)
from .process_backend import LocalProcessSupervisorBackend
from .oci_backend import OciGvisorSandboxBackend
from .remote_control import RemoteControlPlane
from .remote_journal import RemoteControlJournal
from .remote_execution import (
    SecureRemoteAssignmentAdmitter,
    SecureRemoteExecutionAdapter,
)
from .remote_fleet import RemoteFleetCoordinator
from .remote_observability import BoundedRemoteObservability
from .remote_protocol import AuthenticatedWorker
from .remote_scheduling import (
    DeterministicRemoteScheduler,
    WorkerDescriptor,
)
from .remote_worker import (
    RemoteWorkerClient,
    RemoteWorkerDaemon,
)
from .scheduler import (
    ActivityClaim,
    ActivityReceipt,
    ApprovalResolution,
    DefinitionMismatchError,
    DurableScheduler,
    InputMappingError,
    InputPersistenceError,
    ReconcileResult,
    ResultPersistenceError,
    RunInputReceipt,
    SchedulerError,
    SchedulerStateError,
    StoreCapabilityError,
)
from .store import (
    ActivityAdmissionDenied,
    ArtifactGCReferenceConflictError,
    DurableRunStore,
    IdempotencyConflictError,
    InvalidStateTransition,
    OrchestrationStoreError,
    ProjectionConflictError,
    ProjectionReplayLimitError,
    RunAlreadyExistsError,
    RunHierarchyLimitError,
    RunNotFoundError,
    StoreSchemaError,
    WorkflowBindingConflictError,
    WorkflowBindingRecord,
)
from .worker_security import WorkerAuthorizationGate
from .workflow import (
    CompiledWorkflow,
    NodeDefinition,
    WorkflowCompileError,
    compile_team_workflow_v1,
    compile_workflow,
)

__all__ = [
    # Artifact boundary.
    "ArtifactEncryption",
    "ArtifactKind",
    "ArtifactRef",
    "ArtifactSensitivity",
    "ArtifactStore",
    "JsonArtifactResultWriter",
    "LocalArtifactStore",
    "ArtifactGCError",
    "ArtifactGCIntegrityError",
    "ArtifactGCReport",
    "ArtifactGCRestoreError",
    "LocalArtifactGarbageCollector",
    "QuarantinedArtifact",
    "QuarantinedTemporaryArtifact",
    "ArtifactGrantBroker",
    # Durable records and Store.
    "AttemptRecord",
    "AttemptStatus",
    "ActivityAdmissionDenied",
    "ArtifactGCReferenceConflictError",
    "ClaimDisposition",
    "DurableRunStore",
    "EventRecord",
    "IdempotencyConflictError",
    "InvalidStateTransition",
    "ModelValidationError",
    "NodeRecord",
    "NodeStatus",
    "OrchestrationStoreError",
    "ProjectionConflictError",
    "ProjectionReplayLimitError",
    "RunAlreadyExistsError",
    "RunHierarchyLimitError",
    "RunNotFoundError",
    "RunRecord",
    "RunStatus",
    "StoreSchemaError",
    "WorkflowBindingConflictError",
    "WorkflowBindingRecord",
    # Workflow and Scheduler.
    "ActivityClaim",
    "ActivityReceipt",
    "ApprovalResolution",
    "CompiledWorkflow",
    "DefinitionMismatchError",
    "DurableScheduler",
    "InputMappingError",
    "InputPersistenceError",
    "NodeDefinition",
    "ReconcileResult",
    "ResultPersistenceError",
    "RunInputReceipt",
    "SchedulerError",
    "SchedulerStateError",
    "StoreCapabilityError",
    "WorkflowCompileError",
    "compile_team_workflow_v1",
    "compile_workflow",
    # Runtime and deadlines.
    "AuthorizationRequest",
    "ExecutionDriver",
    "ExecutorFactory",
    "HierarchySubmitter",
    "OrchestrationRuntime",
    "RecoveryDriver",
    "RuntimeAuthorizationError",
    "RuntimeCapabilityUnavailable",
    "RuntimeErrorBase",
    "RuntimeInputError",
    "SchedulerFactory",
    "SchedulerResolver",
    "TrustedAuthorizer",
    "WorkflowCompiler",
    "DeadlineAction",
    "DeadlineReport",
    "DurableDeadlineScanner",
    # Hierarchy.
    "DurableHierarchy",
    "WorkflowRegistry",
    # Policy and trusted execution.
    "ActionRequest",
    "ActivityExecutionConflict",
    "ActivityExecutionResult",
    "ActivityExecutorError",
    "ApprovalGrant",
    "ArtifactReceiptError",
    "Capability",
    "ControlPlaneIsolationError",
    "DurableApprovalRegistry",
    "EffectClass",
    "PolicyDecision",
    "PolicyEngine",
    "PolicyOutcome",
    "PolicyRule",
    "ToolTimeoutBehavior",
    "ToolPolicy",
    "ToolReceipt",
    "ToolReceiptError",
    "ToolReceiptVerification",
    "TrustedActivityExecutor",
    # Sandbox composition.
    "BackendExecutionResult",
    "CancellationProbe",
    "CancellationSignal",
    "EnvironmentBinding",
    "LocalProcessSupervisorBackend",
    "NetworkMode",
    "ResourceLimits",
    "SandboxDispatchDenied",
    "SandboxDispatcher",
    "SandboxOutcome",
    "SandboxProfile",
    "SandboxReceipt",
    "SandboxValidationError",
    "SecurityLevel",
    # Explicit secure-distributed composition. These imports are inert: users
    # must inject transport, identity, broker, runtime, and trust adapters.
    "AuthenticatedWorker",
    "RemoteControlPlane",
    "RemoteControlJournal",
    "SecureRemoteAssignmentAdmitter",
    "SecureRemoteExecutionAdapter",
    "RemoteWorkerClient",
    "RemoteWorkerDaemon",
    "WorkerAuthorizationGate",
    "DeterministicRemoteScheduler",
    "WorkerDescriptor",
    "RemoteFleetCoordinator",
    "BoundedRemoteObservability",
    "OciGvisorSandboxBackend",
    # Replay and evaluation.
    "ReliabilityEvidence",
    "ReliabilityReport",
    "ReplayReport",
    "Suite",
    "SuiteReport",
    "build_replay_report",
    "evaluate_reliability",
    "fault_scenarios",
    # Stateless MCP.
    "MCP_PROTOCOL_VERSION",
    "MCPServer",
    "InvalidInputTokenBucket",
    "PerContextRateLimiter",
    # Explicit conservative legacy adapters.
    "LegacyCheckpointImport",
    "LegacyCheckpointImportError",
    "LegacyCheckpointImporter",
    "LegacyAgentLoopAdapter",
    "run_legacy_agent_activity",
]
