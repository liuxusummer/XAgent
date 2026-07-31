"""Stable lazy surface for the optional durable orchestration control plane.

The package does not enable orchestration or alter the existing CLI/Web paths
on import.  Public composition entries are loaded from their defining modules
only when requested, so importing a focused submodule such as ``policy`` does
not initialize the entire optional control plane.

Attempts are at-least-once.  External side effects and the local Event Store do
not share a transaction, so callers must use idempotency/probe contracts where
available and preserve ``OUTCOME_UNKNOWN`` when completion cannot be proved.
Advanced implementation types remain available from their defining submodules;
only the composition entries below are covered by the top-level API.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


_MODULE_EXPORTS: dict[str, tuple[str, ...]] = {
    ".agent_activity_executor": (
        "AgentActivityExecutionError",
        "AgentActivityExecutionResult",
        "DurableAgentActivityExecutor",
    ),
    ".agent_execution_evidence": (
        "AgentExecutionEvidenceCollector",
        "AgentExecutionEvidenceCollectorError",
        "AgentProviderInvocationContext",
        "AgentToolInvocationContext",
    ),
    ".agent_execution_manifest": (
        "AgentActivityExecutionManifest",
        "AgentExecutionManifestArtifactStore",
        "AgentExecutionManifestError",
        "AgentProviderReceiptBinding",
        "AgentToolReceiptBinding",
        "agent_tool_operation_key",
        "canonical_tool_call_digest",
    ),
    ".agent_provider_client": (
        "AgentProviderClientError",
        "DurableAgentProviderClient",
    ),
    ".agent_tool_handler": (
        "AgentToolHandlerError",
        "AgentToolInvocationRequest",
        "AgentToolInvocationResult",
        "AgentToolSpec",
        "DurableAgentToolHandler",
    ),
    ".agent_tool_executor": (
        "AgentToolExecutionError",
        "AgentToolExecutionSpec",
        "DurableAgentToolExecutor",
    ),
    ".agent_tool_result": (
        "AgentToolResult",
        "AgentToolResultArtifactStore",
        "AgentToolResultError",
    ),
    ".agent_terminal": (
        "AgentActivityTerminalCommit",
        "AgentTerminalCommitError",
        "DurableAgentTerminalCommitter",
    ),
    ".agent_turn_checkpoint": (
        "AgentTurnCheckpoint",
        "AgentTurnCheckpointArtifactStore",
        "AgentTurnCheckpointError",
    ),
    ".agent_request": (
        "AgentActivityRequest",
        "AgentActivityRequestArtifactStore",
        "AgentActivityRequestError",
    ),
    ".agent_receipt": (
        "AgentActivityReceipt",
        "AgentActivityReceiptError",
        "AgentActivityVerification",
    ),
    ".artifacts": (
        "ArtifactEncryption",
        "ArtifactKind",
        "ArtifactRef",
        "ArtifactSensitivity",
        "ArtifactStore",
        "JsonArtifactResultWriter",
        "LocalArtifactStore",
    ),
    ".artifacts_gc": (
        "ArtifactGCError",
        "ArtifactGCIntegrityError",
        "ArtifactGCReport",
        "ArtifactGCRestoreError",
        "LocalArtifactGarbageCollector",
        "QuarantinedArtifact",
        "QuarantinedTemporaryArtifact",
    ),
    ".artifact_broker": ("ArtifactGrantBroker",),
    ".deadline": (
        "DeadlineAction",
        "DeadlineReport",
        "DurableDeadlineScanner",
    ),
    ".evaluation": (
        "ReliabilityEvidence",
        "ReliabilityReport",
        "Suite",
        "SuiteReport",
        "evaluate_reliability",
        "fault_scenarios",
    ),
    ".executor": (
        "ActivityExecutionConflict",
        "ActivityExecutionResult",
        "ActivityExecutorError",
        "ArtifactReceiptError",
        "ControlPlaneIsolationError",
        "DurableApprovalRegistry",
        "ToolReceipt",
        "ToolReceiptError",
        "ToolReceiptVerification",
        "TrustedActivityExecutor",
    ),
    ".hierarchy": (
        "DurableHierarchy",
        "WorkflowRegistry",
    ),
    ".legacy_checkpoint": (
        "LegacyCheckpointImport",
        "LegacyCheckpointImporter",
        "LegacyCheckpointImportError",
    ),
    ".legacy_loop": (
        "LegacyAgentLoopAdapter",
        "run_legacy_agent_activity",
    ),
    ".mcp": (
        "InvalidInputTokenBucket",
        "MCP_PROTOCOL_VERSION",
        "MCPServer",
        "PerContextRateLimiter",
    ),
    ".maintenance": (
        "DurableMaintenanceSupervisor",
        "MaintenanceConfigurationError",
        "MaintenanceCycleConflict",
        "MaintenanceCycleReport",
        "MaintenanceFailure",
        "MaintenanceFleetControl",
        "MaintenanceNotBootstrapped",
    ),
    ".models": (
        "AttemptRecord",
        "AttemptStatus",
        "ClaimDisposition",
        "EventRecord",
        "ModelValidationError",
        "NodeRecord",
        "NodeStatus",
        "RunRecord",
        "RunStatus",
    ),
    ".operator_diagnostics": (
        "OperatorDiagnostic",
        "diagnose_attempt",
        "diagnose_node",
        "diagnose_run",
        "summarize_operator_diagnostics",
    ),
    ".policy": (
        "ActionRequest",
        "ApprovalGrant",
        "Capability",
        "EffectClass",
        "PolicyCheck",
        "PolicyDecision",
        "PolicyEngine",
        "PolicyOutcome",
        "PolicyResolutionSource",
        "PolicyRule",
        "PolicySimulation",
        "ToolTimeoutBehavior",
        "ToolPolicy",
    ),
    ".policy_conformance": (
        "PolicyConformanceCase",
        "PolicyConformanceReport",
        "PolicyConformanceResult",
        "evaluate_policy_conformance",
    ),
    ".provider_access": (
        "ProviderAccessBroker",
        "ProviderAccessDenied",
        "ProviderAccessGrant",
        "ProviderInvocationCompletionMode",
        "ProviderInvocationReceipt",
        "ProviderInvocationResult",
        "ProviderInvoker",
        "ProviderOperationRecovery",
        "ProviderOperationState",
        "ProviderRecoveryEvidenceBinding",
        "ProviderRouteDescriptor",
        "RecoverableProviderInvoker",
    ),
    ".replay": (
        "ReplayReport",
        "build_replay_report",
    ),
    ".recovery": (
        "RecoveryDecisionError",
        "UnknownOutcomeDecision",
        "UnknownOutcomeResolution",
    ),
    ".runtime": (
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
    ),
    ".sandbox": (
        "BackendExecutionResult",
        "CancellationProbe",
        "CancellationSignal",
        "EnvironmentBinding",
        "NetworkMode",
        "ResourceLimits",
        "SandboxDispatchDenied",
        "SandboxDispatcher",
        "SandboxOutcome",
        "SandboxProfile",
        "SandboxReceipt",
        "SandboxValidationError",
        "SecurityLevel",
    ),
    ".process_backend": ("LocalProcessSupervisorBackend",),
    ".oci_backend": ("OciGvisorSandboxBackend",),
    ".remote_control": (
        "RemoteAdmissionMaintenanceGate",
        "RemoteControlPlane",
    ),
    ".remote_journal": ("RemoteControlJournal",),
    ".remote_execution_journal": (
        "RemoteExecutionBindingRecord",
        "RemoteExecutionJournal",
    ),
    ".remote_execution": (
        "RemoteExecutionRecoveryReport",
        "SecureRemoteAssignmentAdmitter",
        "SecureRemoteExecutionAdapter",
    ),
    ".remote_fleet": (
        "FleetQueueReconcileReport",
        "RemoteFleetCoordinator",
    ),
    ".remote_fleet_control": (
        "DurableFleetProjector",
        "FleetControlSnapshot",
        "FleetTerminalReconcileReport",
        "FleetToolRoutingPolicy",
        "FleetWorkerPolicy",
        "RemoteControlFleetClaimer",
        "RemoteFleetControlConfigurationError",
        "RemoteFleetControlConflict",
        "SecureRemoteFleetPoller",
        "StaticFleetToolPolicyResolver",
        "StaticFleetWorkerResolver",
    ),
    ".remote_fleet_reconcile": (
        "DurableFleetReconciler",
        "DurableStoreFleetRunSource",
        "FleetProjectionReconcileReport",
        "FleetRunRoute",
        "FleetRunSource",
        "StaticFleetRunSource",
    ),
    ".remote_http": (
        "AsgiTlsPeerAuthenticator",
        "HttpsRemoteTransport",
        "PinnedCertificateIdentityVerifier",
        "PinnedWorkerCertificate",
        "RemoteHttpASGIApp",
        "RemoteHttpConfigurationError",
        "RemotePeerAuthenticationError",
        "TlsPeerEvidence",
    ),
    ".remote_observability": ("BoundedRemoteObservability",),
    ".remote_protocol": ("AuthenticatedWorker",),
    ".remote_scheduling": (
        "DeterministicRemoteScheduler",
        "WorkerDescriptor",
    ),
    ".remote_worker": (
        "RemoteWorkerClient",
        "RemoteWorkerDaemon",
    ),
    ".scheduler": (
        "ActivityClaim",
        "ActivityReceipt",
        "ApprovalResolution",
        "DefinitionMismatchError",
        "DurableScheduler",
        "InputMappingError",
        "InputPersistenceError",
        "ReconcileResult",
        "ResultPersistenceError",
        "RunInputReceipt",
        "SchedulerError",
        "SchedulerStateError",
        "StoreCapabilityError",
    ),
    ".store": (
        "ActivityAdmissionDenied",
        "AgentToolExecutionClaim",
        "AgentToolInvocationConflict",
        "AgentToolInvocationRecord",
        "ArtifactGCReferenceConflictError",
        "DurableRunStore",
        "FleetFairnessCursor",
        "FleetRunRouteCapacityError",
        "FleetRunRouteConflict",
        "FleetRunRouteRecord",
        "FleetShardOwnership",
        "FleetShardOwnershipCapacityError",
        "FleetShardOwnershipConflict",
        "IdempotencyConflictError",
        "InvalidStateTransition",
        "OrchestrationStoreError",
        "ProjectionConflictError",
        "ProjectionReplayLimitError",
        "RunAlreadyExistsError",
        "RunHierarchyLimitError",
        "RunNotFoundError",
        "StoreSchemaError",
        "WorkflowBindingConflictError",
        "WorkflowBindingRecord",
    ),
    ".tool_receipt_artifact": (
        "ToolReceiptArtifactError",
        "ToolReceiptArtifactStore",
    ),
    ".worker_security": ("WorkerAuthorizationGate",),
    ".workflow": (
        "CompiledWorkflow",
        "NodeDefinition",
        "WorkflowCompileError",
        "compile_team_workflow_v1",
        "compile_workflow",
    ),
}

_EXPORT_TO_MODULE = {
    export: module
    for module, exports in _MODULE_EXPORTS.items()
    for export in exports
}

__all__ = list(_EXPORT_TO_MODULE)


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()).union(__all__))
