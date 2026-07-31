"""Trusted policy/sandbox composition for Agent-internal Tool calls."""

from __future__ import annotations

import hashlib
import math
import os
import re
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .agent_tool_handler import (
    AgentToolInvocationRequest,
    AgentToolInvocationResult,
)
from .agent_tool_result import (
    AGENT_TOOL_RESULT_MEDIA_TYPE,
    AgentToolResultArtifactStore,
    AgentToolResultError,
)
from .artifacts import (
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    ArtifactStore,
    LocalArtifactStore,
    canonical_json_bytes,
)
from .executor import ToolReceipt, ToolReceiptVerification
from .models import AttemptStatus, NodeStatus, RunStatus
from .policy import (
    ActionRequest,
    Capability,
    EffectClass,
    PolicyDecision,
    PolicyEngine,
    PolicyOutcome,
    sensitive_argument_bytes,
)
from .sandbox import (
    CancellationProbe,
    CancellationSignal,
    EnvironmentBinding,
    ExecutionRequest,
    ResourceLimits,
    SandboxDispatchDenied,
    SandboxDispatcher,
    SandboxOutcome,
    SandboxProfile,
    SandboxReceipt,
    build_execution_binding_digest,
)
from .store import (
    AgentToolExecutionClaim,
    AgentToolInvocationConflict,
    AgentToolInvocationRecord,
    DurableRunStore,
    MAX_AGENT_TOOL_LEASE_SECONDS,
)


_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_UNCERTAIN_EFFECTS = frozenset(
    {EffectClass.NON_IDEMPOTENT_WRITE, EffectClass.DESTRUCTIVE}
)
_ERROR_CODES = frozenset(
    {
        "agent_tool_active",
        "agent_tool_artifact_invalid",
        "agent_tool_configuration_invalid",
        "agent_tool_execution_invalid",
        "agent_tool_preflight_abandoned",
        "agent_tool_store_unavailable",
    }
)


class AgentToolExecutionError(RuntimeError, ValueError):
    """A dynamic child failed at a fixed, payload-free trust boundary."""

    def __init__(self, reason_code: str) -> None:
        if reason_code not in _ERROR_CODES:
            raise ValueError("invalid Agent Tool execution reason code")
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class AgentToolExecutionSpec:
    """Deployment-owned, immutable execution contract for one dynamic Tool."""

    tool_name: str
    effect_class: EffectClass
    profile: SandboxProfile
    cwd: str
    argv_builder: Callable[[Mapping[str, Any]], tuple[str, ...]] = field(
        repr=False,
        compare=False,
    )
    capabilities: tuple[Capability, ...] = ()
    resource_locks: tuple[str, ...] = ()
    limits: ResourceLimits | None = None
    input_artifact_refs: tuple[ArtifactRef, ...] = ()
    script_artifact_ref: ArtifactRef | None = None
    environment: tuple[EnvironmentBinding, ...] = field(
        default=(),
        repr=False,
    )

    def __post_init__(self) -> None:
        if (
            type(self.tool_name) is not str
            or _SAFE_NAME.fullmatch(self.tool_name) is None
            or type(self.profile) is not SandboxProfile
            or not callable(self.argv_builder)
        ):
            raise AgentToolExecutionError(
                "agent_tool_configuration_invalid"
            )
        try:
            effect_class = EffectClass(self.effect_class)
            cwd = os.fspath(self.cwd)
            capabilities = tuple(self.capabilities)
            resource_locks = tuple(self.resource_locks)
            input_refs = tuple(self.input_artifact_refs)
            environment = tuple(self.environment)
        except (TypeError, ValueError):
            raise AgentToolExecutionError(
                "agent_tool_configuration_invalid"
            ) from None
        if (
            not cwd
            or "\x00" in cwd
            or len(cwd) > 4096
            or any(type(value) is not Capability for value in capabilities)
            or any(type(value) is not str for value in resource_locks)
            or any(type(value) is not ArtifactRef for value in input_refs)
            or any(
                type(value) is not EnvironmentBinding
                for value in environment
            )
            or (
                self.limits is not None
                and type(self.limits) is not ResourceLimits
            )
            or (
                self.script_artifact_ref is not None
                and type(self.script_artifact_ref) is not ArtifactRef
            )
        ):
            raise AgentToolExecutionError(
                "agent_tool_configuration_invalid"
            )
        object.__setattr__(self, "effect_class", effect_class)
        object.__setattr__(self, "cwd", cwd)
        object.__setattr__(self, "capabilities", capabilities)
        object.__setattr__(
            self,
            "resource_locks",
            tuple(sorted(set(resource_locks))),
        )
        object.__setattr__(self, "input_artifact_refs", input_refs)
        object.__setattr__(self, "environment", environment)


@dataclass(frozen=True, slots=True)
class _PreparedAgentToolExecution:
    record: AgentToolInvocationRecord
    action: ActionRequest
    decision: PolicyDecision
    request: ExecutionRequest = field(repr=False)
    spec: AgentToolExecutionSpec = field(repr=False)


class DurableAgentToolExecutor:
    """Issue and consume one durable dynamic child authority end to end."""

    def __init__(
        self,
        store: DurableRunStore,
        policy: PolicyEngine,
        sandbox: SandboxDispatcher,
        result_store: ArtifactStore,
        specs: Iterable[AgentToolExecutionSpec],
        *,
        worker_id: str,
        artifact_verifier: Callable[[ArtifactRef], bool] | None = None,
        artifact_reader: Callable[[ArtifactRef], bytes] | None = None,
        lease_grace_seconds: float = 30.0,
        clock: Callable[[], float] | None = None,
        fault_hook: Callable[[str], None] | None = None,
        control_plane_roots: Iterable[str | Path] = (),
    ) -> None:
        if (
            not isinstance(store, DurableRunStore)
            or not isinstance(policy, PolicyEngine)
            or not isinstance(sandbox, SandboxDispatcher)
            or not isinstance(result_store, ArtifactStore)
            or sandbox.policy_version != policy.policy_version
            or type(worker_id) is not str
            or _SAFE_NAME.fullmatch(worker_id) is None
        ):
            raise AgentToolExecutionError(
                "agent_tool_configuration_invalid"
            )
        try:
            durable_artifacts = (
                isinstance(result_store, LocalArtifactStore)
                or getattr(
                    result_store,
                    "durable_result_recovery_ready",
                    False,
                )
                is True
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            durable_artifacts = False
        if not durable_artifacts:
            raise AgentToolExecutionError(
                "agent_tool_configuration_invalid"
            )
        verifier = result_store.verify if artifact_verifier is None else (
            artifact_verifier
        )
        reader = result_store.read if artifact_reader is None else (
            artifact_reader
        )
        if not callable(verifier) or not callable(reader):
            raise AgentToolExecutionError(
                "agent_tool_configuration_invalid"
            )
        try:
            grace = float(lease_grace_seconds)
            values = tuple(specs)
        except (TypeError, ValueError):
            raise AgentToolExecutionError(
                "agent_tool_configuration_invalid"
            ) from None
        if (
            not math.isfinite(grace)
            or grace <= 0
            or grace >= MAX_AGENT_TOOL_LEASE_SECONDS
            or not values
            or not all(type(value) is AgentToolExecutionSpec for value in values)
            or len({value.tool_name for value in values}) != len(values)
        ):
            raise AgentToolExecutionError(
                "agent_tool_configuration_invalid"
            )
        self.store = store
        self.policy = policy
        self.sandbox = sandbox
        self.result_store = result_store
        self._specs = {value.tool_name: value for value in values}
        self._worker_id = worker_id
        self._artifact_verifier = verifier
        self._artifact_reader = reader
        self._lease_grace_seconds = grace
        self._clock = clock or time.time
        self._fault_hook = fault_hook
        self._control_plane_paths = self._resolve_control_plane_paths(
            control_plane_roots,
        )
        for spec in values:
            self._validate_control_plane_isolation(spec)

    @property
    def durable_result_recovery_ready(self) -> bool:
        return True

    @property
    def production_security_ready(self) -> bool:
        """Deployment attestation and secret storage remain external."""

        return False

    def __repr__(self) -> str:
        return (
            "DurableAgentToolExecutor("
            f"tool_count={len(self._specs)}, worker_id={self._worker_id!r})"
        )

    def execute(
        self,
        request: AgentToolInvocationRequest,
    ) -> AgentToolInvocationResult:
        if type(request) is not AgentToolInvocationRequest:
            raise AgentToolExecutionError("agent_tool_execution_invalid")
        spec = self._specs.get(request.tool_name)
        if spec is None:
            raise AgentToolExecutionError(
                "agent_tool_configuration_invalid"
            )
        now = self._now()
        try:
            record = self.store.reserve_agent_tool_invocation(
                request,
                now=now,
            )
        except AgentToolInvocationConflict:
            raise AgentToolExecutionError(
                "agent_tool_store_unavailable"
            ) from None
        if record.is_terminal:
            return self._replay(record)
        if record.status is AttemptStatus.RUNNING:
            return self._recover_running(request, record, now=now)

        try:
            prepared = self._prepare(request, record, spec)
        except (KeyboardInterrupt, SystemExit):
            self._abandon_scheduled(request, record)
            raise
        except BaseException:
            self._abandon_scheduled(request, record)
            raise AgentToolExecutionError(
                "agent_tool_execution_invalid"
            ) from None

        if prepared.decision.outcome is not PolicyOutcome.ALLOW:
            try:
                rejected = self.store.reject_agent_tool_invocation(
                    record.invocation_id,
                    request.invocation_digest,
                    action_digest=prepared.action.action_digest,
                    execution_binding_digest=(
                        prepared.action.execution_binding_digest
                    ),
                    effect_class=prepared.action.effect_class.value,
                    policy_outcome=prepared.decision.outcome.value,
                    policy_version=self.policy.policy_version,
                    policy_digest=self.policy.policy_digest,
                    profile_id=spec.profile.profile_id,
                    profile_digest=spec.profile.profile_digest,
                    decision_digest=_decision_digest(prepared.decision),
                    now=self._now(),
                )
            except AgentToolInvocationConflict:
                raise AgentToolExecutionError(
                    "agent_tool_store_unavailable"
                ) from None
            return self._replay(rejected)

        lease_seconds = min(
            MAX_AGENT_TOOL_LEASE_SECONDS,
            prepared.request.limits.timeout_seconds
            + self._lease_grace_seconds,
        )
        try:
            claim = self.store.start_agent_tool_invocation(
                record.invocation_id,
                request.invocation_digest,
                self._worker_id,
                action_digest=prepared.action.action_digest,
                execution_binding_digest=(
                    prepared.action.execution_binding_digest
                ),
                effect_class=prepared.action.effect_class.value,
                policy_version=self.policy.policy_version,
                policy_digest=self.policy.policy_digest,
                profile_id=spec.profile.profile_id,
                profile_digest=spec.profile.profile_digest,
                decision_digest=_decision_digest(prepared.decision),
                lease_seconds=lease_seconds,
                now=self._now(),
            )
        except AgentToolInvocationConflict:
            latest = self.store.get_agent_tool_invocation(
                record.invocation_id
            )
            if latest is not None and latest.is_terminal:
                return self._replay(latest)
            raise AgentToolExecutionError("agent_tool_active") from None

        self._fault("agent_tool.authorization_committed")
        try:
            sandbox_receipt = self.sandbox.dispatch(
                prepared.request,
                spec.profile,
            )
        except SandboxDispatchDenied as exc:
            return self._complete_without_receipt(
                request,
                claim,
                prepared,
                error_code=exc.reason_code,
                absence_reason="dispatch_denied_before_backend",
                uncertain=False,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return self._complete_without_receipt(
                request,
                claim,
                prepared,
                error_code="dispatcher_exception",
                absence_reason="dispatcher_exception_before_receipt",
                uncertain=True,
            )
        self._fault("agent_tool.backend_returned")
        return self._complete_sandbox_receipt(
            request,
            claim,
            prepared,
            sandbox_receipt,
        )

    def _prepare(
        self,
        invocation_request: AgentToolInvocationRequest,
        record: AgentToolInvocationRecord,
        spec: AgentToolExecutionSpec,
    ) -> _PreparedAgentToolExecution:
        argv = spec.argv_builder(invocation_request.args)
        if type(argv) is not tuple:
            raise AgentToolExecutionError("agent_tool_execution_invalid")
        sensitive_values = sensitive_argument_bytes(
            invocation_request.args,
            invocation_request.sensitive_keys,
        )
        controls = [
            value.encode("utf-8")
            for value in (*argv, spec.cwd, *spec.resource_locks)
            if type(value) is str
        ]
        if any(
            secret in control
            for secret in sensitive_values
            for control in controls
        ):
            raise AgentToolExecutionError("agent_tool_execution_invalid")
        inputs = self._verified_inputs(
            invocation_request,
            spec.input_artifact_refs,
        )
        script_ref, script = self._materialized_script(
            invocation_request,
            spec.script_artifact_ref,
        )
        limits = spec.limits or spec.profile.limits
        execution_digest = build_execution_binding_digest(
            argv=argv,
            cwd=spec.cwd,
            profile=spec.profile,
            limits=limits,
            input_artifact_refs=inputs,
            script_artifact_ref=script_ref,
            operation_key=invocation_request.operation_key,
            idempotency_key=invocation_request.operation_key,
            environment=spec.environment,
            capabilities=spec.capabilities,
            resource_locks=spec.resource_locks,
        )
        tool_policy = self.policy.tool_policy(spec.tool_name)
        action = ActionRequest.from_args(
            run_id=record.run_id,
            node_id=record.child_node_id,
            attempt_id=record.child_attempt_id,
            tool_name=record.tool_name,
            args=invocation_request.args,
            execution_binding_digest=execution_digest,
            operation_key=invocation_request.operation_key,
            idempotency_key=invocation_request.operation_key,
            script_artifact_ref=script_ref,
            requires_script_artifact=(
                False
                if tool_policy is None
                else tool_policy.requires_script_artifact
            ),
            effect_class=spec.effect_class,
            capabilities=spec.capabilities,
            resource_locks=spec.resource_locks,
            sensitive_keys=invocation_request.sensitive_keys,
        )
        if action.args_digest != invocation_request.args_digest:
            raise AgentToolExecutionError("agent_tool_execution_invalid")
        decision = self.policy.evaluate(action)
        execution = ExecutionRequest(
            action=action,
            policy_decision=decision,
            argv=argv,
            operation_key=invocation_request.operation_key,
            idempotency_key=invocation_request.operation_key,
            cwd=spec.cwd,
            limits=limits,
            input_artifact_refs=inputs,
            script_artifact_ref=script_ref,
            materialized_script=script,
            environment=spec.environment,
            cancellation_probe=CancellationProbe(
                run_id=record.run_id,
                node_id=record.child_node_id,
                attempt_id=record.child_attempt_id,
                callback=lambda: self._cancellation_signal(
                    invocation_request,
                    record,
                ),
            ),
        ).validated_for_profile(spec.profile)
        return _PreparedAgentToolExecution(
            record=record,
            action=action,
            decision=decision,
            request=execution,
            spec=spec,
        )

    def _complete_sandbox_receipt(
        self,
        invocation_request: AgentToolInvocationRequest,
        claim: AgentToolExecutionClaim,
        prepared: _PreparedAgentToolExecution,
        receipt: SandboxReceipt,
    ) -> AgentToolInvocationResult:
        if (
            type(receipt) is not SandboxReceipt
            or receipt.action_digest != prepared.action.action_digest
            or receipt.policy_version != self.policy.policy_version
            or receipt.profile_id != prepared.spec.profile.profile_id
            or receipt.profile_digest
            != prepared.spec.profile.profile_digest
        ):
            return self._complete_without_receipt(
                invocation_request,
                claim,
                prepared,
                error_code="sandbox_receipt_invalid",
                absence_reason="sandbox_receipt_invalid",
                uncertain=True,
            )

        result_ref: ArtifactRef | None = None
        status: AttemptStatus
        verification: ToolReceiptVerification
        error_code: str | None = receipt.error_code
        effect = prepared.action.effect_class
        if receipt.outcome is SandboxOutcome.SUCCEEDED:
            try:
                result_ref = self._result_ref(
                    invocation_request,
                    claim.invocation,
                    receipt,
                )
            except AgentToolExecutionError:
                status = (
                    AttemptStatus.OUTCOME_UNKNOWN
                    if effect in _UNCERTAIN_EFFECTS
                    else AttemptStatus.FAILED
                )
                verification = ToolReceiptVerification.UNVERIFIED
                error_code = "artifact_integrity"
            else:
                status = AttemptStatus.SUCCEEDED
                verification = (
                    ToolReceiptVerification.VERIFIED
                    if effect is EffectClass.READ_ONLY
                    else ToolReceiptVerification.INFERRED
                )
        elif receipt.outcome is SandboxOutcome.CANCELLATION_UNKNOWN:
            status = AttemptStatus.OUTCOME_UNKNOWN
            verification = ToolReceiptVerification.UNVERIFIED
            error_code = "cancellation_probe_unavailable"
        elif receipt.outcome is SandboxOutcome.CANCELLED:
            run = self.store.get_run(claim.invocation.run_id)
            if run is not None and run.status in {
                RunStatus.CANCELLING,
                RunStatus.CANCELLED,
            }:
                status = AttemptStatus.CANCELLED
                verification = ToolReceiptVerification.INFERRED
                error_code = "cancelled"
            else:
                status = AttemptStatus.OUTCOME_UNKNOWN
                verification = ToolReceiptVerification.UNVERIFIED
                error_code = "unexpected_backend_cancellation"
        else:
            uncertain = effect in _UNCERTAIN_EFFECTS
            status = (
                AttemptStatus.OUTCOME_UNKNOWN
                if uncertain
                else (
                    AttemptStatus.TIMED_OUT
                    if receipt.outcome is SandboxOutcome.TIMED_OUT
                    else AttemptStatus.FAILED
                )
            )
            verification = (
                ToolReceiptVerification.UNVERIFIED
                if receipt.outcome is SandboxOutcome.BACKEND_ERROR
                or uncertain
                else ToolReceiptVerification.INFERRED
            )
            error_code = error_code or {
                SandboxOutcome.FAILED: "nonzero_exit",
                SandboxOutcome.TIMED_OUT: "timeout",
                SandboxOutcome.BACKEND_ERROR: "backend_error",
            }[receipt.outcome]

        tool_receipt = self._tool_receipt(
            prepared,
            status=status,
            sandbox_receipt=receipt,
            verification=verification,
            error_code=error_code,
        )
        if status is not AttemptStatus.SUCCEEDED:
            result_ref = None
        return self._commit(
            invocation_request,
            claim,
            tool_receipt,
            result_ref,
        )

    def _complete_without_receipt(
        self,
        invocation_request: AgentToolInvocationRequest,
        claim: AgentToolExecutionClaim,
        prepared: _PreparedAgentToolExecution,
        *,
        error_code: str,
        absence_reason: str,
        uncertain: bool,
    ) -> AgentToolInvocationResult:
        status = (
            AttemptStatus.OUTCOME_UNKNOWN
            if uncertain
            and prepared.action.effect_class in _UNCERTAIN_EFFECTS
            else AttemptStatus.FAILED
        )
        receipt = self._tool_receipt(
            prepared,
            status=status,
            sandbox_receipt=None,
            verification=ToolReceiptVerification.UNVERIFIED,
            error_code=error_code,
            absence_reason=absence_reason,
        )
        return self._commit(invocation_request, claim, receipt, None)

    def _commit(
        self,
        invocation_request: AgentToolInvocationRequest,
        claim: AgentToolExecutionClaim,
        receipt: ToolReceipt,
        result_ref: ArtifactRef | None,
    ) -> AgentToolInvocationResult:
        try:
            stored = self.store.complete_agent_tool_invocation(
                claim,
                receipt,
                result_artifact_ref=result_ref,
                now=self._now(),
            )
        except AgentToolInvocationConflict:
            current = self.store.get_agent_tool_invocation(
                claim.invocation.invocation_id
            )
            now = self._now()
            if (
                current is not None
                and current.status is AttemptStatus.RUNNING
                and current.lease_expires_at is not None
                and current.lease_expires_at <= now
            ):
                current = self.store.expire_agent_tool_invocation(
                    current.invocation_id,
                    invocation_request.invocation_digest,
                    now=now,
                )
                return self._replay(current)
            if current is not None and current.is_terminal:
                return self._replay(current)
            raise AgentToolExecutionError(
                "agent_tool_store_unavailable"
            ) from None
        return self._replay(stored)

    def _tool_receipt(
        self,
        prepared: _PreparedAgentToolExecution,
        *,
        status: AttemptStatus,
        sandbox_receipt: SandboxReceipt | None,
        verification: ToolReceiptVerification,
        error_code: str | None,
        absence_reason: str | None = None,
    ) -> ToolReceipt:
        action = prepared.action
        return ToolReceipt(
            run_id=action.run_id,
            node_id=action.node_id,
            attempt_id=action.attempt_id,
            tool_name=action.tool_name,
            effect_class=action.effect_class,
            attempt_status=status,
            args_digest=action.args_digest,
            action_digest=action.action_digest,
            execution_binding_digest=action.execution_binding_digest,
            operation_key_digest=action.operation_key_digest,
            idempotency_key_digest=action.idempotency_key_digest,
            policy_version=self.policy.policy_version,
            policy_digest=self.policy.policy_digest,
            profile_id=prepared.spec.profile.profile_id,
            profile_digest=prepared.spec.profile.profile_digest,
            verification=verification,
            sandbox_receipt=(
                None if sandbox_receipt is None else sandbox_receipt.to_dict()
            ),
            sandbox_receipt_absence_reason=absence_reason,
            error_code=error_code,
        )

    def _result_ref(
        self,
        request: AgentToolInvocationRequest,
        record: AgentToolInvocationRecord,
        receipt: SandboxReceipt,
    ) -> ArtifactRef:
        candidates = tuple(
            ref
            for ref in receipt.output_artifact_refs
            if ref.kind is ArtifactKind.TOOL_RESULT
            and ref.media_type == AGENT_TOOL_RESULT_MEDIA_TYPE
        )
        if len(candidates) != 1:
            raise AgentToolExecutionError("agent_tool_artifact_invalid")
        ref = candidates[0]
        if (
            ref.producer_run_id != record.run_id
            or ref.producer_node_id != record.child_node_id
            or ref.producer_attempt_id != record.child_attempt_id
            or _sensitivity_rank(ref.sensitivity)
            < _sensitivity_rank(request.sensitivity)
        ):
            raise AgentToolExecutionError("agent_tool_artifact_invalid")
        try:
            AgentToolResultArtifactStore.load(self.result_store, ref)
        except AgentToolResultError:
            raise AgentToolExecutionError(
                "agent_tool_artifact_invalid"
            ) from None
        return ref

    def _verified_inputs(
        self,
        request: AgentToolInvocationRequest,
        refs: tuple[ArtifactRef, ...],
    ) -> tuple[ArtifactRef, ...]:
        if len(refs) > 64:
            raise AgentToolExecutionError("agent_tool_artifact_invalid")
        detached: list[ArtifactRef] = []
        for ref in refs:
            try:
                value = ArtifactRef.from_dict(ref.to_dict())
                valid = self._artifact_verifier(value)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                raise AgentToolExecutionError(
                    "agent_tool_artifact_invalid"
                ) from None
            if (
                valid is not True
                or value.metadata
                or value.producer_run_id not in {None, request.run_id}
                or (
                    value.producer_run_id is None
                    and (
                        value.producer_node_id is not None
                        or value.producer_attempt_id is not None
                    )
                )
            ):
                raise AgentToolExecutionError(
                    "agent_tool_artifact_invalid"
                )
            detached.append(value)
        return tuple(detached)

    def _materialized_script(
        self,
        request: AgentToolInvocationRequest,
        ref: ArtifactRef | None,
    ) -> tuple[ArtifactRef | None, bytes | None]:
        if ref is None:
            return None, None
        verified = self._verified_inputs(request, (ref,))[0]
        try:
            content = self._artifact_reader(verified)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise AgentToolExecutionError(
                "agent_tool_artifact_invalid"
            ) from None
        if (
            type(content) is not bytes
            or len(content) != verified.size
            or hashlib.sha256(content).hexdigest() != verified.sha256
        ):
            raise AgentToolExecutionError("agent_tool_artifact_invalid")
        return verified, content

    def _recover_running(
        self,
        request: AgentToolInvocationRequest,
        record: AgentToolInvocationRecord,
        *,
        now: float,
    ) -> AgentToolInvocationResult:
        if record.lease_expires_at is None or record.lease_expires_at > now:
            raise AgentToolExecutionError("agent_tool_active")
        try:
            expired = self.store.expire_agent_tool_invocation(
                record.invocation_id,
                request.invocation_digest,
                now=now,
            )
        except AgentToolInvocationConflict:
            raise AgentToolExecutionError("agent_tool_active") from None
        return self._replay(expired)

    def _replay(
        self,
        record: AgentToolInvocationRecord,
    ) -> AgentToolInvocationResult:
        receipt = self.store.get_tool_receipt(
            record.run_id,
            record.child_attempt_id,
        )
        if receipt is None:
            raise AgentToolExecutionError(
                "agent_tool_preflight_abandoned"
            )
        result_ref = record.result_artifact_ref
        if receipt.attempt_status is AttemptStatus.SUCCEEDED:
            if result_ref is None:
                raise AgentToolExecutionError(
                    "agent_tool_artifact_invalid"
                )
            try:
                AgentToolResultArtifactStore.load(
                    self.result_store,
                    result_ref,
                )
            except AgentToolResultError:
                raise AgentToolExecutionError(
                    "agent_tool_artifact_invalid"
                ) from None
        elif result_ref is not None:
            raise AgentToolExecutionError("agent_tool_artifact_invalid")
        return AgentToolInvocationResult(
            receipt=receipt,
            result_artifact_ref=result_ref,
        )

    def _abandon_scheduled(
        self,
        request: AgentToolInvocationRequest,
        record: AgentToolInvocationRecord,
    ) -> None:
        try:
            self.store.abandon_scheduled_agent_tool_invocation(
                record.invocation_id,
                request.invocation_digest,
                now=max(self._now(), record.updated_at),
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass

    def _cancellation_signal(
        self,
        request: AgentToolInvocationRequest,
        record: AgentToolInvocationRecord,
    ) -> CancellationSignal:
        try:
            run = self.store.get_run(record.run_id)
            parent_node = self.store.get_node(
                record.run_id,
                record.parent_node_id,
            )
            parent_attempt = self.store.get_attempt(
                record.parent_attempt_id
            )
            child = self.store.get_agent_tool_invocation(
                record.invocation_id
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return CancellationSignal.UNKNOWN
        if run is None:
            return CancellationSignal.UNKNOWN
        if run.status in {RunStatus.CANCELLING, RunStatus.CANCELLED}:
            return CancellationSignal.CANCEL_REQUESTED
        if (
            run.status is not RunStatus.RUNNING
            or parent_node is None
            or parent_node.status is not NodeStatus.RUNNING
            or parent_attempt is None
            or parent_attempt.status is not AttemptStatus.RUNNING
            or parent_attempt.metadata.get("request_hash")
            != request.request_digest
            or child is None
            or child.status is not AttemptStatus.RUNNING
            or child.invocation_digest != request.invocation_digest
            or child.lease_expires_at is None
            or child.lease_expires_at <= self._now()
        ):
            return CancellationSignal.UNKNOWN
        return CancellationSignal.CONTINUE

    def _resolve_control_plane_paths(
        self,
        explicit_roots: Iterable[str | Path],
    ) -> tuple[Path, ...]:
        candidates = [self.store.path]
        result_root = getattr(self.result_store, "root", None)
        if result_root is not None:
            candidates.append(Path(result_root))
        try:
            candidates.extend(Path(value) for value in explicit_roots)
        except TypeError:
            raise AgentToolExecutionError(
                "agent_tool_configuration_invalid"
            ) from None
        resolved: dict[str, Path] = {}
        for candidate in candidates:
            try:
                path = candidate.expanduser().resolve(strict=True)
            except OSError:
                raise AgentToolExecutionError(
                    "agent_tool_configuration_invalid"
                ) from None
            resolved[str(path)] = path
        return tuple(resolved[key] for key in sorted(resolved))

    def _validate_control_plane_isolation(
        self,
        spec: AgentToolExecutionSpec,
    ) -> None:
        try:
            agent_paths = [
                Path(value).expanduser().resolve(strict=True)
                for value in (*spec.profile.allowed_roots, spec.cwd)
            ]
        except OSError:
            raise AgentToolExecutionError(
                "agent_tool_configuration_invalid"
            ) from None
        for agent_path in agent_paths:
            for control_path in self._control_plane_paths:
                if (
                    agent_path == control_path
                    or agent_path in control_path.parents
                    or control_path in agent_path.parents
                ):
                    raise AgentToolExecutionError(
                        "agent_tool_configuration_invalid"
                    )

    def _fault(self, stage: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(stage)

    def _now(self) -> float:
        try:
            current = float(self._clock())
        except (TypeError, ValueError):
            raise AgentToolExecutionError(
                "agent_tool_execution_invalid"
            ) from None
        if not math.isfinite(current) or current < 0:
            raise AgentToolExecutionError(
                "agent_tool_execution_invalid"
            )
        return current


def _decision_digest(decision: PolicyDecision) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "schema_version": decision.schema_version,
                "outcome": decision.outcome.value,
                "action_digest": decision.action_digest,
                "policy_version": decision.policy_version,
                "reason_code": decision.reason_code,
                "matched_rule_ids": list(decision.matched_rule_ids),
            }
        )
    ).hexdigest()


def _sensitivity_rank(value: ArtifactSensitivity) -> int:
    return {
        ArtifactSensitivity.PUBLIC: 0,
        ArtifactSensitivity.INTERNAL: 1,
        ArtifactSensitivity.SENSITIVE: 2,
        ArtifactSensitivity.SECRET: 3,
    }[ArtifactSensitivity(value)]


__all__ = [
    "AgentToolExecutionError",
    "AgentToolExecutionSpec",
    "DurableAgentToolExecutor",
]
