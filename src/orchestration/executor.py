"""Trusted execution gate joining policy, approval, sandbox, and durability.

The executor is deliberately synchronous and local.  It never registers an
approval grant and it never calls a sandbox backend until a durable
``policy.decided`` authorization exists and ``attempt.started`` commits.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from .artifacts import ArtifactRef
from .models import AttemptStatus, EventRecord, IdempotencyStatus, NodeStatus, RunStatus
from .policy import (
    ActionRequest,
    ApprovalGrant,
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
    SandboxValidationError,
    build_execution_binding_digest,
)
from .scheduler import ActivityClaim, ActivityReceipt, DurableScheduler
from .store import DurableRunStore

_UNCERTAIN_EFFECTS = frozenset(
    {EffectClass.NON_IDEMPOTENT_WRITE, EffectClass.DESTRUCTIVE}
)
TOOL_RECEIPT_SCHEMA_VERSION = 1


class ActivityExecutorError(RuntimeError):
    """Base trusted-executor failure."""


class ActivityExecutionConflict(ActivityExecutorError):
    """The supplied claim or durable authorization is stale or inconsistent."""


class ArtifactReceiptError(ActivityExecutorError):
    """Sandbox output did not resolve to verified immutable artifacts."""


class ControlPlaneIsolationError(ActivityExecutionConflict):
    """Agent execution scope overlaps trusted durable control-plane storage."""


class ToolReceiptError(ActivityExecutorError, ValueError):
    """A durable ToolReceipt is malformed or does not prove its binding."""


class ToolReceiptVerification(StrEnum):
    """Strength of the outcome evidence, never an external-effect guarantee."""

    VERIFIED = "verified"
    INFERRED = "inferred"
    UNVERIFIED = "unverified"


@dataclass(frozen=True, slots=True)
class ToolReceipt:
    """Versioned, secret-free proof of one authorized Tool Activity."""

    run_id: str
    node_id: str
    attempt_id: str
    tool_name: str
    effect_class: EffectClass
    attempt_status: AttemptStatus
    args_digest: str
    action_digest: str
    execution_binding_digest: str
    operation_key_digest: str
    idempotency_key_digest: str
    policy_version: str
    policy_digest: str
    profile_id: str
    profile_digest: str
    verification: ToolReceiptVerification
    sandbox_receipt: Mapping[str, Any] | None = None
    sandbox_receipt_absence_reason: str | None = None
    error_code: str | None = None
    schema_version: int = TOOL_RECEIPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in (
            "run_id",
            "node_id",
            "attempt_id",
            "tool_name",
            "policy_version",
            "profile_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _receipt_text(getattr(self, field_name), field_name),
            )
        for field_name in (
            "args_digest",
            "action_digest",
            "execution_binding_digest",
            "operation_key_digest",
            "idempotency_key_digest",
            "policy_digest",
            "profile_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _receipt_digest(getattr(self, field_name), field_name),
            )
        if _policy_digest(self.policy_version) != self.policy_digest:
            raise ToolReceiptError(
                "ToolReceipt policy version does not match policy_digest"
            )
        try:
            object.__setattr__(
                self,
                "effect_class",
                EffectClass(self.effect_class),
            )
            object.__setattr__(
                self,
                "attempt_status",
                AttemptStatus(self.attempt_status),
            )
            object.__setattr__(
                self,
                "verification",
                ToolReceiptVerification(self.verification),
            )
        except ValueError as exc:
            raise ToolReceiptError("ToolReceipt enum is invalid") from exc
        if not self.attempt_status.is_terminal:
            raise ToolReceiptError("ToolReceipt requires a terminal Attempt status")
        if self.schema_version != TOOL_RECEIPT_SCHEMA_VERSION:
            raise ToolReceiptError("unsupported ToolReceipt schema_version")

        receipt = self.sandbox_receipt
        absence_reason = self.sandbox_receipt_absence_reason
        if receipt is None:
            if absence_reason is None:
                raise ToolReceiptError(
                    "ToolReceipt without SandboxReceipt requires an absence reason"
                )
            object.__setattr__(
                self,
                "sandbox_receipt_absence_reason",
                _receipt_text(
                    absence_reason,
                    "sandbox_receipt_absence_reason",
                    max_chars=128,
                ),
            )
            if self.verification is not ToolReceiptVerification.UNVERIFIED:
                raise ToolReceiptError(
                    "ToolReceipt without SandboxReceipt must be unverified"
                )
            if self.attempt_status is AttemptStatus.SUCCEEDED:
                raise ToolReceiptError(
                    "successful ToolReceipt requires a SandboxReceipt"
                )
        else:
            if absence_reason is not None:
                raise ToolReceiptError(
                    "ToolReceipt cannot contain a receipt and an absence reason"
                )
            try:
                normalized_receipt = SandboxReceipt.validate_serialized(receipt)
            except SandboxValidationError as exc:
                raise ToolReceiptError("SandboxReceipt payload is invalid") from exc
            if (
                normalized_receipt["action_digest"] != self.action_digest
                or normalized_receipt["policy_version"] != self.policy_version
                or normalized_receipt["profile_id"] != self.profile_id
                or normalized_receipt["profile_digest"] != self.profile_digest
            ):
                raise ToolReceiptError(
                    "SandboxReceipt does not match the ToolReceipt binding"
                )
            if (
                self.attempt_status is AttemptStatus.SUCCEEDED
                and normalized_receipt["outcome"] != SandboxOutcome.SUCCEEDED.value
            ):
                raise ToolReceiptError(
                    "successful ToolReceipt requires a successful SandboxReceipt"
                )
            object.__setattr__(self, "sandbox_receipt", normalized_receipt)

        if self.verification is ToolReceiptVerification.VERIFIED and (
            self.effect_class is not EffectClass.READ_ONLY
            or self.attempt_status is not AttemptStatus.SUCCEEDED
            or self.sandbox_receipt is None
            or self.sandbox_receipt.get("outcome")
            != SandboxOutcome.SUCCEEDED.value
        ):
            raise ToolReceiptError(
                "verified ToolReceipt is reserved for verified read-only success"
            )
        if self.error_code is not None:
            object.__setattr__(
                self,
                "error_code",
                _receipt_text(
                    self.error_code,
                    "error_code",
                    max_chars=128,
                ),
            )

    @property
    def sandbox_receipt_digest(self) -> str | None:
        if self.sandbox_receipt is None:
            return None
        return _canonical_digest(self.sandbox_receipt)

    @property
    def receipt_digest(self) -> str:
        return _canonical_digest(self.to_dict())

    def validate_binding(
        self,
        *,
        action: ActionRequest,
        attempt_status: AttemptStatus,
        policy_digest: str,
        profile: SandboxProfile,
    ) -> None:
        if (
            self.run_id != action.run_id
            or self.node_id != action.node_id
            or self.attempt_id != action.attempt_id
            or self.tool_name != action.tool_name
            or self.effect_class is not action.effect_class
            or self.attempt_status is not attempt_status
            or self.args_digest != action.args_digest
            or self.action_digest != action.action_digest
            or self.execution_binding_digest
            != action.execution_binding_digest
            or self.operation_key_digest != action.operation_key_digest
            or self.idempotency_key_digest != action.idempotency_key_digest
            or self.policy_digest != policy_digest
            or self.profile_id != profile.profile_id
            or self.profile_digest != profile.profile_digest
        ):
            raise ActivityExecutionConflict(
                "durable ToolReceipt does not match this execution"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": "tool_receipt",
            "run_id": self.run_id,
            "node_id": self.node_id,
            "attempt_id": self.attempt_id,
            "tool_name": self.tool_name,
            "effect_class": self.effect_class.value,
            "attempt_status": self.attempt_status.value,
            "args_digest": self.args_digest,
            "action_digest": self.action_digest,
            "execution_binding_digest": self.execution_binding_digest,
            "operation_key_digest": self.operation_key_digest,
            "idempotency_key_digest": self.idempotency_key_digest,
            "policy_version": self.policy_version,
            "policy_digest": self.policy_digest,
            "profile_id": self.profile_id,
            "profile_digest": self.profile_digest,
            "verification": self.verification.value,
            "sandbox_receipt": (
                None
                if self.sandbox_receipt is None
                else dict(self.sandbox_receipt)
            ),
            "sandbox_receipt_absence_reason": (
                self.sandbox_receipt_absence_reason
            ),
            "error_code": self.error_code,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ToolReceipt":
        if not isinstance(payload, Mapping):
            raise ToolReceiptError("ToolReceipt must be an object")
        required = {
            "schema_version",
            "kind",
            "run_id",
            "node_id",
            "attempt_id",
            "tool_name",
            "effect_class",
            "attempt_status",
            "args_digest",
            "action_digest",
            "execution_binding_digest",
            "operation_key_digest",
            "idempotency_key_digest",
            "policy_version",
            "policy_digest",
            "profile_id",
            "profile_digest",
            "verification",
            "sandbox_receipt",
            "sandbox_receipt_absence_reason",
            "error_code",
        }
        if set(payload) != required or payload.get("kind") != "tool_receipt":
            raise ToolReceiptError(
                "ToolReceipt contains unknown or missing fields"
            )
        return cls(
            schema_version=payload.get("schema_version"),
            run_id=payload.get("run_id"),
            node_id=payload.get("node_id"),
            attempt_id=payload.get("attempt_id"),
            tool_name=payload.get("tool_name"),
            effect_class=payload.get("effect_class"),
            attempt_status=payload.get("attempt_status"),
            args_digest=payload.get("args_digest"),
            action_digest=payload.get("action_digest"),
            execution_binding_digest=payload.get("execution_binding_digest"),
            operation_key_digest=payload.get("operation_key_digest"),
            idempotency_key_digest=payload.get("idempotency_key_digest"),
            policy_version=payload.get("policy_version"),
            policy_digest=payload.get("policy_digest"),
            profile_id=payload.get("profile_id"),
            profile_digest=payload.get("profile_digest"),
            verification=payload.get("verification"),
            sandbox_receipt=payload.get("sandbox_receipt"),
            sandbox_receipt_absence_reason=payload.get(
                "sandbox_receipt_absence_reason"
            ),
            error_code=payload.get("error_code"),
        )


@dataclass(frozen=True, slots=True)
class ActivityExecutionResult:
    attempt_status: AttemptStatus
    policy_event: EventRecord | None
    completion_event: EventRecord | None
    sandbox_outcome: str | None
    sandbox_receipt_digest: str | None
    tool_receipt_digest: str | None = None
    receipt_absence_reason: str | None = None
    replayed: bool = False

    @property
    def succeeded(self) -> bool:
        return self.attempt_status is AttemptStatus.SUCCEEDED


@dataclass(frozen=True, slots=True)
class PreparedActivityExecution:
    """Control-plane-only authorization prepared for one exact claim.

    The opaque executor binding deliberately makes preparations process-local.
    A controller restart must recover the durable RUNNING Attempt through the
    normal lease/effect-class recovery path; it must not reconstruct authority
    from an untrusted remote completion.
    """

    claim: ActivityClaim
    action: ActionRequest
    request: ExecutionRequest
    profile: SandboxProfile
    policy_event: EventRecord
    policy_digest: str
    _executor_binding: object = field(
        repr=False,
        compare=False,
    )


@dataclass(frozen=True, slots=True)
class PreauthorizedActivityExecution:
    """Read-only exact policy intent prepared before a durable claim exists."""

    claim: ActivityClaim
    action: ActionRequest
    profile: SandboxProfile
    argv: tuple[str, ...]
    cwd: str
    limits: ResourceLimits
    input_artifact_refs: tuple[ArtifactRef, ...]
    script_artifact_ref: ArtifactRef | None
    materialized_script: bytes | None = field(repr=False)
    environment: tuple[EnvironmentBinding, ...]
    decision: PolicyDecision
    policy_digest: str
    decision_digest: str
    approval_grant_digest: str | None
    _executor_binding: object = field(repr=False, compare=False)

    @property
    def durable_policy_binding(self) -> dict[str, str | None]:
        return {
            "outcome": self.decision.outcome.value,
            "reason_code": self.decision.reason_code,
            "action_digest": self.action.action_digest,
            "policy_digest": self.policy_digest,
            "profile_digest": self.profile.profile_digest,
            "decision_digest": self.decision_digest,
            "approval_grant_digest": self.approval_grant_digest,
        }


class DurableApprovalRegistry:
    """Trusted control-plane ingress for durable single-use approval grants.

    This object must not be passed to Activity implementations.  The executor
    accepts grants but intentionally has no issuance method or registry
    reference.  Actor text is checked here and never written to Domain Events.
    """

    def __init__(
        self,
        store: DurableRunStore,
        *,
        trusted_actors: Iterable[str],
        clock: Callable[[], float] | None = None,
    ) -> None:
        actors = frozenset(str(actor).strip() for actor in trusted_actors)
        if not actors or "" in actors:
            raise ValueError("trusted_actors must contain bounded identities")
        self._store = store
        self._trusted_actors = actors
        self._clock = clock or time.time

    def register_issued(self, grant: ApprovalGrant) -> EventRecord:
        if not isinstance(grant, ApprovalGrant):
            raise TypeError("grant must be ApprovalGrant")
        if grant.actor not in self._trusted_actors:
            raise ActivityExecutionConflict("approval actor is not trusted")
        return self._store.register_approval_grant(
            grant.run_id,
            grant_binding_digest=_approval_execution_binding_digest(grant),
            action_digest=grant.action_digest,
            policy_digest=_policy_digest(grant.policy_version),
            expires_at=grant.expires_at,
            now=_finite_now(self._clock()),
        )

    def reject_requested(
        self,
        run_id: str,
        attempt_id: str,
        *,
        actor: str,
        reason_code: str = "approval_rejected",
    ) -> EventRecord:
        """Reject a durable request through the same trusted actor boundary."""

        if actor not in self._trusted_actors:
            raise ActivityExecutionConflict("approval actor is not trusted")
        return self._store.reject_activity_approval(
            run_id,
            attempt_id,
            reason_code=reason_code,
            now=_finite_now(self._clock()),
        )


class TrustedActivityExecutor:
    """Execute one already-claimed Tool Activity through every durable gate."""

    def __init__(
        self,
        scheduler: DurableScheduler,
        policy: PolicyEngine,
        sandbox: SandboxDispatcher,
        *,
        artifact_verifier: Callable[[ArtifactRef], bool],
        artifact_reader: Callable[[ArtifactRef], bytes] | None = None,
        clock: Callable[[], float] | None = None,
        fault_hook: Callable[[str], None] | None = None,
        control_plane_roots: Iterable[str | Path] = (),
    ) -> None:
        if sandbox.policy_version != policy.policy_version:
            raise ValueError("sandbox and policy versions must match")
        if not callable(artifact_verifier):
            raise TypeError("artifact_verifier must be callable")
        if artifact_reader is not None and not callable(artifact_reader):
            raise TypeError("artifact_reader must be callable")
        self.scheduler = scheduler
        self.store = scheduler.store
        self.policy = policy
        self.sandbox = sandbox
        self._artifact_verifier = artifact_verifier
        self._artifact_reader = artifact_reader
        self._clock = clock or time.time
        self._fault_hook = fault_hook
        self._preparation_binding = object()
        self._control_plane_paths = self._resolve_control_plane_paths(
            control_plane_roots,
        )

    def build_action(
        self,
        claim: ActivityClaim,
        *,
        argv: tuple[str, ...],
        cwd: str,
        profile: SandboxProfile,
        capabilities: Iterable[Capability | str] = (),
        resource_locks: Iterable[str] = (),
        sensitive_keys: Iterable[str] = (),
        limits: ResourceLimits | None = None,
        input_artifact_refs: tuple[ArtifactRef, ...] = (),
        script_artifact_ref: ArtifactRef | None = None,
        environment: tuple[EnvironmentBinding, ...] = (),
    ) -> ActionRequest:
        self._validate_claim_binding(claim)
        (
            action,
            _verified_inputs,
            _verified_script_ref,
            _materialized_script,
        ) = self._build_execution_intent(
            claim,
            argv=argv,
            cwd=cwd,
            profile=profile,
            capabilities=capabilities,
            resource_locks=resource_locks,
            sensitive_keys=sensitive_keys,
            limits=limits,
            input_artifact_refs=input_artifact_refs,
            script_artifact_ref=script_artifact_ref,
            environment=environment,
        )
        return action

    def _build_execution_intent(
        self,
        claim: ActivityClaim,
        *,
        argv: tuple[str, ...],
        cwd: str,
        profile: SandboxProfile,
        capabilities: Iterable[Capability | str] = (),
        resource_locks: Iterable[str] = (),
        sensitive_keys: Iterable[str] = (),
        limits: ResourceLimits | None = None,
        input_artifact_refs: tuple[ArtifactRef, ...] = (),
        script_artifact_ref: ArtifactRef | None = None,
        environment: tuple[EnvironmentBinding, ...] = (),
    ) -> tuple[
        ActionRequest,
        tuple[ArtifactRef, ...],
        ArtifactRef | None,
        bytes | None,
    ]:
        self._validate_control_plane_isolation(profile, cwd)
        if claim.activity_kind != "tool":
            raise ActivityExecutionConflict("trusted executor only accepts Tool claims")
        tool_name = claim.config.get("tool")
        arguments = claim.config.get("arguments", {})
        if not isinstance(tool_name, str) or not tool_name:
            raise ActivityExecutionConflict("Tool claim has no bounded tool identity")
        if not isinstance(arguments, Mapping):
            raise ActivityExecutionConflict("Tool claim arguments must be an object")
        tool_policy = self.policy.tool_policy(tool_name)
        requires_script_artifact = (
            False
            if tool_policy is None
            else tool_policy.requires_script_artifact
        )
        capability_values = tuple(capabilities)
        resource_lock_values = tuple(resource_locks)
        sensitive_key_values = tuple(sensitive_keys)
        self._reject_sensitive_control_values(
            arguments,
            sensitive_key_values,
            argv=argv,
            cwd=cwd,
            resource_locks=resource_lock_values,
        )
        if tuple(sorted(set(resource_lock_values))) != claim.resource_keys:
            raise ActivityExecutionConflict(
                "execution resource locks do not match the durable Workflow claim"
            )
        environment_values = tuple(environment)
        verified_inputs = self._verify_input_refs(
            claim,
            tuple(input_artifact_refs),
        )
        verified_script_ref, materialized_script = self._verify_script_ref(
            claim,
            script_artifact_ref,
        )
        effective_limits = limits or profile.limits
        execution_binding_digest = build_execution_binding_digest(
            argv=argv,
            cwd=cwd,
            profile=profile,
            limits=effective_limits,
            input_artifact_refs=verified_inputs,
            script_artifact_ref=verified_script_ref,
            operation_key=claim.operation_key,
            idempotency_key=claim.idempotency_key,
            environment=environment_values,
            capabilities=capability_values,
            resource_locks=resource_lock_values,
        )
        action = ActionRequest.from_args(
            run_id=claim.run_id,
            node_id=claim.node_id,
            attempt_id=claim.attempt_id,
            tool_name=tool_name,
            args=arguments,
            execution_binding_digest=execution_binding_digest,
            operation_key=claim.operation_key,
            idempotency_key=claim.idempotency_key,
            script_artifact_ref=verified_script_ref,
            requires_script_artifact=requires_script_artifact,
            effect_class=EffectClass(claim.effect_class),
            capabilities=capability_values,
            resource_locks=resource_lock_values,
            sensitive_keys=sensitive_key_values,
        )
        return action, verified_inputs, verified_script_ref, materialized_script

    def _resolve_control_plane_paths(
        self,
        explicit_roots: Iterable[str | Path],
    ) -> tuple[Path, ...]:
        candidates: list[Path] = [self.store.path]
        for callback in (self._artifact_verifier, self._artifact_reader):
            owner = getattr(callback, "__self__", None)
            root = getattr(owner, "root", None)
            if root is not None:
                candidates.append(Path(root))
        for backend in getattr(self.sandbox, "_backends", ()):
            root = getattr(getattr(backend, "artifacts", None), "root", None)
            if root is not None:
                candidates.append(Path(root))
        try:
            candidates.extend(Path(value) for value in explicit_roots)
        except TypeError as exc:
            raise TypeError("control_plane_roots must be iterable paths") from exc

        resolved: dict[str, Path] = {}
        for candidate in candidates:
            try:
                path = candidate.expanduser().resolve(strict=True)
            except OSError as exc:
                raise ControlPlaneIsolationError(
                    "control-plane path is missing"
                ) from exc
            resolved[str(path)] = path
        return tuple(resolved[key] for key in sorted(resolved))

    def _validate_control_plane_isolation(
        self,
        profile: SandboxProfile,
        cwd: str,
    ) -> None:
        try:
            agent_paths = [
                Path(value).expanduser().resolve(strict=True)
                for value in (*profile.allowed_roots, cwd)
            ]
        except OSError as exc:
            raise ControlPlaneIsolationError(
                "agent execution path is missing"
            ) from exc
        for agent_path in agent_paths:
            for control_path in self._control_plane_paths:
                if (
                    agent_path == control_path
                    or agent_path in control_path.parents
                    or control_path in agent_path.parents
                ):
                    raise ControlPlaneIsolationError(
                        "agent scope overlaps trusted control-plane storage"
                    )

    def prepare_execution(
        self,
        claim: ActivityClaim,
        *,
        argv: tuple[str, ...],
        cwd: str,
        profile: SandboxProfile,
        approval_grant: ApprovalGrant | None = None,
        capabilities: Iterable[Capability | str] = (),
        resource_locks: Iterable[str] = (),
        sensitive_keys: Iterable[str] = (),
        limits: ResourceLimits | None = None,
        input_artifact_refs: tuple[ArtifactRef, ...] = (),
        script_artifact_ref: ArtifactRef | None = None,
        environment: tuple[EnvironmentBinding, ...] = (),
    ) -> PreparedActivityExecution | ActivityExecutionResult:
        """Persist policy authorization without starting the sandbox.

        A successful preparation remains control-plane local.  Callers must
        durably start the exact claim before passing a trusted SandboxReceipt
        to :meth:`complete_prepared`.
        """

        self._validate_claim_binding(claim)
        capability_values = tuple(capabilities)
        resource_lock_values = tuple(resource_locks)
        input_refs = tuple(input_artifact_refs)
        environment_values = tuple(environment)
        effective_limits = limits or profile.limits
        (
            action,
            verified_inputs,
            verified_script_ref,
            materialized_script,
        ) = self._build_execution_intent(
            claim,
            argv=argv,
            cwd=cwd,
            profile=profile,
            capabilities=capability_values,
            resource_locks=resource_lock_values,
            sensitive_keys=sensitive_keys,
            limits=effective_limits,
            input_artifact_refs=input_refs,
            script_artifact_ref=script_artifact_ref,
            environment=environment_values,
        )
        policy_digest = _policy_digest(self.policy.policy_version)
        profile_digest = profile.profile_digest
        current_attempt = self.store.get_attempt(claim.attempt_id)
        assert current_attempt is not None
        if current_attempt.status.is_terminal:
            policy_event = self.store.get_activity_policy_event(
                claim.run_id,
                claim.attempt_id,
            )
            if policy_event is None:
                raise ActivityExecutionConflict(
                    "terminal Activity has no durable policy binding"
                )
            self._validate_policy_binding(
                policy_event,
                claim=claim,
                action=action,
                policy_digest=policy_digest,
                profile_digest=profile_digest,
                require_allow=False,
            )
            completion_event, tool_receipt = self._terminal_tool_receipt(
                claim,
                action=action,
                attempt_status=current_attempt.status,
                policy_digest=policy_digest,
                profile=profile,
            )
            sandbox_outcome = (
                None
                if tool_receipt is None
                or tool_receipt.sandbox_receipt is None
                else str(tool_receipt.sandbox_receipt["outcome"])
            )
            return ActivityExecutionResult(
                attempt_status=current_attempt.status,
                policy_event=policy_event,
                completion_event=completion_event,
                sandbox_outcome=sandbox_outcome,
                sandbox_receipt_digest=(
                    None
                    if tool_receipt is None
                    else tool_receipt.sandbox_receipt_digest
                ),
                tool_receipt_digest=(
                    None
                    if tool_receipt is None
                    else tool_receipt.receipt_digest
                ),
                receipt_absence_reason=(
                    (
                        "policy_denied_before_backend"
                        if completion_event is None
                        else "cancel_confirmed_by_scheduler"
                    )
                    if tool_receipt is None
                    else tool_receipt.sandbox_receipt_absence_reason
                ),
                replayed=True,
            )
        if current_attempt.status is AttemptStatus.RUNNING:
            raise ActivityExecutionConflict(
                "Activity is already RUNNING; recovery must resolve its outcome"
            )
        if current_attempt.status is not AttemptStatus.CLAIMED:
            raise ActivityExecutionConflict("Activity claim is not executable")

        decision, approval_identity = self._authorize_decision(
            action,
            approval_grant,
        )
        decision_digest = _decision_digest(decision)

        existing = self.store.get_activity_policy_event(
            claim.run_id,
            claim.attempt_id,
        )
        if existing is not None:
            self._validate_policy_binding(
                existing,
                claim=claim,
                action=action,
                policy_digest=policy_digest,
                profile_digest=profile_digest,
                require_allow=True,
            )
            decision = PolicyDecision(
                outcome=PolicyOutcome.ALLOW,
                action_digest=action.action_digest,
                policy_version=self.policy.policy_version,
                reason_code=str(existing.payload["reason_code"]),
            )
        else:
            policy_event = self.store.commit_activity_policy_decision(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
                claim.request_hash,
                claim.worker_id,
                claim_token=claim.claim_token,
                outcome=decision.outcome.value,
                reason_code=decision.reason_code,
                action_digest=action.action_digest,
                policy_digest=policy_digest,
                profile_digest=profile_digest,
                decision_digest=decision_digest,
                approval_grant_digest=approval_identity,
                now=self._now(),
            )
            if decision.outcome is not PolicyOutcome.ALLOW:
                suspended_or_rejected = self.store.get_attempt(claim.attempt_id)
                assert suspended_or_rejected is not None
                self.scheduler.reconcile(claim.run_id)
                return ActivityExecutionResult(
                    attempt_status=suspended_or_rejected.status,
                    policy_event=policy_event,
                    completion_event=(
                        None
                        if decision.outcome
                        is PolicyOutcome.REQUIRE_APPROVAL
                        else policy_event
                    ),
                    sandbox_outcome=None,
                    sandbox_receipt_digest=None,
                    receipt_absence_reason=(
                        "approval_required_before_backend"
                        if decision.outcome
                        is PolicyOutcome.REQUIRE_APPROVAL
                        else "policy_denied_before_backend"
                    ),
                )
            existing = policy_event

        request = ExecutionRequest(
            action=action,
            policy_decision=decision,
            argv=argv,
            operation_key=claim.operation_key,
            idempotency_key=claim.idempotency_key,
            cwd=cwd,
            limits=effective_limits,
            input_artifact_refs=verified_inputs,
            script_artifact_ref=verified_script_ref,
            materialized_script=materialized_script,
            environment=environment_values,
            cancellation_probe=self._durable_cancellation_probe(claim),
        )
        request = request.validated_for_profile(profile)
        self._fault("authorization.committed")
        return PreparedActivityExecution(
            claim=claim,
            action=action,
            request=request,
            profile=profile,
            policy_event=existing,
            policy_digest=policy_digest,
            _executor_binding=self._preparation_binding,
        )

    def preauthorize_execution(
        self,
        claim: ActivityClaim,
        *,
        argv: tuple[str, ...],
        cwd: str,
        profile: SandboxProfile,
        approval_grant: ApprovalGrant | None = None,
        capabilities: Iterable[Capability | str] = (),
        resource_locks: Iterable[str] = (),
        sensitive_keys: Iterable[str] = (),
        limits: ResourceLimits | None = None,
        input_artifact_refs: tuple[ArtifactRef, ...] = (),
        script_artifact_ref: ArtifactRef | None = None,
        environment: tuple[EnvironmentBinding, ...] = (),
    ) -> PreauthorizedActivityExecution:
        """Evaluate one exact unclaimed intent without Domain mutation.

        Artifact verification, approval issuance checks, and policy evaluation
        happen here, outside the Store write transaction.  Only ALLOW may
        proceed to the candidate-CAS path; deny/approval-needed outcomes cannot
        reserve an Attempt or lease.
        """

        if claim.claim_token or claim.fencing_token != 0:
            raise ActivityExecutionConflict(
                "preauthorization requires an unclaimed Activity candidate"
            )
        capability_values = tuple(capabilities)
        resource_lock_values = tuple(resource_locks)
        environment_values = tuple(environment)
        effective_limits = limits or profile.limits
        (
            action,
            verified_inputs,
            verified_script_ref,
            materialized_script,
        ) = self._build_execution_intent(
            claim,
            argv=argv,
            cwd=cwd,
            profile=profile,
            capabilities=capability_values,
            resource_locks=resource_lock_values,
            sensitive_keys=sensitive_keys,
            limits=effective_limits,
            input_artifact_refs=tuple(input_artifact_refs),
            script_artifact_ref=script_artifact_ref,
            environment=environment_values,
        )
        decision, approval_identity = self._authorize_decision(
            action,
            approval_grant,
        )
        if decision.outcome is PolicyOutcome.DENY:
            raise ActivityExecutionConflict(
                "remote candidate policy did not authorize execution"
            )
        return PreauthorizedActivityExecution(
            claim=claim,
            action=action,
            profile=profile,
            argv=tuple(argv),
            cwd=cwd,
            limits=effective_limits,
            input_artifact_refs=verified_inputs,
            script_artifact_ref=verified_script_ref,
            materialized_script=materialized_script,
            environment=environment_values,
            decision=decision,
            policy_digest=_policy_digest(self.policy.policy_version),
            decision_digest=_decision_digest(decision),
            approval_grant_digest=approval_identity,
            _executor_binding=self._preparation_binding,
        )

    def activate_preauthorized(
        self,
        preview: PreauthorizedActivityExecution,
        claim: ActivityClaim,
        policy_event: EventRecord,
    ) -> PreparedActivityExecution:
        """Bind a consumed candidate ticket to its exact durable claim."""

        if (
            not isinstance(preview, PreauthorizedActivityExecution)
            or preview._executor_binding is not self._preparation_binding
            or _preclaim_identity(preview.claim) != _preclaim_identity(claim)
            or preview.claim.worker_id != claim.worker_id
        ):
            raise ActivityExecutionConflict(
                "preauthorization does not match the durable claim"
            )
        self._validate_claim_binding(claim)
        self._validate_policy_binding(
            policy_event,
            claim=claim,
            action=preview.action,
            policy_digest=preview.policy_digest,
            profile_digest=preview.profile.profile_digest,
            require_allow=True,
        )
        if (
            policy_event.payload.get("decision_digest")
            != preview.decision_digest
            or policy_event.payload.get("approval_grant_digest")
            != preview.approval_grant_digest
        ):
            raise ActivityExecutionConflict(
                "durable policy event does not match preauthorization"
            )
        request = ExecutionRequest(
            action=preview.action,
            policy_decision=preview.decision,
            argv=preview.argv,
            operation_key=claim.operation_key,
            idempotency_key=claim.idempotency_key,
            cwd=preview.cwd,
            limits=preview.limits,
            input_artifact_refs=preview.input_artifact_refs,
            script_artifact_ref=preview.script_artifact_ref,
            materialized_script=preview.materialized_script,
            environment=preview.environment,
            cancellation_probe=self._durable_cancellation_probe(claim),
        ).validated_for_profile(preview.profile)
        return PreparedActivityExecution(
            claim=claim,
            action=preview.action,
            request=request,
            profile=preview.profile,
            policy_event=policy_event,
            policy_digest=preview.policy_digest,
            _executor_binding=self._preparation_binding,
        )

    def execute(
        self,
        claim: ActivityClaim,
        *,
        argv: tuple[str, ...],
        cwd: str,
        profile: SandboxProfile,
        approval_grant: ApprovalGrant | None = None,
        capabilities: Iterable[Capability | str] = (),
        resource_locks: Iterable[str] = (),
        sensitive_keys: Iterable[str] = (),
        limits: ResourceLimits | None = None,
        input_artifact_refs: tuple[ArtifactRef, ...] = (),
        script_artifact_ref: ArtifactRef | None = None,
        environment: tuple[EnvironmentBinding, ...] = (),
    ) -> ActivityExecutionResult:
        """Execute at most once for the exact durable claim and fencing token."""

        prepared = self.prepare_execution(
            claim,
            argv=argv,
            cwd=cwd,
            profile=profile,
            approval_grant=approval_grant,
            capabilities=capabilities,
            resource_locks=resource_locks,
            sensitive_keys=sensitive_keys,
            limits=limits,
            input_artifact_refs=input_artifact_refs,
            script_artifact_ref=script_artifact_ref,
            environment=environment,
        )
        if isinstance(prepared, ActivityExecutionResult):
            return prepared
        self.scheduler.start_claim(claim)

        try:
            receipt = self.sandbox.dispatch(prepared.request, profile)
        except SandboxDispatchDenied as exc:
            return self._complete_without_backend(
                claim,
                prepared.policy_event,
                action=prepared.action,
                policy_digest=prepared.policy_digest,
                profile=profile,
                error_code=exc.reason_code,
            )
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            return self._complete_unknown_dispatch(
                claim,
                prepared.policy_event,
                action=prepared.action,
                policy_digest=prepared.policy_digest,
                profile=profile,
            )

        return self.complete_prepared(prepared, receipt)

    def complete_prepared(
        self,
        prepared: PreparedActivityExecution,
        receipt: SandboxReceipt,
        *,
        completion_evidence: Mapping[str, str] | None = None,
        claim: ActivityClaim | None = None,
    ) -> ActivityExecutionResult:
        """Verify and durably commit a trusted receipt for a preparation."""

        completion_claim = prepared.claim if claim is None else claim
        self._validate_prepared(prepared, completion_claim)
        if not isinstance(receipt, SandboxReceipt):
            raise ActivityExecutionConflict(
                "completion requires a trusted SandboxReceipt"
            )
        evidence = _completion_evidence(completion_evidence)
        self._fault("backend.returned")
        return self._complete_receipt(
            completion_claim,
            prepared.policy_event,
            action=prepared.action,
            policy_digest=prepared.policy_digest,
            profile=prepared.profile,
            receipt=receipt,
            completion_evidence=evidence,
        )

    def _validate_prepared(
        self,
        prepared: PreparedActivityExecution,
        claim: ActivityClaim,
    ) -> None:
        if (
            not isinstance(prepared, PreparedActivityExecution)
            or prepared._executor_binding is not self._preparation_binding
        ):
            raise ActivityExecutionConflict(
                "execution preparation does not belong to this controller"
            )
        if _prepared_claim_authority(prepared.claim) != (
            _prepared_claim_authority(claim)
        ):
            raise ActivityExecutionConflict(
                "completion claim changed durable execution authority"
            )
        self._validate_claim_binding(claim)
        attempt = self.store.get_attempt(claim.attempt_id)
        if attempt is None or attempt.status is not AttemptStatus.RUNNING:
            raise ActivityExecutionConflict(
                "prepared completion requires the exact RUNNING Attempt"
            )
        current_policy = self.store.get_activity_policy_event(
            claim.run_id,
            claim.attempt_id,
        )
        if current_policy != prepared.policy_event:
            raise ActivityExecutionConflict(
                "durable policy authorization changed after preparation"
            )
        self._validate_policy_binding(
            prepared.policy_event,
            claim=claim,
            action=prepared.action,
            policy_digest=prepared.policy_digest,
            profile_digest=prepared.profile.profile_digest,
            require_allow=True,
        )
        if (
            prepared.request.action != prepared.action
            or prepared.request.policy_decision.outcome is not PolicyOutcome.ALLOW
            or prepared.request.policy_decision.action_digest
            != prepared.action.action_digest
            or prepared.request.validated_for_profile(prepared.profile)
            != prepared.request
        ):
            raise ActivityExecutionConflict(
                "prepared execution request binding is inconsistent"
            )

    def _authorize_decision(
        self,
        action: ActionRequest,
        grant: ApprovalGrant | None,
    ) -> tuple[PolicyDecision, str | None]:
        current = self.policy.evaluate(action)
        if current.outcome is not PolicyOutcome.REQUIRE_APPROVAL:
            return current, None
        if grant is None:
            return current, None
        reason = self._approval_rejection_reason(action, grant)
        if reason is not None:
            return (
                PolicyDecision(
                    outcome=PolicyOutcome.DENY,
                    action_digest=action.action_digest,
                    policy_version=self.policy.policy_version,
                    reason_code=reason,
                    matched_rule_ids=current.matched_rule_ids,
                ),
                None,
            )
        decision = PolicyDecision(
            outcome=PolicyOutcome.ALLOW,
            action_digest=action.action_digest,
            policy_version=self.policy.policy_version,
            reason_code="approval_consumed",
            matched_rule_ids=current.matched_rule_ids,
            approval_id=grant.approval_id,
            approved_by=grant.actor,
        )
        return (
            decision,
            _approval_execution_binding_digest(grant),
        )

    def _approval_rejection_reason(
        self,
        action: ActionRequest,
        grant: ApprovalGrant,
    ) -> str | None:
        if grant.action_digest != action.action_digest:
            return "approval_action_mismatch"
        if grant.run_id != action.run_id or grant.node_id != action.node_id:
            return "approval_scope_mismatch"
        if grant.policy_version != self.policy.policy_version:
            return "approval_policy_version_mismatch"
        if grant.expires_at <= self._now():
            return "approval_expired"
        if not self.store.verify_registered_approval(
            action.run_id,
            grant_binding_digest=_approval_execution_binding_digest(grant),
            action_digest=action.action_digest,
            policy_digest=_policy_digest(self.policy.policy_version),
            now=self._now(),
        ):
            return "approval_unissued"
        return None

    @staticmethod
    def _validate_policy_binding(
        event: EventRecord,
        *,
        claim: ActivityClaim,
        action: ActionRequest,
        policy_digest: str,
        profile_digest: str,
        require_allow: bool,
    ) -> None:
        payload = event.payload
        if (
            payload.get("kind") != "activity_authorization"
            or (
                require_allow
                and payload.get("outcome") != PolicyOutcome.ALLOW.value
            )
            or payload.get("outcome")
            not in {PolicyOutcome.ALLOW.value, PolicyOutcome.DENY.value}
            or payload.get("action_digest") != action.action_digest
            or payload.get("policy_digest") != policy_digest
            or payload.get("profile_digest") != profile_digest
            or payload.get("claim_token_digest")
            != _text_digest(claim.claim_token)
        ):
            raise ActivityExecutionConflict(
                "durable policy authorization does not match this execution"
            )

    def _terminal_tool_receipt(
        self,
        claim: ActivityClaim,
        *,
        action: ActionRequest,
        attempt_status: AttemptStatus,
        policy_digest: str,
        profile: SandboxProfile,
    ) -> tuple[EventRecord | None, ToolReceipt | None]:
        terminal_types = {
            f"attempt.{AttemptStatus.SUCCEEDED.value}",
            f"attempt.{AttemptStatus.FAILED.value}",
            f"attempt.{AttemptStatus.TIMED_OUT.value}",
            f"attempt.{AttemptStatus.CANCELLED.value}",
            f"attempt.{AttemptStatus.ABANDONED.value}",
            f"attempt.{AttemptStatus.OUTCOME_UNKNOWN.value}",
        }
        completion = next(
            (
                event
                for event in reversed(self.store.list_events(claim.run_id))
                if event.attempt_id == claim.attempt_id
                and event.event_type in terminal_types
            ),
            None,
        )
        if completion is None:
            policy_event = self.store.get_activity_policy_event(
                claim.run_id,
                claim.attempt_id,
            )
            if (
                policy_event is not None
                and policy_event.payload.get("outcome")
                == PolicyOutcome.DENY.value
            ):
                return None, None
            raise ActivityExecutionConflict(
                "terminal Tool Activity has no terminal completion Event"
            )
        raw_receipt = completion.payload.get("tool_receipt")
        try:
            receipt = ToolReceipt.from_dict(raw_receipt)
        except (ToolReceiptError, TypeError, ValueError) as exc:
            raise ActivityExecutionConflict(
                "terminal Tool Activity has no valid durable ToolReceipt"
            ) from exc
        receipt.validate_binding(
            action=action,
            attempt_status=attempt_status,
            policy_digest=policy_digest,
            profile=profile,
        )
        if (
            completion.payload.get("receipt_digest") != receipt.receipt_digest
            or completion.payload.get("tool_receipt_digest")
            != receipt.receipt_digest
            or completion.payload.get("sandbox_receipt_digest")
            != receipt.sandbox_receipt_digest
            or completion.payload.get("sandbox_receipt_absence_reason")
            != receipt.sandbox_receipt_absence_reason
        ):
            raise ActivityExecutionConflict(
                "terminal ToolReceipt digest metadata is inconsistent"
            )
        return completion, receipt

    def _verify_input_refs(
        self,
        claim: ActivityClaim,
        refs: tuple[ArtifactRef, ...],
    ) -> tuple[ArtifactRef, ...]:
        values = tuple(refs)
        if len(values) > 64:
            raise ArtifactReceiptError("input exceeds 64 ArtifactRefs")
        for ref in values:
            if not isinstance(ref, ArtifactRef):
                raise ArtifactReceiptError(
                    "input_artifact_refs must contain ArtifactRefs"
                )
            if ref.metadata:
                raise ArtifactReceiptError(
                    "input ArtifactRef metadata must be empty at the Event boundary"
                )
            if ref.producer_run_id not in {None, claim.run_id}:
                raise ArtifactReceiptError(
                    "input ArtifactRef producer belongs to another Run"
                )
            if ref.producer_run_id is None and (
                ref.producer_node_id is not None
                or ref.producer_attempt_id is not None
            ):
                raise ArtifactReceiptError(
                    "input ArtifactRef has an incomplete producer scope"
                )
            try:
                verified = self._artifact_verifier(ref)
            except Exception as exc:
                raise ArtifactReceiptError(
                    "input ArtifactRef verification failed"
                ) from exc
            if verified is not True:
                raise ArtifactReceiptError(
                    "input ArtifactRef bytes failed integrity validation"
                )
        return values

    def _verify_script_ref(
        self,
        claim: ActivityClaim,
        ref: ArtifactRef | None,
    ) -> tuple[ArtifactRef | None, bytes | None]:
        if ref is None:
            return None, None
        try:
            detached = ArtifactRef.from_dict(ref.to_dict())
        except Exception as exc:
            raise ArtifactReceiptError("script ArtifactRef is malformed") from exc
        verified = self._verify_input_refs(claim, (detached,))
        if self._artifact_reader is None:
            raise ArtifactReceiptError(
                "script Artifact requires a trusted artifact_reader"
            )
        try:
            content = self._artifact_reader(verified[0])
        except Exception as exc:
            raise ArtifactReceiptError(
                "script Artifact materialization failed"
            ) from exc
        if not isinstance(content, bytes):
            raise ArtifactReceiptError(
                "script Artifact materialization must return immutable bytes"
            )
        try:
            materialized_ref = ArtifactRef.from_dict(verified[0].to_dict())
        except Exception as exc:
            raise ArtifactReceiptError(
                "script ArtifactRef changed during materialization"
            ) from exc
        if (
            len(content) != materialized_ref.size
            or hashlib.sha256(content).hexdigest() != materialized_ref.sha256
        ):
            raise ArtifactReceiptError(
                "materialized script bytes do not match the verified ArtifactRef"
            )
        return materialized_ref, content

    @staticmethod
    def _reject_sensitive_control_values(
        arguments: Mapping[str, Any],
        sensitive_keys: tuple[str, ...],
        *,
        argv: tuple[str, ...],
        cwd: str,
        resource_locks: tuple[str, ...],
    ) -> None:
        sensitive_values = sensitive_argument_bytes(arguments, sensitive_keys)
        if not sensitive_values:
            return
        controls: list[bytes] = []
        controls.extend(
            item.encode("utf-8")
            for item in argv
            if isinstance(item, str)
        )
        if isinstance(cwd, str):
            controls.append(cwd.encode("utf-8"))
        controls.extend(
            item.encode("utf-8")
            for item in resource_locks
            if isinstance(item, str)
        )
        if any(
            sensitive in control
            for sensitive in sensitive_values
            for control in controls
        ):
            raise ActivityExecutionConflict(
                "sensitive argument values are forbidden in execution control fields"
            )

    def _validate_claim_binding(self, claim: ActivityClaim) -> None:
        attempt = self.store.get_attempt(claim.attempt_id)
        record = (
            None
            if attempt is None
            else self.store.get_idempotency(
                claim.run_id,
                attempt.idempotency_key,
            )
        )
        if (
            attempt is None
            or attempt.run_id != claim.run_id
            or attempt.node_id != claim.node_id
            or attempt.attempt_number != claim.attempt_number
            or attempt.metadata.get("request_hash") != claim.request_hash
            or attempt.metadata.get("operation_key") != claim.operation_key
            or claim.idempotency_key != claim.operation_key
            or tuple(attempt.metadata.get("resource_keys", ())) != claim.resource_keys
            or record is None
            or record.request_hash != claim.request_hash
            or record.owner_id != claim.worker_id
            or record.claim_token != claim.claim_token
            or record.claim_count != claim.fencing_token
        ):
            raise ActivityExecutionConflict(
                "claim does not match durable identity or fencing state"
            )
        if (
            not attempt.status.is_terminal
            and record.status is not IdempotencyStatus.IN_PROGRESS
        ):
            raise ActivityExecutionConflict("active claim has a completed receipt")

    def _durable_cancellation_probe(
        self,
        claim: ActivityClaim,
    ) -> CancellationProbe:
        """Build a trusted probe; Tool arguments never select its Store identity."""

        return CancellationProbe(
            run_id=claim.run_id,
            node_id=claim.node_id,
            attempt_id=claim.attempt_id,
            callback=lambda: self._read_durable_cancellation(claim),
        )

    def _read_durable_cancellation(
        self,
        claim: ActivityClaim,
    ) -> CancellationSignal:
        try:
            run = self.store.get_run(claim.run_id)
            node = self.store.get_node(claim.run_id, claim.node_id)
            attempt = self.store.get_attempt(claim.attempt_id)
            record = (
                None
                if attempt is None
                else self.store.get_idempotency(
                    claim.run_id,
                    attempt.idempotency_key,
                )
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return CancellationSignal.UNKNOWN
        if (
            run is None
            or node is None
            or attempt is None
            or attempt.run_id != claim.run_id
            or attempt.node_id != claim.node_id
            or attempt.attempt_number != claim.attempt_number
        ):
            return CancellationSignal.UNKNOWN
        if (
            run.status in {RunStatus.CANCELLING, RunStatus.CANCELLED}
            or node.status is NodeStatus.CANCELLED
            or attempt.status is AttemptStatus.CANCELLED
        ):
            return CancellationSignal.CANCEL_REQUESTED
        if (
            run.status.is_terminal
            or node.status.is_terminal
            or attempt.status.is_terminal
            or record is None
            or record.status is not IdempotencyStatus.IN_PROGRESS
            or record.request_hash != claim.request_hash
            or record.owner_id != claim.worker_id
            or record.claim_token != claim.claim_token
            or record.claim_count != claim.fencing_token
        ):
            return CancellationSignal.UNKNOWN
        return CancellationSignal.CONTINUE

    def _complete_receipt(
        self,
        claim: ActivityClaim,
        policy_event: EventRecord,
        *,
        action: ActionRequest,
        policy_digest: str,
        profile: SandboxProfile,
        receipt: SandboxReceipt,
        completion_evidence: Mapping[str, str] | None = None,
    ) -> ActivityExecutionResult:
        if (
            receipt.action_digest != action.action_digest
            or receipt.policy_version != self.policy.policy_version
            or receipt.profile_id != profile.profile_id
            or receipt.profile_digest != profile.profile_digest
        ):
            raise ActivityExecutionConflict(
                "SandboxReceipt does not match the authorized execution"
            )
        if receipt.outcome is SandboxOutcome.CANCELLED:
            return self._complete_cancelled(
                claim,
                policy_event,
                action=action,
                policy_digest=policy_digest,
                profile=profile,
                receipt=receipt,
                completion_evidence=completion_evidence,
            )
        if receipt.outcome is SandboxOutcome.CANCELLATION_UNKNOWN:
            return self._complete_failure(
                claim,
                policy_event,
                action=action,
                policy_digest=policy_digest,
                profile=profile,
                sandbox_receipt=receipt,
                sandbox_outcome=receipt.outcome.value,
                error_code="cancellation_probe_unavailable",
                uncertain=True,
                verification=ToolReceiptVerification.UNVERIFIED,
                completion_evidence=completion_evidence,
            )
        if receipt.outcome is SandboxOutcome.SUCCEEDED:
            try:
                activity_receipt = ActivityReceipt(
                    self._verify_output_refs(
                        claim,
                        receipt.output_artifact_refs,
                    )
                )
            except ArtifactReceiptError:
                return self._complete_failure(
                    claim,
                    policy_event,
                    action=action,
                    policy_digest=policy_digest,
                    profile=profile,
                    sandbox_receipt=receipt,
                    sandbox_outcome=receipt.outcome.value,
                    error_code="artifact_integrity",
                    uncertain=EffectClass(claim.effect_class) in _UNCERTAIN_EFFECTS,
                    verification=ToolReceiptVerification.UNVERIFIED,
                    completion_evidence=completion_evidence,
                )
            tool_receipt = self._build_tool_receipt(
                action=action,
                attempt_status=AttemptStatus.SUCCEEDED,
                policy_digest=policy_digest,
                profile=profile,
                sandbox_receipt=receipt,
                verification=(
                    ToolReceiptVerification.VERIFIED
                    if action.effect_class is EffectClass.READ_ONLY
                    else ToolReceiptVerification.INFERRED
                ),
                error_code=None,
            )
            record, completion = self.store.complete_activity(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
                claim.request_hash,
                claim.worker_id,
                claim_token=claim.claim_token,
                result=activity_receipt.to_dict(),
                event_payload=self._receipt_event_payload(
                    tool_receipt=tool_receipt,
                    sandbox_outcome=receipt.outcome.value,
                    completion_evidence=completion_evidence,
                ),
                now=self._now(),
            )
            del record
            self.scheduler.reconcile(claim.run_id)
            return ActivityExecutionResult(
                attempt_status=AttemptStatus.SUCCEEDED,
                policy_event=policy_event,
                completion_event=completion,
                sandbox_outcome=receipt.outcome.value,
                sandbox_receipt_digest=tool_receipt.sandbox_receipt_digest,
                tool_receipt_digest=tool_receipt.receipt_digest,
            )

        error_code = receipt.error_code or {
            SandboxOutcome.FAILED: "nonzero_exit",
            SandboxOutcome.TIMED_OUT: "timeout",
            SandboxOutcome.BACKEND_ERROR: "backend_error",
        }[receipt.outcome]
        return self._complete_failure(
            claim,
            policy_event,
            action=action,
            policy_digest=policy_digest,
            profile=profile,
            sandbox_receipt=receipt,
            sandbox_outcome=receipt.outcome.value,
            error_code=error_code,
            uncertain=EffectClass(claim.effect_class) in _UNCERTAIN_EFFECTS,
            timed_out=receipt.outcome is SandboxOutcome.TIMED_OUT,
            verification=(
                ToolReceiptVerification.UNVERIFIED
                if receipt.outcome is SandboxOutcome.BACKEND_ERROR
                else ToolReceiptVerification.INFERRED
            ),
            completion_evidence=completion_evidence,
        )

    def _complete_cancelled(
        self,
        claim: ActivityClaim,
        policy_event: EventRecord,
        *,
        action: ActionRequest,
        policy_digest: str,
        profile: SandboxProfile,
        receipt: SandboxReceipt,
        completion_evidence: Mapping[str, str] | None = None,
    ) -> ActivityExecutionResult:
        run = self.store.get_run(claim.run_id)
        if run is None or run.status not in {
            RunStatus.CANCELLING,
            RunStatus.CANCELLED,
        }:
            raise ActivityExecutionConflict(
                "backend cancellation has no matching durable Run intent"
            )
        tool_receipt = self._build_tool_receipt(
            action=action,
            attempt_status=AttemptStatus.CANCELLED,
            policy_digest=policy_digest,
            profile=profile,
            sandbox_receipt=receipt,
            verification=ToolReceiptVerification.INFERRED,
            error_code="cancelled",
        )
        _record, completion = self.scheduler.confirm_cancel_claim(
            claim,
            event_payload=self._receipt_event_payload(
                tool_receipt=tool_receipt,
                sandbox_outcome=receipt.outcome.value,
                completion_evidence=completion_evidence,
            ),
        )
        return ActivityExecutionResult(
            attempt_status=AttemptStatus.CANCELLED,
            policy_event=policy_event,
            completion_event=completion,
            sandbox_outcome=receipt.outcome.value,
            sandbox_receipt_digest=tool_receipt.sandbox_receipt_digest,
            tool_receipt_digest=tool_receipt.receipt_digest,
        )

    def _complete_without_backend(
        self,
        claim: ActivityClaim,
        policy_event: EventRecord,
        *,
        action: ActionRequest,
        policy_digest: str,
        profile: SandboxProfile,
        error_code: str,
    ) -> ActivityExecutionResult:
        return self._complete_failure(
            claim,
            policy_event,
            action=action,
            policy_digest=policy_digest,
            profile=profile,
            sandbox_receipt=None,
            receipt_absence_reason="dispatch_denied_before_backend",
            sandbox_outcome="dispatch_denied",
            error_code=error_code,
            uncertain=False,
            verification=ToolReceiptVerification.UNVERIFIED,
        )

    def _complete_unknown_dispatch(
        self,
        claim: ActivityClaim,
        policy_event: EventRecord,
        *,
        action: ActionRequest,
        policy_digest: str,
        profile: SandboxProfile,
    ) -> ActivityExecutionResult:
        return self._complete_failure(
            claim,
            policy_event,
            action=action,
            policy_digest=policy_digest,
            profile=profile,
            sandbox_receipt=None,
            receipt_absence_reason="dispatcher_exception_before_receipt",
            sandbox_outcome="backend_error",
            error_code="dispatcher_exception",
            uncertain=EffectClass(claim.effect_class) in _UNCERTAIN_EFFECTS,
            verification=ToolReceiptVerification.UNVERIFIED,
        )

    def _complete_failure(
        self,
        claim: ActivityClaim,
        policy_event: EventRecord,
        *,
        action: ActionRequest,
        policy_digest: str,
        profile: SandboxProfile,
        sandbox_receipt: SandboxReceipt | None,
        sandbox_outcome: str,
        error_code: str,
        uncertain: bool,
        timed_out: bool = False,
        receipt_absence_reason: str | None = None,
        verification: ToolReceiptVerification,
        completion_evidence: Mapping[str, str] | None = None,
    ) -> ActivityExecutionResult:
        if uncertain:
            attempt_status = AttemptStatus.OUTCOME_UNKNOWN
            node_status = NodeStatus.WAITING_RECOVERY
            run_status: RunStatus | None = RunStatus.WAITING_RECOVERY
        else:
            attempt_status = (
                AttemptStatus.TIMED_OUT if timed_out else AttemptStatus.FAILED
            )
            node_status = NodeStatus.FAILED
            run_status = None
        safe_result = {
            "outcome": attempt_status.value,
            "error_class": "sandbox",
            "error_code": error_code,
        }
        tool_receipt = self._build_tool_receipt(
            action=action,
            attempt_status=attempt_status,
            policy_digest=policy_digest,
            profile=profile,
            sandbox_receipt=sandbox_receipt,
            receipt_absence_reason=receipt_absence_reason,
            verification=verification,
            error_code=error_code,
        )
        _, completion = self.store.complete_activity(
            claim.run_id,
            claim.node_id,
            claim.attempt_id,
            claim.request_hash,
            claim.worker_id,
            claim_token=claim.claim_token,
            result=safe_result,
            event_payload=self._receipt_event_payload(
                tool_receipt=tool_receipt,
                sandbox_outcome=sandbox_outcome,
                completion_evidence=completion_evidence,
            ),
            attempt_status=attempt_status,
            node_status=node_status,
            run_status=run_status,
            now=self._now(),
        )
        self.scheduler.reconcile(claim.run_id)
        return ActivityExecutionResult(
            attempt_status=attempt_status,
            policy_event=policy_event,
            completion_event=completion,
            sandbox_outcome=sandbox_outcome,
            sandbox_receipt_digest=tool_receipt.sandbox_receipt_digest,
            tool_receipt_digest=tool_receipt.receipt_digest,
            receipt_absence_reason=(
                tool_receipt.sandbox_receipt_absence_reason
            ),
        )

    def _verify_output_refs(
        self,
        claim: ActivityClaim,
        refs: tuple[ArtifactRef, ...],
    ) -> tuple[ArtifactRef, ...]:
        if not refs:
            raise ArtifactReceiptError("successful sandbox result has no ArtifactRefs")
        if len(refs) > 64:
            raise ArtifactReceiptError("sandbox result exceeds ArtifactRef bound")
        for ref in refs:
            if ref.metadata:
                raise ArtifactReceiptError(
                    "Sandbox ArtifactRef metadata must be empty at the Event boundary"
                )
            if (
                ref.producer_run_id not in {None, claim.run_id}
                or ref.producer_node_id not in {None, claim.node_id}
                or ref.producer_attempt_id not in {None, claim.attempt_id}
            ):
                raise ArtifactReceiptError(
                    "ArtifactRef producer does not match the Activity claim"
                )
            try:
                verified = self._artifact_verifier(ref)
            except Exception as exc:
                raise ArtifactReceiptError("ArtifactRef verification failed") from exc
            if verified is not True:
                raise ArtifactReceiptError("ArtifactRef bytes failed integrity validation")
        return refs

    def _build_tool_receipt(
        self,
        *,
        action: ActionRequest,
        attempt_status: AttemptStatus,
        policy_digest: str,
        profile: SandboxProfile,
        sandbox_receipt: SandboxReceipt | None,
        verification: ToolReceiptVerification,
        error_code: str | None,
        receipt_absence_reason: str | None = None,
    ) -> ToolReceipt:
        return ToolReceipt(
            run_id=action.run_id,
            node_id=action.node_id,
            attempt_id=action.attempt_id,
            tool_name=action.tool_name,
            effect_class=action.effect_class,
            attempt_status=attempt_status,
            args_digest=action.args_digest,
            action_digest=action.action_digest,
            execution_binding_digest=action.execution_binding_digest,
            operation_key_digest=action.operation_key_digest,
            idempotency_key_digest=action.idempotency_key_digest,
            policy_version=self.policy.policy_version,
            policy_digest=policy_digest,
            profile_id=profile.profile_id,
            profile_digest=profile.profile_digest,
            verification=verification,
            sandbox_receipt=(
                None if sandbox_receipt is None else sandbox_receipt.to_dict()
            ),
            sandbox_receipt_absence_reason=receipt_absence_reason,
            error_code=error_code,
        )

    @staticmethod
    def _receipt_event_payload(
        *,
        tool_receipt: ToolReceipt,
        sandbox_outcome: str,
        completion_evidence: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "action_digest": tool_receipt.action_digest,
            "policy_digest": tool_receipt.policy_digest,
            "profile_digest": tool_receipt.profile_digest,
            "receipt_digest": tool_receipt.receipt_digest,
            "tool_receipt_digest": tool_receipt.receipt_digest,
            "sandbox_receipt_digest": (
                tool_receipt.sandbox_receipt_digest
            ),
            "sandbox_receipt_absence_reason": (
                tool_receipt.sandbox_receipt_absence_reason
            ),
            "sandbox_outcome": sandbox_outcome,
            "verification": tool_receipt.verification.value,
            "error_code": tool_receipt.error_code,
            "tool_receipt": tool_receipt.to_dict(),
        }
        if completion_evidence is not None:
            payload["remote_evidence"] = dict(completion_evidence)
        return payload

    def _fault(self, stage: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(stage)

    def _now(self) -> float:
        return _finite_now(self._clock())


def _receipt_text(
    value: Any,
    field_name: str,
    *,
    max_chars: int = 256,
) -> str:
    if not isinstance(value, str):
        raise ToolReceiptError(f"{field_name} must be a string")
    text = value.strip()
    if (
        not text
        or len(text) > max_chars
        or any(ord(character) < 32 or ord(character) == 127 for character in text)
    ):
        raise ToolReceiptError(f"{field_name} is invalid")
    return text


def _receipt_digest(value: Any, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ToolReceiptError(
            f"{field_name} must be a lowercase SHA-256 digest"
        )
    return value


def _policy_digest(policy_version: str) -> str:
    prefix = "sha256:"
    if not policy_version.startswith(prefix):
        raise ActivityExecutionConflict("policy version is not digest-addressed")
    digest = policy_version[len(prefix) :]
    if (
        len(digest) != 64
        or digest != digest.lower()
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise ActivityExecutionConflict("policy version digest is invalid")
    return digest


def _text_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _completion_evidence(
    value: Mapping[str, str] | None,
) -> dict[str, str] | None:
    """Normalize the only remote evidence permitted in a Domain Event."""

    if value is None:
        return None
    required = {
        "authorization_digest",
        "execution_plan_digest",
        "grant_binding_digest",
        "runtime_proof_digest",
        "session_binding_digest",
        "runtime_attestation_digest",
        "sandbox_spec_digest",
    }
    if not isinstance(value, Mapping) or set(value) != required:
        raise ActivityExecutionConflict("remote completion evidence is invalid")
    return {
        key: _receipt_digest(value[key], key)
        for key in sorted(required)
    }


def _prepared_claim_authority(claim: ActivityClaim) -> tuple[Any, ...]:
    """Bind a preparation to claim authority while allowing lease renewal."""

    return (
        claim.run_id,
        claim.node_id,
        claim.attempt_id,
        claim.attempt_number,
        claim.worker_id,
        claim.request_hash,
        claim.claim_token,
        claim.fencing_token,
        claim.operation_key,
        claim.idempotency_key,
        claim.claim_key,
        claim.activity_kind,
        claim.effect_class,
        claim.resource_keys,
        dict(claim.config),
        claim.input_artifact_bindings,
        claim.input_artifact_refs,
    )


def _preclaim_identity(claim: ActivityClaim) -> tuple[Any, ...]:
    """Immutable claim fields authorized before lease authority is minted."""

    return (
        claim.run_id,
        claim.node_id,
        claim.attempt_id,
        claim.attempt_number,
        claim.request_hash,
        claim.operation_key,
        claim.idempotency_key,
        claim.claim_key,
        claim.activity_kind,
        claim.effect_class,
        claim.resource_keys,
        dict(claim.config),
        claim.input_artifact_bindings,
        claim.input_artifact_refs,
    )


def _approval_execution_binding_digest(grant: ApprovalGrant) -> str:
    """Hash execution scope only; actor and approval identity stay in memory."""

    return _canonical_digest(
        {
            "schema": "approval_execution_binding_v1",
            "run_id": grant.run_id,
            "node_id": grant.node_id,
            "action_digest": grant.action_digest,
            "policy_version": grant.policy_version,
            "expires_at": grant.expires_at,
        }
    )


def _decision_digest(decision: PolicyDecision) -> str:
    """Digest only non-identity policy facts safe for a Domain Event."""

    return _canonical_digest(
        {
            "schema_version": decision.schema_version,
            "outcome": decision.outcome.value,
            "action_digest": decision.action_digest,
            "policy_version": decision.policy_version,
            "reason_code": decision.reason_code,
            "matched_rule_ids": list(decision.matched_rule_ids),
        }
    )


def _finite_now(value: Any) -> float:
    try:
        current = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("clock must return a finite timestamp") from exc
    if not math.isfinite(current) or current < 0:
        raise ValueError("clock must return a finite timestamp")
    return current


__all__ = [
    "ActivityExecutionConflict",
    "ActivityExecutionResult",
    "ActivityExecutorError",
    "ArtifactReceiptError",
    "ControlPlaneIsolationError",
    "DurableApprovalRegistry",
    "PreparedActivityExecution",
    "ToolReceipt",
    "ToolReceiptError",
    "ToolReceiptVerification",
    "TrustedActivityExecutor",
]
