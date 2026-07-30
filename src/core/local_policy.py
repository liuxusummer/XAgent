"""PolicyEngine adapter for the legacy/default Agent Loop.

The orchestration policy core remains the single source of truth.  This module
only translates a local tool call into its bounded ActionRequest and provides a
durable, metadata-only approval ledger for the synchronous local control plane.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from src.core.agent_kernel import Principal
from src.core.workspace_storage import atomic_write_json, workspace_write_lock
from src.orchestration.policy import (
    ActionRequest,
    ApprovalConsumeResult,
    ApprovalGrant,
    Capability,
    EffectClass,
    PolicyDecision,
    PolicyEngine,
    PolicyOutcome,
    PolicyRule,
    PolicyValidationError,
    ToolPolicy,
    ToolTimeoutBehavior,
)

_FILE_REF_PATTERN = re.compile(r"\{\{file:(.+?):(\d+):(\d+)}}")


LOCAL_APPROVAL_LEDGER = Path("runtime") / "agent_kernel" / "approval-ledger.json"
LOCAL_POLICY_ACTOR_PREFIX = "principal:"
LOCAL_POLICY_SCHEMA_VERSION = 1
HOST_READ_SCOPE = "host.read"


@dataclass(frozen=True, slots=True)
class LocalToolContract:
    tool_name: str
    effect_class: EffectClass
    capabilities: tuple[Capability, ...]
    resource_keys: tuple[str, ...]
    explicitly_allowed: bool = False

    def to_tool_policy(self) -> ToolPolicy:
        is_read = self.effect_class is EffectClass.READ_ONLY
        return ToolPolicy(
            tool_name=self.tool_name,
            effect_class=self.effect_class,
            capabilities=self.capabilities,
            supports_idempotency_key=False,
            supports_status_probe=False,
            supports_compensation=False,
            requires_script_artifact=False,
            timeout_behavior=(
                ToolTimeoutBehavior.SAFE_TO_RETRY
                if is_read
                else ToolTimeoutBehavior.OUTCOME_UNKNOWN
            ),
            allowed_resource_keys=self.resource_keys,
            required_resource_keys=() if is_read else self.resource_keys,
        )


LOCAL_TOOL_CONTRACTS = (
    LocalToolContract(
        "file_read",
        EffectClass.READ_ONLY,
        (Capability("workspace.read"),),
        ("workspace",),
    ),
    LocalToolContract(
        "file_search",
        EffectClass.READ_ONLY,
        (Capability("workspace.read"),),
        ("workspace",),
    ),
    LocalToolContract(
        "file_write",
        EffectClass.NON_IDEMPOTENT_WRITE,
        (Capability("workspace.write"),),
        ("workspace",),
        explicitly_allowed=True,
    ),
    LocalToolContract(
        "file_patch",
        EffectClass.NON_IDEMPOTENT_WRITE,
        (Capability("workspace.write"),),
        ("workspace",),
        explicitly_allowed=True,
    ),
    LocalToolContract(
        "file_delete",
        EffectClass.DESTRUCTIVE,
        (Capability("workspace.delete"),),
        ("workspace",),
    ),
    LocalToolContract(
        "code_run",
        EffectClass.DESTRUCTIVE,
        (Capability("process.execute"),),
        ("workspace", "network"),
    ),
    LocalToolContract(
        "web_scan",
        EffectClass.READ_ONLY,
        (Capability("network.read"),),
        ("browser", "network"),
    ),
    LocalToolContract(
        "web_execute_js",
        EffectClass.NON_IDEMPOTENT_WRITE,
        (Capability("browser.execute"),),
        ("browser", "network"),
    ),
    LocalToolContract(
        "ask_user",
        EffectClass.NON_IDEMPOTENT_WRITE,
        (Capability("user.interact"),),
        ("user",),
        explicitly_allowed=True,
    ),
    LocalToolContract(
        "agent_delegate",
        EffectClass.NON_IDEMPOTENT_WRITE,
        (Capability("agent.delegate"),),
        ("agent-runtime",),
        explicitly_allowed=True,
    ),
    LocalToolContract(
        "update_working_checkpoint",
        EffectClass.NON_IDEMPOTENT_WRITE,
        (Capability("state.write"),),
        ("agent-state",),
        explicitly_allowed=True,
    ),
    LocalToolContract(
        "skill_activate",
        EffectClass.NON_IDEMPOTENT_WRITE,
        (Capability("skill.activate"),),
        ("agent-state",),
        explicitly_allowed=True,
    ),
    LocalToolContract(
        "start_long_term_update",
        EffectClass.READ_ONLY,
        (Capability("memory.read"),),
        ("agent-state",),
    ),
    LocalToolContract(
        "memory_propose",
        EffectClass.NON_IDEMPOTENT_WRITE,
        (Capability("memory.propose"),),
        ("agent-state",),
        explicitly_allowed=True,
    ),
    LocalToolContract(
        "plan_update",
        EffectClass.NON_IDEMPOTENT_WRITE,
        (Capability("state.write"),),
        ("agent-state",),
        explicitly_allowed=True,
    ),
)

LOCAL_TOOL_CONTRACT_MAP = {
    contract.tool_name: contract for contract in LOCAL_TOOL_CONTRACTS
}
LOCAL_PRINCIPAL_SCOPES = tuple(
    sorted(
        {
            capability.name
            for contract in LOCAL_TOOL_CONTRACTS
            for capability in contract.capabilities
        }
        | {HOST_READ_SCOPE}
    )
)


class DurableLocalApprovalLedger:
    """Workspace-scoped, process-safe, single-use approval grant registry."""

    def __init__(self, workspace_root: str | Path) -> None:
        self.workspace_root = Path(workspace_root).expanduser().resolve()
        self.path = self.workspace_root / LOCAL_APPROVAL_LEDGER

    def register_issued(self, grant: ApprovalGrant) -> bool:
        with workspace_write_lock(self.workspace_root):
            state = self._load()
            issued = state["issued"]
            existing = issued.get(grant.approval_id)
            if existing is not None:
                return existing == grant.grant_digest
            issued[grant.approval_id] = grant.grant_digest
            self._write(state)
            return True

    def consume(self, grant: ApprovalGrant) -> ApprovalConsumeResult:
        with workspace_write_lock(self.workspace_root):
            state = self._load()
            expected = state["issued"].get(grant.approval_id)
            if expected is None:
                return ApprovalConsumeResult.UNKNOWN
            if expected != grant.grant_digest:
                return ApprovalConsumeResult.GRANT_MISMATCH
            if grant.approval_id in state["consumed"]:
                return ApprovalConsumeResult.REPLAYED
            state["consumed"].append(grant.approval_id)
            state["consumed"].sort()
            self._write(state)
            return ApprovalConsumeResult.CONSUMED

    def _load(self) -> dict[str, Any]:
        if not self.path.is_file():
            return {
                "schema_version": LOCAL_POLICY_SCHEMA_VERSION,
                "issued": {},
                "consumed": [],
            }
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("local approval ledger is unreadable") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != LOCAL_POLICY_SCHEMA_VERSION
            or not isinstance(payload.get("issued"), dict)
            or not isinstance(payload.get("consumed"), list)
        ):
            raise RuntimeError("local approval ledger is invalid")
        if not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in payload["issued"].items()
        ) or not all(isinstance(value, str) for value in payload["consumed"]):
            raise RuntimeError("local approval ledger is invalid")
        return payload

    def _write(self, state: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.path, state)


@dataclass(frozen=True, slots=True)
class LocalAuthorization:
    action: ActionRequest | None
    decision: PolicyDecision


class LocalPolicyGate:
    """Translate and authorize every built-in default-loop tool call."""

    def __init__(self, workspace_root: str | Path, *, actor: str) -> None:
        self.workspace_root = Path(workspace_root).expanduser().resolve()
        self.actor = f"{LOCAL_POLICY_ACTOR_PREFIX}{actor}"
        self.ledger = DurableLocalApprovalLedger(self.workspace_root)
        allow_rules = tuple(
            PolicyRule(
                rule_id=f"local-explicit-{contract.tool_name}",
                outcome=PolicyOutcome.ALLOW,
                tool_name=contract.tool_name,
                effect_classes=(contract.effect_class,),
                required_capabilities=contract.capabilities,
                required_resource_locks=contract.resource_keys,
                reason_code="local_explicit_allow",
            )
            for contract in LOCAL_TOOL_CONTRACTS
            if contract.explicitly_allowed
        )
        self.engine = PolicyEngine(
            tuple(contract.to_tool_policy() for contract in LOCAL_TOOL_CONTRACTS),
            allow_rules,
            approval_actors=(self.actor,),
            ledger=self.ledger,
        )
        self._call_counter = 0

    def evaluate(
        self,
        *,
        principal: Principal,
        tool_name: str,
        args: Mapping[str, Any],
        turn: int,
    ) -> LocalAuthorization:
        contract = LOCAL_TOOL_CONTRACT_MAP.get(tool_name)
        if contract is None:
            return LocalAuthorization(
                action=None,
                decision=self._unknown_tool_decision(tool_name),
            )
        try:
            action = self._action(principal, contract, args, turn)
        except (
            PolicyValidationError,
            TypeError,
            ValueError,
            OverflowError,
            RecursionError,
        ):
            return LocalAuthorization(
                action=None,
                decision=self._invalid_action_decision(principal, tool_name),
            )
        if self.actor != f"{LOCAL_POLICY_ACTOR_PREFIX}{principal.subject}":
            return LocalAuthorization(
                action=action,
                decision=PolicyDecision(
                    outcome=PolicyOutcome.DENY,
                    action_digest=action.action_digest,
                    policy_version=self.engine.policy_version,
                    reason_code="principal_actor_mismatch",
                ),
            )
        action_capabilities = {item.name for item in contract.capabilities}
        if (
            tool_name == "web_execute_js"
            and isinstance(args.get("save_to_file"), str)
            and str(args.get("save_to_file") or "").strip()
        ):
            action_capabilities.add("workspace.write")
        requires_host_read = (
            tool_name == "file_read"
            and _file_target_is_outside_workspace(self.workspace_root, args)
        )
        if requires_host_read:
            action_capabilities.add(HOST_READ_SCOPE)
            if HOST_READ_SCOPE not in principal.scopes:
                return LocalAuthorization(
                    action=action,
                    decision=PolicyDecision(
                        outcome=PolicyOutcome.DENY,
                        action_digest=action.action_digest,
                        policy_version=self.engine.policy_version,
                        reason_code="host_read_scope_required",
                    ),
                )
        if not action_capabilities.issubset(set(principal.scopes)):
            return LocalAuthorization(
                action=action,
                decision=PolicyDecision(
                    outcome=PolicyOutcome.DENY,
                    action_digest=action.action_digest,
                    policy_version=self.engine.policy_version,
                    reason_code="principal_scope_missing",
                ),
            )
        target_reason = (
            _protected_file_target_reason(self.workspace_root, args)
            if tool_name
            in {"file_read", "file_write", "file_patch", "file_delete"}
            else ""
        )
        if tool_name == "web_execute_js" and isinstance(
            args.get("save_to_file"),
            str,
        ):
            target_reason = _protected_file_target_reason(
                self.workspace_root,
                {"path": args.get("save_to_file")},
            )
        reference_reason = (
            _protected_file_reference_reason(self.workspace_root, args)
            if tool_name in {"file_write", "file_patch"}
            else ""
        )
        protected_reason = (
            target_reason
            if (
                tool_name
                in {"file_write", "file_patch", "file_delete"}
                or (
                    tool_name == "web_execute_js"
                    and bool(str(args.get("save_to_file") or "").strip())
                )
            )
            or target_reason == "protected_control_plane"
            else ""
        )
        if reference_reason:
            protected_reason = reference_reason
        if (
            tool_name == "file_read"
            and target_reason == "managed_memory_requires_candidate"
            and "memory.read" not in principal.scopes
        ):
            protected_reason = "memory_read_scope_required"
        if protected_reason:
            return LocalAuthorization(
                action=action,
                decision=PolicyDecision(
                    outcome=PolicyOutcome.DENY,
                    action_digest=action.action_digest,
                    policy_version=self.engine.policy_version,
                    reason_code=protected_reason,
                ),
            )
        return LocalAuthorization(action=action, decision=self.engine.evaluate(action))

    def approve(
        self,
        authorization: LocalAuthorization,
        *,
        principal: Principal,
        ttl_seconds: float = 300,
        now: float | None = None,
    ) -> PolicyDecision:
        action = authorization.action
        if action is None or authorization.decision.outcome is not PolicyOutcome.REQUIRE_APPROVAL:
            raise ValueError("authorization does not contain a pending action")
        current = time.time() if now is None else float(now)
        approval_id = hashlib.sha256(
            f"{action.action_digest}\0{principal.principal_digest}\0{current}".encode(
                "utf-8"
            )
        ).hexdigest()[:32]
        grant = ApprovalGrant(
            approval_id=approval_id,
            action_digest=action.action_digest,
            run_id=action.run_id,
            node_id=action.node_id,
            policy_version=self.engine.policy_version,
            actor=self.actor,
            expires_at=current + ttl_seconds,
        )
        if not self.ledger.register_issued(grant):
            return PolicyDecision(
                outcome=PolicyOutcome.DENY,
                action_digest=action.action_digest,
                policy_version=self.engine.policy_version,
                reason_code="approval_registration_conflict",
            )
        return self.engine.consume_approval(action, grant, now=current)

    def _action(
        self,
        principal: Principal,
        contract: LocalToolContract,
        args: Mapping[str, Any],
        turn: int,
    ) -> ActionRequest:
        self._call_counter += 1
        call_id = f"turn-{max(0, int(turn))}-call-{self._call_counter}"
        binding_digest = hashlib.sha256(
            (
                f"{principal.principal_digest}\0{self.workspace_root}\0"
                f"{contract.tool_name}"
            ).encode("utf-8")
        ).hexdigest()
        operation_key = (
            f"{principal.session_id}:{call_id}:{contract.tool_name}"
        )
        return ActionRequest.from_args(
            run_id=principal.run_id,
            node_id=f"agent-loop-turn-{max(0, int(turn))}",
            attempt_id=call_id,
            tool_name=contract.tool_name,
            args=_bounded_action_args(args),
            execution_binding_digest=binding_digest,
            operation_key=operation_key,
            idempotency_key=operation_key,
            effect_class=contract.effect_class,
            capabilities=contract.capabilities,
            resource_locks=contract.resource_keys,
        )

    def _unknown_tool_decision(self, tool_name: str) -> PolicyDecision:
        fallback_digest = hashlib.sha256(
            f"unknown-tool\0{tool_name}".encode("utf-8")
        ).hexdigest()
        return PolicyDecision(
            outcome=PolicyOutcome.DENY,
            action_digest=fallback_digest,
            policy_version=self.engine.policy_version,
            reason_code="unknown_tool",
        )

    def _invalid_action_decision(
        self,
        principal: Principal,
        tool_name: str,
    ) -> PolicyDecision:
        fallback_digest = hashlib.sha256(
            (
                f"invalid-action\0{principal.principal_digest}\0"
                f"{self.workspace_root}\0{tool_name}"
            ).encode("utf-8")
        ).hexdigest()
        return PolicyDecision(
            outcome=PolicyOutcome.DENY,
            action_digest=fallback_digest,
            policy_version=self.engine.policy_version,
            reason_code="invalid_action_args",
        )


def _bounded_action_args(value: Any) -> Any:
    """Replace oversized strings before ActionRequest's bounded canonicalization."""

    if isinstance(value, Mapping):
        return {str(key): _bounded_action_args(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_bounded_action_args(item) for item in value]
    if isinstance(value, str) and len(value) > 4096:
        encoded = value.encode("utf-8")
        return {
            "$content_sha256": hashlib.sha256(encoded).hexdigest(),
            "$content_bytes": len(encoded),
        }
    return value


def _protected_file_target_reason(
    workspace_root: Path,
    args: Mapping[str, Any],
) -> str:
    raw_path = args.get("path")
    if not isinstance(raw_path, str) or not raw_path.strip():
        return ""
    candidate = Path(raw_path).expanduser()
    try:
        resolved = (
            candidate.resolve()
            if candidate.is_absolute()
            else (workspace_root / candidate).resolve()
        )
        relative = resolved.relative_to(workspace_root)
    except (OSError, RuntimeError, ValueError):
        return ""
    parts = tuple(part.casefold() for part in relative.parts)
    if not parts:
        return ""
    if len(parts) == 1 and parts[0].casefold() in {
        "_intervene",
        "_keyinfo",
        "plan.md",
    }:
        return "protected_control_plane"
    if parts[0] == "memory":
        return "managed_memory_requires_candidate"
    if len(parts) >= 2 and parts[:2] == ("runtime", "agent_kernel"):
        return "protected_control_plane"
    if len(parts) >= 2 and parts[:2] == ("runtime", "checkpoints"):
        return "protected_control_plane"
    if (
        len(parts) >= 2
        and parts[0] == "runtime"
        and parts[1].startswith("file_index.sqlite3")
    ):
        return "protected_control_plane"
    if parts[0] == "runtime":
        return "workspace_runtime_read_only"
    if len(parts) >= 2 and parts[:2] == ("system", "memory"):
        return "managed_memory_requires_candidate"
    if (
        len(parts) >= 4
        and parts[0] == "system"
        and parts[1] == "agents"
        and parts[-1].casefold() == "memory.md"
    ):
        return "managed_memory_requires_candidate"
    if parts[0] == "system":
        return "workspace_system_read_only"
    return ""


def _file_target_is_outside_workspace(
    workspace_root: Path,
    args: Mapping[str, Any],
) -> bool:
    raw_path = args.get("path")
    if not isinstance(raw_path, str) or not raw_path.strip():
        return False
    candidate = Path(raw_path).expanduser()
    try:
        resolved = (
            candidate.resolve()
            if candidate.is_absolute()
            else (workspace_root / candidate).resolve()
        )
        resolved.relative_to(workspace_root)
    except ValueError:
        return True
    except (OSError, RuntimeError):
        return False
    return False


def _protected_file_reference_reason(
    workspace_root: Path,
    args: Mapping[str, Any],
) -> str:
    for field_name in ("content", "old_content", "new_content"):
        value = args.get(field_name)
        if not isinstance(value, str):
            continue
        for match in _FILE_REF_PATTERN.finditer(value):
            reason = _protected_file_target_reason(
                workspace_root,
                {"path": match.group(1)},
            )
            if reason in {
                "managed_memory_requires_candidate",
                "protected_control_plane",
            }:
                return "protected_file_reference"
    return ""


__all__ = [
    "DurableLocalApprovalLedger",
    "HOST_READ_SCOPE",
    "LOCAL_PRINCIPAL_SCOPES",
    "LOCAL_TOOL_CONTRACTS",
    "LOCAL_TOOL_CONTRACT_MAP",
    "LocalAuthorization",
    "LocalPolicyGate",
    "LocalToolContract",
]
