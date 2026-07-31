"""Fail-closed Handler bridge for durable Agent-internal Tool executions."""

from __future__ import annotations

import hashlib
import json
import re
import threading
from dataclasses import dataclass, field
from functools import partial
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Protocol, runtime_checkable

from src.core.agent_loop import ActionResult, AgentContext, BaseHandler

from .agent_execution_evidence import (
    AgentExecutionEvidenceCollector,
    AgentExecutionEvidenceCollectorError,
)
from .agent_execution_manifest import (
    AgentActivityExecutionManifest,
    MAX_AGENT_EXECUTION_TOOL_RECEIPTS,
    MAX_AGENT_EXECUTION_TURNS,
)
from .agent_request import (
    AgentActivityRequest,
    AgentActivityRequestArtifactStore,
    AgentActivityRequestError,
)
from .agent_tool_result import (
    AgentToolResultArtifactStore,
    AgentToolResultError,
)
from .artifacts import (
    ArtifactEncryption,
    ArtifactRef,
    ArtifactSensitivity,
    LocalArtifactStore,
    canonical_json_bytes,
)
from .executor import ToolReceipt
from .models import AttemptStatus
from .policy import (
    MAX_ACTION_ARGS_BYTES,
    PolicyValidationError,
    canonical_action_args_digest,
    sensitive_argument_bytes,
)
from .store import DurableRunStore


AGENT_TOOL_INVOCATION_SCHEMA_VERSION = 1
MAX_AGENT_TOOL_SPECS = 128
_SAFE_TOOL_NAME = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$"
)
_SAFE_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_CLIENT_ERROR_REASONS = frozenset(
    {
        "agent_tool_execution_in_progress",
        "agent_tool_executor_unavailable",
        "agent_tool_invocation_binding_mismatch",
        "agent_tool_invocation_unavailable",
        "agent_tool_parent_binding_mismatch",
        "agent_tool_receipt_invalid",
        "agent_tool_result_invalid",
        "invalid_agent_tool_handler",
    }
)


class AgentToolHandlerError(RuntimeError, ValueError):
    """A durable Agent Tool bridge failed closed."""

    def __init__(self, reason_code: str) -> None:
        if reason_code not in _CLIENT_ERROR_REASONS:
            raise ValueError("invalid Agent Tool handler reason code")
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class AgentToolSpec:
    """Trusted redaction metadata for one remotely executed Tool."""

    tool_name: str
    sensitive_keys: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if (
            type(self.tool_name) is not str
            or _SAFE_TOOL_NAME.fullmatch(self.tool_name) is None
        ):
            raise AgentToolHandlerError("invalid_agent_tool_handler")
        keys = tuple(self.sensitive_keys)
        if (
            len(keys) > 64
            or len(set(keys)) != len(keys)
            or any(
                type(key) is not str
                or _SAFE_KEY.fullmatch(key) is None
                for key in keys
            )
        ):
            raise AgentToolHandlerError("invalid_agent_tool_handler")
        object.__setattr__(self, "sensitive_keys", tuple(sorted(keys)))


@dataclass(frozen=True, slots=True)
class AgentToolInvocationRequest:
    """Ephemeral exact request passed to a deployment-owned Tool executor."""

    run_id: str
    node_id: str
    attempt_id: str
    request_digest: str
    request_artifact_digest: str
    sequence: int
    turn: int
    tool_name: str
    tool_call_id_digest: str
    args_digest: str
    sensitivity: ArtifactSensitivity
    sensitive_keys: tuple[str, ...]
    operation_key: str = field(repr=False)
    args: Mapping[str, Any] = field(repr=False)
    schema_version: int = AGENT_TOOL_INVOCATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            type(self.run_id) is not str
            or type(self.node_id) is not str
            or type(self.attempt_id) is not str
            or not self.run_id
            or not self.node_id
            or not self.attempt_id
            or any(
                len(value) > 255
                or any(
                    ord(character) < 32
                    or ord(character) == 127
                    for character in value
                )
                for value in (
                    self.run_id,
                    self.node_id,
                    self.attempt_id,
                )
            )
            or type(self.request_digest) is not str
            or _DIGEST.fullmatch(self.request_digest) is None
            or type(self.request_artifact_digest) is not str
            or _DIGEST.fullmatch(self.request_artifact_digest) is None
            or type(self.sequence) is not int
            or not 1
            <= self.sequence
            <= MAX_AGENT_EXECUTION_TOOL_RECEIPTS
            or type(self.turn) is not int
            or not 1 <= self.turn <= MAX_AGENT_EXECUTION_TURNS
            or type(self.tool_name) is not str
            or _SAFE_TOOL_NAME.fullmatch(self.tool_name) is None
            or type(self.tool_call_id_digest) is not str
            or _DIGEST.fullmatch(self.tool_call_id_digest) is None
            or type(self.args_digest) is not str
            or _DIGEST.fullmatch(self.args_digest) is None
            or type(self.operation_key) is not str
            or not self.operation_key
            or len(self.operation_key) > 1024
            or any(
                ord(character) < 32 or ord(character) == 127
                for character in self.operation_key
            )
            or type(self.sensitive_keys) is not tuple
            or len(self.sensitive_keys) > 64
            or len(set(self.sensitive_keys))
            != len(self.sensitive_keys)
            or any(
                type(key) is not str
                or _SAFE_KEY.fullmatch(key) is None
                for key in self.sensitive_keys
            )
            or type(self.schema_version) is not int
            or self.schema_version
            != AGENT_TOOL_INVOCATION_SCHEMA_VERSION
            or type(self.args) is not dict
        ):
            raise AgentToolHandlerError(
                "agent_tool_invocation_binding_mismatch"
            )
        try:
            sensitivity = ArtifactSensitivity(self.sensitivity)
            encoded = json.dumps(
                dict(self.args),
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            if len(encoded) > MAX_ACTION_ARGS_BYTES:
                raise ValueError
            normalized_args = json.loads(encoded.decode("utf-8"))
            expected_args_digest = canonical_action_args_digest(
                normalized_args,
                sensitive_keys=self.sensitive_keys,
            )
            if sensitive_argument_bytes(
                normalized_args,
                self.sensitive_keys,
            ):
                raise ValueError
        except (KeyboardInterrupt, SystemExit):
            raise
        except (
            TypeError,
            ValueError,
            UnicodeError,
            OverflowError,
            RecursionError,
            PolicyValidationError,
        ):
            raise AgentToolHandlerError(
                "agent_tool_invocation_binding_mismatch"
            ) from None
        if (
            sensitivity
            not in {
                ArtifactSensitivity.SENSITIVE,
                ArtifactSensitivity.SECRET,
            }
            or expected_args_digest != self.args_digest
        ):
            raise AgentToolHandlerError(
                "agent_tool_invocation_binding_mismatch"
            )
        object.__setattr__(self, "sensitivity", sensitivity)
        object.__setattr__(
            self,
            "args",
            _freeze_json(normalized_args),
        )

    @property
    def invocation_digest(self) -> str:
        return hashlib.sha256(
            canonical_json_bytes(
                {
                    "schema_version": self.schema_version,
                    "kind": "agent_tool_invocation",
                    "run_id": self.run_id,
                    "node_id": self.node_id,
                    "attempt_id": self.attempt_id,
                    "request_digest": self.request_digest,
                    "request_artifact_digest": (
                        self.request_artifact_digest
                    ),
                    "sequence": self.sequence,
                    "turn": self.turn,
                    "tool_name": self.tool_name,
                    "tool_call_id_digest": (
                        self.tool_call_id_digest
                    ),
                    "args_digest": self.args_digest,
                    "operation_key_digest": hashlib.sha256(
                        self.operation_key.encode("utf-8")
                    ).hexdigest(),
                    "sensitivity": self.sensitivity.value,
                    "sensitive_keys": list(self.sensitive_keys),
                }
            )
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class AgentToolInvocationResult:
    """Typed executor response; raw Tool data remains in its Artifact."""

    receipt: ToolReceipt = field(repr=False)
    result_artifact_ref: ArtifactRef | None = field(
        default=None,
        repr=False,
    )


@runtime_checkable
class AgentToolExecutor(Protocol):
    """Deployment-owned executor that durably commits before returning."""

    @property
    def durable_result_recovery_ready(self) -> bool: ...

    def execute(
        self,
        request: AgentToolInvocationRequest,
    ) -> AgentToolInvocationResult: ...


class DurableAgentToolHandler(BaseHandler):
    """Expose an allowlisted durable Tool executor through BaseHandler."""

    def __init__(
        self,
        *,
        ctx: AgentContext,
        executor: AgentToolExecutor,
        receipt_store: DurableRunStore,
        result_store: LocalArtifactStore,
        request: AgentActivityRequest,
        request_ref: ArtifactRef,
        collector: AgentExecutionEvidenceCollector,
        tool_specs: Iterable[AgentToolSpec],
        checkpoint_manifest: AgentActivityExecutionManifest | None = None,
    ) -> None:
        if (
            type(ctx) is not AgentContext
            or not isinstance(receipt_store, DurableRunStore)
            or not isinstance(result_store, LocalArtifactStore)
            or type(request) is not AgentActivityRequest
            or type(collector) is not AgentExecutionEvidenceCollector
        ):
            raise AgentToolHandlerError("invalid_agent_tool_handler")
        try:
            execute = getattr(executor, "execute")
            recovery_ready = (
                executor.durable_result_recovery_ready is True
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            execute = None
            recovery_ready = False
        if not callable(execute) or not recovery_ready:
            raise AgentToolHandlerError("invalid_agent_tool_handler")
        parent_invalid = False
        try:
            stored_request = AgentActivityRequestArtifactStore(
                result_store
            ).load(request_ref)
            if (
                stored_request != request
                or (
                    request_ref.sensitivity
                    is ArtifactSensitivity.SECRET
                    and request_ref.encryption
                    is not ArtifactEncryption.DEPLOYMENT_MANAGED
                )
            ):
                raise AgentActivityRequestError(
                    "agent_request_artifact_binding_mismatch"
                )
            collector.validate_parent_binding(
                run_id=request.run_id,
                node_id=request.node_id,
                attempt_id=request.attempt_id,
                request_digest=request.request_digest,
                request_artifact_digest=request_ref.sha256,
                definition_digest=request.definition_digest,
                request_sensitivity=request_ref.sensitivity,
            )
        except (
            AgentActivityRequestError,
            AgentExecutionEvidenceCollectorError,
        ):
            parent_invalid = True
        if (
            parent_invalid
            or ctx.execution_evidence_observer is not collector
        ):
            raise AgentToolHandlerError(
                "agent_tool_parent_binding_mismatch"
            ) from None
        if checkpoint_manifest is not None:
            if (
                type(checkpoint_manifest)
                is not AgentActivityExecutionManifest
            ):
                raise AgentToolHandlerError(
                    "agent_tool_parent_binding_mismatch"
                )
            try:
                restored_manifest = collector.checkpoint_manifest(
                    completed_turn=checkpoint_manifest.turns or 0
                )
            except AgentExecutionEvidenceCollectorError:
                restored_manifest = None
            if (
                restored_manifest != checkpoint_manifest
            ):
                raise AgentToolHandlerError(
                    "agent_tool_parent_binding_mismatch"
                )
        try:
            specs = tuple(tool_specs)
        except TypeError:
            raise AgentToolHandlerError(
                "invalid_agent_tool_handler"
            ) from None
        if (
            not specs
            or len(specs) > MAX_AGENT_TOOL_SPECS
            or not all(type(spec) is AgentToolSpec for spec in specs)
            or len({spec.tool_name for spec in specs}) != len(specs)
        ):
            raise AgentToolHandlerError("invalid_agent_tool_handler")
        super().__init__(ctx)
        self._executor = executor
        self._receipt_store = receipt_store
        self._result_store = result_store
        self._request = request
        self._request_ref = request_ref
        self._collector = collector
        self._tool_specs = {
            spec.tool_name: spec for spec in specs
        }
        self._execution_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._seen_attempt_ids: set[str] = set()
        self._seen_receipt_digests: set[str] = set()
        if checkpoint_manifest is not None:
            self._seen_attempt_ids.update(
                binding.attempt_id
                for binding in checkpoint_manifest.tool_receipts
            )
            self._seen_receipt_digests.update(
                checkpoint_manifest.tool_receipt_digests
            )
            if (
                len(self._seen_attempt_ids)
                != len(checkpoint_manifest.tool_receipts)
                or len(self._seen_receipt_digests)
                != len(checkpoint_manifest.tool_receipts)
            ):
                raise AgentToolHandlerError(
                    "agent_tool_parent_binding_mismatch"
                )

    def __repr__(self) -> str:
        return (
            "DurableAgentToolHandler("
            f"tool_count={len(self._tool_specs)}, "
            f"completed_calls={len(self._seen_receipt_digests)})"
        )

    @property
    def production_security_ready(self) -> bool:
        """Dynamic child authority is not yet issued by this adapter."""

        return False

    def __getattr__(self, name: str):
        if type(name) is str and name.startswith("exec_"):
            tool_name = name[5:]
            if tool_name in self._tool_specs:
                return partial(self._execute_tool, tool_name)
        raise AttributeError(name)

    def _execute_tool(
        self,
        tool_name: str,
        args: Mapping[str, Any],
    ) -> ActionResult:
        if not self._execution_lock.acquire(blocking=False):
            raise AgentToolHandlerError(
                "agent_tool_execution_in_progress"
            )
        try:
            return self._execute_tool_once(tool_name, args)
        finally:
            self._execution_lock.release()

    def _execute_tool_once(
        self,
        tool_name: str,
        args: Mapping[str, Any],
    ) -> ActionResult:
        if type(args) is not dict:
            raise AgentToolHandlerError(
                "agent_tool_invocation_binding_mismatch"
            )
        context_failed = False
        try:
            invocation = self._collector.active_tool_invocation()
        except AgentExecutionEvidenceCollectorError:
            context_failed = True
            invocation = None
        if (
            context_failed
            or invocation is None
            or invocation.tool_name != tool_name
            or invocation.sequence
            > MAX_AGENT_EXECUTION_TOOL_RECEIPTS
        ):
            raise AgentToolHandlerError(
                "agent_tool_invocation_unavailable"
            ) from None
        spec = self._tool_specs[tool_name]
        request_failed = False
        try:
            args_digest = canonical_action_args_digest(
                args,
                sensitive_keys=spec.sensitive_keys,
            )
            if sensitive_argument_bytes(
                args,
                spec.sensitive_keys,
            ):
                raise ValueError
            request = AgentToolInvocationRequest(
                run_id=self._request.run_id,
                node_id=self._request.node_id,
                attempt_id=self._request.attempt_id,
                request_digest=self._request.request_digest,
                request_artifact_digest=self._request_ref.sha256,
                sequence=invocation.sequence,
                turn=invocation.turn,
                tool_name=tool_name,
                tool_call_id_digest=hashlib.sha256(
                    invocation.tool_call_id.encode("utf-8")
                ).hexdigest(),
                args_digest=args_digest,
                sensitivity=self._request_ref.sensitivity,
                sensitive_keys=spec.sensitive_keys,
                operation_key=invocation.operation_key,
                args=args,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except (
            AgentToolHandlerError,
            PolicyValidationError,
            TypeError,
            ValueError,
            UnicodeError,
            OverflowError,
            RecursionError,
        ):
            request_failed = True
            request = None
        if request_failed or request is None:
            raise AgentToolHandlerError(
                "agent_tool_invocation_binding_mismatch"
            ) from None
        executor_failed = False
        try:
            result = self._executor.execute(request)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            executor_failed = True
            result = None
        if executor_failed:
            raise AgentToolHandlerError(
                "agent_tool_executor_unavailable"
            ) from None
        return self._validate_result(request, result)

    def _validate_result(
        self,
        request: AgentToolInvocationRequest,
        result: Any,
    ) -> ActionResult:
        if (
            type(result) is not AgentToolInvocationResult
            or type(result.receipt) is not ToolReceipt
        ):
            raise AgentToolHandlerError(
                "agent_tool_receipt_invalid"
            )
        receipt = result.receipt
        operation_digest = hashlib.sha256(
            request.operation_key.encode("utf-8")
        ).hexdigest()
        if (
            receipt.run_id != request.run_id
            or receipt.node_id == request.node_id
            or receipt.attempt_id == request.attempt_id
            or receipt.tool_name != request.tool_name
            or receipt.args_digest != request.args_digest
            or receipt.operation_key_digest != operation_digest
            or receipt.idempotency_key_digest != operation_digest
        ):
            raise AgentToolHandlerError(
                "agent_tool_invocation_binding_mismatch"
            )
        verification_failed = False
        try:
            stored = self._receipt_store.get_tool_receipt(
                receipt.run_id,
                receipt.attempt_id,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            verification_failed = True
            stored = None
        if verification_failed or stored != receipt:
            raise AgentToolHandlerError(
                "agent_tool_receipt_invalid"
            ) from None
        with self._state_lock:
            if (
                receipt.attempt_id in self._seen_attempt_ids
                or receipt.receipt_digest
                in self._seen_receipt_digests
            ):
                raise AgentToolHandlerError(
                    "agent_tool_invocation_binding_mismatch"
                )
        if receipt.attempt_status is AttemptStatus.SUCCEEDED:
            action_result = self._successful_result(
                receipt,
                result.result_artifact_ref,
            )
        else:
            if result.result_artifact_ref is not None:
                raise AgentToolHandlerError(
                    "agent_tool_result_invalid"
                )
            action_result = _failed_action_result(receipt)
        with self._state_lock:
            self._seen_attempt_ids.add(receipt.attempt_id)
            self._seen_receipt_digests.add(receipt.receipt_digest)
        return action_result

    def _successful_result(
        self,
        receipt: ToolReceipt,
        ref: ArtifactRef | None,
    ) -> ActionResult:
        if (
            type(ref) is not ArtifactRef
            or ref.producer_run_id != receipt.run_id
            or ref.producer_node_id != receipt.node_id
            or ref.producer_attempt_id != receipt.attempt_id
            or _sensitivity_rank(ref.sensitivity)
            < _sensitivity_rank(self._request_ref.sensitivity)
            or receipt.sandbox_receipt is None
        ):
            raise AgentToolHandlerError(
                "agent_tool_result_invalid"
            )
        identity = {
            "artifact_id": ref.artifact_id,
            "sha256": ref.sha256,
            "size": ref.size,
            "kind": ref.kind.value,
        }
        output_refs = receipt.sandbox_receipt.get(
            "output_artifact_refs"
        )
        if (
            type(output_refs) is not list
            or output_refs.count(identity) != 1
        ):
            raise AgentToolHandlerError(
                "agent_tool_result_invalid"
            )
        result_failed = False
        try:
            tool_result = AgentToolResultArtifactStore.load(
                self._result_store,
                ref,
            )
        except AgentToolResultError:
            result_failed = True
            tool_result = None
        if result_failed or tool_result is None:
            raise AgentToolHandlerError(
                "agent_tool_result_invalid"
            ) from None
        return tool_result.to_action_result(tool_receipt=receipt)


def _failed_action_result(receipt: ToolReceipt) -> ActionResult:
    status = receipt.attempt_status
    stop = status in {
        AttemptStatus.CANCELLED,
        AttemptStatus.ABANDONED,
        AttemptStatus.OUTCOME_UNKNOWN,
    }
    reason_code = {
        AttemptStatus.FAILED: "tool_execution_failed",
        AttemptStatus.TIMED_OUT: "tool_execution_timed_out",
        AttemptStatus.CANCELLED: "tool_execution_cancelled",
        AttemptStatus.ABANDONED: "tool_execution_abandoned",
        AttemptStatus.OUTCOME_UNKNOWN: (
            "tool_execution_outcome_unknown"
        ),
    }[status]
    return ActionResult(
        data={
            "status": "DURABLE_TOOL_FAILED",
            "attempt_status": status.value,
            "error_code": reason_code,
        },
        next_prompt=(
            ""
            if stop
            else (
                "The durable tool execution failed with a fixed error code. "
                "Inspect the status and choose a safe alternative."
            )
        ),
        should_exit=stop,
        tool_receipt=receipt,
    )


def _sensitivity_rank(value: ArtifactSensitivity) -> int:
    return {
        ArtifactSensitivity.PUBLIC: 0,
        ArtifactSensitivity.INTERNAL: 1,
        ArtifactSensitivity.SENSITIVE: 2,
        ArtifactSensitivity.SECRET: 3,
    }[ArtifactSensitivity(value)]


def _freeze_json(value: Any) -> Any:
    if type(value) is dict:
        return MappingProxyType(
            {key: _freeze_json(item) for key, item in value.items()}
        )
    if type(value) is list:
        return tuple(_freeze_json(item) for item in value)
    return value


__all__ = [
    "AGENT_TOOL_INVOCATION_SCHEMA_VERSION",
    "AgentToolExecutor",
    "AgentToolHandlerError",
    "AgentToolInvocationRequest",
    "AgentToolInvocationResult",
    "AgentToolSpec",
    "DurableAgentToolHandler",
]
