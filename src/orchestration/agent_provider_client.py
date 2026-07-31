"""Durable provider-gateway client for one remote Agent Attempt."""

from __future__ import annotations

import hashlib
import math
import threading
from typing import Any, Callable

from src.core.llm import ChatResponse

from .agent_execution_evidence import (
    AgentExecutionEvidenceCollector,
    AgentExecutionEvidenceCollectorError,
)
from .agent_request import (
    AgentActivityRequest,
    AgentActivityRequestArtifactStore,
    AgentActivityRequestError,
)
from .agent_provider_wire import (
    AgentProviderWireError,
    MAX_AGENT_PROVIDER_MESSAGES,
    MAX_AGENT_PROVIDER_OUTPUT_TOKENS,
    decode_agent_provider_response,
    encode_agent_provider_request,
)
from .artifacts import (
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    canonical_json_bytes,
)
from .provider_access import (
    MAX_PROVIDER_GRANT_TTL_SECONDS,
    MAX_PROVIDER_REQUEST_BYTES,
    ProviderAccessBroker,
    ProviderAccessDenied,
    ProviderInvocationResult,
)
from .worker_security import WorkerAuthorization


MAX_AGENT_PROVIDER_HISTORY_COMPACTIONS = 128
MAX_AGENT_PROVIDER_BROKER_ATTEMPTS = 3
_RETRYABLE_BROKER_REASONS = frozenset(
    {
        "provider_invocation_failed",
        "provider_invocation_outcome_unknown",
        "provider_operation_recovery_failed",
        "provider_grant_registry_unavailable",
    }
)
_CLIENT_ERROR_REASONS = frozenset(
    {
        "agent_provider_authorization_binding_mismatch",
        "agent_provider_authorization_invalid",
        "agent_provider_authorization_lineage_changed",
        "agent_provider_authorization_unavailable",
        "agent_provider_durable_result_required",
        "agent_provider_invocation_in_progress",
        "agent_provider_invocation_unavailable",
        "agent_provider_parent_binding_mismatch",
        "agent_provider_request_invalid",
        "agent_provider_request_too_large",
        "agent_provider_response_invalid",
        "agent_provider_result_binding_mismatch",
        "invalid_agent_provider_client",
    }
)


class AgentProviderClientError(RuntimeError, ValueError):
    """A remote Agent provider-client boundary failed closed."""

    def __init__(self, reason_code: str) -> None:
        if reason_code not in _CLIENT_ERROR_REASONS:
            raise ValueError("invalid Agent provider client reason code")
        self.reason_code = reason_code
        super().__init__(reason_code)


class DurableAgentProviderClient:
    """Bridge Agent Loop chat calls to one-call durable provider grants.

    The client is scoped to one Agent request/Attempt. It maintains the
    protocol-neutral chat history expected by Agent Loop, but it never holds
    an upstream provider credential. A fresh WorkerAuthorization may be
    supplied for every call; all successful calls must retain the first
    authorization lineage.
    """

    __slots__ = (
        "_authorization_digest",
        "_authorization_source",
        "_broker",
        "_chat_lock",
        "_collector",
        "_grant_ttl_seconds",
        "_lock",
        "_maximum_broker_attempts",
        "_request",
        "_request_ref",
        "_route",
        "_route_id",
        "_system_messages",
        "context_window_chars",
        "history",
        "history_compaction",
        "last_tools",
        "max_tokens",
        "request_count",
        "temperature",
    )

    def __init__(
        self,
        *,
        broker: ProviderAccessBroker,
        authorization_source: Callable[[], WorkerAuthorization],
        request: AgentActivityRequest,
        request_ref: ArtifactRef,
        collector: AgentExecutionEvidenceCollector,
        route_id: str,
        grant_ttl_seconds: float | None = None,
        maximum_broker_attempts: int = 2,
        temperature: float = 0.2,
        max_tokens: int = 4_096,
        context_window_chars: int = 24_000,
    ) -> None:
        if (
            not isinstance(broker, ProviderAccessBroker)
            or not callable(authorization_source)
            or type(request) is not AgentActivityRequest
            or type(collector) is not AgentExecutionEvidenceCollector
            or type(route_id) is not str
        ):
            raise AgentProviderClientError(
                "invalid_agent_provider_client"
            )
        route = broker.describe_route(route_id)
        parent_invalid = False
        try:
            AgentActivityRequestArtifactStore.validate_ref(
                request_ref,
                request,
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
        if parent_invalid:
            raise AgentProviderClientError(
                "agent_provider_parent_binding_mismatch"
            ) from None
        readiness_failed = False
        try:
            durable_result_ready = (
                broker.durable_result_recovery_ready is True
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            readiness_failed = True
            durable_result_ready = False
        if readiness_failed or not durable_result_ready:
            raise AgentProviderClientError(
                "agent_provider_durable_result_required"
            )
        if grant_ttl_seconds is not None and (
            isinstance(grant_ttl_seconds, bool)
            or type(grant_ttl_seconds) not in {int, float}
            or not math.isfinite(float(grant_ttl_seconds))
            or not 0
            < float(grant_ttl_seconds)
            <= MAX_PROVIDER_GRANT_TTL_SECONDS
        ):
            raise AgentProviderClientError(
                "invalid_agent_provider_client"
            )
        if (
            isinstance(maximum_broker_attempts, bool)
            or not isinstance(maximum_broker_attempts, int)
            or not 1
            <= maximum_broker_attempts
            <= MAX_AGENT_PROVIDER_BROKER_ATTEMPTS
            or isinstance(max_tokens, bool)
            or type(max_tokens) is not int
            or not 1 <= max_tokens <= MAX_AGENT_PROVIDER_OUTPUT_TOKENS
            or isinstance(context_window_chars, bool)
            or type(context_window_chars) is not int
            or not 1_024
            <= context_window_chars
            <= MAX_PROVIDER_REQUEST_BYTES
            or isinstance(temperature, bool)
            or type(temperature) not in {int, float}
            or not math.isfinite(float(temperature))
            or not 0 <= float(temperature) <= 2
        ):
            raise AgentProviderClientError(
                "invalid_agent_provider_client"
            )
        self._broker = broker
        self._authorization_source = authorization_source
        self._request = request
        self._request_ref = request_ref
        self._collector = collector
        self._route = route
        self._route_id = route_id
        self._grant_ttl_seconds = (
            None
            if grant_ttl_seconds is None
            else float(grant_ttl_seconds)
        )
        self._maximum_broker_attempts = maximum_broker_attempts
        self._authorization_digest: str | None = None
        self._system_messages: list[dict[str, Any]] = []
        self._chat_lock = threading.Lock()
        self._lock = threading.RLock()
        self.temperature = float(temperature)
        self.max_tokens = max_tokens
        self.context_window_chars = context_window_chars
        self.history: list[dict[str, Any]] = []
        self.history_compaction: list[dict[str, Any]] = []
        self.last_tools = ""
        self.request_count = 0

    def __repr__(self) -> str:
        return (
            "DurableAgentProviderClient("
            f"route_id={self._route_id!r}, "
            f"request_digest={self._request.request_digest!r}, "
            f"request_count={self.request_count}, "
            f"history_messages={len(self.history)})"
        )

    @property
    def backend(self) -> "DurableAgentProviderClient":
        """Expose Session-compatible history controls to Agent Loop."""

        return self

    @property
    def production_security_ready(self) -> bool:
        """The reference Broker cannot attest a production gateway."""

        return False

    def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> ChatResponse:
        if not self._chat_lock.acquire(blocking=False):
            raise AgentProviderClientError(
                "agent_provider_invocation_in_progress"
            )
        try:
            return self._chat_once(messages, tools)
        finally:
            self._chat_lock.release()

    def _chat_once(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> ChatResponse:
        invocation = self._active_invocation()
        (
            payload,
            current_system,
            current_messages,
        ) = self._request_payload(
            messages,
            tools,
            invocation_index=invocation.sequence,
        )
        authorization = self._authorization()
        self._validate_authorization(authorization)
        with self._lock:
            current_authorization_digest = (
                self._authorization_digest
            )
        if (
            current_authorization_digest is not None
            and authorization.authorization_digest
            != current_authorization_digest
        ):
            raise AgentProviderClientError(
                "agent_provider_authorization_lineage_changed"
            )
        grant = self._broker.issue(
            authorization,
            route_id=self._route_id,
            request_digest=self._request.request_digest,
            request_artifact_digest=self._request_ref.sha256,
            invocation_index=invocation.sequence,
            response_sensitivity=(
                authorization.maximum_artifact_sensitivity
            ),
            ttl_seconds=self._grant_ttl_seconds,
        )
        with self._lock:
            if self._authorization_digest is None:
                self._authorization_digest = (
                    authorization.authorization_digest
                )
            elif (
                self._authorization_digest
                != authorization.authorization_digest
            ):
                raise AgentProviderClientError(
                    "agent_provider_authorization_lineage_changed"
                )
        result = self._invoke(grant, authorization, payload)
        self._validate_result(
            result,
            invocation_index=invocation.sequence,
            authorization=authorization,
            payload=payload,
        )
        response_failure: str | None = None
        try:
            response = decode_agent_provider_response(
                result.content
            )
        except AgentProviderWireError as exc:
            response_failure = exc.reason_code
            response = None
        if response_failure is not None or response is None:
            raise AgentProviderClientError(
                response_failure
                or "agent_provider_response_invalid"
            ) from None
        durable_response = ChatResponse(
            thinking=response.thinking,
            content=response.content,
            tool_calls=response.tool_calls,
            raw=response.raw,
            stop_reason=response.stop_reason,
            usage=response.usage,
            provider_receipt=result.receipt,
        )
        self._commit_history(
            current_system=current_system,
            current_messages=current_messages,
            response=durable_response,
        )
        return durable_response

    def _active_invocation(self):
        unavailable = False
        try:
            invocation = self._collector.active_provider_invocation()
        except AgentExecutionEvidenceCollectorError:
            unavailable = True
            invocation = None
        if unavailable or invocation is None:
            raise AgentProviderClientError(
                "agent_provider_invocation_unavailable"
            ) from None
        return invocation

    def _authorization(self) -> WorkerAuthorization:
        failed = False
        try:
            authorization = self._authorization_source()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            failed = True
            authorization = None
        if failed:
            raise AgentProviderClientError(
                "agent_provider_authorization_unavailable"
            ) from None
        if type(authorization) is not WorkerAuthorization:
            raise AgentProviderClientError(
                "agent_provider_authorization_invalid"
            )
        return authorization

    def _validate_authorization(
        self,
        authorization: WorkerAuthorization,
    ) -> None:
        if (
            authorization.run_id != self._request.run_id
            or authorization.node_id != self._request.node_id
            or authorization.attempt_id != self._request.attempt_id
            or _sensitivity_rank(
                authorization.maximum_artifact_sensitivity
            )
            < _sensitivity_rank(self._request_ref.sensitivity)
        ):
            raise AgentProviderClientError(
                "agent_provider_authorization_binding_mismatch"
            )

    def _request_payload(
        self,
        messages: Any,
        tools: Any,
        *,
        invocation_index: int,
    ) -> tuple[
        bytes,
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
        wire_failure: str | None = None
        payload = b""
        current_system: list[dict[str, Any]] = []
        current_non_system: list[dict[str, Any]] = []
        with self._lock:
            retained_system = list(self._system_messages)
            retained_history = list(self.history)
        try:
            (
                payload,
                current_system,
                current_non_system,
            ) = encode_agent_provider_request(
                messages,
                tools,
                retained_system=retained_system,
                retained_history=retained_history,
                invocation_index=invocation_index,
                temperature=self.temperature,
                max_output_tokens=self.max_tokens,
            )
        except AgentProviderWireError as exc:
            wire_failure = exc.reason_code
        if wire_failure is not None:
            raise AgentProviderClientError(wire_failure) from None
        if len(payload) > self._route.maximum_request_bytes:
            raise AgentProviderClientError(
                "agent_provider_request_too_large"
            )
        return payload, current_system, current_non_system

    def _invoke(
        self,
        grant,
        authorization: WorkerAuthorization,
        payload: bytes,
    ) -> ProviderInvocationResult:
        for attempt in range(self._maximum_broker_attempts):
            try:
                return self._broker.invoke(
                    grant,
                    authorization,
                    payload,
                )
            except ProviderAccessDenied as exc:
                if (
                    attempt + 1
                    >= self._maximum_broker_attempts
                    or exc.reason_code
                    not in _RETRYABLE_BROKER_REASONS
                ):
                    raise
        raise AgentProviderClientError(
            "agent_provider_result_binding_mismatch"
        )

    def _validate_result(
        self,
        result: Any,
        *,
        invocation_index: int,
        authorization: WorkerAuthorization,
        payload: bytes,
    ) -> None:
        if (
            type(result) is not ProviderInvocationResult
            or len(result.content)
            > self._route.maximum_response_bytes
            or result.artifact_ref is None
            or type(result.artifact_ref) is not ArtifactRef
            or result.artifact_ref.kind
            is not ArtifactKind.MODEL_RESPONSE
            or result.artifact_ref.sensitivity
            is not authorization.maximum_artifact_sensitivity
            or result.artifact_ref.producer_run_id
            != self._request.run_id
            or result.artifact_ref.producer_node_id
            != self._request.node_id
            or result.artifact_ref.producer_attempt_id
            != self._request.attempt_id
            or dict(result.artifact_ref.metadata)
            or result.receipt.response_sensitivity
            is not authorization.maximum_artifact_sensitivity
            or result.receipt.action_digest
            != authorization.action_digest
            or result.receipt.response_artifact_ref_digest is None
            or result.receipt.run_id != self._request.run_id
            or result.receipt.node_id != self._request.node_id
            or result.receipt.attempt_id != self._request.attempt_id
            or result.receipt.request_digest
            != self._request.request_digest
            or result.receipt.request_artifact_digest
            != self._request_ref.sha256
            or result.receipt.invocation_index != invocation_index
            or result.receipt.authorization_digest
            != authorization.authorization_digest
            or result.receipt.route_id != self._route_id
            or result.receipt.route_digest
            != self._route.route_digest
            or result.receipt.request_payload_digest
            != hashlib.sha256(payload).hexdigest()
        ):
            raise AgentProviderClientError(
                "agent_provider_result_binding_mismatch"
            )

    def _commit_history(
        self,
        *,
        current_system: list[dict[str, Any]],
        current_messages: list[dict[str, Any]],
        response: ChatResponse,
    ) -> None:
        assistant: dict[str, Any] = {
            "role": "assistant",
            "content": response.content,
        }
        if response.thinking:
            assistant["thinking"] = response.thinking
        if response.tool_calls:
            assistant["tool_calls"] = [
                {
                    "name": call.name,
                    "arguments": call.args,
                    "id": call.id,
                }
                for call in response.tool_calls
            ]
        with self._lock:
            if current_system:
                self._system_messages = current_system
            self.history.extend(current_messages)
            self.history.append(assistant)
            self.request_count += 1
            self._bound_history()

    def _bound_history(self) -> None:
        maximum_history = MAX_AGENT_PROVIDER_MESSAGES - 2
        if len(self.history) <= maximum_history:
            return
        first_user_index = next(
            (
                index
                for index, message in enumerate(self.history)
                if message.get("role") == "user"
            ),
            None,
        )
        keep_recent = maximum_history - (
            1 if first_user_index is not None else 0
        )
        recent_start = len(self.history) - keep_recent
        retained_indexes = set(
            range(recent_start, len(self.history))
        )
        if first_user_index is not None:
            retained_indexes.add(first_user_index)
        retained = [
            message
            for index, message in enumerate(self.history)
            if index in retained_indexes
        ]
        omitted = [
            message
            for index, message in enumerate(self.history)
            if index not in retained_indexes
        ]
        omitted_count = len(omitted)
        omitted_digest = hashlib.sha256(
            canonical_json_bytes(omitted)
        ).hexdigest()
        self.history = retained
        self.history_compaction.append(
            {
                "reason": "agent_provider_history_bound",
                "omitted_count": omitted_count,
                "source_sha256": omitted_digest,
            }
        )
        if (
            len(self.history_compaction)
            > MAX_AGENT_PROVIDER_HISTORY_COMPACTIONS
        ):
            self.history_compaction = self.history_compaction[
                -MAX_AGENT_PROVIDER_HISTORY_COMPACTIONS:
            ]


def _sensitivity_rank(value: ArtifactSensitivity) -> int:
    return {
        ArtifactSensitivity.PUBLIC: 0,
        ArtifactSensitivity.INTERNAL: 1,
        ArtifactSensitivity.SENSITIVE: 2,
        ArtifactSensitivity.SECRET: 3,
    }[ArtifactSensitivity(value)]


__all__ = [
    "AgentProviderClientError",
    "DurableAgentProviderClient",
    "MAX_AGENT_PROVIDER_BROKER_ATTEMPTS",
    "MAX_AGENT_PROVIDER_HISTORY_COMPACTIONS",
]
