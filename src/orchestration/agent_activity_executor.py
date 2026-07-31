"""Local durable composition for one already-claimed Agent Activity."""

from __future__ import annotations

import json
import math
import re
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from src.core.agent_loop import AgentContext, run_agent_loop

from .agent_execution_evidence import AgentExecutionEvidenceCollector
from .agent_provider_client import DurableAgentProviderClient
from .agent_request import (
    AgentActivityRequest,
    AgentActivityRequestArtifactStore,
    AgentActivityRequestError,
)
from .agent_terminal import (
    AgentActivityTerminalCommit,
    DurableAgentTerminalCommitter,
)
from .agent_tool_handler import (
    AgentToolExecutor,
    AgentToolSpec,
    DurableAgentToolHandler,
)
from .agent_provider_wire import MAX_AGENT_PROVIDER_TOOLS
from .artifacts import (
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    LocalArtifactStore,
    canonical_json_bytes,
)
from .models import AttemptStatus
from .scheduler import (
    ActivityAdmissionCandidate,
    ActivityClaim,
    DurableScheduler,
)


MAX_AGENT_ACTIVITY_TURNS = 40
MAX_AGENT_SYSTEM_PROMPT_BYTES = 1024 * 1024
MAX_AGENT_TOOLS_SCHEMA_BYTES = 1024 * 1024
MIN_AGENT_HEARTBEAT_INTERVAL_SECONDS = 0.01
MAX_AGENT_LEASE_RENEWAL_SECONDS = 60 * 60.0
_SAFE_METRIC_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]{0,63}$")
_ERROR_REASONS = frozenset(
    {
        "agent_activity_claim_invalid",
        "agent_activity_configuration_invalid",
        "agent_activity_execution_failed",
        "agent_activity_preflight_failed",
        "agent_activity_request_invalid",
        "agent_activity_terminal_failed",
    }
)


class AgentActivityExecutionError(RuntimeError, ValueError):
    """The local Agent composition failed at a fixed trust boundary."""

    def __init__(self, reason_code: str) -> None:
        if reason_code not in _ERROR_REASONS:
            raise ValueError("invalid Agent Activity execution reason code")
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True, repr=False)
class AgentActivityExecutionResult:
    """Verified terminal plus the user-facing result Artifact reference."""

    terminal: AgentActivityTerminalCommit
    result_artifact_ref: ArtifactRef = field(repr=False)

    def __repr__(self) -> str:
        return (
            "AgentActivityExecutionResult("
            f"run_id={self.terminal.receipt.run_id!r}, "
            f"attempt_id={self.terminal.receipt.attempt_id!r}, "
            "result_artifact_digest="
            f"{self.result_artifact_ref.sha256!r}, "
            f"replayed={self.terminal.replayed})"
        )


ProviderClientFactory = Callable[
    [
        AgentActivityRequest,
        ArtifactRef,
        AgentExecutionEvidenceCollector,
    ],
    DurableAgentProviderClient,
]
SystemPromptSource = Callable[[AgentActivityRequest], str]
ContextFactory = Callable[[AgentActivityRequest], AgentContext]


class _ClaimHeartbeat:
    """Renew one local Claim without retaining diagnostic exceptions."""

    def __init__(
        self,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        *,
        interval_seconds: float,
        lease_seconds: float,
    ) -> None:
        self._scheduler = scheduler
        self._claim = claim
        self._interval_seconds = interval_seconds
        self._lease_seconds = lease_seconds
        self._stop = threading.Event()
        self._failed = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def failed(self) -> bool:
        return self._failed.is_set()

    def start(self) -> bool:
        if not self._renew():
            return False
        self._thread = threading.Thread(
            target=self._run,
            name=f"agent-lease-{self._claim.attempt_id}",
            daemon=True,
        )
        self._thread.start()
        return True

    def stop(self) -> bool:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=5.0)
            if thread.is_alive():
                self._failed.set()
        return not self.failed

    def _run(self) -> None:
        try:
            while not self._stop.wait(self._interval_seconds):
                if not self._renew():
                    return
        except BaseException:
            self._failed.set()

    def _renew(self) -> bool:
        try:
            self._scheduler.renew_claim(
                self._claim,
                lease_seconds=self._lease_seconds,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            self._failed.set()
            return False
        return True


class DurableAgentActivityExecutor:
    """Run the Core loop with durable provider/tool/terminal boundaries."""

    def __init__(
        self,
        scheduler: DurableScheduler,
        artifact_store: LocalArtifactStore,
        *,
        provider_client_factory: ProviderClientFactory,
        tool_executor: AgentToolExecutor,
        tool_specs: Iterable[AgentToolSpec],
        tools_schema: Iterable[Mapping[str, Any]],
        system_prompt_source: SystemPromptSource,
        context_factory: ContextFactory | None = None,
        max_turns: int = MAX_AGENT_ACTIVITY_TURNS,
        heartbeat_interval_seconds: float = 20.0,
        lease_renewal_seconds: float = 60.0,
        terminal_committer: DurableAgentTerminalCommitter | None = None,
    ) -> None:
        heartbeat_interval = _duration(heartbeat_interval_seconds)
        lease_renewal = _duration(lease_renewal_seconds)
        if (
            not isinstance(scheduler, DurableScheduler)
            or not isinstance(artifact_store, LocalArtifactStore)
            or not callable(provider_client_factory)
            or not callable(system_prompt_source)
            or (context_factory is not None and not callable(context_factory))
            or type(max_turns) is not int
            or not 1 <= max_turns <= MAX_AGENT_ACTIVITY_TURNS
            or heartbeat_interval is None
            or lease_renewal is None
            or heartbeat_interval
            < MIN_AGENT_HEARTBEAT_INTERVAL_SECONDS
            or lease_renewal > MAX_AGENT_LEASE_RENEWAL_SECONDS
            or heartbeat_interval * 2 > lease_renewal
        ):
            raise AgentActivityExecutionError(
                "agent_activity_configuration_invalid"
            )
        specs_failed = False
        try:
            specs = tuple(tool_specs)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            specs_failed = True
            specs = ()
        if specs_failed:
            raise AgentActivityExecutionError(
                "agent_activity_configuration_invalid"
            )
        if (
            not specs
            or not all(type(spec) is AgentToolSpec for spec in specs)
            or len({spec.tool_name for spec in specs}) != len(specs)
        ):
            raise AgentActivityExecutionError(
                "agent_activity_configuration_invalid"
            )
        schemas = _tools_schema(tools_schema, specs)
        committer_failed = False
        try:
            committer = (
                terminal_committer
                or DurableAgentTerminalCommitter(
                    scheduler,
                    artifact_store,
                )
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            committer_failed = True
            committer = None
        if (
            committer_failed
            or not isinstance(committer, DurableAgentTerminalCommitter)
            or committer.scheduler is not scheduler
            or committer.artifact_store is not artifact_store
        ):
            raise AgentActivityExecutionError(
                "agent_activity_configuration_invalid"
            )
        self.scheduler = scheduler
        self.store = scheduler.store
        self.artifact_store = artifact_store
        self._provider_client_factory = provider_client_factory
        self._tool_executor = tool_executor
        self._tool_specs = specs
        self._tools_schema = schemas
        self._system_prompt_source = system_prompt_source
        self._context_factory = context_factory
        self._max_turns = max_turns
        self._heartbeat_interval_seconds = heartbeat_interval
        self._lease_renewal_seconds = lease_renewal
        self._terminal = committer
        self._request_artifacts = AgentActivityRequestArtifactStore(
            artifact_store
        )
        self._active_attempts: set[str] = set()
        self._active_lock = threading.Lock()

    @property
    def durable_result_recovery_ready(self) -> bool:
        """Exact cross-process Agent Loop recovery is not implemented yet."""

        return False

    @property
    def production_security_ready(self) -> bool:
        """Remote attestation and exact in-flight history remain external."""

        return False

    def __repr__(self) -> str:
        return (
            "DurableAgentActivityExecutor("
            f"tool_count={len(self._tool_specs)}, "
            f"max_turns={self._max_turns}, "
            "production_security_ready=False)"
        )

    def prepare(
        self,
        candidate: ActivityAdmissionCandidate,
    ) -> ArtifactRef:
        """Stage the exact request before the admission Claim transaction."""

        if (
            type(candidate) is not ActivityAdmissionCandidate
            or candidate.definition_digest
            != self.scheduler.workflow.definition_digest
            or candidate.claim.activity_kind != "agent"
        ):
            raise AgentActivityExecutionError(
                "agent_activity_request_invalid"
            )
        try:
            request = AgentActivityRequest.from_candidate(candidate)
        except (KeyboardInterrupt, SystemExit):
            raise
        except AgentActivityRequestError:
            request = None
        if (
            request is None
            or request.artifact_sensitivity
            is ArtifactSensitivity.SECRET
        ):
            raise AgentActivityExecutionError(
                "agent_activity_request_invalid"
            )
        stage_failed = False
        try:
            ref = self._request_artifacts.stage(candidate)
        except (KeyboardInterrupt, SystemExit):
            raise
        except AgentActivityRequestError:
            stage_failed = True
            ref = None
        if stage_failed or ref is None:
            raise AgentActivityExecutionError(
                "agent_activity_request_invalid"
            )
        return ref

    def execute(
        self,
        claim: ActivityClaim,
        request_ref: ArtifactRef,
    ) -> AgentActivityExecutionResult:
        """Execute one CLAIMED Agent Attempt and commit only verified success."""

        if type(claim) is not ActivityClaim:
            raise AgentActivityExecutionError(
                "agent_activity_claim_invalid"
            )
        with self._active_lock:
            if claim.attempt_id in self._active_attempts:
                raise AgentActivityExecutionError(
                    "agent_activity_claim_invalid"
                )
            self._active_attempts.add(claim.attempt_id)
        try:
            return self._execute_once(claim, request_ref)
        finally:
            with self._active_lock:
                self._active_attempts.discard(claim.attempt_id)

    def _execute_once(
        self,
        claim: ActivityClaim,
        request_ref: ArtifactRef,
    ) -> AgentActivityExecutionResult:
        """Execute after acquiring this process's Attempt slot."""

        request = self._preflight_request(claim, request_ref)
        collector = AgentExecutionEvidenceCollector(
            run_id=request.run_id,
            node_id=request.node_id,
            attempt_id=request.attempt_id,
            request_digest=request.request_digest,
            request_artifact_digest=request_ref.sha256,
            definition_digest=request.definition_digest,
            request_sensitivity=request_ref.sensitivity,
        )
        context = self._context(request, collector)
        client = self._provider_client(
            request,
            request_ref,
            collector,
        )
        preflight_failed = False
        try:
            handler = DurableAgentToolHandler(
                ctx=context,
                executor=self._tool_executor,
                receipt_store=self.store,
                result_store=self.artifact_store,
                request=request,
                request_ref=request_ref,
                collector=collector,
                tool_specs=self._tool_specs,
            )
            system_prompt = self._system_prompt(request)
            user_input = _user_input(request)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            preflight_failed = True
            handler = None
            system_prompt = None
            user_input = None
        if (
            preflight_failed
            or handler is None
            or system_prompt is None
            or user_input is None
        ):
            raise AgentActivityExecutionError(
                "agent_activity_preflight_failed"
            )
        self._start(claim)
        heartbeat = _ClaimHeartbeat(
            self.scheduler,
            claim,
            interval_seconds=self._heartbeat_interval_seconds,
            lease_seconds=self._lease_renewal_seconds,
        )
        try:
            heartbeat_started = heartbeat.start()
        except (KeyboardInterrupt, SystemExit):
            self._mark_uncertain(claim)
            raise
        except BaseException:
            heartbeat_started = False
        if not heartbeat_started:
            self._mark_uncertain(claim)
            raise AgentActivityExecutionError(
                "agent_activity_claim_invalid"
            )
        loop_failed = False
        try:
            loop_result = run_agent_loop(
                client=client,
                system_prompt=system_prompt,
                user_input=user_input,
                handler=handler,
                tools_schema=list(self._tools_schema),
                max_turns=self._max_turns,
            )
        except (KeyboardInterrupt, SystemExit):
            self._mark_uncertain(claim)
            raise
        except BaseException:
            self._mark_uncertain(claim)
            loop_failed = True
            loop_result = None
        finally:
            heartbeat_clean = heartbeat.stop()
        if not heartbeat_clean:
            self._mark_uncertain(claim)
            loop_failed = True
        if loop_failed or loop_result is None:
            raise AgentActivityExecutionError(
                "agent_activity_execution_failed"
            )
        if loop_result.get("exit_reason") != "CURRENT_TASK_DONE":
            self._complete_known_non_success(claim, loop_result)
            raise AgentActivityExecutionError(
                "agent_activity_execution_failed"
            )
        terminal_failed = False
        try:
            manifest = collector.finalize(
                exit_reason="CURRENT_TASK_DONE",
                turns=loop_result["turns"],
            )
            result_ref = self._stage_result(
                claim,
                loop_result,
                sensitivity=manifest.artifact_sensitivity,
            )
            evidence_refs = _unique_refs(
                (
                    result_ref,
                    *client.result_artifact_refs,
                )
            )
            terminal = self._terminal.commit_success(
                claim,
                request_ref,
                collector,
                exit_reason="CURRENT_TASK_DONE",
                turns=manifest.turns or 0,
                result_artifact_refs=evidence_refs,
                metrics=_terminal_metrics(loop_result),
            )
        except (KeyboardInterrupt, SystemExit):
            self._mark_uncertain(claim)
            raise
        except BaseException:
            self._mark_uncertain(claim)
            terminal_failed = True
            manifest = None
            result_ref = None
            terminal = None
        if (
            terminal_failed
            or manifest is None
            or result_ref is None
            or terminal is None
        ):
            raise AgentActivityExecutionError(
                "agent_activity_terminal_failed"
            )
        return AgentActivityExecutionResult(
            terminal=terminal,
            result_artifact_ref=result_ref,
        )

    def _preflight_request(
        self,
        claim: ActivityClaim,
        request_ref: ArtifactRef,
    ) -> AgentActivityRequest:
        if (
            type(claim) is not ActivityClaim
            or claim.activity_kind != "agent"
            or not claim.claim_token
            or claim.fencing_token < 1
        ):
            raise AgentActivityExecutionError(
                "agent_activity_claim_invalid"
            )
        load_failed = False
        try:
            request = self._request_artifacts.load(request_ref)
        except (KeyboardInterrupt, SystemExit):
            raise
        except AgentActivityRequestError:
            load_failed = True
            request = None
        if load_failed or request is None:
            raise AgentActivityExecutionError(
                "agent_activity_request_invalid"
            )
        attempt_failed = False
        try:
            attempt = self.store.get_attempt(claim.attempt_id)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            attempt_failed = True
            attempt = None
        if attempt_failed:
            raise AgentActivityExecutionError(
                "agent_activity_claim_invalid"
            )
        if (
            request.run_id != claim.run_id
            or request.node_id != claim.node_id
            or request.attempt_id != claim.attempt_id
            or request.attempt_number != claim.attempt_number
            or request.request_digest != claim.request_hash
            or request.definition_digest
            != self.scheduler.workflow.definition_digest
            or request.agent_name != claim.config.get("agent")
            or attempt is None
            or attempt.status is not AttemptStatus.CLAIMED
            or attempt.worker_id != claim.worker_id
            or attempt.lease_id != claim.claim_token
            or attempt.fencing_token != claim.fencing_token
        ):
            raise AgentActivityExecutionError(
                "agent_activity_claim_invalid"
            )
        return request

    def _context(
        self,
        request: AgentActivityRequest,
        collector: AgentExecutionEvidenceCollector,
    ) -> AgentContext:
        context_failed = False
        try:
            context = (
                AgentContext(display_fn=lambda _message: None)
                if self._context_factory is None
                else self._context_factory(request)
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            context_failed = True
            context = None
        if context_failed:
            raise AgentActivityExecutionError(
                "agent_activity_preflight_failed"
            )
        if (
            type(context) is not AgentContext
            or context.execution_evidence_observer is not None
            or context.agent_name not in {"", request.agent_name}
        ):
            raise AgentActivityExecutionError(
                "agent_activity_preflight_failed"
            )
        context.agent_name = request.agent_name
        context.execution_evidence_observer = collector
        if not context.session_id:
            context.session_id = (
                f"orchestration:{request.run_id}:{request.attempt_id}"
            )
        return context

    def _provider_client(
        self,
        request: AgentActivityRequest,
        request_ref: ArtifactRef,
        collector: AgentExecutionEvidenceCollector,
    ) -> DurableAgentProviderClient:
        client_failed = False
        try:
            client = self._provider_client_factory(
                request,
                request_ref,
                collector,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            client_failed = True
            client = None
        if client_failed:
            raise AgentActivityExecutionError(
                "agent_activity_preflight_failed"
            )
        if type(client) is not DurableAgentProviderClient:
            raise AgentActivityExecutionError(
                "agent_activity_preflight_failed"
            )
        return client

    def _system_prompt(self, request: AgentActivityRequest) -> str:
        prompt_failed = False
        try:
            value = self._system_prompt_source(request)
            encoded = value.encode("utf-8")
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            prompt_failed = True
            value = None
            encoded = b""
        if prompt_failed:
            raise AgentActivityExecutionError(
                "agent_activity_preflight_failed"
            )
        if (
            type(value) is not str
            or not value.strip()
            or len(encoded) > MAX_AGENT_SYSTEM_PROMPT_BYTES
        ):
            raise AgentActivityExecutionError(
                "agent_activity_preflight_failed"
            )
        return value

    def _start(self, claim: ActivityClaim) -> None:
        start_failed = False
        try:
            self.scheduler.start_claim(claim)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            start_failed = True
        if start_failed:
            try:
                attempt = self.store.get_attempt(claim.attempt_id)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                attempt = None
            if (
                attempt is None
                or attempt.status is not AttemptStatus.RUNNING
                or attempt.worker_id != claim.worker_id
                or attempt.lease_id != claim.claim_token
                or attempt.fencing_token != claim.fencing_token
            ):
                raise AgentActivityExecutionError(
                    "agent_activity_claim_invalid"
                )

    def _stage_result(
        self,
        claim: ActivityClaim,
        loop_result: Mapping[str, Any],
        *,
        sensitivity: ArtifactSensitivity,
    ) -> ArtifactRef:
        payload = {
            "schema_version": 1,
            "kind": "agent_activity_result",
            "response": loop_result.get("response", ""),
            "exit_reason": loop_result.get("exit_reason"),
            "turns": loop_result.get("turns"),
            "usage": loop_result.get("usage", {}),
        }
        stage_failed = False
        try:
            ref = self.artifact_store.put_json(
                payload,
                kind=ArtifactKind.MODEL_RESPONSE,
                sensitivity=sensitivity,
                producer_run_id=claim.run_id,
                producer_node_id=claim.node_id,
                producer_attempt_id=claim.attempt_id,
                metadata={},
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            stage_failed = True
            ref = None
        if stage_failed or ref is None:
            raise AgentActivityExecutionError(
                "agent_activity_terminal_failed"
            )
        return ref

    def _mark_uncertain(self, claim: ActivityClaim) -> None:
        try:
            self.scheduler.complete_claim(
                claim,
                {"error_code": "agent_activity_execution_uncertain"},
                attempt_status=AttemptStatus.OUTCOME_UNKNOWN,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass

    def _complete_known_non_success(
        self,
        claim: ActivityClaim,
        loop_result: Mapping[str, Any],
    ) -> None:
        status = (
            AttemptStatus.CANCELLED
            if loop_result.get("exit_reason") == "INTERRUPTED"
            else AttemptStatus.FAILED
        )
        completion_failed = False
        try:
            self.scheduler.complete_claim(
                claim,
                {"error_code": "agent_activity_incomplete"},
                attempt_status=status,
            )
        except (KeyboardInterrupt, SystemExit):
            self._mark_uncertain(claim)
            raise
        except BaseException:
            completion_failed = True
        if completion_failed:
            self._mark_uncertain(claim)
            raise AgentActivityExecutionError(
                "agent_activity_terminal_failed"
            )


def _tools_schema(
    values: Iterable[Mapping[str, Any]],
    specs: tuple[AgentToolSpec, ...],
) -> tuple[dict[str, Any], ...]:
    schema_failed = False
    try:
        raw = list(values)
        encoded = canonical_json_bytes(raw)
        normalized = json.loads(encoded.decode("utf-8"))
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        schema_failed = True
        raw = []
        encoded = b""
        normalized = None
    if schema_failed:
        raise AgentActivityExecutionError(
            "agent_activity_configuration_invalid"
        )
    if (
        not raw
        or len(raw) > MAX_AGENT_PROVIDER_TOOLS
        or len(encoded) > MAX_AGENT_TOOLS_SCHEMA_BYTES
        or type(normalized) is not list
        or any(
            type(item) is not dict
            or type(item.get("name")) is not str
            for item in normalized
        )
        or len({item["name"] for item in normalized}) != len(normalized)
        or {item["name"] for item in normalized}
        != {spec.tool_name for spec in specs}
    ):
        raise AgentActivityExecutionError(
            "agent_activity_configuration_invalid"
        )
    return tuple(normalized)


def _duration(value: Any) -> float | None:
    if (
        isinstance(value, bool)
        or type(value) not in {int, float}
    ):
        return None
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0:
        return None
    return normalized


def _user_input(request: AgentActivityRequest) -> str:
    sections = [request.task or ""]
    for label, value in (
        ("Context", request.context),
        ("Expected output", request.expected_output),
        ("Output target", request.output),
    ):
        if value:
            sections.append(f"{label}:\n{value}")
    if request.input_bindings:
        sections.append(
            "Input Artifacts:\n"
            + "\n".join(
                f"- {binding.name}: "
                + ", ".join(
                    artifact.sha256
                    for artifact in binding.artifacts
                )
                for binding in request.input_bindings
            )
        )
    value = "\n\n".join(section for section in sections if section)
    if not value:
        raise AgentActivityExecutionError(
            "agent_activity_request_invalid"
        )
    return value


def _unique_refs(values: Iterable[ArtifactRef]) -> tuple[ArtifactRef, ...]:
    refs: list[ArtifactRef] = []
    seen: set[str] = set()
    for ref in values:
        if type(ref) is not ArtifactRef:
            raise AgentActivityExecutionError(
                "agent_activity_terminal_failed"
            )
        if ref.sha256 in seen:
            continue
        seen.add(ref.sha256)
        refs.append(ref)
    return tuple(refs)


def _terminal_metrics(
    loop_result: Mapping[str, Any],
) -> dict[str, int | float | bool]:
    metrics: dict[str, int | float | bool] = {}
    turns = loop_result.get("turns")
    if type(turns) is int and turns >= 0:
        metrics["turns"] = turns
    usage = loop_result.get("usage")
    if isinstance(usage, Mapping):
        for key, value in usage.items():
            metric_key = f"usage.{key}"
            if (
                type(key) is str
                and _SAFE_METRIC_KEY.fullmatch(metric_key)
                and type(value) in {int, float}
                and not isinstance(value, bool)
                and (
                    type(value) is int
                    or math.isfinite(value)
                )
                and value >= 0
            ):
                metrics[metric_key] = value
    return metrics


__all__ = [
    "AgentActivityExecutionError",
    "AgentActivityExecutionResult",
    "DurableAgentActivityExecutor",
    "MAX_AGENT_ACTIVITY_TURNS",
]
