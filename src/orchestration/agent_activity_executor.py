"""Local durable composition for one already-claimed Agent Activity."""

from __future__ import annotations

import hashlib
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
from .agent_turn_checkpoint import (
    AgentTurnCheckpoint,
    AgentTurnCheckpointArtifactStore,
    AgentTurnCheckpointError,
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
        "agent_activity_checkpoint_failed",
        "agent_activity_configuration_invalid",
        "agent_activity_execution_failed",
        "agent_activity_preflight_failed",
        "agent_activity_request_invalid",
        "agent_activity_recovery_unavailable",
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
        self._checkpoint_artifacts = AgentTurnCheckpointArtifactStore(
            artifact_store
        )
        self._active_attempts: set[str] = set()
        self._active_lock = threading.Lock()

    @property
    def durable_result_recovery_ready(self) -> bool:
        """Safe-turn state supports live-Claim restart or fenced takeover."""

        return True

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

    def resume(
        self,
        claim: ActivityClaim,
        request_ref: ArtifactRef,
    ) -> AgentActivityExecutionResult:
        """Continue one RUNNING Attempt from its newest exact safe turn."""

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
            self._preflight_request(
                claim,
                request_ref,
                allow_running=True,
            )
            checkpoint, reset_authorization_lineage = (
                self._load_checkpoint(claim, request_ref)
            )
            return self._execute_once(
                claim,
                request_ref,
                checkpoint=checkpoint,
                reset_authorization_lineage=(
                    reset_authorization_lineage
                ),
            )
        finally:
            with self._active_lock:
                self._active_attempts.discard(claim.attempt_id)

    def _execute_once(
        self,
        claim: ActivityClaim,
        request_ref: ArtifactRef,
        *,
        checkpoint: AgentTurnCheckpoint | None = None,
        reset_authorization_lineage: bool = False,
    ) -> AgentActivityExecutionResult:
        """Execute after acquiring this process's Attempt slot."""

        request = self._preflight_request(
            claim,
            request_ref,
            allow_running=checkpoint is not None,
        )
        collector = (
            AgentExecutionEvidenceCollector(
                run_id=request.run_id,
                node_id=request.node_id,
                attempt_id=request.attempt_id,
                request_digest=request.request_digest,
                request_artifact_digest=request_ref.sha256,
                definition_digest=request.definition_digest,
                request_sensitivity=request_ref.sensitivity,
            )
            if checkpoint is None
            else AgentExecutionEvidenceCollector.from_checkpoint_manifest(
                checkpoint.evidence_manifest,
                request_sensitivity=request_ref.sensitivity,
            )
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
                checkpoint_manifest=(
                    None
                    if checkpoint is None
                    else checkpoint.evidence_manifest
                ),
            )
            system_prompt = self._system_prompt(request)
            user_input = _user_input(request)
            runtime_configuration_digest = (
                self._runtime_configuration_digest(
                    system_prompt,
                    client,
                )
            )
            if checkpoint is not None:
                if (
                    checkpoint.runtime_configuration_digest
                    != runtime_configuration_digest
                ):
                    raise AgentTurnCheckpointError(
                        "invalid_agent_turn_checkpoint"
                    )
                client.restore_checkpoint_state(
                    checkpoint.provider_state,
                    evidence_manifest=checkpoint.evidence_manifest,
                    reset_authorization_lineage=(
                        reset_authorization_lineage
                    ),
                )
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
        if checkpoint is None:
            self._start(claim)
        previous_checkpoint_digest = [
            None
            if checkpoint is None
            else checkpoint.checkpoint_digest
        ]
        context.durable_turn_callback = self._checkpoint_callback(
            claim=claim,
            request=request,
            request_ref=request_ref,
            collector=collector,
            client=client,
            runtime_configuration_digest=(
                runtime_configuration_digest
            ),
            previous_checkpoint_digest=previous_checkpoint_digest,
        )
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
                resume_state=(
                    None
                    if checkpoint is None
                    else dict(checkpoint.loop_state)
                ),
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
            self._complete_known_non_success(
                claim,
                loop_result,
                collector,
            )
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
        *,
        allow_running: bool = False,
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
            or attempt.status
            not in (
                {AttemptStatus.CLAIMED, AttemptStatus.RUNNING}
                if allow_running
                else {AttemptStatus.CLAIMED}
            )
            or attempt.worker_id != claim.worker_id
            or attempt.lease_id != claim.claim_token
            or attempt.fencing_token != claim.fencing_token
        ):
            raise AgentActivityExecutionError(
                "agent_activity_claim_invalid"
            )
        return request

    def _load_checkpoint(
        self,
        claim: ActivityClaim,
        request_ref: ArtifactRef,
    ) -> tuple[AgentTurnCheckpoint, bool]:
        failed = False
        try:
            record = self.store.get_latest_agent_turn_checkpoint(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
            )
            attempt = self.store.get_attempt(claim.attempt_id)
            checkpoint = (
                None
                if record is None
                else self._checkpoint_artifacts.load(
                    record.checkpoint_ref
                )
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            failed = True
            record = None
            attempt = None
            checkpoint = None
        checkpoint_adopted = (
            record is not None
            and attempt is not None
            and record.fencing_token != claim.fencing_token
            and self.store.agent_turn_checkpoint_is_adopted(
                claim.run_id,
                claim.node_id,
                claim.attempt_id,
                claim.request_hash,
                claim.worker_id,
                claim_token=claim.claim_token,
                fencing_token=claim.fencing_token,
                checkpoint_digest=record.checkpoint_digest,
                completed_turn=record.completed_turn,
                checkpoint_fencing_token=record.fencing_token,
                now=self.scheduler.current_time(),
            )
        )
        if (
            failed
            or record is None
            or attempt is None
            or checkpoint is None
            or record.run_id != claim.run_id
            or record.node_id != claim.node_id
            or record.attempt_id != claim.attempt_id
            or record.request_digest != claim.request_hash
            or (
                record.fencing_token != claim.fencing_token
                and not checkpoint_adopted
            )
            or record.completed_turn != checkpoint.completed_turn
            or record.checkpoint_digest
            != checkpoint.checkpoint_digest
            or record.previous_checkpoint_digest
            != checkpoint.previous_checkpoint_digest
            or record.checkpoint_ref.sha256
            != checkpoint.checkpoint_digest
            or not record.dependency_artifact_refs
            or record.dependency_artifact_refs[0] != request_ref
            or record.dependency_artifact_refs[1:]
            != checkpoint.dependency_artifact_refs
            or checkpoint.run_id != claim.run_id
            or checkpoint.node_id != claim.node_id
            or checkpoint.attempt_id != claim.attempt_id
            or checkpoint.request_digest != claim.request_hash
            or checkpoint.request_artifact_digest
            != request_ref.sha256
            or checkpoint.definition_digest
            != self.scheduler.workflow.definition_digest
        ):
            raise AgentActivityExecutionError(
                "agent_activity_recovery_unavailable"
            )
        return checkpoint, checkpoint_adopted

    def _checkpoint_callback(
        self,
        *,
        claim: ActivityClaim,
        request: AgentActivityRequest,
        request_ref: ArtifactRef,
        collector: AgentExecutionEvidenceCollector,
        client: DurableAgentProviderClient,
        runtime_configuration_digest: str,
        previous_checkpoint_digest: list[str | None],
    ) -> Callable[[dict[str, Any]], None]:
        def commit(snapshot: dict[str, Any]) -> None:
            try:
                completed_turn = snapshot["completed_turn"]
                manifest = collector.checkpoint_manifest(
                    completed_turn=completed_turn
                )
                provider_state = client.checkpoint_state()
                checkpoint = AgentTurnCheckpoint(
                    run_id=request.run_id,
                    node_id=request.node_id,
                    attempt_id=request.attempt_id,
                    request_digest=request.request_digest,
                    request_artifact_digest=request_ref.sha256,
                    definition_digest=request.definition_digest,
                    runtime_configuration_digest=(
                        runtime_configuration_digest
                    ),
                    completed_turn=completed_turn,
                    previous_checkpoint_digest=(
                        previous_checkpoint_digest[0]
                    ),
                    loop_state=snapshot,
                    provider_state=provider_state,
                    evidence_manifest=manifest,
                    artifact_sensitivity=(
                        manifest.artifact_sensitivity
                    ),
                )
                ref = self._checkpoint_artifacts.stage(checkpoint)
                record = self.store.commit_agent_turn_checkpoint(
                    claim.run_id,
                    claim.node_id,
                    claim.attempt_id,
                    claim.request_hash,
                    claim.worker_id,
                    claim_token=claim.claim_token,
                    fencing_token=claim.fencing_token,
                    completed_turn=completed_turn,
                    previous_checkpoint_digest=(
                        previous_checkpoint_digest[0]
                    ),
                    checkpoint_ref=ref,
                    request_ref=request_ref,
                    provider_response_refs=(
                        checkpoint.dependency_artifact_refs
                    ),
                    now=self.scheduler.current_time(),
                )
                if (
                    record.checkpoint_ref != ref
                    or record.checkpoint_digest
                    != checkpoint.checkpoint_digest
                ):
                    raise AgentTurnCheckpointError(
                        "invalid_agent_turn_checkpoint"
                    )
                previous_checkpoint_digest[0] = (
                    checkpoint.checkpoint_digest
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                raise AgentActivityExecutionError(
                    "agent_activity_checkpoint_failed"
                ) from None

        return commit

    def _runtime_configuration_digest(
        self,
        system_prompt: str,
        client: DurableAgentProviderClient,
    ) -> str:
        return hashlib.sha256(
            canonical_json_bytes(
                {
                    "schema": "agent_activity_checkpoint_runtime_v1",
                    "system_prompt_digest": hashlib.sha256(
                        system_prompt.encode("utf-8")
                    ).hexdigest(),
                    "tools_schema_digest": hashlib.sha256(
                        canonical_json_bytes(list(self._tools_schema))
                    ).hexdigest(),
                    "provider_configuration_digest": (
                        client.checkpoint_configuration_digest
                    ),
                    "max_turns": self._max_turns,
                }
            )
        ).hexdigest()

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
            or context.durable_turn_callback is not None
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
        collector: AgentExecutionEvidenceCollector,
    ) -> None:
        evidence_unknown = False
        try:
            manifest = collector.finalize(
                exit_reason=str(
                    loop_result.get("exit_reason") or "ERROR"
                ),
                turns=loop_result["turns"],
            )
            for binding in manifest.tool_receipts:
                receipt = self.store.get_tool_receipt(
                    binding.run_id,
                    binding.attempt_id,
                )
                if receipt is None:
                    evidence_unknown = True
                    break
                binding.validate_receipt(receipt)
                if receipt.attempt_status is AttemptStatus.OUTCOME_UNKNOWN:
                    evidence_unknown = True
                    break
        except (KeyboardInterrupt, SystemExit):
            self._mark_uncertain(claim)
            raise
        except BaseException:
            evidence_unknown = True
        if evidence_unknown:
            self._mark_uncertain(claim)
            return
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
