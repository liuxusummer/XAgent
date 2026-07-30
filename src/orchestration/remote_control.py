"""Reference control-plane endpoint for the transport-neutral worker protocol.

Only this object receives a :class:`DurableScheduler`; remote workers receive a
transport callback and wire models, never a Store handle or filesystem path.
"""

from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Protocol, runtime_checkable

from .artifact_broker import ArtifactOutputHandle
from .metadata_security import contains_sensitive_key
from .models import AttemptStatus, NodeStatus, RunStatus
from .remote_journal import (
    RemoteControlJournal,
    RemoteJournalCapacityError,
    RemoteJournalError,
    RemoteJournalIdentityError,
    RemoteJournalRequestConflict,
    RemoteJournalSessionSuperseded,
)
from .remote_protocol import (
    MAX_LEASE_SECONDS,
    MIN_LEASE_SECONDS,
    AuthenticatedWorker,
    ClaimBinding,
    ExecutionAuthorization,
    RemoteActivityDescriptor,
    RemoteRuntimeProof,
    RemoteOperation,
    RemoteProtocolError,
    RemoteRequest,
    WorkAssignment,
    canonical_digest,
    make_response,
    output_handles_from_completion,
    parse_request,
)
from .scheduler import (
    ACTIVITY_KINDS,
    ActivityClaim,
    DurableScheduler,
    SchedulerError,
)
from .store import (
    ActivityAdmissionDenied,
    IdempotencyConflictError,
    InvalidStateTransition,
    ProjectionConflictError,
)

SchedulerResolver = Callable[[str], DurableScheduler]
RunAuthorizer = Callable[[AuthenticatedWorker, str], bool]

MAX_REQUEST_CACHE = 4_096
MAX_WORKER_REGISTRATIONS = 4_096
MAX_REGISTRATION_IDLE_SECONDS = 86_400.0
MAX_INFLIGHT_WAIT_SECONDS = 10.0


@runtime_checkable
class RemoteAssignmentAdmitter(Protocol):
    """Trusted control-side policy/attestation gate.

    Implementations may inspect or commit durable policy state through the
    supplied Scheduler.  Policy decisions are never delegated to the worker.
    """

    production_security_ready: bool

    def preflight(
        self,
        identity: AuthenticatedWorker,
        registration: "WorkerRegistration",
        scheduler: DurableScheduler,
    ) -> None: ...

    def admit(
        self,
        identity: AuthenticatedWorker,
        registration: "WorkerRegistration",
        scheduler: DurableScheduler,
        claim: ActivityClaim,
    ) -> ExecutionAuthorization: ...

    def complete(
        self,
        identity: AuthenticatedWorker,
        registration: "WorkerRegistration",
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        authorization: ExecutionAuthorization,
        candidate: "RemoteCompletionCandidate",
        runtime_proof: RemoteRuntimeProof,
    ) -> None: ...

    def cancel(
        self,
        identity: AuthenticatedWorker,
        registration: "WorkerRegistration",
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        authorization: ExecutionAuthorization,
        runtime_proof: RemoteRuntimeProof,
    ) -> None: ...


class RemoteControlError(RuntimeError):
    """Safe public control-plane rejection with no internal diagnostic."""

    _ALLOWED_CODES = frozenset(
        {
            "already_registered",
            "authorization_conflict",
            "claim_conflict",
            "control_unavailable",
            "forbidden",
            "internal_error",
            "invalid_assignment",
            "not_registered",
            "registration_capacity",
            "request_id_conflict",
            "run_not_found",
            "security_not_ready",
            "unsupported_activity",
            "worker_identity_mismatch",
        }
    )

    def __init__(self, code: str) -> None:
        if code not in self._ALLOWED_CODES:
            raise ValueError("unsupported RemoteControlError code")
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class WorkerRegistration:
    worker_id: str
    tenant_id: str
    identity_digest: str
    instance_id: str
    runtime_version: str
    capabilities: tuple[str, ...]
    resource_keys: tuple[str, ...]
    activity_kinds: tuple[str, ...]
    max_concurrency: int
    session_epoch: int
    session_owner_id: str
    session_binding_digest: str


@runtime_checkable
class RemoteFleetPoller(Protocol):
    """Trusted cross-Run selector; durable claims remain in this control."""

    production_security_ready: bool

    def poll(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
    ) -> WorkAssignment | None: ...

    def release_terminal(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        claim: ClaimBinding,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class RemoteAdmissionAuthorization:
    """Opaque control-local ticket plus digest-only durable policy binding."""

    ticket_id: str = field(repr=False)
    candidate_digest: str
    expires_at: float
    policy_binding: Mapping[str, str | None]


@dataclass(frozen=True, slots=True)
class RemoteCompletionCandidate:
    """Untrusted worker candidate; only the trusted admitter may commit it."""

    outcome: AttemptStatus
    output_handles: tuple[ArtifactOutputHandle, ...]
    runtime_proof: RemoteRuntimeProof
    error_class: str | None
    error_code: str | None

    def __post_init__(self) -> None:
        outcome = AttemptStatus(self.outcome)
        if outcome not in {
            AttemptStatus.SUCCEEDED,
            AttemptStatus.FAILED,
            AttemptStatus.TIMED_OUT,
            AttemptStatus.ABANDONED,
            AttemptStatus.OUTCOME_UNKNOWN,
        }:
            raise ValueError("invalid remote completion outcome")
        object.__setattr__(self, "outcome", outcome)
        handles = tuple(self.output_handles)
        if not all(isinstance(handle, ArtifactOutputHandle) for handle in handles):
            raise ValueError("invalid output handles")
        object.__setattr__(self, "output_handles", handles)
        if not isinstance(self.runtime_proof, RemoteRuntimeProof):
            raise ValueError("invalid runtime proof")
        if outcome is AttemptStatus.SUCCEEDED:
            if (
                not self.output_handles
                or self.error_class is not None
                or self.error_code is not None
            ):
                raise ValueError("invalid successful completion candidate")
        elif (
            self.output_handles
            or self.error_class is None
            or self.error_code is None
        ):
            raise ValueError("invalid failed completion candidate")


@dataclass(slots=True)
class _Inflight:
    request_digest: str
    completed: threading.Event


@dataclass(slots=True)
class _WorkerSessionGuard:
    lock: threading.Lock
    users: int = 0


class RemoteControlPlane:
    """Bounded in-process reference endpoint.

    A real HTTP/gRPC/queue adapter should authenticate the peer first, construct
    :class:`AuthenticatedWorker`, then pass the decoded JSON object to
    :meth:`handle`.  The adapter must not expose ``scheduler`` or its Store.
    """

    def __init__(
        self,
        scheduler_resolver: SchedulerResolver,
        *,
        authorize_run: RunAuthorizer,
        max_request_cache: int = MAX_REQUEST_CACHE,
        max_registrations: int = 1_024,
        registration_idle_seconds: float = 3_600.0,
        clock: Callable[[], float] = time.monotonic,
        assignment_admitter: RemoteAssignmentAdmitter | None = None,
        allow_reference_admission: bool = False,
        journal: RemoteControlJournal | None = None,
    ) -> None:
        if not callable(scheduler_resolver) or not callable(authorize_run):
            raise TypeError("scheduler_resolver and authorize_run must be callable")
        if (
            isinstance(max_request_cache, bool)
            or not isinstance(max_request_cache, int)
            or not 1 <= max_request_cache <= MAX_REQUEST_CACHE
        ):
            raise ValueError(
                f"max_request_cache must be between 1 and {MAX_REQUEST_CACHE}"
            )
        if (
            isinstance(max_registrations, bool)
            or not isinstance(max_registrations, int)
            or not 1 <= max_registrations <= MAX_WORKER_REGISTRATIONS
        ):
            raise ValueError(
                "max_registrations must be between 1 and "
                f"{MAX_WORKER_REGISTRATIONS}"
            )
        try:
            idle_seconds = float(registration_idle_seconds)
        except (TypeError, ValueError) as exc:
            raise ValueError("registration_idle_seconds must be finite") from exc
        if not 1.0 <= idle_seconds <= MAX_REGISTRATION_IDLE_SECONDS:
            raise ValueError(
                "registration_idle_seconds must be between 1 and "
                f"{MAX_REGISTRATION_IDLE_SECONDS}"
            )
        if not callable(clock):
            raise TypeError("clock must be callable")
        if assignment_admitter is not None and not isinstance(
            assignment_admitter,
            RemoteAssignmentAdmitter,
        ):
            raise TypeError(
                "assignment_admitter must implement RemoteAssignmentAdmitter"
            )
        if not isinstance(allow_reference_admission, bool):
            raise TypeError("allow_reference_admission must be a bool")
        if journal is not None and not isinstance(journal, RemoteControlJournal):
            raise TypeError("journal must be a RemoteControlJournal")
        self._scheduler_resolver = scheduler_resolver
        self._authorize_run = authorize_run
        self._max_request_cache = max_request_cache
        self._max_registrations = max_registrations
        self._registration_idle_seconds = idle_seconds
        self._clock = clock
        self._assignment_admitter = assignment_admitter
        self._allow_reference_admission = allow_reference_admission
        # The in-memory default preserves backwards-compatible reference
        # composition only. Deployments and restart tests must inject a journal
        # backed by a protected control-plane SQLite path.
        self._journal = journal or RemoteControlJournal()
        self._lock = threading.RLock()
        # This is a bounded authentication/session cache, never a scheduling
        # fact source.  Run/Attempt ownership remains exclusively in Store.
        self._registrations: OrderedDict[
            str,
            tuple[WorkerRegistration, float],
        ] = OrderedDict()
        self._response_cache: OrderedDict[
            tuple[str, str, str],
            tuple[str, dict[str, Any]],
        ] = OrderedDict()
        self._inflight: dict[tuple[str, str, str], _Inflight] = {}
        self._worker_session_guards: dict[str, _WorkerSessionGuard] = {}
        self._fleet_poller: RemoteFleetPoller | None = None

    @property
    def production_security_ready(self) -> bool:
        """Whether a network transport may expose this control composition."""

        adapter = self._assignment_admitter
        return (
            self._journal.durable
            and adapter is not None
            and getattr(adapter, "production_security_ready", None) is True
            and getattr(adapter, "secure_two_phase_admission", None) is True
            and not self._allow_reference_admission
        )

    @property
    def production_fleet_ready(self) -> bool:
        poller = self._fleet_poller
        try:
            return (
                self.production_security_ready
                and poller is not None
                and poller.production_security_ready is True
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return False

    def bind_fleet_poller(self, poller: RemoteFleetPoller) -> None:
        """Bind one production fleet selector before serving fleet polls."""

        if not isinstance(poller, RemoteFleetPoller):
            raise TypeError("poller must implement RemoteFleetPoller")
        try:
            ready = poller.production_security_ready
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as exc:
            raise RemoteControlError("security_not_ready") from exc
        if ready is not True:
            raise RemoteControlError("security_not_ready")
        with self._lock:
            if self._fleet_poller is not None:
                raise RemoteControlError("already_registered")
            self._fleet_poller = poller

    def handle(
        self,
        identity: AuthenticatedWorker,
        wire_request: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Validate, authorize, execute, and return one canonical response."""

        if not isinstance(identity, AuthenticatedWorker):
            raise TypeError("identity must be an AuthenticatedWorker")
        request = parse_request(wire_request)
        if request.worker_id != identity.worker_id:
            return self._error_response(request, "worker_identity_mismatch")

        registration: WorkerRegistration | None = None
        if request.operation is not RemoteOperation.REGISTER:
            try:
                registration = self._require_registration(identity, request)
            except RemoteControlError as exc:
                return self._error_response(request, exc.code)
        session_scope = (
            f"register:{request.instance_id}"
            if registration is None
            else f"epoch:{registration.session_epoch}"
        )
        cache_key = (request.worker_id, session_scope, request.request_id)
        try:
            owner, cached, known = self._enter_request(
                cache_key,
                request,
                identity=identity,
                registration=registration,
            )
        except RemoteControlError as exc:
            return self._error_response(request, exc.code)
        if cached is not None:
            return cached
        if known:
            try:
                replay = self._replay_known_request(
                    identity,
                    registration,
                    request,
                )
            except RemoteControlError as exc:
                return self._error_response(request, exc.code)
            if replay is not None:
                return replay
        if not owner:
            return self._error_response(request, "control_unavailable")

        try:
            response = self._dispatch(identity, request)
        except RemoteControlError as exc:
            response = self._error_response(request, exc.code)
        except (
            IdempotencyConflictError,
            InvalidStateTransition,
            ProjectionConflictError,
            SchedulerError,
        ):
            response = self._error_response(request, "claim_conflict")
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            response = self._error_response(request, "internal_error")
        self._finish_request(cache_key, request.request_digest, response)
        return _detach(response)

    def _replay_known_request(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration | None,
        request: RemoteRequest,
    ) -> dict[str, Any] | None:
        """Replay only durable terminal evidence after response-body eviction."""

        if (
            registration is None
            or request.operation
            not in {RemoteOperation.COMPLETE, RemoteOperation.ACK_CANCEL}
        ):
            return None
        claim = ClaimBinding.from_wire(request.body["claim"])
        if not self._authorize_run(identity, claim.run_id):
            raise RemoteControlError("forbidden")
        scheduler = self._scheduler(claim.run_id)
        return self._terminal_replay(
            registration,
            request,
            scheduler,
            claim,
        )

    def request_cancel(self, identity: AuthenticatedWorker, run_id: str) -> None:
        """Trusted control-plane cancellation entry point.

        Cancellation intent is persisted by the Scheduler.  Workers observe it
        with ``cancellation_status`` and acknowledge process termination with
        ``ack_cancel``.
        """

        if not self._authorize_run(identity, run_id):
            raise RemoteControlError("forbidden")
        scheduler = self._scheduler(run_id)
        scheduler.request_cancel(run_id)

    def get_registration(self, worker_id: str) -> WorkerRegistration | None:
        with self._lock:
            self._expire_registrations_locked(self._now())
            item = self._registrations.get(worker_id)
            return None if item is None else item[0]

    def _dispatch(
        self,
        identity: AuthenticatedWorker,
        request: RemoteRequest,
    ) -> dict[str, Any]:
        if request.operation is RemoteOperation.REGISTER:
            return self._register(identity, request)
        registration = self._require_registration(identity, request)
        if request.operation is RemoteOperation.POLL:
            return self._poll(identity, registration, request)
        if request.operation is RemoteOperation.POLL_FLEET:
            return self._poll_fleet(identity, registration, request)

        claim = ClaimBinding.from_wire(request.body["claim"])
        if not self._authorize_run(identity, claim.run_id):
            raise RemoteControlError("forbidden")
        scheduler = self._scheduler(claim.run_id)
        if request.operation in {
            RemoteOperation.COMPLETE,
            RemoteOperation.ACK_CANCEL,
        }:
            replay = self._terminal_replay(
                registration,
                request,
                scheduler,
                claim,
            )
            if replay is not None:
                return replay
        handle = scheduler.restore_claim(
            claim.run_id,
            claim.node_id,
            claim.attempt_id,
            registration.session_owner_id,
            request_hash=claim.activity_request_digest,
            claim_token=claim.claim_token,
            fencing_token=claim.fencing_token,
        )
        self._validate_handle(handle, claim, registration)
        authorization = self._authorize_claim(
            identity,
            registration,
            scheduler,
            handle,
        )
        if (
            claim.action_digest != authorization.action_digest
            or claim.authorization_digest != authorization.authorization_digest
            or claim.profile_digest != authorization.profile_digest
            or claim.request_digest != authorization.request_digest
            or claim.session_binding_digest
            != authorization.session_binding_digest
            or claim.grant_binding_digest
            != authorization.grant_binding_digest
            or claim.execution_plan_digest
            != authorization.execution_plan_digest
            or claim.runtime_attestation_digest
            != authorization.runtime_attestation_digest
        ):
            raise RemoteControlError("authorization_conflict")

        if request.operation is RemoteOperation.START:
            return self._start(request, scheduler, handle)
        if request.operation is RemoteOperation.HEARTBEAT:
            return self._heartbeat(request, scheduler, handle)
        if request.operation is RemoteOperation.CANCELLATION_STATUS:
            return self._cancellation_status(request, scheduler, handle)
        if request.operation is RemoteOperation.COMPLETE:
            return self._complete(
                identity,
                registration,
                request,
                scheduler,
                handle,
                authorization,
            )
        if request.operation is RemoteOperation.ACK_CANCEL:
            return self._ack_cancel(
                identity,
                registration,
                request,
                scheduler,
                handle,
                authorization,
            )
        raise RemoteProtocolError("unsupported_operation")

    def _terminal_replay(
        self,
        registration: WorkerRegistration,
        request: RemoteRequest,
        scheduler: DurableScheduler,
        claim: ClaimBinding,
    ) -> dict[str, Any] | None:
        """Replay one exact durable terminal result without volatile authority."""

        attempt = scheduler.store.get_attempt(claim.attempt_id)
        if attempt is None or not attempt.status.is_terminal:
            return None
        receipt = scheduler.store.get_tool_receipt(
            claim.run_id,
            claim.attempt_id,
        )
        record = scheduler.store.get_idempotency(
            claim.run_id,
            attempt.idempotency_key,
        )
        terminal = next(
            (
                event
                for event in reversed(
                    scheduler.store.list_events(claim.run_id)
                )
                if event.attempt_id == claim.attempt_id
                and event.event_type
                == f"attempt.{attempt.status.value}"
            ),
            None,
        )
        try:
            proof = RemoteRuntimeProof.from_wire(
                request.body["runtime_proof"]
            )
        except (KeyError, TypeError, ValueError, RemoteProtocolError) as exc:
            raise RemoteControlError("authorization_conflict") from exc
        evidence = (
            None
            if terminal is None
            else terminal.payload.get("remote_evidence")
        )
        remote_evidence_fields = {
            "authorization_digest",
            "execution_plan_digest",
            "grant_binding_digest",
            "runtime_proof_digest",
            "session_binding_digest",
            "runtime_attestation_digest",
            "sandbox_spec_digest",
        }
        if (
            receipt is None
            or terminal is None
            or not isinstance(evidence, Mapping)
            or set(evidence) != remote_evidence_fields
        ):
            # Only an exact remote terminal record is replayable.  Local or
            # lease-reaper terminals must continue through restore_claim so a
            # stale worker observes the durable fencing conflict.
            return None
        expected_outcome = (
            AttemptStatus.CANCELLED
            if request.operation is RemoteOperation.ACK_CANCEL
            else AttemptStatus(str(request.body["outcome"]))
        )
        output_handles = (
            ()
            if request.operation is RemoteOperation.ACK_CANCEL
            else output_handles_from_completion(request.body)
        )
        try:
            proof.validate_binding(
                claim,
                outcome=expected_outcome.value,
                output_handles=output_handles,
            )
        except (RemoteProtocolError, TypeError, ValueError) as exc:
            raise RemoteControlError("authorization_conflict") from exc
        sandbox_receipt = (
            None if receipt is None else receipt.sandbox_receipt
        )
        error_fields_match = (
            request.operation is RemoteOperation.ACK_CANCEL
            or (
                (
                    request.body.get("error_class") is None
                    and request.body.get("error_code") is None
                )
                if expected_outcome is AttemptStatus.SUCCEEDED
                else (
                    request.body.get("error_class") == "sandbox"
                    and request.body.get("error_code")
                    == proof.sandbox_receipt.get("error_code")
                )
            )
        )
        required_evidence = {
            "authorization_digest": claim.authorization_digest,
            "execution_plan_digest": claim.execution_plan_digest,
            "grant_binding_digest": claim.grant_binding_digest,
            "runtime_proof_digest": proof.proof_digest,
            "session_binding_digest": claim.session_binding_digest,
            "runtime_attestation_digest": (
                claim.runtime_attestation_digest
            ),
            "sandbox_spec_digest": proof.sandbox_spec_digest,
        }
        if (
            record is None
            or dict(evidence) != required_evidence
            or attempt.run_id != claim.run_id
            or attempt.node_id != claim.node_id
            or attempt.metadata.get("request_hash")
            != claim.activity_request_digest
            or attempt.fencing_token != claim.fencing_token
            or attempt.lease_id != claim.claim_token
            or record.owner_id != registration.session_owner_id
            or record.claim_token != claim.claim_token
            or record.claim_count != claim.fencing_token
            or receipt.run_id != claim.run_id
            or receipt.node_id != claim.node_id
            or receipt.attempt_id != claim.attempt_id
            or receipt.attempt_status is not expected_outcome
            or receipt.action_digest != claim.action_digest
            or receipt.profile_digest != claim.profile_digest
            or sandbox_receipt is None
            or sandbox_receipt.get("request_digest")
            != claim.request_digest
            or dict(sandbox_receipt) != dict(proof.sandbox_receipt)
            or proof.sandbox_receipt_digest
            != receipt.sandbox_receipt_digest
            or claim.session_binding_digest
            != registration.session_binding_digest
            or not error_fields_match
        ):
            raise RemoteControlError("authorization_conflict")
        self._release_fleet_terminal(
            registration,
            ClaimBinding.from_wire(request.body["claim"]),
        )
        return make_response(
            request,
            ok=True,
            body={
                "accepted": True,
                "run_id": claim.run_id,
                "node_id": claim.node_id,
                "attempt_id": claim.attempt_id,
                "fencing_token": claim.fencing_token,
                "outcome": expected_outcome.value,
            },
        ).to_wire()

    def _register(
        self,
        identity: AuthenticatedWorker,
        request: RemoteRequest,
    ) -> dict[str, Any]:
        # Session replacement and the secure poll linearization boundary use
        # the same per-worker guard.  The guard is acquired without holding the
        # global control lock, so an unrelated Worker remains independent.
        with self._worker_session_guard(identity.worker_id):
            return self._register_guarded(identity, request)

    def _register_guarded(
        self,
        identity: AuthenticatedWorker,
        request: RemoteRequest,
    ) -> dict[str, Any]:
        body = request.body
        now = self._now()
        with self._lock:
            self._expire_registrations_locked(now)
            current_item = self._registrations.get(identity.worker_id)
            current = None if current_item is None else current_item[0]
            if current is not None and (
                current.tenant_id != identity.tenant_id
                or current.identity_digest != identity.identity_digest
            ):
                raise RemoteControlError("worker_identity_mismatch")
            if current is None and len(self._registrations) >= self._max_registrations:
                raise RemoteControlError("registration_capacity")

        try:
            session = self._journal.register_session(
                worker_id=identity.worker_id,
                tenant_id=identity.tenant_id,
                identity_digest=identity.identity_digest,
                instance_id=request.instance_id,
                request_id=request.request_id,
                request_digest=request.request_digest,
                operation=request.operation.value,
                now=now,
            )
        except RemoteJournalRequestConflict as exc:
            raise RemoteControlError("request_id_conflict") from exc
        except (
            RemoteJournalIdentityError,
            RemoteJournalSessionSuperseded,
        ) as exc:
            raise RemoteControlError("worker_identity_mismatch") from exc
        except (RemoteJournalCapacityError, RemoteJournalError) as exc:
            raise RemoteControlError("control_unavailable") from exc
        registration = WorkerRegistration(
            worker_id=identity.worker_id,
            tenant_id=identity.tenant_id,
            identity_digest=identity.identity_digest,
            instance_id=request.instance_id,
            runtime_version=str(body["runtime_version"]),
            capabilities=tuple(body["capabilities"]),
            resource_keys=tuple(body["resource_keys"]),
            activity_kinds=tuple(body["activity_kinds"]),
            max_concurrency=int(body["max_concurrency"]),
            session_epoch=session.epoch,
            session_owner_id=_session_owner_id(
                identity,
                request.instance_id,
                session.epoch,
            ),
            session_binding_digest=_session_binding_digest(
                identity,
                request.instance_id,
                session.epoch,
            ),
        )
        with self._lock:
            publish_now = self._now()
            self._expire_registrations_locked(publish_now)
            current_item = self._registrations.get(identity.worker_id)
            current = None if current_item is None else current_item[0]
            if current is not None and (
                current.tenant_id != identity.tenant_id
                or current.identity_digest != identity.identity_digest
                or current.session_epoch > registration.session_epoch
            ):
                raise RemoteControlError("worker_identity_mismatch")
            if current is None and len(self._registrations) >= self._max_registrations:
                raise RemoteControlError("registration_capacity")
            self._registrations[identity.worker_id] = (
                registration,
                publish_now,
            )
            self._registrations.move_to_end(identity.worker_id)
        return make_response(
            request,
            ok=True,
            body={
                "accepted": True,
                "protocol_version": 1,
                "worker_id": identity.worker_id,
                "instance_id": request.instance_id,
                "registration_digest": _registration_digest(registration),
            },
        ).to_wire()

    def _poll(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        request: RemoteRequest,
    ) -> dict[str, Any]:
        assignment = self._claim_assignment(
            identity,
            registration,
            run_id=str(request.body["run_id"]),
            lease_seconds=float(request.body["lease_seconds"]),
        )
        return make_response(
            request,
            ok=True,
            body={
                "assignment": (
                    None if assignment is None else assignment.to_wire()
                )
            },
        ).to_wire()

    def _poll_fleet(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        request: RemoteRequest,
    ) -> dict[str, Any]:
        poller = self._fleet_poller
        try:
            ready = (
                poller is not None
                and poller.production_security_ready is True
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as exc:
            raise RemoteControlError("security_not_ready") from exc
        if not ready:
            raise RemoteControlError("security_not_ready")
        try:
            assignment = poller.poll(identity, registration)
        except RemoteControlError:
            raise
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as exc:
            raise RemoteControlError("control_unavailable") from exc
        if assignment is not None and (
            not isinstance(assignment, WorkAssignment)
            or assignment.worker_id != registration.worker_id
        ):
            raise RemoteControlError("authorization_conflict")
        return make_response(
            request,
            ok=True,
            body={
                "assignment": (
                    None if assignment is None else assignment.to_wire()
                )
            },
        ).to_wire()

    def claim_for_fleet(
        self,
        run_id: str,
        worker_id: str,
        *,
        lease_seconds: float,
        node_id: str,
        activity_config_digest: str,
        expected_session_binding_digest: str,
    ) -> WorkAssignment:
        """Trusted Fleet callback that preserves current session authority."""

        if not self.production_security_ready:
            raise RemoteControlError("security_not_ready")
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(float(lease_seconds))
            or not MIN_LEASE_SECONDS
            <= float(lease_seconds)
            <= MAX_LEASE_SECONDS
        ):
            raise RemoteControlError("claim_conflict")
        registration = self.get_registration(worker_id)
        if registration is None:
            raise RemoteControlError("not_registered")
        if (
            registration.session_binding_digest
            != expected_session_binding_digest
        ):
            raise RemoteControlError("worker_identity_mismatch")
        identity = AuthenticatedWorker(
            worker_id=registration.worker_id,
            tenant_id=registration.tenant_id,
            identity_digest=registration.identity_digest,
        )
        assignment = self._claim_assignment(
            identity,
            registration,
            run_id=run_id,
            lease_seconds=lease_seconds,
            expected_node_id=node_id,
            expected_config_digest=activity_config_digest,
        )
        if assignment is None:
            raise RemoteControlError("claim_conflict")
        return assignment

    def fleet_claim_is_terminal(self, claim: ClaimBinding) -> bool:
        """Probe durable truth for one previously issued Fleet assignment."""

        if not isinstance(claim, ClaimBinding):
            raise TypeError("claim must be a ClaimBinding")
        scheduler = self._scheduler(claim.run_id)
        attempt = scheduler.store.get_attempt(claim.attempt_id)
        if (
            attempt is None
            or attempt.run_id != claim.run_id
            or attempt.node_id != claim.node_id
            or attempt.metadata.get("request_hash")
            != claim.activity_request_digest
        ):
            raise RemoteControlError("claim_conflict")
        return attempt.status.is_terminal

    def _claim_assignment(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        *,
        run_id: str,
        lease_seconds: float,
        expected_node_id: str | None = None,
        expected_config_digest: str | None = None,
    ) -> WorkAssignment | None:
        if not self._authorize_run(identity, run_id):
            raise RemoteControlError("forbidden")
        scheduler = self._scheduler(run_id)
        if expected_node_id is None:
            candidate_nodes = tuple(scheduler.workflow.nodes)
        else:
            try:
                candidate_nodes = (
                    scheduler.workflow.get_node(expected_node_id),
                )
            except KeyError:
                return None
        required_kinds = {
            node.kind
            for node in candidate_nodes
            if node.kind in ACTIVITY_KINDS
        }
        required_capabilities = {
            f"activity.{activity_kind}" for activity_kind in required_kinds
        }
        if (
            not required_kinds.issubset(registration.activity_kinds)
            or not required_capabilities.issubset(registration.capabilities)
        ):
            raise RemoteControlError("unsupported_activity")
        for node in candidate_nodes:
            if node.kind in ACTIVITY_KINDS and contains_sensitive_key(
                node.config.to_dict()
            ):
                raise RemoteControlError("invalid_assignment")
        self._require_security_admitter()
        adapter = self._require_security_admitter()
        secure_two_phase = (
            getattr(adapter, "secure_two_phase_admission", False) is True
        )
        if secure_two_phase:
            prepare = getattr(adapter, "prepare_admission", None)
            begin = getattr(adapter, "begin_admission", None)
            commit = getattr(adapter, "commit_admission", None)
            cancel = getattr(adapter, "cancel_admission", None)
            if not all(
                callable(item)
                for item in (prepare, begin, commit, cancel)
            ):
                raise RemoteControlError("security_not_ready")
        else:
            reference_only = (
                getattr(adapter, "reference_admission_only", False) is True
            )
            if not (
                self._allow_reference_admission and reference_only
            ):
                raise RemoteControlError("security_not_ready")
        candidate = scheduler.prepare_next_admission(
            run_id,
            registration.session_owner_id,
            resource_keys=registration.resource_keys,
            target_node_id=expected_node_id,
        )
        if candidate is None:
            return None
        candidate_config_digest = canonical_digest(
            dict(candidate.claim.config)
        )
        if (
            expected_config_digest is not None
            and candidate_config_digest != expected_config_digest
        ):
            return None
        activity = scheduler.workflow.get_node(candidate.claim.node_id)
        compatibility = activity.runtime_compatibility
        if (
            compatibility is not None
            and not compatibility.accepts(registration.runtime_version)
        ):
            return None
        try:
            adapter.preflight(identity, registration, scheduler)
        except RemoteControlError:
            raise
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise RemoteControlError("authorization_conflict") from exc
        if not secure_two_phase:
            # Compatibility path for the bounded protocol/reference adapters.
            # Production secure composition implements an opaque candidate
            # ticket and never enters this fallback.
            self._authorize_claim(
                identity,
                registration,
                scheduler,
                candidate.claim,
            )
            claim = scheduler.claim_next(
                run_id,
                registration.session_owner_id,
                capacity=registration.max_concurrency,
                resource_keys=registration.resource_keys,
                lease_seconds=lease_seconds,
            )
            authorization = (
                None
                if claim is None
                else self._authorize_claim(
                    identity,
                    registration,
                    scheduler,
                    claim,
                )
            )
        else:
            try:
                admission = prepare(
                    identity,
                    registration,
                    scheduler,
                    candidate,
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as exc:
                raise RemoteControlError(
                    "authorization_conflict"
                ) from exc
            if not isinstance(admission, RemoteAdmissionAuthorization):
                raise RemoteControlError("authorization_conflict")
            try:
                admission_expires_at = begin(
                    identity,
                    registration,
                    scheduler,
                    candidate,
                    admission,
                )
            except (KeyboardInterrupt, SystemExit):
                cancel(admission)
                raise
            except BaseException as exc:
                cancel(admission)
                raise RemoteControlError(
                    "authorization_conflict"
                ) from exc
            try:
                claim, policy_event = scheduler.claim_admitted(
                    candidate,
                    lease_seconds=lease_seconds,
                    capacity=registration.max_concurrency,
                    admission_expires_at=admission_expires_at,
                    policy_binding=admission.policy_binding,
                    linearization_guard=lambda: (
                        self._worker_session_guard(identity.worker_id)
                    ),
                    linearization_validator=lambda: (
                        self._registration_is_current(registration)
                    ),
                )
            except (KeyboardInterrupt, SystemExit):
                cancel(admission)
                raise
            except ActivityAdmissionDenied:
                cancel(admission)
                if not self._registration_is_current(registration):
                    raise RemoteControlError(
                        "worker_identity_mismatch"
                    )
                claim = None
                authorization = None
            except (
                IdempotencyConflictError,
                ProjectionConflictError,
                SchedulerError,
            ):
                cancel(admission)
                claim = None
                authorization = None
            except BaseException as exc:
                cancel(admission)
                raise RemoteControlError("internal_error") from exc
            else:
                if claim is None:
                    # REQUIRE_APPROVAL commits only the durable request.  The
                    # preclaim ticket is no longer useful and must not issue
                    # Worker or Artifact authority.
                    cancel(admission)
                    authorization = None
                    policy_event = None
                    return None
                with self._worker_session_guard(identity.worker_id):
                    if not self._registration_is_current(registration):
                        cancel(admission)
                        raise RemoteControlError(
                            "worker_identity_mismatch"
                        )
                    try:
                        authorization = commit(
                            identity,
                            registration,
                            scheduler,
                            candidate,
                            claim,
                            admission,
                            policy_event,
                        )
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except BaseException as exc:
                        # The durable claim and policy authorization are
                        # explicit. Broker/admitter failure now converges
                        # through lease recovery; it must never be reported as
                        # zero mutation.
                        raise RemoteControlError(
                            "control_unavailable"
                        ) from exc
                    authorization = self._validate_authorization(
                        identity,
                        registration,
                        claim,
                        authorization,
                    )
        if claim is None:
            return None
        assert authorization is not None
        return self._assignment(
            claim,
            authorization,
            registration,
        )

    def _start(
        self,
        request: RemoteRequest,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
    ) -> dict[str, Any]:
        run = scheduler.store.get_run(claim.run_id)
        attempt = scheduler.store.get_attempt(claim.attempt_id)
        if run is None or attempt is None:
            raise RemoteControlError("claim_conflict")
        if attempt.status is AttemptStatus.CLAIMED:
            if run.status is not RunStatus.RUNNING:
                raise RemoteControlError("claim_conflict")
            scheduler.start_claim(claim)
        elif attempt.status is not AttemptStatus.RUNNING:
            raise RemoteControlError("claim_conflict")
        return make_response(
            request,
            ok=True,
            body={
                "started": True,
                "run_id": claim.run_id,
                "node_id": claim.node_id,
                "attempt_id": claim.attempt_id,
                "fencing_token": claim.fencing_token,
            },
        ).to_wire()

    def _heartbeat(
        self,
        request: RemoteRequest,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
    ) -> dict[str, Any]:
        record = scheduler.renew_claim(
            claim,
            lease_seconds=float(request.body["lease_seconds"]),
        )
        return make_response(
            request,
            ok=True,
            body={
                "renewed": True,
                "run_id": claim.run_id,
                "node_id": claim.node_id,
                "attempt_id": claim.attempt_id,
                "fencing_token": claim.fencing_token,
                "lease_expires_at": record.lease_expires_at,
            },
        ).to_wire()

    def _cancellation_status(
        self,
        request: RemoteRequest,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
    ) -> dict[str, Any]:
        run = scheduler.store.get_run(claim.run_id)
        node = scheduler.store.get_node(claim.run_id, claim.node_id)
        attempt = scheduler.store.get_attempt(claim.attempt_id)
        if run is None or node is None or attempt is None:
            raise RemoteControlError("claim_conflict")
        requested = (
            run.status in {RunStatus.CANCELLING, RunStatus.CANCELLED}
            or node.status is NodeStatus.CANCELLED
            or attempt.status is AttemptStatus.CANCELLED
        )
        return make_response(
            request,
            ok=True,
            body={
                "cancel_requested": requested,
                "run_id": claim.run_id,
                "node_id": claim.node_id,
                "attempt_id": claim.attempt_id,
                "fencing_token": claim.fencing_token,
            },
        ).to_wire()

    def _complete(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        request: RemoteRequest,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        authorization: ExecutionAuthorization,
    ) -> dict[str, Any]:
        outcome = AttemptStatus(str(request.body["outcome"]))
        proof = RemoteRuntimeProof.from_wire(request.body["runtime_proof"])
        candidate = RemoteCompletionCandidate(
            outcome=outcome,
            output_handles=output_handles_from_completion(request.body),
            runtime_proof=proof,
            error_class=request.body["error_class"],
            error_code=request.body["error_code"],
        )
        adapter = self._require_security_admitter()
        try:
            adapter.complete(
                identity,
                registration,
                scheduler,
                claim,
                authorization,
                candidate,
                proof,
            )
        except RemoteControlError:
            raise
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise RemoteControlError("authorization_conflict") from exc
        committed = scheduler.store.get_attempt(claim.attempt_id)
        if committed is None or committed.status is not outcome:
            raise RemoteControlError("authorization_conflict")
        self._release_fleet_terminal(
            registration,
            ClaimBinding.from_wire(request.body["claim"]),
        )
        return make_response(
            request,
            ok=True,
            body={
                "accepted": True,
                "run_id": claim.run_id,
                "node_id": claim.node_id,
                "attempt_id": claim.attempt_id,
                "fencing_token": claim.fencing_token,
                "outcome": outcome.value,
            },
        ).to_wire()

    def _ack_cancel(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        request: RemoteRequest,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
        authorization: ExecutionAuthorization,
    ) -> dict[str, Any]:
        run = scheduler.store.get_run(claim.run_id)
        if run is None or run.status is not RunStatus.CANCELLING:
            raise RemoteControlError("claim_conflict")
        adapter = self._require_security_admitter()
        proof = RemoteRuntimeProof.from_wire(request.body["runtime_proof"])
        try:
            adapter.cancel(
                identity,
                registration,
                scheduler,
                claim,
                authorization,
                proof,
            )
        except RemoteControlError:
            raise
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise RemoteControlError("authorization_conflict") from exc
        committed = scheduler.store.get_attempt(claim.attempt_id)
        if committed is None or committed.status is not AttemptStatus.CANCELLED:
            raise RemoteControlError("authorization_conflict")
        self._release_fleet_terminal(
            registration,
            ClaimBinding.from_wire(request.body["claim"]),
        )
        return make_response(
            request,
            ok=True,
            body={
                "accepted": True,
                "run_id": claim.run_id,
                "node_id": claim.node_id,
                "attempt_id": claim.attempt_id,
                "fencing_token": claim.fencing_token,
                "outcome": "cancelled",
            },
        ).to_wire()

    def _release_fleet_terminal(
        self,
        registration: WorkerRegistration,
        claim: ClaimBinding,
    ) -> None:
        """Release non-authoritative capacity after durable terminal proof."""

        poller = self._fleet_poller
        if poller is None:
            return
        identity = AuthenticatedWorker(
            worker_id=registration.worker_id,
            tenant_id=registration.tenant_id,
            identity_digest=registration.identity_digest,
        )
        try:
            poller.release_terminal(identity, registration, claim)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            # Durable terminal state is execution truth. A projection release
            # failure must be repaired by fleet reconciliation, never by
            # rolling back or falsifying the committed terminal response.
            return

    def _assignment(
        self,
        claim: ActivityClaim,
        authorization: ExecutionAuthorization,
        registration: WorkerRegistration,
    ) -> WorkAssignment:
        config = dict(claim.config)
        activity_name = config.get(claim.activity_kind)
        if not isinstance(activity_name, str) or not activity_name.strip():
            activity_name = claim.activity_kind
        return WorkAssignment(
            claim=ClaimBinding(
                run_id=claim.run_id,
                node_id=claim.node_id,
                attempt_id=claim.attempt_id,
                activity_request_digest=claim.request_hash,
                action_digest=authorization.action_digest,
                authorization_digest=authorization.authorization_digest,
                profile_digest=authorization.profile_digest,
                request_digest=authorization.request_digest,
                session_binding_digest=authorization.session_binding_digest,
                grant_binding_digest=authorization.grant_binding_digest,
                execution_plan_digest=authorization.execution_plan_digest,
                runtime_attestation_digest=(
                    authorization.runtime_attestation_digest
                ),
                claim_token=claim.claim_token,
                fencing_token=claim.fencing_token,
            ),
            worker_id=registration.worker_id,
            attempt_number=claim.attempt_number,
            lease_expires_at=claim.lease_expires_at,
            activity_kind=claim.activity_kind,
            effect_class=claim.effect_class,
            resource_keys=claim.resource_keys,
            activity_descriptor=RemoteActivityDescriptor(
                activity_name=activity_name,
                config_digest=canonical_digest(config),
            ),
            execution_plan=authorization.execution_plan,
            input_grants=authorization.input_grants,
            output_grants=authorization.output_grants,
        )

    def _require_security_admitter(self) -> RemoteAssignmentAdmitter:
        adapter = self._assignment_admitter
        if (
            adapter is None
            or getattr(adapter, "production_security_ready", None) is not True
            or not self._journal.durable
        ):
            raise RemoteControlError("security_not_ready")
        return adapter

    def _authorize_claim(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        scheduler: DurableScheduler,
        claim: ActivityClaim,
    ) -> ExecutionAuthorization:
        adapter = self._require_security_admitter()
        try:
            authorization = adapter.admit(
                identity,
                registration,
                scheduler,
                claim,
            )
        except RemoteControlError:
            raise
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise RemoteControlError("authorization_conflict") from exc
        return self._validate_authorization(
            identity,
            registration,
            claim,
            authorization,
        )

    def _validate_authorization(
        self,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration,
        claim: ActivityClaim,
        authorization: ExecutionAuthorization,
    ) -> ExecutionAuthorization:
        if not isinstance(authorization, ExecutionAuthorization):
            raise RemoteControlError("authorization_conflict")
        if (
            authorization.session_binding_digest
            != registration.session_binding_digest
            or not set(authorization.execution_plan.capabilities).issubset(
                registration.capabilities
            )
        ):
            raise RemoteControlError("authorization_conflict")
        for grant in (*authorization.input_grants, *authorization.output_grants):
            if (
                grant.tenant_id != identity.tenant_id
                or grant.worker_id != identity.worker_id
                or grant.run_id != claim.run_id
                or grant.attempt_id != claim.attempt_id
                or grant.action_digest != authorization.action_digest
                or grant.authorization_digest
                != authorization.authorization_digest
            ):
                raise RemoteControlError("authorization_conflict")
        for grant in authorization.output_grants:
            if grant.node_id != claim.node_id:
                raise RemoteControlError("authorization_conflict")
        return authorization

    def _validate_handle(
        self,
        handle: ActivityClaim,
        binding: ClaimBinding,
        registration: WorkerRegistration,
    ) -> None:
        if (
            handle.worker_id != registration.session_owner_id
            or handle.activity_kind not in registration.activity_kinds
            or not set(handle.resource_keys).issubset(registration.resource_keys)
            or handle.run_id != binding.run_id
            or handle.node_id != binding.node_id
            or handle.attempt_id != binding.attempt_id
            or handle.request_hash != binding.activity_request_digest
            or handle.claim_token != binding.claim_token
            or handle.fencing_token != binding.fencing_token
        ):
            raise RemoteControlError("claim_conflict")

    def _require_registration(
        self,
        identity: AuthenticatedWorker,
        request: RemoteRequest,
    ) -> WorkerRegistration:
        with self._lock:
            now = self._now()
            self._expire_registrations_locked(now)
            item = self._registrations.get(identity.worker_id)
            if item is not None:
                registration, _last_seen = item
                self._registrations[identity.worker_id] = (registration, now)
                self._registrations.move_to_end(identity.worker_id)
            else:
                registration = None
        if registration is None:
            raise RemoteControlError("not_registered")
        if (
            registration.instance_id != request.instance_id
            or registration.tenant_id != identity.tenant_id
            or registration.identity_digest != identity.identity_digest
        ):
            raise RemoteControlError("worker_identity_mismatch")
        try:
            self._journal.assert_current(
                worker_id=registration.worker_id,
                tenant_id=registration.tenant_id,
                identity_digest=registration.identity_digest,
                instance_id=registration.instance_id,
                epoch=registration.session_epoch,
            )
        except (
            RemoteJournalIdentityError,
            RemoteJournalSessionSuperseded,
        ) as exc:
            raise RemoteControlError("worker_identity_mismatch") from exc
        except RemoteJournalError as exc:
            raise RemoteControlError("control_unavailable") from exc
        return registration

    def _expire_registrations_locked(self, now: float) -> None:
        expired = [
            worker_id
            for worker_id, (_registration, last_seen) in self._registrations.items()
            if now - last_seen >= self._registration_idle_seconds
        ]
        for worker_id in expired:
            self._registrations.pop(worker_id, None)

    def _now(self) -> float:
        try:
            value = float(self._clock())
        except (TypeError, ValueError, OverflowError) as exc:
            raise RemoteControlError("control_unavailable") from exc
        if value < 0 or value == float("inf") or value != value:
            raise RemoteControlError("control_unavailable")
        return value

    def _scheduler(self, run_id: str) -> DurableScheduler:
        try:
            scheduler = self._scheduler_resolver(run_id)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise RemoteControlError("run_not_found") from exc
        if not isinstance(scheduler, DurableScheduler):
            raise RemoteControlError("run_not_found")
        run = scheduler.store.get_run(run_id)
        if run is None:
            raise RemoteControlError("run_not_found")
        return scheduler

    def _enter_request(
        self,
        cache_key: tuple[str, str, str],
        request: RemoteRequest,
        *,
        identity: AuthenticatedWorker,
        registration: WorkerRegistration | None,
    ) -> tuple[bool, dict[str, Any] | None, bool]:
        journal_owner = False
        with self._lock:
            cached = self._response_cache.get(cache_key)
            if cached is not None:
                cached_digest, response = cached
                if cached_digest != request.request_digest:
                    raise RemoteControlError("request_id_conflict")
                if request.operation is not RemoteOperation.REGISTER:
                    self._response_cache.move_to_end(cache_key)
                    return False, _detach(response), False
                # Registration must always re-check the durable session head:
                # an old cached A response cannot survive A -> B supersession,
                # and an idle-cache restart must republish the registration.
                self._response_cache.pop(cache_key, None)
            inflight = self._inflight.get(cache_key)
            if inflight is not None:
                if inflight.request_digest != request.request_digest:
                    raise RemoteControlError("request_id_conflict")
                completed = inflight.completed
            else:
                if len(self._inflight) >= self._max_request_cache:
                    raise RemoteControlError("control_unavailable")
                inflight = _Inflight(
                    request_digest=request.request_digest,
                    completed=threading.Event(),
                )
                self._inflight[cache_key] = inflight
                journal_owner = True
                completed = inflight.completed
        if not journal_owner:
            if not completed.wait(MAX_INFLIGHT_WAIT_SECONDS):
                return False, None, False
            with self._lock:
                cached = self._response_cache.get(cache_key)
                if cached is None or cached[0] != request.request_digest:
                    return False, None, False
                return False, _detach(cached[1]), False

        request_is_new = True
        if registration is not None:
            try:
                request_is_new = self._journal.record_request(
                    worker_id=registration.worker_id,
                    tenant_id=registration.tenant_id,
                    identity_digest=registration.identity_digest,
                    instance_id=registration.instance_id,
                    epoch=registration.session_epoch,
                    request_id=request.request_id,
                    request_digest=request.request_digest,
                    operation=request.operation.value,
                    now=self._now(),
                )
            except RemoteJournalRequestConflict as exc:
                self._abort_request(cache_key, inflight)
                raise RemoteControlError("request_id_conflict") from exc
            except (
                RemoteJournalIdentityError,
                RemoteJournalSessionSuperseded,
            ) as exc:
                self._abort_request(cache_key, inflight)
                raise RemoteControlError("worker_identity_mismatch") from exc
            except (
                RemoteJournalCapacityError,
                RemoteJournalError,
            ) as exc:
                self._abort_request(cache_key, inflight)
                raise RemoteControlError("control_unavailable") from exc
        with self._lock:
            current = self._inflight.get(cache_key)
            if current is not inflight:
                return False, None, False
            if not request_is_new:
                self._inflight.pop(cache_key, None)
                inflight.completed.set()
                return False, None, True
            return True, None, False

    def _abort_request(
        self,
        cache_key: tuple[str, str, str],
        inflight: _Inflight,
    ) -> None:
        with self._lock:
            if self._inflight.get(cache_key) is inflight:
                self._inflight.pop(cache_key, None)
            inflight.completed.set()

    def _finish_request(
        self,
        cache_key: tuple[str, str, str],
        request_digest: str,
        response: dict[str, Any],
    ) -> None:
        with self._lock:
            body = response.get("body")
            is_request_conflict = (
                isinstance(body, Mapping)
                and body.get("error_code") == "request_id_conflict"
            )
            if not is_request_conflict:
                self._response_cache[cache_key] = (
                    request_digest,
                    _detach(response),
                )
                self._response_cache.move_to_end(cache_key)
                while len(self._response_cache) > self._max_request_cache:
                    self._response_cache.popitem(last=False)
            inflight = self._inflight.pop(cache_key, None)
            if inflight is not None:
                inflight.completed.set()

    def _registration_is_current(
        self,
        registration: WorkerRegistration,
    ) -> bool:
        with self._lock:
            current = self._registrations.get(registration.worker_id)
            return current is not None and current[0] == registration

    @contextmanager
    def _worker_session_guard(
        self,
        worker_id: str,
    ) -> Iterable[None]:
        """Serialize one Worker's session replacement and secure claim.

        Entries are reference-counted before waiting and removed after the last
        holder/waiter exits.  At most ``max_registrations`` distinct Worker
        guards may be live, preventing an unauthenticated ID spray from
        creating an unbounded lock registry.
        """

        with self._lock:
            guard = self._worker_session_guards.get(worker_id)
            if guard is None:
                if (
                    len(self._worker_session_guards)
                    >= self._max_registrations
                ):
                    raise RemoteControlError("control_unavailable")
                guard = _WorkerSessionGuard(lock=threading.Lock())
                self._worker_session_guards[worker_id] = guard
            guard.users += 1
        guard.lock.acquire()
        try:
            yield
        finally:
            guard.lock.release()
            with self._lock:
                guard.users -= 1
                if (
                    guard.users == 0
                    and self._worker_session_guards.get(worker_id) is guard
                ):
                    self._worker_session_guards.pop(worker_id, None)

    @staticmethod
    def _error_response(
        request: RemoteRequest,
        error_code: str,
    ) -> dict[str, Any]:
        return make_response(
            request,
            ok=False,
            body={"error_code": error_code},
        ).to_wire()


def _registration_digest(registration: WorkerRegistration) -> str:
    payload = {
        "worker_id": registration.worker_id,
        "tenant_id": registration.tenant_id,
        "identity_digest": registration.identity_digest,
        "instance_id": registration.instance_id,
        "runtime_version": registration.runtime_version,
        "capabilities": list(registration.capabilities),
        "resource_keys": list(registration.resource_keys),
        "activity_kinds": list(registration.activity_kinds),
        "max_concurrency": registration.max_concurrency,
        "session_epoch": registration.session_epoch,
        "session_binding_digest": registration.session_binding_digest,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _session_binding_digest(
    identity: AuthenticatedWorker,
    instance_id: str,
    session_epoch: int,
) -> str:
    """Bind one durable claim owner to trusted identity and server epoch."""

    payload = {
        "schema": "remote_worker_session_v2",
        "tenant_id": identity.tenant_id,
        "worker_id": identity.worker_id,
        "identity_digest": identity.identity_digest,
        "instance_id": instance_id,
        "session_epoch": session_epoch,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _session_owner_id(
    identity: AuthenticatedWorker,
    instance_id: str,
    session_epoch: int,
) -> str:
    """Return the opaque Store owner; never reuse the logical worker id."""

    return (
        "remote-session:"
        f"{_session_binding_digest(identity, instance_id, session_epoch)}"
    )


def _detach(value: Mapping[str, Any]) -> dict[str, Any]:
    return json.loads(
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    )


__all__ = [
    "RemoteAssignmentAdmitter",
    "RemoteCompletionCandidate",
    "RemoteControlError",
    "RemoteControlPlane",
    "RunAuthorizer",
    "SchedulerResolver",
    "WorkerRegistration",
]
