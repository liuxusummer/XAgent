from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

from src.orchestration.provider_access import (
    ProviderAccessBroker,
    ProviderAccessDenied,
    ProviderAccessGrant,
    ProviderOperationRecovery,
    ProviderOperationState,
    ProviderRouteDescriptor,
)
from src.orchestration.remote_execution_journal import (
    RemoteExecutionJournal,
)
from src.orchestration.worker_security import WorkerAuthorization
from src.orchestration.artifacts import (
    ArtifactSensitivity,
    LocalArtifactStore,
)


_UPSTREAM_SECRET = "upstream-provider-secret-28c6a"
_REQUEST_DIGEST = "d" * 64
_REQUEST_ARTIFACT_DIGEST = "e" * 64


class Clock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class AuthorizationVerifier:
    def __init__(self, *, explode: bool = False) -> None:
        self.explode = explode

    def verify(
        self,
        authorization: WorkerAuthorization,
        *,
        now: float,
    ) -> bool:
        if self.explode:
            raise RuntimeError(
                f"verifier leaked {_UPSTREAM_SECRET}"
            )
        return (
            authorization.authorization_id == "trusted-authorization"
            and now < authorization.expires_at
        )


class ProviderInvoker:
    def __init__(
        self,
        *,
        response: bytes = b'{"status":"ok"}',
        explode: bool = False,
    ) -> None:
        self.api_key = _UPSTREAM_SECRET
        self.response = response
        self.explode = explode
        self.calls: list[tuple[str, bytes, str]] = []

    def invoke(
        self,
        route: ProviderRouteDescriptor,
        payload: bytes,
        *,
        request_id: str,
    ) -> bytes:
        self.calls.append((route.route_id, payload, request_id))
        if self.explode:
            raise RuntimeError(
                f"provider leaked {self.api_key}"
            )
        return self.response


class RecoverableGatewayInvoker:
    def __init__(
        self,
        *,
        invoke_outcomes: list[bytes | BaseException] | None = None,
        recovery_state: ProviderOperationState = (
            ProviderOperationState.NOT_STARTED
        ),
        recovery_content: bytes = b'{"status":"recovered"}',
        ready: object = True,
        recover_explode: bool = False,
        readiness_explode: bool = False,
        operation_id_override: str | None = None,
        route_id_override: str | None = None,
        request_digest_override: str | None = None,
    ) -> None:
        self.api_key = _UPSTREAM_SECRET
        self.invoke_outcomes = list(invoke_outcomes or [])
        self.recovery_state = recovery_state
        self.recovery_content = recovery_content
        self.ready = ready
        self.recover_explode = recover_explode
        self.readiness_explode = readiness_explode
        self.operation_id_override = operation_id_override
        self.route_id_override = route_id_override
        self.request_digest_override = request_digest_override
        self.recovery_override: object | None = None
        self.calls: list[tuple[str, bytes, str]] = []
        self.recovery_calls: list[tuple[str, str, str]] = []
        self._lock = threading.Lock()

    @property
    def operation_recovery_ready(self) -> object:
        if self.readiness_explode:
            raise RuntimeError(
                f"readiness leaked {self.api_key}"
            )
        return self.ready

    def invoke(
        self,
        route: ProviderRouteDescriptor,
        payload: bytes,
        *,
        request_id: str,
    ) -> bytes:
        with self._lock:
            self.calls.append(
                (route.route_id, payload, request_id)
            )
            outcome = (
                self.invoke_outcomes.pop(0)
                if self.invoke_outcomes
                else b'{"status":"retried"}'
            )
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def recover(
        self,
        route: ProviderRouteDescriptor,
        *,
        operation_id: str,
        request_payload_digest: str,
    ) -> ProviderOperationRecovery:
        with self._lock:
            self.recovery_calls.append(
                (
                    route.route_id,
                    operation_id,
                    request_payload_digest,
                )
            )
        if self.recover_explode:
            raise RuntimeError(
                f"recovery leaked {self.api_key}"
            )
        on_recover = getattr(self, "on_recover", None)
        if callable(on_recover):
            on_recover()
        if self.recovery_override is not None:
            return self.recovery_override  # type: ignore[return-value]
        content = (
            self.recovery_content
            if self.recovery_state
            is ProviderOperationState.COMPLETED
            else None
        )
        return ProviderOperationRecovery(
            operation_id=(
                self.operation_id_override or operation_id
            ),
            route_id=self.route_id_override or route.route_id,
            request_payload_digest=(
                self.request_digest_override
                or request_payload_digest
            ),
            state=self.recovery_state,
            evidence_digest=hashlib.sha256(
                (
                    f"{operation_id}:{request_payload_digest}:"
                    f"{self.recovery_state.value}"
                ).encode()
            ).hexdigest(),
            verifier_id="trusted-provider-verifier",
            response_digest=(
                None
                if content is None
                else hashlib.sha256(content).hexdigest()
            ),
            content=content,
        )


class CrashAfterProviderCompletionJournal(RemoteExecutionJournal):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.crash_after_completion = True

    def complete_provider_invocation(self, **kwargs):
        completed = super().complete_provider_invocation(**kwargs)
        if self.crash_after_completion:
            self.crash_after_completion = False
            raise SystemExit("simulated response loss")
        return completed


def _route(
    *,
    maximum_request_bytes: int = 1024,
    maximum_response_bytes: int = 1024,
) -> ProviderRouteDescriptor:
    return ProviderRouteDescriptor(
        route_id="primary-route",
        tenant_id="tenant-1",
        pool_id="pool-1",
        worker_rule_id="agent-rule",
        provider="openai-compatible",
        model="model-deployment",
        gateway_binding_digest="f" * 64,
        maximum_request_bytes=maximum_request_bytes,
        maximum_response_bytes=maximum_response_bytes,
    )


def _authorization(**changes) -> WorkerAuthorization:
    values = {
        "authorization_id": "trusted-authorization",
        "worker_id": "worker-1",
        "tenant_id": "tenant-1",
        "pool_id": "pool-1",
        "run_id": "run-1",
        "node_id": "agent-node",
        "attempt_id": "attempt-1",
        "action_digest": "a" * 64,
        "identity_binding_digest": "b" * 64,
        "transport_binding_digest": "c" * 64,
        "rule_id": "agent-rule",
        "maximum_artifact_sensitivity": (
            ArtifactSensitivity.SENSITIVE
        ),
        "issued_at": 90.0,
        "expires_at": 200.0,
    }
    values.update(changes)
    return WorkerAuthorization(**values)


class ProviderAccessBrokerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.invoker = ProviderInvoker()
        self.broker = ProviderAccessBroker(
            (_route(),),
            authorization_verifier=AuthorizationVerifier(),
            invoker=self.invoker,
            clock=self.clock,
        )
        self.authorization = _authorization()

    def issue(self, **changes) -> ProviderAccessGrant:
        values = {
            "route_id": "primary-route",
            "request_digest": _REQUEST_DIGEST,
            "request_artifact_digest": (
                _REQUEST_ARTIFACT_DIGEST
            ),
            "invocation_index": 1,
            "ttl_seconds": 30.0,
        }
        values.update(changes)
        return self.broker.issue(self.authorization, **values)

    def issue_from(
        self,
        broker: ProviderAccessBroker,
        *,
        invocation_index: int = 1,
    ) -> ProviderAccessGrant:
        return broker.issue(
            self.authorization,
            route_id="primary-route",
            request_digest=_REQUEST_DIGEST,
            request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
            invocation_index=invocation_index,
            ttl_seconds=30.0,
        )

    def test_grant_is_canonical_path_free_and_credential_free(
        self,
    ) -> None:
        grant = self.issue()
        wire = grant.to_wire_dict()
        restored = ProviderAccessGrant.from_wire_dict(wire)

        self.assertEqual(restored, grant)
        self.assertEqual(
            restored.binding_digest,
            grant.binding_digest,
        )
        self.assertNotIn(grant.token, repr(grant))
        serialized = json.dumps(wire, sort_keys=True)
        self.assertNotIn(_UPSTREAM_SECRET, serialized)
        self.assertNotIn("api_key", serialized)
        self.assertNotIn("base_url", serialized)
        self.assertNotIn("endpoint", serialized)
        self.assertFalse(self.broker.production_security_ready)
        self.assertFalse(self.broker.durable_recovery_ready)
        self.assertFalse(
            self.broker.durable_result_recovery_ready
        )
        self.assertFalse(
            self.broker.provider_operation_recovery_ready
        )
        self.assertEqual(
            grant.response_sensitivity,
            ArtifactSensitivity.SENSITIVE,
        )
        record = self.broker._recovery_journal.get_provider_grant(
            grant.grant_id
        )
        self.assertIsNotNone(record)
        self.assertNotIn(grant.token, repr(record))
        self.assertFalse(hasattr(record, "token"))

    def test_invoke_consumes_once_and_hides_response_from_repr(
        self,
    ) -> None:
        grant = self.issue()
        payload = b'{"messages":[{"content":"safe"}]}'

        result = self.broker.invoke(
            grant,
            self.authorization,
            payload,
        )

        self.assertEqual(result.content, b'{"status":"ok"}')
        self.assertNotIn('{"status":"ok"}', repr(result))
        self.assertEqual(
            self.invoker.calls,
            [("primary-route", payload, grant.grant_id)],
        )
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_result_unavailable",
        ):
            self.broker.invoke(
                grant,
                self.authorization,
                payload,
            )
        self.assertEqual(len(self.invoker.calls), 1)

    def test_wrong_authorization_and_tampering_do_not_consume(
        self,
    ) -> None:
        grant = self.issue()
        other = _authorization(tenant_id="tenant-2")
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "authorization_binding_mismatch",
        ):
            self.broker.invoke(grant, other, b"safe")

        tampered = replace(grant, request_digest="0" * 64)
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_grant_unavailable",
        ):
            self.broker.invoke(
                tampered,
                self.authorization,
                b"safe",
            )

        result = self.broker.invoke(
            grant,
            self.authorization,
            b"safe",
        )
        self.assertEqual(result.content, b'{"status":"ok"}')

    def test_concurrent_replay_invokes_gateway_exactly_once(self) -> None:
        grant = self.issue()

        def invoke_once(_index: int) -> str:
            try:
                self.broker.invoke(
                    grant,
                    self.authorization,
                    b"safe",
                )
            except ProviderAccessDenied as exc:
                return exc.reason_code
            return "ok"

        with ThreadPoolExecutor(max_workers=8) as executor:
            outcomes = list(executor.map(invoke_once, range(16)))

        self.assertEqual(outcomes.count("ok"), 1)
        self.assertTrue(
            all(
                outcome
                in {
                    "ok",
                    "provider_invocation_outcome_unknown",
                    "provider_result_unavailable",
                }
                for outcome in outcomes
            )
        )
        self.assertEqual(len(self.invoker.calls), 1)

    def test_logical_invocation_is_issued_only_once(self) -> None:
        def issue_once(_index: int) -> str:
            try:
                self.issue()
            except ProviderAccessDenied as exc:
                return exc.reason_code
            return "ok"

        with ThreadPoolExecutor(max_workers=8) as executor:
            outcomes = list(executor.map(issue_once, range(16)))

        self.assertEqual(outcomes.count("ok"), 1)
        self.assertEqual(
            outcomes.count("provider_invocation_already_issued"),
            15,
        )
        with self.assertRaises(ProviderAccessDenied) as raised:
            self.issue()
        self.assertEqual(
            raised.exception.reason_code,
            "provider_invocation_already_issued",
        )
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)
        second = self.issue(invocation_index=2)
        self.assertEqual(second.invocation_index, 2)

    def test_invocation_failure_is_sanitized_and_spends_grant(
        self,
    ) -> None:
        invoker = ProviderInvoker(explode=True)
        broker = ProviderAccessBroker(
            (_route(),),
            authorization_verifier=AuthorizationVerifier(),
            invoker=invoker,
            clock=self.clock,
        )
        grant = broker.issue(
            self.authorization,
            route_id="primary-route",
            request_digest=_REQUEST_DIGEST,
            request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
            invocation_index=1,
        )

        with self.assertRaises(ProviderAccessDenied) as raised:
            broker.invoke(grant, self.authorization, b"safe")
        self.assertEqual(
            raised.exception.reason_code,
            "provider_invocation_failed",
        )
        self.assertNotIn(_UPSTREAM_SECRET, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_invocation_outcome_unknown",
        ):
            broker.invoke(grant, self.authorization, b"safe")
        self.assertEqual(len(invoker.calls), 1)

    def test_verified_not_started_retries_one_unknown_call(
        self,
    ) -> None:
        invoker = RecoverableGatewayInvoker(
            invoke_outcomes=[
                RuntimeError(
                    f"provider leaked {_UPSTREAM_SECRET}"
                ),
                b'{"status":"retried"}',
            ],
        )
        broker = ProviderAccessBroker(
            (_route(),),
            authorization_verifier=AuthorizationVerifier(),
            invoker=invoker,
            clock=self.clock,
        )
        grant = self.issue_from(broker)
        self.assertTrue(
            broker.provider_operation_recovery_ready
        )
        self.assertFalse(broker.production_security_ready)
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_invocation_failed",
        ):
            broker.invoke(grant, self.authorization, b"safe")

        result = broker.invoke(
            grant,
            self.authorization,
            b"safe",
        )

        self.assertEqual(result.content, b'{"status":"retried"}')
        self.assertEqual(len(invoker.calls), 2)
        self.assertEqual(len(invoker.recovery_calls), 1)
        evidence = (
            broker._recovery_journal
            .list_provider_recovery_evidence(grant.grant_id)
        )
        self.assertEqual(len(evidence), 1)
        self.assertEqual(evidence[0].decision, "not_started")

    def test_not_started_evidence_cannot_authorize_two_retries(
        self,
    ) -> None:
        invoker = RecoverableGatewayInvoker(
            invoke_outcomes=[
                RuntimeError("first call outcome unknown"),
                RuntimeError("retry outcome unknown"),
            ],
        )
        broker = ProviderAccessBroker(
            (_route(),),
            authorization_verifier=AuthorizationVerifier(),
            invoker=invoker,
            clock=self.clock,
        )
        grant = self.issue_from(broker)
        for _index in range(2):
            with self.assertRaisesRegex(
                ProviderAccessDenied,
                "provider_invocation_failed",
            ):
                broker.invoke(
                    grant,
                    self.authorization,
                    b"safe",
                )
        with self.assertRaises(ProviderAccessDenied) as raised:
            broker.invoke(
                grant,
                self.authorization,
                b"safe",
            )
        self.assertEqual(
            raised.exception.reason_code,
            "provider_operation_recovery_replayed",
        )
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)
        self.assertEqual(len(invoker.calls), 2)
        evidence = (
            broker._recovery_journal
            .list_provider_recovery_evidence(grant.grant_id)
        )
        self.assertEqual(len(evidence), 1)

    def test_not_started_never_retries_raw_invoking_zombie(
        self,
    ) -> None:
        invoker = RecoverableGatewayInvoker()
        journal = RemoteExecutionJournal()
        broker = ProviderAccessBroker(
            (_route(),),
            authorization_verifier=AuthorizationVerifier(),
            invoker=invoker,
            clock=self.clock,
            recovery_journal=journal,
        )
        grant = self.issue_from(broker)
        payload = b"zombie-race"
        journal.claim_provider_invocation(
            grant_id=grant.grant_id,
            token_digest=hashlib.sha256(
                grant.token.encode()
            ).hexdigest(),
            binding_digest=grant.binding_digest,
            route_id=grant.route.route_id,
            expires_at=grant.expires_at,
            request_payload_digest=hashlib.sha256(
                payload
            ).hexdigest(),
            now=self.clock.now,
        )

        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_invocation_outcome_unknown",
        ):
            broker.invoke(grant, self.authorization, payload)

        self.assertEqual(invoker.calls, [])
        self.assertEqual(len(invoker.recovery_calls), 1)
        self.assertEqual(
            journal.list_provider_recovery_evidence(
                grant.grant_id
            ),
            (),
        )

    def test_not_started_cannot_retry_after_grant_expires(
        self,
    ) -> None:
        invoker = RecoverableGatewayInvoker(
            invoke_outcomes=[
                RuntimeError("first call outcome unknown")
            ]
        )
        broker = ProviderAccessBroker(
            (_route(),),
            authorization_verifier=AuthorizationVerifier(),
            invoker=invoker,
            clock=self.clock,
        )
        grant = broker.issue(
            self.authorization,
            route_id="primary-route",
            request_digest=_REQUEST_DIGEST,
            request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
            invocation_index=1,
            ttl_seconds=1.0,
        )
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_invocation_failed",
        ):
            broker.invoke(grant, self.authorization, b"safe")
        invoker.on_recover = lambda: setattr(
            self.clock,
            "now",
            grant.expires_at,
        )

        with self.assertRaises(ProviderAccessDenied) as raised:
            broker.invoke(grant, self.authorization, b"safe")

        self.assertEqual(
            raised.exception.reason_code,
            "provider_grant_unavailable",
        )
        self.assertEqual(len(invoker.calls), 1)
        self.assertEqual(
            broker._recovery_journal
            .list_provider_recovery_evidence(grant.grant_id),
            (),
        )

    def test_completed_gateway_evidence_recovers_and_replays(
        self,
    ) -> None:
        payload = b"completed-recovery-request"
        response = b'{"status":"already-completed"}'
        invoker = RecoverableGatewayInvoker(
            recovery_state=ProviderOperationState.COMPLETED,
            recovery_content=response,
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "provider-grants.sqlite3"
            store = LocalArtifactStore(
                Path(temporary) / "provider-results"
            )
            first_journal = RemoteExecutionJournal(path)
            first = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=invoker,
                clock=self.clock,
                recovery_journal=first_journal,
                result_store=store,
            )
            grant = self.issue_from(first)
            first_journal.claim_provider_invocation(
                grant_id=grant.grant_id,
                token_digest=hashlib.sha256(
                    grant.token.encode()
                ).hexdigest(),
                binding_digest=grant.binding_digest,
                route_id=grant.route.route_id,
                expires_at=grant.expires_at,
                request_payload_digest=hashlib.sha256(
                    payload
                ).hexdigest(),
                now=self.clock.now,
            )
            first_journal.close()

            second_journal = RemoteExecutionJournal(path)
            second = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=invoker,
                clock=self.clock,
                recovery_journal=second_journal,
                result_store=store,
            )
            recovered = second.invoke(
                grant,
                self.authorization,
                payload,
            )
            self.assertEqual(recovered.content, response)
            self.assertEqual(invoker.calls, [])
            self.assertEqual(len(invoker.recovery_calls), 1)
            evidence = (
                second_journal.list_provider_recovery_evidence(
                    grant.grant_id
                )
            )
            self.assertEqual(len(evidence), 1)
            self.assertEqual(evidence[0].decision, "completed")
            second_journal.close()

            third_journal = RemoteExecutionJournal(path)
            third_invoker = ProviderInvoker()
            third = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=third_invoker,
                clock=self.clock,
                recovery_journal=third_journal,
                result_store=store,
            )
            replayed = third.invoke(
                grant,
                self.authorization,
                payload,
            )
            self.assertEqual(replayed.content, response)
            self.assertEqual(third_invoker.calls, [])
            third_journal.close()

            for suffix in ("", "-wal", "-shm"):
                candidate = Path(f"{path}{suffix}")
                if candidate.exists():
                    durable_bytes = candidate.read_bytes()
                    self.assertNotIn(payload, durable_bytes)
                    self.assertNotIn(response, durable_bytes)
                    self.assertNotIn(
                        grant.token.encode(),
                        durable_bytes,
                    )
                    self.assertNotIn(
                        _UPSTREAM_SECRET.encode(),
                        durable_bytes,
                    )

    def test_concurrent_not_started_recovery_claims_one_retry(
        self,
    ) -> None:
        invoker = RecoverableGatewayInvoker(
            invoke_outcomes=[
                RuntimeError("first call outcome unknown"),
                b'{"status":"retried-once"}',
            ],
        )
        broker = ProviderAccessBroker(
            (_route(),),
            authorization_verifier=AuthorizationVerifier(),
            invoker=invoker,
            clock=self.clock,
        )
        grant = self.issue_from(broker)
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_invocation_failed",
        ):
            broker.invoke(grant, self.authorization, b"safe")

        def recover_once(_index: int) -> str:
            try:
                broker.invoke(
                    grant,
                    self.authorization,
                    b"safe",
                )
            except ProviderAccessDenied as exc:
                return exc.reason_code
            return "ok"

        with ThreadPoolExecutor(max_workers=8) as executor:
            outcomes = list(
                executor.map(recover_once, range(16))
            )

        self.assertEqual(outcomes.count("ok"), 1)
        self.assertTrue(
            all(
                outcome
                in {
                    "ok",
                    "provider_invocation_outcome_unknown",
                    "provider_result_unavailable",
                }
                for outcome in outcomes
            )
        )
        self.assertEqual(len(invoker.calls), 2)
        evidence = (
            broker._recovery_journal
            .list_provider_recovery_evidence(grant.grant_id)
        )
        self.assertEqual(len(evidence), 1)

    def test_recovery_binding_mismatch_fails_closed(
        self,
    ) -> None:
        configurations = (
            {"operation_id_override": "wrong-operation"},
            {"route_id_override": "wrong-route"},
            {"request_digest_override": "0" * 64},
        )
        for index, configuration in enumerate(
            configurations,
            start=1,
        ):
            with self.subTest(configuration=configuration):
                invoker = RecoverableGatewayInvoker(
                    invoke_outcomes=[
                        RuntimeError("unknown outcome")
                    ],
                    **configuration,
                )
                broker = ProviderAccessBroker(
                    (_route(),),
                    authorization_verifier=(
                        AuthorizationVerifier()
                    ),
                    invoker=invoker,
                    clock=self.clock,
                )
                grant = self.issue_from(
                    broker,
                    invocation_index=index,
                )
                with self.assertRaises(ProviderAccessDenied):
                    broker.invoke(
                        grant,
                        self.authorization,
                        b"safe",
                    )
                with self.assertRaises(ProviderAccessDenied) as raised:
                    broker.invoke(
                        grant,
                        self.authorization,
                        b"safe",
                    )
                self.assertEqual(
                    raised.exception.reason_code,
                    "invalid_provider_operation_recovery",
                )
                self.assertEqual(len(invoker.calls), 1)
                self.assertEqual(
                    broker._recovery_journal
                    .list_provider_recovery_evidence(
                        grant.grant_id
                    ),
                    (),
                )

    def test_recovery_errors_are_sanitized_without_exception_chain(
        self,
    ) -> None:
        for readiness_explode, recover_explode in (
            (True, False),
            (False, True),
        ):
            with self.subTest(
                readiness_explode=readiness_explode,
                recover_explode=recover_explode,
            ):
                invoker = RecoverableGatewayInvoker(
                    invoke_outcomes=[
                        RuntimeError("unknown outcome")
                    ],
                    readiness_explode=readiness_explode,
                    recover_explode=recover_explode,
                )
                broker = ProviderAccessBroker(
                    (_route(),),
                    authorization_verifier=(
                        AuthorizationVerifier()
                    ),
                    invoker=invoker,
                    clock=self.clock,
                )
                grant = self.issue_from(broker)
                with self.assertRaises(ProviderAccessDenied):
                    broker.invoke(
                        grant,
                        self.authorization,
                        b"safe",
                    )
                with self.assertRaises(ProviderAccessDenied) as raised:
                    broker.invoke(
                        grant,
                        self.authorization,
                        b"safe",
                    )
                self.assertEqual(
                    raised.exception.reason_code,
                    "provider_operation_recovery_failed",
                )
                self.assertNotIn(
                    _UPSTREAM_SECRET,
                    str(raised.exception),
                )
                self.assertIsNone(raised.exception.__cause__)
                self.assertIsNone(raised.exception.__context__)
                self.assertEqual(len(invoker.calls), 1)

    def test_in_progress_unknown_and_unready_never_retry(
        self,
    ) -> None:
        cases = (
            (ProviderOperationState.IN_PROGRESS, True),
            (ProviderOperationState.UNKNOWN, True),
            (ProviderOperationState.NOT_STARTED, False),
            (ProviderOperationState.NOT_STARTED, 1),
        )
        for index, (state, ready) in enumerate(cases, start=1):
            with self.subTest(state=state, ready=ready):
                invoker = RecoverableGatewayInvoker(
                    invoke_outcomes=[
                        RuntimeError("unknown outcome")
                    ],
                    recovery_state=state,
                    ready=ready,
                )
                broker = ProviderAccessBroker(
                    (_route(),),
                    authorization_verifier=(
                        AuthorizationVerifier()
                    ),
                    invoker=invoker,
                    clock=self.clock,
                )
                grant = self.issue_from(
                    broker,
                    invocation_index=index,
                )
                with self.assertRaises(ProviderAccessDenied):
                    broker.invoke(
                        grant,
                        self.authorization,
                        b"safe",
                    )
                with self.assertRaisesRegex(
                    ProviderAccessDenied,
                    "provider_invocation_outcome_unknown",
                ):
                    broker.invoke(
                        grant,
                        self.authorization,
                        b"safe",
                    )
                self.assertEqual(len(invoker.calls), 1)

    def test_operation_recovery_record_is_strict_and_payload_safe(
        self,
    ) -> None:
        content = b"recovered-secret-response"
        recovery = ProviderOperationRecovery(
            operation_id="provider-grant-1",
            route_id="primary-route",
            request_payload_digest="a" * 64,
            state=ProviderOperationState.COMPLETED,
            evidence_digest="b" * 64,
            verifier_id="trusted-provider-verifier",
            response_digest=hashlib.sha256(content).hexdigest(),
            content=content,
        )
        self.assertNotIn(content.decode(), repr(recovery))
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "invalid_provider_operation_recovery",
        ):
            ProviderOperationRecovery(
                operation_id="provider-grant-1",
                route_id="primary-route",
                request_payload_digest="a" * 64,
                state=ProviderOperationState.COMPLETED,
                evidence_digest="b" * 64,
                verifier_id="trusted-provider-verifier",
                response_digest="c" * 64,
                content=content,
            )
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "invalid_provider_operation_recovery",
        ):
            ProviderOperationRecovery(
                operation_id="provider-grant-1",
                route_id="primary-route",
                request_payload_digest="a" * 64,
                state=ProviderOperationState.NOT_STARTED,
                evidence_digest="b" * 64,
                verifier_id="trusted-provider-verifier",
                response_digest=hashlib.sha256(
                    content
                ).hexdigest(),
                content=content,
            )

    def test_recovery_record_subclass_cannot_override_semantics(
        self,
    ) -> None:
        class ForgedRecovery(ProviderOperationRecovery):
            pass

        invoker = RecoverableGatewayInvoker(
            invoke_outcomes=[RuntimeError("unknown outcome")]
        )
        invoker.recovery_override = ForgedRecovery(
            operation_id="placeholder-operation",
            route_id="primary-route",
            request_payload_digest=hashlib.sha256(
                b"safe"
            ).hexdigest(),
            state=ProviderOperationState.NOT_STARTED,
            evidence_digest="b" * 64,
            verifier_id="trusted-provider-verifier",
        )
        broker = ProviderAccessBroker(
            (_route(),),
            authorization_verifier=AuthorizationVerifier(),
            invoker=invoker,
            clock=self.clock,
        )
        grant = self.issue_from(broker)
        object.__setattr__(
            invoker.recovery_override,
            "operation_id",
            grant.grant_id,
        )
        with self.assertRaises(ProviderAccessDenied):
            broker.invoke(grant, self.authorization, b"safe")
        with self.assertRaises(ProviderAccessDenied) as raised:
            broker.invoke(grant, self.authorization, b"safe")
        self.assertEqual(
            raised.exception.reason_code,
            "invalid_provider_operation_recovery",
        )
        self.assertEqual(len(invoker.calls), 1)

    def test_request_response_and_expiry_limits_fail_closed(
        self,
    ) -> None:
        broker = ProviderAccessBroker(
            (
                _route(
                    maximum_request_bytes=4,
                    maximum_response_bytes=4,
                ),
            ),
            authorization_verifier=AuthorizationVerifier(),
            invoker=ProviderInvoker(response=b"oversized"),
            clock=self.clock,
        )
        grant = broker.issue(
            self.authorization,
            route_id="primary-route",
            request_digest=_REQUEST_DIGEST,
            request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
            invocation_index=1,
            ttl_seconds=10.0,
        )
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_request_exceeds_policy",
        ):
            broker.invoke(grant, self.authorization, b"12345")
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_response_exceeds_policy",
        ):
            broker.invoke(grant, self.authorization, b"1234")
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_invocation_outcome_unknown",
        ):
            broker.invoke(grant, self.authorization, b"1234")

        expiring = self.issue(ttl_seconds=5.0)
        self.clock.now = 106.0
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_grant_expired",
        ):
            self.broker.invoke(
                expiring,
                self.authorization,
                b"safe",
            )
        self.assertEqual(self.broker.purge_expired(), 1)

    def test_capacity_and_verifier_errors_are_bounded(self) -> None:
        broker = ProviderAccessBroker(
            (_route(),),
            authorization_verifier=AuthorizationVerifier(),
            invoker=self.invoker,
            clock=self.clock,
            maximum_active_grants=1,
        )
        broker.issue(
            self.authorization,
            route_id="primary-route",
            request_digest=_REQUEST_DIGEST,
            request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
            invocation_index=1,
        )
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "active_provider_grant_limit_reached",
        ):
            broker.issue(
                self.authorization,
                route_id="primary-route",
                request_digest=_REQUEST_DIGEST,
                request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
                invocation_index=2,
            )

        exploding = ProviderAccessBroker(
            (_route(),),
            authorization_verifier=AuthorizationVerifier(
                explode=True
            ),
            invoker=self.invoker,
            clock=self.clock,
        )
        with self.assertRaises(ProviderAccessDenied) as raised:
            exploding.issue(
                self.authorization,
                route_id="primary-route",
                request_digest=_REQUEST_DIGEST,
                request_artifact_digest=(
                    _REQUEST_ARTIFACT_DIGEST
                ),
                invocation_index=1,
            )
        self.assertEqual(
            raised.exception.reason_code,
            "worker_authorization_invalid",
        )
        self.assertNotIn(_UPSTREAM_SECRET, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)

        consumable_journal = RemoteExecutionJournal()
        consumable = ProviderAccessBroker(
            (_route(),),
            authorization_verifier=AuthorizationVerifier(),
            invoker=self.invoker,
            clock=self.clock,
            recovery_journal=consumable_journal,
        )
        grant = consumable.issue(
            self.authorization,
            route_id="primary-route",
            request_digest=_REQUEST_DIGEST,
            request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
            invocation_index=1,
        )
        consumable_journal.close()
        with self.assertRaises(ProviderAccessDenied) as consume_raised:
            consumable.invoke(
                grant,
                self.authorization,
                b"safe",
            )
        self.assertEqual(
            consume_raised.exception.reason_code,
            "provider_grant_registry_unavailable",
        )
        self.assertIsNone(consume_raised.exception.__cause__)
        self.assertIsNone(consume_raised.exception.__context__)

    def test_route_is_bound_to_tenant_pool_and_worker_rule(self) -> None:
        other_tenant = _authorization(tenant_id="tenant-2")
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_route_unauthorized",
        ):
            self.broker.issue(
                other_tenant,
                route_id="primary-route",
                request_digest=_REQUEST_DIGEST,
                request_artifact_digest=(
                    _REQUEST_ARTIFACT_DIGEST
                ),
                invocation_index=1,
            )

        other_rule = _authorization(rule_id="other-rule")
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_route_unauthorized",
        ):
            self.broker.issue(
                other_rule,
                route_id="primary-route",
                request_digest=_REQUEST_DIGEST,
                request_artifact_digest=(
                    _REQUEST_ARTIFACT_DIGEST
                ),
                invocation_index=1,
            )

    def test_wire_schema_is_exact_and_bool_index_is_rejected(
        self,
    ) -> None:
        grant = self.issue()
        unknown = grant.to_wire_dict()
        unknown["unexpected"] = True
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "invalid_provider_grant",
        ):
            ProviderAccessGrant.from_wire_dict(unknown)

        bool_index = grant.to_wire_dict()
        bool_index["invocation_index"] = True
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "invalid_provider_invocation_index",
        ):
            ProviderAccessGrant.from_wire_dict(bool_index)

        bool_time = grant.to_wire_dict()
        bool_time["issued_at"] = True
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "invalid_provider_time",
        ):
            ProviderAccessGrant.from_wire_dict(bool_time)

    def test_durable_grant_survives_restart_and_replay_tombstone_does_too(
        self,
    ) -> None:
        payload = b'{"messages":[{"content":"restart-safe"}]}'
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "provider-grants.sqlite3"
            result_store = LocalArtifactStore(
                Path(temporary) / "provider-results"
            )
            first_journal = RemoteExecutionJournal(path)
            first = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                recovery_journal=first_journal,
                result_store=result_store,
            )
            grant = first.issue(
                self.authorization,
                route_id="primary-route",
                request_digest=_REQUEST_DIGEST,
                request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
                invocation_index=1,
            )
            first_journal.close()

            second_journal = RemoteExecutionJournal(path)
            second = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                recovery_journal=second_journal,
                result_store=result_store,
            )
            self.assertTrue(second.durable_recovery_ready)
            self.assertTrue(second.durable_result_recovery_ready)
            self.assertFalse(second.production_security_ready)
            result = second.invoke(
                grant,
                self.authorization,
                payload,
            )
            self.assertEqual(result.content, b'{"status":"ok"}')
            self.assertIsNotNone(result.artifact_ref)
            assert result.artifact_ref is not None
            self.assertEqual(
                result.artifact_ref.sensitivity,
                ArtifactSensitivity.SENSITIVE,
            )
            self.assertNotIn(
                result.artifact_ref.artifact_id,
                repr(result),
            )
            second_journal.close()

            third_journal = RemoteExecutionJournal(path)
            third = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                recovery_journal=third_journal,
                result_store=result_store,
            )
            replayed = third.invoke(
                grant,
                self.authorization,
                payload,
            )
            self.assertEqual(replayed.content, result.content)
            self.assertEqual(
                replayed.artifact_ref,
                result.artifact_ref,
            )
            with self.assertRaisesRegex(
                ProviderAccessDenied,
                "provider_grant_unavailable",
            ):
                third.invoke(
                    grant,
                    self.authorization,
                    b"different-payload",
                )
            self.assertEqual(len(self.invoker.calls), 1)
            third_journal.close()

            for suffix in ("", "-wal", "-shm"):
                candidate = Path(f"{path}{suffix}")
                if candidate.exists():
                    durable_bytes = candidate.read_bytes()
                    self.assertNotIn(
                        grant.token.encode(),
                        durable_bytes,
                    )
                    self.assertNotIn(payload, durable_bytes)
                    self.assertNotIn(
                        _UPSTREAM_SECRET.encode(),
                        durable_bytes,
                    )
                    self.assertNotIn(
                        result.content,
                        durable_bytes,
                    )

    def test_crash_after_claim_recovers_as_unknown_without_reinvoke(
        self,
    ) -> None:
        payload = b"provider-payload"
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "provider-grants.sqlite3"
            journal = RemoteExecutionJournal(path)
            broker = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                recovery_journal=journal,
            )
            grant = broker.issue(
                self.authorization,
                route_id="primary-route",
                request_digest=_REQUEST_DIGEST,
                request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
                invocation_index=1,
            )
            claim = journal.claim_provider_invocation(
                grant_id=grant.grant_id,
                token_digest=hashlib.sha256(
                    grant.token.encode()
                ).hexdigest(),
                binding_digest=grant.binding_digest,
                route_id=grant.route.route_id,
                expires_at=grant.expires_at,
                request_payload_digest=hashlib.sha256(
                    payload
                ).hexdigest(),
                now=self.clock.now,
            )
            self.assertTrue(claim.execute)
            journal.close()

            restarted_journal = RemoteExecutionJournal(path)
            restarted = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                recovery_journal=restarted_journal,
            )
            with self.assertRaisesRegex(
                ProviderAccessDenied,
                "provider_invocation_outcome_unknown",
            ):
                restarted.invoke(
                    grant,
                    self.authorization,
                    payload,
                )
            self.assertEqual(self.invoker.calls, [])
            restarted_journal.close()

    def test_result_store_failure_is_sanitized_and_becomes_unknown(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            def fail_store(_stage: str, _path: Path) -> None:
                raise RuntimeError(
                    f"artifact path leaked {_UPSTREAM_SECRET}"
                )

            store = LocalArtifactStore(
                Path(temporary) / "provider-results",
                fault_hook=fail_store,
            )
            broker = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                result_store=store,
            )
            grant = broker.issue(
                self.authorization,
                route_id="primary-route",
                request_digest=_REQUEST_DIGEST,
                request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
                invocation_index=1,
            )
            with self.assertRaises(ProviderAccessDenied) as raised:
                broker.invoke(
                    grant,
                    self.authorization,
                    b"safe",
                )
            self.assertEqual(
                raised.exception.reason_code,
                "provider_result_persistence_failed",
            )
            self.assertNotIn(
                _UPSTREAM_SECRET,
                str(raised.exception),
            )
            self.assertIsNone(raised.exception.__cause__)
            self.assertIsNone(raised.exception.__context__)
            with self.assertRaisesRegex(
                ProviderAccessDenied,
                "provider_invocation_outcome_unknown",
            ):
                broker.invoke(
                    grant,
                    self.authorization,
                    b"safe",
                )
            self.assertEqual(len(self.invoker.calls), 1)

    def test_corrupt_result_artifact_fails_closed_without_reinvoke(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalArtifactStore(
                Path(temporary) / "provider-results"
            )
            broker = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                result_store=store,
            )
            grant = broker.issue(
                self.authorization,
                route_id="primary-route",
                request_digest=_REQUEST_DIGEST,
                request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
                invocation_index=1,
            )
            result = broker.invoke(
                grant,
                self.authorization,
                b"safe",
            )
            self.assertIsNotNone(result.artifact_ref)
            assert result.artifact_ref is not None
            (store.root / result.artifact_ref.uri).write_bytes(
                b"corrupt"
            )
            with self.assertRaises(ProviderAccessDenied) as raised:
                broker.invoke(
                    grant,
                    self.authorization,
                    b"safe",
                )
            self.assertEqual(
                raised.exception.reason_code,
                "provider_result_integrity_failed",
            )
            self.assertNotIn(str(store.root), str(raised.exception))
            self.assertIsNone(raised.exception.__cause__)
            self.assertIsNone(raised.exception.__context__)
            self.assertEqual(len(self.invoker.calls), 1)

    def test_completion_commit_then_response_loss_replays_without_reinvoke(
        self,
    ) -> None:
        payload = b"response-loss-payload"
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "provider-grants.sqlite3"
            store = LocalArtifactStore(
                Path(temporary) / "provider-results"
            )
            crashing_journal = CrashAfterProviderCompletionJournal(
                path
            )
            first = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                recovery_journal=crashing_journal,
                result_store=store,
            )
            grant = first.issue(
                self.authorization,
                route_id="primary-route",
                request_digest=_REQUEST_DIGEST,
                request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
                invocation_index=1,
            )
            with self.assertRaises(SystemExit):
                first.invoke(
                    grant,
                    self.authorization,
                    payload,
                )
            crashing_journal.close()

            restarted_journal = RemoteExecutionJournal(path)
            restarted = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                recovery_journal=restarted_journal,
                result_store=store,
            )
            replayed = restarted.invoke(
                grant,
                self.authorization,
                payload,
            )
            self.assertEqual(replayed.content, b'{"status":"ok"}')
            self.assertEqual(len(self.invoker.calls), 1)
            restarted_journal.close()

    def test_result_classification_is_bound_and_secret_local_store_denied(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_result_sensitivity_exceeds_policy",
        ):
            self.broker.issue(
                _authorization(
                    maximum_artifact_sensitivity=(
                        ArtifactSensitivity.INTERNAL
                    )
                ),
                route_id="primary-route",
                request_digest=_REQUEST_DIGEST,
                request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
                invocation_index=1,
            )
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_result_sensitivity_mismatch",
        ):
            self.broker.issue(
                self.authorization,
                route_id="primary-route",
                request_digest=_REQUEST_DIGEST,
                request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
                invocation_index=1,
                response_sensitivity=ArtifactSensitivity.INTERNAL,
            )
        with tempfile.TemporaryDirectory() as temporary:
            local = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                result_store=LocalArtifactStore(
                    Path(temporary) / "provider-results"
                ),
            )
            with self.assertRaisesRegex(
                ProviderAccessDenied,
                "secret_provider_result_requires_encrypted_store",
            ):
                local.issue(
                    _authorization(
                        maximum_artifact_sensitivity=(
                            ArtifactSensitivity.SECRET
                        )
                    ),
                    route_id="primary-route",
                    request_digest=_REQUEST_DIGEST,
                    request_artifact_digest=(
                        _REQUEST_ARTIFACT_DIGEST
                    ),
                    invocation_index=1,
                    response_sensitivity=ArtifactSensitivity.SECRET,
                )

    def test_separate_registries_serialize_issue_and_consume(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "provider-grants.sqlite3"
            RemoteExecutionJournal(path).close()
            first_journal = RemoteExecutionJournal(path)
            second_journal = RemoteExecutionJournal(path)
            first = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                recovery_journal=first_journal,
            )
            second = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                recovery_journal=second_journal,
            )

            def issue(
                broker: ProviderAccessBroker,
            ) -> ProviderAccessGrant | str:
                try:
                    return broker.issue(
                        self.authorization,
                        route_id="primary-route",
                        request_digest=_REQUEST_DIGEST,
                        request_artifact_digest=(
                            _REQUEST_ARTIFACT_DIGEST
                        ),
                        invocation_index=1,
                    )
                except ProviderAccessDenied as exc:
                    return exc.reason_code

            with ThreadPoolExecutor(max_workers=2) as executor:
                issued = list(executor.map(issue, (first, second)))
            grants = [
                value
                for value in issued
                if isinstance(value, ProviderAccessGrant)
            ]
            self.assertEqual(len(grants), 1)
            self.assertEqual(
                issued.count("provider_invocation_already_issued"),
                1,
            )
            grant = grants[0]

            def invoke(broker: ProviderAccessBroker) -> str:
                try:
                    broker.invoke(
                        grant,
                        self.authorization,
                        b"safe",
                    )
                except ProviderAccessDenied as exc:
                    return exc.reason_code
                return "ok"

            with ThreadPoolExecutor(max_workers=2) as executor:
                invoked = list(executor.map(invoke, (first, second)))
            self.assertEqual(invoked.count("ok"), 1)
            self.assertTrue(
                all(
                    outcome
                    in {
                        "ok",
                        "provider_invocation_outcome_unknown",
                        "provider_result_unavailable",
                    }
                    for outcome in invoked
                )
            )
            self.assertEqual(len(self.invoker.calls), 1)
            first_journal.close()
            second_journal.close()

    def test_closed_registry_fails_without_leaking_exception_context(
        self,
    ) -> None:
        journal = RemoteExecutionJournal()
        broker = ProviderAccessBroker(
            (_route(),),
            authorization_verifier=AuthorizationVerifier(),
            invoker=self.invoker,
            clock=self.clock,
            recovery_journal=journal,
        )
        journal.close()
        with self.assertRaises(ProviderAccessDenied) as raised:
            broker.issue(
                self.authorization,
                route_id="primary-route",
                request_digest=_REQUEST_DIGEST,
                request_artifact_digest=_REQUEST_ARTIFACT_DIGEST,
                invocation_index=1,
            )
        self.assertEqual(
            raised.exception.reason_code,
            "provider_grant_registry_unavailable",
        )
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)

    def test_external_registry_rejects_conflicting_explicit_capacity(
        self,
    ) -> None:
        journal = RemoteExecutionJournal(maximum_provider_grants=2)
        self.addCleanup(journal.close)
        with self.assertRaisesRegex(
            ProviderAccessDenied,
            "provider_grant_capacity_mismatch",
        ):
            ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                maximum_active_grants=1,
                recovery_journal=journal,
            )


if __name__ == "__main__":
    unittest.main()
