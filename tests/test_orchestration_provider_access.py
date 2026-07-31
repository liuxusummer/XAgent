from __future__ import annotations

import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

from src.orchestration.provider_access import (
    ProviderAccessBroker,
    ProviderAccessDenied,
    ProviderAccessGrant,
    ProviderRouteDescriptor,
)
from src.orchestration.remote_execution_journal import (
    RemoteExecutionJournal,
)
from src.orchestration.worker_security import WorkerAuthorization
from src.orchestration.artifacts import ArtifactSensitivity


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
            "provider_grant_unavailable",
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
        self.assertEqual(
            outcomes.count("provider_grant_unavailable"),
            15,
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
            "provider_grant_unavailable",
        ):
            broker.invoke(grant, self.authorization, b"safe")
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
            "provider_grant_unavailable",
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
            first_journal = RemoteExecutionJournal(path)
            first = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                recovery_journal=first_journal,
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
            )
            self.assertTrue(second.durable_recovery_ready)
            result = second.invoke(
                grant,
                self.authorization,
                payload,
            )
            self.assertEqual(result.content, b'{"status":"ok"}')
            second_journal.close()

            third_journal = RemoteExecutionJournal(path)
            third = ProviderAccessBroker(
                (_route(),),
                authorization_verifier=AuthorizationVerifier(),
                invoker=self.invoker,
                clock=self.clock,
                recovery_journal=third_journal,
            )
            with self.assertRaisesRegex(
                ProviderAccessDenied,
                "provider_grant_unavailable",
            ):
                third.invoke(grant, self.authorization, payload)
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
            self.assertEqual(
                invoked.count("provider_grant_unavailable"),
                1,
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
