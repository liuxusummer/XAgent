from __future__ import annotations

import unittest
from dataclasses import replace

from src.orchestration.artifacts import ArtifactSensitivity
from src.orchestration.policy import ActionRequest, Capability, EffectClass
from src.orchestration.worker_security import (
    WorkerAccessRule,
    WorkerAuthorizationDenied,
    WorkerAuthorizationGate,
    WorkerIdentity,
    WorkerIdentityInvalid,
)


READ = Capability("workspace.read")
RUN = Capability("process.execute")
TRANSPORT_DIGEST = "a" * 64


class _Clock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class _Attestor:
    def __init__(
        self,
        identity: WorkerIdentity | None,
        *,
        error: BaseException | None = None,
    ) -> None:
        self.identity = identity
        self.error = error
        self.presentations: list[object] = []

    def attest(self, presentation: object, *, now: float) -> WorkerIdentity:
        self.presentations.append(presentation)
        if self.error is not None:
            raise self.error
        assert self.identity is not None
        return self.identity


def _identity(**overrides) -> WorkerIdentity:
    values = {
        "worker_id": "worker-1",
        "subject": "spiffe://example.test/worker/1",
        "issuer": "test-workload-attestor",
        "tenant_id": "tenant-1",
        "pool_id": "pool-code",
        "capabilities": (READ, RUN),
        "transport_binding_digest": TRANSPORT_DIGEST,
        "issued_at": 90.0,
        "not_before": 90.0,
        "expires_at": 200.0,
        "attestation_id": "attestation-1",
    }
    values.update(overrides)
    return WorkerIdentity(**values)


def _action(**overrides) -> ActionRequest:
    values = {
        "run_id": "run-1",
        "node_id": "node-1",
        "attempt_id": "attempt-1",
        "tool_name": "code_run",
        "args": {"source": "artifact"},
        "execution_binding_digest": "b" * 64,
        "operation_key": "operation-1",
        "idempotency_key": "operation-1",
        "effect_class": EffectClass.READ_ONLY,
        "capabilities": (READ, RUN),
    }
    values.update(overrides)
    return ActionRequest.from_args(**values)


def _rule(**overrides) -> WorkerAccessRule:
    values = {
        "rule_id": "tenant-1-code",
        "tenant_id": "tenant-1",
        "pool_id": "pool-code",
        "action_names": ("code_run",),
        "allowed_capabilities": (READ, RUN),
        "allowed_effect_classes": (EffectClass.READ_ONLY,),
        "maximum_artifact_sensitivity": ArtifactSensitivity.SENSITIVE,
    }
    values.update(overrides)
    return WorkerAccessRule(**values)


class WorkerSecurityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = _Clock()
        self.attestor = _Attestor(_identity())
        self.gate = WorkerAuthorizationGate(
            self.attestor,
            (_rule(),),
            clock=self.clock,
            authorization_ttl_seconds=30,
        )

    def test_attests_opaque_presentation_and_binds_exact_action(self) -> None:
        presentation = object()
        action = _action()
        authorization = self.gate.authorize(
            presentation,
            tenant_id="tenant-1",
            pool_id="pool-code",
            action=action,
            expected_transport_binding_digest=TRANSPORT_DIGEST,
            expected_worker_id="worker-1",
        )

        self.assertEqual(self.attestor.presentations, [presentation])
        self.assertEqual(authorization.worker_id, "worker-1")
        self.assertEqual(authorization.action_digest, action.action_digest)
        self.assertEqual(authorization.expires_at, 130.0)
        self.assertEqual(
            authorization.maximum_artifact_sensitivity,
            ArtifactSensitivity.SENSITIVE,
        )
        self.assertTrue(self.gate.verify(authorization, now=129.0))
        self.assertFalse(
            self.gate.verify(
                replace(authorization, worker_id="worker-2"),
                now=129.0,
            )
        )
        self.assertFalse(self.gate.verify(authorization, now=130.0))
        authorization.validate_for(
            action=action,
            tenant_id="tenant-1",
            pool_id="pool-code",
            now=129.0,
        )
        with self.assertRaises(WorkerAuthorizationDenied):
            authorization.validate_for(
                action=_action(attempt_id="attempt-2"),
                tenant_id="tenant-1",
                pool_id="pool-code",
                now=129.0,
            )

    def test_direct_identity_payload_is_not_trusted_without_attestor(self) -> None:
        class RejectIdentityPayload:
            def attest(self, presentation: object, *, now: float) -> WorkerIdentity:
                del now
                if isinstance(presentation, WorkerIdentity):
                    raise ValueError("worker supplied identity")
                return _identity()

        gate = WorkerAuthorizationGate(RejectIdentityPayload(), (_rule(),), clock=self.clock)
        with self.assertRaisesRegex(
            WorkerAuthorizationDenied,
            "attestation_failed",
        ):
            gate.authorize(
                _identity(),
                tenant_id="tenant-1",
                pool_id="pool-code",
                action=_action(),
                expected_transport_binding_digest=TRANSPORT_DIGEST,
            )

    def test_transport_tenant_worker_and_capability_mismatches_fail_closed(self) -> None:
        cases = (
            {
                "expected_transport_binding_digest": "c" * 64,
                "reason": "transport_binding_mismatch",
            },
            {
                "tenant_id": "tenant-2",
                "reason": "tenant_pool_mismatch",
            },
            {
                "expected_worker_id": "worker-2",
                "reason": "worker_binding_mismatch",
            },
        )
        for case in cases:
            with self.subTest(case=case["reason"]):
                kwargs = {
                    "tenant_id": case.get("tenant_id", "tenant-1"),
                    "pool_id": "pool-code",
                    "action": _action(),
                    "expected_transport_binding_digest": case.get(
                        "expected_transport_binding_digest",
                        TRANSPORT_DIGEST,
                    ),
                    "expected_worker_id": case.get("expected_worker_id"),
                }
                with self.assertRaisesRegex(
                    WorkerAuthorizationDenied,
                    case["reason"],
                ):
                    self.gate.authorize(object(), **kwargs)

        capability_gate = WorkerAuthorizationGate(
            _Attestor(_identity(capabilities=(READ,))),
            (_rule(),),
            clock=self.clock,
        )
        with self.assertRaisesRegex(
            WorkerAuthorizationDenied,
            "identity_capability_mismatch",
        ):
            capability_gate.authorize(
                object(),
                tenant_id="tenant-1",
                pool_id="pool-code",
                action=_action(),
                expected_transport_binding_digest=TRANSPORT_DIGEST,
            )

    def test_absent_or_underprivileged_rule_denies(self) -> None:
        no_rule = WorkerAuthorizationGate(self.attestor, (), clock=self.clock)
        with self.assertRaisesRegex(
            WorkerAuthorizationDenied,
            "no_matching_worker_rule",
        ):
            no_rule.authorize(
                object(),
                tenant_id="tenant-1",
                pool_id="pool-code",
                action=_action(),
                expected_transport_binding_digest=TRANSPORT_DIGEST,
            )

        underprivileged = WorkerAuthorizationGate(
            self.attestor,
            (_rule(allowed_capabilities=(READ,)),),
            clock=self.clock,
        )
        with self.assertRaisesRegex(
            WorkerAuthorizationDenied,
            "no_matching_worker_rule",
        ):
            underprivileged.authorize(
                object(),
                tenant_id="tenant-1",
                pool_id="pool-code",
                action=_action(),
                expected_transport_binding_digest=TRANSPORT_DIGEST,
            )
        with self.assertRaisesRegex(
            WorkerAuthorizationDenied,
            "no_matching_worker_rule",
        ):
            self.gate.authorize(
                object(),
                tenant_id="tenant-1",
                pool_id="pool-code",
                action=_action(effect_class=EffectClass.DESTRUCTIVE),
                expected_transport_binding_digest=TRANSPORT_DIGEST,
            )

    def test_expired_future_and_overlong_identity_deny(self) -> None:
        identities = (
            _identity(expires_at=100.0),
            _identity(issued_at=140.0, not_before=140.0, expires_at=180.0),
            _identity(issued_at=0.0, not_before=0.0, expires_at=901.0),
        )
        for identity in identities:
            with self.subTest(identity=identity.attestation_id):
                gate = WorkerAuthorizationGate(
                    _Attestor(identity),
                    (_rule(),),
                    clock=self.clock,
                )
                with self.assertRaisesRegex(
                    WorkerAuthorizationDenied,
                    "identity_invalid",
                ):
                    gate.authorize(
                        object(),
                        tenant_id="tenant-1",
                        pool_id="pool-code",
                        action=_action(),
                        expected_transport_binding_digest=TRANSPORT_DIGEST,
                    )

    def test_attestor_failure_is_sanitized(self) -> None:
        gate = WorkerAuthorizationGate(
            _Attestor(None, error=RuntimeError("/control/store.sqlite secret")),
            (_rule(),),
            clock=self.clock,
        )
        with self.assertRaises(WorkerAuthorizationDenied) as raised:
            gate.authorize(
                object(),
                tenant_id="tenant-1",
                pool_id="pool-code",
                action=_action(),
                expected_transport_binding_digest=TRANSPORT_DIGEST,
            )
        self.assertEqual(str(raised.exception), "attestation_failed")
        self.assertNotIn("store.sqlite", str(raised.exception))

    def test_rule_rejects_wildcard_and_authorization_expires_strictly(self) -> None:
        with self.assertRaises(WorkerIdentityInvalid):
            _rule(action_names=("*",))
        authorization = self.gate.authorize(
            object(),
            tenant_id="tenant-1",
            pool_id="pool-code",
            action=_action(),
            expected_transport_binding_digest=TRANSPORT_DIGEST,
        )
        with self.assertRaisesRegex(
            WorkerAuthorizationDenied,
            "authorization_expired",
        ):
            authorization.validate_for(
                action=_action(),
                tenant_id="tenant-1",
                pool_id="pool-code",
                now=authorization.expires_at,
            )
        self.assertNotEqual(
            authorization.authorization_digest,
            replace(authorization, worker_id="worker-2").authorization_digest,
        )

    def test_expired_credential_renews_same_lineage_after_registry_purge(self):
        action = _action()
        authorization = self.gate.authorize(
            object(),
            tenant_id="tenant-1",
            pool_id="pool-code",
            action=action,
            expected_transport_binding_digest=TRANSPORT_DIGEST,
            expected_worker_id="worker-1",
        )
        original_digest = authorization.authorization_digest
        self.clock.value = authorization.expires_at + 1
        # Issuing another credential purges the expired registry entry.  The
        # process-local issuer capability on the trusted record must still
        # permit exact-lineage renewal for a live durable execution.
        self.gate.authorize(
            object(),
            tenant_id="tenant-1",
            pool_id="pool-code",
            action=_action(attempt_id="attempt-2"),
            expected_transport_binding_digest=TRANSPORT_DIGEST,
            expected_worker_id="worker-1",
        )
        renewed = self.gate.renew(
            authorization,
            object(),
            tenant_id="tenant-1",
            pool_id="pool-code",
            action=action,
            expected_transport_binding_digest=TRANSPORT_DIGEST,
            expected_worker_id="worker-1",
        )
        self.assertEqual(renewed.authorization_digest, original_digest)
        self.assertGreater(renewed.expires_at, self.clock.value)
        self.assertTrue(self.gate.verify(renewed, now=self.clock.value))

    def test_renewal_rejects_changed_workload_lineage(self):
        action = _action()
        authorization = self.gate.authorize(
            object(),
            tenant_id="tenant-1",
            pool_id="pool-code",
            action=action,
            expected_transport_binding_digest=TRANSPORT_DIGEST,
            expected_worker_id="worker-1",
        )
        self.attestor.identity = _identity(
            subject="spiffe://example.test/worker/replaced",
        )
        with self.assertRaisesRegex(
            WorkerAuthorizationDenied,
            "authorization_lineage_mismatch",
        ):
            self.gate.renew(
                authorization,
                object(),
                tenant_id="tenant-1",
                pool_id="pool-code",
                action=action,
                expected_transport_binding_digest=TRANSPORT_DIGEST,
                expected_worker_id="worker-1",
            )


if __name__ == "__main__":
    unittest.main()
