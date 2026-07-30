from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

from src.orchestration.artifact_broker import (
    ArtifactGrantBroker,
    ArtifactGrantConsumed,
    ArtifactGrantDenied,
    ArtifactOutputHandle,
    ArtifactStagingReceipt,
    MAX_BROKER_ARTIFACT_BYTES,
    MAX_BROKER_RESIDENT_BYTES,
)
from src.orchestration.artifacts import (
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    LocalArtifactStore,
)
from src.orchestration.policy import ActionRequest, Capability, EffectClass
from src.orchestration.worker_security import (
    WorkerAccessRule,
    WorkerAuthorization,
    WorkerAuthorizationGate,
    WorkerIdentity,
)


class _Clock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


def _authorization(**overrides) -> WorkerAuthorization:
    values = {
        "authorization_id": "test-trusted-authorization",
        "worker_id": "worker-1",
        "tenant_id": "tenant-1",
        "pool_id": "pool-code",
        "run_id": "run-1",
        "node_id": "node-1",
        "attempt_id": "attempt-1",
        "action_digest": "a" * 64,
        "identity_binding_digest": "b" * 64,
        "transport_binding_digest": "c" * 64,
        "rule_id": "rule-1",
        "maximum_artifact_sensitivity": ArtifactSensitivity.INTERNAL,
        "issued_at": 90.0,
        "expires_at": 200.0,
    }
    values.update(overrides)
    return WorkerAuthorization(**values)


class _AuthorizationVerifier:
    def verify(self, authorization: WorkerAuthorization, *, now: float) -> bool:
        return (
            authorization.authorization_id == "test-trusted-authorization"
            and now < authorization.expires_at
        )


class _CountingArtifactStore(LocalArtifactStore):
    def __init__(self, root: Path) -> None:
        self.put_bytes_calls = 0
        super().__init__(root)

    def put_bytes(self, content: bytes, **metadata) -> ArtifactRef:
        self.put_bytes_calls += 1
        return super().put_bytes(content, **metadata)


class _BlockingArtifactStore(_CountingArtifactStore):
    def __init__(self, root: Path) -> None:
        self.write_started = threading.Event()
        self.allow_write = threading.Event()
        super().__init__(root)

    def put_bytes(self, content: bytes, **metadata) -> ArtifactRef:
        self.write_started.set()
        if not self.allow_write.wait(timeout=5):
            raise TimeoutError("blocked artifact write was not released")
        return super().put_bytes(content, **metadata)


class _FailingArtifactStore(_CountingArtifactStore):
    def put_bytes(self, content: bytes, **metadata) -> ArtifactRef:
        del content, metadata
        self.put_bytes_calls += 1
        raise OSError("/private/control-plane/path")


EXECUTE = Capability("process.execute")
TRANSPORT_DIGEST = "f" * 64


class _WorkloadAttestor:
    def attest(self, presentation: object, *, now: float) -> WorkerIdentity:
        del presentation, now
        return WorkerIdentity(
            worker_id="worker-1",
            subject="spiffe://example.test/worker/1",
            issuer="test-workload-attestor",
            tenant_id="tenant-1",
            pool_id="pool-code",
            capabilities=(EXECUTE,),
            transport_binding_digest=TRANSPORT_DIGEST,
            issued_at=90.0,
            not_before=90.0,
            expires_at=200.0,
            attestation_id="attestation-1",
        )


class ArtifactGrantBrokerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.store = _CountingArtifactStore(
            self.root / "control-plane-artifacts"
        )
        self.clock = _Clock()
        self.authorization_verifier = _AuthorizationVerifier()
        self.broker = ArtifactGrantBroker(
            self.store,
            authorization_verifier=self.authorization_verifier,
            clock=self.clock,
        )
        self.content = b"verified worker input"
        self.ref = self.store.put_bytes(
            self.content,
            kind=ArtifactKind.FILE_SNAPSHOT,
            sensitivity=ArtifactSensitivity.INTERNAL,
            producer_run_id="upstream-run",
            producer_node_id="upstream-node",
            producer_attempt_id="upstream-attempt",
        )
        self.worker_gate = WorkerAuthorizationGate(
            _WorkloadAttestor(),
            (
                WorkerAccessRule(
                    rule_id="tenant-code",
                    tenant_id="tenant-1",
                    pool_id="pool-code",
                    action_names=("code_run",),
                    allowed_capabilities=(EXECUTE,),
                    allowed_effect_classes=(EffectClass.READ_ONLY,),
                    maximum_artifact_sensitivity=ArtifactSensitivity.INTERNAL,
                ),
            ),
            clock=self.clock,
            authorization_ttl_seconds=60,
        )
        self.write_broker = ArtifactGrantBroker(
            self.store,
            authorization_verifier=self.worker_gate,
            clock=self.clock,
        )
        self.worker_authorization = self._worker_authorization()

    def _grant(self, **kwargs):
        return self.broker.issue_read_grant(
            _authorization(),
            tenant_id="tenant-1",
            run_id="run-1",
            attempt_id="attempt-1",
            ref=self.ref,
            **kwargs,
        )

    def _worker_authorization(
        self,
        *,
        attempt_id: str = "attempt-1",
        marker: str = "primary",
    ) -> WorkerAuthorization:
        action = ActionRequest.from_args(
            run_id="run-1",
            node_id="node-1",
            attempt_id=attempt_id,
            tool_name="code_run",
            args={"marker": marker},
            execution_binding_digest="1" * 64,
            operation_key=f"operation-{attempt_id}-{marker}",
            idempotency_key=f"operation-{attempt_id}-{marker}",
            effect_class=EffectClass.READ_ONLY,
            capabilities=(EXECUTE,),
        )
        return self.worker_gate.authorize(
            object(),
            tenant_id="tenant-1",
            pool_id="pool-code",
            action=action,
            expected_transport_binding_digest=TRANSPORT_DIGEST,
            expected_worker_id="worker-1",
        )

    def _write_grant(
        self,
        content: bytes,
        *,
        authorization: WorkerAuthorization | None = None,
        maximum_bytes: int | None = None,
        ttl_seconds: float | None = None,
    ):
        selected = authorization or self.worker_authorization
        return self.write_broker.issue_write_grant(
            selected,
            tenant_id="tenant-1",
            run_id="run-1",
            node_id="node-1",
            attempt_id=selected.attempt_id,
            kind=ArtifactKind.TOOL_RESULT,
            sensitivity=ArtifactSensitivity.INTERNAL,
            media_type="application/json",
            maximum_bytes=len(content) if maximum_bytes is None else maximum_bytes,
            declared_sha256=hashlib.sha256(content).hexdigest(),
            ttl_seconds=ttl_seconds,
        )

    def test_grant_is_path_free_exact_and_single_use(self) -> None:
        grant = self._grant()
        wire = grant.to_wire_dict()
        encoded = json.dumps(wire, sort_keys=True)

        self.assertNotIn("uri", encoded)
        self.assertNotIn(str(self.store.root), encoded)
        self.assertEqual(wire["descriptor"]["sha256"], self.ref.sha256)
        self.assertEqual(wire["worker_id"], "worker-1")
        self.assertEqual(wire["run_id"], "run-1")
        self.assertEqual(wire["attempt_id"], "attempt-1")

        payload = self.broker.redeem_read_grant(grant, _authorization())
        self.assertEqual(payload.content, self.content)
        self.assertEqual(payload.descriptor.sha256, self.ref.sha256)
        with self.assertRaisesRegex(ArtifactGrantConsumed, "grant_unavailable"):
            self.broker.redeem_read_grant(grant, _authorization())

    def test_wrong_worker_or_action_cannot_redeem_and_does_not_consume(self) -> None:
        grant = self._grant()
        for authorization in (
            _authorization(worker_id="worker-2"),
            _authorization(action_digest="d" * 64),
            _authorization(attempt_id="attempt-2"),
        ):
            with self.subTest(worker=authorization.worker_id):
                with self.assertRaisesRegex(
                    ArtifactGrantDenied,
                    "authorization_binding_mismatch",
                ):
                    self.broker.redeem_read_grant(grant, authorization)

        payload = self.broker.redeem_read_grant(grant, _authorization())
        self.assertEqual(payload.content, self.content)

    def test_grant_tampering_and_token_substitution_fail_closed(self) -> None:
        first = self._grant()
        second = self._grant()
        with self.assertRaisesRegex(ArtifactGrantDenied, "grant_binding_mismatch"):
            self.broker.redeem_read_grant(
                replace(first, token=second.token),
                _authorization(),
            )
        with self.assertRaisesRegex(ArtifactGrantDenied, "grant_binding_mismatch"):
            self.broker.redeem_read_grant(
                replace(first, artifact_binding_digest="e" * 64),
                _authorization(),
            )
        self.assertEqual(
            self.broker.redeem_read_grant(first, _authorization()).content,
            self.content,
        )

    def test_concurrent_redeem_delivers_bytes_exactly_once(self) -> None:
        grant = self._grant()

        def redeem() -> str:
            try:
                return self.broker.redeem_read_grant(
                    grant,
                    _authorization(),
                ).content.decode("utf-8")
            except ArtifactGrantConsumed:
                return "consumed"

        with ThreadPoolExecutor(max_workers=16) as pool:
            outcomes = list(pool.map(lambda _: redeem(), range(32)))
        self.assertEqual(outcomes.count(self.content.decode("utf-8")), 1)
        self.assertEqual(outcomes.count("consumed"), 31)

    def test_expiry_is_strict_and_bounded_by_authorization(self) -> None:
        authorization = _authorization()
        grant = self.broker.issue_read_grant(
            authorization,
            tenant_id="tenant-1",
            run_id="run-1",
            attempt_id="attempt-1",
            ref=self.ref,
            ttl_seconds=5,
        )
        self.assertEqual(grant.expires_at, 105.0)
        self.clock.value = 105.0
        with self.assertRaisesRegex(ArtifactGrantConsumed, "grant_unavailable"):
            self.broker.redeem_read_grant(grant, authorization)
        self.clock.value = 100.0
        authorization_bound = _authorization(expires_at=103.0)
        authorization_bound_grant = self.broker.issue_read_grant(
            authorization_bound,
            tenant_id="tenant-1",
            run_id="run-1",
            attempt_id="attempt-1",
            ref=self.ref,
            ttl_seconds=30,
        )
        self.assertEqual(authorization_bound_grant.expires_at, 103.0)
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "grant_ttl_exceeds_policy",
        ):
            self._grant(ttl_seconds=301)

    def test_cross_tenant_run_or_attempt_issue_is_denied(self) -> None:
        for values in (
            {"tenant_id": "tenant-2", "run_id": "run-1", "attempt_id": "attempt-1"},
            {"tenant_id": "tenant-1", "run_id": "run-2", "attempt_id": "attempt-1"},
            {"tenant_id": "tenant-1", "run_id": "run-1", "attempt_id": "attempt-2"},
        ):
            with self.subTest(values=values):
                with self.assertRaisesRegex(
                    ArtifactGrantDenied,
                    "authorization_binding_mismatch",
                ):
                    self.broker.issue_read_grant(
                        _authorization(),
                        ref=self.ref,
                        **values,
                    )

    def test_forged_authorization_is_rejected_before_grant_binding(self) -> None:
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "worker_authorization_unverified",
        ):
            self.broker.issue_read_grant(
                _authorization(authorization_id="forged"),
                tenant_id="tenant-1",
                run_id="run-1",
                attempt_id="attempt-1",
                ref=self.ref,
            )

    def test_sensitivity_and_unencrypted_secret_fail_closed(self) -> None:
        sensitive = self.store.put_bytes(
            b"sensitive",
            sensitivity=ArtifactSensitivity.SENSITIVE,
        )
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "artifact_sensitivity_exceeds_policy",
        ):
            self.broker.issue_read_grant(
                _authorization(),
                tenant_id="tenant-1",
                run_id="run-1",
                attempt_id="attempt-1",
                ref=sensitive,
            )

        secret = self.store.put_bytes(
            b"secret",
            sensitivity=ArtifactSensitivity.SECRET,
        )
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "unencrypted_secret_artifact",
        ):
            self.broker.issue_read_grant(
                _authorization(
                    maximum_artifact_sensitivity=ArtifactSensitivity.SECRET,
                ),
                tenant_id="tenant-1",
                run_id="run-1",
                attempt_id="attempt-1",
                ref=secret,
            )

    def test_store_tamper_is_sanitized_and_consumes_grant(self) -> None:
        grant = self._grant()
        (self.store.root / self.ref.uri).write_bytes(b"tampered")
        with self.assertRaises(ArtifactGrantDenied) as raised:
            self.broker.redeem_read_grant(grant, _authorization())
        self.assertEqual(str(raised.exception), "artifact_integrity_failed")
        self.assertNotIn(str(self.store.root), str(raised.exception))
        with self.assertRaises(ArtifactGrantConsumed):
            self.broker.redeem_read_grant(grant, _authorization())

    def test_size_limit_is_checked_before_issuance(self) -> None:
        broker = ArtifactGrantBroker(
            self.store,
            authorization_verifier=self.authorization_verifier,
            clock=self.clock,
            maximum_artifact_bytes=4,
        )
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "artifact_size_exceeds_policy",
        ):
            broker.issue_read_grant(
                _authorization(),
                tenant_id="tenant-1",
                run_id="run-1",
                attempt_id="attempt-1",
                ref=self.ref,
            )

    def test_write_grant_stages_and_finalizes_control_plane_ref(self) -> None:
        content = b'{"result":"verified"}'
        baseline = self.store.put_bytes_calls
        grant = self._write_grant(content)
        wire = grant.to_wire_dict()
        encoded = json.dumps(wire, sort_keys=True)
        self.assertNotIn("uri", encoded)
        self.assertNotIn(str(self.store.root), encoded)
        self.assertEqual(wire["tenant_id"], "tenant-1")
        self.assertEqual(wire["worker_id"], "worker-1")
        self.assertEqual(wire["run_id"], "run-1")
        self.assertEqual(wire["node_id"], "node-1")
        self.assertEqual(wire["attempt_id"], "attempt-1")
        self.assertEqual(
            wire["action_digest"],
            self.worker_authorization.action_digest,
        )
        handle = ArtifactOutputHandle.from_wire_dict(wire["handle"])
        self.assertEqual(
            self.write_broker.resolve_output_handle(handle),
            grant,
        )
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "grant_binding_mismatch",
        ):
            self.write_broker.resolve_output_handle(
                replace(handle, token="wrong-token"),
            )
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "invalid_output_handle",
        ):
            ArtifactOutputHandle.from_wire_dict(
                {**wire["handle"], "uri": "/control/artifacts"},
            )

        staged = self.write_broker.stage_write(
            grant,
            self.worker_authorization,
            content=content,
            declared_sha256=hashlib.sha256(content).hexdigest(),
        )
        self.assertIsInstance(staged, ArtifactStagingReceipt)
        self.assertEqual(self.store.put_bytes_calls, baseline)

        ref = self.write_broker.finalize_write(
            grant,
            self.worker_authorization,
            staged,
        )
        self.assertEqual(self.store.put_bytes_calls, baseline + 1)
        self.assertTrue(self.store.verify(ref))
        self.assertEqual(self.store.read(ref), content)
        self.assertEqual(ref.sha256, hashlib.sha256(content).hexdigest())
        self.assertEqual(ref.size, len(content))
        self.assertEqual(ref.kind, ArtifactKind.TOOL_RESULT)
        self.assertEqual(ref.sensitivity, ArtifactSensitivity.INTERNAL)
        self.assertEqual(ref.media_type, "application/json")
        self.assertEqual(ref.producer_run_id, "run-1")
        self.assertEqual(ref.producer_node_id, "node-1")
        self.assertEqual(ref.producer_attempt_id, "attempt-1")
        self.assertEqual(dict(ref.metadata), {})
        replayed = self.write_broker.finalize_write(
            grant,
            self.worker_authorization,
            staged,
        )
        self.assertEqual(replayed, ref)
        self.assertEqual(self.store.put_bytes_calls, baseline + 1)

    def test_concurrent_write_finalize_installs_exactly_once(self) -> None:
        content = b"one durable output"
        grant = self._write_grant(content)
        staged = self.write_broker.stage_write(
            grant,
            self.worker_authorization,
            content=content,
            declared_sha256=hashlib.sha256(content).hexdigest(),
        )
        baseline = self.store.put_bytes_calls

        def finalize() -> ArtifactRef | str:
            try:
                return self.write_broker.finalize_write(
                    grant,
                    self.worker_authorization,
                    staged,
                )
            except ArtifactGrantConsumed:
                return "consumed"

        with ThreadPoolExecutor(max_workers=16) as pool:
            outcomes = list(pool.map(lambda _: finalize(), range(32)))
        refs = [outcome for outcome in outcomes if isinstance(outcome, ArtifactRef)]
        self.assertGreaterEqual(len(refs), 1)
        self.assertTrue(all(ref == refs[0] for ref in refs))
        self.assertEqual(len(refs) + outcomes.count("consumed"), 32)
        self.assertEqual(self.store.put_bytes_calls, baseline + 1)
        self.assertTrue(self.store.verify(refs[0]))

    def test_output_handle_finalize_replays_same_server_receipt(self) -> None:
        content = b"completion response may be lost"
        grant = self._write_grant(content)
        staged = self.write_broker.stage_write(
            grant,
            self.worker_authorization,
            content=content,
            declared_sha256=grant.declared_sha256,
        )
        handle = grant.output_handle
        baseline = self.store.put_bytes_calls

        first = self.write_broker.finalize_output_handle(
            handle,
            self.worker_authorization,
        )
        # Simulate a lost completion response: the caller only retains handle.
        replay = self.write_broker.finalize_output_handle(
            ArtifactOutputHandle.from_wire_dict(handle.to_wire_dict()),
            self.worker_authorization,
        )
        low_level_replay = self.write_broker.finalize_write(
            grant,
            self.worker_authorization,
            staged,
        )

        self.assertEqual(first, replay)
        self.assertEqual(first, low_level_replay)
        self.assertEqual(self.store.put_bytes_calls, baseline + 1)
        self.assertTrue(self.store.verify(replay))

    def test_concurrent_output_handle_finalize_writes_at_most_once(self) -> None:
        content = b"concurrent completion"
        grant = self._write_grant(content)
        self.write_broker.stage_write(
            grant,
            self.worker_authorization,
            content=content,
            declared_sha256=grant.declared_sha256,
        )
        baseline = self.store.put_bytes_calls

        def finalize() -> ArtifactRef | str:
            try:
                return self.write_broker.finalize_output_handle(
                    grant.output_handle,
                    self.worker_authorization,
                )
            except ArtifactGrantConsumed:
                return "in_progress"

        with ThreadPoolExecutor(max_workers=16) as pool:
            outcomes = list(pool.map(lambda _: finalize(), range(32)))
        refs = [outcome for outcome in outcomes if isinstance(outcome, ArtifactRef)]
        self.assertGreaterEqual(len(refs), 1)
        self.assertTrue(all(ref == refs[0] for ref in refs))
        self.assertEqual(len(refs) + outcomes.count("in_progress"), 32)
        self.assertEqual(self.store.put_bytes_calls, baseline + 1)
        replay = self.write_broker.finalize_output_handle(
            grant.output_handle,
            self.worker_authorization,
        )
        self.assertEqual(replay, refs[0])
        self.assertEqual(self.store.put_bytes_calls, baseline + 1)

    def test_output_handle_finalize_rejects_unstaged_wrong_or_expired(self) -> None:
        content = b"handle validation"

        def assert_no_store_write(callback) -> None:
            baseline = self.store.put_bytes_calls
            with self.assertRaises(ArtifactGrantDenied):
                callback()
            self.assertEqual(self.store.put_bytes_calls, baseline)

        unstaged = self._write_grant(content)
        assert_no_store_write(
            lambda: self.write_broker.finalize_output_handle(
                unstaged.output_handle,
                self.worker_authorization,
            )
        )
        assert_no_store_write(
            lambda: self.write_broker.finalize_output_handle(
                replace(unstaged.output_handle, token="wrong-token"),
                self.worker_authorization,
            )
        )
        other_authorization = self._worker_authorization(
            attempt_id="attempt-2",
            marker="other-finalize",
        )
        assert_no_store_write(
            lambda: self.write_broker.finalize_output_handle(
                unstaged.output_handle,
                other_authorization,
            )
        )
        assert_no_store_write(
            lambda: self.write_broker.finalize_output_handle(
                unstaged.output_handle,
                replace(
                    self.worker_authorization,
                    authorization_id="forged",
                ),
            )
        )

        expiring = self._write_grant(content, ttl_seconds=5)
        self.write_broker.stage_write(
            expiring,
            self.worker_authorization,
            content=content,
            declared_sha256=expiring.declared_sha256,
        )
        self.clock.value = 105.0
        assert_no_store_write(
            lambda: self.write_broker.finalize_output_handle(
                expiring.output_handle,
                self.worker_authorization,
            )
        )

    def test_invalid_write_inputs_create_no_store_reference(self) -> None:
        content = b"bounded output"

        def assert_no_store_write(callback) -> None:
            baseline = self.store.put_bytes_calls
            with self.assertRaises(ArtifactGrantDenied):
                callback()
            self.assertEqual(self.store.put_bytes_calls, baseline)

        oversized_grant = self._write_grant(content, maximum_bytes=4)
        assert_no_store_write(
            lambda: self.write_broker.stage_write(
                oversized_grant,
                self.worker_authorization,
                content=content,
                declared_sha256=hashlib.sha256(content).hexdigest(),
            )
        )

        digest_grant = self._write_grant(content)
        assert_no_store_write(
            lambda: self.write_broker.stage_write(
                digest_grant,
                self.worker_authorization,
                content=b"tampered",
                declared_sha256=digest_grant.declared_sha256,
            )
        )
        assert_no_store_write(
            lambda: self.write_broker.stage_write(
                digest_grant,
                self.worker_authorization,
                content=content,
                declared_sha256="9" * 64,
            )
        )
        assert_no_store_write(
            lambda: self.write_broker.stage_write(
                digest_grant,
                self.worker_authorization,
                content=self.ref,  # type: ignore[arg-type]
                declared_sha256=digest_grant.declared_sha256,
            )
        )

        other_authorization = self._worker_authorization(
            attempt_id="attempt-2",
            marker="other",
        )
        assert_no_store_write(
            lambda: self.write_broker.stage_write(
                digest_grant,
                other_authorization,
                content=content,
                declared_sha256=digest_grant.declared_sha256,
            )
        )
        assert_no_store_write(
            lambda: self.write_broker.stage_write(
                digest_grant,
                replace(
                    self.worker_authorization,
                    authorization_id="forged",
                ),
                content=content,
                declared_sha256=digest_grant.declared_sha256,
            )
        )
        assert_no_store_write(
            lambda: self.write_broker.issue_write_grant(
                self.worker_authorization,
                tenant_id="tenant-2",
                run_id="run-1",
                node_id="node-1",
                attempt_id="attempt-1",
                kind=ArtifactKind.TOOL_RESULT,
                sensitivity=ArtifactSensitivity.INTERNAL,
                media_type="application/json",
                maximum_bytes=len(content),
                declared_sha256=hashlib.sha256(content).hexdigest(),
            )
        )

    def test_expired_write_finalize_never_touches_store(self) -> None:
        content = b"expires before finalization"
        grant = self._write_grant(content, ttl_seconds=5)
        staged = self.write_broker.stage_write(
            grant,
            self.worker_authorization,
            content=content,
            declared_sha256=hashlib.sha256(content).hexdigest(),
        )
        baseline = self.store.put_bytes_calls
        self.clock.value = 105.0
        with self.assertRaisesRegex(
            ArtifactGrantConsumed,
            "write_grant_unavailable",
        ):
            self.write_broker.finalize_write(
                grant,
                self.worker_authorization,
                staged,
            )
        self.assertEqual(self.store.put_bytes_calls, baseline)

    def test_write_policy_rejects_sensitivity_size_and_secret_pre_store(self) -> None:
        content = b"policy rejected"
        baseline = self.store.put_bytes_calls
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "artifact_sensitivity_exceeds_policy",
        ):
            self.write_broker.issue_write_grant(
                self.worker_authorization,
                tenant_id="tenant-1",
                run_id="run-1",
                node_id="node-1",
                attempt_id="attempt-1",
                kind=ArtifactKind.TOOL_RESULT,
                sensitivity=ArtifactSensitivity.SENSITIVE,
                media_type="application/json",
                maximum_bytes=len(content),
                declared_sha256=hashlib.sha256(content).hexdigest(),
            )
        secret_authorization = replace(
            self.worker_authorization,
            maximum_artifact_sensitivity=ArtifactSensitivity.SECRET,
        )
        # A mutated authorization is not Gate-issued, so it fails before
        # sensitivity handling and still cannot reach ArtifactStore.
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "worker_authorization_unverified",
        ):
            self.write_broker.issue_write_grant(
                secret_authorization,
                tenant_id="tenant-1",
                run_id="run-1",
                node_id="node-1",
                attempt_id="attempt-1",
                kind=ArtifactKind.TOOL_RESULT,
                sensitivity=ArtifactSensitivity.SECRET,
                media_type="application/octet-stream",
                maximum_bytes=len(content),
                declared_sha256=hashlib.sha256(content).hexdigest(),
            )
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "artifact_size_exceeds_policy",
        ):
            self.write_broker.issue_write_grant(
                self.worker_authorization,
                tenant_id="tenant-1",
                run_id="run-1",
                node_id="node-1",
                attempt_id="attempt-1",
                kind=ArtifactKind.TOOL_RESULT,
                sensitivity=ArtifactSensitivity.INTERNAL,
                media_type="application/json",
                maximum_bytes=MAX_BROKER_ARTIFACT_BYTES + 1,
                declared_sha256=hashlib.sha256(content).hexdigest(),
            )
        self.assertEqual(self.store.put_bytes_calls, baseline)

    def test_global_resident_budget_rejects_new_stage_without_losing_old(self) -> None:
        broker = ArtifactGrantBroker(
            self.store,
            authorization_verifier=self.worker_gate,
            clock=self.clock,
            maximum_artifact_bytes=8,
            maximum_resident_bytes=4,
        )

        def issue(content: bytes):
            return broker.issue_write_grant(
                self.worker_authorization,
                tenant_id="tenant-1",
                run_id="run-1",
                node_id="node-1",
                attempt_id="attempt-1",
                kind=ArtifactKind.TOOL_RESULT,
                sensitivity=ArtifactSensitivity.INTERNAL,
                media_type="application/json",
                maximum_bytes=len(content),
                declared_sha256=hashlib.sha256(content).hexdigest(),
            )

        first_content = b"AAAA"
        second_content = b"BBBB"
        first = issue(first_content)
        second = issue(second_content)
        first_receipt = broker.stage_write(
            first,
            self.worker_authorization,
            content=first_content,
            declared_sha256=first.declared_sha256,
        )
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "resident_byte_limit_reached",
        ):
            broker.stage_write(
                second,
                self.worker_authorization,
                content=second_content,
                declared_sha256=second.declared_sha256,
            )
        self.assertEqual(broker._resident_bytes, 4)
        self.assertEqual(broker._write_grants[first.grant_id].state, "staged")
        self.assertEqual(
            broker._write_grants[first.grant_id].content,
            first_content,
        )
        self.assertEqual(broker._write_grants[second.grant_id].state, "issued")

        broker.finalize_write(
            first,
            self.worker_authorization,
            first_receipt,
        )
        self.assertEqual(broker._resident_bytes, 0)
        broker.stage_write(
            second,
            self.worker_authorization,
            content=second_content,
            declared_sha256=second.declared_sha256,
        )
        self.assertEqual(broker._resident_bytes, 4)

    def test_resident_limit_configuration_is_explicitly_bounded(self) -> None:
        for invalid in (False, 0, MAX_BROKER_RESIDENT_BYTES + 1):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(
                    ArtifactGrantDenied,
                    "invalid_resident_byte_limit",
                ):
                    ArtifactGrantBroker(
                        self.store,
                        authorization_verifier=self.worker_gate,
                        clock=self.clock,
                        maximum_resident_bytes=invalid,
                    )

    def test_concurrent_stage_reserves_global_budget_atomically(self) -> None:
        broker = ArtifactGrantBroker(
            self.store,
            authorization_verifier=self.worker_gate,
            clock=self.clock,
            maximum_artifact_bytes=4,
            maximum_resident_bytes=4,
        )
        content = b"race"

        def issue():
            return broker.issue_write_grant(
                self.worker_authorization,
                tenant_id="tenant-1",
                run_id="run-1",
                node_id="node-1",
                attempt_id="attempt-1",
                kind=ArtifactKind.TOOL_RESULT,
                sensitivity=ArtifactSensitivity.INTERNAL,
                media_type="application/json",
                maximum_bytes=len(content),
                declared_sha256=hashlib.sha256(content).hexdigest(),
            )

        grants = (issue(), issue())
        barrier = threading.Barrier(3)

        def stage(grant):
            barrier.wait()
            try:
                receipt = broker.stage_write(
                    grant,
                    self.worker_authorization,
                    content=content,
                    declared_sha256=grant.declared_sha256,
                )
            except ArtifactGrantDenied as exc:
                return exc.reason_code
            return receipt

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(stage, grant) for grant in grants]
            barrier.wait()
            outcomes = [future.result(timeout=2) for future in futures]
        receipts = [
            outcome
            for outcome in outcomes
            if isinstance(outcome, ArtifactStagingReceipt)
        ]
        self.assertEqual(1, len(receipts))
        self.assertEqual(1, outcomes.count("resident_byte_limit_reached"))
        self.assertEqual(len(content), broker._resident_bytes)
        staged_grant = next(
            grant
            for grant in grants
            if broker._write_grants[grant.grant_id].state == "staged"
        )
        issued_grant = next(grant for grant in grants if grant != staged_grant)
        broker.finalize_write(
            staged_grant,
            self.worker_authorization,
            receipts[0],
        )
        self.assertEqual(0, broker._resident_bytes)
        broker.stage_write(
            issued_grant,
            self.worker_authorization,
            content=content,
            declared_sha256=issued_grant.declared_sha256,
        )
        self.assertEqual(len(content), broker._resident_bytes)

    def test_finalizing_content_remains_charged_until_store_returns(self) -> None:
        store = _BlockingArtifactStore(self.root / "blocking-artifacts")
        broker = ArtifactGrantBroker(
            store,
            authorization_verifier=self.worker_gate,
            clock=self.clock,
            maximum_artifact_bytes=8,
            maximum_resident_bytes=4,
        )
        content = b"hold"

        def issue():
            return broker.issue_write_grant(
                self.worker_authorization,
                tenant_id="tenant-1",
                run_id="run-1",
                node_id="node-1",
                attempt_id="attempt-1",
                kind=ArtifactKind.TOOL_RESULT,
                sensitivity=ArtifactSensitivity.INTERNAL,
                media_type="application/json",
                maximum_bytes=len(content),
                declared_sha256=hashlib.sha256(content).hexdigest(),
            )

        first = issue()
        first_receipt = broker.stage_write(
            first,
            self.worker_authorization,
            content=content,
            declared_sha256=first.declared_sha256,
        )
        second = issue()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                broker.finalize_write,
                first,
                self.worker_authorization,
                first_receipt,
            )
            self.assertTrue(store.write_started.wait(timeout=2))
            try:
                record = broker._write_grants[first.grant_id]
                self.assertEqual(record.state, "finalizing")
                self.assertEqual(record.content, content)
                self.assertEqual(broker._resident_bytes, len(content))
                with self.assertRaisesRegex(
                    ArtifactGrantDenied,
                    "resident_byte_limit_reached",
                ):
                    broker.stage_write(
                        second,
                        self.worker_authorization,
                        content=content,
                        declared_sha256=second.declared_sha256,
                    )
            finally:
                store.allow_write.set()
            self.assertIsInstance(future.result(timeout=2), ArtifactRef)
        self.assertEqual(broker._resident_bytes, 0)
        self.assertIsNone(broker._write_grants[first.grant_id].content)

    def test_expired_finalizing_grant_releases_only_after_store_returns(self) -> None:
        store = _BlockingArtifactStore(self.root / "expiring-artifacts")
        broker = ArtifactGrantBroker(
            store,
            authorization_verifier=self.worker_gate,
            clock=self.clock,
            maximum_artifact_bytes=4,
            maximum_resident_bytes=4,
        )
        content = b"hold"
        grant = broker.issue_write_grant(
            self.worker_authorization,
            tenant_id="tenant-1",
            run_id="run-1",
            node_id="node-1",
            attempt_id="attempt-1",
            kind=ArtifactKind.TOOL_RESULT,
            sensitivity=ArtifactSensitivity.INTERNAL,
            media_type="application/json",
            maximum_bytes=len(content),
            declared_sha256=hashlib.sha256(content).hexdigest(),
            ttl_seconds=5,
        )
        receipt = broker.stage_write(
            grant,
            self.worker_authorization,
            content=content,
            declared_sha256=grant.declared_sha256,
        )
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                broker.finalize_write,
                grant,
                self.worker_authorization,
                receipt,
            )
            self.assertTrue(store.write_started.wait(timeout=2))
            self.clock.value = grant.expires_at
            try:
                with self.assertRaises(ArtifactGrantConsumed):
                    broker.resolve_output_handle(grant.output_handle)
                record = broker._write_grants[grant.grant_id]
                self.assertEqual(record.state, "expired")
                self.assertEqual(record.content, content)
                self.assertEqual(broker._resident_bytes, len(content))
            finally:
                store.allow_write.set()
            with self.assertRaises(ArtifactGrantConsumed):
                future.result(timeout=2)
        self.assertEqual(broker._resident_bytes, 0)
        self.clock.value = grant.expires_at - 1
        with self.assertRaises(ArtifactGrantConsumed):
            broker.resolve_output_handle(grant.output_handle)

    def test_failed_finalization_releases_budget_and_is_irreversible(self) -> None:
        store = _FailingArtifactStore(self.root / "failing-artifacts")
        broker = ArtifactGrantBroker(
            store,
            authorization_verifier=self.worker_gate,
            clock=self.clock,
            maximum_artifact_bytes=4,
            maximum_resident_bytes=4,
        )
        content = b"fail"
        grant = broker.issue_write_grant(
            self.worker_authorization,
            tenant_id="tenant-1",
            run_id="run-1",
            node_id="node-1",
            attempt_id="attempt-1",
            kind=ArtifactKind.TOOL_RESULT,
            sensitivity=ArtifactSensitivity.INTERNAL,
            media_type="application/json",
            maximum_bytes=len(content),
            declared_sha256=hashlib.sha256(content).hexdigest(),
        )
        receipt = broker.stage_write(
            grant,
            self.worker_authorization,
            content=content,
            declared_sha256=grant.declared_sha256,
        )
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "artifact_write_failed",
        ) as raised:
            broker.finalize_write(
                grant,
                self.worker_authorization,
                receipt,
            )
        self.assertNotIn(str(store.root), str(raised.exception))
        self.assertEqual(broker._resident_bytes, 0)
        self.assertEqual(broker._write_grants[grant.grant_id].state, "failed")
        self.clock.value -= 1
        with self.assertRaises(ArtifactGrantConsumed):
            broker.resolve_output_handle(grant.output_handle)


if __name__ == "__main__":
    unittest.main()
