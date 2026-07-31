from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

from src.orchestration.agent_request import (
    AGENT_ACTIVITY_REQUEST_MEDIA_TYPE,
    MAX_AGENT_ACTIVITY_INPUT_REFS,
    MAX_AGENT_ACTIVITY_REQUEST_BYTES,
    AgentActivityInputBinding,
    AgentActivityRequest,
    AgentActivityRequestArtifactStore,
    AgentActivityRequestError,
)
from src.orchestration.artifact_broker import (
    ArtifactDescriptor,
    ArtifactGrantBroker,
    ArtifactGrantConsumed,
    ArtifactGrantDenied,
)
from src.orchestration.artifacts import (
    ArtifactKind,
    ArtifactSensitivity,
    LocalArtifactStore,
)
from src.orchestration.scheduler import (
    ActivityReceipt,
    DurableScheduler,
    RunInputReceipt,
)
from src.orchestration.store import DurableRunStore
from src.orchestration.worker_security import WorkerAuthorization
from src.orchestration.workflow import compile_workflow


class Clock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class IdFactory:
    def __init__(self) -> None:
        self.value = 0

    def __call__(self, prefix: str) -> str:
        self.value += 1
        return f"{prefix}-agent-request-{self.value}"


class AuthorizationVerifier:
    def verify(
        self,
        authorization: WorkerAuthorization,
        *,
        now: float,
    ) -> bool:
        return (
            authorization.authorization_id == "trusted-auth"
            and now < authorization.expires_at
        )


class AgentRequestFixture:
    def __init__(
        self,
        root: Path,
        *,
        input_sensitivity: ArtifactSensitivity = (
            ArtifactSensitivity.SENSITIVE
        ),
    ) -> None:
        self.clock = Clock()
        self.input_sensitivity = input_sensitivity
        self.artifacts = LocalArtifactStore(root / "artifacts")
        self.store = DurableRunStore(root / "orchestration.sqlite3")
        self.workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": "agent-request-test",
                "version": 1,
                "nodes": [
                    {
                        "id": "research",
                        "kind": "agent",
                        "config": {
                            "agent": "main",
                            "task": "task-secret-4f191",
                            "context": "context-secret-5c282",
                            "expected_output": "bounded report",
                        },
                        "input_mapping": {
                            "evidence": {"source": "run_input"}
                        },
                        "effect_class": "non_idempotent_write",
                    }
                ],
            }
        )
        self.scheduler = DurableScheduler(
            self.store,
            self.workflow,
            clock=self.clock,
            id_factory=IdFactory(),
            result_writer=self._result_writer,
            input_writer=self._input_writer,
            artifact_verifier=self.artifacts.verify,
        )

    def _input_writer(self, run_id: str, value) -> RunInputReceipt:
        ref = self.artifacts.put_json(
            value,
            kind=ArtifactKind.GENERIC,
            sensitivity=self.input_sensitivity,
            producer_run_id=run_id,
        )
        return RunInputReceipt((ref,))

    def _result_writer(self, claim, value) -> ActivityReceipt:
        ref = self.artifacts.put_json(
            value,
            kind=ArtifactKind.MODEL_RESPONSE,
            sensitivity=ArtifactSensitivity.SENSITIVE,
            producer_run_id=claim.run_id,
            producer_node_id=claim.node_id,
            producer_attempt_id=claim.attempt_id,
        )
        return ActivityReceipt((ref,))

    def candidate(self, run_id: str = "run-1"):
        self.scheduler.create_run(
            run_id,
            input={"evidence": f"input-for-{run_id}"},
        )
        self.scheduler.reconcile(run_id)
        candidate = self.scheduler.prepare_next_admission(
            run_id,
            "worker-1",
        )
        assert candidate is not None
        return candidate


def _authorization(
    candidate,
    *,
    maximum_sensitivity: ArtifactSensitivity,
) -> WorkerAuthorization:
    return WorkerAuthorization(
        authorization_id="trusted-auth",
        worker_id="worker-1",
        tenant_id="tenant-1",
        pool_id="pool-1",
        run_id=candidate.claim.run_id,
        node_id=candidate.claim.node_id,
        attempt_id=candidate.claim.attempt_id,
        action_digest="a" * 64,
        identity_binding_digest="b" * 64,
        transport_binding_digest="c" * 64,
        rule_id="agent-rule",
        maximum_artifact_sensitivity=maximum_sensitivity,
        issued_at=90.0,
        expires_at=200.0,
    )


class AgentActivityRequestContractTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.fixture = AgentRequestFixture(self.root)

    def test_candidate_round_trip_is_canonical_and_path_free(self) -> None:
        candidate = self.fixture.candidate()

        request = AgentActivityRequest.from_candidate(candidate)
        restored = AgentActivityRequest.from_bytes(request.to_bytes())

        self.assertEqual(restored, request)
        self.assertEqual(request.task, "task-secret-4f191")
        self.assertEqual(request.context, "context-secret-5c282")
        self.assertEqual(request.request_digest, candidate.claim.request_hash)
        self.assertEqual(
            request.definition_digest,
            self.fixture.workflow.definition_digest,
        )
        other_worker = replace(
            candidate,
            claim=replace(candidate.claim, worker_id="worker-2"),
        )
        self.assertNotEqual(
            candidate.candidate_digest,
            other_worker.candidate_digest,
        )
        self.assertEqual(
            request,
            AgentActivityRequest.from_candidate(other_worker),
        )
        exact_task = "\n  preserve exact task whitespace  \n"
        exact_config = dict(candidate.claim.config)
        exact_config["task"] = exact_task
        exact_candidate = replace(
            candidate,
            claim=replace(candidate.claim, config=exact_config),
        )
        exact_request = AgentActivityRequest.from_candidate(
            exact_candidate
        )
        self.assertEqual(exact_request.task, exact_task)
        self.assertEqual(
            exact_request.config_digest,
            hashlib.sha256(
                json.dumps(
                    exact_config,
                    ensure_ascii=False,
                    allow_nan=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest(),
        )
        self.assertEqual(len(request.input_bindings), 1)
        descriptor = request.input_bindings[0].artifacts[0].to_dict()
        self.assertNotIn("uri", descriptor)
        self.assertNotIn("producer_run_id", descriptor)
        self.assertNotIn("task-secret-4f191", repr(request))
        self.assertNotIn("context-secret-5c282", repr(request))
        self.assertEqual(
            request.artifact_digest,
            hashlib.sha256(request.to_bytes()).hexdigest(),
        )
        expected_config_digest = hashlib.sha256(
            json.dumps(
                dict(candidate.claim.config),
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        self.assertEqual(request.config_digest, expected_config_digest)
        request.validate_runtime_binding(
            run_id=candidate.claim.run_id,
            node_id=candidate.claim.node_id,
            attempt_id=candidate.claim.attempt_id,
            attempt_number=candidate.claim.attempt_number,
            activity_kind="agent",
            activity_request_digest=candidate.claim.request_hash,
            activity_name=candidate.claim.config["agent"],
            config_digest=expected_config_digest,
        )
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "runtime_binding_mismatch",
        ):
            request.validate_runtime_binding(
                run_id=candidate.claim.run_id,
                node_id=candidate.claim.node_id,
                attempt_id=candidate.claim.attempt_id,
                attempt_number=candidate.claim.attempt_number,
                activity_kind="agent",
                activity_request_digest="0" * 64,
                activity_name=candidate.claim.config["agent"],
                config_digest=expected_config_digest,
            )

    def test_exact_schema_noncanonical_and_bool_version_fail_closed(
        self,
    ) -> None:
        request = AgentActivityRequest.from_candidate(
            self.fixture.candidate()
        )
        payload = request.to_dict()
        payload["unknown"] = "not allowed"
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "invalid_agent_request_schema",
        ):
            AgentActivityRequest.from_dict(payload)

        bool_version = request.to_dict()
        bool_version["schema_version"] = True
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "unsupported_agent_request_schema",
        ):
            AgentActivityRequest.from_dict(bool_version)

        pretty = json.dumps(
            request.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
        ).encode()
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "noncanonical_agent_request_payload",
        ):
            AgentActivityRequest.from_bytes(pretty)

        malformed = b'{"task":"parse-secret-91f4",'
        with self.assertRaises(AgentActivityRequestError) as raised:
            AgentActivityRequest.from_bytes(malformed)
        self.assertIsNone(raised.exception.__cause__)
        self.assertNotIn("parse-secret-91f4", str(raised.exception))

        invalid_unicode = request.to_dict()
        invalid_unicode["task"] = "\ud800"
        encoded = json.dumps(
            invalid_unicode,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        with self.assertRaises(AgentActivityRequestError) as raised:
            AgentActivityRequest.from_bytes(encoded)
        self.assertIsNone(raised.exception.__cause__)

    def test_empty_oversized_and_control_instruction_are_rejected(
        self,
    ) -> None:
        candidate = self.fixture.candidate()
        base = AgentActivityRequest.from_candidate(candidate)
        values = base.to_dict()
        values.update(
            {
                "task": None,
                "context": None,
                "input_bindings": [],
            }
        )
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "no_instruction",
        ):
            AgentActivityRequest.from_dict(values)

        values = base.to_dict()
        values["task"] = "x" * (MAX_AGENT_ACTIVITY_REQUEST_BYTES + 1)
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "invalid_task",
        ):
            AgentActivityRequest.from_dict(values)

        values = base.to_dict()
        values["task"] = "unsafe\u0000instruction"
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "invalid_task",
        ):
            AgentActivityRequest.from_dict(values)

        descriptor = base.input_bindings[0].artifacts[0]
        too_many = tuple(
            AgentActivityInputBinding(
                name=f"input{index:02d}",
                artifacts=(descriptor,),
            )
            for index in range(MAX_AGENT_ACTIVITY_INPUT_REFS + 1)
        )
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "invalid_input_bindings",
        ):
            replace(base, input_bindings=too_many)

    def test_candidate_authority_and_input_mismatch_are_rejected(
        self,
    ) -> None:
        candidate = self.fixture.candidate()
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "candidate_binding_mismatch",
        ):
            AgentActivityRequest.from_candidate(
                replace(
                    candidate,
                    claim=replace(
                        candidate.claim,
                        claim_token="live-claim-token",
                    ),
                )
            )

        corrupted_candidate = self.fixture.candidate("run-residual")
        object.__setattr__(
            corrupted_candidate.attempt,
            "worker_id",
            "residual-worker",
        )
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "candidate_binding_mismatch",
        ):
            AgentActivityRequest.from_candidate(
                corrupted_candidate
            )

        malformed = replace(
            candidate,
            claim=replace(
                candidate.claim,
                input_artifact_bindings=(
                    ("evidence", ("raw-secret-ref-7c412",)),
                ),
            ),
        )
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "invalid_agent_candidate_inputs",
        ) as raised:
            AgentActivityRequest.from_candidate(malformed)
        self.assertIsNone(raised.exception.__cause__)
        self.assertNotIn("raw-secret-ref-7c412", str(raised.exception))

        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "candidate_input_mismatch",
        ):
            AgentActivityRequest.from_candidate(
                replace(
                    candidate,
                    claim=replace(
                        candidate.claim,
                        input_artifact_refs=(),
                    ),
                )
            )


class AgentActivityRequestArtifactTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.fixture = AgentRequestFixture(self.root)
        self.request_store = AgentActivityRequestArtifactStore(
            self.fixture.artifacts
        )

    def test_stage_is_deterministic_sensitive_and_metadata_safe(self) -> None:
        candidate = self.fixture.candidate()

        first = self.request_store.stage(candidate)
        second = self.request_store.stage(candidate)
        loaded = self.request_store.load(
            first,
            expected_candidate=candidate,
        )

        self.assertEqual(first, second)
        self.assertEqual(first.kind, ArtifactKind.AGENT_REQUEST)
        self.assertEqual(
            first.media_type,
            AGENT_ACTIVITY_REQUEST_MEDIA_TYPE,
        )
        self.assertEqual(
            first.sensitivity,
            ArtifactSensitivity.SENSITIVE,
        )
        self.assertEqual(first.metadata, {"schema": "agent_activity_request_v1"})
        self.assertEqual(first.sha256, loaded.artifact_digest)
        ref_json = json.dumps(first.to_dict(), sort_keys=True)
        self.assertNotIn("task-secret-4f191", ref_json)
        self.assertNotIn("context-secret-5c282", ref_json)

    def test_concurrent_stage_converges_to_one_artifact(self) -> None:
        candidate = self.fixture.candidate()

        with ThreadPoolExecutor(max_workers=8) as executor:
            refs = list(
                executor.map(
                    lambda _index: self.request_store.stage(candidate),
                    range(16),
                )
            )

        self.assertEqual(
            {ref.artifact_id for ref in refs},
            {refs[0].artifact_id},
        )
        self.assertEqual(
            {ref.sha256 for ref in refs},
            {refs[0].sha256},
        )
        self.assertEqual({ref.uri for ref in refs}, {refs[0].uri})
        self.assertTrue(
            all(self.fixture.artifacts.verify(ref) for ref in refs)
        )
        stored_files = [
            path
            for path in self.fixture.artifacts.root.rglob("*")
            if path.is_file()
            and not path.name.startswith(".")
            and path.name == refs[0].sha256
        ]
        self.assertEqual(len(stored_files), 1)

    def test_retry_recovers_atomic_install_after_lost_response(
        self,
    ) -> None:
        candidate = self.fixture.candidate()
        interrupt_once = True

        def lose_response(stage: str, _path: Path) -> None:
            nonlocal interrupt_once
            if stage == "after_replace" and interrupt_once:
                interrupt_once = False
                raise OSError("injected after atomic install")

        artifact_store = LocalArtifactStore(
            self.root / "fault-artifacts",
            fault_hook=lose_response,
        )
        request_store = AgentActivityRequestArtifactStore(
            artifact_store
        )
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "artifact_write_failed",
        ) as raised:
            request_store.stage(candidate)
        self.assertIsNone(raised.exception.__cause__)
        self.assertNotIn("injected", str(raised.exception))

        recovered = request_store.stage(candidate)
        loaded = request_store.load(
            recovered,
            expected_candidate=candidate,
        )
        self.assertEqual(
            loaded,
            AgentActivityRequest.from_candidate(candidate),
        )

    def test_wrong_kind_corruption_and_candidate_swap_fail_closed(
        self,
    ) -> None:
        candidate = self.fixture.candidate()
        ref = self.request_store.stage(candidate)
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "invalid_agent_request_artifact",
        ):
            self.request_store.load(
                replace(ref, kind=ArtifactKind.GENERIC)
            )

        other = self.fixture.candidate("run-2")
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "candidate_mismatch",
        ):
            self.request_store.load(
                ref,
                expected_candidate=other,
            )

        path = self.fixture.artifacts.root / ref.uri
        path.write_bytes(b"corrupt")
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "verify_failed",
        ):
            self.request_store.load(ref)

    def test_broker_delivers_once_without_store_path(self) -> None:
        candidate = self.fixture.candidate()
        ref = self.request_store.stage(candidate)
        authorization = _authorization(
            candidate,
            maximum_sensitivity=ArtifactSensitivity.SENSITIVE,
        )
        broker = ArtifactGrantBroker(
            self.fixture.artifacts,
            authorization_verifier=AuthorizationVerifier(),
            clock=self.fixture.clock,
        )

        grant = broker.issue_read_grant(
            authorization,
            tenant_id="tenant-1",
            run_id=candidate.claim.run_id,
            attempt_id=candidate.claim.attempt_id,
            ref=ref,
        )
        context_grant = broker.issue_read_grant(
            authorization,
            tenant_id="tenant-1",
            run_id=candidate.claim.run_id,
            attempt_id=candidate.claim.attempt_id,
            ref=candidate.claim.input_artifact_refs[0],
        )
        payload = broker.redeem_read_grant(grant, authorization)
        request = AgentActivityRequest.from_bytes(payload.content)

        self.assertEqual(
            grant.descriptor.kind,
            ArtifactKind.AGENT_REQUEST.value,
        )
        self.assertNotIn("uri", grant.to_wire_dict()["descriptor"])
        self.assertEqual(
            request,
            AgentActivityRequest.from_candidate(candidate),
        )
        request.validate_grant_descriptors(
            (grant.descriptor, context_grant.descriptor)
        )
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "grant_binding_mismatch",
        ):
            request.validate_grant_descriptors((grant.descriptor,))
        with self.assertRaisesRegex(
            AgentActivityRequestError,
            "grant_binding_mismatch",
        ):
            request.validate_grant_descriptors([grant.descriptor])  # type: ignore[arg-type]
        with self.assertRaises(ArtifactGrantConsumed):
            broker.redeem_read_grant(grant, authorization)

    def test_broker_requires_explicit_sensitive_authority(self) -> None:
        candidate = self.fixture.candidate()
        ref = self.request_store.stage(candidate)
        authorization = _authorization(
            candidate,
            maximum_sensitivity=ArtifactSensitivity.INTERNAL,
        )
        broker = ArtifactGrantBroker(
            self.fixture.artifacts,
            authorization_verifier=AuthorizationVerifier(),
            clock=self.fixture.clock,
        )

        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "artifact_sensitivity_exceeds_policy",
        ):
            broker.issue_read_grant(
                authorization,
                tenant_id="tenant-1",
                run_id=candidate.claim.run_id,
                attempt_id=candidate.claim.attempt_id,
                ref=ref,
            )
    def test_request_inherits_secret_context_classification(self) -> None:
        secret_fixture = AgentRequestFixture(
            self.root / "secret-fixture",
            input_sensitivity=ArtifactSensitivity.SECRET,
        )
        candidate = secret_fixture.candidate("secret-run")
        request_store = AgentActivityRequestArtifactStore(
            secret_fixture.artifacts
        )

        ref = request_store.stage(candidate)
        request = request_store.load(
            ref,
            expected_candidate=candidate,
        )

        self.assertEqual(
            request.artifact_sensitivity,
            ArtifactSensitivity.SECRET,
        )
        self.assertEqual(ref.sensitivity, ArtifactSensitivity.SECRET)
        broker = ArtifactGrantBroker(
            secret_fixture.artifacts,
            authorization_verifier=AuthorizationVerifier(),
            clock=secret_fixture.clock,
        )
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "artifact_sensitivity_exceeds_policy",
        ):
            broker.issue_read_grant(
                _authorization(
                    candidate,
                    maximum_sensitivity=ArtifactSensitivity.SENSITIVE,
                ),
                tenant_id="tenant-1",
                run_id=candidate.claim.run_id,
                attempt_id=candidate.claim.attempt_id,
                ref=ref,
            )
        secret_authorization = _authorization(
            candidate,
            maximum_sensitivity=ArtifactSensitivity.SECRET,
        )
        with self.assertRaisesRegex(
            ArtifactGrantDenied,
            "unencrypted_secret_artifact",
        ):
            broker.issue_read_grant(
                secret_authorization,
                tenant_id="tenant-1",
                run_id=candidate.claim.run_id,
                attempt_id=candidate.claim.attempt_id,
                ref=ref,
            )
        request.validate_grant_descriptors(
            (
                ArtifactDescriptor.from_ref(ref),
                ArtifactDescriptor.from_ref(
                    candidate.claim.input_artifact_refs[0]
                ),
            )
        )


if __name__ == "__main__":
    unittest.main()
