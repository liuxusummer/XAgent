from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from src.orchestration.agent_execution_manifest import (
    AGENT_EXECUTION_MANIFEST_MEDIA_TYPE,
    AgentActivityExecutionManifest,
    AgentExecutionManifestArtifactStore,
    AgentExecutionManifestError,
    AgentToolReceiptBinding,
    agent_tool_operation_key,
    canonical_tool_call_digest,
)
from src.orchestration.agent_request import (
    AGENT_ACTIVITY_REQUEST_MEDIA_TYPE,
    AgentActivityInputBinding,
    AgentActivityRequest,
)
from src.orchestration.artifact_broker import ArtifactDescriptor
from src.orchestration.agent_receipt import (
    AgentActivityReceipt,
    AgentActivityReceiptError,
    AgentActivityVerification,
)
from src.orchestration.artifacts import (
    ArtifactKind,
    ArtifactSensitivity,
    LocalArtifactStore,
)
from src.orchestration.executor import (
    ToolReceipt,
    ToolReceiptVerification,
)
from src.orchestration.models import AttemptStatus
from src.orchestration.policy import EffectClass
from src.orchestration.store import (
    _validate_agent_execution_manifest_ref,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _tool_receipt(index: int) -> ToolReceipt:
    policy_digest = _digest("policy-v1")
    operation_key = agent_tool_operation_key(
        run_id="run-1",
        node_id="agent-node",
        attempt_id="agent-attempt",
        request_digest=_digest("request"),
        sequence=index,
    )
    operation_digest = hashlib.sha256(
        operation_key.encode("utf-8")
    ).hexdigest()
    return ToolReceipt(
        run_id="run-1",
        node_id=f"tool-node-{index}",
        attempt_id=f"tool-attempt-{index}",
        tool_name=f"tool_{index}",
        effect_class=EffectClass.READ_ONLY,
        attempt_status=AttemptStatus.FAILED,
        args_digest=_digest(f"args-{index}"),
        action_digest=_digest(f"action-{index}"),
        execution_binding_digest=_digest(f"execution-{index}"),
        operation_key_digest=operation_digest,
        idempotency_key_digest=operation_digest,
        policy_version=f"sha256:{policy_digest}",
        policy_digest=policy_digest,
        profile_id="container",
        profile_digest=_digest("profile"),
        verification=ToolReceiptVerification.UNVERIFIED,
        sandbox_receipt=None,
        sandbox_receipt_absence_reason="dispatch_denied_before_backend",
        error_code="no_qualified_backend",
    )


def _manifest(
    receipts: tuple[ToolReceipt, ...] | None = None,
    **overrides,
) -> AgentActivityExecutionManifest:
    receipts = receipts or (_tool_receipt(1), _tool_receipt(2))
    values = {
        "run_id": "run-1",
        "node_id": "agent-node",
        "attempt_id": "agent-attempt",
        "request_digest": _digest("request"),
        "request_artifact_digest": _digest("request-artifact"),
        "definition_digest": _digest("definition"),
        "artifact_sensitivity": ArtifactSensitivity.SENSITIVE,
        "exit_reason": "CURRENT_TASK_DONE",
        "turns": 2,
        "observed_tool_results": len(receipts),
        "tool_receipts_complete": True,
        "tool_receipts": tuple(
            AgentToolReceiptBinding.from_receipt(
                receipt,
                sequence=index,
                turn=index,
            )
            for index, receipt in enumerate(receipts, start=1)
        ),
    }
    values.update(overrides)
    return AgentActivityExecutionManifest(**values)


def _agent_receipt(
    manifest: AgentActivityExecutionManifest,
    **overrides,
) -> AgentActivityReceipt:
    values = {
        "run_id": manifest.run_id,
        "node_id": manifest.node_id,
        "attempt_id": manifest.attempt_id,
        "activity_name": "main",
        "effect_class": EffectClass.NON_IDEMPOTENT_WRITE,
        "attempt_status": AttemptStatus.SUCCEEDED,
        "request_digest": manifest.request_digest,
        "result_digest": _digest("node-result"),
        "exit_reason": manifest.exit_reason,
        "turns": manifest.turns,
        "observed_tool_results": manifest.observed_tool_results,
        "result_artifact_digests": (manifest.manifest_digest,),
        "tool_receipt_digests": manifest.tool_receipt_digests,
        "internal_tool_receipts_complete": (
            manifest.tool_receipts_complete
        ),
        "execution_manifest_digest": manifest.manifest_digest,
        "verification": AgentActivityVerification.RUNTIME_OBSERVED,
    }
    values.update(overrides)
    return AgentActivityReceipt(**values)


class AgentExecutionManifestContractTests(unittest.TestCase):
    def test_child_operation_key_is_stable_and_parent_scoped(self) -> None:
        values = {
            "run_id": "run-1",
            "node_id": "agent-node",
            "attempt_id": "agent-attempt",
            "request_digest": _digest("request"),
            "sequence": 1,
        }
        first = agent_tool_operation_key(**values)
        second = agent_tool_operation_key(**values)

        self.assertEqual(first, second)
        self.assertRegex(first, r"^agent-tool:[0-9a-f]{64}$")
        self.assertNotEqual(
            first,
            agent_tool_operation_key(
                **{**values, "attempt_id": "other-attempt"}
            ),
        )
        self.assertNotEqual(
            first,
            agent_tool_operation_key(
                **{**values, "sequence": 2}
            ),
        )

    def test_round_trip_binds_ordered_tool_receipts_and_agent_receipt(
        self,
    ) -> None:
        receipts = (_tool_receipt(1), _tool_receipt(2))
        manifest = _manifest(receipts)
        restored = AgentActivityExecutionManifest.from_bytes(
            manifest.to_bytes()
        )
        agent_receipt = _agent_receipt(manifest)

        self.assertEqual(restored, manifest)
        self.assertEqual(
            restored.manifest_digest,
            manifest.manifest_digest,
        )
        restored.validate_tool_receipts(receipts)
        restored.validate_agent_receipt(agent_receipt)
        agent_receipt.validate_execution_manifest(restored)
        self.assertEqual(
            restored.tool_receipts[0].tool_call_digest,
            canonical_tool_call_digest(receipts[0]),
        )

        serialized = manifest.to_bytes().decode("utf-8")
        for forbidden in (
            "raw task secret",
            "tool arguments",
            "tool result",
            "sandbox_receipt",
            "claim_token",
        ):
            self.assertNotIn(forbidden, serialized)
        self.assertNotIn("raw task secret", repr(manifest))

    def test_order_duplicates_and_receipt_substitution_fail_closed(
        self,
    ) -> None:
        first = _tool_receipt(1)
        second = _tool_receipt(2)
        manifest = _manifest((first, second))

        with self.assertRaisesRegex(
            AgentExecutionManifestError,
            "binding_mismatch",
        ):
            manifest.validate_tool_receipts((second, first))
        with self.assertRaisesRegex(
            AgentExecutionManifestError,
            "invalid_ordered",
        ):
            replace(
                manifest,
                tool_receipts=(
                    manifest.tool_receipts[0],
                    replace(
                        manifest.tool_receipts[0],
                        sequence=2,
                    ),
                ),
            )
        with self.assertRaisesRegex(
            AgentExecutionManifestError,
            "out_of_range",
        ):
            replace(
                manifest,
                tool_receipts=(
                    replace(
                        manifest.tool_receipts[0],
                        turn=3,
                    ),
                    replace(
                        manifest.tool_receipts[1],
                        turn=3,
                    ),
                ),
            )
        with self.assertRaisesRegex(
            AgentExecutionManifestError,
            "set_mismatch",
        ):
            manifest.validate_tool_receipts((first,))

    def test_completeness_requires_exact_observed_coverage(self) -> None:
        receipt = _tool_receipt(1)
        binding = AgentToolReceiptBinding.from_receipt(
            receipt,
            sequence=1,
            turn=1,
        )
        with self.assertRaisesRegex(
            AgentExecutionManifestError,
            "exact_coverage",
        ):
            _manifest(
                (receipt,),
                observed_tool_results=2,
                tool_receipts=(binding,),
            )
        with self.assertRaisesRegex(
            AgentExecutionManifestError,
            "exceed_observed",
        ):
            _manifest(
                (receipt,),
                observed_tool_results=0,
                tool_receipts_complete=False,
                tool_receipts=(binding,),
            )
        partial = _manifest(
            (receipt,),
            observed_tool_results=2,
            tool_receipts_complete=False,
            tool_receipts=(binding,),
        )
        self.assertFalse(partial.tool_receipts_complete)

    def test_v2_agent_receipt_requires_one_manifest_result_artifact(
        self,
    ) -> None:
        manifest = _manifest()
        receipt = _agent_receipt(manifest)
        restored = AgentActivityReceipt.from_dict(receipt.to_dict())

        self.assertEqual(restored, receipt)
        self.assertEqual(restored.schema_version, 2)
        self.assertTrue(
            restored.has_manifest_bound_tool_receipt_lineage
        )
        with self.assertRaisesRegex(
            AgentActivityReceiptError,
            "require an execution manifest",
        ):
            replace(receipt, execution_manifest_digest=None)
        with self.assertRaisesRegex(
            AgentActivityReceiptError,
            "one result Artifact",
        ):
            replace(receipt, result_artifact_digests=())
        with self.assertRaisesRegex(
            AgentActivityReceiptError,
            "does not match",
        ):
            receipt.validate_execution_manifest(
                replace(manifest, exit_reason="ERROR")
            )

    def test_v1_receipt_round_trip_remains_exactly_compatible(self) -> None:
        payload = {
            "schema_version": 1,
            "kind": "agent_activity_receipt",
            "run_id": "run-1",
            "node_id": "agent-node",
            "attempt_id": "agent-attempt",
            "activity_name": "main",
            "effect_class": "read_only",
            "attempt_status": "succeeded",
            "request_digest": _digest("request"),
            "result_digest": _digest("result"),
            "exit_reason": "CURRENT_TASK_DONE",
            "turns": 1,
            "observed_tool_results": 1,
            "result_artifact_digests": [],
            "tool_receipt_digests": [_digest("legacy-tool")],
            "internal_tool_receipts_complete": True,
            "verification": "runtime_observed",
        }

        receipt = AgentActivityReceipt.from_dict(payload)

        self.assertEqual(receipt.to_dict(), payload)
        self.assertIsNone(receipt.execution_manifest_digest)
        self.assertFalse(
            receipt.has_manifest_bound_tool_receipt_lineage
        )
        payload["execution_manifest_digest"] = None
        with self.assertRaisesRegex(
            AgentActivityReceiptError,
            "unknown or missing",
        ):
            AgentActivityReceipt.from_dict(payload)

    def test_manifest_payload_is_exact_and_canonical(self) -> None:
        manifest = _manifest()
        payload = manifest.to_dict()
        payload["task"] = "raw task secret"
        with self.assertRaisesRegex(
            AgentExecutionManifestError,
            "schema",
        ):
            AgentActivityExecutionManifest.from_dict(payload)
        noncanonical = json.dumps(
            manifest.to_dict(),
            ensure_ascii=False,
        ).encode("utf-8")
        with self.assertRaisesRegex(
            AgentExecutionManifestError,
            "noncanonical",
        ):
            AgentActivityExecutionManifest.from_bytes(noncanonical)
        with self.assertRaisesRegex(
            AgentExecutionManifestError,
            "sensitivity",
        ):
            replace(
                manifest,
                artifact_sensitivity=ArtifactSensitivity.INTERNAL,
            )

    def test_parent_operation_and_request_artifact_bindings_are_exact(
        self,
    ) -> None:
        receipt = _tool_receipt(1)
        binding = AgentToolReceiptBinding.from_receipt(
            receipt,
            sequence=1,
            turn=1,
        )
        with self.assertRaisesRegex(
            AgentExecutionManifestError,
            "parent_binding_mismatch",
        ):
            _manifest(
                (receipt,),
                request_digest=_digest("other-request"),
                observed_tool_results=1,
                tool_receipts=(binding,),
            )

        with tempfile.TemporaryDirectory() as raw:
            store = LocalArtifactStore(Path(raw))
            context_ref = store.put_bytes(
                b"sensitive context",
                kind=ArtifactKind.GENERIC,
                sensitivity=ArtifactSensitivity.SECRET,
                producer_run_id="run-1",
            )
            request = AgentActivityRequest(
                run_id="run-1",
                node_id="agent-node",
                attempt_id="agent-attempt",
                attempt_number=1,
                agent_name="main",
                request_digest=_digest("request"),
                definition_digest=_digest("definition"),
                task="sensitive task",
                input_bindings=(
                    AgentActivityInputBinding(
                        name="evidence",
                        artifacts=(
                            ArtifactDescriptor.from_ref(context_ref),
                        ),
                    ),
                ),
            )
            request_ref = store.put_bytes(
                request.to_bytes(),
                media_type=AGENT_ACTIVITY_REQUEST_MEDIA_TYPE,
                kind=ArtifactKind.AGENT_REQUEST,
                sensitivity=ArtifactSensitivity.SECRET,
                producer_run_id=request.run_id,
                producer_node_id=request.node_id,
                producer_attempt_id=request.attempt_id,
                metadata={"schema": "agent_activity_request_v1"},
            )
            manifest = _manifest(
                request_artifact_digest=request.artifact_digest,
                artifact_sensitivity=ArtifactSensitivity.SECRET,
            )

            manifest.validate_request(request, request_ref)
            with self.assertRaisesRegex(
                AgentExecutionManifestError,
                "request_manifest_binding_mismatch",
            ):
                replace(
                    manifest,
                    artifact_sensitivity=ArtifactSensitivity.SENSITIVE,
                ).validate_request(request, request_ref)


class AgentExecutionManifestArtifactTests(unittest.TestCase):
    def test_stage_and_load_preserve_sensitive_typed_binding(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            store = LocalArtifactStore(Path(raw))
            artifacts = AgentExecutionManifestArtifactStore(store)
            receipts = (_tool_receipt(1), _tool_receipt(2))
            manifest = _manifest(receipts)
            agent_receipt = _agent_receipt(manifest)

            first = artifacts.stage(manifest)
            second = artifacts.stage(manifest)
            restored = artifacts.load(
                first,
                expected_receipt=agent_receipt,
                tool_receipts=receipts,
            )

            self.assertEqual(restored, manifest)
            self.assertEqual(first.sha256, manifest.manifest_digest)
            self.assertEqual(first.artifact_id, second.artifact_id)
            self.assertEqual(
                first.kind,
                ArtifactKind.AGENT_EXECUTION_MANIFEST,
            )
            self.assertEqual(
                first.media_type,
                AGENT_EXECUTION_MANIFEST_MEDIA_TYPE,
            )
            self.assertEqual(
                first.sensitivity,
                ArtifactSensitivity.SENSITIVE,
            )
            self.assertEqual(
                dict(first.metadata),
                {"schema": "agent_execution_manifest_v1"},
            )
            self.assertEqual(
                (
                    first.producer_run_id,
                    first.producer_node_id,
                    first.producer_attempt_id,
                ),
                (
                    manifest.run_id,
                    manifest.node_id,
                    manifest.attempt_id,
                ),
            )

    def test_manifest_inherits_secret_request_classification(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            artifacts = AgentExecutionManifestArtifactStore(
                LocalArtifactStore(Path(raw))
            )
            manifest = _manifest(
                artifact_sensitivity=ArtifactSensitivity.SECRET,
            )

            ref = artifacts.stage(manifest)

            self.assertEqual(
                ref.sensitivity,
                ArtifactSensitivity.SECRET,
            )
            self.assertEqual(artifacts.load(ref), manifest)

    def test_wrong_artifact_kind_and_producer_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            artifacts = AgentExecutionManifestArtifactStore(
                LocalArtifactStore(Path(raw))
            )
            manifest = _manifest()
            ref = artifacts.stage(manifest)

            with self.assertRaisesRegex(
                AgentExecutionManifestError,
                "invalid_agent_execution_manifest_artifact",
            ):
                artifacts.validate_ref(
                    replace(ref, kind=ArtifactKind.GENERIC)
                )
            with self.assertRaisesRegex(
                AgentExecutionManifestError,
                "invalid_agent_execution_manifest_artifact",
            ):
                artifacts.validate_ref(replace(ref, size=0))
            with self.assertRaisesRegex(
                AgentExecutionManifestError,
                "artifact_binding_mismatch",
            ):
                artifacts.validate_ref(
                    replace(ref, producer_attempt_id="other-attempt"),
                    manifest=manifest,
                )

    def test_store_binding_rejects_wrong_or_duplicate_manifest_refs(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            artifacts = AgentExecutionManifestArtifactStore(
                LocalArtifactStore(Path(raw))
            )
            manifest = _manifest()
            ref = artifacts.stage(manifest)
            receipt = _agent_receipt(manifest)

            _validate_agent_execution_manifest_ref(
                {"artifact_refs": [ref.to_dict()]},
                receipt,
            )
            with self.assertRaisesRegex(ValueError, "wrong producer"):
                _validate_agent_execution_manifest_ref(
                    {
                        "artifact_refs": [
                            replace(
                                ref,
                                producer_attempt_id="other-attempt",
                            ).to_dict()
                        ]
                    },
                    receipt,
                )
            with self.assertRaisesRegex(ValueError, "must be unique"):
                _validate_agent_execution_manifest_ref(
                    {
                        "artifact_refs": [
                            ref.to_dict(),
                            ref.to_dict(),
                        ]
                    },
                    receipt,
                )
            with self.assertRaisesRegex(
                AgentExecutionManifestError,
                "invalid_agent_execution_manifest_artifact",
            ):
                _validate_agent_execution_manifest_ref(
                    {
                        "artifact_refs": [
                            replace(
                                ref,
                                kind=ArtifactKind.GENERIC,
                            ).to_dict()
                        ]
                    },
                    receipt,
                )

    def test_failed_verification_never_reads_manifest_bytes(self) -> None:
        class VerifyFailStore(LocalArtifactStore):
            def __init__(self, root: Path) -> None:
                super().__init__(root)
                self.read_calls = 0

            def verify(self, ref) -> bool:
                del ref
                return False

            def read(self, ref) -> bytes:
                self.read_calls += 1
                return super().read(ref)

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            manifest = _manifest()
            ref = AgentExecutionManifestArtifactStore(
                LocalArtifactStore(root)
            ).stage(manifest)
            failing_store = VerifyFailStore(root)
            artifacts = AgentExecutionManifestArtifactStore(
                failing_store
            )

            with self.assertRaisesRegex(
                AgentExecutionManifestError,
                "verify_failed",
            ):
                artifacts.load(ref)
            self.assertEqual(failing_store.read_calls, 0)


if __name__ == "__main__":
    unittest.main()
