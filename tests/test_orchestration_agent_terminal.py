from __future__ import annotations

import hashlib
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

from src.orchestration.agent_execution_evidence import (
    AgentExecutionEvidenceCollector,
)
from src.orchestration.agent_request import (
    AGENT_ACTIVITY_REQUEST_MEDIA_TYPE,
    AgentActivityRequest,
)
from src.orchestration.agent_terminal import (
    AgentTerminalCommitError,
    DurableAgentTerminalCommitter,
)
from src.orchestration.artifacts import (
    ArtifactKind,
    ArtifactSensitivity,
    canonical_json_bytes,
)
from src.orchestration.artifacts_gc import (
    LocalArtifactGarbageCollector,
)
from src.orchestration.models import AttemptStatus, RunStatus
from src.orchestration.provider_access import (
    ProviderInvocationCompletionMode,
    ProviderInvocationReceipt,
)
from src.orchestration.store import InvalidStateTransition
from src.orchestration.tool_receipt_artifact import (
    ToolReceiptArtifactError,
    ToolReceiptArtifactStore,
)
from tests.test_orchestration_agent_tool_authority import _Fixture


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


class _TerminalFixture:
    def __init__(
        self,
        root: Path,
        *,
        with_tool: bool = True,
        provider_sensitivity: ArtifactSensitivity = (
            ArtifactSensitivity.SENSITIVE
        ),
    ) -> None:
        self.runtime = _Fixture(root)
        self.claim = self.runtime.parent
        self.request = AgentActivityRequest(
            run_id=self.claim.run_id,
            node_id=self.claim.node_id,
            attempt_id=self.claim.attempt_id,
            attempt_number=self.claim.attempt_number,
            agent_name=self.claim.config["agent"],
            request_digest=self.claim.request_hash,
            definition_digest=self.runtime.workflow.definition_digest,
            task="perform the safe task",
        )
        self.request_ref = self.runtime.artifacts.put_bytes(
            self.request.to_bytes(),
            media_type=AGENT_ACTIVITY_REQUEST_MEDIA_TYPE,
            kind=ArtifactKind.AGENT_REQUEST,
            sensitivity=self.request.artifact_sensitivity,
            producer_run_id=self.claim.run_id,
            producer_node_id=self.claim.node_id,
            producer_attempt_id=self.claim.attempt_id,
            metadata={"schema": "agent_activity_request_v1"},
        )
        self.collector = AgentExecutionEvidenceCollector(
            run_id=self.claim.run_id,
            node_id=self.claim.node_id,
            attempt_id=self.claim.attempt_id,
            request_digest=self.claim.request_hash,
            request_artifact_digest=self.request_ref.sha256,
            definition_digest=self.runtime.workflow.definition_digest,
            request_sensitivity=self.request_ref.sensitivity,
        )
        self.provider_response_ref = self.runtime.artifacts.put_bytes(
            b"provider response",
            media_type="application/octet-stream",
            kind=ArtifactKind.MODEL_RESPONSE,
            sensitivity=provider_sensitivity,
            producer_run_id=self.claim.run_id,
            producer_node_id=self.claim.node_id,
            producer_attempt_id=self.claim.attempt_id,
            metadata={},
        )
        self.collector.provider_call_started(turn=1)
        self.collector.provider_call_finished(
            turn=1,
            receipt=self.provider_receipt(),
        )
        self.tool_receipt = None
        if with_tool:
            request = self.runtime.request(sequence=1, turn=1)
            _reservation, child_claim = self.runtime.start(request)
            _stored, receipt, _result_ref = self.runtime.complete(
                request,
                child_claim,
            )
            self.tool_receipt = receipt
            self.collector.tool_call_started(
                turn=1,
                tool_name=request.tool_name,
                tool_call_id="call-1",
            )
            self.collector.tool_call_finished(
                turn=1,
                tool_name=request.tool_name,
                tool_call_id="call-1",
                receipt=receipt,
            )

    def provider_receipt(self) -> ProviderInvocationReceipt:
        return ProviderInvocationReceipt(
            grant_id="provider-grant-1",
            run_id=self.claim.run_id,
            node_id=self.claim.node_id,
            attempt_id=self.claim.attempt_id,
            action_digest=_digest("agent-action"),
            authorization_digest=_digest("authorization"),
            request_digest=self.claim.request_hash,
            request_artifact_digest=self.request_ref.sha256,
            request_payload_digest=_digest("provider-payload-1"),
            invocation_index=1,
            route_id="primary-route",
            route_digest=_digest("primary-route"),
            grant_binding_digest=_digest("provider-grant-1"),
            response_digest=self.provider_response_ref.sha256,
            response_artifact_ref_digest=hashlib.sha256(
                canonical_json_bytes(
                    self.provider_response_ref.to_dict()
                )
            ).hexdigest(),
            response_sensitivity=self.provider_response_ref.sensitivity,
            completion_mode=ProviderInvocationCompletionMode.INVOKED,
        )

    def committer(self, *, fault_hook=None):
        return DurableAgentTerminalCommitter(
            self.runtime.scheduler,
            self.runtime.artifacts,
            clock=self.runtime.clock,
            fault_hook=fault_hook,
        )

    def commit(self, *, committer=None, result_artifact_refs=None):
        target = committer or self.committer()
        refs = (
            (self.provider_response_ref,)
            if result_artifact_refs is None
            else result_artifact_refs
        )
        return target.commit_success(
            self.claim,
            self.request_ref,
            self.collector,
            exit_reason="CURRENT_TASK_DONE",
            turns=1,
            result_artifact_refs=refs,
            metrics={"turns": 1},
        )

    def parent_terminal_events(self):
        return [
            event
            for event in self.runtime.store.list_events(self.claim.run_id)
            if event.attempt_id == self.claim.attempt_id
            and event.event_type == "attempt.succeeded"
        ]


class DurableAgentTerminalCommitterTests(unittest.TestCase):
    def test_success_atomically_binds_manifest_and_tool_receipt_artifacts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory))

            result = fixture.commit()

            attempt = fixture.runtime.store.get_attempt(
                fixture.claim.attempt_id
            )
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertIs(attempt.status, AttemptStatus.SUCCEEDED)
            self.assertEqual(
                fixture.runtime.store.get_agent_activity_receipt(
                    fixture.claim.run_id,
                    fixture.claim.attempt_id,
                ),
                result.receipt,
            )
            self.assertEqual(len(result.tool_receipt_refs), 1)
            self.assertIs(
                result.tool_receipt_refs[0].kind,
                ArtifactKind.TOOL_RECEIPT,
            )
            self.assertEqual(
                ToolReceiptArtifactStore(
                    fixture.runtime.artifacts
                ).load(result.tool_receipt_refs[0]),
                fixture.tool_receipt,
            )
            self.assertEqual(
                result.receipt.result_artifact_digests[-1],
                result.manifest_ref.sha256,
            )
            self.assertEqual(
                result.receipt.tool_receipt_digests,
                tuple(ref.sha256 for ref in result.tool_receipt_refs),
            )
            self.assertEqual(len(fixture.parent_terminal_events()), 1)
            run = fixture.runtime.store.get_run(fixture.claim.run_id)
            self.assertIsNotNone(run)
            assert run is not None
            self.assertIs(run.status, RunStatus.COMPLETED)

    def test_artifact_stage_crash_leaves_parent_running_and_retryable(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory))

            def fail_after_staging(stage: str) -> None:
                if stage == "agent_terminal.artifacts_staged":
                    raise SystemExit("simulated crash")

            with self.assertRaisesRegex(SystemExit, "simulated crash"):
                fixture.commit(
                    committer=fixture.committer(
                        fault_hook=fail_after_staging
                    )
                )

            attempt = fixture.runtime.store.get_attempt(
                fixture.claim.attempt_id
            )
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertIs(attempt.status, AttemptStatus.RUNNING)
            self.assertIsNone(
                fixture.runtime.store.get_agent_activity_receipt(
                    fixture.claim.run_id,
                    fixture.claim.attempt_id,
                )
            )
            recovered = fixture.commit()
            self.assertEqual(len(fixture.parent_terminal_events()), 1)
            self.assertEqual(
                fixture.runtime.store.get_agent_activity_receipt(
                    fixture.claim.run_id,
                    fixture.claim.attempt_id,
                ),
                recovered.receipt,
            )

    def test_response_loss_after_store_commit_replays_exact_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory))

            def lose_response(stage: str) -> None:
                if stage == "agent_terminal.store_committed":
                    raise SystemExit("response lost")

            with self.assertRaisesRegex(SystemExit, "response lost"):
                fixture.commit(
                    committer=fixture.committer(fault_hook=lose_response)
                )

            durable = fixture.runtime.store.get_agent_activity_receipt(
                fixture.claim.run_id,
                fixture.claim.attempt_id,
            )
            self.assertIsNotNone(durable)
            recovered = fixture.commit()
            self.assertTrue(recovered.replayed)
            self.assertEqual(recovered.receipt, durable)
            self.assertEqual(len(fixture.parent_terminal_events()), 1)

    def test_concurrent_duplicate_commits_create_one_terminal_event(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory))

            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(
                    pool.map(lambda _index: fixture.commit(), range(8))
                )

            self.assertEqual(
                {item.receipt.receipt_digest for item in results},
                {results[0].receipt.receipt_digest},
            )
            self.assertEqual(len(fixture.parent_terminal_events()), 1)

    def test_missing_provider_receipt_fails_before_terminal_acceptance(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory), with_tool=False)
            incomplete = AgentExecutionEvidenceCollector(
                run_id=fixture.claim.run_id,
                node_id=fixture.claim.node_id,
                attempt_id=fixture.claim.attempt_id,
                request_digest=fixture.claim.request_hash,
                request_artifact_digest=fixture.request_ref.sha256,
                definition_digest=fixture.runtime.workflow.definition_digest,
                request_sensitivity=fixture.request_ref.sensitivity,
            )
            incomplete.provider_call_started(turn=1)
            incomplete.provider_call_finished(turn=1, receipt=None)

            with self.assertRaisesRegex(
                AgentTerminalCommitError,
                "agent_terminal_evidence_incomplete",
            ):
                fixture.committer().commit_success(
                    fixture.claim,
                    fixture.request_ref,
                    incomplete,
                    exit_reason="CURRENT_TASK_DONE",
                    turns=1,
                )

            attempt = fixture.runtime.store.get_attempt(
                fixture.claim.attempt_id
            )
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertIs(attempt.status, AttemptStatus.RUNNING)
            self.assertEqual(fixture.parent_terminal_events(), [])

    def test_missing_or_substituted_provider_artifact_fails_closed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory), with_tool=False)
            with self.assertRaisesRegex(
                AgentTerminalCommitError,
                "agent_terminal_evidence_incomplete",
            ):
                fixture.commit(result_artifact_refs=())

            substitute = fixture.runtime.artifacts.put_bytes(
                b"different provider response",
                media_type="application/octet-stream",
                kind=ArtifactKind.MODEL_RESPONSE,
                sensitivity=ArtifactSensitivity.SENSITIVE,
                producer_run_id=fixture.claim.run_id,
                producer_node_id=fixture.claim.node_id,
                producer_attempt_id=fixture.claim.attempt_id,
                metadata={},
            )
            with self.assertRaisesRegex(
                AgentTerminalCommitError,
                "agent_terminal_evidence_incomplete",
            ):
                fixture.commit(result_artifact_refs=(substitute,))
            with self.assertRaisesRegex(
                AgentTerminalCommitError,
                "agent_terminal_evidence_incomplete",
            ):
                fixture.commit(
                    result_artifact_refs=(
                        fixture.provider_response_ref,
                        substitute,
                    )
                )

            attempt = fixture.runtime.store.get_attempt(
                fixture.claim.attempt_id
            )
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertIs(attempt.status, AttemptStatus.RUNNING)

    def test_request_ref_and_result_sensitivity_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory), with_tool=False)
            forged_request_ref = replace(
                fixture.request_ref,
                producer_attempt_id="other-attempt",
            )
            with self.assertRaisesRegex(
                AgentTerminalCommitError,
                "agent_terminal_request_invalid",
            ):
                fixture.committer().commit_success(
                    fixture.claim,
                    forged_request_ref,
                    fixture.collector,
                    exit_reason="CURRENT_TASK_DONE",
                    turns=1,
                )

        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory), with_tool=False)
            public_ref = fixture.runtime.artifacts.put_bytes(
                b"public result",
                kind=ArtifactKind.REPORT,
                sensitivity=ArtifactSensitivity.PUBLIC,
                producer_run_id=fixture.claim.run_id,
                producer_node_id=fixture.claim.node_id,
                producer_attempt_id=fixture.claim.attempt_id,
            )
            with self.assertRaisesRegex(
                AgentTerminalCommitError,
                "agent_terminal_artifact_invalid",
            ):
                fixture.commit(result_artifact_refs=(public_ref,))

            attempt = fixture.runtime.store.get_attempt(
                fixture.claim.attempt_id
            )
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertIs(attempt.status, AttemptStatus.RUNNING)

        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory), with_tool=False)
            foreign_ref = fixture.runtime.artifacts.put_bytes(
                b"foreign result",
                kind=ArtifactKind.REPORT,
                sensitivity=ArtifactSensitivity.SENSITIVE,
                producer_run_id="another-run",
                producer_node_id=fixture.claim.node_id,
                producer_attempt_id=fixture.claim.attempt_id,
            )
            with self.assertRaisesRegex(
                AgentTerminalCommitError,
                "agent_terminal_artifact_invalid",
            ):
                fixture.commit(result_artifact_refs=(foreign_ref,))

        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory), with_tool=False)
            unscoped_ref = fixture.runtime.artifacts.put_bytes(
                b"unscoped result",
                kind=ArtifactKind.REPORT,
                sensitivity=ArtifactSensitivity.SENSITIVE,
            )
            with self.assertRaisesRegex(
                AgentTerminalCommitError,
                "agent_terminal_artifact_invalid",
            ):
                fixture.commit(result_artifact_refs=(unscoped_ref,))

        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(
                Path(directory),
                with_tool=False,
                provider_sensitivity=ArtifactSensitivity.SECRET,
            )
            downgraded_ref = fixture.runtime.artifacts.put_bytes(
                b"downgraded final result",
                kind=ArtifactKind.REPORT,
                sensitivity=ArtifactSensitivity.SENSITIVE,
                producer_run_id=fixture.claim.run_id,
                producer_node_id=fixture.claim.node_id,
                producer_attempt_id=fixture.claim.attempt_id,
            )
            with self.assertRaisesRegex(
                AgentTerminalCommitError,
                "agent_terminal_artifact_invalid",
            ):
                fixture.commit(
                    result_artifact_refs=(
                        fixture.provider_response_ref,
                        downgraded_ref,
                    )
                )

    def test_store_transaction_crash_rolls_back_all_terminal_facts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory), with_tool=False)
            injected = False

            def fail_store(stage: str) -> None:
                nonlocal injected
                if stage == "complete.after_idempotency" and not injected:
                    injected = True
                    raise OSError("simulated Store crash")

            fixture.runtime.store._fault = fail_store
            with self.assertRaisesRegex(
                AgentTerminalCommitError,
                "agent_terminal_store_unavailable",
            ):
                fixture.commit()

            fixture.runtime.store._fault = lambda _stage: None
            attempt = fixture.runtime.store.get_attempt(
                fixture.claim.attempt_id
            )
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertIs(attempt.status, AttemptStatus.RUNNING)
            self.assertIsNone(
                fixture.runtime.store.get_agent_activity_receipt(
                    fixture.claim.run_id,
                    fixture.claim.attempt_id,
                )
            )
            self.assertEqual(fixture.parent_terminal_events(), [])

            recovered = fixture.commit()
            self.assertEqual(len(fixture.parent_terminal_events()), 1)
            self.assertEqual(
                fixture.runtime.store.get_agent_activity_receipt(
                    fixture.claim.run_id,
                    fixture.claim.attempt_id,
                ),
                recovered.receipt,
            )

    def test_inline_free_text_metrics_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory), with_tool=False)

            with self.assertRaisesRegex(
                AgentTerminalCommitError,
                "agent_terminal_result_invalid",
            ):
                fixture.committer().commit_success(
                    fixture.claim,
                    fixture.request_ref,
                    fixture.collector,
                    exit_reason="CURRENT_TASK_DONE",
                    turns=1,
                    result_artifact_refs=(
                        fixture.provider_response_ref,
                    ),
                    metrics={"summary": "raw model output must be an Artifact"},
                )

            attempt = fixture.runtime.store.get_attempt(
                fixture.claim.attempt_id
            )
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertIs(attempt.status, AttemptStatus.RUNNING)

    def test_gc_reachability_includes_terminal_evidence_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory))
            committed = fixture.commit()
            reachable = LocalArtifactGarbageCollector(
                fixture.runtime.artifacts,
                fixture.runtime.store,
            ).scan_reachable_digests()

            self.assertIn(committed.manifest_ref.sha256, reachable)
            self.assertTrue(
                all(
                    ref.sha256 in reachable
                    for ref in committed.tool_receipt_refs
                )
            )

    def test_store_rejects_tampered_verified_replay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory), with_tool=False)
            committed = fixture.commit()
            tampered = replace(
                committed.receipt,
                result_digest=_digest("tampered-result"),
            )
            attempt = fixture.runtime.store.get_attempt(
                fixture.claim.attempt_id
            )
            self.assertIsNotNone(attempt)
            assert attempt is not None

            with self.assertRaisesRegex(
                InvalidStateTransition,
                "binding is invalid",
            ):
                fixture.runtime.store.complete_verified_agent_activity(
                    fixture.claim.run_id,
                    fixture.claim.node_id,
                    fixture.claim.attempt_id,
                    fixture.claim.request_hash,
                    fixture.claim.worker_id,
                    claim_token=fixture.claim.claim_token,
                    result=attempt.result,
                    receipt=tampered,
                    now=fixture.runtime.clock.now,
                )

            self.assertEqual(
                fixture.runtime.store.get_agent_activity_receipt(
                    fixture.claim.run_id,
                    fixture.claim.attempt_id,
                ),
                committed.receipt,
            )
            self.assertEqual(len(fixture.parent_terminal_events()), 1)


class ToolReceiptArtifactStoreTests(unittest.TestCase):
    def test_round_trip_and_ref_tampering_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _TerminalFixture(Path(directory))
            assert fixture.tool_receipt is not None
            artifacts = ToolReceiptArtifactStore(
                fixture.runtime.artifacts
            )

            ref = artifacts.stage(
                fixture.tool_receipt,
                sensitivity=ArtifactSensitivity.SENSITIVE,
            )

            self.assertEqual(artifacts.load(ref), fixture.tool_receipt)
            self.assertEqual(ref.sha256, fixture.tool_receipt.receipt_digest)
            with self.assertRaisesRegex(
                ToolReceiptArtifactError,
                "tool_receipt_artifact_invalid",
            ):
                artifacts.load(replace(ref, kind=ArtifactKind.LOG))
            with self.assertRaisesRegex(
                ToolReceiptArtifactError,
                "tool_receipt_artifact_invalid",
            ):
                artifacts.stage(
                    fixture.tool_receipt,
                    sensitivity=ArtifactSensitivity.PUBLIC,
                )


if __name__ == "__main__":
    unittest.main()
