from __future__ import annotations

import threading
import unittest
from dataclasses import replace

from src.orchestration.models import ClaimDisposition, IdempotencyClaim
from src.orchestration.remote_execution import SecureRemoteExecutionError
from src.orchestration.remote_protocol import (
    MAX_OUTPUT_HANDLES,
    RemoteOperation,
    make_request,
    parse_response,
)
from src.orchestration.sandbox import ResourceLimits

from tests import test_orchestration_remote_execution as remote_execution_tests
from tests import test_orchestration_remote_protocol as remote_protocol_tests


class Round2RedactionAndBudgetTests(unittest.TestCase):
    def _harness(
        self,
    ) -> remote_execution_tests.SecureRemoteExecutionTests:
        harness = remote_execution_tests.SecureRemoteExecutionTests(
            methodName="runTest"
        )
        harness.setUp()
        self.addCleanup(harness.doCleanups)
        return harness

    def _admitted_attempt(
        self,
        *,
        run_id: str,
        output_bytes: int,
    ):
        harness = self._harness()
        limits = ResourceLimits(
            timeout_seconds=60.0,
            cpu_seconds=60.0,
            memory_bytes=256 * 1024 * 1024,
            output_bytes=output_bytes,
            process_count=16,
        )
        preparation = replace(harness.preparation, limits=limits)
        admitter = harness._admitter(
            plan_resolver=remote_execution_tests._PlanResolver(preparation)
        )
        claim = harness._claim(run_id)
        authorization = admitter.admit(
            harness.identity,
            harness.registration,
            harness.scheduler,
            claim,
        )
        harness.scheduler.start_claim(claim)
        return harness, admitter, claim, authorization

    def test_response_repr_omits_nested_live_credentials(self) -> None:
        harness = remote_protocol_tests.RemoteProtocolTests(
            methodName="runTest"
        )
        harness.setUp()
        self.addCleanup(harness.doCleanups)
        harness._register(harness.client)
        request = make_request(
            RemoteOperation.POLL,
            request_id="request-safe",
            worker_id=harness.identity.worker_id,
            instance_id="instance-1",
            body={"run_id": "run-remote", "lease_seconds": 30.0},
        )
        response = parse_response(
            harness.control.handle(harness.identity, request.to_wire())
        )
        assignment = response.body["assignment"]
        self.assertIsNotNone(assignment)
        assert assignment is not None
        claim_token = assignment["claim"]["claim_token"]
        artifact_token = assignment["output_grants"][0]["handle"]["token"]

        rendered = repr(response)

        self.assertIn("operation='poll'", rendered)
        self.assertIn("body_field_count=1", rendered)
        for credential in (claim_token, artifact_token):
            self.assertNotIn(credential, rendered)

    def test_nested_control_record_repr_omits_claim_credential(self) -> None:
        harness, admitter, claim, authorization = self._admitted_attempt(
            run_id="repr-combination",
            output_bytes=64,
        )
        del admitter, authorization
        attempt = harness.store.get_attempt(claim.attempt_id)
        self.assertIsNotNone(attempt)
        assert attempt is not None
        idempotency = harness.store.get_idempotency(
            claim.run_id,
            attempt.idempotency_key,
        )
        self.assertIsNotNone(idempotency)
        assert idempotency is not None
        combination = (
            claim,
            attempt,
            idempotency,
            IdempotencyClaim(ClaimDisposition.ACQUIRED, idempotency),
        )

        rendered = repr(combination)

        self.assertNotIn(claim.claim_token, rendered)
        self.assertIn(claim.attempt_id, rendered)
        self.assertIn("status='running'", rendered)

    def test_exact_byte_boundary_succeeds_then_rejects_before_mutation(
        self,
    ) -> None:
        harness, admitter, claim, authorization = self._admitted_attempt(
            run_id="byte-boundary",
            output_bytes=4,
        )
        for content in (b"ab", b"cd"):
            admitter.stage_output(
                harness.identity,
                harness.registration,
                harness.scheduler,
                claim,
                authorization,
                content=content,
            )
        before = tuple(sorted(harness.artifacts.root.rglob("*")))

        with self.assertRaisesRegex(
            SecureRemoteExecutionError,
            "output_budget_exceeded",
        ):
            admitter.stage_output(
                harness.identity,
                harness.registration,
                harness.scheduler,
                claim,
                authorization,
                content=b"x",
            )

        self.assertEqual(
            before,
            tuple(sorted(harness.artifacts.root.rglob("*"))),
        )

    def test_sixty_fifth_handle_rejected_before_broker_mutation(self) -> None:
        harness, admitter, claim, authorization = self._admitted_attempt(
            run_id="handle-boundary",
            output_bytes=1,
        )
        for _ in range(MAX_OUTPUT_HANDLES):
            admitter.stage_output(
                harness.identity,
                harness.registration,
                harness.scheduler,
                claim,
                authorization,
                content=b"",
            )
        grant_count = len(harness.broker._write_grants)

        with self.assertRaisesRegex(
            SecureRemoteExecutionError,
            "output_budget_exceeded",
        ):
            admitter.stage_output(
                harness.identity,
                harness.registration,
                harness.scheduler,
                claim,
                authorization,
                content=b"",
            )

        self.assertEqual(MAX_OUTPUT_HANDLES, grant_count)
        self.assertEqual(grant_count, len(harness.broker._write_grants))

    def test_concurrent_stage_cannot_cross_cumulative_byte_limit(self) -> None:
        harness, admitter, claim, authorization = self._admitted_attempt(
            run_id="concurrent-budget",
            output_bytes=4,
        )
        workers = 8
        barrier = threading.Barrier(workers + 1)
        outcome_lock = threading.Lock()
        outcomes: list[str] = []

        def stage() -> None:
            barrier.wait()
            try:
                admitter.stage_output(
                    harness.identity,
                    harness.registration,
                    harness.scheduler,
                    claim,
                    authorization,
                    content=b"xy",
                )
            except SecureRemoteExecutionError as exc:
                outcome = exc.reason_code
            else:
                outcome = "succeeded"
            with outcome_lock:
                outcomes.append(outcome)

        threads = [
            threading.Thread(target=stage, name=f"stage-{index}")
            for index in range(workers)
        ]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=10)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(2, outcomes.count("succeeded"))
        self.assertEqual(
            workers - 2,
            outcomes.count("output_budget_exceeded"),
        )

    def test_store_exception_keeps_conservative_budget_charge(self) -> None:
        harness, admitter, claim, authorization = self._admitted_attempt(
            run_id="ambiguous-store-write",
            output_bytes=4,
        )
        original_finalize = harness.broker.finalize_write
        finalized = threading.Event()

        def fail_after_store_write(*args, **kwargs):
            original_finalize(*args, **kwargs)
            finalized.set()
            raise RuntimeError("simulated response loss after Store write")

        harness.broker.finalize_write = fail_after_store_write
        with self.assertRaisesRegex(RuntimeError, "simulated response loss"):
            admitter.stage_output(
                harness.identity,
                harness.registration,
                harness.scheduler,
                claim,
                authorization,
                content=b"data",
            )
        self.assertTrue(finalized.is_set())
        harness.broker.finalize_write = original_finalize
        grant_count = len(harness.broker._write_grants)

        with self.assertRaisesRegex(
            SecureRemoteExecutionError,
            "output_budget_exceeded",
        ):
            admitter.stage_output(
                harness.identity,
                harness.registration,
                harness.scheduler,
                claim,
                authorization,
                content=b"data",
            )

        self.assertEqual(grant_count, len(harness.broker._write_grants))


if __name__ == "__main__":
    unittest.main()
