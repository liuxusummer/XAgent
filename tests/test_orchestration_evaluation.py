from __future__ import annotations

import json
import unittest
from types import SimpleNamespace

from src.orchestration.evaluation import (
    DEFAULT_INVARIANTS,
    EvaluationContext,
    ReliabilityEvidence,
    Scenario,
    Suite,
    evaluate_reliability,
    fault_scenarios,
)
from src.orchestration.models import (
    AttemptRecord,
    AttemptStatus,
    RunRecord,
    RunStatus,
)
from src.orchestration.replay import ProjectionDiff, ReplaySnapshot

_DIGEST = "b" * 64


def _snapshot(
    *,
    terminal: bool = True,
    active_attempt: bool = False,
) -> ReplaySnapshot:
    run = RunRecord(
        "run",
        "workflow",
        definition_digest=_DIGEST,
        status=RunStatus.COMPLETED if terminal else RunStatus.RUNNING,
        created_at=1,
        updated_at=2,
        last_event_sequence=2,
        projection_version=2,
    )
    attempts = ()
    if active_attempt:
        attempts = (
            AttemptRecord(
                "attempt",
                "run",
                "node",
                1,
                status=AttemptStatus.RUNNING,
                worker_id="worker",
                lease_id="lease",
                fencing_token=1,
                scheduled_at=1,
                started_at=2,
                last_event_sequence=2,
                projection_version=2,
            ),
        )
    return ReplaySnapshot(
        run=run,
        nodes=(),
        attempts=attempts,
        event_count=2,
        last_sequence=2,
    )


class OrchestrationEvaluationTests(unittest.TestCase):
    def test_passing_suite_has_stable_json_and_junit_summary(self) -> None:
        scenario = Scenario("happy", "run")
        suite = Suite("durability", (scenario,))

        def runner(current: Scenario) -> EvaluationContext:
            return EvaluationContext(
                scenario=current,
                events=(
                    SimpleNamespace(
                        seq=1,
                        event_type="run.created",
                        node_id=None,
                        attempt_id=None,
                    ),
                    SimpleNamespace(
                        seq=2,
                        event_type="run.completed",
                        node_id=None,
                        attempt_id=None,
                    ),
                ),
                snapshot=_snapshot(),
                sqlite_bytes=b"safe database",
                trace_bytes=b"safe trace",
                secret_tokens=("not-present",),
            )

        first = suite.run(runner)
        second = suite.run(runner)

        self.assertEqual(first.to_json(), second.to_json())
        self.assertEqual(first.passed_count, 1)
        self.assertEqual(first.failed_count, 0)
        self.assertEqual(first.pass_rate, 1.0)
        self.assertEqual(
            first.to_dict()["summary"]["failure_codes"],
            {},
        )
        self.assertEqual(
            first.to_junit_dict(),
            {
                "testsuite": {
                    "name": "durability",
                    "tests": 1,
                    "failures": 0,
                    "testcase": [
                        {
                            "name": "happy",
                            "classname": "durability",
                            "status": "passed",
                        }
                    ],
                }
            },
        )

    def test_failure_suite_reports_every_code_without_secret(self) -> None:
        scenario = Scenario("broken", "run", fault_point="F25")
        suite = Suite("durability", (scenario,), DEFAULT_INVARIANTS)
        secret = "credential-that-must-not-leak"

        def runner(current: Scenario) -> EvaluationContext:
            return EvaluationContext(
                scenario=current,
                events=(
                    SimpleNamespace(
                        seq=1,
                        event_type="run.completed",
                        node_id=None,
                        attempt_id=None,
                    ),
                    SimpleNamespace(
                        seq=3,
                        event_type="run.failed",
                        node_id=None,
                        attempt_id=None,
                    ),
                ),
                snapshot=_snapshot(active_attempt=True),
                projection_diffs=(
                    ProjectionDiff(
                        "run",
                        "run",
                        "status",
                        "completed",
                        "running",
                    ),
                ),
                sqlite_bytes=f"sqlite:{secret}".encode(),
                trace_bytes=f"trace:{secret}".encode(),
                secret_tokens=(secret,),
            )

        report = suite.run(runner)
        result = report.results[0]

        self.assertFalse(result.passed)
        self.assertEqual(
            set(result.failure_codes),
            {
                "active_attempt_on_terminal",
                "duplicate_terminal",
                "projection_drift",
                "secret_token_found",
                "sequence_gap",
            },
        )
        self.assertEqual(
            report.to_dict()["summary"]["failure_codes"],
            {
                "active_attempt_on_terminal": 1,
                "duplicate_terminal": 1,
                "projection_drift": 1,
                "secret_token_found": 2,
                "sequence_gap": 1,
            },
        )
        self.assertNotIn(secret, report.to_json())
        self.assertEqual(report.to_junit_dict()["testsuite"]["failures"], 1)

    def test_fault_matrix_is_sorted_and_deterministic(self) -> None:
        scenarios = fault_scenarios(
            [
                {"fault_point": "F08", "parameters": {"effect": "unknown"}},
                "F02",
                "F01",
            ],
            run_id_prefix="eval",
        )
        self.assertEqual(
            [scenario.fault_point for scenario in scenarios],
            ["F01", "F02", "F08"],
        )
        suite = Suite("fault-matrix", scenarios)

        def runner(current: Scenario) -> EvaluationContext:
            first_sequence = 2 if current.fault_point == "F02" else 1
            return EvaluationContext(
                scenario=current,
                events=(
                    SimpleNamespace(
                        seq=first_sequence,
                        event_type="run.created",
                        node_id=None,
                        attempt_id=None,
                    ),
                ),
            )

        first = suite.run(runner)
        second = suite.run(runner)

        self.assertEqual(first.to_dict(), second.to_dict())
        self.assertEqual(first.passed_count, 2)
        self.assertEqual(first.failed_count, 1)
        self.assertEqual(first.results[1].scenario_id, "fault:F02")
        self.assertEqual(first.results[1].failure_codes, ("sequence_gap",))
        self.assertEqual(json.loads(first.to_json()), first.to_dict())

    def test_runner_failure_is_contained_and_stably_classified(self) -> None:
        suite = Suite("runner", (Scenario("case", "run"),))

        def runner(_: Scenario) -> EvaluationContext:
            raise RuntimeError("response body with credentials")

        result = suite.run(runner).results[0]

        self.assertEqual(result.failure_codes, ("runner_error",))
        self.assertEqual(
            result.failures[0].details,
            {"exception_type": "RuntimeError"},
        )
        self.assertNotIn("credentials", json.dumps(result.to_dict()))

    def test_suite_orders_scenarios_and_rejects_duplicate_ids(self) -> None:
        scenarios = (
            Scenario("z", "run-z"),
            Scenario("a", "run-a"),
        )
        suite = Suite("ordered", scenarios, invariants=())

        report = suite.run(
            lambda scenario: EvaluationContext(scenario=scenario)
        )

        self.assertEqual(
            [result.scenario_id for result in report.results],
            ["a", "z"],
        )
        with self.assertRaises(ValueError):
            Suite(
                "duplicate",
                (Scenario("same", "run-1"), Scenario("same", "run-2")),
            )

    def test_reliability_report_calculates_only_explicit_evidence(self) -> None:
        report = evaluate_reliability(
            ReliabilityEvidence(
                recovery_successes=8,
                recovery_attempts=10,
                duplicate_visible_side_effects=1,
                visible_side_effect_checks=20,
                outcome_unknown_attempts=2,
                terminal_attempt_checks=8,
                projection_replay_divergences=0,
                projection_replay_checks=10,
                cancellation_leaks=1,
                cancellation_checks=4,
                stale_worker_commit_rejections=3,
                stale_worker_commit_attempts=3,
                resume_latencies_ms=(100, 50, 200, 150, 400),
            )
        )

        self.assertEqual(report.recovery_success_rate.value, 0.8)
        self.assertEqual(
            report.duplicate_visible_side_effect_rate.value,
            0.05,
        )
        self.assertEqual(report.outcome_unknown_rate.value, 0.25)
        self.assertEqual(
            report.projection_replay_divergence_rate.value,
            0.0,
        )
        self.assertTrue(report.projection_replay_divergence_rate.known)
        self.assertEqual(report.cancellation_leak_rate.value, 0.25)
        self.assertEqual(
            report.stale_worker_commit_rejection_rate.value,
            1.0,
        )
        self.assertEqual(report.p50_resume_latency_ms.value, 150)
        self.assertEqual(report.p95_resume_latency_ms.value, 400)
        self.assertEqual(report.p50_resume_latency_ms.sample_count, 5)
        self.assertEqual(
            report.to_dict()["recovery_success_rate"],
            {
                "status": "known",
                "value": 0.8,
                "unit": "ratio",
                "numerator": 8,
                "denominator": 10,
            },
        )

    def test_missing_and_zero_denominator_evidence_are_unknown_not_zero(self) -> None:
        missing = evaluate_reliability(ReliabilityEvidence())
        zero_denominator = evaluate_reliability(
            ReliabilityEvidence(
                recovery_successes=0,
                recovery_attempts=0,
                projection_replay_divergences=0,
                projection_replay_checks=0,
                resume_latencies_ms=(),
            )
        )

        for metric in missing.to_dict().values():
            self.assertEqual(metric["status"], "unknown")
            self.assertIsNone(metric["value"])
        self.assertEqual(
            missing.recovery_success_rate.unknown_reason,
            "missing_evidence",
        )
        self.assertEqual(
            zero_denominator.recovery_success_rate.unknown_reason,
            "zero_denominator",
        )
        self.assertEqual(
            zero_denominator.projection_replay_divergence_rate.unknown_reason,
            "zero_denominator",
        )
        self.assertEqual(
            zero_denominator.p50_resume_latency_ms.unknown_reason,
            "no_samples",
        )
        self.assertFalse(zero_denominator.recovery_success_rate.known)

    def test_resume_percentiles_are_stable_and_input_order_independent(self) -> None:
        first = evaluate_reliability(
            ReliabilityEvidence(resume_latencies_ms=(9, 1, 5, 3, 7))
        )
        second = evaluate_reliability(
            ReliabilityEvidence(resume_latencies_ms=(3, 7, 9, 5, 1))
        )

        self.assertEqual(first.p50_resume_latency_ms.value, 5)
        self.assertEqual(first.p95_resume_latency_ms.value, 9)
        self.assertEqual(
            first.p50_resume_latency_ms,
            second.p50_resume_latency_ms,
        )
        self.assertEqual(
            first.p95_resume_latency_ms,
            second.p95_resume_latency_ms,
        )

    def test_reliability_evidence_rejects_invalid_or_inferred_inputs(self) -> None:
        with self.assertRaises(ValueError):
            ReliabilityEvidence(
                recovery_successes=2,
                recovery_attempts=1,
            )
        with self.assertRaises(ValueError):
            ReliabilityEvidence(resume_latencies_ms=(1, float("nan")))
        with self.assertRaises(TypeError):
            evaluate_reliability(
                [
                    SimpleNamespace(
                        event_type="attempt.outcome_unknown",
                    )
                ]
            )


if __name__ == "__main__":
    unittest.main()
