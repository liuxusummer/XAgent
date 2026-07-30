from __future__ import annotations

import io
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from src.core.eval_report import (
    EvalReportError,
    build_regression_report,
    resolve_metric,
)
from src.eval_report import main


def _run(
    run_id: str,
    *,
    digest: str = "a" * 64,
    dataset_id: str = "suite-v1",
    pass_rate: float = 0.9,
    error_rate: float = 0.0,
    p95_duration: float = 10.0,
    evaluation_digest: str = "",
) -> dict:
    result = {
        "version": 2,
        "id": run_id,
        "dataset_id": dataset_id,
        "dataset_digest": digest,
        "agent": "main",
        "status": "completed",
        "summary": {
            "total": 10,
            "pass_rate": pass_rate,
            "error_rate": error_rate,
            "p95_duration": p95_duration,
            "tags": {"retrieval": {"pass_rate": pass_rate}},
        },
        "cases": [
            {
                "task": "SECRET TASK",
                "response_excerpt": "SECRET RESPONSE",
                "tool_payload": "SECRET TOOL PAYLOAD",
            }
        ],
    }
    if evaluation_digest:
        result["evaluation_digest"] = evaluation_digest
    return result


def _budget() -> dict:
    return {
        "version": 1,
        "require_same_dataset": True,
        "checks": [
            {
                "id": "pass-rate",
                "path": "/summary/pass_rate",
                "direction": "higher",
                "threshold": 0.85,
                "max_regression": 0.02,
            },
            {
                "id": "p95",
                "path": "/summary/p95_duration",
                "direction": "lower",
                "max_regression_ratio": 0.2,
            },
        ],
    }


class EvalRegressionReportTests(unittest.TestCase):
    def test_report_passes_within_explicit_budget_and_omits_payloads(self) -> None:
        report = build_regression_report(
            _run("current", pass_rate=0.89, p95_duration=11.9),
            _run("baseline", pass_rate=0.9, p95_duration=10),
            _budget(),
        )

        self.assertEqual(report["verdict"], "passed")
        self.assertEqual(report["failed_checks"], [])
        rendered = json.dumps(report)
        self.assertNotIn("SECRET TASK", rendered)
        self.assertNotIn("SECRET RESPONSE", rendered)
        self.assertNotIn("SECRET TOOL PAYLOAD", rendered)

    def test_report_fails_threshold_regression_and_dataset_mismatch(self) -> None:
        report = build_regression_report(
            _run(
                "current",
                digest="b" * 64,
                pass_rate=0.8,
                p95_duration=13,
            ),
            _run("baseline", pass_rate=0.9, p95_duration=10),
            _budget(),
        )

        self.assertEqual(report["verdict"], "failed")
        self.assertEqual(
            report["failed_checks"],
            ["dataset_compatibility", "pass-rate", "p95"],
        )
        self.assertEqual(len(report["checks"][0]["reasons"]), 2)

    def test_legacy_compatibility_requires_same_dataset_and_case_count(self) -> None:
        current = _run("current", digest="")
        baseline = _run("baseline", digest="")
        self.assertTrue(
            build_regression_report(current, baseline, _budget())[
                "dataset_compatibility"
            ]["passed"]
        )
        baseline["summary"]["total"] = 9
        self.assertFalse(
            build_regression_report(current, baseline, _budget())[
                "dataset_compatibility"
            ]["passed"]
        )

    def test_scenario_pack_digest_takes_precedence_over_dataset_digest(self) -> None:
        current = _run("current", evaluation_digest="b" * 64)
        baseline = _run("baseline", evaluation_digest="c" * 64)
        report = build_regression_report(current, baseline, _budget())
        self.assertFalse(report["dataset_compatibility"]["passed"])
        self.assertEqual(
            report["dataset_compatibility"]["mode"],
            "evaluation_digest",
        )

        baseline["evaluation_digest"] = "b" * 64
        report = build_regression_report(current, baseline, _budget())
        self.assertTrue(report["dataset_compatibility"]["passed"])
        baseline = _run("baseline", digest="a" * 64)
        self.assertFalse(
            build_regression_report(current, baseline, _budget())[
                "dataset_compatibility"
            ]["passed"]
        )

    def test_json_pointer_decodes_standard_escapes(self) -> None:
        run = {"summary": {"a/b": {"x~y": 0.75}}}
        self.assertEqual(resolve_metric(run, "/summary/a~1b/x~0y"), 0.75)

    def test_invalid_inputs_fail_closed(self) -> None:
        with self.assertRaises(EvalReportError):
            build_regression_report(
                _run("current", pass_rate=float("nan")),
                _run("baseline"),
                _budget(),
            )
        duplicate_budget = _budget()
        duplicate_budget["checks"].append(dict(duplicate_budget["checks"][0]))
        with self.assertRaises(EvalReportError):
            build_regression_report(
                _run("current"),
                _run("baseline"),
                duplicate_budget,
            )
        oversized = _run("x" * 121)
        with self.assertRaises(EvalReportError):
            build_regression_report(oversized, _run("baseline"), _budget())
        malformed_digest = _run("current", digest="not-a-digest")
        with self.assertRaises(EvalReportError):
            build_regression_report(
                malformed_digest,
                _run("baseline"),
                _budget(),
            )
        overflow_budget = {
            "version": 1,
            "checks": [
                {
                    "id": "overflow",
                    "path": "/summary/p95_duration",
                    "direction": "lower",
                    "max_regression_ratio": 1e308,
                }
            ],
        }
        with self.assertRaises(EvalReportError):
            build_regression_report(
                _run("current", p95_duration=1e308),
                _run("baseline", p95_duration=1e308),
                overflow_budget,
            )


class EvalReportCliTests(unittest.TestCase):
    def test_cli_exit_codes_and_machine_readable_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            current = root / "current.json"
            baseline = root / "baseline.json"
            budget = root / "budget.json"
            report_path = root / "report.json"
            current.write_text(
                json.dumps(_run("current", pass_rate=0.7)),
                encoding="utf-8",
            )
            baseline.write_text(json.dumps(_run("baseline")), encoding="utf-8")
            budget.write_text(json.dumps(_budget()), encoding="utf-8")

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "--current",
                        str(current),
                        "--baseline",
                        str(baseline),
                        "--budget",
                        str(budget),
                        "--output",
                        str(report_path),
                        "--compact",
                    ]
                )

            self.assertEqual(exit_code, 1)
            self.assertEqual(json.loads(stdout.getvalue())["verdict"], "failed")
            self.assertEqual(
                json.loads(report_path.read_text(encoding="utf-8"))["verdict"],
                "failed",
            )
            self.assertNotIn("SECRET", stdout.getvalue())

            budget.write_text("{not-json", encoding="utf-8")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                invalid_exit = main(
                    [
                        "--current",
                        str(current),
                        "--baseline",
                        str(baseline),
                        "--budget",
                        str(budget),
                    ]
                )
            self.assertEqual(invalid_exit, 2)
            self.assertEqual(json.loads(stderr.getvalue())["verdict"], "invalid")

            budget.write_text(
                '{"version":1,"version":1,"checks":[]}',
                encoding="utf-8",
            )
            duplicate_stderr = io.StringIO()
            with redirect_stderr(duplicate_stderr):
                duplicate_exit = main(
                    [
                        "--current",
                        str(current),
                        "--baseline",
                        str(baseline),
                        "--budget",
                        str(budget),
                    ]
                )
            self.assertEqual(duplicate_exit, 2)
            self.assertIn("duplicate JSON key", duplicate_stderr.getvalue())

    def test_module_process_returns_regression_exit_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            current = root / "current.json"
            baseline = root / "baseline.json"
            budget = root / "budget.json"
            current.write_text(
                json.dumps(_run("current", pass_rate=0.7)),
                encoding="utf-8",
            )
            baseline.write_text(json.dumps(_run("baseline")), encoding="utf-8")
            budget.write_text(json.dumps(_budget()), encoding="utf-8")

            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "src.eval_report",
                    "--current",
                    str(current),
                    "--baseline",
                    str(baseline),
                    "--budget",
                    str(budget),
                    "--compact",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(completed.returncode, 1)
            self.assertEqual(json.loads(completed.stdout)["verdict"], "failed")
            self.assertEqual(completed.stderr, "")


if __name__ == "__main__":
    unittest.main()
