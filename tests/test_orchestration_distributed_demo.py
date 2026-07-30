from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from examples.distributed_execution_demo import run_demo, run_soak


class DistributedExecutionDemoTests(unittest.TestCase):
    def test_flagship_demo_exercises_required_distributed_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            summary = run_demo(Path(directory))

        self.assertEqual(summary["primary_run"]["status"], "completed")
        self.assertEqual(summary["primary_run"]["worker_sessions"], 3)
        self.assertEqual(
            set(summary["primary_run"]["participating_workers"]),
            {"worker-a", "worker-b", "worker-c"},
        )
        self.assertGreaterEqual(summary["primary_run"]["maximum_concurrency"], 2)
        self.assertTrue(summary["primary_run"]["concurrent_overlap_proved"])
        self.assertEqual(summary["primary_run"]["approval_status"], "succeeded")
        self.assertEqual(summary["primary_run"]["artifact_count"], 3)
        self.assertEqual(summary["primary_run"]["tool_receipt_count"], 3)
        self.assertTrue(summary["primary_run"]["replay_matches_live"])
        self.assertEqual(summary["cancel_run"]["status"], "cancelled")
        self.assertEqual(summary["cancel_run"]["attempt_status"], "cancelled")
        self.assertEqual(summary["cancel_run"]["worker"], "worker-c")
        self.assertTrue(summary["cancel_run"]["replay_matches_live"])
        self.assertEqual(
            summary["security_boundary"]["transport"],
            "in_process_reference",
        )
        self.assertFalse(summary["security_boundary"]["mtls_deployed"])
        self.assertFalse(summary["security_boundary"]["gvisor_deployed"])
        self.assertTrue(
            summary["security_boundary"][
                "digest_only_execution_recovery"
            ]
        )
        self.assertTrue(
            summary["security_boundary"][
                "bearer_free_artifact_recovery"
            ]
        )

    def test_soak_reports_workload_separately_from_wall_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = run_soak(Path(directory), logical_runs=2)

        self.assertEqual(report["logical_runs"], 2)
        self.assertEqual(report["remote_attempts"], 6)
        self.assertEqual(report["worker_sessions"], 3)
        self.assertEqual(report["completed_runs"], 2)
        self.assertGreaterEqual(report["wall_time_seconds"], 0)
        self.assertEqual(report["claim"], "reference_workload_not_production_slo")

    def test_cli_emits_json_without_credentials_or_host_paths(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            completed = subprocess.run(
                [
                    sys.executable,
                    "examples/distributed_execution_demo.py",
                    "--runtime-dir",
                    directory,
                ],
                cwd=".",
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertEqual(
            completed.returncode,
            0,
            msg=completed.stderr or completed.stdout,
        )
        payload = json.loads(completed.stdout)
        self.assertNotIn(directory, completed.stdout)
        self.assertNotIn("token", completed.stdout.casefold())
        self.assertEqual(payload["primary_run"]["status"], "completed")


if __name__ == "__main__":
    unittest.main()
