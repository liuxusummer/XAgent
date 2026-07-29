from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from examples.durable_orchestration_demo import (
    DEMO_SECRET_MARKER,
    _VisibleEffectLedger,
    run_demo,
)
from examples.orchestration_runtime_minimal import run as run_minimal_runtime
from src.orchestration.artifacts import LocalArtifactStore


class DurableOrchestrationDemoTests(unittest.TestCase):
    def test_minimal_runtime_composition_completes_with_disjoint_roots(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = run_minimal_runtime(root)

            self.assertEqual(summary["status"], "completed")
            self.assertEqual(summary["processed"], 1)
            self.assertTrue(summary["replay_matches_live"])
            self.assertGreater(summary["event_count"], 0)
            control_plane = Path(summary["control_plane"])
            agent_workspace = Path(summary["agent_workspace"])
            self.assertFalse(control_plane.is_relative_to(agent_workspace))
            self.assertFalse(agent_workspace.is_relative_to(control_plane))
            self.assertTrue(
                (control_plane / "orchestration.sqlite3").is_file()
            )

    def test_flagship_demo_proves_the_durable_acceptance_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = run_demo(root)

            self.assertEqual(summary["run"]["status"], "completed")
            self.assertEqual(summary["run"]["attempt_count"], 3)
            self.assertTrue(
                all(
                    status == "succeeded"
                    for status in summary["run"]["node_statuses"].values()
                )
            )
            self.assertTrue(
                summary["execution"]["parallel_claimed_before_execution"]
            )
            self.assertEqual(summary["execution"]["approval_gate"], "consumed")
            self.assertTrue(summary["execution"]["response_loss_recovered"])
            self.assertTrue(summary["execution"]["duplicate_retry_replayed"])
            self.assertTrue(
                summary["execution"]["external_write_before_tx_d_recovered"]
            )
            self.assertTrue(summary["execution"]["operation_key_deduplicated"])
            self.assertTrue(summary["execution"]["stale_claim_rejected"])
            self.assertEqual(summary["execution"]["visible_side_effect_count"], 1)
            self.assertEqual(summary["execution"]["backend_dispatch_count"], 3)
            self.assertTrue(summary["replay"]["matches_live"])
            self.assertRegex(summary["replay"]["golden_digest"], r"^[0-9a-f]{64}$")

            for metric in summary["reliability"].values():
                self.assertEqual(metric["status"], "known")
                self.assertIsNotNone(metric["value"])

            rendered = json.dumps(summary, ensure_ascii=False)
            self.assertNotIn(DEMO_SECRET_MARKER, rendered)
            control_plane = (root / "control-plane").resolve()
            agent_workspace = (root / "agent-workspace").resolve()
            self.assertTrue(control_plane.is_dir())
            self.assertTrue(agent_workspace.is_dir())
            self.assertFalse(control_plane.is_relative_to(agent_workspace))
            self.assertFalse(agent_workspace.is_relative_to(control_plane))
            self.assertTrue((control_plane / "durable-demo.sqlite3").is_file())
            self.assertFalse(
                any(
                    path.name.startswith("durable-demo.sqlite3")
                    for path in agent_workspace.rglob("*")
                )
            )
            for sqlite_file in control_plane.glob("durable-demo.sqlite3*"):
                with self.subTest(sqlite_file=sqlite_file.name):
                    self.assertNotIn(
                        DEMO_SECRET_MARKER.encode("utf-8"),
                        sqlite_file.read_bytes(),
                    )

    def test_external_ledger_deduplicates_by_stable_operation_key(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifacts = LocalArtifactStore(root / "artifacts")
            ledger = _VisibleEffectLedger(root / "external-effects.sqlite3")
            first = artifacts.put_json({"result": "first"})
            duplicate_candidate = artifacts.put_json({"result": "duplicate"})

            committed = ledger.publish("stable-key", "a" * 64, first)
            duplicate = ledger.publish(
                "stable-key",
                "a" * 64,
                duplicate_candidate,
            )

            self.assertEqual(committed, first)
            self.assertEqual(duplicate, first)
            self.assertEqual(ledger.count(), 1)
            self.assertEqual(ledger.probe("stable-key"), first)
            with self.assertRaises(RuntimeError):
                ledger.publish("stable-key", "b" * 64, duplicate_candidate)

    def test_runtime_layout_rejects_control_plane_symlink_escape(self) -> None:
        with (
            tempfile.TemporaryDirectory() as temporary,
            tempfile.TemporaryDirectory() as outside,
        ):
            root = Path(temporary)
            (root / "control-plane").symlink_to(
                Path(outside),
                target_is_directory=True,
            )

            with self.assertRaisesRegex(ValueError, "direct children"):
                run_demo(root)

    def test_golden_replay_digest_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            first_summary = run_demo(first)
            second_summary = run_demo(second)

        self.assertEqual(
            first_summary["replay"]["golden_digest"],
            second_summary["replay"]["golden_digest"],
        )


if __name__ == "__main__":
    unittest.main()
