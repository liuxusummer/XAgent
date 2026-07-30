from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.orchestration.artifacts import LocalArtifactStore
from src.orchestration.protocol import (
    EventsBody,
    Operation,
    ParentSubmission,
    ProtocolResponse,
    ProtocolValidationError,
    ResolveRecoveryBody,
    SubmitBody,
    parse_request,
)


class OrchestrationProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.artifacts = LocalArtifactStore(
            Path(self.temporary.name) / "artifacts"
        )
        self.workflow_ref = self.artifacts.put_json(
            {
                "schema_version": 2,
                "name": "protocol",
                "version": 1,
                "nodes": [
                    {
                        "id": "step",
                        "kind": "tool",
                        "depends_on": [],
                        "config": {"tool": "inspect", "arguments": {}},
                        "effect_class": "read_only",
                    }
                ],
            }
        )
        self.input_ref = self.artifacts.put_json({"input": "artifact-only"})

    def _request(self, operation: str, body: dict) -> dict:
        return {
            "protocol_version": 1,
            "request_id": "request-1",
            "operation": operation,
            "body": body,
        }

    def test_submit_parses_only_complete_artifact_receipts(self) -> None:
        request = parse_request(
            self._request(
                "submit",
                {
                    "run_id": "stable-run-1",
                    "workflow_ref": self.workflow_ref.to_dict(),
                    "input_receipt": {
                        "artifact_refs": [self.input_ref.to_dict()]
                    },
                    "parent": None,
                },
            )
        )

        self.assertEqual(request.operation, Operation.SUBMIT)
        self.assertIsInstance(request.body, SubmitBody)
        self.assertEqual(request.body.run_id, "stable-run-1")
        self.assertEqual(
            request.body.input_receipt.artifact_refs,
            (self.input_ref,),
        )

    def test_raw_submit_payload_and_unknown_fields_are_rejected(self) -> None:
        invalid = (
            self._request(
                "submit",
                {
                    "run_id": "run",
                    "workflow_ref": self.workflow_ref.to_dict(),
                    "input_receipt": None,
                    "parent": None,
                    "raw_input": {"secret": "not-an-artifact"},
                },
            ),
            {
                **self._request("status", {"run_id": "run"}),
                "future_field": True,
            },
            self._request(
                "status",
                {"run_id": "run", "unknown": "extension"},
            ),
        )
        for payload in invalid:
            with self.subTest(payload=payload):
                with self.assertRaises(ProtocolValidationError):
                    parse_request(payload)

    def test_version_ids_artifact_schema_and_bounds_are_strict(self) -> None:
        bad_version = self._request("status", {"run_id": "run"})
        bad_version["protocol_version"] = 2
        bad_artifact = self.workflow_ref.to_dict()
        bad_artifact["future"] = "field"
        invalid = (
            bad_version,
            self._request("status", {"run_id": "../runtime.sqlite3"}),
            self._request(
                "events",
                {"run_id": "run", "after_sequence": 0, "limit": 101},
            ),
            self._request(
                "submit",
                {
                    "run_id": "run",
                    "workflow_ref": bad_artifact,
                    "input_receipt": None,
                    "parent": None,
                },
            ),
        )
        for payload in invalid:
            with self.subTest(payload=payload):
                with self.assertRaises(ProtocolValidationError):
                    parse_request(payload)
        with self.assertRaises(ProtocolValidationError):
            ParentSubmission("../parent", "node")

    def test_every_control_operation_has_a_bounded_typed_body(self) -> None:
        cases = {
            "status": {"run_id": "run"},
            "events": {
                "run_id": "run",
                "after_sequence": 4,
                "limit": 10,
            },
            "cancel": {"run_id": "run"},
            "pause": {"run_id": "run"},
            "resume": {"run_id": "run"},
            "recover": {"run_id": "run", "limit": 5},
            "resolve_recovery": {
                "resolution_id": "resolution-1",
                "run_id": "run",
                "node_id": "node",
                "attempt_id": "attempt",
                "resolution": "confirmed_succeeded",
                "evidence_ref": self.input_ref.to_dict(),
                "result_ref": self.input_ref.to_dict(),
            },
            "tick": {"run_id": "run", "max_steps": 3},
        }
        for operation, body in cases.items():
            with self.subTest(operation=operation):
                parsed = parse_request(self._request(operation, body))
                self.assertEqual(parsed.operation.value, operation)
        events = parse_request(self._request("events", cases["events"]))
        self.assertIsInstance(events.body, EventsBody)
        self.assertEqual(events.body.limit, 10)
        resolution = parse_request(
            self._request("resolve_recovery", cases["resolve_recovery"])
        )
        self.assertIsInstance(resolution.body, ResolveRecoveryBody)
        self.assertEqual(
            resolution.body.decision.resolution.value,
            "confirmed_succeeded",
        )

    def test_recovery_resolution_requires_consistent_artifact_evidence(self) -> None:
        invalid = {
            "resolution_id": "resolution-1",
            "run_id": "run",
            "node_id": "node",
            "attempt_id": "attempt",
            "resolution": "confirmed_failed",
            "evidence_ref": self.input_ref.to_dict(),
            "result_ref": self.input_ref.to_dict(),
        }

        with self.assertRaises(ProtocolValidationError):
            parse_request(self._request("resolve_recovery", invalid))

    def test_response_marks_only_protocol_success_or_failure(self) -> None:
        success = ProtocolResponse.success(
            "request-1",
            {"run_id": "run", "status": "running"},
        ).to_dict()
        failure = ProtocolResponse.failure(
            "request-1",
            "authorization_denied",
            "operation is not authorized",
        ).to_dict()

        self.assertEqual(success["protocol_version"], 1)
        self.assertTrue(success["ok"])
        self.assertNotIn("error", success)
        self.assertFalse(failure["ok"])
        self.assertNotIn("result", failure)


if __name__ == "__main__":
    unittest.main()
