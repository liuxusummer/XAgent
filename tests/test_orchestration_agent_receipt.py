from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.orchestration.agent_receipt import (
    AgentActivityReceipt,
    AgentActivityReceiptError,
    AgentActivityVerification,
    canonical_agent_result_digest,
)
from src.orchestration.legacy_loop import (
    ActivityRecoveryRequired,
    DurabilityError,
)
from src.orchestration.models import AttemptStatus
from src.orchestration.policy import EffectClass
from src.orchestration.store import StoreSchemaError, _content_digest
from tests.test_orchestration_legacy_loop import (
    StoreFixture,
    _completed_result,
)


def _receipt(**overrides) -> AgentActivityReceipt:
    values = {
        "run_id": "run-1",
        "node_id": "agent-node",
        "attempt_id": "attempt-1",
        "activity_name": "main",
        "effect_class": EffectClass.NON_IDEMPOTENT_WRITE,
        "attempt_status": AttemptStatus.SUCCEEDED,
        "request_digest": "a" * 64,
        "result_digest": "b" * 64,
        "exit_reason": "CURRENT_TASK_DONE",
        "turns": 2,
        "observed_tool_results": 0,
        "result_artifact_digests": (),
        "tool_receipt_digests": (),
        "internal_tool_receipts_complete": False,
        "verification": AgentActivityVerification.RUNTIME_OBSERVED,
    }
    values.update(overrides)
    return AgentActivityReceipt(**values)


class AgentActivityReceiptContractTests(unittest.TestCase):
    def test_round_trip_is_canonical_and_payload_free(self) -> None:
        receipt = _receipt(
            result_artifact_digests=("c" * 64,),
            tool_receipt_digests=("d" * 64,),
            observed_tool_results=1,
            internal_tool_receipts_complete=True,
            execution_manifest_digest="c" * 64,
        )

        restored = AgentActivityReceipt.from_dict(receipt.to_dict())

        self.assertEqual(restored, receipt)
        self.assertEqual(restored.receipt_digest, receipt.receipt_digest)
        serialized = json.dumps(receipt.to_dict(), sort_keys=True)
        self.assertNotIn("prompt", serialized)
        self.assertNotIn("response", serialized)
        self.assertNotIn("arguments", serialized)

    def test_success_and_uncertain_statuses_have_conservative_evidence(self) -> None:
        with self.assertRaisesRegex(
            AgentActivityReceiptError,
            "runtime-observed",
        ):
            _receipt(
                verification=AgentActivityVerification.UNVERIFIED
            )
        with self.assertRaisesRegex(
            AgentActivityReceiptError,
            "cannot claim observed",
        ):
            _receipt(
                attempt_status=AttemptStatus.OUTCOME_UNKNOWN,
                verification=(
                    AgentActivityVerification.RUNTIME_OBSERVED
                ),
            )

    def test_internal_receipt_completeness_requires_exact_coverage(self) -> None:
        with self.assertRaisesRegex(
            AgentActivityReceiptError,
            "exact observed coverage",
        ):
            _receipt(
                result_artifact_digests=("c" * 64,),
                observed_tool_results=2,
                tool_receipt_digests=("d" * 64,),
                internal_tool_receipts_complete=True,
                execution_manifest_digest="c" * 64,
            )

    def test_unknown_fields_and_non_identifier_activity_name_are_rejected(
        self,
    ) -> None:
        payload = _receipt().to_dict()
        payload["task"] = "must-not-enter-receipt"
        with self.assertRaisesRegex(
            AgentActivityReceiptError,
            "unknown or missing",
        ):
            AgentActivityReceipt.from_dict(payload)
        with self.assertRaisesRegex(
            AgentActivityReceiptError,
            "bounded identifier",
        ):
            _receipt(activity_name="agent name with secret text")
        with self.assertRaisesRegex(
            AgentActivityReceiptError,
            "schema version",
        ):
            _receipt(schema_version=True)
        with self.assertRaisesRegex(
            AgentActivityReceiptError,
            "enum",
        ):
            _receipt(effect_class={"malformed": True})

    def test_canonical_result_digest_rejects_non_json_and_nan(self) -> None:
        with self.assertRaises(AgentActivityReceiptError):
            canonical_agent_result_digest({"bad": object()})
        with self.assertRaises(AgentActivityReceiptError):
            canonical_agent_result_digest({"bad": float("nan")})


class DurableAgentActivityReceiptTests(unittest.TestCase):
    def test_legacy_success_persists_verifiable_agent_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            fixture.execute(
                lambda _task: _completed_result(),
                adapter_kwargs={"activity_name": "planner"},
            )

            receipt = fixture.store.get_agent_activity_receipt(
                fixture.run_id,
                fixture.attempt_id,
            )
            attempt = fixture.store.get_attempt(fixture.attempt_id)
            assert receipt is not None
            assert attempt is not None
            self.assertEqual(receipt.activity_name, "planner")
            self.assertEqual(
                receipt.attempt_status,
                AttemptStatus.SUCCEEDED,
            )
            self.assertEqual(
                receipt.verification,
                AgentActivityVerification.RUNTIME_OBSERVED,
            )
            self.assertEqual(receipt.observed_tool_results, 1)
            self.assertFalse(receipt.internal_tool_receipts_complete)
            self.assertEqual(
                receipt.result_digest,
                canonical_agent_result_digest(attempt.result),
            )
            self.assertEqual(
                receipt.result_artifact_digests,
                (
                    attempt.result["artifact_refs"][0]["sha256"],
                ),
            )
            event = fixture.store.list_events(fixture.run_id)[-1]
            self.assertNotIn("response", event.payload)
            self.assertNotIn("do work", str(event.payload))

    def test_known_read_only_failure_has_runtime_observed_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(
                Path(raw),
                effect_class="read_only",
            )
            fixture.execute(
                lambda _task: {
                    "response": "",
                    "exit_reason": "MAX_TURNS_EXCEEDED",
                    "tool_results": [],
                    "turns": 40,
                }
            )

            receipt = fixture.store.get_agent_activity_receipt(
                fixture.run_id,
                fixture.attempt_id,
            )
            assert receipt is not None
            self.assertEqual(
                receipt.attempt_status,
                AttemptStatus.FAILED,
            )
            self.assertEqual(receipt.exit_reason, "MAX_TURNS_EXCEEDED")
            self.assertEqual(receipt.turns, 40)
            self.assertEqual(
                receipt.verification,
                AgentActivityVerification.RUNTIME_OBSERVED,
            )

    def test_unknown_write_receipt_never_claims_side_effect_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            with self.assertRaises(ActivityRecoveryRequired):
                fixture.execute(
                    lambda _task: {
                        "response": "partial",
                        "exit_reason": "MAX_TURNS_EXCEEDED",
                        "tool_results": [{"status": "unknown"}],
                        "turns": 40,
                    }
                )

            receipt = fixture.store.get_agent_activity_receipt(
                fixture.run_id,
                fixture.attempt_id,
            )
            assert receipt is not None
            self.assertEqual(
                receipt.attempt_status,
                AttemptStatus.OUTCOME_UNKNOWN,
            )
            self.assertEqual(
                receipt.verification,
                AgentActivityVerification.UNVERIFIED,
            )
            self.assertFalse(receipt.internal_tool_receipts_complete)
            self.assertEqual(receipt.observed_tool_results, 1)

    def test_receipt_payload_tamper_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            fixture.execute(lambda _task: _completed_result())
            with sqlite3.connect(fixture.store.path) as conn:
                conn.execute("DROP TRIGGER domain_events_no_update")
                row = conn.execute(
                    """
                    SELECT event_id, payload_json FROM domain_events
                    WHERE attempt_id = ? AND event_type = 'attempt.succeeded'
                    """,
                    (fixture.attempt_id,),
                ).fetchone()
                assert row is not None
                payload = json.loads(row[1])
                payload["agent_activity_receipt"]["result_digest"] = (
                    "0" * 64
                )
                conn.execute(
                    """
                    UPDATE domain_events
                    SET payload_json = ?, content_digest = ?
                    WHERE event_id = ?
                    """,
                    (
                        json.dumps(
                            payload,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        _content_digest(payload),
                        row[0],
                    ),
                )

            with self.assertRaisesRegex(
                StoreSchemaError,
                "AgentActivityReceipt",
            ):
                fixture.store.get_agent_activity_receipt(
                    fixture.run_id,
                    fixture.attempt_id,
                )
            called = False

            def should_not_run(_task: str) -> dict:
                nonlocal called
                called = True
                return _completed_result()

            with self.assertRaisesRegex(
                DurabilityError,
                "agent_activity_receipt.read",
            ):
                fixture.execute(should_not_run)
            self.assertFalse(called)

    def test_partial_receipt_fields_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            fixture.execute(lambda _task: _completed_result())
            with sqlite3.connect(fixture.store.path) as conn:
                conn.execute("DROP TRIGGER domain_events_no_update")
                row = conn.execute(
                    """
                    SELECT event_id, payload_json FROM domain_events
                    WHERE attempt_id = ? AND event_type = 'attempt.succeeded'
                    """,
                    (fixture.attempt_id,),
                ).fetchone()
                assert row is not None
                payload = json.loads(row[1])
                payload.pop("agent_activity_receipt")
                conn.execute(
                    """
                    UPDATE domain_events
                    SET payload_json = ?, content_digest = ?
                    WHERE event_id = ?
                    """,
                    (
                        json.dumps(
                            payload,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        _content_digest(payload),
                        row[0],
                    ),
                )

            with self.assertRaisesRegex(
                StoreSchemaError,
                "AgentActivityReceipt",
            ):
                fixture.store.get_agent_activity_receipt(
                    fixture.run_id,
                    fixture.attempt_id,
                )

    def test_missing_terminal_event_is_not_legacy_compatibility(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            fixture.execute(lambda _task: _completed_result())
            with sqlite3.connect(fixture.store.path) as conn:
                conn.execute("DROP TRIGGER domain_events_no_delete")
                conn.execute(
                    """
                    DELETE FROM domain_events
                    WHERE attempt_id = ? AND event_type = 'attempt.succeeded'
                    """,
                    (fixture.attempt_id,),
                )

            with self.assertRaisesRegex(
                StoreSchemaError,
                "missing its Domain Event",
            ):
                fixture.store.get_agent_activity_receipt(
                    fixture.run_id,
                    fixture.attempt_id,
                )
            called = False

            def should_not_run(_task: str) -> dict:
                nonlocal called
                called = True
                return _completed_result()

            with self.assertRaises(DurabilityError):
                fixture.execute(should_not_run)
            self.assertFalse(called)

    def test_fully_absent_receipt_remains_legacy_compatible(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            fixture.execute(lambda _task: _completed_result())
            with sqlite3.connect(fixture.store.path) as conn:
                conn.execute("DROP TRIGGER domain_events_no_update")
                row = conn.execute(
                    """
                    SELECT event_id, payload_json FROM domain_events
                    WHERE attempt_id = ? AND event_type = 'attempt.succeeded'
                    """,
                    (fixture.attempt_id,),
                ).fetchone()
                assert row is not None
                payload = json.loads(row[1])
                payload.pop("agent_activity_receipt")
                payload.pop("agent_activity_receipt_digest")
                conn.execute(
                    """
                    UPDATE domain_events
                    SET payload_json = ?, content_digest = ?
                    WHERE event_id = ?
                    """,
                    (
                        json.dumps(
                            payload,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        _content_digest(payload),
                        row[0],
                    ),
                )

            self.assertIsNone(
                fixture.store.get_agent_activity_receipt(
                    fixture.run_id,
                    fixture.attempt_id,
                )
            )
            called = False

            def should_not_run(_task: str) -> dict:
                nonlocal called
                called = True
                return _completed_result()

            recovered = fixture.execute(should_not_run)
            self.assertTrue(recovered["recovered"])
            self.assertFalse(called)

    def test_terminal_commit_failure_records_unverified_unknown_receipt(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            injected = False

            def fail_after_idempotency(stage: str) -> None:
                nonlocal injected
                if (
                    stage == "complete.after_idempotency"
                    and not injected
                ):
                    injected = True
                    raise OSError("injected")

            fixture.store._fault = fail_after_idempotency
            with self.assertRaises(DurabilityError):
                fixture.execute(lambda _task: _completed_result())

            receipt = fixture.store.get_agent_activity_receipt(
                fixture.run_id,
                fixture.attempt_id,
            )
            attempt = fixture.store.get_attempt(fixture.attempt_id)
            assert receipt is not None
            assert attempt is not None
            self.assertEqual(
                receipt.attempt_status,
                AttemptStatus.OUTCOME_UNKNOWN,
            )
            self.assertEqual(
                receipt.verification,
                AgentActivityVerification.UNVERIFIED,
            )
            self.assertEqual(receipt.observed_tool_results, 1)
            self.assertEqual(
                receipt.result_digest,
                canonical_agent_result_digest(attempt.result),
            )

    def test_post_commit_response_loss_requires_durable_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            original_complete = fixture.store.complete_activity
            raised = False

            def complete_then_lose_response(*args, **kwargs):
                nonlocal raised
                committed = original_complete(*args, **kwargs)
                if not raised:
                    raised = True
                    raise OSError("response lost after commit")
                return committed

            with mock.patch.object(
                fixture.store,
                "complete_activity",
                side_effect=complete_then_lose_response,
            ):
                result = fixture.execute(
                    lambda _task: _completed_result()
                )

            self.assertEqual(result, _completed_result())
            self.assertTrue(raised)
            self.assertIsNotNone(
                fixture.store.get_agent_activity_receipt(
                    fixture.run_id,
                    fixture.attempt_id,
                )
            )

    def test_new_post_commit_completion_cannot_downgrade_to_legacy(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            original_complete = fixture.store.complete_activity

            def commit_remove_receipt_then_lose_response(*args, **kwargs):
                committed = original_complete(*args, **kwargs)
                with sqlite3.connect(fixture.store.path) as conn:
                    conn.execute(
                        "DROP TRIGGER domain_events_no_update"
                    )
                    row = conn.execute(
                        """
                        SELECT event_id, payload_json FROM domain_events
                        WHERE attempt_id = ?
                          AND event_type = 'attempt.succeeded'
                        """,
                        (fixture.attempt_id,),
                    ).fetchone()
                    assert row is not None
                    payload = json.loads(row[1])
                    payload.pop("agent_activity_receipt")
                    payload.pop("agent_activity_receipt_digest")
                    conn.execute(
                        """
                        UPDATE domain_events
                        SET payload_json = ?, content_digest = ?
                        WHERE event_id = ?
                        """,
                        (
                            json.dumps(
                                payload,
                                ensure_ascii=False,
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                            _content_digest(payload),
                            row[0],
                        ),
                    )
                raise OSError("response and receipt lost")

            with mock.patch.object(
                fixture.store,
                "complete_activity",
                side_effect=(
                    commit_remove_receipt_then_lose_response
                ),
            ):
                with self.assertRaisesRegex(
                    DurabilityError,
                    "agent_activity_receipt.read",
                ):
                    fixture.execute(
                        lambda _task: _completed_result()
                    )

    def test_idempotency_result_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            fixture.execute(lambda _task: _completed_result())
            attempt = fixture.store.get_attempt(fixture.attempt_id)
            assert attempt is not None
            with sqlite3.connect(fixture.store.path) as conn:
                conn.execute(
                    """
                    UPDATE idempotency_records
                    SET result_json = '{}'
                    WHERE run_id = ? AND key = ?
                    """,
                    (fixture.run_id, attempt.idempotency_key),
                )

            with self.assertRaisesRegex(
                StoreSchemaError,
                "AgentActivityReceipt",
            ):
                fixture.store.get_agent_activity_receipt(
                    fixture.run_id,
                    fixture.attempt_id,
                )

    def test_receipt_reads_one_sqlite_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            fixture = StoreFixture(Path(raw))
            fixture.execute(lambda _task: _completed_result())
            original_connect = fixture.store._connect
            mutated = False

            def mutate_after_event_read() -> None:
                nonlocal mutated
                if mutated:
                    return
                mutated = True
                with sqlite3.connect(fixture.store.path) as writer:
                    writer.execute(
                        """
                        UPDATE idempotency_records
                        SET request_hash = ?
                        WHERE run_id = ?
                        """,
                        ("0" * 64, fixture.run_id),
                    )

            class CursorProxy:
                def __init__(self, cursor) -> None:
                    self._cursor = cursor

                def fetchone(self):
                    row = self._cursor.fetchone()
                    mutate_after_event_read()
                    return row

            class ConnectionProxy:
                def __init__(self) -> None:
                    self._connection = original_connect()

                def execute(self, sql, parameters=()):
                    cursor = self._connection.execute(sql, parameters)
                    if "FROM domain_events" in sql:
                        return CursorProxy(cursor)
                    return cursor

                def close(self) -> None:
                    self._connection.close()

            with mock.patch.object(
                fixture.store,
                "_connect",
                side_effect=ConnectionProxy,
            ):
                receipt = fixture.store.get_agent_activity_receipt(
                    fixture.run_id,
                    fixture.attempt_id,
                )

            self.assertIsNotNone(receipt)
            self.assertTrue(mutated)
            with self.assertRaises(StoreSchemaError):
                fixture.store.get_agent_activity_receipt(
                    fixture.run_id,
                    fixture.attempt_id,
                )


if __name__ == "__main__":
    unittest.main()
