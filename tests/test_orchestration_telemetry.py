from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.orchestration.models import EventRecord, RunRecord
from src.orchestration.store import DurableRunStore
from src.orchestration.telemetry import (
    DomainTelemetryBridge,
    OTelDomainExporter,
    SQLiteTelemetryCursorStore,
    create_otel_domain_exporter,
    project_domain_event,
    project_domain_event_to_otel,
)


class _Reader:
    def __init__(self, events: list[EventRecord]) -> None:
        self.events = events
        self.calls = 0

    def list_events(self, run_id: str, *, after_seq: int = 0, limit: int = 1000):
        self.calls += 1
        return [
            event
            for event in self.events
            if event.run_id == run_id and event.seq > after_seq
        ][:limit]


class _Sink:
    def __init__(self, *, fail_at: int | None = None) -> None:
        self.events = []
        self.fail_at = fail_at

    def emit(self, event) -> None:
        if self.fail_at is not None and len(self.events) == self.fail_at:
            raise RuntimeError("export unavailable")
        self.events.append(event)

    def close(self) -> None:
        return None


class _Span:
    def __init__(self) -> None:
        self.end_time = None

    def end(self, *, end_time: int) -> None:
        self.end_time = end_time


class _Tracer:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.started = []

    def start_span(self, name, *, attributes, start_time):
        if self.fail:
            raise RuntimeError("trace exporter unavailable")
        span = _Span()
        self.started.append((name, attributes, start_time, span))
        return span


class _Counter:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.added = []

    def add(self, value, *, attributes) -> None:
        if self.fail:
            raise RuntimeError("metric exporter unavailable")
        self.added.append((value, attributes))


class _Meter:
    def __init__(self, *, fail: bool = False) -> None:
        self.counter = _Counter(fail=fail)

    def create_counter(self, _name, *, unit, description):
        self.unit = unit
        self.description = description
        return self.counter


def _event(sequence: int, *, payload=None) -> EventRecord:
    return EventRecord(
        run_id="run-telemetry",
        seq=sequence,
        event_type="attempt.succeeded" if sequence == 2 else "run.created",
        node_id="secret-node" if sequence == 2 else None,
        attempt_id="secret-attempt" if sequence == 2 else None,
        payload=payload or {},
        occurred_at=float(sequence),
    )


class OrchestrationTelemetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_projection_forwards_only_bounded_metadata(self) -> None:
        token = "SUPER-SECRET-LOW-ENTROPY"
        event = _event(
            2,
            payload={
                "projection": {
                    "run": {"input": token},
                    "node": {"output": token},
                },
                "result": token,
            },
        )

        projected = project_domain_event(event)
        rendered = repr(projected)

        self.assertNotIn(token, rendered)
        self.assertNotIn("secret-node", rendered)
        self.assertNotIn("secret-attempt", rendered)
        self.assertNotIn("run-telemetry", rendered)
        self.assertTrue(projected.session_id.startswith("run_"))
        self.assertEqual(projected.name, "attempt.succeeded")
        self.assertTrue(projected.data["terminal"])

    def test_unknown_event_type_is_redacted_without_blocking_scan(self) -> None:
        event = EventRecord(
            run_id="run-telemetry",
            seq=1,
            event_type="secret.low_entropy_token",
        )
        projected = project_domain_event(event)
        self.assertEqual(projected.name, "orchestration.unknown")
        self.assertEqual(projected.data["family"], "unknown")
        self.assertNotIn("low_entropy_token", repr(projected))

    def test_cursor_survives_restart_and_never_moves_backwards(self) -> None:
        path = self.root / "cursor.sqlite3"
        first = SQLiteTelemetryCursorStore(path)
        self.assertEqual(first.get("exporter", "run"), 0)
        self.assertEqual(first.advance("exporter", "run", 4, now=1), 4)
        self.assertEqual(first.advance("exporter", "run", 2, now=2), 4)
        self.assertEqual(
            SQLiteTelemetryCursorStore(path).get("exporter", "run"),
            4,
        )

    def test_bridge_pages_committed_events_and_resumes_without_duplicates(self) -> None:
        reader = _Reader([_event(1), _event(2)])
        sink = _Sink()
        cursor = SQLiteTelemetryCursorStore(self.root / "cursor.sqlite3")
        bridge = DomainTelemetryBridge(
            reader,
            sink,
            cursor,
            exporter_id="local",
            page_size=1,
        )

        first = bridge.export_run("run-telemetry")
        second = DomainTelemetryBridge(
            reader,
            sink,
            SQLiteTelemetryCursorStore(self.root / "cursor.sqlite3"),
            exporter_id="local",
            page_size=1,
        ).export_run("run-telemetry")

        self.assertTrue(first.complete)
        self.assertEqual(first.attempted, 2)
        self.assertTrue(second.complete)
        self.assertEqual(second.attempted, 0)
        self.assertEqual([event.turn for event in sink.events], [1, 2])

    def test_export_failure_advances_attempted_cursor_without_claiming_ack(self) -> None:
        reader = _Reader([_event(1), _event(2)])
        cursor = SQLiteTelemetryCursorStore(self.root / "cursor.sqlite3")
        failing = DomainTelemetryBridge(
            reader,
            _Sink(fail_at=1),
            cursor,
            exporter_id="remote",
        )

        progress = failing.export_run("run-telemetry")

        self.assertTrue(progress.complete)
        self.assertEqual(progress.attempted, 2)
        self.assertEqual(progress.export_failures, 1)
        self.assertEqual(progress.delivery_semantics, "attempted")
        self.assertIsNone(progress.remote_acknowledged)
        self.assertEqual(cursor.get("remote", "run-telemetry"), 2)

        recovered_sink = _Sink()
        recovered = DomainTelemetryBridge(
            reader,
            recovered_sink,
            cursor,
            exporter_id="remote",
        ).export_run("run-telemetry")
        self.assertTrue(recovered.complete)
        self.assertEqual(recovered.attempted, 0)
        self.assertEqual(recovered_sink.events, [])

    def test_otel_mapping_is_bounded_and_sensitive_artifacts_are_not_exported(self) -> None:
        token = "SECRET-ARTIFACT-CONTENT"
        event = _event(
            2,
            payload={
                "artifact_refs": [
                    {
                        "artifact_id": "secret-artifact-id",
                        "sensitivity": "secret",
                        "uri": token,
                    },
                    {
                        "artifact_id": "sensitive-artifact-id",
                        "sensitivity": "sensitive",
                        "uri": token,
                    },
                ]
            },
        )

        projection = project_domain_event_to_otel(event)
        rendered = repr(projection)

        self.assertEqual(projection.span_name, "xagent.domain.attempt.succeeded")
        self.assertNotIn(token, rendered)
        self.assertNotIn("secret-artifact-id", rendered)
        self.assertNotIn("secret-node", rendered)
        self.assertNotIn("secret-attempt", rendered)
        self.assertNotIn("run-telemetry", rendered)
        self.assertEqual(
            set(projection.attributes),
            {
                "xagent.schema_version",
                "xagent.event.sequence",
                "xagent.event.family",
                "xagent.event.action",
                "xagent.event.terminal",
                "xagent.event.has_node",
                "xagent.event.has_attempt",
                "xagent.run.correlation_id",
            },
        )

    def test_event_sink_and_otel_failures_do_not_mutate_committed_run(self) -> None:
        store = DurableRunStore(self.root / "orchestration.sqlite3")
        before = store.create_run(
            RunRecord(
                "run-failure",
                "workflow",
                definition_digest="a" * 64,
                created_at=1,
                updated_at=1,
            )
        )
        cursor = SQLiteTelemetryCursorStore(self.root / "cursor.sqlite3")
        bridge = DomainTelemetryBridge(
            store,
            _Sink(fail_at=0),
            cursor,
            exporter_id="all-fail",
            otel_exporter=OTelDomainExporter(
                tracer=_Tracer(fail=True),
                meter=_Meter(fail=True),
            ),
        )

        first = bridge.export_run(before.run_id)
        second = bridge.export_run(before.run_id)

        self.assertTrue(first.complete)
        self.assertEqual(first.attempted, 1)
        self.assertEqual(first.export_failures, 1)
        self.assertIsNone(first.remote_acknowledged)
        self.assertEqual(second.attempted, 0)
        self.assertEqual(cursor.get("all-fail", before.run_id), 1)
        self.assertEqual(store.get_run(before.run_id), before)
        self.assertTrue(store.verify_projections(before.run_id))

    def test_injected_otel_receives_sanitized_mapping(self) -> None:
        tracer = _Tracer()
        meter = _Meter()
        cursor = SQLiteTelemetryCursorStore(self.root / "cursor.sqlite3")
        progress = DomainTelemetryBridge(
            _Reader([_event(1)]),
            _Sink(),
            cursor,
            otel_exporter=OTelDomainExporter(tracer=tracer, meter=meter),
        ).export_run("run-telemetry")

        self.assertTrue(progress.complete)
        self.assertEqual(len(tracer.started), 1)
        self.assertEqual(len(meter.counter.added), 1)
        rendered = repr((tracer.started, meter.counter.added))
        self.assertNotIn("run-telemetry", rendered)
        self.assertNotIn("secret-node", rendered)

    def test_otel_factory_is_none_when_optional_dependency_is_absent(self) -> None:
        with patch(
            "src.orchestration.telemetry.importlib.import_module",
            side_effect=ModuleNotFoundError("opentelemetry"),
        ):
            self.assertIsNone(create_otel_domain_exporter())


if __name__ == "__main__":
    unittest.main()
