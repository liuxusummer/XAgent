"""Best-effort telemetry projection for committed orchestration events.

Domain Events remain the execution truth.  This module only reads committed
events and emits a bounded, payload-free projection after the transaction has
finished.  Export failure must never mutate or roll back a Run.
"""

from __future__ import annotations

import hashlib
import importlib
import math
import re
import sqlite3
import threading
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, Sequence, runtime_checkable

from src.core.telemetry import Event, EventSink

from .models import EventRecord

_SAFE_EVENT_TYPE = re.compile(r"^[a-z][a-z0-9_]{0,63}\.[a-z][a-z0-9_]{0,63}$")
_SAFE_FAMILIES = frozenset(
    {
        "approval",
        "artifact",
        "activity",
        "attempt",
        "audit",
        "input",
        "lease",
        "node",
        "run",
        "tool",
    }
)
_SAFE_ACTIONS = frozenset(
    {
        "abandoned",
        "acquired",
        "cancel_requested",
        "cancelled",
        "cancelling",
        "claimed",
        "commit_rejected",
        "completed",
        "created",
        "expired",
        "failed",
        "note",
        "outcome_unknown",
        "pause_requested",
        "paused",
        "pausing",
        "ready",
        "receipt_recorded",
        "recovery_resolved",
        "released",
        "requested",
        "resolved",
        "resumed",
        "running",
        "scheduled",
        "skipped",
        "started",
        "succeeded",
        "timed_out",
        "waiting_approval",
        "waiting_input",
        "waiting_recovery",
        "waiting_retry",
    }
)
_TERMINAL_SUFFIXES = frozenset(
    {
        "abandoned",
        "cancelled",
        "completed",
        "failed",
        "outcome_unknown",
        "skipped",
        "succeeded",
        "timed_out",
    }
)


class TelemetryProjectionError(RuntimeError):
    """A committed event cannot be converted to the telemetry contract."""


@runtime_checkable
class DomainEventReader(Protocol):
    def list_events(
        self,
        run_id: str,
        *,
        after_seq: int = 0,
        limit: int = 1_000,
    ) -> Sequence[EventRecord]: ...


@dataclass(frozen=True, slots=True)
class ExportProgress:
    """Local scan progress; ``complete`` never means remote acknowledgement."""

    run_id: str
    attempted: int
    last_sequence: int
    complete: bool
    export_failures: int = 0
    delivery_semantics: str = "attempted"
    remote_acknowledged: None = None

    @property
    def last_attempted_sequence(self) -> int:
        return self.last_sequence

    @property
    def scan_complete(self) -> bool:
        return self.complete


@dataclass(frozen=True, slots=True)
class OTelEventProjection:
    """A bounded OpenTelemetry projection containing no raw Domain IDs."""

    span_name: str
    timestamp_ns: int
    attributes: dict[str, str | int | bool]


@runtime_checkable
class OTelTracer(Protocol):
    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, str | int | bool],
        start_time: int,
    ) -> Any: ...


@runtime_checkable
class OTelMeter(Protocol):
    def create_counter(
        self,
        name: str,
        *,
        unit: str,
        description: str,
    ) -> Any: ...


class OTelDomainExporter:
    """Best-effort adapter for injected OpenTelemetry tracer/meter objects."""

    def __init__(
        self,
        *,
        tracer: OTelTracer | None = None,
        meter: OTelMeter | None = None,
    ) -> None:
        self.tracer = tracer
        self.meter = meter
        self._counter = None
        if meter is not None:
            try:
                self._counter = meter.create_counter(
                    "xagent.orchestration.domain_events",
                    unit="{event}",
                    description="Attempted exports of committed orchestration events",
                )
            except Exception:
                self._counter = None

    def emit(self, projection: OTelEventProjection) -> None:
        """Emit one sanitized projection; callers contain any exporter error."""

        errors: list[Exception] = []
        if self.tracer is not None:
            try:
                span = self.tracer.start_span(
                    projection.span_name,
                    attributes=projection.attributes,
                    start_time=projection.timestamp_ns,
                )
                span.end(end_time=projection.timestamp_ns)
            except Exception as exc:
                errors.append(exc)
        if self._counter is not None:
            try:
                self._counter.add(1, attributes=projection.attributes)
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise RuntimeError("OpenTelemetry export failed") from errors[0]


class SQLiteTelemetryCursorStore:
    """Durable cursor for *attempted* best-effort telemetry delivery.

    The cursor advances after local exporters have been invoked, even when an
    exporter raises.  It records at-most-one local delivery attempt across
    sequential restarts, never that a remote backend acknowledged the event.
    It is observability state, never execution state.
    """

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path).expanduser().resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_lock = threading.Lock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.database_path),
            timeout=10.0,
            isolation_level=None,
        )
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    def _initialize(self) -> None:
        with self._initialize_lock, closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS telemetry_cursors (
                    exporter_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    last_sequence INTEGER NOT NULL
                        CHECK(last_sequence >= 0),
                    updated_at REAL NOT NULL,
                    PRIMARY KEY(exporter_id, run_id)
                )
                """
            )

    def get(self, exporter_id: str, run_id: str) -> int:
        exporter_id = _required_identifier(exporter_id, "exporter_id")
        run_id = _required_identifier(run_id, "run_id")
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT last_sequence
                FROM telemetry_cursors
                WHERE exporter_id = ? AND run_id = ?
                """,
                (exporter_id, run_id),
            ).fetchone()
        return 0 if row is None else int(row[0])

    def advance(
        self,
        exporter_id: str,
        run_id: str,
        sequence: int,
        *,
        now: float | None = None,
    ) -> int:
        exporter_id = _required_identifier(exporter_id, "exporter_id")
        run_id = _required_identifier(run_id, "run_id")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
            raise ValueError("sequence must be a positive integer")
        timestamp = float(time.time() if now is None else now)
        if not math.isfinite(timestamp) or timestamp < 0:
            raise ValueError("now must be a finite non-negative timestamp")
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    """
                    INSERT INTO telemetry_cursors(
                        exporter_id, run_id, last_sequence, updated_at
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(exporter_id, run_id) DO UPDATE SET
                        last_sequence = MAX(
                            telemetry_cursors.last_sequence,
                            excluded.last_sequence
                        ),
                        updated_at = excluded.updated_at
                    """,
                    (exporter_id, run_id, sequence, timestamp),
                )
                row = connection.execute(
                    """
                    SELECT last_sequence
                    FROM telemetry_cursors
                    WHERE exporter_id = ? AND run_id = ?
                    """,
                    (exporter_id, run_id),
                ).fetchone()
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        assert row is not None
        return int(row[0])


class DomainTelemetryBridge:
    """Project committed Domain Events into the existing EventSink surface."""

    def __init__(
        self,
        reader: DomainEventReader,
        sink: EventSink,
        cursor_store: SQLiteTelemetryCursorStore,
        *,
        exporter_id: str = "orchestration",
        page_size: int = 500,
        otel_exporter: OTelDomainExporter | None = None,
    ) -> None:
        if page_size < 1 or page_size > 10_000:
            raise ValueError("page_size must be between 1 and 10000")
        self.reader = reader
        self.sink = sink
        self.cursor_store = cursor_store
        self.exporter_id = _required_identifier(exporter_id, "exporter_id")
        self.page_size = int(page_size)
        self.otel_exporter = otel_exporter

    def export_run(self, run_id: str) -> ExportProgress:
        """Attempt unseen events; only reader/projection/cursor faults stop scanning."""

        run_id = _required_identifier(run_id, "run_id")
        cursor = self.cursor_store.get(self.exporter_id, run_id)
        attempted = 0
        export_failures = 0
        while True:
            try:
                batch = tuple(
                    self.reader.list_events(
                        run_id,
                        after_seq=cursor,
                        limit=self.page_size,
                    )
                )
            except Exception:
                return ExportProgress(
                    run_id,
                    attempted,
                    cursor,
                    False,
                    export_failures,
                )
            if not batch:
                return ExportProgress(
                    run_id,
                    attempted,
                    cursor,
                    True,
                    export_failures,
                )
            for event in batch:
                try:
                    projected = project_domain_event(event)
                    otel_projection = project_domain_event_to_otel(event)
                except Exception:
                    return ExportProgress(
                        run_id,
                        attempted,
                        cursor,
                        False,
                        export_failures,
                    )
                export_failed = False
                try:
                    self.sink.emit(projected)
                except Exception:
                    export_failed = True
                if self.otel_exporter is not None:
                    try:
                        self.otel_exporter.emit(otel_projection)
                    except Exception:
                        export_failed = True
                attempted += 1
                if export_failed:
                    export_failures += 1
                try:
                    cursor = self.cursor_store.advance(
                        self.exporter_id,
                        run_id,
                        event.seq,
                    )
                except Exception:
                    return ExportProgress(
                        run_id,
                        attempted,
                        cursor,
                        False,
                        export_failures,
                    )
            if len(batch) < self.page_size:
                return ExportProgress(
                    run_id,
                    attempted,
                    cursor,
                    True,
                    export_failures,
                )


def project_domain_event(event: EventRecord) -> Event:
    """Convert one committed event without forwarding its canonical payload.

    Unknown extension names are projected to one constant label.  This keeps a
    caller from smuggling arbitrary text through ``event_type`` while allowing
    telemetry scanning to continue past an extension event.
    """

    if not isinstance(event, EventRecord):
        raise TelemetryProjectionError("event must be an EventRecord")
    event_name = "orchestration.unknown"
    family = "unknown"
    action = "unknown"
    if _SAFE_EVENT_TYPE.fullmatch(event.event_type):
        candidate_family, candidate_action = event.event_type.split(".", 1)
        if candidate_family in _SAFE_FAMILIES and candidate_action in _SAFE_ACTIONS:
            event_name = event.event_type
            family = candidate_family
            action = candidate_action
    return Event(
        session_id=_pseudonymous_id("run", event.run_id),
        turn=event.seq,
        kind="orchestration_event",
        name=event_name,
        ts=event.occurred_at,
        data={
            "schema_version": event.schema_version,
            "sequence": event.seq,
            "family": family,
            "action": action,
            "has_node": event.node_id is not None,
            "has_attempt": event.attempt_id is not None,
            "terminal": action in _TERMINAL_SUFFIXES,
        },
    )


def project_domain_event_to_otel(event: EventRecord) -> OTelEventProjection:
    """Purely map a committed event to bounded OTel names and attributes."""

    projected = project_domain_event(event)
    timestamp = float(projected.ts)
    if not math.isfinite(timestamp) or timestamp < 0:
        raise TelemetryProjectionError("event timestamp must be finite")
    return OTelEventProjection(
        span_name=f"xagent.domain.{projected.name}",
        timestamp_ns=int(timestamp * 1_000_000_000),
        attributes={
            "xagent.schema_version": int(projected.data["schema_version"]),
            "xagent.event.sequence": int(projected.data["sequence"]),
            "xagent.event.family": str(projected.data["family"]),
            "xagent.event.action": str(projected.data["action"]),
            "xagent.event.terminal": bool(projected.data["terminal"]),
            "xagent.event.has_node": bool(projected.data["has_node"]),
            "xagent.event.has_attempt": bool(projected.data["has_attempt"]),
            "xagent.run.correlation_id": projected.session_id,
        },
    )


def create_otel_domain_exporter(
    *,
    instrumentation_name: str = "xagent.orchestration",
) -> OTelDomainExporter | None:
    """Create an OTel adapter lazily; absence or SDK failure is a safe no-op."""

    try:
        trace_module = importlib.import_module("opentelemetry.trace")
        metrics_module = importlib.import_module("opentelemetry.metrics")
        tracer = trace_module.get_tracer(instrumentation_name)
        meter = metrics_module.get_meter(instrumentation_name)
        return OTelDomainExporter(tracer=tracer, meter=meter)
    except Exception:
        return None


def _pseudonymous_id(kind: str, value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"{kind}_{digest[:24]}"


def _required_identifier(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise ValueError(f"{field} must be a non-empty string up to 512 characters")
    return value


__all__ = [
    "DomainEventReader",
    "DomainTelemetryBridge",
    "ExportProgress",
    "OTelDomainExporter",
    "OTelEventProjection",
    "OTelMeter",
    "OTelTracer",
    "SQLiteTelemetryCursorStore",
    "TelemetryProjectionError",
    "create_otel_domain_exporter",
    "project_domain_event",
    "project_domain_event_to_otel",
]
