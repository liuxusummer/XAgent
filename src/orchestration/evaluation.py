"""Deterministic reliability evaluation for durable orchestration.

Evaluation is intentionally runner-injected and network-free.  A runner creates
an :class:`EvaluationContext` from a controlled scenario; invariants only
inspect the supplied projections, events, database bytes, and trace bytes.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Sequence

from .models import JsonValue, normalize_json
from .replay import ProjectionDiff, ReplaySnapshot

_RUN_TERMINAL_EVENTS = frozenset(
    {"run.completed", "run.failed", "run.cancelled"}
)
_NODE_TERMINAL_EVENTS = frozenset(
    {"node.succeeded", "node.failed", "node.cancelled", "node.skipped"}
)
_ATTEMPT_TERMINAL_EVENTS = frozenset(
    {
        "attempt.succeeded",
        "attempt.failed",
        "attempt.timed_out",
        "attempt.cancelled",
        "attempt.abandoned",
        "attempt.outcome_unknown",
    }
)


@dataclass(frozen=True, slots=True)
class Scenario:
    """One controlled execution or fault-injection case."""

    scenario_id: str
    run_id: str
    fault_point: str | None = None
    parameters: dict[str, JsonValue] = field(default_factory=dict)
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.scenario_id.strip():
            raise ValueError("scenario_id must not be empty")
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        object.__setattr__(
            self,
            "parameters",
            normalize_json(self.parameters, "scenario.parameters"),
        )
        object.__setattr__(
            self,
            "tags",
            tuple(sorted({str(tag) for tag in self.tags if str(tag)})),
        )

    def to_dict(self) -> dict[str, JsonValue]:
        parameter_bytes = _canonical_bytes(self.parameters)
        return {
            "scenario_id": self.scenario_id,
            "run_id": self.run_id,
            "fault_point": self.fault_point,
            "parameters_redacted": True,
            "parameter_size": len(parameter_bytes),
            "tags": list(self.tags),
        }


@dataclass(frozen=True, slots=True)
class EvaluationContext:
    """All local evidence supplied by a scenario runner."""

    scenario: Scenario
    events: tuple[Any, ...] = ()
    snapshot: ReplaySnapshot | None = None
    projection_diffs: tuple[ProjectionDiff, ...] = ()
    sqlite_bytes: bytes = b""
    trace_bytes: bytes = b""
    secret_tokens: tuple[str | bytes, ...] = ()


@dataclass(frozen=True, slots=True)
class InvariantFailure:
    code: str
    invariant: str
    message: str
    details: dict[str, JsonValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "code": self.code,
            "invariant": self.invariant,
            "message": self.message,
            "details": normalize_json(self.details),
        }


InvariantCheck = Callable[[EvaluationContext], Sequence[InvariantFailure]]


@dataclass(frozen=True, slots=True)
class Invariant:
    """A named, pure check over runner-supplied evidence."""

    name: str
    check: InvariantCheck

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("invariant name must not be empty")


@dataclass(frozen=True, slots=True)
class EvalResult:
    scenario_id: str
    run_id: str
    fault_point: str | None
    failures: tuple[InvariantFailure, ...] = ()

    @property
    def passed(self) -> bool:
        return not self.failures

    @property
    def failure_codes(self) -> tuple[str, ...]:
        return tuple(sorted({failure.code for failure in self.failures}))

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "scenario_id": self.scenario_id,
            "run_id": self.run_id,
            "fault_point": self.fault_point,
            "passed": self.passed,
            "failure_codes": list(self.failure_codes),
            "failures": [failure.to_dict() for failure in self.failures],
        }


@dataclass(frozen=True, slots=True)
class SuiteReport:
    suite_name: str
    results: tuple[EvalResult, ...]

    @property
    def passed_count(self) -> int:
        return sum(result.passed for result in self.results)

    @property
    def failed_count(self) -> int:
        return len(self.results) - self.passed_count

    @property
    def pass_rate(self) -> float:
        if not self.results:
            return 1.0
        return self.passed_count / len(self.results)

    def to_dict(self) -> dict[str, JsonValue]:
        code_counts = Counter(
            failure.code
            for result in self.results
            for failure in result.failures
        )
        return {
            "suite_name": self.suite_name,
            "summary": {
                "total": len(self.results),
                "passed": self.passed_count,
                "failed": self.failed_count,
                "pass_rate": round(self.pass_rate, 6),
                "failure_codes": {
                    code: code_counts[code] for code in sorted(code_counts)
                },
            },
            "results": [result.to_dict() for result in self.results],
        }

    def to_json(self) -> str:
        return _canonical_bytes(self.to_dict()).decode("utf-8")

    def to_junit_dict(self) -> dict[str, JsonValue]:
        cases: list[dict[str, JsonValue]] = []
        for result in self.results:
            case: dict[str, JsonValue] = {
                "name": result.scenario_id,
                "classname": self.suite_name,
                "status": "passed" if result.passed else "failed",
            }
            if result.failures:
                case["failures"] = [
                    {
                        "type": failure.code,
                        "message": failure.message,
                    }
                    for failure in result.failures
                ]
            cases.append(case)
        return {
            "testsuite": {
                "name": self.suite_name,
                "tests": len(self.results),
                "failures": self.failed_count,
                "testcase": cases,
            }
        }


@dataclass(frozen=True, slots=True)
class ReliabilityEvidence:
    """Explicit runner counters; telemetry is deliberately not an input.

    Each numerator/denominator pair is optional.  Missing or zero-denominator
    evidence produces an unknown metric, never a fabricated zero.
    """

    recovery_successes: int | None = None
    recovery_attempts: int | None = None
    duplicate_visible_side_effects: int | None = None
    visible_side_effect_checks: int | None = None
    outcome_unknown_attempts: int | None = None
    terminal_attempt_checks: int | None = None
    projection_replay_divergences: int | None = None
    projection_replay_checks: int | None = None
    cancellation_leaks: int | None = None
    cancellation_checks: int | None = None
    stale_worker_commit_rejections: int | None = None
    stale_worker_commit_attempts: int | None = None
    resume_latencies_ms: tuple[float, ...] | None = None

    def __post_init__(self) -> None:
        pairs = (
            ("recovery_successes", "recovery_attempts"),
            ("duplicate_visible_side_effects", "visible_side_effect_checks"),
            ("outcome_unknown_attempts", "terminal_attempt_checks"),
            ("projection_replay_divergences", "projection_replay_checks"),
            ("cancellation_leaks", "cancellation_checks"),
            (
                "stale_worker_commit_rejections",
                "stale_worker_commit_attempts",
            ),
        )
        for numerator_name, denominator_name in pairs:
            numerator = _optional_counter(getattr(self, numerator_name), numerator_name)
            denominator = _optional_counter(
                getattr(self, denominator_name),
                denominator_name,
            )
            object.__setattr__(self, numerator_name, numerator)
            object.__setattr__(self, denominator_name, denominator)
            if (
                numerator is not None
                and denominator is not None
                and numerator > denominator
            ):
                raise ValueError(
                    f"{numerator_name} must not exceed {denominator_name}"
                )

        samples = self.resume_latencies_ms
        if samples is None:
            return
        normalized: list[float] = []
        for sample in samples:
            if isinstance(sample, bool):
                raise ValueError("resume latency samples must be finite milliseconds")
            try:
                latency = float(sample)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "resume latency samples must be finite milliseconds"
                ) from exc
            if not math.isfinite(latency) or latency < 0:
                raise ValueError(
                    "resume latency samples must be finite milliseconds"
                )
            normalized.append(latency)
        object.__setattr__(self, "resume_latencies_ms", tuple(normalized))


@dataclass(frozen=True, slots=True)
class ReliabilityMetric:
    value: float | None
    unit: str
    numerator: int | None = None
    denominator: int | None = None
    sample_count: int | None = None
    unknown_reason: str | None = None

    @property
    def known(self) -> bool:
        return self.value is not None

    def to_dict(self) -> dict[str, JsonValue]:
        payload: dict[str, JsonValue] = {
            "status": "known" if self.known else "unknown",
            "value": self.value,
            "unit": self.unit,
        }
        if self.numerator is not None:
            payload["numerator"] = self.numerator
        if self.denominator is not None:
            payload["denominator"] = self.denominator
        if self.sample_count is not None:
            payload["sample_count"] = self.sample_count
        if self.unknown_reason is not None:
            payload["unknown_reason"] = self.unknown_reason
        return payload


@dataclass(frozen=True, slots=True)
class ReliabilityReport:
    recovery_success_rate: ReliabilityMetric
    duplicate_visible_side_effect_rate: ReliabilityMetric
    outcome_unknown_rate: ReliabilityMetric
    projection_replay_divergence_rate: ReliabilityMetric
    cancellation_leak_rate: ReliabilityMetric
    stale_worker_commit_rejection_rate: ReliabilityMetric
    p50_resume_latency_ms: ReliabilityMetric
    p95_resume_latency_ms: ReliabilityMetric

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "recovery_success_rate": self.recovery_success_rate.to_dict(),
            "duplicate_visible_side_effect_rate": (
                self.duplicate_visible_side_effect_rate.to_dict()
            ),
            "outcome_unknown_rate": self.outcome_unknown_rate.to_dict(),
            "projection_replay_divergence_rate": (
                self.projection_replay_divergence_rate.to_dict()
            ),
            "cancellation_leak_rate": self.cancellation_leak_rate.to_dict(),
            "stale_worker_commit_rejection_rate": (
                self.stale_worker_commit_rejection_rate.to_dict()
            ),
            "p50_resume_latency_ms": self.p50_resume_latency_ms.to_dict(),
            "p95_resume_latency_ms": self.p95_resume_latency_ms.to_dict(),
        }


def evaluate_reliability(evidence: ReliabilityEvidence) -> ReliabilityReport:
    """Calculate reliability metrics solely from explicit runner evidence."""

    if not isinstance(evidence, ReliabilityEvidence):
        raise TypeError("evidence must be ReliabilityEvidence")
    p50, p95 = _resume_latency_metrics(evidence.resume_latencies_ms)
    return ReliabilityReport(
        recovery_success_rate=_ratio_metric(
            evidence.recovery_successes,
            evidence.recovery_attempts,
        ),
        duplicate_visible_side_effect_rate=_ratio_metric(
            evidence.duplicate_visible_side_effects,
            evidence.visible_side_effect_checks,
        ),
        outcome_unknown_rate=_ratio_metric(
            evidence.outcome_unknown_attempts,
            evidence.terminal_attempt_checks,
        ),
        projection_replay_divergence_rate=_ratio_metric(
            evidence.projection_replay_divergences,
            evidence.projection_replay_checks,
        ),
        cancellation_leak_rate=_ratio_metric(
            evidence.cancellation_leaks,
            evidence.cancellation_checks,
        ),
        stale_worker_commit_rejection_rate=_ratio_metric(
            evidence.stale_worker_commit_rejections,
            evidence.stale_worker_commit_attempts,
        ),
        p50_resume_latency_ms=p50,
        p95_resume_latency_ms=p95,
    )


def _ratio_metric(
    numerator: int | None,
    denominator: int | None,
) -> ReliabilityMetric:
    if numerator is None or denominator is None:
        return ReliabilityMetric(
            value=None,
            unit="ratio",
            numerator=numerator,
            denominator=denominator,
            unknown_reason="missing_evidence",
        )
    if denominator == 0:
        return ReliabilityMetric(
            value=None,
            unit="ratio",
            numerator=numerator,
            denominator=denominator,
            unknown_reason="zero_denominator",
        )
    return ReliabilityMetric(
        value=numerator / denominator,
        unit="ratio",
        numerator=numerator,
        denominator=denominator,
    )


def _resume_latency_metrics(
    samples: tuple[float, ...] | None,
) -> tuple[ReliabilityMetric, ReliabilityMetric]:
    if samples is None:
        unknown = ReliabilityMetric(
            value=None,
            unit="ms",
            unknown_reason="missing_evidence",
        )
        return unknown, unknown
    if not samples:
        unknown = ReliabilityMetric(
            value=None,
            unit="ms",
            sample_count=0,
            unknown_reason="no_samples",
        )
        return unknown, unknown
    ordered = tuple(sorted(samples))
    return (
        ReliabilityMetric(
            value=_nearest_rank_percentile(ordered, 50),
            unit="ms",
            sample_count=len(ordered),
        ),
        ReliabilityMetric(
            value=_nearest_rank_percentile(ordered, 95),
            unit="ms",
            sample_count=len(ordered),
        ),
    )


def _nearest_rank_percentile(
    ordered_samples: tuple[float, ...],
    percentile: int,
) -> float:
    rank = max(1, math.ceil(percentile * len(ordered_samples) / 100))
    return ordered_samples[rank - 1]


def _optional_counter(value: int | None, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer or None")
    return value


ScenarioRunner = Callable[[Scenario], EvaluationContext]


@dataclass(frozen=True, slots=True)
class Suite:
    """A deterministic collection of scenarios and invariants."""

    name: str
    scenarios: tuple[Scenario, ...]
    invariants: tuple[Invariant, ...] = field(
        default_factory=lambda: DEFAULT_INVARIANTS
    )

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("suite name must not be empty")
        scenario_ids = [scenario.scenario_id for scenario in self.scenarios]
        if len(scenario_ids) != len(set(scenario_ids)):
            raise ValueError("scenario_id must be unique within a suite")
        invariant_names = [invariant.name for invariant in self.invariants]
        if len(invariant_names) != len(set(invariant_names)):
            raise ValueError("invariant names must be unique within a suite")

    def run(self, runner: ScenarioRunner) -> SuiteReport:
        results: list[EvalResult] = []
        for scenario in sorted(
            self.scenarios,
            key=lambda item: item.scenario_id,
        ):
            try:
                context = runner(scenario)
                if not isinstance(context, EvaluationContext):
                    raise TypeError("runner must return EvaluationContext")
                if context.scenario.scenario_id != scenario.scenario_id:
                    raise ValueError("runner returned context for another scenario")
            except Exception as exc:
                failure = InvariantFailure(
                    code="runner_error",
                    invariant="runner",
                    message="scenario runner failed",
                    details={"exception_type": type(exc).__name__},
                )
                results.append(
                    EvalResult(
                        scenario_id=scenario.scenario_id,
                        run_id=scenario.run_id,
                        fault_point=scenario.fault_point,
                        failures=(failure,),
                    )
                )
                continue

            failures: list[InvariantFailure] = []
            for invariant in self.invariants:
                try:
                    reported = tuple(invariant.check(context))
                except Exception as exc:
                    reported = (
                        InvariantFailure(
                            code="invariant_error",
                            invariant=invariant.name,
                            message="invariant check failed",
                            details={"exception_type": type(exc).__name__},
                        ),
                    )
                failures.extend(
                    sorted(
                        reported,
                        key=lambda item: (
                            item.code,
                            item.invariant,
                            _canonical_bytes(item.details),
                        ),
                    )
                )
            results.append(
                EvalResult(
                    scenario_id=scenario.scenario_id,
                    run_id=scenario.run_id,
                    fault_point=scenario.fault_point,
                    failures=tuple(failures),
                )
            )
        return SuiteReport(suite_name=self.name, results=tuple(results))


def fault_scenarios(
    fault_points: Iterable[str | Mapping[str, Any]],
    *,
    run_id_prefix: str = "fault",
) -> tuple[Scenario, ...]:
    """Build table-driven scenarios in stable fault-point order."""

    normalized: list[tuple[str, dict[str, JsonValue]]] = []
    for item in fault_points:
        if isinstance(item, str):
            fault_point = item
            parameters: dict[str, JsonValue] = {}
        else:
            fault_point = str(item.get("fault_point", ""))
            raw_parameters = item.get("parameters", {})
            if not isinstance(raw_parameters, dict):
                raise ValueError("fault scenario parameters must be an object")
            parameters = normalize_json(raw_parameters, "fault.parameters")
        if not fault_point:
            raise ValueError("fault_point must not be empty")
        normalized.append((fault_point, parameters))

    return tuple(
        Scenario(
            scenario_id=f"fault:{fault_point}",
            run_id=f"{run_id_prefix}:{fault_point}",
            fault_point=fault_point,
            parameters=parameters,
            tags=("fault-injection",),
        )
        for fault_point, parameters in sorted(
            normalized,
            key=lambda item: (item[0], _canonical_bytes(item[1])),
        )
    )


def _sequence_contiguous(
    context: EvaluationContext,
) -> Sequence[InvariantFailure]:
    expected = 1
    for event in context.events:
        sequence = getattr(event, "seq", None)
        if sequence != expected:
            return (
                InvariantFailure(
                    code="sequence_gap",
                    invariant="sequence_contiguous",
                    message="event sequence is not contiguous from one",
                    details={"expected": expected, "actual": sequence},
                ),
            )
        expected += 1
    return ()


def _projection_matches_replay(
    context: EvaluationContext,
) -> Sequence[InvariantFailure]:
    if not context.projection_diffs:
        return ()
    return (
        InvariantFailure(
            code="projection_drift",
            invariant="projection_matches_replay",
            message="online projection differs from logical replay",
            details={
                "diff_count": len(context.projection_diffs),
                "objects": sorted(
                    {
                        f"{diff.object_type}:{diff.object_id}"
                        for diff in context.projection_diffs
                    }
                ),
            },
        ),
    )


def _no_active_attempt_on_terminal(
    context: EvaluationContext,
) -> Sequence[InvariantFailure]:
    snapshot = context.snapshot
    if snapshot is None or not snapshot.run.status.is_terminal:
        return ()
    active = sorted(
        attempt.attempt_id
        for attempt in snapshot.attempts
        if not attempt.status.is_terminal
    )
    if not active:
        return ()
    return (
        InvariantFailure(
            code="active_attempt_on_terminal",
            invariant="no_active_attempt_on_terminal",
            message="terminal Run retains active Attempts",
            details={"attempt_ids": active},
        ),
    )


def _no_secret_tokens(
    context: EvaluationContext,
) -> Sequence[InvariantFailure]:
    sources = (
        ("sqlite", bytes(context.sqlite_bytes)),
        ("trace", bytes(context.trace_bytes)),
    )
    failures: list[InvariantFailure] = []
    for token_index, token in enumerate(context.secret_tokens):
        token_bytes = token.encode("utf-8") if isinstance(token, str) else bytes(token)
        if not token_bytes:
            continue
        for source_name, source_bytes in sources:
            if token_bytes in source_bytes:
                failures.append(
                    InvariantFailure(
                        code="secret_token_found",
                        invariant="no_secret_tokens",
                        message="sensitive token found in persisted evaluation bytes",
                        details={
                            "source": source_name,
                            "token_index": token_index,
                        },
                    )
                )
    return tuple(failures)


def _no_duplicate_terminal(
    context: EvaluationContext,
) -> Sequence[InvariantFailure]:
    counts: Counter[tuple[str, str]] = Counter()
    for event in context.events:
        event_type = getattr(event, "event_type", "")
        if event_type in _RUN_TERMINAL_EVENTS:
            counts[("run", context.scenario.run_id)] += 1
        elif event_type in _NODE_TERMINAL_EVENTS:
            counts[("node", str(getattr(event, "node_id", "")))] += 1
        elif event_type in _ATTEMPT_TERMINAL_EVENTS:
            counts[("attempt", str(getattr(event, "attempt_id", "")))] += 1
    failures: list[InvariantFailure] = []
    for (object_type, object_id), count in sorted(counts.items()):
        if count > 1:
            failures.append(
                InvariantFailure(
                    code="duplicate_terminal",
                    invariant="no_duplicate_terminal",
                    message="object has more than one terminal event",
                    details={
                        "object_type": object_type,
                        "object_id": object_id,
                        "count": count,
                    },
                )
            )
    return tuple(failures)


SEQUENCE_CONTIGUOUS = Invariant(
    name="sequence_contiguous",
    check=_sequence_contiguous,
)
PROJECTION_MATCHES_REPLAY = Invariant(
    name="projection_matches_replay",
    check=_projection_matches_replay,
)
NO_ACTIVE_ATTEMPT_ON_TERMINAL = Invariant(
    name="no_active_attempt_on_terminal",
    check=_no_active_attempt_on_terminal,
)
NO_SECRET_TOKENS = Invariant(
    name="no_secret_tokens",
    check=_no_secret_tokens,
)
NO_DUPLICATE_TERMINAL = Invariant(
    name="no_duplicate_terminal",
    check=_no_duplicate_terminal,
)
DEFAULT_INVARIANTS = (
    SEQUENCE_CONTIGUOUS,
    PROJECTION_MATCHES_REPLAY,
    NO_ACTIVE_ATTEMPT_ON_TERMINAL,
    NO_SECRET_TOKENS,
    NO_DUPLICATE_TERMINAL,
)


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


__all__ = [
    "DEFAULT_INVARIANTS",
    "EvalResult",
    "EvaluationContext",
    "Invariant",
    "InvariantFailure",
    "NO_ACTIVE_ATTEMPT_ON_TERMINAL",
    "NO_DUPLICATE_TERMINAL",
    "NO_SECRET_TOKENS",
    "PROJECTION_MATCHES_REPLAY",
    "ReliabilityEvidence",
    "ReliabilityMetric",
    "ReliabilityReport",
    "SEQUENCE_CONTIGUOUS",
    "Scenario",
    "Suite",
    "SuiteReport",
    "evaluate_reliability",
    "fault_scenarios",
]
