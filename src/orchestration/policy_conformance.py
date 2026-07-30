"""Deterministic, payload-free conformance packs for executable tool policy."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from itertools import islice
from typing import Any, Iterable

from .policy import (
    ActionRequest,
    PolicyEngine,
    PolicyOutcome,
    PolicyValidationError,
)

MAX_CONFORMANCE_CASES = 1024
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$")


def _safe_name(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not _SAFE_NAME.fullmatch(value):
        raise PolicyValidationError(f"{field_name} is invalid")
    return value


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise PolicyValidationError(
            "policy conformance report must be canonical JSON"
        ) from exc


@dataclass(frozen=True, slots=True)
class PolicyConformanceCase:
    """One exact action and its expected stable policy result."""

    case_id: str
    action: ActionRequest
    expected_outcome: PolicyOutcome
    expected_reason_code: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "case_id", _safe_name(self.case_id, "case_id"))
        if not isinstance(self.action, ActionRequest):
            raise PolicyValidationError(
                "conformance case action must be an ActionRequest"
            )
        try:
            object.__setattr__(
                self,
                "expected_outcome",
                PolicyOutcome(self.expected_outcome),
            )
        except ValueError as exc:
            raise PolicyValidationError(
                "conformance expected_outcome is invalid"
            ) from exc
        object.__setattr__(
            self,
            "expected_reason_code",
            _safe_name(self.expected_reason_code, "expected_reason_code"),
        )


@dataclass(frozen=True, slots=True)
class PolicyConformanceResult:
    case_id: str
    tool_name: str
    passed: bool
    expected_outcome: PolicyOutcome
    actual_outcome: PolicyOutcome
    expected_reason_code: str
    actual_reason_code: str
    simulation_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "tool_name": self.tool_name,
            "passed": self.passed,
            "expected_outcome": self.expected_outcome.value,
            "actual_outcome": self.actual_outcome.value,
            "expected_reason_code": self.expected_reason_code,
            "actual_reason_code": self.actual_reason_code,
            "simulation_digest": self.simulation_digest,
        }


@dataclass(frozen=True, slots=True)
class PolicyConformanceReport:
    policy_version: str
    results: tuple[PolicyConformanceResult, ...]
    covered_tools: tuple[str, ...]
    missing_tools: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.missing_tools and all(result.passed for result in self.results)

    @property
    def report_digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_dict())).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "policy_version": self.policy_version,
            "passed": self.passed,
            "results": [result.to_dict() for result in self.results],
            "covered_tools": list(self.covered_tools),
            "missing_tools": list(self.missing_tools),
        }


def evaluate_policy_conformance(
    policy: PolicyEngine,
    cases: Iterable[PolicyConformanceCase],
    *,
    required_tools: Iterable[str] = (),
) -> PolicyConformanceReport:
    """Evaluate a bounded pack without executing tools or consuming approvals."""

    if not isinstance(policy, PolicyEngine):
        raise PolicyValidationError("policy must be a PolicyEngine")
    try:
        normalized_cases = tuple(
            islice(iter(cases), MAX_CONFORMANCE_CASES + 1)
        )
    except TypeError as exc:
        raise PolicyValidationError("conformance cases must be iterable") from exc
    if not normalized_cases or len(normalized_cases) > MAX_CONFORMANCE_CASES:
        raise PolicyValidationError(
            "conformance cases must contain a bounded non-empty pack"
        )
    if not all(isinstance(case, PolicyConformanceCase) for case in normalized_cases):
        raise PolicyValidationError(
            "conformance cases must contain PolicyConformanceCase values"
        )
    case_ids = [case.case_id for case in normalized_cases]
    if len(set(case_ids)) != len(case_ids):
        raise PolicyValidationError("conformance case IDs must be unique")
    try:
        raw_required = tuple(
            islice(iter(required_tools), MAX_CONFORMANCE_CASES + 1)
        )
    except TypeError as exc:
        raise PolicyValidationError("required_tools must be iterable") from exc
    if len(raw_required) > MAX_CONFORMANCE_CASES:
        raise PolicyValidationError("required_tools exceed the bounded pack limit")
    required = tuple(
        sorted({_safe_name(name, "required_tool") for name in raw_required})
    )

    results: list[PolicyConformanceResult] = []
    covered: set[str] = set()
    for case in sorted(normalized_cases, key=lambda item: item.case_id):
        simulation = policy.simulate(case.action)
        decision = simulation.decision
        passed = (
            decision.outcome is case.expected_outcome
            and decision.reason_code == case.expected_reason_code
        )
        results.append(
            PolicyConformanceResult(
                case_id=case.case_id,
                tool_name=case.action.tool_name,
                passed=passed,
                expected_outcome=case.expected_outcome,
                actual_outcome=decision.outcome,
                expected_reason_code=case.expected_reason_code,
                actual_reason_code=decision.reason_code,
                simulation_digest=simulation.simulation_digest,
            )
        )
        if policy.tool_policy(case.action.tool_name) is not None:
            covered.add(case.action.tool_name)

    return PolicyConformanceReport(
        policy_version=policy.policy_version,
        results=tuple(results),
        covered_tools=tuple(sorted(covered)),
        missing_tools=tuple(sorted(set(required).difference(covered))),
    )


__all__ = [
    "MAX_CONFORMANCE_CASES",
    "PolicyConformanceCase",
    "PolicyConformanceReport",
    "PolicyConformanceResult",
    "evaluate_policy_conformance",
]
