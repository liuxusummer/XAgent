from __future__ import annotations

import math
from typing import Any


REPORT_VERSION = 1
MAX_CHECKS = 100
MAX_POINTER_LENGTH = 240


class EvalReportError(ValueError):
    pass


def _bounded_text(value: Any, field: str, *, limit: int = 120) -> str:
    if not isinstance(value, str):
        raise EvalReportError(f"{field} must be a string")
    text = value.strip()
    if not text:
        raise EvalReportError(f"{field} must not be empty")
    if len(text) > limit:
        raise EvalReportError(f"{field} exceeds {limit} characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise EvalReportError(f"{field} contains control characters")
    return text


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EvalReportError(f"{field} must resolve to a number")
    number = float(value)
    if not math.isfinite(number):
        raise EvalReportError(f"{field} must be finite")
    return number


def _decode_pointer_part(value: str) -> str:
    index = 0
    decoded: list[str] = []
    while index < len(value):
        character = value[index]
        if character != "~":
            decoded.append(character)
            index += 1
            continue
        if index + 1 >= len(value) or value[index + 1] not in {"0", "1"}:
            raise EvalReportError("metric path contains an invalid JSON Pointer escape")
        decoded.append("~" if value[index + 1] == "0" else "/")
        index += 2
    return "".join(decoded)


def resolve_metric(run: dict[str, Any], pointer: str) -> float:
    pointer = _bounded_text(pointer, "metric path", limit=MAX_POINTER_LENGTH)
    if not pointer.startswith("/"):
        raise EvalReportError("metric path must be a JSON Pointer starting with /")
    current: Any = run
    for raw_part in pointer[1:].split("/"):
        part = _decode_pointer_part(raw_part)
        if not isinstance(current, dict) or part not in current:
            raise EvalReportError(f"metric path not found: {pointer}")
        current = current[part]
    return _number(current, f"metric {pointer}")


def _optional_bounded_text(value: Any, field: str, *, limit: int = 120) -> str:
    if value is None or value == "":
        return ""
    return _bounded_text(value, field, limit=limit)


def _optional_digest(value: Any, field: str) -> str:
    digest = _optional_bounded_text(value, field, limit=64)
    if digest and (
        len(digest) != 64
        or digest != digest.lower()
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise EvalReportError(f"{field} must be a lowercase SHA-256 digest")
    return digest


def _run_identity(run: dict[str, Any]) -> dict[str, str]:
    return {
        "run_id": _bounded_text(run.get("id"), "run id"),
        "dataset_id": _bounded_text(run.get("dataset_id"), "dataset id"),
        "dataset_digest": _optional_digest(
            run.get("dataset_digest"),
            "dataset digest",
        ),
        "evaluation_digest": _optional_digest(
            run.get("evaluation_digest"),
            "evaluation digest",
        ),
        "agent": _optional_bounded_text(run.get("agent"), "agent"),
    }


def _dataset_compatibility(
    current: dict[str, Any],
    baseline: dict[str, Any],
) -> tuple[bool, str]:
    current_evaluation = str(current.get("evaluation_digest") or "")
    baseline_evaluation = str(baseline.get("evaluation_digest") or "")
    if current_evaluation and baseline_evaluation:
        return (
            current_evaluation == baseline_evaluation,
            "evaluation_digest",
        )
    if current_evaluation or baseline_evaluation:
        return False, "mixed_evaluation_identity_unverifiable"
    current_digest = str(current.get("dataset_digest") or "")
    baseline_digest = str(baseline.get("dataset_digest") or "")
    if current_digest and baseline_digest:
        return current_digest == baseline_digest, "digest"
    if current_digest or baseline_digest:
        return False, "mixed_identity_unverifiable"
    current_id = str(current.get("dataset_id") or "")
    baseline_id = str(baseline.get("dataset_id") or "")
    current_total = current.get("summary", {}).get("total")
    baseline_total = baseline.get("summary", {}).get("total")
    return (
        bool(
            current_id
            and current_id == baseline_id
            and current_total == baseline_total
        ),
        "dataset_id_and_total",
    )


def _validate_run(run: Any, field: str) -> dict[str, Any]:
    if not isinstance(run, dict):
        raise EvalReportError(f"{field} run must be an object")
    if run.get("status") != "completed":
        raise EvalReportError(f"{field} run must have status completed")
    if not isinstance(run.get("summary"), dict):
        raise EvalReportError(f"{field} run summary must be an object")
    _run_identity(run)
    return run


def build_regression_report(
    current: dict[str, Any],
    baseline: dict[str, Any],
    budget: dict[str, Any],
) -> dict[str, Any]:
    current = _validate_run(current, "current")
    baseline = _validate_run(baseline, "baseline")
    if not isinstance(budget, dict):
        raise EvalReportError("budget must be an object")
    if budget.get("version") != REPORT_VERSION:
        raise EvalReportError(f"budget version must be {REPORT_VERSION}")

    raw_checks = budget.get("checks")
    if not isinstance(raw_checks, list) or not raw_checks:
        raise EvalReportError("budget checks must be a non-empty array")
    if len(raw_checks) > MAX_CHECKS:
        raise EvalReportError(f"budget exceeds {MAX_CHECKS} checks")

    compatible, compatibility_mode = _dataset_compatibility(current, baseline)
    require_same_dataset = budget.get("require_same_dataset", True)
    if not isinstance(require_same_dataset, bool):
        raise EvalReportError("require_same_dataset must be a boolean")

    checks: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, raw_check in enumerate(raw_checks):
        if not isinstance(raw_check, dict):
            raise EvalReportError(f"check #{index + 1} must be an object")
        check_id = _bounded_text(
            raw_check.get("id", f"check-{index + 1}"),
            f"check #{index + 1} id",
        )
        if check_id in seen_ids:
            raise EvalReportError(f"duplicate check id: {check_id}")
        seen_ids.add(check_id)
        pointer = _bounded_text(
            raw_check.get("path"),
            f"check {check_id} path",
            limit=MAX_POINTER_LENGTH,
        )
        direction = _bounded_text(
            raw_check.get("direction"),
            f"check {check_id} direction",
            limit=16,
        )
        if direction not in {"higher", "lower"}:
            raise EvalReportError(
                f"check {check_id} direction must be higher or lower"
            )

        current_value = resolve_metric(current, pointer)
        baseline_value = resolve_metric(baseline, pointer)
        reasons: list[str] = []
        threshold: float | None = None
        if "threshold" in raw_check:
            threshold = _number(
                raw_check["threshold"],
                f"check {check_id} threshold",
            )
            threshold_failed = (
                current_value < threshold
                if direction == "higher"
                else current_value > threshold
            )
            if threshold_failed:
                operator = ">=" if direction == "higher" else "<="
                reasons.append(
                    f"current {current_value:g} must be {operator} {threshold:g}"
                )

        max_regression = _number(
            raw_check.get("max_regression", 0),
            f"check {check_id} max_regression",
        )
        if max_regression < 0:
            raise EvalReportError(
                f"check {check_id} max_regression must be non-negative"
            )
        max_regression_ratio = _number(
            raw_check.get("max_regression_ratio", 0),
            f"check {check_id} max_regression_ratio",
        )
        if max_regression_ratio < 0:
            raise EvalReportError(
                f"check {check_id} max_regression_ratio must be non-negative"
            )
        relative_allowance = _number(
            abs(baseline_value) * max_regression_ratio,
            f"check {check_id} relative allowance",
        )
        allowed_regression = _number(
            max(max_regression, relative_allowance),
            f"check {check_id} allowed regression",
        )
        regression = _number(
            (
                baseline_value - current_value
                if direction == "higher"
                else current_value - baseline_value
            ),
            f"check {check_id} regression",
        )
        if regression > allowed_regression:
            reasons.append(
                f"regression {regression:g} exceeds allowance {allowed_regression:g}"
            )

        checks.append(
            {
                "id": check_id,
                "path": pointer,
                "direction": direction,
                "current": current_value,
                "baseline": baseline_value,
                "threshold": threshold,
                "allowed_regression": allowed_regression,
                "regression": regression,
                "passed": not reasons,
                "reasons": reasons,
            }
        )

    dataset_check_passed = compatible or not require_same_dataset
    failed_checks = [check["id"] for check in checks if not check["passed"]]
    if not dataset_check_passed:
        failed_checks.insert(0, "dataset_compatibility")
    return {
        "version": REPORT_VERSION,
        "verdict": "passed" if not failed_checks else "failed",
        "current": _run_identity(current),
        "baseline": _run_identity(baseline),
        "dataset_compatibility": {
            "required": require_same_dataset,
            "compatible": compatible,
            "mode": compatibility_mode,
            "passed": dataset_check_passed,
        },
        "checks": checks,
        "failed_checks": failed_checks,
    }
