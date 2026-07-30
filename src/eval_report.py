from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from src.core.eval_report import EvalReportError, build_regression_report


MAX_REPORT_INPUT_BYTES = 20 * 1024 * 1024


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise EvalReportError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _reject_nonstandard_number(value: str) -> None:
    raise EvalReportError(f"non-standard JSON number is not allowed: {value}")


def _read_json(path: str, field: str) -> dict[str, Any]:
    target = Path(path)
    try:
        with target.open("rb") as file:
            encoded = file.read(MAX_REPORT_INPUT_BYTES + 1)
        if len(encoded) > MAX_REPORT_INPUT_BYTES:
            raise EvalReportError(f"{field} exceeds 20MB limit")
        value = json.loads(
            encoded.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonstandard_number,
        )
    except EvalReportError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise EvalReportError(f"failed to read {field}: {type(exc).__name__}") from exc
    if not isinstance(value, dict):
        raise EvalReportError(f"{field} must contain a JSON object")
    return value


def _write_json(path: str, payload: dict[str, Any], *, compact: bool) -> None:
    target = Path(path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        rendered = (
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
            if compact
            else json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        )
        file_descriptor, tmp_name = tempfile.mkstemp(
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
        )
        try:
            with os.fdopen(file_descriptor, "w", encoding="utf-8") as file:
                file.write(rendered + "\n")
                file.flush()
                os.fsync(file.fileno())
            os.replace(tmp_name, target)
        finally:
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
    except OSError as exc:
        raise EvalReportError(
            f"failed to write report: {type(exc).__name__}"
        ) from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare two completed XAgent Eval runs against a regression budget"
    )
    parser.add_argument("--current", required=True, help="Current Eval result.json")
    parser.add_argument("--baseline", required=True, help="Baseline Eval result.json")
    parser.add_argument("--budget", required=True, help="Regression budget JSON")
    parser.add_argument("--output", help="Optional path for the machine-readable report")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON")
    args = parser.parse_args(argv)

    try:
        report = build_regression_report(
            _read_json(args.current, "current run"),
            _read_json(args.baseline, "baseline run"),
            _read_json(args.budget, "regression budget"),
        )
        if args.output:
            _write_json(args.output, report, compact=args.compact)
        rendered = (
            json.dumps(report, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
            if args.compact
            else json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
        )
        print(rendered)
        return 0 if report["verdict"] == "passed" else 1
    except EvalReportError as exc:
        print(
            json.dumps(
                {"version": 1, "verdict": "invalid", "error": str(exc)},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
