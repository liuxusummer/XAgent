"""观测事件聚合：从 JSONL 生成延迟与退出报表。"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from src.tools.reflect.reader import group_by_session, load_dir

TOKEN_FIELDS = (
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cache_creation_input_tokens",
    "cache_read_input_tokens",
    "reasoning_tokens",
)


def compute_percentiles(values: list[float]) -> dict[str, float]:
    """百分位计算。空/单值走 fallback 常量路径；≥2 时线性插值。"""
    if not values:
        return {"count": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}

    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        only = ordered[0]
        return {"count": 1.0, "p50": only, "p95": only, "p99": only, "max": only}

    def _percentile(p: float) -> float:
        pos = (len(ordered) - 1) * p
        lower = int(pos)
        upper = min(lower + 1, len(ordered) - 1)
        weight = pos - lower
        return ordered[lower] + (ordered[upper] - ordered[lower]) * weight

    return {
        "count": float(len(ordered)),
        "p50": _percentile(0.50),
        "p95": _percentile(0.95),
        "p99": _percentile(0.99),
        "max": ordered[-1],
    }


def _event_token_data(event: dict[str, Any]) -> dict[str, int]:
    data = event.get("data") or {}
    tokens: dict[str, int] = {}
    for field in TOKEN_FIELDS:
        value = data.get(field)
        if isinstance(value, int) and not isinstance(value, bool):
            tokens[field] = value
    return tokens


def _add_tokens(target: dict[str, int], source: dict[str, int]) -> None:
    for field, value in source.items():
        target[field] = target.get(field, 0) + value


def aggregate(events: list[dict[str, Any]]) -> dict[str, Any]:
    """按 kind 聚合延迟、退出原因、hook 注入等统计。"""
    groups = group_by_session(events)
    llm_values: list[float] = []
    tool_values_by_name: dict[str, list[float]] = {}
    exit_reasons: dict[str, int] = {}
    hook_counts: dict[str, int] = {}
    llm_tokens_by_session: dict[str, dict[str, int]] = {}
    run_tokens_by_session: dict[str, dict[str, int]] = {}
    turn_count = 0
    tool_call_count = 0

    for event in events:
        kind = str(event.get("kind", ""))
        name = str(event.get("name", ""))
        session_id = str(event.get("session_id", ""))
        if kind == "turn_end":
            turn_count += 1
        elif kind == "llm_end" and event.get("duration_ms") is not None:
            llm_values.append(float(event["duration_ms"]))
            _add_tokens(llm_tokens_by_session.setdefault(session_id, {}), _event_token_data(event))
        elif kind == "tool_end" and event.get("duration_ms") is not None:
            tool_call_count += 1
            tool_values_by_name.setdefault(name, []).append(float(event["duration_ms"]))
        elif kind == "run_end":
            exit_reasons[name] = exit_reasons.get(name, 0) + 1
            tokens = _event_token_data(event)
            if tokens:
                run_tokens_by_session[session_id] = tokens
        elif kind == "hook_inject":
            hook_counts[name] = hook_counts.get(name, 0) + 1

    token_usage_by_session = {
        session_id: run_tokens_by_session.get(session_id, llm_tokens_by_session.get(session_id, {}))
        for session_id in groups
    }
    token_usage: dict[str, int] = {}
    for tokens in token_usage_by_session.values():
        _add_tokens(token_usage, tokens)

    tool_latency = {
        name: compute_percentiles(values)
        for name, values in sorted(tool_values_by_name.items())
    }
    return {
        "session_count": len(groups),
        "turn_count": turn_count,
        "tool_call_count": tool_call_count,
        "llm_latency": compute_percentiles(llm_values),
        "tool_latency": tool_latency,
        "exit_reasons": dict(sorted(exit_reasons.items())),
        "hook_injections": dict(sorted(hook_counts.items())),
        "token_usage": token_usage,
        "token_usage_by_session": {
            session_id: tokens
            for session_id, tokens in sorted(token_usage_by_session.items())
            if tokens
        },
    }


def render(report: dict[str, Any]) -> str:
    """把聚合报表渲染成 stdout 友好的文本。"""
    lines = [
        (
            f"=== aggregated over {report['session_count']} sessions, "
            f"{report['turn_count']} turns, {report['tool_call_count']} tool calls ==="
        ),
        "LLM latency (ms):",
    ]
    llm = report["llm_latency"]
    lines.append(
        "  count={count:.0f}  p50={p50:.1f}  p95={p95:.1f}  p99={p99:.1f}  max={max:.1f}".format(
            **llm
        )
    )
    lines.append("Tool latency by name (ms):")
    if report["tool_latency"]:
        for name, stats in report["tool_latency"].items():
            lines.append(
                "  {name:<12} count={count:.0f}  p50={p50:.1f}  p95={p95:.1f}  p99={p99:.1f}".format(
                    name=name,
                    **stats,
                )
            )
    else:
        lines.append("  (none)")

    lines.append("Exit reasons:")
    if report["exit_reasons"]:
        for name, count in report["exit_reasons"].items():
            lines.append(f"  {name}  {count}")
    else:
        lines.append("  (none)")

    lines.append("Hook injections:")
    if report["hook_injections"]:
        for name, count in report["hook_injections"].items():
            lines.append(f"  {name}  {count}")
    else:
        lines.append("  (none)")

    lines.append("Token usage:")
    if report.get("token_usage"):
        token_text = "  " + "  ".join(
            f"{field}={report['token_usage'].get(field, 0)}" for field in TOKEN_FIELDS
            if field in report["token_usage"]
        )
        lines.append(token_text)
    else:
        lines.append("  (unknown)")

    if len(report.get("token_usage_by_session", {})) == 1:
        session_id, tokens = next(iter(report["token_usage_by_session"].items()))
        token_text = "  " + "  ".join(
            f"{field}={tokens.get(field, 0)}" for field in TOKEN_FIELDS if field in tokens
        )
        lines.append(f"Token usage for session {session_id}:")
        lines.append(token_text)
    return "\n".join(lines)


def render_stats(log_dir: str | Path, session_id: str | None = None) -> str:
    events = load_dir(log_dir)
    if session_id:
        events = [event for event in events if event.get("session_id", "") == session_id]
    return render(aggregate(events))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Summarize XAgent telemetry JSONL logs")
    parser.add_argument("log_dir", help="Directory containing *.jsonl telemetry files")
    parser.add_argument("--session", dest="session_id", help="Summarize only one session")
    args = parser.parse_args(argv)

    print(render_stats(args.log_dir, session_id=args.session_id))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
