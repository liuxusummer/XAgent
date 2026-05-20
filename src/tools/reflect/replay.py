"""观测事件回放：把 JSONL 事件流渲染成可读轨迹。"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Iterable

from src.tools.reflect.reader import group_by_session, load_dir


def _fmt_ms(duration_ms: float | None) -> str:
    if duration_ms is None:
        return ""
    return f" +{duration_ms:.0f}ms"


def _fmt_rel(ts: float, start_ts: float) -> str:
    return f"[{max(ts - start_ts, 0):06.3f}s]"


def _fmt_tokens(data: dict[str, Any]) -> str:
    total = data.get("total_tokens")
    if total is None:
        return ""
    return f", tokens={total}"


def format_event(event: dict[str, Any], start_ts: float) -> str:
    """把单条事件渲染为单行文本。"""
    kind = str(event.get("kind", ""))
    name = str(event.get("name", ""))
    data = event.get("data") or {}
    turn = int(event.get("turn", 0) or 0)
    rel = _fmt_rel(float(event.get("ts", start_ts) or start_ts), start_ts)
    dur = _fmt_ms(event.get("duration_ms"))

    if kind == "run_start":
        query = name if len(name) <= 40 else name[:40] + "..."
        return f'{rel} run_start: "{query}" (query_len={data.get("query_len", 0)})'
    if kind == "run_end":
        return f'{rel} run_end {name}{dur} (turns={data.get("turns", turn)})'
    if kind == "llm_end":
        label = name or "stop"
        return (
            f"{rel} llm_end {label}{dur} "
            f"(content={data.get('content_len', 0)}, tools={data.get('tool_call_count', 0)}"
            f"{_fmt_tokens(data)})"
        )
    if kind == "tool_start":
        return f"{rel} tool_start {name}"
    if kind == "tool_end":
        return f"{rel} tool_end {name}{dur} (should_exit={data.get('should_exit', False)})"
    if kind == "turn_end":
        return f"{rel} turn_end (tool_count={data.get('tool_count', 0)})"
    if kind == "hook_inject":
        return f"{rel} hook_inject {name} (prompt_len={data.get('prompt_len', 0)})"
    if kind == "turn_start":
        return f"[turn {turn}]"
    return f"{rel} {kind} {name}{dur}".rstrip()


def render_session(events: Iterable[dict[str, Any]]) -> str:
    """渲染单个 session 的完整轨迹。"""
    event_list = list(events)
    if not event_list:
        return ""
    session_id = str(event_list[0].get("session_id", ""))
    start_ts = float(event_list[0].get("ts", 0.0) or 0.0)
    lines = [f"=== session {session_id} ({len(event_list)} events) ==="]
    for event in event_list:
        kind = str(event.get("kind", ""))
        line = format_event(event, start_ts)
        if kind == "turn_start":
            lines.append(f"  {line}")
        elif int(event.get("turn", 0) or 0) > 0:
            lines.append(f"    {line}")
        else:
            lines.append(line)
    return "\n".join(lines)


def render_sessions(log_dir: str | Path, session_id: str | None = None) -> str:
    """读取目录并渲染全部或指定 session。"""
    groups = group_by_session(load_dir(log_dir))
    chunks: list[str] = []
    for current_session_id, events in groups.items():
        if session_id and current_session_id != session_id:
            continue
        chunks.append(render_session(events))
    return "\n\n".join(chunk for chunk in chunks if chunk)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay XAgent telemetry JSONL logs")
    parser.add_argument("log_dir", help="Directory containing *.jsonl telemetry files")
    parser.add_argument("--session", dest="session_id", help="Render only one session")
    args = parser.parse_args(argv)

    output = render_sessions(args.log_dir, session_id=args.session_id)
    if output:
        print(output)
    else:
        # 明确区分"无事件"与"成功但静默"
        hint = f"(no events in {args.log_dir}"
        if args.session_id:
            hint += f" for session {args.session_id}"
        hint += ")"
        print(hint)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
