from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.core.workspace_storage import atomic_write_json, atomic_write_text, workspace_write_lock

RUNBOOK_SKILL_NAME = "auto-runbooks"
RUNBOOK_TASK_ID = "managed-runbook-auto-distillation-review"
RUNBOOK_TASK_NAME = "Runbook Auto Distillation Review"
RUNBOOK_TEMPLATE_PATH = Path("system") / "templates" / "runbook-sop.md"
RUNBOOK_SKILL_DIR = Path("system") / "skills" / RUNBOOK_SKILL_NAME
RUNBOOK_SKILL_FILE = RUNBOOK_SKILL_DIR / "SKILL.md"
RUNBOOK_META_FILE = RUNBOOK_SKILL_DIR / "_meta.json"
RUNBOOK_TASK_STORE = Path("runtime") / "tasks" / "tasks.json"
RUNBOOK_SECTION_HEADER = "## Distilled Runbooks"
DEFAULT_MIN_INTERACTION_RECORDS = 10
ENTRY_KEY_RE = re.compile(r"^- Key: `([^`]+)`$", re.MULTILINE)
PROBLEM_STATUSES = {
    "ERROR",
    "SKIP",
    "TIMEOUT",
    "EMPTY_RESPONSE",
    "RETRYABLE_RESPONSE_ERROR",
    "CODE_BLOCK_WITHOUT_TOOL",
}


def distill_runbook_from_task(
    workspace_root: str | Path,
    task: str,
    result: dict[str, Any],
    *,
    agent_name: str = "",
    trace_events: list[dict[str, Any]] | None = None,
    max_entries: int = 30,
    min_interaction_records: int = DEFAULT_MIN_INTERACTION_RECORDS,
) -> dict[str, Any]:
    root = Path(workspace_root)
    if not root.exists() or not root.is_dir():
        return {"status": "SKIP", "error": "workspace root does not exist", "path": str(root)}
    if not isinstance(result, dict):
        return {"status": "SKIP", "error": "result is not a dict", "path": str(root)}
    interaction_records = _interaction_record_count(result)
    min_records = max(1, int(min_interaction_records))
    if interaction_records < min_records:
        return {
            "status": "SKIP",
            "error": "insufficient_interaction_records",
            "interaction_records": interaction_records,
            "min_interaction_records": min_records,
            "path": str(root),
        }

    try:
        entry = build_runbook_entry(
            task=task,
            result=result,
            agent_name=agent_name,
            trace_events=trace_events or [],
        )
        with workspace_write_lock(root):
            skill_path = update_runbook_skill(root, entry, max_entries=max_entries)
            meta_path = write_runbook_skill_meta(root)
            template_path = ensure_runbook_template(root)
            task_path = upsert_runbook_review_task(root, agent_name=agent_name)
    except OSError as exc:
        return {"status": "ERROR", "error": type(exc).__name__, "path": str(root)}
    except UnicodeError as exc:
        return {"status": "ERROR", "error": type(exc).__name__, "path": str(root)}
    except json.JSONDecodeError as exc:
        return {"status": "ERROR", "error": type(exc).__name__, "path": str(root)}

    return {
        "status": "OK",
        "entry_key": entry["key"],
        "outcome": entry["outcome"],
        "artifacts": {
            "skill": str(skill_path),
            "meta": str(meta_path),
            "template": str(template_path),
            "task_store": str(task_path),
        },
    }


def build_runbook_entry(
    *,
    task: str,
    result: dict[str, Any],
    agent_name: str = "",
    trace_events: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    trace_events = trace_events or []
    title = _task_title(task)
    digest = hashlib.sha1(str(task or "").encode("utf-8")).hexdigest()[:12]  # noqa: S324
    exit_reason = str(result.get("exit_reason") or "UNKNOWN")
    tool_summary = _tool_summary(result.get("tool_results", []))
    trace_summary = _trace_summary(trace_events)
    problem = _first_problem(result.get("tool_results", []), exit_reason)
    outcome = "failure" if problem else "success"
    key = f"{digest}:{outcome}"

    steps = _sop_steps(outcome, tool_summary, problem)
    note = _entry_note(outcome, problem, exit_reason)

    return {
        "key": key,
        "title": title,
        "agent": _safe_inline(agent_name or "main", limit=40),
        "outcome": outcome,
        "exit_reason": _safe_inline(exit_reason, limit=60),
        "tools": tool_summary or "no_tool",
        "trace": trace_summary,
        "steps": steps,
        "note": note,
    }


def update_runbook_skill(workspace_root: str | Path, entry: dict[str, Any], *, max_entries: int = 30) -> Path:
    root = Path(workspace_root)
    path = root / RUNBOOK_SKILL_FILE
    with workspace_write_lock(root):
        content = path.read_text(encoding="utf-8") if path.is_file() else _default_skill_content()

        entries = _parse_runbook_entries(content)
        key = str(entry["key"])
        rendered = _render_entry(entry)
        filtered = [item for item in entries if item["key"] != key]
        filtered.append({"key": key, "body": rendered})
        limit = max(1, int(max_entries))
        filtered = filtered[-limit:]

        section = (
            RUNBOOK_SECTION_HEADER
            + "\n\n"
            + "\n".join(item["body"].rstrip() for item in filtered).rstrip()
            + "\n"
        )
        updated = _replace_runbook_section(content, section)
        atomic_write_text(path, updated)
    return path


def write_runbook_skill_meta(workspace_root: str | Path) -> Path:
    root = Path(workspace_root)
    path = root / RUNBOOK_META_FILE
    payload = {
        "name": RUNBOOK_SKILL_NAME,
        "description": "Workspace-local SOPs distilled from successful and failed XAgent runs.",
        "triggers": ["runbook", "sop", "SOP", "复盘", "经验", "自动沉淀", "故障", "成功路径"],
        "max_inject_chars": 12000,
    }
    with workspace_write_lock(root):
        atomic_write_json(path, payload)
    return path


def ensure_runbook_template(workspace_root: str | Path) -> Path:
    root = Path(workspace_root)
    path = root / RUNBOOK_TEMPLATE_PATH
    with workspace_write_lock(root):
        if not path.exists():
            atomic_write_text(
                path,
                "\n".join(
                    [
                        "# Runbook SOP",
                        "",
                        "## Trigger",
                        "",
                        "- When this SOP should be used.",
                        "",
                        "## Preconditions",
                        "",
                        "- Required workspace state, files, credentials, or user confirmations.",
                        "",
                        "## Steps",
                        "",
                        "1. Inspect the current state before making changes.",
                        "2. Execute the smallest safe action.",
                        "3. Verify the result with a read-only check or targeted test.",
                        "",
                        "## Failure Handling",
                        "",
                        "- Record the failing tool/status and switch strategy before retrying.",
                        "",
                    ]
                ),
            )
    return path


def upsert_runbook_review_task(workspace_root: str | Path, *, agent_name: str = "") -> Path:
    root = Path(workspace_root)
    path = root / RUNBOOK_TASK_STORE
    with workspace_write_lock(root):
        payload = _read_task_payload(path)
        tasks = payload.get("tasks")
        if not isinstance(tasks, list):
            tasks = []

        now = time.time()
        workspace_name = root.name if root.name.endswith(".ws") else ""
        task = _managed_review_task(agent_name=agent_name, workspace_name=workspace_name, now_ts=now)
        replaced = False
        for index, existing in enumerate(tasks):
            if isinstance(existing, dict) and existing.get("id") == RUNBOOK_TASK_ID:
                merged = {**existing, **task}
                merged.setdefault("created_at", existing.get("created_at", now))
                tasks[index] = merged
                replaced = True
                break
        if not replaced:
            tasks.append(task)

        atomic_write_json(path, {"tasks": tasks, "updated_at": now})
    return path


def _default_skill_content() -> str:
    return (
        "# Auto Runbooks\n\n"
        "Use this skill when a task resembles prior successful or failed runs in this workspace. "
        "Start from the distilled SOP, verify current state, and avoid repeating recorded failure patterns.\n\n"
        "## Operating Rules\n\n"
        "- Treat entries as reusable procedures, not guarantees.\n"
        "- Re-check workspace state before taking any destructive or irreversible action.\n"
        "- Prefer read-only inspection before writes, patches, browser actions, or code execution.\n\n"
        f"{RUNBOOK_SECTION_HEADER}\n"
    )


def _parse_runbook_entries(content: str) -> list[dict[str, str]]:
    section = _runbook_section(content)
    if not section:
        return []
    blocks = re.split(r"(?m)^### ", section)
    entries: list[dict[str, str]] = []
    for block in blocks:
        block = block.strip()
        if not block or block == RUNBOOK_SECTION_HEADER:
            continue
        body = "### " + block + "\n"
        match = ENTRY_KEY_RE.search(body)
        if match:
            entries.append({"key": match.group(1), "body": body})
    return entries


def _runbook_section(content: str) -> str:
    start = content.find(RUNBOOK_SECTION_HEADER)
    if start < 0:
        return ""
    tail = content[start:]
    next_match = re.search(rf"\n## (?!{re.escape(RUNBOOK_SECTION_HEADER[3:])})", tail)
    if next_match:
        return tail[: next_match.start()].strip()
    return tail.strip()


def _replace_runbook_section(content: str, section: str) -> str:
    if RUNBOOK_SECTION_HEADER not in content:
        prefix = content.rstrip()
        return f"{prefix}\n\n{section}" if prefix else section

    start = content.find(RUNBOOK_SECTION_HEADER)
    tail = content[start:]
    next_match = re.search(rf"\n## (?!{re.escape(RUNBOOK_SECTION_HEADER[3:])})", tail)
    if next_match:
        end = start + next_match.start()
        return (content[:start].rstrip() + "\n\n" + section.rstrip() + "\n" + content[end:]).rstrip() + "\n"
    return (content[:start].rstrip() + "\n\n" + section.rstrip() + "\n").lstrip()


def _render_entry(entry: dict[str, Any]) -> str:
    title = _safe_heading(str(entry["title"]))
    steps = [str(item) for item in entry.get("steps", []) if str(item).strip()]
    step_lines = "\n".join(f"  {idx}. {_safe_inline(step, limit=180)}" for idx, step in enumerate(steps, start=1))
    return (
        f"### {title}\n"
        f"- Key: `{entry['key']}`\n"
        f"- Outcome: `{entry['outcome']}`\n"
        f"- Agent: `{entry['agent']}`\n"
        f"- Exit: `{entry['exit_reason']}`\n"
        f"- Tools: `{entry['tools']}`\n"
        f"- Trace: `{entry['trace']}`\n"
        "- SOP:\n"
        f"{step_lines}\n"
        f"- Note: {_safe_inline(str(entry['note']), limit=220)}\n"
    )


def _read_task_payload(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"tasks": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {"tasks": []}
    return payload if isinstance(payload, dict) else {"tasks": []}


def _interaction_record_count(result: dict[str, Any]) -> int:
    try:
        turns = int(result.get("turns") or 0)
    except (TypeError, ValueError):
        turns = 0
    tool_results = result.get("tool_results", [])
    tool_count = len(tool_results) if isinstance(tool_results, list) else 0
    return max(0, turns) + tool_count


def _managed_review_task(*, agent_name: str = "", workspace_name: str = "", now_ts: float | None = None) -> dict[str, Any]:
    now_ts = time.time() if now_ts is None else now_ts
    agent = _safe_child_name(agent_name) or "main"
    today = datetime.fromtimestamp(now_ts)
    next_run = today.replace(hour=10, minute=0, second=0, microsecond=0)
    while next_run.timestamp() <= now_ts:
        next_run += timedelta(days=7)
    return {
        "id": RUNBOOK_TASK_ID,
        "workspace": workspace_name,
        "name": RUNBOOK_TASK_NAME,
        "prompt": (
            "检查 system/skills/auto-runbooks/SKILL.md、system/templates/runbook-sop.md "
            "以及 runtime/traces 中的近期运行元数据；合并重复 SOP，标记过时经验，补充缺失的验证步骤。"
            "不要删除用户文件。"
        ),
        "agent": agent,
        "repeat": "weekly",
        "date": today.strftime("%Y-%m-%d"),
        "time": "10:00",
        "end_date": "",
        "interval_minutes": 0,
        "keep_one_chat": True,
        "status": "running",
        "config_path": "config.json",
        "observability_config_path": "",
        "chat_id": "",
        "next_run": next_run.strftime("%Y-%m-%d %H:%M"),
        "last_run": None,
        "last_session_id": "",
        "last_error": "",
        "last_debug_run": None,
        "last_debug_session_id": "",
        "last_debug_error": "",
        "created_at": now_ts,
        "updated_at": now_ts,
    }


def _tool_summary(raw_tool_results: Any) -> str:
    if not isinstance(raw_tool_results, list):
        return ""
    parts: list[str] = []
    for item in raw_tool_results[:12]:
        if not isinstance(item, dict):
            continue
        name = _safe_inline(str(item.get("tool_name") or "tool"), limit=40)
        data = item.get("data")
        status = ""
        if isinstance(data, dict):
            status = str(data.get("status") or ("ERROR" if data.get("error") else "OK"))
        status = _safe_inline(status or "OK", limit=40).upper()
        parts.append(f"{name}:{status}")
    if isinstance(raw_tool_results, list) and len(raw_tool_results) > 12:
        parts.append(f"+{len(raw_tool_results) - 12} more")
    return " -> ".join(parts)


def _trace_summary(events: list[dict[str, Any]]) -> str:
    if not events:
        return "current task result"
    counts: dict[str, int] = {}
    for event in events:
        kind = str(event.get("kind") or "")
        if kind:
            counts[kind] = counts.get(kind, 0) + 1
    if not counts:
        return "current task result"
    return ", ".join(f"{key}:{counts[key]}" for key in sorted(counts)[:8])


def _first_problem(raw_tool_results: Any, exit_reason: str) -> str:
    if str(exit_reason or "") in {"ERROR", "EXITED", "MAX_TURNS_EXCEEDED"}:
        return f"exit:{exit_reason}"
    if not isinstance(raw_tool_results, list):
        return ""
    for item in raw_tool_results:
        if not isinstance(item, dict):
            continue
        data = item.get("data")
        if not isinstance(data, dict):
            continue
        status = str(data.get("status") or "").upper()
        has_error = bool(data.get("error"))
        if status in PROBLEM_STATUSES or (has_error and not status):
            tool = _safe_inline(str(item.get("tool_name") or "tool"), limit=40)
            return f"{tool}:{status or 'ERROR'}"
    return ""


def _sop_steps(outcome: str, tool_summary: str, problem: str) -> list[str]:
    steps = ["Confirm the workspace, target files, and expected final state before acting."]
    if tool_summary:
        steps.append(f"Reuse the observed tool chain as a candidate path: {tool_summary}.")
    if outcome == "success":
        steps.extend([
            "Start with read-only inspection, then perform the smallest necessary side-effecting action.",
            "Verify completion with a targeted read, command, browser check, or test before reporting done.",
        ])
    else:
        steps.extend([
            f"When this path fails at {problem}, stop and diagnose the root cause before retrying.",
            "Switch to a smaller probe, narrower patch, or user clarification instead of repeating the same action.",
        ])
    return steps


def _entry_note(outcome: str, problem: str, exit_reason: str) -> str:
    if outcome == "success":
        return "Successful trace distilled. Reuse the sequence only after re-validating current workspace state."
    return f"Failure trace distilled from {problem or exit_reason}. Treat it as a guardrail against repeated retries."


def _task_title(task: str) -> str:
    first_line = str(task or "").strip().splitlines()[0] if str(task or "").strip() else "Untitled task"
    return _safe_inline(first_line, limit=80)


def _safe_inline(text: str, *, limit: int = 120) -> str:
    clean = " ".join(str(text or "").replace("`", "'").split())
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 3)].rstrip() + "..."


def _safe_heading(text: str) -> str:
    clean = _safe_inline(text.replace("#", "").strip(), limit=80)
    return clean or "Untitled task"


def _safe_child_name(name: str) -> str:
    clean = str(name or "").strip()
    if clean and re.fullmatch(r"[A-Za-z0-9_.-]+", clean):
        return clean
    return ""
