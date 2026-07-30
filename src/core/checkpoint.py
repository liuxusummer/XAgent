from __future__ import annotations

import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any

from src.core.workspace_storage import atomic_write_json, workspace_write_lock

CHECKPOINT_SCHEMA_VERSION = 3
CHECKPOINT_DIR = Path("runtime") / "checkpoints"
LATEST_CHECKPOINT_FILE = "latest.json"
MAX_TASK_CHARS = 4000
MAX_PLAN_CHARS = 20000
MAX_TEXT_FIELD_CHARS = 1000
MAX_TOOL_RESULTS = 80
MAX_FILE_STATES = 40
MAX_HASH_BYTES = 5 * 1024 * 1024
MAX_CONTEXT_ITEMS = 256
PENDING_CHECKBOX_RE = re.compile(r"^\s*[-*]\s+\[\s\]\s+(.+?)\s*$")


def build_task_checkpoint(
    workspace_root: str | Path,
    *,
    checkpoint_id: str,
    session_id: str,
    task: str,
    agent_name: str = "",
    turn: int = 0,
    status: str = "running",
    exit_reason: str = "",
    tool_results: list[dict[str, Any]] | None = None,
    working: dict[str, str] | None = None,
    history_info: list[str] | None = None,
    pending_prompts: list[str] | None = None,
    pending_approval: dict[str, Any] | None = None,
    principal_digest: str = "",
    principal_boundary_digest: str = "",
    context_manifest_digest: str = "",
    context_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(workspace_root).resolve()
    tool_results = tool_results or []
    plan = _read_plan(root)
    pending_steps = _pending_steps(plan, pending_prompts or [])
    file_states = _file_states(root, tool_results)
    now = time.time()
    return {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "checkpoint_id": _safe_id(checkpoint_id or session_id),
        "session_id": str(session_id or ""),
        "task_title": _task_title(task),
        "task": _truncate(str(task or ""), MAX_TASK_CHARS),
        "agent_name": str(agent_name or ""),
        "turn": max(0, int(turn or 0)),
        "status": _normalize_status(status, exit_reason),
        "exit_reason": str(exit_reason or ""),
        "plan": plan,
        "pending_steps": pending_steps,
        "working": _safe_working(working or {}),
        "history_tail": [_truncate(str(item), MAX_TEXT_FIELD_CHARS) for item in (history_info or [])[-10:]],
        "tool_results": _tool_summaries(tool_results),
        "file_states": file_states,
        "pending_approval": _safe_pending_approval(pending_approval),
        "principal_digest": _safe_digest(principal_digest),
        "principal_boundary_digest": _safe_digest(
            principal_boundary_digest
        ),
        "context_manifest_digest": _safe_digest(context_manifest_digest),
        "context_state": _safe_context_state(context_state),
        "created_at": now,
        "updated_at": now,
    }


def write_task_checkpoint(workspace_root: str | Path, checkpoint: dict[str, Any]) -> dict[str, Any]:
    root = Path(workspace_root).resolve()
    if not root.is_dir():
        return {"status": "SKIP", "error": "workspace root does not exist", "path": str(root)}
    checkpoint_id = _safe_id(str(checkpoint.get("checkpoint_id") or checkpoint.get("session_id") or "checkpoint"))
    checkpoint = dict(checkpoint)
    checkpoint["checkpoint_id"] = checkpoint_id
    checkpoint["updated_at"] = time.time()
    directory = root / CHECKPOINT_DIR
    path = directory / f"{checkpoint_id}.json"
    latest_path = directory / LATEST_CHECKPOINT_FILE
    try:
        with workspace_write_lock(root):
            directory.mkdir(parents=True, exist_ok=True)
            atomic_write_json(path, checkpoint)
            atomic_write_json(latest_path, checkpoint)
    except OSError as exc:
        return {"status": "ERROR", "error": type(exc).__name__, "path": str(path)}
    except TypeError as exc:
        return {"status": "ERROR", "error": type(exc).__name__, "path": str(path)}
    return {"status": "OK", "path": str(path), "latest_path": str(latest_path), "checkpoint_id": checkpoint_id}


def load_task_checkpoint(workspace_root: str | Path, checkpoint_id: str = "latest") -> dict[str, Any]:
    root = Path(workspace_root).resolve()
    checkpoint_path = _checkpoint_path(root, checkpoint_id)
    if checkpoint_path is None:
        return {"status": "SKIP", "error": "invalid checkpoint id", "checkpoint_id": checkpoint_id}
    if not checkpoint_path.is_file():
        return {"status": "SKIP", "error": "checkpoint not found", "path": str(checkpoint_path)}
    try:
        payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except OSError as exc:
        return {"status": "ERROR", "error": type(exc).__name__, "path": str(checkpoint_path)}
    except (json.JSONDecodeError, UnicodeError) as exc:
        return {"status": "ERROR", "error": type(exc).__name__, "path": str(checkpoint_path)}
    if not isinstance(payload, dict):
        return {"status": "ERROR", "error": "invalid checkpoint payload", "path": str(checkpoint_path)}
    return {"status": "OK", "checkpoint": payload, "path": str(checkpoint_path)}


def render_resume_prompt(checkpoint: dict[str, Any], query: str = "") -> str:
    task = str(checkpoint.get("task") or checkpoint.get("task_title") or "").strip()
    plan = str(checkpoint.get("plan") or "").strip()
    pending_steps = [str(item) for item in checkpoint.get("pending_steps", []) if str(item).strip()]
    tool_results = [item for item in checkpoint.get("tool_results", []) if isinstance(item, dict)]
    file_states = [item for item in checkpoint.get("file_states", []) if isinstance(item, dict)]
    pending_approval = checkpoint.get("pending_approval")
    context_state = checkpoint.get("context_state")

    parts = [
        "[Resume Checkpoint]",
        f"checkpoint_id: {checkpoint.get('checkpoint_id', '')}",
        f"previous_status: {checkpoint.get('status', '')}",
        f"previous_turn: {checkpoint.get('turn', 0)}",
        f"previous_exit_reason: {checkpoint.get('exit_reason', '')}",
    ]
    if task:
        parts.append(f"\nOriginal task:\n{task}")
    if plan:
        parts.append(f"\nCurrent plan.md:\n{plan}")
    if pending_steps:
        parts.append("\nPending steps:\n" + "\n".join(f"- {item}" for item in pending_steps[:20]))
    if tool_results:
        rendered = []
        for item in tool_results[-20:]:
            status = item.get("status", "")
            path = item.get("path", "")
            error = item.get("error", "")
            suffix = f" path={path}" if path else ""
            if error:
                suffix += f" error={error}"
            rendered.append(f"- {item.get('tool_name', 'tool')} status={status}{suffix}")
        parts.append("\nCompleted tool results:\n" + "\n".join(rendered))
    if file_states:
        rendered_states = []
        for item in file_states[:20]:
            rendered_states.append(
                f"- {item.get('path', '')}: exists={item.get('exists')} size={item.get('size')} "
                f"mtime_ns={item.get('mtime_ns')} sha256={item.get('sha256', '')}"
            )
        parts.append("\nKey file states:\n" + "\n".join(rendered_states))
    if isinstance(pending_approval, dict):
        parts.append(
            "\nApproval recovery:\n"
            f"- tool: {pending_approval.get('tool_name', '')}\n"
            f"- action_digest: {pending_approval.get('action_digest', '')}\n"
            f"- policy_version: {pending_approval.get('policy_version', '')}\n"
            f"- previous_status: {pending_approval.get('status', '')}\n"
            "The previous process did not persist a completed tool result for "
            "this approval. Treat the side-effect outcome as unknown: inspect "
            "current state first, and never replay or mutate automatically from "
            "this record."
        )
    if isinstance(context_state, dict):
        compaction = context_state.get("compaction")
        manifest = context_state.get("manifest")
        if isinstance(manifest, dict):
            parts.append(
                "\nContext recovery:\n"
                f"- manifest_id: {manifest.get('manifest_id', '')}\n"
                f"- manifest_digest: {context_state.get('manifest_digest', '')}\n"
                f"- visible_items: "
                f"{sum(1 for item in manifest.get('items', []) if isinstance(item, dict) and item.get('llm_visible'))}\n"
                f"- compacted_items: "
                f"{len(compaction) if isinstance(compaction, list) else 0}\n"
                "Only metadata and digests were checkpointed. Rebuild the LLM "
                "context from current authorized sources; do not recover raw "
                "content from hashes."
            )
    parts.append(
        "\nResume instruction:\n"
        "先核对关键文件当前状态，再从 Pending steps 继续；不要重放已完成工具动作。"
        "如 checkpoint 与当前文件状态冲突，先解释差异并选择最小验证步骤。"
    )
    if str(query or "").strip():
        parts.append(f"\nAdditional user instruction:\n{str(query).strip()}")
    return "\n".join(parts)


def _checkpoint_path(root: Path, checkpoint_id: str) -> Path | None:
    if str(checkpoint_id or "") == "latest":
        return root / CHECKPOINT_DIR / LATEST_CHECKPOINT_FILE
    safe = _safe_id(str(checkpoint_id or ""))
    if not safe:
        return None
    return root / CHECKPOINT_DIR / f"{safe}.json"


def _read_plan(root: Path) -> str:
    plan_path = root / "plan.md"
    if not plan_path.is_file():
        return ""
    try:
        return _truncate(plan_path.read_text(encoding="utf-8"), MAX_PLAN_CHARS)
    except (OSError, UnicodeError):
        return ""


def _pending_steps(plan: str, pending_prompts: list[str]) -> list[str]:
    steps: list[str] = []
    for line in plan.splitlines():
        match = PENDING_CHECKBOX_RE.match(line)
        if match:
            steps.append(_truncate(match.group(1), MAX_TEXT_FIELD_CHARS))
    for prompt in pending_prompts[-5:]:
        clean = _truncate(" ".join(str(prompt or "").split()), MAX_TEXT_FIELD_CHARS)
        if clean:
            steps.append(f"Loop prompt: {clean}")
    return steps[:30]


def _tool_summaries(tool_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for result in tool_results[-MAX_TOOL_RESULTS:]:
        data = result.get("data") if isinstance(result, dict) else None
        data_dict = data if isinstance(data, dict) else {}
        summary = {
            "tool_name": _truncate(str(result.get("tool_name", "tool")), 80),
            "tool_call_id": _truncate(str(result.get("tool_call_id", "")), 80),
            "status": _truncate(str(data_dict.get("status") or ("ERROR" if data_dict.get("error") else "OK")), 80),
        }
        path = data_dict.get("path")
        if path:
            summary["path"] = _truncate(str(path), MAX_TEXT_FIELD_CHARS)
        error = data_dict.get("error")
        if error:
            summary["error"] = _truncate(str(error), MAX_TEXT_FIELD_CHARS)
        summaries.append(summary)
    return summaries


def _file_states(root: Path, tool_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    states: list[dict[str, Any]] = []
    seen: set[Path] = set()
    for result in tool_results:
        data = result.get("data") if isinstance(result, dict) else None
        if not isinstance(data, dict) or not data.get("path"):
            continue
        path = Path(str(data["path"]))
        resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
        try:
            relative = resolved.relative_to(root)
        except ValueError:
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        states.append(_single_file_state(root, relative))
        if len(states) >= MAX_FILE_STATES:
            break
    return states


def _single_file_state(root: Path, relative: Path) -> dict[str, Any]:
    path = root / relative
    state: dict[str, Any] = {"path": str(relative), "exists": path.exists()}
    if not path.exists():
        return state
    try:
        stat = path.stat()
    except OSError as exc:
        state["error"] = type(exc).__name__
        return state
    state.update({
        "is_directory": path.is_dir(),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    })
    if path.is_file() and stat.st_size <= MAX_HASH_BYTES:
        try:
            state["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError as exc:
            state["error"] = type(exc).__name__
    return state


def _safe_working(working: dict[str, str]) -> dict[str, str]:
    return {str(key): _truncate(str(value), MAX_TEXT_FIELD_CHARS) for key, value in working.items()}


def _safe_digest(value: str) -> str:
    digest = str(value or "")
    if len(digest) == 64 and all(character in "0123456789abcdef" for character in digest):
        return digest
    return ""


def _safe_pending_approval(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    result: dict[str, Any] = {}
    for key in (
        "request_id",
        "tool_name",
        "status",
        "reason_code",
        "action_digest",
        "policy_version",
        "principal_digest",
        "approval_id",
    ):
        item = value.get(key)
        if item is not None:
            result[key] = _truncate(str(item), MAX_TEXT_FIELD_CHARS)
    for key in ("requested_at", "expires_at", "resolved_at"):
        item = value.get(key)
        if isinstance(item, (int, float)) and not isinstance(item, bool):
            result[key] = float(item)
    preview = value.get("parameter_preview")
    if isinstance(preview, dict):
        safe_preview: dict[str, Any] = {}
        for key, item in preview.items():
            if isinstance(item, str):
                safe_preview[_truncate(str(key), 80)] = _truncate(
                    item,
                    MAX_TEXT_FIELD_CHARS,
                )
            elif isinstance(item, (int, float, bool)) or item is None:
                safe_preview[_truncate(str(key), 80)] = item
        result["parameter_preview"] = safe_preview
    return result or None


def _safe_context_state(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    manifest = value.get("manifest")
    if not isinstance(manifest, dict):
        return None
    safe_items: list[dict[str, Any]] = []
    for raw in manifest.get("items", [])[:MAX_CONTEXT_ITEMS]:
        if not isinstance(raw, dict):
            continue
        item = {
            "schema_version": _safe_nonnegative_int(
                raw.get("schema_version", 1),
                default=1,
            ),
            "ref_id": _truncate(str(raw.get("ref_id", "")), 256),
            "kind": _truncate(str(raw.get("kind", "")), 80),
            "token_count": _safe_nonnegative_int(raw.get("token_count", 0)),
            "priority": min(
                100,
                _safe_nonnegative_int(raw.get("priority", 0)),
            ),
            "trust": _truncate(str(raw.get("trust", "")), 80),
            "source_sha256": _safe_digest(str(raw.get("source_sha256", ""))),
            "llm_visible": bool(raw.get("llm_visible", False)),
        }
        safe_items.append(item)
    safe_manifest = {
        "schema_version": _safe_nonnegative_int(
            manifest.get("schema_version", 1),
            default=1,
        ),
        "manifest_id": _truncate(str(manifest.get("manifest_id", "")), 256),
        "principal_digest": _safe_digest(
            str(manifest.get("principal_digest", ""))
        ),
        "max_input_tokens": _safe_nonnegative_int(
            manifest.get("max_input_tokens", 0)
        ),
        "reserved_output_tokens": _safe_nonnegative_int(
            manifest.get("reserved_output_tokens", 0),
        ),
        "items": safe_items,
    }
    safe_compaction: list[dict[str, Any]] = []
    for raw in value.get("compaction", [])[:MAX_CONTEXT_ITEMS]:
        if not isinstance(raw, dict):
            continue
        safe_compaction.append(
            {
                "ref_id": _truncate(str(raw.get("ref_id", "")), 256),
                "kind": _truncate(str(raw.get("kind", "")), 80),
                "source_sha256": _safe_digest(
                    str(raw.get("source_sha256", ""))
                ),
                "original_tokens": _safe_nonnegative_int(
                    raw.get("original_tokens", 0)
                ),
                "visible_tokens": _safe_nonnegative_int(
                    raw.get("visible_tokens", 0)
                ),
                "reason": _truncate(str(raw.get("reason", "")), 80),
            }
        )
    usage = value.get("component_usage")
    safe_usage = {
        _truncate(str(key), 80): _safe_nonnegative_int(item)
        for key, item in (usage.items() if isinstance(usage, dict) else ())
        if isinstance(item, int) and not isinstance(item, bool)
    }
    safe_session_compaction: list[dict[str, Any]] = []
    for raw in value.get("session_compaction", [])[-128:]:
        if not isinstance(raw, dict):
            continue
        safe_session_compaction.append(
            {
                "role": _truncate(str(raw.get("role", "")), 40),
                "source_sha256": _safe_digest(
                    str(raw.get("source_sha256", ""))
                ),
                "token_count": _safe_nonnegative_int(
                    raw.get("token_count", 0)
                ),
                "reason": _truncate(str(raw.get("reason", "")), 80),
            }
        )
    return {
        "manifest": safe_manifest,
        "manifest_digest": _safe_digest(
            str(value.get("manifest_digest", ""))
        ),
        "component_usage": safe_usage,
        "compaction": safe_compaction,
        "session_compaction": safe_session_compaction,
    }


def _safe_nonnegative_int(value: Any, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return max(0, result)


def _normalize_status(status: str, exit_reason: str) -> str:
    raw = str(status or "").strip().lower()
    if raw in {"running", "waiting_approval", "completed", "interrupted", "failed"}:
        return raw
    if exit_reason == "CURRENT_TASK_DONE":
        return "completed"
    if exit_reason == "INTERRUPTED":
        return "interrupted"
    if exit_reason:
        return "failed"
    return "running"


def _safe_id(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip())[:120].strip(".-")
    return safe or "checkpoint"


def _task_title(task: str) -> str:
    clean = str(task or "").strip()
    first = clean.splitlines()[0] if clean else "Untitled task"
    return _truncate(" ".join(first.split()), 120)


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 18)].rstrip() + "\n...[truncated]..."
