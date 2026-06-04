from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Callable

from src.core.agent_profiles import is_safe_child_name


STEP_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$")
TEMPLATE_RE = re.compile(r"{{\s*([^{}]+?)\s*}}")
ON_ERROR_VALUES = {"stop", "continue"}


def workflow_path(workspace_dir: str | Path, team_name: str) -> tuple[Path | None, str | None]:
    if not is_safe_child_name(team_name):
        return None, "Invalid team name"
    root = Path(workspace_dir).resolve() / "system" / "teams"
    target = (root / f"{team_name}.workflow.json").resolve()
    try:
        target.relative_to(root.resolve())
    except ValueError:
        return None, "Path traversal not allowed"
    return target, None


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def normalize_team_workflow(
    raw: dict[str, Any],
    *,
    team_name: str,
    team_config: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    steps_raw = raw.get("steps")
    if not isinstance(steps_raw, list) or not steps_raw:
        return None, "Workflow steps must be a non-empty list"

    allowed_agents: set[str] = set()
    if isinstance(team_config, dict):
        leader = str(team_config.get("leader") or "").strip()
        if leader:
            allowed_agents.add(leader)
        members = team_config.get("members") if isinstance(team_config.get("members"), list) else []
        for member in members:
            if isinstance(member, dict) and member.get("autoDelegate", True):
                agent = str(member.get("agent") or "").strip()
                if agent:
                    allowed_agents.add(agent)

    seen: set[str] = set()
    normalized_steps: list[dict[str, Any]] = []
    for index, item in enumerate(steps_raw, start=1):
        if not isinstance(item, dict):
            return None, f"Workflow step #{index} must be an object"
        step_id = str(item.get("id") or "").strip()
        if not STEP_ID_RE.fullmatch(step_id):
            return None, f"Invalid workflow step id: {step_id or index}"
        if step_id in seen:
            return None, f"Duplicate workflow step id: {step_id}"
        agent = str(item.get("agent") or "").strip()
        if not is_safe_child_name(agent):
            return None, f"Invalid workflow step agent: {agent or step_id}"
        if allowed_agents and agent not in allowed_agents:
            return None, f"Workflow step agent is not enabled in team: {agent}"
        task = str(item.get("task") or "").strip()
        if not task:
            return None, f"Workflow step task is required: {step_id}"
        depends_on = _string_list(item.get("depends_on", item.get("dependsOn")))
        missing = [dep for dep in depends_on if dep not in seen]
        if missing:
            return None, f"Workflow step has unknown or forward dependency: {step_id}"
        on_error = str(item.get("on_error", item.get("onError", "stop")) or "stop").strip()
        if on_error not in ON_ERROR_VALUES:
            on_error = "stop"
        normalized_steps.append(
            {
                "id": step_id,
                "agent": agent,
                "task": task,
                "depends_on": depends_on,
                "context": str(item.get("context") or "").strip(),
                "expected_output": str(item.get("expected_output", item.get("expectedOutput", "")) or "").strip(),
                "output": str(item.get("output") or step_id).strip() or step_id,
                "on_error": on_error,
                "max_turns": _positive_int(item.get("max_turns", item.get("maxTurns"))),
            }
        )
        seen.add(step_id)

    return {
        "name": str(raw.get("name") or f"{team_name}-workflow").strip(),
        "version": _positive_int(raw.get("version")) or 1,
        "description": str(raw.get("description") or "").strip(),
        "steps": normalized_steps,
    }, None


def _positive_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def read_team_workflow(
    workspace_dir: str | Path,
    team_name: str,
    team_config: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    embedded = team_config.get("workflow") if isinstance(team_config, dict) else None
    if isinstance(embedded, dict):
        return normalize_team_workflow(embedded, team_name=team_name, team_config=team_config)

    path, error = workflow_path(workspace_dir, team_name)
    if error or path is None:
        return None, error
    if not path.is_file():
        return None, None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, str(exc)
    if not isinstance(raw, dict):
        return None, "Invalid team workflow"
    return normalize_team_workflow(raw, team_name=team_name, team_config=team_config)


def write_team_workflow(
    workspace_dir: str | Path,
    team_name: str,
    workflow: dict[str, Any],
    team_config: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    normalized, error = normalize_team_workflow(workflow, team_name=team_name, team_config=team_config)
    if error or normalized is None:
        return None, error
    path, error = workflow_path(workspace_dir, team_name)
    if error or path is None:
        return None, error
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return normalized, None


def _resolve_template_value(key: str, task: str, step_results: dict[str, dict[str, Any]]) -> str:
    key = key.strip()
    if key in {"input", "topic", "task"}:
        return task
    parts = key.split(".")
    if len(parts) >= 3 and parts[0] == "steps":
        step = step_results.get(parts[1]) or {}
        if parts[2] == "response":
            return str(step.get("response") or "")
        if parts[2] == "status":
            return str(step.get("status") or "")
        if parts[2] == "output":
            return str(step.get("response") or step.get("output") or "")
    return ""


def render_workflow_template(text: str, task: str, step_results: dict[str, dict[str, Any]]) -> str:
    return TEMPLATE_RE.sub(lambda match: _resolve_template_value(match.group(1), task, step_results), text)


def _dependency_context(step: dict[str, Any], step_results: dict[str, dict[str, Any]]) -> str:
    parts: list[str] = []
    for dep in step.get("depends_on", []):
        result = step_results.get(dep)
        if not result:
            continue
        parts.append(
            "\n".join(
                [
                    f"[Step {dep}]",
                    f"agent: {result.get('agent', '')}",
                    f"status: {result.get('status', '')}",
                    str(result.get("response") or result.get("error") or "").strip(),
                ]
            ).strip()
        )
    return "\n\n".join(part for part in parts if part)


def run_team_workflow(
    workflow: dict[str, Any],
    task: str,
    run_step: Callable[..., dict[str, Any]],
    *,
    parent_ctx: Any | None = None,
    emit: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    started_at = time.time()
    steps = workflow.get("steps") if isinstance(workflow.get("steps"), list) else []
    step_results: dict[str, dict[str, Any]] = {}
    tool_results: list[dict[str, Any]] = []
    exit_reason = "CURRENT_TASK_DONE"

    if emit:
        emit(f"[Team Workflow] start {workflow.get('name', '')} ({len(steps)} steps)")

    for step in steps:
        step_id = str(step.get("id") or "")
        agent = str(step.get("agent") or "")
        rendered_task = render_workflow_template(str(step.get("task") or ""), task, step_results)
        explicit_context = render_workflow_template(str(step.get("context") or ""), task, step_results)
        dependency_context = _dependency_context(step, step_results)
        context = "\n\n".join(part for part in [explicit_context, dependency_context] if part)
        expected_output = render_workflow_template(str(step.get("expected_output") or ""), task, step_results)
        if emit:
            emit(f"[Team Workflow] step {step_id}: {agent} start")
        try:
            result = run_step(
                agent=agent,
                task=rendered_task,
                context=context,
                expected_output=expected_output,
                step_id=step_id,
                max_turns=step.get("max_turns"),
                parent_ctx=parent_ctx,
            )
        except Exception as exc:  # noqa: BLE001
            result = {"status": "ERROR", "agent": agent, "error": str(exc)}
        if not isinstance(result, dict):
            result = {"status": "ERROR", "agent": agent, "error": f"step runner returned {type(result)!r}"}
        step_result = {
            "id": step_id,
            "agent": agent,
            "status": str(result.get("status") or "UNKNOWN"),
            "response": str(result.get("response") or ""),
            "error": str(result.get("error") or ""),
            "exit_reason": str(result.get("exit_reason") or ""),
            "turns": _int_or_zero(result.get("turns")),
            "tool_result_count": _int_or_zero(result.get("tool_result_count")),
        }
        step_results[step_id] = step_result
        tool_results.append(
            {
                "tool_name": "team_step",
                "tool_call_id": step_id,
                "data": step_result,
            }
        )
        if emit:
            emit(f"[Team Workflow] step {step_id}: {agent} {step_result['status']}")
        if step_result["status"] != "OK" and step.get("on_error") != "continue":
            exit_reason = "ERROR"
            break

    final_response = ""
    for result in reversed(list(step_results.values())):
        if result.get("response"):
            final_response = str(result["response"])
            break
    if not final_response:
        final_response = f"Team workflow {workflow.get('name', '')} finished with {len(step_results)} step(s)."

    if emit:
        emit(f"[Team Workflow] done exit_reason={exit_reason}, steps={len(step_results)}")
    return {
        "response": final_response,
        "exit_reason": exit_reason,
        "tool_results": tool_results,
        "turns": len(step_results),
        "team_workflow": {
            "name": workflow.get("name", ""),
            "step_count": len(step_results),
            "duration_ms": int((time.time() - started_at) * 1000),
            "steps": list(step_results.values()),
        },
    }
