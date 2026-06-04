from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from src.core.agent_profiles import is_safe_child_name


TEAM_MODES = {"manual", "leader_delegates", "roundtable_review"}
DEFAULT_TEAM_MODE = "leader_delegates"


def teams_root(workspace_dir: str | Path) -> Path:
    return Path(workspace_dir).resolve() / "system" / "teams"


def team_path(workspace_dir: str | Path, team_name: str) -> tuple[Path | None, str | None]:
    if not is_safe_child_name(team_name):
        return None, "Invalid team name"
    root = teams_root(workspace_dir)
    target = (root / f"{team_name}.json").resolve()
    try:
        target.relative_to(root.resolve())
    except ValueError:
        return None, "Path traversal not allowed"
    return target, None


def _agent_exists(workspace_dir: str | Path, agent_name: str) -> bool:
    if not is_safe_child_name(agent_name):
        return False
    return (Path(workspace_dir).resolve() / "system" / "agents" / agent_name).is_dir()


def normalize_team_config(
    raw: dict[str, Any],
    workspace_dir: str | Path,
    *,
    existing: dict[str, Any] | None = None,
    validate_agents: bool = True,
) -> tuple[dict[str, Any] | None, str | None]:
    name = str(raw.get("name") or "").strip()
    if not is_safe_child_name(name):
        return None, "Invalid team name"
    leader = str(raw.get("leader") or "main").strip()
    if not is_safe_child_name(leader):
        return None, "Invalid leader agent"
    if validate_agents and not _agent_exists(workspace_dir, leader):
        return None, f"Leader agent not found: {leader}"

    mode = str(raw.get("mode") or DEFAULT_TEAM_MODE).strip()
    if mode not in TEAM_MODES:
        mode = DEFAULT_TEAM_MODE

    seen: set[str] = set()
    members: list[dict[str, Any]] = []
    raw_members = raw.get("members")
    if isinstance(raw_members, list):
        for item in raw_members:
            member = item if isinstance(item, dict) else {"agent": item}
            agent = str(member.get("agent") or "").strip()
            if not is_safe_child_name(agent) or agent in seen:
                continue
            if validate_agents and not _agent_exists(workspace_dir, agent):
                return None, f"Member agent not found: {agent}"
            seen.add(agent)
            members.append(
                {
                    "agent": agent,
                    "role": str(member.get("role") or "").strip(),
                    "autoDelegate": bool(member.get("autoDelegate", True)),
                }
            )

    now = time.time()
    created_at = existing.get("created_at") if isinstance(existing, dict) else raw.get("created_at")
    try:
        created = float(created_at)
    except (TypeError, ValueError):
        created = now
    return {
        "name": name,
        "description": str(raw.get("description") or "").strip(),
        "leader": leader,
        "mode": mode,
        "members": members,
        "created_at": created,
        "updated_at": now,
    }, None


def read_team_config(workspace_dir: str | Path, team_name: str) -> tuple[dict[str, Any] | None, str | None]:
    path, error = team_path(workspace_dir, team_name)
    if error or path is None:
        return None, error
    if not path.is_file():
        return None, "Team not found"
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, str(exc)
    if not isinstance(raw, dict):
        return None, "Invalid team config"
    return normalize_team_config(raw, workspace_dir, existing=raw, validate_agents=False)


def list_team_configs(workspace_dir: str | Path) -> list[dict[str, Any]]:
    root = teams_root(workspace_dir)
    if not root.is_dir():
        return []
    teams: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.json")):
        if not path.is_file():
            continue
        if path.name.endswith(".workflow.json"):
            continue
        config, error = read_team_config(workspace_dir, path.stem)
        if error is None and config is not None:
            teams.append(config)
    return teams


def write_team_config(workspace_dir: str | Path, raw: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    name = str(raw.get("name") or "").strip()
    path, error = team_path(workspace_dir, name)
    if error or path is None:
        return None, error
    existing: dict[str, Any] | None = None
    if path.is_file():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            existing = loaded if isinstance(loaded, dict) else None
        except (OSError, json.JSONDecodeError):
            existing = None
    normalized, error = normalize_team_config(raw, workspace_dir, existing=existing, validate_agents=True)
    if error or normalized is None:
        return None, error
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return normalized, None


def delete_team_config(workspace_dir: str | Path, team_name: str) -> str | None:
    path, error = team_path(workspace_dir, team_name)
    if error or path is None:
        return error
    if not path.is_file():
        return "Team not found"
    try:
        path.unlink()
        workflow = path.with_name(f"{team_name}.workflow.json")
        if workflow.is_file():
            workflow.unlink()
    except OSError as exc:
        return str(exc)
    return None


def render_team_prompt(team_config: dict[str, Any] | None) -> str:
    if not isinstance(team_config, dict) or not team_config.get("name"):
        return ""
    lines = [
        "[Agent Team]",
        f"name: {team_config.get('name', '')}",
        f"mode: {team_config.get('mode', DEFAULT_TEAM_MODE)}",
        f"leader: {team_config.get('leader', '')}",
        "members:",
    ]
    members = team_config.get("members")
    if isinstance(members, list) and members:
        for item in members:
            if not isinstance(item, dict):
                continue
            enabled = "enabled" if item.get("autoDelegate", True) else "manual-only"
            role = str(item.get("role") or "").strip()
            role_text = f" role={role}" if role else ""
            lines.append(f"- {item.get('agent', '')} ({enabled}){role_text}")
    else:
        lines.append("- none")
    lines.append("Use agent_delegate only for enabled members when a sub-task fits their role.")
    return "\n".join(lines)
