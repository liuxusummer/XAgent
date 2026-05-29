from __future__ import annotations

import json
from pathlib import Path
from typing import Any


AGENT_PROFILE_FIELDS = (
    "name",
    "description",
    "tools",
    "model",
    "maxTurns",
    "memory",
    "skills",
    "project_agents",
)
AGENT_LIST_FIELDS = {"tools", "skills", "project_agents"}


def is_safe_child_name(name: str) -> bool:
    path = Path(name)
    return bool(name) and not path.is_absolute() and path.name == name and name not in {".", ".."}


def default_agent_profile(agent_name: str) -> dict[str, Any]:
    return {
        "name": agent_name,
        "description": "",
        "tools": [],
        "model": "",
        "maxTurns": 300,
        "memory": "",
        "skills": [],
        "project_agents": [],
    }


def split_agent_frontmatter(content: str) -> tuple[str, str] | None:
    lines = content.splitlines()
    if not lines or lines[0].strip() != "---":
        return None
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            frontmatter = "\n".join(lines[1:index])
            body = "\n".join(lines[index + 1 :]).lstrip("\n")
            if content.endswith("\n"):
                body += "\n"
            return frontmatter, body
    return None


def strip_yaml_comment(value: str) -> str:
    in_single = False
    in_double = False
    escaped = False
    for index, char in enumerate(value):
        if escaped:
            escaped = False
            continue
        if char == "\\" and in_double:
            escaped = True
            continue
        if char == "'" and not in_double:
            in_single = not in_single
            continue
        if char == '"' and not in_single:
            in_double = not in_double
            continue
        if char == "#" and not in_single and not in_double:
            return value[:index].rstrip()
    return value.strip()


def parse_scalar(value: str) -> Any:
    value = strip_yaml_comment(value)
    if value == "":
        return ""
    if value == "[]":
        return []
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        return value


def parse_inline_list(value: str) -> list[str]:
    value = strip_yaml_comment(value)
    if value == "[]":
        return []
    if not (value.startswith("[") and value.endswith("]")):
        parsed = parse_scalar(value)
        return parsed if isinstance(parsed, list) else []
    inner = value[1:-1].strip()
    if not inner:
        return []
    return [str(parse_scalar(item.strip())) for item in inner.split(",") if item.strip()]


def parse_agent_markdown(content: str, agent_name: str) -> dict[str, Any]:
    profile = default_agent_profile(agent_name)
    split = split_agent_frontmatter(content)
    if split is None:
        return {"profile": profile, "body": content}

    frontmatter, body = split
    current_list_key: str | None = None
    for raw_line in frontmatter.splitlines():
        if not raw_line.strip():
            continue
        stripped = raw_line.strip()
        if stripped.startswith("- ") and current_list_key:
            profile[current_list_key].append(str(parse_scalar(stripped[2:].strip())))
            continue
        if ":" not in raw_line:
            current_list_key = None
            continue
        key, raw_value = raw_line.split(":", 1)
        key = key.strip()
        if key not in AGENT_PROFILE_FIELDS:
            current_list_key = None
            continue
        value = raw_value.strip()
        if key in AGENT_LIST_FIELDS:
            if value:
                profile[key] = parse_inline_list(value)
                current_list_key = None
            else:
                profile[key] = []
                current_list_key = key
        else:
            profile[key] = parse_scalar(value)
            current_list_key = None
    if not isinstance(profile.get("name"), str) or not profile["name"]:
        profile["name"] = agent_name
    return {"profile": profile, "body": body}


def yaml_string(value: Any) -> str:
    return json.dumps(str(value), ensure_ascii=False)


def serialize_agent_markdown(profile: dict[str, Any], body: str) -> str:
    normalized = default_agent_profile(str(profile.get("name") or "agent"))
    for key in AGENT_PROFILE_FIELDS:
        if key in profile:
            normalized[key] = profile[key]

    lines = ["---"]
    for key in AGENT_PROFILE_FIELDS:
        value = normalized[key]
        if key in AGENT_LIST_FIELDS:
            items = value if isinstance(value, list) else []
            if not items:
                lines.append(f"{key}: []")
            else:
                lines.append(f"{key}:")
                lines.extend(f"  - {yaml_string(item)}" for item in items)
        elif isinstance(value, int):
            lines.append(f"{key}: {value}")
        else:
            lines.append(f"{key}: {yaml_string(value)}")
    lines.append("---")
    normalized_body = body.lstrip("\n")
    return "\n".join(lines) + "\n\n" + normalized_body


def read_agent_profile(agent_dir: str | Path, agent_name: str) -> dict[str, Any]:
    agent_path = Path(agent_dir)
    agent_file = agent_path / "AGENT.md"
    if not agent_file.is_file():
        parsed = {"profile": default_agent_profile(agent_name), "body": ""}
        content = serialize_agent_markdown(parsed["profile"], parsed["body"])
    else:
        content = agent_file.read_text(encoding="utf-8")
        parsed = parse_agent_markdown(content, agent_name)
    return {
        "agent": agent_name,
        "path": f"system/agents/{agent_name}/AGENT.md",
        "profile": parsed["profile"],
        "body": parsed["body"],
        "content": content,
    }


def positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def read_optional_text(path: str | Path) -> str:
    file_path = Path(path)
    if not file_path.is_file():
        return ""
    return file_path.read_text(encoding="utf-8")


def agent_dir_from_workspace(workspace_dir: str | Path, agent_name: str) -> tuple[Path | None, str | None]:
    if not agent_name:
        return None, None
    if not is_safe_child_name(agent_name):
        return None, "Invalid agent name"
    workspace = Path(workspace_dir).resolve()
    if not workspace.is_dir():
        return None, "Workspace not found"
    agents_root = (workspace / "system" / "agents").resolve()
    agent_dir = (agents_root / agent_name).resolve()
    try:
        agent_dir.relative_to(agents_root)
    except ValueError:
        return None, "Path traversal not allowed"
    if not agent_dir.is_dir():
        return None, "Agent not found"
    return agent_dir, None


def load_agent_runtime_config(workspace_dir: str | Path, agent_name: str) -> tuple[dict[str, Any] | None, str | None]:
    if not agent_name:
        return None, None
    agent_dir, error = agent_dir_from_workspace(workspace_dir, agent_name)
    if error or agent_dir is None:
        return None, error
    try:
        profile_data = read_agent_profile(agent_dir, agent_name)
        profile = profile_data["profile"]
        model_override = str(profile.get("model") or "").strip()
        return {
            "agent_prompt": profile_data["body"],
            "agent_soul": read_optional_text(agent_dir / "SOUL.md"),
            "tools_allowlist": string_list(profile.get("tools")),
            "skill_allowlist": string_list(profile.get("skills")),
            "model_override": model_override,
            "max_turns": positive_int(profile.get("maxTurns")),
            "memory_mode": str(profile.get("memory") or "project").strip() or "project",
        }, None
    except (OSError, UnicodeDecodeError) as exc:
        return None, str(exc)
