from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_MAX_INJECT_CHARS = 6000
_SAFE_SKILL_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


@dataclass(frozen=True)
class SkillManifest:
    name: str
    description: str
    path: Path
    triggers: tuple[str, ...] = ()
    max_inject_chars: int = DEFAULT_MAX_INJECT_CHARS


class SkillRegistry:
    def __init__(self, skills: dict[str, SkillManifest] | None = None) -> None:
        self.skills = skills or {}

    @classmethod
    def load(cls, paths: list[Path]) -> "SkillRegistry":
        skills: dict[str, SkillManifest] = {}
        for root in paths:
            if not root.exists() or not root.is_dir():
                continue
            for skill_dir in sorted(root.iterdir()):
                manifest = cls._load_manifest(skill_dir)
                if manifest is not None:
                    skills[manifest.name] = manifest
        return cls(skills)

    @staticmethod
    def _load_manifest(skill_dir: Path) -> SkillManifest | None:
        if not skill_dir.is_dir():
            return None
        skill_file = skill_dir / "SKILL.md"
        if not skill_file.exists() or not _is_relative_to(skill_file.resolve(), skill_dir.resolve()):
            return None

        meta = _read_meta(skill_dir / "_meta.json")
        name = str(meta.get("name") or skill_dir.name).strip()
        if not is_safe_skill_name(name):
            return None
        description = str(meta.get("description") or _first_heading_or_line(skill_file)).strip()
        triggers_raw = meta.get("triggers", [])
        triggers = tuple(str(item).strip() for item in triggers_raw if str(item).strip()) if isinstance(triggers_raw, list) else ()
        try:
            max_inject_chars = int(meta.get("max_inject_chars", DEFAULT_MAX_INJECT_CHARS))
        except (TypeError, ValueError):
            max_inject_chars = DEFAULT_MAX_INJECT_CHARS
        max_inject_chars = max(500, min(max_inject_chars, 20000))
        return SkillManifest(
            name=name,
            description=description,
            path=skill_dir.resolve(),
            triggers=triggers,
            max_inject_chars=max_inject_chars,
        )

    def get(self, name: str) -> SkillManifest | None:
        if not is_safe_skill_name(name):
            return None
        return self.skills.get(name)

    def list_index(self) -> str:
        lines = []
        for name in sorted(self.skills):
            manifest = self.skills[name]
            lines.append(f"- {manifest.name}: {manifest.description}")
        return "\n".join(lines)

    def read_skill_content(self, name: str) -> str:
        manifest = self.get(name)
        if manifest is None:
            return ""
        skill_file = (manifest.path / "SKILL.md").resolve()
        if not _is_relative_to(skill_file, manifest.path):
            return ""
        text = skill_file.read_text(encoding="utf-8").strip()
        if len(text) <= manifest.max_inject_chars:
            return text
        return text[: manifest.max_inject_chars].rstrip() + "\n...[truncated]..."


def is_safe_skill_name(name: str) -> bool:
    return bool(name and _SAFE_SKILL_NAME_RE.fullmatch(name))


def select_skills(query: str, registry: SkillRegistry, limit: int = 3) -> list[str]:
    query_l = query.lower()
    scored: list[tuple[int, str]] = []
    for name, manifest in registry.skills.items():
        score = 0
        if name.lower() in query_l:
            score += 3
        for trigger in manifest.triggers:
            trigger_l = trigger.lower()
            if trigger_l and trigger_l in query_l:
                score += 2
        for token in _tokens(manifest.description):
            if len(token) >= 4 and token in query_l:
                score += 1
        if score > 0:
            scored.append((score, name))
    scored.sort(key=lambda item: (-item[0], item[1]))
    return [name for _, name in scored[:limit]]


def render_active_skills(active_names: list[str], registry: SkillRegistry) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    for name in active_names:
        if name in seen:
            continue
        seen.add(name)
        content = registry.read_skill_content(name)
        if content:
            parts.append(f'<skill name="{name}">\n{content}\n</skill>')
    if not parts:
        return ""
    return "<active_skills>\n" + "\n\n".join(parts) + "\n</active_skills>"


def dedupe_skill_names(names: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for name in names:
        name = str(name).strip()
        if not is_safe_skill_name(name) or name in seen:
            continue
        seen.add(name)
        result.append(name)
    return result


def _read_meta(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _first_heading_or_line(path: Path) -> str:
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip().lstrip("#").strip()
            if line:
                return line
    except OSError:
        return ""
    return ""


def _tokens(text: str) -> set[str]:
    return {item.lower() for item in re.findall(r"[A-Za-z0-9_\-]+", text)}


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True
