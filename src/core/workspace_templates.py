from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.core.XAgent import ensure_workspace_layout


_WORKSPACE_BASE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$")


@dataclass(frozen=True)
class TemplateAgent:
    name: str
    description: str
    tools: tuple[str, ...]
    skills: tuple[str, ...]
    body: str
    soul: str
    memory: str = ""
    model: str = ""
    max_turns: int = 80
    memory_mode: str = "project"
    project_agents: tuple[str, ...] = ()


@dataclass(frozen=True)
class TemplateSkill:
    name: str
    description: str
    content: str
    triggers: tuple[str, ...] = ()


@dataclass(frozen=True)
class TemplateTeamMember:
    agent: str
    role: str
    auto_delegate: bool = True


@dataclass(frozen=True)
class TemplateTeam:
    name: str
    description: str
    leader: str
    mode: str
    members: tuple[TemplateTeamMember, ...] = ()
    workflow: dict[str, Any] | None = None


@dataclass(frozen=True)
class WorkspaceTemplate:
    id: str
    name: str
    description: str
    directories: tuple[str, ...] = ()
    agents: tuple[TemplateAgent, ...] = ()
    teams: tuple[TemplateTeam, ...] = ()
    skills: tuple[TemplateSkill, ...] = ()
    memory_files: dict[str, str] = field(default_factory=dict)
    template_files: dict[str, str] = field(default_factory=dict)
    scheduled_tasks: tuple[dict[str, Any], ...] = ()

    def summary(self) -> dict[str, str]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
        }


def _common_tools() -> tuple[str, ...]:
    return (
        "file_read",
        "file_search",
        "file_write",
        "file_patch",
        "code_run",
        "web_scan",
        "web_execute_js",
        "ask_user",
        "update_working_checkpoint",
        "plan_update",
    )


def _agent(
    name: str,
    description: str,
    body: str,
    *,
    skills: tuple[str, ...] = (),
    tools: tuple[str, ...] | None = None,
    soul: str = "",
    memory: str = "",
    project_agents: tuple[str, ...] = (),
    max_turns: int = 80,
) -> TemplateAgent:
    return TemplateAgent(
        name=name,
        description=description,
        tools=tools or _common_tools(),
        skills=skills,
        body=body.strip() + "\n",
        soul=(soul or "保持简洁、先确认事实、只做与当前任务相关的动作。").strip() + "\n",
        memory=memory.strip() + "\n" if memory.strip() else "",
        project_agents=project_agents,
        max_turns=max_turns,
    )


def _skill(name: str, description: str, content: str, *triggers: str) -> TemplateSkill:
    return TemplateSkill(
        name=name,
        description=description,
        content=content.strip() + "\n",
        triggers=tuple(item for item in triggers if item),
    )


def _team_member(agent: str, role: str, *, auto_delegate: bool = True) -> TemplateTeamMember:
    return TemplateTeamMember(agent=agent, role=role, auto_delegate=auto_delegate)


def _team(
    name: str,
    description: str,
    leader: str,
    *members: TemplateTeamMember,
    mode: str = "leader_delegates",
    workflow: dict[str, Any] | None = None,
) -> TemplateTeam:
    return TemplateTeam(
        name=name,
        description=description,
        leader=leader,
        mode=mode,
        members=members,
        workflow=workflow,
    )


def _deepresearch_workflow() -> dict[str, Any]:
    return {
        "name": "deepresearch-workflow",
        "version": 1,
        "description": "Serial deep research workflow owned by the deepresearch team.",
        "steps": [
            {
                "id": "source_scout",
                "agent": "source_scout",
                "task": "围绕 {{input}} 检索权威资料，输出来源列表、可信度评级、关键数据和引用线索。",
                "expected_output": "source map with credible sources, dates, data points, and open gaps",
                "output": "sources",
            },
            {
                "id": "evidence_analyst",
                "agent": "evidence_analyst",
                "depends_on": ["source_scout"],
                "task": "基于资料侦察结果，抽取核心事实、证据强弱、冲突数据、假设和待验证问题。",
                "expected_output": "evidence table with claims, support level, contradictions, and caveats",
                "output": "evidence",
            },
            {
                "id": "synthesis_writer",
                "agent": "synthesis_writer",
                "depends_on": ["evidence_analyst"],
                "task": "基于证据分析结果，为 {{input}} 写一版结构化研究报告草稿。",
                "expected_output": "structured research draft with cited evidence and uncertainty notes",
                "output": "draft",
            },
            {
                "id": "research_critic",
                "agent": "research_critic",
                "depends_on": ["synthesis_writer"],
                "task": "审查研究草稿，指出证据缺口、过度推断、遗漏反例和需要修正的表述。",
                "expected_output": "critique with concrete corrections and missing evidence",
                "output": "critique",
            },
            {
                "id": "final",
                "agent": "main",
                "depends_on": ["synthesis_writer", "research_critic"],
                "task": "结合草稿和审查意见，输出面向用户的最终深度研究结果：{{input}}",
                "expected_output": "final user-facing research report",
                "output": "final",
            },
        ],
    }


def _task(task_id: str, name: str, prompt: str, agent: str, *, repeat: str = "none") -> dict[str, Any]:
    now = time.time()
    today = time.strftime("%Y-%m-%d", time.localtime(now))
    return {
        "id": task_id,
        "workspace": "",
        "name": name,
        "prompt": prompt,
        "agent": agent,
        "repeat": repeat,
        "date": today,
        "time": "09:00",
        "end_date": "",
        "interval_minutes": 0,
        "keep_one_chat": True,
        "chat_id": "",
        "status": "paused",
        "next_run": None,
        "last_run": None,
        "last_session_id": "",
        "last_error": "",
        "last_debug_run": None,
        "last_debug_session_id": "",
        "last_debug_error": "",
        "config_path": "",
        "observability_config_path": "",
        "created_at": now,
        "updated_at": now,
    }


def _frontmatter(agent: TemplateAgent) -> str:
    profile: dict[str, Any] = {
        "name": agent.name,
        "description": agent.description,
        "tools": list(agent.tools),
        "model": agent.model,
        "maxTurns": agent.max_turns,
        "memory": agent.memory_mode,
        "skills": list(agent.skills),
        "project_agents": list(agent.project_agents),
    }
    lines = ["---"]
    for key, value in profile.items():
        if isinstance(value, list):
            if value:
                lines.append(f"{key}:")
                lines.extend(f"  - {json.dumps(str(item), ensure_ascii=False)}" for item in value)
            else:
                lines.append(f"{key}: []")
        elif isinstance(value, int):
            lines.append(f"{key}: {value}")
        else:
            lines.append(f"{key}: {json.dumps(str(value), ensure_ascii=False)}")
    lines.append("---")
    return "\n".join(lines) + "\n\n" + agent.body


def _templates() -> dict[str, WorkspaceTemplate]:
    blank = WorkspaceTemplate(
        id="blank",
        name="Blank",
        description="Standard workspace layout with minimal main and coding agents.",
        agents=(
            _agent("main", "General-purpose workspace agent.", "你是该 workspace 的主 Agent，负责理解任务、协调工具并交付结果."),
            _agent("coding", "Focused coding agent.", "你是该 workspace 的编码 Agent，负责代码阅读、修改、测试和审查."),
        ),
    )

    code_project = WorkspaceTemplate(
        id="code_project",
        name="Code Project",
        description="Starter agents, skills, memory, and tasks for software projects.",
        directories=("business/src", "business/docs", "business/tests", "runtime/artifacts"),
        agents=(
            _agent(
                "main",
                "Coordinates coding work and delegates implementation details.",
                "你是代码项目的主 Agent。先厘清目标和风险，再协调 coding/review Agent 完成实现、验证和交付。",
                project_agents=("coding", "review"),
            ),
            _agent(
                "coding",
                "Implements focused code changes.",
                "你是代码实现 Agent。遵循最小 diff，先读相关文件，再修改、测试并报告结果。",
                skills=("code-review",),
            ),
            _agent(
                "review",
                "Reviews changes for regressions and missing tests.",
                "你是代码审查 Agent。优先指出 bug、回归风险和测试缺口，结论必须引用具体文件或证据。",
                skills=("code-review",),
            ),
        ),
        skills=(
            _skill(
                "code-review",
                "Review code changes for correctness, regression risk, and missing tests.",
                """
# Code Review

Use a review-first stance. Lead with concrete findings ordered by severity.
Prefer file and line references. If no issue is found, state that clearly and
name remaining test gaps.
""",
                "review",
                "code quality",
            ),
        ),
        memory_files={
            "project.md": "# Project Memory\n\n- Keep changes small and verify with focused tests.\n",
        },
        template_files={
            "code-review-checklist.md": "# Code Review Checklist\n\n- Behavior changed intentionally\n- Tests cover the changed path\n- No unrelated files were modified\n",
        },
        scheduled_tasks=(
            _task(
                "tpl-code-weekly-quality",
                "Weekly Code Quality Audit",
                "Review the workspace for code quality risks, stale tests, and docs drift. Do not modify files.",
                "review",
                repeat="weekly",
            ),
        ),
    )

    research_project = WorkspaceTemplate(
        id="research_project",
        name="Research Project",
        description="Agents, teams, and tasks for deep research, notes, and synthesis.",
        directories=("business/sources", "business/notes", "business/drafts", "runtime/citations"),
        agents=(
            _agent(
                "main",
                "Coordinates research work and delegates deep-research sub-tasks.",
                "你是研究项目主 Agent。先明确研究问题、范围和交付形式，再协调 deepresearch 团队完成检索、证据分析、综合写作和批判审查。",
                project_agents=("source_scout", "evidence_analyst", "synthesis_writer", "research_critic"),
            ),
            _agent(
                "source_scout",
                "Finds and triages relevant sources for a research question.",
                "你是资料侦察 Agent。围绕研究问题寻找候选资料、判断来源可信度、提取可追溯引用线索，并输出 source map。不要写最终结论。",
                tools=("file_read", "file_search", "web_scan", "web_execute_js", "update_working_checkpoint", "plan_update"),
                skills=("research-notes",),
            ),
            _agent(
                "evidence_analyst",
                "Extracts claims, evidence, assumptions, and contradictions.",
                "你是证据分析 Agent。对给定资料做事实抽取、证据分级、冲突识别和假设标注。必须区分事实、推断、观点和待验证信息。",
                tools=("file_read", "file_search", "code_run", "update_working_checkpoint", "plan_update"),
                skills=("research-notes",),
            ),
            _agent(
                "synthesis_writer",
                "Turns evidence into structured research briefs and drafts.",
                "你是综合写作 Agent。把已验证证据组织成结构化研究简报、提纲或草稿。输出必须保留证据链和不确定性，不编造引用。",
                tools=("file_read", "file_search", "file_write", "file_patch", "update_working_checkpoint", "plan_update"),
                skills=("research-notes",),
            ),
            _agent(
                "research_critic",
                "Reviews research for gaps, weak evidence, and overclaims.",
                "你是研究审查 Agent。专门寻找证据缺口、逻辑跳跃、过度结论、遗漏反例和引用不充分处。优先输出可执行的修正建议。",
                tools=("file_read", "file_search", "update_working_checkpoint", "plan_update"),
                skills=("research-notes",),
            ),
            _agent(
                "research",
                "Collects, organizes, and synthesizes research notes.",
                "你是通用研究 Agent。区分事实、推断和待验证信息，输出结构化研究笔记。",
                skills=("research-notes",),
            ),
        ),
        teams=(
            _team(
                "deepresearch",
                "深度研究团队：由主控 Agent 串行委派资料检索、证据分析、综合写作和批判审查。",
                "main",
                _team_member("source_scout", "source discovery and credibility triage"),
                _team_member("evidence_analyst", "claim extraction and evidence analysis"),
                _team_member("synthesis_writer", "structured synthesis and draft writing"),
                _team_member("research_critic", "gap analysis and overclaim review"),
                mode="leader_delegates",
                workflow=_deepresearch_workflow(),
            ),
        ),
        skills=(
            _skill(
                "research-notes",
                "Structure research notes with claims, evidence, and open questions.",
                """
# Research Notes

Separate claims, evidence, citations, assumptions, and open questions. Avoid
presenting unverified material as established fact.
""",
                "research",
                "citation",
            ),
        ),
        memory_files={"research.md": "# Research Memory\n\n- Keep source notes under business/notes.\n"},
        template_files={"literature-note.md": "# Source\n\n# Key Claims\n\n# Evidence\n\n# Open Questions\n"},
        scheduled_tasks=(
            _task("tpl-research-weekly-synthesis", "Weekly Research Synthesis", "Summarize new notes, unresolved questions, and next research steps.", "research", repeat="weekly"),
        ),
    )

    operations = WorkspaceTemplate(
        id="operations",
        name="Operations",
        description="Agents and tasks for recurring operational checks.",
        directories=("business/runbooks", "business/reports", "runtime/incidents"),
        agents=(
            _agent("main", "Coordinates operations tasks.", "你是运营任务主 Agent。关注状态、异常、待办和升级路径。", project_agents=("ops",)),
            _agent("ops", "Runs operational checks and produces concise reports.", "你是运营 Agent。按 runbook 检查，记录异常、影响、下一步和责任人。", skills=("ops-runbook",)),
        ),
        skills=(
            _skill(
                "ops-runbook",
                "Use runbook-style checks and escalation summaries.",
                """
# Ops Runbook

Report status, evidence, impact, next action, and escalation owner. Keep output
short enough for repeated daily use.
""",
                "ops",
                "runbook",
            ),
        ),
        memory_files={"operations.md": "# Operations Memory\n\n- Keep recurring reports under business/reports.\n"},
        template_files={"daily-report.md": "# Status\n\n# Exceptions\n\n# Next Actions\n"},
        scheduled_tasks=(
            _task("tpl-ops-daily-check", "Daily Operations Check", "Run the daily operations checklist and summarize exceptions.", "ops", repeat="daily"),
        ),
    )

    data_analysis = WorkspaceTemplate(
        id="data_analysis",
        name="Data Analysis",
        description="Agents and templates for dataset inspection and reports.",
        directories=("business/data", "business/notebooks", "business/reports", "runtime/plots"),
        agents=(
            _agent("main", "Coordinates analysis requests.", "你是数据分析工作区主 Agent。明确问题、数据位置、输出格式和验证口径。", project_agents=("data",)),
            _agent("data", "Profiles data and drafts analysis reports.", "你是数据分析 Agent。先检查数据质量，再计算、可视化并解释限制。", skills=("analysis-report",)),
        ),
        skills=(
            _skill(
                "analysis-report",
                "Produce analysis reports with assumptions, methods, and checks.",
                """
# Analysis Report

Include objective, inputs, assumptions, quality checks, method, findings, and
limitations. Prefer reproducible scripts for calculations.
""",
                "analysis",
                "dataset",
            ),
        ),
        memory_files={"data.md": "# Data Memory\n\n- Keep source datasets under business/data.\n"},
        template_files={"analysis-report.md": "# Objective\n\n# Inputs\n\n# Quality Checks\n\n# Findings\n\n# Limitations\n"},
        scheduled_tasks=(
            _task("tpl-data-quality-check", "Dataset Quality Check", "Inspect business/data for new datasets and summarize quality issues.", "data", repeat="weekly"),
        ),
    )

    personal_assistant = WorkspaceTemplate(
        id="personal_assistant",
        name="Personal Assistant",
        description="Agents and tasks for personal planning and lightweight organization.",
        directories=("business/inbox", "business/plans", "business/archive"),
        agents=(
            _agent("main", "Personal assistant coordinator.", "你是个人助理主 Agent。帮助整理事项、计划和回顾，必要时向用户确认优先级。", project_agents=("assistant",)),
            _agent("assistant", "Handles personal planning and inbox organization.", "你是个人助理 Agent。把输入整理成待办、日程、等待和归档，避免过度自动决策。", skills=("personal-planning",)),
        ),
        skills=(
            _skill(
                "personal-planning",
                "Organize personal tasks into actionable plans.",
                """
# Personal Planning

Separate now, next, scheduled, waiting, and archived items. Ask the user before
making irreversible decisions.
""",
                "plan",
                "inbox",
            ),
        ),
        memory_files={"personal.md": "# Personal Memory\n\n- Ask before changing personal priorities or deleting notes.\n"},
        template_files={"weekly-plan.md": "# Priorities\n\n# Calendar\n\n# Waiting\n\n# Notes\n"},
        scheduled_tasks=(
            _task("tpl-personal-daily-review", "Daily Personal Review", "Review inbox and plans, then summarize today's priorities.", "assistant", repeat="daily"),
        ),
    )

    return {
        item.id: item
        for item in (
            blank,
            code_project,
            research_project,
            operations,
            data_analysis,
            personal_assistant,
        )
    }


def list_workspace_templates() -> list[dict[str, str]]:
    return [template.summary() for template in _templates().values()]


def get_workspace_template(template_id: str) -> WorkspaceTemplate | None:
    return _templates().get(str(template_id or "").strip())


def normalize_workspace_name(name: str) -> tuple[str | None, str | None]:
    raw = str(name or "").strip()
    if not raw:
        return None, "Workspace name is required"
    if raw.endswith(".ws"):
        base = raw[:-3]
    else:
        base = raw
    if not _WORKSPACE_BASE_RE.fullmatch(base) or ".." in base.split("."):
        return None, "Invalid workspace name"
    return f"{base}.ws", None


def create_workspace_from_template(workspace_parent: str | Path, name: str, template_id: str) -> dict[str, str]:
    ws_name, name_error = normalize_workspace_name(name)
    if name_error or ws_name is None:
        raise ValueError(name_error or "Invalid workspace name")
    template = get_workspace_template(template_id)
    if template is None:
        raise ValueError("Unknown workspace template")

    parent = Path(workspace_parent).resolve()
    root = (parent / ws_name).resolve()
    if os.path.commonpath([str(parent), str(root)]) != str(parent):
        raise ValueError("Path traversal not allowed")
    if root.exists():
        raise FileExistsError("Workspace already exists")

    root.mkdir(parents=True)
    ensure_workspace_layout(root)
    _write_template(root, ws_name, template)
    return {
        "name": ws_name,
        "template_id": template.id,
        "path": str(root),
    }


def _write_template(root: Path, workspace_name: str, template: WorkspaceTemplate) -> None:
    for rel_dir in template.directories:
        _safe_path(root, rel_dir).mkdir(parents=True, exist_ok=True)
    for agent in template.agents:
        agent_dir = _safe_path(root, f"system/agents/{agent.name}")
        agent_dir.mkdir(parents=True, exist_ok=True)
        (agent_dir / "AGENT.md").write_text(_frontmatter(agent), encoding="utf-8")
        (agent_dir / "SOUL.md").write_text(agent.soul, encoding="utf-8")
        (agent_dir / "MEMORY.md").write_text(agent.memory, encoding="utf-8")
    for team in template.teams:
        team_path = _safe_path(root, f"system/teams/{team.name}.json")
        team_path.parent.mkdir(parents=True, exist_ok=True)
        team_config = {
            "name": team.name,
            "description": team.description,
            "leader": team.leader,
            "mode": team.mode,
            "members": [
                {
                    "agent": member.agent,
                    "role": member.role,
                    "autoDelegate": member.auto_delegate,
                }
                for member in team.members
            ],
            "created_at": time.time(),
            "updated_at": time.time(),
        }
        team_path.write_text(
            json.dumps(team_config, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if team.workflow:
            workflow_path = _safe_path(root, f"system/teams/{team.name}.workflow.json")
            workflow_path.write_text(
                json.dumps(team.workflow, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
    for skill in template.skills:
        skill_dir = _safe_path(root, f"system/skills/{skill.name}")
        skill_dir.mkdir(parents=True, exist_ok=True)
        (skill_dir / "SKILL.md").write_text(skill.content, encoding="utf-8")
        meta = {
            "name": skill.name,
            "description": skill.description,
            "triggers": list(skill.triggers),
        }
        (skill_dir / "_meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    for rel_path, content in template.memory_files.items():
        path = _safe_path(root, f"system/memory/{rel_path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    for rel_path, content in template.template_files.items():
        path = _safe_path(root, f"system/templates/{rel_path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    if template.scheduled_tasks:
        tasks = []
        for seed in template.scheduled_tasks:
            task = dict(seed)
            task["workspace"] = workspace_name
            tasks.append(task)
        task_path = _safe_path(root, "runtime/tasks/tasks.json")
        task_path.parent.mkdir(parents=True, exist_ok=True)
        task_path.write_text(
            json.dumps({"tasks": tasks, "updated_at": time.time()}, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )


def _safe_path(root: Path, relative_path: str) -> Path:
    path = (root / relative_path).resolve()
    if os.path.commonpath([str(root), str(path)]) != str(root):
        raise ValueError("Path traversal not allowed")
    return path
