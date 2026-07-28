from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from src.core.XAgent import XAgent, resolve_workspace_dir
from src.core.agent_profiles import load_agent_runtime_config
from src.core.agent_teams import render_team_prompt
from src.core.memory import load_boot_memory, load_effective_memory
from src.core.observability import build_langfuse_sink
from src.core.skills import SkillRegistry
from src.core.telemetry import EventSink, JsonlSink, MultiSink, NullSink, StderrSink


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def load_tools_schema(path: Path) -> list[dict]:
    return json.loads(read_text(path))


def filter_tools_schema(tools_schema: list[dict], allowed_tools: list[str] | set[str] | None) -> list[dict]:
    if allowed_tools is None:
        return tools_schema
    allowed = {str(name).strip() for name in allowed_tools if str(name).strip()}
    return [
        item
        for item in tools_schema
        if item.get("type") == "function"
        and item.get("function", {}).get("name") in allowed
    ]


def load_memory_content(project_root: Path) -> str:
    return load_boot_memory(project_root / "memory").content


def load_workspace_memory_content(
    workspace_dir: str | Path,
    agent_name: str = "",
    memory_mode: str = "project",
) -> str:
    return load_effective_memory(workspace_dir, agent_name, memory_mode).content


def build_system_prompt(
    assets_dir: Path,
    project_root: Path,
    workspace_dir: str,
    skill_registry: SkillRegistry | None = None,
    agent_name: str = "",
    agent_prompt: str = "",
    agent_soul: str = "",
    memory_mode: str = "project",
    team_config: dict[str, Any] | None = None,
) -> str:
    base_parts: list[str] = []
    if agent_prompt.strip() or agent_soul.strip():
        if agent_prompt.strip():
            base_parts.append(agent_prompt.strip())
        if agent_soul.strip():
            base_parts.append(agent_soul.strip())
    else:
        base_parts.append(read_text(assets_dir / "sys_prompt.txt"))

    workspace_path = Path(workspace_dir)
    workspace_memory = load_workspace_memory_content(workspace_path, agent_name, memory_mode)
    if str(memory_mode or "project").strip().lower() == "none":
        memory_content = ""
    elif (workspace_path / "system").is_dir():
        memory_content = workspace_memory
    else:
        memory_content = workspace_memory if workspace_memory else load_memory_content(project_root)
    today = datetime.now().strftime("%Y-%m-%d %a")
    dynamic = (
        f"\n\n[动态注入]\nToday: {today}\n[Memory]\n{memory_content}"
        f"\nworkspace = {workspace_dir}"
        f"\ncwd = {workspace_dir}"
        "\n相对路径默认基于 workspace 解析。"
    )
    if skill_registry is not None and skill_registry.skills:
        dynamic += (
            "\n\n[Available Skills]\n"
            "Skills are prompt instruction packs only. Activate relevant skills with skill_activate if needed.\n"
            f"{skill_registry.list_index()}"
        )
    team_prompt = render_team_prompt(team_config)
    if team_prompt:
        dynamic += f"\n\n{team_prompt}"
    return "\n\n".join(base_parts) + dynamic


def _append_unique_tool(allowed_tools: list[str] | None, tool_name: str) -> list[str] | None:
    if allowed_tools is None:
        return None
    normalized = [str(item).strip() for item in allowed_tools if str(item).strip()]
    if tool_name not in normalized:
        normalized.append(tool_name)
    return normalized


def _inherit_parent_runtime(child: XAgent, parent_ctx: Any | None) -> None:
    if parent_ctx is None:
        return
    child.handler.ctx.sink = parent_ctx.sink
    child.sink = parent_ctx.sink
    child.handler.ctx.verbose = getattr(parent_ctx, "verbose", False)
    child.handler.ctx.display_fn = parent_ctx.display_fn
    parent_input_fn = getattr(parent_ctx, "user_input_fn", None)
    if callable(parent_input_fn):
        child.handler.ctx.user_input_fn = parent_input_fn
    parent_stop_signal = getattr(parent_ctx, "stop_signal", None)
    if isinstance(parent_stop_signal, threading.Event):
        child.stop_event = parent_stop_signal
        child.owns_stop_event = False
        child.handler.ctx.stop_signal = parent_stop_signal


def _build_delegate_runner(
    *,
    config_path: str | None,
    observability_config_path: str | None,
    skills_dir: str | None,
    workspace: str,
    current_agent: str,
    team_config: dict[str, Any] | None,
):
    def _run_delegate(
        *,
        agent: str,
        task: str,
        context: str = "",
        expected_output: str = "",
        parent_ctx: Any | None = None,
    ) -> dict[str, Any]:
        if not isinstance(team_config, dict) or not team_config.get("name"):
            return {"status": "ERROR", "error": "no active team"}
        target = str(agent or "").strip()
        if not target:
            return {"status": "ERROR", "error": "agent is required"}
        if target == current_agent:
            return {"status": "ERROR", "error": "agent_delegate cannot delegate to the current leader"}
        members = team_config.get("members") if isinstance(team_config.get("members"), list) else []
        member = next((item for item in members if isinstance(item, dict) and item.get("agent") == target), None)
        if member is None:
            return {"status": "ERROR", "error": f"agent is not a member of active team: {target}"}
        if not bool(member.get("autoDelegate", True)):
            return {"status": "ERROR", "error": f"agent is manual-only in active team: {target}"}

        runtime_config, runtime_error = load_agent_runtime_config(workspace, target)
        if runtime_error:
            return {"status": "ERROR", "error": runtime_error, "agent": target}
        runtime_config = runtime_config or {}
        child_prompt = (
            "你正在作为团队成员 Agent 执行 leader 委派的子任务。"
            "你需要完成子任务并把结果汇报给 leader，不要假设你正在直接回复最终用户。\n\n"
            f"[Subtask]\n{task.strip()}"
        )
        if context.strip():
            child_prompt += f"\n\n[Leader Context]\n{context.strip()}"
        if expected_output.strip():
            child_prompt += f"\n\n[Expected Output]\n{expected_output.strip()}"

        child = build_agent(
            config_path=config_path,
            observability_config_path=observability_config_path,
            skills_dir=skills_dir,
            workspace_dir=workspace,
            agent_name=target,
            agent_prompt=str(runtime_config.get("agent_prompt", "")),
            agent_soul=str(runtime_config.get("agent_soul", "")),
            tools_allowlist=runtime_config.get("tools_allowlist"),
            skill_allowlist=runtime_config.get("skill_allowlist"),
            model_override=str(runtime_config.get("model_override", "")),
            max_turns=runtime_config.get("max_turns"),
            memory_mode=str(runtime_config.get("memory_mode", "project")),
            team_config=None,
        )
        original_child_sink = child.sink
        _inherit_parent_runtime(child, parent_ctx)
        try:
            result = child.run_task(child_prompt)
            exit_reason = str(result.get("exit_reason", ""))
            status = (
                "ERROR"
                if exit_reason == "ERROR"
                else ("INTERRUPTED" if exit_reason == "INTERRUPTED" else "OK")
            )
            return {
                "status": status,
                "agent": target,
                "team": team_config.get("name", ""),
                "exit_reason": exit_reason,
                "turns": result.get("turns", 0),
                "response": result.get("response", ""),
                "tool_result_count": len(result.get("tool_results", []) or []),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "ERROR", "agent": target, "error": str(exc)}
        finally:
            child.sink = original_child_sink
            child.close()

    return _run_delegate


def build_team_step_runner(
    *,
    config_path: str | None,
    observability_config_path: str | None,
    skills_dir: str | None,
    workspace: str,
    team_config: dict[str, Any] | None,
):
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

    def _run_step(
        *,
        agent: str,
        task: str,
        context: str = "",
        expected_output: str = "",
        step_id: str = "",
        max_turns: int | None = None,
        parent_ctx: Any | None = None,
    ) -> dict[str, Any]:
        target = str(agent or "").strip()
        if not target:
            return {"status": "ERROR", "error": "agent is required", "step_id": step_id}
        if allowed_agents and target not in allowed_agents:
            return {"status": "ERROR", "agent": target, "step_id": step_id, "error": f"agent is not enabled in team: {target}"}

        runtime_config, runtime_error = load_agent_runtime_config(workspace, target)
        if runtime_error:
            return {"status": "ERROR", "agent": target, "step_id": step_id, "error": runtime_error}
        runtime_config = runtime_config or {}
        step_prompt = (
            "你正在作为团队 workflow 的一个步骤执行任务。"
            "完成本步骤并输出可被后续步骤复用的结果，不要假设你正在直接回复最终用户。\n\n"
            f"[Workflow Step]\n{step_id}\n\n"
            f"[Step Task]\n{task.strip()}"
        )
        if context.strip():
            step_prompt += f"\n\n[Workflow Context]\n{context.strip()}"
        if expected_output.strip():
            step_prompt += f"\n\n[Expected Output]\n{expected_output.strip()}"

        child = build_agent(
            config_path=config_path,
            observability_config_path=observability_config_path,
            skills_dir=skills_dir,
            workspace_dir=workspace,
            agent_name=target,
            agent_prompt=str(runtime_config.get("agent_prompt", "")),
            agent_soul=str(runtime_config.get("agent_soul", "")),
            tools_allowlist=runtime_config.get("tools_allowlist"),
            skill_allowlist=runtime_config.get("skill_allowlist"),
            model_override=str(runtime_config.get("model_override", "")),
            max_turns=max_turns or runtime_config.get("max_turns"),
            memory_mode=str(runtime_config.get("memory_mode", "project")),
            team_config=None,
        )
        original_child_sink = child.sink
        _inherit_parent_runtime(child, parent_ctx)
        try:
            result = child.run_task(step_prompt)
            exit_reason = str(result.get("exit_reason", ""))
            status = (
                "ERROR"
                if exit_reason == "ERROR"
                else ("INTERRUPTED" if exit_reason == "INTERRUPTED" else "OK")
            )
            return {
                "status": status,
                "agent": target,
                "step_id": step_id,
                "team": team_config.get("name", "") if isinstance(team_config, dict) else "",
                "exit_reason": exit_reason,
                "turns": result.get("turns", 0),
                "response": result.get("response", ""),
                "tool_result_count": len(result.get("tool_results", []) or []),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "ERROR", "agent": target, "step_id": step_id, "error": str(exc)}
        finally:
            child.sink = original_child_sink
            child.close()

    return _run_step


def load_observability_config(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("observability config must be a JSON object")
    return raw


def build_sink(observability_config: dict[str, Any] | None = None) -> EventSink:
    """按环境变量装配 sink：
    - XAGENT_LOG_DIR=/path       → JsonlSink(path)
    - XAGENT_LOG_STDERR=1        → 叠加 StderrSink
    - XAGENT_OBS_BACKEND=langfuse 或 XAGENT_LANGFUSE_ENABLED=1 → Langfuse sink
    - 全部未设 → NullSink
    """
    observability_config = observability_config or {}
    sinks: list[EventSink] = []
    log_dir = (os.environ.get("XAGENT_LOG_DIR", "") or str(observability_config.get("log_dir", ""))).strip()
    if log_dir:
        sinks.append(JsonlSink(log_dir))
    langfuse_sink = build_langfuse_sink(observability_config)
    if langfuse_sink is not None:
        sinks.append(langfuse_sink)
    stderr_enabled = os.environ.get("XAGENT_LOG_STDERR", "")
    if not stderr_enabled and observability_config.get("stderr") is True:
        stderr_enabled = "1"
    if stderr_enabled == "1":
        sinks.append(StderrSink())
    if not sinks:
        return NullSink()
    if len(sinks) == 1:
        return sinks[0]
    return MultiSink(*sinks)


def build_agent(
    config_path: str | None = None,
    observability_config_path: str | None = None,
    skills_dir: str | None = None,
    workspace_dir: str | None = None,
    agent_name: str = "",
    agent_prompt: str = "",
    agent_soul: str = "",
    tools_allowlist: list[str] | None = None,
    skill_allowlist: list[str] | None = None,
    model_override: str = "",
    max_turns: int | None = None,
    memory_mode: str = "project",
    team_config: dict[str, Any] | None = None,
) -> XAgent:
    project_root = Path(__file__).resolve().parent.parent
    assets_dir = Path(__file__).resolve().parent / "assets"
    workspace = resolve_workspace_dir(workspace_dir, project_root)
    observability_config = load_observability_config(observability_config_path)
    skill_registry = SkillRegistry.load([Path(skills_dir)] if skills_dir else [project_root / "skills"])
    prompt_skill_registry = skill_registry
    if skill_allowlist is not None:
        allowed_skills = {str(name).strip() for name in skill_allowlist if str(name).strip()}
        prompt_skill_registry = SkillRegistry(
            {name: manifest for name, manifest in skill_registry.skills.items() if name in allowed_skills}
        )

    system_prompt = build_system_prompt(
        assets_dir,
        project_root,
        workspace,
        skill_registry=prompt_skill_registry,
        agent_name=agent_name,
        agent_prompt=agent_prompt,
        agent_soul=agent_soul,
        memory_mode=memory_mode,
        team_config=team_config,
    )
    if team_config is not None:
        tools_allowlist = _append_unique_tool(tools_allowlist, "agent_delegate")
    tools_schema = filter_tools_schema(load_tools_schema(assets_dir / "tools_schema.json"), tools_allowlist)

    api_key = os.environ.get("OPENAI_API_KEY", "")
    base_url = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions")
    model = model_override

    agent = XAgent(
        system_prompt=system_prompt,
        tools_schema=tools_schema,
        api_key=api_key,
        base_url=base_url,
        model=model,
        workspace_dir=workspace,
        config_path=config_path,
        sink=build_sink(observability_config),
        skills_dir=skills_dir,
        max_turns=max_turns or 40,
        agent_name=agent_name,
        memory_mode=memory_mode,
        team_config=team_config,
        delegate_runner=_build_delegate_runner(
            config_path=config_path,
            observability_config_path=observability_config_path,
            skills_dir=skills_dir,
            workspace=workspace,
            current_agent=agent_name,
            team_config=team_config,
        ),
    )
    allowed_tools = {item["function"]["name"] for item in tools_schema if item.get("type") == "function"}
    agent.handler.ctx.allowed_tools = allowed_tools if tools_allowlist is not None else None
    agent.handler.ctx.skill_allowlist = set(skill_allowlist or []) if skill_allowlist is not None else None
    agent.skill_registry = skill_registry
    agent.handler.ctx.skills = skill_registry
    return agent


def _parse_session_value(value_str: str) -> Any:
    try:
        return json.loads(value_str)
    except (json.JSONDecodeError, ValueError):
        pass
    if os.path.isfile(value_str):
        return Path(value_str).read_text(encoding="utf-8")
    return value_str


def handle_session_command(agent: XAgent, key: str, value_str: str) -> None:
    value = _parse_session_value(value_str)
    backend = getattr(agent.client, "backend", None)
    if backend is None:
        print("[error] no backend session found")
        return
    allowed_attrs = {"temperature", "max_tokens", "reasoning_effort", "thinking_type"}
    if key not in allowed_attrs:
        print(f"[error] cannot set '{key}', allowed: {sorted(allowed_attrs)}")
        return
    old = getattr(backend, key, "<unset>")
    setattr(backend, key, value)
    print(f"[session] {key}: {old!r} → {value!r}")


def handle_verbose_command(agent: XAgent, value: str) -> None:
    if value.lower() in ("on", "true", "1"):
        agent.handler.ctx.verbose = True
        print("[verbose] on")
    elif value.lower() in ("off", "false", "0"):
        agent.handler.ctx.verbose = False
        print("[verbose] off")
    else:
        print("[error] usage: /verbose on|off")


def handle_command(agent: XAgent, line: str) -> None:
    parts = line[1:].strip()
    if parts.startswith("session."):
        kv = parts[len("session."):]
        if "=" not in kv:
            print("[error] usage: /session.key=value")
            return
        key, value_str = kv.split("=", 1)
        handle_session_command(agent, key.strip(), value_str.strip())
    elif parts.startswith("verbose"):
        rest = parts[len("verbose"):].strip()
        handle_verbose_command(agent, rest)
    elif parts == "stop":
        if agent.is_running():
            agent.stop()
            print("[stop] interrupt signal sent")
        else:
            print("[stop] no task running")
    elif parts in ("exit", "quit", "q"):
        raise SystemExit(0)
    elif parts == "help":
        print("Commands:")
        print("  /session.key=value  Set session attribute (temperature, max_tokens, etc.)")
        print("  /verbose on|off     Toggle verbose output")
        print("  /stop               Interrupt current task")
        print("  /exit               Exit REPL")
    else:
        print(f"[error] unknown command: /{parts}, type /help for usage")


_XML_TAG_RE = re.compile(r"</?(?:thinking|tool_use|summary|history|key_info|earlier_context)[^>]*>")


def _clean_content(text: str) -> str:
    text = _XML_TAG_RE.sub("", text)
    lines = text.split("\n")
    if len(lines) > 6:
        return "\n".join(lines[:3]) + f"\n... ({len(lines) - 6} lines omitted) ...\n" + "\n".join(lines[-3:])
    return text


def format_tool_result(tool_result: dict[str, Any]) -> str:
    name = tool_result.get("tool_name", "?")
    data = tool_result.get("data", {})
    if isinstance(data, dict):
        status = data.get("status", "")
        compact = f"{name}({status})"
        if "content" in data:
            content = str(data["content"])
            if len(content) > 100:
                content = content[:100] + "..."
            compact += f" {content}"
        return compact
    return f"{name}({str(data)[:80]})"


def format_result(result: dict[str, Any], verbose: bool) -> str:
    if verbose:
        parts: list[str] = []
        for tr in result.get("tool_results", []):
            name = tr.get("tool_name", "?")
            data = tr.get("data", {})
            parts.append(f"🛠️ {name}")
            if isinstance(data, dict):
                for k, v in data.items():
                    val_str = _clean_content(str(v))
                    parts.append(f"  {k}: {val_str}")
        response = result.get("response", "")
        if response:
            parts.append(_clean_content(response))
        return "\n".join(parts)
    else:
        response = result.get("response", "")
        if response:
            return _clean_content(response)
        tool_results = result.get("tool_results", [])
        if tool_results:
            return " | ".join(format_tool_result(tr) for tr in tool_results)
        return "(no output)"


def repl(agent: XAgent) -> None:
    print("XAgent REPL — type /help for commands, /exit to quit")

    def drain_display_queue() -> None:
        while True:
            msg = agent.display_queue.get()
            if "done" in msg:
                result = msg["done"]
                print(format_result(result, agent.handler.ctx.verbose))
                print(f"[exit_reason] {result.get('exit_reason', '')}")
                break
            elif "progress" in msg:
                print(msg["progress"], file=sys.stderr)
            elif "ask_user" in msg:
                message = msg["ask_user"]
                reply = input(f"🤖 {message}\n👤 ")
                agent.reply_queue.put(reply)

    while True:
        try:
            if agent.is_running():
                drain_display_queue()
            else:
                try:
                    line = input("XAgent> ").strip()
                except EOFError:
                    break
                if not line:
                    continue
                if line.startswith("/"):
                    handle_command(agent, line)
                    continue

                agent.run_task_async(line)
                drain_display_queue()
        except KeyboardInterrupt:
            if agent.is_running():
                agent.stop()
                print("\n[interrupted]")
            else:
                print()
                break


def main() -> None:
    parser = argparse.ArgumentParser(description="XAgent CLI")
    parser.add_argument("--config", type=str, default=None, help="Path to config JSON file")
    parser.add_argument(
        "--observability-config",
        type=str,
        default=None,
        help="Optional path to observability JSON config file",
    )
    parser.add_argument("--skills-dir", type=str, default=None, help="Optional path to local skills directory")
    parser.add_argument(
        "--workspace",
        type=str,
        default=None,
        help="Agent workspace directory; defaults to <project_root>/workspace",
    )
    args = parser.parse_args()

    agent = build_agent(
        config_path=args.config,
        observability_config_path=args.observability_config,
        skills_dir=args.skills_dir,
        workspace_dir=args.workspace,
    )
    try:
        repl(agent)
    finally:
        agent.close()


if __name__ == "__main__":
    main()
