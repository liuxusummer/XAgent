from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from src.core.XAgent import XAgent, resolve_workspace_dir
from src.core.memory import load_boot_memory
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


def build_system_prompt(
    assets_dir: Path,
    project_root: Path,
    workspace_dir: str,
    skill_registry: SkillRegistry | None = None,
    agent_name: str = "",
    agent_prompt: str = "",
    agent_soul: str = "",
) -> str:
    if agent_prompt.strip() or agent_soul.strip():
        agent_parts: list[str] = []
        if agent_prompt.strip():
            agent_parts.append(agent_prompt.strip())
        if agent_soul.strip():
            agent_parts.append(agent_soul.strip())
        return "\n\n".join(agent_parts)

    base = read_text(assets_dir / "sys_prompt.txt")
    memory_content = load_memory_content(project_root)
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
    return base + dynamic


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
    )
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
