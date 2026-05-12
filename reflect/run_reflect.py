from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

from src.config import SessionConfig, create_client, load_config
from src.core.agent_loop import AgentContext, run_agent_loop
from src.core.llm import OpenAITextSession, ToolClient
from src.handler import XAgentHandler


PROJECT_ROOT = Path(__file__).resolve().parent.parent
MEMORY_DIR = PROJECT_ROOT / "memory"
ASSETS_DIR = PROJECT_ROOT / "src" / "assets"

REFLECT_PROMPT = (
    "请对当前记忆进行结算：\n"
    "1. 读取 memory/global_mem.txt 和 memory/global_mem_insight.txt\n"
    "2. 回顾当前对话中的关键发现和决策\n"
    "3. 判断是否有需要更新/清理/整合的内容\n"
    "4. 如果有变化，使用 file_patch 执行最小化更新\n"
    "5. 如果无实质变化，直接报告无需更新\n"
)


def build_reflect_agent(config_path: str | None = None) -> tuple:
    if config_path:
        configs = load_config(config_path)
        if configs:
            first_config = next(iter(configs.values()))
            client = create_client(first_config)
        else:
            raise ValueError(f"no valid config found in {config_path}")
    else:
        api_key = os.environ.get("OPENAI_API_KEY", "")
        base_url = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions")
        model = os.environ.get("OPENAI_MODEL", "gpt-4o")
        session = OpenAITextSession(
            api_key=api_key,
            base_url=base_url,
            model=model,
        )
        client = ToolClient(backend=session)

    sys_prompt_path = ASSETS_DIR / "sys_prompt.txt"
    tools_schema_path = ASSETS_DIR / "tools_schema.json"

    base_prompt = sys_prompt_path.read_text(encoding="utf-8")
    tools_schema = json.loads(tools_schema_path.read_text(encoding="utf-8"))

    memory_parts: list[str] = []
    for name in ("global_mem_insight.txt", "insight_fixed_structure.txt"):
        p = MEMORY_DIR / name
        if p.exists():
            memory_parts.append(p.read_text(encoding="utf-8").strip())
    memory_content = "\n\n".join(memory_parts)

    today = datetime.now().strftime("%Y-%m-%d %a")
    cwd = str(PROJECT_ROOT)
    system_prompt = (
        base_prompt
        + f"\n\n[动态注入]\nToday: {today}\n[Memory]\n{memory_content}\ncwd = {cwd}"
    )

    handler = XAgentHandler(ctx=AgentContext(cwd=cwd))
    return client, system_prompt, tools_schema, handler


def run_reflect(config_path: str | None = None) -> dict:
    client, system_prompt, tools_schema, handler = build_reflect_agent(config_path)
    result = run_agent_loop(
        client=client,
        system_prompt=system_prompt,
        user_input=REFLECT_PROMPT,
        handler=handler,
        tools_schema=tools_schema,
        max_turns=20,
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="XAgent Memory Reflect")
    parser.add_argument("--config", type=str, default=None, help="Path to config JSON file")
    args = parser.parse_args()

    print(f"[Reflect] {datetime.now().isoformat()} 开始记忆结算...")
    result = run_reflect(config_path=args.config)
    print(f"[Reflect] exit_reason={result.get('exit_reason')}, turns={result.get('turns')}")
    response = result.get("response", "")
    if response:
        print(f"[Reflect] 结果：{response[:500]}")


if __name__ == "__main__":
    main()
