from __future__ import annotations

from pathlib import Path
from queue import Queue
from typing import Any, Callable

from src.core.memory import load_memory_sop


MEMORY_DIR = Path(__file__).resolve().parent.parent.parent / "memory"
MAX_WORKING_CHECKPOINT_CHARS = 8_000

_default_input_fn: Callable[[str], str] = lambda prompt: input(prompt)


def make_progress_emitter(display_queue: Queue[dict[str, Any]]) -> Callable[[str], None]:
    def _emit_progress(message: str) -> None:
        if message:
            display_queue.put({"progress": message})

    return _emit_progress


def make_user_input_bridge(
    display_queue: Queue[dict[str, Any]],
    reply_queue: Queue[str],
) -> Callable[[str], str]:
    def _bridge_input(prompt: str) -> str:
        display_queue.put({"ask_user": prompt.strip()})
        return reply_queue.get()

    return _bridge_input


def ask_user(
    message: str,
    options: list[str] | None = None,
    input_fn: Callable[[str], str] | None = None,
) -> dict[str, Any]:
    fn = input_fn or _default_input_fn
    clean_options = [str(option).strip() for option in options or [] if str(option).strip()]
    prompt_message = format_user_prompt(message, clean_options)
    try:
        raw_reply = fn(f"\n🤖 {prompt_message}\n👤 ")
        user_reply, selected_option = resolve_user_reply(raw_reply, clean_options)
        result: dict[str, Any] = {
            "status": "OK",
            "message": message,
            "user_reply": user_reply,
        }
        if clean_options:
            result["options"] = clean_options
            result["raw_user_reply"] = raw_reply.strip()
            if selected_option is not None:
                result["selected_option"] = selected_option
        return result
    except (EOFError, KeyboardInterrupt):
        result = {
            "status": "SKIP",
            "message": message,
            "user_reply": "",
        }
        if clean_options:
            result["options"] = clean_options
        return result


def format_user_prompt(message: str, options: list[str]) -> str:
    if not options:
        return message
    option_lines = "\n".join(f"{index}. {option}" for index, option in enumerate(options, start=1))
    option_hint = " / ".join(options)
    return f"{message}\n\n选项：\n{option_lines}\n请回复：{option_hint}"


def resolve_user_reply(raw_reply: str, options: list[str]) -> tuple[str, str | None]:
    reply = raw_reply.strip()
    if not options:
        return reply, None
    if reply.isdigit():
        index = int(reply)
        if 1 <= index <= len(options):
            selected = options[index - 1]
            return selected, selected
    for option in options:
        if reply == option:
            return option, option
    return reply, None


def update_working_checkpoint(
    key_info: str | None = None,
    related_sop: str | None = None,
) -> dict[str, str]:
    for field_name, value in (
        ("key_info", key_info),
        ("related_sop", related_sop),
    ):
        if value is not None and len(value) > MAX_WORKING_CHECKPOINT_CHARS:
            return {
                "status": "ERROR",
                "error": (
                    f"{field_name} exceeds "
                    f"{MAX_WORKING_CHECKPOINT_CHARS} characters"
                ),
            }
    result: dict[str, str] = {"status": "OK"}
    if key_info is not None:
        result["key_info"] = key_info
    if related_sop is not None:
        result["related_sop"] = related_sop
    return result


def start_long_term_update() -> dict[str, str]:
    sop_content = load_memory_sop(MEMORY_DIR).content
    return {
        "status": "OK",
        "sop_content": sop_content,
        "instruction": (
            "请根据以上 SOP 对当前对话进行记忆结算：提取有来源的关键信息，"
            "判断记忆类型，并调用 memory_propose 创建待审核候选。"
            "禁止使用 file_write/file_patch 直接修改长期记忆。"
        ),
    }


def plan_update(task_dir: str | None, content: str | None = None) -> dict[str, Any]:
    """读或覆写 {task_dir}/plan.md。

    - task_dir 为 None → Plan 模式未启用，返回 SKIP
    - content is None → 读取；plan.md 不存在时 content 为空字符串
    - content 为字符串（含空串）→ 覆写
    """
    if not task_dir:
        return {
            "status": "SKIP",
            "message": "plan mode requires task_dir",
            "content": "",
        }
    plan_path = Path(task_dir) / "plan.md"
    if content is None:
        if plan_path.exists():
            existing = plan_path.read_text(encoding="utf-8")
        else:
            existing = ""
        return {"status": "OK", "mode": "read", "content": existing}
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(content, encoding="utf-8")
    return {"status": "OK", "mode": "write", "content": content}
