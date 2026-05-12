from __future__ import annotations

from pathlib import Path
from queue import Queue
from typing import Any, Callable


MEMORY_DIR = Path(__file__).resolve().parent.parent.parent / "memory"

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


def ask_user(message: str, input_fn: Callable[[str], str] | None = None) -> dict[str, str]:
    fn = input_fn or _default_input_fn
    try:
        user_reply = fn(f"\n🤖 {message}\n👤 ")
        return {
            "status": "OK",
            "message": message,
            "user_reply": user_reply.strip(),
        }
    except (EOFError, KeyboardInterrupt):
        return {
            "status": "SKIP",
            "message": message,
            "user_reply": "",
        }


def update_working_checkpoint(
    key_info: str | None = None,
    related_sop: str | None = None,
) -> dict[str, str]:
    result: dict[str, str] = {"status": "OK"}
    if key_info is not None:
        result["key_info"] = key_info
    if related_sop is not None:
        result["related_sop"] = related_sop
    return result


def start_long_term_update() -> dict[str, str]:
    sop_path = MEMORY_DIR / "memory_management_sop.md"
    if sop_path.exists():
        sop_content = sop_path.read_text(encoding="utf-8").strip()
    else:
        sop_content = ""
    return {
        "status": "OK",
        "sop_content": sop_content,
        "instruction": "请根据以上 SOP 对当前对话进行记忆结算：提取关键信息，判断更新类型，执行最小化更新。",
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
