from __future__ import annotations

import re
import sys
import threading
import time
from dataclasses import dataclass, field
from types import GeneratorType
from typing import Any, Callable

from src.core.llm import ChatResponse
from src.core.telemetry import Event, EventSink, NullSink


CODE_BLOCK_PATTERN = re.compile(r"```[\s\S]+?```")
EMPTY_RESPONSE_LIMIT = 3


@dataclass
class ActionResult:
    data: Any
    next_prompt: str | None
    should_exit: bool = False
    flags: frozenset[str] = frozenset()


def _default_display(msg: str) -> None:
    print(msg, file=sys.stderr)


@dataclass
class AgentContext:
    working: dict[str, str] = field(
        default_factory=lambda: {"key_info": "", "related_sop": ""}
    )
    cwd: str = ""
    memory_root: str = ""
    current_turn: int = 0
    history_info: list[str] = field(default_factory=list)
    # 代码执行级中断信号。语义：由代码执行子模块（例如未来的 web_execute_js 长任务
    # 监控）写入 True，主循环下一轮检测到后退出循环。目前生产路径暂无写入者，仅
    # 测试场景直接注入。主循环在检测到中断并 break 后会自动复位，避免残留影响
    # 下一次 run_task。
    code_stop_signal: bool = False
    done_hooks: list[str] = field(default_factory=list)
    empty_count: int = 0
    verbose: bool = False
    display_fn: Callable[[str], None] = field(default_factory=lambda: _default_display)
    user_input_fn: Callable[[str], str] | None = None
    stop_signal: threading.Event | None = None
    skills: Any | None = None
    active_skills: list[str] = field(default_factory=list)
    # Phase 8 观测性：每次 run_task 刷新 session_id；sink 默认 NullSink 零开销
    session_id: str = ""
    sink: EventSink = field(default_factory=NullSink)


@dataclass
class TurnEndHook:
    name: str
    fn: Callable[..., str | None]
    priority: int = 0


def exhaust(generator: GeneratorType) -> Any:
    try:
        while True:
            next(generator)
    except StopIteration as stop:
        return stop.value


class BaseHandler:
    def __init__(self, ctx: AgentContext | None = None) -> None:
        self.ctx = ctx or AgentContext()
        self._turn_end_hooks: list[TurnEndHook] = []
        self._latest_response: ChatResponse | None = None

    def tool_before_callback(self, _tool_name: str, _args: dict[str, Any]) -> None:
        del _tool_name, _args
        return None

    def tool_after_callback(self, _tool_name: str, result: ActionResult) -> ActionResult:
        del _tool_name
        return result

    def turn_end_callback(
        self,
        response: ChatResponse,
        tool_results: list[dict[str, Any]] | None = None,
    ) -> str | None:
        prompt_parts: list[str] = []
        sink = self.ctx.sink
        for hook in sorted(self._turn_end_hooks, key=lambda item: -item.priority):
            part = hook.fn(response=response, tool_results=tool_results or [], ctx=self.ctx)
            if part:
                prompt_parts.append(part)
                sink.emit(
                    Event(
                        session_id=self.ctx.session_id,
                        turn=self.ctx.current_turn,
                        kind="hook_inject",
                        name=hook.name,
                        data={"prompt_len": len(part)},
                    )
                )
        if not prompt_parts:
            return None
        return "\n".join(prompt_parts)

    def dispatch(
        self,
        tool_name: str,
        args: dict[str, Any] | None,
        response: ChatResponse | None = None,
    ):
        args = args or {}
        sink = self.ctx.sink
        started_at = time.time()
        sink.emit(
            Event(
                session_id=self.ctx.session_id,
                turn=self.ctx.current_turn,
                kind="tool_start",
                name=tool_name,
                data={
                    "args_len": len(str(args)),
                    "active_skills": list(getattr(self.ctx, "active_skills", []) or []),
                },
            )
        )

        def _emit_tool_end(result: ActionResult) -> None:
            sink.emit(
                Event(
                    session_id=self.ctx.session_id,
                    turn=self.ctx.current_turn,
                    kind="tool_end",
                    name=tool_name,
                    duration_ms=(time.time() - started_at) * 1000,
                    data={
                        "should_exit": result.should_exit,
                        "next_prompt_len": len(result.next_prompt or ""),
                        "flags": sorted(result.flags),
                        "status": result.data.get("status") if isinstance(result.data, dict) else None,
                        "active_skills": list(getattr(self.ctx, "active_skills", []) or []),
                    },
                )
            )

        if tool_name == "bad_json":
            result = ActionResult(
                data={"error": "invalid tool json", "raw": args.get("raw", "")},
                next_prompt="工具调用 JSON 非法，请只输出一个合法的 <tool_use> 块。",
                flags=frozenset({"retry"}),
            )
            _emit_tool_end(result)
            return result

        method = getattr(self, f"exec_{tool_name}", None)
        if method is None:
            result = ActionResult(
                data={"error": f"unknown tool: {tool_name}"},
                next_prompt=f"未知工具 {tool_name}，请改用已提供工具重试。",
                flags=frozenset({"reset_tools"}),
            )
            _emit_tool_end(result)
            return result

        if isinstance(response, ChatResponse):
            self._latest_response = response
        self.tool_before_callback(tool_name, args)
        raw_result = method(args)
        if isinstance(raw_result, GeneratorType):
            result = yield from raw_result
        else:
            result = raw_result
        if not isinstance(result, ActionResult):
            raise TypeError(f"tool {tool_name} must return ActionResult, got {type(result)!r}")
        final = self.tool_after_callback(tool_name, result)
        _emit_tool_end(final)
        return final


def handle_no_tool_call(
    handler: BaseHandler,
    response: ChatResponse,
) -> ActionResult:
    content = (response.content or "").strip()
    raw_text = str(response.raw or "")
    if response.stop_reason in {"max_tokens", "length"} or raw_text.rstrip().endswith("!!!Error"):
        handler.ctx.empty_count = 0
        return ActionResult(
            data={
                "status": "RETRYABLE_RESPONSE_ERROR",
                "stop_reason": response.stop_reason,
            },
            next_prompt="上轮响应可能被截断或流异常中断。请基于当前任务重试，并确保给出完整结果或合法的工具调用。",
            flags=frozenset({"retry"}),
        )

    if not content:
        handler.ctx.empty_count += 1
        if handler.ctx.empty_count >= EMPTY_RESPONSE_LIMIT:
            return ActionResult(
                data={"status": "EMPTY_RESPONSE", "empty_count": handler.ctx.empty_count},
                next_prompt="",
                should_exit=True,
            )
        return ActionResult(
            data={"status": "EMPTY_RESPONSE", "empty_count": handler.ctx.empty_count},
            next_prompt="你刚才没有输出有效内容。请直接给出最终答案，或调用合适的工具继续执行。",
            flags=frozenset({"retry"}),
        )

    if CODE_BLOCK_PATTERN.search(content):
        handler.ctx.empty_count = 0
        return ActionResult(
            data={"status": "CODE_BLOCK_WITHOUT_TOOL", "content": content},
            next_prompt="你输出了代码块但没有调用工具。若需要执行代码，请调用 code_run；若只是汇报结果，请不要输出代码块。",
            flags=frozenset({"retry"}),
        )

    handler.ctx.empty_count = 0
    return ActionResult(
        data={"status": "DONE", "content": content},
        next_prompt=None,
    )


def run_agent_loop(
    client: Any,
    system_prompt: str,
    user_input: str,
    handler: BaseHandler,
    tools_schema: list[dict[str, Any]],
    max_turns: int = 40,
    stop_event: threading.Event | None = None,
) -> dict[str, Any]:
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_input},
    ]
    final_response = ""
    all_tool_results: list[dict[str, Any]] = []
    exit_reason = "MAX_TURNS_EXCEEDED"

    sink = handler.ctx.sink
    session_id = handler.ctx.session_id
    run_started_at = time.time()

    def _skill_state_data() -> dict[str, Any]:
        active_skills = list(getattr(handler.ctx, "active_skills", []) or [])
        return {"active_skills": active_skills}

    sink.emit(
        Event(
            session_id=session_id,
            turn=0,
            kind="run_start",
            name=user_input[:80],
            data={"query_len": len(user_input), "max_turns": max_turns, **_skill_state_data()},
        )
    )

    def _emit_run_end() -> None:
        sink.emit(
            Event(
                session_id=session_id,
                turn=handler.ctx.current_turn,
                kind="run_end",
                name=exit_reason,
                duration_ms=(time.time() - run_started_at) * 1000,
                data={"turns": handler.ctx.current_turn, **_skill_state_data()},
            )
        )

    def _check_interrupt() -> bool:
        if stop_event is not None and stop_event.is_set():
            return True
        if handler.ctx.code_stop_signal:
            return True
        return False

    def _emit_turn_end(tool_count: int) -> None:
        sink.emit(
            Event(
                session_id=session_id,
                turn=handler.ctx.current_turn,
                kind="turn_end",
                name="",
                data={"tool_count": tool_count, **_skill_state_data()},
            )
        )

    def build_next_user_message(
        prompt_parts: list[str],
        tool_results: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if hasattr(handler, "align_history_info"):
            session_history_size = None
            backend = getattr(client, "backend", None)
            if backend is not None and hasattr(backend, "history"):
                session_history_size = len(backend.history)
            handler.align_history_info(session_history_size)

        anchor_prompt = None
        if hasattr(handler, "get_anchor_prompt"):
            anchor_prompt = handler.get_anchor_prompt()
        merged_parts = [part for part in [anchor_prompt, *prompt_parts] if part]
        return [
            {
                "role": "user",
                "content": "\n\n".join(merged_parts),
                "tool_results": tool_results,
            }
        ]

    for turn in range(1, max_turns + 1):
        handler.ctx.current_turn = turn
        if _check_interrupt():
            exit_reason = "INTERRUPTED"
            # 复位 code_stop_signal，防止一次性的代码级中断残留到下一次 run_task。
            # stop_event 由调用方（XAgent.run_task）在每次任务开始前 clear。
            handler.ctx.code_stop_signal = False
            break

        sink.emit(Event(session_id=session_id, turn=turn, kind="turn_start", name="", data=_skill_state_data()))

        handler.ctx.display_fn(f"[Turn {turn}]")

        llm_started_at = time.time()
        response = client.chat(messages=messages, tools=tools_schema)
        sink.emit(
            Event(
                session_id=session_id,
                turn=turn,
                kind="llm_end",
                name=response.stop_reason or "",
                duration_ms=(time.time() - llm_started_at) * 1000,
                data={
                    "has_tool_calls": bool(response.tool_calls),
                    "content_len": len(response.content or ""),
                    "tool_call_count": len(response.tool_calls or []),
                    **_skill_state_data(),
                },
            )
        )
        if response.content.strip():
            final_response = response.content.strip()

        if not response.tool_calls:
            result = handle_no_tool_call(handler, response)
            all_tool_results.append(
                {
                    "tool_name": "no_tool",
                    "tool_call_id": "",
                    "data": result.data,
                }
            )
            turn_end_prompt = handler.turn_end_callback(response, [])
            _emit_turn_end(0)
            if result.should_exit or result.next_prompt == "":
                exit_reason = "EXITED"
                break
            if result.next_prompt is None:
                exit_reason = "CURRENT_TASK_DONE"
                break
            next_prompts = [result.next_prompt]
            if turn_end_prompt:
                next_prompts.append(turn_end_prompt)
            messages = build_next_user_message(next_prompts, [])
            continue

        next_prompts: list[str] = []
        turn_tool_results: list[dict[str, Any]] = []

        for tool_call in response.tool_calls:
            handler.ctx.display_fn(f"  tool: {tool_call.name}")
            dispatched = handler.dispatch(tool_call.name, tool_call.args, response=response)
            if isinstance(dispatched, GeneratorType):
                result = exhaust(dispatched)
            else:
                result = dispatched
            if "reset_tools" in result.flags and hasattr(client, "last_tools"):
                client.last_tools = ""

            tool_result = {
                "tool_name": tool_call.name,
                "tool_call_id": tool_call.id,
                "data": result.data,
            }
            turn_tool_results.append(tool_result)
            all_tool_results.append(tool_result)

            if result.next_prompt not in (None, ""):
                next_prompts.append(result.next_prompt)

            if result.should_exit or result.next_prompt == "":
                exit_reason = "EXITED"
                break
            if result.next_prompt is None:
                exit_reason = "CURRENT_TASK_DONE"
                break

        if exit_reason in {"EXITED", "CURRENT_TASK_DONE"}:
            _emit_turn_end(len(turn_tool_results))
            break

        turn_end_prompt = handler.turn_end_callback(response, turn_tool_results)
        if turn_end_prompt:
            next_prompts.append(turn_end_prompt)

        if not next_prompts:
            if handler.ctx.done_hooks:
                next_prompts.append(handler.ctx.done_hooks.pop(0))
            else:
                exit_reason = "CURRENT_TASK_DONE"
                _emit_turn_end(len(turn_tool_results))
                break

        messages = build_next_user_message(next_prompts, turn_tool_results)
        _emit_turn_end(len(turn_tool_results))

    handler.ctx.display_fn(f"[Done] exit_reason={exit_reason}, turns={handler.ctx.current_turn}")

    _emit_run_end()
    return {
        "response": final_response,
        "exit_reason": exit_reason,
        "tool_results": all_tool_results,
        "turns": handler.ctx.current_turn,
    }
