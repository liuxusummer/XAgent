from __future__ import annotations

import os
import re
import threading
from pathlib import Path
from typing import Any

from src.core.agent_loop import ActionResult, AgentContext, BaseHandler, TurnEndHook
from src.core.memory import load_global_memory
from src.core.skills import SkillRegistry, dedupe_skill_names, render_active_skills
from src.core.telemetry import Event
from src.tools import ask_user, delete_file, patch_file, plan_update, read_file, start_long_term_update, update_working_checkpoint, web_execute_js, web_scan, write_file
from src.tools.file_ops import resolve_path_for_operation
from src.tools.code_run import run_code_stream


SUMMARY_PATTERN = re.compile(r"<summary>\s*(.*?)\s*</summary>", re.DOTALL)
FILE_CONTENT_PATTERN = re.compile(r"<file_content>\s*(.*?)\s*</file_content>", re.DOTALL)
CODE_BLOCK_PATTERN = re.compile(r"```(?:[\w+-]+)?\n?([\s\S]*?)```", re.DOTALL)
MAX_HISTORY_INFO_ENTRIES = 60
PLAN_REMIND_INTERVAL = 15
PLAN_PREVIEW_CHARS = 400
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
    except ValueError:
        return False
    return True


def _is_delete_authorized(user_reply: str) -> bool:
    normalized = user_reply.strip().lower()
    return normalized in {"yes", "y", "ok", "confirm", "confirmed", "delete", "确认", "授权", "同意", "删除"}


class XAgentHandler(BaseHandler):
    run_code_stream = staticmethod(run_code_stream)

    def __init__(self, ctx: AgentContext | None = None, task_dir: str | None = None) -> None:
        super().__init__(ctx=ctx)
        self.task_dir = task_dir
        self._turn_end_hooks.extend([
            TurnEndHook(name="external_intervene", fn=self._external_intervene_hook, priority=30),
            TurnEndHook(name="plan_reminder", fn=self._plan_reminder_hook, priority=25),
            TurnEndHook(name="periodic_inject", fn=self._periodic_inject_hook, priority=20),
            TurnEndHook(name="summary_extract", fn=self._summary_extract_hook, priority=10),
        ])

    def _summary_extract_hook(
        self,
        response,
        tool_results,
        ctx: AgentContext,
    ) -> str | None:
        del tool_results
        raw_text = str(response.raw or "")
        summary_match = SUMMARY_PATTERN.search(raw_text)
        if not summary_match:
            content = (response.content or "").strip()
            if content and ctx.current_turn > 1:
                return "提醒：你没有输出 <summary>。每轮回复必须包含一个 <summary> 单行总结。"
            return None

        summary = summary_match.group(1).strip()
        if not summary:
            return None

        entry = f"[Agent] {summary}"
        if not ctx.history_info or ctx.history_info[-1] != entry:
            ctx.history_info.append(entry)
        return None

    def _external_intervene_hook(
        self,
        response,
        tool_results,
        ctx: AgentContext,
    ) -> str | None:
        del response, tool_results
        if not self.task_dir:
            return None
        parts: list[str] = []
        for signal_file in ("_keyinfo", "_intervene"):
            signal_path = Path(self.task_dir) / signal_file
            if signal_path.exists():
                content = signal_path.read_text(encoding="utf-8").strip()
                if content:
                    parts.append(content)
                signal_path.unlink(missing_ok=True)
        return "\n".join(parts) if parts else None

    def _periodic_inject_hook(
        self,
        response,
        tool_results,
        ctx: AgentContext,
    ) -> str | None:
        del response, tool_results
        turn = ctx.current_turn
        parts: list[str] = []
        if turn % 7 == 0 and turn > 0:
            parts.append(
                f"[DANGER] 已连续执行第 {turn} 轮。禁止无效重试。"
                "若无有效进展，必须切换策略或调用 ask_user 请求用户指导。"
            )
        if turn % 10 == 0 and turn > 0:
            memory_root = Path(ctx.memory_root) if ctx.memory_root else PROJECT_ROOT / "memory"
            mem_content = load_global_memory(memory_root).content
            if mem_content:
                parts.append(f"[Memory Refresh]\n{mem_content}")
        if turn % 65 == 0 and turn > 0:
            parts.append(
                f"[DANGER] 已连续执行第 {turn} 轮。必须总结当前情况并调用 ask_user 请求用户确认。"
            )
        return "\n".join(parts) if parts else None

    def _plan_reminder_hook(
        self,
        response,
        tool_results,
        ctx: AgentContext,
    ) -> str | None:
        del tool_results
        if not self.task_dir:
            return None
        turn = ctx.current_turn
        if turn == 0 or turn % PLAN_REMIND_INTERVAL != 0:
            return None
        plan_path = Path(self.task_dir) / "plan.md"
        if not plan_path.exists():
            return None
        # 若本轮已调用 plan_update，跳过本轮提醒（避免刚写完又被提醒）
        raw_text = str(response.raw or "")
        if "plan_update" in raw_text:
            return None
        plan_content = plan_path.read_text(encoding="utf-8")
        preview = plan_content[:PLAN_PREVIEW_CHARS]
        if len(plan_content) > PLAN_PREVIEW_CHARS:
            preview += "\n...[truncated]..."
        return (
            f"[Plan Alignment] 已执行 {turn} 轮。当前 plan.md 前 {PLAN_PREVIEW_CHARS} 字预览：\n"
            f"<plan_preview>\n{preview}\n</plan_preview>\n"
            "请对照计划判断是否偏离。若偏离，调用 plan_update 调整计划或切换策略。"
        )

    def align_history_info(self, session_history_size: int | None = None) -> None:
        if session_history_size is None:
            limit = MAX_HISTORY_INFO_ENTRIES
        else:
            limit = max(10, min(MAX_HISTORY_INFO_ENTRIES, session_history_size * 2))
        if len(self.ctx.history_info) > limit:
            self.ctx.history_info = self.ctx.history_info[-limit:]

    def get_anchor_prompt(self) -> str | None:
        parts: list[str] = [f"<current_turn>{self.ctx.current_turn}</current_turn>"]
        key_info = self.ctx.working.get("key_info", "").strip()
        related_sop = self.ctx.working.get("related_sop", "").strip()
        if key_info:
            parts.append(f"<key_info>\n{key_info}\n</key_info>")
        if related_sop:
            parts.append(f"<related_sop>\n{related_sop}\n</related_sop>")
        if self.ctx.cwd:
            parts.append(f"<workspace>\n{self.ctx.cwd}\n</workspace>")
            parts.append("相对路径默认基于 workspace 解析。")

        if self.ctx.history_info:
            earlier = self.ctx.history_info[:-30]
            recent = self.ctx.history_info[-30:]
            if earlier:
                parts.append(
                    "<earlier_context>\n"
                    f"[...前 {len(earlier)} 条摘要已折叠]\n"
                    "</earlier_context>"
                )
            parts.append("<history>\n" + "\n".join(recent) + "\n</history>")

        registry = self.ctx.skills
        if isinstance(registry, SkillRegistry) and self.ctx.active_skills:
            active_skills = render_active_skills(self.ctx.active_skills, registry)
            if active_skills:
                parts.append(active_skills)
                self.ctx.sink.emit(
                    Event(
                        session_id=self.ctx.session_id,
                        turn=self.ctx.current_turn,
                        kind="skill_injected",
                        name="active_skills",
                        data={
                            "active_count": len(self.ctx.active_skills),
                            "inject_len": len(active_skills),
                        },
                    )
                )

        return "\n".join(parts)

    def _latest_raw_response(self) -> str:
        return str(getattr(self._latest_response, "raw", "") or "")

    def _extract_file_content(self, args: dict[str, Any]) -> str:
        raw_text = self._latest_raw_response()
        tagged = FILE_CONTENT_PATTERN.search(raw_text)
        if tagged:
            return tagged.group(1).strip()
        code_block = CODE_BLOCK_PATTERN.search(raw_text)
        if code_block:
            return code_block.group(1).strip()
        return str(args.get("content", ""))

    def _extract_code_script(self, args: dict[str, Any]) -> str:
        script = str(args.get("script", "")).strip()
        if script:
            return script
        code = str(args.get("code", "")).strip()
        if code:
            return code
        raw_text = self._latest_raw_response()
        code_block = CODE_BLOCK_PATTERN.search(raw_text)
        if code_block:
            return code_block.group(1).strip()
        return ""

    def exec_code_run(self, args: dict[str, Any]) -> ActionResult:
        script = self._extract_code_script(args)
        language = str(args.get("language", "python"))
        try:
            timeout = int(args.get("timeout", 60))
        except (TypeError, ValueError):
            result = {"status": "ERROR", "error": f"invalid timeout: {args.get('timeout')!r}"}
        else:
            stdout_chunks: list[str] = []
            result = {
                "status": "ERROR",
                "error": "code execution did not produce a final result",
            }
            self.ctx.display_fn(f"  code_run: start ({language}, timeout={timeout}s)")
            stop_signal = self._stop_signal()
            for chunk in self.run_code_stream(
                script=script,
                language=language,
                timeout=timeout,
                cwd=self.ctx.cwd or None,
                stop_signal=stop_signal,
            ):
                chunk_type = chunk.get("type", "")
                chunk_data = chunk.get("data")
                if chunk_type == "stdout" and isinstance(chunk_data, str):
                    stdout_chunks.append(chunk_data)
                    self.ctx.display_fn(f"  code_run | {chunk_data.rstrip()}")
                elif chunk_type in {"result", "error"} and isinstance(chunk_data, dict):
                    result = dict(chunk_data)
            if stdout_chunks:
                result["stdout"] = "".join(stdout_chunks)
            self.ctx.display_fn(f"  code_run: done ({result.get('status', 'UNKNOWN')})")
        return ActionResult(
            data=result,
            next_prompt="代码执行完成，请基于 tool_results 判断任务是否完成；若未完成，继续调用工具。",
        )

    def _stop_signal(self) -> threading.Event | None:
        signal = getattr(self.ctx, "stop_signal", None)
        if isinstance(signal, threading.Event):
            return signal
        return None

    def exec_file_read(self, args: dict[str, Any]) -> ActionResult:
        path = str(args.get("path", ""))
        start_line = args.get("start_line")
        end_line = args.get("end_line")
        keyword = args.get("keyword")
        result = read_file(
            path=path,
            cwd=self.ctx.cwd or None,
            start_line=int(start_line) if start_line is not None else None,
            end_line=int(end_line) if end_line is not None else None,
            keyword=str(keyword) if keyword is not None else None,
        )
        next_prompt = "文件读取完成，请基于 tool_results 继续分析或执行下一步。"
        if self._is_memory_path(path):
            next_prompt += (
                "\n\n⚠️ 你正在操作记忆文件。请遵循 memory_management_sop.md 中的规范："
                "\n- 最小化更新：只修改需要变更的部分"
                "\n- 唯一性：避免重复条目"
                "\n- 格式：按主题分段，每段以 ## 开头"
                "\n- 优先使用 file_patch 而非 file_write"
            )
        return ActionResult(
            data=result,
            next_prompt=next_prompt,
        )

    @staticmethod
    def _is_memory_path(path: str) -> bool:
        normalized = os.path.normpath(path)
        parts = normalized.replace("\\", "/").split("/")
        return "memory" in parts

    def exec_file_write(self, args: dict[str, Any]) -> ActionResult:
        path = str(args.get("path", ""))
        content = self._extract_file_content(args)
        mode = str(args.get("mode", "overwrite"))
        result = write_file(
            path=path,
            content=content,
            mode=mode,
            cwd=self.ctx.cwd or None,
        )
        return ActionResult(
            data=result,
            next_prompt="文件写入完成，请基于 tool_results 判断是否需要验证或继续操作。",
        )

    def exec_file_patch(self, args: dict[str, Any]) -> ActionResult:
        path = str(args.get("path", ""))
        old_content = str(args.get("old_content", ""))
        new_content = str(args.get("new_content", ""))
        result = patch_file(
            path=path,
            old_content=old_content,
            new_content=new_content,
            cwd=self.ctx.cwd or None,
        )
        return ActionResult(
            data=result,
            next_prompt="文件 patch 执行完成，请基于 tool_results 判断是否需要 file_read 验证或继续修改。",
        )

    def exec_file_delete(self, args: dict[str, Any]) -> ActionResult:
        path = str(args.get("path", ""))
        recursive = bool(args.get("recursive", False))
        if not path:
            return ActionResult(
                data={"status": "ERROR", "error": "path is required"},
                next_prompt="文件删除未执行，请基于错误信息决定是否修正参数或停止。",
            )

        try:
            target = resolve_path_for_operation(path, self.ctx.cwd or None, operation="read")
            workspace = Path(self.ctx.cwd or os.getcwd()).resolve()
            location = "工作区内" if _is_relative_to(target, workspace) else "工作区外"
        except (OSError, ValueError) as exc:
            return ActionResult(
                data={"status": "ERROR", "error": str(exc)},
                next_prompt="文件删除未执行，请基于错误信息决定是否修正路径或停止。",
            )

        auth = ask_user(
            (
                "请确认是否授权删除以下路径：\n"
                f"- 路径：{target}\n"
                f"- 位置：{location}\n"
                f"- 递归删除：{'是' if recursive else '否'}\n"
                "- 注意：删除后系统无法自动回滚。\n"
                "如确认删除，请回复 yes / 确认 / 授权。"
            ),
            input_fn=self.ctx.user_input_fn,
        )
        if not _is_delete_authorized(auth.get("user_reply", "")):
            return ActionResult(
                data={
                    "status": "SKIP",
                    "error": "delete not authorized by user",
                    "path": str(target),
                    "authorization": auth,
                },
                next_prompt="用户未授权删除，文件删除未执行。请基于已有信息继续或停止。",
            )

        if location == "工作区外":
            return ActionResult(
                data={
                    "status": "ERROR",
                    "error": "outside-workspace deletion is not supported",
                    "path": str(target),
                    "workspace": str(workspace),
                    "operation": "delete",
                    "authorization": auth,
                },
                next_prompt="用户已授权，但第一版不支持删除工作区外路径。请停止删除或改用工作区内路径。",
            )

        result = delete_file(path=path, cwd=self.ctx.cwd or None, recursive=recursive)
        result["authorization"] = auth
        return ActionResult(
            data=result,
            next_prompt="文件删除流程完成，请基于 tool_results 判断是否需要验证或继续操作。",
        )

    def exec_ask_user(self, args: dict[str, Any]) -> ActionResult:
        message = str(args.get("message", ""))
        raw_options = args.get("options")
        options = [str(item) for item in raw_options] if isinstance(raw_options, list) else None
        result = ask_user(message, options=options, input_fn=self.ctx.user_input_fn)
        if result.get("status") == "SKIP":
            return ActionResult(
                data=result,
                next_prompt="用户跳过了交互，请基于已有信息继续执行。",
            )
        user_reply = result.get("user_reply", "")
        return ActionResult(
            data=result,
            next_prompt=f"用户回复：{user_reply}" if user_reply else "用户未提供回复，请基于已有信息继续执行。",
        )

    def exec_update_working_checkpoint(self, args: dict[str, Any]) -> ActionResult:
        key_info = args.get("key_info")
        related_sop = args.get("related_sop")
        result = update_working_checkpoint(
            key_info=str(key_info) if key_info is not None else None,
            related_sop=str(related_sop) if related_sop is not None else None,
        )
        if key_info is not None:
            self.ctx.working["key_info"] = str(key_info)
        if related_sop is not None:
            self.ctx.working["related_sop"] = str(related_sop)
        return ActionResult(
            data=result,
            next_prompt="工作记忆已更新，后续轮次将自动注入最新 key_info。",
        )

    def exec_skill_activate(self, args: dict[str, Any]) -> ActionResult:
        registry = self.ctx.skills
        raw_names = args.get("names", [])
        if isinstance(raw_names, str):
            requested = [raw_names]
        elif isinstance(raw_names, list):
            requested = [str(item) for item in raw_names]
        else:
            requested = []

        activated: list[str] = []
        missing: list[str] = []
        disallowed: list[str] = []
        allowlist = getattr(self.ctx, "skill_allowlist", None)
        if isinstance(registry, SkillRegistry):
            for name in dedupe_skill_names(requested):
                if allowlist is not None and name not in allowlist:
                    disallowed.append(name)
                elif registry.get(name) is None:
                    missing.append(name)
                else:
                    activated.append(name)
        else:
            missing = dedupe_skill_names(requested)

        self.ctx.active_skills = dedupe_skill_names([*self.ctx.active_skills, *activated])
        self.ctx.sink.emit(
            Event(
                session_id=self.ctx.session_id,
                turn=self.ctx.current_turn,
                kind="skill_activated",
                name="skill_activate",
                data={"activated": activated, "missing": missing, "disallowed": disallowed},
            )
        )
        return ActionResult(
            data={"status": "OK", "activated": activated, "missing": missing, "disallowed": disallowed},
            next_prompt="Skill activation updated. Future turns will include active skill instructions.",
        )

    def exec_web_scan(self, args: dict[str, Any]) -> ActionResult:
        url = args.get("url")
        mode = str(args.get("mode", "summary"))
        session_id = args.get("session_id")
        max_chars = args.get("max_chars")
        tab_index = args.get("tab_index")
        result = web_scan(
            url=str(url) if url else None,
            mode=mode,
            session_id=str(session_id) if session_id else None,
            max_chars=int(max_chars) if max_chars is not None else 8000,
            tab_index=int(tab_index) if tab_index is not None else None,
        )
        return ActionResult(
            data=result,
            next_prompt="页面扫描完成，请基于 tool_results 分析页面内容或继续操作。",
        )

    def exec_web_execute_js(self, args: dict[str, Any]) -> ActionResult:
        script = self._extract_code_script(args)
        session_id = args.get("session_id")
        timeout = args.get("timeout")
        save_to_file = args.get("save_to_file")
        no_monitor = bool(args.get("no_monitor", False))
        await_navigation = args.get("await_navigation")
        result = web_execute_js(
            script=script,
            session_id=str(session_id) if session_id else None,
            timeout=int(timeout) if timeout is not None else 10,
            save_to_file=str(save_to_file) if save_to_file else None,
            no_monitor=no_monitor,
            cwd=self.ctx.cwd or None,
            await_navigation=bool(await_navigation) if await_navigation is not None else None,
        )
        return ActionResult(
            data=result,
            next_prompt="JS 执行完成，请基于 tool_results 判断是否需要继续操作。",
        )

    def exec_start_long_term_update(self, args: dict[str, Any]) -> ActionResult:
        del args
        result = start_long_term_update()
        sop_content = result.get("sop_content", "")
        instruction = result.get("instruction", "")
        if sop_content:
            prompt = f"{instruction}\n\n--- SOP ---\n{sop_content}"
        else:
            prompt = "长期记忆结算：请总结当前对话中的关键发现和决策，写入 global_mem.txt。"
        return ActionResult(
            data=result,
            next_prompt=prompt,
        )

    def exec_plan_update(self, args: dict[str, Any]) -> ActionResult:
        # content 三态：缺省/None → 读取；字符串（含空串）→ 覆写
        content = args.get("content") if "content" in args else None
        result = plan_update(
            task_dir=self.task_dir,
            content=str(content) if content is not None else None,
        )
        status = result.get("status", "")
        mode = result.get("mode", "")
        if status == "SKIP":
            next_prompt = "plan 模式未启用（当前 Agent 无 task_dir）。"
        elif mode == "read":
            next_prompt = "plan.md 已读取，请对照计划判断当前位置和下一步行动。"
        else:
            next_prompt = "plan.md 已更新，请按更新后的计划继续执行。"
        return ActionResult(data=result, next_prompt=next_prompt)
