from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.core.agent_kernel import ContextManifest, DataSensitivity, Principal, TrustLevel
from src.core.agent_loop import ActionResult, AgentContext, BaseHandler, TurnEndHook
from src.core.local_policy import (
    HOST_READ_SCOPE,
    LOCAL_PRINCIPAL_SCOPES,
    LocalPolicyGate,
)
from src.core.memory import load_effective_memory, load_global_memory
from src.core.memory_store import MEMORY_READ_SCOPE, MemoryKind, MemoryStore
from src.core.skills import SkillRegistry, dedupe_skill_names, render_active_skills
from src.core.telemetry import Event
from src.tools import ask_user, create_browser_driver, delete_file, patch_file, plan_update, read_file, search_file_index, start_long_term_update, update_working_checkpoint, web_execute_js, web_scan, write_file
from src.tools.browser_driver import BrowserDriver
from src.tools.code_sandbox import (
    CodeExecutionPlan,
    CodeSandboxError,
    cleanup_execution_plan,
)
from src.tools.code_run import prepare_code_run_execution, run_code_stream
from src.tools.file_ops import resolve_path_for_operation
from src.orchestration.policy import PolicyDecision, PolicyOutcome


SUMMARY_PATTERN = re.compile(r"<summary>\s*(.*?)\s*</summary>", re.DOTALL)
FILE_CONTENT_PATTERN = re.compile(r"<file_content>\s*(.*?)\s*</file_content>", re.DOTALL)
CODE_BLOCK_PATTERN = re.compile(r"```(?:[\w+-]+)?\n?([\s\S]*?)```", re.DOTALL)
MAX_HISTORY_INFO_ENTRIES = 60
PLAN_REMIND_INTERVAL = 15
PLAN_PREVIEW_CHARS = 400
PROBLEM_STATUSES = {
    "ERROR",
    "SKIP",
    "TIMEOUT",
    "EMPTY_RESPONSE",
    "RETRYABLE_RESPONSE_ERROR",
    "CODE_BLOCK_WITHOUT_TOOL",
}
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


def _is_code_run_authorized(user_reply: str) -> bool:
    normalized = user_reply.strip().lower()
    return normalized in {"yes", "y", "ok", "confirm", "confirmed", "run", "确认", "授权", "同意", "执行"}


def _is_outside_read_authorized(user_reply: str) -> bool:
    normalized = user_reply.strip().lower()
    return normalized in {"yes", "y", "ok", "confirm", "confirmed", "read", "确认", "授权", "同意", "读取"}


def _is_kernel_approval_authorized(user_reply: str) -> bool:
    normalized = user_reply.strip().lower()
    return normalized in {
        "yes",
        "y",
        "ok",
        "confirm",
        "confirmed",
        "确认",
        "授权",
        "同意",
        "执行",
        "删除",
    }


class XAgentHandler(BaseHandler):
    run_code_stream = staticmethod(run_code_stream)

    def __init__(
        self,
        ctx: AgentContext | None = None,
        task_dir: str | None = None,
        browser_driver: BrowserDriver | None = None,
        browser_driver_factory: Callable[[], BrowserDriver] = create_browser_driver,
    ) -> None:
        super().__init__(ctx=ctx)
        self.task_dir = task_dir
        self._browser_driver = browser_driver
        self._browser_driver_factory = browser_driver_factory
        self._browser_driver_lock = threading.Lock()
        self._browser_driver_closed = False
        principal = self.ctx.principal
        if not isinstance(principal, Principal):
            session_id = self.ctx.session_id or "local-session"
            principal = Principal(
                subject="local-user",
                tenant_id="local",
                session_id=session_id,
                run_id=session_id,
                agent_id=self.ctx.agent_name or "main",
                scopes=LOCAL_PRINCIPAL_SCOPES,
            )
            self.ctx.principal = principal
        self._policy_gate = LocalPolicyGate(
            self.ctx.cwd or Path.cwd(),
            actor=principal.subject,
        )
        self._kernel_authorization_state = threading.local()
        self._turn_end_hooks.extend([
            TurnEndHook(name="external_intervene", fn=self._external_intervene_hook, priority=30),
            TurnEndHook(name="plan_reminder", fn=self._plan_reminder_hook, priority=25),
            TurnEndHook(name="self_evolution", fn=self._self_evolution_hook, priority=23),
            TurnEndHook(name="periodic_inject", fn=self._periodic_inject_hook, priority=20),
            TurnEndHook(name="summary_extract", fn=self._summary_extract_hook, priority=10),
        ])

    def tool_before_callback(
        self,
        tool_name: str,
        args: dict[str, Any],
    ) -> ActionResult | None:
        self._clear_kernel_authorization()
        principal = self.ctx.principal
        if not isinstance(principal, Principal):
            return ActionResult(
                data={
                    "status": "SKIP",
                    "error": "tool authorization requires an authenticated principal",
                    "reason_code": "principal_missing",
                },
                next_prompt="工具未执行：当前运行缺少经过验证的调用者身份。",
            )
        effective_args = self._kernel_action_args(tool_name, args)
        if tool_name == "code_run":
            try:
                execution_plan = self._prepare_code_run_plan(effective_args)
            except CodeSandboxError as exc:
                self._emit_code_sandbox_failure(exc.reason_code)
                return ActionResult(
                    data={
                        "status": "ERROR",
                        "error": str(exc),
                        "reason_code": exc.reason_code,
                        "security": {
                            "security_level": "unavailable",
                            "unsafe": False,
                        },
                    },
                    next_prompt=(
                        "代码未执行：经过验证的隔离后端不可用或配置无效；"
                        "安全模式不会回退到宿主进程。"
                    ),
                )
            self._kernel_authorization_state.execution_plan = execution_plan
            effective_args["execution_plan"] = execution_plan.approval_binding()
        authorization = self._policy_gate.evaluate(
            principal=principal,
            tool_name=tool_name,
            args=effective_args,
            turn=self.ctx.current_turn,
        )
        decision = authorization.decision
        self._emit_policy_decision(tool_name, decision)
        if decision.outcome is PolicyOutcome.DENY:
            return self._policy_denial_result(decision)
        if decision.outcome is PolicyOutcome.REQUIRE_APPROVAL:
            requested_at = time.time()
            preview = self._approval_preview(effective_args)
            request_id = hashlib.sha256(
                (
                    f"{decision.action_digest}\0{principal.principal_digest}\0"
                    f"{requested_at}"
                ).encode("utf-8")
            ).hexdigest()[:32]
            self.ctx.pending_approval = {
                "request_id": request_id,
                "tool_name": tool_name,
                "status": "pending",
                "reason_code": decision.reason_code,
                "action_digest": decision.action_digest,
                "policy_version": decision.policy_version,
                "principal_digest": principal.principal_digest,
                "requested_at": requested_at,
                "expires_at": requested_at + 300,
                "parameter_preview": preview,
            }
            self._checkpoint_approval_state("waiting_approval")
            reply = ask_user(
                self._approval_prompt(
                    tool_name,
                    preview,
                    decision,
                    args=effective_args,
                ),
                input_fn=self.ctx.user_input_fn,
            )
            if not _is_kernel_approval_authorized(reply.get("user_reply", "")):
                self.ctx.pending_approval.update(
                    status="rejected",
                    reason_code="approval_rejected",
                    resolved_at=time.time(),
                )
                self._checkpoint_approval_state("running")
                self.ctx.pending_approval = None
                return ActionResult(
                    data={
                        "status": "SKIP",
                        "error": "tool action not authorized by user",
                        "reason_code": "approval_rejected",
                        "action_digest": decision.action_digest,
                        "policy_version": decision.policy_version,
                    },
                    next_prompt="工具未执行：用户未授权这一组精确参数。",
                )
            decision = self._policy_gate.approve(
                authorization,
                principal=principal,
                now=time.time(),
            )
            self._emit_policy_decision(tool_name, decision)
            if decision.outcome is not PolicyOutcome.ALLOW:
                self.ctx.pending_approval.update(
                    status="denied",
                    reason_code=decision.reason_code,
                    resolved_at=time.time(),
                )
                self._checkpoint_approval_state("running")
                self.ctx.pending_approval = None
                return self._policy_denial_result(decision)
            self.ctx.pending_approval.update(
                status="approved",
                reason_code=decision.reason_code,
                approval_id=decision.approval_id or "",
                resolved_at=time.time(),
            )
            self._checkpoint_approval_state("running")
        self._kernel_authorization_state.tool_name = tool_name
        self._kernel_authorization_state.action_digest = decision.action_digest
        return None

    def tool_after_callback(self, tool_name: str, result: ActionResult) -> ActionResult:
        del tool_name
        self.ctx.pending_approval = None
        return result

    def tool_finally_callback(self, tool_name: str) -> None:
        del tool_name
        self._clear_kernel_authorization()

    def _kernel_action_args(
        self,
        tool_name: str,
        args: dict[str, Any],
    ) -> dict[str, Any]:
        effective = dict(args)
        if tool_name in {"code_run", "web_execute_js"}:
            effective["script"] = self._extract_code_script(args)
            effective.pop("code", None)
        elif tool_name == "file_write":
            effective["content"] = self._extract_file_content(args)
        return effective

    def _emit_policy_decision(
        self,
        tool_name: str,
        decision: PolicyDecision,
    ) -> None:
        principal = self.ctx.principal
        previous_outcomes = (
            list(self.ctx.last_policy_decision.get("outcomes", []))
            if self.ctx.last_policy_decision.get("tool_name") == tool_name
            and isinstance(self.ctx.last_policy_decision.get("outcomes"), list)
            else []
        )
        previous_outcomes.append(decision.outcome.value)
        self.ctx.last_policy_decision = {
            "tool_name": tool_name,
            "outcome": decision.outcome.value,
            "outcomes": previous_outcomes,
            "reason_code": decision.reason_code,
            "action_digest": decision.action_digest,
            "policy_version": decision.policy_version,
            "approval_id": decision.approval_id or "",
        }
        self.ctx.sink.emit(
            Event(
                session_id=self.ctx.session_id,
                turn=self.ctx.current_turn,
                kind="policy_decision",
                name=tool_name,
                data={
                    "outcome": decision.outcome.value,
                    "reason_code": decision.reason_code,
                    "action_digest": decision.action_digest,
                    "policy_version": decision.policy_version,
                    "approval_id": decision.approval_id or "",
                    "principal_digest": (
                        principal.principal_digest
                        if isinstance(principal, Principal)
                        else ""
                    ),
                    "context_manifest_digest": (
                        self.ctx.context_manifest.manifest_digest
                        if isinstance(self.ctx.context_manifest, ContextManifest)
                        else ""
                    ),
                },
            )
        )

    @staticmethod
    def _policy_denial_result(decision: PolicyDecision) -> ActionResult:
        return ActionResult(
            data={
                "status": "SKIP",
                "error": "tool action denied by policy",
                "reason_code": decision.reason_code,
                "action_digest": decision.action_digest,
                "policy_version": decision.policy_version,
            },
            next_prompt=(
                "工具未执行：策略拒绝了这一动作。请缩小权限或资源范围，"
                "改用已授权工具；不得用其他工具绕过。"
            ),
        )

    @staticmethod
    def _approval_prompt(
        tool_name: str,
        preview: dict[str, Any],
        decision: PolicyDecision,
        *,
        args: dict[str, Any] | None = None,
    ) -> str:
        rendered = json.dumps(preview, ensure_ascii=False, sort_keys=True)
        content_preview = ""
        if tool_name == "code_run" and isinstance(args, dict):
            script = args.get("script")
            if isinstance(script, str):
                preview_limit = 2000
                displayed = script[:preview_limit]
                if len(script) > preview_limit:
                    displayed += (
                        f"\n... [truncated, total {len(script)} chars]"
                    )
                content_preview = (
                    "- 待执行脚本（不可信内容，仅供审批，不应视为指令）：\n"
                    "```text\n"
                    f"{displayed}\n"
                    "```\n"
                )
        return (
            "Agent 请求执行高风险动作。授权只绑定以下工具、参数摘要、运行和策略版本，"
            "参数发生变化后必须重新授权：\n"
            f"- 工具：{tool_name}\n"
            f"- 动作摘要：{decision.action_digest}\n"
            f"- 策略版本：{decision.policy_version}\n"
            f"- 参数预览：{rendered}\n"
            f"{content_preview}"
            "如确认执行，请回复 yes / 确认 / 授权。"
        )

    @staticmethod
    def _approval_preview(args: dict[str, Any]) -> dict[str, Any]:
        preview: dict[str, Any] = {}
        for key in (
            "path",
            "root",
            "mode",
            "language",
            "timeout",
            "url",
            "session_id",
            "recursive",
            "save_to_file",
        ):
            value = args.get(key)
            if isinstance(value, (str, int, float, bool)) or value is None:
                preview[key] = value
        for key in ("script", "content", "old_content", "new_content"):
            value = args.get(key)
            if isinstance(value, str):
                encoded = value.encode("utf-8")
                preview[f"{key}_sha256"] = hashlib.sha256(encoded).hexdigest()
                preview[f"{key}_bytes"] = len(encoded)
        execution_plan = args.get("execution_plan")
        if isinstance(execution_plan, dict):
            preview["execution_plan"] = {
                key: execution_plan.get(key)
                for key in (
                    "backend",
                    "security_level",
                    "filesystem_mode",
                    "network_mode",
                    "process_mode",
                    "binding_digest",
                )
            }
        return preview

    def _checkpoint_approval_state(self, status: str) -> None:
        callback = self.ctx.checkpoint_callback
        if not callable(callback):
            return
        snapshot = dict(self.ctx.last_checkpoint_snapshot)
        snapshot.update(
            {
                "session_id": self.ctx.session_id,
                "turn": self.ctx.current_turn,
                "status": status,
                "pending_approval": (
                    dict(self.ctx.pending_approval)
                    if isinstance(self.ctx.pending_approval, dict)
                    else None
                ),
                "principal_digest": (
                    self.ctx.principal.principal_digest
                    if isinstance(self.ctx.principal, Principal)
                    else ""
                ),
                "principal_boundary_digest": (
                    self.ctx.principal.boundary_digest
                    if isinstance(self.ctx.principal, Principal)
                    else ""
                ),
                "context_manifest_digest": (
                    self.ctx.context_manifest.manifest_digest
                    if self.ctx.context_manifest is not None
                    else ""
                ),
                "context_state": dict(self.ctx.context_state),
            }
        )
        callback(snapshot)

    def _is_kernel_authorized(self, tool_name: str) -> bool:
        return (
            getattr(self._kernel_authorization_state, "tool_name", "") == tool_name
            and bool(
                getattr(self._kernel_authorization_state, "action_digest", "")
            )
        )

    def _kernel_action_digest(self) -> str:
        return str(
            getattr(self._kernel_authorization_state, "action_digest", "") or ""
        )

    def _clear_kernel_authorization(self) -> None:
        cleanup_execution_plan(
            getattr(self._kernel_authorization_state, "execution_plan", None)
        )
        self._kernel_authorization_state.tool_name = ""
        self._kernel_authorization_state.action_digest = ""
        self._kernel_authorization_state.execution_plan = None

    def close(self) -> None:
        with self._browser_driver_lock:
            if self._browser_driver_closed:
                return
            self._browser_driver_closed = True
            driver = self._browser_driver
            self._browser_driver = None
        if driver is not None:
            driver.close()

    def _get_browser_driver(self) -> BrowserDriver:
        with self._browser_driver_lock:
            if self._browser_driver_closed:
                raise RuntimeError("handler is closed")
            if self._browser_driver is None:
                self._browser_driver = self._browser_driver_factory()
            return self._browser_driver

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
            principal = ctx.principal
            if (
                isinstance(principal, Principal)
                and MEMORY_READ_SCOPE in principal.scopes
            ):
                memory_root = (
                    Path(ctx.memory_root)
                    if ctx.memory_root
                    else PROJECT_ROOT / "workspace" / "default.ws"
                )
                if (memory_root / "system").is_dir():
                    mem_content = load_effective_memory(
                        memory_root,
                        getattr(ctx, "agent_name", ""),
                        getattr(ctx, "memory_mode", "project"),
                    ).content
                else:
                    mem_content = load_global_memory(memory_root).content
                if mem_content:
                    parts.append(f"[Memory Refresh]\n{mem_content}")
        if turn % 65 == 0 and turn > 0:
            parts.append(
                f"[DANGER] 已连续执行第 {turn} 轮。必须总结当前情况并调用 ask_user 请求用户确认。"
            )
        return "\n".join(parts) if parts else None

    def _self_evolution_hook(
        self,
        response,
        tool_results,
        ctx: AgentContext,
    ) -> str | None:
        del response
        issue = self._select_self_evolution_issue(tool_results)
        if issue is None:
            return None

        memory_root_value = ctx.memory_root or ctx.cwd
        principal = ctx.principal
        if memory_root_value and isinstance(principal, Principal):
            try:
                candidate = MemoryStore(Path(memory_root_value)).propose(
                    principal=principal,
                    content=issue["lesson"],
                    namespace=(
                        "tenant",
                        principal.tenant_id,
                        "agent",
                        principal.agent_id,
                    ),
                    kind=MemoryKind.PROCEDURAL,
                    source_refs=(
                        self._turn_source_ref(principal, ctx.current_turn),
                        self._issue_source_ref(issue),
                    ),
                    trust=TrustLevel.TOOL_UNTRUSTED,
                    confidence=0.5,
                    sensitivity=DataSensitivity.SENSITIVE,
                    acl=(f"subject:{principal.subject}",),
                    ttl_seconds=7 * 24 * 60 * 60,
                )
                record_status = "OK"
                candidate_id = candidate.candidate_id
            except (OSError, RuntimeError, ValueError, PermissionError):
                record_status = "ERROR"
                candidate_id = ""
        else:
            record_status = "SKIP"
            candidate_id = ""
        if record_status == "OK":
            memory_note = f"经验已提交待审核候选（{candidate_id}），尚未进入生效记忆。"
        elif record_status == "SKIP":
            memory_note = "当前未配置记忆根目录，本轮仅注入策略修正。"
        else:
            memory_note = "经验候选提交失败，本轮仍必须按该经验调整策略。"

        return (
            "[Self Evolution]\n"
            f"检测到本轮问题：{issue['problem']}。\n"
            f"已沉淀经验：{issue['lesson']}\n"
            f"{memory_note}\n"
            "下一轮必须先判断根因并选择最优路径：优先补充探测信息、换更小验证步骤或切换方案；"
            "不要重复同一失败动作。只有缺少外部决策时才调用 ask_user。"
        )

    @staticmethod
    def _turn_source_ref(principal: Principal, turn: int) -> str:
        run_digest = hashlib.sha256(principal.run_id.encode("utf-8")).hexdigest()
        return f"run-sha256:{run_digest}:turn:{max(0, int(turn))}"

    @staticmethod
    def _issue_source_ref(issue: dict[str, str]) -> str:
        encoded = json.dumps(
            issue,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"tool-result-sha256:{hashlib.sha256(encoded).hexdigest()}"

    def _select_self_evolution_issue(self, tool_results: list[dict[str, Any]]) -> dict[str, str] | None:
        for result in tool_results:
            data = result.get("data")
            if not isinstance(data, dict):
                continue
            status = str(data.get("status", "")).strip().upper()
            has_error = "error" in data and bool(data.get("error"))
            if status not in PROBLEM_STATUSES and not (has_error and not status):
                continue
            tool_name = str(result.get("tool_name", "tool"))
            return self._build_self_evolution_issue(tool_name, status or "ERROR")
        return None

    @staticmethod
    def _build_self_evolution_issue(tool_name: str, status: str) -> dict[str, str]:
        problem = f"{tool_name} 返回 {status}"
        if tool_name == "file_patch":
            lesson = "file_patch 失败后先用 file_read 精读目标片段，再用唯一 old_content 重试。"
        elif tool_name == "file_read":
            lesson = "file_read 失败后先用 file_search 或检查 workspace 路径候选，再重试。"
        elif tool_name == "code_run":
            lesson = "code_run 失败后先阅读 stdout/stderr 和环境状态，用更小脚本验证根因再继续。"
        elif tool_name in {"web_scan", "web_execute_js"}:
            lesson = "浏览器工具失败后先确认当前 session 和页面状态，必要时缩小 JS 查询范围。"
        elif tool_name == "no_tool" and status == "CODE_BLOCK_WITHOUT_TOOL":
            lesson = "需要执行或验证代码时必须调用 code_run；仅汇报结果时不要输出代码块。"
        elif tool_name == "no_tool" and status == "EMPTY_RESPONSE":
            lesson = "空响应后必须直接给最终答案或调用合适工具继续，禁止继续空转。"
        elif tool_name == "no_tool" and status == "RETRYABLE_RESPONSE_ERROR":
            lesson = "响应截断或流异常后应缩短输出，重试必要工具调用并避免大段一次性输出。"
        elif status == "SKIP":
            lesson = "工具返回 SKIP 时先识别授权或能力边界，换可行路径；必要时再请求用户确认。"
        else:
            lesson = "工具失败后不要原样重试；先判断根因，补充探测信息或切换更小的验证步骤。"
        return {"problem": problem, "lesson": lesson}

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
        if not script.strip():
            return ActionResult(
                data={"status": "ERROR", "error": "script is empty"},
                next_prompt="代码未执行：script 不能为空。",
            )
        try:
            timeout = int(args.get("timeout", 60))
        except (TypeError, ValueError):
            result = {"status": "ERROR", "error": f"invalid timeout: {args.get('timeout')!r}"}
        else:
            if timeout <= 0:
                return ActionResult(
                    data={"status": "ERROR", "error": f"timeout must be positive: {timeout}"},
                    next_prompt="代码未执行：timeout 必须为正数。",
                )
            if language not in {"python", "shell", "bash", "sh"}:
                return ActionResult(
                    data={"status": "ERROR", "error": f"unsupported language: {language}"},
                    next_prompt="代码未执行：language 仅支持 python 或 shell。",
                )
            plan = getattr(
                self._kernel_authorization_state,
                "execution_plan",
                None,
            )
            if not isinstance(plan, CodeExecutionPlan):
                try:
                    plan = self._prepare_code_run_plan(
                        {
                            "script": script,
                            "language": language,
                            "timeout": timeout,
                        }
                    )
                except CodeSandboxError as exc:
                    self._emit_code_sandbox_failure(exc.reason_code)
                    return ActionResult(
                        data={
                            "status": "ERROR",
                            "error": str(exc),
                            "reason_code": exc.reason_code,
                            "security": {
                                "security_level": "unavailable",
                                "unsafe": False,
                            },
                        },
                        next_prompt=(
                            "代码未执行：经过验证的隔离后端不可用或配置无效；"
                            "安全模式不会回退到宿主进程。"
                        ),
                    )
            authorization_error = self._authorize_code_run(
                script,
                language,
                timeout,
                plan,
            )
            if authorization_error is not None:
                return authorization_error
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
                allow_unsafe=plan.unsafe,
                isolation_mode=("unsafe" if plan.unsafe else plan.backend),
                execution_plan=plan,
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

    def _authorize_code_run(
        self,
        script: str,
        language: str,
        timeout: int,
        plan: CodeExecutionPlan,
    ) -> ActionResult | None:
        stop_signal = self._stop_signal()
        if stop_signal is not None and stop_signal.is_set():
            return ActionResult(
                data={"status": "INTERRUPTED", "error": "code execution cancelled before authorization"},
                next_prompt="",
                should_exit=True,
            )
        policy = os.environ.get("XAGENT_CODE_RUN_POLICY", "confirm").strip().lower()
        if policy == "deny":
            return ActionResult(
                data={"status": "SKIP", "error": "code execution is disabled by policy"},
                next_prompt="代码未执行：当前策略禁止 code_run。请改用其他工具或请用户调整策略。",
            )
        if policy != "confirm":
            if policy != "allow":
                return ActionResult(
                    data={"status": "ERROR", "error": f"invalid XAGENT_CODE_RUN_POLICY: {policy!r}"},
                    next_prompt="代码未执行：code_run 安全策略配置无效。",
                )
        # The kernel grant is already a single-use approval for this exact
        # immutable plan. It may satisfy confirm, but it can never override an
        # operator-level deny or invalid configuration.
        if self._is_kernel_authorized("code_run"):
            return None
        if policy == "allow" and not plan.unsafe:
            return None
        if policy == "allow" and plan.unsafe:
            policy = "confirm"

        digest = hashlib.sha256(script.encode("utf-8")).hexdigest()[:16]
        preview_limit = 2000
        preview = script[:preview_limit]
        if len(script) > preview_limit:
            preview += f"\n... [truncated, total {len(script)} chars]"
        security_warning = (
            "Agent 请求在显式启用的开发级非隔离宿主进程中执行代码。"
            "它可能访问宿主文件、网络和其他同 UID 资源；本次仍需单独授权。\n"
            if plan.unsafe
            else
            "Agent 请求在已通过功能探测的 OS 隔离后端中执行只读代码。"
            "工作区控制面被隐藏、网络被禁止，持久写入需使用文件工具。\n"
        )
        auth = ask_user(
            (
                security_warning
                +
                f"- 语言：{language}\n"
                f"- 超时：{timeout}s\n"
                f"- 隔离后端：{plan.backend}\n"
                f"- 安全级别：{plan.security_level}\n"
                f"- SHA256：{digest}\n"
                "```text\n"
                f"{preview}\n"
                "```\n"
                "如确认执行，请回复 yes / 确认 / 授权。"
            ),
            input_fn=self.ctx.user_input_fn,
        )
        if _is_code_run_authorized(auth.get("user_reply", "")):
            return None
        return ActionResult(
            data={
                "status": "SKIP",
                "error": "code execution not authorized by user",
                "script_sha256": digest,
            },
            next_prompt="代码未执行：用户未授权本次 code_run。请改用其他工具或停止。",
        )

    def _prepare_code_run_plan(
        self,
        args: dict[str, Any],
    ) -> CodeExecutionPlan:
        script = self._extract_code_script(args)
        language = str(args.get("language", "python"))
        try:
            timeout = int(args.get("timeout", 60))
        except (TypeError, ValueError) as exc:
            raise CodeSandboxError(
                "TIMEOUT_INVALID",
                "timeout must be an integer",
            ) from exc
        configured = os.environ.get(
            "XAGENT_CODE_RUN_BACKEND",
            os.environ.get("XAGENT_CODE_RUN_ISOLATION", "auto"),
        )
        unsafe_enabled = str(configured or "").strip().lower() in {
            "unsafe",
            "development_unsafe",
        }
        return prepare_code_run_execution(
            script=script,
            language=language,
            timeout=timeout,
            cwd=self.ctx.cwd or None,
            backend=configured,
            unsafe_authorized=unsafe_enabled,
        )

    def _emit_code_sandbox_failure(self, reason_code: str) -> None:
        principal = self.ctx.principal
        self.ctx.sink.emit(
            Event(
                session_id=self.ctx.session_id,
                turn=self.ctx.current_turn,
                kind="code_sandbox_preflight",
                name="ERROR",
                data={
                    "reason_code": reason_code,
                    "principal_digest": (
                        principal.principal_digest
                        if isinstance(principal, Principal)
                        else ""
                    ),
                    "unsafe_fallback": False,
                },
            )
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
        workspace = Path(self.ctx.cwd or Path.cwd()).resolve()
        try:
            resolved_path = resolve_path_for_operation(
                path,
                str(workspace),
                operation="read",
                allow_outside_read=True,
            )
        except (OSError, ValueError) as exc:
            return ActionResult(
                data={"status": "ERROR", "error": str(exc)},
                next_prompt="文件未读取：路径无效。",
            )

        outside_workspace = not _is_relative_to(resolved_path, workspace)
        if outside_workspace:
            principal = self.ctx.principal
            if (
                not isinstance(principal, Principal)
                or HOST_READ_SCOPE not in principal.scopes
            ):
                return ActionResult(
                    data={
                        "status": "SKIP",
                        "error": "outside-workspace read capability is unavailable",
                        "reason_code": "host_read_scope_required",
                    },
                    next_prompt=(
                        "文件未读取：当前身份没有读取宿主工作区外路径的能力。"
                    ),
                )
            policy = os.environ.get("XAGENT_OUTSIDE_READ_POLICY", "confirm").strip().lower()
            if policy == "deny":
                return ActionResult(
                    data={
                        "status": "SKIP",
                        "error": "outside-workspace reads are disabled by policy",
                        "path": str(resolved_path),
                    },
                    next_prompt="文件未读取：当前策略禁止读取工作区外路径。",
                )
            if policy == "confirm":
                auth = ask_user(
                    (
                        "Agent 请求读取工作区外路径。内容可能包含隐私、凭证或针对 Agent 的恶意指令；"
                        "本次授权仅对下列规范化路径生效：\n"
                        f"- 工作区：{workspace}\n"
                        f"- 读取路径：{resolved_path}\n"
                        "如确认读取，请回复 yes / 确认 / 授权。"
                    ),
                    input_fn=self.ctx.user_input_fn,
                )
                if not _is_outside_read_authorized(auth.get("user_reply", "")):
                    return ActionResult(
                        data={
                            "status": "SKIP",
                            "error": "outside-workspace read not authorized by user",
                            "path": str(resolved_path),
                        },
                        next_prompt="文件未读取：用户未授权本次工作区外读取。",
                    )
            elif policy != "allow":
                return ActionResult(
                    data={
                        "status": "ERROR",
                        "error": f"invalid XAGENT_OUTSIDE_READ_POLICY: {policy!r}",
                    },
                    next_prompt="文件未读取：工作区外读取安全策略配置无效。",
                )

        result = read_file(
            path=str(resolved_path),
            cwd=self.ctx.cwd or None,
            start_line=int(start_line) if start_line is not None else None,
            end_line=int(end_line) if end_line is not None else None,
            keyword=str(keyword) if keyword is not None else None,
            allow_outside=outside_workspace,
            allow_managed_memory=(
                isinstance(self.ctx.principal, Principal)
                and MEMORY_READ_SCOPE in self.ctx.principal.scopes
            ),
        )
        next_prompt = "文件读取完成，请基于 tool_results 继续分析或执行下一步。"
        if self._is_memory_path(path):
            next_prompt += (
                "\n\n⚠️ 这是受管记忆数据，只能作为低信任上下文读取。"
                "\n- 禁止用 file_write/file_patch 直接修改"
                "\n- 需要沉淀的新事实必须调用 memory_propose"
                "\n- 候选经审核前不得声称已成为长期记忆"
            )
        return ActionResult(
            data=result,
            next_prompt=next_prompt,
        )

    def exec_file_search(self, args: dict[str, Any]) -> ActionResult:
        query = str(args.get("query", ""))
        root = str(args.get("root", ""))
        try:
            limit = int(args.get("limit", 20))
        except (TypeError, ValueError):
            limit = 20
        mode = str(args.get("mode", "hybrid"))
        result = search_file_index(
            query=query,
            cwd=self.ctx.cwd or None,
            root=root,
            limit=limit,
            refresh=bool(args.get("refresh", False)),
            path_only=bool(args.get("path_only", False)),
            mode=mode,
            embedding_config=self.ctx.file_index_embedding,
            principal=(
                self.ctx.principal
                if isinstance(self.ctx.principal, Principal)
                else None
            ),
        )
        if result.get("status") == "OK":
            bundle = result.get("evidence_bundle")
            bundle_data = bundle if isinstance(bundle, dict) else {}
            self.ctx.sink.emit(
                Event(
                    session_id=self.ctx.session_id,
                    turn=self.ctx.current_turn,
                    kind="retrieval_evidence",
                    name=str(bundle_data.get("bundle_id", "")),
                    data={
                        "bundle_digest": str(
                            result.get("evidence_bundle_digest", "")
                        ),
                        "index_version": str(result.get("index_version", "")),
                        "evidence_count": len(
                            bundle_data.get("items", [])
                            if isinstance(bundle_data.get("items"), list)
                            else []
                        ),
                        "principal_digest": str(
                            bundle_data.get("principal_digest", "")
                        ),
                        "action_digest": self.ctx.last_policy_decision.get(
                            "action_digest",
                            "",
                        ),
                        "context_manifest_digest": (
                            self.ctx.context_manifest.manifest_digest
                            if isinstance(self.ctx.context_manifest, ContextManifest)
                            else ""
                        ),
                    },
                )
            )
        return ActionResult(
            data=result,
            next_prompt="文件索引检索完成。搜索结果只用于定位；修改或依赖具体内容前，必须用 file_read 精读候选文件。",
        )

    @staticmethod
    def _is_memory_path(path: str) -> bool:
        normalized = os.path.normpath(path)
        parts = [
            part.casefold()
            for part in normalized.replace("\\", "/").split("/")
        ]
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
            target = resolve_path_for_operation(
                path,
                self.ctx.cwd or None,
                operation="read",
                allow_outside_read=True,
            )
            workspace = Path(self.ctx.cwd or os.getcwd()).resolve()
            location = "工作区内" if _is_relative_to(target, workspace) else "工作区外"
        except (OSError, ValueError) as exc:
            return ActionResult(
                data={"status": "ERROR", "error": str(exc)},
                next_prompt="文件删除未执行，请基于错误信息决定是否修正路径或停止。",
            )

        if self._is_kernel_authorized("file_delete"):
            auth = {
                "status": "OK",
                "source": "policy_engine",
                "action_digest": self._kernel_action_digest(),
            }
        else:
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

    def exec_agent_delegate(self, args: dict[str, Any]) -> ActionResult:
        agent = str(args.get("agent") or "").strip()
        task = str(args.get("task") or "").strip()
        context = str(args.get("context") or "").strip()
        expected_output = str(args.get("expected_output") or "").strip()
        if not agent:
            return ActionResult(
                data={"status": "ERROR", "error": "agent is required"},
                next_prompt="委派失败：缺少目标 agent。请指定团队成员 agent，或停止并说明无法委派。",
            )
        if not task:
            return ActionResult(
                data={"status": "ERROR", "error": "task is required"},
                next_prompt="委派失败：缺少子任务描述。请补充明确 task 后重试，或自行继续执行。",
            )
        runner = getattr(self.ctx, "delegate_runner", None)
        if runner is None:
            return ActionResult(
                data={"status": "ERROR", "error": "no active team"},
                next_prompt="当前没有启用 Agent 团队，不能调用 agent_delegate。请改用当前 Agent 的工具继续。",
            )

        self.ctx.display_fn(f"  agent_delegate: start ({agent})")
        result = runner(
            agent=agent,
            task=task,
            context=context,
            expected_output=expected_output,
            parent_ctx=self.ctx,
        )
        status = str(result.get("status", "UNKNOWN")) if isinstance(result, dict) else "ERROR"
        self.ctx.display_fn(f"  agent_delegate: done ({agent}, {status})")
        if not isinstance(result, dict):
            result = {"status": "ERROR", "error": f"delegate runner returned {type(result)!r}"}
        if result.get("status") == "OK":
            next_prompt = (
                f"团队成员 {agent} 已完成委派任务。请基于 tool_results 中的 response 和元数据整合结果；"
                "若总任务未完成，可以继续调用工具或委派其他成员。"
            )
        else:
            next_prompt = "Agent 委派失败。请基于错误信息改用其他成员、当前 Agent 工具，或向用户说明限制。"
        return ActionResult(data=result, next_prompt=next_prompt)

    def exec_update_working_checkpoint(self, args: dict[str, Any]) -> ActionResult:
        key_info = args.get("key_info")
        related_sop = args.get("related_sop")
        result = update_working_checkpoint(
            key_info=str(key_info) if key_info is not None else None,
            related_sop=str(related_sop) if related_sop is not None else None,
        )
        if result.get("status") == "OK":
            if key_info is not None:
                self.ctx.working["key_info"] = str(key_info)
            if related_sop is not None:
                self.ctx.working["related_sop"] = str(related_sop)
            next_prompt = (
                "工作记忆已更新，后续轮次将自动注入最新 key_info。"
            )
        else:
            next_prompt = (
                "工作记忆未更新：内容超过有界上下文限制。"
                "请只保留目标、约束、已验证事实和下一步。"
            )
        return ActionResult(
            data=result,
            next_prompt=next_prompt,
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
            self._get_browser_driver(),
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
            self._get_browser_driver(),
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
            prompt = (
                "长期记忆结算：请总结当前对话中有来源的关键发现和决策，"
                "调用 memory_propose 提交待审核候选；禁止直接修改长期记忆文件。"
            )
        return ActionResult(
            data=result,
            next_prompt=prompt,
        )

    def exec_memory_propose(self, args: dict[str, Any]) -> ActionResult:
        principal = self.ctx.principal
        memory_root = self.ctx.memory_root or self.ctx.cwd
        if not isinstance(principal, Principal) or not memory_root:
            return ActionResult(
                data={
                    "status": "SKIP",
                    "error": "memory proposal requires a principal and memory root",
                },
                next_prompt="记忆候选未提交：当前运行缺少身份或记忆工作区。",
            )
        content = str(args.get("content", "")).strip()
        raw_refs = args.get("source_refs", ())
        if not isinstance(raw_refs, (list, tuple)):
            raw_refs = ()
        source_refs = tuple(str(item).strip() for item in raw_refs if str(item).strip())
        try:
            confidence = float(args.get("confidence", 0.5))
            candidate = MemoryStore(Path(memory_root)).propose(
                principal=principal,
                content=content,
                namespace=(
                    "tenant",
                    principal.tenant_id,
                    "agent",
                    principal.agent_id,
                ),
                kind=MemoryKind(str(args.get("kind", MemoryKind.SEMANTIC.value))),
                source_refs=(
                    self._turn_source_ref(principal, self.ctx.current_turn),
                    *source_refs,
                ),
                trust=TrustLevel.AGENT_DERIVED,
                confidence=confidence,
                sensitivity=DataSensitivity.SENSITIVE,
                acl=(f"subject:{principal.subject}",),
                ttl_seconds=7 * 24 * 60 * 60,
            )
        except (OSError, RuntimeError, ValueError, PermissionError) as exc:
            return ActionResult(
                data={
                    "status": "ERROR",
                    "error": str(exc),
                    "reason_code": "memory_candidate_rejected",
                },
                next_prompt=(
                    "记忆候选未提交。请缩短内容、提供可验证来源并修正类型或置信度；"
                    "不得改用文件工具绕过。"
                ),
            )
        self.ctx.sink.emit(
            Event(
                session_id=self.ctx.session_id,
                turn=self.ctx.current_turn,
                kind="memory_candidate_created",
                name=candidate.kind.value,
                data={
                    "candidate_id": candidate.candidate_id,
                    "content_sha256": candidate.content_sha256,
                    "review_status": candidate.review_status.value,
                    "trust": candidate.trust.value,
                    "principal_digest": principal.principal_digest,
                    "action_digest": self.ctx.last_policy_decision.get(
                        "action_digest",
                        "",
                    ),
                    "context_manifest_digest": (
                        self.ctx.context_manifest.manifest_digest
                        if isinstance(self.ctx.context_manifest, ContextManifest)
                        else ""
                    ),
                },
            )
        )
        return ActionResult(
            data={
                "status": "OK",
                "candidate_id": candidate.candidate_id,
                "content_sha256": candidate.content_sha256,
                "review_status": candidate.review_status.value,
                "trust": candidate.trust.value,
            },
            next_prompt=(
                "记忆候选已隔离保存，尚未生效。继续任务即可；"
                "不得声称它已成为长期记忆。"
            ),
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
