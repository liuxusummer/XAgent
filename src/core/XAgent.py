from __future__ import annotations

import os
import threading
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from queue import Queue
from typing import Any

from src.config import SessionConfig, create_client, load_config
from src.core.agent_loop import AgentContext, run_agent_loop
from src.core.checkpoint import build_task_checkpoint, load_task_checkpoint, render_resume_prompt, write_task_checkpoint
from src.core.llm import MixinSession, NativeToolClient, OpenAITextSession, ToolClient
from src.core.runbook import distill_runbook_from_task
from src.core.skills import SkillRegistry, dedupe_skill_names, select_skills
from src.core.telemetry import Event, EventSink, MultiSink, NullSink
from src.handler import XAgentHandler
from src.tools.interaction import make_progress_emitter, make_user_input_bridge


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_WORKSPACE_NAME = "default.ws"
WORKSPACE_LAYOUT_DIRS = (
    "business",
    "runtime",
    "system/agents/main",
    "system/agents/coding",
    "system/memory",
    "system/skills",
    "system/templates",
    "system/teams",
)
WORKSPACE_LAYOUT_FILES = (
    "system/agents/main/AGENT.md",
    "system/agents/main/SOUL.md",
    "system/agents/main/MEMORY.md",
    "system/agents/coding/AGENT.md",
    "system/agents/coding/SOUL.md",
    "system/agents/coding/MEMORY.md",
)


class _RunbookEventCollector:
    def __init__(self) -> None:
        self.events: list[Event] = []

    def emit(self, event: Event) -> None:
        self.events.append(event)

    def close(self) -> None:
        return None


def ensure_workspace_layout(workspace_dir: str | Path) -> None:
    workspace = Path(workspace_dir)
    for rel_dir in WORKSPACE_LAYOUT_DIRS:
        (workspace / rel_dir).mkdir(parents=True, exist_ok=True)
    for rel_file in WORKSPACE_LAYOUT_FILES:
        file_path = workspace / rel_file
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.touch(exist_ok=True)


def resolve_workspace_dir(workspace_dir: str | None = None, code_root: str | Path | None = None) -> str:
    root = Path(code_root).expanduser() if code_root is not None else PROJECT_ROOT
    if workspace_dir:
        path = Path(workspace_dir).expanduser()
    else:
        path = root / "workspace" / DEFAULT_WORKSPACE_NAME
    path = path.resolve()
    ensure_workspace_layout(path)
    return str(path)


@dataclass
class XAgent:
    system_prompt: str
    tools_schema: list[dict[str, Any]]
    api_key: str = ""
    base_url: str = ""
    model: str = ""
    cwd: str = ""
    workspace_dir: str | None = None
    timeout: float = 60.0
    config_path: str | None = None
    task_queue: Queue[str] = field(default_factory=Queue)
    display_queue: Queue[dict[str, Any]] = field(default_factory=Queue)
    reply_queue: Queue[str] = field(default_factory=Queue)
    stop_event: threading.Event = field(default_factory=threading.Event)
    owns_stop_event: bool = True
    _running: threading.Event = field(default_factory=threading.Event)
    sink: EventSink = field(default_factory=NullSink)
    skills_dir: str | None = None
    max_turns: int = 40
    agent_name: str = ""
    memory_mode: str = "project"
    team_config: dict[str, Any] | None = None
    delegate_runner: Any | None = None
    file_index_embedding_config: dict[str, Any] | None = None
    runbook_min_interaction_records: int = 10

    def __post_init__(self) -> None:
        self.workspace_dir = resolve_workspace_dir(self.workspace_dir or self.cwd)
        self.cwd = self.workspace_dir
        progress_fn = make_progress_emitter(self.display_queue)
        user_input_fn = make_user_input_bridge(self.display_queue, self.reply_queue)
        self.skill_registry = self._load_skill_registry()
        self.client = self._create_client()
        self.handler = XAgentHandler(
            ctx=AgentContext(
                cwd=self.cwd,
                sink=self.sink,
                stop_signal=self.stop_event,
                display_fn=progress_fn,
                user_input_fn=user_input_fn,
                skills=self.skill_registry,
                memory_root=self.cwd,
                agent_name=self.agent_name,
                memory_mode=self.memory_mode,
                team_config=self.team_config,
                delegate_runner=self.delegate_runner,
                file_index_embedding=self.file_index_embedding_config,
            ),
            task_dir=self.workspace_dir,
        )

    def _load_skill_registry(self) -> SkillRegistry:
        if self.skills_dir:
            paths = [Path(self.skills_dir)]
        else:
            paths = [PROJECT_ROOT / "skills", Path(self.cwd) / "system" / "skills"]
        return SkillRegistry.load(paths)

    def _create_client(self) -> ToolClient | NativeToolClient:
        if self.config_path:
            configs = load_config(self.config_path)
            if configs:
                first_config = next(iter(configs.values()))
                self.file_index_embedding_config = self._file_index_embedding_config(first_config)
                if self.model:
                    first_config.model = self.model
                client = create_client(first_config)
                self._attach_stream_callback(client)
                return client

        if not self.api_key:
            self.api_key = os.environ.get("OPENAI_API_KEY", "")
        if not self.base_url:
            self.base_url = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions")
        if not self.model:
            self.model = os.environ.get("OPENAI_MODEL", "gpt-4o")

        session = OpenAITextSession(
            api_key=self.api_key,
            base_url=self.base_url,
            model=self.model,
            timeout=self.timeout,
        )
        client = ToolClient(backend=session)
        self._attach_stream_callback(client)
        return client

    @staticmethod
    def _file_index_embedding_config(config: SessionConfig) -> dict[str, Any] | None:
        raw = config.extra.get("file_index_embedding")
        if not isinstance(raw, dict):
            return None
        merged = dict(raw)
        merged.setdefault("apikey", config.apikey)
        return {"file_index_embedding": merged}

    def _attach_stream_callback(self, client: ToolClient | NativeToolClient) -> None:
        backend = getattr(client, "backend", None)
        if backend is None:
            return
        if isinstance(backend, MixinSession):
            callback = self._build_stream_callback()
            for session in backend.sessions:
                session.stream_callback = callback
            return
        if not hasattr(backend, "stream_callback"):
            return

        backend.stream_callback = self._build_stream_callback()

    def _build_stream_callback(self):
        def _emit_llm_chunk(event: dict[str, Any]) -> None:
            event_type = event.get("type", "")
            if event_type == "text":
                delta = str(event.get("delta", ""))
                if delta:
                    self.handler.ctx.display_fn(f"  llm | {delta}")
            elif event_type == "thinking":
                delta = str(event.get("delta", ""))
                if delta:
                    self.handler.ctx.display_fn(f"  thinking | {delta}")
            elif event_type == "tool_call":
                name = str(event.get("name", ""))
                if name:
                    self.handler.ctx.display_fn(f"  llm tool_call | {name}")

        return _emit_llm_chunk

    def put_task(self, query: str) -> None:
        self.task_queue.put(query)

    def run_task(
        self,
        query: str,
        resume_checkpoint: str | None = None,
        checkpoint_id: str | None = None,
        *,
        reset_stop_event: bool = True,
    ) -> dict[str, Any]:
        self._running.set()
        if self.owns_stop_event and reset_stop_event:
            self.stop_event.clear()
        base_sink = self.handler.ctx.sink
        base_checkpoint_callback = self.handler.ctx.checkpoint_callback
        runbook_collector = _RunbookEventCollector()
        self.handler.ctx.sink = MultiSink(base_sink, runbook_collector)
        # 每次任务使用独立 session_id；Web 等前端可预先指定，从而把聊天
        # 状态与该任务的 checkpoint 稳定关联起来。
        self.handler.ctx.session_id = str(checkpoint_id or uuid.uuid4().hex[:16])
        if resume_checkpoint:
            query = self._query_with_resume_checkpoint(query, resume_checkpoint)
        self.handler.ctx.checkpoint_callback = self._build_checkpoint_callback(query)
        skill_registry = getattr(self, "skill_registry", SkillRegistry())
        skill_allowlist = getattr(self.handler.ctx, "skill_allowlist", None)
        if skill_allowlist is None:
            auto_skills = select_skills(query, skill_registry)
        elif skill_allowlist:
            allowed_registry = SkillRegistry(
                {name: manifest for name, manifest in skill_registry.skills.items() if name in skill_allowlist}
            )
            auto_skills = select_skills(query, allowed_registry)
        else:
            auto_skills = []
        self.handler.ctx.active_skills = dedupe_skill_names(auto_skills)
        self.handler.ctx.sink.emit(
            Event(
                session_id=self.handler.ctx.session_id,
                turn=0,
                kind="skill_auto_selected",
                name="auto",
                data={"selected": list(self.handler.ctx.active_skills), "query_len": len(query)},
            )
        )
        try:
            try:
                result = run_agent_loop(
                    client=self.client,
                    system_prompt=self.system_prompt,
                    user_input=query,
                    handler=self.handler,
                    tools_schema=self.tools_schema,
                    max_turns=getattr(self, "max_turns", 40),
                    stop_event=self.stop_event,
                )
            except Exception as exc:  # noqa: BLE001
                result = {
                    "response": f"[error] {exc}",
                    "exit_reason": "ERROR",
                    "tool_results": [
                        {
                            "tool_name": "run_task",
                            "tool_call_id": "",
                            "data": {"status": "ERROR", "error": str(exc)},
                        }
                    ],
                    "turns": self.handler.ctx.current_turn,
                }
                self.handler.ctx.display_fn(f"[error] {exc}")
                self._write_terminal_checkpoint(query, result, "failed")
            self._distill_runbook(query, result, runbook_collector.events)
            self.display_queue.put({"done": result})
            return result
        finally:
            self.handler.ctx.checkpoint_callback = base_checkpoint_callback
            self.handler.ctx.sink = base_sink
            self._running.clear()

    def resume_task(self, checkpoint_id: str = "latest", query: str = "") -> dict[str, Any]:
        return self.run_task(query or "继续执行 checkpoint 中未完成的任务。", resume_checkpoint=checkpoint_id)

    def _query_with_resume_checkpoint(self, query: str, checkpoint_id: str) -> str:
        loaded = load_task_checkpoint(self.cwd, checkpoint_id)
        if loaded.get("status") != "OK":
            self.handler.ctx.display_fn(
                f"[checkpoint] resume skipped: {loaded.get('error', 'unknown error')}"
            )
            return query
        checkpoint = loaded.get("checkpoint")
        if not isinstance(checkpoint, dict):
            self.handler.ctx.display_fn("[checkpoint] resume skipped: invalid checkpoint")
            return query
        self.handler.ctx.display_fn(f"[checkpoint] resume from {checkpoint.get('checkpoint_id', checkpoint_id)}")
        return render_resume_prompt(checkpoint, query)

    def _build_checkpoint_callback(self, query: str):
        workspace_root = getattr(self, "cwd", "") or getattr(self, "workspace_dir", "")
        if not workspace_root:
            return lambda _snapshot: None

        def _checkpoint(snapshot: dict[str, Any]) -> None:
            checkpoint = build_task_checkpoint(
                workspace_root,
                checkpoint_id=str(snapshot.get("session_id") or self.handler.ctx.session_id),
                session_id=str(snapshot.get("session_id") or self.handler.ctx.session_id),
                task=query,
                agent_name=self.agent_name,
                turn=int(snapshot.get("turn") or 0),
                status=str(snapshot.get("status") or "running"),
                exit_reason=str(snapshot.get("exit_reason") or ""),
                tool_results=snapshot.get("tool_results") if isinstance(snapshot.get("tool_results"), list) else [],
                working=snapshot.get("working") if isinstance(snapshot.get("working"), dict) else {},
                history_info=snapshot.get("history_info") if isinstance(snapshot.get("history_info"), list) else [],
                pending_prompts=snapshot.get("pending_prompts") if isinstance(snapshot.get("pending_prompts"), list) else [],
            )
            record = write_task_checkpoint(workspace_root, checkpoint)
            self.handler.ctx.sink.emit(
                Event(
                    session_id=self.handler.ctx.session_id,
                    turn=int(snapshot.get("turn") or 0),
                    kind="checkpoint_written",
                    name=str(record.get("status", "UNKNOWN")),
                    data={"checkpoint_id": record.get("checkpoint_id", ""), "path": record.get("path", "")},
                )
            )

        return _checkpoint

    def _write_terminal_checkpoint(self, query: str, result: dict[str, Any], status: str) -> None:
        callback = getattr(self.handler.ctx, "checkpoint_callback", None)
        if not callable(callback):
            return
        callback(
            {
                "session_id": self.handler.ctx.session_id,
                "turn": self.handler.ctx.current_turn,
                "status": status,
                "exit_reason": str(result.get("exit_reason", "")),
                "tool_results": result.get("tool_results", []),
                "working": dict(getattr(self.handler.ctx, "working", {}) or {}),
                "history_info": list(getattr(self.handler.ctx, "history_info", []) or []),
                "pending_prompts": [query],
            }
        )

    def _distill_runbook(self, query: str, result: dict[str, Any], events: list[Event]) -> None:
        try:
            workspace_root = getattr(self, "cwd", "") or getattr(self, "workspace_dir", "")
            if not workspace_root:
                self.handler.ctx.sink.emit(
                    Event(
                        session_id=self.handler.ctx.session_id,
                        turn=self.handler.ctx.current_turn,
                        kind="runbook_distilled",
                        name="SKIP",
                        data={"error": "missing_workspace"},
                    )
                )
                return
            record = distill_runbook_from_task(
                workspace_root,
                query,
                result,
                agent_name=self.agent_name,
                trace_events=[asdict(event) for event in events],
                min_interaction_records=self.runbook_min_interaction_records,
            )
            self.handler.ctx.sink.emit(
                Event(
                    session_id=self.handler.ctx.session_id,
                    turn=self.handler.ctx.current_turn,
                    kind="runbook_distilled",
                    name=str(record.get("status", "UNKNOWN")),
                    data={
                        "outcome": record.get("outcome", ""),
                        "entry_key": record.get("entry_key", ""),
                    },
                )
            )
            if record.get("status") == "OK":
                self.skill_registry = self._load_skill_registry()
                self.handler.ctx.skills = self.skill_registry
        except Exception as exc:  # noqa: BLE001
            self.handler.ctx.sink.emit(
                Event(
                    session_id=self.handler.ctx.session_id,
                    turn=self.handler.ctx.current_turn,
                    kind="runbook_distilled",
                    name="ERROR",
                    data={"error": type(exc).__name__},
                )
            )

    def run_task_async(
        self,
        query: str,
        *,
        resume_checkpoint: str | None = None,
        checkpoint_id: str | None = None,
        reset_stop_event: bool = True,
    ) -> None:
        if self.owns_stop_event and reset_stop_event:
            self.stop_event.clear()
        thread = threading.Thread(
            target=self.run_task,
            args=(query, resume_checkpoint, checkpoint_id),
            kwargs={"reset_stop_event": False},
            daemon=True,
        )
        thread.start()

    def is_running(self) -> bool:
        return self._running.is_set()

    def stop(self) -> None:
        self.stop_event.set()

    def close(self) -> None:
        try:
            handler = getattr(self, "handler", None)
            if handler is not None:
                handler.close()
        finally:
            self.sink.close()

    def run(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        while not self.task_queue.empty():
            query = self.task_queue.get()
            results.append(self.run_task(query))
        return results
