from __future__ import annotations

import os
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from queue import Queue
from typing import Any

from src.config import create_client, load_config
from src.core.agent_loop import AgentContext, run_agent_loop
from src.core.llm import MixinSession, NativeToolClient, OpenAITextSession, ToolClient
from src.core.skills import SkillRegistry, dedupe_skill_names, select_skills
from src.core.telemetry import Event, EventSink, NullSink
from src.handler import XAgentHandler
from src.tools.interaction import make_progress_emitter, make_user_input_bridge


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MEMORY_ROOT = PROJECT_ROOT / "memory"
DEFAULT_WORKSPACE_NAME = "default.ws"
WORKSPACE_LAYOUT_DIRS = (
    "business",
    "runtime",
    "system/agents/main",
    "system/agents/coding",
    "system/memory",
    "system/skills",
    "system/templates",
)
WORKSPACE_LAYOUT_FILES = (
    "system/agents/main/AGENT.md",
    "system/agents/main/SOUL.md",
    "system/agents/coding/AGENT.md",
    "system/agents/coding/SOUL.md",
)


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
    _running: threading.Event = field(default_factory=threading.Event)
    sink: EventSink = field(default_factory=NullSink)
    skills_dir: str | None = None
    max_turns: int = 40

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
                memory_root=str(MEMORY_ROOT),
            ),
            task_dir=self.workspace_dir,
        )

    def _load_skill_registry(self) -> SkillRegistry:
        if self.skills_dir:
            paths = [Path(self.skills_dir)]
        else:
            paths = [PROJECT_ROOT / "skills"]
        return SkillRegistry.load(paths)

    def _create_client(self) -> ToolClient | NativeToolClient:
        if self.config_path:
            configs = load_config(self.config_path)
            if configs:
                first_config = next(iter(configs.values()))
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

    def run_task(self, query: str) -> dict[str, Any]:
        self._running.set()
        self.stop_event.clear()
        # Phase 8：每次任务刷新 session_id，便于事件流按会话归集
        self.handler.ctx.session_id = uuid.uuid4().hex[:16]
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
            self.display_queue.put({"done": result})
            return result
        finally:
            self._running.clear()

    def run_task_async(self, query: str) -> None:
        thread = threading.Thread(target=self.run_task, args=(query,), daemon=True)
        thread.start()

    def is_running(self) -> bool:
        return self._running.is_set()

    def stop(self) -> None:
        self.stop_event.set()

    def close(self) -> None:
        self.sink.close()

    def run(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        while not self.task_queue.empty():
            query = self.task_queue.get()
            results.append(self.run_task(query))
        return results
