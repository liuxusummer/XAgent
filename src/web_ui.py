from __future__ import annotations

import argparse
import queue
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from src.main import build_agent, format_result


@dataclass
class UISession:
    agent: object | None = None
    logs: list[str] = field(default_factory=list)
    llm_stream_raw: str = ""
    thinking_stream_raw: str = ""
    last_agent_text: str = ""
    waiting_for_user: bool = False
    ask_prompt: str = ""
    running: bool = False
    config_path: str = ""
    observability_config_path: str = ""
    workspace_dir: str = ""


_SESSION_LOCK = threading.Lock()
_HIDDEN_STREAM_TAGS = ("thinking", "tool_use", "summary", "history", "key_info", "earlier_context")
_STREAM_TAG_RE = re.compile(r"</?(?:thinking|tool_use|summary|history|key_info|earlier_context)[^>]*>", re.DOTALL)
_HIDDEN_TAG_PREFIXES = tuple(prefix for tag in _HIDDEN_STREAM_TAGS for prefix in (f"<{tag}", f"</{tag}"))


def _append(session: UISession, line: str) -> None:
    if line:
        session.logs.append(line)


def _strip_hidden_tag_tail(text: str) -> str:
    last_open = text.rfind("<")
    last_close = text.rfind(">")
    if last_open > last_close:
        tail = text[last_open:]
        if any(prefix.startswith(tail) or tail.startswith(prefix) for prefix in _HIDDEN_TAG_PREFIXES):
            return text[:last_open]
    return text


def _friendly_stream_text(raw: str) -> str:
    text = raw.replace("\r", "")
    for tag in _HIDDEN_STREAM_TAGS:
        text = re.sub(fr"<{tag}[^>]*>.*?</{tag}>", "", text, flags=re.DOTALL)
        start = text.rfind(f"<{tag}")
        end = text.rfind(f"</{tag}>")
        if start != -1 and end < start:
            text = text[:start]
    text = _STREAM_TAG_RE.sub("", text)
    return _strip_hidden_tag_tail(text).strip()


def _live_lines(session: UISession) -> list[str]:
    lines: list[str] = []
    llm_text = _friendly_stream_text(session.llm_stream_raw)
    if llm_text:
        lines.append(f"Agent: {llm_text}")
    thinking_text = _friendly_stream_text(session.thinking_stream_raw)
    if thinking_text:
        lines.append(f"Thinking: {thinking_text}")
    return lines


def _flush_live_lines(session: UISession) -> None:
    lines = _live_lines(session)
    agent_text = _friendly_stream_text(session.llm_stream_raw)
    for line in lines:
        _append(session, line)
    if agent_text:
        session.last_agent_text = agent_text
    session.llm_stream_raw = ""
    session.thinking_stream_raw = ""


def _should_append_done_result(session: UISession, result: dict[str, Any], verbose: bool) -> bool:
    if verbose:
        return True
    response = str(result.get("response", "")).strip()
    if not response:
        return True
    final_text = format_result(result, verbose=False).strip()
    return final_text != session.last_agent_text.strip()


def _append_progress(session: UISession, message: str) -> None:
    if message.startswith("  llm | "):
        session.llm_stream_raw += message[len("  llm | "):]
        return
    if message.startswith("  thinking | "):
        session.thinking_stream_raw += message[len("  thinking | "):]
        return
    _flush_live_lines(session)
    _append(session, message)


def _render(session: UISession) -> str:
    return "\n".join(session.logs + _live_lines(session))


def _close_agent(session: UISession) -> None:
    agent = session.agent
    if agent is not None:
        agent.close()
    session.agent = None


def _normalize_path_input(value: str | None) -> str:
    return value.strip() if isinstance(value, str) else ""


def _ensure_agent(session: UISession, config_path: str, observability_config_path: str, workspace_dir: str):
    if (
        session.agent is None
        or session.config_path != config_path
        or session.observability_config_path != observability_config_path
        or session.workspace_dir != workspace_dir
    ):
        _close_agent(session)
        session.agent = build_agent(
            config_path=config_path or None,
            observability_config_path=observability_config_path or None,
            workspace_dir=workspace_dir or None,
        )
        session.config_path = config_path
        session.observability_config_path = observability_config_path
        session.workspace_dir = workspace_dir
    return session.agent


def _drain(session: UISession, wait: bool) -> bool:
    agent = session.agent
    if agent is None:
        return True

    finished = False
    while True:
        timeout = 0.1 if wait and session.running else 0.0
        try:
            msg = agent.display_queue.get(timeout=timeout)
        except queue.Empty:
            break

        if "progress" in msg:
            _append_progress(session, msg["progress"])
        elif "ask_user" in msg:
            _flush_live_lines(session)
            session.waiting_for_user = True
            session.running = False
            session.ask_prompt = msg["ask_user"]
            _append(session, f"[ask_user] {msg['ask_user']}")
            break
        elif "done" in msg:
            _flush_live_lines(session)
            result = msg["done"]
            if _should_append_done_result(session, result, agent.handler.ctx.verbose):
                _append(session, format_result(result, agent.handler.ctx.verbose))
            _append(session, f"[exit_reason] {result.get('exit_reason', '')}")
            session.running = False
            session.waiting_for_user = False
            session.ask_prompt = ""
            finished = True
            break

        if not wait:
            continue
        if not agent.is_running():
            break
    return finished


def submit_task(task: str, config_path: str, observability_config_path: str, workspace_dir: str, session: UISession | None):
    gr = _gradio()
    session = session or UISession()
    with _SESSION_LOCK:
        agent = _ensure_agent(
            session,
            _normalize_path_input(config_path),
            _normalize_path_input(observability_config_path),
            _normalize_path_input(workspace_dir),
        )
        if session.running:
            yield session, _render(session), session.ask_prompt, gr.update(interactive=False), gr.update(interactive=True)
            return
        session.logs = []
        session.llm_stream_raw = ""
        session.thinking_stream_raw = ""
        session.last_agent_text = ""
        session.waiting_for_user = False
        session.ask_prompt = ""
        session.running = True
        _append(session, f"> {task.strip()}")
        agent.run_task_async(task.strip())

    while True:
        with _SESSION_LOCK:
            finished = _drain(session, wait=True)
            yield session, _render(session), session.ask_prompt, gr.update(interactive=session.waiting_for_user), gr.update(interactive=session.running)
            if finished or session.waiting_for_user or not session.running:
                break
        time.sleep(0.05)


def send_reply(reply: str, session: UISession | None):
    gr = _gradio()
    session = session or UISession()
    with _SESSION_LOCK:
        agent = session.agent
        if agent is None or not session.waiting_for_user:
            yield session, _render(session), session.ask_prompt, gr.update(value="", interactive=False), gr.update(interactive=session.running)
            return
        session.waiting_for_user = False
        session.running = True
        _append(session, f"[user_reply] {reply.strip()}")
        agent.reply_queue.put(reply.strip())

    while True:
        with _SESSION_LOCK:
            finished = _drain(session, wait=True)
            yield session, _render(session), session.ask_prompt, gr.update(value="", interactive=session.waiting_for_user), gr.update(interactive=session.running)
            if finished or session.waiting_for_user or not session.running:
                break
        time.sleep(0.05)


def stop_task(session: UISession | None):
    gr = _gradio()
    session = session or UISession()
    with _SESSION_LOCK:
        agent = session.agent
        if agent is not None and agent.is_running():
            agent.stop()
            _append(session, "[stop] interrupt signal sent")
        return session, _render(session), gr.update(interactive=False)


def build_demo() -> Any:
    gr = _gradio()
    with gr.Blocks(title="XAgent Web UI") as demo:
        session_state = gr.State(UISession())
        gr.Markdown("# XAgent\nEnter a task and watch live output.")
        task_input = gr.Textbox(label="Task", lines=5, placeholder="Tell XAgent what to do...")
        with gr.Row():
            run_btn = gr.Button("Run", variant="primary")
            stop_btn = gr.Button("Stop", interactive=False)
        logs = gr.Textbox(label="Output", lines=26, interactive=False)
        ask_prompt = gr.Textbox(label="Agent asks", interactive=False, visible=True)
        reply_input = gr.Textbox(label="Reply", interactive=False, placeholder="Reply when agent asks a question...")
        reply_btn = gr.Button("Send Reply")
        with gr.Accordion("Advanced", open=False):
            config_path = gr.Textbox(label="Config Path", value="config.json", placeholder="config.json")
            observability_config_path = gr.Textbox(
                label="Observability Config",
                placeholder="observability.example.json",
            )
            workspace_dir = gr.Textbox(
                label="Workspace Directory",
                placeholder="Defaults to <project_root>/workspace",
            )

        run_btn.click(
            submit_task,
            inputs=[task_input, config_path, observability_config_path, workspace_dir, session_state],
            outputs=[session_state, logs, ask_prompt, reply_input, stop_btn],
        )
        reply_btn.click(
            send_reply,
            inputs=[reply_input, session_state],
            outputs=[session_state, logs, ask_prompt, reply_input, stop_btn],
        )
        stop_btn.click(stop_task, inputs=[session_state], outputs=[session_state, logs, stop_btn])
    return demo


def main() -> None:
    parser = argparse.ArgumentParser(description="XAgent minimal Gradio UI")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7860)
    args = parser.parse_args()
    demo = build_demo()
    demo.launch(server_name=args.host, server_port=args.port)


def _gradio():
    try:
        import gradio as gr
    except ImportError as exc:
        raise RuntimeError("gradio is not installed; run uv sync --extra web first") from exc
    return gr


if __name__ == "__main__":
    main()
