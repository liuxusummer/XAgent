from __future__ import annotations

import json
import queue
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.main import (
    _clean_content,
    _parse_session_value,
    build_agent,
    format_result,
    format_tool_result,
    handle_session_command,
    handle_verbose_command,
    load_observability_config,
    repl,
)
from src.core.XAgent import XAgent, resolve_workspace_dir
from src.core.telemetry import JsonlSink, MultiSink, NullSink, StderrSink


class ParseSessionValueTests(unittest.TestCase):
    def test_json_number(self) -> None:
        self.assertEqual(_parse_session_value("0.7"), 0.7)

    def test_json_int(self) -> None:
        self.assertEqual(_parse_session_value("4096"), 4096)

    def test_plain_string(self) -> None:
        self.assertEqual(_parse_session_value("enabled"), "enabled")


class CleanContentTests(unittest.TestCase):
    def test_removes_xml_tags(self) -> None:
        text = "<thinking>deep</thinking> result <summary>ok</summary>"
        cleaned = _clean_content(text)
        self.assertNotIn("<thinking>", cleaned)
        self.assertNotIn("</thinking>", cleaned)
        self.assertIn("result", cleaned)

    def test_folds_long_output(self) -> None:
        lines = [f"line {i}" for i in range(20)]
        text = "\n".join(lines)
        cleaned = _clean_content(text)
        self.assertIn("lines omitted", cleaned)
        self.assertIn("line 0", cleaned)
        self.assertIn("line 19", cleaned)

    def test_short_output_unchanged(self) -> None:
        text = "short output"
        self.assertEqual(_clean_content(text), text)


class FormatToolResultTests(unittest.TestCase):
    def test_dict_with_status(self) -> None:
        result = format_tool_result({"tool_name": "file_read", "data": {"status": "OK", "content": "hello"}})
        self.assertIn("file_read", result)
        self.assertIn("OK", result)

    def test_non_dict_data(self) -> None:
        result = format_tool_result({"tool_name": "echo", "data": "plain text"})
        self.assertIn("echo", result)


class FormatResultTests(unittest.TestCase):
    def test_verbose_mode(self) -> None:
        result = {
            "response": "done",
            "tool_results": [{"tool_name": "file_read", "data": {"status": "OK"}}],
        }
        formatted = format_result(result, verbose=True)
        self.assertIn("file_read", formatted)
        self.assertIn("done", formatted)

    def test_non_verbose_mode(self) -> None:
        result = {"response": "task completed", "tool_results": []}
        formatted = format_result(result, verbose=False)
        self.assertEqual(formatted, "task completed")

    def test_no_output(self) -> None:
        result = {"response": "", "tool_results": []}
        formatted = format_result(result, verbose=False)
        self.assertEqual(formatted, "(no output)")


class HandleSessionCommandTests(unittest.TestCase):
    def test_set_temperature(self) -> None:
        agent = XAgent(
            system_prompt="test",
            tools_schema=[],
            api_key="sk-test",
            base_url="https://api.openai.com/v1/chat/completions",
            model="gpt-4o",
        )
        handle_session_command(agent, "temperature", "0.5")
        backend = agent.client.backend
        self.assertEqual(backend.temperature, 0.5)

    def test_disallowed_attr(self) -> None:
        agent = XAgent(
            system_prompt="test",
            tools_schema=[],
            api_key="sk-test",
            base_url="https://api.openai.com/v1/chat/completions",
            model="gpt-4o",
        )
        with patch("builtins.print") as mock_print:
            handle_session_command(agent, "api_key", "new_key")
            mock_print.assert_called_once()
            self.assertIn("cannot set", mock_print.call_args[0][0])


class HandleVerboseCommandTests(unittest.TestCase):
    def test_verbose_on(self) -> None:
        agent = XAgent(
            system_prompt="test",
            tools_schema=[],
            api_key="sk-test",
            base_url="https://api.openai.com/v1/chat/completions",
            model="gpt-4o",
        )
        handle_verbose_command(agent, "on")
        self.assertTrue(agent.handler.ctx.verbose)

    def test_verbose_off(self) -> None:
        agent = XAgent(
            system_prompt="test",
            tools_schema=[],
            api_key="sk-test",
            base_url="https://api.openai.com/v1/chat/completions",
            model="gpt-4o",
        )
        agent.handler.ctx.verbose = True
        handle_verbose_command(agent, "off")
        self.assertFalse(agent.handler.ctx.verbose)


class WorkspaceTests(unittest.TestCase):
    def test_resolve_workspace_dir_defaults_to_project_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = resolve_workspace_dir(code_root=tmp_dir)
            workspace_path = Path(workspace)

            self.assertEqual(workspace_path, (Path(tmp_dir) / "workspace" / "default.ws").resolve())
            self.assertTrue(workspace_path.is_dir())
            self.assertTrue((workspace_path / "business").is_dir())
            self.assertTrue((workspace_path / "runtime").is_dir())
            self.assertTrue((workspace_path / "system" / "agents" / "main").is_dir())
            self.assertTrue((workspace_path / "system" / "agents" / "coding").is_dir())
            self.assertTrue((workspace_path / "system" / "memory").is_dir())
            self.assertTrue((workspace_path / "system" / "skills").is_dir())
            self.assertTrue((workspace_path / "system" / "templates").is_dir())
            self.assertTrue((workspace_path / "system" / "agents" / "main" / "AGENT.md").is_file())
            self.assertTrue((workspace_path / "system" / "agents" / "main" / "SOUL.md").is_file())
            self.assertTrue((workspace_path / "system" / "agents" / "main" / "MEMORY.md").is_file())
            self.assertTrue((workspace_path / "system" / "agents" / "coding" / "AGENT.md").is_file())
            self.assertTrue((workspace_path / "system" / "agents" / "coding" / "SOUL.md").is_file())
            self.assertTrue((workspace_path / "system" / "agents" / "coding" / "MEMORY.md").is_file())

    def test_xagent_uses_explicit_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir) / "agent-work"
            agent = XAgent(
                system_prompt="test",
                tools_schema=[],
                api_key="sk-test",
                base_url="https://api.openai.com/v1/chat/completions",
                model="gpt-4o",
                workspace_dir=str(workspace),
            )

            self.assertEqual(agent.workspace_dir, str(workspace.resolve()))
            self.assertEqual(agent.cwd, str(workspace.resolve()))
            self.assertEqual(agent.handler.ctx.cwd, str(workspace.resolve()))
            self.assertTrue(workspace.is_dir())
            self.assertTrue((workspace / "business").is_dir())
            self.assertTrue((workspace / "system" / "agents" / "main" / "AGENT.md").is_file())

    def test_build_agent_injects_workspace_into_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            agent = build_agent(workspace_dir=tmp_dir)

            self.assertIn(f"workspace = {Path(tmp_dir).resolve()}", agent.system_prompt)
            self.assertIn(f"cwd = {Path(tmp_dir).resolve()}", agent.system_prompt)
            self.assertEqual(agent.handler.ctx.cwd, str(Path(tmp_dir).resolve()))


class ReplStreamingTests(unittest.TestCase):
    def test_repl_prints_progress_before_done(self) -> None:
        class _Agent:
            def __init__(self) -> None:
                self.display_queue = queue.Queue()
                self.reply_queue = queue.Queue()
                self._running = False
                self.handler = type("H", (), {"ctx": type("Ctx", (), {"verbose": False})()})()

            def is_running(self) -> bool:
                return self._running

            def run_task_async(self, query: str) -> None:
                self._running = True
                self.display_queue.put({"progress": f"running {query}"})
                self.display_queue.put({"done": {"response": "done", "exit_reason": "CURRENT_TASK_DONE", "tool_results": []}})
                self._running = False

            def stop(self) -> None:
                self._running = False

        agent = _Agent()
        with patch("builtins.input", side_effect=["hello", EOFError]), patch("builtins.print") as mock_print:
            repl(agent)  # type: ignore[arg-type]

        printed = [call.args[0] for call in mock_print.call_args_list if call.args]
        self.assertIn("XAgent REPL — type /help for commands, /exit to quit", printed)
        self.assertIn("done", printed)

        stderr_progress = [call.args[0] for call in mock_print.call_args_list if call.kwargs.get("file")]
        self.assertIn("running hello", stderr_progress)


class ObservabilityConfigTests(unittest.TestCase):
    def test_load_observability_config_defaults_empty(self) -> None:
        self.assertEqual(load_observability_config(None), {})

    def test_load_observability_config_reads_json_object(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "obs.json"
            path.write_text(json.dumps({"backend": "langfuse", "stderr": True}), encoding="utf-8")

            self.assertEqual(
                load_observability_config(str(path)),
                {"backend": "langfuse", "stderr": True},
            )

    def test_load_observability_config_rejects_non_object(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "obs.json"
            path.write_text("[]", encoding="utf-8")

            with self.assertRaises(ValueError):
                load_observability_config(str(path))

    def test_build_sink_uses_observability_config(self) -> None:
        from src.main import build_sink

        old_env = __import__("os").environ.copy()
        try:
            for key in (
                "XAGENT_LOG_DIR",
                "XAGENT_LOG_STDERR",
                "XAGENT_OBS_BACKEND",
                "XAGENT_LANGFUSE_ENABLED",
            ):
                __import__("os").environ.pop(key, None)
            with tempfile.TemporaryDirectory() as tmp_dir:
                sink = build_sink({"log_dir": tmp_dir, "stderr": True})
                self.assertIsInstance(sink, MultiSink)
                self.assertTrue(any(isinstance(item, JsonlSink) for item in sink.sinks))
                self.assertTrue(any(isinstance(item, StderrSink) for item in sink.sinks))
                sink.close()
        finally:
            __import__("os").environ.clear()
            __import__("os").environ.update(old_env)

    def test_build_sink_env_overrides_observability_config(self) -> None:
        from src.main import build_sink

        old_env = __import__("os").environ.copy()
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                __import__("os").environ["XAGENT_LOG_STDERR"] = "1"
                sink = build_sink({"log_dir": tmp_dir, "stderr": False})
                self.assertIsInstance(sink, MultiSink)
                self.assertTrue(any(isinstance(item, JsonlSink) for item in sink.sinks))
                self.assertTrue(any(isinstance(item, StderrSink) for item in sink.sinks))
                sink.close()
        finally:
            __import__("os").environ.clear()
            __import__("os").environ.update(old_env)


if __name__ == "__main__":
    unittest.main()
