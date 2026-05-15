from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from src.core.agent_loop import AgentContext
from src.handler import XAgentHandler
from src.tools.browser_driver import BrowserSession, save_result
from src.tools.file_ops import resolve_path
from src.tools.web import set_browser_driver, web_execute_js, web_scan


class FakeBrowserDriver:
    def __init__(self) -> None:
        self.scan_args: dict[str, Any] = {}
        self.execute_args: dict[str, Any] = {}

    def list_sessions(self) -> list[BrowserSession]:
        return [
            BrowserSession(
                id="tab-1",
                url="https://example.com",
                title="Example",
                type="fake",
                active=True,
                connected_at=1.0,
            )
        ]

    def scan(
        self,
        url: str | None = None,
        session_id: str | None = None,
        mode: str = "summary",
        max_chars: int = 8000,
        tab_index: int | None = None,
    ) -> dict[str, Any]:
        self.scan_args = {
            "url": url,
            "session_id": session_id,
            "mode": mode,
            "max_chars": max_chars,
            "tab_index": tab_index,
        }
        return {
            "status": "OK",
            "sessions": [session.__dict__ for session in self.list_sessions()],
            "current_session_id": "tab-1",
            "page": {"url": url or "https://example.com", "title": "Example", "content": "body"},
        }

    def execute_js(
        self,
        script: str,
        session_id: str | None = None,
        timeout: int = 10,
        save_to_file: str | None = None,
        cwd: str | None = None,
        await_navigation: bool = True,
    ) -> dict[str, Any]:
        self.execute_args = {
            "script": script,
            "session_id": session_id,
            "timeout": timeout,
            "save_to_file": save_to_file,
            "cwd": cwd,
            "await_navigation": await_navigation,
        }
        return {
            "status": "OK",
            "session_id": session_id or "tab-1",
            "exec_id": "exec-1",
            "ack": True,
            "result_received": True,
            "result": "ok",
            "diagnostics": {"navigation": False, "reloaded": False, "new_tabs": [], "duration_ms": 1},
        }


class WebToolTests(unittest.TestCase):
    def tearDown(self) -> None:
        set_browser_driver(None)

    def test_web_scan_delegates_to_browser_driver(self) -> None:
        driver = FakeBrowserDriver()
        set_browser_driver(driver)

        result = web_scan(url=None, session_id="tab-1", mode="elements", max_chars=123)

        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["current_session_id"], "tab-1")
        self.assertEqual(driver.scan_args["session_id"], "tab-1")
        self.assertEqual(driver.scan_args["mode"], "elements")
        self.assertEqual(driver.scan_args["max_chars"], 123)

    def test_handler_passes_browser_session_arguments(self) -> None:
        driver = FakeBrowserDriver()
        set_browser_driver(driver)
        handler = XAgentHandler(ctx=AgentContext(cwd="/tmp/workspace"))

        result = handler.exec_web_execute_js(
            {
                "script": "return document.title",
                "session_id": "tab-1",
                "timeout": 7,
                "await_navigation": False,
            }
        )

        self.assertEqual(result.data["status"], "OK")
        self.assertEqual(driver.execute_args["session_id"], "tab-1")
        self.assertEqual(driver.execute_args["timeout"], 7)
        self.assertFalse(driver.execute_args["await_navigation"])

    def test_save_result_writes_full_payload_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            payload = "[1, 2, 3]"

            result = save_result(payload, "web/result.json", str(root))

            target = resolve_path("web/result.json", str(root))
            self.assertEqual(target.read_text(encoding="utf-8"), payload)
            self.assertEqual(result["saved_to"], str(target))
            self.assertEqual(result["summary"]["type"], "array")
            self.assertEqual(result["summary"]["items"], 3)

    def test_save_result_denies_outside_workspace_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "workspace"
            outside = Path(tmp_dir) / "outside.json"
            root.mkdir()

            result = save_result("payload", str(outside), str(root))

            self.assertEqual(result["status"], "ERROR")
            self.assertEqual(result["operation"], "write")
            self.assertFalse(outside.exists())

    def test_save_result_denies_workspace_system_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)

            result = save_result("payload", "system/output.json", str(root))

            self.assertEqual(result["status"], "ERROR")
            self.assertEqual(result["operation"], "write")
            self.assertFalse((root / "system" / "output.json").exists())

    def test_save_result_allows_runtime_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)

            result = save_result("payload", "runtime/output.json", str(root))

            target = root / "runtime" / "output.json"
            self.assertEqual(result["saved_to"], str(target.resolve()))
            self.assertEqual(target.read_text(encoding="utf-8"), "payload")


if __name__ == "__main__":
    unittest.main()
