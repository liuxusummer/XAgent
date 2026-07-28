from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from selenium.common.exceptions import TimeoutException

from src.core.agent_loop import AgentContext
from src.handler import XAgentHandler
from src.tools.browser_driver import (
    BrowserSession,
    SeleniumBrowserDriver,
    UnsafeNavigationError,
    _chrome_launch_arguments,
    _page_load_timeout,
    _validate_navigation_url,
    save_result,
)
from src.tools.file_ops import resolve_path
from src.tools.web import web_scan


class FakeBrowserDriver:
    def __init__(self, name: str = "fake") -> None:
        self.name = name
        self.scan_args: dict[str, Any] = {}
        self.execute_args: dict[str, Any] = {}
        self.closed = False

    def close(self) -> None:
        self.closed = True

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
            "page": {"url": url or "https://example.com", "title": self.name, "content": "body"},
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


class FakeSeleniumDriver:
    def __init__(
        self,
        *,
        delay: float = 0.0,
        timeout: bool = False,
        navigation_timeout: bool = False,
    ) -> None:
        self.current_url = "about:blank"
        self.current_window_handle = "tab-1"
        self.window_handles = ["tab-1"]
        self.title = "Example"
        self.delay = delay
        self.timeout = timeout
        self.navigation_timeout = navigation_timeout
        self.script_timeout: float | None = None
        self.page_load_timeout: float | None = None
        self.requested_urls: list[str] = []
        self.async_script = ""
        self.active_calls = 0
        self.max_active_calls = 0
        self._counter_lock = threading.Lock()
        self.quit_called = False
        self.switch_to = self

    def window(self, handle: str) -> None:
        self.current_window_handle = handle

    def set_script_timeout(self, timeout: float) -> None:
        self.script_timeout = timeout

    def set_page_load_timeout(self, timeout: float) -> None:
        self.page_load_timeout = timeout

    def get(self, url: str) -> None:
        self.requested_urls.append(url)
        if self.navigation_timeout:
            raise TimeoutException("page load timeout")
        self.current_url = url

    def execute_async_script(self, script: str) -> dict[str, Any]:
        self.async_script = script
        with self._counter_lock:
            self.active_calls += 1
            self.max_active_calls = max(self.max_active_calls, self.active_calls)
        try:
            time.sleep(self.delay)
            if self.timeout:
                raise TimeoutException("script timeout")
            return {"__xagent_script_status__": "ok", "value": "done"}
        finally:
            with self._counter_lock:
                self.active_calls -= 1

    def quit(self) -> None:
        self.quit_called = True


class WebToolTests(unittest.TestCase):
    def test_web_scan_delegates_to_browser_driver(self) -> None:
        driver = FakeBrowserDriver()

        result = web_scan(driver, url=None, session_id="tab-1", mode="elements", max_chars=123)

        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["current_session_id"], "tab-1")
        self.assertEqual(driver.scan_args["session_id"], "tab-1")
        self.assertEqual(driver.scan_args["mode"], "elements")
        self.assertEqual(driver.scan_args["max_chars"], 123)

    def test_handler_passes_browser_session_arguments(self) -> None:
        driver = FakeBrowserDriver()
        handler = XAgentHandler(ctx=AgentContext(cwd="/tmp/workspace"), browser_driver=driver)

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

    def test_handlers_keep_browser_drivers_isolated(self) -> None:
        first_driver = FakeBrowserDriver("first")
        second_driver = FakeBrowserDriver("second")
        first = XAgentHandler(ctx=AgentContext(), browser_driver=first_driver)
        second = XAgentHandler(ctx=AgentContext(), browser_driver=second_driver)

        first_result = first.exec_web_scan({}).data
        second_result = second.exec_web_scan({}).data

        self.assertEqual(first_result["page"]["title"], "first")
        self.assertEqual(second_result["page"]["title"], "second")
        self.assertIsNot(first._get_browser_driver(), second._get_browser_driver())

    def test_handler_close_releases_only_its_browser_driver(self) -> None:
        first_driver = FakeBrowserDriver("first")
        second_driver = FakeBrowserDriver("second")
        first = XAgentHandler(ctx=AgentContext(), browser_driver=first_driver)
        second = XAgentHandler(ctx=AgentContext(), browser_driver=second_driver)

        first.close()

        self.assertTrue(first_driver.closed)
        self.assertFalse(second_driver.closed)
        with self.assertRaisesRegex(RuntimeError, "handler is closed"):
            first._get_browser_driver()

    def test_selenium_driver_uses_native_script_timeout(self) -> None:
        raw_driver = FakeSeleniumDriver()
        driver = SeleniumBrowserDriver()
        driver._driver = raw_driver

        result = driver.execute_js("return document.title", timeout=3)

        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["result"], "done")
        self.assertEqual(raw_driver.script_timeout, 3)
        self.assertIn("return document.title", raw_driver.async_script)

    def test_selenium_scan_blocks_cloud_metadata_address_before_navigation(self) -> None:
        raw_driver = FakeSeleniumDriver()
        driver = SeleniumBrowserDriver()
        driver._driver = raw_driver

        result = driver.scan("http://169.254.169.254/latest/meta-data/")

        self.assertEqual(result["status"], "ERROR")
        self.assertEqual(result["error_type"], "SSRF_BLOCKED")
        self.assertEqual(raw_driver.requested_urls, [])

    def test_navigation_validation_rejects_dns_resolving_to_private_address(self) -> None:
        private_record = [(2, 1, 6, "", ("10.0.0.8", 443))]
        with patch("src.core.network_guard.socket.getaddrinfo", return_value=private_record):
            with self.assertRaises(UnsafeNavigationError):
                _validate_navigation_url("https://internal.example/data")

    def test_selenium_scan_enforces_page_load_timeout(self) -> None:
        raw_driver = FakeSeleniumDriver(navigation_timeout=True)
        driver = SeleniumBrowserDriver()
        driver._driver = raw_driver
        public_record = [(2, 1, 6, "", ("93.184.216.34", 443))]

        with patch("src.core.network_guard.socket.getaddrinfo", return_value=public_record):
            result = driver.scan("https://example.com")

        self.assertEqual(result["status"], "TIMEOUT")
        self.assertEqual(result["error_type"], "TIMEOUT")
        self.assertEqual(raw_driver.page_load_timeout, 30)

    def test_chrome_sandbox_is_disabled_only_by_explicit_opt_in(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            self.assertNotIn("--no-sandbox", _chrome_launch_arguments())
        with patch.dict("os.environ", {"XAGENT_CHROME_NO_SANDBOX": "1"}, clear=True):
            self.assertIn("--no-sandbox", _chrome_launch_arguments())

    def test_chrome_routes_all_supported_network_traffic_through_guard_proxy(self) -> None:
        arguments = _chrome_launch_arguments("http://127.0.0.1:43210")

        self.assertIn("--proxy-server=http://127.0.0.1:43210", arguments)
        self.assertIn("--proxy-bypass-list=<-loopback>", arguments)
        self.assertIn("--disable-quic", arguments)
        self.assertIn(
            "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
            arguments,
        )

    def test_page_load_timeout_is_bounded(self) -> None:
        with patch.dict("os.environ", {"XAGENT_CHROME_PAGE_LOAD_TIMEOUT": "999"}, clear=True):
            self.assertEqual(_page_load_timeout(), 120)

    def test_selenium_driver_reports_script_timeout(self) -> None:
        raw_driver = FakeSeleniumDriver(timeout=True)
        driver = SeleniumBrowserDriver()
        driver._driver = raw_driver

        result = driver.execute_js("while (true) {}", timeout=1)

        self.assertEqual(result["status"], "TIMEOUT")
        self.assertEqual(result["error_type"], "TIMEOUT")
        self.assertTrue(result["ack"])
        self.assertFalse(result["result_received"])

    def test_selenium_driver_serializes_operations(self) -> None:
        raw_driver = FakeSeleniumDriver(delay=0.03)
        driver = SeleniumBrowserDriver()
        driver._driver = raw_driver
        results: list[dict[str, Any]] = []

        threads = [
            threading.Thread(target=lambda: results.append(driver.execute_js("return 1"))),
            threading.Thread(target=lambda: results.append(driver.execute_js("return 2"))),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(results), 2)
        self.assertTrue(all(result["status"] == "OK" for result in results))
        self.assertEqual(raw_driver.max_active_calls, 1)

    def test_selenium_driver_close_quits_browser(self) -> None:
        raw_driver = FakeSeleniumDriver()
        driver = SeleniumBrowserDriver()
        driver._driver = raw_driver

        driver.close()

        self.assertTrue(raw_driver.quit_called)
        self.assertIsNone(driver._driver)
        result = driver.execute_js("return 1")
        self.assertEqual(result["status"], "ERROR")
        self.assertIn("closed", result["error"])

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
