from __future__ import annotations

import json
import os
import re
import threading
import time
import urllib.parse
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from src.core.network_guard import (
    PublicEgressProxy,
    UnsafeNetworkTargetError,
    resolve_public_endpoint,
)
from src.core.workspace_storage import atomic_write_text, workspace_write_lock
from src.tools.file_ops import WorkspacePermissionError, resolve_path, truncate_text


WEB_CHAR_LIMIT = 8000
DEFAULT_PAGE_LOAD_TIMEOUT = 30
MAX_PAGE_LOAD_TIMEOUT = 120
_ASYNC_SCRIPT_STATUS_KEY = "__xagent_script_status__"


@dataclass
class BrowserSession:
    id: str
    url: str
    title: str
    type: str
    active: bool
    connected_at: float
    disconnected_at: float | None = None


class BrowserDriver(Protocol):
    def close(self) -> None:
        ...

    def list_sessions(self) -> list[BrowserSession]:
        ...

    def scan(
        self,
        url: str | None = None,
        session_id: str | None = None,
        mode: str = "summary",
        max_chars: int = WEB_CHAR_LIMIT,
        tab_index: int | None = None,
    ) -> dict[str, Any]:
        ...

    def execute_js(
        self,
        script: str,
        session_id: str | None = None,
        timeout: int = 10,
        save_to_file: str | None = None,
        cwd: str | None = None,
        await_navigation: bool = True,
    ) -> dict[str, Any]:
        ...


class SeleniumBrowserDriver:
    def __init__(self) -> None:
        self._driver: Any = None
        self._lock = threading.RLock()
        self._connected_at: dict[str, float] = {}
        self._closed = False
        self._proxy: PublicEgressProxy | None = None

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            driver = self._driver
            proxy = self._proxy
            self._driver = None
            self._proxy = None
            self._connected_at.clear()
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass
            if proxy is not None:
                proxy.close()

    def list_sessions(self) -> list[BrowserSession]:
        with self._lock:
            driver = self._get_driver()
            return self._list_sessions(driver)

    def scan(
        self,
        url: str | None = None,
        session_id: str | None = None,
        mode: str = "summary",
        max_chars: int = WEB_CHAR_LIMIT,
        tab_index: int | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            try:
                driver = self._get_driver()
                self._select_session(driver, session_id=session_id, tab_index=tab_index)
                if url:
                    _validate_navigation_url(url)
                    driver.set_page_load_timeout(_page_load_timeout())
                    driver.get(url)
                    _validate_navigation_url(driver.current_url)
                else:
                    _validate_existing_page_url(driver.current_url)

                content, elements = self._scan_content(driver, mode)
                sessions = self._list_sessions(driver)
                current_session_id = driver.current_window_handle
                truncated = len(content) > max_chars
                return {
                    "status": "OK",
                    "sessions": [asdict(session) for session in sessions],
                    "current_session_id": current_session_id,
                    "page": {
                        "url": driver.current_url,
                        "title": driver.title,
                        "mode": mode,
                        "content": truncate_text(content, max_chars),
                        "truncated": truncated,
                        "elements": elements,
                    },
                    # Backward-compatible top-level fields.
                    "url": driver.current_url,
                    "title": driver.title,
                    "mode": mode,
                    "content": truncate_text(content, max_chars),
                }
            except ImportError:
                return self._error("BRIDGE_DISCONNECTED", "selenium is not installed")
            except UnsafeNavigationError as exc:
                return self._error("SSRF_BLOCKED", str(exc))
            except Exception as exc:
                if _is_script_timeout(exc):
                    return self._error(
                        "TIMEOUT",
                        f"page navigation exceeded {_page_load_timeout()} seconds",
                        status="TIMEOUT",
                    )
                return self._error("JS_ERROR", str(exc))

    def execute_js(
        self,
        script: str,
        session_id: str | None = None,
        timeout: int = 10,
        save_to_file: str | None = None,
        cwd: str | None = None,
        await_navigation: bool = True,
    ) -> dict[str, Any]:
        exec_id = f"exec-{uuid.uuid4().hex}"
        start = time.time()
        with self._lock:
            try:
                if timeout <= 0:
                    return self._error(
                        "INVALID_ARGUMENT",
                        "timeout must be greater than zero",
                        exec_id=exec_id,
                    )
                driver = self._get_driver()
                self._select_session(driver, session_id=session_id, tab_index=None)
                before_url = driver.current_url
                _validate_existing_page_url(before_url)
                before_handles = set(driver.window_handles)
                current_session_id = driver.current_window_handle

                driver.set_script_timeout(timeout)
                payload = driver.execute_async_script(_wrap_async_script(script))
                if not isinstance(payload, dict) or _ASYNC_SCRIPT_STATUS_KEY not in payload:
                    return self._error(
                        "JS_ERROR",
                        "browser returned an invalid JavaScript result",
                        exec_id=exec_id,
                        ack=True,
                        result_received=True,
                    )
                if payload[_ASYNC_SCRIPT_STATUS_KEY] != "ok":
                    return self._error(
                        "JS_ERROR",
                        str(payload.get("error") or "JavaScript execution failed"),
                        exec_id=exec_id,
                        ack=True,
                        result_received=True,
                    )

                result_str = serialize_result(payload.get("value"))
                if await_navigation and driver.current_url != before_url:
                    _validate_existing_page_url(driver.current_url)
                after_handles = set(driver.window_handles)
                new_tabs = sorted(after_handles - before_handles)
                diagnostics = {
                    "navigation": await_navigation and driver.current_url != before_url,
                    "reloaded": False,
                    "new_tabs": new_tabs,
                    "duration_ms": int((time.time() - start) * 1000),
                }

                response: dict[str, Any] = {
                    "status": "OK",
                    "session_id": current_session_id,
                    "exec_id": exec_id,
                    "ack": True,
                    "result_received": True,
                    "diagnostics": diagnostics,
                }
                if save_to_file:
                    response.update(save_result(result_str, save_to_file, cwd))
                else:
                    response["result"] = truncate_text(result_str, WEB_CHAR_LIMIT)
                    response["truncated"] = len(result_str) > WEB_CHAR_LIMIT
                return response
            except ImportError:
                return self._error("BRIDGE_DISCONNECTED", "selenium is not installed", exec_id=exec_id)
            except UnsafeNavigationError as exc:
                return self._error(
                    "SSRF_BLOCKED",
                    str(exc),
                    exec_id=exec_id,
                    ack=True,
                    result_received=False,
                )
            except Exception as exc:
                if _is_script_timeout(exc):
                    return self._error(
                        "TIMEOUT",
                        f"JavaScript execution exceeded {timeout} seconds",
                        exec_id=exec_id,
                        ack=True,
                        result_received=False,
                        status="TIMEOUT",
                    )
                return self._error("JS_ERROR", str(exc), exec_id=exec_id, ack=True, result_received=True)

    def _get_driver(self) -> Any:
        with self._lock:
            if self._closed:
                raise RuntimeError("browser driver is closed")
            if self._driver is not None:
                try:
                    self._driver.current_url
                    return self._driver
                except Exception:
                    self._driver = None
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options

            if self._proxy is None:
                self._proxy = PublicEgressProxy().start()
            options = Options()
            for argument in _chrome_launch_arguments(self._proxy.url):
                options.add_argument(argument)
            try:
                self._driver = webdriver.Chrome(options=options)
            except Exception:
                self._proxy.close()
                self._proxy = None
                raise
            self._driver.set_page_load_timeout(_page_load_timeout())
            return self._driver

    def _select_session(self, driver: Any, session_id: str | None, tab_index: int | None) -> None:
        handles = driver.window_handles
        if tab_index is not None:
            if tab_index < 0 or tab_index >= len(handles):
                raise ValueError(f"tab_index {tab_index} out of range (0-{len(handles) - 1})")
            driver.switch_to.window(handles[tab_index])
            return
        if session_id:
            if session_id not in handles:
                raise ValueError(f"session_id not found: {session_id}")
            driver.switch_to.window(session_id)

    def _list_sessions(self, driver: Any) -> list[BrowserSession]:
        sessions: list[BrowserSession] = []
        handles = driver.window_handles
        active = driver.current_window_handle if handles else ""
        original = active
        for handle in handles:
            if handle not in self._connected_at:
                self._connected_at[handle] = time.time()
            try:
                driver.switch_to.window(handle)
                url = driver.current_url
                title = driver.title
            except Exception:
                url = ""
                title = ""
            sessions.append(
                BrowserSession(
                    id=handle,
                    url=url,
                    title=title,
                    type="selenium",
                    active=handle == active,
                    connected_at=self._connected_at[handle],
                )
            )
        if original:
            driver.switch_to.window(original)
        return sessions

    def _scan_content(self, driver: Any, mode: str) -> tuple[str, list[dict[str, Any]]]:
        elements = extract_elements(driver)
        if mode == "text":
            return extract_text(driver), elements
        if mode == "elements":
            return serialize_result(elements), elements
        if mode == "simplified_html":
            return simplify_html(driver), elements
        summary = extract_text(driver)
        element_lines = [
            f"[{item['i']}] {item['tag']} text={item.get('text', '')!r} href={item.get('href', '')!r}"
            for item in elements[:80]
        ]
        if element_lines:
            summary = f"{summary}\n\n[interactive_elements]\n" + "\n".join(element_lines)
        return summary.strip(), elements

    @staticmethod
    def _error(
        error_type: str,
        message: str,
        exec_id: str | None = None,
        ack: bool = False,
        result_received: bool = False,
        status: str = "ERROR",
    ) -> dict[str, Any]:
        data: dict[str, Any] = {
            "status": status,
            "error_type": error_type,
            "message": message,
            "error": message,
            "ack": ack,
            "result_received": result_received,
            "sessions": [],
        }
        if exec_id:
            data["exec_id"] = exec_id
        return data


class UnsafeNavigationError(ValueError):
    pass


def _page_load_timeout() -> int:
    raw = os.environ.get("XAGENT_CHROME_PAGE_LOAD_TIMEOUT", str(DEFAULT_PAGE_LOAD_TIMEOUT))
    try:
        timeout = int(raw)
    except ValueError:
        return DEFAULT_PAGE_LOAD_TIMEOUT
    return max(1, min(timeout, MAX_PAGE_LOAD_TIMEOUT))


def _chrome_launch_arguments(proxy_url: str | None = None) -> list[str]:
    arguments = ["--headless=new", "--disable-dev-shm-usage"]
    if proxy_url:
        arguments.extend(
            [
                f"--proxy-server={proxy_url}",
                "--proxy-bypass-list=<-loopback>",
                "--disable-quic",
                "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
            ]
        )
    if os.environ.get("XAGENT_CHROME_NO_SANDBOX", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }:
        arguments.append("--no-sandbox")
    return arguments


def _validate_navigation_url(url: str) -> None:
    try:
        parsed = urllib.parse.urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise UnsafeNavigationError(f"invalid navigation URL: {exc}") from exc
    if parsed.scheme.lower() not in {"http", "https"}:
        raise UnsafeNavigationError("navigation URL must use http or https")
    if parsed.username is not None or parsed.password is not None:
        raise UnsafeNavigationError("navigation URL must not contain credentials")
    hostname = parsed.hostname
    if not hostname:
        raise UnsafeNavigationError("navigation URL must include a hostname")

    try:
        resolve_public_endpoint(
            hostname,
            port or (443 if parsed.scheme.lower() == "https" else 80),
        )
    except UnsafeNetworkTargetError as exc:
        raise UnsafeNavigationError(str(exc)) from exc


def _validate_existing_page_url(url: str) -> None:
    normalized = str(url or "").strip()
    if normalized in {"", "about:blank", "data:,"}:
        return
    _validate_navigation_url(normalized)


def _wrap_async_script(script: str) -> str:
    return f"""
const __xagentDone = arguments[arguments.length - 1];
(async function () {{
  try {{
    const __xagentValue = await (async function () {{
{script}
    }}).call(window);
    __xagentDone({{"{_ASYNC_SCRIPT_STATUS_KEY}": "ok", "value": __xagentValue}});
  }} catch (__xagentError) {{
    const __xagentMessage = __xagentError && (__xagentError.stack || __xagentError.message)
      ? (__xagentError.stack || __xagentError.message)
      : String(__xagentError);
    __xagentDone({{"{_ASYNC_SCRIPT_STATUS_KEY}": "error", "error": __xagentMessage}});
  }}
}})();
"""


def _is_script_timeout(exc: Exception) -> bool:
    try:
        from selenium.common.exceptions import TimeoutException
    except ImportError:
        return False
    return isinstance(exc, TimeoutException)


def extract_text(driver: Any) -> str:
    body = driver.find_element("tag name", "body")
    return body.text


def simplify_html(driver: Any) -> str:
    body = driver.find_element("tag name", "body")
    html = body.get_attribute("innerHTML")
    html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<svg[^>]*>.*?</svg>", "", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<[^>]+>", lambda match: tag_to_placeholder(match), html)
    html = re.sub(r"\n{3,}", "\n\n", html)
    return html.strip()


def tag_to_placeholder(match: re.Match[str]) -> str:
    tag = match.group(0)
    for closing in ("</a>", "</button>", "</h1>", "</h2>", "</h3>", "</h4>", "</li>", "</option>", "</tr>"):
        if tag == closing:
            return ""
    if tag.startswith("</"):
        return "\n"
    if tag.startswith("<br"):
        return "\n"
    if tag.startswith("<hr"):
        return "\n---\n"
    if tag.startswith("<img"):
        alt_match = re.search(r'alt="([^"]*)"', tag)
        alt = alt_match.group(1) if alt_match else "image"
        return f"[img: {alt}]"
    if tag.startswith("<a "):
        href_match = re.search(r'href="([^"]*)"', tag)
        href = href_match.group(1) if href_match else ""
        return f"[link: {href}] "
    return ""


def extract_elements(driver: Any) -> list[dict[str, Any]]:
    script = """
return [...document.querySelectorAll('button,input,select,textarea,a,[role="button"]')]
  .slice(0, 200)
  .map((el, i) => ({
    i,
    tag: el.tagName,
    text: (el.innerText || el.value || el.placeholder || el.getAttribute('aria-label') || '').trim().slice(0, 160),
    href: el.href || '',
    name: el.name || '',
    id: el.id || '',
    type: el.type || '',
    visible: !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length)
  }));
"""
    result = driver.execute_script(script)
    return result if isinstance(result, list) else []


def serialize_result(result: Any) -> str:
    if result is None:
        return "null"
    if isinstance(result, (str, int, float, bool)):
        return str(result)
    try:
        return json.dumps(result, ensure_ascii=False, indent=2)
    except (TypeError, ValueError):
        return str(result)


def save_result(result_str: str, save_to_file: str, cwd: str | None) -> dict[str, Any]:
    path: Path | None = None
    try:
        path = resolve_path(save_to_file, cwd, operation="write")
        with workspace_write_lock(Path(cwd or Path.cwd()).resolve()):
            atomic_write_text(path, result_str)
    except WorkspacePermissionError as exc:
        return exc.to_result()
    except OSError as exc:
        return {
            "status": "ERROR",
            "error": f"failed to save browser result: {exc}",
            "path": str(path or save_to_file),
        }
    assert path is not None
    return {
        "saved_to": str(path),
        "bytes": len(result_str.encode("utf-8")),
        "summary": summarize_saved_result(result_str),
    }


def summarize_saved_result(result_str: str) -> dict[str, Any]:
    try:
        value = json.loads(result_str)
    except json.JSONDecodeError:
        return {"type": "text", "chars": len(result_str), "preview": truncate_text(result_str, 400)}
    if isinstance(value, list):
        return {"type": "array", "items": len(value), "preview": truncate_text(result_str, 400)}
    if isinstance(value, dict):
        return {"type": "object", "keys": list(value.keys())[:20], "preview": truncate_text(result_str, 400)}
    return {"type": type(value).__name__, "preview": truncate_text(result_str, 400)}


def load_bridge_driver_from_env() -> BrowserDriver | None:
    # 预留桥接入口：后续可按环境变量返回 WebSocket/Long-Poll driver。
    return None
