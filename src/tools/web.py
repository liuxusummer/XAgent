from __future__ import annotations

from typing import Any

from src.tools.browser_driver import WEB_CHAR_LIMIT, BrowserDriver, SeleniumBrowserDriver, load_bridge_driver_from_env

_driver: BrowserDriver | None = None


def get_browser_driver() -> BrowserDriver:
    global _driver
    if _driver is None:
        _driver = load_bridge_driver_from_env() or SeleniumBrowserDriver()
    return _driver


def set_browser_driver(driver: BrowserDriver | None) -> None:
    global _driver
    _driver = driver


def web_scan(
    url: str | None = None,
    mode: str = "summary",
    session_id: str | None = None,
    max_chars: int = WEB_CHAR_LIMIT,
    tab_index: int | None = None,
) -> dict[str, Any]:
    return get_browser_driver().scan(
        url=url,
        session_id=session_id,
        mode=mode,
        max_chars=max_chars,
        tab_index=tab_index,
    )


def web_execute_js(
    script: str,
    session_id: str | None = None,
    timeout: int = 10,
    save_to_file: str | None = None,
    no_monitor: bool = False,
    cwd: str | None = None,
    await_navigation: bool | None = None,
) -> dict[str, Any]:
    return get_browser_driver().execute_js(
        script=script,
        session_id=session_id,
        timeout=timeout,
        save_to_file=save_to_file,
        cwd=cwd,
        await_navigation=(not no_monitor) if await_navigation is None else await_navigation,
    )
