from __future__ import annotations

from typing import Any

from src.tools.browser_driver import WEB_CHAR_LIMIT, BrowserDriver, SeleniumBrowserDriver, load_bridge_driver_from_env


def create_browser_driver() -> BrowserDriver:
    return load_bridge_driver_from_env() or SeleniumBrowserDriver()


def web_scan(
    driver: BrowserDriver,
    url: str | None = None,
    mode: str = "summary",
    session_id: str | None = None,
    max_chars: int = WEB_CHAR_LIMIT,
    tab_index: int | None = None,
) -> dict[str, Any]:
    return driver.scan(
        url=url,
        session_id=session_id,
        mode=mode,
        max_chars=max_chars,
        tab_index=tab_index,
    )


def web_execute_js(
    driver: BrowserDriver,
    script: str,
    session_id: str | None = None,
    timeout: int = 10,
    save_to_file: str | None = None,
    no_monitor: bool = False,
    cwd: str | None = None,
    await_navigation: bool | None = None,
) -> dict[str, Any]:
    return driver.execute_js(
        script=script,
        session_id=session_id,
        timeout=timeout,
        save_to_file=save_to_file,
        cwd=cwd,
        await_navigation=(not no_monitor) if await_navigation is None else await_navigation,
    )
