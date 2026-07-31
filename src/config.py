from __future__ import annotations

import importlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.core.llm import (
    BaseSession,
    ClaudeNativeSession,
    ClaudeTextSession,
    NativeToolClient,
    OpenAINativeSession,
    OpenAITextSession,
    ToolClient,
)


@dataclass
class SessionConfig:
    name: str
    apikey: str = field(repr=False)
    apibase: str = field(repr=False)
    model: str
    session_type: str = ""
    temperature: float | None = None
    max_tokens: int = 4096
    timeout: int = 120
    max_retries: int = 2
    group: str = ""
    extra: dict[str, Any] = field(default_factory=dict, repr=False)


def load_config(config_path: str | None = None) -> dict[str, SessionConfig]:
    raw = _load_raw_config(config_path)
    if not raw:
        return {}
    return {name: _parse_session_config(name, cfg) for name, cfg in raw.items()}


def _load_raw_config(config_path: str | None = None) -> dict[str, Any]:
    if config_path:
        path = Path(config_path)
        if path.suffix == ".json":
            return json.loads(path.read_text(encoding="utf-8"))
        return _load_mykey_module(config_path)

    for candidate in ("config.json", "mykey", "mykey.json"):
        if candidate == "mykey":
            result = _load_mykey_module("mykey")
            if result:
                return result
        else:
            path = Path(candidate)
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8"))
    return {}


def _load_mykey_module(module_name: str) -> dict[str, Any]:
    try:
        mod = importlib.import_module(module_name)
        return {k: v for k, v in vars(mod).items() if not k.startswith("_") and isinstance(v, dict)}
    except ImportError:
        return {}


def _parse_session_config(name: str, cfg: dict[str, Any]) -> SessionConfig:
    name_lower = name.lower()
    session_type = _route_session_type(name_lower, cfg)
    apibase = _normalize_apibase(cfg.get("apibase", ""), session_type)

    return SessionConfig(
        name=name,
        apikey=cfg.get("apikey", ""),
        apibase=apibase,
        model=cfg.get("model", ""),
        session_type=session_type,
        temperature=cfg.get("temperature"),
        max_tokens=cfg.get("max_tokens", 4096),
        timeout=cfg.get("timeout", 120),
        max_retries=cfg.get("max_retries", 2),
        group=cfg.get("group", ""),
        extra={k: v for k, v in cfg.items() if k not in {
            "apikey", "apibase", "model", "temperature", "max_tokens",
            "timeout", "max_retries", "group",
        }},
    )


def _route_session_type(name_lower: str, cfg: dict[str, Any]) -> str:
    has_native = "native" in name_lower
    has_claude = "claude" in name_lower
    has_openai = "openai" in name_lower or "oai" in name_lower
    has_mixin = "mixin" in name_lower

    if has_mixin:
        return "mixin"
    if has_native and has_claude:
        return "claude_native"
    if has_native and has_openai:
        return "openai_native"
    if has_claude:
        return "claude_text"
    if has_openai:
        return "openai_text"
    return cfg.get("type", "openai_text")


def _normalize_apibase(apibase: str, session_type: str) -> str:
    if not apibase:
        return apibase
    if apibase.startswith(("http://", "https://")) and (
        "/v1/" in apibase
        or "/messages" in apibase
        or "/chat/completions" in apibase
    ):
        return apibase.rstrip("/")
    if "claude" in session_type:
        if not apibase.startswith(("http://", "https://")):
            apibase = f"https://{apibase}"
        if "/v1/" not in apibase and "/messages" not in apibase:
            apibase = apibase.rstrip("/") + "/v1/messages"
        return apibase.rstrip("/")
    if not apibase.startswith(("http://", "https://")):
        apibase = f"https://{apibase}"
    if "/v1/" not in apibase:
        apibase = apibase.rstrip("/") + "/v1/chat/completions"
    return apibase.rstrip("/")


def create_session(config: SessionConfig) -> BaseSession:
    cls_map: dict[str, type[BaseSession]] = {
        "openai_text": OpenAITextSession,
        "claude_text": ClaudeTextSession,
        "openai_native": OpenAINativeSession,
        "claude_native": ClaudeNativeSession,
    }
    session_type = config.session_type
    if session_type == "mixin":
        raise ValueError("mixin session_type is not supported by create_session")

    cls = cls_map.get(session_type, OpenAITextSession)
    session = cls(
        api_key=config.apikey,
        base_url=config.apibase,
        model=config.model,
        temperature=config.temperature,
        max_tokens=config.max_tokens,
        timeout=config.timeout,
        max_retries=config.max_retries,
    )
    if session_type == "claude_native":
        extra = config.extra
        if "thinking_type" in extra:
            session.thinking_type = extra["thinking_type"]
        if "reasoning_effort" in extra:
            session.reasoning_effort = extra["reasoning_effort"]
        if "fake_cc_system_prompt" in extra:
            session.fake_cc_system_prompt = extra["fake_cc_system_prompt"]
    return session


def create_client(config: SessionConfig) -> ToolClient | NativeToolClient:
    session = create_session(config)
    if config.session_type in ("claude_native", "openai_native"):
        return NativeToolClient(backend=session)
    return ToolClient(backend=session)
