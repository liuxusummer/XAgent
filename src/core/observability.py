"""Platform observability adapters built on top of telemetry.Event.

The agent loop emits a small, privacy-preserving Event stream. This module keeps
platform-specific logic out of the loop by adapting those Events to exporters such
as Langfuse. All adapters are best-effort: exporter failures must never interrupt
agent execution.
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable

from src.core.telemetry import Event, EventSink


@runtime_checkable
class EventExporter(Protocol):
    """Platform-neutral exporter contract for structured agent events."""

    def export(self, event: Event) -> None: ...
    def close(self) -> None: ...


class PlatformSink:
    """EventSink wrapper that isolates exporter exceptions from the agent loop."""

    def __init__(self, exporter: EventExporter) -> None:
        self.exporter = exporter

    def emit(self, event: Event) -> None:
        try:
            self.exporter.export(event)
        except Exception:  # noqa: BLE001
            pass

    def close(self) -> None:
        try:
            self.exporter.close()
        except Exception:  # noqa: BLE001
            pass


class LangfuseExporter:
    """Best-effort Langfuse exporter for XAgent execution trajectories.

    The exporter intentionally uses light dynamic dispatch instead of importing a
    hard SDK version into core code. It supports the classic low-level Langfuse
    client shape (`trace`, `span`, `generation`, `event`, `flush`) and degrades to
    no-op for unsupported methods.
    """

    def __init__(self, client: Any | None = None, service_name: str = "xagent") -> None:
        self.client = client if client is not None else self._create_client_from_env()
        self.service_name = service_name
        self._traces: dict[str, Any] = {}
        self._turn_spans: dict[tuple[str, int], Any] = {}
        self._tool_spans: dict[tuple[str, int, str], Any] = {}
        self._trace_ids: dict[str, str] = {}

    @staticmethod
    def _create_client_from_env() -> Any:
        try:
            from langfuse import Langfuse  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("langfuse package is not installed") from exc

        kwargs: dict[str, str] = {}
        public_key = os.environ.get("XAGENT_LANGFUSE_PUBLIC_KEY") or os.environ.get(
            "LANGFUSE_PUBLIC_KEY"
        )
        secret_key = os.environ.get("XAGENT_LANGFUSE_SECRET_KEY") or os.environ.get(
            "LANGFUSE_SECRET_KEY"
        )
        host = os.environ.get("XAGENT_LANGFUSE_HOST") or os.environ.get("LANGFUSE_HOST")
        if public_key:
            kwargs["public_key"] = public_key
        if secret_key:
            kwargs["secret_key"] = secret_key
        if host:
            kwargs["host"] = host
        if not public_key or not secret_key:
            raise RuntimeError("langfuse public_key and secret_key are required")
        return Langfuse(**kwargs)

    def export(self, event: Event) -> None:
        if callable(getattr(self.client, "create_event", None)):
            self._export_event_api(event)
            return
        if event.kind == "run_start":
            self._handle_run_start(event)
        elif event.kind == "run_end":
            self._handle_run_end(event)
        elif event.kind == "turn_start":
            self._handle_turn_start(event)
        elif event.kind == "turn_end":
            self._handle_turn_end(event)
        elif event.kind == "llm_end":
            self._handle_llm_end(event)
        elif event.kind == "tool_start":
            self._handle_tool_start(event)
        elif event.kind == "tool_end":
            self._handle_tool_end(event)
        elif event.kind == "hook_inject":
            self._handle_hook_inject(event)
        else:
            self._event(event.session_id, event.kind, event)

    def close(self) -> None:
        flush = getattr(self.client, "flush", None)
        if callable(flush):
            flush()
        shutdown = getattr(self.client, "shutdown", None)
        if callable(shutdown):
            shutdown()

    def _export_event_api(self, event: Event) -> None:
        create_event = getattr(self.client, "create_event", None)
        if not callable(create_event):
            return
        metadata = self._metadata(event)
        kwargs: dict[str, Any] = {
            "trace_context": {"trace_id": self._trace_id(event.session_id)},
            "name": f"{self.service_name}.{event.kind}",
            "metadata": metadata,
        }
        if event.kind == "run_start":
            kwargs["input"] = {"query_len": event.data.get("query_len", 0)}
        elif event.kind == "run_end":
            kwargs["output"] = {"exit_reason": event.name}
        create_event(**kwargs)

    def _trace_id(self, session_id: str) -> str:
        if session_id in self._trace_ids:
            return self._trace_ids[session_id]
        create_trace_id = getattr(self.client, "create_trace_id", None)
        if callable(create_trace_id):
            trace_id = create_trace_id(seed=session_id)
        else:
            trace_id = hashlib.md5(session_id.encode("utf-8")).hexdigest()  # noqa: S324
        self._trace_ids[session_id] = trace_id
        return trace_id

    def _trace(self, session_id: str) -> Any | None:
        if session_id in self._traces:
            return self._traces[session_id]
        trace_fn = getattr(self.client, "trace", None)
        if not callable(trace_fn):
            return None
        try:
            trace = trace_fn(id=session_id, name=f"{self.service_name}.run")
        except TypeError:
            trace = trace_fn(name=f"{self.service_name}.run")
        self._traces[session_id] = trace
        return trace

    def _handle_run_start(self, event: Event) -> None:
        trace_fn = getattr(self.client, "trace", None)
        if not callable(trace_fn):
            return
        metadata = self._metadata(event)
        metadata["query_len"] = event.data.get("query_len", 0)
        metadata["max_turns"] = event.data.get("max_turns", 0)
        try:
            trace = trace_fn(
                id=event.session_id,
                name=f"{self.service_name}.run",
                input={"query_len": event.data.get("query_len", 0)},
                metadata=metadata,
            )
        except TypeError:
            trace = trace_fn(id=event.session_id, name=f"{self.service_name}.run")
            self._update(trace, metadata=metadata)
        self._traces[event.session_id] = trace

    def _handle_run_end(self, event: Event) -> None:
        trace = self._trace(event.session_id)
        if trace is None:
            return
        metadata = self._metadata(event)
        metadata["exit_reason"] = event.name
        metadata["turns"] = event.data.get("turns", event.turn)
        self._update(trace, output={"exit_reason": event.name}, metadata=metadata)

    def _handle_turn_start(self, event: Event) -> None:
        span = self._span(
            event.session_id,
            name=f"turn.{event.turn}",
            event=event,
            metadata={"turn": event.turn},
        )
        if span is not None:
            self._turn_spans[(event.session_id, event.turn)] = span

    def _handle_turn_end(self, event: Event) -> None:
        key = (event.session_id, event.turn)
        span = self._turn_spans.pop(key, None)
        metadata = self._metadata(event)
        metadata["tool_count"] = event.data.get("tool_count", 0)
        if span is not None:
            self._end(span, event, metadata=metadata)
        else:
            self._event(event.session_id, "turn_end", event, metadata)

    def _handle_llm_end(self, event: Event) -> None:
        metadata = self._metadata(event)
        metadata.update(
            {
                "stop_reason": event.name,
                "has_tool_calls": event.data.get("has_tool_calls", False),
                "content_len": event.data.get("content_len", 0),
                "tool_call_count": event.data.get("tool_call_count", 0),
            }
        )
        trace = self._trace(event.session_id)
        if trace is None:
            return
        generation_fn = getattr(trace, "generation", None)
        if callable(generation_fn):
            self._call_observation(
                generation_fn,
                name="llm",
                event=event,
                metadata=metadata,
            )
        else:
            self._event(event.session_id, "llm_end", event, metadata)

    def _handle_tool_start(self, event: Event) -> None:
        span = self._span(
            event.session_id,
            name=f"tool.{event.name}",
            event=event,
            metadata={"tool_name": event.name, "args_len": event.data.get("args_len", 0)},
        )
        if span is not None:
            self._tool_spans[(event.session_id, event.turn, event.name)] = span

    def _handle_tool_end(self, event: Event) -> None:
        key = (event.session_id, event.turn, event.name)
        span = self._tool_spans.pop(key, None)
        metadata = self._metadata(event)
        metadata.update(
            {
                "tool_name": event.name,
                "should_exit": event.data.get("should_exit", False),
                "next_prompt_len": event.data.get("next_prompt_len", 0),
                "flags": event.data.get("flags", []),
                "status": event.data.get("status"),
            }
        )
        if span is not None:
            self._end(span, event, metadata=metadata)
        else:
            self._event(event.session_id, f"tool.{event.name}", event, metadata)

    def _handle_hook_inject(self, event: Event) -> None:
        self._event(
            event.session_id,
            f"hook.{event.name}",
            event,
            {"hook_name": event.name, "prompt_len": event.data.get("prompt_len", 0)},
        )

    def _span(
        self,
        session_id: str,
        name: str,
        event: Event,
        metadata: dict[str, Any] | None = None,
    ) -> Any | None:
        trace = self._trace(session_id)
        if trace is None:
            return None
        span_fn = getattr(trace, "span", None)
        if not callable(span_fn):
            return None
        return self._call_observation(span_fn, name=name, event=event, metadata=metadata)

    def _event(
        self,
        session_id: str,
        name: str,
        event: Event,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        trace = self._trace(session_id)
        if trace is None:
            return
        event_fn = getattr(trace, "event", None)
        if not callable(event_fn):
            return
        self._call_observation(event_fn, name=name, event=event, metadata=metadata)

    def _call_observation(
        self,
        fn: Any,
        name: str,
        event: Event,
        metadata: dict[str, Any] | None = None,
    ) -> Any | None:
        kwargs = {
            "name": name,
            "start_time": self._datetime(event.ts, event.duration_ms),
            "end_time": self._datetime(event.ts),
            "metadata": metadata or self._metadata(event),
        }
        try:
            return fn(**kwargs)
        except TypeError:
            kwargs.pop("end_time", None)
            try:
                return fn(**kwargs)
            except TypeError:
                return fn(name=name, metadata=metadata or self._metadata(event))

    def _end(self, observation: Any, event: Event, metadata: dict[str, Any]) -> None:
        end_fn = getattr(observation, "end", None)
        if callable(end_fn):
            try:
                end_fn(end_time=self._datetime(event.ts), metadata=metadata)
                return
            except TypeError:
                try:
                    end_fn(metadata=metadata)
                    return
                except TypeError:
                    end_fn()
                    return
        self._update(observation, metadata=metadata)

    @staticmethod
    def _update(target: Any, **kwargs: Any) -> None:
        update = getattr(target, "update", None)
        if callable(update):
            try:
                update(**kwargs)
            except TypeError:
                update(kwargs)

    @staticmethod
    def _datetime(ts: float, duration_ms: float | None = None) -> datetime:
        if duration_ms is not None:
            ts = ts - (duration_ms / 1000)
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    def _metadata(self, event: Event) -> dict[str, Any]:
        # run_start.name is a human-readable query preview for local JSONL/replay.
        # Do not forward it to remote platforms by default to preserve the
        # "lengths/booleans/enums only" telemetry contract.
        name = "" if event.kind == "run_start" else event.name
        return {
            "service": self.service_name,
            "schema_version": "1.0",
            "session_id": event.session_id,
            "turn": event.turn,
            "kind": event.kind,
            "name": name,
            "duration_ms": event.duration_ms,
            **event.data,
        }


def build_langfuse_sink_from_env() -> EventSink | None:
    return build_langfuse_sink()


def build_langfuse_sink(config: dict[str, Any] | None = None) -> EventSink | None:
    """Build a Langfuse-backed sink from env vars with optional config fallback."""
    config = config or {}
    backend = (os.environ.get("XAGENT_OBS_BACKEND", "") or str(config.get("backend", ""))).strip().lower()
    enabled = (os.environ.get("XAGENT_LANGFUSE_ENABLED", "") or _bool_to_env(config.get("enabled"))).strip() == "1"
    if backend != "langfuse" and not enabled:
        return None
    service_name = (os.environ.get("XAGENT_OBS_SERVICE", "") or str(config.get("service", "xagent"))).strip() or "xagent"

    langfuse_cfg = config.get("langfuse", {})
    if not isinstance(langfuse_cfg, dict):
        langfuse_cfg = {}

    public_key = os.environ.get("XAGENT_LANGFUSE_PUBLIC_KEY") or os.environ.get("LANGFUSE_PUBLIC_KEY") or str(langfuse_cfg.get("public_key", ""))
    secret_key = os.environ.get("XAGENT_LANGFUSE_SECRET_KEY") or os.environ.get("LANGFUSE_SECRET_KEY") or str(langfuse_cfg.get("secret_key", ""))
    host = os.environ.get("XAGENT_LANGFUSE_HOST") or os.environ.get("LANGFUSE_HOST") or str(langfuse_cfg.get("host", ""))

    try:
        return PlatformSink(
            LangfuseExporter(
                client=LangfuseExporter._create_client_from_env() if not config else _build_langfuse_client(public_key, secret_key, host),
                service_name=service_name,
            )
        )
    except Exception:  # noqa: BLE001
        # Missing optional dependency or bad SDK configuration must not break startup.
        return None


def _build_langfuse_client(public_key: str, secret_key: str, host: str) -> Any:
    try:
        from langfuse import Langfuse  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("langfuse package is not installed") from exc

    if not public_key or not secret_key:
        raise RuntimeError("langfuse public_key and secret_key are required")

    kwargs: dict[str, str] = {
        "public_key": public_key,
        "secret_key": secret_key,
    }
    if host:
        kwargs["host"] = host
    return Langfuse(**kwargs)


def _bool_to_env(value: Any) -> str:
    if value is True:
        return "1"
    if value is False:
        return "0"
    return ""
