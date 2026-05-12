"""事件流观测：结构化事件 + 可插拔 Sink。

设计原则：
- Event 字段扁平，不嵌套；data 字典只放长度/布尔等小载荷，不塞完整 prompt
- Sink 实现内部吞异常，永不上抛到主循环
- 默认 NullSink 零开销，保证库调用/测试路径无感
"""
from __future__ import annotations

import json
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@dataclass
class Event:
    session_id: str
    turn: int
    kind: str
    name: str
    ts: float = field(default_factory=time.time)
    duration_ms: float | None = None
    data: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class EventSink(Protocol):
    def emit(self, event: Event) -> None: ...
    def close(self) -> None: ...


class NullSink:
    """零开销默认 sink。所有方法空操作。"""

    def emit(self, event: Event) -> None:  # noqa: ARG002
        return None

    def close(self) -> None:
        return None


class StderrSink:
    """面向人眼的调试 sink。打印单行摘要。"""

    def emit(self, event: Event) -> None:
        try:
            dur = f" +{event.duration_ms:.1f}ms" if event.duration_ms is not None else ""
            data_preview = json.dumps(event.data, ensure_ascii=False) if event.data else ""
            print(
                f"[telemetry] {event.kind} {event.name}{dur} {data_preview}",
                file=sys.stderr,
            )
        except Exception:  # noqa: BLE001
            # Sink 永不阻断主循环
            pass

    def close(self) -> None:
        return None


class JsonlSink:
    """按 session_id 切分的 JSONL 追加写 sink。线程安全。"""

    def __init__(self, path: str | Path) -> None:
        self.root = Path(path)
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._handles: dict[str, Any] = {}

    def _handle(self, session_id: str):
        handle = self._handles.get(session_id)
        if handle is None:
            file_path = self.root / f"{session_id or 'unknown'}.jsonl"
            handle = open(file_path, "a", buffering=1, encoding="utf-8")  # noqa: SIM115
            self._handles[session_id] = handle
        return handle

    def emit(self, event: Event) -> None:
        try:
            line = json.dumps(asdict(event), ensure_ascii=False)
            with self._lock:
                handle = self._handle(event.session_id)
                handle.write(line + "\n")
        except Exception:  # noqa: BLE001
            pass

    def close(self) -> None:
        with self._lock:
            for handle in self._handles.values():
                try:
                    handle.close()
                except Exception:  # noqa: BLE001
                    pass
            self._handles.clear()


class MultiSink:
    """广播到多个 sink；任一 sink 抛异常不影响其他。"""

    def __init__(self, *sinks: EventSink) -> None:
        self.sinks: tuple[EventSink, ...] = sinks

    def emit(self, event: Event) -> None:
        for sink in self.sinks:
            try:
                sink.emit(event)
            except Exception:  # noqa: BLE001
                pass

    def close(self) -> None:
        for sink in self.sinks:
            try:
                sink.close()
            except Exception:  # noqa: BLE001
                pass
