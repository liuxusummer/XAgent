"""观测事件 JSONL 读取器（离线消费端）。

契约：
- 宽 schema：直接返回 dict，不构造 dataclass
- 破损容忍：空行、解析失败静默跳过（可选 stderr 警告）
- 仅依赖标准库
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Iterable, Iterator


def iter_events(path: str | Path, warn: bool = False) -> Iterator[dict[str, Any]]:
    """逐行流式读取单个 JSONL 文件。

    - 空行跳过
    - JSON 解析失败跳过；warn=True 时到 stderr 提示行号
    - 路径不存在抛 FileNotFoundError（破损容忍是针对内容，不是路径）
    """
    p = Path(path)
    with p.open("r", encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                if warn:
                    print(f"[reader] skip broken line {p}:{lineno}", file=sys.stderr)
                continue


def load_events(path: str | Path, warn: bool = False) -> list[dict[str, Any]]:
    """全量读取一个 JSONL 文件为列表。"""
    return list(iter_events(path, warn=warn))


def load_dir(log_dir: str | Path, warn: bool = False) -> list[dict[str, Any]]:
    """读取目录下所有 .jsonl 文件；事件按文件名排序后拼接。

    不存在的目录抛 FileNotFoundError；空目录返回 []。
    """
    root = Path(log_dir)
    if not root.exists():
        raise FileNotFoundError(str(root))
    events: list[dict[str, Any]] = []
    for file_path in sorted(root.glob("*.jsonl")):
        events.extend(load_events(file_path, warn=warn))
    return events


def group_by_session(events: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """按 session_id 分组，保持入场顺序。"""
    groups: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        sid = event.get("session_id", "")
        groups.setdefault(sid, []).append(event)
    return groups
