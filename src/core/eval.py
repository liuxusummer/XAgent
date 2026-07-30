from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from src.core.network_guard import (
    PublicEgressProxy,
    UnsafeNetworkTargetError,
    resolve_public_endpoint,
)


MAX_DATASET_BYTES = 20 * 1024 * 1024
MAX_CASES = 500
DOWNLOAD_TIMEOUT_SEC = 15
RUN_RESULT_VERSION = 1

ASSERTION_LIST_KEYS = {"contains", "contains_any", "not_contains", "exit_reason", "tool_called", "file_exists"}
ASSERTION_KEYS = ASSERTION_LIST_KEYS | {"file_contains", "max_turns", "max_duration_sec"}
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


class EvalError(ValueError):
    pass


@dataclass(frozen=True)
class EvalCase:
    id: str
    name: str
    task: str
    tags: list[str]
    assertions: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "task": self.task,
            "tags": list(self.tags),
            "assertions": dict(self.assertions),
        }


def utc_timestamp() -> float:
    return time.time()


def eval_root(
    workspace_root: str | Path,
    *,
    storage_root: str | Path | None = None,
) -> Path:
    return (
        Path(storage_root)
        if storage_root is not None
        else Path(workspace_root) / "runtime" / "eval"
    )


def datasets_root(
    workspace_root: str | Path,
    *,
    storage_root: str | Path | None = None,
) -> Path:
    return eval_root(workspace_root, storage_root=storage_root) / "datasets"


def runs_root(
    workspace_root: str | Path,
    *,
    storage_root: str | Path | None = None,
) -> Path:
    return eval_root(workspace_root, storage_root=storage_root) / "runs"


def downloads_root(
    workspace_root: str | Path,
    *,
    storage_root: str | Path | None = None,
) -> Path:
    return eval_root(workspace_root, storage_root=storage_root) / "downloads"


def validate_safe_id(value: str, kind: str = "id") -> str:
    value = str(value or "").strip()
    if not value or not SAFE_ID_RE.match(value) or value in {".", ".."}:
        raise EvalError(f"Invalid {kind}")
    return value


def sanitize_name(value: str, fallback: str = "dataset") -> str:
    raw = str(value or "").strip() or fallback
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", raw).strip(".-")
    return cleaned[:80] or fallback


def infer_format(name: str, fmt: str = "") -> str:
    normalized = str(fmt or "").strip().lower()
    if normalized:
        if normalized not in {"jsonl", "json", "csv"}:
            raise EvalError("Unsupported dataset format")
        return normalized
    suffix = Path(name).suffix.lower()
    if suffix == ".jsonl":
        return "jsonl"
    if suffix == ".csv":
        return "csv"
    return "json"


def _stable_case_id(task: str, index: int) -> str:
    digest = hashlib.sha1(f"{index}\0{task}".encode("utf-8")).hexdigest()[:10]
    return f"case-{index + 1:03d}-{digest}"


def _string_list(value: Any) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
                return _string_list(parsed)
            except json.JSONDecodeError:
                pass
        return [item.strip() for item in stripped.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _parse_assertion_cell(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return None
    if stripped[0] in "[{":
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return stripped
    return stripped


def normalize_assertions(raw: Any) -> dict[str, Any]:
    if raw in (None, ""):
        return {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise EvalError(f"Invalid assertions JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise EvalError("assertions must be an object")

    assertions: dict[str, Any] = {}
    for key, value in raw.items():
        if key not in ASSERTION_KEYS:
            continue
        if value in (None, ""):
            continue
        if key in ASSERTION_LIST_KEYS:
            assertions[key] = _string_list(value)
        elif key == "file_contains":
            assertions[key] = value
        elif key == "max_turns":
            try:
                assertions[key] = int(value)
            except (TypeError, ValueError) as exc:
                raise EvalError("max_turns must be an integer") from exc
        elif key == "max_duration_sec":
            try:
                assertions[key] = float(value)
            except (TypeError, ValueError) as exc:
                raise EvalError("max_duration_sec must be a number") from exc
    return assertions


def normalize_case(raw: dict[str, Any], index: int) -> EvalCase:
    if not isinstance(raw, dict):
        raise EvalError(f"case #{index + 1} must be an object")
    task = str(raw.get("task") or "").strip()
    if not task:
        raise EvalError(f"case #{index + 1} missing required task")
    case_id = str(raw.get("id") or "").strip() or _stable_case_id(task, index)
    case_id = sanitize_name(case_id, fallback=f"case-{index + 1:03d}")
    name = str(raw.get("name") or case_id).strip()
    tags = _string_list(raw.get("tags"))
    assertions = normalize_assertions(raw.get("assertions", {}))
    return EvalCase(id=case_id, name=name, task=task, tags=tags, assertions=assertions)


def parse_dataset(content: str, fmt: str) -> list[EvalCase]:
    fmt = infer_format("", fmt)
    if len(content.encode("utf-8")) > MAX_DATASET_BYTES:
        raise EvalError("Dataset exceeds 20MB limit")

    raw_cases: list[dict[str, Any]] = []
    if fmt == "jsonl":
        for line_no, line in enumerate(content.splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                value = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise EvalError(f"Invalid JSONL at line {line_no}: {exc}") from exc
            if not isinstance(value, dict):
                raise EvalError(f"JSONL line {line_no} must be an object")
            raw_cases.append(value)
    elif fmt == "json":
        try:
            value = json.loads(content)
        except json.JSONDecodeError as exc:
            raise EvalError(f"Invalid JSON dataset: {exc}") from exc
        if isinstance(value, dict) and isinstance(value.get("cases"), list):
            raw_cases = value["cases"]
        elif isinstance(value, list):
            raw_cases = value
        else:
            raise EvalError("JSON dataset must be an array or an object with cases")
    elif fmt == "csv":
        reader = csv.DictReader(io.StringIO(content))
        if not reader.fieldnames or "task" not in reader.fieldnames:
            raise EvalError("CSV dataset must include a task column")
        for row in reader:
            assertions: dict[str, Any] = {}
            if row.get("assertions"):
                parsed = _parse_assertion_cell(row["assertions"])
                if not isinstance(parsed, dict):
                    raise EvalError("CSV assertions column must be a JSON object")
                assertions.update(parsed)
            for key in ASSERTION_KEYS:
                if key in row and row.get(key) not in (None, ""):
                    assertions[key] = _parse_assertion_cell(row[key])
            raw_cases.append(
                {
                    "id": row.get("id", ""),
                    "name": row.get("name", ""),
                    "task": row.get("task", ""),
                    "tags": row.get("tags", ""),
                    "assertions": assertions,
                }
            )

    cases = [normalize_case(item, index) for index, item in enumerate(raw_cases)]
    if not cases:
        raise EvalError("Dataset contains no cases")
    if len(cases) > MAX_CASES:
        raise EvalError("Dataset exceeds 500 case limit")
    return cases


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)


def _write_dataset_jsonl(path: Path, cases: list[EvalCase]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for case in cases:
            f.write(json.dumps(case.to_dict(), ensure_ascii=False, sort_keys=True) + "\n")


def read_dataset_cases(
    workspace_root: str | Path,
    dataset_id: str,
    *,
    storage_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    dataset_id = validate_safe_id(dataset_id, "dataset_id")
    path = (
        datasets_root(workspace_root, storage_root=storage_root)
        / dataset_id
        / "dataset.jsonl"
    )
    if not path.is_file():
        raise EvalError("Dataset not found")
    cases: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                value = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise EvalError(f"Stored dataset is invalid at line {line_no}: {exc}") from exc
            if isinstance(value, dict):
                cases.append(value)
    return cases


def read_dataset_metadata(
    workspace_root: str | Path,
    dataset_id: str,
    *,
    storage_root: str | Path | None = None,
) -> dict[str, Any]:
    dataset_id = validate_safe_id(dataset_id, "dataset_id")
    path = (
        datasets_root(workspace_root, storage_root=storage_root)
        / dataset_id
        / "metadata.json"
    )
    if not path.is_file():
        raise EvalError("Dataset not found")
    return json.loads(path.read_text(encoding="utf-8"))


def import_dataset_content(
    workspace_root: str | Path,
    *,
    name: str,
    content: str,
    fmt: str = "",
    source: dict[str, Any] | None = None,
    storage_root: str | Path | None = None,
    owner_digest: str = "",
) -> dict[str, Any]:
    fmt = infer_format(name, fmt)
    cases = parse_dataset(content, fmt)
    now = utc_timestamp()
    base = sanitize_name(Path(name).stem or name, "dataset")
    dataset_id = f"{base}-{int(now)}-{uuid.uuid4().hex[:8]}"
    target_dir = (
        datasets_root(workspace_root, storage_root=storage_root) / dataset_id
    )
    dataset_path = target_dir / "dataset.jsonl"
    _write_dataset_jsonl(dataset_path, cases)
    metadata = {
        "id": dataset_id,
        "name": str(name or base),
        "format": fmt,
        "source": source or {"type": "content"},
        "created_at": now,
        "case_count": len(cases),
        "size_bytes": dataset_path.stat().st_size,
        "dataset_path": str(dataset_path),
        **({"owner_digest": owner_digest} if owner_digest else {}),
    }
    _write_json(target_dir / "metadata.json", metadata)
    return metadata


def import_dataset_path(
    workspace_root: str | Path,
    *,
    rel_path: str,
    name: str = "",
    fmt: str = "",
    storage_root: str | Path | None = None,
    owner_digest: str = "",
) -> dict[str, Any]:
    rel_path = str(rel_path or "").strip().replace("\\", "/")
    if not rel_path:
        raise EvalError("path is required")
    if os.path.isabs(rel_path):
        raise EvalError("Absolute paths are not allowed")
    workspace = Path(workspace_root).resolve()
    real_path = (workspace / rel_path).resolve()
    if os.path.commonpath([str(workspace), str(real_path)]) != str(workspace):
        raise EvalError("Path traversal not allowed")
    if not real_path.is_file():
        raise EvalError("Dataset file not found")
    if real_path.stat().st_size > MAX_DATASET_BYTES:
        raise EvalError("Dataset exceeds 20MB limit")
    try:
        content = real_path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise EvalError("Dataset file is not valid UTF-8 text") from exc
    return import_dataset_content(
        workspace_root,
        name=name or real_path.name,
        content=content,
        fmt=fmt or infer_format(real_path.name),
        source={"type": "path", "path": rel_path},
        storage_root=storage_root,
        owner_digest=owner_digest,
    )


def list_datasets(
    workspace_root: str | Path,
    *,
    storage_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    root = datasets_root(workspace_root, storage_root=storage_root)
    if not root.is_dir():
        return []
    datasets: list[dict[str, Any]] = []
    for entry in root.iterdir():
        if not entry.is_dir():
            continue
        metadata_path = entry / "metadata.json"
        if not metadata_path.is_file():
            continue
        try:
            datasets.append(json.loads(metadata_path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            continue
    return sorted(datasets, key=lambda item: float(item.get("created_at", 0)), reverse=True)


def list_workspace_eval_datasets(workspace_root: str | Path) -> list[dict[str, Any]]:
    root = Path(workspace_root) / "system" / "eval"
    if not root.is_dir():
        return []

    datasets: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".jsonl", ".json", ".csv"}:
            continue
        try:
            rel_path = path.relative_to(workspace_root).as_posix()
        except ValueError:
            continue
        try:
            fmt = infer_format(path.name)
            content = path.read_text(encoding="utf-8")
            cases = parse_dataset(content, fmt)
            stat = path.stat()
        except (EvalError, OSError, UnicodeError):
            continue
        datasets.append(
            {
                "id": f"workspace:{rel_path}",
                "name": path.stem,
                "format": fmt,
                "source": {"type": "workspace_path", "path": rel_path},
                "created_at": stat.st_mtime,
                "case_count": len(cases),
                "size_bytes": stat.st_size,
                "dataset_path": rel_path,
                "imported": False,
            }
        )
    return datasets


def get_dataset_detail(
    workspace_root: str | Path,
    dataset_id: str,
    *,
    storage_root: str | Path | None = None,
) -> dict[str, Any]:
    metadata = read_dataset_metadata(
        workspace_root,
        dataset_id,
        storage_root=storage_root,
    )
    return {
        **metadata,
        "cases": read_dataset_cases(
            workspace_root,
            dataset_id,
            storage_root=storage_root,
        ),
    }


def _validate_dataset_url(url: str) -> str:
    normalized = str(url or "").strip()
    try:
        parsed = urllib.parse.urlsplit(normalized)
        port = parsed.port
    except ValueError as exc:
        raise EvalError("Invalid dataset URL") from exc
    if parsed.scheme not in {"http", "https"}:
        raise EvalError("Only http/https dataset URLs are allowed")
    if not parsed.hostname:
        raise EvalError("Dataset URL must include a host")
    if parsed.username is not None or parsed.password is not None:
        raise EvalError("Dataset URL credentials are not allowed")

    try:
        resolve_public_endpoint(
            parsed.hostname,
            port or (443 if parsed.scheme == "https" else 80),
        )
    except UnsafeNetworkTargetError as exc:
        raise EvalError(str(exc)) from exc
    return normalized


class _DatasetRedirectHandler(urllib.request.HTTPRedirectHandler):
    max_redirections = 5
    max_repeats = 2

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        _validate_dataset_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class _NoBypassProxyHandler(urllib.request.ProxyHandler):
    def proxy_open(self, req, proxy, proxy_type):
        parsed_proxy = urllib.parse.urlsplit(proxy)
        if parsed_proxy.scheme not in {"http", "https"} or not parsed_proxy.netloc:
            raise EvalError("Dataset proxy URL is invalid")
        req.set_proxy(parsed_proxy.netloc, parsed_proxy.scheme or proxy_type)
        return None


def _open_dataset_url(
    request: urllib.request.Request,
    timeout: float,
    proxy_url: str | None = None,
):
    handlers: list[object] = [_DatasetRedirectHandler()]
    if proxy_url:
        handlers.insert(
            0,
            _NoBypassProxyHandler(
                {
                    "http": proxy_url,
                    "https": proxy_url,
                }
            ),
        )
    opener = urllib.request.build_opener(*handlers)
    return opener.open(request, timeout=timeout)


def _read_url_limited(url: str, timeout: float = DOWNLOAD_TIMEOUT_SEC) -> tuple[bytes, str]:
    validated_url = _validate_dataset_url(url)
    request = urllib.request.Request(validated_url, headers={"User-Agent": "XAgent-Eval/1.0"})
    try:
        with PublicEgressProxy() as proxy:
            with _open_dataset_url(request, timeout, proxy.url) as response:
                final_url = str(getattr(response, "url", validated_url))
                _validate_dataset_url(final_url)
                content_length = response.headers.get("Content-Length")
                if content_length:
                    try:
                        if int(content_length) > MAX_DATASET_BYTES:
                            raise EvalError("Download exceeds 20MB limit")
                    except ValueError:
                        pass
                chunks: list[bytes] = []
                total = 0
                while True:
                    chunk = response.read(64 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > MAX_DATASET_BYTES:
                        raise EvalError("Download exceeds 20MB limit")
                    chunks.append(chunk)
    except EvalError:
        raise
    except (OSError, urllib.error.URLError) as exc:
        raise EvalError(f"Dataset download failed: {exc}") from exc
    return b"".join(chunks), final_url


def download_dataset(
    workspace_root: str | Path,
    *,
    url: str,
    name: str = "",
    fmt: str = "",
    timeout: float = DOWNLOAD_TIMEOUT_SEC,
    storage_root: str | Path | None = None,
    owner_digest: str = "",
) -> dict[str, Any]:
    data, final_url = _read_url_limited(url, timeout=timeout)
    parsed = urllib.parse.urlparse(final_url)
    filename = sanitize_name(Path(parsed.path).name or name or "dataset")
    target = (
        downloads_root(workspace_root, storage_root=storage_root)
        / f"{int(utc_timestamp())}-{uuid.uuid4().hex[:8]}-{filename}"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    try:
        content = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EvalError("Downloaded dataset is not valid UTF-8 text") from exc
    metadata = import_dataset_content(
        workspace_root,
        name=name or filename,
        content=content,
        fmt=fmt or infer_format(filename),
        source={"type": "url", "url": url, "final_url": final_url, "download_path": str(target)},
        storage_root=storage_root,
        owner_digest=owner_digest,
    )
    return metadata


def _resolve_workspace_file(workspace_root: str | Path, rel_path: str) -> Path:
    rel_path = str(rel_path or "").strip().replace("\\", "/")
    if not rel_path or os.path.isabs(rel_path):
        raise EvalError("Invalid workspace file path")
    workspace = Path(workspace_root).resolve()
    real_path = (workspace / rel_path).resolve()
    if os.path.commonpath([str(workspace), str(real_path)]) != str(workspace):
        raise EvalError("Path traversal not allowed")
    return real_path


def _tool_names(result: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for item in result.get("tool_results", []) or []:
        if not isinstance(item, dict):
            continue
        for key in ("tool_name", "name"):
            value = str(item.get(key) or "").strip()
            if value:
                names.add(value)
    return names


def _file_contains_items(value: Any) -> list[tuple[str, list[str]]]:
    items: list[tuple[str, list[str]]] = []
    if isinstance(value, dict):
        for path, expected in value.items():
            items.append((str(path), _string_list(expected)))
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                path = str(item.get("path") or "").strip()
                expected = item.get("contains", item.get("content", item.get("text", "")))
                if path:
                    items.append((path, _string_list(expected)))
    return items


def evaluate_assertions(
    case: dict[str, Any],
    result: dict[str, Any],
    *,
    workspace_root: str | Path,
    duration_sec: float,
) -> tuple[str, list[str]]:
    assertions = normalize_assertions(case.get("assertions", {}))
    response = str(result.get("response") or "")
    failures: list[str] = []
    expected_exit = assertions.get("exit_reason", [])

    if str(result.get("exit_reason") or "") == "ERROR" and not expected_exit:
        message = response.strip() or "agent exited with ERROR"
        return "error", [message]

    for expected in assertions.get("contains", []):
        if expected not in response:
            failures.append(f"response missing expected text: {expected}")

    any_expected = assertions.get("contains_any", [])
    if any_expected and not any(expected in response for expected in any_expected):
        failures.append(f"response missing any expected text: {any_expected}")

    for forbidden in assertions.get("not_contains", []):
        if forbidden in response:
            failures.append(f"response contains forbidden text: {forbidden}")

    if expected_exit:
        actual = str(result.get("exit_reason") or "")
        if actual not in set(expected_exit):
            failures.append(f"exit_reason {actual!r} not in {expected_exit}")

    called = _tool_names(result)
    for expected_tool in assertions.get("tool_called", []):
        if expected_tool not in called:
            failures.append(f"tool not called: {expected_tool}")

    for rel_path in assertions.get("file_exists", []):
        try:
            if not _resolve_workspace_file(workspace_root, rel_path).is_file():
                failures.append(f"file does not exist: {rel_path}")
        except EvalError as exc:
            failures.append(str(exc))

    for rel_path, expected_texts in _file_contains_items(assertions.get("file_contains")):
        try:
            path = _resolve_workspace_file(workspace_root, rel_path)
            if not path.is_file():
                failures.append(f"file does not exist: {rel_path}")
                continue
            try:
                content = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                failures.append(f"file is not valid UTF-8 text: {rel_path}")
                continue
            for expected in expected_texts:
                if expected not in content:
                    failures.append(f"file {rel_path} missing expected text: {expected}")
        except (EvalError, OSError) as exc:
            failures.append(str(exc))

    if "max_turns" in assertions:
        turns = int(result.get("turns") or 0)
        if turns > int(assertions["max_turns"]):
            failures.append(f"turns {turns} exceeds max_turns {assertions['max_turns']}")

    if "max_duration_sec" in assertions and duration_sec > float(assertions["max_duration_sec"]):
        failures.append(
            f"duration {duration_sec:.2f}s exceeds max_duration_sec {assertions['max_duration_sec']}"
        )

    return ("failed" if failures else "passed"), failures


def summarize_cases(case_results: list[dict[str, Any]], total: int | None = None) -> dict[str, Any]:
    total_count = total if total is not None else len(case_results)
    passed = sum(1 for item in case_results if item.get("status") == "passed")
    failed = sum(1 for item in case_results if item.get("status") == "failed")
    error = sum(1 for item in case_results if item.get("status") == "error")
    durations = [float(item.get("duration_sec") or 0) for item in case_results]
    turns = [int(item.get("turns") or 0) for item in case_results]
    completed = passed + failed + error
    return {
        "total": total_count,
        "completed": completed,
        "passed": passed,
        "failed": failed,
        "error": error,
        "pass_rate": passed / completed if completed else 0.0,
        "avg_duration": sum(durations) / len(durations) if durations else 0.0,
        "avg_turns": sum(turns) / len(turns) if turns else 0.0,
    }


def create_eval_run(
    workspace_root: str | Path,
    *,
    workspace: str,
    dataset_id: str,
    agent: str,
    case_limit: int | None = None,
    storage_root: str | Path | None = None,
    owner_digest: str = "",
) -> dict[str, Any]:
    metadata = read_dataset_metadata(
        workspace_root,
        dataset_id,
        storage_root=storage_root,
    )
    cases = read_dataset_cases(
        workspace_root,
        dataset_id,
        storage_root=storage_root,
    )
    if case_limit is not None and case_limit > 0:
        cases = cases[:case_limit]
    now = utc_timestamp()
    run_id = f"run-{int(now)}-{uuid.uuid4().hex[:8]}"
    result = {
        "version": RUN_RESULT_VERSION,
        "id": run_id,
        "workspace": workspace,
        "dataset_id": dataset_id,
        "dataset_name": metadata.get("name", dataset_id),
        "agent": agent,
        "status": "pending",
        "created_at": now,
        "started_at": None,
        "finished_at": None,
        "case_limit": case_limit or 0,
        "summary": summarize_cases([], total=len(cases)),
        "cases": [],
        "error": "",
        **({"owner_digest": owner_digest} if owner_digest else {}),
    }
    _write_json(
        runs_root(workspace_root, storage_root=storage_root)
        / run_id
        / "result.json",
        result,
    )
    return result


def read_eval_run(
    workspace_root: str | Path,
    run_id: str,
    *,
    storage_root: str | Path | None = None,
) -> dict[str, Any]:
    run_id = validate_safe_id(run_id, "run_id")
    path = (
        runs_root(workspace_root, storage_root=storage_root)
        / run_id
        / "result.json"
    )
    if not path.is_file():
        raise EvalError("Run not found")
    return json.loads(path.read_text(encoding="utf-8"))


def write_eval_run(
    workspace_root: str | Path,
    result: dict[str, Any],
    *,
    storage_root: str | Path | None = None,
) -> None:
    run_id = validate_safe_id(str(result.get("id") or ""), "run_id")
    _write_json(
        runs_root(workspace_root, storage_root=storage_root)
        / run_id
        / "result.json",
        result,
    )


def list_eval_runs(
    workspace_root: str | Path,
    *,
    storage_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    root = runs_root(workspace_root, storage_root=storage_root)
    if not root.is_dir():
        return []
    runs: list[dict[str, Any]] = []
    for entry in root.iterdir():
        if not entry.is_dir():
            continue
        result_path = entry / "result.json"
        if not result_path.is_file():
            continue
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
            runs.append(
                {
                    key: result.get(key)
                    for key in (
                        "id",
                        "workspace",
                        "dataset_id",
                        "dataset_name",
                        "agent",
                        "status",
                        "created_at",
                        "started_at",
                        "finished_at",
                        "case_limit",
                        "summary",
                        "error",
                        "owner_digest",
                    )
                }
            )
        except (OSError, json.JSONDecodeError):
            continue
    return sorted(runs, key=lambda item: float(item.get("created_at") or 0), reverse=True)


AgentFactory = Callable[[], Any]


def _stop_eval_agent_on_cancel(
    cancel_event: threading.Event,
    case_finished: threading.Event,
    agent: Any,
) -> None:
    while not case_finished.wait(0.05):
        if not cancel_event.is_set():
            continue
        stop = getattr(agent, "stop", None)
        if callable(stop):
            try:
                stop()
            except Exception:
                pass
        return


def execute_eval_run(
    workspace_root: str | Path,
    run_id: str,
    *,
    agent_factory: AgentFactory,
    cancel_event: threading.Event | None = None,
    storage_root: str | Path | None = None,
    redact_errors: bool = False,
) -> dict[str, Any]:
    cancel_event = cancel_event or threading.Event()
    result = read_eval_run(
        workspace_root,
        run_id,
        storage_root=storage_root,
    )
    dataset_id = str(result.get("dataset_id") or "")
    cases = read_dataset_cases(
        workspace_root,
        dataset_id,
        storage_root=storage_root,
    )
    case_limit = int(result.get("case_limit") or 0)
    if case_limit > 0:
        cases = cases[:case_limit]

    result["status"] = "running"
    result["started_at"] = result.get("started_at") or utc_timestamp()
    result["summary"] = summarize_cases(result.get("cases", []), total=len(cases))
    write_eval_run(workspace_root, result, storage_root=storage_root)

    case_results: list[dict[str, Any]] = []
    try:
        for case in cases:
            if cancel_event.is_set():
                result["status"] = "canceled"
                break
            start = time.monotonic()
            agent = None
            cancel_watcher: threading.Thread | None = None
            case_finished = threading.Event()
            raw_result: dict[str, Any]
            try:
                agent = agent_factory()
                cancel_watcher = threading.Thread(
                    target=_stop_eval_agent_on_cancel,
                    args=(cancel_event, case_finished, agent),
                    daemon=True,
                )
                cancel_watcher.start()
                raw_result = agent.run_task(str(case.get("task") or ""))
                if not isinstance(raw_result, dict):
                    raw_result = {"response": str(raw_result), "exit_reason": "", "tool_results": [], "turns": 0}
                duration_sec = time.monotonic() - start
                status, failures = evaluate_assertions(
                    case,
                    raw_result,
                    workspace_root=workspace_root,
                    duration_sec=duration_sec,
                )
            except Exception as exc:  # noqa: BLE001 - eval records per-case errors without killing the server.
                duration_sec = time.monotonic() - start
                error_detail = (
                    "evaluation case failed" if redact_errors else str(exc)
                )
                raw_result = {"response": f"[error] {error_detail}", "exit_reason": "ERROR", "tool_results": [], "turns": 0}
                status = "error"
                failures = [error_detail]
            finally:
                case_finished.set()
                if cancel_watcher is not None:
                    cancel_watcher.join(timeout=0.2)
                if agent is not None and hasattr(agent, "close"):
                    try:
                        agent.close()
                    except Exception:
                        pass

            response = str(raw_result.get("response") or "")
            case_result = {
                "id": case.get("id", ""),
                "name": case.get("name", case.get("id", "")),
                "task": case.get("task", ""),
                "tags": case.get("tags", []),
                "status": status,
                "duration_sec": duration_sec,
                "turns": int(raw_result.get("turns") or 0),
                "exit_reason": str(raw_result.get("exit_reason") or ""),
                "tool_calls": sorted(_tool_names(raw_result)),
                "failures": failures,
                "response_excerpt": response[:1200],
            }
            case_results.append(case_result)
            result["cases"] = case_results
            result["summary"] = summarize_cases(case_results, total=len(cases))
            write_eval_run(workspace_root, result, storage_root=storage_root)
            if cancel_event.is_set():
                result["status"] = "canceled"
                break

        if result.get("status") != "canceled":
            result["status"] = "completed"
    except Exception as exc:  # noqa: BLE001
        result["status"] = "error"
        result["error"] = (
            "evaluation run failed" if redact_errors else str(exc)
        )
    finally:
        result["finished_at"] = utc_timestamp()
        result["summary"] = summarize_cases(case_results, total=len(cases))
        write_eval_run(workspace_root, result, storage_root=storage_root)
    return result
