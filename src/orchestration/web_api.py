"""GET-only, redacted Web projections with no execution/control mutations."""

from __future__ import annotations

import asyncio
import json
import re
import threading
from collections import OrderedDict
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from .event_types import PUBLIC_EVENT_TYPES
from .models import AttemptRecord, EventRecord, NodeRecord, RunRecord
from .operator_diagnostics import diagnose_attempt, diagnose_node, diagnose_run
from .store import (
    DurableRunStore,
    OrchestrationStoreError,
    ProjectionReplayLimitError,
    StoreSchemaError,
)

WORKSPACE_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,62}\.ws$"
RUN_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$"
MAX_RUN_PAGE = 100
MAX_EVENT_PAGE = 500
MAX_SNAPSHOT_NODE_PAGE = 100
MAX_SNAPSHOT_ATTEMPT_PAGE = 100
DEFAULT_SNAPSHOT_NODE_PAGE = 50
DEFAULT_SNAPSHOT_ATTEMPT_PAGE = 50
MAX_OFFSET = 100_000
MAX_SEQUENCE = 2**63 - 1
MAX_WORKSPACE_STORE_CACHE = 64
MAX_INTEGRITY_REPLAY_EVENTS = 10_000
MAX_INTEGRITY_REPLAY_PAYLOAD_BYTES = 8 * 1024 * 1024
MAX_INTEGRITY_REPLAY_SECONDS = 2.0
INTEGRITY_REPLAY_PAGE_SIZE = 128
SSE_POLL_SECONDS = 0.25

_WORKSPACE_RE = re.compile(WORKSPACE_PATTERN)
_RUN_ID_RE = re.compile(RUN_ID_PATTERN)


class WorkspaceProjectionError(RuntimeError):
    pass


class InvalidWorkspaceError(WorkspaceProjectionError):
    pass


class WorkspaceStoreRegistry:
    """Lazily open trusted external control-plane Stores by tenant name.

    ``database_resolver`` is a deployment-owned boundary.  It must return the
    exact database file exposed by a control-plane service/OS identity, never a
    path inside an Agent-writable workspace.  This adapter deliberately does
    not derive a Store path from the workspace name.
    """

    def __init__(
        self,
        database_resolver: Callable[[str], str | Path | None],
        *,
        store_factory: Callable[[Path], DurableRunStore] = DurableRunStore,
        max_stores: int = MAX_WORKSPACE_STORE_CACHE,
    ) -> None:
        if (
            isinstance(max_stores, bool)
            or not isinstance(max_stores, int)
            or max_stores < 1
            or max_stores > MAX_WORKSPACE_STORE_CACHE
        ):
            raise ValueError("max_stores must be a positive bounded integer")
        if not callable(database_resolver):
            raise TypeError("database_resolver must be callable")
        self._database_resolver = database_resolver
        self._store_factory = store_factory
        self._max_stores = max_stores
        self._lock = threading.Lock()
        self._stores: OrderedDict[Path, DurableRunStore] = OrderedDict()

    def get(self, workspace: str) -> DurableRunStore | None:
        if not isinstance(workspace, str) or not _WORKSPACE_RE.fullmatch(workspace):
            raise InvalidWorkspaceError("invalid workspace name")
        raw_database = self._database_resolver(workspace)
        if raw_database is None:
            return None
        database_input = Path(raw_database)
        if database_input.is_symlink():
            raise WorkspaceProjectionError(
                "orchestration database must not be a symlink"
            )
        if not database_input.is_file():
            return None
        database = database_input.resolve(strict=True)
        if database.name != "orchestration.sqlite3":
            raise WorkspaceProjectionError(
                "trusted database resolver returned an unexpected filename"
            )
        parts = database.parts
        if any(
            part.endswith(".ws")
            and index + 1 < len(parts)
            and parts[index + 1] == "runtime"
            for index, part in enumerate(parts)
        ):
            raise WorkspaceProjectionError(
                "orchestration database must be outside Agent workspaces"
            )
        with self._lock:
            existing = self._stores.get(database)
            if existing is not None:
                self._stores.move_to_end(database)
                return existing
        # Store construction may inspect or migrate SQLite.  Keep it outside
        # the registry lock so first access to one workspace cannot block
        # unrelated workspace lookups.  A concurrent same-path construction is
        # harmless: only the first retained Store is returned below.
        candidate = self._store_factory(database)
        with self._lock:
            existing = self._stores.get(database)
            if existing is not None:
                self._stores.move_to_end(database)
                return existing
            self._stores[database] = candidate
            while len(self._stores) > self._max_stores:
                self._stores.popitem(last=False)
            return candidate


def create_projection_router(
    registry: WorkspaceStoreRegistry,
    *,
    integrity_authorizer: Callable[[Request], bool] | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/api/orchestration", tags=["orchestration-readonly"])

    @router.get("/runs")
    def list_runs(
        request: Request,
        ws: Annotated[str, Query(pattern=WORKSPACE_PATTERN)] = "default.ws",
        limit: Annotated[int, Query(ge=1, le=MAX_RUN_PAGE)] = 50,
        offset: Annotated[int, Query(ge=0, le=MAX_OFFSET)] = 0,
    ) -> dict[str, Any]:
        _reject_unknown_query(request, {"ws", "limit", "offset"})
        store = _require_store(registry, ws)
        try:
            runs = store.list_runs(limit=limit, offset=offset)
        except HTTPException:
            raise
        except BaseException as exc:
            _raise_projection_error(exc)
        return {
            "workspace": ws,
            "limit": limit,
            "offset": offset,
            "runs": [_sanitize_run(run) for run in runs],
        }

    @router.get("/runs/{run_id}")
    def get_run_detail(
        request: Request,
        run_id: str,
        ws: Annotated[str, Query(pattern=WORKSPACE_PATTERN)] = "default.ws",
        node_limit: Annotated[
            int,
            Query(ge=1, le=MAX_SNAPSHOT_NODE_PAGE),
        ] = DEFAULT_SNAPSHOT_NODE_PAGE,
        node_offset: Annotated[int, Query(ge=0, le=MAX_OFFSET)] = 0,
        attempt_limit: Annotated[
            int,
            Query(ge=1, le=MAX_SNAPSHOT_ATTEMPT_PAGE),
        ] = DEFAULT_SNAPSHOT_ATTEMPT_PAGE,
        attempt_offset: Annotated[int, Query(ge=0, le=MAX_OFFSET)] = 0,
    ) -> dict[str, Any]:
        _reject_unknown_query(
            request,
            {
                "ws",
                "node_limit",
                "node_offset",
                "attempt_limit",
                "attempt_offset",
            },
        )
        _validate_run_id(run_id)
        store = _require_store(registry, ws)
        try:
            run, nodes, attempts = store.get_projection_snapshot(
                run_id,
                node_limit=node_limit + 1,
                node_offset=node_offset,
                attempt_limit=attempt_limit + 1,
                attempt_offset=attempt_offset,
            )
            if run is None:
                raise HTTPException(status_code=404, detail="run not found")
        except HTTPException:
            raise
        except BaseException as exc:
            _raise_projection_error(exc)
        return _sanitize_snapshot(
            run,
            nodes,
            attempts,
            node_limit=node_limit,
            node_offset=node_offset,
            attempt_limit=attempt_limit,
            attempt_offset=attempt_offset,
        )

    @router.get("/runs/{run_id}/integrity")
    def verify_run_integrity(
        request: Request,
        run_id: str,
        ws: Annotated[str, Query(pattern=WORKSPACE_PATTERN)] = "default.ws",
    ) -> dict[str, Any]:
        """Explicitly replay one Run's full Event History."""

        _reject_unknown_query(request, {"ws"})
        _validate_run_id(run_id)
        _require_integrity_operator(request, integrity_authorizer)
        store = _require_store(registry, ws)
        try:
            run = store.get_run(run_id)
            if run is None:
                raise HTTPException(status_code=404, detail="run not found")
            if run.last_event_sequence > MAX_INTEGRITY_REPLAY_EVENTS:
                raise ProjectionReplayLimitError("event_count_limit")
            _require_integrity(store, run_id)
        except HTTPException:
            raise
        except BaseException as exc:
            _raise_projection_error(exc)
        return {
            "run_id": run_id,
            "projection_matches_events": True,
            "last_event_sequence": run.last_event_sequence,
        }

    @router.get("/runs/{run_id}/events")
    def get_run_events(
        request: Request,
        run_id: str,
        ws: Annotated[str, Query(pattern=WORKSPACE_PATTERN)] = "default.ws",
        after_seq: Annotated[int, Query(ge=0, le=MAX_SEQUENCE)] = 0,
        limit: Annotated[int, Query(ge=1, le=MAX_EVENT_PAGE)] = 100,
    ) -> dict[str, Any]:
        _reject_unknown_query(request, {"ws", "after_seq", "limit"})
        _validate_run_id(run_id)
        store = _require_store(registry, ws)
        try:
            run = store.get_run(run_id)
            if run is None:
                raise HTTPException(status_code=404, detail="run not found")
            events = store.list_events(run_id, after_seq=after_seq, limit=limit)
        except HTTPException:
            raise
        except BaseException as exc:
            _raise_projection_error(exc)
        return {
            "run_id": run_id,
            "after_seq": after_seq,
            "events": [_sanitize_event(event) for event in events],
            "latest_sequence": run.last_event_sequence,
        }

    @router.get("/runs/{run_id}/stream")
    async def stream_run_events(
        request: Request,
        run_id: str,
        ws: Annotated[str, Query(pattern=WORKSPACE_PATTERN)] = "default.ws",
        after_seq: Annotated[int, Query(ge=0, le=MAX_SEQUENCE)] = 0,
        batch_size: Annotated[int, Query(ge=1, le=MAX_EVENT_PAGE)] = 100,
        node_limit: Annotated[
            int,
            Query(ge=1, le=MAX_SNAPSHOT_NODE_PAGE),
        ] = DEFAULT_SNAPSHOT_NODE_PAGE,
        node_offset: Annotated[int, Query(ge=0, le=MAX_OFFSET)] = 0,
        attempt_limit: Annotated[
            int,
            Query(ge=1, le=MAX_SNAPSHOT_ATTEMPT_PAGE),
        ] = DEFAULT_SNAPSHOT_ATTEMPT_PAGE,
        attempt_offset: Annotated[int, Query(ge=0, le=MAX_OFFSET)] = 0,
        once: Annotated[str, Query(pattern=r"^(true|false)$")] = "false",
        last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    ) -> StreamingResponse:
        _reject_unknown_query(
            request,
            {
                "ws",
                "after_seq",
                "batch_size",
                "node_limit",
                "node_offset",
                "attempt_limit",
                "attempt_offset",
                "once",
            },
        )
        _validate_run_id(run_id)
        cursor = _resolve_sse_cursor(after_seq, last_event_id)
        store = _require_store(registry, ws)
        try:
            snapshot = await asyncio.to_thread(
                _load_snapshot,
                store,
                run_id,
                node_limit=node_limit,
                node_offset=node_offset,
                attempt_limit=attempt_limit,
                attempt_offset=attempt_offset,
            )
        except HTTPException:
            raise
        except BaseException as exc:
            _raise_projection_error(exc)
        generator = _stream_events(
            request,
            store,
            snapshot,
            cursor=cursor,
            batch_size=batch_size,
            once=once == "true",
        )
        return StreamingResponse(
            generator,
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-store",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router


async def _stream_events(
    request: Request,
    store: DurableRunStore,
    snapshot: dict[str, Any],
    *,
    cursor: int,
    batch_size: int,
    once: bool,
):
    """Yield bounded reads without retaining a Store connection or Web lock."""

    yield _encode_sse("snapshot", snapshot)
    latest_at_snapshot = int(snapshot["run"]["last_event_sequence"])
    while True:
        try:
            events = await asyncio.to_thread(
                store.list_events,
                snapshot["run"]["run_id"],
                after_seq=cursor,
                limit=batch_size,
            )
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            yield _encode_sse(
                "error",
                {"code": "orchestration_integrity_error"},
            )
            return
        for event in events:
            cursor = event.seq
            yield _encode_sse(
                "event",
                _sanitize_event(event),
                event_id=event.seq,
            )
        if once and (not events or cursor >= latest_at_snapshot):
            return
        if await request.is_disconnected():
            return
        if not events:
            try:
                latest_run = await asyncio.to_thread(
                    store.get_run,
                    snapshot["run"]["run_id"],
                )
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                yield _encode_sse(
                    "error",
                    {"code": "orchestration_integrity_error"},
                )
                return
            if latest_run is None:
                yield _encode_sse(
                    "error",
                    {"code": "orchestration_projection_unavailable"},
                )
                return
            if latest_run.status.is_terminal:
                return
            yield ": heartbeat\n\n"
            await asyncio.sleep(SSE_POLL_SECONDS)


def _load_snapshot(
    store: DurableRunStore,
    run_id: str,
    *,
    node_limit: int = DEFAULT_SNAPSHOT_NODE_PAGE,
    node_offset: int = 0,
    attempt_limit: int = DEFAULT_SNAPSHOT_ATTEMPT_PAGE,
    attempt_offset: int = 0,
) -> dict[str, Any]:
    run, nodes, attempts = store.get_projection_snapshot(
        run_id,
        node_limit=node_limit + 1,
        node_offset=node_offset,
        attempt_limit=attempt_limit + 1,
        attempt_offset=attempt_offset,
    )
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    return _sanitize_snapshot(
        run,
        nodes,
        attempts,
        node_limit=node_limit,
        node_offset=node_offset,
        attempt_limit=attempt_limit,
        attempt_offset=attempt_offset,
    )


def _require_store(
    registry: WorkspaceStoreRegistry,
    workspace: str,
) -> DurableRunStore:
    try:
        store = registry.get(workspace)
    except InvalidWorkspaceError as exc:
        raise HTTPException(status_code=422, detail="invalid workspace") from exc
    except (OSError, WorkspaceProjectionError) as exc:
        raise HTTPException(
            status_code=500,
            detail="workspace orchestration store unavailable",
        ) from exc
    if store is None:
        raise HTTPException(
            status_code=404,
            detail="workspace orchestration store not found",
        )
    return store


def _require_integrity(store: DurableRunStore, run_id: str) -> None:
    if not store.verify_projections_bounded(
        run_id,
        max_events=MAX_INTEGRITY_REPLAY_EVENTS,
        max_payload_bytes=MAX_INTEGRITY_REPLAY_PAYLOAD_BYTES,
        max_wall_seconds=MAX_INTEGRITY_REPLAY_SECONDS,
        page_size=INTEGRITY_REPLAY_PAGE_SIZE,
    ):
        raise StoreSchemaError("projection replay mismatch")


def _require_integrity_operator(
    request: Request,
    authorizer: Callable[[Request], bool] | None,
) -> None:
    if authorizer is None:
        raise HTTPException(
            status_code=403,
            detail="operator authorization required",
        )
    try:
        allowed = authorizer(request)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as exc:
        raise HTTPException(
            status_code=403,
            detail="operator authorization required",
        ) from exc
    if allowed is not True:
        raise HTTPException(
            status_code=403,
            detail="operator authorization required",
        )


def _raise_projection_error(exc: BaseException) -> None:
    if isinstance(exc, (KeyboardInterrupt, SystemExit)):
        raise exc
    if isinstance(exc, ProjectionReplayLimitError):
        raise HTTPException(
            status_code=413,
            detail="orchestration_integrity_limit_exceeded",
        ) from exc
    if isinstance(exc, (StoreSchemaError, OrchestrationStoreError, ValueError)):
        raise HTTPException(
            status_code=500,
            detail="orchestration integrity check failed",
        ) from exc
    raise HTTPException(
        status_code=500,
        detail="orchestration projection unavailable",
    ) from exc


def _validate_run_id(run_id: str) -> None:
    if not isinstance(run_id, str) or not _RUN_ID_RE.fullmatch(run_id):
        raise HTTPException(status_code=422, detail="invalid run id")


def _reject_unknown_query(request: Request, allowed: set[str]) -> None:
    counts: dict[str, int] = {}
    for key, _value in request.query_params.multi_items():
        counts[key] = counts.get(key, 0) + 1
    unknown = sorted(set(counts).difference(allowed))
    duplicated = sorted(key for key, count in counts.items() if count > 1)
    if unknown or duplicated:
        raise HTTPException(status_code=422, detail="invalid query parameters")


def _resolve_sse_cursor(after_seq: int, last_event_id: str | None) -> int:
    if last_event_id in (None, ""):
        return after_seq
    if not last_event_id.isascii() or not last_event_id.isdigit():
        raise HTTPException(status_code=400, detail="invalid Last-Event-ID")
    header_value = int(last_event_id)
    if header_value < 0 or header_value > MAX_SEQUENCE:
        raise HTTPException(status_code=400, detail="invalid Last-Event-ID")
    return max(after_seq, header_value)


def _sanitize_snapshot(
    run: RunRecord,
    nodes: list[NodeRecord],
    attempts: list[AttemptRecord],
    *,
    node_limit: int,
    node_offset: int,
    attempt_limit: int,
    attempt_offset: int,
) -> dict[str, Any]:
    node_has_more = len(nodes) > node_limit
    attempt_has_more = len(attempts) > attempt_limit
    return {
        "run": _sanitize_run(run),
        "nodes": [_sanitize_node(node) for node in nodes[:node_limit]],
        "attempts": [
            _sanitize_attempt(attempt)
            for attempt in attempts[:attempt_limit]
        ],
        "node_page": {
            "limit": node_limit,
            "offset": node_offset,
            "has_more": node_has_more,
            "next_offset": (
                node_offset + node_limit if node_has_more else None
            ),
        },
        "attempt_page": {
            "limit": attempt_limit,
            "offset": attempt_offset,
            "has_more": attempt_has_more,
            "next_offset": (
                attempt_offset + attempt_limit if attempt_has_more else None
            ),
        },
    }


def _sanitize_run(run: RunRecord) -> dict[str, Any]:
    diagnostic = diagnose_run(run)
    return {
        "schema_version": run.schema_version,
        "run_id": run.run_id,
        "workflow_id": run.workflow_id,
        "workflow_version": run.workflow_version,
        "definition_digest": run.definition_digest,
        "status": run.status.value,
        "terminal": run.status.is_terminal,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
        "last_event_sequence": run.last_event_sequence,
        "projection_version": run.projection_version,
        "diagnostic": (
            None if diagnostic is None else diagnostic.to_dict()
        ),
    }


def _sanitize_node(node: NodeRecord) -> dict[str, Any]:
    diagnostic = diagnose_node(node)
    return {
        "schema_version": node.schema_version,
        "run_id": node.run_id,
        "node_id": node.node_id,
        "node_type": node.node_type,
        "status": node.status.value,
        "terminal": node.status.is_terminal,
        "attempt_count": node.attempt_count,
        "created_at": node.created_at,
        "updated_at": node.updated_at,
        "last_event_sequence": node.last_event_sequence,
        "projection_version": node.projection_version,
        "diagnostic": (
            None if diagnostic is None else diagnostic.to_dict()
        ),
    }


def _sanitize_attempt(attempt: AttemptRecord) -> dict[str, Any]:
    diagnostic = diagnose_attempt(attempt)
    return {
        "schema_version": attempt.schema_version,
        "run_id": attempt.run_id,
        "node_id": attempt.node_id,
        "attempt_id": attempt.attempt_id,
        "attempt_number": attempt.attempt_number,
        "activity_kind": attempt.activity_kind,
        "effect_class": attempt.effect_class,
        "status": attempt.status.value,
        "terminal": attempt.status.is_terminal,
        "scheduled_at": attempt.scheduled_at,
        "started_at": attempt.started_at,
        "finished_at": attempt.finished_at,
        "last_event_sequence": attempt.last_event_sequence,
        "projection_version": attempt.projection_version,
        "diagnostic": (
            None if diagnostic is None else diagnostic.to_dict()
        ),
    }


def _sanitize_event(event: EventRecord) -> dict[str, Any]:
    return {
        "schema_version": event.schema_version,
        "sequence": event.seq,
        "event_type": (
            event.event_type
            if event.event_type in PUBLIC_EVENT_TYPES
            else "orchestration.unknown"
        ),
        "node_id": event.node_id,
        "attempt_id": event.attempt_id,
        "occurred_at": event.occurred_at,
    }


def _encode_sse(
    event: str,
    data: dict[str, Any],
    *,
    event_id: int | None = None,
) -> str:
    lines = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    lines.append(
        "data: "
        + json.dumps(
            data,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return "\n".join(lines) + "\n\n"


__all__ = [
    "InvalidWorkspaceError",
    "WorkspaceProjectionError",
    "WorkspaceStoreRegistry",
    "create_projection_router",
]
