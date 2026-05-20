# Persistent Agent Trace Log Spec

## Problem

Web UI runs can stream progress to the browser but, without explicit `XAGENT_LOG_DIR` or `observability_config.log_dir`, the structured execution trace is lost after the process exits. That makes token usage, exits, tool status, and turn-level timing hard to inspect later.

## Goals

- Persist Web UI agent traces by default under `workspace/<name>.ws/runtime/traces/`.
- Preserve existing overrides: `XAGENT_LOG_DIR` first, then `observability_config.log_dir`.
- Keep low-level library/test defaults as `NullSink` unless an app entrypoint opts in.
- Expose read-only trace APIs for session summaries and event detail.
- Reuse the existing JSONL `Event` schema and tolerant reflect reader.
- Keep default logs metadata-only: no full prompts, model responses, tool args, or tool results.

## Non-Goals

- No retention cleanup, compression, or database storage.
- No token or cost estimation.
- No prompt/response/tool payload capture.
- No changes to CLI observability defaults outside existing env/config behavior.

## Backend Contract

Trace log directory resolution:

1. `XAGENT_LOG_DIR`
2. `observability_config.log_dir`
3. `workspace/<ws>/runtime/traces/`

Web UI agent construction wraps the agent sink with a default `JsonlSink` only when env/config did not already create one, then adds the Web UI token SSE sink.

Read APIs:

- `GET /api/usage/summary?ws=...&observability_config_path=...&limit=...`
- `GET /api/trace/sessions?ws=...&observability_config_path=...&limit=...`
- `GET /api/trace/sessions/{session_id}?ws=...&observability_config_path=...`

Unsafe session ids are rejected before file access.

## Frontend Contract

The Usage page loads historical usage from the resolved trace directory and lets a user select a recent task to inspect its structured timeline. Empty states distinguish "no task has run yet" from API errors.

## Acceptance Criteria

- A Web UI task with no observability config creates `workspace/<ws>/runtime/traces/<session_id>.jsonl`.
- Existing env/config log dirs still win.
- Usage totals and trace detail read the same JSONL source.
- Frontend build passes and the Usage page can render session timelines.
