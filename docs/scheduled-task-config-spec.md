# Scheduled Task Configuration Spec

## Problem

The Web UI has a Tasks panel for configuring repeated or one-off tasks, but the data is mock-only. There is no backend persistence, API contract, or runtime path that turns a configured task into an XAgent execution.

## Goals

- Persist scheduled task configuration per workspace under `workspace/<ws>/runtime/tasks/tasks.json`.
- Seed a small set of default tasks for the project `default.ws` when its task store does not exist.
- Provide Web UI APIs to list, create, update, pause/resume, delete, and debug-run a configured task once.
- Execute due tasks in the backend by reusing the existing chat task path and selected workspace agent runtime config.
- Support one-off, daily, weekly, and custom minute-interval schedules.
- Optionally keep repeated task runs in one persistent agent chat.

## Non-Goals

- No external scheduler, database, distributed execution, missed-run replay, or cron expression parser.
- No recovery of a task that was already running when the backend restarted.
- No changes to Agent Loop, tool contracts, LLM sessions, or workspace file permissions.

## Data Contract

Each task record is a JSON object with:

- `id`, `workspace`, `name`, `prompt`, `agent`
- `repeat`: `none`, `daily`, `weekly`, or `custom`
- `date`, `time`, optional `end_date`, optional `interval_minutes`
- optional `config_path`, optional `observability_config_path`
- `keep_one_chat`, optional `chat_id`
- `status`: `running` or `paused`
- `next_run`, `last_run`, `last_session_id`, `last_error`
- `last_debug_run`, `last_debug_session_id`, `last_debug_error`
- `created_at`, `updated_at`

The frontend displays the backend record directly and derives the repeat label locally.

The project `default.ws` is initialized with four preset tasks when `runtime/tasks/tasks.json` is missing:

- `Daily Workspace Check`
- `Weekly Code Quality Audit`
- `Runtime Observability Review`
- `Docs Implementation Consistency`

If the task store exists, even if it is empty, the backend does not recreate presets.

## Runtime Behavior

The scheduler loop checks workspace task files periodically. A task is due when `status == "running"` and `next_run <= now`.

When a task runs:

- Build the selected workspace agent using the same runtime config path as `/api/chat`.
- If `keep_one_chat` is true and an agent is selected, create or reuse an agent-bound persistent chat.
- Start the task asynchronously with the existing `UISession` and `_run_task_background` path.
- Record `last_run`, `last_session_id`, clear `last_error`, and compute the next run.
- One-off tasks are paused after their first run.

When a user clicks debug run:

- Start the task immediately through the same `_run_task_background` path.
- Record `last_debug_run`, `last_debug_session_id`, and `last_debug_error`.
- Do not change `next_run`, `status`, or regular scheduled-run fields.

## Error Handling

- Invalid workspace, agent, task id, empty name, empty prompt, invalid date/time, or unsupported repeat returns `{success: false, error}`.
- Scheduler failures are stored on the task as `last_error` and do not stop the scheduler thread.
- If `end_date` is before the computed next occurrence, `next_run` becomes `null` and the task is paused.

## Tests

- Unit tests cover task creation, validation, persistence, updates, pause/resume, deletion, and due-run dispatch.
- Frontend build/type checks verify the Tasks panel binds to the API contract.
