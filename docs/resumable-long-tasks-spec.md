# Resumable Long Tasks Spec

## Problem

Long XAgent tasks can span many tool calls and plan updates. If the process
restarts, the model fails, or the user pauses execution, the current task state
is mostly implicit in live memory and `Session.history`. A later run can inspect
trace files, but there is no compact checkpoint that says what was planned, what
tools already completed, which files mattered, and what remains.

## Goals

- Persist a checkpoint during long task execution, not only at task completion.
- Capture enough state to resume safely:
  - current `plan.md`
  - completed tool-result summaries
  - key file state for files touched or read by tools
  - pending steps from unchecked plan items and pending loop prompts
- Allow a new task run to resume from `latest` or a specific checkpoint id by
  injecting a resume prompt.
- Keep the Agent Loop generic; it only calls an optional callback with current
  turn state.
- Keep tools pure and do not store tool state in `tools/`.
- Store metadata and bounded summaries only; do not persist full tool payloads,
  stdout, model responses, or tool args.

## Non-Goals

- No database, vector store, background worker, or external scheduler.
- No replay of already executed tools.
- No rollback of file changes.
- No migration of `plan.md` into `runtime/`.
- No change to `Session.history` trimming or provider protocol behavior.

## Runtime Contract

`AgentContext` gains an optional `checkpoint_callback`.

`run_agent_loop()` calls it:

- once at run start with turn `0`
- after each turn with current accumulated tool summaries
- at terminal states with final `exit_reason`

Callback failures are swallowed and emitted as normal execution continues.

`XAgent.run_task(query, resume_checkpoint=None)` sets the callback for the
duration of the task. If `resume_checkpoint` is provided, it loads the checkpoint
from the current workspace and prepends a resume prompt to the query.

`XAgent.resume_task(checkpoint_id="latest", query="")` is a convenience wrapper.

The Web frontend assigns a task-specific checkpoint id before starting an Agent
run and stores it in that persistent chat's `state.json`. After a service
restart, a chat persisted as `running` or `waiting_for_user` is exposed as
`interrupted`. Resume is offered only when that exact checkpoint exists and
belongs to the chat's agent; Web never falls back to workspace-level
`latest.json`.

## Persistence Contract

Checkpoints live under:

```text
workspace/<name>.ws/runtime/checkpoints/
  <session_id>.json
  latest.json
```

Each checkpoint stores:

- schema version
- checkpoint id and session id
- task title and bounded task text
- agent name
- turn
- status: `running`, `completed`, `interrupted`, or `failed`
- exit reason
- plan content from `plan.md`, capped
- pending steps from unchecked Markdown task list items and pending prompts
- completed tool result summaries, capped
- file state for paths visible in tool result data: relative path, exists,
  directory flag, size, `mtime_ns`, and `sha256` for regular files up to a fixed
  size limit
- timestamps

Writes are atomic by writing a `.tmp` file then replacing the target. `latest`
is updated after the session checkpoint write succeeds.

## Resume Prompt Contract

The resume prompt includes:

- original task summary
- checkpoint id, prior status, turn, and exit reason
- current plan
- pending steps
- completed tool result summaries
- key file states
- a short instruction to verify current file state before making changes and to
  continue from pending steps rather than replaying completed work

The model still chooses the next tools. Resume does not skip safety checks.

## Boundaries

- `src.core.checkpoint` owns file format, sanitization, file state capture, and
  resume prompt rendering.
- `run_agent_loop` only calls `ctx.checkpoint_callback(snapshot)`.
- `XAgent` owns workspace/session wiring and exposes resume entry points.
- `XAgentHandler` continues owning plan and working-memory behavior.

## Error Handling

- Missing checkpoint returns a structured `SKIP`.
- Malformed checkpoint returns `ERROR`.
- Callback write errors do not fail the agent loop.
- File-state reads ignore paths outside the workspace and record errors without
  stopping checkpoint writes.

## Tests

- Unit tests cover checkpoint writing, latest loading, file state capture, plan
  pending-step extraction, and resume prompt rendering.
- Agent loop tests cover callback invocation at start, after turns, and terminal
  states.
- XAgent tests cover resuming from `latest` and restoring the previous callback
  after a run.

## Acceptance Criteria

- A running long task writes `runtime/checkpoints/<session_id>.json` and
  `latest.json` before completion.
- The checkpoint includes current plan, completed tool summaries, key file
  state, and pending steps.
- Interrupted, failed, and max-turn runs update the checkpoint with a terminal
  status.
- A later run can call `resume_task("latest")` or
  `run_task(..., resume_checkpoint="latest")` to continue from checkpoint
  context.
- Focused tests pass.
