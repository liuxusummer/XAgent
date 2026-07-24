# Agent-Bound Persistent Chat Sessions Spec

## Problem

Web UI chat state is currently temporary. Refreshing the browser or restarting the backend loses the visible message list and the LLM session history, so an agent-bound conversation cannot be resumed with context.

## Goals

- Keep global chat as a temporary scratchpad.
- Persist chats only when they are bound to `workspace + agent`.
- Support create, list, restore, and delete for agent-bound chats.
- Restore both visible UI messages and LLM `history`, so later tasks continue with prior context.
- Store data locally under `workspace/<ws>/runtime/chats/<agent>/<chat_id>/`.

## Non-Goals

- No database, search, archive, rename, or cross-workspace migration.
- No running task recovery after backend restart.
- No reuse of trace JSONL for chat replay; trace remains metadata-oriented.

## Contracts

- `metadata.json` stores `chat_id`, `workspace`, `agent`, `title`, timestamps, preview, message count, and status.
- `state.json` stores frontend messages, backend UI session id, runtime config key, waiting state, and full LLM history.
- `POST /api/chat` accepts optional `chat_id`; if present, `agent` and workspace are required and the chat is restored or created before execution.
- Unsafe `ws`, `agent`, and `chat_id` values are rejected before file access.
- The global session lock protects only the in-memory session registry and is never held while acquiring a per-session lock. Mutable chat state is protected by a per-session lock so one slow session cannot block other sessions.
- Streaming updates mark persistent state dirty but do not write immediately. Persistence is rate-limited per session and force-flushed on user-wait, completion, stop, and session eviction.
- Stopping a user-waiting session unblocks its pending reply queue and resumes the normal drain path until the interrupted terminal result is persisted; repeated stop requests are idempotent while termination is pending.
- Idle in-memory sessions expire after one hour. Running sessions time out after six hours and user-waiting sessions after 24 hours; timed-out sessions are stopped, force-flushed, and closed.
- The registry has a hard limit of 256 sessions. Admission first evicts the oldest inactive session; if every slot is active, the new session is rejected instead of exceeding the limit.
- Web shutdown stops active sessions, flushes dirty persistent state, closes agents, and clears the registry. Eviction and shutdown do not delete persistent chat files.

## Acceptance

- A new agent chat writes metadata and state under workspace runtime.
- Restoring a chat after clearing in-memory sessions reloads messages and LLM history.
- Global chat continues to work without writing chat files.
- Frontend sidebar shows chats for the selected agent and can create, restore, and delete them.
- Looking up, scanning, cleaning, or waiting on one session does not hold the global registry lock while acquiring that session's lock.
- Repeated streaming deltas within the persistence interval produce at most one state write, while terminal transitions are persisted immediately.
- Stopping a user-waiting session releases the blocked Agent, emits one stop event, and reaches the normal terminal drain without duplicating work on repeated stop requests.
- Expired idle, running, and user-waiting sessions are closed and removed according to their separate timeouts.
- New sessions never raise the registry above the hard capacity, and active sessions are not evicted merely to admit another session.
- Application shutdown leaves no registered session or open Agent owned by the Web UI.
