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

## Acceptance

- A new agent chat writes metadata and state under workspace runtime.
- Restoring a chat after clearing in-memory sessions reloads messages and LLM history.
- Global chat continues to work without writing chat files.
- Frontend sidebar shows chats for the selected agent and can create, restore, and delete them.
