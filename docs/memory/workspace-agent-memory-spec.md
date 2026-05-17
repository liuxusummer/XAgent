# Workspace and Agent Memory Spec

## Problem

Workspace memory now has two scopes:

- `workspace/default.ws/system/memory/` stores memory shared by all agents in the workspace.
- `workspace/default.ws/system/agents/<agent>/MEMORY.md` stores memory private to one agent.

The runtime still reads repository-level `memory/` for boot and refresh memory. That makes workspace memory editable in the UI but invisible to agent execution.

## Goals

- Add provider functions for workspace-global, agent-private, and effective memory reads.
- Make `memory: "project"` mean workspace-global plus current agent private memory.
- Keep missing or empty memory files non-fatal.
- Keep `system/` write protection in Agent file tools unchanged.
- Preserve repository-level memory provider functions as compatibility helpers.

## Non-goals

- No database, vector retrieval, background writes, or automatic memory settlement.
- No change to `AGENT.md` / `SOUL.md` parsing beyond consuming the existing `memory` field.
- No Agent file-tool write access to `system/memory/` or `MEMORY.md`.
- No migration of existing repository `memory/` files.

## Contract

`src/core/memory.py` exposes:

- `load_workspace_memory(workspace_root)`: reads direct text files under `system/memory/` in deterministic order.
- `load_agent_memory(workspace_root, agent_name)`: reads `system/agents/<agent>/MEMORY.md`.
- `load_effective_memory(workspace_root, agent_name, mode)`: combines memory by mode.

Modes:

- `none`: no long-term memory.
- `private`: only current agent `MEMORY.md`.
- `global`: only `system/memory/`.
- `project`: workspace-global then current agent private memory.

Unknown or empty modes default to `project`.

## Runtime Behavior

- `build_system_prompt()` appends effective workspace memory after `AGENT.md` / `SOUL.md` content and before workspace path hints.
- `XAgent` stores `agent_name` and `memory_mode` in `AgentContext`.
- Periodic memory refresh uses the same effective workspace memory.
- `main` never reads `coding/MEMORY.md`, and `coding` never reads `main/MEMORY.md`.

## Tests

- Provider tests cover all memory modes, missing private memory, and agent isolation.
- Prompt tests cover `memory: project` and agent prompt plus memory injection.
- Handler tests cover periodic refresh from workspace memory.
- Existing file permission tests continue to cover `system/` write protection.
