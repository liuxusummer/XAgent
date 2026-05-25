# Self Evolution Spec

## Problem

XAgent already tells the model not to retry blindly, but the runtime does not turn concrete failures into durable behavior changes. When a tool fails, the next turn only receives the tool result and a generic continuation prompt. Future tasks also do not inherit lessons from recurring failure patterns unless the model explicitly updates memory.

## Goals

- Detect runtime problems at the turn boundary from existing tool results.
- Inject a concise self-evolution prompt on the next turn that forces the agent to choose a better strategy instead of repeating the failed action.
- Persist a small, de-duplicated lesson into workspace memory so future tasks can reuse the correction.
- Keep the feature inside Handler/memory infrastructure; tools remain pure functions and the loop remains generic.

## Non-goals

- No model self-call for reflection.
- No background jobs, vector stores, databases, or automatic global memory rewrites.
- No full prompt, tool args, tool result, stdout, or user data stored in evolution memory.
- No change to file-tool `system/` write protection.
- No automatic rollback or retry execution. The LLM still chooses the next action.

## Runtime Contract

`XAgentHandler` registers a `self_evolution` `TurnEndHook`. The hook inspects `tool_results` and treats these as problems:

- `status` in `ERROR`, `SKIP`, `TIMEOUT`, `EMPTY_RESPONSE`, `RETRYABLE_RESPONSE_ERROR`, `CODE_BLOCK_WITHOUT_TOOL`
- missing `status` with an `error` field

For each problem turn, the hook selects one best lesson from a deterministic rule table, persists it, and injects:

```text
[Self Evolution]
检测到本轮问题：...
已沉淀经验：...
下一轮必须先判断根因并选择最优路径...
```

The hook writes only one lesson per turn to avoid noisy memory growth.

## Persistence Contract

`src.core.memory.record_self_evolution_lesson(workspace_root, lesson, agent_name="")` writes a bullet under `## 自我进化经验`.

- Runtime calls this only when `memory_root` or `cwd` identifies a workspace.
- If `agent_name` is safe and non-empty, write to `system/agents/<agent>/MEMORY.md`.
- Otherwise, write to `system/memory/self_evolution.md`, which is already read by workspace memory loading.
- Preserve unrelated file content.
- De-duplicate identical lessons.
- Keep only the latest 20 self-evolution lessons.
- Return structured status; memory write failures must not block the agent loop.

## Boundaries

- The main loop only passes current no-tool results into `turn_end_callback`; it does not know self-evolution policy.
- `XAgentHandler` owns problem classification and prompt injection because it already owns turn-level hooks and `AgentContext`.
- `src.core.memory` owns memory file mutation because it already owns workspace memory path rules.

## Tests

- Memory provider tests cover global fallback, agent-private writes, de-duplication, and cap behavior.
- Handler tests cover error detection, prompt injection, and memory persistence.
- Agent loop tests cover no-tool retry results being visible to turn-end hooks.

## Acceptance Criteria

- A failed tool turn receives a self-evolution prompt before the next LLM call.
- A no-tool retry problem can also trigger self-evolution.
- A reusable lesson is persisted without storing raw prompts or tool payloads.
- Focused tests pass.
