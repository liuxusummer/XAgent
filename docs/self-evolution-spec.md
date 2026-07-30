# Self Evolution Spec

## Problem

XAgent already tells the model not to retry blindly, but the runtime does not turn concrete failures into durable behavior changes. When a tool fails, the next turn only receives the tool result and a generic continuation prompt. Future tasks also do not inherit lessons from recurring failure patterns unless the model explicitly updates memory.

## Goals

- Detect runtime problems at the turn boundary from existing tool results.
- Inject a concise self-evolution prompt on the next turn that forces the agent to choose a better strategy instead of repeating the failed action.
- Submit a small, de-duplicated lesson as a pending MemoryCandidate; future
  tasks can use it only after separate review.
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

For each problem turn, the hook selects one best lesson from a deterministic rule table, proposes it, and injects:

```text
[Self Evolution]
检测到本轮问题：...
已沉淀经验：...
下一轮必须先判断根因并选择最优路径...
```

The hook proposes only one lesson per turn to avoid noisy candidate growth.

## Persistence Contract

The production hook calls `MemoryStore.propose(...)` with
`trust=tool_untrusted`, a seven-day TTL, Principal-derived namespace/ACL and
source digests. It never writes `MEMORY.md`.

- Runtime calls this only when `memory_root` or `cwd` identifies a workspace
  and an authenticated Principal has `memory.propose`.
- Candidate and record state share the workspace lock and atomic replacement.
- Exact candidate fingerprints de-duplicate repeated proposals.
- Proposal failures must not block the Agent Loop.
- The legacy `record_self_evolution_lesson` helper remains only for direct
  compatibility tests and is not called by the production hook.

## Boundaries

- The main loop only passes current no-tool results into `turn_end_callback`; it does not know self-evolution policy.
- `XAgentHandler` owns problem classification and prompt injection because it already owns turn-level hooks and `AgentContext`.
- `src.core.memory_store` owns candidate validation, review and atomic state.

## Tests

- Memory provider tests cover global fallback, agent-private writes, de-duplication, and cap behavior.
- Handler tests cover error detection, prompt injection, candidate quarantine and no direct memory write.
- Agent loop tests cover no-tool retry results being visible to turn-end hooks.

## Acceptance Criteria

- A failed tool turn receives a self-evolution prompt before the next LLM call.
- A no-tool retry problem can also trigger self-evolution.
- A reusable lesson is quarantined without storing raw prompts or tool payloads,
  and remains inactive until separate review.
- Focused tests pass.
