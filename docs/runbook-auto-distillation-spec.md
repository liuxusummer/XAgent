# Runbook Auto Distillation Spec

## Problem

XAgent already persists traces, memory, workspace skills, workspace templates, and
scheduled tasks, but those systems do not automatically turn completed work into
reusable operating knowledge. A successful or failed run can leave useful
execution patterns in the trace and task result, yet future tasks only benefit if
the model manually writes a skill, template, or task.

## Goals

- Distill completed task runs with enough interaction evidence into a small
  reusable runbook entry.
- Use both success and failure signals from the current task result and
  metadata-level trace events.
- Automatically create or update workspace-local artifacts:
  - `system/skills/auto-runbooks/SKILL.md`
  - `system/skills/auto-runbooks/_meta.json`
  - `system/templates/runbook-sop.md`
  - `runtime/tasks/tasks.json`
- Keep the feature deterministic and local; no model self-call is needed.
- Preserve the existing privacy boundary by storing metadata and short summaries
  only, not full prompts, tool args, model responses, stdout, or raw tool results.
- Keep the Agent Loop generic; distillation runs after `XAgent.run_task()` returns.

## Non-Goals

- No vector store, database, background worker, or external scheduler.
- No offline batch reader in the first version.
- No automatic editing of repository-level `skills/` or `memory/`.
- No change to file-tool `system/` write protection.
- No attempt to synthesize perfect natural-language SOPs from full transcripts.
- No deletion or cleanup of older workspace artifacts outside the distiller's own
  managed sections.

## Runtime Contract

`src.core.runbook.distill_runbook_from_task(...)` receives:

- `workspace_root`
- `task`
- `result` from `run_agent_loop`
- `agent_name`
- optional trace `events`

The caller is `XAgent.run_task()` after the normal task result is produced, in a
best-effort block. Distillation failure must not change the user-visible task
result or stop the agent.

Before writing artifacts, the distiller applies an interaction-volume gate:

- `interaction_records = result.turns + len(result.tool_results)`.
- Default minimum is 10 records.
- Runs below the threshold return `SKIP` with
  `error=insufficient_interaction_records` and do not create or update any
  runbook artifacts.

The distiller classifies a run as:

- `success` when `exit_reason == CURRENT_TASK_DONE` and no problem status appears.
- `failure` when the run exits with `ERROR`, `EXITED`, or `MAX_TURNS_EXCEEDED`,
  or any tool result contains `ERROR`, `SKIP`, `TIMEOUT`, `EMPTY_RESPONSE`,
  `RETRYABLE_RESPONSE_ERROR`, or `CODE_BLOCK_WITHOUT_TOOL`.

Each entry records:

- task digest and short title
- agent name
- outcome
- exit reason
- ordered tool status summary
- reusable SOP steps
- a short caution for failures or a reuse note for successes

Entries are de-duplicated by task digest plus outcome. The latest entry for the
same key replaces the previous one. The managed runbook keeps the latest 30
entries.

## Artifact Contract

### Skill

`system/skills/auto-runbooks/SKILL.md` contains a stable intro and a managed
`## Distilled Runbooks` section. The section is replaced wholesale by the
distiller. Other content outside the section is preserved.

`_meta.json` describes the skill and adds triggers such as `runbook`, `sop`,
`复盘`, `经验`, and `自动沉淀`, so automatic skill selection can reuse it.

### Template

`system/templates/runbook-sop.md` is created if missing. It gives a compact
manual format for future SOP writing and does not overwrite user edits once the
file exists.

### Scheduled Task

`runtime/tasks/tasks.json` is upserted with a paused or running maintenance task
named `Runbook Auto Distillation Review`. The first version uses a weekly task
assigned to the current agent or `main`, with `keep_one_chat=true`. Existing
tasks are preserved. If the task already exists, only the managed fields are
refreshed.

The task prompt asks the agent to inspect `system/skills/auto-runbooks/`,
`system/templates/runbook-sop.md`, and recent traces, then consolidate stale or
duplicate runbook entries.

## Boundaries

- The Agent Loop and Handler do not know the distillation policy.
- `XAgent.run_task()` only calls a best-effort core helper after the run.
- `src.core.runbook` owns workspace file mutation for runbook artifacts.
- The Web UI scheduled task code may call the same upsert helper when creating
  template workspaces or when the distiller runs.

## Error Handling

- Invalid or missing workspace root returns `SKIP`.
- Runs below the interaction threshold return `SKIP` and do not write files.
- Artifact write failures return `ERROR` with the target path and do not raise.
- Malformed existing `tasks.json` is replaced only when it cannot be parsed as a
  dict with a list `tasks`; otherwise existing tasks are preserved.
- Non-serializable result values are stringified only for short metadata
  summaries.

## Tests

- Unit tests cover interaction-threshold skip behavior, success entry creation,
  failure entry creation, de-duplication, entry cap behavior, skill metadata,
  template creation, and scheduled task upsert.
- Integration tests cover `XAgent.run_task()` calling the distiller without
  changing the task result.

## Acceptance Criteria

- A successful run creates or updates `system/skills/auto-runbooks/SKILL.md` with
  a success SOP entry only when it has enough interaction records.
- A failed run creates or updates the same skill with a failure SOP entry and a
  caution only when it has enough interaction records.
- A short one-turn task below the threshold does not create or update runbook
  artifacts.
- `system/templates/runbook-sop.md` is created automatically.
- `runtime/tasks/tasks.json` contains one managed runbook review task without
  deleting unrelated tasks.
- Distillation stores no full prompt, model response, tool args, stdout, or raw
  tool result payload.
- Focused tests pass.
