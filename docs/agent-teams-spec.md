# Agent Teams Spec

## 1. Problem

Workspace users can create many agents under `system/agents/<agent>/`, but the runtime has no first-class way to group them into a task-specific team. `AGENT.md` already supports `project_agents`, yet that field is agent-local metadata and does not let users save reusable combinations, choose a leader, or let a leader delegate work to a member agent.

This spec adds a conservative first version of agent teams: a team is a workspace system configuration object, and execution still runs through one leader agent that delegates sub-tasks serially.

## 2. Goals

- Store reusable team definitions under `workspace/<ws>/system/teams/<team>.json`.
- Let the Web UI list, create, edit, and select teams independently from agent definitions.
- Reuse existing agents by reference; do not copy `AGENT.md`, `SOUL.md`, memory, tools, skills, or model settings into a team.
- Add an `agent_delegate` tool for leader agents to run a selected member agent on a sub-task.
- Keep delegation serial and bounded. A delegated agent gets its own `XAgent`, `Session`, prompt, tool allowlist, skill allowlist, memory mode, and history.
- Return the delegated agent's final response and compact execution metadata to the leader as one `ActionResult`.

## 3. Non-Goals

- No parallel multi-agent execution.
- No DAG planner, blackboard, voting, consensus, or background worker system.
- No database or remote synchronization.
- No sharing full LLM history between leader and member agents.
- No automatic migration of `project_agents`; it remains agent-local metadata.
- No direct Agent file-tool write access to `system/teams/`; team writes go through Web UI/backend management APIs.

## 4. Workspace Contract

Team files live at:

```text
workspace/<ws>/system/teams/<team>.json
```

Team names use the same safe child-name model as agents: no slash, no path traversal, no empty names.

Schema:

```json
{
  "name": "dev-team",
  "description": "Coding implementation team",
  "leader": "main",
  "mode": "leader_delegates",
  "members": [
    {
      "agent": "coding",
      "role": "implementation",
      "autoDelegate": true
    }
  ],
  "created_at": 1710000000.0,
  "updated_at": 1710000000.0
}
```

Supported `mode` values:

- `manual`: UI-selected team context only; no extra runtime automation beyond member visibility.
- `leader_delegates`: leader can call `agent_delegate` for members with `autoDelegate=true`.
- `roundtable_review`: first version stores the mode but does not auto-run members; member agents should be invoked manually or by the leader through `agent_delegate`.

Unknown modes are normalized to `leader_delegates`.

## 5. Runtime Contract

### 5.1 Agent Construction

`build_agent()` accepts optional team context:

- `team_name`
- `team_config`

The leader's system prompt receives a compact `[Agent Team]` section listing:

- team name and mode
- leader
- member agent names
- member roles
- whether each member is delegate-enabled

This section is prompt context only. The runtime source of truth for valid delegation remains `AgentContext.team_config`.

### 5.2 Delegation Tool

Add tool schema:

```text
agent_delegate(agent, task, context?, expected_output?)
```

Behavior:

1. Validate that a team is active.
2. Validate that `agent` is a safe member of the active team.
3. Reject delegation to the current leader agent.
4. Reject members where `autoDelegate` is false unless future manual execution explicitly enables it. First version keeps the tool conservative.
5. Build a child `XAgent` from the target agent's `AGENT.md` runtime config.
6. Run the child agent synchronously and serially.
7. Close the child agent after completion.
8. Return a compact result containing member name, exit reason, turns, response, and tool result count.

The child prompt combines:

- the sub-task
- optional context from the leader
- optional expected output instructions
- a reminder that the child reports results back to the leader, not directly to the user

### 5.3 Isolation

Each delegated child gets an independent:

- LLM client/session/history
- system prompt
- tools schema filtered by that agent's `tools`
- skills registry and allowlist
- memory mode and effective memory
- `AgentContext`

The child shares the same workspace root and observability sink. It does not share the leader's `Session.history`, `history_info`, `working`, or active skills.

### 5.4 Recursion Guard

First version prevents nested delegation by constructing delegated child agents without active team context. This avoids uncontrolled recursive team calls while preserving the main leader delegation use case.

## 6. API Contract

Add backend endpoints:

- `GET /api/workspace/teams?ws=default.ws`
- `GET /api/workspace/team?ws=default.ws&team=dev-team`
- `PUT /api/workspace/team`
- `DELETE /api/workspace/team`

`PUT` creates or replaces a team file after validating:

- team name is safe
- leader exists under `system/agents/<leader>/`
- each member agent exists
- duplicate members are collapsed by agent name
- member fields are normalized to strings/booleans

`DELETE` removes only one safe team JSON file.

Existing `POST /api/chat` accepts optional `team`. When present, the backend loads the team, applies the team leader as the effective runtime agent if no explicit `agent` is sent, and passes team context into `build_agent()`.

## 7. Frontend Contract

The Web UI adds team management without replacing existing agent editing:

- Load teams for the selected workspace.
- Let users select an active team for chat.
- Let users create/edit/delete a team with name, description, leader, mode, and member rows.
- Show team members by referencing existing workspace agents.
- Send selected team with chat requests.

The first version can use a compact management panel. It does not need a visual workflow designer.

## 8. Preset Team

The Research Project workspace template seeds a `deepresearch` team:

- leader: `main`
- mode: `leader_delegates`
- members:
  - `source_scout`: source discovery and credibility triage
  - `evidence_analyst`: claim extraction and evidence analysis
  - `synthesis_writer`: structured synthesis and draft writing
  - `research_critic`: gap analysis and overclaim review

The default workspace may also include this team so users can try team delegation without creating a new workspace.

## 9. Error Handling

Delegation failures return `ActionResult.data.status = "ERROR"` and a diagnostic message, not exceptions that crash the leader loop.

Examples:

- no active team
- unknown member
- member not delegate-enabled
- delegation to self
- invalid target agent runtime config
- child agent execution exception

Team API errors return `{"success": false, "error": "..."}` matching existing Web UI API style.

## 10. Tests

Focused tests should cover:

- Team name validation and JSON round trip.
- Listing, reading, writing, and deleting team configs.
- Runtime config rejects missing leader/member agents.
- `build_system_prompt()` includes the compact team section when a team is active.
- `agent_delegate` rejects no-team, unknown-agent, self, and disabled-member cases.
- `agent_delegate` can run a fake child runner and return compact success metadata without sharing leader state.
- `POST /api/chat` accepts `team` and applies the team leader when no explicit agent is provided.
- Research Project template seeds `system/teams/deepresearch.json` and all referenced member agents.

## 11. Acceptance Criteria

- Users can save free-form combinations of existing agents as named teams.
- Users can select a team in Web UI chat.
- A team leader can delegate serially to enabled member agents through `agent_delegate`.
- Delegated agents use their own runtime configuration and session history.
- Existing single-agent chat continues to work without selecting a team.
- The implementation does not introduce parallel execution, databases, or direct Agent file-tool writes to `system/teams/`.

## 12. Benefits And Risks

Benefits:

- Converts existing many-agent workspaces into reusable team workflows.
- Keeps the first runtime boundary small and aligned with current `XAgent -> agent_loop -> handler` design.
- Avoids multi-agent history explosion by returning only compact child results to the leader.

Risks:

- Synchronous child execution can make one leader turn long; bounded `maxTurns` and clear progress output mitigate this.
- Child agents with write tools can still modify the same workspace; serial execution avoids races but does not prevent bad delegation choices.
- `roundtable_review` is stored before it is fully automated; UI should present it as a saved collaboration mode, not an auto-run engine.
