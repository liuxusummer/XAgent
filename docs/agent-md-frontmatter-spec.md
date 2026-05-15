# AGENT.md Frontmatter Spec

## 1. Problem

Workspace agents currently use several possible files for configuration: `AGENT.md`, `SOUL.md`, `skills.json`, `tools.json`, and `config.json`. This splits one agent's identity and runtime settings across multiple sources and makes the frontend responsible for knowing several file formats.

This spec makes `system/agents/<name>/AGENT.md` the single source of truth for agent metadata and editable configuration.

## 2. Goals

- Store agent metadata in YAML frontmatter at the top of `AGENT.md`.
- Keep the Markdown body as the human-readable role and behavior description.
- Let the backend parse and serialize the supported frontmatter fields.
- Let the frontend edit profile fields, tools, skills, and Markdown body through backend profile APIs.
- Ignore legacy `config.json`, `skills.json`, and `tools.json`.

## 3. Supported Frontmatter

Supported fields:

```yaml
---
name: "main"
description: "日常对话分析"
tools:
  - "file_read"
model: "minimax-m2.7"
runtime_model: "claude-sonnet"
maxTurns: 300
memory: "project"
skills: []
project_agents: []
---
```

First version supports only strings, integers, empty arrays, inline string arrays, and multiline string arrays. Nested objects are intentionally unsupported.

## 4. API Behavior

- `GET /api/workspace/agents` returns `name`, `description`, `files`, and `profile`.
- `GET /api/workspace/agent-profile` returns parsed `profile`, Markdown `body`, full `content`, and file path.
- `PUT /api/workspace/agent-profile` serializes `profile` and `body` back into `AGENT.md`.
- Existing `GET/PUT /api/workspace/file` remains available for raw system file editing.

## 5. Non-Goals

- Do not make runtime Agent execution consume these fields in this pass.
- Do not support deleting agents.
- Do not support nested YAML or arbitrary frontmatter values.
- Do not read or write `config.json`, `skills.json`, or `tools.json`.

## 6. Tests

- Parse AGENT.md with frontmatter.
- Parse legacy AGENT.md without frontmatter.
- Serialize profile while preserving Markdown body.
- Verify `GET /api/workspace/agents` includes profile and description.
- Verify agent profile endpoint writes back to AGENT.md.
