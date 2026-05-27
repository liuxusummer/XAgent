# Workspace Templates Spec

## 1. Problem

XAgent workspaces already have a standard layout and several workspace-scoped
resources: agents, skills, memory, templates, runtime tasks, and business files.
The Web UI can list existing `.ws` workspaces, but users cannot create a new
workspace from the UI, and every new workspace starts from the same bare layout.

This makes common workspace types tedious to set up. A code project, research
project, operations workspace, data analysis workspace, and personal assistant
workspace need different starter agents, skills, memory, directory hints, and
default scheduled tasks.

## 2. Goals

- Add first-class workspace templates for:
  - Blank
  - Code Project
  - Research Project
  - Operations
  - Data Analysis
  - Personal Assistant
- Let users create a workspace by choosing a template.
- Keep created workspaces under `<project_root>/workspace/<name>.ws`.
- Reuse the existing workspace layout contract:
  `business/`, `runtime/`, `system/agents/`, `system/memory/`,
  `system/skills/`, and `system/templates/`.
- Seed template-specific agents via `system/agents/<agent>/AGENT.md`,
  `SOUL.md`, and `MEMORY.md`.
- Seed template-specific prompt skills under `system/skills/<skill>/`.
- Seed workspace memory under `system/memory/`.
- Seed default scheduled tasks under `runtime/tasks/tasks.json`.
- Expose template listing and workspace creation through backend APIs and the
  React sidebar.

## 3. Non-Goals

- No template editor in this version.
- No applying a template to an existing workspace.
- No delete, clone, export, import, or rename workspace flow.
- No database or external storage.
- No changes to Agent file-tool permissions for `system/`.
- No changes to `resolve_workspace_dir()` behavior beyond continuing to ensure
  the base layout exists for implicit workspaces.

## 4. Backend Contract

### List Templates

```http
GET /api/workspace/templates
```

Returns:

```json
{
  "success": true,
  "data": [
    {
      "id": "code_project",
      "name": "Code Project",
      "description": "Starter agents, skills, memory, and tasks for code work."
    }
  ]
}
```

### Create Workspace

```http
POST /api/workspace
Content-Type: application/json

{
  "name": "my-project",
  "template_id": "code_project"
}
```

Rules:

- `name` may be supplied as `my-project` or `my-project.ws`; it is normalized to
  a `.ws` workspace name.
- The normalized workspace must be a direct child of `<project_root>/workspace`.
- Existing workspaces are rejected.
- Unknown template ids are rejected.
- Creation is all local filesystem writes; no Agent run is started.

Success:

```json
{
  "success": true,
  "data": {
    "name": "my-project.ws",
    "template_id": "code_project",
    "path": "/repo/workspace/my-project.ws"
  }
}
```

## 5. Template Registry

The first version uses a small Python registry in `src/core/workspace_templates.py`.
Templates are structured data rather than copied directory trees so tests can
inspect the generated content deterministically and the implementation stays
dependency-free.

Each template declares:

- `id`, `name`, `description`
- `directories`
- `agents`: name, description, tool allowlist, skill allowlist, body, soul,
  and initial memory
- `skills`: name, description, `SKILL.md`, optional `_meta.json`
- `memory_files`
- `template_files`
- `scheduled_tasks`

`blank` seeds only the standard layout plus minimal `main` and `coding` agents
compatible with the existing layout contract.

## 6. Frontend Behavior

The sidebar workspace selector gains a create button. The create dialog lets the
user enter a workspace name and choose one of the backend templates.

After a successful create:

- refresh `/api/workspace/list`
- switch the app to the new workspace
- close the dialog
- leave chat/session state reset through the existing `onWorkspaceChange` path

## 7. Error Handling

- Empty names return `Workspace name is required`.
- Invalid names return `Invalid workspace name`.
- Duplicate names return `Workspace already exists`.
- Unknown templates return `Unknown workspace template`.
- Filesystem failures return the OS error string.

Partial creation should be avoided by validating workspace name and template id
before creating the directory. If a write fails after directory creation, the
error is returned and the partially created local directory is left for manual
inspection rather than automatically deleting user-visible files.

## 8. Tests

Focused backend tests cover:

- template list includes the five requested templates plus `blank`
- workspace name normalization
- duplicate workspace rejection
- invalid name and unknown template rejection
- code project creation seeds agents, skills, memory, business directories, and
  scheduled tasks
- every requested non-blank template seeds at least one specific agent, skill,
  memory file, and scheduled task

Frontend verification covers:

- TypeScript build after adding the create-workspace client contract and sidebar
  dialog.

## 9. Acceptance Criteria

- A user can create a new workspace from the Web UI by selecting a template.
- The backend can list available templates.
- The backend can create `.ws` workspaces from template ids.
- The supported templates include code project, research project, operations,
  data analysis, and personal assistant.
- Created workspaces contain the standard layout.
- Created non-blank workspaces contain preconfigured agents, skills, memory,
  directory structure, and default scheduled tasks.
- Existing workspace APIs continue to read the generated agents, skills, memory,
  and task files.
