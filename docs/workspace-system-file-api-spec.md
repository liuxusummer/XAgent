# Workspace System File API Spec

## 1. Problem

`workspace/default.ws/system/` is intentionally read-only for Agent file tools, but the Web UI still needs a controlled way to manage workspace-level configuration files such as agent prompts, skills, templates, and memory files.

The existing Web UI workspace endpoints can list `.ws` folders and read `system/` files. This spec adds the first write boundary for frontend-managed configuration without weakening Agent tool permissions.

## 2. Goals

- Provide a backend API for the frontend to read, create, and overwrite UTF-8 text files under `<project_root>/workspace/<name>.ws/system/`.
- Keep Agent tools unable to create, patch, write, or delete `system/` files.
- Use the same path validation for workspace file reads and writes.
- Make sidebar-selected workspace names such as `default.ws` resolve to the project workspace path when starting an Agent from Web UI.
- Keep the first version small: no delete, no patch format, no binary upload, no database.

## 3. API Contract

### Read

```http
GET /api/workspace/file?ws=default.ws&path=system/agents/main/AGENT.md
```

Success response remains compatible with the existing frontend:

```json
{
  "success": true,
  "data": {
    "path": "system/agents/main/AGENT.md",
    "content": "..."
  }
}
```

### Save

```http
PUT /api/workspace/file
Content-Type: application/json

{
  "ws": "default.ws",
  "path": "system/templates/foo.md",
  "content": "..."
}
```

Success response:

```json
{
  "success": true,
  "data": {
    "path": "system/templates/foo.md",
    "content": "...",
    "bytes": 3,
    "created": true
  }
}
```

## 4. Validation

- `ws` must name an existing direct child directory of `workspace/` and must end with `.ws`.
- `path` must be a relative path under `system/`.
- Path traversal, absolute paths, missing workspaces, directory targets, and non-`system/` paths are rejected.
- Reads require an existing regular file.
- Saves may create missing parent directories and files, but may not overwrite a directory.
- Content is handled as UTF-8 text.

## 5. Non-Goals

- No delete endpoint in this version.
- No patch or conflict detection.
- No binary upload.
- No change to Agent file tool permissions.
- No full frontend editor UI in this version; the frontend client only gains an API wrapper.

## 6. Tests

- Read an existing `system/` file.
- Create and overwrite a `system/` file, verifying `created`, `bytes`, and content.
- Reject non-`system/`, path traversal, absolute path, invalid `.ws`, and missing workspace requests.
- Verify Agent file tools still reject writing `system/` files.
