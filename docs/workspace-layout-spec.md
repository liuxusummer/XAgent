# Workspace Layout Spec

## 1. 问题定义

当前 XAgent 已有 `ctx.cwd` 工作区概念，默认工作区是 `<project_root>/workspace`。最近的本地结构已经出现更细的工作区骨架：

```text
workspace/
└── default.ws/
    ├── business/
    ├── runtime/
    └── system/
        ├── agents/
        │   ├── main/
        │   │   ├── AGENT.md
        │   │   └── SOUL.md
        │   └── coding/
        │       ├── AGENT.md
        │       └── SOUL.md
        ├── memory/
        ├── skills/
        └── templates/
```

这说明工作区正在从“一个目录”演进为“一个带系统区、业务区、运行区的工作空间”。如果不先明确路径语义，后续文件工具、运行时产物、前端管理能力会很容易产生冲突。

本文档定义第一版 `default.ws` 布局契约，尤其明确两个产品决策：

- 普通 `file_read("foo.txt")` / `file_write("foo.txt")` 仍默认解析到 `workspace/default.ws/foo.txt`。
- `system/` 下的文件不允许通过 Agent 文件工具修改；写入能力后续由前端管理功能提供。

## 2. 目标

- 固化 `default.ws` 下的一级目录语义。
- 保持现有相对路径兼容：相对路径默认基于 `ctx.cwd` 根目录解析，而不是自动进入 `business/`。
- 建立 `system/` 只读边界：Agent 可通过文件工具查看，但不能创建、修改或删除。
- 为后续前端管理 `system/agents/*/AGENT.md`、`SOUL.md`、`system/skills/`、`system/templates/` 留出清晰边界。
- 明确哪些运行时产物未来应该进入 `runtime/`，但第一版不强制迁移所有产物。

## 3. 非目标

- 不改变当前仓库级 `memory/`、`assets/` 资源模型。
- 不把普通相对路径默认重定向到 `business/`。
- 不在本 spec 中实现多 Agent 调度。
- 不定义 `AGENT.md` / `SOUL.md` 的完整内容格式，只定义它们的存放位置和权限边界。
- 不引入数据库、索引服务、远程同步或多用户 ACL。
- 不要求本轮实现前端管理页面；这里只为后续前端写入能力划边界。

## 4. 目录语义

### 4.1 工作区根

`workspace/default.ws/` 是一个具体工作区实例。后续可支持多个 `*.ws` 工作区，但第一版以 `default.ws` 为默认实例。

当 Agent 使用该工作区时：

```text
ctx.cwd = <project_root>/workspace/default.ws
```

相对路径解析规则：

| 输入路径 | 解析目标 |
|---|---|
| `foo.txt` | `<ctx.cwd>/foo.txt` |
| `business/foo.txt` | `<ctx.cwd>/business/foo.txt` |
| `runtime/out.json` | `<ctx.cwd>/runtime/out.json` |
| `system/agents/main/AGENT.md` | `<ctx.cwd>/system/agents/main/AGENT.md` |

### 4.2 `business/`

`business/` 用于业务文件、用户素材、任务输入输出、需要由 Agent 直接操作的业务产物。

第一版不把普通相对路径自动落入 `business/`，原因是：

- 保持现有 workspace 相对路径行为。
- 避免模型和用户看到的路径与实际写入位置不一致。
- 允许用户显式使用 `business/foo.txt` 表达业务文件归属。

### 4.3 `runtime/`

`runtime/` 用于运行时产物，例如：

- `plan.md` 或未来的任务计划文件。
- `web_execute_js.save_to_file` 的长结果。
- 代码执行产生的临时输出。
- 会话日志、任务中间缓存、可清理的临时文件。

第一版不强制迁移现有 `plan_update` 或浏览器保存路径，但后续迁移应优先把自动生成文件放入 `runtime/`，避免污染工作区根和业务目录。

### 4.4 `system/`

`system/` 是工作区级系统资源区。它不属于普通业务文件，也不应由 Agent 文件工具直接改写。

子目录语义：

| 路径 | 用途 |
|---|---|
| `system/agents/<name>/AGENT.md` | Agent 角色职责、工具边界、执行规范。 |
| `system/agents/<name>/SOUL.md` | Agent 风格、人格、长期行为偏好。 |
| `system/memory/` | 工作区级记忆资源。 |
| `system/skills/` | 工作区级技能资源。 |
| `system/templates/` | 工作区级模板资源。 |

## 5. 权限模型

本 spec 叠加在 [workspace-file-permission-spec.md](workspace-file-permission-spec.md) 之上。

### 5.1 路径分类

在工作区内新增系统区分类：

| 分类 | 定义 |
|---|---|
| `WORKSPACE_ROOT` | 解析后路径等于 `ctx.cwd` 或位于 `ctx.cwd` 下，且不在 `system/` 下。 |
| `WORKSPACE_SYSTEM` | 解析后路径位于 `<ctx.cwd>/system` 下。 |
| `OUTSIDE_WORKSPACE` | 解析后路径位于 `ctx.cwd` 外。 |

### 5.2 操作权限

| 操作 | `WORKSPACE_ROOT` | `WORKSPACE_SYSTEM` | `OUTSIDE_WORKSPACE` |
|---|---|---|---|
| 查看 | 允许 | 允许 | 允许 |
| 创建 | 允许 | 拒绝 | 拒绝 |
| 修改 | 允许 | 拒绝 | 拒绝 |
| 删除 | 需用户授权 | 拒绝 | 第一版拒绝 |

关键规则：

- Agent 文件工具可以读取 `system/` 下文件，方便模型理解角色配置和模板。
- Agent 文件工具不能创建、修改、patch 或删除 `system/` 下文件。
- 即使用户通过 `ask_user` 授权删除，`system/` 删除仍应拒绝。
- `system/` 写入能力后续只能通过前端管理功能或专门管理 API 提供。

## 6. 当前代码路径影响

后续实现应重点触达：

- `src/core/XAgent.py::resolve_workspace_dir()`：默认工作区需要从 `<project_root>/workspace` 过渡到 `<project_root>/workspace/default.ws`，并创建标准子目录。
- `src/tools/file_ops.py::resolve_path_for_operation()`：在当前工作区内外权限判断基础上，新增 `system/` 写保护。
- `src/tools/file_ops.py::{write_file, patch_file, delete_file}`：应继承 `system/` 写保护。
- `src/tools/browser_driver.py::save_result()`：写入行为应继续受工作区写权限约束；未来可默认建议保存到 `runtime/`。
- `src/tools/interaction.py::plan_update()`：未来可考虑把 `plan.md` 放入 `runtime/plan.md`，但本 spec 不强制迁移。
- `src/handler/XAgentHandler.py`：不应绕过 `file_ops` 的路径权限；删除授权后仍必须走纯函数删除边界。
- 前端管理功能：未来负责 `system/` 下文件的创建和修改。

## 7. 错误处理

对 `system/` 写操作应返回权限专属错误，而不是泛化为 path escape。

示例：

```json
{
  "status": "ERROR",
  "error": "workspace system files are read-only for agent file tools",
  "path": "/repo/workspace/default.ws/system/agents/main/AGENT.md",
  "workspace": "/repo/workspace/default.ws",
  "operation": "write"
}
```

删除 `system/` 文件时，即使用户已授权，也应返回：

```json
{
  "status": "ERROR",
  "error": "workspace system files cannot be deleted by agent file tools",
  "path": "/repo/workspace/default.ws/system/agents/main/SOUL.md",
  "workspace": "/repo/workspace/default.ws",
  "operation": "delete"
}
```

## 8. 测试要求

聚焦测试应覆盖：

- 默认工作区解析为 `<project_root>/workspace/default.ws`。
- 启动时自动创建 `business/`、`runtime/`、`system/agents/main/`、`system/agents/coding/`、`system/memory/`、`system/skills/`、`system/templates/`。
- `file_write("foo.txt")` 写入 `<ctx.cwd>/foo.txt`，不写入 `business/foo.txt`。
- `file_read("foo.txt")` 从 `<ctx.cwd>/foo.txt` 读取。
- `file_read("system/agents/main/AGENT.md")` 允许。
- `file_write("system/agents/main/AGENT.md")` 拒绝。
- `file_patch("system/agents/main/AGENT.md")` 拒绝。
- `file_delete("system/agents/main/AGENT.md")` 即使获得用户授权也拒绝。
- `web_execute_js.save_to_file="system/output.json"` 拒绝。
- `web_execute_js.save_to_file="runtime/output.json"` 允许。

兼容性测试应覆盖：

- 显式传入旧式 workspace 目录时，是否需要自动补 `default.ws`，由实现方案明确并测试。
- 工作区外读取仍保持允许。
- 工作区外写入仍保持拒绝。

## 9. 验收标准

- `ctx.cwd` 指向具体工作区实例根，例如 `workspace/default.ws`。
- 普通相对路径仍解析到 `ctx.cwd` 根目录。
- `business/` 不被隐式作为默认目录。
- `system/` 对 Agent 文件工具只读。
- `system/` 写入职责明确留给前端管理能力。
- 错误返回可诊断，包含 path、workspace、operation 和权限原因。
- 不引入长期权限状态或数据库依赖。

## 10. 收益与风险

收益：

- 工作区结构更清晰：业务文件、运行时文件、系统资源分区明确。
- 保持现有相对路径心智模型，降低迁移风险。
- 防止 Agent 通过文件工具误改系统配置、角色文件和工作区级技能。
- 为前端管理系统资源预留干净边界。

风险：

- `business/` 不是默认目录，用户可能疑惑为什么它存在但普通文件不自动进入其中；需要在 UI 或文档中解释。
- `runtime/` 第一版如果不强制使用，短期仍可能有运行产物落在根目录。
- `code_run` 仍可能通过 OS 能力写入 `system/`，除非后续增加子进程沙箱或代码执行策略。
- 多工作区选择、命名、迁移策略尚未定义，需要后续独立 spec。
