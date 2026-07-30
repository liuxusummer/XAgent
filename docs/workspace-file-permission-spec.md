# 工作区文件权限 Spec

## 1. 问题定义

XAgent 已经有工作区概念：`ctx.cwd` 指向当前 Agent 工作区，相对路径默认基于该目录解析，默认工作区是 `<project_root>/workspace`。

工作区外文件可能包含凭证、隐私数据或提示注入内容。若 `file_read` 静默允许模型读取任意宿主路径，网页或文件中的恶意内容就可能扩大数据访问范围。

目标行为是：

- 工作区内文件允许创建、读取、修改、删除。
- 工作区外文件只允许具有本地 operator `host.read` 能力的会话在用户逐次授权后查看。
- 多用户安全 Web 的注册表不能授予 `host.read`，租户会话必须直接拒绝工作区外读取。
- 无论文件在工作区内还是工作区外，删除文件都必须先获得用户明确授权。

本文档定义该行为的第一版实现边界。

## 2. 目标

- 引入基于工作区归属和操作类型的文件访问策略。
- 一等文件查看工具访问 `ctx.cwd` 外部路径前必须逐次获得用户授权。
- 保持创建、修改操作只能发生在 `ctx.cwd` 内。
- 任何一等文件删除操作执行前都必须获得用户授权。
- 保持现有工作区相对路径语义不变。
- 返回可诊断的权限错误，使 Agent 能决定是否询问用户、换路径或停止。

## 3. 非目标

- 不改变 `ctx.cwd` 语义。它仍表示 Agent 工作区，不表示代码仓库根目录。
- 不把 `memory/`、`assets/` 等仓库资源迁移到工作区模型下。
- 不引入数据库权限、ACL 文件或长期授权状态。
- 不做超出删除授权所需的工具 Schema 大改。
- 显式开发兼容模式 `XAGENT_CODE_RUN_BACKEND=unsafe` 不属于生产隔离保证。
- 工作区内普通创建、修改操作不要求用户授权。

## 4. 当前代码路径

相关路径：

- `src/core/XAgent.py::resolve_workspace_dir()` 创建并规范化工作区。
- `src/core/XAgent.py::XAgent.__post_init__()` 将解析后的工作区写入 `workspace_dir` 和 `cwd`。
- `src/handler/XAgentHandler.py::exec_file_read()` 调用 `read_file(..., cwd=self.ctx.cwd)`。
- `src/handler/XAgentHandler.py::exec_file_write()` 调用 `write_file(..., cwd=self.ctx.cwd)`。
- `src/handler/XAgentHandler.py::exec_file_patch()` 调用 `patch_file(..., cwd=self.ctx.cwd)`。
- `src/tools/file_ops.py::resolve_path()` 当前强制所有解析后路径必须位于 `cwd` 内。
- `src/tools/browser_driver.py::save_result()` 也通过 `resolve_path()` 处理 `web_execute_js.save_to_file`。
- `src/tools/code_sandbox.py` 生成并验证不可变隔离计划，`src/tools/code_run.py` 只按该计划启动和监督进程树。

## 5. 权限模型

### 5.1 路径分类

一等文件工具使用的每个路径都必须先规范化为绝对解析路径，再进行权限判断。

路径分类：

| 分类 | 定义 |
|---|---|
| `WORKSPACE` | 解析后路径等于 `ctx.cwd`，或位于 `ctx.cwd` 内。 |
| `OUTSIDE_WORKSPACE` | 解析后路径位于 `ctx.cwd` 外。 |

相对路径始终基于 `ctx.cwd` 解析，因此通常会被分类为 `WORKSPACE`；如果包含 `../` 等路径穿越片段，则可能解析到 `OUTSIDE_WORKSPACE`。

绝对路径可能被分类为 `WORKSPACE`，也可能被分类为 `OUTSIDE_WORKSPACE`。

### 5.2 操作分类

| 操作 | 示例 | 工作区内 | 工作区外 |
|---|---|---|---|
| 查看 | `file_read`、目录 listing | 允许 | 仅本地 operator：`host.read` + 逐次授权；安全 Web 租户拒绝 |
| 文件引用展开 | `{{file:...}}` | 允许 | 拒绝 |
| 创建 | `file_write` 写入不存在文件、创建父目录 | 允许 | 拒绝 |
| 修改 | `file_write` 覆盖/追加/前插、`file_patch` | 允许 | 拒绝 |
| 删除 | 后续 `file_delete`、如增加递归删除 | 需要用户授权 | 需要用户授权；第一版仍拒绝，除非后续批准的删除工具显式支持 |

删除有两道门：

1. 用户必须明确授权本次删除。
2. 删除实现仍必须执行自身配置的路径范围校验。

第一版删除能力只应支持工作区内路径，并且必须先获得用户授权。工作区外删除即使模型请求，也应返回明确拒绝；除非后续经过批准的 spec 扩展该范围。

## 6. 契约

### 6.1 路径解析契约

将单一职责的 `resolve_path(path, cwd)` 行为替换为面向操作类型的路径解析。

概念 API：

```python
def resolve_path_for_operation(path: str, cwd: str | None, operation: str) -> Path:
    ...
```

要求行为：

- 使用 `Path.resolve()` 规范化 `cwd` 和 `path`。
- 相对路径基于 `cwd` 解析。
- `operation="read"` 时默认只允许 `WORKSPACE`；Handler 完成逐次授权后，才可通过内部标记读取对应的规范化 `OUTSIDE_WORKSPACE` 路径。
- `operation in {"create", "update", "write", "patch"}` 时拒绝 `OUTSIDE_WORKSPACE`。
- 错误信息应包含解析后路径、工作区路径、操作类型和拒绝原因。

错误示例：

```text
path outside workspace is read-only: /tmp/example.txt (workspace: /repo/workspace, operation: write)
```

### 6.2 读取契约

`file_read` 必须支持：

- 读取工作区内文件。
- 列出工作区内目录。
- 具有本地 operator `host.read` 时，用户逐次授权后读取工作区外文件。
- 具有本地 operator `host.read` 时，用户逐次授权后在 OS 权限允许范围内列出工作区外目录。
- 安全 Web 注册表拒绝 `host.read`（包括规范化后的空白变体），租户确认不能提升能力。
- 保持现有截断、行范围读取行为。

底层 `read_file` 默认拒绝工作区外路径；授权由 Handler 负责，授权只对提示中展示的规范化绝对路径和本次调用生效。访问工作区外路径时不得创建文件、目录、缓存、旁路元数据或任何其他写入产物。

### 6.3 写入与 Patch 契约

`file_write` 和 `file_patch` 只能修改 `ctx.cwd` 内文件。

如果目标路径位于工作区外，必须返回 `status="ERROR"` 和权限相关错误。它们不得在工作区外创建父目录。

`{{file:path:start:end}}` 是写入工具内部的隐式读取，无法单独向用户展示并授权，因此只能展开工作区内文件。工作区外引用必须先通过显式 `file_read` 获得授权，再由用户决定是否将内容写入工作区。

普通文件写入、追加、前插、patch、删除和浏览器结果保存必须持有统一工作区写锁；文件内容使用同目录临时文件加原子替换，避免并发读—改—写丢失更新或留下半写文件。

### 6.4 删除契约

如果增加一等删除工具，它必须在删除任何内容前获得用户明确授权。

用户交互由 Handler 负责，而不是由 `tools/file_ops.py` 负责；因为 `tools/` 下的工具应保持纯函数，不持有 `AgentContext.user_input_fn`。

预期流程：

```text
exec_file_delete(args)
  -> 解析目标路径并分类
  -> ask_user("Authorize deletion of <path>?")
  -> 用户拒绝或跳过：返回 SKIP/ERROR，不删除
  -> 用户授权：调用纯函数 delete 工具
  -> 返回删除路径和元数据
```

授权必须按次生效。一次授权不得形成长期权限 grant。

展示给用户的授权提示必须包含：

- 将被删除的绝对路径。
- 路径位于工作区内还是工作区外。
- 是否为递归删除。
- 简短说明：删除无法由系统自动回滚。

### 6.5 浏览器保存契约

`web_execute_js.save_to_file` 会把工具输出写入磁盘，因此遵循写入契约：

- 相对保存路径基于 `ctx.cwd` 解析。
- 指向 `ctx.cwd` 外的绝对保存路径会被拒绝。

### 6.6 代码执行边界

`code_run` 的生产默认模式要求经过功能探测的 OS sandbox：工作区只读可见，控制面目录和根级控制文件被隐藏，私有临时目录可写，网络默认拒绝，并限制进程、CPU、内存、文件和输出资源。隔离后端不可用时直接拒绝，不回退到宿主进程。

默认执行策略为 `XAGENT_CODE_RUN_POLICY=confirm`：每次执行都必须取得用户明确授权；`deny` 可完全禁用。`XAGENT_CODE_RUN_BACKEND=unsafe` 仅用于显式开发兼容，并且即使策略为 `allow` 仍需逐次审批。所有模式都使用最小环境，宿主 API key、代理、SSH agent 等敏感变量不得透传。

逐次授权与 OS sandbox 是两道独立门禁：前者表达调用意图，后者限制实际可见资源；任何一层都不能替代另一层。

## 7. 错误处理

权限错误应结构化且可诊断：

```json
{
  "status": "ERROR",
  "error": "path outside workspace is read-only",
  "path": "/absolute/path",
  "workspace": "/absolute/workspace",
  "operation": "write"
}
```

删除未授权应返回独立状态或原因，例如：

```json
{
  "status": "SKIP",
  "error": "delete not authorized by user",
  "path": "/absolute/path"
}
```

## 8. 测试

聚焦测试应覆盖：

- 工作区内相对路径读取成功。
- 工作区内绝对路径读取成功。
- 工作区外绝对路径默认拒绝。
- 本地 operator 具有 `host.read` 且逐次授权后，工作区外文件读取或目录 listing 成功。
- 安全 Web Principal 在 Policy preflight 和 Handler 执行层均无法读取工作区外路径。
- 工作区外写入在创建父目录前被拒绝。
- 工作区外 patch 在读取并修改目标文件前被拒绝。
- 写入工作区内目标时，`{{file:outside:path}}` 被拒绝。
- 并发 append 不丢失任何成功写入，覆盖写与 patch 不产生半写文件。
- `web_execute_js.save_to_file` 拒绝工作区外保存目标。
- 删除工作区内文件时会询问用户；用户跳过或拒绝时不删除。
- 删除工作区内文件只在用户授权后执行。
- 第一版中，工作区外删除保持拒绝。

兼容性测试应验证现有工作区行为：

- 默认工作区仍是 `<project_root>/workspace`。
- Prompt 注入仍说明相对路径基于 workspace 解析。
- 对工作区路径，现有 `file_read`、`file_write`、`file_patch` 成功返回结构保持不变。

## 9. 验收标准

- 工作区仍是一等工具唯一允许创建或修改文件的位置。
- 一等文件读取工具只有在本地 operator 同时具有 `host.read` 且逐次授权后，才可查看工作区外绝对路径；安全 Web 租户始终拒绝。
- 任何一等文件删除路径在删除前都会触发用户授权往返。
- 被拒绝的操作返回权限专属诊断，而不是泛化的 path escape 错误。
- 现有基于工作区相对路径的文件工作流继续可用。
- 实现不新增长期权限状态。

## 10. 收益与风险

收益：

- Agent 可以在用户明确知情后检查工作区外文件，而不必先复制到工作区。
- 默认阻止意外写入工作区外文件。
- 删除从模型单方决策变成明确的用户参与动作。

风险：

- 授权读取工作区外文件仍会扩大 Agent 可查看的本地信息范围，因此授权提示必须显示规范化绝对路径，工具结果始终按不可信数据处理。
- 私有 scratch 总量由父进程周期监督，存在短暂超限窗口；生产部署仍需在目标 OS 验证 sandbox、资源限制和进程树终止。
- 如果后续引入递归删除，需要额外处理符号链接和路径分类问题。
