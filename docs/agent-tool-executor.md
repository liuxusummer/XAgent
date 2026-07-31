# 动态 Agent Tool 执行器契约

## 1. 目标

`DurableAgentToolExecutor` 为父 Agent Attempt 在运行时产生的 Tool call 提供真实、持久、
可审计的 child authority。动态调用不是静态 Workflow DAG 的组成部分：强行伪造 Node 会污染
拓扑、重试和 Run 聚合语义。因此实现使用同一个 `DurableRunStore` 中的独立
`agent_tool_invocations` 账本，同时把所有状态变化追加到父 Run 的 Domain Event 链。

稳定 identity 由父 `run_id/node_id/attempt_id/sequence` 派生；同一 observation 的重放得到同一
invocation、child node、child attempt 和 operation digest，不同绑定复用同一序号会失败关闭。

## 2. 状态机

```text
SCHEDULED
  -> RUNNING -> SUCCEEDED | FAILED | TIMED_OUT | CANCELLED | OUTCOME_UNKNOWN
  -> FAILED                 # deny / require_approval，sandbox 未启动
  -> ABANDONED              # preflight 失败，未签发 authority

RUNNING --lease expired--> OUTCOME_UNKNOWN
```

- `SCHEDULED` 不含 owner、claim token、policy authority 或 receipt；
- `RUNNING` 只持久化 claim token digest，原始 bearer 仅存在于进程内 claim；
- policy 非 ALLOW 会写 terminal failure receipt，且没有 owner/token/lease；
- terminal row 保存 canonical `ToolReceipt`；只有 `SUCCEEDED` 可以保存
  `AgentToolResult` ArtifactRef；
- 父 Agent Attempt 和 Run 在仍有 `SCHEDULED/RUNNING` child 时禁止终结。

## 3. 执行顺序

1. Store 事务验证父 Run/Agent Node/Agent Attempt 均为 `RUNNING`，并原子 reserve identity；
2. deployment-owned `AgentToolExecutionSpec` 生成无 shell 字符串的 argv，验证 control-plane
   路径隔离、Artifact、脚本、环境、capability、resource lock 和敏感控制值；
3. 构造完整 `ActionRequest`/`ExecutionRequest`，由同一个 `PolicyEngine` 判定；
4. deny/require-approval 在 Store 中直接终结，绝不调用 sandbox；
5. ALLOW 将 action/execution/policy/profile/decision digest、owner、token digest 和短期 lease
   原子写入 `agent_tool.started`，之后才允许 dispatch；
6. sandbox 必须返回绑定同一 action/policy/profile 的 typed `SandboxReceipt`；
7. success 必须恰有一个 canonical Agent Tool result Artifact，producer、分类、完整性均与
   child 匹配；
8. Store 比较原始 claim 的 token digest 与未过期 lease，在一个事务中写 terminal row 和
   `agent_tool.<status>` Event；执行器随后从 Store 重读 receipt/result 才返回 Handler。

## 4. 崩溃与竞态

- reserve 后、start 前崩溃：同一请求可继续完成；preflight 异常会转为无 authority 的
  `ABANDONED`；
- start 后、backend 前崩溃：lease 到期后只允许转为 `OUTCOME_UNKNOWN`；
- backend 返回后、terminal commit 前崩溃：不得再次执行；lease 到期后仍转为
  `OUTCOME_UNKNOWN`；
- completion 与 expiry 在边界并发：`lease_expires_at <= now` 时 expiry 胜出，迟到 receipt
  不能覆盖终态；
- 父 Agent lease recovery 会在同一事务中先清理 child：未启动 child 变为 `ABANDONED`，
  已过期 RUNNING child 变为 `OUTCOME_UNKNOWN`，仍持有有效 lease 的 child 阻止父回收；只要
  任一 child outcome unknown，父 Agent 的较弱 retry/fail 决议会被提升为 `WAITING_RECOVERY`；
- 已完成请求：跨进程重建 executor 后直接重放同一 durable receipt/result，sandbox 不再调用；
- 活跃请求：并发重复调用返回固定 `agent_tool_active`，不会产生第二次副作用。

这是一种保守的 at-least-once/unknown-outcome 模型，不宣称跨 SQLite 与外部系统的
exactly-once。

## 5. 部署边界

`durable_result_recovery_ready` 只在 SQLite Store 与可证明持久的 ArtifactStore 组合时为 true。
内存 ArtifactStore 被拒绝。reference `LocalArtifactStore` 不支持 secret 加密；自定义 secret
Store 必须明确声明 durability，并由 `AgentToolResultArtifactStore` 验证 deployment-managed
encryption。

`production_security_ready` 仍为 false：项目内 reference composition 不证明远程 Worker
mTLS、runtime attestation、sandbox 镜像 provenance、egress policy、secret-manager、外部
幂等账本或跨主机共识。`REQUIRE_APPROVAL` 当前安全终结并返回固定错误，尚未实现动态 child
的暂停/恢复审批状态机。
