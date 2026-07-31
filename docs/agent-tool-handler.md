# Durable Agent Tool Handler 契约

## 1. 目标与边界

`DurableAgentToolHandler` 把 Core Agent Loop 的动态
`dispatch(tool_name, args) -> ActionResult` 调用接到部署侧持久 Tool executor。它只接受：

1. collector 当前打开的真实 Tool observation；
2. allowlist 中的精确工具名；
3. 已从 ArtifactStore 重读并验证的父 `AgentActivityRequest`；
4. 与父 Attempt、序号、工具和参数完全绑定的 durable `ToolReceipt`；
5. 成功时由同一 child Attempt 产生、进入 SandboxReceipt 输出集合且通过 Store 完整性验证的
   canonical `AgentToolResult` Artifact。

Handler 不执行 shell、浏览器或文件操作，也不自行授予能力。真正的策略判定、child Attempt
创建、sandbox 启动、幂等账本和 durable terminal commit 属于部署侧 `AgentToolExecutor`。

本类的 `production_security_ready` 固定为 false，因为 Handler 本身只负责消费。
`DurableAgentToolExecutor` 现已通过独立的 `agent_tool_invocations` 账本为 Agent Loop
运行时产生的 tool call 签发同一 Run 内的 child authority；它不伪造 Workflow DAG Node，
并把 policy、sandbox、Artifact、receipt 与 child terminal Event 串成可重放链。完整发行契约见
[动态 Agent Tool 执行器](agent-tool-executor.md)。

## 2. 构造前置条件

一个 Handler 实例只服务一个确切的父 Agent Attempt。构造时必须同时满足：

- request Artifact 可从传入 Store 验证、读取并还原为同一个 `AgentActivityRequest`；
- request/ref/collector 的 Run、Node、Attempt、request digest、definition digest 与分类一致；
- Core `AgentContext.execution_evidence_observer` 就是该 collector；
- executor 明确声明 `durable_result_recovery_ready is True`；
- 工具列表非空、去重、有界，名称和敏感参数 key 使用安全字符集；
- `secret` 父 request 必须由 deployment-managed encryption 保护，不能把本地明文标签当加密。

任一前置条件失败都在读取 Tool 参数或调用 executor 之前以固定 reason code 关闭。

## 3. 调用顺序

每次动态 Tool 调用严格按以下顺序执行：

1. per-handler non-blocking single-flight guard 拒绝同一父 Attempt 内的并发执行；
2. 从 collector 取得当前 observation 的 sequence、turn、tool name、call ID 和父派生
   operation key；
3. 校验工具 allowlist，canonical 化参数并计算与 `ActionRequest` 相同的 redacted digest；
4. 发现任意内联 secret/token/password 类值即拒绝，credential 只能由部署侧受信环境绑定；
5. 深复制并冻结参数树，防止 executor 在 digest 之后改写嵌套对象；
6. 调用 deployment-owned executor；普通异常映射为固定错误，不泄露正文或 cause；
7. 校验返回 receipt 的父 Run、不同 child Node/Attempt、工具名、参数 digest、operation 与
   idempotency digest；
8. 从 `DurableRunStore.get_tool_receipt()` 重读同一 receipt，返回值与持久终态必须完全相等；
9. 成功时校验 result Artifact；失败或不确定时只生成固定、有界的诊断结果；
10. 返回携带 out-of-band typed receipt 的 `ActionResult`，由 Agent Loop observer 完成证据收集。

同一 Handler 不共享进程级全局锁；不同 Agent Attempt 可以并发。Core 仍保持一个 Loop 内工具
串行执行的安全默认。

## 4. 绑定矩阵

| 对象 | 必须绑定 |
|---|---|
| invocation | 父 Run/Node/Attempt、request/ref digest、sequence、turn、tool、call ID digest、args digest、operation key、分类 |
| ToolReceipt | 同一 Run、不同 child Node/Attempt、同一 tool/args、父派生 operation 与 idempotency digest |
| durable Store | 同一 Run/child Attempt 的 terminal Event 中必须能重建完全相同的 receipt |
| result Artifact | child producer 三元组、至少父 request 的分类、ToolResult kind、专用 media type、空 metadata、canonical bytes |
| SandboxReceipt | `output_artifact_refs` 中必须恰好出现一次 result 的 artifact ID/SHA/size/kind |

receipt 的 action、execution binding、policy、profile 和 sandbox 字段仍由 Tool executor 的
发行/验证路径负责。operation key 在父 Attempt 内唯一，Handler 不把不完整的自制
`ActionRequest` 冒充完整策略授权。

## 5. AgentToolResult Artifact

结果 wire 是 exact-field canonical UTF-8 JSON：

```json
{
  "data": {"status": "OK"},
  "flags": [],
  "kind": "agent_tool_result",
  "next_prompt": "continue",
  "schema_version": 1,
  "should_exit": false
}
```

约束包括：

- 最大 1 MiB、深度 24、65,536 个节点、单字符串 1 MiB；
- 拒绝重复 key、未知字段、非 canonical 编码、NaN/Infinity、循环和任意 Python 对象；
- `next_prompt` 最大 64 KiB；
- flags 只允许 `reset_tools` 和 `retry`；
- Artifact 只能是 `sensitive` 或 `secret`；`secret` 必须使用 deployment-managed encryption；
- 参考 `LocalArtifactStore` 在写入前拒绝 secret，不能先落明文再做事后检查。

`next_prompt`、`should_exit` 和 flags 是控制字段，必须由部署侧受信 adapter 根据工具类型生成；
不能把任意 sandbox stdout 直接解释为这些字段。原始 Tool 内容仍作为不可信 `data` 进入 Core
已有的 `<untrusted_tool_results>` 边界。

## 6. 失败与恢复语义

- executor 只有在 child terminal Event、ToolReceipt 和结果 Artifact 已持久化后才能返回；
- Handler 永远从 Store 重读 receipt，不能只相信进程内返回对象；
- `SUCCEEDED` 必须有 result Artifact；非成功终态禁止携带 result Artifact；
- `cancelled`、`abandoned`、`outcome_unknown` 立即停止 Agent Loop，避免在未知副作用后继续；
- 可重试失败只暴露固定安全 reason code；自由文本错误退化为 `tool_execution_failed`；
- 同一 Handler 内重复 child attempt/receipt 被拒绝，避免把旧终态作为新的 observation 使用。

恢复并不意味着 Handler 自己重跑 Tool。executor 必须通过 durable ledger 判断 replay、
NOT_STARTED 或 COMPLETED；Handler 只消费收敛后的唯一终态。

## 7. 已完成与剩余边界

- reference control plane 已实现动态 child identity、策略拒绝、ALLOW authority、短期 claim
  lease、sandbox dispatch、terminal receipt/result 原子提交和 durable replay；
- 进程在授权后崩溃时不会重放未知副作用；租约到期后原子收敛为 `OUTCOME_UNKNOWN`，迟到
  receipt 被 fencing 拒绝；
- preflight 失败会以无执行权限的 `ABANDONED` 终态清理 reservation，避免永久阻塞父 Attempt；
- 仍缺少该 issuer 与远程 Worker 的 mTLS/attestation/egress/secret-manager 生产组合；
- provider history、tool result、manifest、checkpoint、NodeResult、Agent receipt 与 terminal Event
  尚未组成单一 crash-consistent transaction；
- reference Store 不提供 encrypted secret Artifact；
- reference executor readiness 不证明 mTLS、attestation、egress、外部幂等账本或跨主机共识；
- result 控制 envelope 的生产者身份尚未由独立签名或 attestation 证明。

因此 Handler + reference executor 已消除动态 child authority 缺口，但不能单独开放 remote
`agent` capability。消费边界审查见
[Durable Agent Tool Handler 三轮对抗性审查](agent-tool-handler-adversarial-review.md)，发行与恢复审查见
[Durable Agent Tool Executor 三轮对抗性审查](agent-tool-executor-adversarial-review.md)。
