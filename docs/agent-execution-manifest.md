# Agent Activity Execution Manifest 契约

## 1. 目的

`AgentActivityReceipt` 的 v1 `internal_tool_receipts_complete` 只能比较“观察到的工具结果数”
与 receipt digest 数。它不能阻止以下替换：

- 重排合法 ToolReceipt；
- 重复使用同一 ToolReceipt；
- 引用同一 Run 中另一个 Agent/分支产生的 ToolReceipt；
- 把 `secret` Agent request 的摘要元数据降级写入 `sensitive` Artifact。

`AgentActivityExecutionManifest` 是后续远程 Agent runtime 的最小证据前置。它把一个父
Agent Attempt、精确 request Artifact、Workflow definition、终止观察，以及有序子
ToolReceipt 链绑定为 canonical、payload-free Artifact。当前生产远程能力仍保持
Tool-only；manifest 存在不自动开放 `agent` capability。

## 2. 证据链

```text
AgentActivityRequest Artifact
  run/node/attempt + request + definition + sensitivity
                    |
                    v
Agent runtime -> deterministic child operation key per sequence
                    |
                    v
ordered ToolReceipt bindings
  child identity + action + operation/idempotency + receipt digest
                    |
                    v
AgentActivityExecutionManifest Artifact
                    |
                    v
AgentActivityReceipt v2
  result digest + exact manifest digest + exact ToolReceipt digests
```

manifest 不保存 task、prompt、response、工具参数、工具结果、异常消息、路径、URL、bearer
或 provider credential。所有大内容仍先进入受分类的不可变 Artifact。

## 3. 父子 operation key

第 `sequence` 个内部工具调用必须使用：

```text
agent-tool:<sha256(canonical parent binding)>
```

parent binding 包含 schema、父 `run_id/node_id/attempt_id`、父 request digest 和从 1
开始的 sequence。Tool Activity 的 operation key 与 idempotency key 必须相同；
manifest 校验 ToolReceipt 中两者的 digest 均等于该确定性 key 的 SHA-256。

因此：

- 同一父 Attempt 崩溃重启后，同一 sequence 得到同一幂等身份；
- 换父 Attempt、换 request 或换 sequence 都得到不同身份；
- 同一 Run 中无关分支的合法 ToolReceipt 不能被拼接进 manifest；
- 交换两个 ToolReceipt 会同时破坏 sequence 与 operation-key 绑定。

当前 manifest 只允许同一 Run 的子 ToolReceipt。跨 child Run 的 Agent 工具编排必须先
增加独立的 durable hierarchy proof，不能用弱化本约束的方式兼容。

## 4. Schema v1

顶层字段：

| 字段 | 约束 |
|---|---|
| `run_id/node_id/attempt_id` | 父 Agent Attempt 的有界身份 |
| `request_digest` | Scheduler 业务请求 digest |
| `request_artifact_digest` | canonical Agent request Artifact SHA-256 |
| `definition_digest` | immutable Workflow definition digest |
| `artifact_sensitivity` | 仅 `sensitive/secret`，与 request Artifact 精确一致 |
| `exit_reason/turns` | 有界终止观察 |
| `observed_tool_results` | runtime 观察到的工具结果数 |
| `tool_receipts_complete` | 是否逐项完整覆盖 |
| `tool_receipts[]` | 最多 64 个有序绑定 |

每个 ToolReceipt 绑定包含：

- 连续 `sequence` 与非递减 `turn`；
- child `run_id/node_id/attempt_id` 和安全 tool name；
- action、operation key、idempotency key、logical tool call 和 ToolReceipt digest。

receipt digest、logical call digest和 child Attempt 身份均不得重复。完整覆盖要求
`len(tool_receipts) == observed_tool_results`；partial manifest 允许少于观察数，但不能
多于观察数。存在工具绑定时必须有 turn count，且每个 turn 不得越界。

canonical UTF-8 JSON 和 Artifact 均不得超过 256 KiB。解析采用 exact-field schema，
拒绝 bool-as-int、NaN、未知/缺失字段、非规范编码、控制字符和无界集合。

## 5. AgentActivityReceipt v2

v2 增加 `execution_manifest_digest`：

- 只要 v2 引用任一 ToolReceipt，就必须绑定 manifest；
- manifest digest 必须在 `result_artifact_digests` 中恰好出现一次；
- v2 声明 `internal_tool_receipts_complete=true` 时必须有 manifest；
- `has_manifest_bound_tool_receipt_lineage` 只表示 receipt 已绑定 manifest digest，
  不表示 manifest bytes 已加载或外部副作用已验证；
- runtime/control 必须实际加载 manifest，验证 request/ref、全部 ToolReceipt 和
  `AgentActivityReceipt` 后，才可接纳新远程 Agent 终态。

Store 的 receipt 查询会额外要求 NodeResult 中存在唯一、canonical、
`kind=agent_execution_manifest` 的 ref，并校验 schema metadata、sensitivity 和精确
producer identity。Store 不持有 ArtifactStore reader，因此只验证 durable ref；
需要信任内容的入口必须调用 `AgentExecutionManifestArtifactStore.load()`。

v1 receipt 继续按原 exact schema 读取，保持历史兼容。v1 的
`internal_tool_receipts_complete=true` 仍只是旧的 count-only 语义，不能用于新远程门禁。

## 6. Artifact 与恢复

manifest Artifact：

- `kind=agent_execution_manifest`；
- media type 为
  `application/vnd.xagent.agent-execution-manifest+json`；
- metadata 仅 `{"schema":"agent_execution_manifest_v1"}`；
- producer 精确绑定父 Agent Run/Node/Attempt；
- 分类继承 request Artifact，只允许 `sensitive/secret`；
- content address 保证并发 staging 和响应丢失后的重试收敛到同一内容身份。

load 顺序是 ref 结构校验、Store verify、读取 bytes、size/SHA 校验、canonical 解析、
request/receipt/ToolReceipt 绑定校验。verify 失败时不得继续读取内容。

## 7. 仍未承诺

- 当前 `SecureRemoteAssignmentAdmitter`、Worker adapter 和 Fleet projector 仍为
  Tool-only。
- manifest 不证明 sandbox 禁止了绕过 Tool/Provider gateway 的直接网络或文件副作用；
  远程 Agent runtime 必须由 attested sandbox 和 egress policy 独立证明。
- `ToolReceipt.verification` 强度必须逐项判断。manifest 完整不等于 write exactly-once。
- provider invocation 需要独立的有序 receipt/operation evidence，不能伪装成 ToolReceipt。
- Artifact digest 不是抗 ArtifactStore/数据库管理员的数字签名；生产仍需独立 OS 身份、
  ACL、加密和可选透明日志。

三轮攻击复现、修复与测试见
[Agent Execution Manifest 三轮对抗性审查](agent-execution-manifest-adversarial-review.md)。
