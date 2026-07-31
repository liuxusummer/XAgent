# Agent Activity Receipt 契约

## 1. 目的

一次 Agent Activity 可以跨越多轮模型调用和多次工具调用。仅保存最终 `NodeResult`
无法回答三个恢复问题：

1. 该结果是否属于精确的 Run、Node、Attempt 和请求；
2. 编排运行时是否真的观察到 Agent Loop 的终态；
3. 持久 Attempt result 引用的 Artifact 是否仍与终态 Event 和 projection 一致。

`AgentActivityReceipt` 为这三个问题提供 payload-free 的可验证边界。它不替代
`ToolReceipt`，也不尝试证明 Agent Loop 内的外部副作用 exactly-once。

## 2. 信任边界

```text
task / context Artifact
          |
          v
  Agent Activity Adapter
          |
          +-- Agent Loop result -----------+
          |                                |
          +-- request digest               v
          +-- runtime observation --> AgentActivityReceipt
                                           |
                                           v
                              terminal Event + Attempt
                                           |
                                           v
                                strict Store verification
```

当前 Legacy adapter 的请求仍由 task 与 runner kwargs 的规范 JSON digest 绑定；task 和
上下文本身不进入 receipt。远程 Agent 可使用独立 `AgentActivityRequest` 不可变
Artifact，但生产 remote adapter 尚未接线，不能因为已有 request/manifest digest 就
声称远程 Agent 已完成。

## 3. Schema v1/v2

| 字段 | 约束 |
|---|---|
| `run_id / node_id / attempt_id` | 非空、有界，并与 Attempt/Event 精确一致 |
| `activity_name` | 最多 128 字符的安全标识符，不接收自由文本 |
| `effect_class` | 与 Attempt 的 effect class 一致 |
| `attempt_status` | 必须为终态，并与 Event/Attempt 一致 |
| `request_digest` | idempotency record 的小写 SHA-256 |
| `result_digest` | Attempt `result` 规范 JSON 的 SHA-256；成功/已知失败通常是 NodeResult |
| `exit_reason` | 最多 128 字符的安全代码，不保存异常消息 |
| `turns` | 空或有界非负整数 |
| `observed_tool_results` | 运行时观察到的工具结果数量 |
| `result_artifact_digests` | 与 NodeResult `artifact_refs` 顺序一致，最多 64 个 |
| `tool_receipt_digests` | 与 NodeResult `tool_receipt_refs` 顺序一致，最多 64 个 |
| `internal_tool_receipts_complete` | 是否一项观察结果对应一个 receipt digest |
| `execution_manifest_digest` | v2 可选；有 ToolReceipt 或声明完整时必填，并对应唯一 result Artifact |
| `verification` | `runtime_observed` 或 `unverified` |
| `schema_version` | 严格整数 `1/2`；布尔值不视为整数 |

反序列化采用 exact-field schema。未知字段、缺字段、NaN、非 JSON 值、畸形 digest、
控制字符和超界集合全部拒绝。

## 4. 证据语义

`runtime_observed` 表示 adapter 观察到了 Loop 的正常返回或明确异常边界。它不表示：

- 每个内部工具都生成了 `ToolReceipt`；
- 外部系统确认了副作用；
- 操作只执行了一次；
- 模型响应或工具内容可以安全写入 Event Store。

成功 Attempt 只能使用 `runtime_observed`。`OUTCOME_UNKNOWN` 和 `ABANDONED` 只能使用
`unverified`。Legacy adapter 始终设置
`internal_tool_receipts_complete=false`，即使工具结果数恰好与引用数相等。

v2 有 ToolReceipt 时必须绑定
[AgentActivityExecutionManifest](agent-execution-manifest.md)。只有实际加载并验证
manifest、request Artifact 和逐个 ToolReceipt 后，才能把
`has_manifest_bound_tool_receipt_lineage` 用作新路径的 lineage 门禁。该属性本身只说明
receipt 已绑定 digest，不说明 Artifact bytes 已加载。调度器仍必须逐个判断 ToolReceipt
的 verification、effect class 和外部操作身份，不能把覆盖完整等同于副作用 verified。

manifest schema v2 还可绑定逐次 `ProviderInvocationReceipt`。远程接纳方必须加载
manifest、验证实际 provider receipt 集合，并设置 `require_complete=True`；仅有
`execution_manifest_digest` 不代表 provider lineage 完整，也不证明外部 gateway 的
operation ledger 或 attestation。

v1 保持 exact-field 历史兼容；其中 `internal_tool_receipts_complete=true` 仍是旧的
count-only 语义，不能用于新远程 Agent。

## 5. 原子提交与读取

receipt dict 和其 canonical digest 与 Attempt 终态 Event、Attempt/Node/Run projection
及 idempotency completion 一起提交。数据库事务失败时不允许留下单独的完成 receipt。
对非幂等 Agent，无法确认终态提交是否成功时写入 `OUTCOME_UNKNOWN` 的 unverified receipt，
并进入显式恢复。

`DurableRunStore.get_agent_activity_receipt()` 在同一个 SQLite 连接的 read transaction
读取终态 Event、Attempt 和 idempotency record，然后校验：

1. receipt 自身 exact schema 和 canonical digest；
2. Run、Node、Attempt identity 与 `activity_kind=agent`；
3. effect class、Attempt status 和终态 Event type；
4. request digest、`COMPLETED` 状态与 idempotency record；
5. result digest 与规范化 idempotency result、Attempt result；
6. 两组 Artifact digest 与 NodeResult 引用的精确顺序；
7. v2 manifest digest 对应唯一、typed、精确 producer 的 result Artifact ref。

任一不一致都抛出完整性错误。查询不得从 projection 的普通 dict 推断或伪造 receipt。
如果事务已经提交但 adapter 没收到响应，adapter 只有在重新读取到非空、验证通过的
新 receipt 后才能确认成功；该路径不得降级为历史兼容。

## 6. 兼容与恢复

- 终态 Event、Attempt 和 completed idempotency result 一致，且两个 receipt 字段都
  不存在：返回空，表示历史数据。
- 终态 Agent Attempt 缺少终态 Event：损坏，fail closed。
- receipt 或 digest 仅缺一个：损坏，fail closed。
- receipt 存在但解析、身份或 digest 不一致：损坏，fail closed。
- Legacy 终态 replay：完全缺失可恢复；存在则必须验证后恢复。
- 新远程 Agent Activity：必须要求非空且验证通过，不得使用历史兼容分支。

这条兼容规则有意无法区分“真正的旧 Event”和“攻击者同时删除两个字段”。Event Store
本身通过 append-only trigger 和 content digest 提供第一层防护；需要抵抗有数据库写权限
的攻击者时，部署还必须使用独立 OS identity、只读查询身份和外部签名/透明日志。新路径
通过“receipt 必须存在”消除该降级空间。

## 7. 隐私与大小边界

receipt 只保存安全代码、计数和 digest。禁止加入 task、prompt、response、异常文本、
工具参数、工具结果、路径、URL、tenant 自由文本、worker bearer 或凭据。需要保存的内容
必须先写入带 sensitivity 的不可变 Artifact，再由 digest 引用。

单个 receipt 最多引用 64 个结果 Artifact 和 64 个 ToolReceipt Artifact；这使终态
Event 大小与聊天长度、模型响应长度和工具输出长度无关。

实现的三轮威胁审查、发现和测试证据见
[Agent Activity Receipt 三轮对抗性审查](agent-activity-receipt-adversarial-review.md)。
