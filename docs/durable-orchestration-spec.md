# Durable Orchestration 规范

> 状态：v1 本地内核已实现并通过 F01–F30 故障矩阵；Phase 2 远程 worker 参考面
> 已提供独立 ADR、故障矩阵和 runnable demo；强隔离、生产传输与生产多租户仍不在
> 承诺范围
>
> 适用范围：XAgent 编排内核、Team Workflow、长任务恢复、工具活动持久化
>
> 相关文档：[architecture.md](architecture.md)、[agent-loop.md](agent-loop.md)、
> [tool-layer.md](tool-layer.md)、[agent-teams-spec.md](agent-teams-spec.md)、
> [resumable-long-tasks-spec.md](resumable-long-tasks-spec.md)、
> [observability.md](observability.md)

## 1. 背景与决策

现有 XAgent 已经具备单 Agent 循环、工具执行、串行 Team Workflow、文件 checkpoint
和结构化 telemetry，但这些机制不能共同提供长任务的执行正确性：

- Agent Loop 的状态主要存在于进程内存、`Session.history` 和 prompt 中。
- checkpoint 是有界摘要，写入失败不会阻断主循环，也不记录每个副作用的提交状态。
- Team Workflow 虽然声明 `depends_on`，执行时仍按文件顺序串行运行。
- telemetry 明确是 best-effort，Sink 失败不能阻断执行，因此不能作为恢复事实源。
- 工具失败由 LLM 决定是否重试，没有统一的 Attempt、timeout、幂等和取消语义。

本 ADR 决定增加一个独立的 Durable Orchestration 层。它以不可变 Domain Event
为执行事实，以可重建 projection 提供查询，以类型化 Run/Node/Attempt 管理生命周期，
并把现有 Agent Loop 和工具执行包装为 Activity。

Durable Orchestration 不替换 Agent Loop。二者边界是：

```text
Durable Orchestration
  ├─ 决定：哪个节点何时可以运行、是否重试、如何暂停/恢复/取消
  ├─ 持久化：Run / Node / Attempt / Receipt / Approval / Artifact 引用
  └─ 调用 Activity
       ├─ AgentActivity  -> 现有 run_agent_loop()
       └─ ToolActivity   -> 现有 Handler / tools
```

本 ADR 对旧文档中的下列约束作**局部覆盖**：

- “不做数据库”继续适用于 Memory、业务工作区和普通工具；编排元数据允许使用 Python
  标准库 SQLite，不引入 ORM。
- “不做循环状态持久化”继续适用于 Agent Loop 内部实现；编排层持久化 Agent Activity
  的边界状态和结果，不持久化生成器栈或 Python 调用栈。
- “不做自定义循环拓扑”和“不做工具编排”继续适用于单个 Agent Loop；编排层允许执行
  声明式 DAG，Agent Loop 内部的多轮工具调用仍保持原有串行语义。
- “不做自动重试”继续适用于 LLM 在 Agent Loop 内对普通工具错误的决策；编排层只依据
  显式、确定性的 RetryPolicy 重试整个 Activity Attempt。
- “不做分布式”对本地 v1 默认路径继续成立；lease 首先用于单进程崩溃恢复。可选
  Phase 2 参考面由独立的 [分布式执行 ADR](distributed-execution-adr.md) 约束，
  不把单机 lease 或参考轮询协议解释为生产分布式基础设施。

## 2. 术语和规范用语

本文中的“必须”“禁止”是正确性约束，“应该”是默认策略，“可以”是兼容扩展。

| 术语 | 含义 |
|---|---|
| Workflow | 有版本的声明式编排定义 |
| Run | 某个 Workflow 的一次执行实例 |
| Node | Workflow 中一个可调度步骤的定义 |
| NodeRun | Node 在某次 Run 中的运行实例 |
| Attempt | NodeRun 的一次有 lease 的执行尝试 |
| Activity | 会调用 LLM、工具、子 Agent 或外部系统的非确定性/副作用单元 |
| Domain Event | 影响执行正确性、必须可靠持久化的领域事件 |
| Projection | 由 Domain Event 推导的当前状态查询表 |
| ToolReceipt | 一次工具调用的输入摘要、结果摘要和副作用证明 |
| Artifact | 大型输出、文件快照、模型响应或报告的持久化对象 |
| Outcome Unknown | Activity 可能已产生副作用，但运行时没有可靠完成收据 |

## 3. 定位

### 3.1 目标

- 进程重启后，仅依赖持久化状态即可重建 Run、NodeRun 和 Attempt 的准确生命周期。
- 将 LLM 决策与确定性编排分离；允许线性流程、DAG、并行、join、router 和 subworkflow。
- 在“执行副作用”和“记录完成结果”之间的故障窗口中采取保守恢复策略。
- 为读取、幂等写入、非幂等写入提供不同的 retry 和恢复语义。
- 统一 timeout、取消、暂停、审批、lease、heartbeat 和父子任务传播。
- 让 UI、Eval、replay 和 observability 消费同一个领域模型，但不互相成为事实源。
- 平滑接入现有单 Agent、Team Workflow、checkpoint 和 Web 持久聊天。

### 3.2 非目标

本地 v1 不实现：

- 生产远程 worker server/pull transport、消息中间件或跨机器一致性；Phase 2
  参考协议与安全组合由独立 ADR 管理。
- 任意 Python/JavaScript Workflow 代码；只运行经过校验和版本化的声明式定义。
- 持久化或恢复 Python 生成器栈、线程栈、WebDriver 内部对象、子进程内存。
- 通用文件系统事务、通用分布式事务或任意工具的自动回滚。
- 让 telemetry JSONL、Langfuse 或 OTel 成为恢复依据。
- 将完整 prompt、response、工具参数或凭据默认写入 Event Store。
- 自动迁移旧 checkpoint 为“精确恢复”；旧 checkpoint 只能导入为带来源标记的新 Run。
- 在 v1 中承诺多租户、RBAC 或远程 Artifact Store；Artifact 静态加密由部署适配层实现，
  本地默认存储不声称加密。

### 3.3 明确不保证通用 exactly-once

外部系统和本地 SQLite 之间不存在共同事务。系统不能原子地同时完成：

1. 修改外部世界；
2. 在本地 Event Store 记录“修改已完成”。

因此，XAgent **禁止承诺通用 exactly-once 工具执行**。规范保证的是：

- Domain Event 和本地 projection 在一个 SQLite 事务内原子提交；
- 调度采用 at-least-once Attempt 语义；
- 支持幂等键的工具在重试时复用稳定幂等键；
- 受保护的幂等工具可以达到“副作用观察上一次”的效果；
- 无法证明结果的非幂等写入进入 `OUTCOME_UNKNOWN` / `WAITING_RECOVERY`，
  禁止自动盲重试；
- 只有工具能提供幂等、状态探测或补偿契约时，运行时才能给出更强保证。

文档、API 和 UI 必须区分“Attempt 被调度一次”“工具可能执行一次或多次”和“完成结果只提交一次”。

## 4. 设计原则

1. **Event 是事实，projection 是缓存。** projection 丢失后必须能从 Event 重建。
2. **先持久化意图，再执行副作用。** Activity 开始前必须提交 Attempt 和稳定幂等键。
3. **未知优于猜测。** 无法证明未执行或已执行时，进入人工/探测恢复，不把 unknown 当失败重试。
4. **控制流必须确定。** readiness、重试、超时和状态转换由代码决定，不由 LLM 文本决定。
5. **副作用必须有身份。** 每次活动都关联 `run_id / node_id / attempt_id / idempotency_key`。
6. **大数据引用化。** Event 只保存有界元数据和 Artifact 引用。
7. **取消是持久请求，不是瞬时信号。** 重启后仍必须继续传播未完成的取消。
8. **安全在模型之外。** policy/approval 的结论必须由编排层门禁执行。
9. **兼容通过适配，不污染核心。** `ActionResult`、checkpoint 和旧 Workflow 由 adapter 转换。

## 5. 领域对象

所有持久化对象必须包含 `schema_version`。所有 ID 均由运行时生成，不使用用户输入直接
拼接路径；推荐使用 UUIDv7，缺少实现时可使用 UUID4。

### 5.1 WorkflowDefinition

```text
workflow_id          稳定逻辑标识
workflow_version     单调递增的整数版本
definition_digest    规范化定义的 SHA-256
name
nodes[]              NodeDefinition
input_schema
output_schema
created_at
```

已开始 Run 引用的 Workflow 版本必须不可变。修改 Workflow 必须生成新版本。
Store 必须在创建任意顶层或子 Run 的同一事务中，以
`(workflow_id, workflow_version)` 全局绑定唯一 `definition_digest`。Runtime 还会
把经过验证的不可变定义 `ArtifactRef` 绑定到该身份；允许先写 digest、后补同一
定义的 ref，但禁止替换。并发创建、直接 Store API、子工作流和进程重启均不得绕过
这条约束。已有数据库升级时若发现同一逻辑版本对应多个 digest，migration 必须
fail closed。

### 5.2 NodeDefinition

```text
node_id              Workflow 内稳定且唯一
kind                 agent | tool | router | parallel | join | map |
                     approval | subworkflow
depends_on[]
input_mapping
output_schema
activity_spec
retry_policy
timeout_policy
resource_keys[]
on_error             fail_run | continue | skip_dependents
```

编译器必须拒绝重复 ID、未知依赖、环、无效模板引用和不兼容的输入输出映射。
`resource_keys` 用于限制会修改同一 workspace、浏览器 session 或其他资源的节点并发。

Workflow v2 的 `input_mapping` 是以本地输入名为 key 的有界对象。每个 selector
只能是以下两种严格结构之一：

```json
{"source": "run_input", "artifact_index": 0}
{"source": "node_output", "node_id": "upstream", "artifact_index": 0}
```

`artifact_index` 可省略；省略时选择该 receipt 的全部 ArtifactRefs。输入名按字典序
规范化，因此声明对象的字段顺序和上游完成顺序不影响结果。`node_output` 只能引用
当前 Node 的传递依赖闭包；未知字段、未知 Node、闭包外引用、缺失/损坏 receipt、
索引越界或解析后超过 64 个 ArtifactRefs 均在 Attempt 创建前 fail closed。
声明 mapping 与解析得到的完整 Artifact identities 同时进入 Activity request hash；
只有 mapping 为空的旧定义保留原有 Run-input digest 行为和 canonical definition
digest。

### 5.3 RunRecord

```text
run_id
workflow_id / workflow_version / definition_digest
status
input_artifact_refs[]
output_artifact_refs[]
parent_run_id? / parent_node_id?
requested_by
created_at / started_at? / finished_at?
deadline_at?
last_event_sequence
error_code? / error_summary?
```

### 5.4 NodeRun

```text
run_id / node_id
status
dependency_snapshot
current_attempt_no
input_artifact_refs[]
output_artifact_refs[]
ready_at? / started_at? / finished_at?
error_code? / error_summary?
```

### 5.5 Attempt

```text
attempt_id
run_id / node_id / attempt_no
status
worker_id? / lease_id? / fencing_token?
idempotency_key
activity_kind
started_at? / finished_at?
timeout_at?
error_class? / error_code? / error_summary?
receipt_id?
```

同一 `(run_id, node_id, attempt_no)` 必须唯一。状态转换提交时必须校验当前
`fencing_token`，过期 worker 不能覆盖新 owner 的结果。

### 5.6 NodeResult

Activity adapter 向编排层返回类型化结果，禁止让 `ActionResult`、provider response
或任意工具 dict 直接成为持久化契约：

```text
schema_version
outcome               succeeded | failed | waiting_input |
                      waiting_approval | cancelled
output                 有界且符合 Node output_schema 的 JSON
artifact_refs[]
tool_receipt_refs[]
metrics                turns、token、duration 等有界非控制信息
error_class? / error_code? / bounded_error_summary?
```

NodeResult 是 Activity 边界传输对象，不是事实源。编排层校验后必须把其中的事实转换为
Artifact/Receipt 引用和 Domain Event；未提交前的 NodeResult 在崩溃后可以丢失。

### 5.7 DomainEvent

Domain Event schema：

```json
{
  "schema_version": 1,
  "event_id": "uuid",
  "run_id": "uuid",
  "sequence": 42,
  "type": "attempt.succeeded",
  "occurred_at": "RFC3339 UTC",
  "workflow_id": "coding-fix",
  "workflow_version": 3,
  "node_id": "run_tests",
  "attempt_id": "uuid",
  "causation_id": "uuid-or-null",
  "correlation_id": "uuid",
  "actor": {"kind": "worker", "id": "local-123"},
  "data": {},
  "artifact_refs": [],
  "sensitivity": "metadata"
}
```

约束：

- `(run_id, sequence)` 唯一且从 1 开始连续；sequence 只用于同一 Run 内排序。
- `event_id` 全局唯一，用于幂等追加。
- `type` 使用小写点分命名；Event payload 的 breaking change 必须提升 schema version。
- `occurred_at` 只用于展示，不决定顺序和超时；顺序以 sequence、超时以持久 deadline 为准。
- `data` 必须是有界 JSON；禁止默认写入凭据、完整 prompt、完整 response 和原始工具参数。
- 大内容必须先写 Artifact，再在事件中保存 digest 和引用。
- 每个会改变 projection 的事件必须包含版本化的规范载荷
  `projection: {schema_version, run?, node?, attempt?}`。其中保存提交后的有界 projection
  snapshot；字段顺序、缺省值、枚举值和 JSON 编码必须规范化。projection 只能包含 Artifact
  引用和有界元数据，禁止借 snapshot 绕过敏感内容限制。
- 每种已知事件类型必须定义 payload schema 和确定性的 reducer。相同事件流交给在线 reducer、
  离线 rebuild 和测试 reducer 时，必须得到逐字段相同的 projection。
- 未知字段读取时忽略，未知事件类型重建 projection 时必须 fail closed，而不是静默跳过。

最小事件集合：

```text
run.created / run.started
run.pause_requested / run.paused / run.resumed
run.waiting_input / run.waiting_approval / run.waiting_recovery
run.cancel_requested / run.cancelling
run.completed / run.failed / run.cancelled

node.pending / node.ready / node.started
node.waiting_input / node.waiting_approval / node.waiting_retry /
node.waiting_recovery / node.paused
node.succeeded / node.failed / node.cancelled / node.skipped

attempt.scheduled / attempt.claimed / attempt.started
attempt.succeeded / attempt.failed / attempt.timed_out /
attempt.cancelled / attempt.abandoned / attempt.outcome_unknown

activity.commit_rejected
tool.receipt_recorded
artifact.created
approval.requested / approval.resolved
input.requested / input.received
lease.acquired / lease.expired / lease.released
```

高频 heartbeat 不要求逐条写 Domain Event；lease 表中的有条件更新属于控制面持久状态，
只有 acquire、expire、release 等生命周期边界需要产生事件。

`activity.commit_rejected` 在原提交事务回滚后由独立事务追加，只记录有界
`reason_code`、首次拒绝观察到的安全 fencing 数字和终态布尔值。它不改变 Node/Attempt
业务状态，不接收或派生保存 worker ID、claim token、request hash、结果或原始 payload；
同一 Attempt + reason 的拒绝使用稳定 Event ID，并采用 first-write-wins 的审计快照去重。
后续 fencing 代际变化不会改写该 Event，也不会放大审计流。

### 5.8 ArtifactRef

```text
artifact_id
kind                 model_response | tool_result | file_snapshot |
                     report | log | generic
uri                  运行时管理的相对 URI，不是任意用户路径
sha256
size
media_type
producer_run_id / producer_node_id / producer_attempt_id
sensitivity          public | internal | sensitive | secret
encryption            none | deployment_managed
encryption_key_ref?   只保存部署层不透明引用，不保存密钥
created_at
```

Event Store 只保存 Artifact 元数据。Artifact 写入必须使用临时文件 + fsync（平台支持时）+
原子替换，并在内容完成后才允许首个 Domain Event 引用；部署可以额外追加
`artifact.created`，但内核不依赖该专用 Event 才登记引用。孤立但未引用的 Artifact
由 GC 清理；Event 引用了不存在或 digest 不匹配的 Artifact 时必须报数据完整性错误。

内核必须定义 `ArtifactStore` 能力接口和 encryption metadata，使部署适配层可以提供静态
加密；密钥获取、轮换和实际加密实现不进入 v1 内核。本地默认 ArtifactStore 必须明确报告
`encryption=none`，禁止仅因 sensitivity 被标为 `secret` 就宣称内容已加密。

### 5.9 ToolReceipt

```text
receipt_id
run_id / node_id / attempt_id
tool_name / tool_version
idempotency_key
args_digest
effect_class          read_only | idempotent_write | non_idempotent_write
started_at / finished_at
outcome               succeeded | failed | cancelled | unknown
external_operation_id?
target_refs[]
precondition_digest?
postcondition_digest?
result_digest?
result_artifact_refs[]
verification          verified | provider_ack | inferred | unverified
error_class? / error_code? / bounded_error_summary?
```

ToolReceipt 是“运行时知道发生了什么”的证明，不等于外部事务收据。只有
`verification=verified` 或具备可信 `external_operation_id` 时，才能据此跳过相同幂等操作。

参数和结果默认只保存 digest；确需保存原文时必须进入带 sensitivity 的 Artifact。
Store 的查询 API 必须从 Attempt 的终态 Event 重新解析 ToolReceipt，并校验 receipt
自身 digest、Run/Attempt identity 以及 Event 终态一致性；缺少收据返回空，损坏或错绑
必须 fail closed，不能把 projection 中的普通 result dict 当作已验证收据。

### 5.10 Lease

```text
lease_id
run_id / node_id / attempt_id
owner_worker_id
fencing_token
acquired_at / heartbeat_at / expires_at
```

每次重新 claim 都生成更大的 fencing token。所有 Attempt 结果提交必须比较 token；
旧 worker 即使稍后恢复，也只能得到 stale-owner 错误，不能覆盖当前状态。

## 6. 状态机

状态转换必须由单一 Orchestration Store API 完成，并在事务内追加 Event、更新 projection。
禁止调用方直接修改 projection 表。

### 6.1 Run 状态机

```text
CREATED
  -> RUNNING | CANCELLING | FAILED

RUNNING
  -> PAUSING | WAITING_INPUT | WAITING_APPROVAL | WAITING_RECOVERY
  -> CANCELLING | COMPLETED | FAILED

PAUSING
  -> PAUSED | CANCELLING

WAITING_INPUT
  -> RUNNING | CANCELLING

WAITING_APPROVAL
  -> RUNNING | CANCELLING

WAITING_RECOVERY
  -> RUNNING | FAILED | CANCELLING

PAUSED
  -> RUNNING | CANCELLING

CANCELLING
  -> CANCELLED
```

语义：

- `RUNNING` 表示仍有运行中或可调度节点。某一分支等待输入而另一分支可继续时，Run
  保持 `RUNNING`。
- 只有不存在可运行/运行中节点且所有阻塞节点类型相同时，Run 才投影为相应
  `WAITING_*`；多种等待同时存在时优先级为 recovery > approval > input。
- `PAUSING` 停止调度新 Attempt，等待已有 Attempt 到达安全边界；暂停不是强杀。
- `CANCELLING` 是持久化的取消传播过程。所有非终态子 Run、Node 和 Attempt 处理完后
  才进入 `CANCELLED`。
- `COMPLETED`、`FAILED`、`CANCELLED` 是终态。
- `COMPLETED` 必须满足所有必要输出已提交且所有必需节点为 `SUCCEEDED`，可选分支可以
  为 `SKIPPED`。

### 6.2 NodeRun 状态机

```text
PENDING
  -> READY | SKIPPED | CANCELLED

READY
  -> RUNNING | SKIPPED | CANCELLED

RUNNING
  -> SUCCEEDED | PAUSED | WAITING_RETRY | WAITING_INPUT | WAITING_APPROVAL
  -> WAITING_RECOVERY | FAILED | CANCELLED

PAUSED
  -> READY | CANCELLED

WAITING_RETRY
  -> READY | CANCELLED

WAITING_INPUT
  -> READY | CANCELLED

WAITING_APPROVAL
  -> READY | CANCELLED

WAITING_RECOVERY
  -> READY | FAILED | CANCELLED
```

约束：

- 所有必需依赖成功后，Node 才能从 `PENDING` 进入 `READY`。
- router 未选择的分支进入 `SKIPPED`，不能伪装成成功。
- Node 为 `RUNNING` 时必须存在且只能存在一个 active Attempt。
- Attempt 失败后，仅在 RetryPolicy 明确允许时进入 `WAITING_RETRY`。
- `PAUSED` 只表示运行中的 Activity 已确认到达可安全暂停边界；对应 Attempt 必须已经终止，
  Node 不得以 `RUNNING` 伪装暂停。Run resume 时，Node 从 `PAUSED` 进入 `READY`，并创建
  新 Attempt，禁止复用或重开旧 Attempt。
- Node 输出 Artifact 和 `node.succeeded` 必须在同一事务中建立引用。
- `WAITING_RECOVERY` 禁止自动生成新 Attempt，直到探测器或用户提交显式 resolution。
- router、approval 和显式 control resolution 必须以入口 Run+Node snapshot 做 CAS。
  同一可信决议的重复提交幂等返回，冲突决议 fail closed；同一 Run 的无关 Event 先提交时，
  只允许对 Store 明确标记的 committed projection update 有界重读，禁止吞掉普通完整性错误。
- `SUCCEEDED`、`FAILED`、`CANCELLED`、`SKIPPED` 是终态。

### 6.3 Attempt 状态机

```text
SCHEDULED
  -> CLAIMED | CANCELLED

CLAIMED
  -> RUNNING | WAITING_APPROVAL | FAILED | ABANDONED | CANCELLED

WAITING_APPROVAL
  -> SCHEDULED | FAILED | CANCELLED

RUNNING
  -> SUCCEEDED | FAILED | TIMED_OUT | CANCELLED
  -> ABANDONED | OUTCOME_UNKNOWN
```

语义：

- `SCHEDULED` 已持久化 idempotency key，但尚无 worker owner。
- `CLAIMED` 已持有 lease，但 Activity 尚未通过开始门禁。
- policy 返回 `REQUIRE_APPROVAL` 时，claim lease 必须在同一事务中释放，Run、Node
  和 Attempt 一起进入 `WAITING_APPROVAL`，并写入只含有界 digest 的
  `approval.requested`。可信 grant 恢复**同一 Attempt**为 `SCHEDULED`，下一次 claim
  获得更高 fencing token；可信 rejection 终止该 Attempt，取消意图则收敛为
  `CANCELLED`。重启不得重复申请或创建新 Attempt。
- `RUNNING` 表示 Activity 可能已经开始；从该状态丢失 lease 时必须依据 effect class
  决定 `ABANDONED` 或 `OUTCOME_UNKNOWN`。
- `ABANDONED` 只表示 worker 丢失且可以证明副作用尚未开始，或 Activity 是
  `read_only` / 可安全重试的 `idempotent_write`。
- `OUTCOME_UNKNOWN` 表示不能证明副作用结果，必须推动 Node/Run 进入
  `WAITING_RECOVERY`。
- Attempt 的所有**结果状态**均为终态；重试必须创建新的 Attempt。唯一允许把同一
  Attempt 恢复为 `SCHEDULED` 的非结果状态是尚未启动 backend 的持久审批等待。

## 7. 持久化与事务

### 7.1 存储布局

v1 每个逻辑 workspace/tenant 使用一个独立控制面目录：

```text
<trusted-control-plane-root>/<tenant>/orchestration.sqlite3
<trusted-control-plane-root>/<tenant>/artifacts/
<trusted-control-plane-root>/<tenant>/artifact-gc/
```

该根目录禁止位于 Agent workspace 内，必须由独立控制面服务/OS identity 持有，且不能
挂载给 legacy `file_*`、`code_run`、浏览器下载或任意 Activity。仅把最小 Artifact
输入通过受信 adapter 交给隔离 backend。单纯使用隐藏路径或同 UID 的文件权限不是安全
边界。`web_ui_new.py` 不自动发现或挂载该目录；只读投影服务显式向
`WorkspaceStoreRegistry` 注入 tenant 到精确外部数据库文件的受信映射。

数据库使用 WAL、foreign keys、busy timeout。该数据库只保存控制面元数据，不改变
Memory、Runbook、Team 配置等业务文件格式。不同 tenant 禁止共享数据库连接或 Run。
旧版实验路径 `workspace/<name>.ws/runtime/orchestration.sqlite3` 及其 WAL/SHM 只作为
迁移输入；普通文件工具对这些名称拒写，但这不替代进程/UID/挂载隔离。工作区自身的
`runtime/artifacts/` 是普通 Agent 运行产物目录，禁止作为 Durable ArtifactStore 根。

当前本地 Store 的物理表：

```text
workflow_bindings
runs
node_runs
attempts
domain_events
idempotency_records
artifact_references
artifact_gc_claims
schema_migrations
```

不可变 Workflow definition 和大内容位于 ArtifactStore；ToolReceipt、approval 与
lease 生命周期作为有 schema 的 Event/projection 字段持久化，而不是建立可被调用方
绕过 Event Tx 直接修改的旁路真相表。Telemetry cursor 属于独立的 best-effort adapter，
不进入上述执行 Store。

### 7.2 写入规则

每次状态转换执行一个短事务：

1. 读取并校验当前 projection version、状态和 fencing token。
2. 分配下一个 Run sequence。
3. 以 `event_id` 幂等追加 Domain Event。
4. 更新相应 projection 和 `last_event_sequence`。
5. 必要时更新 idempotency、receipt、approval、lease 或 Artifact 引用。
6. commit。

任何一步失败均 rollback，调用方收到明确错误。Domain Event 写入禁止像 telemetry Sink
一样吞错。SQLite 事务期间禁止执行 LLM、工具、文件写入或网络调用。

Store/Artifact/GC 根是控制面数据，Executor 必须在调用 backend 前解析 Sandbox profile
的全部 allowed roots 和 cwd，并拒绝与任一控制面根相等、互为祖先或互为后代的组合。
该进程内校验用于防止配置失误，不替代独立服务 identity、mount 隔离或强沙箱。

重复提交相同 `event_id` 时，必须比较完整的 canonical event intent，而不是只比较自由
`data/payload`。canonical intent 至少包含：

```text
schema_version
run_id / workflow_id / workflow_version
type / node_id? / attempt_id?
causation_id? / correlation_id / actor
规范化 data / artifact_refs
expected projection version / fencing token?
目标 projection delta 或提交前 snapshot digest
```

`sequence`、服务端生成的 `occurred_at` 和数据库行位置不进入 intent；它们由第一次提交决定。
canonical intent 必须使用稳定 JSON 规范化后计算 digest，并随 Event 保存，以便重复请求和
离线完整性检查使用同一算法。

重复提交规则：

- canonical intent digest 相同：仅当已存 Event 及其目标 projection 与原提交结果一致时，
  返回先前提交结果；
- canonical intent、目标 projection 或 fencing/version 不同：视为完整性冲突并 fail closed；
- 禁止出现“Event 被判定为重复成功，但调用方请求的 projection 从未提交”的结果。

### 7.3 Projection

projection 是 `runs`、`node_runs`、`attempts` 等当前状态表。必须提供：

- 从 Run 的 sequence 1 开始全量重建；
- 校验 projection 的 `last_event_sequence`；
- 启动时修复“Event 已提交、projection 未提交”不应出现的问题，因为二者同事务；
- 对未知 Event schema/version 停止重建并报告 migration required；
- 在测试中比较在线 projection 和离线 replay projection 一致。
- 为每个事件 schema 保存 golden event stream；测试必须同时使用在线 reducer 和离线
  rebuild 消费这些固定流，并逐字段比较 Run、NodeRun、Attempt projection。新增字段、
  默认值或 reducer 变化必须更新 schema/migration，禁止只改在线写路径。
- rebuild 必须逐页 reduce，只保留最终 Run、Node、Attempt 状态，禁止先拼接完整 Event
  History。请求路径中的显式完整性校验还必须同时限制 Event 总数、canonical payload
  累计字节数和 wall-time；回放与 live projection 比较必须位于同一个 SQLite read
  snapshot，active Run 的并发提交只能整体落在该 snapshot 之前或之后，不能制造假
  mismatch。单个 SQLite 页和单个 Event reducer 是 wall-time 的协作式检查边界，不宣称
  OS 级抢占。

查询 API 默认读 projection，不在请求路径中全量 replay。

### 7.4 Artifact 与数据库事务边界

Artifact 文件与 SQLite 也不是共同事务，采用“先内容、后引用”：

1. 写临时 Artifact，计算 digest；
2. 原子替换为 content-addressed 最终路径；
3. SQLite 事务内登记 Artifact 并引用；
4. 未被登记的孤立文件允许由 GC 清理。

禁止先提交 Event 引用再写 Artifact。

Store schema v3 为完整 canonical `ArtifactRef` 建立规范化
`artifact_references` 索引，并在 v2→v3 migration 中从既有 Domain Event payload
回填。所有三条 Domain Event 写入路径都必须在同一个 SQLite write transaction 中先
检查 `artifact_gc_claims` tombstone、再登记引用和提交 Event；未知的更高 Store schema
必须 fail closed，禁止以缺少索引的旧语义继续运行。

GC 在同一个 workspace 内使用跨进程排他锁串行化 collect、regular/temporary list 与
restore。对每个候选对象，GC 在 SQLite `BEGIN IMMEDIATE` 中重新检查引用索引：若已有
引用则跳过；否则提交唯一的逐 digest `moving` claim，作为 GC 与后续 Event 引用提交的
线性化点。文件原子移入同文件系统 quarantine 后，claim 转为 `quarantined`；restore
只有在文件验证成功后才清除 tombstone。进程在 claim 后或 move 后崩溃时，下一次持锁
操作根据源文件、quarantine 文件和 claim state 恢复；状态无法唯一判定时 fail closed。
普通异常必须把文件与 `moving` claim 一起回滚。

因此，只要引用通过 `DurableRunStore` Domain Event transaction 登记，不会出现已提交
引用暂时或永久指向已隔离字节的窗口。grace period 与第二次可达性扫描仍是保守筛选，
不是线性化依据。直接调用外部存储、绕过 Store Event transaction 的任意登记不在此保证
内，也不宣称跨外部系统 exactly-once。

## 8. 不确定提交窗口

Activity 的标准执行顺序：

```text
Tx A:
  attempt.scheduled
  idempotency_record(PENDING)
commit

Tx B（claim）:
  lease.acquired
  attempt.claimed
commit

Tx C（副作用开始门禁）:
  attempt.started
commit

执行外部 Activity

Tx D（完成）:
  tool.receipt_recorded / result Artifact 引用
  attempt.succeeded | attempt.failed
  node projection
commit
```

### 8.1 故障分类

| 故障窗口 | 已知事实 | 恢复 |
|---|---|---|
| Tx A 前 | 没有调度事实 | 可以重新调度 |
| Tx A 后、Tx B 前 | Attempt=SCHEDULED，副作用未开始 | 可以 claim 同一 Attempt |
| Tx B 后、Tx C 前 | Attempt=CLAIMED 且有 lease，尚未越过副作用门禁 | lease 回收后标记 `ABANDONED` 并按策略新建 Attempt |
| Activity 执行中 | 可能部分执行 | 依据工具 effect class、探测和幂等能力处理 |
| Activity 完成后、Tx D 前 | 外部可能完成，本地没有完成收据 | 幂等/可探测工具恢复；否则 `OUTCOME_UNKNOWN` |
| Tx D 后 | 本地已有完成事实 | replay 复用 Receipt/Artifact，禁止重新执行 |

Tx C 的 `attempt.started` 表示“运行时已经越过副作用门禁”，不表示外部系统已开始。
一旦存在该事件且没有终态，恢复器必须保守处理。

### 8.2 恢复决策

| effect class | 丢失 lease 且无 Receipt | 默认动作 |
|---|---|---|
| `read_only` | 结果未知但无持久副作用 | 标记旧 Attempt `ABANDONED`，允许重试 |
| `idempotent_write` | 可能已写入 | 使用同一 operation key 探测或重试；新 Attempt 复用稳定业务幂等键 |
| `non_idempotent_write` + 可探测 | 可能已写入 | 先探测；确认完成则补录 verified Receipt，确认未执行才重试 |
| `non_idempotent_write` + 不可探测 | 可能已写入 | `OUTCOME_UNKNOWN`，等待人工裁决，禁止自动重试 |

“超时”“连接断开”“进程被 kill”均不等价于“操作未发生”。

### 8.3 LLM Activity

LLM 调用通常没有用户环境副作用，但可能重复计费且结果不确定：

- 已持久化模型响应 Artifact 时，logical replay 必须复用，不重新调用模型。
- provider 支持 request id/idempotency key 时必须记录并使用。
- 结果在 Tx D 前丢失时允许按 RetryPolicy 重试，但必须记录可能重复计费。
- LLM 重试产生的新响应属于新 Attempt，不覆盖旧 Attempt。

## 9. 幂等

### 9.1 幂等键

运行时幂等键至少由以下稳定字段派生：

```text
workflow_id / workflow_version / run_id / node_id / logical_operation_key
```

Attempt 编号不应默认进入业务幂等键，否则每次重试都会成为新的外部操作。Attempt 自身
仍有唯一 `attempt_id`，二者用途不同。

### 9.2 工具要求

每个编排可用工具必须声明：

```text
effect_class
supports_idempotency_key
supports_status_probe
supports_compensation
timeout_behavior
resource_keys
```

缺少声明的工具默认按 `non_idempotent_write` 处理，而不是假设只读。

Policy 与 Approval 绑定的 `action_digest` 必须覆盖实际执行意图，而不只是工具名和
Workflow 中的声明参数。至少包括：

```text
normalized argv / cwd / resource limits / Sandbox profile digest
input Artifact identities / script Artifact digest
environment redacted-or-HMAC bindings
capabilities / resource locks
```

完整执行意图必须在提交 `policy.decided` 和 `attempt.started` 前生成并验证；Sandbox
Dispatcher 必须再次确认收到的 ExecutionRequest 与已授权 digest 完全一致。审批签发后
替换 argv、输入 Artifact、脚本、环境、资源限制、工作目录或 profile，必须在 backend
执行前 fail closed。Domain Event 只保存有界 digest，不保存 argv、路径、凭据或 Artifact
内容。秘密不得作为 argv 明文传入；需要精确绑定秘密值时由可信边界提供不可离线枚举的
HMAC/redacted binding。ExecutionRequest 只携带环境名与该 binding，不携带秘密值；
真正的值只能由支持此契约的后端侧可信 broker 解析。
敏感键识别必须复用 metadata boundary 的 NFKC、casefold 和分隔符移除规则。
实现必须对显式 `sensitive_keys` 及内置敏感字段名中的非 null、非空 JSON scalar
（包括 numeric/bool；兼容受信边界 bytes-like 值）做结构化检查，在 policy 前
拒绝其出现在 argv、cwd 或 resource locks；不得以通用熵猜测替代显式字段语义，
也不得持久化被拒绝值或其可离线枚举的直接 hash。

脚本绑定不得接受 caller 单独声称的 digest。脚本必须以已验证 immutable
`ArtifactRef` 进入受信边界，并物化为与其 size/SHA-256 一致的不可变 bytes；
Dispatcher 只允许显式声明支持该物化契约的 backend 接收。稳定 operation key 与
idempotency key 必须由只允许 workflow/run/node 固定字段与 input digest 的模板
生成，不得承载秘密。为支持重启后的 probe/reconcile，原值保存在内部 Attempt
projection 与 idempotency state，并交给受信 backend；Policy/approval/receipt
公共 payload 只绑定其 SHA-256 摘要，Web/telemetry 不得导出原值。Policy 必须校验
工具声明的 timeout behavior 和
allowed/required resource keys，不能仅凭 Workflow 的 effect class 假设幂等。
执行脚本或动态代码的工具必须额外声明 `requires_script_artifact`；该声明进入
action digest，缺少 ref/bytes 时在 `attempt.started` 前拒绝。固定受信函数或
immutable image 命令可以显式声明为 `false`，不得根据 tool name、argv 后缀或路径
形态猜测。

Scheduler、Policy 与 Sandbox 必须共享同一个稳定 operation/idempotency key 长度
上限；当前 v1 上限为 1024 字符。渲染结果超限必须在 Attempt schedule/claim 前
fail closed，不能先创建一个后续无法授权或执行的 Attempt。

对于文件工具：

- `file_read` 可声明 `read_only`。
- `file_patch` 可以通过目标路径、前置 digest、补丁 digest 和后置 digest 提供可探测语义；
  恢复时后置 digest 匹配可补录成功，前置 digest 匹配可安全重试，均不匹配则进入冲突恢复。
- 覆盖式 `file_write` 只有在具备前置/后置 digest 和原子替换时才能声明可探测幂等；
  append/prepend 默认是非幂等写入。
- `code_run` 默认是非幂等写入，因为脚本可产生任意副作用。
- 浏览器 JS 默认是非幂等写入，除非调用方为具体动作提供可验证 operation key。

## 10. Retry、Timeout、Cancel 和 Pause

### 10.1 RetryPolicy

```text
max_attempts
retry_on[]            transient | rate_limited | conflict | selected_timeout
initial_delay_ms
max_delay_ms
backoff_multiplier
jitter
max_elapsed_ms?
```

错误分类必须由工具/adapter 的结构化错误确定，不能通过让 LLM 阅读错误文本来控制
编排层重试。

默认不重试：

- invalid input
- policy denied
- approval rejected
- permission denied
- permanent error
- cancellation
- `OUTCOME_UNKNOWN`
- 不可探测非幂等写入的 timeout/断连

每次重试必须创建新 Attempt，并保留全部失败历史。Run deadline 优先于 retry policy。

### 10.2 Timeout

区分：

- `schedule_timeout`：节点进入 READY 后长期未被 claim；
- `start_timeout`：claim 后未开始；
- `execution_timeout`：Activity 执行时间；
- `heartbeat_timeout`：worker/Activity 失去活性；
- `run_deadline`：整个 Run 的绝对截止时间。

timeout 必须使用持久化绝对 deadline，不能只依赖进程内 timer。重启后立即处理已过期 deadline。

execution timeout 触发取消信号，但只有执行后端确认结束时才能标记 `TIMED_OUT`；
外部操作结果无法确认时必须标记 `OUTCOME_UNKNOWN`。

### 10.3 Approval

Activity 审批是可恢复状态，不是一次函数调用：

1. Executor 在完整 action digest 与 policy digest 确定后提交
   `approval.requested`，释放 worker lease，不启动 backend。
2. 可信控制面签发 action-bound、policy-bound、带过期时间的 grant；如果请求仍在等待，
   grant 的 Event 与 Run/Node/Attempt 恢复在同一事务提交。
3. 重启后 Scheduler 重新 claim 同一 Attempt，以更高 fencing token 继续门禁和执行。
4. 可信拒绝和持久取消分别原子收敛为失败或取消；两者都不得调用 backend。

预先签发 grant 仍可使用，但消费必须是单次且与执行意图完全一致。审批 actor、原始
operation key、argv、路径和 Artifact 内容不得进入公共 Event。

### 10.3.1 Unknown outcome resolution

`OUTCOME_UNKNOWN` 禁止自动重试。可信 operator control plane 只能提交两种结论：
`confirmed_succeeded` 或 `confirmed_failed`。两者都必须绑定原 Run/Node/Attempt 和经
Artifact Store 验证的 immutable evidence Artifact；确认成功还必须绑定 result Artifact。
Store 以 `run.recovery_resolved` 原子更新 Run/Node，但保留原 terminal
`OUTCOME_UNKNOWN` Attempt。重复相同 decision 幂等，冲突 decision fail closed。
公共 projection 只暴露固定状态、reason code、允许的 operator action 和 digest，不暴露
evidence 内容、错误 payload、路径或 actor。模拟和 diagnostics 不是授权，执行/恢复入口仍须
通过受信 authorizer。

### 10.4 Cancel

取消流程：

1. 事务内追加 `run.cancel_requested`。
2. 停止调度新 Attempt。
3. 向所有非终态 child Run、Node 和 Activity 传播取消。
4. 可取消 Activity 进行 cooperative cancel；子进程执行在超时后 kill 并 wait。
5. 处理未知副作用和 Receipt。
6. 所有子状态收敛后追加 `run.cancelled`。

现有 `stop_sig`、`code_stop_signal` 和 `_stop` 文件是执行后端的取消传输手段，不是取消
事实来源。恢复后编排器必须从 Domain Event 重新发送尚未完成的取消。

活着的本地 worker 必须通过绑定到 Run/Node/Attempt 的 `CancellationProbe` 轮询
Store，而不是依赖进程内布尔值。后端只能报告“观察到信号”；Executor 仅在 durable
Run 已进入取消流程时接受该结果并提交 Receipt。读取取消事实不确定时结果必须是
`CANCELLATION_UNKNOWN`，禁止伪装成工具失败或已取消。

本地 supervisor 能在 worker 存活时对独立进程组执行 TERM→有界 grace→KILL→reap；
controller/worker 自身崩溃后重新 attach 遗失进程不在 v1 保证内，生产部署需要保存
外部 execution identity 且支持 reattach 的 supervisor。

父 Run 取消默认级联到全部子 Run。只有 Workflow 显式声明 detached 的未来扩展才可例外；
v1 禁止 detached。

子 Run admission 必须在同一个 `BEGIN IMMEDIATE` 事务中重读父 Run 与 control Node、
执行 root/per-control 有界计数并插入 child。仅两者均为 `RUNNING` 时允许创建：child
先提交时后续 cancel/pause 必须通过 hierarchy 索引发现并传播；intent 先提交时必须
拒绝新 child。

### 10.5 Pause

Pause 与 Cancel 不同：

- pause 停止新 Attempt，并等待运行中的 Activity 到安全边界；
- pause 不回滚已完成节点；
- `PAUSED` Run resume 后继续使用相同 Run ID；
- Activity 明确确认安全中断时，旧 Attempt 以 `CANCELLED` 终止并记录
  `reason=pause`，对应 Node 进入 `PAUSED`；resume 后 Node 进入 `READY` 并创建新 Attempt；
- 等待用户输入/审批不是用户主动 pause；
- 不支持安全暂停的长 Activity 只能等其结束、超时或取消，不能假装已暂停。

## 11. Lease 与崩溃恢复

即使 v1 只有本地 worker，也必须使用 lease：

- worker claim Attempt 时以 compare-and-swap 获取 lease；
- heartbeat 只能由当前 owner 和 fencing token 更新；
- lease 过期后由 recovery scanner 处理；
- scanner 先提交 `lease.expired`，再根据 Attempt 状态和 effect class 决定恢复；
- 新 owner 得到更大的 fencing token；
- 旧 owner 的 heartbeat、Receipt 和完成提交全部被拒绝。

进程启动恢复顺序：

1. 完成数据库 schema migration。
2. 校验 Event/Projection sequence。
3. 扫描非终态 Run。
4. 处理过期 lease 和 deadline。
5. 继续传播持久化 cancel/pause 请求。
6. 将符合条件的 Node 投影为 READY。
7. 恢复调度。

恢复器禁止仅依据 Web 聊天状态、`Session.history`、`latest.json` 或 telemetry 判断任务状态。

## 12. Domain Event 与 Telemetry 分离

| 项目 | Domain Event | Telemetry Event |
|---|---|---|
| 目的 | 正确性、恢复、审计 | 排障、统计、平台导出 |
| 写入失败 | 阻止状态提交 | 吞错/降级，不阻断执行 |
| 内容 | 状态转换和有界事实 | 延迟、token、显示摘要 |
| 顺序 | Run 内连续 sequence | best-effort 时间顺序 |
| 保留 | 受运行和审计策略约束 | 可采样、轮转、删除 |
| 重放 | 可以重建 projection | 只能渲染轨迹 |

推荐映射：

```text
Domain Event commit
  -> 提交成功后生成 telemetry
  -> EventSink / PlatformSink / OTel exporter
```

不得在同一 SQLite 事务内调用远端 telemetry exporter。导出失败不回滚 Domain Event。
为避免导出遗漏，可以使用可重扫的本地 export cursor；cursor 仍不是执行事实。

现有 `session_id` 作为 telemetry correlation id 保留，新事件使用 `run_id`。兼容期内 trace
metadata 同时包含二者，但禁止假设二者永远一一对应：一个 Web chat 可以产生多个 Run，
一个父 Run 也可以包含多个 Agent Activity session。

Logical replay 只读取 Domain Event、Receipt 和 Artifact，禁止执行 Activity；现有
JSONL replay 继续是 telemetry viewer，名称和 UI 必须明确区分。

## 13. Workflow 调度语义

### 13.1 确定性

Workflow 编译和调度只能依赖：

- 固化的 WorkflowDefinition；
- 已提交 Domain Event；
- 已持久化 Node 输出和 Artifact；
- 显式 router 输出。

禁止依赖当前时间、随机数、未持久化文件内容或进程全局状态直接改变控制流。若需要这些
输入，必须通过 Activity 获取并持久化。

### 13.2 并发

- 默认 Agent Loop 内工具调用仍串行。
- DAG 中互不依赖的 Node 可以并发，但必须同时满足全局并发上限、工具并发上限和
  `resource_keys` 锁。
- 修改同一 workspace 的未知工具默认共享 workspace 写锁。
- 同一 BrowserDriver/session 的扫描、切换、导航和 JS 保持串行。
- join 只能在所有所需上游处于终态且满足 join policy 后 READY。
- 并发节点完成顺序不能改变确定性输出映射；声明了 `input_mapping` 的 join 按规范化
  输入名归并已验证 ArtifactRefs，并把该顺序持久化为自身输出 receipt；未声明 mapping
  的兼容 join 保持原控制面结果。

### 13.3 子工作流

subworkflow 创建 child Run，并记录 `parent_run_id / parent_node_id`。父 Node 只有在
child Run 终态后才能完成。pause、cancel、deadline 和 sensitivity 默认向下传播。

## 14. 旧系统迁移

### 14.1 Agent Loop

- 现有 `run_agent_loop()` 不改变生成器协议和 `ActionResult`。
- 新增 Legacy Agent Activity adapter，把一次 Agent Loop 运行包装为一个 Node Attempt。
- `ActionResult` 继续是 Agent Loop/Handler 内部兼容契约，禁止直接作为持久化 schema。
- adapter 将最终响应、exit reason、turns、工具摘要转换为类型化 `NodeResult` 和 Artifact。
- `should_exit`、`next_prompt=None` 等旧退出语义只在 adapter 内映射，编排核心不解析 prompt。
- Agent Activity 内的工具在完成逐工具 Receipt 接入前，整体按保守 effect class 处理；
  不能因为 Agent 最终返回成功就推断所有中间副作用具备 exactly-once。

### 14.2 旧 checkpoint

现有 checkpoint 继续可读写一段兼容期，但规则变为：

- 新 Durable Run 的恢复事实来自 Event Store，不来自 `latest.json`。
- checkpoint 可以作为人类可读 snapshot 或迁移输入，写入失败不影响已有 Durable
  状态，但不得声称 checkpoint 已保证恢复。
- 导入旧 checkpoint 时创建一个新 Run，记录原 checkpoint id、digest、agent、旧状态，
  并把 resume prompt 保存为输入 Artifact。
- 导入 Run 标记 `recovery_mode=legacy_prompt`；它是“根据摘要继续”，不是原 Run 的
  精确续跑。
- Web 必须继续绑定聊天的特定 checkpoint，禁止恢复到 workspace `latest`；迁移完成后
  聊天改为绑定 `run_id`。
- 旧 checkpoint 不自动转换已完成工具为 verified Receipt。

### 14.3 Team Workflow

现有 `<team>.workflow.json` version 1 通过编译器迁移：

- 每个 step 转成 `agent` Node。
- 文件顺序保留为稳定显示顺序。
- `depends_on` 继续校验只能引用已知 step；迁移编译器可以保留旧“只引用前序”的约束。
- 为保持旧行为，未显式启用 durable graph 的 v1 Workflow 按文件顺序补充隐式依赖，
  因此仍串行。
- `{{steps.<id>.response}}` 和 `output` 映射为上游 NodeResult/Artifact 引用。
- `on_error=stop` 映射为 `fail_run`，`continue` 映射为允许下游读取结构化失败结果。
- step child Agent 继续独立 Session、tools、skills、memory 和 AgentContext。
- 旧 Workflow 文件不原地改写；编译后保存不可变 definition digest。

当前 Durable Workflow schema v2 已支持：

- 显式 parallel、router、join、map 和 subworkflow 控制节点；
- node timeout/retry/resource keys；
- 由 Run input 或传递依赖 Node output 选择 Artifact 的严格 `input_mapping`；
- 子工作流定义持久为不可变 Artifact，内存 registry 为空时可经全局 binding 恢复；
- 同名 mapping 按 key 排序、map 聚合按 child index 排序，完成先后不改变结果。

通用输入输出 JSON Schema 执行期校验和外部 DSL 选择仍属于后续 ADR。

### 14.4 Telemetry 和 Web

- 独立 Web projection adapter 只通过 Run 查询和 Event stream 展示状态，不持有权威
  执行状态；legacy `web_ui_new.py` 不自动挂载 Durable Store。
- 服务重启时 Web 不自行把 Durable Run 强制改为空闲；它展示 projection 的真实状态。
- Web 详情和 SSE 初始 snapshot 必须在同一个 SQLite read transaction 中读取
  Run/Node/Attempt projection，并使用有硬上限的分页查询；禁止拼接不同提交版本的
  projection。常规列表、详情和 stream 禁止隐式重放完整 Event History。完整 replay
  校验只由显式单 Run integrity endpoint 触发。该端点是 operator-only：router 未注入
  明确 authorizer 时默认拒绝；通过授权后仍必须使用固定 Event 数、累计 canonical
  payload bytes 和协作式 wall-time 上限，超限只返回固定安全错误码。
- SSE 掉线只影响显示，客户端用有界 Run snapshot + sequence 恢复。
- 现有 telemetry API 保持只读兼容，逐步增加 `run_id` 过滤。

## 15. 兼容边界

v1 必须满足：

- 未启用 Durable Orchestration 时，现有 CLI、Web 单 Agent 和 Team Workflow 行为不变。
- 默认工具和 `ActionResult` 公共字段不做 breaking change。
- `Session.history` 的裁剪和 provider 协议行为不改变。
- `tools/` 纯函数边界保持；Receipt 由 Handler/adapter/Activity 层生成。
- 工作区业务文件、Memory、Runbook、Agent 和 Team 配置格式不改变。
- Durable 元数据和 Artifact 全部位于 Agent workspace 外的受信控制面根，不参与文件
  搜索、业务 diff 和 Agent 内容语义，也不向 Agent 工具进程挂载。
- 新 API 只能以可选字段增加 `run_id`、`node_id`、status 和 sequence；旧客户端忽略后仍可用。
- Event、Receipt、Artifact 的 schema 必须有 migration，禁止依赖 Python pickle。
- 写入 Event Store 失败时 Durable Run fail closed；不能静默回退为仅 checkpoint 模式继续执行。

## 16. 数据安全与保留

- Event 只保存执行元数据和 digest；原始 prompt、response、tool args/result 默认不入库。
- Artifact 根据 sensitivity 执行访问控制；`secret` Artifact 禁止远端 telemetry 导出。
- Run、Node、Attempt 与 Artifact metadata 必须在 Unicode、大小写和分隔符规范化后
  递归拒绝 credential/cookie/token/password/private-key 等敏感键；禁止静默 redaction
  导致 canonical identity 碰撞。需要保存的敏感内容必须通过受保护的 ArtifactRef 边界。
- 错误摘要必须有长度上限并经过凭据脱敏。
- MCP transport 对 malformed、oversized 和 over-deep 输入使用独立的按 transport
  context token bucket，并在 JSON parse / 结构遍历前尽可能预占；合法结构在进入普通
  请求限流前退还该预算。无 context 请求共享固定 anonymous bucket，context 表满后未知
  context 进入共享 overflow bucket，禁止通过 LRU 驱逐重置坏输入预算。
- `args_digest` 使用规范化、脱敏后的参数计算；需要验证精确外部请求时另存受保护 Artifact。
- GC 只能删除无 Event/Receipt 引用的 Artifact，或依据明确保留策略删除已终态 Run 的
  全套可删除数据。
- 删除 Domain Event 会破坏 replay。v1 默认不做部分 Event 删除；若需要合规删除，必须
  删除整个 Run 及其 Artifact，或先实现可验证的 redaction tombstone 方案。

## 17. 分阶段交付与验收

### Phase 0：契约与基线

交付：

- 本 ADR、状态转换表、错误分类表、故障注入清单。
- 固定当前单 Agent、checkpoint、Team Workflow 和 telemetry 回归基线。

验收：

- 所有既有测试通过。
- 每个领域对象、状态和事件有 schema version。
- 不存在“通用 exactly-once”承诺。

### Phase 1：Event Store 与单节点 Run

交付：

- SQLite schema/migration、Domain Event append、projection rebuild。
- Run/NodeRun/Attempt 状态转换 API。
- 最小本地 Artifact Store：content-addressed 写入、digest/size 校验、临时文件 +
  原子替换、Artifact 元数据登记，以及孤立文件 GC。Phase 1 的成功结果禁止只存在于调用栈。
- Legacy Agent Activity adapter；一个旧单 Agent 任务映射为单节点 Workflow。

验收：

- 任意已提交 sequence 后 kill 进程，重启 projection 与离线 rebuild 完全一致。
- 重复 `event_id` 不产生重复事件，冲突 payload fail closed。
- canonical golden event stream 经在线 reducer 和离线 rebuild 后逐字段完全一致。
- Activity 完成提交后即使调用方收不到响应，也能从 Artifact/NodeResult 恢复结果而不重新执行。
- 未启用 durable 模式时行为不变。
- Store 写入失败时 Activity 不越过开始门禁。

### Phase 2：Receipt、Retry、Timeout、Cancel、Lease

交付：

- ToolReceipt、idempotency record、effect class。
- RetryPolicy、持久 deadline、lease/heartbeat/fencing token。
- pause/cancel 传播和 recovery scanner。

验收：

- 过期 worker 无法提交结果。
- read-only 和测试用幂等写入在故障注入下可安全恢复。
- 不可探测非幂等写入在不确定窗口中必定进入 `WAITING_RECOVERY`，不自动重试。
- 重启后取消继续传播；已过期 deadline 立即生效。
- 受保护工具的重复可见副作用率为 0。

### Phase 3：DAG Runtime

交付：

- Workflow v2 compiler、ready queue、router、parallel、join、map、subworkflow。
- 全局/工具/资源并发限制。
- Team Workflow v1 adapter。

验收：

- 环检测、无效引用、schema 不匹配在执行前失败。
- 独立只读节点可真实并行。
- workspace/browser 冲突节点严格串行。
- 并发完成顺序变化不影响最终输出映射和 projection。
- 父 Run 取消/暂停/deadline 正确传播到 child Run。

### Phase 4：Policy、Approval 和 Artifact 安全增强

交付：

- 工具 capability/effect metadata、PolicyDecision、Approval Node。
- 在 Phase 1 最小 Artifact Store 上增加 lineage、sensitivity、访问控制、保留策略和
  deployment-managed encryption metadata；不重复实现基础写入和 digest 契约。

验收：

- 高风险 Activity 在 approval/policy Event 提交前不能开始。
- 重启后审批请求和决议不丢失、不重复执行。
- Event Store 不包含测试凭据和完整敏感 payload。
- Artifact 缺失或 digest 错误时 Run fail closed。

### Phase 5：Replay、Eval 和 Observability 对齐

交付：

- Domain logical replay、projection inspector、run fork。
- Domain Event 到现有 EventSink/OTel 的映射。
- 故障注入和可靠性报表。

验收：

- logical replay 的外部 Activity 调用数为 0。
- 在线 projection 与 replay projection 100% 一致。
- telemetry exporter 全部失败时，Domain Run 仍正确完成。
- 能报告恢复成功率、`OUTCOME_UNKNOWN` 率、重复副作用率、取消泄漏率和 replay divergence。

### Phase 6：远程执行参考面

Phase 1–5 稳定后已交付独立
[分布式执行 ADR](distributed-execution-adr.md)、
[故障矩阵](distributed-execution-fault-matrix.md)和
[runnable quickstart](distributed-execution-quickstart.md)。该参考面验证会话身份、
fencing、Artifact grant、runtime proof、取消与 replay；不得把它解释为生产
gVisor 或持久 broker registry。其后新增了严格、有界的
[HTTPS/ASGI transport](remote-worker-https-transport.md)、证书 pin identity 和 TLS
客户端，以及由服务端 policy 驱动的
[跨 Run Fleet 数据面](remote-fleet-data-plane.md)。同一 Store 内的 Fleet
global/tenant/pool quota 已与 Attempt claim 原子持久化；生产化仍需验证
TLS-extension server/PKI 部署、跨控制面共享 broker 或单写 shard ownership、跨
Store quota，并在 fleet claim 失败后从 Store 重新投影。

## 18. 故障注入矩阵

以下测试必须使用可控 fake Activity 和持久测试 workspace，不依赖 timing 碰运气。

| 编号 | 注入点 | 预期持久状态 | 恢复后要求 |
|---|---|---|---|
| F01 | `run.created` 事务前崩溃 | 无 Run | 同一用户请求可重新创建，不产生残缺 Run |
| F02 | Event append 后、projection 更新前抛错 | 整个事务回滚 | sequence 无空洞 |
| F03 | Tx A 后、claim 前崩溃 | Attempt=SCHEDULED | 可重新 claim，同一 Attempt 不重复创建 |
| F04 | Tx B claim 提交后、Tx C `attempt.started` 前崩溃 | CLAIMED + lease，且无 `attempt.started` | lease 过期后 ABANDONED，可新建 Attempt |
| F05 | `attempt.started` 后、只读工具前崩溃 | RUNNING | lease 过期后可安全重试 |
| F06 | 只读工具返回后、Tx D 前崩溃 | RUNNING、无 Receipt | 旧 Attempt ABANDONED，新 Attempt 重跑 |
| F07 | 幂等写入完成后、Tx D 前崩溃 | RUNNING、外部已有 operation key | probe/同 key 重试，外部只产生一个可见结果 |
| F08 | 非幂等不可探测写入完成后、Tx D 前崩溃 | RUNNING、无 Receipt | OUTCOME_UNKNOWN，Run WAITING_RECOVERY，零自动重试 |
| F09 | Artifact 写完、登记前崩溃；或 GC claim/move 时崩溃、并发登记引用 | 孤立 Artifact 或持久 `moving`/`quarantined` claim | Event Tx 与 claim 线性化；新引用不会指向隔离内容；重启恢复或 fail closed，普通失败回滚 |
| F10 | Tx D 的 Artifact 登记/Receipt 事务失败 | 无完成 Event | 按 effect class 恢复，不能生成悬空引用 |
| F11 | Tx D commit 后响应丢失 | SUCCEEDED + Receipt | 重复完成请求幂等返回，不重新执行 |
| F12 | worker lease 过期后旧 worker 返回 | 新 fencing token 已生效 | 旧结果被拒绝并审计 |
| F13 | retry backoff 期间重启 | WAITING_RETRY + deadline | 到期后只调度一次新 Attempt |
| F14 | **已满足**：`LocalProcessSupervisorBackend` execution timeout 与 selector 注册故障（受控 parent/child/grandchild fixture） | TIMED_OUT；或监督异常 fail closed；输出仅以 ArtifactRef 返回 | Popen 后任意监督异常均对独立进程组执行 TERM→有界等待→KILL→wait，关闭 selector/streams，且无运行中后代；`tests.test_orchestration_process_backend` |
| F15 | timeout 时外部 HTTP 结果未知 | OUTCOME_UNKNOWN | 不把 timeout 当未执行 |
| F16 | cancel 请求提交后立即崩溃 | CANCELLING | 重启继续传播，最终 CANCELLED |
| F17 | cancel/pause/resume intent 与 Attempt success 或重复 intent 并发 | 单一合法事务顺序 | projection 已推进时有界重读并收敛；同版本真实冲突不吞；success Receipt 必须保留 |
| F18 | pause 时有运行中 Activity | PAUSING | 到安全边界才 PAUSED，不启动新 Attempt |
| F19 | 等待审批时重启 | WAITING_APPROVAL | 同一 approval id 恢复，不重复请求/执行 |
| F20 | 审批通过后、Activity 调度前崩溃 | approval.resolved | 重启后只调度一次 |
| F21 | 并行两节点完成顺序颠倒 | 两个独立 sequence | join 输出顺序确定 |
| F22 | 父 Run 取消时 child Run 运行中 | 父 CANCELLING | child 收到取消，父等待其收敛 |
| F23 | Domain Event 成功、telemetry 全失败 | 正确 Domain 状态 | Run 不受影响，trace 可缺失 |
| F24 | telemetry 成功、Domain Event 失败 | 无状态提交 | 禁止仅凭 trace 继续 |
| F25 | projection 人为删除/损坏 | Event 完整 | rebuild 得到相同状态 |
| F26 | Event schema 未知 | rebuild 停止 | 报 migration required，不静默跳过 |
| F27 | 重复 event id、相同 payload | 只保留一条 | 返回原提交结果 |
| F28 | 重复 event id、不同 payload | 完整性冲突 | fail closed |
| F29 | Web SSE 断线/重连 | Run 继续 | 从 snapshot + sequence 恢复显示，不重复执行 |
| F30 | 导入旧 checkpoint | 新 legacy_prompt Run | 不伪造旧 ToolReceipt，不使用 workspace latest 猜测 |

可靠性测试至少记录：

```text
recovery_success_rate
duplicate_visible_side_effect_rate
outcome_unknown_rate
projection_replay_divergence_rate
cancellation_leak_rate
stale_worker_commit_rejection_rate
p50/p95 resume_latency
```

## 19. 待后续 ADR 裁决

以下问题不阻塞 v1，但不得在实现中暗自决定：

1. **补偿/Saga**：哪些工具可以声明 compensation、补偿失败如何升级；v1 只记录能力，
   不提供通用自动补偿。
2. **远程 worker 协议**：认证、传输、网络分区、调度公平性和 worker capability；
   必须在本地 lease/fencing 故障测试稳定后另写 ADR。
3. **Event 保留与合规删除**：长期审计、磁盘预算和隐私删除的具体默认值。
4. **Workflow v2 外部格式**：继续扩展 Team JSON，还是引入独立 YAML/JSON DSL；
   内部领域模型不依赖最终文本格式。

### 19.1 外部设计校准

本规范的边界参考以下官方资料，并只吸收可由 XAgent 自身测试证明的原则：

- [Temporal: What is Temporal?](https://docs.temporal.io/temporal)：Durable Execution
  依靠 Event History 在故障后恢复状态和进度。对应本规范的 Event 事实源、可重建
  projection 和确定性恢复。
- [Temporal: What is an Activity?](https://docs.temporal.io/activities)：Activity 应是
  边界清晰的小工作单元，推荐幂等；失败后的新 Attempt 从初始状态开始，长任务通过
  heartbeat details 保存可恢复进度。对应本规范的 Activity/Attempt 分离、幂等键和
  heartbeat timeout。
- [LangGraph: Interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts)
  和 [Persistence](https://docs.langchain.com/oss/python/langgraph/persistence)：恢复
  interrupt 会重新进入节点，interrupt 前的副作用必须可重放；checkpoint/thread
  identity 是恢复定位依据。对应本规范的 approval/input 持久等待、稳定 Run ID，以及
  “副作用先有身份、未知时不盲重试”。
- [MCP 2026-07-28 Tools](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)
  和 [Versioning](https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning)：
  现代协议使用无会话、每请求携带版本与客户端能力的模型；工具注解是不可信提示，
  服务端仍须校验输入、实施访问控制、限流并净化输出。对应本规范的
  Policy/Approval/Sandbox 门禁、稳定 Run handle 和 transport-neutral Runtime API。
- [OpenTelemetry Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/)：
  telemetry 是可互操作的观测投影，不是业务事务或恢复事实。对应本规范的
  Domain Event/telemetry 分离和 best-effort exporter。

这些参考不构成“兼容某框架”的声明。验收仍以第 18 节故障矩阵、projection replay
一致性和显式可靠性指标为准。

## 20. 最终正确性边界

Durable Orchestration v1 可以声称：

- 已提交的编排状态可重建；
- 状态转换和 projection 本地原子；
- 运行时崩溃后能保守恢复；
- 幂等/可探测工具可安全重试；
- 不确定非幂等副作用不会被盲目重放；
- logical replay 不产生外部副作用；
- 取消、暂停、deadline 和 lease 有持久语义。

它不能声称：

- 任意工具 exactly-once；
- 任意外部副作用自动回滚；
- 进程被 kill 时一定知道外部操作是否完成；
- 单机 SQLite lease 已等价于分布式共识；
- checkpoint 摘要或 telemetry 日志能够替代执行事实。

系统的可靠性来自显式暴露这些边界，并在未知时停止，而不是用“恢复成功”掩盖无法证明的外部状态。
