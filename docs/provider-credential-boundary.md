# Provider Credential 与模型网关边界

## 1. 安全目标

远程 Agent 需要调用模型，但远程 Worker 不应持有 OpenAI、Anthropic 或兼容服务的长期
API key。真正的 provider credential 必须留在部署侧模型网关；Worker 只能得到一次模型
调用所需的短期、窄作用域 gateway grant。

本边界区分三类对象：

- **upstream credential**：长期 API key、OAuth client secret、云厂商签名密钥；只存在于
  部署拥有的 `ProviderInvoker` 及其 secret manager；
- **gateway grant**：可经受保护传输交给 Worker 的短期 bearer，只允许一次有界模型调用；
- **durable evidence**：route/request/authorization digest 和消费墓碑，不含上述任一明文
  secret。

`ProviderAccessBroker` 是该契约的参考实现。它可注入 durable
`RemoteExecutionJournal`，但仍不会开启生产 remote Agent。

## 2. 既有本地 LLM 路径

CLI/Web 的兼容路径仍以原始字符串持有 `SessionConfig.apikey` 和 `BaseSession.api_key`。
这些对象不是远程协议，也不是 durable model，禁止对其使用 `dataclasses.asdict()`、
`vars()` 或通用 JSON 序列化。

最低日志安全基线：

- SessionConfig、BaseSession、ToolClient、NativeToolClient、MixinSession、
  EmbeddingConfig、XAgent 和 AgentContext 的 credential/endpoint/embedding config
  不进入 repr；
- ToolCall arguments、ChatResponse raw provider body 和 Session prompt/history 不进入
  repr；
- 向量索引只持久化 endpoint SHA-256 fingerprint，不把 embedding base URL 写入 SQLite；
- transport、provider body、failover 异常只向上返回固定 `ProviderRequestError.reason_code`；
- 原始 URL、响应 body 和异常 cause/context 不保留在上层异常对象。

这只解决意外诊断泄露，不把兼容路径提升为远程 credential transport。

## 3. Deployment-owned route

`ProviderRouteDescriptor` 只包含无凭据的模型网关元数据：

```text
route_id
tenant_id / pool_id / worker_rule_id
provider / model
gateway_binding_digest
maximum_request_bytes / maximum_response_bytes
```

route 由服务端静态目录注入，不能由 Workflow、Agent task、Worker 注册声明或 request
Artifact 创建。签发时必须同时匹配 WorkerAuthorization 的 tenant、pool 和 rule。
`gateway_binding_digest` 用于绑定部署选择的网关身份/配置，但参考实现不据此完成 mTLS、
网络出口或网关 attestation；这些仍是生产 `ProviderInvoker` 的责任。

## 4. One-call grant

`ProviderAccessGrant` 绑定：

- tenant、worker、Run、Node、Attempt；
- Worker action digest 和 authorization lineage digest；
- Scheduler request digest 与 AgentActivityRequest Artifact digest；
- provider route 的完整 credential-free descriptor；
- 单调的 `invocation_index`；
- 响应 Artifact 分类；为防止 Worker 降级分类，它必须等于
  WorkerAuthorization 的 `maximum_artifact_sensitivity`；
- issued/expiry 时间。

token 使用 CSPRNG 生成并从 repr 隐藏。`to_wire_dict()` 会有意携带 token，因为 Worker
必须把它提交给模型网关；它不得进入 Event、Attempt、checkpoint、recovery journal、
telemetry 或普通日志。Broker 只保留 token SHA-256 和 grant binding digest，不保留
token 明文。

同一 tenant/Run/Node/Attempt/request/invocation-index 在活跃墓碑窗口内只能签发一次，
即使改换 Worker 或 route 也不能复制同一逻辑模型调用。

## 5. Invoke 与消费顺序

Broker 的调用顺序固定为：

1. 使用受信 verifier 重验新鲜 WorkerAuthorization；
2. 重验 route、tenant、worker、Attempt、action、authorization 和 request binding；
3. 校验 payload 是非空 bytes 且不超过 route 上限；
4. 在 SQLite `BEGIN IMMEDIATE` 事务中用 constant-time token digest 比较，以
   `issued → consumed` CAS 提交墓碑，并插入绑定实际 payload digest 的 `invoking`；
5. 仅把 credential-free route、raw request bytes 和 grant ID 交给部署侧
   `ProviderInvoker`；
6. 若配置 result store，将有界 response 写为按 sensitivity 分类的 MODEL_RESPONSE
   Artifact；
7. 提交 completed response digest/ArtifactRef receipt；
8. 将 invoker 的有界 bytes 包装成 repr-safe `ProviderInvocationResult`。

消费发生在 provider 调用之前。invoker 抛错、返回类型错误或响应超限时 token 仍已花费，
错误只返回固定 reason code，禁止把 provider 异常或响应内容保留为 cause/context。

`ProviderInvoker` 自己持有或从 secret manager 临时解析 upstream credential。Broker、
grant、route、Worker 和 orchestration Store 均不接触 API key。

grant ID 同时是稳定 provider operation ID。普通 `ProviderInvoker` 不声明查询或
幂等能力；只有部署显式注入 `RecoverableProviderInvoker` 且其
`operation_recovery_ready is True` 时，Broker 才会尝试恢复 unknown operation。

## 6. Durable grant registry

默认 Broker 使用进程内 SQLite，仅用于本地参考。部署方可注入位于 Worker 不可访问的
durable `RemoteExecutionJournal`。schema v3 保存：

```text
grant id / logical invocation digest / token digest / binding digest
route id / issued|consumed / expires_at / updated_at
provider purge watermark
```

它不保存 token、request/prompt、response、provider credential、endpoint 或完整
authorization。逻辑调用唯一索引和 `BEGIN IMMEDIATE` 使共享同一数据库的多个 Broker
实例只能签发一次、消费一次。consumed tombstone 在 TTL 内不得因容量压力被淘汰。

过期删除只允许经显式 purge 事务发生。journal 同时持久化 purge 时间水位；发生时钟
回拨后，早于水位的签发、消费和清理全部 fail closed，避免“先前跳删除、再回拨复活”。
这要求所有共享 journal 的 Broker 使用一致、可信的 wall clock。水位导致的拒绝需要
operator 修复时钟，不能通过清空安全数据库绕过。

`durable_recovery_ready` 只表示 grant/tombstone 可跨 Broker 重启恢复：内存 journal
返回 false，磁盘 journal 返回 true。它不等价于端到端生产 readiness。

## 7. Invocation receipt 与结果 Artifact

schema v4 用独立 `remote_provider_invocations` 表记录：

```text
grant id / actual request payload SHA-256
invoking | completed | outcome_unknown
response SHA-256 / optional canonical ArtifactRef / updated_at
```

首次调用在同一事务中把 grant 置为 consumed 并插入 `invoking`。因此重启或并发重放不会
再次进入 invoker。实际 payload digest 在该线性化点绑定；同一个 bearer 不能换 prompt
重放。

若注入受信 ArtifactStore，成功 response 先写为 `MODEL_RESPONSE` Artifact，再把 exact
ref 与 response digest 提交为 completed。response bytes、prompt 和 provider body
不进入 SQLite。journal commit 后响应丢失时，相同 bearer + authorization + payload
可以验证 Artifact 并重放相同 bytes；Artifact 写成功而 journal 未提交只留下可 GC
orphan，状态仍为 unknown，不会重调 provider。

Result Artifact 必须精确匹配 grant 的 sensitivity、Run/Node/Attempt producer、digest、
size、kind 和空 metadata。参考 `LocalArtifactStore` 不加密，因此拒绝 SECRET；
SECRET 必须由返回 `deployment_managed` encryption 的部署 Store 处理。

没有 result store 时，首次调用仍返回 response，并记录 completed response digest；
响应丢失后的重放返回 `provider_result_unavailable`，不会重新调用 provider。
`durable_result_recovery_ready` 仅在磁盘 journal 与项目内可证明落盘的
`LocalArtifactStore` 同时存在时为 true；自定义 ArtifactStore 不会被参考实现自动声明
为 durable。

## 8. 可验证 operation 恢复

schema v5 增加有界、append-only 的恢复证据表：

```text
grant id / sequence / actual request payload SHA-256
not_started | completed / evidence SHA-256 / verifier id / created_at
```

`RecoverableProviderInvoker.recover()` 必须按稳定 operation ID 与 payload digest 返回
严格的 `ProviderOperationRecovery`：

- `IN_PROGRESS`、`UNKNOWN`：继续返回 outcome unknown，绝不调用 provider；
- `COMPLETED`：必须返回与 response digest 精确匹配的有界 bytes；Broker 先按原
  sensitivity 写 Result Artifact，再原子提交 completed evidence/receipt；
- `NOT_STARTED`：只有 journal 已经明确收敛为 `outcome_unknown` 时，才能在同一事务中
  追加证据并以 CAS 重新取得一次 `invoking` claim；原始 `invoking` 可能仍有 zombie
  caller，因此即使网关报告 NOT_STARTED 也禁止重试。

多个 Broker 同时拿到 NOT_STARTED 时，只有一个能完成
`outcome_unknown → invoking` CAS。其余调用保持 unknown，不能形成并行上游重试。
completed evidence 可安全收敛 `invoking` 或 `outcome_unknown`，因为它不会再次产生
provider 副作用。

NOT_STARTED evidence digest 在同一 grant 内只能使用一次，每次后续 retry 必须取得新的
gateway evidence；否则固定旧证据会绕过 16 条上限。retry CAS 还会在事务内再次检查
grant 未过期，防止恢复查询跨过授权截止点后启动新调用。COMPLETED 的同一终态证据允许
幂等读取，但不能改变已登记的 response digest/ArtifactRef。

NOT_STARTED 不是普通最终一致查询的瞬时快照。生产 gateway 必须以 grant ID 作为稳定
幂等键，在线性一致、单调的 operation ledger 上生成证据，并保证延迟到达的旧请求与
恢复后的请求只能竞争同一个 operation；否则“查询未开始—旧请求随后到达”的竞态仍会
重复计费。`evidence_digest` 与 `verifier_id` 只是参考 journal 接受过哪份部署证据的
审计记录，参考实现不会替部署验证 gateway attestation。

`provider_operation_recovery_ready` 只反映注入 adapter 明确声明了该协议能力，不等价于
durable result recovery，更不等价于生产 readiness。journal 不保存 response、
prompt、bearer、provider credential 或证据原文；每个 invocation 最多保存 16 条证据，
并与过期 grant 一起清理。

## 9. 当前 fail-closed 限制

- 签发响应丢失后无法重取同一个 token，只能等待墓碑过期或由 operator 处理；
- token 在调用 invoker 前消费；若进程在 `invoking` 后、completed receipt 前崩溃，
  普通 invoker 状态为 outcome unknown，安全地拒绝重调；
- RecoverableProviderInvoker 可用可信 NOT_STARTED/COMPLETED 证据收敛部分 unknown
  窗口，但参考实现无法证明部署 adapter 背后的 operation ledger、attestation 或
  upstream 幂等保证真实成立；
- 同一 SQLite 文件可跨本机进程线性化，但不提供跨主机共识、复制或自动故障转移；
- `production_security_ready` 仍固定为 false；
- 参考实现不提供 mTLS、provider egress allowlist、secret manager、经过 attestation
  的 invoker。

生产 remote Agent 仍必须提供经过验证的 upstream idempotency/operation ledger、
经过 attestation 的 ProviderInvoker 和完整远程 Agent runtime。未完成前禁止把
`agent` 加入 `SecureRemoteAssignmentAdmitter`、`SecureRemoteExecutionAdapter`、
Fleet projector 或 Worker daemon 的 `supported_activity_kinds`。

credential 基线审查见
[Provider Credential 三轮对抗性审查](provider-credential-adversarial-review.md)，
durable journal 审查见
[Provider Grant Journal 三轮对抗性审查](provider-grant-journal-adversarial-review.md)，
invocation result 审查见
[Provider Invocation Receipt 三轮对抗性审查](provider-invocation-receipt-adversarial-review.md)，
operation 恢复审查见
[Provider Operation Recovery 三轮对抗性审查](provider-operation-recovery-adversarial-review.md)。
