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

`ProviderAccessBroker` 是该契约的单进程参考实现，不会开启生产 remote Agent。

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
4. 在锁内用 constant-time token digest 比较并把 grant 标记 consumed；
5. 仅把 credential-free route、raw request bytes 和 grant ID 交给部署侧
   `ProviderInvoker`；
6. 将 invoker 的有界 bytes 包装成 repr-safe `ProviderInvocationResult`。

消费发生在 provider 调用之前。invoker 抛错、返回类型错误或响应超限时 token 仍已花费，
错误只返回固定 reason code，禁止把 provider 异常或响应内容保留为 cause/context。

`ProviderInvoker` 自己持有或从 secret manager 临时解析 upstream credential。Broker、
grant、route、Worker 和 orchestration Store 均不接触 API key。

## 6. 当前 fail-closed 限制

参考 Broker 的 grant/tombstone registry 仅在内存中，因此：

- `production_security_ready` 与 `durable_recovery_ready` 固定为 false；
- 进程重启后既有 token 因服务端记录丢失而不可兑换，安全上 fail closed、可用性上丢失；
- 签发响应丢失后无法重取同一个 token，只能等待墓碑过期或由 operator 处理；
- provider 已执行但响应丢失、随后网关重启时，当前实现没有 durable invocation receipt，
  不能证明是否已经产生费用；
- 多副本网关不能共享消费墓碑或逻辑调用唯一性；
- 参考实现不提供 mTLS、provider egress allowlist、secret manager 或上游 idempotency。

生产 remote Agent 必须先提供受保护的 durable provider-grant journal、跨副本原子消费、
invocation receipt/恢复语义和经过 attestation 的 ProviderInvoker。未完成前禁止把
`agent` 加入 `SecureRemoteAssignmentAdmitter`、`SecureRemoteExecutionAdapter`、
Fleet projector 或 Worker daemon 的 `supported_activity_kinds`。

三轮审查证据见
[Provider Credential 三轮对抗性审查](provider-credential-adversarial-review.md)。
