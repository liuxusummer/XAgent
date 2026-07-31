# Provider Credential 三轮对抗性审查

> 审查对象：本地 LLM credential 诊断边界、ProviderRouteDescriptor、
> ProviderAccessGrant、ProviderAccessBroker
>
> 结论：credential 基线契约通过；后续 durable journal 不改变生产 remote Agent
> 仍未开放的结论。

## Round 1：credential 与诊断泄露

攻击问题：

- API key 是否通过 config/session/client/mixin/context 的嵌套 repr 泄露；
- embedding config、ToolCall arguments、provider raw body 和 history 是否重新暴露；
- embedding endpoint 是否通过 semantic fingerprint 落入 SQLite；
- HTTP/URL/provider body/failover 异常是否保留 secret 文本、cause 或隐藏的 context；
- gateway grant 是否错误携带 upstream API key、base URL 或 endpoint。

修正与证据：

- 所有 credential holder 及其常见组合的敏感字段均 `repr=False`；
- semantic fingerprint 只保留 endpoint SHA-256，不持久化原始 base URL；
- transport/body/failover 使用固定 `ProviderRequestError.reason_code`，转换在 catch scope
  外完成，异常对象不保留 cause/context；
- route/grant 只含 credential-free metadata；upstream secret 只存在于注入的 invoker；
- gateway token 仅存在于 wire grant，repr 隐藏；服务端 record 只保存 token digest。

对应测试：

- `test_credential_holders_and_compositions_have_safe_repr`
- `test_transport_diagnostics_and_causes_are_removed`
- `test_provider_body_is_not_copied_into_validation_error`
- `test_malformed_provider_json_does_not_survive_as_cause`
- `test_failover_does_not_rethrow_child_diagnostic`
- `test_grant_is_canonical_path_free_and_credential_free`

Round 1：通过。

## Round 2：身份、route 与 grant 拼接

攻击问题：

- 跨 tenant/pool/rule 是否能请求其他模型 route；
- grant 能否与另一个 WorkerAuthorization、request 或 token 拼接；
- 同一逻辑 `invocation_index` 是否能并发签发多个 token；
- unknown field、bool-as-int/time 和非规范 wire 值是否可绕过。

修正与证据：

- route 绑定部署侧 tenant、pool、worker rule，签发时逐项匹配；
- grant 绑定 Worker/Attempt/action/authorization/request/request-Artifact/route；
- 服务端用 token digest constant-time 比较，并比较 token-free binding digest；
- 活跃窗口内逻辑 invocation key 唯一，跨 Worker/route 也不能重复签发；
- exact-field parser 和显式 bool 检查 fail closed。

对应测试：

- `test_wrong_authorization_and_tampering_do_not_consume`
- `test_route_is_bound_to_tenant_pool_and_worker_rule`
- `test_logical_invocation_is_issued_only_once`
- `test_wire_schema_is_exact_and_bool_index_is_rejected`

Round 2：通过。

## Round 3：并发、崩溃与消费顺序

攻击问题：

- 同一个 grant 并发兑换是否调用 provider 多次；
- provider 抛错或响应超限后 token 是否还能重放；
- request/response/TTL/capacity 是否有无界资源路径；
- verifier/provider 异常是否泄露 secret；
- 重启和多副本是否被错误宣称为生产安全。

修正与证据：

- consumed 在 invoker 调用前于锁内设置，并发兑换恰好一个进入 invoker；
- invoker failure 和 invalid response 均保留 consumed 墓碑；
- request、response、TTL、invocation index 和 active grants 全部有硬上限；
- verifier/invoker 异常被固定 reason code 替换且无 cause/context；
- 当时的 readiness 固定 false，文档明确内存墓碑的响应丢失、重启与多副本限制。

对应测试：

- `test_concurrent_replay_invokes_gateway_exactly_once`
- `test_invocation_failure_is_sanitized_and_spends_grant`
- `test_request_response_and_expiry_limits_fail_closed`
- `test_capacity_and_verifier_errors_are_bounded`

Round 3：参考契约通过。

## 后续状态

- durable token-digest/logic-key/consumed tombstone journal 与共享同一本机 SQLite
  的跨 Broker 原子签发/消费已在后续阶段完成，审查证据见
  [Provider Grant Journal 三轮对抗性审查](provider-grant-journal-adversarial-review.md)；
- provider invocation receipt、响应丢失与上游 idempotency；
- mTLS/attestation、egress allowlist 和 secret-manager backed invoker；
- 与 AgentActivityRequest、远程 Agent runtime、逐工具 receipt、AgentActivityReceipt
  completion 的完整组合审查。
