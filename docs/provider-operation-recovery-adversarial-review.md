# Provider Operation Recovery 三轮对抗性审查

> 审查对象：`RecoverableProviderInvoker`、`ProviderOperationRecovery`、
> `ProviderAccessBroker` unknown reconciliation、`RemoteExecutionJournal`
> schema v5
>
> 结论：参考实现的本机恢复状态机通过；外部 gateway 的幂等账本与 attestation
> 仍是未由本仓库证明的生产前置条件，因此 `production_security_ready` 保持 false。

## Round 1：unknown、zombie 与重复调用

攻击问题：

- invoker 抛错后，网关证明 operation 从未开始时能否安全重试；
- 进程只留下原始 `invoking` 时，延迟/zombie caller 是否会与恢复重试并行；
- 多个 Broker 同时获得 NOT_STARTED 证据时是否会产生多个 retry winner；
- IN_PROGRESS/UNKNOWN 是否被误当成可重试。

修正与证据：

- grant ID 是稳定 operation ID，恢复查询同时绑定 route 与实际 payload SHA-256；
- invoker 失败先把 receipt 收敛为 durable `outcome_unknown`；只有该状态可以在
  `BEGIN IMMEDIATE` 中追加 NOT_STARTED evidence，并以
  `outcome_unknown → invoking` CAS 取得一次 retry claim；
- 原始 `invoking` 即使收到 NOT_STARTED 也保持 unknown，不写 evidence、不调用
  provider，因为旧 caller 可能仍然存活；
- 并发查询可以重复，但 journal CAS 只允许一个调用进入 retry invoker；
- retry CAS 在线性化事务内再次检查 grant 未过期，恢复查询跨过 expiry 时不启动调用；
- IN_PROGRESS、UNKNOWN 与未显式声明 recovery-ready 的 adapter 全部 fail closed。

对应测试：

- `test_verified_not_started_retries_one_unknown_call`
- `test_not_started_never_retries_raw_invoking_zombie`
- `test_concurrent_not_started_recovery_claims_one_retry`
- `test_not_started_cannot_retry_after_grant_expires`
- `test_retry_evidence_cannot_claim_at_or_after_expiry`
- `test_in_progress_unknown_and_unready_never_retry`
- `test_not_started_evidence_only_retries_durable_unknown`

Round 1：在“部署 gateway 的稳定 operation ID 账本真实可靠”的声明范围内通过。

## Round 2：completed result、绑定与持久化

攻击问题：

- gateway 已完成、控制面只留下 invoking/unknown 时能否恢复 exact response；
- operation ID、route、payload digest 或 response digest 被替换时能否拼接结果；
- completed recovery 后再次重启是否调用 provider；
- response、prompt、bearer 或 credential 是否进入 recovery journal。

修正与证据：

- `ProviderOperationRecovery` 是 exact schema：COMPLETED 必须携带非空有界 bytes 与
  匹配 SHA-256，其他状态禁止夹带 response；
- Broker 重验 operation/route/request 三重绑定和 route response-size 上限；
- recovered response 先按原 authorization sensitivity 写 `MODEL_RESPONSE`
  Artifact，再把 response digest、canonical ArtifactRef 与 COMPLETED evidence
  原子收敛到 journal；重启后从 Artifact 重放，不再调用 invoker；
- schema v5 evidence 只记录 grant/request/evidence digest、decision、verifier 和时间，
  不记录证据原文或 response；数据库/WAL/SHM canary 验证 request、response、bearer 与
  upstream secret 均不存在；
- v1/v2/v3 直接原子迁移到 v5，v4 只增加 evidence 表且保留既有 invocation receipt。

对应测试：

- `test_completed_gateway_evidence_recovers_and_replays`
- `test_recovery_binding_mismatch_fails_closed`
- `test_operation_recovery_record_is_strict_and_payload_safe`
- `test_exact_v1_schema_migrates_atomically_to_v5`
- `test_exact_v2_schema_migrates_atomically_to_v5`
- `test_exact_v3_schema_migrates_atomically_to_v5`
- `test_exact_v4_schema_migrates_atomically_to_v5`

Round 2：通过。

## Round 3：伪造能力、异常泄露与持久证据攻击

攻击问题：

- 任意带 `recover` 方法的对象是否会被自动视为生产安全 gateway；
- readiness/recovery 内部异常是否泄露 API key 或保留 exception chain；
- evidence 是否可无限增长、断序、脱离 grant/invocation 或在过期后残留；
- 一个瞬时 NOT_STARTED 查询是否足以证明 exactly-once。

修正与证据：

- 只有运行时满足 `RecoverableProviderInvoker` 且
  `operation_recovery_ready is True` 才进入查询协议；`1` 等 truthy 值不提升能力；
- recovery record 必须是精确 `ProviderOperationRecovery` 类型，拒绝可覆写字段语义的
  子类；
- readiness/recovery 异常统一映射为固定
  `provider_operation_recovery_failed`，不保留 cause/context；
- evidence 每 invocation 最多 16 条、sequence 必须从 1 连续增长，启动时重验
  consumed grant、invocation、request digest 与 COMPLETED terminal state；孤儿、断序、
  malformed row 拒绝打开数据库；
- NOT_STARTED evidence digest 在同一 grant 内只能授权一次；重复旧证据不会以“幂等”
  为名绕过 16 次硬上限，启动校验同样拒绝重复 digest 与过期 NOT_STARTED；
- evidence 与 invocation 先于过期 grant 在同一 purge transaction 删除；
- schema 校验逐个验证预期 index 的所属表、唯一性和精确列，不能只伪造同名错误索引；
- 文档明确把 NOT_STARTED 定义为线性一致、单调 operation ledger 的结论：延迟旧请求和
  恢复请求必须使用同一 operation ID 去重。参考 Protocol 和 readiness 布尔值不能证明
  外部系统满足该语义。

对应测试：

- `test_recovery_errors_are_sanitized_without_exception_chain`
- `test_recovery_record_subclass_cannot_override_semantics`
- `test_not_started_evidence_cannot_authorize_two_retries`
- `test_not_started_evidence_digest_is_single_use`
- `test_duplicate_recovery_evidence_digest_fails_startup`
- `test_expired_not_started_evidence_fails_startup`
- `test_rebound_expected_index_fails_at_startup`
- `test_orphan_or_noncontiguous_recovery_evidence_fails_startup`
- `test_recovery_evidence_is_bounded_and_purged_with_grant`
- `test_completed_evidence_resolves_invoking_idempotently`
- `test_expiry_indexes_exist_and_unexpected_trigger_fails_closed`

Round 3：参考边界通过，生产能力声明仍拒绝提升。

## 剩余生产前置条件

- 对 provider operation ledger、verifier identity 与 adapter binary 做部署级
  attestation；
- 使用 secret manager、mTLS、gateway egress allowlist 与跨主机线性一致存储；
- 验证 upstream/gateway 对稳定 operation ID 的去重、保留期和灾备语义；
- 为永久 UNKNOWN 提供受权 operator reconciliation、费用核对与审计工作流；
- 把 provider recovery receipt 与远程 Agent 的逐工具 receipt、整体
  `AgentActivityReceipt` 组合后，再做端到端三轮审查。
