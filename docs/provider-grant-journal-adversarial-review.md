# Provider Grant Journal 三轮对抗性审查

> 审查对象：`RemoteExecutionJournal` schema v3、
> `RemoteProviderGrantRecord`、`ProviderAccessBroker`
>
> 结论：本机 durable anti-replay 边界通过；后续 completed invocation result replay
> 不改变跨主机高可用与 upstream unknown window 未实现的结论，
> `production_security_ready` 保持 false。

## Round 1：重启、时钟与墓碑复活

攻击问题：

- Broker 重启后，已签发 token 是否因进程内状态丢失而无法兑换；
- 已消费 token 是否会在重启后重新变为可用；
- 系统时钟先前跳清理过期行、再回拨时，同一逻辑调用能否被重新签发；
- 容量触顶时，能否通过 LRU 淘汰未过期 consumed tombstone。

修正与证据：

- schema v3 持久化 grant id、逻辑调用 digest、token/binding digest、route、状态和
  时间，不持久化 bearer；
- `issued → consumed` 在 invoker 调用前通过 SQLite 事务提交，重启后仍拒绝重放；
- 普通消费不再隐式删除过期行，只有显式 purge 可以删除；
- purge 同事务推进持久化时间水位，水位之后的时钟回拨使签发、消费和再次清理
  fail closed；
- consumed 与 issued 共用硬容量，未过期 tombstone 不因容量压力淘汰。

对应测试：

- `test_durable_grant_survives_restart_and_replay_tombstone_does_too`
- `test_provider_tombstone_capacity_and_purge_clock_are_monotonic`

Round 1：通过。

## Round 2：跨 Broker 并发与绑定拼接

攻击问题：

- 两个 Broker/数据库连接同时签发同一逻辑 invocation，是否得到两个 bearer；
- 两个 Broker 同时消费同一 grant，是否调用 provider 两次；
- token、binding、route 或 expiry 拼接是否能绕过消费 CAS；
- schema v2 升级时是否会先暴露部分 provider 表。

修正与证据：

- 逻辑 invocation digest 有唯一索引，签发在 `BEGIN IMMEDIATE` 中完成检查与插入；
- 消费在同类事务中校验 exact route/binding/expiry 和 constant-time token digest，
  再以 `state = 'issued'` 条件更新；
- 任一绑定不一致统一返回 unavailable，不泄露哪一字段匹配；
- v1/v2 只在 exact schema/version 校验成功后迁移；当前实现于单事务直接升级到 v4，
  仍覆盖本阶段 v3 grant/clock schema，半迁移或未知 table/index/trigger/view
  fail closed。

对应测试：

- `test_separate_registries_serialize_issue_and_consume`
- `test_wrong_authorization_and_tampering_do_not_consume`
- `test_exact_v1_schema_migrates_atomically_to_v4`
- `test_exact_v2_schema_migrates_atomically_to_v4`

Round 2：通过。

## Round 3：秘密、崩溃窗口与能力真实性

攻击问题：

- SQLite 主文件、WAL、SHM 或对象 repr 是否包含 grant token、prompt、response 或
  upstream credential；
- journal/SQLite 异常是否通过 cause/context 暴露内部诊断；
- consume commit 后、invoker 调用前崩溃是否导致重复 provider 调用；
- durable grant registry 是否被错误描述为端到端 result recovery 或跨主机共识。

修正与证据：

- durable row 只保存 SHA-256 digest 和 credential-free route id；canary 扫描覆盖
  SQLite、WAL、SHM；
- Broker 在 catch scope 外转换为固定 reason code，上层异常无 cause/context；
- crash after consume 最多丢失一次调用可用性，不允许 bearer 重放；provider 已执行但
  响应丢失仍是明确的 unknown outcome；
- `durable_recovery_ready` 只反映 journal 是否落盘，
  `production_security_ready` 始终 false；
- purge 水位单例在 schema 启动校验中验证；缺失或损坏时数据库拒绝打开，不会先报告
  durable readiness、再延迟到首次调用失败；
- 当时的文档明确单机 SQLite 不提供复制、跨主机共识、invocation receipt、mTLS、
  attestation 或 upstream idempotency。

对应测试：

- `test_closed_registry_fails_without_leaking_exception_context`
- `test_durable_grant_survives_restart_and_replay_tombstone_does_too`
- `test_missing_provider_clock_singleton_fails_at_startup`
- `test_invocation_failure_is_sanitized_and_spends_grant`
- `test_grant_is_canonical_path_free_and_credential_free`

Round 3：在声明的本机 anti-replay 范围内通过。

## 剩余工作

- completed provider invocation/result receipt 与响应重放已在后续阶段完成，见
  [Provider Invocation Receipt 三轮对抗性审查](provider-invocation-receipt-adversarial-review.md)；
- 明确的上游 idempotency key 和 unknown-outcome operator protocol；
- secret-manager backed、mTLS、egress allowlist 与 attested ProviderInvoker；
- 跨主机线性一致存储或单写者 fencing；
- 接入远程 Agent 前，对 request Artifact、逐工具 receipt、最终 Activity receipt 和
  取消/恢复做组合状态机审查。
