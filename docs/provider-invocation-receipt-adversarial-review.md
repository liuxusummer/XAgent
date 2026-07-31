# Provider Invocation Receipt 三轮对抗性审查

> 审查对象：`RemoteExecutionJournal` schema v4、provider wire schema v2、
> `ProviderAccessBroker` 的 invocation claim/result persistence/replay
>
> 结论：本机 completed-result replay 边界通过；upstream 调用与 receipt 提交之间仍有
> 明确的 outcome-unknown 窗口，生产 remote Agent 仍未开放。

## Round 1：崩溃窗口与重复调用

攻击问题：

- grant 消费后、进入 invoker 前崩溃，重启是否会再次调用 provider；
- provider 返回并写入 Artifact 后、journal completion 前崩溃，是否会猜测 orphan；
- completion 已提交但响应未送达，重试是否第二次调用 provider；
- invoker、响应校验或 Artifact 写入失败后，bearer 是否可重放。

修正与证据：

- schema v4 将 invocation receipt 与 grant 分表；首次调用在同一
  `BEGIN IMMEDIATE` 中执行 `issued → consumed` 并插入 `invoking`；
- `invoking` 在重启后只返回 `provider_invocation_outcome_unknown`，不进入 invoker；
- response 先写 immutable Artifact，再以 exact ref 提交 completed；Artifact 写后、
  completion 前的崩溃只留下可 GC orphan；
- completed commit 后响应丢失，可用相同 bearer、authorization 和 payload 从 Artifact
  重放，invoker 调用次数保持一次；
- invoker/invalid response/persistence failure 将 receipt 收敛到
  `outcome_unknown`；即使该 refinement 写失败，durable `invoking` 仍保持 fail closed。

对应测试：

- `test_crash_after_claim_recovers_as_unknown_without_reinvoke`
- `test_completion_commit_then_response_loss_replays_without_reinvoke`
- `test_invocation_failure_is_sanitized_and_spends_grant`
- `test_result_store_failure_is_sanitized_and_becomes_unknown`

Round 1：通过。

## Round 2：payload、Artifact 与分类拼接

攻击问题：

- 同一 bearer 是否能在重放时换成另一段 prompt/payload；
- completed receipt 是否能拼接另一个 Run/Node/Attempt 的 ArtifactRef；
- response digest、size、kind、sensitivity、producer 或 Store bytes 被篡改时能否返回；
- Worker 是否能把 SECRET/SENSITIVE provider output 降级为 public/internal。

修正与证据：

- claim 原子记录实际 request payload SHA-256；completed/unknown/replay 全部绑定该 digest；
- replay 重验 grant token/binding/route/expiry、authorization 和 exact payload digest；
- Result Artifact 必须是 `MODEL_RESPONSE`、空 metadata、精确 producer、digest、size 与
  grant sensitivity，并再次执行 Store verify 和读取后 SHA-256；
- response sensitivity 必须等于 WorkerAuthorization 的最大 Artifact 分类，只能保守
  继承、不能由调用方调低；
- `LocalArtifactStore` 明确拒绝 SECRET；部署 Store 只有返回
  `deployment_managed` encryption 才能接受 SECRET result。

对应测试：

- `test_durable_grant_survives_restart_and_replay_tombstone_does_too`
- `test_corrupt_result_artifact_fails_closed_without_reinvoke`
- `test_result_classification_is_bound_and_secret_local_store_denied`
- `test_wrong_authorization_and_tampering_do_not_consume`

Round 2：通过。

## Round 3：秘密、schema 与能力真实性

攻击问题：

- prompt、response、bearer 或 upstream credential 是否进入 SQLite/WAL/SHM；
- response ArtifactRef、Store 错误或宿主路径是否通过 repr/exception cause 泄露；
- v1/v2/v3 迁移是否把旧 consumed grant 伪装为 completed；
- 任意实现了 ArtifactStore Protocol 的对象是否被错误宣称为 durable；
- malformed/orphan invocation row 是否在启动后才延迟暴露。

修正与证据：

- journal 只保存 request/response digest、状态和 canonical ArtifactRef，不保存正文；
  response bytes 只存在于按 sensitivity 隔离的 ArtifactStore；
- `ProviderInvocationResult` 同时隐藏 content 与 ArtifactRef repr；invoker/Store/parser
  异常转换为固定 reason code 且无 cause/context；
- 当前 exact v1/v2 直接原子迁移到 v5，exact v3 增 receipt/evidence 表，exact v4
  只增 evidence 表；v3 consumed grant 因缺失 receipt 只能恢复为 unknown，旧 wire
  v1 fail closed；
- `durable_result_recovery_ready` 只接受磁盘 journal + 项目内已知 durable 的
  `LocalArtifactStore`，自定义 Protocol 实现不自动提升 readiness；
- 启动 schema 校验逐行验证 invocation record、关联 grant 必须存在且为 consumed；
  orphan/malformed row 直接拒绝打开数据库。

对应测试：

- `test_grant_is_canonical_path_free_and_credential_free`
- `test_result_store_failure_is_sanitized_and_becomes_unknown`
- `test_exact_v3_schema_migrates_atomically_to_v5`
- `test_orphan_provider_invocation_fails_at_startup`
- `test_malformed_provider_invocation_fails_at_startup`
- `test_durable_grant_survives_restart_and_replay_tombstone_does_too`

Round 3：在声明的本机 completed-result replay 范围内通过。

## 后续进展与剩余工作

- schema v5 已加入可信 provider operation recovery 协议与 digest-only evidence
  journal，详见
  [Provider Operation Recovery 三轮对抗性审查](provider-operation-recovery-adversarial-review.md)；
- 仍需生产 gateway 对稳定 operation ID 提供经过 attestation 的线性一致幂等账本；
- outcome-unknown 的 operator resolution 与费用/外部效果 reconciliation；
- secret-manager backed、mTLS、egress allowlist 与 attested ProviderInvoker；
- 跨主机单写者 fencing 或线性一致存储；
- 将 provider receipt 与 AgentActivityRequest、逐工具 receipt、AgentActivityReceipt
  组合进真正的远程 Agent runtime 后重新做端到端状态机审查。
