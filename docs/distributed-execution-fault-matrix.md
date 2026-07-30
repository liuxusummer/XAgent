# Distributed Execution 故障注入验收矩阵

本文是 [distributed-execution-adr.md](distributed-execution-adr.md) 第 10 节
R01–R20 的可执行证据索引。所有 R 编号均直接映射到
`tests/test_orchestration_distributed_fault_matrix.py` 中的显式测试方法；仅有同类单元测试
不计作本矩阵证据。

矩阵使用逻辑时钟、固定并发 barrier、内存 transport、受控 fake 外部 ledger 和 fake
runtime adapter。它不等待真实时间窗口，不发起真实网络请求，也不宣称 reference
transport 提供 mTLS，或 fake runtime 提供真实 gVisor 隔离。

状态含义：

- **满足**：测试直接注入故障，并断言 ADR 要求的安全状态。
- **阻断**：红测已提交，当前实现仍允许规范禁止的结果；交付前必须修复。
- **满足（reference 边界）**：使用可运行的模块组合直接证明 ADR 控制边界，但不把
  reference 组件伪装成真实部署证据。

## R01–R20 证据

| ID | 状态 | 具体测试方法 | 可核验证据 |
|---|---|---|---|
| R01 | 满足 | `DistributedExecutionFaultMatrixTests.test_r01_unauthenticated_register_has_zero_state_change` | 未经认证 adapter 构造的调用在协议分发前拒绝；registration、Attempt 和 Domain Event 均无变化。 |
| R02 | 满足 | `test_r02_body_impersonation_never_overrides_transport_identity` | wire `worker_id` 与 transport identity 不一致时返回固定拒绝；body 注入 tenant identity 字段被 strict schema 拒绝，未创建 registration/Attempt。 |
| R03 | 满足 | `test_r03_same_request_id_with_different_body_is_conflict` | 相同 request id、不同 canonical body 返回 `request_id_conflict`；首次 registration 的 capacity 不被覆盖。第 1 轮专项审查另证明 digest identity 在 response LRU 淘汰和控制面重启后仍持久。 |
| R04 | 满足 | `test_r04_duplicate_poll_after_response_loss_has_one_attempt` | 丢弃首次 poll 响应后发送完全相同请求；响应可重放且 Store 中只有一个 Attempt。 |
| R05 | 满足 | `test_r05_wrong_heartbeat_token_and_fencing_do_not_extend_lease` | 分别篡改 claim token 和 fencing token；两次 heartbeat 均被拒绝，持久 lease deadline 不变。 |
| R06 | 满足 | `test_r06_replaced_session_cannot_use_old_claim_authority` | 同当前 instance 从持久 journal 恢复相同 epoch 后可 start 原 claim；新 instance 原子递增 epoch。第 1 轮专项审查另证明 A→B→A 的旧 instance tombstone 跨控制面重启仍拒绝，旧/新 session 均不能伪造原 claim 终态。 |
| R07 | 满足 | `test_r07_crash_before_start_recovers_claimed_as_abandoned` | poll 后不 start，逻辑 lease 到期；Reaper 将 CLAIMED 解析为 ABANDONED，旧 start 被 fencing 拒绝且没有第二个并发 Attempt。 |
| R08 | 满足 | `test_r08_read_only_crash_after_start_fences_old_completion` | RUNNING read-only lease 到期后创建新 Attempt；新 fencing 更高，旧 completion 被拒，旧 Attempt 保持 ABANDONED。 |
| R09 | 满足 | `test_r09_idempotent_write_disconnect_probes_single_effect` | fake 外部 ledger 用稳定 operation key 只写一次；断网窗口由可信 probe 返回已提交 Artifact，Reaper 将原 Attempt 置为 SUCCEEDED，不创建第二个 Attempt。 |
| R10 | 满足 | `test_r10_unprobeable_write_disconnect_is_outcome_unknown` | started non-idempotent write 失联后即使 retry budget 充足也进入 `OUTCOME_UNKNOWN/WAITING_RECOVERY`，Attempt 数仍为 1。 |
| R11 | 满足 | `test_r11_cancel_completion_race_preserves_the_winning_receipt` | barrier 固定 cancel/completion 并发；Store 只形成一个合法事务顺序。completion 胜出时真实 ArtifactRef 保留，否则 stale completion 不伪造 Receipt，取消显式确认。 |
| R12 | 满足 | `test_r12_drain_poll_race_has_no_post_drain_claim` | drain 与 poll 同 barrier 竞争；最终 Worker 必为 DRAINING，后续 poll 恒拒绝；在线性化点前已领取的 assignment 可释放收敛。 |
| R13 | 满足 | `test_r13_capacity_one_concurrent_poll_claims_at_most_one` | 16 个并发 poll 攻击 capacity=1 Worker；最多一个 assignment，active_count 精确为 1。 |
| R14 | 满足 | `test_r14_tenant_quota_race_never_crosses_tenant_or_quota` | 两个 tenant-a Worker 与一个 tenant-b Worker 并发 poll；tenant active 均不超过 quota=1，assignment tenant 必须属于 Worker 授权集合。 |
| R15 | 满足 | `test_r15_hot_tenant_cannot_starve_an_eligible_tenant` | 热 tenant 每轮补充任务；持续 eligible 的 tenant-b 在前两个选择内获得一次服务。 |
| R16 | 满足（reference 边界） | `test_r16_expired_or_wrongly_bound_grant_leaks_no_bytes_or_refs` | read grant 的错 Worker 绑定和严格 expiry 均拒绝 redeem；write grant 的错 Worker stage 和过期 output handle 也 fail closed。没有返回 ArtifactPayload、暂存输出或新增 Domain Event/Attempt result。 |
| R17 | 满足（reference 边界） | `test_r17_artifact_digest_mismatch_prevents_sandbox_start` | read grant 签发后篡改 Store bytes，redeem 重新校验 digest/size 并拒绝；write grant 的 staged bytes 与声明 digest 不符也被拒绝。两条路径均未进入受控 sandbox-start seam。 |
| R18 | 满足（reference 边界） | `test_r18_control_plane_path_is_rejected_before_runtime_adapter` | 将控制面目录作为 cwd；execution binding/profile 校验在 runtime adapter 前 fail closed，adapter spec 调用为零。 |
| R19 | 满足（reference 边界） | `test_r19_forged_or_missing_attestation_never_claims_container` | attestation verifier 缺失证明或返回错绑 adapter identity 均拒绝构造 CONTAINER backend；runtime 调用为零。测试不声称 fake verifier 是真实 gVisor 证明。 |
| R20 | 满足（reference 边界） | `test_r20_telemetry_failure_does_not_mutate_scheduling_truth` | best-effort remote observation snapshot 的 exporter 整体抛错；前后 scheduling snapshot 完全一致，active assignment 保持，telemetry 明确 `execution_truth=false`。 |

## 组合不变量与剩余部署边界

- Store、Event、Attempt、lease、fencing 和 Receipt 仍只由控制面 mutation API 写入。
- Worker 协议对象、routing envelope、broker grant 和 OCI spec 均不包含 Store/Artifact
  物理路径；R16–R19 验证的是 reference 组合边界。
- R09/R10 证明的是 at-least-once Attempt 下的保守恢复，不是外部副作用 exactly-once。
- `OciGvisorSandboxBackend` 只有在注入 verifier 返回当前、精确绑定的 attestation 时才暴露
  `SecurityLevel.CONTAINER`；本矩阵的 fake verifier 只测试绑定逻辑，不证明本机实际运行
  gVisor。
- 真实部署仍须单独提供并验证 transport mTLS/SPIFFE adapter、Artifact 服务传输加密、
  OCI runtime/Kubernetes 配置与部署 attestation。缺少这些部署证据时，不得将 reference
  transport 或 fake runtime 描述为生产安全平面。

## 当前验收

- 满足：20 / 20（其中 5 项为 reference 边界组合证据）
- 阻断：0 / 20

计数以状态列为准：R01–R15 为直接“满足”，R16–R20 为“满足（reference 边界）”。
专项命令 `.venv/bin/python -m unittest
tests.test_orchestration_distributed_fault_matrix` 当前 20/20 通过；仍须保持 reference
边界声明，并继续执行三轮独立对抗审查。
