# Remote Fleet 数据面六轮对抗审查

审查对象：

- `remote_protocol.py` / `remote_worker.py` 的 `poll_fleet`；
- `remote_fleet.py` 的 routing projection；
- `remote_fleet_control.py` 的可信组合；
- `RemoteControlPlane` 的精确 claim 与 terminal release；
- `DurableScheduler.prepare_next_admission()` 的目标节点路径。

攻击者可控制 Worker 进程、register 声明、请求重试/乱序、断线和执行结果，但不能修改
控制面进程、服务端 Worker/Tool policy、mTLS identity 或 Durable Store。

## 第一轮：一致性与精确候选

### 已关闭 P1

旧 Fleet binding 只关联 Run 和 Tool。一个 Run 中有多个同名 Tool 节点时，routing
选中的任务可能被 control 的“下一个 ready 节点”替换。

修复后 production binding 必须包含精确 `node_id`、Activity config digest 和版本化
Tool routing policy digest。Scheduler 的只读 admission 接受目标节点；config
不一致、节点不存在或 candidate CAS 失败都在对应 mutation 前拒绝。返回的 durable
assignment 再次逐字段匹配。

证据：

- `test_exact_projection_selects_second_same_tool_node`
- `test_exact_candidate_mismatch_has_zero_durable_mutation`
- `test_reference_binding_without_node_digest_is_not_network_ready`
- `test_tool_routing_policy_is_complete_versioned_and_fail_closed`

结论：P0=0，未关闭 P1=0。

## 第二轮：崩溃、lease 与终态容量

### 已关闭 P1

lease reaper 可以在 Worker 之外把 Attempt 推进到 terminal，但原 Fleet active
assignment 没有 completion 请求触发 release，长期运行会泄漏 Worker、pool 和 tenant
容量。

修复后 `SecureRemoteFleetPoller` 保留有界、非事实型的 active 索引，并提供 exact
terminal reconcile。probe 异常或非 terminal 保守保留；只有 Durable Store 证明终态
才释放。索引使用 `(run_id, attempt_id)`，不假设多个 Store 的 Attempt id 全局唯一。
正常 completion/replay/cancel 的 release 失败不会回滚已提交终态，而由 reconciler
修复。

证据：

- `test_reconciler_releases_lease_reaper_terminal_projection`
- `test_reconciler_probe_failure_retains_projection`
- `test_active_index_is_scoped_by_run_across_store_shards`
- `test_fleet_poll_claims_and_releases_real_durable_assignment`
- `test_terminal_release_rejects_forged_session_without_mutating_fleet`

结论：P0=0，未关闭 P1=0。

## 第三轮：身份、会话与 capability 提权

### 已关闭 P1

如果 routing 直接信任 register body，恶意 Worker 可声明额外 capability 或更高容量，
影响跨 Run 选择；session replacement 还可能在 routing 与 Store claim 之间发生。

修复后 Worker descriptor 完全由当前认证 registration 与不可变服务端 policy 合成。
capabilities、resource keys 和 capacity 都取交集/最小值，tenant、pool 和 tools 只取
服务端值。Fleet callback 携带当前 session digest；control 在 claim 前检查，并在
Store 线性化 guard 内再次检查 current registration。poll body 为空，不能选择 Run
或 lease。

证据：

- `test_server_policy_intersects_untrusted_worker_claims`
- `test_server_policy_is_exact_and_cross_tenant_is_denied`
- `test_claimer_rechecks_exact_session_before_store_mutation`
- `test_resolver_cannot_override_authenticated_session`
- `test_fleet_body_cannot_choose_run_or_lease`
- `test_fleet_poll_is_exposed_through_authenticated_https_surface`

结论：P0=0，未关闭 P1=0。

## 第四轮：异构路由与策略漂移

### 已关闭 P1

初版集成没有把 Workflow resource keys 纳入 Fleet compatibility，可能把任务预留给
缺少资源权限的 Worker，再由 durable candidate 拒绝并丢掉投影。另一个缺口是只用
默认 `activity.tool` 作为需求，无法证明路由 capability 与实际工具策略一致。

修复后 `RemoteTask` 和 `WorkerDescriptor` 都包含有界资源集合，compatibility 在 claim
前检查 subset。Tool 的完整 capability 需求来自版本化
`FleetToolRoutingPolicy`；策略缺失拒绝投影，策略 digest 变化与旧 binding 冲突并要求
显式 rebuild。目标 Fleet claim 只验证该节点的 Activity kind，不再因同 Workflow 中
尚未 ready 的其他 Activity 类型错误拒绝。

证据：

- `test_resource_requirements_are_part_of_worker_compatibility`
- `test_server_resource_policy_prevents_misrouting_and_task_loss`
- `test_exact_fleet_claim_does_not_require_unrelated_activity_kind`
- `test_tool_routing_policy_is_complete_versioned_and_fail_closed`

结论：P0=0，未关闭 P1=0。

## 第五轮：多控制器 durable quota

### 第一遍：并发、重启与终态释放

旧 global/tenant/pool quota 只读取每个进程自己的 routing active map。两个控制器共享
Store、或控制器重启后 active map 为空时，都可能同时通过本地检查。

修复后 routing 生成固定字段的 `FleetAdmissionScope`，Store 在
`claim_activity_with_policy()` 的同一 `BEGIN IMMEDIATE` 事务中按 active Attempt
检查 global/tenant/pool 上限并写入 scope。两个独立 Scheduler 并发 claim 同一 tenant
只能有一个成功；新 Store/Scheduler 实例仍读取旧 active scope，终态提交后容量才释放。

### 第二遍：策略漂移、旧版本与保留域绕过

quota policy 的 defaults、override map 和 global limit 形成 canonical digest。只要
active Fleet Attempt 属于另一 policy generation，后续 claim 就 fail closed；同一
generation 下 global limit、同 tenant limit 和同 pool limit 还必须逐字段一致。
旧版本 `remote-session:` active Attempt 没有 scope 时，新 Fleet claim 以
`fleet_unscoped_remote_active` 拒绝，要求先 drain。

`fleet_admission` 是 Store 保留 metadata 域。已带 scope 的 SCHEDULED Attempt 不能由
普通 run-scoped claim 跳过 quota CAS，也不能替换 scope；输入字段、类型、标识符、摘要
和上限都严格验证，失败时 schedule/claim/policy Event 整体回滚。

### 第三遍：authority、敏感信息与有界性

生产 callback 要求 scope 的 task/tenant/pool/routing digest 与 exact Fleet binding
一致；Control 再验证 scope tenant 等于认证 registration tenant，Worker 无法通过空
`poll_fleet` body 选择或伪造 scope。持久内容只有标识、摘要和整数上限，不含 bearer、
脚本、参数、环境或 Artifact 内容。Store 只扫描 status partial index 内的 active
Attempt；终态历史不会增加 admission 扫描集合。

证据：

- `test_fleet_tenant_quota_is_atomic_across_control_projections`
- `test_fleet_global_and_pool_quotas_use_durable_active_attempts`
- `test_fleet_quota_policy_drift_fails_closed_while_active`
- `test_fleet_claim_waits_for_unscoped_remote_upgrade_drain`
- `test_invalid_fleet_scope_rolls_back_scheduling_mutation`
- `test_fleet_scope_cannot_charge_another_tenant`
- `test_legacy_production_callback_without_durable_quota_is_rejected`
- `test_fleet_poll_claims_and_releases_real_durable_assignment`

结论：同一 Store 的 quota P0/P1=0；共享 queue/fairness 和跨 Store quota 仍是明确残余
边界。

## 第六轮：多控制器队列单写者与 fencing

### 第一遍：双活、scope alias 与最终线性化

只把 queue 保存在各进程内，即使 durable quota 不超限，两个控制器仍可能选择同一
ready task。初始所有权方案还存在两个绕过：持有任意 shard 可以给另一
tenant/pool 投影，或者两个不同 shard 指向同一 routing scope。

修复后 `fleet_shard_owners` 对 `(tenant_id, pool_id)` 建唯一约束；ownership envelope
同时包含 shard、scope、owner、policy digest 和单调 epoch。Projector 先检查 scope，
但安全线性化点仍在 `claim_activity_with_policy()` 的 `BEGIN IMMEDIATE` 内：Store
重新读取当前 owner row 并与 exact envelope 比较。陈旧队列在 schedule/claim/policy
Event 提交前整体回滚。

### 第二遍：owner 身份、ABA、崩溃与时钟

owner ID 不是秘密，因此只校验 Store row 不足以防另一个可信控制进程误用读到的记录。
strict coordinator 与 claimer 都必须配置同一个 `fleet_owner_id`，且 binding owner
必须等于该实例身份。计划接管使用 exact current-value CAS，每次都增加 epoch；并发
transfer 只能有一个成功，A→B→A 不能使 A 的旧 envelope 再次有效。

部署必须为每个存活控制进程世代配置唯一 owner ID；两个副本复用同一个 owner 名称
会破坏单写假设。相同 owner 的幂等恢复仅在外部已经 fencing/确认旧进程死亡时使用，
否则接管必须换新 owner 并 transfer。

epoch 是唯一顺序依据。wall clock 回拨不会阻止 transfer，也不会恢复旧 authority；
`assigned_at` 只供审计。insert/transfer 的注入故障随事务回滚。4096 条硬上限拒绝新
scope 而不驱逐旧 owner，避免通过容量压力重置 epoch。

### 第三遍：就绪门、敏感信息与恢复

strict 模式缺少 owner 配置会在构造时拒绝；binding 缩减为无 ownership、scope
不匹配、callback 未声明 durable ownership 或 owner 不一致时，网络 poll 的动态
production-ready 门关闭。成功 claim 只持久化无密标识、摘要和 `fencing_epoch`；
曾使用的 `fencing_token` 命名会触发现有 credential-shaped metadata 防线，现已改为
准确的非秘密 epoch，而没有放宽敏感键规则。

仅靠新进程的 readiness 不能约束滚动升级中的旧二进制。ownership row 因此也是
Store 级 scope 开关：记录存在后，不带 ownership 的旧 Fleet claim 在同一事务中以
`fleet_shard_ownership_required` 拒绝。启用前已经 active 的无 scope claim 不会被
伪造终态，但 quota 升级门会要求它们先 drain。为覆盖迁移前已经构造、不会重新执行
版本检查的旧 Store 实例，schema v5 还在 active Attempt INSERT/UPDATE 上安装
ownership trigger；缺失或陈旧 envelope 由 SQLite 自身拒绝。

同 owner 重启可幂等读取当前 epoch；策略或 owner 变更必须显式 transfer。旧 owner
已经 `RUNNING` 的 Attempt 仍由原 Activity lease/fencing 收敛，但不能领取新任务；
尚处于 `CLAIMED` 的旧 epoch 不能再转为 `RUNNING`，由 lease recovery 接管。自动
故障检测不在 Store 内用超时推断，避免暂停或时钟异常触发双主。

证据：

- `test_fleet_shard_transfer_is_clock_free_monotonic_and_aba_safe`
- `test_concurrent_fleet_shard_transfer_has_one_winner`
- `test_concurrent_shard_aliases_cannot_share_a_routing_scope`
- `test_schema_trigger_fences_already_running_legacy_processes`
- `test_fleet_shard_faults_roll_back_and_capacity_never_evicts`
- `test_strict_multi_control_claim_persists_shard_fencing`
- `test_shard_transfer_fences_claimed_but_not_started_work`
- `test_running_work_can_finish_after_shard_transfer`
- `test_store_rejects_valid_ownership_for_another_scope`
- `test_shard_transfer_fences_stale_queue_then_new_epoch_claims`
- `test_strict_control_rejects_another_owners_binding`
- `test_owned_scope_blocks_legacy_control_claims`
- `test_strict_multi_control_readiness_rejects_unowned_binding`

结论：共享同一 Store、按 `(tenant,pool)` 分配的 strict Fleet 单写者 P0/P1=0。共享
broker、跨 Store 共识、自动接管和公平游标持久化仍是明确残余边界。

## 残余边界

- Fleet queue、Worker registry、active routing 和公平游标是单进程 projection，
  不是共享 broker。同一 Store 的 quota 已原子化，strict `(tenant,pool)` shard
  ownership 可保证可信控制器单写；不同 Store shard 的 quota 与 ownership 不会自动
  合并。
- 自动 failure detector、接管编排、跨进程公平游标和在线无损 queue handoff 尚未
  实现。所有权记录不自动删除，以免 epoch 重置产生 ABA；部署必须规划 4096-scope
  上限。
- 升级前存在的无 scope 远程 active claim 会阻塞新 Fleet admission；必须 drain 或
  隔离 Store，不能绕过该安全门。
- Store claim 已提交、assignment 返回前进程崩溃时，Worker 不获得执行权；lease
  recovery 会收敛该 claim。该窗口不能伪装成零 mutation。
- ready Run 的发现、周期投影、策略变更后的 rebuild 和 terminal reconcile 由部署的
  可信控制循环调用；本模块不隐式启动后台线程。
- digest-only execution binding 已可跨重启从 Store/当前配置/新鲜 attestation 重建，
  且不会从 Worker 候选结果恢复 bearer。Artifact broker 只持久化 token digest、消费/
  失败墓碑和 exact finalized ref；持有原 grant/handle 的 Worker 可跨 broker 重启完成
  单次读取或成功 completion，但控制面仍不能重新发送 assignment/bearer。
- 生产声明仍依赖真实 TLS-extension server、PKI/Workload Identity 和经过验证的
  Sandbox 部署。

这些边界均 fail closed 或明确交给 lease/reconcile，不把 telemetry、Fleet snapshot
或 HTTP 成功当作 Domain execution truth。
