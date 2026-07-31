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

结论：schema v5 已证明共享同一 Store、按 `(tenant,pool)` 分配的 strict Fleet
单写者不会被 scope alias 绕过；但同一 pool 的 tenant 由不同控制器持有时，进程内
round-robin 仍可能形成多个互不知情的公平域。第七轮通过 pool authority 和持久 cursor
关闭该设计缺口。

## 第七轮：pool authority 与事务性公平恢复

### 第一遍：公平域、双写与迁移对抗

第一性约束是“谁能推进一个公平队列，谁就必须拥有这个完整队列”。tenant
round-robin 的状态按 pool 维护，因此 `(tenant,pool)` owner 无法阻止两个控制器分别
持有同池不同 tenant，并各自从本地默认 cursor 调度。schema v6 把唯一约束提升为
`pool_id`：同一 pool 只能有一个 shard/owner，ownership envelope schema v2 不再包含
tenant，而 Projector 允许该 owner 为池内多个 tenant 生成 binding，但拒绝另一 pool。

v5→v6 迁移按 pool 分组。只有 owner 与 policy digest 全部一致时才合并，选择确定性
最小 shard id，并把 epoch 提升到旧最大值加一以 fencing 所有旧 envelope；split
owner、split policy 或耗尽的 epoch 使整个迁移事务回滚，不发布半迁移表。旧 trigger
缺失也可幂等升级，不会因不完整旧安装卡死。

证据：

- `test_pool_owner_authorizes_multiple_tenants_but_not_another_pool`
- `test_concurrent_shard_aliases_cannot_share_a_routing_scope`
- `test_version_five_pool_owners_consolidate_and_fence_legacy`
- `test_version_five_split_pool_ownership_fails_migration`
- `test_version_five_exhausted_epoch_fails_migration_atomically`
- `test_version_five_malformed_owner_fails_migration_atomically`

### 第二遍：失败窗口、事务回滚与 epoch 对抗

持久 cursor 与 owner 共表，记录 `last_served_tenant` 和单调
`selection_sequence`。Store 在验证 exact shard/pool/owner/epoch/policy 后、写入
Attempt 的同一 `BEGIN IMMEDIATE` 事务中推进 cursor。因此 quota 满、claim CAS
冲突、Event 写入失败或进程在 commit 前崩溃时，cursor 与 Attempt 一起回滚；成功
claim 后即使 assignment 返回前崩溃，cursor 与已存在的 durable claim 一致。sequence
耗尽时拒绝新 claim，不允许整数回绕。

transfer 保留 tenant/sequence，但同时提升 epoch 并替换 owner/policy，故新控制器可
延续公平位置，旧 cursor identity 又不能通过就绪门。两个并发 Store writer 仍由
SQLite 写事务线性化；cursor 表不是跨 Store 共识。

证据：

- `test_fleet_cursor_advances_only_with_successful_durable_claim`
- `test_exhausted_fleet_cursor_fails_before_durable_claim`
- `test_fleet_shard_transfer_is_clock_free_monotonic_and_aba_safe`
- `test_concurrent_fleet_shard_transfer_has_one_winner`
- `test_shard_transfer_fences_stale_queue_then_new_epoch_claims`

### 第三遍：恢复顺序、陈旧投影与有界性

strict production-ready 不只要求 callback 声明 durable quota/ownership/fairness，
还要求每个 binding 的 pool cursor 已恢复，且 cursor 与 binding 的
shard/pool/owner/epoch/policy 完全相同。`rebuild()` 先在候选 scheduler 恢复全部
cursor，再接纳任务；缺 cursor、重复 pool 或陈旧 identity 会在交换内存状态前拒绝。
直接组合也必须先 `restore_fairness_cursor()` 再 `admit()`。这样重启不会因任务先入队
而覆盖持久轮转位置。

恢复 registry 上限为 4096，与 Store pool owner 上限一致；已恢复 pool 的 cursor 不会
因暂时没有 Worker/任务被清除。它是 durable restart continuity，不把 Fleet snapshot
升级为执行事实，也不恢复 queue、Worker session 或 active routing。

证据：

- `test_restored_pool_cursor_preserves_restart_fairness`
- `test_cursor_restore_requires_an_idle_pool`
- `test_restored_pool_cursor_registry_is_bounded`
- `test_strict_multi_control_readiness_requires_durable_cursor`
- `test_strict_rebuild_restores_matching_pool_cursor_atomically`
- `test_strict_rebuild_rejects_stale_cursor_before_state_swap`

结论：共享同一 Store 的 strict pool 单写者、fencing 和重启公平连续性 P0/P1=0。
共享 broker、跨 Store 共识/cursor、自动接管与在线无损 queue handoff 仍是明确残余
边界。

## 第八轮：受信 Run 投影与撤销优先控制循环

### 第一遍：policy 撤销、Run 退出与 queue 竞态

只提供 `rebuild()` 会留下一个执行窗口：Tool routing policy 已变化或 Run 已退出，
但旧 queued binding 在部署手写 rebuild 前仍可被 Worker 领取。新的
`reconcile_queued()` 先完整验证 desired snapshot，再在 Coordinator 同一把锁内完成
withdraw-old / admit-new。与 Worker poll 并发时只有两个合法线性化结果：poll 先赢则
旧 binding 已成为 active durable authority，reconcile 报延期；reconcile 先赢则
Worker 只能看到新 binding。不存在旧、新队列同时可领取的中间态。

`DurableFleetReconciler` 不从 Fleet snapshot 猜 Run，而从 production-ready
`FleetRunSource` 获取有界 Run/tenant/pool 路由，经 scheduler resolver 和
`DurableFleetProjector` 重新读取 current Store。Run 不再 `RUNNING` 时 desired 为空，
旧 queued binding 被主动撤回；policy digest 改变但 task id 相同时，旧 binding 先撤回
再以新 exact binding 入队。

证据：

- `test_queue_reconcile_replaces_stale_policy_binding`
- `test_policy_reconcile_and_poll_have_one_queue_linearization`
- `test_policy_change_withdraws_old_queue_before_replacement`
- `test_run_exit_withdraws_queued_projection`
- `test_run_source_converges_queue_idempotently`

### 第二遍：active authority、容量压力与未知结果

active assignment 已经越过队列线性化点，可能已提交 durable claim，不能因 route 删除
或 policy 变化被内存控制循环伪装撤销。`WithdrawalOutcome.ACTIVE` 因此保留 exact
binding 并计入 `deferred_active_tasks`；terminal callback、exact terminal probe 或
lease recovery 释放后，下一轮再收敛。这个语义同时覆盖 callback “Store 已提交但返回
失败”的未知结果窗口。

对抗容量压力时，stale queued 仍优先撤销，但 active binding 永不驱逐。每次新 admit
前重新检查当前 binding registry；“active old + desired new”不能绕过
`max_task_bindings`。新项因 registry/tenant/pool/queue capacity 被拒绝时只增加
`rejected_tasks`，旧 policy queued item 不会为了可用性被复活。telemetry 更新在锁外
best-effort 执行，不影响 queue 事实。

证据：

- `test_queue_reconcile_defers_active_authority_until_terminal`
- `test_queue_reconcile_never_evicts_active_binding_for_capacity`
- `test_queue_reconcile_validates_entire_projection_before_mutation`
- `test_queue_reconcile_ignores_observer_failures`
- `test_terminal_probe_and_retry_projection_converge_together`
- `test_reconciler_probe_failure_retains_projection`

### 第三遍：route authority、边界与 restart bootstrap

Run route source 是受保护控制面 authority，必须显式声明 production-ready；route 值只
包含有界 run/tenant/pool 标识，不允许 Worker、Run input 或任意 metadata 扩权。
reconciler 以 `max_routes + 1` 请求 snapshot，拒绝静默截断、重复 Run、resolver
异常、缺失 durable Run、重复 task 和 coordinator capacity 溢出。任何动态 readiness
撤销或非中断异常都会先把 desired 视为空并撤回所有 queued binding，再暴露原错误；
active authority 仍只等待 terminal。因此 route authority 不可用时停止新执行，而不是
静默沿用旧 policy。所有失败路径都禁止新 admission，quarantine 自身仍使用同一个
queue linearization。

strict 模式对每个 route 从对应 Store 读取 current pool ownership/cursor，校验本控制
世代 owner，并在接纳 task 前幂等恢复 cursor。同一个调度 pool 不能跨两个独立 Store：
否则两个 Store-local owner/cursor 都无法代表合并后的公平域，控制循环在任何 queue
mutation 前拒绝。合法 epoch transfer 只有在该 pool 本地 queue/active 已 drain 时才
能替换 cursor authority；有 queued task 时首轮先 quarantine 并报错，下一轮 idle
retry 安装新 authority；有 active task 时继续等待 terminal。

证据：

- `test_strict_bootstrap_restores_cursor_once`
- `test_cursor_progress_during_multi_route_snapshot_is_allowed`
- `test_strict_pool_cannot_span_independent_stores`
- `test_source_overflow_fails_before_queue_mutation`
- `test_scheduler_resolution_failure_quarantines_queued_work`
- `test_non_production_run_source_is_rejected`
- `test_revoked_run_source_quarantines_queued_work`
- `test_authority_change_quarantines_old_queue_then_retries`
- `test_concurrent_control_rounds_converge_to_one_binding`
- `test_shard_transfer_fences_stale_queue_then_new_epoch_claims`

结论：显式 Fleet projection 控制轮次的 policy/Run 收敛、active authority 保留和
restart bootstrap P0/P1=0。生产 Run registry、自动 failure detector/接管、共享 broker
与跨 Store 共识仍是部署或后续阶段边界。

## 残余边界

- Fleet queue、Worker registry 和 active routing 是单进程 projection，不是共享
  broker。同一 Store 的 quota 与 pool fairness cursor 已原子化，strict pool shard
  ownership 可保证可信控制器单写；不同 Store shard 的 quota、ownership 与 cursor
  不会自动合并。
- 自动 failure detector、接管编排和在线无损 queue handoff 尚未实现。所有权记录
  不自动删除，以免 epoch 重置产生 ABA；部署必须规划 4096-pool
  上限。
- 升级前存在的无 scope 远程 active claim 会阻塞新 Fleet admission；必须 drain 或
  隔离 Store，不能绕过该安全门。
- Store claim 已提交、assignment 返回前进程崩溃时，Worker 不获得执行权；lease
  recovery 会收敛该 claim。该窗口不能伪装成零 mutation。
- ready Run route、tenant/pool 归属仍来自部署注入的受保护 registry；参考实现提供
  `StaticFleetRunSource` 和显式 `DurableFleetReconciler.run_once()`，但不隐式启动
  后台线程、不从 Run input 猜身份，也不负责 Domain scheduler reconcile。
- digest-only execution binding 已可跨重启从 Store/当前配置/新鲜 attestation 重建，
  且不会从 Worker 候选结果恢复 bearer。Artifact broker 只持久化 token digest、消费/
  失败墓碑和 exact finalized ref；持有原 grant/handle 的 Worker 可跨 broker 重启完成
  单次读取或成功 completion，但控制面仍不能重新发送 assignment/bearer。
- 生产声明仍依赖真实 TLS-extension server、PKI/Workload Identity 和经过验证的
  Sandbox 部署。

这些边界均 fail closed 或明确交给 lease/reconcile，不把 telemetry、Fleet snapshot
或 HTTP 成功当作 Domain execution truth。
