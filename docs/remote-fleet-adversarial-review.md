# Remote Fleet 数据面四轮对抗审查

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

## 残余边界

- Fleet queue/active registry 是单进程 projection，不是跨控制面共识队列。多控制面
  部署必须增加共享 broker 或按 shard 保证单写者。
- Store claim 已提交、assignment 返回前进程崩溃时，Worker 不获得执行权；lease
  recovery 会收敛该 claim。该窗口不能伪装成零 mutation。
- ready Run 的发现、周期投影、策略变更后的 rebuild 和 terminal reconcile 由部署的
  可信控制循环调用；本模块不隐式启动后台线程。
- digest-only execution binding 已可跨重启从 Store/当前配置/新鲜 attestation 重建，
  且不会从 Worker 候选结果恢复 bearer。Artifact grant/finalization registry 仍是
  进程内状态，因此成功输出 completion 在 broker 重启后继续 fail closed。
- 生产声明仍依赖真实 TLS-extension server、PKI/Workload Identity 和经过验证的
  Sandbox 部署。

这些边界均 fail closed 或明确交给 lease/reconcile，不把 telemetry、Fleet snapshot
或 HTTP 成功当作 Domain execution truth。
