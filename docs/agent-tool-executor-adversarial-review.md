# Durable Agent Tool Executor 三轮对抗性审查

> 审查对象：动态 child Store 账本、`DurableAgentToolExecutor`、policy/sandbox/result 组合、
> crash recovery 与 Handler/Agent Loop 集成。

## Round 1：identity、状态与父子聚合

攻击包括同 sequence 不同绑定、并发 reserve、伪造父 Attempt、超界 turn、SQL 字段篡改、
父 Agent 提前终结以及 preflight 异常留下永久 child。

结论：child identity 可重复推导且 reserve 事务幂等；父 Run/Node/Attempt/request digest 全部
在写事务内复验；turn 上限与 Agent Loop 契约一致；malformed projection 重读失败；父终态
被活跃 child 阻止；未签发 authority 的 preflight failure 可审计地转为 `ABANDONED`。

主要证据：`test_reservation_is_deterministic_and_concurrent_safe`、
`test_parent_cannot_finish_with_active_child`、`test_large_valid_turn_is_not_narrowed_by_store_schema`、
`test_preflight_failure_can_abandon_unstarted_child`、
`test_receipt_substitution_and_store_tampering_fail_closed`。

## Round 2：authority、policy 与 receipt 替换

攻击包括原始 bearer 落盘、策略拒绝后仍 dispatch、并发重复执行、control-plane 路径挂载、
ephemeral ArtifactStore 冒充可恢复，以及持有有效 claim 后绕过 executor 直接替换 result 的
producer、媒体类型、分类、元数据、加密声明或内容。

结论：Store/Event/repr 只保存 token digest；deny/require-approval 在 sandbox 前 terminal；同一
invocation 只有一个 RUNNING authority；sandbox scope 与 Store/Artifact root 必须互斥；没有
durability 声明的 Store 构造失败；success result 的 canonical 结构校验同时存在于 executor
和 Store terminal commit 边界，且必须是同 child 产生并可重读的 Artifact。

主要证据：`test_raw_claim_token_is_never_persisted_or_rendered`、
`test_policy_denial_never_dispatches_backend`、`test_concurrent_duplicate_executes_backend_once`、
`test_control_plane_overlap_is_rejected_at_construction`、
`test_ephemeral_result_store_cannot_claim_recovery_readiness`、
`test_invalid_success_artifact_is_fail_closed`、
`test_store_rejects_noncanonical_success_artifact_shape`。

## Round 3：崩溃窗口、fencing 与真实 Loop

攻击覆盖 authorization commit 后崩溃、backend 返回后崩溃、lease 边界 completion/expiry
竞态、父 reaper 与 child lease 的嵌套竞态、未知非幂等副作用错误继续，以及真实 Agent Loop
是否收到未持久 receipt。

结论：两个崩溃窗口都不会自动重放副作用；lease 到期后只产生一个
`agent_tool.outcome_unknown`，迟到 completion 被拒；非幂等成功但结果证据损坏也进入 unknown；
父 reaper 在同一事务内先清理 child，并让 child unknown 强制提升父恢复语义；真实 Loop 仅收到
Store 重读后的 receipt，Event 顺序为 scheduled/started/terminal，projection replay 与 live
state 一致。

主要证据：`test_restart_after_authorization_expires_to_unknown`、
`test_crash_after_backend_return_does_not_repeat_side_effect`、
`test_expiry_wins_race_at_exact_lease_boundary`、
`test_parent_lease_recovery_abandons_scheduled_child_atomically`、
`test_parent_recovery_resolves_only_expired_running_child`、
`test_uncertain_write_with_invalid_result_stops_recovery`、
`test_real_agent_loop_uses_durable_dynamic_child_authority`。

## 残余风险

- 动态 child 的 `REQUIRE_APPROVAL` 暂以安全失败收敛，尚无 WAITING_APPROVAL/resume；
- reference executor 是同进程 composition，远程认证、attestation 与外部幂等由部署负责；
- child terminal 与父 checkpoint/terminal 仍是顺序事务；安全轮次 checkpoint 会重验完整
  receipt prefix，稳定 operation key 和 append-only child ledger 用于跨事务恢复，但这不等价于
  外部副作用 exactly-once；
- 独立全局 child scanner 尚未实现；父 lease recovery 已能事务化清理 orphan，父仍活跃时依赖
  同请求重放或显式 preflight abandonment；
- reference LocalArtifactStore 不提供 secret encryption。
