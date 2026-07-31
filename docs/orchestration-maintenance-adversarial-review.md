# Durable Maintenance 三轮对抗性审查

审查对象：

- `DurableMaintenanceSupervisor`
- `DurableDeadlineScanner` / `DurableLeaseReaper`
- due-retry、control-intent、hierarchy-control Store 查询
- `DurableFleetReconciler.quarantine()`
- `RemoteControlPlane` maintenance admission gate

审查目标不是证明 exactly-once，而是验证：进程重启或周期维护异常时，新 remote claim
不会在未恢复的 durable 状态上继续产生；已有 lease/fencing authority 仍能保守收敛；
报告不泄露 payload、路径、异常文本或 credential。

## Round 1：恢复完整性与 fail-closed

### 攻击假设 1：部署创建了 supervisor，但 Worker admission 没有消费 readiness

初版只暴露 `maintenance.production_security_ready`。这仍允许部署漏接：维护循环已经
失败或过期，`RemoteControlPlane.poll_fleet()` 却只检查 Fleet poller readiness。

修正：

- 新增一次性 `bind_maintenance_gate()`；
- 未绑定 gate 的 Fleet 永远不报告 production-ready；
- `poll_fleet` 和 `claim_for_fleet` 都复验 maintenance readiness；
- gate 失效不阻塞 heartbeat、completion、cancel acknowledgement。

证据：

- `test_stale_maintenance_gate_blocks_new_fleet_claims`
- `test_fleet_requires_an_explicit_maintenance_gate`

### 攻击假设 2：大量 Run deadline 到期，但 saturation 检查只看到 Activity

`DeadlineReport.scanned` 原先只统计 Activity deadline。Run deadline 查询同样有
`limit`，却没有暴露扫描数；超过上限时 supervisor 可能错误开放 Fleet。

修正：

- `DeadlineReport.run_deadlines_scanned` 显式记录 Run deadline 扫描数；
- Activity 或 Run 任一计数达到 `scan_limit` 都保持 quarantine；
- report 仍只暴露计数。

证据：

- `test_run_deadline_saturation_also_quarantines_fleet`

### 攻击假设 3：future retry 在首次 recovery 后永久休眠

lease recovery 可以提交未来的 `retry_due_at`。到期后它已不是 expired lease，
Fleet reconciler 又明确不推进 Domain Run，因此只消费 recovery report 会漏掉它。

修正：

- Store 有界查询到期或损坏的 `WAITING_RETRY`；
- steady cycle 把这些 Run 纳入 Domain reconcile；
- 查询触顶同样 fail closed；
- Scheduler 对损坏的 persisted retry deadline 显式报错，不再静默 continue。

证据：

- `test_steady_cycle_wakes_due_retry_without_another_lease`
- `test_malformed_retry_projection_fails_closed_without_leaking`

Round 1 结论：通过。发现的三条绕过/静默停滞路径均已加入回归。

## Round 2：并发、TOCTOU 与线性化

### 攻击假设 1：gate 在 poll 时健康，在 policy ticket 准备后、Store claim 前失效

仅在 `_claim_assignment` 入口复验仍有 TOCTOU。preflight、Artifact grant preparation
或外部 policy adapter 可以耗时，维护状态可能在真正 mutation 前撤销。

修正：

- maintenance gate 提供 admission linearization guard 与 guard 内 validator；
- Remote control 将它与 Worker session current guard 组合；
- Scheduler 在进入 `claim_activity_with_policy` 前、同一 guard 内同时检查 session 与
  maintenance；
- maintenance failure/success 状态更新使用同一状态锁，因此撤销与 claim 有明确先后；
- validator 异常统一按不健康处理，不暴露异常文本。

证据：

- `test_maintenance_revocation_wins_before_claim_linearization`
- `test_overlapping_cycles_are_rejected_without_corrupting_state`

### 攻击假设 2：reconcile 出错后 quarantine 自身也失败

只记录第一阶段异常会让运维误以为 queue 已经撤空。

修正：

- 报告分别记录固定 `stage/code`；
- quarantine 失败额外产生 `quarantine_failed`；
- 原异常和 quarantine 异常文本都不会进入报告；
- queued quarantine 不伪造 active assignment 已撤销。

证据：

- `test_failure_report_redacts_exception_and_quarantines`
- `test_quarantine_failure_is_bounded_and_visible`
- `test_fleet_failure_runs_its_explicit_quarantine`

Round 2 结论：通过。new claim 的进程内撤销顺序已经线性化；active authority 仍按
Store lease/fencing 收敛。

## Round 3：重启、时钟、分页与层级进度

### 攻击假设 1：Supervisor 与 Scheduler 时钟不一致

Supervisor 可在 wall clock 上判断 retry 已到期，但注入 Scheduler 仍使用更早时钟；
调用 `reconcile()` 后节点不变，初版仍可能报告 healthy。

修正：

- Domain reconcile 后重新查询当前时刻的 due retry；
- 任一残留产生 `due_retry_not_converged` 并 quarantine；
- 不试图自动校正或覆盖 Scheduler 时钟。

证据：

- `test_scheduler_clock_skew_cannot_report_due_retry_healthy`

### 攻击假设 2：bootstrap 的 OFFSET 分页在并发 terminal 更新时跳过 Run

前一页记录被另一进程移出 active 集合后，后一页会左移；继续使用旧 offset 会漏掉一条
重启前 Run。

修正：

- bootstrap 使用单条有界 `list_nonterminal_runs()` 查询取得 SQLite statement
  snapshot；
- 超过 `max_active_runs` 时 fail closed，不截断后继续；
- 新 supervisor 实例不继承旧健康状态，必须重新 bootstrap。

证据：

- `test_bootstrap_recovers_all_active_runs_before_readiness`
- `test_active_run_capacity_fails_closed_before_resolution`

### 攻击假设 3：其他 writer 在 bootstrap 后崩溃，留下 CREATED/PAUSING/CANCELLING

只恢复启动时快照无法处理本进程存活期间出现的孤立控制意图。

修正：

- steady cycle 有界扫描 create/pause/cancel control intents；
- 达到上限保持 quarantine；
- parent-before-child 排序避免 hierarchy intent 逆序推进。

证据：

- `test_steady_cycle_recovers_late_created_and_cancel_intents`
- `test_parent_runs_are_ordered_before_children`

### 攻击假设 4：remote child terminal 后无人推进 parent hierarchy control

安全 remote poll 只读生成 candidate，Fleet projector 不推进 Domain；child completion
只 reconcile child。原测试需要手工反复 reconcile root，生产长进程会悬挂父 Run。

修正：

- steady cycle 有界查询 active `map/subworkflow` control；
- 周期 reconcile 观察 child durable terminal 状态并收敛 parent；
- hierarchy 扫描触顶同样关闭新 Fleet admission。

证据：

- `test_maintenance_converges_parent_after_remote_child_completion`

Round 3 结论：通过。启动快照、steady 控制意图、retry timer 和 hierarchy observer
均有独立有界来源，且最终状态从 Store 重读验证。

## 保留边界与上线要求

本轮没有、也不应声称解决：

- 跨 Store queue/quota/cursor 共识；
- 自动判断旧 Fleet owner 已死亡或自动 transfer；
- 运行中 active assignment 的强制伪撤销；
- 任意外部副作用 exactly-once；
- 旧二进制自动服从新进程内 maintenance gate。

滚动升级必须先停止旧实例的新 admission 并 drain；严格多控制面部署使用唯一进程世代
owner 与 Store fencing epoch。维护 gate 是当前进程的新 claim 门禁，不是跨版本数据库
trigger。真实部署还必须把 control Store、journal 和 Artifact 根放在 Agent
workspace 外，并让服务编排器以小于 `max_staleness_seconds` 的周期串行调用
`run_once()`。
