# Durable Orchestration 生产维护循环

`DurableMaintenanceSupervisor` 把重启恢复和周期维护组合成一个显式、可测试的控制轮次。
它不启动线程、不执行 Activity、不保存 Domain 事实，也不替部署判断 leader 是否失效。
`DurableRunStore`、Scheduler 的 Event/Projection、Activity lease 与 Fleet admission
仍是唯一执行 authority。

## 为什么需要统一轮次

单独的 `DurableDeadlineScanner`、`DurableLeaseReaper` 和
`DurableFleetReconciler` 都是正确且幂等的 `run_once()` 原语，但生产部署若漏掉其中
一个、顺序错误，或在恢复积压尚未清空时重新开放 Worker poll，会出现以下风险：

- 到期 Run 没有进入取消传播；
- 旧 lease 长期占用节点、配额或 Fleet active projection；
- 崩溃前的 `CANCELLING` / `PAUSING` Run 没有继续收敛；
- READY queue 在 durable recovery 失败时仍继续发放新 claim；
- 服务进程存活，但维护循环已经静默停止。

统一监督器只收敛这些已有 primitive 的调用契约，不引入第二套状态机。

## 启动与周期契约

部署必须先构造同一个 Store 上的 scanner、reaper、Scheduler resolver 和可选 Fleet
reconciler，然后执行：

```python
from src.orchestration import DurableMaintenanceSupervisor

maintenance = DurableMaintenanceSupervisor(
    durable_store,
    maintenance_scheduler_resolver,
    deadline_scanner=deadline_scanner,
    lease_reaper=lease_reaper,
    fleet_control=fleet_reconciler,
    scan_limit=200,
    max_active_runs=4096,
    max_staleness_seconds=30,
)

startup = maintenance.bootstrap()
if not startup.healthy:
    raise RuntimeError("durable maintenance bootstrap is not converged")

# 进程管理器或服务事件循环显式、串行地周期调用；库本身不 sleep、不起线程。
cycle = maintenance.run_once()
```

启用 Remote Fleet 时还必须在对外服务前调用
`remote_control.bind_maintenance_gate(maintenance)`。Fleet poll 和最终 durable claim
各自复验 gate；最终复验与 Worker session current check 共用 admission
linearization guard，令维护撤销和 Store claim 有明确先后顺序。未绑定 gate 的 Fleet
永远不报告 production-ready。维护失效只阻止新 claim；已经持有 lease/fencing
authority 的 heartbeat、completion、cancellation acknowledgement 仍可继续收敛。

`maintenance_scheduler_resolver` 是受信控制面依赖。它必须根据传入的 durable
`RunRecord` 恢复精确 Workflow 定义，并返回绑定同一物理 Store、相同
`workflow_id/version/definition_digest` 的 `DurableScheduler`。缺失、损坏或错误绑定
会 fail closed。

`bootstrap()` 每次创建新监督器后都必须成功一次，不能复用旧进程的“已健康”判断。
它用单条有界查询取得非终态 Run 的一致快照，再按父 Run 优先顺序恢复，避免
`OFFSET` 分页在并发 terminal 更新时跳过旧 Run。`run_once()` 消费 deadline、lease
报告中的受影响 Run，额外有界查询已经到期的 `WAITING_RETRY` 及
`CREATED/PAUSING/CANCELLING` 控制意图和 active `map/subworkflow` 控制节点，避免
每个 steady-state tick 全量推进所有 Workflow，也避免 future backoff、remote child
完成或其他控制进程崩溃后留下的意图无人唤醒。

每轮固定顺序为：

1. 读取受信 wall clock，并在 bootstrap 时扫描非终态 Run；
2. 扫描并提交到期 Run/Activity deadline；
3. 恢复过期 Activity lease；
4. 查询已到期或损坏的 retry projection；
5. steady cycle 查询新出现的 create/pause/cancel 控制意图；
6. 查询正在等待 child 变化的 active hierarchy control；
7. 解析精确 Scheduler，继续 cancel/pause、retry、hierarchy 和 READY 投影；
8. 复验当前时刻已无到期 retry 残留；
9. 最后才重建 Fleet terminal/queue projection。

deadline 必须先于 lease：否则已经超时的工作可能先被普通 lease policy 解释为可重试。
Domain reconcile 必须先于 Fleet：否则 Fleet 会投影尚未恢复的旧 READY 状态。

## Fail-closed 与 readiness

以下任一情况都会令本轮 `healthy=false`，并通过 Fleet reconciler 的显式
`quarantine()` 撤回全部 queued binding：

- Store 扫描、deadline、lease、Scheduler 解析或 reconcile 异常；
- Fleet source/readiness/reconcile 异常；
- 非终态 Run 数超过 `max_active_runs`；
- Activity deadline、Run deadline、lease 或 due-retry 的扫描数达到 `scan_limit`，
  说明仍可能存在未处理积压；
- steady control-intent 扫描数达到 `scan_limit`；
- active hierarchy-control 扫描数达到 `scan_limit`；
- Scheduler reconcile 后到期 retry 仍未收敛，例如部署注入了不一致的时钟；
- quarantine 自身失败。

达到扫描上限不是“成功处理了一批”后的绿色状态。监督器保留已提交的安全恢复结果，
但 Fleet 继续关闭；部署重复调用 `bootstrap()` 或 `run_once()`，直到某一轮扫描数严格
低于上限，才能重新变为 healthy。

`production_security_ready` 还要求：

- 当前监督器实例已经成功 bootstrap；
- 最近一轮成功且未超过 `max_staleness_seconds`；
- monotonic clock 没有回退；
- 可选 Fleet 控制器当前仍报告 production-ready。

因此进程 liveness 不能替代该 readiness。Worker poll/new-claim 门禁应读取这个组合
状态；超过 freshness 窗口时停止新 admission。它不是整个 transport 的 liveness：
不能因此从网络层阻断 heartbeat、completion 或 cancellation acknowledgement。

同一监督器拒绝重叠轮次。多个控制进程之间仍依赖 Store CAS、lease fencing 和 Fleet
pool ownership；进程内锁不是分布式锁，也不能提供跨 Store 共识。

## 报告与信息边界

`MaintenanceCycleReport` 只包含计数、布尔值和固定 stage/code：

- 不包含 Event、Run/Node/Attempt ID；
- 不包含异常文本、主机路径、payload、Artifact 内容或 credential；
- `execution_truth=false`，不能作为 Domain 恢复依据；
- `sequence` 和 freshness timestamp 都只是进程内诊断，重启后从头开始。

详细调查必须通过受权的 Store/operator diagnostics 查询完成，不能把异常字符串直接
暴露到不受信 Web 或 metrics label。

## 明确不提供

- 隐式后台线程或自动 sleep；
- 自动 Fleet owner 接管、故障检测或 wall-clock leader lease；
- 跨 Store quota、cursor 或 queue 共识；
- 运行中 active assignment 的伪撤销；
- 未知非幂等副作用的自动重试。

这些边界使监督器可由 systemd、Kubernetes controller、async service loop 或测试
harness 驱动，同时保持执行 authority 可审计、可重放且不依赖进程内“健康记忆”。
