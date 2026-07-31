# Remote Fleet 数据面

本模块把 HTTPS Remote Worker 的 server/pull 请求接到跨 Run 的确定性调度器，
同时保留 `DurableRunStore`、lease、fencing、Policy 和 Artifact broker 作为唯一
执行事实。Fleet 的队列、Worker 和 active assignment 都只是有界、可重建
projection。执行中的 Fleet tenant/pool/global 配额占用则随 Attempt 持久化，并在
同一个 Store claim 事务中线性化。多控制面部署还可启用 Store 本地的 routing-scope
单写者所有权；每个 pool 只有一个 owner，每次 claim 在同一事务中校验 owner、
单调 fencing epoch，并推进该 pool 的持久公平游标。

## 数据流与事实边界

```text
authenticated Worker
  -> POST /v1/remote-worker: poll_fleet {}
  -> RemoteControlPlane
  -> SecureRemoteFleetPoller
       -> StaticFleetWorkerResolver
       -> RemoteFleetCoordinator
       -> DeterministicRemoteScheduler
  -> RemoteControlFleetClaimer
  -> exact DurableScheduler candidate
  -> two-phase policy/admission
  -> DurableRunStore claim
  -> path-free WorkAssignment
```

Worker 无权选择 `run_id`、节点或 lease 时长。`poll_fleet` 的 body 必须是空对象；
服务端根据经过认证的当前 session、不可变 Worker policy 和 Fleet 公平队列选择
候选。真正的 schedule/claim/policy Event 仍由 Run 对应的
`DurableScheduler` 原子提交。

## 生产组合

```python
import hashlib

from src.orchestration import (
    DeterministicRemoteScheduler,
    DurableFleetProjector,
    FleetToolRoutingPolicy,
    FleetWorkerPolicy,
    RemoteControlFleetClaimer,
    RemoteFleetCoordinator,
    SecureRemoteFleetPoller,
    StaticFleetToolPolicyResolver,
    StaticFleetWorkerResolver,
)

fleet_owner_id = "control-a"
ownership_policy_digest = hashlib.sha256(
    b"fleet-ownership-policy:v1"
).hexdigest()
shard_ownership = scheduler.store.claim_fleet_shard(
    "analysis-shard",
    fleet_owner_id,
    ownership_policy_digest,
    pool_id="analysis",
)
fairness_cursor = scheduler.store.get_fleet_fairness_cursor("analysis")
if fairness_cursor is None:
    raise RuntimeError("owned Fleet pool has no durable fairness cursor")

tool_policies = StaticFleetToolPolicyResolver(
    [
        FleetToolRoutingPolicy(
            tool_name="repo.inspect",
            required_capabilities=frozenset(
                {"activity.tool", "artifact.refs", "workspace.read"}
            ),
            policy_version="policy-2026-07",
        )
    ]
)
worker_policies = StaticFleetWorkerResolver(
    [
        FleetWorkerPolicy(
            worker_id="worker-a",
            tenant_id="tenant-a",
            pool_id="analysis",
            tools=frozenset({"repo.inspect"}),
            allowed_capabilities=frozenset(
                {"activity.tool", "artifact.refs", "workspace.read"}
            ),
            allowed_resource_keys=frozenset({"workspace:/project"}),
            max_concurrency=4,
        )
    ]
)

claimer = RemoteControlFleetClaimer(
    production_remote_control,
    lease_seconds=60,
    fleet_owner_id=fleet_owner_id,
)
fleet = RemoteFleetCoordinator(
    lambda: DeterministicRemoteScheduler(),
    claimer,
    require_durable_ownership=True,
    fleet_owner_id=fleet_owner_id,
)
fleet.restore_fairness_cursor(fairness_cursor)
poller = SecureRemoteFleetPoller(
    fleet,
    worker_policies,
    claimer,
)
production_remote_control.bind_fleet_poller(poller)

projector = DurableFleetProjector(tool_policies)
for binding in projector.project_ready(
    scheduler,
    run_id,
    tenant_id="tenant-a",
    pool_id="analysis",
    shard_ownership=shard_ownership,
):
    fleet.admit(binding)
```

`production_remote_control` 仍须满足 HTTPS 文档中的 durable journal、production-ready
two-phase admission 和 reference fallback 禁用要求。绑定 poller 不会启动线程、
扫描 Store 或隐式 reconcile；部署负责在可信控制循环中投影已经由
reconciler 推进到 `READY` 的 Run。

`claim_fleet_shard()` 对同一 owner、pool 和 policy 是幂等恢复，不是租约续期。每个
`pool_id` 在一个 Store 中只能对应一个 shard；该 owner 可以调度池内多个 tenant，
不能把同一公平队列拆给多个控制器。计划接管时，运维控制器
必须读取当前值并调用 `transfer_fleet_shard(current, ...)`；成功 CAS 会增加
`fencing_epoch`。旧控制器的已排队 binding 随后在 Store claim 事务中被拒绝，哪怕
owner 名称后来 A→B→A 回到原值，旧 epoch 也不会重新有效。epoch 不依赖 wall clock；
`assigned_at` 仅用于审计。自动故障检测和何时接管由部署层决定，模块不会用超时猜测
leader 已死亡。

生产中的 `fleet_owner_id` 必须代表一个控制进程世代（例如部署实例 ID），不能让两个
存活副本共用同一个服务名。相同 owner 的幂等恢复只适用于外部已经确认旧进程死亡的
重启；不确定时必须使用新的 owner ID 做 transfer。owner ID 不是 credential，不能
替代进程身份认证或 HA fencing。

接管前应先 drain。transfer 之后，旧 epoch 不仅不能领取新任务；尚未从
`CLAIMED` 进入 `RUNNING` 的旧 Attempt 也会被 schema trigger 拒绝并交给 lease
recovery。已经 `RUNNING` 的 Attempt 可提交 terminal 结果，因为执行权仍由它自己的
Activity lease/fencing 校验，ownership 不能替代或伪造该执行 lease。

创建某个 pool 的第一条 ownership 记录也是持久化的升级开关：此后该 pool 的任何
Fleet claim 若不带 ownership，Store 都以
`fleet_shard_ownership_required` 拒绝。因此尚未升级或误配为 non-strict 的控制器
不能绕过 fencing。启用前应先确认新控制器可读取 owner；已有无 scope active claim
仍须按下述滚动升级规则 drain。schema v6 同时在 `attempts` 的 active
INSERT/UPDATE 上安装数据库触发器；已在迁移前打开 Store 的旧进程即使不重新运行
版本检查，也不能写入缺失或陈旧的 ownership。

schema v5 的 ownership 是 `(tenant,pool)`。升级到 v6 时，同一 pool 的旧记录只有在
owner 与 policy digest 完全一致时才会合并；新 epoch 取旧最大值加一，从而 fencing
所有 v5 envelope。若一个 pool 已经出现不同 owner 或 policy，迁移会原子失败并保留
v5 数据，要求运维先消除 split ownership；epoch 已耗尽时同样 fail closed。

## 路由信封

`DurableFleetProjector` 只投影 Tool Activity，并为每项任务绑定：

- tenant、pool 和确定性 task id；
- 精确 `run_id`、`node_id` 和 Activity config digest；
- Tool 名称与服务端工具策略 digest；
- 完整 required capabilities；
- Workflow 声明的 resource keys；
- Activity runtime 最小/最大版本。

`FleetToolRoutingPolicy` 是服务端配置，不读取 Worker 声明。策略缺失、版本变化与既有
同 task id binding 冲突时 fail closed，要求调用方显式 rebuild，避免策略撤销时旧
队列继续执行。

`FleetWorkerPolicy` 同样由服务端持有。有效 Worker descriptor 使用：

```text
capabilities = registration ∩ policy.allowed_capabilities
resources    = registration ∩ policy.allowed_resource_keys
capacity     = min(registration, policy, global bound)
tools/tenant/pool = exact server policy
session      = current durable RemoteControlJournal binding
```

因此 Worker 不能靠 register body 扩大 capability、资源、tenant、pool、工具或容量
权限。缺少任何路由条件时任务保留在队列，不会“先领取再发现不兼容”。

## Claim 与终态

Fleet 预留 routing capacity 后，`RemoteControlFleetClaimer` 用精确 binding 和当前
session 调用 `RemoteControlPlane.claim_for_fleet()`。Coordinator 同时生成
`FleetAdmissionScope`，绑定 task/tenant/pool、Tool routing policy digest、完整 quota
policy digest 和本次生效的 global/tenant/pool 上限。控制面再次检查：

- run authorizer；
- 当前 registration/session，包含 Store 线性化点前后的 supersession 检查；
- 精确节点、config digest、resource keys 和 runtime compatibility；
- production-ready two-phase admission；
- Policy、Artifact、runtime attestation 和 durable candidate CAS；
- 当前 Store 内 active Fleet Attempt 的 global/tenant/pool 配额与 quota policy
  generation；
- strict multi-control 模式下，本控制面 `fleet_owner_id`、pool、ownership
  policy digest 与 Store 当前 fencing epoch。

scope 不含 token、参数、脚本、环境或 Artifact 内容。Store 在 `BEGIN IMMEDIATE`
事务中扫描 active Attempt 的规范化 scope；策略 generation 漂移、重复 active task、
畸形 scope 或任一容量已满都会在 schedule/claim/policy Event 一起提交前回滚。成功
claim 把 exact scope 写入 Attempt metadata，因此新控制进程无需恢复旧的 Fleet
active registry，也能继续计数；Attempt 终态后自然退出 active 集合。
严格模式还把无密的 exact shard ownership 写入 Attempt metadata，并在同一事务中把
`last_served_tenant` 与单调 `selection_sequence` 写入 pool owner row。配额拒绝、
claim CAS 冲突或后续事务失败会连同游标一起回滚；控制面重启必须先恢复该游标，再接纳
该 pool 的任务。字段使用
`fencing_epoch`，不是 bearer 或秘密；任何含 credential-shaped key 的 metadata
仍按原规则拒绝，不为所有权机制增加例外。

滚动升级必须先 drain 旧版远程 active claim。为防旧 Fleet Attempt 因缺少 scope 而从
计数中消失，只要 Store 里仍有 `remote-session:` owner 的无 scope active Attempt，
新 Fleet claim 就以 `fleet_unscoped_remote_active` 拒绝。run-scoped remote 与 Fleet
需要并行时，应使用已携带统一 admission metadata 的版本或隔离 Store；不能用可用性
换取静默超配。

返回的 `WorkAssignment` 必须与 routing 的 Run、节点、config、Tool、capabilities 和
resources 一致，否则不发送给 Worker；已经发生的 durable claim 由 lease recovery
收敛，不能用 projection 回滚伪装成零 mutation。

正常 completion、terminal replay 和 cancellation acknowledgement 在 durable terminal
提交后释放 Fleet projection。若释放过程崩溃或 observer 失败，terminal 响应不回滚；
控制循环调用 `poller.reconcile_terminals()`，逐项读取 exact durable Attempt，只有
确认 terminal 才释放容量。

## 重启与 reconcile

进程重启时：

1. 从可信 Run registry 解析各 Run 的 `DurableScheduler`；
2. 先执行独立 durable reconciler；
3. 读取本进程负责 pool 的当前 shard ownership 和 `FleetFairnessCursor`，确认二者的
   shard/owner/epoch/policy 完全一致且 owner 等于本进程配置；
4. 先调用 `fleet.restore_fairness_cursor(cursor)`，再对 `RUNNING` Run 调用
   `project_ready(..., shard_ownership=ownership)` 并逐项 `admit()`；或者在无 active
   Fleet assignment 时一次调用
   `fleet.rebuild(tasks, workers, fairness_cursors=cursors)`；
5. Worker 重新通过 mTLS register，session journal 恢复或 fencing 旧 instance；
6. lease reaper 处理崩溃前的 durable claims；
7. 新进程的 Fleet active projection 从空状态开始，不从 Worker 响应推断旧 authority；
   新 claim 的配额检查直接读取 Store 中旧 active Attempt 的 durable scope。
   exact claim 的控制权限可由 `RemoteExecutionJournal` + Store + 新鲜 attestation
   重建；projection 随后由 completion 或周期 `reconcile_terminals()` 收敛。

不要从 Fleet snapshot 推断 Attempt 状态，也不要把 `execution_truth=false` 的报告写回
Domain Store。Artifact grant/finalization 已通过 token-digest-only journal 跨进程
恢复。Fleet quota admission 在共享同一 Store 的控制进程间已经原子化，但 queue、
Worker registry 和 active routing projection 仍非共享共识队列。严格模式通过每个
pool 的显式单写 shard owner 防止两个可信控制器同时消费同一公平队列；持久 cursor
保证同 Store、显式接管或重启后的 tenant round-robin 不从默认位置重新开始，但不等价
于共享 broker：跨 Store quota/ownership/cursor、自动 leader 故障检测和在线无损
queue handoff 仍是部署/后续实现边界。所有权表有 4096 个 pool 的硬上限且不自动
删除；pool 生命周期和容量规划必须由运维控制，禁止通过删除记录重置 epoch。真实
TLS-extension server 和生产 Sandbox 也仍需独立验证。

## 验证

```bash
.venv/bin/python -m unittest -q \
  tests.test_orchestration_remote_fleet_control \
  tests.test_orchestration_remote_fleet \
  tests.test_orchestration_remote_scheduling \
  tests.test_orchestration_remote_protocol \
  tests.test_orchestration_remote_execution \
  tests.test_orchestration_remote_journal \
  tests.test_orchestration_scheduler
```

对抗审查与残余边界见
[remote-fleet-adversarial-review.md](remote-fleet-adversarial-review.md)。
