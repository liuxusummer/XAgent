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
    DurableFleetReconciler,
    DurableFleetProjector,
    DurableStoreFleetRunSource,
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
scheduler.store.claim_fleet_shard(
    "analysis-shard",
    fleet_owner_id,
    ownership_policy_digest,
    pool_id="analysis",
)
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
poller = SecureRemoteFleetPoller(
    fleet,
    worker_policies,
    claimer,
)
production_remote_control.bind_fleet_poller(poller)

projector = DurableFleetProjector(tool_policies)
run_route = scheduler.store.register_fleet_run_route(
    run_id,
    "tenant-a",
    "analysis",
)
run_source = DurableStoreFleetRunSource(
    [scheduler.store],
    control_plane_root="/var/lib/xagent/control",
    agent_roots=("/srv/xagent/workspaces",),
)
fleet_reconciler = DurableFleetReconciler(
    fleet,
    poller,
    projector,
    run_source,
    durable_schedulers.__getitem__,
)
fleet_reconciler.run_once()
```

`production_remote_control` 仍须满足 HTTPS 文档中的 durable journal、production-ready
two-phase admission 和 reference fallback 禁用要求。`DurableFleetReconciler` 也不会
启动线程、扫描任意 Store 或推进 Domain Run；部署显式周期调用 `run_once()`，并通过
`FleetRunSource` 与 scheduler resolver 注入已经授权的 Run/tenant/pool 路由。示例的
`DurableStoreFleetRunSource` 动态读取当前 Store schema v8 的持久路由注册表。
`StaticFleetRunSource` 只用于开发和确定性测试，必须显式设置
`allow_reference_source=True`，且永远不会让 reconciler 报告 production-ready。
独立 durable reconciler 仍负责先把 Run/Node 推进到 `RUNNING/READY`。

生产 source 必须显式接收控制面根目录和全部 Agent 可写根目录。每个 Store 文件必须
严格位于控制面根内，控制面根不得与任一 Agent 根互为祖先；source 会固定目录和数据库
的 device/inode，并在每次 snapshot 前后复验。路径被替换、Store 越界、同一物理 Store
经别名重复注册、同一 Run 出现在多个 Store，或 resolver 把 Run 指向另一个克隆 Store
时均 fail closed 并 quarantine 旧 queued binding。这是部署配置的可验证防线，不替代
独立 service/OS identity、ACL 和“控制面目录不挂载给 Agent”的要求。

路由注册是显式控制面操作，不从 Run input、Worker 声明或 Fleet snapshot 猜 tenant。
一个 Run 的 tenant/pool 创建后不可变；撤销与重新启用都使用当前
`FleetRunRouteRecord` 做 CAS，每次状态变化增加 generation 并产生新 digest，防止
A→disabled→A 复活旧队列。注册表最多保留 100,000 条记录且不自动删除 tombstone；
容量规划和 Run 生命周期由运维负责。

每次 `run_once()` 先完整解析有界 route snapshot、scheduler、pool authority 和 READY
投影，再进入 Coordinator 的单锁 queue linearization。陈旧 queued binding 先
withdraw，随后才 admit 新 binding；因此 Tool routing policy 变更不会把旧排队策略
留到下一次 Worker poll。已经 active 的 binding 代表 durable authority 已产生，只
报告 `deferred_active_tasks` 并等待 terminal/lease recovery，不能由队列控制器伪装
撤销。capacity 不足只拒绝新投影，绝不驱逐 active binding。report 始终带
`execution_truth=false`。

route source 动态失去 production-ready、snapshot/resolver/Store/projector 出错或
strict authority 不一致时，控制轮次不会保留旧 queue 继续赌可用性：它先以空 desired
执行同一个 queue reconciliation，撤回全部 queued binding，再向运维抛出原错误。
active assignment 不受内存 quarantine 伪造撤销，仍由 terminal/lease recovery
收敛。修复控制源后下一轮可幂等重建 queue。

`claim_fleet_shard()` 对同一 owner、pool 和 policy 是幂等恢复，不是租约续期。每个
`pool_id` 在一个 Store 中只能对应一个 shard；该 owner 可以调度池内多个 tenant，
不能把同一公平队列拆给多个控制器。计划接管时，运维控制器
必须读取当前值并调用 `transfer_fleet_shard(current, ...)`；成功 CAS 会增加
`fencing_epoch`。旧控制器的已排队 binding 随后在 Store claim 事务中被拒绝，哪怕
owner 名称后来 A→B→A 回到原值，旧 epoch 也不会重新有效。epoch 不依赖 wall clock；
`assigned_at` 仅用于审计。自动故障检测和何时接管由部署层决定，模块不会用超时猜测
leader 已死亡。

`DurableFleetReconciler` 可幂等恢复同一 authority，也能在 pool 已完全 idle 时安装
transfer 后的新 cursor；它不会替部署猜测旧 leader 是否死亡。planned transfer
必须先让旧 route snapshot 撤回该 pool 的 queued binding，并等待 active assignment
terminal。只要本地 queue/active 尚存，cursor authority 切换就以
`Fleet pool must be idle before cursor restore` fail closed。

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

schema v7 新增 Store-local `fleet_run_routes`。注册某个 Run 后，该 Run 的 Fleet
admission 必须使用 schema v2，并精确携带当前 enabled route digest。route 校验在
claim 的 `BEGIN IMMEDIATE` 事务内先于容量、资源、ownership、quota 和公平游标推进；
撤销赢得事务时不会产生 Attempt，claim 先赢时后续 `CLAIMED -> RUNNING` 仍由数据库
trigger 再次检查。已经 `RUNNING` 的合法 Activity 可凭原 lease/fencing 完成。
迁移同时安装 INSERT/UPDATE trigger，因此迁移前已打开数据库的旧进程也不能在已注册
Run 上继续写入 schema v1 或陈旧 route；滚动启用路由前应先 drain 旧版 active claim。

当前 schema v8 在此基础上增加 remote child hierarchy admission fencing。Fleet route
可以直接指向 child Run，也可以由 run-scoped poll 从祖先递归命中 child；两种情况都
必须使用能恢复实际 child Workflow 的 scheduler resolver、授权完整 root-to-child
Run 链，并在 Attempt 中持久化 `hierarchy_admission`。v7→v8 会逐条验证 active
`remote-session:` child authority；任一 unscoped、畸形或已失效记录都会拒绝迁移并
要求先 drain。

## 路由信封

`DurableFleetProjector` 只投影 Tool Activity，并为每项任务绑定：

- tenant、pool 和确定性 task id；
- 精确 `run_id`、`node_id` 和 Activity config digest；
- Tool 名称与服务端工具策略 digest；
- 当前 Store-local Run route generation digest；
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
policy digest、本次生效的 global/tenant/pool 上限，以及当前 Run route digest。
控制面再次检查：

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
控制循环调用 `fleet_reconciler.run_once()`；其内部先执行
`poller.reconcile_terminals()`，逐项读取 exact durable Attempt，只有确认 terminal
才释放容量。

## 重启与 reconcile

进程重启时：

1. 从可信 Run registry 解析各 Run 的 `DurableScheduler`；
2. 用当前 Store 恢复 `DurableStoreFleetRunSource`，复验控制面/Agent 根隔离和物理
   Store 身份；
3. 先执行独立 durable reconciler；
4. 调用 `DurableFleetReconciler.run_once()`；它先读取本
   进程负责 pool 的 current ownership/cursor，验证 shard/owner/epoch/policy，
   恢复 cursor，再差量收敛 READY binding；
5. Worker 重新通过 mTLS register，session journal 恢复或 fencing 旧 instance；
6. lease reaper 处理崩溃前的 durable claims；
7. 新进程的 Fleet active projection 从空状态开始，不从 Worker 响应推断旧 authority；
   新 claim 的配额检查直接读取 Store 中旧 active Attempt 的 durable scope。
   exact claim 的控制权限可由 `RemoteExecutionJournal` + Store + 新鲜 attestation
   重建；projection 随后由 completion 或周期 `run_once()` 收敛。

不要从 Fleet snapshot 推断 Attempt 状态，也不要把 `execution_truth=false` 的报告写回
Domain Store。Artifact grant/finalization 已通过 token-digest-only journal 跨进程
恢复。Fleet quota admission 在共享同一 Store 的控制进程间已经原子化，但 queue、
Worker registry 和 active routing projection 仍非共享共识队列。严格模式通过每个
pool 的显式单写 shard owner 防止两个可信控制器同时消费同一公平队列；持久 cursor
保证同 Store、显式接管或重启后的 tenant round-robin 不从默认位置重新开始，但不等价
于共享 broker：跨 Store quota/ownership/cursor、自动 leader 故障检测和在线无损
queue handoff 仍是部署/后续实现边界。所有权表有 4096 个 pool 的硬上限且不自动
删除；Run route 表有 100,000 条历史记录的硬上限且同样不做 LRU 淘汰。pool/route
生命周期和容量规划必须由运维控制，禁止通过删除记录重置 epoch/generation。真实
TLS-extension server 和生产 Sandbox 也仍需独立验证。

## 验证

```bash
.venv/bin/python -m unittest -q \
  tests.test_orchestration_remote_fleet_control \
  tests.test_orchestration_remote_fleet \
  tests.test_orchestration_remote_fleet_reconcile \
  tests.test_orchestration_remote_scheduling \
  tests.test_orchestration_remote_protocol \
  tests.test_orchestration_remote_execution \
  tests.test_orchestration_remote_journal \
  tests.test_orchestration_scheduler
```

对抗审查与残余边界见
[remote-fleet-adversarial-review.md](remote-fleet-adversarial-review.md)。
