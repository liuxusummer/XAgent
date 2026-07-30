# Remote Fleet 数据面

本模块把 HTTPS Remote Worker 的 server/pull 请求接到跨 Run 的确定性调度器，
同时保留 `DurableRunStore`、lease、fencing、Policy 和 Artifact broker 作为唯一
执行事实。Fleet 的队列、Worker、容量和 active assignment 都只是有界、可重建
projection。

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
)
fleet = RemoteFleetCoordinator(
    lambda: DeterministicRemoteScheduler(),
    claimer,
)
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
):
    fleet.admit(binding)
```

`production_remote_control` 仍须满足 HTTPS 文档中的 durable journal、production-ready
two-phase admission 和 reference fallback 禁用要求。绑定 poller 不会启动线程、
扫描 Store 或隐式 reconcile；部署负责在可信控制循环中投影已经由
reconciler 推进到 `READY` 的 Run。

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
session 调用 `RemoteControlPlane.claim_for_fleet()`。控制面再次检查：

- run authorizer；
- 当前 registration/session，包含 Store 线性化点前后的 supersession 检查；
- 精确节点、config digest、resource keys 和 runtime compatibility；
- production-ready two-phase admission；
- Policy、Artifact、runtime attestation 和 durable candidate CAS。

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
3. 对 `RUNNING` Run 调用 `project_ready()`；
4. 在无 active Fleet assignment 时调用 `fleet.rebuild(tasks, workers)`，或逐项
   `admit()`；
5. Worker 重新通过 mTLS register，session journal 恢复或 fencing 旧 instance；
6. lease reaper 处理崩溃前的 durable claims；
7. 新进程的 Fleet active projection 从空状态开始，不从 Worker 响应推断旧 authority。
   exact claim 的控制权限可由 `RemoteExecutionJournal` + Store + 新鲜 attestation
   重建；projection 随后由 completion 或周期 `reconcile_terminals()` 收敛。

不要从 Fleet snapshot 推断 Attempt 状态，也不要把 `execution_truth=false` 的报告写回
Domain Store。Artifact grant/finalization 已通过 token-digest-only journal 跨进程
恢复，但 Fleet queue/active projection 仍非共享共识队列。跨进程共享 Fleet 队列、
真实 TLS-extension server 和生产 Sandbox 仍是部署/后续实现边界。

## 验证

```bash
.venv/bin/python -m unittest -q \
  tests.test_orchestration_remote_fleet_control \
  tests.test_orchestration_remote_fleet \
  tests.test_orchestration_remote_scheduling \
  tests.test_orchestration_remote_protocol \
  tests.test_orchestration_remote_execution \
  tests.test_orchestration_remote_journal
```

对抗审查与残余边界见
[remote-fleet-adversarial-review.md](remote-fleet-adversarial-review.md)。
