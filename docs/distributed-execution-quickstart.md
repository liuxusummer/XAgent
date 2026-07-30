# Secure Distributed Execution Quickstart

本 quickstart 展示 Phase 2 的可执行 reference composition：真实使用 Durable Store、
Scheduler、policy、Artifact broker、ToolReceipt、remote protocol client/daemon 和
logical replay，同时把尚未部署的网络与隔离能力明确留在边界外。

## 运行旗舰案例

```bash
.venv/bin/python examples/distributed_execution_demo.py
```

也可提供自己的临时控制目录和兼容的 Workflow JSON：

```bash
.venv/bin/python examples/distributed_execution_demo.py \
  --runtime-dir /tmp/xagent-distributed-reference \
  --workflow examples/workflows/distributed_execution_demo.json
```

案例会执行两个 Run：

1. 三个逻辑 Worker session 领取同一 Workflow 的三个独立 Tool Node。barrier 证明至少
   两个执行区间真实重叠；三个 Worker 都必须产生一个 Artifact 和一个持久
   `ToolReceipt`。
2. 三个结果提交后，Workflow 进入持久 `WAITING_APPROVAL`。reference approval ledger
   签发与 Run、Node、definition digest 绑定的决议，Run 才能完成。
3. 第二个 Run 由第三个 Worker 越过 `attempt.started` 后接收持久取消，提交绑定的
   cancellation proof，并收敛到 `CANCELLED`。
4. 两个 Run 都执行 logical replay，并将 replay 与 live projection 逐字段比较。replay
   不调用 Worker 或外部 Activity。

JSON 摘要只输出有界状态、计数和 digest，不输出 Store 路径、broker grant、签名或
Artifact 内容。`security_boundary` 字段会明确报告 mTLS、SPIFFE、gVisor 和 Kubernetes
没有由该示例部署。

## 有限 soak

```bash
.venv/bin/python examples/distributed_execution_demo.py --soak-runs 10
```

报告将逻辑工作量与 wall time 分开：

```text
logical_runs
remote_attempts
worker_sessions
completed_runs
wall_time_seconds
claim=reference_workload_not_production_slo
```

该数字只用于发现 reference implementation 的死锁、泄漏和非有界增长，不能作为生产
吞吐、延迟或可用性 SLO。

## 公共组合入口

`src.orchestration` 顶层提供显式且 inert 的 Phase 2 组合类型，包括：

```python
from src.orchestration import (
    ArtifactGrantBroker,
    AuthenticatedWorker,
    BoundedRemoteObservability,
    DeterministicRemoteScheduler,
    OciGvisorSandboxBackend,
    RemoteControlJournal,
    RemoteControlPlane,
    RemoteFleetCoordinator,
    RemoteWorkerClient,
    RemoteWorkerDaemon,
    SecureRemoteAssignmentAdmitter,
    SecureRemoteExecutionAdapter,
    WorkerAuthorizationGate,
)
```

导入不会启动线程、server、Worker 或网络，也不会创建 Store。调用方必须显式注入：

- 已认证 transport identity；
- workload attestor 与细粒度 allow rules；
- Artifact broker 传输；
- runtime attestation verifier 与 proof signer；
- Store/scheduler resolver；
- 位于独立 control-plane isolation root 的持久 `RemoteControlJournal`；
- 生产部署所需的 server、pull、mTLS 和隔离适配器。

## 生产边界

| 能力 | reference implementation 已证明 | 尚未证明/部署 |
|---|---|---|
| 协议 | 严格、有界、版本化 wire model；持久 request digest identity 与有界 response LRU 分离 | HTTP/gRPC/queue server、pull loop、mTLS、SPIFFE |
| Session | 持久单调 epoch；当前 identity + instance 可跨重启恢复；被替换 instance 的 tombstone 防 A→B→A；启动时校验完整 schema/FK/integrity，首次建库原子发布 | journal 备份、跨服务认证会话续期与证书轮换运维 |
| Artifact | path-free grant、单次读取、受绑定的 staging/finalize、完整性复验、跨 Attempt 常驻字节硬上限、过期不可逆 | 远程对象服务、传输加密、持久 staging registry |
| Sandbox | 限制性 OCI spec、精确 attestation/proof binding、缺证据 fail closed | 本机真实 gVisor/Kubernetes 部署与部署 attestation |
| 调度 | 有界、确定性 tenant round-robin、quota、drain、session/generation 防 ABA；Activity runtime 范围在 claim 前强制 | fleet server 与 Run-scoped control poll 的端到端生产集成 |
| 观测 | 低基数、best-effort、明确 `execution_truth=false` | 生产 exporter、告警与容量规划 |

还必须保留以下残余边界：

- secure path 必须显式注入受保护的持久 `RemoteControlJournal`。其 SQLite 不属于
  Domain Store，不保存 response/grant/proof，路径也不得发送或挂载给 Worker；
  `:memory:` journal 只允许 development reference，assignment/poll/complete 会
  `security_not_ready` fail closed。durable journal 只在同目录临时库完整提交、校验
  和 fsync 后原子发布；既有库缺表、缺 metadata、FK orphan 或 integrity 失败都拒绝
  启动，绝不自动补空表。active session 的 request identity 和退役 instance
  tombstone 不做不安全 TTL 淘汰，容量满时拒绝新请求/instance，需要运维在证明无活动
  authority 后迁移或轮换 control journal。
- `SecureRemoteAssignmentAdmitter` 的 prepared registry 与 reference broker 的
  staged/finalized registry 当前在进程内。控制面崩溃后不能从 untrusted completion
  重建 authority；已写但未被 Domain Event 引用的不可变 Artifact 只能由 GC 保守回收。
- `OciGvisorSandboxBackend` 只在注入 verifier 对当前 adapter、runtime class 和有效期
  返回精确 attestation 时暴露 `SecurityLevel.CONTAINER`。示例中的 HMAC proof 只证明
  绑定代码，不证明进程真的在 gVisor 中运行。
- `RemoteControlPlane` 仍是 run-scoped reference poll；它与
  `RemoteFleetCoordinator` / `DeterministicRemoteScheduler` 尚未组成真实
  server/pull 数据面。fleet 层证明多工具 Worker 的逐节点匹配，但 control poll 当前只
  对整个 Workflow 的 activity kinds/capabilities 做粗预检。因此不能声称已经端到端
  支持生产异构 fleet。
- `RemoteFleetCoordinator` 的 routing 是 projection，不是执行事实。durable claim
  callback 失败后会回滚 routing 并移除绑定；Store reconciler/投影器必须重新投影并
  re-admit ready work。
- 外部副作用仍是 at-least-once Attempt；reference protocol 的幂等 response 不等于
  任意工具 exactly-once。

## 验证

```bash
.venv/bin/python -m unittest tests.test_orchestration_distributed_demo
.venv/bin/python -m unittest tests.test_orchestration_distributed_fault_matrix
.venv/bin/python -m unittest tests.test_orchestration_public_api
```

R01–R20 的逐项证据见
[distributed-execution-fault-matrix.md](distributed-execution-fault-matrix.md)，安全与
正确性契约见 [distributed-execution-adr.md](distributed-execution-adr.md)。
