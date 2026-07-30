# Remote Execution Authority Recovery

> 状态：digest-only control-plane recovery 已实现
> 依赖：`distributed-execution-adr.md`、`durable-orchestration-spec.md`

## 1. 问题

远程 Worker 获得 assignment 后，控制面可能在 `start`、heartbeat、cancel ack 或
completion 之前重启。Domain Store 能恢复 claim/lease/fencing，但旧实现还依赖三个
进程内对象：

- `PreparedActivityExecution`；
- `WorkerAuthorizationGate` 的 issued registry；
- `ArtifactGrantBroker` 的 read/write grant 与 finalized handle registry。

仅凭 Worker 回传的 `ClaimBinding` 重建这些对象不安全，因为 Worker 是不受信输入；
把 grant token、脚本、环境变量或 Artifact 内容原样写入 SQLite 同样会扩大秘密暴露面。

## 2. 决策

控制面在返回 assignment 前，向独立的 `RemoteExecutionJournal` 原子写入一条不可变、
digest-only 的 authorization binding。记录精确绑定：

```text
run / node / attempt / fencing generation
worker / tenant / trusted identity digest
session binding digest
claim token digest
action / worker authorization / profile / request digest
grant binding / execution plan / runtime attestation digest
runtime verifier id / security level
created_at
```

记录明确不包含：

```text
claim token plaintext
Artifact bearer token
input/output content
script bytes
environment values
runtime proof or response body
raw execution plan / argv
```

SQLite 位于 Worker 不可访问的 control-plane isolation root。首次创建使用同目录临时库、
完整事务、`fsync` 和 no-clobber hard link 原子发布；数据库与 bootstrap lock 必须是
`0600` 普通文件，symlink、宽权限、未知/残缺 schema、integrity failure 均拒绝启动。

## 3. 恢复算法

同一受信 Worker instance 重新 register 后，`RemoteControlPlane` 先从 Domain Store
恢复 exact claim，再由 `SecureRemoteAssignmentAdmitter.restore_authorization()`：

1. 读取 exact `(run_id, attempt_id, fencing_token)` journal row；
2. 比较 Worker 回传的全部 authorization digest，但不信任其作为事实源；
3. 从可信 plan resolver、Workflow 与 immutable Artifact refs 重建 Action/Request；
4. 由 `TrustedActivityExecutor.restore_prepared_execution()` 验证 Store 中既有
   `activity_authorization` Event、claim-token digest、当前 policy digest、profile 和
   decision digest；
5. 对当前 transport presentation 重新做 workload attestation。Worker authorization
   digest 使用稳定 workload/session/action/rule lineage，不含 credential id 与时间；
6. 重新做 runtime attestation，并要求 digest、verifier id 和强隔离等级与 journal
   一致；
7. 重建 `ExecutionAuthorizationBinding`。该对象只有摘要和 execution plan，不能序列化
   成新 assignment，也不能签发/重放 Artifact bearer。

任一步不一致均 fail closed，Domain Store 不发生 completion mutation。

## 4. 当前可恢复范围

| 操作/结果 | 重启后状态 | 原因 |
|---|---|---|
| `start` | 可恢复 | exact Store claim + digest-only authority 足够 |
| heartbeat / cancel status | 可恢复 | lease mutation 仍由 Store token/fencing 验证 |
| failed / timed out / abandoned / outcome unknown | 可恢复 | 不携带 output handle |
| cancellation acknowledgement | 可恢复 | signed empty-output receipt 可重新验证 |
| terminal response replay | 可恢复 | 直接使用 Store 的 Receipt/Event evidence |
| successful completion with pre-restart output handle | fail closed | bearer→final `ArtifactRef` registry 尚未持久化 |
| input grant 在 broker 重启后首次兑换 | fail closed | one-time read grant registry 尚未持久化 |
| 重新发送 assignment | 禁止 | digest-only binding 不包含 bearer token |

因此本阶段提升了长任务控制与无输出终态的可用性，但没有谎称完整 output recovery。
下一阶段必须实现持久、token-digest-only 的 Artifact grant/finalization registry；在此
之前，旧 output handle 返回 `write_grant_unavailable`，Attempt 保持 RUNNING 并由
effect-class recovery 收敛。

## 5. 生命周期与容量

Journal row 不按 TTL/LRU 淘汰活跃 authority。达到硬上限时拒绝新 assignment，不删除旧
证据。正常终态提交后 best-effort 删除对应 generation；若进程在终态 commit 与清理之间
崩溃，运维控制循环调用 `reconcile_recovery_journal()`：

- exact claim token digest 与 fencing 仍 active：保留；
- Attempt terminal/缺失，或 generation 已被 durable fencing：删除；
- Store/journal 不可用：失败退出，不猜测。

`RemoteControlPlane.production_recovery_ready` 只有在 security path、持久 session journal、
持久 execution journal 和 recovery API 同时 ready 时才为 `true`。

## 6. 对抗性审查

### Round 1：文件系统与 schema

发现并关闭 bootstrap lock symlink、SQLite 宽权限和 partial schema 风险。数据库/lock
强制普通文件与 `0600`；既有未知表、缺表或损坏 schema 不自动修补。

### Round 2：容量与回收

发现如果只依赖进程内 `_drop_prepared()`，终态后崩溃会残留 row 并最终耗尽容量。加入
显式 Store-backed reconciliation；活跃 row 不做 LRU/TTL 淘汰。

### Round 3：bearer 复活与授权漂移

证明 digest-only recovery 不具有 `input_grants`/`output_grants`，不能生成 assignment。
伪造 grant digest、session、identity、action、plan、runtime attestation 或 policy
reconstruction 任一字段均拒绝。专项测试还证明：即使 Artifact bytes 已写入不可变 Store，
新 broker 也不会仅凭旧 output handle 猜测或重建 `ArtifactRef`。

## 7. 验证

```bash
.venv/bin/python -m unittest -q \
  tests.test_orchestration_remote_execution_journal \
  tests.test_orchestration_worker_security \
  tests.test_orchestration_remote_execution \
  tests.test_orchestration_remote_protocol
```

关键证据：

- journal 重启、并发 publish、generation 隔离、容量、权限/symlink/schema；
- 新 Gate 对同一 workload/session/action lineage 生成相同 authorization digest；
- 全新 Gate/Broker/Admitter 接受运行中失败终态；
- control-plane 端到端重启后接受 exact signed failure；
- Worker 篡改 journal-bound digest 被拒绝；
- pre-restart output bearer 在新 broker 上保持 fail closed；
- terminal crash residue 由 Store-backed reconciliation 清理。
