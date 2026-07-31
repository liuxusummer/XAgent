# Remote Execution Authority Recovery

> 状态：digest-only execution authority、bearer-free Artifact grant recovery 与
> provider grant anti-replay 与 completed-result replay journal 已实现
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
digest-only 的 authorization binding。schema v2 为 Artifact broker 增加两种有界
记录，schema v3 为 provider gateway 增加单次调用墓碑：

```text
run / node / attempt / fencing generation
worker / tenant / trusted identity digest
session binding digest
claim token digest
action / worker authorization / profile / request digest
grant binding / execution plan / runtime attestation digest
runtime verifier id / security level
created_at

read grant:
grant id / token digest / canonical safe metadata / trusted ArtifactRef
issued | consumed / expires_at

write grant:
grant id / token digest / canonical safe metadata
issued | finalized | failed / staging digest / final ArtifactRef / expires_at

provider grant:
grant id / logical invocation digest / token digest / binding digest / route id
issued | consumed / expires_at / purge watermark

provider invocation（schema v4）:
grant id / actual request payload digest
invoking | completed | outcome_unknown
response digest / optional MODEL_RESPONSE ArtifactRef / updated_at
```

记录明确不包含：

```text
claim token plaintext
Artifact/provider gateway bearer token（只保存 SHA-256 digest）
input/output content
provider prompt/response/credential/endpoint
script bytes
environment values
runtime proof or response body
raw execution plan / argv
```

SQLite 位于 Worker 不可访问的 control-plane isolation root。首次创建使用同目录临时库、
完整事务、`fsync` 和 no-clobber hard link 原子发布；数据库与 bootstrap lock 必须是
`0600` 普通文件，symlink、宽权限、未知/残缺 schema、额外 trigger/view/index、
integrity failure 均拒绝启动。既有精确 v1/v2 schema 在一个 SQLite transaction 内
直接迁移到 v4；半迁移状态不会被自动补齐。

精确 v3 可在单事务内只增 invocation receipt 表并迁移到 v4。v3 consumed grant 没有
足够证据推断调用结果，在 v4 Broker 中只能恢复为 outcome unknown。Provider wire schema
同时升级到 v2；部署升级前应 drain 最长五分钟的活跃 grant，旧 wire grant 不会被新进程
猜测补全。

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
   成新 assignment，也不能签发 Artifact bearer。Worker 必须持有原 assignment 中的
   grant/handle；控制面只按 token digest 验证它。

任一步不一致均 fail closed，Domain Store 不发生 completion mutation。

Artifact 的独立线性化点为：

- read：authorization 校验后，SQLite `issued → consumed` CAS 先提交，之后才读取字节；
  响应丢失不会使 one-time grant 可重放；
- write：ArtifactStore 返回并通过完整 binding/integrity 校验后，SQLite 才将
  `issued → finalized`，保存 exact final ref；Domain completion 只接受该 ref；
- Store 已写、journal 未提交时崩溃只留下未引用的 immutable orphan，不产生伪造
  completion；journal 已提交、响应未返回时，重启可重放 exact final ref；
- Store/验证失败会持久化 `failed` tombstone，进程重启或时钟回拨均不能复活授权。

## 4. 当前可恢复范围与边界

| 操作/结果 | 重启后状态 | 原因 |
|---|---|---|
| `start` | 可恢复 | exact Store claim + digest-only authority 足够 |
| heartbeat / cancel status | 可恢复 | lease mutation 仍由 Store token/fencing 验证 |
| failed / timed out / abandoned / outcome unknown | 可恢复 | 不携带 output handle |
| cancellation acknowledgement | 可恢复 | signed empty-output receipt 可重新验证 |
| terminal response replay | 可恢复 | 直接使用 Store 的 Receipt/Event evidence |
| successful completion with pre-restart output handle | 可恢复 | Worker 持有原 handle；broker 只按 token digest 重放 exact finalized `ArtifactRef` |
| input grant 在 broker 重启后首次兑换 | 可恢复一次 | durable `issued → consumed` CAS 跨进程保持单次语义 |
| provider completed receipt + result Artifact | 可恢复 | exact bearer/binding/payload 重验后读取并校验同一 Artifact |
| provider `invoking` 或 v3 consumed 无 receipt | outcome unknown | 不重新调用 provider，不猜测费用或响应 |
| provider response Artifact 写后、receipt 前崩溃 | 不猜测结果 | 只产生可 GC orphan；不把它自动绑定为 completed |
| Store 写入后、finalization journal 前崩溃 | 不猜测结果 | 只产生可 GC orphan；原 grant 保持 issued，持有原内容的一方可在有效期内重试 |
| 只完成内存 staging、尚未写 Store 就崩溃 | staging 丢失 | journal 不保存 Artifact bytes；调用方必须重新 stage 原内容 |
| 重新发送 assignment | 禁止 | digest-only binding 不包含 bearer token |

`SecureRemoteAssignmentAdmitter.stage_output()` 在把 handle 返回给 Worker 前已完成 Store
写入与 durable finalization，因此“Worker 已拿到 output handle”不会落入仅内存 staging
窗口。该机制不把 bearer 复制回控制面，也不承诺任意外部 ArtifactStore 的 exactly-once
写入；跨进程重复写必须由 immutable/content-addressed Store 幂等吸收。

## 5. 生命周期与容量

Execution journal row 不按 TTL/LRU 淘汰活跃 authority。达到硬上限时拒绝新 assignment，
不删除旧证据。正常终态提交后 best-effort 删除对应 generation；若进程在终态 commit 与
清理之间崩溃，运维控制循环调用 `reconcile_recovery_journal()`：

- exact claim token digest 与 fencing 仍 active：保留；
- Attempt terminal/缺失，或 generation 已被 durable fencing：删除；
- Store/journal 不可用：失败退出，不猜测。

Artifact consumed/finalized/failed tombstone 在 grant 严格 expiry 前保留，防止时钟回拨
复活。读写共用硬记录上限，不做 LRU；容量满时拒绝签发。过期列有受 schema 校验的索引，
签发会清理过期记录，运维也可显式调用 `ArtifactGrantBroker.purge_expired_grants()`。

Provider grant purge 会先删除同 grant 的 invocation receipt，再删除 grant，并在同一
事务推进 purge watermark。未过期 invoking/completed/outcome-unknown evidence 不因容量
压力淘汰。

`RemoteControlPlane.production_recovery_ready` 只有在 security path、持久 session
journal、持久 execution journal、持久 Artifact broker registry 和 recovery API 同时
ready 时才为 `true`。

## 6. 对抗性审查

### Round 1：bearer secrecy 与 replay/CAS

逐文件扫描 main DB/WAL/SHM，证明 read/write bearer canary 从未落盘；并发的两个 journal
实例只能有一个成功执行 read CAS。consumed/failed tombstone 在 expiry 前不可重新签发，
wrong token 或 canonical metadata 漂移均 fail closed。

### Round 2：崩溃窗口与并发 finalization

覆盖 Store 前、Store 后 journal 前、journal commit 后 response 前和 Domain commit 前
窗口。journal commit 后注入进程退出，新 broker 不做第二次 Store 写即可恢复 exact ref；
Store 失败 tombstone 跨重启仍不可逆。多线程 finalization 最多产生一个进程内 Store
调用，跨进程 exact mapping 由 SQLite CAS/idempotent compare 收敛。

### Round 3：schema、容量、时钟与锁

发现并修复 expiry 清理的无索引扫描，以及“transaction 内删除后抛异常导致删除回滚、
时钟回拨可复活 grant”的 P0；过期删除现在先提交再返回固定拒绝。v2 精确校验索引并拒绝
额外 trigger/view。硬容量不淘汰未过期 tombstone。Broker 的 SQLite/Store I/O 均在进程
全局锁外；本地热路径只在 cache miss 读取 journal。伪造 grant、session、authorization、
staging digest 或 final ref 任一绑定均拒绝。

## 7. 验证

```bash
.venv/bin/python -m unittest -q \
  tests.test_orchestration_remote_execution_journal \
  tests.test_orchestration_artifact_broker \
  tests.test_orchestration_worker_security \
  tests.test_orchestration_remote_execution \
  tests.test_orchestration_remote_protocol
```

关键证据：

- journal v1→v2 原子迁移、并发 publish/CAS、generation 隔离、容量、权限/symlink/
  schema/index/trigger；
- 新 Gate 对同一 workload/session/action lineage 生成相同 authorization digest；
- 全新 Gate/Broker/Admitter 接受运行中失败终态；
- control-plane 端到端重启后接受 exact signed failure；
- Worker 篡改 journal-bound digest 被拒绝；
- pre-restart read grant 只能兑换一次，finalized output handle 重放 exact ref；
- bearer canary 不出现在 SQLite main/WAL/SHM；
- finalization commit 后崩溃不触发第二次 Store 写，Store 失败 tombstone 跨重启；
- terminal crash residue 由 Store-backed reconciliation 清理。
