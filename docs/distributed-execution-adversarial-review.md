# Distributed Execution 对抗审查记录

> 状态：三轮审查、独立 postfix 复审与最终全仓门禁均已通过
>
> 门禁：三轮审查必须顺序执行；每轮修复后重跑本轮、此前轮次、R01–R20 和全量
> Orchestration 回归。未解决的 P0/P1 数量必须为 0。

## 第 1 轮：并发与一致性

### 范围与方法

独立审查者从线性化、重复消息、乱序、fencing、session ABA、缓存淘汰、控制面重启、
Artifact finalize、Fleet rebuild、observer 重入、registry 容量和 prepared TTL
生命周期检查 Phase 2 实现。审查者没有修改生产代码，只新增
`tests/test_orchestration_review_round1.py`，使用逻辑时钟和固定 barrier 复现交错。

### 发现

| 级别 | 缺陷 | 失败不变量 | 复现 |
|---|---|---|---|
| P1 | Worker session A→B→A 会重建 A 的旧 Store owner | 被新 session 取代的 authority 不得复活 | `test_session_instance_aba_restores_superseded_claim_authority` |
| P1 | prepared 绝对 TTL 可早于仍有效的 RUNNING lease | 及时 heartbeat 不得改变同一 claim 的执行 authority | `test_prepared_ttl_rotates_authority_for_a_live_running_claim` |
| P1 | response LRU 淘汰时同时遗忘 request-id 首次 intent | 同一 session 的 request id 只能绑定一个 canonical intent | `test_request_id_conflict_survives_response_cache_eviction` |
| P1 | Fleet durable mismatch rollback 可删除随后 rebuild 的新 binding | 旧 projection 收尾不得修改新 projection | `test_fleet_failed_projection_pop_races_rebuild_of_same_task_id` |

未发现 P0。初始复现命令：

```bash
.venv/bin/python -m unittest -v tests.test_orchestration_review_round1
```

初始结果为 4/4 红测稳定复现。

### 修复状态

- Fleet rollback 的 inflight generation barrier 已扩展到 durable 结果验证与完整回滚；
  binding 清理同时比较 scheduler identity 与预期 `task_id → run_id`，rebuild 在旧
  callback 收尾前 fail closed。专项 46/46 通过。
- session authority 改为独立 SQLite journal 分配的服务端单调 epoch；epoch 纳入
  session binding/Store owner。当前 instance 可跨控制面重启恢复同 epoch，被替换的
  instance 保留 durable tombstone，A→B→A 跨重启仍在 register 阶段拒绝。
- request-id canonical digest identity 已与 response LRU 分离并按当前 epoch 持久化；
  LRU 淘汰或控制面重启后不同 intent 恒冲突，同 intent 若没有正文/Domain terminal
  证据则 fail closed，不再次 mutation。active epoch 达到硬容量时拒绝新 request，
  不淘汰 live digest。
- journal 的 `BEGIN IMMEDIATE` 和磁盘 I/O 均在 control 全局锁之外；固定 blocking
  barrier 证明某 Worker 的 request/register journal 卡住时，其他 registration 查询
  不被串行阻塞。secure path 未显式注入持久 journal 时固定
  `security_not_ready`。
- session/request 专项与重启、容量、SQLite schema、16 路并发线性化测试均通过；
  prepared 记录同时拆分为未启动 idle TTL 与 RUNNING durable lease 生命周期。
  heartbeat、Artifact stage、complete 和 cancel 会重新认证同一 identity、runtime、
  rule、action 与 claim lineage；内部 credential 可轮换，外部 authority digest
  保持不变。

第 1 轮修复后门禁：

- 独立审查测试：5/5 通过（四个缺陷复现与全局锁 I/O barrier）。
- Journal、Protocol、Execution、Security、Artifact、Fleet 联合专项：84/84 通过。
- R01–R20：20/20 通过。
- 完整 Orchestration 回归：550/550 通过。
- 未解决 P0/P1：0。

### 无异常覆盖

- Artifact write/output-handle 并发 finalize 只调用 Store 一次并返回同一引用。
- 阻塞或抛错的 observer 不持有 Fleet coordinator 锁且不改变 Domain 状态。
- capacity 并发 poll、drain/poll、withdraw/claim、claim-lock 异常回收均保持有界。
- 审查前 R01–R20 为 20/20，Orchestration 基线为 534/534。

## 第 2 轮：安全与权限边界

### 范围与方法

独立审查者按身份认证、session fencing、最小权限、凭证生命周期、Artifact
授权、资源耗尽、输入绑定、TOCTOU、审批单次消费和安全降级检查远程执行面。
审查者不修改生产代码，只新增稳定复现测试。完成首轮修复后，又由另一名独立
审查者执行 postfix 复审，重点攻击 candidate → ticket → Store 原子提交 →
postcommit grant 的失败窗口。

### 发现

首轮安全审查发现 3 个 P1：

| 级别 | 缺陷 | 失败不变量 |
|---|---|---|
| P1 | response、claim 和 control record 的默认 `repr` 暴露 bearer/claim token | 凭证不得进入日志和诊断字符串 |
| P1 | `stage_output` 没有按 Attempt 累计执行 output bytes/handle 上限 | 已授权 Worker 也不得绕过资源预算 |
| P1 | poll 在精确授权前先执行 `claim_next` | 精确 Worker/Policy 授权拒绝必须零 schedule/claim/policy mutation |

初始独立测试 5 项中 4 项稳定红测、1 项字段替换攻击保持通过。另记录 1 个
P2：部分冻结模型只冻结顶层 Mapping；wire parser 会深拷贝，当前没有形成远程
越权路径。

首轮修复后的独立 postfix 复审又发现 2 个 P1 和 1 个 P2：

| 级别 | 缺陷 | 失败不变量 |
|---|---|---|
| P1 | ticket begin 后发生 session supersession，旧 poll 仍可提交 claim/policy 并获得 broker authority | 被替换 session 不得在替换后线性化新的执行权 |
| P1 | 64 MiB materialized-script 上限只统计 ticket，commit 后 `_prepared` 常驻 bytes 不再计费，且物化发生在容量预留前 | inflight、ticket、prepared 的 slot/bytes 必须使用统一硬上限 |
| P2 | 非 two-phase adapter 在没有 ready candidate 时延迟暴露错误配置 | 安全配置错误应在空 poll 时同样 fail closed |

两个 postfix P1 均先由独立测试稳定复现为 2/2 红测；资源测试使用
17 × 4 MiB 脚本证明 68 MiB 常驻可突破 64 MiB 硬上限。

### 修复状态

- 所有远程 response/claim/control record 使用显式安全 `repr`，不包含 live token、
  签名或嵌套 bearer 数据。
- output staging 在 per-Attempt guard 内按实际 bytes 和 handle 数保守计费；
  Store/broker 模糊失败不回退预算，避免重试放大。
- Scheduler 先构造无 lease authority 的只读 exact candidate。计划解析、Artifact
  验证、Policy、Worker authorization 和 runtime attestation 在 Store 事务外完成；
  Store 事务只执行 candidate CAS、schedule、claim、approval grant 单次消费和
  `policy.decided`。
- admission ticket 为进程内、短 TTL、一次性对象；绑定 candidate、action、
  policy、Worker session、authorization 和 attestation。SQLite 等待写锁跨过 TTL
  时，在 `BEGIN IMMEDIATE` 后用新鲜时钟拒绝，保证零 mutation。
- `REQUIRE_APPROVAL` 首次 poll 只原子写入同一 Attempt 的
  `attempt.scheduled → WAITING_APPROVAL + approval.requested`，不创建 Worker
  claim、idempotency lease、WorkerAuthorization 或 Artifact grant。批准后恢复同一
  Attempt 为 SCHEDULED，再次执行精确授权并在 claim 事务内单次消费 grant。
- session replacement 与 claim 使用按 Worker 隔离的有界线性化 guard。Store
  linearization 前和 postcommit grant 前均重新验证当前 session；不同 Worker 不共享
  该 guard。替换在 Store 后获胜时不补偿 durable claim，也不发布旧 session grant，
  由 lease recovery 收敛。
- materialized-script 在读取前按已验证 ArtifactRef size 预留统一 reservation；
  inflight、ticket、prepared 和 reference direct-admit 共用 slot/bytes 账本。
  ticket → prepared 只转移 reservation，不减少总账；失败、过期、CAS loser、完成、
  取消和 stale purge 精确释放一次并在锁外撤销 authority。
- legacy reference admission 默认关闭，必须由 control 和 adapter 双重显式启用。
  two-phase 方法缺失或配置错误即使当前没有 candidate 也立即
  `security_not_ready`。
- 安全 hierarchy 只读路由尚未实现时 fail closed，不调用会 reconcile/mutate 的旧
  `claim_next_child` 路径。
- 协议对象使用递归冻结的内部 JSON 表示并在读取时返回 detached value；
  `RemoteRequest` / `RemoteResponse` 的公开构造器也执行与 make/parse 相同的
  shape、digest 和深层不可变性校验。外部无法通过嵌套 dict/list alias 制造
  “正文已变但 digest 未变”的对象。

### 修复后门禁

- 独立首轮安全审查：5/5 通过。
- redaction/output budget 专项：6/6 通过。
- two-phase/preclaim/approval 专项：7/7 通过。
- 独立 postfix 审查：5/5 通过，包含 session supersession、68 MiB 常驻绕过、
  approval approve-vs-reject、三个 Store Event fault seam 和 postcommit publish
  failure。
- postfix 非审查回归：5/5 通过，包含不同 Worker 并发、Store 后 session 替换、
  物化前 reservation、direct-admit 统一账本和空 poll 安全配置。
- 第二轮 focused 回归：64/64 通过。
- R01–R20：20/20 通过。
- 完整 Orchestration 回归：579/579 通过。
- 全仓回归：1049/1049 通过。
- 未解决 P0/P1：0。

最终 postfix 又复攻了 make/parse 与公开构造器的嵌套容器 alias、构造后 mutation、
伪造 digest、错误 shape 和 repr redaction；未再发现 P0/P1/P2。

## Remote child hierarchy admission：三轮专项审查

### 第 1 轮：authority substitution 与越权

攻击面覆盖从 root/middle/leaf 任意 Run 发起 poll、用 root 授权代替 child 授权、
替换 scheduler/Store、伪造 parent/child/root/depth/definition/input binding、map index
与嵌套链顺序。审查发现原 candidate 的 Run projection version 虽已记录，但 Store
claim 路径只实质比较 Node version；已改为 Scheduler 和 Store 两层都比较 exact Run
version。最终实现要求请求 Run 位于 scope，authorizer 对 scope 中每个 Run 都返回 true，
目标 Scheduler 与入口 Scheduler 共享同一个 Store 实例，并在 Store 内从持久行重验完整
ancestry 与 child input receipt digest。

证据：

- `test_every_ancestor_and_target_require_authorization`
- `test_direct_child_poll_cannot_bypass_root_authorization`
- `test_nested_poll_carries_every_ancestor_hop`
- `test_map_poll_uses_deterministic_child_and_exact_scope`
- `test_corrupt_child_link_fails_closed_without_attempt`

结论：未保留 P0/P1；损坏或越权均在 Attempt 创建前 fail closed。

### 第 2 轮：TOCTOU、取消与租约存活

攻击面固定在 candidate 已授权但 Store 尚未 claim、claim 已提交但 Activity 尚未 start、
以及 Activity 已运行并持续 heartbeat 三个窗口。父 Run 的任意 projection 变化都会使
scope CAS 失效；父状态撤销或 hierarchy link 在 ticket 期间被修改会在
`BEGIN IMMEDIATE` 内重验并回滚 child schedule/claim。

本轮发现父取消已提交、向 child 的 reconcile 尚未执行时，remote cancellation probe
原本只看 child projection，恶意 Worker 还能继续续租。修复后 probe 从同一 SQLite
read snapshot 验证持久 ancestor authority；失效立即报告 cancellation，heartbeat
在自身写事务内再次拒绝续租，start 和非取消 completion 也拒绝越界。精确绑定的
`CANCELLED` 回执只校验链路完整性而不要求祖先仍 RUNNING，使撤销后的在线 Worker
可以安全确认终止并让父 Run 收敛。

证据：

- `test_parent_projection_change_between_prepare_and_claim_is_stale`
- `test_child_link_damage_during_admission_is_rechecked_in_store`
- `test_parent_cancellation_between_prepare_and_claim_rolls_back_child`
- `test_parent_cancellation_after_claim_blocks_start_gate`
- `test_heartbeat_rechecks_parent_after_read_probe_race`
- `test_parent_cancel_allows_exact_child_cancel_acknowledgement`
- `test_root_remote_cancellation_also_fences_lease_renewal`

结论：未保留 P0/P1；父 authority 撤销不依赖异步传播才能阻止新副作用或无限续租。

### 第 3 轮：重启、滚动升级与边界耗尽

攻击面覆盖空内存 Workflow registry 重启、旧 schema 进程继续写库、v7 中已有
unscoped/malformed/inactive remote child authority、trigger 被旧连接绕过、深层递归、
环和 child fan-out。根、subworkflow 和 derived map Workflow 都进入 immutable durable
binding，重启后 resolver 按 child Run identity 恢复实际 Scheduler。选路沿用
`max_depth`、每控制节点 child 上限和 total descendant 上限，scope 自身再限制 64 hops
并拒绝环、不连续链和非规范字段。

本轮把迁移门禁从“只查无 scope”加强为逐条解析并验证所有 active remote child
authority；任一 unsafe 记录使 v7→v8 整体回滚。成功迁移后 active INSERT/UPDATE trigger
拒绝旧进程写入缺失或陈旧 scope，local hierarchy worker 保持兼容。

证据：

- `test_restart_recovers_child_scheduler_and_claim_authority`
- `test_schema_eight_fences_legacy_unscoped_remote_child_claim`
- `test_schema_eight_migration_requires_legacy_child_drain`
- `tests.test_orchestration_hierarchy` 的 depth/cycle/child/descendant hard-limit 用例

结论：三轮专项审查后未保留 P0/P1/P2。生产升级必须先 drain 被迁移门禁报告的 unsafe
remote child Attempt；不得删除 trigger、手工补 scope 或把所有 child 映射到 root
Workflow 来绕过恢复校验。

## 第 3 轮：故障恢复与运维

### 范围与方法

独立审查者检查控制面崩溃/重启、SQLite 部分损坏、并发启动、时钟回拨、长 Store
I/O、滚动升级兼容、Artifact 常驻内存、授权过期、`OUTCOME_UNKNOWN` 和取消恢复。
首轮只新增 `tests/test_orchestration_review_round3.py`；修复后由另一名独立审查者
新增 `tests/test_orchestration_review_round3_postfix.py`，生产代码均由主流程修复。

### 发现

首轮发现 2 个 P1、2 个 P2，并证伪 1 个疑似容量问题：

| 级别 | 缺陷 | 失败不变量 |
|---|---|---|
| P1 | Artifact broker 只有单 Artifact 上限，没有跨 Attempt 的 staged/finalizing 总常驻字节账本 | 每个合法 Attempt 不能叠加突破进程级硬上限 |
| P1 | write bearer 被观察为过期后，时钟回拨可使同一 grant 复活 | 过期/失败/消费是不可逆状态 |
| P2 | secure direct poll 没有受支持的 Workflow/Activity runtime compatibility 声明和执行路径 | 滚动升级期间不兼容 Worker 必须在 Store mutation 前拒绝 |
| P2 | metadata 声明 v1 但结构残缺的 journal 可通过启动，到首个请求才失败 | 控制面不得以损坏身份 journal 宣告 ready |

过期 finalized write record 会由既有 purge 正确释放 active-grant 容量，因此该项
保持绿测，不计 finding。

postfix 又发现 1 个 P1：既有 v1 journal 缺失 `remote_request_identities` 时，
`CREATE TABLE IF NOT EXISTS` 会静默补空表，历史 request digest identity 丢失，
同 request id 在重启后可被当作新请求接受。进一步检查证明“全部四张表丢失但文件
仍存在”也不能被当成首次建库，否则 session epoch、ABA tombstone 和请求幂等证据
都会重置。

最终全仓门禁还稳定复现 1 个并发 P1：registry purge 的只读 `restore_claim` 先读旧
lease，heartbeat 随后延长同一 token/fencing authority，第二次投影校验却要求 deadline
完全相等，因而把 live prepared record 误判为 stale 并删除。修复前旗舰 Demo 可在
几十次并发运行内出现 `prepared_registry_miss`。

### 修复状态

- Artifact broker 新增全局 `maximum_resident_bytes`，stage 在锁内原子 charge，
  超限拒绝新 stage 且不淘汰旧合法内容。staged → finalizing 到 Store I/O 返回前持续
  计费；成功、失败、过期、purge 和并发 finalize 通过幂等 helper 精确释放。内容
  hash、Store write/verify 均在锁外。
- write grant 的 `expired/failed/consumed` 状态改为不可逆 tombstone；所有 resolve、
  stage、finalize 入口使用统一 live-record 校验，时钟回拨不能复活 bearer。
- Workflow v2 Activity Node 支持保留 metadata
  `min_runtime_version` / `max_runtime_version`。编译器把合法范围冻结并纳入 definition
  digest，拒绝非 canonical、倒置范围和非 Activity 声明；direct secure poll 在
  candidate 后、adapter preflight 和 Store mutation 前用与 RemoteTask 相同的中立
  `RuntimeCompatibility` 规则拒绝不兼容 Worker。
- journal 启动执行 `quick_check`、foreign-key check、精确列/FK/唯一索引验证。
  既有文件必须在任何建表动作前拥有完整 schema 和 metadata；缺一张或全部表均
  fail closed，不静默修补。
- 首次 durable journal 不再直接写最终路径：同目录固定 bootstrap lock 串行化
  初始化，marker 区分 `bootstrapping` / `initialized`，固定临时 SQLite 以单事务
  完成四张表、metadata、完整性校验和 fsync，再用 no-clobber hard link 原子发布。
  发布前崩溃可安全重试，link 后 marker 前崩溃会验证已发布库后收敛；初始化证据
  已存在但最终库丢失时 fail closed。临时库及 sidecar 名称固定，重复崩溃不会无界
  积累。
- 同一 request/owner/token/fencing authority 的 heartbeat 只允许 durable deadline
  单调延长。claim 中旧 deadline 可由当前 durable deadline 证明仍有效；worker
  声称的未来 deadline、旧 fencing、foreign token/owner/request 仍 fail closed。
  registry purge 不再因续租读交错删除 live prepared record。

### 修复后门禁

- 首轮独立审查：5/5 通过。
- postfix 独立 canary：6/6 通过。
- Round 3 + postfix + journal 回归：19/19 通过。
- runtime compatibility 联合专项：90/90 通过。
- Artifact broker 专项：27/27；相关 remote execution/protocol/fault matrix：
  62/62 通过。
- Journal 多进程/崩溃独立复核：600/600 个并发构造成功；发布前 SIGKILL 5/5
  可恢复，link 后 marker 前、既有库验证窗口和 final 丢失均符合 fail-closed 契约。
- Orchestration 回归：606/606 通过。
- 旗舰 Demo：功能断言通过；修复 heartbeat/purge 竞态后 50 次独立 CLI 并发压力
  运行全部通过。
- 未解决 P0/P1/P2：0。

## 最终交付门禁

- 三轮独立审查及各自 postfix：通过；未解决 P0/P1/P2 为 0。
- R01–R20：20/20 通过。
- Orchestration：606/606 通过。
- 全仓：1076/1076 通过。
- 旗舰分布式 Demo：primary completed、cancelled 分支完成、replay 与 live 一致、
  三 Worker 最大并发 3。
- 有限 soak：10/10 logical runs，30 个 remote attempts，全部完成；该数字只证明
  reference workload，不声明生产 SLO。
- 新增生产代码、示例和文档的 credential/private-key、本机绝对路径扫描无命中；
  README 仅保留既有空配置占位符 `API_KEY=`。
- Python 编译、`git diff --check`：通过。
