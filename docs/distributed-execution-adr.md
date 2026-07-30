# Secure Distributed Execution ADR

> 状态：Phase 2 实现契约
>
> 依赖：[durable-orchestration-spec.md](durable-orchestration-spec.md)、
> [architecture.md](architecture.md)、[tool-layer.md](tool-layer.md)
>
> 范围：远程 Worker、能力调度、工作负载身份、Artifact broker、强 Sandbox 组合、
> 运维证据和故障注入

## 1. 决策

XAgent 在已经通过本地 F01–F30 故障矩阵的 Durable Orchestration 之上增加远程执行平面。
控制面继续是 Run、Node、Attempt、lease、fencing、Receipt 和 Artifact 引用的唯一事实
写入者。远程 Worker 是不受信任的执行能力提供方，只能通过版本化协议申请工作、报告
活性和提交候选结果。

第一版远程执行采用“单控制面服务 + 可横向扩展 Worker”：

```text
trusted caller
  -> control-plane transport/authn
  -> RemoteControlService
       -> Scheduler / Store / Policy / Approval
       -> ArtifactBroker
       -> RemoteAdmissionController
  -> versioned response

RemoteWorker
  -> attested identity
  -> poll/claim
  -> fetch exact immutable Artifacts
  -> attested Sandbox backend
  -> heartbeat/cancel polling
  -> candidate Receipt
  -> control plane verifies and commits
```

远程 Worker 禁止：

- 打开、复制或挂载 SQLite、WAL、SHM、ArtifactStore 根、GC 根或控制面锁；
- 自行构造可信 identity、policy decision、approval、fencing token 或 verified Receipt；
- 依据 telemetry、HTTP 成功码或本地缓存推进 Domain 状态；
- 在协议中传输原始 secret、宿主路径、控制面路径或任意环境变量值；
- 把“消息只发送一次”表述为副作用 exactly-once。

## 2. 正确性边界

### 2.1 事实所有权

只有控制面进程能够调用 `DurableRunStore` 的 mutation API。Worker transport adapter
只调用 `RemoteControlService`；服务端根据受信 transport context 注入 Worker identity，
禁止从请求 JSON 读取可信身份。

网络响应丢失时：

- 相同 request id、相同 canonical intent：响应正文仍在有界 LRU 时返回首次结果；
- 正文淘汰或控制面重启后，非终态请求 fail closed 而不是再次 mutation；远程 terminal
  completion 只能依据 Store 中精确绑定的 Receipt/Event 证据重放；
- 相同 request id、不同 intent：完整性冲突；
- completion 已提交但响应丢失：重试只返回原终态，不再次执行；
- 无法证明 completion 是否提交：Worker 查询 Attempt，不自行重跑外部副作用。

### 2.2 远程 lease

本地 lease/fencing 语义保持不变，但远程协议增加完整绑定：

```text
protocol_version
request_id
worker_identity_digest
worker_session_id
run_id / node_id / attempt_id
lease_id / fencing_token / claim_token
action_digest / execution_binding_digest
request_digest
```

每个 heartbeat、start、completion、cancel acknowledgement 都必须验证以上适用字段。
heartbeat 只能为相同 request、owner、claim token 和 fencing generation 单调延长
durable deadline。控制面内部持有的较旧 deadline 可由当前 durable deadline 证明仍属
同一 authority；worker 声称的未来 deadline、旧 fencing 或 foreign token 不能据此
获得权限。registry purge 与 heartbeat 并发时不得把这种续租读交错误判成 stale claim。
控制面使用独立、受保护的 SQLite `RemoteControlJournal` 为每个逻辑 Worker 分配服务端
单调 session epoch。epoch 纳入 session binding 和 opaque Store owner。控制面进程
重启后，同一受信 identity 与当前 `instance_id` 从 journal 恢复相同 epoch，因而可恢复
仍由 Store 持有的原 claim；这不是创建新 session。新的 `instance_id` 原子递增 epoch，
旧 instance 写入永久 tombstone，A→B→A 即使跨控制面重启也不能复活。Store 中更高
fencing generation 仍立即使旧消息失效。

同一当前 epoch 的 request-id→canonical digest 也写入 journal，和响应正文 LRU 分离。
当前 epoch 的 digest 不按时间或 LRU 淘汰；达到硬容量后 fail closed。session 被替换后，
旧 epoch 已不可能认证，其 request rows 可在替换事务中删除；instance tombstone 不设
TTL，因为删除 tombstone 会重新引入 ABA。journal 不进入 Domain Event，不保存 response、
grant、claim token、runtime proof 或 telemetry，并且绝不能挂载或暴露给 Worker。
既有 durable journal 在宣告 ready 前必须通过 exact schema、unique index、foreign-key
和 SQLite integrity 校验；任何缺表或空 metadata 都 fail closed，不能自动补表后遗忘
历史 identity。首次建库只允许在同目录临时 SQLite 中完整事务提交并 fsync，再用
no-clobber 原子发布到最终路径；崩溃或多进程并发初始化不能暴露 partial schema。

### 2.3 外部副作用

远程 transport 不改变 at-least-once Attempt 语义：

- read-only 可以在 lease 丢失后新建 Attempt；
- idempotent write 必须使用稳定 operation key，并优先 probe；
- 不可探测写操作在开始后失联进入 `OUTCOME_UNKNOWN`；
- 网络 timeout、TLS 断开、Worker exit 和 Sandbox kill 都不等于“未执行”。

## 3. 协议

协议是 transport-neutral 的严格 JSON 数据模型。HTTP/gRPC/stdio 只能负责 framing、
认证上下文和错误码翻译。

### 3.1 公共 Envelope

```json
{
  "protocol_version": 1,
  "request_id": "bounded-id",
  "operation": "register|poll|heartbeat|start|complete|cancel_status",
  "body": {}
}
```

约束：

- UTF-8 编码后最多 1 MiB；
- JSON 最深 32 层，容器 item 最多 4096；
- unknown field、bool-as-int、NaN/Infinity、重复语义字段全部拒绝；
- identifier、capability、tool、runtime version 均有字符和数量上限；
- error 只返回固定 code 和有界安全摘要，不回显原请求。

### 3.2 操作

| 操作 | 用途 | 必须绑定 |
|---|---|---|
| `register` | 建立短期 Worker session | transport identity、pool、tenant、capabilities、runtime |
| `poll` | 获取一个匹配的 durable claim | session、capacity、兼容版本 |
| `heartbeat` | 延长当前 lease | exact Attempt、lease、fencing、claim token |
| `start` | 提交越过副作用门禁的意图 | action/execution digest、policy authorization |
| `complete` | 提交候选 ToolReceipt | exact terminal outcome、Artifact refs、receipt digest |
| `cancel_status` | 查询持久取消并确认已停止 | exact Attempt、durable cancellation state |

`poll` 返回的 claim 只包含执行所需的有界 identity、digest、path-free descriptor
和 broker grant；
不包含数据库路径、Artifact 物理 URI、审批 actor、原始 operation key、secret 或任意
控制面文件路径。

实现时 `ArtifactRef` 只能留在控制面。Worker wire 实际接收 path-free descriptor
与一次性 broker grant；输出 completion 只提交 broker 已 finalize 的 opaque handle，
由控制面解析为完整 `ArtifactRef`。Worker 自行构造的 URI、`ArtifactRef` 或结果摘要
都不能直接进入 Store。

`complete` 和取消确认还必须携带有界的 runtime proof。该 proof 绑定 Worker session、
Claim/fencing、Action、ExecutionRequest、Sandbox profile、runtime attestation 和
SandboxReceipt digest，并由控制面注入的 trust anchor 验证。Worker 本地
`verify_candidate()` 只是早期拒绝，不构成控制面证据。

## 4. Worker 身份与授权

`WorkerIdentity` 由受信认证 adapter 创建，至少包含：

```text
trust_domain
subject
tenant_id
pool_id
capabilities[]
issued_at / expires_at
attestation_digest
```

授权必须回答：

```text
identity 是否能注册声明的 tenant/pool/capability？
identity 是否能执行该 tenant 下该 Tool/EffectClass？
identity 是否能读取或写入该 Attempt 的指定 Artifact？
identity 是否仍处于有效期且没有被 drain/revoke？
```

第一版提供可注入的 trusted authorizer，不内置对某个身份平台的网络依赖。部署可将
SPIFFE X.509-SVID、Kubernetes service account 或受信网关身份转换为上述对象。转换前
的 header、证书字符串或 JWT claims 都是不受信输入。

## 5. Artifact Broker

远程 Worker 不能看到 LocalArtifactStore 路径。Broker 只接受完整 `ArtifactRef`，
验证 Store 绑定后签发短期 grant：

```text
grant_id
tenant / worker_session / run / attempt
direction             read | write
artifact identity
max_bytes
issued_at / expires_at
single_use
grant_digest
```

规则：

- grant 使用服务端不可预测随机数，不使用 Artifact digest 充当 bearer credential；
- grant 原文不进入 Domain Event、telemetry 或错误文本；
- 使用时 atomically consume；重复使用只允许幂等读取同一内容；
- read 完成后重新验证 size/SHA-256；
- write 先进入临时对象，验证后才能由控制面 Event Tx 建立引用；
- completion 只携带 finalize 后的 opaque output handle；控制面必须再次 resolve，
  验证 handle、grant、authorization、Attempt 和内容摘要的完整绑定；
- `secret` Artifact 默认不允许远程读取，必须有显式 capability 和部署级加密/可信
  broker；
- list、任意路径读取、前缀扫描和跨 tenant grant 全部禁止。

## 6. Sandbox 组合

`OCISandboxBackend` 只接收规范化执行请求并生成/提交受限 OCI execution spec：

- image 必须是不可变 digest，禁止 tag-only；
- root filesystem read-only；
- 非 root UID/GID，禁止 privilege escalation；
- drop all capabilities；
- seccomp profile 必须显式存在；
- host PID/IPC/network/user namespace 禁止；
- network 默认 deny，只接受已授权 egress policy digest；
- CPU、memory、PID、wall-time、output、临时磁盘全部有硬上限；
- mount 只允许 broker 管理的只读 input 和独立临时 output；
- 禁止 Docker socket、宿主根、控制面根和 Agent 之外任意 host path；
- command 使用 argv 数组，无 shell；
- 不继承宿主环境。

只有注入 backend 返回可验证 runtime attestation 时，Receipt 才能声明相应
`SecurityLevel`。生成 OCI spec、调用普通 subprocess 或测试 fake 不等于 gVisor/容器
强隔离，必须标为 `unverified` 或 `development_unsafe`。

## 7. 调度与准入

### 7.1 匹配

Worker 必须同时满足：

- tenant 和 pool 授权；
- Activity 所需 capability/tool/effect class；
- control-plane protocol version；
- Worker runtime version 与 Workflow/Activity compatibility；
- Sandbox security level；
- Worker 可用 capacity；
- tenant、pool、tool、resource key 配额。

任一信息缺失时不 claim，不能“先领取再发现不兼容”。

Workflow v2 通过 Activity Node 的保留 metadata 字段声明 Worker runtime
兼容范围：`min_runtime_version`（缺省 `"0"`）和可选
`max_runtime_version`。只要声明任一字段，编译器就使用
`remote_scheduling.RuntimeCompatibility` 的 canonical numeric 规则校验范围，
并把原字段纳入 Workflow definition digest；`02.1`、非数字版本和最大值小于
最小值均在编译期拒绝。未声明范围的旧 Workflow 保持无 runtime 限制，以兼容
历史使用的具名 runtime 标识；它不等同于显式声明任意字符串范围。secure direct
poll 在生成精确 candidate 后、任何 Store schedule/claim mutation 前，用同一规则
校验该 Activity 与注册 Worker。显式范围与不规范 Worker runtime 不匹配时返回空
assignment，且不创建 Attempt。

控制面采用两阶段准入：

1. 可信 reconciler 先把 Run/Node 推进到 `RUNNING/READY`；poll 本身不隐式
   reconcile，也不为 `CREATED/PENDING` 生成候选；
2. Scheduler 只读生成精确 candidate，绑定 definition、Node/Attempt projection、
   Attempt identity/number、request/operation key、资源、完整 config，以及保留逻辑
   input name/顺序的 Artifact 映射；candidate 的 claim token、fencing 和 lease 均为零；
3. Policy、Artifact 校验、Worker session/action 授权及 runtime attestation 在全局锁和
   Store 写事务之外完成，结果进入有界、短期、进程内、重启失效的单次 ticket；
4. `BEGIN IMMEDIATE` 获得写锁后重新读取时钟并检查 ticket expiry，再验证当前
   `RUNNING/READY`、definition、Node version、精确 SCHEDULED Attempt 或
   `max(attempt_number)+1` 新 Attempt，并在同一事务中 schedule/claim/policy/grant
   consumption；
5. candidate CAS 失败时撤销 ticket 和未使用 Worker authority。若 durable claim 已提交
   而后续 broker grant 组装失败，则不伪装成零 mutation，也不补偿删除 Event，而由 lease
   recovery 收敛。

`REQUIRE_APPROVAL` 是该事务的特殊终点：只 schedule 同一 Attempt，并直接写
`WAITING_APPROVAL + approval.requested`；不得产生 `attempt.claimed`、active
idempotency owner/lease、WorkerAuthorization 或 Artifact grant。可信 grant 将同一
Attempt 恢复为 `SCHEDULED`，下一次 poll 重新授权并在 claim/policy 事务中原子消费。
层级 child Run 尚未接入这条精确 ticket 路径，因此 remote poll 对 child routing
fail closed，不调用可能 reconcile/mutate 的旧 `claim_next_child`。

### 7.2 公平性

第一版使用确定性 tenant round-robin：

1. 每个 tenant 内按 durable ready order；
2. active tenant cursor 循环选择；
3. 超过配额、无兼容 Worker或被 backpressure 的 tenant 本轮跳过但不删除；
4. 每次成功 claim 后 cursor 前进；
5. tenant、queue 和 Worker registry 均有硬上限与空闲过期清理。

同一优先级持续有容量时，任何未超配额 tenant 都必须在有界轮数内获得一次选择机会。

### 7.3 Drain

Worker drain 后：

- 不再获得新 claim；
- 已有 claim 可在 deadline 内完成或响应取消；
- drain deadline 到期由控制面按 effect class 处理 lease；
- 相同受信 identity 与相同 `instance_id` 只恢复同一 Store owner；不同 identity、
  transport binding 或 `instance_id` 的注册创建新 session，不能接管旧 claim
  authority。

### 7.4 Reference 组合边界

当前 `RemoteControlPlane` 是按 Run 轮询的 reference protocol，尚未与
`RemoteFleetCoordinator` / `DeterministicRemoteScheduler` 组成生产 server/pull
数据面。control poll 已按精确节点 candidate 做两阶段准入，但 fleet projection 的
跨 Run 公平选择和 reference control 的按 Run poll 仍是两条边界，不能声称已端到端
支持生产异构 fleet。fleet claim callback 失败后必须丢弃 routing binding，并由 Store
reconciler 重新投影、重新准入。旧单阶段 adapter 只有在控制面显式设置
`allow_reference_admission=true` 且 adapter 同时声明 `reference_admission_only=true`
时才可进入开发兼容路径；任一条件缺失均在 Store mutation 前 fail closed。

`SecureRemoteAssignmentAdmitter` 的 prepared execution registry 与 reference
Artifact broker 的 staged/finalized registry 仍是进程内状态。session epoch/request
identity journal 已持久化，但其 SQLite 必须位于独立 control-plane isolation root；
进程内 `:memory:` journal 明确是 `development_unsafe`，secure poll/complete 路径
fail closed。控制面崩溃后，不得仅凭 untrusted completion 重建 prepared authority；
已写但未被 Domain Event 引用的 Artifact 由保守 GC 处理。生产化仍必须提供可恢复
prepared/staging registry、真实 transport 和独立隔离 backend。

## 8. Observability

远程观测是 projection，不是事实源。至少暴露：

```text
worker registrations / active / draining / expired
poll accepted / denied / empty
claim count / stale claim rejection
queue depth / backpressure rejection
schedule-to-start latency
heartbeat accepted / rejected / lease lost
completion accepted / replayed / rejected
cancel observed / cancellation unknown / leak
outcome_unknown
artifact bytes / broker grant denied
per-tenant active and quota rejection
```

标签只允许低基数 allowlist：operation、outcome、effect class、pool、runtime major/minor。
禁止使用 run id、attempt id、worker subject、tenant secret、Artifact digest、路径或错误
原文作为 metric label。详细 identity 只进入受控审计查询的 digest。

## 9. 兼容与迁移

- `src.orchestration` 导入仍不启动 server、Worker、线程或网络；
- 本地 `TrustedActivityExecutor` 和 F01–F30 行为不变；
- 远程能力显式组合，没有 transport/authenticator/authorizer 时 fail closed；
- secure remote composition 必须显式注入受保护的持久 `RemoteControlJournal`；
  内存 journal 不能进入 assignment/poll/complete 路径；
- Store schema migration 必须支持旧数据库，并拒绝未知更高版本；
- 协议破坏性变更提升 `protocol_version`，服务端只支持明确 allowlist；
- Worker 滚动升级期间，新旧版本只能领取各自兼容 Activity。
- Workflow `version` 是业务定义版本，不参与 Worker runtime 比较；runtime 范围只由
  Activity 的受支持声明决定。范围字段已进入不可变 definition digest，同一
  `(workflow_id, workflow_version)` 仍由 Store binding 阻止静默改写。
- `DeterministicRemoteScheduler`/RemoteFleet 的 `RemoteTask` 与 secure direct poll
  共用 `RuntimeCompatibility` 的 1–4 段 canonical numeric 比较语义；不得在不同
  入口各自解释 semver 或把 `runsc-1.2` 猜测为数字版本。

## 10. R01–R20 故障矩阵

| ID | 对抗场景 | 必须证明 |
|---|---|---|
| R01 | 未认证 transport 调用 register | Store/Event/registry 零变化 |
| R02 | 请求身份字段冒充另一个 Worker | 只信 transport identity，body 冒充无效 |
| R03 | 同 request id 不同 body | 完整性冲突，首次结果不改写 |
| R04 | claim 响应丢失后重复 poll | 不创建第二个 active Attempt |
| R05 | heartbeat 使用错误 lease/token | 拒绝且不延长 lease |
| R06 | 旧 Worker session 在重新注册后提交 | stale authority 被拒绝 |
| R07 | start 前 Worker 崩溃 | lease 到期后按 CLAIMED/ABANDONED 恢复 |
| R08 | start 后 read-only Worker 崩溃 | 允许新 Attempt，不接受旧 completion |
| R09 | 幂等写完成后断网 | probe/同 key 恢复，单一可见副作用 |
| R10 | 不可探测写完成后断网 | `OUTCOME_UNKNOWN`，零自动重试 |
| R11 | cancel 与 completion 乱序 | 只有一个合法 Tx 顺序，真实 Receipt 不丢 |
| R12 | drained Worker 与 poll 竞争 | drain 后零新 claim，已有 claim 可收敛 |
| R13 | capacity=1 并发 poll | 最多一个 active claim |
| R14 | tenant quota 并发竞争 | 不超配额，不发生跨 tenant claim |
| R15 | 恶意 tenant 持续 poll | 其他 tenant 在有界轮数内获得调度 |
| R16 | 过期/错绑 Artifact grant | 零字节泄露，零引用提交 |
| R17 | Artifact bytes 与 ref digest 不符 | fail closed，不启动 Sandbox |
| R18 | Sandbox 请求挂载控制面/宿主路径 | backend 调用前拒绝 |
| R19 | runtime attestation 伪造或缺失 | Receipt 不能声明强隔离 |
| R20 | telemetry exporter 全失败 | Domain 状态和调度结果不受影响 |

## 11. 三轮对抗审查门禁

实现完成后必须由三轮独立审查依次攻击，且每轮修复后重新执行此前全部审查：

1. **一致性与并发审查**：线性化点、重复消息、乱序、fencing、quota、drain、死锁、
   starvation 和 projection/replay。
2. **安全与越权审查**：身份冒充、跨 tenant、grant 重放、路径逃逸、secret 泄露、
   capability 提权、Sandbox attestation 和资源耗尽。
3. **故障恢复与运维审查**：进程/网络崩溃窗口、滚动升级、背压、取消泄漏、未知结果、
   观测失败和长期有界性。

每轮输出：

```text
review scope and attacker model
P0/P1/P2 findings
reproduction test
fix commit/diff
focused regression
previous-round regression
residual boundary
```

以下任一条件阻止交付：

- 存在未修复 P0/P1；
- R01–R20 任一没有直接可执行证据；
- F01–F30 或既有测试回归；
- Worker 能直接读取控制面存储；
- 未证明的 backend 声称强隔离；
- 安全或可靠性指标来自 telemetry 推断而不是 Domain/测试证据。

## 12. Phase 2 验收

- 至少三个逻辑 Worker session 并发执行同一 Workflow 的独立节点；
- Worker 只持有协议对象和短期 broker grant；
- R01–R20 全部通过确定性 fake clock/barrier/fault seam；
- F01–F30 与全仓库测试继续通过；
- 三轮对抗审查全部关闭 P0/P1；
- soak 使用显式逻辑工作量和单独 wall-time 报告，不伪造生产 SLO；
- 文档明确区分 reference transport、真实 mTLS、OCI spec 和实际 gVisor/Kubernetes
  deployment；
- 旗舰案例能展示远程 claim、并行、审批、取消、Artifact、Receipt、replay 和审计证据。
