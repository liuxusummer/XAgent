# XAgent 系统架构设计

## 1. 系统定位

物理级全能执行 Agent——非对话式，强调工具调用闭环。

核心能力：文件读写 / 代码执行 / 浏览器控制 / 用户交互 / 记忆管理

设计哲学：探测优先、失败升级、最小副作用

## 2. 分层架构

```
┌──────────────────────────────────┐
│  Frontends（多端适配层）           │
│  CLI / Feishu / Telegram / ...   │
├──────────────────────────────────┤
│  Durable Orchestration（可选控制面）│
│  Workflow / Scheduler / Event Log│
├──────────────────────────────────┤
│  AgentCore（核心引擎层）          │
│  XAgent → Agent Kernel → handler │
├──────────────────────────────────┤
│  LLM Layer（模型接入层）          │
│  Session / ToolClient / SSE      │
├──────────────────────────────────┤
│  Tool Layer（工具执行层）         │
│  code_run / file_* / web_* / ... │
├──────────────────────────────────┤
│  Infrastructure（基础设施层）     │
│  BrowserDriver / memory / SOP    │
└──────────────────────────────────┘
```

### 层间边界

| 层 | 职责 | 上行输出 | 下行依赖 |
|---|---|---|---|
| Frontends | 用户 IO、消息渲染 | 用户输入 → task_queue | display_queue |
| Durable Orchestration | 长任务状态机、DAG/父子 Run、策略门禁、恢复与回放 | Run/Event/Artifact 引用 | AgentCore adapter + Tool adapter + SQLite |
| AgentCore | 循环调度、Principal/Policy、知识与上下文控制 | ActionResult（含 flags）/ 退出信号 | LLM Layer + Tool Layer |
| LLM Layer | API 调用、历史裁剪、SSE 解析 | ChatResponse（统一格式） | API Key / Endpoint |
| Tool Layer | 工具执行、结果封装 | ActionResult | 文件系统 / 浏览器 / 子进程 |
| Infrastructure | 底层能力封装 | 原始数据 | OS / 网络 |

### 层间数据流

- 通信机制：`queue.Queue` + 生成器协议
- 线程模型：前端主线程或事件循环只负责请求协调；高开销的 Agent 构建、文件/索引/网络 I/O 等阻塞工作进入工作线程，`agent.run()` 由独立后台线程执行
- 会话准入：任务提交在 Agent 构建前用一次性令牌原子预占会话；`starting`、`running`、`waiting_for_user` 均拒绝重复提交，初始化或线程启动失败时释放预占
- 会话恢复：Web 持久化聊天对应的任务级 checkpoint ID；服务重启后将遗留的运行态标记为 `interrupted`，仅允许按该聊天绑定的 checkpoint 恢复，禁止回退到工作区级 `latest`
- Web 访问边界：除校验 TCP 对端为 loopback 外，还必须校验 `Host` 属于本地或显式配置的 allowlist；不得用任意 `Origin == Host` 作为准入依据
- Web 身份入口：未配置注册表时只提供 loopback 单用户兼容模式；多用户部署必须设置
  `XAGENT_WEB_IDENTITY_REGISTRY`。安全模式只接受 opaque Bearer/HttpOnly Cookie，subject、
  tenant、scope 和 workspace 映射全部来自工作区外的受信注册表，忽略客户端身份声明头；
  映射的 workspace 必须预先存在，加载时绑定规范路径及 device/inode identity；跨 owner
  重叠和注册表 containment 通过物理祖先 identity 校验，不依赖路径大小写拼写
- Web 授权传播：认证得到的不可变 `WebIdentity` 必须原样转换为 `Principal` 并传播到
  Agent Loop、子 Agent、Team、Memory、Retrieval、Checkpoint、Eval 和 Telemetry；
  任一内部组件不得自行补 scope 或退回固定 `local-user`
- Web 所有权隔离：同名 workspace alias 由服务端映射到不同物理根；chat、task、trace、
  Eval dataset/run 的物理存储、eval cancel handle 和内存 session 均绑定非秘密 owner
  digest。跨 owner 的 ID 查询只返回 not-found/denied，安全响应同时隐藏宿主绝对路径
- Web 身份生命周期：注册表按文件 identity 热加载。身份被禁用、删除或边界发生变化时，
  下一次请求或 30 秒调度巡检会取消其 Eval、停止并移除其所有会话，且不会再通过旧映射
  写盘；workspace 被替换或改为符号链接时同样撤销对应运行态。注册表缺失、损坏或没有
  有效身份时撤销全部安全 Web 运行态并返回 503
- Web scope 边界：HTTP 路由先做粗粒度 `workspace.read/write/delete`、`state.write`、
  `user.interact` 检查，内层 Tool Policy 再校验具体 capability；两层都 fail closed。
  本地 operator 的 `host.read` 不得由安全 Web 注册表授予，租户确认不能读取 workspace 外文件
- Web 出口边界：浏览器和 Eval URL 下载统一通过仅允许公网目标的本地代理；代理将 DNS 校验结果绑定到实际 TCP 连接，所有重定向和页面脚本发起的请求都重复执行该约束
- Web 流式状态：LLM XML 包装按 chunk 增量解析，原始诊断尾部与单个工具载荷均设 65,536 字符上限；会话事件日志最多保留 2048 条，落后于保留窗口的 SSE 客户端通过 `session_snapshot` 恢复当前消息状态
- 共享存储：Runbook、Memory、定时任务等工作区级“读—改—写”必须持有统一工作区锁；写盘使用同目录唯一临时文件并原子替换，禁止直接覆盖
- 数据格式：层间只传 `ActionResult` 或 `ChatResponse`，禁止跨层直接访问内部状态
- Agent Kernel：默认路径的工具授权、Memory 候选、EvidenceBundle 和 ContextManifest
  使用统一的有界元数据契约；完整边界见 `docs/agent-kernel.md`
- Durable Orchestration 是显式启用的外层控制面：它只持久化 Run、Node、Attempt、Domain
  Event 和 Artifact 引用，不持久化 Agent Loop 的 provider 内部状态，也不允许原始工具输出越过
  Artifact 边界。未启用时，现有 CLI、Web 单 Agent 和 Team Workflow 行为保持不变。完整协议见
  `docs/durable-orchestration-spec.md`
- Agent Activity 终态以 `AgentActivityReceipt` 绑定 request digest、规范 NodeResult 和
  Artifact digest；它只证明运行时观察到整个 Loop 边界，不会把内部未回执的工具副作用
  提升为 verified 或 exactly-once。完整契约见 `docs/agent-activity-receipt.md`
- `AgentActivityExecutionManifest` 进一步用父 Attempt 派生的确定性 child operation key
  绑定有序 ToolReceipt、精确 request Artifact/definition 和分类；schema v2 还绑定
  有序、payload-free 的 ProviderInvocationReceipt 与 response Artifact ref digest。
  只有 v2 receipt 实际加载并验证该 manifest，且显式要求完整 provider lineage 后，
  才具备远程 Agent 所需的工具/provider lineage 前置证据。见
  `docs/agent-execution-manifest.md`
- Core Agent Loop 通过可选 execution-evidence observer 在每次 provider/tool 调用前后
  发出 out-of-band 观察；`AgentExecutionEvidenceCollector` 只接收
  `ChatResponse.provider_receipt` / `ActionResult.tool_receipt` 的精确 typed receipt，
  缺失、失败、错绑或超限永久降级为 partial。普通本地路径不产生 receipt，不能被观察
  Event 或业务结果自动提升为完整证据；审查见
  `docs/agent-execution-evidence-collector-adversarial-review.md`。
- Agent Activity 的 task/context 可由 `AgentActivityRequest` 物化为至少 sensitive
  （并继承 context 最高分类）的 content-addressed Artifact，绑定稳定
  Attempt/request/definition/config/input descriptor，但不绑定 Worker/lease/grant。
  当前仅提供输入契约与 Broker 单次读取闭环，安全远程 adapter 仍为 Tool-only；见
  `docs/agent-activity-request.md`
- 远程 Agent 的长期 provider credential 必须留在部署侧模型网关；Worker 只能获得绑定
  tenant/Attempt/request/route/invocation-index 的短期单次 `ProviderAccessGrant`。
  Broker 可将 token digest、逻辑调用唯一键和消费墓碑写入隔离的
  `RemoteExecutionJournal`，跨进程原子发放/消费；completed invocation 可经
  MODEL_RESPONSE Artifact 重放；部署侧 `RecoverableProviderInvoker` 还可用稳定
  operation ID 的 NOT_STARTED/COMPLETED 证据收敛部分 unknown 窗口。成功调用返回
  canonical `ProviderInvocationReceipt`，正常完成、恢复与 durable replay 共享稳定
  digest；但参考实现不能证明外部幂等账本或 attestation，
  `production_security_ready` 固定 false，不能据此开放 remote Agent；见
  `docs/provider-credential-boundary.md`
- Durable Store、Artifact、GC 和锁必须放在 Agent workspace 外，由独立控制面
  service/OS identity 持有，且不挂载给 legacy 文件、代码或浏览器工具。默认 Web UI 不会
  自动发现数据库；投影服务必须显式注入受信 tenant→database 映射。同 UID 隐藏路径不构成
  隔离。
- Policy simulation、conformance 和 operator diagnostics 只读取并解释现有
  `PolicyEngine`/projection；它们不签发 grant。未知副作用只能经受权
  `resolve_recovery` 使用已验证 evidence/result Artifact 收敛，原
  `OUTCOME_UNKNOWN` Attempt 不可变。边界见 `docs/policy-operations.md`。
- Orchestration Runtime 的进程内 Scheduler 仅是有界 LRU 加速层，默认最多缓存 64 个，
  构造参数只接受 1–1024；
  Domain Store 与不可变 Workflow Artifact 才是事实源。缓存优先淘汰 terminal Run；
  Runtime 提交的 active Run 通过持久化 `runtime_workflow_ref` 自治验证、读取、编译并
  重建 Scheduler，外部 `scheduler_resolver` 只用于部署覆盖或非 Runtime 创建的 Run。
  缺失/损坏/身份不匹配的 definition fail closed。LRU 锁串行化缓存替换，但缓存容量不
  限制可持久 Run 数量。
- 可选的 Phase 2 远程参考面保持 Store/Scheduler 为唯一执行事实：worker 只接收无宿主
  路径的 execution plan、会话绑定 Artifact grant，并回传 output handle 与签名 runtime
  proof。`RemoteControlPlane` 同时提供按 Run `poll` 和跨 Run `poll_fleet`；
  后者通过可信 Fleet 组合接入 server/pull transport。可信 reconciler 必须先把
  Run/Node 推进到
  `RUNNING/READY`；run-scoped `poll` 或空 body 的跨 Run `poll_fleet` 随后只读生成
  不含 lease authority 的精确 Attempt candidate，
  在 Store mutation 前完成节点级 policy、Worker session、Action、profile、Artifact
  与 runtime attestation 绑定，再以短期单次 ticket 做原子 candidate CAS + claim。
  旧的 claim-then-authorize reference adapter 默认禁用，只有控制面显式开启且 adapter
  同时标为 reference-only 时才可用于开发测试。
  Worker session epoch 与 request-id digest identity 使用独立、受保护的控制面 SQLite
  journal；journal 路径不得挂载给 Worker，内存 journal 的 secure path fail closed。
  durable journal 首次建库经临时库完整提交后原子发布；既有库必须通过 schema/FK/
  integrity 校验，禁止缺表时自动重建。
  若精确 Activity 位于 child Run，read-only routing 同时生成有界 root-to-child
  authority scope；控制面授权每个祖先和目标 Run，Store 在 claim 事务内 CAS 父
  Run/控制 Node 并验证持久 hierarchy link。scope 随 Attempt 持久化，start/complete
  再检查祖先仍 active。schema v8 trigger 拒绝旧进程写入无 scope 的 remote child
  active Attempt，升级前必须 drain 此类既有 authority。
  Fleet routing 使用服务端版本化 Tool/Worker policy，并把 capabilities、resource
  keys、runtime 和精确节点/config 纳入匹配；Worker register 声明只能缩小权限。
  assignment 返回前还会写入独立的 digest-only `RemoteExecutionJournal`；它不保存
  claim/grant token、脚本、环境值、Artifact 内容或 raw plan。schema v2 额外保存
  token digest、canonical safe grant metadata、单次消费/失败墓碑和 exact finalized
  `ArtifactRef`；schema v3 再增加 provider grant 的逻辑调用唯一键、token/binding
  digest、消费墓碑和 purge 时间水位，但不保存 bearer、prompt、response 或 provider
  credential；schema v4 增加实际 provider payload digest 与
  invoking/completed/outcome-unknown receipt，completed 可引用 sensitive
  MODEL_RESPONSE Artifact；schema v5 增加有界的 NOT_STARTED/COMPLETED gateway
  recovery evidence digest 与 verifier id，但 journal 仍不保存响应正文或证据原文。
  控制面重启后可从 exact Store
  policy Event、当前配置与新鲜 attestation 重建 start/heartbeat/取消/终态 authority；
  Worker 持有的原 read grant 可跨进程只兑换一次，原 output handle 可重放 exact final
  ref，但控制面绝不重新发送 assignment 或自行复活 bearer。Fleet claim 会把无密的
  task/tenant/pool、routing/quota policy digest 和生效配额写入 Attempt；共享同一
  Store 的控制进程在 claim 事务内原子执行 global/tenant/pool quota CAS。Fleet queue、
  Worker registry 与 active routing 仍为进程内 projection；多控制面部署可启用
  Store 本地 pool shard 单写者，显式控制面 owner、单调 epoch 和持久 tenant 公平游标
  在同一 claim 事务中 fencing 陈旧投影并记录最后成功选择。重启/接管必须在接纳任务
  前恢复该 pool 游标。显式 `DurableFleetReconciler` 从受信 Run route source 解析
  scheduler/tenant/pool，先收敛 terminal projection，再以撤销优先的锁内差量替换
  READY queue；policy 变化后旧 queued binding 不再等待 Worker 触发失败，active
  durable authority 则只延期到 terminal。生产 source 从当前 Store schema v8 恢复
  enabled/running route，绑定隔离控制面物理 Store identity，并以 admission v2
  generation digest 在 claim 事务和 `CLAIMED -> RUNNING` trigger 中 fencing 撤销与
  ABA；静态 source 仅是显式 opt-in 的测试实现。它不使用 wall-clock leader lease，也不
  提供跨 Store 共识或自动故障转移。fleet claim 准入后失败必须从
  Store 重新投影，不能把内存队列或 journal 当作 Domain 事实源。完整边界见
  `docs/distributed-execution-adr.md`、
  `docs/remote-execution-recovery.md` 和 `docs/remote-fleet-data-plane.md`。
- 生产维护由显式 `DurableMaintenanceSupervisor` 串行组合 restart bootstrap、
  deadline、lease recovery、Domain reconcile 和 Fleet projection。它不启动线程；
  扫描触顶、任一阶段失败或成功轮次过期都会撤空 queued Fleet projection 并失去
  readiness。报告只含计数和固定错误码，不携带 Event、ID、异常文本或宿主路径。
  部署契约见 `docs/orchestration-maintenance.md`。

## 3. 核心数据流

```
用户输入
  → put_task(query) → task_queue
  → agent.run() 取出任务
  → 组装 system_prompt + user_input
  → run_agent_loop(client, sys_prompt, user_input, handler, tools_schema)
      ↓ 循环
      client.chat(messages, tools) → LLM 响应
      ↓
      解析 tool_calls → handler.dispatch(tool_name, args)
      ↓
      工具执行 → ActionResult(data, next_prompt, should_exit, flags)
      ↓
      flags 处理（如 reset_tools → 重置 client.last_tools）
      ↓
      拼接下一轮 prompt → 继续循环 或 退出
  → display_queue.put({'done': full_resp})
  → 前端渲染输出
```

### 消息拼接策略

只传当前轮：`messages = [{"role": "user", "content": next_prompt, "tool_results": ...}]`

完整历史由 `Session.history` 维护；Agent Loop 在 LLM 调用前通过 `ContextBuilder`
按组件 token 预算重建当前可见输入，Session 再对协议相关历史执行 token 级压缩。

### 退出条件

| 条件 | exit_reason |
|---|---|
| `should_exit=True` | EXITED |
| `next_prompt` 为 None | CURRENT_TASK_DONE |
| `next_prompt` 为空字符串 | EXITED（中断） |
| `turn >= max_turns` | MAX_TURNS_EXCEEDED |
| 无 next_prompt 且无 done_hooks | 自然退出 |

### 中断机制

三层中断信号：`stop_sig`（全局）+ `code_stop_signal`（代码执行）+ `consume_file(task_dir, '_stop')`（文件信号）

停止检查覆盖轮次开始、LLM 返回后和每个工具分发前。委派子 Agent 与团队 workflow 步骤共享父级 `stop_sig`，不得各自创建无法由父任务取消的独立停止域。

## 4. 关键设计决策

### ADR-1: ActionResult 统一封装

**背景**：工具返回值异构（dict/str/list/None），退出信号和 prompt 接续散落各处。`next_prompt` 承载了多种语义（完成/中断/内容/控制信号），字符串内容匹配触发控制流是隐式协议。

**决策**：所有工具返回 `ActionResult(data, next_prompt, should_exit, flags)`。

**理由**：统一退出信号和 prompt 接续机制，主循环只需处理一种数据结构。`flags` 将控制信号从 `next_prompt` 中显式剥离——原来"未知工具"字符串触发 `reset_tools` 是用内容匹配做控制流，违反契约精神。

**边界**：`data` 不做类型约束，由各工具自行保证序列化；`next_prompt=None` 即完成，不引入额外状态枚举；`flags` 为 `FrozenSet[str]`，合法值有限枚举（`reset_tools`、`retry`），不承载语义信息，语义信息仍由 `next_prompt` 承载。

### ADR-2: 历史由 Session 维护

**背景**：主循环需要感知历史长度吗？

**决策**：主循环通过协议无关的 `ContextManifest` 约束当前可见组件；Session 层负责
协议相关历史的 token 裁剪和 tool-result 配对修复。

**理由**：裁剪策略与模型协议强耦合（Claude content-block vs OpenAI message），放在主循环会导致协议泄漏。

**边界**：Session 对外暴露 `ask(prompt)` 和 `history`，主循环不直接操作 `history`。

### ADR-3: 每 10 轮重置工具描述

**背景**：工具 JSON 占用大量 token。

**决策**：`client.last_tools` 记录上次工具 JSON，相同时只发"工具库状态：持续有效"；每 10 轮强制重置。

**理由**：文本协议下工具描述在 prompt 中重复出现，省略可节省上下文窗口。

**边界**：仅适用于文本协议（ToolClient），原生协议由 API 处理工具描述。

### ADR-4: 生成器协议支持流式输出

**背景**：前端需要实时展示中间结果。

**决策**：`handler.dispatch` 返回生成器，主循环 `yield from` 展开。

**理由**：同步/异步统一，非流式场景用 `exhaust(g)` 消费即可。

**边界**：`dispatch` 内部对 `exec_*` 返回值用 `isinstance(result, GeneratorType)` 判断，避免误展开普通容器。

## 5. 目录结构

```
XAgent/
├── workspace/                  # Agent 默认工作区（未指定时自动创建）
├── src/
│   └── core/
│       ├── agent_loop.py       # 循环引擎 + BaseHandler + ActionResult
│       ├── llm.py              # LLM 接入层（Session 体系 + SSE 解析）
│       └── XAgent.py           # XAgent 入口（任务调度 + 前端桥接）
├── handler/                    # XAgentHandler（exec_* 工具实现）
├── tools/                      # 纯函数工具集（code_run / file_* / web_*）
├── assets/
│   ├── sys_prompt.txt          # 系统提示词
│   ├── tools_schema.json       # 工具定义
│   └── code_run_header.py      # 代码执行头文件
├── memory/
│   ├── global_mem.txt          # L2 长期记忆
│   ├── global_mem_insight.txt  # L0 记忆洞察
│   └── *_sop.md                # L3 SOP 知识库
├── frontends/                  # 多端适配器
├── plugins/                    # 插件（Langfuse 等）
├── reflect/                    # 自主运行脚本
├── docs/                       # 设计文档
└── temp/                       # 运行时临时文件
```

工作区约定：

- `workspace/` 是 Agent 的默认物理工作区；用户未显式指定时自动使用该目录，不存在则创建
- `ctx.cwd` 始终指向当前 Agent 工作区，所有相对路径默认基于该目录解析
- `memory/`、`assets/` 仍属于代码目录资源，不随工作区切换

### 模块边界

| 模块 | 对外接口 | 禁止 |
|---|---|---|
| `agent_loop` | `run_agent_loop()`, `ActionResult`, `BaseHandler`, `AgentContext` | 不依赖具体 Handler 实现 |
| `llm` | `Session.ask()`, `ToolClient.chat()`, `ChatResponse` | 不依赖工具定义 |
| `handler` | `XAgentHandler(BaseHandler)` 的 `exec_*` 方法 + `AgentContext` 状态管理 | 不直接调用 LLM |
| `tools/*` | 纯函数，返回 dict | 不依赖 Agent 状态 |
| `XAgent` | `run()`, `put_task()` | 编排层，不含业务逻辑 |

## 6. 约束与边界

### 默认单 Agent 路径不做的事

- **不做 ORM / 业务数据库**：记忆系统和业务工作区仍基于文件；仅 Durable
  Orchestration 控制面使用 Python 标准库 SQLite 保存编排元数据
- **不做插件热加载**：工具集编译期确定，运行期不动态增删
- **默认不要求分布式基础设施**：默认仍是单机进程，不引入消息中间件；可选远程面
  提供持久协议、授权/恢复边界和严格 HTTPS/ASGI transport，但不捆绑或声称已经验证
  ASGI TLS-extension server、SPIFFE/PKI、真实 gVisor 或异构 fleet 部署
- **不做无边界的通用 Agent 框架**：编排抽象只覆盖物理执行所需的确定性控制面
- **不做前端渲染引擎**：前端只负责消息展示，不做 Markdown/Rich 渲染

### 必须做的事

- 工具执行必须有超时保护
- 历史裁剪必须在 LLM 调用前完成
- 中断信号必须在每个循环轮次检查
- 文件操作必须做路径合法性校验
- 代码执行必须在子进程中隔离
- 未显式指定工作区时，必须默认落到代码目录下的 `workspace/` 并自动创建

### 容量边界

| 指标 | 限制 |
|---|---|
| 单次对话最大轮次 | 40（可配置） |
| 历史裁剪阈值 | `context_window_chars / 3` 的保守 token 预算，另按组件分配 |
| 单工具返回截断 | 20000 字符（file_read）/ 8000 字符（web） |
| 代码执行超时 | 60s（可配置） |
| 超长行截断 | `min(max(100, 256000//行数), 8000)` |
