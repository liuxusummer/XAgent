# 工具执行层设计文档

> 对 [architecture.md](architecture.md) §2 Tool Layer 的细化

## 1. 核心问题

工具执行层要解决的本质问题：**LLM 只有语言能力，但用户要的是物理世界的操作结果。**

LLM 不能读写文件、不能跑代码、不能操作浏览器。它只能"说"要做什么。工具层就是 LLM 的手——把意图变成动作，把结果变成 LLM 能理解的信息。

这层要回答三个问题：
1. LLM 想做什么？（从 tool_calls 解析意图）
2. 怎么安全地做？（隔离执行、超时保护、路径校验）
3. 做完之后告诉 LLM 什么？（结果截断、格式化、注入提示）

## 2. 设计哲学

### 工具是无状态的纯函数，Handler 是有状态的编排器

每个工具接收参数，返回 dict。不持有状态，不依赖 Agent 上下文，不直接调用 LLM。

状态（工作记忆、对话摘要、SOP 索引）由 Handler 通过 `AgentContext` 管理，工具不管。工具只做一件事：**给输入，出输出**。

Handler 具有双重身份：
1. **分发器**：通过 `exec_{tool_name}` 命名约定分发工具调用
2. **上下文管理器**：通过 `AgentContext` 持有和管理所有可变状态

`AgentContext` 将 Handler 的散落状态集中为一个数据类：

```python
@dataclass
class AgentContext:
    working: dict = field(default_factory=lambda: {"key_info": "", "related_sop": ""})
    cwd: str = ""
    memory_root: str = ""
    current_turn: int = 0
    history_info: list = field(default_factory=list)
    code_stop_signal: bool = False
    done_hooks: list = field(default_factory=list)
    empty_count: int = 0  # 连续空响应计数（原 no_tool 逻辑使用）
```

这样做的好处：
- 工具可以独立测试，不需要模拟整个 Agent
- 工具之间没有隐式依赖，不会因为状态泄漏产生 bug
- 新增工具只需写一个函数 + 一条 Schema，不触碰其他代码
- `AgentContext` 是状态的唯一入口，grep `self.ctx` 即可定位所有状态访问
- `ctx` 只在 Handler 的 `exec_*` 方法中使用，不传入 `tools/` 下的纯函数

Durable Orchestration 的参考 Handler 是一个额外的消费边界：它不直接调用上述本地工具，
而是把 collector 当前 observation 交给 deployment-owned executor，只接受已经写入 Store
的 `ToolReceipt` 与 canonical `AgentToolResult` Artifact。成功结果的控制 envelope 必须由
受信 adapter 生成，sandbox/stdout 仍是不可信 `data`。动态 child authority 由独立的
`DurableAgentToolExecutor` ledger 签发，而不是由 Handler 或静态 Workflow Node 冒充；
reference executor 仍依赖部署侧隔离与认证，不能单独声明 production-ready；见
[Durable Agent Tool Handler 契约](agent-tool-handler.md)和
[动态 Agent Tool 执行器契约](agent-tool-executor.md)。

### 执行在隔离区，结果要截断

代码执行和浏览器操作都有不可控的一面。隔离原则：

- **代码执行**：先生成并验证不可变隔离计划，再启动独立进程组；超时、取消或输出超限均终止整个进程组
- **浏览器操作**：独立 WebDriver session，不共享主进程的浏览器状态；导航只允许解析到公网地址的 HTTP(S) URL
- **文件操作**：路径必须转换为绝对路径；相对路径一律基于 `ctx.cwd`（当前 Agent 工作区）解析。工作区外读取需要本地 operator 专用的 `host.read`，并默认逐次确认；安全 Web 注册表不能授予该 scope
- **稳定读取**：普通读取、文件引用展开及读改写阶段均通过逐路径组件的
  descriptor-relative no-follow 原语完成；读取对象必须在开始/结束时保持相同
  device/inode/size/mtime/ctime，单文件原始读取上限为 20MB。能力缺失、符号链接替换或
  并发改写均 fail closed，不退回 `Path.read_text()`
- **安全变更**：文件写入使用已授权父目录描述符内的临时文件与原子 rename；删除使用
  descriptor-relative unlink/rmdir，递归删除只进入 no-follow 打开的真实目录。
  写锁文件也通过相同边界安全打开。父目录被并发替换、目标为 symlink 或平台缺少
  安全原语时均 fail closed

`code_run` 默认要求真实 OS 隔离，独立策略门禁不能替代隔离：

- 默认 `XAGENT_CODE_RUN_POLICY=confirm`，每次执行前向用户展示脚本摘要、哈希、语言和超时并等待明确授权
- `deny` 完全禁用代码执行
- 默认 `XAGENT_CODE_RUN_BACKEND=auto`：Linux 只接受受信系统路径中的
  Bubblewrap，macOS 只接受系统 `sandbox-exec`；启动前必须通过读、写、控制面、
  loopback 网络和私有临时目录的功能探测，失败即拒绝执行，不回退到宿主进程
- 审批 digest 绑定实际脚本哈希、语言、超时、工作区规范路径摘要、后端二进制
  identity、probe、资源限制和安全模式；隔离计划另行捕获工作区目录的
  device/inode identity，并在进程启动前重新校验，任一边界变化都拒绝启动
- 安全模式只读挂载业务工作区，隐藏 `system/`、`runtime/`、`memory/` 以及
  `_intervene`、`_keyinfo`、`plan.md`，只允许写入私有临时目录，禁止网络，并限制
  CPU、地址空间、文件大小、打开文件数、进程数；私有临时目录的总字节数和目录项数
  由 sandbox 外的父进程监督，超限即终止整个进程组
- `XAGENT_CODE_RUN_BACKEND=unsafe` 仅是显式开发兼容模式，安全回执固定标记
  `development_unsafe`；即使策略配置为 `allow`，每次调用仍需用户单独确认
- 无论模式为何，子进程只继承最小运行环境；API key、代理、SSH agent 等宿主敏感变量不得透传

工作区约定：

- `ctx.cwd` 表示 Agent 当前工作区，不再表示代码目录
- Agent 启动时若用户未指定工作区，默认使用 `<code_root>/workspace`，不存在则自动创建
- `memory_root` 指向当前工作区根目录，Memory Provider 负责在其下解析 `system/memory/` 与 `system/agents/<agent>/MEMORY.md`

结果截断不是可选项，是必需品。一个 10000 行的文件读出来全塞给 LLM 会撑爆上下文。截断策略按工具域不同：

- 文件类：20000 字符硬上限
- 浏览器类：8000 字符硬上限
- 代码执行：stdout、stderr 各保留最多 200000 字符；任一流超限立即终止进程组并返回
  `OUTPUT_LIMIT_EXCEEDED`

文件与浏览器结果截断时保留首尾；流式代码输出只保留最先到达的上限内容，并通过
`stdout_truncated` / `stderr_truncated` 明确标记终止原因。

### 失败要可诊断，不要静默吞错

工具失败时，返回给 LLM 的信息必须足够它自行诊断和重试：

- 文件不存在 → 返回错误 + 模糊匹配的候选路径
- patch 匹配 0 次 → 返回错误 + 建议"先 file_read 确认内容"
- patch 匹配多次 → 返回错误 + 建议加长上下文使其唯一
- 代码执行超时 → 返回已执行的 stdout + 超时标记

不要替 LLM 做决策（比如自动重试、自动换路径），把诊断信息给它，让它自己决定下一步。

### 内容提取有优先级，有兜底

LLM 生成的内容不是总能干净地放进参数里。比如 `file_write` 的内容可能很长，LLM 倾向于在回复正文中用标签包裹，而不是塞进 JSON 参数。

提取优先级：

1. **显式标签**：`<file_content>...</file_content>` — 最可靠，结构明确
2. **代码块**：` ```...``` ` — 兜底方案，当 LLM 没用标签时
3. **参数字段**：`args.script` / `args.content` — 短内容时 LLM 可能直接放参数

同样的，`code_run` 的代码提取：`args.script` → 回复代码块，两者互斥，优先从参数取。

## 3. 工具域划分

```
工具层
├── 代码执行域
│   └── code_run          # 子进程执行代码，支持 python / shell
├── 文件操作域
│   ├── file_read         # 读文件（行范围 / 关键词搜索）
│   ├── file_search       # 基于 workspace 文件索引做路径/全文检索
│   ├── file_patch        # 精确替换（唯一性校验）
│   └── file_write        # 写文件（覆盖 / 追加 / 前插）
├── 浏览器操作域
│   ├── web_scan          # 扫描页面（简化 HTML / 纯文本）
│   └── web_execute_js    # 注入 JS 执行
├── 记忆管理域
│   ├── update_working_checkpoint  # 短期工作记忆
│   ├── start_long_term_update     # 读取结算 SOP，不直接写长期记忆
│   ├── memory_propose             # 创建隔离的待审核 MemoryCandidate
│   └── plan_update                # 读写 workspace/plan.md（读：不传 content；写：传 content 覆盖）
└── 交互域
    └── ask_user          # 中断循环，等待用户输入；可附带选项
```

### 域间关系

- 代码执行域和文件操作域是独立的，互不依赖
- 文件索引检索域由 `file_search` 暴露，索引文件落在当前 workspace 的 `runtime/file_index.sqlite3`，属于运行时元数据；受管 Memory 路径永不进入通用索引；工具只返回候选路径和短片段，依赖精确内容或修改文件前仍必须使用 `file_read` 验证
- 浏览器操作域依赖 `BrowserDriver` 接口，默认使用 Selenium fallback；工具只暴露 `web_scan` / `web_execute_js`，底层可替换为 WebSocket / HTTP Long-Poll 浏览器桥
- 记忆管理域只允许工作状态直接更新；长期记忆必须经过
  MemoryCandidate → review → MemoryRecord，普通文件工具不能作为旁路
- 交互域（ask_user）是特殊的——它不执行任何物理操作，只产生中断信号；可提供 `options` 供用户选择，未提供选项时用户自由输入；每个 Agent 使用实例级 display/reply bridge，不共享进程级队列
- Plan 模式、外部干预文件（如 `_keyinfo`、`_intervene`）与相对路径文件操作统一落在工作区内；
  这些根级控制文件只能由专用控制通道访问，普通文件工具读写均 fail closed

### 浏览器操作闭环

浏览器被抽象成 Agent 的远程可观测、可执行环境：

```
web_scan → BrowserDriver.scan → 压缩页面快照 + tab sessions
web_execute_js → BrowserDriver.execute_js → exec_id + ACK/结果诊断
```

关键契约：

- `web_scan` 返回 `sessions`、`current_session_id`、`page`，tab 以稳定 `session_id` 寻址；`tab_index` 仅保留兼容
- `web_execute_js` 返回 `exec_id`、`ack`、`result_received`、`diagnostics`，用于区分送达、执行、导航和失败状态
- 每个 Agent/Handler 延迟创建并独占一个 BrowserDriver；Agent 关闭时同步释放，禁止进程级共享浏览器实例
- 同一 BrowserDriver 内的扫描、标签切换、导航和脚本执行必须串行；`web_execute_js.timeout` 由 WebDriver 原生脚本超时强制执行
- 浏览器的导航、页面脚本网络请求、iframe 与 WebSocket 统一经过本地过滤代理；代理在每次实际连接时解析目标、拒绝任一非公网地址，并直接连接该次校验得到的 IP，避免 DNS rebinding 的校验—使用间隙
- 导航 URL 仅允许无凭证的 HTTP/HTTPS；页面加载默认 30 秒且最多 120 秒
- Chrome 沙箱默认启用；只有受控部署显式设置 `XAGENT_CHROME_NO_SANDBOX=1` 时才添加 `--no-sandbox`
- 长结果通过 `save_to_file` 落盘，tool_result 只返回路径、字节数和摘要；该参数额外要求
  `workspace.write`，且只能写业务路径，不能写 `system/`、`runtime/` 或受管 `memory/`
- 默认扫描模式为 `summary`，只返回语义压缩内容；需要精确状态时应执行局部 JS 查询

### 共享工具

- `truncate_text(content, limit)`：工具输出的 head+tail 截断，marker 为 `[truncated]`，被 `file_read` / `web_scan` / `web_execute_js` 复用
- 注：`src/core/llm.py::_truncate_inner` 用于历史**压缩**（marker `[compressed]`），语义不同，不共用

## 4. 关键机制

### 4.1 代码执行的隔离模型

```
LLM 输出代码
  → 提取代码（script 参数 / 回复代码块）
  → 注入 code_run_header.py（公共 import）
  → 功能探测并生成不可变 CodeExecutionPlan（失败即停止）
  → PolicyEngine + code_run 策略检查（默认逐次用户确认）
  → 再验证脚本 / 工作区 / 后端 identity 与审批计划完全一致
  → Bubblewrap / sandbox-exec 启动资源限制 launcher 和 payload
  → stream_reader 线程流式读取 stdout
  → 超时 / 停止信号 → 独立进程组 TERM → 有界等待 → KILL → wait
  → 返回 {status, stdout, exit_code, security receipt}
```

关键约束：

- **不用 eval/exec**：除非 `inline_eval` 显式开启（仅内部使用），一律走子进程
- **默认双重失败关闭**：未设置策略时按 `confirm`，未设置后端时按 `auto`；无明确授权或
  无通过功能探测的 OS 隔离后端都不启动 payload
- **底层 API 同样 fail closed**：`run_code` / `run_code_stream` 必须由完成策略检查的 Handler 显式传入内部授权标记
- **宿主环境最小化**：不向脚本透传模型密钥、代理、云凭证或 SSH agent 环境
- **流式读取**：stdout 不等进程结束才返回，边读边 yield，前端可实时展示
- **有界保留**：stdout/stderr 达到硬上限后终止整个进程组，避免无限输出继续消耗 CPU
- **双信号停止**：`code_stop_signal`（Handler 级）+ `stop_sig`（全局级），每个循环轮次检查
- **持久写入分离**：安全代码执行看到只读工作区；需要持久修改时必须使用受策略约束的文件工具
- **后代收口**：payload 使用独立进程组；即使组长先正常退出，也会清理其余后代
- **macOS 限制**：当前 Seatbelt profile 禁止 fork；复杂 shell 任务可能失败，应改用 Python
  或部署 Linux Bubblewrap/生产容器后端，不能因此切换到 unsafe

Durable Orchestration 的 `TrustedActivityExecutor` 在 approval/policy 提交前
还会计算完整 execution binding：非秘密 argv 摘要、规范化 cwd 摘要、
profile/limits、输入 Artifact identity、稳定 operation/idempotency key 摘要、
EnvironmentBinding 的受信摘要、capabilities 和 resource locks 都进入
`ActionRequest` 的 action digest。脚本不能只提交 caller 声称的 digest：
必须提供通过 `artifact_verifier` 校验的 immutable `ArtifactRef`，由受信
`artifact_reader` 物化为不可变 bytes；`ExecutionRequest` 和 Dispatcher 会再次
校验 bytes 的长度与 SHA-256，且只有显式声明支持该物化契约的 backend 才可接收。
会执行脚本或动态代码的工具还必须在 `ToolPolicy` 中声明
`requires_script_artifact=true`；此时仅把可变路径放进 argv 而不提交
ArtifactRef 会在 `attempt.started` 前被拒绝。固定受信函数或 immutable image
命令可以显式声明不需要脚本 Artifact。
审批签发后替换 argv、Artifact、脚本 bytes、key、资源限制、工作目录或 profile
都会在 backend 执行前失败。
argv 明确禁止承载秘密；执行请求只允许携带环境变量名及受信边界生成的
redaction/HMAC digest，不携带秘密值。需要秘密的部署必须由后端侧可信 broker
按该 `EnvironmentBinding` 解析，且不得把解析值写回 Domain Event 或 Receipt。
敏感键识别复用元数据边界的 NFKC、casefold 和分隔符移除规则，避免
`accessToken`、全角 `PASSWORD`、`api.key` 或零宽字符绕过。Executor 会针对
`sensitive_keys` 和内置敏感字段名提取非 null、非空的 JSON scalar（包括
numeric/bool；兼容受信边界的 bytes-like 值），并在 policy 之前拒绝这些值出现
在 argv、cwd 或 resource locks 中；该检查不做通用熵猜测，也不会在错误、
digest 或 SQLite 中回显被拒绝值或其可离线枚举的直接 hash。

稳定 operation/idempotency key 在 Scheduler、Policy 和 Sandbox 共享 1024 字符
硬上限。渲染结果超限时不会创建 Attempt；合法上限内的 key 可完整交给 backend，
并作为恢复/probe 所需的内部 durable identity 保存在 Attempt projection 和
idempotency state。模板只能引用编译器 allowlist 中的 workflow/run/node 固定字段
及 input digest，禁止把 key 当作秘密传输通道。Policy/approval/receipt 公共
payload 只携带 key digest，Web/telemetry 不导出原值。

当 policy 返回 `REQUIRE_APPROVAL` 时，Executor 不会把它当作终态失败：Store 原子释放
claim lease，把 Run/Node/同一 Attempt 置为 `WAITING_APPROVAL`，并持久化有界的
`approval.requested`。可信 grant 将同一 Attempt 恢复为 `SCHEDULED`，下次 claim
获得更高 fencing token；可信 rejection 或 durable cancel 在 backend 调用前终止。
远程 preclaim 路径更严格：首次 poll 在同一 Store 事务中只执行
`attempt.scheduled` → `WAITING_APPROVAL` → `approval.requested`，不创建
idempotency owner/lease、不写 `attempt.claimed`、不签发 WorkerAuthorization 或
Artifact grant，未领取过的 Attempt 此时 fencing token 为 0。可信 grant 仍恢复同一
Attempt 为 `SCHEDULED`；下一次 poll 必须重新完成精确授权，并在 claim/policy 的同一
事务中校验、过期判断和单次消费 grant。
终态工具结果通过 `DurableRunStore.get_tool_receipt(run_id, attempt_id)` 查询时会重新
验证 Receipt digest、Run/Attempt identity 和 Event 状态，禁止把任意 result dict
冒充已验证 Receipt。

编排可写工具的 `ToolPolicy` 必须显式声明 idempotency-key、status-probe、
compensation、timeout behavior，以及 allowed/required resource keys；
动态代码工具另需声明 `requires_script_artifact`。Policy
会拒绝缺少业务幂等键能力的 `idempotent_write`、写操作的 `safe_to_retry`
超时声明、无 probe 能力却声称 `probe_before_retry`，以及缺失或越权的资源锁。
原始 operation key 仅在上述内部 durable state 与受信 backend 之间流转。

`LocalProcessSupervisorBackend` 仅证明进程树监管和有界输出，安全等级为
`development_unsafe`，不继承宿主环境且只接受声明为 read-only 的 Action；
它不是任意代码隔离。闭集受信函数使用独立的 `trusted_function` 等级，
默认 `os_sandbox` profile 不会接受这两种较弱 attestation。
运行中的本地进程由活着的 worker 周期性读取 DurableRunStore 的 Run 取消意图；
发现 `CANCELLING` / `CANCELLED` 后必须对整个进程组执行
TERM→有界 grace→KILL→reap，并由 Executor 确认 durable CANCELLED 终态。
该协作模型不声称 controller/worker 崩溃后能重新发现或 attach 已失去监管的本地
进程；需要此能力的部署必须使用保存外部进程身份并支持重新 attach 的 supervisor。
`Popen` 成功后，selector 注册、读取或监督逻辑的任意异常都必须走同一
TERM→有界等待→KILL→wait 进程组清理，并在异常返回前关闭 selector 与
stdout/stderr；初始化失败不能绕过 parent/child/grandchild 的回收契约。

远程参考执行不会把 `cwd`、Artifact 文件路径或控制面根发给 worker。可信控制面先把
请求编译为容器内路径的 execution plan，再签发与 tenant、worker、session、Run、
Attempt、Action 和 authorization digest 绑定的 Artifact grant；worker 只回传有界
output handle 与签名 runtime proof，最终 ArtifactRef 和 ToolReceipt 仍由控制面校验
后提交。参考实现的 prepared-execution/staged-output registry 是进程内状态，OCI
attestation 与 HMAC proof 只演示验证链，不等同于真实 mTLS 或 gVisor 隔离。生产部署
必须提供独立传输、持久 broker 状态和真实隔离 backend；详见
`distributed-execution-adr.md`。
当前这条可信执行与 ToolReceipt 链只证明 `tool` Activity。控制面 Admitter 和 Worker
adapter 都通过不可变 `supported_activity_kinds` 声明真实能力，注册、claim 和 Worker
start 前逐层验证；协议存在 `agent` 枚举或调用方声明 `activity.agent` 都不能扩大能力。
完整 Agent Loop 的远程执行必须另行证明整体副作用、上下文 Artifact 和 Agent 级回执，
不能把内部单次 ToolReceipt 冒充为整个 Agent Activity 的结果。
`AgentActivityExecutionManifest` 已提供工具 lineage 的前置契约：父 Agent 的稳定身份与
request digest 为每个有序 child Tool Activity 派生 operation/idempotency key，并把
精确 ToolReceipt digest 绑定进 v2 Agent receipt。该 manifest 仍不证明 provider 调用
或 sandbox egress 已受控，因此生产 reference adapter 继续保持 Tool-only。

进程内 Artifact broker 除单 Artifact 上限外，还对所有 staged/finalizing 内容实施
全局驻留字节硬上限；超限的新增 stage 必须 fail closed，不能淘汰已接受的旧 stage。
内容进入 finalizing 后仍持续计费，直到 Store 写入与校验在锁外返回，再由锁内的幂等
accounting 释放。已观察过期或进入 failed/consumed 的 write grant 是不可逆终态，
时钟回拨不得恢复；过期时仍在 Store I/O 中的本地内容必须保留计费，待 I/O 返回后
再释放和清理，防止并发 finalize/expiry 造成预算漏记或重复释放。

Executor 会从 Store 文件、已绑定 ArtifactStore 和 backend ArtifactStore 推导控制面
根，并在构造执行请求前解析 profile allowed roots 与 cwd；任何相等、祖先或后代重叠
都会抛出组合错误且不调用 backend。该检查用于防止把控制面误挂进 Agent sandbox，
不能替代不同 UID/service、mount namespace 或容器隔离。

无状态 MCP stdio 入口对单条消息同时施加 1 MiB、32 层和 4096 个容器 item
的上限。JSON 解码的 `RecursionError`/畸形输入返回固定 Parse error；已解码但
超过结构上限的请求返回固定 Invalid Request。两类拒绝都发生在限流和 runtime
authorization 之前，不回显 payload，并且只丢弃当前 newline-delimited 消息，
后续合法请求必须继续处理。

### 4.2 file_patch 的唯一性契约

patch 是最危险的操作——改错一行可能破坏整个文件。唯一性校验是安全网：

- `old_content` 在文件中必须恰好匹配 1 次
- 0 次 → 内容已变，建议先 file_read 确认当前内容
- \>1 次 → 上下文不够，建议加长 old_content 使其唯一

这比"全文搜索替换第一个匹配"安全得多。LLM 看到错误信息后会主动 file_read 再 patch，形成探测-确认-修改的安全闭环。

### 4.3 文件索引检索

`file_search` 用于在当前 workspace 内快速定位文件名、路径片段、符号或文本片段。基础层使用 Python 标准库 `sqlite3` 的 FTS5 能力；可选二层语义检索使用 chunk 级 embedding + SQLite 存储增强召回，不替代 FTS5。

关键契约：

- 索引文件位于 `<ctx.cwd>/runtime/file_index.sqlite3`，`runtime/` 视为 workspace 运行时元数据，只读可观测、不可由 Agent 文件/浏览器工具写入
- 首次搜索或 `refresh=true` 时扫描并增量更新索引；增量依据 `relative_path + mtime_ns + size`
- 只索引 UTF-8 文本文件；默认单文件上限为 5 MiB，并跳过 symlink、二进制、常见
  依赖/构建目录、`runtime/**`、`memory/**`、`system/memory/**` 和 Agent `MEMORY.md`
- 索引 schema v4 打开旧 schema 时会先清空旧内容并重新扫描，防止升级排除规则后继续
  返回历史受管 Memory 条目
- `root` 必须位于当前 workspace 内，避免检索工作区外路径
- `mode=keyword|semantic|hybrid` 控制检索模式；`path_only=true` 强制只走路径/关键词检索
- 语义检索默认关闭，仅在 `file_index_embedding.enabled=true` 或 `XAGENT_FILE_INDEX_EMBEDDING=1` 且 embedding 配置完整时启用
- 语义索引数据同库保存为 `file_index_chunks`、`file_index_chunk_embeddings`，可用 `sqlite-vec` 时额外维护 `file_index_vec`；缺少依赖或 embedding 失败时降级为 FTS5 并返回诊断状态
- 返回结果只作为候选，修改前必须继续 `file_read` 精读目标文件

### 4.4 文件引用展开

`{{file:path:startLine:endLine}}` 语法允许工具参数引用文件内容，而不是把大段文本塞进 JSON。

这在 file_patch 和 file_write 中使用：LLM 可以写 `new_content = "前缀\n{{file:config.yaml:10:20}}\n后缀"`，运行时展开为文件第 10-20 行的实际内容。

设计意图：减少 JSON 参数中的文本量，降低 LLM 生成 JSON 时的截断风险。

其中 `path` 若为相对路径，同样以 `ctx.cwd` 为基准解析。

### 4.5 工作记忆的轮次注入

`update_working_checkpoint` 更新的 `key_info` 不是存在就完了——它每轮通过 `get_anchor_prompt` 注入到 prompt 中。

这意味着：
- LLM 每轮都能看到当前工作记忆，不需要从历史中翻找
- 工作记忆是增量更新的：LLM 先 review 现有内容，再决定 keep / add / remove

### 4.6 无工具调用处理（handle_no_tool_call）

当 LLM 一轮回复中没有调用任何工具时，这不是工具级事件，而是循环级事件。处理逻辑位于主循环的 `handle_no_tool_call` 函数中，不走 Handler 分发。

```
LLM 回复无 tool_calls
  → handle_no_tool_call(handler, response)
  → 1. 空响应检测：连续 3 次空回复 → 退出
  → 2. 流异常检测：末尾含错误标记或 max_tokens 截断 → 返回重试 prompt
  → 3. 代码块未调用检测：回复中有大代码块但没调 code_run → 提醒 LLM
  → 4. 正常结束：LLM 给出了最终答案 → next_prompt=None，退出
```

跨轮次状态（`empty_count`）存储在 `AgentContext` 中，主循环通过 `handler.ctx` 读取，主循环本身不持有状态。

这是最后一道防线，防止 LLM "以为自己做完了但实际没执行"的情况。

## 5. 不做的事

- **Tool Layer 内不做工具编排**：普通工具调用顺序仍由 LLM 决定；显式启用 Durable
  Orchestration 时，外层控制面只通过受策略约束的 Tool Activity adapter 调度 DAG
- **不做工具结果缓存**：文件和浏览器状态随时变化，缓存会导致过期数据
- **不做自动重试**：工具失败返回错误信息，由 LLM 决定是否重试、换参数还是换方案
- **不做通用 RBAC**：工具可见性由 Agent allowlist 控制；`code_run` 等高风险能力仍可有独立的执行策略门禁
- **不做浏览器自动化框架**：只提供 scan + execute_js 两个原语，不封装点击/填写等高级操作
- **不做文件变更回滚**：patch/write 是不可逆的，依赖 LLM 的探测优先策略避免误操作

## 6. 双历史体系对齐

系统存在两套历史：

- **Session.history**（原始层）：完整的消息序列，由 Session 管理，负责裁剪。与模型协议强耦合
- **AgentContext.history_info**（摘要层）：对话摘要序列，由 Handler 管理，通过 `get_anchor_prompt` 注入 prompt。与模型协议无关

两层历史的本质差异：一个是传输格式，一个是语义内容。

### 对齐机制

摘要层可能引用已被原始层裁剪删除的内容，导致 LLM 基于过时信息做决策。对齐检查在 `turn_end_callback` 的 `summary_extract` 钩子中执行：

```python
def align_history_info(self):
    max_entries = len(self.parent.client.backend.history) * 2
    if len(self.ctx.history_info) > max_entries:
        self.ctx.history_info = self.ctx.history_info[-max_entries:]
```

在 `get_anchor_prompt` 中标注裁剪边界：

```python
if len(self.ctx.history_info) > 30:
    earlier_str = f"<earlier_context>\n[...前 {len(earlier)} 条摘要已折叠]\n"
```

关键约束：对齐只在 Handler 层发生，不触碰 Session.history。上层通过 `ask(prompt)` 传入的内容不会被裁剪——只有历史中的旧消息会被动刀。
