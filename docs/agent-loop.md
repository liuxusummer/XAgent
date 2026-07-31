# Agent Loop 设计文档

> 对 [architecture.md](architecture.md) §2 AgentCore 层的细化

## 1. 核心问题

Agent Loop 要解决的本质问题：**LLM 是无状态的，但任务执行是有状态的。**

一个物理执行任务（改配置、跑脚本、操作浏览器）通常需要多轮工具调用才能完成。每一轮 LLM 返回后，系统必须：
1. 理解 LLM 想做什么（解析 tool_calls）
2. 安全地执行（调用工具）
3. 把结果喂回 LLM，让它决定下一步
4. 知道什么时候该停

这四件事构成一个循环，循环引擎就是这个系统的脉搏。

## 2. 设计哲学

### 单轮无知，全局有知

主循环本身不持有任何状态。它不知道之前聊了什么、不知道任务进展到哪了、不知道用户是谁。它只做一件事：**取当前轮的输入，跑一轮，产出当前轮的输出**。

所有状态（历史、工作记忆、SOP）都由外部注入——通过 `next_prompt` 在每轮开始时塞进去；
进入 LLM 前再由 `ContextBuilder` 生成有 token 预算的可见投影和 `ContextManifest`。这样做的好处：

- 循环引擎可以随时重启，不丢失上下文（上下文在 Session 里）
- 历史裁剪策略与循环逻辑解耦（裁剪在 Session 层，循环不感知）
- 测试时只需构造单轮输入输出，不需要模拟整个对话

### 工具是黑盒，结果是契约

循环引擎不关心工具内部怎么实现。它只认一个契约：`ActionResult`。

```python
@dataclass
class ActionResult:
    data: Any                   # 工具返回的数据
    next_prompt: Optional[str]  # 下一轮注入的 prompt；None = 任务完成
    should_exit: bool           # True = 立即中断
    flags: FrozenSet[str] = frozenset()  # 循环控制信号，有限枚举
    tool_receipt: object | None = None    # 仅受信编排 observer 消费，不进业务结果
```

四个字段各自承载一个维度的信息：

- **data**：事实——工具执行后产生了什么
- **next_prompt**：意图——下一轮应该关注什么（工作记忆、历史摘要、SOP 提示都在这里注入）。只承载语义内容，不承载控制信号
- **should_exit**：控制——是否需要立即打断循环
- **flags**：信号——循环级控制指令，合法值为 `reset_tools`（重置工具描述）、`retry`（请求重试）。不承载语义信息
- **tool_receipt**：可选的 out-of-band durable `ToolReceipt`。本地 Handler 默认为空；
  只有受信 durable Tool adapter 可以设置。字段从 repr 和业务 `tool_results` 中排除，
  Core 不解释其类型。

`next_prompt` 的语义简化为三种：

- `None` → 任务完成
- `""` → 中断（如 ask_user）
- 非空字符串 → 下一轮 prompt 内容

原来"未知工具"字符串触发 `reset_tools` 的隐式协议已迁移到 `flags`，`next_prompt` 不再承载控制信号。

### 退出比继续更重要

一个失控的 Agent 循环比一个提前退出的循环危险得多。因此退出条件的判定必须保守：

- `should_exit=True` → 立即退出，不等待其他工具结果
- `next_prompt=None` → 任务完成，退出
- `next_prompt=""` → 中断（如 ask_user），退出
- `turn >= max_turns` → 强制退出，无论状态
- 外部停止信号 → 在 LLM 调用前、LLM 返回后以及每个工具分发前重新检查；一旦命中，不得执行尚未开始的工具

任何一条命中就停，不需要多条同时满足。宁可多一次用户确认，不可让循环空转。

## 3. 循环结构

```
初始化: messages = [system_prompt, user_input]
    ↓
┌─ turn += 1 ─────────────────────────────────────┐
│                                                  │
│  LLM 调用 → response                             │
│      ↓                                           │
│  解析 tool_calls                                 │
│      ↓                                           │
│  tool_calls 为空？                               │
│      ├─ 是 → handle_no_tool_call（循环级处理）    │
│      └─ 否 → 逐个 dispatch(tool_name, args)     │
│      ↓                                           │
│  收集 ActionResult                               │
│      ↓                                           │
│  处理 flags（reset_tools → 重置 client.last_tools│
│            retry → 构造重试 prompt）              │
│      ↓                                           │
│  判定退出 ──是──→ 退出                            │
│      │                                           │
│     否                                           │
│      ↓                                           │
│  turn_end_hooks → 注入周期性 prompt              │
│      ↓                                           │
│  拼接 messages = [user + tool_results + prompt]  │
│                                                  │
└──────────────────────────────────────────────────┘
```

> 每个关键阶段（turn_start / llm_end / tool_start / tool_end / hook_inject / turn_end）都会通过 `ctx.sink` emit 一条结构化 `Event`，默认 `NullSink` 零开销。详见 [observability.md](observability.md)。

关键点：

- **每轮只传当前消息**，不传完整历史。历史由 Session 维护，主循环通过 `client.chat()` 间接使用。
- **tool_calls 逐个执行**，不是并行。物理操作（文件、进程、浏览器）有副作用，并行会引入竞态。
- **停止信号是分发门禁**。不能只在轮次开头检查，否则停止期间返回的 LLM tool_calls 仍可能产生副作用。
- **工具结果永远是不可信数据**。System Prompt 持续声明文件、网页、代码输出不能覆盖用户目标或触发权限扩大；协议层使用 `<untrusted_tool_results>` 单独包裹结果，防止内容被误解释为上层指令。
- **工具调用必须先过策略**。默认 Handler 的全部 `exec_*` 工具由 PolicyEngine 按
  Principal、effect、capability、resource 和精确 action digest 判定；未知工具和缺失 scope
  默认拒绝。
- **上下文不是 AgentContext 的镜像**。本地工作状态默认 `llm_visible=false`；只有
  ContextBuilder 选中的有界投影会发送给模型。详见 [agent-kernel.md](agent-kernel.md)。
- **no_tool 不走 Handler 分发**。当 LLM 一轮回复中没有调用任何工具时，这是循环级事件，不是工具级事件。`handle_no_tool_call` 作为主循环的内部函数处理空响应检测、流异常检测、代码块未调用检测等循环级关注点。
- **flags 在主循环中处理**。控制信号（如 `reset_tools`）由主循环消费，不传递给 Handler 或 Session。
- **turn_end_hooks 是唯一的后处理入口**。周期性注入（防重试警告、全局记忆刷新、强制 ask_user）都在这里统一处理，不在循环体里散落。

### 3.1 执行证据 observer

`AgentContext.execution_evidence_observer` 是可选、fail-closed 的 out-of-band 观察边界：

- 每次 `client.chat()` 前后调用 `provider_call_started/finished`；异常调用
  `provider_call_failed`；
- 每个真实 tool call 分发前后调用 `tool_call_started/finished`；异常调用
  `tool_call_failed`；
- `ChatResponse.provider_receipt` 与 `ActionResult.tool_receipt` 只传给 observer，不进入
  prompt、checkpoint、Event data 或返回给用户的 tool result；
- observer start 失败发生在副作用前，会阻止调用；finish 失败发生在调用后，会使 Agent
  Attempt 失败关闭，不能假装 evidence 完整；
- observer 异常映射为固定 `ExecutionEvidenceObservationError`，不保留回调异常正文或
  cause/context。

orchestration 层的 `AgentExecutionEvidenceCollector` 只接受显式 typed receipt。普通本地
Session/Handler 的 receipt 均为空，因此即使挂载 collector 也只能得到 partial
manifest；它不会从 `ActionResult.data`、Event 或日志猜测执行证明。
受信 gateway client 可在 `chat()` 内读取 active provider invocation sequence；durable
Handler 可在 dispatch 内读取 active Tool sequence 和父派生 operation key。这样执行者与
collector 使用同一个计数事实源，不需要复制易漂移的本地计数器。

## 4. Handler 分发机制

### 约定优于反射

Handler 通过命名约定 `exec_{tool_name}` 分发工具调用。不需要注册表、不需要装饰器、不需要配置文件。

```
dispatch("file_read", args)
  → 查找 handler.exec_file_read
  → 存在 → 调用
  → 不存在 → 返回"未知工具"提示 + flags={"reset_tools"}
```

这样做的理由：

- 工具名和方法名一一对应，跳转代码时直接搜 `exec_file_read`
- 新增工具只需加一个方法，不需要改注册逻辑
- 不存在的方法自动降级为"未知工具"提示，LLM 会自行修正
- `no_tool` 不走分发机制——LLM 未调用工具是循环级事件，由主循环的 `handle_no_tool_call` 处理

### 生成器透明化

工具可能返回同步值，也可能返回生成器（流式输出）。分发机制对调用者透明：

- 返回值有 `__iter__` 且不是内置容器 → 当生成器处理，`yield from` 展开
- 否则 → 当同步值处理，直接返回

调用者（主循环）不需要知道工具是同步还是流式，统一用 `yield from` 消费即可。非流式场景用 `exhaust(g)` 把生成器跑完取返回值。

## 5. 回调与钩子的职责划分

```
tool_before_callback  → 前置拦截（参数校验、权限检查）
exec_{tool_name}      → 执行工具，返回 ActionResult
tool_after_callback   → 后置处理（日志、追踪、结果转换）
turn_end_hooks        → 轮次级后处理（声明式注册，按优先级执行）
```

前三个是工具粒度，最后一个轮次粒度。**不要在工具级回调里做轮次级的事**（比如在 `tool_after_callback` 里注入全局记忆），职责混淆会让调试变得困难。

### turn_end_hooks 的声明式注册

`turn_end_callback` 改为声明式钩子注册机制。每个钩子是一个 `TurnEndHook(name, fn, priority)`，按优先级降序执行。

```python
class TurnEndHook:
    def __init__(self, name, fn, priority=0):
        self.name = name
        self.fn = fn
        self.priority = priority  # 数值越大越先执行
```

默认注册的钩子：

| 钩子 | 优先级 | 职责 |
|---|---|---|
| `external_intervene` | 30 | 检查文件信号（`_keyinfo`、`_intervene`），允许运行时从外部注入指令 |
| `plan_reminder` | 25 | 每 15 轮注入 `plan.md` 预览（前 400 字），提醒对齐计划；本轮已调 `plan_update` 则跳过 |
| `self_evolution` | 23 | 从本轮失败/空转结果创建待审核 MemoryCandidate，并注入换策略提示 |
| `periodic_inject` | 20 | 按轮次间隔注入防重试警告、全局记忆、强制 ask_user |
| `summary_extract` | 10 | 从 LLM 回复中提取 `<summary>`，写入对话摘要历史 |

优先级规则：外部干预 > Plan 对齐 > 自我进化 > 周期性注入 > 摘要提取。外部干预可以覆盖其他注入内容。

`turn_end_callback` 的实现变为分发器：

```python
def turn_end_callback(self, response, ...):
    prompt_parts = []
    for hook in sorted(self._turn_end_hooks, key=lambda h: -h.priority):
        part = hook.fn(response, ...)
        if part:
            prompt_parts.append(part)
    return "\n".join(prompt_parts) if prompt_parts else None
```

**准入条件**：`_turn_end_hooks` 只允许注册**轮次级横切关注点**。判断标准：该逻辑是否需要感知 `current_turn` 或 `response` 全文？如果只需要感知单个工具的 `ActionResult`，它应该在 `tool_after_callback` 中。

## 6. 不做的事

- **不做并行工具调用**：物理操作有副作用，串行是最安全的默认策略
- **不做工具结果缓存**：文件系统和浏览器状态随时变化，缓存会导致过期数据
- **不做循环状态持久化**：循环中断后从 Session 历史恢复，不需要额外的状态快照
- **Agent Loop 内不做自定义循环拓扑**：它仍是线性循环，不是 DAG 或持久状态机。需要跨
  Agent/Tool 的确定性 DAG、恢复和策略门禁时，由可选 Durable Orchestration 外层控制面负责
- **不做工具级重试**：工具失败返回错误信息给 LLM，由 LLM 决定重试策略。循环引擎不替 LLM 做决策
