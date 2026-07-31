# LLM 接入层设计文档

> 对 [architecture.md](architecture.md) §2 LLM Layer 的细化

## 1. 核心问题

LLM 接入层要解决的本质问题：**模型协议是多样的，但上层只想要一种接口。**

Claude 和 OpenAI 用不同的消息格式、不同的工具调用机制、不同的流式协议。Agent 循环引擎不应该关心这些差异——它只需要一个 `chat(messages, tools) → response` 的调用，拿到统一的 `ChatResponse`。

同时，这一层还要独自承担两件上层不该操心的事：**历史裁剪**和**故障转移**。

## 2. 设计哲学

### 协议差异内聚，接口统一外露

所有模型协议的脏活累活（SSE 解析、消息格式转换、工具字段映射）都封在这一层内部。对外只暴露一个统一的响应结构 `ChatResponse`：

```
ChatResponse
├── thinking    # 思维链内容（可能为空）
├── content     # 文本回复
├── tool_calls  # 工具调用列表（可能为空）
├── stop_reason # 停止原因
└── usage       # provider 返回的真实 token 用量（可能为空）
```

无论底层是 Claude content-block 还是 OpenAI delta，上层拿到的都是同一个形状。新增模型支持时，只需要实现 Session 的协议转换逻辑，不触碰上层代码。

`usage` 只保存归一化后的元数据：输入、输出、总量、缓存创建、缓存读取和 reasoning token。provider 未返回 usage 时保持为空，不用字符数估算。

### 两种工具协议，两种策略

工具调用有两种完全不同的实现方式：

**文本协议**（ToolClient）：工具描述和调用都通过 prompt 文本注入。LLM 在回复中输出 `<tool_use>` 标签，由客户端解析。这是对不支持原生 tool calling 的模型的兼容方案。

**原生协议**（NativeToolClient）：工具描述走 API 的 `tools` 字段，调用结果走 `tool_result` block。模型原生理解工具语义，解析更可靠。

两者不混用。一个 Session 要么走文本协议，要么走原生协议，由配置决定。ToolClient 和 NativeToolClient 是平行的包装层，不共享状态。

### 历史是负担，裁剪是生存

长对话中历史会无限膨胀，直到撑爆上下文窗口。历史裁剪不是优化，是生存必需。

裁剪策略的核心矛盾：**裁太少会超限，裁太多会丢上下文**。解决思路是分级裁剪：

1. **先压缩**：截断历史中的长内容（thinking、tool_use、tool_result），保留结构
2. **再删除**：从头部删除整条消息，直到总量低于阈值
3. **后修复**：删除消息后可能产生孤立的 tool_result 引用，必须改写为纯文本

裁剪在每次 `ask()` 调用前自动执行，上层无感知。

### 故障是常态，转移要无声

API 调用会失败——限流、超时、服务端错误。重试是基本操作，但单节点重试有上限。MixinSession 的设计思路：

- **round-robin + 指数退避**：主节点失败后切到备用节点，不是简单重试
- **spring_back**：切到备用后不永久停留，超时自动回切主节点
- **属性广播**：`system`、`temperature` 等参数变更时同步到所有子 Session

故障转移对上层完全透明。上层调 `ask()`，不知道也不需要知道请求最终由哪个节点处理。

## 3. 架构分层

```
┌─────────────────────────────────────────┐
│  对外接口：ToolClient / NativeToolClient  │
│  chat(messages, tools) → ChatResponse    │
├─────────────────────────────────────────┤
│  Session 层：协议转换 + 历史管理          │
│  BaseSession → ClaudeSession / OpenAISession│
│               → NativeClaudeSession / NativeOpenAISession │
│               → MixinSession（故障转移）   │
├─────────────────────────────────────────┤
│  传输层：SSE 解析 + 重试                  │
│  raw_ask() → 流式/非流式 → content_blocks│
└─────────────────────────────────────────┘
```

### 各层职责

| 层 | 做什么 | 不做什么 |
|---|---|---|
| ToolClient / NativeToolClient | 拼接 prompt、解析工具调用、管理工具描述省略 | 不直接发 HTTP 请求 |
| Session | 消息格式转换、历史裁剪、流式消费 | 不解析工具调用（文本协议由 ToolClient 解析） |
| 传输层 | HTTP 请求、SSE 流解析、重试 | 不管理历史、不转换格式 |

### Session 继承逻辑

继承不是为了复用代码，而是为了区分协议：

```
BaseSession          # 公共：历史管理、裁剪、重试、配置
├── TextSession      # 文本协议：消息格式按各自 API 拼接
│   ├── ClaudeTextSession   # Anthropic API + 文本工具
│   └── OpenAITextSession   # OpenAI API + 文本工具
└── NativeSession    # 原生协议：工具走 API tool 字段
    ├── ClaudeNativeSession  # Anthropic API + 原生工具
    └── OpenAINativeSession  # OpenAI API + 原生工具
```

MixinSession 不继承协议，它组合多个**文本协议** Session（OpenAITextSession / ClaudeTextSession），做故障转移。Native session 因 `ask` 签名差异（返回 `ChatResponse` 而非 `str`）被 `__post_init__` 类型守卫拒绝——Native 的 failover 应由 `NativeToolClient` 层自行实现。

## 4. 关键机制

### 4.1 工具描述省略

文本协议下，工具 JSON 每轮都塞进 prompt，占用大量 token。省略策略：

- 记录上次发送的工具 JSON（`last_tools`）
- 本轮工具未变 → 只发"工具库状态：持续有效"
- 每 10 次 `chat` 请求强制重发（由 `ToolClient` 自身的 `request_count % 10` 单点控制），防止 LLM 遗忘
- 累计 token 估算超阈值时也强制重发
- 主循环收到 `ActionResult.flags` 含 `reset_tools` 时强制重发（替代原来的"未知工具"字符串匹配机制）

原生协议不需要这个机制——API 会自动处理工具描述的上下文管理。

### 4.2 SSE 流解析

流式响应不是"收到完整响应后返回"，而是"边收边 yield"。这意味着：

- 解析器必须是增量式的：每个 SSE event 只处理当前 chunk
- tool_calls 的 JSON 参数可能跨多个 chunk 分片到达，需要拼接
- 流中断（连接断开、超时）必须返回已接收内容 + 错误标记，不能丢数据

Claude 和 OpenAI 的 SSE 事件结构完全不同，各自独立实现，不抽象公共层——强行抽象只会增加复杂度。

### 4.3 历史裁剪

裁剪时机：Agent Loop 先按组件构建当前输入；Session 再在每次 `ask()` 前处理协议历史。

裁剪顺序（由轻到重）：

1. **内容截断**：thinking / tool_use / tool_result 内容超过 `max_len` 时截断
2. **标签折叠**：`<history>` / `<key_info>` / `<earlier_context>` 替换为 `[...]`
3. **消息删除**：按保守 token 估算从头部删除整条消息，直到低于 Session 输入预算
4. **引用修复**：删除消息后，首条 user 消息中的孤立 `tool_result` 改写为纯文本

被删除消息只向 `history_compaction` 写入 role、token 数、原因和 SHA-256，不保存原文。
当前轮的 system、task、history、retrieval、memory 和 tool result 另由
`ContextBuilder` 独立预算，详见 [agent-kernel.md](agent-kernel.md)。

关键约束：裁剪只在 Session 内部发生，上层通过 `ask(prompt)` 传入的内容不会被裁剪——只有历史中的旧消息会被动刀。

### 4.4 重试策略

可重试的条件：网络错误（Timeout / ConnectionError）+ 服务端可重试状态码（429 / 5xx）。

退避策略：尊重 `Retry-After` 头，否则指数退避 `min(30s, 1.5 * 2^attempt)`。

流式请求中断后不重试——已经 yield 了部分内容给上层，重试会导致重复。返回错误文本，由上层（循环引擎）决定是否重新发起。

### 4.5 Provider credential

本地兼容 Session 仍持有原始 API key，但 credential、endpoint、prompt/history、
embedding config、tool arguments 和 raw provider body 均不得进入 repr。transport、
provider body 和 failover 异常只返回固定 `ProviderRequestError.reason_code`，不得链接
原始 URL、response 或异常 cause/context。credential holder 禁止使用
`dataclasses.asdict()`、`vars()` 或通用 JSON 序列化。

远程 Agent 不复用本地 Session 的字符串 credential。部署侧 `ProviderInvoker` 保管长期
API key，Worker 只接收一次调用的短期 `ProviderAccessGrant`；完整边界、消费顺序与当前
非生产限制见 [provider-credential-boundary.md](provider-credential-boundary.md)。

## 5. 不做的事

- **不做请求队列 / 并发控制**：Agent 是单线程串行调用，不需要并发管理
- **不做模型路由**：用哪个模型由配置决定，不在运行时动态切换模型
- **不做 embedding / 向量检索**：历史检索靠裁剪策略，不引入向量数据库
- **不做多模态输入**：只处理文本，图片/音频由工具层（浏览器）间接处理
- **不做协议自动探测**：文本协议还是原生协议由配置显式声明，不运行时猜测
- **不做 SSE 解析公共抽象**：Claude 和 OpenAI 的事件结构差异太大，各自独立实现
