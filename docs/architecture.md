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
│  AgentCore（核心引擎层）          │
│  XAgent → agent_loop → handler   │
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
| AgentCore | 循环调度、状态管理 | ActionResult（含 flags）/ 退出信号 | LLM Layer + Tool Layer |
| LLM Layer | API 调用、历史裁剪、SSE 解析 | ChatResponse（统一格式） | API Key / Endpoint |
| Tool Layer | 工具执行、结果封装 | ActionResult | 文件系统 / 浏览器 / 子进程 |
| Infrastructure | 底层能力封装 | 原始数据 | OS / 网络 |

### 层间数据流

- 通信机制：`queue.Queue` + 生成器协议
- 线程模型：主线程跑前端，后台线程跑 `agent.run()`
- 数据格式：层间只传 `ActionResult` 或 `ChatResponse`，禁止跨层直接访问内部状态

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

完整历史由 `Session.history` 维护，主循环不感知历史裁剪。

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

## 4. 关键设计决策

### ADR-1: ActionResult 统一封装

**背景**：工具返回值异构（dict/str/list/None），退出信号和 prompt 接续散落各处。`next_prompt` 承载了多种语义（完成/中断/内容/控制信号），字符串内容匹配触发控制流是隐式协议。

**决策**：所有工具返回 `ActionResult(data, next_prompt, should_exit, flags)`。

**理由**：统一退出信号和 prompt 接续机制，主循环只需处理一种数据结构。`flags` 将控制信号从 `next_prompt` 中显式剥离——原来"未知工具"字符串触发 `reset_tools` 是用内容匹配做控制流，违反契约精神。

**边界**：`data` 不做类型约束，由各工具自行保证序列化；`next_prompt=None` 即完成，不引入额外状态枚举；`flags` 为 `FrozenSet[str]`，合法值有限枚举（`reset_tools`、`retry`），不承载语义信息，语义信息仍由 `next_prompt` 承载。

### ADR-2: 历史由 Session 维护

**背景**：主循环需要感知历史长度吗？

**决策**：主循环只关心当前轮，历史裁剪/缓存策略由 Session 层统一处理。

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

### 不做的事

- **不做 ORM / 数据库**：记忆系统基于文件，不引入 DB 依赖
- **不做插件热加载**：工具集编译期确定，运行期不动态增删
- **不做分布式**：单进程单 Agent，不设计 RPC/消息中间件
- **不做通用 Agent 框架**：面向物理执行场景，不抽象为通用对话框架
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
| 历史裁剪阈值 | `context_win * 3` 字符 |
| 单工具返回截断 | 20000 字符（file_read）/ 8000 字符（web） |
| 代码执行超时 | 60s（可配置） |
| 超长行截断 | `min(max(100, 256000//行数), 8000)` |
