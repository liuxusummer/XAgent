# XAgent 技术文档大纲

> 基于4份核心设计文档（架构、Agent Loop、LLM接入层、工具执行层）
> 目标：构建一个最小可运行的物理级全能执行 Agent 系统

---

## 文档一：系统架构设计（→ [architecture.md](architecture.md)）

### 1.1 系统总览
- 1.1.1 定位：物理级全能执行 Agent（非对话式，强调工具调用闭环）
- 1.1.2 核心能力矩阵：文件读写 / 代码执行 / 浏览器控制 / 用户交互 / 记忆管理
- 1.1.3 设计哲学：探测优先、失败升级、最小副作用

### 1.2 分层架构
```
┌─────────────────────────────────────────┐
│           Frontends（多端适配层）          │
│  WeChat / Feishu / Telegram / Qt / CLI  │
├─────────────────────────────────────────┤
│  AgentCore（核心引擎层）           │
│  XAgent → agent_loop → handler   │
├─────────────────────────────────────────┤
│  LLM Layer（模型接入层）           │
│  Session / ToolClient / SSE      │
├─────────────────────────────────────────┤
│  Tool Layer（工具执行层）           │
│  code_run / file_* / web_* / ... │
├─────────────────────────────────────────┤
│  Infrastructure（基础设施层）       │
│  WebDriver / memory / SOP        │
└─────────────────────────────────────────┘
```
- 1.2.1 各层职责与边界
- 1.2.2 层间数据流：`queue.Queue` + 生成器协议
- 1.2.3 线程模型：主线程跑前端 → 后台线程跑 `agent.run()`

### 1.3 核心数据流
```
用户输入
  ↓
put_task(query) → task_queue
  ↓
agent.run() 取出任务
  ↓
组装 system_prompt + user_input
  ↓
run_agent_loop(client, sys_prompt, user_input, handler, tools_schema)
  ↓ 循环
  client.chat(messages, tools) → LLM 响应
  ↓
  解析 tool_calls → handler.dispatch(tool_name, args)
  ↓
  工具执行 → ActionResult(data, next_prompt, should_exit, flags)
  ↓
  flags 处理（reset_tools → 重置 client.last_tools）
  ↓
  拼接下一轮 prompt → 继续循环 或 退出
  ↓
display_queue.put({'done': full_resp})
  ↓
前端渲染输出
```
- 1.3.1 消息拼接策略：只传当前轮（`messages = [{"role": "user", "content": next_prompt, "tool_results": ...}]`），完整历史由 Session.history 维护
- 1.3.2 退出条件：`should_exit=True` / `next_prompt=None` / `MAX_TURNS_EXCEEDED`
- 1.3.3 中断机制：`stop_sig` + `code_stop_signal` + `consume_file(task_dir, '_stop')`

### 1.4 关键设计决策
- 1.4.1 为什么用 `ActionResult` 统一封装：工具返回值异构，需要统一的退出信号和 prompt 接续机制。`flags` 将控制信号从 `next_prompt` 中显式剥离，避免字符串内容匹配触发控制流
- 1.4.2 为什么历史由外部 Session 维护：主循环只关心当前轮，历史裁剪/缓存策略由 Session 层统一处理
- 1.4.3 为什么每10轮重置工具描述：工具 JSON 占用大量 token，重复时省略可节省上下文
- 1.4.4 为什么用生成器协议：支持流式输出（前端实时展示中间结果）

### 1.5 目录结构规范
```
XAgent/
├── workspace/                  # Agent 默认工作区（未指定时自动创建）
├── src/
│   └── core/
│       ├── agent_loop.py       # 循环引擎 + BaseHandler + ActionResult + AgentContext
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

- 工作区规则：未显式指定时默认使用 `<project_root>/workspace`，并在启动时自动创建
- `ctx.cwd` / prompt 中的 `cwd` 都表示当前 Agent 工作区；相对路径默认基于该目录解析
- `memory/` 仍属于代码目录资源，不跟随工作区变化

---

## 文档二：Agent 循环引擎 Spec（→ [agent-loop.md](agent-loop.md)）

### 2.1 ActionResult 数据结构

`ActionResult(data, next_prompt, should_exit, flags)` —— 工具执行的统一返回契约。

- 2.1.1 `data`：工具执行结果，序列化后作为 `tool_result` 回传 LLM
- 2.1.2 `next_prompt` 的语义（只承载语义内容，不承载控制信号）：
  - `None` → 当前任务完成，退出循环
  - `""` → 强制退出（如 ask_user 中断）
  - 非空字符串 → 注入下一轮 prompt（含 working memory / history / SOP 提示）
- 2.1.3 `should_exit` 的语义：立即中断循环，不等待其他工具结果
- 2.1.4 `flags` 的语义（`FrozenSet[str]`，循环级控制信号，有限枚举）：
  - `frozenset()` → 无控制信号（默认）
  - `{"reset_tools"}` → 重置 `client.last_tools`，强制下轮重发工具描述
  - `{"retry"}` → 请求主循环重试当前轮

### 2.2 BaseHandler 基类

四个核心方法：`tool_before_callback` / `tool_after_callback` / `turn_end_callback` / `dispatch`。持有 `AgentContext` 实例管理所有可变状态。

- 2.2.1 `dispatch` 分发机制：
  - 查找 `exec_{tool_name}` 方法
  - 存在 → 调用 `tool_before_callback` → `exec_{tool_name}` → `tool_after_callback`
  - 不存在且 `tool_name == 'bad_json'` → 返回重试 prompt
  - 不存在 → 返回"未知工具"提示 + `flags={"reset_tools"}`
  - `no_tool` 不走分发——LLM 未调用工具是循环级事件，由主循环的 `handle_no_tool_call` 处理
  - 工具返回生成器时在 `dispatch` 内以 `yield from` 透传；非流式场景由主循环 `exhaust()` 一次性收敛

- 2.2.2 `turn_end_callback` 钩子（声明式注册）：
  - 通过 `TurnEndHook(name, fn, priority)` 注册，按优先级降序执行
  - 默认注册的钩子：
    - `external_intervene`（priority=30）：`_keyinfo` / `_intervene` 文件注入
    - `plan_reminder`（priority=25）：每 15 轮注入 plan.md 预览，提醒对齐计划
    - `self_evolution`（priority=23）：从失败/空转工具结果沉淀经验并注入换策略提示
    - `periodic_inject`（priority=20）：每7轮防重试警告、每10轮全局记忆、每65轮强制 ask_user
    - `summary_extract`（priority=10）：提取 `<summary>` 写入 `ctx.history_info`，无 summary 时强制提醒
  - 准入条件：只允许注册轮次级横切关注点（需感知 `current_turn` 或 `response` 全文）
  - `_turn_end_hooks` 扩展点（飞书卡片等）

### 2.3 run_agent_loop 主循环

签名：`run_agent_loop(client, system_prompt, user_input, handler, tools_schema, max_turns=40)`

- 2.3.1 初始化：
  - `messages = [system, user]`
  - `handler.max_turns = max_turns`

- 2.3.2 单轮流程：
  ```
  1. turn += 1; yield turn 标题
  2. if turn % 10 == 0: client.last_tools = ''  # 重置工具描述
  3. response_gen = client.chat(messages=messages, tools=tools_schema)
  4. 流式消费 response_gen → response
  5. 解析 tool_calls:
     - 有 tool_calls → [{'tool_name', 'args', 'id'}]
     - 无 tool_calls → handle_no_tool_call(handler, response)
  6. 遍历 tool_calls（仅在有 tool_calls 时）:
     a. handler.dispatch(tool_name, args, response, index=ii)
     b. 流式消费生成器 → result
     c. 处理 flags（reset_tools → 重置 client.last_tools）
     d. 判断退出条件
     e. 收集 tool_results + next_prompts
  7. handler.turn_end_callback(...) → next_prompt
  8. messages = [{"role": "user", "content": next_prompt, "tool_results": tool_results}]
  ```

- 2.3.3 退出条件判定：
  | 条件 | 行为 |
  |------|------|
  | `result.should_exit` | break，exit_reason = EXITED |
  | `not result.next_prompt` | break，exit_reason = CURRENT_TASK_DONE |
  | `next_prompts` 为空且无 `_done_hooks` | break |
  | `next_prompts` 为空但有 `_done_hooks` | pop 第一个 hook 作为 next_prompt |
  | `turn >= max_turns` | 循环结束，exit_reason = MAX_TURNS_EXCEEDED |

- 2.3.4 输出格式化：
  - verbose 模式：`🛠️ Tool: name 📥 args: ...` + 代码块包裹输出
  - 非 verbose 模式：`🛠️ name(compact_args)` + 紧凑输出
  - `_clean_content`：代码块超6行时折叠、清理 XML 残留标签

### 2.4 exhaust 辅助函数

消费生成器至结束，返回 `return` 值。用于非 verbose 模式下跳过流式输出。

---

## 文档三：LLM 接入层 Spec（→ [llm-layer.md](llm-layer.md)）

### 3.1 Session 继承体系
```
BaseSession
├── ClaudeTextSession         # Anthropic API（文本协议工具）
├── OpenAITextSession         # OpenAI API（文本协议工具）
├── ClaudeNativeSession       # Anthropic API（原生 tool 字段）
└── OpenAINativeSession       # OpenAI API（原生 tool 字段）
```

### 3.2 BaseSession

公共基类，管理 `history`、`context_win`、`stream`、`max_retries`、`temperature`、`max_tokens`、`thinking_type`、`api_mode` 等配置。

- 3.2.1 `ask(prompt)` 流程：追加到 history → `trim_messages_history` 裁剪 → `make_messages` 转换格式 → `raw_ask` 发送请求 → 流式 yield 或一次性返回

- 3.2.2 `make_messages(raw_list)` 抽象方法：
  - ClaudeTextSession：`_drop_unsigned_thinking` + 添加 cache_control
  - OpenAITextSession：`_msgs_claude2oai` 转换

### 3.3 ClaudeNativeSession
- 3.3.1 与 ClaudeTextSession 的区别：
  - 工具走 API 原生 `tools` 字段（非文本注入）
  - 自动添加 Claude Code beta headers
  - 支持 `[1m]` 后缀触发 1M 上下文
  - `ask()` 返回 `ChatResponse`（含 `tool_calls` 列表）
  - `_pending_tool_ids` 追踪未回复的 tool_use

- 3.3.2 `ask(msg)` 流程：
  ```
  1. msg 是 dict（非 str），直接 append 到 history
  2. trim_messages_history
  3. raw_ask → 流式消费 → content_blocks
  4. _ensure_text_block：无 text block 时从 thinking 注入 <summary>
  5. 追加 assistant 消息到 history
  6. 解析 tool_calls（原生 + 文本 fallback）
  7. 返回 ChatResponse(thinking, content, tool_calls, raw)
  ```

- 3.3.3 请求构造：
  - Headers：`anthropic-version: 2023-06-01`、beta flags、`anthropic-dangerous-direct-browser-access`
  - 鉴权：`sk-ant-` → `x-api-key`，其他 → `Authorization: Bearer`
  - Payload：`tools`（从 OpenAI 格式转换）、`system`（带 cache_control）、`metadata`
  - `_apply_claude_thinking`：thinking_type / reasoning_effort 映射

### 3.4 ToolClient（文本协议包装）

包装 `backend` Session，提供 `chat(messages, tools) → ChatResponse` 接口。维护 `last_tools`（工具描述省略）和 `total_cd_tokens`（token 估算）。

- 3.4.1 `_build_protocol_prompt`：
  - 提取 system_content + history_msgs
  - 拼接工具指令（交互协议 + 工具 JSON）
  - 工具描述省略：`last_tools` 相同时只发"工具库状态：持续有效"
  - token 估算：累计 `len(user) // 3`，超 9000 时重置 `last_tools`

- 3.4.2 `_parse_mixed_response`：
  - 提取 `<thinking>` 块
  - 提取 `<tool_use>` 块 → 解析 JSON → ToolCall
  - Fallback：裸 JSON 检测、`bad_json` 兜底

### 3.5 NativeToolClient（原生协议包装）

包装 `backend` Session，提供 `chat(messages, tools) → ChatResponse` 接口。合并 messages 为单个 user message，tool_results 转为 tool_result blocks，追踪 `_pending_tool_ids`。

### 3.6 MixinSession（故障转移）

组合多个同类型 Session，实现 round-robin + 指数退避 + spring_back 自动回切主节点。`_BROADCAST_ATTRS`（`system` / `tools` / `temperature` 等）变更时同步到所有子 Session。

- 3.6.1 约束：引用的 Session 必须全 Native 或全非 Native

### 3.7 SSE 流解析
- 3.7.1 `_parse_claude_sse`：
  - 事件类型：`message_start` / `content_block_start` / `content_block_delta` / `content_block_stop` / `message_delta` / `message_stop`
  - 逐块构建 content_blocks 列表
  - tool_use 的 input JSON 在 `content_block_stop` 时解析
  - 异常处理：未收到 `message_stop`、`max_tokens` 截断

- 3.7.2 `_parse_openai_sse`：
  - chat_completions 模式：`delta.content` / `delta.tool_calls` / `delta.reasoning_content`
  - responses 模式：`response.output_text.delta` / `response.function_call_arguments.delta`
  - 拼接 JSON 参数、处理 split tool calls

- 3.7.3 `_try_parse_tool_args`：
  - 正常 `json.loads`
  - 拼接 JSON：`{...}{...}` → split by `(?<=\})(?=\{)`
  - 失败 → `{"_raw": raw}`

### 3.8 历史裁剪
- 3.8.1 `compress_history_tags`：
  - 每5次调用执行一次
  - 保留最近 `keep_recent` 条消息
  - 截断 `<thinking>` / `<tool_use>` / `<tool_result>` 内容至 `max_len`
  - 替换 `<history>` / `<key_info>` / `<earlier_context>` 为 `[...]`

- 3.8.2 `trim_messages_history`：
  - 先调 `compress_history_tags`
  - 计算总字符数
  - 超过 `context_win * 3` 时：强制压缩 → 从头部删除消息 → 修复孤立 tool_result

- 3.8.3 `_sanitize_leading_user_msg`：
  - 删除历史头部后，首条 user 消息中的 `tool_result` block 改写为纯文本
  - 避免孤立 `tool_use_id` 引用

### 3.9 重试策略

可重试状态码：`{408, 409, 425, 429, 500, 502, 503, 504, 529}` + `requests.Timeout` / `requests.ConnectionError`。退避策略：尊重 `Retry-After` 头，否则指数退避 `min(30s, 1.5 * 2^attempt)`。流式请求中断后不重试，返回错误文本。

### 3.10 ChatResponse / ToolCall

统一 Claude / OpenAI 两种 API 的响应格式。`ChatResponse(thinking, content, tool_calls, raw, stop_reason, usage)`，`tool_calls` 为空时 `stop_reason = end_turn`。`ToolCall(name, args, id)` 封装单个工具调用。`usage` 为可选的归一化 `TokenUsage`，只记录 provider 返回的真实 token 元数据，不做估算。

---

## 文档四：工具 Schema 设计（→ [tool-layer.md](tool-layer.md)）

### 4.1 Schema 格式规范

遵循 OpenAI function calling 格式（`type: function` + `name` + `description` + `parameters`）。Claude 侧自动转换：`function` → `name` + `input_schema`。

### 4.2 工具清单

#### 4.2.1 code_run
- 代码执行器，支持 python / shell
- `script` 参数与回复代码块互斥，优先从参数取
- `inline_eval`：危险参数，仅内部使用
- 返回：`{status, stdout, exit_code}`

#### 4.2.2 file_read
- 按行范围 / 关键词搜索读取文件
- `keyword` 模式：忽略大小写搜索，返回匹配行 + 上下文
- 超长行截断：`L_MAX = min(max(100, 256000//行数), 8000)`
- 文件不存在时模糊匹配建议

#### 4.2.3 file_search
- 基于当前 workspace 的 `runtime/file_index.sqlite3` 做路径 / 全文关键词检索，可选启用 chunk 级语义向量检索
- 首次搜索或 `refresh=true` 时增量刷新索引，按 `relative_path + mtime_ns + size` 判断文件是否变化
- `mode=keyword|semantic|hybrid` 控制检索模式；默认 `hybrid`，但未配置 embedding 或缺少 `sqlite-vec` 时保持 FTS5 兼容降级
- 只返回候选路径、行号、短片段和基础元信息；修改或依赖精确内容前仍需 `file_read`
- `root` 只能位于当前 workspace 内，索引默认跳过 symlink、超过 5 MiB 的文件、二进制、依赖/构建目录和 `runtime/**`

#### 4.2.4 file_patch
- 精确替换，唯一性校验（0 匹配 / >1 匹配均报错）
- 支持 `{{file:path:startLine:endLine}}` 引用展开
- 失败时引导先 `file_read` 确认内容

#### 4.2.5 file_write
- 写文件（overwrite / append / prepend）
- 内容提取优先级：`<file_content>` 标签 → 代码块兜底
- 支持 `{{file:...}}` 引用展开

#### 4.2.6 web_scan
- 扫描页面，返回简化 HTML 或纯文本
- 支持标签页切换

#### 4.2.7 web_execute_js
- 注入 JS 执行，`script` 与回复代码块互斥
- `save_to_file`：长结果保存到文件
- `no_monitor`：跳过页面变化监控

#### 4.2.8 update_working_checkpoint
- 短期工作记忆（`key_info`），每轮通过 `get_anchor_prompt` 注入 prompt
- 增量更新：review → keep / add / remove

#### 4.2.9 ask_user
- 中断循环等待用户输入，返回 `ActionResult(should_exit=True)`

#### 4.2.10 start_long_term_update
- 触发长期记忆结算：读取 SOP → 判断类型 → 最小化更新

### 4.3 中英文 Schema 切换
- `tools_schema.json`（英文）vs `tools_schema_cn.json`（中文）
- 切换条件：模型名含 `glm` / `minimax` / `kimi` 时加载中文版
- Windows 下 `powershell` → `bash` 替换

---

## 文档五：Handler 工具实现 Spec（→ [tool-layer.md](tool-layer.md)）

### 5.1 XAgentHandler 类结构

继承 `BaseHandler`，持有 `parent`（XAgent 实例）和 `ctx`（`AgentContext` 实例）。`AgentContext` 集中管理所有可变状态：`working`（工作记忆）、`cwd`（当前工作区）、`memory_root`（工作区记忆解析根）、`agent_name`、`memory_mode`、`current_turn`、`history_info`（对话摘要）、`code_stop_signal`、`done_hooks`、`empty_count`（连续空响应计数）。

Handler 具有双重身份：分发器（`exec_*` 命名约定分发工具调用）+ 上下文管理器（通过 `ctx` 管理状态）。`ctx` 只在 Handler 的 `exec_*` 方法中使用，不传入 `tools/` 下的纯函数。

### 5.2 exec_code_run
- 提取代码：`args.script` → `args.code` → 回复代码块
- `inline_eval=True` → eval/exec 直接执行；`inline_eval=False` → 写临时文件 + subprocess.Popen
- 流式读取 stdout，超时/停止信号 → process.kill()
- 子进程 `cwd=self.ctx.cwd`，脚本中的相对路径默认落在工作区
- `code_run_header.py` 注入公共 import
- 输出：`ActionResult(result, next_prompt)`

### 5.3 exec_file_read
- 绝对路径转换 → 按行迭代（start 偏移 + keyword 搜索）→ 超长行截断 → smart_format 截断至 20000 字符
- 相对路径默认基于 `ctx.cwd` 解析
- memory/sop 文件注入 SOP 提示
- 输出：`ActionResult(result, next_prompt)`

### 5.4 exec_file_search
- Handler 只传入 `self.ctx.cwd`、`root`、`query`、`limit`、`refresh`、`path_only`、`mode` 和可选 embedding 配置，不持有索引状态
- 纯函数在 workspace `runtime/file_index.sqlite3` 中维护 SQLite FTS5 索引和可选语义 chunk/embedding 索引
- 输出：`ActionResult(result, next_prompt)`，next_prompt 提醒搜索结果只用于定位，精读候选文件仍用 `file_read`

### 5.5 exec_file_patch
- 绝对路径转换 → expand_file_refs → 唯一性校验 → replace → 写回
- 相对路径默认基于 `ctx.cwd` 解析
- 输出：`ActionResult(result, next_prompt)`

### 5.6 exec_file_write
- 内容提取：`<file_content>` 标签 → 代码块兜底 → expand_file_refs
- mode=overwrite → 'w' / append → 'a' / prepend → 读旧+拼新+'w'
- 相对路径默认基于 `ctx.cwd` 解析
- 输出：`ActionResult({status, writed_bytes}, next_prompt)`

### 5.7 exec_web_scan / exec_web_execute_js
- web_scan：懒初始化 WebDriver → 获取标签页 → 简化 HTML → smart_format 截断
- web_execute_js：提取 JS 代码 → 执行 → save_to_file 保存长结果（相对路径基于 `ctx.cwd`）→ 截断至 8000 字符
- WebDriver 全部 HTTP(S)/WebSocket 出口经本地过滤代理；每次连接只使用当次已校验的公网解析结果，页面 JS 不能绕过 SSRF 边界

### 5.8 exec_update_working_checkpoint
- 更新 `self.ctx.working['key_info']` / `['related_sop']`
- 输出：`ActionResult({"result": "working key_info updated"}, next_prompt)`

### 5.9 exec_ask_user
- 返回中断信号 `{status: "INTERRUPT", intent: "HUMAN_INTERVENTION"}`
- 输出：`ActionResult(result, next_prompt="", should_exit=True)`

### 5.10 handle_no_tool_call（循环级处理，非 exec_* 方法）
- 不走 Handler 分发，由主循环直接调用
- 空响应检测：连续3次空 → 退出（`empty_count` 存储在 `ctx` 中）
- 流异常检测：末尾含 !!!Error / max_tokens → 返回 `ActionResult(flags={"retry"})`
- Plan 模式拦截 / 代码块未调用检测 / Plan 完成检测
- 正常结束：`next_prompt=None`

### 5.11 exec_start_long_term_update
- 构造记忆结算 prompt → 读取 memory_management_sop.md → 返回 SOP + 结算指令
- 输出：`ActionResult(sop_content, next_prompt=结算prompt)`

### 5.12 get_anchor_prompt（核心 prompt 构造）
- `earlier_context`：ctx.history_info[:-30] 折叠（连续 [Agent] 行合并，超限时标注裁剪边界）
- `history`：ctx.history_info[-30:] 原样展示
- `current_turn` + `key_info` 注入 + `related_sop` 提示 + `workspace` 提示

### 5.13 turn_end_callback（声明式钩子分发器）
- 按 `_turn_end_hooks` 列表优先级降序执行各钩子
- `summary_extract` 钩子：提取 `<summary>` 写入 ctx.history_info + 调用 `align_history_info` 对齐双历史
- `periodic_inject` 钩子：每7轮防重试警告、每10轮全局记忆、每65轮强制 ask_user
- `external_intervene` 钩子：`_keyinfo` / `_intervene` 文件注入
- Plan 模式：每5轮提醒读 plan.md
- `_turn_end_hooks` 扩展点

---

## 文档六：配置体系 Spec

### 6.1 配置加载

优先 `import mykey`（Python 模块，支持 `importlib.reload` 热重载），Fallback `mykey.json`。

- 6.1.2 变量名路由规则：
  - 含 'native' + 'claude' → ClaudeNativeSession
  - 含 'native' + 'oai' → OpenAINativeSession
  - 含 'claude'（无 native）→ ClaudeTextSession
  - 含 'oai'（无 native）→ OpenAITextSession
  - 含 'mixin' → MixinSession（引用其他 session 的 name）

### 6.2 Session 配置字段

必填：`apikey` / `apibase` / `model`。可选分组：路由（`name` / `proxy`）、容量（`context_win` / `max_retries` / `connect_timeout` / `read_timeout`）、推理（`reasoning_effort` / `thinking_type` / `thinking_budget_tokens`）、采样（`temperature` / `max_tokens`）、传输（`stream` / `api_mode`）。ClaudeNativeSession 专属：`fake_cc_system_prompt` / `user_agent`。

### 6.3 Mixin 配置

`llm_nos`（按优先级排列的 session name 列表）、`max_retries`（总重试次数）、`base_delay`（退避起始延迟）、`spring_back`（回切超时秒数）。

### 6.4 apibase 自动拼接

裸地址自动补 `/v1/chat/completions`，完整路径原样使用，尾部 `$` 表示不拼接。

### 6.5 运行时调参

REPL 命令 `/session.key=value`，支持 `reasoning_effort` / `thinking_type` / `temperature` / `max_tokens` 等。值为文件路径时读取内容，JSON 可解析时自动转换类型。

### 6.6 前端平台配置

各端独立配置：Telegram（`tg_bot_token` / `tg_allowed_users`）、飞书（`fs_app_id` / `fs_app_secret` / `fs_allowed_users`）、微信（QR 扫码）、钉钉、企微、QQ。

### 6.7 Langfuse 追踪配置

`public_key` / `secret_key` / `host`。

---

## 文档七：Prompt 模板设计

### 7.1 系统提示词结构
```
# Role: 物理级全能执行者
你拥有文件读写、脚本执行、用户浏览器JS注入、系统级干预的物理操作权限。
禁止推诿"无法操作"——不空想，用工具探测。

## 行动原则
调用工具前先推演：当前阶段、上步结果是否符合预期、下步策略，
必须在回复文本中用<summary>输出极简总结。
- 探测优先：失败时先充分获取信息...
- 失败升级：1次→读错误理解原因，2次→探测环境状态，3次→深度分析后换方案或问用户...

[动态注入]
Today: 2026-05-05 Mon
[Memory] (../memory)
... global_mem_insight.txt 内容 ...
... insight_fixed_structure.txt 内容 ...
workspace = /path/to/workspace
cwd = /path/to/workspace
相对路径默认基于 workspace 解析。
```

### 7.2 交互协议（文本协议 - ToolClient）
```
### 交互协议 (必须严格遵守，持续有效)
请按照以下步骤思考并行动：
1. **思考**: 在 `<thinking>` 标签中先进行思考，分析现状和策略。
2. **总结**: 在 `<summary>` 中输出*极为简短*的高度概括的单行（<30字）物理快照，
   包括上次工具调用结果产生的新信息+本次工具调用意图。
3. **行动**: 如需调用工具，请在回复正文之后输出一个（或多个）**<tool_use>块**，然后结束。

Format: ```<tool_use>{"name": "tool_name", "arguments": {...}}</tool_use>```

### Tools (mounted, always in effect):
[tools_schema JSON]
```
- 工具描述省略规则：
  ```
  ### 工具库状态：持续有效（code_run/file_read等），**可正常调用**。调用协议沿用。
  ```

### 7.3 交互协议（原生协议 - NativeToolClient）
```
### 行动规范（持续有效）
每次回复（含工具调用轮）都先在回复文字中包含一个<summary></summary>
中输出极简单行（<30字）物理快照：上次结果新信息+本次意图。
此内容进入长期工作记忆。

**若用户需求未完成，必须进行工具调用！**
```
- 原生协议不需要 `<tool_use>` 标签，工具走 API tool 字段

### 7.4 Working Memory 注入（get_anchor_prompt）
```
### [WORKING MEMORY]
<earlier_context>
[USER]: xxx
[Agent] yyy（3 turns）
[USER]: zzz
</earlier_context>
<history>
[Agent] 调用工具file_read, args: {...}
[Agent] 读取了配置文件，发现...
[USER]: 帮我修改这个配置
</history>
Current turn: 15
<key_info>用户要求修改 nginx 配置，端口改为 8080，注意备份原文件</key_info>
有不清晰的地方请再次读取memory/nginx_sop.md
```

### 7.5 周期性 Prompt 注入
| 时机 | 注入内容 |
|------|---------|
| 每7轮 | `[DANGER] 已连续执行第 N 轮。禁止无效重试。若无有效进展，必须切换策略...` |
| 每10轮 | `get_global_memory()` 全局记忆刷新 |
| 每65轮 | `[DANGER] 已连续执行第 N 轮。必须总结情况进行ask_user...` |
| Plan模式每5轮 | `[Plan Hint] 正在计划模式。必须 file_read(plan.md) 确认当前步骤...` |

### 7.6 中英文切换
- 环境变量 `GA_LANG`：`zh`（默认） / `en`
- 影响文件：
  - `sys_prompt.txt` / `sys_prompt_en.txt`
  - `tools_schema.json` / `tools_schema_cn.json`
  - `global_mem_insight_template.txt` / `_en.txt`
  - `insight_fixed_structure.txt` / `_en.txt`
- 自动检测：`locale.getlocale()` 含 `zh`/`chinese` → `zh`

### 7.7 Prompt Cache 策略
- Claude：最后2条 user 消息的最后一个 block 加 `cache_control: {"type": "ephemeral"}`
- OpenAI 兼容层：model 名含 `claude`/`anthropic` 时自动注入
- ClaudeNativeSession：system prompt 加 `cache_control: {"type": "persistent"}`
- 工具列表最后一个 tool 加 `cache_control: {"type": "ephemeral"}`

---

## 附录：复刻实施路线图

### Phase 1：最小可运行 Agent（~500行）


### Phase 2：Native 协议 + 多模型（~800行增量）


### Phase 3：完整工具集（~600行增量）


### Phase 4：记忆 + 自主运行（~400行增量）


### Phase 5：多端适配（每端 ~200-400行）
