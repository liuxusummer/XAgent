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

### 执行在隔离区，结果要截断

代码执行和浏览器操作都有不可控的一面。隔离原则：

- **代码执行**：子进程，不在线程。超时即 kill，不等待优雅退出
- **浏览器操作**：独立 WebDriver session，不共享主进程的浏览器状态
- **文件操作**：路径必须转换为绝对路径；相对路径一律基于 `ctx.cwd`（当前 Agent 工作区）解析

工作区约定：

- `ctx.cwd` 表示 Agent 当前工作区，不再表示代码目录
- Agent 启动时若用户未指定工作区，默认使用 `<code_root>/workspace`，不存在则自动创建
- `memory_root` 指向代码目录下的 `memory/`，供长期记忆与 SOP 文件读取，避免随工作区切换漂移

结果截断不是可选项，是必需品。一个 10000 行的文件读出来全塞给 LLM 会撑爆上下文。截断策略按工具域不同：

- 文件类：20000 字符硬上限
- 浏览器类：8000 字符硬上限
- 代码执行：stdout 不截断，但超长行会动态计算截断阈值

截断时保留首尾，丢弃中间，让 LLM 知道有内容被省略。

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
│   ├── file_patch        # 精确替换（唯一性校验）
│   └── file_write        # 写文件（覆盖 / 追加 / 前插）
├── 浏览器操作域
│   ├── web_scan          # 扫描页面（简化 HTML / 纯文本）
│   └── web_execute_js    # 注入 JS 执行
├── 记忆管理域
│   ├── update_working_checkpoint  # 短期工作记忆
│   ├── start_long_term_update     # 触发长期记忆结算
│   └── plan_update                # 读写 workspace/plan.md（读：不传 content；写：传 content 覆盖）
└── 交互域
    └── ask_user          # 中断循环，等待用户输入
```

### 域间关系

- 代码执行域和文件操作域是独立的，互不依赖
- 浏览器操作域依赖 `BrowserDriver` 接口，默认使用 Selenium fallback；工具只暴露 `web_scan` / `web_execute_js`，底层可替换为 WebSocket / HTTP Long-Poll 浏览器桥
- 记忆管理域只操作 Handler 内部状态和 memory 目录文件，不依赖其他域
- 交互域（ask_user）是特殊的——它不执行任何物理操作，只产生中断信号；当前桥接为进程级单例（`_bridge_display_queue` / `_bridge_reply_queue`），多 Agent 场景需在上层串行化
- Plan 模式、外部干预文件（如 `_keyinfo`、`_intervene`）与相对路径文件操作统一落在工作区内

### 浏览器操作闭环

浏览器被抽象成 Agent 的远程可观测、可执行环境：

```
web_scan → BrowserDriver.scan → 压缩页面快照 + tab sessions
web_execute_js → BrowserDriver.execute_js → exec_id + ACK/结果诊断
```

关键契约：

- `web_scan` 返回 `sessions`、`current_session_id`、`page`，tab 以稳定 `session_id` 寻址；`tab_index` 仅保留兼容
- `web_execute_js` 返回 `exec_id`、`ack`、`result_received`、`diagnostics`，用于区分送达、执行、导航和失败状态
- 长结果通过 `save_to_file` 落盘，tool_result 只返回路径、字节数和摘要
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
  → subprocess.Popen(cwd=ctx.cwd) 执行
  → stream_reader 线程流式读取 stdout
  → 超时 / 停止信号 → process.kill()
  → 返回 {status, stdout, exit_code}
```

关键约束：

- **不用 eval/exec**：除非 `inline_eval` 显式开启（仅内部使用），一律走子进程
- **流式读取**：stdout 不等进程结束才返回，边读边 yield，前端可实时展示
- **双信号停止**：`code_stop_signal`（Handler 级）+ `stop_sig`（全局级），每个循环轮次检查
- **工作目录固定**：代码执行的进程 `cwd` 固定为 `ctx.cwd`，因此脚本内相对路径也默认落在工作区

### 4.2 file_patch 的唯一性契约

patch 是最危险的操作——改错一行可能破坏整个文件。唯一性校验是安全网：

- `old_content` 在文件中必须恰好匹配 1 次
- 0 次 → 内容已变，建议先 file_read 确认当前内容
- \>1 次 → 上下文不够，建议加长 old_content 使其唯一

这比"全文搜索替换第一个匹配"安全得多。LLM 看到错误信息后会主动 file_read 再 patch，形成探测-确认-修改的安全闭环。

### 4.3 文件引用展开

`{{file:path:startLine:endLine}}` 语法允许工具参数引用文件内容，而不是把大段文本塞进 JSON。

这在 file_patch 和 file_write 中使用：LLM 可以写 `new_content = "前缀\n{{file:config.yaml:10:20}}\n后缀"`，运行时展开为文件第 10-20 行的实际内容。

设计意图：减少 JSON 参数中的文本量，降低 LLM 生成 JSON 时的截断风险。

其中 `path` 若为相对路径，同样以 `ctx.cwd` 为基准解析。

### 4.4 工作记忆的轮次注入

`update_working_checkpoint` 更新的 `key_info` 不是存在就完了——它每轮通过 `get_anchor_prompt` 注入到 prompt 中。

这意味着：
- LLM 每轮都能看到当前工作记忆，不需要从历史中翻找
- 工作记忆是增量更新的：LLM 先 review 现有内容，再决定 keep / add / remove

### 4.5 无工具调用处理（handle_no_tool_call）

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

- **不做工具编排**：工具之间的调用顺序由 LLM 决定，工具层不实现 DAG 或流水线
- **不做工具结果缓存**：文件和浏览器状态随时变化，缓存会导致过期数据
- **不做自动重试**：工具失败返回错误信息，由 LLM 决定是否重试、换参数还是换方案
- **不做工具权限分级**：所有工具对 LLM 平等可见，权限控制在 Schema 描述中引导（如 `inline_eval: "DO NOT USE except explicitly specified"`）
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
