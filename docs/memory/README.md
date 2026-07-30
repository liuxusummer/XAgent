# Memory 模块体系优化思路

> 对 `memory/`、`AgentContext`、`turn_end_hooks`、`Session.history` 和两阶段
> Memory Store 的能力做体系化梳理。当前运行时安全契约见
> [Agent Kernel 第一阶段](../agent-kernel.md)。

相关文档：

- [workspace-agent-memory-spec.md](workspace-agent-memory-spec.md)：定义工作区全局记忆与 Agent 私有记忆的读取边界。
- [memory-provider-read-spec.md](memory-provider-read-spec.md)：定义 Memory Provider 读路径收敛。
- [memory-telemetry-spec.md](memory-telemetry-spec.md)：定义下一步具体改进，为 memory 读取与注入补齐结构化观测事件。

## 1. 背景与目标

XAgent 的定位是物理级执行 Agent。它的 memory 不是聊天产品里的“用户画像库”，而是让 Agent 在多轮工具调用中持续对齐目标、复用经验、减少重复探测的上下文控制系统。

当前系统已经具备四类记忆能力：

| 能力 | 当前载体 | 注入/触发方式 | 主要用途 |
|---|---|---|---|
| 启动洞察 | `memory/global_mem_insight.txt`、`memory/insight_fixed_structure.txt` | `src/main.py::build_system_prompt()` 拼入 system prompt | 固化身份、行为模式、当前状态 |
| 短期工作记忆 | `AgentContext.working["key_info"]`、`["related_sop"]` | `update_working_checkpoint` 更新，`get_anchor_prompt()` 每轮注入 | 保存当前任务关键事实、约束、SOP 提示 |
| 轮次摘要 | `AgentContext.history_info` | `<summary>` 提取，`get_anchor_prompt()` 注入最近 30 条 | 维持跨轮语义连续性 |
| 长期记忆 | `MemoryStore` active records；旧文件为管理员兼容层 | 启动时只加载当前 Principal 有权读取的 approved record | 保存跨任务复用且已审核的事实和经验 |

优化目标：

- 让每一种记忆都有清晰的职责、生命周期、写入规则和注入预算。
- 减少重复、过期、无证据的记忆进入上下文。
- 保持现有文件型记忆模型，不引入数据库或分布式依赖。
- 让 memory 的行为可测试、可观测、可渐进演进。

## 2. 现状问题

### 2.1 读写路径分散

当前 memory 相关逻辑分散在：

- `src/main.py`：启动时读取 insight 类文件。
- `src/core/XAgent.py`：设置 `ctx.memory_root`。
- `src/handler/XAgentHandler.py`：短期记忆、摘要提取、周期性刷新、记忆文件提示。
- `src/tools/interaction.py`：长期记忆结算 SOP 读取。
- `memory/memory_management_sop.md`：写入规范。

这种分散不是错误，但会带来两个风险：不同入口的注入内容不一致；新增记忆能力时难以判断应该改哪一层。

### 2.2 记忆层级命名不统一

现有文档里 `global_mem.txt` 被称为 L2 长期记忆，`global_mem_insight.txt` 被称为 L0 记忆洞察；代码里又有 `working`、`history_info`、`related_sop` 等运行态字段。概念本身合理，但缺少一张统一分层图。

### 2.3 写入质量依赖模型自觉

`start_long_term_update` 只读取 SOP 并要求模型调用 `memory_propose`。候选带来源、trust、
confidence、ACL、TTL 和 review status；未经过独立 reviewer 前不会进入 active memory。

### 2.4 注入预算较粗

`get_anchor_prompt()` 每轮注入工作记忆和最近摘要；`_periodic_inject_hook()` 每 10 轮注入整个 `global_mem.txt`。这保证了简单可靠，但长期看会有 token 浪费，也可能把与当前任务无关的长期记忆反复注入。

### 2.5 双历史对齐只处理长度

`Session.history` 负责原始消息和协议裁剪，`AgentContext.history_info` 负责语义摘要。当前 `align_history_info()` 会按 session 历史长度裁剪摘要条数，但摘要本身没有来源轮次、工具结果引用、置信度等元信息，难以判断一条摘要是否仍然可依赖。

## 3. 目标分层

建议将 memory 明确为五层，每层只有一个核心问题：

```
L0 Identity Insight      启动时我是谁、怎样行动、当前系统状态是什么？
L1 Working Memory        当前任务必须记住什么？
L2 Turn Summary          最近执行过程发生了什么？
L3 Long-term Memory      哪些跨任务事实值得持久保存？
L4 Procedural Knowledge  遇到某类任务应遵循什么 SOP？
```

| 层级 | 载体 | 读入时机 | 写入时机 | 约束 |
|---|---|---|---|---|
| L0 Identity Insight | `global_mem_insight.txt`、`insight_fixed_structure.txt` | Agent 启动 | 低频结算 | 少变、结构固定，保留身份、行为模式、当前状态 |
| L1 Working Memory | `AgentContext.working` | 每轮 `get_anchor_prompt()` | `update_working_checkpoint` | 面向当前任务，任务结束可丢弃 |
| L2 Turn Summary | `AgentContext.history_info` | 每轮 `get_anchor_prompt()` | 每轮 `<summary>` 提取 | 短句、按轮追加、可折叠 |
| L3 Long-term Memory | `MemoryRecord`；旧 `global_mem.txt` 为管理员兼容层 | approved + ACL + TTL | `memory_propose` 后独立 review | 可复用、有证据、去重 |
| L4 Procedural Knowledge | `*_sop.md`、`memory_management_sop.md` | 显式相关或 `related_sop` 命中 | 人工或结算后低频更新 | 稳定流程，不混入临时事实 |

核心原则：L1/L2 负责“本次任务连续性”，L3/L4 负责“跨任务复用”，L0 负责“系统自我约束”。不同层不要互相代偿。

## 4. 优化方向

### 4.1 收敛为 Memory Provider 边界

新增一个轻量的 `MemoryProvider` 或等价模块，先不改变存储形态，只集中读路径：

- `load_boot_memory()`：读取 L0，供 `build_system_prompt()` 使用。
- `load_global_memory()`：读取 L3，供周期性注入或相关性检索使用。
- `load_sop(name_or_hint)`：读取 L4，供 `related_sop` 或工具提示使用。
- `format_anchor(ctx)`：生成 L1/L2 的 prompt 片段，供 Handler 调用。

这样做的价值不是抽象本身，而是把“哪些记忆会进入 prompt”变成单点可审计逻辑。第一阶段可以保持原文件和现有格式不变，降低行为风险。

### 4.2 结构化 L1 工作记忆

`key_info` 目前是自由文本，容易混入目标、约束、发现、待办。建议逐步拆成固定槽位：

| 字段 | 含义 |
|---|---|
| `goal` | 用户当前任务目标 |
| `constraints` | 用户约束、不可做事项、环境边界 |
| `facts` | 已验证事实 |
| `decisions` | 已做决策及原因 |
| `open_questions` | 仍需确认的问题 |
| `artifacts` | 已生成或修改的重要文件 |

短期可继续序列化为一段 prompt，不要求立刻改变工具 schema。后续再把 `update_working_checkpoint` 的参数从自由文本升级为结构化字段。

### 4.3 给 L2 摘要增加最低质量门槛

`<summary>` 是双历史体系的桥。优化重点不是写得更长，而是更可靠：

- 摘要应包含“上次工具结果产生的新事实 + 本轮意图”，继续保持 30 字左右。
- 对连续重复摘要做合并或拒收，避免 `history_info` 被无效进展占满。
- 记录来源轮次，例如内部形态可从 `"[Agent] ..."` 演进到 `{turn, summary}`。
- 当 `Session.history` 发生大幅裁剪时，在 `<earlier_context>` 中显式标注摘要边界，避免模型误以为完整历史仍可追溯。

这些优化都可以在 Handler 层完成，不需要改 LLM Session 协议。

### 4.4 L3 长期记忆改为“相关性注入优先”

每 10 轮注入整个 `global_mem.txt` 简单可靠，但不是长期最优。建议分两步演进：

1. 先按 Markdown 二级标题建立 section index，不引入 embedding。
2. 用当前任务输入、L1 工作记忆、`related_sop`、最近摘要做关键词打分，只注入相关 section。

当没有命中时，保留当前“低频全量刷新”兜底。这样既满足项目“不引入 DB”的边界，也能明显降低无关记忆进入上下文的概率。

### 4.5 L3 写入改为“提案-校验-落盘”

长期记忆写入应从“模型直接改文件”演进为三段式：

1. **提案**：模型输出本次结算想新增、修改、删除的条目，附来源事实。
2. **校验**：系统检查主题段是否存在、是否重复、是否超过长度、是否违反 SOP。
3. **落盘**：仍用 `file_patch` 做最小 diff，并在写后 `file_read` 验证。

校验规则可以先做轻量文本检查：

- 新增条目必须落在一个 `##` 主题段下。
- 新增条目不能与同段已有条目高度相似。
- 单条不超过 SOP 约定长度。
- 每次最多修改 1-2 个主题段。
- 无明确可复用价值时允许“不更新”。

### 4.6 L4 SOP 与长期事实分离

`global_mem.txt` 应保存事实和经验，`*_sop.md` 应保存流程。优化时需要避免两类污染：

- 把一次性事实写进 SOP，导致流程过拟合。
- 把流程步骤写进 `global_mem.txt`，导致长期记忆难以检索。

建议约定：当某条记忆包含“每次、必须、步骤、流程、遇到 X 时”这类程序化信号时，优先进入 SOP；当它是“某项目/环境/工具的稳定事实”时，进入 `global_mem.txt`。

### 4.7 补齐可观测性

Memory 行为应该能在事件流里解释清楚。建议在已有 telemetry 上补充以下字段或事件：

| 事件 | 建议数据 |
|---|---|
| `memory_boot_loaded` | 文件列表、总长度 |
| `memory_anchor_built` | L1/L2 注入长度、摘要条数 |
| `memory_refresh` | 注入模式：full/section/none，命中 section |
| `memory_settlement_started` | SOP 是否存在、候选文件 |
| `memory_settlement_applied` | 修改文件、主题段、diff 大小 |

这些事件只记录元数据，不记录完整记忆正文，避免日志泄露和噪声。

## 5. 推荐演进路线

### Phase 1：统一概念与读路径

- 建立 memory 分层术语，更新相关文档引用。
- 新增轻量读模块，集中读取 L0/L3/L4。
- 保持现有 prompt 内容和文件格式不变。
- 增加单元测试覆盖启动记忆读取、周期刷新、SOP 缺失兜底。

验收：不同入口读取同一组 boot memory；现有 memory 行为不回退。

### Phase 2：增强运行态记忆质量

- 将 L1 工作记忆内部结构化，但对 prompt 仍渲染为兼容文本。
- 为 L2 摘要加去重、空摘要过滤、来源轮次。
- 在 `get_anchor_prompt()` 中输出更稳定的分区格式。

验收：重复摘要不再堆积；工作记忆能区分目标、约束、事实和待确认问题。

### Phase 3：长期记忆相关性注入

- 为 `global_mem.txt` 建 Markdown section index。
- 按当前任务上下文做关键词相关性命中。
- 命中时注入相关 section，未命中时保留全量刷新兜底。

验收：长任务中 L3 注入长度下降，且不影响需要长期事实的任务表现。

### Phase 4：长期记忆结算护栏

- `start_long_term_update` 已收敛为结算提案入口，真正持久提交走 `memory_propose`。
- `MemoryStore` schema v2 使用有界 `memory_key` 检测同一事实的重复与冲突；审批者必须
  显式、原子地替代所有重叠的 active record，不能静默覆盖或留下并行真相。
- `provenance_records()` 保留替代链；`compact()` 按 ACL、tenant 和保留期清除过期正文，
  同时维护累计 purge digest。
- 写后继续做完整状态、候选—记录双向关系和 supersession 图校验，失败时整体拒绝写入。

验收：长期记忆更新 diff 更小，重复条目下降，错误更新可诊断。

### Phase 5：评估与观测闭环

- 增加 memory 相关 telemetry。
- 建立一组回放用例：重复任务、跨任务复用、过期信息修正、无价值信息拒收。
- 用 JSONL 或 Langfuse 对比 memory 优化前后的 token 使用、turn 数、重复探测次数。

验收：能用可复现数据说明 memory 优化是否有效。

## 6. 与现有边界的关系

- 不引入 ORM、数据库、向量库作为第一阶段依赖，继续使用文件型记忆。
- 不让工具层持有状态；状态仍归 `AgentContext` 和 Handler 管理。
- 不把历史裁剪挪出 Session；memory 只消费裁剪后的对齐信号。
- 不把 memory 写入做成自动后台任务；长期落盘仍由显式工具或明确钩子触发。
- 不让 SOP、skills、plan 互相替代：SOP 是流程知识，skills 是提示包，plan 是当前任务计划。

## 7. 后续实现注意事项

任何实现都应保持最小 diff：

- 先新增测试和集中读模块，再迁移调用点。
- 每次只迁移一个入口，例如先处理 `build_system_prompt()`，再处理 periodic refresh。
- 文件记忆更新继续优先使用 `file_patch`，不要全量覆盖。
- 如果阶段执行中发现路线需要调整，先更新本文档并让用户确认，再继续实现。
