# Memory Telemetry Spec

> 本文定义 Memory Provider 之后的下一步具体改进：为 memory 读取与注入补齐结构化观测事件。目标是建立可验证的 memory 行为闭环，不改变模型可见 prompt、不改变 memory 文件格式、不引入新的存储依赖。

## 1. 问题定义

`memory-provider-read-spec.md` 已经把 L0/L3/L4 的文件读取收敛到 `src/core/memory.py`，并让读取结果返回 `MemoryReadResult.files` 元数据。但当前这些元数据还没有进入事件流。

这导致几个问题：

- 启动 prompt 中到底读到了哪些 memory 文件、哪些缺失，只能靠人工读文件推断。
- 第 10 轮 `[Memory Refresh]` 是否注入、注入了多少字符，当前只在 prompt 中体现，无法从 JSONL 事件流聚合。
- `start_long_term_update()` 是否读到了 SOP、SOP 是否缺失或为空，无法被观测。
- 后续要做 L3 相关性注入或 memory 写入护栏时，缺少基线指标。

本 spec 要解决的问题是：**把 memory 读取和注入行为转化为只含元数据的 `Event`，进入现有 `EventSink` 流。**

## 2. 目标与非目标

### 目标

- 为 L0 boot memory、L3 refresh memory、L4 SOP memory 读取补结构化事件。
- 只记录文件名、状态、长度、错误类型等元数据，不记录 memory 正文。
- 复用现有 `Event` / `EventSink` / JSONL / Langfuse 管道。
- 保持现有 prompt 内容、注入时机、工具返回字段不变。
- 增加可测试的事件断言，证明 memory 观测不会影响主流程。

### 非目标

- 不做 memory 内容检索、相关性命中或 section index。
- 不改 `MemoryReadResult` 正文拼接规则。
- 不改 `global_mem.txt`、`*_sop.md` 或 insight 文件格式。
- 不把完整 prompt、memory 正文、SOP 正文写入日志。
- 不新增可视化 UI、实时 tail 或事件采样。
- 不修复与 memory 无关的现有全量测试失败。

## 3. 为什么现在做

这个改进应排在 L1 工作记忆结构化和 L3 相关性注入之前。

原因：

- **风险低**：只加事件元数据，不改变 Agent 决策输入。
- **收益直接**：能回答“memory 是否被读取/注入”这类基础诊断问题。
- **承接自然**：Provider 已经提供 `MemoryFileStatus`，事件数据不需要重新解析文件。
- **后续必要**：没有基线事件，就很难评估相关性注入是否降低了注入长度、是否导致命中缺失。

## 4. 事件设计

新增 3 类事件：

| kind | 触发点 | name | data |
|---|---|---|---|
| `memory_boot_loaded` | 构造启动 system prompt 时 | `boot` | `{file_count, present_count, empty_count, error_count, total_chars, files}` |
| `memory_refresh` | 第 10 轮周期注入判断时 | `global` | `{injected, file_count, present_count, empty_count, error_count, total_chars, files}` |
| `memory_sop_loaded` | `start_long_term_update` 读取 SOP 时 | `memory_management_sop.md` | `{present, empty, error, chars}` |

### 文件元数据格式

`files` 中每个元素只允许包含：

```python
{
    "name": "global_mem.txt",
    "exists": True,
    "empty": False,
    "chars": 120,
    "error": None,
}
```

不记录：

- `path`：避免泄露本地目录结构到远端观测平台。
- `content`：避免泄露长期记忆正文。
- prompt 片段或 tool result 正文。

## 5. 调用点契约

### 5.1 Boot Memory

当前入口：`src/main.py::build_system_prompt()` 调用 `load_memory_content()`。

改进方式：

- 让 `build_system_prompt()` 可选接收 `sink: EventSink | None = None` 和 `session_id: str = ""`。
- `load_memory_content()` 内部仍调用 `load_boot_memory()`。
- 当 sink 存在时，emit `memory_boot_loaded`。

兼容要求：

- 不改变 `build_system_prompt()` 现有必填参数。
- 不改变 `[动态注入]`、`[Memory]`、`workspace`、`cwd` 输出。
- 未传 sink 时保持零观测行为。

### 5.2 Global Memory Refresh

当前入口：`XAgentHandler._periodic_inject_hook()` 第 10 轮调用 `load_global_memory()`。

改进方式：

- 使用 `handler.ctx.sink.emit(...)` 发 `memory_refresh`。
- `turn` 使用 `ctx.current_turn`。
- `injected=True` 仅表示本轮实际追加了 `[Memory Refresh]`。
- 文件缺失、为空、读取失败时仍 emit，但 `injected=False`。

兼容要求：

- 不改变每 7 轮危险提示。
- 不改变每 10 轮 refresh 时机。
- 不改变 `[Memory Refresh]` 标题和正文拼接。

### 5.3 Memory SOP

当前入口：`src/tools/interaction.py::start_long_term_update()` 调用 `load_memory_sop()`。

约束：`start_long_term_update()` 当前是纯工具函数，不持有 `AgentContext` 或 `EventSink`。

改进方式：

- 不在 `src.tools.interaction` 内直接发事件。
- 在 `XAgentHandler.exec_start_long_term_update()` 中读取工具返回后，根据 `result["sop_meta"]` 或等价元数据 emit `memory_sop_loaded`。
- 为保持工具返回兼容，`sop_content` 和 `instruction` 不变；如需新增元数据字段，字段名应为 `sop_meta`，且只含元数据。

兼容要求：

- 不改变工具 schema。
- 不改变 Handler 兜底 prompt。
- 不要求工具层依赖 telemetry。

## 6. 数据汇总规则

建议在 `src/core/memory.py` 增加纯函数：

```python
def memory_event_data(result: MemoryReadResult) -> dict[str, Any]:
    ...
```

输出：

| 字段 | 含义 |
|---|---|
| `file_count` | 尝试读取的文件数 |
| `present_count` | `exists=True` 的文件数 |
| `empty_count` | `empty=True` 的文件数 |
| `error_count` | `error is not None` 的文件数 |
| `total_chars` | 所有文件 `chars` 之和 |
| `files` | 去掉 path/content 后的文件状态列表 |

该函数只处理元数据，不访问文件系统。

## 7. 风险与护栏

| 风险 | 影响 | 护栏 |
|---|---|---|
| 观测污染隐私 | 长期记忆或 SOP 正文被写入 JSONL / Langfuse | 事件数据禁止包含 `content`、`path`、prompt、tool result 正文 |
| 启动路径变复杂 | `build_system_prompt()` 增加 sink 参数后影响调用方 | 新参数必须有默认值；现有调用不传时行为不变 |
| 工具层反向依赖 telemetry | `src/tools/interaction.py` 变成有状态工具 | SOP 事件由 Handler emit，工具层最多返回元数据 |
| 事件过多 | 每轮都记录 memory 会膨胀日志 | 只记录 boot、10 轮 refresh、显式长期结算 SOP |
| 观测失败影响主流程 | sink 异常打断 Agent | 继续依赖现有 Sink 吞异常机制；业务逻辑不捕获或依赖 emit 结果 |

## 8. 测试计划

### 单元测试

新增或扩展 `tests/test_memory_provider.py`：

- `memory_event_data()` 不包含 `path` 和 `content`。
- 正确计算 `file_count`、`present_count`、`empty_count`、`error_count`、`total_chars`。

### 调用点测试

新增或扩展测试：

- `build_system_prompt(..., sink=CaptureSink)` emit `memory_boot_loaded`，且 prompt 文本不变。
- `_periodic_inject_hook()` 第 10 轮 emit `memory_refresh`，非空 memory 时 `injected=True`。
- `_periodic_inject_hook()` memory 缺失或为空时 emit `memory_refresh`，`injected=False`。
- `exec_start_long_term_update()` emit `memory_sop_loaded`，且 `sop_content` 行为不变。

### 回归测试

至少运行：

```bash
.venv/bin/python -m unittest tests.test_memory_provider tests.test_cli.WorkspaceTests
```

若运行更大范围测试发现与 memory telemetry 无关的既有失败，应单独复现并在交付说明中隔离说明，不在本改动中修复。

## 9. 验收标准

完成后应满足：

- JSONL / Stderr / Langfuse 均可接收新增 memory 事件，且只含元数据。
- 不传 sink 的调用路径行为保持兼容。
- 用户可见 prompt、tool schema、tool result 关键字段不变。
- memory 事件测试覆盖正常、缺失、为空、错误元数据场景。
- `git diff --check` 通过。

## 10. 后续扩展

本 spec 完成后，才进入下一类改进：

- L3 相关性注入：基于 `memory_refresh.total_chars` 和后续 `matched_sections` 评估注入收益。
- L1 工作记忆结构化：增加 anchor prompt 的长度与槽位观测。
- L3 写入护栏：增加 settlement proposal / validation / applied 事件。
