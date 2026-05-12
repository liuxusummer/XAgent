# Memory Provider 读路径收敛 Spec

> 本文是 memory 模块下一步的具体优化方案 spec，承接 [README.md](README.md) 的体系分层，但只聚焦一个改动：把 L0/L3/L4 的文件读取收敛到统一边界。本文不展开多阶段行动清单，也不涉及运行时代码实现。

## 1. 问题定义

当前 memory 读路径分散在多个模块：

- `src/main.py::load_memory_content()` 读取 `global_mem_insight.txt` 与 `insight_fixed_structure.txt`，注入启动 system prompt。
- `XAgentHandler._periodic_inject_hook()` 直接读取 `global_mem.txt`，每 10 轮注入长期记忆刷新。
- `src/tools/interaction.py::start_long_term_update()` 直接读取 `memory_management_sop.md`，生成长期记忆结算提示。

这些入口都在读取 `memory/`，但各自处理文件顺序、缺失文件、空文件、错误兜底。短期看可运行，长期会让后续优化变难：相关性注入、SOP 选择、memory telemetry、写入护栏都会缺少稳定落点。

本 spec 要解决的问题是：**建立一个只负责 memory 文件读取和格式化的统一边界，让现有调用点迁移过去，并保持外部行为不变。**

## 2. 目标与非目标

### 目标

- 集中 L0 启动洞察、L3 长期记忆、L4 SOP 的读取逻辑。
- 统一缺失文件、空文件、读取失败的降级行为。
- 保持现有 prompt 注入内容、注入时机、工具 schema 不变。
- 为后续相关性注入和 memory telemetry 留出元数据出口。
- 让 memory 读行为可以独立单元测试。

### 非目标

- 不结构化 `AgentContext.working`。
- 不改变 `history_info` 摘要提取与注入。
- 不做 `global_mem.txt` 相关性检索。
- 不修改长期记忆写入流程。
- 不引入数据库、向量库、embedding 或后台任务。
- 不改变 `memory/` 现有文件格式。

## 3. 设计原则

### 读写分离

Memory Provider 只负责读，不负责写。长期记忆结算仍由现有工具链触发，文件修改仍遵循 `memory_management_sop.md` 的最小化更新原则。

### 行为兼容

迁移后的 prompt 文本应与迁移前等价。允许新增内部元数据，但不应改变模型可见的标签、标题、注入轮次和工具返回字段。

### 宽容失败

memory 文件缺失或读取失败不应阻止 Agent 启动或工具运行。Provider 返回空文本和状态元数据，由调用方决定是否注入。

### 确定性输出

多文件拼接必须保持固定顺序，避免相同 memory 目录在不同运行中生成不同 prompt。

### 元数据不泄露正文

Provider 可以返回文件状态、长度、路径、错误类型等元数据。日志和 telemetry 默认只记录元数据，不记录完整 memory 正文。

## 4. 收益与风险

### 收益

| 收益 | 具体体现 |
|---|---|
| 降低入口分散成本 | `src/main.py`、`XAgentHandler`、`src/tools/interaction.py` 不再各自实现 memory 文件读取细节 |
| 提升行为可测试性 | 文件存在、缺失、为空、读取失败等情况可以在 Provider 单元测试中独立覆盖 |
| 保持 prompt 行为稳定 | 调用点继续负责注入时机和标题，Provider 只返回内容，降低模型可见行为漂移风险 |
| 为观测留出口 | `MemoryReadResult.files` 可以记录文件状态和长度，后续 telemetry 不需要重新解析调用点 |
| 为相关性注入打基础 | 后续 L3 section index 可以在 `load_global_memory()` 边界内扩展，不把检索逻辑嵌进 Handler |
| 缩小 Handler 职责 | Handler 继续处理轮次和工具分发，不承担 memory 文件系统细节 |

### 风险

| 风险 | 影响 | 护栏 |
|---|---|---|
| 抽象过早 | 新模块可能只包了一层文件读取，增加跳转成本 | Provider 只提供 3 个读取函数和简单数据结构，不引入复杂类层次 |
| prompt 细节漂移 | 拼接空行、strip 行为变化会影响模型可见上下文 | 对 `build_system_prompt()`、`[Memory Refresh]`、`sop_content` 做迁移前后等价测试 |
| 错误被静默吞掉 | 宽容失败可能掩盖权限、编码等环境问题 | 失败不抛给主流程，但必须写入 `MemoryFileStatus.error` 供测试和观测使用 |
| 路径边界扩大 | SOP 名称如果支持任意路径，可能越过 `memory_root` | `load_memory_sop()` 只接受文件名，拒绝绝对路径和 `..` |
| 元数据泄露正文 | telemetry 若记录完整内容，会泄露长期记忆 | Provider 元数据只包含文件名、路径、长度、状态、错误类型，不包含正文 |
| 调用点职责混淆 | Provider 可能开始判断 turn 或构造完整 prompt | 明确 Provider 只读文件；注入时机、标题和兜底 prompt 仍由现有调用点负责 |

## 5. 模块边界

建议新增一个轻量模块：`src/core/memory.py`。

选择 `src/core/` 的原因：

- memory 读取会被 CLI prompt 构造、Handler hook、工具结算入口共同使用，不属于单个工具域。
- 该模块不依赖 `XAgentHandler`，避免 Handler 继续膨胀。
- 该模块不依赖 LLM Session，避免协议层反向依赖 memory 文件系统。

依赖方向：

```
src/main.py
src/handler/XAgentHandler.py
src/tools/interaction.py
        ↓
src/core/memory.py
        ↓
memory/ 文件系统
```

禁止方向：

- `src/core/memory.py` 不调用 LLM。
- `src/core/memory.py` 不调用工具。
- `src/core/memory.py` 不读写 `AgentContext`。
- `src/core/memory.py` 不修改任何 memory 文件。

## 6. 数据契约

Provider 返回正文和元数据。正文用于 prompt 或 tool result，元数据用于测试和观测。

建议数据结构：

```python
@dataclass(frozen=True)
class MemoryFileStatus:
    name: str
    path: str
    exists: bool
    empty: bool
    chars: int
    error: str | None = None


@dataclass(frozen=True)
class MemoryReadResult:
    content: str
    files: tuple[MemoryFileStatus, ...]
```

字段语义：

| 字段 | 语义 |
|---|---|
| `content` | 已按固定顺序拼接、去掉首尾空白后的正文 |
| `files` | 本次尝试读取的文件状态，包含不存在和读取失败的文件 |
| `exists` | 文件路径存在且是普通文件 |
| `empty` | 文件存在但 `strip()` 后为空 |
| `chars` | `strip()` 后正文长度；失败或缺失为 0 |
| `error` | 读取异常的短错误类型或消息；正常为 `None` |

## 7. 函数契约

### `load_boot_memory(memory_root: str | Path) -> MemoryReadResult`

读取 L0 启动洞察，固定顺序：

1. `global_mem_insight.txt`
2. `insight_fixed_structure.txt`

输出规则：

- 只拼接存在且非空的文件内容。
- 文件之间用两个换行分隔。
- 两个文件都缺失或为空时，`content == ""`。

兼容要求：

- 替换 `src/main.py::load_memory_content()` 后，`build_system_prompt()` 中 `[Memory]` 的模型可见正文保持等价。

### `load_global_memory(memory_root: str | Path) -> MemoryReadResult`

读取 L3 长期记忆：

1. `global_mem.txt`

输出规则：

- 文件存在且非空时返回其 `strip()` 后内容。
- 文件缺失、为空或读取失败时返回空正文。

兼容要求：

- 替换 `_periodic_inject_hook()` 的直接读取后，第 10 轮仍只在有非空正文时注入：

```text
[Memory Refresh]
{content}
```

### `load_memory_sop(memory_root: str | Path, name: str = "memory_management_sop.md") -> MemoryReadResult`

读取 L4 SOP。

输出规则：

- `name` 只能解析为 `memory_root` 下的文件名，不接受绝对路径或 `..` 路径。
- 默认读取 `memory_management_sop.md`。
- 文件缺失、为空或读取失败时返回空正文。

兼容要求：

- 替换 `start_long_term_update()` 的直接读取后，工具返回中的 `sop_content` 与原行为等价。
- SOP 缺失时仍返回空 `sop_content`，由 Handler 走现有兜底 prompt。

## 8. 错误处理

Provider 不向上抛出普通文件读取错误，除非调用方传入的 `memory_root` 类型完全不可处理。

| 情况 | `content` | 元数据 |
|---|---|---|
| 文件不存在 | `""` | `exists=False, empty=True, chars=0` |
| 文件为空 | `""` | `exists=True, empty=True, chars=0` |
| 权限或编码错误 | `""` | `exists=True, empty=True, chars=0, error=...` |
| `name` 含路径穿越 | `""` | `exists=False, empty=True, chars=0, error="invalid_name"` |

错误信息只需足够诊断，不应包含长正文或堆栈。

## 9. 调用点替换约束

### 启动 prompt

`src/main.py::build_system_prompt()` 可以继续保留 `[Memory]` 区块。唯一变化是内部调用 Provider。

不改变：

- `[动态注入]`
- `Today`
- `[Memory]`
- `workspace`
- `cwd`
- “相对路径默认基于 workspace 解析。”

### 周期性长期记忆刷新

`XAgentHandler._periodic_inject_hook()` 可以继续控制注入时机。Provider 只提供内容，不判断 turn。

不改变：

- 每 7 轮危险提示。
- 每 10 轮 memory refresh 时机。
- 每 65 轮 ask_user 确认提示。
- `[Memory Refresh]` 标题。

### 长期记忆结算 SOP

`src/tools/interaction.py::start_long_term_update()` 可以继续返回：

```python
{
    "status": "OK",
    "sop_content": sop_content,
    "instruction": "请根据以上 SOP 对当前对话进行记忆结算：提取关键信息，判断更新类型，执行最小化更新。",
}
```

不改变工具 schema，不改变 Handler 中的兜底逻辑。

## 10. 测试规格

建议新增 `tests/test_memory_provider.py`，专测 Provider；再用现有测试覆盖调用点兼容。

### Provider 单元测试

| 用例 | 断言 |
|---|---|
| boot 两文件都存在 | 内容按 insight → fixed 顺序拼接 |
| boot 只存在一个文件 | 只返回存在文件正文 |
| boot 文件为空 | 空文件不进入正文，元数据 `empty=True` |
| global 存在 | 返回 `global_mem.txt` 正文 |
| global 缺失 | 正文为空，不抛错 |
| SOP 默认读取 | 返回 `memory_management_sop.md` 正文 |
| SOP 路径穿越 | 正文为空，元数据含 `invalid_name` |

### 调用点兼容测试

| 调用点 | 断言 |
|---|---|
| `build_system_prompt()` | `[Memory]` 区块正文与迁移前等价 |
| `_periodic_inject_hook()` | 第 10 轮且 memory 非空时注入 `[Memory Refresh]` |
| `_periodic_inject_hook()` | memory 缺失或为空时不注入空 refresh |
| `start_long_term_update()` | SOP 存在时 `sop_content` 等价；缺失时为空 |

## 11. 验收标准

本优化完成时应满足：

- 所有 memory 文件读取入口统一经 Provider。
- 用户可见 prompt 与工具返回保持兼容。
- memory 文件缺失不会导致启动、周期 hook 或长期结算失败。
- 新增 Provider 测试覆盖正常、缺失、空文件、非法 SOP 名称。
- 不出现新的运行时依赖。
- 不修改 `memory/` 文件格式。

## 12. 后续扩展点

Provider 收敛后，后续优化只能基于该边界扩展：

- L3 相关性注入：在 `load_global_memory()` 之上增加 section index，不嵌入 Handler。
- Memory telemetry：记录 `MemoryReadResult.files` 的文件状态和长度，不记录正文。
- SOP 选择：扩展 `load_memory_sop()` 的 name 选择规则，但仍限制在 `memory_root` 下。
- 写入护栏：新增独立 settlement validator，不放入 Provider。
