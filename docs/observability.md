# 观测性（Phase 8 + Phase 9）

XAgent 把内部运行过程抽象为**结构化事件流**，通过单一 `EventSink` 协议对外暴露。默认零开销（`NullSink`），按需切换到 JSONL 文件或 stderr。

## 开关

通过环境变量启用：

| 变量 | 作用 |
|---|---|
| `XAGENT_LOG_DIR` | 未设 → NullSink；设值 → `JsonlSink(path)`，按 `session_id` 切分为 `{path}/{session_id}.jsonl` |
| `XAGENT_LOG_STDERR` | `=1` 时叠加 `StderrSink`，与 Jsonl 共存（`MultiSink`） |
| `XAGENT_OBS_BACKEND` | `langfuse` 时启用 Langfuse 远端轨迹 Sink（可选依赖） |
| `XAGENT_LANGFUSE_ENABLED` | `=1` 时启用 Langfuse 远端轨迹 Sink |
| `XAGENT_LANGFUSE_PUBLIC_KEY` / `LANGFUSE_PUBLIC_KEY` | Langfuse public key |
| `XAGENT_LANGFUSE_SECRET_KEY` / `LANGFUSE_SECRET_KEY` | Langfuse secret key |
| `XAGENT_LANGFUSE_HOST` / `LANGFUSE_HOST` | Langfuse host，自托管时配置 |
| `XAGENT_OBS_SERVICE` | 观测平台里的服务名，默认 `xagent` |

仅 `main.build_sink()` 读取这些变量；库/测试调用保持 NullSink 默认。Langfuse 作为可选依赖，未安装或初始化失败时自动降级，不阻断 Agent 启动。

## 事件模型

```python
@dataclass
class Event:
    session_id: str          # 本次 run_task 的 uuid16
    turn: int                # 当前 turn；0 = turn 外（run 边界）
    kind: str                # 枚举，见下
    name: str                # 细分标签：工具名、hook 名、exit_reason 等
    ts: float                # time.time()
    duration_ms: float | None = None
    data: dict[str, Any] = {}
```

## kind 枚举

| kind | 何时触发 | name | data |
|---|---|---|---|
| `run_start` | `run_agent_loop` 入口 | query 首 80 字 | `{query_len, max_turns}` |
| `run_end` | `run_agent_loop` 退出 | `exit_reason` | `{turns}` |
| `turn_start` | 每 turn 顶部 | "" | `{}` |
| `turn_end` | 每 turn 末尾 | "" | `{tool_count}` |
| `llm_end` | `client.chat` 后 | `stop_reason` | `{has_tool_calls, content_len, tool_call_count}` |
| `tool_start` | `BaseHandler.dispatch` 入口 | tool_name | `{args_len}` |
| `tool_end` | `BaseHandler.dispatch` 出口 | tool_name | `{should_exit, next_prompt_len, flags, status}` |
| `hook_inject` | TurnEndHook 返回非空 | hook name | `{prompt_len}` |
| `skill_auto_selected` | `run_task` 开始后 | `auto` | `{selected, query_len}` |
| `skill_activated` | `skill_activate` 工具执行 | `skill_activate` | `{activated, missing}` |
| `skill_injected` | 每轮 active skills 注入 prompt | `active_skills` | `{active_count, inject_len}` |

`run_start` / `run_end` / `turn_start` / `turn_end` / `llm_end` / `tool_start` / `tool_end` 的 `data` 中还会携带当前 skill 使用状态：`active_skills`。该字段只记录 skill 名称，不记录 skill 正文。

原则：
- 不记录完整 prompt / response / tool args（体积 + 隐私）
- 只记长度、布尔位、枚举标签
- `run_end` / `llm_end` / `tool_end` 必有 `duration_ms`；`turn_end` 表示轮次边界，仅记录 `tool_count`

## Sink 实现

- `NullSink`：零开销默认，所有方法 no-op
- `StderrSink`：打印单行摘要到 stderr；人眼 debug
- `JsonlSink(path)`：线程安全（`threading.Lock` + line buffering）；按 `session_id` 分文件
- `MultiSink(*sinks)`：广播；任一 sink 抛异常不影响其他
- `PlatformSink(exporter)`：平台接入统一适配层，包裹 Langfuse / OTel / HTTP 等 exporter
- `LangfuseExporter`：把 `Event` 映射为 Langfuse trace / span / generation / event；位于 `src/core/observability.py`

所有 Sink 内部吞异常，**永不阻断主循环**。平台 SDK 逻辑不进入 `agent_loop.py`，后续扩展 OTel/HTTP 只需新增 exporter。

## 与 verbose 的关系

`verbose` 是面向**人眼**的 debug 开关，打精简可读字符串到 stderr；Sink 是面向**机器**的结构化流。两者职责分离，互不融合。

## 典型排障

```bash
# 启动带 JSONL 日志
XAGENT_LOG_DIR=/tmp/xa python -m src.main

# 查看某 session 的完整事件
cat /tmp/xa/abc123.jsonl | jq .

# 只看 LLM 延迟
cat /tmp/xa/abc123.jsonl | jq 'select(.kind=="llm_end") | {turn, ms: .duration_ms}'

# 只看退出原因
cat /tmp/xa/abc123.jsonl | jq 'select(.kind=="run_end") | {name, turns: .data.turns}'

# 找所有失败的工具
cat /tmp/xa/*.jsonl | jq 'select(.kind=="tool_end" and .data.should_exit==true)'
```

## 消费端 CLI

Phase 9 在 `src/tools/reflect/` 下补了两个离线 CLI：

- `python -m src.tools.reflect.replay /tmp/xa`
- `python -m src.tools.reflect.stats /tmp/xa`

都支持 `--session <session_id>` 过滤单个会话。

### replay

把原始 JSONL 渲染成按 turn 缩进的可读轨迹，适合回答"这次 run 到底做了什么、卡在哪一步"。

```bash
python -m src.tools.reflect.replay /tmp/xa --session abc123
```

示例输出：

```text
=== session abc123 (8 events) ===
[00.000s] run_start: "请帮我修 main.py" (query_len=14)
  [turn 1]
    [00.820s] llm_end tool_use +820ms (content=0, tools=1)
    [00.825s] tool_start file_read
    [00.829s] tool_end file_read +4ms (should_exit=False)
    [00.830s] turn_end (tool_count=1)
    [01.240s] llm_end stop +410ms (content=38, tools=0)
    [01.241s] turn_end (tool_count=0)
    [01.242s] run_end CURRENT_TASK_DONE +1242ms (turns=1)
```

### stats

聚合全部 session 的延迟、退出原因和 hook 命中次数，适合回答"最近是不是变慢了、哪个工具最慢、为什么退出"。

```bash
python -m src.tools.reflect.stats /tmp/xa
```

示例输出：

```text
=== aggregated over 3 sessions, 9 turns, 5 tool calls ===
LLM latency (ms):
  count=6  p50=820.0  p95=1200.0  p99=1350.0  max=1400.0
Tool latency by name (ms):
  file_read    count=3  p50=4.0  p95=9.0  p99=10.0
  run_code     count=2  p50=220.0  p95=400.0  p99=430.0
Exit reasons:
  CURRENT_TASK_DONE  2
  MAX_TURNS_EXCEEDED  1
Hook injections:
  plan_reminder  2
```

## Langfuse 接入

Langfuse 接入走统一平台抽象。当前兼容 Langfuse 4.x `create_event` API：每条 XAgent `Event` 会写入同一个 trace 下的 Langfuse event；旧版 SDK/测试桩若提供 `trace().span()/generation()/event()` 也仍可使用原映射。

```text
agent_loop -> Event -> EventSink -> PlatformSink -> LangfuseExporter -> Langfuse
```

默认仍不上传完整 prompt / response / tool args / tool result，只上传长度、状态、耗时、工具名、退出原因等元数据。`run_start.name` 在本地 JSONL/replay 中保留 query 预览，但 Langfuse 远端 metadata 会清空该字段，避免把用户输入片段上传到第三方平台。

示例：

```bash
XAGENT_LOG_DIR=/tmp/xa \
XAGENT_OBS_BACKEND=langfuse \
XAGENT_LANGFUSE_PUBLIC_KEY=pk-... \
XAGENT_LANGFUSE_SECRET_KEY=sk-... \
XAGENT_LANGFUSE_HOST=https://cloud.langfuse.com \
./.venv/bin/uv run python -m src.main
```

`XAGENT_LANGFUSE_PUBLIC_KEY` 和 `XAGENT_LANGFUSE_SECRET_KEY` 必须同时存在，否则自动降级为未启用，不影响 Agent 启动。`XAGENT_LANGFUSE_HOST` 未设置时使用 SDK 默认 host。

### 可选配置文件

除了环境变量，也可以通过独立 JSON 配置文件传入观测性配置。该文件是**可选项**，不传时保持现有环境变量行为。

仓库根目录提供了示例文件 `observability.example.json`。配置结构如下：

```json
{
  "backend": "langfuse",
  "service": "xagent",
  "log_dir": "/tmp/xa",
  "stderr": true,
  "langfuse": {
    "public_key": "pk-...",
    "secret_key": "sk-...",
    "host": "https://cloud.langfuse.com"
  }
}
```

启动：

```bash
./.venv/bin/uv run python -m src.main --observability-config observability.example.json
```

优先级规则：

- 环境变量优先于配置文件
- 配置文件只作用于观测性，不影响现有 `--config` 的 LLM/session 配置
- 配置文件缺失或未启用 `backend=langfuse` 时，自动回退到原有行为

映射关系：

| Event | Langfuse |
|---|---|
| `run_start` / `run_end` | Langfuse event（同一 trace），记录输入长度 / 退出原因 |
| `turn_start` / `turn_end` | Langfuse event（同一 trace），记录轮次边界 |
| `llm_end` | Langfuse event，记录停止原因、响应长度、工具调用数 |
| `tool_start` / `tool_end` | Langfuse event，记录工具名、耗时、状态 |
| `hook_inject` | Langfuse event，记录 hook 名和注入长度 |

CLI 退出时会调用 `XAgent.close()`，从而触发 sink `close()` / Langfuse `flush()`；嵌入式调用方若自行构造 `XAgent`，应在进程结束前显式调用 `agent.close()`。

## 不做的事

- 不采集 token 用量（`ChatResponse` 无 usage 字段，不为观测倒逼契约变更）
- 不做实时订阅 / 流式 tail / 可视化 UI
- 不做事件采样/节流；离线聚合由 `stats.py` 提供最小可用能力
- 不记录完整 prompt/response（需要现场时用 `verbose` 重放）
