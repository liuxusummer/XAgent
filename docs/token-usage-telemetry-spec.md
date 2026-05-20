# Token Usage Telemetry Spec

## Problem

XAgent 的观测事件已经能记录每次任务的 turn、工具调用、耗时和退出原因，但无法回答一次任务真实消耗了多少 token。当前 `ChatResponse` 没有 usage 契约，`llm_end` / `run_end` 事件也没有 token 字段。

## Goals

- 在 LLM 层归一化 provider 返回的真实 usage。
- 在每个 `llm_end` 事件记录本轮 token 用量。
- 在每个 `run_end` 事件记录本次任务累计 token 用量。
- 让 `reflect replay` 和 `reflect stats` 能消费新字段，同时兼容旧 JSONL。

## Non-goals

- 不做 token 估算；provider 未返回 usage 时保持 unknown。
- 不记录 prompt、response、tool args、tool result 或 provider 原始 usage。
- 不改变模型提示词、工具 schema、历史裁剪策略或 CLI 正常任务输出。
- 不引入数据库、后台任务或新的观测管道。

## Contracts

- `src/core/llm.py` 新增 `TokenUsage`，字段为 `input_tokens`、`output_tokens`、`total_tokens`、`cache_creation_input_tokens`、`cache_read_input_tokens`、`reasoning_tokens`。
- `ChatResponse` 新增 `usage: TokenUsage | None`，调用方必须容忍 `None`。
- `llm_end.data` 在 usage 已知时包含上述 token 字段。
- `run_end.data` 在累计 usage 已知时包含同名 token 字段，表示任务总量。

## Implementation Boundaries

- Claude/OpenAI 的 usage key 映射只在 LLM 层处理。
- `AgentContext` 只保存任务级累计的 `TokenUsage`，不保存 provider 原始响应。
- `reflect` 工具只读取事件元数据，不依赖 `ChatResponse`。

## Tests

- usage 归一化覆盖 Claude、OpenAI、缺失 usage。
- SSE parser 保留 usage。
- `ToolClient` / native session 的 `ChatResponse.usage` 正确透出。
- Agent Loop 对 `llm_end` 和 `run_end` 写入 token 字段。
- `replay` / `stats` 对新旧日志都可渲染。
