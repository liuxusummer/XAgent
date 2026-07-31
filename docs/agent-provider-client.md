# Durable Agent Provider Client 契约

## 1. 目标与边界

`DurableAgentProviderClient` 把 Core Agent Loop 的
`chat(messages, tools) -> ChatResponse` 调用接到
`ProviderAccessBroker`。它解决的是一次 Agent Attempt 内的 provider 调用证据闭环：

1. 使用 collector 分配的 provider invocation sequence；
2. 在签发 grant 前构造并限制确定性的 request bytes；
3. 每轮取得新鲜 `WorkerAuthorization`，但锁定同一 authorization lineage；
4. 通过单次 grant 调用 Broker，并复用 Broker 的 durable replay/recovery；
5. 只接受有持久 MODEL_RESPONSE Artifact 和精确 typed receipt 的结果；
6. 严格解码为 `ChatResponse`，最后才提交本地聊天历史。

它不持有 upstream API key，也不直接发 provider HTTP 请求。route、模型选择、长期
credential、mTLS、egress 和 gateway operation ledger 仍属于 deployment-owned
`ProviderInvoker`。

本类的 `production_security_ready` 固定为 false。它不会把 `agent` 加入远程
assignment/worker 的支持列表。

## 2. 构造前置条件

client 只服务一个确切的 Agent request/Attempt。构造时必须同时满足：

- `AgentActivityRequest` 与 request ArtifactRef 的 digest、分类和 producer 完全一致；
- `AgentExecutionEvidenceCollector` 的 Run/Node/Attempt、request、request Artifact、
  definition 和分类完全一致；
- Broker 的 journal 是磁盘 durable，result store 是参考实现可证明落盘的
  `LocalArtifactStore`；
- route ID 存在，且 Broker 可返回同一个不可变、无 credential 的
  `ProviderRouteDescriptor`；
- generation、TTL、上下文和 retry 上限在固定边界内。

父绑定错误和 durable result 条件缺失都会在读取授权、签 grant 或调用 provider 前失败。
SECRET response 仍会被参考 `LocalArtifactStore` 拒绝；生产加密 Store 不会被参考实现
自动声明为 durable。

## 3. 调用顺序

每次 `chat()` 的顺序不可交换：

0. 以 per-client non-blocking single-flight guard 拒绝同一 Attempt 的并发调用；不同
   client/Attempt 之间没有共享锁；
1. 从 collector 读取当前 open provider observation；没有 observation 时失败；
2. 合并保留的 system/history 与本轮消息，构造 canonical request；
3. 同时按全局上限和 route 的 `maximum_request_bytes` 预检，避免超限请求占住 grant；
4. 调用受信 `authorization_source`，重验父 Attempt 和 Artifact sensitivity；
5. 若已有成功 grant，要求 authorization digest 与首次 lineage 完全相同；
6. Broker 签发绑定 request Artifact、payload sequence 和 route 的 one-call grant；
7. 对同一个 grant 和同一份 bytes 最多进行三次 Broker 调用；
8. 仅对 Broker 明确的 unknown/recovery 类固定 reason code 重试；
9. 重验 Result Artifact、route response bytes 上限、receipt 父绑定、authorization、
   route、sequence 和 payload digest；
10. 严格解码 response wire，构造 receipt 从 repr 隐藏的 `ChatResponse`；
11. 只有以上步骤全部成功，才原子提交 system/history/request count 和 detached response
    ArtifactRef；终态组合据此重读实际 provider 输出，而不是只相信 receipt 中的 digest。

任何失败都会向 Agent Loop 抛出；Loop 随后调用 `provider_call_failed`，collector 将该链
永久降级为 partial。provider 已完成但 response 非法时，历史不会伪装为成功推进。

## 4. Version 1 wire

ProviderInvoker 接收的是 canonical UTF-8 JSON，不是 OpenAI/Anthropic 原生 body：

```json
{
  "generation": {
    "max_output_tokens": 4096,
    "temperature": 0.2
  },
  "invocation_index": 1,
  "kind": "agent_provider_request",
  "messages": [],
  "schema_version": 1,
  "tools": []
}
```

`messages` 支持 `system/user/assistant`，以及可选的 `thinking`、`tool_calls`、
`tool_results`。assistant tool call 使用 exact
`{"name", "arguments", "id"}`。所有值必须是严格 JSON；循环、非有限数、超深结构、
超大字符串、控制字符 key、bool-as-int 和任意对象都被拒绝。

gateway 必须返回 exact-field、canonical JSON：

```json
{
  "content": "done",
  "kind": "agent_provider_response",
  "schema_version": 1,
  "stop_reason": "end_turn",
  "thinking": "",
  "tool_calls": [],
  "usage": null
}
```

response 拒绝重复 key、未知字段、非规范编码、无界 tool call、非法 ID、非 object
arguments 和不完整 usage。`usage` 非空时必须恰好包含 Core `TokenUsage` 的六个字段，
每项为 null 或有界非负整数。

wire 不携带 grant token、WorkerAuthorization、provider credential、endpoint、宿主路径
或 receipt。Broker 通过 out-of-band route 和 grant ID 调用 ProviderInvoker；receipt
只在成功结果返回后附加到 `ChatResponse.provider_receipt`。

## 5. 历史与上下文

client 暴露 Session-compatible `backend/history/history_compaction`，所以
`ContextBuilder` 仍能对历史做身份感知的预算与裁剪。首次 system 消息单独保留；每次
成功把本轮非 system 消息和规范 assistant tool-call 结构加入历史。

client 自身还有 254 条 fallback 历史上限，保留第一个 user task 和最近消息；被省略内容
只记录 count 与 canonical SHA-256，不写入 compaction metadata。正常 Agent Loop 的
ContextBuilder 上限更小，因此 fallback 只防止 client 被脱离 Loop 误用时无界增长。

## 6. Recovery 语义

client 的重试始终复用同一个 grant、同一 authorization 和完全相同的 payload bytes。
它不会自行重调 provider：

- Broker 判断 result 已完成时从 Artifact 重放；
- Broker 对可信 COMPLETED evidence 返回 recovered receipt；
- Broker 对可信 NOT_STARTED evidence 完成唯一 CAS 后才允许再调用；
- IN_PROGRESS/UNKNOWN 或重试耗尽继续失败关闭。

签发 grant 后进程崩溃仍无法在新进程取回 bearer；这是 Broker 已声明的限制。不得为了
可恢复性把 bearer 写进 Event、manifest、checkpoint 或普通日志。

在一次调用已经返回完整 receipt 后，`checkpoint_state()` 可导出 detached system/history、
compaction、当前 authorization digest/lineage 起点、request count 和有序 response refs；它同时
绑定 route、generation、context 和 retry 配置 digest。v2 状态允许不同 Claim owner 形成多个
authorization receipt 段，旧 v1 状态按“全部属于第一段”兼容读取。只有 Store 已验证
checkpoint adoption 时，executor 才令 `restore_checkpoint_state(...,
reset_authorization_lineage=True)` 从当前 request count 开始新段；旧 receipts 仍逐条校验，不能
被新 owner 的授权摘要覆盖。该机制只恢复下一次尚未开始的调用，不恢复或猜测 in-flight grant。

## 7. 尚未完成

- reference Durable Tool Handler、动态 child authority、父终态事务和本地
  `DurableAgentActivityExecutor` 已形成显式组合，但远程 Worker adapter 仍未接入；
- 本地组合已把 client state、collector receipt prefix 和 Loop state 组成安全轮次 checkpoint，
  并由主 Store 原子登记 checkpoint/request/provider response Artifact 引用；
- 当前未过期 Claim 可直接跨进程恢复；过期 Claim 可由受信控制器在主 Store 中换
  owner/fencing 并采用闭合 checkpoint。已经签发但未进入该 checkpoint 的 provider grant 仍
  不可跨 owner 取回 bearer，只能由部署 gateway 的 operation ledger 收敛；
- Broker 的参考 `ProviderInvoker` readiness 不能证明 mTLS、attestation、egress 或外部
  operation ledger；
- reference client 不在进程内线程上伪造可中断 timeout；部署侧 invoker 必须实施有界网络
  deadline，编排层仍以持久 execution deadline/cancellation 处理超时后的 unknown 状态；
- 跨主机共识、加密 SECRET ArtifactStore 和 grant issuance response 恢复仍未实现。

因此这一阶段只完成 provider 子链，不构成可接纳的生产 remote Agent runtime。
