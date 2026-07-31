# Agent Activity Request Artifact 契约

## 1. 目的

远程 Agent 不能通过 argv、Domain Event、Attempt metadata 或普通 RPC JSON 接收原始
task/context。这些载荷可能很长、包含业务敏感内容，而且必须在 Worker 获得权限前保持在
控制面边界内。

`AgentActivityRequest` 将一次已编译 Agent Node 的指令物化为 content-addressed
`agent_request` Artifact。控制面和 Worker 只在授权边界外交换 Artifact descriptor、
一次性 grant 和 digest，不交换 Store URI 或宿主路径。

本契约是远程 Agent 的输入基础，不会把当前安全远程执行链从 Tool-only 改成 Agent-capable。

## 2. 稳定身份与易变权限分离

请求 Artifact 绑定：

- `run_id / node_id / attempt_id / attempt_number`；
- Scheduler 的 `request_digest`；
- immutable Workflow `definition_digest`；
- Agent 名称和完整 task/context/expected-output config；
- 保留逻辑 input name 与顺序的 path-free Artifact descriptors。

它不绑定 Worker、lease、claim token、fencing token、projection version、session 或 grant。
这些是短期执行权限，分别由 candidate CAS、Worker authorization、ClaimBinding 和 grant
约束。因而同一个 candidate 改由另一个 Worker 执行时，请求 Artifact 内容身份保持不变。

`request_digest` 与请求 Artifact 自身的 `sha256` 是两个不同事实：

- `request_digest` 是 Scheduler 对 definition/config/Run input/input mapping 的业务请求绑定；
- Artifact `sha256` 是本请求 envelope 字节的内容身份。

后续安全远程组合必须同时绑定两者，禁止互相替代。

## 3. Schema v1

```text
schema_version        1
kind                  agent_activity_request
run_id / node_id / attempt_id / attempt_number
agent_name
request_digest / definition_digest
task? / context? / expected_output? / output?
input_bindings[]:
  name
  artifacts[]         ArtifactDescriptor（无 URI/producer/path）
```

约束：

- canonical UTF-8 JSON，最多 256 KiB；
- exact-field 解析，unknown/missing field、bool-as-int、非规范编码和控制字符 fail closed；
- 至少有 task、context 或一个 input binding；
- task/context 等单字段最多 64 KiB，组合仍受 256 KiB 总上限；
- binding name 有序且唯一；
- 上下文 Artifact 最多 63 个，预留远程协议 64 个 input grants 中的 1 个给 request
  Artifact 本身；
- 单个上下文 Artifact 不能超过 Broker 的 64 MiB 输入上限。

请求 repr、错误码、ArtifactRef metadata 和 grant descriptor 均不包含 task/context。
解析异常不链接可能持有敏感原文的 JSON/Unicode 异常。

## 4. 分类与秘密边界

参考 stager 以 `sensitive` 为最低分类，并继承所有上下文 Artifact 的最高分类；只要
一个输入为 `secret`，request 也必须是 `secret`，防止 descriptor 的 id/digest 元数据被
降级暴露。metadata 仅为 `{"schema":"agent_activity_request_v1"}`。本地
ArtifactStore 对 sensitivity 标签不宣称静态加密，这与现有模型响应 Artifact 一致。

Bearer credential、私钥和可直接使用的 secret 禁止内联到 Workflow task/context。
它们必须通过部署管理的 secret/Artifact capability 在执行时注入。参考 stager 只传播
已有输入分类，不因 `secret` 标签而声称内容已加密。

## 5. Staging、崩溃与重试

`AgentActivityRequestArtifactStore.stage(candidate)` 只接受：

- `activity_kind=agent` 的精确 `ActivityAdmissionCandidate`；
- `SCHEDULED` Attempt；
- 空 claim token、零 fencing 和零 lease deadline；
- 与 Attempt metadata 一致的 request/definition digest；
- `input_artifact_bindings` 与扁平 refs 完全一致。

stage 使用 canonical bytes 和 content-addressed ArtifactStore。Artifact 在 Domain
mutation 之前安装；若写入后响应丢失，只会留下未引用 orphan，重试收敛到同一
artifact id/SHA/URI。并发 staging 也必须返回可验证的同一内容身份；`created_at` 是
存储观察时间，不作为内容身份。

未来安全 remote admission 必须在 policy/preflight 通过后 staging，在 claim CAS 前验证
Artifact；失败时不 claim。请求 ref 可从同一 durable candidate 重新构造，不把 raw
task/context 写入恢复 journal。

## 6. Broker 与 Worker 校验

请求 Artifact 使用现有 `ArtifactGrantBroker`：

1. Worker authorization 必须显式允许 request 的推导分类（`sensitive` 或 `secret`）；
   未加密的 `secret` 即使有权限也必须 fail closed；
2. Broker 验证完整 ArtifactRef 后签发短期单次 read grant；
3. consume 在 bytes 离开控制面前持久发生；
4. Worker 重验 descriptor size/SHA；
5. `AgentActivityRequest.from_bytes()` 验证 canonical schema；
6. `validate_runtime_binding()` 对比 Assignment 的 Run/Node/Attempt、attempt number、
   Activity kind、request digest、Agent 名称和 config digest；
7. `validate_grant_descriptors()` 要求 request grant 恰好一个，其他 descriptor 与
   envelope 中去重后的上下文全集完全一致，禁止缺失、额外或重复 grant。

## 7. 当前未承诺

- `SecureRemoteAssignmentAdmitter`、`SecureRemoteExecutionAdapter`、
  `DurableFleetProjector` 仍只支持 Tool Activity；
- 尚无独立的远程 Agent runtime、模型 provider credential boundary 或逐工具 receipt
  聚合；
- 本阶段不会把 `agent` 加入任何生产 `supported_activity_kinds`；
- 请求 Artifact 存在不代表 Agent 结果可验证；结果仍由独立
  [AgentActivityReceipt](agent-activity-receipt.md) 契约约束；
- LocalArtifactStore 不提供部署级静态加密或抗数据库/文件系统管理员篡改保证。

三轮对抗性审查和可执行证据见
[Agent Activity Request 三轮对抗性审查](agent-activity-request-adversarial-review.md)。
