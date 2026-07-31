# 本地 Durable Agent Activity 执行器契约

## 1. 范围

`DurableAgentActivityExecutor` 把已经完成 admission 的单个 Agent Claim 组合到 Core Agent
Loop。它复用现有的 request Artifact、provider gateway client、动态 Tool executor、execution
evidence collector 和原子终态提交器，不改变默认 CLI/Web，也不把 `agent` capability 开放给
远程 Worker。

该实现是本地参考组合，不是生产远程 runtime。`durable_result_recovery_ready` 为 true：持有
当前未过期 Claim bearer 的重启进程可以从最近一个安全轮次精确恢复；
`production_security_ready` 仍固定为 false，因为过期 Claim 的自动接管、远程 assignment
attestation 和 SECRET 加密 Store 尚未完成。

## 2. 顺序与边界

```text
prepare(candidate): stage exact AgentActivityRequest Artifact
        |
external admission atomically acquires Claim
        |
execute(claim, request_ref):
  verify request + CLAIMED Store projection
  build context/client/Tool handler/prompt before RUNNING
  start Claim and renew its lease in a bounded heartbeat thread
  after each closed turn, atomically reference an exact checkpoint + dependencies
  run Core Loop through durable provider and Tool boundaries
  stop heartbeat before terminal transition
  stage final response Artifact
  commit exact provider response refs + manifest + receipts atomically
```

`resume(claim, request_ref)` 只接受当前 Store 中同 owner/token/fencing 的 `RUNNING` Attempt。
它先加载 append-only 的最新 checkpoint，再恢复 provider history、authorization lineage、
collector receipt prefix、Tool 去重集合、下一轮 messages、累计 usage 和有界 Context 状态；恢复
完成并同步续租后，才允许下一次 provider 调用。

request 必须在 admission Claim 前 stage，因此敏感 task/context 不进入 Event、Attempt metadata
或远程控制消息。执行前重读 Artifact，并要求 Run/Node/Attempt/attempt number/request digest/
definition/agent name 与 Claim 以及当前 `CLAIMED` projection 完全一致。
本地 reference Store 不能加密 SECRET，因此 executor 在写 request Artifact 前直接拒绝由
SECRET input descriptor 推导出的 request；生产实现必须改用 deployment-managed 加密 Store。

prompt、context factory、provider client、Tool handler 和 user input 都在 `start_claim()` 前构造。
配置错误不会调用 provider，也不会把 Attempt 伪装为已运行。同一 executor 内为 attempt ID
设置 non-blocking execution slot，第二个并发 Loop 立即失败。

## 3. 运行与终态

- Claim 开始后先同步续租，再按 `heartbeat_interval_seconds` 后台续租；interval 至少 10ms，
  且不得超过 renewal window 的一半；renewal window 最长一小时。
- heartbeat 在 provider 阻塞期间仍工作，并在任何终态写入前停止；heartbeat 失败时禁止成功。
- provider client 只在严格解码成功并提交本地 history 后记录实际 MODEL_RESPONSE
  `ArtifactRef`。终态要求 manifest 中每个 provider receipt 恰好对应一个可重读 ref，且禁止
  注入额外的 raw provider response ref。
- 用户可见 response、exit reason、turns 和 usage 写入独立 SENSITIVE/继承分类的 JSON
  Artifact；NodeResult 只保存 refs 与有限数值 metrics。
- 只有 `CURRENT_TASK_DONE` 可以走 verified success。`INTERRUPTED` 记为 CANCELLED，其他已知
  非成功 exit 记为 FAILED。

模型调用开始后的未知异常、heartbeat 失败或终态 staging/提交失败都会保守尝试把父 Attempt
置为 `OUTCOME_UNKNOWN`。若 Store 终态已经提交，该尝试不能覆盖 SUCCEEDED。公开异常只包含
固定 reason code，不保留 deployment callback/provider 诊断异常链。

## 4. 安全轮次 checkpoint

checkpoint 只在 provider receipt 以及该轮所有 ToolReceipt 均已闭合后、下一 provider 调用前
生成。内容是至少 SENSITIVE 的 canonical Artifact，并绑定 request/definition、轮次、前驱
checkpoint、system prompt digest、Tool schema digest、provider route/采样配置和完整 receipt
prefix。主 Store 的 `agent_turn_checkpoints` 表按 Attempt/turn 只增不改；提交时再次校验当前
lease、owner、claim token 和 fencing token，并要求 `turn = previous + 1`。

checkpoint Artifact、request Artifact 和此前每个 provider response Artifact 在同一 SQLite
事务中进入 GC 引用索引。这样不会出现“checkpoint 指针尚在，但恢复依赖被当成孤儿清理”的
状态；若 GC 已先 claim 任一依赖，checkpoint 提交失败，Loop 不会进入下一轮。

恢复不重放已闭合 Tool 或 provider 调用。崩溃发生在安全 checkpoint 之后、下一 provider
调用之前时，可以精确继续；崩溃发生在 provider/Tool 调用中或 checkpoint 提交前时，仍按
unknown outcome 处理，不能猜测重试。

## 5. 明确不承诺

- 不会从 Store 导出 bearer；新进程必须由控制面安全取得当前 claim token，并通过
  `restore_claim()` 重建句柄；过期 Claim 不会因存在 checkpoint 而自动换 owner/fencing；
- 不阻止两个错误配置的独立进程同时持有同一 Claim bearer；Store checkpoint CAS、provider
  operation ledger 和 Tool operation key 会阻止已提交边界分叉，但生产控制面仍必须保证单一
  assignment owner；
- reference LocalArtifactStore 不支持 deployment-managed SECRET 加密；
- provider request payload 目前只有 digest/receipt，没有独立可审计的 request Artifact；
- reference Broker/Invoker 不证明 mTLS、runtime attestation、网络 deadline、egress policy 或
  外部 provider operation ledger；
- heartbeat 是本地 Store 续租，不是远程 worker session heartbeat。

因此该执行器适合验证本地组合与精确安全轮恢复，不应作为开放 remote Agent capability 的
依据。对抗证据见
[本地 Durable Agent Activity 执行器三轮审查](agent-activity-executor-adversarial-review.md)。
