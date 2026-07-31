# 本地 Durable Agent Activity 执行器契约

## 1. 范围

`DurableAgentActivityExecutor` 把已经完成 admission 的单个 Agent Claim 组合到 Core Agent
Loop。它复用现有的 request Artifact、provider gateway client、动态 Tool executor、execution
evidence collector 和原子终态提交器，不改变默认 CLI/Web，也不把 `agent` capability 开放给
远程 Worker。

该实现是本地参考组合，不是生产远程 runtime。`production_security_ready` 和
`durable_result_recovery_ready` 均固定为 false：进程退出后尚不能从持久 checkpoint 精确恢复
Loop history、collector open observation 和下一 provider invocation。

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
  run Core Loop through durable provider and Tool boundaries
  stop heartbeat before terminal transition
  stage final response Artifact
  commit exact provider response refs + manifest + receipts atomically
```

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

## 4. 明确不承诺

- 不能跨进程接管同一个 RUNNING Agent Attempt；新进程也不能恢复精确聊天历史；
- 不阻止两个错误配置的独立进程持有同一 Claim bearer；生产控制面必须保证单一 assignment
  owner，并增加持久 checkpoint/接管协议；
- reference LocalArtifactStore 不支持 deployment-managed SECRET 加密；
- provider request payload 目前只有 digest/receipt，没有独立可审计的 request Artifact；
- reference Broker/Invoker 不证明 mTLS、runtime attestation、网络 deadline、egress policy 或
  外部 provider operation ledger；
- heartbeat 是本地 Store 续租，不是远程 worker session heartbeat。

因此该执行器适合验证组合语义和继续建设恢复协议，不应作为开放 remote Agent capability 的
依据。对抗证据见
[本地 Durable Agent Activity 执行器三轮审查](agent-activity-executor-adversarial-review.md)。
