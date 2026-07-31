# Agent Activity 终态原子提交契约

## 1. 目标

一个成功的 Agent Activity 只有同时满足以下事实才可被控制面接纳：

1. 精确的 `AgentActivityRequest` Artifact 可读且绑定当前 Claim；
2. 每次 provider 调用都有有序、typed、持久响应引用的 receipt；
3. 每次工具结果都有确定性 child operation key 和 Store 中的 `ToolReceipt`；
4. execution manifest、独立 ToolReceipt Artifact 和规范 NodeResult 已落盘；
5. idempotency record、Attempt、Node、终态 Event 和 `AgentActivityReceipt` 在同一
   SQLite 事务中提交。

`DurableAgentTerminalCommitter` 是上述组合边界。它不是新的 Agent runtime，也不会启用
远程 `agent` capability。

## 2. 提交顺序

```text
verified request Artifact
        + complete runtime collector
        + durable child ToolReceipts
                    |
                    v
stage immutable ToolReceipt Artifacts + execution manifest
                    |
                    v
build payload-free NodeResult + AgentActivityReceipt v2
                    |
                    v
one Store transaction:
idempotency + Attempt + Node + Event + receipt
                    |
                    v
re-read exact durable receipt, then Scheduler reconcile
```

ArtifactStore 与 SQLite 无法共享事务，因此采用“不可变 Artifact 先行、单事务引用后置”。
Artifact 阶段失败时不写终态；Artifact 成功而数据库失败时只留下可由 GC 回收的孤儿；数据库
响应丢失时，只能通过重新读取并完整验证完全相同的 receipt 确认成功。

## 3. 接纳约束

- success 只接受 `CURRENT_TASK_DONE`，Agent 名称只能来自已验证 request；
- 至少观察到一次 provider 调用，provider/tool receipt lineage 必须完整；
- ToolReceipt 必须从 Store 重读，不能信任调用方提供的业务结果；
- execution manifest 和每个 ToolReceipt 都写为 content-addressed Artifact；
- 外部结果 Artifact 必须通过完整性校验、不得低于整个 manifest 的最高分类、不得来自其他
  Run/Node/Attempt，也不得把自由 metadata 带入 Event Store；
- provider receipt 必须绑定一个实际可重读的 raw MODEL_RESPONSE Artifact；raw provider
  ref 集合必须与 receipt lineage 精确相等，缺失、替换和额外注入均拒绝；
- NodeResult 只包含 ArtifactRef、ToolReceipt ArtifactRef 和数值/布尔 metrics，不接受
  Agent 输出正文、prompt、response、工具参数、错误文本或凭据；
- Store 的 verified completion 路径只允许结束当前 Attempt/Node。Run 终态必须由
  Scheduler 根据整个 DAG 推导，调用方不能直接声明 Run 完成。

所有集合、文本和 JSON 都有硬上限。任一证据缺失、错绑、重复、降级或不可读均 fail
closed，且不会把父 Attempt 写成成功。

## 4. 并发与恢复

LocalArtifactStore 对同一 digest 使用同目录临时文件和原子 no-clobber link。并发 writer
只有一个能安装目标 inode，其余 writer 验证并复用同一文件，因此相同内容返回完全相同的
`ArtifactRef`，不会因 mtime 竞争破坏终态幂等性。

同一 Claim 的并发重复提交最终只产生一个父 `attempt.succeeded` Event；若已存在的 receipt
与本次构造结果不同则拒绝。Scheduler reconcile 失败不撤销已提交事实，而是返回
`reconcile_pending=true`，由维护循环继续收敛。

## 5. 明确不承诺

- 不声称 ArtifactStore 与 SQLite 跨介质 exactly-once；
- 不恢复进程崩溃时尚未持久化的 Session/provider 内部 history；
- 不验证 provider gateway 的外部透明日志或远程 sandbox attestation；
- 不把完整 receipt coverage 等同于外部副作用 exactly-once；
- production remote Agent adapter 尚未接入，因此 `production_security_ready=false`。

具体攻击、修复和回归证据见
[Agent 终态三轮对抗性审查](agent-terminal-commit-adversarial-review.md)。
