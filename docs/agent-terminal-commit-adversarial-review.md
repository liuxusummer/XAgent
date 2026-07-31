# Agent 终态原子提交三轮对抗性审查

## 范围

审查对象为 `DurableAgentTerminalCommitter`、verified Agent Store completion、
ToolReceipt Artifact 和 LocalArtifactStore 并发安装。目标是证明“证据不完整时不能成功，
成功时所有数据库事实一致，重放不会产生第二个终态”。

## 第一轮：并发、崩溃窗口与幂等

攻击：八个线程用同一 Claim、collector 和内容同时提交。原实现的 LocalArtifactStore 在
`lstat -> os.replace` 之间存在 TOCTOU；多个 writer 会依次覆盖同一 digest 路径，返回不同
mtime 的 `ArtifactRef`，使 NodeResult digest 分叉并触发幂等冲突。

修复：临时文件 fsync 后以原子 no-clobber hard link 安装；输掉竞争的 writer 验证并复用
既有 inode。并发测试现在比较完整 `ArtifactRef`，八个 Agent 提交只保留一个父终态 Event。

另在两个故障点注入中断：

- Artifact 已 stage、Store 尚未提交：父 Attempt 保持 `RUNNING`，重试成功；
- Store 已提交、响应尚未返回：重试重读 exact receipt，标记 replay，不产生重复 Event。

## 第二轮：证据替换、数据泄露与事务撕裂

攻击包括：缺失 provider receipt、伪造 request producer、降低结果敏感级别、引用其他 Run
的 Artifact、只伪造 provider ref digest、替换/额外注入 raw provider response Artifact、
篡改已提交 receipt，以及在 `complete.after_idempotency` 强制数据库异常。

修复后 terminal 必须重读实际 provider Artifact，且 raw provider ref 集合与 manifest
receipt lineage 精确相等；所有替换均 fail closed。Store 故障会回滚 idempotency、Attempt、
Node 和 Event 的整个事务。随后可用原 Claim 和相同 bundle 重试。

审查同时发现内联 `output` 和自由文本 metrics 会绕过 Artifact 边界。最终 API 删除内联
输出，只允许正文先持久化为 Artifact；metrics 只接受最多 64 个安全名称的有限数值或
布尔值。

安全扫描未发现新增脚本风险；新增错误只暴露固定 reason code，repr 不包含 NodeResult、
task、prompt、response、工具参数、路径、bearer 或凭据。

## 第三轮：越权声明、生命周期与容量

攻击：调用方伪造 Agent 名称，或在多节点 DAG 的单个 Agent 成功时要求直接把 Run 标记为
`COMPLETED`。修复后 Agent 名称只能来自 request，verified Store completion 强制
`run_status=None`，全局 Run 状态只由 Scheduler reconcile 推导。

终态 Event/Attempt 中的 manifest 和 ToolReceipt 均保存完整 canonical `ArtifactRef`；GC
reachability 测试确认二者不会作为孤儿被回收。manifest、receipt、引用数、metrics 和
NodeResult 均有固定上限，成本不随聊天正文长度增长。

## 结论与剩余风险

三轮审查覆盖了并发 duplicate、pre/post-commit crash、事务 rollback、证据缺失、引用替换、
分类降级、跨 Run 引用、receipt 篡改、内联数据泄露和 GC reachability。该组合边界适合后续
production adapter 接线，但它仍不证明远程 sandbox attestation、provider 外部账本或
进程内 Session history 的精确恢复；这些能力必须在独立阶段完成后才能把
`production_security_ready` 改为真。
