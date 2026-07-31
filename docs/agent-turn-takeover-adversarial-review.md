# Agent 安全轮接管：三轮对抗性审查

## 范围与不变量

本审查只评价过期 `RUNNING` Agent Claim 从已闭合 turn checkpoint 换 owner/fencing 的本地参考
协议。受信恢复控制器持有旧 Claim 完整绑定并显式调用
`takeover_expired_agent_claim()`；系统不会自动把任意过期工作交给新 Worker。

必须始终成立：

1. 同一旧 fencing 最多产生一个新 Claim；owner/token/fencing 在一个 SQLite 事务中切换；
2. 新 Claim 只能采用同 Attempt/request 的已提交安全 checkpoint；
3. 旧 Worker 在事务提交后不能续租、追加 checkpoint 或提交父终态；
4. deadline 和未知 Tool/provider 尾部优先于“为了可用性继续执行”；
5. takeover ID、旧 bearer、模型内容和 Tool 内容不进入普通 takeover payload；
6. 响应丢失后的相同接管请求返回同一个仍有效 Claim，而不是产生第二次 fencing 跳变。

## Round 1：并发、崩溃与 ABA

攻击问题：

- 两个恢复控制器同时接管同一过期 Claim，是否都会成功？
- idempotency 行已换 owner，但 Attempt/Event 未提交时崩溃，是否留下半状态？
- 接管事务成功但调用响应丢失，重试是否再换一次 bearer？
- 接管后尚未生成新 checkpoint 再次宕机，能否安全采用更早 fencing 的同一安全轮？

结论：通过。

- `BEGIN IMMEDIATE`、旧 owner/token/fencing/lease 的条件更新和 Attempt projection CAS 形成唯一
  线性化点；并发测试只有一个赢家。
- `agent_turn_takeover.after_idempotency` fault seam 证明 idempotency、Attempt 和 Event 同时回滚。
- `takeover_id` 只保存摘要，并与旧/新 Claim、request 和 checkpoint 共同形成 binding digest；
  相同请求在新租约仍有效时返回相同 bearer 和 Event。
- Attempt takeover binding 记录 checkpoint fence、expired fence 和 takeover fence。连续两次
  接管既支持采用上一个 owner 新写的 checkpoint，也支持采用未改变的更早安全 checkpoint；
  fencing 仍逐次递增，不发生 ABA。

对应测试：

- `test_concurrent_takeovers_have_one_winner`
- `test_takeover_fault_rolls_back_claim_and_event`
- `test_takeover_retry_returns_same_live_claim_and_event`
- `test_repeated_takeovers_preserve_provider_lineage_segments`

## Round 2：未知尾部、deadline 与旧 Worker

攻击问题：

- checkpoint 之后已经 reserve/start 的 Tool 子调用会不会被忽略后重放？
- execution/heartbeat/run deadline 已到时，接管是否绕过 timeout scanner？
- 原租约尚有效时，恢复控制器能否提前抢占？
- 接管后旧 Worker 是否还能用原 bearer 写 checkpoint、续租或提交终态？
- checkpoint 之后已签发但未闭合的 provider grant 是否可能被新 owner 重复调用？

结论：在声明边界内通过，in-flight provider grant 保持显式限制。

- 任一 `scheduled`/`running` Agent Tool child 都阻止接管；控制面必须先通过既有 child/父恢复
  路径收敛，不能把“未看到结果”当作“未执行”。
- 接管先检查旧 heartbeat 在内的持久 deadline，换发租约时再以 run/execution/新 heartbeat
  deadline 截断；到期任务只能走 timeout recovery。
- live lease 明确返回 `agent_turn_takeover_not_expired`。
- 主 Attempt 与 idempotency 行同时换 token/fencing；现有 renew/checkpoint/terminal 方法均重验
  owner、bearer、fencing 和有效租约，所以旧句柄失败关闭。
- provider logical invocation 唯一键不包含 Worker，故新 owner 不会获得第二个相同逻辑调用；
  但参考 Broker 不导出旧 grant bearer。若旧进程在 checkpoint 后已经签发 grant，新进程会失败
  关闭，而不是声称精确恢复。生产 gateway 仍须实现稳定 operation ID 的可信查询/接管协议。

对应测试：

- `test_active_tool_tail_blocks_checkpoint_takeover`
- `test_elapsed_execution_deadline_blocks_checkpoint_takeover`
- `test_live_or_checkpointless_claim_cannot_be_taken_over`
- `test_expired_claim_takeover_resumes_with_new_owner_and_fence`

## Round 3：篡改、越权与数据边界

攻击问题：

- 攻击者替换 checkpoint digest/turn/fence 或伪造 Attempt takeover metadata，executor 是否继续？
- 新 owner 能否把旧 receipts 伪装成自己的 authorization lineage？
- 不同 takeover ID、新 owner、旧 bearer 或 request 是否能命中原幂等结果？
- Fleet/hierarchy/capacity 已变化时能否借接管绕过 admission？
- bearer、takeover ID 或模型内容是否泄漏到 repr/普通 Event 字段？

结论：通过。

- resume 同时重验当前 Claim、checkpoint ledger、Attempt metadata、append-only takeover Event 及
  Event 内投影；篡改任一 binding 以固定 `agent_activity_recovery_unavailable` 失败。
- provider checkpoint v2 保存当前 lineage 起点。接管只清空“当前段”的授权摘要，旧段 receipt
  仍由 typed manifest 逐条绑定 response Artifact；下一 checkpoint 可含多个合法 lineage 段。
- binding digest 覆盖旧 owner/bearer/fence、新 owner/fence、request、checkpoint 和 takeover ID
  摘要。相同 ID 的不同绑定不复用结果。
- 接管事务重验 hierarchy 活性、Fleet route/shard ownership，并对新 worker 计算排除被接管
  Attempt 后的 capacity；资源 reservation 不增加，因此不重复占用全局/Fleet名额。
- takeover metadata/Event 仅保存摘要、轮次和 fencing；repr 延续隐藏 Claim bearer。主 Store
  按既有控制面契约仍保存当前 bearer，必须与 Agent workspace/工具进程物理隔离。

对应测试：

- `test_tampered_takeover_binding_cannot_resume`
- `test_repeated_takeovers_preserve_provider_lineage_segments`
- 既有 checkpoint claim-fencing、provider receipt/ref、Store projection replay 与 metadata
  secret-key 测试共同覆盖其余边界。

## 最终判定

本地安全轮跨 owner 接管达到“可审计、原子、fenced、失败关闭”的参考实现标准，但不提升
`production_security_ready`：远程 Worker assignment attestation、跨数据库 provider grant
handoff、生产 gateway operation ledger 和加密 SECRET ArtifactStore 仍是部署前置条件。
