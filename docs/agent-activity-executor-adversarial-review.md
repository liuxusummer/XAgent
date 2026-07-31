# 本地 Durable Agent Activity 执行器三轮对抗性审查

## 范围与结论

审查覆盖 `DurableAgentActivityExecutor`、provider 结果 Artifact lineage、父 Claim lease 和
终态失败收敛。三轮范围内攻击均已回归；结论只适用于单进程本地参考组合，不代表远程 Agent
已具备生产安全性。

## 第一轮：组合顺序、终态和错误泄露

攻击：使用错误 prompt、foreign request ref、provider 异常，以及 provider 已返回后在
`agent_terminal.artifacts_staged` 注入故障。

发现与修复：

1. prompt/client/handler/context 全部前置验证，失败时 Attempt 保持 `CLAIMED`，provider 调用
   次数为零；
2. foreign producer/request binding 在 start 前拒绝；
3. provider 或 Loop 的未知异常把已开始的父 Attempt 置为 `OUTCOME_UNKNOWN`；
4. provider 已执行而终态 staging/Store 未被证明成功时同样置为 unknown，不能遗留虚假的
   `RUNNING`；
5. deployment callback 与 provider 原始异常在离开 catch scope 后转换为固定错误，异常
   `repr`、context 和 SQLite 均不含注入的 secret marker；
6. terminal 默认使用 Scheduler 的权威 clock，避免测试/部署时不同 clock 域把有效 lease
   误判为过期。

## 第二轮：长调用、续租失败与终态竞态

攻击：让 provider 调用持续超过 heartbeat interval；并在第一次同步续租成功后令后台续租
失败，再让 provider 返回一个看似成功的 response。

发现与修复：

1. start 后同步续租，随后由 attempt-scoped daemon heartbeat 在线程中续租；
2. blocking provider 期间观察到至少第二次 renewal，成功后先停止并 join heartbeat，再写
   终态，避免 renew/complete 并发；
3. heartbeat 只保存失败 bit，不保存底层诊断异常；任何失败都阻止 success 并收敛为
   `OUTCOME_UNKNOWN`；
4. interval、renewal window 和比例有硬边界，防止无界 lease 或忙循环配置；
5. final result 的分类继承整个 manifest 的最高分类，不能被 request-only 分类降级。

## 第三轮：重复执行和 provider 证据注入

攻击：同一 executor 用完全相同的 Claim 并发进入两次 Loop；向 terminal bundle 注入一个
未出现在 manifest receipt lineage 中的额外 raw MODEL_RESPONSE Artifact；仅提供 receipt
digest 而不提供实际 response Artifact。

发现与修复：

1. attempt execution slot 使同一进程的第二次 Loop 在 provider 前失败，阻塞调用最终只产生
   一次 provider invocation 和一个父终态；
2. provider client 返回 detached、ordered 的真实 response Artifact refs；
3. terminal 重读每个 Artifact，校验完整 ref identity digest、content digest、kind、media
   type、producer 和 sensitivity；
4. raw provider ref 集合必须与 manifest receipt 集合完全相等，缺失、替换或额外注入全部
   fail closed；
5. success Event、Attempt 和 receipt 仍由单个 Store transaction 接纳，payload 正文只在
   Artifact 中。

## 回归证据与剩余风险

关键测试：

- `test_success_commits_payload_free_verified_terminal`
- `test_provider_failure_becomes_outcome_unknown_without_leak`
- `test_terminal_failure_after_provider_becomes_outcome_unknown`
- `test_blocking_provider_call_keeps_parent_lease_alive`
- `test_heartbeat_failure_prevents_false_success`
- `test_same_attempt_cannot_enter_two_local_loops`
- `test_missing_or_substituted_provider_artifact_fails_closed`

剩余风险没有被测试结果掩盖：跨进程 execution slot、精确 Loop checkpoint/resume、provider
request Artifact、SECRET 加密 Store、远程 attestation/assignment ownership 和生产网络
deadline 仍未完成，所以两个 readiness 属性保持 false。
