# 本地 Durable Agent Activity 执行器三轮对抗性审查

## 范围与结论

审查覆盖 `DurableAgentActivityExecutor`、provider 结果 Artifact lineage、父 Claim lease、
安全轮次 checkpoint/resume 和终态失败收敛。两阶段共六轮攻击均已回归；结论适用于持有当前
Claim bearer 的本地跨进程恢复，不代表远程 Agent 已具备生产安全性。

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

## Checkpoint 阶段第一轮：崩溃窗口与未知子调用

攻击：分别把进程终止点放在 checkpoint 之后/下一 provider 之前、provider 返回后、Tool
terminal 后，以及让动态 Tool 返回 `OUTCOME_UNKNOWN` 后使 Core Loop 正常 `EXITED`。

发现与修复：

1. checkpoint 只在 provider receipt 和该轮全部 ToolReceipt 闭合后提交；下一 provider 调用
   发生前必须先完成 checkpoint，否则 Loop fail closed；
2. 恢复从上一个闭合 prefix 继续，provider operation ledger 和稳定 Tool operation key 负责
   replay 已提交的下一轮子操作，调用中 unknown 仍会阻止继续；
3. 非成功父终态现在也会 finalize collector 并重读每个 child ToolReceipt；缺失、错绑或
   `OUTCOME_UNKNOWN` 一律把父 Attempt 收敛为 `OUTCOME_UNKNOWN`，不再伪装成普通 FAILED；
4. 旧 summary checkpoint 继续只用于兼容诊断，不会被提升为精确恢复事实。

## Checkpoint 阶段第二轮：分叉、fencing、GC 与迁移

攻击：用相同 Attempt/turn 提交不同 checkpoint、伪造更高 fencing、让两个 provider turn 返回
完全相同 bytes、尝试 GC 已提交 checkpoint/request/response，并从 schema 9 升级或删除新表。

发现与修复：

1. Store 在同一事务校验 RUNNING、owner、claim token、fencing、未过期 lease 和连续前驱；
   同轮只允许完全相同的幂等重放，任何内容分叉都返回固定 conflict；
2. provider response 按有序调用保留，允许不同 turn 指向相同 content digest，避免把合法的
   相同模型输出误判为重放攻击；
3. checkpoint、本次 request 和此前全部 provider response refs 在 checkpoint 指针提交事务中
   一并进入 Artifact GC 引用索引；GC 先 claim 时提交失败，提交先完成时 GC 不能再 claim；
4. schema 10 migration 是事务性的，启动时要求 checkpoint ledger 精确列集合，缺表或残缺表
   fail closed。

## Checkpoint 阶段第三轮：配置漂移、篡改与敏感数据

攻击：重启时改变 max turns/system prompt/Tool schema/provider route 或采样参数；在序列化
ArtifactRef 中加入隐藏字段；篡改 session/principal binding；向 checkpoint 内容注入 secret
marker 并扫描 domain/provider SQLite。

发现与修复：

1. checkpoint 绑定 runtime configuration digest；任一影响下一 wire request 的配置改变都会在
   heartbeat/provider 前拒绝恢复；
2. Loop state、provider state、manifest 和 ArtifactRef 都要求 canonical/有界/精确 schema，
   session、agent、principal 和 request/definition/producer identity 必须一致；
3. provider state 的 ArtifactRef 禁止未知字段，response ref 还逐项绑定 receipt 的 content/ref
   digest、authorization lineage 和 sensitivity；
4. raw messages/history/tool data 只存在于至少 SENSITIVE 的 checkpoint Artifact；控制数据库只
   保存 digest、ref 和有限身份字段，异常与 `repr` 不携带 payload；
5. LocalArtifactStore 仍不加密，因此 SECRET request 在该组合入口继续被拒绝。

## 回归证据与剩余风险

关键测试：

- `test_success_commits_payload_free_verified_terminal`
- `test_provider_failure_becomes_outcome_unknown_without_leak`
- `test_terminal_failure_after_provider_becomes_outcome_unknown`
- `test_blocking_provider_call_keeps_parent_lease_alive`
- `test_heartbeat_failure_prevents_false_success`
- `test_same_attempt_cannot_enter_two_local_loops`
- `test_missing_or_substituted_provider_artifact_fails_closed`
- `test_safe_turn_checkpoint_resumes_before_next_provider_call`
- `test_resume_rejects_runtime_configuration_drift`
- `test_checkpoint_same_turn_is_append_only_and_claim_fenced`
- `test_equal_provider_payloads_preserve_ordered_checkpoint_chain`
- `test_checkpoint_payload_stays_outside_control_databases`
- `test_unknown_child_tool_outcome_cannot_become_known_parent_failure`
- `test_version_ten_migration_creates_agent_checkpoint_ledger`
- `test_current_schema_cannot_omit_agent_checkpoint_ledger`

剩余风险没有被测试结果掩盖：跨进程 execution slot 仍依赖控制面单 assignment；新进程必须
安全取得未过期 bearer，过期 Claim 不会自动接管；provider request Artifact、SECRET 加密
Store、远程 attestation/assignment ownership 和生产网络 deadline 仍未完成。因此
`durable_result_recovery_ready` 为 true，而 `production_security_ready` 保持 false。
