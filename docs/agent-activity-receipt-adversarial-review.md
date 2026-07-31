# Agent Activity Receipt 三轮对抗性审查

> 审查对象：`AgentActivityReceipt`、Legacy Agent terminal commit/replay、
> `DurableRunStore.get_agent_activity_receipt()`
>
> 结论：本阶段范围通过。该结论不代表远程 Agent Activity 已实现，也不代表内部工具
> 副作用 exactly-once。

## Round 1：隐私、Schema 与证据越权

攻击问题：

- 能否把 task、prompt、response、异常消息、工具参数或凭据写入 receipt；
- 能否用自由文本 activity/exit 字段绕过 Event 元数据边界；
- 能否用 Python `bool == 1`、畸形 enum 或未知字段绕过 exact schema；
- 能否把“Agent 正常返回”提升为“内部工具副作用 verified”。

发现与修正：

1. `schema_version=True` 原本会与整数 `1` 相等；已改为严格拒绝 bool 和非 int。
2. enum 构造的畸形不可哈希值可能抛出裸 `TypeError`；已统一转换为 receipt schema error。
3. `activity_name` 和 `exit_reason` 改为最长 128 字符的安全代码；receipt 只包含 identity、
   计数和 digest。
4. 成功必须为 `runtime_observed`，unknown/abandoned 必须为 `unverified`。
5. `internal_tool_receipts_complete=true` 至少要求 runtime-observed 且 receipt digest 数
   与观察工具结果数完全一致；Legacy 无条件保持 false。

验证：

- `test_round_trip_is_canonical_and_payload_free`
- `test_success_and_uncertain_statuses_have_conservative_evidence`
- `test_internal_receipt_completeness_requires_exact_coverage`
- `test_unknown_fields_and_non_identifier_activity_name_are_rejected`
- `test_canonical_result_digest_rejects_non_json_and_nan`

Round 1 结论：通过。receipt 没有扩大持久化敏感载荷面，也没有升级内部副作用证据。

## Round 2：完整性、并发与降级攻击

攻击问题：

- receipt 是否真的绑定 durable request，而不是不存在的 Attempt metadata；
- Event、Attempt 与 idempotency 能否来自不同并发 snapshot；
- 已完成 Attempt 能否与仍在进行或结果不同的 idempotency record 拼接；
- 删除一个 receipt 字段、篡改 digest、删除整个 terminal Event 是否会退化成“旧数据”；
- receipt 的 Artifact digest 是否与 Attempt result 精确同序。

发现与修正：

1. 初版错误地从 `Attempt.metadata.request_hash` 取绑定，但真实来源是
   `idempotency_records`；已改为读取 durable idempotency record。
2. 同一连接处于 autocommit 时多个 `SELECT` 不保证同一 snapshot；已显式 `BEGIN`
   只读事务，在一个 WAL snapshot 中读取 Event、Attempt 和 idempotency。
3. 已增加 `IdempotencyStatus.COMPLETED` 与 idempotency result/Attempt result 规范 JSON
   完全一致校验。
4. 历史兼容现在只允许“完整 terminal Event + terminal Agent Attempt + completed
   idempotency result 一致，但两个 receipt 字段都没有”。缺 Event、仅缺一个字段、
   内容/identity/digest 错绑全部 fail closed。
5. Store 同时校验 receipt digest、request/result digest、effect/status、Event type 和
   两组 Artifact digest 的精确顺序。

验证：

- `test_receipt_payload_tamper_fails_closed`
- `test_partial_receipt_fields_fail_closed`
- `test_missing_terminal_event_is_not_legacy_compatibility`
- `test_idempotency_result_mismatch_fails_closed`
- `test_receipt_reads_one_sqlite_snapshot`

Round 2 结论：通过。查询不会拼接跨时刻 durable facts，也不会把部分损坏解释为历史兼容。

## Round 3：崩溃窗口、重放与兼容

攻击问题：

- 终态 replay 能否只信 projection、绕过存在但损坏的 receipt；
- 非幂等执行失败能否被误记为已观察成功；
- `complete_activity` 提交成功但响应丢失时，能否在 receipt 丢失后仍确认成功；
- 历史 receiptless Attempt 是否被破坏性地拒绝；
- replay 是否会再次调用 Agent runner。

发现与修正：

1. Legacy terminal replay 原本不读取 receipt；现改为：receipt 存在必须完整验证，完全
   不存在才走历史兼容，任何校验错误都阻断 replay，且 runner 不会执行。
2. 非幂等异常、异常 exit、结果落盘失败和 terminal commit 失败统一进入
   `OUTCOME_UNKNOWN + unverified`，不声明内部工具证据完整。
3. 新增“提交成功/响应丢失”处理：fresh projection 只能作为候选事实；adapter 必须再
   读到非空、验证通过的新 receipt 才确认成功，禁止借历史兼容降级。
4. 旧的完整 receiptless Event 仍可恢复，且 request hash 继续由 idempotency record
   约束；同一请求 replay 不会调用 runner。

验证：

- `test_legacy_success_persists_verifiable_agent_receipt`
- `test_known_read_only_failure_has_runtime_observed_receipt`
- `test_unknown_write_receipt_never_claims_side_effect_evidence`
- `test_terminal_commit_failure_records_unverified_unknown_receipt`
- `test_post_commit_response_loss_requires_durable_receipt`
- `test_new_post_commit_completion_cannot_downgrade_to_legacy`
- `test_fully_absent_receipt_remains_legacy_compatible`

Round 3 结论：通过。已覆盖 commit 前失败、事务回滚后 unknown、commit 后响应丢失和终态
replay 四类关键窗口。

## 剩余边界

- 当前安全 remote control/worker 参考链仍只证明 `tool` Activity；不能因为本 receipt
  契约存在就开放 `agent` capability。
- Legacy Agent 的 `internal_tool_receipts_complete` 始终为 false。逐工具 receipt 接入
  是后续工作。
- task/context 目前只进入 canonical request digest；将远程 Agent 输入物化为受 grant
  约束的不可变 Artifact 是后续独立阶段。
- receipt digest 是完整性绑定，不是抗数据库管理员篡改的数字签名。生产部署仍需要独立
  OS identity、只读查询身份、备份和可选外部签名/透明日志。
- 外部系统与 SQLite 没有共同事务；本功能不承诺通用 exactly-once。
