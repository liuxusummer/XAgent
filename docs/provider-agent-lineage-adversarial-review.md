# Provider Receipt 与 Agent Lineage 三轮对抗性审查

> 审查对象：`ProviderInvocationReceipt`、`ProviderInvocationResult`、
> `AgentProviderReceiptBinding`、`AgentActivityExecutionManifest` schema v2 和
> `AgentExecutionManifestArtifactStore`。
>
> 结论：payload-free provider receipt 与 durable Agent manifest 的组合契约通过；
> 真实远程 Agent Loop 尚未接线，生产 `agent` capability 继续关闭。

## Round 1：正文、凭据与 schema 注入

攻击：

- 将 prompt、response、bearer、API key、endpoint、异常或路径塞进 provider receipt；
- 用 unknown field、错误 digest、bool-as-int、语义子类或非规范 manifest 绕过解析；
- 让 `ProviderInvocationResult` 携带与 receipt 不同的 response/ref；
- 把 sensitive/secret provider output 写入更低分类的 Agent manifest。

发现与修复：

1. receipt 是 exact-field、payload-free schema，只保存父 identity、grant/route/request、
   payload/response/ref digest、分类、completion mode 和有界 recovery evidence。
2. receipt/evidence/result 使用精确实现类型；可覆写序列化或 digest 语义的子类被拒绝。
3. result 构造与 `validate_binding()` 同时比较 grant、route、payload、response、
   canonical ArtifactRef digest 和分类；反序列化后的等价值可验证，不依赖对象身份。
4. manifest 只允许 sensitive/secret，且分类不得低于 provider response；secret response
   会把 sensitive request 对应的 manifest 提升为 secret。
5. nested provider parser 的内部错误统一转换为固定
   `invalid_provider_receipt_binding`，不保留外部异常链。

证据：

- `test_invoke_consumes_once_and_hides_response_from_repr`
- `test_v2_binds_ordered_provider_receipts_canonically`
- `test_provider_parent_artifact_and_sensitivity_are_exact`
- `test_malformed_nested_provider_receipt_is_sanitized`

Round 1：通过。

## Round 2：重排、替换与伪完整性

攻击：

- 交换两个合法 provider receipt；
- 重复 grant，或替换一个合法 response digest；
- 把另一父 Agent、request 或 request Artifact 的 receipt 拼接进当前 manifest；
- receipt 数少于 observed count，却声明完整；
- 把没有 durable response Artifact 的首次调用提升为可恢复 Agent 证据；
- 用 v1 manifest 注入 provider 字段或伪造完整性。

发现与修复：

1. binding sequence 必须从 1 连续，等于 receipt invocation index；turn 非递减且不得超过
   Agent turn count。
2. grant ID 与 receipt digest 必须唯一，顺序和逐项内容必须与外部 receipt 集合精确相等。
3. 每项必须绑定同一父 Run/Node/Attempt、request 和 request Artifact digest。
4. complete 只在 receipt 数精确等于 observed count 时成立；
   `has_complete_provider_receipt_lineage` 和 `require_complete=True` 提供显式接纳门禁。
5. manifest 要求非空 response ArtifactRef digest；没有 result store 的 provider 返回
   不能被提升为 durable lineage。
6. v1 保持原 exact-field bytes，不接受任何 provider 字段或 provider 完整声明。

证据：

- `test_provider_reorder_duplicate_and_substitution_fail_closed`
- `test_provider_completeness_requires_exact_coverage`
- `test_v1_manifest_remains_exact_and_cannot_claim_providers`
- `test_provider_parent_artifact_and_sensitivity_are_exact`

Round 2：通过。

## Round 3：崩溃重放、恢复证据与持久引用

攻击：

- completed commit 后响应丢失，重放时生成不同 receipt；
- COMPLETED recovery 与普通调用混淆，或把 COMPLETED evidence 放在非终项；
- 重排、重复或超量 recovery evidence；
- manifest Artifact metadata 与内容 schema 不一致；
- Store verify 失败后仍读取并接纳 manifest；
- 接收端只加载 manifest digest，不复核 provider receipt 集合。

发现与修复：

1. receipt 排除 issued/updated 时间等易变字段；Broker 从 grant、durable invocation record
   和有序 evidence 重构，并重验 journal 的 completed 状态、payload/response digest
   与 exact ArtifactRef；正常 replay 与 recovered replay 保持相同 receipt digest。
2. completion mode 由 journal evidence 推导：`recovered_completed` 必须且只能以一个末尾
   COMPLETED evidence 收敛；正常完成只允许 NOT_STARTED 前缀。
3. evidence sequence 连续、digest 唯一并共享 journal 的 16 项硬上限。
4. manifest v1/v2 metadata 分别精确绑定
   `agent_execution_manifest_v1/v2`；load 先 verify，再读 bytes 并复核 size/SHA/canonical
   内容。
5. load 可接收实际 provider receipt 集合，并用
   `require_complete_provider_receipts=True` 同时阻止 partial manifest。
6. AgentActivityReceipt 的 manifest digest 提交整个 provider 链；但接收端仍必须实际
   load，不能只验证 digest 字段存在。

证据：

- `test_completed_gateway_evidence_recovers_and_replays`
- `test_stage_and_load_preserve_sensitive_typed_binding`
- `test_failed_verification_never_reads_manifest_bytes`
- `test_wrong_artifact_kind_and_producer_are_rejected`

Round 3：通过。

## 残余边界

- 当前代码提供的是受信 Broker/runtime 的证据契约，不是 provider 的数字签名。
- Core Agent Loop 已能通过 fail-closed observer 收集显式 provider receipt；但生产
  gateway client、manifest staging 与 AgentActivityReceipt 原子提交尚未组合，远程
  `agent` capability 不得因此开放。
- reference Broker 不能证明部署 gateway 的 mTLS、attestation、跨主机共识或 upstream
  exactly-once；`production_security_ready` 继续固定为 false。
- response ArtifactRef digest 证明 receipt 当时绑定的引用身份；需要读取模型响应的审计
  入口仍必须从受信 journal/结果索引取得 exact ArtifactRef，并调用 ArtifactStore verify。
- manifest/receipt digest 不能抵抗同时控制 Store 与 ArtifactStore 的管理员；生产需独立
  OS identity、ACL、加密、密钥管理和可选透明日志。
