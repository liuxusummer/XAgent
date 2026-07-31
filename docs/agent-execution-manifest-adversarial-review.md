# Agent Execution Manifest 三轮对抗性审查

> 审查对象：`AgentActivityExecutionManifest`、`AgentToolReceiptBinding`、
> `AgentExecutionManifestArtifactStore`、`AgentActivityReceipt` v2 和 Store ref 校验。
>
> 结论：本阶段前置契约通过。远程 Agent capability 仍保持关闭。

## Round 1：Schema、隐私与分类攻击

攻击：

- 在 manifest 中塞入 task、response、工具参数、异常或 bearer；
- 用 unknown field、bool-as-int、NaN、非规范 JSON 或零字节 ref 绕过 parser；
- 将引用 secret request 的 manifest 降级为 sensitive/internal；
- Artifact verify 已失败时仍读取潜在恶意 bytes。

发现与修复：

1. manifest 采用 exact-field canonical JSON，只保存 identity、安全代码、计数和 digest。
2. request/manifest 单项与总大小均有界；ArtifactRef 显式要求非零且不超过 256 KiB。
3. `artifact_sensitivity` 只允许 sensitive/secret，并与 request Artifact 精确一致；
   stager 按该分类写入，消除 secret digest 元数据降级。
4. loader 将 verify 和 read 分开；verify 不为精确 `True` 时立即失败且不调用 read。

证据：

- `test_manifest_payload_is_exact_and_canonical`
- `test_manifest_inherits_secret_request_classification`
- `test_failed_verification_never_reads_manifest_bytes`
- `test_stage_and_load_preserve_sensitive_typed_binding`

## Round 2：合法 receipt 替换、重排与父子错绑

攻击：

- 交换两个合法 ToolReceipt；
- 重复一个 receipt 或 child Attempt；
- 把同一 Run 中另一个分支的合法 receipt 塞进当前 Agent；
- 改父 request 后继续复用旧 receipt；
- 用错误 request Artifact、definition 或 sensitivity 组合 manifest。

发现与修复：

1. sequence 必须从 1 连续，turn 必须非递减并位于父 turn count 内。
2. receipt digest、logical tool-call digest 和 child Attempt identity 均必须唯一。
3. 子 Tool Activity 必须使用由父 run/node/attempt/request/sequence 确定派生的 operation
   key；ToolReceipt 的 operation/idempotency digest 必须同时匹配。
4. 子 ToolReceipt 当前必须与父 Agent 位于同一 Run；跨 Run 需要独立 hierarchy proof。
5. `validate_request()` 同时验证 request canonical ArtifactRef、内容 digest、definition、
   parent identity 和 sensitivity。

证据：

- `test_order_duplicates_and_receipt_substitution_fail_closed`
- `test_parent_operation_and_request_artifact_bindings_are_exact`
- `test_child_operation_key_is_stable_and_parent_scoped`
- `test_round_trip_binds_ordered_tool_receipts_and_agent_receipt`

## Round 3：持久引用、恢复与兼容降级

攻击：

- v2 引用 ToolReceipt 但不保存 manifest；
- receipt 指向不存在、重复、错误 kind 或错误 producer 的 manifest ref；
- 只比较 manifest digest，不比较 Agent receipt 的 parent/request/exit/turn/count；
- 把旧 v1 count-only receipt 当成新 lineage proof；
- response 丢失后恢复到错误 Artifact。

发现与修复：

1. v2 有 ToolReceipt 时强制 `execution_manifest_digest`；完整标志也必须有 manifest。
2. digest 必须在 NodeResult result Artifact 中恰好出现一次；Store 校验 canonical ref、
   kind、media type、schema metadata、sensitivity 和 producer。
3. manifest 与 Agent receipt 双向校验 parent/request/exit/turn/observed count、完整标志和
   ToolReceipt digest 精确顺序。
4. v1 保持 exact-field 历史解析，但
   `has_manifest_bound_tool_receipt_lineage=false`；新远程门禁不得接受 v1。
5. content-addressed staging 幂等；load 重验 ArtifactStore verify、size、SHA 和 canonical
   bytes，避免 response-loss 恢复到同 digest 之外的内容。

证据：

- `test_v2_agent_receipt_requires_one_manifest_result_artifact`
- `test_v1_receipt_round_trip_remains_exactly_compatible`
- `test_store_binding_rejects_wrong_or_duplicate_manifest_refs`
- `test_wrong_artifact_kind_and_producer_are_rejected`

## 残余边界

- manifest 是受信 runtime 的有序观察，不是外部系统事务收据。
- manifest v2、Core Loop collector observer 与 reference provider client 已组成
  provider 子链；Tool 消费 Handler 已接线，但动态 child authority 与终态原子提交仍缺失。
- 当前本地 Legacy adapter 继续产生 v2、无 manifest、`complete=false` receipt；它不会被
  误升级为远程 Agent proof。
- Store receipt API 只验证 manifest ref，不读取 Artifact bytes；接纳执行证据的入口必须
  持有 ArtifactStore capability 并调用 loader。
- secure remote control/worker/Fleet 仍只发布 Tool capability。
