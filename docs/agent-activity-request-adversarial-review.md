# Agent Activity Request 三轮对抗性审查

> 审查对象：`AgentActivityRequest`、request Artifact staging/load、Artifact Broker
> 单次读取边界
>
> 结论：输入基础契约范围通过；远程 Agent capability 仍未开放。

## Round 1：隐私、分类与解析

攻击问题：

- task/context 是否进入 repr、Event metadata、ArtifactRef metadata、grant descriptor
  或异常链；
- unknown field、bool version、非规范 JSON、控制字符和超大载荷是否可绕过；
- 普通 Worker 是否能读取 sensitive/secret request；
- 指令前后空白是否被静默改写，导致 config digest 漂移；
- secret context descriptor 是否被降级封装进 sensitive request。

修正与证据：

- 除既有 Workflow source/control-plane definition 外，本边界只把 raw instruction 写入
  受分类保护的 Artifact bytes；repr 和 ref metadata 不含原文；
- exact schema + canonical bytes + 256 KiB/64 KiB 双重上限；
- 指令仅校验非空和边界，原样保存包括有意义的前后空白；
- malformed JSON/Unicode 异常使用固定错误码且不保留可能持有原文的 cause；
- request 以 sensitive 为最低分类并继承 context 的最高分类，Broker 必须显式允许该
  推导分类；
- LocalArtifactStore 产生的未加密 secret request 即使具备 SECRET 授权也拒绝下发；
- path-free descriptor 不含 URI 和 producer identity。

对应测试：

- `test_candidate_round_trip_is_canonical_and_path_free`
- `test_exact_schema_noncanonical_and_bool_version_fail_closed`
- `test_empty_oversized_and_control_instruction_are_rejected`
- `test_stage_is_deterministic_sensitive_and_metadata_safe`
- `test_broker_requires_explicit_sensitive_authority`
- `test_request_inherits_secret_context_classification`

Round 1：通过。

## Round 2：身份、权限与 grant 拼接

攻击问题：

- Worker 或 projection version 是否错误进入 durable 请求身份；
- claimed/running candidate 能否重新 staging；
- claim refs 与逻辑 binding 不一致时能否生成请求；
- request/config/Assignment/grants 能否交叉拼接；
- 64 个上下文 refs 加 request grant 是否造成协议超限。

修正与证据：

- 移除易变 `candidate_digest`；请求只绑定稳定 Attempt/request/definition/config/input；
- Worker 变化不改变 request bytes；
- 只接受 SCHEDULED、空 claim token、零 fencing/lease 的 Agent candidate；
- Attempt 自身也必须无 owner/lease/fencing/result/execution time，且 effect、operation、
  claim key 与 proposal 一致；
- 扁平 input refs 必须与有序 binding 完全一致；
- 畸形 binding/ref 结构只返回固定 reason code，不泄漏原始对象或异常链；
- `validate_runtime_binding()` 绑定 Assignment 的 path-free 身份；
- `validate_grant_descriptors()` 要求 request grant 恰好一个且上下文 grant 全集精确；
- 上下文上限从 64 收紧为 63，为请求 Artifact 预留第 64 个 input grant。

对应测试：

- `test_candidate_round_trip_is_canonical_and_path_free`
- `test_candidate_authority_and_input_mismatch_are_rejected`
- `test_empty_oversized_and_control_instruction_are_rejected`
- `test_broker_delivers_once_without_store_path`
- `test_wrong_kind_corruption_and_candidate_swap_fail_closed`

Round 2：通过。

## Round 3：崩溃、并发与重放

攻击问题：

- Artifact 原子安装后调用响应丢失是否导致内容重复或错误恢复；
- 同一 candidate 并发 stage 是否产生不同内容身份；
- ref kind/metadata/content 被替换后是否仍可 load；
- read grant 是否能重复兑换；
- grant descriptor 参数是否可用任意 iterable 触发边界外代码。

修正与证据：

- stage/load 双重验证 kind/media/推导 sensitivity/metadata、producer identity、size
  和 SHA；
- 安装后响应丢失只留下 content-addressed orphan，重试复用同一内容；
- 并发 stage 收敛到同一 artifact id/SHA/URI，所有返回 ref 都重新可验证；
- corrupt bytes、wrong kind 和 candidate swap fail closed；
- Broker grant 在 bytes 发送前 consume，第二次兑换拒绝；
- Worker 校验只接受已解析的 descriptor tuple，不迭代任意外部对象。

对应测试：

- `test_retry_recovers_atomic_install_after_lost_response`
- `test_concurrent_stage_converges_to_one_artifact`
- `test_wrong_kind_corruption_and_candidate_swap_fail_closed`
- `test_broker_delivers_once_without_store_path`

Round 3：通过。

## 剩余风险

- Workflow task/context 本身已有 durable definition 边界；本阶段新增的是 Worker 可授权
  的专用 Artifact，不修复旧定义中错误内联 credential 的问题。
- LocalArtifactStore 的 `sensitive/secret` 标签不等于静态加密；生产 secret 必须走部署
  管理能力。
- 多进程并发相同内容的 `created_at` 可能不同；artifact id/SHA/URI 才是内容身份。
- 只有后续独立 Agent runtime、provider credential boundary、逐工具 receipt 聚合和
  `AgentActivityReceipt` completion 接入完成后，才能审查是否开放 remote `agent`。
