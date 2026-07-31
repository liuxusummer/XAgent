# Durable Agent Tool Handler 三轮对抗性审查

> 审查对象：`DurableAgentToolHandler`、`AgentToolInvocationRequest`、
> `AgentToolResult` Artifact 以及 Agent Loop/collector 组合边界。
>
> 结论：消费/组合边界通过三轮审查；后续 `DurableAgentToolExecutor` 已补齐本地 reference
> 动态 child authority，但 Agent 终态事务与远程生产隔离尚未实现，
> `production_security_ready=false`，remote `agent` capability 保持关闭。

## Round 1：父绑定、参数与调用顺序

攻击：

- 使用伪造、缺失或 Store 无法验证的父 request Artifact；
- Handler 与 collector/Context 不属于同一父 Attempt；
- 没有 active Tool observation 时绕过 Loop 直接 dispatch；
- 不在 allowlist 的动态方法名触发任意反射；
- 参数超限、循环、非 JSON，或在 digest 后改写嵌套对象；
- token/password 等敏感值经 args、repr 或异常进入 executor/log；
- 同一 Handler 被两个线程同时调用。

发现与修复：

1. 构造时从 ArtifactStore verify + read 父 request，并与传入对象逐字段相等验证。
2. collector 父绑定和 `ctx.execution_evidence_observer is collector` 都是强前置条件。
3. active observation 是调用 executor 前的第一运行时事实；缺失时固定失败。
4. `__getattr__` 只生成 allowlist 中的精确 `exec_<tool>`，其余返回 `AttributeError`。
5. 参数复用 Policy 的 bounded redacted digest，随后 JSON 深复制并递归冻结。
6. structurally sensitive 或显式 sensitive key 的非空值在 executor 前拒绝；request 的 args 与
   operation key 都从 repr 排除。
7. per-handler non-blocking lock 拒绝重入，不影响其他 Attempt 并发。

证据：

- `test_parent_request_must_be_store_verified`
- `test_missing_observation_and_executor_errors_are_sanitized`
- `test_inline_sensitive_arguments_fail_before_executor`
- `test_invocation_arguments_are_detached_and_deeply_read_only`
- `test_same_handler_rejects_concurrent_execution`

Round 1：通过。

## Round 2：receipt、Artifact 与控制字段替换

攻击：

- 替换 receipt 的父 Run、tool、args 或 operation/idempotency digest；
- 返回进程内 receipt，但不写 durable terminal Event；
- 重放同一 child attempt/receipt；
- 替换 result Artifact、producer、分类或 SandboxReceipt 输出引用；
- 使用重复 JSON key、未知字段、非 canonical bytes、循环、超大 prompt 或任意 flags；
- 把 secret 结果写进本地明文 ArtifactStore；
- 让 sandbox 原始输出直接控制 `should_exit`/`flags`。

发现与修复：

1. Handler 重验父派生 operation 和 idempotency digest、args、tool、Run 及独立 child identity。
2. receipt 必须从 `DurableRunStore.get_tool_receipt()` 重读且完全相等。
3. 已消费 child attempt 和 receipt digest 在 Handler 内去重。
4. 成功 result 必须精确绑定 child producer，并在 SandboxReceipt 输出集合中恰好出现一次；
   Store verify/read/size/SHA/canonical parse 全部通过后才生成 ActionResult。
5. result wire 使用 exact schema 和独立的深度、节点、字符串、prompt、flags、总 bytes 上限。
6. LocalArtifactStore 在 `put_bytes` 前拒绝 secret；通用 Store 返回 secret ref 时必须证明
   deployment-managed encryption。
7. 文档把结果 envelope 定义为部署侧受信控制对象，sandbox 原始输出只能放在不可信 data。

证据：

- `test_argument_receipt_and_result_substitutions_fail_closed`
- `test_receipt_must_exist_in_durable_store`
- `test_malformed_results_are_rejected_without_context`
- `test_stage_requires_store_integrity_confirmation`
- `test_plaintext_secret_result_is_rejected`

Round 2：通过。

## Round 3：真实 Loop、失败恢复与能力误报

攻击：

- 在真实 Agent Loop 中 receipt 是否被放入 prompt、业务 tool result 或 repr；
- Tool 完整而 provider 不完整时，manifest 是否被错误提升为全链完整；
- executor 抛出含 secret 的异常是否逃逸；
- failed/cancelled/unknown receipt 是否仍读取不存在或不可信的 result；
- outcome unknown 后 Agent 是否继续产生副作用；
- reference boundary 是否被误报成 production-ready 动态 Tool runtime。

发现与修复：

1. `ActionResult.tool_receipt` 只由 observer 消费，Core 业务结果继续只包含 `data`。
2. collector 分别计算 Tool 与 Provider 完整性；真实两轮测试得到 Tool complete、Provider
   partial，不互相冒充。
3. executor 常规异常映射为固定 `agent_tool_executor_unavailable`，cause/context 清除。
4. 非成功 receipt 禁止 result Artifact，只产生有界固定诊断；自由文本 error code 不透传。
5. cancelled、abandoned、outcome_unknown 设置 `should_exit=true` 和空 next prompt。
6. Handler readiness 固定 false；现有 immutable Workflow Tool executor 不能签发运行时动态
   child claim，该架构缺口在契约和残余风险中显式保留。

证据：

- `test_real_loop_attaches_verified_tool_receipt`
- `test_missing_observation_and_executor_errors_are_sanitized`
- `test_uncertain_receipt_stops_loop_with_safe_result`
- Agent Execution Manifest 与 collector 既有父绑定/完整性回归。

Round 3：通过。

## 验证基线（2026-07-31）

- Handler/result/policy/public API 定向测试：44/44；
- 全部 `test_orchestration_*.py`：942/942；
- 全项目 `unittest discover`：1,649/1,649；
- canonical result wire 连续 10,000 次：编码约 1.88 µs/次、解码约 16.2 µs/次
  （开发机微基准，仅用于回归量级，不作为生产 SLO）；
- phase 文件通过 `py_compile`、`git diff --check` 与 secret/path/logging 静态扫描。

## 残余风险

- 测试 `_ReceiptStore` 是 Store 消费边界 fixture，不是动态 authority 的替代实现。
- operation key 唯一性已被 Handler/collector 验证；child claim 与 policy grant 现由
  `DurableAgentToolExecutor` 的 ledger 实现，发行侧残余风险见其独立对抗审查。
- result envelope 由受信 adapter 生成的约束目前是接口/部署契约，尚无独立签名证明。
- Agent terminal transaction 未完成前，进程可能留下完整 Tool/Provider 子结果但没有可接纳的
  父 Agent 终态；恢复端不得靠聊天文本或 `latest` 猜测。
