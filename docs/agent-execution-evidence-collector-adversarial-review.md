# Agent Execution Evidence Collector 三轮对抗性审查

> 审查对象：Core Agent Loop execution-evidence observer、
> `ChatResponse.provider_receipt`、`ActionResult.tool_receipt` 和
> `AgentExecutionEvidenceCollector`。
>
> 结论：真实 Loop 的 fail-closed 观察接线通过；生产 remote gateway client、
> durable Tool handler、manifest staging 与终态原子提交仍未完成，因此 remote
> `agent` capability 保持关闭。

## Round 1：观察时序与副作用窗口

攻击：

- observer 在 provider/tool start 时失败，底层调用是否仍会执行；
- provider/tool 已执行后 finish observer 失败，Loop 是否继续并伪装成功；
- client/handler 抛错后 collector 是否永久残留 open call；
- 底层调用与失败 callback 同时抛错时，原始异常是否通过隐式 context 泄露；
- observer 方法解析抛错时，是否绕过异常正文清洗；
- `KeyboardInterrupt/SystemExit` 是否被 evidence callback 吞掉或改写；
- observer 异常是否泄露 receipt、credential 或异常正文；
- receipt 是否进入业务 tool result、prompt、checkpoint、Event 或 repr。

发现与修复：

1. start callback 在线性化调用前执行；失败会阻止 `client.chat()` 或 Handler dispatch。
2. finish callback 在返回值验证后执行；失败向上抛固定
   `ExecutionEvidenceObservationError`，使 Attempt 失败关闭。
3. client/handler 异常路径分别调用 `provider_call_failed/tool_call_failed`，collector
   关闭 open observation 并永久标记该链 partial。
4. 底层异常先离开原始 `except`，再调用 failure callback；callback 异常也在离开自身
   `except` 后转换为固定 reason code，因此双重失败不保留任一 cause/context。
5. observer 方法解析和调用都位于清洗边界内；进程控制异常不进入普通 failure callback，
   继续原样传播。
6. receipt 槽使用 `repr=False/compare=False`，Core 仅 out-of-band 传给 observer；
   `run_agent_loop` 返回的 `tool_results` 不含 receipt。

证据：

- `test_receipts_are_observed_out_of_band_not_in_tool_result`
- `test_evidence_observer_failure_is_sanitized_before_provider`
- `test_process_control_exceptions_bypass_evidence_callbacks`
- `test_provider_and_tool_failures_close_observations`
- `test_dual_failures_do_not_retain_sensitive_exception_context`
- `test_credential_holders_and_compositions_have_safe_repr`

Round 1：通过。

## Round 2：假 receipt、缺失证据与容量攻击

攻击：

- 从 `ActionResult.data`、telemetry 或调用成功状态猜测 receipt；
- 传入任意 duck-typed/子类对象伪装 Tool/Provider receipt；
- 使用另一父 Agent、错误 request/Artifact 或错误 invocation sequence 的合法 receipt；
- 第一项缺 receipt、后续项有 receipt，是否能把稀疏集合伪装成完整；
- 超过 64 项后让 manifest 无界膨胀，或丢掉真实 observed count；
- provider receipt 缺失时仍按 sensitive 写 manifest，掩盖可能的 secret response。

发现与修复：

1. collector 只接受精确 `ToolReceipt` / `ProviderInvocationReceipt` 类型；`None` 永久打断
   complete prefix，不从任何业务字段补猜。
2. Provider receipt 立即重验父 Run/Node/Attempt、request/request Artifact、sequence
   和非空 response ArtifactRef digest。
3. Tool receipt 立即重验 tool name、父 Run，以及由父
   run/node/attempt/request/sequence 派生的 operation/idempotency digest。
4. partial 只保存从 sequence 1 开始的连续 receipt 前缀；缺口之后的 receipt 即使有效也
   不加入清单。
5. receipt 集合最多保存 64 项；observed count 继续增长到 1,000,000，超限只降级为
   bounded partial，不在副作用完成后因容量抛错。
6. 任一 provider 缺证都会把 manifest 分类保守提升为 secret。

证据：

- `test_missing_receipts_remain_partial_and_promote_secret`
- `test_wrong_parent_and_wrong_type_close_as_partial`
- `test_receipt_capacity_degrades_to_bounded_partial_prefix`
- `test_secret_provider_receipt_promotes_complete_manifest`

Round 2：通过。

## Round 3：终态、重入与端到端拼接

攻击：

- provider/tool observation 尚未关闭时提前 finalization；
- 用小于最后 observed turn 的 terminal count 隐藏调用；
- finalization 后追加 receipt，或用不同 exit/turn 重写 manifest；
- 同一完成响应丢失后重复 finalization 得到不同 digest；
- 只分别测试 observer 和 collector，不验证真实 Loop 的组合顺序；
- 普通本地 Session/Handler 被误认为有完整证据。

发现与修复：

1. collector 用锁保护单一 open observation；重叠、乱序、turn 回退与未关闭终态全部拒绝。
2. terminal turns 必须不小于最后观察 turn；bool-as-int、负数和超限值被拒绝。
3. 第一次 finalization 冻结 collector；相同 exit/turn 幂等返回同一 manifest 对象，
   冲突终态或后续观察失败关闭。
4. 端到端测试让两次 `client.chat()` 和一次 Handler dispatch 分别携带真实 typed
   receipt，并在调用内部读取 collector 分配的 provider sequence 与 Tool operation
   key，验证 `run_agent_loop → collector → manifest` 的顺序和完整标志。
5. 普通本地 response/result 的 receipt 默认 `None`；挂载 collector 只会记录 partial，
   不改变既有 Loop 行为。

证据：

- `test_observation_order_and_terminal_turn_fail_closed`
- `test_concurrent_observation_and_finalization_have_one_winner`
- `test_complete_ordered_lineage_finalizes_idempotently`
- `test_real_loop_wires_typed_receipts_into_manifest`
- 既有全部 `tests.test_agent_loop` 回归。

Round 3：通过。

## 残余边界

- observer 证明 Core 确实看到了显式 receipt，不证明 receipt 签名或外部 gateway
  attestation；这些仍由 Provider/Tool 安全边界负责。
- 当前仓库尚无生产 `ProviderAccessBroker → ChatResponse` client wrapper，也没有
  `TrustedActivityExecutor → ActionResult` remote Handler。
- collector 只构造 manifest；尚未与 AgentActivityRequest load、Artifact staging、
  NodeResult、AgentActivityReceipt 和 terminal Store transaction 组合为单一 adapter。
- MixinSession 的内部 retry/failover 是本地兼容行为，不能自动对应一个稳定 provider
  operation ID；remote client 必须禁用隐式多调用或逐次产生 receipt。
- collector 的进程内锁不是跨主机 fencing。未来 remote adapter 仍必须绑定 Assignment、
  lease、worker authorization 和 Attempt fencing token。
