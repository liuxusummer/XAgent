# Durable Agent Provider Client 三轮对抗性审查

> 审查对象：`DurableAgentProviderClient`、provider wire、
> `ProviderAccessBroker.describe_route()` 与 collector 父绑定。
>
> 结论：provider 子链通过三轮审查；Tool 消费 Handler 已单独完成，但动态 child authority、
> crash-consistent staging 和终态原子提交仍未完成，`production_security_ready=false`，
> remote `agent` capability 保持关闭。

## Round 1：授权、调用顺序与秘密泄露

攻击：

- 没有 Loop observation 时直接调用 client；
- 循环/非法 request 是否仍读取授权或签 grant；
- authorization source 抛出包含秘密的异常；
- authorization 指向其他 Attempt，或中途换 action/worker lineage；
- 同一个 Attempt client 被两个线程同时调用，是否产生双 grant 或历史竞争；
- response 解码失败后是否仍提交历史；
- request task、authorization source、grant bearer、upstream credential 或 receipt
  是否进入 repr/异常。

发现与修复：

1. active collector context 是第一个前置条件；缺失时 authorization source 不执行。
2. canonical request 和 route size 预检先于授权和 grant。
3. authorization source 的常规异常在离开 `except` 后映射为固定 reason code，
   `__cause__`/`__context__` 均为空；进程控制异常原样传播。
4. 每轮 authorization 必须绑定 request 的 Run/Node/Attempt 和足够分类；首次成功签 grant
   后固定 authorization digest，renewal 可换 freshness、不可换 lineage。
5. per-client non-blocking single-flight guard 在读取 observation/authorization 前拒绝重入，
   不会产生第二份 grant；锁不跨 client，因此不同 Attempt 仍可并发。
6. Artifact/receipt/wire 全部验证成功后才提交历史与 request count。
7. client 使用 slots 和自定义 repr；request 指令、source、Broker、历史和 system 不进入
   repr。ChatResponse receipt 继续 `repr=False`。

证据：

- `test_missing_observation_and_invalid_request_precede_authority`
- `test_authorization_errors_are_sanitized_and_lineage_is_fixed`
- `test_wrong_authorization_parent_fails_before_grant`
- `test_concurrent_call_on_one_attempt_fails_closed`
- `test_invalid_gateway_response_does_not_commit_history`
- `test_real_loop_preserves_history_and_emits_durable_receipts`

Round 1：通过。

## Round 2：wire、容量与 grant 消耗窗口

攻击：

- 重复 JSON key、未知字段、pretty/noncanonical response；
- 循环、超深/超多 JSON、非有限数、非法 Unicode 和自定义对象；
- bool-as-int、负 usage、超长 tool call ID 或非 object arguments；
- response 内容合法但 receipt/Artifact 属于另一父对象；
- payload 超过 route 上限时先签 grant，永久占住 invocation index；
- 大历史、工具表或 response 造成无界内存与持久化膨胀。

发现与修复：

1. wire v1 exact-field 解析，response 必须是 canonical UTF-8 JSON；所有层级拒绝重复 key。
2. request/response 使用严格 JSON 类型、深度 24、65,536 item、单字符串 4 MiB 和全局
   bytes 上限；messages/tools/tool calls 另有独立数量限制。
3. usage 六字段 exact 解析；tool name 使用 safe code，tool call ID 与 collector 同为
   255 字符上限，arguments 必须是 object。
4. client 重验 Result Artifact kind/classification/producer/empty metadata、route response
   bytes 上限，以及 receipt 的 action、authorization、父 request、route digest、sequence
   和 payload digest。
5. Broker 增加只读 `describe_route()`；client 在读取授权和签 grant 前按精确 route 上限
   拒绝 payload，因此无副作用的超限请求不会留下无法重签的墓碑。
6. Session history 和 compaction 分别有界；response bytes 还受 Broker route 上限约束。

证据：

- `test_route_size_preflight_does_not_burn_a_grant`
- `test_invalid_gateway_response_does_not_commit_history`
- `test_route_description_is_credential_free_and_exact`
- `test_parent_binding_validation_is_exact_and_sanitized`
- 既有 ProviderAccessBroker payload/result/receipt 攻击回归。

Round 2：通过。

## Round 3：恢复、历史一致性与能力误报

攻击：

- invoker 首次失败后，client 是否用新 grant 绕过 exactly-once；
- NOT_STARTED/COMPLETED recovery 是否改变 payload 或 invocation sequence；
- 多轮调用是否丢 system、assistant tool call 或 tool result；
- ContextBuilder 启用 Principal 后是否仍能管理 remote client history；
- 普通内存 journal、无 result Artifact 或 SECRET 本地 Store 是否被误报为 durable；
- provider 子链完成后是否提前开放 remote Agent。

发现与修复：

1. bounded retry 只复用原 grant、authorization 和 bytes；不签第二个逻辑 invocation。
2. NOT_STARTED 必须由 Broker 的 evidence CAS 取得唯一重试权；COMPLETED 从 Artifact
   恢复，receipt completion mode 和 recovery evidence 保持可验证。
3. client 暴露 Session-compatible backend；成功历史保留 system、user、assistant
   tool-call 和下一轮 tool result，request payload 中的 sequence 单调递增。
4. 端到端测试在真实 `run_agent_loop + Principal + ContextBuilder + Broker + collector`
   组合下验证两轮历史和 durable provider manifest。
5. 构造时要求 `durable_result_recovery_ready is True`；内存 journal 直接拒绝。Broker
   继续拒绝 LocalArtifactStore 的 SECRET response。
6. client 的 `production_security_ready` 固定 false；公共 remote adapter 支持集合没有
   任何变化。

证据：

- `test_broker_retry_converges_not_started_and_completed`
- `test_real_loop_preserves_history_and_emits_durable_receipts`
- `test_requires_durable_result_and_exact_parent_binding`
- 既有 collector concurrency/finalization 与 Broker replay/recovery 全部回归。

Round 3：通过。

## 验证基线（2026-07-31）

- provider client、evidence、Broker、manifest、Agent Loop、安全与公共 API：103/103；
- 全部 `test_orchestration_*.py`：927/927；
- 全项目 `unittest discover`：1,634/1,634；
- canonical wire 连续 10,000 次：编码约 0.79 秒、解码约 0.47 秒，流式消费
  `tracemalloc` 峰值约 18 KB（开发机微基准，仅用于回归量级，不作为生产 SLO）。
- phase 文件通过 `py_compile`、`git diff --check` 与 credential/path/logging 静态扫描。

## 残余风险

- grant 签发成功后进程崩溃，新的进程无法重取 bearer；禁止通过普通持久化泄露 token。
- client history 与 checkpoint 尚未 crash-consistent；恢复 Attempt 不能仅凭聊天文本决定
  下一 invocation。
- provider 子链完整不等于 Tool lineage 完整；即使 Tool 消费 Handler 也完整，动态 authority
  与 Agent terminal transaction 未完成前仍不能开放远程 Agent。
- reference Broker/Invoker 不证明生产 gateway attestation、外部 ledger 或网络隔离。
- reference ProviderInvoker 接口尚未证明有界网络 deadline 或 cooperative cancellation；
  client 不用不可终止的后台线程伪造 timeout，超时后的外部结果必须保持 unknown。
- 当前仅证明单机磁盘 journal + LocalArtifactStore 的 SENSITIVE response durability；
  SECRET 和跨主机部署仍需独立受信实现。
