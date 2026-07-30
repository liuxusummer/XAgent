# Agent Kernel 第一阶段

## 1. 目标与边界

Agent Kernel 为默认 Agent Loop 增加统一的身份、授权、知识来源和 LLM 上下文控制面，同时保持
CLI、Web、Team Workflow 和可选 Durable Orchestration 的调用方式不变。

第一阶段包含：

- 默认 Agent Loop 的逐工具策略判定、参数绑定审批和保守恢复；
- 长期记忆的候选、审核、生效两阶段写入；
- 文件检索的租户边界和 `EvidenceBundle`；
- token 预算的 `ContextBuilder`、`ContextManifest` 和结构化 compaction；
- 来源、上下文、策略、工具、记忆之间的元数据追踪链。

本阶段不迁移向量数据库，不引入 GraphRAG，不重构 Web UI，也不把默认线性 Agent Loop
改造成另一套编排器。

## 2. 核心契约

`src/core/agent_kernel.py` 定义只含有界元数据的共享契约：

| 契约 | 用途 | 不保存 |
|---|---|---|
| `Principal` | subject、tenant、session、run、agent、scope | 凭据、Cookie、原始请求 |
| `KnowledgeItem` | namespace、来源、信任级别、敏感度、ACL、TTL、版本、内容哈希 | 原始知识内容 |
| `ContextItem` | LLM 可见性、组件类型、token 数、优先级、来源哈希 | 原始 prompt、工具输出 |
| `ContextManifest` | 一次 LLM 输入的预算与选中引用 | 本地 Principal 内容、未选中数据 |

`Principal.principal_digest` 绑定当前 session/run，用于动作和审批；`boundary_digest`
只绑定 subject、tenant、agent 和 scopes，用于跨进程恢复时验证同一授权边界。

所有 digest 使用规范化 JSON 和 SHA-256。ACL 缺失、枚举未知、集合无界、TTL 非法或 digest
格式错误均拒绝构造，不做隐式默认。

## 3. 工具授权

默认 Handler 的每个 `exec_*` 方法都必须存在于 `LOCAL_TOOL_CONTRACT_MAP`。测试直接比较两者
集合，新增工具但未声明 effect class、capability 和 resource 时会失败。

执行顺序：

1. Handler 从实际生效参数提取脚本或文件内容；
2. `LocalPolicyGate` 构造 orchestration `ActionRequest`；
3. `PolicyEngine` 校验 Principal scope、effect class、capability 和 resource；
4. 高风险动作返回 `REQUIRE_APPROVAL`；
5. UI 只展示参数摘要、字节数和 SHA-256，不展示脚本或内容；
6. grant 绑定 action digest、run、node、policy version、actor 和 TTL；
7. grant 在工作区持久账本中单次消费后才执行工具；
8. `tool_finally_callback` 清除线程局部授权，异常不得把授权留给下一次调用。

`NaN`、不可规范化、递归或超过 ActionRequest 元数据上限的参数在策略入口直接
`DENY(reason_code=invalid_action_args)`，不能用校验异常终止循环或绕过工具前置检查。

审批后进程崩溃且没有持久工具结果时，checkpoint 将结果标为 unknown。恢复只允许先探测当前
状态，禁止自动重放。

普通文件工具不能读取 `runtime/agent_kernel/`、`runtime/checkpoints/`、文件索引数据库以及
根级控制文件 `_intervene`、`_keyinfo`、`plan.md`，
也不能写入任何 `runtime/` 或 `system/` 路径。`memory/`、受管 `MEMORY.md` 和 Memory
Store 的写入只能走记忆候选协议；直接读取受管 Memory 还必须具有 `memory.read`。
`file_write` / `file_patch` 的 `{{file:...}}` 展开器不接受受管 Memory 或控制面来源，
避免把隐式读取伪装成普通业务写入。
上述保留目录和文件名统一按 case-insensitive 语义分类；`SYSTEM/MEMORY`、
`RUNTIME/CHECKPOINTS` 等大小写变体不能绕过策略、底层文件工具、Web preview/tree
或文件索引。

`host.read` 是本地 operator 兼容能力，不属于租户 scope。多用户 Web 注册表若声明该
scope 会在加载时被拒绝；工作区外 `file_read` 同时在 Policy preflight 和 Handler
执行层检查该能力，租户的交互确认不能把 `workspace.read` 升级为宿主读取权限。

## 4. 长期记忆

`MemoryStore` 在同一原子 JSON 状态中维护：

```text
MemoryCandidate(pending/rejected/expired)
    -- memory.review scope + ACL + TTL -->
MemoryRecord(approved)
```

候选和记录均包含 namespace、kind、source refs、trust、confidence、sensitivity、ACL、
version、TTL 和 review status。写入规则：

- 用户输入、检索结果、工具输出和 Agent 总结最多创建 `pending` candidate；
- `memory_propose` 的 namespace 和 ACL 由当前 Principal 派生，模型不能指定 tenant；
- proposer 只能授予自己的 subject 或 tenant，不能使用通配 ACL；
- namespace 必须规范绑定当前 tenant；ACL 命中仍需先通过独立 tenant 边界校验，
  同名 subject、agent、scope 或通配 grant 均不能跨 tenant；
- reviewer 必须同时具备 `memory.review` scope，并通过候选 ACL；
- pending/rejected/expired candidate 不会被 `active_records()` 返回；
- Store 读取使用 descriptor-relative no-follow 原语并限制为 64 MiB，新状态文件权限不
  超过 `0600`；candidate 与 active record 必须在同一状态中双向对应且审核元数据完全
  一致，孤立、篡改、超量或非规范 JSON 均整体 fail closed；
- 运行时只把已审核、未过期且当前 Principal 有权读取的记录注入 `[Reviewed Memory Data]`；
- 运行时还按 Agent 的 `none/private/global/project` 模式过滤 namespace：`private`
  只接受当前 agent namespace，`global` 只接受 workspace namespace，`project`
  接受两者；其他 agent 的记录不能因 subject ACL 相同而串入当前上下文；
- 记录即使已审核仍保留原始 trust，内容始终被声明为 data，不获得系统指令权限。

旧的文件型 Memory Provider 保留为管理员配置兼容层，但生产 Agent 的自我进化 Hook 和长期
结算工具不再直接修改它。`start_long_term_update` 只提供 SOP，真正提交必须调用
`memory_propose`。

## 5. 检索证据

文件索引首次由已认证 Principal 建立，并绑定 tenant。检索时：

1. 无 Principal 直接返回 `principal_required`；
2. 索引 schema、owner、ACL 或 revision 缺失、未知或损坏时 fail closed；
3. `memory/**`、`system/memory/**` 和 Agent `MEMORY.md` 永不进入通用文件索引；
4. 不同 tenant 不能查询或刷新既有索引；
5. 生成证据前重新计算索引内容 SHA-256，发现与记录 digest 不一致时整次查询 fail closed；
6. 每个结果绑定完整索引内容的 SHA-256、snippet SHA-256 和 `schema:revision`；
7. 返回 `EvidenceBundle`，其中的 `KnowledgeItem` 再次执行 caller ACL 校验；
8. bundle 只在权限过滤后生成，拒绝响应不包含 match、snippet 或源内容。

索引刷新通过逐路径组件的 descriptor-relative `open` 与 `O_NOFOLLOW` 读取，最终文件
必须是常规文件，读取前后的 device/inode/size/mtime/ctime 必须一致。不支持该安全原语的
平台显式返回 `secure_file_read_unavailable`，不能降级为跟随符号链接的普通读取。

索引中的哈希绑定“被索引的内容”，而不是未经刷新后的当前文件。依赖结果执行修改前仍需
`file_read` 读取当前状态，这是防止陈旧证据变成写入依据的第二道边界。

## 6. 上下文构建与恢复

`ContextBuilder` 使用保守的 mixed CJK/ASCII token 估算，将输入分为：

- system rules；
- task state；
- recent / compacted history；
- retrieval evidence；
- reviewed memory；
- tool results；
- skills。

每类有独立预算，总可见 token 不得超过 `max_input_tokens - reserved_output_tokens`。高优先级
内容先进入；超限内容保留有界首尾视图或完全省略，同时记录原始 SHA-256、原 token 数、
可见 token 数和原因。原始本地工作状态以 `llm_visible=false` 进入 manifest，只记录哈希，
不会因为存在于 `AgentContext` 就自动发给模型。

单个 ContextSource、严格 JSON 的深度/元素数和单字符串长度另有压力上限。token 估算与
SHA-256 使用分块 UTF-8 处理，不为超大文本创建第二份完整字节副本；超限工具字符串先变成
带原始摘要和有界预览的结构化记录，任意对象不会通过 `default=str` 执行代码。

Session 历史也按 token 而不是字符裁剪。被删除消息只留下 role、token 数、原因和 SHA-256。
工具历史即使为兼容后端而转换成 `user` 角色，也保留显式 untrusted 包络，后续轮次不得
提升为用户信任。非规范 JSON 和单轮过多工具结果会降级为 hash-only 的有界记录，不会让
上下文构建失败。`update_working_checkpoint` 的单字段上限为 8000 字符，超限不修改状态。
checkpoint schema v3 保存 ContextManifest、component usage 和 compaction 元数据，不保存被
裁剪的原始内容。恢复时重新从当前、已授权来源构建上下文，不能从哈希“还原”内容。

checkpoint 恢复还必须验证 `Principal.boundary_digest`。旧 checkpoint 或跨 subject/tenant/
agent/scope 的 checkpoint 默认拒绝注入任务内容。显式 resume 遇到缺失、损坏或边界不匹配
时直接返回终止错误，不得悄悄创建新任务，也不得回退到工作区 `latest`。

### 6.1 Web Principal 入口

多用户 Web 模式的 Principal 只能由 `WebIdentityProvider` 产生。注册表保存高熵 opaque
token 的 SHA-256，而不保存明文 token；每条受信记录提供 subject、tenant、精确 scopes
和 workspace alias→绝对路径映射。注册表必须位于 Agent workspace 外；映射目标必须
预先存在，加载时绑定规范路径及 device/inode identity。跨 owner 的 workspace 重叠和
注册表 containment 按物理 device/inode 祖先链校验，大小写别名不能改变边界判定。

- Bearer/Cookie 只用于查找受信记录，不进入 Principal、checkpoint、telemetry 或响应；
- owner digest 不依赖 token，因此轮换 token 不会改变状态所有权；
- subject、tenant、scope 或 workspace 映射变化会改变 owner boundary，并撤销旧运行态；
- workspace 路径被替换、删除或改成符号链接时，文件 identity 校验失败并撤销旧运行态；
- 注册表缺失、损坏或没有 enabled identity 时，安全 Web 撤销全部 session/Eval 运行态；
- 子 Agent 和 Team step 在构造前继承父 Principal，发现边界不一致立即停止；
- secure Web 不接受客户端绝对 workspace/config/observability 路径；
- Eval dataset/run 按 owner digest 使用独立物理目录，安全响应不返回宿主绝对路径；
- chat/checkpoint 恢复同时验证 owner、agent、checkpoint ID 和 Principal boundary；
- 没有 `memory.read` 的 Principal 在初始 prompt、周期刷新和 Web 文件 API 中均看不到
  reviewed/legacy memory；
- secure Web 文件预览不暴露任何 `runtime/` 或根级控制文件，受管 Memory 写入只能走
  candidate/review；目录树使用同一过滤边界；
- secure Web 的后台异常和失败 tool result 在响应边界统一去除宿主路径与 backend 细节。

## 7. 追踪链与指标

同一 session/turn 下可按以下字段关联：

```text
retrieval_evidence.bundle_digest
  -> context_manifest.manifest_digest
  -> policy_decision.action_digest + policy_version
  -> tool_end.action_digest + context_manifest_digest
  -> memory_candidate_created.candidate_id + content_sha256
```

Telemetry 只保存长度、计数、ID 和 digest，不保存脚本、完整参数、文件内容、prompt 或工具
输出。建议看板至少统计：

- policy allow / deny / require approval 及 reason code；
- approval rejection、expiry、replay；
- EvidenceBundle 数量、ACL deny、索引版本；
- 每组件 visible token、compaction 数；
- Memory candidate 的 pending/approved/rejected/expired；
- checkpoint resume deny 和 unknown-outcome 恢复。
- code sandbox probe failure、unsafe 请求、执行计划不匹配和输出上限终止；
- Web 认证失败、scope deny、owner mismatch、identity revocation 和跨 owner 资源探测。

Telemetry 是审计线索，不是执行事实源。工具是否已完成仍以工具结果、当前物理状态和 durable
store/checkpoint 的相应契约为准。

## 8. 测试门槛

合入前至少运行：

```bash
python -m unittest -q \
  tests.test_agent_kernel \
  tests.test_local_policy \
  tests.test_agent_kernel_integration \
  tests.test_memory_store \
  tests.test_file_index \
  tests.test_context_builder \
  tests.test_agent_kernel_e2e \
  tests.test_code_sandbox \
  tests.test_web_identity \
  tests.test_web_principal_security
```

对抗用例覆盖参数替换、异常/超大参数、审批拒绝后的零副作用、授权残留、跨 tenant 检索、
未知索引 schema、索引内容哈希不一致、跨 Principal checkpoint、Memory poisoning、ACL
损坏、控制面文件旁路、长工具结果、工具信任跨轮保持和长历史压缩。安全断言要求拒绝响应
不含受保护内容，未审核候选的 active record 数为零。
