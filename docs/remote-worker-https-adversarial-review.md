# Remote Worker HTTPS 三轮对抗审查

审查对象：

- `src/orchestration/remote_http.py`
- `RemoteControlPlane.production_security_ready`
- HTTPS 与真实 `RemoteControlPlane` 的组合测试

攻击者可控制 URL 请求、headers、JSON body、连接中断时机和 Worker 进程，但不能修改
控制面进程、部署 pin allowlist 或 ASGI server 生成的可信 TLS scope。

## 第一轮：协议、输入和网络攻击面

### 已关闭 P1

1. **确定性 4xx 被误当作可重试网络故障。**
   旧映射会重试 `400/404/415`。现在认证失败、确定性 4xx 和非法响应均不重试，只有
   `408/425/429/503`、5xx 和连接故障可以有界重试；重试复用原 canonical bytes。
2. **慢请求体可长期占用 inflight slot。**
   增加独立 body timeout；超时固定返回 `408 request_timeout`，不调用控制面。
3. **query、编码路径、header 控制字符和非 canonical Content-Length 存在歧义。**
   现在拒绝 query、与固定路径不一致的 `raw_path`、非法 header token、CR/LF/NUL、
   重复或前导零 Content-Length。
4. **动态 TLS provider 异常可能把私钥路径带入异常链。**
   构造和轮换阶段均转换为固定错误码并抑制原异常文本。

### 回归证据

- `test_query_header_smuggling_and_noncanonical_lengths_are_rejected`
- `test_slow_request_body_is_bounded_by_timeout`
- `test_authentication_and_invalid_response_failures_are_not_retried`
- `test_connection_loss_retries_the_exact_same_wire_intent`
- `test_initial_tls_provider_failure_is_redacted`
- `test_rotating_tls_provider_failure_is_mapped_to_safe_worker_error`

第一轮结论：P0=0，未关闭 P1=0。

## 第二轮：身份冒充、换证和撤销

### 已关闭 P1

1. **只有 verifier Protocol，部署容易注入“始终允许”的伪实现。**
   增加 `PinnedCertificateIdentityVerifier`：按 TLS scope 的叶证书 DER SHA-256 精确
   allowlist，未知证书 fail closed。
2. **把叶证书 fingerprint 直接作为 `identity_digest` 会使合法换证破坏 session。**
   凭据 fingerprint 与稳定 tenant/worker lineage 分离。同一 Worker 的新旧证书可
   重叠轮换，控制面身份保持稳定。
3. **撤销快照需要并发原子性。**
   pin snapshot 在锁内一次替换；认证读取只看到旧或新完整快照。空 snapshot 立即使
   verifier non-ready，应用在控制面调用前拒绝。
4. **wire/header 身份可尝试覆盖 TLS 身份。**
   `x-worker-id`、`x-tenant-id`、Authorization/Cookie 明确拒绝；协议中的 worker id
   只用于与 transport identity 比对，不能创建身份。

### 回归证据

- `test_leaf_certificate_is_bound_to_exact_protocol_identity`
- `test_atomic_rotation_revokes_old_certificate_and_can_fail_closed`
- `test_invalid_or_duplicate_bindings_are_rejected`
- `test_request_identity_cannot_override_tls_identity`
- `test_missing_stale_or_failed_tls_evidence_is_rejected`
- `test_https_asgi_transport_composes_with_real_control_plane`

第二轮结论：P0=0，未关闭 P1=0。

## 第三轮：并发、崩溃恢复和诚实边界

### 已关闭 P1

1. **网络 app 可能暴露 reference-only 控制面。**
   `RemoteHttpASGIApp` 在构造和每次请求时检查控制面及 authenticator readiness；
   内存 journal、非 two-phase admission、启用 reference admission 的组合不能暴露。
2. **无限排队会在控制面变慢时放大内存与线程。**
   app 使用硬上限 semaphore 和有界 admission wait，饱和时返回固定 503；同步认证和
   控制调用离开 event loop。
3. **HTTP adapter 与持久控制面只各自单测，没有组合证据。**
   新增真实 `RemoteControlPlane` + durable journal + secure admission 的 ASGI
   register/poll 测试，断言生成唯一持久 Attempt。
4. **实现容易被文档误述为“已经部署真实 mTLS/SPIFFE”。**
   新增独立传输文档，明确 ASGI TLS scope 是 server trust boundary；仓库不捆绑
   TLS-extension server、证书基础设施或 SPIFFE Workload API。
5. **可信控制实现的异常响应可能把内部异常带到 server 日志。**
   response framing 捕获非系统级序列化错误，以固定 `503 control_unavailable`
   fail closed，不回显对象或异常文本。

### 回归证据

- `test_inflight_bound_rejects_queue_growth`
- `test_control_readiness_is_rechecked_for_every_request`
- `test_verifier_revocation_is_rechecked_without_reading_body`
- `test_non_production_composition_cannot_be_exposed`
- `test_control_response_serialization_failure_is_redacted`
- `test_security_adapters_are_required_and_arbitrary_callable_is_rejected`
- 既有 remote journal/execution/fault matrix 回归

第三轮结论：P0=0，未关闭 P1=0。

## 残余边界

- ASGI scope 真实性只能由承载它的 server/sidecar 证明。未通过真实 socket-level
  mTLS 测试的部署不得标记为 production-ready。
- 标准库客户端会重新读取 `SSLContext`，但证书签发、私钥保护、CA 撤销和 SPIFFE
  Workload API 生命周期属于部署系统。
- `asyncio.to_thread` 不能强杀已经进入可信控制面的同步调用；inflight 上限阻止无界
  扩张，进程级 watchdog、graceful drain 和容量告警仍是部署职责。
- HTTP request 去重不改变外部副作用的 at-least-once 本质。

上述均为显式部署边界，不是被 telemetry 或文档掩盖的执行保证。
