# Remote Worker HTTPS 传输

本模块把既有的 transport-neutral Remote Worker 协议接到严格、有界的 HTTPS
请求面。它补齐网络 framing、mTLS peer evidence、证书到 Worker 身份的绑定和客户端
TLS 校验，但不改变 Durable Store、lease、fencing、Artifact grant 或 runtime proof
的执行事实。

## 已实现边界

```text
Worker request
  -> HttpsRemoteTransport
  -> TLS 1.2+ / server hostname verification / client certificate
  -> ASGI server that implements the ASGI TLS extension
  -> RemoteHttpASGIApp
  -> AsgiTlsPeerAuthenticator
  -> PinnedCertificateIdentityVerifier
  -> RemoteControlPlane
  -> durable RemoteControlJournal + DurableRunStore
```

- 只有固定 `POST /v1/remote-worker`，拒绝 query、重定向、代理、压缩和普通 HTTP
  credential headers。
- 请求、响应、header 数量、header 字节、证书链、并发量和等待时间均有硬上限。
- JSON 拒绝重复 key、非有限常量、非对象顶层和超限消息。
- Worker/tenant 身份只来自 TLS scope，wire body 和 `x-worker-id` 等 header 无权覆盖。
- 内建 pin verifier 对叶证书 DER 的 SHA-256 做精确匹配；allowlist 原子发布，可重叠
  换证或立即撤销。
- 证书指纹是可轮换凭据绑定；`identity_digest` 是稳定的 tenant/worker lineage。
  因此合法换证不使持久 session 变成另一个 Worker。
- 客户端每次 attempt 重新读取 `SSLContext`，允许部署接入证书轮换 provider。只接受
  `CERT_REQUIRED + check_hostname + TLS 1.2+` 的 context。
- 连接丢失重试复用完全相同的 canonical request id/digest。确定性 4xx、认证失败和
  非法响应不重试。
- 同一端点承载 run-scoped `poll` 和空 body 的 `poll_fleet`；后者只有在控制面显式
  绑定 production-ready Fleet poller 后才可领取跨 Run 工作，完整边界见
  [remote-fleet-data-plane.md](remote-fleet-data-plane.md)。

ASGI TLS extension 定义 `client_cert_chain` 为 PEM Unicode 字符串序列，并规定第一个
元素是叶证书；`tls_version` 和 `cipher_suite` 是整数。实现按该契约严格解析：
[ASGI TLS Extension](https://asgi.readthedocs.io/en/latest/specs/tls.html)。
ASGI HTTP 规范同时要求 server 负责 de-chunk，应用接收的是 body bytes：
[ASGI HTTP Message Format](https://asgi.readthedocs.io/en/latest/specs/www.html)。

## 服务端组合

```python
import hashlib
import ssl

from src.orchestration import (
    AsgiTlsPeerAuthenticator,
    PinnedCertificateIdentityVerifier,
    PinnedWorkerCertificate,
    RemoteHttpASGIApp,
)

with open("worker.crt", encoding="ascii") as stream:
    worker_pem = stream.read()

fingerprint = hashlib.sha256(
    ssl.PEM_cert_to_DER_cert(worker_pem)
).hexdigest()

pins = PinnedCertificateIdentityVerifier(
    [
        PinnedWorkerCertificate(
            certificate_sha256=fingerprint,
            worker_id="worker-a",
            tenant_id="tenant-a",
        )
    ]
)
app = RemoteHttpASGIApp(
    production_remote_control,
    AsgiTlsPeerAuthenticator(pins),
)
```

`production_remote_control.production_security_ready` 只有在以下条件同时成立时才为真：

- 使用 durable `RemoteControlJournal`；
- assignment adapter 明确提供 production-ready 的 two-phase admission；
- reference admission 未启用。

pin 轮换必须发布完整快照。先同时加入旧、新证书，确认 Worker 已换证，再删除旧证书：

```python
pins.replace([old_binding, new_binding])
pins.replace([new_binding])
```

`pins.replace([])` 是紧急 fail-closed 开关；随后所有请求在进入控制面前被拒绝。

## Worker 客户端

```python
import ssl

from src.orchestration import HttpsRemoteTransport, RemoteWorkerClient

def current_mtls_context() -> ssl.SSLContext:
    context = ssl.create_default_context(
        ssl.Purpose.SERVER_AUTH,
        cafile="control-plane-ca.pem",
    )
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(
        certfile="worker.crt",
        keyfile="worker.key",
    )
    return context

transport = HttpsRemoteTransport(
    "https://control.example:8443",
    current_mtls_context,
)
client = RemoteWorkerClient(
    transport,
    worker_id="worker-a",
    instance_id="worker-a-pod-17",
)
```

Python `SSLContext` 的 client 模式、证书验证和证书链加载契约见
[Python ssl 文档](https://docs.python.org/3/library/ssl.html)。

## 必须由部署证明的事项

`RemoteHttpASGIApp` 信任 ASGI server 构造的 scope；普通客户端不能直接提供这个
Python 对象。因此部署必须证明：

1. ASGI server 实际实现 TLS extension，并从当前 `SSLSocket` 填充证书链；不能由
   普通 HTTP header 拼装。
2. TLS 在该 server 处终止，或前置代理到 server 的 hop 具有独立认证和防伪造机制。
3. server 要求 client certificate，使用受控 CA、TLS 1.2+、请求头/请求体超时和
   graceful drain。
4. Worker key 由 workload identity 系统轮换；不能把私钥放入仓库、日志或
   `RemoteControlJournal`。
5. 生产环境用真实证书做一次 socket-level 集成测试，断言缺证书、错证书、撤销证书、
   过期证书和 hostname mismatch 全部在进入控制面前失败。

本仓库不捆绑或声称某个 ASGI server 已提供 TLS extension，也没有实现 SPIFFE
Workload API。SPIFFE 部署应通过 Workload API 获取和轮换 X.509-SVID，再由部署 adapter
映射到稳定 Worker lineage；Workload API 的安全模型和本地通信要求见
[SPIFFE Workload API](https://spiffe.io/docs/latest/spiffe-specs/spiffe_workload_api/)。

## 故障语义

| 故障 | HTTP/客户端语义 | 是否自动重试 |
|---|---|---|
| TLS identity 缺失/失败 | `401` / `transport_unauthorized` | 否 |
| 非法 JSON、schema 或确定性 4xx | `400` / `invalid_response` | 否 |
| admission 饱和、`408/425/429/503` | backpressure | 有界、同 request |
| socket、TLS、5xx | unavailable | 有界、同 request |
| 控制面协议拒绝 | HTTP 200 + protocol `ok=false` | 交给 Worker 状态机 |
| response 丢失 | journal/request digest 去重；终态可 durable replay | 同 request |

外部副作用仍是 at-least-once。HTTP 重试没有把任意 Tool 变成 exactly-once；
不可探测写入仍必须收敛到 `OUTCOME_UNKNOWN`。

## 验证

```bash
.venv/bin/python -m unittest -q \
  tests.test_orchestration_remote_http \
  tests.test_orchestration_remote_protocol \
  tests.test_orchestration_remote_execution \
  tests.test_orchestration_remote_journal
```

三轮威胁审查和修复证据见
[remote-worker-https-adversarial-review.md](remote-worker-https-adversarial-review.md)。
