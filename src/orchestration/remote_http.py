"""Authenticated HTTPS/ASGI transport for the remote worker protocol.

The transport performs framing, bounded JSON decoding, and peer authentication.
It never derives a trusted Worker identity from request JSON or ordinary HTTP
headers. Durable execution authority remains in ``RemoteControlPlane``.
"""

from __future__ import annotations

import asyncio
import hashlib
import http.client
import json
import math
import re
import ssl
import threading
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping, Protocol, runtime_checkable
from urllib.parse import urlsplit

from .remote_protocol import (
    MAX_REMOTE_MESSAGE_BYTES,
    AuthenticatedWorker,
    RemoteProtocolError,
    canonical_bytes,
)

REMOTE_HTTP_PATH = "/v1/remote-worker"
MAX_HTTP_HEADERS = 64
MAX_HTTP_HEADER_BYTES = 16 * 1024
MAX_TLS_CERTIFICATES = 8
MAX_TLS_CERTIFICATE_CHARS = 64 * 1024
MAX_TLS_CHAIN_CHARS = 256 * 1024
MAX_TLS_SUBJECT_CHARS = 2 * 1024
MAX_HTTP_INFLIGHT = 1024
MAX_PINNED_WORKER_CERTIFICATES = 10_000
MIN_TLS_VERSION = 0x0303
_CONTENT_TYPE = "application/json"
_FORBIDDEN_CREDENTIAL_HEADERS = frozenset(
    {
        b"authorization",
        b"cookie",
        b"proxy-authorization",
        b"x-client-cert",
        b"x-tenant-id",
        b"x-worker-id",
    }
)
_HOSTNAME = re.compile(r"^[A-Za-z0-9.:-]{1,253}$")
_HTTP_TOKEN = re.compile(rb"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")

AsgiReceive = Callable[[], Awaitable[dict[str, Any]]]
AsgiSend = Callable[[dict[str, Any]], Awaitable[None]]


class RemotePeerAuthenticationError(RuntimeError):
    """A TLS peer could not be mapped to one trusted Worker identity."""


class RemoteHttpConfigurationError(ValueError):
    """The HTTPS transport composition is not safe to expose."""


@dataclass(frozen=True, slots=True)
class PinnedWorkerCertificate:
    """Bind one leaf-certificate digest to one protocol identity."""

    certificate_sha256: str
    worker_id: str
    tenant_id: str

    def __post_init__(self) -> None:
        try:
            validated = AuthenticatedWorker(
                worker_id=self.worker_id,
                tenant_id=self.tenant_id,
                identity_digest=self.certificate_sha256,
            )
        except (TypeError, ValueError) as exc:
            raise RemoteHttpConfigurationError(
                "pinned Worker certificate binding is invalid"
            ) from exc
        object.__setattr__(
            self,
            "certificate_sha256",
            validated.identity_digest,
        )
        object.__setattr__(self, "worker_id", validated.worker_id)
        object.__setattr__(self, "tenant_id", validated.tenant_id)

    @property
    def identity(self) -> AuthenticatedWorker:
        return AuthenticatedWorker(
            worker_id=self.worker_id,
            tenant_id=self.tenant_id,
            identity_digest=_pinned_identity_digest(
                self.worker_id,
                self.tenant_id,
            ),
        )


class PinnedCertificateIdentityVerifier:
    """Atomically rotatable leaf-certificate allowlist.

    The TLS server remains responsible for chain and hostname validation.
    This verifier adds an exact authorization binding from the already
    validated leaf certificate to the protocol Worker and tenant.
    """

    def __init__(
        self,
        bindings: tuple[PinnedWorkerCertificate, ...]
        | list[PinnedWorkerCertificate],
    ) -> None:
        if not isinstance(bindings, (list, tuple)) or not bindings:
            raise RemoteHttpConfigurationError(
                "at least one pinned Worker certificate is required"
            )
        self._lock = threading.Lock()
        self._identities: dict[str, AuthenticatedWorker] = {}
        self._generation = 0
        self.replace(bindings)

    @property
    def production_security_ready(self) -> bool:
        with self._lock:
            return bool(self._identities)

    @property
    def generation(self) -> int:
        with self._lock:
            return self._generation

    def replace(
        self,
        bindings: tuple[PinnedWorkerCertificate, ...]
        | list[PinnedWorkerCertificate],
    ) -> int:
        """Publish one complete allowlist snapshot and return its generation."""

        if (
            not isinstance(bindings, (list, tuple))
            or len(bindings) > MAX_PINNED_WORKER_CERTIFICATES
        ):
            raise RemoteHttpConfigurationError(
                "pinned Worker certificate bindings are invalid"
            )
        replacement: dict[str, AuthenticatedWorker] = {}
        worker_tenants: dict[str, str] = {}
        for binding in bindings:
            if not isinstance(binding, PinnedWorkerCertificate):
                raise RemoteHttpConfigurationError(
                    "pinned Worker certificate binding is invalid"
                )
            digest = binding.certificate_sha256
            if digest in replacement:
                raise RemoteHttpConfigurationError(
                    "duplicate pinned Worker certificate"
                )
            identity = binding.identity
            existing_tenant = worker_tenants.get(identity.worker_id)
            if (
                existing_tenant is not None
                and existing_tenant != identity.tenant_id
            ):
                raise RemoteHttpConfigurationError(
                    "pinned Worker has ambiguous tenant bindings"
                )
            worker_tenants[identity.worker_id] = identity.tenant_id
            replacement[digest] = identity
        with self._lock:
            self._identities = replacement
            self._generation += 1
            return self._generation

    def verify(self, evidence: TlsPeerEvidence) -> AuthenticatedWorker:
        if not isinstance(evidence, TlsPeerEvidence):
            raise RemotePeerAuthenticationError
        if (
            not isinstance(evidence.client_cert_chain, tuple)
            or not evidence.client_cert_chain
            or not isinstance(evidence.client_cert_chain[0], str)
        ):
            raise RemotePeerAuthenticationError
        certificate = evidence.client_cert_chain[0]
        if (
            certificate.count("-----BEGIN CERTIFICATE-----") != 1
            or certificate.count("-----END CERTIFICATE-----") != 1
        ):
            raise RemotePeerAuthenticationError
        try:
            der = ssl.PEM_cert_to_DER_cert(certificate)
            fingerprint = hashlib.sha256(der).hexdigest()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise RemotePeerAuthenticationError from None
        with self._lock:
            identity = self._identities.get(fingerprint)
        if identity is None:
            raise RemotePeerAuthenticationError
        return identity


@dataclass(frozen=True, slots=True)
class TlsPeerEvidence:
    """Bounded TLS evidence supplied to a deployment trust verifier."""

    client_cert_chain: tuple[str, ...] = field(repr=False)
    client_cert_name: str = field(repr=False)
    tls_version: int
    cipher_suite: int | None


@runtime_checkable
class TlsPeerIdentityVerifier(Protocol):
    """Deployment trust anchor for an already TLS-verified certificate chain."""

    production_security_ready: bool

    def verify(self, evidence: TlsPeerEvidence) -> AuthenticatedWorker: ...


@runtime_checkable
class RemotePeerAuthenticator(Protocol):
    """Map trusted transport scope to identity without reading request JSON."""

    production_security_ready: bool

    def authenticate(self, scope: Mapping[str, Any]) -> AuthenticatedWorker: ...


@runtime_checkable
class RemoteHttpControl(Protocol):
    """Minimal production control surface exposed to the ASGI adapter."""

    @property
    def production_security_ready(self) -> bool: ...

    def handle(
        self,
        identity: AuthenticatedWorker,
        wire_request: Mapping[str, Any],
    ) -> dict[str, Any]: ...


class AsgiTlsPeerAuthenticator:
    """Require verified ASGI TLS-extension evidence and a trusted verifier."""

    def __init__(
        self,
        verifier: TlsPeerIdentityVerifier,
        *,
        minimum_tls_version: int = MIN_TLS_VERSION,
    ) -> None:
        if not isinstance(verifier, TlsPeerIdentityVerifier):
            raise TypeError("verifier must implement TlsPeerIdentityVerifier")
        if verifier.production_security_ready is not True:
            raise RemoteHttpConfigurationError(
                "TLS peer verifier is not production ready"
            )
        if (
            isinstance(minimum_tls_version, bool)
            or not isinstance(minimum_tls_version, int)
            or not MIN_TLS_VERSION <= minimum_tls_version <= 0xFFFF
        ):
            raise RemoteHttpConfigurationError("minimum TLS version is invalid")
        self._verifier = verifier
        self.minimum_tls_version = minimum_tls_version

    @property
    def production_security_ready(self) -> bool:
        try:
            return self._verifier.production_security_ready is True
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return False

    def authenticate(self, scope: Mapping[str, Any]) -> AuthenticatedWorker:
        try:
            if self.production_security_ready is not True:
                raise RemotePeerAuthenticationError
            if scope.get("type") != "http" or scope.get("scheme") != "https":
                raise RemotePeerAuthenticationError
            extensions = scope.get("extensions")
            if not isinstance(extensions, Mapping):
                raise RemotePeerAuthenticationError
            tls = extensions.get("tls")
            if not isinstance(tls, Mapping):
                raise RemotePeerAuthenticationError
            if tls.get("client_cert_error") is not None:
                raise RemotePeerAuthenticationError
            tls_version = tls.get("tls_version")
            cipher_suite = tls.get("cipher_suite")
            if (
                isinstance(tls_version, bool)
                or not isinstance(tls_version, int)
                or tls_version < self.minimum_tls_version
            ):
                raise RemotePeerAuthenticationError
            if (
                cipher_suite is not None
                and (
                    isinstance(cipher_suite, bool)
                    or not isinstance(cipher_suite, int)
                    or not 1 <= cipher_suite <= 0xFFFF
                )
            ):
                raise RemotePeerAuthenticationError
            raw_chain = tls.get("client_cert_chain")
            if (
                not isinstance(raw_chain, (list, tuple))
                or not 1 <= len(raw_chain) <= MAX_TLS_CERTIFICATES
            ):
                raise RemotePeerAuthenticationError
            chain: list[str] = []
            total_chars = 0
            for certificate in raw_chain:
                if (
                    not isinstance(certificate, str)
                    or not 1 <= len(certificate) <= MAX_TLS_CERTIFICATE_CHARS
                    or not certificate.isascii()
                    or not certificate.startswith(
                        "-----BEGIN CERTIFICATE-----"
                    )
                    or not certificate.rstrip().endswith(
                        "-----END CERTIFICATE-----"
                    )
                ):
                    raise RemotePeerAuthenticationError
                total_chars += len(certificate)
                if total_chars > MAX_TLS_CHAIN_CHARS:
                    raise RemotePeerAuthenticationError
                chain.append(certificate)
            subject = tls.get("client_cert_name")
            if (
                not isinstance(subject, str)
                or not 1 <= len(subject) <= MAX_TLS_SUBJECT_CHARS
                or any(
                    ord(character) < 32 or ord(character) == 127
                    for character in subject
                )
            ):
                raise RemotePeerAuthenticationError
            identity = self._verifier.verify(
                TlsPeerEvidence(
                    client_cert_chain=tuple(chain),
                    client_cert_name=subject,
                    tls_version=tls_version,
                    cipher_suite=cipher_suite,
                )
            )
            if not isinstance(identity, AuthenticatedWorker):
                raise RemotePeerAuthenticationError
            return identity
        except RemotePeerAuthenticationError:
            raise
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as exc:
            raise RemotePeerAuthenticationError from exc


class _HttpReject(Exception):
    def __init__(self, status: int, code: str) -> None:
        self.status = status
        self.code = code
        super().__init__(code)


class _DuplicateJsonKey(ValueError):
    pass


class RemoteHttpASGIApp:
    """Strict POST endpoint with bounded concurrency and payload-free failures."""

    def __init__(
        self,
        control: RemoteHttpControl,
        authenticator: RemotePeerAuthenticator,
        *,
        path: str = REMOTE_HTTP_PATH,
        max_inflight: int = 256,
        admission_timeout_seconds: float = 0.05,
        body_timeout_seconds: float = 10.0,
    ) -> None:
        if not isinstance(control, RemoteHttpControl):
            raise TypeError("control must implement RemoteHttpControl")
        if not isinstance(authenticator, RemotePeerAuthenticator):
            raise TypeError(
                "authenticator must implement RemotePeerAuthenticator"
            )
        if control.production_security_ready is not True:
            raise RemoteHttpConfigurationError(
                "remote control is not production ready"
            )
        if authenticator.production_security_ready is not True:
            raise RemoteHttpConfigurationError(
                "peer authenticator is not production ready"
            )
        if not _valid_http_path(path):
            raise RemoteHttpConfigurationError("HTTP path is invalid")
        if (
            isinstance(max_inflight, bool)
            or not isinstance(max_inflight, int)
            or not 1 <= max_inflight <= MAX_HTTP_INFLIGHT
        ):
            raise RemoteHttpConfigurationError(
                "max_inflight is outside its bound"
            )
        try:
            admission_timeout = float(admission_timeout_seconds)
        except (TypeError, ValueError) as exc:
            raise RemoteHttpConfigurationError(
                "admission timeout is invalid"
            ) from exc
        if (
            not math.isfinite(admission_timeout)
            or not 0.001 <= admission_timeout <= 5.0
        ):
            raise RemoteHttpConfigurationError(
                "admission timeout is invalid"
            )
        try:
            body_timeout = float(body_timeout_seconds)
        except (TypeError, ValueError) as exc:
            raise RemoteHttpConfigurationError(
                "body timeout is invalid"
            ) from exc
        if (
            not math.isfinite(body_timeout)
            or not 0.01 <= body_timeout <= 300.0
        ):
            raise RemoteHttpConfigurationError("body timeout is invalid")
        self._control = control
        self._authenticator = authenticator
        self.path = path
        self._admission_timeout = admission_timeout
        self._body_timeout = body_timeout
        self._inflight = asyncio.Semaphore(max_inflight)

    async def __call__(
        self,
        scope: Mapping[str, Any],
        receive: AsgiReceive,
        send: AsgiSend,
    ) -> None:
        raw_path = scope.get("raw_path")
        if (
            scope.get("type") != "http"
            or scope.get("path") != self.path
            or (
                raw_path is not None
                and raw_path != self.path.encode("ascii")
            )
        ):
            await _send_json(send, 404, {"error": "not_found"})
            return
        if scope.get("query_string", b"") != b"":
            await _send_json(send, 400, {"error": "query_forbidden"})
            return
        if scope.get("method") != "POST":
            await _send_json(
                send,
                405,
                {"error": "method_not_allowed"},
                extra_headers=((b"allow", b"POST"),),
            )
            return
        acquired = False
        try:
            await asyncio.wait_for(
                self._inflight.acquire(),
                timeout=self._admission_timeout,
            )
            acquired = True
        except TimeoutError:
            await _send_json(send, 503, {"error": "control_busy"})
            return
        try:
            try:
                control_ready = self._control.production_security_ready
                authenticator_ready = (
                    self._authenticator.production_security_ready
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as exc:
                raise _HttpReject(503, "control_unavailable") from exc
            if control_ready is not True or authenticator_ready is not True:
                raise _HttpReject(503, "control_unavailable")
            headers = _headers(scope)
            _validate_request_headers(headers)
            try:
                identity = await asyncio.to_thread(
                    self._authenticator.authenticate,
                    scope,
                )
            except RemotePeerAuthenticationError as exc:
                raise _HttpReject(401, "unauthorized") from exc
            try:
                body = await asyncio.wait_for(
                    _read_body(receive, headers),
                    timeout=self._body_timeout,
                )
            except TimeoutError as exc:
                raise _HttpReject(408, "request_timeout") from exc
            payload = _decode_object(body)
            try:
                response = await asyncio.to_thread(
                    self._control.handle,
                    identity,
                    payload,
                )
            except RemoteProtocolError as exc:
                raise _HttpReject(400, "invalid_request") from exc
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as exc:
                raise _HttpReject(503, "control_unavailable") from exc
            await _send_json(send, 200, response)
        except _HttpReject as exc:
            await _send_json(send, exc.status, {"error": exc.code})
        finally:
            if acquired:
                self._inflight.release()


class HttpsRemoteTransport:
    """Synchronous no-proxy/no-redirect HTTPS transport for ``RemoteWorkerClient``."""

    def __init__(
        self,
        origin: str,
        tls_context_provider: Callable[[], ssl.SSLContext],
        *,
        path: str = REMOTE_HTTP_PATH,
        timeout_seconds: float = 30.0,
        max_attempts: int = 2,
    ) -> None:
        if not callable(tls_context_provider):
            raise TypeError("tls_context_provider must be callable")
        if not isinstance(origin, str) or len(origin) > 2048:
            raise RemoteHttpConfigurationError("HTTPS origin is invalid")
        try:
            parsed = urlsplit(origin)
            port = parsed.port
            hostname = parsed.hostname
        except ValueError as exc:
            raise RemoteHttpConfigurationError(
                "HTTPS origin is invalid"
            ) from exc
        if (
            parsed.scheme != "https"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
            or hostname is None
            or not hostname.isascii()
            or not _HOSTNAME.fullmatch(hostname)
        ):
            raise RemoteHttpConfigurationError("HTTPS origin is invalid")
        if port is not None and not 1 <= port <= 65535:
            raise RemoteHttpConfigurationError("HTTPS port is invalid")
        if not _valid_http_path(path):
            raise RemoteHttpConfigurationError("HTTP path is invalid")
        try:
            timeout = float(timeout_seconds)
        except (TypeError, ValueError) as exc:
            raise RemoteHttpConfigurationError(
                "HTTPS timeout is invalid"
            ) from exc
        if not math.isfinite(timeout) or not 0.1 <= timeout <= 300.0:
            raise RemoteHttpConfigurationError("HTTPS timeout is invalid")
        if (
            isinstance(max_attempts, bool)
            or not isinstance(max_attempts, int)
            or not 1 <= max_attempts <= 3
        ):
            raise RemoteHttpConfigurationError(
                "HTTPS max_attempts is outside its bound"
            )
        self._hostname = hostname
        self._port = 443 if port is None else port
        self._path = path
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._tls_context_provider = tls_context_provider
        try:
            initial_context = tls_context_provider()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise RemoteHttpConfigurationError(
                "TLS context provider unavailable"
            ) from None
        _validate_client_context(initial_context)

    def __call__(
        self,
        request: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        encoded = canonical_bytes(request)
        for attempt in range(self._max_attempts):
            try:
                return self._exchange(encoded)
            except RemoteProtocolError as exc:
                if (
                    attempt + 1 >= self._max_attempts
                    or exc.code
                    not in {
                        "transport_backpressure",
                        "transport_unavailable",
                    }
                ):
                    raise
        raise RemoteProtocolError("transport_unavailable")

    def _exchange(self, encoded: bytes) -> Mapping[str, Any]:
        connection: http.client.HTTPSConnection | None = None
        try:
            context = self._tls_context_provider()
            _validate_client_context(context)
            connection = http.client.HTTPSConnection(
                self._hostname,
                self._port,
                timeout=self._timeout,
                context=context,
            )
            connection.request(
                "POST",
                self._path,
                body=encoded,
                headers={
                    "Accept": _CONTENT_TYPE,
                    "Cache-Control": "no-store",
                    "Content-Type": _CONTENT_TYPE,
                },
            )
            response = connection.getresponse()
            raw = response.read(MAX_REMOTE_MESSAGE_BYTES + 1)
            if len(raw) > MAX_REMOTE_MESSAGE_BYTES:
                raise RemoteProtocolError("invalid_response")
            if response.status != 200:
                code = (
                    "transport_unauthorized"
                    if response.status in {401, 403}
                    else "transport_backpressure"
                    if response.status in {408, 425, 429, 503}
                    else "invalid_response"
                    if 400 <= response.status < 500
                    else "transport_unavailable"
                    if 500 <= response.status < 600
                    else "invalid_response"
                )
                raise RemoteProtocolError(code)
            if not _is_json_content_type(response.getheader("Content-Type")):
                raise RemoteProtocolError("invalid_response")
            if response.getheader("Content-Encoding") not in {None, "identity"}:
                raise RemoteProtocolError("invalid_response")
            try:
                return _decode_object(raw)
            except _HttpReject as exc:
                raise RemoteProtocolError("invalid_response") from exc
        except RemoteProtocolError:
            raise
        except RemoteHttpConfigurationError:
            raise RemoteProtocolError("transport_unavailable") from None
        except (KeyboardInterrupt, SystemExit):
            raise
        except (
            http.client.HTTPException,
            OSError,
            TimeoutError,
            ssl.SSLError,
        ):
            raise RemoteProtocolError("transport_unavailable") from None
        except BaseException:
            raise RemoteProtocolError("transport_unavailable") from None
        finally:
            if connection is not None:
                connection.close()


def _validate_client_context(context: Any) -> None:
    maximum_version = getattr(context, "maximum_version", None)
    maximum_supported = ssl.TLSVersion.MAXIMUM_SUPPORTED
    if (
        not isinstance(context, ssl.SSLContext)
        or context.verify_mode != ssl.CERT_REQUIRED
        or context.check_hostname is not True
        or context.minimum_version < ssl.TLSVersion.TLSv1_2
        or (
            maximum_version != maximum_supported
            and maximum_version < ssl.TLSVersion.TLSv1_2
        )
    ):
        raise RemoteHttpConfigurationError(
            "TLS context must require hostname verification and TLS 1.2+"
        )


def _headers(scope: Mapping[str, Any]) -> dict[bytes, tuple[bytes, ...]]:
    raw_headers = scope.get("headers", ())
    if not isinstance(raw_headers, (list, tuple)):
        raise _HttpReject(400, "invalid_headers")
    if len(raw_headers) > MAX_HTTP_HEADERS:
        raise _HttpReject(431, "headers_too_large")
    total = 0
    grouped: dict[bytes, list[bytes]] = {}
    for raw in raw_headers:
        if (
            not isinstance(raw, (list, tuple))
            or len(raw) != 2
            or not isinstance(raw[0], bytes)
            or not isinstance(raw[1], bytes)
        ):
            raise _HttpReject(400, "invalid_headers")
        name = raw[0].lower()
        value = raw[1]
        if (
            not _HTTP_TOKEN.fullmatch(name)
            or b"\x00" in value
            or b"\r" in value
            or b"\n" in value
        ):
            raise _HttpReject(400, "invalid_headers")
        total += len(name) + len(value)
        if total > MAX_HTTP_HEADER_BYTES:
            raise _HttpReject(431, "headers_too_large")
        grouped.setdefault(name, []).append(value)
    return {name: tuple(values) for name, values in grouped.items()}


def _validate_request_headers(
    headers: Mapping[bytes, tuple[bytes, ...]],
) -> None:
    if _FORBIDDEN_CREDENTIAL_HEADERS.intersection(headers):
        raise _HttpReject(400, "credential_header_forbidden")
    content_types = headers.get(b"content-type", ())
    if (
        len(content_types) != 1
        or not _is_json_content_type(content_types[0])
    ):
        raise _HttpReject(415, "unsupported_media_type")
    encodings = headers.get(b"content-encoding", ())
    if len(encodings) > 1 or (
        encodings and encodings[0].strip().lower() != b"identity"
    ):
        raise _HttpReject(415, "content_encoding_forbidden")
    lengths = headers.get(b"content-length", ())
    if len(lengths) > 1:
        raise _HttpReject(400, "invalid_content_length")
    if lengths:
        raw_length = lengths[0]
        if not raw_length.isdigit():
            raise _HttpReject(400, "invalid_content_length")
        try:
            length = int(raw_length.decode("ascii"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise _HttpReject(400, "invalid_content_length") from exc
        if length < 1:
            raise _HttpReject(400, "invalid_content_length")
        if raw_length != str(length).encode("ascii"):
            raise _HttpReject(400, "invalid_content_length")
        if length > MAX_REMOTE_MESSAGE_BYTES:
            raise _HttpReject(413, "request_too_large")


async def _read_body(
    receive: AsgiReceive,
    headers: Mapping[bytes, tuple[bytes, ...]],
) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        message = await receive()
        message_type = message.get("type")
        if message_type == "http.disconnect":
            raise _HttpReject(400, "request_disconnected")
        if message_type != "http.request":
            raise _HttpReject(400, "invalid_request_body")
        chunk = message.get("body", b"")
        if not isinstance(chunk, bytes):
            raise _HttpReject(400, "invalid_request_body")
        total += len(chunk)
        if total > MAX_REMOTE_MESSAGE_BYTES:
            raise _HttpReject(413, "request_too_large")
        if chunk:
            chunks.append(chunk)
        more_body = message.get("more_body", False)
        if not isinstance(more_body, bool):
            raise _HttpReject(400, "invalid_request_body")
        if not more_body:
            break
    body = b"".join(chunks)
    if not body:
        raise _HttpReject(400, "invalid_request_body")
    lengths = headers.get(b"content-length", ())
    if lengths and int(lengths[0].decode("ascii")) != len(body):
        raise _HttpReject(400, "invalid_content_length")
    return body


def _decode_object(raw: bytes) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8")

        def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in pairs:
                if key in result:
                    raise _DuplicateJsonKey
                result[key] = value
            return result

        value = json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        _DuplicateJsonKey,
        ValueError,
        RecursionError,
    ) as exc:
        raise _HttpReject(400, "invalid_json") from exc
    if not isinstance(value, dict):
        raise _HttpReject(400, "invalid_json")
    return value


def _is_json_content_type(value: Any) -> bool:
    if isinstance(value, bytes):
        try:
            value = value.decode("ascii")
        except UnicodeDecodeError:
            return False
    if not isinstance(value, str):
        return False
    parts = [part.strip().lower() for part in value.split(";")]
    if not parts or parts[0] != _CONTENT_TYPE:
        return False
    return all(part in {"charset=utf-8", "charset=\"utf-8\""} for part in parts[1:])


def _valid_http_path(path: Any) -> bool:
    return (
        isinstance(path, str)
        and path.startswith("/")
        and not path.startswith("//")
        and len(path) <= 255
        and path.isascii()
        and all(0x21 <= ord(character) <= 0x7E for character in path)
        and "?" not in path
        and "#" not in path
    )


def _pinned_identity_digest(worker_id: str, tenant_id: str) -> str:
    encoded = json.dumps(
        {
            "schema": "pinned_worker_identity_v1",
            "tenant_id": tenant_id,
            "worker_id": worker_id,
        },
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


async def _send_json(
    send: AsgiSend,
    status: int,
    payload: Mapping[str, Any],
    *,
    extra_headers: tuple[tuple[bytes, bytes], ...] = (),
) -> None:
    try:
        body = json.dumps(
            dict(payload),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        status = 503
        body = b'{"error":"control_unavailable"}'
    if len(body) > MAX_REMOTE_MESSAGE_BYTES:
        status = 503
        body = b'{"error":"control_unavailable"}'
    headers = (
        (b"cache-control", b"no-store"),
        (b"content-length", str(len(body)).encode("ascii")),
        (b"content-type", b"application/json"),
        (b"x-content-type-options", b"nosniff"),
        *extra_headers,
    )
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": list(headers),
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": body,
            "more_body": False,
        }
    )


__all__ = [
    "AsgiTlsPeerAuthenticator",
    "HttpsRemoteTransport",
    "PinnedCertificateIdentityVerifier",
    "PinnedWorkerCertificate",
    "RemoteHttpASGIApp",
    "RemoteHttpConfigurationError",
    "RemotePeerAuthenticationError",
    "TlsPeerEvidence",
]
