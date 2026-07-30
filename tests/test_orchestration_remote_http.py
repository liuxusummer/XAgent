from __future__ import annotations

import asyncio
import hashlib
import json
import ssl
import threading
import unittest
from unittest import mock

from src.orchestration.remote_http import (
    AsgiTlsPeerAuthenticator,
    HttpsRemoteTransport,
    PinnedCertificateIdentityVerifier,
    PinnedWorkerCertificate,
    RemoteHttpASGIApp,
    RemoteHttpConfigurationError,
    RemotePeerAuthenticationError,
    TlsPeerEvidence,
)
from src.orchestration.remote_protocol import (
    MAX_REMOTE_MESSAGE_BYTES,
    AuthenticatedWorker,
    RemoteOperation,
    RemoteProtocolError,
    make_request,
    make_response,
    parse_request,
    parse_response,
)

CERTIFICATE = (
    "-----BEGIN CERTIFICATE-----\n"
    "VEVTVA==\n"
    "-----END CERTIFICATE-----"
)


class _Verifier:
    production_security_ready = True

    def __init__(self) -> None:
        self.evidence = []

    def verify(self, evidence):
        self.evidence.append(evidence)
        return AuthenticatedWorker("worker-1", "tenant-1", "a" * 64)


class _Control:
    production_security_ready = True

    def __init__(self) -> None:
        self.calls = []

    def handle(self, identity, wire_request):
        self.calls.append((identity, wire_request))
        request = parse_request(wire_request)
        if request.worker_id != identity.worker_id:
            return make_response(
                request,
                ok=False,
                body={"error_code": "worker_identity_mismatch"},
            ).to_wire()
        return make_response(
            request,
            ok=True,
            body={
                "accepted": True,
                "protocol_version": 1,
                "worker_id": request.worker_id,
                "instance_id": request.instance_id,
                "registration_digest": "b" * 64,
            },
        ).to_wire()


class _BlockingControl(_Control):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def handle(self, identity, wire_request):
        self.entered.set()
        if not self.release.wait(2):
            raise RuntimeError("test control release timed out")
        return super().handle(identity, wire_request)


class _FlakyControl(_Control):
    def __init__(self) -> None:
        self.raise_on_readiness = False
        self.calls = []

    @property
    def production_security_ready(self):
        if self.raise_on_readiness:
            raise RuntimeError("readiness-secret")
        return True


class _InvalidResponseControl(_Control):
    class Response:
        def keys(self):
            raise RuntimeError("control-private-path")

    def handle(self, identity, wire_request):
        self.calls.append((identity, wire_request))
        return self.Response()


def _registration_request(*, worker_id: str = "worker-1") -> bytes:
    request = make_request(
        RemoteOperation.REGISTER,
        request_id="request-1",
        worker_id=worker_id,
        instance_id="instance-1",
        body={
            "runtime_version": "1",
            "capabilities": ["activity.tool"],
            "resource_keys": ["workspace"],
            "activity_kinds": ["tool"],
            "max_concurrency": 1,
        },
    )
    return json.dumps(
        request.to_wire(),
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _tls_scope() -> dict:
    return {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "https",
        "path": "/v1/remote-worker",
        "raw_path": b"/v1/remote-worker",
        "query_string": b"",
        "headers": [],
        "extensions": {
            "tls": {
                "server_cert": None,
                "client_cert_chain": [CERTIFICATE],
                "client_cert_name": "CN=worker-1",
                "client_cert_error": None,
                "tls_version": 0x0304,
                "cipher_suite": 0x1301,
            }
        },
    }


async def _exchange(
    app,
    body: bytes,
    *,
    scope: dict | None = None,
    chunks: tuple[bytes, ...] | None = None,
) -> tuple[int, dict[bytes, bytes], bytes]:
    request_scope = _tls_scope() if scope is None else scope
    if not request_scope.get("headers"):
        request_scope["headers"] = [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode("ascii")),
        ]
    if chunks is None:
        chunks = (body,)
    messages = [
        {
            "type": "http.request",
            "body": chunk,
            "more_body": index < len(chunks) - 1,
        }
        for index, chunk in enumerate(chunks)
    ]
    sent = []

    async def receive():
        return messages.pop(0)

    async def send(message):
        sent.append(message)

    await app(request_scope, receive, send)
    start, response = sent
    headers = {name: value for name, value in start["headers"]}
    return start["status"], headers, response["body"]


class RemoteHttpASGITests(unittest.TestCase):
    def setUp(self) -> None:
        self.verifier = _Verifier()
        self.control = _Control()
        self.app = RemoteHttpASGIApp(
            self.control,
            AsgiTlsPeerAuthenticator(self.verifier),
        )

    def exchange(self, body: bytes, **kwargs):
        return asyncio.run(_exchange(self.app, body, **kwargs))

    def test_verified_tls_identity_is_injected_out_of_band(self) -> None:
        status, headers, body = self.exchange(_registration_request())

        response = parse_response(json.loads(body))
        self.assertEqual(status, 200)
        self.assertTrue(response.ok)
        self.assertEqual(headers[b"cache-control"], b"no-store")
        self.assertEqual(len(self.control.calls), 1)
        identity, _wire = self.control.calls[0]
        self.assertEqual(identity.worker_id, "worker-1")
        self.assertEqual(self.verifier.evidence[0].tls_version, 0x0304)
        self.assertNotIn(CERTIFICATE, repr(self.verifier.evidence[0]))

    def test_request_identity_cannot_override_tls_identity(self) -> None:
        status, _headers, body = self.exchange(
            _registration_request(worker_id="worker-2")
        )

        response = parse_response(json.loads(body))
        self.assertEqual(status, 200)
        self.assertFalse(response.ok)
        self.assertEqual(
            response.body["error_code"],
            "worker_identity_mismatch",
        )
        self.assertEqual(self.control.calls[0][0].worker_id, "worker-1")

    def test_missing_stale_or_failed_tls_evidence_is_rejected(self) -> None:
        scopes = []
        missing = _tls_scope()
        missing["extensions"] = {}
        scopes.append(missing)
        failed = _tls_scope()
        failed["extensions"]["tls"]["client_cert_error"] = "bad certificate"
        scopes.append(failed)
        stale = _tls_scope()
        stale["extensions"]["tls"]["tls_version"] = 0x0302
        scopes.append(stale)
        empty = _tls_scope()
        empty["extensions"]["tls"]["client_cert_chain"] = []
        scopes.append(empty)

        for scope in scopes:
            with self.subTest(scope=scope):
                status, _headers, body = self.exchange(
                    _registration_request(),
                    scope=scope,
                )
                self.assertEqual(status, 401)
                self.assertEqual(json.loads(body), {"error": "unauthorized"})
        self.assertEqual(self.control.calls, [])

    def test_duplicate_json_and_chunked_oversize_are_rejected_pre_control(self) -> None:
        duplicate = (
            b'{"protocol":"xagent.remote-worker",'
            b'"protocol":"xagent.remote-worker"}'
        )
        duplicate_status, _headers, _body = self.exchange(duplicate)

        oversize_scope = _tls_scope()
        oversize_scope["headers"] = [(b"content-type", b"application/json")]
        oversized = b"x" * (MAX_REMOTE_MESSAGE_BYTES + 1)
        oversize_status, _headers, _body = self.exchange(
            oversized,
            scope=oversize_scope,
            chunks=(oversized[:32000], oversized[32000:]),
        )

        self.assertEqual(duplicate_status, 400)
        self.assertEqual(oversize_status, 413)
        self.assertEqual(self.control.calls, [])

    def test_credential_headers_compression_and_wrong_media_type_are_rejected(self):
        cases = (
            [(b"content-type", b"text/plain")],
            [
                (b"content-type", b"application/json"),
                (b"content-encoding", b"gzip"),
            ],
            [
                (b"content-type", b"application/json"),
                (b"authorization", b"Bearer do-not-accept"),
            ],
            [
                (b"content-type", b"application/json"),
                (b"x-worker-id", b"worker-2"),
            ],
        )
        for headers in cases:
            scope = _tls_scope()
            scope["headers"] = headers
            with self.subTest(headers=headers):
                status, _response_headers, _body = self.exchange(
                    _registration_request(),
                    scope=scope,
                )
                self.assertIn(status, {400, 415})
        self.assertEqual(self.control.calls, [])

    def test_query_header_smuggling_and_noncanonical_lengths_are_rejected(self):
        cases = []
        query = _tls_scope()
        query["query_string"] = b"worker=other"
        cases.append(query)
        invalid_name = _tls_scope()
        invalid_name["headers"] = [
            (b"content-type", b"application/json"),
            (b"bad name", b"value"),
        ]
        cases.append(invalid_name)
        invalid_value = _tls_scope()
        invalid_value["headers"] = [
            (b"content-type", b"application/json"),
            (b"x-trace", b"safe\r\nx-worker-id: worker-2"),
        ]
        cases.append(invalid_value)
        noncanonical_length = _tls_scope()
        noncanonical_length["headers"] = [
            (b"content-type", b"application/json"),
            (b"content-length", b"000123"),
        ]
        cases.append(noncanonical_length)

        for scope in cases:
            with self.subTest(scope=scope):
                status, _headers, _body = self.exchange(
                    _registration_request(),
                    scope=scope,
                )
                self.assertEqual(status, 400)
        self.assertEqual(self.control.calls, [])

        encoded_path = _tls_scope()
        encoded_path["raw_path"] = b"/v1/remote%2Dworker"
        status, _headers, _body = self.exchange(
            _registration_request(),
            scope=encoded_path,
        )
        self.assertEqual(status, 404)
        self.assertEqual(self.control.calls, [])

    def test_slow_request_body_is_bounded_by_timeout(self) -> None:
        app = RemoteHttpASGIApp(
            self.control,
            AsgiTlsPeerAuthenticator(self.verifier),
            body_timeout_seconds=0.01,
        )

        async def scenario():
            sent = []
            scope = _tls_scope()
            scope["headers"] = [
                (b"content-type", b"application/json"),
            ]

            async def receive():
                await asyncio.Event().wait()

            async def send(message):
                sent.append(message)

            await app(scope, receive, send)
            return sent

        start, response = asyncio.run(scenario())

        self.assertEqual(start["status"], 408)
        self.assertEqual(
            json.loads(response["body"]),
            {"error": "request_timeout"},
        )
        self.assertEqual(self.control.calls, [])

    def test_control_readiness_is_rechecked_for_every_request(self) -> None:
        self.control.production_security_ready = False

        status, _headers, body = self.exchange(_registration_request())

        self.assertEqual(status, 503)
        self.assertEqual(
            json.loads(body),
            {"error": "control_unavailable"},
        )
        self.assertEqual(self.control.calls, [])

    def test_readiness_exception_is_a_safe_transport_failure(self) -> None:
        control = _FlakyControl()
        app = RemoteHttpASGIApp(
            control,
            AsgiTlsPeerAuthenticator(self.verifier),
        )
        control.raise_on_readiness = True

        status, _headers, body = asyncio.run(
            _exchange(app, _registration_request())
        )

        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body), {"error": "control_unavailable"})
        self.assertNotIn("readiness-secret", body.decode())
        self.assertEqual(control.calls, [])

    def test_control_response_serialization_failure_is_redacted(self) -> None:
        control = _InvalidResponseControl()
        app = RemoteHttpASGIApp(
            control,
            AsgiTlsPeerAuthenticator(self.verifier),
        )

        status, _headers, body = asyncio.run(
            _exchange(app, _registration_request())
        )

        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body), {"error": "control_unavailable"})
        self.assertNotIn("control-private-path", body.decode())
        self.assertEqual(len(control.calls), 1)

    def test_verifier_revocation_is_rechecked_without_reading_body(self) -> None:
        self.verifier.production_security_ready = False

        status, _headers, body = self.exchange(_registration_request())

        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body), {"error": "control_unavailable"})
        self.assertEqual(self.control.calls, [])

    def test_inflight_bound_rejects_queue_growth(self) -> None:
        control = _BlockingControl()
        app = RemoteHttpASGIApp(
            control,
            AsgiTlsPeerAuthenticator(self.verifier),
            max_inflight=1,
            admission_timeout_seconds=0.01,
        )

        async def scenario():
            first = asyncio.create_task(
                _exchange(app, _registration_request())
            )
            ready = await asyncio.to_thread(control.entered.wait, 1)
            self.assertTrue(ready)
            second = await _exchange(app, _registration_request())
            control.release.set()
            return await first, second

        first, second = asyncio.run(scenario())

        self.assertEqual(first[0], 200)
        self.assertEqual(second[0], 503)
        self.assertEqual(json.loads(second[2]), {"error": "control_busy"})
        self.assertEqual(len(control.calls), 1)

    def test_non_production_composition_cannot_be_exposed(self) -> None:
        self.control.production_security_ready = False
        with self.assertRaises(RemoteHttpConfigurationError):
            RemoteHttpASGIApp(
                self.control,
                AsgiTlsPeerAuthenticator(self.verifier),
            )


class PinnedCertificateIdentityVerifierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fingerprint = hashlib.sha256(
            ssl.PEM_cert_to_DER_cert(CERTIFICATE)
        ).hexdigest()
        self.binding = PinnedWorkerCertificate(
            self.fingerprint,
            "worker-1",
            "tenant-1",
        )
        self.evidence = TlsPeerEvidence(
            client_cert_chain=(CERTIFICATE,),
            client_cert_name="CN=worker-1",
            tls_version=0x0304,
            cipher_suite=0x1301,
        )

    def test_leaf_certificate_is_bound_to_exact_protocol_identity(self) -> None:
        verifier = PinnedCertificateIdentityVerifier([self.binding])

        identity = verifier.verify(self.evidence)

        self.assertEqual(identity.worker_id, "worker-1")
        self.assertEqual(identity.tenant_id, "tenant-1")
        self.assertRegex(identity.identity_digest, r"^[0-9a-f]{64}$")
        self.assertNotEqual(identity.identity_digest, self.fingerprint)
        self.assertTrue(verifier.production_security_ready)
        self.assertEqual(verifier.generation, 1)

    def test_atomic_rotation_revokes_old_certificate_and_can_fail_closed(self):
        verifier = PinnedCertificateIdentityVerifier([self.binding])
        other_pem = (
            "-----BEGIN CERTIFICATE-----\n"
            "T1RIRVI=\n"
            "-----END CERTIFICATE-----"
        )
        other_fingerprint = hashlib.sha256(
            ssl.PEM_cert_to_DER_cert(other_pem)
        ).hexdigest()
        other_evidence = TlsPeerEvidence(
            client_cert_chain=(other_pem,),
            client_cert_name="CN=worker-1",
            tls_version=0x0304,
            cipher_suite=0x1301,
        )

        generation = verifier.replace(
            [
                PinnedWorkerCertificate(
                    other_fingerprint,
                    "worker-1",
                    "tenant-1",
                )
            ]
        )

        self.assertEqual(generation, 2)
        with self.assertRaises(RemotePeerAuthenticationError):
            verifier.verify(self.evidence)
        rotated = verifier.verify(other_evidence)
        self.assertEqual(rotated.worker_id, "worker-1")
        self.assertEqual(
            rotated.identity_digest,
            self.binding.identity.identity_digest,
        )
        self.assertEqual(verifier.replace([]), 3)
        self.assertFalse(verifier.production_security_ready)
        with self.assertRaises(RemotePeerAuthenticationError):
            verifier.verify(other_evidence)

    def test_invalid_or_duplicate_bindings_are_rejected(self) -> None:
        with self.assertRaises(RemoteHttpConfigurationError):
            PinnedCertificateIdentityVerifier([])
        with self.assertRaises(RemoteHttpConfigurationError):
            PinnedCertificateIdentityVerifier(
                [self.binding, self.binding]
            )
        other_fingerprint = hashlib.sha256(b"other-certificate").hexdigest()
        with self.assertRaises(RemoteHttpConfigurationError):
            PinnedCertificateIdentityVerifier(
                [
                    self.binding,
                    PinnedWorkerCertificate(
                        other_fingerprint,
                        "worker-1",
                        "tenant-2",
                    ),
                ]
            )
        with self.assertRaises(RemoteHttpConfigurationError):
            PinnedWorkerCertificate(
                "not-a-digest",
                "worker-1",
                "tenant-1",
            )


class _HttpResponse:
    def __init__(
        self,
        body: bytes,
        *,
        status: int = 200,
        content_type: str = "application/json",
        content_encoding: str | None = None,
    ) -> None:
        self.status = status
        self._body = body
        self._headers = {
            "Content-Type": content_type,
            "Content-Encoding": content_encoding,
        }

    def read(self, maximum: int) -> bytes:
        return self._body[:maximum]

    def getheader(self, name: str):
        return self._headers.get(name)


class _HttpsConnection:
    response = None
    instances = []
    failures = []

    def __init__(self, host, port, *, timeout, context):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.context = context
        self.requests = []
        self.closed = False
        self.__class__.instances.append(self)

    def request(self, method, path, *, body, headers):
        self.requests.append((method, path, body, headers))
        if self.__class__.failures:
            failure = self.__class__.failures.pop(0)
            if failure is not None:
                raise failure

    def getresponse(self):
        return self.__class__.response

    def close(self):
        self.closed = True


class HttpsRemoteTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = ssl.create_default_context()
        self.request = make_request(
            RemoteOperation.REGISTER,
            request_id="request-client-1",
            worker_id="worker-1",
            instance_id="instance-1",
            body={
                "runtime_version": "1",
                "capabilities": ["activity.tool"],
                "resource_keys": ["workspace"],
                "activity_kinds": ["tool"],
                "max_concurrency": 1,
            },
        )
        response = make_response(
            self.request,
            ok=True,
            body={
                "accepted": True,
                "protocol_version": 1,
                "worker_id": "worker-1",
                "instance_id": "instance-1",
                "registration_digest": "c" * 64,
            },
        )
        _HttpsConnection.response = _HttpResponse(
            json.dumps(response.to_wire()).encode()
        )
        _HttpsConnection.instances = []
        _HttpsConnection.failures = []

    def test_client_uses_fixed_https_origin_without_proxy_or_redirect(self) -> None:
        with mock.patch(
            "src.orchestration.remote_http.http.client.HTTPSConnection",
            _HttpsConnection,
        ):
            transport = HttpsRemoteTransport(
                "https://control.example:8443",
                lambda: self.context,
                timeout_seconds=2,
            )
            raw = transport(self.request.to_wire())

        response = parse_response(raw)
        connection = _HttpsConnection.instances[0]
        self.assertTrue(response.ok)
        self.assertEqual(
            (connection.host, connection.port, connection.timeout),
            ("control.example", 8443, 2.0),
        )
        method, path, _body, headers = connection.requests[0]
        self.assertEqual((method, path), ("POST", "/v1/remote-worker"))
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertTrue(connection.closed)

    def test_client_rejects_insecure_contexts_and_ambiguous_origins(self) -> None:
        insecure = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        insecure.check_hostname = False
        insecure.verify_mode = ssl.CERT_NONE
        invalid_origins = (
            "http://control.example",
            "https://user:secret@control.example",
            "https://control.example/path",
            "https://control.example?target=other",
        )

        with self.assertRaises(RemoteHttpConfigurationError):
            HttpsRemoteTransport(
                "https://control.example",
                lambda: insecure,
            )
        for origin in invalid_origins:
            with self.subTest(origin=origin):
                with self.assertRaises(RemoteHttpConfigurationError):
                    HttpsRemoteTransport(origin, lambda: self.context)
        for path in ("//other.example", "/worker\nx-header:value", "worker"):
            with self.subTest(path=path):
                with self.assertRaises(RemoteHttpConfigurationError):
                    HttpsRemoteTransport(
                        "https://control.example",
                        lambda: self.context,
                        path=path,
                    )

    def test_client_bounds_and_validates_response_before_protocol_parse(self) -> None:
        cases = (
            _HttpResponse(b"{}", status=302),
            _HttpResponse(b"{}", content_type="text/plain"),
            _HttpResponse(b"{" + b"x" * MAX_REMOTE_MESSAGE_BYTES + b"}"),
            _HttpResponse(b'{"duplicate":1,"duplicate":2}'),
        )
        for response in cases:
            with self.subTest(response=response):
                _HttpsConnection.response = response
                _HttpsConnection.instances = []
                with mock.patch(
                    "src.orchestration.remote_http.http.client.HTTPSConnection",
                    _HttpsConnection,
                ):
                    transport = HttpsRemoteTransport(
                        "https://control.example",
                        lambda: self.context,
                    )
                    with self.assertRaises(RemoteProtocolError):
                        transport(self.request.to_wire())
                self.assertTrue(_HttpsConnection.instances[0].closed)

    def test_connection_loss_retries_the_exact_same_wire_intent(self) -> None:
        _HttpsConnection.failures = [OSError("response-lost"), None]
        with mock.patch(
            "src.orchestration.remote_http.http.client.HTTPSConnection",
            _HttpsConnection,
        ):
            transport = HttpsRemoteTransport(
                "https://control.example",
                lambda: self.context,
                max_attempts=2,
            )
            raw = transport(self.request.to_wire())

        self.assertTrue(parse_response(raw).ok)
        self.assertEqual(len(_HttpsConnection.instances), 2)
        first = _HttpsConnection.instances[0].requests[0]
        second = _HttpsConnection.instances[1].requests[0]
        self.assertEqual(first, second)
        self.assertTrue(all(item.closed for item in _HttpsConnection.instances))

    def test_authentication_and_invalid_response_failures_are_not_retried(self):
        cases = (
            _HttpResponse(b"{}", status=401),
            _HttpResponse(b"{}", status=400),
            _HttpResponse(b"not-json"),
        )
        for response in cases:
            with self.subTest(response=response):
                _HttpsConnection.response = response
                _HttpsConnection.instances = []
                _HttpsConnection.failures = []
                with mock.patch(
                    "src.orchestration.remote_http.http.client.HTTPSConnection",
                    _HttpsConnection,
                ):
                    transport = HttpsRemoteTransport(
                        "https://control.example",
                        lambda: self.context,
                        max_attempts=3,
                    )
                    with self.assertRaises(RemoteProtocolError):
                        transport(self.request.to_wire())
                self.assertEqual(len(_HttpsConnection.instances), 1)

    def test_initial_tls_provider_failure_is_redacted(self) -> None:
        def provider():
            raise RuntimeError("private-key-path")

        with self.assertRaises(RemoteHttpConfigurationError) as raised:
            HttpsRemoteTransport(
                "https://control.example",
                provider,
            )

        self.assertNotIn("private-key-path", str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertTrue(raised.exception.__suppress_context__)

    def test_rotating_tls_provider_failure_is_mapped_to_safe_worker_error(self):
        calls = 0

        def provider():
            nonlocal calls
            calls += 1
            if calls == 1:
                return self.context
            raise RuntimeError("svid-private-path")

        transport = HttpsRemoteTransport(
            "https://control.example",
            provider,
        )

        with self.assertRaises(RemoteProtocolError) as raised:
            transport(self.request.to_wire())

        self.assertEqual(raised.exception.code, "transport_unavailable")
        self.assertNotIn("svid-private-path", str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertTrue(raised.exception.__suppress_context__)


if __name__ == "__main__":
    unittest.main()
