from __future__ import annotations

import ipaddress
import selectors
import socket
import socketserver
import threading
import urllib.parse
from dataclasses import dataclass


PROXY_HEADER_LIMIT = 64 * 1024
PROXY_CONNECT_TIMEOUT = 15.0
PROXY_IDLE_TIMEOUT = 60.0


class UnsafeNetworkTargetError(ValueError):
    pass


@dataclass(frozen=True)
class ResolvedEndpoint:
    family: int
    socktype: int
    proto: int
    sockaddr: tuple


def is_public_ip_address(value: str) -> bool:
    address = ipaddress.ip_address(value.split("%", 1)[0])
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return (
        address.is_global
        and not address.is_multicast
        and not address.is_reserved
        and not address.is_unspecified
    )


def resolve_public_endpoint(host: str, port: int) -> list[ResolvedEndpoint]:
    normalized_host = str(host or "").strip().rstrip(".")
    if not normalized_host:
        raise UnsafeNetworkTargetError("network target must include a hostname")
    if not 1 <= int(port) <= 65535:
        raise UnsafeNetworkTargetError("network target port is invalid")

    try:
        literal_address = ipaddress.ip_address(normalized_host.split("%", 1)[0])
    except ValueError:
        literal_address = None
    if literal_address is not None:
        if not is_public_ip_address(normalized_host):
            raise UnsafeNetworkTargetError(
                f"network target resolved to a non-public address: {literal_address}"
            )
        if isinstance(literal_address, ipaddress.IPv6Address):
            sockaddr = (str(literal_address), int(port), 0, 0)
            family = socket.AF_INET6
        else:
            sockaddr = (str(literal_address), int(port))
            family = socket.AF_INET
        return [
            ResolvedEndpoint(
                family=family,
                socktype=socket.SOCK_STREAM,
                proto=socket.IPPROTO_TCP,
                sockaddr=sockaddr,
            )
        ]

    try:
        records = socket.getaddrinfo(
            normalized_host,
            int(port),
            type=socket.SOCK_STREAM,
        )
    except (OSError, UnicodeError) as exc:
        raise UnsafeNetworkTargetError(
            f"failed to resolve network target: {normalized_host}"
        ) from exc
    if not records:
        raise UnsafeNetworkTargetError(
            f"network target resolved to no addresses: {normalized_host}"
        )

    endpoints: list[ResolvedEndpoint] = []
    seen: set[tuple[int, int, int, tuple]] = set()
    for family, socktype, proto, _canonname, sockaddr in records:
        try:
            public = is_public_ip_address(str(sockaddr[0]))
        except ValueError as exc:
            raise UnsafeNetworkTargetError(
                f"network target resolved to an invalid address: {sockaddr[0]}"
            ) from exc
        if not public:
            raise UnsafeNetworkTargetError(
                f"network target resolved to a non-public address: {sockaddr[0]}"
            )
        key = (family, socktype, proto, tuple(sockaddr))
        if key in seen:
            continue
        seen.add(key)
        endpoints.append(
            ResolvedEndpoint(
                family=family,
                socktype=socktype,
                proto=proto,
                sockaddr=tuple(sockaddr),
            )
        )
    if not endpoints:
        raise UnsafeNetworkTargetError(
            f"network target resolved to no usable addresses: {normalized_host}"
        )
    return endpoints


def connect_public_tcp(
    host: str,
    port: int,
    *,
    timeout: float = PROXY_CONNECT_TIMEOUT,
) -> socket.socket:
    endpoints = resolve_public_endpoint(host, port)
    last_error: OSError | None = None
    for endpoint in endpoints:
        outbound = socket.socket(
            endpoint.family,
            endpoint.socktype,
            endpoint.proto,
        )
        outbound.settimeout(timeout)
        try:
            # Connect to the exact address returned by the validated lookup. Passing
            # the hostname here would create a DNS-rebinding time-of-check/time-of-use gap.
            outbound.connect(endpoint.sockaddr)
            return outbound
        except OSError as exc:
            last_error = exc
            outbound.close()
    if last_error is not None:
        raise last_error
    raise OSError("network target has no usable addresses")


def _parse_authority(authority: str, default_port: int) -> tuple[str, int]:
    try:
        parsed = urllib.parse.urlsplit(f"//{authority}")
        port = parsed.port or default_port
    except ValueError as exc:
        raise UnsafeNetworkTargetError("proxy target authority is invalid") from exc
    if not parsed.hostname or parsed.username is not None or parsed.password is not None:
        raise UnsafeNetworkTargetError("proxy target authority is invalid")
    return parsed.hostname, port


def _read_request_head(client: socket.socket) -> tuple[bytes, bytes]:
    data = bytearray()
    while b"\r\n\r\n" not in data:
        chunk = client.recv(min(8192, PROXY_HEADER_LIMIT + 1 - len(data)))
        if not chunk:
            break
        data.extend(chunk)
        if len(data) > PROXY_HEADER_LIMIT:
            raise ValueError("proxy request headers are too large")
    marker = data.find(b"\r\n\r\n")
    if marker < 0:
        raise ValueError("proxy request headers are incomplete")
    return bytes(data[: marker + 4]), bytes(data[marker + 4 :])


def _rewrite_http_request(head: bytes) -> tuple[str, int, bytes]:
    lines = head.decode("iso-8859-1").split("\r\n")
    request_parts = lines[0].split(" ", 2)
    if len(request_parts) != 3:
        raise ValueError("proxy request line is invalid")
    method, target, version = request_parts
    try:
        parsed = urllib.parse.urlsplit(target)
        port = parsed.port
    except ValueError as exc:
        raise UnsafeNetworkTargetError("proxy request URL is invalid") from exc
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "ws"} or not parsed.hostname:
        raise UnsafeNetworkTargetError("proxy request URL must use http or ws")
    if parsed.username is not None or parsed.password is not None:
        raise UnsafeNetworkTargetError("proxy request URL credentials are not allowed")

    origin_target = urllib.parse.urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
    forwarded_lines = [f"{method} {origin_target} {version}"]
    forwarded_lines.extend(
        line
        for line in lines[1:]
        if not line.lower().startswith(("proxy-connection:", "proxy-authorization:"))
    )
    return parsed.hostname, port or 80, "\r\n".join(forwarded_lines).encode("iso-8859-1")


def _relay_bidirectional(left: socket.socket, right: socket.socket) -> None:
    left.settimeout(PROXY_IDLE_TIMEOUT)
    right.settimeout(PROXY_IDLE_TIMEOUT)
    selector = selectors.DefaultSelector()
    selector.register(left, selectors.EVENT_READ, right)
    selector.register(right, selectors.EVENT_READ, left)
    try:
        while selector.get_map():
            events = selector.select(PROXY_IDLE_TIMEOUT)
            if not events:
                break
            for key, _mask in events:
                source = key.fileobj
                destination = key.data
                try:
                    chunk = source.recv(64 * 1024)
                except (BlockingIOError, InterruptedError):
                    continue
                if not chunk:
                    return
                destination.sendall(chunk)
    finally:
        selector.close()


class _PublicProxyHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        client: socket.socket = self.request
        client.settimeout(PROXY_IDLE_TIMEOUT)
        outbound: socket.socket | None = None
        tunnel_established = False
        try:
            head, buffered_body = _read_request_head(client)
            first_line = head.split(b"\r\n", 1)[0].decode("iso-8859-1")
            parts = first_line.split(" ", 2)
            if len(parts) != 3:
                raise ValueError("proxy request line is invalid")

            if parts[0].upper() == "CONNECT":
                host, port = _parse_authority(parts[1], 443)
                outbound = connect_public_tcp(host, port)
                client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                tunnel_established = True
                if buffered_body:
                    outbound.sendall(buffered_body)
            else:
                host, port, rewritten_head = _rewrite_http_request(head)
                outbound = connect_public_tcp(host, port)
                outbound.sendall(rewritten_head)
                if buffered_body:
                    outbound.sendall(buffered_body)
            _relay_bidirectional(client, outbound)
        except UnsafeNetworkTargetError as exc:
            if not tunnel_established:
                _send_proxy_error(client, 403, str(exc))
        except (OSError, ValueError) as exc:
            if not tunnel_established:
                _send_proxy_error(client, 502, str(exc))
        finally:
            if outbound is not None:
                outbound.close()


def _send_proxy_error(client: socket.socket, status: int, message: str) -> None:
    reason = "Forbidden" if status == 403 else "Bad Gateway"
    body = message.encode("utf-8", errors="replace")[:2048]
    response = (
        f"HTTP/1.1 {status} {reason}\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        "X-XAgent-Proxy-Error: 1\r\n"
        "\r\n"
    ).encode("ascii") + body
    try:
        client.sendall(response)
    except OSError:
        pass


class _ThreadingProxyServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def handle_error(self, request, client_address) -> None:
        return None


class PublicEgressProxy:
    def __init__(self) -> None:
        self._server: _ThreadingProxyServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        if self._server is None:
            raise RuntimeError("public egress proxy is not running")
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def start(self) -> PublicEgressProxy:
        if self._server is not None:
            return self
        server = _ThreadingProxyServer(("127.0.0.1", 0), _PublicProxyHandler)
        thread = threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.05},
            name="xagent-public-egress-proxy",
            daemon=True,
        )
        thread.start()
        self._server = server
        self._thread = thread
        return self

    def close(self) -> None:
        server = self._server
        thread = self._thread
        self._server = None
        self._thread = None
        if server is None:
            return
        server.shutdown()
        server.server_close()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2)

    def __enter__(self) -> PublicEgressProxy:
        return self.start()

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.close()
