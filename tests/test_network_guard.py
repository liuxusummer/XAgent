from __future__ import annotations

import socket
import unittest
from unittest.mock import patch

from src.core.network_guard import (
    UnsafeNetworkTargetError,
    _PublicProxyHandler,
    connect_public_tcp,
    resolve_public_endpoint,
)


class _FakeSocket:
    def __init__(self) -> None:
        self.timeout: float | None = None
        self.connected_to: tuple | None = None
        self.closed = False

    def settimeout(self, timeout: float) -> None:
        self.timeout = timeout

    def connect(self, sockaddr: tuple) -> None:
        self.connected_to = sockaddr

    def close(self) -> None:
        self.closed = True


class NetworkGuardTests(unittest.TestCase):
    def test_resolver_rejects_mixed_public_and_private_answers(self) -> None:
        records = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.8", 443)),
        ]

        with (
            patch("src.core.network_guard.socket.getaddrinfo", return_value=records),
            self.assertRaises(UnsafeNetworkTargetError),
        ):
            resolve_public_endpoint("example.test", 443)

    def test_connection_uses_validated_sockaddr_without_resolving_again(self) -> None:
        records = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
        ]
        fake_socket = _FakeSocket()

        with (
            patch("src.core.network_guard.socket.getaddrinfo", return_value=records) as resolver,
            patch("src.core.network_guard.socket.socket", return_value=fake_socket),
        ):
            result = connect_public_tcp("example.test", 443, timeout=3)

        self.assertIs(result, fake_socket)
        self.assertEqual(fake_socket.connected_to, ("93.184.216.34", 443))
        self.assertEqual(fake_socket.timeout, 3)
        resolver.assert_called_once()

    def test_proxy_rejects_loopback_connect_target(self) -> None:
        client, proxy_side = socket.socketpair()
        try:
            client.sendall(
                b"CONNECT 127.0.0.1:80 HTTP/1.1\r\n"
                b"Host: 127.0.0.1:80\r\n"
                b"\r\n"
            )
            _PublicProxyHandler(proxy_side, ("local", 0), object())
            response = client.recv(4096)
        finally:
            client.close()
            proxy_side.close()

        self.assertTrue(response.startswith(b"HTTP/1.1 403"))
        self.assertIn(b"non-public address", response)


if __name__ == "__main__":
    unittest.main()
