"""Newline-delimited stdio composition for the stateless MCP adapter.

This module never writes diagnostics to stdout.  Runtime, credentials, and
rate-limit identity are injected by the embedding process; no ambient
credential or connection/session state is inferred here.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, BinaryIO, Callable, TextIO

from .mcp import MAX_MCP_MESSAGE_BYTES, MCPServer


@dataclass(frozen=True, slots=True)
class StdioRequestContext:
    context_id: str | bytes | None
    authorization_context: Any


def encode_json_line(response: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            response,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def serve_stdio(
    server: MCPServer,
    input_stream: BinaryIO | TextIO,
    output_stream: BinaryIO | TextIO,
    *,
    request_context: Callable[[], StdioRequestContext],
) -> None:
    """Serve independent newline-delimited requests until EOF."""

    if not isinstance(server, MCPServer):
        raise TypeError("server must be an MCPServer")
    if not callable(request_context):
        raise TypeError("request_context must be callable")
    while True:
        line = input_stream.readline(MAX_MCP_MESSAGE_BYTES + 2)
        if line == b"" or line == "":
            return
        oversized = _encoded_length(line) > MAX_MCP_MESSAGE_BYTES
        if oversized and not _ends_with_newline(line):
            _drain_line(input_stream)
        try:
            context = request_context()
        except Exception:
            context = StdioRequestContext(
                context_id="stdio-uncredentialed",
                authorization_context=None,
            )
        if not isinstance(context, StdioRequestContext):
            context = StdioRequestContext(
                context_id="stdio-invalid-context",
                authorization_context=None,
            )
        response = server.handle_json(
            line if not oversized else b" " * (MAX_MCP_MESSAGE_BYTES + 1),
            context_id=context.context_id,
            authorization_context=context.authorization_context,
        )
        _write_line(output_stream, encode_json_line(response))


def _drain_line(stream: BinaryIO | TextIO) -> None:
    while True:
        remainder = stream.readline(MAX_MCP_MESSAGE_BYTES + 2)
        if remainder == b"" or remainder == "" or _ends_with_newline(remainder):
            return


def _encoded_length(value: str | bytes) -> int:
    if isinstance(value, bytes):
        return len(value)
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError:
        return MAX_MCP_MESSAGE_BYTES + 1


def _ends_with_newline(value: str | bytes) -> bool:
    return value.endswith(b"\n") if isinstance(value, bytes) else value.endswith("\n")


def _write_line(stream: BinaryIO | TextIO, encoded: bytes) -> None:
    try:
        stream.write(encoded)
    except TypeError:
        stream.write(encoded.decode("utf-8"))
    stream.flush()


__all__ = [
    "StdioRequestContext",
    "encode_json_line",
    "serve_stdio",
]
