from __future__ import annotations

import io
import json
import unittest

from src.orchestration.mcp import (
    InvalidInputTokenBucket,
    JSON_SCHEMA_2020_12,
    MAX_MCP_JSON_DEPTH,
    MAX_MCP_MESSAGE_BYTES,
    MCP_PROTOCOL_VERSION,
    MCPServer,
    PerContextRateLimiter,
)
from src.orchestration.mcp_stdio import StdioRequestContext, serve_stdio
from src.orchestration.protocol import ProtocolResponse
from src.orchestration.runtime import OrchestrationRuntime


class _Runtime:
    def __init__(self) -> None:
        self.calls: list[tuple[dict, object]] = []
        self.response = ProtocolResponse.success(
            "runtime",
            {
                "run_id": "run-1",
                "status": "running",
                "terminal": False,
                "last_event_sequence": 3,
                "projection_version": 1,
                "node_status_counts": {"running": 1},
                "attempt_status_counts": {},
            },
        )

    def handle(self, payload, *, authorization_context=None):
        self.calls.append((dict(payload), authorization_context))
        return self.response


def _meta(*, version: str = MCP_PROTOCOL_VERSION) -> dict:
    return {
        "io.modelcontextprotocol/protocolVersion": version,
        "io.modelcontextprotocol/clientInfo": {
            "name": "unit-client",
            "version": "1.0.0",
        },
        "io.modelcontextprotocol/clientCapabilities": {},
    }


def _request(
    method: str,
    *,
    request_id: str | int = "request-1",
    params: dict | None = None,
) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": {"_meta": _meta()} if params is None else params,
    }


class MCPServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime = _Runtime()
        self.server = MCPServer(self.runtime)

    def handle(self, request: dict, **overrides) -> dict:
        return self.server.handle(
            request,
            context_id=overrides.get("context_id", "transport-principal"),
            authorization_context=overrides.get(
                "authorization_context",
                {"principal": "trusted"},
            ),
        )

    def test_discover_is_stateless_and_advertises_only_tools(self) -> None:
        response = self.handle(_request("server/discover"))

        self.assertEqual(response["jsonrpc"], "2.0")
        result = response["result"]
        self.assertEqual(result["resultType"], "complete")
        self.assertEqual(result["supportedVersions"], ["2026-07-28"])
        self.assertEqual(result["capabilities"], {"tools": {}})
        self.assertEqual(result["cacheScope"], "public")
        self.assertGreater(result["ttlMs"], 0)
        self.assertEqual(
            result["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
            "xagent-durable-orchestration",
        )

        initialize = self.handle(_request("initialize", request_id="old"))
        self.assertEqual(initialize["error"]["code"], -32601)

    def test_protocol_version_error_has_final_schema(self) -> None:
        request = _request(
            "server/discover",
            params={"_meta": _meta(version="2025-11-25")},
        )

        response = self.handle(request)

        self.assertEqual(response["error"]["code"], -32022)
        self.assertEqual(
            response["error"]["data"],
            {
                "supported": ["2026-07-28"],
                "requested": "2025-11-25",
            },
        )

    def test_metadata_is_required_and_revalidated_on_every_request(self) -> None:
        first = self.handle(_request("server/discover", request_id=1))
        missing = self.handle(
            _request("tools/list", request_id=2, params={}),
        )
        valid_again = self.handle(_request("tools/list", request_id=3))

        self.assertIn("result", first)
        self.assertEqual(missing["error"]["code"], -32602)
        self.assertIn("result", valid_again)

    def test_optional_client_info_and_safe_meta_extensions_are_accepted(
        self,
    ) -> None:
        without_client = _meta()
        without_client.pop("io.modelcontextprotocol/clientInfo")
        extended = _meta()
        extended["progressToken"] = "progress-1"
        extended["io.modelcontextprotocol/logLevel"] = "warning"
        extended["com.example/trace"] = {"sampled": True}
        extended["io.modelcontextprotocol/clientInfo"]["description"] = (
            "Final protocol client"
        )

        absent_response = self.handle(
            _request("server/discover", params={"_meta": without_client})
        )
        extended_response = self.handle(
            _request("server/discover", params={"_meta": extended})
        )

        self.assertIn("result", absent_response)
        self.assertIn("result", extended_response)

    def test_required_meta_and_nested_unknown_fields_are_strict(self) -> None:
        bad_client = _meta()
        bad_client["io.modelcontextprotocol/clientInfo"]["unknown"] = "extra"
        response = self.handle(
            _request("server/discover", params={"_meta": bad_client})
        )
        self.assertEqual(response["error"]["code"], -32602)

        unknown_meta = _meta()
        unknown_meta["bad key"] = "legacy"
        response = self.handle(
            _request("server/discover", params={"_meta": unknown_meta})
        )
        self.assertEqual(response["error"]["code"], -32602)

        for required in (
            "io.modelcontextprotocol/protocolVersion",
            "io.modelcontextprotocol/clientCapabilities",
        ):
            incomplete = _meta()
            incomplete.pop(required)
            with self.subTest(required=required):
                response = self.handle(
                    _request(
                        "server/discover",
                        params={"_meta": incomplete},
                    )
                )
                self.assertEqual(response["error"]["code"], -32602)

    def test_tools_list_is_complete_deterministic_and_artifact_only(self) -> None:
        first = self.handle(_request("tools/list", request_id=1))["result"]
        second = self.handle(_request("tools/list", request_id=2))["result"]

        self.assertEqual(first["resultType"], "complete")
        self.assertEqual(first["cacheScope"], "public")
        self.assertGreater(first["ttlMs"], 0)
        self.assertEqual(first["tools"], second["tools"])
        self.assertEqual(
            [tool["name"] for tool in first["tools"]],
            [
                "submit",
                "status",
                "events",
                "cancel",
                "pause",
                "resume",
                "recover",
                "resolve_recovery",
                "tick",
            ],
        )
        for tool in first["tools"]:
            schema = tool["inputSchema"]
            self.assertEqual(schema["$schema"], JSON_SCHEMA_2020_12)
            self.assertEqual(schema["type"], "object")
            self.assertIs(schema["additionalProperties"], False)
            self.assertEqual(tool["annotations"]["openWorldHint"], False)
        submit = first["tools"][0]["inputSchema"]
        self.assertEqual(
            set(submit["required"]),
            {"run_id", "workflow_ref", "input_receipt", "parent"},
        )
        self.assertNotIn("raw", submit["properties"])
        self.assertNotIn("input", submit["properties"])
        self.assertIs(
            submit["$defs"]["artifactRef"]["additionalProperties"],
            False,
        )
        self.assertEqual(
            submit["$defs"]["artifactRef"]["properties"]["metadata"],
            {
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": False,
            },
        )

    def test_tool_call_maps_runtime_and_passes_out_of_band_authorization(self) -> None:
        authorization = {"bearer": "DO-NOT-RETURN"}
        response = self.handle(
            _request(
                "tools/call",
                params={
                    "_meta": _meta(),
                    "name": "status",
                    "arguments": {"run_id": "run-1"},
                },
            ),
            authorization_context=authorization,
        )

        result = response["result"]
        self.assertFalse(result["isError"])
        self.assertEqual(
            json.loads(result["content"][0]["text"]),
            result["structuredContent"],
        )
        runtime_request, passed_context = self.runtime.calls[-1]
        self.assertEqual(runtime_request["operation"], "status")
        self.assertEqual(runtime_request["body"], {"run_id": "run-1"})
        self.assertIs(passed_context, authorization)
        self.assertNotIn("DO-NOT-RETURN", json.dumps(response))

    def test_runtime_input_and_business_errors_are_tool_results(self) -> None:
        self.runtime.response = ProtocolResponse.failure(
            "runtime",
            "invalid_input",
            "private /path token=SECRET payload",
        )

        response = self.handle(
            _request(
                "tools/call",
                params={
                    "_meta": _meta(),
                    "name": "status",
                    "arguments": {"run_id": "../bad"},
                },
            )
        )

        self.assertNotIn("error", response)
        self.assertTrue(response["result"]["isError"])
        self.assertEqual(
            response["result"]["structuredContent"]["error"]["code"],
            "invalid_input",
        )
        encoded = json.dumps(response)
        self.assertNotIn("SECRET", encoded)
        self.assertNotIn("/path", encoded)
        self.assertNotIn("payload", encoded)

    def test_runtime_result_is_allowlisted_before_return(self) -> None:
        self.runtime.response = ProtocolResponse.success(
            "runtime",
            {
                "run_id": "run-1",
                "status": "running",
                "payload": "SECRET-PAYLOAD",
                "token": "SECRET-TOKEN",
                "path": "/private/workspace",
                "operator_diagnostics": {
                    "attention_required": True,
                    "diagnostic_count": 2,
                    "automatic_retry_blocked": False,
                    "category_counts": {
                        "approval": 1,
                        "credential_canary": 1,
                    },
                },
            },
        )

        response = self.handle(
            _request(
                "tools/call",
                params={
                    "_meta": _meta(),
                    "name": "status",
                    "arguments": {"run_id": "run-1"},
                },
            )
        )

        encoded = json.dumps(response)
        self.assertNotIn("SECRET", encoded)
        self.assertNotIn("/private", encoded)
        self.assertNotIn("credential_canary", encoded)
        self.assertEqual(
            response["result"]["structuredContent"],
            {
                "run_id": "run-1",
                "status": "running",
                "operator_diagnostics": {
                    "attention_required": True,
                    "diagnostic_count": 2,
                    "automatic_retry_blocked": False,
                    "category_counts": {"approval": 1},
                },
            },
        )

    def test_default_runtime_authorization_fails_closed_as_tool_error(self) -> None:
        runtime = OrchestrationRuntime(
            store=object(),
            artifact_store=object(),
            scheduler_factory=lambda _store, _workflow: object(),
        )
        server = MCPServer(runtime)

        response = server.handle(
            _request(
                "tools/call",
                params={
                    "_meta": _meta(),
                    "name": "status",
                    "arguments": {"run_id": "run-1"},
                },
            ),
            context_id="principal",
            authorization_context={"untrusted": True},
        )

        self.assertNotIn("error", response)
        self.assertTrue(response["result"]["isError"])
        self.assertEqual(
            response["result"]["structuredContent"]["error"]["code"],
            "authorization_denied",
        )

    def test_unknown_tool_and_malformed_call_are_protocol_errors(self) -> None:
        unknown = self.handle(
            _request(
                "tools/call",
                params={
                    "_meta": _meta(),
                    "name": "artifact.ingest",
                    "arguments": {},
                },
            )
        )
        malformed = self.handle(
            _request(
                "tools/call",
                params={
                    "_meta": _meta(),
                    "name": "status",
                    "arguments": [],
                },
            )
        )

        self.assertEqual(unknown["error"]["code"], -32602)
        self.assertEqual(malformed["error"]["code"], -32602)
        self.assertEqual(self.runtime.calls, [])

    def test_batches_unknown_fields_duplicate_keys_and_oversize_are_rejected(
        self,
    ) -> None:
        batch = self.server.handle_json(
            json.dumps([_request("server/discover")]),
            context_id="batch",
        )
        extra = self.handle(
            {
                **_request("server/discover"),
                "session": "legacy",
            }
        )
        duplicate = self.server.handle_json(
            (
                '{"jsonrpc":"2.0","id":1,"id":2,'
                '"method":"server/discover","params":{}}'
            ),
            context_id="duplicate",
        )
        oversized = self.server.handle_json(
            b"{" + b" " * MAX_MCP_MESSAGE_BYTES + b"}",
            context_id="oversized",
        )

        self.assertEqual(batch["error"]["code"], -32600)
        self.assertEqual(extra["error"]["code"], -32600)
        self.assertEqual(duplicate["error"]["code"], -32700)
        self.assertEqual(oversized["error"]["code"], -32600)

    def test_rate_limit_is_bounded_per_transport_context(self) -> None:
        now = [10.0]
        limiter = PerContextRateLimiter(
            limit=1,
            window_seconds=10,
            max_contexts=2,
            clock=lambda: now[0],
        )
        server = MCPServer(self.runtime, rate_limiter=limiter)

        first = server.handle(
            _request("server/discover", request_id=1),
            context_id="principal-a",
        )
        denied = server.handle(
            _request("server/discover", request_id=2),
            context_id="principal-a",
        )
        independent = server.handle(
            _request("server/discover", request_id=3),
            context_id="principal-b",
        )
        now[0] = 21.0
        reset = server.handle(
            _request("server/discover", request_id=4),
            context_id="principal-a",
        )

        self.assertIn("result", first)
        self.assertEqual(denied["error"]["code"], -32900)
        self.assertGreater(denied["error"]["data"]["retryAfterMs"], 0)
        self.assertIn("result", independent)
        self.assertIn("result", reset)

    def test_invalid_input_has_separate_preparse_token_budget(self) -> None:
        now = [10.0]
        server = MCPServer(
            self.runtime,
            rate_limiter=PerContextRateLimiter(
                limit=1,
                window_seconds=60,
                clock=lambda: now[0],
            ),
            invalid_input_limiter=InvalidInputTokenBucket(
                capacity=2,
                refill_seconds=10,
                max_contexts=2,
                clock=lambda: now[0],
            ),
        )
        malformed = server.handle_json(
            b"{",
            context_id="invalid-principal",
        )
        nested: object = "private-invalid-marker"
        for _depth in range(MAX_MCP_JSON_DEPTH + 1):
            nested = [nested]
        too_deep = server.handle(
            nested,
            context_id="invalid-principal",
        )
        limited = server.handle_json(
            b"{",
            context_id="invalid-principal",
        )

        self.assertEqual(malformed["error"]["code"], -32700)
        self.assertEqual(too_deep["error"]["code"], -32600)
        self.assertEqual(limited["error"]["code"], -32901)
        self.assertEqual(
            limited["error"]["message"],
            "Invalid input rate limit exceeded",
        )
        self.assertGreater(limited["error"]["data"]["retryAfterMs"], 0)

        now[0] = 20.0
        valid = server.handle_json(
            json.dumps(_request("server/discover", request_id="valid")).encode(),
            context_id="invalid-principal",
        )
        normal_limited = server.handle_json(
            json.dumps(_request("server/discover", request_id="normal")).encode(),
            context_id="invalid-principal",
        )

        self.assertIn("result", valid)
        self.assertEqual(normal_limited["error"]["code"], -32900)

    def test_invalid_input_anonymous_and_context_churn_share_bounded_state(
        self,
    ) -> None:
        anonymous = MCPServer(
            self.runtime,
            invalid_input_limiter=InvalidInputTokenBucket(
                capacity=1,
                refill_seconds=60,
                max_contexts=1,
                clock=lambda: 10.0,
            ),
        )
        first = anonymous.handle_json(b"{", context_id=None)
        denied = anonymous.handle_json(b"{", context_id=None)
        churned = anonymous.handle_json(b"{", context_id="new-context")

        self.assertEqual(first["error"]["code"], -32700)
        self.assertEqual(denied["error"]["code"], -32901)
        self.assertEqual(churned["error"]["code"], -32901)


class MCPStdioTests(unittest.TestCase):
    def test_stdio_is_newline_delimited_and_rejects_batch_without_noise(self) -> None:
        runtime = _Runtime()
        server = MCPServer(runtime)
        first = json.dumps(
            _request("server/discover", request_id="discover")
        ).encode()
        batch = json.dumps([_request("tools/list")]).encode()
        source = io.BytesIO(first + b"\n" + batch + b"\n")
        destination = io.BytesIO()

        serve_stdio(
            server,
            source,
            destination,
            request_context=lambda: StdioRequestContext(
                context_id="stdio-principal",
                authorization_context={"principal": "trusted"},
            ),
        )

        lines = destination.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        discover = json.loads(lines[0])
        rejected_batch = json.loads(lines[1])
        self.assertEqual(discover["id"], "discover")
        self.assertEqual(discover["result"]["resultType"], "complete")
        self.assertEqual(rejected_batch["error"]["code"], -32600)

    def test_stdio_drains_one_oversized_line_without_echoing_it(self) -> None:
        server = MCPServer(_Runtime())
        oversized = b'{"secret":"' + b"SENSITIVE" * 140_000 + b'"}\n'
        valid = json.dumps(
            _request("server/discover", request_id="after-large")
        ).encode()
        source = io.BytesIO(oversized + valid + b"\n")
        destination = io.BytesIO()

        serve_stdio(
            server,
            source,
            destination,
            request_context=lambda: StdioRequestContext(
                context_id="stdio-principal",
                authorization_context=None,
            ),
        )

        lines = destination.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0])["error"]["code"], -32600)
        self.assertEqual(json.loads(lines[1])["id"], "after-large")
        self.assertNotIn(b"SENSITIVE", destination.getvalue())

    def test_stdio_context_failure_fails_closed_without_exception_text(self) -> None:
        runtime = OrchestrationRuntime(
            store=object(),
            artifact_store=object(),
            scheduler_factory=lambda _store, _workflow: object(),
        )
        server = MCPServer(runtime)
        source = io.BytesIO(
            json.dumps(
                _request(
                    "tools/call",
                    params={
                        "_meta": _meta(),
                        "name": "status",
                        "arguments": {"run_id": "run-1"},
                    },
                )
            ).encode()
            + b"\n"
        )
        destination = io.BytesIO()

        def broken_context() -> StdioRequestContext:
            raise RuntimeError("CREDENTIAL-SECRET /private/path")

        serve_stdio(
            server,
            source,
            destination,
            request_context=broken_context,
        )

        response = json.loads(destination.getvalue())
        self.assertTrue(response["result"]["isError"])
        self.assertEqual(
            response["result"]["structuredContent"]["error"]["code"],
            "authorization_denied",
        )
        self.assertNotIn(b"CREDENTIAL-SECRET", destination.getvalue())
        self.assertNotIn(b"/private/path", destination.getvalue())

    def test_deep_json_errors_are_bounded_and_stdio_continues(self) -> None:
        canary = "DEEP-JSON-PRIVATE-CANARY"
        parser_overflow = (
            b"[" * 10_000
            + json.dumps(canary).encode()
            + b"]" * 10_000
        )
        nested: object = canary
        for _depth in range(MAX_MCP_JSON_DEPTH + 1):
            nested = [nested]
        bounded_request = _request(
            "server/discover",
            request_id="too-deep",
        )
        bounded_request["params"]["_meta"][
            "io.modelcontextprotocol/clientCapabilities"
        ] = nested
        valid = _request("server/discover", request_id="after-deep")
        source = io.BytesIO(
            parser_overflow
            + b"\n"
            + json.dumps(bounded_request).encode()
            + b"\n"
            + json.dumps(valid).encode()
            + b"\n"
        )
        destination = io.BytesIO()
        server = MCPServer(
            _Runtime(),
            rate_limiter=PerContextRateLimiter(limit=1),
        )

        serve_stdio(
            server,
            source,
            destination,
            request_context=lambda: StdioRequestContext(
                context_id="deep-json-principal",
                authorization_context={"private": canary},
            ),
        )

        responses = [
            json.loads(line)
            for line in destination.getvalue().splitlines()
        ]
        self.assertEqual(len(responses), 3)
        self.assertEqual(responses[0]["error"]["code"], -32700)
        self.assertEqual(responses[0]["error"]["message"], "Parse error")
        self.assertEqual(responses[1]["error"]["code"], -32600)
        self.assertEqual(
            responses[1]["error"]["message"],
            "Invalid Request",
        )
        self.assertEqual(responses[2]["id"], "after-deep")
        self.assertIn("result", responses[2])
        self.assertNotIn(
            canary.encode(),
            destination.getvalue(),
        )


if __name__ == "__main__":
    unittest.main()
