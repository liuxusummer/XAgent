from __future__ import annotations

import json
import urllib.error
import unittest
from unittest.mock import patch

from src.config import SessionConfig
from src.core.XAgent import XAgent
from src.core.agent_loop import AgentContext
from src.core.llm import (
    BaseSession,
    ChatResponse,
    MixinSession,
    NativeToolClient,
    ProviderRequestError,
    ToolCall,
    ToolClient,
)
from src.tools.file_index import EmbeddingConfig


_SECRET = "provider-secret-marker-7f39a"


class Response:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def __enter__(self) -> "Response":
        return self

    def __exit__(self, *_args) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


class FailingSession(BaseSession):
    def ask(self, prompt: str) -> str:
        raise RuntimeError(f"upstream leaked {_SECRET}: {prompt}")


class ProviderCredentialRepresentationTests(unittest.TestCase):
    def test_credential_holders_and_compositions_have_safe_repr(self) -> None:
        base = BaseSession(
            api_key=_SECRET,
            base_url=f"https://{_SECRET}@example.invalid/v1",
            model="model-safe",
            system=_SECRET,
            history=[{"role": "user", "content": _SECRET}],
        )
        embedding = EmbeddingConfig(
            enabled=True,
            apikey=_SECRET,
            apibase=f"https://{_SECRET}@example.invalid/v1",
            model="embedding-safe",
            dimension=16,
        )
        values = (
            SessionConfig(
                name="primary",
                apikey=_SECRET,
                apibase=f"https://{_SECRET}@example.invalid/v1",
                model="model-safe",
                extra={
                    "file_index_embedding": {"apikey": _SECRET}
                },
            ),
            base,
            ToolClient(backend=base, last_tools=_SECRET),
            NativeToolClient(backend=base),
            MixinSession(sessions=[base]),
            embedding,
            AgentContext(
                file_index_embedding={
                    "file_index_embedding": {"apikey": _SECRET}
                }
            ),
            ToolCall(name="tool-safe", args={"token": _SECRET}, id="1"),
            ChatResponse(
                thinking="",
                content="safe",
                tool_calls=[],
                raw={"authorization": _SECRET},
            ),
        )

        for value in values:
            with self.subTest(type=type(value).__name__):
                self.assertNotIn(_SECRET, repr(value))

        for field_name in (
            "api_key",
            "base_url",
            "file_index_embedding_config",
        ):
            self.assertFalse(
                XAgent.__dataclass_fields__[field_name].repr
            )
        self.assertNotIn(_SECRET, embedding.fingerprint)
        self.assertNotIn("example.invalid", embedding.fingerprint)


class ProviderFailureSanitizationTests(unittest.TestCase):
    def _session(self) -> BaseSession:
        return BaseSession(
            api_key=_SECRET,
            base_url=f"https://{_SECRET}@example.invalid/v1",
            model="model-safe",
            max_retries=0,
        )

    def test_transport_diagnostics_and_causes_are_removed(self) -> None:
        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.URLError(
                f"upstream diagnostic {_SECRET}"
            ),
        ):
            with self.assertRaises(ProviderRequestError) as raised:
                self._session().raw_ask(
                    [{"role": "user", "content": "safe"}]
                )

        self.assertEqual(
            raised.exception.reason_code,
            "provider_request_failed",
        )
        self.assertEqual(
            str(raised.exception),
            "provider_request_failed",
        )
        self.assertNotIn(_SECRET, repr(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)

    def test_provider_body_is_not_copied_into_validation_error(self) -> None:
        payload = json.dumps(
            {"diagnostic": _SECRET},
            separators=(",", ":"),
        ).encode()
        with patch(
            "urllib.request.urlopen",
            return_value=Response(payload),
        ):
            with self.assertRaises(ProviderRequestError) as raised:
                self._session().raw_ask(
                    [{"role": "user", "content": "safe"}]
                )

        self.assertEqual(
            raised.exception.reason_code,
            "provider_response_invalid",
        )
        self.assertNotIn(_SECRET, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)

    def test_malformed_provider_json_does_not_survive_as_cause(self) -> None:
        with patch(
            "urllib.request.urlopen",
            return_value=Response(
                b'{"diagnostic":"provider-secret-marker-7f39a",'
            ),
        ):
            with self.assertRaises(ProviderRequestError) as raised:
                self._session().raw_ask(
                    [{"role": "user", "content": "safe"}]
                )

        self.assertEqual(
            raised.exception.reason_code,
            "provider_response_invalid",
        )
        self.assertNotIn(_SECRET, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)

    def test_failover_does_not_rethrow_child_diagnostic(self) -> None:
        session = FailingSession(
            api_key=_SECRET,
            base_url="https://example.invalid/v1",
            model="model-safe",
        )
        mixin = MixinSession(sessions=[session], max_retries=0)

        with patch("time.sleep"):
            with self.assertRaises(ProviderRequestError) as raised:
                mixin.ask(_SECRET)

        self.assertEqual(
            raised.exception.reason_code,
            "provider_pool_exhausted",
        )
        self.assertNotIn(_SECRET, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)


if __name__ == "__main__":
    unittest.main()
