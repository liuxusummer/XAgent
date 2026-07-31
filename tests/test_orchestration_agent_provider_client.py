from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path

from src.core.agent_loop import (
    ActionResult,
    AgentContext,
    BaseHandler,
    run_agent_loop,
)
from src.core.agent_kernel import Principal
from src.orchestration.agent_execution_evidence import (
    AgentExecutionEvidenceCollector,
)
from src.orchestration.agent_provider_client import (
    AgentProviderClientError,
    DurableAgentProviderClient,
)
from src.orchestration.agent_request import (
    AGENT_ACTIVITY_REQUEST_MEDIA_TYPE,
    AgentActivityRequest,
)
from src.orchestration.artifacts import (
    ArtifactKind,
    ArtifactSensitivity,
    LocalArtifactStore,
    canonical_json_bytes,
)
from src.orchestration.provider_access import (
    ProviderAccessBroker,
    ProviderInvocationCompletionMode,
    ProviderOperationRecovery,
    ProviderOperationState,
    ProviderRouteDescriptor,
)
from src.orchestration.remote_execution_journal import (
    RemoteExecutionJournal,
)
from src.orchestration.worker_security import WorkerAuthorization


_UPSTREAM_SECRET = "upstream-secret-agent-provider-client"


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _response_bytes(
    *,
    content: str = "done",
    tool_calls: list[dict] | None = None,
) -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "kind": "agent_provider_response",
            "thinking": "",
            "content": content,
            "tool_calls": tool_calls or [],
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 10,
                "output_tokens": 4,
                "total_tokens": 14,
                "cache_creation_input_tokens": None,
                "cache_read_input_tokens": None,
                "reasoning_tokens": None,
            },
        },
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class _AuthorizationVerifier:
    def verify(
        self,
        authorization: WorkerAuthorization,
        *,
        now: float,
    ) -> bool:
        return (
            type(authorization) is WorkerAuthorization
            and authorization.authorization_id.startswith("auth-")
            and now < authorization.expires_at
        )


class _Invoker:
    def __init__(self, responses: list[bytes]) -> None:
        self.api_key = _UPSTREAM_SECRET
        self.responses = list(responses)
        self.calls: list[tuple[str, bytes, str]] = []

    def invoke(
        self,
        route: ProviderRouteDescriptor,
        payload: bytes,
        *,
        request_id: str,
    ) -> bytes:
        self.calls.append((route.route_id, payload, request_id))
        return self.responses.pop(0)


class _BlockingInvoker(_Invoker):
    def __init__(self, response: bytes) -> None:
        super().__init__([response])
        self.entered = threading.Event()
        self.release = threading.Event()

    def invoke(
        self,
        route: ProviderRouteDescriptor,
        payload: bytes,
        *,
        request_id: str,
    ) -> bytes:
        self.entered.set()
        if not self.release.wait(timeout=2):
            raise TimeoutError("blocking invoker test timed out")
        return super().invoke(
            route,
            payload,
            request_id=request_id,
        )


class _RecoverableInvoker(_Invoker):
    operation_recovery_ready = True

    def __init__(
        self,
        response: bytes,
        *,
        recovery_state: ProviderOperationState,
    ) -> None:
        super().__init__([])
        self.response = response
        self.recovery_state = recovery_state
        self.recovery_calls: list[tuple[str, str, str]] = []
        self._first = True

    def invoke(
        self,
        route: ProviderRouteDescriptor,
        payload: bytes,
        *,
        request_id: str,
    ) -> bytes:
        self.calls.append((route.route_id, payload, request_id))
        if self._first:
            self._first = False
            raise RuntimeError(
                f"provider diagnostic {_UPSTREAM_SECRET}"
            )
        return self.response

    def recover(
        self,
        route: ProviderRouteDescriptor,
        *,
        operation_id: str,
        request_payload_digest: str,
    ) -> ProviderOperationRecovery:
        self.recovery_calls.append(
            (
                route.route_id,
                operation_id,
                request_payload_digest,
            )
        )
        completed = (
            self.recovery_state is ProviderOperationState.COMPLETED
        )
        return ProviderOperationRecovery(
            operation_id=operation_id,
            route_id=route.route_id,
            request_payload_digest=request_payload_digest,
            state=self.recovery_state,
            evidence_digest=_digest(
                f"{operation_id}:{self.recovery_state.value}"
            ),
            verifier_id="gateway-verifier",
            response_digest=(
                hashlib.sha256(self.response).hexdigest()
                if completed
                else None
            ),
            content=self.response if completed else None,
        )


def _authorization(**changes) -> WorkerAuthorization:
    values = {
        "authorization_id": "auth-1",
        "worker_id": "worker-1",
        "tenant_id": "tenant-1",
        "pool_id": "pool-1",
        "run_id": "run-1",
        "node_id": "agent-node",
        "attempt_id": "attempt-1",
        "action_digest": _digest("agent-action"),
        "identity_binding_digest": _digest("identity"),
        "transport_binding_digest": _digest("transport"),
        "rule_id": "agent-rule",
        "maximum_artifact_sensitivity": (
            ArtifactSensitivity.SENSITIVE
        ),
        "issued_at": 90.0,
        "expires_at": 200.0,
    }
    values.update(changes)
    return WorkerAuthorization(**values)


def _route(
    *,
    maximum_request_bytes: int = 1024 * 1024,
) -> ProviderRouteDescriptor:
    return ProviderRouteDescriptor(
        route_id="primary-route",
        tenant_id="tenant-1",
        pool_id="pool-1",
        worker_rule_id="agent-rule",
        provider="openai-compatible",
        model="model-deployment",
        gateway_binding_digest=_digest("gateway"),
        maximum_request_bytes=maximum_request_bytes,
        maximum_response_bytes=1024 * 1024,
    )


class _RuntimeFixture:
    def __init__(
        self,
        root: Path,
        *,
        invoker,
        authorization_source=None,
        durable_journal: bool = True,
        collector_request_digest: str | None = None,
        route: ProviderRouteDescriptor | None = None,
    ) -> None:
        self.clock = _Clock()
        self.store = LocalArtifactStore(root / "artifacts")
        self.request = AgentActivityRequest(
            run_id="run-1",
            node_id="agent-node",
            attempt_id="attempt-1",
            attempt_number=1,
            agent_name="main",
            request_digest=_digest("agent-request"),
            definition_digest=_digest("definition"),
            task="sensitive-agent-task",
        )
        self.request_ref = self.store.put_bytes(
            self.request.to_bytes(),
            media_type=AGENT_ACTIVITY_REQUEST_MEDIA_TYPE,
            kind=ArtifactKind.AGENT_REQUEST,
            sensitivity=self.request.artifact_sensitivity,
            producer_run_id=self.request.run_id,
            producer_node_id=self.request.node_id,
            producer_attempt_id=self.request.attempt_id,
            metadata={"schema": "agent_activity_request_v1"},
        )
        self.collector = AgentExecutionEvidenceCollector(
            run_id=self.request.run_id,
            node_id=self.request.node_id,
            attempt_id=self.request.attempt_id,
            request_digest=(
                collector_request_digest
                or self.request.request_digest
            ),
            request_artifact_digest=self.request_ref.sha256,
            definition_digest=self.request.definition_digest,
            request_sensitivity=self.request_ref.sensitivity,
        )
        journal_path = (
            root / "provider-journal.sqlite"
            if durable_journal
            else ":memory:"
        )
        self.journal = RemoteExecutionJournal(journal_path)
        self.invoker = invoker
        self.broker = ProviderAccessBroker(
            (route or _route(),),
            authorization_verifier=_AuthorizationVerifier(),
            invoker=invoker,
            clock=self.clock,
            recovery_journal=self.journal,
            result_store=self.store,
        )
        self.authorization = _authorization()
        self.authorization_source = (
            authorization_source
            if authorization_source is not None
            else lambda: self.authorization
        )

    def client(self, **changes) -> DurableAgentProviderClient:
        values = {
            "broker": self.broker,
            "authorization_source": self.authorization_source,
            "request": self.request,
            "request_ref": self.request_ref,
            "collector": self.collector,
            "route_id": "primary-route",
            "grant_ttl_seconds": 30.0,
        }
        values.update(changes)
        return DurableAgentProviderClient(**values)


class DurableAgentProviderClientTests(unittest.TestCase):
    def test_safe_turn_checkpoint_restores_history_and_lineage(self):
        with tempfile.TemporaryDirectory() as directory:
            invoker = _Invoker(
                [
                    _response_bytes(content="first"),
                    _response_bytes(content="second"),
                ]
            )
            fixture = _RuntimeFixture(
                Path(directory),
                invoker=invoker,
            )
            first = fixture.client()
            fixture.collector.provider_call_started(turn=1)
            response = first.chat(
                [{"role": "user", "content": "task"}],
                [],
            )
            fixture.collector.provider_call_finished(
                turn=1,
                receipt=response.provider_receipt,
            )
            evidence = fixture.collector.checkpoint_manifest(
                completed_turn=1
            )
            state = first.checkpoint_state()
            restored_collector = (
                AgentExecutionEvidenceCollector.from_checkpoint_manifest(
                    evidence,
                    request_sensitivity=ArtifactSensitivity.SENSITIVE,
                )
            )
            restored = fixture.client(collector=restored_collector)

            restored.restore_checkpoint_state(
                state,
                evidence_manifest=evidence,
            )
            restored_collector.provider_call_started(turn=2)
            second = restored.chat(
                [{"role": "user", "content": "continue"}],
                [],
            )
            restored_collector.provider_call_finished(
                turn=2,
                receipt=second.provider_receipt,
            )

            self.assertEqual(restored.request_count, 2)
            self.assertEqual(len(restored.result_artifact_refs), 2)
            payload = json.loads(invoker.calls[1][1].decode("utf-8"))
            self.assertEqual(
                [message["content"] for message in payload["messages"]],
                ["task", "first", "continue"],
            )
            tampered = dict(state)
            tampered["authorization_digest"] = _digest("foreign")
            fresh = fixture.client(collector=restored_collector)
            with self.assertRaises(
                AgentProviderClientError
            ) as raised:
                fresh.restore_checkpoint_state(
                    tampered,
                    evidence_manifest=evidence,
                )
            self.assertEqual(
                raised.exception.reason_code,
                "agent_provider_checkpoint_invalid",
            )

    def test_real_loop_preserves_history_and_emits_durable_receipts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            invoker = _Invoker(
                [
                    _response_bytes(
                        content="",
                        tool_calls=[
                            {
                                "name": "echo",
                                "arguments": {"value": "safe"},
                                "id": "call-1",
                            }
                        ],
                    ),
                    _response_bytes(content="complete"),
                ]
            )
            fixture = _RuntimeFixture(
                Path(directory),
                invoker=invoker,
            )
            client = fixture.client()

            class Handler(BaseHandler):
                def exec_echo(self, args):
                    if args != {"value": "safe"}:
                        raise AssertionError("unexpected tool args")
                    return ActionResult(
                        data={"echo": "safe"},
                        next_prompt="continue",
                    )

            handler = Handler(
                AgentContext(
                    execution_evidence_observer=(
                        fixture.collector
                    ),
                    display_fn=lambda _message: None,
                    principal=Principal(
                        subject="worker-subject",
                        tenant_id="tenant-1",
                        session_id="session-1",
                        run_id="run-1",
                        scopes=("workspace.read",),
                    ),
                )
            )
            result = run_agent_loop(
                client=client,
                system_prompt="system",
                user_input="task",
                handler=handler,
                tools_schema=[
                    {
                        "name": "echo",
                        "description": "echo",
                        "input_schema": {"type": "object"},
                    }
                ],
                max_turns=3,
            )
            manifest = fixture.collector.finalize(
                exit_reason=result["exit_reason"],
                turns=result["turns"],
            )

            self.assertEqual(result["response"], "complete")
            self.assertEqual(result["usage"]["total_tokens"], 28)
            self.assertEqual(len(invoker.calls), 2)
            self.assertTrue(manifest.provider_receipts_complete)
            self.assertEqual(
                manifest.observed_provider_invocations,
                2,
            )
            self.assertFalse(manifest.tool_receipts_complete)
            self.assertTrue(
                all(
                    item.receipt.response_artifact_ref_digest
                    is not None
                    for item in manifest.provider_receipts
                )
            )
            response_refs = client.result_artifact_refs
            self.assertEqual(len(response_refs), 2)
            self.assertTrue(
                all(fixture.store.verify(ref) for ref in response_refs)
            )
            self.assertEqual(
                tuple(
                    hashlib.sha256(
                        canonical_json_bytes(ref.to_dict())
                    ).hexdigest()
                    for ref in response_refs
                ),
                tuple(
                    item.receipt.response_artifact_ref_digest
                    for item in manifest.provider_receipts
                ),
            )
            first_payload = json.loads(
                invoker.calls[0][1].decode("utf-8")
            )
            second_payload = json.loads(
                invoker.calls[1][1].decode("utf-8")
            )
            self.assertEqual(first_payload["invocation_index"], 1)
            self.assertEqual(second_payload["invocation_index"], 2)
            self.assertEqual(
                [item["role"] for item in second_payload["messages"]],
                ["system", "user", "assistant", "user"],
            )
            self.assertEqual(
                second_payload["messages"][-1]["tool_results"][0][
                    "tool_call_id"
                ],
                "call-1",
            )
            self.assertNotIn(_UPSTREAM_SECRET, repr(client))
            self.assertNotIn("sensitive-agent-task", repr(client))
            self.assertFalse(client.production_security_ready)

    def test_requires_durable_result_and_exact_parent_binding(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            nondurable = _RuntimeFixture(
                root / "nondurable",
                invoker=_Invoker([_response_bytes()]),
                durable_journal=False,
            )
            with self.assertRaisesRegex(
                AgentProviderClientError,
                "durable_result_required",
            ):
                nondurable.client()

            mismatched = _RuntimeFixture(
                root / "mismatched",
                invoker=_Invoker([_response_bytes()]),
                collector_request_digest=_digest("other-request"),
            )
            with self.assertRaisesRegex(
                AgentProviderClientError,
                "parent_binding_mismatch",
            ):
                mismatched.client()
            self.assertEqual(nondurable.invoker.calls, [])
            self.assertEqual(mismatched.invoker.calls, [])

    def test_missing_observation_and_invalid_request_precede_authority(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            authorization_calls = 0

            def authorization_source() -> WorkerAuthorization:
                nonlocal authorization_calls
                authorization_calls += 1
                return _authorization()

            fixture = _RuntimeFixture(
                Path(directory),
                invoker=_Invoker([_response_bytes()]),
                authorization_source=authorization_source,
            )
            client = fixture.client()
            with self.assertRaisesRegex(
                AgentProviderClientError,
                "invocation_unavailable",
            ):
                client.chat(
                    [{"role": "user", "content": "task"}],
                    [],
                )

            cyclic: list = []
            cyclic.append(cyclic)
            deep: dict = {}
            cursor = deep
            for _index in range(30):
                cursor["next"] = {}
                cursor = cursor["next"]
            attacks = (
                (
                    [
                        {
                            "role": "user",
                            "content": "task",
                            "tool_results": cyclic,
                        }
                    ],
                    [],
                ),
                (
                    [{"role": "user", "content": "task"}],
                    [{"schema": float("nan")}],
                ),
                (
                    [{"role": "user", "content": "task"}],
                    [deep],
                ),
                (
                    [{"role": "user", "content": "task"}],
                    [{"\ud800": "invalid-key"}],
                ),
                (
                    [{"role": "user", "content": "task"}],
                    [{"schema": object()}],
                ),
            )
            for turn, (messages, tools) in enumerate(
                attacks,
                start=1,
            ):
                with self.subTest(turn=turn):
                    fixture.collector.provider_call_started(
                        turn=turn
                    )
                    with self.assertRaises(
                        AgentProviderClientError
                    ) as raised:
                        client.chat(messages, tools)
                    fixture.collector.provider_call_failed(
                        turn=turn
                    )
                    self.assertEqual(
                        str(raised.exception),
                        "agent_provider_request_invalid",
                    )
                    self.assertIsNone(raised.exception.__cause__)
                    self.assertIsNone(raised.exception.__context__)
            self.assertEqual(authorization_calls, 0)
            self.assertEqual(fixture.invoker.calls, [])

    def test_concurrent_call_on_one_attempt_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            invoker = _BlockingInvoker(_response_bytes())
            fixture = _RuntimeFixture(
                Path(directory),
                invoker=invoker,
            )
            client = fixture.client()
            fixture.collector.provider_call_started(turn=1)
            responses = []
            failures = []

            def first_call() -> None:
                try:
                    responses.append(
                        client.chat(
                            [{"role": "user", "content": "first"}],
                            [],
                        )
                    )
                except BaseException as exc:
                    failures.append(exc)

            thread = threading.Thread(target=first_call)
            thread.start()
            try:
                self.assertTrue(invoker.entered.wait(timeout=2))
                with self.assertRaises(
                    AgentProviderClientError
                ) as raised:
                    client.chat(
                        [{"role": "user", "content": "second"}],
                        [],
                    )
                self.assertEqual(
                    str(raised.exception),
                    "agent_provider_invocation_in_progress",
                )
                self.assertIsNone(raised.exception.__context__)
            finally:
                invoker.release.set()
                thread.join(timeout=2)

            self.assertFalse(thread.is_alive())
            self.assertEqual(failures, [])
            self.assertEqual(len(responses), 1)
            self.assertEqual(len(invoker.calls), 1)
            self.assertEqual(client.request_count, 1)
            fixture.collector.provider_call_finished(
                turn=1,
                receipt=responses[0].provider_receipt,
            )

    def test_route_size_preflight_does_not_burn_a_grant(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            authorization_calls = 0

            def authorization_source() -> WorkerAuthorization:
                nonlocal authorization_calls
                authorization_calls += 1
                return _authorization()

            fixture = _RuntimeFixture(
                Path(directory),
                invoker=_Invoker([_response_bytes()]),
                authorization_source=authorization_source,
                route=_route(maximum_request_bytes=64),
            )
            client = fixture.client()
            fixture.collector.provider_call_started(turn=1)
            with self.assertRaisesRegex(
                AgentProviderClientError,
                "request_too_large",
            ):
                client.chat(
                    [
                        {
                            "role": "user",
                            "content": "payload exceeds route limit",
                        }
                    ],
                    [],
                )
            fixture.collector.provider_call_failed(turn=1)

            self.assertEqual(authorization_calls, 0)
            self.assertEqual(fixture.invoker.calls, [])

    def test_authorization_errors_are_sanitized_and_lineage_is_fixed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            secret = "authorization-source-secret"

            def exploding_source() -> WorkerAuthorization:
                raise RuntimeError(secret)

            exploding = _RuntimeFixture(
                Path(directory) / "exploding",
                invoker=_Invoker([_response_bytes()]),
                authorization_source=exploding_source,
            )
            exploding_client = exploding.client()
            exploding.collector.provider_call_started(turn=1)
            with self.assertRaises(
                AgentProviderClientError
            ) as raised:
                exploding_client.chat(
                    [{"role": "user", "content": "task"}],
                    [],
                )
            exploding.collector.provider_call_failed(turn=1)
            self.assertEqual(
                str(raised.exception),
                "agent_provider_authorization_unavailable",
            )
            self.assertNotIn(secret, repr(raised.exception))
            self.assertIsNone(raised.exception.__cause__)
            self.assertIsNone(raised.exception.__context__)

            authorizations = [
                _authorization(),
                _authorization(
                    authorization_id="auth-2",
                    action_digest=_digest("different-action"),
                ),
            ]
            invoker = _Invoker(
                [_response_bytes(), _response_bytes()]
            )
            fixture = _RuntimeFixture(
                Path(directory) / "lineage",
                invoker=invoker,
                authorization_source=lambda: authorizations.pop(0),
            )
            client = fixture.client()
            fixture.collector.provider_call_started(turn=1)
            first = client.chat(
                [{"role": "user", "content": "task"}],
                [],
            )
            fixture.collector.provider_call_finished(
                turn=1,
                receipt=first.provider_receipt,
            )
            fixture.collector.provider_call_started(turn=2)
            with self.assertRaisesRegex(
                AgentProviderClientError,
                "authorization_lineage_changed",
            ):
                client.chat(
                    [{"role": "user", "content": "next"}],
                    [],
                )
            fixture.collector.provider_call_failed(turn=2)
            self.assertEqual(len(invoker.calls), 1)

    def test_invalid_gateway_response_does_not_commit_history(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            secret = "invalid-response-secret"
            duplicate = (
                b'{"schema_version":1,"schema_version":1,'
                b'"kind":"agent_provider_response","thinking":"",'
                b'"content":"' + secret.encode("utf-8") + b'",'
                b'"tool_calls":[],"stop_reason":"end_turn",'
                b'"usage":null}'
            )
            base = json.loads(_response_bytes().decode("utf-8"))
            pretty = json.dumps(
                base,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ).encode("utf-8")
            unknown = dict(base)
            unknown["unknown"] = "field"
            invalid_usage = dict(base)
            invalid_usage["usage"] = dict(base["usage"])
            invalid_usage["usage"]["input_tokens"] = True
            invalid_tool_id = dict(base)
            invalid_tool_id["tool_calls"] = [
                {
                    "name": "echo",
                    "arguments": {},
                    "id": "x" * 256,
                }
            ]
            invalid_responses = [
                duplicate,
                pretty,
                json.dumps(
                    unknown,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8"),
                json.dumps(
                    invalid_usage,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8"),
                json.dumps(
                    invalid_tool_id,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8"),
            ]
            fixture = _RuntimeFixture(
                Path(directory),
                invoker=_Invoker(invalid_responses),
            )
            client = fixture.client()
            for turn in range(1, len(invalid_responses) + 1):
                with self.subTest(turn=turn):
                    fixture.collector.provider_call_started(
                        turn=turn
                    )
                    with self.assertRaises(
                        AgentProviderClientError
                    ) as raised:
                        client.chat(
                            [
                                {
                                    "role": "user",
                                    "content": "task",
                                }
                            ],
                            [],
                        )
                    fixture.collector.provider_call_failed(
                        turn=turn
                    )
                    self.assertEqual(
                        str(raised.exception),
                        "agent_provider_response_invalid",
                    )
                    self.assertNotIn(
                        secret,
                        repr(raised.exception),
                    )
                    self.assertIsNone(raised.exception.__cause__)
                    self.assertIsNone(raised.exception.__context__)
            self.assertEqual(client.history, [])
            self.assertEqual(client.request_count, 0)
            self.assertEqual(client.result_artifact_refs, ())

    def test_broker_retry_converges_not_started_and_completed(
        self,
    ) -> None:
        for state in (
            ProviderOperationState.NOT_STARTED,
            ProviderOperationState.COMPLETED,
        ):
            with self.subTest(state=state):
                with tempfile.TemporaryDirectory() as directory:
                    invoker = _RecoverableInvoker(
                        _response_bytes(),
                        recovery_state=state,
                    )
                    fixture = _RuntimeFixture(
                        Path(directory),
                        invoker=invoker,
                    )
                    client = fixture.client()
                    fixture.collector.provider_call_started(turn=1)
                    response = client.chat(
                        [{"role": "user", "content": "task"}],
                        [],
                    )
                    fixture.collector.provider_call_finished(
                        turn=1,
                        receipt=response.provider_receipt,
                    )
                    manifest = fixture.collector.finalize(
                        exit_reason="CURRENT_TASK_DONE",
                        turns=1,
                    )

                    self.assertTrue(
                        manifest.provider_receipts_complete
                    )
                    self.assertEqual(len(invoker.recovery_calls), 1)
                    self.assertNotIn(
                        _UPSTREAM_SECRET,
                        repr(response.provider_receipt),
                    )
                    if state is ProviderOperationState.NOT_STARTED:
                        self.assertEqual(len(invoker.calls), 2)
                        expected_mode = (
                            ProviderInvocationCompletionMode.INVOKED
                        )
                    else:
                        self.assertEqual(len(invoker.calls), 1)
                        expected_mode = (
                            ProviderInvocationCompletionMode
                            .RECOVERED_COMPLETED
                        )
                    self.assertIs(
                        response.provider_receipt.completion_mode,
                        expected_mode,
                    )

    def test_wrong_authorization_parent_fails_before_grant(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = _RuntimeFixture(
                Path(directory),
                invoker=_Invoker([_response_bytes()]),
                authorization_source=lambda: replace(
                    _authorization(),
                    attempt_id="other-attempt",
                ),
            )
            client = fixture.client()
            fixture.collector.provider_call_started(turn=1)
            with self.assertRaisesRegex(
                AgentProviderClientError,
                "authorization_binding_mismatch",
            ):
                client.chat(
                    [{"role": "user", "content": "task"}],
                    [],
                )
            fixture.collector.provider_call_failed(turn=1)
            self.assertEqual(fixture.invoker.calls, [])


if __name__ == "__main__":
    unittest.main()
