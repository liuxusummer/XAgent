from __future__ import annotations

import concurrent.futures
import hashlib
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from src.core.eval import create_eval_run, import_dataset_content
from src.core.web_identity import WebIdentityConfigurationError, WebIdentityProvider
from src.web_ui_new import (
    _eval_cancel_events,
    _eval_lock,
    _get_session,
    _get_web_identity_provider,
    _secure_web_result,
    _shutdown_sessions,
    app,
)


def _record(
    token: str,
    *,
    subject: str,
    tenant: str,
    workspace: Path,
    scopes: list[str],
) -> dict:
    return {
        "token_sha256": hashlib.sha256(token.encode("utf-8")).hexdigest(),
        "subject": subject,
        "tenant_id": tenant,
        "scopes": scopes,
        "workspaces": {"default.ws": str(workspace.resolve())},
    }


class WebPrincipalSecurityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.workspace_a = root / "tenant-a" / "default.ws"
        self.workspace_b = root / "tenant-b" / "default.ws"
        for workspace, agent_name in (
            (self.workspace_a, "agent-a"),
            (self.workspace_b, "agent-b"),
        ):
            (workspace / "system" / "agents" / agent_name).mkdir(parents=True)
            (workspace / "runtime").mkdir(exist_ok=True)
        self.token_a = "opaque-token-a"
        self.token_b = "opaque-token-b"
        scopes = [
            "state.write",
            "user.interact",
            "workspace.delete",
            "workspace.read",
            "workspace.write",
        ]
        self.scopes = scopes
        registry = {
            "schema_version": 1,
            "identities": [
                _record(
                    self.token_a,
                    subject="user-a",
                    tenant="tenant-a",
                    workspace=self.workspace_a,
                    scopes=scopes,
                ),
                _record(
                    self.token_b,
                    subject="user-b",
                    tenant="tenant-b",
                    workspace=self.workspace_b,
                    scopes=[*scopes, "memory.read"],
                ),
            ],
        }
        self.registry_path = root / "web-identities.json"
        self.registry_path.write_text(json.dumps(registry), encoding="utf-8")
        self.environment = patch.dict(
            "os.environ",
            {
                "XAGENT_WEB_IDENTITY_REGISTRY": str(self.registry_path),
                "XAGENT_WEB_COOKIE_SECURE": "1",
            },
            clear=False,
        )
        self.environment.start()
        self.client = TestClient(
            app,
            base_url="http://127.0.0.1",
            client=("127.0.0.1", 50000),
        )

    def tearDown(self) -> None:
        _shutdown_sessions()
        self.client.close()
        self.environment.stop()
        self.temp.cleanup()

    @staticmethod
    def _headers(token: str, **extra: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}", **extra}

    def test_missing_wrong_and_forged_claim_headers_fail_closed(self) -> None:
        missing = self.client.get("/api/workspace/list")
        wrong = self.client.get(
            "/api/workspace/list",
            headers=self._headers("wrong"),
        )
        forged = self.client.get(
            "/api/workspace/agents?ws=default.ws",
            headers=self._headers(
                self.token_a,
                **{
                    "X-Subject": "user-b",
                    "X-Tenant": "tenant-b",
                    "X-Scopes": "workspace.write",
                },
            ),
        )

        self.assertEqual(missing.status_code, 401)
        self.assertEqual(wrong.status_code, 401)
        self.assertEqual(forged.status_code, 200)
        self.assertEqual(
            [item["name"] for item in forged.json()["data"]],
            ["agent-a"],
        )

    def test_workspace_file_routes_cannot_bypass_memory_or_runtime_boundaries(
        self,
    ) -> None:
        memory = self.workspace_a / "system" / "memory" / "secret.md"
        checkpoint = self.workspace_a / "runtime" / "checkpoints" / "latest.json"
        memory.parent.mkdir(parents=True)
        checkpoint.parent.mkdir(parents=True)
        memory.write_text("MEMORY-SECRET", encoding="utf-8")
        checkpoint.write_text("CP-SECRET", encoding="utf-8")
        mixed_memory = self.workspace_a / "SYSTEM" / "MEMORY" / "mixed.md"
        mixed_checkpoint = (
            self.workspace_a / "RUNTIME" / "CHECKPOINTS" / "mixed.json"
        )
        mixed_memory.parent.mkdir(parents=True, exist_ok=True)
        mixed_checkpoint.parent.mkdir(parents=True, exist_ok=True)
        mixed_memory.write_text("MIXED-MEMORY-SECRET", encoding="utf-8")
        mixed_checkpoint.write_text("MIXED-CP-SECRET", encoding="utf-8")
        allowed_memory = self.workspace_b / "system" / "memory" / "allowed.md"
        allowed_memory.parent.mkdir(parents=True)
        allowed_memory.write_text("ALLOWED-MEMORY", encoding="utf-8")

        memory_read = self.client.get(
            "/api/workspace/file",
            params={"ws": "default.ws", "path": "system/memory/secret.md"},
            headers=self._headers(self.token_a),
        )
        checkpoint_read = self.client.get(
            "/api/workspace/preview",
            params={
                "ws": "default.ws",
                "path": "runtime/checkpoints/latest.json",
            },
            headers=self._headers(self.token_a),
        )
        mixed_memory_read = self.client.get(
            "/api/workspace/preview",
            params={
                "ws": "default.ws",
                "path": "SYSTEM/MEMORY/mixed.md",
            },
            headers=self._headers(self.token_a),
        )
        mixed_checkpoint_read = self.client.get(
            "/api/workspace/preview",
            params={
                "ws": "default.ws",
                "path": "RUNTIME/CHECKPOINTS/mixed.json",
            },
            headers=self._headers(self.token_a),
        )
        memory_write = self.client.put(
            "/api/workspace/file",
            json={
                "ws": "default.ws",
                "path": "system/memory/secret.md",
                "content": "POISON",
            },
            headers=self._headers(self.token_a),
        )
        tree = self.client.get(
            "/api/workspace/tree",
            params={"ws": "default.ws"},
            headers=self._headers(self.token_a),
        )
        allowed_read = self.client.get(
            "/api/workspace/file",
            params={"ws": "default.ws", "path": "system/memory/allowed.md"},
            headers=self._headers(self.token_b),
        )

        self.assertFalse(memory_read.json()["success"])
        self.assertFalse(checkpoint_read.json()["success"])
        self.assertFalse(mixed_memory_read.json()["success"])
        self.assertFalse(mixed_checkpoint_read.json()["success"])
        self.assertFalse(memory_write.json()["success"])
        self.assertTrue(allowed_read.json()["success"])
        self.assertEqual(
            allowed_read.json()["data"]["content"],
            "ALLOWED-MEMORY",
        )
        self.assertEqual(memory.read_text(encoding="utf-8"), "MEMORY-SECRET")
        rendered_tree = json.dumps(tree.json(), ensure_ascii=False)
        self.assertNotIn("runtime", rendered_tree)
        self.assertNotIn("RUNTIME", rendered_tree)
        self.assertNotIn("secret.md", rendered_tree)

    def test_secure_done_result_redacts_runtime_exception_details(self) -> None:
        result = _secure_web_result(
            {
                "response": "[error] /host/private/secret",
                "exit_reason": "ERROR",
                "tool_results": [
                    {
                        "tool_name": "run_task",
                        "data": {
                            "status": "ERROR",
                            "error": "/host/private/secret",
                            "path": "/host/private/secret",
                        },
                    }
                ],
            }
        )

        rendered = json.dumps(result, ensure_ascii=False)
        self.assertNotIn("/host/private/secret", rendered)
        self.assertEqual(result["response"], "[error] Agent task failed")

        successful = _secure_web_result(
            {
                "response": "saved under /Users/service/private/result.txt",
                "exit_reason": "CURRENT_TASK_DONE",
                "tool_results": [],
            }
        )
        self.assertNotIn("/Users/service", successful["response"])

    def test_same_workspace_alias_maps_to_isolated_tenant_roots(self) -> None:
        response_a = self.client.get(
            "/api/workspace/agents?ws=default.ws",
            headers=self._headers(self.token_a),
        )
        response_b = self.client.get(
            "/api/workspace/agents?ws=default.ws",
            headers=self._headers(self.token_b),
        )

        self.assertEqual(
            [item["name"] for item in response_a.json()["data"]],
            ["agent-a"],
        )
        self.assertEqual(
            [item["name"] for item in response_b.json()["data"]],
            ["agent-b"],
        )

    def test_cross_owner_chat_and_live_session_ids_do_not_leak(self) -> None:
        created = self.client.post(
            "/api/chats",
            headers=self._headers(self.token_a),
            json={"ws": "default.ws", "agent": "agent-a"},
        ).json()
        chat_id = created["data"]["metadata"]["chat_id"]
        session_id = created["data"]["state"]["backend_session_id"]

        other_chat = self.client.get(
            f"/api/chats/{chat_id}?ws=default.ws&agent=agent-a",
            headers=self._headers(self.token_b),
        )
        other_stream = self.client.get(
            f"/api/chat/stream?session_id={session_id}",
            headers=self._headers(self.token_b),
        )

        self.assertFalse(other_chat.json()["success"])
        self.assertNotIn("messages", other_chat.text)
        self.assertIn("Session not found", other_stream.text)
        self.assertNotIn(chat_id, other_stream.text)

    def test_secure_mode_rejects_client_selected_host_paths_before_agent_start(self) -> None:
        absolute_workspace = self.client.post(
            "/api/chat",
            headers=self._headers(self.token_a),
            json={"task": "x", "workspace_dir": "/tmp", "agent": "agent-a"},
        )
        host_config = self.client.post(
            "/api/chat",
            headers=self._headers(self.token_a),
            json={
                "task": "x",
                "workspace_dir": "default.ws",
                "agent": "agent-a",
                "config_path": "/tmp/attacker.json",
            },
        )

        self.assertFalse(absolute_workspace.json()["success"])
        self.assertEqual(absolute_workspace.json()["error"], "Workspace not found")
        self.assertFalse(host_config.json()["success"])
        self.assertIn("configuration paths", host_config.json()["error"])

    def test_secure_mode_rejects_server_config_inside_any_agent_workspace(self) -> None:
        config = self.workspace_a / "business" / "config.json"
        config.parent.mkdir()
        config.write_text("{}", encoding="utf-8")
        with (
            patch.dict(
                "os.environ",
                {"XAGENT_WEB_CONFIG_PATH": str(config)},
                clear=False,
            ),
            patch("src.web_ui_new.build_agent") as build_agent,
        ):
            response = self.client.post(
                "/api/chat",
                headers=self._headers(self.token_a),
                json={
                    "task": "x",
                    "workspace_dir": "default.ws",
                    "agent": "agent-a",
                },
            )

        self.assertFalse(response.json()["success"])
        self.assertEqual(
            response.json()["error"],
            "Trusted Web configuration is unavailable",
        )
        build_agent.assert_not_called()

    def test_secure_mode_redacts_backend_exception_details(self) -> None:
        with patch(
            "src.web_ui_new.build_agent",
            side_effect=RuntimeError(
                "failed to read /private/host/secrets/provider-key.json"
            ),
        ):
            response = self.client.post(
                "/api/chat",
                headers=self._headers(self.token_a),
                json={
                    "task": "x",
                    "workspace_dir": "default.ws",
                    "agent": "agent-a",
                },
            )

        payload = response.json()
        self.assertFalse(payload["success"])
        self.assertEqual(payload["error"], "Failed to initialize Agent")
        self.assertNotIn("/private/host", response.text)
        self.assertNotIn("provider-key", response.text)

    def test_scope_reduction_is_enforced_at_the_http_boundary(self) -> None:
        read_token = "read-only"
        registry = {
            "schema_version": 1,
            "identities": [
                _record(
                    read_token,
                    subject="reader",
                    tenant="reader-tenant",
                    workspace=self.workspace_a,
                    scopes=["workspace.read"],
                )
            ],
        }
        alternate = Path(self.temp.name) / "read-only-identities.json"
        alternate.write_text(json.dumps(registry), encoding="utf-8")
        with patch.dict(
            "os.environ",
            {"XAGENT_WEB_IDENTITY_REGISTRY": str(alternate)},
            clear=False,
        ):
            denied = self.client.post(
                "/api/chats",
                headers=self._headers(read_token),
                json={"ws": "default.ws", "agent": "agent-a"},
            )
            allowed = self.client.get(
                "/api/workspace/agents?ws=default.ws",
                headers=self._headers(read_token),
            )

        self.assertEqual(denied.status_code, 403)
        self.assertEqual(allowed.status_code, 200)

    def test_user_interact_scope_cannot_mutate_chat_state(self) -> None:
        interact_token = "interact-only"
        alternate = Path(self.temp.name) / "interact-identities.json"
        alternate.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "identities": [
                        _record(
                            interact_token,
                            subject="interactor",
                            tenant="interactor-tenant",
                            workspace=self.workspace_a,
                            scopes=["user.interact"],
                        )
                    ],
                }
            ),
            encoding="utf-8",
        )
        with patch.dict(
            "os.environ",
            {"XAGENT_WEB_IDENTITY_REGISTRY": str(alternate)},
            clear=False,
        ):
            response = self.client.post(
                "/api/chats",
                headers=self._headers(interact_token),
                json={"ws": "default.ws", "agent": "agent-a"},
            )

        self.assertEqual(response.status_code, 403)

    def test_bearer_can_be_exchanged_for_hardened_sse_cookie(self) -> None:
        response = self.client.post(
            "/api/auth/session",
            headers=self._headers(self.token_a),
        )

        cookie = response.headers.get("set-cookie", "")
        self.assertEqual(response.status_code, 200)
        self.assertIn("xagent_session=", cookie)
        self.assertIn("HttpOnly", cookie)
        self.assertIn("SameSite=strict", cookie)
        self.assertIn("Secure", cookie)

    def test_concurrent_requests_do_not_cross_wire_identity_context(self) -> None:
        def names(token: str) -> list[str]:
            response = self.client.get(
                "/api/workspace/agents?ws=default.ws",
                headers=self._headers(token),
            )
            return [item["name"] for item in response.json()["data"]]

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(names, self.token_a if index % 2 == 0 else self.token_b)
                for index in range(20)
            ]
        results = [future.result() for future in futures]

        self.assertEqual(
            results,
            [["agent-a"] if index % 2 == 0 else ["agent-b"] for index in range(20)],
        )

    def test_registry_rejects_workspace_shared_across_identity_boundaries(self) -> None:
        payload = {
            "schema_version": 1,
            "identities": [
                _record(
                    "one",
                    subject="one",
                    tenant="tenant-one",
                    workspace=self.workspace_a,
                    scopes=["workspace.read"],
                ),
                _record(
                    "two",
                    subject="two",
                    tenant="tenant-two",
                    workspace=self.workspace_a,
                    scopes=["workspace.read"],
                ),
            ],
        }
        with self.assertRaises(WebIdentityConfigurationError):
            WebIdentityProvider.from_trusted_config(payload)

    def test_registry_rejects_nested_cross_identity_workspace_roots(self) -> None:
        payload = {
            "schema_version": 1,
            "identities": [
                _record(
                    "parent",
                    subject="parent",
                    tenant="tenant-parent",
                    workspace=self.workspace_a.parent,
                    scopes=["workspace.read"],
                ),
                _record(
                    "child",
                    subject="child",
                    tenant="tenant-child",
                    workspace=self.workspace_a,
                    scopes=["workspace.read"],
                ),
            ],
        }
        with self.assertRaises(WebIdentityConfigurationError):
            WebIdentityProvider.from_trusted_config(payload)

    def test_registry_file_cannot_reside_inside_an_authorized_workspace(self) -> None:
        registry = self.workspace_a / "business" / "identities.json"
        registry.parent.mkdir(exist_ok=True)
        registry.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "identities": [
                        _record(
                            "nested",
                            subject="nested",
                            tenant="nested",
                            workspace=self.workspace_a,
                            scopes=["workspace.read"],
                        )
                    ],
                }
            ),
            encoding="utf-8",
        )

        with self.assertRaises(WebIdentityConfigurationError):
            WebIdentityProvider.from_registry_file(registry)

    def test_registry_revocation_stops_and_removes_live_session(self) -> None:
        class _LiveAgent:
            def __init__(self) -> None:
                self.stopped = False
                self.closed = False

            def stop(self) -> None:
                self.stopped = True

            def close(self) -> None:
                self.closed = True

        created = self.client.post(
            "/api/chats",
            headers=self._headers(self.token_a),
            json={"ws": "default.ws", "agent": "agent-a"},
        ).json()
        session_id = created["data"]["state"]["backend_session_id"]
        session = _get_session(session_id)
        self.assertIsNotNone(session)
        agent = _LiveAgent()
        with session.lock:
            session.agent = agent
            session.running = True

        self.registry_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "identities": [
                        _record(
                            self.token_b,
                            subject="user-b",
                            tenant="tenant-b",
                            workspace=self.workspace_b,
                            scopes=self.scopes,
                        )
                    ],
                }
            ),
            encoding="utf-8",
        )
        response = self.client.get(
            "/api/workspace/list",
            headers=self._headers(self.token_b),
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(_get_session(session_id))
        self.assertTrue(agent.stopped)
        self.assertTrue(agent.closed)

    def test_invalid_registry_revokes_all_live_sessions_and_eval_work(self) -> None:
        class _LiveAgent:
            def __init__(self) -> None:
                self.stopped = False
                self.closed = False

            def stop(self) -> None:
                self.stopped = True

            def close(self) -> None:
                self.closed = True

        created = self.client.post(
            "/api/chats",
            headers=self._headers(self.token_a),
            json={"ws": "default.ws", "agent": "agent-a"},
        ).json()
        session_id = created["data"]["state"]["backend_session_id"]
        session = _get_session(session_id)
        self.assertIsNotNone(session)
        agent = _LiveAgent()
        with session.lock:
            session.agent = agent
            session.running = True

        identity = _get_web_identity_provider().authenticate(self.token_a)
        cancel_event = threading.Event()
        cancel_key = (identity.owner_digest, "run-invalid-registry")
        with _eval_lock:
            _eval_cancel_events[cancel_key] = cancel_event
        self.registry_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "identities": [
                        {
                            **_record(
                                self.token_a,
                                subject="user-a",
                                tenant="tenant-a",
                                workspace=self.workspace_a,
                                scopes=self.scopes,
                            ),
                            "enabled": False,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        try:
            response = self.client.get(
                "/api/workspace/list",
                headers=self._headers(self.token_a),
            )
        finally:
            with _eval_lock:
                _eval_cancel_events.pop(cancel_key, None)

        self.assertEqual(response.status_code, 503)
        self.assertIsNone(_get_session(session_id))
        self.assertTrue(agent.stopped)
        self.assertTrue(agent.closed)
        self.assertTrue(cancel_event.is_set())

    def test_workspace_replacement_revokes_live_identity_boundary(self) -> None:
        class _LiveAgent:
            def __init__(self) -> None:
                self.stopped = False
                self.closed = False

            def stop(self) -> None:
                self.stopped = True

            def close(self) -> None:
                self.closed = True

        created = self.client.post(
            "/api/chats",
            headers=self._headers(self.token_a),
            json={"ws": "default.ws", "agent": "agent-a"},
        ).json()
        session_id = created["data"]["state"]["backend_session_id"]
        session = _get_session(session_id)
        self.assertIsNotNone(session)
        agent = _LiveAgent()
        with session.lock:
            session.agent = agent
            session.running = True

        original_workspace = self.workspace_a.with_name(
            "default.ws.original"
        )
        self.workspace_a.rename(original_workspace)
        self.workspace_a.symlink_to(
            self.workspace_b,
            target_is_directory=True,
        )
        response = self.client.get(
            "/api/workspace/agents?ws=default.ws",
            headers=self._headers(self.token_a),
        )

        self.assertEqual(response.status_code, 401)
        self.assertNotIn("agent-b", response.text)
        self.assertIsNone(_get_session(session_id))
        self.assertTrue(agent.stopped)
        self.assertTrue(agent.closed)

    def test_eval_imports_remain_hidden_after_workspace_reassignment(self) -> None:
        imported = self.client.post(
            "/api/eval/datasets/import",
            headers=self._headers(self.token_a),
            json={
                "ws": "default.ws",
                "name": "owner-a.jsonl",
                "format": "jsonl",
                "content": '{"task":"private-a"}\n',
            },
        ).json()
        self.assertTrue(imported["success"])
        self.assertEqual(
            imported["data"]["dataset_path"],
            "<redacted-path>",
        )
        imported_id = imported["data"]["id"]
        owner_a = imported["data"]["owner_digest"]

        self.registry_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "identities": [
                        _record(
                            self.token_b,
                            subject="user-b",
                            tenant="tenant-b",
                            workspace=self.workspace_a,
                            scopes=self.scopes,
                        )
                    ],
                }
            ),
            encoding="utf-8",
        )
        response = self.client.get(
            "/api/eval/datasets?ws=default.ws",
            headers=self._headers(self.token_b),
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["success"])
        self.assertNotIn(
            imported_id,
            {item["id"] for item in response.json()["data"]},
        )
        self.assertTrue(
            (
                self.workspace_a
                / "runtime"
                / "eval"
                / "_owners"
                / owner_a
                / "datasets"
                / imported_id
            ).is_dir()
        )

    def test_eval_cancel_handle_is_bound_to_authenticated_owner(self) -> None:
        provider = _get_web_identity_provider()
        identity_a = provider.authenticate(self.token_a)
        identity_b = provider.authenticate(self.token_b)
        storage_root = (
            self.workspace_b
            / "runtime"
            / "eval"
            / "_owners"
            / identity_b.owner_digest
        )
        dataset = import_dataset_content(
            self.workspace_b,
            name="other.jsonl",
            fmt="jsonl",
            content='{"task":"say ok"}\n',
            storage_root=storage_root,
            owner_digest=identity_b.owner_digest,
        )
        run = create_eval_run(
            self.workspace_b,
            workspace="default.ws",
            dataset_id=dataset["id"],
            agent="",
            storage_root=storage_root,
            owner_digest=identity_b.owner_digest,
        )
        event_a = threading.Event()
        event_b = threading.Event()
        key_a = (identity_a.owner_digest, run["id"])
        key_b = (identity_b.owner_digest, run["id"])
        with _eval_lock:
            _eval_cancel_events[key_a] = event_a
            _eval_cancel_events[key_b] = event_b
        try:
            response = self.client.post(
                f"/api/eval/runs/{run['id']}/cancel?ws=default.ws",
                headers=self._headers(self.token_b),
            )
        finally:
            with _eval_lock:
                _eval_cancel_events.pop(key_a, None)
                _eval_cancel_events.pop(key_b, None)

        self.assertEqual(response.status_code, 200)
        self.assertFalse(event_a.is_set())
        self.assertTrue(event_b.is_set())


if __name__ == "__main__":
    unittest.main()
