from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.core import web_identity as web_identity_module
from src.core.agent_kernel import Principal
from src.core.telemetry import NullSink
from src.core.web_identity import (
    MAX_OPAQUE_TOKEN_BYTES,
    WebIdentityConfigurationError,
    WebIdentityError,
    WebIdentityProvider,
)
from src.main import _build_delegate_runner, build_agent, build_team_step_runner


def _principal(agent_id: str = "main") -> Principal:
    return Principal(
        subject="user-a",
        tenant_id="tenant-a",
        session_id="parent-session",
        run_id="parent-run",
        agent_id=agent_id,
        scopes=("workspace.read", "memory.read"),
    )


def _registry(token: str, workspace: str) -> dict:
    return {
        "schema_version": 1,
        "identities": [
            {
                "token_sha256": hashlib.sha256(token.encode("utf-8")).hexdigest(),
                "subject": "user-a",
                "tenant_id": "tenant-a",
                "scopes": ["workspace.read"],
                "workspaces": {"default.ws": workspace},
            }
        ],
    }


class WebIdentityProviderTests(unittest.TestCase):
    def test_local_provider_uses_server_owned_compatibility_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            provider = WebIdentityProvider.local(tmp_dir)
            identity = provider.authenticate("ignored-local-token")

            self.assertTrue(provider.is_local)
            self.assertEqual(identity.subject, "local-user")
            self.assertEqual(identity.tenant_id, "local")
            self.assertEqual(
                identity.resolve_workspace("default.ws"),
                str((Path(tmp_dir) / "default.ws").resolve()),
            )

    def test_secure_registry_authenticates_opaque_token_and_maps_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            token = "opaque-random-token"
            workspace_path = Path(tmp_dir) / "tenant-a" / "default.ws"
            workspace_path.mkdir(parents=True)
            workspace = str(workspace_path.resolve())
            registry_path = Path(tmp_dir) / "identities.json"
            registry_path.write_text(
                json.dumps(_registry(token, workspace)),
                encoding="utf-8",
            )

            provider = WebIdentityProvider.from_registry_file(registry_path)
            identity = provider.authenticate(token)
            principal = identity.principal(
                session_id="web-session",
                agent_id="coding",
            )

            self.assertFalse(provider.is_local)
            self.assertEqual(identity.resolve_workspace("default.ws"), workspace)
            self.assertEqual(principal.subject, "user-a")
            self.assertEqual(principal.tenant_id, "tenant-a")
            self.assertEqual(principal.scopes, ("workspace.read",))
            self.assertEqual(principal.agent_id, "coding")
            self.assertNotIn(token, identity.owner_digest)

    def test_secure_registry_fails_closed_for_missing_wrong_and_large_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "default.ws").mkdir()
            provider = WebIdentityProvider.from_trusted_config(
                _registry("valid", str((Path(tmp_dir) / "default.ws").resolve()))
            )
            with self.assertRaises(WebIdentityError):
                provider.authenticate()
            with self.assertRaises(WebIdentityError):
                provider.authenticate("wrong")
            with self.assertRaises(WebIdentityError):
                provider.authenticate("x" * (MAX_OPAQUE_TOKEN_BYTES + 1))

    def test_secure_registry_scans_all_digests_with_constant_time_comparison(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace_path = Path(tmp_dir) / "default.ws"
            workspace_path.mkdir()
            other_workspace = Path(tmp_dir) / "user-b" / "default.ws"
            other_workspace.mkdir(parents=True)
            workspace = str(workspace_path.resolve())
            payload = _registry("first", workspace)
            payload["identities"].append(
                {
                    **payload["identities"][0],
                    "token_sha256": hashlib.sha256(b"second").hexdigest(),
                    "subject": "user-b",
                    "workspaces": {
                        "default.ws": str(other_workspace.resolve())
                    },
                }
            )
            provider = WebIdentityProvider.from_trusted_config(payload)

            with patch(
                "src.core.web_identity.hmac.compare_digest",
                wraps=__import__("hmac").compare_digest,
            ) as compare:
                identity = provider.authenticate("first")

            self.assertEqual(identity.subject, "user-a")
            self.assertEqual(compare.call_count, 2)

    def test_registry_rejects_raw_token_and_unknown_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace_path = Path(tmp_dir) / "default.ws"
            workspace_path.mkdir()
            workspace = str(workspace_path.resolve())
            payload = _registry("valid", workspace)
            payload["identities"][0]["token"] = "must-not-be-stored"
            with self.assertRaises(WebIdentityConfigurationError):
                WebIdentityProvider.from_trusted_config(payload)

            provider = WebIdentityProvider.from_trusted_config(
                _registry("valid", workspace)
            )
            with self.assertRaises(WebIdentityError):
                provider.authenticate("valid").resolve_workspace("other.ws")

    def test_secure_registry_cannot_grant_local_host_read_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace_path = Path(tmp_dir) / "default.ws"
            workspace_path.mkdir()
            payload = _registry("valid", str(workspace_path.resolve()))
            for reserved_scope in ("host.read", " host.read "):
                with self.subTest(scope=reserved_scope):
                    payload["identities"][0]["scopes"] = [
                        "workspace.read",
                        reserved_scope,
                    ]
                    with self.assertRaisesRegex(
                        WebIdentityConfigurationError,
                        "reserved for local operator",
                    ):
                        WebIdentityProvider.from_trusted_config(payload)

    def test_registry_file_and_identity_counts_are_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace_path = Path(tmp_dir) / "default.ws"
            workspace_path.mkdir()
            other_workspace = Path(tmp_dir) / "other.ws"
            other_workspace.mkdir()
            workspace = str(workspace_path.resolve())
            registry_path = Path(tmp_dir) / "identities.json"
            registry_path.write_text(
                json.dumps(_registry("valid", workspace)),
                encoding="utf-8",
            )
            with patch(
                "src.core.web_identity.MAX_WEB_IDENTITY_REGISTRY_BYTES",
                8,
            ), self.assertRaises(WebIdentityConfigurationError):
                WebIdentityProvider.from_registry_file(registry_path)

            payload = _registry("first", workspace)
            payload["identities"].append(
                {
                    **payload["identities"][0],
                    "token_sha256": hashlib.sha256(b"second").hexdigest(),
                    "subject": "user-b",
                    "workspaces": {
                        "default.ws": str(other_workspace.resolve())
                    },
                }
            )
            with patch(
                "src.core.web_identity.MAX_WEB_IDENTITIES",
                1,
            ), self.assertRaises(WebIdentityConfigurationError):
                WebIdentityProvider.from_trusted_config(payload)

    def test_registry_rejects_physically_overlapping_workspace_owners(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            first_workspace = Path(tmp_dir) / "first.ws"
            second_workspace = first_workspace / "second.ws"
            second_workspace.mkdir(parents=True)
            payload = _registry("first", str(first_workspace.resolve()))
            payload["identities"].append(
                {
                    **payload["identities"][0],
                    "token_sha256": hashlib.sha256(b"second").hexdigest(),
                    "subject": "user-b",
                    "workspaces": {
                        "default.ws": str(second_workspace.resolve())
                    },
                }
            )

            with self.assertRaisesRegex(
                WebIdentityConfigurationError,
                "cannot overlap",
            ):
                WebIdentityProvider.from_trusted_config(payload)

    def test_physical_overlap_follows_inode_ancestry_not_path_case(self) -> None:
        identities = {
            Path("/srv/tenant.ws"): (1, 10),
            Path("/srv/TENANT.WS/child.ws"): (1, 20),
            Path("/srv/TENANT.WS"): (1, 10),
            Path("/srv"): (1, 2),
            Path("/"): (1, 1),
        }

        with patch.object(
            web_identity_module,
            "_path_identity",
            side_effect=lambda path: identities[path],
        ):
            self.assertTrue(
                web_identity_module._directories_physically_overlap(
                    Path("/srv/tenant.ws"),
                    Path("/srv/TENANT.WS/child.ws"),
                )
            )

    def test_registry_file_must_be_physically_outside_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir) / "default.ws"
            workspace.mkdir()
            registry_path = Path(tmp_dir) / "identities.json"
            registry_path.write_text(
                json.dumps(_registry("valid", str(workspace.resolve()))),
                encoding="utf-8",
            )

            with patch(
                "src.core.web_identity._directory_contains_path",
                return_value=True,
            ), self.assertRaisesRegex(
                WebIdentityConfigurationError,
                "outside Agent workspaces",
            ):
                WebIdentityProvider.from_registry_file(registry_path)

    def test_case_variant_workspace_paths_cannot_cross_owner_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir) / "Tenant.ws"
            child = workspace / "child.ws"
            child.mkdir(parents=True)
            case_variant = Path(tmp_dir) / "TENANT.WS"
            try:
                same_workspace = case_variant.is_dir() and workspace.samefile(
                    case_variant
                )
            except OSError:
                same_workspace = False
            if not same_workspace:
                self.skipTest("filesystem paths are case-sensitive")

            payload = _registry("first", str(workspace))
            payload["identities"].append(
                {
                    **payload["identities"][0],
                    "token_sha256": hashlib.sha256(b"second").hexdigest(),
                    "subject": "user-b",
                    "workspaces": {
                        "default.ws": str(case_variant / "child.ws")
                    },
                }
            )

            with self.assertRaisesRegex(
                WebIdentityConfigurationError,
                "cannot overlap",
            ):
                WebIdentityProvider.from_trusted_config(payload)


class PrincipalPropagationTests(unittest.TestCase):
    def test_principal_without_memory_scope_does_not_receive_boot_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            memory = Path(tmp_dir) / "system" / "agents" / "main" / "MEMORY.md"
            memory.parent.mkdir(parents=True)
            memory.write_text("SCOPE_PROTECTED_MEMORY", encoding="utf-8")
            principal = Principal(
                subject="reader",
                tenant_id="tenant-a",
                session_id="session",
                run_id="run",
                agent_id="main",
                scopes=("workspace.read",),
            )
            agent = build_agent(
                workspace_dir=tmp_dir,
                agent_name="main",
                principal=principal,
            )
            try:
                self.assertNotIn(
                    "SCOPE_PROTECTED_MEMORY",
                    agent.system_prompt,
                )
                agent.handler.ctx.principal = principal
                self.assertNotIn(
                    "SCOPE_PROTECTED_MEMORY",
                    agent._runtime_system_prompt(),
                )
            finally:
                agent.close()

    def test_build_agent_preserves_explicit_scope_set_across_run_binding(self) -> None:
        captured: list[Principal] = []

        def _fake_loop(*, handler, **_kwargs):
            captured.append(handler.ctx.principal)
            return {
                "response": "",
                "exit_reason": "CURRENT_TASK_DONE",
                "tool_results": [],
                "turns": 0,
            }

        with tempfile.TemporaryDirectory() as tmp_dir:
            source = _principal()
            agent = build_agent(
                workspace_dir=tmp_dir,
                agent_name="main",
                principal=source,
            )
            try:
                with (
                    patch("src.core.XAgent.run_agent_loop", side_effect=_fake_loop),
                    patch.object(agent, "_distill_runbook"),
                ):
                    agent.run_task("inspect", checkpoint_id="run-1")
            finally:
                agent.close()

        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].subject, source.subject)
        self.assertEqual(captured[0].tenant_id, source.tenant_id)
        self.assertEqual(captured[0].scopes, source.scopes)
        self.assertEqual(captured[0].session_id, "run-1")

    def test_delegate_and_team_child_are_constructed_with_parent_identity(self) -> None:
        built_principals: list[Principal | None] = []

        class _Child:
            def __init__(self, principal: Principal | None) -> None:
                self.sink = NullSink()
                self.stop_event = threading.Event()
                self.owns_stop_event = True
                self.handler = SimpleNamespace(
                    ctx=SimpleNamespace(
                        principal=principal,
                        sink=self.sink,
                        verbose=False,
                        display_fn=lambda _message: None,
                        user_input_fn=None,
                        stop_signal=self.stop_event,
                    )
                )

            def run_task(self, _task: str) -> dict:
                return {
                    "response": "ok",
                    "exit_reason": "CURRENT_TASK_DONE",
                    "tool_results": [],
                    "turns": 1,
                }

            def close(self) -> None:
                return None

        def _fake_build_agent(**kwargs):
            principal = kwargs.get("principal")
            built_principals.append(principal)
            return _Child(principal)

        parent = _principal()
        parent_ctx = SimpleNamespace(
            principal=parent,
            sink=NullSink(),
            verbose=False,
            display_fn=lambda _message: None,
            user_input_fn=None,
            stop_signal=threading.Event(),
        )
        team_config = {
            "name": "team",
            "leader": "main",
            "members": [{"agent": "coding", "autoDelegate": True}],
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            agent_dir = Path(tmp_dir) / "system" / "agents" / "coding"
            agent_dir.mkdir(parents=True)
            (agent_dir / "AGENT.md").write_text("", encoding="utf-8")
            delegate = _build_delegate_runner(
                config_path=None,
                observability_config_path=None,
                skills_dir=None,
                workspace=tmp_dir,
                current_agent="main",
                team_config=team_config,
                principal_template=parent,
            )
            team_step = build_team_step_runner(
                config_path=None,
                observability_config_path=None,
                skills_dir=None,
                workspace=tmp_dir,
                team_config=team_config,
                principal_template=parent,
            )
            with patch("src.main.build_agent", side_effect=_fake_build_agent):
                delegate_result = delegate(
                    agent="coding",
                    task="delegate",
                    parent_ctx=parent_ctx,
                )
                step_result = team_step(
                    agent="coding",
                    task="step",
                    step_id="code",
                    parent_ctx=parent_ctx,
                )

        self.assertEqual(delegate_result["status"], "OK")
        self.assertEqual(step_result["status"], "OK")
        self.assertEqual(len(built_principals), 2)
        for child in built_principals:
            self.assertIsInstance(child, Principal)
            assert child is not None
            self.assertEqual(child.subject, parent.subject)
            self.assertEqual(child.tenant_id, parent.tenant_id)
            self.assertEqual(child.scopes, parent.scopes)
            self.assertEqual(child.agent_id, "coding")

    def test_child_identity_mismatch_is_rejected_before_execution(self) -> None:
        executed = False

        class _Child:
            def __init__(self) -> None:
                self.sink = NullSink()
                self.handler = SimpleNamespace(
                    ctx=SimpleNamespace(
                        principal=Principal(
                            subject="attacker",
                            tenant_id="other",
                            session_id="initializing",
                            run_id="initializing",
                            agent_id="coding",
                            scopes=("workspace.read",),
                        )
                    )
                )

            def run_task(self, _task: str) -> dict:
                nonlocal executed
                executed = True
                return {}

            def close(self) -> None:
                return None

        team_config = {
            "name": "team",
            "leader": "main",
            "members": [{"agent": "coding", "autoDelegate": True}],
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            agent_dir = Path(tmp_dir) / "system" / "agents" / "coding"
            agent_dir.mkdir(parents=True)
            (agent_dir / "AGENT.md").write_text("", encoding="utf-8")
            runner = build_team_step_runner(
                config_path=None,
                observability_config_path=None,
                skills_dir=None,
                workspace=tmp_dir,
                team_config=team_config,
                principal_template=_principal(),
            )
            with patch("src.main.build_agent", return_value=_Child()):
                result = runner(agent="coding", task="step", step_id="code")

        self.assertEqual(result["status"], "ERROR")
        self.assertEqual(
            result["reason_code"],
            "WORKFLOW_AGENT_RUNTIME_FAILED",
        )
        self.assertFalse(executed)


if __name__ == "__main__":
    unittest.main()
