from __future__ import annotations

import io
import json
import shutil
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.core.agent_kernel import Principal
from src.core.eval import (
    EvalError,
    create_eval_run,
    execute_eval_run,
    import_dataset_path,
)
from src.core.eval_scenarios import (
    EvalCaseRuntime,
    ScenarioPackError,
    cleanup_case_workspace,
    load_scenario_pack_for_dataset,
    prepare_case_workspace,
)
from src.eval_scenarios import main as scenario_cli_main
from src.web_ui_new import _eval_agent_factory


def _write_pack(
    workspace: Path,
    *,
    fixture_content: str = "CAPABILITY_TOKEN\n",
) -> tuple[Path, Path]:
    pack_dir = workspace / "system" / "eval" / "core-capabilities"
    fixture = pack_dir / "fixtures" / "recovery" / "business" / "answer.txt"
    fixture.parent.mkdir(parents=True)
    fixture.write_text(fixture_content, encoding="utf-8")
    dataset = pack_dir / "cases.jsonl"
    dataset.write_text(
        json.dumps(
            {
                "id": "recovery",
                "name": "Recover from stale path",
                "task": "Inspect the stale path, recover, and report the token.",
                "tags": ["recovery"],
                "grader": {"type": "deterministic"},
                "runtime": {
                    "fixture": "recovery",
                    "scopes": ["workspace.read"],
                },
                "assertions": {
                    "contains": ["CAPABILITY_TOKEN"],
                    "tool_called": ["file_read", "file_search"],
                    "policy_outcome": ["allow"],
                    "file_exists": ["business/answer.txt"],
                    "recovered": True,
                    "max_total_tokens": 100,
                },
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    (pack_dir / "pack.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "id": "core-capabilities",
                "name": "Core capabilities",
                "version": "1.0.0",
                "dataset_file": "cases.jsonl",
                "fixtures_dir": "fixtures",
                "default_scopes": ["workspace.read"],
                "tools_allowlist": [
                    "file_read",
                    "file_search",
                    "file_write",
                ],
                "skill_allowlist": [],
                "memory_mode": "none",
                "max_turns": 8,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return dataset, fixture


class ScenarioPackContractTests(unittest.TestCase):
    def test_pack_digest_binds_manifest_dataset_and_fixtures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir)
            dataset, fixture = _write_pack(workspace)

            first = load_scenario_pack_for_dataset(workspace, dataset)
            self.assertIsNotNone(first)
            assert first is not None
            self.assertEqual(first.fixture_files, 1)
            self.assertEqual(first.default_scopes, ("workspace.read",))
            self.assertEqual(len(first.pack_digest), 64)

            fixture.write_text("CHANGED\n", encoding="utf-8")
            changed = load_scenario_pack_for_dataset(workspace, dataset)
            self.assertIsNotNone(changed)
            assert changed is not None
            self.assertNotEqual(first.pack_digest, changed.pack_digest)

    def test_pack_rejects_symlinked_fixture_and_invalid_location(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir)
            dataset, fixture = _write_pack(workspace)
            fixture.unlink()
            fixture.symlink_to(Path(tmp_dir) / "outside.txt")
            with self.assertRaises(ScenarioPackError):
                load_scenario_pack_for_dataset(workspace, dataset)

            business_pack = workspace / "business" / "pack"
            business_pack.mkdir(parents=True)
            (business_pack / "pack.json").write_text(
                (dataset.parent / "pack.json").read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            (business_pack / "cases.jsonl").write_text(
                dataset.read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            with self.assertRaises(ScenarioPackError):
                load_scenario_pack_for_dataset(
                    workspace,
                    business_pack / "cases.jsonl",
                )

    def test_pack_v1_rejects_host_and_shared_runtime_capabilities(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir)
            dataset, _fixture = _write_pack(workspace)
            manifest_path = dataset.parent / "pack.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

            manifest["default_scopes"].append("host.read")
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(
                ScenarioPackError,
                "does not isolate scopes",
            ):
                load_scenario_pack_for_dataset(workspace, dataset)

            manifest["default_scopes"] = ["workspace.read"]
            manifest["tools_allowlist"].append("web_scan")
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(
                ScenarioPackError,
                "does not isolate tools",
            ):
                load_scenario_pack_for_dataset(workspace, dataset)

    def test_pack_v1_allows_proposals_but_not_memory_activation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir)
            dataset, _fixture = _write_pack(workspace)
            manifest_path = dataset.parent / "pack.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["default_scopes"].append("memory.propose")
            manifest["tools_allowlist"].append("memory_propose")
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            pack = load_scenario_pack_for_dataset(workspace, dataset)

            self.assertIsNotNone(pack)
            assert pack is not None
            self.assertIn("memory.propose", pack.default_scopes)
            self.assertIn("memory_propose", pack.tools_allowlist)

            manifest["default_scopes"].append("memory.review")
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(
                ScenarioPackError,
                "does not isolate scopes",
            ):
                load_scenario_pack_for_dataset(workspace, dataset)

    def test_builtin_core_pack_is_importable(self) -> None:
        source = (
            Path(__file__).resolve().parents[1]
            / "workspace"
            / "default.ws"
            / "system"
            / "eval"
            / "core-capabilities-v1"
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir)
            destination = (
                workspace
                / "system"
                / "eval"
                / "core-capabilities-v1"
            )
            shutil.copytree(source, destination)

            metadata = import_dataset_path(
                workspace,
                rel_path=(
                    "system/eval/core-capabilities-v1/cases.jsonl"
                ),
            )

            self.assertEqual(metadata["case_count"], 6)
        self.assertEqual(
            metadata["scenario_pack"]["version"],
            "1.2.0",
        )

    def test_case_workspace_is_isolated_and_cleanup_is_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            workspace = root / "workspace"
            dataset, _fixture = _write_pack(workspace)
            pack = load_scenario_pack_for_dataset(workspace, dataset)
            assert pack is not None
            case = json.loads(dataset.read_text(encoding="utf-8"))

            runtime = prepare_case_workspace(pack, case, root / "run")

            copied = Path(runtime.workspace_root) / "business" / "answer.txt"
            self.assertEqual(copied.read_text(encoding="utf-8"), "CAPABILITY_TOKEN\n")
            self.assertEqual(runtime.scopes, ("workspace.read",))
            self.assertEqual(runtime.memory_mode, "none")
            cleanup_case_workspace(runtime)
            self.assertFalse(Path(runtime.workspace_root).exists())

            forged = EvalCaseRuntime(
                workspace_root=str(root / "not-a-sandbox"),
                sandbox_parent=str(root),
                scopes=(),
                tools_allowlist=(),
                skill_allowlist=(),
                memory_mode="none",
                max_turns=1,
                pack_id="pack",
                pack_version="1.0.0",
                pack_digest="a" * 64,
            )
            with self.assertRaises(ScenarioPackError):
                cleanup_case_workspace(forged)

    def test_fixture_copy_rejects_content_drift_after_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            workspace = root / "workspace"
            dataset, fixture = _write_pack(workspace)
            pack = load_scenario_pack_for_dataset(workspace, dataset)
            assert pack is not None
            case = json.loads(dataset.read_text(encoding="utf-8"))
            fixture.write_text("DRIFTED\n", encoding="utf-8")

            with self.assertRaisesRegex(
                ScenarioPackError,
                "changed after pack validation",
            ):
                prepare_case_workspace(pack, case, root / "run")

            sandboxes = root / "run" / "sandboxes"
            self.assertEqual(list(sandboxes.iterdir()), [])

    def test_import_rejects_dataset_changed_between_read_and_pack_load(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir)
            dataset, _fixture = _write_pack(workspace)
            original_load = load_scenario_pack_for_dataset

            def mutate_then_load(workspace_root, dataset_path):
                dataset.write_text(
                    dataset.read_text(encoding="utf-8").replace(
                        "CAPABILITY_TOKEN",
                        "DIFFERENT_TOKEN",
                    ),
                    encoding="utf-8",
                )
                return original_load(workspace_root, dataset_path)

            with (
                patch(
                    "src.core.eval.load_scenario_pack_for_dataset",
                    side_effect=mutate_then_load,
                ),
                self.assertRaisesRegex(
                    EvalError,
                    "changed while the pack was being read",
                ),
            ):
                import_dataset_path(
                    workspace,
                    rel_path=dataset.relative_to(workspace).as_posix(),
                )

    def test_cli_returns_machine_readable_error_for_invalid_case(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir)
            dataset, _fixture = _write_pack(workspace)
            dataset.write_text(
                '{"task":"bad","assertions":{"tool_callled":["file_read"]}}\n',
                encoding="utf-8",
            )
            stderr = io.StringIO()

            with redirect_stderr(stderr):
                exit_code = scenario_cli_main(
                    [
                        "--workspace",
                        str(workspace),
                        "--dataset",
                        dataset.relative_to(workspace).as_posix(),
                    ]
                )

            self.assertEqual(exit_code, 2)
            payload = json.loads(stderr.getvalue())
            self.assertEqual(payload["verdict"], "invalid")
            self.assertIn("unsupported assertion", payload["error"])


class ScenarioPackExecutionTests(unittest.TestCase):
    def test_import_and_execute_pack_in_ephemeral_case_workspace(self) -> None:
        class _FixtureAgent:
            def __init__(self, runtime: EvalCaseRuntime) -> None:
                self.runtime = runtime
                self.closed = False

            def run_task(self, _task: str) -> dict:
                token = (
                    Path(self.runtime.workspace_root)
                    / "business"
                    / "answer.txt"
                ).read_text(encoding="utf-8")
                return {
                    "response": token,
                    "exit_reason": "CURRENT_TASK_DONE",
                    "turns": 3,
                    "usage": {"total_tokens": 50},
                    "tool_results": [
                        {
                            "tool_name": "file_read",
                            "data": {"status": "ERROR"},
                            "policy": {"outcome": "allow"},
                        },
                        {
                            "tool_name": "file_search",
                            "data": {"status": "OK"},
                            "policy": {"outcome": "allow"},
                        },
                    ],
                }

            def close(self) -> None:
                self.closed = True

        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir)
            dataset, fixture = _write_pack(workspace)
            metadata = import_dataset_path(
                workspace,
                rel_path=dataset.relative_to(workspace).as_posix(),
            )
            run = create_eval_run(
                workspace,
                workspace="default.ws",
                dataset_id=metadata["id"],
                agent="main",
            )
            built: list[_FixtureAgent] = []

            def case_factory(runtime: EvalCaseRuntime) -> _FixtureAgent:
                agent = _FixtureAgent(runtime)
                built.append(agent)
                return agent

            result = execute_eval_run(
                workspace,
                run["id"],
                agent_factory=lambda: self.fail("shared workspace factory used"),
                case_agent_factory=case_factory,
            )

            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["summary"]["passed"], 1)
            self.assertEqual(result["scenario_pack"]["version"], "1.0.0")
            self.assertEqual(len(result["evaluation_digest"]), 64)
            self.assertEqual(len(built), 1)
            self.assertTrue(built[0].closed)
            self.assertFalse(Path(built[0].runtime.workspace_root).exists())
            self.assertEqual(
                fixture.read_text(encoding="utf-8"),
                "CAPABILITY_TOKEN\n",
            )

    def test_pack_drift_fails_before_agent_execution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir)
            dataset, fixture = _write_pack(workspace)
            metadata = import_dataset_path(
                workspace,
                rel_path=dataset.relative_to(workspace).as_posix(),
            )
            run = create_eval_run(
                workspace,
                workspace="default.ws",
                dataset_id=metadata["id"],
                agent="main",
            )
            fixture.write_text("DRIFTED\n", encoding="utf-8")
            called = False

            def case_factory(_runtime: EvalCaseRuntime):
                nonlocal called
                called = True
                return None

            result = execute_eval_run(
                workspace,
                run["id"],
                agent_factory=lambda: None,
                case_agent_factory=case_factory,
            )

            self.assertEqual(result["status"], "error")
            self.assertIn("changed after dataset import", result["error"])
            self.assertFalse(called)


class ScenarioWebFactoryTests(unittest.TestCase):
    def test_case_factory_only_narrows_principal_and_runtime(self) -> None:
        principal = Principal(
            subject="user",
            tenant_id="tenant",
            session_id="session",
            run_id="run",
            scopes=("workspace.read", "workspace.write"),
        )
        runtime = EvalCaseRuntime(
            workspace_root="/tmp/eval-case",
            sandbox_parent="/tmp/sandboxes",
            scopes=("workspace.read",),
            tools_allowlist=("file_read", "file_write"),
            skill_allowlist=(),
            memory_mode="none",
            max_turns=8,
            pack_id="pack",
            pack_version="1.0.0",
            pack_digest="a" * 64,
        )
        fake_agent = SimpleNamespace(
            handler=SimpleNamespace(ctx=SimpleNamespace(verbose=False))
        )
        with patch("src.web_ui_new.build_agent", return_value=fake_agent) as build:
            factory = _eval_agent_factory(
                ws="default.ws",
                ws_root="/tmp/source",
                agent_name="main",
                config_path="",
                observability_config_path="",
                runtime_config={
                    "tools_allowlist": ["file_read"],
                    "skill_allowlist": [],
                    "memory_mode": "project",
                    "max_turns": 20,
                },
                principal_template=principal,
            )
            factory(runtime)

        kwargs = build.call_args.kwargs
        self.assertEqual(kwargs["workspace_dir"], "/tmp/eval-case")
        self.assertEqual(kwargs["tools_allowlist"], ["file_read"])
        self.assertEqual(kwargs["skill_allowlist"], [])
        self.assertEqual(kwargs["memory_mode"], "none")
        self.assertEqual(kwargs["max_turns"], 8)
        self.assertEqual(kwargs["principal"].scopes, ("workspace.read",))

    def test_case_factory_rejects_scope_expansion(self) -> None:
        principal = Principal(
            subject="user",
            tenant_id="tenant",
            session_id="session",
            run_id="run",
            scopes=("workspace.read",),
        )
        runtime = EvalCaseRuntime(
            workspace_root="/tmp/eval-case",
            sandbox_parent="/tmp/sandboxes",
            scopes=("workspace.write",),
            tools_allowlist=("file_write",),
            skill_allowlist=(),
            memory_mode="none",
            max_turns=8,
            pack_id="pack",
            pack_version="1.0.0",
            pack_digest="a" * 64,
        )
        factory = _eval_agent_factory(
            ws="default.ws",
            ws_root="/tmp/source",
            agent_name="main",
            config_path="",
            observability_config_path="",
            runtime_config={},
            principal_template=principal,
        )

        with self.assertRaises(EvalError):
            factory(runtime)

    def test_case_factory_rejects_host_scope_even_when_caller_has_it(self) -> None:
        principal = Principal(
            subject="local-user",
            tenant_id="local",
            session_id="session",
            run_id="run",
            scopes=("workspace.read", "host.read"),
        )
        runtime = EvalCaseRuntime(
            workspace_root="/tmp/eval-case",
            sandbox_parent="/tmp/sandboxes",
            scopes=("workspace.read", "host.read"),
            tools_allowlist=("file_read",),
            skill_allowlist=(),
            memory_mode="none",
            max_turns=8,
            pack_id="pack",
            pack_version="1.0.0",
            pack_digest="a" * 64,
        )
        factory = _eval_agent_factory(
            ws="default.ws",
            ws_root="/tmp/source",
            agent_name="main",
            config_path="",
            observability_config_path="",
            runtime_config={},
            principal_template=principal,
        )

        with self.assertRaisesRegex(EvalError, "non-isolated scope"):
            factory(runtime)


if __name__ == "__main__":
    unittest.main()
