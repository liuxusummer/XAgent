from __future__ import annotations

import hashlib
import os
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from src.core.agent_loop import AgentContext
from src.handler import XAgentHandler
from src.tools.code_run import (
    _scratch_limit_exceeded,
    prepare_code_run_execution,
    run_code,
    run_code_stream,
)
from src.tools.code_sandbox import (
    CodeSandboxError,
    ExecutionLimits,
    SandboxCapability,
    _backend_binary,
    cleanup_execution_plan,
)


def _unavailable() -> SandboxCapability:
    return SandboxCapability(
        available=False,
        backend="none",
        security_level="unavailable",
        reason_code="SANDBOX_UNAVAILABLE",
        binary_identity_digest="",
        probe_digest="",
        filesystem_mode="none",
        network_mode="none",
        process_mode="none",
    )


class CodeSandboxTests(unittest.TestCase):
    @staticmethod
    def _verified(backend: str) -> SandboxCapability:
        return SandboxCapability(
            available=True,
            backend=backend,
            security_level="os_sandbox",
            reason_code="SANDBOX_VERIFIED",
            binary_identity_digest="a" * 64,
            probe_digest="b" * 64,
            filesystem_mode="workspace_read_only_control_hidden",
            network_mode="deny",
            process_mode=(
                "private_pid_namespace"
                if backend == "bubblewrap"
                else "fork_denied_seatbelt"
            ),
        )

    def test_required_backend_failure_never_starts_payload_or_falls_back(self) -> None:
        with (
            patch(
                "src.tools.code_sandbox.probe_code_sandbox",
                return_value=_unavailable(),
            ),
            patch("src.tools.code_run._start_process") as start,
        ):
            result = run_code(
                "raise SystemExit('must not run')",
                isolation_mode="required",
            )

        self.assertEqual(result["status"], "ERROR")
        self.assertEqual(result["reason_code"], "SANDBOX_UNAVAILABLE")
        self.assertFalse(result["security"]["unsafe"])
        start.assert_not_called()

    def test_unsafe_backend_requires_both_explicit_config_and_authorization(self) -> None:
        with self.assertRaises(CodeSandboxError) as raised:
            prepare_code_run_execution(
                script="print('x')",
                language="python",
                timeout=5,
                cwd=None,
                backend="unsafe",
                unsafe_authorized=False,
            )

        self.assertEqual(
            raised.exception.reason_code,
            "UNSAFE_EXECUTION_NOT_AUTHORIZED",
        )

    def test_explicit_unsafe_receipt_cannot_be_confused_with_a_sandbox(self) -> None:
        result = run_code("print('ok')", timeout=5, allow_unsafe=True)

        self.assertEqual(result["status"], "OK")
        receipt = result["security"]
        self.assertEqual(receipt["backend"], "local-process")
        self.assertEqual(receipt["security_level"], "development_unsafe")
        self.assertEqual(receipt["filesystem_mode"], "host_read_write")
        self.assertEqual(receipt["network_mode"], "host")
        self.assertTrue(receipt["unsafe"])
        self.assertNotIn(str(Path.cwd()), str(receipt))

    def test_shell_does_not_load_workspace_login_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            marker = root / "profile-loaded"
            (root / ".bash_profile").write_text(
                f"touch {marker}\n",
                encoding="utf-8",
            )
            result = run_code(
                "printf safe",
                language="shell",
                cwd=str(root),
                timeout=5,
                allow_unsafe=True,
            )

            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["stdout"], "safe")
            self.assertFalse(marker.exists())

    def test_successful_parent_exit_kills_remaining_process_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            marker = Path(tmp_dir) / "descendant-survived"
            script = (
                "import subprocess,sys; "
                "subprocess.Popen([sys.executable,'-c',"
                f"\"import time,pathlib; time.sleep(.5); pathlib.Path({str(marker)!r}).write_text('bad')\""
                "])"
            )
            result = run_code(
                script,
                cwd=tmp_dir,
                timeout=5,
                allow_unsafe=True,
            )
            time.sleep(0.8)

            self.assertEqual(result["status"], "OK")
            self.assertFalse(marker.exists())

    def test_closing_stream_kills_the_active_process_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            marker = Path(tmp_dir) / "abandoned-descendant"
            script = (
                "import subprocess,sys,time; "
                "subprocess.Popen([sys.executable,'-c',"
                f"\"import time,pathlib; time.sleep(.5); pathlib.Path({str(marker)!r}).write_text('bad')\""
                "]); "
                "print('ready',flush=True); time.sleep(10)"
            )
            stream = run_code_stream(
                script,
                cwd=tmp_dir,
                timeout=20,
                allow_unsafe=True,
            )
            first = next(stream)
            self.assertEqual(first["type"], "stdout")
            stream.close()
            time.sleep(0.8)

            self.assertFalse(marker.exists())

    def test_execution_plan_mismatch_is_rejected_before_process_start(self) -> None:
        plan = prepare_code_run_execution(
            script="print('approved')",
            language="python",
            timeout=5,
            cwd=None,
            backend="unsafe",
            unsafe_authorized=True,
        )
        with patch("src.tools.code_run._start_process") as start:
            chunks = list(
                run_code_stream(
                    "print('changed')",
                    timeout=5,
                    allow_unsafe=True,
                    execution_plan=plan,
                )
            )

        self.assertEqual(chunks[-1]["data"]["reason_code"], "EXECUTION_PLAN_MISMATCH")
        start.assert_not_called()

    def test_forged_launch_command_is_rejected_before_process_start(self) -> None:
        plan = prepare_code_run_execution(
            script="print('approved')",
            language="python",
            timeout=5,
            cwd=None,
            backend="unsafe",
            unsafe_authorized=True,
        )
        forged = replace(
            plan,
            launch_command=(
                "/bin/bash",
                "--noprofile",
                "--norc",
                "-c",
                "touch /tmp/forged-plan-ran",
            ),
        )
        with patch("src.tools.code_run._start_process") as start:
            chunks = list(
                run_code_stream(
                    "print('approved')",
                    timeout=5,
                    allow_unsafe=True,
                    execution_plan=forged,
                )
            )

        self.assertEqual(
            chunks[-1]["data"]["reason_code"],
            "EXECUTION_PLAN_MISMATCH",
        )
        start.assert_not_called()

    def test_unsafe_plan_still_requires_explicit_execution_authorization(self) -> None:
        plan = prepare_code_run_execution(
            script="print('approved')",
            language="python",
            timeout=5,
            cwd=None,
            backend="unsafe",
            unsafe_authorized=True,
        )
        with patch("src.tools.code_run._start_process") as start:
            chunks = list(
                run_code_stream(
                    "print('approved')",
                    timeout=5,
                    execution_plan=plan,
                )
            )

        self.assertEqual(
            chunks[-1]["data"]["reason_code"],
            "EXECUTION_AUTHORIZATION_REQUIRED",
        )
        start.assert_not_called()

    def test_arbitrary_execution_plan_object_fails_closed(self) -> None:
        with patch("src.tools.code_run._start_process") as start:
            chunks = list(
                run_code_stream(
                    "print('approved')",
                    timeout=5,
                    allow_unsafe=True,
                    execution_plan=object(),
                )
            )

        self.assertEqual(
            chunks[-1]["data"]["reason_code"],
            "EXECUTION_PLAN_MISMATCH",
        )
        start.assert_not_called()

    def test_invalid_unicode_script_fails_with_stable_reason(self) -> None:
        with self.assertRaises(CodeSandboxError) as raised:
            prepare_code_run_execution(
                script="print('\ud800')",
                language="python",
                timeout=5,
                cwd=None,
                backend="unsafe",
                unsafe_authorized=True,
            )

        self.assertEqual(raised.exception.reason_code, "SCRIPT_INVALID")

    def test_handler_preflight_fails_before_user_approval(self) -> None:
        prompts: list[str] = []
        handler = XAgentHandler(
            ctx=AgentContext(
                user_input_fn=lambda prompt: prompts.append(prompt) or "yes",
            )
        )
        with patch.dict(
            os.environ,
            {"XAGENT_CODE_RUN_BACKEND": "auto"},
            clear=False,
        ), patch(
            "src.handler.XAgentHandler.prepare_code_run_execution",
            side_effect=CodeSandboxError(
                "SANDBOX_UNAVAILABLE",
                "verified code sandbox is unavailable",
            ),
        ), patch.object(handler, "run_code_stream") as run_stream:
            result = handler.tool_before_callback(
                "code_run",
                {"script": "print('must not run')"},
            )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.data["reason_code"], "SANDBOX_UNAVAILABLE")
        self.assertEqual(prompts, [])
        run_stream.assert_not_called()

    def test_operator_deny_cannot_be_overridden_by_kernel_authorization(self) -> None:
        handler = XAgentHandler(ctx=AgentContext())
        handler._kernel_authorization_state.tool_name = "code_run"
        handler._kernel_authorization_state.action_digest = "a" * 64
        plan = prepare_code_run_execution(
            script="print('x')",
            language="python",
            timeout=5,
            cwd=None,
            backend="unsafe",
            unsafe_authorized=True,
        )
        with patch.dict(
            os.environ,
            {"XAGENT_CODE_RUN_POLICY": "deny"},
            clear=False,
        ):
            result = handler._authorize_code_run(
                "print('x')",
                "python",
                5,
                plan,
            )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.data["status"], "SKIP")
        self.assertIn("disabled", result.data["error"])

    def test_kernel_code_approval_displays_bounded_script_content(self) -> None:
        prompts: list[str] = []
        checkpoints: list[dict] = []
        script = "print('review me')\n" + ("x" * 2500)
        handler = XAgentHandler(
            ctx=AgentContext(
                user_input_fn=lambda prompt: prompts.append(prompt) or "no",
                checkpoint_callback=lambda snapshot: checkpoints.append(
                    dict(snapshot)
                ),
            )
        )
        plan = prepare_code_run_execution(
            script=script,
            language="python",
            timeout=5,
            cwd=None,
            backend="unsafe",
            unsafe_authorized=True,
        )
        with patch.object(
            handler,
            "_prepare_code_run_plan",
            return_value=plan,
        ):
            result = handler.tool_before_callback(
                "code_run",
                {
                    "script": script,
                    "language": "python",
                    "timeout": 5,
                },
            )

        self.assertIsNotNone(result)
        self.assertEqual(len(prompts), 1)
        self.assertIn("print('review me')", prompts[0])
        self.assertIn("不可信内容", prompts[0])
        self.assertIn(
            hashlib.sha256(script.encode("utf-8")).hexdigest(),
            prompts[0],
        )
        self.assertIn(f"truncated, total {len(script)} chars", prompts[0])
        rendered_script = (
            prompts[0]
            .split("```text\n", 1)[1]
            .split("\n```\n", 1)[0]
            .split("\n... [truncated", 1)[0]
        )
        self.assertEqual(rendered_script, script[:2000])
        self.assertNotIn("print('review me')", str(checkpoints))

    def test_bubblewrap_plan_binds_required_negative_capabilities(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir)
            for name in ("system", "runtime", "memory"):
                (workspace / name).mkdir()
            with (
                patch(
                    "src.tools.code_sandbox.probe_code_sandbox",
                    return_value=self._verified("bubblewrap"),
                ),
                patch(
                    "src.tools.code_sandbox._backend_binary",
                    return_value=Path("/usr/bin/true"),
                ),
            ):
                plan = prepare_code_run_execution(
                    script="print('safe')",
                    language="python",
                    timeout=5,
                    cwd=tmp_dir,
                    backend="bubblewrap",
                )

        command = list(plan.launch_command)
        self.assertIn("--unshare-all", command)
        self.assertIn("--clearenv", command)
        self.assertIn("--disable-userns", command)
        self.assertIn("--cap-drop", command)
        self.assertIn("--remount-ro", command)
        self.assertIn("--size", command)
        self.assertIn("--ro-bind", command)
        self.assertIn("/workspace", command)
        self.assertIn(str(plan.scratch_handle.name), command)
        for protected in ("system", "runtime", "memory"):
            self.assertIn(f"/workspace/{protected}", command)
        for protected in ("_intervene", "_keyinfo", "plan.md"):
            index = command.index(f"/workspace/{protected}")
            self.assertEqual(command[index - 2 : index], ["--ro-bind", "/dev/null"])
        binding = plan.approval_binding()
        self.assertEqual(binding["network_mode"], "deny")
        self.assertEqual(
            binding["filesystem_mode"],
            "workspace_read_only_control_hidden",
        )
        self.assertEqual(binding["process_mode"], "private_pid_namespace")
        self.assertNotIn(tmp_dir, str(plan.security_receipt()))
        cleanup_execution_plan(plan)

    def test_seatbelt_plan_uses_private_scratch_and_explicit_denies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir)
            for name in ("system", "runtime", "memory"):
                (workspace / name).mkdir()
            with (
                patch(
                    "src.tools.code_sandbox.probe_code_sandbox",
                    return_value=self._verified("sandbox-exec"),
                ),
                patch(
                    "src.tools.code_sandbox._backend_binary",
                    return_value=Path("/usr/bin/true"),
                ),
            ):
                plan = prepare_code_run_execution(
                    script="print('safe')",
                    language="python",
                    timeout=5,
                    cwd=tmp_dir,
                    backend="sandbox-exec",
                )
                scratch = Path(plan.scratch_handle.name)
                profile = plan.launch_command[2]
                self.assertTrue(scratch.is_dir())
                self.assertIn("(deny network*)", profile)
                for protected in ("system", "runtime", "memory"):
                    self.assertIn(str(workspace / protected), profile)
                for protected in ("_intervene", "_keyinfo", "plan.md"):
                    self.assertIn(
                        f'(literal "{workspace.resolve() / protected}")',
                        profile,
                    )
                self.assertIn(str(scratch), profile)
                self.assertNotEqual(scratch, Path(tempfile.gettempdir()))
                cleanup_execution_plan(plan)
                self.assertFalse(scratch.exists())

    def test_safe_backend_scratch_quota_is_supervised_by_the_parent(self) -> None:
        for backend in ("bubblewrap", "sandbox-exec"):
            with self.subTest(backend=backend):
                self._assert_safe_backend_scratch_quota(backend)

    def _assert_safe_backend_scratch_quota(self, backend: str) -> None:
        with (
            tempfile.TemporaryDirectory() as tmp_dir,
            patch(
                "src.tools.code_sandbox.probe_code_sandbox",
                return_value=self._verified(backend),
            ),
            patch(
                "src.tools.code_sandbox._backend_binary",
                return_value=Path("/usr/bin/true"),
            ),
        ):
            plan = prepare_code_run_execution(
                script="print('safe')",
                language="python",
                timeout=5,
                cwd=tmp_dir,
                backend=backend,
            )
            scratch = Path(plan.scratch_handle.name)
            (scratch / "large").write_text("12345", encoding="utf-8")
            limited = replace(
                plan,
                limits=ExecutionLimits(
                    scratch_bytes=4,
                    scratch_entries=64,
                ),
            )
            try:
                self.assertTrue(_scratch_limit_exceeded(limited))
            finally:
                cleanup_execution_plan(plan)

    def test_reader_thread_start_failure_reaps_process(self) -> None:
        class _Stream:
            def close(self) -> None:
                return None

        class _Process:
            pid = 12345
            stdout = _Stream()
            stderr = _Stream()
            returncode = -9

            def poll(self):
                return None

            def kill(self) -> None:
                return None

            def wait(self, timeout=None):
                del timeout
                return self.returncode

        process = _Process()
        plan = prepare_code_run_execution(
            script="print('safe')",
            language="python",
            timeout=5,
            cwd=None,
            backend="unsafe",
            unsafe_authorized=True,
        )
        with (
            patch("src.tools.code_run._start_process", return_value=process),
            patch("src.tools.code_run._kill_process_tree") as kill,
            patch.object(
                threading.Thread,
                "start",
                side_effect=RuntimeError("thread quota"),
            ),
        ):
            events = list(
                run_code_stream(
                    "print('safe')",
                    timeout=5,
                    allow_unsafe=True,
                    execution_plan=plan,
                )
            )

        self.assertEqual(
            events[-1]["data"]["reason_code"],
            "EXECUTION_SUPERVISOR_START_FAILED",
        )
        kill.assert_called_with(process)

    def test_protected_workspace_symlink_fails_before_backend_probe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir, tempfile.TemporaryDirectory() as outside:
            (Path(tmp_dir) / "runtime").symlink_to(outside, target_is_directory=True)
            with patch(
                "src.tools.code_sandbox.probe_code_sandbox"
            ) as probe, self.assertRaises(CodeSandboxError) as raised:
                prepare_code_run_execution(
                    script="print('x')",
                    language="python",
                    timeout=5,
                    cwd=tmp_dir,
                    backend="bubblewrap",
                )

        self.assertEqual(
            raised.exception.reason_code,
            "WORKSPACE_BOUNDARY_INVALID",
        )
        probe.assert_not_called()

    def test_broken_protected_symlink_also_fails_before_backend_probe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "runtime").symlink_to(
                Path(tmp_dir) / "missing",
                target_is_directory=True,
            )
            with patch(
                "src.tools.code_sandbox.probe_code_sandbox"
            ) as probe, self.assertRaises(CodeSandboxError) as raised:
                prepare_code_run_execution(
                    script="print('x')",
                    language="python",
                    timeout=5,
                    cwd=tmp_dir,
                    backend="bubblewrap",
                )

        self.assertEqual(
            raised.exception.reason_code,
            "WORKSPACE_BOUNDARY_INVALID",
        )
        probe.assert_not_called()

    def test_backend_binary_cannot_be_selected_from_untrusted_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            fake = Path(tmp_dir) / "bwrap"
            fake.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            fake.chmod(0o755)
            with patch(
                "src.tools.code_sandbox._SYSTEM_BACKEND_PATHS",
                {"bubblewrap": (fake,)},
            ), self.assertRaises(CodeSandboxError) as raised:
                _backend_binary("bubblewrap")

        self.assertEqual(raised.exception.reason_code, "SANDBOX_UNAVAILABLE")

    def test_bubblewrap_masks_control_paths_even_when_initially_absent(self) -> None:
        with (
            tempfile.TemporaryDirectory() as tmp_dir,
            patch(
                "src.tools.code_sandbox.probe_code_sandbox",
                return_value=self._verified("bubblewrap"),
            ),
            patch(
                "src.tools.code_sandbox._backend_binary",
                return_value=Path("/usr/bin/true"),
            ),
        ):
            plan = prepare_code_run_execution(
                script="print('safe')",
                language="python",
                timeout=5,
                cwd=tmp_dir,
                backend="bubblewrap",
            )

        command = list(plan.launch_command)
        for protected in ("system", "runtime", "memory"):
            self.assertIn(f"/workspace/{protected}", command)
        cleanup_execution_plan(plan)

    def test_safe_plans_mask_mixed_case_reserved_entries(self) -> None:
        for backend in ("bubblewrap", "sandbox-exec"):
            with self.subTest(backend=backend), tempfile.TemporaryDirectory() as tmp_dir:
                workspace = Path(tmp_dir)
                (workspace / "RUNTIME").mkdir()
                (workspace / "PLAN.MD").write_text(
                    "control",
                    encoding="utf-8",
                )
                with (
                    patch(
                        "src.tools.code_sandbox.probe_code_sandbox",
                        return_value=self._verified(backend),
                    ),
                    patch(
                        "src.tools.code_sandbox._backend_binary",
                        return_value=Path("/usr/bin/true"),
                    ),
                ):
                    plan = prepare_code_run_execution(
                        script="print('safe')",
                        language="python",
                        timeout=5,
                        cwd=tmp_dir,
                        backend=backend,
                    )
                try:
                    rendered = " ".join(plan.launch_command)
                    self.assertIn("RUNTIME", rendered)
                    self.assertIn("PLAN.MD", rendered)
                finally:
                    cleanup_execution_plan(plan)

    def test_resource_limits_are_part_of_the_immutable_plan_digest(self) -> None:
        with patch.dict(
            os.environ,
            {"XAGENT_CODE_RUN_CPU_SECONDS": "5"},
            clear=False,
        ):
            first = prepare_code_run_execution(
                script="print('x')",
                language="python",
                timeout=5,
                cwd=None,
                backend="unsafe",
                unsafe_authorized=True,
            )
        with patch.dict(
            os.environ,
            {"XAGENT_CODE_RUN_CPU_SECONDS": "6"},
            clear=False,
        ):
            second = prepare_code_run_execution(
                script="print('x')",
                language="python",
                timeout=5,
                cwd=None,
                backend="unsafe",
                unsafe_authorized=True,
            )

        self.assertNotEqual(first.binding_digest, second.binding_digest)
        self.assertEqual(first.approval_binding()["limits"]["cpu_seconds"], 5)
        self.assertEqual(second.approval_binding()["limits"]["cpu_seconds"], 6)

    def test_oversized_script_is_rejected_before_backend_probe(self) -> None:
        with patch(
            "src.tools.code_sandbox.MAX_CODE_SCRIPT_BYTES",
            8,
        ), patch(
            "src.tools.code_sandbox.probe_code_sandbox"
        ) as probe, self.assertRaises(CodeSandboxError) as raised:
            prepare_code_run_execution(
                script="print('too large')",
                language="python",
                timeout=5,
                cwd=None,
                backend="bubblewrap",
            )

        self.assertEqual(raised.exception.reason_code, "SCRIPT_TOO_LARGE")
        probe.assert_not_called()


if __name__ == "__main__":
    unittest.main()
