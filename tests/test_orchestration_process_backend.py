from __future__ import annotations

import json
import os
import re
import selectors
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from src.orchestration.artifacts import LocalArtifactStore
from src.orchestration.policy import (
    ActionRequest,
    EffectClass,
    PolicyDecision,
    PolicyOutcome,
)
from src.orchestration.process_backend import LocalProcessSupervisorBackend
from src.orchestration.sandbox import (
    CancellationProbe,
    CancellationSignal,
    ExecutionRequest,
    ResourceLimits,
    SandboxDispatcher,
    SandboxOutcome,
    SandboxProfile,
    SandboxValidationError,
    SecurityLevel,
    build_execution_binding_digest,
)

POLICY_VERSION = "sha256:" + ("a" * 64)
FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "orchestration_process_tree.py"
)


class LocalProcessSupervisorBackendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.artifacts = LocalArtifactStore(self.root / "artifacts")
        self.profile = SandboxProfile(
            "unsafe-process-supervision",
            (self.root,),
            (),
            minimum_security_level=SecurityLevel.DEVELOPMENT_UNSAFE,
        )
        self.backend = LocalProcessSupervisorBackend(self.artifacts)
        self.dispatcher = SandboxDispatcher(
            (self.backend,),
            policy_version=POLICY_VERSION,
        )

    def _request(
        self,
        argv: tuple[str, ...],
        *,
        limits: ResourceLimits,
        cancellation_probe: CancellationProbe | None = None,
    ) -> ExecutionRequest:
        placeholder = ActionRequest.from_args(
            run_id="process-run",
            node_id="process-node",
            attempt_id="process-attempt",
            tool_name="local_process",
            args={"fixture": "f14"},
            execution_binding_digest="e" * 64,
            operation_key="process-operation",
            idempotency_key="process-operation",
            effect_class=EffectClass.READ_ONLY,
        )
        binding = build_execution_binding_digest(
            argv=argv,
            cwd=str(self.root),
            profile=self.profile,
            limits=limits,
            operation_key="process-operation",
            idempotency_key="process-operation",
        )
        action = replace(
            placeholder,
            execution_binding_digest=binding,
        )
        decision = PolicyDecision(
            PolicyOutcome.ALLOW,
            action.action_digest,
            POLICY_VERSION,
            "test_allow",
        )
        return ExecutionRequest(
            action,
            decision,
            argv=argv,
            operation_key="process-operation",
            idempotency_key="process-operation",
            cwd=str(self.root),
            limits=limits,
            cancellation_probe=cancellation_probe,
        )

    def test_short_process_succeeds_and_output_only_crosses_as_artifact(self) -> None:
        marker = "process-output-artifact-marker"
        request = self._request(
            (
                sys.executable,
                "-c",
                f"import sys; print('{marker}'); print('stderr-line', file=sys.stderr)",
            ),
            limits=ResourceLimits(timeout_seconds=2, output_bytes=4096),
        )

        receipt = self.dispatcher.dispatch(request, self.profile)

        self.assertEqual(receipt.outcome, SandboxOutcome.SUCCEEDED)
        self.assertEqual(
            receipt.security_level,
            SecurityLevel.DEVELOPMENT_UNSAFE,
        )
        self.assertEqual(len(receipt.output_artifact_refs), 1)
        self.assertNotIn(marker, json.dumps(receipt.to_dict()))
        output = json.loads(
            self.artifacts.read(receipt.output_artifact_refs[0]).decode("utf-8")
        )
        self.assertIn(marker, output["stdout"])
        self.assertIn("stderr-line", output["stderr"])
        self.assertFalse(output["timed_out"])

    def test_output_is_bounded_and_host_environment_is_not_inherited(self) -> None:
        request = self._request(
            (
                sys.executable,
                "-c",
                (
                    "import os;"
                    "print(os.getenv('XAGENT_HOST_SECRET', 'absent'));"
                    "print('x' * 8192)"
                ),
            ),
            limits=ResourceLimits(timeout_seconds=2, output_bytes=128),
        )

        with patch.dict(os.environ, {"XAGENT_HOST_SECRET": "must-not-cross"}):
            receipt = self.dispatcher.dispatch(request, self.profile)

        self.assertEqual(receipt.outcome, SandboxOutcome.FAILED)
        output = json.loads(
            self.artifacts.read(receipt.output_artifact_refs[0]).decode("utf-8")
        )
        self.assertTrue(output["output_truncated"])
        self.assertLessEqual(
            len(output["stdout"].encode("utf-8"))
            + len(output["stderr"].encode("utf-8")),
            128,
        )
        self.assertIn("absent", output["stdout"])
        self.assertNotIn("must-not-cross", output["stdout"])

    def test_cancellation_probe_identity_cannot_be_substituted(self) -> None:
        probe = CancellationProbe(
            "another-run",
            "process-node",
            "process-attempt",
            lambda: CancellationSignal.CONTINUE,
        )

        with self.assertRaisesRegex(
            SandboxValidationError,
            "does not match",
        ):
            self._request(
                (sys.executable, "-c", "pass"),
                limits=ResourceLimits(timeout_seconds=2),
                cancellation_probe=probe,
            )

    def test_probe_exception_stops_before_spawn_and_is_uncertain(self) -> None:
        def broken_probe() -> CancellationSignal:
            raise OSError("durable store unavailable")

        probe = CancellationProbe(
            "process-run",
            "process-node",
            "process-attempt",
            broken_probe,
        )
        request = self._request(
            (sys.executable, "-c", "raise SystemExit(0)"),
            limits=ResourceLimits(timeout_seconds=2),
            cancellation_probe=probe,
        )

        with patch(
            "src.orchestration.process_backend.subprocess.Popen"
        ) as popen:
            receipt = self.dispatcher.dispatch(request, self.profile)

        popen.assert_not_called()
        self.assertEqual(
            receipt.outcome,
            SandboxOutcome.CANCELLATION_UNKNOWN,
        )

    @unittest.skipUnless(os.name == "posix", "requires POSIX process groups")
    def test_durable_cancellation_reaps_running_process_group(self) -> None:
        script = self.root / "cancellable_tree.py"
        pid_file = self.root / "tree.pids"
        ready_file = self.root / "tree.ready"
        script.write_text(
            "\n".join(
                (
                    "import os, signal, subprocess, sys, time",
                    "from pathlib import Path",
                    "role, pid_path, ready_path = sys.argv[1:4]",
                    "signal.signal(signal.SIGTERM, lambda *_: None)",
                    "with open(pid_path, 'a', encoding='utf-8') as stream:",
                    "    stream.write(f'{role}:{os.getpid()}\\n')",
                    "    stream.flush()",
                    "if role == 'parent':",
                    "    subprocess.Popen((sys.executable, __file__, 'child', pid_path, ready_path))",
                    "elif role == 'child':",
                    "    subprocess.Popen((sys.executable, __file__, 'grandchild', pid_path, ready_path))",
                    "if role == 'parent':",
                    "    deadline = time.monotonic() + 5",
                    "    while time.monotonic() < deadline:",
                    "        if Path(pid_path).exists() and len(Path(pid_path).read_text().splitlines()) >= 3:",
                    "            Path(ready_path).write_text('ready', encoding='utf-8')",
                    "            break",
                    "        time.sleep(0.01)",
                    "time.sleep(60)",
                )
            ),
            encoding="utf-8",
        )
        barrier = threading.Barrier(2)
        cancel = threading.Event()

        def poll() -> CancellationSignal:
            if not ready_file.exists():
                return CancellationSignal.CONTINUE
            barrier.wait(timeout=5)
            if not cancel.wait(timeout=5):
                return CancellationSignal.UNKNOWN
            return CancellationSignal.CANCEL_REQUESTED

        probe = CancellationProbe(
            "process-run",
            "process-node",
            "process-attempt",
            poll,
        )
        request = self._request(
            (
                sys.executable,
                str(script),
                "parent",
                str(pid_file),
                str(ready_file),
            ),
            limits=ResourceLimits(timeout_seconds=10, output_bytes=4096),
            cancellation_probe=probe,
        )

        def request_cancel() -> None:
            barrier.wait(timeout=5)
            cancel.set()

        controller = threading.Thread(target=request_cancel)
        controller.start()
        receipt = self.dispatcher.dispatch(request, self.profile)
        controller.join(timeout=5)

        self.assertFalse(controller.is_alive())
        self.assertEqual(receipt.outcome, SandboxOutcome.CANCELLED)
        roles = {
            role: int(pid)
            for role, pid in re.findall(
                r"^(parent|child|grandchild):(\d+)$",
                pid_file.read_text(encoding="utf-8"),
                flags=re.MULTILINE,
            )
        }
        self.assertEqual(set(roles), {"parent", "child", "grandchild"})
        for role, pid in roles.items():
            with self.subTest(role=role, pid=pid):
                self.assertTrue(self._wait_not_running(pid))

    @unittest.skipUnless(os.name == "posix", "requires POSIX process groups")
    def test_timeout_terminates_parent_child_and_grandchild_group(self) -> None:
        request = self._request(
            (sys.executable, str(FIXTURE), "parent"),
            limits=ResourceLimits(timeout_seconds=0.5, output_bytes=4096),
        )

        receipt = self.dispatcher.dispatch(request, self.profile)

        self.assertEqual(receipt.outcome, SandboxOutcome.TIMED_OUT)
        output = json.loads(
            self.artifacts.read(receipt.output_artifact_refs[0]).decode("utf-8")
        )
        roles = {
            role: int(pid)
            for role, pid in re.findall(
                r"^(parent|child|grandchild):(\d+)$",
                output["stdout"],
                flags=re.MULTILINE,
            )
        }
        self.assertEqual(set(roles), {"parent", "child", "grandchild"})
        for role, pid in roles.items():
            with self.subTest(role=role, pid=pid):
                self.assertTrue(self._wait_not_running(pid))

    @unittest.skipUnless(os.name == "posix", "requires POSIX process groups")
    def test_vanished_group_leader_probe_still_reaps_descendants(self) -> None:
        request = self._request(
            (sys.executable, str(FIXTURE), "parent"),
            limits=ResourceLimits(timeout_seconds=0.5, output_bytes=4096),
        )

        with patch(
            "src.orchestration.process_backend.os.getpgid",
            side_effect=ProcessLookupError,
        ):
            receipt = self.dispatcher.dispatch(request, self.profile)

        self.assertEqual(receipt.outcome, SandboxOutcome.TIMED_OUT)
        output = json.loads(
            self.artifacts.read(receipt.output_artifact_refs[0]).decode("utf-8")
        )
        pids = [
            int(pid)
            for pid in re.findall(
                r"^(?:parent|child|grandchild):(\d+)$",
                output["stdout"],
                flags=re.MULTILINE,
            )
        ]
        self.assertEqual(len(pids), 3)
        self.assertTrue(all(self._wait_not_running(pid) for pid in pids))

    @unittest.skipUnless(os.name == "posix", "requires POSIX process groups")
    def test_selector_registration_failure_reaps_process_tree_and_streams(
        self,
    ) -> None:
        request = self._request(
            (sys.executable, str(FIXTURE), "parent"),
            limits=ResourceLimits(timeout_seconds=5, output_bytes=4096),
        )
        captured = bytearray()
        spawned: list[subprocess.Popen[bytes]] = []
        selector_instances = []
        sent_signals: list[int] = []
        real_selector = selectors.DefaultSelector
        real_killpg = os.killpg
        real_popen = subprocess.Popen

        class RegisterFailureSelector:
            def __init__(self) -> None:
                self.delegate = real_selector()
                self.closed = False
                selector_instances.append(self)

            def register(self, fileobj, events, data=None):
                key = self.delegate.register(fileobj, events, data)
                deadline = time.monotonic() + 5
                while (
                    captured.count(b"\n") < 3
                    and time.monotonic() < deadline
                ):
                    for ready, _mask in self.delegate.select(timeout=0.05):
                        chunk = os.read(ready.fd, 4096)
                        if chunk:
                            captured.extend(chunk)
                self.delegate.unregister(key.fileobj)
                if captured.count(b"\n") < 3:
                    raise AssertionError(
                        "process tree did not start before selector seam"
                    )
                raise OSError("selector-register-failure")

            def close(self) -> None:
                self.closed = True
                self.delegate.close()

        def recording_popen(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            spawned.append(process)
            return process

        def recording_killpg(process_group, sent_signal):
            sent_signals.append(sent_signal)
            return real_killpg(process_group, sent_signal)

        with (
            patch(
                "src.orchestration.process_backend.selectors.DefaultSelector",
                RegisterFailureSelector,
            ),
            patch(
                "src.orchestration.process_backend.subprocess.Popen",
                side_effect=recording_popen,
            ),
            patch(
                "src.orchestration.process_backend.os.killpg",
                side_effect=recording_killpg,
            ),
            self.assertRaisesRegex(
                OSError,
                "selector-register-failure",
            ),
        ):
            self.backend.execute(request, self.profile)

        self.assertEqual(len(spawned), 1)
        process = spawned[0]
        self.assertIsNotNone(process.returncode)
        assert process.stdout is not None and process.stderr is not None
        self.assertTrue(process.stdout.closed)
        self.assertTrue(process.stderr.closed)
        self.assertEqual(len(selector_instances), 1)
        self.assertTrue(selector_instances[0].closed)
        termination_signals = [
            sent_signal
            for sent_signal in sent_signals
            if sent_signal != 0
        ]
        self.assertEqual(termination_signals[0], signal.SIGTERM)
        self.assertIn(signal.SIGKILL, termination_signals[1:])
        roles = {
            role: int(pid)
            for role, pid in re.findall(
                rb"^(parent|child|grandchild):(\d+)$",
                bytes(captured),
                flags=re.MULTILINE,
            )
        }
        self.assertEqual(set(roles), {b"parent", b"child", b"grandchild"})
        for role, pid in roles.items():
            with self.subTest(role=role, pid=pid):
                self.assertTrue(self._wait_not_running(pid))

    @unittest.skipUnless(os.name == "posix", "requires POSIX process groups")
    def test_successful_leader_exit_reaps_silent_descendant(self) -> None:
        request = self._request(
            (
                sys.executable,
                str(FIXTURE),
                "exit-with-silent-child",
            ),
            limits=ResourceLimits(timeout_seconds=2, output_bytes=4096),
        )

        receipt = self.dispatcher.dispatch(request, self.profile)

        self.assertEqual(receipt.outcome, SandboxOutcome.SUCCEEDED)
        output = json.loads(
            self.artifacts.read(receipt.output_artifact_refs[0]).decode("utf-8")
        )
        match = re.search(
            r"^silent-child-pid:(\d+)$",
            output["stdout"],
            flags=re.MULTILINE,
        )
        self.assertIsNotNone(match)
        assert match is not None
        self.assertTrue(self._wait_not_running(int(match.group(1))))

    @staticmethod
    def _wait_not_running(pid: int) -> bool:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return True
            except PermissionError:
                return False
            time.sleep(0.02)
        return False


if __name__ == "__main__":
    unittest.main()
