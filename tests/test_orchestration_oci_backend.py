from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.orchestration.artifacts import LocalArtifactStore
from src.orchestration.oci_backend import (
    OciGvisorSandboxBackend,
    OciReceiptBindingError,
    OciRuntimeResult,
    OciSandboxSpecBuilder,
    OciSpecDenied,
    RuntimeAttestation,
    RuntimeAttestationInvalid,
    RuntimeIsolationKind,
)
from src.orchestration.policy import (
    ActionRequest,
    Capability,
    EffectClass,
    PolicyDecision,
    PolicyOutcome,
)
from src.orchestration.sandbox import (
    ExecutionRequest,
    NetworkMode,
    ResourceLimits,
    SandboxDispatcher,
    SandboxOutcome,
    SandboxProfile,
    SecurityLevel,
    build_execution_binding_digest,
)


POLICY_VERSION = "sha256:" + ("a" * 64)
IMAGE = "registry.example/xagent/worker@sha256:" + ("1" * 64)
SECCOMP_DIGEST = "2" * 64
READ = Capability("workspace.read")
RUN = Capability("process.execute")


class _Clock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class _Adapter:
    adapter_id = "adapter-1"

    def __init__(self, *, mismatch: str | None = None) -> None:
        self.mismatch = mismatch
        self.specs = []

    def execute(self, spec):
        self.specs.append(spec)
        values = {
            "spec_digest": spec.spec_digest,
            "action_digest": spec.action_digest,
            "request_digest": spec.request_digest,
            "runtime_attestation_digest": spec.runtime_attestation_digest,
            "exit_code": 0,
        }
        if self.mismatch is not None:
            values[self.mismatch] = "f" * 64
        return OciRuntimeResult(**values)


class _Verifier:
    def __init__(
        self,
        attestation: RuntimeAttestation | None = None,
        *,
        error: BaseException | None = None,
    ) -> None:
        self.attestation = attestation or RuntimeAttestation(
            adapter_id="adapter-1",
            isolation_kind=RuntimeIsolationKind.GVISOR,
            runtime_class="runsc",
            runtime_binary_digest="3" * 64,
            verifier_id="deployment-attestor",
            attestation_id="runtime-attestation-1",
            issued_at=90.0,
            expires_at=180.0,
        )
        self.error = error
        self.calls = 0

    def verify(self, adapter, *, now: float) -> RuntimeAttestation:
        del adapter, now
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.attestation


class OciGvisorSandboxBackendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.workspace = self.root / "agent-workspace"
        self.cwd = self.workspace / "project"
        self.cwd.mkdir(parents=True)
        self.artifacts = LocalArtifactStore(self.root / "control-plane" / "artifacts")
        self.input_ref = self.artifacts.put_bytes(b"immutable input")
        self.clock = _Clock()
        self.profile = SandboxProfile(
            profile_id="remote-code",
            allowed_roots=(self.workspace,),
            capabilities=(READ, RUN),
            limits=ResourceLimits(
                timeout_seconds=30,
                cpu_seconds=20,
                memory_bytes=256 * 1024 * 1024,
                output_bytes=1024 * 1024,
                process_count=32,
            ),
            minimum_security_level=SecurityLevel.CONTAINER,
            network_mode=NetworkMode.DENY,
        )
        self.builder = OciSandboxSpecBuilder(
            image=IMAGE,
            runtime_class="runsc",
            seccomp_profile_digest=SECCOMP_DIGEST,
        )

    def _request(
        self,
        *,
        argv: tuple[str, ...] = ("/usr/bin/python3", "/inputs/artifact-0"),
        profile: SandboxProfile | None = None,
        limits: ResourceLimits | None = None,
    ) -> ExecutionRequest:
        selected_profile = profile or self.profile
        selected_limits = limits or ResourceLimits(
            timeout_seconds=10,
            cpu_seconds=5,
            memory_bytes=64 * 1024 * 1024,
            output_bytes=1024 * 1024,
            process_count=4,
        )
        operation_key = "operation-1"
        binding = build_execution_binding_digest(
            argv=argv,
            cwd=str(self.cwd),
            profile=selected_profile,
            limits=selected_limits,
            input_artifact_refs=(self.input_ref,),
            operation_key=operation_key,
            idempotency_key=operation_key,
            capabilities=(READ, RUN),
        )
        action = ActionRequest.from_args(
            run_id="run-1",
            node_id="node-1",
            attempt_id="attempt-1",
            tool_name="code_run",
            args={"input": "artifact"},
            execution_binding_digest=binding,
            operation_key=operation_key,
            idempotency_key=operation_key,
            effect_class=EffectClass.READ_ONLY,
            capabilities=(READ, RUN),
        )
        decision = PolicyDecision(
            outcome=PolicyOutcome.ALLOW,
            action_digest=action.action_digest,
            policy_version=POLICY_VERSION,
            reason_code="test_allow",
        )
        return ExecutionRequest(
            action=action,
            policy_decision=decision,
            argv=argv,
            operation_key=operation_key,
            idempotency_key=operation_key,
            cwd=str(self.cwd),
            limits=selected_limits,
            input_artifact_refs=(self.input_ref,),
        )

    def _backend(
        self,
        *,
        adapter: _Adapter | None = None,
        verifier: _Verifier | None = None,
    ) -> OciGvisorSandboxBackend:
        return OciGvisorSandboxBackend(
            backend_id="gvisor-reference",
            adapter=adapter or _Adapter(),
            attestation_verifier=verifier or _Verifier(),
            spec_builder=self.builder,
            capabilities=(READ, RUN),
            clock=self.clock,
        )

    def test_builds_hardened_path_free_spec_and_executes_exact_argv(self) -> None:
        adapter = _Adapter()
        verifier = _Verifier()
        backend = self._backend(adapter=adapter, verifier=verifier)

        result = backend.execute(self._request(), self.profile)

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(verifier.calls, 2)
        self.assertEqual(len(adapter.specs), 1)
        spec = adapter.specs[0]
        self.assertEqual(spec.image, IMAGE)
        self.assertTrue(spec.rootfs_read_only)
        self.assertTrue(spec.no_new_privileges)
        self.assertEqual(spec.dropped_capabilities, ("ALL",))
        self.assertGreater(spec.run_as_uid, 0)
        self.assertGreater(spec.run_as_gid, 0)
        self.assertEqual(spec.network_mode, NetworkMode.DENY)
        self.assertEqual(spec.argv[0], "/usr/bin/python3")
        self.assertTrue(all(mount.kind.value in {"tmpfs", "artifact"} for mount in spec.mounts))
        self.assertTrue(
            all(
                mount.source_binding_digest is None
                or len(mount.source_binding_digest) == 64
                for mount in spec.mounts
            )
        )
        encoded = json.dumps(spec.to_dict(), sort_keys=True)
        self.assertNotIn(str(self.workspace), encoded)
        self.assertNotIn(str(self.artifacts.root), encoded)
        self.assertNotIn(self.input_ref.uri, encoded)

    def test_dispatcher_receipt_uses_container_only_after_attestation(self) -> None:
        backend = self._backend()
        receipt = SandboxDispatcher(
            (backend,),
            policy_version=POLICY_VERSION,
        ).dispatch(self._request(), self.profile)

        self.assertEqual(receipt.outcome, SandboxOutcome.SUCCEEDED)
        self.assertEqual(receipt.security_level, SecurityLevel.CONTAINER)
        self.assertEqual(receipt.backend_id, "gvisor-reference")

    def test_immutable_image_and_seccomp_digest_are_mandatory(self) -> None:
        with self.assertRaisesRegex(OciSpecDenied, "image_must_use_immutable_digest"):
            OciSandboxSpecBuilder(
                image="registry.example/xagent/worker:latest",
                runtime_class="runsc",
                seccomp_profile_digest=SECCOMP_DIGEST,
            )
        with self.assertRaisesRegex(OciSpecDenied, "invalid_oci_digest"):
            OciSandboxSpecBuilder(
                image=IMAGE,
                runtime_class="runsc",
                seccomp_profile_digest="default.json",
            )

    def test_shell_and_relative_executables_are_rejected(self) -> None:
        backend = self._backend()
        for argv in (
            ("/bin/sh", "-c", "id"),
            ("python3", "-c", "print(1)"),
        ):
            with self.subTest(argv=argv):
                with self.assertRaisesRegex(
                    OciSpecDenied,
                    "direct_non_shell_executable_required",
                ):
                    backend.execute(self._request(argv=argv), self.profile)

    def test_non_deny_network_profile_is_rejected(self) -> None:
        network_profile = SandboxProfile(
            profile_id="networked",
            allowed_roots=(self.workspace,),
            capabilities=(READ, RUN),
            limits=self.profile.limits,
            minimum_security_level=SecurityLevel.CONTAINER,
            network_mode=NetworkMode.PUBLIC_PROXY,
        )
        request = self._request(profile=network_profile)
        with self.assertRaisesRegex(
            OciSpecDenied,
            "network_default_deny_required",
        ):
            self._backend().execute(request, network_profile)

    def test_missing_stale_or_wrong_adapter_attestation_never_claims_container(self) -> None:
        with self.assertRaisesRegex(
            RuntimeAttestationInvalid,
            "runtime attestation failed",
        ):
            self._backend(
                verifier=_Verifier(error=RuntimeError("fake gvisor claim")),
            )

        stale = RuntimeAttestation(
            adapter_id="adapter-1",
            isolation_kind=RuntimeIsolationKind.GVISOR,
            runtime_class="runsc",
            runtime_binary_digest="3" * 64,
            verifier_id="deployment-attestor",
            attestation_id="stale",
            issued_at=10.0,
            expires_at=100.0,
        )
        with self.assertRaisesRegex(
            RuntimeAttestationInvalid,
            "not current",
        ):
            self._backend(verifier=_Verifier(stale))

        wrong_adapter = RuntimeAttestation(
            adapter_id="adapter-2",
            isolation_kind=RuntimeIsolationKind.GVISOR,
            runtime_class="runsc",
            runtime_binary_digest="3" * 64,
            verifier_id="deployment-attestor",
            attestation_id="wrong-adapter",
            issued_at=90.0,
            expires_at=180.0,
        )
        with self.assertRaisesRegex(
            RuntimeAttestationInvalid,
            "adapter binding mismatch",
        ):
            self._backend(verifier=_Verifier(wrong_adapter))

    def test_attestation_is_rechecked_at_execution_time(self) -> None:
        attestation = RuntimeAttestation(
            adapter_id="adapter-1",
            isolation_kind=RuntimeIsolationKind.GVISOR,
            runtime_class="runsc",
            runtime_binary_digest="3" * 64,
            verifier_id="deployment-attestor",
            attestation_id="short-lived",
            issued_at=90.0,
            expires_at=101.0,
        )
        backend = self._backend(verifier=_Verifier(attestation))
        self.clock.value = 101.0
        with self.assertRaisesRegex(
            RuntimeAttestationInvalid,
            "not current",
        ):
            backend.execute(self._request(), self.profile)

    def test_runtime_result_must_bind_spec_action_request_and_attestation(self) -> None:
        for mismatch in (
            "spec_digest",
            "action_digest",
            "request_digest",
            "runtime_attestation_digest",
        ):
            with self.subTest(mismatch=mismatch):
                backend = self._backend(adapter=_Adapter(mismatch=mismatch))
                with self.assertRaisesRegex(
                    OciReceiptBindingError,
                    "runtime result binding mismatch",
                ):
                    backend.execute(self._request(), self.profile)

    def test_resource_limits_and_mount_shapes_fail_closed(self) -> None:
        oversized = ResourceLimits(
            timeout_seconds=40,
            cpu_seconds=30,
            memory_bytes=512 * 1024 * 1024,
            output_bytes=2 * 1024 * 1024,
            process_count=64,
        )
        request = self._request(limits=oversized)
        with self.assertRaisesRegex(
            OciSpecDenied,
            "resource_limits_exceed_profile",
        ):
            self._backend().execute(request, self.profile)

        with self.assertRaises(OciSpecDenied):
            OciSandboxSpecBuilder(
                image=IMAGE,
                runtime_class="runsc",
                seccomp_profile_digest=SECCOMP_DIGEST,
                workspace_tmpfs_bytes=0,
            )


if __name__ == "__main__":
    unittest.main()
