from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from src.orchestration.artifacts import LocalArtifactStore
from src.orchestration.policy import (
    ActionRequest,
    Capability,
    EffectClass,
    PolicyDecision,
    PolicyOutcome,
)
from src.orchestration.sandbox import (
    BackendExecutionResult,
    EnvironmentBinding,
    ExecutionRequest,
    NetworkMode,
    ResourceLimits,
    SandboxDispatchDenied,
    SandboxDispatcher,
    SandboxOutcome,
    SandboxProfile,
    SandboxValidationError,
    SecurityLevel,
    build_execution_binding_digest,
)


POLICY_VERSION = "sha256:" + ("a" * 64)
READ = Capability("workspace.read")
WRITE = Capability("workspace.write")
RUN = Capability("process.execute")


class FakeBackend:
    supports_materialized_script = True

    def __init__(
        self,
        backend_id: str,
        *,
        security_level: SecurityLevel = SecurityLevel.OS_SANDBOX,
        capabilities: tuple[Capability, ...] = (READ, RUN),
        result: BackendExecutionResult | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.backend_id = backend_id
        self.security_level = security_level
        self.capabilities = capabilities
        self.result = result or BackendExecutionResult(exit_code=0)
        self.error = error
        self.calls: list[tuple[ExecutionRequest, SandboxProfile]] = []

    def execute(
        self,
        request: ExecutionRequest,
        profile: SandboxProfile,
    ) -> BackendExecutionResult:
        self.calls.append((request, profile))
        if self.error is not None:
            raise self.error
        return self.result


class SandboxDispatcherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name) / "workspace"
        self.root.mkdir()
        self.cwd = self.root / "project"
        self.cwd.mkdir()
        self.artifacts = LocalArtifactStore(Path(self.temp_dir.name) / "scripts")
        self.script_bytes = b"print('verified script')"
        self.script_ref = self.artifacts.put_bytes(self.script_bytes)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _action(
        self,
        *,
        effect: EffectClass = EffectClass.READ_ONLY,
        capabilities: tuple[Capability, ...] = (READ, RUN),
    ) -> ActionRequest:
        return ActionRequest.from_args(
            run_id="run-1",
            node_id="node-1",
            attempt_id="attempt-1",
            tool_name="code_run",
            args={"script_artifact": "artifact-1"},
            execution_binding_digest="e" * 64,
            operation_key="operation-1",
            idempotency_key="operation-1",
            effect_class=effect,
            capabilities=capabilities,
            resource_locks=("workspace:/project",),
        )

    def _decision(
        self,
        action: ActionRequest,
        *,
        outcome: PolicyOutcome = PolicyOutcome.ALLOW,
        action_digest: str | None = None,
        policy_version: str = POLICY_VERSION,
    ) -> PolicyDecision:
        return PolicyDecision(
            outcome=outcome,
            action_digest=action.action_digest if action_digest is None else action_digest,
            policy_version=policy_version,
            reason_code="test_policy",
        )

    def _profile(
        self,
        *,
        capabilities: tuple[Capability, ...] = (READ, RUN),
        minimum_security_level: SecurityLevel = SecurityLevel.OS_SANDBOX,
        limits: ResourceLimits | None = None,
        environment_allowlist: tuple[str, ...] = (),
    ) -> SandboxProfile:
        return SandboxProfile(
            profile_id="test-profile",
            allowed_roots=(self.root,),
            capabilities=capabilities,
            limits=limits or ResourceLimits(),
            minimum_security_level=minimum_security_level,
            network_mode=NetworkMode.DENY,
            environment_allowlist=environment_allowlist,
        )

    def _request(
        self,
        action: ActionRequest | None = None,
        decision: PolicyDecision | None = None,
        *,
        cwd: Path | str | None = None,
        argv: tuple[str, ...] = ("python", "worker.py"),
        limits: ResourceLimits | None = None,
        environment: tuple[EnvironmentBinding, ...] = (),
        profile: SandboxProfile | None = None,
    ) -> ExecutionRequest:
        selected_action = action or self._action()
        selected_profile = profile or self._profile(
            capabilities=selected_action.capabilities,
        )
        selected_limits = limits or ResourceLimits()
        selected_cwd = str(self.cwd if cwd is None else cwd)
        binding_digest = build_execution_binding_digest(
            argv=argv,
            cwd=selected_cwd,
            profile=selected_profile,
            limits=selected_limits,
            script_artifact_ref=self.script_ref,
            operation_key="operation-1",
            idempotency_key="operation-1",
            environment=environment,
            capabilities=selected_action.capabilities,
            resource_locks=selected_action.resource_locks,
        )
        bound_action = replace(
            selected_action,
            execution_binding_digest=binding_digest,
            script_artifact_sha256=self.script_ref.sha256,
        )
        selected_decision = decision or self._decision(bound_action)
        if (
            decision is not None
            and decision.action_digest == selected_action.action_digest
        ):
            selected_decision = replace(
                decision,
                action_digest=bound_action.action_digest,
            )
        return ExecutionRequest(
            action=bound_action,
            policy_decision=selected_decision,
            argv=argv,
            operation_key="operation-1",
            idempotency_key="operation-1",
            cwd=selected_cwd,
            limits=selected_limits,
            script_artifact_ref=self.script_ref,
            materialized_script=self.script_bytes,
            environment=environment,
        )

    def test_success_receipt_is_stable_and_contains_no_process_output(self) -> None:
        artifact_store = LocalArtifactStore(Path(self.temp_dir.name) / "artifacts")
        output_ref = artifact_store.put_json({"result": "stored externally"})
        backend = FakeBackend(
            "secure",
            result=BackendExecutionResult(exit_code=0, output_artifact_refs=(output_ref,)),
        )
        dispatcher = SandboxDispatcher((backend,), policy_version=POLICY_VERSION)
        request = self._request()
        profile = self._profile()

        first = dispatcher.dispatch(request, profile)
        second = dispatcher.dispatch(request, profile)

        self.assertEqual(first.outcome, SandboxOutcome.SUCCEEDED)
        self.assertEqual(first.receipt_digest, second.receipt_digest)
        serialized = json.dumps(first.to_dict())
        self.assertNotIn("stdout", serialized)
        self.assertNotIn("stored externally", serialized)
        self.assertEqual(first.output_artifact_refs[0].artifact_id, output_ref.artifact_id)

    def test_policy_outcome_action_and_version_must_match(self) -> None:
        action = self._action()
        cases = (
            (
                self._decision(action, outcome=PolicyOutcome.DENY),
                "policy_not_allowed",
            ),
            (
                self._decision(action, action_digest="c" * 64),
                "policy_action_mismatch",
            ),
            (
                self._decision(action, policy_version="sha256:" + ("d" * 64)),
                "policy_version_mismatch",
            ),
        )
        dispatcher = SandboxDispatcher(
            (FakeBackend("secure"),),
            policy_version=POLICY_VERSION,
        )
        for decision, reason in cases:
            with self.subTest(reason=reason):
                with self.assertRaises(SandboxDispatchDenied) as raised:
                    dispatcher.dispatch(
                        self._request(action, decision),
                        self._profile(),
                    )
                self.assertEqual(raised.exception.reason_code, reason)

    def test_cwd_traversal_and_parent_symlink_are_denied(self) -> None:
        dispatcher = SandboxDispatcher(
            (FakeBackend("secure"),),
            policy_version=POLICY_VERSION,
        )
        outside = Path(self.temp_dir.name) / "outside"
        outside.mkdir()
        link = self.root / "outside-link"
        try:
            link.symlink_to(outside, target_is_directory=True)
        except (NotImplementedError, OSError):
            self.skipTest("symlinks are unavailable")

        for cwd in (self.root / ".." / "outside", link):
            with self.subTest(cwd=cwd):
                with self.assertRaises(SandboxDispatchDenied) as raised:
                    dispatcher.dispatch(self._request(cwd=cwd), self._profile())
                self.assertEqual(raised.exception.reason_code, "cwd_outside_allowed_roots")

    def test_argv_must_be_tuple_not_shell_string(self) -> None:
        action = self._action()
        with self.assertRaises(SandboxValidationError):
            ExecutionRequest(
                action=action,
                policy_decision=self._decision(action),
                argv="python worker.py",  # type: ignore[arg-type]
                operation_key="operation-1",
                idempotency_key="operation-1",
                cwd=str(self.cwd),
            )

    def test_environment_contains_only_names_and_redacted_digests(self) -> None:
        secret = "host-secret-must-never-appear"
        binding = EnvironmentBinding(
            "API_TOKEN",
            hashlib.sha256(("trusted-hmac:" + secret).encode()).hexdigest(),
        )
        profile = self._profile(environment_allowlist=("API_TOKEN",))
        request = self._request(environment=(binding,), profile=profile)
        receipt = SandboxDispatcher(
            (FakeBackend("secure"),),
            policy_version=POLICY_VERSION,
        ).dispatch(request, profile)

        self.assertNotIn(secret, repr(request))
        self.assertNotIn(secret, json.dumps(request.to_dict()))
        self.assertNotIn(secret, json.dumps(receipt.to_dict()))
        self.assertEqual(request.environment[0].name, "API_TOKEN")

    def test_profile_cannot_drop_action_capabilities(self) -> None:
        dispatcher = SandboxDispatcher(
            (FakeBackend("secure"),),
            policy_version=POLICY_VERSION,
        )
        profile = self._profile(capabilities=(READ,))
        with self.assertRaises(SandboxDispatchDenied) as raised:
            dispatcher.dispatch(
                self._request(profile=profile),
                profile,
            )
        self.assertEqual(raised.exception.reason_code, "profile_capability_mismatch")

    def test_dispatcher_rechecks_bound_execution_intent(self) -> None:
        backend = FakeBackend("secure")
        dispatcher = SandboxDispatcher((backend,), policy_version=POLICY_VERSION)
        request = self._request()

        with self.assertRaises(SandboxDispatchDenied) as raised:
            dispatcher.dispatch(
                replace(request, argv=("python", "substituted.py")),
                self._profile(),
            )

        self.assertEqual(raised.exception.reason_code, "execution_binding_mismatch")
        self.assertEqual(backend.calls, [])

    def test_materialized_script_must_match_immutable_artifact(self) -> None:
        backend = FakeBackend("secure")
        dispatcher = SandboxDispatcher((backend,), policy_version=POLICY_VERSION)
        request = self._request()

        with self.assertRaises(SandboxValidationError):
            replace(request, materialized_script=b"substituted after approval")

        self.assertEqual(backend.calls, [])

    def test_backend_must_explicitly_support_materialized_scripts(self) -> None:
        backend = FakeBackend("no-script-contract")
        backend.supports_materialized_script = False
        dispatcher = SandboxDispatcher((backend,), policy_version=POLICY_VERSION)

        with self.assertRaises(SandboxDispatchDenied) as raised:
            dispatcher.dispatch(self._request(), self._profile())

        self.assertEqual(raised.exception.reason_code, "no_qualified_backend")
        self.assertEqual(backend.calls, [])

    def test_operation_key_substitution_is_rejected_before_dispatch(self) -> None:
        backend = FakeBackend("secure")
        request = self._request()

        with self.assertRaises(SandboxValidationError):
            replace(request, operation_key="substituted-operation")

        self.assertEqual(backend.calls, [])

    def test_required_script_request_cannot_omit_artifact_materialization(self) -> None:
        action = replace(
            self._action(),
            requires_script_artifact=True,
            script_artifact_sha256=None,
        )
        decision = self._decision(action)

        with self.assertRaisesRegex(
            SandboxValidationError,
            "requires a verified script Artifact",
        ):
            ExecutionRequest(
                action=action,
                policy_decision=decision,
                argv=("python", str(self.cwd / "mutable.py")),
                operation_key="operation-1",
                idempotency_key="operation-1",
                cwd=str(self.cwd),
            )

    def test_backend_must_have_profile_and_network_capabilities(self) -> None:
        profile = SandboxProfile(
            profile_id="network-profile",
            allowed_roots=(self.root,),
            capabilities=(READ, RUN),
            network_mode=NetworkMode.PUBLIC_PROXY,
        )
        backend = FakeBackend("missing-network", capabilities=(READ, RUN))

        with self.assertRaises(SandboxDispatchDenied) as raised:
            SandboxDispatcher((backend,), policy_version=POLICY_VERSION).dispatch(
                self._request(profile=profile),
                profile,
            )
        self.assertEqual(raised.exception.reason_code, "no_qualified_backend")
        self.assertEqual(backend.calls, [])

    def test_no_backend_fails_closed(self) -> None:
        dispatcher = SandboxDispatcher((), policy_version=POLICY_VERSION)
        with self.assertRaises(SandboxDispatchDenied) as raised:
            dispatcher.dispatch(self._request(), self._profile())
        self.assertEqual(raised.exception.reason_code, "no_qualified_backend")

    def test_development_unsafe_backend_cannot_execute_writes(self) -> None:
        action = self._action(
            effect=EffectClass.IDEMPOTENT_WRITE,
            capabilities=(WRITE, RUN),
        )
        backend = FakeBackend(
            "unsafe",
            security_level=SecurityLevel.DEVELOPMENT_UNSAFE,
            capabilities=(WRITE, RUN),
        )
        dispatcher = SandboxDispatcher((backend,), policy_version=POLICY_VERSION)
        profile = self._profile(
            capabilities=(WRITE, RUN),
            minimum_security_level=SecurityLevel.DEVELOPMENT_UNSAFE,
        )

        with self.assertRaises(SandboxDispatchDenied) as raised:
            dispatcher.dispatch(
                self._request(
                    action,
                    self._decision(action),
                    profile=profile,
                ),
                profile,
            )
        self.assertEqual(raised.exception.reason_code, "no_qualified_backend")
        self.assertEqual(backend.calls, [])

    def test_trusted_function_write_requires_explicit_profile_opt_in(self) -> None:
        action = self._action(
            effect=EffectClass.IDEMPOTENT_WRITE,
            capabilities=(WRITE, RUN),
        )
        backend = FakeBackend(
            "trusted-closed-function",
            security_level=SecurityLevel.TRUSTED_FUNCTION,
            capabilities=(WRITE, RUN),
        )
        dispatcher = SandboxDispatcher((backend,), policy_version=POLICY_VERSION)
        default_profile = self._profile(capabilities=(WRITE, RUN))
        request = self._request(
            action,
            self._decision(action),
            profile=default_profile,
        )

        with self.assertRaises(SandboxDispatchDenied) as raised:
            dispatcher.dispatch(request, default_profile)
        self.assertEqual(raised.exception.reason_code, "no_qualified_backend")
        self.assertEqual(backend.calls, [])

        trusted_profile = self._profile(
            capabilities=(WRITE, RUN),
            minimum_security_level=SecurityLevel.TRUSTED_FUNCTION,
        )
        receipt = dispatcher.dispatch(
            self._request(
                action,
                self._decision(action),
                profile=trusted_profile,
            ),
            trusted_profile,
        )
        self.assertEqual(receipt.outcome, SandboxOutcome.SUCCEEDED)
        self.assertEqual(receipt.security_level, SecurityLevel.TRUSTED_FUNCTION)
        self.assertEqual(len(backend.calls), 1)

    def test_backend_exception_returns_failure_without_fallback(self) -> None:
        failing = FakeBackend("a-secure", error=RuntimeError("secret backend error"))
        unsafe = FakeBackend(
            "z-unsafe",
            security_level=SecurityLevel.DEVELOPMENT_UNSAFE,
        )
        dispatcher = SandboxDispatcher(
            (unsafe, failing),
            policy_version=POLICY_VERSION,
        )
        profile = self._profile(
            minimum_security_level=SecurityLevel.DEVELOPMENT_UNSAFE,
        )

        receipt = dispatcher.dispatch(self._request(profile=profile), profile)

        self.assertEqual(receipt.outcome, SandboxOutcome.BACKEND_ERROR)
        self.assertEqual(receipt.error_code, "backend_exception")
        self.assertEqual(len(failing.calls), 1)
        self.assertEqual(unsafe.calls, [])
        self.assertNotIn("secret backend error", json.dumps(receipt.to_dict()))

    def test_request_limits_cannot_exceed_profile_or_global_bounds(self) -> None:
        profile = self._profile(
            limits=ResourceLimits(
                timeout_seconds=10,
                cpu_seconds=10,
                memory_bytes=1024,
                output_bytes=1024,
                process_count=1,
            )
        )
        request = self._request(
            limits=ResourceLimits(
                timeout_seconds=11,
                cpu_seconds=10,
                memory_bytes=1024,
                output_bytes=1024,
                process_count=1,
            )
        )
        dispatcher = SandboxDispatcher(
            (FakeBackend("secure"),),
            policy_version=POLICY_VERSION,
        )

        with self.assertRaises(SandboxDispatchDenied) as raised:
            dispatcher.dispatch(request, profile)
        self.assertEqual(raised.exception.reason_code, "resource_limits_exceed_profile")
        with self.assertRaises(SandboxValidationError):
            ResourceLimits(output_bytes=65 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
