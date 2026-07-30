from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from src.core.agent_kernel import Principal
from src.core.agent_loop import AgentContext, exhaust
from src.core.checkpoint import build_task_checkpoint, render_resume_prompt
from src.core.local_policy import LOCAL_APPROVAL_LEDGER, LOCAL_PRINCIPAL_SCOPES
from src.core.telemetry import Event
from src.handler import XAgentHandler


class _Sink:
    def __init__(self) -> None:
        self.events: list[Event] = []

    def emit(self, event: Event) -> None:
        self.events.append(event)

    def close(self) -> None:
        return None


class DefaultLoopPolicyIntegrationTests(unittest.TestCase):
    def _principal(self, scopes: tuple[str, ...] = LOCAL_PRINCIPAL_SCOPES) -> Principal:
        return Principal(
            subject="user-1",
            tenant_id="tenant-1",
            session_id="session-1",
            run_id="run-1",
            scopes=scopes,
        )

    def test_scope_denial_happens_before_a_write_side_effect(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "blocked.txt"
            handler = XAgentHandler(
                ctx=AgentContext(
                    cwd=tmp,
                    principal=self._principal(("workspace.read",)),
                )
            )
            result = exhaust(
                handler.dispatch(
                    "file_write",
                    {"path": "blocked.txt", "content": "must not exist"},
                )
            )
            self.assertEqual(result.data["status"], "SKIP")
            self.assertEqual(result.data["reason_code"], "principal_scope_missing")
            self.assertFalse(target.exists())

    def test_rejected_high_risk_action_never_reaches_the_tool(self) -> None:
        prompts: list[str] = []
        checkpoints: list[dict] = []
        with tempfile.TemporaryDirectory() as tmp:
            handler = XAgentHandler(
                ctx=AgentContext(
                    cwd=tmp,
                    principal=self._principal(),
                    user_input_fn=lambda prompt: prompts.append(prompt) or "no",
                    checkpoint_callback=lambda snapshot: checkpoints.append(
                        dict(snapshot)
                    ),
                    last_checkpoint_snapshot={
                        "session_id": "session-1",
                        "turn": 1,
                        "tool_results": [
                            {"tool_name": "file_read", "data": {"status": "OK"}}
                        ],
                    },
                )
            )
            with patch.object(handler, "_get_browser_driver") as get_driver:
                result = exhaust(
                    handler.dispatch(
                        "web_execute_js",
                        {"script": "document.title = 'changed'"},
                    )
                )
            self.assertEqual(result.data["status"], "SKIP")
            self.assertEqual(result.data["reason_code"], "approval_rejected")
            self.assertIn("动作摘要", prompts[0])
            get_driver.assert_not_called()
            self.assertEqual(
                [snapshot["status"] for snapshot in checkpoints],
                ["waiting_approval", "running"],
            )
            self.assertEqual(
                checkpoints[0]["pending_approval"]["status"],
                "pending",
            )
            self.assertEqual(
                checkpoints[1]["pending_approval"]["status"],
                "rejected",
            )
            self.assertEqual(
                checkpoints[0]["tool_results"][0]["tool_name"],
                "file_read",
            )

    def test_approved_code_run_is_prompted_once_and_persistently_bound(self) -> None:
        prompts: list[str] = []
        sink = _Sink()
        chunks = iter(
            [
                {
                    "type": "result",
                    "data": {"status": "OK", "exit_code": 0, "stderr": ""},
                }
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            handler = XAgentHandler(
                ctx=AgentContext(
                    cwd=tmp,
                    stop_signal=threading.Event(),
                    principal=self._principal(),
                    user_input_fn=lambda prompt: prompts.append(prompt) or "yes",
                    sink=sink,
                    session_id="session-1",
                )
            )
            with (
                patch.dict(
                    "os.environ",
                    {"XAGENT_CODE_RUN_BACKEND": "unsafe"},
                ),
                patch.object(handler, "run_code_stream", return_value=chunks),
            ):
                result = exhaust(
                    handler.dispatch(
                        "code_run",
                        {"language": "python", "script": "print('safe')"},
                    )
                )
            self.assertEqual(result.data["status"], "OK")
            self.assertEqual(len(prompts), 1)
            self.assertIn("print('safe')", prompts[0])
            self.assertIn("不可信内容", prompts[0])
            self.assertIn("script_sha256", prompts[0])
            self.assertTrue((Path(tmp) / LOCAL_APPROVAL_LEDGER).is_file())

            decisions = [
                event
                for event in sink.events
                if event.kind == "policy_decision" and event.name == "code_run"
            ]
            self.assertEqual(
                [event.data["outcome"] for event in decisions],
                ["require_approval", "allow"],
            )
            self.assertEqual(
                decisions[0].data["action_digest"],
                decisions[1].data["action_digest"],
            )
            self.assertTrue(decisions[1].data["approval_id"])

    def test_failed_tool_cannot_leave_authorization_for_a_direct_followup(self) -> None:
        prompts: list[str] = []
        checkpoints: list[dict] = []
        with tempfile.TemporaryDirectory() as tmp:
            handler = XAgentHandler(
                ctx=AgentContext(
                    cwd=tmp,
                    stop_signal=threading.Event(),
                    principal=self._principal(),
                    user_input_fn=lambda prompt: prompts.append(prompt) or "yes",
                    checkpoint_callback=lambda snapshot: checkpoints.append(
                        dict(snapshot)
                    ),
                    last_checkpoint_snapshot={
                        "session_id": "session-1",
                        "turn": 2,
                    },
                )
            )
            with (
                patch.dict(
                    "os.environ",
                    {"XAGENT_CODE_RUN_BACKEND": "unsafe"},
                ),
                patch.object(
                    handler,
                    "run_code_stream",
                    side_effect=RuntimeError("boom"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "boom"):
                    exhaust(
                        handler.dispatch(
                            "code_run",
                            {"language": "python", "script": "print('first')"},
                        )
                    )
            self.assertFalse(handler._is_kernel_authorized("code_run"))
            self.assertEqual(
                checkpoints[-1]["pending_approval"]["status"],
                "approved",
            )
            checkpoint = build_task_checkpoint(
                tmp,
                checkpoint_id="session-1",
                session_id="session-1",
                task="run code",
                pending_approval=checkpoints[-1]["pending_approval"],
                principal_digest=self._principal().principal_digest,
            )
            resume = render_resume_prompt(checkpoint)
            self.assertIn("Treat the side-effect outcome as unknown", resume)
            self.assertIn(
                checkpoints[-1]["pending_approval"]["action_digest"],
                resume,
            )

            with (
                patch.dict(
                    "os.environ",
                    {
                        "XAGENT_CODE_RUN_POLICY": "confirm",
                        "XAGENT_CODE_RUN_BACKEND": "unsafe",
                    },
                ),
                patch.object(handler, "run_code_stream") as run_stream,
            ):
                handler.ctx.user_input_fn = lambda prompt: prompts.append(prompt) or "no"
                result = handler.exec_code_run(
                    {"language": "python", "script": "print('second')"}
                )
            self.assertEqual(result.data["status"], "SKIP")
            run_stream.assert_not_called()


if __name__ == "__main__":
    unittest.main()
