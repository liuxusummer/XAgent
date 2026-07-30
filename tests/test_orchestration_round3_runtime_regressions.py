from __future__ import annotations

import unittest

from src.orchestration.remote_scheduling import (
    RemoteTask,
    RuntimeCompatibility,
    WorkerDescriptor,
    is_worker_compatible,
)
from src.orchestration.runtime_compatibility import (
    RuntimeCompatibility as NeutralRuntimeCompatibility,
    RuntimeCompatibilityValidationError,
)
from src.orchestration.workflow import WorkflowCompileError, compile_workflow


def _workflow(metadata: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": 2,
        "name": "runtime-compatibility",
        "version": 1,
        "nodes": [
            {
                "id": "tool",
                "kind": "tool",
                "config": {"tool": "exec", "arguments": {}},
                "metadata": metadata,
                "effect_class": "read_only",
            }
        ],
    }


class RuntimeCompatibilityRegressionTests(unittest.TestCase):
    def test_workflow_compiles_immutable_activity_runtime_range(self) -> None:
        workflow = compile_workflow(
            _workflow(
                {
                    "min_runtime_version": "2",
                    "max_runtime_version": "2.9",
                }
            )
        )
        node = workflow.get_node("tool")

        self.assertEqual(
            RuntimeCompatibility("2", "2.9"),
            node.runtime_compatibility,
        )
        self.assertEqual("2", node.metadata["min_runtime_version"])
        self.assertNotEqual(
            workflow.definition_digest,
            compile_workflow(_workflow({})).definition_digest,
        )

    def test_invalid_activity_runtime_ranges_fail_at_compile_boundary(self) -> None:
        invalid = (
            {"min_runtime_version": "02.1"},
            {"min_runtime_version": "3", "max_runtime_version": "2.9"},
            {"min_runtime_version": 2},
            {"max_runtime_version": "runtime/2"},
        )
        for metadata in invalid:
            with self.subTest(metadata=metadata):
                with self.assertRaises(WorkflowCompileError):
                    compile_workflow(_workflow(metadata))

    def test_control_node_cannot_masquerade_as_runtime_constrained(self) -> None:
        workflow = _workflow({})
        workflow["nodes"] = [
            {
                "id": "approval",
                "kind": "approval",
                "config": {"prompt": "approve"},
                "metadata": {"min_runtime_version": "2"},
            }
        ]
        with self.assertRaises(WorkflowCompileError):
            compile_workflow(workflow)

    def test_undeclared_activity_remains_backward_compatible(self) -> None:
        node = compile_workflow(_workflow({})).get_node("tool")
        self.assertIsNone(node.runtime_compatibility)

    def test_remote_task_and_direct_range_share_numeric_semantics(self) -> None:
        self.assertIs(RuntimeCompatibility, NeutralRuntimeCompatibility)
        compatibility = RuntimeCompatibility("2", "2.9")
        task = RemoteTask(
            task_id="task",
            tenant_id="tenant",
            pool_id="pool",
            tool_name="exec",
            min_runtime_version="2",
            max_runtime_version="2.9",
        )
        worker = WorkerDescriptor(
            worker_id="worker",
            session_id="session",
            pool_id="pool",
            runtime_version="2.1",
            capabilities=frozenset(),
            tools=frozenset({"exec"}),
            authorized_tenants=frozenset({"tenant"}),
        )

        self.assertTrue(compatibility.accepts(worker.runtime_version))
        self.assertTrue(is_worker_compatible(worker, task))
        self.assertFalse(compatibility.accepts("1.9"))
        self.assertFalse(compatibility.accepts("runsc-2.1"))
        with self.assertRaises(RuntimeCompatibilityValidationError):
            RuntimeCompatibility("02.1")


if __name__ == "__main__":
    unittest.main()
