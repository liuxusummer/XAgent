from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError

from src.orchestration.workflow import (
    MAX_WORKFLOW_BYTES,
    WorkflowCompileError,
    compile_team_workflow_v1,
    compile_workflow,
)


def _agent(node_id: str, depends_on=None, **extra):
    node = {
        "id": node_id,
        "kind": "agent",
        "depends_on": list(depends_on or []),
        "config": {"agent": "main", "task": f"run {node_id}"},
    }
    node.update(extra)
    return node


def _workflow(nodes):
    return {
        "schema_version": 2,
        "name": "test-workflow",
        "version": 1,
        "nodes": nodes,
    }


class WorkflowCompilerTests(unittest.TestCase):
    def test_accepts_out_of_order_dependencies(self) -> None:
        compiled = compile_workflow(
            _workflow(
                [
                    _agent("finish", ["plan"]),
                    _agent("plan"),
                ]
            )
        )

        self.assertEqual(compiled.topological_order, ("plan", "finish"))
        self.assertEqual(compiled.dependents["plan"], ("finish",))
        self.assertEqual(compiled.roots, ("plan",))
        self.assertEqual(compiled.leaves, ("finish",))

    def test_multiple_roots_form_deterministic_parallel_layer(self) -> None:
        compiled = compile_workflow(
            _workflow(
                [
                    {
                        "id": "join",
                        "kind": "join",
                        "depends_on": ["b", "a"],
                        "config": {"mode": "all"},
                    },
                    _agent("b"),
                    _agent("a"),
                ]
            )
        )

        self.assertEqual(compiled.layers, (("a", "b"), ("join",)))
        self.assertEqual(compiled.topological_order, ("a", "b", "join"))
        self.assertEqual(compiled.roots, ("a", "b"))

    def test_digest_does_not_depend_on_input_node_or_dependency_order(self) -> None:
        first = compile_workflow(
            _workflow([_agent("a"), _agent("b"), _agent("c", ["b", "a"])])
        )
        second = compile_workflow(
            _workflow([_agent("c", ["a", "b"]), _agent("b"), _agent("a")])
        )

        self.assertEqual(first.definition_digest, second.definition_digest)
        self.assertEqual(first.topological_order, second.topological_order)

    def test_cycle_report_is_stable(self) -> None:
        raw = _workflow(
            [
                _agent("c", ["a"]),
                _agent("a", ["b"]),
                _agent("b", ["c"]),
            ]
        )

        with self.assertRaisesRegex(
            WorkflowCompileError,
            r"workflow contains cycle: a -> b -> c -> a",
        ):
            compile_workflow(raw)

        raw["nodes"].reverse()
        with self.assertRaisesRegex(
            WorkflowCompileError,
            r"workflow contains cycle: a -> b -> c -> a",
        ):
            compile_workflow(raw)

    def test_rejects_unknown_dependency_self_dependency_and_duplicate_id(self) -> None:
        cases = [
            (_workflow([_agent("a", ["missing"])]), "unknown dependencies"),
            (_workflow([_agent("a", ["a"])]), "cannot depend on itself"),
            (_workflow([_agent("a"), _agent("a")]), "duplicate node id"),
        ]
        for raw, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(WorkflowCompileError, message):
                    compile_workflow(raw)

    def test_rejects_unknown_kind_and_invalid_kind_config(self) -> None:
        invalid = [
            (
                _workflow(
                    [{"id": "x", "kind": "python", "depends_on": [], "config": {}}]
                ),
                "unsupported kind",
            ),
            (
                _workflow(
                    [{"id": "x", "kind": "tool", "depends_on": [], "config": {}}]
                ),
                "tool.*must not be empty",
            ),
            (
                _workflow(
                    [
                        {
                            "id": "x",
                            "kind": "approval",
                            "depends_on": [],
                            "config": {"prompt": ""},
                        }
                    ]
                ),
                "must not be empty",
            ),
        ]
        for raw, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(WorkflowCompileError, message):
                    compile_workflow(raw)

    def test_all_required_node_kinds_compile(self) -> None:
        compiled = compile_workflow(
            _workflow(
                [
                    _agent("agent"),
                    {
                        "id": "tool",
                        "kind": "tool",
                        "depends_on": ["agent"],
                        "config": {"tool": "file_read", "arguments": {"path": "x"}},
                        "effect_class": "read_only",
                    },
                    {
                        "id": "router",
                        "kind": "router",
                        "depends_on": ["tool"],
                        "config": {"routes": {"yes": "approval"}},
                    },
                    {
                        "id": "approval",
                        "kind": "approval",
                        "depends_on": ["router"],
                        "config": {"prompt": "continue?"},
                    },
                    {
                        "id": "sub",
                        "kind": "subworkflow",
                        "depends_on": ["approval"],
                        "config": {"workflow_id": "child"},
                    },
                    {
                        "id": "join",
                        "kind": "join",
                        "depends_on": ["sub"],
                        "config": {},
                    },
                    {
                        "id": "parallel",
                        "kind": "parallel",
                        "depends_on": ["join"],
                        "config": {"branches": ["branch"]},
                    },
                    _agent("branch", ["parallel"]),
                    {
                        "id": "map",
                        "kind": "map",
                        "depends_on": ["join"],
                        "config": {
                            "items": "{{input.items}}",
                            "body": "mapped",
                            "item_name": "work_item",
                            "max_concurrency": 4,
                        },
                    },
                    _agent("mapped", ["map"]),
                ]
            )
        )

        self.assertEqual(
            {node.kind for node in compiled.nodes},
            {
                "agent",
                "tool",
                "router",
                "parallel",
                "join",
                "map",
                "approval",
                "subworkflow",
            },
        )

    def test_control_targets_must_be_direct_dependents(self) -> None:
        cases = [
            (
                [
                    {
                        "id": "control",
                        "kind": "router",
                        "depends_on": [],
                        "config": {"routes": {"yes": "target"}},
                    },
                    _agent("target"),
                ],
                "router",
            ),
            (
                [
                    {
                        "id": "control",
                        "kind": "parallel",
                        "depends_on": [],
                        "config": {"branches": ["target"]},
                    },
                    _agent("target"),
                ],
                "parallel",
            ),
            (
                [
                    {
                        "id": "control",
                        "kind": "map",
                        "depends_on": [],
                        "config": {"items": [1, 2], "body": "target"},
                    },
                    _agent("target"),
                ],
                "map",
            ),
        ]
        for nodes, kind in cases:
            with self.subTest(kind=kind):
                with self.assertRaisesRegex(
                    WorkflowCompileError,
                    rf"{kind}.*targets must directly depend",
                ):
                    compile_workflow(_workflow(nodes))

    def test_parallel_branch_order_does_not_change_digest(self) -> None:
        common = [_agent("a", ["fork"]), _agent("b", ["fork"])]
        first = compile_workflow(
            _workflow(
                [
                    {
                        "id": "fork",
                        "kind": "parallel",
                        "depends_on": [],
                        "config": {"branches": ["b", "a"]},
                    },
                    *common,
                ]
            )
        )
        second = compile_workflow(
            _workflow(
                [
                    *reversed(common),
                    {
                        "id": "fork",
                        "kind": "parallel",
                        "depends_on": [],
                        "config": {"branches": ["a", "b"]},
                    },
                ]
            )
        )

        self.assertEqual(first.definition_digest, second.definition_digest)
        self.assertEqual(first.get_node("fork").config["branches"], ("a", "b"))

    def test_retry_timeout_resources_and_idempotency_are_typed(self) -> None:
        compiled = compile_workflow(
            _workflow(
                [
                    _agent(
                        "write",
                        retry={
                            "max_attempts": 3,
                            "retry_on": ["transient", "conflict"],
                            "initial_delay_ms": 10,
                            "max_delay_ms": 100,
                            "backoff_multiplier": 2,
                            "jitter": 0.2,
                        },
                        timeout={
                            "execution_timeout_ms": 5000,
                            "heartbeat_timeout_ms": 1000,
                        },
                        resource_keys=["workspace:main", "file:a"],
                        concurrency_key="workspace:main",
                        effect_class="idempotent_write",
                        idempotency_key_template="{{run_id}}:{{node_id}}",
                    )
                ]
            )
        )
        node = compiled.get_node("write")

        self.assertEqual(node.retry_policy.max_attempts, 3)
        self.assertEqual(node.retry_policy.retry_on, ("conflict", "transient"))
        self.assertEqual(node.timeout_policy.execution_timeout_ms, 5000)
        self.assertEqual(node.resource_keys, ("file:a", "workspace:main"))
        self.assertEqual(node.effect_class, "idempotent_write")

    def test_invalid_retry_timeout_and_idempotency_fail_closed(self) -> None:
        invalid_nodes = [
            _agent("a", retry={"max_attempts": 2, "retry_on": []}),
            _agent(
                "a",
                timeout={
                    "execution_timeout_ms": 100,
                    "heartbeat_timeout_ms": 200,
                },
            ),
            _agent("a", effect_class="idempotent_write"),
            _agent(
                "a",
                effect_class="idempotent_write",
                idempotency_key_template="{{secret}}",
            ),
            _agent(
                "a",
                effect_class="idempotent_write",
                idempotency_key_template="{{workflow_id}}",
            ),
            _agent("a", resource_keys=["../escape"]),
            {
                "id": "a",
                "kind": "parallel",
                "depends_on": [],
                "config": {"branches": []},
            },
            {
                "id": "a",
                "kind": "map",
                "depends_on": [],
                "config": {"items": {}, "body": "b"},
            },
        ]
        for node in invalid_nodes:
            with self.subTest(node=node):
                with self.assertRaises(WorkflowCompileError):
                    compile_workflow(_workflow([node]))

    def test_metadata_is_bounded_json(self) -> None:
        compiled = compile_workflow(
            {
                **_workflow([_agent("a")]),
                "metadata": {"large": "x" * 70_000},
            }
        )
        self.assertEqual(len(compiled.metadata["large"]), 70_000)

        with self.assertRaisesRegex(WorkflowCompileError, "exceeds"):
            compile_workflow(
                {
                    **_workflow([_agent("a")]),
                    "metadata": {"large": "x" * (MAX_WORKFLOW_BYTES + 1)},
                }
            )
        with self.assertRaises(WorkflowCompileError):
            compile_workflow(
                {
                    **_workflow([_agent("a")]),
                    "metadata": {"bad": object()},
                }
            )

    def test_compiled_definition_is_immutable(self) -> None:
        compiled = compile_workflow(_workflow([_agent("a")]))

        with self.assertRaises(FrozenInstanceError):
            compiled.name = "changed"  # type: ignore[misc]
        with self.assertRaises(TypeError):
            compiled.get_node("a").config["agent"] = "changed"  # type: ignore[index]

    def test_v1_compatibility_preserves_explicit_dependencies(self) -> None:
        compiled = compile_team_workflow_v1(
            {
                "name": "legacy",
                "version": 1,
                "steps": [
                    {"id": "first", "agent": "main", "task": "first"},
                    {
                        "id": "second",
                        "agent": "coding",
                        "task": "second",
                        "depends_on": ["first"],
                    },
                    {
                        "id": "third",
                        "agent": "main",
                        "task": "third",
                        "depends_on": ["first"],
                        "on_error": "continue",
                    },
                ],
            },
            team_name="dev-team",
        )

        self.assertEqual(compiled.get_node("second").depends_on, ("first",))
        self.assertEqual(compiled.get_node("third").depends_on, ("first",))
        self.assertEqual(compiled.get_node("third").on_error, "continue")
        self.assertEqual(compiled.metadata["source_schema"], "team_workflow_v1")

    def test_v1_steps_without_dependencies_are_serialized_in_file_order(self) -> None:
        compiled = compile_team_workflow_v1(
            {
                "steps": [
                    {"id": "one", "agent": "main", "task": "one"},
                    {"id": "two", "agent": "main", "task": "two"},
                    {"id": "three", "agent": "main", "task": "three"},
                ]
            }
        )

        self.assertEqual(compiled.topological_order, ("one", "two", "three"))
        self.assertEqual(compiled.get_node("two").depends_on, ("one",))
        self.assertEqual(compiled.get_node("three").depends_on, ("two",))


if __name__ == "__main__":
    unittest.main()
