from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.orchestration.artifacts import ArtifactKind, ArtifactRef, LocalArtifactStore
from src.orchestration.scheduler import (
    ActivityReceipt,
    DurableScheduler,
    InputMappingError,
    RunInputReceipt,
)
from src.orchestration.store import DurableRunStore
from src.orchestration.workflow import WorkflowCompileError, compile_workflow


def _agent(
    node_id: str,
    depends_on: tuple[str, ...] = (),
    *,
    input_mapping: dict | None = None,
) -> dict:
    node = {
        "id": node_id,
        "kind": "agent",
        "depends_on": list(depends_on),
        "config": {"agent": "main", "task": f"run {node_id}"},
        "effect_class": "read_only",
    }
    if input_mapping is not None:
        node["input_mapping"] = input_mapping
    return node


def _workflow(nodes: list[dict]):
    return compile_workflow(
        {
            "schema_version": 2,
            "name": "dataflow-test",
            "version": 1,
            "nodes": nodes,
        }
    )


class WorkflowDataflowCompilerTests(unittest.TestCase):
    def test_mapping_is_canonical_and_binds_definition_digest(self) -> None:
        first = _workflow(
            [
                _agent("source"),
                _agent(
                    "consumer",
                    ("source",),
                    input_mapping={
                        "zeta": {
                            "source": "node_output",
                            "node_id": "source",
                        },
                        "alpha": {"source": "run_input"},
                    },
                ),
            ]
        )
        reordered = _workflow(
            [
                _agent(
                    "consumer",
                    ("source",),
                    input_mapping={
                        "alpha": {"source": "run_input"},
                        "zeta": {
                            "node_id": "source",
                            "source": "node_output",
                        },
                    },
                ),
                _agent("source"),
            ]
        )
        changed = _workflow(
            [
                _agent("source"),
                _agent(
                    "consumer",
                    ("source",),
                    input_mapping={"alpha": {"source": "run_input"}},
                ),
            ]
        )

        self.assertEqual(first.definition_digest, reordered.definition_digest)
        self.assertNotEqual(first.definition_digest, changed.definition_digest)
        self.assertEqual(
            tuple(first.get_node("consumer").input_mapping),
            ("alpha", "zeta"),
        )

    def test_empty_mapping_preserves_legacy_definition_digest(self) -> None:
        implicit = _workflow([_agent("only")])
        explicit = _workflow([_agent("only", input_mapping={})])

        self.assertEqual(implicit.definition_digest, explicit.definition_digest)
        self.assertNotIn(
            "input_mapping",
            implicit.get_node("only").to_dict(),
        )

    def test_rejects_sources_outside_dependency_closure(self) -> None:
        with self.assertRaisesRegex(
            WorkflowCompileError,
            "outside its dependency closure",
        ):
            _workflow(
                [
                    _agent("allowed"),
                    _agent("unrelated"),
                    _agent(
                        "consumer",
                        ("allowed",),
                        input_mapping={
                            "leak": {
                                "source": "node_output",
                                "node_id": "unrelated",
                            }
                        },
                    ),
                ]
            )

    def test_accepts_transitive_ancestor_output(self) -> None:
        compiled = _workflow(
            [
                _agent("root"),
                _agent("middle", ("root",)),
                _agent(
                    "consumer",
                    ("middle",),
                    input_mapping={
                        "original": {
                            "source": "node_output",
                            "node_id": "root",
                        }
                    },
                ),
            ]
        )

        self.assertEqual(
            compiled.get_node("consumer").input_mapping["original"]["node_id"],
            "root",
        )

    def test_rejects_unknown_nodes_and_selector_fields(self) -> None:
        cases = (
            (
                {"value": {"source": "node_output", "node_id": "missing"}},
                "unknown node output",
            ),
            (
                {"value": {"source": "run_input", "path": "secret"}},
                "unknown fields",
            ),
            (
                {"value": {"source": "run_input", "node_id": "source"}},
                "must not declare node_id",
            ),
        )
        for mapping, message in cases:
            with self.subTest(mapping=mapping):
                with self.assertRaisesRegex(WorkflowCompileError, message):
                    _workflow(
                        [
                            _agent("source"),
                            _agent(
                                "consumer",
                                ("source",),
                                input_mapping=mapping,
                            ),
                        ]
                    )


class SchedulerDataflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.artifacts = LocalArtifactStore(self.root / "artifacts")
        self.store = DurableRunStore(self.root / "orchestration.sqlite3")
        self.next_id = 0

    def _id(self, prefix: str) -> str:
        self.next_id += 1
        return f"{prefix}-dataflow-{self.next_id}"

    def _result_writer(self, claim, result) -> ActivityReceipt:
        ref = self.artifacts.put_json(
            result,
            kind=ArtifactKind.TOOL_RESULT,
            producer_run_id=claim.run_id,
            producer_node_id=claim.node_id,
            producer_attempt_id=claim.attempt_id,
        )
        return ActivityReceipt((ref,))

    def _scheduler(self, workflow) -> DurableScheduler:
        return DurableScheduler(
            self.store,
            workflow,
            id_factory=self._id,
            result_writer=self._result_writer,
            artifact_verifier=self.artifacts.verify,
        )

    def _run_input(self, run_id: str, value: dict) -> RunInputReceipt:
        ref = self.artifacts.put_json(
            value,
            producer_run_id=run_id,
        )
        return RunInputReceipt((ref,))

    def test_end_to_end_join_and_claim_inputs_are_deterministic(self) -> None:
        workflow = _workflow(
            [
                _agent(
                    "alpha",
                    input_mapping={"source": {"source": "run_input"}},
                ),
                _agent(
                    "beta",
                    input_mapping={"source": {"source": "run_input"}},
                ),
                {
                    "id": "join",
                    "kind": "join",
                    "depends_on": ["alpha", "beta"],
                    "input_mapping": {
                        "zeta_alpha": {
                            "source": "node_output",
                            "node_id": "alpha",
                        },
                        "alpha_beta": {
                            "source": "node_output",
                            "node_id": "beta",
                        },
                    },
                    "config": {"mode": "all"},
                },
                _agent(
                    "publish",
                    ("join",),
                    input_mapping={
                        "fragments": {
                            "source": "node_output",
                            "node_id": "join",
                        }
                    },
                ),
            ]
        )
        scheduler = self._scheduler(workflow)
        input_receipt = self._run_input("run-e2e", {"ticket": 7})
        scheduler.create_run("run-e2e", input=input_receipt)

        alpha = scheduler.claim_next("run-e2e", "worker-alpha")
        beta = scheduler.claim_next("run-e2e", "worker-beta")
        self.assertIsNotNone(alpha)
        self.assertIsNotNone(beta)
        assert alpha is not None and beta is not None
        self.assertEqual(alpha.input_artifact_refs, input_receipt.artifact_refs)
        self.assertEqual(
            alpha.input_artifact_bindings,
            (("source", input_receipt.artifact_refs),),
        )

        claims = {alpha.node_id: alpha, beta.node_id: beta}
        for node_id in ("beta", "alpha"):
            claim = claims[node_id]
            scheduler.start_claim(claim)
            scheduler.complete_claim(claim, {"branch": node_id})

        beta_node = self.store.get_node("run-e2e", "beta")
        alpha_node = self.store.get_node("run-e2e", "alpha")
        assert beta_node is not None and alpha_node is not None
        beta_ref = self._refs_from_output(beta_node.output)[0]
        alpha_ref = self._refs_from_output(alpha_node.output)[0]
        publish = scheduler.claim_next("run-e2e", "worker-publish")
        self.assertIsNotNone(publish)
        assert publish is not None
        self.assertEqual(publish.node_id, "publish")
        self.assertEqual(
            publish.input_artifact_refs,
            (beta_ref, alpha_ref),
        )
        self.assertEqual(
            publish.input_artifact_bindings,
            (("fragments", (beta_ref, alpha_ref)),),
        )

    def test_request_hash_binds_resolved_artifact_identity(self) -> None:
        workflow = _workflow(
            [
                _agent(
                    "work",
                    input_mapping={"source": {"source": "run_input"}},
                )
            ]
        )
        scheduler = self._scheduler(workflow)
        scheduler.create_run(
            "run-hash-a",
            input=self._run_input("run-hash-a", {"value": "a"}),
        )
        scheduler.create_run(
            "run-hash-b",
            input=self._run_input("run-hash-b", {"value": "b"}),
        )

        claim_a = scheduler.claim_next("run-hash-a", "worker-a")
        claim_b = scheduler.claim_next("run-hash-b", "worker-b")
        assert claim_a is not None and claim_b is not None
        self.assertNotEqual(claim_a.request_hash, claim_b.request_hash)

    def test_request_hash_is_deterministic_for_reordered_mapping(self) -> None:
        first = _workflow(
            [
                _agent(
                    "work",
                    input_mapping={
                        "zeta": {"source": "run_input"},
                        "alpha": {
                            "source": "run_input",
                            "artifact_index": 0,
                        },
                    },
                )
            ]
        )
        second = _workflow(
            [
                _agent(
                    "work",
                    input_mapping={
                        "alpha": {
                            "artifact_index": 0,
                            "source": "run_input",
                        },
                        "zeta": {"source": "run_input"},
                    },
                )
            ]
        )
        ref = self.artifacts.put_json(
            {"stable": True},
            producer_run_id="run-stable",
        )
        receipt = RunInputReceipt((ref,))
        first_scheduler = self._scheduler(first)
        second_scheduler = DurableScheduler(
            DurableRunStore(self.root / "second.sqlite3"),
            second,
            id_factory=self._id,
            result_writer=self._result_writer,
            artifact_verifier=self.artifacts.verify,
        )
        first_scheduler.create_run("run-stable", input=receipt)
        second_scheduler.create_run("run-stable", input=receipt)

        first_claim = first_scheduler.claim_next("run-stable", "worker")
        second_claim = second_scheduler.claim_next("run-stable", "worker")
        assert first_claim is not None and second_claim is not None
        self.assertEqual(first.definition_digest, second.definition_digest)
        self.assertEqual(first_claim.request_hash, second_claim.request_hash)

    def test_artifact_index_out_of_range_fails_before_attempt_schedule(self) -> None:
        workflow = _workflow(
            [
                _agent("source"),
                _agent(
                    "consumer",
                    ("source",),
                    input_mapping={
                        "selected": {
                            "source": "node_output",
                            "node_id": "source",
                            "artifact_index": 1,
                        }
                    },
                ),
            ]
        )
        scheduler = self._scheduler(workflow)
        scheduler.create_run("run-index")
        source = scheduler.claim_next("run-index", "worker-source")
        assert source is not None
        scheduler.start_claim(source)
        scheduler.complete_claim(source, {"only": "one"})

        with self.assertRaisesRegex(InputMappingError, "out of range"):
            scheduler.claim_next("run-index", "worker-consumer")
        self.assertEqual(
            self.store.list_attempts("run-index", node_id="consumer"),
            [],
        )

    def test_missing_upstream_artifact_fails_closed(self) -> None:
        workflow = _workflow(
            [
                _agent("source"),
                _agent(
                    "consumer",
                    ("source",),
                    input_mapping={
                        "source": {
                            "source": "node_output",
                            "node_id": "source",
                        }
                    },
                ),
            ]
        )
        scheduler = self._scheduler(workflow)
        scheduler.create_run("run-missing")
        source = scheduler.claim_next("run-missing", "worker-source")
        assert source is not None
        scheduler.start_claim(source)
        scheduler.complete_claim(source, {"durable": True})
        source_node = self.store.get_node("run-missing", "source")
        assert source_node is not None
        source_ref = self._refs_from_output(source_node.output)[0]
        (self.artifacts.root / source_ref.uri).unlink()

        with self.assertRaisesRegex(InputMappingError, "verification failed"):
            scheduler.claim_next("run-missing", "worker-consumer")
        self.assertEqual(
            self.store.list_attempts("run-missing", node_id="consumer"),
            [],
        )

    @staticmethod
    def _refs_from_output(output) -> tuple[ArtifactRef, ...]:
        return tuple(
            ArtifactRef.from_dict(value)
            for value in output["artifact_refs"]
        )


if __name__ == "__main__":
    unittest.main()
