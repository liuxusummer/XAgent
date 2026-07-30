"""Pure, deterministic compiler for declarative orchestration workflows."""

from __future__ import annotations

import hashlib
import heapq
import json
import math
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, TypeAlias, Union

from .runtime_compatibility import (
    RuntimeCompatibility,
    RuntimeCompatibilityValidationError,
)

WORKFLOW_SCHEMA_VERSION = 2
MAX_WORKFLOW_BYTES = 1024 * 1024
MAX_NODES = 1000
MAX_TEXT = 4096
MAX_TEMPLATE = 512
MAX_RESOURCES = 64
MAX_INPUT_MAPPINGS = 64
MAX_TIMEOUT_MS = 30 * 24 * 60 * 60 * 1000

ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$")
RESOURCE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$")
TEMPLATE_RE = re.compile(r"{{\s*([A-Za-z0-9_.-]+)\s*}}")

NODE_KINDS = frozenset(
    {
        "agent",
        "tool",
        "router",
        "parallel",
        "join",
        "map",
        "approval",
        "subworkflow",
    }
)
RUNTIME_COMPATIBLE_NODE_KINDS = frozenset({"agent", "tool"})
EFFECT_CLASSES = frozenset(
    {"read_only", "idempotent_write", "non_idempotent_write", "destructive"}
)
ON_ERROR_VALUES = frozenset({"fail_run", "continue", "skip_dependents"})
RETRY_CLASSES = frozenset({"transient", "rate_limited", "conflict", "timeout"})
IDEMPOTENCY_TEMPLATE_FIELDS = frozenset(
    {"workflow_id", "workflow_version", "run_id", "node_id", "logical_operation_key", "input_digest"}
)

_TOP_LEVEL_KEYS = frozenset({"schema_version", "name", "version", "nodes", "metadata"})
_NODE_KEYS = frozenset(
    {
        "id",
        "kind",
        "depends_on",
        "input_mapping",
        "config",
        "metadata",
        "resource_keys",
        "concurrency_key",
        "retry",
        "timeout",
        "effect_class",
        "idempotency_key_template",
        "on_error",
    }
)
_RETRY_KEYS = frozenset(
    {
        "max_attempts",
        "retry_on",
        "initial_delay_ms",
        "max_delay_ms",
        "backoff_multiplier",
        "jitter",
        "max_elapsed_ms",
    }
)
_TIMEOUT_KEYS = frozenset(
    {
        "schedule_timeout_ms",
        "start_timeout_ms",
        "execution_timeout_ms",
        "heartbeat_timeout_ms",
    }
)
_CONFIG_KEYS = {
    "agent": frozenset({"agent", "task", "context", "expected_output", "output"}),
    "tool": frozenset({"tool", "arguments"}),
    "router": frozenset({"routes", "default_route"}),
    "parallel": frozenset({"branches"}),
    "join": frozenset({"mode"}),
    "map": frozenset({"items", "body", "item_name", "max_concurrency"}),
    "approval": frozenset({"prompt", "risk", "options"}),
    "subworkflow": frozenset({"workflow_id", "workflow_version", "input"}),
}


class WorkflowCompileError(ValueError):
    """The declarative workflow is invalid or ambiguous."""


FrozenJson: TypeAlias = Union[
    None,
    bool,
    int,
    float,
    str,
    tuple["FrozenJson", ...],
    "FrozenDict",
]


@dataclass(frozen=True, slots=True)
class FrozenDict(Mapping[str, FrozenJson]):
    """Small immutable mapping used by compiled definitions."""

    _items: tuple[tuple[str, FrozenJson], ...] = ()

    def __getitem__(self, key: str) -> FrozenJson:
        for item_key, value in self._items:
            if item_key == key:
                return value
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return (key for key, _value in self._items)

    def __len__(self) -> int:
        return len(self._items)

    def to_dict(self) -> dict[str, Any]:
        return {key: _thaw(value) for key, value in self._items}


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 1
    retry_on: tuple[str, ...] = ()
    initial_delay_ms: int = 0
    max_delay_ms: int = 0
    backoff_multiplier: float = 1.0
    jitter: float = 0.0
    max_elapsed_ms: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_attempts": self.max_attempts,
            "retry_on": list(self.retry_on),
            "initial_delay_ms": self.initial_delay_ms,
            "max_delay_ms": self.max_delay_ms,
            "backoff_multiplier": self.backoff_multiplier,
            "jitter": self.jitter,
            "max_elapsed_ms": self.max_elapsed_ms,
        }


@dataclass(frozen=True, slots=True)
class TimeoutPolicy:
    schedule_timeout_ms: int | None = None
    start_timeout_ms: int | None = None
    execution_timeout_ms: int | None = None
    heartbeat_timeout_ms: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schedule_timeout_ms": self.schedule_timeout_ms,
            "start_timeout_ms": self.start_timeout_ms,
            "execution_timeout_ms": self.execution_timeout_ms,
            "heartbeat_timeout_ms": self.heartbeat_timeout_ms,
        }


@dataclass(frozen=True, slots=True)
class NodeDefinition:
    node_id: str
    kind: str
    depends_on: tuple[str, ...]
    input_mapping: FrozenDict
    config: FrozenDict
    metadata: FrozenDict
    runtime_compatibility: RuntimeCompatibility | None
    resource_keys: tuple[str, ...]
    concurrency_key: str | None
    retry_policy: RetryPolicy
    timeout_policy: TimeoutPolicy
    effect_class: str
    idempotency_key_template: str | None
    on_error: str

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "id": self.node_id,
            "kind": self.kind,
            "depends_on": list(self.depends_on),
            "config": self.config.to_dict(),
            "metadata": self.metadata.to_dict(),
            "resource_keys": list(self.resource_keys),
            "concurrency_key": self.concurrency_key,
            "retry": self.retry_policy.to_dict(),
            "timeout": self.timeout_policy.to_dict(),
            "effect_class": self.effect_class,
            "idempotency_key_template": self.idempotency_key_template,
            "on_error": self.on_error,
        }
        if self.input_mapping:
            payload["input_mapping"] = self.input_mapping.to_dict()
        return payload


@dataclass(frozen=True, slots=True)
class CompiledWorkflow:
    schema_version: int
    name: str
    version: int
    nodes: tuple[NodeDefinition, ...]
    metadata: FrozenDict
    definition_digest: str
    topological_order: tuple[str, ...]
    layers: tuple[tuple[str, ...], ...]
    dependents: FrozenDict
    roots: tuple[str, ...]
    leaves: tuple[str, ...]
    _nodes_by_id: FrozenDict = field(repr=False, compare=False)

    def get_node(self, node_id: str) -> NodeDefinition:
        node = self._nodes_by_id[node_id]
        if not isinstance(node, NodeDefinition):
            raise KeyError(node_id)
        return node

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "name": self.name,
            "version": self.version,
            "nodes": [node.to_dict() for node in self.nodes],
            "metadata": self.metadata.to_dict(),
            "definition_digest": self.definition_digest,
            "topological_order": list(self.topological_order),
            "layers": [list(layer) for layer in self.layers],
            "dependents": self.dependents.to_dict(),
            "roots": list(self.roots),
            "leaves": list(self.leaves),
        }


def compile_workflow(raw: Mapping[str, Any]) -> CompiledWorkflow:
    """Validate and deterministically compile a Workflow v2 definition."""

    payload = _json_object(raw, "workflow")
    _reject_unknown_keys(payload, _TOP_LEVEL_KEYS, "workflow")
    if _positive_int(payload.get("schema_version"), "schema_version") != WORKFLOW_SCHEMA_VERSION:
        raise WorkflowCompileError(
            f"schema_version must be {WORKFLOW_SCHEMA_VERSION}"
        )
    name = _required_text(payload.get("name"), "name", max_length=160)
    version = _positive_int(payload.get("version"), "version")
    metadata = _frozen_object(payload.get("metadata", {}), "workflow metadata")
    raw_nodes = payload.get("nodes")
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise WorkflowCompileError("nodes must be a non-empty list")
    if len(raw_nodes) > MAX_NODES:
        raise WorkflowCompileError(f"nodes exceeds limit {MAX_NODES}")

    nodes_by_id: dict[str, NodeDefinition] = {}
    for index, item in enumerate(raw_nodes):
        node = _compile_node(item, index)
        if node.node_id in nodes_by_id:
            raise WorkflowCompileError(f"duplicate node id: {node.node_id}")
        nodes_by_id[node.node_id] = node

    _validate_dependencies(nodes_by_id)
    dependents = _build_dependents(nodes_by_id)
    layers = _topological_layers(nodes_by_id, dependents)
    _validate_input_dependency_closure(nodes_by_id)
    order = tuple(node_id for layer in layers for node_id in layer)
    roots = tuple(sorted(node_id for node_id, node in nodes_by_id.items() if not node.depends_on))
    leaves = tuple(sorted(node_id for node_id, values in dependents.items() if not values))

    canonical_nodes = [nodes_by_id[node_id].to_dict() for node_id in sorted(nodes_by_id)]
    canonical = {
        "schema_version": WORKFLOW_SCHEMA_VERSION,
        "name": name,
        "version": version,
        "nodes": canonical_nodes,
        "metadata": metadata.to_dict(),
    }
    digest = hashlib.sha256(_canonical_json(canonical)).hexdigest()
    compiled_nodes = tuple(nodes_by_id[node_id] for node_id in order)
    frozen_dependents = FrozenDict(
        tuple((node_id, tuple(dependents[node_id])) for node_id in sorted(dependents))
    )
    node_lookup = FrozenDict(
        tuple((node_id, nodes_by_id[node_id]) for node_id in sorted(nodes_by_id))
    )
    return CompiledWorkflow(
        schema_version=WORKFLOW_SCHEMA_VERSION,
        name=name,
        version=version,
        nodes=compiled_nodes,
        metadata=metadata,
        definition_digest=digest,
        topological_order=order,
        layers=layers,
        dependents=frozen_dependents,
        roots=roots,
        leaves=leaves,
        _nodes_by_id=node_lookup,
    )


def compile_team_workflow_v1(
    raw: Mapping[str, Any],
    *,
    team_name: str = "team",
) -> CompiledWorkflow:
    """Compile the legacy serial team step list into an equivalent DAG."""

    payload = _json_object(raw, "team workflow")
    allowed = {"name", "version", "description", "steps"}
    _reject_unknown_keys(payload, allowed, "team workflow")
    steps = payload.get("steps")
    if not isinstance(steps, list) or not steps:
        raise WorkflowCompileError("legacy workflow steps must be a non-empty list")
    seen_ids: set[str] = set()
    v2_nodes: list[dict[str, Any]] = []
    previous_id: str | None = None
    for index, raw_step in enumerate(steps):
        if not isinstance(raw_step, dict):
            raise WorkflowCompileError(f"legacy step #{index + 1} must be an object")
        allowed_step = {
            "id",
            "agent",
            "task",
            "depends_on",
            "dependsOn",
            "context",
            "expected_output",
            "expectedOutput",
            "output",
            "on_error",
            "onError",
            "max_turns",
            "maxTurns",
        }
        _reject_unknown_keys(raw_step, allowed_step, f"legacy step #{index + 1}")
        step_id = _valid_id(raw_step.get("id"), f"legacy step #{index + 1} id")
        if step_id in seen_ids:
            raise WorkflowCompileError(f"duplicate node id: {step_id}")
        agent = _valid_id(raw_step.get("agent"), f"legacy step {step_id} agent")
        task = _required_text(raw_step.get("task"), f"legacy step {step_id} task")
        dependency_value = raw_step.get("depends_on", raw_step.get("dependsOn"))
        explicit_dependencies = _string_tuple(
            dependency_value if dependency_value is not None else [],
            f"legacy step {step_id} depends_on",
            validator=_valid_id,
        )
        dependencies = explicit_dependencies
        if not dependencies and previous_id is not None:
            dependencies = (previous_id,)
        raw_on_error = str(
            raw_step.get("on_error", raw_step.get("onError", "stop")) or "stop"
        )
        if raw_on_error not in {"stop", "continue"}:
            raise WorkflowCompileError(
                f"legacy step {step_id} on_error must be stop or continue"
            )
        config: dict[str, Any] = {
            "agent": agent,
            "task": task,
        }
        for target, snake, camel in (
            ("context", "context", "context"),
            ("expected_output", "expected_output", "expectedOutput"),
            ("output", "output", "output"),
        ):
            value = raw_step.get(snake, raw_step.get(camel))
            if value not in (None, ""):
                config[target] = _required_text(
                    value,
                    f"legacy step {step_id} {target}",
                )
        max_turns = raw_step.get("max_turns", raw_step.get("maxTurns"))
        if max_turns is not None:
            config["max_turns"] = _positive_int(
                max_turns,
                f"legacy step {step_id} max_turns",
            )
        # max_turns is a legacy Agent config extension, admitted only by this
        # adapter and stripped before the strict v2 compiler.
        max_turns_value = config.pop("max_turns", None)
        metadata = {"source_schema": "team_workflow_v1"}
        if max_turns_value is not None:
            metadata["max_turns"] = max_turns_value
        v2_nodes.append(
            {
                "id": step_id,
                "kind": "agent",
                "depends_on": list(dependencies),
                "config": config,
                "metadata": metadata,
                "effect_class": "non_idempotent_write",
                "on_error": "continue" if raw_on_error == "continue" else "fail_run",
            }
        )
        seen_ids.add(step_id)
        previous_id = step_id

    name = str(payload.get("name") or f"{team_name}-workflow").strip()
    metadata: dict[str, Any] = {
        "source_schema": "team_workflow_v1",
        "team_name": _required_text(team_name, "team_name", max_length=160),
    }
    description = payload.get("description")
    if description not in (None, ""):
        metadata["description"] = _required_text(
            description,
            "description",
            max_length=MAX_TEXT,
        )
    return compile_workflow(
        {
            "schema_version": WORKFLOW_SCHEMA_VERSION,
            "name": _required_text(name, "name", max_length=160),
            "version": _positive_int(payload.get("version", 1), "version"),
            "nodes": v2_nodes,
            "metadata": metadata,
        }
    )


def _compile_node(raw: Any, index: int) -> NodeDefinition:
    if not isinstance(raw, dict):
        raise WorkflowCompileError(f"node #{index + 1} must be an object")
    _reject_unknown_keys(raw, _NODE_KEYS, f"node #{index + 1}")
    node_id = _valid_id(raw.get("id"), f"node #{index + 1} id")
    kind = _required_text(raw.get("kind"), f"node {node_id} kind", max_length=32)
    if kind not in NODE_KINDS:
        raise WorkflowCompileError(f"node {node_id} has unsupported kind: {kind}")
    depends_on = _string_tuple(
        raw.get("depends_on", []),
        f"node {node_id} depends_on",
        validator=_valid_id,
    )
    input_mapping = _compile_input_mapping(
        raw.get("input_mapping", {}),
        node_id,
    )
    config = _compile_config(kind, raw.get("config", {}), node_id)
    metadata = _frozen_object(raw.get("metadata", {}), f"node {node_id} metadata")
    runtime_compatibility = _compile_runtime_compatibility(metadata, node_id)
    if (
        runtime_compatibility is not None
        and kind not in RUNTIME_COMPATIBLE_NODE_KINDS
    ):
        raise WorkflowCompileError(
            f"node {node_id} runtime compatibility is only valid for Activities"
        )
    resources = _string_tuple(
        raw.get("resource_keys", []),
        f"node {node_id} resource_keys",
        validator=_valid_resource,
        maximum=MAX_RESOURCES,
        sort_values=True,
    )
    concurrency_key = raw.get("concurrency_key")
    if concurrency_key is not None:
        concurrency_key = _valid_resource(
            concurrency_key,
            f"node {node_id} concurrency_key",
        )
    retry = _compile_retry(raw.get("retry", {}), node_id)
    timeout = _compile_timeout(raw.get("timeout", {}), node_id)
    effect_class = str(raw.get("effect_class") or _default_effect_class(kind))
    if effect_class not in EFFECT_CLASSES:
        raise WorkflowCompileError(
            f"node {node_id} has invalid effect_class: {effect_class}"
        )
    idempotency_template = raw.get("idempotency_key_template")
    if idempotency_template is not None:
        idempotency_template = _validate_idempotency_template(
            idempotency_template,
            node_id,
        )
    if effect_class == "idempotent_write" and not idempotency_template:
        raise WorkflowCompileError(
            f"node {node_id} idempotent_write requires idempotency_key_template"
        )
    on_error = str(raw.get("on_error") or "fail_run")
    if on_error not in ON_ERROR_VALUES:
        raise WorkflowCompileError(f"node {node_id} has invalid on_error: {on_error}")
    return NodeDefinition(
        node_id=node_id,
        kind=kind,
        depends_on=tuple(sorted(depends_on)),
        input_mapping=input_mapping,
        config=config,
        metadata=metadata,
        runtime_compatibility=runtime_compatibility,
        resource_keys=resources,
        concurrency_key=concurrency_key,
        retry_policy=retry,
        timeout_policy=timeout,
        effect_class=effect_class,
        idempotency_key_template=idempotency_template,
        on_error=on_error,
    )


def _compile_runtime_compatibility(
    metadata: FrozenDict,
    node_id: str,
) -> RuntimeCompatibility | None:
    """Compile reserved Activity compatibility metadata without changing v2 JSON."""

    keys = {"min_runtime_version", "max_runtime_version"}
    if not keys.intersection(metadata):
        return None
    minimum = metadata.get("min_runtime_version", "0")
    maximum = metadata.get("max_runtime_version")
    if not isinstance(minimum, str) or (
        maximum is not None and not isinstance(maximum, str)
    ):
        raise WorkflowCompileError(
            f"node {node_id} has invalid runtime compatibility"
        )
    try:
        return RuntimeCompatibility(
            min_runtime_version=minimum,
            max_runtime_version=maximum,
        )
    except RuntimeCompatibilityValidationError as exc:
        raise WorkflowCompileError(
            f"node {node_id} has invalid runtime compatibility"
        ) from exc


def _compile_input_mapping(raw: Any, node_id: str) -> FrozenDict:
    if isinstance(raw, Mapping) and any(
        not isinstance(key, str) for key in raw
    ):
        raise WorkflowCompileError(
            f"node {node_id} input_mapping keys must be strings"
        )
    mapping = _json_object(raw, f"node {node_id} input_mapping")
    if len(mapping) > MAX_INPUT_MAPPINGS:
        raise WorkflowCompileError(
            f"node {node_id} input_mapping exceeds limit {MAX_INPUT_MAPPINGS}"
        )
    normalized: dict[str, Any] = {}
    for raw_name, raw_selector in mapping.items():
        name = _valid_id(raw_name, f"node {node_id} input name")
        selector = _json_object(
            raw_selector,
            f"node {node_id} input_mapping.{name}",
        )
        _reject_unknown_keys(
            selector,
            frozenset({"source", "node_id", "artifact_index"}),
            f"node {node_id} input_mapping.{name}",
        )
        source = _choice(
            selector.get("source"),
            f"node {node_id} input_mapping.{name}.source",
            frozenset({"run_input", "node_output"}),
        )
        normalized_selector: dict[str, Any] = {"source": source}
        if source == "node_output":
            normalized_selector["node_id"] = _valid_id(
                selector.get("node_id"),
                f"node {node_id} input_mapping.{name}.node_id",
            )
        elif "node_id" in selector:
            raise WorkflowCompileError(
                f"node {node_id} input_mapping.{name} run_input "
                "must not declare node_id"
            )
        if "artifact_index" in selector:
            normalized_selector["artifact_index"] = _bounded_int(
                selector["artifact_index"],
                f"node {node_id} input_mapping.{name}.artifact_index",
                minimum=0,
                maximum=63,
            )
        normalized[name] = normalized_selector
    return _freeze_object(normalized)


def _compile_config(kind: str, raw: Any, node_id: str) -> FrozenDict:
    config = _json_object(raw, f"node {node_id} config")
    _reject_unknown_keys(config, _CONFIG_KEYS[kind], f"node {node_id} config")
    if kind == "agent":
        config["agent"] = _valid_id(config.get("agent"), f"node {node_id} agent")
        for key in ("task", "context", "expected_output", "output"):
            if key in config:
                config[key] = _required_text(
                    config[key],
                    f"node {node_id} config.{key}",
                )
    elif kind == "tool":
        config["tool"] = _valid_id(config.get("tool"), f"node {node_id} tool")
        arguments = config.get("arguments", {})
        if not isinstance(arguments, dict):
            raise WorkflowCompileError(f"node {node_id} config.arguments must be an object")
        config["arguments"] = _json_object(
            arguments,
            f"node {node_id} config.arguments",
        )
    elif kind == "router":
        routes = config.get("routes")
        if not isinstance(routes, dict) or not routes:
            raise WorkflowCompileError(f"node {node_id} config.routes must be a non-empty object")
        normalized_routes: dict[str, str] = {}
        for route_name, target in routes.items():
            key = _required_text(route_name, f"node {node_id} route name", max_length=128)
            normalized_routes[key] = _valid_id(
                target,
                f"node {node_id} route {key}",
            )
        config["routes"] = normalized_routes
        if "default_route" in config:
            config["default_route"] = _valid_id(
                config["default_route"],
                f"node {node_id} default_route",
            )
    elif kind == "parallel":
        branches = _string_tuple(
            config.get("branches", []),
            f"node {node_id} parallel branches",
            validator=_valid_id,
        )
        if not branches:
            raise WorkflowCompileError(
                f"node {node_id} parallel branches must not be empty"
            )
        config["branches"] = list(sorted(branches))
    elif kind == "join":
        mode = str(config.get("mode") or "all")
        if mode not in {"all", "any"}:
            raise WorkflowCompileError(f"node {node_id} join mode must be all or any")
        config["mode"] = mode
    elif kind == "map":
        if "items" not in config:
            raise WorkflowCompileError(f"node {node_id} map items must not be empty")
        items = _json_value(config["items"], f"node {node_id} map items")
        if isinstance(items, str):
            items = _required_text(
                items,
                f"node {node_id} map items",
                max_length=MAX_TEMPLATE,
            )
        elif not isinstance(items, list):
            raise WorkflowCompileError(
                f"node {node_id} map items must be a list or template string"
            )
        config["items"] = items
        config["body"] = _valid_id(
            config.get("body"),
            f"node {node_id} map body",
        )
        config["item_name"] = _valid_id(
            config.get("item_name", "item"),
            f"node {node_id} map item_name",
        )
        config["max_concurrency"] = _bounded_int(
            config.get("max_concurrency", 1),
            f"node {node_id} map max_concurrency",
            minimum=1,
            maximum=100,
        )
    elif kind == "approval":
        config["prompt"] = _required_text(
            config.get("prompt"),
            f"node {node_id} approval prompt",
        )
        if "risk" in config:
            config["risk"] = _required_text(
                config["risk"],
                f"node {node_id} approval risk",
                max_length=128,
            )
        options = config.get("options", [])
        config["options"] = list(
            _string_tuple(
                options,
                f"node {node_id} approval options",
                validator=lambda value, name: _required_text(value, name, max_length=256),
                maximum=32,
            )
        )
    elif kind == "subworkflow":
        config["workflow_id"] = _valid_id(
            config.get("workflow_id"),
            f"node {node_id} workflow_id",
        )
        config["workflow_version"] = _positive_int(
            config.get("workflow_version", 1),
            f"node {node_id} workflow_version",
        )
        config["input"] = _json_object(
            config.get("input", {}),
            f"node {node_id} subworkflow input",
        )
    return _freeze_object(config)


def _compile_retry(raw: Any, node_id: str) -> RetryPolicy:
    payload = _json_object(raw, f"node {node_id} retry")
    _reject_unknown_keys(payload, _RETRY_KEYS, f"node {node_id} retry")
    max_attempts = _bounded_int(
        payload.get("max_attempts", 1),
        f"node {node_id} retry.max_attempts",
        minimum=1,
        maximum=100,
    )
    retry_on = _string_tuple(
        payload.get("retry_on", []),
        f"node {node_id} retry.retry_on",
        validator=lambda value, name: _choice(value, name, RETRY_CLASSES),
        maximum=len(RETRY_CLASSES),
        sort_values=True,
    )
    initial = _bounded_int(
        payload.get("initial_delay_ms", 0),
        f"node {node_id} retry.initial_delay_ms",
        minimum=0,
        maximum=MAX_TIMEOUT_MS,
    )
    maximum = _bounded_int(
        payload.get("max_delay_ms", initial),
        f"node {node_id} retry.max_delay_ms",
        minimum=0,
        maximum=MAX_TIMEOUT_MS,
    )
    if maximum < initial:
        raise WorkflowCompileError(
            f"node {node_id} retry.max_delay_ms must be >= initial_delay_ms"
        )
    multiplier = _finite_float(
        payload.get("backoff_multiplier", 1.0),
        f"node {node_id} retry.backoff_multiplier",
        minimum=1.0,
        maximum=100.0,
    )
    jitter = _finite_float(
        payload.get("jitter", 0.0),
        f"node {node_id} retry.jitter",
        minimum=0.0,
        maximum=1.0,
    )
    max_elapsed = payload.get("max_elapsed_ms")
    if max_elapsed is not None:
        max_elapsed = _bounded_int(
            max_elapsed,
            f"node {node_id} retry.max_elapsed_ms",
            minimum=1,
            maximum=MAX_TIMEOUT_MS,
        )
    if max_attempts > 1 and not retry_on:
        raise WorkflowCompileError(
            f"node {node_id} retries require non-empty retry_on"
        )
    return RetryPolicy(
        max_attempts=max_attempts,
        retry_on=retry_on,
        initial_delay_ms=initial,
        max_delay_ms=maximum,
        backoff_multiplier=multiplier,
        jitter=jitter,
        max_elapsed_ms=max_elapsed,
    )


def _compile_timeout(raw: Any, node_id: str) -> TimeoutPolicy:
    payload = _json_object(raw, f"node {node_id} timeout")
    _reject_unknown_keys(payload, _TIMEOUT_KEYS, f"node {node_id} timeout")
    values: dict[str, int | None] = {}
    for key in sorted(_TIMEOUT_KEYS):
        value = payload.get(key)
        values[key] = (
            None
            if value is None
            else _bounded_int(
                value,
                f"node {node_id} timeout.{key}",
                minimum=1,
                maximum=MAX_TIMEOUT_MS,
            )
        )
    heartbeat = values["heartbeat_timeout_ms"]
    execution = values["execution_timeout_ms"]
    if heartbeat is not None and execution is not None and heartbeat > execution:
        raise WorkflowCompileError(
            f"node {node_id} heartbeat_timeout_ms cannot exceed execution_timeout_ms"
        )
    return TimeoutPolicy(**values)


def _validate_dependencies(nodes: Mapping[str, NodeDefinition]) -> None:
    for node_id in sorted(nodes):
        node = nodes[node_id]
        if node_id in node.depends_on:
            raise WorkflowCompileError(f"node {node_id} cannot depend on itself")
        missing = sorted(dep for dep in node.depends_on if dep not in nodes)
        if missing:
            raise WorkflowCompileError(
                f"node {node_id} has unknown dependencies: {', '.join(missing)}"
            )
        if node.kind == "join" and not node.depends_on:
            raise WorkflowCompileError(f"join node {node_id} requires dependencies")
        if node.kind == "router":
            routes = node.config["routes"]
            assert isinstance(routes, FrozenDict)
            targets = list(routes.values())
            default = node.config.get("default_route")
            if default is not None:
                targets.append(default)
            _validate_direct_targets(
                node_id,
                "router",
                tuple(str(target) for target in targets),
                nodes,
            )
        elif node.kind == "parallel":
            branches = node.config["branches"]
            assert isinstance(branches, tuple)
            _validate_direct_targets(node_id, "parallel", branches, nodes)
        elif node.kind == "map":
            body = node.config["body"]
            assert isinstance(body, str)
            _validate_direct_targets(node_id, "map", (body,), nodes)


def _validate_input_dependency_closure(
    nodes: Mapping[str, NodeDefinition],
) -> None:
    for node_id in sorted(nodes):
        values: set[str] = set()
        pending = list(nodes[node_id].depends_on)
        while pending:
            dependency = pending.pop()
            if dependency in values:
                continue
            values.add(dependency)
            pending.extend(nodes[dependency].depends_on)
        for input_name, raw_selector in nodes[node_id].input_mapping.items():
            assert isinstance(raw_selector, FrozenDict)
            if raw_selector["source"] != "node_output":
                continue
            source_node_id = raw_selector["node_id"]
            assert isinstance(source_node_id, str)
            if source_node_id not in nodes:
                raise WorkflowCompileError(
                    f"node {node_id} input_mapping.{input_name} references "
                    f"unknown node output: {source_node_id}"
                )
            if source_node_id not in values:
                raise WorkflowCompileError(
                    f"node {node_id} input_mapping.{input_name} references "
                    f"node {source_node_id} outside its dependency closure"
                )


def _validate_direct_targets(
    node_id: str,
    kind: str,
    targets: tuple[str, ...],
    nodes: Mapping[str, NodeDefinition],
) -> None:
    missing = sorted(set(targets).difference(nodes))
    if missing:
        raise WorkflowCompileError(
            f"{kind} node {node_id} has unknown targets: {', '.join(missing)}"
        )
    if node_id in targets:
        raise WorkflowCompileError(f"{kind} node {node_id} cannot target itself")
    non_direct = sorted(
        target for target in set(targets) if node_id not in nodes[target].depends_on
    )
    if non_direct:
        raise WorkflowCompileError(
            f"{kind} node {node_id} targets must directly depend on it: "
            f"{', '.join(non_direct)}"
        )


def _build_dependents(
    nodes: Mapping[str, NodeDefinition],
) -> dict[str, tuple[str, ...]]:
    values: dict[str, list[str]] = {node_id: [] for node_id in nodes}
    for node_id, node in nodes.items():
        for dependency in node.depends_on:
            values[dependency].append(node_id)
    return {node_id: tuple(sorted(items)) for node_id, items in values.items()}


def _topological_layers(
    nodes: Mapping[str, NodeDefinition],
    dependents: Mapping[str, tuple[str, ...]],
) -> tuple[tuple[str, ...], ...]:
    indegree = {node_id: len(node.depends_on) for node_id, node in nodes.items()}
    ready = [node_id for node_id, count in indegree.items() if count == 0]
    heapq.heapify(ready)
    layers: list[tuple[str, ...]] = []
    visited = 0
    while ready:
        layer: list[str] = []
        while ready:
            layer.append(heapq.heappop(ready))
        next_ready: list[str] = []
        for node_id in layer:
            visited += 1
            for child in dependents[node_id]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    heapq.heappush(next_ready, child)
        layers.append(tuple(layer))
        ready = next_ready
    if visited != len(nodes):
        cycle = _stable_cycle(nodes, {node_id for node_id, count in indegree.items() if count > 0})
        raise WorkflowCompileError(f"workflow contains cycle: {' -> '.join(cycle)}")
    return tuple(layers)


def _stable_cycle(
    nodes: Mapping[str, NodeDefinition],
    candidates: set[str],
) -> tuple[str, ...]:
    state: dict[str, int] = {}
    stack: list[str] = []
    positions: dict[str, int] = {}

    def visit(node_id: str) -> tuple[str, ...] | None:
        state[node_id] = 1
        positions[node_id] = len(stack)
        stack.append(node_id)
        for dependency in sorted(nodes[node_id].depends_on):
            if dependency not in candidates:
                continue
            if state.get(dependency, 0) == 0:
                found = visit(dependency)
                if found:
                    return found
            elif state.get(dependency) == 1:
                cycle = stack[positions[dependency] :] + [dependency]
                return _canonical_cycle(cycle)
        stack.pop()
        positions.pop(node_id, None)
        state[node_id] = 2
        return None

    for node_id in sorted(candidates):
        if state.get(node_id, 0) == 0:
            found = visit(node_id)
            if found:
                return found
    return tuple(sorted(candidates))


def _canonical_cycle(cycle: list[str]) -> tuple[str, ...]:
    body = cycle[:-1]
    smallest = min(range(len(body)), key=lambda index: body[index])
    rotated = body[smallest:] + body[:smallest]
    return tuple(rotated + [rotated[0]])


def _validate_idempotency_template(value: Any, node_id: str) -> str:
    template = _required_text(
        value,
        f"node {node_id} idempotency_key_template",
        max_length=MAX_TEMPLATE,
    )
    fields = TEMPLATE_RE.findall(template)
    if not fields:
        raise WorkflowCompileError(
            f"node {node_id} idempotency_key_template requires placeholders"
        )
    unknown = sorted(set(fields).difference(IDEMPOTENCY_TEMPLATE_FIELDS))
    if unknown:
        raise WorkflowCompileError(
            f"node {node_id} idempotency template has unknown fields: {', '.join(unknown)}"
        )
    required = {"run_id", "node_id"}
    missing = sorted(required.difference(fields))
    if missing:
        raise WorkflowCompileError(
            f"node {node_id} idempotency template is missing stable fields: "
            f"{', '.join(missing)}"
        )
    residue = TEMPLATE_RE.sub("", template)
    if "{{" in residue or "}}" in residue:
        raise WorkflowCompileError(
            f"node {node_id} idempotency_key_template is malformed"
        )
    return template


def _default_effect_class(kind: str) -> str:
    return "read_only" if kind in {"router", "join", "approval"} else "non_idempotent_write"


def _frozen_object(value: Any, name: str) -> FrozenDict:
    return _freeze_object(_json_object(value, name))


def _freeze_object(value: Mapping[str, Any]) -> FrozenDict:
    return FrozenDict(
        tuple((str(key), _freeze(item)) for key, item in sorted(value.items()))
    )


def _freeze(value: Any) -> FrozenJson:
    if isinstance(value, dict):
        return _freeze_object(value)
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise WorkflowCompileError(f"unsupported frozen JSON value: {type(value).__name__}")


def _thaw(value: Any) -> Any:
    if isinstance(value, FrozenDict):
        return value.to_dict()
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    if isinstance(value, NodeDefinition):
        return value.to_dict()
    return value


def _json_object(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise WorkflowCompileError(f"{name} must be an object")
    normalized = _json_value(value, name)
    if not isinstance(normalized, dict):
        raise WorkflowCompileError(f"{name} must be an object")
    return normalized


def _json_value(value: Any, name: str) -> Any:
    try:
        encoded = _canonical_json(value)
        if len(encoded) > MAX_WORKFLOW_BYTES:
            raise WorkflowCompileError(
                f"{name} exceeds {MAX_WORKFLOW_BYTES} bytes"
            )
        return json.loads(encoded)
    except WorkflowCompileError:
        raise
    except (
        TypeError,
        ValueError,
        OverflowError,
        RecursionError,
        UnicodeError,
    ) as exc:
        raise WorkflowCompileError(f"{name} must be bounded JSON") from exc


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _reject_unknown_keys(
    value: Mapping[str, Any],
    allowed: set[str] | frozenset[str],
    name: str,
) -> None:
    unknown = sorted(set(value).difference(allowed))
    if unknown:
        raise WorkflowCompileError(f"{name} has unknown fields: {', '.join(unknown)}")


def _required_text(value: Any, name: str, *, max_length: int = MAX_TEXT) -> str:
    if value is None or (isinstance(value, str) and not value.strip()):
        raise WorkflowCompileError(f"{name} must not be empty")
    if not isinstance(value, str):
        raise WorkflowCompileError(f"{name} must be a string")
    text = value.strip()
    if len(text) > max_length:
        raise WorkflowCompileError(f"{name} exceeds {max_length} characters")
    return text


def _valid_id(value: Any, name: str) -> str:
    text = _required_text(value, name, max_length=63)
    if not ID_RE.fullmatch(text):
        raise WorkflowCompileError(f"{name} is invalid: {text}")
    return text


def _valid_resource(value: Any, name: str) -> str:
    text = _required_text(value, name, max_length=128)
    if not RESOURCE_RE.fullmatch(text):
        raise WorkflowCompileError(f"{name} is invalid: {text}")
    return text


def _string_tuple(
    value: Any,
    name: str,
    *,
    validator: Any,
    maximum: int = MAX_NODES,
    sort_values: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise WorkflowCompileError(f"{name} must be a list")
    if len(value) > maximum:
        raise WorkflowCompileError(f"{name} exceeds limit {maximum}")
    result = tuple(validator(item, f"{name}[{index}]") for index, item in enumerate(value))
    if len(set(result)) != len(result):
        raise WorkflowCompileError(f"{name} contains duplicates")
    return tuple(sorted(result)) if sort_values else result


def _positive_int(value: Any, name: str) -> int:
    return _bounded_int(value, name, minimum=1, maximum=2**31 - 1)


def _bounded_int(
    value: Any,
    name: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise WorkflowCompileError(f"{name} must be an integer")
    if value < minimum or value > maximum:
        raise WorkflowCompileError(f"{name} must be between {minimum} and {maximum}")
    return value


def _finite_float(
    value: Any,
    name: str,
    *,
    minimum: float,
    maximum: float,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise WorkflowCompileError(f"{name} must be a number")
    number = float(value)
    if not math.isfinite(number) or number < minimum or number > maximum:
        raise WorkflowCompileError(f"{name} must be between {minimum} and {maximum}")
    return number


def _choice(value: Any, name: str, choices: frozenset[str]) -> str:
    text = _required_text(value, name, max_length=64)
    if text not in choices:
        raise WorkflowCompileError(
            f"{name} must be one of: {', '.join(sorted(choices))}"
        )
    return text
