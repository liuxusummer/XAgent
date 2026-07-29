"""Explicit, non-exact import of a legacy task checkpoint.

The adapter accepts an already-selected checkpoint payload. It intentionally
has no workspace path or ``latest`` lookup API, so callers cannot accidentally
resume a different task. Imported summaries become a new Run input Artifact;
legacy tool summaries are never promoted to verified Activity receipts.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from src.core.checkpoint import render_resume_prompt

from .artifacts import (
    ArtifactKind,
    ArtifactRef,
    ArtifactSensitivity,
    ArtifactStore,
)
from .models import RunRecord
from .scheduler import DurableScheduler, RunInputReceipt
from .store import DurableRunStore
from .protocol import ProtocolValidationError, validate_identifier
from .workflow import compile_workflow

MAX_LEGACY_CHECKPOINT_BYTES = 1024 * 1024
MAX_LEGACY_ID_CHARS = 255
MAX_LEGACY_QUERY_BYTES = 16 * 1024
MAX_LEGACY_PROMPT_BYTES = 128 * 1024


class LegacyCheckpointImportError(ValueError):
    """A legacy checkpoint cannot be safely bound to a new durable Run."""


@dataclass(frozen=True, slots=True)
class LegacyCheckpointImport:
    run: RunRecord
    resume_prompt_ref: ArtifactRef
    source_checkpoint_digest: str
    replayed: bool = False


class LegacyCheckpointImporter:
    """Create one new ``legacy_prompt`` Run from an explicit checkpoint."""

    def __init__(
        self,
        store: DurableRunStore,
        artifact_store: ArtifactStore,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if not isinstance(store, DurableRunStore):
            raise TypeError("store must be a DurableRunStore")
        if not isinstance(artifact_store, ArtifactStore):
            raise TypeError("artifact_store must implement ArtifactStore")
        self.store = store
        self.artifact_store = artifact_store
        self._clock = clock or time.time

    def import_checkpoint(
        self,
        run_id: str,
        *,
        checkpoint_id: str,
        checkpoint: Mapping[str, Any],
        query: str = "",
    ) -> LegacyCheckpointImport:
        try:
            run_id = validate_identifier(run_id, "run_id")
        except ProtocolValidationError as exc:
            raise LegacyCheckpointImportError("run_id is invalid") from exc
        source_id = _bounded_text(checkpoint_id, "checkpoint_id")
        if source_id == "latest":
            raise LegacyCheckpointImportError(
                "checkpoint_id must be explicit; latest is not importable"
            )
        if not isinstance(checkpoint, Mapping):
            raise LegacyCheckpointImportError("checkpoint must be an object")
        detached = dict(checkpoint)
        embedded_id = _bounded_text(
            detached.get("checkpoint_id"),
            "checkpoint.checkpoint_id",
        )
        if embedded_id != source_id:
            raise LegacyCheckpointImportError(
                "checkpoint payload does not match the selected checkpoint_id"
            )
        canonical = _canonical_checkpoint(detached)
        source_digest = hashlib.sha256(canonical).hexdigest()
        agent_name = _optional_bounded_text(
            detached.get("agent_name"),
            "checkpoint.agent_name",
        )
        previous_status = _optional_bounded_text(
            detached.get("status"),
            "checkpoint.status",
        )
        if not isinstance(query, str):
            raise LegacyCheckpointImportError("query must be text")
        query_bytes = query.encode("utf-8")
        if len(query_bytes) > MAX_LEGACY_QUERY_BYTES:
            raise LegacyCheckpointImportError("query exceeds the import bound")
        prompt = render_resume_prompt(detached, query=query)
        prompt_bytes = prompt.encode("utf-8")
        if len(prompt_bytes) > MAX_LEGACY_PROMPT_BYTES:
            raise LegacyCheckpointImportError(
                "rendered resume prompt exceeds the import bound"
            )
        prompt_ref = self.artifact_store.put_bytes(
            prompt_bytes,
            media_type="text/plain; charset=utf-8",
            kind=ArtifactKind.GENERIC,
            sensitivity=ArtifactSensitivity.SENSITIVE,
            producer_run_id=run_id,
        )
        workflow = compile_workflow(
            {
                "schema_version": 2,
                "name": "legacy-checkpoint-import",
                "version": 1,
                "nodes": [
                    {
                        "id": "legacy_prompt",
                        "kind": "agent",
                        "config": {
                            "agent": "main",
                            "task": (
                                "Continue from the explicitly imported legacy "
                                "checkpoint prompt Artifact."
                            ),
                        },
                        "effect_class": "non_idempotent_write",
                    }
                ],
            }
        )
        scheduler = DurableScheduler(
            self.store,
            workflow,
            clock=self._now,
            artifact_verifier=self.artifact_store.verify,
        )
        existing = self.store.get_run(run_id)
        if existing is not None:
            _validate_existing_import(
                existing,
                checkpoint_id=source_id,
                checkpoint_digest=source_digest,
                prompt_ref=prompt_ref,
            )
            return LegacyCheckpointImport(
                existing,
                prompt_ref,
                source_digest,
                replayed=True,
            )
        run = scheduler.create_run(
            run_id,
            input=RunInputReceipt((prompt_ref,)),
            metadata={
                "recovery_mode": "legacy_prompt",
                "exact_recovery": False,
                "source_checkpoint_id": source_id,
                "source_checkpoint_digest": source_digest,
                "source_agent": agent_name,
                "source_status": previous_status,
            },
        )
        return LegacyCheckpointImport(run, prompt_ref, source_digest)

    def _now(self) -> float:
        try:
            value = float(self._clock())
        except (TypeError, ValueError) as exc:
            raise LegacyCheckpointImportError(
                "clock must return a finite timestamp"
            ) from exc
        if not math.isfinite(value) or value < 0:
            raise LegacyCheckpointImportError(
                "clock must return a finite timestamp"
            )
        return value


def _validate_existing_import(
    run: RunRecord,
    *,
    checkpoint_id: str,
    checkpoint_digest: str,
    prompt_ref: ArtifactRef,
) -> None:
    refs = run.input.get("artifact_refs") if isinstance(run.input, dict) else None
    artifact_id = (
        refs[0].get("artifact_id")
        if isinstance(refs, list)
        and len(refs) == 1
        and isinstance(refs[0], dict)
        else None
    )
    if (
        run.metadata.get("recovery_mode") != "legacy_prompt"
        or run.metadata.get("source_checkpoint_id") != checkpoint_id
        or run.metadata.get("source_checkpoint_digest") != checkpoint_digest
        or artifact_id != prompt_ref.artifact_id
    ):
        raise LegacyCheckpointImportError(
            "run_id is already bound to another import"
        )


def _canonical_checkpoint(checkpoint: Mapping[str, Any]) -> bytes:
    try:
        encoded = json.dumps(
            checkpoint,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise LegacyCheckpointImportError(
            "checkpoint must be bounded canonical JSON"
        ) from exc
    if len(encoded) > MAX_LEGACY_CHECKPOINT_BYTES:
        raise LegacyCheckpointImportError("checkpoint exceeds the import bound")
    return encoded


def _bounded_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise LegacyCheckpointImportError(f"{field_name} must be text")
    text = value.strip()
    if (
        not text
        or len(text) > MAX_LEGACY_ID_CHARS
        or any(ord(character) < 32 or ord(character) == 127 for character in text)
    ):
        raise LegacyCheckpointImportError(f"{field_name} is invalid")
    return text


def _optional_bounded_text(value: Any, field_name: str) -> str | None:
    if value in {None, ""}:
        return None
    return _bounded_text(value, field_name)


__all__ = [
    "LegacyCheckpointImport",
    "LegacyCheckpointImporter",
    "LegacyCheckpointImportError",
]
