"""Token-budgeted construction of the LLM-visible context.

The builder keeps raw local state outside the manifest.  Its output contains
only the selected renderings plus bounded hashes and token counts, making the
exact LLM view auditable without persisting prompts or tool output.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from src.core.agent_kernel import (
    ContextItem,
    ContextKind,
    ContextManifest,
    KernelValidationError,
    Principal,
    TrustLevel,
)


DEFAULT_CONTEXT_TOKENS = 16_384
DEFAULT_RESERVED_OUTPUT_TOKENS = 4_096
MAX_CONTEXT_SOURCES = 240
_BUDGET_WEIGHTS: dict[ContextKind, float] = {
    ContextKind.SYSTEM: 0.35,
    ContextKind.TASK_STATE: 0.20,
    ContextKind.RECENT_HISTORY: 0.12,
    ContextKind.COMPACTED_HISTORY: 0.07,
    ContextKind.RETRIEVAL_EVIDENCE: 0.10,
    ContextKind.MEMORY: 0.05,
    ContextKind.TOOL_RESULT: 0.08,
    ContextKind.SKILL: 0.03,
}


def estimate_tokens(text: str) -> int:
    """Conservative tokenizer-independent estimate for mixed CJK/ASCII text."""

    if not text:
        return 0
    return max(1, math.ceil(len(text.encode("utf-8")) / 3))


@dataclass(frozen=True, slots=True)
class ContextSource:
    ref_id: str
    kind: ContextKind
    content: str
    trust: TrustLevel
    priority: int
    llm_visible: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.ref_id, str) or not self.ref_id.strip():
            raise KernelValidationError("ContextSource ref_id is required")
        if len(self.ref_id) > 256:
            raise KernelValidationError("ContextSource ref_id exceeds its bound")
        object.__setattr__(self, "ref_id", self.ref_id.strip())
        try:
            object.__setattr__(self, "kind", ContextKind(self.kind))
            object.__setattr__(self, "trust", TrustLevel(self.trust))
        except ValueError as exc:
            raise KernelValidationError("invalid ContextSource enum value") from exc
        if not isinstance(self.content, str):
            raise KernelValidationError("ContextSource content must be text")
        if (
            not isinstance(self.priority, int)
            or isinstance(self.priority, bool)
            or not 0 <= self.priority <= 100
        ):
            raise KernelValidationError("ContextSource priority must be 0..100")
        if not isinstance(self.llm_visible, bool):
            raise KernelValidationError("ContextSource llm_visible must be boolean")


@dataclass(frozen=True, slots=True)
class ContextBuildResult:
    manifest: ContextManifest
    visible_content: Mapping[str, str]
    compaction: tuple[dict[str, Any], ...]
    component_usage: Mapping[str, int]

    def context_state(self) -> dict[str, Any]:
        return {
            "manifest": self.manifest.to_dict(),
            "manifest_digest": self.manifest.manifest_digest,
            "component_usage": dict(self.component_usage),
            "compaction": [dict(item) for item in self.compaction],
        }


class ContextBuilder:
    def __init__(
        self,
        *,
        max_input_tokens: int = DEFAULT_CONTEXT_TOKENS,
        reserved_output_tokens: int = DEFAULT_RESERVED_OUTPUT_TOKENS,
        component_budgets: Mapping[ContextKind | str, int] | None = None,
    ) -> None:
        if (
            not isinstance(max_input_tokens, int)
            or isinstance(max_input_tokens, bool)
            or max_input_tokens <= 0
        ):
            raise KernelValidationError("max_input_tokens must be positive")
        if (
            not isinstance(reserved_output_tokens, int)
            or isinstance(reserved_output_tokens, bool)
            or reserved_output_tokens < 0
            or reserved_output_tokens >= max_input_tokens
        ):
            raise KernelValidationError("reserved_output_tokens is invalid")
        self.max_input_tokens = max_input_tokens
        self.reserved_output_tokens = reserved_output_tokens
        available = max_input_tokens - reserved_output_tokens
        if component_budgets is None:
            budgets = {
                kind: max(1, math.floor(available * weight))
                for kind, weight in _BUDGET_WEIGHTS.items()
            }
            difference = available - sum(budgets.values())
            budgets[ContextKind.SYSTEM] += difference
        else:
            budgets = {
                ContextKind(kind): int(value)
                for kind, value in component_budgets.items()
            }
            for kind in ContextKind:
                budgets.setdefault(kind, 0)
            if any(value < 0 for value in budgets.values()):
                raise KernelValidationError("component budgets must not be negative")
            if sum(budgets.values()) > available:
                raise KernelValidationError("component budgets exceed available input")
        self.component_budgets = budgets

    def build(
        self,
        *,
        principal: Principal,
        manifest_id: str,
        sources: Iterable[ContextSource],
    ) -> ContextBuildResult:
        if not isinstance(principal, Principal):
            raise KernelValidationError("principal is required")
        source_items = tuple(sources)
        if len(source_items) > MAX_CONTEXT_SOURCES:
            raise KernelValidationError("context source count exceeds its bound")
        if len({source.ref_id for source in source_items}) != len(source_items):
            raise KernelValidationError("context source ref_id values must be unique")

        visible_content: dict[str, str] = {}
        compaction: list[dict[str, Any]] = []
        manifest_items: list[ContextItem] = []
        component_usage = {kind.value: 0 for kind in ContextKind}

        indexed = list(enumerate(source_items))
        indexed.sort(key=lambda item: (-item[1].priority, item[0]))
        for _index, source in indexed:
            source_sha256 = hashlib.sha256(
                source.content.encode("utf-8")
            ).hexdigest()
            original_tokens = estimate_tokens(source.content)
            if not source.llm_visible:
                manifest_items.append(
                    ContextItem(
                        ref_id=source.ref_id,
                        kind=source.kind,
                        token_count=original_tokens,
                        priority=source.priority,
                        trust=source.trust,
                        source_sha256=source_sha256,
                        llm_visible=False,
                    )
                )
                continue

            remaining = max(
                0,
                self.component_budgets[source.kind]
                - component_usage[source.kind.value],
            )
            selected = _truncate_to_tokens(source.content, remaining)
            selected_tokens = estimate_tokens(selected)
            if selected:
                visible_content[source.ref_id] = selected
                component_usage[source.kind.value] += selected_tokens
                manifest_items.append(
                    ContextItem(
                        ref_id=source.ref_id,
                        kind=source.kind,
                        token_count=selected_tokens,
                        priority=source.priority,
                        trust=source.trust,
                        source_sha256=source_sha256,
                        llm_visible=True,
                    )
                )
            else:
                manifest_items.append(
                    ContextItem(
                        ref_id=source.ref_id,
                        kind=source.kind,
                        token_count=original_tokens,
                        priority=source.priority,
                        trust=source.trust,
                        source_sha256=source_sha256,
                        llm_visible=False,
                    )
                )
            if selected_tokens < original_tokens:
                compaction.append(
                    {
                        "ref_id": source.ref_id,
                        "kind": source.kind.value,
                        "source_sha256": source_sha256,
                        "original_tokens": original_tokens,
                        "visible_tokens": selected_tokens,
                        "reason": (
                            "component_budget_truncated"
                            if selected
                            else "component_budget_dropped"
                        ),
                    }
                )

        # Restore source order so the manifest mirrors the original render path.
        order = {source.ref_id: index for index, source in enumerate(source_items)}
        manifest_items.sort(key=lambda item: order[item.ref_id])
        manifest = ContextManifest(
            manifest_id=manifest_id,
            principal_digest=principal.principal_digest,
            max_input_tokens=self.max_input_tokens,
            reserved_output_tokens=self.reserved_output_tokens,
            items=tuple(manifest_items),
        )
        return ContextBuildResult(
            manifest=manifest,
            visible_content=visible_content,
            compaction=tuple(compaction),
            component_usage=component_usage,
        )


def _truncate_to_tokens(text: str, token_budget: int) -> str:
    if token_budget <= 0 or not text:
        return ""
    if estimate_tokens(text) <= token_budget:
        return text
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    marker = f"\n...[context truncated sha256={digest}]...\n"
    if estimate_tokens(marker) >= token_budget:
        candidate = text[: max(1, token_budget * 3)]
        while candidate and estimate_tokens(candidate) > token_budget:
            candidate = candidate[:-1]
        return candidate
    target_bytes = max(1, token_budget * 3 - len(marker.encode("utf-8")))
    head_chars = max(1, target_bytes // 4)
    tail_chars = max(1, target_bytes // 4)
    candidate = text[:head_chars] + marker + text[-tail_chars:]
    while candidate and estimate_tokens(candidate) > token_budget:
        head_chars = max(0, head_chars - max(1, head_chars // 8))
        tail_chars = max(0, tail_chars - max(1, tail_chars // 8))
        if head_chars == 0 and tail_chars == 0:
            return ""
        candidate = text[:head_chars] + marker + text[-tail_chars:]
    return candidate


__all__ = [
    "ContextBuildResult",
    "ContextBuilder",
    "ContextSource",
    "DEFAULT_CONTEXT_TOKENS",
    "DEFAULT_RESERVED_OUTPUT_TOKENS",
    "MAX_CONTEXT_SOURCES",
    "estimate_tokens",
]
