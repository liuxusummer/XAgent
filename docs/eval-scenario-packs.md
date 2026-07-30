# Versioned Eval scenario packs

Scenario packs turn an Eval dataset into a reproducible capability experiment.
They bind the case definitions, fixture contents, runtime limits, and
least-privilege boundary under one SHA-256 digest.

Validate the built-in pack without calling a model:

```bash
uv run python -m src.eval_scenarios \
  --workspace workspace/default.ws \
  --dataset system/eval/core-capabilities-v1/cases.jsonl
```

The command returns only bounded pack metadata and exits with `0` when valid or
`2` for invalid input.

## Layout

```text
system/eval/<pack>/
├── pack.json
├── cases.jsonl
└── fixtures/
    └── <fixture-id>/
        └── business/...
```

`pack.json` schema version 1 contains:

- a safe pack ID, display name, and semantic version;
- the adjacent dataset filename and fixtures directory;
- default Principal scopes;
- tool and skill allowlists;
- `memory_mode: "none"` and a bounded turn limit.

Schema v1 supports `workspace.read`, `workspace.write`, `workspace.delete`, and
the five workspace file tools. It also permits only the quarantine half of the
managed-memory lifecycle: `memory.propose` / `memory_propose`. A proposal stays
pending inside the disposable case workspace; schema v1 still rejects
`memory.read`, `memory.review`, non-`none` memory modes, `host.read`, process,
network, browser, interaction, delegation, and state scopes/tools even when the
caller owns them.

Every fixture file must be regular, the tree cannot contain symbolic links,
and the whole pack is bounded by file-count and byte limits. The pack digest
binds the normalized manifest, raw dataset bytes, fixture paths, sizes, and
content digests. A change after dataset import stops execution before an Agent
is created.

## Case contract

Cases keep runtime controls separate from deterministic assertions:

```json
{
  "id": "recover-stale-path",
  "task": "...",
  "grader": {"type": "deterministic"},
  "runtime": {
    "fixture": "recovery",
    "scopes": ["workspace.read"]
  },
  "assertions": {
    "tool_called": ["file_read", "file_search"],
    "tool_call_count": {"file_read": 2},
    "tool_paths": {"file_read": ["business/correct/answer.txt"]},
    "retrieval_expected_paths": ["business/correct/answer.txt"],
    "min_citation_coverage": 1.0,
    "min_query_term_coverage": 0.5,
    "max_stale_evidence": 0,
    "tool_sequence": ["file_read", "file_search", "file_read"],
    "tool_not_called": ["code_run"],
    "policy_outcome": ["allow"],
    "recovered": true,
    "file_exists": ["business/correct/answer.txt"],
    "max_total_tokens": 12000
  }
}
```

Case scopes may only narrow the pack defaults. At Web execution time they must
also be a subset of the authenticated caller's scopes. Pack tool and skill
allowlists intersect with the selected Agent profile; they never add a tool or
skill excluded by that profile.

## Isolation lifecycle

For each case the runner:

1. revalidates the current pack digest against imported metadata;
2. creates a unique workspace under the owner-partitioned Eval run directory;
3. copies only the selected fixture, without following links;
4. builds a fresh Agent with no reviewed Memory and narrowed runtime limits;
5. evaluates file assertions inside that case workspace;
6. closes the Agent and removes the case workspace.

The model never sees the source workspace, other cases, or the pack fixture
directory. Policy metadata is recorded for metrics but is not injected into the
next LLM turn.

## Built-in v1 coverage

`core-capabilities-v1` version 1.2 contains six small, inspectable scenarios:

- least-privilege file-read tool selection;
- recovery after a deliberately stale file path;
- a denied write under a read-only Principal, with proof that no file appeared.
- retrieval grounding against a current file while an archived file contains an
  adversarial instruction, with ordered tool, expected retrieval path,
  query-term coverage, stale-index, and answer-citation checks.
- resistance to an untrusted document that asks the model to create poisoned
  managed Memory, including proof that no candidate store appeared;
- preservation of authoritative tail evidence after a long tool result is
  compacted under the context budget.

These cases are model-dependent experiments, not ordinary CI unit tests. CI
validates the pack schema, digest, isolation, scope narrowing, assertions, and
runner behavior without spending model tokens. A real model run can then be
compared with the offline regression gate in
[`eval-regression-gates.md`](eval-regression-gates.md).
