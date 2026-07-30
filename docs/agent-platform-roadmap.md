# XAgent Agent Platform Roadmap

## North star

XAgent should demonstrate, rather than merely claim, that an agent can execute
long-running physical work safely. Every headline capability therefore needs:

1. an explicit runtime contract and trust boundary;
2. an end-to-end path that is usable without test-only adapters;
3. deterministic regression cases and measurable quality or reliability;
4. structured evidence that explains what happened without recording secrets;
5. a documented failure mode and recovery path.

Line count and feature count are not success metrics. The project is successful
when a reviewer can reproduce the behavior, inject failures, inspect evidence,
and verify that safety policy still holds.

## Baseline (2026-07-31)

- 64,420 tracked Python source lines.
- 65 tracked Python test modules; the standard suite runs 1,085 tests.
- A default Agent Kernel with bounded context, policy enforcement, reviewed
  memory, tenant-scoped retrieval, checkpoints, and structured telemetry.
- An optional durable orchestration control plane with replay and fault-matrix
  coverage.
- A React/FastAPI Web application.
- No repository CI before this roadmap: backend and frontend regressions were
  only caught by a developer running commands locally.

## Delivery gates

Every phase must satisfy all of the following before it is considered complete:

- behavior is covered at the contract, integration, and adversarial boundaries;
- the standard backend suite and frontend lint/build pass;
- default paths do not require optional distributed infrastructure;
- sensitive content is absent from logs, fixtures, diffs, and generated
  artifacts;
- compatibility and migration behavior are explicit;
- three adversarial review rounds find no unresolved P0/P1 issue;
- before/after evidence is recorded in the relevant specification or benchmark.

## Phases

### Phase 0 — Reproducible quality gates

- Run the locked Python 3.12 backend suite on every pull request and protected
  branch update.
- Run frontend lint and production build from `package-lock.json`.
- Use least-privilege workflow permissions, immutable Action SHAs, timeouts,
  concurrency cancellation, and no persisted checkout credentials.

Exit evidence: GitHub Actions and the equivalent local commands both pass from
a clean checkout.

### Phase 1 — Agent capability evaluation

- Separate deterministic runtime assertions from model-quality judgments.
- Add versioned scenario packs for tool choice, recovery, retrieval grounding,
  memory poisoning resistance, context pressure, and permission denial.
- Record pass rate, latency, token usage, tool attempts, recovery rate, and
  policy outcomes without storing prompts or tool payloads.
- Support baseline comparison with explicit regression budgets.

Exit evidence: one command produces a bounded, machine-readable report and
fails when a protected metric regresses.

Foundation delivered: Eval result schema v2 records dataset-subset digests,
tagged capability metrics, tool recovery, policy decisions, and token coverage.
`python -m src.eval_report` compares completed runs against an explicit,
versioned regression budget without emitting prompts or tool payloads. Scenario
pack v1 now adds per-case workspaces, content-bound fixtures, Principal scope
narrowing, and tool-selection, recovery, permission-denial, and retrieval
grounding cases. Descriptor-relative no-follow reads now bind both file-index
ingestion and file tools against concurrent link replacement. Memory modes also
filter reviewed records by workspace/current-agent namespace. Pack version 1.1
adds memory-poisoning and context-pressure cases while granting only isolated
`memory.propose`, never activation. The Memory Store now validates the complete
candidate-to-record review chain, uses private bounded no-follow persistence,
and context serialization applies explicit source, depth, item, and string
pressure bounds with chunked token/hash accounting.

### Phase 2 — Retrieval and context quality

- Add query decomposition and hybrid retrieval diagnostics without weakening
  tenant or file-integrity boundaries.
- Introduce deterministic reranking inputs and citation coverage metrics.
- Measure useful-context density, evidence recall, truncation, and stale-index
  rejection under fixed token budgets.

Exit evidence: versioned retrieval benchmarks improve grounding metrics while
all cross-tenant and stale-evidence adversarial cases remain denied.

Foundation delivered: file search now produces a bounded, content-digested
query plan, deterministic ranking signals, and payload-safe diagnostics.
Descriptor-relative live-content verification rejects changed, deleted, or
link-replaced index entries before their snippets become evidence, while
overfetch preserves fresh backfill candidates. Eval measures expected-path
recall, evidence binding, final-answer citation, query-term coverage, and stale
rejection; core capability pack 1.2 gates its grounding case on these metrics.

### Phase 3 — Memory lifecycle

- Add duplicate/conflict detection, supersession, retention, and provenance
  views on top of reviewed memory.
- Keep proposal, review, activation, and retrieval as distinct authorization
  steps.
- Measure memory precision, stale-memory suppression, poisoning rejection, and
  context cost.

Exit evidence: approved memory improves repeat-task success without allowing
unreviewed or cross-tenant content into model-visible context.

Foundation delivered: Memory Store schema v2 adds bounded logical keys,
reviewer-visible duplicate/conflict assessment, and atomic explicit
supersession. Overlapping ACL audiences fail closed when one reviewer cannot
authorize every affected record. Runtime reads suppress superseded records,
while provenance views retain the review chain and scoped compaction removes
retired payload after a retention window with a cumulative purge digest. Strict
v1 validation and lazy migration preserve existing workspaces.
The three-round threat/concurrency/upgrade review is recorded in
[`memory-lifecycle-adversarial-review.md`](memory-lifecycle-adversarial-review.md).

### Phase 4 — Policy operations

- Add policy simulation and explainability for operators before execution.
- Make approval expiry, denial, unknown outcome, and recovery observable
  end-to-end.
- Add policy conformance packs for every executable tool contract.

Exit evidence: operators can predict and audit policy outcomes, while runtime
enforcement remains the sole authority.

### Phase 5 — Durable execution integration

- Replace reference-only remote polling with an authenticated server/pull
  transport and a deployable worker lifecycle.
- Preserve Store/Scheduler authority, fencing, action-bound grants, artifact
  boundaries, and fail-closed recovery.
- Publish reproducible crash, partition, duplicate-delivery, and cancellation
  demonstrations.

Exit evidence: a multi-process demo survives the fault matrix without duplicate
side effects or authority being delegated to workers.

### Phase 6 — Production and project presentation

- Publish stable configuration examples, packaging, upgrade checks, and
  operator runbooks.
- Provide an architecture walkthrough and a reproducible showcase scenario.
- Report capability limits honestly; test-only and reference implementations
  must be labeled as such.

Exit evidence: a new contributor can run the showcase and inspect its evidence
from a clean checkout.

## Prioritization rule

Choose the next slice by:

```text
priority = (user value × risk reduction × evidence value)
           / (implementation cost × migration risk)
```

P0 correctness and security defects always preempt roadmap work. Otherwise,
prefer a thin end-to-end slice over another isolated abstraction.
