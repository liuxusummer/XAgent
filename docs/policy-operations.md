# Policy Operations

This document describes the operator-facing policy evidence implemented by the
durable orchestration and default Agent Loop. Runtime enforcement remains the
only authority.

## 1. Simulation is evidence, not authorization

`PolicyEngine.simulate(action)` and `PolicyEngine.evaluate(action)` execute the
same deterministic decision path. A simulation reports:

- every contract check reached before the decision;
- all matching rules, the highest-specificity finalists, and the stable winner;
- whether the decision came from a tool contract failure, a rule, a default, or
  the destructive-action approval floor;
- the declared timeout, idempotency, status-probe, compensation, and resource
  lock posture;
- a deterministic simulation digest.

The projection contains no raw arguments, resource-lock values, operation keys,
approval actors, or Artifact content. It always returns
`authorizes_execution=false` and `runtime_recheck_required=true`.

`LocalPolicyGate.simulate(...)` additionally runs Principal scope and protected
workspace preflight. It uses a distinct preview identity and does not increment
the live call counter. Its preview action digest therefore cannot be used as an
approval grant for the later execution.

## 2. Policy conformance packs

`evaluate_policy_conformance(...)` evaluates a bounded set of exact
`PolicyConformanceCase` values without executing a tool or consuming an
approval. The report is deterministic and includes missing required tools, case
drift, simulation digests, and a report digest.

`LocalPolicyGate.conformance_report()` supplies a baseline case for every
`LOCAL_TOOL_CONTRACTS` entry. CI also compares that registry with every default
Handler `exec_*` method, so a new executable tool cannot silently omit its
contract or baseline expectation.

## 3. Approval and recovery diagnostics

Web snapshots and Runtime status return a bounded, allowlisted diagnostic
projection. It can identify:

- pending, expired, and denied approvals;
- policy denial reason codes;
- `OUTCOME_UNKNOWN` attempts that prohibit automatic retry;
- evidence-backed recovery decisions.

Only safe reason codes and fixed operator actions are returned. Raw Domain Event
payloads, errors, backend details, paths, arguments, and credentials remain
hidden. `policy.decided` remains non-public.

## 4. Resolving `OUTCOME_UNKNOWN`

`resolve_recovery` is a mutation and must pass the Runtime's trusted out-of-band
authorizer. It accepts only:

- the exact Run, Node, and Attempt identity;
- one immutable evidence `ArtifactRef`;
- `confirmed_succeeded` plus an immutable result `ArtifactRef`, or
  `confirmed_failed` with no result Artifact.

Runtime verifies each Artifact against the injected Artifact Store before the
Store atomically commits `run.recovery_resolved`. The original terminal
`OUTCOME_UNKNOWN` Attempt is never rewritten or retried. The recovered Node
becomes `SUCCEEDED` with an artifact-only output or `FAILED` with a fixed error
code, after which Scheduler reconciliation resumes aggregate state transitions.
Replaying the same decision is idempotent; a conflicting later decision fails
closed.

There is intentionally no `retry_anyway` resolution. If the external outcome
cannot be proven, the Run stays in `WAITING_RECOVERY`.

## 5. Verification

```bash
python -m unittest -q \
  tests.test_orchestration_policy \
  tests.test_policy_conformance \
  tests.test_local_policy \
  tests.test_operator_diagnostics \
  tests.test_orchestration_store \
  tests.test_orchestration_protocol \
  tests.test_orchestration_runtime \
  tests.test_orchestration_mcp \
  tests.test_orchestration_web_api
```
