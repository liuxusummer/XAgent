# Policy Operations Adversarial Review

This record covers the three mandatory review rounds for Agent Platform Phase 4.
The reviewed trust boundary is policy simulation, durable approval/recovery
state, operator diagnostics, and the public Runtime/Web/MCP projections.

## Round 1 — authorization and information flow

Threats exercised:

- submitting a valid recovery decision through authority scoped only to a Run;
- spoofing a policy resolution source with a rule-controlled reason code;
- bypassing the recovery API by appending a forged projection event;
- leaking evidence, arguments, paths, actors, or error payloads in diagnostics.

Findings and fixes:

- Recovery authorization now binds the exact Run, Node, Attempt, and decision
  digest.
- Local policy reports whether the engine actually evaluated the action instead
  of inferring provenance from a potentially spoofable reason code.
- Store validates the complete `run.recovery_resolved` payload and projection
  identity at both low-level append and transactional commit boundaries.
- Public diagnostics return fixed fields only; Domain Event payloads and
  `policy.decided` remain private.

## Round 2 — concurrency, replay, and resource bounds

Faults exercised:

- concurrent contradictory recovery decisions;
- two Nodes waiting for unknown-outcome recovery in one Run;
- concurrent local policy calls;
- infinite or oversized tool, rule, actor, capability, lock, conformance-case,
  and sensitive-key iterables.

Findings and fixes:

- Contradictory decisions linearize to one durable event; the loser fails closed.
- Resolving one Node no longer resumes a Run while another Node still waits for
  recovery.
- Local action call identities are allocated under a lock.
- Policy and conformance constructors consume at most their declared bounds;
  they do not materialize untrusted infinite iterables.
- Replay preserves the original terminal `OUTCOME_UNKNOWN` Attempt and
  reconstructs the resolved Node/Run projections from the recovery event.

## Round 3 — compatibility and observability honesty

Contracts exercised:

- recovered success output as an upstream workflow input;
- strict protocol parsing and additive MCP tool discovery;
- approval expiry and policy denial across Web, Runtime, and MCP;
- syntactically valid but secret-bearing diagnostic codes/categories;
- cyclic, deeply nested, high-node-count, and oversized sensitive argument
  structures.

Findings and fixes:

- Recovery success uses the existing Artifact receipt shape consumed by workflow
  input mapping.
- Unknown diagnostic reason codes now degrade to the fixed `policy_denied`
  value; MCP category counts accept only the four documented categories.
- Argument trees have explicit depth, node-count, scalar-size, and cumulative
  sensitive-byte limits. Cycles terminate without recursion.
- Simulation remains non-authoritative and every recovery mutation is
  re-authorized and re-verified at Runtime ingress.

## Verification evidence

The phase gate runs:

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

It is followed by the complete tracked backend suite, frontend lint/build,
projection replay checks, diff hygiene, and secret-pattern scanning. No blind
`retry_anyway` path exists. If evidence cannot establish an external outcome,
the Run remains `WAITING_RECOVERY`.
