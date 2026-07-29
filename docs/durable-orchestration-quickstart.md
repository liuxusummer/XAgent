# Durable Orchestration Quickstart

Both examples run locally without an API key, network access, model call, or
external service.

## Minimal Runtime composition

Start with the smallest public-API composition:

```bash
.venv/bin/python examples/orchestration_runtime_minimal.py
```

It creates disjoint control-plane and Agent-workspace roots, submits one
Workflow through `OrchestrationRuntime`, binds an executor to the scheduler
selected for the requested Run, executes one read-only Activity, and verifies
logical replay. The summary should contain `status=completed`,
`processed=1`, and `replay_matches_live=true`.

For a persistent directory:

```bash
.venv/bin/python examples/orchestration_runtime_minimal.py \
  --runtime-dir /tmp/xagent-runtime-minimal
```

Production composition should use an `executor_factory(scheduler)` rather than
sharing an executor that was constructed for another Run's scheduler.
`execution_driver` and `recovery_driver` receive the exact requested `run_id`;
they must not scan for or act on unrelated Runs.

## Fault-injection acceptance demo

From the repository root:

```bash
.venv/bin/python examples/durable_orchestration_demo.py
```

The command creates a temporary runtime, executes the workflow, verifies its
invariants, prints a sanitized JSON summary, and removes the temporary files.
To inspect the SQLite event store, immutable Artifacts, and visible-effect
ledger after the run:

```bash
.venv/bin/python examples/durable_orchestration_demo.py \
  --runtime-dir /tmp/xagent-durable-demo
```

Persistent control-plane files are created under
`/tmp/xagent-durable-demo/control-plane/`. The sandbox profile receives only
the disjoint `/tmp/xagent-durable-demo/agent-workspace/` root; the Store,
Artifact store, and GC namespace are not mounted as an Activity working
directory.

The summary should report a completed Run, three backend dispatches, one
visible publication, a replayed duplicate retry, recovery of an external write
that committed before Tx D, operation-key deduplication, a rejected stale
claim, live projection/replay equality, and only `known` reliability metrics.
The exact replay digest is deterministic for this workflow.

## What the acceptance scenario proves

The declarative workflow is
[`examples/workflows/durable_orchestration_demo.json`](../examples/workflows/durable_orchestration_demo.json).
It exercises the kernel in this order:

1. Raw Run input and the closed-function descriptors are stored as immutable
   Artifacts. Only bounded `ArtifactRef` identities enter Run state and Domain
   Events; both demo tools declare `requires_script_artifact`, so script bytes
   are verified and materialized before authorization. A mutable argv path
   cannot substitute for the Artifact.
2. Each branch declares `input_mapping.source = {"source": "run_input"}`.
   The Scheduler resolves and verifies that named binding, includes its
   canonical Artifact identity in the Activity request hash, and exposes the
   resulting refs on the durable claim. No demo code manually wires the Run
   input into either Activity. A `parallel` control node makes both Tool
   Activities claimable before either executes. One completed branch response
   is discarded and the exact retry is durably replayed.
3. An `all` join waits for both branch receipts. Its named upstream mappings
   are resolved in mapping-name order, independent of branch completion order,
   and projected as one deterministic Artifact-ref receipt. Publication
   consumes that join receipt through its own declarative mapping.
4. Publication is an idempotent-write Tool Activity whose policy explicitly
   requires a stable idempotency key, status probe, probe-before-retry timeout
   behavior, and the external-ledger resource lock. It also requires a
   registered, action-bound, expiring approval grant before dispatch.
5. The external ledger commits the publication under the stable operation key.
   The demo then crashes after the backend returns but before Tx D records the
   Activity receipt, leaving the Attempt durably `RUNNING`.
6. Store, Scheduler, Policy, Sandbox Dispatcher, backend, and Executor objects
   are reconstructed over the same files. The lease reaper probes the external
   ledger, verifies its Artifact receipt, and commits `verified_succeeded`
   without redispatching the backend.
7. A stale fencing token is rejected, and logical replay is compared with the
   live projections.
8. Explicit scenario counters produce a reliability report. The local logical
   restart latency is `0 ms`; production SLOs must replace this controlled
   sample with monotonic wall-clock evidence.

The visible-effect ledger is a separate SQLite stand-in for an external API. A
unique operation key atomically deduplicates publication, and reuse of that key
for a different execution binding fails closed. Its status probe returns the
original immutable result Artifact.

## Security boundary

The program never prints raw Run input, Tool input, approval identity, Artifact
content, operation key, or command arguments. Recovery does require the
non-secret operation key in the internal Attempt projection and idempotency
state. It is generated only from compiler-allowlisted stable workflow/run/node
fields and the input digest; policy, approval, receipt, Web, and telemetry
surfaces expose only its digest. The test also checks that the private input
marker is absent from both the returned summary and SQLite bytes.

The demo resolves the control-plane and Agent-workspace roots before startup
and rejects overlapping paths. Only the Agent-workspace root is supplied as
the sandbox profile's `cwd`/allowed root. The closed backend receives verified
Artifact bytes through the trusted adapter contract, never a control-plane
filesystem root.

`_ClosedFunctionBackend` is a zero-dependency acceptance adapter: it recognizes
only two fixed in-process functions and accepts no shell, executable,
environment, or network input. Each function is selected only when the
materialized bytes match its verified immutable descriptor Artifact. Its
receipt truthfully reports
`trusted_function`, which means pre-registered trusted code and **does not**
claim arbitrary-program isolation. The demo profile opts in to that weaker
level explicitly; the default `os_sandbox` profile rejects it. Replace the
adapter with a backend that provides real OS/container isolation before
running user-supplied code.

The separate `LocalProcessSupervisorBackend` proves only the F14 local process
lifecycle contract. It reports `development_unsafe`, accepts only read-only
actions, launches without a shell or host environment, bounds retained output,
and terminates a dedicated process group with TERM followed by KILL and wait.
The same bounded cleanup runs if selector registration, output reading, or
supervision fails after `Popen`, and closes both pipes before propagating the
sanitized failure.
It does not enforce CPU, memory, or process-count isolation; it validates only
wall-time timeout, retained-output, and process-group lifecycle. It is
process-tree supervision, not arbitrary-code isolation.

## Run the example tests

```bash
PYTHONPYCACHEPREFIX=/tmp/xagent-pycache \
  .venv/bin/python -m unittest tests.test_orchestration_demo -v
```
