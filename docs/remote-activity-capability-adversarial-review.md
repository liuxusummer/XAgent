# Remote Activity Capability Adversarial Review

> Scope: capability truth from Worker registration through control-plane claim
> and worker-side execution.
>
> Boundary: the production reference chain proves only Tool Activity. This
> review does not claim that remote Agent Activity is implemented.

## Invariants

1. A registration cannot widen either the control-side Admitter or worker-side
   execution adapter's proven Activity kinds.
2. Every registered kind also requires the exact `activity.<kind>` capability.
3. Capability validation occurs before registration journal mutation, before
   Store claim mutation, and before Worker prepare/start.
4. Missing, malformed, exceptional, or changed capability evidence fails
   closed with bounded error codes.
5. A direct worker adapter API call cannot bypass the same kind check.

## Round 1: authority and configuration attack

Attacker model: a deployment operator or compromised composition mutates
adapter metadata, advertises `activity.agent`, or supplies malformed capability
evidence.

Findings:

- P1: `supported_activity_kinds` was initially a replaceable class attribute on
  the secure reference adapters.
- P1: a third-party worker adapter property exception could escape as an
  internal exception.

Fixes:

- Secure adapters expose a read-only property backed by the fixed Tool-only
  capability set.
- Worker contract reads reduce property failures to `security_not_ready`.
- Malformed control proofs make production readiness false and registration
  fails before publication.

Executable evidence:

- `test_secure_worker_adapter_rejects_unproven_agent_assignment`
- `test_worker_reduces_adapter_capability_failure_to_safe_error`
- `test_malformed_control_capability_proof_is_not_production_ready`

Residual: a custom trusted adapter can truthfully add Agent only if it
implements the full control and execution protocols. The reference secure
adapter cannot be widened per instance.

## Round 2: concurrency and stale-proof attack

Attacker model: capability evidence changes while Worker poll blocks or between
registration and claim.

Findings:

- P1: Worker originally retained the capability snapshot taken before poll, so
  a later revocation could leave a stale local decision.

Fixes:

- Worker re-reads the adapter contract immediately after poll and before
  assignment validation/prepare/start.
- Control re-checks current Admitter kinds for every new claim.
- Secure control admission independently validates claim kind, registration
  kind, and exact capability.

Executable evidence:

- `test_claim_rechecks_adapter_activity_kind_after_registration`
- `test_registration_rejects_unproven_activity_kind_before_publish`
- `test_worker_rejects_configured_kind_not_proven_by_adapter`

Residual: an already claimed Activity is still governed by durable
lease/fencing and completion rules. Capability drift does not rewrite or delete
durable execution facts.

## Round 3: alternate-entry and malformed-object attack

Attacker model: a caller skips `prepare()`, constructs a digest-consistent
`RemoteExecutionGrant`, or supplies hostile/non-string collection members.

Findings:

- P0: `SecureRemoteExecutionAdapter.execute()` trusted a preconstructed grant
  without rechecking its Activity kind.
- P2: collection membership validation relied on generic set behavior and did
  not require exact string instances.

Fixes:

- The common worker `_require_ready()` gate now checks the grant assignment kind,
  covering execute, verify, and cancel paths even when prepare is bypassed.
- Both control and worker contracts require non-empty `frozenset` proofs with
  exact string members from the closed `{agent, tool}` set.
- Local configuration conversion and adapter property access are exception
  bounded.

Executable evidence:

- `test_secure_worker_adapter_rejects_unproven_agent_assignment` includes the
  direct-grant bypass reproduction.
- `test_worker_default_activity_kind_matches_secure_reference_adapter`
- Protocol, remote execution, Fleet control/reconcile, scheduling, and
  distributed fault-matrix regression suites.

## Closure

All P0/P1 findings above are fixed. The remaining boundary is intentional:
`agent` remains a protocol enum for future compatibility, while the current
production reference control, worker, and Fleet projector remain Tool-only.
Remote Agent support requires a separate whole-Agent execution and receipt
contract; changing an advertised string is insufficient.

## Full-suite integration review

The first 1511-test repository run found one P1 compatibility omission: the
flagship distributed demo's private worker adapter implemented the execution
methods but did not publish the new proof, so three demo paths failed during
composition. The adapter is now explicitly Tool-only, matching its workflow,
policy, and daemon registration. No compatibility fallback was added because
silently inferring kinds would recreate the original false-advertising bug.
