# Eval regression gates

XAgent Eval stores single-run evidence in `result.json`. A regression gate
compares two completed runs and applies an explicit, versioned budget:

```bash
uv run python -m src.eval_report \
  --current workspace/default.ws/runtime/eval/runs/<current>/result.json \
  --baseline workspace/default.ws/runtime/eval/runs/<baseline>/result.json \
  --budget docs/examples/eval-regression-budget.json \
  --output /tmp/xagent-eval-report.json
```

The command exits with `0` when all checks pass, `1` on a protected regression,
and `2` for invalid or incompatible input. Its JSON report contains run
identities, selected scalar metrics, and failure reasons. It deliberately omits
case tasks, response excerpts, and tool payloads.

## Budget contract

Each check addresses a numeric run field with a JSON Pointer. `direction` is
explicit because a higher value is desirable for some metrics (pass rate) and
undesirable for others (latency or error rate).

```json
{
  "version": 1,
  "require_same_dataset": true,
  "checks": [
    {
      "id": "pass-rate",
      "path": "/summary/pass_rate",
      "direction": "higher",
      "threshold": 0.9,
      "max_regression": 0.02
    },
    {
      "id": "p95-latency",
      "path": "/summary/p95_duration",
      "direction": "lower",
      "max_regression_ratio": 0.2
    },
    {
      "id": "retrieval-pass-rate",
      "path": "/summary/tags/retrieval/pass_rate",
      "direction": "higher",
      "threshold": 0.85,
      "max_regression": 0.02
    }
  ]
}
```

`threshold` is an absolute floor or ceiling. `max_regression` is an absolute
allowance relative to the baseline; `max_regression_ratio` is a relative
allowance. When both are present, the larger allowance applies. Dataset
compatibility uses the canonical SHA-256 digest of the evaluated case subset
for new runs and falls back to the dataset ID plus case count only when both
runs are legacy. A mixed legacy/versioned comparison fails compatibility.

## Metrics

Run summaries retain the existing pass rate, average duration, and average
turns, and add:

- error and combined failure rates;
- p95 duration;
- tool attempts and known-status success rate;
- recovery opportunities and recovery rate;
- policy decision outcome counts, including approval-required transitions;
- token totals, any-usage coverage, total-token coverage, and average total
  tokens over cases that reported a total;
- the same metrics grouped by scenario tag.

A recovery opportunity is a case with at least one non-success tool result. It
counts as recovered only when the case ultimately passes. Token coverage makes
missing provider usage data visible instead of treating it as measured zero.

The gate is deterministic and offline. Running the agent remains a separate,
potentially model-dependent step; the comparison command never calls an LLM or
external service.
