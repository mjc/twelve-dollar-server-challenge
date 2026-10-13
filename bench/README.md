# Goose load test

Run this on a separate machine from the submission, from the repository root.
Install Rust with Cargo, generate `seed/tokens.json`, and build once before benchmarking:

```bash
cargo build --release --locked --manifest-path bench/Cargo.toml
VUS=1000 bench/target/release/feed-load
BASE_URL=http://10.0.0.5 VUS=8000 bench/target/release/feed-load
```

| Environment variable | Default | Meaning |
|---|---|---|
| `BASE_URL` | `http://127.0.0.1:3000` | Server to benchmark |
| `VUS` | `100` | Number of concurrent users during the hold |
| `RAMP_UP` | `60s` | Ramp from zero users to `VUS` |
| `DURATION` | `5m` | Hold `VUS` for this long |
| `TOKENS` | `seed/tokens.json` | Token file, relative to the working directory |
| `SUMMARY` | `results/goose-summary.json` | Scoring JSON destination (overwritten each run) |

Times accept integer seconds or `h`, `m`, `s`, such as `1m30s`.
After the hold, users ramp down to zero over 30 seconds. Each user gets its own
HTTP/1.1 client with keep-alive and a 60-second request timeout. Users cycle through
tokens by their zero-based index, with a random 0–5 second delay at startup.

Each user repeatedly fetches the feed, waits 3–7 seconds, fetches a random post
from that feed, waits 3–8 seconds, likes it with 15% probability, creates a post
with 2% probability, and idles 5–15 seconds. A failed feed ends that iteration after
a 3–7 second pause. Failed post, like, or create requests leave the rest of the
iteration running. Think times are continuous uniform random values, as in k6.

For an unscored warm-up, use `VUS=1000 DURATION=2m`; ignore the score for that run.
For a scored run, use the default 5-minute hold. Give each run a distinct `SUMMARY`
path if you want to retain results. Goose's CLI also supports optional diagnostic
reports, for example `--report-file results/goose.html`. Use the environment variables
above for scoring runs; CLI options that change the test plan are for exploration.

## Scoring and the k6 differences

The final **Scoring summary** and its JSON file determine the score. Exit status
is 0 for a pass, 1 for a threshold failure, an interrupted run, or no requests,
and 2 for configuration/runtime/output errors. Thresholds use the unrounded
values: aggregate p95 **< 500 ms**, aggregate p99 **< 1,000 ms**, and HTTP failure
rate **< 0.01**. Equality fails. Failed requests also contribute latency samples.

The aggregate combines individual samples from every endpoint before calculating
percentiles; it does not average endpoint percentiles. Empty endpoint statistics
are `null`. The report also includes average, median, maximum, request counts,
HTTP failures, and check failures per endpoint and in aggregate.
It records the target hosts, maximum and total users, and actual run duration
alongside the pass/fail result.

| Property | Original k6 test | Goose port |
|---|---|---|
| Request latency | `http_req_duration`: sending + waiting + receiving; excludes initial DNS/connection/TLS setup | Scoring timer covers sending through the complete body, **including** DNS/connection/TLS setup and client overhead |
| Percentiles | Linear interpolation at `p * (n - 1)` over individual samples | Same interpolation on unrounded floating-point milliseconds |
| Native Goose metrics | — | Diagnostic only: Goose 0.18.1 times to headers, stores integer milliseconds, and rounds percentile buckets (up to 100 ms near the thresholds) |
| HTTP failure rate | Transport errors and statuses outside 200–399 | Same status range; connection, timeout, and body-read failures count once |
| Endpoint checks | Feed/post 200, like 200/201, create 201; checks have no scoring threshold | Same status checks, reported separately; unusable feed JSON also counts as a check failure |
| Sampling window | Whole run: ramp-up, hold, ramp-down, and graceful completion | Whole run, including completed requests during drain; no reset after ramp-up |
| Coordinated omission | Closed loop; no synthetic correction | Scoring uses actual requests only; Goose mitigation is disabled by default |
| Shutdown | k6 `gracefulRampDown: 10s` can interrupt an iteration | Goose finishes an in-progress browsing iteration; the drain can extend beyond the scheduled ramp-down |

Exact timing parity is unavailable through Goose's reqwest client: it does not
expose k6's individual DNS/connect/send/wait/receive timings. Full-body client
wall time avoids missing slow body transfers and can be higher than k6's metric,
especially for new connections. Historical k6 scores should be labeled with their
generator and remeasured before comparing them directly with Goose scores.
Goose's native reports may differ from the scoring JSON in both latency and error
counts, particularly for body-read failures or a CLI-enabled coordinated-omission
correction. Transaction/scenario duration includes think time and is not request
latency. The JSON's `checks_failed` is diagnostic, matching the old script's lack
of a check threshold; correctness is still enforced by `test/test.sh`.

Samples use about 8 MB per million requests, plus vector capacity and a second
copy for the aggregate at reporting time. Watch the generator's CPU, memory,
network, and file descriptor limits so its capacity does not cap the score.

These definitions follow the pinned [Goose request implementation](https://docs.rs/crate/goose/0.18.1/source/src/goose.rs),
[Goose timing aggregation](https://docs.rs/crate/goose/0.18.1/source/src/metrics.rs),
[k6 metric definitions](https://grafana.com/docs/k6/latest/using-k6/metrics/reference/),
and [k6 percentile implementation](https://github.com/grafana/k6/blob/master/metrics/sink.go).

## Tests

```bash
cargo test --locked --manifest-path bench/Cargo.toml
```

The tests cover interpolated percentiles, strict threshold boundaries, empty
runs, and the distinction between HTTP errors and endpoint checks. A local HTTP
fixture checks delayed bodies, authentication and JSON writes, failed statuses,
truncated bodies, and refused connections without loading a submission.
