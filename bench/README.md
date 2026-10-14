# Goose load test

Start your server separately. The runner builds Goose and connects to that
server. From the repository root:

```bash
./bench/run.sh 127.0.0.1 3000
VUS=8000 ./bench/run.sh 10.0.0.5 3000
```

Supply IP and port together, or omit both to use `BASE_URL` below.
The runner uses Rust 1.99.0, installing it with rustup if needed, and builds
release with locked dependencies. Invoke it by path from any directory; relative
`TOKENS`, `SUMMARY`, and `--output-dir` paths resolve from `bench/`, as k6's token
paths did.
The token file must already exist.
When a worktree lacks the default tokens, the runner reuses the main checkout's
`seed/tokens.json` if available.

| Variable | Default | Meaning |
|---|---|---|
| `BASE_URL` | `http://127.0.0.1:3000` | Target server |
| `VUS` | `100` | Concurrent users |
| `RAMP_UP` | `60s` | Ramp to `VUS` |
| `DURATION` | `5m` | Hold at `VUS` |
| `TOKENS` | `../seed/tokens.json` | User tokens |
| `SUMMARY` | unset | Optional additional copy of `aggregate.json` |
| `SOURCE_IPS` | unset | Local source addresses, or SSH hosts with `--user` |

Times accept seconds or `h`, `m`, `s`, such as `1m30s`. Ramp-down takes 30 seconds.

To spread connections across source IPs:

```bash
SOURCE_IPS='127.0.0.[1-5]' ./bench/run.sh 127.0.0.1 3000
SOURCE_IPS='192.0.2.10,192.0.2.11' ./bench/run.sh 192.0.2.20 80
```

Lists accept IPv4/IPv6 and inclusive IPv4 final-octet ranges. Without `--user`,
one local generator assigns addresses round-robin and keeps them across
reconnects. Addresses must be locally bindable and reach the target.
Non-loopback addresses must already be assigned.
The runner checks binding and records the expanded `source_ips` in JSON.

For multiple generator machines, add `--user USER`. `SOURCE_IPS` then selects
SSH hosts instead of local addresses:

```bash
SOURCE_IPS='10.0.0.11,10.0.0.12' VUS=8000 ./bench/run.sh 10.0.0.5 3000 --user bench
```

The runner copies the release binary and tokens to every host and checks that
the binary runs there. All SSH sessions must be ready before a shared future
start; host clocks must be synchronized. Hosts must support
the local binary's OS, architecture, and runtime. SSH/SCP use keys or your agent
and normal known-host checks. `VUS` is the total across hosts, split evenly with
distinct user offsets; it must be at least the host count. Remote generators
use their OS's source-address selection.
Use `VUS`, `RAMP_UP`, and `DURATION` in SSH mode; Goose overrides for users,
test plans, run time, hatch rate, startup time, iterations, and autostart are rejected.

Ctrl-C or SIGTERM stops local and remote generators, allowing 10 seconds for graceful
cancellation before terminating them if needed, and collects available outputs.

Every run gets a new `../results/run-<timestamp>-<pid>/` directory. Choose another
with `--output-dir DIR`; it must not already exist. It contains `aggregate.json`
and a `local/` or per-host directory with `summary.json`, exact `samples.json`,
and `output.log`. Remote result directories, including native reports, are copied
back recursively. Relative native report/log paths resolve inside each generator's
result directory. Set `SUMMARY` for an additional copy of the aggregate.

Users cycle through tokens, wait 0–5 seconds initially, then repeat: fetch the
feed, wait 3–7 seconds, fetch a random post, wait 3–8 seconds, like it with 15%
probability, create a post with 2% probability, and idle 5–15 seconds.

## Scoring and the k6 differences

Use `aggregate.json` and the final PASS/FAIL. Whole-run thresholds, including
shutdown: p95 **< 500 ms**, p99 **< 1,000 ms**, HTTP failures **< 1%**.
Failed requests include transport errors, incomplete bodies, and statuses outside
200–399. Endpoint checks are diagnostic, as in k6. Empty and interrupted runs
fail; runtime errors must be zero. Remote generators must overlap at full load
for `DURATION`, allowing 250 ms scheduling tolerance; a missed start or insufficient
common hold fails. Exit status: 0 PASS, 1 FAIL, 2 setup/execution/output error.

- Latency includes the complete body and connection setup. k6 excludes
  DNS/connect/TLS setup; new connections can measure slower.
- Scoring p95/p99 use k6's interpolation over combined unrounded samples from
  all endpoints and hosts; per-host percentiles are never averaged.
  Goose's native header timings and rounded percentiles are diagnostic.
- During normal shutdown, Goose finishes an active browsing iteration; k6 allowed 10
  seconds before interrupting it. Shutdown can take longer here, and `duration_s`
  includes that time.
