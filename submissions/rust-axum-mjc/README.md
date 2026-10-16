# Rust / Axum submission

The five challenge endpoints use a native Hyper service with Axum request
extraction, a current-thread Tokio runtime, and one dedicated SQLite worker.
On Linux, idle TCP connections keep a standard socket and their service handle
in a shared map. One epoll instance reports activity through a single Tokio
registration. Hyper state, a Tokio socket registration, and a task exist only
while a connection is active. After Hyper has finished the request and flushed
the response, the socket and service return to idle storage. Partial requests,
pipelined input, pending handlers, and unfinished writes stay active.
Each accepted connection receives a token that is never reused, so a stale
readiness event cannot select a new connection that reused its file descriptor.
Its one-shot epoll entry stays disabled while the socket is active. Returning to
idle rearms the same entry with MOD, which checks readiness again after any
temporary Tokio registration has ended. Closing a socket removes its entry.
The idle map's key holds the token; only active sessions carry another copy.
Active sessions have separate Tokio tasks and scheduling budgets. Readiness
batches contain at most 1,024 events; the server does not scan idle sockets.
There are at most 8,192 active sessions and one shared epoll descriptor. Active
entries retain kernel storage until close or shutdown.
Hyper uses Tokio sockets through its native adapter while active, avoiding a
second transport buffer and boxed completion-I/O futures for reads and flushes.
The vendored Hyper 1.12.0 starts HTTP/1 reads at 1 KiB and allocates response
header storage when needed. Its configured minimum and default maximum remain
8 KiB and 417,792 bytes; request-size limits are unchanged. The upstream source
and local patch are documented in [vendor/hyper/LOCAL-PATCHES.md](vendor/hyper/LOCAL-PATCHES.md).
A bounded queue keeps database
work off the HTTP event loop. Up to 4,096 jobs occupy channel slots, with at most
1,024 dequeued jobs in either a mixed write batch or a read batch. Collection ends
when the batch fills or 1 ms after its first request. Read batches also stop at
the next write.
Requests wait up to 1 second, the benchmark p99 ceiling, to acquire a channel
slot before receiving HTTP 503. Execution adds to that admission wait, so the
ceiling is an overload cutoff rather than a latency guarantee.
This admission timeout does not cancel database work that has already been
queued. Canceled jobs are skipped before their SQL starts. SQLite uses the supplied schema and indexes, WAL,
NORMAL synchronous mode, and cached prepared statements. Responses read current transaction data; writes commit before returning success. JWT signatures are verified
on every authenticated request.
Every request executes its own SQLite query, including feed requests in the
same batch. Read batches retain prepared transaction-control statements. A failed
commit rolls back before replies; if cleanup leaves a transaction open, the
worker closes that connection.

On Unix, both database connections use SQLite's
[unix-excl filesystem backend](https://www.sqlite.org/vfs.html). The temporary
index that locates pages in the WAL lives in process memory, so updating it
avoids filesystem write faults in the `-shm` file. The SQL worker and checkpoint
thread retain separate connections. Other SQLite processes must wait until the
server releases the database lock. The database and WAL remain on disk, with
NORMAL synchronization and successful write responses sent after commit.

When the worker reaches a write, it collects up to 1,024 requests within the
1 ms window and processes them in queue order in one transaction. Intervening reads share
that transaction, and their responses wait for the same commit as the writes.
Per-write savepoints isolate failed writes, while a failed group commit rolls back and
fails the affected replies. Canceled requests are skipped before execution;
canceling a response after execution does not undo its write.

SQLite's automatic WAL checkpoint threshold is disabled. A dedicated worker
checks once per second and starts a PASSIVE checkpoint only after the database
worker has done no batch work for a full interval. It skips further checkpoints
until more work arrives if the previous one copied all committed WAL frames.
This moves checkpoint I/O out of sustained traffic. New requests can still
arrive while a checkpoint is running. The WAL can grow during continuous load.
With `journal_size_limit=0`, SQLite discards its unused tail on the first commit
after a checkpointed WAL resets. Later writes extend the file, avoiding reads
of cold old blocks needed for partial-page overwrites.
Read transactions end before responses are sent, so a slow client does not
keep a database snapshot open.

Idle connections return read buffers, queued write-header vectors, and cached
header maps to bounded per-thread pools for reuse by active connections. Each
pool retains at most 128 items; byte buffers larger than 8 KiB and header maps
with capacity above 32 are discarded. Small empty reads use a stack buffer until
data arrives; partial requests and pending writes retain their bytes.
Server-only builds keep header maps in these pools without reserving a header
cache in every connection. The bounded pools last for the runtime's lifetime,
so parking the last active HTTP session does not discard reusable buffers.
The idle connection map releases its storage after all connections drain.

HTTP response bodies use immutable `Bytes`, taking ownership of serialized
vectors without copying.

Rust allocations use jemalloc with background decay. SQLite's C allocations
keep the system allocator.
The vendored Tokio 1.53.2 reserves 64 KiB for its current-thread ready queue
when the runtime starts. The queue can still grow; the initial reserve avoids
growing it among connection allocations during load. See
[vendor/tokio/LOCAL-PATCHES.md](vendor/tokio/LOCAL-PATCHES.md).

Post text, timestamps, and authors borrow SQLite row values while serializing.
Single-post and fallback feed queries write directly into response bytes.
Unescaped request bodies reuse the HTTP buffer;
escaped bodies reuse their decoded allocation. No post or token cache is used.
Fixed JSON keys are appended as static bytes; serde_json encodes the values.
The dispatcher keeps Hyper's concrete request body for routes that do not read
it. Only post creation wraps the body for Axum's byte extractor and size limit.
Response allocation sizes reuse the observed capacity for each query type.
Each feed request discovers the latest 20 post IDs in timestamp and ID order.
When their checked ID span is below 256, prepared range queries count likes in
SQLite and read joined posts through the existing indexes. The response restores
the selected ID order. Sparse spans, arithmetic overflow, and missing author
joins use the original feed query, whose limit follows the join. All queries use
the request's current transaction; no result survives between requests.
The range path serializes into worker scratch storage, then copies those bytes
into the response. Scratch contents are cleared on success, fallback, and error;
at most 64 KiB of capacity is retained.
The supplied schema and indexes are left unchanged. The request connection
targets a 32 MiB SQLite page cache by default. Successful likes rely on SQLite's
foreign-key checks; post existence is queried only for duplicates and foreign-key
failures.

Dependencies: Rust stable, Axum 0.8.9, Mio 1.2.4, rusqlite 0.40.2 with bundled SQLite,
jsonwebtoken 11.1.0, Divan 0.1.21, Gungraun 0.20.0, and Mold 3.0.0.
The bundled SQLite build disables optional global page-cache memory management
through shared Cargo configuration, including the contest build.
Private cache groups avoid its shared mutex while SQLite thread safety remains
enabled; keeping pages per connection may increase memory use.

## Run

On the Ubuntu challenge image, run `sudo bash install.sh` once, then:

```sh
bash build.sh
SQLITE_PATH=/path/to/feed.db JWT_SECRET=twelve-dollar-challenge \
  HOST=0.0.0.0 PORT=80 bash start.sh
```

`SQLITE_PATH` must name an existing database. `JWT_SECRET` is required. The
local defaults are `HOST=127.0.0.1` and `PORT=3000`. The foreground server handles
SIGINT and SIGTERM. Accepted sockets enable TCP_NODELAY through Tokio's safe API.
Set `HOST=0.0.0.0 PORT=3100-3105` to listen on all six ports in one process.
`HOST` also accepts comma-separated IPv4/IPv6 addresses and inclusive IPv4
ranges, such as `127.0.0.[1-5]` or `127.0.0.1-127.0.0.5`. `PORT` accepts
comma-separated ports and inclusive ranges. Every requested IP/port combination
is bound, with duplicates removed in input order. IPv6 addresses can be listed
individually. Expansion is limited to 65,536 socket addresses.
All listeners must bind successfully before serving requests. They share one
database worker, application state, and graceful shutdown. HTTP/1.1 connection
reuse follows the request's normal `Connection` semantics; the server does not
retire keep-alive connections based on connection counts or descriptor limits.

The profiling tasks can request a range with `BENCH_PORT=3100` and
`BENCH_PORT_END=3105` in the private benchmark config. The load generator and
health probe use the first port. A single `BENCH_PORT` keeps one listener.

On Linux, HTTP/1 vectored writes borrow the queued header and body buffers in
one syscall when possible. Partial and pending writes keep those buffers until
the bytes drain; the header vector then returns to the bounded pool. Pipeline
flushing is disabled. Other platforms keep scalar writes. This write strategy
has not yet been compared with scalar writes under equivalent load. `READ_BATCHING`
accepts `true` or `false` and defaults to `true`.
`SQLITE_MMAP_SIZE` sets the request connection's mmap size in bytes and defaults
to `0`; startup reports both the requested and effective sizes.
`SQLITE_PAGE_CACHE_KIB` sets the request connection's page-cache target in KiB
and defaults to `32768` (32 MiB). Set it to `65536` for a 64 MiB target.
It must be positive, fit a signed 32-bit KiB value, and cover between one and
one billion database pages. Startup reports the selected target.
`SQLITE_TIMINGS=true` records queue waits and timeouts, batch sizes and durations,
slow commits, and checkpoint timing. It is disabled by default.

## Development

Use an up-to-date Rust stable with rustfmt and Clippy. The instruction benchmarks
require Gungraun runner 0.20.0 and Valgrind. Local shell setup and profiling orchestration
are maintained outside this submission in the shared development configuration.

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo nextest run --locked
mkdir -p target/test-runs
TMPDIR="$PWD/target/test-runs" bash ../../test/run.sh .
```

Tests cover endpoint status codes and response shapes, authentication, input
validation, JSON escaping, durable writes, transaction rollback and cancellation,
queue admission, batch limits and feed snapshots, checkpoint behavior, and
HTTP keep-alive, pipelining, HEAD responses, and graceful shutdown.

## Benchmarking and profiling

```sh
cargo bench --locked --jobs 1 --bench micro
cargo bench --locked --jobs 1 --bench instructions
```

The microbenchmarks measure token verification, health, feed, post lookup, and
likes against a fixed database fixture. Divan reports elapsed time; Gungraun
reports instruction counts across the caller and SQLite worker.

With the standalone benchmark repository checked out at
`../twelve-dollar-benchmark`, run its existing launcher from the challenge
repository root. Set the token file and both result destinations explicitly:

```sh
TOKENS="$PWD/seed/tokens.json" \
SUMMARY="$PWD/results/benchmark-summary.json" \
  ../twelve-dollar-benchmark/run.sh 127.0.0.1 3000 --users 1000 \
  --output-dir "$PWD/results/run-$(date -u +%Y%m%dT%H%M%SZ)-$$"
```

Run the load generator on a separate machine for capacity measurements.
For CPU or heap profiling, `cargo build --locked --profile profiling --jobs 1`
retains full DWARF symbols with the release optimization settings. Rust and
bundled SQLite preserve frame pointers. Run perf, heaptrack, or Pyroclast against
that executable; deployment and recording scripts live in the shared devshell.

### Ubuntu VM comparison

These 125,000-user runs used Ubuntu 24.04 with one vCPU, 2 GiB RAM, no swap,
ten listeners, a 32 MiB SQLite cache, and mmap disabled. Each used a fresh seed,
a 60-second ramp, 60-second nominal user hold, 30-second drain and 10-second
grace period, with the normal observer and 99 Hz CPU profile.

| Build | Run p95 / p99 | HTTP errors | Hold app CPU | Hold whole-VM CPU |
| --- | ---: | ---: | ---: | ---: |
| Before (`1ae2cc4`) | 173.23 / 1,363.31 ms | 5,144 (0.346%) | 52.82% | 78.16% |
| Three changes together (`85bde84`) | 3.64 / 17.95 ms | 0 | 37.37% | 64.58% |

Hold throughput was about 13,210 and 13,206 requests/s respectively. Reported
hold CPU and throughput imply about 40.0 versus 28.3 microseconds of app CPU per
request. The three changes were vectored response writes, the range feed query,
and persistent epoll entries. This is one run per build, with no measurement of
each change separately. The earlier run followed a ladder of lower loads; the
new run started from a clean VM. The earlier errors coincided with I/O wait
during ramp, so this comparison does not establish that the storage stall is
fixed. These one-minute holds do not establish the five-minute contest result.

Continuing that build in 5,000-user steps produced:

| Users | Observer | Result | p95 / p99 | HTTP errors | Hold whole-VM CPU |
| --- | --- | --- | ---: | ---: | ---: |
| 170,000 | Original | Pass | 476.42 / 537.59 ms | 0 | 92.35% |
| 175,000 | Original | Fail: p95 above 500 ms | 553.64 / 727.06 ms | 0 | 92.44% |
| 175,000 | Separate, slower descriptor census | Pass | 433.79 / 569.73 ms | 0 | 71.22% |

The observer used about 28% of the VM's only CPU in both runs, already included
in the whole-VM totals. Each census dumped the socket table and resolved every
process descriptor, taking about 645 ms of CPU per cycle at 175,000 users.
The repeat kept the server executable and generator unchanged. Observer CPU fell
to 5.91%, with CPU/RSS sampling still at 1 Hz and exact descriptor/socket counts
reported only for newly completed scans. It passed without errors, but the
remaining high p95 means census bursts may still affect latency. The retry used
a clean VM rather than the earlier ladder's disk history; this is one comparison,
not proof that all latency or storage stalls are fixed.

### Earlier capacity measurements

The same optimized binary and ten listeners produced:

| Connections | SQLite cache | Hold requests/s | p95 / p99 | Errors | Peak server RSS | Hold CPU, one core = 100% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 400,000 | 256 MiB | 42,195 | 5.50 / 25.77 ms | 0 | 244.22 MiB | 96.14% |
| 450,000 | 256 MiB | 47,509 | 4.71 / 50.13 ms | 0 | 329.65 MiB | 106.86% |
| 500,000 | 32 MiB | 52,635 | 74.05 / 564.41 ms | 0 | 191.44 MiB | 130.02% |

All runs completed ramp, hold, and drain without errors or OOM. Each used a
60-second nominal user hold; initial user think time delays reaching the full
TCP count. CPU, memory, and I/O profilers were on.
The application cgroup limit was 2,147,483,648 bytes, including kernel memory,
socket buffers, and file cache. These measurements do not establish the
five-minute, whole-system 2 GB / one-vCPU contest result. In particular, the
450,000- and 500,000-connection runs used more than one core while profiled, and enforcement
of the configured CPU quota was not verified.
