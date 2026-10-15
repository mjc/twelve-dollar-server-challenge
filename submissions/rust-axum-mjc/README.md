# Rust / Axum submission

The five challenge endpoints use a native Hyper service with Axum request
extraction, a current-thread Tokio runtime, and one dedicated SQLite worker.
The connection task maps completion directly onto error logging, avoiding an
async wrapper that duplicated the stored connection future. Hyper uses Tokio sockets directly
through its native adapter, avoiding a second transport buffer and boxed
completion-I/O futures for reads and flushes.
The vendored Hyper 1.12.0 starts HTTP/1 read and header buffers at 1 KiB and
grows them as needed. Its configured minimum and default maximum remain
8 KiB and 417,792 bytes; request-size limits are unchanged. The upstream source
and local patch are documented in [vendor/hyper/LOCAL-PATCHES.md](vendor/hyper/LOCAL-PATCHES.md).
A bounded queue keeps database
work off the HTTP event loop. Up to 4,096 jobs occupy channel slots, with at most 255 dequeued jobs in either a mixed
write batch or a read batch.
Requests wait up to 1 second, the benchmark p99 ceiling, to acquire a channel
slot before receiving HTTP 503. Execution adds to that admission wait, so the
ceiling is an overload cutoff rather than a latency guarantee.
This admission timeout does not cancel database work that has already been
queued. Canceled jobs are skipped before their SQL starts. SQLite uses the supplied schema and indexes, WAL,
NORMAL synchronous mode, and cached prepared statements. Responses read current transaction data; writes commit before returning success. JWT signatures are verified
on every authenticated request.
Within each read-only batch, identical feed reads execute and serialize once
against the transaction snapshot. Replies share immutable `Bytes` without copying their payloads.
The worker retains no result between batches, and the next batch reads current data.
Read batches also retain prepared transaction-control statements. A failed
commit rolls back before replies; if cleanup leaves a transaction open, the
worker closes that connection.

When the worker reaches a write, it freezes up to 255 already-queued requests
and processes them in queue order in one transaction. Intervening reads share
that transaction, and their responses wait for the same commit as the writes.
The worker never waits for additional arrivals to fill a batch. Per-write
savepoints isolate failed writes, while a failed group commit rolls back and
fails the affected replies. Canceled requests are skipped before execution;
canceling a response after execution does not undo its write.

SQLite's automatic WAL checkpoint threshold is disabled. A dedicated worker
attempts a PASSIVE checkpoint when sampled database, WAL, and SHM file sizes
reach 32 GiB or write admission is closed. The first eligible sample triggers
immediately; retries wait 30 seconds. Time alone does not trigger a checkpoint
below the threshold. It does not force a reset or truncate. The app worker
continues serializing
reads and writes while the checkpoint copies pages. A reader can pin frames and
prevent full backfill, so the WAL can grow when checkpointing cannot catch up.
After a full backfill and when readers release their snapshots, SQLite can
reuse the WAL file; its retained capacity is not capped. The connection uses
`synchronous=NORMAL`. Checkpoint I/O can compete with request I/O, and writers
may sync a reused WAL header. Read
transactions end before responses are sent, so clients cannot pin a snapshot
while receiving its response.

A background monitor samples the combined main database, WAL, and SHM file
sizes once per second. At 39 GiB it closes write admission while reads continue,
leaving 1 GiB of
operational headroom under the 40 GiB budget. Writes reopen only after the
sampled total falls below 38 GiB and a checkpoint fully backfills. This is a
sampled operational guard, not a hard disk quota. PASSIVE checkpointing does
not shrink retained WAL capacity, so writes stay closed if pinned readers or
retained files keep the total above the resume threshold; cleanup or restart
may be needed to recover.

Idle connections return read buffers, scalar write-header vectors, and cached
header maps to bounded per-thread pools for reuse by active connections. Each
pool retains at most 128 items; byte buffers larger than 8 KiB and header maps
with capacity above 32 are discarded. Small empty reads use a stack buffer until
data arrives; partial requests and pending writes retain their bytes.

HTTP response bodies use immutable `Bytes`, taking ownership of serialized
vectors without copying. Feed replies within a read batch share those bytes.

Post text, timestamps, and authors borrow SQLite row values while serializing
directly into response bytes. Unescaped request bodies reuse the HTTP buffer;
escaped bodies reuse their decoded allocation. No post or token cache is used.
Fixed JSON keys are appended as static bytes; serde_json encodes the values.
The dispatcher keeps Hyper's concrete request body for routes that do not read
it. Only post creation wraps the body for Axum's byte extractor and size limit.
Response allocation sizes reuse the observed capacity for each query type.
One prepared feed query selects the latest 20 posts with valid authors in
timestamp and ID order, then counts likes through the existing post-ID index.
The same query handles short and sparse feeds without additional ID queries
or a separate range path.
The supplied schema and indexes are left unchanged. The request connection
targets a 256 MiB SQLite page cache. Successful likes rely on SQLite's foreign-key
checks; post existence is queried only for duplicates and foreign-key failures.

Dependencies: Rust 1.99.0, Axum 0.8.9, rusqlite 0.40.2 with bundled SQLite,
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

HTTP/1 vectored writes and pipeline flushing are disabled. `READ_BATCHING`
accepts `true` or `false` and defaults to `true`.
`SQLITE_MMAP_SIZE` sets the request connection's mmap size in bytes and defaults
to `0`; startup reports both the requested and effective sizes.

## Development

Use Rust 1.99.0 with rustfmt and Clippy. The instruction benchmarks require
Gungraun runner 0.20.0 and Valgrind. Local shell setup and profiling orchestration
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

From the challenge repository root, run the standalone load generator through
the upstream launcher:

```sh
VUS=1000 bash bench/run.sh 127.0.0.1 3000
```

Run the load generator on a separate machine for capacity measurements.
For CPU or heap profiling, `cargo build --locked --profile profiling --jobs 1`
retains full DWARF symbols with the release optimization settings. Rust and
bundled SQLite preserve frame pointers. Run perf, heaptrack, or Pyroclast against
that executable; deployment and recording scripts live in the shared devshell.
