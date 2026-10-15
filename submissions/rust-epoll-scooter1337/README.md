# Scooter: raw io_uring and SQLite

This is the runnable Scooter implementation used for the frozen `77412ab7` capacity runs. It derives from Scooter1337's [`codex/rust-io-uring-v2`](https://github.com/Scooter1337/twelve-dollar-server-challenge/tree/codex/rust-io-uring-v2), pinned at `63eeda6655a0ba12f763b337545ae36041ffdbf3`. The later upstream `9321315` response-ordering/unknown-route change is **not included or covered by these measurements**.

The source is in `src/`, with pinned Cargo dependencies and checksum-verified SQLite 3.53.4. No Axum, Tokio, Monoio or HTTP framework is used. The historical directory name is retained; raw io_uring is the default transport and `--epoll` retains the upstream comparison path.

## Build and run

On the challenge's Debian/Ubuntu host, from this directory:

```bash
sudo bash install.sh
bash build.sh
SQLITE_PATH=/path/to/feed.db JWT_SECRET=twelve-dollar-challenge \
  HOST=0.0.0.0 PORT=8080 bash start.sh
```

`build.sh` and `rust-toolchain.toml` pin Rust 1.99.1, downloads SQLite with a fixed archive checksum, and builds release/native/fat-LTO with synthetic SQLite PGO. Generated `vendor/` and `target/` are ignored. Native compilation requires GCC and Linux with io_uring enabled. The measured executable used Rust 1.99/GCC 16.2 with profiling symbols/frame pointers; source identity is recorded in [evidence/provenance.json](evidence/provenance.json). A different compiler/CPU does not reproduce that executable hash.

For the measured ten-port configuration, set `PORT=3100-3109`, `SCOOTER_CQ_LIMIT=32`, `SCOOTER_REARM_AFTER_DRAIN=1`, and run `target/release/twelve-rust-poll --uring-wait-us=25`. These are benchmark settings, not changes to the schema or request workload. Direct descriptor tables adapt to the inherited hard NOFILE limit; a small ordinary FD count does not remove kernel socket/file/inode memory.

On NixOS use the repository's existing devenv workflow (or the shared benchmark devenv), with this submission as the working directory. Run Cargo directly inside that environment after SQLite source preparation; `build.sh` intentionally selects the challenge's installed Rust toolchain.

## Ownership and storage

Each io_uring issuer consumes direct-accept/receive/send completions. A client slot is 24 bytes; input/output/flight buffer headers are allocated only while active. Payloads remain stable through send completion. Shutdown retains fixed-slot ownership until its CQE, and generations reject stale events.

A dedicated SQL worker owns ordered batches of up to 32 jobs. The ring copies parsed SQL fields, recycles provided receive buffers, and continues consuming transport completions while SQLite blocks. Replies are released after COMMIT, or replaced after rollback. A global 65536-job bound includes queued, executing and unconsumed replies; excess requests backpressure through owned input. It is a count bound, not a byte quota.

SQLite uses WAL/NORMAL, foreign keys, normal locking, SQLite compiled with `SQLITE_THREADSAFE=2`, a 256 MiB cache target and effective database mmap 0 (read back at startup). Disabling database mmap avoids repeated mapped-page WAL-index searches; the WAL index itself remains mapped. A separate connection attempts PASSIVE checkpoints after a complete quiet SQL interval. New traffic can overlap an idle-started checkpoint and continuous traffic can grow WAL. There is no application result cache, schema/index change, host tuning or CPU affinity.

The unbenchmarked 1024-job coalescing, cache32 and separate chunk-copy candidates are excluded. Remaining latency tails include SQL WAL writes throttled by dirty-page writeback; the HTTP passes do not mean those stalls are eliminated.

## Checks and measurements

From the challenge root, `bash test/run.sh submissions/rust-epoll-scooter1337` runs the official 42 API checks. From this directory, `cargo test --release --locked -- --test-threads=1` runs focused ownership/checkpoint tests; two upstream tests require a disposable seeded database and are ignored by default.

The branch-added tests cover fixed-slot shutdown/reuse, ten-port parsing, blocked-SQL receive-buffer recycling, half-closed pipelines in one or two receives, cross-ring capacity wakes, closing SQL waiters, quiet checkpoint generations, and small-input allocation. They exercise resource lifetime and ordering rather than merely copying the implementation.

| Frozen run | Requests | Errors | p95 | p99 | Full TCP observation |
|---|---:|---:|---:|---:|---|
|475,000 users|6,411,805|0|191.47ms|747.80ms|67.010s at all 475k|
|500,000 users|5,963,637|0|123.41ms|645.81ms|60 s nominal VU hold, shorter all-TCP interval|

[475k](evidence/capacity-475k.json) and [500k evidence](evidence/capacity-500k.json) record settings, exact timestamps and raw artifact locations. [CPU profile](docs/cpu-profile-475k.md) analyzes a separate same-build 475k repeat.

These are diagnostic runs with an application 2147483648-byte memory cgroup and observers excluded. They do not certify whole-system 2 GB/one-vCPU limits or the contest's longer sustained duration. The configured CPU quota was not proven enforced under sched_ext/lavd; report actual CPU separately.
