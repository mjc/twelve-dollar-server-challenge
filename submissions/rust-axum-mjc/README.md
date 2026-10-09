# Rust / Axum submission

The five challenge endpoints run directly through Axum, with a current-thread
Tokio runtime and one dedicated SQLite worker. A bounded queue keeps database
work off the HTTP event loop. SQLite uses the supplied schema and indexes, WAL,
NORMAL synchronous mode, and cached prepared statements. Every response reads
current data; writes commit before returning success. JWT signatures are verified
on every authenticated request.

Post text, timestamps, and authors borrow SQLite row values while serializing
directly into response bytes. Unescaped request bodies reuse the HTTP buffer;
escaped bodies reuse their decoded allocation. No post or token cache is used.

Dependencies: Rust 1.99.0, Axum 0.8.9, rusqlite 0.40.2 with bundled SQLite,
jsonwebtoken 11.1.0, Divan 0.1.21, Gungraun 0.20.0, and Mold 3.0.0.

## Run

On the Ubuntu challenge image, run `sudo bash install.sh` once, then:

```sh
bash build.sh
SQLITE_PATH=/path/to/feed.db JWT_SECRET=twelve-dollar-challenge \
  HOST=0.0.0.0 PORT=80 bash start.sh
```

`SQLITE_PATH` must name an existing database. `JWT_SECRET` is required. The
local defaults are `HOST=127.0.0.1` and `PORT=3000`. The foreground server handles
SIGINT and SIGTERM. HTTP/1.1 connections have no idle timeout, including when
running behind the challenge's Nginx configuration. Resource limits belong in
the service configuration.

## Development and tests

From this directory, enter `devenv shell`. The environment supplies the Rust
toolchain, Mold 3.0.0, SQLite, Node.js, the challenge's curl/jq/OpenSSL tools,
k6, Divan's matching Gungraun runner and Valgrind, and
[Pyroclast](https://github.com/mjc/pyroclast).

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo nextest run --locked
mkdir -p target/test-runs
TMPDIR="$PWD/target/test-runs" bash ../../test/run.sh .
```

Database tests exercise durable writes, exact JSON order and escaping, timestamp
ties, feed limits, current like counts, idempotent likes, invalid inputs, and
worker errors. Authentication tests cover signatures, HS256 restrictions,
expiration without leeway, and invalid claims. Router tests cover authentication
precedence, Unicode trimming and length, request-buffer borrowing, malformed
JSON and paths, fresh reads, and the required status codes and response shapes.
Fixtures and build artifacts stay inside this submission.

## Benchmarking and profiling

```sh
devenv tasks run --show-output bench:micro
devenv tasks run --show-output bench:k6
VUS=2500 BASE_URL=http://server:3000 devenv tasks run --show-output bench:k6
```

The microbenchmarks measure HS256 verification, SQLite health, feed, post lookup,
and duplicate likes against a fixed populated fixture. Divan reports elapsed
time. Gungraun counts instructions across the HTTP-side caller and SQLite worker;
fixture setup and teardown are outside the measurement. The k6 task uses the
challenge's existing workload and defaults to 1,000 users. Run k6 on a separate
machine for capacity measurements.

```sh
SQLITE_PATH=/path/to/feed.db JWT_SECRET=twelve-dollar-challenge \
  devenv tasks run --show-output profile:cpu
SQLITE_PATH=/path/to/feed.db JWT_SECRET=twelve-dollar-challenge \
  devenv tasks run --show-output profile:memory
```

Profiling retains full DWARF symbols with fat LTO, one codegen unit, and overflow
checks. Rust and bundled SQLite preserve frame pointers. Pyroclast handles CPU
and heap recording; its Linux heap backend tools are supplied by devenv.
Dependencies are pinned in Cargo.lock and devenv.lock. Test, benchmark, and
profiling artifacts are ignored by Git.
