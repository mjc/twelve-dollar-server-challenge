# Local Hyper patch

This directory contains the published Hyper 1.12.0 crate, licensed under MIT.
The upstream license and notices are retained.

- Upstream source: https://github.com/hyperium/hyper/tree/v1.12.0
- Upstream commit: `7f67699ce32d717081deb7018b4da7d679277ac0`
- Published crate SHA-256: `2c3e324da4c95177d6291d4c8730197c0d1822f8a9766814a4a44fa5ab797c9c`

HTTP/1's initial adaptive read size and reusable header buffer capacity are
1,024 bytes instead
of 8,192. Adaptive reads retain their growth behavior and decrease to the new
initial size. The configured minimum remains 8,192 bytes and the default maximum
remains 417,792 bytes. Empty header buffers are retained, as upstream does.

With `writev(false)`, HTTP/1 queues the body's existing buffers and writes each
contiguous chunk with ordinary `poll_write` calls. It avoids copying response
bodies into a connection's retained header buffer. Partial writes and pending
writes advance only the bytes actually written. The vectored strategy remains
available with `writev(true)`; `pipeline_flush(true)` keeps the flattening path.
The scalar queue is implemented in `src/proto/h1/io.rs`, forwarded through
`src/proto/h1/conn.rs`, and selected by both HTTP/1 connection builders.

`src/proto/h1/dispatch.rs` allocates the server's pinned handler future lazily
for an active request and drops it at completion. Idle connections retain an
empty option rather than an allocated slot large enough for the handler future.
This trades one reusable per-connection allocation for an allocation per active
request.

The adaptive-read tests use the new initial size. Added tests check initial read
and write capacities, unchanged maximums, and rejection of a configured maximum
below 8,192 bytes. Additional tests cover scalar-write ordering, partial and
pending writes, zero-write errors, pipeline-flush selection, and handler-future
allocation and cleanup on success, error, and cancellation.
