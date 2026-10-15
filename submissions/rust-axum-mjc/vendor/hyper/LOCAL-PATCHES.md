# Local Hyper patch

This directory contains the published Hyper 1.12.0 crate, licensed under MIT.
The upstream license and notices are retained.

- Upstream source: https://github.com/hyperium/hyper/tree/v1.12.0
- Upstream commit: `7f67699ce32d717081deb7018b4da7d679277ac0`
- Published crate SHA-256: `2c3e324da4c95177d6291d4c8730197c0d1822f8a9766814a4a44fa5ab797c9c`

HTTP/1's initial adaptive read size is 1,024 bytes instead of 8,192. Adaptive
reads retain their growth behavior and decrease to the new initial size. The
configured minimum remains 8,192 bytes and the default maximum remains 417,792
bytes. The reusable write-header vector starts without a per-connection
reserve. The existing HTTP/1 encoders reserve an estimate of 30 bytes plus 30
bytes per header on first use; the vector can grow further for actual headers
or flattened pipeline output and retains capacity for reuse. This removes only
the initial reserve: existing `max_buf_size` enforcement is unchanged, and no
new cap is imposed on retained capacity. The read buffer sizing is unchanged by
this write-side allocation change.

With `writev(false)`, HTTP/1 queues the body's existing buffers and writes each
contiguous chunk with ordinary `poll_write` calls. It avoids copying response
bodies into a connection's retained header buffer. Partial writes and pending
writes advance only the bytes actually written. The vectored strategy remains
available with `writev(true)`; `pipeline_flush(true)` keeps the flattening path.
The scalar queue is implemented in `src/proto/h1/io.rs`, forwarded through
`src/proto/h1/conn.rs`, and selected by both HTTP/1 connection builders.
Scalar body queues leave the lazy write-header vector untouched, avoiding its
previous 1,024-byte per-connection initial reserve.
`BufList` now requests one element of deque capacity on its first queued body;
the application returns `Full<Bytes>` bodies as a single data buffer.
Further streamed buffers still use normal deque growth and preserve ordering.
This is a structural allocation reduction, not yet a measured performance
result.

`src/proto/h1/dispatch.rs` allocates the server's pinned handler future lazily
for an active request and drops it at completion. Idle connections retain an
empty option rather than an allocated slot large enough for the handler future.
This trades one reusable per-connection allocation for an allocation per active
request.

The adaptive-read tests use the new initial size. Added tests check initial read
and lazy write-header capacities, unchanged maximums, and rejection of a
configured maximum below 8,192 bytes. Additional tests cover scalar-write
ordering, partial and pending writes, zero-write errors, pipeline-flush
selection, and handler-future allocation and cleanup on success, error, and
cancellation.

Idle server connections return read buffers, scalar write-header vectors, and
cached header maps to bounded thread-local pools. Each pool retains at most
128 items; buffers above 8 KiB and header maps with capacity above 32 are not
retained. Active connections reuse this storage, avoiding both per-connection
idle capacity and repeated allocation on the normal small-request path.
Server responses return their empty header maps after encoding. Server-only
builds omit the per-connection header-cache field; client caching is unchanged.
Pools retain their bounded storage for the runtime's lifetime. They are not
cleared when the last active HTTP session parks its still-open TCP socket.
Scalar body deques retain their existing single-buffer slot for reuse.
Empty reads at the initial adaptive size use a 1 KiB stack buffer, then acquire
a pooled buffer only when bytes arrive. Larger adaptive reads keep the direct
read path. Read allocation growth is tracked before splitting, so a small tail cannot
return an oversized backing allocation to the pool. Partial headers, pipelined
bytes, and pending writes retain their data. Tests check pool reuse and bounds, empty and partial Pending reads,
partial and completed scalar writes, and cached-header cleanup on keep-alive.

The local fork uses edition 2024 and the application's Rust 1.99 toolchain.

`server::conn::http1::Connection::poll_until_idle` drives an active connection
without shutting down its transport. It returns `true` only at a keep-alive
boundary with no partial or pipelined input, request body, handler future,
response body, or buffered output. `into_parts` then returns the socket and
service so the application can drop HTTP state while retaining the TCP
connection. A later request starts a new Hyper session with that same service.
Closed connections return `false`; errors retain the normal error path.
