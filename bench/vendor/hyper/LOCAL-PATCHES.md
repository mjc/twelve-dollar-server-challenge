# Local Hyper patch

This directory contains the published Hyper 1.12.0 crate, licensed under MIT.
The upstream license and notices are retained.

- Upstream source: https://github.com/hyperium/hyper/tree/v1.12.0
- Upstream commit: `7f67699ce32d717081deb7018b4da7d679277ac0`
- Published crate SHA-256: `2c3e324da4c95177d6291d4c8730197c0d1822f8a9766814a4a44fa5ab797c9c`

Only `src/proto/h1/io.rs` differs from the published source. HTTP/1's initial
adaptive read size and reusable header buffer capacity are 1,024 bytes instead
of 8,192. Adaptive reads retain their growth behavior and decrease to the new
initial size. The configured minimum remains 8,192 bytes and the default maximum
remains 417,792 bytes. Empty buffers are retained, as upstream does.

The adaptive-read tests use the new initial size. Added tests check initial read
and write capacities, unchanged maximums, and rejection of a configured maximum
below 8,192 bytes.
