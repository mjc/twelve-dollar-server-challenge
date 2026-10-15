# Local patches

This directory vendors Tokio 1.53.2 from the crates.io source package. The
upstream source, manifest, documentation, and license are preserved.

## Current-thread scheduler queue capacity

`src/runtime/scheduler/current_thread/mod.rs` raises `INITIAL_CAPACITY` from
64 to 8,192 `Notified` entries. At 8 bytes per entry, this reserves 64 KiB
when Tokio constructs the current-thread scheduler core. This moves the
allocation to runtime startup for this service. The queue remains a
`VecDeque` and can grow beyond the initial capacity; this is not a hard task
limit and does not discard wakeups.

The source package version is pinned by the application's exact Tokio
dependency (`=1.53.2`). The local override is wired in the root
`[patch.crates-io]` section.
