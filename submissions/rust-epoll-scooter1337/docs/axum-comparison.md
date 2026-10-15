# Axum ideas compared with Scooter

Read-only comparison of final Axum `cde146a` and Scooter baseline `c509cdc`, using sem entity/context results, scoped source and signed commit diffs. Three agents separately inspected transport/HTTP, SQLite, and allocation/build/history. Some Axum graph-context lookups stalled; scoped entity results and explicit source reads supplied the missing context.

The actual checkout475k baseline puts 58.13% of sampled CPU in SQLite and 41.87% in the ring; ring userspace alone is 5.29%. Inclusive read/feed/COMMIT callers are 38.59% / 31.20% / 9.31%. Percentages overlap. Prioritize measured missing work rather than transplanting Tokio/Hyper into raw io_uring.

| Axum commit | Change | Scooter equivalent or remaining opportunity |
|---|---|---|
|`0c73d53`|Dedicated SQLite owner|Already present: ordered `sqlite-requests` worker; ring recycles receive buffers before SQL wait.|
|`cd2c05b`|Typed borrowed JWT claims|Borrowed RawValue/Cow and reusable decode buffers already present. Escaped usernames still allocate on likes; small unmeasured opportunity.|
|`e73576e`|Borrow row strings and serialize directly|Already present, including SIMD-assisted escaping; no intermediate JSON tree.|
|`2eee937`|Mixed SQL batches; separate checkpoint connection|Architecture already present. Collection policy differs.|
|`345ed39`|Native HTTP; reusable headers; shared listeners|Already direct parser/literal headers. **All 3100–3109 are bound**, with one multishot direct-descriptor accept per listener. All ten health checks and generator destination-port configuration are recorded. Equal per-port accept distribution was not measured.|
|`3103a2e`|Vendor Hyper|Not applicable to httparse/raw io_uring.|
|`6f939c3`|Lazy buffers and handler; scalar body queue|Lazy24 B client/optional48 B headers and shared provided receive buffers already present. SQL reply is still copied into output; ownership-preserving scalar/scatter writes are a separate candidate, with partial-send/pipeline/CQE lifetime risks.|
|`767bacd`|Bound retained idle buffers|Already present: at most 256 buffers of at most 16 KiB, released at safe idle boundaries.|
|`7083b31`|Disable optional global cache grouping; simplify likes|Scooter never enables SQLite memory-management grouping. Its INSERT SELECT WHERE EXISTS still adds a successful-like lookup beyond FK checks; unlike Axum INSERT VALUES. Separate small query candidate; duplicate/missing/invalid-user behavior must remain correct.|
|`d09be7e`|Strategy docs/request benchmarks|Evidence tooling, not runtime optimization. Existing Scooter tests/workload retained.|
|`2f8035b`|Up to1024 mixed jobs within one 1 ms deadline|Missing: baseline takes at most 32 immediately available jobs, averaging 7.5 jobs/transaction across the run. First focused candidate.|
|`e286a31`|Idle activity-generation checkpoint; remove gates; per-request SQL|Already present: stable even generation/full quiet 1 s before PASSIVE; no physical-size gate or shared query-result cache. Axum also suppresses repeated caught-up idle checkpoints; Scooter lacks that small idle-only optimization.|
|`9b862e7`|Remove idle HeaderMap field; tighten pool lifetime|No HeaderMap/future on Scooter idle connections. Existing lazy headers and bounded pool address the ownership goal.|
|`ff42fbd`|Rust jemalloc/background decay|No equivalent allocator-retention problem established on current Scooter. Do not copy Axum's remedy for its measured451 MiB free glibc heap. SQLite remains glibc in Axum too.|
|`4432865`|Early growable64 KiB Tokio queue|No Tokio scheduler in Scooter. Not a transferable allocation.|
|`4ae28b7`|Discard unused physical WAL tail on reset|Conditional only: Scooter fresh seed/growing active WAL differs from Axum restored34 GB cold tail. Current dirty-writeback stalls do not prove old-tail read-modify-write. Does not cap active WAL.|
|`1433b5a`|SQL/allocation documentation|No runtime change.|
|`d1f46be`|Remove outer future wrapper; save 256 B|No boxed Hyper service future to shrink.|
|`fb75aec`|Compact idle ownership/event readiness/independent task budgets|Already achieved with24 B raw-ring client state and continuous multishot receives. No idle task, Hyper dispatcher, ScheduledIo or repeated epoll registration.|
|`251a5d7`|400k/450k evidence|No runtime change.|
|`596b7be`|Cache64 targets proven live pager allocation|Budgeting principle applies; settings do not transfer automatically across DB workloads.|
|`7795146`|Cache32 reserves transient/file-fault margin|Scooter's own earlier32 MiB experiment failed latency despite saving memory. Preserve 256 MiB for this comparison.|
|`fb91286`|Active task cap8192 before readiness consumption|Scooter bounds 65,536 queued/executing/unconsumed-reply jobs, but waiting clients retain input and accepts continue. Count is not a byte bound. A separate measured admission/byte-budget experiment may help under stalls.|
|`cde146a`|Register Tokio sockets only on WouldBlock|Scooter already avoids idle-to-active epoll ADD/DEL via persistent raw io_uring operations; no comparable epoll_ctl hotspot.|

Additional differences: Axum stores numeric response-capacity hints; Scooter starts each SQL reply at 512 B. Hints can avoid repeated Vec growth without caching data, but allocator/JSON costs are currently small. Axum skips cancelled queued requests; Scooter checks connection generation after SQL completion. Axum verifies failed COMMIT/ROLLBACK recovery before reusing its connection; Scooter currently ignores failed rollback. Axum has signal-driven graceful drain; Scooter lacks that stop-admission/worker-drain protocol. These are separate ownership/correctness issues, not benefits delivered by batch collection.

Scooter already uses native CPU, opt3/fat LTO/one codegen unit/panic-abort and default SQLite GCC PGO, THREADSAFE2, global MEMSTATUS0. Axum adds no missing PGO trick. Preserve compiler/profiling flags and actual CPU counters; configured quota is not proof of enforcement under lavd.

## First experiment

Copy only Axum's fixed-deadline mixed FIFO collection: 1024 maximum or 1 ms from first dequeued job, whichever comes first. Keep per-request SQL, cache256/mmap0, checkpoint/WAL policy, admission, transaction-error policy, ports, parser, allocator and seed unchanged. Hold replies through COMMIT. The 1 ms bounds collection, not execution/COMMIT. Larger groups may retain more completed bodies and burst sends.

Record batch-size histogram, collection/execution/COMMIT totals, retained reply capacities, pending/CQ/input ownership, WAL bytes/dirty waits and CPU/request in the same 475k window. Added totals/histogram are cumulative; old queue/COMMIT maxima reset per reporting interval. `response_bytes_max` means summed Vec capacities, not serialized lengths. `execution_us` includes mutex acquisition. A lower commit count is not proof of eliminating per-query work or storage stalls.
