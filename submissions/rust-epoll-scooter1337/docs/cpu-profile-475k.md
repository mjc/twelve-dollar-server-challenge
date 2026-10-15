# CPU profile at 475,000 connections

The unchanged frozen `77412ab7` server held all 475,000 TCP connections for **66.614 seconds**. It completed 6,429,972 requests with zero errors: p50 **0.504 ms**, p95 **1.053 ms**, p99 **12.593 ms**. During the nominal hold it served **50,217 requests/s** and used **59.59% of one core**. CPU cost was approximately **12 microseconds per completed request**; this excludes time spent waiting for I/O.

This repeats the previous run's header24/cache256/mmap0/SQL worker, CQ32/retry1/wait25 settings and fresh seed. The workload remains 60 seconds ramp, 75 seconds hold, 30 seconds drain and 10 seconds grace. No source, executable, application setting or host configuration changed.

## Where CPU time goes

The main analysis covers only the measured full-TCP window: 16:49:18.468741607–16:50:25.082446886 UTC on October 15, 2026. It contains **4,088 samples at 99 Hz**, with no lost samples. Task CPU counters independently agree on the thread split.

| Thread and sampled execution mode | Share of sampled CPU |
|---|---:|
| SQLite worker, user space | 48.61% |
| I/O ring, kernel | 37.55% |
| SQLite worker, kernel | 9.20% |
| I/O ring, user space | 4.65% |

Total: **57.80% on the SQLite worker**, **42.20% on the ring**. These are CPU shares, not latency percentages. Task/cgroup user/system accounting differs from sampled execution modes; both are preserved in the JSON rather than presented as exact cycle counts.

## Hot caller paths

These percentages include descendants and therefore overlap:

- **Database reads: 37.08%** of all sampled CPU. The feed-range path accounts for **30.19%**, within that read path.
- **TCP sends: 24.95%.** The timer cost follows `tcp_sendmsg -> tcp_write_xmit -> tcp_event_new_data_sent -> sk_reset_timer -> __mod_timer`; the timer leaf itself accounts for **3.91%**. Conntrack lookup accounts for **3.47%**, mostly on outgoing packets.
- **Statement reset: 15.56%.** This is mostly underneath COMMIT (**9.59%**) and LIKE writes (**5.14%**). Read/feed resets together account for about **0.22%**; the reset cost should not be attributed to feed-query setup.
- **JSON string generation: 2.05%.** Sending SQL jobs through the channel accounts for **0.49%**.

The largest application area to investigate next is feed/read query execution. Most transport CPU goes into the kernel send/TCP path. This run changes no implementation and makes no optimization claim.

## Measurement limits

Visible recorder tracepoint paths account for **4.43%** of samples, already included above. Other BPF frames include normal networking policy, so they cannot all be subtracted as observer overhead. About **1.15%** of instruction leaves remain unresolved.

Flame-graph widths are sample counts, not wall-time latency. CPU outside the target threads is not fully captured. The configured 2 GiB application cgroup and CPU quota under lavd do not establish whole-system 2 GB/one-vCPU compliance.

Latency was lower than in the preceding run of the same binary and settings. That is repeat variability, not evidence of a code speedup.

## Artifacts and shutdown

The self-contained interactive flame graph and CPU breakdown are preserved with the raw profile:

`/home/mjc/projects/twelve-dollar-benchmark-no-goose/results/tali-scooter-frozen-header24-cache256-475000-20261015T164752Z/profile/`

Key files are `flamegraph-steady.html`, `cpu-steady.json`, `cpu-callpath-details.json`, `perf.data` and `analyze-cpu-steady.py`. The generator report is in the adjacent `run/` directory.

The frozen executable SHA is `77412ab79a14ee3a03b9d4d50173f58397e1be5bcb986caca278daa9a0dcfb2d`. Its 13 source-manifest hashes match the reconstructed tested source after the checksum-pinned SQLite preparation step.

After generator/observer cleanup and worker resumption, all five recorder identities and the application identity were verified and stopped. READY was invalidated and the ports released at 16:53:09.610128 UTC. No additional run was started.
