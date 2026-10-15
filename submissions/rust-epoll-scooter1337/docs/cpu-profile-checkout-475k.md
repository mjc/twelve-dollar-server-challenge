# Reconstructed checkout: 475k CPU profile

Source `1ac9977`, native Tali executable `16814375ebcddf6497eb8fe84e4852548ee4edceb110a2e5958b0db0eb9eef45`. All 13 runtime inputs match the frozen77412 source manifest. Release/native/fatLTO/default SQLite PGO with Rust/C frame pointers and debug symbols.

One unchanged 60s ramp / 75s VU hold / 30s drain / 10s grace run: **6,427,321 requests, zero errors**, p95 **1.139ms**, p99 **472.295ms**. Hold50,219.38 requests/s and measured59.56% of one core. Observed full475k TCP65.759s; nominal VU hold75.000s. Peak app RSS106,672,128B; no OOM. Direct descriptors make ordinaryFDcount20 unsuitable for counting TCP sockets.

The exact full-TCP window has4,048 CPU samples, about12.39µs sampledCPU per completed request. SQLite owns58.13% of sampledCPU and the ring41.87%; task counters independently give58.04%/41.96%.

| Inclusive caller | Share of all sampled CPU |
|---|---:|
| SQLite read |38.59%|
| Feed range query (inside read) |31.20%|
| TCP send |23.86%|
| Statement reset |14.53%|
| COMMIT |9.31%|
| JSON escaping |2.00%|

Caller percentages overlap; they must not be summed. Ring kernel CPU is36.59% of total, ring userspace5.29%. Exclusive SQLite VM6.69%/BtreeTableMoveto6.37%; conntrack lookup3.56%, TCP timer rearm3.48%.

A drain COMMIT reached1.042s, SQL queue1.043s/pending37,434, with slow WAL writes. First checkpoint17:21:17.236Z followed workload finish17:21:16.096Z. CPU samples measure running work, not off-CPU latency; no per-request stall attribution is claimed.

Artifacts: `/home/mjc/projects/twelve-dollar-benchmark-no-goose/results/tali-scooter-actual-1ac9977-header24-cache256-475000-20261015T171510Z/profile/` includes rawperf, exact symbols, kallsyms, CPU/memory/storage/slab/CQ recordings, analysis script output and interactive `flamegraph-steady.html`. Details in `../evidence/cpu-profile-checkout-475k.json`.

One stale shared Cargo artifact was rejected by cache-size startup verification before load. The fresh target was cleaned after preserving the executable (217.4MiB removed). No rebuild speedup claim: source/runtime settings are unchanged. All servers/recorders stopped and ownership released. Actual quota enforcement and whole-system resource certification remain separate from this diagnostic pass.
