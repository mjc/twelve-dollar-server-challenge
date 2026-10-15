# Source history

The branch now tracks runnable source. The upstream history through `63eeda6` is unchanged, including Scooter1337’s author identities and timestamps. The following focused commits replace the earlier patch/evidence branch; each retains its original implementation author identity and date. New reconstruction documentation and CPU analysis use their actual new author date.

| Source change | New commit | Original implementation | Author date retained |
|---|---|---|---|
|01-checkpoint-ports|`f777825`|`e63f209`|2026-10-15T03:07:09-06:00|
|02-diagnostics|`94a4135`|`f3cbd0d`|2026-10-15T04:11:10-06:00|
|03-shutdown|`df4f332`|`f183743`|2026-10-15T04:44:13-06:00|
|04-sql-worker|`b15b7da`|`2d7d0e9`|2026-10-15T06:39:24-06:00|
|05-idle-input|`e9a8e00`|`09f30fc`|2026-10-15T07:08:06-06:00|
|06-pager|`f50d7b1`|`3c56644`|2026-10-15T07:23:40-06:00|
|07-headers|`a3b5a43`|`5d73a45`|2026-10-15T08:03:36-06:00|

Combined source commits retain the first implementation timestamp: quiet checkpoints/input also include `50ffec3` and `64ee7d1`; pager configuration/counters include `ca076ee`. Diagnostic controls consolidate the original tracing/storage/CQ experiments. This history does not imply those combined changes were benchmarked in isolation.

The original branch tip was `c52fa1f5b4b1434d1b46d195e1420dce9135d657`. It remains restorable through local ref `backup/rust-scooter-before-source-rewrite-20261015` and the external `original-history.bundle`. All original author/committer dates, including excluded candidates and evidence-only commits, are preserved in `original-author-timestamps.json`; the rewritten mapping is in `rewritten-author-mapping.json`. The complete backup directory is `/home/mjc/bench/scooter-history-rewrite-20261015`.

The named fork branch currently has newer upstream commit `9321315` after the tested pin. It is intentionally excluded rather than silently changing measured source. The chunk-copy, cache32 and1024-job coalescing experiments likewise remain in the original backup, not this tested source tree.
