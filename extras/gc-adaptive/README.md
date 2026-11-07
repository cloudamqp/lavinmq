# Adaptive GC experiments (stock bdwgc 8.2.6, not the patched libgc)

Experimental, env-gated instrumentation in `src/lavinmq/gc_tuner.cr`:
GC pause accounting via `GC_set_on_collection_event`, exposed under
`/api/nodes/gc_stats` → `tuner`, plus:

- `LAVINMQ_GC_MIN_BYTES=N` static `min_bytes_allocd` floor
- `LAVINMQ_GC_AUTOTUNE=PCT` feedback loop resizing the floor to hold GC time near PCT of wall time

Harness: broker pinned to cpus 0-1, lavinmqperf to 2-3, `GC_UNMAP_THRESHOLD=1`.
`run.sh VARIANT SCENARIO REP` (`BIG=1` pre-creates 10k queues + 2k consumers),
`markers.sh CPUSET MARKERS LOAD REP` measures full-GC pause per marker count.

Findings (4 vCPU, Crystal 1.22.2):

- GC frequency knobs (free space divisor 1/6, 8/64 MiB floors, autotune) change
  collection counts 10-90x but throughput stays within noise; GC is 2-5% of
  wall time. Autotune only traded heap for nothing.
- Pause duration is set by live heap and marker threads. bdwgc sizes markers
  from online CPUs, ignoring affinity/cgroup quota. With a ~140 MB heap,
  1 marker gives ~22-39 ms pauses vs 45-73 ms with the default 4.
- Not yet rerun on the patched libgc (experimental-gc-grow-mark-stack).
