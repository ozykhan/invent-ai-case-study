# Superseded: first 1 vs 4 replica run (2026-09-25)

The two result documents in this folder are kept for the record only. Do not use their numbers.

They were recorded with `load browse --max-page 5` against the 200-product seed, where each category holds 50
products, so only pages 1 to 3 exist at 20 items per page. About 28% of requests asked for a page past the end.
At the time, a cache reader of an empty past-the-end page waited out the full 200 ms coalescing poll
(`packages/core/src/cache/read-through.ts`) before querying Postgres, because the lock holder released the lock
without writing an entry. That sleep made up about 80% of each run's mean latency and produced the ~200 ms p95/p99
"tail" in both runs. It has since been fixed (waiters stop polling when the lock is released without an entry, and
empty pages are cached with a 5 s TTL; ADR §4).

So the 1.42× throughput ratio and the tail analysis drawn from these files measured that artifact, not scaling.
Only two observations survive: nginx split requests evenly across the four replicas, and median/p90 latency fell
with four replicas.

The clean rerun, on the 500k-row catalog with the fix in place, is in [`../2026-09-25-rerun/`](../2026-09-25-rerun/summary.md).
