# 1 vs 4 API replicas behind nginx: clean rerun (2026-09-25)

This replaces the first run in [`../2026-09-25/`](../2026-09-25/README.md). That run measured a 200 ms cache-wait
artifact on past-the-end pages, not scaling. The ADR analysis is in [ADR.md §11](../../../ADR.md#11-horizontal-scaling-1-vs-4-replicas-behind-nginx).

## Environment

| | |
|---|---|
| Host | Apple M4 Pro Mac mini, 12 cores (8 performance + 4 efficiency), 24 GiB, macOS 27.0 |
| Docker | Docker Desktop, engine 29.6.2; VM with **6 CPUs**, 5.79 GiB |
| API replicas | `api-lb` image built from this commit (`node:22-alpine`, Node 22.23.1). `API_CPUS` = 1, the default (`NanoCpus=1e9`, cgroup `cpu.max` 100000/100000). Postgres pool of 10 per replica |
| nginx | 1.27.5 (`nginx:1.27-alpine`), `worker_processes auto` (6), `infra/nginx/nginx.conf` as committed |
| Postgres / Redis | 16.15 (`max_connections` 100, `shared_buffers` 128 MB) / 7.4.11; neither is CPU-limited |
| Load client | `apps/cli` running on the host (outside the VM) as one Node 22.13.0 process |
| Code | `dc149a2` on branch `claude/cli-load-testing-tool-b70cc2`, which includes the lock-release fix (`d369d17`, `fbfbf65`) |
| Catalog | 500k-row vendor ingest: 499,479 products in 8 categories of 62,426 to 62,444 products (about 3,120 pages of 20 each), 0 promotions. No seed data, so every sampled category has far more than 5 pages and the `--max-page 5` clamp never applies |

Everything except the client ran in the one 6-CPU Docker VM: the replicas, nginx, Postgres, Redis and LocalStack
(idle). The VM also held an idle, unrelated Compose project (`studio-hold-*`, 0 to 1% CPU). During some runs it also
held short-lived `testcontainers` containers from another project's tests. The `docker stats` captures show them. These
used more than 0.5% CPU inside a recorded window:

| Run | Foreign container CPU in the window |
|---|---|
| closed-1r-run1 | `awesome_mendel` 2.6 to 7.6% for the first 25 s |
| open-1r-run1 | `relaxed_newton` 42.5% in the sample at the start of the recorded phase (16:46:13Z) |
| write-mix-4r | `elated_golick` 16 to 21% for the first 10 s, then 3 to 5% |
| open-4r-coldstart-failed | `magical_vaughan` 60% in one sample |

## Method and exact commands

The base infra (Postgres on 5433, Redis, LocalStack) was already running as Compose project `modaco-api`, whose working
directory no longer exists. So the replicas joined that project's network through `-p modaco-api` and `--no-deps`,
which leaves the running Postgres, Redis and LocalStack alone. Apart from that, these are the README commands.

Catalog (once):

```bash
export PATH="$HOME/.nvm/versions/node/v22.13.0/bin:$PATH"; set -a && . ./.env.example && set +a
pnpm db:migrate
pnpm vendor-file --rows=500000               # tmp/vendor-500k.csv
pnpm dev:api &  pnpm dev:runner &            # stopped again after the ingest
pnpm demo:ingest tmp/vendor-500k.csv         # completed: 9/9 chunks, 499,479 rows + 521 rejected, 7.3 s
```

Before each run (to switch or re-check the replica count):

```bash
docker compose -p modaco-api --profile lb up -d --no-deps --scale api-lb=N api-lb nginx   # image built once with --build
# wait until every replica answers GET /health itself (docker exec <replica> node -e "fetch('http://localhost:3000/health')...")
docker compose -p modaco-api restart nginx                                               # nginx resolves replicas at startup
node --import tsx apps/cli/src/main.ts --url http://localhost:8080 health               # repeated until N distinct instance ids (health-*.txt)
# Drop every cache entry except the version counters. Each run then starts cold, and nothing built in its warm-up
# can reach its 300 s TTL inside the recorded window:
redis-cli --scan --pattern 'list:*' | xargs redis-cli del      # likewise product:*, category:*, stock:*, lock:*
```

The per-replica readiness wait was added after the first attempt at closed-4r-run2. That attempt saw only 3 of the 4
instance ids through nginx, because nginx restarted while one replica was still booting and marked that replica
failed. It was stopped before any load was sent. The closed-4r-run2 recorded here replaced it.

Closed model: 3 runs per configuration, interleaved 1, 4, 1, 4, 1, 4:

```bash
node --import tsx apps/cli/src/main.ts --url http://localhost:8080 load browse \
  --seed 42 --concurrency 100 --duration 60s --warmup 15s --out closed-<N>r-run<k>.json
```

Open model at 4,900 req/s, about 70% of the median 1-replica closed throughput (6,994.3 req/s). An open-model run on a
cold cache collapsed (see "Cold-start collapse" below), so a 25 s closed-model pre-warm fills the cache first. Its
output is in `prewarm-*.txt` and is not part of the results:

```bash
node --import tsx apps/cli/src/main.ts --url http://localhost:8080 load browse --seed 42 --concurrency 100 --duration 25s --warmup 0s
node --import tsx apps/cli/src/main.ts --url http://localhost:8080 load browse \
  --seed 42 --rate 4900/s --duration 60s --warmup 15s --out open-<N>r.json
```

Write-mix sanity check with 4 replicas, after the same pre-warm:

```bash
node --import tsx apps/cli/src/main.ts --url http://localhost:8080 load write-mix --seed 42 --rate 100/s --duration 30s --warmup 5s --out write-mix-4r.json
```

Diagnostic, one run with 4 replicas: the closed-model browse command above with `--concurrency 200`.

Captured during every run:
- `docker stats --no-stream` for all containers, about every 5 s (`stats-*.txt`).
- The client's `ps -o %cpu,rss,time` every 5 s (`client-*.txt`) and its `/usr/bin/time -l` totals (end of `log-*.txt`).
- For the open, write-mix and diagnostic runs, each replica's cgroup `cpu.stat` before and after (`cgroup-*.txt`).

How the CPU figures below are computed:
- Container CPU is the mean of the `docker stats` samples inside the recorded 60 s window, where 100% is one CPU.
- Client CPU is the change in the client's cumulative CPU time over the same window.
- "CPU per request" is the replicas' summed CPU divided by req/s.

## Files

| Pattern | Content |
|---|---|
| `closed-{1,4}r-run{1,2,3}.json`, `open-4r.json`, `open-1r-run{1,2}.json`, `write-mix-4r.json`, `diag-closed-4r-c200.json` | CLI result documents (`--out`) |
| `log-*.txt` | the command, the CLI's timestamped stderr (setup, phase start, progress), the report, `/usr/bin/time -l` |
| `stats-*.txt` | `docker stats` captures |
| `client-*.txt` | client process CPU and RSS samples |
| `health-*.txt` | scaling output and the distinct instance ids seen through nginx before the run |
| `cgroup-*.txt` | each replica's `cpu.stat` (usage, `nr_periods`, `nr_throttled`, `throttled_usec`) before and after |
| `prewarm-*.txt` | the pre-warm before each open-model and write-mix run |
| `*-coldstart-*`, `drain-*` | the two open-model attempts on a cold cache that collapsed |

## Closed model: 100 workers, 60 s recorded after a 15 s warm-up

Latency is in ms. `max` is the single slowest request. 5xx and transport errors were 0 in every run.

**1 replica**

| Run | req/s | p50 | p90 | p99 | p99.9 | max | 5xx | Transport | Instance share | Replica CPU | nginx | Redis | Postgres | Client | CPU per request |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 7,120.4 | 12.75 | 19.44 | 37.12 | 44.90 | 105.91 | 0 | 0 | 100% | 100% | 36% | 16% | 0.5% | 31% | 140 µs |
| 2 | 6,994.3 | 13.02 | 19.62 | 36.77 | 45.34 | 56.14 | 0 | 0 | 100% | 100% | 34% | 17% | 0.5% | 34% | 143 µs |
| 3 | 6,934.0 | 13.15 | 19.78 | 37.22 | 45.44 | 61.46 | 0 | 0 | 100% | 100% | 33% | 16% | 0.4% | 32% | 145 µs |
| **Mean ± sd** | **7,016 ± 95** | 12.97 ± 0.20 | 19.61 ± 0.17 | 37.04 ± 0.24 | 45.23 ± 0.29 | | | | | | | | | | |
| Range | 6,934 to 7,120 | 12.75 to 13.15 | 19.44 to 19.78 | 36.77 to 37.22 | 44.90 to 45.44 | | | | | | | | | | |

**4 replicas**

| Run | req/s | p50 | p90 | p99 | p99.9 | max | 5xx | Transport | Instance share | Replica CPU | nginx | Redis | Postgres | Client | CPU per request |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 11,962.5 | 1.15 | 31.55 | 38.02 | 52.35 | 67.44 | 0 | 0 | 25.0 / 25.0 / 25.0 / 25.0% | **100** / 70 / 69 / 68% | 56% | 36% | 0.3% | 50% | 256 µs |
| 2 | 15,120.4 | 3.39 | 22.38 | 36.96 | 44.93 | 231.69 | 0 | 0 | 25.0 / 25.0 / 25.0 / 25.0% | 82 / 81 / 79 / 77% | 80% | 35% | 0.5% | 59% | 211 µs |
| 3 | 15,320.5 | 3.38 | 22.13 | 36.61 | 41.82 | 77.63 | 0 | 0 | 25.0 / 25.0 / 25.0 / 25.0% | 81 / 78 / 78 / 77% | 82% | 36% | 0.3% | 58% | 205 µs |
| **Mean ± sd** | **14,134 ± 1,884** | 2.64 ± 1.29 | 25.35 ± 5.37 | 37.20 ± 0.73 | 46.37 ± 5.41 | | | | | | | | | | |
| Range | 11,963 to 15,321 | 1.15 to 3.39 | 22.13 to 31.55 | 36.61 to 38.02 | 41.82 to 52.35 | | | | | | | | | | |

The instance shares are exact: in every 4-replica run, nginx's round robin gave each replica a quarter of the
requests, give or take 3.

**Throughput ratio, 4 vs 1 replicas: 2.01× on the means** (14,134 / 7,016). Paired by position in the interleaving,
the ratios are 1.68×, 2.16× and 2.21×. Leaving out 4-replica run 1 (explained below), the ratio is 2.19×.

**p99 did not move:** 37.04 ms with 1 replica and 37.20 ms with 4. The median fell from 12.97 ms to 2.64 ms. p90 rose
from 19.61 ms to 25.35 ms: with 4 replicas, most requests are fast, and more than 10% take 20 to 38 ms.

**Diagnostic, 4 replicas with 200 workers (one run):**
- 18,957.3 req/s: p50 7.32 ms, p90 20.21 ms, p99 62.75 ms, p99.9 121.98 ms.
- 0 errors, 25.0% per instance.
- Replicas at 83 to 85%, nginx 114%, Redis 35%, Postgres 1.3%, client 69%.

## Open model: 4,900 req/s, warm cache, 60 s recorded after a 15 s warm-up

| Run | req/s | p50 | p90 | p99 | p99.9 | max | 5xx | Transport | Dropped | Replica CPU | nginx | Redis | Postgres | Client |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 replica, run 1 | 4,897.9 | 2.85 | 76.03 | **4,759.55** | 9,076.74 | 9,764.69 | 0 | 125 (timeout) | 0 | 99% | 28% | 16% | 0.3% | 37% |
| 1 replica, run 2 | 4,900.0 | 1.15 | 15.26 | **51.97** | 558.59 | 955.45 | 0 | 0 | 0 | 100% | 22% | 18% | 0.2% | 34% |
| 4 replicas | 4,900.0 | 0.62 | 0.79 | **2.46** | 11.85 | 22.55 | 0 | 0 | 0 | 44 / 44 / 44 / 44% | 25% | 28% | 0.3% | 37% |

- **1 replica, run 1** had several stalls in which the in-flight count climbed into the hundreds or thousands (4,632 at the 10 s
  mark), and 125 requests hit the 10 s client timeout in total. The first stall began just after a foreign container
  used 42.5% CPU at the start of the window (see the table above). The replica's cgroup was throttled in 474 of 755
  CFS periods, 4.5 s in total.
- **1 replica, run 2** was run to check run 1 and had no stall over 1 s. The replica sat at 99 to 100% CPU in both
  runs. In run 2 it was throttled in 457 of 754 periods, 2.9 s in total.
- **4 replicas:** every replica at 44%, and 0 throttled periods during the run.
- **Why 1 replica is saturated here.** The rate is 70% of the 1-replica closed throughput, but not 70% of the
  replica's CPU. At 4,900 req/s the replica spends about 204 µs of CPU per request; at 7,000 req/s in the closed model
  it spent 143 µs (next section).

## CPU observations

- **1 replica, closed.** The replica is pinned at its 1-CPU quota in every sample of every run (98.5 to 101.3%).
  - Postgres is idle (under 1%, the cache is warm), Redis about 16%, nginx about 34%.
  - The client uses about a third of a core. The VM used about 1.5 of its 6 CPUs.
  - The bottleneck is the single replica's CPU limit.
- **4 replicas, closed, runs 2 and 3.** No container is pinned.
  - Replicas average 77 to 82% (single samples up to 93%), nginx about 0.8 CPU, Redis about 0.35 CPU, Postgres under
    1%. The client uses about 0.6 of a host core, and the VM about 4.3 of its 6 CPUs.
  - With 200 workers, throughput rose another 24% (18,957 req/s), with replicas at about 84% and the VM at about 4.9
    CPUs. So 100 workers did not saturate 4 replicas.
  - Even at 200 workers no single container is pinned, and the VM has about 1.1 CPUs of headroom left.
  - 1 replica with 200 workers was not measured.
- **4 replicas, closed, run 1.** One replica was pinned at 100% while the other three sat at 68 to 70% for the same
  number of requests.
  - That replica used about 1.45× the CPU per request of the other three.
  - Round robin gives every replica the same share regardless of speed, so the whole run went at that replica's pace:
    11,963 req/s, with p90 at 31.55 ms because a quarter of the requests queued behind it.
  - The other three replicas were recreated before runs 2 and 3, and it did not happen again.
  - The cause was not found. No foreign container was active in that window.
- **CPU per request falls as per-replica load rises.** Summed replica CPU ÷ req/s:

  | Per-replica load | Run | CPU per request |
  |---|---|---|
  | 7,000 req/s | closed, 1 replica | 143 µs |
  | 4,900 req/s | open, 1 replica | about 204 µs |
  | about 3,800 req/s | closed, 4 replicas, runs 2 and 3 | 208 µs |
  | about 3,000 req/s | the unpinned replicas of 4-replica run 1 | 231 µs |
  | 1,225 req/s | open, 4 replicas | 359 µs |

  So 4 replicas at about 79% (3.2 CPUs of work) deliver 2.2× the throughput, not 3.2×. Why a busier Node process
  spends less CPU per request was not measured.
- **CFS throttling.** Heavy on a pinned replica: about 61 to 63% of periods in the open 1-replica runs. Light on an
  unpinned one: in the 200-worker diagnostic, 18 to 22 of about 780 periods per replica (under 3%), about 6 ms each.
- **Client.** It used 31 to 37% of one host core at 5,000 to 7,000 req/s, and 58 to 69% at 15,000 to 19,000 req/s. It
  was never the limit.
- **Postgres** stayed under 1.5% in every warm run. These runs measure the cached read path (nginx, Node, Redis), not
  the price computation in SQL.
- **Warm-up.** In the closed runs, the flushed cache refilled within about 10 s of warm-up. For example, in
  closed-1r-run2 Postgres ran at 590% in the first warm-up sample and at 1.9% ten seconds later. No recorded window
  contains rebuilds.

## Write-mix through nginx, 4 replicas

`write-mix --rate 100/s --duration 30s`:
- 3,000 requests: **0 5xx, 0 transport errors**, 25.0% per instance.
- Status 200 × 2,984 and 201 × 16. That is 16 promotion creates, 15 cancels during the run, and 448 stock writes.
  Cleanup cancelled the rest, and no promotion was left active.
- p50 2.58 ms, p99 219.78 ms. List p90 is 173 ms: every promotion bumps its category's version, the next reads rebuild
  those pages, and Postgres averaged 140% CPU. ADR §5 describes this behavior.
- At this low rate, upstream connections stay idle for longer, which is where the old nginx keepalive race produced
  502s. None appeared.

## Cold-start collapse (open model, not in the comparison)

The first two open-model attempts with 4 replicas started from a flushed cache, one with no ramp and one with
`--ramp 30s` (`open-4r-coldstart-failed.*` and `open-4r-coldstart-ramp30s-failed.*`). Both collapsed within the first
seconds of load and never recovered:

- **Results.** 7 and 1,645 successful responses in 60 s, and 46,541 and 50,741 transport errors (mostly 10 s
  timeouts). About 247k and 242k requests were dropped at the 10,000 in-flight cap. nginx logged
  `4096 worker_connections are not enough`.
- **Postgres** ran at 470 to 590% CPU with 40 active queries. That is all 4 replicas' pools of 10, busy building
  listing pages.
- **No recovery.** 5 minutes after the client exited, Postgres was still at about 590% with 40 active queries
  (`drain-open-4r-coldstart-failed.txt`). It went idle only when the replicas were restarted: they keep working through
  queued queries whose clients have gone.
- **Mechanism.** On an idle stack, building one cold category page (62k products) takes 220 to 240 ms, measured with
  single `curl` requests. That is longer than the 200 ms a concurrent reader waits for it (`waitMs` in
  `packages/core/src/cache/read-through.ts`). So under open-model load, every reader of a cold page falls through to its
  own Postgres query. At a few thousand req/s, those queries arrive faster than Postgres finishes them.
- **The pre-warm survived it.** The closed-model pre-warm (at most 100 requests in flight) filled the same cold cache
  without collapsing. Its p99.9 was 1.5 to 2.6 s and its max 4.3 to 7.4 s while it rebuilt.

Not tested: whether 1 replica (a pool of 10 rather than 40) collapses the same way, and the rate at which the collapse
starts.
