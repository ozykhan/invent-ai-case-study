# `modaco` CLI and Load-Balanced Local Stack — Design

Date: 2026-09-25
Status: Approved

## 1. Goal

A command-line tool that both a person and an agent can use to operate the ModaCo API and to load test it, including when the API runs as several replicas behind a load balancer.

- **Operate:** call every endpoint from the terminal with typed arguments and machine-readable output.
- **Load test:** generate sustained, measurable load with realistic scenarios, report latency percentiles correctly under overload, and show how requests spread across API instances.
- **Scale locally:** run N API replicas behind nginx with Docker Compose, so that 1 vs N replica runs can be compared on one machine.

The CLI is target-agnostic: it only needs a base URL. An AWS deployment (ALB in front of ECS or EC2) is a later, separate piece of work; nothing in this design depends on it.

## 2. Decisions

| Topic | Decision |
|---|---|
| Package | New workspace package `apps/cli` (`@modaco/cli`), invoked as `pnpm modaco <command>` |
| Language / runtime | TypeScript on Node 22, run with `tsx` like the rest of the repo |
| Argument parsing | `commander` (subcommands and generated `--help`) |
| HTTP client | `undici` `Agent` with an explicit connection count and keep-alive. The global `fetch` gives no control over the pool |
| Latency histogram | `hdr-histogram-js`, recording microseconds, one histogram per endpoint label |
| Load models | Open (constant arrival rate, `--rate`) and closed (fixed workers, `--concurrency`) |
| Instance attribution | API sets an `X-Instance-Id` response header; the CLI counts responses per instance |
| Local load balancer | nginx in a new Compose profile `lb`, round robin over a scalable `api-lb` service |
| Output | Human-readable by default; `--json` on every command prints one JSON document to stdout |
| Existing demo scripts | Unchanged. README and ADR reference them and their recorded numbers |

## 3. Layout

```
apps/cli/
  package.json            @modaco/cli (dependencies: commander, undici, hdr-histogram-js)
  src/main.ts             commander program: global options, registers command groups
  src/client.ts           ApiClient: base URL, undici Agent, typed methods per endpoint, ApiError
  src/output.ts           print(result, { json }) and table/line formatting
  src/commands/
    health.ts             health [--watch <interval>]
    products.ts           products list|get|create|stock
    promotions.ts         promotions create|get|cancel|target
    ingest.ts             ingest <file> | ingest status <jobId> | ingest rejections <jobId>
    load.ts               load <scenario> with the shared load options
  src/load/
    engine.ts             runs a phase: open or closed model, warmup, ramp, duration, max-inflight
    metrics.ts            per-label HDR histograms, status and error counters, instance counters
    report.ts             live progress line, final table, JSON result document
    parse.ts              durations ("30s", "2m", "500ms") and rates ("2000/s", "120000/m")
    scenarios/
      types.ts            Scenario interface
      browse.ts
      flash-sale.ts
      write-mix.ts
  test/                   Vitest: unit tests and a stub-server integration test
infra/nginx/nginx.conf    upstream over api-lb replicas
```

The root `package.json` gets `"modaco": "tsx apps/cli/src/main.ts"`, so `pnpm modaco …` works from the repo root, arguments pass through, and relative paths (`--out tmp/run.json`, `ingest tmp/vendor-500k.csv`) resolve against the repo root. The CLI's own dependencies resolve from `apps/cli/node_modules` because module resolution starts at the source file.

## 4. Global options

| Option | Default | Meaning |
|---|---|---|
| `--url <url>` | `API_URL` env, else `http://localhost:3000` | API base URL (a replica, nginx, or a remote load balancer) |
| `--json` | off | Print one JSON document to stdout; progress and warnings go to stderr |
| `--timeout <duration>` | `10s` | Per-request timeout |

Exit codes: `0` success, `1` API or check failure (non-2xx, failed assertion), `2` usage error (bad arguments).

## 5. Operational commands

Each command is a thin typed wrapper over one endpoint. On a non-2xx response the CLI prints the API's error envelope (`code`, `message`, `details`) and exits 1.

| Command | Endpoint |
|---|---|
| `health [--watch 2s]` | `GET /health`. `--watch` repeats until interrupted and prints the instance id per poll |
| `products list [--category <slug>] [--sort effective_price\|-effective_price] [--page n] [--page-size n]` | `GET /products` |
| `products get <id>` | `GET /products/:id` |
| `products create --sku --name --category-id --base-price [--stock]` | `POST /products` |
| `products stock <id> (--set n \| --delta n)` | `PATCH /products/:id/stock` |
| `promotions create --name --type percentage\|fixed --value [--starts <iso>] [--ends <iso>] (--product <id> \| --category <id>)` | `POST /promotions`. `--starts` defaults to now minus 1 s, `--ends` to now plus 1 h |
| `promotions get <id>` | `GET /promotions/:id` |
| `promotions cancel <id>` | `POST /promotions/:id/cancel` |
| `promotions target <id> (--product <id> \| --category <id>)` | `PUT /promotions/:id/target` |
| `ingest <file> [--poll 1s]` | `POST /ingestion/jobs`, `PUT` to the presigned URL, then poll until `completed` or `failed` |
| `ingest status <jobId>` | `GET /ingestion/jobs/:id` |
| `ingest rejections <jobId> [--page n] [--page-size n]` | `GET /ingestion/jobs/:id/rejections` |

Human output: a compact table for lists, key/value lines for single objects. `--json` prints the response body unchanged. For `ingest <file>` it prints the final job document.

## 6. Load engine

### 6.1 Load models

- **Open model, `--rate <r>`:** a scheduler emits requests at fixed intervals on a monotonic clock, independent of response times. Latency is measured from the request's **scheduled** start time, not its actual send time. This corrects for coordinated omission: when the server stalls, the queued requests are charged the stall instead of hiding it. `--max-inflight <n>` (default 10,000) caps outstanding requests. A request due while the cap is reached is not sent. It is counted as `dropped`, and the report states that the client could not sustain the rate.
- **Closed model, `--concurrency <n>`:** `n` workers each loop request, await response, repeat. This matches the existing `demo:flash-sale` and is useful for finding saturation throughput. Latency is measured from actual send time.

Exactly one of `--rate` and `--concurrency` is required.

### 6.2 Phases and timing options

| Option | Default | Meaning |
|---|---|---|
| `--duration <d>` | `30s` | Recorded measurement time |
| `--warmup <d>` | `5s` | Load runs but is not recorded. Lets keep-alive connections and caches settle |
| `--ramp <d>` | `0s` | Open model only: the rate rises linearly from 0 to `--rate` over this time, which is recorded |
| `--connections <n>` | `max(concurrency, 64)` for closed; `256` for open | Size of the undici connection pool to the target |
| `--report-every <d>` | `5s` | Interval of the live progress line on stderr |
| `--out <file>` | none | Also write the JSON result document to a file |
| `--seed <n>` | random | Seed for the scenario's random choices, so runs are repeatable |

Response bodies are always read to the end so pooled connections are released, as the existing demo does.

### 6.3 Metrics

Per endpoint label (for example `list`, `detail`, `stock`, `promo:create`) and for the total:

- HDR histogram of latency in microseconds, 1 µs to 60 s, 3 significant digits. Reported: count, req/s, mean, p50, p90, p95, p99, p99.9, max.
- Status code counts (`200`, `404`, `503`, …).
- Error counts by kind: `timeout`, `ECONNRESET`, `ECONNREFUSED`, `other`. A transport error has no status code and is not put in the histogram.
- `dropped` (open model only).

Across all requests:

- **Instance distribution:** the count of responses per `X-Instance-Id` value, with each instance's share. Responses without the header count under `unknown`. This is how a run shows whether the load balancer spreads load evenly.

The live progress line prints elapsed time, current req/s, p50 and p99 over the last interval, error count and in-flight count.

### 6.4 Result document (`--json` / `--out`)

```json
{
  "scenario": "browse",
  "target": "http://localhost:8080",
  "startedAt": "2026-09-25T10:00:00.000Z",
  "options": { "model": "open", "rate": 2000, "duration": "30s", "warmup": "5s", "connections": 256, "seed": 42 },
  "phases": [
    {
      "name": "main",
      "elapsedSeconds": 30.01,
      "total": { "count": 60012, "rps": 1999.7, "latencyMs": { "mean": 4.1, "p50": 3.2, "p90": 6.0, "p95": 7.9, "p99": 15.3, "p999": 41.0, "max": 88.2 }, "status": { "200": 60012 }, "errors": {}, "dropped": 0 },
      "byLabel": { "list": { "…": "same shape as total" }, "detail": { "…": "…" } }
    }
  ],
  "instances": { "api-lb-1": 15010, "api-lb-2": 14998, "api-lb-3": 15003, "api-lb-4": 15001 },
  "checks": [ { "name": "mid-sale product discounted", "ok": true, "message": "…" } ],
  "ok": true
}
```

`ok` is false, and the process exits 1, if any check fails. Scenarios that report before and after phases (flash-sale) have more than one entry in `phases`.

## 7. Scenarios

A scenario implements:

```ts
interface Scenario {
  name: string;
  /** Discovers data (ids, categories) before load starts. May call the API; not measured. */
  setup(ctx: SetupContext): Promise<void>;
  /** Returns the next request to send: label, method, path, optional JSON body, and an optional
   *  onResponse(status, body) hook for scenarios that keep state (write-mix). Called once per request. */
  next(rng: Rng): RequestSpec;
  /** Optional orchestration across phases (flash-sale). Default: one "main" phase. */
  run?(ctx: RunContext): Promise<void>;
}
```

### 7.1 `browse`

Read-only traffic. Setup reads `GET /products?pageSize=100` for up to 20 pages, spread across the catalog using `pagination.total`. From those pages it collects the product ids it saw and the distinct category slugs. It fails with a clear message if the catalog is empty (run `pnpm seed` or `pnpm modaco ingest …` first).

Default mix, overridable with `--mix list=70,detail=30`:

- `list` (70%): `GET /products?category=<random slug>&sort=<random direction>&page=<1..--max-page>&pageSize=20`. `--max-page` defaults to 5, which keeps hits within the cached pages and mirrors the demo. Raising it exercises cache misses and deeper offsets.
- `detail` (30%): `GET /products/<random sampled id>`.

`--category <slug>` restricts both request types to one category.

### 7.2 `flash-sale`

A port of `scripts/demo-flash-sale.ts` onto the engine. Arguments: `--category <slug>` (default `accessories`).

1. Setup: resolve the category id from `GET /products?category=<slug>&pageSize=1`. Warn if it has fewer than 50,000 products.
2. Warmup with the `browse` list requests restricted to that category (not recorded).
3. Phase `before`: one third of `--duration`.
4. Create a 50% percentage promotion on the category.
5. Phase `after`: two thirds of `--duration`. It runs concurrently with the mid-sale check: create a product in the category with base price 20.00, then read it and require `effectivePrice` 10.00 and `activePromotion.id` equal to the new promotion.
6. Cancel the promotion, even if an earlier step failed.

The report shows both phases and the check result, and records the first item's price before and after.

### 7.3 `write-mix`

Reads with concurrent writes, to exercise version-counter cache invalidation and stock counters under load. Setup is the same as `browse`. Default mix, overridable with `--mix`:

- `list` 60%, `detail` 25%: as in `browse`.
- `stock` 14%: `PATCH /products/<random id>/stock` with `{ "delta": ±1 }`.
- `promo` 1%: alternates between creating and cancelling promotions, one request per slot. If the scenario holds no open promotion, the slot sends `POST /promotions` (10% on a random sampled product, labelled `promo:create`), and the response hook stores the new id. Otherwise it sends `POST /promotions/<id>/cancel` for the oldest open one (`promo:cancel`). Each promotion therefore lives about one promo slot interval. Every create and cancel bumps version counters, which forces cache rebuilds while reads continue.

At the end, the scenario cancels any promotion it created that is still open, for example because the run was interrupted.

### 7.4 Interruption

SIGINT stops scheduling and waits up to 5 s for in-flight requests. It then prints and writes the partial report with `"interrupted": true`, runs scenario cleanup, and exits 130.

## 8. API change: `X-Instance-Id`

A middleware in `apps/api/src/middleware/instance-id.ts`, registered first in `createApp`, sets `X-Instance-Id` on every response. The value is `INSTANCE_ID` when set, otherwise `os.hostname()`. In Docker Compose the hostname is the container id, which is unique per replica. The value is computed once at startup. `Config` gains `instanceId`. The header exposes no internal information beyond a container hostname; it can be disabled later if the API is ever exposed publicly (noted in the README).

## 9. Local load-balanced stack

`docker-compose.yml` changes:

- The `api` service body moves into an extension field `x-api: &api` (build, command, environment, depends_on).
- `api` (profile `app`) is `<<: *api` plus `ports: ["3000:3000"]`. Behaviour and the README are unchanged.
- New `api-lb` (profile `lb`): `<<: *api` with no published ports, so it can be scaled, and `deploy.resources.limits.cpus: ${API_CPUS:-1}`. The CPU limit keeps replicas from sharing every core of the host, so adding replicas adds capacity.
- New `nginx` (profile `lb`): `nginx:1.27-alpine`, publishes `8080:80`, mounts `infra/nginx/nginx.conf`, depends on `api-lb`.

`infra/nginx/nginx.conf`:

- `upstream api { server api-lb:3000; keepalive 256; }`. Docker's DNS returns every replica's address for `api-lb`, and nginx round-robins across them.
- `proxy_http_version 1.1`, an empty `Connection` header (upstream keep-alive), and `proxy_set_header X-Request-Id $http_x_request_id`.
- `worker_processes auto`, `worker_connections 4096`, access log off (the load run is the measurement).
- nginx resolves `api-lb` once at startup. After changing the replica count, run `docker compose restart nginx`.

Usage:

```bash
docker compose --profile lb up --build -d --scale api-lb=4
docker compose exec api-lb pnpm db:migrate   # once
pnpm modaco --url http://localhost:8080 load browse --rate 2000/s --duration 60s --out tmp/lb-4.json
```

**Known limit:** each replica holds a Postgres pool of 10 and Postgres allows 100 connections by default. Up to about 9 replicas fit, leaving room for migrations and the runner. More replicas need `max_connections` raised or a pooler (PgBouncer, or RDS Proxy on AWS). On one machine, Postgres, Redis, nginx, the replicas and the load generator all share the host's CPUs. Local runs compare configurations; they do not predict production capacity.

## 10. Error handling

- Invalid options (unknown scenario, both or neither of `--rate`/`--concurrency`, malformed duration or mix) produce a usage message and exit code 2 before any request is sent.
- Operational commands: non-2xx prints the envelope and exits 1. A transport error prints the target URL and the cause (for example `ECONNREFUSED http://localhost:8080`) and exits 1.
- Load commands never abort on individual request failures; those are metrics. Setup failures (empty catalog, unknown category) abort with exit 1 before load starts.

## 11. Testing

Vitest in `apps/cli/test`, needing no database or Redis:

- `parse.test.ts`: durations, rates, mix parsing, rejection of bad input.
- `metrics.test.ts`: histogram percentiles against known samples, status and error counting, instance tallies, summary shape.
- `engine.test.ts`: against a local `node:http` stub server with a configurable delay:
  - Open model holds the target rate within 5% over 2 s.
  - With an injected stall, latencies measured from scheduled start include the stall (the coordinated omission check).
  - Closed model keeps exactly `n` requests in flight.
  - `--max-inflight` produces `dropped` counts.
- `scenarios.test.ts`: `browse` and `write-mix` against a stub that serves canned `/products` pages and sets `X-Instance-Id` from a rotating pool of 3. Asserts that setup samples ids and slugs, the mix proportions are within tolerance for a fixed seed, and instance tallies add up to the response count.
- `api/test/instance-id.test.ts`: the API sets `X-Instance-Id` from `INSTANCE_ID`.

Manual verification, recorded in the README:

1. `pnpm modaco health` and one command from each operational group against the local API.
2. `load browse` against the single `api` service, then against `nginx` with `--scale api-lb=1` and `=4`. The instance distribution shows 4 roughly equal shares.

## 12. Out of scope

- AWS infrastructure for the API (ALB, ECS or EC2, RDS, ElastiCache). This is the planned next step, and the CLI needs no change for it.
- Distributed load generation across machines. Run several CLI processes and add up their JSON results by hand.
- A `compare` command for result files.
- Changes to the existing `scripts/demo-*` files.
