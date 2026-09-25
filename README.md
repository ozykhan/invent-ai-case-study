# ModaCo Promotion Management API

## What this is

An internal REST API (Node.js 22, Express 5, TypeScript, PostgreSQL 16 with Drizzle, Redis) for ModaCo's product catalog and promotions. It lists products with filtering, pagination and sorting by effective price, serves single products, creates, cancels and reassigns promotions, and ingests vendor files through serverless functions (AWS Lambda, S3, SQS; LocalStack locally).

Scenario B (flash sales): the effective price is computed at read time in SQL and never stored, and reads go through a Redis cache whose entries embed the version counters they were built under, so a promotion write invalidates any number of entries with one `INCR`. Scenario A (500k-row vendor files): a splitter Lambda cuts the uploaded file into byte ranges using only its size, and worker Lambdas each stream one range, run the pricing rules, and upsert in batches of 1000.

Design rationale, trade-offs and measured numbers are in [ADR.md](ADR.md).

## Quick start

Requires Node 22, pnpm 9 (`corepack enable`) and Docker.

```bash
pnpm install
docker compose up -d postgres redis localstack
set -a; source .env.example; set +a   # nothing loads .env automatically; export the vars in each terminal
pnpm db:migrate
pnpm seed
pnpm dev:api        # terminal 1: http://localhost:3000
pnpm dev:runner     # terminal 2 (local Lambda stand-in)
```

- Postgres is published on host port **5433**, not 5432: `postgres://modaco:modaco@localhost:5433/modaco`.
- The API and the migrate/seed scripts default to the same local values as `.env.example`. The runner does not: without `AWS_ENDPOINT_URL` it talks to real AWS, so export the file in the runner's terminal.
- LocalStack's init script (`docker/localstack/init-aws.sh`) creates the bucket, the three queues, the redrive policy and the S3 notification.

All-in-Docker alternative (API and runner in containers too):

```bash
docker compose --profile app up --build -d
docker compose exec api pnpm db:migrate
docker compose exec api pnpm seed
```

## Configuration

Everything is read from environment variables; `.env.example` lists them with their local values. Most are connection strings and AWS names. Three tune how the API's Postgres pool behaves under overload (the ingestion worker keeps the driver defaults):

| Variable | Default | Effect |
|---|---|---|
| `DB_POOL_ACQUIRE_TIMEOUT_MS` | `2000` | A request that waits longer than this for a pool connection fails with 503 `overloaded` and `Retry-After: 1` instead of queueing without bound. `0` waits forever. |
| `DB_STATEMENT_TIMEOUT_MS` | `5000` | Postgres `statement_timeout` on every API connection. A cancelled statement (SQLSTATE 57014) is also a 503 `overloaded`. `0` disables it. |
| `DB_JIT` | `off` | Postgres JIT for the API's connections (`on`/`off`). It added 83 to 131 ms to every cold listing build (ADR §4). |

Keep the worst-case bounded cache build, three sequential queries of at most acquire + statement timeout each (21 s with the defaults), below the 30 s cache lock TTL (ADR §4). If the lock expired under a live build, its waiters would take over and build again.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Postgres and Redis checks; 503 only if Postgres is down |
| GET | `/products?category=&sort=&page=&pageSize=` | List with effective price. `category` is a slug; `sort` is `effective_price` (default) or `-effective_price`; `page` max 1000, `pageSize` max 100 |
| GET | `/products/:id` | Single product with effective price, active promotion and live stock |
| POST | `/products` | Create a product; it inherits any active category promotion immediately |
| PATCH | `/products/:id/stock` | Set `{ "stock": n }` or adjust `{ "delta": n }` |
| POST | `/promotions` | Create a percentage or fixed promotion for a product or a category |
| GET | `/promotions/:id` | Promotion detail |
| POST | `/promotions/:id/cancel` | Cancel (idempotent) |
| PUT | `/promotions/:id/target` | Assign: move the promotion to another product or category |
| POST | `/ingestion/jobs` | Create an ingestion job; returns a presigned S3 upload URL (15 min) |
| GET | `/ingestion/jobs/:id` | Job status and counters |
| GET | `/ingestion/jobs/:id/rejections?page=&pageSize=` | Rejected rows with reason, chunk index and chunk-relative line number |

Errors use one envelope: `{ "error": { "code", "message", "details?" } }` with 400 validation, 404 not found, 409 duplicate SKU, 422 semantic violation, 503 database unavailable, 500 unexpected. Money is a decimal string in JSON.

```bash
# List, cheapest first, one category
curl 'localhost:3000/products?category=accessories&sort=effective_price&page=1&pageSize=20'

# Detail
curl localhost:3000/products/1

# Create a category-wide promotion
curl -X POST localhost:3000/promotions -H 'content-type: application/json' -d '{
  "name": "50% off accessories", "discountType": "percentage", "value": "50",
  "startsAt": "2026-09-25T00:00:00Z", "endsAt": "2026-12-31T00:00:00Z",
  "target": { "categoryId": 1 } }'

# Cancel
curl -X POST localhost:3000/promotions/<promotionId>/cancel

# Assign to a single product (or {"categoryId": n})
curl -X PUT localhost:3000/promotions/<promotionId>/target -H 'content-type: application/json' -d '{ "productId": 2 }'

# Create an ingestion job, then upload the file to the returned uploadUrl
curl -X POST localhost:3000/ingestion/jobs -H 'content-type: application/json' -d '{ "filename": "vendor.csv" }'
curl -X PUT --upload-file tmp/vendor-500k.csv '<uploadUrl>'

# Job status
curl localhost:3000/ingestion/jobs/<jobId>
```

Vendor CSV format: header `sku,name,category,vendor_price,stock`, UTF-8, no embedded newlines. Unknown categories are created with default pricing (30% margin, floor 0.99, ceiling 99999.99).

## Demos

With the API and the runner running (Quick start):

```bash
pnpm vendor-file --rows=500000          # writes tmp/vendor-500k.csv (~32 MB, ~0.1% deliberately bad rows)
pnpm demo:ingest tmp/vendor-500k.csv    # creates a job, uploads, polls every second
pnpm demo:flash-sale accessories        # needs the ingest first so the category has 50k+ products
```

What to look for:

- `demo:ingest`: the job moves `pending -> processing -> completed`; `chunks n/9` for the 32 MB file at 4 MiB chunks; the last line reports total rows, elapsed time and rows/s. `rowsProcessed + rowsRejected` equals the row count.
- `demo:flash-sale`: prints the category size, warms up for 4 s (unrecorded), measures warm-cache latency, creates a 50% category promotion, keeps the load running while it creates a new product in the category, then cancels the promotion. Look for `errors: 0`, the first item's price halving, the p50/p95/p99 before vs after, and `PASS mid-sale check` (the new product reads back at half price on its first read). The script exits non-zero if that check fails. Tunables: `CONCURRENCY` (50), `DURATION_MS` (15000), `WARMUP_MS` (4000), `API_URL`.

Results from one run are in ADR.md sections 5 and 6.

## CLI (`pnpm modaco`)

`apps/cli` is a command-line client for every endpoint plus a load generator. It needs only a base URL (`--url`, else `API_URL`, else `http://localhost:3000`), so it works the same against one local API, the nginx load balancer below, or a remote deployment. Every command takes `--json` and then prints one JSON document to stdout; `health --watch` is the exception and prints one JSON line per poll. Exit codes: 0 ok, 1 API/check/transport failure, 2 usage error, 130 interrupted load run.

Ctrl-C on a load run stops scheduling, waits up to 5 s for in-flight requests, runs cleanup and reports what was measured with `"interrupted": true`. The CLI exits 130, but through `pnpm modaco` a Ctrl-C in a terminal also reaches pnpm, which prints `ELIFECYCLE Command failed with exit code 130` and can still exit 0 to the shell. Scripts that depend on the exit code should call the CLI directly from the repo root, `node --import tsx apps/cli/src/main.ts load …`, or check `"interrupted"` in the `--json`/`--out` document. Promotions a load run creates end a minute after the run would, so a run killed before its cleanup leaves no discount on for long.

```bash
pnpm modaco health                                        # status, checks and the answering instance
pnpm modaco health --watch 1s                             # keep polling (useful while scaling replicas)
pnpm modaco products list --category accessories --sort=-effective_price --page-size 5
pnpm modaco products get 1
pnpm modaco products create --sku DEMO-1 --name Demo --category-id 1 --base-price 19.99 --stock 3
pnpm modaco products stock 1 --delta -2                   # or --set 10
pnpm modaco promotions create --name "50% off" --type percentage --value 50 --category 1
pnpm modaco promotions target <promotionId> --product 2
pnpm modaco promotions cancel <promotionId>
pnpm modaco ingest upload tmp/vendor-500k.csv             # create job, upload, poll to completion
pnpm modaco ingest rejections <jobId> --page-size 10
```

### Load testing

```bash
pnpm modaco load browse --rate 2000/s --duration 60s               # open model: fixed arrival rate
pnpm modaco load browse --concurrency 100 --duration 60s           # closed model: fixed workers
pnpm modaco load write-mix --rate 500/s --duration 60s             # reads + stock writes + promotion churn
pnpm modaco load flash-sale --category accessories --concurrency 50 --duration 15s
pnpm modaco load browse --rate 2000/s --ramp 30s --duration 90s --out tmp/run.json
pnpm modaco load browse --rate 2000/s --duration 60s --max-error-rate 0.01   # tolerate up to 1% 5xx/transport errors
```

- **Scenarios.**
  - `browse`: category listings with random page and sort, plus product details (`--mix list=70,detail=30`, `--max-page 5` (at most 1000, the API's page limit; further clamped down per category to that category's real page count, logged at startup), `--category`).
  - `write-mix`: reads plus stock writes plus promotion create/cancel cycles that bump cache versions (`--mix list=60,detail=25,stock=14,promo=1`).
  - `flash-sale`: the `demo:flash-sale` flow on the load engine. It records a `before` and an `after` phase and checks that a mid-sale product reads back at half price *and* that the category listing itself reflects the discount (page 1, default sort, read once right after the promotion is created); the run exits 1 if either check fails. The mid-sale product is created fresh every run (`MIDSALE-LOAD-<timestamp>`), deliberately: the check is that a *new* row reads back correctly on its first read, not that an existing cache entry gets invalidated (the listing check above already covers that). Each run leaves one row behind; clean them up with `delete from products where sku like 'MIDSALE-LOAD-%'`.
- **Models.** `--rate` holds a constant arrival rate and times each request from its scheduled start, so a stalling server is charged for the requests it delayed (no coordinated omission). Requests over `--max-inflight` (default 10000) are counted as `dropped`. `--ramp` runs as its own unrecorded phase before the first recorded phase, so its rising rate never lands in that phase's percentiles. `--concurrency` runs fixed workers, which is useful for finding saturation throughput. The open model's connection pool defaults to `--rate` × `--timeout` (in seconds), floored at 256 and capped at `--max-inflight`, unless `--connections` is set.
- **Output.** Per label and in total: count, req/s, p50/p90/p95/p99/p99.9/max in ms, status codes, errors and drops. `count`, `req/s` and the percentiles cover 2xx-4xx responses only; a >= 500 response is kept out of the latency histogram and the req/s count and instead counted under `errors.serverError`, alongside the transport error kinds (timeout, ECONNRESET, ECONNREFUSED). `--max-error-rate` (default 0) fails the run (`"ok": false`, exit 1) when the 5xx + transport error rate exceeds it; 4xx is visible in `status` but never counted as an error or fails the run by itself (write-mix legitimately gets some 404/409/422 from promo cancel races). Also the share of responses served by each `X-Instance-Id`. `--json` or `--out` gives the full result document for comparing runs.
- **Cold cache.** A run against an empty cache starts with a burst of 503 `overloaded` while the listing pages are built (one build per key; waiters give up after 5 s instead of querying Postgres themselves), then settles on cache hits. See ADR §11 for the measured size of that burst.
- **Repeatability.** `--seed` makes the request sequence repeatable for `browse` and `flash-sale`. In `write-mix` the reads and stock writes follow the seed, but whether a promo slot creates or cancels depends on which earlier creates have answered, so that part varies with response timing. Setup samples up to 20 listing pages to find product ids and categories, so the catalog must not be empty.

### Several API replicas behind nginx

```bash
docker compose up -d postgres redis localstack && pnpm db:migrate && pnpm seed   # if not done already
docker compose --profile lb up --build -d --no-deps --scale api-lb=4 api-lb nginx   # 4 replicas + nginx on :8080
pnpm modaco --url http://localhost:8080 health --watch 1s                        # instance id rotates
pnpm modaco --url http://localhost:8080 load browse --concurrency 100 --duration 20s --out tmp/lb-4.json

docker compose --profile lb up -d --no-deps --scale api-lb=1 api-lb nginx && docker compose restart nginx   # nginx resolves replicas at startup
pnpm modaco --url http://localhost:8080 load browse --concurrency 100 --duration 20s --out tmp/lb-1.json
```

These use the closed model because it measures how much throughput each configuration can take. With `--rate` the throughput is fixed by the flag, so a 1 vs N replica comparison only shows up in latency (and in drops once one side saturates).

- Each replica is limited to `API_CPUS` cores (default 1), so adding replicas adds capacity instead of sharing every host core.
- The API sets `X-Instance-Id` on every response: `INSTANCE_ID` if set, else the hostname, which is the container id under Compose. It exposes nothing beyond that hostname; drop the middleware if the API is ever public.
- Each replica holds a Postgres pool of 10 and Postgres allows 100 connections by default, so up to about 9 replicas fit. More need `max_connections` raised or a pooler (PgBouncer; RDS Proxy on AWS).
- Postgres, Redis, nginx, the replicas and the load generator share this machine's CPUs. Local runs compare configurations (1 vs N replicas); they do not predict production capacity.
- Stop the replicas with `docker compose --profile lb stop api-lb nginx`.
- Measured on an M4 Pro Mac (one 6-CPU Docker Desktop VM shared by all containers) with the 500k-product catalog, three interleaved `load browse --concurrency 100 --duration 60s` runs each. 1 replica: 7,016 ± 95 req/s, p99 37.0 ms. 4 replicas: 14,134 ± 1,884 req/s (2.0×), p99 37.2 ms, requests split exactly 25% per replica. At a fixed 4,900 req/s, p99 was 52 ms to 4.8 s with 1 replica (pinned at its CPU limit) and 2.5 ms with 4. Starting from an empty cache at that rate, 4 replicas used to collapse; they now shed 5.4% of requests as 503s in the first 15 to 20 s and then hold the rate. Analysis in [ADR.md §11](ADR.md#11-horizontal-scaling-1-vs-4-replicas-behind-nginx); method and raw results in [`docs/load-tests/2026-09-25-rerun/`](docs/load-tests/2026-09-25-rerun/summary.md).

## Tests

```bash
docker compose up -d postgres redis localstack   # required: integration tests use the real services
pnpm test                                        # all packages, one at a time (they share one database)
pnpm typecheck
```

- `pnpm test` runs `packages/core`, `apps/api` and `apps/ingest` serially (`--workspace-concurrency=1`) because each truncates the same Postgres database. Tests wipe the tables, so re-run `pnpm seed` afterwards if you want demo data.
- `pnpm test:unit` runs only `packages/core`. It still needs Postgres and Redis for five of its twelve files (schema, queries, db client, versions, read-through). The pure tests alone (money, pricing, slugs, CSV, chunking, cache keys) run with no infrastructure:

  ```bash
  pnpm --filter @modaco/core exec vitest run src/money.test.ts src/slug.test.ts src/pricing src/ingest src/cache/keys.test.ts src/cache/redis.test.ts
  ```

## Deploying the ingestion pipeline to AWS

```bash
pnpm build:lambda                                  # esbuild bundle -> apps/ingest/dist/lambda.mjs
sam deploy --guided --template infra/template.yaml
```

`infra/template.yaml` creates the uploads bucket (`<stack-name>-vendor-uploads`), the chunk queue (visibility timeout 360 s, redrive to the DLQ after 3 receives), the DLQ, a splitter-failure queue (the splitter's `OnFailure` destination after 2 async retries; ADR §6.4), and three functions (splitter, worker with reserved concurrency 10 and `ScalingConfig.MaximumConcurrency` 10, dead-letter handler) on Node 22 arm64, 256 MB.

Required parameters:

- `DatabaseUrl`: Postgres connection string. In production point this at an **RDS Proxy** endpoint: each worker holds a pool of 2, and the proxy multiplexes Lambda connections onto a small set of database connections.
- `RedisUrl`: Redis (e.g. ElastiCache) URL, used for version bumps and stock counters.

Optional: `ChunkSizeBytes` (4194304), `UpsertBatchSize` (1000), `WorkerReservedConcurrency` (10). The template has no `VpcConfig`; if Postgres and Redis are in a VPC, add one to the functions. The API is not part of the template. To use the deployed bucket, set the API's `S3_BUCKET` to the stack's bucket name and `S3_PUBLIC_ENDPOINT` to the regional S3 endpoint (for example `https://s3.us-east-1.amazonaws.com`). The API's only S3 client is the presigner, which never calls S3 itself: it signs `PUT` URLs against that explicit endpoint (path-style), so the endpoint must be one the uploader can reach. `AWS_ENDPOINT_URL` is not read by the API. The presigner's credentials come from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (falling back to `test`, no session token; see `apps/api/src/deps.ts`), so a deployed API needs static access keys rather than an IAM role or temporary credentials (ADR §10).

## Layout

```
apps/api/            Express API: products, promotions, ingestion jobs; integration tests
apps/ingest/         Lambda handlers (splitter, worker, dead-letter), local runner, esbuild bundle
apps/cli/            modaco CLI: operational commands and the load generator (pnpm modaco)
packages/core/       Drizzle schema and migrations, effective-price SQL, pricing rules, cache helpers, chunking
infra/template.yaml  SAM template for the ingestion pipeline
infra/nginx/         nginx load balancer config for the Compose "lb" profile
docker/localstack/   LocalStack init script: bucket, queues, redrive, S3 notification
scripts/             seed, vendor file generator, ingest and flash-sale demos, schema export
docs/superpowers/    design spec and implementation plan
docker-compose.yml   postgres, redis, localstack; api and runner under profile "app"
Dockerfile           node:22-alpine image for the api and runner services
schema.sql           PostgreSQL DDL generated from the migrations (pnpm schema:export)
```

## Further reading

- [ADR.md](ADR.md): architecture decisions, trade-offs, measurements, known limitations.
- [AI_APPENDIX.md](AI_APPENDIX.md): Form 5, how AI was used and which of its mistakes were caught.
- [schema.sql](schema.sql): the full DDL, regenerated from `packages/core/drizzle` with `pnpm schema:export`.
