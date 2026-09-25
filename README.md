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

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Postgres and Redis checks; 503 only if Postgres is down |
| GET | `/products?category=&sort=&page=&pageSize=` | List with effective price. `category` is a slug; `sort` is `effective_price` (default) or `-effective_price`; `pageSize` max 100 |
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

## Tests

```bash
docker compose up -d postgres redis localstack   # required: integration tests use the real services
pnpm test                                        # all packages, one at a time (they share one database)
pnpm typecheck
```

- `pnpm test` runs `packages/core`, `apps/api` and `apps/ingest` serially (`--workspace-concurrency=1`) because each truncates the same Postgres database. Tests wipe the tables, so re-run `pnpm seed` afterwards if you want demo data.
- `pnpm test:unit` runs only `packages/core`. It still needs Postgres and Redis for four of its ten files (schema, queries, versions, read-through). The pure tests alone (money, pricing, CSV, chunking, cache keys) run with no infrastructure:

  ```bash
  pnpm --filter @modaco/core exec vitest run src/money.test.ts src/pricing src/ingest src/cache/keys.test.ts src/cache/redis.test.ts
  ```

## Deploying the ingestion pipeline to AWS

```bash
pnpm build:lambda                                  # esbuild bundle -> apps/ingest/dist/lambda.mjs
sam deploy --guided --template infra/template.yaml
```

`infra/template.yaml` creates the uploads bucket (`<stack-name>-vendor-uploads`), the chunk queue (visibility timeout 360 s, redrive to the DLQ after 3 receives), the DLQ, and three functions (splitter, worker with reserved concurrency 10 and `ScalingConfig.MaximumConcurrency` 10, dead-letter handler) on Node 22 arm64, 256 MB.

Required parameters:

- `DatabaseUrl`: Postgres connection string. In production point this at an **RDS Proxy** endpoint: each worker holds a pool of 2, and the proxy multiplexes Lambda connections onto a small set of database connections.
- `RedisUrl`: Redis (e.g. ElastiCache) URL, used for version bumps and stock counters.

Optional: `ChunkSizeBytes` (4194304), `UpsertBatchSize` (1000), `WorkerReservedConcurrency` (10). The template has no `VpcConfig`; if Postgres and Redis are in a VPC, add one to the functions. The API is not part of the template. To use the deployed bucket, set the API's `S3_BUCKET` to the stack's bucket name and both `AWS_ENDPOINT_URL` and `S3_PUBLIC_ENDPOINT` to the regional S3 endpoint (for example `https://s3.us-east-1.amazonaws.com`), because the API always uses an explicit endpoint. The API also builds its S3 credentials from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (falling back to `test`, no session token; see `apps/api/src/deps.ts`), so a deployed API needs static access keys rather than an IAM role or temporary credentials (ADR §10).

## Layout

```
apps/api/            Express API: products, promotions, ingestion jobs; integration tests
apps/ingest/         Lambda handlers (splitter, worker, dead-letter), local runner, esbuild bundle
packages/core/       Drizzle schema and migrations, effective-price SQL, pricing rules, cache helpers, chunking
infra/template.yaml  SAM template for the ingestion pipeline
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
