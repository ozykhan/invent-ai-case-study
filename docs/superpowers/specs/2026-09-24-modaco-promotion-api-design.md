# ModaCo Promotion Management API — Design

Date: 2026-09-24
Status: Approved

> **This is the pre-implementation design.** Where it differs from the implementation, `ADR.md` and the code govern. The two places below that described behavior the code does not have (the worker's chunk line rule in §7 and the `ver:product` bump in §5) have been amended to match it.

## 1. Goal

Build the ModaCo internal API for products and promotions as described in the case study, with structural answers to the two scalability scenarios:

- Scenario A: ingest weekly vendor files of 500,000+ rows through application-layer pricing rules, on a serverless consumption plan with short timeouts, small memory, and no state between invocations.
- Scenario B: a category promotion that instantly affects 50,000+ products while the product listing endpoint is under heavy read load, and a product added to that category during the sale automatically gets the discount.

Deliverables in the repo root: code, `schema.sql` (DDL), `ADR.md`, `AI_APPENDIX.md` (Form 5), `README.md`.

## 2. Decisions

| Topic | Decision |
|---|---|
| Runtime | Node.js, Express, TypeScript |
| Serverless target | AWS Lambda, S3, SQS |
| Local environment | Docker Compose with Postgres, Redis, LocalStack (S3, SQS), the API, and a local Lambda runner |
| Database / ORM | PostgreSQL with Drizzle ORM |
| Effective price | Computed at read time in SQL, cached in Redis with version-stamped keys |
| Stock | Never in the catalog cache. Read live from a Redis counter backed by Postgres |
| Promotion conflicts | Most recent wins, resolved at read time. Overlaps are allowed |
| Pricing rules | Small deterministic pipeline in a shared package |
| Ingestion mechanism | Presigned S3 upload, splitter Lambda enqueues byte ranges, worker Lambdas process chunks |

## 3. Repository layout

```
apps/api           Express service: products, promotions, ingestion jobs
apps/ingest        Lambda handlers: splitter and worker, plus AWS event adapters
packages/core      Drizzle schema and migrations, effective-price SQL, pricing rules,
                   promotion resolution, cache key and version helpers, money helper
infra/             SAM template: bucket, chunk queue, dead letter queue, two functions,
                   S3 notification, memory, timeout, reserved concurrency
docker-compose.yml Postgres, Redis, LocalStack, API, local Lambda runner
scripts/           seed data, 500k-row vendor file generator, load demo scripts
ADR.md, AI_APPENDIX.md, schema.sql, README.md
```

Tooling: pnpm workspaces, Vitest, Zod, esbuild for Lambda bundles, integration tests run against the compose stack.

## 4. Data model

### categories
- `id`, `name` (unique), `slug` (unique)
- `margin_pct` numeric, `price_floor` numeric(12,2), `price_ceiling` numeric(12,2), all with defaults. Used by the pricing rules.

### products
- `id`, `sku` (unique), `name`, `category_id` FK, `base_price` numeric(12,2), `stock` integer, `created_at`, `updated_at`
- Index on `(category_id, base_price)`.
- Sorting by effective price cannot use an index because the price depends on a join. The listing query sorts in SQL and the Redis listing cache absorbs the load. This trade-off is stated in the ADR.

### promotions
- `id`, `name`, `discount_type` enum (`percentage`, `fixed`), `value` numeric(12,2), `starts_at`, `ends_at`, `scope` enum (`product`, `category`), `product_id` nullable FK, `category_id` nullable FK, `cancelled_at` nullable, `created_at`
- Check: exactly one of `product_id` or `category_id` is set and matches `scope`.
- Check: `ends_at > starts_at`. Check: percentage value between 0 and 100. Check: value non-negative.
- Partial index on `(category_id, starts_at, ends_at) where cancelled_at is null` and the same for `product_id`.
- "Active" is derived: `cancelled_at is null and starts_at <= now() and now() < ends_at`. Nothing is stored.

### ingestion_jobs
- `id`, `s3_key`, `status` enum (`pending`, `splitting`, `processing`, `completed`, `failed`), `total_chunks`, `completed_chunks`, `failed_chunks`, `rows_processed`, `rows_rejected`, `error` nullable, `created_at`, `updated_at`

### ingestion_chunks
- `id`, `job_id` FK, `chunk_index`, `byte_start`, `byte_end`, `status` enum (`pending`, `processing`, `completed`, `failed`), `attempts`, `rows_processed`, `rows_rejected`, `error` nullable, `updated_at`
- Unique on `(job_id, chunk_index)`. Completion is written in the same transaction as the chunk's last batch. A redelivered message that finds `completed` exits without work. This is the idempotency guard.

### ingestion_rejections
- `id`, `job_id` FK, `chunk_index`, `line_number`, `raw_line`, `reason`

### Effective price resolution (SQL, defined once in core)

For each product, a lateral subquery selects the single promotion that is active and targets either the product directly or the product's category, ordered by `created_at desc`, limit 1. Effective price is a case expression:

- percentage: `round(base_price * (1 - value / 100), 2)`
- fixed: `greatest(base_price - value, 0)`
- no promotion: `base_price`

The same fragment is used by the listing sort, the listing projection, and the single product endpoint.

## 5. Redis keys

| Key | Value | Purpose |
|---|---|---|
| `ver:category:{id}` | integer | bumped on any promotion or ingestion change touching the category |
| `ver:product:{id}` | integer | bumped on a product-scoped promotion change (create, cancel, or assign to or away from the product). Product create bumps its `ver:category` and `ver:all` instead |
| `ver:all` | integer | bumped whenever any category version is bumped; guards the uncategorized listing |
| `product:{id}` | JSON `{ productVersion, categoryVersion, categoryId, record }`, record has no stock | single product cache |
| `list:{categoryId or all}:{sort}:{page}:{size}` | JSON `{ version, items, total }`, items have no stock | listing cache |
| `category:slug:{slug}` | integer category id | slug lookup for listings, long TTL |
| `stock:{productId}` | integer | live stock, source of truth is Postgres, rebuilt on miss |
| `lock:{cacheKey}` | short TTL | request coalescing on rebuild |

Cache keys are not version-stamped. Each cached value records the version numbers it was built under, and a read compares them against the current version counters. A mismatch is treated as a miss and the entry is rebuilt and overwritten. This gives the same instant invalidation as version-stamped keys, without needing to know a product's category before reading it, and it leaves no orphaned entries. Warm path for a single product is two Redis round trips: one `MGET` for the product version and the entry, then one `MGET` for the category version and stock.

Version bumps happen after the database commit. A bump that fails is retried three times and then logged as an alert.

### TTL rule
Each cached entry's TTL is the minimum of the default TTL (five minutes) and the time until the next promotion boundary (`starts_at` or `ends_at`) for the product and its category. This makes scheduled activations and expiries take effect with no scheduler. The five minute cap is also the self-healing bound for any missed version bump.

### Rebuild rule
On a cache miss, the reader tries to acquire `lock:{key}`. The lock holder queries Postgres, writes the entry with the computed TTL, and releases the lock. Other readers wait up to a short bound (200 ms) for the entry to appear, then fall through to Postgres without writing the cache.

## 6. API

All bodies and queries validated with Zod. Money is a string in JSON and numeric in Postgres. No authentication (internal API, gateway assumed in the ADR). Structured JSON logs with a request id. `GET /health` checks Postgres and Redis.

### Products
- `GET /products?category=<slug>&sort=effective_price|-effective_price&page=1&pageSize=20`
  - `category` optional. `pageSize` max 100. Default sort `effective_price` ascending.
  - Item shape: `id, sku, name, category {id, name, slug}, basePrice, effectivePrice, activePromotion {id, name, discountType, value} | null, stock`
  - Response: `{ items, pagination: { page, pageSize, total } }`
- `GET /products/:id` — same item shape. Warm path: one Redis get for the entry, one for stock, no Postgres.
- `POST /products` — `sku, name, categoryId, basePrice, stock`. Bumps the category version so listings include the product, and it inherits any active category promotion immediately.
- `PATCH /products/:id/stock` — `{ delta }` or `{ stock }`. Writes Postgres, then sets `stock:{id}`.

### Promotions
- `POST /promotions` — `name, discountType, value, startsAt, endsAt, target: { productId } | { categoryId }`. 201. Bumps the target version after commit.
- `POST /promotions/:id/cancel` — sets `cancelled_at`, bumps target version. Idempotent.
- `PUT /promotions/:id/target` — the assign operation. Moves the promotion to another product or category. Bumps old and new target versions.
- `GET /promotions/:id`

### Ingestion
- `POST /ingestion/jobs` — optional `filename`. Creates a pending job, returns `{ jobId, uploadUrl, key }`. Key format `uploads/{jobId}/{filename}`.
- `GET /ingestion/jobs/:id` — status and counters.
- `GET /ingestion/jobs/:id/rejections?page&pageSize`

### Error envelope
`{ error: { code, message, details? } }`. 400 validation, 404 not found, 422 semantic promotion violations, 503 database unavailable, 500 unexpected.

## 7. Ingestion flow

Vendor file: CSV, header row, columns `sku,name,category,vendor_price,stock`, UTF-8, no embedded newlines.

### Splitter Lambda
1. Triggered by S3 object-created on `uploads/`. Parses the job id from the key.
2. HEAD request for content length. Never reads the body.
3. Computes fixed-size chunks (default 4 MB, env `CHUNK_SIZE_BYTES`) arithmetically.
4. In one transaction: inserts chunk rows (on conflict do nothing on `(job_id, chunk_index)`), sets job status `processing` and `total_chunks`.
5. Sends one SQS message per chunk `{ jobId, chunkIndex, byteStart, byteEnd }` in batches of ten.

### Worker Lambda
1. Batch size one. Loads the chunk row. If `completed`, exits. Otherwise increments `attempts`, sets `processing`.
2. S3 range GET from `byteStart - 1` (one byte early; `0` for chunk zero) to `byteEnd + 65536`. The chunk owns exactly the lines whose first byte lies in `[byteStart, byteEnd]`: the early byte shows whether `byteStart` begins a line (the byte before it is a newline) or falls mid-line (the partial line belongs to the previous chunk). Stops at the first line that starts past `byteEnd`. Chunk zero skips the header. Every line is processed exactly once across chunks.
3. Streams lines: parse, Zod validate, run pricing rules. Failures go to a rejections buffer with a reason.
4. Batches of 1000 rows: upsert categories by name (new categories get default margin, floor, ceiling), multi-row insert into products with on-conflict-on-sku update of `name, category_id, base_price, stock, updated_at`, insert rejections. One transaction per batch.
5. After each batch commit: bump `ver:category` for each category touched, bump `ver:all`, set `stock:{id}` for upserted products.
6. On completion: mark chunk `completed` with counts, atomically increment job counters. When `completed_chunks + failed_chunks = total_chunks`, set job `completed` or `failed`.

Memory is bounded by one batch plus the stream buffer. Target 256 MB, 60 second timeout. A 4 MB chunk is roughly 50k rows and takes single-digit seconds locally.

### Pricing rules (packages/core, run in order)
1. Validate: sku non-empty, name non-empty, vendor_price > 0, stock >= 0 integer. Reject otherwise.
2. Margin: `price = vendor_price * (1 + margin_pct / 100)` using the category's margin.
3. Price point: round up to the nearest `.99`.
4. Bounds: clamp to the category's floor and ceiling.

### Failure handling
- Uncaught error rethrows; SQS redelivers after the visibility timeout (six times the function timeout). After three receives the message goes to the dead letter queue. A DLQ consumer marks the chunk and the job `failed` with the error text.
- A job with any failed chunk is `failed`, never silently partial.
- Reserved concurrency on the worker: 10 by default, pool size 2 per invocation. RDS Proxy is the production answer, noted in the ADR.

### Local runner
A Node process in the compose stack polls an SQS queue subscribed to LocalStack S3 notifications and invokes the splitter, then polls the chunk queue and invokes the worker per message. Each invocation is wrapped in a timeout equal to the configured Lambda timeout and aborted on overrun, and the process runs with `--max-old-space-size` matching the configured memory. Timeouts are counted as failures and the message is returned to the queue.

## 8. Error handling summary

- Redis is a soft dependency for reads: any cache failure logs and falls through to Postgres. Stock falls back to the Postgres column.
- Postgres write failure returns 503, no API-side retry.
- All multi-statement writes are transactional.
- Missed version bumps self-heal within the five minute TTL cap.

## 9. Testing

### Unit (packages/core)
- Pricing rules including edge values and rejections.
- Promotion resolution: overlapping product and category promotions, most recent wins, cancelled and out-of-window ignored.
- Effective price for both discount types including the zero floor and rounding.
- Cache key construction and TTL calculation including next-boundary logic.
- Byte-range line alignment: chunks starting and ending mid-line, chunk zero header skip, final chunk without trailing newline.

### Integration (compose stack)
- Listing: filter, pagination, both sort directions checked against a direct SQL computation.
- Single product: hit and miss paths, Redis down fallback.
- Promotion create and cancel invalidate cached prices.
- Product created in a category with an active promotion appears discounted immediately.
- Stock change visible immediately while the catalog entry stays cached.
- Ingestion: small file with a tiny chunk size produces many chunks, every row lands exactly once, rejections recorded, job counters correct, redelivered chunk is a no-op, forced worker failure reaches `failed` via the DLQ path.

### Demo scripts (not tests)
- Generate and ingest a 500k row file, print job progress and elapsed time.
- Concurrent listing load while a category promotion is created; report latency before and after the flip.

Tests are written before implementation for each unit.

## 10. Out of scope

Authentication, rate limiting, multi-currency, promotion stacking, product deletion, a scheduler for promotion activation (TTL covers it), editable pricing rules via API.

## 11. Trade-offs to carry into ADR.md

- Read-time effective price versus materialized column: O(1) promotion writes and automatic inclusion of new products, at the cost of a join on every uncached read and no index for effective-price sort.
- Version counters validated on read versus explicit deletes: instant invalidation of arbitrarily many entries with one increment, at the cost of one extra version lookup per read.
- Most-recent-wins conflict rule: simplest consistent rule, but a category sale can shadow a better product promotion.
- Split stock out of the cache: correct stock at the cost of one extra Redis read per item.
- Byte-range fan-out: splitter cost independent of file size and parallel workers, at the cost of line-alignment logic and a bounded over-read per chunk.
- Batch size one on the worker queue: fine-grained retries at the cost of more invocations.
