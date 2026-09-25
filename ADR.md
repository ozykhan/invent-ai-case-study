# Architecture Decision Record: ModaCo Promotion Management API

Status: accepted. Date: 2026-09-25.

## 1. Context

ModaCo needs an internal API for products (name, category, SKU, base price, stock) and promotions (percentage or fixed, a validity window, assigned to one product or a whole category). A product has at most one active promotion at a time, listings must return the effective price, and conflicts must be resolved consistently. Required endpoints: list products (category filter, pagination, sort by effective price), product detail (the hottest endpoint), and create, cancel and assign promotions.

Two scenarios carry most of the evaluation:

- **Scenario A, massive ingestion.** Weekly vendor files of 500,000+ rows. Every row must go through application-layer pricing rules before it is saved. The ingestion unit must run on a serverless consumption plan: short timeouts, little memory, no state between invocations.
- **Scenario B, flash sales.** A promotion such as "50% off all accessories" affects 50,000+ products the moment it is created, while `GET /products` is under heavy read load. A product added to the category during the sale must get the discount automatically.

Fixed constraints: Node.js, Express, TypeScript. The database, ORM and serverless platform were ours to choose.

### Decisions at a glance

Where a simpler and a more rigorous option existed, this project took the simpler one and records the rigorous one here, with the point at which it would be worth switching.

| Topic | Chosen | Rigorous alternative | Switch when |
|---|---|---|---|
| Effective price | Computed at read time in SQL (§3) | Precomputed column refreshed on promotion writes, indexed for sort | Cache-miss rebuilds of large sorted categories dominate (§5) |
| Cache invalidation | Version counters embedded in entries, checked on read (§4) | Version-stamped keys, or pub/sub to in-process caches | An in-process L1 cache is added |
| Conflict rule | Most recent `created_at` wins (§7) | Explicit precedence or best price, overlap checks on write | Merchandisers need to protect specific promotions |
| Scheduled promotions | TTL capped at the next promotion boundary (§4) | A scheduler that bumps versions at each boundary | Never needed so far; the TTL cap is exact |
| Ingestion fan-out | Byte-range chunks from object size (§6) | Step Functions Distributed Map over the S3 object | The team is AWS-only and wants managed retries and visibility |
| Line ownership | First byte decides the owning chunk (§6.2) | Splitter pre-scans for newline offsets | Never; the pre-scan reintroduces file-size-bound splitter time |
| Worker batching | SQS batch size 1 (§6.4) | Batch size > 1 with partial batch responses | Invocation count or cost matters more than retry granularity |
| Connection pooling | Pool of 2 per worker, reserved concurrency 10 | RDS Proxy in front of Postgres | Any production deployment |

## 2. Stack: PostgreSQL + Drizzle, Redis, AWS Lambda/S3/SQS, LocalStack

- **PostgreSQL 16.** `numeric(12,2)` for money, so no float rounding. Check constraints enforce the promotion invariants in the database: exactly one target that matches `scope`, `ends_at > starts_at`, value non-negative, percentage at most 100. A `LEFT JOIN LATERAL` picks one promotion per product in a single query, partial indexes on `(category_id, starts_at, ends_at) where cancelled_at is null` (and the same for `product_id`) keep that lookup cheap, and `INSERT ... ON CONFLICT (sku) DO UPDATE` makes the ingestion upsert idempotent.
- **Drizzle ORM.** The schema and migrations are typed, and SQL stays visible where it matters. The effective-price expression is one SQL fragment (`effectivePriceExpr` in `packages/core/src/products/queries.ts`) used by the listing projection, the listing sort and the detail query, so the three cannot disagree. `schema.sql` is generated from the committed migrations.
- **Redis.** It holds three things: version counters (`INCR`), cached catalog entries, and live stock counters. The client fails fast (300 ms command timeout, no offline queue), so an unreachable Redis costs a read at most a few hundred milliseconds before it falls through to Postgres.
- **AWS Lambda, S3, SQS.** Lambda is the consumption plan. S3 with presigned uploads keeps 30+ MB files out of the API. SQS gives at-least-once delivery, visibility-timeout retries and a dead-letter queue without writing any of that ourselves.
- **LocalStack.** A reviewer can run the whole pipeline (S3 notification, SQS, redrive to the DLQ) locally: `docker compose up` starts the infrastructure, and the API and runner run either with `pnpm dev:api` / `pnpm dev:runner` or in containers with `docker compose --profile app up`. A local runner stands in for the Lambda service (§6.5). The same handler code is bundled with esbuild and deployed through `infra/template.yaml` (SAM).

## 3. Effective price is computed at read time, never stored

**Context.** A category promotion touches 50k+ products at once. A product created mid-sale must inherit it. Promotions also start and end on a schedule.

**Decision.** Nothing price-derived is stored. For each product, a lateral subquery selects the single promotion that is not cancelled, is inside its window (`starts_at <= now < ends_at`), and targets either the product or the product's category, ordered by `created_at desc, id desc limit 1`. A `CASE` expression computes the price: `round(base * (1 - v/100), 2)` for percentage, `greatest(base - v, 0)` for fixed, the base price when there is no promotion. "Active" is always derived, never a stored flag.

**Consequences.**
- Creating, cancelling or reassigning a promotion writes one row, whether it covers one product or 60,000.
- New products qualify automatically: the lateral join finds the category promotion on the product's first read. There is nothing to backfill.
- Scheduled starts and ends need no job: the window check uses the query's `now`.
- There is one source of truth, so a price and its promotion cannot drift apart.
- Cost: every uncached read runs the join, and **sorting by effective price cannot use an index**. A listing page sorted by effective price has to evaluate the lateral subquery for every product in the filtered set and sort all of them, even to return 20 rows. At 62k products that is a real query (see §5). The cache (§4) absorbs it on the warm path; it does not make a rebuild cheaper.

**Rejected for now: a materialized `effective_price` column.** It makes sorting indexable, but a category promotion becomes a 50k-row `UPDATE` inside the write transaction (lock contention with ingestion upserts on the same rows), new products need a trigger or application hook to inherit the active promotion, schedules need a job to flip prices at `starts_at`/`ends_at`, and there are two sources of truth to keep consistent. **This is the rigorous alternative to adopt first at higher scale** (§5, §9): a denormalized column (or a `product_prices` table) refreshed asynchronously after promotion writes, with an index on `(category_id, effective_price, id)` and keyset pagination.

## 4. Cache with version counters validated on read

**Context.** Heavy reads during flash sales; a promotion must be visible immediately on every affected product and page; stock changes on every sale.

**Decision.**
- **Counters.** `ver:product:{id}`, `ver:category:{id}` and `ver:all` are Redis integers. A promotion write bumps its counters after the database commit, never inside the transaction, with one pipelined round of `INCR`s per affected target: create and cancel issue one, an assign issues two (the old target, then the new one). A category-scoped promotion bumps its category and `ver:all`. A product-scoped promotion bumps the product, the product's category and `ver:all`, because it moves the product within category and all-products listings. Bumps are retried three times and then logged; they never fail the request.
- **Embedded versions.** Keys are not version-stamped. Each entry stores the versions it was built under: `product:{id}` holds `{productVersion, categoryVersion, categoryId, record}`, `list:{cat|all}:{sort}:{page}:{size}` holds `{version, items, total}`. A read compares them against the live counters; a mismatch is treated as a miss and the entry is overwritten.
- **Warm path round trips.** Product detail takes two: `MGET(product:{id}, ver:product:{id})`, then `MGET(ver:category:{cid}, stock:{id})`. A listing takes two: `MGET(entry, version)`, then `MGET` of the page's stock counters. A category-filtered listing adds a slug lookup, so three.
- **Build order.** On a rebuild, the category version is read before the price query runs. If it were read after, a promotion committing in between would stamp old prices with the new version and they would look fresh for the whole TTL.
- **Stock is kept out of catalog entries.** It lives in `stock:{id}` counters with a 300 s TTL, the same bound as the cache entries. A miss reads Postgres and backfills with `SET NX`, so a backfill never overwrites a counter written after its read. Every stock write and ingestion batch sets the counter after its commit; if that `SET` fails, the key is deleted (best effort) so the next read goes to Postgres instead of the old value. Stock changes never invalidate cached prices. Stock can still be stale in one bounded case: two concurrent writes whose `SET`s reach Redis out of commit order leave the older value for up to 300 s, or until the next write (§10).
- **TTL.** Each entry's TTL is `min(300 s, time until the next promotion boundary)`, where a boundary is the next `starts_at` of a scheduled promotion or the next `ends_at` of an active one for that product, its category, or (for list pages) the products in the category. A scheduled promotion therefore takes effect when its entries expire, at its start time, with no scheduler. The 300 s cap is also the self-healing bound for a lost bump.
- **Coalescing.** On a miss, one reader takes `lock:{key}` (`SET NX PX 2000`) and rebuilds. The others poll for 200 ms, then query Postgres directly without writing the cache.
- **Degradation.** Every Redis failure on the read path is logged and falls through to Postgres. An entry built while a version read failed is served but not cached. Stock falls back to the Postgres column. No read endpoint fails because of Redis.

**Consequences.** One `INCR` invalidates any number of entries, and stale entries are overwritten in place instead of leaving orphans. The cost is one extra lookup per read, up to 200 ms of added latency for readers that wait on a rebuild (they poll Redis for the new entry and query Postgres only if it has not appeared by then), and a stale window of at most 5 minutes if Redis loses a bump. Because `ver:all` moves with every category change, the all-products listing is invalidated by any change anywhere, including every ingestion batch.

**Rejected.**
- *Delete by pattern.* `SCAN` plus `DEL` over 50k+ keys on every promotion write is slow and not atomic: readers repopulate keys while the scan runs.
- *Version-stamped keys* (`product:{id}:v{pv}:c{cv}`). This was the first design. A cold read cannot build the key, because the product's category, and so its category version, is unknown until the product has been read. It would need an extra id-to-category lookup on every read. Embedded versions give the same invalidation semantics.
- *Pub/sub invalidation.* It only pays off with in-process caches on each API instance. Pub/sub delivery is fire-and-forget, so a missed message has the same failure mode as a missed bump, plus a subscriber per instance. Worth revisiting if an L1 cache is added.

## 5. Scenario B walkthrough and measurement

**Flow of `POST /promotions` during load.**
1. Validate the body (Zod), check that the target exists, insert one row. The check constraints enforce the window and value rules.
2. After the commit, `INCR ver:category:{id}` and `ver:all`.
3. The next read of any cached list page for that category sees a version mismatch. One reader per page key rebuilds from Postgres with the new promotion applied; the others wait up to 200 ms and then query Postgres themselves.
4. Cached product details are not touched. Each one fails its category-version check on its next read and is rebuilt individually. There is no mass write.
5. **Product created mid-sale.** `POST /products` inserts the row and bumps the category version, so listings pick the product up. Its first read runs the lateral join, which finds the category promotion. The product is discounted immediately, and no promotion data was written for it.
6. **Cancellation** is a single conditional `UPDATE ... where cancelled_at is null` (compare-and-set, so it is idempotent under concurrency), followed by the same bump.

**Measurement.** `pnpm demo:flash-sale accessories`, run after ingesting the 500k-row file, so the category held **62,479 products**. 50 concurrent clients request pages 1 to 5 (20 items, sorted by effective price). A 4 s warm-up is not recorded. Then a 5 s warm-cache window, the 50% category promotion is created, then a 10 s window during which the script also creates a product in the category and checks it.

| Window | Requests | req/s | p50 | p95 | p99 |
|---|---|---|---|---|---|
| Warm cache, before the promotion | 46,829 | 9,354 | 5.2 ms | 6.7 ms | 9.2 ms |
| After the promotion (includes rebuilds) | 52,712 | 5,269 | 5.5 ms | 6.9 ms | 9.4 ms |

- **0 errors across 99,541 requests.** The first item's price went from 1.99 to 1.00. The mid-sale check **passed**: a product created during the sale with base price 20.00 read back with effective price 10.00 and the new promotion attached, on its first read.
- **p95 barely moved (6.7 to 6.9 ms).** Once the pages are rebuilt, a discounted page costs the same to serve as an undiscounted one.
- **Throughput dropped from about 9.4k to 5.3k req/s.** This is the cost of the design in §3, and the percentiles hide it. With 50 clients in a closed loop, mean latency is about 50 / 5,269 ≈ 9.5 ms after the flip against 50 / 9,354 ≈ 5.3 ms before, while p99 is 9.4 ms. So the lost time sits in fewer than 1% of requests. Those are the rebuilds. Each invalidated page is re-sorted against the full 62k-product category, with the lateral promotion lookup running once per product. Readers that wait longer than 200 ms run the same full query directly, so for a moment several full-category sorts hit Postgres at once. The window also contains more than one invalidation: creating the mid-sale product bumps the category version again, and the check then reads five 100-item pages that were never cached. We did not instrument individual rebuild times; this reading comes from the numbers above and the code path.
- **What would fix it, in the order we would try:** (1) a precomputed effective price, either a denormalized column or a materialized table refreshed on promotion writes, with an index on `(category_id, effective_price, id)`, so a page becomes an index range scan instead of a full sort; (2) keyset pagination on that index, so deep pages cost the same as page 1; (3) serve-stale-while-revalidate, which keeps serving the previous entry while one reader rebuilds instead of sending waiters to Postgres, and warming the first few pages right after a promotion write.
- **These are local laptop numbers.** The API, Postgres, Redis, LocalStack and the load generator shared one machine through Docker Desktop. They show relative behavior (before vs after, errors, correctness), not what AWS would do.

## 6. Scenario A: byte-range fan-out on Lambda

### 6.1 Decision

1. `POST /ingestion/jobs` creates a `pending` job and returns a presigned S3 `PUT` URL for `uploads/{jobId}/{filename}` (15 minutes). The file never passes through the API.
2. The S3 `ObjectCreated` event triggers the **splitter**. It issues a `HEAD` for the object size and never reads the body. It computes fixed 4 MiB byte ranges arithmetically, inserts the chunk rows and sets the job to `processing` in one transaction, and enqueues one SQS message per chunk (`{jobId, chunkIndex, byteStart, byteEnd}`) in batches of 10. Its runtime depends on the chunk count, not on the bytes.
3. Each **worker** invocation (SQS batch size 1) streams its byte range with a ranged `GET`, keeps the lines it owns (§6.2), parses and validates each row, runs the pricing rules from `packages/core` (§8), and upserts in batches of 1,000: categories by name, then products with `ON CONFLICT (sku) DO UPDATE`, with duplicate SKUs within a batch collapsed to the last occurrence (Postgres refuses to update one row twice in a single statement). Rejected rows go to `ingestion_rejections` with a reason, with any NUL stripped from the stored raw line. If the database still refuses a batch with a data error (SQLSTATE class 22 or 23), which validation is meant to make impossible, the worker retries that batch row by row and records the rows it refuses as rejections, so one bad row cannot fail its chunk on every retry. After each batch commits, the worker sets the batch's stock counters and bumps the touched categories' versions (and `ver:all`), so the storefront sees new prices within one batch.
4. When the chunk is done, the worker marks it `completed` and adds its counts to the job in one transaction. The delivery that completes the last chunk sets the job to `completed`, or to `failed` if any chunk failed.

**Consequences.** Splitter time is independent of file size. Worker memory is bounded by one batch of 1,000 rows plus the stream buffer and a 64 KiB over-read, regardless of file size. Parallelism equals the worker's reserved concurrency (10). Database connections are bounded at 10 workers × 2. Costs: the line-alignment logic, one `HEAD` plus one ranged `GET` per chunk, a cold start per concurrent worker, and many more invocations than a monolithic job.

**Rejected.**
- *Streaming splitter that rewrites the file into chunk objects.* Its runtime grows with file size and eventually hits the timeout, which is the problem we are solving. It also doubles S3 storage and writes.
- *A single Lambda that processes rows and re-invokes itself with a checkpoint.* It is sequential, so wall time is roughly total rows ÷ single-worker speed. It also mixes checkpoint state with retry semantics: a retry after a partial batch has to reconcile the checkpoint.
- *Step Functions Distributed Map over the S3 object.* This is the rigorous managed alternative, with built-in batching, retries and per-item visibility. It ties the pipeline to AWS-specific orchestration and adds a state machine to emulate locally. For a case study that must run on a reviewer's laptop, plain SQS plus a small runner was simpler to test end to end.

### 6.2 Line ownership by first byte

A chunk owns exactly the lines whose **first byte** falls inside `[byteStart, byteEnd]`. The worker requests `[byteStart − 1, byteEnd + 65536]`. The extra leading byte shows whether `byteStart` begins a line (the previous byte is `\n`) or falls mid-line (then the partial line belongs to the previous chunk). The 64 KiB over-read finishes the last owned line. Lines starting after `byteEnd` are left for the next chunk. Chunk 0 validates and skips the header, tolerating a UTF-8 BOM. CRLF endings are accepted. A line longer than 64 KiB fails the chunk.

The first rule proposed was "if `byteStart > 0`, discard everything up to the first newline". It silently loses a full line whenever a boundary falls exactly after a newline, because that line starts at `byteStart` and is discarded, and the previous chunk stops at its own `byteEnd`. A test asserts that every line is yielded exactly once for many chunk sizes and stream piece sizes, including that exact case.

### 6.3 Idempotency

SQS is at-least-once, S3 events can be redelivered, and workers run concurrently. Every step is safe to repeat:

- **Splitter.** Chunk rows are inserted with `ON CONFLICT (job_id, chunk_index) DO NOTHING`. The job update only applies while the job is `pending`, `splitting` or `processing`. A redelivered S3 event for a `completed` or `failed` job is a no-op; without this guard it would have set a finished job back to `processing`. Only chunks still `pending` are re-enqueued.
- **Chunk claim.** A chunk already `completed` is skipped without writing. Otherwise the worker claims it with a conditional `UPDATE ... where status not in ('completed','failed')` that also increments `attempts`. A DLQ redrive of a failed chunk, or a delivery that lost a race, cannot reset a finished chunk to `processing`.
- **Batches.** The product upsert is idempotent: the same row produces the same values. Rejections have a unique index on `(job_id, chunk_index, line_number)` and are inserted with `ON CONFLICT DO NOTHING`, so a retried chunk does not duplicate them.
- **Completion.** The chunk's `completed` update is conditional on the chunk not being finished, and the job counters are incremented in the same transaction only when that update matched a row. Of two concurrent deliveries of one chunk, exactly one advances the job counters. A retry recomputes its counts from the whole chunk; it never adds to an earlier attempt's.
- **Difference from the spec.** The spec put chunk completion in the same transaction as the chunk's last batch. The code commits it in a separate transaction right after. A crash between the two means the retry reprocesses the whole chunk, which the rules above make safe.

### 6.4 Failure handling, dead-letter queue, concurrency cap

- A worker that throws leaves its message undeleted. It becomes visible again after the queue's **360 s visibility timeout**, six times the 60 s function timeout as AWS recommends, so a slow invocation is never duplicated while it is still running. After **3 receives** SQS moves the message to the DLQ.
- The **dead-letter handler** locks the chunk row, marks it `failed`, and marks the job `failed` immediately with the error. It does nothing if the chunk already finished. A job with a failed chunk never reports `completed`. Malformed messages on the DLQ or the S3-events queue are logged and dropped, because those queues have no redrive and would otherwise loop forever.
- **A failed job is partially applied.** Batches from the other chunks have committed. Re-running the same file is safe because the upserts are idempotent. The rigorous alternative is staging tables plus an atomic swap, which is not worth it for a weekly upsert feed.
- **Retry latency.** One transient failure delays a chunk by about 6 minutes, and a persistent one reaches the DLQ after about 18 (three receives, 6 minutes apart). We accepted this in exchange for never double-running a live invocation.
- **`ScalingConfig.MaximumConcurrency` = reserved concurrency (10).** Reserved concurrency alone is not enough. Lambda's SQS poller can scale beyond it. The extra invocations are throttled, their messages go back to the queue, and **each throttle counts as a receive**, so healthy chunks can reach the DLQ under load. Capping the event source's concurrency at the reserved concurrency prevents this. Implementation review caught this: the first template had only reserved concurrency.
- **Batch size 1.** A failure retries only one chunk. The cost is one invocation per chunk; a 500k-row file needs 9.
- **Splitter failures.** In AWS, S3 invokes the splitter asynchronously. Lambda retries a failed invocation twice (`EventInvokeConfig.MaximumRetryAttempts: 2`) and then sends the invocation record, which wraps the original S3 event, to a dedicated `SplitterFailureQueue` through an `OnFailure` destination; without one the event would be dropped and the job would stay `pending` forever. It is not the chunk DLQ, because the payload shape differs and the dead-letter handler only understands chunk messages. Nothing consumes that queue: an operator inspects and replays it, and an alarm on its depth is the obvious next step. **The local topology differs:** LocalStack sends S3 notifications to the `modaco-s3-events` SQS queue and the runner invokes the splitter from there. A failed split leaves the message undeleted, so it is retried every 60 s (the queue's visibility timeout) until it succeeds or expires; that queue has no redrive and no failure destination.

### 6.5 Local runner simulating Lambda limits

`pnpm dev:runner` polls the S3-events, chunk and DLQ queues on LocalStack. Each message runs in a **fresh child process** (`node --import tsx src/invoke.ts <handler>`, event on stdin) with a **SIGKILL at the function timeout** (60 s worker, 30 s splitter and DLQ handler) and a **V8 heap cap** of `--max-old-space-size=256`. The message is deleted only when the handler succeeds, so timeouts and crashes go through LocalStack's real redrive to the DLQ.

The first version ran the `tsx` CLI. That CLI is a wrapper that spawns a second Node process, so the timeout killed the wrapper and left the handler running. Implementation review caught it, and the runner now starts `node` directly; a test asserts that the handler's own PID is dead after a timeout.

What the runner does not simulate: container reuse (every local invocation is a cold start), Lambda's account-level concurrency (the runner handles up to 10 messages per receive concurrently), memory accounting beyond the V8 heap, and Lambda's network placement.

### 6.6 Measurement

`pnpm vendor-file --rows=500000`, then `pnpm demo:ingest tmp/vendor-500k.csv`:

- File: 500,000 rows, 33,579,173 bytes, about 0.1% deliberately invalid rows.
- **9 chunks** of 4 MiB (8 full plus a 24 KB remainder), **0 chunk failures**.
- **7.4 s end to end**, measured from job creation to the job reading `completed` (0.3 s of that was the upload, and status was polled every second), which is **about 67.5k rows/s**.
- **499,479 rows upserted, 521 rejected** with reasons (`rowsProcessed + rowsRejected = 500,000`).
- All 9 chunks ran in parallel. A full chunk (about 62k rows) took roughly 5 to 7 s including process start-up, about a tenth of the 60 s timeout. This was not measured per chunk; it is bounded by the demo's 1 s status polling.

These are **local laptop numbers against LocalStack and a Docker Postgres, not AWS**. Real Lambda adds network latency to S3 and RDS and, at 10 workers, contention on a managed database. Peak worker memory was not measured; the bound comes from the design (one batch in memory), and the local heap cap was never hit.

## 7. Promotion conflict rule: most recent wins

**Decision.** Overlapping promotions are allowed. Of the promotions active for a product (its own, or its category's), the most recently created wins (`created_at desc, id desc`). The rule is applied at read time in the same lateral subquery, so "at most one active promotion" holds by construction and nothing needs cleaning up when promotions expire or are cancelled.

**Why.** It is the simplest rule that is consistent and explainable ("the latest campaign applies"). It needs no write-time conflict detection. A product promotion and a category promotion target different columns, so a single Postgres exclusion constraint cannot express "no overlap for any product". And cancelling the newer promotion reveals the older one automatically.

**Consequences.** A later category sale shadows an earlier product promotion even when the product promotion is the better deal for the customer. Reassigning a promotion keeps its original `created_at`, so it keeps its original precedence. Because the rule is evaluated at read time from the same SQL fragment everywhere, the list, the sort and the detail view always agree.

**Rigorous alternatives:** explicit precedence (product over category, or a priority column), "best price for the customer" (`order by effective_price asc`), or rejecting overlaps at write time with a check across both scopes, done under a lock. Each is a change to the ordering or to the create path and does not change the cache design.

## 8. Pricing rules as an application-layer pipeline

**Decision.** `priceVendorRow` in `packages/core/src/pricing/rules.ts` runs four steps in order on integer cents:

1. **Validate** (Zod): SKU, name and category present and length-bounded, `vendor_price` a positive decimal with at most two places and ten integer digits (so it fits `numeric(12,2)`), `stock` a non-negative integer no larger than 2,147,483,647 (Postgres `integer`), and no field containing a NUL character (Postgres `text` cannot store one). Failures become rejections with a reason. The bounds exist because any value Postgres refuses aborts the whole 1,000-row batch.
2. **Margin**: `vendor_price × (1 + margin_pct / 100)`, using the category's margin (default 30%).
3. **Price point**: round up to the nearest `.99`.
4. **Clamp** to the category's floor and ceiling (defaults 0.99 and 99,999.99).

Categories that do not exist yet are created with the defaults. Rows are validated before any category is created, so an invalid row cannot create one. A new name that slugifies to an existing slug is rejected rather than merged.

**Why here.** The case study requires the rules to run in the application layer, not in the database. The rules are pure functions in the shared core package, which the worker imports and esbuild bundles into the Lambda artifact. They are unit-tested without infrastructure and deterministic, so a retried chunk produces identical rows. Products created through `POST /products` take the base price as given: they are internal admin input, not vendor data.

**Trade-off.** The rules are code, so changing a margin rule requires a deploy (per-category margin, floor and ceiling are data in `categories`). A rules table editable through the API is out of scope.

## 9. Operational notes and what is out of scope

- **Out of scope.** Authentication (internal API behind a gateway that authenticates), rate limiting (also the gateway's job), product deletion and general product editing (the vendor sync is the update path; the API only adjusts stock), multi-currency, promotion stacking, pricing rules editable through the API.
- **Observability.** Structured JSON logs (pino) with a request id, and `GET /health` for Postgres and Redis. Failed version bumps are logged at error level because they mean up to 5 minutes of staleness.
- **Writes.** Multi-statement writes are transactional. Postgres connection failures return 503 and the API does not retry them. Version bumps happen after commit and never fail a write.
- **What changes first at 10× scale.**
  1. Precomputed effective price with an index and keyset pagination (§3, §5). This is the known bottleneck.
  2. Serve-stale-while-revalidate, plus warming the first pages of a category right after a promotion write.
  3. Read replicas for cache rebuilds.
  4. RDS Proxy between the workers and Postgres.
  5. Larger chunks with S3 multipart uploads, and a worker batch size above 1 with partial batch responses (`ReportBatchItemFailures`).
  6. One version bump per chunk instead of per 1,000-row batch, so a large ingest does not keep invalidating the category pages.

## 10. Known limitations

Material items the per-task reviews deferred, plus one found while writing this document:

- **Cross-chunk duplicate SKUs: the winner is nondeterministic.** Within a batch the last occurrence wins. Across chunks, whichever batch commits last wins, which depends on worker timing.
- **The header is validated only in chunk 0.** A file whose header is valid but has its columns reordered fails chunk 0, but other chunks parse their rows in the expected column order and may commit wrong data before the job fails. Fix: the splitter reads the first line with a small ranged `GET` and validates it before enqueuing.
- **Re-uploading to the same key is not guarded.** The presigned URL stays valid for 15 minutes. A second upload while the job is processing re-runs the splitter against the new size but keeps the existing chunk rows; after the job has finished, the upload is ignored. Fix: record the object's ETag and size at split time and reject mismatches, or make the key single-use.
- **A short ranged read.** `ownedLines` cannot tell a body that ended early from the natural end of its range; it would price a cut-off final line as complete and drop the rest. The worker guards this by counting the bytes it receives and failing the chunk (so the queue retries it) when the total differs from the response's `ContentLength`. The guard only runs when the body is read to its end; when the worker stops past `byteEnd`, it already has every line it owns.
- **Rejection line numbers are chunk-relative.** A chunk cannot know absolute line numbers without reading every preceding chunk, so `chunk_index` is stored alongside `line_number`.
- **Category-filtered warm reads take 3 Redis round trips** (slug lookup, entry and version, stock), one more than the spec's target of 2.
- **Test coverage gaps in pricing math.** No test pins percentage rounding (for example 15% off 19.99), the window edges (`starts_at = now`, `ends_at = now`), or the tie-break between promotions with equal `created_at`. The SQL implements all three, but untested behavior can regress.
- **Stock counter ordering.** `PATCH /products/:id/stock` (and an ingestion batch) writes Postgres and then `SET`s the counter to the committed value. Two concurrent writes to one product can reach Redis out of commit order, leaving the older value in the counter until the next write or the 300 s TTL. A failed `SET` deletes the key, and a backfill after a miss uses `SET NX`, so neither of those can leave a stale counter behind; only the out-of-order case remains, bounded at 300 s. Fix: guard the `SET` with a per-product version (a Lua compare-and-set on `updated_at` or a write counter), or apply deltas with `INCRBY`.
- **The lock is released with an unconditional `DEL`.** If a rebuild outlives the 2 s lock, it can release another reader's lock. The only effect is an extra rebuild.
- **The SAM template has no `VpcConfig`**, and the API is not part of the template.
- **The API's S3 client uses static credentials.** The API has one S3 client, the presigner. `apps/api/src/deps.ts` builds it from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, with a `test` fallback and no session token, instead of the SDK's default provider chain. A deployed API therefore needs long-lived static keys and cannot use an IAM role. Fix: use the default chain outside local development, as `apps/ingest/src/aws.ts` already does.
