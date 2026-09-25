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
| Local load balancing | nginx round-robin over Compose replicas, DNS resolved at startup (§11) | AWS ALB over ECS tasks, or nginx with a `resolver` that re-resolves | Replicas change while nginx runs, or tests move to AWS |

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
- **Stock is kept out of catalog entries.** It lives in `stock:{id}` counters with a 300 s TTL, the same bound as the cache entries. A miss reads Postgres and backfills with `SET NX`, so a backfill never overwrites a counter written after its read. Every stock write and ingestion batch sets the counter after its commit; if that `SET` fails, the key is deleted (best effort) so the next read goes to Postgres instead of the old value. Stock changes never invalidate cached prices. Stock can still be stale in two bounded cases — concurrent writes whose `SET`s reach Redis out of commit order, and a backfill racing a failed write's delete — both capped at 300 s (§10).
- **TTL.** Each entry's TTL is `min(300 s, time until the next promotion boundary)`, where a boundary is the next `starts_at` of a scheduled promotion or the next `ends_at` of an active one for that product, its category, or (for list pages) the products in the category. A scheduled promotion therefore takes effect when its entries expire, at its start time, with no scheduler. The 300 s cap is also the self-healing bound for a lost bump.
- **Coalescing fails closed.** On a miss, one reader takes `lock:{key}` (`SET NX PX 30000` with a random token) and rebuilds. The others poll, backing off from 20 ms to 100 ms; each poll reads the entry and the lock together (one `MGET`). A waiter stops when:
  - the entry appears: it is served as a hit;
  - the holder built with `cache: false` (a version read failed), which it signals by swapping its token for a 1 s `uncached` marker: the waiter builds directly, because no entry is coming;
  - the lock is gone with no entry (the holder's build threw, its `SET` failed, or its lock expired): the waiter tries to take the lock. One wins and builds as the new holder; the rest keep waiting for its entry;
  - 5 s pass: it throws `CacheWaitTimeoutError`, which the API answers with 503 `overloaded` and `Retry-After: 1`. It never builds.

  The holder releases the lock with an atomic compare-and-delete (a Lua script), so a holder whose lock expired cannot delete a newer holder's lock. The 30 s TTL only matters if a holder dies, and it must stay above the worst-case build, or the lock would expire under a live holder and a waiter would take over and build again. The API bounds every build: a pool acquire fails after 2 s and a statement after 5 s (`DB_POOL_ACQUIRE_TIMEOUT_MS`, `DB_STATEMENT_TIMEOUT_MS`), so the longest build, a product detail's three sequential queries, is at most 3 × (2 s + 5 s) = 21 s.
- **Load shedding.** Besides the two timeouts above, which also answer 503 `overloaded`, each request carries an `AbortSignal` that fires when the client disconnects before the response is written. Every database phase of the product read path checks it first and skips the work, so an overload ends when its traffic does instead of when its queued queries drain. Postgres JIT is off for the API's connections (`DB_JIT`): it added 83 to 131 ms to every cold listing build.
- **Why it fails closed.** The first version let a waiter build after 200 ms, and let a 2 s lock expire under a holder still queued for a connection. Under a cold start both turned coalescing into amplification: 3,019 page queries for 80 keys at 300 req/s on one replica, and a collapse that outlived its load (§11).
- **List pages past the end of the result set.** `page` is capped at `MAX_PAGE = 1000`, but a crawler can still request every page up to the cap. Rather than skip the cache for those (which would make every concurrent request for the same empty page wait out the coalescing poll above), an empty past-the-end page is cached like any other entry, just with a short, fixed TTL (`NOT_FOUND_TTL_SECONDS`, 5 s) instead of the promotion-boundary TTL. Staleness — a page that starts returning items after the entry is cached empty — is bounded by that TTL and, sooner in practice, by the version check: a write that adds a matching product bumps the category/all version and invalidates the entry immediately.
- **Degradation.** Every Redis failure on the read path is logged and falls through to Postgres. An entry built while a version read failed is served but not cached. Stock falls back to the Postgres column. No read endpoint fails because of Redis.

**Consequences.** One `INCR` invalidates any number of entries, and stale entries are overwritten in place instead of leaving orphans. The cost is one extra lookup per read, the rebuild time for readers that wait on a rebuild, a 503 for a reader whose rebuild takes longer than 5 s (instead of a query of its own), and a stale window of at most 5 minutes if Redis loses a bump. Because `ver:all` moves with every category change, the all-products listing is invalidated by any change anywhere, including every ingestion batch.

**Rejected.**
- *Delete by pattern.* `SCAN` plus `DEL` over 50k+ keys on every promotion write is slow and not atomic: readers repopulate keys while the scan runs.
- *Version-stamped keys* (`product:{id}:v{pv}:c{cv}`). This was the first design. A cold read cannot build the key, because the product's category, and so its category version, is unknown until the product has been read. It would need an extra id-to-category lookup on every read. Embedded versions give the same invalidation semantics.
- *Pub/sub invalidation.* It only pays off with in-process caches on each API instance. Pub/sub delivery is fire-and-forget, so a missed message has the same failure mode as a missed bump, plus a subscriber per instance. Worth revisiting if an L1 cache is added.

## 5. Scenario B walkthrough and measurement

**Flow of `POST /promotions` during load.**
1. Validate the body (Zod), check that the target exists, insert one row. The check constraints enforce the window and value rules.
2. After the commit, `INCR ver:category:{id}` and `ver:all`.
3. The next read of any cached list page for that category sees a version mismatch. One reader per page key rebuilds from Postgres with the new promotion applied; the others wait for its entry (up to 5 s, then 503, §4) instead of querying Postgres themselves.
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
- **Throughput dropped from about 9.4k to 5.3k req/s.** This is the cost of the design in §3, and the percentiles hide it. With 50 clients in a closed loop, mean latency is about 50 / 5,269 ≈ 9.5 ms after the flip against 50 / 9,354 ≈ 5.3 ms before, while p99 is 9.4 ms. So the lost time sits in fewer than 1% of requests. Those are the rebuilds. Each invalidated page is re-sorted against the full 62k-product category, with the lateral promotion lookup running once per product. At the time of this measurement, readers that waited longer than 200 ms ran the same full query directly, so for a moment several full-category sorts hit Postgres at once; §4 has since closed that path. The window also contains more than one invalidation: creating the mid-sale product bumps the category version again, and the check then reads five 100-item pages that were never cached. We did not instrument individual rebuild times; this reading comes from the numbers above and the code path.
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
- **Stock counter ordering.** `PATCH /products/:id/stock` (and an ingestion batch) writes Postgres and then `SET`s the counter to the committed value. Two concurrent writes to one product can reach Redis out of commit order, leaving the older value in the counter until the next write or the 300 s TTL. The failed-`SET`-deletes-the-key and backfill-uses-`SET NX` rules narrow this but don't close it: one interleaving still leaves a stale counter — a backfill reads the committed value `v1`, then a second write commits `v2` and its `SET` fails and deletes the key, then the backfill's `SET NX v1` (still in flight) lands on the now-empty key. The counter reads `v1` with nothing left to correct it until the next write or the 300 s TTL. Fix: guard the `SET` with a per-product version (a Lua compare-and-set on `updated_at` or a write counter), or apply deltas with `INCRBY`.
- **A cold listing page still scans its whole category.** Coalescing and shedding stop a cold start from collapsing (§11), but each cold page still evaluates the lateral promotion probe for all 62k products of its category and sorts them: about 110 ms idle with JIT off, several hundred ms under concurrent builds. A cold start at 4,900 req/s over 4 replicas still shed about 5% of requests as 503s in its first 15 to 20 s. The rigorous fix is the one in §3: a maintained effective price with an index on `(category_id, effective_price)`, so a page becomes an index range scan.
- **Promotion invalidation on a hot category still costs one cold build per key.** A promotion bumps its category's version and every cached page of that category goes stale at once. Readers of each page now wait for one rebuild instead of each running their own, but every page key still pays one full-category build, and waiters see 503s if those builds take more than 5 s under load. Serve-stale-while-revalidate (§9) would keep serving the previous entry instead.
- **Holders share one FIFO pool queue with everything else.** A holder's queries wait behind other requests' queries for a connection; the 2 s acquire timeout bounds that wait, and a timed-out holder hands its key to one waiter, but a busy pool can still fail a build that would otherwise have succeeded. Stock backfills (`loadStocks`) are not coalesced either, so a list page whose stock counters are cold runs its own stock query on each hit until they are filled.
- **The SAM template has no `VpcConfig`**, and the API is not part of the template.
- **The API's S3 client uses static credentials.** The API has one S3 client, the presigner. `apps/api/src/deps.ts` builds it from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, with a `test` fallback and no session token, instead of the SDK's default provider chain. A deployed API therefore needs long-lived static keys and cannot use an IAM role. Fix: use the default chain outside local development, as `apps/ingest/src/aws.ts` already does.

## 11. Horizontal scaling: 1 vs 4 replicas behind nginx

**Setup.** Measured on 2026-09-25 with the `lb` Compose profile.
- **Stack.** N `api-lb` replicas behind nginx 1.27.5 on `:8080` (round robin, upstream keepalive). Each replica is limited to 1 CPU (`API_CPUS`) and holds a Postgres pool of 10.
- **Catalog.** The 500k-row ingest from §5: 8 categories of about 62k products, so every requested page exists.
- **Host.** An Apple M4 Pro Mac (12 cores, 24 GiB). The replicas, nginx, Postgres and Redis shared one Docker Desktop VM with 6 CPUs. The load client ran on the host, outside the VM.
- **Cache.** Every entry except the version counters was deleted before each run. Each run rebuilt its entries during an unrecorded warm-up, and none expired inside the recorded window.

The environment, exact commands, per-run tables, and the `docker stats`, client-CPU and cgroup captures are in [`docs/load-tests/2026-09-25-rerun/`](docs/load-tests/2026-09-25-rerun/summary.md).

**Method.**
- **Closed model.** `load browse --seed 42 --concurrency 100 --warmup 15s --duration 60s`: 100 workers, 70% listings (pages 1 to 5, 20 items each) and 30% product details. Three runs per configuration, interleaved 1, 4, 1, 4, 1, 4.
- **Open model.** For latency at equal load without coordinated omission: `--rate 4900/s` with the same seed, once per configuration, on a pre-warmed cache. 4,900 req/s is 70% of the median 1-replica closed throughput. The 1-replica run was repeated once (see below).
- **Captured.** `docker stats` for every container about every 5 s, the client's CPU time, and each replica's cgroup `cpu.stat`.

| Replicas (closed, 100 workers) | req/s, mean ± sd (range) | p50 | p90 | p99 | p99.9 | 5xx / transport errors |
|---|---|---|---|---|---|---|
| 1 | 7,016 ± 95 (6,934 to 7,120) | 12.97 ms | 19.61 ms | 37.04 ms | 45.23 ms | 0 / 0 |
| 4 | 14,134 ± 1,884 (11,963 to 15,321) | 2.64 ms | 25.35 ms | 37.20 ms | 46.37 ms | 0 / 0 |

Percentiles are the means of the three runs; ranges and per-run values are in the summary.

- **Throughput doubled: 2.01× on the means.** The three interleaved pairs gave 1.68×, 2.16× and 2.21×. The low pair is explained under round robin below.
- **With 1 replica, the bottleneck is the replica's CPU limit.**
  - The replica was at 100% of its 1-CPU quota in every sample of every run.
  - Postgres stayed under 1% (the cache was warm), Redis at about 16% of a CPU, nginx at about 34%.
  - The client used about a third of a host core.
- **With 4 replicas, no single container saturated.**
  - In runs 2 and 3 the replicas averaged 77 to 82%, nginx 0.8 CPU, Redis 0.35 CPU, Postgres under 1%. The VM was using about 4.3 of its 6 CPUs, and the client about 0.6 of a core.
  - A diagnostic run with 200 workers reached 18,957 req/s (+24%), with replicas at about 84% and the VM at about 4.9 CPUs. So 100 workers did not saturate four replicas.
  - The 2× is a ratio at equal concurrency, not a ratio of capacities. 1 replica with 200 workers was not measured.
- **Why the gain is 2× and not 4×.**
  - CPU per request, measured as replica CPU ÷ req/s, rises as each replica's load falls: 143 µs at 7,000 req/s per replica, about 208 µs at 3,800, and 359 µs at 1,225.
  - Four replicas at about 79% (3.2 CPUs of work) therefore delivered 2.2× the throughput.
  - We did not measure why a busier Node process spends less CPU per request.
- **Round robin runs at the pace of the slowest replica.**
  - In the first 4-replica run, one replica needed about 1.45× the CPU per request of the other three. It was pinned at 100% while they ran at about 69%.
  - Round robin still sent it exactly a quarter of the requests, so the whole run slowed to its pace: 11,963 req/s, with p90 at 31.6 ms.
  - It did not recur after the replicas were recreated. The cause was not found.
  - nginx's `least_conn` would route around such a replica; we did not test it.
- **nginx spread load evenly.** Every 4-replica run split requests into exact quarters, give or take 3 requests.
- **The median improved; p99 did not.** The closed-model median fell from 13.0 ms to 2.6 ms, but p99 stayed at 37 ms, and p90 rose from 19.6 ms to 25.4 ms.
- **At equal load, the difference is in the tail.** Open model at 4,900 req/s:

  | Replicas | p50 | p90 | p99 | p99.9 | Errors | Replica CPU |
  |---|---|---|---|---|---|---|
  | 1, run 1 | 2.85 ms | 76.03 ms | 4,759.55 ms | 9,076.74 ms | 125 timeouts | 99% |
  | 1, run 2 | 1.15 ms | 15.26 ms | 51.97 ms | 558.59 ms | 0 | 100% |
  | 4 | 0.62 ms | 0.79 ms | 2.46 ms | 11.85 ms | 0 | 44% each |

  - 4,900 req/s is 70% of the 1-replica closed throughput, but it saturates one replica under an open model, where each request costs about 204 µs of CPU.
  - **1 replica.** It ran at its CPU limit in both runs, throttled by the cgroup in about 60% of scheduling periods. Run 1 stalled several times. When the first stall began, a container from an unrelated project was briefly using 0.4 CPU on the same VM. Run 2 held, with p99 at 52 ms.
  - **4 replicas.** At 44% each, they held p99 at 2.5 ms and p99.9 at 12 ms.
- **What this shows.** The stack scales out, the balancer spreads load evenly, and warm-read throughput at 100 workers doubles. One replica cannot serve 4,900 req/s with a stable tail; four can.
- **What this does not show.** What separate hosts would do:
  - Everything except the client shared one 6-CPU VM. During some runs that VM also held short-lived test containers from another project (listed in the summary).
  - There is no network between the tiers.
  - Postgres was idle throughout, so these runs measure the cached read path (nginx, Node, Redis). They do not measure price computation or the rebuild cost in §5.
- **An open-model load on a cold cache collapsed; it now recovers.** Two attempts at 4,900 req/s against 4 replicas, starting from an empty cache (one of them with a 30 s ramp), both collapsed:
  - 7 and 1,645 successful responses in 60 s. Postgres ran at 5 to 6 CPUs on 40 concurrent listing builds (four pools of 10), with 10,000 requests in flight and nginx out of worker connections. More than 46k requests timed out.
  - Postgres was still busy 5 minutes after the client stopped, until the replicas were restarted.
  - **Root cause, confirmed by an instrumented 1-replica reproduction.** One replica collapses from about 125 to 150 req/s. At 300 req/s, 3,019 page queries ran for 80 keys and 96.5% of requests failed, because coalescing leaked on two paths: waiters gave up after 200 ms and ran their own query, and the 2 s lock expired under holders still queued for a pool connection, so waiters saw no lock and built too. The holders queued behind those bypass builds in the same pool queue, which kept builds slow. Nothing shed the backlog: 95.5% of queries ran for requests whose client had already gone, and Postgres stayed busy for about 81 s after a 30 s run. The JIT added 83 to 131 ms to each cold page.
  - **Fix (§4).** Coalescing fails closed: waiters wait up to 5 s for the entry, then get a 503 instead of building, and a failed or expired holder hands its key to one waiter. The lock is 30 s, owner-checked, and above the bounded build time. The pool fails an acquire after 2 s, Postgres cancels a statement after 5 s, no query starts for a client that has gone, and JIT is off.
  - **After, same commands, cold cache.**

    | Run | Succeeded | 503 overloaded | Transport errors | Dropped by the client | p50 | p99 | Page queries | Postgres idle after the client stopped |
    |---|---|---|---|---|---|---|---|---|
    | 1 replica, 300 req/s, before | 314 of 9,000 | 0 | 8,686 | 0 | – | – | 3,019 | after about 81 s |
    | 1 replica, 300 req/s | 9,000 of 9,000 | 0 | 0 | 0 | 1.35 ms | 1,867 ms | 87 | immediately |
    | 1 replica, 1,225 req/s | 36,751 of 36,751 | 0 | 0 | 0 | 0.60 ms | 2,257 ms | 86 | immediately |
    | 4 replicas, 4,900 req/s, before | 7 of 294,001 | 0 | 46,541 | 247,453 | – | – | – | not within 5 min |
    | 4 replicas, 4,900 req/s | 256,131 of 294,001 (87%) | 14,652 | 46 | 23,172 | 1.05 ms | 5,378 ms | 138 | immediately |

  - With 1 replica the whole tail is the first 2 s of cache fill; from the 10 s mark p99 was under 8 ms. With 4 replicas the 503s (5.4% of the requests sent) and the drops fell in the first 15 to 20 s, while the replicas were pinned at their 1-CPU limit with 10,000 requests in flight; from then on the run held 4,900 req/s with p99 under 21 ms after the 25 s mark. The 503s were 8,923 cache waits that reached 5 s and 5,731 pool acquire timeouts.
  - A first version of the fix still let every waiter build directly when a holder failed. At 4 replicas that failed 71% of requests: each holder that timed out on the pool released a herd whose uncached queries starved the next holder. The takeover rule closed it.
  - The closed-model runs survived the original cold start because they never had more than 100 requests in flight. Details and raw files: ["Cold-start collapse" in the rerun summary](docs/load-tests/2026-09-25-rerun/summary.md#cold-start-collapse-open-model-not-in-the-comparison).
- **Also checked end to end.**
  - Through nginx with 4 replicas, `write-mix --rate 100/s --duration 30s` completed 3,000 requests, including 16 promotion creates, 15 cancels and 448 stock writes, with 0 5xx and 0 transport errors. That result supports the upstream keepalive timeout fix.
  - Against a single API, `browse`, `write-mix` and `flash-sale` ran with no transport errors, and the flash-sale mid-sale check passed.
  - A `write-mix` run interrupted with Ctrl-C printed a partial report, exited 130, and left 0 uncancelled promotions.
- **An earlier run was invalidated.** The first run ([`docs/load-tests/2026-09-25/`](docs/load-tests/2026-09-25/README.md)) used the 200-product seed. 28% of its requests hit pages past the end, and each of those waited out a 200 ms lock-wait bug (since fixed, §4). Its 1.42× ratio and its tail analysis measured that bug, not scaling.
- **Limits of the local setup.**
  - About 9 replicas fit before the per-replica pools exhaust Postgres's default `max_connections` of 100.
  - nginx resolves the replica list at startup, so it has to be restarted after rescaling.
  - Both are in the README. On AWS, an ALB and RDS Proxy remove them.
