# Form 5: AI Interaction Summary

> Sources: the design conversation with the AI (my messages are quoted verbatim below), the design spec and implementation plan under `docs/superpowers/`, and the commit history, which records every task and fix round. The per-task review notes are not part of the submission; the findings that were deferred rather than fixed are listed in ADR §10. **[CANDIDATE]** marks what I still have to write in my own words; **[CANDIDATE: confirm]** marks claims the record does not settle.

## 1. Tool Manifest

| Model / Tool | Primary purpose of use | Effectiveness (1-5) and brief why |
|---|---|---|
| Claude Code (desktop app) running **Claude Fable 5.1** | Design phase: reading the case study and Form 5, question-by-question architecture brainstorming, the design spec (`docs/superpowers/specs/`), and the 17-task implementation plan (`docs/superpowers/plans/`) | **[CANDIDATE]** |
| Claude Code running **Claude Opus 5.5** | Implementation phase: a controller session dispatching one implementer subagent and one reviewer subagent per plan task; demo runs; drafting README, ADR and this appendix | **[CANDIDATE]** |
| Superpowers plugin skills (brainstorming, writing-plans, subagent-driven-development, test-driven-development, requesting-code-review) | Forced a one-question-at-a-time design phase, a written plan before any code, test-first implementation, and an independent review of every task | **[CANDIDATE]** |
| Context7 MCP (library documentation) **[CANDIDATE: confirm it was used, or delete this row]** | Checking current Drizzle ORM and LocalStack documentation | **[CANDIDATE]** |

## 2. AI Tool Usage Approach

| Phase / specific task | Prompting strategy and context provided | Human refinement (how I edited or iterated) |
|---|---|---|
| Requirements | The AI worked from the case study PDF and the Form 5 template **[CANDIDATE: confirm how they were provided and whether the AI summarized them first]** | Answered the AI's design questions one at a time |
| Architecture | For each decision the AI offered numbered options with trade-offs | I chose each one. Stack: "aws lambda with sqs and s3", "local-first with localstack", "postgresql with drizzle". Price and cache: "compute at read time with redis", with the stock question attached (prompt 1 below). Invalidation: prompt 2 below. Ingestion: "byte-range fan-out, yes presigned upload is fine". Conflict rule: "go with option 3 since its simpler and we are doing a case study" (most recent wins; the rigorous options are recorded in ADR §7) **[CANDIDATE: confirm whether this differed from the AI's recommendation]**. Pricing: "go with option 1" (deterministic pipeline) |
| Spec | The AI wrote the design spec section by section | I approved each section in turn: data model, API, ingestion flow, error handling and testing |
| Plan | The AI wrote a 17-task plan with exact code and tests to write before implementation, and self-reviewed it | While planning, the AI itself caught two flaws in its own design: the cache key could not be built on a cold read, and the chunk-boundary rule dropped lines (see §3). The approved plan is what was executed |
| Implementation | Subagent-driven development in a git worktree (my choice of workflow). A controller session gave each of the 17 tasks to a fresh implementer subagent (task brief plus shared rules: see the failing test first, then implement, then commit). A separate reviewer subagent checked each diff against the spec and plan and graded findings Critical / Important / Minor | The controller asked me before fixing any plan-mandated behavior (Tasks 5, 8, 10, 12, 13, 14) and I approved each fix. In Tasks 8, 10, 12, 13 and 14 I also chose to fold in the cheap minor findings. I decided to publish Postgres on host port 5433. On its own, the controller patched the plan twice (the drizzle error-code fix, and carrying the approved ioredis pipeline fix into later tasks), ran the Task 15 and 16 fix rounds (not plan-mandated), and deferred all other minor findings (the material ones are in ADR §10). 8 of the 16 code tasks needed at least one fix round, 9 rounds in total (see the `fix(...)` commits) |
| Demos and measurements | Scripted demos for both scenarios; the AI ran them and reported numbers | The review of the demo task found a flawed measurement (a cold-start confound, see §3). The numbers in the ADR come from the corrected rerun |
| Documentation | The AI drafted the README, ADR and this appendix from the code, spec, plan, its implementation notes and the demo output, with the instruction that the code wins where they disagree | **[CANDIDATE]**: my edits to wording, ratings and reflection |

**The two most critical prompts:**

1. "compute at read time with redis (but make sure that we can actually cache it, products have stock quantity can we safely cache that?)"
2. "what happens when we release a promo, unit prices changes on the backend but we cached responses will return old prices. Shall we add an invalidation logic or something else?"

The first kept stock out of the catalog cache. The second led to the version-counter invalidation that the whole Scenario B answer rests on.

## 3. Judgement, Challenges and Verification

**Biggest architectural/logical mistake** **[CANDIDATE: choose, and say who caught it]**. The record gives two candidates:

- *Caught by me:* the cache design had to answer two questions I raised before it was written: whether stock could be cached at all (prompt 1), and what happens to cached prices when a promotion is released (prompt 2). Stock is therefore never in a cached entry, and one counter increment invalidates every affected entry the moment a promotion is written.
- *Caught by the AI during planning:* the first chunk-alignment rule for Scenario A, "when `byteStart > 0`, discard everything up to the first newline". When a chunk boundary falls exactly after a newline, the line starting at `byteStart` belongs to this chunk and the rule throws it away. The previous chunk stops at its own end, so no chunk processes the line: silent data loss that depends on where the byte offsets happen to fall. The AI found this while planning and replaced it with "a chunk owns the lines whose first byte is inside its range; read one byte early to see the preceding character". A property test checks exactly-once delivery across many chunk sizes, including that boundary.

### Design phase

| Challenge encountered | Judgement / verification | Resolution |
|---|---|---|
| Whether a product with a stock quantity can be cached at all | Raised by me (prompt 1). Stock changes on every sale, so a cached value would be wrong for the whole TTL | Split the cache: catalog and price entry with a long TTL; stock read live from a Redis counter with a Postgres fallback |
| Cached responses keep returning old prices after a promotion is released | Raised by me (prompt 2) | Version counters bumped after each promotion write and checked on every read (ADR §4) |
| The spec keyed the product cache as `product:{id}:v{pv}:c{cv}` | Caught by the AI during planning: the API cannot know a product's category, and so its category version, before reading the product, so the key could never be built on a cold read | Store the versions inside the cached value and validate them on read. Same invalidation, two round trips when warm. The spec was amended in the plan commit (`e4e6d28`) |
| Chunk alignment: "discard up to the first newline when `byteStart > 0`" | Caught by the AI during planning, by walking through a boundary that falls exactly after a newline | Ownership by first byte, one byte read early, and a property test over many chunk sizes (above) |
| Promotions with a future start change prices with no write to trigger invalidation | A scheduler adds an operational component and a race **[CANDIDATE: confirm who raised this]** | Cap each entry's TTL at the next promotion boundary, computed from the promotions table |
| Multi-row upsert with duplicate SKUs in one batch | Postgres raises "ON CONFLICT DO UPDATE command cannot affect row a second time" | Dedupe by SKU within a batch, last occurrence wins; covered by a test |

### Implementation phase (plan defects caught by tests and per-task review)

All of these were in the approved plan's own code, not implementer slips. They were caught by the implementer's failing tests or by the reviewer subagent. Who decided each fix:

- **I approved them.** The controller asked me before changing plan-mandated behavior: the Redis rows (Task 5), the read-path rows (Task 8), cancel/assign (Task 10), the splitter (Task 12), completion, rejections and SKU moves (Task 13), and the runner and SAM rows (Task 14).
- **The controller decided alone:**
  - the drizzle row, applied as a plan patch (`7a1c0a2`);
  - the plan patch carrying the approved ioredis fix into Tasks 8 and 13 (`0b09769`);
  - the last two rows (Tasks 15 and 16), which were not plan-mandated.

| Challenge encountered | Judgement / verification | Resolution |
|---|---|---|
| drizzle-orm 0.44 wraps driver errors in `DrizzleQueryError`; the Postgres SQLSTATE is on `cause`, but the plan read `err.code` | The Task 2 constraint tests failed against the installed version, and a probe confirmed the wrapping. A duplicate SKU would have returned 500 instead of 409, and a database outage 500 instead of 503 | `pgErrorCode()` reads `code ?? cause.code`; the plan was patched before the error handler and product service were built |
| ioredis `pipeline.exec()` resolves with per-command `[err, result]` pairs instead of rejecting | Review showed the version-bump retry-and-log loop could never trigger: a failed bump was silent | `throwOnPipelineError()` in core, used by every pipeline; the plan was patched for later tasks |
| No Redis command timeout | A stalled (not dead) Redis would hang reads instead of degrading to Postgres | `commandTimeout: 300` and no offline queue |
| Reads returned 500 when Redis was unreachable (Critical) | The cache builder read version counters outside the read-through's error guard, and the bypass path calls the builder again. A test with Redis down reproduced it | Version reads never throw; an entry built from a failed version read is served but not cached |
| Category-version read after the price fetch | A promotion committing between the fetch and the version read would stamp old prices with the new version: stale for the whole TTL | Read the version before the fetch (comment in `cached-reads.ts` explains why) |
| Product-scoped promotions did not invalidate category listings | The plan bumped only `ver:product`, but the price change also moves the product within category and all-products lists | Bump product, its category and `ver:all`; the test asserts the category list reprices |
| Cancel and assign were read-then-write | Concurrent cancels could both "win"; concurrent moves X→Y and X→Z could leave Y's caches never invalidated | Cancel is a conditional `UPDATE ... where cancelled_at is null`; assign locks the row (`FOR UPDATE`) in a transaction |
| The splitter resurrected finished jobs on S3 redelivery | It set the job to `processing` unconditionally, so a redelivered event would reopen a completed or failed job | No-op for finished jobs; every status write is conditional on a splittable status |
| Non-atomic chunk completion, then a status-reset hole found on re-review | Two deliveries of one chunk could both increment the job counters. After the first fix, the re-review found the worker's claim step still reset a finished chunk to `processing`, which undid the guard for DLQ redrives and lost races | Completion is a conditional update plus the counter increments in one transaction; the claim is conditional on the chunk not being `completed` or `failed` |
| Duplicate rejection rows on retry | A retried chunk re-rejects the same lines | Unique index on `(job_id, chunk_index, line_number)` (migration 0001) with `ON CONFLICT DO NOTHING` |
| A SKU moved to another category by a vendor file left caches stale | Only the new category was bumped; the old category's pages and the product's own entry kept validating | Capture the pre-upsert categories inside the transaction and bump both |
| The runner's timeout killed only the `tsx` wrapper | The `tsx` CLI spawns a second Node process, so SIGKILL left the handler running. Reproduced outside the test suite | Run `node --import tsx` directly; a test asserts the handler's own PID is dead after a timeout |
| SAM template missing `ScalingConfig.MaximumConcurrency` | With only reserved concurrency, the SQS poller over-scales, throttles count as receives, and healthy chunks can reach the DLQ | `MaximumConcurrency` set to the reserved concurrency |
| The end-to-end DLQ test stranded an in-flight message | Its induced failure left the message invisible on the shared chunk queue for 360 s; the implementer had been purging it by hand, and a rerun within 6 minutes could process a stale message | The test deletes its own message; queues are verified empty after back-to-back runs |
| The flash-sale demo compared a cold "before" with a warm "after" | The first report concluded that latency improved after the flip (p95 13.6 → 7.1 ms). Review saw the confound and that the mid-sale product check was manual | Unrecorded warm-up and a scripted mid-sale check. The rerun gave p95 6.7 → 6.9 ms and exposed the real cost, a throughput drop from 9.4k to 5.3k req/s, now explained in ADR §5 |

## 4. Overall Reflection

**Estimated ratio (AI-generated vs human-crafted or heavily edited):** **[CANDIDATE]**

**Key takeaway (blind spots in the tools, or how AI changed the original approach):** **[CANDIDATE]**
