# Form 5: AI Interaction Summary

> Placeholders marked **[CANDIDATE]** are for the candidate to write in their own words. Everything else is drawn from the repository history, the design spec, the implementation plan and the per-task review ledger.

## 1. Tool Manifest

| Model / Tool | Primary purpose of use | Effectiveness (1-5) and brief why |
|---|---|---|
| Claude Code (desktop app) running **Claude Fable 5.1** | Design phase: reading the case study and Form 5, question-by-question architecture brainstorming, the design spec (`docs/superpowers/specs/`), and the 17-task implementation plan (`docs/superpowers/plans/`) | **[CANDIDATE]** |
| Claude Code running **Claude Opus 5.5** | Implementation phase: a controller session dispatching one implementer subagent and one reviewer subagent per plan task; demo runs; drafting README, ADR and this appendix | **[CANDIDATE]** |
| Superpowers plugin skills (brainstorming, writing-plans, subagent-driven-development, test-driven-development, requesting-code-review) | Forced a one-question-at-a-time design phase, a written plan before any code, test-first implementation, and an independent review of every task | **[CANDIDATE]** |
| Context7 MCP (library documentation) | Checked current Drizzle ORM syntax (check constraints, partial indexes, upserts) and the LocalStack S3-to-SQS notification setup before writing code | **[CANDIDATE]** |

## 2. AI Tool Usage Approach

| Phase / specific task | Prompting strategy and context provided | Human refinement (how I edited or iterated) |
|---|---|---|
| Requirements | Asked the AI to read the case study PDF and the Form 5 template first and summarize them before designing anything | Confirmed the summary, then answered design questions one at a time |
| Architecture | For each decision the AI offered 2-3 options with trade-offs: serverless target, local-first with LocalStack, Postgres + Drizzle, read-time price + Redis, ingestion mechanism, conflict rule, pricing pipeline | Made every choice myself. Overrode the AI's recommendation on the conflict rule (chose most-recent-wins to keep scope down, rigorous option recorded in the ADR). Pushed back twice on the cache design (the two prompts below) |
| Spec and plan | Had the AI write a design spec, review it against the case study, then write a task-by-task plan with exact code and tests written before implementation | Reviewed and approved the spec section by section. Rejected the first cache key scheme before the plan was written (see §3). The approved plan is what was executed |
| Implementation | Subagent-driven development: for each of the 17 tasks a fresh implementer subagent received a task brief plus shared rules (see the failing test first, then implement, then commit). A separate reviewer subagent then checked the diff against the spec and plan and graded findings Critical / Important / Minor | Every fix to plan-mandated behavior needed my approval before a fix round was dispatched. I decided which minor findings to fold in and which to defer to a ledger (now ADR §10). Plan defects that would carry into later tasks were patched in the plan itself (3 plan patches). 8 of the 16 code tasks needed at least one fix round, 9 rounds in total |
| Demos and measurements | Scripted demos for both scenarios; the AI ran them and reported numbers | The demo's own review found a flawed measurement (a cold-start confound, see §3); the numbers in the ADR come from the corrected rerun |
| Documentation | The AI drafted the README, ADR and this appendix from the code, spec, review ledger and demo output, with the instruction that the code wins where they disagree | **[CANDIDATE]**: my edits to wording, ratings and reflection |

**The two most critical prompts:**

1. "compute at read time with redis (but make sure that we can actually cache it, products have stock quantity can we safely cache that?)"
2. "what happens when we release a promo, unit prices change on the backend but cached responses will return old prices. Shall we add an invalidation logic or something else?"

The first forced stock out of the catalog cache. The second produced the version-counter invalidation that the whole Scenario B answer rests on.

## 3. Judgement, Challenges and Verification

**Biggest architectural/logical mistake** **[CANDIDATE: confirm or replace]**. The AI's first chunk-alignment rule for Scenario A: "when `byteStart > 0`, discard everything up to the first newline." It looks right and passes a casual test. But when a chunk boundary falls exactly after a newline, the line starting at `byteStart` belongs to this chunk, and the rule throws it away. The previous chunk stops at its own end, so no chunk processes that line. That is silent data loss that depends on where the byte offsets happen to fall. It was corrected to "a chunk owns the lines whose first byte is inside its range; read one byte early to see the preceding character", and a property test now checks exactly-once delivery across many chunk sizes, including that boundary case.

### Design phase

| Challenge encountered | Judgement / verification | Resolution |
|---|---|---|
| The first cache design cached the whole product row, including stock, under a long TTL | Stock changes on every sale; a cached value would be wrong for the whole TTL | Split the cache: catalog and price entry with a long TTL; stock read live from a Redis counter with a Postgres fallback |
| The spec keyed the product cache as `product:{id}:v{pv}:c{cv}` | The API cannot know a product's category, and so its category version, before reading the product. The key could never be built on a cold read | Store the versions inside the cached value and validate them on read. Same invalidation, two round trips when warm |
| Chunk alignment: "discard up to the first newline when `byteStart > 0`" | Walked through a boundary that falls exactly after a newline: a full, owned line is discarded and no chunk processes it | Ownership by first byte, one byte read early, and a property test over many chunk sizes (above) |
| Promotions with a future start change prices with no write to trigger invalidation | A scheduler adds an operational component and a race | Cap each entry's TTL at the next promotion boundary, computed from the promotions table |
| Multi-row upsert with duplicate SKUs in one batch | Postgres raises "ON CONFLICT DO UPDATE command cannot affect row a second time" | Dedupe by SKU within a batch, last occurrence wins; covered by a test |

### Implementation phase (plan defects caught by tests and per-task review)

All of these were in the approved plan's own code, not implementer slips. Each fix was approved by me before it was made.

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
