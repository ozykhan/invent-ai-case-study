import { GetObjectCommand } from '@aws-sdk/client-s3';
import { and, eq, inArray, sql } from 'drizzle-orm';
import type { Readable } from 'node:stream';
import {
  bumpVersions, categories, categoryPricingFromRow, DEFAULT_CATEGORY_PRICING, fromCents, ingestionChunks, ingestionJobs,
  ingestionRejections, keys, ownedLines, parseCsvLine, priceVendorRow, products, rangeFor, rowFromFields, slugify,
  STOCK_TTL_SECONDS, throwOnPipelineError, VENDOR_COLUMNS, type CategoryPricing, type Db, type PricedRow, type Redis,
} from '@modaco/core';
import type { Logger } from 'pino';
import type { IngestDeps } from './deps';
import { markChunkCompleted } from './job-state';
import type { ChunkMessage } from './splitter';

type Deps = Pick<IngestDeps, 'db' | 's3' | 'redis' | 'config' | 'logger'>;

interface PendingLine { lineNumber: number; raw: string }
interface Rejection { lineNumber: number; rawLine: string; reason: string }

/** Accumulates parsed lines and writes them in bounded batches. Memory never exceeds one batch. */
class BatchWriter {
  private pending: PendingLine[] = [];
  rowsProcessed = 0;
  rowsRejected = 0;

  constructor(
    private readonly db: Db, private readonly redis: Redis, private readonly logger: Logger,
    private readonly jobId: string, private readonly chunkIndex: number, private readonly batchSize: number,
  ) {}

  async add(line: PendingLine): Promise<void> {
    this.pending.push(line);
    if (this.pending.length >= this.batchSize) await this.flush();
  }

  async flush(): Promise<void> {
    if (this.pending.length === 0) return;
    const batch = this.pending;
    this.pending = [];

    const rejections: Rejection[] = [];
    const parsed: Array<{ lineNumber: number; raw: string; row: Record<string, string> }> = [];
    for (const { lineNumber, raw } of batch) {
      const fields = parseCsvLine(raw);
      const row = rowFromFields(fields);
      if (!row) rejections.push({ lineNumber, rawLine: raw, reason: `malformed: expected ${VENDOR_COLUMNS.length} fields, got ${fields.length}` });
      else parsed.push({ lineNumber, raw, row });
    }

    // Validate every row before creating any category: a row that fails validation (bad name, a
    // non-positive vendor_price, an over-length category name, ...) must never have the side effect of
    // inserting a new category, even though its category field looks well-formed on its own. Priced with
    // the default pricing here since we don't know the real category pricing yet and only care about ok/not-ok.
    const validated: Array<{ lineNumber: number; raw: string; row: Record<string, string> }> = [];
    for (const { lineNumber, raw, row } of parsed) {
      const probe = priceVendorRow(row, () => DEFAULT_CATEGORY_PRICING);
      if (!probe.ok) { rejections.push({ lineNumber, rawLine: raw, reason: probe.reason }); continue; }
      validated.push({ lineNumber, raw, row });
    }

    const pricingByName = await this.ensureCategories([...new Set(validated.map((v) => v.row.category!.trim()).filter(Boolean))]);
    const priced: PricedRow[] = [];
    for (const { lineNumber, raw, row } of validated) {
      const outcome = priceVendorRow(row, (name) => pricingByName.get(name)?.pricing ?? DEFAULT_CATEGORY_PRICING);
      if (!outcome.ok) { rejections.push({ lineNumber, rawLine: raw, reason: outcome.reason }); continue; }
      // Two distinct names that slugify identically (e.g. "Shoes" and "shoes") cannot both exist; the loser is rejected.
      if (!pricingByName.has(outcome.row.category)) { rejections.push({ lineNumber, rawLine: raw, reason: `category '${outcome.row.category}' collides with an existing category slug` }); continue; }
      priced.push(outcome.row);
    }

    // Postgres refuses to update the same row twice in one INSERT ... ON CONFLICT: dedupe within this
    // batch keeps only the row's last occurrence here. That guarantee is per batch only — across batches,
    // or across a retry of the same chunk, whichever upsert commits last wins.
    const bySku = new Map<string, PricedRow>();
    for (const r of priced) bySku.set(r.sku, r);
    const unique = [...bySku.values()];

    const touchedCategories = new Set<number>();
    const written: Array<{ id: number; stock: number }> = [];
    await this.db.transaction(async (tx) => {
      if (unique.length > 0) {
        const skus = unique.map((r) => r.sku);
        // A vendor file can move an existing SKU into a different category. The OLD category's cached
        // list pages, and the product's own cache entry (validated against the OLD ver:category), must be
        // invalidated too — so capture the pre-upsert category before it gets overwritten below.
        const existing = await tx.select({ categoryId: products.categoryId }).from(products).where(inArray(products.sku, skus));
        for (const e of existing) touchedCategories.add(e.categoryId);

        const rows = await tx.insert(products).values(unique.map((r) => {
          const categoryId = pricingByName.get(r.category)!.id;
          touchedCategories.add(categoryId);
          return { sku: r.sku, name: r.name, categoryId, basePrice: fromCents(r.basePriceCents), stock: r.stock };
        })).onConflictDoUpdate({
          target: products.sku,
          set: {
            name: sql.raw(`excluded.${products.name.name}`),
            categoryId: sql.raw(`excluded.${products.categoryId.name}`),
            basePrice: sql.raw(`excluded.${products.basePrice.name}`),
            stock: sql.raw(`excluded.${products.stock.name}`),
            updatedAt: sql`now()`,
          },
        }).returning({ id: products.id, stock: products.stock });
        written.push(...rows);
      }
      if (rejections.length > 0) {
        // A retried chunk re-parses and re-rejects the same lines; onConflictDoNothing on the (job,
        // chunk, line) unique index keeps a redelivery from duplicating rows already committed by an
        // earlier, partially-completed attempt.
        await tx.insert(ingestionRejections)
          .values(rejections.map((r) => ({ jobId: this.jobId, chunkIndex: this.chunkIndex, ...r })))
          .onConflictDoNothing({ target: [ingestionRejections.jobId, ingestionRejections.chunkIndex, ingestionRejections.lineNumber] });
      }
    });

    this.rowsProcessed += priced.length;
    this.rowsRejected += rejections.length;
    await this.publish(written, [...touchedCategories]);
  }

  private async ensureCategories(names: string[]): Promise<Map<string, { id: number; pricing: CategoryPricing }>> {
    const out = new Map<string, { id: number; pricing: CategoryPricing }>();
    if (names.length === 0) return out;
    await this.db.insert(categories).values(names.map((name) => ({ name, slug: slugify(name) }))).onConflictDoNothing();
    const rows = await this.db.select().from(categories).where(inArray(categories.name, names));
    for (const c of rows) out.set(c.name, { id: c.id, pricing: categoryPricingFromRow(c) });
    return out;
  }

  /** After commit: stock counters and version bumps so the storefront sees new prices at once. Never throws. */
  private async publish(written: Array<{ id: number; stock: number }>, categoryIds: number[]): Promise<void> {
    const log = (msg: string, err: unknown) => this.logger.error({ err, jobId: this.jobId, chunkIndex: this.chunkIndex }, msg);
    if (written.length > 0) {
      const pipe = this.redis.pipeline();
      for (const w of written) pipe.set(keys.stock(w.id), String(w.stock), 'EX', STOCK_TTL_SECONDS);
      await pipe.exec().then(throwOnPipelineError).catch((err) => log('stock counter publish failed', err));
    }
    if (categoryIds.length > 0) {
      await bumpVersions(this.redis, [...categoryIds.map(keys.categoryVersion), keys.allVersion()], log);
    }
  }
}

/**
 * Processes exactly one byte-range chunk: streams it from S3, prices every owned line, upserts in batches,
 * then records completion. Idempotent: a redelivered message for a chunk already marked completed exits
 * immediately (the fast path below), and a genuine race between two concurrent deliveries of the same
 * not-yet-completed chunk is resolved by markChunkCompleted's atomic guard, so job counters are only ever
 * incremented once per chunk.
 */
export async function processChunk(deps: Deps, msg: ChunkMessage): Promise<{ skipped: boolean; rowsProcessed: number; rowsRejected: number }> {
  const [chunk] = await deps.db.select().from(ingestionChunks)
    .where(and(eq(ingestionChunks.jobId, msg.jobId), eq(ingestionChunks.chunkIndex, msg.chunkIndex)));
  if (!chunk) throw new Error(`chunk ${msg.jobId}/${msg.chunkIndex} not found`);
  if (chunk.status === 'completed') return { skipped: true, rowsProcessed: chunk.rowsProcessed, rowsRejected: chunk.rowsRejected };
  const [job] = await deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, msg.jobId));
  if (!job) throw new Error(`job ${msg.jobId} not found`);

  await deps.db.update(ingestionChunks)
    .set({ status: 'processing', attempts: sql`${ingestionChunks.attempts} + 1`, updatedAt: new Date() })
    .where(eq(ingestionChunks.id, chunk.id));

  const writer = new BatchWriter(deps.db, deps.redis, deps.logger, msg.jobId, msg.chunkIndex, deps.config.upsertBatchSize);
  try {
    const { rangeStart, rangeEnd } = rangeFor(chunk);
    const obj = await deps.s3.send(new GetObjectCommand({ Bucket: deps.config.s3Bucket, Key: job.s3Key, Range: `bytes=${rangeStart}-${rangeEnd}` }));
    const body = obj.Body as Readable;

    let skipHeader = chunk.byteStart === 0;
    let lineNumber = 0;
    for await (const { line } of ownedLines(body, chunk, rangeStart)) {
      if (skipHeader) {
        skipHeader = false;
        const header = parseCsvLine(line);
        const matches = header.length === VENDOR_COLUMNS.length && VENDOR_COLUMNS.every((col, i) => header[i] === col);
        if (!matches) throw new Error(`unexpected header row: expected "${VENDOR_COLUMNS.join(',')}", got "${line}"`);
        continue;
      }
      lineNumber++;
      if (line.trim() === '') continue;
      await writer.add({ lineNumber, raw: line });
    }
    await writer.flush();
  } catch (err) {
    deps.logger.error({ err, jobId: msg.jobId, chunkIndex: msg.chunkIndex }, 'chunk failed; will be retried by the queue');
    const message = err instanceof Error ? err.message : String(err);
    await deps.db.update(ingestionChunks).set({ error: message.slice(0, 2000), updatedAt: new Date() })
      .where(eq(ingestionChunks.id, chunk.id))
      .catch((updateErr) => deps.logger.error({ err: updateErr, jobId: msg.jobId, chunkIndex: msg.chunkIndex }, 'failed to record chunk error on the chunk row'));
    throw err;
  }

  const result = await markChunkCompleted(deps.db, { jobId: msg.jobId, chunkIndex: msg.chunkIndex, rowsProcessed: writer.rowsProcessed, rowsRejected: writer.rowsRejected });
  deps.logger.info(
    { jobId: msg.jobId, chunkIndex: msg.chunkIndex, rowsProcessed: writer.rowsProcessed, rowsRejected: writer.rowsRejected, changed: result.changed },
    result.changed ? 'chunk complete' : 'chunk already finalized by a concurrent delivery; discarding this redelivery\'s bookkeeping',
  );
  return { skipped: !result.changed, rowsProcessed: writer.rowsProcessed, rowsRejected: writer.rowsRejected };
}
