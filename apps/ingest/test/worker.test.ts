import { eq, sql } from 'drizzle-orm';
import { Readable } from 'node:stream';
import { categories, computeChunks, ingestionChunks, ingestionJobs, ingestionRejections, keys, products } from '@modaco/core';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { markChunkFailed } from '../src/job-state';
import { processChunk } from '../src/worker';
import { setupIngestTest, type IngestTestContext } from './helpers';

let ctx: IngestTestContext;
beforeAll(async () => { ctx = await setupIngestTest({ UPSERT_BATCH_SIZE: '3' }); });
afterAll(() => ctx.close());
beforeEach(() => ctx.truncateAll());

const csv = [
  'sku,name,category,vendor_price,stock',
  'S1,Belt,Accessories,10.00,5',
  'S2,"Hat, wool",Accessories,20.00,0',
  'S3,Boot,Shoes,50.00,2',
  'BAD1,,Shoes,50.00,2',
  'S4,Bag,Bags,0,1',
  'S1,Belt v2,Accessories,12.00,9',
  'S5,Scarf,Accessories,7.50,3',
  'short,row',
  'S6,Coat,Outerwear,100.00,1',
].join('\n') + '\n';
const dataRows = 9;

async function prepare(body: string, chunkSize: number) {
  const jobId = await ctx.createJob('placeholder');
  const key = `uploads/${jobId}/vendor.csv`;
  await ctx.deps.db.update(ingestionJobs).set({ s3Key: key }).where(eq(ingestionJobs.id, jobId));
  await ctx.putObject(key, body);
  const chunks = computeChunks(Buffer.byteLength(body), chunkSize);
  await ctx.deps.db.insert(ingestionChunks).values(chunks.map((c) => ({ jobId, ...c })));
  await ctx.deps.db.update(ingestionJobs).set({ status: 'processing', totalChunks: chunks.length }).where(eq(ingestionJobs.id, jobId));
  return { jobId, chunks };
}

describe('processChunk', () => {
  it('prices, upserts, rejects, and completes the job across many small chunks', async () => {
    const { jobId, chunks } = await prepare(csv, 40);
    expect(chunks.length).toBeGreaterThan(3);
    for (const c of chunks) await processChunk(ctx.deps, { jobId, ...c });

    const rows = await ctx.deps.db.select().from(products).orderBy(products.sku);
    expect(rows.map((r) => [r.sku, r.name, r.basePrice, r.stock])).toEqual([
      ['S1', 'Belt v2', '15.99', 9],   // 12.00 * 1.3 = 15.60 -> 15.99 (both S1 rows land in the same batch here, so the later one wins)
      ['S2', 'Hat, wool', '26.99', 0], // 20.00 * 1.3 = 26.00 -> 26.99
      ['S3', 'Boot', '65.99', 2],      // 50 * 1.3 = 65.00 -> 65.99
      ['S5', 'Scarf', '9.99', 3],      // 7.50 * 1.3 = 9.75 -> 9.99
      ['S6', 'Coat', '130.99', 1],     // 100 * 1.3 = 130.00 -> 130.99
    ]);
    // S4 (Bags) is rejected for vendor_price == 0, so "Bags" must never be created: a row that fails
    // validation must not have the side effect of creating its category.
    const cats = await ctx.deps.db.select().from(categories).orderBy(categories.name);
    expect(cats.map((c) => c.slug)).toEqual(['accessories', 'outerwear', 'shoes']);

    const rejections = await ctx.deps.db.select().from(ingestionRejections).where(eq(ingestionRejections.jobId, jobId));
    expect(rejections).toHaveLength(3);
    expect(rejections.map((r) => r.reason)).toEqual(expect.arrayContaining([
      expect.stringContaining('name'), expect.stringContaining('vendor_price'), expect.stringContaining('expected 5 fields'),
    ]));

    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job).toMatchObject({ status: 'completed', completedChunks: chunks.length, failedChunks: 0, rowsRejected: 3 });
    expect(job!.rowsProcessed + job!.rowsRejected).toBe(dataRows);

    const accessories = cats.find((c) => c.slug === 'accessories')!;
    expect(Number(await ctx.deps.redis.get(keys.categoryVersion(accessories.id)))).toBeGreaterThan(0);
    expect(Number(await ctx.deps.redis.get(keys.allVersion()))).toBeGreaterThan(0);
    const s1 = rows.find((r) => r.sku === 'S1')!;
    expect(await ctx.deps.redis.get(keys.stock(s1.id))).toBe('9');
  });

  it('is a no-op when the chunk is already completed', async () => {
    const { jobId, chunks } = await prepare(csv, 10_000);
    const first = await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    expect(first.skipped).toBe(false);
    const second = await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    expect(second).toEqual({ skipped: true, rowsProcessed: first.rowsProcessed, rowsRejected: first.rowsRejected });
    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job!.completedChunks).toBe(1);
  });

  it('uses category pricing overrides and updates an existing product', async () => {
    const [shoes] = await ctx.deps.db.insert(categories)
      .values({ name: 'Shoes', slug: 'shoes', marginPct: '0', priceFloor: '60.00', priceCeiling: '61.00' }).returning();
    await ctx.deps.db.insert(products).values({ sku: 'S3', name: 'Old Boot', categoryId: shoes!.id, basePrice: '10.00', stock: 0 });
    const { jobId, chunks } = await prepare('sku,name,category,vendor_price,stock\nS3,New Boot,Shoes,50.00,2\n', 10_000);
    await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    const [boot] = await ctx.deps.db.select().from(products).where(eq(products.sku, 'S3'));
    expect(boot).toMatchObject({ name: 'New Boot', basePrice: '60.00', stock: 2 });
  });

  it('bumps the old category version when a SKU moves to a new category', async () => {
    const [hats] = await ctx.deps.db.insert(categories).values({ name: 'Hats', slug: 'hats' }).returning();
    await ctx.deps.db.insert(products).values({ sku: 'S3', name: 'Old Boot', categoryId: hats!.id, basePrice: '10.00', stock: 1 });
    const { jobId, chunks } = await prepare('sku,name,category,vendor_price,stock\nS3,Boot,Shoes,50.00,2\n', 10_000);
    await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    const [shoes] = await ctx.deps.db.select().from(categories).where(eq(categories.slug, 'shoes'));
    expect(Number(await ctx.deps.redis.get(keys.categoryVersion(hats!.id)))).toBeGreaterThan(0);
    expect(Number(await ctx.deps.redis.get(keys.categoryVersion(shoes!.id)))).toBeGreaterThan(0);
  });

  it('does not create a category for a row rejected on validation (e.g. vendor_price)', async () => {
    const { jobId, chunks } = await prepare('sku,name,category,vendor_price,stock\nS4,Bag,Bags,0,1\n', 10_000);
    const r = await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    expect(r).toMatchObject({ rowsProcessed: 0, rowsRejected: 1 });
    const bags = await ctx.deps.db.select().from(categories).where(eq(categories.slug, 'bags'));
    expect(bags).toHaveLength(0);
  });

  it('rejects a row whose category name collides with an existing slug', async () => {
    await ctx.deps.db.insert(categories).values({ name: 'Shoes', slug: 'shoes' });
    const { jobId, chunks } = await prepare('sku,name,category,vendor_price,stock\nS9,Sneaker,shoes,50.00,2\n', 10_000);
    const r = await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    expect(r).toMatchObject({ rowsProcessed: 0, rowsRejected: 1 });
    const [rej] = await ctx.deps.db.select().from(ingestionRejections).where(eq(ingestionRejections.jobId, jobId));
    expect(rej!.reason).toContain('collides');
  });

  it('records the error, leaves the chunk retryable, and rethrows on failure', async () => {
    const { jobId, chunks } = await prepare(csv, 10_000);
    await ctx.deps.db.update(ingestionJobs).set({ s3Key: `uploads/${jobId}/missing.csv` }).where(eq(ingestionJobs.id, jobId));
    await expect(processChunk(ctx.deps, { jobId, ...chunks[0]! })).rejects.toThrow();
    const [chunk] = await ctx.deps.db.select().from(ingestionChunks).where(eq(ingestionChunks.jobId, jobId));
    expect(chunk).toMatchObject({ status: 'processing', attempts: 1 });
    expect(chunk!.error).toBeTruthy();
  });

  it('fails a chunk whose header does not match the expected vendor columns', async () => {
    const body = 'name,sku,category,vendor_price,stock\nBelt,S1,Accessories,10.00,5\n';
    const { jobId, chunks } = await prepare(body, 10_000);
    await expect(processChunk(ctx.deps, { jobId, ...chunks[0]! })).rejects.toThrow(/header/i);
    const [chunk] = await ctx.deps.db.select().from(ingestionChunks).where(eq(ingestionChunks.jobId, jobId));
    expect(chunk).toMatchObject({ status: 'processing', attempts: 1 });
    expect(chunk!.error).toContain('header');
  });

  it('accepts a UTF-8 BOM-prefixed header (Excel "CSV UTF-8" exports)', async () => {
    const body = '﻿sku,name,category,vendor_price,stock\nS3,Boot,Shoes,50.00,2\n';
    const { jobId, chunks } = await prepare(body, 10_000);
    const r = await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    expect(r).toMatchObject({ skipped: false, rowsProcessed: 1, rowsRejected: 0 });
    const [boot] = await ctx.deps.db.select().from(products).where(eq(products.sku, 'S3'));
    expect(boot).toMatchObject({ name: 'Boot', basePrice: '65.99', stock: 2 }); // 50 * 1.3 = 65.00 -> 65.99
  });

  it('is a no-op when the chunk is already marked failed (e.g. a DLQ redrive)', async () => {
    const { jobId, chunks } = await prepare(csv, 10_000); // a single chunk
    await ctx.deps.db.update(ingestionChunks).set({ status: 'failed', error: 'dead-lettered', attempts: 3 }).where(eq(ingestionChunks.jobId, jobId));
    await ctx.deps.db.update(ingestionJobs).set({ status: 'failed', failedChunks: 1 }).where(eq(ingestionJobs.id, jobId));

    const r = await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    expect(r.skipped).toBe(true);

    // The chunk must stay exactly as the DLQ redrive found it: no reprocessing, no cleared error.
    const [chunk] = await ctx.deps.db.select().from(ingestionChunks).where(eq(ingestionChunks.jobId, jobId));
    expect(chunk).toMatchObject({ status: 'failed', error: 'dead-lettered', attempts: 3 });

    // The job's counters must be untouched: this delivery must never reach markChunkCompleted.
    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job).toMatchObject({ status: 'failed', failedChunks: 1, completedChunks: 0, rowsProcessed: 0, rowsRejected: 0 });

    expect(await ctx.deps.db.select().from(products)).toHaveLength(0);
    expect(await ctx.deps.db.select().from(ingestionRejections).where(eq(ingestionRejections.jobId, jobId))).toHaveLength(0);
  });

  it('is safe against two concurrent deliveries of the same not-yet-completed chunk', async () => {
    const { jobId, chunks } = await prepare(csv, 10_000); // a single chunk
    const [a, b] = await Promise.all([
      processChunk(ctx.deps, { jobId, ...chunks[0]! }),
      processChunk(ctx.deps, { jobId, ...chunks[0]! }),
    ]);
    // Exactly one delivery wins the race to finalize the chunk; the other reports skipped.
    expect([a.skipped, b.skipped].sort()).toEqual([false, true]);

    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job).toMatchObject({ status: 'completed', completedChunks: 1, failedChunks: 0, rowsRejected: 3 });
    expect(job!.rowsProcessed + job!.rowsRejected).toBe(dataRows);

    const [chunk] = await ctx.deps.db.select().from(ingestionChunks).where(eq(ingestionChunks.jobId, jobId));
    expect(chunk!.status).toBe('completed');
  });

  it('does not duplicate rejection rows when a chunk is retried after a partial commit', async () => {
    const { jobId, chunks } = await prepare(csv, 10_000); // a single chunk
    await processChunk(ctx.deps, { jobId, ...chunks[0]! });

    // Simulate a crash between BatchWriter's per-batch commits (already durable, including the
    // rejection rows) and the final markChunkCompleted call: roll the chunk and job bookkeeping back to
    // their pre-finalize state, as if that last step never ran, while the already-committed rows remain.
    await ctx.deps.db.update(ingestionChunks).set({ status: 'processing' }).where(eq(ingestionChunks.jobId, jobId));
    await ctx.deps.db.update(ingestionJobs)
      .set({ status: 'processing', completedChunks: 0, rowsProcessed: 0, rowsRejected: 0 })
      .where(eq(ingestionJobs.id, jobId));

    await processChunk(ctx.deps, { jobId, ...chunks[0]! });

    const rejections = await ctx.deps.db.select().from(ingestionRejections).where(eq(ingestionRejections.jobId, jobId));
    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(rejections).toHaveLength(3);
    expect(job!.rowsRejected).toBe(3);
    expect(rejections.length).toBe(job!.rowsRejected);
  });

  it('rejects an int4-overflowing stock and NUL-bearing rows without failing the chunk', async () => {
    // Before validation bounded stock and rejected NUL, either row made Postgres abort the whole batch
    // transaction (22003 / 22021) on every retry, dead-lettering the chunk and failing the job.
    const body = [
      'sku,name,category,vendor_price,stock',
      'G1,Belt,Accessories,10.00,5',
      'OVF,Boot,Shoes,50.00,3000000000',
      'NUL1,Ha\u0000t,Accessories,5.00,1',
      'G2,Scarf,Accessories,7.50,3',
      'nul\u0000short',
      'G3,Coat,Outerwear,100.00,1',
    ].join('\n') + '\n';
    const { jobId, chunks } = await prepare(body, 10_000);
    const r = await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    expect(r).toMatchObject({ skipped: false, rowsProcessed: 3, rowsRejected: 3 });

    const rows = await ctx.deps.db.select().from(products).orderBy(products.sku);
    expect(rows.map((p) => p.sku)).toEqual(['G1', 'G2', 'G3']);

    const rejections = await ctx.deps.db.select().from(ingestionRejections)
      .where(eq(ingestionRejections.jobId, jobId)).orderBy(ingestionRejections.lineNumber);
    expect(rejections.map((x) => [x.lineNumber, x.reason])).toEqual([
      [2, expect.stringContaining('stock')],
      [3, expect.stringMatching(/name.*NUL/)],
      [5, expect.stringContaining('expected 5 fields')],
    ]);
    // NUL is stripped from the stored raw line (Postgres text cannot hold it); the rest is kept verbatim.
    expect(rejections.map((x) => x.rawLine)).toEqual(['OVF,Boot,Shoes,50.00,3000000000', 'NUL1,Hat,Accessories,5.00,1', 'nulshort']);

    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job).toMatchObject({ status: 'completed', failedChunks: 0, rowsProcessed: 3, rowsRejected: 3 });
  });

  it('falls back to row-by-row writes when the database rejects a batch, rejecting only the offending row', async () => {
    // Nothing that passes validation is known to be refused by Postgres any more, so a trigger stands in
    // for "a row the database rejects": it raises check_violation (SQLSTATE 23514) for one SKU only.
    await ctx.deps.db.execute(sql.raw(`
      create or replace function test_poison_sku() returns trigger language plpgsql as $$
      begin
        if new.sku = 'POISON' then raise exception 'poisoned sku' using errcode = 'check_violation'; end if;
        return new;
      end $$;
      create trigger test_poison_sku before insert or update on products for each row execute function test_poison_sku();
    `));
    try {
      // UPSERT_BATCH_SIZE is 3 in this suite: P1, POISON and a malformed row share the first batch, so the
      // batch's rejection insert rolls back with it and must be re-applied by the fallback too.
      const body = 'sku,name,category,vendor_price,stock\nP1,Belt,Accessories,10.00,5\nPOISON,Hat,Accessories,20.00,1\nshort,row\nP2,Boot,Shoes,50.00,2\n';
      const { jobId, chunks } = await prepare(body, 10_000);
      const r = await processChunk(ctx.deps, { jobId, ...chunks[0]! });
      expect(r).toMatchObject({ skipped: false, rowsProcessed: 2, rowsRejected: 2 });

      const rows = await ctx.deps.db.select().from(products).orderBy(products.sku);
      expect(rows.map((p) => p.sku)).toEqual(['P1', 'P2']);
      const rejections = await ctx.deps.db.select().from(ingestionRejections)
        .where(eq(ingestionRejections.jobId, jobId)).orderBy(ingestionRejections.lineNumber);
      expect(rejections.map((x) => [x.lineNumber, x.reason])).toEqual([
        [2, expect.stringMatching(/database.*23514.*poisoned sku/)],
        [3, expect.stringContaining('expected 5 fields')],
      ]);
      const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
      expect(job).toMatchObject({ status: 'completed', failedChunks: 0, rowsProcessed: 2, rowsRejected: 2 });
      const p1 = rows.find((p) => p.sku === 'P1')!;
      expect(await ctx.deps.redis.get(keys.stock(p1.id))).toBe('5');
    } finally {
      await ctx.deps.db.execute(sql.raw('drop trigger if exists test_poison_sku on products; drop function if exists test_poison_sku();'));
    }
  });

  it('fails the chunk (for a retry) when S3 returns fewer bytes than its ContentLength', async () => {
    // ownedLines cannot tell a truncated body from the end of the range: without the length check the
    // cut-off tail would be priced as a complete line and every line after it silently dropped.
    const { jobId, chunks } = await prepare('sku,name,category,vendor_price,stock\nS1,Belt,Accessories,10.00,5\n', 10_000);
    const truncated = Buffer.from('sku,name,category,vendor_price,stock\nS1,Belt,Accessories,10.0');
    const s3 = { send: async () => ({ Body: Readable.from([truncated]), ContentLength: truncated.length + 3 }) } as unknown as typeof ctx.deps.s3;
    await expect(processChunk({ ...ctx.deps, s3 }, { jobId, ...chunks[0]! })).rejects.toThrow(/short read/);
    expect(await ctx.deps.db.select().from(products)).toHaveLength(0);
    expect(await ctx.deps.db.select().from(ingestionRejections)).toHaveLength(0);
  });

  it('drops stock counters it failed to overwrite, so reads fall back to postgres instead of a stale value', async () => {
    const [shoes] = await ctx.deps.db.insert(categories).values({ name: 'Shoes', slug: 'shoes' }).returning();
    const [boot] = await ctx.deps.db.insert(products).values({ sku: 'S3', name: 'Boot', categoryId: shoes!.id, basePrice: '10.00', stock: 1 }).returning();
    await ctx.deps.redis.set(keys.stock(boot!.id), '1');
    const failingPipeline = { set() { return failingPipeline; }, exec: async () => { throw new Error('transient'); } };
    const pipeline = vi.spyOn(ctx.deps.redis, 'pipeline').mockReturnValueOnce(failingPipeline as never);
    const { jobId, chunks } = await prepare('sku,name,category,vendor_price,stock\nS3,Boot,Shoes,50.00,7\n', 10_000);
    await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    pipeline.mockRestore();
    expect(await ctx.deps.redis.get(keys.stock(boot!.id))).toBeNull();
  });

  it('markChunkFailed fails the job once and ignores completed chunks', async () => {
    const { jobId, chunks } = await prepare(csv, 60);
    await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    expect(await markChunkFailed(ctx.deps.db, { jobId, chunkIndex: 0, error: 'x' })).toEqual({ changed: false });
    expect(await markChunkFailed(ctx.deps.db, { jobId, chunkIndex: 1, error: 'boom' })).toEqual({ changed: true });
    expect(await markChunkFailed(ctx.deps.db, { jobId, chunkIndex: 1, error: 'boom again' })).toEqual({ changed: false });
    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job).toMatchObject({ status: 'failed', failedChunks: 1 });
    expect(job!.error).toContain('chunk 1');
  });
});
