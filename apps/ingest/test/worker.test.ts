import { eq } from 'drizzle-orm';
import { categories, computeChunks, ingestionChunks, ingestionJobs, ingestionRejections, keys, products } from '@modaco/core';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
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
      ['S1', 'Belt v2', '15.99', 9],   // 12.00 * 1.3 = 15.60 -> 15.99 (last occurrence wins)
      ['S2', 'Hat, wool', '26.99', 0], // 20.00 * 1.3 = 26.00 -> 26.99
      ['S3', 'Boot', '65.99', 2],      // 50 * 1.3 = 65.00 -> 65.99
      ['S5', 'Scarf', '9.99', 3],      // 7.50 * 1.3 = 9.75 -> 9.99
      ['S6', 'Coat', '130.99', 1],     // 100 * 1.3 = 130.00 -> 130.99
    ]);
    const cats = await ctx.deps.db.select().from(categories).orderBy(categories.name);
    expect(cats.map((c) => c.slug)).toEqual(['accessories', 'bags', 'outerwear', 'shoes']);

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

  it('uses category pricing overrides and updates existing products', async () => {
    await ctx.deps.db.insert(categories).values({ name: 'Shoes', slug: 'shoes', marginPct: '0', priceFloor: '60.00', priceCeiling: '61.00' });
    const { jobId, chunks } = await prepare('sku,name,category,vendor_price,stock\nS3,Old Boot,Shoes,50.00,2\n', 10_000);
    await processChunk(ctx.deps, { jobId, ...chunks[0]! });
    const [boot] = await ctx.deps.db.select().from(products).where(eq(products.sku, 'S3'));
    expect(boot).toMatchObject({ name: 'Old Boot', basePrice: '60.00', stock: 2 });
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
