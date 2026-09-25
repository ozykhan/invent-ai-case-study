import { count, eq } from 'drizzle-orm';
import { ingestionChunks, ingestionJobs, keys, products } from '@modaco/core';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { handleDeadLetter } from '../src/dlq';
import { pollOnce } from '../src/queue';
import { chunkMessageSchema, splitUpload } from '../src/splitter';
import { processChunk } from '../src/worker';
import { setupIngestTest, type IngestTestContext } from './helpers';

let ctx: IngestTestContext;
beforeAll(async () => { ctx = await setupIngestTest({ CHUNK_SIZE_BYTES: '4096', UPSERT_BATCH_SIZE: '100' }); });
afterAll(() => ctx.close());
beforeEach(async () => {
  await ctx.truncateAll();
  await Promise.all([ctx.deps.config.s3EventsQueueUrl, ctx.deps.config.chunkQueueUrl, ctx.deps.config.dlqUrl].map((u) => ctx.drainQueue(u)));
});

function vendorCsv(rows: number): { body: string; validSkus: number; badRows: number } {
  const cats = ['Accessories', 'Shoes', 'Bags'];
  const lines = ['sku,name,category,vendor_price,stock'];
  let badRows = 0;
  for (let i = 0; i < rows; i++) {
    if (i % 100 === 7) { lines.push(`BAD-${i},,${cats[i % 3]},1.00,1`); badRows++; continue; }
    lines.push(`SKU-${String(i).padStart(6, '0')},"Item ${i}, deluxe",${cats[i % 3]},${(1 + (i % 500)).toFixed(2)},${i % 50}`);
  }
  return { body: lines.join('\n') + '\n', validSkus: rows - badRows, badRows };
}

async function untilJob(jobId: string, pred: (j: typeof ingestionJobs.$inferSelect) => boolean, driver: () => Promise<unknown>, timeoutMs = 90_000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    if (job && pred(job)) return job;
    if (Date.now() > deadline) throw new Error(`timeout waiting for job; last state ${JSON.stringify(job)}`);
    await driver();
  }
}

const driveS3Events = () => pollOnce(ctx.deps.sqs, ctx.deps.config.s3EventsQueueUrl, async (m) => {
  const body = JSON.parse(m.Body ?? '{}');
  for (const rec of body.Records ?? []) await splitUpload(ctx.deps, { key: decodeURIComponent(rec.s3.object.key.replace(/\+/g, ' ')) });
}, { waitSeconds: 1 });

const driveChunks = () => pollOnce(ctx.deps.sqs, ctx.deps.config.chunkQueueUrl, async (m) => {
  await processChunk(ctx.deps, chunkMessageSchema.parse(JSON.parse(m.Body!)));
}, { waitSeconds: 1 });

describe('ingestion end to end', () => {
  it('uploads a file, splits via the S3 notification, processes every chunk, and lands every row exactly once', async () => {
    const { body, validSkus, badRows } = vendorCsv(2500);
    const jobId = await ctx.createJob('placeholder');
    const key = `uploads/${jobId}/vendor.csv`;
    await ctx.deps.db.update(ingestionJobs).set({ s3Key: key }).where(eq(ingestionJobs.id, jobId));
    await ctx.putObject(key, body);

    const split = await untilJob(jobId, (j) => j.status === 'processing', driveS3Events);
    expect(split.totalChunks).toBe(Math.ceil(Buffer.byteLength(body) / 4096));

    const done = await untilJob(jobId, (j) => j.status === 'completed' || j.status === 'failed', driveChunks);
    expect(done.status).toBe('completed');
    expect(done.completedChunks).toBe(done.totalChunks);
    expect(done.rowsRejected).toBe(badRows);
    expect(done.rowsProcessed).toBe(validSkus);

    const [productCount] = await ctx.deps.db.select({ n: count() }).from(products);
    expect(Number(productCount!.n)).toBe(validSkus);
    const chunks = await ctx.deps.db.select().from(ingestionChunks).where(eq(ingestionChunks.jobId, jobId));
    expect(chunks.every((c) => c.status === 'completed')).toBe(true);

    const [sample] = await ctx.deps.db.select().from(products).where(eq(products.sku, 'SKU-000010'));
    expect(sample).toMatchObject({ name: 'Item 10, deluxe', basePrice: '14.99', stock: 10 }); // 11.00 * 1.3 = 14.30 -> 14.99
    expect(Number(await ctx.deps.redis.get(keys.allVersion()))).toBeGreaterThan(0);
  });

  it('fails the job through the dead-letter path when a chunk keeps failing', async () => {
    const jobId = await ctx.createJob('placeholder');
    const key = `uploads/${jobId}/vendor.csv`;
    await ctx.deps.db.update(ingestionJobs).set({ s3Key: key }).where(eq(ingestionJobs.id, jobId));
    await ctx.putObject(key, vendorCsv(50).body);
    await untilJob(jobId, (j) => j.status === 'processing', driveS3Events);
    // Simulate the object disappearing so every attempt fails.
    await ctx.deps.db.update(ingestionJobs).set({ s3Key: `uploads/${jobId}/gone.csv` }).where(eq(ingestionJobs.id, jobId));
    const r = await driveChunks();
    expect(r.failed).toBe(1);
    // Three failed receives would route the message to the DLQ; call the handler directly since the visibility timeout is 360s.
    const [chunk] = await ctx.deps.db.select().from(ingestionChunks).where(eq(ingestionChunks.jobId, jobId));
    await handleDeadLetter(ctx.deps, { jobId, chunkIndex: chunk!.chunkIndex, byteStart: chunk!.byteStart, byteEnd: chunk!.byteEnd }, 'exceeded max receive count');
    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job).toMatchObject({ status: 'failed', failedChunks: 1 });
  });
});
