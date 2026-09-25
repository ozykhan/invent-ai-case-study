import { eq } from 'drizzle-orm';
import { ingestionChunks, ingestionJobs } from '@modaco/core';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { jobIdFromKey, splitUpload } from '../src/splitter';
import { setupIngestTest, type IngestTestContext } from './helpers';

let ctx: IngestTestContext;
beforeAll(async () => { ctx = await setupIngestTest({ CHUNK_SIZE_BYTES: '4' }); });
afterAll(() => ctx.close());
beforeEach(async () => { await ctx.truncateAll(); await ctx.drainQueue(ctx.deps.config.chunkQueueUrl); });

describe('jobIdFromKey', () => {
  it('extracts the job id from an upload key', () => {
    expect(jobIdFromKey('uploads/0b6a1a4e-2c1b-4c2a-9f7f-8d1e7a5b3c21/vendor.csv')).toBe('0b6a1a4e-2c1b-4c2a-9f7f-8d1e7a5b3c21');
    expect(jobIdFromKey('other/x.csv')).toBeNull();
  });
});

describe('splitUpload', () => {
  it('enqueues one byte-range message per chunk without reading the body', async () => {
    const jobId = await ctx.createJob('placeholder');
    const key = `uploads/${jobId}/vendor.csv`;
    await ctx.deps.db.update(ingestionJobs).set({ s3Key: key }).where(eq(ingestionJobs.id, jobId));
    await ctx.putObject(key, '0123456789'); // 10 bytes, chunk size 4 -> 3 chunks

    const result = await splitUpload(ctx.deps, { key });
    expect(result).toEqual({ jobId, totalChunks: 3, contentLength: 10 });

    const chunks = await ctx.deps.db.select().from(ingestionChunks).where(eq(ingestionChunks.jobId, jobId)).orderBy(ingestionChunks.chunkIndex);
    expect(chunks.map((c) => [c.chunkIndex, c.byteStart, c.byteEnd, c.status])).toEqual([[0, 0, 3, 'pending'], [1, 4, 7, 'pending'], [2, 8, 9, 'pending']]);
    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job).toMatchObject({ status: 'processing', totalChunks: 3 });

    const messages = await ctx.receiveAll(ctx.deps.config.chunkQueueUrl, 3);
    const bodies = messages.map((m) => JSON.parse(m.Body!)).sort((a, b) => a.chunkIndex - b.chunkIndex);
    expect(bodies).toEqual([
      { jobId, chunkIndex: 0, byteStart: 0, byteEnd: 3 },
      { jobId, chunkIndex: 1, byteStart: 4, byteEnd: 7 },
      { jobId, chunkIndex: 2, byteStart: 8, byteEnd: 9 },
    ]);
  });

  it('is idempotent: a re-run re-sends only pending chunks and inserts nothing new', async () => {
    const jobId = await ctx.createJob('placeholder');
    const key = `uploads/${jobId}/vendor.csv`;
    await ctx.deps.db.update(ingestionJobs).set({ s3Key: key }).where(eq(ingestionJobs.id, jobId));
    await ctx.putObject(key, '0123456789');
    await splitUpload(ctx.deps, { key });
    await ctx.receiveAll(ctx.deps.config.chunkQueueUrl, 3);
    await ctx.deps.db.update(ingestionChunks).set({ status: 'completed' }).where(eq(ingestionChunks.chunkIndex, 0));

    const again = await splitUpload(ctx.deps, { key });
    expect(again).toEqual({ jobId, totalChunks: 3, contentLength: 10 });
    expect((await ctx.deps.db.select().from(ingestionChunks).where(eq(ingestionChunks.jobId, jobId))).length).toBe(3);
    const messages = await ctx.receiveAll(ctx.deps.config.chunkQueueUrl, 2, 5000);
    expect(messages.map((m) => JSON.parse(m.Body!).chunkIndex).sort()).toEqual([1, 2]);
  });

  it('completes an empty file immediately', async () => {
    const jobId = await ctx.createJob('placeholder');
    const key = `uploads/${jobId}/empty.csv`;
    await ctx.putObject(key, '');
    expect(await splitUpload(ctx.deps, { key })).toEqual({ jobId, totalChunks: 0, contentLength: 0 });
    const [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job!.status).toBe('completed');
  });

  it('returns null for a key with no job', async () => {
    const key = 'uploads/00000000-0000-0000-0000-000000000000/x.csv';
    await ctx.putObject(key, 'abc');
    expect(await splitUpload(ctx.deps, { key })).toBeNull();
  });
});
