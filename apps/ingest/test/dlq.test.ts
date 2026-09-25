import { and, eq } from 'drizzle-orm';
import { ingestionChunks, ingestionJobs } from '@modaco/core';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { handleDeadLetter } from '../src/dlq';
import { markChunkCompleted } from '../src/job-state';
import { setupIngestTest, type IngestTestContext } from './helpers';

let ctx: IngestTestContext;
beforeAll(async () => { ctx = await setupIngestTest(); });
afterAll(() => ctx.close());
beforeEach(() => ctx.truncateAll());

describe('handleDeadLetter', () => {
  it('fails the chunk and the job on first delivery, and is a no-op on redelivery', async () => {
    const jobId = await ctx.createJob('uploads/placeholder/vendor.csv');
    await ctx.deps.db.insert(ingestionChunks).values([
      { jobId, chunkIndex: 0, byteStart: 0, byteEnd: 99 },
      { jobId, chunkIndex: 1, byteStart: 100, byteEnd: 199 },
    ]);
    await ctx.deps.db.update(ingestionJobs).set({ status: 'processing', totalChunks: 2 }).where(eq(ingestionJobs.id, jobId));

    // Chunk 1 completes normally; chunk 0 exhausts its retries and is dead-lettered — once every chunk is
    // accounted for (1 completed + 1 failed == totalChunks), the job ends 'failed'.
    await markChunkCompleted(ctx.deps.db, { jobId, chunkIndex: 1, rowsProcessed: 5, rowsRejected: 0 });
    const msg = { jobId, chunkIndex: 0, byteStart: 0, byteEnd: 99 };
    await handleDeadLetter(ctx.deps, msg, 'exceeded max receive count (receives=3)');

    const [chunk0] = await ctx.deps.db.select().from(ingestionChunks).where(and(eq(ingestionChunks.jobId, jobId), eq(ingestionChunks.chunkIndex, 0)));
    expect(chunk0).toMatchObject({ status: 'failed', error: 'exceeded max receive count (receives=3)' });

    let [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job).toMatchObject({ status: 'failed', completedChunks: 1, failedChunks: 1 });

    // A redelivery of the same already-dead-lettered message (a DLQ consumer retry, or a genuine SQS
    // redelivery) must not double-count the failure or clobber the recorded error.
    await handleDeadLetter(ctx.deps, msg, 'a different reason this time');
    const [chunk0Again] = await ctx.deps.db.select().from(ingestionChunks).where(and(eq(ingestionChunks.jobId, jobId), eq(ingestionChunks.chunkIndex, 0)));
    expect(chunk0Again).toMatchObject({ status: 'failed', error: 'exceeded max receive count (receives=3)' });

    [job] = await ctx.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
    expect(job).toMatchObject({ status: 'failed', completedChunks: 1, failedChunks: 1 });
  });
});
