import { and, eq, sql } from 'drizzle-orm';
import { ingestionChunks, ingestionJobs, type Db } from '@modaco/core';

/** Chunk done: write its counts and atomically advance the job. Flips the job status when every chunk is accounted for. */
export async function markChunkCompleted(db: Db, args: { jobId: string; chunkIndex: number; rowsProcessed: number; rowsRejected: number }): Promise<void> {
  await db.transaction(async (tx) => {
    await tx.update(ingestionChunks)
      .set({ status: 'completed', rowsProcessed: args.rowsProcessed, rowsRejected: args.rowsRejected, error: null, updatedAt: new Date() })
      .where(and(eq(ingestionChunks.jobId, args.jobId), eq(ingestionChunks.chunkIndex, args.chunkIndex)));
    const [job] = await tx.update(ingestionJobs).set({
      completedChunks: sql`${ingestionJobs.completedChunks} + 1`,
      rowsProcessed: sql`${ingestionJobs.rowsProcessed} + ${args.rowsProcessed}`,
      rowsRejected: sql`${ingestionJobs.rowsRejected} + ${args.rowsRejected}`,
      updatedAt: new Date(),
    }).where(eq(ingestionJobs.id, args.jobId)).returning();
    if (job && job.completedChunks + job.failedChunks >= job.totalChunks) {
      await tx.update(ingestionJobs).set({ status: job.failedChunks > 0 ? 'failed' : 'completed', updatedAt: new Date() }).where(eq(ingestionJobs.id, args.jobId));
    }
  });
}

/** Dead-letter path: a chunk that exhausted its retries. The job is failed immediately, never left silently partial. */
export async function markChunkFailed(db: Db, args: { jobId: string; chunkIndex: number; error: string }): Promise<{ changed: boolean }> {
  return db.transaction(async (tx) => {
    const [chunk] = await tx.select().from(ingestionChunks)
      .where(and(eq(ingestionChunks.jobId, args.jobId), eq(ingestionChunks.chunkIndex, args.chunkIndex))).for('update');
    if (!chunk || chunk.status === 'completed' || chunk.status === 'failed') return { changed: false };
    await tx.update(ingestionChunks).set({ status: 'failed', error: args.error, updatedAt: new Date() }).where(eq(ingestionChunks.id, chunk.id));
    await tx.update(ingestionJobs).set({
      status: 'failed',
      failedChunks: sql`${ingestionJobs.failedChunks} + 1`,
      error: `chunk ${args.chunkIndex}: ${args.error}`.slice(0, 2000),
      updatedAt: new Date(),
    }).where(eq(ingestionJobs.id, args.jobId));
    return { changed: true };
  });
}
