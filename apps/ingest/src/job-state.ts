import { and, eq, notInArray, sql } from 'drizzle-orm';
import { ingestionChunks, ingestionJobs, type Db } from '@modaco/core';

/**
 * Chunk done: write its counts and atomically advance the job — but only if this delivery is the one that
 * gets to finalize the chunk. SQS is at-least-once and worker concurrency is > 1, so two deliveries of the
 * same chunk can call this concurrently (a genuine redelivery, or a race with a re-enqueue from a
 * redelivered S3 split event). The chunk UPDATE is conditioned on the row's current status, so Postgres's
 * row lock serializes the two: whichever commits first wins, and Postgres re-evaluates the WHERE clause
 * against the now-committed row for the second, which then matches zero rows and reports `changed: false`.
 * The job counters are only ever incremented by the delivery that won. Flips the job status to
 * completed/failed once every chunk is accounted for.
 */
export async function markChunkCompleted(db: Db, args: { jobId: string; chunkIndex: number; rowsProcessed: number; rowsRejected: number }): Promise<{ changed: boolean }> {
  return db.transaction(async (tx) => {
    const [updated] = await tx.update(ingestionChunks)
      .set({ status: 'completed', rowsProcessed: args.rowsProcessed, rowsRejected: args.rowsRejected, error: null, updatedAt: new Date() })
      .where(and(
        eq(ingestionChunks.jobId, args.jobId),
        eq(ingestionChunks.chunkIndex, args.chunkIndex),
        notInArray(ingestionChunks.status, ['completed', 'failed']),
      ))
      .returning({ id: ingestionChunks.id });
    if (!updated) return { changed: false };

    const [job] = await tx.update(ingestionJobs).set({
      completedChunks: sql`${ingestionJobs.completedChunks} + 1`,
      rowsProcessed: sql`${ingestionJobs.rowsProcessed} + ${args.rowsProcessed}`,
      rowsRejected: sql`${ingestionJobs.rowsRejected} + ${args.rowsRejected}`,
      updatedAt: new Date(),
    }).where(eq(ingestionJobs.id, args.jobId)).returning();
    if (job && job.completedChunks + job.failedChunks >= job.totalChunks) {
      await tx.update(ingestionJobs).set({ status: job.failedChunks > 0 ? 'failed' : 'completed', updatedAt: new Date() }).where(eq(ingestionJobs.id, args.jobId));
    }
    return { changed: true };
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
