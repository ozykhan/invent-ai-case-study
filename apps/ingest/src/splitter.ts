import { HeadObjectCommand } from '@aws-sdk/client-s3';
import { SendMessageBatchCommand } from '@aws-sdk/client-sqs';
import { and, eq, inArray } from 'drizzle-orm';
import { computeChunks, ingestionChunks, ingestionJobs, type ChunkRange } from '@modaco/core';
import { z } from 'zod';
import type { IngestDeps } from './deps';

export const chunkMessageSchema = z.object({
  jobId: z.string().uuid(),
  chunkIndex: z.number().int().min(0),
  byteStart: z.number().int().min(0),
  byteEnd: z.number().int().min(0),
});
export type ChunkMessage = z.infer<typeof chunkMessageSchema>;

const KEY_RE = /^uploads\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\//;
export function jobIdFromKey(key: string): string | null {
  return KEY_RE.exec(key)?.[1] ?? null;
}

type Deps = Pick<IngestDeps, 'db' | 's3' | 'sqs' | 'config' | 'logger'>;

/** Statuses from which a split may still run: the job hasn't been finalized by the last chunk worker yet. */
const SPLITTABLE_STATUSES = ['pending', 'splitting', 'processing'] as const;

async function enqueue(deps: Deps, jobId: string, chunks: ChunkRange[]): Promise<void> {
  for (let i = 0; i < chunks.length; i += 10) {
    const batch = chunks.slice(i, i + 10);
    const res = await deps.sqs.send(new SendMessageBatchCommand({
      QueueUrl: deps.config.chunkQueueUrl,
      Entries: batch.map((c) => ({
        Id: String(c.chunkIndex),
        MessageBody: JSON.stringify({ jobId, chunkIndex: c.chunkIndex, byteStart: c.byteStart, byteEnd: c.byteEnd } satisfies ChunkMessage),
      })),
    }));
    if (res.Failed?.length) throw new Error(`sqs batch send failed for chunks ${res.Failed.map((f) => f.Id).join(',')}`);
  }
}

/**
 * Splits an uploaded file into byte-range chunks using only its size (HEAD), records them, and enqueues one message per chunk.
 * Runtime is independent of file size. Safe to re-run: existing chunk rows are kept and only still-pending chunks are re-sent.
 *
 * A redelivered S3 event (or an at-least-once retry) must never resurrect a job the last chunk worker already finalized:
 * once a job is 'completed' or 'failed' this is a no-op, and every status write below is conditioned on the job still
 * being in a splittable status, so a last-worker finalize racing concurrently with a split loses cleanly (0 rows
 * updated, nothing inserted, nothing enqueued) instead of being clobbered back to 'processing'.
 */
export async function splitUpload(deps: Deps, input: { key: string }): Promise<{ jobId: string; totalChunks: number; contentLength: number } | null> {
  const jobId = jobIdFromKey(input.key);
  if (!jobId) { deps.logger.warn({ key: input.key }, 'ignoring object outside uploads/<jobId>/'); return null; }
  const [job] = await deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
  if (!job) { deps.logger.warn({ key: input.key, jobId }, 'no job for key; ignoring'); return null; }

  const head = await deps.s3.send(new HeadObjectCommand({ Bucket: deps.config.s3Bucket, Key: input.key }));
  const contentLength = head.ContentLength ?? 0;

  if (job.status === 'completed' || job.status === 'failed') {
    deps.logger.info({ jobId, status: job.status }, 'job already finished; ignoring redelivered split event');
    return { jobId, totalChunks: job.totalChunks, contentLength };
  }

  const chunks = computeChunks(contentLength, deps.config.chunkSizeBytes);

  if (chunks.length === 0) {
    const done = await deps.db.update(ingestionJobs)
      .set({ status: 'completed', s3Key: input.key, totalChunks: 0, updatedAt: new Date() })
      .where(and(eq(ingestionJobs.id, jobId), inArray(ingestionJobs.status, SPLITTABLE_STATUSES)))
      .returning({ id: ingestionJobs.id });
    if (done.length === 0) deps.logger.info({ jobId }, 'job finished concurrently; skipping empty-file completion');
    return { jobId, totalChunks: 0, contentLength };
  }

  let raced = false;
  await deps.db.transaction(async (tx) => {
    const updated = await tx.update(ingestionJobs)
      .set({ status: 'processing', s3Key: input.key, totalChunks: chunks.length, updatedAt: new Date() })
      .where(and(eq(ingestionJobs.id, jobId), inArray(ingestionJobs.status, SPLITTABLE_STATUSES)))
      .returning({ id: ingestionJobs.id });
    if (updated.length === 0) { raced = true; return; }
    await tx.insert(ingestionChunks)
      .values(chunks.map((c) => ({ jobId, chunkIndex: c.chunkIndex, byteStart: c.byteStart, byteEnd: c.byteEnd })))
      .onConflictDoNothing({ target: [ingestionChunks.jobId, ingestionChunks.chunkIndex] });
  });

  if (raced) {
    deps.logger.info({ jobId }, 'job finished concurrently during split; skipping enqueue');
    return { jobId, totalChunks: chunks.length, contentLength };
  }

  const pending = await deps.db.select({ chunkIndex: ingestionChunks.chunkIndex, byteStart: ingestionChunks.byteStart, byteEnd: ingestionChunks.byteEnd })
    .from(ingestionChunks).where(and(eq(ingestionChunks.jobId, jobId), eq(ingestionChunks.status, 'pending')));
  await enqueue(deps, jobId, pending);

  deps.logger.info({ jobId, contentLength, totalChunks: chunks.length, enqueued: pending.length }, 'split complete');
  return { jobId, totalChunks: chunks.length, contentLength };
}
