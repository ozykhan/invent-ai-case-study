import { HeadObjectCommand } from '@aws-sdk/client-s3';
import { SendMessageBatchCommand } from '@aws-sdk/client-sqs';
import { and, eq } from 'drizzle-orm';
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
 */
export async function splitUpload(deps: Deps, input: { key: string }): Promise<{ jobId: string; totalChunks: number; contentLength: number } | null> {
  const jobId = jobIdFromKey(input.key);
  if (!jobId) { deps.logger.warn({ key: input.key }, 'ignoring object outside uploads/<jobId>/'); return null; }
  const [job] = await deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, jobId));
  if (!job) { deps.logger.warn({ key: input.key, jobId }, 'no job for key; ignoring'); return null; }

  const head = await deps.s3.send(new HeadObjectCommand({ Bucket: deps.config.s3Bucket, Key: input.key }));
  const contentLength = head.ContentLength ?? 0;
  const chunks = computeChunks(contentLength, deps.config.chunkSizeBytes);

  if (chunks.length === 0) {
    await deps.db.update(ingestionJobs).set({ status: 'completed', s3Key: input.key, totalChunks: 0, updatedAt: new Date() }).where(eq(ingestionJobs.id, jobId));
    return { jobId, totalChunks: 0, contentLength };
  }

  await deps.db.transaction(async (tx) => {
    await tx.update(ingestionJobs).set({ status: 'splitting', s3Key: input.key, updatedAt: new Date() }).where(eq(ingestionJobs.id, jobId));
    await tx.insert(ingestionChunks)
      .values(chunks.map((c) => ({ jobId, chunkIndex: c.chunkIndex, byteStart: c.byteStart, byteEnd: c.byteEnd })))
      .onConflictDoNothing({ target: [ingestionChunks.jobId, ingestionChunks.chunkIndex] });
    await tx.update(ingestionJobs).set({ status: 'processing', totalChunks: chunks.length, updatedAt: new Date() }).where(eq(ingestionJobs.id, jobId));
  });

  const pending = await deps.db.select({ chunkIndex: ingestionChunks.chunkIndex, byteStart: ingestionChunks.byteStart, byteEnd: ingestionChunks.byteEnd })
    .from(ingestionChunks).where(and(eq(ingestionChunks.jobId, jobId), eq(ingestionChunks.status, 'pending')));
  await enqueue(deps, jobId, pending);

  deps.logger.info({ jobId, contentLength, totalChunks: chunks.length, enqueued: pending.length }, 'split complete');
  return { jobId, totalChunks: chunks.length, contentLength };
}
