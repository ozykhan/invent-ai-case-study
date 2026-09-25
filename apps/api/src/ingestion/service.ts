import { PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { asc, count, eq } from 'drizzle-orm';
import { ingestionJobs, ingestionRejections } from '@modaco/core';
import type { AppDeps } from '../deps';

export const PRESIGN_TTL_SECONDS = 900;

type JobRow = typeof ingestionJobs.$inferSelect;
export interface JobView {
  id: string; status: JobRow['status']; s3Key: string; totalChunks: number; completedChunks: number; failedChunks: number;
  rowsProcessed: number; rowsRejected: number; error: string | null; createdAt: string; updatedAt: string;
}
export interface RejectionView { chunkIndex: number; lineNumber: number; rawLine: string; reason: string }

const toJobView = (r: JobRow): JobView => ({
  id: r.id, status: r.status, s3Key: r.s3Key, totalChunks: r.totalChunks, completedChunks: r.completedChunks,
  failedChunks: r.failedChunks, rowsProcessed: r.rowsProcessed, rowsRejected: r.rowsRejected, error: r.error,
  createdAt: r.createdAt.toISOString(), updatedAt: r.updatedAt.toISOString(),
});

export class IngestionService {
  constructor(private readonly deps: AppDeps) {}

  async createJob(filename = 'vendor.csv'): Promise<{ jobId: string; uploadUrl: string; key: string; expiresInSeconds: number }> {
    const [job] = await this.deps.db.insert(ingestionJobs).values({ s3Key: 'pending' }).returning({ id: ingestionJobs.id });
    const key = `uploads/${job!.id}/${filename}`;
    await this.deps.db.update(ingestionJobs).set({ s3Key: key }).where(eq(ingestionJobs.id, job!.id));
    const uploadUrl = await getSignedUrl(
      this.deps.presigner,
      new PutObjectCommand({ Bucket: this.deps.config.s3Bucket, Key: key }), // no ContentType: a signed header would force every uploader to match it exactly
      { expiresIn: PRESIGN_TTL_SECONDS },
    );
    return { jobId: job!.id, uploadUrl, key, expiresInSeconds: PRESIGN_TTL_SECONDS };
  }

  async getJob(id: string): Promise<JobView | null> {
    const [row] = await this.deps.db.select().from(ingestionJobs).where(eq(ingestionJobs.id, id));
    return row ? toJobView(row) : null;
  }

  async listRejections(id: string, page: number, pageSize: number) {
    if (!(await this.getJob(id))) return null;
    const [items, totalRows] = await Promise.all([
      this.deps.db.select({
        chunkIndex: ingestionRejections.chunkIndex, lineNumber: ingestionRejections.lineNumber,
        rawLine: ingestionRejections.rawLine, reason: ingestionRejections.reason,
      }).from(ingestionRejections).where(eq(ingestionRejections.jobId, id))
        .orderBy(asc(ingestionRejections.chunkIndex), asc(ingestionRejections.lineNumber))
        .limit(pageSize).offset((page - 1) * pageSize),
      this.deps.db.select({ total: count() }).from(ingestionRejections).where(eq(ingestionRejections.jobId, id)),
    ]);
    return { items: items as RejectionView[], pagination: { page, pageSize, total: Number(totalRows[0]?.total ?? 0) } };
  }
}
