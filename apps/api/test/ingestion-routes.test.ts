import { HeadObjectCommand } from '@aws-sdk/client-s3';
import { ingestionJobs, ingestionRejections } from '@modaco/core';
import request from 'supertest';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { setupTestDeps, type TestContext } from './helpers';

let ctx: TestContext;
beforeAll(async () => { ctx = await setupTestDeps(); });
afterAll(() => ctx.close());
beforeEach(() => ctx.truncateAll());

describe('POST /ingestion/jobs', () => {
  it('creates a pending job and a presigned PUT url that works against localstack', async () => {
    const res = await request(ctx.app).post('/ingestion/jobs').send({ filename: 'vendor.csv' });
    expect(res.status).toBe(201);
    expect(res.body.key).toBe(`uploads/${res.body.jobId}/vendor.csv`);
    expect(res.body.uploadUrl).toContain('X-Amz-Signature');
    const put = await fetch(res.body.uploadUrl, { method: 'PUT', body: 'sku,name,category,vendor_price,stock\n' });
    expect(put.status).toBe(200);
    // The API has no S3 client of its own beyond the presigner; reuse it (it points at LocalStack here) to check the upload landed.
    const head = await ctx.deps.presigner.send(new HeadObjectCommand({ Bucket: ctx.deps.config.s3Bucket, Key: res.body.key }));
    expect(head.ContentLength).toBe(37);
    const job = await request(ctx.app).get(`/ingestion/jobs/${res.body.jobId}`);
    expect(job.body).toMatchObject({ id: res.body.jobId, status: 'pending', totalChunks: 0 });
  });
  it('defaults the filename and rejects bad names', async () => {
    const res = await request(ctx.app).post('/ingestion/jobs').send({});
    expect(res.body.key).toMatch(/^uploads\/[0-9a-f-]{36}\/vendor\.csv$/);
    expect((await request(ctx.app).post('/ingestion/jobs').send({ filename: '../x.csv' })).status).toBe(400);
  });
});

describe('GET /ingestion/jobs/:id/rejections', () => {
  it('pages through rejections', async () => {
    const [job] = await ctx.db.insert(ingestionJobs).values({ s3Key: 'uploads/x/y.csv' }).returning();
    await ctx.db.insert(ingestionRejections).values([1, 2, 3].map((n) => ({ jobId: job!.id, chunkIndex: 0, lineNumber: n, rawLine: `bad,${n}`, reason: 'validation' })));
    const res = await request(ctx.app).get(`/ingestion/jobs/${job!.id}/rejections?pageSize=2&page=2`);
    expect(res.body.items).toHaveLength(1);
    expect(res.body.items[0]).toMatchObject({ lineNumber: 3, rawLine: 'bad,3', reason: 'validation' });
    expect(res.body.pagination).toEqual({ page: 2, pageSize: 2, total: 3 });
    expect((await request(ctx.app).get('/ingestion/jobs/00000000-0000-0000-0000-000000000000/rejections')).status).toBe(404);
  });
});
