import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { setupTestDeps, type TestContext } from './helpers';

let ctx: TestContext;
beforeAll(async () => { ctx = await setupTestDeps(); });
afterAll(() => ctx.close());

describe('GET /health', () => {
  it('reports postgres and redis', async () => {
    const res = await request(ctx.app).get('/health');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ status: 'ok', checks: { postgres: true, redis: true } });
    expect(res.headers['x-request-id']).toBeTruthy();
  });
  it('returns the error envelope for unknown routes', async () => {
    const res = await request(ctx.app).get('/nope');
    expect(res.status).toBe(404);
    expect(res.body.error.code).toBe('not_found');
  });
});
