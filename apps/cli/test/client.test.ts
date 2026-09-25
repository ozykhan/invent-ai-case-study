import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { performance } from 'node:perf_hooks';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ApiClient } from '../src/client';
import { ApiError } from '../src/errors';
import { formatFields, formatTable } from '../src/output';
import { startStubApi, type StubApi } from './stub-api';

let stub: StubApi;
let client: ApiClient;
beforeEach(async () => {
  stub = await startStubApi();
  client = new ApiClient({ baseUrl: `${stub.url}/`, timeoutMs: 5000 });
});
afterEach(async () => {
  await client.close();
  await stub.close();
});

describe('ApiClient', () => {
  it('calls endpoints and exposes the answering instance', async () => {
    const health = await client.health();
    expect(health.body.status).toBe('ok');
    expect(health.instance).toMatch(/^i\d$/);
    const page = await client.listProducts({ category: 'shoes', page: 2, pageSize: 5 });
    expect(page.items.map((p) => p.id)).toEqual([6, 7, 8, 9, 10]);
    expect(page.pagination.total).toBe(1000);
  });

  it('throws ApiError with the envelope on 4xx', async () => {
    const err = await client.getProduct(404).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect(err).toMatchObject({ status: 404, code: 'not_found', message: 'product 404 not found' });
    expect((err as ApiError).toJSON()).toEqual({ status: 404, error: { code: 'not_found', message: 'product 404 not found' } });
  });

  it('send() never throws on status and only parses bodies on request', async () => {
    const drained = await client.send({ label: 'detail', method: 'GET', path: '/products/404' });
    expect(drained).toEqual({ status: 404, instance: expect.stringMatching(/^i\d$/) });
    const parsed = await client.send({ label: 'promo:create', method: 'POST', path: '/promotions', body: { name: 'x' }, onResponse: () => {} });
    expect(parsed.status).toBe(201);
    expect(parsed.body).toMatchObject({ name: 'x', id: expect.any(String) });
  });

  it('health() accepts a 503 instead of throwing', async () => {
    const degraded = await startStubApi({ degraded: true });
    const c = new ApiClient({ baseUrl: degraded.url, timeoutMs: 5000 });
    try {
      const r = await c.health();
      expect(r.status).toBe(503);
      expect(r.body.checks.postgres).toBe(false);
    } finally {
      await c.close();
      await degraded.close();
    }
  });
});

describe('ApiClient.close', () => {
  it('with force, returns at once instead of waiting for requests still in flight', async () => {
    const hanging = createServer(() => { /* never answers */ });
    await new Promise<void>((resolve) => hanging.listen(0, '127.0.0.1', resolve));
    const c = new ApiClient({ baseUrl: `http://127.0.0.1:${(hanging.address() as AddressInfo).port}`, timeoutMs: 10_000 });
    try {
      const pending = c.health().catch((e: unknown) => e);
      await new Promise((resolve) => setTimeout(resolve, 50));
      const t0 = performance.now();
      await c.close({ force: true });
      expect(performance.now() - t0).toBeLessThan(500);
      expect(await pending).toBeInstanceOf(Error);
    } finally {
      hanging.closeAllConnections();
      await new Promise<void>((resolve) => hanging.close(() => resolve()));
    }
  });
});

describe('output', () => {
  it('aligns table columns and renders empty fields as -', () => {
    expect(formatTable([['id', 'name'], [1, 'a'], [100, 'bb']])).toBe('id   name\n1    a\n100  bb');
    expect(formatFields([['a', null], ['long', { x: 1 }]])).toBe('a     -\nlong  {"x":1}');
  });
});
