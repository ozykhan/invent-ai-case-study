import { createRedis, keys } from '@modaco/core';
import { promotions } from '@modaco/core';
import type { Express } from 'express';
import request from 'supertest';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { createApp } from '../src/app';
import { setupTestDeps, type TestContext } from './helpers';

let ctx: TestContext;
let acc: { id: number; slug: string };
let shoes: { id: number; slug: string };
let belt: { id: number };
let hat: { id: number };

beforeAll(async () => { ctx = await setupTestDeps(); });
afterAll(() => ctx.close());
beforeEach(async () => {
  await ctx.truncateAll();
  acc = await ctx.insertCategory('Accessories');
  shoes = await ctx.insertCategory('Shoes');
  belt = await ctx.insertProduct({ categoryId: acc.id, sku: 'BELT', name: 'Belt', basePrice: '20.00', stock: 3 });
  hat = await ctx.insertProduct({ categoryId: acc.id, sku: 'HAT', name: 'Hat', basePrice: '10.00', stock: 0 });
  await ctx.insertProduct({ categoryId: shoes.id, sku: 'BOOT', name: 'Boot', basePrice: '100.00', stock: 7 });
});

const activePromo = (over: Partial<typeof promotions.$inferInsert> = {}) => ctx.db.insert(promotions).values({
  name: 'Sale', discountType: 'percentage', value: '50', scope: 'category', categoryId: acc.id,
  startsAt: new Date(Date.now() - 60_000), endsAt: new Date(Date.now() + 3_600_000), ...over,
}).returning();

describe('GET /products/:id', () => {
  it('returns the item shape with effective price and stock', async () => {
    const res = await request(ctx.app).get(`/products/${belt.id}`);
    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      id: belt.id, sku: 'BELT', name: 'Belt', category: { id: acc.id, name: 'Accessories', slug: 'accessories' },
      basePrice: '20.00', effectivePrice: '20.00', activePromotion: null, stock: 3,
    });
  });

  it('404s for an unknown id and 400s for a bad id', async () => {
    expect((await request(ctx.app).get('/products/999999')).status).toBe(404);
    expect((await request(ctx.app).get('/products/abc')).status).toBe(400);
  });

  it('serves from cache and invalidates when the category version is bumped', async () => {
    await request(ctx.app).get(`/products/${belt.id}`);
    expect(await ctx.redis.exists(keys.product(belt.id))).toBe(1);
    await activePromo();
    // no bump yet: cached price still 20.00
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('20.00');
    await ctx.redis.incr(keys.categoryVersion(acc.id));
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('10.00');
  });

  it('reads stock live while the catalog entry stays cached', async () => {
    await request(ctx.app).get(`/products/${belt.id}`);
    await ctx.redis.set(keys.stock(belt.id), '42');
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.stock).toBe(42);
  });

  it('caps the TTL at the next promotion boundary', async () => {
    await activePromo({ startsAt: new Date(Date.now() + 30_000), endsAt: new Date(Date.now() + 60_000) });
    await request(ctx.app).get(`/products/${belt.id}`);
    const ttl = await ctx.redis.ttl(keys.product(belt.id));
    expect(ttl).toBeGreaterThan(20);
    expect(ttl).toBeLessThanOrEqual(31);
  });

  it('still answers when redis is down', async () => {
    const dead = { ...ctx.deps, redis: null };
    const { createApp } = await import('../src/app');
    const res = await request(createApp(dead)).get(`/products/${belt.id}`);
    expect(res.status).toBe(200);
    expect(res.body.stock).toBe(3);
  });

  it('warm reads take exactly two redis round trips: entry+productVersion, then categoryVersion+stock', async () => {
    await request(ctx.app).get(`/products/${belt.id}`); // cold: builds and caches the entry, backfills stock:{id}
    const mgetSpy = vi.spyOn(ctx.redis, 'mget');
    const getSpy = vi.spyOn(ctx.redis, 'get');
    const res = await request(ctx.app).get(`/products/${belt.id}`);
    expect(res.status).toBe(200);
    expect(res.body.stock).toBe(3);
    expect(getSpy).not.toHaveBeenCalled();
    expect(mgetSpy).toHaveBeenCalledTimes(2);
    mgetSpy.mockRestore();
    getSpy.mockRestore();
  });
});

describe('GET /products', () => {
  it('filters, sorts, and paginates', async () => {
    await activePromo({ scope: 'product', categoryId: null, productId: belt.id, value: '90' }); // belt -> 2.00
    const asc = await request(ctx.app).get(`/products?category=accessories&sort=effective_price`);
    expect(asc.status).toBe(200);
    expect(asc.body.items.map((i: { sku: string }) => i.sku)).toEqual(['BELT', 'HAT']);
    expect(asc.body.pagination).toEqual({ page: 1, pageSize: 20, total: 2 });
    const desc = await request(ctx.app).get(`/products?category=accessories&sort=-effective_price&pageSize=1&page=2`);
    expect(desc.body.items.map((i: { sku: string }) => i.sku)).toEqual(['BELT']);
    expect(desc.body.pagination).toEqual({ page: 2, pageSize: 1, total: 2 });
    const all = await request(ctx.app).get('/products');
    expect(all.body.pagination.total).toBe(3);
    expect(all.body.items[0]).toHaveProperty('stock');
  });

  it('validates query params', async () => {
    expect((await request(ctx.app).get('/products?pageSize=500')).status).toBe(400);
    expect((await request(ctx.app).get('/products?sort=name')).status).toBe(400);
    expect((await request(ctx.app).get('/products?category=nope')).status).toBe(404);
  });

  it('caches a page and rebuilds it after a version bump', async () => {
    await request(ctx.app).get('/products?category=accessories');
    expect(await ctx.redis.exists(keys.list(acc.id, 'asc', 1, 20))).toBe(1);
    await activePromo();
    expect((await request(ctx.app).get('/products?category=accessories')).body.items[0].effectivePrice).toBe('10.00'); // HAT cached at 10.00
    await ctx.redis.incr(keys.categoryVersion(acc.id));
    const after = await request(ctx.app).get('/products?category=accessories');
    expect(after.body.items.map((i: { effectivePrice: string }) => i.effectivePrice)).toEqual(['5.00', '10.00']);
  });

  it('warm reads take exactly two redis round trips: entry+version, then stock', async () => {
    await request(ctx.app).get('/products'); // cold: builds and caches the page, backfills stock counters
    const mgetSpy = vi.spyOn(ctx.redis, 'mget');
    const getSpy = vi.spyOn(ctx.redis, 'get');
    const res = await request(ctx.app).get('/products');
    expect(res.status).toBe(200);
    expect(res.body.pagination.total).toBe(3);
    expect(getSpy).not.toHaveBeenCalled();
    expect(mgetSpy).toHaveBeenCalledTimes(2);
    mgetSpy.mockRestore();
    getSpy.mockRestore();
  });
});

describe('when redis is unreachable', () => {
  let dead: ReturnType<typeof createRedis>;
  let deadApp: Express;

  beforeAll(() => {
    dead = createRedis('redis://localhost:1');
    dead.on('error', () => {}); // ioredis emits 'error' events; unhandled ones would throw
    deadApp = createApp({ ...ctx.deps, redis: dead });
  });
  afterAll(() => dead.disconnect());

  it('GET /products/:id still returns 200 with live price and stock from postgres', async () => {
    const res = await request(deadApp).get(`/products/${belt.id}`);
    expect(res.status).toBe(200);
    expect(res.body.effectivePrice).toBe('20.00');
    expect(res.body.stock).toBe(3);
  });

  it('GET /products still returns 200 with the full unfiltered page from postgres', async () => {
    const res = await request(deadApp).get('/products');
    expect(res.status).toBe(200);
    expect(res.body.pagination.total).toBe(3);
    expect(res.body.items.every((i: { stock: number }) => typeof i.stock === 'number')).toBe(true);
  });

  it('GET /products?category=... still returns 200 with the filtered, priced, stocked page from postgres', async () => {
    const res = await request(deadApp).get('/products?category=accessories');
    expect(res.status).toBe(200);
    expect(res.body.items.map((i: { sku: string }) => i.sku)).toEqual(['HAT', 'BELT']);
    expect(res.body.items.map((i: { stock: number }) => i.stock)).toEqual([0, 3]);
  });
});
