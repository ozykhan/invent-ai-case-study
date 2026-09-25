import { createRedis, keys, STOCK_TTL_SECONDS } from '@modaco/core';
import { promotions } from '@modaco/core';
import type { Express } from 'express';
import request from 'supertest';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { createApp } from '../src/app';
import { loadStocks } from '../src/products/stock';
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

describe('POST /products', () => {
  it('creates a product that immediately inherits an active category promotion', async () => {
    await activePromo();
    await request(ctx.app).get('/products?category=accessories'); // warm the list cache
    const res = await request(ctx.app).post('/products').send({ sku: 'RING', name: 'Ring', categoryId: acc.id, basePrice: '30.00', stock: 2 });
    expect(res.status).toBe(201);
    expect(res.body).toMatchObject({ sku: 'RING', effectivePrice: '15.00', stock: 2, activePromotion: { name: 'Sale' } });
    const list = await request(ctx.app).get('/products?category=accessories');
    expect(list.body.pagination.total).toBe(3);
    expect(list.body.items.find((i: { sku: string }) => i.sku === 'RING').effectivePrice).toBe('15.00');
  });
  it('rejects a duplicate sku, unknown category, and bad body', async () => {
    expect((await request(ctx.app).post('/products').send({ sku: 'BELT', name: 'x', categoryId: acc.id, basePrice: '1.00' })).status).toBe(409);
    expect((await request(ctx.app).post('/products').send({ sku: 'NEW', name: 'x', categoryId: 999, basePrice: '1.00' })).status).toBe(404);
    expect((await request(ctx.app).post('/products').send({ sku: 'NEW', name: 'x', categoryId: acc.id, basePrice: '1.999' })).status).toBe(400);
  });
});

describe('PATCH /products/:id/stock', () => {
  it('applies a delta and an absolute value, visible immediately', async () => {
    await request(ctx.app).get(`/products/${belt.id}`);
    let res = await request(ctx.app).patch(`/products/${belt.id}/stock`).send({ delta: -2 });
    expect(res.body).toEqual({ id: belt.id, stock: 1 });
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.stock).toBe(1);
    res = await request(ctx.app).patch(`/products/${belt.id}/stock`).send({ stock: 10 });
    expect(res.body).toEqual({ id: belt.id, stock: 10 });
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.stock).toBe(10);
  });
  it('refuses to go negative and 404s on unknown ids', async () => {
    expect((await request(ctx.app).patch(`/products/${hat.id}/stock`).send({ delta: -1 })).status).toBe(422);
    expect((await request(ctx.app).patch('/products/999999/stock').send({ delta: 1 })).status).toBe(404);
  });
});

describe('stock counters', () => {
  it('expire on the same 300 s bound as the cache versions', async () => {
    expect(STOCK_TTL_SECONDS).toBe(300);
    await request(ctx.app).get(`/products/${belt.id}`); // backfill after a miss
    let ttl = await ctx.redis.ttl(keys.stock(belt.id));
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(300);
    await request(ctx.app).patch(`/products/${belt.id}/stock`).send({ delta: 1 }); // write-through
    ttl = await ctx.redis.ttl(keys.stock(belt.id));
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(300);
  });

  it('a backfill after a miss never overwrites a counter a concurrent write set in the meantime', async () => {
    // The MGET misses; before the backfill lands, a PATCH commits and writes the newer value 99.
    const mget = vi.spyOn(ctx.redis, 'mget').mockImplementationOnce((async () => {
      await ctx.redis.set(keys.stock(belt.id), '99');
      return [null];
    }) as never);
    const stocks = await loadStocks(ctx.deps, [belt.id]);
    mget.mockRestore();
    expect(stocks.get(belt.id)).toBe(3); // this read serves what it saw in postgres...
    expect(await ctx.redis.get(keys.stock(belt.id))).toBe('99'); // ...but must not clobber the newer counter
  });

  it('drops the counter when a write-through SET fails, so the next read falls back to postgres', async () => {
    await request(ctx.app).get(`/products/${belt.id}`); // stock:{id} = 3
    const set = vi.spyOn(ctx.redis, 'set').mockRejectedValueOnce(new Error('transient'));
    const res = await request(ctx.app).patch(`/products/${belt.id}/stock`).send({ delta: -2 });
    set.mockRestore();
    expect(res.body).toEqual({ id: belt.id, stock: 1 });
    expect(await ctx.redis.get(keys.stock(belt.id))).toBeNull();
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.stock).toBe(1);
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
