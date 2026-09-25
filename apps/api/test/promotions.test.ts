import { keys } from '@modaco/core';
import request from 'supertest';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { setupTestDeps, type TestContext } from './helpers';

let ctx: TestContext;
let acc: { id: number; slug: string };
let shoes: { id: number; slug: string };
let belt: { id: number };
let boot: { id: number };

const iso = (offsetMs: number) => new Date(Date.now() + offsetMs).toISOString();
const body = (over: Record<string, unknown> = {}) => ({
  name: 'Sale', discountType: 'percentage', value: '50', startsAt: iso(-60_000), endsAt: iso(3_600_000), target: { categoryId: acc.id }, ...over,
});

beforeAll(async () => { ctx = await setupTestDeps(); });
afterAll(() => ctx.close());
beforeEach(async () => {
  await ctx.truncateAll();
  acc = await ctx.insertCategory('Accessories');
  shoes = await ctx.insertCategory('Shoes');
  belt = await ctx.insertProduct({ categoryId: acc.id, sku: 'BELT', basePrice: '20.00' });
  boot = await ctx.insertProduct({ categoryId: shoes.id, sku: 'BOOT', basePrice: '100.00' });
});

describe('POST /promotions', () => {
  it('creates a category promotion and prices flip instantly for every product in it', async () => {
    await request(ctx.app).get(`/products/${belt.id}`);
    await request(ctx.app).get('/products?category=accessories');
    const res = await request(ctx.app).post('/promotions').send(body());
    expect(res.status).toBe(201);
    expect(res.body).toMatchObject({ name: 'Sale', discountType: 'percentage', value: '50.00', target: { categoryId: acc.id }, cancelledAt: null });
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('10.00');
    expect((await request(ctx.app).get('/products?category=accessories')).body.items[0].effectivePrice).toBe('10.00');
    expect((await request(ctx.app).get(`/products/${boot.id}`)).body.effectivePrice).toBe('100.00');
    expect(await ctx.redis.get(keys.categoryVersion(acc.id))).toBe('1');
    expect(await ctx.redis.get(keys.allVersion())).toBe('1');
  });

  it('creates a product promotion and bumps the product, its category, and all', async () => {
    const warm = await request(ctx.app).get('/products?category=accessories');
    expect(warm.body.items.find((i: { id: number }) => i.id === belt.id).effectivePrice).toBe('20.00');
    const res = await request(ctx.app).post('/promotions').send(body({ target: { productId: belt.id }, discountType: 'fixed', value: '5.00' }));
    expect(res.status).toBe(201);
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('15.00');
    expect(await ctx.redis.get(keys.productVersion(belt.id))).toBe('1');
    expect(await ctx.redis.get(keys.categoryVersion(acc.id))).toBe('1');
    expect(await ctx.redis.get(keys.allVersion())).toBe('1');
    const list = await request(ctx.app).get('/products?category=accessories');
    expect(list.body.items.find((i: { id: number }) => i.id === belt.id).effectivePrice).toBe('15.00');
  });

  it('most recent promotion wins', async () => {
    await request(ctx.app).post('/promotions').send(body({ target: { productId: belt.id }, value: '10' }));
    await request(ctx.app).post('/promotions').send(body({ value: '50' }));
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('10.00');
  });

  it('validates semantics', async () => {
    expect((await request(ctx.app).post('/promotions').send(body({ startsAt: iso(10), endsAt: iso(0) }))).status).toBe(422);
    expect((await request(ctx.app).post('/promotions').send(body({ value: '150' }))).status).toBe(422);
    expect((await request(ctx.app).post('/promotions').send(body({ target: { categoryId: 9999 } }))).status).toBe(404);
    expect((await request(ctx.app).post('/promotions').send(body({ target: {} }))).status).toBe(400);
    expect((await request(ctx.app).post('/promotions').send(body({ value: 'abc' }))).status).toBe(400);
    expect((await request(ctx.app).post('/promotions').send(body({ value: '12345678901' }))).status).toBe(400); // 11 integer digits
  });
});

describe('POST /promotions/:id/cancel', () => {
  it('cancels idempotently and restores prices', async () => {
    const { body: promo } = await request(ctx.app).post('/promotions').send(body());
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('10.00');
    const first = await request(ctx.app).post(`/promotions/${promo.id}/cancel`);
    expect(first.status).toBe(200);
    expect(first.body.cancelledAt).not.toBeNull();
    const second = await request(ctx.app).post(`/promotions/${promo.id}/cancel`);
    expect(second.body.cancelledAt).toBe(first.body.cancelledAt);
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('20.00');
    expect((await request(ctx.app).post('/promotions/00000000-0000-0000-0000-000000000000/cancel')).status).toBe(404);
  });

  it('a second cancel does not bump the category or all versions again', async () => {
    const { body: promo } = await request(ctx.app).post('/promotions').send(body());
    const afterCreate = { cat: await ctx.redis.get(keys.categoryVersion(acc.id)), all: await ctx.redis.get(keys.allVersion()) };
    await request(ctx.app).post(`/promotions/${promo.id}/cancel`);
    const afterFirst = { cat: await ctx.redis.get(keys.categoryVersion(acc.id)), all: await ctx.redis.get(keys.allVersion()) };
    expect(afterFirst).not.toEqual(afterCreate);
    await request(ctx.app).post(`/promotions/${promo.id}/cancel`);
    const afterSecond = { cat: await ctx.redis.get(keys.categoryVersion(acc.id)), all: await ctx.redis.get(keys.allVersion()) };
    expect(afterSecond).toEqual(afterFirst);
  });

  it('two concurrent cancels bump exactly once', async () => {
    const { body: promo } = await request(ctx.app).post('/promotions').send(body());
    const before = Number(await ctx.redis.get(keys.categoryVersion(acc.id)));
    const [r1, r2] = await Promise.all([
      request(ctx.app).post(`/promotions/${promo.id}/cancel`),
      request(ctx.app).post(`/promotions/${promo.id}/cancel`),
    ]);
    expect(r1.status).toBe(200);
    expect(r2.status).toBe(200);
    expect(r1.body.cancelledAt).toBe(r2.body.cancelledAt);
    const after = Number(await ctx.redis.get(keys.categoryVersion(acc.id)));
    expect(after).toBe(before + 1);
  });
});

describe('PUT /promotions/:id/target', () => {
  it('moves a promotion and bumps both old and new targets', async () => {
    const { body: promo } = await request(ctx.app).post('/promotions').send(body());
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('10.00');
    const res = await request(ctx.app).put(`/promotions/${promo.id}/target`).send({ categoryId: shoes.id });
    expect(res.status).toBe(200);
    expect(res.body.target).toEqual({ categoryId: shoes.id });
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('20.00');
    expect((await request(ctx.app).get(`/products/${boot.id}`)).body.effectivePrice).toBe('50.00');
    const toProduct = await request(ctx.app).put(`/promotions/${promo.id}/target`).send({ productId: belt.id });
    expect(toProduct.body.target).toEqual({ productId: belt.id });
    expect((await request(ctx.app).get(`/products/${boot.id}`)).body.effectivePrice).toBe('100.00');
    expect((await request(ctx.app).get(`/products/${belt.id}`)).body.effectivePrice).toBe('10.00');
  });

  it('404s for an unknown promotion and an unknown target, 400s for an invalid body', async () => {
    const { body: promo } = await request(ctx.app).post('/promotions').send(body());
    expect((await request(ctx.app).put('/promotions/00000000-0000-0000-0000-000000000000/target').send({ categoryId: shoes.id })).status).toBe(404);
    expect((await request(ctx.app).put(`/promotions/${promo.id}/target`).send({ categoryId: 9999 })).status).toBe(404);
    expect((await request(ctx.app).put(`/promotions/${promo.id}/target`).send({})).status).toBe(400);
  });
});

describe('GET /promotions/:id', () => {
  it('returns the promotion or 404', async () => {
    const { body: promo } = await request(ctx.app).post('/promotions').send(body());
    expect((await request(ctx.app).get(`/promotions/${promo.id}`)).body.id).toBe(promo.id);
    expect((await request(ctx.app).get('/promotions/not-a-uuid')).status).toBe(400);
    expect((await request(ctx.app).get('/promotions/00000000-0000-0000-0000-000000000000')).status).toBe(404);
  });
});
