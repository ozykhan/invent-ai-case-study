import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createDb, TEST_DATABASE_URL } from '../db/client';
import { runMigrations } from '../db/migrate';
import { categories, products, promotions } from '../db/schema';
import { fetchProductById, fetchProductPage, fetchStock, nextPromotionBoundary } from './queries';

const { db, close } = createDb(process.env.DATABASE_URL ?? TEST_DATABASE_URL, { max: 2 });
const now = new Date('2026-06-15T12:00:00Z');
const day = (d: number) => new Date(now.getTime() + d * 86_400_000);

let accessoriesId: number;
let shoesId: number;
let beltId: number;
let hatId: number;
let bootId: number;

beforeAll(() => runMigrations(db));
afterAll(() => close());

beforeEach(async () => {
  await db.execute(sql`truncate promotions, products, categories restart identity cascade`);
  const [acc, shoes] = await db.insert(categories).values([
    { name: 'Accessories', slug: 'accessories' }, { name: 'Shoes', slug: 'shoes' },
  ]).returning();
  accessoriesId = acc!.id; shoesId = shoes!.id;
  const rows = await db.insert(products).values([
    { sku: 'BELT', name: 'Belt', categoryId: accessoriesId, basePrice: '20.00', stock: 3 },
    { sku: 'HAT', name: 'Hat', categoryId: accessoriesId, basePrice: '10.00', stock: 0 },
    { sku: 'BOOT', name: 'Boot', categoryId: shoesId, basePrice: '100.00', stock: 7 },
  ]).returning();
  beltId = rows[0]!.id; hatId = rows[1]!.id; bootId = rows[2]!.id;
});

const promo = (over: Partial<typeof promotions.$inferInsert>) => db.insert(promotions).values({
  name: 'p', discountType: 'percentage', value: '50', scope: 'category', categoryId: accessoriesId,
  startsAt: day(-1), endsAt: day(1), ...over,
}).returning();

describe('fetchProductById', () => {
  it('returns base price when no promotion is active', async () => {
    const p = await fetchProductById(db, beltId, now);
    expect(p).toMatchObject({ sku: 'BELT', basePrice: '20.00', effectivePrice: '20.00', activePromotion: null, category: { slug: 'accessories' } });
    expect(p).not.toHaveProperty('stock');
  });

  it('applies a category percentage promotion', async () => {
    await promo({});
    expect((await fetchProductById(db, beltId, now))!.effectivePrice).toBe('10.00');
    expect((await fetchProductById(db, bootId, now))!.effectivePrice).toBe('100.00');
  });

  it('applies a fixed promotion floored at zero', async () => {
    await promo({ discountType: 'fixed', value: '15.00' });
    expect((await fetchProductById(db, beltId, now))!.effectivePrice).toBe('5.00');
    expect((await fetchProductById(db, hatId, now))!.effectivePrice).toBe('0.00');
  });

  it('ignores cancelled and out-of-window promotions', async () => {
    await promo({ cancelledAt: now });
    await promo({ startsAt: day(1), endsAt: day(2) });
    await promo({ startsAt: day(-3), endsAt: day(-2) });
    expect((await fetchProductById(db, beltId, now))!.effectivePrice).toBe('20.00');
  });

  it('most recently created promotion wins across scopes', async () => {
    await promo({ scope: 'product', categoryId: null, productId: beltId, value: '10', createdAt: day(-2) });
    const [cat] = await promo({ value: '50', createdAt: day(-1) });
    const p = await fetchProductById(db, beltId, now);
    expect(p!.effectivePrice).toBe('10.00');
    expect(p!.activePromotion!.id).toBe(cat!.id);
  });

  it('returns null for an unknown id', async () => {
    expect(await fetchProductById(db, 999999, now)).toBeNull();
  });
});

describe('fetchProductPage', () => {
  it('filters by category and sorts by effective price', async () => {
    await promo({ scope: 'product', categoryId: null, productId: beltId, value: '90' }); // belt -> 2.00
    const asc = await fetchProductPage(db, { categoryId: accessoriesId, sort: 'asc', limit: 10, offset: 0, now });
    expect(asc.total).toBe(2);
    expect(asc.items.map((i) => i.sku)).toEqual(['BELT', 'HAT']);
    const desc = await fetchProductPage(db, { categoryId: accessoriesId, sort: 'desc', limit: 10, offset: 0, now });
    expect(desc.items.map((i) => i.sku)).toEqual(['HAT', 'BELT']);
  });

  it('paginates across all categories', async () => {
    const page2 = await fetchProductPage(db, { categoryId: null, sort: 'asc', limit: 2, offset: 2, now });
    expect(page2.total).toBe(3);
    expect(page2.items.map((i) => i.sku)).toEqual(['BOOT']);
  });
});

describe('fetchStock', () => {
  it('returns stock for the given ids', async () => {
    const m = await fetchStock(db, [beltId, bootId, 424242]);
    expect(m.get(beltId)).toBe(3);
    expect(m.get(bootId)).toBe(7);
    expect(m.has(424242)).toBe(false);
    expect((await fetchStock(db, [])).size).toBe(0);
  });
});

describe('nextPromotionBoundary', () => {
  it('returns the earliest upcoming start or end', async () => {
    await promo({ startsAt: day(-1), endsAt: day(3) });            // ends in 3 days
    await promo({ startsAt: day(2), endsAt: day(5) });             // starts in 2 days
    await promo({ scope: 'product', categoryId: null, productId: bootId, startsAt: day(1), endsAt: day(9) }); // other category, product-scoped
    expect(await nextPromotionBoundary(db, { productId: beltId, categoryId: accessoriesId }, now)).toEqual(day(2));
    expect(await nextPromotionBoundary(db, { categoryId: accessoriesId }, now)).toEqual(day(2));
    expect(await nextPromotionBoundary(db, { categoryId: shoesId }, now)).toEqual(day(1));
    expect(await nextPromotionBoundary(db, { categoryId: null }, now)).toEqual(day(1));
  });
  it('returns null when nothing is scheduled', async () => {
    expect(await nextPromotionBoundary(db, { categoryId: accessoriesId }, now)).toBeNull();
  });
  it('ignores cancelled promotions', async () => {
    await promo({ startsAt: day(2), endsAt: day(5), cancelledAt: now });
    expect(await nextPromotionBoundary(db, { categoryId: accessoriesId }, now)).toBeNull();
  });
});
