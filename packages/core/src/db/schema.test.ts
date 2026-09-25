import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createDb, TEST_DATABASE_URL } from './client';
import { runMigrations } from './migrate';
import { categories, products, promotions } from './schema';

const { db, close } = createDb(process.env.DATABASE_URL ?? TEST_DATABASE_URL, { max: 2 });

beforeAll(async () => {
  await runMigrations(db);
  await db.execute(sql`truncate promotions, products, categories restart identity cascade`);
});
afterAll(() => close());

// drizzle-orm wraps driver errors as `Failed query: ...` and puts the underlying
// Postgres message (which carries the constraint name) on `error.cause`.
async function causeMessage(promise: Promise<unknown>): Promise<string> {
  try {
    await promise;
  } catch (e) {
    const err = e as { cause?: { message?: string }; message?: string };
    return err.cause?.message ?? err.message ?? String(e);
  }
  throw new Error('expected promise to reject');
}

describe('schema', () => {
  it('creates all tables', async () => {
    const res = await db.execute(sql`select table_name from information_schema.tables where table_schema = 'public' order by table_name`);
    const names = res.rows.map((r) => r.table_name);
    expect(names).toEqual(expect.arrayContaining([
      'categories', 'products', 'promotions', 'ingestion_jobs', 'ingestion_chunks', 'ingestion_rejections',
    ]));
  });

  it('rejects a promotion whose scope does not match its target', async () => {
    const [cat] = await db.insert(categories).values({ name: 'Accessories', slug: 'accessories' }).returning();
    await expect(causeMessage(db.insert(promotions).values({
      name: 'bad', discountType: 'percentage', value: '10', scope: 'product',
      categoryId: cat!.id, startsAt: new Date('2026-01-01'), endsAt: new Date('2026-02-01'),
    }))).resolves.toMatch(/promotions_scope_target/);
  });

  it('rejects a percentage over 100 and an inverted window', async () => {
    const [cat] = await db.select().from(categories).limit(1);
    await expect(causeMessage(db.insert(promotions).values({
      name: 'bad', discountType: 'percentage', value: '150', scope: 'category',
      categoryId: cat!.id, startsAt: new Date('2026-01-01'), endsAt: new Date('2026-02-01'),
    }))).resolves.toMatch(/promotions_value/);
    await expect(causeMessage(db.insert(promotions).values({
      name: 'bad', discountType: 'fixed', value: '5', scope: 'category',
      categoryId: cat!.id, startsAt: new Date('2026-02-01'), endsAt: new Date('2026-01-01'),
    }))).resolves.toMatch(/promotions_window/);
  });

  it('rejects negative stock and duplicate sku', async () => {
    const [cat] = await db.select().from(categories).limit(1);
    await expect(causeMessage(db.insert(products).values({ sku: 'A', name: 'a', categoryId: cat!.id, basePrice: '1.00', stock: -1 })))
      .resolves.toMatch(/products_stock_nonnegative/);
    await db.insert(products).values({ sku: 'A', name: 'a', categoryId: cat!.id, basePrice: '1.00', stock: 1 });
    await expect(causeMessage(db.insert(products).values({ sku: 'A', name: 'b', categoryId: cat!.id, basePrice: '1.00', stock: 1 })))
      .resolves.toMatch(/duplicate key/);
  });
});
