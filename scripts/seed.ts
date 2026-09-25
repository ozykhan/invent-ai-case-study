import { categories, createDb, products, runMigrations, slugify, TEST_DATABASE_URL } from '@modaco/core';

const { db, close } = createDb(process.env.DATABASE_URL ?? TEST_DATABASE_URL, { max: 2 });
await runMigrations(db);

const names = ['Accessories', 'Shoes', 'Bags', 'Outerwear'];
await db.insert(categories).values(names.map((name) => ({ name, slug: slugify(name) }))).onConflictDoNothing();
const cats = await db.select().from(categories);

const rows = cats.flatMap((c) => Array.from({ length: 50 }, (_, i) => ({
  sku: `${c.slug.toUpperCase()}-${String(i + 1).padStart(3, '0')}`,
  name: `${c.name} item ${i + 1}`,
  categoryId: c.id,
  basePrice: (9.99 + i * 3).toFixed(2),
  stock: (i * 7) % 40,
})));
await db.insert(products).values(rows).onConflictDoNothing();
console.log(`seeded ${cats.length} categories, ${rows.length} products`);
await close();
