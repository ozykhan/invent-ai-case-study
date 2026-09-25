import { sql } from 'drizzle-orm';
import type { Express } from 'express';
import { categories, products, runMigrations, slugify, type Db, type Redis } from '@modaco/core';
import { createApp } from '../src/app';
import { loadConfig } from '../src/config';
import { createDeps, type AppDeps } from '../src/deps';

export interface TestContext {
  deps: AppDeps;
  app: Express;
  db: Db;
  redis: Redis;
  truncateAll(): Promise<void>;
  insertCategory(name: string, over?: Partial<typeof categories.$inferInsert>): Promise<{ id: number; slug: string }>;
  insertProduct(over: Partial<typeof products.$inferInsert> & { categoryId: number }): Promise<{ id: number }>;
  close(): Promise<void>;
}

export async function setupTestDeps(): Promise<TestContext> {
  const config = loadConfig({ ...process.env, LOG_LEVEL: 'silent' });
  const real = await createDeps(config);
  const deps: AppDeps = { ...real };
  await runMigrations(deps.db);
  const redis = deps.redis!;
  let counter = 0;
  return {
    deps,
    app: createApp(deps),
    db: deps.db,
    redis,
    truncateAll: async () => {
      await deps.db.execute(sql`truncate ingestion_rejections, ingestion_chunks, ingestion_jobs, promotions, products, categories restart identity cascade`);
      await redis.flushdb();
    },
    insertCategory: async (name, over = {}) => {
      const [row] = await deps.db.insert(categories).values({ name, slug: slugify(name), ...over }).returning();
      return { id: row!.id, slug: row!.slug };
    },
    insertProduct: async (over) => {
      counter++;
      const [row] = await deps.db.insert(products).values({ sku: `SKU-${counter}`, name: `Product ${counter}`, basePrice: '10.00', stock: 5, ...over }).returning();
      return { id: row!.id };
    },
    close: () => real.close(),
  };
}
