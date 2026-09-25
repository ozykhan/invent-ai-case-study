import { eq } from 'drizzle-orm';
import {
  categories, fetchProductById, fetchProductPage, getVersions, keys, nextPromotionBoundary, readThrough,
  SLUG_TTL_SECONDS, ttlSeconds, type ProductRecord, type SortDir,
} from '@modaco/core';
import type { AppDeps } from '../deps';

interface ProductEntry { productVersion: number; categoryVersion: number; categoryId: number; record: ProductRecord }
interface PageEntry { version: number; items: ProductRecord[]; total: number }

const onError = (deps: AppDeps) => (err: unknown) => deps.logger.warn({ err }, 'cache degraded; serving from postgres');

export async function resolveCategoryId(deps: AppDeps, slug: string): Promise<number | null> {
  const { value } = await readThrough<number | null>(deps.redis, keys.categorySlug(slug), async () => {
    const [row] = await deps.db.select({ id: categories.id }).from(categories).where(eq(categories.slug, slug));
    return { value: row?.id ?? null, ttlSeconds: row ? SLUG_TTL_SECONDS : 5 };
  }, { onError: onError(deps) });
  return value;
}

export async function getCachedProduct(deps: AppDeps, id: number): Promise<ProductRecord | null> {
  const redis = deps.redis;
  const { value } = await readThrough<ProductEntry | null>(redis, keys.product(id), async () => {
    const now = deps.now();
    const [productVersion] = redis ? await getVersions(redis, [keys.productVersion(id)]) : [0];
    const record = await fetchProductById(deps.db, id, now);
    if (!record) return { value: null, ttlSeconds: 5 };
    const [categoryVersion] = redis ? await getVersions(redis, [keys.categoryVersion(record.category.id)]) : [0];
    const boundary = await nextPromotionBoundary(deps.db, { productId: id, categoryId: record.category.id }, now);
    return {
      value: { productVersion: productVersion ?? 0, categoryVersion: categoryVersion ?? 0, categoryId: record.category.id, record },
      ttlSeconds: ttlSeconds(now, boundary),
    };
  }, {
    onError: onError(deps),
    isFresh: async (entry) => {
      if (!entry || !redis) return true;
      const [pv, cv] = await getVersions(redis, [keys.productVersion(id), keys.categoryVersion(entry.categoryId)]);
      return entry.productVersion === pv && entry.categoryVersion === cv;
    },
  });
  return value?.record ?? null;
}

export async function getCachedProductPage(
  deps: AppDeps,
  opts: { categoryId: number | null; sort: SortDir; page: number; pageSize: number },
): Promise<{ items: ProductRecord[]; total: number }> {
  const redis = deps.redis;
  const versionKey = opts.categoryId === null ? keys.allVersion() : keys.categoryVersion(opts.categoryId);
  const { value } = await readThrough<PageEntry>(redis, keys.list(opts.categoryId, opts.sort, opts.page, opts.pageSize), async () => {
    const now = deps.now();
    const [version] = redis ? await getVersions(redis, [versionKey]) : [0];
    const page = await fetchProductPage(deps.db, {
      categoryId: opts.categoryId, sort: opts.sort, limit: opts.pageSize, offset: (opts.page - 1) * opts.pageSize, now,
    });
    const boundary = await nextPromotionBoundary(deps.db, { categoryId: opts.categoryId }, now);
    return { value: { version: version ?? 0, ...page }, ttlSeconds: ttlSeconds(now, boundary) };
  }, {
    onError: onError(deps),
    isFresh: async (entry) => {
      if (!redis) return true;
      const [v] = await getVersions(redis, [versionKey]);
      return entry.version === v;
    },
  });
  return { items: value.items, total: value.total };
}
