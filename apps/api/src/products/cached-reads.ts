import { eq } from 'drizzle-orm';
import {
  categories, fetchProductById, fetchProductPage, getVersions, keys, nextPromotionBoundary, NOT_FOUND_TTL_SECONDS,
  parseVersion, products, readThrough, SLUG_TTL_SECONDS, ttlSeconds, type ProductRecord, type Redis, type SortDir,
} from '@modaco/core';
import type { AppDeps } from '../deps';
import { throwIfClientGone } from '../middleware/client-abort';

interface ProductEntry { productVersion: number; categoryVersion: number; categoryId: number; record: ProductRecord }
interface PageEntry { version: number; items: ProductRecord[]; total: number }

export interface CachedProduct { record: ProductRecord; stock?: number }

const onError = (deps: AppDeps) => (err: unknown) => deps.logger.warn({ err }, 'cache degraded; serving from postgres');

/**
 * Version reads must never throw, and a value built from a version read that failed must never be
 * cached: a dead Redis client rejects the command outright (not just times out on a GET), and that
 * happens inside the builder itself, whose errors readThrough propagates to the caller from both the
 * lock-holder and the bypass path — so the failure has to be swallowed here.
 */
async function safeVersions(deps: AppDeps, redis: Redis | null, versionKeys: string[]): Promise<{ versions: number[]; ok: boolean }> {
  if (!redis) return { versions: versionKeys.map(() => 0), ok: true };
  try {
    return { versions: await getVersions(redis, versionKeys), ok: true };
  } catch (err) {
    deps.logger.warn({ err }, 'version read failed while building cache entry; serving uncached');
    return { versions: versionKeys.map(() => 0), ok: false };
  }
}

export async function resolveCategoryId(deps: AppDeps, slug: string, signal?: AbortSignal): Promise<number | null> {
  const { value } = await readThrough<number | null>(deps.redis, keys.categorySlug(slug), async () => {
    throwIfClientGone(signal);
    const [row] = await deps.db.select({ id: categories.id }).from(categories).where(eq(categories.slug, slug));
    return { value: row?.id ?? null, ttlSeconds: row ? SLUG_TTL_SECONDS : 5 };
  }, { onError: onError(deps) });
  return value;
}

export async function getCachedProduct(deps: AppDeps, id: number, signal?: AbortSignal): Promise<CachedProduct | null> {
  const redis = deps.redis;
  const { value, extra } = await readThrough<ProductEntry | null, number>(redis, keys.product(id), async () => {
    const now = deps.now();
    // The category id must be captured, and its version read, BEFORE the price-affecting fetch
    // below: if we read the category version only after fetching, a promotion that commits (and
    // bumps ver:category) in the gap between the fetch and that read would stamp data computed
    // under the OLD promotion state with the NEW version number, and the entry would then read as
    // fresh — serving stale prices for the rest of the TTL. Reading it first means any bump during
    // or after the fetch makes the current version strictly newer than what's stamped, so isFresh
    // correctly rejects it on the next read instead. The lookup is a cheap indexed PK read.
    throwIfClientGone(signal);
    const [catRow] = await deps.db.select({ categoryId: products.categoryId }).from(products).where(eq(products.id, id));
    if (!catRow) return { value: null, ttlSeconds: NOT_FOUND_TTL_SECONDS };
    const { versions, ok } = await safeVersions(deps, redis, [keys.productVersion(id), keys.categoryVersion(catRow.categoryId)]);
    const [productVersion, categoryVersion] = versions;
    throwIfClientGone(signal);
    const record = await fetchProductById(deps.db, id, now);
    if (!record) return { value: null, ttlSeconds: NOT_FOUND_TTL_SECONDS };
    // Defensive: if the product's category itself changed between the lookup above and this
    // fetch (a rare admin operation, not a promotion bump), the version we captured belongs to
    // the wrong category — don't cache that mismatch.
    const categoryChanged = record.category.id !== catRow.categoryId;
    throwIfClientGone(signal);
    const boundary = await nextPromotionBoundary(deps.db, { productId: id, categoryId: record.category.id }, now);
    return {
      value: { productVersion: productVersion ?? 0, categoryVersion: categoryVersion ?? 0, categoryId: record.category.id, record },
      ttlSeconds: ttlSeconds(now, boundary),
      cache: ok && !categoryChanged,
    };
  }, {
    onError: onError(deps),
    // Round trip 1 (on a hit): MGET(product:{id}, ver:product:{id}) — the entry and its own
    // version together, so checking productVersion doesn't need a separate read.
    extraKeys: [keys.productVersion(id)],
    isFresh: async (entry, extraRaw) => {
      if (!entry || !redis) return true;
      if (entry.productVersion !== parseVersion(extraRaw[0])) return false;
      try {
        // Round trip 2 (on a hit): MGET(ver:category:{catId}, stock:{id}) — the category version
        // needed to confirm freshness (only known once the entry is parsed) bundled with the live
        // stock counter, so the service can skip its own stock lookup on this path.
        const [cvRaw, stockRaw] = await redis.mget(keys.categoryVersion(entry.categoryId), keys.stock(id));
        if (entry.categoryVersion !== parseVersion(cvRaw)) return false;
        return { fresh: true, extra: stockRaw == null ? undefined : Number(stockRaw) };
      } catch (err) {
        onError(deps)(err);
        return false;
      }
    },
  });
  if (!value) return null;
  return { record: value.record, stock: extra };
}

export async function getCachedProductPage(
  deps: AppDeps,
  opts: { categoryId: number | null; sort: SortDir; page: number; pageSize: number },
  signal?: AbortSignal,
): Promise<{ items: ProductRecord[]; total: number }> {
  const redis = deps.redis;
  const versionKey = opts.categoryId === null ? keys.allVersion() : keys.categoryVersion(opts.categoryId);
  const { value } = await readThrough<PageEntry>(redis, keys.list(opts.categoryId, opts.sort, opts.page, opts.pageSize), async () => {
    const now = deps.now();
    const { versions: [version], ok } = await safeVersions(deps, redis, [versionKey]);
    throwIfClientGone(signal);
    const page = await fetchProductPage(deps.db, {
      categoryId: opts.categoryId, sort: opts.sort, limit: opts.pageSize, offset: (opts.page - 1) * opts.pageSize, now,
    });
    // A page past the end is empty and cheap to recompute. It's still worth caching (with a short
    // TTL) rather than marking it uncacheable: `cache: false` makes every concurrent reader of the
    // key fall back to its own Postgres query once the holder releases the lock (crawlers hammer
    // exactly this kind of key, since `page` is attacker-controlled up to its cap). A short-TTL
    // entry serves those readers a cache hit instead, and staleness is bounded by the TTL plus the
    // normal version check above (a write that populates the page bumps the version and invalidates
    // it sooner).
    if (page.items.length === 0 && opts.page > 1) {
      return { value: { version: version ?? 0, ...page }, ttlSeconds: NOT_FOUND_TTL_SECONDS, cache: ok };
    }
    throwIfClientGone(signal);
    const boundary = await nextPromotionBoundary(deps.db, { categoryId: opts.categoryId }, now);
    return { value: { version: version ?? 0, ...page }, ttlSeconds: ttlSeconds(now, boundary), cache: ok };
  }, {
    onError: onError(deps),
    // Round trip 1 (on a hit): MGET(list entry, its version key) — the version key is known
    // upfront (it only depends on the query's category, not on the fetched page), so it's bundled
    // with the entry read. Stock is fetched separately by the caller once item ids are known
    // (round trip 2), same as the cold-path fetch.
    extraKeys: [versionKey],
    isFresh: async (entry, extraRaw) => entry.version === parseVersion(extraRaw[0]),
  });
  return { items: value.items, total: value.total };
}
