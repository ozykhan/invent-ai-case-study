import type { SortDir } from '../products/queries';

export const DEFAULT_TTL_SECONDS = 300;
/**
 * Stock counters self-heal on the same bound as cache entries: concurrent write-throughs can land in
 * Redis out of commit order, and this TTL caps how long such a counter can disagree with Postgres.
 */
export const STOCK_TTL_SECONDS = 300;
export const SLUG_TTL_SECONDS = 300;
/**
 * Short TTL for negative/empty cache entries: a missing product, a missing category slug, or a
 * list page past the end of the result set. These are cheap to recompute, so the TTL only needs
 * to bound how long a stale "not found"/"empty" answer can outlive the write that would fix it
 * (version validation invalidates it sooner, when the write bumps a version this entry depends on).
 */
export const NOT_FOUND_TTL_SECONDS = 5;

export const keys = {
  productVersion: (id: number) => `ver:product:${id}`,
  categoryVersion: (id: number) => `ver:category:${id}`,
  allVersion: () => 'ver:all',
  product: (id: number) => `product:${id}`,
  list: (categoryId: number | null, sort: SortDir, page: number, pageSize: number) =>
    `list:${categoryId ?? 'all'}:${sort}:${page}:${pageSize}`,
  categorySlug: (slug: string) => `category:slug:${slug}`,
  stock: (id: number) => `stock:${id}`,
  lock: (key: string) => `lock:${key}`,
};

/** TTL is the default, capped so the entry expires exactly when the next promotion boundary would change a price. */
export function ttlSeconds(now: Date, nextBoundary: Date | null, max = DEFAULT_TTL_SECONDS): number {
  if (!nextBoundary) return max;
  const secs = Math.ceil((nextBoundary.getTime() - now.getTime()) / 1000);
  return Math.max(1, Math.min(max, secs));
}
