import type { SortDir } from '../products/queries';

export const DEFAULT_TTL_SECONDS = 300;
export const STOCK_TTL_SECONDS = 86_400;
export const SLUG_TTL_SECONDS = 300;

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
