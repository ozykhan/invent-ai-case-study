import type { Product } from '../../api-types';
import type { ApiClient } from '../../client';
import { SetupError } from '../../errors';
import type { RequestSpec } from '../engine';
import type { Rng } from '../rng';

export interface CatalogSample {
  total: number;
  ids: number[];
  slugs: string[];
  /** Real product count per sampled category slug (`pagination.total`), for clamping --max-page to what exists. */
  totalsBySlug: Record<string, number>;
}

const SAMPLE_PAGE_SIZE = 100;
const API_MAX_PAGE = 1000;
/** pageSize the load engine's own listing requests use (`listRequest` below and the flash-sale scenario). */
export const LIST_PAGE_SIZE = 20;

/** Highest real page number for a category with `total` products at LIST_PAGE_SIZE, or Infinity if `total` is unknown. */
export function pagesFor(total: number | undefined): number {
  return total === undefined ? Infinity : Math.max(1, Math.ceil(total / LIST_PAGE_SIZE));
}

/**
 * Reads up to `pages` listing pages spread evenly across the catalog and returns the product ids and category slugs
 * it saw, plus each sampled category's real product count. The API has no categories endpoint, and ids are not
 * contiguous after ingests, so sampling is how a scenario finds real targets.
 */
export async function sampleCatalog(client: ApiClient, opts: { category?: string; pages?: number } = {}): Promise<CatalogSample> {
  const first = await client.listProducts({ category: opts.category, page: 1, pageSize: SAMPLE_PAGE_SIZE });
  const total = first.pagination.total;
  if (total === 0) {
    throw new SetupError(`no products${opts.category ? ` in category '${opts.category}'` : ''}; run pnpm seed or pnpm modaco ingest upload <file> first`);
  }
  const pageCount = Math.min(Math.ceil(total / SAMPLE_PAGE_SIZE), API_MAX_PAGE);
  const n = Math.min(opts.pages ?? 20, pageCount);
  const ids = new Set<number>();
  const slugs = new Set<string>();
  const add = (items: Product[]) => { for (const p of items) { ids.add(p.id); slugs.add(p.category.slug); } };
  add(first.items);
  for (let i = 1; i < n; i++) {
    const page = 1 + Math.floor((i * pageCount) / n);
    add((await client.listProducts({ category: opts.category, page, pageSize: SAMPLE_PAGE_SIZE })).items);
  }
  // A single fixed --category already has its exact total from `first`; a multi-category sample needs one cheap,
  // unmeasured request per slug to find how many pages it actually has.
  const totalsBySlug: Record<string, number> = {};
  for (const slug of slugs) {
    totalsBySlug[slug] = opts.category !== undefined ? total : (await client.listProducts({ category: slug, page: 1, pageSize: 1 })).pagination.total;
  }
  return { total, ids: [...ids], slugs: [...slugs], totalsBySlug };
}

/** Lines reporting which sampled categories have fewer than `maxPage` pages, for the scenario's setup log. */
export function describeMaxPageClamp(sample: CatalogSample, maxPage: number): string[] {
  return Object.entries(sample.totalsBySlug)
    .map(([slug, total]): [string, number, number] => [slug, pagesFor(total), total])
    .filter(([, pages]) => pages < maxPage)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([slug, pages, total]) => `max-page for '${slug}' clamped ${maxPage} -> ${pages} (${total} products)`);
}

export function listRequest(rng: Rng, sample: CatalogSample, opts: { category?: string; maxPage: number }): RequestSpec {
  const category = opts.category ?? rng.pick(sample.slugs);
  const sort = rng.next() < 0.5 ? 'effective_price' : '-effective_price';
  const maxPage = Math.max(1, Math.min(opts.maxPage, pagesFor(sample.totalsBySlug[category])));
  return { label: 'list', method: 'GET', path: `/products?category=${encodeURIComponent(category)}&sort=${sort}&page=${rng.int(1, maxPage)}&pageSize=${LIST_PAGE_SIZE}` };
}

export function detailRequest(rng: Rng, sample: CatalogSample): RequestSpec {
  return { label: 'detail', method: 'GET', path: `/products/${rng.pick(sample.ids)}` };
}
