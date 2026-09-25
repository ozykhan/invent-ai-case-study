import type { Product } from '../../api-types';
import type { ApiClient } from '../../client';
import { SetupError } from '../../errors';
import type { RequestSpec } from '../engine';
import type { Rng } from '../rng';

export interface CatalogSample { total: number; ids: number[]; slugs: string[] }

const SAMPLE_PAGE_SIZE = 100;
const API_MAX_PAGE = 1000;

/**
 * Reads up to `pages` listing pages spread evenly across the catalog and returns the product ids and category slugs
 * it saw. The API has no categories endpoint, and ids are not contiguous after ingests, so sampling is how a
 * scenario finds real targets.
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
  return { total, ids: [...ids], slugs: [...slugs] };
}

export function listRequest(rng: Rng, sample: CatalogSample, opts: { category?: string; maxPage: number }): RequestSpec {
  const category = opts.category ?? rng.pick(sample.slugs);
  const sort = rng.next() < 0.5 ? 'effective_price' : '-effective_price';
  return { label: 'list', method: 'GET', path: `/products?category=${encodeURIComponent(category)}&sort=${sort}&page=${rng.int(1, opts.maxPage)}&pageSize=20` };
}

export function detailRequest(rng: Rng, sample: CatalogSample): RequestSpec {
  return { label: 'detail', method: 'GET', path: `/products/${rng.pick(sample.ids)}` };
}
