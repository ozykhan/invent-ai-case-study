import type { ProductRecord } from '@modaco/core';
import type { AppDeps } from '../deps';
import { notFound } from '../errors';
import { getCachedProduct, getCachedProductPage, resolveCategoryId } from './cached-reads';
import type { ListQuery } from './schemas';
import { loadStocks } from './stock';

export interface ProductItem extends ProductRecord { stock: number }

export class ProductService {
  constructor(private readonly deps: AppDeps) {}

  async getProduct(id: number): Promise<ProductItem | null> {
    const cached = await getCachedProduct(this.deps, id);
    if (!cached) return null;
    // A warm cache hit already carries the live stock counter (fetched in the same round trip as
    // the category-version freshness check); only fall back to a separate lookup when it doesn't.
    if (cached.stock !== undefined) return { ...cached.record, stock: cached.stock };
    const stocks = await loadStocks(this.deps, [id]);
    return { ...cached.record, stock: stocks.get(id) ?? 0 };
  }

  async listProducts(q: ListQuery): Promise<{ items: ProductItem[]; pagination: { page: number; pageSize: number; total: number } }> {
    let categoryId: number | null = null;
    if (q.category) {
      categoryId = await resolveCategoryId(this.deps, q.category);
      if (categoryId === null) throw notFound(`category '${q.category}' not found`);
    }
    const sort = q.sort === '-effective_price' ? 'desc' : 'asc';
    const page = await getCachedProductPage(this.deps, { categoryId, sort, page: q.page, pageSize: q.pageSize });
    const stocks = await loadStocks(this.deps, page.items.map((i) => i.id));
    return {
      items: page.items.map((i) => ({ ...i, stock: stocks.get(i.id) ?? 0 })),
      pagination: { page: q.page, pageSize: q.pageSize, total: page.total },
    };
  }
}
