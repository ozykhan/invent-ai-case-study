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
    const record = await getCachedProduct(this.deps, id);
    if (!record) return null;
    const stocks = await loadStocks(this.deps, [id]);
    return { ...record, stock: stocks.get(id) ?? 0 };
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
