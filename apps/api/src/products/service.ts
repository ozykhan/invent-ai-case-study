import { eq, sql } from 'drizzle-orm';
import { bumpCategory, categories, fetchProductById, keys, products, type ProductRecord } from '@modaco/core';
import type { AppDeps } from '../deps';
import { conflict, notFound, pgErrorCode, unprocessable } from '../errors';
import { getCachedProduct, getCachedProductPage, resolveCategoryId } from './cached-reads';
import type { CreateProductBody, ListQuery, StockBody } from './schemas';
import { loadStocks, setStock } from './stock';

export interface ProductItem extends ProductRecord { stock: number }

export class ProductService {
  constructor(private readonly deps: AppDeps) {}

  /** `signal` fires when the client disconnects; every database phase checks it first and skips the work. */
  async getProduct(id: number, signal?: AbortSignal): Promise<ProductItem | null> {
    const cached = await getCachedProduct(this.deps, id, signal);
    if (!cached) return null;
    // A warm cache hit already carries the live stock counter (fetched in the same round trip as
    // the category-version freshness check); only fall back to a separate lookup when it doesn't.
    if (cached.stock !== undefined) return { ...cached.record, stock: cached.stock };
    const stocks = await loadStocks(this.deps, [id], signal);
    return { ...cached.record, stock: stocks.get(id) ?? 0 };
  }

  async listProducts(q: ListQuery, signal?: AbortSignal): Promise<{ items: ProductItem[]; pagination: { page: number; pageSize: number; total: number } }> {
    let categoryId: number | null = null;
    if (q.category) {
      categoryId = await resolveCategoryId(this.deps, q.category, signal);
      if (categoryId === null) throw notFound(`category '${q.category}' not found`);
    }
    const sort = q.sort === '-effective_price' ? 'desc' : 'asc';
    const page = await getCachedProductPage(this.deps, { categoryId, sort, page: q.page, pageSize: q.pageSize }, signal);
    const stocks = await loadStocks(this.deps, page.items.map((i) => i.id), signal);
    return {
      items: page.items.map((i) => ({ ...i, stock: stocks.get(i.id) ?? 0 })),
      pagination: { page: q.page, pageSize: q.pageSize, total: page.total },
    };
  }

  async createProduct(body: CreateProductBody): Promise<ProductItem> {
    const [cat] = await this.deps.db.select({ id: categories.id }).from(categories).where(eq(categories.id, body.categoryId));
    if (!cat) throw notFound(`category ${body.categoryId} not found`);
    let created: { id: number; stock: number };
    try {
      const [row] = await this.deps.db.insert(products).values({
        sku: body.sku, name: body.name, categoryId: body.categoryId, basePrice: body.basePrice, stock: body.stock,
      }).returning({ id: products.id, stock: products.stock });
      created = row!;
    } catch (err) {
      if (pgErrorCode(err) === '23505') throw conflict(`sku '${body.sku}' already exists`);
      throw err;
    }
    const redis = this.deps.redis;
    if (redis) {
      await bumpCategory(redis, body.categoryId, (msg, err) => this.deps.logger.error({ err }, msg));
      // A GET for this id just before the insert cached a short-lived "not found" entry, which carries
      // no versions and so would read as fresh: drop it (best effort) so GETs see the new product.
      await redis.del(keys.product(created.id)).catch((err) => this.deps.logger.warn({ err, id: created.id }, 'product cache delete failed'));
    }
    await setStock(this.deps, created.id, created.stock);
    // Built straight from Postgres rather than through the cache, so the response never depends on that delete.
    const record = await fetchProductById(this.deps.db, created.id, this.deps.now());
    return { ...record!, stock: created.stock };
  }

  async adjustStock(id: number, body: StockBody): Promise<{ id: number; stock: number } | null> {
    const [exists] = await this.deps.db.select({ stock: products.stock }).from(products).where(eq(products.id, id));
    if (!exists) return null;
    const set = 'delta' in body
      ? { stock: sql`${products.stock} + ${body.delta}`, updatedAt: sql`now()` }
      : { stock: body.stock, updatedAt: sql`now()` };
    let row: { id: number; stock: number } | undefined;
    try {
      [row] = await this.deps.db.update(products).set(set).where(eq(products.id, id)).returning({ id: products.id, stock: products.stock });
    } catch (err) {
      if (pgErrorCode(err) === '23514') throw unprocessable('stock cannot go below zero');
      if (pgErrorCode(err) === '22003') throw unprocessable('stock would exceed the maximum of 2147483647');
      throw err;
    }
    await setStock(this.deps, id, row!.stock);
    return row!;
  }
}
