import type { Product } from '../../api-types';
import type { ApiClient } from '../../client';
import { ApiError, SetupError } from '../../errors';
import { discountedPrice } from '../../money';
import type { Check } from '../report';
import { LIST_PAGE_SIZE, pagesFor } from './catalog';
import { DEFAULT_PROMOTION_TTL_MS, type Scenario, type ScenarioOptions } from './types';

const MID_SALE_BASE = '20.00';
const MID_SALE_EXPECTED = '10.00'; // 50% off

/**
 * Port of scripts/demo-flash-sale.ts onto the engine. It records a "before" phase, turns on a 50% category
 * promotion, and records an "after" phase while a mid-sale product and the category listing must both reflect the
 * discount. The promotion is always cancelled at the end. An interrupt before the sale starts ends the run with no
 * writes.
 */
export function createFlashSale(opts: ScenarioOptions): Scenario {
  const slug = opts.category ?? 'accessories';
  const ttlMs = opts.promotionTtlMs ?? DEFAULT_PROMOTION_TTL_MS;
  let categoryId = 0;
  let firstItemPrice = '';
  let maxPage = opts.maxPage;

  return {
    name: 'flash-sale',
    async setup(client, log) {
      const first = await client.listProducts({ category: slug, pageSize: 1 });
      const item = first.items[0];
      if (!item) throw new SetupError(`no products in category '${slug}'; run pnpm seed or ingest a vendor file first`);
      categoryId = item.category.id;
      log(`category ${slug} (id ${categoryId}): ${first.pagination.total} products`);
      if (first.pagination.total < 50_000) log(`warning: flash-sale expects 50k+ products in '${slug}'; ingest tmp/vendor-500k.csv first`);
      const pages = pagesFor(first.pagination.total);
      maxPage = Math.max(1, Math.min(opts.maxPage, pages));
      if (maxPage < opts.maxPage) log(`max-page for '${slug}' clamped ${opts.maxPage} -> ${maxPage} (${first.pagination.total} products)`);
    },
    next(rng) {
      const page = rng.int(1, maxPage);
      return {
        label: 'list', method: 'GET', path: `/products?category=${encodeURIComponent(slug)}&page=${page}&pageSize=${LIST_PAGE_SIZE}`,
        onResponse: page === 1
          ? (status, body) => {
              const price = (body as { items?: Product[] } | undefined)?.items?.[0]?.effectivePrice;
              if (status === 200 && price) firstItemPrice = price;
            }
          : undefined,
      };
    },
    async run(runner) {
      const { client } = runner;
      await runner.warmup();
      const beforeMs = Math.floor(runner.durationMs / 3);
      await runner.phase('before', beforeMs);
      // Interrupted during warmup or `before`: stop here. Starting the sale now would write a category-wide discount
      // and record a check that never ran under load.
      if (runner.aborted()) return;
      runner.note('firstItemPriceBefore', firstItemPrice || '-');

      const now = Date.now();
      const promo = await client.createPromotion({
        name: 'Flash sale load test', discountType: 'percentage', value: '50',
        startsAt: new Date(now - 1000).toISOString(), endsAt: new Date(now + ttlMs).toISOString(),
        target: { categoryId },
      });
      runner.note('promotionId', promo.id);
      runner.log(`flash sale created -> promotion ${promo.id}`);
      try {
        // Interrupted while the promotion was being created: cancel it (finally) without the product or the checks.
        if (runner.aborted()) return;
        const [, midSaleCheck, listingCheck] = await Promise.all([
          runner.phase('after', runner.durationMs - beforeMs),
          verifyMidSaleProduct(client, categoryId, promo.id),
          verifyListingInvalidated(client, slug, promo.value),
        ]);
        runner.addCheck(midSaleCheck);
        runner.addCheck(listingCheck);
        runner.note('firstItemPriceAfter', firstItemPrice || '-');
      } finally {
        await client.cancelPromotion(promo.id).catch((err: unknown) => runner.log(`warning: could not cancel promotion ${promo.id}: ${String(err)}`));
      }
    },
  };
}

/**
 * Reads page 1 of the category listing (default sort, not part of the load) right after the promotion is created,
 * and requires its first item's effectivePrice to already reflect the discount -- proof the listing cache was
 * invalidated by the promotion write, not just the single-product cache verifyMidSaleProduct exercises.
 */
async function verifyListingInvalidated(client: ApiClient, slug: string, percentOff: string): Promise<Check> {
  const name = 'category listing reflects the promo';
  try {
    const page = await client.listProducts({ category: slug, page: 1, pageSize: LIST_PAGE_SIZE });
    const item = page.items[0];
    if (!item) return { name, ok: false, message: `category '${slug}' page 1 has no items` };
    const expected = discountedPrice(item.basePrice, 'percentage', percentOff);
    const ok = item.effectivePrice === expected;
    return {
      name,
      ok,
      message: ok
        ? `category '${slug}' page 1 item ${item.id} reads ${item.effectivePrice} (base ${item.basePrice}, ${percentOff}% off)`
        : `category '${slug}' page 1 item ${item.id} reads ${item.effectivePrice}, expected ${expected} (base ${item.basePrice}, ${percentOff}% off)`,
    };
  } catch (err) {
    return { name, ok: false, message: err instanceof ApiError ? `HTTP ${err.status} ${err.code}: ${err.message}` : String(err) };
  }
}

/**
 * Requires a product created *during* the sale to read back at half price on its very first read -- no product is
 * reused between runs (a fixed sku would only prove an existing cache entry gets invalidated, not that a genuinely
 * new row is correct from its first read). This does leak one `products` row per run; see the README for the
 * cleanup query.
 */
async function verifyMidSaleProduct(client: ApiClient, categoryId: number, promotionId: string): Promise<Check> {
  const name = 'mid-sale product discounted';
  const sku = `MIDSALE-LOAD-${Date.now()}`;
  try {
    const created = await client.createProduct({ sku, name: 'Mid-sale load test product', categoryId, basePrice: MID_SALE_BASE, stock: 1 });
    const read = await client.getProduct(created.id);
    const ok = read.effectivePrice === MID_SALE_EXPECTED && read.activePromotion?.id === promotionId;
    return {
      name,
      ok,
      message: ok
        ? `product ${read.id} (sku ${sku}) created during the sale reads back at ${read.effectivePrice} (base ${MID_SALE_BASE}) under promotion ${promotionId}`
        : `product ${read.id} (sku ${sku}) reads back at ${read.effectivePrice}, expected ${MID_SALE_EXPECTED} under promotion ${promotionId}; activePromotion ${JSON.stringify(read.activePromotion)}`,
    };
  } catch (err) {
    return { name, ok: false, message: err instanceof ApiError ? `HTTP ${err.status} ${err.code}: ${err.message}` : String(err) };
  }
}
