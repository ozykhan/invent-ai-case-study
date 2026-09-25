import type { Product } from '../../api-types';
import type { ApiClient } from '../../client';
import { ApiError, SetupError } from '../../errors';
import type { Check } from '../report';
import { DEFAULT_PROMOTION_TTL_MS, type Scenario, type ScenarioOptions } from './types';

const MID_SALE_BASE = '20.00';
const MID_SALE_EXPECTED = '10.00'; // 50% off

/**
 * Port of scripts/demo-flash-sale.ts onto the engine. It records a "before" phase, turns on a 50% category
 * promotion, and records an "after" phase while a product created mid-sale must read back discounted on its first
 * read. The promotion is always cancelled at the end. An interrupt before the sale starts ends the run with no writes.
 */
export function createFlashSale(opts: ScenarioOptions): Scenario {
  const slug = opts.category ?? 'accessories';
  const ttlMs = opts.promotionTtlMs ?? DEFAULT_PROMOTION_TTL_MS;
  let categoryId = 0;
  let firstItemPrice = '';

  return {
    name: 'flash-sale',
    async setup(client, log) {
      const first = await client.listProducts({ category: slug, pageSize: 1 });
      const item = first.items[0];
      if (!item) throw new SetupError(`no products in category '${slug}'; run pnpm seed or ingest a vendor file first`);
      categoryId = item.category.id;
      log(`category ${slug} (id ${categoryId}): ${first.pagination.total} products`);
      if (first.pagination.total < 50_000) log(`warning: flash-sale expects 50k+ products in '${slug}'; ingest tmp/vendor-500k.csv first`);
    },
    next(rng) {
      const page = rng.int(1, opts.maxPage);
      return {
        label: 'list', method: 'GET', path: `/products?category=${encodeURIComponent(slug)}&page=${page}&pageSize=20`,
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
        // Interrupted while the promotion was being created: cancel it (finally) without the product or the check.
        if (runner.aborted()) return;
        const [, check] = await Promise.all([
          runner.phase('after', runner.durationMs - beforeMs),
          verifyMidSaleProduct(client, categoryId, promo.id),
        ]);
        runner.addCheck(check);
        runner.note('firstItemPriceAfter', firstItemPrice || '-');
      } finally {
        await client.cancelPromotion(promo.id).catch((err: unknown) => runner.log(`warning: could not cancel promotion ${promo.id}: ${String(err)}`));
      }
    },
  };
}

/** Creates a product in the promoted category during the sale and requires it to read back at half price. */
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
