import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Product } from '../../api-types';
import type { ApiClient } from '../../client';
import { ApiError, SetupError } from '../../errors';
import type { Check } from '../report';
import { pagesFor } from './catalog';
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

/** Same rounding the API applies: `round(base * (1 - value / 100), 2)` (packages/core/src/products/queries.ts). */
function discountedPrice(basePrice: string, percentOff: string): string {
  const price = Number(basePrice) * (1 - Number(percentOff) / 100);
  return (Math.round(price * 100) / 100).toFixed(2);
}

/**
 * Reads page 1 of the category listing (default sort, not part of the load) right after the promotion is created,
 * and requires its first item's effectivePrice to already reflect the discount -- proof the listing cache was
 * invalidated by the promotion write, not just the single-product cache verifyMidSaleProduct exercises.
 */
async function verifyListingInvalidated(client: ApiClient, slug: string, percentOff: string): Promise<Check> {
  const name = 'category listing reflects the promo';
  try {
    const page = await client.listProducts({ category: slug, page: 1, pageSize: 20 });
    const item = page.items[0];
    if (!item) return { name, ok: false, message: `category '${slug}' page 1 has no items` };
    const expected = discountedPrice(item.basePrice, percentOff);
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
 * One fixture file per target (hashed base URL) under the OS temp dir, mapping sku -> product id, so repeated runs
 * against the same target reuse one product instead of leaking a new row every time. Best-effort: a read or write
 * failure just falls back to creating (and not remembering) a product for this run.
 */
function fixturePath(baseUrl: string): string {
  const hash = createHash('sha1').update(baseUrl).digest('hex').slice(0, 16);
  return join(tmpdir(), `modaco-cli-flash-sale-${hash}.json`);
}

function readFixtureId(baseUrl: string, sku: string): number | undefined {
  try {
    const state = JSON.parse(readFileSync(fixturePath(baseUrl), 'utf8')) as Record<string, number>;
    return state[sku];
  } catch {
    return undefined;
  }
}

function writeFixtureId(baseUrl: string, sku: string, id: number): void {
  let state: Record<string, number> = {};
  try { state = JSON.parse(readFileSync(fixturePath(baseUrl), 'utf8')) as Record<string, number>; } catch { /* no fixture yet */ }
  try { writeFileSync(fixturePath(baseUrl), JSON.stringify({ ...state, [sku]: id })); } catch { /* best effort; this run just won't be remembered */ }
}

/** The remembered product for `sku`, if it still exists; otherwise a freshly created one (remembered for next time). */
async function resolveMidSaleProductId(client: ApiClient, sku: string, categoryId: number): Promise<number> {
  const cached = readFixtureId(client.baseUrl, sku);
  if (cached !== undefined) {
    try { return (await client.getProduct(cached)).id; } catch { /* gone (fresh db, or a different target); recreate below */ }
  }
  try {
    const created = await client.createProduct({ sku, name: 'Mid-sale load test product', categoryId, basePrice: MID_SALE_BASE, stock: 1 });
    writeFixtureId(client.baseUrl, sku, created.id);
    return created.id;
  } catch (err) {
    // The sku exists but the fixture didn't know its id (cleared, or a run against this target from elsewhere): a
    // one-off suffixed sku keeps this run going instead of failing the check outright.
    if (err instanceof ApiError && err.status === 409) {
      const created = await client.createProduct({ sku: `${sku}-${Date.now()}`, name: 'Mid-sale load test product', categoryId, basePrice: MID_SALE_BASE, stock: 1 });
      writeFixtureId(client.baseUrl, sku, created.id);
      return created.id;
    }
    throw err;
  }
}

/** Requires a product in the promoted category to read back at half price, reusing the same product run to run. */
async function verifyMidSaleProduct(client: ApiClient, categoryId: number, promotionId: string): Promise<Check> {
  const name = 'mid-sale product discounted';
  const sku = `MIDSALE-LOAD-${categoryId}`;
  try {
    const id = await resolveMidSaleProductId(client, sku, categoryId);
    const read = await client.getProduct(id);
    const ok = read.effectivePrice === MID_SALE_EXPECTED && read.activePromotion?.id === promotionId;
    return {
      name,
      ok,
      message: ok
        ? `product ${read.id} (sku ${sku}) reads back at ${read.effectivePrice} (base ${MID_SALE_BASE}) under promotion ${promotionId}`
        : `product ${read.id} (sku ${sku}) reads back at ${read.effectivePrice}, expected ${MID_SALE_EXPECTED} under promotion ${promotionId}; activePromotion ${JSON.stringify(read.activePromotion)}`,
    };
  } catch (err) {
    return { name, ok: false, message: err instanceof ApiError ? `HTTP ${err.status} ${err.code}: ${err.message}` : String(err) };
  }
}
