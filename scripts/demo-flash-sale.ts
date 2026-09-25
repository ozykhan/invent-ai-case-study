const api = process.env.API_URL ?? 'http://localhost:3000';
const slug = process.argv[2] ?? 'accessories';
const durationMs = Number(process.env.DURATION_MS ?? 15_000);
const warmupMs = Number(process.env.WARMUP_MS ?? 4_000);
const concurrency = Number(process.env.CONCURRENCY ?? 50);
const beforeMs = Math.floor(durationMs / 3);
const afterMs = durationMs - beforeMs;

const firstRes = await fetch(`${api}/products?category=${slug}&pageSize=1`);
if (!firstRes.ok) {
  const text = await firstRes.text().catch(() => '');
  throw new Error(`category lookup failed: GET /products?category=${slug}&pageSize=1 -> ${firstRes.status} ${text}`);
}
const first = await firstRes.json();
const categoryId = first.items[0]?.category.id;
if (!categoryId) throw new Error(`no products in category ${slug}; run pnpm seed or ingest first`);
const productCount = Number(first.pagination?.total ?? 0);
console.log(`category ${slug} (id ${categoryId}): ${productCount} products`);
if (productCount < 50_000) {
  console.warn(`warning: category '${slug}' has only ${productCount} products; the flash-sale scenario expects 50k+ (run pnpm demo:ingest first)`);
}

let errors = 0;
let priceBefore = '';
let priceAfter = '';

/** Hammers the category listing for `ms` wall-clock time. Returns the actually measured span in seconds
 *  (not the nominal `ms`), so req/s figures reflect real elapsed time rather than the requested budget. */
async function runLoad(ms: number, sink: number[] | null, onPageOnePrice?: (price: string) => void): Promise<number> {
  const start = performance.now();
  const deadline = start + ms;

  async function worker() {
    while (performance.now() < deadline) {
      const page = 1 + Math.floor(Math.random() * 5);
      const t0 = performance.now();
      const res = await fetch(`${api}/products?category=${slug}&page=${page}&pageSize=20`);
      const elapsed = performance.now() - t0;
      if (!res.ok) {
        errors++;
        await res.arrayBuffer().catch(() => {}); // release the connection even on error
        continue;
      }
      const body = await res.json(); // always drain the body so pooled connections are released
      if (sink) sink.push(elapsed);
      if (page === 1 && onPageOnePrice && body.items[0]?.effectivePrice) onPageOnePrice(body.items[0].effectivePrice);
    }
  }

  await Promise.all(Array.from({ length: concurrency }, worker));
  return (performance.now() - start) / 1000;
}

/** Best-effort: looks for `sku` in the first few pages of the category listing, sorted by effective price. */
async function findInCategoryListing(sku: string): Promise<boolean> {
  const pageSize = 100;
  const maxPages = 5;
  for (let page = 1; page <= maxPages; page++) {
    const res = await fetch(`${api}/products?category=${slug}&page=${page}&pageSize=${pageSize}`);
    if (!res.ok) { await res.arrayBuffer().catch(() => {}); return false; }
    const body = await res.json();
    if (body.items.some((i: { sku: string }) => i.sku === sku)) return true;
    if (body.items.length < pageSize) break;
  }
  return false;
}

/** Creates a brand-new product inside the promoted category while the sale is live, and asserts it comes
 *  back discounted on its very first read (no cache-warming or special-casing required). */
async function verifyMidSaleProduct(catId: number, promoId: string): Promise<{ ok: boolean; message: string }> {
  const sku = `MIDSALE-DEMO-${Date.now()}`;
  const createRes = await fetch(`${api}/products`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ sku, name: 'Mid-sale demo product', categoryId: catId, basePrice: '20.00', stock: 1 }),
  });
  if (!createRes.ok) {
    const text = await createRes.text().catch(() => '');
    return { ok: false, message: `FAIL mid-sale check: POST /products -> ${createRes.status} ${text}` };
  }
  const created = await createRes.json();

  const getRes = await fetch(`${api}/products/${created.id}`);
  if (!getRes.ok) {
    await getRes.arrayBuffer().catch(() => {});
    return { ok: false, message: `FAIL mid-sale check: GET /products/${created.id} -> ${getRes.status}` };
  }
  const fetched = await getRes.json();

  const expectedPrice = '10.00'; // 50% off the 20.00 base price
  const gotPromo = fetched.activePromotion?.id === promoId;
  const gotPrice = fetched.effectivePrice === expectedPrice;

  let listNote: string;
  try {
    const found = await findInCategoryListing(sku);
    listNote = found ? '; also found via the category listing' : '; not found in the first pages of the category listing (expected in a large category — not itself a failure)';
  } catch {
    listNote = '; category listing check skipped (request error)';
  }

  if (!gotPromo || !gotPrice) {
    return {
      ok: false,
      message: `FAIL mid-sale check: product ${created.id} (sku ${sku}) did NOT get the discount -- effectivePrice ${fetched.effectivePrice} (expected ${expectedPrice}), activePromotion ${JSON.stringify(fetched.activePromotion)}${listNote}`,
    };
  }
  return {
    ok: true,
    message: `PASS mid-sale check: product ${created.id} (sku ${sku}) created during the sale correctly shows effectivePrice ${fetched.effectivePrice} (basePrice ${fetched.basePrice}) under promotion ${promoId}${listNote}`,
  };
}

const before: number[] = [];
const after: number[] = [];

console.log(`warming up for ${(warmupMs / 1000).toFixed(1)}s (not recorded; lets connections and the cache settle)...`);
await runLoad(warmupMs, null);

console.log('measuring warm-cache latency before the flip...');
const beforeSecs = await runLoad(beforeMs, before, (p) => { priceBefore = p; });

const promoRes = await fetch(`${api}/promotions`, {
  method: 'POST', headers: { 'content-type': 'application/json' },
  body: JSON.stringify({
    name: 'Flash sale demo', discountType: 'percentage', value: '50',
    startsAt: new Date(Date.now() - 1000).toISOString(), endsAt: new Date(Date.now() + 3_600_000).toISOString(), target: { categoryId },
  }),
});
if (!promoRes.ok) {
  const text = await promoRes.text().catch(() => '');
  throw new Error(`flash sale promotion creation failed: ${promoRes.status} ${text}`);
}
const promo = await promoRes.json();
console.log(`flash sale created -> promotion ${promo.id}`);

// Run the "after" load window and the mid-sale product check concurrently, so the new product is created
// and verified while load is still hitting the category (not after the load phase has already finished).
const afterPromise = runLoad(afterMs, after, (p) => { priceAfter = p; });
const midSalePromise = verifyMidSaleProduct(categoryId, promo.id);
const [afterSecs, midSaleResult] = await Promise.all([afterPromise, midSalePromise]);

const cancelRes = await fetch(`${api}/promotions/${promo.id}/cancel`, { method: 'POST' });
await cancelRes.arrayBuffer().catch(() => {});

const pct = (xs: number[], p: number) => { const s = [...xs].sort((a, b) => a - b); return s.length ? s[Math.min(s.length - 1, Math.floor(p * s.length))]!.toFixed(1) : 'n/a'; };
const report = (label: string, xs: number[], secs: number) =>
  console.log(`${label}: ${xs.length} req in ${secs.toFixed(1)}s, ${(xs.length / secs).toFixed(0)} req/s, p50 ${pct(xs, 0.5)}ms, p95 ${pct(xs, 0.95)}ms, p99 ${pct(xs, 0.99)}ms`);
report('warm cache, before flip                ', before, beforeSecs);
report('after flip (first requests rebuild cache entries)', after, afterSecs);
console.log(`errors: ${errors}; first item price before ${priceBefore} -> after ${priceAfter} (promotion cancelled again)`);

console.log(midSaleResult.message);
if (!midSaleResult.ok) {
  process.exitCode = 1;
}
