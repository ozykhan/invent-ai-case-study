const api = process.env.API_URL ?? 'http://localhost:3000';
const slug = process.argv[2] ?? 'accessories';
const durationMs = Number(process.env.DURATION_MS ?? 15_000);
const concurrency = Number(process.env.CONCURRENCY ?? 50);
const flipAtMs = Math.floor(durationMs / 3);

const first = await (await fetch(`${api}/products?category=${slug}&pageSize=1`)).json();
const categoryId = first.items[0]?.category.id;
if (!categoryId) throw new Error(`no products in category ${slug}; run pnpm seed or ingest first`);

const before: number[] = []; const after: number[] = [];
let flipped = false; let errors = 0; let priceBefore = ''; let priceAfter = '';
const started = Date.now();

async function worker() {
  while (Date.now() - started < durationMs) {
    const page = 1 + Math.floor(Math.random() * 5);
    const t = performance.now();
    const res = await fetch(`${api}/products?category=${slug}&page=${page}&pageSize=20`);
    const ms = performance.now() - t;
    if (!res.ok) { errors++; continue; }
    (flipped ? after : before).push(ms);
    if (page === 1) {
      const body = await res.json();
      if (!flipped) priceBefore = body.items[0].effectivePrice; else priceAfter = body.items[0].effectivePrice;
    }
  }
}

const flip = (async () => {
  await new Promise((r) => setTimeout(r, flipAtMs));
  const res = await fetch(`${api}/promotions`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({
    name: 'Flash sale demo', discountType: 'percentage', value: '50',
    startsAt: new Date(Date.now() - 1000).toISOString(), endsAt: new Date(Date.now() + 3_600_000).toISOString(), target: { categoryId },
  }) });
  const promo = await res.json();
  flipped = true;
  console.log(`flash sale created at ${((Date.now() - started) / 1000).toFixed(1)}s -> promotion ${promo.id}`);
  return promo.id as string;
})();

await Promise.all(Array.from({ length: concurrency }, worker));
const promoId = await flip;
await fetch(`${api}/promotions/${promoId}/cancel`, { method: 'POST' });

const pct = (xs: number[], p: number) => { const s = [...xs].sort((a, b) => a - b); return s.length ? s[Math.min(s.length - 1, Math.floor(p * s.length))]!.toFixed(1) : 'n/a'; };
const report = (label: string, xs: number[], secs: number) =>
  console.log(`${label}: ${xs.length} req, ${(xs.length / secs).toFixed(0)} req/s, p50 ${pct(xs, 0.5)}ms, p95 ${pct(xs, 0.95)}ms, p99 ${pct(xs, 0.99)}ms`);
report('before flip', before, flipAtMs / 1000);
report('after flip ', after, (durationMs - flipAtMs) / 1000);
console.log(`errors: ${errors}; first item price before ${priceBefore} -> after ${priceAfter} (promotion cancelled again)`);
