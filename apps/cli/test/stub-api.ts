import { randomUUID } from 'node:crypto';
import { createServer, type IncomingMessage } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { Product } from '../src/api-types';

/** The id POST /products returns; GET of it reads back with the stub's mid-sale price and the latest promotion. */
export const MIDSALE_ID = 900001;
/** The one ingestion job GET /ingestion/jobs/:id knows; any other id is a 404. */
export const STUB_JOB_ID = '0f8fad5b-d9cb-469f-a165-70867728950e';
const SLUGS = ['accessories', 'shoes', 'bags'];
const INSTANCES = ['i1', 'i2', 'i3'];

export interface StubState {
  /** Request counts by route, e.g. "GET /products/:id". */
  hits: Map<string, number>;
  /** Listing requests with pageSize=100, which is what catalog sampling uses. */
  samplePages: number;
  openPromotions: Set<string>;
  lastPromotionId?: string;
}

export interface StubApi { url: string; state: StubState; close(): Promise<void> }

async function readJson(req: IncomingMessage): Promise<Record<string, unknown> | undefined> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const text = Buffer.concat(chunks).toString();
  return text ? (JSON.parse(text) as Record<string, unknown>) : undefined;
}

const product = (id: number): Product => {
  const slug = SLUGS[id % SLUGS.length]!;
  return {
    id, sku: `SKU-${id}`, name: `Product ${id}`,
    category: { id: (id % SLUGS.length) + 1, name: slug, slug },
    basePrice: '20.00', effectivePrice: '20.00', activePromotion: null, stock: 5,
  };
};

/**
 * If `lastPromotionId` names a live (uncancelled) promotion that targets this product's category or id, overrides
 * its effectivePrice/activePromotion the same way the real API's listing would -- so a scenario's "did the listing
 * pick up the promo" check has something real to observe, not just the single-product GET path.
 */
function withActivePromotion(
  p: Product, lastPromotionId: string | undefined, promotionsById: Map<string, Record<string, unknown>>,
): Product {
  const promo = lastPromotionId ? promotionsById.get(lastPromotionId) : undefined;
  if (!promo || promo.cancelledAt) return p;
  const target = promo.target as { categoryId?: number; productId?: number } | undefined;
  if (target?.categoryId !== p.category.id && target?.productId !== p.id) return p;
  const value = Number(promo.value);
  const base = Number(p.basePrice);
  const discountType = promo.discountType as 'percentage' | 'fixed';
  const effectivePrice = discountType === 'percentage'
    ? (Math.round(base * (1 - value / 100) * 100) / 100).toFixed(2)
    : Math.max(0, base - value).toFixed(2);
  return {
    ...p, effectivePrice,
    activePromotion: { id: promo.id as string, name: promo.name as string, discountType, value: promo.value as string },
  };
}

/** A fake ModaCo API: a catalog of `total` products and in-memory promotions. Rotates X-Instance-Id over i1, i2, i3. */
export async function startStubApi(
  opts: { total?: number; degraded?: boolean; midSalePrice?: string; listingIgnoresPromotions?: boolean; failStatus?: number } = {},
): Promise<StubApi> {
  const total = opts.total ?? 1000;
  const state: StubState = { hits: new Map(), samplePages: 0, openPromotions: new Set() };
  const promotionsById = new Map<string, Record<string, unknown>>();
  const stockById = new Map<number, number>();
  let served = 0;

  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? '/', 'http://stub');
    const body = await readJson(req);
    res.setHeader('x-instance-id', INSTANCES[served++ % INSTANCES.length]!);
    const send = (status: number, value: unknown) => {
      res.statusCode = status;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify(value));
    };
    const route = `${req.method} ${url.pathname.replace(/\/[0-9a-f-]{36}(?=\/|$)/, '/:uuid').replace(/\/\d+(?=\/|$)/, '/:id')}`;
    state.hits.set(route, (state.hits.get(route) ?? 0) + 1);
    const segment = url.pathname.split('/')[2] ?? '';

    // Simulates a target that fails every request the load engine itself sends (a listing at the engine's own
    // pageSize=20, or any single-product GET), while leaving setup's own catalog sampling (pageSize 100 or 1) alone
    // -- so a scenario can still start, and a test can prove the load engine counts 5xx as errors instead of
    // silently mixing them into the success stats.
    if (opts.failStatus !== undefined) {
      const isSetupSampling = route === 'GET /products' && Number(url.searchParams.get('pageSize') ?? 20) !== 20;
      if (!isSetupSampling) return send(opts.failStatus, { error: { code: 'stub_failure', message: `stub always fails with ${opts.failStatus}` } });
    }

    switch (route) {
      case 'GET /health':
        return opts.degraded
          ? send(503, { status: 'degraded', checks: { postgres: false, redis: true } })
          : send(200, { status: 'ok', checks: { postgres: true, redis: true } });
      case 'GET /products': {
        const page = Number(url.searchParams.get('page') ?? 1);
        const pageSize = Number(url.searchParams.get('pageSize') ?? 20);
        if (pageSize === 100) state.samplePages++;
        const items = [];
        for (let id = (page - 1) * pageSize + 1; id <= Math.min(total, page * pageSize); id++) {
          items.push(opts.listingIgnoresPromotions ? product(id) : withActivePromotion(product(id), state.lastPromotionId, promotionsById));
        }
        return send(200, { items, pagination: { page, pageSize, total } });
      }
      case 'GET /products/:id': {
        const id = Number(segment);
        if (id === 404) return send(404, { error: { code: 'not_found', message: 'product 404 not found' } });
        if (id === MIDSALE_ID) {
          const promo = state.lastPromotionId ? { id: state.lastPromotionId, name: 'Flash', discountType: 'percentage', value: '50' } : null;
          return send(200, { ...product(id), effectivePrice: opts.midSalePrice ?? '10.00', activePromotion: promo });
        }
        return send(200, product(id));
      }
      case 'POST /products':
        return send(201, { ...product(MIDSALE_ID), sku: body?.sku, basePrice: body?.basePrice });
      case 'PATCH /products/:id/stock': {
        // Mirrors the API's body: exactly one of { stock } (set) or { delta } (adjust); every product starts at 5.
        const id = Number(segment);
        const stock = typeof body?.stock === 'number' ? body.stock : (stockById.get(id) ?? 5) + Number(body?.delta ?? 0);
        stockById.set(id, stock);
        return send(200, { id, stock });
      }
      case 'POST /promotions': {
        const id = randomUUID();
        state.openPromotions.add(id);
        state.lastPromotionId = id;
        const promotion = { id, ...body, cancelledAt: null, createdAt: new Date().toISOString() };
        promotionsById.set(id, promotion);
        return send(201, promotion);
      }
      case 'POST /promotions/:uuid/cancel': {
        state.openPromotions.delete(segment);
        // Mirrors the real API, which returns the full promotion view (not just id/cancelledAt) from cancel.
        const existing = promotionsById.get(segment) ?? { id: segment };
        const cancelled = { ...existing, cancelledAt: new Date().toISOString() };
        promotionsById.set(segment, cancelled);
        return send(200, cancelled);
      }
      case 'PUT /promotions/:uuid/target': {
        const existing = promotionsById.get(segment);
        if (!existing) return send(404, { error: { code: 'not_found', message: `promotion ${segment} not found` } });
        const moved = { ...existing, target: body };
        promotionsById.set(segment, moved);
        return send(200, moved);
      }
      case 'GET /ingestion/jobs/:uuid': {
        const jobId = url.pathname.split('/')[3];
        if (jobId !== STUB_JOB_ID) return send(404, { error: { code: 'not_found', message: `job ${jobId} not found` } });
        // The API's JobView (apps/api/src/ingestion/service.ts).
        return send(200, {
          id: STUB_JOB_ID, status: 'processing', s3Key: `uploads/${STUB_JOB_ID}/vendor.csv`, totalChunks: 9, completedChunks: 3,
          failedChunks: 0, rowsProcessed: 150000, rowsRejected: 12, error: null,
          createdAt: '2026-09-25T10:00:00.000Z', updatedAt: '2026-09-25T10:00:05.000Z',
        });
      }
      default:
        return send(404, { error: { code: 'not_found', message: 'route not found' } });
    }
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    state,
    close: () => new Promise<void>((resolve) => { server.closeAllConnections(); server.close(() => resolve()); }),
  };
}
