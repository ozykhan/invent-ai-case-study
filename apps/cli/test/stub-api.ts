import { randomUUID } from 'node:crypto';
import { createServer, type IncomingMessage } from 'node:http';
import type { AddressInfo } from 'node:net';

/** The id POST /products returns; GET of it reads back with the stub's mid-sale price and the latest promotion. */
export const MIDSALE_ID = 900001;
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

const product = (id: number) => {
  const slug = SLUGS[id % SLUGS.length]!;
  return {
    id, sku: `SKU-${id}`, name: `Product ${id}`,
    category: { id: (id % SLUGS.length) + 1, name: slug, slug },
    basePrice: '20.00', effectivePrice: '20.00', activePromotion: null, stock: 5,
  };
};

/** A fake ModaCo API: a catalog of `total` products and in-memory promotions. Rotates X-Instance-Id over i1, i2, i3. */
export async function startStubApi(opts: { total?: number; degraded?: boolean; midSalePrice?: string } = {}): Promise<StubApi> {
  const total = opts.total ?? 1000;
  const state: StubState = { hits: new Map(), samplePages: 0, openPromotions: new Set() };
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
        for (let id = (page - 1) * pageSize + 1; id <= Math.min(total, page * pageSize); id++) items.push(product(id));
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
      case 'PATCH /products/:id/stock':
        return send(200, { id: Number(segment), stock: Number(body?.stock ?? 0) });
      case 'POST /promotions': {
        const id = randomUUID();
        state.openPromotions.add(id);
        state.lastPromotionId = id;
        return send(201, { id, ...body, cancelledAt: null, createdAt: new Date().toISOString() });
      }
      case 'POST /promotions/:uuid/cancel':
        state.openPromotions.delete(segment);
        return send(200, { id: segment, cancelledAt: new Date().toISOString() });
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
