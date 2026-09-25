import type { RequestSpec } from '../engine';
import { pickWeighted, type Rng } from '../rng';
import { detailRequest, listRequest, sampleCatalog, type CatalogSample } from './catalog';
import { DEFAULT_PROMOTION_TTL_MS, type Scenario, type ScenarioOptions } from './types';

export const WRITE_MIX_LABELS = ['list', 'detail', 'stock', 'promo'] as const;
const DEFAULT_MIX = { list: 60, detail: 25, stock: 14, promo: 1 };

/**
 * Reads with concurrent writes. Stock writes hit the live stock counters. Promotion create/cancel cycles bump the
 * cache version counters, which forces cache rebuilds while reads continue.
 */
export function createWriteMix(opts: ScenarioOptions): Scenario {
  const mix = opts.mix ?? DEFAULT_MIX;
  const ttlMs = opts.promotionTtlMs ?? DEFAULT_PROMOTION_TTL_MS;
  let sample: CatalogSample = { total: 0, ids: [], slugs: [] };
  /** Created, no cancel sent yet: the next promo slot cancels the oldest. */
  const open: string[] = [];
  /** Created, cancel not yet confirmed: cleanup cancels whatever is left. */
  const uncancelled = new Set<string>();

  const promo = (rng: Rng): RequestSpec => {
    const id = open.shift();
    if (id) {
      return {
        label: 'promo:cancel', method: 'POST', path: `/promotions/${id}/cancel`,
        onResponse: (status) => { if (status === 200) uncancelled.delete(id); },
      };
    }
    const now = Date.now();
    return {
      label: 'promo:create', method: 'POST', path: '/promotions',
      body: {
        name: 'write-mix load test', discountType: 'percentage', value: '10',
        startsAt: new Date(now - 1000).toISOString(), endsAt: new Date(now + ttlMs).toISOString(),
        target: { productId: rng.pick(sample.ids) },
      },
      // On an interrupted run a create can answer after the drain window, and so after cleanup ran: that promotion is
      // never cancelled, but it expires on its own `ttlMs` after creation.
      onResponse: (status, body) => {
        const created = (body as { id?: unknown } | undefined)?.id;
        if (status === 201 && typeof created === 'string') { open.push(created); uncancelled.add(created); }
      },
    };
  };

  return {
    name: 'write-mix',
    async setup(client, log) {
      sample = await sampleCatalog(client, { category: opts.category });
      log(`sampled ${sample.ids.length} product ids across ${sample.slugs.length} categories (catalog total ${sample.total})`);
    },
    next(rng) {
      switch (pickWeighted(rng, mix)) {
        case 'list': return listRequest(rng, sample, opts);
        case 'detail': return detailRequest(rng, sample);
        // An absolute set never fails. A −1 delta would return 422 at stock 0 and pollute the error stats.
        case 'stock': return { label: 'stock', method: 'PATCH', path: `/products/${rng.pick(sample.ids)}/stock`, body: { stock: rng.int(0, 100) } };
        default: return promo(rng);
      }
    },
    async cleanup(client) {
      for (const id of [...uncancelled]) {
        await client.cancelPromotion(id).catch(() => undefined);
        uncancelled.delete(id);
      }
    },
  };
}
