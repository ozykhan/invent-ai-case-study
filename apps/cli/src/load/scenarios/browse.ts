import { pickWeighted } from '../rng';
import { detailRequest, listRequest, sampleCatalog, type CatalogSample } from './catalog';
import type { Scenario, ScenarioOptions } from './types';

export const BROWSE_LABELS = ['list', 'detail'] as const;
const DEFAULT_MIX = { list: 70, detail: 30 };

/** Read-only traffic: category listings with random page and sort, and product details by sampled id. */
export function createBrowse(opts: ScenarioOptions): Scenario {
  const mix = opts.mix ?? DEFAULT_MIX;
  let sample: CatalogSample = { total: 0, ids: [], slugs: [] };
  return {
    name: 'browse',
    async setup(client, log) {
      sample = await sampleCatalog(client, { category: opts.category });
      log(`sampled ${sample.ids.length} product ids across ${sample.slugs.length} categories (catalog total ${sample.total})`);
    },
    next(rng) {
      return pickWeighted(rng, mix) === 'list' ? listRequest(rng, sample, opts) : detailRequest(rng, sample);
    },
  };
}
