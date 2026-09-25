import { UsageError } from '../../errors';
import { BROWSE_LABELS, createBrowse } from './browse';
import { createFlashSale } from './flash-sale';
import type { Scenario, ScenarioOptions } from './types';
import { WRITE_MIX_LABELS, createWriteMix } from './write-mix';

interface ScenarioDef {
  /** Labels --mix may weight; empty when the scenario takes no mix. */
  labels: readonly string[];
  create(opts: ScenarioOptions): Scenario;
}

export const SCENARIOS: Record<string, ScenarioDef> = {
  browse: { labels: BROWSE_LABELS, create: createBrowse },
  'write-mix': { labels: WRITE_MIX_LABELS, create: createWriteMix },
  'flash-sale': { labels: [], create: createFlashSale },
};

export function createScenario(name: string, opts: ScenarioOptions): Scenario {
  const def = SCENARIOS[name];
  if (!def) throw new UsageError(`unknown scenario '${name}' (available: ${Object.keys(SCENARIOS).join(', ')})`);
  return def.create(opts);
}
