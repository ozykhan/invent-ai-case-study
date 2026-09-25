import { UsageError } from '../../errors';
import { BROWSE_LABELS, createBrowse } from './browse';
import type { Scenario, ScenarioOptions } from './types';

interface ScenarioDef {
  /** Labels --mix may weight; empty when the scenario takes no mix. */
  labels: readonly string[];
  create(opts: ScenarioOptions): Scenario;
}

export const SCENARIOS: Record<string, ScenarioDef> = {
  browse: { labels: BROWSE_LABELS, create: createBrowse },
};

export function createScenario(name: string, opts: ScenarioOptions): Scenario {
  const def = SCENARIOS[name];
  if (!def) throw new UsageError(`unknown scenario '${name}' (available: ${Object.keys(SCENARIOS).join(', ')})`);
  return def.create(opts);
}
