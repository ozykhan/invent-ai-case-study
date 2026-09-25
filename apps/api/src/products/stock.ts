import { fetchStock, keys, STOCK_TTL_SECONDS, throwOnPipelineError } from '@modaco/core';
import type { AppDeps } from '../deps';

/** Redis counters first, Postgres for misses, counters backfilled. Redis failure means Postgres only. */
export async function loadStocks(deps: AppDeps, ids: number[]): Promise<Map<number, number>> {
  const out = new Map<number, number>();
  if (ids.length === 0) return out;
  let missing = ids;
  if (deps.redis) {
    try {
      const raw = await deps.redis.mget(...ids.map(keys.stock));
      missing = [];
      ids.forEach((id, i) => {
        const v = raw[i];
        if (v === null || v === undefined) missing.push(id); else out.set(id, Number(v));
      });
    } catch (err) {
      deps.logger.warn({ err }, 'stock mget failed; falling back to postgres');
      missing = ids;
    }
  }
  if (missing.length > 0) {
    const fromDb = await fetchStock(deps.db, missing);
    for (const [id, stock] of fromDb) out.set(id, stock);
    if (deps.redis && fromDb.size > 0) {
      const pipe = deps.redis.pipeline();
      for (const [id, stock] of fromDb) pipe.set(keys.stock(id), String(stock), 'EX', STOCK_TTL_SECONDS);
      await pipe.exec().then(throwOnPipelineError).catch((err) => deps.logger.warn({ err }, 'stock backfill failed'));
    }
  }
  return out;
}

export async function setStock(deps: AppDeps, id: number, stock: number): Promise<void> {
  if (!deps.redis) return;
  await deps.redis.set(keys.stock(id), String(stock), 'EX', STOCK_TTL_SECONDS)
    .catch((err) => deps.logger.warn({ err, id }, 'stock counter set failed'));
}
