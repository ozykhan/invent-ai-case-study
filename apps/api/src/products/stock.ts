import { fetchStock, keys, STOCK_TTL_SECONDS, throwOnPipelineError } from '@modaco/core';
import type { AppDeps } from '../deps';

/** Redis counters first, Postgres for misses, empty counters backfilled. Redis failure means Postgres only. */
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
      // NX: this value was read before any write-through that may have landed since the MGET miss, so it
      // only fills an empty key and never overwrites a newer counter. A failed backfill just leaves the key
      // empty, which the next read handles the same way.
      const pipe = deps.redis.pipeline();
      for (const [id, stock] of fromDb) pipe.set(keys.stock(id), String(stock), 'EX', STOCK_TTL_SECONDS, 'NX');
      await pipe.exec().then(throwOnPipelineError).catch((err) => deps.logger.warn({ err }, 'stock backfill failed'));
    }
  }
  return out;
}

/**
 * Write-through after the database commit. If the SET fails, the old counter would keep being served, so
 * the key is deleted instead (best effort) and the next read falls back to Postgres and backfills.
 */
export async function setStock(deps: AppDeps, id: number, stock: number): Promise<void> {
  const redis = deps.redis;
  if (!redis) return;
  await redis.set(keys.stock(id), String(stock), 'EX', STOCK_TTL_SECONDS).catch(async (err) => {
    deps.logger.warn({ err, id }, 'stock counter set failed; dropping the counter');
    await redis.del(keys.stock(id)).catch((delErr) => deps.logger.warn({ err: delErr, id }, 'stock counter delete failed; stale until TTL'));
  });
}
