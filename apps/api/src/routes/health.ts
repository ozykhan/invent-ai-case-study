import { Router } from 'express';
import { sql } from 'drizzle-orm';
import type { AppDeps } from '../deps';

export function healthRoutes(deps: AppDeps): Router {
  const r = Router();
  r.get('/health', async (_req, res) => {
    const checks = { postgres: false, redis: false };
    try { await deps.db.execute(sql`select 1`); checks.postgres = true; } catch { /* reported below */ }
    try { if (deps.redis && (await deps.redis.ping()) === 'PONG') checks.redis = true; } catch { /* reported below */ }
    res.status(checks.postgres ? 200 : 503).json({ status: checks.postgres ? 'ok' : 'degraded', checks });
  });
  return r;
}
