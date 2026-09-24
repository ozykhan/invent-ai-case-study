import type Redis from 'ioredis';
import { keys } from './keys';

export function parseVersion(raw: string | null | undefined): number {
  const n = Number(raw);
  return Number.isFinite(n) ? n : 0;
}

export async function getVersions(redis: Redis, versionKeys: string[]): Promise<number[]> {
  if (versionKeys.length === 0) return [];
  const raw = await redis.mget(...versionKeys);
  return raw.map(parseVersion);
}

type Log = (msg: string, err: unknown) => void;

/** Increment every key. Retries 3 times, then logs. Never throws: a missed bump self-heals via TTL. */
export async function bumpVersions(redis: Redis, versionKeys: string[], log: Log = () => {}): Promise<void> {
  if (versionKeys.length === 0) return;
  let lastErr: unknown;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const pipe = redis.pipeline();
      for (const k of versionKeys) pipe.incr(k);
      // ioredis resolves pipeline().exec() with [err, result] pairs (or null) instead of
      // rejecting when a queued command fails, so a dead-connection failure must be surfaced
      // manually to trigger the retry loop below.
      const results = await pipe.exec();
      if (!results) throw new Error('pipeline exec returned null');
      const failed = results.find(([err]) => err != null);
      if (failed) throw failed[0];
      return;
    } catch (err) {
      lastErr = err;
      await new Promise((r) => setTimeout(r, 50 * attempt));
    }
  }
  log(`version bump failed for ${versionKeys.join(',')}; stale until TTL`, lastErr);
}

export function bumpCategory(redis: Redis, categoryId: number, log?: Log): Promise<void> {
  return bumpVersions(redis, [keys.categoryVersion(categoryId), keys.allVersion()], log);
}

export function bumpProduct(redis: Redis, productId: number, log?: Log): Promise<void> {
  return bumpVersions(redis, [keys.productVersion(productId)], log);
}
