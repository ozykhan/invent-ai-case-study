import type Redis from 'ioredis';
import { keys } from './keys';

export interface ReadThroughOptions<T> {
  isFresh?: (value: T) => Promise<boolean>;
  lockMs?: number;
  waitMs?: number;
  pollMs?: number;
  onError?: (err: unknown) => void;
}

export interface CacheOutcome<T> {
  value: T;
  source: 'hit' | 'built' | 'bypass';
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Cache-aside read with request coalescing. Any Redis failure degrades to a direct build.
 * `isFresh` lets the caller reject an entry whose embedded version numbers are stale.
 */
export async function readThrough<T>(
  redis: Redis | null,
  key: string,
  build: () => Promise<{ value: T; ttlSeconds: number }>,
  opts: ReadThroughOptions<T> = {},
): Promise<CacheOutcome<T>> {
  const { lockMs = 2000, waitMs = 200, pollMs = 20, onError = () => {} } = opts;
  if (!redis) return { value: (await build()).value, source: 'bypass' };

  const tryHit = async (): Promise<T | undefined> => {
    const raw = await redis.get(key);
    if (raw === null) return undefined;
    const value = JSON.parse(raw) as T;
    if (opts.isFresh && !(await opts.isFresh(value))) return undefined;
    return value;
  };

  try {
    const hit = await tryHit();
    if (hit !== undefined) return { value: hit, source: 'hit' };

    const lockKey = keys.lock(key);
    const locked = await redis.set(lockKey, '1', 'PX', lockMs, 'NX');
    if (locked === 'OK') {
      try {
        const built = await build();
        await redis.set(key, JSON.stringify(built.value), 'EX', Math.max(1, built.ttlSeconds));
        return { value: built.value, source: 'built' };
      } finally {
        await redis.del(lockKey).catch(onError);
      }
    }

    const deadline = Date.now() + waitMs;
    while (Date.now() < deadline) {
      await sleep(pollMs);
      const late = await tryHit();
      if (late !== undefined) return { value: late, source: 'hit' };
    }
  } catch (err) {
    onError(err);
  }
  return { value: (await build()).value, source: 'bypass' };
}
