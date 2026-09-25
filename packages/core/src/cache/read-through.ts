import type Redis from 'ioredis';
import { keys } from './keys';

export interface FreshResult<E = undefined> {
  fresh: boolean;
  extra?: E;
}

export interface ReadThroughOptions<T, E = undefined> {
  /**
   * Rejects a hit whose embedded version numbers are stale. Receives the raw values of
   * `extraKeys`, fetched in the same round trip as the entry, so a version check doesn't need one
   * of its own. May return a plain boolean, or `{ fresh, extra }` to hand data it fetched itself
   * (e.g. a live counter) back to the caller alongside the hit, avoiding a second lookup.
   */
  isFresh?: (value: T, extraRaw: (string | null | undefined)[]) => Promise<boolean | FreshResult<E>>;
  /** Keys MGET'd alongside the entry key on every read attempt, in `isFresh`'s `extraRaw`. */
  extraKeys?: string[];
  lockMs?: number;
  waitMs?: number;
  pollMs?: number;
  onError?: (err: unknown) => void;
}

export interface BuildResult<T> {
  value: T;
  ttlSeconds: number;
  /** false skips writing this value to the cache — e.g. it was built from unverifiable inputs (a
   * version read failed) or from data that no longer matches the version it would be stamped
   * with. Defaults to true. */
  cache?: boolean;
}

export interface CacheOutcome<T, E = undefined> {
  value: T;
  source: 'hit' | 'built' | 'bypass';
  extra?: E;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Cache-aside read with request coalescing. Any Redis failure degrades to a direct build.
 * `isFresh` lets the caller reject an entry whose embedded version numbers are stale.
 */
export async function readThrough<T, E = undefined>(
  redis: Redis | null,
  key: string,
  build: () => Promise<BuildResult<T>>,
  opts: ReadThroughOptions<T, E> = {},
): Promise<CacheOutcome<T, E>> {
  const { lockMs = 2000, waitMs = 200, pollMs = 20, onError = () => {}, extraKeys = [] } = opts;
  if (!redis) return { value: (await build()).value, source: 'bypass' };

  const tryHit = async (): Promise<{ value: T; extra?: E } | undefined> => {
    const raw = extraKeys.length > 0 ? await redis.mget(key, ...extraKeys) : [await redis.get(key)];
    const rawEntry = raw[0];
    if (rawEntry === null || rawEntry === undefined) return undefined;
    const value = JSON.parse(rawEntry) as T;
    if (opts.isFresh) {
      const result = await opts.isFresh(value, raw.slice(1));
      const check: FreshResult<E> = typeof result === 'boolean' ? { fresh: result } : result;
      if (!check.fresh) return undefined;
      return { value, extra: check.extra };
    }
    return { value };
  };

  try {
    const hit = await tryHit();
    if (hit !== undefined) return { value: hit.value, source: 'hit', extra: hit.extra };

    const lockKey = keys.lock(key);
    const locked = await redis.set(lockKey, '1', 'PX', lockMs, 'NX');
    if (locked === 'OK') {
      try {
        const built = await build();
        if (built.cache !== false) {
          await redis.set(key, JSON.stringify(built.value), 'EX', Math.max(1, built.ttlSeconds));
        }
        return { value: built.value, source: 'built' };
      } finally {
        await redis.del(lockKey).catch(onError);
      }
    }

    const deadline = Date.now() + waitMs;
    while (Date.now() < deadline) {
      await sleep(pollMs);
      const late = await tryHit();
      if (late !== undefined) return { value: late.value, source: 'hit', extra: late.extra };
    }
  } catch (err) {
    onError(err);
  }
  return { value: (await build()).value, source: 'bypass' };
}
