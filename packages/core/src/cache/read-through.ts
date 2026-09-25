import { randomUUID } from 'node:crypto';
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
  /**
   * TTL of the build lock. It only matters if a holder dies without releasing it, and it must stay
   * above the worst-case build time: a lock that expires under a live holder lets a second holder
   * in and tells every waiter to build (the cold-start collapse this replaced). Callers bound their
   * builds (pool acquire timeout plus statement_timeout) to keep that true.
   */
  lockMs?: number;
  /** How long a waiter waits for the holder before failing with CacheWaitTimeoutError. */
  waitMs?: number;
  /** First poll interval. Each poll doubles it, up to maxPollMs. */
  pollMs?: number;
  maxPollMs?: number;
  onError?: (err: unknown) => void;
}

/**
 * A waiter gave up on the key's lock holder. It did not build, so an overloaded origin isn't handed
 * one more build per waiter; the caller should shed the request (e.g. 503 and retry).
 */
export class CacheWaitTimeoutError extends Error {
  constructor(public readonly key: string, public readonly waitedMs: number) {
    super(`gave up after ${waitedMs} ms waiting for another request to build cache key ${key}`);
    this.name = 'CacheWaitTimeoutError';
  }
}

// Compare-and-delete: only the holder whose token is still in the lock may release it. A holder
// whose lock expired must not delete the lock a newer holder took since.
const RELEASE_LOCK = `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`;

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
 *
 * Coalescing fails closed. A miss takes the key's lock (a random token, `lockMs` TTL) and builds;
 * every other miss polls, with backoff, until one of:
 * - a fresh entry appears: a hit;
 * - the lock is released without an entry (the holder built with `cache: false`, its SET failed,
 *   or its build threw): the waiter builds directly;
 * - `waitMs` passes: CacheWaitTimeoutError, with no build.
 */
export async function readThrough<T, E = undefined>(
  redis: Redis | null,
  key: string,
  build: () => Promise<BuildResult<T>>,
  opts: ReadThroughOptions<T, E> = {},
): Promise<CacheOutcome<T, E>> {
  const { lockMs = 30_000, waitMs = 5000, pollMs = 20, maxPollMs = 100, onError = () => {}, extraKeys = [] } = opts;
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

  // Used while polling as a non-holder: fetches the entry, the lock, and extraKeys together in one
  // round trip, so a waiter can tell "still building" (lock held, no entry) apart from "the holder
  // decided not to cache" (lock released, no entry) without a second call.
  const tryHitAndLock = async (): Promise<{ hit: { value: T; extra?: E } | undefined; lockHeld: boolean }> => {
    const raw = await redis.mget(key, keys.lock(key), ...extraKeys);
    const lockHeld = raw[1] !== null && raw[1] !== undefined;
    const rawEntry = raw[0];
    if (rawEntry === null || rawEntry === undefined) return { hit: undefined, lockHeld };
    const value = JSON.parse(rawEntry) as T;
    if (opts.isFresh) {
      const result = await opts.isFresh(value, raw.slice(2));
      const check: FreshResult<E> = typeof result === 'boolean' ? { fresh: result } : result;
      if (!check.fresh) return { hit: undefined, lockHeld };
      return { hit: { value, extra: check.extra }, lockHeld };
    }
    return { hit: { value }, lockHeld };
  };

  const lockKey = keys.lock(key);
  let lockToken: string | undefined;
  let timedOut = false;
  try {
    const hit = await tryHit();
    if (hit !== undefined) return { value: hit.value, source: 'hit', extra: hit.extra };

    const token = randomUUID();
    const locked = await redis.set(lockKey, token, 'PX', lockMs, 'NX');
    if (locked === 'OK') {
      lockToken = token;
    } else {
      const deadline = Date.now() + waitMs;
      let delay = pollMs;
      for (;;) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) {
          timedOut = true;
          break;
        }
        await sleep(Math.min(delay, remaining));
        delay = Math.min(maxPollMs, delay * 2);
        const { hit: late, lockHeld } = await tryHitAndLock();
        if (late !== undefined) return { value: late.value, source: 'hit', extra: late.extra };
        // The holder released the lock without writing an entry (e.g. it built with `cache:
        // false`). Waiting out the rest of waitMs would only delay every waiter by the same
        // amount; build directly instead, same as the no-lock-acquired bypass path below.
        if (!lockHeld) break;
      }
    }
  } catch (err) {
    onError(err);
  }
  // Fail closed: the holder is still building (or queued behind an overloaded origin). Building
  // here too is what turned a slow cold start into a collapse, so shed the request instead.
  if (timedOut) throw new CacheWaitTimeoutError(key, waitMs);
  if (lockToken === undefined) return { value: (await build()).value, source: 'bypass' };

  // The lock holder. A build() error is the caller's (e.g. Postgres down), not cache degradation: it
  // propagates as is, and building again would only repeat it. Only the cache write is best effort.
  try {
    const built = await build();
    if (built.cache !== false) {
      await redis.set(key, JSON.stringify(built.value), 'EX', Math.max(1, built.ttlSeconds)).catch(onError);
    }
    return { value: built.value, source: 'built' };
  } finally {
    await redis.eval(RELEASE_LOCK, 1, lockKey, lockToken).catch(onError);
  }
}
