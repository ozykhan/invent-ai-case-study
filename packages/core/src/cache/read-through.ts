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
  /**
   * The caller's interest in the result (e.g. the request's client is still connected). A waiter
   * checks it after every poll interval and, once it has aborted, stops waiting and throws
   * `signal.reason` without building or taking over the lock. The builder checks it itself.
   */
  signal?: AbortSignal;
  onError?: (err: unknown) => void;
}

/**
 * A waiter gave up on the key's lock holder. It did not build, so an overloaded origin isn't handed
 * one more build per waiter; the caller should shed the request (e.g. 503 and retry).
 */
export class CacheWaitTimeoutError extends Error {
  /** `waitedMs` is the time actually spent waiting, which can overrun the configured waitMs by one poll. */
  constructor(public readonly key: string, public readonly waitedMs: number) {
    super(`gave up after ${waitedMs} ms waiting for another request to build cache key ${key}`);
    this.name = 'CacheWaitTimeoutError';
  }
}

// Compare-and-delete: only the holder whose token is still in the lock may release it. A holder
// whose lock expired must not delete the lock a newer holder took since.
const RELEASE_LOCK = `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`;
// Compare-and-set: the holder built with `cache: false`, so there will be no entry to wait for. It
// swaps its token for this marker, briefly, and waiters that see it build directly.
const UNCACHED = 'uncached';
const UNCACHED_MARK_MS = 1000;
const MARK_UNCACHED = `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('set', KEYS[1], ARGV[2], 'PX', ARGV[3]) else return 0 end`;

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
 * - the holder built with `cache: false` (it leaves an "uncached" marker in the lock): the waiter
 *   builds directly, since no entry is coming;
 * - the lock is gone with no entry (the holder's build threw, its SET failed, or its lock
 *   expired): the waiter tries to take the lock over. One wins and builds as the new holder; the
 *   rest keep waiting for its entry. Letting them all build is what kept an overloaded cold start
 *   from recovering: each failed holder released a herd whose queries starved the next holder;
 * - `waitMs` passes: CacheWaitTimeoutError, with no build.
 */
export async function readThrough<T, E = undefined>(
  redis: Redis | null,
  key: string,
  build: () => Promise<BuildResult<T>>,
  opts: ReadThroughOptions<T, E> = {},
): Promise<CacheOutcome<T, E>> {
  const { lockMs = 30_000, waitMs = 5000, pollMs = 20, maxPollMs = 100, onError = () => {}, extraKeys = [], signal } = opts;
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
  // round trip, so a waiter can tell "still building" (lock held, no entry), "built without
  // caching" (the uncached marker) and "holder gone" (no lock, no entry) apart without a second call.
  const tryHitAndLock = async (): Promise<{ hit: { value: T; extra?: E } | undefined; lock: string | null }> => {
    const raw = await redis.mget(key, keys.lock(key), ...extraKeys);
    const lock = raw[1] ?? null;
    const rawEntry = raw[0];
    if (rawEntry === null || rawEntry === undefined) return { hit: undefined, lock };
    const value = JSON.parse(rawEntry) as T;
    if (opts.isFresh) {
      const result = await opts.isFresh(value, raw.slice(2));
      const check: FreshResult<E> = typeof result === 'boolean' ? { fresh: result } : result;
      if (!check.fresh) return { hit: undefined, lock };
      return { hit: { value, extra: check.extra }, lock };
    }
    return { hit: { value }, lock };
  };

  const lockKey = keys.lock(key);
  const tryLock = async (): Promise<string | undefined> => {
    const token = randomUUID();
    return (await redis.set(lockKey, token, 'PX', lockMs, 'NX')) === 'OK' ? token : undefined;
  };
  let lockToken: string | undefined;
  let waitedMs: number | undefined; // set when the wait deadline passed
  let abandoned = false; // set when the signal aborted while waiting
  try {
    const hit = await tryHit();
    if (hit !== undefined) return { value: hit.value, source: 'hit', extra: hit.extra };

    lockToken = await tryLock();
    if (lockToken === undefined) {
      const waitStart = Date.now();
      const deadline = waitStart + waitMs;
      let delay = pollMs;
      for (;;) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) {
          waitedMs = Date.now() - waitStart;
          break;
        }
        await sleep(Math.min(delay, remaining));
        delay = Math.min(maxPollMs, delay * 2);
        if (signal?.aborted) {
          abandoned = true;
          break;
        }
        const { hit: late, lock } = await tryHitAndLock();
        if (late !== undefined) return { value: late.value, source: 'hit', extra: late.extra };
        // The holder built with `cache: false`: no entry is coming, and waiting out the rest of
        // waitMs would only delay every waiter by the same amount. Build directly.
        if (lock === UNCACHED) break;
        // The holder is gone without an entry. Take over, so one waiter rebuilds, not all of them.
        if (lock === null) {
          lockToken = await tryLock();
          if (lockToken !== undefined) break;
        }
      }
    }
  } catch (err) {
    onError(err);
  }
  // Nobody wants the result any more: stop without building or taking the lock over.
  if (abandoned) signal!.throwIfAborted();
  // Fail closed: the holder is still building (or queued behind an overloaded origin). Building
  // here too is what turned a slow cold start into a collapse, so shed the request instead.
  if (waitedMs !== undefined) throw new CacheWaitTimeoutError(key, waitedMs);
  if (lockToken === undefined) return { value: (await build()).value, source: 'bypass' };

  // The lock holder. A build() error is the caller's (e.g. Postgres down), not cache degradation: it
  // propagates as is, and building again would only repeat it. Only the cache write is best effort.
  let uncached = false;
  try {
    const built = await build();
    if (built.cache !== false) {
      await redis.set(key, JSON.stringify(built.value), 'EX', Math.max(1, built.ttlSeconds)).catch(onError);
    } else {
      uncached = true;
    }
    return { value: built.value, source: 'built' };
  } finally {
    await (uncached
      ? redis.eval(MARK_UNCACHED, 1, lockKey, lockToken, UNCACHED, String(UNCACHED_MARK_MS))
      : redis.eval(RELEASE_LOCK, 1, lockKey, lockToken)
    ).catch(onError);
  }
}
