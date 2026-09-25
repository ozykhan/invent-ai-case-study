import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { createRedis } from './redis';
import { CacheWaitTimeoutError, readThrough } from './read-through';

const redis = createRedis(process.env.REDIS_URL ?? 'redis://localhost:6379');
beforeAll(() => redis.connect());
beforeEach(() => redis.flushdb());
afterAll(() => redis.quit());

describe('readThrough', () => {
  it('builds on miss, hits afterwards, and honours the TTL', async () => {
    let builds = 0;
    const build = async () => { builds++; return { value: { n: 1 }, ttlSeconds: 30 }; };
    const a = await readThrough(redis, 'k', build);
    expect(a).toEqual({ value: { n: 1 }, source: 'built' });
    const b = await readThrough(redis, 'k', build);
    expect(b).toEqual({ value: { n: 1 }, source: 'hit' });
    expect(builds).toBe(1);
    expect(await redis.ttl('k')).toBeGreaterThan(25);
  });

  it('treats a stale entry as a miss and overwrites it', async () => {
    await redis.set('k', JSON.stringify({ v: 1 }));
    const res = await readThrough(redis, 'k', async () => ({ value: { v: 2 }, ttlSeconds: 10 }), {
      isFresh: async (value) => (value as { v: number }).v === 2,
    });
    expect(res).toEqual({ value: { v: 2 }, source: 'built' });
    expect(JSON.parse((await redis.get('k'))!)).toEqual({ v: 2 });
  });

  it('coalesces concurrent misses into one build', async () => {
    let builds = 0;
    const build = async () => {
      builds++;
      await new Promise((r) => setTimeout(r, 100));
      return { value: 'x', ttlSeconds: 10 };
    };
    const results = await Promise.all(Array.from({ length: 8 }, () => readThrough(redis, 'k', build)));
    expect(results.every((r) => r.value === 'x')).toBe(true);
    expect(builds).toBe(1);
    expect(results.filter((r) => r.source === 'built')).toHaveLength(1);
    expect(results.filter((r) => r.source === 'hit')).toHaveLength(7);
  });

  it('stops waiters as soon as the lock is released without an entry being written, instead of polling out waitMs', async () => {
    let builds = 0;
    const build = async () => {
      builds++;
      await new Promise((r) => setTimeout(r, 20));
      return { value: 'empty', ttlSeconds: 10, cache: false };
    };
    const start = Date.now();
    const results = await Promise.all(Array.from({ length: 10 }, () => readThrough(redis, 'k', build, { waitMs: 1000, pollMs: 20 })));
    const elapsed = Date.now() - start;
    expect(results.every((r) => r.value === 'empty')).toBe(true);
    expect(builds).toBe(10); // no coalescing possible: nothing is ever written for the others to hit
    expect(elapsed).toBeLessThan(500); // well under waitMs (1000ms): waiters stop polling long before the deadline
    expect(await redis.get('k')).toBeNull();
  });

  it('coalesces a slow (~400 ms) holder build across 20 concurrent waiters with the default options', async () => {
    let builds = 0;
    const build = async () => {
      builds++;
      await new Promise((r) => setTimeout(r, 400));
      return { value: 'slow', ttlSeconds: 10 };
    };
    const results = await Promise.all(Array.from({ length: 21 }, () => readThrough(redis, 'k', build)));
    expect(builds).toBe(1);
    expect(results.filter((r) => r.source === 'built')).toHaveLength(1);
    expect(results.filter((r) => r.source === 'hit')).toHaveLength(20);
    expect(results.every((r) => r.value === 'slow')).toBe(true);
  });

  it('fails closed: a waiter whose deadline passes while the lock is held throws CacheWaitTimeoutError and never builds', async () => {
    await redis.set('lock:k', 'someone-else', 'PX', 5000);
    const build = vi.fn(async () => ({ value: 'y', ttlSeconds: 10 }));
    const start = Date.now();
    const err = await readThrough(redis, 'k', build, { waitMs: 150 }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(CacheWaitTimeoutError);
    expect((err as CacheWaitTimeoutError).key).toBe('k');
    expect(Date.now() - start).toBeGreaterThanOrEqual(140);
    expect(build).not.toHaveBeenCalled();
    expect(await redis.get('k')).toBeNull();
    expect(await redis.get('lock:k')).toBe('someone-else');
  });

  it('backs off while polling, so a long wait costs a handful of round trips rather than one every 20 ms', async () => {
    await redis.set('lock:k', 'someone-else', 'PX', 5000);
    const mgetSpy = vi.spyOn(redis, 'mget');
    try {
      await expect(readThrough(redis, 'k', async () => ({ value: 'y', ttlSeconds: 10 }), { waitMs: 1000 }))
        .rejects.toBeInstanceOf(CacheWaitTimeoutError);
      // A fixed 20 ms poll would be ~50 MGETs over 1 s; 20 ms growing to a 100 ms cap is ~13.
      expect(mgetSpy.mock.calls.length).toBeLessThanOrEqual(15);
      expect(mgetSpy.mock.calls.length).toBeGreaterThanOrEqual(5);
    } finally {
      mgetSpy.mockRestore();
    }
  });

  it('an old holder whose lock expired mid-build never deletes the lock a newer holder took', async () => {
    const res = readThrough(redis, 'k', async () => {
      await new Promise((r) => setTimeout(r, 250));
      return { value: 'old', ttlSeconds: 10 };
    }, { lockMs: 100 });
    await new Promise((r) => setTimeout(r, 150)); // the old holder's 100 ms lock has expired
    expect(await redis.set('lock:k', 'newer-holder', 'PX', 5000, 'NX')).toBe('OK');
    expect(await res).toEqual({ value: 'old', source: 'built' });
    expect(await redis.get('lock:k')).toBe('newer-holder');
  });

  it('when the holder build fails, one waiter takes the lock over and builds; the rest wait for its entry', async () => {
    let builds = 0;
    const build = async () => {
      const n = ++builds;
      await new Promise((r) => setTimeout(r, 100));
      if (n === 1) throw new Error('pool acquire timeout');
      return { value: 'second', ttlSeconds: 10 };
    };
    const results = await Promise.allSettled(Array.from({ length: 11 }, () => readThrough(redis, 'k', build)));
    expect(builds).toBe(2);
    expect(results.filter((r) => r.status === 'rejected')).toHaveLength(1);
    const values = results.flatMap((r) => (r.status === 'fulfilled' ? [r.value] : []));
    expect(values.filter((v) => v.source === 'built')).toHaveLength(1);
    expect(values.filter((v) => v.source === 'hit')).toHaveLength(9);
    expect(await redis.get('lock:k')).toBeNull();
  });

  it('when the holder lock expires mid-build, one waiter takes it over instead of every waiter building', async () => {
    let builds = 0;
    const build = async () => {
      builds++;
      await new Promise((r) => setTimeout(r, 300));
      return { value: 'v', ttlSeconds: 10 };
    };
    const first = readThrough(redis, 'k', build, { lockMs: 100 }); // this holder's lock expires under it
    await new Promise((r) => setTimeout(r, 10));
    const results = await Promise.all([first, ...Array.from({ length: 10 }, () => readThrough(redis, 'k', build))]);
    expect(builds).toBe(2);
    expect(results.every((r) => r.value === 'v')).toBe(true);
    expect(results.filter((r) => r.source === 'hit')).toHaveLength(9);
  });

  it('waiters still build directly when a poll hits a redis error', async () => {
    await redis.set('lock:k', 'someone-else', 'PX', 5000);
    const realMget = redis.mget.bind(redis) as (...args: unknown[]) => Promise<unknown>;
    let calls = 0;
    const mget = vi.spyOn(redis, 'mget').mockImplementation(((...args: unknown[]) =>
      ++calls >= 2 ? Promise.reject(new Error('redis gone')) : realMget(...args)) as never);
    const errors: unknown[] = [];
    try {
      const res = await readThrough(redis, 'k', async () => ({ value: 'direct', ttlSeconds: 10 }), { onError: (e) => errors.push(e) });
      expect(res).toEqual({ value: 'direct', source: 'bypass' });
    } finally {
      mget.mockRestore();
    }
    expect(errors).toEqual([expect.objectContaining({ message: 'redis gone' })]);
  });

  it('falls through to the builder when redis is unavailable', async () => {
    const dead = createRedis('redis://localhost:1');
    dead.on('error', () => {}); // ioredis emits 'error' events; unhandled ones would throw
    const errors: unknown[] = [];
    const res = await readThrough(dead, 'k', async () => ({ value: 'z', ttlSeconds: 10 }), { onError: (e) => errors.push(e) });
    expect(res).toEqual({ value: 'z', source: 'bypass' });
    expect(errors.length).toBeGreaterThan(0);
    dead.disconnect();
  });

  it('works with a null redis', async () => {
    expect(await readThrough(null, 'k', async () => ({ value: 1, ttlSeconds: 1 }))).toEqual({ value: 1, source: 'bypass' });
  });

  it('fetches extraKeys in the same round trip as the entry and hands them to isFresh', async () => {
    await redis.set('k', JSON.stringify({ v: 1 }));
    await redis.set('ver', '7');
    const mgetSpy = vi.spyOn(redis, 'mget');
    const getSpy = vi.spyOn(redis, 'get');
    const res = await readThrough(redis, 'k', async () => ({ value: { v: 99 }, ttlSeconds: 10 }), {
      extraKeys: ['ver'],
      isFresh: async (value, extraRaw) => extraRaw[0] === '7' && (value as { v: number }).v === 1,
    });
    expect(res).toEqual({ value: { v: 1 }, source: 'hit' });
    expect(getSpy).not.toHaveBeenCalled();
    expect(mgetSpy).toHaveBeenCalledTimes(1);
    expect(mgetSpy).toHaveBeenCalledWith('k', 'ver');
    mgetSpy.mockRestore();
    getSpy.mockRestore();
  });

  it('lets isFresh hand extra data back to the caller alongside a hit', async () => {
    await redis.set('k', JSON.stringify({ v: 1 }));
    const res = await readThrough<{ v: number }, string>(redis, 'k', async () => ({ value: { v: 1 }, ttlSeconds: 10 }), {
      isFresh: async () => ({ fresh: true, extra: 'side-channel' }),
    });
    expect(res).toEqual({ value: { v: 1 }, source: 'hit', extra: 'side-channel' });
  });

  it('skips writing to the cache when the builder marks the value cache: false', async () => {
    const res = await readThrough(redis, 'k', async () => ({ value: { v: 1 }, ttlSeconds: 10, cache: false }), {});
    expect(res).toEqual({ value: { v: 1 }, source: 'built' });
    expect(await redis.get('k')).toBeNull();
  });

  it('returns the built value without rebuilding when only the cache SET fails', async () => {
    const realSet = redis.set.bind(redis) as (...args: unknown[]) => Promise<unknown>;
    const set = vi.spyOn(redis, 'set').mockImplementation(((...args: unknown[]) =>
      args[0] === 'k' ? Promise.reject(new Error('set failed')) : realSet(...args)) as never);
    const build = vi.fn(async () => ({ value: { v: 1 }, ttlSeconds: 10 }));
    const errors: unknown[] = [];
    try {
      const res = await readThrough(redis, 'k', build, { onError: (e) => errors.push(e) });
      expect(res).toEqual({ value: { v: 1 }, source: 'built' });
    } finally {
      set.mockRestore();
    }
    expect(build).toHaveBeenCalledTimes(1);
    expect(errors).toEqual([expect.objectContaining({ message: 'set failed' })]);
    expect(await redis.get('lock:k')).toBeNull();
  });

  it('propagates a build error from the lock holder without rebuilding or reporting cache degradation', async () => {
    const build = vi.fn(async (): Promise<{ value: string; ttlSeconds: number }> => { throw new Error('db down'); });
    const onError = vi.fn();
    await expect(readThrough(redis, 'k', build, { onError })).rejects.toThrow('db down');
    expect(build).toHaveBeenCalledTimes(1);
    expect(onError).not.toHaveBeenCalled();
    expect(await redis.get('lock:k')).toBeNull();
  });

  it('falls back to the builder when a command stalls past commandTimeout', async () => {
    // CLIENT PAUSE stalls the server's replies to every connection, including new ones, for
    // pauseMs; createRedis's commandTimeout (300ms) should reject `redis`'s GET client-side well
    // before that. Note: CLIENT UNPAUSE is itself blocked by an active pause (verified against
    // the compose Redis), so we can't lift the pause early — instead we pause for just longer
    // than commandTimeout and let it expire naturally before the next test's beforeEach runs.
    const pauser = createRedis('redis://localhost:6379');
    await pauser.connect();
    const pauseMs = 450;
    const pauseStart = Date.now();
    await pauser.call('CLIENT', 'PAUSE', String(pauseMs));
    try {
      const errors: unknown[] = [];
      const start = Date.now();
      const res = await readThrough(redis, 'k', async () => ({ value: 'paused', ttlSeconds: 10 }), {
        onError: (e) => errors.push(e),
      });
      expect(res).toEqual({ value: 'paused', source: 'bypass' });
      expect(errors.length).toBeGreaterThan(0);
      expect(Date.now() - start).toBeLessThan(pauseMs);
    } finally {
      const remaining = pauseMs - (Date.now() - pauseStart) + 50;
      if (remaining > 0) await new Promise((r) => setTimeout(r, remaining));
      await pauser.quit();
    }
  });
});
