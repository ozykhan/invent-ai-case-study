import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createRedis } from './redis';
import { readThrough } from './read-through';

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

  it('bypasses the cache when the lock holder is slow', async () => {
    await redis.set('lock:k', '1', 'PX', 5000);
    const res = await readThrough(redis, 'k', async () => ({ value: 'y', ttlSeconds: 10 }), { waitMs: 50, pollMs: 10 });
    expect(res).toEqual({ value: 'y', source: 'bypass' });
    expect(await redis.get('k')).toBeNull();
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
});
