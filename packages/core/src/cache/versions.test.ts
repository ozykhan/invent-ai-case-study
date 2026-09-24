import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { keys } from './keys';
import { createRedis } from './redis';
import { bumpCategory, bumpProduct, bumpVersions, getVersions, parseVersion } from './versions';

const redis = createRedis(process.env.REDIS_URL ?? 'redis://localhost:6379');
beforeAll(() => redis.connect());
beforeEach(() => redis.flushdb());
afterAll(() => redis.quit());

describe('parseVersion', () => {
  it('parses numeric strings and defaults missing/invalid values to 0', () => {
    expect(parseVersion('5')).toBe(5);
    expect(parseVersion(null)).toBe(0);
    expect(parseVersion(undefined)).toBe(0);
    expect(parseVersion('not-a-number')).toBe(0);
  });
});

describe('bumpVersions', () => {
  it('retries against a dead client, never throws, and logs exactly once', async () => {
    const dead = createRedis('redis://localhost:1');
    dead.on('error', () => {}); // ioredis emits 'error' events; unhandled ones would throw
    const log = vi.fn();

    await expect(bumpVersions(dead, ['ver:x'], log)).resolves.toBeUndefined();

    expect(log).toHaveBeenCalledTimes(1);
    const [msg, err] = log.mock.calls[0]!;
    expect(msg).toContain('ver:x');
    expect(err).toBeDefined();

    dead.disconnect();
  });

  it('round-trips against real redis: bumpCategory/bumpProduct are reflected by getVersions', async () => {
    await bumpCategory(redis, 3);
    await bumpProduct(redis, 7);
    await bumpCategory(redis, 3);

    const [categoryV, allV, productV, missingV] = await getVersions(redis, [
      keys.categoryVersion(3),
      keys.allVersion(),
      keys.productVersion(7),
      keys.productVersion(999),
    ]);

    expect(categoryV).toBe(2);
    expect(allV).toBe(2);
    expect(productV).toBe(1);
    expect(missingV).toBe(0);
  });
});
