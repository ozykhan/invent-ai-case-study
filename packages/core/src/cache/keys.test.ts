import { describe, expect, it } from 'vitest';
import { DEFAULT_TTL_SECONDS, keys, ttlSeconds } from './keys';

describe('keys', () => {
  it('builds stable keys', () => {
    expect(keys.productVersion(7)).toBe('ver:product:7');
    expect(keys.categoryVersion(3)).toBe('ver:category:3');
    expect(keys.allVersion()).toBe('ver:all');
    expect(keys.product(7)).toBe('product:7');
    expect(keys.list(3, 'asc', 2, 20)).toBe('list:3:asc:2:20');
    expect(keys.list(null, 'desc', 1, 50)).toBe('list:all:desc:1:50');
    expect(keys.categorySlug('accessories')).toBe('category:slug:accessories');
    expect(keys.stock(7)).toBe('stock:7');
    expect(keys.lock('product:7')).toBe('lock:product:7');
  });
});

describe('ttlSeconds', () => {
  const now = new Date('2026-06-15T12:00:00Z');
  it('uses the default when nothing is scheduled', () => {
    expect(ttlSeconds(now, null)).toBe(DEFAULT_TTL_SECONDS);
  });
  it('caps at the next boundary, rounded up, minimum 1', () => {
    expect(ttlSeconds(now, new Date(now.getTime() + 90_500))).toBe(91);
    expect(ttlSeconds(now, new Date(now.getTime() + 10))).toBe(1);
    expect(ttlSeconds(now, new Date(now.getTime() - 1000))).toBe(1);
  });
  it('never exceeds the max', () => {
    expect(ttlSeconds(now, new Date(now.getTime() + 3_600_000))).toBe(DEFAULT_TTL_SECONDS);
    expect(ttlSeconds(now, null, 60)).toBe(60);
  });
});
