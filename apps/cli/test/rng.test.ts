import { describe, expect, it } from 'vitest';
import { createRng, pickWeighted } from '../src/load/rng';

describe('createRng', () => {
  it('is deterministic for a seed', () => {
    const a = createRng(42);
    const b = createRng(42);
    const xs = Array.from({ length: 5 }, () => a.next());
    expect(Array.from({ length: 5 }, () => b.next())).toEqual(xs);
    expect(createRng(43).next()).not.toBe(xs[0]);
  });
  it('keeps int() within inclusive bounds and pick() within the list', () => {
    const rng = createRng(1);
    const seen = new Set<number>();
    for (let i = 0; i < 1000; i++) seen.add(rng.int(1, 5));
    expect([...seen].sort()).toEqual([1, 2, 3, 4, 5]);
    expect(['a', 'b']).toContain(rng.pick(['a', 'b']));
    expect(() => rng.pick([])).toThrow();
  });
});

describe('pickWeighted', () => {
  it('follows the weights and never picks a zero weight', () => {
    const rng = createRng(7);
    const counts: Record<string, number> = { a: 0, b: 0, c: 0 };
    for (let i = 0; i < 100_000; i++) counts[pickWeighted(rng, { a: 70, b: 30, c: 0 })]!++;
    expect(counts.a! / 100_000).toBeCloseTo(0.7, 1);
    expect(counts.b! / 100_000).toBeCloseTo(0.3, 1);
    expect(counts.c).toBe(0);
  });
});
