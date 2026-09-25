export interface Rng {
  /** Uniform in [0, 1). */
  next(): number;
  /** Uniform integer in [min, max], both inclusive. */
  int(min: number, max: number): number;
  pick<T>(xs: readonly T[]): T;
}

/** mulberry32: small, fast and seedable, so a run's request sequence is repeatable with --seed. */
export function createRng(seed: number): Rng {
  let a = seed >>> 0;
  const next = (): number => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  return {
    next,
    int: (min, max) => min + Math.floor(next() * (max - min + 1)),
    pick: <T>(xs: readonly T[]): T => {
      if (xs.length === 0) throw new Error('cannot pick from an empty list');
      return xs[Math.floor(next() * xs.length)]!;
    },
  };
}

/** Picks a key with probability proportional to its weight; zero weights are never picked. */
export function pickWeighted(rng: Rng, weights: Readonly<Record<string, number>>): string {
  const entries = Object.entries(weights).filter(([, w]) => w > 0);
  let x = rng.next() * entries.reduce((sum, [, w]) => sum + w, 0);
  for (const [key, w] of entries) {
    x -= w;
    if (x < 0) return key;
  }
  return entries[entries.length - 1]![0];
}
