import { describe, expect, it } from 'vitest';
import { createRng } from '../src/load/rng';
import { describeMaxPageClamp, listRequest, pagesFor, type CatalogSample } from '../src/load/scenarios/catalog';

describe('pagesFor', () => {
  it('rounds up to a whole page at pageSize 20, floored at 1', () => {
    expect(pagesFor(0)).toBe(1);
    expect(pagesFor(1)).toBe(1);
    expect(pagesFor(20)).toBe(1);
    expect(pagesFor(21)).toBe(2);
    expect(pagesFor(90)).toBe(5);
  });
  it('is unbounded when the total is unknown', () => {
    expect(pagesFor(undefined)).toBe(Infinity);
  });
});

describe('describeMaxPageClamp', () => {
  const sample: CatalogSample = { total: 200, ids: [], slugs: ['shoes', 'bags'], totalsBySlug: { shoes: 90, bags: 5000 } };
  it('reports only categories with fewer pages than requested', () => {
    expect(describeMaxPageClamp(sample, 10)).toEqual(["max-page for 'shoes' clamped 10 -> 5 (90 products)"]);
    expect(describeMaxPageClamp(sample, 3)).toEqual([]);
  });
});

describe('listRequest', () => {
  const sample: CatalogSample = { total: 200, ids: [], slugs: ['shoes'], totalsBySlug: { shoes: 90 } };
  it('never asks for a page past the category real page count, even when --max-page asks for more', () => {
    const rng = createRng(1);
    const pages = new Set<number>();
    for (let i = 0; i < 200; i++) {
      const spec = listRequest(rng, sample, { category: 'shoes', maxPage: 50 });
      pages.add(Number(new URL(spec.path, 'http://x').searchParams.get('page')));
    }
    expect(Math.max(...pages)).toBeLessThanOrEqual(5); // ceil(90 / 20)
  });

  it('falls back to --max-page for a category with no known total', () => {
    const rng = createRng(1);
    const spec = listRequest(rng, { total: 0, ids: [], slugs: [], totalsBySlug: {} }, { category: 'unknown', maxPage: 3 });
    const page = Number(new URL(spec.path, 'http://x').searchParams.get('page'));
    expect(page).toBeGreaterThanOrEqual(1);
    expect(page).toBeLessThanOrEqual(3);
  });
});
