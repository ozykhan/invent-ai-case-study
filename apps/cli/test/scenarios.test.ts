import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ApiClient } from '../src/client';
import { SetupError } from '../src/errors';
import { runLoad, type RunOptions } from '../src/load/run';
import { createScenario } from '../src/load/scenarios';
import { startStubApi, type StubApi } from './stub-api';

let stub: StubApi;
let client: ApiClient;
const quiet = { log: () => {} };
const opts = (over: Partial<RunOptions> = {}): RunOptions => ({
  model: { kind: 'closed', concurrency: 4 }, durationMs: 400, warmupMs: 0, reportEveryMs: 0, seed: 7, options: {}, ...over,
});

beforeEach(async () => {
  stub = await startStubApi();
  client = new ApiClient({ baseUrl: stub.url, timeoutMs: 5000, connections: 8 });
});
afterEach(async () => {
  await client.close();
  await stub.close();
});

describe('browse', () => {
  it('samples the catalog, mixes list and detail, and tallies instances', async () => {
    const doc = await runLoad(client, createScenario('browse', { maxPage: 5 }), opts({ warmupMs: 100 }), quiet);
    expect(stub.state.samplePages).toBe(10); // 1000 products / 100 per page
    expect(doc.phases.map((p) => p.name)).toEqual(['main']);
    const main = doc.phases[0]!;
    expect(Object.keys(main.byLabel)).toEqual(['detail', 'list']);
    const listShare = main.byLabel.list!.count / main.total.count;
    expect(listShare).toBeGreaterThan(0.6);
    expect(listShare).toBeLessThan(0.8);
    // Warmup is not recorded: every recorded response is attributed to exactly one instance.
    expect(Object.values(doc.instances).reduce((a, b) => a + b, 0)).toBe(main.total.count);
    expect(Object.keys(doc.instances)).toEqual(['i1', 'i2', 'i3']);
    expect(doc).toMatchObject({ scenario: 'browse', target: stub.url, ok: true, interrupted: false, checks: [] });
  });

  it('honours --mix and --category', async () => {
    const doc = await runLoad(client, createScenario('browse', { maxPage: 2, mix: { list: 1 }, category: 'shoes' }), opts(), quiet);
    expect(Object.keys(doc.phases[0]!.byLabel)).toEqual(['list']);
  });

  it('fails setup on an empty catalog', async () => {
    const empty = await startStubApi({ total: 0 });
    const c = new ApiClient({ baseUrl: empty.url, timeoutMs: 5000 });
    try {
      await expect(runLoad(c, createScenario('browse', { maxPage: 5 }), opts(), quiet)).rejects.toThrow(SetupError);
    } finally {
      await c.close();
      await empty.close();
    }
  });

  it('marks the run interrupted when the signal aborts', async () => {
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 100);
    const doc = await runLoad(client, createScenario('browse', { maxPage: 5 }), opts({ durationMs: 10_000 }), { ...quiet, signal: ac.signal });
    expect(doc.interrupted).toBe(true);
    expect(doc.phases[0]!.elapsedSeconds).toBeLessThan(2);
  });
});

describe('createScenario', () => {
  it('rejects unknown scenarios', () => {
    expect(() => createScenario('nope', { maxPage: 5 })).toThrow(/unknown scenario 'nope'/);
  });
});

describe('write-mix', () => {
  it('mixes reads with stock writes and cleans up its promotions', async () => {
    const doc = await runLoad(client, createScenario('write-mix', { maxPage: 5 }), opts({ durationMs: 600 }), quiet);
    const main = doc.phases[0]!;
    expect(Object.keys(main.byLabel)).toEqual(expect.arrayContaining(['detail', 'list', 'stock']));
    const stockShare = main.byLabel.stock!.count / main.total.count;
    expect(stockShare).toBeGreaterThan(0.08);
    expect(stockShare).toBeLessThan(0.2);
    expect(stub.state.hits.get('PATCH /products/:id/stock')).toBeGreaterThan(0);
    expect(stub.state.openPromotions.size).toBe(0);
  });

  it('alternates promotion create and cancel, one request per slot', async () => {
    const doc = await runLoad(client, createScenario('write-mix', { maxPage: 5, mix: { promo: 1 } }), opts({ model: { kind: 'closed', concurrency: 1 } }), quiet);
    const { byLabel } = doc.phases[0]!;
    expect(byLabel['promo:create']!.count).toBeGreaterThan(0);
    expect(byLabel['promo:cancel']!.count).toBeGreaterThan(0);
    expect(Math.abs(byLabel['promo:create']!.count - byLabel['promo:cancel']!.count)).toBeLessThanOrEqual(1);
    expect(stub.state.openPromotions.size).toBe(0);
  });
});

describe('flash-sale', () => {
  it('records before and after phases, passes the mid-sale check, and cancels the promotion', async () => {
    const doc = await runLoad(client, createScenario('flash-sale', { maxPage: 5, category: 'shoes' }), opts({ durationMs: 300 }), quiet);
    expect(doc.phases.map((p) => p.name)).toEqual(['before', 'after']);
    expect(doc.checks).toHaveLength(1);
    expect(doc.checks[0]).toMatchObject({ name: 'mid-sale product discounted', ok: true });
    expect(doc.notes.promotionId).toBe(stub.state.lastPromotionId);
    expect(doc.notes.firstItemPriceBefore).toBe('20.00');
    expect(doc.ok).toBe(true);
    expect(stub.state.openPromotions.size).toBe(0);
  });

  it('fails the check but still cancels when the new product is not discounted', async () => {
    const wrong = await startStubApi({ midSalePrice: '20.00' });
    const c = new ApiClient({ baseUrl: wrong.url, timeoutMs: 5000 });
    try {
      const doc = await runLoad(c, createScenario('flash-sale', { maxPage: 5 }), opts({ durationMs: 300 }), quiet);
      expect(doc.checks[0]).toMatchObject({ ok: false });
      expect(doc.checks[0]!.message).toContain('expected 10.00');
      expect(doc.ok).toBe(false);
      expect(wrong.state.openPromotions.size).toBe(0);
    } finally {
      await c.close();
      await wrong.close();
    }
  });
});
