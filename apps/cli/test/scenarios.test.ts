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
