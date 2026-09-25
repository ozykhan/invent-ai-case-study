import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ApiClient } from '../src/client';
import { runLoad, type RunOptions } from '../src/load/run';
import { createScenario } from '../src/load/scenarios';
import { startStubApi, type StubApi } from './stub-api';

let stub: StubApi;
let client: ApiClient;
const opts = (over: Partial<RunOptions> = {}): RunOptions => ({
  model: { kind: 'closed', concurrency: 4 }, durationMs: 300, warmupMs: 0, reportEveryMs: 0, seed: 7,
  timeoutMs: 5000, maxErrorRate: 0, options: {}, ...over,
});

beforeEach(async () => {
  stub = await startStubApi();
  client = new ApiClient({ baseUrl: stub.url, timeoutMs: 5000, connections: 8 });
});
afterEach(async () => {
  await client.close();
  await stub.close();
});

describe('runLoad: --ramp', () => {
  it('runs the ramp once as its own unrecorded phase before the first recorded phase', async () => {
    const logs: string[] = [];
    const doc = await runLoad(
      client, createScenario('browse', { maxPage: 5, mix: { list: 1 } }),
      opts({ model: { kind: 'open', rate: 100, rampMs: 100, maxInflight: 10_000 }, durationMs: 200 }),
      { log: (m) => logs.push(m) },
    );
    // Only "main" is a recorded phase: the ramp itself never becomes one, so its rising-rate latencies never land
    // in "main"'s percentiles.
    expect(doc.phases.map((p) => p.name)).toEqual(['main']);
    expect(logs.some((l) => /^ramp 0\.1s to --rate \(not recorded\)$/.test(l))).toBe(true);
    expect(logs.some((l) => l.startsWith('phase main'))).toBe(true);
  });

  it('does not add a ramp phase for the closed model or when --ramp is 0', async () => {
    const logs: string[] = [];
    await runLoad(client, createScenario('browse', { maxPage: 5, mix: { list: 1 } }), opts({ durationMs: 200 }), { log: (m) => logs.push(m) });
    expect(logs.some((l) => l.startsWith('ramp '))).toBe(false);
  });
});

describe('runLoad: --max-error-rate', () => {
  /** The scenario's setup (catalog sampling) succeeds normally; only load-path responses (client.send) are 503. */
  function withFailingLoadPath(): ApiClient {
    const spy: ApiClient = Object.create(client) as ApiClient;
    spy.send = async () => ({ status: 503, instance: 'i1' });
    return spy;
  }

  it('fails the run by default when every response is a 5xx', async () => {
    const doc = await runLoad(withFailingLoadPath(), createScenario('browse', { maxPage: 5, mix: { list: 1 } }), opts(), { log: () => {} });
    expect(doc.ok).toBe(false);
    expect(doc.phases[0]!.total.count).toBe(0);
    expect(doc.phases[0]!.total.errors.serverError).toBeGreaterThan(0);
    const check = doc.checks.find((c) => c.name === 'error rate');
    expect(check).toMatchObject({ ok: false });
  });

  it('passes when --max-error-rate covers the observed rate', async () => {
    const doc = await runLoad(withFailingLoadPath(), createScenario('browse', { maxPage: 5, mix: { list: 1 } }), opts({ maxErrorRate: 1 }), { log: () => {} });
    expect(doc.ok).toBe(true);
    expect(doc.checks.find((c) => c.name === 'error rate')).toMatchObject({ ok: true });
  });

  it('adds no error-rate check when a phase records no responses at all', async () => {
    const ac = new AbortController();
    ac.abort();
    const doc = await runLoad(client, createScenario('browse', { maxPage: 5, mix: { list: 1 } }), opts(), { log: () => {}, signal: ac.signal });
    expect(doc.interrupted).toBe(true);
    expect(doc.checks.find((c) => c.name === 'error rate')).toBeUndefined();
  });
});
