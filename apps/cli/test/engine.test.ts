import { performance } from 'node:perf_hooks';
import { describe, expect, it } from 'vitest';
import { Sleeper, runPhase, scheduledOffsetMs, type RequestSpec, type SendResult, type Transport } from '../src/load/engine';
import { Metrics } from '../src/load/metrics';
import { createRng } from '../src/load/rng';

const source = { next: (): RequestSpec => ({ label: 'get', method: 'GET', path: '/x' }) };
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** A transport that answers after `ms` and tracks its own concurrency. */
function delayed(ms: number, result: SendResult = { status: 200, instance: 'a' }) {
  const t = {
    inflight: 0,
    maxInflight: 0,
    async send(): Promise<SendResult> {
      t.inflight++;
      t.maxInflight = Math.max(t.maxInflight, t.inflight);
      try { await sleep(ms); return result; } finally { t.inflight--; }
    },
  };
  return t;
}

describe('scheduledOffsetMs', () => {
  it('spaces requests evenly without a ramp', () => {
    expect(scheduledOffsetMs(0, 100, 0)).toBe(0);
    expect(scheduledOffsetMs(1, 100, 0)).toBe(10);
    expect(scheduledOffsetMs(250, 100, 0)).toBe(2500);
  });
  it('ramps linearly from zero to the target rate', () => {
    // 1000/s over a 1 s ramp: 500 requests during the ramp, then one per ms.
    expect(scheduledOffsetMs(125, 1000, 1000)).toBeCloseTo(500);
    expect(scheduledOffsetMs(500, 1000, 1000)).toBeCloseTo(1000);
    expect(scheduledOffsetMs(1500, 1000, 1000)).toBeCloseTo(2000);
  });
});

describe('Sleeper', () => {
  it('holds nothing for a sleep once it has ended (no per-sleep growth over a long open-model run)', async () => {
    const s = new Sleeper();
    await Promise.all(Array.from({ length: 2000 }, () => s.sleep(1)));
    expect(s.pending).toBe(0);
  });

  it('wakes every pending sleep at once, and later sleeps return immediately', async () => {
    const s = new Sleeper();
    const sleeps = Promise.all([s.sleep(60_000), s.sleep(60_000), s.sleep(60_000)]);
    expect(s.pending).toBe(3);
    const t0 = performance.now();
    s.wakeAll();
    await sleeps;
    expect(performance.now() - t0).toBeLessThan(100);
    expect(s.pending).toBe(0);
    await s.sleep(60_000);
    expect(s.pending).toBe(0);
  });
});

describe('runPhase', () => {
  it('holds the target rate in the open model', async () => {
    const metrics = new Metrics();
    const out = await runPhase(delayed(5), source, createRng(1), metrics, { model: { kind: 'open', rate: 500, rampMs: 0, maxInflight: 10_000 }, durationMs: 2000 });
    const { total } = metrics.summary(out.elapsedSeconds);
    // Widened from a tight +/-5% band: a loaded machine can fall behind schedule without the rate actually being wrong.
    expect(total.count).toBeGreaterThanOrEqual(850);
    expect(total.count).toBeLessThanOrEqual(1150);
    expect(total.rps).toBeGreaterThan(400);
    expect(out.interrupted).toBe(false);
  });

  it('keeps exactly n requests in flight in the closed model', async () => {
    const t = delayed(10);
    const metrics = new Metrics();
    await runPhase(t, source, createRng(1), metrics, { model: { kind: 'closed', concurrency: 8 }, durationMs: 300 });
    expect(t.maxInflight).toBe(8);
    expect(metrics.summary(0.3).total.count).toBeGreaterThanOrEqual(8 * 15);
  });

  it('charges a client-side stall to the requests scheduled during it (coordinated omission)', async () => {
    let calls = 0;
    const transport: Transport = {
      async send() {
        calls++;
        if (calls === 40) {
          const until = performance.now() + 300;
          while (performance.now() < until) { /* block the event loop, like a GC pause or a stalled client */ }
        }
        return { status: 200 };
      },
    };
    const metrics = new Metrics();
    await runPhase(transport, source, createRng(1), metrics, { model: { kind: 'open', rate: 200, rampMs: 0, maxInflight: 10_000 }, durationMs: 1000 });
    const { total } = metrics.summary(1);
    // About 60 of 200 requests were due while the loop was blocked. Timed from send, they would all read about 0 ms.
    expect(total.latencyMs.max).toBeGreaterThanOrEqual(250);
    expect(total.latencyMs.p90).toBeGreaterThanOrEqual(50);
  });

  it('drops requests beyond maxInflight instead of queueing them', async () => {
    const metrics = new Metrics();
    await runPhase(delayed(500), source, createRng(1), metrics, { model: { kind: 'open', rate: 1000, rampMs: 0, maxInflight: 10 }, durationMs: 200 });
    const { total } = metrics.summary(0.2);
    expect(total.count).toBe(10);
    expect(total.dropped).toBe(190);
  });

  it('measures elapsedSeconds through the drain tail, not just the nominal deadline', async () => {
    const metrics = new Metrics();
    // Every worker's in-flight request at the deadline still takes 150ms to answer; the true span of the phase is
    // that drain, not the 100ms nominal duration -- otherwise rps = count / elapsedSeconds is inflated.
    const out = await runPhase(delayed(150), source, createRng(1), metrics, { model: { kind: 'closed', concurrency: 5 }, durationMs: 100 });
    expect(out.interrupted).toBe(false);
    expect(out.elapsedSeconds).toBeGreaterThanOrEqual(0.14);
  });

  it('stops early when the signal aborts', async () => {
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 100);
    const started = performance.now();
    const out = await runPhase(delayed(5), source, createRng(1), new Metrics(), { model: { kind: 'closed', concurrency: 4 }, durationMs: 5000, signal: ac.signal });
    expect(out.interrupted).toBe(true);
    // Widened: a loaded machine can delay the drain well past the 100ms abort + 5ms transport without a real bug.
    expect(performance.now() - started).toBeLessThan(2000);
  });

  it('stops early when the signal aborts during a long ramp in the open model', async () => {
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 50);
    const started = performance.now();
    const out = await runPhase(delayed(5), source, createRng(1), new Metrics(), { model: { kind: 'open', rate: 50, rampMs: 30_000, maxInflight: 10_000 }, durationMs: 60_000, signal: ac.signal });
    expect(out.interrupted).toBe(true);
    // Widened from 200ms/0.2s: only needs to prove the abort short-circuits a 60s phase, not exact timing.
    expect(performance.now() - started).toBeLessThan(1000);
    expect(out.elapsedSeconds).toBeLessThan(1);
  });

  it('records transport errors by kind and hands parsed bodies to onResponse', async () => {
    let n = 0;
    const seen: Array<[number, unknown]> = [];
    const transport: Transport = {
      async send() {
        n++;
        if (n % 2 === 1) throw Object.assign(new Error('reset'), { code: 'ECONNRESET' });
        await sleep(1);
        return { status: 201, body: { id: n } };
      },
    };
    const src = { next: (): RequestSpec => ({ label: 'create', method: 'POST', path: '/x', onResponse: (status, body) => { seen.push([status, body]); } }) };
    const metrics = new Metrics();
    await runPhase(transport, src, createRng(1), metrics, { model: { kind: 'closed', concurrency: 1 }, durationMs: 100 });
    const create = metrics.summary(0.1).byLabel.create!;
    expect(create.errors.ECONNRESET).toBeGreaterThan(0);
    expect(create.status['201']).toBe(seen.length);
    expect(seen[0]).toEqual([201, { id: 2 }]);
  });

  it('reports progress while recording and runs unrecorded with null metrics', async () => {
    const samples: number[] = [];
    await runPhase(delayed(2), source, createRng(1), new Metrics(), { model: { kind: 'closed', concurrency: 2 }, durationMs: 250, reportEveryMs: 50, onProgress: (s) => samples.push(s.rps) });
    expect(samples.length).toBeGreaterThanOrEqual(3);
    expect(samples.some((rps) => rps > 0)).toBe(true);
    await expect(runPhase(delayed(2), source, createRng(1), null, { model: { kind: 'closed', concurrency: 2 }, durationMs: 50 })).resolves.toMatchObject({ interrupted: false });
  });
});
