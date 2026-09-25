import { describe, expect, it } from 'vitest';
import { classifyError, Metrics } from '../src/load/metrics';

describe('Metrics', () => {
  it('reports percentiles in ms from microsecond samples', () => {
    const m = new Metrics();
    for (let i = 1; i <= 1000; i++) m.record('list', i * 1000, 200, 'a'); // 1..1000 ms
    const { total } = m.summary(2);
    expect(total.count).toBe(1000);
    expect(total.rps).toBe(500);
    expect(Math.abs(total.latencyMs.p50 - 500)).toBeLessThan(5);
    expect(Math.abs(total.latencyMs.p99 - 990)).toBeLessThan(10);
    expect(Math.abs(total.latencyMs.mean - 500.5)).toBeLessThan(5);
    expect(Math.abs(total.latencyMs.max - 1000)).toBeLessThan(10);
  });

  it('keeps per-label stats, status codes, errors and drops', () => {
    const m = new Metrics();
    m.record('list', 1000, 200, 'a');
    m.record('detail', 2000, 404, 'b');
    m.recordError('detail', 'timeout');
    m.recordDropped('list');
    const { total, byLabel } = m.summary(1);
    expect(total.status).toEqual({ '200': 1, '404': 1 });
    expect(total.errors).toEqual({ timeout: 1 });
    expect(total.dropped).toBe(1);
    expect(byLabel.list!.count).toBe(1);
    expect(byLabel.list!.dropped).toBe(1);
    expect(byLabel.detail!.status).toEqual({ '404': 1 });
    expect(byLabel.detail!.errors).toEqual({ timeout: 1 });
  });

  it('returns zeros for a label with no responses', () => {
    const m = new Metrics();
    m.recordError('list', 'other');
    expect(m.summary(1).byLabel.list!.latencyMs).toEqual({ mean: 0, p50: 0, p90: 0, p95: 0, p99: 0, p999: 0, max: 0 });
  });

  it('tallies responses per instance, with a missing header as unknown', () => {
    const m = new Metrics();
    m.record('list', 1000, 200, 'b');
    m.record('list', 1000, 200, 'a');
    m.record('list', 1000, 200, 'a');
    m.record('list', 1000, 200, undefined);
    expect(m.instanceCounts()).toEqual({ a: 2, b: 1, unknown: 1 });
    expect(Object.keys(m.instanceCounts())).toEqual(['a', 'b', 'unknown']);
  });

  it('takeInterval returns the interval sample and resets it', () => {
    const m = new Metrics();
    m.record('list', 5000, 200, 'a');
    m.recordError('list', 'other');
    m.recordDropped('list');
    const first = m.takeInterval();
    expect(first.count).toBe(1);
    expect(first.errors).toBe(1);
    expect(first.dropped).toBe(1);
    expect(Math.abs(first.p50Ms - 5)).toBeLessThan(0.1);
    expect(m.takeInterval()).toEqual({ count: 0, errors: 0, dropped: 0, p50Ms: 0, p99Ms: 0 });
    expect(m.summary(1).total.count).toBe(1); // totals are not reset
  });
});

describe('classifyError', () => {
  const withCode = (code: string) => Object.assign(new Error(code), { code });
  it('maps undici and socket codes to kinds', () => {
    expect(classifyError(withCode('UND_ERR_HEADERS_TIMEOUT'))).toBe('timeout');
    expect(classifyError(withCode('UND_ERR_BODY_TIMEOUT'))).toBe('timeout');
    expect(classifyError(withCode('UND_ERR_CONNECT_TIMEOUT'))).toBe('timeout');
    expect(classifyError(withCode('ECONNRESET'))).toBe('ECONNRESET');
    expect(classifyError(withCode('UND_ERR_SOCKET'))).toBe('ECONNRESET');
    expect(classifyError(withCode('ECONNREFUSED'))).toBe('ECONNREFUSED');
    expect(classifyError(new Error('boom'))).toBe('other');
    expect(classifyError('nope')).toBe('other');
  });
});
