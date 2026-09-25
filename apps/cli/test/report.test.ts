import { describe, expect, it } from 'vitest';
import { errorRate, formatProgress, formatReport, type PhaseResult, type ResultDocument } from '../src/load/report';

const stats = (count: number) => ({
  count, rps: count / 10, latencyMs: { mean: 2, p50: 1.5, p90: 3, p95: 4, p99: 8, p999: 12, max: 20 },
  status: { '200': count }, errors: {}, dropped: 0,
});

describe('formatReport', () => {
  it('renders phases, instance shares, checks and notes', () => {
    const doc: ResultDocument = {
      scenario: 'flash-sale', target: 'http://localhost:8080', startedAt: '2026-09-25T10:00:00.000Z',
      options: { model: 'closed', concurrency: 50, mix: { list: 1 } },
      phases: [{ name: 'before', elapsedSeconds: 10, total: { ...stats(300), dropped: 5, errors: { timeout: 2 } }, byLabel: { list: stats(300) } }],
      instances: { a: 100, b: 200 },
      checks: [{ name: 'mid-sale product discounted', ok: false, message: 'expected 10.00' }],
      notes: { promotionId: 'abc' },
      interrupted: false, ok: false,
    };
    const text = formatReport(doc);
    expect(text).toContain('flash-sale -> http://localhost:8080');
    expect(text).toContain('mix={"list":1}');
    expect(text).toContain('phase before (10s)');
    expect(text).toContain('errors: timeout:2');
    expect(text).toContain('5 requests dropped');
    expect(text).toMatch(/b\s+200\s+66\.7%/);
    expect(text).toContain('FAIL mid-sale product discounted: expected 10.00');
    expect(text).toMatch(/promotionId\s+abc/);
  });
});

describe('errorRate', () => {
  it('is zero with no recorded phases', () => {
    expect(errorRate([])).toEqual({ errors: 0, total: 0, rate: 0 });
  });

  it('sums 5xx and transport errors against successes plus errors, over every phase', () => {
    const phases: PhaseResult[] = [
      { name: 'before', elapsedSeconds: 1, total: { ...stats(90), errors: { serverError: 10 } }, byLabel: {} },
      { name: 'after', elapsedSeconds: 1, total: { ...stats(190), errors: { serverError: 5, timeout: 5 } }, byLabel: {} },
    ];
    // errors: 10 + 5 + 5 = 20; total: (90 + 10) + (190 + 10) = 300
    expect(errorRate(phases)).toEqual({ errors: 20, total: 300, rate: 20 / 300 });
  });
});

describe('formatProgress', () => {
  it('prints one compact line', () => {
    expect(formatProgress('main', { elapsedSeconds: 5, rps: 1999.6, p50Ms: 3.2, p99Ms: 15, errors: 0, dropped: 0, inflight: 12 }))
      .toBe('[main 5.0s] 2000 req/s  p50 3.2ms  p99 15ms  errors 0  dropped 0  in-flight 12');
  });
});
