import { build, type Histogram } from 'hdr-histogram-js';

export type ErrorKind = 'timeout' | 'ECONNRESET' | 'ECONNREFUSED' | 'other';

const TIMEOUT_CODES = new Set(['UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_BODY_TIMEOUT', 'UND_ERR_CONNECT_TIMEOUT', 'ETIMEDOUT']);
const RESET_CODES = new Set(['ECONNRESET', 'UND_ERR_SOCKET', 'EPIPE']);

/** Buckets a transport error (no HTTP status) by its code. */
export function classifyError(err: unknown): ErrorKind {
  const code = (err as { code?: unknown } | null)?.code;
  if (typeof code === 'string') {
    if (TIMEOUT_CODES.has(code)) return 'timeout';
    if (RESET_CODES.has(code)) return 'ECONNRESET';
    if (code === 'ECONNREFUSED') return 'ECONNREFUSED';
  }
  if ((err as { name?: unknown } | null)?.name === 'TimeoutError') return 'timeout';
  return 'other';
}

export interface LatencySummary { mean: number; p50: number; p90: number; p95: number; p99: number; p999: number; max: number }

export interface StatsSummary {
  count: number;
  rps: number;
  latencyMs: LatencySummary;
  status: Record<string, number>;
  errors: Partial<Record<ErrorKind, number>>;
  dropped: number;
}

export interface IntervalSample { count: number; errors: number; dropped: number; p50Ms: number; p99Ms: number }

const MAX_US = 60_000_000;
const newHistogram = (): Histogram => build({ lowestDiscernibleValue: 1, highestTrackableValue: MAX_US, numberOfSignificantValueDigits: 3 });
const clampUs = (us: number): number => Math.min(MAX_US, Math.max(1, Math.round(us)));
const toMs = (us: number): number => Math.round(us / 10) / 100;

class LabelStats {
  private readonly histogram = newHistogram();
  private readonly status: Record<string, number> = {};
  private readonly errors: Partial<Record<ErrorKind, number>> = {};
  dropped = 0;

  record(latencyUs: number, status: number): void {
    this.histogram.recordValue(clampUs(latencyUs));
    this.status[status] = (this.status[status] ?? 0) + 1;
  }

  recordError(kind: ErrorKind): void {
    this.errors[kind] = (this.errors[kind] ?? 0) + 1;
  }

  summary(elapsedSeconds: number): StatsSummary {
    const h = this.histogram;
    const count = h.totalCount;
    const at = (p: number) => (count === 0 ? 0 : toMs(h.getValueAtPercentile(p)));
    return {
      count,
      rps: elapsedSeconds > 0 ? Math.round((count / elapsedSeconds) * 10) / 10 : 0,
      latencyMs: {
        mean: count === 0 ? 0 : toMs(h.mean), p50: at(50), p90: at(90), p95: at(95), p99: at(99), p999: at(99.9),
        max: count === 0 ? 0 : toMs(h.maxValue),
      },
      status: { ...this.status },
      errors: { ...this.errors },
      dropped: this.dropped,
    };
  }
}

/** Everything one recorded phase measures. Warmup runs pass `null` instead of a Metrics. */
export class Metrics {
  private readonly total = new LabelStats();
  private readonly labels = new Map<string, LabelStats>();
  private readonly instances = new Map<string, number>();
  private readonly interval = newHistogram();
  private intervalErrors = 0;
  private intervalDropped = 0;

  record(label: string, latencyUs: number, status: number, instance: string | undefined): void {
    this.total.record(latencyUs, status);
    this.label(label).record(latencyUs, status);
    this.interval.recordValue(clampUs(latencyUs));
    const key = instance ?? 'unknown';
    this.instances.set(key, (this.instances.get(key) ?? 0) + 1);
  }

  recordError(label: string, kind: ErrorKind): void {
    this.total.recordError(kind);
    this.label(label).recordError(kind);
    this.intervalErrors++;
  }

  recordDropped(label: string): void {
    this.total.dropped++;
    this.label(label).dropped++;
    this.intervalDropped++;
  }

  /** Stats since the previous call, for the live progress line. */
  takeInterval(): IntervalSample {
    const count = this.interval.totalCount;
    const sample = {
      count, errors: this.intervalErrors, dropped: this.intervalDropped,
      p50Ms: count === 0 ? 0 : toMs(this.interval.getValueAtPercentile(50)),
      p99Ms: count === 0 ? 0 : toMs(this.interval.getValueAtPercentile(99)),
    };
    this.interval.reset();
    this.intervalErrors = 0;
    this.intervalDropped = 0;
    return sample;
  }

  summary(elapsedSeconds: number): { total: StatsSummary; byLabel: Record<string, StatsSummary> } {
    const byLabel: Record<string, StatsSummary> = {};
    for (const name of [...this.labels.keys()].sort()) byLabel[name] = this.labels.get(name)!.summary(elapsedSeconds);
    return { total: this.total.summary(elapsedSeconds), byLabel };
  }

  instanceCounts(): Record<string, number> {
    return Object.fromEntries([...this.instances.entries()].sort(([a], [b]) => a.localeCompare(b)));
  }

  private label(name: string): LabelStats {
    let stats = this.labels.get(name);
    if (!stats) { stats = new LabelStats(); this.labels.set(name, stats); }
    return stats;
  }
}
