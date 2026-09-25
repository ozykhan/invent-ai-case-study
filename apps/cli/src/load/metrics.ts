import { build, type Histogram } from 'hdr-histogram-js';

/** `serverError` is any HTTP status >= 500; the rest are transport failures (no HTTP status at all). */
export type ErrorKind = 'timeout' | 'ECONNRESET' | 'ECONNREFUSED' | 'other' | 'serverError';

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
  /** Responses recorded into the latency histogram: status < 500 only (see `latencyMs`). */
  count: number;
  rps: number;
  /** Percentiles over 2xx-4xx responses only; a >= 500 response is counted in `errors.serverError` instead. */
  latencyMs: LatencySummary;
  status: Record<string, number>;
  errors: Partial<Record<ErrorKind, number>>;
  dropped: number;
}

export interface IntervalSample { count: number; errors: number; dropped: number; p50Ms: number; p99Ms: number }

/** Default histogram ceiling: generous for the common case, raised per phase to follow --timeout and --duration. */
const DEFAULT_MAX_US = 60_000_000;
const newHistogram = (maxUs: number): Histogram => build({ lowestDiscernibleValue: 1, highestTrackableValue: maxUs, numberOfSignificantValueDigits: 3 });
const clampUs = (us: number, maxUs: number): number => Math.min(maxUs, Math.max(1, Math.round(us)));
const toMs = (us: number): number => Math.round(us / 10) / 100;

class LabelStats {
  private readonly histogram: Histogram;
  private readonly status: Record<string, number> = {};
  private readonly errors: Partial<Record<ErrorKind, number>> = {};
  dropped = 0;

  constructor(private readonly maxUs: number) {
    this.histogram = newHistogram(maxUs);
  }

  /** A >= 500 status is a server error: counted in `status` and `errors.serverError`, kept out of the histogram. */
  record(latencyUs: number, status: number): void {
    this.status[status] = (this.status[status] ?? 0) + 1;
    if (status >= 500) { this.errors.serverError = (this.errors.serverError ?? 0) + 1; return; }
    this.histogram.recordValue(clampUs(latencyUs, this.maxUs));
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

/**
 * Everything one recorded phase measures. Warmup runs pass `null` instead of a Metrics.
 * `highestTrackableUs` bounds the HDR histogram; pass `timeoutMs`/`durationMs` scaled to µs so a long phase or a
 * raised --timeout doesn't silently clamp its own tail (default 60 s, the old fixed ceiling).
 */
export class Metrics {
  private readonly maxUs: number;
  private readonly total: LabelStats;
  private readonly labels = new Map<string, LabelStats>();
  private readonly instances = new Map<string, number>();
  private readonly interval: Histogram;
  private intervalErrors = 0;
  private intervalDropped = 0;

  constructor(highestTrackableUs: number = DEFAULT_MAX_US) {
    this.maxUs = Math.max(DEFAULT_MAX_US, highestTrackableUs);
    this.total = new LabelStats(this.maxUs);
    this.interval = newHistogram(this.maxUs);
  }

  /** A >= 500 status is a server error: it counts toward `errors` but not the latency histogram (see LabelStats). */
  record(label: string, latencyUs: number, status: number, instance: string | undefined): void {
    this.total.record(latencyUs, status);
    this.label(label).record(latencyUs, status);
    if (status >= 500) this.intervalErrors++;
    else this.interval.recordValue(clampUs(latencyUs, this.maxUs));
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
    if (!stats) { stats = new LabelStats(this.maxUs); this.labels.set(name, stats); }
    return stats;
  }
}
