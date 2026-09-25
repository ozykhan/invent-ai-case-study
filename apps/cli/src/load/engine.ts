import { performance } from 'node:perf_hooks';
import { classifyError, type Metrics } from './metrics';
import type { Rng } from './rng';

export interface RequestSpec {
  /** Groups the request in the report, e.g. "list" or "promo:create". */
  label: string;
  method: 'GET' | 'POST' | 'PATCH' | 'PUT';
  path: string;
  body?: unknown;
  /** Receives the parsed JSON body. Only requests that set it pay for parsing. */
  onResponse?(status: number, body: unknown): void;
}

export interface SendResult { status: number; instance?: string; body?: unknown }

export interface Transport { send(spec: RequestSpec): Promise<SendResult> }

export interface LoadSource { next(rng: Rng): RequestSpec }

export type LoadModel =
  | { kind: 'open'; rate: number; rampMs: number; maxInflight: number }
  | { kind: 'closed'; concurrency: number };

export interface ProgressSample { elapsedSeconds: number; rps: number; p50Ms: number; p99Ms: number; errors: number; dropped: number; inflight: number }

export interface PhaseOptions {
  model: LoadModel;
  durationMs: number;
  signal?: AbortSignal;
  reportEveryMs?: number;
  onProgress?(sample: ProgressSample): void;
  /** How long an interrupted phase waits for in-flight requests. Default 5 s. */
  drainTimeoutMs?: number;
}

export interface PhaseOutcome { elapsedSeconds: number; interrupted: boolean }

/**
 * Offset in ms from phase start of the k-th request (0-based) when the rate rises linearly from 0 to
 * `ratePerSec` over `rampMs` and then stays flat. It inverts the cumulative count N(t): r·t²/(2·ramp) during
 * the ramp, and r·ramp/2 + r·(t − ramp) after it.
 */
export function scheduledOffsetMs(k: number, ratePerSec: number, rampMs: number): number {
  const perMs = ratePerSec / 1000;
  if (rampMs <= 0) return k / perMs;
  const rampCount = (perMs * rampMs) / 2;
  if (k <= rampCount) return Math.sqrt((2 * k * rampMs) / perMs);
  return rampMs + (k - rampCount) / perMs;
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const yieldToIo = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * Runs one phase of load. Open model: requests are due on a fixed schedule whatever the response times are, and
 * latency runs from the scheduled time, so a stalled server (or client) is charged for the requests it delayed
 * (coordinated omission). Closed model: `concurrency` workers loop send -> await -> send, and latency runs from the
 * send time.
 */
export async function runPhase(transport: Transport, source: LoadSource, rng: Rng, metrics: Metrics | null, opts: PhaseOptions): Promise<PhaseOutcome> {
  const inflight = new Set<Promise<void>>();
  const aborted = () => opts.signal?.aborted === true;
  const start = performance.now();
  const deadline = start + opts.durationMs;

  const issue = (spec: RequestSpec, t0: number): Promise<void> => {
    const done: Promise<void> = transport.send(spec).then(
      (res) => {
        metrics?.record(spec.label, (performance.now() - t0) * 1000, res.status, res.instance);
        try { spec.onResponse?.(res.status, res.body); } catch { /* a scenario hook must not stop the load */ }
      },
      (err: unknown) => { metrics?.recordError(spec.label, classifyError(err)); },
    ).finally(() => { inflight.delete(done); });
    inflight.add(done);
    return done;
  };

  let timer: NodeJS.Timeout | undefined;
  if (metrics && opts.onProgress && opts.reportEveryMs) {
    const onProgress = opts.onProgress;
    let last = start;
    timer = setInterval(() => {
      const now = performance.now();
      const s = metrics.takeInterval();
      onProgress({ elapsedSeconds: (now - start) / 1000, rps: s.count / ((now - last) / 1000), p50Ms: s.p50Ms, p99Ms: s.p99Ms, errors: s.errors, dropped: s.dropped, inflight: inflight.size });
      last = now;
    }, opts.reportEveryMs);
  }

  try {
    if (opts.model.kind === 'open') {
      const { rate, rampMs, maxInflight } = opts.model;
      const fire = (t0: number) => {
        const spec = source.next(rng);
        if (inflight.size >= maxInflight) { metrics?.recordDropped(spec.label); return; }
        void issue(spec, t0);
      };
      let k = 0;
      while (!aborted()) {
        const next = scheduledOffsetMs(k, rate, rampMs);
        if (next >= opts.durationMs) break;
        const wait = start + next - performance.now();
        if (wait >= 1) { await sleep(wait); continue; }
        // Fire everything that is due, including requests that fell behind during a stall.
        const now = performance.now() - start;
        for (let t = next; t <= now && t < opts.durationMs; t = scheduledOffsetMs(k, rate, rampMs)) {
          fire(start + t);
          k++;
        }
        await yieldToIo();
      }
      const rest = deadline - performance.now();
      if (rest > 0 && !aborted()) await sleep(rest);
    } else {
      const worker = async () => {
        while (!aborted() && performance.now() < deadline) await issue(source.next(rng), performance.now());
      };
      const workers = Promise.all(Array.from({ length: opts.model.concurrency }, worker));
      const onAbort = new Promise<void>((resolve) => {
        if (aborted()) resolve();
        else opts.signal?.addEventListener('abort', () => resolve(), { once: true });
      });
      await Promise.race([workers, onAbort]);
    }
  } finally {
    clearInterval(timer);
  }

  const stoppedAt = Math.min(performance.now(), deadline);
  const interrupted = aborted();
  const drain = Promise.allSettled([...inflight]);
  if (interrupted) {
    let grace: NodeJS.Timeout | undefined;
    await Promise.race([drain, new Promise<void>((resolve) => { grace = setTimeout(resolve, opts.drainTimeoutMs ?? 5000); })]);
    clearTimeout(grace);
  } else {
    await drain;
  }
  return { elapsedSeconds: (stoppedAt - start) / 1000, interrupted };
}
