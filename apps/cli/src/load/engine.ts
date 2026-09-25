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

const yieldToIo = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * Sleeps that one abort can end early. Each pending sleep holds one entry, which it removes when it ends, so nothing
 * accumulates per past sleep. Below ~1000 req/s the open model sleeps once per request, and a per-sleep reaction on a
 * long-lived abort promise used to keep ~0.5 KB per sleep until the phase ended (about 1 GB over a 1 h soak at 500/s).
 */
export class Sleeper {
  private readonly wakers = new Set<() => void>();
  private woken = false;

  /** Sleeps currently waiting. */
  get pending(): number { return this.wakers.size; }

  /** Resolves after `ms`, or as soon as wakeAll() is called; at once if it already was. */
  sleep(ms: number): Promise<void> {
    if (this.woken) return Promise.resolve();
    return new Promise<void>((resolve) => {
      const wake = () => { clearTimeout(timer); this.wakers.delete(wake); resolve(); };
      const timer = setTimeout(wake, ms);
      this.wakers.add(wake);
    });
  }

  /** Ends every pending sleep now, and every later one immediately. */
  wakeAll(): void {
    this.woken = true;
    for (const wake of [...this.wakers]) wake();
  }
}

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

  // Abort ends every wait at once: the open model's sleeps through `sleeper`, the closed model's race on `onAbort`
  // (awaited once per phase, so it gathers no per-request reactions). `abortedAt` records the moment the signal
  // actually fired, for an accurate `elapsedSeconds` on an interrupted phase.
  let abortedAt: number | undefined = aborted() ? start : undefined;
  let resolveAbort!: () => void;
  const onAbort = new Promise<void>((resolve) => { resolveAbort = resolve; });
  const sleeper = new Sleeper();
  const onAbortHandler = () => { abortedAt = performance.now(); resolveAbort(); sleeper.wakeAll(); };
  if (aborted()) { resolveAbort(); sleeper.wakeAll(); }
  else opts.signal?.addEventListener('abort', onAbortHandler, { once: true });
  const sleepOrAbort = (ms: number) => sleeper.sleep(ms);

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
        // setTimeout cannot wait less than 1 ms, so a gap under that is not slept but yielded (setImmediate below).
        // Above ~1000 req/s every gap is under 1 ms and this loop never sleeps: it keeps one core busy between
        // I/O turns. That is deliberate, the price of holding the schedule at high rates.
        if (wait >= 1) { await sleepOrAbort(wait); continue; }
        // Fire everything that is due, including requests that fell behind during a stall.
        const now = performance.now() - start;
        for (let t = next; t <= now && t < opts.durationMs; t = scheduledOffsetMs(k, rate, rampMs)) {
          fire(start + t);
          k++;
        }
        await yieldToIo();
      }
      if (!aborted()) {
        const rest = deadline - performance.now();
        if (rest > 0) await sleepOrAbort(rest);
      }
    } else {
      const worker = async () => {
        while (!aborted() && performance.now() < deadline) await issue(source.next(rng), performance.now());
      };
      const workers = Promise.all(Array.from({ length: opts.model.concurrency }, worker));
      await Promise.race([workers, onAbort]);
    }
  } finally {
    clearInterval(timer);
    opts.signal?.removeEventListener('abort', onAbortHandler);
  }

  const interrupted = aborted();
  const stoppedAt = interrupted ? (abortedAt ?? performance.now()) : Math.min(performance.now(), deadline);
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
