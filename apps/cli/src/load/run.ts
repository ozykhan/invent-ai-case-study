import type { ApiClient } from '../client';
import { runPhase, type LoadModel, type LoadSource } from './engine';
import { Metrics } from './metrics';
import { errorRate, formatProgress, type Check, type PhaseResult, type ResultDocument } from './report';
import { createRng } from './rng';
import type { PhaseRunner, Scenario } from './scenarios/types';

export interface RunOptions {
  model: LoadModel;
  durationMs: number;
  warmupMs: number;
  reportEveryMs: number;
  seed: number;
  /** Per-request timeout, for sizing the latency histogram (see `histogramUs`). */
  timeoutMs: number;
  /** 5xx + transport error rate, in [0, 1], above which the run fails (ok=false). Default 0: any such error fails it. */
  maxErrorRate: number;
  /** Echoed into the result document. */
  options: Record<string, unknown>;
}

export interface RunIo { log(msg: string): void; signal?: AbortSignal }

/**
 * The histogram must hold the slowest latency a phase could legitimately record: bounded by --timeout for any one
 * request, or by the phase's own duration for an open-model request that fell far behind schedule (coordinated
 * omission can charge it the whole phase). `Metrics` itself floors this at 60s.
 */
const histogramUs = (o: Pick<RunOptions, 'timeoutMs' | 'durationMs'>): number => Math.max(o.timeoutMs, o.durationMs) * 1000;

/** Runs setup, the scenario's phases (default: warmup + "main") and cleanup, and assembles the result document. */
export async function runLoad(client: ApiClient, scenario: Scenario, o: RunOptions, io: RunIo): Promise<ResultDocument> {
  const rng = createRng(o.seed);
  const startedAt = new Date().toISOString();
  const phases: PhaseResult[] = [];
  const instances: Record<string, number> = {};
  const checks: Check[] = [];
  const notes: Record<string, string> = {};
  let interrupted = false;
  const flatModel = (): LoadModel => (o.model.kind === 'open' ? { ...o.model, rampMs: 0 } : o.model);

  const runner: PhaseRunner = {
    client,
    durationMs: o.durationMs,
    log: io.log,
    aborted: () => io.signal?.aborted === true,
    async warmup(source: LoadSource = scenario) {
      if (o.warmupMs <= 0 || io.signal?.aborted) return;
      io.log(`warmup ${o.warmupMs / 1000}s (not recorded)`);
      const r = await runPhase(client, source, rng, null, { model: flatModel(), durationMs: o.warmupMs, signal: io.signal });
      if (r.interrupted) interrupted = true;
    },
    async phase(name: string, durationMs: number, source: LoadSource = scenario) {
      if (io.signal?.aborted) { interrupted = true; return; }
      io.log(`phase ${name}: ${durationMs / 1000}s`);
      const metrics = new Metrics(histogramUs({ timeoutMs: o.timeoutMs, durationMs }));
      const r = await runPhase(client, source, rng, metrics, {
        model: flatModel(), durationMs, signal: io.signal, reportEveryMs: o.reportEveryMs,
        onProgress: (s) => io.log(formatProgress(name, s)),
      });
      if (r.interrupted) interrupted = true;
      phases.push({ name, elapsedSeconds: Math.round(r.elapsedSeconds * 100) / 100, ...metrics.summary(r.elapsedSeconds) });
      for (const [id, n] of Object.entries(metrics.instanceCounts())) instances[id] = (instances[id] ?? 0) + n;
    },
    addCheck: (check) => { checks.push(check); },
    note: (key, value) => { notes[key] = value; },
  };

  await scenario.setup(client, io.log);
  // The ramp runs once, unrecorded, before anything else the scenario does (including --warmup): it climbs from 0
  // to --rate, so running it after warmup would mean throttling back down from a rate warmup had already survived
  // at, for no benefit. Every later phase (warmup included) runs flat at the full rate.
  if (o.model.kind === 'open' && o.model.rampMs > 0 && !io.signal?.aborted) {
    io.log(`ramp ${o.model.rampMs / 1000}s to --rate (not recorded)`);
    const rr = await runPhase(client, scenario, rng, null, { model: o.model, durationMs: o.model.rampMs, signal: io.signal });
    if (rr.interrupted) interrupted = true;
  }
  try {
    if (scenario.run) {
      await scenario.run(runner);
    } else {
      await runner.warmup();
      await runner.phase('main', o.durationMs);
    }
  } finally {
    if (scenario.cleanup) await scenario.cleanup(client).catch((err: unknown) => io.log(`warning: cleanup failed: ${String(err)}`));
  }
  // A scenario may stop early on an abort that no phase saw (e.g. between phases); never report that as a full run.
  if (io.signal?.aborted) interrupted = true;

  // Only surfaced when there was something to report: a clean run (the common case) shouldn't grow a
  // trivially-passing "0 errors <= 0%" check on top of whatever the scenario itself checks.
  const rate = errorRate(phases);
  if (rate.errors > 0) {
    const pct = (n: number) => `${(n * 100).toFixed(2)}%`;
    const ok = rate.rate <= o.maxErrorRate;
    checks.push({
      name: 'error rate',
      ok,
      message: `${pct(rate.rate)} (${rate.errors} of ${rate.total}) ${ok ? '<=' : '>'} --max-error-rate ${pct(o.maxErrorRate)}`,
    });
  }

  return {
    scenario: scenario.name, target: client.baseUrl, startedAt, options: o.options,
    phases, instances, checks, notes, interrupted, ok: checks.every((c) => c.ok),
  };
}
