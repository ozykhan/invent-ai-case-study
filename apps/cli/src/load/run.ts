import type { ApiClient } from '../client';
import { runPhase, type LoadModel, type LoadSource } from './engine';
import { Metrics } from './metrics';
import { formatProgress, type Check, type PhaseResult, type ResultDocument } from './report';
import { createRng } from './rng';
import type { PhaseRunner, Scenario } from './scenarios/types';

export interface RunOptions {
  model: LoadModel;
  durationMs: number;
  warmupMs: number;
  reportEveryMs: number;
  seed: number;
  /** Echoed into the result document. */
  options: Record<string, unknown>;
}

export interface RunIo { log(msg: string): void; signal?: AbortSignal }

/** Runs setup, the scenario's phases (default: warmup + "main") and cleanup, and assembles the result document. */
export async function runLoad(client: ApiClient, scenario: Scenario, o: RunOptions, io: RunIo): Promise<ResultDocument> {
  const rng = createRng(o.seed);
  const startedAt = new Date().toISOString();
  const phases: PhaseResult[] = [];
  const instances: Record<string, number> = {};
  const checks: Check[] = [];
  const notes: Record<string, string> = {};
  let interrupted = false;
  let rampPending = true;

  // The ramp belongs to the first recorded phase only; warmup and later phases run at the full rate.
  const modelFor = (recorded: boolean): LoadModel => {
    if (o.model.kind !== 'open') return o.model;
    const ramp = recorded && rampPending;
    if (recorded) rampPending = false;
    return ramp ? o.model : { ...o.model, rampMs: 0 };
  };

  const runner: PhaseRunner = {
    client,
    durationMs: o.durationMs,
    log: io.log,
    async warmup(source: LoadSource = scenario) {
      if (o.warmupMs <= 0 || io.signal?.aborted) return;
      io.log(`warmup ${o.warmupMs / 1000}s (not recorded)`);
      const r = await runPhase(client, source, rng, null, { model: modelFor(false), durationMs: o.warmupMs, signal: io.signal });
      if (r.interrupted) interrupted = true;
    },
    async phase(name: string, durationMs: number, source: LoadSource = scenario) {
      if (io.signal?.aborted) { interrupted = true; return; }
      io.log(`phase ${name}: ${durationMs / 1000}s`);
      const metrics = new Metrics();
      const r = await runPhase(client, source, rng, metrics, {
        model: modelFor(true), durationMs, signal: io.signal, reportEveryMs: o.reportEveryMs,
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

  return {
    scenario: scenario.name, target: client.baseUrl, startedAt, options: o.options,
    phases, instances, checks, notes, interrupted, ok: checks.every((c) => c.ok),
  };
}
