import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import type { Command } from 'commander';
import { ApiClient } from '../client';
import { UsageError } from '../errors';
import type { LoadModel } from '../load/engine';
import { parseMix, parseRate } from '../load/parse';
import { formatReport } from '../load/report';
import { runLoad } from '../load/run';
import { SCENARIOS, createScenario } from '../load/scenarios';
import { emit, log } from '../output';
import { arg, durationArg, globals, intArg } from './common';

const MAX_PAGE = 1000;

interface LoadFlags {
  rate?: number;
  concurrency?: number;
  duration?: number;
  warmup?: number;
  ramp?: number;
  connections?: number;
  maxInflight?: number;
  reportEvery?: number;
  out?: string;
  seed?: number;
  category?: string;
  maxPage?: number;
  mix?: string;
}

export function registerLoad(program: Command): void {
  program.command('load')
    .description('Generate load and report latency percentiles, errors and the per-instance distribution')
    .argument('<scenario>', `one of: ${Object.keys(SCENARIOS).join(', ')}`)
    .option('--rate <rate>', 'open model: constant arrival rate, e.g. 2000/s (latency from scheduled time)', arg(parseRate))
    .option('--concurrency <n>', 'closed model: number of concurrent workers', intArg('concurrency'))
    .option('--duration <d>', 'recorded measurement time (default 30s)', durationArg)
    .option('--warmup <d>', 'unrecorded warmup before measuring (default 5s)', durationArg)
    .option('--ramp <d>', 'open model: rise linearly to --rate over this time, recorded (default 0s)', durationArg)
    .option('--connections <n>', 'HTTP connection pool size (default: max(concurrency, 64), or 256 with --rate)', intArg('connections'))
    .option('--max-inflight <n>', 'open model: cap on outstanding requests; requests over it are dropped (default 10000)', intArg('max-inflight'))
    .option('--report-every <d>', 'progress line interval on stderr, 0s to disable (default 5s)', durationArg)
    .option('--out <file>', 'also write the JSON result document to this file')
    .option('--seed <n>', 'random seed, for a repeatable request sequence', intArg('seed', 0))
    .option('--category <slug>', 'restrict requests to one category (flash-sale default: accessories)')
    .option('--max-page <n>', 'highest listing page requested (default 5)', intArg('max-page'))
    .option('--mix <mix>', 'request weights, e.g. list=70,detail=30 (browse: list, detail; write-mix: list, detail, stock, promo)')
    .action(async (name: string, flags: LoadFlags, cmd: Command) => {
      const g = globals(cmd);
      const def = SCENARIOS[name];
      if (!def) throw new UsageError(`unknown scenario '${name}' (available: ${Object.keys(SCENARIOS).join(', ')})`);
      if ((flags.rate === undefined) === (flags.concurrency === undefined)) {
        throw new UsageError('pass exactly one of --rate (open model) or --concurrency (closed model)');
      }
      if (flags.rate === undefined && (flags.ramp !== undefined || flags.maxInflight !== undefined)) {
        throw new UsageError('--ramp and --max-inflight apply to --rate only');
      }
      if (flags.mix !== undefined && def.labels.length === 0) throw new UsageError(`${name} does not take --mix`);
      if (flags.maxPage !== undefined && flags.maxPage > MAX_PAGE) throw new UsageError(`--max-page must be <= ${MAX_PAGE}`);

      const model: LoadModel = flags.rate !== undefined
        ? { kind: 'open', rate: flags.rate, rampMs: flags.ramp ?? 0, maxInflight: flags.maxInflight ?? 10_000 }
        : { kind: 'closed', concurrency: flags.concurrency! };
      const mix = flags.mix === undefined ? undefined : parseMix(flags.mix, def.labels);
      const durationMs = flags.duration ?? 30_000;
      const warmupMs = flags.warmup ?? 5_000;
      const maxPage = flags.maxPage ?? 5;
      const seed = flags.seed ?? Math.floor(Math.random() * 2 ** 31);
      const connections = flags.connections ?? (model.kind === 'closed' ? Math.max(model.concurrency, 64) : 256);
      // Load-test promotions expire a minute after the run would end, so a killed run cleans up after itself.
      const scenario = createScenario(name, { category: flags.category, maxPage, mix, promotionTtlMs: warmupMs + durationMs + 60_000 });

      const client = new ApiClient({ baseUrl: g.url, timeoutMs: g.timeoutMs, connections });
      const ac = new AbortController();
      const onSigint = () => { log('interrupted: stopping, waiting up to 5s for in-flight requests (Ctrl-C again to kill)'); ac.abort(); };
      process.once('SIGINT', onSigint);
      try {
        const doc = await runLoad(client, scenario, {
          model, durationMs, warmupMs, seed, reportEveryMs: flags.reportEvery ?? 5_000,
          options: {
            model: model.kind,
            ...(model.kind === 'open' ? { rate: model.rate, rampMs: model.rampMs, maxInflight: model.maxInflight } : { concurrency: model.concurrency }),
            durationMs, warmupMs, connections, timeoutMs: g.timeoutMs, seed, maxPage,
            ...(flags.category ? { category: flags.category } : {}),
            ...(mix ? { mix } : {}),
          },
        }, { log, signal: ac.signal });
        if (flags.out) {
          mkdirSync(dirname(flags.out), { recursive: true });
          writeFileSync(flags.out, `${JSON.stringify(doc, null, 2)}\n`);
        }
        emit(g.json, doc, () => formatReport(doc));
        process.exitCode = doc.interrupted ? 130 : doc.ok ? 0 : 1;
      } finally {
        process.off('SIGINT', onSigint);
        // Interrupted: requests that outlived the drain window are abandoned, not awaited up to --timeout.
        await client.close({ force: ac.signal.aborted });
      }
    });
}
