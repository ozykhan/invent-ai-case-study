import type { Command } from 'commander';
import type { HealthBody } from '../api-types';
import type { Reply } from '../client';
import { emit, out } from '../output';
import { durationArg, withClient } from './common';

const describe = (r: Reply<HealthBody>) =>
  `${r.body.status}  postgres ${r.body.checks.postgres ? 'up' : 'DOWN'}  redis ${r.body.checks.redis ? 'up' : 'DOWN'}  instance ${r.instance ?? '-'}`;

export function registerHealth(program: Command): void {
  program.command('health')
    .description('GET /health: Postgres and Redis checks, and which instance answered')
    .option('--watch <interval>', 'poll until interrupted, e.g. 2s (with --json: one JSON line per poll)', durationArg)
    .action((opts: { watch?: number }, cmd: Command) => withClient(cmd, async (client, g) => {
      if (opts.watch === undefined) {
        const r = await client.health();
        emit(g.json, r.body, () => describe(r));
        if (r.status !== 200) process.exitCode = 1;
        return;
      }
      for (;;) {
        const at = new Date().toISOString();
        try {
          const r = await client.health();
          out(g.json ? JSON.stringify({ at, instance: r.instance ?? null, ...r.body }) : `${at}  ${describe(r)}`);
        } catch (err) {
          // Keep polling through outages: watching a replica go down and come back is the point.
          const cause = (err as { code?: string }).code ?? (err as Error).message;
          out(g.json ? JSON.stringify({ at, status: 'unreachable', cause }) : `${at}  unreachable (${cause})`);
        }
        await new Promise((resolve) => setTimeout(resolve, opts.watch));
      }
    }));
}
