import { Command, CommanderError } from 'commander';
import { registerHealth } from './commands/health';
import { registerIngest } from './commands/ingest';
import { registerLoad } from './commands/load';
import { registerProducts } from './commands/products';
import { registerPromotions } from './commands/promotions';
import { ApiError, SetupError, UsageError } from './errors';
import { log, out } from './output';

const program = new Command('modaco')
  .description('Operate and load test the ModaCo API')
  .option('--url <url>', 'API base URL (env API_URL)', process.env.API_URL || 'http://localhost:3000')
  .option('--json', 'print one JSON document to stdout')
  .option('--timeout <duration>', 'per-request timeout', '10s')
  .exitOverride() // before adding commands, so subcommands inherit it
  .showHelpAfterError();

registerHealth(program);
registerProducts(program);
registerPromotions(program);
registerIngest(program);
registerLoad(program);

/** Maps an error to an exit code: 2 for usage errors, 1 for API, setup and transport failures. */
function report(err: unknown, opts: { url: string; json?: boolean }): number {
  if (err instanceof CommanderError) return err.exitCode === 0 ? 0 : 2; // commander has already printed it
  if (err instanceof UsageError) { log(`error: ${err.message}`); return 2; }
  if (err instanceof ApiError) {
    if (opts.json) out(JSON.stringify(err.toJSON(), null, 2));
    else log(`error: HTTP ${err.status} ${err.code}: ${err.message}${err.details === undefined ? '' : `\n${JSON.stringify(err.details, null, 2)}`}`);
    return 1;
  }
  if (err instanceof SetupError) { log(`error: ${err.message}`); return 1; }
  const code = (err as { code?: unknown } | null)?.code;
  const message = err instanceof Error ? err.message : String(err);
  log(`error: ${typeof code === 'string' ? `${code} ` : ''}${message} (target ${opts.url})`);
  return 1;
}

try {
  await program.parseAsync(process.argv);
} catch (err) {
  process.exitCode = report(err, program.opts<{ url: string; json?: boolean }>());
}
