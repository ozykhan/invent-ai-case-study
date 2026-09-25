import type { Message } from '@aws-sdk/client-sqs';
import { execFile } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import pino, { type Logger } from 'pino';
import { createSqs } from './aws';
import { loadIngestConfig } from './config';
import { pollOnce, sqsEventFrom } from './queue';

const invokePath = path.join(path.dirname(fileURLToPath(import.meta.url)), 'invoke.ts');

export interface InvokeOptions {
  /** Hard wall-clock limit. The child is SIGKILLed on overrun, like a Lambda timeout. */
  timeoutMs: number;
  /** V8 heap cap passed as --max-old-space-size, like a Lambda memory limit. Omitted where no cap is needed. */
  memoryMb?: number;
  logger?: Logger;
  /** Label used in log lines and the rejection message; defaults to the first script argument. */
  label?: string;
}

interface ExecFileFailure extends Error {
  killed?: boolean;
  signal?: NodeJS.Signals | null;
  code?: string | number | null;
}

function describeFailure(err: ExecFileFailure, timeoutMs: number): string {
  if (err.code === 'ERR_CHILD_PROCESS_STDIO_MAXBUFFER') return 'maxBuffer exceeded';
  if (err.killed) return `timeout after ${timeoutMs}ms`;
  if (err.signal) return `signal ${err.signal}`;
  return `exit ${err.code}`;
}

/**
 * Runs one Lambda-handler invocation in a fresh child process with a hard timeout and a memory cap, mimicking
 * the Lambda execution environment locally.
 *
 * Launches `node --import tsx <scriptArgs...>` directly rather than the `tsx` CLI binary: the CLI is itself a
 * wrapper that spawns a second Node process to actually run the loaded file, so a timeout that SIGKILLs only
 * the CLI process leaves that grandchild running, reparented to init. Running node directly means the process
 * this function's `timeout`/`killSignal` targets IS the process executing the handler, so an overrun actually
 * stops it. Exported (and taking raw `scriptArgs` rather than a fixed handler name) so tests can point it at
 * a throwaway script instead of the real Lambda dispatcher.
 */
export function invokeHandler(scriptArgs: string[], event: unknown, opts: InvokeOptions): Promise<{ ms: number }> {
  const label = opts.label ?? scriptArgs[0] ?? 'invoke';
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const child = execFile(process.execPath, ['--import', 'tsx', ...scriptArgs], {
      timeout: opts.timeoutMs,
      killSignal: 'SIGKILL',
      maxBuffer: 16 * 1024 * 1024,
      env: opts.memoryMb ? { ...process.env, NODE_OPTIONS: `--max-old-space-size=${opts.memoryMb}` } : process.env,
    }, (err, stdout, stderr) => {
      const ms = Date.now() - started;
      // Forwarded (buffered, at the end) so the invoked handler's own pino logs are visible locally.
      if (stdout) process.stdout.write(stdout);
      if (stderr) process.stderr.write(stderr);
      if (err) {
        const why = describeFailure(err, opts.timeoutMs);
        opts.logger?.error({ handler: label, ms, why }, 'invocation failed');
        reject(new Error(`${label}: ${why}`));
      } else {
        opts.logger?.info({ handler: label, ms }, 'invocation ok');
        resolve({ ms });
      }
    });
    child.stdin!.end(JSON.stringify(event ?? null));
  });
}

type HandlerName = 'splitter' | 'worker' | 'deadLetter';

/**
 * Matches infra/template.yaml's per-function Timeout: SplitterFunction and DeadLetterFunction are fixed at
 * 30s there, while WorkerFunction's matches the shared LAMBDA_TIMEOUT_MS config (60s by default).
 */
function handlerTimeoutMs(handler: HandlerName, lambdaTimeoutMs: number): number {
  return handler === 'worker' ? lambdaTimeoutMs : 30_000;
}

const isMainModule = process.argv[1] != null && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);

// Guarded so importing this module (e.g. from a test, to reach `invokeHandler`) never starts the local
// poller service as a side effect.
if (isMainModule) {
  const config = loadIngestConfig();
  const logger = pino({ level: config.logLevel });
  const sqs = createSqs(config);

  const invoke = (handler: HandlerName, event: unknown): Promise<void> =>
    invokeHandler([invokePath, handler], event, {
      timeoutMs: handlerTimeoutMs(handler, config.lambdaTimeoutMs),
      memoryMb: config.lambdaMemoryMb,
      logger,
      label: handler,
    }).then(() => undefined);

  let stopping = false;
  process.on('SIGINT', () => { stopping = true; });
  process.on('SIGTERM', () => { stopping = true; });

  const loop = async (name: string, queueUrl: string, handle: (m: Message) => Promise<void>) => {
    logger.info({ name, queueUrl }, 'polling');
    while (!stopping) {
      try {
        await pollOnce(sqs, queueUrl, handle);
      } catch (err) {
        logger.error({ err, name }, 'poll failed; backing off');
        await new Promise((r) => setTimeout(r, 2000));
      }
    }
  };

  await Promise.all([
    loop('s3-events', config.s3EventsQueueUrl, async (m) => {
      // A malformed body must not loop forever: this local queue has no redrive/DLQ of its own, so log and
      // drop it (the handler resolves, and pollOnce deletes the message) instead of throwing.
      let body: unknown;
      try {
        body = JSON.parse(m.Body ?? '{}');
      } catch (err) {
        logger.error({ err, body: m.Body }, 's3-events message is not valid JSON; dropping');
        return;
      }
      if (!Array.isArray((body as { Records?: unknown }).Records)) return; // LocalStack sends an s3:TestEvent on configuration
      await invoke('splitter', body);
    }),
    loop('chunks', config.chunkQueueUrl, (m) => invoke('worker', sqsEventFrom(m))),
    loop('dead-letter', config.dlqUrl, (m) => invoke('deadLetter', sqsEventFrom(m))),
  ]);
}
