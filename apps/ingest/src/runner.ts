import { execFile } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import pino from 'pino';
import { createSqs } from './aws';
import { loadIngestConfig } from './config';
import { pollOnce, sqsEventFrom } from './queue';

/**
 * Local stand-in for the Lambda service. Every invocation runs in a fresh child process with a hard timeout
 * (SIGKILL on overrun, like a Lambda timeout) and a V8 heap cap (like the Lambda memory limit).
 */
const config = loadIngestConfig();
const logger = pino({ level: config.logLevel });
const sqs = createSqs(config);
const invokePath = path.join(path.dirname(fileURLToPath(import.meta.url)), 'invoke.ts');

function invoke(handler: 'splitter' | 'worker' | 'deadLetter', event: unknown): Promise<void> {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const child = execFile('tsx', [invokePath, handler], {
      timeout: config.lambdaTimeoutMs,
      killSignal: 'SIGKILL',
      maxBuffer: 16 * 1024 * 1024,
      env: { ...process.env, NODE_OPTIONS: `--max-old-space-size=${config.lambdaMemoryMb}` },
    }, (err, _stdout, stderr) => {
      const ms = Date.now() - started;
      if (err) {
        const why = err.killed ? `timeout after ${config.lambdaTimeoutMs}ms` : `exit ${err.code}`;
        logger.error({ handler, ms, why, stderr: stderr.slice(-2000) }, 'invocation failed');
        reject(new Error(`${handler}: ${why}`));
      } else {
        logger.info({ handler, ms }, 'invocation ok');
        resolve();
      }
    });
    child.stdin!.end(JSON.stringify(event));
  });
}

let stopping = false;
process.on('SIGINT', () => { stopping = true; });
process.on('SIGTERM', () => { stopping = true; });

async function loop(name: string, queueUrl: string, handle: (m: import('@aws-sdk/client-sqs').Message) => Promise<void>) {
  logger.info({ name, queueUrl }, 'polling');
  while (!stopping) {
    try {
      await pollOnce(sqs, queueUrl, handle);
    } catch (err) {
      logger.error({ err, name }, 'poll failed; backing off');
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
}

await Promise.all([
  loop('s3-events', config.s3EventsQueueUrl, async (m) => {
    const body = JSON.parse(m.Body ?? '{}');
    if (!Array.isArray(body.Records)) return; // LocalStack sends an s3:TestEvent on configuration
    await invoke('splitter', body);
  }),
  loop('chunks', config.chunkQueueUrl, (m) => invoke('worker', sqsEventFrom(m))),
  loop('dead-letter', config.dlqUrl, (m) => invoke('deadLetter', sqsEventFrom(m))),
]);
