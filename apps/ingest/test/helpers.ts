import { PutObjectCommand } from '@aws-sdk/client-s3';
import { DeleteMessageCommand, ReceiveMessageCommand, type Message } from '@aws-sdk/client-sqs';
import { sql } from 'drizzle-orm';
import { ingestionJobs, runMigrations } from '@modaco/core';
import { loadIngestConfig } from '../src/config';
import { createIngestDeps, type IngestDeps } from '../src/deps';

export interface IngestTestContext {
  deps: IngestDeps;
  truncateAll(): Promise<void>;
  drainQueue(url: string): Promise<void>;
  receiveAll(url: string, expected: number, timeoutMs?: number): Promise<Message[]>;
  putObject(key: string, body: string | Buffer): Promise<void>;
  createJob(key: string): Promise<string>;
  close(): Promise<void>;
}

export async function setupIngestTest(env: Record<string, string> = {}): Promise<IngestTestContext> {
  const config = loadIngestConfig({ ...process.env, AWS_ENDPOINT_URL: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566', LOG_LEVEL: 'silent', ...env });
  const real = await createIngestDeps(config);
  await runMigrations(real.db);
  const receive = (url: string) => real.sqs.send(new ReceiveMessageCommand({ QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
  const del = (url: string, m: Message) => real.sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: m.ReceiptHandle! }));
  return {
    deps: real,
    truncateAll: async () => {
      await real.db.execute(sql`truncate ingestion_rejections, ingestion_chunks, ingestion_jobs, promotions, products, categories restart identity cascade`);
      await real.redis.flushdb();
    },
    drainQueue: async (url) => {
      for (;;) {
        const res = await receive(url);
        if (!res.Messages?.length) return;
        await Promise.all(res.Messages.map((m) => del(url, m)));
      }
    },
    receiveAll: async (url, expected, timeoutMs = 20_000) => {
      const out: Message[] = [];
      const deadline = Date.now() + timeoutMs;
      while (out.length < expected && Date.now() < deadline) {
        const res = await receive(url);
        for (const m of res.Messages ?? []) { out.push(m); await del(url, m); }
      }
      return out;
    },
    putObject: async (key, body) => { await real.s3.send(new PutObjectCommand({ Bucket: config.s3Bucket, Key: key, Body: body })); },
    createJob: async (key) => {
      const [job] = await real.db.insert(ingestionJobs).values({ s3Key: key }).returning({ id: ingestionJobs.id });
      return job!.id;
    },
    close: () => real.close(),
  };
}
