import type { S3Client } from '@aws-sdk/client-s3';
import type { SQSClient } from '@aws-sdk/client-sqs';
import { createDb, createRedis, type Db, type Redis } from '@modaco/core';
import pino, { type Logger } from 'pino';
import { createS3, createSqs } from './aws';
import { loadIngestConfig, type IngestConfig } from './config';

export interface IngestDeps {
  db: Db;
  redis: Redis;
  s3: S3Client;
  sqs: SQSClient;
  config: IngestConfig;
  logger: Logger;
}

export async function createIngestDeps(config: IngestConfig): Promise<IngestDeps & { close(): Promise<void> }> {
  const logger = pino({ level: config.logLevel });
  const { db, close: closeDb } = createDb(config.databaseUrl, { max: 2 });
  const redis = createRedis(config.redisUrl);
  redis.on('error', (err) => logger.warn({ err }, 'redis error'));
  await redis.connect().catch((err) => logger.warn({ err }, 'redis connect failed; version bumps will be retried'));
  return {
    db, redis, s3: createS3(config), sqs: createSqs(config), config, logger,
    close: async () => { await closeDb(); redis.disconnect(); },
  };
}

let singleton: Promise<IngestDeps> | undefined;
/** Reused across warm invocations of the same Lambda container. */
export function getDeps(): Promise<IngestDeps> {
  singleton ??= createIngestDeps(loadIngestConfig());
  return singleton;
}
