import { S3Client } from '@aws-sdk/client-s3';
import { createDb, createRedis, type Db, type Redis } from '@modaco/core';
import type { Config } from './config';
import { createLogger, type Logger } from './logger';

export interface AppDeps {
  db: Db;
  redis: Redis | null;
  /** The API's only S3 client: it signs upload URLs and never calls S3 itself. */
  presigner: S3Client;
  config: Config;
  logger: Logger;
  now: () => Date;
}

export async function createDeps(config: Config): Promise<AppDeps & { close(): Promise<void> }> {
  const logger = createLogger(config.logLevel);
  const { db, close: closeDb } = createDb(config.databaseUrl, {
    max: 10,
    connectionTimeoutMillis: config.dbPoolAcquireTimeoutMs,
    statementTimeoutMs: config.dbStatementTimeoutMs,
    jit: config.dbJit,
  });
  const redis = createRedis(config.redisUrl);
  redis.on('error', (err) => logger.warn({ err }, 'redis error'));
  await redis.connect().catch((err) => logger.warn({ err }, 'redis initial connect failed; continuing degraded'));
  // The endpoint is the one the uploader will connect to, so it is baked into every presigned URL.
  const presigner = new S3Client({
    region: config.awsRegion, forcePathStyle: true, endpoint: config.s3PublicEndpoint,
    credentials: { accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test', secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test' },
  });
  return {
    db, redis, presigner, config, logger, now: () => new Date(),
    close: async () => { await closeDb(); redis.disconnect(); },
  };
}
