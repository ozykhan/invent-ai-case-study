import { S3Client } from '@aws-sdk/client-s3';
import { createDb, createRedis, type Db, type Redis } from '@modaco/core';
import type { Config } from './config';
import { createLogger, type Logger } from './logger';

export interface AppDeps {
  db: Db;
  redis: Redis | null;
  s3: S3Client;
  presigner: S3Client;
  config: Config;
  logger: Logger;
  now: () => Date;
}

export async function createDeps(config: Config): Promise<AppDeps & { close(): Promise<void> }> {
  const logger = createLogger(config.logLevel);
  const { db, close: closeDb } = createDb(config.databaseUrl, { max: 10 });
  const redis = createRedis(config.redisUrl);
  redis.on('error', (err) => logger.warn({ err }, 'redis error'));
  await redis.connect().catch((err) => logger.warn({ err }, 'redis initial connect failed; continuing degraded'));
  const s3Opts = { region: config.awsRegion, forcePathStyle: true, credentials: { accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test', secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test' } };
  const s3 = new S3Client({ ...s3Opts, endpoint: config.awsEndpointUrl });
  const presigner = new S3Client({ ...s3Opts, endpoint: config.s3PublicEndpoint });
  return {
    db, redis, s3, presigner, config, logger, now: () => new Date(),
    close: async () => { await closeDb(); redis.disconnect(); },
  };
}
