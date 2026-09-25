import { hostname } from 'node:os';
import { z } from 'zod';

const schema = z.object({
  PORT: z.coerce.number().default(3000),
  DATABASE_URL: z.string().default('postgres://modaco:modaco@localhost:5433/modaco'),
  REDIS_URL: z.string().default('redis://localhost:6379'),
  AWS_REGION: z.string().default('us-east-1'),
  S3_PUBLIC_ENDPOINT: z.string().default('http://localhost:4566'),
  S3_BUCKET: z.string().default('modaco-vendor-uploads'),
  LOG_LEVEL: z.string().default('info'),
  // An empty string (e.g. INSTANCE_ID="" from an unset compose/k8s substitution) is treated the same as unset,
  // rather than failing min(1) validation: it still falls back to the hostname below.
  INSTANCE_ID: z.preprocess((v) => (v === '' ? undefined : v), z.string().min(1).optional()),
});

export interface Config {
  port: number;
  databaseUrl: string;
  redisUrl: string;
  awsRegion: string;
  s3PublicEndpoint: string;
  s3Bucket: string;
  logLevel: string;
  /** Sent as X-Instance-Id. In Docker the hostname is the container id, unique per replica. */
  instanceId: string;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const e = schema.parse(env);
  return {
    port: e.PORT, databaseUrl: e.DATABASE_URL, redisUrl: e.REDIS_URL, awsRegion: e.AWS_REGION,
    s3PublicEndpoint: e.S3_PUBLIC_ENDPOINT, s3Bucket: e.S3_BUCKET, logLevel: e.LOG_LEVEL,
    instanceId: e.INSTANCE_ID ?? hostname(),
  };
}
