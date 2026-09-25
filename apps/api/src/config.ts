import { z } from 'zod';

const schema = z.object({
  PORT: z.coerce.number().default(3000),
  DATABASE_URL: z.string().default('postgres://modaco:modaco@localhost:5433/modaco'),
  REDIS_URL: z.string().default('redis://localhost:6379'),
  AWS_REGION: z.string().default('us-east-1'),
  S3_PUBLIC_ENDPOINT: z.string().default('http://localhost:4566'),
  S3_BUCKET: z.string().default('modaco-vendor-uploads'),
  LOG_LEVEL: z.string().default('info'),
});

export interface Config {
  port: number;
  databaseUrl: string;
  redisUrl: string;
  awsRegion: string;
  s3PublicEndpoint: string;
  s3Bucket: string;
  logLevel: string;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const e = schema.parse(env);
  return {
    port: e.PORT, databaseUrl: e.DATABASE_URL, redisUrl: e.REDIS_URL, awsRegion: e.AWS_REGION,
    s3PublicEndpoint: e.S3_PUBLIC_ENDPOINT, s3Bucket: e.S3_BUCKET, logLevel: e.LOG_LEVEL,
  };
}
