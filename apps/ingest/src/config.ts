import { z } from 'zod';

const schema = z.object({
  DATABASE_URL: z.string().default('postgres://modaco:modaco@localhost:5433/modaco'),
  REDIS_URL: z.string().default('redis://localhost:6379'),
  AWS_REGION: z.string().default('us-east-1'),
  AWS_ENDPOINT_URL: z.string().optional(),
  S3_BUCKET: z.string().default('modaco-vendor-uploads'),
  S3_EVENTS_QUEUE_URL: z.string().default('http://localhost:4566/000000000000/modaco-s3-events'),
  CHUNK_QUEUE_URL: z.string().default('http://localhost:4566/000000000000/modaco-ingest-chunks'),
  DLQ_URL: z.string().default('http://localhost:4566/000000000000/modaco-ingest-dlq'),
  CHUNK_SIZE_BYTES: z.coerce.number().int().positive().default(4_194_304),
  UPSERT_BATCH_SIZE: z.coerce.number().int().positive().default(1000),
  LAMBDA_TIMEOUT_MS: z.coerce.number().int().positive().default(60_000),
  LAMBDA_MEMORY_MB: z.coerce.number().int().positive().default(256),
  LOG_LEVEL: z.string().default('info'),
});

export interface IngestConfig {
  databaseUrl: string; redisUrl: string; awsRegion: string; awsEndpointUrl: string | undefined; s3Bucket: string;
  s3EventsQueueUrl: string; chunkQueueUrl: string; dlqUrl: string; chunkSizeBytes: number; upsertBatchSize: number;
  lambdaTimeoutMs: number; lambdaMemoryMb: number; logLevel: string;
}

export function loadIngestConfig(env: NodeJS.ProcessEnv = process.env): IngestConfig {
  const e = schema.parse(env);
  return {
    databaseUrl: e.DATABASE_URL, redisUrl: e.REDIS_URL, awsRegion: e.AWS_REGION, awsEndpointUrl: e.AWS_ENDPOINT_URL,
    s3Bucket: e.S3_BUCKET, s3EventsQueueUrl: e.S3_EVENTS_QUEUE_URL, chunkQueueUrl: e.CHUNK_QUEUE_URL, dlqUrl: e.DLQ_URL,
    chunkSizeBytes: e.CHUNK_SIZE_BYTES, upsertBatchSize: e.UPSERT_BATCH_SIZE, lambdaTimeoutMs: e.LAMBDA_TIMEOUT_MS,
    lambdaMemoryMb: e.LAMBDA_MEMORY_MB, logLevel: e.LOG_LEVEL,
  };
}
