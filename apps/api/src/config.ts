import { hostname } from 'node:os';
import { READ_THROUGH_LOCK_MS } from '@modaco/core';
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
  DB_POOL_ACQUIRE_TIMEOUT_MS: z.coerce.number().int().nonnegative().default(2000),
  DB_STATEMENT_TIMEOUT_MS: z.coerce.number().int().nonnegative().default(5000),
  DB_JIT: z.enum(['on', 'off', 'true', 'false', '1', '0']).default('off'),
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
  /**
   * Load shedding. A pool acquire that waits longer than dbPoolAcquireTimeoutMs fails (503) instead
   * of queueing without bound, and Postgres cancels any statement past dbStatementTimeoutMs (503).
   * 0 disables either one.
   *
   * Invariant: readThrough's lock TTL (30 s) must stay above the worst-case build, or the lock
   * expires under a live holder and a waiter takes it over and builds again (`lockTtlWarning`
   * logs at startup when this is violated). The longest build is a product detail, three sequential queries (category lookup, fetch by id, promotion boundary), each bounded by
   * acquire + statement timeout: 3 x (2 s + 5 s) = 21 s with the defaults. A list page is two
   * phases (page with count in parallel, then boundary), 14 s. Raise the timeouts only with that in mind.
   */
  dbPoolAcquireTimeoutMs: number;
  dbStatementTimeoutMs: number;
  /** Postgres JIT for the API's connections. Off: it added 83-131 ms to every cold page build. */
  dbJit: boolean;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const e = schema.parse(env);
  return {
    port: e.PORT, databaseUrl: e.DATABASE_URL, redisUrl: e.REDIS_URL, awsRegion: e.AWS_REGION,
    s3PublicEndpoint: e.S3_PUBLIC_ENDPOINT, s3Bucket: e.S3_BUCKET, logLevel: e.LOG_LEVEL,
    instanceId: e.INSTANCE_ID ?? hostname(),
    dbPoolAcquireTimeoutMs: e.DB_POOL_ACQUIRE_TIMEOUT_MS, dbStatementTimeoutMs: e.DB_STATEMENT_TIMEOUT_MS,
    dbJit: ['on', 'true', '1'].includes(e.DB_JIT),
  };
}

/** A product detail build runs three sequential queries (category lookup, fetch by id, promotion boundary). */
const MAX_SEQUENTIAL_BUILD_QUERIES = 3;

/**
 * Checks the invariant documented on `Config`: the worst-case bounded cache build must finish inside
 * the build lock's TTL. Returns a warning to log at startup, or undefined when it holds.
 */
export function lockTtlWarning(config: Config): string | undefined {
  const { dbPoolAcquireTimeoutMs: acquire, dbStatementTimeoutMs: statement } = config;
  if (acquire === 0 || statement === 0) {
    return `cache builds are unbounded (DB_POOL_ACQUIRE_TIMEOUT_MS=${acquire}, DB_STATEMENT_TIMEOUT_MS=${statement}); a slow build can outlive its ${READ_THROUGH_LOCK_MS} ms lock and a waiter will build it again`;
  }
  const worst = MAX_SEQUENTIAL_BUILD_QUERIES * (acquire + statement);
  if (worst >= READ_THROUGH_LOCK_MS) {
    return `worst-case cache build ${MAX_SEQUENTIAL_BUILD_QUERIES} x (${acquire} + ${statement}) = ${worst} ms is not below the ${READ_THROUGH_LOCK_MS} ms lock TTL; a slow build can outlive its lock and a waiter will build it again`;
  }
  return undefined;
}
