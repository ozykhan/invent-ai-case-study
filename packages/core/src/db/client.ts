import { drizzle, type NodePgDatabase } from 'drizzle-orm/node-postgres';
import pg from 'pg';
import * as schema from './schema';

export type Db = NodePgDatabase<typeof schema>;
export type Tx = Parameters<Parameters<Db['transaction']>[0]>[0];
export type DbOrTx = Db | Tx;

export const TEST_DATABASE_URL = 'postgres://modaco:modaco@localhost:5433/modaco';

export interface CreateDbOptions {
  max?: number;
  /** Fail a pool acquire that waits longer than this (pg's connectionTimeoutMillis). Unset waits forever. */
  connectionTimeoutMillis?: number;
  /** Server-side statement_timeout for every pooled connection, in ms. Unset keeps the server default. */
  statementTimeoutMs?: number;
  /** Sets the session's `jit` parameter on every pooled connection. Unset keeps the server default. */
  jit?: boolean;
}

export function createDb(connectionString: string, opts: CreateDbOptions = {}) {
  const pool = new pg.Pool({
    connectionString,
    max: opts.max ?? 10,
    ...(opts.connectionTimeoutMillis !== undefined && { connectionTimeoutMillis: opts.connectionTimeoutMillis }),
    ...(opts.statementTimeoutMs !== undefined && { statement_timeout: opts.statementTimeoutMs }),
    ...(opts.jit !== undefined && { options: `-c jit=${opts.jit ? 'on' : 'off'}` }),
  });
  const db = drizzle(pool, { schema });
  return { db, pool, close: () => pool.end() };
}
