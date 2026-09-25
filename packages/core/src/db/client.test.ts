import { sql } from 'drizzle-orm';
import { describe, expect, it } from 'vitest';
import { createDb, TEST_DATABASE_URL } from './client';

const url = process.env.DATABASE_URL ?? TEST_DATABASE_URL;

async function show(db: ReturnType<typeof createDb>['db'], setting: 'jit' | 'statement_timeout'): Promise<string> {
  const res = await db.execute(sql.raw(`show ${setting}`));
  return String((res.rows[0] as Record<string, unknown>)[setting]);
}

describe('createDb', () => {
  it('passes the acquire timeout, statement timeout and JIT setting through to every pooled connection', async () => {
    const { db, pool, close } = createDb(url, { max: 1, connectionTimeoutMillis: 1500, statementTimeoutMs: 4000, jit: false });
    try {
      expect(pool.options.connectionTimeoutMillis).toBe(1500);
      expect(await show(db, 'jit')).toBe('off');
      expect(await show(db, 'statement_timeout')).toBe('4s');
    } finally {
      await close();
    }
  });

  it('cancels a statement that runs past statementTimeoutMs with SQLSTATE 57014', async () => {
    const { db, close } = createDb(url, { max: 1, statementTimeoutMs: 50 });
    try {
      const err = await db.execute(sql`select pg_sleep(1)`).catch((e: unknown) => e);
      expect((err as { cause?: { code?: string } }).cause?.code).toBe('57014');
    } finally {
      await close();
    }
  });

  it('fails a pool acquire that waits past connectionTimeoutMillis instead of queueing forever', async () => {
    const { db, pool, close } = createDb(url, { max: 1, connectionTimeoutMillis: 100 });
    const holder = await pool.connect();
    try {
      const err = await db.execute(sql`select 1`).catch((e: unknown) => e) as Error & { cause?: Error };
      expect(`${err.message} ${err.cause?.message ?? ''}`).toMatch(/timeout exceeded when trying to connect/);
    } finally {
      holder.release();
      await close();
    }
  });

  it('keeps the server defaults when no options are given', async () => {
    const { db, pool, close } = createDb(url, { max: 1 });
    try {
      expect(pool.options.connectionTimeoutMillis).toBeUndefined();
      expect(await show(db, 'statement_timeout')).toBe('0');
      expect(await show(db, 'jit')).toBe('on');
    } finally {
      await close();
    }
  });
});
