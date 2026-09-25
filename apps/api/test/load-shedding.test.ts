import { CacheWaitTimeoutError, createRedis, keys, type Db } from '@modaco/core';
import { sql } from 'drizzle-orm';
import express, { type NextFunction, type Request, type Response } from 'express';
import { request as httpRequest, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import request from 'supertest';
import { afterAll, describe, expect, it, vi } from 'vitest';
import { loadConfig } from '../src/config';
import { createDeps, type AppDeps } from '../src/deps';
import { createLogger } from '../src/logger';
import { ClientGoneError, clientAbort, clientSignal } from '../src/middleware/client-abort';
import { errorHandler } from '../src/middleware/error-handler';
import { getCachedProduct, getCachedProductPage, resolveCategoryId } from '../src/products/cached-reads';
import { ProductService } from '../src/products/service';
import { loadStocks } from '../src/products/stock';

describe('database load-shedding config', () => {
  it('defaults to a 2 s pool acquire timeout, a 5 s statement timeout and JIT off', () => {
    const c = loadConfig({});
    expect(c.dbPoolAcquireTimeoutMs).toBe(2000);
    expect(c.dbStatementTimeoutMs).toBe(5000);
    expect(c.dbJit).toBe(false);
  });

  it('reads DB_POOL_ACQUIRE_TIMEOUT_MS, DB_STATEMENT_TIMEOUT_MS and DB_JIT', () => {
    const c = loadConfig({ DB_POOL_ACQUIRE_TIMEOUT_MS: '1500', DB_STATEMENT_TIMEOUT_MS: '3000', DB_JIT: 'on' });
    expect(c.dbPoolAcquireTimeoutMs).toBe(1500);
    expect(c.dbStatementTimeoutMs).toBe(3000);
    expect(c.dbJit).toBe(true);
    for (const v of ['off', 'false', '0']) expect(loadConfig({ DB_JIT: v }).dbJit).toBe(false);
    for (const v of ['on', 'true', '1']) expect(loadConfig({ DB_JIT: v }).dbJit).toBe(true);
  });

  it('rejects malformed values instead of silently disabling a timeout', () => {
    expect(() => loadConfig({ DB_POOL_ACQUIRE_TIMEOUT_MS: 'soon' })).toThrow();
    expect(() => loadConfig({ DB_STATEMENT_TIMEOUT_MS: '-1' })).toThrow();
    expect(() => loadConfig({ DB_JIT: 'maybe' })).toThrow();
  });
});

describe("the API's database pool", () => {
  let deps: (AppDeps & { close(): Promise<void> }) | undefined;
  afterAll(() => deps?.close());

  it('runs every query with JIT off and the configured statement timeout', async () => {
    deps = await createDeps(loadConfig({ ...process.env, LOG_LEVEL: 'silent' }));
    const jit = await deps.db.execute(sql`show jit`);
    const timeout = await deps.db.execute(sql`show statement_timeout`);
    expect(jit.rows[0]).toEqual({ jit: 'off' });
    expect(timeout.rows[0]).toEqual({ statement_timeout: '5s' });
  });
});

describe('overload error mapping', () => {
  const app = (err: unknown) => {
    const a = express();
    a.get('/boom', () => { throw err; });
    a.use(errorHandler(createLogger('silent')));
    return a;
  };
  const expectOverloaded = async (err: unknown) => {
    const res = await request(app(err)).get('/boom');
    expect(res.status).toBe(503);
    expect(res.headers['retry-after']).toBe('1');
    expect(res.body.error.code).toBe('overloaded');
  };
  const drizzleWrapped = (cause: Error) => Object.assign(new Error('Failed query: select 1'), { cause });

  it('503 overloaded with Retry-After: 1 when a cache waiter gives up', () =>
    expectOverloaded(new CacheWaitTimeoutError('list:1:asc:1:20', 5000)));

  it('503 overloaded when the pool acquire times out, raw or wrapped by drizzle', async () => {
    await expectOverloaded(new Error('timeout exceeded when trying to connect'));
    await expectOverloaded(drizzleWrapped(new Error('timeout exceeded when trying to connect')));
  });

  it('503 overloaded when Postgres cancels a statement (SQLSTATE 57014), raw or wrapped by drizzle', async () => {
    const pgErr = Object.assign(new Error('canceling statement due to statement timeout'), { code: '57014' });
    await expectOverloaded(pgErr);
    await expectOverloaded(drizzleWrapped(pgErr));
  });

  it('503 database_unavailable when a new pool connection times out while connecting, raw or wrapped by drizzle', async () => {
    // pg-pool's error when connectionTimeoutMillis expires during a *new* connection's handshake: no SQLSTATE.
    const connectTimeout = () => new Error('Connection terminated due to connection timeout', { cause: new Error('timeout expired') });
    for (const err of [connectTimeout(), drizzleWrapped(connectTimeout())]) {
      const res = await request(app(err)).get('/boom');
      expect(res.status).toBe(503);
      expect(res.body.error.code).toBe('database_unavailable');
    }
  });

  it('keeps 503 database_unavailable for connection failures', async () => {
    const res = await request(app(Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' }))).get('/boom');
    expect(res.status).toBe(503);
    expect(res.body.error.code).toBe('database_unavailable');
    expect(res.headers['retry-after']).toBeUndefined();
  });

  it('writes nothing for a request whose client has gone, logging it at debug only', () => {
    const logger = createLogger('silent');
    const debug = vi.spyOn(logger, 'debug');
    const error = vi.spyOn(logger, 'error');
    const res = { locals: {}, status: vi.fn(), json: vi.fn(), set: vi.fn(), end: vi.fn() };
    errorHandler(logger)(new ClientGoneError(), {} as Request, res as unknown as Response, (() => {}) as NextFunction);
    expect(res.status).not.toHaveBeenCalled();
    expect(res.json).not.toHaveBeenCalled();
    expect(res.end).not.toHaveBeenCalled();
    expect(debug).toHaveBeenCalledTimes(1);
    expect(error).not.toHaveBeenCalled();
  });
});

describe('client abort propagation', () => {
  // Any database access at all fails the test: an aborted request must not reach Postgres.
  const noDb = new Proxy({}, { get: (_t, prop) => { throw new Error(`database touched: ${String(prop)}`); } }) as Db;
  const deps = (): AppDeps => ({
    db: noDb, redis: null, presigner: {} as AppDeps['presigner'], config: loadConfig({}),
    logger: createLogger('silent'), now: () => new Date(),
  });
  const aborted = () => { const c = new AbortController(); c.abort(); return c.signal; };

  it('skips every database phase of the product read path once the client has gone', async () => {
    await expect(getCachedProductPage(deps(), { categoryId: 1, sort: 'asc', page: 1, pageSize: 20 }, aborted()))
      .rejects.toBeInstanceOf(ClientGoneError);
    await expect(getCachedProduct(deps(), 1, aborted())).rejects.toBeInstanceOf(ClientGoneError);
    await expect(resolveCategoryId(deps(), 'shoes', aborted())).rejects.toBeInstanceOf(ClientGoneError);
    await expect(loadStocks(deps(), [1, 2], aborted())).rejects.toBeInstanceOf(ClientGoneError);
    const service = new ProductService(deps());
    await expect(service.listProducts({ category: 'shoes', sort: 'effective_price', page: 1, pageSize: 20 }, aborted()))
      .rejects.toBeInstanceOf(ClientGoneError);
    await expect(service.getProduct(1, aborted())).rejects.toBeInstanceOf(ClientGoneError);
  });

  it('stops a cache waiter as soon as its client goes, instead of polling out the 5 s wait', async () => {
    const redis = createRedis(process.env.REDIS_URL ?? 'redis://localhost:6379');
    await redis.connect();
    const lock = keys.lock(keys.list(987654, 'asc', 1, 20)); // a key no real category uses
    await redis.set(lock, 'another-request', 'PX', 5000);
    try {
      const controller = new AbortController();
      setTimeout(() => controller.abort(new ClientGoneError()), 50);
      const start = Date.now();
      await expect(getCachedProductPage({ ...deps(), redis }, { categoryId: 987654, sort: 'asc', page: 1, pageSize: 20 }, controller.signal))
        .rejects.toBeInstanceOf(ClientGoneError);
      expect(Date.now() - start).toBeLessThan(1000);
    } finally {
      await redis.del(lock);
      await redis.quit();
    }
  });

  describe('the clientAbort middleware', () => {
    let server: Server | undefined;
    afterAll(() => new Promise<void>((r) => (server ? server.close(() => r()) : r())));

    const listen = async (handler: (signal: AbortSignal, res: Response) => void) => {
      const app = express().use(clientAbort);
      app.get('/', (_req, res) => handler(clientSignal(res)!, res));
      server = app.listen(0);
      await new Promise((r) => server!.once('listening', r));
      return (server.address() as AddressInfo).port;
    };

    it('aborts the request signal when the client disconnects before the response is written', async () => {
      let seen: AbortSignal | undefined;
      const abortedSeen = new Promise<void>((resolve) => {
        void listen((signal) => { seen = signal; signal.addEventListener('abort', () => resolve()); }).then((port) => {
          const req = httpRequest({ port, path: '/' });
          req.on('error', () => {});
          req.end();
          setTimeout(() => req.destroy(), 50);
        });
      });
      await abortedSeen;
      expect(seen?.aborted).toBe(true);
      expect(seen?.reason).toBeInstanceOf(ClientGoneError);
      await new Promise<void>((r) => server!.close(() => r()));
      server = undefined;
    });

    it('does not abort the signal of a response that finished normally', async () => {
      let seen: AbortSignal | undefined;
      const port = await listen((signal, res) => { seen = signal; res.json({ ok: true }); });
      const res = await fetch(`http://localhost:${port}/`);
      expect(res.status).toBe(200);
      await new Promise((r) => setTimeout(r, 20));
      expect(seen?.aborted).toBe(false);
    });
  });
});
