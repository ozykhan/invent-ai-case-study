import { CacheWaitTimeoutError, pgErrorMessage } from '@modaco/core';
import type { ErrorRequestHandler } from 'express';
import type { Logger } from '../logger';
import { HttpError, pgErrorCode } from '../errors';
import { ClientGoneError } from './client-abort';

const DB_UNAVAILABLE = new Set(['ECONNREFUSED', 'ETIMEDOUT', '57P01', '57P02', '57P03', '08006', '08001']);
/** query_canceled: statement_timeout fired. */
const STATEMENT_TIMEOUT = '57014';
/** node-pg's pool rejects an acquire that waited past connectionTimeoutMillis with this message; drizzle wraps it in `cause`. */
const POOL_ACQUIRE_TIMEOUT = /timeout exceeded when trying to connect/i;

function isPoolAcquireTimeout(err: unknown): boolean {
  for (let e = err as { message?: unknown; cause?: unknown } | undefined, depth = 0; e && depth < 5; e = e.cause as typeof e, depth++) {
    if (typeof e.message === 'string' && POOL_ACQUIRE_TIMEOUT.test(e.message)) return true;
  }
  return false;
}

/** The request was shed, not failed: a cache waiter gave up, the pool queue was too long, or a statement ran too long. */
function isOverload(err: unknown): boolean {
  return err instanceof CacheWaitTimeoutError || isPoolAcquireTimeout(err) || pgErrorCode(err) === STATEMENT_TIMEOUT;
}

export function errorHandler(logger: Logger): ErrorRequestHandler {
  return (err, _req, res, _next) => {
    const requestId = res.locals.requestId as string | undefined;
    if (err instanceof ClientGoneError) {
      // Nobody is listening: the socket is already closed, so there is nothing to write.
      logger.debug({ requestId }, 'client gone; skipped the rest of the request');
      return;
    }
    if (err instanceof HttpError) {
      res.status(err.status).json({ error: { code: err.code, message: err.message, details: err.details } });
      return;
    }
    if (isOverload(err)) {
      // No stack: under overload this fires per request, and serialising stacks would only add load.
      logger.warn({ requestId, reason: pgErrorMessage(err) }, 'overloaded; shedding request');
      res.status(503).set('Retry-After', '1').json({ error: { code: 'overloaded', message: 'service overloaded, retry shortly' } });
      return;
    }
    const code = pgErrorCode(err);
    if (code && DB_UNAVAILABLE.has(code)) {
      logger.error({ err, requestId }, 'database unavailable');
      res.status(503).json({ error: { code: 'database_unavailable', message: 'database unavailable' } });
      return;
    }
    if (err?.type === 'entity.parse.failed') {
      res.status(400).json({ error: { code: 'validation_error', message: 'malformed JSON body' } });
      return;
    }
    logger.error({ err, requestId }, 'unhandled error');
    res.status(500).json({ error: { code: 'internal_error', message: 'internal error', requestId } });
  };
}
