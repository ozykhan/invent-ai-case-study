import type { ErrorRequestHandler } from 'express';
import type { Logger } from '../logger';
import { HttpError, pgErrorCode } from '../errors';

const DB_UNAVAILABLE = new Set(['ECONNREFUSED', 'ETIMEDOUT', '57P01', '57P02', '57P03', '08006', '08001']);

export function errorHandler(logger: Logger): ErrorRequestHandler {
  return (err, _req, res, _next) => {
    const requestId = res.locals.requestId as string | undefined;
    if (err instanceof HttpError) {
      res.status(err.status).json({ error: { code: err.code, message: err.message, details: err.details } });
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
