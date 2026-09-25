import type { RequestHandler, Response } from 'express';

/**
 * The client closed the connection before the response was written. Thrown before a database
 * phase so work nobody will receive is skipped; the error handler swallows it and writes nothing.
 */
export class ClientGoneError extends Error {
  constructor() {
    super('client closed the connection before the response was written');
    this.name = 'ClientGoneError';
  }
}

/**
 * Gives each request an AbortSignal (in res.locals) that fires when the connection closes before
 * the response finished. Without it, a handler for a client that timed out keeps queueing queries,
 * and an overload outlives its traffic by the length of that backlog.
 */
export const clientAbort: RequestHandler = (_req, res, next) => {
  const controller = new AbortController();
  res.on('close', () => {
    if (!res.writableFinished) controller.abort();
  });
  res.locals.signal = controller.signal;
  next();
};

export function clientSignal(res: Response): AbortSignal | undefined {
  return res.locals.signal as AbortSignal | undefined;
}

/** Call before each database phase. Queries already running are bounded by statement_timeout instead. */
export function throwIfClientGone(signal: AbortSignal | undefined): void {
  if (signal?.aborted) throw new ClientGoneError();
}
