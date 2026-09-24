import type { RequestHandler } from 'express';
import { randomUUID } from 'node:crypto';

export const requestId: RequestHandler = (req, res, next) => {
  const id = (req.header('x-request-id') ?? randomUUID()).slice(0, 64);
  res.locals.requestId = id;
  res.setHeader('x-request-id', id);
  next();
};
