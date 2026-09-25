import type { RequestHandler } from 'express';

/** Tags every response with the serving instance, so a load test behind a load balancer can see how requests spread. */
export function instanceId(id: string): RequestHandler {
  return (_req, res, next) => {
    res.setHeader('x-instance-id', id);
    next();
  };
}
