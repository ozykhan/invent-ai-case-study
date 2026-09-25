import type { RequestHandler, Response } from 'express';
import type { ZodTypeAny } from 'zod';
import { HttpError } from '../errors';

interface Schemas { body?: ZodTypeAny; query?: ZodTypeAny; params?: ZodTypeAny }

export function validate(schemas: Schemas): RequestHandler {
  return (req, res, next) => {
    const parsed: Record<string, unknown> = {};
    for (const part of ['params', 'query', 'body'] as const) {
      const schema = schemas[part];
      if (!schema) continue;
      const result = schema.safeParse(req[part]);
      if (!result.success) {
        const details = result.error.issues.map((i) => ({ path: [part, ...i.path].join('.'), message: i.message }));
        return next(new HttpError(400, 'validation_error', `invalid ${part}`, details));
      }
      parsed[part] = result.data;
    }
    res.locals.input = parsed;
    next();
  };
}

export function input<B = unknown, Q = unknown, P = unknown>(res: Response): { body: B; query: Q; params: P } {
  return res.locals.input as { body: B; query: Q; params: P };
}
