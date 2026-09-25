import { Router } from 'express';
import type { AppDeps } from '../deps';
import { notFound } from '../errors';
import { input, validate } from '../middleware/validate';
import { createPromotionBody, promotionIdParam, targetSchema, type CreatePromotionBody, type Target } from './schemas';
import { PromotionService } from './service';

export function promotionRoutes(deps: AppDeps): Router {
  const r = Router();
  const service = new PromotionService(deps);

  r.post('/promotions', validate({ body: createPromotionBody }), async (_req, res) => {
    const { body } = input<CreatePromotionBody>(res);
    res.status(201).json(await service.create(body));
  });

  r.get('/promotions/:id', validate({ params: promotionIdParam }), async (_req, res) => {
    const { params } = input<unknown, unknown, { id: string }>(res);
    const view = await service.get(params.id);
    if (!view) throw notFound(`promotion ${params.id} not found`);
    res.json(view);
  });

  r.post('/promotions/:id/cancel', validate({ params: promotionIdParam }), async (_req, res) => {
    const { params } = input<unknown, unknown, { id: string }>(res);
    const view = await service.cancel(params.id);
    if (!view) throw notFound(`promotion ${params.id} not found`);
    res.json(view);
  });

  r.put('/promotions/:id/target', validate({ params: promotionIdParam, body: targetSchema }), async (_req, res) => {
    const { params, body } = input<Target, unknown, { id: string }>(res);
    const view = await service.assign(params.id, body);
    if (!view) throw notFound(`promotion ${params.id} not found`);
    res.json(view);
  });

  return r;
}
