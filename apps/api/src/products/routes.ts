import { Router } from 'express';
import type { AppDeps } from '../deps';
import { notFound } from '../errors';
import { input, validate } from '../middleware/validate';
import { idParam, listQuery, type ListQuery } from './schemas';
import { ProductService } from './service';

export function productRoutes(deps: AppDeps): Router {
  const r = Router();
  const service = new ProductService(deps);

  r.get('/products', validate({ query: listQuery }), async (_req, res) => {
    const { query } = input<unknown, ListQuery>(res);
    res.json(await service.listProducts(query));
  });

  r.get('/products/:id', validate({ params: idParam }), async (_req, res) => {
    const { params } = input<unknown, unknown, { id: number }>(res);
    const item = await service.getProduct(params.id);
    if (!item) throw notFound(`product ${params.id} not found`);
    res.json(item);
  });

  return r;
}
