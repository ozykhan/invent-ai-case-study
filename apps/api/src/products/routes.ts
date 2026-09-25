import { Router } from 'express';
import type { AppDeps } from '../deps';
import { notFound } from '../errors';
import { clientSignal } from '../middleware/client-abort';
import { input, validate } from '../middleware/validate';
import { createProductBody, idParam, listQuery, stockBody, type CreateProductBody, type ListQuery, type StockBody } from './schemas';
import { ProductService } from './service';

export function productRoutes(deps: AppDeps): Router {
  const r = Router();
  const service = new ProductService(deps);

  r.get('/products', validate({ query: listQuery }), async (_req, res) => {
    const { query } = input<unknown, ListQuery>(res);
    res.json(await service.listProducts(query, clientSignal(res)));
  });

  r.get('/products/:id', validate({ params: idParam }), async (_req, res) => {
    const { params } = input<unknown, unknown, { id: number }>(res);
    const item = await service.getProduct(params.id, clientSignal(res));
    if (!item) throw notFound(`product ${params.id} not found`);
    res.json(item);
  });

  r.post('/products', validate({ body: createProductBody }), async (_req, res) => {
    const { body } = input<CreateProductBody>(res);
    res.status(201).json(await service.createProduct(body));
  });

  r.patch('/products/:id/stock', validate({ params: idParam, body: stockBody }), async (_req, res) => {
    const { params, body } = input<StockBody, unknown, { id: number }>(res);
    const result = await service.adjustStock(params.id, body);
    if (!result) throw notFound(`product ${params.id} not found`);
    res.json(result);
  });

  return r;
}
