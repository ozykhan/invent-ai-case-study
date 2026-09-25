import { Router } from 'express';
import { z } from 'zod';
import type { AppDeps } from '../deps';
import { notFound } from '../errors';
import { input, validate } from '../middleware/validate';
import { IngestionService } from './service';

const createBody = z.object({ filename: z.string().regex(/^[A-Za-z0-9._-]{1,128}$/).optional() });
const jobParam = z.object({ id: z.string().uuid() });
const pageQuery = z.object({ page: z.coerce.number().int().min(1).default(1), pageSize: z.coerce.number().int().min(1).max(100).default(20) });

export function ingestionRoutes(deps: AppDeps): Router {
  const r = Router();
  const service = new IngestionService(deps);

  r.post('/ingestion/jobs', validate({ body: createBody }), async (_req, res) => {
    const { body } = input<{ filename?: string }>(res);
    res.status(201).json(await service.createJob(body.filename));
  });

  r.get('/ingestion/jobs/:id', validate({ params: jobParam }), async (_req, res) => {
    const { params } = input<unknown, unknown, { id: string }>(res);
    const job = await service.getJob(params.id);
    if (!job) throw notFound(`job ${params.id} not found`);
    res.json(job);
  });

  r.get('/ingestion/jobs/:id/rejections', validate({ params: jobParam, query: pageQuery }), async (_req, res) => {
    const { params, query } = input<unknown, { page: number; pageSize: number }, { id: string }>(res);
    const result = await service.listRejections(params.id, query.page, query.pageSize);
    if (!result) throw notFound(`job ${params.id} not found`);
    res.json(result);
  });

  return r;
}
