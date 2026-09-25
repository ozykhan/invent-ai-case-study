import { z } from 'zod';
import { int4, moneyString } from '../schemas';

export const idParam = z.object({ id: int4(z.coerce.number()).positive() });

/** Deep offset pagination re-sorts the whole filtered set on every miss (ADR §3), so the page number is capped. */
export const MAX_PAGE = 1000;

export const listQuery = z.object({
  category: z.string().min(1).max(100).optional(),
  sort: z.enum(['effective_price', '-effective_price']).default('effective_price'),
  page: z.coerce.number().int().min(1).max(MAX_PAGE).default(1),
  pageSize: z.coerce.number().int().min(1).max(100).default(20),
});
export type ListQuery = z.infer<typeof listQuery>;

export const createProductBody = z.object({
  sku: z.string().trim().min(1).max(64),
  name: z.string().trim().min(1).max(255),
  categoryId: int4().positive(),
  basePrice: moneyString('basePrice'),
  stock: int4().min(0).default(0),
});
export type CreateProductBody = z.infer<typeof createProductBody>;

export const stockBody = z.union([
  z.object({ delta: int4() }),
  z.object({ stock: int4().min(0) }),
]);
export type StockBody = z.infer<typeof stockBody>;
