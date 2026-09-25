import { z } from 'zod';
import { moneyString } from '../schemas';

export const idParam = z.object({ id: z.coerce.number().int().positive() });

export const listQuery = z.object({
  category: z.string().min(1).max(100).optional(),
  sort: z.enum(['effective_price', '-effective_price']).default('effective_price'),
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(100).default(20),
});
export type ListQuery = z.infer<typeof listQuery>;

export const createProductBody = z.object({
  sku: z.string().trim().min(1).max(64),
  name: z.string().trim().min(1).max(255),
  categoryId: z.number().int().positive(),
  basePrice: moneyString('basePrice'),
  stock: z.number().int().min(0).default(0),
});
export type CreateProductBody = z.infer<typeof createProductBody>;

export const stockBody = z.union([
  z.object({ delta: z.number().int() }),
  z.object({ stock: z.number().int().min(0) }),
]);
export type StockBody = z.infer<typeof stockBody>;
