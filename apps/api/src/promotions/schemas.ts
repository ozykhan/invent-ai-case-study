import { z } from 'zod';

export const promotionIdParam = z.object({ id: z.string().uuid() });

export const targetSchema = z.union([
  z.object({ productId: z.number().int().positive() }).strict(),
  z.object({ categoryId: z.number().int().positive() }).strict(),
]);
export type Target = z.infer<typeof targetSchema>;

export const createPromotionBody = z.object({
  name: z.string().trim().min(1).max(255),
  discountType: z.enum(['percentage', 'fixed']),
  value: z.string().regex(/^\d+(\.\d{1,2})?$/, 'value must be a decimal with up to 2 places'),
  startsAt: z.string().datetime({ offset: true }),
  endsAt: z.string().datetime({ offset: true }),
  target: targetSchema,
});
export type CreatePromotionBody = z.infer<typeof createPromotionBody>;
