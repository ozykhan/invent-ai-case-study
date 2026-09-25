import { eq } from 'drizzle-orm';
import { bumpCategory, bumpVersions, categories, keys, products, promotions, type Redis } from '@modaco/core';
import type { AppDeps } from '../deps';
import { notFound, unprocessable } from '../errors';
import type { CreatePromotionBody, Target } from './schemas';

export interface PromotionView {
  id: string; name: string; discountType: 'percentage' | 'fixed'; value: string;
  startsAt: string; endsAt: string; target: Target; cancelledAt: string | null; createdAt: string;
}

type Row = typeof promotions.$inferSelect;

function toView(r: Row): PromotionView {
  return {
    id: r.id, name: r.name, discountType: r.discountType, value: r.value,
    startsAt: r.startsAt.toISOString(), endsAt: r.endsAt.toISOString(),
    target: r.scope === 'product' ? { productId: r.productId! } : { categoryId: r.categoryId! },
    cancelledAt: r.cancelledAt ? r.cancelledAt.toISOString() : null,
    createdAt: r.createdAt.toISOString(),
  };
}

export class PromotionService {
  constructor(private readonly deps: AppDeps) {}

  private async assertTarget(target: Target): Promise<void> {
    if ('productId' in target) {
      const [p] = await this.deps.db.select({ id: products.id }).from(products).where(eq(products.id, target.productId));
      if (!p) throw notFound(`product ${target.productId} not found`);
    } else {
      const [c] = await this.deps.db.select({ id: categories.id }).from(categories).where(eq(categories.id, target.categoryId));
      if (!c) throw notFound(`category ${target.categoryId} not found`);
    }
  }

  private async bump(target: Target): Promise<void> {
    const redis: Redis | null = this.deps.redis;
    if (!redis) return;
    const log = (msg: string, err: unknown) => this.deps.logger.error({ err }, msg);
    if ('productId' in target) {
      // A product-scoped promo changes that product's price and its position in category and
      // all-products lists, so bump the product's category (and ver:all) as well.
      const [p] = await this.deps.db.select({ categoryId: products.categoryId }).from(products).where(eq(products.id, target.productId));
      await bumpVersions(redis, [
        keys.productVersion(target.productId),
        ...(p ? [keys.categoryVersion(p.categoryId)] : []),
        keys.allVersion(),
      ], log);
    } else {
      await bumpCategory(redis, target.categoryId, log);
    }
  }

  async create(body: CreatePromotionBody): Promise<PromotionView> {
    const startsAt = new Date(body.startsAt);
    const endsAt = new Date(body.endsAt);
    if (endsAt <= startsAt) throw unprocessable('endsAt must be after startsAt');
    if (body.discountType === 'percentage' && Number(body.value) > 100) throw unprocessable('percentage value cannot exceed 100');
    await this.assertTarget(body.target);
    const [row] = await this.deps.db.insert(promotions).values({
      name: body.name, discountType: body.discountType, value: body.value, startsAt, endsAt,
      scope: 'productId' in body.target ? 'product' : 'category',
      productId: 'productId' in body.target ? body.target.productId : null,
      categoryId: 'categoryId' in body.target ? body.target.categoryId : null,
    }).returning();
    await this.bump(body.target);
    return toView(row!);
  }

  async get(id: string): Promise<PromotionView | null> {
    const [row] = await this.deps.db.select().from(promotions).where(eq(promotions.id, id));
    return row ? toView(row) : null;
  }

  async cancel(id: string): Promise<PromotionView | null> {
    const [existing] = await this.deps.db.select().from(promotions).where(eq(promotions.id, id));
    if (!existing) return null;
    if (existing.cancelledAt) return toView(existing);
    const [row] = await this.deps.db.update(promotions).set({ cancelledAt: this.deps.now() }).where(eq(promotions.id, id)).returning();
    await this.bump(toView(row!).target);
    return toView(row!);
  }

  async assign(id: string, target: Target): Promise<PromotionView | null> {
    const [existing] = await this.deps.db.select().from(promotions).where(eq(promotions.id, id));
    if (!existing) return null;
    await this.assertTarget(target);
    const [row] = await this.deps.db.update(promotions).set({
      scope: 'productId' in target ? 'product' : 'category',
      productId: 'productId' in target ? target.productId : null,
      categoryId: 'categoryId' in target ? target.categoryId : null,
    }).where(eq(promotions.id, id)).returning();
    await this.bump(toView(existing).target);
    await this.bump(target);
    return toView(row!);
  }
}
