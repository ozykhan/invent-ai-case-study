import { and, eq, isNull } from 'drizzle-orm';
import { bumpCategory, bumpVersions, categories, keys, products, promotions, type DbOrTx, type Redis } from '@modaco/core';
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

  /**
   * Validates the target exists and, for a product target, returns its category id — so callers
   * that already need to touch the target row (create, assign) can pass that id straight to
   * bump() instead of re-querying it after the write commits.
   */
  private async resolveTarget(db: DbOrTx, target: Target): Promise<number | undefined> {
    if ('productId' in target) {
      const [p] = await db.select({ categoryId: products.categoryId }).from(products).where(eq(products.id, target.productId));
      if (!p) throw notFound(`product ${target.productId} not found`);
      return p.categoryId;
    }
    const [c] = await db.select({ id: categories.id }).from(categories).where(eq(categories.id, target.categoryId));
    if (!c) throw notFound(`category ${target.categoryId} not found`);
    return undefined;
  }

  /** productCategoryId must be supplied by the caller for a product target; bump() never queries. */
  private async bump(target: Target, productCategoryId?: number): Promise<void> {
    const redis: Redis | null = this.deps.redis;
    if (!redis) return;
    const log = (msg: string, err: unknown) => this.deps.logger.error({ err }, msg);
    if ('productId' in target) {
      // A product-scoped promo changes that product's price and its position in category and
      // all-products lists, so bump the product's category (and ver:all) as well.
      await bumpVersions(redis, [
        keys.productVersion(target.productId),
        ...(productCategoryId !== undefined ? [keys.categoryVersion(productCategoryId)] : []),
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
    // Resolves the target and, for a product, its category id in one query, before the insert —
    // so the write commits are followed only by the bump, never by another read.
    const productCategoryId = await this.resolveTarget(this.deps.db, body.target);
    const [row] = await this.deps.db.insert(promotions).values({
      name: body.name, discountType: body.discountType, value: body.value, startsAt, endsAt,
      scope: 'productId' in body.target ? 'product' : 'category',
      productId: 'productId' in body.target ? body.target.productId : null,
      categoryId: 'categoryId' in body.target ? body.target.categoryId : null,
    }).returning();
    await this.bump(body.target, productCategoryId);
    return toView(row!);
  }

  async get(id: string): Promise<PromotionView | null> {
    const [row] = await this.deps.db.select().from(promotions).where(eq(promotions.id, id));
    return row ? toView(row) : null;
  }

  async cancel(id: string): Promise<PromotionView | null> {
    const [current] = await this.deps.db.select().from(promotions).where(eq(promotions.id, id));
    if (!current) return null;
    if (current.cancelledAt) return toView(current);
    // The bump's category lookup runs BEFORE the cancelling UPDATE: if it fails, nothing has been written
    // and a retry goes through the whole path again. Run after the commit, a failure would leave the
    // promotion cancelled with its bump lost for good, because every retry takes the no-op path below.
    const target = toView(current).target;
    const productCategoryId = 'productId' in target
      ? (await this.deps.db.select({ categoryId: products.categoryId }).from(products).where(eq(products.id, target.productId)))[0]?.categoryId
      : undefined;
    // A single conditional UPDATE, guarded by `cancelled_at is null`, makes this compare-and-set:
    // of two concurrent cancels, Postgres row-level locking lets exactly one UPDATE actually flip
    // cancelled_at and return a row: the other's WHERE clause no longer matches, so it returns none.
    const [row] = await this.deps.db.update(promotions)
      .set({ cancelledAt: this.deps.now() })
      .where(and(eq(promotions.id, id), isNull(promotions.cancelledAt)))
      .returning();
    if (row) {
      // Only the winner of the race reaches here, so the bump runs at most once per actual cancellation.
      await this.bump(target, productCategoryId);
      return toView(row);
    }
    const [existing] = await this.deps.db.select().from(promotions).where(eq(promotions.id, id));
    return existing ? toView(existing) : null;
  }

  async assign(id: string, target: Target): Promise<PromotionView | null> {
    const result = await this.deps.db.transaction(async (tx) => {
      // Lock the row so a concurrent assign can't read the same stale "old" target: with
      // concurrent moves X->Y and X->Z, whichever transaction commits second must see the first
      // transaction's write as its own "old" target, not the original X, or Y would never be
      // invalidated. FOR UPDATE plus the transaction serializes the two around that read.
      const [existing] = await tx.select().from(promotions).where(eq(promotions.id, id)).for('update');
      if (!existing) return null;
      const oldTarget = toView(existing).target;
      const oldCategoryId = 'productId' in oldTarget
        ? (await tx.select({ categoryId: products.categoryId }).from(products).where(eq(products.id, oldTarget.productId)))[0]?.categoryId
        : undefined;
      const newCategoryId = await this.resolveTarget(tx, target);
      const [row] = await tx.update(promotions).set({
        scope: 'productId' in target ? 'product' : 'category',
        productId: 'productId' in target ? target.productId : null,
        categoryId: 'categoryId' in target ? target.categoryId : null,
      }).where(eq(promotions.id, id)).returning();
      return { row: row!, oldTarget, oldCategoryId, newCategoryId };
    });
    if (!result) return null;
    // Bumps happen after the transaction has committed, never inside it.
    await this.bump(result.oldTarget, result.oldCategoryId);
    await this.bump(target, result.newCategoryId);
    return toView(result.row);
  }
}
