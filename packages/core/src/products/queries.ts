import { sql, type SQL } from 'drizzle-orm';
import type { DbOrTx } from '../db/client';

export type SortDir = 'asc' | 'desc';

export interface ProductRecord {
  id: number;
  sku: string;
  name: string;
  category: { id: number; name: string; slug: string };
  basePrice: string;
  effectivePrice: string;
  activePromotion: { id: string; name: string; discountType: 'percentage' | 'fixed'; value: string } | null;
}

/** Single source of truth for the effective price. `promo` is the lateral alias below. */
export const effectivePriceExpr: SQL = sql`
  case
    when promo.id is null then p.base_price
    when promo.discount_type = 'percentage' then round(p.base_price * (1 - promo.value / 100), 2)
    else greatest(p.base_price - promo.value, 0)::numeric(12,2)
  end`;

function baseSelect(now: Date): SQL {
  const ts = now.toISOString();
  return sql`
    select p.id, p.sku, p.name, p.base_price,
           c.id as category_id, c.name as category_name, c.slug as category_slug,
           promo.id as promo_id, promo.name as promo_name, promo.discount_type as promo_discount_type, promo.value as promo_value,
           ${effectivePriceExpr} as effective_price
    from products p
    join categories c on c.id = p.category_id
    left join lateral (
      select pr.id, pr.name, pr.discount_type, pr.value
      from promotions pr
      where pr.cancelled_at is null
        and pr.starts_at <= ${ts}::timestamptz and ${ts}::timestamptz < pr.ends_at
        and ((pr.scope = 'product' and pr.product_id = p.id)
          or (pr.scope = 'category' and pr.category_id = p.category_id))
      order by pr.created_at desc, pr.id desc
      limit 1
    ) promo on true`;
}

function toRecord(r: Record<string, unknown>): ProductRecord {
  return {
    id: Number(r.id),
    sku: String(r.sku),
    name: String(r.name),
    category: { id: Number(r.category_id), name: String(r.category_name), slug: String(r.category_slug) },
    basePrice: String(r.base_price),
    effectivePrice: String(r.effective_price),
    activePromotion: r.promo_id
      ? { id: String(r.promo_id), name: String(r.promo_name), discountType: r.promo_discount_type as 'percentage' | 'fixed', value: String(r.promo_value) }
      : null,
  };
}

export async function fetchProductById(db: DbOrTx, id: number, now: Date): Promise<ProductRecord | null> {
  const res = await db.execute(sql`${baseSelect(now)} where p.id = ${id}`);
  const row = res.rows[0];
  return row ? toRecord(row) : null;
}

export async function fetchProductPage(
  db: DbOrTx,
  opts: { categoryId: number | null; sort: SortDir; limit: number; offset: number; now: Date },
): Promise<{ items: ProductRecord[]; total: number }> {
  const where = opts.categoryId === null ? sql`` : sql`where p.category_id = ${opts.categoryId}`;
  const dir = sql.raw(opts.sort === 'desc' ? 'desc' : 'asc');
  const [pageRes, countRes] = await Promise.all([
    db.execute(sql`${baseSelect(opts.now)} ${where} order by effective_price ${dir}, p.id ${dir} limit ${opts.limit} offset ${opts.offset}`),
    db.execute(sql`select count(*)::int as total from products p ${where}`),
  ]);
  return { items: pageRes.rows.map(toRecord), total: Number(countRes.rows[0]?.total ?? 0) };
}

export async function fetchStock(db: DbOrTx, ids: number[]): Promise<Map<number, number>> {
  const out = new Map<number, number>();
  if (ids.length === 0) return out;
  const idList = sql.join(ids.map((id) => sql`${id}`), sql`, `);
  const res = await db.execute(sql`select id, stock from products where id = any(array[${idList}]::int[])`);
  for (const r of res.rows) out.set(Number(r.id), Number(r.stock));
  return out;
}

/**
 * Earliest future moment at which the effective price of anything in `scope` can change:
 * the next start of a scheduled promotion or the next end of an active one.
 */
export async function nextPromotionBoundary(
  db: DbOrTx,
  scope: { productId: number; categoryId: number } | { categoryId: number | null },
  now: Date,
): Promise<Date | null> {
  const ts = now.toISOString();
  let target: SQL;
  if ('productId' in scope) {
    target = sql`(pr.product_id = ${scope.productId} or pr.category_id = ${scope.categoryId})`;
  } else if (scope.categoryId === null) {
    target = sql`true`;
  } else {
    target = sql`(pr.category_id = ${scope.categoryId}
      or exists (select 1 from products p where p.id = pr.product_id and p.category_id = ${scope.categoryId}))`;
  }
  const res = await db.execute(sql`
    select min(b) as boundary from (
      select pr.starts_at as b from promotions pr
        where pr.cancelled_at is null and pr.starts_at > ${ts}::timestamptz and ${target}
      union all
      select pr.ends_at as b from promotions pr
        where pr.cancelled_at is null and pr.starts_at <= ${ts}::timestamptz and pr.ends_at > ${ts}::timestamptz and ${target}
    ) t`);
  const b = res.rows[0]?.boundary;
  return b ? new Date(b as string | Date) : null;
}
