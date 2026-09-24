import { z } from 'zod';
import { clampCents, roundUpTo99, toCents } from '../money';

export interface CategoryPricing {
  marginPct: number;
  priceFloorCents: number;
  priceCeilingCents: number;
}

export const DEFAULT_CATEGORY_PRICING: CategoryPricing = { marginPct: 30, priceFloorCents: 99, priceCeilingCents: 9999999 };

export function categoryPricingFromRow(row: { marginPct: string; priceFloor: string; priceCeiling: string }): CategoryPricing {
  return {
    marginPct: Number(row.marginPct),
    priceFloorCents: toCents(row.priceFloor),
    priceCeilingCents: toCents(row.priceCeiling),
  };
}

export const rawVendorRowSchema = z.object({
  sku: z.string().trim().min(1, 'sku is required').max(64),
  name: z.string().trim().min(1, 'name is required').max(255),
  category: z.string().trim().min(1, 'category is required').max(100),
  vendor_price: z.string().trim().regex(/^\d+(\.\d{1,2})?$/, 'vendor_price must be a decimal with up to 2 places'),
  stock: z.string().trim().regex(/^\d+$/, 'stock must be a non-negative integer'),
});

export interface PricedRow {
  sku: string;
  name: string;
  category: string;
  basePriceCents: number;
  stock: number;
}

export type PricingOutcome = { ok: true; row: PricedRow } | { ok: false; reason: string };

/**
 * The application-layer pricing pipeline every vendor row passes through:
 * 1. validate  2. margin  3. round up to .99  4. clamp to category floor/ceiling
 */
export function priceVendorRow(
  input: Record<string, string>,
  pricingFor: (category: string) => CategoryPricing,
): PricingOutcome {
  const parsed = rawVendorRowSchema.safeParse(input);
  if (!parsed.success) {
    const issue = parsed.error.issues[0]!;
    return { ok: false, reason: `validation: ${issue.path.join('.')}: ${issue.message}` };
  }
  const raw = parsed.data;
  const vendorCents = toCents(raw.vendor_price);
  if (vendorCents <= 0) return { ok: false, reason: 'validation: vendor_price must be greater than 0' };

  const pricing = pricingFor(raw.category);
  const withMargin = Math.round(vendorCents * (1 + pricing.marginPct / 100));
  const pricePoint = roundUpTo99(withMargin);
  const basePriceCents = clampCents(pricePoint, pricing.priceFloorCents, pricing.priceCeilingCents);

  return {
    ok: true,
    row: { sku: raw.sku, name: raw.name, category: raw.category, basePriceCents, stock: Number(raw.stock) },
  };
}
