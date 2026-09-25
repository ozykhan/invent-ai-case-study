/**
 * The discounted price of `basePrice` under a promotion, in the same integer-cents, round-half-up arithmetic
 * Postgres's `round(numeric, 2)` uses for `effectivePrice` (packages/core/src/products/queries.ts's
 * `effectivePriceExpr`: `round(base_price * (1 - value / 100), 2)` for a percentage discount, else
 * `greatest(base_price - value, 0)`).
 *
 * A float computation (`base * (1 - value / 100)`, then `Math.round(... * 100) / 100`) disagrees with Postgres for
 * about 2.3% of base prices at 50% off -- e.g. base 2.01 rounds to 1.00 in floating point but 1.01 in Postgres;
 * base 0.29 rounds to 0.14 instead of 0.15. Both the load engine's own checks and its test stub share this instead
 * of each risking their own float rounding.
 */
export function discountedPrice(basePrice: string, discountType: 'percentage' | 'fixed', value: string): string {
  const baseCents = toCents(basePrice);
  const cents = discountType === 'percentage' ? percentageOffCents(baseCents, value) : Math.max(0, baseCents - toCents(value));
  return (cents / 100).toFixed(2);
}

/** A decimal money string, rounded to the nearest cent (half up) -- inputs here are already at most 2dp. */
function toCents(value: string): number {
  return Math.round(Number(value) * 100);
}

/** `floor((baseCents * (10000 - valuePercent*100) + 5000) / 10000)`: baseCents * (1 - value/100), rounded half up. */
function percentageOffCents(baseCents: number, percentOff: string): number {
  const valueBasisPoints = toCents(percentOff); // e.g. "50" -> 5000, "12.5" -> 1250
  return Math.floor((baseCents * (10_000 - valueBasisPoints) + 5000) / 10_000);
}
