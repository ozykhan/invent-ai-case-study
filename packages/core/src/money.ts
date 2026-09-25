/**
 * A non-negative money amount as text: up to 2 decimal places and at most 10 integer digits, so every
 * accepted value fits a `numeric(12,2)` column (12 digits, 2 of them the fraction). An unbounded integer
 * part would let a huge value reach Postgres and fail there with 22003 (numeric field overflow).
 */
export const MONEY_RE = /^\d{1,10}(\.\d{1,2})?$/;

/** Parse a non-negative decimal (string or number) into integer cents. Throws on invalid input. */
export function toCents(value: string | number): number {
  const s = typeof value === 'number' ? value.toFixed(2) : value.trim();
  if (!MONEY_RE.test(s)) throw new Error(`invalid money value: ${String(value)}`);
  const [whole, frac = ''] = s.split('.');
  return Number(whole) * 100 + Number(frac.padEnd(2, '0'));
}

export function fromCents(cents: number): string {
  if (!Number.isInteger(cents) || cents < 0) throw new Error(`invalid cents: ${cents}`);
  const whole = Math.floor(cents / 100);
  const frac = cents % 100;
  return `${whole}.${frac.toString().padStart(2, '0')}`;
}

/** Smallest x.99 price point that is >= cents. */
export function roundUpTo99(cents: number): number {
  const candidate = Math.floor(cents / 100) * 100 + 99;
  return candidate >= cents ? candidate : candidate + 100;
}

export function clampCents(cents: number, floor: number, ceiling: number): number {
  return Math.min(Math.max(cents, floor), ceiling);
}
