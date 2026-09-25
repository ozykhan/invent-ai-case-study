import { MONEY_RE, PG_INT4_MAX } from '@modaco/core';
import { z } from 'zod';

/**
 * A monetary amount as sent/received in JSON: a decimal string with up to 2 places, capped at
 * 10 integer digits so it always fits a `numeric(12,2)` column (12 total digits, 2 reserved for
 * the fraction) — an unbounded integer part would otherwise let a huge value hit Postgres error
 * 22003 (numeric field overflow) and surface as a 500 instead of a validation error.
 */
export const moneyString = (label: string) =>
  z.string().regex(MONEY_RE, `${label} must be a decimal with up to 2 places and at most 10 integer digits`);

/**
 * An integer that fits a Postgres `integer` (int4) column. Like the money cap, it turns an out-of-range
 * id or count into a 400 instead of a Postgres 22003 (value out of range) surfacing as a 500. Pass
 * `z.coerce.number()` for path and query parameters.
 */
export const int4 = (base: z.ZodNumber = z.number()) => base.int().min(-PG_INT4_MAX - 1).max(PG_INT4_MAX);
