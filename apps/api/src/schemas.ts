import { z } from 'zod';

/**
 * A monetary amount as sent/received in JSON: a decimal string with up to 2 places, capped at
 * 10 integer digits so it always fits a `numeric(12,2)` column (12 total digits, 2 reserved for
 * the fraction) — an unbounded integer part would otherwise let a huge value hit Postgres error
 * 22003 (numeric field overflow) and surface as a 500 instead of a validation error.
 */
export const moneyString = (label: string) =>
  z.string().regex(/^\d{1,10}(\.\d{1,2})?$/, `${label} must be a decimal with up to 2 places and at most 10 integer digits`);
