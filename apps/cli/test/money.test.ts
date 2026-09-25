import { describe, expect, it } from 'vitest';
import { discountedPrice } from '../src/money';

describe('discountedPrice', () => {
  it('matches Postgres round(base * (1 - value / 100), 2) at the cases where a float computation disagrees', () => {
    // Float: 2.01 * 0.5 = 1.0049999999999999 -> rounds to 1.00. Postgres (exact decimal): 1.005 -> rounds to 1.01.
    expect(discountedPrice('2.01', 'percentage', '50')).toBe('1.01');
    // Float: 0.29 * 0.5 = 0.145 as a double is actually 0.14499999999999999 -> rounds to 0.14. Postgres: 0.145 -> 0.15.
    expect(discountedPrice('0.29', 'percentage', '50')).toBe('0.15');
  });

  it('rounds an exact half cent up, like Postgres numeric rounding', () => {
    expect(discountedPrice('20.00', 'percentage', '50')).toBe('10.00');
    expect(discountedPrice('19.99', 'percentage', '10')).toBe('17.99'); // 17.991 -> 17.99
    expect(discountedPrice('19.95', 'percentage', '10')).toBe('17.96'); // 17.955 -> 17.96
  });

  it('applies a fixed discount as a plain subtraction, clamped at zero', () => {
    expect(discountedPrice('20.00', 'fixed', '5.00')).toBe('15.00');
    expect(discountedPrice('3.00', 'fixed', '5.00')).toBe('0.00');
  });
});
