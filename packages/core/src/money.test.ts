import { describe, expect, it } from 'vitest';
import { clampCents, fromCents, roundUpTo99, toCents } from './money';

describe('toCents', () => {
  it('parses decimal strings exactly', () => {
    expect(toCents('12.34')).toBe(1234);
    expect(toCents('12')).toBe(1200);
    expect(toCents('0.5')).toBe(50);
    expect(toCents(19.99)).toBe(1999);
  });
  it('rejects invalid input', () => {
    expect(() => toCents('abc')).toThrow();
    expect(() => toCents('1.234')).toThrow();
    expect(() => toCents('-1')).toThrow();
    expect(() => toCents('12345678901')).toThrow(); // 11 integer digits cannot fit numeric(12,2)
  });
  it('accepts the numeric(12,2) maximum', () => {
    expect(toCents('9999999999.99')).toBe(999999999999);
  });
});

describe('fromCents', () => {
  it('formats with two decimals', () => {
    expect(fromCents(1234)).toBe('12.34');
    expect(fromCents(5)).toBe('0.05');
    expect(fromCents(0)).toBe('0.00');
  });
});

describe('roundUpTo99', () => {
  it('rounds up to the next .99 price point', () => {
    expect(roundUpTo99(1234)).toBe(1299);
    expect(roundUpTo99(1299)).toBe(1299);
    expect(roundUpTo99(1300)).toBe(1399);
    expect(roundUpTo99(1)).toBe(99);
    expect(roundUpTo99(0)).toBe(99);
  });
});

describe('clampCents', () => {
  it('clamps into the inclusive range', () => {
    expect(clampCents(50, 99, 1000)).toBe(99);
    expect(clampCents(5000, 99, 1000)).toBe(1000);
    expect(clampCents(500, 99, 1000)).toBe(500);
  });
});
