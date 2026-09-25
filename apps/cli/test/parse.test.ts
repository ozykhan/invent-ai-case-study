import { describe, expect, it } from 'vitest';
import { UsageError } from '../src/errors';
import { parseDuration, parseErrorRate, parseIntStrict, parseMix, parseRate } from '../src/load/parse';

describe('parseDuration', () => {
  it('converts units to milliseconds', () => {
    expect(parseDuration('500ms')).toBe(500);
    expect(parseDuration('30s')).toBe(30_000);
    expect(parseDuration('2m')).toBe(120_000);
    expect(parseDuration('1.5h')).toBe(5_400_000);
    expect(parseDuration('0s')).toBe(0);
  });
  it('rejects malformed input', () => {
    for (const bad of ['30', 's', '-1s', '1d', '']) expect(() => parseDuration(bad)).toThrow(UsageError);
  });
});

describe('parseRate', () => {
  it('returns requests per second', () => {
    expect(parseRate('2000/s')).toBe(2000);
    expect(parseRate('2000')).toBe(2000);
    expect(parseRate('120000/m')).toBe(2000);
  });
  it('rejects zero and malformed input', () => {
    for (const bad of ['0/s', '/s', 'fast', '10/h']) expect(() => parseRate(bad)).toThrow(UsageError);
  });
});

describe('parseMix', () => {
  const allowed = ['list', 'detail', 'promo'];
  it('parses label=weight pairs', () => {
    expect(parseMix('list=70, detail=30', allowed)).toEqual({ list: 70, detail: 30 });
    expect(parseMix('promo=1', allowed)).toEqual({ promo: 1 });
  });
  it('rejects unknown labels, bad syntax and all-zero weights', () => {
    expect(() => parseMix('search=5', allowed)).toThrow(/unknown mix label 'search'/);
    expect(() => parseMix('list:70', allowed)).toThrow(UsageError);
    expect(() => parseMix('list=0,detail=0', allowed)).toThrow(/must not all be zero/);
  });
});

describe('parseErrorRate', () => {
  it('accepts a fraction between 0 and 1', () => {
    expect(parseErrorRate('0')).toBe(0);
    expect(parseErrorRate('0.02')).toBe(0.02);
    expect(parseErrorRate('1')).toBe(1);
  });
  it('rejects out-of-range and malformed input', () => {
    for (const bad of ['-0.1', '1.1', 'fast', '']) expect(() => parseErrorRate(bad)).toThrow(UsageError);
  });
});

describe('parseIntStrict', () => {
  it('accepts integers at or above the minimum', () => {
    expect(parseIntStrict('42', 'page')).toBe(42);
    expect(parseIntStrict('-3', 'delta', -10)).toBe(-3);
    expect(parseIntStrict('0', 'stock', 0)).toBe(0);
  });
  it('rejects non-integers and values below the minimum', () => {
    expect(() => parseIntStrict('1.5', 'page')).toThrow(UsageError);
    expect(() => parseIntStrict('abc', 'page')).toThrow(/page must be an integer >= 1/);
    expect(() => parseIntStrict('0', 'page')).toThrow(UsageError);
  });
});
