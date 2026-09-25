import { describe, expect, it } from 'vitest';
import { slugify } from './slug';

describe('slugify', () => {
  it('strips diacritics, lowercases and hyphenates', () => {
    expect(slugify('Café Crème')).toBe('cafe-creme');
    expect(slugify('  Shoes & Boots ')).toBe('shoes-boots');
  });
  it('falls back to a fixed slug when nothing usable is left', () => {
    expect(slugify('日本')).toBe('category');
  });
});
