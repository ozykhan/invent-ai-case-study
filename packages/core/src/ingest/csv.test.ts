import { describe, expect, it } from 'vitest';
import { parseCsvLine, rowFromFields, VENDOR_COLUMNS } from './csv';

describe('parseCsvLine', () => {
  it('splits plain fields', () => {
    expect(parseCsvLine('a,b,c')).toEqual(['a', 'b', 'c']);
  });
  it('handles quoted fields with commas and escaped quotes', () => {
    expect(parseCsvLine('SKU-1,"Belt, leather",Accessories,"10.00",5')).toEqual(['SKU-1', 'Belt, leather', 'Accessories', '10.00', '5']);
    expect(parseCsvLine('a,"say ""hi""",c')).toEqual(['a', 'say "hi"', 'c']);
  });
  it('keeps empty fields', () => {
    expect(parseCsvLine('a,,c')).toEqual(['a', '', 'c']);
  });
});

describe('rowFromFields', () => {
  it('maps fields to the vendor columns', () => {
    expect(rowFromFields(['s', 'n', 'c', '1.00', '2'])).toEqual({ sku: 's', name: 'n', category: 'c', vendor_price: '1.00', stock: '2' });
    expect(VENDOR_COLUMNS).toHaveLength(5);
  });
  it('returns null on a wrong field count', () => {
    expect(rowFromFields(['a', 'b'])).toBeNull();
    expect(rowFromFields(['a', 'b', 'c', 'd', 'e', 'f'])).toBeNull();
  });
});
