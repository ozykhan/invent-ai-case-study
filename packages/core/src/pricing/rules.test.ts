import { describe, expect, it } from 'vitest';
import { DEFAULT_CATEGORY_PRICING, priceVendorRow, type CategoryPricing } from './rules';

const defaults = () => DEFAULT_CATEGORY_PRICING;
const row = (over: Partial<Record<string, string>> = {}) => ({
  sku: 'SKU-1', name: 'Belt', category: 'Accessories', vendor_price: '10.00', stock: '5', ...over,
});

describe('priceVendorRow', () => {
  it('applies margin, rounds up to .99, and keeps stock', () => {
    const out = priceVendorRow(row(), defaults);
    expect(out).toEqual({ ok: true, row: { sku: 'SKU-1', name: 'Belt', category: 'Accessories', basePriceCents: 1399, stock: 5 } });
  });

  it('uses the category margin', () => {
    const pricing: CategoryPricing = { marginPct: 100, priceFloorCents: 99, priceCeilingCents: 9999999 };
    const out = priceVendorRow(row(), () => pricing);
    expect(out.ok && out.row.basePriceCents).toBe(2099);
  });

  it('clamps to the category floor and ceiling after rounding', () => {
    const floor: CategoryPricing = { marginPct: 0, priceFloorCents: 1999, priceCeilingCents: 9999999 };
    expect(priceVendorRow(row({ vendor_price: '1.00' }), () => floor)).toMatchObject({ ok: true, row: { basePriceCents: 1999 } });
    const ceiling: CategoryPricing = { marginPct: 0, priceFloorCents: 99, priceCeilingCents: 500 };
    expect(priceVendorRow(row({ vendor_price: '100.00' }), () => ceiling)).toMatchObject({ ok: true, row: { basePriceCents: 500 } });
  });

  it('rejects invalid rows with a reason', () => {
    expect(priceVendorRow(row({ sku: '' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('sku') });
    expect(priceVendorRow(row({ vendor_price: '0' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('vendor_price') });
    expect(priceVendorRow(row({ vendor_price: '-3' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('vendor_price') });
    expect(priceVendorRow(row({ vendor_price: 'abc' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('vendor_price') });
    expect(priceVendorRow(row({ stock: '1.5' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('stock') });
    expect(priceVendorRow(row({ stock: '-1' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('stock') });
    expect(priceVendorRow(row({ category: '   ' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('category') });
  });

  it('bounds stock to the Postgres integer range', () => {
    expect(priceVendorRow(row({ stock: '2147483647' }), defaults)).toMatchObject({ ok: true, row: { stock: 2147483647 } });
    expect(priceVendorRow(row({ stock: '2147483648' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('stock') });
    expect(priceVendorRow(row({ stock: '3000000000' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('stock') });
    expect(priceVendorRow(row({ stock: '00000000001' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('stock') });
  });

  it('bounds vendor_price to numeric(12,2): at most 10 integer digits', () => {
    expect(priceVendorRow(row({ vendor_price: '9999999999.99' }), defaults)).toMatchObject({ ok: true });
    expect(priceVendorRow(row({ vendor_price: '12345678901.00' }), defaults)).toMatchObject({ ok: false, reason: expect.stringContaining('vendor_price') });
  });

  it('rejects any field containing a NUL character, which Postgres text cannot store', () => {
    for (const field of ['sku', 'name', 'category', 'vendor_price', 'stock']) {
      const bad = row({ [field]: `${row()[field as keyof ReturnType<typeof row>]}\u0000` });
      expect(priceVendorRow(bad, defaults)).toMatchObject({ ok: false, reason: expect.stringMatching(new RegExp(`${field}.*NUL`)) });
    }
  });

  it('trims whitespace on text fields', () => {
    const out = priceVendorRow(row({ sku: '  SKU-9 ', name: ' Hat ' }), defaults);
    expect(out.ok && out.row).toMatchObject({ sku: 'SKU-9', name: 'Hat' });
  });
});
