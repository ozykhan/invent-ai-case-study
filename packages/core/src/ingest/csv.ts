export const VENDOR_COLUMNS = ['sku', 'name', 'category', 'vendor_price', 'stock'] as const;

/** Minimal RFC 4180 line parser: commas, double quotes, doubled quotes. No embedded newlines. */
export function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let field = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') { field += '"'; i++; } else { inQuotes = false; }
      } else {
        field += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === ',') {
      out.push(field); field = '';
    } else {
      field += ch;
    }
  }
  out.push(field);
  return out;
}

export function rowFromFields(fields: string[]): Record<string, string> | null {
  if (fields.length !== VENDOR_COLUMNS.length) return null;
  const row: Record<string, string> = {};
  VENDOR_COLUMNS.forEach((col, i) => { row[col] = fields[i]!; });
  return row;
}
