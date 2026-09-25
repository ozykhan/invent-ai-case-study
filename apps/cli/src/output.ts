/** Results go to stdout; progress, warnings and errors go to stderr, so `--json` output stays parseable. */
export function out(text: string): void {
  process.stdout.write(`${text}\n`);
}

export function log(text: string): void {
  process.stderr.write(`${text}\n`);
}

/** Prints `value` as JSON in --json mode, otherwise the human rendering. */
export function emit(json: boolean, value: unknown, human: () => string): void {
  out(json ? JSON.stringify(value, null, 2) : human());
}

/** Left-aligned columns separated by two spaces; the last column is not padded. */
export function formatTable(rows: ReadonlyArray<ReadonlyArray<string | number>>): string {
  const cells = rows.map((row) => row.map(String));
  const widths: number[] = [];
  for (const row of cells) row.forEach((cell, i) => { widths[i] = Math.max(widths[i] ?? 0, cell.length); });
  return cells.map((row) => row.map((cell, i) => (i === row.length - 1 ? cell : cell.padEnd(widths[i]!))).join('  ')).join('\n');
}

/** Key/value lines; null and undefined render as "-", objects as compact JSON. */
export function formatFields(fields: ReadonlyArray<readonly [string, unknown]>): string {
  return formatTable(fields.map(([key, value]) => [
    key,
    value === null || value === undefined ? '-' : typeof value === 'object' ? JSON.stringify(value) : String(value),
  ]));
}
