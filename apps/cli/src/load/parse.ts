import { UsageError } from '../errors';

const UNIT_MS: Record<string, number> = { ms: 1, s: 1_000, m: 60_000, h: 3_600_000 };

/** "500ms", "30s", "2m", "1.5h" -> milliseconds. */
export function parseDuration(input: string): number {
  const m = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/.exec(input.trim());
  if (!m) throw new UsageError(`invalid duration '${input}' (examples: 500ms, 30s, 2m, 1h)`);
  return Math.round(Number(m[1]) * UNIT_MS[m[2]!]!);
}

/** "2000/s", "120000/m" or a bare "2000" -> requests per second. */
export function parseRate(input: string): number {
  const m = /^(\d+(?:\.\d+)?)(?:\/(s|m))?$/.exec(input.trim());
  const value = m ? Number(m[1]) : Number.NaN;
  if (!m || !(value > 0)) throw new UsageError(`invalid rate '${input}' (examples: 2000/s, 120000/m)`);
  return m[2] === 'm' ? value / 60 : value;
}

/** "list=70,detail=30" -> { list: 70, detail: 30 }. Weights are relative; labels must be in `allowed`. */
export function parseMix(input: string, allowed: readonly string[]): Record<string, number> {
  const mix: Record<string, number> = {};
  for (const part of input.split(',')) {
    const m = /^([a-z][a-z:-]*)=(\d+)$/.exec(part.trim());
    if (!m) throw new UsageError(`invalid mix entry '${part}' (expected label=weight, e.g. list=70)`);
    if (!allowed.includes(m[1]!)) throw new UsageError(`unknown mix label '${m[1]}' (allowed: ${allowed.join(', ')})`);
    mix[m[1]!] = Number(m[2]);
  }
  if (Object.values(mix).every((w) => w === 0)) throw new UsageError('mix weights must not all be zero');
  return mix;
}

/** "0", "0.02", "1" -> a fraction in [0, 1], e.g. for --max-error-rate. */
export function parseErrorRate(input: string): number {
  const trimmed = input.trim();
  // Number('') is 0, and Number(' ') is also 0: reject anything that isn't itself a number literal.
  const n = trimmed === '' ? Number.NaN : Number(trimmed);
  if (!Number.isFinite(n) || n < 0 || n > 1) throw new UsageError(`invalid --max-error-rate '${input}' (a fraction between 0 and 1, e.g. 0.02 for 2%)`);
  return n;
}

export function parseIntStrict(input: string, name: string, min = 1): number {
  const n = /^-?\d+$/.test(input.trim()) ? Number(input) : Number.NaN;
  if (!Number.isSafeInteger(n) || n < min) throw new UsageError(`${name} must be an integer >= ${min}, got '${input}'`);
  return n;
}
