# `modaco` CLI and Load-Balanced Local Stack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `modaco` CLI that operates every ModaCo API endpoint and load tests it with open- and closed-model scenarios, plus a Docker Compose profile that runs N API replicas behind nginx.

**Architecture:** A new workspace package `apps/cli` with commander subcommands. An `ApiClient` wraps an undici `Agent` and serves both the operational commands (`call`, which throws `ApiError`) and the load engine (`send`, which never throws on status). The engine (`runPhase`) drives a `LoadSource` in an open model (latency measured from scheduled start) or a closed model, and records into HDR histograms (`Metrics`). Scenarios (`browse`, `write-mix`, `flash-sale`) plug into `runLoad`, which orchestrates warmup, recorded phases, checks and cleanup, and returns a JSON `ResultDocument`. The API gains one middleware that sets `X-Instance-Id`, so the CLI can report how the load balancer spreads requests.

**Tech Stack:** Node 22, TypeScript, tsx, commander 14, undici 7, hdr-histogram-js 3, Vitest 3, Express 5 (API side), nginx 1.27, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-09-25-modaco-cli-design.md`

## Global Constraints

- Node `>=22` (root `engines`). Use `commander@^14.0.3` and `undici@^7.30.0`, not commander 15 or undici 8, which require Node ≥22.12 / ≥22.19.
- Package manager: pnpm 9 workspaces. The new package is named `@modaco/cli` and lives in `apps/cli`.
- TypeScript extends `../../tsconfig.base.json` (strict, `noUncheckedIndexedAccess`, ESM, `moduleResolution: Bundler`). Relative imports have no file extension (for example `from './errors'`), as in the rest of the repo.
- Root script: `"modaco": "tsx apps/cli/src/main.ts"`. Relative paths resolve against the repo root.
- Global options: `--url` (default `API_URL` env, else `http://localhost:3000`), `--json`, `--timeout` (default `10s`).
- Exit codes: `0` success, `1` API/check/transport failure, `2` usage error, `130` interrupted load run.
- `--json` prints exactly one JSON document to stdout (except `health --watch`, which prints one JSON line per poll). Progress, warnings and errors in human mode go to stderr.
- Response header name: `x-instance-id`. Value: `INSTANCE_ID` env, else `os.hostname()`.
- Load defaults: `--duration 30s`, `--warmup 5s`, `--ramp 0s`, `--max-inflight 10000`, `--report-every 5s`, `--max-page 5`. `--connections` defaults to `max(concurrency, 64)` for closed and `256` for open.
- Histogram: microseconds, 1 µs to 60 s, 3 significant digits. Reported latencies are in ms, rounded to 2 decimals.
- Default mixes: browse `list=70,detail=30`; write-mix `list=60,detail=25,stock=14,promo=1`.
- The existing `scripts/demo-*.ts` files are not modified.
- Commit messages end with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

Two small spec deviations, which Task 10 writes back into the spec:
1. `ingest <file>` becomes `ingest upload <file>`. Commander cannot cleanly mix a positional file argument with `status`/`rejections` subcommands on the same command.
2. write-mix `stock` sends `{ "stock": <0..100> }` instead of `{ "delta": ±1 }`. A −1 delta on a product at stock 0 returns 422, which would show up as noise in the error stats.

## File Structure

```
apps/api/src/middleware/instance-id.ts   NEW  X-Instance-Id middleware
apps/api/src/config.ts                   MOD  instanceId from INSTANCE_ID or hostname
apps/api/src/app.ts                      MOD  register instanceId first
apps/api/test/instance-id.test.ts        NEW

apps/cli/package.json, tsconfig.json, vitest.config.ts   NEW
apps/cli/src/errors.ts                   UsageError, SetupError, ApiError
apps/cli/src/api-types.ts                response and input shapes of the API
apps/cli/src/client.ts                   ApiClient (undici Agent): call() for ops, send() for load
apps/cli/src/output.ts                   out/log/emit, formatTable, formatFields
apps/cli/src/main.ts                     commander program, error -> exit code mapping
apps/cli/src/commands/common.ts          globals(), withClient(), arg parsers for commander
apps/cli/src/commands/health.ts
apps/cli/src/commands/products.ts
apps/cli/src/commands/promotions.ts
apps/cli/src/commands/ingest.ts
apps/cli/src/commands/load.ts
apps/cli/src/load/parse.ts               durations, rates, mixes, ints
apps/cli/src/load/rng.ts                 seeded PRNG, weighted pick
apps/cli/src/load/metrics.ts             HDR histograms, status/error/instance counters
apps/cli/src/load/engine.ts              runPhase: open/closed models, scheduling, drain
apps/cli/src/load/report.ts              ResultDocument types, progress line, final report
apps/cli/src/load/run.ts                 runLoad: setup, warmup, phases, checks, cleanup
apps/cli/src/load/scenarios/types.ts     Scenario, ScenarioOptions, PhaseRunner
apps/cli/src/load/scenarios/catalog.ts   sampleCatalog, listRequest, detailRequest
apps/cli/src/load/scenarios/browse.ts
apps/cli/src/load/scenarios/write-mix.ts
apps/cli/src/load/scenarios/flash-sale.ts
apps/cli/src/load/scenarios/index.ts     SCENARIOS registry, createScenario
apps/cli/test/stub-api.ts                in-process fake ModaCo API (node:http)
apps/cli/test/*.test.ts

infra/nginx/nginx.conf                   NEW  upstream over api-lb replicas
docker-compose.yml                       MOD  x-api anchor, api-lb + nginx under profile lb
package.json                             MOD  "modaco" script
README.md                                MOD  CLI and load-balancer sections
docs/superpowers/specs/2026-09-25-modaco-cli-design.md   MOD  the two deviations above
```

---

### Task 1: API `X-Instance-Id` header

**Files:**
- Create: `apps/api/src/middleware/instance-id.ts`
- Modify: `apps/api/src/config.ts`
- Modify: `apps/api/src/app.ts`
- Test: `apps/api/test/instance-id.test.ts`

**Interfaces:**
- Produces: `instanceId(id: string): RequestHandler`. `Config.instanceId: string`. Every API response carries `x-instance-id`.

- [ ] **Step 1: Write the failing test**

`apps/api/test/instance-id.test.ts`:

```ts
import express from 'express';
import { hostname } from 'node:os';
import request from 'supertest';
import { describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config';
import { instanceId } from '../src/middleware/instance-id';

describe('instance id', () => {
  it('sets X-Instance-Id on every response, including 404s', async () => {
    const app = express().use(instanceId('replica-7'));
    app.get('/ping', (_req, res) => { res.json({ ok: true }); });
    const ok = await request(app).get('/ping');
    expect(ok.headers['x-instance-id']).toBe('replica-7');
    const missing = await request(app).get('/nope');
    expect(missing.status).toBe(404);
    expect(missing.headers['x-instance-id']).toBe('replica-7');
  });

  it('reads INSTANCE_ID and falls back to the hostname', () => {
    expect(loadConfig({ INSTANCE_ID: 'api-1' }).instanceId).toBe('api-1');
    expect(loadConfig({}).instanceId).toBe(hostname());
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --filter @modaco/api exec vitest run test/instance-id.test.ts`
Expected: FAIL, `Cannot find module '../src/middleware/instance-id'` (or `instanceId` undefined on config).

- [ ] **Step 3: Implement**

`apps/api/src/middleware/instance-id.ts`:

```ts
import type { RequestHandler } from 'express';

/** Tags every response with the serving instance, so a load test behind a load balancer can see how requests spread. */
export function instanceId(id: string): RequestHandler {
  return (_req, res, next) => {
    res.setHeader('x-instance-id', id);
    next();
  };
}
```

`apps/api/src/config.ts`: replace the whole file with:

```ts
import { hostname } from 'node:os';
import { z } from 'zod';

const schema = z.object({
  PORT: z.coerce.number().default(3000),
  DATABASE_URL: z.string().default('postgres://modaco:modaco@localhost:5433/modaco'),
  REDIS_URL: z.string().default('redis://localhost:6379'),
  AWS_REGION: z.string().default('us-east-1'),
  S3_PUBLIC_ENDPOINT: z.string().default('http://localhost:4566'),
  S3_BUCKET: z.string().default('modaco-vendor-uploads'),
  LOG_LEVEL: z.string().default('info'),
  INSTANCE_ID: z.string().min(1).optional(),
});

export interface Config {
  port: number;
  databaseUrl: string;
  redisUrl: string;
  awsRegion: string;
  s3PublicEndpoint: string;
  s3Bucket: string;
  logLevel: string;
  /** Sent as X-Instance-Id. In Docker the hostname is the container id, unique per replica. */
  instanceId: string;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const e = schema.parse(env);
  return {
    port: e.PORT, databaseUrl: e.DATABASE_URL, redisUrl: e.REDIS_URL, awsRegion: e.AWS_REGION,
    s3PublicEndpoint: e.S3_PUBLIC_ENDPOINT, s3Bucket: e.S3_BUCKET, logLevel: e.LOG_LEVEL,
    instanceId: e.INSTANCE_ID ?? hostname(),
  };
}
```

`apps/api/src/app.ts`: add the import and register the middleware first:

```ts
import { instanceId } from './middleware/instance-id';
```

```ts
  app.disable('x-powered-by');
  app.use(instanceId(deps.config.instanceId));
  app.use(requestId);
```

- [ ] **Step 4: Run the test and the typecheck**

Run: `pnpm --filter @modaco/api exec vitest run test/instance-id.test.ts && pnpm --filter @modaco/api typecheck`
Expected: 2 tests PASS, no type errors.

- [ ] **Step 5: Commit**

```bash
git add apps/api/src/middleware/instance-id.ts apps/api/src/config.ts apps/api/src/app.ts apps/api/test/instance-id.test.ts
git commit -m "feat(api): tag responses with X-Instance-Id

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: CLI package scaffold, parsing helpers, seeded RNG

**Files:**
- Create: `apps/cli/package.json`, `apps/cli/tsconfig.json`, `apps/cli/vitest.config.ts`
- Create: `apps/cli/src/errors.ts`, `apps/cli/src/load/parse.ts`, `apps/cli/src/load/rng.ts`
- Modify: `package.json` (root): add the `modaco` script
- Test: `apps/cli/test/parse.test.ts`, `apps/cli/test/rng.test.ts`

**Interfaces:**
- Produces:
  - `class UsageError extends Error`, `class SetupError extends Error`, `class ApiError extends Error { status: number; code: string; details?: unknown; toJSON(): { status: number; error: { code: string; message: string; details?: unknown } } }`
  - `parseDuration(input: string): number` (ms), `parseRate(input: string): number` (req/s), `parseMix(input: string, allowed: readonly string[]): Record<string, number>`, `parseIntStrict(input: string, name: string, min?: number): number`. All throw `UsageError`.
  - `interface Rng { next(): number; int(min: number, max: number): number; pick<T>(xs: readonly T[]): T }`, `createRng(seed: number): Rng`, `pickWeighted(rng: Rng, weights: Readonly<Record<string, number>>): string`

- [ ] **Step 1: Create the package files**

`apps/cli/package.json`:

```json
{
  "name": "@modaco/cli",
  "version": "0.0.1",
  "private": true,
  "type": "module",
  "scripts": {
    "start": "tsx src/main.ts",
    "test": "vitest run",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "commander": "^14.0.3",
    "hdr-histogram-js": "^3.0.1",
    "undici": "^7.30.0"
  },
  "devDependencies": {
    "tsx": "^4.19.1",
    "typescript": "^5.6.3",
    "vitest": "^3.0.5"
  }
}
```

`apps/cli/tsconfig.json`:

```json
{ "extends": "../../tsconfig.base.json", "include": ["src", "test"] }
```

`apps/cli/vitest.config.ts`:

```ts
import { defineConfig } from 'vitest/config';
export default defineConfig({ test: { include: ['test/**/*.test.ts'], testTimeout: 30000, hookTimeout: 30000 } });
```

Root `package.json`: add this entry to `scripts`, after `"demo:flash-sale"` (add a comma to the preceding line):

```json
    "modaco": "tsx apps/cli/src/main.ts"
```

Run: `pnpm install`
Expected: installs commander, hdr-histogram-js and undici for `@modaco/cli` and updates `pnpm-lock.yaml`.

- [ ] **Step 2: Write the failing tests**

`apps/cli/test/parse.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { UsageError } from '../src/errors';
import { parseDuration, parseIntStrict, parseMix, parseRate } from '../src/load/parse';

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
```

`apps/cli/test/rng.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { createRng, pickWeighted } from '../src/load/rng';

describe('createRng', () => {
  it('is deterministic for a seed', () => {
    const a = createRng(42);
    const b = createRng(42);
    const xs = Array.from({ length: 5 }, () => a.next());
    expect(Array.from({ length: 5 }, () => b.next())).toEqual(xs);
    expect(createRng(43).next()).not.toBe(xs[0]);
  });
  it('keeps int() within inclusive bounds and pick() within the list', () => {
    const rng = createRng(1);
    const seen = new Set<number>();
    for (let i = 0; i < 1000; i++) seen.add(rng.int(1, 5));
    expect([...seen].sort()).toEqual([1, 2, 3, 4, 5]);
    expect(['a', 'b']).toContain(rng.pick(['a', 'b']));
    expect(() => rng.pick([])).toThrow();
  });
});

describe('pickWeighted', () => {
  it('follows the weights and never picks a zero weight', () => {
    const rng = createRng(7);
    const counts: Record<string, number> = { a: 0, b: 0, c: 0 };
    for (let i = 0; i < 100_000; i++) counts[pickWeighted(rng, { a: 70, b: 30, c: 0 })]!++;
    expect(counts.a! / 100_000).toBeCloseTo(0.7, 1);
    expect(counts.b! / 100_000).toBeCloseTo(0.3, 1);
    expect(counts.c).toBe(0);
  });
});
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `pnpm --filter @modaco/cli test`
Expected: FAIL, cannot resolve `../src/errors`, `../src/load/parse` and `../src/load/rng`.

- [ ] **Step 4: Implement**

`apps/cli/src/errors.ts`:

```ts
/** Bad command-line input. Exit code 2. */
export class UsageError extends Error {
  override name = 'UsageError';
}

/** A load scenario could not prepare (empty catalog, unknown category). Exit code 1. */
export class SetupError extends Error {
  override name = 'SetupError';
}

/** A 4xx/5xx API response, carrying the API's error envelope. Exit code 1. */
export class ApiError extends Error {
  override name = 'ApiError';
  constructor(readonly status: number, readonly code: string, message: string, readonly details?: unknown) {
    super(message);
  }

  toJSON(): { status: number; error: { code: string; message: string; details?: unknown } } {
    return { status: this.status, error: { code: this.code, message: this.message, ...(this.details === undefined ? {} : { details: this.details }) } };
  }
}
```

`apps/cli/src/load/parse.ts`:

```ts
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

export function parseIntStrict(input: string, name: string, min = 1): number {
  const n = /^-?\d+$/.test(input.trim()) ? Number(input) : Number.NaN;
  if (!Number.isSafeInteger(n) || n < min) throw new UsageError(`${name} must be an integer >= ${min}, got '${input}'`);
  return n;
}
```

`apps/cli/src/load/rng.ts`:

```ts
export interface Rng {
  /** Uniform in [0, 1). */
  next(): number;
  /** Uniform integer in [min, max], both inclusive. */
  int(min: number, max: number): number;
  pick<T>(xs: readonly T[]): T;
}

/** mulberry32: small, fast and seedable, so a run's request sequence is repeatable with --seed. */
export function createRng(seed: number): Rng {
  let a = seed >>> 0;
  const next = (): number => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  return {
    next,
    int: (min, max) => min + Math.floor(next() * (max - min + 1)),
    pick: <T>(xs: readonly T[]): T => {
      if (xs.length === 0) throw new Error('cannot pick from an empty list');
      return xs[Math.floor(next() * xs.length)]!;
    },
  };
}

/** Picks a key with probability proportional to its weight; zero weights are never picked. */
export function pickWeighted(rng: Rng, weights: Readonly<Record<string, number>>): string {
  const entries = Object.entries(weights).filter(([, w]) => w > 0);
  let x = rng.next() * entries.reduce((sum, [, w]) => sum + w, 0);
  for (const [key, w] of entries) {
    x -= w;
    if (x < 0) return key;
  }
  return entries[entries.length - 1]![0];
}
```

- [ ] **Step 5: Run the tests and the typecheck**

Run: `pnpm --filter @modaco/cli test && pnpm --filter @modaco/cli typecheck`
Expected: all parse and rng tests PASS, no type errors.

- [ ] **Step 6: Commit**

```bash
git add apps/cli package.json pnpm-lock.yaml
git commit -m "feat(cli): scaffold @modaco/cli with parsing helpers and seeded RNG

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Metrics (HDR histograms, status/error/instance counters)

**Files:**
- Create: `apps/cli/src/load/metrics.ts`
- Test: `apps/cli/test/metrics.test.ts`

**Interfaces:**
- Produces:
  - `type ErrorKind = 'timeout' | 'ECONNRESET' | 'ECONNREFUSED' | 'other'`, `classifyError(err: unknown): ErrorKind`
  - `interface LatencySummary { mean; p50; p90; p95; p99; p999; max }` (all `number`, ms)
  - `interface StatsSummary { count: number; rps: number; latencyMs: LatencySummary; status: Record<string, number>; errors: Partial<Record<ErrorKind, number>>; dropped: number }`
  - `interface IntervalSample { count: number; errors: number; dropped: number; p50Ms: number; p99Ms: number }`
  - `class Metrics { record(label: string, latencyUs: number, status: number, instance: string | undefined): void; recordError(label: string, kind: ErrorKind): void; recordDropped(label: string): void; takeInterval(): IntervalSample; summary(elapsedSeconds: number): { total: StatsSummary; byLabel: Record<string, StatsSummary> }; instanceCounts(): Record<string, number> }`

- [ ] **Step 1: Write the failing test**

`apps/cli/test/metrics.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { classifyError, Metrics } from '../src/load/metrics';

describe('Metrics', () => {
  it('reports percentiles in ms from microsecond samples', () => {
    const m = new Metrics();
    for (let i = 1; i <= 1000; i++) m.record('list', i * 1000, 200, 'a'); // 1..1000 ms
    const { total } = m.summary(2);
    expect(total.count).toBe(1000);
    expect(total.rps).toBe(500);
    expect(Math.abs(total.latencyMs.p50 - 500)).toBeLessThan(5);
    expect(Math.abs(total.latencyMs.p99 - 990)).toBeLessThan(10);
    expect(Math.abs(total.latencyMs.mean - 500.5)).toBeLessThan(5);
    expect(Math.abs(total.latencyMs.max - 1000)).toBeLessThan(10);
  });

  it('keeps per-label stats, status codes, errors and drops', () => {
    const m = new Metrics();
    m.record('list', 1000, 200, 'a');
    m.record('detail', 2000, 404, 'b');
    m.recordError('detail', 'timeout');
    m.recordDropped('list');
    const { total, byLabel } = m.summary(1);
    expect(total.status).toEqual({ '200': 1, '404': 1 });
    expect(total.errors).toEqual({ timeout: 1 });
    expect(total.dropped).toBe(1);
    expect(byLabel.list!.count).toBe(1);
    expect(byLabel.list!.dropped).toBe(1);
    expect(byLabel.detail!.status).toEqual({ '404': 1 });
    expect(byLabel.detail!.errors).toEqual({ timeout: 1 });
  });

  it('returns zeros for a label with no responses', () => {
    const m = new Metrics();
    m.recordError('list', 'other');
    expect(m.summary(1).byLabel.list!.latencyMs).toEqual({ mean: 0, p50: 0, p90: 0, p95: 0, p99: 0, p999: 0, max: 0 });
  });

  it('tallies responses per instance, with a missing header as unknown', () => {
    const m = new Metrics();
    m.record('list', 1000, 200, 'b');
    m.record('list', 1000, 200, 'a');
    m.record('list', 1000, 200, 'a');
    m.record('list', 1000, 200, undefined);
    expect(m.instanceCounts()).toEqual({ a: 2, b: 1, unknown: 1 });
    expect(Object.keys(m.instanceCounts())).toEqual(['a', 'b', 'unknown']);
  });

  it('takeInterval returns the interval sample and resets it', () => {
    const m = new Metrics();
    m.record('list', 5000, 200, 'a');
    m.recordError('list', 'other');
    m.recordDropped('list');
    const first = m.takeInterval();
    expect(first.count).toBe(1);
    expect(first.errors).toBe(1);
    expect(first.dropped).toBe(1);
    expect(Math.abs(first.p50Ms - 5)).toBeLessThan(0.1);
    expect(m.takeInterval()).toEqual({ count: 0, errors: 0, dropped: 0, p50Ms: 0, p99Ms: 0 });
    expect(m.summary(1).total.count).toBe(1); // totals are not reset
  });
});

describe('classifyError', () => {
  const withCode = (code: string) => Object.assign(new Error(code), { code });
  it('maps undici and socket codes to kinds', () => {
    expect(classifyError(withCode('UND_ERR_HEADERS_TIMEOUT'))).toBe('timeout');
    expect(classifyError(withCode('UND_ERR_BODY_TIMEOUT'))).toBe('timeout');
    expect(classifyError(withCode('UND_ERR_CONNECT_TIMEOUT'))).toBe('timeout');
    expect(classifyError(withCode('ECONNRESET'))).toBe('ECONNRESET');
    expect(classifyError(withCode('UND_ERR_SOCKET'))).toBe('ECONNRESET');
    expect(classifyError(withCode('ECONNREFUSED'))).toBe('ECONNREFUSED');
    expect(classifyError(new Error('boom'))).toBe('other');
    expect(classifyError('nope')).toBe('other');
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --filter @modaco/cli exec vitest run test/metrics.test.ts`
Expected: FAIL, cannot resolve `../src/load/metrics`.

- [ ] **Step 3: Implement**

`apps/cli/src/load/metrics.ts`:

```ts
import { build, type Histogram } from 'hdr-histogram-js';

export type ErrorKind = 'timeout' | 'ECONNRESET' | 'ECONNREFUSED' | 'other';

const TIMEOUT_CODES = new Set(['UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_BODY_TIMEOUT', 'UND_ERR_CONNECT_TIMEOUT', 'ETIMEDOUT']);
const RESET_CODES = new Set(['ECONNRESET', 'UND_ERR_SOCKET', 'EPIPE']);

/** Buckets a transport error (no HTTP status) by its code. */
export function classifyError(err: unknown): ErrorKind {
  const code = (err as { code?: unknown } | null)?.code;
  if (typeof code === 'string') {
    if (TIMEOUT_CODES.has(code)) return 'timeout';
    if (RESET_CODES.has(code)) return 'ECONNRESET';
    if (code === 'ECONNREFUSED') return 'ECONNREFUSED';
  }
  if ((err as { name?: unknown } | null)?.name === 'TimeoutError') return 'timeout';
  return 'other';
}

export interface LatencySummary { mean: number; p50: number; p90: number; p95: number; p99: number; p999: number; max: number }

export interface StatsSummary {
  count: number;
  rps: number;
  latencyMs: LatencySummary;
  status: Record<string, number>;
  errors: Partial<Record<ErrorKind, number>>;
  dropped: number;
}

export interface IntervalSample { count: number; errors: number; dropped: number; p50Ms: number; p99Ms: number }

const MAX_US = 60_000_000;
const newHistogram = (): Histogram => build({ lowestDiscernibleValue: 1, highestTrackableValue: MAX_US, numberOfSignificantValueDigits: 3 });
const clampUs = (us: number): number => Math.min(MAX_US, Math.max(1, Math.round(us)));
const toMs = (us: number): number => Math.round(us / 10) / 100;

class LabelStats {
  private readonly histogram = newHistogram();
  private readonly status: Record<string, number> = {};
  private readonly errors: Partial<Record<ErrorKind, number>> = {};
  dropped = 0;

  record(latencyUs: number, status: number): void {
    this.histogram.recordValue(clampUs(latencyUs));
    this.status[status] = (this.status[status] ?? 0) + 1;
  }

  recordError(kind: ErrorKind): void {
    this.errors[kind] = (this.errors[kind] ?? 0) + 1;
  }

  summary(elapsedSeconds: number): StatsSummary {
    const h = this.histogram;
    const count = h.totalCount;
    const at = (p: number) => (count === 0 ? 0 : toMs(h.getValueAtPercentile(p)));
    return {
      count,
      rps: elapsedSeconds > 0 ? Math.round((count / elapsedSeconds) * 10) / 10 : 0,
      latencyMs: {
        mean: count === 0 ? 0 : toMs(h.mean), p50: at(50), p90: at(90), p95: at(95), p99: at(99), p999: at(99.9),
        max: count === 0 ? 0 : toMs(h.maxValue),
      },
      status: { ...this.status },
      errors: { ...this.errors },
      dropped: this.dropped,
    };
  }
}

/** Everything one recorded phase measures. Warmup runs pass `null` instead of a Metrics. */
export class Metrics {
  private readonly total = new LabelStats();
  private readonly labels = new Map<string, LabelStats>();
  private readonly instances = new Map<string, number>();
  private readonly interval = newHistogram();
  private intervalErrors = 0;
  private intervalDropped = 0;

  record(label: string, latencyUs: number, status: number, instance: string | undefined): void {
    this.total.record(latencyUs, status);
    this.label(label).record(latencyUs, status);
    this.interval.recordValue(clampUs(latencyUs));
    const key = instance ?? 'unknown';
    this.instances.set(key, (this.instances.get(key) ?? 0) + 1);
  }

  recordError(label: string, kind: ErrorKind): void {
    this.total.recordError(kind);
    this.label(label).recordError(kind);
    this.intervalErrors++;
  }

  recordDropped(label: string): void {
    this.total.dropped++;
    this.label(label).dropped++;
    this.intervalDropped++;
  }

  /** Stats since the previous call, for the live progress line. */
  takeInterval(): IntervalSample {
    const count = this.interval.totalCount;
    const sample = {
      count, errors: this.intervalErrors, dropped: this.intervalDropped,
      p50Ms: count === 0 ? 0 : toMs(this.interval.getValueAtPercentile(50)),
      p99Ms: count === 0 ? 0 : toMs(this.interval.getValueAtPercentile(99)),
    };
    this.interval.reset();
    this.intervalErrors = 0;
    this.intervalDropped = 0;
    return sample;
  }

  summary(elapsedSeconds: number): { total: StatsSummary; byLabel: Record<string, StatsSummary> } {
    const byLabel: Record<string, StatsSummary> = {};
    for (const name of [...this.labels.keys()].sort()) byLabel[name] = this.labels.get(name)!.summary(elapsedSeconds);
    return { total: this.total.summary(elapsedSeconds), byLabel };
  }

  instanceCounts(): Record<string, number> {
    return Object.fromEntries([...this.instances.entries()].sort(([a], [b]) => a.localeCompare(b)));
  }

  private label(name: string): LabelStats {
    let stats = this.labels.get(name);
    if (!stats) { stats = new LabelStats(); this.labels.set(name, stats); }
    return stats;
  }
}
```

- [ ] **Step 4: Run the test and the typecheck**

Run: `pnpm --filter @modaco/cli exec vitest run test/metrics.test.ts && pnpm --filter @modaco/cli typecheck`
Expected: PASS, no type errors.

- [ ] **Step 5: Commit**

```bash
git add apps/cli/src/load/metrics.ts apps/cli/test/metrics.test.ts
git commit -m "feat(cli): HDR-histogram metrics with status, error and instance counters

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Load engine (open and closed models)

**Files:**
- Create: `apps/cli/src/load/engine.ts`
- Test: `apps/cli/test/engine.test.ts`

**Interfaces:**
- Consumes: `Metrics`, `classifyError` (Task 3); `Rng` (Task 2).
- Produces:
  - `interface RequestSpec { label: string; method: 'GET' | 'POST' | 'PATCH' | 'PUT'; path: string; body?: unknown; onResponse?(status: number, body: unknown): void }`
  - `interface SendResult { status: number; instance?: string; body?: unknown }`
  - `interface Transport { send(spec: RequestSpec): Promise<SendResult> }`
  - `interface LoadSource { next(rng: Rng): RequestSpec }`
  - `type LoadModel = { kind: 'open'; rate: number; rampMs: number; maxInflight: number } | { kind: 'closed'; concurrency: number }`
  - `interface ProgressSample { elapsedSeconds; rps; p50Ms; p99Ms; errors; dropped; inflight }` (all `number`)
  - `interface PhaseOptions { model: LoadModel; durationMs: number; signal?: AbortSignal; reportEveryMs?: number; onProgress?(s: ProgressSample): void; drainTimeoutMs?: number }`
  - `interface PhaseOutcome { elapsedSeconds: number; interrupted: boolean }`
  - `scheduledOffsetMs(k: number, ratePerSec: number, rampMs: number): number`
  - `runPhase(transport: Transport, source: LoadSource, rng: Rng, metrics: Metrics | null, opts: PhaseOptions): Promise<PhaseOutcome>`

- [ ] **Step 1: Write the failing test**

`apps/cli/test/engine.test.ts`:

```ts
import { performance } from 'node:perf_hooks';
import { describe, expect, it } from 'vitest';
import { runPhase, scheduledOffsetMs, type RequestSpec, type SendResult, type Transport } from '../src/load/engine';
import { Metrics } from '../src/load/metrics';
import { createRng } from '../src/load/rng';

const source = { next: (): RequestSpec => ({ label: 'get', method: 'GET', path: '/x' }) };
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** A transport that answers after `ms` and tracks its own concurrency. */
function delayed(ms: number, result: SendResult = { status: 200, instance: 'a' }) {
  const t = {
    inflight: 0,
    maxInflight: 0,
    async send(): Promise<SendResult> {
      t.inflight++;
      t.maxInflight = Math.max(t.maxInflight, t.inflight);
      try { await sleep(ms); return result; } finally { t.inflight--; }
    },
  };
  return t;
}

describe('scheduledOffsetMs', () => {
  it('spaces requests evenly without a ramp', () => {
    expect(scheduledOffsetMs(0, 100, 0)).toBe(0);
    expect(scheduledOffsetMs(1, 100, 0)).toBe(10);
    expect(scheduledOffsetMs(250, 100, 0)).toBe(2500);
  });
  it('ramps linearly from zero to the target rate', () => {
    // 1000/s over a 1 s ramp: 500 requests during the ramp, then one per ms.
    expect(scheduledOffsetMs(125, 1000, 1000)).toBeCloseTo(500);
    expect(scheduledOffsetMs(500, 1000, 1000)).toBeCloseTo(1000);
    expect(scheduledOffsetMs(1500, 1000, 1000)).toBeCloseTo(2000);
  });
});

describe('runPhase', () => {
  it('holds the target rate in the open model', async () => {
    const metrics = new Metrics();
    const out = await runPhase(delayed(5), source, createRng(1), metrics, { model: { kind: 'open', rate: 500, rampMs: 0, maxInflight: 10_000 }, durationMs: 2000 });
    const { total } = metrics.summary(out.elapsedSeconds);
    expect(total.count).toBeGreaterThanOrEqual(950);
    expect(total.count).toBeLessThanOrEqual(1050);
    expect(total.rps).toBeGreaterThan(475);
    expect(out.interrupted).toBe(false);
  });

  it('keeps exactly n requests in flight in the closed model', async () => {
    const t = delayed(10);
    const metrics = new Metrics();
    await runPhase(t, source, createRng(1), metrics, { model: { kind: 'closed', concurrency: 8 }, durationMs: 300 });
    expect(t.maxInflight).toBe(8);
    expect(metrics.summary(0.3).total.count).toBeGreaterThanOrEqual(8 * 15);
  });

  it('charges a client-side stall to the requests scheduled during it (coordinated omission)', async () => {
    let calls = 0;
    const transport: Transport = {
      async send() {
        calls++;
        if (calls === 40) {
          const until = performance.now() + 300;
          while (performance.now() < until) { /* block the event loop, like a GC pause or a stalled client */ }
        }
        return { status: 200 };
      },
    };
    const metrics = new Metrics();
    await runPhase(transport, source, createRng(1), metrics, { model: { kind: 'open', rate: 200, rampMs: 0, maxInflight: 10_000 }, durationMs: 1000 });
    const { total } = metrics.summary(1);
    // About 60 of 200 requests were due while the loop was blocked. Timed from send, they would all read about 0 ms.
    expect(total.latencyMs.max).toBeGreaterThanOrEqual(250);
    expect(total.latencyMs.p90).toBeGreaterThanOrEqual(50);
  });

  it('drops requests beyond maxInflight instead of queueing them', async () => {
    const metrics = new Metrics();
    await runPhase(delayed(500), source, createRng(1), metrics, { model: { kind: 'open', rate: 1000, rampMs: 0, maxInflight: 10 }, durationMs: 200 });
    const { total } = metrics.summary(0.2);
    expect(total.count).toBe(10);
    expect(total.dropped).toBe(190);
  });

  it('stops early when the signal aborts', async () => {
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 100);
    const started = performance.now();
    const out = await runPhase(delayed(5), source, createRng(1), new Metrics(), { model: { kind: 'closed', concurrency: 4 }, durationMs: 5000, signal: ac.signal });
    expect(out.interrupted).toBe(true);
    expect(performance.now() - started).toBeLessThan(1000);
  });

  it('records transport errors by kind and hands parsed bodies to onResponse', async () => {
    let n = 0;
    const seen: Array<[number, unknown]> = [];
    const transport: Transport = {
      async send() {
        n++;
        if (n % 2 === 1) throw Object.assign(new Error('reset'), { code: 'ECONNRESET' });
        await sleep(1);
        return { status: 201, body: { id: n } };
      },
    };
    const src = { next: (): RequestSpec => ({ label: 'create', method: 'POST', path: '/x', onResponse: (status, body) => { seen.push([status, body]); } }) };
    const metrics = new Metrics();
    await runPhase(transport, src, createRng(1), metrics, { model: { kind: 'closed', concurrency: 1 }, durationMs: 100 });
    const create = metrics.summary(0.1).byLabel.create!;
    expect(create.errors.ECONNRESET).toBeGreaterThan(0);
    expect(create.status['201']).toBe(seen.length);
    expect(seen[0]).toEqual([201, { id: 2 }]);
  });

  it('reports progress while recording and runs unrecorded with null metrics', async () => {
    const samples: number[] = [];
    await runPhase(delayed(2), source, createRng(1), new Metrics(), { model: { kind: 'closed', concurrency: 2 }, durationMs: 250, reportEveryMs: 50, onProgress: (s) => samples.push(s.rps) });
    expect(samples.length).toBeGreaterThanOrEqual(3);
    expect(samples.some((rps) => rps > 0)).toBe(true);
    await expect(runPhase(delayed(2), source, createRng(1), null, { model: { kind: 'closed', concurrency: 2 }, durationMs: 50 })).resolves.toMatchObject({ interrupted: false });
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --filter @modaco/cli exec vitest run test/engine.test.ts`
Expected: FAIL, cannot resolve `../src/load/engine`.

- [ ] **Step 3: Implement**

`apps/cli/src/load/engine.ts`:

```ts
import { performance } from 'node:perf_hooks';
import { classifyError, type Metrics } from './metrics';
import type { Rng } from './rng';

export interface RequestSpec {
  /** Groups the request in the report, e.g. "list" or "promo:create". */
  label: string;
  method: 'GET' | 'POST' | 'PATCH' | 'PUT';
  path: string;
  body?: unknown;
  /** Receives the parsed JSON body. Only requests that set it pay for parsing. */
  onResponse?(status: number, body: unknown): void;
}

export interface SendResult { status: number; instance?: string; body?: unknown }

export interface Transport { send(spec: RequestSpec): Promise<SendResult> }

export interface LoadSource { next(rng: Rng): RequestSpec }

export type LoadModel =
  | { kind: 'open'; rate: number; rampMs: number; maxInflight: number }
  | { kind: 'closed'; concurrency: number };

export interface ProgressSample { elapsedSeconds: number; rps: number; p50Ms: number; p99Ms: number; errors: number; dropped: number; inflight: number }

export interface PhaseOptions {
  model: LoadModel;
  durationMs: number;
  signal?: AbortSignal;
  reportEveryMs?: number;
  onProgress?(sample: ProgressSample): void;
  /** How long an interrupted phase waits for in-flight requests. Default 5 s. */
  drainTimeoutMs?: number;
}

export interface PhaseOutcome { elapsedSeconds: number; interrupted: boolean }

/**
 * Offset in ms from phase start of the k-th request (0-based) when the rate rises linearly from 0 to
 * `ratePerSec` over `rampMs` and then stays flat. It inverts the cumulative count N(t): r·t²/(2·ramp) during
 * the ramp, and r·ramp/2 + r·(t − ramp) after it.
 */
export function scheduledOffsetMs(k: number, ratePerSec: number, rampMs: number): number {
  const perMs = ratePerSec / 1000;
  if (rampMs <= 0) return k / perMs;
  const rampCount = (perMs * rampMs) / 2;
  if (k <= rampCount) return Math.sqrt((2 * k * rampMs) / perMs);
  return rampMs + (k - rampCount) / perMs;
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const yieldToIo = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * Runs one phase of load. Open model: requests are due on a fixed schedule whatever the response times are, and
 * latency runs from the scheduled time, so a stalled server (or client) is charged for the requests it delayed
 * (coordinated omission). Closed model: `concurrency` workers loop send -> await -> send, and latency runs from the
 * send time.
 */
export async function runPhase(transport: Transport, source: LoadSource, rng: Rng, metrics: Metrics | null, opts: PhaseOptions): Promise<PhaseOutcome> {
  const inflight = new Set<Promise<void>>();
  const aborted = () => opts.signal?.aborted === true;
  const start = performance.now();
  const deadline = start + opts.durationMs;

  const issue = (spec: RequestSpec, t0: number): Promise<void> => {
    const done: Promise<void> = transport.send(spec).then(
      (res) => {
        metrics?.record(spec.label, (performance.now() - t0) * 1000, res.status, res.instance);
        try { spec.onResponse?.(res.status, res.body); } catch { /* a scenario hook must not stop the load */ }
      },
      (err: unknown) => { metrics?.recordError(spec.label, classifyError(err)); },
    ).finally(() => { inflight.delete(done); });
    inflight.add(done);
    return done;
  };

  let timer: NodeJS.Timeout | undefined;
  if (metrics && opts.onProgress && opts.reportEveryMs) {
    const onProgress = opts.onProgress;
    let last = start;
    timer = setInterval(() => {
      const now = performance.now();
      const s = metrics.takeInterval();
      onProgress({ elapsedSeconds: (now - start) / 1000, rps: s.count / ((now - last) / 1000), p50Ms: s.p50Ms, p99Ms: s.p99Ms, errors: s.errors, dropped: s.dropped, inflight: inflight.size });
      last = now;
    }, opts.reportEveryMs);
  }

  try {
    if (opts.model.kind === 'open') {
      const { rate, rampMs, maxInflight } = opts.model;
      const fire = (t0: number) => {
        const spec = source.next(rng);
        if (inflight.size >= maxInflight) { metrics?.recordDropped(spec.label); return; }
        void issue(spec, t0);
      };
      let k = 0;
      while (!aborted()) {
        const next = scheduledOffsetMs(k, rate, rampMs);
        if (next >= opts.durationMs) break;
        const wait = start + next - performance.now();
        if (wait >= 1) { await sleep(wait); continue; }
        // Fire everything that is due, including requests that fell behind during a stall.
        const now = performance.now() - start;
        for (let t = next; t <= now && t < opts.durationMs; t = scheduledOffsetMs(k, rate, rampMs)) {
          fire(start + t);
          k++;
        }
        await yieldToIo();
      }
      const rest = deadline - performance.now();
      if (rest > 0 && !aborted()) await sleep(rest);
    } else {
      const worker = async () => {
        while (!aborted() && performance.now() < deadline) await issue(source.next(rng), performance.now());
      };
      const workers = Promise.all(Array.from({ length: opts.model.concurrency }, worker));
      const onAbort = new Promise<void>((resolve) => {
        if (aborted()) resolve();
        else opts.signal?.addEventListener('abort', () => resolve(), { once: true });
      });
      await Promise.race([workers, onAbort]);
    }
  } finally {
    clearInterval(timer);
  }

  const stoppedAt = Math.min(performance.now(), deadline);
  const interrupted = aborted();
  const drain = Promise.allSettled([...inflight]);
  if (interrupted) {
    let grace: NodeJS.Timeout | undefined;
    await Promise.race([drain, new Promise<void>((resolve) => { grace = setTimeout(resolve, opts.drainTimeoutMs ?? 5000); })]);
    clearTimeout(grace);
  } else {
    await drain;
  }
  return { elapsedSeconds: (stoppedAt - start) / 1000, interrupted };
}
```

- [ ] **Step 4: Run the test and the typecheck**

Run: `pnpm --filter @modaco/cli exec vitest run test/engine.test.ts && pnpm --filter @modaco/cli typecheck`
Expected: all 9 tests PASS, no type errors. If the rate test is flaky on a loaded machine, re-run it once. It relies on the catch-up loop, not on timer precision, so the count should be 1000 give or take a handful.

- [ ] **Step 5: Commit**

```bash
git add apps/cli/src/load/engine.ts apps/cli/test/engine.test.ts
git commit -m "feat(cli): load engine with open (scheduled-time) and closed models

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: API client, response types, output helpers, stub API

**Files:**
- Create: `apps/cli/src/api-types.ts`, `apps/cli/src/client.ts`, `apps/cli/src/output.ts`
- Create: `apps/cli/test/stub-api.ts` (test helper, used by Tasks 5 to 9)
- Test: `apps/cli/test/client.test.ts`

**Interfaces:**
- Consumes: `ApiError` (Task 2); `RequestSpec`, `SendResult`, `Transport` (Task 4).
- Produces:
  - Types in `api-types.ts`: `Category`, `ActivePromotion`, `Product`, `Page<T>`, `ListProductsQuery`, `CreateProductInput`, `StockInput`, `Target`, `CreatePromotionInput`, `Promotion`, `IngestionJobCreated`, `IngestionJob`, `Rejection`, `HealthBody` (exact fields below).
  - `interface Reply<T> { status: number; body: T; instance?: string }`
  - `class ApiClient implements Transport` with `constructor({ baseUrl, timeoutMs, connections? })`, `readonly baseUrl`, `send(spec)`, `call<T>(method, path, body?, accept?)`, `health(): Promise<Reply<HealthBody>>`, `listProducts(q?)`, `getProduct(id)`, `createProduct(input)`, `setStock(id, input)`, `createPromotion(input)`, `getPromotion(id)`, `cancelPromotion(id)`, `retargetPromotion(id, target)`, `createIngestionJob(filename)`, `getIngestionJob(id)`, `listRejections(id, q?)`, `close()`.
  - `output.ts`: `out(text)`, `log(text)`, `emit(json: boolean, value: unknown, human: () => string)`, `formatTable(rows)`, `formatFields(fields)`.
  - `stub-api.ts`: `startStubApi(opts?: { total?: number; degraded?: boolean; midSalePrice?: string }): Promise<StubApi>` where `StubApi = { url: string; state: StubState; close(): Promise<void> }` and `StubState = { hits: Map<string, number>; samplePages: number; openPromotions: Set<string>; lastPromotionId?: string }`. `MIDSALE_ID = 900001`.

- [ ] **Step 1: Write the stub API test helper**

`apps/cli/test/stub-api.ts`:

```ts
import { randomUUID } from 'node:crypto';
import { createServer, type IncomingMessage } from 'node:http';
import type { AddressInfo } from 'node:net';

/** The id POST /products returns; GET of it reads back with the stub's mid-sale price and the latest promotion. */
export const MIDSALE_ID = 900001;
const SLUGS = ['accessories', 'shoes', 'bags'];
const INSTANCES = ['i1', 'i2', 'i3'];

export interface StubState {
  /** Request counts by route, e.g. "GET /products/:id". */
  hits: Map<string, number>;
  /** Listing requests with pageSize=100, which is what catalog sampling uses. */
  samplePages: number;
  openPromotions: Set<string>;
  lastPromotionId?: string;
}

export interface StubApi { url: string; state: StubState; close(): Promise<void> }

async function readJson(req: IncomingMessage): Promise<Record<string, unknown> | undefined> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const text = Buffer.concat(chunks).toString();
  return text ? (JSON.parse(text) as Record<string, unknown>) : undefined;
}

const product = (id: number) => {
  const slug = SLUGS[id % SLUGS.length]!;
  return {
    id, sku: `SKU-${id}`, name: `Product ${id}`,
    category: { id: (id % SLUGS.length) + 1, name: slug, slug },
    basePrice: '20.00', effectivePrice: '20.00', activePromotion: null, stock: 5,
  };
};

/** A fake ModaCo API: a catalog of `total` products and in-memory promotions. Rotates X-Instance-Id over i1, i2, i3. */
export async function startStubApi(opts: { total?: number; degraded?: boolean; midSalePrice?: string } = {}): Promise<StubApi> {
  const total = opts.total ?? 1000;
  const state: StubState = { hits: new Map(), samplePages: 0, openPromotions: new Set() };
  let served = 0;

  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? '/', 'http://stub');
    const body = await readJson(req);
    res.setHeader('x-instance-id', INSTANCES[served++ % INSTANCES.length]!);
    const send = (status: number, value: unknown) => {
      res.statusCode = status;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify(value));
    };
    const route = `${req.method} ${url.pathname.replace(/\/[0-9a-f-]{36}(?=\/|$)/, '/:uuid').replace(/\/\d+(?=\/|$)/, '/:id')}`;
    state.hits.set(route, (state.hits.get(route) ?? 0) + 1);
    const segment = url.pathname.split('/')[2] ?? '';

    switch (route) {
      case 'GET /health':
        return opts.degraded
          ? send(503, { status: 'degraded', checks: { postgres: false, redis: true } })
          : send(200, { status: 'ok', checks: { postgres: true, redis: true } });
      case 'GET /products': {
        const page = Number(url.searchParams.get('page') ?? 1);
        const pageSize = Number(url.searchParams.get('pageSize') ?? 20);
        if (pageSize === 100) state.samplePages++;
        const items = [];
        for (let id = (page - 1) * pageSize + 1; id <= Math.min(total, page * pageSize); id++) items.push(product(id));
        return send(200, { items, pagination: { page, pageSize, total } });
      }
      case 'GET /products/:id': {
        const id = Number(segment);
        if (id === 404) return send(404, { error: { code: 'not_found', message: 'product 404 not found' } });
        if (id === MIDSALE_ID) {
          const promo = state.lastPromotionId ? { id: state.lastPromotionId, name: 'Flash', discountType: 'percentage', value: '50' } : null;
          return send(200, { ...product(id), effectivePrice: opts.midSalePrice ?? '10.00', activePromotion: promo });
        }
        return send(200, product(id));
      }
      case 'POST /products':
        return send(201, { ...product(MIDSALE_ID), sku: body?.sku, basePrice: body?.basePrice });
      case 'PATCH /products/:id/stock':
        return send(200, { id: Number(segment), stock: Number(body?.stock ?? 0) });
      case 'POST /promotions': {
        const id = randomUUID();
        state.openPromotions.add(id);
        state.lastPromotionId = id;
        return send(201, { id, ...body, cancelledAt: null, createdAt: new Date().toISOString() });
      }
      case 'POST /promotions/:uuid/cancel':
        state.openPromotions.delete(segment);
        return send(200, { id: segment, cancelledAt: new Date().toISOString() });
      default:
        return send(404, { error: { code: 'not_found', message: 'route not found' } });
    }
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    state,
    close: () => new Promise<void>((resolve) => { server.closeAllConnections(); server.close(() => resolve()); }),
  };
}
```

- [ ] **Step 2: Write the failing test**

`apps/cli/test/client.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ApiClient } from '../src/client';
import { ApiError } from '../src/errors';
import { formatFields, formatTable } from '../src/output';
import { startStubApi, type StubApi } from './stub-api';

let stub: StubApi;
let client: ApiClient;
beforeEach(async () => {
  stub = await startStubApi();
  client = new ApiClient({ baseUrl: `${stub.url}/`, timeoutMs: 5000 });
});
afterEach(async () => {
  await client.close();
  await stub.close();
});

describe('ApiClient', () => {
  it('calls endpoints and exposes the answering instance', async () => {
    const health = await client.health();
    expect(health.body.status).toBe('ok');
    expect(health.instance).toMatch(/^i\d$/);
    const page = await client.listProducts({ category: 'shoes', page: 2, pageSize: 5 });
    expect(page.items.map((p) => p.id)).toEqual([6, 7, 8, 9, 10]);
    expect(page.pagination.total).toBe(1000);
  });

  it('throws ApiError with the envelope on 4xx', async () => {
    const err = await client.getProduct(404).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect(err).toMatchObject({ status: 404, code: 'not_found', message: 'product 404 not found' });
    expect((err as ApiError).toJSON()).toEqual({ status: 404, error: { code: 'not_found', message: 'product 404 not found' } });
  });

  it('send() never throws on status and only parses bodies on request', async () => {
    const drained = await client.send({ label: 'detail', method: 'GET', path: '/products/404' });
    expect(drained).toEqual({ status: 404, instance: expect.stringMatching(/^i\d$/) });
    const parsed = await client.send({ label: 'promo:create', method: 'POST', path: '/promotions', body: { name: 'x' }, onResponse: () => {} });
    expect(parsed.status).toBe(201);
    expect(parsed.body).toMatchObject({ name: 'x', id: expect.any(String) });
  });

  it('health() accepts a 503 instead of throwing', async () => {
    const degraded = await startStubApi({ degraded: true });
    const c = new ApiClient({ baseUrl: degraded.url, timeoutMs: 5000 });
    try {
      const r = await c.health();
      expect(r.status).toBe(503);
      expect(r.body.checks.postgres).toBe(false);
    } finally {
      await c.close();
      await degraded.close();
    }
  });
});

describe('output', () => {
  it('aligns table columns and renders empty fields as -', () => {
    expect(formatTable([['id', 'name'], [1, 'a'], [100, 'bb']])).toBe('id   name\n1    a\n100  bb');
    expect(formatFields([['a', null], ['long', { x: 1 }]])).toBe('a     -\nlong  {"x":1}');
  });
});
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `pnpm --filter @modaco/cli exec vitest run test/client.test.ts`
Expected: FAIL, cannot resolve `../src/client` and `../src/output`.

- [ ] **Step 4: Implement**

`apps/cli/src/api-types.ts`:

```ts
/** Response and input shapes of the ModaCo API, as the CLI uses them. Money is a decimal string. */

export interface Category { id: number; name: string; slug: string }

export interface ActivePromotion { id: string; name: string; discountType: 'percentage' | 'fixed'; value: string }

export interface Product {
  id: number;
  sku: string;
  name: string;
  category: Category;
  basePrice: string;
  effectivePrice: string;
  activePromotion: ActivePromotion | null;
  stock: number;
}

export interface Page<T> { items: T[]; pagination: { page: number; pageSize: number; total: number } }

export interface ListProductsQuery { category?: string; sort?: 'effective_price' | '-effective_price'; page?: number; pageSize?: number }

export interface CreateProductInput { sku: string; name: string; categoryId: number; basePrice: string; stock?: number }

export type StockInput = { stock: number } | { delta: number };

export type Target = { productId: number } | { categoryId: number };

export interface CreatePromotionInput {
  name: string;
  discountType: 'percentage' | 'fixed';
  value: string;
  startsAt: string;
  endsAt: string;
  target: Target;
}

export interface Promotion {
  id: string;
  name: string;
  discountType: 'percentage' | 'fixed';
  value: string;
  startsAt: string;
  endsAt: string;
  target: Target;
  cancelledAt: string | null;
  createdAt: string;
}

export interface IngestionJobCreated { jobId: string; uploadUrl: string; key: string; expiresInSeconds: number }

export interface IngestionJob {
  id: string;
  status: 'pending' | 'splitting' | 'processing' | 'completed' | 'failed';
  s3Key: string;
  totalChunks: number;
  completedChunks: number;
  failedChunks: number;
  rowsProcessed: number;
  rowsRejected: number;
  error: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface Rejection { chunkIndex: number; lineNumber: number; rawLine: string; reason: string }

export interface HealthBody { status: 'ok' | 'degraded'; checks: { postgres: boolean; redis: boolean } }
```

`apps/cli/src/client.ts`:

```ts
import { Agent, request } from 'undici';
import type {
  CreateProductInput, CreatePromotionInput, HealthBody, IngestionJob, IngestionJobCreated, ListProductsQuery,
  Page, Product, Promotion, Rejection, StockInput, Target,
} from './api-types';
import { ApiError } from './errors';
import type { RequestSpec, SendResult, Transport } from './load/engine';

type Method = RequestSpec['method'];

export interface Reply<T> { status: number; body: T; instance?: string }

export interface ClientOptions {
  baseUrl: string;
  timeoutMs: number;
  /** Connection pool size to the target. Default 16, which is plenty for one-off commands. */
  connections?: number;
}

/** Bodies up to this size are read to the end so the keep-alive connection can be reused. */
const MAX_DRAIN_BYTES = 8 * 1024 * 1024;

const headerValue = (v: string | string[] | undefined): string | undefined => (Array.isArray(v) ? v[0] : v);

function parseJson(text: string): unknown {
  if (!text) return undefined;
  try { return JSON.parse(text); } catch { return text; }
}

function query(params: Record<string, string | number | undefined>): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined) q.set(k, String(v));
  const s = q.toString();
  return s ? `?${s}` : '';
}

export class ApiClient implements Transport {
  readonly baseUrl: string;
  private readonly agent: Agent;
  private readonly timeoutMs: number;

  constructor(opts: ClientOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/+$/, '');
    this.timeoutMs = opts.timeoutMs;
    this.agent = new Agent({ connections: opts.connections ?? 16, keepAliveTimeout: 30_000, connect: { timeout: opts.timeoutMs } });
  }

  private dispatch(method: Method, path: string, body?: unknown) {
    return request(`${this.baseUrl}${path}`, {
      dispatcher: this.agent,
      method,
      headers: body === undefined ? {} : { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
      headersTimeout: this.timeoutMs,
      bodyTimeout: this.timeoutMs,
    });
  }

  /** Load path: never throws on status; drains the body unless the spec asks for it. */
  async send(spec: RequestSpec): Promise<SendResult> {
    const res = await this.dispatch(spec.method, spec.path, spec.body);
    const instance = headerValue(res.headers['x-instance-id']);
    if (spec.onResponse) return { status: res.statusCode, instance, body: parseJson(await res.body.text()) };
    await res.body.dump({ limit: MAX_DRAIN_BYTES });
    return { status: res.statusCode, instance };
  }

  /** Operational path: parses JSON and throws ApiError on 4xx/5xx unless the status is in `accept`. */
  async call<T>(method: Method, path: string, body?: unknown, accept: number[] = []): Promise<Reply<T>> {
    const res = await this.dispatch(method, path, body);
    const text = await res.body.text();
    const parsed = parseJson(text);
    const instance = headerValue(res.headers['x-instance-id']);
    if (res.statusCode >= 400 && !accept.includes(res.statusCode)) {
      const e = (parsed as { error?: { code?: string; message?: string; details?: unknown } } | undefined)?.error;
      throw new ApiError(res.statusCode, e?.code ?? `http_${res.statusCode}`, e?.message ?? (text.slice(0, 200) || `HTTP ${res.statusCode}`), e?.details);
    }
    return { status: res.statusCode, body: parsed as T, instance };
  }

  health(): Promise<Reply<HealthBody>> {
    return this.call<HealthBody>('GET', '/health', undefined, [503]);
  }

  async listProducts(q: ListProductsQuery = {}): Promise<Page<Product>> {
    return (await this.call<Page<Product>>('GET', `/products${query({ category: q.category, sort: q.sort, page: q.page, pageSize: q.pageSize })}`)).body;
  }

  async getProduct(id: number): Promise<Product> {
    return (await this.call<Product>('GET', `/products/${id}`)).body;
  }

  async createProduct(input: CreateProductInput): Promise<Product> {
    return (await this.call<Product>('POST', '/products', input)).body;
  }

  async setStock(id: number, input: StockInput): Promise<{ id: number; stock: number }> {
    return (await this.call<{ id: number; stock: number }>('PATCH', `/products/${id}/stock`, input)).body;
  }

  async createPromotion(input: CreatePromotionInput): Promise<Promotion> {
    return (await this.call<Promotion>('POST', '/promotions', input)).body;
  }

  async getPromotion(id: string): Promise<Promotion> {
    return (await this.call<Promotion>('GET', `/promotions/${encodeURIComponent(id)}`)).body;
  }

  async cancelPromotion(id: string): Promise<Promotion> {
    return (await this.call<Promotion>('POST', `/promotions/${encodeURIComponent(id)}/cancel`)).body;
  }

  async retargetPromotion(id: string, target: Target): Promise<Promotion> {
    return (await this.call<Promotion>('PUT', `/promotions/${encodeURIComponent(id)}/target`, target)).body;
  }

  async createIngestionJob(filename: string): Promise<IngestionJobCreated> {
    return (await this.call<IngestionJobCreated>('POST', '/ingestion/jobs', { filename })).body;
  }

  async getIngestionJob(id: string): Promise<IngestionJob> {
    return (await this.call<IngestionJob>('GET', `/ingestion/jobs/${encodeURIComponent(id)}`)).body;
  }

  async listRejections(id: string, q: { page?: number; pageSize?: number } = {}): Promise<Page<Rejection>> {
    return (await this.call<Page<Rejection>>('GET', `/ingestion/jobs/${encodeURIComponent(id)}/rejections${query({ page: q.page, pageSize: q.pageSize })}`)).body;
  }

  close(): Promise<void> {
    return this.agent.close();
  }
}
```

`apps/cli/src/output.ts`:

```ts
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
```

- [ ] **Step 5: Run the test and the typecheck**

Run: `pnpm --filter @modaco/cli exec vitest run test/client.test.ts && pnpm --filter @modaco/cli typecheck`
Expected: PASS, no type errors.

- [ ] **Step 6: Commit**

```bash
git add apps/cli/src/api-types.ts apps/cli/src/client.ts apps/cli/src/output.ts apps/cli/test/stub-api.ts apps/cli/test/client.test.ts
git commit -m "feat(cli): undici API client, output helpers and a stub API for tests

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Operational commands and the `modaco` entry point

**Files:**
- Create: `apps/cli/src/main.ts`, `apps/cli/src/commands/common.ts`, `apps/cli/src/commands/health.ts`, `apps/cli/src/commands/products.ts`, `apps/cli/src/commands/promotions.ts`, `apps/cli/src/commands/ingest.ts`
- Test: `apps/cli/test/cli.test.ts`

**Interfaces:**
- Consumes: `ApiClient`, `Reply` (Task 5); `emit`, `out`, `log`, `formatTable`, `formatFields` (Task 5); `parseDuration`, `parseIntStrict` (Task 2); error classes (Task 2).
- Produces:
  - `interface Globals { url: string; json: boolean; timeoutMs: number }`, `globals(cmd: Command): Globals`
  - `withClient<T>(cmd: Command, fn: (client: ApiClient, g: Globals) => Promise<T>): Promise<T>`
  - `arg<T>(parse: (v: string) => T): (v: string) => T` (wraps errors as commander `InvalidArgumentError`), `intArg(name: string, min?: number)`, `durationArg`
  - `registerHealth`, `registerProducts`, `registerPromotions`, `registerIngest`, each `(program: Command) => void`
  - `main.ts` builds the program; Task 8 adds `registerLoad`.

- [ ] **Step 1: Write the failing test**

`apps/cli/test/cli.test.ts`:

```ts
import { execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { startStubApi, type StubApi } from './stub-api';

const cliDir = fileURLToPath(new URL('..', import.meta.url));

/** Runs the real CLI in a child process, the way a person or an agent would. */
export function runCli(args: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
  return new Promise((resolve) => {
    execFile(process.execPath, ['--import', 'tsx', 'src/main.ts', ...args], { cwd: cliDir, env: { ...process.env, API_URL: '' } }, (err, stdout, stderr) => {
      resolve({ code: err ? (typeof err.code === 'number' ? err.code : 1) : 0, stdout, stderr });
    });
  });
}

let stub: StubApi;
beforeAll(async () => { stub = await startStubApi(); });
afterAll(() => stub.close());

describe('operational commands', () => {
  it('health --json prints the body', async () => {
    const r = await runCli(['--url', stub.url, 'health', '--json']);
    expect(r.code).toBe(0);
    expect(JSON.parse(r.stdout)).toEqual({ status: 'ok', checks: { postgres: true, redis: true } });
  });

  it('products list renders a table with the total', async () => {
    const r = await runCli(['products', 'list', '--page-size', '2', '--url', stub.url]);
    expect(r.code).toBe(0);
    expect(r.stdout).toContain('SKU-1');
    expect(r.stdout).toContain('SKU-2');
    expect(r.stdout).toContain('2 of 1000 products');
  });

  it('prints the API error envelope and exits 1 on 404', async () => {
    const human = await runCli(['--url', stub.url, 'products', 'get', '404']);
    expect(human.code).toBe(1);
    expect(human.stderr).toContain('HTTP 404 not_found: product 404 not found');
    const json = await runCli(['--url', stub.url, '--json', 'products', 'get', '404']);
    expect(json.code).toBe(1);
    expect(JSON.parse(json.stdout)).toEqual({ status: 404, error: { code: 'not_found', message: 'product 404 not found' } });
  });

  it('creates and cancels a promotion', async () => {
    const created = await runCli(['--url', stub.url, '--json', 'promotions', 'create', '--name', 'Test', '--type', 'percentage', '--value', '10', '--category', '1']);
    expect(created.code).toBe(0);
    const promo = JSON.parse(created.stdout) as { id: string; target: unknown };
    expect(promo.target).toEqual({ categoryId: 1 });
    expect(stub.state.openPromotions.has(promo.id)).toBe(true);
    const cancelled = await runCli(['--url', stub.url, 'promotions', 'cancel', promo.id]);
    expect(cancelled.code).toBe(0);
    expect(stub.state.openPromotions.has(promo.id)).toBe(false);
  });

  it('exits 2 on usage errors', async () => {
    expect((await runCli(['--url', stub.url, 'products', 'get', 'abc'])).code).toBe(2);
    expect((await runCli(['--url', stub.url, 'products', 'stock', '1'])).code).toBe(2);
    expect((await runCli(['--url', stub.url, 'promotions', 'create', '--name', 'x', '--type', 'percentage', '--value', '1'])).code).toBe(2);
    expect((await runCli(['--url', stub.url, '--timeout', 'soon', 'health'])).code).toBe(2);
  });

  it('exits 1 with the cause when the target is unreachable', async () => {
    const r = await runCli(['--url', 'http://127.0.0.1:9', 'health']);
    expect(r.code).toBe(1);
    expect(r.stderr).toContain('ECONNREFUSED');
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --filter @modaco/cli exec vitest run test/cli.test.ts`
Expected: FAIL, every case exits 1 with `Cannot find module .../src/main.ts`.

- [ ] **Step 3: Implement**

`apps/cli/src/commands/common.ts`:

```ts
import { InvalidArgumentError, type Command } from 'commander';
import { ApiClient } from '../client';
import { parseDuration, parseIntStrict } from '../load/parse';

export interface Globals { url: string; json: boolean; timeoutMs: number }

/** The program-level options, read from any subcommand. */
export function globals(cmd: Command): Globals {
  const o = cmd.optsWithGlobals<{ url: string; json?: boolean; timeout: string }>();
  return { url: o.url, json: o.json === true, timeoutMs: parseDuration(o.timeout) };
}

/** Runs `fn` with a client for the target and always closes its connection pool. */
export async function withClient<T>(cmd: Command, fn: (client: ApiClient, g: Globals) => Promise<T>): Promise<T> {
  const g = globals(cmd);
  const client = new ApiClient({ baseUrl: g.url, timeoutMs: g.timeoutMs });
  try {
    return await fn(client, g);
  } finally {
    await client.close();
  }
}

/** Adapts a parser that throws UsageError to commander, which reports InvalidArgumentError as a usage error. */
export function arg<T>(parse: (value: string) => T): (value: string) => T {
  return (value) => {
    try {
      return parse(value);
    } catch (err) {
      throw new InvalidArgumentError((err as Error).message);
    }
  };
}

export const intArg = (name: string, min = 1) => arg((v) => parseIntStrict(v, name, min));
export const durationArg = arg(parseDuration);
```

`apps/cli/src/commands/health.ts`:

```ts
import type { Command } from 'commander';
import type { HealthBody } from '../api-types';
import type { Reply } from '../client';
import { emit, out } from '../output';
import { durationArg, withClient } from './common';

const describe = (r: Reply<HealthBody>) =>
  `${r.body.status}  postgres ${r.body.checks.postgres ? 'up' : 'DOWN'}  redis ${r.body.checks.redis ? 'up' : 'DOWN'}  instance ${r.instance ?? '-'}`;

export function registerHealth(program: Command): void {
  program.command('health')
    .description('GET /health: Postgres and Redis checks, and which instance answered')
    .option('--watch <interval>', 'poll until interrupted, e.g. 2s (with --json: one JSON line per poll)', durationArg)
    .action((opts: { watch?: number }, cmd: Command) => withClient(cmd, async (client, g) => {
      if (opts.watch === undefined) {
        const r = await client.health();
        emit(g.json, r.body, () => describe(r));
        if (r.status !== 200) process.exitCode = 1;
        return;
      }
      for (;;) {
        const at = new Date().toISOString();
        try {
          const r = await client.health();
          out(g.json ? JSON.stringify({ at, instance: r.instance ?? null, ...r.body }) : `${at}  ${describe(r)}`);
        } catch (err) {
          // Keep polling through outages: watching a replica go down and come back is the point.
          const cause = (err as { code?: string }).code ?? (err as Error).message;
          out(g.json ? JSON.stringify({ at, status: 'unreachable', cause }) : `${at}  unreachable (${cause})`);
        }
        await new Promise((resolve) => setTimeout(resolve, opts.watch));
      }
    }));
}
```

`apps/cli/src/commands/products.ts`:

```ts
import { Option, type Command } from 'commander';
import type { ListProductsQuery, Product } from '../api-types';
import { UsageError } from '../errors';
import { emit, formatFields, formatTable } from '../output';
import { intArg, withClient } from './common';

const HEADER = ['id', 'sku', 'name', 'category', 'base', 'effective', 'promotion', 'stock'];
const row = (p: Product) => [p.id, p.sku, p.name, p.category.slug, p.basePrice, p.effectivePrice, p.activePromotion?.name ?? '-', p.stock];

const fields = (p: Product) => formatFields([
  ['id', p.id],
  ['sku', p.sku],
  ['name', p.name],
  ['category', `${p.category.slug} (id ${p.category.id})`],
  ['basePrice', p.basePrice],
  ['effectivePrice', p.effectivePrice],
  ['activePromotion', p.activePromotion ? `${p.activePromotion.name} (${p.activePromotion.discountType} ${p.activePromotion.value}, id ${p.activePromotion.id})` : null],
  ['stock', p.stock],
]);

export function registerProducts(program: Command): void {
  const products = program.command('products').description('List, read and create products, and change stock');

  products.command('list')
    .description('GET /products')
    .option('--category <slug>', 'category slug, e.g. accessories')
    .addOption(new Option('--sort <sort>', 'sort order (use --sort=-effective_price for descending)').choices(['effective_price', '-effective_price']))
    .option('--page <n>', 'page number (max 1000)', intArg('page'))
    .option('--page-size <n>', 'items per page (max 100)', intArg('page-size'))
    .action((opts: ListProductsQuery, cmd: Command) => withClient(cmd, async (client, g) => {
      const page = await client.listProducts(opts);
      emit(g.json, page, () => `${formatTable([HEADER, ...page.items.map(row)])}\npage ${page.pagination.page}, ${page.items.length} of ${page.pagination.total} products`);
    }));

  products.command('get')
    .description('GET /products/:id')
    .argument('<id>', 'product id', intArg('id'))
    .action((id: number, _opts: unknown, cmd: Command) => withClient(cmd, async (client, g) => {
      const p = await client.getProduct(id);
      emit(g.json, p, () => fields(p));
    }));

  products.command('create')
    .description('POST /products (inherits any active category promotion immediately)')
    .requiredOption('--sku <sku>', 'unique SKU')
    .requiredOption('--name <name>', 'display name')
    .requiredOption('--category-id <id>', 'category id', intArg('category-id'))
    .requiredOption('--base-price <price>', 'decimal string, e.g. 19.99')
    .option('--stock <n>', 'initial stock', intArg('stock', 0), 0)
    .action((opts: { sku: string; name: string; categoryId: number; basePrice: string; stock: number }, cmd: Command) => withClient(cmd, async (client, g) => {
      const p = await client.createProduct(opts);
      emit(g.json, p, () => fields(p));
    }));

  products.command('stock')
    .description('PATCH /products/:id/stock: set with --set, or adjust with --delta')
    .argument('<id>', 'product id', intArg('id'))
    .option('--set <n>', 'set the stock to n', intArg('set', 0))
    .option('--delta <n>', 'add n (negative removes)', intArg('delta', -2_147_483_648))
    .action((id: number, opts: { set?: number; delta?: number }, cmd: Command) => withClient(cmd, async (client, g) => {
      if ((opts.set === undefined) === (opts.delta === undefined)) throw new UsageError('pass exactly one of --set or --delta');
      const r = await client.setStock(id, opts.set !== undefined ? { stock: opts.set } : { delta: opts.delta! });
      emit(g.json, r, () => `product ${r.id}: stock ${r.stock}`);
    }));
}
```

`apps/cli/src/commands/promotions.ts`:

```ts
import { Option, type Command } from 'commander';
import type { Promotion, Target } from '../api-types';
import { UsageError } from '../errors';
import { emit, formatFields } from '../output';
import { intArg, withClient } from './common';

interface TargetFlags { product?: number; category?: number }

function targetOf(opts: TargetFlags): Target {
  if ((opts.product === undefined) === (opts.category === undefined)) throw new UsageError('pass exactly one of --product or --category');
  return opts.product !== undefined ? { productId: opts.product } : { categoryId: opts.category! };
}

const fields = (p: Promotion) => formatFields([
  ['id', p.id],
  ['name', p.name],
  ['discount', `${p.discountType} ${p.value}`],
  ['startsAt', p.startsAt],
  ['endsAt', p.endsAt],
  ['target', 'productId' in p.target ? `product ${p.target.productId}` : `category ${p.target.categoryId}`],
  ['cancelledAt', p.cancelledAt],
  ['createdAt', p.createdAt],
]);

export function registerPromotions(program: Command): void {
  const promotions = program.command('promotions').description('Create, read, cancel and retarget promotions');

  promotions.command('create')
    .description('POST /promotions')
    .requiredOption('--name <name>', 'promotion name')
    .addOption(new Option('--type <type>', 'discount type').choices(['percentage', 'fixed']).makeOptionMandatory())
    .requiredOption('--value <value>', 'percent (e.g. 50) or amount (e.g. 5.00), as a decimal string')
    .option('--starts <iso>', 'start time, ISO 8601 (default: now minus 1 s)')
    .option('--ends <iso>', 'end time, ISO 8601 (default: now plus 1 h)')
    .option('--product <id>', 'target one product', intArg('product'))
    .option('--category <id>', 'target a whole category', intArg('category'))
    .action((opts: TargetFlags & { name: string; type: 'percentage' | 'fixed'; value: string; starts?: string; ends?: string }, cmd: Command) => withClient(cmd, async (client, g) => {
      const target = targetOf(opts);
      const now = Date.now();
      const p = await client.createPromotion({
        name: opts.name, discountType: opts.type, value: opts.value,
        startsAt: opts.starts ?? new Date(now - 1000).toISOString(),
        endsAt: opts.ends ?? new Date(now + 3_600_000).toISOString(),
        target,
      });
      emit(g.json, p, () => fields(p));
    }));

  promotions.command('get')
    .description('GET /promotions/:id')
    .argument('<id>', 'promotion id (uuid)')
    .action((id: string, _opts: unknown, cmd: Command) => withClient(cmd, async (client, g) => {
      const p = await client.getPromotion(id);
      emit(g.json, p, () => fields(p));
    }));

  promotions.command('cancel')
    .description('POST /promotions/:id/cancel (idempotent)')
    .argument('<id>', 'promotion id (uuid)')
    .action((id: string, _opts: unknown, cmd: Command) => withClient(cmd, async (client, g) => {
      const p = await client.cancelPromotion(id);
      emit(g.json, p, () => fields(p));
    }));

  promotions.command('target')
    .description('PUT /promotions/:id/target: move the promotion to another product or category')
    .argument('<id>', 'promotion id (uuid)')
    .option('--product <id>', 'new product target', intArg('product'))
    .option('--category <id>', 'new category target', intArg('category'))
    .action((id: string, opts: TargetFlags, cmd: Command) => withClient(cmd, async (client, g) => {
      const p = await client.retargetPromotion(id, targetOf(opts));
      emit(g.json, p, () => fields(p));
    }));
}
```

`apps/cli/src/commands/ingest.ts`:

```ts
import { readFileSync, statSync } from 'node:fs';
import { basename } from 'node:path';
import type { Command } from 'commander';
import type { IngestionJob } from '../api-types';
import { emit, formatFields, formatTable, log } from '../output';
import { durationArg, intArg, withClient } from './common';

const jobFields = (j: IngestionJob) => formatFields([
  ['id', j.id], ['status', j.status], ['s3Key', j.s3Key],
  ['chunks', `${j.completedChunks}/${j.totalChunks} completed, ${j.failedChunks} failed`],
  ['rowsProcessed', j.rowsProcessed], ['rowsRejected', j.rowsRejected], ['error', j.error],
  ['createdAt', j.createdAt], ['updatedAt', j.updatedAt],
]);

export function registerIngest(program: Command): void {
  const ingest = program.command('ingest').description('Vendor file ingestion: upload a CSV, follow a job, list rejected rows');

  ingest.command('upload')
    .description('Create a job, PUT the file to the presigned URL, and poll until completed or failed')
    .argument('<file>', 'vendor CSV (header sku,name,category,vendor_price,stock)')
    .option('--poll <interval>', 'status poll interval', durationArg, 1000)
    .action((file: string, opts: { poll: number }, cmd: Command) => withClient(cmd, async (client, g) => {
      const size = statSync(file).size;
      const started = Date.now();
      const elapsed = () => (Date.now() - started) / 1000;
      // The API accepts [A-Za-z0-9._-]{1,128} as a filename.
      const job = await client.createIngestionJob(basename(file).replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 128) || 'vendor.csv');
      log(`job ${job.jobId}: uploading ${file} (${(size / 1_048_576).toFixed(1)} MB)`);
      const put = await fetch(job.uploadUrl, { method: 'PUT', body: readFileSync(file) });
      if (!put.ok) throw new Error(`upload failed: HTTP ${put.status} ${await put.text()}`);
      log(`uploaded in ${elapsed().toFixed(1)}s; waiting for the splitter and workers`);
      for (;;) {
        const j = await client.getIngestionJob(job.jobId);
        log(`[${elapsed().toFixed(1)}s] ${j.status} chunks ${j.completedChunks}/${j.totalChunks} rows ${j.rowsProcessed} rejected ${j.rowsRejected}`);
        if (j.status === 'completed' || j.status === 'failed') {
          const rows = j.rowsProcessed + j.rowsRejected;
          const secs = elapsed();
          emit(g.json, j, () => `${j.status}: ${rows} rows in ${secs.toFixed(1)}s (${Math.round(rows / secs)} rows/s), ${j.rowsRejected} rejected${j.error ? `\nerror: ${j.error}` : ''}`);
          if (j.status === 'failed') process.exitCode = 1;
          return;
        }
        await new Promise((resolve) => setTimeout(resolve, opts.poll));
      }
    }));

  ingest.command('status')
    .description('GET /ingestion/jobs/:id')
    .argument('<jobId>', 'job id (uuid)')
    .action((jobId: string, _opts: unknown, cmd: Command) => withClient(cmd, async (client, g) => {
      const j = await client.getIngestionJob(jobId);
      emit(g.json, j, () => jobFields(j));
    }));

  ingest.command('rejections')
    .description('GET /ingestion/jobs/:id/rejections')
    .argument('<jobId>', 'job id (uuid)')
    .option('--page <n>', 'page number', intArg('page'))
    .option('--page-size <n>', 'items per page (max 100)', intArg('page-size'))
    .action((jobId: string, opts: { page?: number; pageSize?: number }, cmd: Command) => withClient(cmd, async (client, g) => {
      const page = await client.listRejections(jobId, opts);
      emit(g.json, page, () => `${formatTable([
        ['chunk', 'line', 'reason', 'raw'],
        ...page.items.map((r) => [r.chunkIndex, r.lineNumber, r.reason, r.rawLine.length > 60 ? `${r.rawLine.slice(0, 57)}...` : r.rawLine]),
      ])}\npage ${page.pagination.page}, ${page.items.length} of ${page.pagination.total} rejections`);
    }));
}
```

`apps/cli/src/main.ts`:

```ts
import { Command, CommanderError } from 'commander';
import { registerHealth } from './commands/health';
import { registerIngest } from './commands/ingest';
import { registerProducts } from './commands/products';
import { registerPromotions } from './commands/promotions';
import { ApiError, SetupError, UsageError } from './errors';
import { log, out } from './output';

const program = new Command('modaco')
  .description('Operate and load test the ModaCo API')
  .option('--url <url>', 'API base URL (env API_URL)', process.env.API_URL || 'http://localhost:3000')
  .option('--json', 'print one JSON document to stdout')
  .option('--timeout <duration>', 'per-request timeout', '10s')
  .exitOverride() // before adding commands, so subcommands inherit it
  .showHelpAfterError();

registerHealth(program);
registerProducts(program);
registerPromotions(program);
registerIngest(program);

/** Maps an error to an exit code: 2 for usage errors, 1 for API, setup and transport failures. */
function report(err: unknown, opts: { url: string; json?: boolean }): number {
  if (err instanceof CommanderError) return err.exitCode === 0 ? 0 : 2; // commander has already printed it
  if (err instanceof UsageError) { log(`error: ${err.message}`); return 2; }
  if (err instanceof ApiError) {
    if (opts.json) out(JSON.stringify(err.toJSON(), null, 2));
    else log(`error: HTTP ${err.status} ${err.code}: ${err.message}${err.details === undefined ? '' : `\n${JSON.stringify(err.details, null, 2)}`}`);
    return 1;
  }
  if (err instanceof SetupError) { log(`error: ${err.message}`); return 1; }
  const code = (err as { code?: unknown } | null)?.code;
  const message = err instanceof Error ? err.message : String(err);
  log(`error: ${typeof code === 'string' ? `${code} ` : ''}${message} (target ${opts.url})`);
  return 1;
}

try {
  await program.parseAsync(process.argv);
} catch (err) {
  process.exitCode = report(err, program.opts<{ url: string; json?: boolean }>());
}
```

Note: `process.exitCode` is set instead of calling `process.exit()`. On macOS, stdout to a pipe is asynchronous, and `process.exit()` can cut off large JSON output.

- [ ] **Step 4: Run the test and the typecheck**

Run: `pnpm --filter @modaco/cli exec vitest run test/cli.test.ts && pnpm --filter @modaco/cli typecheck`
Expected: PASS, no type errors. Then from the repo root run `pnpm modaco --help` and `pnpm modaco products --help`. Expected: help text listing health, products, promotions, ingest.

- [ ] **Step 5: Commit**

```bash
git add apps/cli/src/main.ts apps/cli/src/commands apps/cli/test/cli.test.ts
git commit -m "feat(cli): health, products, promotions and ingest commands

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Load orchestration, report, and the `browse` scenario

**Files:**
- Create: `apps/cli/src/load/report.ts`, `apps/cli/src/load/run.ts`
- Create: `apps/cli/src/load/scenarios/types.ts`, `apps/cli/src/load/scenarios/catalog.ts`, `apps/cli/src/load/scenarios/browse.ts`, `apps/cli/src/load/scenarios/index.ts`
- Test: `apps/cli/test/scenarios.test.ts`, `apps/cli/test/report.test.ts`

**Interfaces:**
- Consumes: `runPhase`, `LoadModel`, `LoadSource`, `RequestSpec`, `ProgressSample` (Task 4); `Metrics`, `StatsSummary` (Task 3); `createRng`, `Rng`, `pickWeighted` (Task 2); `ApiClient` (Task 5); `SetupError`, `UsageError` (Task 2); `formatTable` (Task 5).
- Produces:
  - `report.ts`: `interface Check { name: string; ok: boolean; message: string }`, `interface PhaseResult { name: string; elapsedSeconds: number; total: StatsSummary; byLabel: Record<string, StatsSummary> }`, `interface ResultDocument { scenario; target; startedAt; options: Record<string, unknown>; phases: PhaseResult[]; instances: Record<string, number>; checks: Check[]; notes: Record<string, string>; interrupted: boolean; ok: boolean }`, `formatProgress(phase: string, s: ProgressSample): string`, `formatReport(doc: ResultDocument): string`
  - `scenarios/types.ts`: `interface ScenarioOptions { category?: string; maxPage: number; mix?: Record<string, number> }`, `interface PhaseRunner { readonly client: ApiClient; readonly durationMs: number; log(msg: string): void; warmup(source?: LoadSource): Promise<void>; phase(name: string, durationMs: number, source?: LoadSource): Promise<void>; addCheck(check: Check): void; note(key: string, value: string): void }`, `interface Scenario extends LoadSource { readonly name: string; setup(client: ApiClient, log: (msg: string) => void): Promise<void>; run?(runner: PhaseRunner): Promise<void>; cleanup?(client: ApiClient): Promise<void> }`
  - `scenarios/catalog.ts`: `interface CatalogSample { total: number; ids: number[]; slugs: string[] }`, `sampleCatalog(client, opts?: { category?: string; pages?: number }): Promise<CatalogSample>`, `listRequest(rng, sample, opts: { category?: string; maxPage: number }): RequestSpec`, `detailRequest(rng, sample): RequestSpec`
  - `scenarios/index.ts`: `SCENARIOS: Record<string, { labels: readonly string[]; create(opts: ScenarioOptions): Scenario }>`, `createScenario(name: string, opts: ScenarioOptions): Scenario`
  - `run.ts`: `interface RunOptions { model: LoadModel; durationMs: number; warmupMs: number; reportEveryMs: number; seed: number; options: Record<string, unknown> }`, `interface RunIo { log(msg: string): void; signal?: AbortSignal }`, `runLoad(client: ApiClient, scenario: Scenario, o: RunOptions, io: RunIo): Promise<ResultDocument>`

- [ ] **Step 1: Write the failing tests**

`apps/cli/test/scenarios.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ApiClient } from '../src/client';
import { SetupError } from '../src/errors';
import { runLoad, type RunOptions } from '../src/load/run';
import { createScenario } from '../src/load/scenarios';
import { startStubApi, type StubApi } from './stub-api';

let stub: StubApi;
let client: ApiClient;
const quiet = { log: () => {} };
const opts = (over: Partial<RunOptions> = {}): RunOptions => ({
  model: { kind: 'closed', concurrency: 4 }, durationMs: 400, warmupMs: 0, reportEveryMs: 0, seed: 7, options: {}, ...over,
});

beforeEach(async () => {
  stub = await startStubApi();
  client = new ApiClient({ baseUrl: stub.url, timeoutMs: 5000, connections: 8 });
});
afterEach(async () => {
  await client.close();
  await stub.close();
});

describe('browse', () => {
  it('samples the catalog, mixes list and detail, and tallies instances', async () => {
    const doc = await runLoad(client, createScenario('browse', { maxPage: 5 }), opts({ warmupMs: 100 }), quiet);
    expect(stub.state.samplePages).toBe(10); // 1000 products / 100 per page
    expect(doc.phases.map((p) => p.name)).toEqual(['main']);
    const main = doc.phases[0]!;
    expect(Object.keys(main.byLabel)).toEqual(['detail', 'list']);
    const listShare = main.byLabel.list!.count / main.total.count;
    expect(listShare).toBeGreaterThan(0.6);
    expect(listShare).toBeLessThan(0.8);
    // Warmup is not recorded: every recorded response is attributed to exactly one instance.
    expect(Object.values(doc.instances).reduce((a, b) => a + b, 0)).toBe(main.total.count);
    expect(Object.keys(doc.instances)).toEqual(['i1', 'i2', 'i3']);
    expect(doc).toMatchObject({ scenario: 'browse', target: stub.url, ok: true, interrupted: false, checks: [] });
  });

  it('honours --mix and --category', async () => {
    const doc = await runLoad(client, createScenario('browse', { maxPage: 2, mix: { list: 1 }, category: 'shoes' }), opts(), quiet);
    expect(Object.keys(doc.phases[0]!.byLabel)).toEqual(['list']);
  });

  it('fails setup on an empty catalog', async () => {
    const empty = await startStubApi({ total: 0 });
    const c = new ApiClient({ baseUrl: empty.url, timeoutMs: 5000 });
    try {
      await expect(runLoad(c, createScenario('browse', { maxPage: 5 }), opts(), quiet)).rejects.toThrow(SetupError);
    } finally {
      await c.close();
      await empty.close();
    }
  });

  it('marks the run interrupted when the signal aborts', async () => {
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 100);
    const doc = await runLoad(client, createScenario('browse', { maxPage: 5 }), opts({ durationMs: 10_000 }), { ...quiet, signal: ac.signal });
    expect(doc.interrupted).toBe(true);
    expect(doc.phases[0]!.elapsedSeconds).toBeLessThan(2);
  });
});

describe('createScenario', () => {
  it('rejects unknown scenarios', () => {
    expect(() => createScenario('nope', { maxPage: 5 })).toThrow(/unknown scenario 'nope'/);
  });
});
```

`apps/cli/test/report.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { formatProgress, formatReport, type ResultDocument } from '../src/load/report';

const stats = (count: number) => ({
  count, rps: count / 10, latencyMs: { mean: 2, p50: 1.5, p90: 3, p95: 4, p99: 8, p999: 12, max: 20 },
  status: { '200': count }, errors: {}, dropped: 0,
});

describe('formatReport', () => {
  it('renders phases, instance shares, checks and notes', () => {
    const doc: ResultDocument = {
      scenario: 'flash-sale', target: 'http://localhost:8080', startedAt: '2026-09-25T10:00:00.000Z',
      options: { model: 'closed', concurrency: 50, mix: { list: 1 } },
      phases: [{ name: 'before', elapsedSeconds: 10, total: { ...stats(300), dropped: 5, errors: { timeout: 2 } }, byLabel: { list: stats(300) } }],
      instances: { a: 100, b: 200 },
      checks: [{ name: 'mid-sale product discounted', ok: false, message: 'expected 10.00' }],
      notes: { promotionId: 'abc' },
      interrupted: false, ok: false,
    };
    const text = formatReport(doc);
    expect(text).toContain('flash-sale -> http://localhost:8080');
    expect(text).toContain('mix={"list":1}');
    expect(text).toContain('phase before (10s)');
    expect(text).toContain('errors: timeout:2');
    expect(text).toContain('5 requests dropped');
    expect(text).toMatch(/b\s+200\s+66\.7%/);
    expect(text).toContain('FAIL mid-sale product discounted: expected 10.00');
    expect(text).toMatch(/promotionId\s+abc/);
  });
});

describe('formatProgress', () => {
  it('prints one compact line', () => {
    expect(formatProgress('main', { elapsedSeconds: 5, rps: 1999.6, p50Ms: 3.2, p99Ms: 15, errors: 0, dropped: 0, inflight: 12 }))
      .toBe('[main 5.0s] 2000 req/s  p50 3.2ms  p99 15ms  errors 0  dropped 0  in-flight 12');
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pnpm --filter @modaco/cli exec vitest run test/scenarios.test.ts test/report.test.ts`
Expected: FAIL, cannot resolve `../src/load/run`, `../src/load/scenarios` and `../src/load/report`.

- [ ] **Step 3: Implement**

`apps/cli/src/load/report.ts`:

```ts
import { formatTable } from '../output';
import type { ProgressSample } from './engine';
import type { StatsSummary } from './metrics';

export interface Check { name: string; ok: boolean; message: string }

export interface PhaseResult { name: string; elapsedSeconds: number; total: StatsSummary; byLabel: Record<string, StatsSummary> }

/** What `load --json` prints and `--out` writes. */
export interface ResultDocument {
  scenario: string;
  target: string;
  startedAt: string;
  options: Record<string, unknown>;
  phases: PhaseResult[];
  /** Responses per X-Instance-Id over all recorded phases. */
  instances: Record<string, number>;
  checks: Check[];
  notes: Record<string, string>;
  interrupted: boolean;
  /** False when any check failed. */
  ok: boolean;
}

export function formatProgress(phase: string, s: ProgressSample): string {
  return `[${phase} ${s.elapsedSeconds.toFixed(1)}s] ${s.rps.toFixed(0)} req/s  p50 ${s.p50Ms}ms  p99 ${s.p99Ms}ms  errors ${s.errors}  dropped ${s.dropped}  in-flight ${s.inflight}`;
}

const HEADER = ['label', 'count', 'req/s', 'p50', 'p90', 'p95', 'p99', 'p99.9', 'max', 'errors', 'dropped', 'status'];
const errorCount = (s: StatsSummary) => Object.values(s.errors).reduce((sum, n) => sum + (n ?? 0), 0);
const pairs = (o: Record<string, unknown>) => Object.entries(o).map(([k, v]) => `${k}:${String(v)}`).join(' ');
const statsRow = (label: string, s: StatsSummary) => [
  label, s.count, s.rps, s.latencyMs.p50, s.latencyMs.p90, s.latencyMs.p95, s.latencyMs.p99, s.latencyMs.p999, s.latencyMs.max,
  errorCount(s), s.dropped, pairs(s.status) || '-',
];
const indent = (text: string) => text.split('\n').map((line) => `  ${line}`).join('\n');
const describeOptions = (o: Record<string, unknown>) =>
  Object.entries(o).map(([k, v]) => `${k}=${typeof v === 'object' && v !== null ? JSON.stringify(v) : String(v)}`).join(' ');

export function formatReport(doc: ResultDocument): string {
  const lines = [`${doc.scenario} -> ${doc.target}  ${describeOptions(doc.options)}${doc.interrupted ? '  (INTERRUPTED)' : ''}`];
  for (const p of doc.phases) {
    lines.push('', `phase ${p.name} (${p.elapsedSeconds}s), latency in ms`);
    lines.push(indent(formatTable([HEADER, statsRow('total', p.total), ...Object.entries(p.byLabel).map(([label, s]) => statsRow(label, s))])));
    const errors = pairs(p.total.errors);
    if (errors) lines.push(`  errors: ${errors}`);
    if (p.total.dropped > 0) {
      lines.push(`  warning: ${p.total.dropped} requests dropped at --max-inflight; the target or this client could not keep up with the rate`);
    }
  }
  const served = Object.values(doc.instances).reduce((a, b) => a + b, 0);
  if (served > 0) {
    lines.push('', `instances (${Object.keys(doc.instances).length})`);
    lines.push(indent(formatTable(Object.entries(doc.instances).map(([id, n]) => [id, n, `${((n / served) * 100).toFixed(1)}%`]))));
  }
  if (doc.checks.length > 0) {
    lines.push('', 'checks');
    for (const c of doc.checks) lines.push(`  ${c.ok ? 'PASS' : 'FAIL'} ${c.name}: ${c.message}`);
  }
  const notes = Object.entries(doc.notes);
  if (notes.length > 0) lines.push('', 'notes', indent(formatTable(notes)));
  return lines.join('\n');
}
```

`apps/cli/src/load/scenarios/types.ts`:

```ts
import type { ApiClient } from '../../client';
import type { LoadSource } from '../engine';
import type { Check } from '../report';

export interface ScenarioOptions {
  /** Restrict requests to one category slug. */
  category?: string;
  /** Highest listing page requested. */
  maxPage: number;
  /** Relative weights by request label; the scenario's default mix when absent. */
  mix?: Record<string, number>;
}

/** What runLoad hands a scenario that orchestrates its own phases (flash-sale). */
export interface PhaseRunner {
  readonly client: ApiClient;
  /** The --duration budget for all recorded phases together. */
  readonly durationMs: number;
  log(msg: string): void;
  /** Runs --warmup of unrecorded load. */
  warmup(source?: LoadSource): Promise<void>;
  /** Runs and records one phase. */
  phase(name: string, durationMs: number, source?: LoadSource): Promise<void>;
  addCheck(check: Check): void;
  note(key: string, value: string): void;
}

export interface Scenario extends LoadSource {
  readonly name: string;
  /** Discovers ids and categories before load starts. Not measured. */
  setup(client: ApiClient, log: (msg: string) => void): Promise<void>;
  /** Custom phases. Default: warmup, then one recorded phase named "main". */
  run?(runner: PhaseRunner): Promise<void>;
  /** Undoes the scenario's writes. Runs even when the run fails or is interrupted. */
  cleanup?(client: ApiClient): Promise<void>;
}
```

`apps/cli/src/load/scenarios/catalog.ts`:

```ts
import type { Product } from '../../api-types';
import type { ApiClient } from '../../client';
import { SetupError } from '../../errors';
import type { RequestSpec } from '../engine';
import type { Rng } from '../rng';

export interface CatalogSample { total: number; ids: number[]; slugs: string[] }

const SAMPLE_PAGE_SIZE = 100;
const API_MAX_PAGE = 1000;

/**
 * Reads up to `pages` listing pages spread evenly across the catalog and returns the product ids and category slugs
 * it saw. The API has no categories endpoint, and ids are not contiguous after ingests, so sampling is how a
 * scenario finds real targets.
 */
export async function sampleCatalog(client: ApiClient, opts: { category?: string; pages?: number } = {}): Promise<CatalogSample> {
  const first = await client.listProducts({ category: opts.category, page: 1, pageSize: SAMPLE_PAGE_SIZE });
  const total = first.pagination.total;
  if (total === 0) {
    throw new SetupError(`no products${opts.category ? ` in category '${opts.category}'` : ''}; run pnpm seed or pnpm modaco ingest upload <file> first`);
  }
  const pageCount = Math.min(Math.ceil(total / SAMPLE_PAGE_SIZE), API_MAX_PAGE);
  const n = Math.min(opts.pages ?? 20, pageCount);
  const ids = new Set<number>();
  const slugs = new Set<string>();
  const add = (items: Product[]) => { for (const p of items) { ids.add(p.id); slugs.add(p.category.slug); } };
  add(first.items);
  for (let i = 1; i < n; i++) {
    const page = 1 + Math.floor((i * pageCount) / n);
    add((await client.listProducts({ category: opts.category, page, pageSize: SAMPLE_PAGE_SIZE })).items);
  }
  return { total, ids: [...ids], slugs: [...slugs] };
}

export function listRequest(rng: Rng, sample: CatalogSample, opts: { category?: string; maxPage: number }): RequestSpec {
  const category = opts.category ?? rng.pick(sample.slugs);
  const sort = rng.next() < 0.5 ? 'effective_price' : '-effective_price';
  return { label: 'list', method: 'GET', path: `/products?category=${encodeURIComponent(category)}&sort=${sort}&page=${rng.int(1, opts.maxPage)}&pageSize=20` };
}

export function detailRequest(rng: Rng, sample: CatalogSample): RequestSpec {
  return { label: 'detail', method: 'GET', path: `/products/${rng.pick(sample.ids)}` };
}
```

`apps/cli/src/load/scenarios/browse.ts`:

```ts
import { pickWeighted } from '../rng';
import { detailRequest, listRequest, sampleCatalog, type CatalogSample } from './catalog';
import type { Scenario, ScenarioOptions } from './types';

export const BROWSE_LABELS = ['list', 'detail'] as const;
const DEFAULT_MIX = { list: 70, detail: 30 };

/** Read-only traffic: category listings with random page and sort, and product details by sampled id. */
export function createBrowse(opts: ScenarioOptions): Scenario {
  const mix = opts.mix ?? DEFAULT_MIX;
  let sample: CatalogSample = { total: 0, ids: [], slugs: [] };
  return {
    name: 'browse',
    async setup(client, log) {
      sample = await sampleCatalog(client, { category: opts.category });
      log(`sampled ${sample.ids.length} product ids across ${sample.slugs.length} categories (catalog total ${sample.total})`);
    },
    next(rng) {
      return pickWeighted(rng, mix) === 'list' ? listRequest(rng, sample, opts) : detailRequest(rng, sample);
    },
  };
}
```

`apps/cli/src/load/scenarios/index.ts`:

```ts
import { UsageError } from '../../errors';
import { BROWSE_LABELS, createBrowse } from './browse';
import type { Scenario, ScenarioOptions } from './types';

interface ScenarioDef {
  /** Labels --mix may weight; empty when the scenario takes no mix. */
  labels: readonly string[];
  create(opts: ScenarioOptions): Scenario;
}

export const SCENARIOS: Record<string, ScenarioDef> = {
  browse: { labels: BROWSE_LABELS, create: createBrowse },
};

export function createScenario(name: string, opts: ScenarioOptions): Scenario {
  const def = SCENARIOS[name];
  if (!def) throw new UsageError(`unknown scenario '${name}' (available: ${Object.keys(SCENARIOS).join(', ')})`);
  return def.create(opts);
}
```

`apps/cli/src/load/run.ts`:

```ts
import type { ApiClient } from '../client';
import { runPhase, type LoadModel, type LoadSource } from './engine';
import { Metrics } from './metrics';
import { formatProgress, type Check, type PhaseResult, type ResultDocument } from './report';
import { createRng } from './rng';
import type { PhaseRunner, Scenario } from './scenarios/types';

export interface RunOptions {
  model: LoadModel;
  durationMs: number;
  warmupMs: number;
  reportEveryMs: number;
  seed: number;
  /** Echoed into the result document. */
  options: Record<string, unknown>;
}

export interface RunIo { log(msg: string): void; signal?: AbortSignal }

/** Runs setup, the scenario's phases (default: warmup + "main") and cleanup, and assembles the result document. */
export async function runLoad(client: ApiClient, scenario: Scenario, o: RunOptions, io: RunIo): Promise<ResultDocument> {
  const rng = createRng(o.seed);
  const startedAt = new Date().toISOString();
  const phases: PhaseResult[] = [];
  const instances: Record<string, number> = {};
  const checks: Check[] = [];
  const notes: Record<string, string> = {};
  let interrupted = false;
  let rampPending = true;

  // The ramp belongs to the first recorded phase only; warmup and later phases run at the full rate.
  const modelFor = (recorded: boolean): LoadModel => {
    if (o.model.kind !== 'open') return o.model;
    const ramp = recorded && rampPending;
    if (recorded) rampPending = false;
    return ramp ? o.model : { ...o.model, rampMs: 0 };
  };

  const runner: PhaseRunner = {
    client,
    durationMs: o.durationMs,
    log: io.log,
    async warmup(source: LoadSource = scenario) {
      if (o.warmupMs <= 0 || io.signal?.aborted) return;
      io.log(`warmup ${o.warmupMs / 1000}s (not recorded)`);
      const r = await runPhase(client, source, rng, null, { model: modelFor(false), durationMs: o.warmupMs, signal: io.signal });
      if (r.interrupted) interrupted = true;
    },
    async phase(name: string, durationMs: number, source: LoadSource = scenario) {
      if (io.signal?.aborted) { interrupted = true; return; }
      io.log(`phase ${name}: ${durationMs / 1000}s`);
      const metrics = new Metrics();
      const r = await runPhase(client, source, rng, metrics, {
        model: modelFor(true), durationMs, signal: io.signal, reportEveryMs: o.reportEveryMs,
        onProgress: (s) => io.log(formatProgress(name, s)),
      });
      if (r.interrupted) interrupted = true;
      phases.push({ name, elapsedSeconds: Math.round(r.elapsedSeconds * 100) / 100, ...metrics.summary(r.elapsedSeconds) });
      for (const [id, n] of Object.entries(metrics.instanceCounts())) instances[id] = (instances[id] ?? 0) + n;
    },
    addCheck: (check) => { checks.push(check); },
    note: (key, value) => { notes[key] = value; },
  };

  await scenario.setup(client, io.log);
  try {
    if (scenario.run) {
      await scenario.run(runner);
    } else {
      await runner.warmup();
      await runner.phase('main', o.durationMs);
    }
  } finally {
    if (scenario.cleanup) await scenario.cleanup(client).catch((err: unknown) => io.log(`warning: cleanup failed: ${String(err)}`));
  }

  return {
    scenario: scenario.name, target: client.baseUrl, startedAt, options: o.options,
    phases, instances, checks, notes, interrupted, ok: checks.every((c) => c.ok),
  };
}
```

- [ ] **Step 4: Run the tests and the typecheck**

Run: `pnpm --filter @modaco/cli exec vitest run test/scenarios.test.ts test/report.test.ts && pnpm --filter @modaco/cli typecheck`
Expected: PASS, no type errors.

- [ ] **Step 5: Commit**

```bash
git add apps/cli/src/load/report.ts apps/cli/src/load/run.ts apps/cli/src/load/scenarios apps/cli/test/scenarios.test.ts apps/cli/test/report.test.ts
git commit -m "feat(cli): load orchestration, result report and the browse scenario

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: `write-mix` and `flash-sale` scenarios

**Files:**
- Create: `apps/cli/src/load/scenarios/write-mix.ts`, `apps/cli/src/load/scenarios/flash-sale.ts`
- Modify: `apps/cli/src/load/scenarios/index.ts`
- Test: `apps/cli/test/scenarios.test.ts` (append)

**Interfaces:**
- Consumes: `sampleCatalog`, `listRequest`, `detailRequest`, `CatalogSample` (Task 7); `Scenario`, `ScenarioOptions`, `PhaseRunner` (Task 7); `Check` (Task 7); `ApiClient` (Task 5); `ApiError`, `SetupError` (Task 2); `pickWeighted`, `Rng` (Task 2); `Product` (Task 5).
- Produces: `WRITE_MIX_LABELS`, `createWriteMix(opts: ScenarioOptions): Scenario`, `createFlashSale(opts: ScenarioOptions): Scenario`; `SCENARIOS` gains `write-mix` (labels `list, detail, stock, promo`) and `flash-sale` (labels `[]`).

- [ ] **Step 1: Append the failing tests**

Add these `describe` blocks at the end of `apps/cli/test/scenarios.test.ts`. The existing imports already cover `ApiClient`, `runLoad`, `createScenario` and `startStubApi`:

```ts
describe('write-mix', () => {
  it('mixes reads with stock writes and cleans up its promotions', async () => {
    const doc = await runLoad(client, createScenario('write-mix', { maxPage: 5 }), opts({ durationMs: 600 }), quiet);
    const main = doc.phases[0]!;
    expect(Object.keys(main.byLabel)).toEqual(expect.arrayContaining(['detail', 'list', 'stock']));
    const stockShare = main.byLabel.stock!.count / main.total.count;
    expect(stockShare).toBeGreaterThan(0.08);
    expect(stockShare).toBeLessThan(0.2);
    expect(stub.state.hits.get('PATCH /products/:id/stock')).toBeGreaterThan(0);
    expect(stub.state.openPromotions.size).toBe(0);
  });

  it('alternates promotion create and cancel, one request per slot', async () => {
    const doc = await runLoad(client, createScenario('write-mix', { maxPage: 5, mix: { promo: 1 } }), opts({ model: { kind: 'closed', concurrency: 1 } }), quiet);
    const { byLabel } = doc.phases[0]!;
    expect(byLabel['promo:create']!.count).toBeGreaterThan(0);
    expect(byLabel['promo:cancel']!.count).toBeGreaterThan(0);
    expect(Math.abs(byLabel['promo:create']!.count - byLabel['promo:cancel']!.count)).toBeLessThanOrEqual(1);
    expect(stub.state.openPromotions.size).toBe(0);
  });
});

describe('flash-sale', () => {
  it('records before and after phases, passes the mid-sale check, and cancels the promotion', async () => {
    const doc = await runLoad(client, createScenario('flash-sale', { maxPage: 5, category: 'shoes' }), opts({ durationMs: 300 }), quiet);
    expect(doc.phases.map((p) => p.name)).toEqual(['before', 'after']);
    expect(doc.checks).toHaveLength(1);
    expect(doc.checks[0]).toMatchObject({ name: 'mid-sale product discounted', ok: true });
    expect(doc.notes.promotionId).toBe(stub.state.lastPromotionId);
    expect(doc.notes.firstItemPriceBefore).toBe('20.00');
    expect(doc.ok).toBe(true);
    expect(stub.state.openPromotions.size).toBe(0);
  });

  it('fails the check but still cancels when the new product is not discounted', async () => {
    const wrong = await startStubApi({ midSalePrice: '20.00' });
    const c = new ApiClient({ baseUrl: wrong.url, timeoutMs: 5000 });
    try {
      const doc = await runLoad(c, createScenario('flash-sale', { maxPage: 5 }), opts({ durationMs: 300 }), quiet);
      expect(doc.checks[0]).toMatchObject({ ok: false });
      expect(doc.checks[0]!.message).toContain('expected 10.00');
      expect(doc.ok).toBe(false);
      expect(wrong.state.openPromotions.size).toBe(0);
    } finally {
      await c.close();
      await wrong.close();
    }
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pnpm --filter @modaco/cli exec vitest run test/scenarios.test.ts`
Expected: the new tests FAIL with `unknown scenario 'write-mix'` / `unknown scenario 'flash-sale'`. The browse tests still pass.

- [ ] **Step 3: Implement**

`apps/cli/src/load/scenarios/write-mix.ts`:

```ts
import type { RequestSpec } from '../engine';
import { pickWeighted, type Rng } from '../rng';
import { detailRequest, listRequest, sampleCatalog, type CatalogSample } from './catalog';
import type { Scenario, ScenarioOptions } from './types';

export const WRITE_MIX_LABELS = ['list', 'detail', 'stock', 'promo'] as const;
const DEFAULT_MIX = { list: 60, detail: 25, stock: 14, promo: 1 };

/**
 * Reads with concurrent writes. Stock writes hit the live stock counters. Promotion create/cancel cycles bump the
 * cache version counters, which forces cache rebuilds while reads continue.
 */
export function createWriteMix(opts: ScenarioOptions): Scenario {
  const mix = opts.mix ?? DEFAULT_MIX;
  let sample: CatalogSample = { total: 0, ids: [], slugs: [] };
  /** Created, no cancel sent yet: the next promo slot cancels the oldest. */
  const open: string[] = [];
  /** Created, cancel not yet confirmed: cleanup cancels whatever is left. */
  const uncancelled = new Set<string>();

  const promo = (rng: Rng): RequestSpec => {
    const id = open.shift();
    if (id) {
      return {
        label: 'promo:cancel', method: 'POST', path: `/promotions/${id}/cancel`,
        onResponse: (status) => { if (status === 200) uncancelled.delete(id); },
      };
    }
    const now = Date.now();
    return {
      label: 'promo:create', method: 'POST', path: '/promotions',
      body: {
        name: 'write-mix load test', discountType: 'percentage', value: '10',
        startsAt: new Date(now - 1000).toISOString(), endsAt: new Date(now + 3_600_000).toISOString(),
        target: { productId: rng.pick(sample.ids) },
      },
      onResponse: (status, body) => {
        const created = (body as { id?: unknown } | undefined)?.id;
        if (status === 201 && typeof created === 'string') { open.push(created); uncancelled.add(created); }
      },
    };
  };

  return {
    name: 'write-mix',
    async setup(client, log) {
      sample = await sampleCatalog(client, { category: opts.category });
      log(`sampled ${sample.ids.length} product ids across ${sample.slugs.length} categories (catalog total ${sample.total})`);
    },
    next(rng) {
      switch (pickWeighted(rng, mix)) {
        case 'list': return listRequest(rng, sample, opts);
        case 'detail': return detailRequest(rng, sample);
        // An absolute set never fails. A −1 delta would return 422 at stock 0 and pollute the error stats.
        case 'stock': return { label: 'stock', method: 'PATCH', path: `/products/${rng.pick(sample.ids)}/stock`, body: { stock: rng.int(0, 100) } };
        default: return promo(rng);
      }
    },
    async cleanup(client) {
      for (const id of [...uncancelled]) {
        await client.cancelPromotion(id).catch(() => undefined);
        uncancelled.delete(id);
      }
    },
  };
}
```

`apps/cli/src/load/scenarios/flash-sale.ts`:

```ts
import type { Product } from '../../api-types';
import type { ApiClient } from '../../client';
import { ApiError, SetupError } from '../../errors';
import type { Check } from '../report';
import type { Scenario, ScenarioOptions } from './types';

const MID_SALE_BASE = '20.00';
const MID_SALE_EXPECTED = '10.00'; // 50% off

/**
 * Port of scripts/demo-flash-sale.ts onto the engine. It records a "before" phase, turns on a 50% category
 * promotion, and records an "after" phase while a product created mid-sale must read back discounted on its first
 * read. The promotion is always cancelled at the end.
 */
export function createFlashSale(opts: ScenarioOptions): Scenario {
  const slug = opts.category ?? 'accessories';
  let categoryId = 0;
  let firstItemPrice = '';

  return {
    name: 'flash-sale',
    async setup(client, log) {
      const first = await client.listProducts({ category: slug, pageSize: 1 });
      const item = first.items[0];
      if (!item) throw new SetupError(`no products in category '${slug}'; run pnpm seed or ingest a vendor file first`);
      categoryId = item.category.id;
      log(`category ${slug} (id ${categoryId}): ${first.pagination.total} products`);
      if (first.pagination.total < 50_000) log(`warning: flash-sale expects 50k+ products in '${slug}'; ingest tmp/vendor-500k.csv first`);
    },
    next(rng) {
      const page = rng.int(1, opts.maxPage);
      return {
        label: 'list', method: 'GET', path: `/products?category=${encodeURIComponent(slug)}&page=${page}&pageSize=20`,
        onResponse: page === 1
          ? (status, body) => {
              const price = (body as { items?: Product[] } | undefined)?.items?.[0]?.effectivePrice;
              if (status === 200 && price) firstItemPrice = price;
            }
          : undefined,
      };
    },
    async run(runner) {
      const { client } = runner;
      await runner.warmup();
      const beforeMs = Math.floor(runner.durationMs / 3);
      await runner.phase('before', beforeMs);
      runner.note('firstItemPriceBefore', firstItemPrice || '-');

      const now = Date.now();
      const promo = await client.createPromotion({
        name: 'Flash sale load test', discountType: 'percentage', value: '50',
        startsAt: new Date(now - 1000).toISOString(), endsAt: new Date(now + 3_600_000).toISOString(),
        target: { categoryId },
      });
      runner.note('promotionId', promo.id);
      runner.log(`flash sale created -> promotion ${promo.id}`);
      try {
        const [, check] = await Promise.all([
          runner.phase('after', runner.durationMs - beforeMs),
          verifyMidSaleProduct(client, categoryId, promo.id),
        ]);
        runner.addCheck(check);
        runner.note('firstItemPriceAfter', firstItemPrice || '-');
      } finally {
        await client.cancelPromotion(promo.id).catch((err: unknown) => runner.log(`warning: could not cancel promotion ${promo.id}: ${String(err)}`));
      }
    },
  };
}

/** Creates a product in the promoted category during the sale and requires it to read back at half price. */
async function verifyMidSaleProduct(client: ApiClient, categoryId: number, promotionId: string): Promise<Check> {
  const name = 'mid-sale product discounted';
  const sku = `MIDSALE-LOAD-${Date.now()}`;
  try {
    const created = await client.createProduct({ sku, name: 'Mid-sale load test product', categoryId, basePrice: MID_SALE_BASE, stock: 1 });
    const read = await client.getProduct(created.id);
    const ok = read.effectivePrice === MID_SALE_EXPECTED && read.activePromotion?.id === promotionId;
    return {
      name,
      ok,
      message: ok
        ? `product ${read.id} (sku ${sku}) created during the sale reads back at ${read.effectivePrice} (base ${MID_SALE_BASE}) under promotion ${promotionId}`
        : `product ${read.id} (sku ${sku}) reads back at ${read.effectivePrice}, expected ${MID_SALE_EXPECTED} under promotion ${promotionId}; activePromotion ${JSON.stringify(read.activePromotion)}`,
    };
  } catch (err) {
    return { name, ok: false, message: err instanceof ApiError ? `HTTP ${err.status} ${err.code}: ${err.message}` : String(err) };
  }
}
```

`apps/cli/src/load/scenarios/index.ts`: register the two scenarios. Replace the imports and `SCENARIOS` with:

```ts
import { UsageError } from '../../errors';
import { BROWSE_LABELS, createBrowse } from './browse';
import { createFlashSale } from './flash-sale';
import type { Scenario, ScenarioOptions } from './types';
import { WRITE_MIX_LABELS, createWriteMix } from './write-mix';
```

```ts
export const SCENARIOS: Record<string, ScenarioDef> = {
  browse: { labels: BROWSE_LABELS, create: createBrowse },
  'write-mix': { labels: WRITE_MIX_LABELS, create: createWriteMix },
  'flash-sale': { labels: [], create: createFlashSale },
};
```

- [ ] **Step 4: Run the tests and the typecheck**

Run: `pnpm --filter @modaco/cli exec vitest run test/scenarios.test.ts && pnpm --filter @modaco/cli typecheck`
Expected: all scenario tests PASS, no type errors.

- [ ] **Step 5: Commit**

```bash
git add apps/cli/src/load/scenarios apps/cli/test/scenarios.test.ts
git commit -m "feat(cli): write-mix and flash-sale load scenarios

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: `load` command

**Files:**
- Create: `apps/cli/src/commands/load.ts`
- Modify: `apps/cli/src/main.ts`
- Test: `apps/cli/test/cli.test.ts` (append)

**Interfaces:**
- Consumes: `globals`, `arg`, `intArg`, `durationArg` (Task 6); `parseRate`, `parseMix` (Task 2); `SCENARIOS`, `createScenario` (Tasks 7–8); `runLoad` (Task 7); `formatReport` (Task 7); `ApiClient` (Task 5); `emit`, `log` (Task 5).
- Produces: `registerLoad(program: Command): void`.

- [ ] **Step 1: Append the failing tests**

Add to `apps/cli/test/cli.test.ts`. Add these imports at the top:

```ts
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
```

Then add at the end of the file:

```ts
describe('load command', () => {
  it('runs a closed-model browse and prints the result document with --json', async () => {
    const r = await runCli(['--url', stub.url, '--json', 'load', 'browse', '--concurrency', '2', '--duration', '300ms', '--warmup', '0s', '--seed', '1']);
    expect(r.code).toBe(0);
    const doc = JSON.parse(r.stdout) as { scenario: string; phases: Array<{ total: { count: number } }>; instances: Record<string, number>; options: Record<string, unknown> };
    expect(doc.scenario).toBe('browse');
    expect(doc.phases[0]!.total.count).toBeGreaterThan(0);
    expect(Object.keys(doc.instances).sort()).toEqual(['i1', 'i2', 'i3']);
    expect(doc.options).toMatchObject({ model: 'closed', concurrency: 2, durationMs: 300, warmupMs: 0, seed: 1, connections: 64 });
  });

  it('runs an open-model browse, prints the human report and writes --out', async () => {
    const out = join(mkdtempSync(join(tmpdir(), 'modaco-cli-')), 'nested', 'run.json');
    const r = await runCli(['--url', stub.url, 'load', 'browse', '--rate', '200/s', '--duration', '500ms', '--warmup', '0s', '--out', out]);
    expect(r.code).toBe(0);
    expect(r.stdout).toContain('phase main');
    expect(r.stdout).toContain('instances (3)');
    const doc = JSON.parse(readFileSync(out, 'utf8')) as { options: Record<string, unknown>; phases: Array<{ total: { count: number } }> };
    expect(doc.options).toMatchObject({ model: 'open', rate: 200, connections: 256, maxInflight: 10000 });
    expect(doc.phases[0]!.total.count).toBeGreaterThanOrEqual(90);
  });

  it('exits 1 when a flash-sale check fails', async () => {
    const wrong = await startStubApi({ midSalePrice: '20.00' });
    try {
      const r = await runCli(['--url', wrong.url, 'load', 'flash-sale', '--concurrency', '2', '--duration', '300ms', '--warmup', '0s']);
      expect(r.code).toBe(1);
      expect(r.stdout).toContain('FAIL mid-sale product discounted');
    } finally {
      await wrong.close();
    }
  });

  it('exits 2 on bad load options', async () => {
    const cases: Array<[string[], string]> = [
      [['load', 'browse', '--duration', '1s'], 'exactly one of --rate'],
      [['load', 'browse', '--rate', '10/s', '--concurrency', '2'], 'exactly one of --rate'],
      [['load', 'nope', '--concurrency', '1'], "unknown scenario 'nope'"],
      [['load', 'browse', '--concurrency', '1', '--ramp', '5s'], '--ramp and --max-inflight apply to --rate only'],
      [['load', 'flash-sale', '--concurrency', '1', '--mix', 'list=1'], 'flash-sale does not take --mix'],
      [['load', 'browse', '--concurrency', '1', '--mix', 'stock=1'], "unknown mix label 'stock'"],
      [['load', 'browse', '--rate', 'fast'], 'invalid rate'],
    ];
    for (const [args, message] of cases) {
      const r = await runCli(['--url', stub.url, ...args]);
      expect(r.code, args.join(' ')).toBe(2);
      expect(r.stderr, args.join(' ')).toContain(message);
    }
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pnpm --filter @modaco/cli exec vitest run test/cli.test.ts`
Expected: the new `load command` tests FAIL (commander prints `unknown command 'load'` and exits 2, so the first case gets code 2 instead of 0).

- [ ] **Step 3: Implement**

`apps/cli/src/commands/load.ts`:

```ts
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import type { Command } from 'commander';
import { ApiClient } from '../client';
import { UsageError } from '../errors';
import type { LoadModel } from '../load/engine';
import { parseMix, parseRate } from '../load/parse';
import { formatReport } from '../load/report';
import { runLoad } from '../load/run';
import { SCENARIOS, createScenario } from '../load/scenarios';
import { emit, log } from '../output';
import { arg, durationArg, globals, intArg } from './common';

interface LoadFlags {
  rate?: number;
  concurrency?: number;
  duration?: number;
  warmup?: number;
  ramp?: number;
  connections?: number;
  maxInflight?: number;
  reportEvery?: number;
  out?: string;
  seed?: number;
  category?: string;
  maxPage?: number;
  mix?: string;
}

export function registerLoad(program: Command): void {
  program.command('load')
    .description('Generate load and report latency percentiles, errors and the per-instance distribution')
    .argument('<scenario>', `one of: ${Object.keys(SCENARIOS).join(', ')}`)
    .option('--rate <rate>', 'open model: constant arrival rate, e.g. 2000/s (latency from scheduled time)', arg(parseRate))
    .option('--concurrency <n>', 'closed model: number of concurrent workers', intArg('concurrency'))
    .option('--duration <d>', 'recorded measurement time (default 30s)', durationArg)
    .option('--warmup <d>', 'unrecorded warmup before measuring (default 5s)', durationArg)
    .option('--ramp <d>', 'open model: rise linearly to --rate over this time, recorded (default 0s)', durationArg)
    .option('--connections <n>', 'HTTP connection pool size (default: max(concurrency, 64), or 256 with --rate)', intArg('connections'))
    .option('--max-inflight <n>', 'open model: cap on outstanding requests; requests over it are dropped (default 10000)', intArg('max-inflight'))
    .option('--report-every <d>', 'progress line interval on stderr, 0s to disable (default 5s)', durationArg)
    .option('--out <file>', 'also write the JSON result document to this file')
    .option('--seed <n>', 'random seed, for a repeatable request sequence', intArg('seed', 0))
    .option('--category <slug>', 'restrict requests to one category (flash-sale default: accessories)')
    .option('--max-page <n>', 'highest listing page requested (default 5)', intArg('max-page'))
    .option('--mix <mix>', 'request weights, e.g. list=70,detail=30 (browse: list, detail; write-mix: list, detail, stock, promo)')
    .action(async (name: string, flags: LoadFlags, cmd: Command) => {
      const g = globals(cmd);
      const def = SCENARIOS[name];
      if (!def) throw new UsageError(`unknown scenario '${name}' (available: ${Object.keys(SCENARIOS).join(', ')})`);
      if ((flags.rate === undefined) === (flags.concurrency === undefined)) {
        throw new UsageError('pass exactly one of --rate (open model) or --concurrency (closed model)');
      }
      if (flags.rate === undefined && (flags.ramp !== undefined || flags.maxInflight !== undefined)) {
        throw new UsageError('--ramp and --max-inflight apply to --rate only');
      }
      if (flags.mix !== undefined && def.labels.length === 0) throw new UsageError(`${name} does not take --mix`);

      const model: LoadModel = flags.rate !== undefined
        ? { kind: 'open', rate: flags.rate, rampMs: flags.ramp ?? 0, maxInflight: flags.maxInflight ?? 10_000 }
        : { kind: 'closed', concurrency: flags.concurrency! };
      const mix = flags.mix === undefined ? undefined : parseMix(flags.mix, def.labels);
      const durationMs = flags.duration ?? 30_000;
      const warmupMs = flags.warmup ?? 5_000;
      const maxPage = flags.maxPage ?? 5;
      const seed = flags.seed ?? Math.floor(Math.random() * 2 ** 31);
      const connections = flags.connections ?? (model.kind === 'closed' ? Math.max(model.concurrency, 64) : 256);
      const scenario = createScenario(name, { category: flags.category, maxPage, mix });

      const client = new ApiClient({ baseUrl: g.url, timeoutMs: g.timeoutMs, connections });
      const ac = new AbortController();
      const onSigint = () => { log('interrupted: stopping, waiting up to 5s for in-flight requests (Ctrl-C again to kill)'); ac.abort(); };
      process.once('SIGINT', onSigint);
      try {
        const doc = await runLoad(client, scenario, {
          model, durationMs, warmupMs, seed, reportEveryMs: flags.reportEvery ?? 5_000,
          options: {
            model: model.kind,
            ...(model.kind === 'open' ? { rate: model.rate, rampMs: model.rampMs, maxInflight: model.maxInflight } : { concurrency: model.concurrency }),
            durationMs, warmupMs, connections, timeoutMs: g.timeoutMs, seed, maxPage,
            ...(flags.category ? { category: flags.category } : {}),
            ...(mix ? { mix } : {}),
          },
        }, { log, signal: ac.signal });
        if (flags.out) {
          mkdirSync(dirname(flags.out), { recursive: true });
          writeFileSync(flags.out, `${JSON.stringify(doc, null, 2)}\n`);
        }
        emit(g.json, doc, () => formatReport(doc));
        process.exitCode = doc.interrupted ? 130 : doc.ok ? 0 : 1;
      } finally {
        process.off('SIGINT', onSigint);
        await client.close();
      }
    });
}
```

`apps/cli/src/main.ts`: add the import and registration.

```ts
import { registerLoad } from './commands/load';
```

```ts
registerIngest(program);
registerLoad(program);
```

- [ ] **Step 4: Run the full CLI suite and the typecheck**

Run: `pnpm --filter @modaco/cli test && pnpm --filter @modaco/cli typecheck`
Expected: every CLI test file PASSES, no type errors. Then run `pnpm modaco load --help` from the repo root. Expected: every load option listed with its default in the description.

- [ ] **Step 5: Commit**

```bash
git add apps/cli/src/commands/load.ts apps/cli/src/main.ts apps/cli/test/cli.test.ts
git commit -m "feat(cli): load command with open/closed models, --out and SIGINT handling

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Load-balanced Compose profile, docs, spec amendments

**Files:**
- Create: `infra/nginx/nginx.conf`
- Modify: `docker-compose.yml`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-09-25-modaco-cli-design.md`

**Interfaces:**
- Consumes: the `api` service definition in `docker-compose.yml`; the `x-instance-id` header (Task 1).
- Produces: Compose profile `lb` with services `api-lb` (scalable, no host port, CPU limit `${API_CPUS:-1}`) and `nginx` (host port 8080).

- [ ] **Step 1: Write the nginx config**

`infra/nginx/nginx.conf`:

```nginx
# Round robin over every api-lb replica. Docker's DNS returns all replica addresses for "api-lb", and nginx
# resolves the name once at startup: run `docker compose restart nginx` after changing the replica count.
worker_processes auto;

events {
  worker_connections 4096;
}

http {
  access_log off; # the load generator is the measurement

  upstream api {
    server api-lb:3000;
    keepalive 256;
  }

  server {
    listen 80;

    location / {
      proxy_pass http://api;
      proxy_http_version 1.1;
      proxy_set_header Connection ""; # keep upstream connections alive
      proxy_set_header Host $host;
    }
  }
}
```

- [ ] **Step 2: Restructure `docker-compose.yml`**

Replace the `api:` service block (from `  api:` up to, but not including, `  runner:`) with the following. Also add the `x-api` extension field at the very top of the file, before `services:`.

Top of the file:

```yaml
# Shared by the single "api" service (profile app, port 3000) and the scalable "api-lb" service (profile lb).
x-api: &api
  build: .
  command: pnpm --filter @modaco/api start
  environment:
    PORT: 3000
    DATABASE_URL: postgres://modaco:modaco@postgres:5432/modaco
    REDIS_URL: redis://redis:6379
    AWS_REGION: us-east-1
    AWS_ACCESS_KEY_ID: test
    AWS_SECRET_ACCESS_KEY: test
    S3_PUBLIC_ENDPOINT: http://localhost:4566
    S3_BUCKET: modaco-vendor-uploads
  depends_on:
    postgres: { condition: service_healthy }
    redis: { condition: service_healthy }
    localstack: { condition: service_healthy }

```

Replacement for the `api` service:

```yaml
  api:
    <<: *api
    profiles: ["app"]
    ports: ["3000:3000"]

  # Scalable replicas behind nginx: docker compose --profile lb up --build -d --scale api-lb=4
  # No host port, so replicas do not collide. The CPU limit makes each replica one unit of capacity.
  api-lb:
    <<: *api
    profiles: ["lb"]
    deploy:
      resources:
        limits:
          cpus: "${API_CPUS:-1}"

  nginx:
    profiles: ["lb"]
    image: nginx:1.27-alpine
    ports: ["8080:80"]
    volumes:
      - ./infra/nginx/nginx.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      - api-lb

```

- [ ] **Step 3: Validate the Compose file**

Run: `docker compose --profile app --profile lb config --services && docker compose --profile lb config | grep -A3 'cpus'`
Expected: services `postgres redis localstack api runner api-lb nginx` (in any order), and `cpus: 1` under `api-lb`. The `api` service must still publish `3000:3000`; check with `docker compose --profile app config | grep -B2 -A2 'published: "3000"'`.

- [ ] **Step 4: Update the README**

In `README.md`, insert a new section after `## Demos` and before `## Tests`:

````markdown
## CLI (`pnpm modaco`)

`apps/cli` is a command-line client for every endpoint plus a load generator. It needs only a base URL (`--url`, else `API_URL`, else `http://localhost:3000`), so it works the same against one local API, the nginx load balancer below, or a remote deployment. Every command takes `--json` and then prints one JSON document to stdout. Exit codes: 0 ok, 1 API/check/transport failure, 2 usage error, 130 interrupted.

```bash
pnpm modaco health                                        # status, checks and the answering instance
pnpm modaco health --watch 1s                             # keep polling (useful while scaling replicas)
pnpm modaco products list --category accessories --sort=-effective_price --page-size 5
pnpm modaco products get 1
pnpm modaco products create --sku DEMO-1 --name Demo --category-id 1 --base-price 19.99 --stock 3
pnpm modaco products stock 1 --delta -2                   # or --set 10
pnpm modaco promotions create --name "50% off" --type percentage --value 50 --category 1
pnpm modaco promotions target <promotionId> --product 2
pnpm modaco promotions cancel <promotionId>
pnpm modaco ingest upload tmp/vendor-500k.csv             # create job, upload, poll to completion
pnpm modaco ingest rejections <jobId> --page-size 10
```

### Load testing

```bash
pnpm modaco load browse --rate 2000/s --duration 60s               # open model: fixed arrival rate
pnpm modaco load browse --concurrency 100 --duration 60s           # closed model: fixed workers
pnpm modaco load write-mix --rate 500/s --duration 60s             # reads + stock writes + promotion churn
pnpm modaco load flash-sale --category accessories --concurrency 50 --duration 15s
pnpm modaco load browse --rate 2000/s --ramp 30s --duration 90s --out tmp/run.json
```

- **Scenarios.**
  - `browse`: category listings with random page and sort, plus product details (`--mix list=70,detail=30`, `--max-page 5`, `--category`).
  - `write-mix`: reads plus stock writes plus promotion create/cancel cycles that bump cache versions (`--mix list=60,detail=25,stock=14,promo=1`).
  - `flash-sale`: the `demo:flash-sale` flow on the load engine. It records a `before` and an `after` phase and checks that a product created mid-sale reads back at half price; the run exits 1 if that check fails.
- **Models.** `--rate` holds a constant arrival rate and times each request from its scheduled start, so a stalling server is charged for the requests it delayed (no coordinated omission). Requests over `--max-inflight` (default 10000) are counted as `dropped`. `--concurrency` runs fixed workers, which is useful for finding saturation throughput.
- **Output.** Per label and in total: count, req/s, p50/p90/p95/p99/p99.9/max in ms, status codes, transport errors (timeout, ECONNRESET, ECONNREFUSED) and drops. Also the share of responses served by each `X-Instance-Id`. `--json` or `--out` gives the full result document for comparing runs.
- **Repeatability.** `--seed` makes the request sequence repeatable. Setup samples up to 20 listing pages to find product ids and categories, so the catalog must not be empty.

### Several API replicas behind nginx

```bash
docker compose up -d postgres redis localstack && pnpm db:migrate && pnpm seed   # if not done already
docker compose --profile lb up --build -d --scale api-lb=4                       # 4 replicas + nginx on :8080
pnpm modaco --url http://localhost:8080 health --watch 1s                        # instance id rotates
pnpm modaco --url http://localhost:8080 load browse --rate 2000/s --duration 60s --out tmp/lb-4.json

docker compose --profile lb up -d --scale api-lb=1 && docker compose restart nginx   # nginx resolves replicas at startup
pnpm modaco --url http://localhost:8080 load browse --rate 2000/s --duration 60s --out tmp/lb-1.json
```

- Each replica is limited to `API_CPUS` cores (default 1), so adding replicas adds capacity instead of sharing every host core.
- The API sets `X-Instance-Id` on every response: `INSTANCE_ID` if set, else the hostname, which is the container id under Compose. It exposes nothing beyond that hostname; drop the middleware if the API is ever public.
- Each replica holds a Postgres pool of 10 and Postgres allows 100 connections by default, so up to about 9 replicas fit. More need `max_connections` raised or a pooler (PgBouncer; RDS Proxy on AWS).
- Postgres, Redis, nginx, the replicas and the load generator share this machine's CPUs. Local runs compare configurations (1 vs N replicas); they do not predict production capacity.
- Stop the replicas with `docker compose --profile lb stop api-lb nginx`.
````

In the `## Layout` code block of `README.md`, add after the `apps/ingest/` line:

```
apps/cli/            modaco CLI: operational commands and the load generator (pnpm modaco)
```

and after the `infra/template.yaml` line:

```
infra/nginx/         nginx load balancer config for the Compose "lb" profile
```

- [ ] **Step 5: Write the two deviations into the spec**

In `docs/superpowers/specs/2026-09-25-modaco-cli-design.md`:

1. In the §5 table, replace the row starting `` | `ingest <file> [--poll 1s]` `` with:

```markdown
| `ingest upload <file> [--poll 1s]` | `POST /ingestion/jobs`, `PUT` to the presigned URL, then poll until `completed` or `failed`. (`upload` is a subcommand: commander cannot mix a positional file with the `status`/`rejections` subcommands.) |
```

   In §3's layout block, change `ingest <file> | ingest status <jobId>` to `ingest upload <file> | ingest status <jobId>`. In §5, change the sentence ending `For `ingest <file>` it prints the final job document.` to `For `ingest upload` it prints the final job document.`

2. In §7.3, replace the `stock` bullet with:

```markdown
- `stock` 14%: `PATCH /products/<random id>/stock` with `{ "stock": <0..100> }`. An absolute set never fails. A `{ "delta": -1 }` on a product at stock 0 returns 422, which would add noise to the error stats.
```

- [ ] **Step 6: Commit**

```bash
git add infra/nginx/nginx.conf docker-compose.yml README.md docs/superpowers/specs/2026-09-25-modaco-cli-design.md
git commit -m "feat(infra): nginx load balancer over scalable api-lb replicas; document the CLI

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: End-to-end verification against the real stack

No new code. This task proves the tool works against the real API, alone and behind nginx. Fix any defect it finds in the task that owns the file, re-run that task's tests, and commit the fix separately.

**Files:** none (optionally `README.md` if measured numbers are recorded)

- [ ] **Step 1: Full test suite and typecheck**

```bash
docker compose up -d postgres redis localstack
pnpm test
pnpm typecheck
```

Expected: all packages pass, including `@modaco/cli` and the new API test. `pnpm test` truncates the database, so re-seed afterwards:

```bash
pnpm db:migrate && pnpm seed
```

- [ ] **Step 2: Operational commands against a local API**

Start the API in the background (`pnpm dev:api`, with `.env.example` exported as in the README), then run:

```bash
pnpm modaco health
pnpm modaco products list --category accessories --page-size 3
pnpm modaco --json promotions create --name "CLI check" --type percentage --value 50 --category 1
pnpm modaco products list --category accessories --page-size 3     # effective prices halved
pnpm modaco promotions cancel <id from the create output>
pnpm modaco products get 999999; echo "exit $?"                    # HTTP 404 not_found, exit 1
```

Expected: `health` shows `ok ... instance <hostname>`. The promotion halves the listed effective prices, and cancelling it restores them. The 404 prints the envelope and exits 1.

- [ ] **Step 3: Each scenario against the single API**

```bash
pnpm modaco load browse --concurrency 20 --duration 10s --warmup 2s
pnpm modaco load write-mix --rate 300/s --duration 10s --warmup 2s
pnpm modaco load flash-sale --category accessories --concurrency 20 --duration 6s --warmup 2s
```

Expected: no transport errors. `browse` reports only 200s. `write-mix` shows 200/201 for `promo:*` and 200 for `stock`. `flash-sale` prints `PASS mid-sale product discounted` and warns that the category has fewer than 50k products, which is expected with seed data. Stop `pnpm dev:api` afterwards.

- [ ] **Step 4: Replicas behind nginx**

```bash
docker compose --profile lb up --build -d --scale api-lb=1
pnpm modaco --url http://localhost:8080 load browse --concurrency 100 --duration 20s --out tmp/lb-1.json
docker compose --profile lb up -d --scale api-lb=4 && docker compose restart nginx
pnpm modaco --url http://localhost:8080 health --watch 500ms      # Ctrl-C after a few lines: the instance id rotates
pnpm modaco --url http://localhost:8080 load browse --concurrency 100 --duration 20s --out tmp/lb-4.json
```

Expected: `tmp/lb-4.json` has 4 entries under `instances`, each close to 25%. Saturated throughput (req/s at concurrency 100) is higher with 4 replicas than with 1. Postgres, Redis and the load generator share the host, so expect well under 4x. If throughput does not rise, check `docker stats` for the bottleneck before concluding anything, and note it.

- [ ] **Step 5: Interrupt handling**

```bash
pnpm modaco --url http://localhost:8080 load write-mix --rate 200/s --duration 60s
```

Press Ctrl-C after about 5 s. Expected: the `interrupted: stopping ...` line, a partial report marked `(INTERRUPTED)`, and exit code 130 (`echo $?`). Then confirm that cleanup cancelled every promotion the run created:

```bash
docker compose exec postgres psql -U modaco -tAc "select count(*) from promotions where name = 'write-mix load test' and cancelled_at is null"
```

Expected: `0`.

- [ ] **Step 6: Tear down and record**

```bash
docker compose --profile lb stop api-lb nginx
```

Optionally add a short "measured on <machine>" line with the 1 vs 4 replica req/s and p99 to the README's load-balancer section, and commit:

```bash
git add README.md
git commit -m "docs: record 1 vs 4 replica load test results

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
