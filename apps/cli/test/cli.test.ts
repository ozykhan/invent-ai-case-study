import { execFile } from 'node:child_process';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
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
      [['load', 'browse', '--concurrency', '1', '--max-page', '1001'], '--max-page must be <= 1000'],
    ];
    for (const [args, message] of cases) {
      const r = await runCli(['--url', stub.url, ...args]);
      expect(r.code, args.join(' ')).toBe(2);
      expect(r.stderr, args.join(' ')).toContain(message);
    }
  });
});
