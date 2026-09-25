import { execFile } from 'node:child_process';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { STUB_JOB_ID, startStubApi, type StubApi } from './stub-api';

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

  it('products stock sets or adjusts the stock', async () => {
    const delta = await runCli(['--url', stub.url, '--json', 'products', 'stock', '7', '--delta', '-2']);
    expect(delta.code).toBe(0);
    expect(JSON.parse(delta.stdout)).toEqual({ id: 7, stock: 3 });
    const set = await runCli(['--url', stub.url, 'products', 'stock', '7', '--set', '10']);
    expect(set.code).toBe(0);
    expect(set.stdout.trim()).toBe('product 7: stock 10');
  });

  it('promotions target moves a promotion to a product', async () => {
    const created = await runCli(['--url', stub.url, '--json', 'promotions', 'create', '--name', 'Move me', '--type', 'fixed', '--value', '5.00', '--category', '2']);
    const { id } = JSON.parse(created.stdout) as { id: string };
    const moved = await runCli(['--url', stub.url, '--json', 'promotions', 'target', id, '--product', '42']);
    expect(moved.code).toBe(0);
    expect(JSON.parse(moved.stdout)).toMatchObject({ id, name: 'Move me', target: { productId: 42 } });
    const human = await runCli(['--url', stub.url, 'promotions', 'target', id, '--category', '3']);
    expect(human.code).toBe(0);
    expect(human.stdout).toMatch(/target\s+category 3/);
    const missing = await runCli(['--url', stub.url, 'promotions', 'target', '00000000-0000-4000-8000-000000000000', '--product', '1']);
    expect(missing.code).toBe(1);
    expect(missing.stderr).toContain('HTTP 404 not_found');
  });

  it('ingest status prints the job', async () => {
    const json = await runCli(['--url', stub.url, '--json', 'ingest', 'status', STUB_JOB_ID]);
    expect(json.code).toBe(0);
    expect(JSON.parse(json.stdout)).toMatchObject({ id: STUB_JOB_ID, status: 'processing', totalChunks: 9, completedChunks: 3, rowsProcessed: 150000 });
    const human = await runCli(['--url', stub.url, 'ingest', 'status', STUB_JOB_ID]);
    expect(human.code).toBe(0);
    expect(human.stdout).toMatch(/chunks\s+3\/9 completed, 0 failed/);
  });

  it('exits 2 on usage errors', async () => {
    expect((await runCli(['--url', stub.url, 'products', 'get', 'abc'])).code).toBe(2);
    expect((await runCli(['--url', stub.url, 'products', 'stock', '1'])).code).toBe(2);
    expect((await runCli(['--url', stub.url, 'promotions', 'create', '--name', 'x', '--type', 'percentage', '--value', '1'])).code).toBe(2);
    expect((await runCli(['--url', stub.url, '--timeout', 'soon', 'health'])).code).toBe(2);
    // undici treats a 0 timeout as "no timeout", so 0 (or anything rounding to 0 ms) is refused.
    for (const timeout of ['0s', '0ms', '0.4ms']) {
      const r = await runCli(['--url', stub.url, '--timeout', timeout, 'health']);
      expect(r.code, timeout).toBe(2);
      expect(r.stderr, timeout).toContain('--timeout must be at least 1ms');
    }
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
    // connections defaults to rate * (--timeout in seconds), floored at 256 and capped at --max-inflight: 200 * 10s = 2000.
    expect(doc.options).toMatchObject({ model: 'open', rate: 200, connections: 2000, maxInflight: 10000 });
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
      [['load', 'browse', '--concurrency', '1', '--max-error-rate', '2'], 'invalid --max-error-rate'],
    ];
    for (const [args, message] of cases) {
      const r = await runCli(['--url', stub.url, ...args]);
      expect(r.code, args.join(' ')).toBe(2);
      expect(r.stderr, args.join(' ')).toContain(message);
    }
  });

  it('counts 5xx as errors and fails the run by default, but --max-error-rate can accept them', async () => {
    const down = await startStubApi({ failStatus: 503 });
    try {
      const r = await runCli(['--url', down.url, '--json', 'load', 'browse', '--concurrency', '4', '--duration', '300ms', '--warmup', '0s']);
      expect(r.code).toBe(1);
      const doc = JSON.parse(r.stdout) as {
        ok: boolean;
        phases: Array<{ total: { count: number; errors: Record<string, number>; status: Record<string, number> } }>;
        checks: Array<{ name: string; ok: boolean }>;
      };
      expect(doc.ok).toBe(false);
      const total = doc.phases[0]!.total;
      expect(total.count).toBe(0); // no 2xx/3xx responses at all: 503 is kept out of the success histogram
      expect(total.errors.serverError).toBeGreaterThan(0);
      expect(total.status['503']).toBe(total.errors.serverError);
      expect(doc.checks).toContainEqual(expect.objectContaining({ name: 'error rate', ok: false }));

      const tolerant = await runCli(['--url', down.url, 'load', 'browse', '--concurrency', '4', '--duration', '300ms', '--warmup', '0s', '--max-error-rate', '1']);
      expect(tolerant.code).toBe(0);
      expect(tolerant.stdout).toContain('PASS error rate');
    } finally {
      await down.close();
    }
  });

  it('SIGINT during a load run exits 130 (not by signal) and leaves no open promotions on the stub', async () => {
    // Its own stub, not the shared one: other tests in this file leave promotions open on purpose (e.g. the
    // retargeting test above never cancels its promotion), so asserting a global 0 against the shared stub would
    // depend on test order instead of on this run's own cleanup.
    const own = await startStubApi();
    try {
      const child = execFile(
        process.execPath,
        ['--import', 'tsx', 'src/main.ts', '--url', own.url, 'load', 'write-mix', '--concurrency', '8', '--duration', '10s', '--warmup', '0s', '--mix', 'promo=1'],
        { cwd: cliDir, env: { ...process.env, API_URL: '' } },
      );
      // Attached before anything else can happen, so a fast exit is never missed.
      const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
        child.once('exit', (code, signal) => resolve({ code, signal }));
      });

      // Proof the load phase actually started -- not a blind sleep, which either races tsx's startup time on a
      // loaded machine (too short) or sends SIGINT well after the run is already underway (too long, and closer to
      // testing the drain path than the "abort while running" path this test is for).
      await new Promise<void>((resolve, reject) => {
        let buf = '';
        const onData = (chunk: Buffer) => {
          buf += chunk.toString();
          if (buf.includes('phase main')) cleanup(resolve);
        };
        const onExit = (code: number | null) => cleanup(() => reject(new Error(`child exited (code ${code}) before the load phase started; stderr/stdout so far:\n${buf}`)));
        const cleanup = (then: () => void) => {
          child.stdout?.off('data', onData);
          child.stderr?.off('data', onData);
          child.off('exit', onExit);
          then();
        };
        child.stdout?.on('data', onData);
        child.stderr?.on('data', onData);
        child.once('exit', onExit);
      });

      child.kill('SIGINT');
      const { code, signal } = await exited;
      expect(code).toBe(130);
      expect(signal).toBeNull();
      expect(own.state.openPromotions.size).toBe(0);
    } finally {
      await own.close();
    }
  }, 15_000);
});
