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
