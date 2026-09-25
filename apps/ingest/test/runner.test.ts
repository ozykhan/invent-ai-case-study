import { existsSync, mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { invokeHandler } from '../src/runner';

const fixturePath = path.join(path.dirname(fileURLToPath(import.meta.url)), 'fixtures/sleeper.ts');

describe('invokeHandler', () => {
  it('kills the process actually running the handler when the timeout elapses, not just a wrapper', async () => {
    const dir = mkdtempSync(path.join(tmpdir(), 'modaco-runner-timeout-'));
    const pidFile = path.join(dir, 'pid');
    const doneFile = path.join(dir, 'done');

    await expect(invokeHandler([fixturePath, pidFile, doneFile], null, { timeoutMs: 300 })).rejects.toThrow(/timeout after 300ms/);

    const pid = Number(readFileSync(pidFile, 'utf8'));
    expect(Number.isInteger(pid)).toBe(true);

    // Give the OS a moment to finish reaping the killed process before we check it.
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(() => process.kill(pid, 0)).toThrow(); // ESRCH: no such process — it was actually killed.
    expect(existsSync(doneFile)).toBe(false); // never reached the point of writing past the timeout.
  }, 10_000);
});
