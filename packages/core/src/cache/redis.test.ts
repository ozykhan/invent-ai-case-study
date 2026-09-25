import { describe, expect, it } from 'vitest';
import { throwOnPipelineError } from './redis';

describe('throwOnPipelineError', () => {
  it('throws when the pipeline result is null', () => {
    expect(() => throwOnPipelineError(null)).toThrow('pipeline exec returned null');
  });

  it('throws the first per-command error when any pair failed', () => {
    const boom = new Error('boom');
    const results: [Error | null, unknown][] = [[null, 'OK'], [boom, null]];
    expect(() => throwOnPipelineError(results)).toThrow(boom);
  });

  it('returns (does not throw) when every pair succeeded', () => {
    const results: [Error | null, unknown][] = [[null, 1], [null, 2]];
    expect(() => throwOnPipelineError(results)).not.toThrow();
  });
});
