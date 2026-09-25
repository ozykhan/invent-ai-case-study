import Redis from 'ioredis';

/** Fast-failing client: a down Redis rejects commands immediately instead of queueing them. */
export function createRedis(url: string): Redis {
  return new Redis(url, {
    lazyConnect: true,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
    connectTimeout: 2000,
    commandTimeout: 300,
    retryStrategy: (times) => Math.min(times * 200, 2000),
  });
}

export type { Redis };

/** ioredis resolves pipeline().exec() instead of rejecting; surface a null result or any per-command error. */
export function throwOnPipelineError(results: [Error | null, unknown][] | null): void {
  if (!results) throw new Error('pipeline exec returned null');
  const failed = results.find(([err]) => err != null);
  if (failed) throw failed[0];
}
