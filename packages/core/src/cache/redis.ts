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
