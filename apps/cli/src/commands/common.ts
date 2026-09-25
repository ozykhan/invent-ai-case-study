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
