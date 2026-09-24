export class HttpError extends Error {
  constructor(public readonly status: number, public readonly code: string, message: string, public readonly details?: unknown) {
    super(message);
  }
}
export const notFound = (message: string) => new HttpError(404, 'not_found', message);
export const unprocessable = (message: string, details?: unknown) => new HttpError(422, 'unprocessable', message, details);
export const conflict = (message: string) => new HttpError(409, 'conflict', message);

// drizzle-orm 0.44 wraps driver errors in DrizzleQueryError; the pg SQLSTATE or
// errno code lives on `cause`. Check both so raw pg errors keep working.
export function pgErrorCode(err: unknown): string | undefined {
  const e = err as { code?: unknown; cause?: { code?: unknown } } | undefined;
  const code = e?.code ?? e?.cause?.code;
  return typeof code === 'string' ? code : undefined;
}
