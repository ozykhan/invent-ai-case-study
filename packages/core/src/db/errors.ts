// drizzle-orm 0.44 wraps driver errors in DrizzleQueryError; the pg SQLSTATE (or a socket errno code)
// and the driver's own message live on `cause`. Check both so raw pg errors keep working.
type MaybePgError = { code?: unknown; message?: unknown; cause?: { code?: unknown; message?: unknown } } | undefined;

export function pgErrorCode(err: unknown): string | undefined {
  const e = err as MaybePgError;
  const code = e?.code ?? e?.cause?.code;
  return typeof code === 'string' ? code : undefined;
}

/** The driver's message (e.g. `value "3000000000" is out of range for type integer`), not drizzle's "Failed query: ..." wrapper. */
export function pgErrorMessage(err: unknown): string {
  const e = err as MaybePgError;
  const msg = e?.cause?.message ?? e?.message;
  return typeof msg === 'string' ? msg : String(err);
}

/**
 * SQLSTATE class 22 (data exception) or 23 (integrity constraint violation): the statement's data is at
 * fault, so retrying it unchanged fails the same way every time — unlike a connection or timeout error.
 */
export function isDataError(err: unknown): boolean {
  const code = pgErrorCode(err);
  return code !== undefined && /^2[23][0-9A-Z]{3}$/.test(code);
}
