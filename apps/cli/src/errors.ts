/** Bad command-line input. Exit code 2. */
export class UsageError extends Error {
  override name = 'UsageError';
}

/** A load scenario could not prepare (empty catalog, unknown category). Exit code 1. */
export class SetupError extends Error {
  override name = 'SetupError';
}

/** A 4xx/5xx API response, carrying the API's error envelope. Exit code 1. */
export class ApiError extends Error {
  override name = 'ApiError';
  constructor(readonly status: number, readonly code: string, message: string, readonly details?: unknown) {
    super(message);
  }

  toJSON(): { status: number; error: { code: string; message: string; details?: unknown } } {
    return { status: this.status, error: { code: this.code, message: this.message, ...(this.details === undefined ? {} : { details: this.details }) } };
  }
}
