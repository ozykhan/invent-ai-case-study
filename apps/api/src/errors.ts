export class HttpError extends Error {
  constructor(public readonly status: number, public readonly code: string, message: string, public readonly details?: unknown) {
    super(message);
  }
}
export const notFound = (message: string) => new HttpError(404, 'not_found', message);
export const unprocessable = (message: string, details?: unknown) => new HttpError(422, 'unprocessable', message, details);
export const conflict = (message: string) => new HttpError(409, 'conflict', message);

export { pgErrorCode } from '@modaco/core';
