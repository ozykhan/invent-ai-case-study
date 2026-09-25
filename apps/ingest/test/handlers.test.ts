import { describe, expect, it } from 'vitest';
import { decodeKey } from '../src/handlers/lambda';

describe('decodeKey', () => {
  it('decodes a literal + as a space (S3 event key encoding)', () => {
    expect(decodeKey('uploads/abc/vendor+file.csv')).toBe('uploads/abc/vendor file.csv');
  });

  it('decodes percent-escapes such as %2C and %2F', () => {
    expect(decodeKey('uploads/abc/vendor%2Cfile%2Fname.csv')).toBe('uploads/abc/vendor,file/name.csv');
  });

  it('decodes + and percent-escapes together', () => {
    expect(decodeKey('uploads/abc/vendor+file%2Cv2.csv')).toBe('uploads/abc/vendor file,v2.csv');
  });

  it('leaves an already-plain key untouched', () => {
    expect(decodeKey('uploads/abc/vendor-file.csv')).toBe('uploads/abc/vendor-file.csv');
  });
});
