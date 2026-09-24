import { describe, expect, it } from 'vitest';
import { computeChunks, MAX_LINE_BYTES, ownedLines, rangeFor } from './chunking';

async function* pieces(buf: Buffer, size: number): AsyncGenerator<Uint8Array> {
  for (let i = 0; i < buf.length; i += size) yield buf.subarray(i, Math.min(i + size, buf.length));
}

async function collectAll(buf: Buffer, chunkSize: number, pieceSize: number): Promise<string[]> {
  const lines: string[] = [];
  for (const chunk of computeChunks(buf.length, chunkSize)) {
    const { rangeStart, rangeEnd } = rangeFor(chunk);
    const slice = buf.subarray(rangeStart, Math.min(rangeEnd + 1, buf.length));
    for await (const { line } of ownedLines(pieces(slice, pieceSize), chunk, rangeStart)) lines.push(line);
  }
  return lines;
}

describe('computeChunks', () => {
  it('splits a length into inclusive byte ranges', () => {
    expect(computeChunks(10, 4)).toEqual([
      { chunkIndex: 0, byteStart: 0, byteEnd: 3 },
      { chunkIndex: 1, byteStart: 4, byteEnd: 7 },
      { chunkIndex: 2, byteStart: 8, byteEnd: 9 },
    ]);
    expect(computeChunks(8, 4)).toHaveLength(2);
    expect(computeChunks(0, 4)).toEqual([]);
    expect(computeChunks(3, 4)).toEqual([{ chunkIndex: 0, byteStart: 0, byteEnd: 2 }]);
  });
  it('rangeFor reads one byte early and over-reads past the end', () => {
    expect(rangeFor({ byteStart: 0, byteEnd: 3 })).toEqual({ rangeStart: 0, rangeEnd: 3 + 65536 });
    expect(rangeFor({ byteStart: 4, byteEnd: 7 })).toEqual({ rangeStart: 3, rangeEnd: 7 + 65536 });
  });
});

describe('ownedLines', () => {
  const text = ['sku,name', 'A,alpha', 'BB,beta', 'C,c', 'DDDD,delta delta', 'E,e', 'F,f'].join('\n') + '\n';
  const expected = text.trimEnd().split('\n');

  it('yields every line exactly once for any chunk size and piece size', async () => {
    const buf = Buffer.from(text);
    for (const chunkSize of [1, 2, 3, 5, 7, 8, 11, 16, 64, 1000]) {
      for (const pieceSize of [1, 3, 1000]) {
        expect(await collectAll(buf, chunkSize, pieceSize), `chunk=${chunkSize} piece=${pieceSize}`).toEqual(expected);
      }
    }
  });

  it('handles a boundary exactly after a newline', async () => {
    const buf = Buffer.from('ab\ncd\nef\n'); // newline at index 2; chunk 1 starts at 3
    const lines = await collectAll(buf, 3, 100);
    expect(lines).toEqual(['ab', 'cd', 'ef']);
  });

  it('handles a file without a trailing newline and CRLF endings', async () => {
    expect(await collectAll(Buffer.from('a\r\nb\r\nc'), 2, 1)).toEqual(['a', 'b', 'c']);
    expect(await collectAll(Buffer.from('a\nb\nc'), 100, 100)).toEqual(['a', 'b', 'c']);
  });

  it('reports absolute offsets', async () => {
    const buf = Buffer.from('ab\ncd\nef\n');
    const chunk = { byteStart: 3, byteEnd: 5 };
    const { rangeStart } = rangeFor(chunk);
    const out: number[] = [];
    for await (const { offset } of ownedLines(pieces(buf.subarray(rangeStart), 100), chunk, rangeStart)) out.push(offset);
    expect(out).toEqual([3]);
  });

  it('rejects a line longer than the limit', async () => {
    const buf = Buffer.from('x'.repeat(MAX_LINE_BYTES + 10) + '\n');
    const chunk = { byteStart: 0, byteEnd: buf.length - 1 };
    await expect(async () => { for await (const _ of ownedLines(pieces(buf, 4096), chunk, 0)) { /* drain */ } }).rejects.toThrow(/exceeds/);
  });
});
