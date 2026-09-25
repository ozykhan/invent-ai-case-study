export interface ChunkRange { chunkIndex: number; byteStart: number; byteEnd: number }

export const OVERREAD_BYTES = 65_536;
export const MAX_LINE_BYTES = 65_536;

/** Fixed-size inclusive byte ranges covering [0, contentLength). Pure arithmetic: never reads the file. */
export function computeChunks(contentLength: number, chunkSizeBytes: number): ChunkRange[] {
  if (chunkSizeBytes <= 0) throw new Error('chunkSizeBytes must be positive');
  const out: ChunkRange[] = [];
  for (let start = 0, i = 0; start < contentLength; start += chunkSizeBytes, i++) {
    out.push({ chunkIndex: i, byteStart: start, byteEnd: Math.min(start + chunkSizeBytes, contentLength) - 1 });
  }
  return out;
}

/** The S3 range to request for a chunk: one byte early (to see the preceding newline) and an over-read to finish the last line. */
export function rangeFor(chunk: { byteStart: number; byteEnd: number }): { rangeStart: number; rangeEnd: number } {
  return { rangeStart: Math.max(chunk.byteStart - 1, 0), rangeEnd: chunk.byteEnd + OVERREAD_BYTES };
}

/**
 * Yields the lines owned by `chunk`: those whose first byte lies in [byteStart, byteEnd].
 * `rangeStart` is the absolute offset of the first byte of `source`.
 */
export async function* ownedLines(
  source: AsyncIterable<Uint8Array>,
  chunk: { byteStart: number; byteEnd: number },
  rangeStart: number,
): AsyncGenerator<{ line: string; offset: number }> {
  let buffered = Buffer.alloc(0);
  let segmentStart = rangeStart; // absolute offset of buffered[0]

  const toLine = (b: Buffer) => (b.length && b[b.length - 1] === 13 ? b.subarray(0, -1) : b).toString('utf8');

  for await (const piece of source) {
    buffered = buffered.length ? Buffer.concat([buffered, piece]) : Buffer.from(piece);
    let nl: number;
    while ((nl = buffered.indexOf(10)) !== -1) {
      const offset = segmentStart;
      const lineBuf = buffered.subarray(0, nl);
      if (lineBuf.length > MAX_LINE_BYTES) throw new Error(`line at byte ${offset} exceeds ${MAX_LINE_BYTES} bytes`);
      buffered = buffered.subarray(nl + 1);
      segmentStart = offset + nl + 1;
      if (offset > chunk.byteEnd) return;
      if (offset >= chunk.byteStart) yield { line: toLine(lineBuf), offset };
    }
    if (segmentStart > chunk.byteEnd) return;
    if (buffered.length > MAX_LINE_BYTES) throw new Error(`line at byte ${segmentStart} exceeds ${MAX_LINE_BYTES} bytes`);
  }
  if (buffered.length > 0 && segmentStart >= chunk.byteStart && segmentStart <= chunk.byteEnd) {
    yield { line: toLine(buffered), offset: segmentStart };
  }
}
