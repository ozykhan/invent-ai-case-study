import type { IngestDeps } from './deps';
import { markChunkFailed } from './job-state';
import type { ChunkMessage } from './splitter';

export async function handleDeadLetter(deps: Pick<IngestDeps, 'db' | 'logger'>, msg: ChunkMessage, reason: string): Promise<void> {
  const { changed } = await markChunkFailed(deps.db, { jobId: msg.jobId, chunkIndex: msg.chunkIndex, error: reason });
  deps.logger.error({ jobId: msg.jobId, chunkIndex: msg.chunkIndex, reason, changed }, 'chunk dead-lettered');
}
