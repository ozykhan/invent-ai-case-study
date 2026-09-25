import type { S3Event, S3Handler, SQSHandler } from 'aws-lambda';
import { getDeps } from '../deps';
import { handleDeadLetter } from '../dlq';
import { chunkMessageSchema, splitUpload } from '../splitter';
import { processChunk } from '../worker';

const decodeKey = (key: string) => decodeURIComponent(key.replace(/\+/g, ' '));

export const splitter: S3Handler = async (event: Partial<S3Event>) => {
  const deps = await getDeps();
  for (const record of event.Records ?? []) {
    await splitUpload(deps, { key: decodeKey(record.s3.object.key) });
  }
};

export const worker: SQSHandler = async (event) => {
  const deps = await getDeps();
  for (const record of event.Records) {
    await processChunk(deps, chunkMessageSchema.parse(JSON.parse(record.body)));
  }
};

export const deadLetter: SQSHandler = async (event) => {
  const deps = await getDeps();
  for (const record of event.Records) {
    const receives = record.attributes?.ApproximateReceiveCount ?? '?';
    await handleDeadLetter(deps, chunkMessageSchema.parse(JSON.parse(record.body)), `exceeded max receive count (receives=${receives})`);
  }
};
