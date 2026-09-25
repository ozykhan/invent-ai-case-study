import type { S3Event, S3Handler, SQSHandler } from 'aws-lambda';
import { getDeps } from '../deps';
import { handleDeadLetter } from '../dlq';
import { chunkMessageSchema, splitUpload } from '../splitter';
import { processChunk } from '../worker';

export const decodeKey = (key: string) => decodeURIComponent(key.replace(/\+/g, ' '));

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
    // The DLQ has no redrive of its own: a message that can never be parsed would otherwise be redelivered
    // forever (throw -> not deleted -> visible again -> throw -> ...). Log and drop it instead of throwing,
    // so the handler resolves and the message is deleted; a genuine failure in handleDeadLetter itself
    // (e.g. the database being down) still throws and leaves the message for redelivery.
    let msg;
    try {
      msg = chunkMessageSchema.parse(JSON.parse(record.body));
    } catch (err) {
      deps.logger.error({ err, messageId: record.messageId, body: record.body.slice(0, 500) }, 'dead-letter message is not a valid chunk message; dropping');
      continue;
    }
    await handleDeadLetter(deps, msg, `exceeded max receive count (receives=${receives})`);
  }
};
