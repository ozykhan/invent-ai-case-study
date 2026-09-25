import { CreateQueueCommand, DeleteQueueCommand, SendMessageCommand } from '@aws-sdk/client-sqs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { pollOnce, sqsEventFrom } from '../src/queue';
import { setupIngestTest, type IngestTestContext } from './helpers';

let ctx: IngestTestContext;
// A dedicated, ephemeral queue rather than the shared real DLQ: this test intentionally leaves a message
// stuck invisible (to prove a failed handler doesn't delete it), and the shared DLQ is used by other tests
// and the runner smoke test, so reusing it would leave stray messages behind for them to trip over.
let testQueueUrl: string;

beforeAll(async () => {
  ctx = await setupIngestTest();
  const created = await ctx.deps.sqs.send(new CreateQueueCommand({ QueueName: `modaco-test-poll-${Date.now()}-${Math.random().toString(36).slice(2)}` }));
  testQueueUrl = created.QueueUrl!;
});

afterAll(async () => {
  await ctx.deps.sqs.send(new DeleteQueueCommand({ QueueUrl: testQueueUrl })).catch(() => {});
  await ctx.close();
});

describe('pollOnce', () => {
  it('deletes handled messages and leaves failed ones for redelivery', async () => {
    await ctx.deps.sqs.send(new SendMessageCommand({ QueueUrl: testQueueUrl, MessageBody: 'ok' }));
    await ctx.deps.sqs.send(new SendMessageCommand({ QueueUrl: testQueueUrl, MessageBody: 'fail' }));
    const seen: string[] = [];
    const r = await pollOnce(ctx.deps.sqs, testQueueUrl, async (m) => { seen.push(m.Body!); if (m.Body === 'fail') throw new Error('nope'); }, { waitSeconds: 1 });
    expect(seen.sort()).toEqual(['fail', 'ok']);
    expect(r).toEqual({ received: 2, succeeded: 1, failed: 1 });
    const empty = await pollOnce(ctx.deps.sqs, testQueueUrl, async () => {}, { waitSeconds: 1 });
    expect(empty.received).toBe(0); // 'fail' is invisible until its visibility timeout elapses
  });

  it('wraps a message as an SQSEvent', () => {
    const ev = sqsEventFrom({ MessageId: 'm1', ReceiptHandle: 'rh', Body: '{"a":1}', Attributes: { ApproximateReceiveCount: '2' } });
    expect(ev.Records).toHaveLength(1);
    expect(ev.Records[0]).toMatchObject({ messageId: 'm1', body: '{"a":1}', attributes: { ApproximateReceiveCount: '2' } });
  });
});
