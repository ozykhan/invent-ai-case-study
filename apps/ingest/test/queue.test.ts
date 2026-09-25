import { SendMessageCommand } from '@aws-sdk/client-sqs';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { pollOnce, sqsEventFrom } from '../src/queue';
import { setupIngestTest, type IngestTestContext } from './helpers';

let ctx: IngestTestContext;
beforeAll(async () => { ctx = await setupIngestTest(); });
afterAll(() => ctx.close());
beforeEach(() => ctx.drainQueue(ctx.deps.config.dlqUrl));

describe('pollOnce', () => {
  it('deletes handled messages and leaves failed ones for redelivery', async () => {
    const url = ctx.deps.config.dlqUrl; // any queue without consumers works for this test
    await ctx.deps.sqs.send(new SendMessageCommand({ QueueUrl: url, MessageBody: 'ok' }));
    await ctx.deps.sqs.send(new SendMessageCommand({ QueueUrl: url, MessageBody: 'fail' }));
    const seen: string[] = [];
    const r = await pollOnce(ctx.deps.sqs, url, async (m) => { seen.push(m.Body!); if (m.Body === 'fail') throw new Error('nope'); }, { waitSeconds: 1 });
    expect(seen.sort()).toEqual(['fail', 'ok']);
    expect(r).toEqual({ received: 2, succeeded: 1, failed: 1 });
    const empty = await pollOnce(ctx.deps.sqs, url, async () => {}, { waitSeconds: 1 });
    expect(empty.received).toBe(0); // 'fail' is invisible until its visibility timeout elapses
  });

  it('wraps a message as an SQSEvent', () => {
    const ev = sqsEventFrom({ MessageId: 'm1', ReceiptHandle: 'rh', Body: '{"a":1}', Attributes: { ApproximateReceiveCount: '2' } });
    expect(ev.Records).toHaveLength(1);
    expect(ev.Records[0]).toMatchObject({ messageId: 'm1', body: '{"a":1}', attributes: { ApproximateReceiveCount: '2' } });
  });
});
