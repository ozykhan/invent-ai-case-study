import { DeleteMessageCommand, ReceiveMessageCommand, type Message, type SQSClient } from '@aws-sdk/client-sqs';
import type { SQSEvent } from 'aws-lambda';

/** One receive cycle: handle up to `max` messages concurrently, delete each only after its handler resolves. */
export async function pollOnce(
  sqs: SQSClient, queueUrl: string, handle: (message: Message) => Promise<void>,
  opts: { waitSeconds?: number; max?: number } = {},
): Promise<{ received: number; succeeded: number; failed: number }> {
  const res = await sqs.send(new ReceiveMessageCommand({
    QueueUrl: queueUrl, MaxNumberOfMessages: opts.max ?? 10, WaitTimeSeconds: opts.waitSeconds ?? 5,
    MessageSystemAttributeNames: ['ApproximateReceiveCount'],
  }));
  const messages = res.Messages ?? [];
  const results = await Promise.allSettled(messages.map(async (m) => {
    await handle(m);
    await sqs.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle! }));
  }));
  const succeeded = results.filter((r) => r.status === 'fulfilled').length;
  return { received: messages.length, succeeded, failed: messages.length - succeeded };
}

export function sqsEventFrom(message: Message): SQSEvent {
  return {
    Records: [{
      messageId: message.MessageId ?? '', receiptHandle: message.ReceiptHandle ?? '', body: message.Body ?? '',
      attributes: {
        ApproximateReceiveCount: message.Attributes?.ApproximateReceiveCount ?? '1',
        SentTimestamp: '', SenderId: '', ApproximateFirstReceiveTimestamp: '',
      },
      messageAttributes: {}, md5OfBody: '', eventSource: 'aws:sqs', eventSourceARN: '', awsRegion: '',
    }],
  };
}
