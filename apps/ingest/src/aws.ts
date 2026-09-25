import { S3Client } from '@aws-sdk/client-s3';
import { SQSClient } from '@aws-sdk/client-sqs';
import type { IngestConfig } from './config';

/** With an endpoint (LocalStack) use path-style and static test credentials; without one, the default AWS provider chain. */
function base(config: Pick<IngestConfig, 'awsRegion' | 'awsEndpointUrl'>) {
  return config.awsEndpointUrl
    ? {
        region: config.awsRegion, endpoint: config.awsEndpointUrl,
        credentials: { accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test', secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test' },
      }
    : { region: config.awsRegion };
}

export const createS3 = (config: Pick<IngestConfig, 'awsRegion' | 'awsEndpointUrl'>) =>
  new S3Client({ ...base(config), forcePathStyle: Boolean(config.awsEndpointUrl) });
export const createSqs = (config: Pick<IngestConfig, 'awsRegion' | 'awsEndpointUrl'>) => new SQSClient(base(config));
