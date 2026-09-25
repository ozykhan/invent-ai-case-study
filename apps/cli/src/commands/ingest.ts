import { readFileSync, statSync } from 'node:fs';
import { basename } from 'node:path';
import type { Command } from 'commander';
import type { IngestionJob } from '../api-types';
import { emit, formatFields, formatTable, log } from '../output';
import { durationArg, intArg, withClient } from './common';

const jobFields = (j: IngestionJob) => formatFields([
  ['id', j.id], ['status', j.status], ['s3Key', j.s3Key],
  ['chunks', `${j.completedChunks}/${j.totalChunks} completed, ${j.failedChunks} failed`],
  ['rowsProcessed', j.rowsProcessed], ['rowsRejected', j.rowsRejected], ['error', j.error],
  ['createdAt', j.createdAt], ['updatedAt', j.updatedAt],
]);

export function registerIngest(program: Command): void {
  const ingest = program.command('ingest').description('Vendor file ingestion: upload a CSV, follow a job, list rejected rows');

  ingest.command('upload')
    .description('Create a job, PUT the file to the presigned URL, and poll until completed or failed')
    .argument('<file>', 'vendor CSV (header sku,name,category,vendor_price,stock)')
    .option('--poll <interval>', 'status poll interval', durationArg, 1000)
    .action((file: string, opts: { poll: number }, cmd: Command) => withClient(cmd, async (client, g) => {
      const size = statSync(file).size;
      const started = Date.now();
      const elapsed = () => (Date.now() - started) / 1000;
      // The API accepts [A-Za-z0-9._-]{1,128} as a filename.
      const job = await client.createIngestionJob(basename(file).replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 128) || 'vendor.csv');
      log(`job ${job.jobId}: uploading ${file} (${(size / 1_048_576).toFixed(1)} MB)`);
      const put = await fetch(job.uploadUrl, { method: 'PUT', body: readFileSync(file) });
      if (!put.ok) throw new Error(`upload failed: HTTP ${put.status} ${await put.text()}`);
      log(`uploaded in ${elapsed().toFixed(1)}s; waiting for the splitter and workers`);
      for (;;) {
        const j = await client.getIngestionJob(job.jobId);
        log(`[${elapsed().toFixed(1)}s] ${j.status} chunks ${j.completedChunks}/${j.totalChunks} rows ${j.rowsProcessed} rejected ${j.rowsRejected}`);
        if (j.status === 'completed' || j.status === 'failed') {
          const rows = j.rowsProcessed + j.rowsRejected;
          const secs = elapsed();
          emit(g.json, j, () => `${j.status}: ${rows} rows in ${secs.toFixed(1)}s (${Math.round(rows / secs)} rows/s), ${j.rowsRejected} rejected${j.error ? `\nerror: ${j.error}` : ''}`);
          if (j.status === 'failed') process.exitCode = 1;
          return;
        }
        await new Promise((resolve) => setTimeout(resolve, opts.poll));
      }
    }));

  ingest.command('status')
    .description('GET /ingestion/jobs/:id')
    .argument('<jobId>', 'job id (uuid)')
    .action((jobId: string, _opts: unknown, cmd: Command) => withClient(cmd, async (client, g) => {
      const j = await client.getIngestionJob(jobId);
      emit(g.json, j, () => jobFields(j));
    }));

  ingest.command('rejections')
    .description('GET /ingestion/jobs/:id/rejections')
    .argument('<jobId>', 'job id (uuid)')
    .option('--page <n>', 'page number', intArg('page'))
    .option('--page-size <n>', 'items per page (max 100)', intArg('page-size'))
    .action((jobId: string, opts: { page?: number; pageSize?: number }, cmd: Command) => withClient(cmd, async (client, g) => {
      const page = await client.listRejections(jobId, opts);
      emit(g.json, page, () => `${formatTable([
        ['chunk', 'line', 'reason', 'raw'],
        ...page.items.map((r) => [r.chunkIndex, r.lineNumber, r.reason, r.rawLine.length > 60 ? `${r.rawLine.slice(0, 57)}...` : r.rawLine]),
      ])}\npage ${page.pagination.page}, ${page.items.length} of ${page.pagination.total} rejections`);
    }));
}
