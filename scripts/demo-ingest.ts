import { readFileSync, statSync } from 'node:fs';

const api = process.env.API_URL ?? 'http://localhost:3000';
const file = process.argv[2] ?? 'tmp/vendor-500k.csv';
const started = Date.now();

const job = await (await fetch(`${api}/ingestion/jobs`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ filename: 'vendor.csv' }) })).json();
console.log(`job ${job.jobId}: uploading ${file} (${(statSync(file).size / 1_048_576).toFixed(1)} MB)`);
const put = await fetch(job.uploadUrl, { method: 'PUT', body: readFileSync(file) });
if (!put.ok) throw new Error(`upload failed: ${put.status}`);
console.log(`uploaded in ${((Date.now() - started) / 1000).toFixed(1)}s; waiting for the splitter and workers`);

for (;;) {
  const j = await (await fetch(`${api}/ingestion/jobs/${job.jobId}`)).json();
  const elapsed = ((Date.now() - started) / 1000).toFixed(1);
  console.log(`[${elapsed}s] ${j.status} chunks ${j.completedChunks}/${j.totalChunks} rows ${j.rowsProcessed} rejected ${j.rowsRejected}`);
  if (j.status === 'completed' || j.status === 'failed') {
    const total = j.rowsProcessed + j.rowsRejected;
    console.log(`${j.status}: ${total} rows in ${elapsed}s (${Math.round(total / Number(elapsed))} rows/s)${j.error ? `\nerror: ${j.error}` : ''}`);
    break;
  }
  await new Promise((r) => setTimeout(r, 1000));
}
